# Cross-ledger policy

Cross-ledger policy lets a single **model ledger** hold a policy
rule set that governs many **data ledgers** that reference it.
Update the model once and every governed data ledger sees the new
rules on its next request — no per-dataset rule duplication.

This page covers how to configure cross-ledger policy. For the
underlying design (resolver contract, term-space translation,
cache shape, failure taxonomy) see
[Cross-ledger model enforcement](../design/cross-ledger-model-enforcement.md).

## When to use it

Cross-ledger policy is the right tool when:

- Multiple data ledgers share a common access-control model
  (e.g., every customer dataset enforces the same baseline
  policy on `Document` / `User` classes).
- Policy authoring needs to be decoupled from data authoring
  (a security team owns the model ledger; product teams own the
  data ledgers).
- Updates to policy rules must propagate atomically across all
  governed datasets — no per-dataset re-sync window.

If your policy lives entirely inside one ledger, stick with the
local pattern in
[Policy model and inputs](policy-model.md) — it's simpler.

## The two-ledger pattern

| Term            | Meaning |
|-----------------|---------|
| **Model ledger** (M) | The ledger holding the policy rule set. Identified by its canonical id (e.g., `org/governance:main`). |
| **Data ledger** (D) | The application ledger holding the data being protected. References M in its `#config`. |

Both ledgers must live on the same Fluree instance (same
nameservice, same storage namespace). Cross-instance federation is
out of scope.

## Setting up the model ledger

The model ledger holds policy resources just like a same-ledger
configuration would — there is nothing special about them on M's
side. The convention is to put them in a named graph so they
don't mix with any data that happens to live on M:

```trig
@prefix f:    <https://ns.flur.ee/db#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:   <http://example.org/ns/> .

GRAPH <http://example.org/governance/policies> {
    ex:denyUsers
        rdf:type    f:AccessPolicy ;
        f:action    f:view ;
        f:onClass   ex:User ;
        f:allow     false .
}
```

That graph IRI (`http://example.org/governance/policies` above) is
what the data ledger's config will name. Any number of policies
can live in the same graph; they're all loaded together on
resolution.

## Configuring the data ledger

D's `#config` declares an `f:policySource` whose `f:graphSource`
carries an explicit `f:ledger` field pointing at M:

```trig
@prefix f:   <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:mydb:main#config> {
    <urn:cfg:main> rdf:type f:LedgerConfig ;
        f:policyDefaults <urn:cfg:policy> .

    <urn:cfg:policy>
        f:defaultAllow false ;
        f:policyClass  f:AccessPolicy ;
        f:policySource <urn:cfg:policy-ref> .

    <urn:cfg:policy-ref> rdf:type f:GraphRef ;
        f:graphSource <urn:cfg:policy-src> .

    <urn:cfg:policy-src>
        f:ledger        <org/governance:main> ;
        f:graphSelector <http://example.org/governance/policies> .
}
```

Three things to notice:

1. **`f:ledger`** carries the canonical id of the model ledger
   (`org/governance:main`). Use `nameservice.lookup()` if you
   need to confirm the canonical form — aliases are resolved
   into the canonical id before the resolver runs.
2. **`f:graphSelector`** names the graph IRI within M that
   holds the policies. It must match exactly what M used in
   its `GRAPH <...>` block — there's no fuzzy matching.
3. **`f:policyClass`** is what determines which rules from M
   actually apply. See below.

## How `f:policyClass` filtering works

When D's request reaches the resolver, every rule materialized
from M's policy graph is filtered against the data ledger's
configured `f:policyClass` set by exact IRI intersection. A rule
passes the filter if any of its `rdf:type` IRIs appears in D's
`f:policyClass` list.

| D's `f:policyClass`                  | Rules from M that apply |
|--------------------------------------|--------------------------|
| not set                              | Defaults to `{f:AccessPolicy}` — all `rdf:type f:AccessPolicy` rules apply. |
| `f:AccessPolicy`                     | All `rdf:type f:AccessPolicy` rules apply. |
| `ex:OrgPolicy`                       | Only rules typed `rdf:type ex:OrgPolicy`. |
| `f:AccessPolicy`, `ex:OrgPolicy`     | Rules typed as either. |

The match is **exact-IRI only**. There is no subclass entailment:
declaring `ex:OrgPolicy rdfs:subClassOf f:AccessPolicy` doesn't
make `ex:OrgPolicy`-typed rules match a config that asks for
`f:AccessPolicy`. This mirrors the same-ledger
`load_policies_by_class` behavior.

The `{f:AccessPolicy}` default makes "set `f:policySource` and
get baseline enforcement" the no-configuration path. Custom-typed
rules are opt-in — operators name the class to enroll them.

## Engaging policy enforcement

There's a subtlety in how the server's JSON-LD query route
chooses whether to invoke policy enforcement at all. Requests
without an `fluree-policy-class`, `fluree-identity`, or inline
`opts.policy` go through a no-policy fast path that bypasses the
cross-ledger dispatch. A configured `f:policySource` in `#config`
is **not** enough on its own to force enforcement at the HTTP
layer today.

To engage cross-ledger policy via HTTP, send a request with at
least one of:

- `fluree-policy-class: <iri>` — the policy class header (the
  cleanest way to declare "use the configured policy"). Matching
  the class in D's config (e.g., `f:AccessPolicy`) is the
  natural choice.
- `fluree-identity: <iri>` — an identity header. Identity-mode
  has a different contract; see below.
- `opts.policy` in the body — inline JSON-LD policy. This still
  merges with cross-ledger rules.

When using the in-process Rust API, calling
`fluree.db_with_policy(ledger_id, &opts)` always engages the
policy path, even with empty opts. Programmatic users don't see
this gating.

## Limitations

The following behaviors are **not yet implemented** and fail
closed when configured:

| Configuration                              | Outcome |
|--------------------------------------------|---------|
| `f:atT` (temporal pinning of M)            | Request fails with `UnsupportedFeature { feature: "f:atT", phase: "Phase 3" }`. |
| `f:trustPolicy` (commit-signer allowlist)  | Request fails with `UnsupportedFeature`. |
| `f:rollbackGuard` (freshness constraints)  | Request fails with `UnsupportedFeature`. |
| `opts.identity` + cross-ledger `f:policySource` | Request fails with a config error. Identity-mode loads policies via the identity's `f:policyClass` triples, which would have to resolve in D (the identity isn't an M concept); combining the two modes ambiguously is rejected rather than silently choosing one. Use `opts.policy_class` with cross-ledger configs. |
| `f:policySource` with `f:graphSelector` naming M's `#config` or `#txn-meta` | Request fails with `ReservedGraphSelected` before any storage read on M. |
| `f:ledger` on `f:shapesSource`, `f:schemaSource`, `f:rulesSource`, `f:constraintsSource` | Request fails — cross-ledger support is currently only implemented for `f:policySource`. |

The other reserved fields and source predicates may land in
later releases; the resolver's contract is shared across all of
them. See [Cross-ledger model enforcement → Scope](../design/cross-ledger-model-enforcement.md#scope).

## Failure modes

When cross-ledger resolution fails, the request returns HTTP
**502 Bad Gateway** with a structured JSON body naming the
specific failure:

```json
{
  "status": 502,
  "@type": "err:system/CrossLedgerError",
  "error": "model ledger 'org/governance:main' is not present on this instance"
}
```

The specific failure modes operators see:

| Variant                       | Trigger |
|-------------------------------|---------|
| `ModelLedgerMissing`          | The named model ledger isn't present or is retracted on this instance. |
| `GraphMissingAtT`             | The model ledger exists but the named graph IRI isn't in its graph registry. |
| `ReservedGraphSelected`       | The selector targets `#config` or `#txn-meta` on M. |
| `TranslationFailed`           | The policy graph was read but couldn't be projected to the wire format (typically corruption or a dictionary loss in M). |
| `UnsupportedFeature`          | A reserved field (`f:atT` / `f:trustPolicy` / `f:rollbackGuard`) was set. |
| `CrossInstanceUnsupported`    | `f:ledger` names a ledger on a different instance. |
| `CycleDetected`               | A model ledger graph transitively references itself. |

The choice of 502 rather than 500 is deliberate: the data ledger
isn't broken — its upstream governance dependency is — and
operators distinguishing those two cases in their dashboards
matters. The wrapped variant is preserved in the response body so
clients can branch on the specific failure.

## Behavior on model ledger updates

There is no explicit invalidation channel. The cache key includes
the model ledger's `resolved_t` (its commit head at the time of
capture), so new commits to M produce new cache keys
automatically. The next request after M advances captures the
new head; older entries age out under the cache's LRU/TinyLFU
policy.

Within a single request, every cross-ledger reference to the
same M reuses one `resolved_t` value. Policy and any future
shapes / schema lookups on the same M can never disagree about
which version they're enforcing for that request.

If M is dropped while D references it, the next request against
D that needs governance from M fails closed with
`ModelLedgerMissing`. D isn't proactively notified — the
failure surface is the next request.

## Related

- [Cross-ledger model enforcement](../design/cross-ledger-model-enforcement.md) — design rationale and the shared resolver contract.
- [Policy model and inputs](policy-model.md) — policy structure (the rules themselves look the same in cross-ledger configs as in same-ledger ones).
- [Setting groups](../ledger-config/setting-groups.md#policy-defaults) — `f:policySource` and the full `f:GraphRef` shape in the config schema.
- [Programmatic policy API (Rust)](programmatic-policy.md) — how cross-ledger interacts with the in-process Rust API.
