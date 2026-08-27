# Cross-ledger governance

A single **model ledger** can hold governance artifacts —
policy rules, uniqueness constraints, ontology/schema axioms,
SHACL shapes, and datalog rules — that govern many **data
ledgers** that reference it. Update the model once and every
governed data ledger sees the new artifacts on its next
request, with no per-dataset duplication.

All five `f:GraphRef`-shaped governance predicates support
cross-ledger references today:

- **Cross-ledger policy** (`f:policySource` with `f:ledger`) —
  M's policy rule set is applied to queries (`f:view`) and
  transactions (`f:modify`) against D.
- **Cross-ledger constraints** (`f:constraintsSource` with
  `f:ledger`) — M's `f:enforceUnique` annotations are applied
  to transactions against D.
- **Cross-ledger schema** (`f:schemaSource` with `f:ledger`) —
  M's RDFS/OWL axioms (class hierarchy, property hierarchy,
  domain/range, equivalences, owl:imports declarations) feed
  D's reasoner. Single-graph only today; transitive
  `owl:imports` recursion across multiple model ledgers is
  reserved.
- **Cross-ledger SHACL shapes** (`f:shapesSource` with
  `f:ledger`) — M's shape definitions are compiled against D's
  staged namespace at validation time and rejected/accepted
  transactions on D accordingly.
- **Cross-ledger datalog rules** (`f:rulesSource` with
  `f:ledger`) — M's `f:rule` JSON bodies feed D's query-time
  datalog evaluator alongside any rules D stores locally.

This page covers configuration for all five. For the
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

**Transactions engage cross-ledger policy automatically.** The
transact path (JSON-LD / SPARQL UPDATE / Turtle / TriG through
the server, push replication, credentialed transactions, and the
CLI's local mode with policy flags) resolves D's config before
staging: a cross-ledger `f:policySource` always builds a policy
context, and M's `f:modify` rules are enforced on the staged
flakes even when the request carries no policy inputs at all.
Config `f:defaultAllow` / `f:policyClass` defaults merge in the
same way they do for reads.

For **queries**, there's a subtlety in how the server's JSON-LD
query route chooses whether to invoke policy enforcement at all.
Requests without an `fluree-policy-class`, `fluree-identity`, or
inline `opts.policy` go through a no-policy fast path that
bypasses the cross-ledger dispatch. A configured `f:policySource`
in `#config` is **not** enough on its own to force enforcement at
the HTTP query layer today.

To engage cross-ledger policy on an HTTP query, send a request
with at least one of:

- `fluree-policy-class: <iri>` — the policy class header (the
  cleanest way to declare "use the configured policy"). Matching
  the class in D's config (e.g., `f:AccessPolicy`) is the
  natural choice.
- `fluree-identity: <iri>` — an identity header. Under
  cross-ledger the identity is bind-only; see
  [Identity binding](#identity-binding-under-cross-ledger-policy).
- `opts.policy` in the body — inline JSON-LD policy. This still
  merges with cross-ledger rules.

When using the in-process Rust API, calling
`fluree.db_with_policy(ledger_id, &opts)` always engages the
policy path, even with empty opts. Programmatic users don't see
this gating. The write-side equivalent is
`build_transact_policy_context` — see
[Programmatic policy API (Rust)](programmatic-policy.md).

## Identity binding under cross-ledger policy

An identity on the request (`fluree-identity` header,
`opts.identity`, or a verified credential's DID) is **bind-only**
under a cross-ledger `f:policySource`:

- The identity resolves against **D** (identities are a
  data-ledger concept — M never contributes identity records) and
  populates `?$identity` for any `f:query` rules in M's policy
  set. An owner-only rule authored in M therefore works across
  every governed data ledger, with each D binding its own
  identities.
- The identity **never selects rules**. Same-ledger identity-mode
  loads policies via the identity's `f:policyClass` triples;
  under cross-ledger those D-local triples are intentionally not
  consulted — declaring a cross-ledger `f:policySource` makes M
  the policy authority, and rule selection is exclusively the
  policy-class filter chain:

  1. the request's `policy_class` (when present),
  2. else the config's `f:policyClass`,
  3. else — for anonymous requests only — `{f:AccessPolicy}`.

- Because the identity can't select rules, an identity-carrying
  request with **no policy class anywhere** (request or config)
  fails closed: the operator must name which classes govern.
  In practice, setting `f:policyClass` in D's config (as in the
  configuration example above) makes authenticated requests work
  with no per-request changes.
- An identity IRI with no subject node in D yields an unbound
  `?$identity`: `f:query` rules referencing it match nothing, so
  `f:required` rules deny — the same contract as same-ledger
  identity-mode's unknown-identity case.
- **Identity records must live in D's default graph.** The
  subject-existence probe that decides whether to bind `?$identity`
  searches D's default graph only. An identity whose subject node
  is written into a *named* graph is treated as absent (unbound
  `?$identity`), so `f:required` rules deny even for a legitimate
  owner. Keep identity/user records in the default graph, or bind
  `?$identity` explicitly via `opts.policy_values`.

One merge subtlety: an identity counts as a request policy input,
so under the default `f:overrideControl` (`f:OverrideAll`) the
request's options take precedence and the config's
`f:defaultAllow` is **not** merged for identity-carrying requests
(same long-standing contract as same-ledger reads). Send the
`fluree-default-allow` header explicitly, or set a stricter
override control if the config should always win.

## Cross-ledger uniqueness constraints

Same two-ledger pattern, different subsystem. M holds an
`f:enforceUnique true` annotation on a property; D references
M's constraints graph in its `#config`. Every transaction
against D that would create a duplicate value on that property
is rejected.

```trig
# On model ledger M
@prefix f:  <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

GRAPH <http://example.org/governance/constraints> {
    ex:email f:enforceUnique true .
}
```

```trig
# On data ledger D — #config
@prefix f:   <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:mydb:main#config> {
    <urn:cfg:main> rdf:type f:LedgerConfig ;
        f:transactDefaults <urn:cfg:transact> .

    <urn:cfg:transact>
        f:uniqueEnabled      true ;
        f:constraintsSource  <urn:cfg:cref> .

    <urn:cfg:cref> rdf:type f:GraphRef ;
        f:graphSource <urn:cfg:csrc> .

    <urn:cfg:csrc>
        f:ledger        <org/governance:main> ;
        f:graphSelector <http://example.org/governance/constraints> .
}
```

After this config lands on D, the next transaction that creates
a duplicate `ex:email` value is rejected with
`TransactError::UniqueConstraintViolation`. The annotation
itself never appears on D — D enforces it because its config
points at M.

A few specifics that differ from cross-ledger policy:

- Constraints are enforced at **transaction time**, not query
  time. No HTTP header gymnastics are needed to engage them —
  the staging pipeline picks them up automatically whenever the
  data ledger's config has `f:uniqueEnabled true` plus a
  cross-ledger `f:constraintsSource`.
- Failures during cross-ledger constraints resolution surface
  as `TransactError::Parse` (mapped to HTTP **400 Bad Request**
  at the API layer) rather than `ApiError::CrossLedger` (502).
  That's a staging-pipeline quirk — the error message preserves
  the underlying cross-ledger failure variant for diagnostics,
  but the HTTP status differs from the query-side policy path.
- The wire artifact for constraints is simpler than policy:
  just a list of property IRIs. There's no equivalent of
  `f:policyClass` filtering — every property M declares unique
  applies.

## Cross-ledger schema / ontology

Same two-ledger pattern, third subsystem. M holds an RDFS/OWL
schema in a named graph (class hierarchy, property hierarchy,
domain/range, equivalences); D references that graph in its
`#config` under `f:reasoningDefaults.f:schemaSource`. When D's
queries enable reasoning, M's axioms feed the reasoner exactly
as if they lived on D.

```trig
# On model ledger M — ontology in a named graph
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <http://example.org/ns/> .

GRAPH <http://example.org/ontology/core> {
    ex:Animal  rdf:type        owl:Class .
    ex:Dog     rdf:type        owl:Class ;
               rdfs:subClassOf ex:Animal .

    ex:knows   rdf:type           owl:ObjectProperty .
    ex:friend  rdf:type           owl:ObjectProperty ;
               rdfs:subPropertyOf ex:knows .
}
```

```trig
# On data ledger D — #config
@prefix f:   <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:mydb:main#config> {
    <urn:cfg:main> rdf:type f:LedgerConfig ;
        f:reasoningDefaults <urn:cfg:reasoning> .

    <urn:cfg:reasoning>
        f:reasoningModes  ( "rdfs" "owl2rl" ) ;
        f:schemaSource    <urn:cfg:schema-ref> .

    <urn:cfg:schema-ref> rdf:type f:GraphRef ;
        f:graphSource <urn:cfg:schema-src> .

    <urn:cfg:schema-src>
        f:ledger        <org/ontology:main> ;
        f:graphSelector <http://example.org/ontology/core> .
}
```

With this config, a query against D for `?s rdf:type ex:Animal`
returns every `ex:Dog` instance on D (subClassOf reasoning) —
even though `ex:Dog rdfs:subClassOf ex:Animal` never appears on
D itself.

The materializer pulls a **whitelisted subset** of axioms from
M's schema graph: the predicates `rdfs:subClassOf`,
`rdfs:subPropertyOf`, `rdfs:domain`, `rdfs:range`,
`owl:inverseOf`, `owl:equivalentClass`, `owl:equivalentProperty`,
`owl:sameAs`, `owl:imports`, plus `rdf:type` declarations for
the schema class set (`owl:Class`, `owl:ObjectProperty`,
`owl:DatatypeProperty`, the property characteristic classes,
`owl:Ontology`, `rdf:Property`). Instance data in the schema
graph is filtered out; only axioms cross over.

A few specifics that differ from cross-ledger policy:

- Reasoning must be **enabled** for cross-ledger schema to take
  effect. The data ledger's config can set
  `f:reasoningModes` (e.g., `["rdfs"]` or `["owl2rl"]`), or
  the query can opt in via the `reasoning` option.
- Failures during cross-ledger schema resolution surface as
  `ApiError::OntologyImport` (with the underlying
  `CrossLedgerError` displayed in the message) rather than
  `ApiError::CrossLedger`. That preserves continuity with the
  same-ledger ontology-imports error path.
- Single graph only in this phase: `owl:imports` triples in
  M's schema graph are carried through to the wire (so a
  future reader can see them), but the resolver does NOT
  transitively follow them across ledger boundaries yet. If M's
  schema declares `owl:imports <X>` where X is on a different
  model ledger M2, that import isn't resolved.

## Cross-ledger SHACL shapes

Configure `f:shapesSource` on D's `#config` under
`f:shaclDefaults`. When the source carries `f:ledger`, the
resolver pulls M's shapes graph at transaction time, compiles
the SHACL whitelist against D's *staged* namespace registry
(post-stage, not pre-stage — so IRIs the in-flight transaction
introduced are encodable), and feeds the resulting overlay to
the same SHACL engine the same-ledger path uses.

```trig
# On model ledger M — shapes in a named graph
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix ex:   <http://example.org/ns/> .

GRAPH <http://example.org/governance/shapes> {
    ex:PersonShape a sh:NodeShape ;
        sh:targetClass ex:Person ;
        sh:property [
            sh:path     ex:name ;
            sh:minCount 1 ;
            sh:datatype xsd:string
        ] .
}
```

```trig
# On data ledger D — #config
@prefix f:   <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:data:main#config> {
    <urn:cfg:main>      a f:LedgerConfig ;
                        f:shaclDefaults <urn:cfg:shacl> .
    <urn:cfg:shacl>     f:shaclEnabled  true ;
                        f:shapesSource  <urn:cfg:sref> .
    <urn:cfg:sref>      a f:GraphRef ;
                        f:graphSource   <urn:cfg:ssrc> .
    <urn:cfg:ssrc>      f:ledger        <model:main> ;
                        f:graphSelector <http://example.org/governance/shapes> .
}
```

Specifics:

- Shapes are **compiled against D's staged namespace registry**,
  not its pre-stage snapshot. The in-flight transaction's
  ns_registry sees the IRIs the transaction is introducing
  (e.g., `ex:Person` declared by the same tx the shape is
  validating), which the pre-stage snapshot doesn't.
- IRIs the staged registry has never seen are dropped silently —
  M-only IRIs can't apply to data D doesn't have, and
  allocating a fresh namespace code for an M-only term would
  introduce namespace churn into D for no benefit.
- Inline `opts.shapes` layers **additively** on top of the
  cross-ledger source: both shape sets enforce. See
  [Cookbook: SHACL validation — Inline shapes per
  transaction](../guides/cookbook-shacl.md#inline-shapes-per-transaction).
- **Enforced surfaces**: JSON-LD transactions (insert / upsert /
  update, including TriG upserts), direct-flake Turtle inserts,
  and validation reports (`fluree validate` in ledger mode, the
  HTTP validate endpoint, `Fluree::validate_ledger`). Commit
  replay (graph-sync push) intentionally skips SHACL
  re-validation for cross-ledger sources: the origin already
  validated against M when the commit was authored, and
  re-resolving M at replay time could see a different head.
- **`sh:sparql` constraints travel over the wire** — the query
  text, `sh:prefixes` declarations, and their `owl:imports`
  closure are all projected. One caveat: the query lowers
  against D's committed namespace registry, so a constraint over
  a predicate whose namespace has never been committed to D
  cannot match until that namespace lands (this mirrors the
  same-ledger behavior).
- **Steady-state cost is one nameservice lookup.** The wire is
  cached per `(model, graph, resolved_t)` and the *compiled*
  shapes (including parsed `sh:sparql` queries) are reused
  across transactions while M's head and D's shape-affecting
  epochs are unchanged. A commit on M invalidates both on the
  next transaction — governance updates propagate immediately.

## Cross-ledger datalog rules

Configure `f:rulesSource` on D's `#config` under
`f:datalogDefaults`. When the source carries `f:ledger`, the
resolver pulls the `f:rule` JSON bodies from M's rules graph at
query time and merges them into D's query-time datalog evaluator
alongside any rules D stores locally and any rules supplied via
the top-level `rules` query field.

```trig
# On model ledger M — rules in a named graph
@prefix f:   <https://ns.flur.ee/db#> .
@prefix ex:  <http://example.org/> .

GRAPH <http://example.org/governance/rules> {
    ex:grandparentRule f:rule """
        {
          \"@context\": {\"ex\": \"http://example.org/\"},
          \"where\":   {\"@id\": \"?p\", \"ex:parent\": {\"ex:parent\": \"?g\"}},
          \"insert\":  {\"@id\": \"?p\", \"ex:grandparent\": {\"@id\": \"?g\"}}
        }
    """^^rdf:JSON .
}
```

```trig
# On data ledger D — #config
@prefix f:   <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:data:main#config> {
    <urn:cfg:main>     a f:LedgerConfig ;
                       f:datalogDefaults <urn:cfg:datalog> .
    <urn:cfg:datalog>  f:datalogEnabled  true ;
                       f:rulesSource     <urn:cfg:rref> .
    <urn:cfg:rref>     a f:GraphRef ;
                       f:graphSource     <urn:cfg:rsrc> .
    <urn:cfg:rsrc>     f:ledger          <model:main> ;
                       f:graphSelector   <http://example.org/governance/rules> .
}
```

Specifics:

- Rules are **inherently term-portable** — the JSON body is a
  JSON-LD document whose IRIs resolve at parse time against
  D's snapshot, the same way query-time rules from the top-level
  `rules` field are handled. No term translation step on the
  wire.
- Engaging the rules still requires datalog reasoning on the
  query: either `"reasoning": "datalog"` on the JSON-LD query
  or `PRAGMA reasoning: datalog` on SPARQL.
- **Fail-closed on malformed rules.** A bad JSON body in M's
  rules graph errors the query rather than silently dropping
  the rule (this differs from query-time `rules` where a single
  bad entry only logs a warning — cross-ledger rules are
  admin-authored, so silently weakening the configured
  reasoning model is the worst failure mode).

## Limitations

The following behaviors are **not yet implemented** and fail
closed when configured:

| Configuration                              | Outcome |
|--------------------------------------------|---------|
| `f:atT` (temporal pinning of M)            | Request fails with `UnsupportedFeature { feature: "f:atT", phase: "Phase 3" }`. |
| `f:trustPolicy` (commit-signer allowlist)  | Request fails with `UnsupportedFeature`. |
| `f:rollbackGuard` (freshness constraints)  | Request fails with `UnsupportedFeature`. |
| `opts.identity` + cross-ledger `f:policySource` **with no policy class anywhere** | Request fails with a config error. The identity is bind-only under cross-ledger (see [Identity binding](#identity-binding-under-cross-ledger-policy)) and can't select rules, so a policy class must be named on the request or in D's config. With a class available, identity-carrying requests work normally. |
| `f:policySource` with `f:graphSelector` naming M's `#config` or `#txn-meta` | Request fails with `ReservedGraphSelected` before any storage read on M. |
| Transitive `owl:imports` across model ledgers (`f:schemaSource` recursion) | Not yet honored. Imports inside M's schema graph are projected but the resolver doesn't follow them across ledger boundaries. |

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
