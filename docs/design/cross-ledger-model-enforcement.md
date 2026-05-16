# Cross-ledger model enforcement

Fluree's `f:GraphRef` shape was designed to point at "the graph that
holds my policy / shapes / schema / rules / constraints." Today that
pointer is constrained to the *current* ledger. This document
specifies the contract for making the pointer cross-ledger so that a
single **model ledger** — holding the ontology, SHACL shapes, policy
rule set, datalog rules, and uniqueness constraints — can be
referenced by many **data ledgers** that it governs.

Status: design — implementation lands incrementally per the phasing
in the last section.

Topics:

- Glossary and the basic shape of cross-ledger enforcement.
- The resolver contract: a single `resolve_graph_ref` helper shared
  by all five subsystems, returning term-neutral artifacts.
- Term-space translation — why model-ledger Sids/GraphIds/t values
  cannot leak into data-ledger execution.
- Resolution time, `f:atT` pinning, caching, and failure variants.
- Policy IR identity split — definitional vs contextual term
  binding.
- Trust model, reserved-graph guards, cycle detection, drop
  interaction.
- Phasing and scope.

Related docs:

- [Ontology imports](ontology-imports.md) — the same-ledger
  schema-source mechanism this generalizes.
- [Policy enforcement](../concepts/policy-enforcement.md) — the
  enforcement model the model ledger contributes rules to.
- [Configuration / setting groups](../ledger-config/setting-groups.md)
  — the `f:GraphRef` shape and where it appears.

## Glossary

| Term                        | Meaning |
|-----------------------------|---------|
| **Data ledger** (D)         | The ledger holding application data and serving the request. |
| **Model ledger** (M)        | A ledger referenced by D's `#config` to provide governance artifacts (policy / shapes / schema / rules / constraints). |
| **Governance artifact**     | The materialized output of resolving a `f:GraphRef` for one subsystem: a policy rule set, a shape set, a schema closure, a datalog rule set, or a constraint set. |
| **Resolved t** (`resolved_t`) | The transaction time of M at which the artifact is materialized for this request. |
| **Canonical ledger id**     | `NsRecord.ledger_id` from `nameservice.lookup()`, never a user-typed alias. |

## What "cross-ledger" means at the contract level

A data ledger D's `#config` declares a `f:GraphRef` whose `f:ledger`
field names a model ledger M. When D is served a request, every
configured `f:*Source` predicate that points at M is resolved to a
governance artifact materialized from M and applied to the request
against D. The data ledger's policy authority, identity, and term
space remain the binding authority for the request; M contributes
*rules*, not *identities*.

This is read-only: cross-ledger writes are out of scope.

## The resolver contract

All five subsystems — policy, shapes, schema, datalog rules,
constraints — share a single helper:

```rust
pub(crate) async fn resolve_graph_ref<S, N>(
    graph_ref: &GraphRef,
    ctx: &ResolveCtx<'_, S, N>,
) -> Result<ResolvedGraph, CrossLedgerError>;
```

where `ResolveCtx` carries everything the resolver needs without each
subsystem rebuilding the surrounding state:

```rust
pub(crate) struct ResolveCtx<'a, S, N> {
    pub data_ledger_id: &'a str,                    // canonical id of D
    pub fluree: &'a Fluree<S, N>,                    // nameservice lookup of M
    /// Governance-context capture: lazily-populated map from canonical
    /// model ledger id → `resolved_t` for this request. First
    /// cross-ledger resolve to a given model populates the entry; every
    /// subsequent unpinned reference to that model in the same request
    /// reuses it.
    pub resolved_ts: &'a mut HashMap<String, i64>,
    /// Active resolution stack — the chain of `(ledger, graph,
    /// resolved_t)` tuples currently being resolved. Used only for cycle
    /// detection; an entry is pushed before recursion and popped after.
    pub active: &'a mut Vec<(String, String, i64)>,
    /// Per-request memo of fully-resolved artifacts. Hits short-circuit
    /// without storage round-trip and without entering the cycle stack.
    pub memo: &'a mut HashMap<(String, String, i64), Arc<ResolvedGraph>>,
}
```

The helper performs, in order:

1. **Same-instance check.** M must live on the same nameservice and
   storage namespace as D. Cross-instance federation is out of
   scope (see below).
2. **Canonical id resolution.** `f:ledger` is run through
   `nameservice.lookup()`; the returned `NsRecord.ledger_id` is used
   for every subsequent step, never the user-typed alias. (Same
   discipline as the default-context content store.)
3. **Reserved-graph guard.** Selectors that would resolve to
   `#config` or `#txn-meta` on M are rejected *before* any storage
   round-trip.
4. **Governance-context capture (`resolved_t`).** If `f:atT N` is set,
   `resolved_t = N`. Otherwise, look up `ctx.resolved_ts[model_id]`;
   on miss, read M's current head `t` and store it. The capture is
   **lazy and per request**: the head is consulted only on the first
   unpinned reference to a given model, and every later unpinned
   reference in the same request reuses the same `resolved_t` so
   policy and shapes can never disagree about which version of M
   they're enforcing.
5. **Memo / cycle check.** Form the tuple `(canonical_model_ledger_id,
   graph_iri, resolved_t)`. If it appears in `ctx.memo`, return the
   memoized artifact (this is the cross-subsystem de-dup path —
   `policySource` and `shapesSource` pointing at the same model graph
   resolve once). Otherwise check it against `ctx.active` (the
   resolution stack); presence there means a true cycle and is an
   error. Two different `atT` pins of the same `(ledger, graph)` are
   not a cycle; the same triple is. Push the tuple onto `active`
   before recursing into transitive imports.
6. **Translation and materialization.** The graph at `resolved_t` is
   read and projected into term-neutral form (see next section).
   Pop the tuple from `active` and insert the resolved artifact into
   `ctx.memo` and the global cache.
7. **Caching.** On cache hit the materialized artifact is returned
   directly. On miss the artifact is inserted under the key
   `(canonical_model_ledger_id, graph_iri, resolved_t)`.

A `ResolvedGraph` is term-neutral and t-fixed:

```rust
pub struct ResolvedGraph {
    pub model_ledger_id: String,    // canonical
    pub graph_iri: String,
    pub resolved_t: i64,
    pub artifact: GovernanceArtifact, // tagged union per subsystem
    pub fingerprint: ContentId,      // for downstream cache keys
}
```

The `GovernanceArtifact` variants are:

```rust
pub enum GovernanceArtifact {
    PolicyRules(PolicyRuleSet),       // canonical IR, IRI-form
    Shapes(ShapeSet),                 // SHACL shapes in IRI-form
    SchemaClosure(SchemaBundleIR),    // ontology in IRI-form
    DatalogRules(DatalogRuleSet),     // rules in IRI-form
    Constraints(ConstraintSet),       // f:enforceUnique annotations
}
```

Each variant is **term-neutral**: every subject, predicate, object,
class, and datatype reference is stored as an IRI (or canonical
literal), never as a model-ledger Sid or GraphId.

## Term-space translation

This is the load-bearing technical claim and the reason the resolver
contract changes rather than just gaining a new `f:ledger` branch.

Within a ledger, Fluree internally identifies subjects/predicates by
`Sid(namespace_code, local_id)` and graphs by `GraphId(u16)`. Both
are *ledger-local*. The IRI `<http://example.org/Person>` may be
`Sid(ns=7, id=42)` in M and `Sid(ns=13, id=200)` in D. M's
`GraphId(3)` and D's `GraphId(3)` have nothing to do with each
other. M's `t=10` and D's `t=3` are not comparable.

Today's same-ledger schema bundle (`build_schema_bundle_flakes` in
`fluree-db-query/src/schema_bundle.rs`) returns raw flakes and
relies on the data query's `to_t` for further filtering. That
contract makes three implicit assumptions that all break
cross-ledger:

1. Sid namespace codes are shared between source and consumer.
2. Graph ids are shared.
3. The artifact's `t` is comparable to the consumer's `t`.

The cross-ledger contract therefore returns a **term-space-neutral,
model-t-fixed** artifact. Concretely:

- All identifiers in the artifact are IRIs (interned at use site
  against D's term space) or canonical literal values.
- All time filtering against M is applied *at materialization time*
  inside the resolver; the consumer never re-applies its own `to_t`
  to the resolved artifact.
- The data-ledger executor re-interns IRIs as needed against D's
  dictionary. New IRIs that D has not seen are interned on demand
  (the standard intern path); they do not need to pre-exist in D's
  registry.

The cache key is `(canonical_model_ledger_id, graph_iri,
resolved_t)` — *not* parameterized by the data ledger's term space.
This is the property that makes "model edit propagates atomically to
all governed datasets" cheap: one cache entry per `(model, graph,
t)` is reused across every data ledger that references it.

Per-data-ledger interning is a derived view of the cached artifact,
memoized at the use site only if profiling shows interning is a
bottleneck.

## Policy IR identity split

Policy rules typically carry two kinds of term references that the
existing IR conflates:

| Reference kind | Examples                                      | Must bind in |
|----------------|-----------------------------------------------|--------------|
| Definitional  | Rule classes, predicate IRIs, target classes  | M (model ledger) |
| Contextual    | Request identity, tested resource subjects    | D (data ledger) + request context |

A naive "load rules from M, evaluate them as if D were M" build
produces a worse-than-useless result: only model-ledger identities
could ever satisfy the rules.

The IR therefore distinguishes the two binding scopes explicitly.
When the resolver returns a `PolicyRules` artifact, every term
reference is tagged with its scope (`Definitional` resolves in M's
term space at materialization time; `Contextual` is left as an IRI
to bind at evaluation time against D).

Authentication and identity flow only one way: from D and the
request context. The model ledger contributes rules and definitions;
it never contributes identity, authentication keys, or session
state. This keeps trust one-directional.

### Identity-mode resolution under cross-ledger policy

The same-ledger materializer in `policy_builder.rs` supports three
modes, in priority order: identity (request identity → policies via
that identity's `f:policyClass`), policy_class (configured class IRIs
→ all policies of that class), and policy (inline JSON-LD).

Cross-ledger does **not** generalize "identity mode" by querying
M for `<identity> f:policyClass ?class`. The identity binding lives
in D and the request context; querying M for identity records would
either return empty (M has no entry for D's user) or — worse — match a
model-ledger identity that happens to share an IRI with D's user. The
resulting policy attribution is silently wrong.

Concretely, cross-ledger policy resolution always uses
**policy_class mode**: the data ledger's effective policy classes
(from `opts.policyClass` or D's config) are looked up in M to load
the corresponding policy rules. The request identity is bound to
`?$identity` at evaluation time against D and the request context,
exactly as in the same-ledger flow.

If the data ledger's effective config does not specify
`f:policyClass`, cross-ledger policy enforcement loads no rules
from M (Allow/Deny depend on `f:defaultAllow`). Inline `opts.policy`
JSON-LD continues to merge against D, never against M.

## Resolution time and `f:atT`

| Case                       | Behavior |
|----------------------------|----------|
| No `f:atT`                 | On first unpinned reference to M in this request, read M's current head `t` and cache it in `ctx.resolved_ts[model_id]`. Subsequent unpinned references to the same M reuse it. |
| `f:atT N`                  | `resolved_t = N`. Per-resolve, not stored in `resolved_ts`. M time-travels to that point. |
| `f:atT N`, N pruned        | Fail closed (see [Failure variants](#failure-variants)). No fallback to nearest-available. |
| Mid-request M advancement  | Not reflected in the current request. The next request re-captures on its first reference. |

Capture is **lazy and per request**: a request that never needs M
never pays for M's head lookup. Once captured, `resolved_t` is
stable across every subsystem in that request — policy and shapes
can never disagree about which version of M they're enforcing. This
is what makes the "model edit propagates atomically to all governed
datasets" property hold within a single request boundary.

## Caching

Resolved artifacts are cached at the **API layer** (in
`fluree-db-api`), not in `fluree-db-binary-index::LeafletCache`. The
binary-index crate sits below `fluree-db-api`, `fluree-db-policy`,
and the cross-ledger module; making it depend upward on typed
governance-artifact representations would be a layering inversion.

For Phase 1a the implementation is a Moka cache bounded by entry
count, scoped to a `Fluree` instance. Single memory-pool unification
with `LeafletCache` is a follow-up that requires adding an
opaque-blob variant to the binary-index cache (the artifact is
serialized to bytes at the cache boundary), and is deferred until
the artifact representation stabilizes.

The key is `(canonical_model_ledger_id, graph_iri, resolved_t)`. New
commits to M produce new keys without explicit invalidation;
unreferenced entries age out under the cache's eviction policy.
There is no "watermark-on-write" channel.

The cache value is the term-neutral `ResolvedGraph` (IRIs, not Sids).
Per-data-ledger interning is not part of the cache key — the cache
is shareable across every data ledger that references the same
`(model, graph, t)`, which is what makes "model edit propagates
atomically to all governed datasets" cheap.

## Failure variants

Cross-ledger failures must be distinguishable for audit; they MUST
NOT collapse into a single generic import error.

```rust
pub enum CrossLedgerError {
    /// `f:ledger` names a ledger that does not exist or has been
    /// dropped on this instance.
    ModelLedgerMissing { ledger_id: String },

    /// `f:ledger` resolves but the named graph IRI has no entry in
    /// the model ledger's graph registry at `resolved_t`.
    GraphMissingAtT { ledger_id: String, graph_iri: String, resolved_t: i64 },

    /// `f:atT N` was requested but M no longer retains state at N
    /// (index pruning, history retention).
    TAtUnavailable { ledger_id: String, requested_t: i64, oldest_available_t: i64 },

    /// The selector targets `#config` or `#txn-meta` on the model
    /// ledger.
    ReservedGraphSelected { graph_iri: String },

    /// The resolver successfully read the graph but could not
    /// translate it to term-neutral form (missing IRI on a Sid that
    /// the model dictionary lost, malformed rule, etc.).
    TranslationFailed { ledger_id: String, graph_iri: String, detail: String },

    /// `f:trustPolicy` failed verification, or `f:rollbackGuard`
    /// would be violated. (Phase 4.)
    TrustCheckFailed { ledger_id: String, detail: String },

    /// `f:ledger` targets a ledger on a different instance.
    CrossInstanceUnsupported { ledger_id: String },

    /// Cycle detected through `(ledger, graph, resolved_t)` chain.
    CycleDetected { chain: Vec<(String, String, i64)> },
}
```

Every variant is fail-closed: the request fails. There is no silent
fallback to "no policy" or "no shapes."

## Trust model

A data ledger D's `#config` declaring `f:ledger <M>` is itself the
capability assertion. Writing to D's `#config` already requires
policy authority on D, so "whoever can write D's `#config` asserts
that M is a trusted governance source for D."

For v1, no consent is required from M. Phase 4 introduces
`f:trustPolicy` and `f:rollbackGuard` for ledgers that need
stronger guarantees (commit signer allowlist, hash pin, maximum
staleness window).

Cross-instance federation requires a different trust model (auth,
transport, signing) and is out of scope.

## Reserved-graph guard

The same-ledger version of this guard lives in
`ontology_imports::resolve_local_graph_source`: selectors resolving
to `g_id=1` (`#txn-meta`) or `g_id=2` (`#config`) are rejected.

The cross-ledger version applies the same check by IRI *before* any
storage round-trip on M. The motivation is doubled: `#txn-meta` on
M could leak commit metadata to D's request handler.

## Cycle detection

Two structures, distinct purposes:

- `ctx.active` is the **active resolution stack** — push the
  `(canonical_ledger_id, graph_iri, resolved_t)` tuple before
  recursing into a transitive import, pop on return. A tuple is a
  cycle only if it is encountered while *already on the stack*.
- `ctx.memo` is the **per-request completed map**. Once a tuple
  resolves successfully, it lands here. Subsequent references to
  the same tuple — from any subsystem in the same request — short-
  circuit on the memo, never enter `active`, and never trip cycle
  detection.

So if `policySource` and `shapesSource` both reference the same
`(ledger, graph, t)`, the second resolve is a memo hit, not a
cycle. Two different `atT` pins of the same `(ledger, graph)` are
not a cycle. Two different graphs on the same ledger are not a
cycle. Only re-entering an in-flight tuple is.

This generalizes the BFS+dedupe pattern the existing same-ledger
`owl:imports` resolver uses to a 3-tuple, with the
active-vs-completed distinction surfaced explicitly.

## Drop interaction

V1: if a data ledger D references model ledger M and M is dropped,
the next request against D that needs governance from M fails
closed with `ModelLedgerMissing`. There is no reverse-reference
index and no rejection of M's drop based on outstanding references.

This is the smallest contract that's safe; introducing reverse
indexes requires nameservice schema work and is deferred. Operators
who need stronger guarantees can publish a `f:trustPolicy` (Phase 4)
or coordinate drops at the application layer.

## Same-instance constraint (v1)

Both D and M must:

- Belong to the same nameservice instance.
- Live within the same storage namespace.

Same-instance failures surface as `CrossInstanceUnsupported` before
any storage round-trip.

## Phasing

| Phase | Scope | Status |
|-------|-------|--------|
| 0     | Same-ledger fail-closed across the five subsystems (policy, shapes, schema, constraints, rules). | Policy, shapes, schema, constraints: done. Rules: pending (deferred behind this design). |
| 1a    | `f:policySource` cross-ledger via `resolve_graph_ref`. Policy IR identity split lands as part of this. | After this doc. |
| 1b    | `f:schemaSource` + `f:ontologyImportMap` cross-ledger; transitive imports across ≥2 model ledgers. | After 1a. |
| 2     | `f:shapesSource`, `f:rulesSource`, `f:constraintsSource` cross-ledger via the same resolver. | After 1a/1b. |
| 3     | `f:atT` temporal pinning. | After 2. |
| 4     | `f:trustPolicy`, `f:rollbackGuard`. Separate RFE. | Out of scope here. |

## Test plan (per phase)

Acceptance tests live next to the subsystems they exercise:

- `it_policy_cross_ledger.rs` — D references policy in M; query
  against D enforces M's policies; inline `opts.policy` still
  merges; fail-closed when M is unreachable.
- `it_schema_cross_ledger.rs` — extension of
  `it_reasoning_imports.rs` across a ledger boundary; transitive
  imports across two model ledgers; cycle detection.
- `it_shapes_cross_ledger.rs` — tx against D validates against
  shapes in M.
- `it_at_t_pinning.rs` — `f:atT N` pins; commits after N don't
  affect the governed query.
- `it_fail_closed.rs` — every failure variant rejects the request.

Two cross-cutting tests are mandatory regardless of phase:

- **Distinct namespace codes.** M and D both interned the same
  class IRI under different `ns_code` values; the resolved artifact
  re-interns correctly against D and policy/shapes fire as
  expected. This is the canary for term-space translation.
- **Single-resolution-t.** Within one request, every subsystem
  receives the same `resolved_t` for M, even when admission and
  enforcement happen tens of milliseconds apart and M advanced in
  between.

Tests must drive through `Fluree::db()` (not
`GraphDb::from_ledger_state`) so the config-graph path is exercised.

## Out of scope

- **Cross-instance federation.** Different nameservices, transport,
  cross-org auth/signing. Separate RFE.
- **`f:trustPolicy` / `f:rollbackGuard` implementations.** Phase 4.
- **Auto-resolution by IRI namespace.** "Which model governs
  `schema:*`?" — application-layer concern.
- **Writing back to a model ledger from a governed ledger's
  request.** Read-only references only.
- **Reverse-reference indexes for safe drop.** V1 allows M to be
  dropped; D fails closed on next request.

## Error type and HTTP mapping

`CrossLedgerError` surfaces through the API crate as a dedicated
variant:

```rust
pub enum ApiError {
    // ...
    /// Cross-ledger governance resolution failed. The wrapped
    /// variant carries the specific failure (missing ledger, graph
    /// missing at t, retention pruned, etc.) for audit and
    /// operator diagnostics.
    #[error("Cross-ledger error: {0}")]
    CrossLedger(#[from] CrossLedgerError),
}
```

It is **not** collapsed into `ApiError::Http { status: 502, .. }` —
preserving the structured variant is what makes "the model ledger
this data ledger depends on is broken" distinguishable from "your
data ledger is broken" in logs and audit trails.

HTTP status mapping: **502 Bad Gateway** is the default. A model
ledger dependency that cannot be resolved or used is conceptually
an upstream-dependency failure, not an internal panic. 424 Failed
Dependency is semantically closer but less commonly handled by
client tooling; 502 is the pragmatic choice. The server layer reads
the variant for the response body so callers can branch on the
specific failure even when the status is generic.

