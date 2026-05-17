# Cross-ledger model enforcement

Fluree's `f:GraphRef` shape lets a data ledger reference a graph
containing policy / shapes / schema / rules / constraints. Without
cross-ledger references, every data ledger has to carry its own
copy of those governance artifacts. This document explains the
contracts that make `f:GraphRef` work cross-ledger so a single
**model ledger** — holding the ontology, SHACL shapes, policy rule
set, datalog rules, and uniqueness constraints — can be referenced
by many **data ledgers** it governs.

For the user-facing how-to (TriG examples, configuration steps),
see [Cross-ledger policy](../security/cross-ledger-policy.md). This
document explains the design decisions behind that mechanism: why
the resolver returns term-neutral artifacts, why the cache is
keyed the way it is, what the identity contract is, and how
failures are surfaced.

Topics:

- Glossary and what "cross-ledger" means at the contract level.
- The resolver contract: a single `resolve_graph_ref` helper shared
  by every subsystem, returning term-neutral artifacts.
- Term-space translation — why model-ledger Sids/GraphIds/t values
  cannot leak into data-ledger execution.
- Resolution time, caching, and failure variants.
- Policy IR identity split — definitional vs contextual term
  binding.
- Trust model, reserved-graph guards, cycle detection, drop
  interaction.
- Scope: what the resolver covers today and what's deferred.

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

Every subsystem that needs to read a cross-ledger graph goes
through the same helper:

```rust
pub async fn resolve_graph_ref(
    graph_ref: &GraphSourceRef,
    kind: ArtifactKind,
    ctx: &mut ResolveCtx<'_>,
) -> Result<Arc<ResolvedGraph>, CrossLedgerError>;
```

where `ResolveCtx` carries everything the resolver needs without
each subsystem rebuilding the surrounding state:

```rust
pub struct ResolveCtx<'a> {
    /// Canonical data-ledger id D.
    pub data_ledger_id: &'a str,
    /// The Fluree instance hosting D and (per the same-instance
    /// constraint) the referenced model ledger.
    pub fluree: &'a Fluree,
    /// Governance-context capture: lazily-populated map from
    /// canonical model ledger id → `resolved_t` for this request.
    /// The first reference to a given model populates the entry;
    /// every subsequent reference in the same request reuses it.
    pub resolved_ts: HashMap<String, i64>,
    /// Active resolution stack — the chain of `(kind, ledger,
    /// graph, resolved_t)` tuples currently being resolved. Used
    /// only for cycle detection; an entry is pushed before
    /// recursion and popped after. `ArtifactKind` is part of the
    /// key so a `PolicyRules` resolve of `(M, graph, t)` doesn't
    /// make a `Shapes` resolve of the same `(M, graph, t)` look
    /// like a cycle (or vice versa).
    pub active: Vec<(ArtifactKind, String, String, i64)>,
    /// Per-request memo of fully-resolved artifacts. Hits short-
    /// circuit without storage round-trip and without entering the
    /// cycle stack. Same `ArtifactKind`-extended key as `active`
    /// so different artifact kinds can't return each other's
    /// entries.
    pub memo: HashMap<(ArtifactKind, String, String, i64), Arc<ResolvedGraph>>,
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
   `(ArtifactKind, canonical_model_ledger_id, graph_iri, resolved_t)`.

A `ResolvedGraph` is term-neutral and t-fixed:

```rust
pub struct ResolvedGraph {
    pub model_ledger_id: String,       // canonical
    pub graph_iri: String,
    pub resolved_t: i64,
    pub artifact: GovernanceArtifact,  // tagged union per subsystem
}
```

`GovernanceArtifact` is a tagged union with one variant per
subsystem. Only `PolicyRules` is implemented today; the rest are
named in [Scope](#scope) and land as new variants when their
materializers do.

```rust
pub enum GovernanceArtifact {
    PolicyRules(PolicyArtifactWire),   // IRI-form policy rules
}
```

Each variant is **term-neutral**: every subject, predicate, object,
class, and datatype reference is stored as an IRI (or canonical
literal), never as a model-ledger Sid or GraphId. The data-ledger
consumer re-interns IRIs against its own dictionary at use time.

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
`f:policyClass`, the filter defaults to `{f:AccessPolicy}` — the
canonical policy class IRI. Cross-ledger enforcement then pulls
in every rule in M's policy graph that's typed as
`f:AccessPolicy` directly. Custom-typed rules (e.g.,
`ex:OrgPolicy`) require an explicit `f:policyClass` entry in D's
config to be enforced. This default is intentionally a small
allowlist rather than "every structural rule from M": operators
opt into custom-class enforcement by naming it, never by
omission. Inline `opts.policy` JSON-LD continues to merge against
D, never against M.

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

The implementation is a Moka TinyLFU cache bounded by entry count
(see `cross_ledger::GovernanceCache`), scoped to a `Fluree`
instance. Single memory-pool unification with `LeafletCache` would
require adding an opaque-blob variant to the binary-index cache
(serializing the artifact to bytes at the cache boundary) and is
deliberately deferred until the artifact representation
stabilizes — keeping the two caches separate while artifact shapes
are still evolving prevents premature coupling.

The key is `(ArtifactKind, canonical_model_ledger_id, graph_iri,
resolved_t)`. `ArtifactKind` is part of the key so a memoized
`PolicyRules` entry never short-circuits a `Shapes` lookup of the
same `(M, graph, t)`. New commits to M produce new keys without
explicit invalidation; unreferenced entries age out under the
cache's eviction policy. There is no "watermark-on-write" channel.

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

    /// `f:trustPolicy` verification failed (reserved for when
    /// trust-policy enforcement is implemented; see Scope).
    TrustCheckFailed { ledger_id: String, detail: String },

    /// `f:atT`, `f:trustPolicy`, or `f:rollbackGuard` was set on
    /// a `GraphSourceRef`. Those fields are parsed by the config
    /// layer but their semantics are not yet implemented (see
    /// Scope); the request fails closed rather than silently
    /// ignoring the field.
    UnsupportedFeature { feature: &'static str, phase: &'static str, ledger_id: String },

    /// `f:ledger` targets a ledger on a different instance.
    CrossInstanceUnsupported { ledger_id: String },

    /// Cycle detected through the `(kind, ledger, graph, resolved_t)`
    /// chain.
    CycleDetected {
        chain: Vec<(ArtifactKind, String, String, i64)>,
    },
}
```

Every variant is fail-closed: the request fails. There is no silent
fallback to "no policy" or "no shapes."

## Trust model

A data ledger D's `#config` declaring `f:ledger <M>` is itself the
capability assertion. Writing to D's `#config` already requires
policy authority on D, so "whoever can write D's `#config` asserts
that M is a trusted governance source for D" is the binding
decision; no separate consent is required from M.

Cross-instance federation would require a different trust model
(auth, transport, signing for ledgers hosted on different
nameservices) and is out of scope. See [Scope](#scope).

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

If a data ledger D references model ledger M and M is dropped,
the next request against D that needs governance from M fails
closed with `ModelLedgerMissing`. There is no reverse-reference
index and no rejection of M's drop based on outstanding
references.

This is the smallest contract that's safe — operators get a clear
failure on the next governed request rather than a silent shift in
enforcement. Introducing reverse indexes would require nameservice
schema work and is out of scope (see [Scope](#scope)). Operators
who need stronger guarantees coordinate drops at the application
layer for now.

## Same-instance constraint

Both D and M must:

- Belong to the same nameservice instance.
- Live within the same storage namespace.

A reference that targets a ledger on a different instance surfaces
as `CrossInstanceUnsupported` before any storage round-trip. This
boundary is enforced implicitly by the nameservice lookup — a
ledger not present in this instance's nameservice can't be
canonicalized — and the variant exists so a future cross-instance
mode can be added without rewriting the failure taxonomy.

## Scope

### Implemented

- `f:policySource` cross-ledger via `resolve_graph_ref`. The
  policy IR carries definitional/contextual term references
  separately so the model ledger contributes rules while the
  data ledger contributes identity binding.
- `f:constraintsSource` cross-ledger via the same shared
  resolver. M's `f:enforceUnique true` annotations on
  properties apply to D's transactions; a tx that would
  create a duplicate value on one of those properties is
  rejected with `TransactError::UniqueConstraintViolation`
  even though the annotation never lives on D.
- `f:schemaSource` cross-ledger via the same shared resolver.
  The whitelisted schema axiom triples in M's ontology graph
  (rdfs:subClassOf, rdfs:subPropertyOf, rdfs:domain, rdfs:range,
  owl:equivalentClass / equivalentProperty / inverseOf / sameAs /
  imports, and rdf:type for the schema-class set) are projected
  into a `SchemaBundleFlakes` against D's snapshot and feed D's
  reasoner. Single-graph only today; transitive `owl:imports`
  recursion across multiple model ledgers is reserved.
- Per-request memo + per-instance governance cache, both keyed
  on `(ArtifactKind, canonical_model_ledger_id, graph_iri,
  resolved_t)`.
- Reserved-graph guard (rejects `#config` / `#txn-meta` on M
  before any storage round-trip).
- Reserved-feature rejection: `f:atT`, `f:trustPolicy`, and
  `f:rollbackGuard` are surfaced as `UnsupportedFeature` rather
  than silently ignored.
- Identity-mode + cross-ledger policy combination fails closed
  with a config error — the design's "M contributes rules, D
  contributes identity" boundary is enforced at the request
  surface.

### Reserved

The following subsystems share the resolver's contract but their
materializers aren't implemented. Each lands as a new
`GovernanceArtifact` variant + per-subsystem materializer:

- Transitive `owl:imports` recursion across multiple model
  ledgers (Phase 1b's full scope). The single-graph schema
  materialization above already projects M's `owl:imports`
  triples into the wire so a future reader can see what M
  declared — the recursion through `resolve_graph_ref`
  (which would exercise `ResolveCtx.active` for cycle detection
  across ledgers) lands separately.
- `f:ontologyImportMap` cross-ledger (mapping table entries
  whose `f:graphRef` targets another model ledger).
- `f:shapesSource` cross-ledger (SHACL shapes).
- `f:rulesSource` cross-ledger (datalog rules). The same-ledger
  routing for `f:rulesSource` is also still pending; the
  cross-ledger path will route through the shared resolver
  once same-ledger lands.
- `f:atT` temporal pinning (currently rejected as
  `UnsupportedFeature`).
- `f:trustPolicy` and `f:rollbackGuard` (rejected as
  `UnsupportedFeature` for the same reason).

### Out of scope

- **Cross-instance federation.** Different nameservices,
  transport, cross-org auth/signing.
- **Auto-resolution by IRI namespace.** "Which model governs
  `schema:*`?" — application-layer concern.
- **Writing back to a model ledger from a governed ledger's
  request.** Cross-ledger references are read-only.
- **Reverse-reference indexes for safe drop.** A drop on M
  surfaces on the next governed request against D, not at
  drop time.
- **Subclass entailment in policy_class filtering.** The
  filter is exact-IRI; D must name the same class IRI M
  declared. Mirrors same-ledger `load_policies_by_class`
  semantics.

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

