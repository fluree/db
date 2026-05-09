# Edge Annotations — Implementation Plan

Companion to `EDGE_ANNOTATIONS.md`. The design doc owns the *what* and
*why*; this plan owns the *how, in what order, and where in the
codebase*. The "Design Decisions (v1)" section of the source doc is the
frozen contract this plan implements.

## Milestone overview

Each milestone ships as one PR (or a small chain) and is independently
useful — a reviewer can merge without waiting for the next.

| ID  | Scope | Persistence | Approx |
|-----|-------|-------------|--------|
| M0  | Parser surface + IR stub | none — execution errors | ~3 days |
| M1a | Foundation + write side | durable via `f:reifies*` | ~1 wk |
| M1b | Read side + cascade + integration | (M1a + arena lookups) | ~1–2 wk |
| M2  | Binary arena + `IndexRoot` extension | survives indexing | 3–4 wk |
| M3  | Planner / costing | — | 1–2 wk |

**M1 was split into two slices during implementation.** M1a and M1b
are each independently mergeable. M1a is a complete write-side
feature (annotations persist, but queries don't yet read them); M1b
adds the read-side dispatch and visibility wiring that turns it into
a queryable feature.

The deferred list at the bottom of the source doc is **not** in v1.

---

## M0 — Parser surface + IR stub

**Goal:** lock the surface syntax. Inserts and queries containing
`@annotation` / `@reifies` parse cleanly and produce a stable AST and
IR. Execution returns a clear "not yet implemented" error. No storage
or executor changes.

### Files / changes

- `fluree-db-query/src/parse/ast.rs`
  - New variants on `UnresolvedPattern`:
    ```rust
    EdgeAnnotation {
        edge: UnresolvedTriplePattern,
        annotation: UnresolvedTerm,   // anonymous, named, or var
        body: Vec<UnresolvedPattern>, // patterns about the annotation subject
    }
    AnnotationTarget {
        annotation: UnresolvedTerm,
        edge: UnresolvedTriplePattern,
        body: Vec<UnresolvedPattern>,
    }
    ```
  - Reuse `UnresolvedTriplePattern` (already at line 140 today) for the
    edge — no new triple-shape types.

- `fluree-db-query/src/parse/node_map.rs`
  - Recognize `@annotation` and `@edge` (alias) on object positions
    where the value is `@id`-valued. Lower into `EdgeAnnotation`.
  - Recognize `@reifies` per the parsing rules below. Lower into
    `AnnotationTarget`.
  - Reject the deferred shapes from the decisions section (literal
    object annotations, list-occurrence annotations, reifiers for
    unasserted triples, multi-triple reifiers) with explicit error
    messages naming the keyword and the deferred decision.

#### `@reifies` parsing rules

The enclosing node-map is the annotation subject; `@reifies` names the
edge it reifies; the rest of the node's properties are facts about the
annotation subject:

1. The enclosing node's `@id` (if present) is the annotation subject.
   If absent: mint a fresh blank node in **insert mode** or generate a
   fresh synthetic variable (e.g. `?__ann_<n>`) in **query mode**.
2. The `@reifies` value is itself a node-map identifying the base
   triple's `(subject, predicate, object)`. The plan reuses
   `UnresolvedTriplePattern` for this so the existing path logic and
   variable handling apply.
3. Every other (non-`@`-keyword) property on the enclosing node is a
   fact about the annotation subject — emitted as an annotation flake
   on insert, or as a body `Pattern` on query.
4. Exactly one `@reifies` per enclosing node-map. Multiple reifications
   per annotation subject are deferred per the decisions section.

Example query input:
```json
{
  "ex:role": "Engineer",
  "@reifies": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
}
```
lowers to:
```text
AnnotationTarget {
  annotation: <synthetic ?__ann_0>,
  edge: TriplePattern(?person, ex:worksFor, ?org),
  body: [Triple(?__ann_0, ex:role, "Engineer")],
}
```

- `fluree-db-query/src/ir/pattern.rs`
  - Mirror IR variants:
    ```rust
    EdgeAnnotation {
        edge: TriplePattern,
        annotation: Ref,              // resolved Var or Sid
        body: Vec<Pattern>,
    }
    AnnotationTarget { annotation: Ref, edge: TriplePattern, body: Vec<Pattern> }
    ```
  - Implement `referenced_vars()` / `produced_vars()` so the planner
    treats them like any other binding pattern once they're real.

#### Pattern variant inventory — every exhaustive match site

Adding two new `Pattern` variants requires touching every `match` over
`Pattern` in the codebase. Before opening PR #1, regenerate the
inventory with:

```bash
rg -n 'match\s+.*Pattern\b|fn\s+\w+.*Pattern\s*[,)]' \
   --type rust /Users/bplatz/fluree/db
```

Known sites at the time of writing (verified against the
`refactor/streamline-query` tree):

- `fluree-db-query/src/ir/pattern.rs` — variant defs, `referenced_vars`, `produced_vars`, `Display`/`Debug` impls.
- `fluree-db-query/src/parse/ast.rs` — `UnresolvedPattern` and any helpers that walk it.
- `fluree-db-query/src/parse/lower.rs` — pattern lowering (already in this milestone).
- `fluree-db-query/src/parse/mod.rs` — top-level parse/lower glue.
- `fluree-db-query/src/planner.rs` — top-level planner; classifies patterns into operator types.
- `fluree-db-query/src/rewrite.rs` — generic rewriter passes.
- `fluree-db-query/src/rewrite_owl_ql.rs` — OWL-QL pattern rewriting.
- `fluree-db-query/src/geo_rewrite.rs` — geo-pattern rewriting.
- `fluree-db-query/src/execute/operator_tree.rs` — operator-tree assembly / fast-path detection.
- `fluree-db-query/src/execute/where_plan.rs` — WHERE plan composition.
- `fluree-db-query/src/execute/dependency.rs` — dependency analysis and required-var tracing.
- `fluree-db-query/src/execute/runner.rs` — runner-level rewrites.
- `fluree-db-query/src/execute/pushdown.rs` — pattern pushdown into scans.
- `fluree-db-query/src/execute/rewrite_glue.rs` — rewrite-output → executor wiring.
- `fluree-db-query/src/explain.rs` — `/explain` rendering for each variant.
- `fluree-db-transact/` — if WHERE patterns reach the transactor (UPDATE/DELETE WHERE).
- Any test helpers that match `Pattern` exhaustively.

**No `unimplemented!` / `panic!` / `todo!` in reachable paths.** The
M0 surface still has to behave reasonably for users who write
syntactically valid queries — even if those queries can't execute
yet. Use one of three patterns at each match site, in order of
preference:

1. **Real impl** for analyze-only passes that have an obvious
   semantic answer:
   - `referenced_vars()` / `produced_vars()` walk the body.
   - `Display` / `Debug` render the new variants.
   - Dependency analysis recurses into `body`.
   - Rewrite / pushdown / OWL-QL passes pass through (do not
     transform, do recurse into `body`).
   - Explain prints a single line per variant.
2. **`Err(QueryError::UnsupportedFeature("edge annotations: storage
   not yet implemented"))`** at the operator-tree assembly site —
   this is the only path that needs to fail, because building an
   executor requires storage support.
3. **Conservative pass-through** for any other match (the variant
   contributes nothing to the pass and the surrounding work
   continues unaffected).

End of M0 the compile must succeed with no `match` arm missing and
no `unimplemented!` reachable from any user-input path.

- `fluree-db-query/src/parse/lower.rs`
  - Lower the AST variants into the IR variants — variable resolution,
    IRI encoding, alias minting.
  - Add a `lower_edge_pattern_with_body` helper because both variants
    share a body-of-patterns structure.

- `fluree-db-query/src/execute/operator_tree.rs`
  - When the planner encounters either IR variant, return
    `Err(UnsupportedFeature("edge annotations: storage not yet
    implemented"))`. M1 replaces this with real operators.

- Transactor parse path
  (`fluree-db-transact/src/parse/` and `import.rs`)
  - Parse `@annotation` on insert. For M0, error at the staging layer
    with the same `UnsupportedFeature` shape so the surface is real
    end-to-end.

### Tests

- `fluree-db-query/src/parse/`: unit tests for each canonical shape in
  the source doc's "User-Facing Syntax" section.
- `fluree-db-api/tests/it_edge_annotations_parse.rs` (new): end-to-end
  parse-only coverage — parse succeeds, execute returns the marker
  error.

### Definition of done

- [ ] Every example in the source doc's syntax section parses without
      panic.
- [ ] Deferred shapes produce the documented error message.
- [ ] Every `match` over `Pattern` compiles after the new variants
      land — no missing arms.
- [ ] No behavior change for queries that don't use `@annotation` /
      `@reifies` (verified by full test suite).

---

## M1 — Novelty-only end-to-end (with durable encoding)

**Implementation status:** split into M1a (write side, **DONE**) and
M1b (read side, **TODO**) during execution. The two slices share the
same goal and contracts — they just shipped as separate PR-sized
units. The original goal statement and design contracts below apply
to the combined M1.

### Status snapshot (as of latest commit)

**M1a — DONE** (`feat(M1)` commits):
- ✅ `f:reifies*` predicates added to `fluree-vocab`
  (`db::REIFIES_*`, `reifies_iris::*`).
- ✅ `is_reserved_reifies_predicate(sid)` and per-predicate test
  helpers in `fluree-db-core::namespaces`.
- ✅ `EdgeKey` value type in `fluree-db-core::edge` with
  `to_reifies_facts` / `from_reifies_facts` round-trip.
- ✅ `AttachmentNovelty` overlay in `fluree-db-novelty` with
  forward / reverse maps, `(t, op)` rows, and `has_annotations`
  gate.
- ✅ Observer hook in `Novelty::apply_commit` and
  `Novelty::bulk_apply_commits` populating the overlay from
  post-dedup `f:reifies*` flakes.
- ✅ Pre-expansion JSON-LD lowering for `@annotation` / `@edge` in
  `fluree-db-transact::parse::edge_annotations`.
- ✅ Strict deferred-shape rejection: literal-valued annotations,
  multi-triple reifiers, annotation-of-annotation, user-authored
  `f:reifies*` IRIs, `@reifies` on inserts.
- ✅ M0 keyword scan replaced by the M1 lowering at
  `parse_transaction` entry.
- ✅ M0 integration tests updated to M1 semantics: inserts succeed,
  queries still error at the operator layer.
- ✅ Test counts: 657 core + 35 novelty + 186 transact + 5 api
  integration + 12 query parser tests pass.

**M1b — partially landed (read-side dispatch + firewall in;
correctness items remaining):**

- ✅ **Planner expansion** (`feat(M1b): planner expansion`): IR-level
  flattening of `Pattern::EdgeAnnotation` /
  `Pattern::AnnotationTarget` into base edge + three `f:reifies*`
  lookup triples + body. Replaces the M0 `UnsupportedFeature` stub
  in `where_plan.rs`. The standard scan/join/dedup machinery handles
  the rest. The base-edge triple gives the reverse-direction
  visibility check for free.
- ✅ **Read-side firewall** (`feat(M1b): read-side firewall`):
  user queries naming `f:reifies*` IRIs directly (full or compact)
  are rejected at parse time with a system-controlled message.
- ✅ **Round-trip integration tests:**
  `query_inline_annotation_returns_matching_role` and
  `query_reifies_form_runs_with_visibility_check` — both demonstrate
  insert-then-query end-to-end on memory storage.

- ⏳ **System-fact filter at the scan layer.** Variable-predicate
  scans (`?p` matching all predicates) currently expose `f:reifies*`
  in their results. Wildcard `select: "*"` projects them as ordinary
  properties on annotation subjects. Both need filters with an
  `opts.includeSystemFacts: true` escape and a history-range
  carve-out. The parser-level firewall blocks *direct named
  mention*; this is the broader leakage path.
- ⏳ **Graph-bound expansion in multi-graph queries.** The IR-level
  expansion in `expand_edge_annotation_patterns` correctly handles
  single-graph queries and `Pattern::Graph`-wrapped patterns (the
  base edge and `f:reifies*` lookups all scope to the same graph
  via the standard scan filter). It does **not** explicitly bind
  the graph variable across the lookup, so a multi-graph dataset
  with the same `(s,p,o)` in multiple graphs and SPARQL
  `FROM` / `FROM NAMED` semantics can join an annotation from
  graph X with a base edge from graph Y. The proper fix needs
  either a custom operator that carries graph identity through the
  lookup or an `(?ann, f:reifiesGraph, ?graph)` triple bound to
  the base-edge match. Tracked alongside the M2 binary-arena work
  since the custom operator becomes the right vehicle then.
- ⏳ **Per-language disambiguation.** Same architectural shape:
  `f:reifiesLang` is emitted by the write side but not constrained
  in the read-side expansion, so cross-language misjoin is possible
  when the same string is asserted with multiple language tags.
  Same fix path as the graph-bound expansion.
- ⏳ **Wildcard hide of anonymous annotation SIDs.** Explicit-IRI
  annotation subjects stay visible; anonymous (blank-node) ones are
  filtered out of `select: "*"` per the design decisions.
- ⏳ **Cascade rules** for the three retract shapes
  (plain-edge / occurrence-by-selector / by-annotation-id) plus the
  RDF-mode default vs. `lpgEdgeLifecycle: true` opt-in.
- ⏳ **JSON-LD subject-expansion output:** emit `@annotation` blocks
  when materializing an annotated edge through subject expansion.
- ⏳ **Broader `it_edge_annotations.rs` integration tests:** parallel
  annotations on one edge, multiplicity contract, cascade behavior,
  lifecycle (RDF default vs. LPG opt-in), restart-from-commits,
  policy visibility independence.

- ❌ **Custom `EdgeAnnotationOp` / `AnnotationTargetOp` operators —
  not pursued.** The IR-level expansion approach achieves the same
  result through the existing scan / join machinery, including the
  visibility check (base edge triple is policy-filtered by the
  standard scan). The custom-operator approach was the original
  M1b plan; we kept the IR variants and added a flattening pass
  instead, which avoids duplicating planner / dedup / policy logic.
  Custom operators may still arrive in M3 if cost-based direction
  selection (edge-first vs. annotation-first) requires them.

### Original goal (still applies to combined M1)

**Goal:** ledgers fully support edge annotations. Inserts, queries
(both directions), and the three retract shapes work. Attachments
**survive commit and replay** by riding the existing flake/commit
pipeline. The arena is still deferred to M2; until then, attachment
state is reconstructed in-memory at snapshot time from the durable
fact encoding.

### Durable attachment encoding (mandatory before any persistence)

Attachments are primary truth — losing them on restart would be data
loss. Two viable shapes were considered:

- **(a) Commit-format extension:** add an `AttachmentRecord` variant to
  the commit envelope alongside flake records. Replay produces both
  novelty flakes and novelty attachment rows.
- **(b) System-fact encoding:** express each attachment as fixed-shape
  facts about the annotation subject, riding the existing
  flake/commit/replay pipeline.

**Decision: (b).** The annotation arena (M2) becomes a derived
secondary index, like the dictionary arenas, with system facts as the
durable source of truth. This reuses every piece of existing
infrastructure — commits, history, policy filtering, snapshot
visibility, and replay — and keeps M1's surface area small.

The encoding adds the following Fluree-namespaced predicates (added
to `fluree-vocab`):

```text
_:ann1 f:reifiesGraph     ?g          # OPTIONAL: present iff edge is in a named graph
_:ann1 f:reifiesSubject   ?s          # required, IRI ref
_:ann1 f:reifiesPredicate ?p          # required, IRI ref
_:ann1 f:reifiesObject    ?o          # required, any FlakeValue (refs, literals)
_:ann1 f:reifiesDatatype  ?dt         # required, IRI ref
_:ann1 f:reifiesLang      "fr"        # optional, omitted when none
_:ann1 f:reifiesListIndex 3           # optional, v1 always omitted
```

**`f:reifiesGraph` for the default graph.** Omitted entirely. The
absence of an `f:reifiesGraph` flake on an annotation subject means
"default graph"; this avoids inventing a sentinel IRI and matches the
convention already used elsewhere in the stack for `g_id: None`. The
bundle validator therefore enforces "*at most one* `f:reifiesGraph`"
rather than "exactly one" — present-and-named for a named-graph edge,
absent for a default-graph edge.

##### Reserved-predicate invariants

These predicates are **system-controlled**. User transactions cannot
assert or retract them directly; only the staging path emits
`f:reifies*` flakes, and only as part of a complete bundle. The
transactor enforces this in three layers:

1. **Surface rejection at every write entry point.** Any user-
   authored mention of an `f:reifies*` predicate is rejected with a
   clear error pointing the user at `@annotation` / `@reifies`. The
   filter is implemented as a single SID-set check
   (`fluree_vocab::is_reserved_reifies_predicate(sid)`) called from
   each write surface so the rule cannot drift between paths:
   - `fluree-db-transact/src/parse/` — JSON-LD insert / update /
     upsert / WHERE+DELETE+INSERT.
   - `fluree-db-transact/src/lower_sparql_update.rs` — SPARQL
     `INSERT DATA` / `DELETE DATA` / `INSERT WHERE` / `DELETE WHERE`.
   - `fluree-graph-turtle` ingest path (Turtle / TriG / N-Quads).
   - `fluree-db-transact/src/import.rs` and `import_sink.rs` — bulk
     import sinks.
   - `fluree-db-transact/src/raw_txn_upload.rs` — raw transaction
     upload.
   - Any future write surface added after v1 must include the same
     check; enforce by exposing a single helper that returns the
     reserved-predicate set, with a unit test that compares it to
     the on-the-wire predicate enumeration in `fluree-vocab`.

   **Defense in depth at the flake sink.** The lowest write layer
   (`fluree-db-transact/src/flake_sink.rs`) also rejects any
   `f:reifies*` flake whose origin tag is not the internal
   annotation lowering. Even if a future write path forgets the
   surface check, the sink still refuses the flake with a panic-free
   error.
2. **Staging-bundle atomicity.** When `@annotation` / `@reifies`
   lowering emits attachment facts, it always writes the **complete
   required bundle**:
   - `f:reifiesSubject`, `f:reifiesPredicate`, `f:reifiesObject`,
     `f:reifiesDatatype` — exactly one assertion each.
   - `f:reifiesGraph` — exactly one when the edge is in a named
     graph; **omitted** for the default graph (see encoding section).
   - `f:reifiesLang` and `f:reifiesListIndex` only when needed.
   Retraction emits the inverse bundle. Mid-bundle failures abort the
   whole transaction.
3. **Replay validation.** Warmup and arena-build paths
   (`fluree-db-novelty` observer + indexer) validate every observed
   bundle:
   - Reject (skip + telemetry counter) any `ann_sid` that has a
     partial bundle — e.g. `f:reifiesSubject` without
     `f:reifiesPredicate`. The annotation is treated as if it didn't
     exist; the metadata facts about the annotation subject are still
     visible as ordinary RDF.
   - Reject duplicate-pointer bundles where the same `ann_sid` has
     two different `(s, p, o)` targets simultaneously asserted — see
     the M2 read API note below; v1 enforces single-current-target
     at stage time.
   The same validator runs at index build time, so the arena never
   contains malformed rows.

##### Query visibility for `f:reifies*` predicates

`f:reifies*` predicates are **system facts, not user data.** They are
filtered out of every default user-facing query path:

- **Variable-predicate scans** that bind a `?p` variable do not
  return any `f:reifies*` predicate. The scan-layer filter is keyed
  on the predicate-SID set in `fluree-vocab` so the cost is a single
  SID membership check per row.
- **Direct named-predicate queries** that explicitly mention an
  `f:reifies*` predicate (e.g.
  `{ "@id": "?ann", "f:reifiesSubject": "?s" }`) are subject to the
  same filter — the predicate SID is in the reserved set, so the scan
  returns zero rows. Callers that genuinely need to see system facts
  must set `opts.includeSystemFacts: true` (below).
- **Wildcard `select: "*"`** — same filter, plus the existing
  hide-anonymous-annotation-SIDs rule.
- **JSON-LD subject expansion** never recurses through `f:reifies*`
  edges.
- **CONSTRUCT and graph-export** emit annotation metadata in the
  *surface-appropriate annotation form* for the target serialization,
  never the underlying `f:reifies*` triples. JSON-LD output uses
  `@annotation` / `@reifies`. Turtle/TriG, N-Quads, and SPARQL
  CONSTRUCT output need a separate decision (Turtle-star / RDF 1.2
  reifier syntax are the candidates) — this is **out of v1 scope**;
  M1 implements the JSON-LD path only and emits a clear
  `UnsupportedFeature` error if a non-JSON-LD target asks to project
  annotation metadata. Tracked as a follow-up in the deferred list.

Two opt-in escape hatches keep these facts queryable when callers
genuinely need them:

- A query option `opts.includeSystemFacts: true` disables the
  predicate filter for that one query (debug/inspection workflows).
  Applies to both variable-predicate and named-predicate scans.
- **History-range queries** (queries with explicit `"from": "...@t:N"`
  and `"to": "...@t:M"` time bounds) always include `f:reifies*`
  events, since attachment lifecycle is part of the ledger's history.
  Same scope rule as ordinary flake history.

Other properties of the encoding:

- **Atomic at the commit boundary** — the required facts plus the
  metadata facts assert/retract together, gated by transactional
  consistency rules.
- **Replayable** — a fresh process rehydrating a snapshot from
  commits reconstructs `AttachmentNovelty` by scanning these facts
  during warmup. The `has_annotations` flag is `true` iff at least
  one `f:reifiesSubject` fact exists in the snapshot.
- **History-clean** — `f:reifies*` facts share the standard
  `(t, op)` history, so attachment events appear naturally in
  history queries.
- **Policy-clean** — annotation visibility flows through the
  existing policy filter without a new primitive. (See "Cross-cutting
  concerns" for the visibility independence test.)

### Files / changes

- `fluree-vocab/src/`
  - Add the `f:reifies*` predicate IRIs (and any owl:Class declarations
    needed for SHACL constraints if the vocab tracks them).

- `fluree-db-core/src/edge.rs` (new)
  - `EdgeKey` matching the shape in the decisions section. Total order
    so it can key a `BTreeMap`. Stable serde for diagnostic output.
  - Helpers: `EdgeKey::from_flake(&Flake)`,
    `EdgeKey::matches(&Flake) -> bool`.
  - `EdgeKey::to_reifies_facts(ann_sid, t) -> Vec<Flake>` and the
    inverse `EdgeKey::from_reifies_facts(facts) -> Result<EdgeKey>` for
    the durable encoding.

- `fluree-db-novelty/src/`
  - New module `attachments.rs`:
    ```rust
    pub struct AttachmentNovelty {
        pub forward: BTreeMap<EdgeKey, Vec<AttachmentRow>>, // multimap
        pub reverse: BTreeMap<Sid, Vec<AttachmentRow>>,
        pub has_annotations: bool, // gate flag
    }
    pub struct AttachmentRow { pub other: AttachmentEnd, pub t: i64, pub op: bool }
    ```
  - **`AttachmentNovelty` is derived state**, not primary truth: it is
    populated by observing `f:reifies*` flakes flowing through the
    novelty pipeline. The same observer runs at warmup against any
    facts already in the snapshot from prior commits.
  - Snapshot-time visibility helpers reusing the same `(t, op)` rules
    flake novelty already uses — no new visibility logic.

- `fluree-db-transact/`
  - `stage.rs` / `commit.rs`: lower `@annotation` blocks during
    staging. For each annotated edge:
    1. Assert base flake (existing path).
    2. Mint or resolve `ann_sid` (anonymous → fresh Sid via existing
       blank-node minter; explicit IRI → encode through nameservice).
    3. Emit `f:reifies*` flakes for the attachment (durable encoding).
    4. Emit annotation flakes for the metadata properties.
    5. Update `AttachmentNovelty` from the same observer hook so
       reads see the attachment immediately.
  - `flake_sink.rs`: extend so retraction emit paths cascade per the
    rules in the decisions section.
    - Plain edge retract: enumerate current attachments via
      `AttachmentNovelty.forward.get(&edge_key)`, retract `f:reifies*`
      flakes + owned annotation flakes (anonymous always; explicit
      only in LPG mode). Then retract base.
    - Occurrence retract by selector: filter attachments, retract
      matching ones; if last and LPG mode, retract base.
    - Delete-by-annotation-id: single attachment's `f:reifies*` +
      owned facts.
  - Transaction option `lpgEdgeLifecycle: bool` parsed in
    `fluree-db-transact/src/parse/options.rs` (or wherever opts live)
    and threaded into `flake_sink`.

- `fluree-db-query/src/execute/`
  - New operator `EdgeAnnotationOp` reading `AttachmentNovelty.forward`
    by edge key.
  - New operator `AnnotationTargetOp` reading
    `AttachmentNovelty.reverse` by annotation SID.
  - Both emit one row per `(edge_key, ann_sid)` pair currently asserted
    under the snapshot's `(t, op)` rules. They feed downstream operators
    that match `body` patterns about the annotation subject.
  - Wire the planner to map `Pattern::EdgeAnnotation` /
    `Pattern::AnnotationTarget` to these operators.

  ##### Base-edge visibility — required for both operators

  Both operators **must verify the base edge is currently asserted
  and policy-visible to the calling user before emitting a row.**
  Otherwise a user with read access to annotation metadata but not
  the underlying edge could enumerate hidden `(s, p, o)` tuples by
  observing which annotations have a target — a confused-deputy
  leak.

  - `EdgeAnnotationOp` is naturally gated: its input is the matched
    base edge, which already passed the standard scan-time policy
    filter. The operator just confirms attachment.
  - `AnnotationTargetOp` (the reverse direction) is the dangerous
    one. After looking up `EdgeKey` via the reverse arena/novelty,
    it must:
    1. Probe the regular fact indexes for the corresponding
       `(g, s, p, o, dt, ...)` flake under the same snapshot rules
       used by ordinary `Pattern::Triple` execution.
    2. Confirm the flake is **currently asserted** (latest `op` is
       `+`).
    3. Confirm the flake passes the **same policy filter** that a
       direct `Triple(?s, p, ?o)` scan would apply.

    Only rows whose base edge survives both checks are emitted. Edges
    that fail either check are skipped silently (no telemetry leak —
    a counter is fine, but the per-row outcome must not be
    user-observable, since that itself would leak existence).
  - The cost is one extra fact-index probe per matched annotation.
    M3 may push the visibility check into the reverse-arena scan via
    the same dictionary the policy filter already uses.

  Test target (M1):
  - `policy_hides_base_edge_blocks_annotation_rooted_query` — user
    can read `?ann ex:role "Engineer"` but policy hides
    `(?person, ex:worksFor, ?org)`; query starting from the
    annotation must return zero rows, not leak the hidden edge.
  - `retracted_base_edge_drops_annotation_rooted_row` — base edge
    retracted in current snapshot but attachment still present in
    history; current-snapshot annotation-rooted query returns zero
    rows.

- `fluree-graph-json-ld/`
  - Insert/query parsers already see the new keys via M0; ensure
    formatter (output side) emits `@annotation` blocks correctly when
    a query selects through `EdgeAnnotation` — i.e. when subject
    expansion is asked for the edge object, nest annotation properties
    under `@annotation`.

- Wildcard / `select: "*"` projection
  - In the projection-resolver path
    (`fluree-db-query/src/parse/lower.rs` wildcard branch), filter
    anonymous annotation SIDs out of the wildcard variable set per the
    visibility decision. Explicit-IRI annotation SIDs remain visible.

### Tests

- `fluree-db-api/tests/it_edge_annotations.rs` (new):
  - `insert_anonymous_annotation_round_trips` — single annotation,
    inline query reads it back.
  - `insert_two_parallel_annotations_returns_two_rows` — Cypher
    fidelity: two parallel `(a, worksFor, b)` edges with distinct
    metadata, inline query returns 2 rows.
  - `bare_triple_does_not_multiply_by_occurrence` — multiplicity
    contract: `Triple(?s, p, ?o)` returns one row even though two
    parallel annotations exist.
  - `select_distinct_unchanged_with_annotations` — `selectDistinct`
    over (s, p, o) ignores parallel occurrences.
  - `annotation_rooted_query_via_reifies` — `@reifies` finds the edge
    from metadata.
  - `delete_base_edge_cascades_anonymous_annotations` — anonymous
    metadata is gone; explicit-IRI metadata is preserved (RDF mode).
  - `delete_base_edge_lpg_mode_cascades_explicit_metadata` — opt-in
    flag flips behavior.
  - `delete_by_annotation_id_targets_one_occurrence` — leaves siblings
    intact.
  - `delete_by_selector_filters_matching_occurrences` — partial
    metadata match.
  - `empty_annotation_block_is_noop_in_rdf_mode` — no attachment row
    minted.
  - `empty_annotation_block_mints_subject_in_lpg_mode` — opt-in
    behavior.
  - `wildcard_select_hides_anonymous_annotation_sids` — visibility
    rule.
  - `wildcard_select_shows_explicit_annotation_iris` — counterpart.
  - `deferred_shapes_error_clearly` — literal-object annotation,
    list-occurrence annotation, multi-triple reifier all fail with
    the documented messages.

- Property-test idea (optional but cheap): random sequences of
  `(insert n parallel annotations, retract m by selector)` operations
  against a model multimap; assert novelty state matches.

### Definition of done

- [ ] All canonical shapes from the source doc work end-to-end on a
      memory-storage ledger.
- [ ] **Restart round-trip:** insert annotations → commit → drop the
      in-memory state → rehydrate from commit history → queries return
      the same results. Locks in the durable encoding.
- [ ] Multiplicity contract verified: bare-triple cardinality
      unchanged.
- [ ] Lifecycle rules verified: RDF default preserves base fact;
      `lpgEdgeLifecycle` opt-in retracts it.
- [ ] Cascade rules verified for all three retract shapes.
- [ ] **Reverse-direction visibility check** verified:
      `AnnotationTargetOp` cannot leak base edges that policy hides
      or that the snapshot has retracted.
- [ ] **Reserved predicate firewall** verified across every write
      surface: JSON-LD insert/update, SPARQL UPDATE, Turtle/TriG
      ingest, bulk import, raw-txn upload, and the flake-sink defense
      layer all reject user-authored `f:reifies*`.
- [ ] No regression on non-annotation queries (full
      `cargo nextest run --workspace --all-features` passes).

---

## M2 — Binary arena + `IndexRoot` extension

**Goal:** make attachment lookups arena-fast at scale. Durability
already lives in the `f:reifies*` flakes from M1 — the arena is a
secondary index built from those facts at index-build time. Reads
merge indexed-arena + novelty-derived attachment state under existing
snapshot rules. Non-annotation ledgers continue to write zero
annotation artifacts.

### Files / changes

- New crate `fluree-db-annotation-index` (or new module under
  `fluree-db-binary-index/src/format/`). Format mirrors the dictionary
  trees:
  - Forward arena: branches range-route on `EdgeKey`; leaves store
    sorted `(EdgeKey, ann_sid, t, op)` rows with column-wise
    compression.
  - Reverse arena: branches range-route on `ann_sid`; leaves store
    sorted `(ann_sid, EdgeKey, t, op)` rows.
  - Magic numbers: `EAFB1`/`EAFL1` (forward branch/leaf), `EARB1`/`EARL1`
    (reverse branch/leaf), or similar — bikeshed during PR.
  - `AnnotationStats` populated at build time.

- `fluree-db-binary-index/src/format/index_root.rs` (line 127 today)
  - Add `pub annotation_index: Option<AnnotationIndexRoot>` to
    `IndexRoot`.
  - **Conservative absence:** `None` is a hard guarantee that the
    indexed snapshot has zero annotation attachments. Builders never
    write `None` when uncertain — they write `Some(empty)` and let the
    cascade path no-op cheaply.

- `fluree-db-indexer/src/`
  - **Build path is fact-driven, not novelty-driven.** The arena is a
    derived secondary index over the snapshot's `f:reifies*` flakes
    (the durable source of truth from M1). The builder must produce
    correct arenas in every entry path, not only the
    novelty-just-asserted-something case:
    - **Incremental rebuild** (next index from previous index +
      novelty): merge the previous arena with the novelty bundle
      delta.
    - **Full rebuild from commit history** (e.g. after corruption or
      a format change): scan the entire `f:reifies*` predicate space
      in the rebuilt fact indexes and emit fresh arenas. Novelty may
      be empty in this path.
    - **Bulk import** (Turtle / JSON-LD ingest): same as full rebuild
      — the importer emits `f:reifies*` flakes through the normal
      path, the indexer derives the arena from those facts at build
      time.
  - The arena builder runs the same bundle validator described under
    "Reserved-predicate invariants" so the on-disk arena never
    contains rows from malformed bundles.
  - Omit the annotation section entirely (and leave
    `IndexRoot.annotation_index = None`) only when the resulting
    snapshot has **zero** valid `f:reifies*` bundles — independent of
    whether novelty contributed any. This preserves the "no cost for
    non-annotation ledgers" property without leaking restart-state
    into the absence flag.
  - Treat the arenas like the existing dictionary trees: derived
    artifacts that can be rebuilt from facts whenever needed;
    corruption or omission is recoverable, never destructive.

- `fluree-db-binary-index/` reader / scan path
  - Lazy load forward/reverse arenas only when a query / cascade
    actually asks. Cache in the snapshot the same way dictionary trees
    are cached.
  - Merged read API. Both directions are iterators because the
    underlying storage is a multimap and visibility filtering may
    surface zero, one, or many results regardless of write-time
    invariants:
    ```rust
    fn current_annotations(snapshot, edge_key) -> impl Iterator<Item = Sid>
    fn current_targets(snapshot, ann_sid)      -> impl Iterator<Item = EdgeKey>

    // History queries see every (t, op) event for either direction.
    fn target_history(snapshot, ann_sid)       -> impl Iterator<Item = (EdgeKey, t, op)>
    ```
    The "exactly one current target per annotation SID" invariant is
    enforced at **stage time** in `fluree-db-transact/src/stage.rs` —
    re-attaching an SID to a different edge is a transaction error
    unless the prior attachment is being retracted in the same
    transaction. The reader returns whatever the snapshot actually
    contains; legacy or replayed-from-corrupt-history anomalies surface
    as multiple results rather than silent loss.
    Internally walks indexed arena + novelty multimap and applies
    `(t, op)` visibility.

- `fluree-db-transact/src/flake_sink.rs`
  - The plain-edge retract fast path now gates on:
    ```text
    indexed_has_annotations = root.annotation_index.is_some()
    novelty_has_annotations = novelty.attachments.has_annotations
    if !indexed_has_annotations && !novelty_has_annotations {
        // skip cascade lookup entirely
    }
    ```

### Tests

- `fluree-db-api/tests/it_edge_annotations_indexed.rs`:
  - All M1 tests, but force a reindex between insert and query.
  - `index_then_retract_cascades_through_arena` — annotation only in
    indexed arena (none in novelty) is correctly cascaded.
  - `non_annotation_ledger_writes_no_annotation_artifacts` — verify
    via storage inspection that `IndexRoot.annotation_index` is `None`.
  - `partial_novelty_partial_indexed_attachments_merge_correctly` —
    same edge has one attachment in arena and another in novelty;
    cascade and queries see both.

### Definition of done

- [ ] M1 test suite passes against a file-backed ledger that goes
      through reindexing.
- [ ] **Full rebuild from commit history** produces the same arenas
      as incremental indexing for the same end state. Test by:
      (a) ingest a corpus, index, snapshot arenas;
      (b) drop indexes, rebuild from commits, snapshot arenas;
      (c) compare byte-exact.
- [ ] **Bulk import** of a Turtle/JSON-LD corpus containing
      `@annotation` round-trips through the importer → indexer with
      arenas derived from the resulting `f:reifies*` facts.
- [ ] Bundle-validator rejects malformed bundles at index time and
      emits a telemetry counter; surrounding non-`f:reifies*` facts
      are unaffected.
- [ ] Storage inspector confirms zero annotation artifacts on
      ledgers that never used `@annotation`.
- [ ] Bench: non-annotation query throughput unchanged within noise.

---

## M3 — Planner / costing

**Goal:** the `EdgeAnnotation` operator picks the cheaper scan
direction based on selectivity. Annotation-rooted queries that filter
metadata constants don't pay the full edge scan.

### Files / changes

- `fluree-db-query/src/execute/operator_tree.rs`
  - Cost-driven choice between edge-first scan + forward-arena lookup
    vs annotation-first scan + reverse-arena lookup.
- `fluree-db-query/src/ir/`
  - `AnnotationStats` consumed by costing. Histogram of annotation
    predicates if cheap.
- Possibly: an `EdgeAnnotationStrategy` enum on the operator so the
  planner's choice is observable in `/explain`.

### Tests / benchmarks

- Throughput benchmarks on a 10M-edge / 100k-annotated synthetic
  dataset:
  - Edge-rooted query with low-selectivity edge: should choose
    annotation-first when metadata is selective.
  - Annotation-rooted query: stays annotation-first.
- `it_query_explain.rs` extension verifying chosen strategy.

---

## What I'd open as PR #1 (M0 first slice)

Concretely the next 2–3 days:

1. AST variants in `fluree-db-query/src/parse/ast.rs`.
2. Node-map parsing for `@annotation` / `@edge` / `@reifies` in
   `fluree-db-query/src/parse/node_map.rs`.
3. IR variants in `fluree-db-query/src/ir/pattern.rs` + lowering in
   `fluree-db-query/src/parse/lower.rs`.
4. `UnsupportedFeature` stub at the operator layer.
5. Parser tests covering every canonical shape.
6. Targeted error messages for the deferred shapes.

That's a self-contained PR that locks the syntax and produces a stable
AST/IR for M1 to fill in.

## Cross-cutting concerns

These touch every milestone; calling them out so they don't get lost.

- **Tracing.** Add `debug_span!("edge_annotation_lookup")` and
  `debug_span!("annotation_target_lookup")` once M1 lands. Update
  `.claude/skills/trace-{inspect,overview}/references/span-hierarchy.md`
  per `CLAUDE.md`.
- **Policy.** Annotation facts are ordinary RDF and pass through the
  existing policy filter unchanged. No new policy primitive in v1.
  Confirm with an integration test in M1
  (`policy_hides_annotation_metadata_independently_of_base_edge`).
- **History.** Attachment rows carry `(t, op)` so history queries
  surface attachment and detachment events on the same timeline as
  flake history. Verify in M1.
- **Documentation.** Each milestone updates `docs/`:
  - M0: extend `docs/query/jsonld-query.md` with `@annotation` syntax.
  - M1: add a new `docs/concepts/edge-annotations.md` walking the
    surface and lifecycle rules.
  - M2: extend `docs/design/index-format.md` to describe the
    annotation arenas.
- **Telemetry.** No new metrics in v1 beyond the spans. M3 may add
  cardinality-estimation counters once the planner needs them.

## Pointer back to source doc

Storage shape, edge-key structure, syntax examples, and rationale all
live in `EDGE_ANNOTATIONS.md`. This plan tracks the *implementation*
schedule; the design contract is the source doc's Decisions section.
Conflicts between the two documents are bugs in this plan — file
against `EDGE_ANNOTATIONS_IMPL_PLAN.md`, not the design doc.
