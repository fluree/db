# Edge Annotations — Implementation Plan

Companion to `EDGE_ANNOTATIONS.md`. The design doc owns the *what* and
*why*; this plan owns the *how, in what order, and where in the
codebase*. The "Design Decisions (v1)" section of the source doc is the
frozen contract this plan implements.

## Milestone overview

Each milestone ships as one PR (or a small chain) and is independently
useful — a reviewer can merge without waiting for the next.

| ID  | Scope | Persistence | Status |
|-----|-------|-------------|--------|
| M0  | Parser surface + IR stub | none — execution errors | ✅ shipped |
| M1a | Foundation + write side | durable via `f:reifies*` | ✅ shipped |
| M1b | Read side + cascade + integration | (M1a + scan-based lookups) | ✅ shipped |
| M2a | Scan-based indexed read path | works pre + post-index | ✅ shipped |
| M2b | Binary annotation-arena format | O(log N) lookups, lazy load | ✅ shipped (slices 1–5) |
| M3  | Planner / costing | — | ✅ shipped (M3.1 + M3.2 + M3.3) |

**M1 was split into two slices during implementation.** M1a and M1b
are each independently mergeable. M1a is a complete write-side
feature (annotations persist, but queries don't yet read them); M1b
adds the read-side dispatch and visibility wiring that turns it into
a queryable feature.

**M2b was implemented as seven slices** (1: format, 2: IndexRoot
field, 3a–c: pure builder + bundle reconstruction + indexer-side
orchestration, 3d: incremental wire-up, 3e: orchestrator provider
trait, 3f: complete-history contract, 3g: full-rebuild seal). Slice
4 (lazy reader + merged hydration path) and slice 5 (validation +
benchmarks) close the read path. The arena-vs-scan benchmark table
is at the end of this document.

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

- ✅ **Wildcard subject-hydration filter** (`fluree-db-api/src/format/hydration.rs`):
  the projection layer skips any predicate where
  `is_reserved_reifies_predicate(&p)` returns true. Closes the
  `select: {"?s": ["*"]}` leak path that the parser firewall
  doesn't catch.
- ✅ **Variable-predicate scan filter.** `BinaryScanOperator` filters
  `f:reifies*` (and the broader `f:` namespace in the default graph)
  out of variable-predicate scans on both the indexed-cursor path
  (`is_internal_predicate`, `binary_scan.rs:1047`) and the
  range/overlay fallback (`flakes_to_bindings`, `binary_scan.rs:707`).
  Coverage at `tests/it_edge_annotations.rs::variable_predicate_scan_hides_f_reifies{,_in_named_graph}`.

  **Contract** (uniform for JSON-LD and SPARQL):

  - **Direct mention of an `f:reifies*` IRI is parse-rejected**, full stop.
    JSON-LD: `parse::reject_user_authored_reifies_in_query` runs at the
    top of `parse_query_ast`. SPARQL: `lower::reject_direct_reifies_in_patterns`
    walks the post-lower pattern tree (recursing through OPTIONAL /
    UNION / MINUS / GRAPH / SERVICE / SUBQUERY / EdgeAnnotation
    bodies). The parser rejection is the contract-level boundary —
    even with the opt-in (below), a query naming `f:reifies*` directly
    is rejected.
  - **`opts.includeSystemFacts: true`** (JSON-LD only; SPARQL has no
    equivalent option today) only relaxes the **variable-predicate**
    scan filter — the `?p`-shape probe. Parsed in
    `parse::options::parse_include_system_facts`, accepts camel /
    snake / kebab variants. Threaded `UnresolvedOptions` →
    `Query::include_system_facts` → `ContextConfig::include_system_facts`
    → `ExecutionContext::include_system_facts` → snapshotted onto
    the scan operator at `open()`/`prime_history_flakes()`. Wired
    through both the single-graph view path (`view::query.rs`) and
    the dataset / connection path (`view::dataset_query.rs`). Also
    parsed inline on the JSON-LD ASK branch since ASK returns from
    the parser before the standard `parse_options` call. Tests:
    `opts_include_system_facts_surfaces_f_reifies`,
    `opts_include_system_facts_does_not_relax_direct_mention_firewall`,
    `opts_include_system_facts_propagates_through_dataset_path`,
    `opts_include_system_facts_works_for_ask_queries`,
    `lower::tests::test_rejects_user_authored_reifies_iri{,_inside_optional}`.
  - **History-range carve-out**: `BinaryScanOperator` with
    `mode == TemporalMode::History` (the inner of
    `BinaryHistoryScanOperator`) unconditionally bypasses the filter
    so attachment lifecycle stays inspectable. Applied in both
    `open()` and `prime_history_flakes()`. Test:
    `history_query_surfaces_f_reifies_events`.
- ⏳ **Graph-bound expansion in multi-graph queries — bug confirmed,
  workaround documented, architectural fix scoped.**

  The IR-level expansion in `expand_edge_annotation_patterns`
  correctly handles two shapes (each scan iteration is per-graph in
  these cases, so the join cannot cross graphs):
  - **Single-graph queries** (one ledger, no dataset).
  - **`Pattern::Graph`-wrapped patterns** (the wrapper carries
    through `map_subpatterns` and the executor iterates one named
    graph at a time, scoping every expanded triple per iteration).

  The bug fires when the dataset's default graph is the union of two
  or more sources (`from: [g1, g2]` / SPARQL `FROM <g1> FROM <g2>`)
  and the EdgeAnnotation/AnnotationTarget is **not** wrapped in
  `Pattern::Graph`. `DatasetOperator` fans every scan independently
  across the sources, so each base-edge match gets cross-joined with
  annotations from every source — N×M rows instead of N+M. The
  base-edge graph and the annotation graph aren't correlated through
  the `?ann` join key.

  Pinning test:
  `it_edge_annotations::cross_graph_misjoin_in_multi_source_default_known_limitation`
  (asserts the current 4-row output for a 2-graph + 1-edge-each
  scenario). When the architectural fix lands, this test flips to
  asserting 2 rows. Workaround coverage:
  `graph_wrapped_query_correctly_pairs_annotations_per_graph` shows
  the GRAPH-scoped form returns the correct 1 row per graph.

  **Fix path** (own slice, not in this changeset):
  - **Preferred — custom operator**. An `EdgeAnnotation` /
    `AnnotationTarget` operator that carries source-graph identity
    through the join. Same vehicle handles per-language
    disambiguation. Touches the planner + scan dispatch + dataset
    fanout.
  - **Alternative — graph-aware expansion rewrite**. At expansion
    time, when context indicates a multi-source dataset, wrap the
    expansion in a synthetic `Pattern::Graph { graph: ?fresh }` so
    each iteration scopes per-graph. Pure IR rewrite but needs
    dataset-shape plumbing into `build_where_plan` and only handles
    named-graph cases (default-graph wrap excludes default data).

  See the comment block on `expand_edge_annotation_patterns` in
  `fluree-db-query/src/execute/where_plan.rs` for the in-code
  contract.
- ⏳ **Per-language disambiguation.** Same architectural shape as the
  graph-bound bug above: `f:reifiesLang` is emitted by the write side
  but not constrained in the read-side expansion, so cross-language
  misjoin is possible when the same string is asserted with multiple
  language tags. Same custom-operator fix path; lands in the same
  slice. No pinning test today — needs setup that exercises
  multi-language flakes through the inline annotation form.
- ⏳ **Wildcard hide of anonymous annotation SIDs.** Explicit-IRI
  annotation subjects stay visible; anonymous (blank-node) ones are
  filtered out of `select: "*"` per the design decisions.
- ✅ **Plain-edge retract cascade** (`feat(M1b): cascade f:reifies*`):
  retracting a base edge via DELETE / SPARQL UPDATE auto-retracts
  the `f:reifies*` bundle pointing at it via a stage-time pass that
  walks the retract flake set, looks up annotations in the
  `AttachmentNovelty` overlay, and emits the inverse bundle. After
  cascade, `@reifies` queries correctly return zero rows for the
  retracted edge.
- ✅ **Annotation-metadata cascade** (RDF-mode anonymous + LPG-mode
  explicit opt-in): when a plain base-edge retract cascades the
  `f:reifies*` bundle, the cascade also retracts anonymous annotation
  body metadata by default. If the transaction sets
  `opts.lpgEdgeLifecycle: true`, explicit-IRI annotation body metadata
  is retracted too, matching Cypher relationship lifecycle. Default
  RDF mode still preserves explicit-IRI annotations as ordinary
  user-named RDF subjects.
- ⏳ **Occurrence-by-selector and by-annotation-id retracts.** The
  more targeted retract shapes that select a specific occurrence
  among parallel annotations need IR support (matching by metadata)
  and aren't covered by the plain-edge cascade.
- ✅ **JSON-LD subject-expansion `@annotation` output**
  (`feat(M1b): @annotation in subject expansion` +
  `fix(M1b): @annotation output respects as-of t`):
  `format_predicate_values`'s `Ref` arm downcasts the overlay to
  `Novelty`, builds an `EdgeKey` from `(flake.s, flake.p, flake.o)`,
  and when `current_annotations_for_at(edge, self.db.t)` returns
  any Sids, recursively hydrates each annotation subject and
  injects the result as an `@annotation` key on the expanded
  value. Time-travel-correct: a historical view at `t=N` only sees
  attachment events with `t <= N`. Single annotation renders as a
  bare object; multiple parallel annotations render as an array.
  Anonymous (blank-node) annotation SIDs have their `@id` stripped
  from the body. The wildcard-hydration filter keeps `f:reifies*`
  out of the rendered body.

  **M1b limitation: novelty-only.** The output path is
  novelty-backed — it downcasts the overlay to
  `fluree_db_novelty::Novelty` and reads `attachments`. Once a
  ledger's annotation rows roll into base storage / a binary
  arena (M2), this lookup returns nothing and the `@annotation`
  output disappears, even though the durable `f:reifies*` facts
  themselves remain queryable via normal scans. The M2 work
  introduces an indexed/arena-backed lookup that the hydrator
  consults alongside (or unified with) the novelty overlay. For
  the M1 milestone — which is explicitly novelty-only end-to-end
  — this is the documented behavior.
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
  - `stage.rs`: extend so retraction emit paths cascade per the
    rules in the decisions section.
    - Plain edge retract: enumerate current attachments via the
      merged snapshot+novelty scan path, retract `f:reifies*` flakes
      + owned annotation flakes (anonymous always; explicit only in
      LPG mode). The base-edge retract is the user-authored operation
      that triggered the cascade.
    - Occurrence retract by selector: filter attachments, retract
      matching ones; if last and LPG mode, retract base.
    - Delete-by-annotation-id: single attachment's `f:reifies*` +
      owned facts.
  - Transaction option `opts.lpgEdgeLifecycle: bool` parsed into
    `TxnOpts::lpg_edge_lifecycle` in
    `fluree-db-transact/src/parse/jsonld.rs` and threaded into the
    `stage.rs` cascade pass.

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
  - `cascade_cleans_up_anonymous_annotation_metadata` — anonymous
    metadata is gone on base-edge retract.
  - `cascade_keeps_explicit_iri_annotation_metadata` — explicit-IRI
    metadata is preserved in default RDF mode.
  - `cascade_lpg_mode_cleans_explicit_iri_metadata_too` —
    `opts.lpgEdgeLifecycle: true` cleans explicit-IRI metadata on
    base-edge retract.
  - `delete_by_annotation_id_targets_one_occurrence` — leaves siblings
    intact (pending targeted-retract IR work).
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
- [ ] Lifecycle rules verified: RDF default preserves explicit-IRI
      annotation metadata on base-edge retract; `lpgEdgeLifecycle`
      opt-in cleans it. Occurrence-level base-fact lifecycle remains
      deferred to targeted retract work.
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
    `IndexRoot.annotation_index = None`, `IndexRoot.has_annotations =
    false`) only when the resulting snapshot has **zero** valid
    `f:reifies*` bundles — independent of whether novelty contributed
    any. This preserves the "no cost for non-annotation ledgers"
    property without leaking restart-state into the absence flag. The
    encoder enforces the converse invariant: any populated
    `annotation_index` forces `FLAG_HAS_ANNOTATIONS` on the wire so
    the cascade fast-path never desynchronizes from a built arena.
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

- [x] M1 test suite passes against a file-backed ledger that goes
      through reindexing.
      *(`it_edge_annotations_indexed::incremental_arena_seal_then_arena_backed_query`
      and the existing `it_edge_annotations` suite.)*
- [x] Storage inspector confirms zero annotation artifacts on
      ledgers that never used `@annotation`.
      *(`non_annotation_ledger_skips_inject_annotations` —
      `snapshot.has_annotations=false`, novelty empty, no
      `@annotation` keys in hydrated output. The hydration
      zero-cost gate guarantees no POST scan either.)*
- [x] Bench: non-annotation query throughput unchanged within noise.
      *(`non_annotation_hydration/baseline` — see table below.)*
- [x] Bundle-validator rejects malformed bundles at index time;
      surrounding non-`f:reifies*` facts are unaffected.
      *(`bundle.rs::tests::malformed_bundle_skipped_with_counter`
      and `bundle_with_mismatched_flake_graph_is_skipped`.)*
- [ ] **Full rebuild from commit history** produces the same arenas
      as incremental indexing for the same end state.
      *Open: byte-exact equivalence still TBD — see "Remaining V1
      validation gaps" below.*
- [ ] **Bulk import** of a Turtle/JSON-LD corpus containing
      `@annotation` round-trips through the importer → indexer with
      arenas derived from the resulting `f:reifies*` facts.
      *Open: bulk-import validation deferred — see "Remaining V1
      validation gaps" below.*
- [ ] Telemetry counter for malformed-bundle skips at index time.
      *Open: validator emits `tracing::warn!` per skip but no
      Prometheus counter yet — see "Remaining V1 validation gaps".*

### Remaining V1 validation gaps

Slices 1–5 ship the read + write paths and the validation matrix.
Three items from the original DoD are deliberately deferred:

1. **Rebuild equivalence (byte-exact).** Today's tests prove
   correctness (`incremental_arena_seal_then_arena_backed_query`
   plus `full_rebuild_without_authoritative_falls_back_to_scan`
   together establish that the read path returns identical results
   regardless of which indexer path produced the new root). They do
   not prove that the **CIDs** match across two independent index
   builds of the same end state. Tracking this would need either
   a deterministic CID scheme for arena leaves (currently sha256 of
   the encoded blob, which is sensitive to row ordering and chunk
   boundaries) or a structural-equivalence helper that loads both
   arenas and compares the row sets.

2. **Bulk import path.** The Turtle/JSON-LD importer emits
   `f:reifies*` flakes through the normal write path, so the
   resolved RunRecords carry the same shape an incremental commit
   would produce. The arena seal hook (slice 3g) attaches in the
   full-rebuild path, but only when the caller supplies
   `Authoritative` events. The api's `BackgroundIndexerWorker`
   provider doesn't exercise this — bulk imports usually run via
   the CLI, which today doesn't populate
   `IndexerConfig.attachment_events`. End-to-end import ➜ arena
   coverage is a follow-up.

3. **Malformed-bundle telemetry counter.** Both
   `bundle::build_arenas_from_flakes` and
   `AttachmentNovelty::observe_flakes` `tracing::warn!` on each
   malformed bundle, and `ArenaBuildOutput.skipped_bundles`
   surfaces a per-build count. A long-lived per-ledger counter
   that operators can scrape from a metrics endpoint hasn't been
   wired through the telemetry layer yet.

---

## M3 — Planner / costing

**Goal:** the `EdgeAnnotation` operator picks the cheaper scan
direction based on selectivity. Annotation-rooted queries that filter
metadata constants don't pay the full edge scan.

`Pattern::EdgeAnnotation` / `Pattern::AnnotationTarget` are flattened
into a chain of triple patterns by `expand_edge_annotation_patterns`
*before* the join planner runs (`fluree-db-query/src/execute/where_plan.rs`).
That means the standard `reorder_patterns` machinery picks the
direction — there's no separate "EdgeAnnotation strategy" enum to
plumb through. The planner just needs accurate selectivity for the
expanded `f:reifies*` triples.

### Slices

| Slice | Goal | Status |
|-------|------|--------|
| M3.1 | Wire `AnnotationStats` into the cardinality estimator so `f:reifies*` triples on snapshots with `annotation_index = Some(_)` get tight selectivity instead of generic property-stats fallbacks. | ✅ shipped |
| M3.2 | Surface the chosen ordering in `/explain` output so the planner's decisions for edge-annotation queries are observable. | ✅ shipped |
| M3.3 | Throughput benchmark measuring current planner choices for edge-rooted vs annotation-rooted shapes on a non-trivial dataset. | ✅ shipped |

### M3.1 — what landed

`StatsView::merge_annotation_stats` overlays per-predicate stats for
the seven `f:reifies*` slots from `AnnotationIndexRoot.stats`
whenever the snapshot has an arena built. The arena builder tracks
per-slot NDV counters across the live (currently-asserted) edges,
so the planner gets sharp `BoundObject` selectivity for any
`?ann f:reifies* <const>`-shape probe.

`AnnotationStats` carries (added in M3.1 follow-up):

- `live_attachment_pairs` — number of live `(edge, ann)` pairs.
  Equals `distinct_annotations` under the v1 single-target
  invariant; tracked separately so the planner stays accurate on
  legacy / replayed-from-corrupt-history ledgers where one ann SID
  may be attached to multiple edges.
- Required slots — `distinct_reified_{subjects,predicates,objects}`.
  The row count for these is `live_attachment_pairs` (one row per
  live pair per required slot).
- Optional slots — row count + distinct-value count for `graph`,
  `lang`, `listIndex`. Row counts are per live `(edge, ann)` pair
  (parallel annotations on one named-graph edge each contribute
  their own `f:reifiesGraph` row). Older arena roots written
  before these fields existed deserialize cleanly with `0` via
  `#[serde(default)]`; the merge treats `0` as "no information"
  and falls back to `ndv_values = 1` (safe upper bound) for the
  required slots, or skips synthesis entirely for the optional
  ones (regular `IndexStats.properties` HLL fills in).
- `f:reifiesDatatype` is intentionally not synthesized from the
  arena — see the field comment on
  `AnnotationStats::reifies_datatype_rows` for why.

Synthesis rules:

- **Required slots** (`f:reifiesSubject` / `f:reifiesPredicate` /
  `f:reifiesObject`): `count = live_attachment_pairs`,
  `ndv_subjects = distinct_annotations`,
  `ndv_values = distinct_reified_<slot>` (or `1` for older arenas).
  `BoundObject` selectivity becomes
  `live_attachment_pairs / distinct_reified_<slot>` — pairs per
  pinned subject / predicate / object. The pair count, not
  `distinct_annotations`, is the row count for these slots so the
  estimate stays accurate even on legacy ledgers where one ann SID
  is attached to multiple edges (the v1 stage-time invariant in
  `fluree-db-transact::stage` rejects this on healthy writes;
  `live_attachment_pairs` defends against replay-from-corrupt-history
  cases). Older arena roots predate the field and report `0`; the
  merge falls back to `distinct_annotations` (safe under the
  invariant). For 10k annotations across 200 distinct subjects,
  a `?ann f:reifiesSubject :alice` probe estimates 50 rows
  instead of the previous 10k (the safe upper bound).
- **Optional slots** (`f:reifiesGraph` / `f:reifiesLang` /
  `f:reifiesListIndex`): synthesized **only** when the per-slot row
  count is non-zero. Row counts are per live `(edge, ann)` pair —
  parallel annotations on the same named-graph edge each contribute
  one `f:reifiesGraph` row, matching the on-wire flake count.
  `count = rows`, `ndv_values = distinct_<slot>`,
  `ndv_subjects = distinct_<slot>_anns` (the per-slot distinct
  annotation SID count tracked by the arena builder). For older
  arena roots that predate the per-slot ann counters, `ndv_subjects`
  falls back to `min(rows, distinct_annotations)` — exact under the
  v1 single-target invariant but heuristic under the multi-target
  anomaly, where it can undercount when a sparse slot has one ann
  SID dominating many edges. A workload that never uses named
  graphs leaves the `f:reifiesGraph` HLL untouched and the planner
  falls back to it.

- **`f:reifiesDatatype` is not synthesized from the arena.** The
  arena reconstructs `EdgeKey.dt` from the flake-level dt of
  `f:reifiesObject`, so it cannot tell whether the on-wire bundle
  emitted a separate `f:reifiesDatatype` flake (full bundle path)
  or omitted it (JSON-LD-compatible cascade — today's user-facing
  insert form). The arena builder reports zero for the datatype
  row count and `merge_annotation_stats` skips datatype entirely;
  the regular `IndexStats.properties` HLL is the source of truth.

The arena-side computation lives in two parallel places (kept in
sync): `bundle::compute_stats` (full-rebuild path, walks live edges
from the bundle decoder) and `builder::forward_arena_stats`
(incremental path, walks pre-built rows). End-to-end coverage:
`it_edge_annotations_indexed::storage_inspection_finds_arena_artifacts`
asserts the wire format carries the per-slot fields.

The merge is called from both `stats_cache::cached_stats_view_for_db`
(query path) and `explain::explain_query` (so `/explain` output sees
the same numbers the planner will). The stats-view cache key folds in
`annotation_index.{forward,reverse}_branch_cid` so a reindex/rebuild
that swaps the arena at the same `snapshot.t` produces a fresh slot.
`/explain` builds the view whenever either ordinary stats or the
arena is available, mirroring the query path.

### M3.2 — what landed

`/explain` (in `fluree-db-api/src/explain.rs`) now runs
`expand_edge_annotation_patterns` on the parsed query before
extracting triples — without it, `Pattern::EdgeAnnotation` /
`Pattern::AnnotationTarget` were silently dropped from
`triples_in_order`, so edge-annotation queries surfaced as empty in
the optimizer output.

Each emitted triple whose predicate is one of the seven `f:reifies*`
system predicates carries a new `annotation-role` field in the JSON
output (`subject` / `predicate` / `object` / `graph` / `datatype` /
`lang` / `listIndex`). The chosen ordering is observable: the slot
that the planner probes first (e.g. `subject` for an edge-first
direction, the body's external filter for annotation-first) is
visible to clients without re-running the planner.

`expand_edge_annotation_patterns` was promoted to `pub` at the
`fluree-db-query` crate root so `/explain` can run the same expansion
the executor does. Acceptance test:
`it_edge_annotations_indexed::explain_tags_annotation_role_and_uses_arena_stats`.

### M3.3 — what landed

`fluree-db-api/benches/annotation_planner.rs` runs four shapes per
size:

- `edge-rooted-{arena,scan}`: `select ?ann ?org where { ex:person-0
  ex:worksFor ?org { @annotation { @id ?ann } } }` — bound subject,
  one annotation result.
- `annotation-rooted-{arena,scan}`: `select ?person ?org where {
  ex:role "Director" ; @reifies { ?person ex:worksFor ?org } }` —
  filter annotations by metadata, return reified edges.

Workload: N people, each with one `ex:worksFor ex:acme` edge carrying
one annotation cycling through five roles. Sizes: 100, 1000. The bench
captures end-to-end timings, **not** the planner's chosen ordering —
inspecting the chosen ordering is M3.2's `/explain` output. A
follow-up could add an explain-plan assertion per shape to pin the
direction picked at each size.

| Bench | N=100 | N=1000 |
|-------|------:|-------:|
| edge-rooted-arena       | 1.71 ms | 25.87 ms |
| edge-rooted-scan        | 1.65 ms | 25.25 ms |
| annotation-rooted-arena | 1.52 ms | 23.23 ms |
| annotation-rooted-scan  | 1.50 ms | 22.99 ms |

Run with `cargo bench -p fluree-db-api --bench annotation_planner`.

The arena/scan delta sits inside measurement noise (≤ 3%) on this
workload — both ledgers carry the regular `IndexStats.properties`
HLL for the `f:reifies*` predicates after their normal index build,
so the planner picks essentially the same ordering with or without
the arena merge. M3.1's win shows up in two cases the bench doesn't
exercise:

1. **Stats-empty / freshly-arena-sealed snapshots** where
   `IndexStats.properties` hasn't been computed yet. Pre-M3.1, the
   planner fell back to `DEFAULT_PROPERTY_SCAN_SELECTIVITY` for every
   `f:reifies*` triple; post-M3.1 it uses the arena's
   `live_attachment_pairs` count and per-slot NDVs.
2. **Heavy-retract workloads** where `IndexStats.count` is inflated by
   asserts+retracts. Arena counters are live-only, so the merged
   estimate matches the actual row count instead of an upper bound.
3. **Workloads with diverse reified subjects/objects** where
   `BoundObject` selectivity for `?ann f:reifiesObject ex:acme` is
   `live_attachment_pairs / distinct_reified_objects` — orders of
   magnitude smaller than the scan-equivalent estimate.

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

---

## Slice 5 benchmarks

Captured on the M2b branch, criterion `--bench` runs (median
sample, 20 samples per cell). Run yourself with:

```
cargo bench -p fluree-db-api --bench annotation_hydration
```

### `annotation_hydration` — scan vs arena on annotated subjects

One base edge with `N` attachments; hydration query is
`select: {"?person": ["*", {"ex:worksFor": ["*"]}]}`. The "scan"
ledger reindexes without an `AttachmentEventsProvider` so it lands
in the M2a indexed-scan-fallback state (`has_annotations=true,
annotation_index=None`). The "arena" ledger reindexes with the
provider attached. Both paths flow through
`HydrationFormatter::inject_annotations`.

| N | scan | arena | arena speedup |
|---|---|---|---|
| 1       | 249 µs | 226 µs | 1.10× |
| 100     | 3.64 ms | 2.10 ms | 1.74× |
| 10 000  | 2.10 s  | 1.06 s  | 1.98× |

**Reading the data.** Lookup-only the arena is ~5000× cheaper at
N=10 000 (2 CAS reads vs ~10 001), but per-annotation body
formatting (`format_subject`) dominates total time and is identical
between paths — that ceiling caps the wall-clock win at ~2×.

### `non_annotation_hydration/baseline` — non-annotation regression guard

Hydration on a ledger that has never observed an `f:reifies*`
flake. Same query shape; no annotated edges anywhere. Exercises
the zero-cost gate added to `inject_annotations` (mirrors the
cascade fast-path in `fluree_db_transact::stage`).

| Ref edges | time |
|---|---|
| 1   | 56 µs |
| 100 | 3.15 ms (~31 µs/edge) |

The ~31 µs/edge is the pure hydration cost (formatting the ref +
nested expansion). The gate adds essentially zero overhead — when
both `snapshot.has_annotations` and `novelty.attachments.has_annotations()`
are false, `inject_annotations` returns immediately without
constructing an `EdgeKey` or issuing a POST scan. Run this bench
on every annotation-related code change to catch a regression
that would re-introduce per-ref scan cost on non-annotation
ledgers.
