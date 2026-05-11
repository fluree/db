# SPARQL Edge Annotations — Implementation Plan

Companion to `EDGE_ANNOTATIONS.md` and `EDGE_ANNOTATIONS_IMPL_PLAN.md`.
Those documents own the storage primitive, the JSON-LD surface, and the
read/write/cascade machinery (M0–M3, all shipped). This plan adds the
**SPARQL 1.2 / RDF 1.2 surface** on top of the same primitive. No new
storage, executor, planner, or arena work — purely parser + lower
changes that funnel into the two existing pipelines:

- **Query path** (SELECT / ASK / DESCRIBE / CONSTRUCT WHERE) lowers to
  the existing `Pattern::EdgeAnnotation` / `Pattern::AnnotationTarget`
  query IR. The executor, planner, and arena read paths from M1–M3
  handle the rest unchanged.
- **Update path** (INSERT DATA / DELETE DATA / INSERT WHERE / DELETE
  WHERE / DELETE+INSERT WHERE) lowers to the same durable
  `f:reifies*` staging records the JSON-LD `@annotation` lowering
  produces in `fluree-db-transact/src/parse/edge_annotations.rs`. The
  staging pass, cascade, and policy/firewall machinery already cover
  these records.

These are two distinct lowerings with different blank-node and
variable rules (see "Blank node and variable semantics" below). Any
slice that touches one must explicitly state which it targets.

## Frozen contract

The **decisions section of `EDGE_ANNOTATIONS.md`** is the contract this
plan implements. SPARQL is a second skin on the same primitive; any
disagreement between the SPARQL surface and the JSON-LD surface is a
bug in this plan.

## Surface — what's in v1

Three syntactic forms, each with a query-path lowering and an
update-path lowering. Same surface, two destinations.

| Form | Spec | Example | Query-path lowering | Update-path lowering |
|---|---|---|---|---|
| Anonymous annotation | RDF 1.2 annotationBlock | `ex:alice ex:worksFor ex:acme {\| ex:role "Engineer" \|} .` | `Pattern::EdgeAnnotation { annotation: fresh non-distinguished Var, body }` | INSERT: mint fresh blank-node Sid via the JSON-LD anonymous-reifier path. DELETE: rejected (no addressable identity). |
| Named annotation | RDF 1.2 reifier + annotationBlock | `ex:alice ex:worksFor ex:acme ~ _:ann {\| ex:role "Engineer" \|} .` | `Pattern::EdgeAnnotation { annotation: query-scoped Var (blank node = non-distinguished variable per SPARQL §4.1.4), body }` | INSERT: blank node minted as fresh Sid; IRI resolved via nameservice. DELETE DATA: blank-node form rejected per SPARQL §3.1.3; IRI form allowed. |
| Explicit reifier with `rdf:reifies` (WHERE only) | RDF 1.2 tripleTerm | `_:ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> ; ex:role "Engineer" .` | `Pattern::AnnotationTarget { annotation: Var/Sid, edge, body }` | **Rejected** in DATA / templates — SPARQL UPDATE accepts only the `~ {\| \|}` form (semantically equivalent, simpler to expand). |

### Blank node and variable semantics — query vs. update

SPARQL gives blank nodes very different semantics by context, and the
plan must respect them:

| Context | `_:ann` semantics | `{| |}` (no `~`) lowers to |
|---|---|---|
| Query WHERE (any clause) | Non-distinguished variable per SPARQL §4.1.4 — bindable inside the BGP, not exposable in `SELECT`. | Fresh synthetic non-distinguished Var (`?#__ann_<n>` — the `?#` prefix is uncollidable (`#` is SPARQL comment-start, so no user variable can lex with it) and hidden from `SELECT *` by the wildcard-formatter filter). |
| INSERT DATA | Fresh blank node, scoped to the operation; minted as a fresh Sid. | Fresh blank-node Sid. |
| DELETE DATA | **Rejected** per SPARQL 1.1 Update §3.1.3 — blank nodes are not allowed in DELETE DATA because they have no stable identity to address. | **Rejected** for the same reason. |
| INSERT template (in WHERE+INSERT) | Per-solution fresh blank node, per SPARQL Update §4.1.3. | Fresh per-solution blank-node template. |
| DELETE template (in WHERE+DELETE) | **Rejected** — blank nodes are forbidden in DELETE templates. The reifier must be a Var bound by WHERE, or an IRI. | **Rejected** for the same reason; require an explicit reifier var bound by the WHERE clause. |

Practical consequence: the parser produces one AST shape per syntactic
form, but the **lower** step branches on whether it is producing query
IR or update staging records, and applies the rules above. The two
lower entry points (`fluree-db-sparql/src/lower/annotation.rs` for
query; `fluree-db-transact/src/lower_sparql_update.rs` for update)
share the AST and the deferred-shape error catalog, but enforce
different blank-node rules.

### Grammar reference (RDF 1.2 Turtle, mirrored by SPARQL 1.2)

```
[13]  objectList       ::= object annotation ( ',' object annotation )*
[35]  annotation       ::= ( reifier | annotationBlock )*
[28]  reifier          ::= '~' ( iri | BlankNode )?
[36]  annotationBlock  ::= '{|' predicateObjectList '|}'
[32]  tripleTerm       ::= '<<(' ttSubject verb ttObject ')>>'
[17]  object           ::= iri | BlankNode | collection | blankNodePropertyList
                         | literal | tripleTerm | reifiedTriple
```

Key behaviors:
- An `annotationBlock` without a preceding `~` mints a fresh blank-node
  reifier.
- The `~` reifier may be empty (`~`) which is equivalent to a fresh
  blank node; or carry an IRI/BlankNode id.
- `tripleTerm` may appear in object position. v1 accepts it **only** as
  the object of `rdf:reifies`; everywhere else it errors.

## Out of scope (rejected with deferred-feature errors)

Identical messages to the JSON-LD deferred shapes so users see one
vocabulary across both surfaces:

- Triple terms as object of any predicate **other than** `rdf:reifies`
  (`ex:doc ex:mentions <<( s p o )>>`).
- Nested triple terms.
- Multi-triple reifiers (one reifier identifier reifying more than one
  triple term).
- Annotation block on a property-path triple (`?s ex:p1/ex:p2 ?o {| ... |}`).
- Annotation block on a literal-valued object.
- Annotation syntax in CONSTRUCT templates that target a non-JSON-LD
  serialization (Turtle-star/RDF 1.2 reifier output is its own decision).

## Legacy carve-out — `<< ... >>` for `f:t` / `f:op` only

The existing `lower/rdf_star.rs` handles a Fluree-specific extension:

```sparql
<< ex:alice ex:age ?age >> f:t ?t ; f:op ?op .
```

This extracts flake transaction time / op into BIND results. It is
**not** related to RDF 1.2 reifiers and **not** part of edge
annotations. The bare `<< s p o >>` (no parens) form remains valid
**only** as the subject of `f:t` / `f:op` triples; any other use is
already a parse error today and stays one. Documented in
`docs/concepts/edge-annotations.md` (M4.6) as a separate, orthogonal
feature.

## Milestone overview

| ID | Scope | Status |
|----|-------|--------|
| M4.1 | Lex (new tokens for `{\|`, `\|}`, `~`, `<<(`, `)>>`) | ✅ shipped |
| M4.2 | AST + parser for annotation tail and `rdf:reifies` triple-term | ✅ shipped |
| M4.3 | Query-path lower → `Pattern::EdgeAnnotation` / `Pattern::AnnotationTarget` | ✅ shipped |
| M4.4 | Update-path lower → `f:reifies*` staging records | ✅ shipped |
| M4.5 | CONSTRUCT boundary error | ✅ shipped |
| M4.6 | Tests + docs | ✅ shipped |

Each slice ships as one PR. M4.1 + M4.2 may combine if the diff is
small enough to review together.

---

## M4.1 — Lex

**Goal:** the lexer recognizes the new tokens with correct
longest-match precedence. No parse or AST changes yet.

### New tokens

```rust
TokenKind::AnnotationOpen   // {|
TokenKind::AnnotationClose  // |}
TokenKind::Tilde            // ~
TokenKind::TripleTermStart  // <<(
TokenKind::TripleTermEnd    // )>>
```

### Precedence rules (longest-match)

The lexer's character dispatch already orders multi-char punctuation
before single-char (`<<` before `<`, `>>` before `>`). New entries:

- `<<(` must be tried before `<<`.
- `)>>` must be tried before `)` and `>>`.
- `{|` must be tried before `{`.
- `|}` must be tried before `|` and `}`.
- `~` is currently unused as a token — add as a new single-char.

### Files

- `fluree-db-sparql/src/lex/token.rs` — `TokenKind` variants + `Display`.
- `fluree-db-sparql/src/lex/lexer.rs` — punctuation parsers and dispatch
  table. Add `parse_triple_term_start`, `parse_triple_term_end`,
  `parse_annotation_open`, `parse_annotation_close`, plus `~` in the
  single-char map.

### Tests

- `fluree-db-sparql/src/lex/lexer.rs` (existing test module): a focused
  test per token, plus one test that verifies `<<(` does not get split
  into `<< (` and that `)>>` does not get split into `) >>`.

### Definition of done

- [ ] All new tokens produced for the canonical examples in this doc.
- [ ] No regression in existing lexer tests (`<<` / `>>` for `f:t`,
      `{` / `}` for graph patterns, `|` in property paths, `}` closing
      a graph pattern).

---

## M4.2 — AST + parser

**Goal:** parse the three forms into a stable AST. Lowering is the next
slice.

### AST additions

```rust
// fluree-db-sparql/src/ast/annotation.rs (new)

/// An annotation tail attached to a triple's object position.
/// Mirrors the RDF 1.2 `annotation` production.
pub struct Annotation {
    /// Optional explicit reifier id. `None` means "mint fresh blank".
    /// Empty `~` (no id) also lowers to `None`.
    pub reifier: Option<ReifierId>,
    /// `{| ... |}` body. `None` means a bare `~` with no annotation
    /// block (still a valid RDF 1.2 production).
    pub block: Option<AnnotationBlock>,
    pub span: SourceSpan,
}

pub enum ReifierId {
    Iri(Iri),
    BlankNode(BlankNode),
    Var(Var), // queries only — UPDATE INSERT DATA rejects
}

pub struct AnnotationBlock {
    pub patterns: Vec<TriplePattern>, // predicate-object list applied to the reifier
    pub span: SourceSpan,
}

/// RDF 1.2 triple term — only valid as object of `rdf:reifies` in v1.
pub struct TripleTerm {
    pub subject: SubjectTerm,
    pub predicate: PredicateTerm,
    pub object: ObjectTerm,
    pub span: SourceSpan,
}
```

### `Term` / `ObjectTerm` widening — minimize blast radius

`ObjectTerm = Term` today. Two options considered:

- (a) Add `Term::TripleTerm(Box<TripleTerm>)` — touches every match
  site that exhausts `Term`.
- (b) Add a new `ObjectOrTripleTerm` enum used only at the parse-object
  call site, with `TripleTerm` strictly outside the existing `Term`.

**Decision: (b).** v1 only allows triple terms as object of
`rdf:reifies`, so a parse-time check ("predicate is `rdf:reifies` or
this is an error") is the correct gate. Keeping `TripleTerm` out of
`Term` means zero new match arms in lower/, planner, executor.

### `TriplePattern` carries the annotation tail

Per the RDF 1.2 grammar, the annotation belongs to the `(s, p, o)`
triple itself, not the object. Add to `TriplePattern`:

```rust
pub struct TriplePattern {
    pub subject: SubjectTerm,
    pub predicate: PredicateTerm,
    pub object: ObjectTerm,
    pub annotation: Option<Annotation>, // NEW
    pub span: SourceSpan,
}
```

Existing constructors `TriplePattern::new(s, p, o, span)` keep
defaulting `annotation: None`. New constructor
`TriplePattern::with_annotation(s, p, o, ann, span)` for the parser.

### Parser changes

`fluree-db-sparql/src/parse/query/term.rs`:

1. **`parse_object_list`** — after each parsed object, peek for
   `Tilde` / `AnnotationOpen` and call `parse_annotation_tail`.
2. **`parse_annotation_tail`** — consume zero-or-more
   `(reifier | annotationBlock)` per the grammar. The grammar allows
   them to repeat (`annotation ::= ( reifier | annotationBlock )*`).
   v1 accepts at most one of each in any order; reject the rest with
   a clear error pointing at the deferred multi-reification case.
3. **`parse_object`** — extended path: when called from a context where
   the predicate is `rdf:reifies`, allow `TripleTermStart` and parse a
   `TripleTerm`. Out-of-context use is rejected at parse time with the
   deferred-feature error.

The cleanest way to thread "predicate is `rdf:reifies`" is to add a
`parse_reifies_object` helper invoked from `parse_predicate_object_list_with_paths`
when the predicate IRI resolves to `rdf:reifies`. Otherwise the
ordinary `parse_object` runs and rejects `TripleTermStart` with the
deferred error.

### Property-path interaction

`parse_path_object_list` does **not** accept an annotation tail.
Annotation syntax requires a simple-predicate triple per the grammar
(`PropertyListPathNotEmpty` distinguishes verb-simple from verb-path
in SPARQL 1.2; only verb-simple has the annotation hook). If a user
writes `?s ex:p1/ex:p2 ?o {| ... |}` we emit:

> `error: annotation block requires a simple-predicate triple; property paths cannot carry annotations`

### Files

- `fluree-db-sparql/src/ast/annotation.rs` (new).
- `fluree-db-sparql/src/ast/term.rs` — `TripleTerm` defined here or
  in `annotation.rs`; either works.
- `fluree-db-sparql/src/ast/mod.rs` — re-exports.
- `fluree-db-sparql/src/ast/query.rs` — `TriplePattern` field +
  constructors.
- `fluree-db-sparql/src/parse/query/term.rs` — parsing.
- `fluree-db-sparql/src/parse/stream.rs` — likely a one-line addition
  to `is_term_start` / similar predicates if `Tilde` / `AnnotationOpen`
  affect them.

### Tests

- `fluree-db-sparql/src/parse/`: per-shape parse tests (anonymous,
  named, `rdf:reifies` form).
- Negative tests for each deferred shape.

### Definition of done

- [ ] All shapes from the surface table parse without error.
- [ ] All deferred shapes produce the documented error message and
      span.
- [ ] No `Pattern` match-arm churn (verifies the `ObjectOrTripleTerm`
      decision).
- [ ] Existing `<< s p ?o >> f:t ?t` parser tests still pass.

---

## M4.3 — Lower

**Goal:** AST → existing IR. Once this slice lands, queries execute
against the same IR the JSON-LD surface produces, so M1–M3 read paths
work unchanged.

### Files

- `fluree-db-sparql/src/lower/annotation.rs` (new) — the two lowering
  rules described below.
- `fluree-db-sparql/src/lower/pattern.rs` — call `lower_annotation`
  from BGP lowering after each triple. The existing
  `lower_bgp_with_rdf_star` path stays for `f:t` / `f:op`.
- `fluree-db-sparql/src/lower/mod.rs` — re-exports.

### Rule 1 — annotation tail on a triple

```text
TriplePattern { s, p, o, annotation: Some(Annotation { reifier, block, .. }) }

  ===>

Pattern::EdgeAnnotation {
    edge: TriplePattern { s, p, o, .. },
    annotation: <reifier id, or fresh synthetic>,
    body: lower_block(block),
}
```

Rules for the annotation subject:
- `Some(ReifierId::Iri(iri))` → resolved Sid.
- `Some(ReifierId::BlankNode(b))` → minted Sid through the existing
  blank-node minter.
- `Some(ReifierId::Var(v))` → resolved Var.
- `None` (no `~`, no `~ <id>`) → fresh synthetic var
  (`?#__ann_<n>` — the `?#` prefix is uncollidable (`#` is SPARQL comment-start, so no user variable can lex with it) and hidden from `SELECT *` by the wildcard-formatter filter), matching the JSON-LD lower's behavior.
- `Some(reifier)` with `block: None` → the bare `~` form. Lowers to
  `EdgeAnnotation` with empty body. The reifier becomes
  bindable/usable elsewhere in the query.

### Rule 2 — `rdf:reifies` with triple-term object

```text
TriplePattern { s: ?ann, p: rdf:reifies, o: TripleTerm { ts, tp, to } }
+ qualifying sibling triples in the same BGP whose subject is ?ann

  ===>

Pattern::AnnotationTarget {
    annotation: ?ann,
    edge: TriplePattern(ts, tp, to),
    body: <qualifying sibling triples about ?ann>,
}
```

#### Body folding — not needed

After implementing M4.2 we revisited the body-folding requirement and
concluded it is **unnecessary** for SPARQL. The IR's
`Pattern::AnnotationTarget { body }` is flattened by
`expand_edge_annotation_patterns` (`fluree-db-query/src/execute/where_plan.rs`)
into the same sequence of `f:reifies*` triples regardless of whether
the metadata patterns live in `body` or sit as siblings in the
surrounding scope joining on the reifier variable. The executor sees
the same flat work either way.

For SPARQL, the lower step therefore emits
`Pattern::AnnotationTarget { annotation, edge, body: vec![] }` and
lets sibling triples about the reifier flow through their natural
SPARQL scoping. This is cheaper to implement, mirrors the parser
(which produces a `GraphPattern::AnnotationTarget` separate from the
surrounding BGP), and avoids needing a fragile syntactic pre-pass over
BGPs. The JSON-LD path keeps body folding because its source form is
node-map nesting, not BGPs — different surface, same end shape.

#### Failure modes the lower step rejects

- More than one `?ann rdf:reifies <<( ... )>>` triple sharing the same
  reifier → deferred-feature error. (Already covered at parse time
  via the comma-separated-triple-terms rejection in M4.2; the lower
  step adds a second check for the cross-statement case where the
  same reifier IRI is named on two different `rdf:reifies` triples
  in the same scope.)
- `rdf:reifies` with anything other than a triple term in object
  position (e.g. `?ann rdf:reifies ?other`) → deferred-feature error
  (this is the "general proposition logic" deferred case). The
  parser does not recognize this form as `AnnotationTarget`; it
  flows through as an ordinary triple, and the lower step detects
  it via predicate matching and rejects.
- Triple term in object position of any predicate other than
  `rdf:reifies` → already rejected at parse time (M4.2).

#### Sharing with the JSON-LD lower

The JSON-LD `@reifies` lower performs the analogous fold over a
node-map. Implement the SPARQL fold in
`fluree-db-sparql/src/lower/annotation.rs` and only extract a shared
helper into `fluree-db-query` if the two implementations turn out to
share substantial code; do not refactor JSON-LD as part of this slice.

### Multiplicity and ordering

The existing `expand_edge_annotation_patterns`
(`fluree-db-query/src/execute/where_plan.rs`) flattens both IR
variants into base edge + `f:reifies*` triples + body, and the
standard `reorder_patterns` machinery handles cardinality. M3.1's
`AnnotationStats`-aware costing applies for free.

### Definition of done

- [ ] All shapes execute against in-memory + indexed snapshots.
- [ ] Round-trip parity test: insert via JSON-LD, query via SPARQL
      (and vice versa) returns identical bindings.
- [ ] Multiplicity contract holds: bare-triple cardinality unchanged
      whether or not annotations exist on the same edge.

---

## M4.4 — SPARQL UPDATE

**Goal:** SPARQL `INSERT DATA` / `INSERT WHERE` / `DELETE DATA` /
`DELETE WHERE` / `DELETE+INSERT WHERE` round-trip through the existing
`parse/edge_annotations.rs` staging pipeline. The output staging
shape is bit-for-bit identical to the JSON-LD `@annotation` path so
the cascade and policy passes don't need to know which surface
produced the records.

### Files

- `fluree-db-transact/src/lower_sparql_update.rs` — extend the lowering
  so triples carrying an annotation tail emit the right
  `AnnotationStaging` / `f:reifies*` records. The shape it produces
  must be identical to what the JSON-LD `@annotation` lowering produces
  so the stage pass is shared.
- `fluree-db-transact/src/parse/edge_annotations.rs` — likely no
  changes; the SPARQL update lower funnels into the same staging
  emitter.

### Per-operation rules

SPARQL Update places different constraints on blank nodes and
variables in different operations. The table below is the contract;
deviations from any row are bugs.

| Operation | Reifier in `~` | Reifier var in `~` (e.g. `~ ?ann`) | Anonymous `{\| \|}` (no `~`) | `_:ann rdf:reifies <<( ... )>>` |
|---|---|---|---|---|
| `INSERT DATA` | IRI: resolved via nameservice. Blank node `_:ann`: minted as fresh Sid, scoped to the operation per SPARQL §4.1.3. | **Rejected** — variables not allowed in `INSERT DATA`. | Mint fresh blank-node Sid for the reifier; emit standard `f:reifies*` bundle + body. | Blank node minted, IRI resolved; standard bundle. |
| `DELETE DATA` | IRI: allowed (addresses an existing reifier by stable identity). Blank node `_:ann`: **rejected** per SPARQL §3.1.3 — blank nodes have no addressable identity in `DELETE DATA`. | **Rejected** — variables not allowed in `DELETE DATA`. | **Rejected** — no addressable identity. Use `DELETE WHERE` with a binding pattern instead. | IRI form allowed; blank-node form rejected (same reason as above). |
| `INSERT WHERE` (template) | IRI: resolved. Blank node: per-solution fresh blank node per SPARQL §4.1.3 (same `_:ann` label across the template binds to the same per-solution blank). | Variable bound by WHERE; lower to that var resolved against the binding. | Per-solution fresh blank-node template. | Blank node per-solution; IRI resolved; var bound by WHERE. |
| `DELETE WHERE` (template) | IRI: allowed. Blank node: **rejected** — DELETE templates forbid blank nodes per SPARQL §3.1.3. | Variable bound by WHERE; required for any reifier identity in the DELETE template. | **Rejected** in DELETE template — there is no way to address an anonymous reifier. Use a named one bound by WHERE. | IRI form allowed; blank-node form rejected; var form requires WHERE binding. |
| `DELETE+INSERT WHERE` | DELETE template follows DELETE rules; INSERT template follows INSERT rules. WHERE uses query-path semantics. | Variable bound by WHERE; usable in both DELETE and INSERT templates. | Allowed in INSERT (per-solution blank); rejected in DELETE. | DELETE: IRI/var allowed, blank rejected. INSERT: blank allowed (per-solution), IRI/var resolved. |
| `WHERE` clause (any) | Query-path lowering per M4.3. Blank node = non-distinguished variable per SPARQL §4.1.4. | Variable usable elsewhere in WHERE. | Fresh non-distinguished synthetic Var (`?#__ann_<n>` — the `?#` prefix is uncollidable (`#` is SPARQL comment-start, so no user variable can lex with it) and hidden from `SELECT *` by the wildcard-formatter filter). | Standard query lowering. |

The reserved-predicate firewall (`is_reserved_reifies_predicate`)
already covers SPARQL UPDATE entry points. M4.4 adds a SPARQL-surface
test that confirms `INSERT DATA { _:a f:reifiesSubject ... }` is
rejected through this path.

### Cascade and policy

Once staging emits the `f:reifies*` bundle, M1 cascade rules
apply unchanged:
- Plain-edge DELETE cascades the bundle (anonymous metadata in RDF
  mode; explicit-IRI metadata too in LPG mode).
- `opts.lpgEdgeLifecycle: true` is accepted via the existing SPARQL
  UPDATE options surface (verify the option already threads through
  `parse_transaction_opts`; if not, add it as part of this slice).

### Definition of done

- [ ] Insert via SPARQL UPDATE, query via JSON-LD: identical results to
      JSON-LD insert + JSON-LD query.
- [ ] Plain-edge DELETE via SPARQL UPDATE triggers the same cascade as
      the JSON-LD DELETE path.
- [ ] LPG-mode opt-in available via the same `opts.lpgEdgeLifecycle`
      knob (already accepted by the SPARQL UPDATE entry point — verify).
- [ ] Reserved-predicate firewall test passes.

---

## M4.5 — CONSTRUCT boundary

**Goal:** be explicit about the deferred Turtle-star/RDF-1.2 output
decision rather than silently emitting `f:reifies*` triples.

### Behavior

A SPARQL CONSTRUCT template that contains an annotation tail or a
`rdf:reifies` + tripleTerm pattern returns:

```
UnsupportedFeature: SPARQL CONSTRUCT projection of edge-annotation
metadata is not supported in v1. The Turtle-star vs RDF 1.2 reifier
output decision is tracked in EDGE_ANNOTATIONS.md "Deferred / Out of
v1 scope". JSON-LD output via the JSON-LD formatter is supported.
```

### Files

- `fluree-db-sparql/src/lower/construct.rs` — detect annotation in the
  template, error out. Same error message used by the JSON-LD
  non-JSON-LD-target path (extract a shared constant).

### Definition of done

- [ ] Test: SPARQL CONSTRUCT with annotation in template returns the
      documented error. Span points at the offending tail.
- [ ] CONSTRUCT *without* annotation in the template still works even
      when the WHERE pattern uses annotations to filter.

---

## M4.6 — Tests + docs

### Tests

`fluree-db-api/tests/it_query_sparql_annotations.rs` (new):

Mirror the JSON-LD `it_edge_annotations.rs` test list one-for-one,
ported to SPARQL syntax:

- Anonymous annotation insert + query round-trip.
- Named annotation (`~ _:ann`) round-trip.
- `rdf:reifies` form round-trip.
- Two parallel annotations on one edge → two query rows.
- Bare-triple cardinality unaffected by annotation presence.
- `selectDistinct` unaffected.
- Annotation-rooted query via `rdf:reifies`.
- Cascade — anonymous metadata cleaned on base-edge retract (RDF mode).
- Cascade — explicit-IRI metadata preserved (RDF mode).
- Cascade — explicit-IRI metadata cleaned in LPG mode opt-in.
- Empty `{| |}` is no-op in RDF mode.
- Empty `{| |}` mints subject in LPG mode.
- Wildcard `select *` hides anonymous annotation Sids.
- All deferred shapes produce documented errors.
- Reserved-predicate firewall through SPARQL UPDATE.
- Reverse-direction visibility: policy hides base edge → annotation-
  rooted query returns zero rows (no leak).
- `<< s p ?o >> f:t ?t` regression: still works, does not trigger any
  annotation lowering.

`fluree-db-api/tests/it_query_sparql_annotations_indexed.rs`:
- Reindex between insert and query for the same shapes — confirms the
  arena read path works through the SPARQL surface.

### W3C testsuite

- `cd testsuite-sparql/ && make count-eval` before and after — confirm
  no regression. SPARQL 1.2 star tests aren't in the W3C suite yet
  (last checked); coverage stays in our integration tests.

### Docs

- `docs/concepts/edge-annotations.md` — add a SPARQL section with each
  surface form, side-by-side with the existing JSON-LD examples.
- `docs/concepts/edge-annotations.md` — add the legacy `<< ... >>` /
  `f:t` / `f:op` carve-out section.
- `docs/SUMMARY.md` — link if the page is new.
- `EDGE_ANNOTATIONS_IMPL_PLAN.md` — flip the "Phase 5 (RDF-star/JSON-LD-star)"
  row in the milestone table to reference this plan.

### Definition of done

- [ ] All tests pass on memory + file storage.
- [ ] `cargo nextest run --workspace --all-features --no-fail-fast`
      passes.
- [ ] `cd testsuite-sparql && make count-eval` shows no regression.
- [ ] Docs updated.

---

## Cross-cutting concerns

- **No new tracing spans.** The IR is unchanged; spans already cover
  parse / lower / execute. If the lower step grows expensive enough to
  matter, add a `debug_span!` then.
- **Policy / history / cascade.** All inherited from M1 — annotations
  produced by SPARQL are indistinguishable from annotations produced by
  JSON-LD once they reach the IR.
- **Reserved-predicate firewall.** Already covers SPARQL UPDATE entry
  points via `is_reserved_reifies_predicate`. M4.4 adds a confirming
  test.

## What I'd open as PR #1

The lex slice (M4.1) is a few hundred lines, self-contained, and gives
the parser slice clean tokens to consume. Concretely:

1. Add the five token kinds + `Display` impls.
2. Add the punctuation parsers with correct longest-match ordering.
3. Per-token unit test + a "doesn't break existing punctuation" test.
4. `cargo test -p fluree-db-sparql` green.

That's a single reviewable PR. M4.2 (AST + parse) follows directly.
