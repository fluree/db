# PR #1453 — feat(sparql): burn-down Wave 1 — validation passes, grammar tightening, authoritative diagnostics, reifier forms

**Branch:** `burndown/wave-1` → `main`
**Author:** aaj3f · **Size:** +5,048 / −657, 34 files · Interim guard for #1438 (full fix lands in Wave 2); actions decisions D-4, D-10a, D-1

---

## Capability / user story

> As a SPARQL user, when I submit a query or update the spec declares invalid — or one the parser flagged as malformed — I get a loud, precisely-labeled error instead of Fluree silently executing a *different* query than the one I wrote.

This is the reject-more wave, and its headline is not the ~123 register entries — it is the diagnostics-swallowing hole it closes: `fluree.query` previously executed error-recovered ASTs, so a query the parser had flagged (stray dot, bare FILTER term, trailing garbage) still ran against a silently-rewritten AST and returned answers to a question the user never asked. The worst instance was #1438: a standard `INSERT …; DELETE …` multi-operation update **committed only its first operation and discarded the rest** — silent data loss through the public transact path. Both belong to the silent-wrong-results class, which is the worst failure class this engine can have.

The four components: (1) semantic validation passes V3–V6 + two SPARQL 1.2 checks (`fluree-db-sparql/src/validate/{bnode_scope,projection}.rs`, both new); (2) V1/V2 grammar tightening + the authoritative-diagnostics seam (`parse/query/pattern.rs`, `fluree-db-api/src/query/helpers.rs:282`); (3) UPDATE validation — GRAPH-in-DELETE-WHERE support + bnode-in-DELETE rejection on both the SPARQL and JSON-LD surfaces (`fluree-db-transact/src/lower_sparql_update.rs`, `parse/jsonld.rs:392`); (4) the RDF 1.2 reifier-form parser lowered onto the existing edge-annotation IR (`ast/annotation.rs`, `lower/{annotation,rdf_star}.rs`).

## Behavior changes a reviewer must consciously bless

| Input that used to "work" | Now | Grounding |
|---|---|---|
| `GROUP BY (expr)` + reprojecting the expression (`SELECT (expr AS ?k)`) | Rejected — alias the key: `GROUP BY (expr AS ?k)` | W3C agg08; the field/#1362 shape; migration pins at `it_query_sparql.rs:673`, `:5533` |
| Grouped-list projection (`SELECT ?person ?favNums … GROUP BY ?person`) on the **SPARQL** surface | Rejected; JSON-LD keeps the extension, parity-pinned | W3C group06; pins at `it_query_sparql.rs:2177`, JSON-LD retention at `it_query_grouping.rs:331` |
| Stray/doubled/leading dots; bare `FILTER ?x`; trailing tokens | Parse error, and the error now **prevents execution** | W3C syn-bad-02..14, filter-missing-parens; `it_query_sparql_parse_errors.rs` proves end-to-end rejection |
| Multi-op UPDATE (`INSERT …; DELETE …`) silently committing only op #1 | Loud error naming the limitation; single trailing `;` still legal | #1438; e2e test incl. a `t == 1` control proving the rejected request staged **nothing** (`it_transact_update.rs:2346`) |
| Blank nodes in DELETE DATA / DELETE WHERE / DELETE templates (both surfaces) | Rejected with a help text; stable `_:fdb-` ids exempt | SPARQL 1.1 §19.8 note 8; JSON-LD nested-object-without-`@id` case included (`it_transact_update.rs:285`) |
| BIND/alias/bnode-label scope violations (V3/V5/V6), nested aggregates, duplicate VALUES vars | Rejected | W3C syntax-BINDscope6-8, SELECTscope2, test_45/65, sparql12 negatives |
| `GRAPH <iri>` blocks in DELETE WHERE (previously **rejected**) | Now supported via the existing Modify graph-template lowering | W3C dawg-delete-where-02/04/06; both prior rejection sites removed |
| `<< s p o ~ r? >>` reifier forms (object/nested/standalone), multi-unit annotation tails, path verbs in blocks | Parsed and lowered to the existing `Pattern::EdgeAnnotation`/`AnnotationTarget` IR; deferred positions error cleanly | W3C sparql12 syntax-triple-terms −61 entries |

## How to read the diff (reviewer guide)

Read in this order:

1. **`fluree-db-api/src/query/helpers.rs:282-297`** — the wave's crux: `Some(ast) if !has_parse_errors` makes error-severity diagnostics authoritative at the single seam every public query entry funnels through (`parse_sparql_to_ir` at `:73` feeds query/dataset/stream/credential/explain). Warning diagnostics still never reject; recovery is preserved for tooling. The update seam already did this (`tx_builder.rs:42`).
2. **`fluree-db-sparql/src/parse/query/mod.rs:94-133`** — the trailing-token/EOF assertion (the #1438 guard, with the one-legal-trailing-`;` carve-out) and the V5 `semantic_reject` that refuses AST production for `BindTargetAlreadyInScope` — note the asymmetry vs V1/V2 (which produce recovered ASTs and rely on the seam) is deliberate and documented: V5's input *parses completely*, so an AST-only consumer would execute it.
3. **`fluree-db-sparql/src/parse/query/pattern.rs:249-380`** — V1 dot-structure via a single `dot_allowed` flag on the existing token flow (no backtracking), V5's in-parser scope check (the single-pattern-group simplification makes legal/illegal shapes byte-identical as ASTs — verify you buy that argument, it is why V5 cannot live in `validate()`), V2's `is_constraint_expression` post-parse predicate, the VALUES row-shape fix (rows follow the var-*list* shape, not the count — W3C values7), and the sub-SELECT trailing VALUES clause.
4. **`fluree-db-sparql/src/validate/bnode_scope.rs` + `projection.rs`** (both new) — V3's scope model (consecutive-`Bgp`-siblings-imply-braced-block rests on the same parser invariant as V5; FILTER correctly does *not* break a BGP via the `prev_was_bgp` reset) and V4/V6's key/alias licensing with its documented leniencies (bracketed bare var keys, earlier-alias chaining). Both are deliberately under-rejecting (BIND/VALUES treated as non-boundaries in V3; HAVING/ORDER BY unchecked in V4) — conservative in the right direction.
5. **`fluree-db-transact/src/lower_sparql_update.rs:938-1100`** — GRAPH-bearing DELETE WHERE routes to `lower_delete_where_with_graphs` (`:1021`), reusing the stored-`SparqlWhereClause` + graph-scoped-template machinery Modify already uses; the triple-only fast path is preserved byte-identical (`:951` keeps unresolved patterns, no stored WHERE — pinned by `test_lower_delete_where_triple_only_path_unchanged`). `rewrite_blank_nodes_to_vars` (`:1080`) keeps existential-bnode semantics by rewriting once on the shared AST so WHERE and template agree.
6. **`fluree-db-transact/src/parse/jsonld.rs:392-407`** — the JSON-LD-surface bnode-in-delete guard (this *is* a behavior change on a Fluree-owned surface: nested delete objects without `@id` previously skolemized fresh SIDs and silently matched nothing).
7. **`fluree-db-sparql/src/ast/annotation.rs` + `lower/{annotation,rdf_star}.rs`** — `Annotation` → `Vec<AnnotationUnit>` per the RDF 1.2 attachment rule; `lower_bgp_with_rdf_star` restructured so the legacy `f:t`/`f:op` carve-out (reifier-less QT subject + those exact predicates) is checked first and everything else takes the spec desugaring `r rdf:reifies <<( s p o )>>`. Note the span-keyed `ReifiedCache` memo: one lexical occurrence = one reifier across `;`-continuations.
8. **Tests + registers** — `it_query_grouping.rs` (new, wired into `grp_query.rs:26`) carries the parity pins *and the divergence pins* (JSON-LD grouped-list and bind-as-constraint acceptance are pinned so the SPARQL rejections cannot silently leak across); `it_query_sparql_parse_errors.rs` (wired into `grp_query_sparql.rs:15`); `it_query_sparql_annotations.rs:1261+` proves `@annotation`-written data is byte-equal readable via the new reifier syntax vs the established `rdf:reifies` form; `testsuite-sparql/tests/registers/mod.rs` shrinks 524 → 403.

### Why this is the right depth of fix

Every component extends an existing shared mechanism rather than bolting on a parallel one: validation passes join the existing `validate()`/`DiagCode` taxonomy (new V001-V008/F010-F011 codes), GRAPH-in-DELETE-WHERE reuses the Modify lowering instead of growing a second graph-template path, and the reifier forms lower onto `Pattern::EdgeAnnotation`/`AnnotationTarget` with zero storage/index surface (D-1's accept-then-defer, with enum widths preserved by boxing — `ast/pattern.rs:205`, `ast/term.rs:300`).

### Performance

Neutral — **no engine crate is touched** (the diff has zero files under `fluree-db-query/`, `fluree-db-binary-index/`, `fluree-db-core/`; I verified the diffstat file list, not just the PR's claim). All new work is parse/validate/lowering/update-staging: once per query, never per row. The `ReifiedCache` HashMaps allocate nothing until a reified triple actually appears; `edge.clone()` per annotation unit is parse-time. The author's BSBM parse-entry numbers (HEAD ≤ base on q3/q5/q9, honestly caveated as noise-bounded) corroborate. No performance-degradation risk.

### Verification performed for this review

- `cargo fmt --all -- --check` at PR HEAD → **FAILS** on two PR-introduced hunks (`parse/query/tests.rs:2928`, `lower_sparql_update.rs:46`) with rustfmt 1.9.0-stable 2026-07-07 — the same stable CI installs. This is an enforced-gate failure (see review).
- `cargo nextest run -p fluree-db-sparql -p fluree-db-transact -p fluree-db-query` → 2,148 passed. `cargo nextest run -p fluree-db-api` → 2,410 passed, 11 skipped (feature-gated).
- Full W3C suite in `testsuite-sparql/` (submodule initialized): `cargo test` → **36/36 suites, 0 failed, 0 stale registers**, 15.6 s; `cargo clippy --all-targets -- -D warnings` there → clean; its `cargo fmt --check` fails only on the same two parent-crate files.
- Scoped clippy on the three touched crates → no denied-lint hits; the two warnings observed are **not** this PR's (`question_mark` at `fluree-db-transact/src/address.rs:15` is untouched pre-existing 1.97 drift; the `dead_code` on `resolve_shapes_source_g_ids` only appears without `--all-features` — its callers at `tx.rs:696`/`validate.rs:376` exist at base and HEAD).
- Register accounting recomputed from the files, not the PR body: base 524 → HEAD 403 entries = **net −121**; 126 removals include 3 moved entries (expr-tripleterm-03/04/05 re-bucketed), so 123 genuine removals and **2 new registrations** (`dawg-delete-insert-01b`, `update-reifier-08`) — both documented in-file as tests that were previously green for the wrong reason (silent bnode acceptance / first-op-only truncation) and are honestly un-greened here, owned by PR-U2/U3.
- Traced the seam's coverage: every public query path routes through `parse_and_validate_sparql` (via `parse_sparql_to_ir`); the transact string path checks `has_errors` at `tx_builder.rs:42`; `graph_transact_builder.rs:204` reuses it; the re-exported `lower_sparql_update` takes an already-parsed AST so it cannot bypass parsing.
- Verified all four test files are actually wired into `grp_*` binaries (this repo's `autotests = false` trap): `grp_query.rs:26`, `grp_query_sparql.rs:15`, `grp_transact.rs:28` (pre-existing), `grp_graphsource.rs:20` (pre-existing).
- Checked CI itself: **no CI-workflow run exists for this PR** — only the Release workflow's `plan` job ran on the pull_request event. The enforced fmt/clippy/nextest/W3C gates have not executed upstream; my local runs above are currently the only gate evidence.
- Commit hygiene: read all 26 full bodies — conventional-commit subjects with scopes, thorough multi-line rationale (e.g. `a58757310` documents the D-4 seam decision and why the AST-withholding alternative was rejected), zero AI-attribution trailers. V1/V2 land as isolated commits (`211535d1e`, `62b6ef06f`) for bisect/revert as promised.
