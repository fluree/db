# edge-annotations → 4-PR separation plan

Source of truth (never edit): `feature/edge-annotations` @ f41512846
Backup tag: `backup/edge-annotations-20260619`
Verifiable base (main commit already merged into branch): **478dde21b** (`c6c304a0d^2`)
Latest main: **2fd03495e** — PR-1 REBASED onto it cleanly (verified conflict-free; main
drift touched ZERO fluree-db-query files). Stack now builds on latest main; no final
rebase needed. PR-1 is PR-able against origin/main as-is.
Tree-equality proof adjusts: compare pr/4 vs (edge-annotations rebased on 2fd03495e).

No Claude mention in any commit or PR (no Co-Authored-By, no generated footer, no commit/CI lists).

## Stack — REVISED to 3 PRs (user chose to bundle operators into PR-2)

```
2fd03495e (latest main)
 └ pr/1-query-planner   ✅ DONE/MERGED-PENDING (PR #1356) — general planner fixes
   └ pr/2-rdf12-annotations   RDF 1.2 edge annotations + GQL operators
                              (unwind/shortestPath/collect) + JSON-LD surface
     └ pr/3-cypher           fluree-db-cypher crate + wiring + cypher-json + csv
```

PR-2 = edge-annotations MINUS the cypher front-end. NO operator strip (operators
are bundled). Exclude only: fluree-db-cypher/*, api/cypher_write.rs,
api/format/cypher.rs, transact/lower_cypher_update.rs, api/csv_import.rs (neo4j),
cypher tests, fluree-bench-support/normalize.rs, cypher routing/deps in api/transact/server/cli.

PR-2 COMMITTED: c7af18607 (174 files, +32.3k/-648) on pr/2-rdf12-annotations.
Built via: merge origin/main into temp copy of edge-annotations (temp-ea-on-main,
recursive merge — reliable, 1 conflict in tx_builder.rs resolved to main's refactor),
then laid that tree onto pr/1 and stripped the cypher front-end.
Compiles workspace-wide (all targets/features); RDF 1.2 + operator tests pass (95).

REMAINING for PR-2 before it's review-ready:
- JSON-LD parse surface for the GQL operators (unwind/shortestPath/collect) — their
  only front-end was cypher (stripped to PR-3), so they're currently unreachable from
  JSON-LD. Need parse surface + it_query.rs tests so they're live + non-dead.
- DEFERRED to PR-3 (cypher): conditional-Cypher-write resolver must be re-integrated
  into main's refactored tx_builder stage_under_lock (tx_builder conflict took main's side).

temp-ea-on-main branch kept as the source for PR-3 (cypher front-end); delete at end.

## Correctness guarantee
After PR-4: `git diff pr/4-cypher feature/edge-annotations` must be empty
(modulo intentionally-dropped diag churn). That proves zero work lost.

## PR-1 — ✅ DONE & VERIFIED (branch pr/1-query-planner @ 150b235e4, squashed to 1 commit)
8 files, +1031/-60, all in fluree-db-query + 1 JSON-LD test. Compiles standalone on
bare main; clippy clean; it_query_{negation,aggregates,jsonld,optional_hashjoin} all pass.
Folds: object→subject hash-join-after-producer (5db3688b0), connectedness source seeding
(981db4f22), subquery producer placement (72ccf853d), correlated-OPTIONAL hash-left-join
(f41512846), EXISTS-as-projected/BIND (6d2eec5bc).

Surgery done: excised annotation-coupled `is_broad_annotation_sidecar` (REIFIES_* vocab)
→ PR-2. Pulled 72ccf853d for `subquery_output_vars` dep (general sub-SELECT, not cypher).
Rewrote it_query_optional_hashjoin.rs cypher→JSON-LD (optional varargs form: `["optional",{a},{b}]`).

DEFERRED out of PR-1:
- 7f09ec855 alternation-transitive paths → PR-3 (drags reifies firewall + Unwind/ShortestPath IR).
- 829795412 deterministic DefaultGraphSource → PR-2 (annotation infra).

Lesson: fixes discovered via Cypher/LDBC carry cypher-coupled tests; PR-1 carries
SPARQL/JSON-LD equivalents instead.

## Mechanism
- Cleanly-owned files → `git checkout edge-annotations -- <path>`
- 7 shared files (planner.rs +614, ir/pattern.rs +202, aggregate.rs, groupby.rs, sort.rs, binding.rs, parse/lower.rs) → hunk-level reconstruction by hand
- PR-1 atomic commits → cherry-pick where clean (validated: f41512846 applies clean), edit otherwise

## Manifest: /tmp/manifest.tsv  (hash<TAB>crates<TAB>subject, 202 our-commits)
New-file bucket map: see conversation. Cypher crate is new+isolated; entanglement is in the 7 shared files.
