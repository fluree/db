# FAMILY-C gate record (admit row-level FILTERs to the fused join aggregate)

Branch: `perf/family-c-filter-join` (base `perf/browse-parity` @ `d21001854`).
The widening lifts the join-path blanket filter decline in
`fluree-db-query/src/r2rml/fused_aggregate.rs::resolve_join_at_open` and ports the
single-table `FilterPlan` machinery to the join path (fact-side residual in the
fact fold; terminal-dim residual during the FK→GKey map build). Closes the two
deployed production DNFs (P4-famc-probe.md).

Recorded: 2026-07-21. All gates run LOCALLY (CI does not fire on a non-main base).
Rides `FLUREE_FUSED_R2RML_AGG_JOIN` as a widening — the join sub-switch OFF ⇒
`resolve_join_at_open` is never reached ⇒ a filtered join reverts to materialize.

## cargo fmt --check (my files) — CLEAN
`rustfmt --edition 2021 --check fluree-db-query/src/r2rml/fused_aggregate.rs
fluree-bench-virtual/src/corpus.rs` → exit 0. (The pre-existing `hash_join.rs`
whitespace drift noted in DEC-002 is NOT my file and was left untouched.)

## clippy — CLEAN for my code (--all-targets --no-deps)
`cargo clippy -p fluree-db-query --all-targets --no-deps` → 0 new lints, 0 errors.
The only warnings are the 5 PRE-EXISTING `doc list item without indentation` at
`fused_aggregate.rs:112-116` (documented in DEC-002; not in my added code, which
is clippy-clean including the tests).

## tests — 0 failed
- `cargo test -p fluree-db-query` (lib + integration) → **1319** lib passed (was
  1315 pre-change; +4 FAMILY-C unit tests) + all integration bins (correctness 2,
  datatype_coercion 16, groupby_aggregate 9, owl2rl 2, values_bind_union 13, …) —
  0 failed.
- `cargo test -p fluree-bench-virtual --bins` → **36** passed, 0 failed
  (corpus now validates at **87** members incl. q086/q087; smoke covers all tags).

New FAMILY-C unit tests (all pass), in `fused_aggregate.rs`:
- `family_c_route_filter_source_admits_and_declines` — admission matrix: both P4
  shapes route to the fact (Q1 `?status`; Q2 `?onHand`/`?reorder`); a dim attribute
  routes to the terminal dim; a fact+dim spanning filter, a variable bound as an
  object on two patterns, and an unbound variable all DECLINE.
- `family_c_build_filter_plan_projects_scalar_declines_ref` — the shared filter
  construction admits a scalar-column var (projecting its column) and DECLINES a
  `RefObjectMap` FK object var.
- `family_c_row_passes_filter_plan_null_excludes_and_compares` — THE D-c5 crux:
  `?category != "Electronics"` with a NULL category EXCLUDES the row (not "not
  Electronics"); `?onHand < ?reorder` with a NULL on EACH side excludes the row;
  genuine comparisons decide the rest.
- `family_c_constraint_and_filter_are_conjunctive` — a terminal dim row is kept
  iff it passes BOTH its folded flag constraint AND the routed filter (no
  over-count).

## no-native featureset gate (DEC-002 permanent addition) — PASS
`cargo check -p fluree-db-api --no-default-features --features aws,iceberg,shacl`
(solo's real combination) → Finished, exit 0.

## native oracles (offline, native-sf01, no creds) — BLESSED
`vbench run --targets native-sf01 --queries q086,q087` then `baseline --expected`:
- q086: 3 rows, `result_hash 69006c4ffd39…` — byte-identical to P4's measured p1
  native hash (Consumer 10788, Enterprise 2639, SMB 2683).
- q087: 10 rows, `result_hash d7aba410bb32…` — byte-identical to P4's measured p2
  native hash (10 category AVG rows).
The native path does not touch the fused R2RML join, so these oracles are
unchanged by this widening — they are the materialized ground truth the virtual
fused path must reproduce.

## LIVE confirm (virtual-sf01, single-flight, 1 rep, PAT in-memory) — PARITY, no violations
Release `vbench run --targets virtual-sf01 --queries q038,q065,q086,q087
--virtual-reps 1` then `compare --run … --gate` → **4 records, 0 hash mismatches,
0 perf violations** (exit 0).

| q | shape | before (P4 virtual) | fused wall | speedup | rows | hash == native oracle |
|---|-------|--------------------|-----------|---------|------|-----------------------|
| q086 | Q1: tickets⋈customer, fact-side `!=` FILTER | ~10.3s (declined→materialize) | **258 ms** | ~40× | 3 | yes (`69006c4ffd39…`) |
| q087 | Q2: inv⋈product, var-to-var FILTER, COUNT+2×AVG | ~5.0s (declined→materialize) | **158 ms** | ~32× | 10 | yes (`d7aba410bb32…`) |
| q065 | REGRESSION: orders⋈current-customer join+flag (no FILTER) | fused | 310 ms | — | 3 | yes (unchanged) |
| q038 | REGRESSION: F1 ungrouped filtered-COUNT | fused | 28 ms | — | 1 | yes (unchanged) |

Both new members collapse from seconds to sub-second and hash-MATCH the native
(materialized) oracle byte-for-byte — including q087's AVG decimals — proving the
fused fold fired AND is exactly correct (a declined-materialize path would still
take seconds on 430K/337K materialized rows). The two regression sentinels are
unchanged. `scan_table` span counts were not captured (this run had
`FLUREE_BENCH_TRACING` off; the warm run elides the scan-plan span — P4 §2), so
the wall + exact hash are the fire/decline discriminators, and both are decisive.
PAT held in-memory only (`$(cat ~/.vbench/snowflake-pat.txt)`), never written; no 401.

## R-1528 review — SHIP verdict + two nonblocking hardening items (2026-07-21)

Review verdict: SHIP, zero blocking (null semantics sound on all four attack
sub-points; no admitted-and-wrong shapes; the shared `build_filter_plan`
extraction confirmed byte-identical). Two hardening items landed on this branch:

1. **Fact-side NULL defense (fail-safe symmetry).** `next_batch`'s fact filter now
   routes through the SAME `row_passes_filter_plan` as the dim side — one filter-eval
   path for single-table + fact-join + dim. A NULL filter-member column excludes the
   row explicitly (`None → false`) instead of via a demotable Unbound. This is
   unreachable today (`validity_cols` null-drops the member first) and
   behavior-identical on every reachable input (`materialize_object_from_batch` over a
   scalar-column ObjectMap — all `build_filter_plan` emits — never returns Err), so it
   is a pure fail-safe against future erosion of the validity invariant. Test:
   `family_c_fact_filter_null_member_excludes_failsafe` (Q1's STATUS shape, bypasses
   validity by calling the helper directly).

2. **Duplicate-parent-key guard.** `insert_dim_gkeys` now DECLINES on ANY duplicate
   parent join key (was: kept an equal-value duplicate). A non-unique parent key means
   the materialized join fans out, which the single-entry-per-key map cannot represent
   — the previously-"harmless" equal-dup is a latent fan-out under-count. Proper star
   schemas have unique parent PKs, so this never fires there (no corpus regression).
   Test: `dim_dup_join_key_always_declines` (deliberately non-unique fixture).

Skipped the reviewer's item (1) (default-ON under the existing join switch) — DEC-002
policy as documented, no change.

Gates re-run (`-p fluree-db-query`): fmt clean; clippy 0 new lints (5 pre-existing
doc-list only); `cargo test -p fluree-db-query` → **1320** lib (+1 fact-null test) +
all integration bins, 0 failed; fused_aggregate module 33 tests pass.
