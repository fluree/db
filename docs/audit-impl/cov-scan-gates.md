# PR-COVERAGE — scan-side gate record

Branch `perf/audit-coverage` (base = `perf/audit-mem-guards` @ 1441e0038). Scan-side
section: audit items 7, 8, 10, 11, 12. Recorded by the scan-side implementer; the
fold-side implementer appends its section below. The single PR body cites this file.

CI does not fire on this branch (base ≠ main); this is the local reproduction of
record. The **perf arm is ADVISORY until PR-HARNESS re-blesses** at the final
integrated head — the committed perf baseline is 62% gate-blind (C2/RT2). The
correctness/hash arm is live: new members carry blessed native oracles (below).

Toolchain: env `cargo`/`clippy` (no repo pin). Two known-pre-existing quirks handled
per `db-verify-gotchas`: (1) `cargo fmt` also touches `hash_join.rs` (pre-existing
drift) — reverted, left out of this branch; (2) the `smoke_minio` **example** fails to
compile without `--features aws` (its own doc requires aws) and is broken on the base
too — excluded from the scoped gates via `--lib --tests --bins`.

## Scan-side gates (verbatim)

- **fmt:** `cargo fmt` then reverted the unrelated `hash_join.rs` hunk → the 26
  changed source files are fmt-clean (`cargo fmt --check` clean on them).

- **clippy (query):** `cargo clippy -p fluree-db-query --lib --tests --bins --no-deps`
  → no warnings/errors in any item-7/8/10/11 file (env clippy's pre-existing
  workspace lints are out of scope per db-verify-gotchas; CI main clippy has no
  `-D warnings`).

- **clippy (api, iceberg):** `cargo clippy -p fluree-db-api --features iceberg
  --all-targets --no-deps` → exit 0.

- **clippy (iceberg, aws):** `cargo clippy -p fluree-db-iceberg --features aws
  --lib --tests --bins --no-deps` → clean for item-10/12 files (examples excluded:
  `smoke_minio` is aws-doc'd and pre-existing-broken).

- **test (query):** `cargo test -p fluree-db-query --lib` → **1306 passed; 0 failed**.
  Full `cargo test -p fluree-db-query` (incl. grp_* integration bins) → exit 0.

- **test (iceberg, aws):** `cargo test -p fluree-db-iceberg --features aws --lib`
  → **246 passed; 0 failed**.

- **test (api, iceberg):** `cargo test -p fluree-db-api --features iceberg` → exit 0
  (live/`#[ignore]` tests skipped; hermetic tests pass).

- **test (bench-virtual):** `cargo test -p fluree-bench-virtual --bins` → exit 0;
  the 7 corpus meta-tests (incl. `shipped_corpus_is_valid` = 74 members,
  `rows_only_hash_gate_marks_nondeterministic_limits`) pass.

## New corpus members + blessed native oracles (item 7/8/10/11)

Promoted from the A3 probe battery; run `vbench run --targets native-sf01` (local
ledger `/Users/ajohnson/vbench/.fluree`, NO credentials — uses the on-disk iceberg
cache), blessed via `vbench baseline --expected`. **Native oracles only** — the live
virtual gate is PR-HARNESS's job.

| member | item | probe | native rows | hash (12) | exec | hash_gate |
|--------|------|-------|-------------|-----------|------|-----------|
| q069 filter_in_fkref       | 7  | probe-02  | 1100 | 67a2035630da | from | full |
| q070 scalar_values_glaccount | 7 | probe-03 | 0    | e3b0c44298fc | from | full |
| q071 asc_topk_order_total  | 8  | probe-01  | 10   | 7dc1c655cb0c | from | full |
| q072 timestamp_range_webevent | 10 | probe-07 | 5000 | 498045e221ce | from | rows_only |
| q073 optional_budget_order_customer | 11 | probe-04 | 50 | b86545e2b0e2 | from | rows_only |
| q074 limit_budget_control  | 11 (control) | probe-04b | 50 | 6e0f430cd272 | from | rows_only |

Item 12 (`read_ranges` parallel GETs) adds NO member by design — the existing cold
corpus members exercise the coalesced-fetch path; PR-HARNESS's cold-subset re-run is
its evidence.

## Notes carried to the PR body / SWITCHES.md (PR-HARNESS)

New kill switches (all default-on): `FLUREE_R2RML_IN_PUSHDOWN`,
`FLUREE_R2RML_IN_PUSHDOWN_MAX` (=64), `FLUREE_R2RML_TOPK_ASC`,
`FLUREE_ICEBERG_TIMESTAMP_STATS`, `FLUREE_R2RML_BUDGET_OPTIONAL`,
`FLUREE_ICEBERG_PARALLEL_RANGE_GETS`.

# PR-COVERAGE — fold-side gate record

Fold-side section: audit items 9, 9b, 14. Recorded by the fold-side implementer;
same toolchain + two known-pre-existing quirks (`hash_join.rs` fmt drift; the
`smoke_minio` aws-doc'd example) as the scan-side, handled identically.

## Fold-side gates (verbatim)

- **fmt:** `cargo fmt` then reverted the unrelated `hash_join.rs` hunk (the same
  pre-existing base drift the scan-side noted) → the fold-side source files
  (`fused_aggregate.rs`, `ledger_info.rs`, `corpus.rs`, `operator_tree.rs`) are
  fmt-clean.

- **clippy (query):** `cargo clippy -p fluree-db-query --lib --tests --bins
  --no-deps` → **exit 0**. This run SURFACED + FIXED a pre-existing scan-side
  `semicolon_if_nothing_returned` deny at `operator_tree.rs:3401` (item 8's ASC
  `set_topk` arm; the workspace `[lints.clippy]` denies that lint and the
  scan-side clippy run predated the arm) — a one-char `;`, no behavior change,
  so the whole branch is now clippy-green.

- **clippy (api, iceberg):** `cargo clippy -p fluree-db-api --features iceberg
  --all-targets --no-deps` → **exit 0**. One pre-existing `question_mark` warning
  at `ledger_info.rs:1626` (the `load_table` match, UNCHANGED by this PR — the
  env clippy-1.97 drift per db-verify-gotchas) — non-gating (not in the workspace
  deny set; CI main has no `-D warnings`).

- **test (query):** `cargo test -p fluree-db-query` (lib 1306 + all it_*/grp_*
  integration bins + doc-tests) → **exit 0**. Includes the new/updated
  fused-aggregate unit tests: `minmax_admissible_datatype_scope`,
  `slice_1_5_admits_and_applies_a_single_table_flag_constraint`,
  `multi_constraint_requires_all_to_match` (the D-c5 AND-semantics guard).

- **test (api, iceberg):** `cargo test -p fluree-db-api --features iceberg` →
  **exit 0** (live/`#[ignore]` skipped; hermetic tests pass, incl. the 3 new
  item-14 `ledger_info` tests: `info_member_routing_default_on_and_falsy_off`,
  `merge_virtual_into_native_unions_classes_graph_source_wins`,
  `mor_approximate_tables_flag_surfaces_in_source`).

- **test (bench-virtual):** `cargo test -p fluree-bench-virtual --bins` → **33
  pass**; `shipped_corpus_is_valid` now asserts **77 members** (74 + the 3
  fold-side members below).

## New corpus members + blessed native oracles (items 9, 9b)

Blessed via `vbench baseline --expected` against `native-sf01` (local ledger
`/Users/ajohnson/vbench/.fluree`, NO credentials). **Native oracles only** — the
live virtual re-bless is PR-HARNESS's job.

| member | item | shape | native rows | native oracle (hash 64) | note |
|--------|------|-------|-------------|-------------------------|------|
| q075 minmax_order_total       | 9  | ungrouped MIN/MAX (xsd:double) | 1 | 9e28b2469a7fe95b | orderTotal min+max, one implicit group |
| q076 minmax_order_total_by_channel | 9 | grouped MIN/MAX (4 channels) | 4 | ee98f6bfed407816 | one MinMax accumulator per group |
| q077 count_current_enterprise_customers | 9b | multi-constraint COUNT | 1 | 45a3f31410f08193 | isCurrent∧segment=Enterprise = 50038 (= q022's Enterprise current row); native took 133s (dev build; `timeout_s`=300) — the fused fold applies BOTH constraints in one scan |

The EXISTING members **q022/q038/q061** already gate the ungrouped + grouped
single-constraint constant-object COUNT: their native oracles are the CONSTRAINED
(materialized) counts, so a fused over-count fails them (q061's own comment is the
D-c5 tripwire — its former "MUST decline" premise is now "fuses WITH the constraint
applied", oracle unchanged).

## Notes carried to the PR body / SWITCHES.md (PR-HARNESS)

- Items **9 + 9b** ride the EXISTING `FLUREE_FUSED_R2RML_AGG` master switch as
  WIDENINGS (no new switch). Per A2's lesson this is documented explicitly:
  **switch-OFF reverts the widening too** — it reverts BOTH the MIN/MAX fold AND
  the constant-object constraint application (back to the pre-PR full-materialize
  decline). `FLUREE_FUSED_R2RML_AGG_JOIN` (the join sub-switch) is unchanged.
- Item **14** adds ONE new default-on switch **`FLUREE_R2RML_INFO_MEMBER_ROUTING`**
  (off = the strict `t == 0` reroute, the prior behavior). Needs a SWITCHES.md row.
</content>

# PR-HARNESS — gate record (audit Tier-1/hygiene, leaf on perf/audit-coverage)

Branch `perf/audit-harness` (base `perf/audit-coverage` @ `729cd686c`). Closes
F-AUD-18 (stale perf baseline), F-AUD-19 (kill-switch hygiene / SWITCHES.md), F-AUD-20
(resilience coverage), + the R-1522 count_shortcut_eligible rider (landed upstream as
`3ece996fd`; my redundant commit dropped on rebase). CI does not fire on this branch
(base ≠ main); this is the reproduction of record.

## THE LIVE GATE — verification of record for the whole stack

`vbench compare --run <merged> --gate` → **77 records, 0 hash mismatch(es), 0 perf
violation(s), exit 0.** Correctness passes end-to-end at the final integrated head
(#1521 mem-fix + #1522 coverage + this PR). The drift-set 300% overrides absorbed the
loadTable-GET network variance (0 perf violations). Accepted residual: those 4×-baseline
(300%-over) budgets on the 6 catalog-dominated drift members (q002/q004/q022/q024/q030/q043)
mean a sub-4× engine regression there won't trip the perf arm (the hash/correctness arm is
unaffected) — a documented anti-flap trade-off, re-narrowable if the catalog path stabilizes.

## Re-bless record (F-AUD-18)

- **native-sf01**: 54 → **77** entries, ZERO timeout caps (was N/A — native has no DNFs);
  all 74 oracles reproduced exactly (no native regression).
- **virtual-sf01**: was 54 entries blessed_from `7d77218e2` with **28 pinned at 180000ms
  (+2 at 120000ms, q044/q050)** (DNF caps blessed as budgets) + 14 missing; now **77 entries,
  ZERO at 180000/120000**. Six are honest no-baseline (`is_unblessable_wall`): q013/q034/q051
  (expected-virtual-error) + q056/q057/q059 (see finding). Every other member carries a real wall.

## Splice methodology (honest, documented)

The live re-bless is a SPLICE across two heads because the #1521 abort fix cascaded
after the full run. The blessed baseline = 76 valid records from the full 77-query run at
the pre-fix head `73a7694bf` + a post-fix single-flight re-run of the accounting-sensitive
subset {q038, q014, q069, q073, q075, q077} at `cd3779480`. The 71 non-subset records are
valid at the final head because the #1521 fix changes only the memory ACCOUNTING (it
affects exactly the members that were false-aborting, not the completing queries). All
three JSONLs ship in `audit-2026-07/data/`: `virtual-full-73a7694bf-prefix.jsonl`,
`virtual-splice-cd3779480-postfix.jsonl`, `virtual-rebless-merged.jsonl` (the bless/gate
input). All PAT-scrubbed (`grep -Ff` + secret-marker scan).

## New / changed member walls (live virtual-sf01, hot median)

| member | item | status | wall | note |
|--------|------|--------|------|------|
| q069 filter_in_fkref | 7 | ok | 325 ms | FK-IRI IN declines to prune (files_pruned=0), documented follow-on |
| q070 scalar_values_glaccount | 7 | ok | 1259 ms | scalar VALUES/IN **prunes 7670 files** |
| q071 asc_topk_order_total | 8 | ok | 250 ms | ASC scan-side top-k |
| q072 timestamp_range_webevent | 10 | ok | 127 ms | timestamp manifest pushdown |
| q073 optional_budget_order_customer | 11 | ok | 1991 ms | OPTIONAL budget forwarded (vs probe-04's 68,828× amplification) |
| q074 limit_budget_control | 11 | ok | 21 ms | control |
| q075 minmax_order_total | 9 | ok | 120 ms | ungrouped MIN/MAX fused |
| q076 minmax_order_total_by_channel | 9 | ok | 141 ms | grouped MIN/MAX fused |
| q077 count_current_enterprise_customers | 9b | ok | 64122 ms | multi-constraint COUNT fuses (streams 332 files / ~129M rows) |
| q038 count_current_customers | (9b class) | ok | 58154 ms | ungrouped filtered COUNT — fusion still DECLINES (see finding) |
| q022 current_customers_by_segment | fused-agg | ok | 87 ms | grouped filtered COUNT **fuses** |
| q061 …_by_segment_dataset | fused-agg | ok | 93 ms | FROM-path grouped **fuses** |

q077 = 64.1 s virtual vs 65.9 s native — the walls CONVERGE on the doubly-constrained
COUNT (both planner-bound; supersedes the earlier "virtual is the fast path" phrasing,
which compared against a 133 s dev-build native).

## FINDING — memory-abort false-positives (F-AUD-3 / #1521), now FIXED

The pre-fix full run aborted 4 members (q038, q056, q057, q059) with
`MemoryBudgetExceeded` (~8.6–9.4 GB) at the 8 GiB budget (macOS fallback) — false
positives of #1521's cumulative-no-decrement counter (it summed total-rows-streamed, not
resident memory, on long bounded-window streams). MEASURED: q038 completes in 52.5 s with
`FLUREE_SCAN_MEM_ACCOUNTING=off`. impl-mem's window-scoped release fix (`cfd773d75`) lands
in the cascade; the post-fix subset re-run **confirms q038 completes (58.2 s, no abort)** —
the fix's live confirmation. q056/q057/q059 (exploration-wildcard family) were NOT in the
lead's re-run subset, so they remain no-baseline from the pre-fix records (the fix would
let them complete too; a follow-up re-run would bless them — they are a known heavy
whole-warehouse-read family regardless).

The bless path guard was broadened this PR (`is_dnf_wall` → `is_unblessable_wall`) so an
error/abort wall (not just a DNF timeout) blesses as no-baseline — otherwise the abort
time would have been pinned as a budget.

## q038 fusion — PARTIAL, not CLOSED (F-AUD-8 ladder refinement)

q038 = `SELECT (COUNT(*)) WHERE { ?s a edw:Customer ; edw:isCurrent true }` uses the
constant-object TRIPLE form (not a SPARQL FILTER), identical to the fusing q077/q022. It
still materializes (58 s), so 9b did NOT admit its exact shape. Discriminator:
ungrouped-vs-grouped — q022 (no-FROM, GROUPED, same isCurrent) fuses at 87 ms; q038
(no-FROM, UNGROUPED COUNT(*)) declines. The matching decline is the empty-GROUP-BY cost
guard `if filter.is_some() && group_by.is_empty()` (`fused_aggregate.rs:441`), which fires
only if q038's constraint arrives as a residual FILTER rather than a folded
`star_constraint`. So the ladder's q038 "886× payoff" is **PARTIAL**: q077-class
(FROM-path multi-constraint) and grouped (q022) constant-object COUNTs fuse; q038's exact
ungrouped direct-path single-constraint form does not yet. A #1522 follow-up (one code
trace).

## Code gates (verbatim, at the final rebased head)

- **fmt:** `cargo fmt --check` exits 1 SOLELY on the pre-existing `hash_join.rs:1070/1120`
  base drift (inherited, deliberately left per db-verify-gotchas / cov-scan precedent). All
  changed files are fmt-clean.
- **clippy:** `cargo clippy -p fluree-bench-virtual -p fluree-db-query --all-targets
  --no-deps` → exit 0.
- **test (db-query):** `cargo test -p fluree-db-query` → exit 0
  (`count_shortcut_declines_constraints_filter_group_and_non_count` upstream,
  `collect_stream_stops_a_drain_loop_mid_sweep` this PR, E1 ON-path test).
- **test (bench-virtual):** `cargo test -p fluree-bench-virtual --bins` → exit 0, 35 passed
  (incl. `is_unblessable_wall_flags_non_completions_and_deadline_walls`,
  `write_perf_blesses_dnf_as_no_baseline_not_the_timeout_cap` w/ the error-abort case);
  `shipped_corpus_is_valid` = 77.
- **test (db-api, iceberg):** `cargo test -p fluree-db-api --features iceberg` → exit 0.

## Resilience coverage assessment (F-AUD-20)

Largely closed by the implementation PRs; PR-HARNESS adds the one genuine gap. Present +
passing: R3-B `r3b_scan_window_budget_aborts_typed` + `r3b_parent_build_budget_aborts_typed`
+ `shared_ceiling_trips_each_query_at_its_divided_budget` (#1521); PR-8 429
`retries_on_429_then_succeeds` + `gives_up_after_max_retries` + backoff-bound (wiremock);
C2 /info `serve_virtual_stats_only_for_empty_shell_graph_source` + `info_member_routing_*`
+ `merge_virtual_into_native_*` + `mor_approximate_tables_*` (#1522). Added: C3
`collect_stream_stops_a_drain_loop_mid_sweep`.
