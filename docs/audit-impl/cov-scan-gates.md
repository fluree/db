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
</content>
