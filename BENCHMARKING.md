# Benchmarking

Performance benchmarks for the Fluree DB workspace. Use this doc to:

- learn what benches exist and what hot paths they cover,
- run them locally,
- read criterion's output and understand regression budgets.

For *adding* a new bench (or a new bench category), see
[`docs/contributing/benches.md`](docs/contributing/benches.md). The
chassis is documented in
[`fluree-bench-support/README.md`](fluree-bench-support/README.md).

## Running benches

```bash
# Run every bench in the workspace (long; uses default Quick profile).
cargo bench

# One specific bench at default scale:
cargo bench -p fluree-db-api --bench insert_formats

# Quick validation — single iteration, no statistics, useful for "did I
# break something":
cargo bench -p fluree-db-api --bench insert_formats -- --test

# Bigger inputs:
FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench insert_formats

# Full criterion sample counts (nightly profile):
FLUREE_BENCH_PROFILE=full cargo bench -p fluree-db-api --bench insert_formats

# Tracing on (writes spans to stderr; useful for debugging slow scenarios):
FLUREE_BENCH_TRACING=1 cargo bench -p fluree-db-api --bench insert_formats
```

## Env vars

| Var | Values | Default | Effect |
|---|---|---|---|
| `FLUREE_BENCH_PROFILE` | `quick` \| `full` | `quick` | sample-count + warmup discipline |
| `FLUREE_BENCH_SCALE` | `tiny` \| `small` \| `medium` \| `large` | `small` | per-bench input size |
| `FLUREE_BENCH_TRACING` | `1` (or unset) | unset | install a stderr tracing subscriber |
| `FLUREE_BENCH_RUNTIME` | `multi` (or unset) | single-threaded | tokio runtime shape |
| `RUST_LOG` | tracing-subscriber filter | `info` when `FLUREE_BENCH_TRACING=1` | tracing levels per crate |

## Current benches

Hand-maintained; add new entries when introducing a bench file.

| Crate | Bench file | Topic |
|---|---|---|
| `fluree-db-api` | `insert_formats.rs` | JSON-LD vs Turtle insert throughput, matrix of (format × txn count × nodes/txn) |
| `fluree-db-api` | `vector_query.rs` | End-to-end vector similarity through the query engine, 1K/5K articles, 768-dim |
| `fluree-db-api` | `fulltext_query.rs` | Full-text query through novelty + index |
| `fluree-db-api` | `import_bulk.rs` | Bulk Turtle import via `fluree.create(id).import(path).execute()`; single- vs default-threaded |
| `fluree-db-api` | `transact_commit.rs` | Single-commit latency on a fresh and a populated ledger (`iter_batched` setup) |
| `fluree-db-api` | `query_cold_reload.rs` | File-backed cold reload (load only, and load + first query) |
| `fluree-db-api` | `reindex_full.rs` | `Fluree::reindex(...)` end-to-end against a single-txn populated ledger |
| `fluree-db-api` | `reindex_incremental.rs` | Orchestrator's incremental path via `Fluree::trigger_index(...)` over delta novelty |
| `fluree-db-api` | `novelty_replay.rs` | Cold reload with `without_indexing()` so populate stays in novelty; scaled by commit count |
| `fluree-db-api` | `query_hot_bsbm.rs` | Warm-cache SPARQL: BSBM-shape Q3/Q5/Q9 against a reindexed ledger |
| `fluree-db-query` | `vector_math.rs` | SIMD vs scalar dot/L2/cosine micro-bench |
| `fluree-db-spatial` | `spatial_bench.rs` | S2 covering build + within/intersects/radius latency |

## Reading criterion output

Each bench produces a console line per scenario like

```
insert_formats/jsonld/100txn_10nodes
                        time:   [184.59 ms 188.42 ms 192.71 ms]
                        thrpt:  [129.32K elem/s 132.34K elem/s 135.10K elem/s]
```

The triple in `time` is `[lower_bound mean upper_bound]` of a 95%
confidence interval. `thrpt` is the throughput unit chosen by the bench
(elements/sec, bytes/sec, etc.).

Criterion also emits an HTML report at
`target/criterion/<group>/<bench>/report/index.html`. Open it to see
plots and prior-run comparisons.

## Regression budgets

`regression-budget.json` at the workspace root sets the per-bench, per-scale
percentage regression that CI's gated job will accept once the gate is in
its final shape. The default is 5% for any (crate, bench, scale) tuple
not explicitly listed.

### CI gate — phases

The gate runs across `ci.yml` (per-PR) and `bench.yml` (nightly +
on-demand):

1. **Reconcile + smoke (`bench.yml` `bench-gate`, nightly).**
   - **Reconcile.** `cargo test -p fluree-bench-support --test workspace_reconcile`
     asserts every `[[bench]]` declared in a workspace member's `Cargo.toml`
     has a matching entry in `regression-budget.json`, and vice versa. A
     missing or stale entry fails with a message naming the `crate/bench` pair.
   - **Smoke.** `cargo bench --workspace -- --test` runs each bench's
     scenarios once at `tiny` scale — catches benches that compile but panic
     at runtime (bad SPARQL, broken setup, missing API surface).
2. **Per-PR compare (`ci.yml` `bench-compare`).** Runs the cheap subset
   (`query_overlay_matrix` + `query_hot_bsbm`) at `tiny`/`quick` and compares
   against the committed baseline via the `bench-baseline` bin. **Both metrics
   enforce only when the baseline's `runner_class` matches the runner's; against
   a cross-class baseline both are advisory** (see
   [Baselines](#baselines-capture--compare) for why). Today's committed baseline
   is `runner_class=local`, so on `ubuntu-latest` the job currently annotates and
   exits 0. The nightly `bench-gate` runs the same compare over a larger sample.

   The job costs ~30 minutes, so it is gated on a `bench-paths` job that skips it
   when a PR touches nothing perf-relevant (engine crates, bench crates,
   `bench-baselines/`, `regression-budget.json`, `Cargo.toml`/`Cargo.lock`, or the
   CI/bench workflows). The list errs inclusive: a false positive costs one bench
   run, a false negative lets a regression through.

3. **CI-class capture (`bench.yml` `bench-capture`, `workflow_dispatch`).**
   Captures the cheap subset on `ubuntu-latest` (`runner_class=ci-ubuntu-latest`)
   and uploads it as an artifact; committing it lands a CI-class baseline that
   flips **both** metrics to enforcing, with no code change.

> **Visibility gap (documented; fix is a follow-up).** The per-PR gate only
> *compiles* benches — `clippy --all --all-features --all-targets` in `ci.yml`
> builds every bench, and `bench-compare` runs only the two cheap ones. Every
> other bench is *executed* nightly-only (`bench.yml`), visible to the last
> default-branch committer rather than to the PR that introduced a break. So a
> bench that compiles cleanly but panics at runtime — bad SPARQL, or a setup
> that matches zero data — sails through every PR and only reddens the nightly.
> This is exactly how the `query_hot_whole_graph_agg` `@vocab` bug reached main
> (its class scenarios matched zero nodes; the filtered-histogram sanity assert
> panicked). Two consequences: historical numbers for that bench's
> `SCALARS_CLASS`/`HISTOGRAM_CLASS` scenarios predate the fix, measured empty
> scans, and are void — re-baseline them the first time a committed baseline
> includes `whole_graph_agg` (the current committed baseline covers only the
> cheap subset, which is unaffected); and whether to promote a tiny `-- --test`
> runtime smoke into per-PR CI is an open follow-up, costed against the same
> budget that keeps the compare subset cheap.

To intentionally accept a regression (or tighten a budget), edit
`regression-budget.json` in the same PR and explain in the PR body.

## Baselines: capture & compare

Committed performance reference points live in
[`bench-baselines/`](bench-baselines/README.md) as one JSON file per phase
reference (`guardrails-pre.json`, …), captured and compared by the
`bench-baseline` bin. (This supersedes the earlier single `bench-baselines.json`
scheme — it is now a directory plus a bin.)

**Capture** — run the benches, then snapshot criterion's estimates plus the
memory sidecars into a git-stamped baseline:

```bash
cargo bench -p fluree-db-api --bench query_overlay_matrix
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label guardrails-pre --out bench-baselines/guardrails-pre.json
```

The baseline records provenance: `git_sha`, `captured_at`, `profile`, `scale`,
and — load-bearing for the gate — `runner_class` (env
`FLUREE_BENCH_RUNNER_CLASS`, default `local`) and `host`. The `"pre"` reference
for a phase is captured at the branch's **merge-base** (see
`bench-baselines/README.md`).

**Compare** — after a change, rerun the benches and compare:

```bash
cargo run -p fluree-bench-support --bin bench-baseline -- \
    compare --baseline bench-baselines/guardrails-pre.json [--only <substr>] [--advisory]
```

Each scenario present in both runs is checked against `baseline × (1 +
budget_pct/100)` from `regression-budget.json`, for both wall-clock time
(criterion `mean`) and peak memory (`peak_bytes`). **One rule covers both
metrics:** they enforce when the baseline's `runner_class` matches the comparing
runner's, and are **advisory** (`::warning::` annotations, non-gating) when it
doesn't. `--advisory` forces advisory for both.

| baseline vs runner class | time breach | peak_mem breach | exit |
| --- | --- | --- | --- |
| matched | fails the gate | fails the gate | 1 |
| mismatched | `::warning::` | `::warning::` | 0 |

### Why the gate has two phases

Neither metric survives a cross-machine comparison. `ubuntu-latest` shared
runners flap, so a 5–10% threshold on a single PR run comparing absolute
nanoseconds against a baseline captured on different (local Apple-silicon)
hardware false-positives every few PRs. Peak memory is steadier but not
portable either: allocator behaviour, page size, and background load all move a
tracking allocator's peak, and the ±2.2% local-vs-CI figure this gate was first
built on is one measurement between two specific machines — not a portability
result. Gating on it while merely warning on time would also put the *less*
portable of the two signals in the blocking position.

So the gate runs in two phases:

- **Phase 1 (today).** The committed `guardrails-pre.json` is
  `runner_class=local`, CI runs as `ci-ubuntu-latest`, the classes don't match,
  and both metrics annotate without gating. The job still earns its keep: the
  annotations surface real movement on the PR that caused it, and the compare
  itself exercises the capture/compare machinery.
- **Phase 2.** Commit a CI-class baseline and both metrics start gating on the
  next PR, automatically — the rule reads the baseline's `runner_class`, so
  there is no code or workflow change to make.

Two ways to reach phase 2:

1. **Committed CI-class baseline (implemented).** Run `bench.yml`'s
   `workflow_dispatch` `bench-capture` job, download the artifact, and commit it
   — its `runner_class=ci-ubuntu-latest` matches CI, so both metrics enforce
   automatically.
2. **Same-runner interleaved base-vs-HEAD (documented future option).** Measure
   merge-base and HEAD in the *same* job on the *same* runner and compare those,
   instead of any committed cross-machine file — the only design that survives
   shared-runner variance at a tight time budget. Deliberately not wired: it
   doubles the per-PR build cost, the exact eviction risk that had removed an
   earlier per-PR compare.

The nightly reconcile + smoke still catches the regressions that don't depend on
baseline comparison: API breakage, panics, missing budgets.

> **Void pre-fix numbers — `query_hot_whole_graph_agg`.** Before the `@vocab`
> fix in this PR, that bench's class-anchored scenarios (`scalars/class`,
> `histogram/class`, `histogram/class_filtered`) matched zero nodes and silently
> measured *empty* results — so any historical numbers for them are void and
> must be re-baselined the first time a committed baseline includes that bench.
> The committed `guardrails-pre.json` subset is `query_overlay_matrix` +
> `query_hot_bsbm` **only** — it does not include `query_hot_whole_graph_agg` —
> so there is no collision, and the recaptured baseline is unaffected.

## Architecture

The bench chassis lives in
[`fluree-bench-support`](fluree-bench-support/README.md):

- `init_tracing_for_bench()` — opt-in tracing subscriber.
- `next_ledger_alias(prefix)` — atomic unique-alias generation.
- `bench_runtime()` — tokio runtime with bench-friendly defaults.
- `BenchProfile`, `BenchScale` — env-driven knobs.
- `gen::*` — deterministic data generators (people graphs, vectors,
  paragraphs).
- `fixtures::*` — vendored / fetched fixture loaders.
- `budget::*` — regression-budget loader and `check()` helper.
- `report::*` — opt-in human-readable end-of-run summary tables.

Benches start from `fluree-bench-support/templates/BENCH_TEMPLATE.rs` and
reuse these helpers rather than reimplementing them. See
[`docs/contributing/benches.md`](docs/contributing/benches.md) for the
six-step workflow to add one.

## Tracing inside a bench

A bench that wants per-stage timings (e.g., bulk import: parse → chunk →
resolve → root-build → publish) can run with
`FLUREE_BENCH_TRACING=1` and inspect the stderr output. The eventual
`FLUREE_BENCH_TRACING=file:./out.json` mode (handled by `BenchSpanLayer`)
will dump JSON spans for offline analysis; that mode is reserved today and
falls back to stderr until it ships.

For tracing conventions inside the database itself (where to put
`debug_span!` vs `trace_span!`, how to use `.instrument()` safely across
`.await`), see [`docs/contributing/tracing-guide.md`](docs/contributing/tracing-guide.md).

## Where benches live

```
fluree-bench-support/        # chassis (helpers, generators, templates, fixtures)
<crate>/benches/<name>.rs    # one file per bench; criterion harness=false
regression-budget.json       # per-bench gate at the workspace root
.github/workflows/ci.yml     # gated bench job (per-PR, lands in bench-5)
.github/workflows/bench-nightly.yml   # full sweep (lands in bench-nightly PR)
```

## Troubleshooting

- **"could not find `Cargo.toml` in `…`"** — run `cargo bench` from the
  workspace root or pass `-p <crate>` to scope to a specific crate.
- **A bench compiles but `cargo bench --bench X` says "no benchmark named X"** —
  check that the crate's `Cargo.toml` has a matching `[[bench]] name = "X"`
  entry.
- **Regression budget fails with no obvious cause** — re-run with
  `FLUREE_BENCH_PROFILE=full` to widen the sample. If still flaky, the
  budget for that bench/scale needs raising; edit the JSON and explain
  in your PR.
- **Tracing output appears in CI but not locally** — set
  `FLUREE_BENCH_TRACING=1` explicitly. CI may set it; local runs do not.
