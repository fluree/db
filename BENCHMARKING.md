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
| `fluree-db-api` | `query_overlay_matrix.rs` | Same query shapes at three ledger conditions — `base` (indexed, epoch 0), `overlay` (indexed + trailing novelty), `novelty` (no index) — with per-scenario memory metrics. The overlay-merge regression gate. |
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

### CI gate — two phases

The gate runs in two phases, defined separately in CI:

1. **`bench-gate` (this PR's contribution)** — runs on every PR and push to
   `main`. Two checks:
   - **Reconcile.** `cargo test -p fluree-bench-support --test workspace_reconcile`
     asserts every `[[bench]]` declared in a workspace member's `Cargo.toml`
     has a matching entry in `regression-budget.json`, and vice versa. A
     missing or stale entry fails the gate with a message naming the
     `crate/bench` pair to fix.
   - **Smoke.** `cargo bench --workspace -- --test` runs each bench's
     scenarios once at `tiny` scale. Catches benches that compile but
     panic at runtime (bad SPARQL, broken setup, missing API surface).

   This phase **does not** compare against runner-stable baselines, so a
   2× regression that still completes successfully won't fail the gate.
   That comparison is phase 2.

2. **`bench-nightly` (separate PR — `bench-nightly`)** — runs on a cron
   schedule with `FLUREE_BENCH_PROFILE=full` against the canonical
   `bench-baselines.json` committed in the repo. Compares observed
   nanoseconds to `baseline × (1 + budget_pct/100)` for each
   `(crate, bench, scale)` tuple and fails the job if any bench exceeds
   its budget.

To intentionally accept a regression (or tighten a budget), edit
`regression-budget.json` in the same PR and explain in the PR body.

## Baselines: capture & compare

`bench-baseline` (a bin in `fluree-bench-support`) turns "benchmark before
and after a change" into a mechanical workflow. It backs the per-phase
performance gate defined in
[`docs/audit/2026-06-architecture-audit.md`](docs/audit/2026-06-architecture-audit.md)
(Phase 0.0): capture a labeled baseline **before** starting a refactor
phase, compare against it on every PR in the phase, and validate at phase
close.

```bash
# 1. Run the benches you care about (criterion writes target/criterion/):
FLUREE_BENCH_SCALE=small cargo bench -p fluree-db-api --bench query_overlay_matrix

# 2. Capture a labeled, git-stamped baseline:
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label phase-1-pre --out bench-baselines/phase-1-pre.json

# 3. ...make changes, rerun the same benches under the same env knobs...

# 4. Compare. Prints regressions AND improvements; exits 1 on any breach:
cargo run -p fluree-bench-support --bin bench-baseline -- \
    compare --baseline bench-baselines/phase-1-pre.json

# PR-scoped subset (only scenario IDs containing the filter):
cargo run -p fluree-bench-support --bin bench-baseline -- \
    compare --baseline bench-baselines/phase-1-pre.json --only query_overlay_matrix
```

Details:

- Scenario IDs are criterion's layout joined with `/` —
  `<group>/<function>/<scale>` — and the scale segment maps comparisons
  onto `regression-budget.json` budgets via the same machinery as
  `budget::check()`.
- Baselines record provenance (label, short git sha, timestamp, profile
  and scale env at capture time). Compare like against like: same
  profile, same scale, same machine class. `quick` profile for direction,
  `full` for decisions.
- Scenarios in the baseline but not rerun are reported as "not rerun" and
  do **not** fail the gate — subset runs are expected on PRs. Breaches
  fail with exit 1.
- Phase reference baselines live in `bench-baselines/` and are committed
  with the first PR of a phase so reviewers and CI share the reference.
  Improvements found at phase close get banked by tightening the budget
  in `regression-budget.json` in the closing PR.

## Memory metrics

Criterion measures wall-clock only. Benches that also want allocation
metrics install the tracking allocator from `fluree-bench-alloc` (a
dedicated crate so `fluree-bench-support` keeps `#![forbid(unsafe_code)]`)
and record per-scenario peak / total allocation:

```rust
use fluree_bench_alloc::TrackingAllocator;

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator::new();

// around a scenario:
fluree_bench_alloc::reset_peak();
group.bench_with_input(/* ... */);
let m = fluree_bench_alloc::snapshot();
fluree_bench_support::mem::record_scenario("my_bench", "my_bench/q1/small",
    MemMetrics { peak_bytes: m.peak_bytes as u64,
                 total_allocated_bytes: m.total_allocated_bytes as u64 });
```

Metrics land in `target/fluree-bench-mem/<bench>.json` sidecars;
`bench-baseline capture` merges them into the baseline and `compare`
gates memory with the same per-bench budgets as time. The gated metric
is `scenario_mem` (`scenario_peak_bytes`: the high-water mark *minus*
live bytes at scenario start) — absolute peak includes the ambient
process baseline, which a recompile can shift by megabytes uniformly
across every scenario, so it is recorded but only used as a fallback
when comparing against baselines captured before the field existed.
`total_allocated_bytes` (churn) is recorded for analysis but not gated —
it scales with criterion's adaptive iteration counts. The tracker costs
two relaxed atomics per alloc; that overhead is identical between a
baseline and a comparison run of the same bench, so deltas stay valid —
but don't compare a tracking bench's absolute times against a
non-tracking bench. `query_overlay_matrix` is the first memory-aware
bench; opt others in as needed.

### Why two phases

`ubuntu-latest` shared runners flap; a 5% threshold on a single PR run
would produce false positives every few PRs. Phase 2 amortizes noise
across the nightly's larger sample (`Full` profile = ~30 samples per
bench) and uses dedicated 4-core runners for stability. Phase 1 catches
the regressions that don't depend on baseline comparison: API breakage,
panics, missing budgets.

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
