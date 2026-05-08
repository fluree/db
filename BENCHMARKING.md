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
