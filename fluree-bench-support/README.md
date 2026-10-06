# `fluree-bench-support`

Shared chassis for Fluree DB benchmarks: helpers, deterministic generators,
fixture loading, regression-budget validation, and bench-mode tracing.

This crate is a `dev-dependency` of every `fluree-db-*` crate that hosts
benches. It exists so the duplicated patterns observed across the five
pre-2026-05 benches — verbatim `init_tracing_for_bench()` blocks, atomic
ledger-alias counters, hand-rolled type aliases — live in one place.

For orientation and per-bench documentation, see [`BENCHMARKING.md`](../BENCHMARKING.md)
at the workspace root and [`docs/contributing/benches.md`](../docs/contributing/benches.md)
for the contributor guide.

## API surface

### Top-level (re-exported from `lib.rs`)

| Item | Purpose |
|---|---|
| `init_tracing_for_bench()` | Install a tracing subscriber if `FLUREE_BENCH_TRACING` is set. Idempotent; off by default. |
| `next_ledger_alias(prefix)` | Atomic, never-reused alias of the form `bench/{prefix}-{n}:main`. |
| `bench_runtime()` | Tokio runtime for `b.iter(\|\| rt.block_on(...))`. Single-threaded by default; set `FLUREE_BENCH_RUNTIME=multi` to switch. |
| `BenchProfile`, `current_profile()` | `Quick` (PR-gated) vs `Full` (nightly). Read from `FLUREE_BENCH_PROFILE`. |
| `BenchScale`, `current_scale()` | `Tiny` / `Small` / `Medium` / `Large`. Read from `FLUREE_BENCH_SCALE`. |

### `tracing` module

Tracing init plus a stub `BenchSpanLayer` for span-capture-to-file (full impl lands later).

### `runtime` module

`BenchProfile` and `BenchScale` enums with `from_env_str` parsing,
`elements_default()` size helpers, and `bench_runtime()`.

### `ledger` module

`next_ledger_id()` and `next_ledger_alias(prefix)`.

### `gen` module

Deterministic data generators reused across benches. Each generator is
byte-identical across runs given the same parameters.

| Submodule | Source | Used by |
|---|---|---|
| `gen::vectors` | lifted from `vector_math.rs` and `vector_query.rs` | vector benches |
| `gen::corpora` | lifted from `fulltext_query.rs` | full-text benches |
| `gen::people` | lifted from `insert_formats.rs` | insert/transact benches |
| `gen::bsbm` | new in chassis | `query_hot_bsbm` and any future bench wanting multi-hop join + filter + aggregate query patterns |

### `fixtures` module

Workspace-root `fluree-bench-support/fixtures/` resolution. The
`load_or_generate(name, scale)` entry point is a stub today; vendored
fixture loading and remote fetch are tracked under the `bench-nightly`
follow-up.

### `budget` module

`RegressionBudget` schema + loader for `regression-budget.json` at the
workspace root. The `check(...)` helper compares observed nanoseconds to
`baseline * (1 + budget_pct/100)` and returns a `BudgetViolation` on
failure. The bench/budget reconciler runs as the `workspace_reconcile`
integration test (`fluree-bench-support/tests/workspace_reconcile.rs`)
and is invoked by the `bench-gate` CI job — it is not exposed as a
library function.

### `report` module

`SummaryRow` + `print_summary(title, rows)` for opt-in human-readable
end-of-run tables. Useful when criterion's HTML output doesn't surface the
domain-specific cross-scenario comparison a bench wants.

### `mem` and `meta` modules — sidecars

Criterion records one number per scenario: wall-clock. Anything else a bench
knows has to be written down beside it, keyed by the same
`<group>/<function>/<scale>` ID criterion uses, so that
`bench-baseline capture` can join the rows.

| Module | Sidecar dir | Carries |
|---|---|---|
| `mem` | `target/fluree-bench-mem/` | `MemMetrics` — scenario-attributable peak and total allocated bytes (needs `fluree-bench-alloc` installed in the bench binary) |
| `meta` | `target/fluree-bench-meta/` | `CorpusInfo` — what the scenario read (sha256, byte length, element count, input/output syntax, thread count) — and `PhaseTiming` per pipeline phase |

Both are best-effort: a bench must not fail because a sidecar write did.
Build phase timings with `ScenarioMeta::with_phase_ns`, which derives each
`share_pct` from the nanoseconds so the two can't disagree.

### `baseline` module

`BaselineFile` capture/compare behind the `bench-baseline` bin, including the
host block, the refusal rules (host class, corpus identity, schema version),
share-drift gating, and `NoiseStats` median + MAD accumulation. See
[`BENCHMARKING.md`](../BENCHMARKING.md) ("Baselines: capture & compare") for
the operator-facing workflow.

## Templates

`templates/BENCH_TEMPLATE.rs` is a working bench skeleton with `// TODO`
markers. Copy it, rename, fill in scenarios. The template demonstrates
every required pattern (env-driven scale/profile, tracing init, group
setup, throughput, sample-size override, async via tokio, `black_box`)
without hiding them behind macros.

## Testing

```bash
cargo test -p fluree-bench-support --lib
```

102 unit tests cover the determinism contract on every generator, env-var
parsing, budget loading, alias uniqueness, tracing init idempotence, corpus
hashing, and every rule the compare gate applies. A separate integration test
(`tests/workspace_reconcile.rs`) reconciles workspace `[[bench]]` entries with
`regression-budget.json`.

## Adding to your crate

In your crate's `Cargo.toml`:

```toml
[dev-dependencies]
criterion = "0.5"
fluree-bench-support = { path = "../fluree-bench-support" }

[[bench]]
name = "your_bench_name"
harness = false
```

Then drop a file into `<crate>/benches/your_bench_name.rs` (start from
`templates/BENCH_TEMPLATE.rs`).

See [`docs/contributing/benches.md`](../docs/contributing/benches.md) for
the full step-by-step guide, including budget registration, category
conventions, and CI integration.
