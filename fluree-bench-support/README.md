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

### `fixtures` module

Workspace-root `fluree-bench-support/fixtures/` resolution. The
`load_or_generate(name, scale)` entry point is a stub today; bodies land in
`bench-4` (vendored fixtures) and `bench-6` (remote fetch).

### `budget` module

`RegressionBudget` schema + loader for `regression-budget.json` at the
workspace root. The `check(...)` helper compares observed nanoseconds to
`baseline * (1 + budget_pct/100)` and returns a `BudgetViolation` on
failure. The `validate_against_workspace()` reconciler is a stub today;
lands in `bench-5` (CI gate).

### `report` module

`SummaryRow` + `print_summary(title, rows)` for opt-in human-readable
end-of-run tables. Useful when criterion's HTML output doesn't surface the
domain-specific cross-scenario comparison a bench wants.

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

37 unit tests cover the determinism contract on every generator, env-var
parsing, budget loading, alias uniqueness, and tracing init idempotence.

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
