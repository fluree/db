# Adding a benchmark

When you add or modify a hot path in Fluree, instrument it with a bench so
regressions surface in CI rather than in production. This guide explains
the six-step workflow, the chassis helpers, the conventions, and the
gotchas.

For *running* existing benches, see [`BENCHMARKING.md`](../../BENCHMARKING.md).
For the chassis API, see
[`fluree-bench-support/README.md`](../../fluree-bench-support/README.md).
For tracing conventions inside the database itself, see
[`tracing-guide.md`](tracing-guide.md).

## When to add a bench

Add one when:

- you touch a hot path the existing benches don't already exercise (commit,
  index build, reindex, incremental index, bulk import, novelty replay,
  reload — see `Current categories` below for the canonical list),
- you add a new feature whose perf characteristics matter,
- you fix a regression and want a regression test that catches it next time.

You don't need a bench for purely correctness-driven changes. Most type
refactors don't move numbers and shouldn't add a bench just because they
touch hot files.

## The six-step workflow

### 1. Choose a category and a name

A bench file lives at `<crate>/benches/<category>_<name>.rs`. Pick a
category from the table below or add a new one (see `Adding a new
category`). The name should be specific: `transact_commit_single_flake`,
not `transact_commit`.

### 2. Copy the template

```bash
cp fluree-bench-support/templates/BENCH_TEMPLATE.rs \
   <crate>/benches/<category>_<name>.rs
```

The template is a working bench against synthetic data with `// TODO`
markers at every spot you need to edit.

### 3. Fill in the scenario

Edit the `bench_main` function. Keep:

- `init_tracing_for_bench()` at the top,
- `let _rt = bench_runtime();` for any async work,
- `current_scale()` and `current_profile()` for env-driven sizing.

Replace `synthetic_work(n)` with the operation you actually want to measure.
Wrap inputs in `black_box` so the optimizer doesn't elide the work.

If your bench needs realistic data, reach for
`fluree_bench_support::gen::*` first:

| Need | Use |
|---|---|
| Linked-data person/company graph | `gen::people::generate_txn_data(...)` + `gen::people::txn_data_to_jsonld(...)` or `txn_data_to_turtle(...)` |
| Random `f64` vectors | `gen::vectors::rng_one(rng, dim)` (RNG-driven) or `gen::vectors::hashed_pair(dim)` (deterministic, no RNG) |
| Paragraph documents | `gen::corpora::random_paragraph(rng)` |

If your domain is genuinely new (e.g., spatial geometries, Turtle-import
edge cases), keep the generator co-located with the bench file. Lift it
into `gen::` only when a second bench wants to reuse it.

### 4. Add the `[[bench]]` entry

Append to your crate's `Cargo.toml`:

```toml
[[bench]]
name = "<category>_<name>"   # matches the file stem
harness = false              # use criterion's harness, not libtest
```

If the crate doesn't already depend on `fluree-bench-support` and
`criterion`, add them to `[dev-dependencies]`:

```toml
[dev-dependencies]
criterion = "0.5"
fluree-bench-support = { path = "../fluree-bench-support" }
```

### 5. Register a regression budget

Append to `regression-budget.json` at the workspace root:

```json
{
  "crates": {
    "<crate-name>": {
      "<category>_<name>": {
        "tiny":   10.0,
        "small":   5.0,
        "medium":  5.0
      }
    }
  }
}
```

Numbers are percent regression allowed vs. the committed baseline. Omit a
scale to fall back to `default_budget_pct` (5%). The CI gate fails if an
observed run exceeds the budget for any listed scale.

If you don't yet have a baseline, leave the entries empty — `default_budget_pct`
applies. After your first nightly run lands a baseline, tighten the budget.

### 6. Document if you added a new category

If you introduced a category that's not in the `Current categories` table
below, add a row. One sentence per category is enough. Reviewers will ask
for this if you forget.

## The chassis helpers

### `init_tracing_for_bench()`

Idempotent. Call at the top of every `bench_*` entry point. Off by default
(zero overhead). Set `FLUREE_BENCH_TRACING=1` to install a stderr
subscriber filtered by `RUST_LOG`.

### `next_ledger_alias(prefix)`

Returns `bench/{prefix}-{n}:main` with an atomic counter that's unique
within the process. Use this when each criterion iteration creates a
fresh ledger:

```rust
b.iter(|| {
    let alias = next_ledger_alias("commit");
    rt.block_on(async {
        let ledger = fluree.create_ledger(&alias).await.unwrap();
        // ...
    });
});
```

Don't hand-roll an `AtomicU64` counter or interpolate a per-iteration index
into a `format!` literal — those patterns cause bench-vs-bench alias
collisions when criterion runs groups concurrently.

### `bench_runtime()`

Single-threaded tokio runtime. Set `FLUREE_BENCH_RUNTIME=multi` for
multi-thread; only use multi when measuring code that intrinsically depends
on parallel scheduling (e.g., parallel bulk-import).

### `current_scale()` and `current_profile()`

```rust
let scale = current_scale();   // BenchScale::{Tiny|Small|Medium|Large}
let profile = current_profile(); // BenchProfile::{Quick|Full}

let n = scale.elements_default();   // 1k / 10k / 100k / 1M
group.sample_size(profile.sample_size()); // 10 / 30
```

Benches with non-element metrics (bytes/sec, txns/sec, articles) should
override `elements_default()` with their own scale-aware mapping.

### `gen::*`

Deterministic generators. Output is byte-identical across runs given the
same parameters; the chassis tests pin this contract. Determinism matters
because regression budgets compare against a stored baseline — a
non-deterministic input would invalidate the baseline.

### `report::print_summary`

Optional. Use when you want a human-readable cross-scenario table at the
end of the run beyond what criterion's HTML report shows:

```rust
use fluree_bench_support::report::{print_summary, SummaryRow};
print_summary("insert_formats", &[
    SummaryRow::new("jsonld 100x10").add("ms", jld_ms).add("flakes/s", jld_fps),
    SummaryRow::new("turtle 100x10").add("ms", ttl_ms).add("flakes/s", ttl_fps),
]);
```

## Current categories

A bench category is just a string in the file-name prefix and the budget
JSON. Adding a new category is one row here and one section in
`regression-budget.json`. CI accepts any category as long as it's
documented.

| Category | Hot path | Where it lives |
|---|---|---|
| `import` | bulk Turtle / N-Quads / JSON-LD ingest | `fluree-db-api/benches/import_*.rs` |
| `transact` | stage + commit | `fluree-db-api/benches/transact_*.rs` |
| `index` | full reindex; incremental; gc | `fluree-db-indexer/benches/index_*.rs` |
| `query_hot` | BSBM-shape SPARQL on warm cache | `fluree-db-api/benches/query_hot_*.rs` |
| `query_cold` | reload + first-query latency | `fluree-db-api/benches/query_cold_*.rs` |
| `novelty` | replay, catch-up, bulk-apply | `fluree-db-novelty/benches/novelty_*.rs` |
| `core` | namespace encode/decode and similar foundational ops | `fluree-db-core/benches/core_*.rs` |
| `query` | scan/join/aggregate micro-benches inside `fluree-db-query` | `fluree-db-query/benches/query_*.rs` |
| `vector_math` | SIMD vs scalar math micro-benches | `fluree-db-query/benches/vector_math.rs` |
| `spatial` | S2 covering / build / query | `fluree-db-spatial/benches/spatial_*.rs` |
| `insert_formats` | JSON-LD vs Turtle insert format comparison | `fluree-db-api/benches/insert_formats.rs` |
| `vector_query` | end-to-end vector similarity through the query engine | `fluree-db-api/benches/vector_query.rs` |
| `fulltext_query` | full-text scoring through novelty + index | `fluree-db-api/benches/fulltext_query.rs` |

## Common patterns

### Setup that shouldn't be measured

Criterion's `iter_batched` accepts a setup closure that's *not* counted in
the timing. Use it when bench setup is heavy (large dataset generation,
ledger creation):

```rust
group.bench_with_input(BenchmarkId::new("commit", scale.as_str()), &n, |b, &n| {
    b.iter_batched(
        || rt.block_on(setup_fresh_ledger(n)),     // setup, not measured
        |ledger| rt.block_on(commit_one_txn(ledger)), // measured
        criterion::BatchSize::SmallInput,
    );
});
```

### One ledger per scenario, multiple iterations

When the bench reads from a populated ledger and doesn't mutate it, create
the ledger once per scenario (not per iteration):

```rust
for &n in DATASET_SIZES {
    let (fluree, ledger, query, ...) = rt.block_on(setup_dataset(n));
    // ledger is reused across all b.iter() calls below
    group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
        b.iter(|| rt.block_on(async {
            black_box(fluree.query(&ledger, &query).await.unwrap())
        }));
    });
}
```

### One ledger per iteration

When the bench mutates state (commit, transact, index), each iteration
needs a fresh ledger:

```rust
b.iter(|| {
    let alias = next_ledger_alias("commit");
    rt.block_on(async {
        let ledger = fluree.create_ledger(&alias).await.unwrap();
        // commit one txn ...
    });
});
```

### Throughput annotations

Choose units that make criterion's `thrpt` line meaningful:

- `Throughput::Elements(n)` → `elem/s` (rows, flakes, articles)
- `Throughput::Bytes(n)` → `B/s`
- omit → ns/op only

`insert_formats.rs` uses `Throughput::Elements(total_flakes)` so its output
is in `flakes/s`, the unit users care about for ingest performance.

## Gotchas

### Determinism is not optional

If a bench uses an RNG, seed it with `StdRng::seed_from_u64(42)` (or a
similar fixed seed). Benches that draw from `rand::thread_rng()` produce
non-comparable runs — every iteration sees different inputs and the
regression budget loses meaning.

The chassis generators are seeded internally and don't need an external
RNG; if you generate something the chassis doesn't cover, follow the same
discipline.

### Don't hold `span.enter()` across `.await`

This is a generic tracing rule (see
[`tracing-guide.md`](tracing-guide.md)) but worth re-emphasizing because
benches under `FLUREE_BENCH_TRACING=1` will surface the cross-task
contamination as nonsense traces. Use `.instrument(span)` for async work.

### Cold-vs-warm cache effects

The first iteration after a fresh build is always slower (CPU cache cold,
allocator warming up). Criterion's default warmup handles this, but if
your bench is short and warmup is skipped, drop the first sample manually
or use `iter_batched` with `BatchSize::PerIteration`.

### Setup dominating the measurement

If your "measured" work is a 1-µs operation but setup is a 10-ms ledger
load, criterion will measure mostly setup. Move setup into `iter_batched`
or amortize it over a larger batch:

```rust
b.iter_batched(
    || setup_one_input(),
    |inputs| inputs.into_iter().map(|x| measured_op(x)).collect::<Vec<_>>(),
    criterion::BatchSize::SmallInput,
);
```

### Runtime configuration leakage

Don't read `FLUREE_BENCH_*` env vars in your bench's hot loop —
`current_scale()` and `current_profile()` cache via `OnceLock`, but a
hand-rolled `std::env::var` call inside `b.iter` is a system call per
iteration. Read once, reuse.

### `iter_batched` setup needs a tokio reactor for file-backed Fluree

`criterion::iter_batched`'s `setup` closure runs **synchronously**, outside
any `block_on`. If `setup` calls anything that requires a running tokio
reactor — most notably `FlureeBuilder::file(...).build()` and any path
that touches the file storage backend during construction — you'll get:

```
thread 'main' panicked: there is no reactor running, must be called from
the context of a Tokio 1.x runtime
```

The fix is to wrap setup work that touches the runtime in `rt.block_on`:

```rust
let rt = bench_runtime();

b.iter_batched(
    // setup — wrap in block_on so the reactor is alive while
    // FlureeBuilder::file(...).build() runs.
    || rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .unwrap();
        (dir, fluree)
    }),
    |(_dir, fluree)| rt.block_on(async {
        // measured op
    }),
    criterion::BatchSize::PerIteration,
);
```

`FlureeBuilder::memory().build_memory()` does **not** have this constraint
— it constructs synchronously without a reactor. Use the memory builder
when the bench's hot path doesn't actually need disk I/O; reach for the
file builder only when you need to exercise persistence/load paths.

### Workspace clippy lints apply to bench code

The workspace `Cargo.toml` denies several clippy lints
(see `[workspace.lints.clippy]`). Two that matter for benches:

- **`needless_raw_string_hashes = "deny"`**: write `r"..."` not `r#"..."#`
  unless the string actually contains `"`. This usually surfaces in
  embedded SPARQL/Turtle string literals.
- **`uninlined_format_args = "deny"`**: write `format!("{x}")` not
  `format!("{}", x)` whenever the variable name is in scope.

Running `cargo clippy --benches` locally before pushing catches these.

## Debugging a flaky bench

A bench is "flaky" when CI runs sometimes pass and sometimes fail with no
code change. Diagnostic steps:

1. **Run the same bench multiple times locally**:

   ```bash
   for i in 1 2 3 4 5; do
     cargo bench -p <crate> --bench <name> -- --quick
   done
   ```

   If results vary by more than the budget, the bench has high variance.

2. **Increase sample size and warmup**:

   ```bash
   FLUREE_BENCH_PROFILE=full cargo bench -p <crate> --bench <name>
   ```

   `Full` widens the sample distribution; if variance is real, this
   exposes it; if variance was a sample-size artifact, it disappears.

3. **Capture a trace under tracing**:

   ```bash
   FLUREE_BENCH_TRACING=1 RUST_LOG=info,fluree_db_api=debug \
       cargo bench -p <crate> --bench <name> -- --test 2> trace.log
   ```

   Inspect the spans for setup/measurement separation issues.

4. **If genuinely flaky on CI hardware**: raise the budget for that bench
   in `regression-budget.json` and document the reasoning in the PR. The
   gate exists to catch real regressions, not to chase shared-runner noise.

## Capturing a span trace

The eventual JSON-emitting tracing layer
(`FLUREE_BENCH_TRACING=file:./out.json`) is reserved but not yet
implemented. Today it falls back to stderr.

Until the file mode ships, use the stderr mode plus shell redirection:

```bash
FLUREE_BENCH_TRACING=1 RUST_LOG=info,fluree_db_api=debug,fluree_db_query=debug \
    cargo bench -p fluree-db-api --bench insert_formats -- --test 2> trace.log
```

Then grep for the spans you care about:

```bash
grep -E 'transact_commit|txn_stage|index_build' trace.log | head -50
```

## Reviewing a bench PR

When reviewing someone else's bench, check:

- [ ] File name matches `<category>_<name>.rs` and the category appears in
      this guide's `Current categories` table.
- [ ] `[[bench]]` entry is present in the crate's `Cargo.toml`.
- [ ] `regression-budget.json` has a matching entry (or default applies).
- [ ] Bench uses `init_tracing_for_bench()`, `bench_runtime()`,
      `next_ledger_alias()` (where applicable) — not hand-rolled equivalents.
- [ ] Determinism: any RNG is seeded with a fixed seed; any non-chassis
      generator is byte-stable across runs.
- [ ] `black_box` wraps the measured operation's inputs *or* outputs so
      LLVM doesn't elide it.
- [ ] Throughput annotation matches the units in the bench's docstring.
- [ ] Bench compiles with `cargo bench --no-run -p <crate> --bench <name>`
      and runs with `-- --test`.

## Future work

- The `BenchSpanLayer` (file-mode tracing) lands as part of the bench
  chassis follow-up work; until then, file mode falls back to stderr.
- The `fixtures::load_or_generate` body lands in `bench-4` (vendored
  fixtures) and `bench-6` (remote fetch).
- The `validate_against_workspace()` reconciler lands in `bench-5` along
  with the CI gated job.

These are tracked in the bench-infrastructure plan; opening a separate
issue isn't needed.
