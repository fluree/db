// TODO: One-line summary of what this bench measures.
//
// ## Scenarios
//
// TODO: List each named scenario; one line each.
//
// 1. `<scenario_name>` — what it measures
//
// ## Matrix
//
//   inputs:   tiny=…, small=…, medium=…, large=…
//   metric:   ns/op | rows/sec | flakes/sec | bytes/sec
//
// ## Running
//
//   cargo bench -p <crate> --bench <name>
//
// Quick validation (single iteration, no stats):
//
//   cargo bench -p <crate> --bench <name> -- --test
//
// Tracing on (writes spans to stderr):
//
//   FLUREE_BENCH_TRACING=1 cargo bench -p <crate> --bench <name>
//
// Larger inputs:
//
//   FLUREE_BENCH_SCALE=medium cargo bench -p <crate> --bench <name>
//
// ## Adding to Cargo.toml
//
// Append to <crate>/Cargo.toml:
//
//   [[bench]]
//   name = "<name>"           # matches the file stem; cargo bench --bench <name>
//   harness = false           # use criterion's own harness, not libtest
//
// And add the dev-dep if not already present:
//
//   [dev-dependencies]
//   criterion = "0.5"
//   fluree-bench-support = { path = "../fluree-bench-support" }
//
// ## Adding a regression budget
//
// Append to regression-budget.json under the right crate:
//
//   {
//     "crates": {
//       "<crate>": {
//         "<name>": {
//           "tiny":   10.0,
//           "small":   5.0,
//           "medium":  5.0
//         }
//       }
//     }
//   }
//
// Numbers are percent regression allowed vs baseline. Omit a scale to fall
// back to `default_budget_pct` (5%).
//
// ## Documenting a new category
//
// If `<name>` introduces a category not yet listed in
// `docs/contributing/benches.md`, add a row to the "Current categories"
// table in that doc.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench,
    BenchProfile, BenchScale,
};

// TODO: pick a category and a name. Both are strings; the file lives at
// `<crate>/benches/<category>_<name>.rs`. The category is one of the values
// listed in `docs/contributing/benches.md` "Current categories" or a new one
// you add there.
const BENCH_CATEGORY: &str = "TODO_category";
const BENCH_NAME: &str = "TODO_name";

fn bench_main(c: &mut Criterion) {
    init_tracing_for_bench();

    // Tokio runtime for any async measurement. Single-threaded by default;
    // set FLUREE_BENCH_RUNTIME=multi to switch.
    let _rt = bench_runtime();

    let scale = current_scale();
    let profile = current_profile();

    // TODO: build any per-bench setup (Fluree handle, fixtures, generated
    // datasets) here. Setup runs once per bench function entry, *not* per
    // iteration. If setup is expensive, consider criterion's
    // `iter_batched(setup, |x| measured_op(x), batch)` so setup is excluded
    // from measurement.
    //
    // Example (uncomment and adapt):
    //
    //     use fluree_bench_support::next_ledger_alias;
    //     use fluree_bench_support::gen::people;
    //     let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    //     let txns: Vec<_> = (0..10)
    //         .map(|i| people::generate_txn_data(i, 100))
    //         .map(|d| people::txn_data_to_jsonld(&d))
    //         .collect();
    //     let alias = next_ledger_alias("template");

    let mut group = c.benchmark_group(format!("{}_{}", BENCH_CATEGORY, BENCH_NAME));
    group.sample_size(profile.sample_size());

    // Pick a parameterization that scales with FLUREE_BENCH_SCALE. The
    // default is `BenchScale::elements_default`; benches with non-element
    // metrics (e.g., bytes/sec, txns/sec) should map differently.
    let n = scale.elements_default();

    // TODO: choose the metric. Throughput::Elements yields rows/sec or
    // ops/sec; Throughput::Bytes yields bytes/sec; omit for ns/op only.
    group.throughput(Throughput::Elements(n));

    group.bench_with_input(
        BenchmarkId::new(BENCH_NAME, scale.as_str()),
        &n,
        |b, &n| {
            b.iter(|| {
                // TODO: replace with the actual measured operation. Wrap
                // every observable side-effect in `black_box(...)` so the
                // optimizer doesn't elide the work.
                black_box(synthetic_work(n))
            });
        },
    );

    group.finish();

    // Optional: emit an end-of-run summary table. Useful when a bench has
    // multiple scenarios and you want a side-by-side comparison beyond what
    // criterion's HTML report shows.
    //
    //     use fluree_bench_support::report::{print_summary, SummaryRow};
    //     print_summary("template", &[
    //         SummaryRow::new("scenario_a").add("ms", 1.2).add("rows/s", 100.0),
    //         SummaryRow::new("scenario_b").add("ms", 1.4).add("rows/s",  90.0),
    //     ]);

    let _ = profile; // silence unused-binding warning in the template
}

/// Placeholder workload. Delete this and replace the call in `b.iter(...)`
/// with the operation you actually want to measure.
fn synthetic_work(n: u64) -> u64 {
    let mut acc: u64 = 0;
    for i in 0..n {
        acc = acc.wrapping_add(i.wrapping_mul(0xdeadbeef));
    }
    acc
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
