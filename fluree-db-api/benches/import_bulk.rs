//! Bulk Turtle import throughput.
//!
//! Measures end-to-end `fluree.create(id).import(path).execute()` for
//! varying input sizes. The hot path under the hood: Turtle streaming
//! parse → chunked staging → root assembly → FIR6 root publish.
//! Distinct from `insert_formats.rs`, which measures per-txn
//! `insert_with_opts` repeated; bulk import takes a single Turtle file
//! and runs the streaming-import pipeline once.
//!
//! ## Scenarios
//!
//! 1. `single_threaded` — `threads(1)`. Useful as a baseline that doesn't
//!    confound with parallelism overhead.
//! 2. `default_threads` — leaves `threads(...)` unset. Exercises the
//!    parallel-import allocator and worker-cache code paths.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → total_nodes (Tiny=1k, Small=10k, Medium=50k,
//!              Large=200k). Each node produces ~4-6 triples; expect
//!              4× to 7× the throughput in triples.
//!   metric:    Throughput::Elements(triples_count) → triples/sec
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench import_bulk
//!   cargo bench -p fluree-db-api --bench import_bulk -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench import_bulk
//!
//! ## Cargo.toml + budget already wired (see fluree-db-api/Cargo.toml,
//! regression-budget.json).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, BenchScale,
};
use fluree_db_api::FlureeBuilder;
use std::io::Write;

/// Map BenchScale to the total node count for this bench. Bulk import is
/// the most setup-heavy bench in the suite; we keep tiny tiny so PR-gated
/// runs finish quickly.
fn scale_total_nodes(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 1_000,
        BenchScale::Small => 10_000,
        BenchScale::Medium => 50_000,
        BenchScale::Large => 200_000,
    }
}

/// Build one Turtle blob containing `total_nodes` of Person+Company data
/// concatenated (single `@prefix` header followed by N batches of
/// triples). Keeps the @prefix declarations from the first batch only;
/// later batches' prefix lines are stripped so the resulting file is one
/// well-formed Turtle document.
///
/// Returns `(turtle_bytes, triple_count_estimate)`. The triple count is a
/// lower bound used for `Throughput::Elements`; actual flake count after
/// commit is the same since bulk import doesn't synthesize extra flakes.
fn build_bulk_turtle(total_nodes: usize) -> (String, u64) {
    // Generate one big TxnData rather than splitting into batches; bulk
    // import handles arbitrary-size files. We use one txn_idx so all IDs
    // are contiguous and the resulting file is one cohesive graph.
    let data = generate_txn_data(0, total_nodes);
    let turtle = txn_data_to_turtle(&data);

    // Triple-count estimate: each Person → 4 triples (a, name, email, age).
    // Each Company → 2 fixed (a, name) + founded + employees + customers.
    // employees/customers may be empty or have multiple values; for the
    // throughput metric, count fixed triples only.
    let n_persons = data.persons.len() as u64;
    let n_companies = data.companies.len() as u64;
    let triples = n_persons * 4
        + n_companies * 3
        + data
            .companies
            .iter()
            .map(|c| (c.employee_ids.len() + c.customer_ids.len()) as u64)
            .sum::<u64>();

    (turtle, triples)
}

/// Write the Turtle to a fresh tempfile in `dir`. Returns the path.
fn write_turtle(dir: &std::path::Path, contents: &str) -> std::path::PathBuf {
    let path = dir.join("import.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(contents.as_bytes()).expect("write ttl");
    path
}

fn bench_import_bulk(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let total_nodes = scale_total_nodes(scale);

    let (turtle_str, triple_count) = build_bulk_turtle(total_nodes);
    eprintln!(
        "  [import_bulk] scale={} nodes={} triples~={} bytes={}",
        scale.as_str(),
        total_nodes,
        triple_count,
        turtle_str.len()
    );

    let mut group = c.benchmark_group("import_bulk");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.throughput(Throughput::Elements(triple_count));

    // --- Scenario 1: single-threaded import ---
    group.bench_with_input(
        BenchmarkId::new("single_threaded", scale.as_str()),
        &turtle_str,
        |b, ttl| {
            b.iter_batched(
                // Setup: fresh tmpdirs + write the Turtle to disk + build
                // a fresh file-backed Fluree. NOT measured.
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let data_dir = tempfile::tempdir().expect("data tmpdir");
                        let ttl_path = write_turtle(data_dir.path(), ttl);
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .build()
                                .expect("build file-backed Fluree");
                        // Move the tmpdirs into the iter so they live for
                        // the duration of the measured op (Drop after the
                        // timed block runs).
                        (db_dir, data_dir, ttl_path, fluree)
                    })
                },
                // Measured: the import itself.
                |(_db_dir, _data_dir, ttl_path, fluree)| {
                    rt.block_on(async {
                        let result = fluree
                            .create("bench/import-bulk-st:main")
                            .import(&ttl_path)
                            .threads(1)
                            .execute()
                            .await
                            .expect("import");
                        black_box(result);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    // --- Scenario 2: default threads (parallel) ---
    // Skip at Tiny because parallel overhead dominates at very small inputs
    // and the comparison would mislead.
    if scale != BenchScale::Tiny {
        group.bench_with_input(
            BenchmarkId::new("default_threads", scale.as_str()),
            &turtle_str,
            |b, ttl| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let db_dir = tempfile::tempdir().expect("db tmpdir");
                            let data_dir = tempfile::tempdir().expect("data tmpdir");
                            let ttl_path = write_turtle(data_dir.path(), ttl);
                            let fluree =
                                FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                    .build()
                                    .expect("build file-backed Fluree");
                            (db_dir, data_dir, ttl_path, fluree)
                        })
                    },
                    |(_db_dir, _data_dir, ttl_path, fluree)| {
                        rt.block_on(async {
                            let result = fluree
                                .create("bench/import-bulk-mt:main")
                                .import(&ttl_path)
                                .execute()
                                .await
                                .expect("import");
                            black_box(result);
                        });
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_import_bulk);
criterion_main!(benches);
