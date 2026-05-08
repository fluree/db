//! Full reindex from the commit chain.
//!
//! Measures `fluree.reindex(id, ReindexOptions::default())` end-to-end
//! against a file-backed ledger of varying size. The hot path under the
//! hood: commit-chain replay → flake collection → binary columnar index
//! build (FLI3 leaves, FBR3 branches) → FIR6 root publish.
//!
//! Reindex always rebuilds the full index from scratch (per
//! `fluree-db-api/src/admin.rs:958` doc comment), so the measurement is
//! end-to-end build time including I/O.
//!
//! ## Scenarios
//!
//! 1. `single_txn` — base data committed in one txn. Measures the
//!    build path under low commit-chain depth.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → base_nodes
//!              (Tiny=200, Small=2k, Medium=10k, Large=50k)
//!   metric:    Throughput::Elements(triples) → triples/sec
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench reindex_full
//!   cargo bench -p fluree-db-api --bench reindex_full -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench reindex_full

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};

const LEDGER_ID: &str = "bench/reindex-full:main";

fn scale_base_nodes(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 200,
        BenchScale::Small => 2_000,
        BenchScale::Medium => 10_000,
        BenchScale::Large => 50_000,
    }
}

/// Estimate the number of triples the reindex will materialize for a
/// given `base_nodes`. Mirrors `import_bulk.rs`'s estimator since the
/// generator is the same.
fn estimate_triples(base_nodes: usize) -> u64 {
    let data = generate_txn_data(0, base_nodes);
    let n_persons = data.persons.len() as u64;
    let n_companies = data.companies.len() as u64;
    let refs = data
        .companies
        .iter()
        .map(|c| (c.employee_ids.len() + c.customer_ids.len()) as u64)
        .sum::<u64>();
    n_persons * 4 + n_companies * 3 + refs
}

fn bench_reindex_full(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let base_nodes = scale_base_nodes(scale);
    let triple_count = estimate_triples(base_nodes);

    eprintln!(
        "  [reindex_full] scale={} base_nodes={} triples~={}",
        scale.as_str(),
        base_nodes,
        triple_count
    );

    let mut group = c.benchmark_group("reindex_full");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.throughput(Throughput::Elements(triple_count));

    group.bench_with_input(
        BenchmarkId::new("single_txn", scale.as_str()),
        &base_nodes,
        |b, &n| {
            b.iter_batched(
                // Setup: fresh tmpdir + file-backed Fluree + one populate
                // txn. NOT measured. Wrapped in block_on per the
                // benches.md gotcha.
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .build()
                                .expect("build file-backed Fluree");
                        let ledger = fluree
                            .create_ledger(LEDGER_ID)
                            .await
                            .expect("create_ledger");
                        let turtle = txn_data_to_turtle(&generate_txn_data(0, n));
                        // Use a permissive IndexConfig so the populate txn
                        // doesn't itself trigger background indexing
                        // (which would race with the measured reindex).
                        let index_config = IndexConfig {
                            reindex_min_bytes: 5_000_000_000,
                            reindex_max_bytes: 5_000_000_000,
                        };
                        let _ = fluree
                            .insert_turtle_with_opts(
                                ledger,
                                &turtle,
                                TxnOpts::default(),
                                CommitOpts::default(),
                                &index_config,
                            )
                            .await
                            .expect("populate insert");
                        // Move db_dir + fluree into the iter so they live
                        // for the duration of the measured op.
                        (db_dir, fluree)
                    })
                },
                // Measured: full reindex.
                |(_db_dir, fluree)| {
                    rt.block_on(async {
                        let result = fluree
                            .reindex(LEDGER_ID, ReindexOptions::default())
                            .await
                            .expect("reindex");
                        black_box(result);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_reindex_full);
criterion_main!(benches);
