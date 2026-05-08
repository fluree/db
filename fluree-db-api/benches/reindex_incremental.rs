//! Incremental indexing latency.
//!
//! Measures the orchestrator's incremental index path: existing index +
//! some commits worth of novelty above it → `trigger_index` → updated
//! index covering the new commits. Distinct from `reindex_full.rs`,
//! which rebuilds from scratch.
//!
//! Setup brings the ledger to a state where an index already covers
//! `base_nodes` of data and `delta_commits` worth of additional commits
//! are sitting in novelty. The measured operation is the
//! `trigger_index` call that drives the orchestrator to extend the
//! existing index over those novelty commits.
//!
//! ## Scenarios
//!
//! 1. `apply_delta` — N commits of novelty above an existing index.
//!    Measures the orchestrator's incremental work.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → (base_nodes, delta_commits)
//!              (Tiny=200×5, Small=2k×20, Medium=10k×50, Large=50k×200)
//!              base committed in one txn; delta is `delta_commits`
//!              small commits of 10 nodes each.
//!   metric:    ns/incremental-index (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench reindex_incremental
//!   cargo bench -p fluree-db-api --bench reindex_incremental -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench reindex_incremental

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, BenchScale,
};
use fluree_db_api::admin::{ReindexOptions, TriggerIndexOptions};
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, LedgerState, TxnOpts};

const LEDGER_ID: &str = "bench/reindex-incremental:main";
const DELTA_NODES_PER_COMMIT: usize = 10;

fn scale_inputs(scale: BenchScale) -> (usize, usize) {
    match scale {
        BenchScale::Tiny => (200, 5),
        BenchScale::Small => (2_000, 20),
        BenchScale::Medium => (10_000, 50),
        BenchScale::Large => (50_000, 200),
    }
}

async fn insert_commit(fluree: &Fluree, ledger: LedgerState, txn: &str) -> LedgerState {
    let index_config = IndexConfig {
        // Set high enough that *background* indexing doesn't fire
        // during populate; we drive indexing manually via
        // trigger_index / reindex.
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    fluree
        .insert_turtle_with_opts(
            ledger,
            txn,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("insert")
        .ledger
}

fn bench_reindex_incremental(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let (base_nodes, delta_commits) = scale_inputs(scale);

    eprintln!(
        "  [reindex_incremental] scale={} base_nodes={} delta_commits={} (nodes/commit={})",
        scale.as_str(),
        base_nodes,
        delta_commits,
        DELTA_NODES_PER_COMMIT,
    );

    let mut group = c.benchmark_group("reindex_incremental");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    group.bench_with_input(
        BenchmarkId::new("apply_delta", scale.as_str()),
        &(base_nodes, delta_commits),
        |b, &(base, delta)| {
            b.iter_batched(
                // Setup: populate, reindex to baseline, then add `delta`
                // commits sitting in novelty above the index. The
                // orchestrator's incremental path is now the next thing
                // to fire. NOT measured.
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .build()
                                .expect("build file-backed Fluree");

                        let mut ledger = fluree
                            .create_ledger(LEDGER_ID)
                            .await
                            .expect("create_ledger");

                        // Base txn — all in one go to keep commit-chain
                        // depth low and isolate "delta over an indexed
                        // base" from "delta over a deep chain."
                        let base_turtle = txn_data_to_turtle(&generate_txn_data(0, base));
                        ledger = insert_commit(&fluree, ledger, &base_turtle).await;

                        // Establish baseline index. Full reindex is the
                        // simplest way to put the ledger into an
                        // "indexed" state from the public API.
                        let _ = fluree
                            .reindex(LEDGER_ID, ReindexOptions::default())
                            .await
                            .expect("baseline reindex");

                        // Drop the per-call ledger handle; reload to
                        // pick up the just-published index.
                        drop(ledger);

                        // Reload to get a LedgerState that knows the
                        // index head, then apply `delta` commits above
                        // it. The next trigger_index will be incremental.
                        let mut ledger = fluree
                            .ledger(LEDGER_ID)
                            .await
                            .expect("reload after baseline reindex");
                        for i in 0..delta {
                            // Offset txn_idx by 1M to avoid colliding
                            // with the base population's IDs.
                            let txn = txn_data_to_turtle(&generate_txn_data(
                                1_000_000 + i,
                                DELTA_NODES_PER_COMMIT,
                            ));
                            ledger = insert_commit(&fluree, ledger, &txn).await;
                        }

                        (db_dir, fluree)
                    })
                },
                // Measured: trigger_index drives the orchestrator's
                // incremental path against the novelty above the index.
                |(_db_dir, fluree)| {
                    rt.block_on(async {
                        let result = fluree
                            .trigger_index(LEDGER_ID, TriggerIndexOptions::default())
                            .await
                            .expect("trigger_index");
                        black_box(result);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_reindex_incremental);
criterion_main!(benches);
