//! Cold reload latency under deep novelty.
//!
//! Distinct from `query_cold_reload.rs`, which scales by amount of data
//! committed in one txn; this bench scales by **commit-chain depth**
//! with no index attached. The cold reload exercises
//! `fluree-db-novelty::Novelty::bulk_apply_commits` (memory:
//! `mem:fact-01kqfy6txdrjppaf6756xzdz25`) plus the per-commit envelope
//! delta application.
//!
//! Background indexing is disabled (`without_indexing()`) so all commits
//! stay in novelty across the populate phase; the measured reload then
//! replays the entire chain.
//!
//! ## Scenarios
//!
//! 1. `replay_chain` — many small commits, single cold reload. Times
//!    the `graph(id).load()` call after dropping the populate handle.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → commit_count (Tiny=20, Small=100,
//!              Medium=500, Large=2000). Each commit is a small txn
//!              (10 nodes) so total flakes scale linearly with commits.
//!   metric:    ns/load (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench novelty_replay
//!   cargo bench -p fluree-db-api --bench novelty_replay -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench novelty_replay

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};

const NODES_PER_COMMIT: usize = 10;

fn scale_commit_count(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 20,
        BenchScale::Small => 100,
        BenchScale::Medium => 500,
        BenchScale::Large => 2_000,
    }
}

/// Build a file-backed Fluree at `db_dir` with background indexing
/// **disabled**, insert `commit_count` small commits, then drop the
/// handle. The disk now has a long commit chain with no index above it;
/// the next open will cold-load via novelty replay.
async fn populate(db_dir: &std::path::Path, alias: &str, commit_count: usize) {
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build file-backed Fluree (populate)");
    let mut ledger = fluree.create_ledger(alias).await.expect("create_ledger");
    let index_config = IndexConfig {
        // Belt-and-braces with `without_indexing()`: a very high threshold
        // is meaningless when the indexer is disabled, but keeps the
        // populate path from making a foreground decision either.
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    for i in 0..commit_count {
        let turtle = txn_data_to_turtle(&generate_txn_data(i, NODES_PER_COMMIT));
        let r = fluree
            .insert_turtle_with_opts(
                ledger,
                &turtle,
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect("populate insert");
        ledger = r.ledger;
    }
    // Drop fluree → next open is cold and replays the whole chain.
}

fn bench_novelty_replay(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let commit_count = scale_commit_count(scale);

    eprintln!(
        "  [novelty_replay] scale={} commit_count={} nodes_per_commit={}",
        scale.as_str(),
        commit_count,
        NODES_PER_COMMIT,
    );

    let mut group = c.benchmark_group("novelty_replay");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    group.bench_with_input(
        BenchmarkId::new("replay_chain", scale.as_str()),
        &commit_count,
        |b, &n| {
            b.iter_batched(
                // Setup: populate then drop. NOT measured.
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let alias = next_ledger_alias("novelty-replay");
                        populate(db_dir.path(), &alias, n).await;
                        (db_dir, alias)
                    })
                },
                // Measured: cold open + load. Without indexing, the
                // load path goes entirely through novelty replay.
                |(db_dir, alias)| {
                    rt.block_on(async {
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .without_indexing()
                                .build()
                                .expect("build file-backed Fluree (cold)");
                        let snapshot = fluree.graph(&alias).load().await.expect("cold load");
                        black_box(snapshot);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_novelty_replay);
criterion_main!(benches);
