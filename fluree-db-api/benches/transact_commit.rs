//! Single-commit latency on a populated ledger.
//!
//! Measures how long one `insert_turtle_with_opts` call takes when applied
//! to a ledger that already has some history. This is the canonical
//! "user makes an update on an existing dataset" hot path. Distinct from
//! `insert_formats.rs`, which measures total throughput across many txns.
//!
//! ## Scenarios
//!
//! 1. `fresh_ledger` — commit one small txn against a freshly-created
//!    ledger (zero base flakes). Measures pure commit overhead.
//! 2. `populated_ledger` — commit one small txn against a ledger
//!    pre-loaded with base flakes. Measures the commit latency that
//!    most users see in production.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → (base_nodes, commit_nodes) tiers
//!              (Tiny=100×10, Small=1k×10, Medium=10k×10, Large=100k×10)
//!   metric:    ns/commit (criterion default; no Throughput)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench transact_commit
//!   cargo bench -p fluree-db-api --bench transact_commit -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench transact_commit
//!
//! ## Adding to Cargo.toml — already wired (see fluree-db-api/Cargo.toml)
//!
//! ## Adding a regression budget — already wired (see regression-budget.json
//! "transact_commit" entry)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};

/// Map BenchScale to (base nodes, commit nodes) for this bench's workload.
fn scale_inputs(scale: BenchScale) -> (usize, usize) {
    match scale {
        // Keep tiny tiny so PR-gated runs finish quickly.
        BenchScale::Tiny => (100, 10),
        BenchScale::Small => (1_000, 10),
        BenchScale::Medium => (10_000, 10),
        BenchScale::Large => (100_000, 10),
    }
}

/// Pre-generate a Turtle blob containing `total_nodes` of Person+Company
/// data, split into ~100-node txns. Returns the per-txn Turtle strings,
/// concatenated for a single bulk `insert` is also possible but here we
/// keep the per-txn split so the base-population path mirrors
/// `insert_formats.rs`.
fn pregen_base_turtles(total_nodes: usize) -> Vec<String> {
    if total_nodes == 0 {
        return Vec::new();
    }
    let nodes_per_txn = 100usize.min(total_nodes);
    let n_txns = total_nodes.div_ceil(nodes_per_txn);
    (0..n_txns)
        .map(|i| {
            let n = if i == n_txns - 1 {
                total_nodes - i * nodes_per_txn
            } else {
                nodes_per_txn
            };
            // Use a deterministic txn_idx that doesn't collide with the
            // measured commit's txn_idx (which we offset by a million below).
            txn_data_to_turtle(&generate_txn_data(i, n))
        })
        .collect()
}

/// One commit-time txn. Distinct global-base offset (1M) keeps it from
/// colliding with the base-load txns above.
fn pregen_commit_turtle(commit_nodes: usize) -> String {
    txn_data_to_turtle(&generate_txn_data(1_000_000, commit_nodes))
}

fn bench_transact_commit(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let (base_nodes, commit_nodes) = scale_inputs(scale);

    let fluree = FlureeBuilder::memory().build_memory();

    // Plenty of headroom; we don't want to trigger reindex during the bench.
    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let base_turtles = pregen_base_turtles(base_nodes);
    let commit_turtle = pregen_commit_turtle(commit_nodes);

    let mut group = c.benchmark_group("transact_commit");
    group.sample_size(profile.sample_size());
    // The "fresh ledger" scenario is short; the populated one can be slow
    // at Medium/Large. iter_batched amortizes setup across the batch.
    group.sampling_mode(criterion::SamplingMode::Flat);

    // --- Scenario 1: fresh ledger (no base load, just commit) ---
    group.bench_with_input(
        BenchmarkId::new("fresh_ledger", scale.as_str()),
        &commit_nodes,
        |b, _| {
            b.iter_batched(
                // Setup: fresh ledger. NOT measured.
                || {
                    rt.block_on(async {
                        let alias = next_ledger_alias("tc-fresh");
                        fluree.create_ledger(&alias).await.unwrap()
                    })
                },
                // Measured: one commit.
                |ledger| {
                    rt.block_on(async {
                        let result = fluree
                            .insert_turtle_with_opts(
                                ledger,
                                &commit_turtle,
                                TxnOpts::default(),
                                CommitOpts::default(),
                                &index_config,
                            )
                            .await
                            .unwrap();
                        black_box(result.ledger);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    // --- Scenario 2: populated ledger (base load, then measured commit) ---
    if base_nodes > 0 {
        group.bench_with_input(
            BenchmarkId::new("populated_ledger", scale.as_str()),
            &(base_nodes, commit_nodes),
            |b, _| {
                b.iter_batched(
                    // Setup: fresh ledger + base load. NOT measured.
                    || {
                        rt.block_on(async {
                            let alias = next_ledger_alias("tc-pop");
                            let mut ledger = fluree.create_ledger(&alias).await.unwrap();
                            for ttl in &base_turtles {
                                let r = fluree
                                    .insert_turtle_with_opts(
                                        ledger,
                                        ttl,
                                        TxnOpts::default(),
                                        CommitOpts::default(),
                                        &index_config,
                                    )
                                    .await
                                    .unwrap();
                                ledger = r.ledger;
                            }
                            ledger
                        })
                    },
                    // Measured: one commit on top of `base_nodes` of history.
                    |ledger| {
                        rt.block_on(async {
                            let result = fluree
                                .insert_turtle_with_opts(
                                    ledger,
                                    &commit_turtle,
                                    TxnOpts::default(),
                                    CommitOpts::default(),
                                    &index_config,
                                )
                                .await
                                .unwrap();
                            black_box(result.ledger);
                        });
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_transact_commit);
criterion_main!(benches);
