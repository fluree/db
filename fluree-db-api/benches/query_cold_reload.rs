//! Cold ledger reload latency.
//!
//! Pre-populate a file-backed ledger, drop the Fluree connection, then
//! measure the time to rebuild the in-memory state when a fresh Fluree
//! handle opens that same ledger. This is the canonical "I restarted my
//! application, what's the time-to-first-query" hot path. Distinct from
//! `vector_query.rs` and `fulltext_query.rs`, which measure warm-cache
//! query latency on an already-loaded ledger.
//!
//! ## Scenarios
//!
//! 1. `cold_load` — just `fluree.graph(id).load()` against a fresh Fluree
//!    handle. Measures the load path: storage read → snapshot decode →
//!    novelty replay → binary-store attach (when an index is present).
//! 2. `cold_load_plus_query` — load + one SPARQL query. Captures the
//!    full user-visible latency between "open the app" and "first
//!    result returned."
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → base_nodes (Tiny=200, Small=2k, Medium=10k,
//!              Large=50k). Each base_nodes is committed in a single txn
//!              to keep the commit chain short and isolate the scaling
//!              of "amount of data" from "depth of commit chain."
//!   metric:    ns/load and ns/(load+query)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_cold_reload
//!   cargo bench -p fluree-db-api --bench query_cold_reload -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_cold_reload

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};

/// Map BenchScale to the base_nodes for this bench. We commit all base
/// data in a single txn to isolate "amount of data" scaling from
/// "depth of commit chain" scaling.
fn scale_base_nodes(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 200,
        BenchScale::Small => 2_000,
        BenchScale::Medium => 10_000,
        BenchScale::Large => 50_000,
    }
}

/// Populate a fresh file-backed ledger at `db_dir` (with the given
/// `alias`) with `base_nodes` of data, then drop the Fluree handle so
/// subsequent opens are cold.
async fn populate(db_dir: &std::path::Path, alias: &str, base_nodes: usize) {
    let fluree = FlureeBuilder::file(db_dir.to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree (populate)");
    let data = generate_txn_data(0, base_nodes);
    let turtle = txn_data_to_turtle(&data);
    let ledger = fluree
        .create_ledger(alias)
        .await
        .expect("create_ledger");
    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };
    let _result = fluree
        .insert_turtle_with_opts(
            ledger,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("populate insert");
    // Dropping `fluree` here is what makes the next open "cold." Any
    // global static caches (LedgerManager, etc.) keyed by ledger_id may
    // still warm-cache; that's acceptable because production sees the
    // same caches.
}

/// One representative SPARQL query for the cold_load_plus_query scenario.
const QUERY: &str = r"
PREFIX ex: <http://example.org/ns/>
SELECT ?p ?name WHERE {
  ?p a ex:Person ;
     ex:name ?name .
}
LIMIT 10
";

fn bench_query_cold_reload(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let base_nodes = scale_base_nodes(scale);

    eprintln!(
        "  [query_cold_reload] scale={} base_nodes={}",
        scale.as_str(),
        base_nodes
    );

    let mut group = c.benchmark_group("query_cold_reload");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    // --- Scenario 1: cold load only ---
    group.bench_with_input(
        BenchmarkId::new("cold_load", scale.as_str()),
        &base_nodes,
        |b, &n| {
            b.iter_batched(
                // Setup: populate a fresh tmpdir, drop the populate
                // connection, leave the directory ready for cold open.
                // NOT measured.
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let alias = next_ledger_alias("cold-reload");
                        populate(db_dir.path(), &alias, n).await;
                        (db_dir, alias)
                    })
                },
                // Measured: build a fresh Fluree at the same path and
                // load the ledger.
                |(db_dir, alias)| {
                    rt.block_on(async {
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .build()
                                .expect("build file-backed Fluree (cold)");
                        let snapshot = fluree
                            .graph(&alias)
                            .load()
                            .await
                            .expect("cold graph load");
                        black_box(snapshot);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    // --- Scenario 2: cold load + first query ---
    // (note: setup mirrors scenario 1; alias is regenerated per iteration)
    group.bench_with_input(
        BenchmarkId::new("cold_load_plus_query", scale.as_str()),
        &base_nodes,
        |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let db_dir = tempfile::tempdir().expect("db tmpdir");
                        let alias = next_ledger_alias("cold-reload");
                        populate(db_dir.path(), &alias, n).await;
                        (db_dir, alias)
                    })
                },
                |(db_dir, alias)| {
                    rt.block_on(async {
                        let fluree =
                            FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
                                .build()
                                .expect("build file-backed Fluree (cold+query)");
                        let snapshot = fluree
                            .graph(&alias)
                            .load()
                            .await
                            .expect("cold graph load");
                        let result = snapshot
                            .query()
                            .sparql(QUERY)
                            .execute()
                            .await
                            .expect("first query");
                        black_box(result);
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_query_cold_reload);
criterion_main!(benches);
