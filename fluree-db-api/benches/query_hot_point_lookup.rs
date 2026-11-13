//! One subject's one property, through the query entry point: what every
//! query pays before and after the work that is its own.
//!
//! A point lookup runs for tens of microseconds, so a cost added per query
//! (a clone of the view, a lookup, an allocation) shows here as a percentage
//! and nowhere else. The ledger is indexed with one commit of novelty over
//! it, as a served ledger usually is. `execute` stops at the result;
//! `execute_formatted` renders it as JSON-LD, which is where a result's
//! retained read state is used. Tiny=1k, small=10k, medium=50k, large=200k
//! people.
//!
//! cargo bench -p fluree-db-api --bench query_hot_point_lookup
//! FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_point_lookup
//! cargo bench -p fluree-db-api --bench query_hot_point_lookup -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use serde_json::json;
use std::fmt::Write;

fn no_auto_index() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    }
}

async fn setup(n: usize) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = next_ledger_alias("query-hot-point-lookup");
    let ledger = fluree.create_ledger(&alias).await.unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        writeln!(
            ttl,
            "ex:p{i} a ex:Person ; ex:name \"person {i}\" ; ex:age {} .",
            20 + i % 60
        )
        .unwrap();
    }
    let indexed = fluree
        .insert_turtle_with_opts(
            ledger,
            &ttl,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto_index(),
            None,
        )
        .await
        .unwrap();
    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .unwrap();
    drop(indexed);
    let ledger = fluree.ledger(&alias).await.unwrap();
    fluree
        .insert_turtle_with_opts(
            ledger,
            "@prefix ex: <http://example.org/> .\nex:novel a ex:Person ; ex:name \"novel\" .",
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto_index(),
            None,
        )
        .await
        .unwrap();
    (dir, fluree, alias)
}

fn bench_query_hot_point_lookup(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = match scale {
        BenchScale::Tiny => 1_000,
        BenchScale::Small => 10_000,
        BenchScale::Medium => 50_000,
        BenchScale::Large => 200_000,
    };
    let (_dir, fluree, alias) = rt.block_on(setup(n));
    let graph = rt.block_on(async { fluree.graph(&alias).load().await.unwrap() });
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?n",
        "where": {"@id": "ex:p17", "ex:name": "?n"}
    });
    let mut group = c.benchmark_group("query_hot_point_lookup");
    group.sample_size(profile.sample_size());
    group.bench_with_input(
        BenchmarkId::new("execute", scale.as_str()),
        &query,
        |b, q| {
            b.iter(|| {
                rt.block_on(async { black_box(graph.query().jsonld(q).execute().await.unwrap()) })
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("execute_formatted", scale.as_str()),
        &query,
        |b, q| {
            b.iter(|| {
                rt.block_on(async {
                    black_box(graph.query().jsonld(q).execute_formatted().await.unwrap())
                })
            });
        },
    );
    group.finish();
}

criterion_group!(benches, bench_query_hot_point_lookup);
criterion_main!(benches);
