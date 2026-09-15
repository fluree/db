//! Two known endpoints in a wide edge star: VALUES must constrain the probes.
//!
//! Compare two multi-row VALUES with the FILTER IN workaround. Singleton and
//! broad-set controls protect existing star plans. Tiny=6k, small=24k,
//! medium=129k (the report's scale), large=500k edges; 256-byte snapshots.
//!
//! cargo bench -p fluree-db-api --bench query_hot_values_star
//! FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_values_star
//! cargo bench -p fluree-db-api --bench query_hot_values_star -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write;

async fn setup(n: usize) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = next_ledger_alias("query-hot-values-star");
    let ledger = fluree.create_ledger(&alias).await.unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    let payload = "x".repeat(256);
    for i in 0..n {
        let (a, b) = match i {
            0 => ("A".to_owned(), "B".to_owned()),
            1 => ("B".to_owned(), "A".to_owned()),
            2 => ("A".to_owned(), "C".to_owned()),
            _ => (format!("item{}", i * 2), format!("item{}", i * 2 + 1)),
        };
        writeln!(ttl, "ex:edge{i} ex:entity1 ex:{a} ; ex:entity2 ex:{b} ; ex:snap \"{i} {payload}\" ; ex:kind ex:Edge .").unwrap();
    }
    fluree
        .insert_turtle_with_opts(
            ledger,
            &ttl,
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .unwrap();
    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .unwrap();
    (dir, fluree, alias)
}

fn bench_query_hot_values_star(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = match scale {
        BenchScale::Tiny => 6_000,
        BenchScale::Small => 24_000,
        BenchScale::Medium => 129_000,
        BenchScale::Large => 500_000,
    };
    let (_dir, fluree, alias) = rt.block_on(setup(n));
    let graph = rt.block_on(async { fluree.graph(&alias).load().await.unwrap() });
    let mut group = c.benchmark_group("query_hot_values_star");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    for (name, constraints, tail) in [
        (
            "two_values",
            "VALUES ?a { ex:A ex:B } VALUES ?b { ex:A ex:B }",
            "",
        ),
        (
            "filter_in",
            "VALUES ?a { ex:A ex:B } FILTER(?b IN (ex:A, ex:B))",
            "",
        ),
        (
            "singleton_anchor",
            "VALUES ?a { ex:A } VALUES ?b { ex:B ex:C }",
            "",
        ),
        (
            "broad_values",
            "VALUES ?kind { ex:Edge ex:Missing } ?ev ex:kind ?kind .",
            "LIMIT 10",
        ),
    ] {
        let sparql = format!(
            "PREFIX ex: <http://example.org/> SELECT ?a ?b ?snap WHERE {{
            {constraints} ?ev ex:snap ?snap ; ex:entity1 ?a ; ex:entity2 ?b . }} {tail}"
        );
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &sparql, |b, q| {
            b.iter(|| {
                rt.block_on(async { black_box(graph.query().sparql(q).execute().await.unwrap()) })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_query_hot_values_star);
criterion_main!(benches);
