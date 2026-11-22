//! Prefix aggregate latency with and without novelty over a bulk import.
//!
//! Run with `cargo bench -p fluree-db-api --bench query_string_prefix_count`.
//! `FLUREE_PREFIX_BENCH_ROWS` changes the default 100,000 facts. Both datasets
//! have the same row count: unique titles exercise dictionary traversal and
//! repeated titles isolate the cost of counting matching index rows.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb};
use serde_json::json;
use std::time::Duration;

const LEDGER: &str = "bench/prefix:main";
const EX: &str = "http://example.org/";

async fn run(fluree: &Fluree, db: &GraphDb, query: &str, expected: usize) {
    let result = fluree.query(db, query).await.expect("prefix query");
    assert_eq!(result.to_jsonld(&db.snapshot).unwrap(), json!([[expected]]));
    black_box(result);
}

fn bench(c: &mut Criterion) {
    let rt = fluree_bench_support::bench_runtime();
    let _runtime = rt.enter();
    let rows: usize = std::env::var("FLUREE_PREFIX_BENCH_ROWS")
        .ok()
        .map(|s| s.parse().expect("FLUREE_PREFIX_BENCH_ROWS"))
        .unwrap_or(100_000);
    assert!(rows >= 1_000 && rows.is_multiple_of(2));
    let mut group = c.benchmark_group("string_prefix_count");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    for repeated in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let mut ttl = String::new();
        for i in 0..rows {
            let prefix = if i % 2 == 0 { "Com" } else { "Other" };
            let title = if repeated { i % 1_024 } else { i };
            ttl.push_str(&format!(
                "<{EX}s{i}> <{EX}title> \"{prefix} title {title:06} with a moderately long suffix\" .\n"
            ));
        }
        std::fs::write(src.path().join("titles.ttl"), ttl).unwrap();
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .unwrap();
        let (indexed, novelty) =
            rt.block_on(async {
                fluree
                    .create(LEDGER)
                    .import(src.path())
                    .threads(2)
                    .memory_budget_mb(256)
                    .execute()
                    .await
                    .unwrap();
                let indexed = fluree.db(LEDGER).await.unwrap();
                assert!(indexed.binary_store().unwrap().lex_sorted_string_ids());
                let handle = fluree.ledger_cached(LEDGER).await.unwrap();
                fluree.stage(&handle).update(&json!({
                "delete": [{"@id": format!("{EX}s0"), format!("{EX}title"):
                    "Com title 000000 with a moderately long suffix"}],
                "insert": [{"@id": format!("{EX}new"), format!("{EX}title"): "Com new title"}]
            })).execute().await.unwrap();
                let novelty =
                    GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
                (indexed, novelty)
            });
        for (state, view) in [("indexed", &indexed), ("novelty", &novelty)] {
            for (form, query) in [
                ("strstarts", format!("SELECT (SUM(<http://www.w3.org/2001/XMLSchema#integer>(STRSTARTS(?o, \"Com\"))) AS ?n) WHERE {{ ?s <{EX}title> ?o }}")),
                ("regex", format!("SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}title> ?o FILTER REGEX(?o, \"^Com\") }}")),
            ] {
                let dataset = if repeated { "repeated" } else { "unique" };
                rt.block_on(run(&fluree, view, &query, rows / 2));
                group.bench_with_input(
                    BenchmarkId::new(format!("{dataset}/{state}/{form}"), rows),
                    &query,
                    |b, query| b.to_async(&rt).iter(|| run(&fluree, view, query, rows / 2)),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
