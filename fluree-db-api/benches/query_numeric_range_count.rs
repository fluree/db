//! Numeric range aggregates with no novelty, unrelated writes, and matching writes.
//! `FLUREE_NUMERIC_BENCH_ROWS` changes the default 100,000 facts per predicate.
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb};
use serde_json::{json, Value};
use std::time::Duration;

const LEDGER: &str = "bench/numeric:main";
const EX: &str = "http://example.org/";

async fn run(fluree: &Fluree, db: &GraphDb, query: &str, expected: &Value) {
    let result = fluree.query(db, query).await.expect("aggregate query");
    assert_eq!(result.to_jsonld(&db.snapshot).unwrap(), *expected);
    black_box(result);
}

fn bench(c: &mut Criterion) {
    let rt = fluree_bench_support::bench_runtime();
    let _runtime = rt.enter();
    let rows: i64 = std::env::var("FLUREE_NUMERIC_BENCH_ROWS")
        .ok()
        .map(|s| s.parse().expect("FLUREE_NUMERIC_BENCH_ROWS"))
        .unwrap_or(100_000);
    assert!(rows >= 1_000 && rows % 2 == 0);
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let mut ttl = String::new();
    for i in 0..rows {
        ttl.push_str(&format!(
            "<{EX}s{i}> <{EX}n> {} ; <{EX}title> \"title {:04}\" .\n",
            i - rows / 2,
            i % 1_000
        ));
    }
    std::fs::write(src.path().join("numbers.ttl"), ttl).unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let (indexed, unrelated, novelty) = rt.block_on(async {
        fluree
            .create(LEDGER)
            .import(src.path())
            .threads(2)
            .memory_budget_mb(256)
            .execute()
            .await
            .unwrap();
        let indexed = fluree.db(LEDGER).await.unwrap();
        let handle = fluree.ledger_cached(LEDGER).await.unwrap();
        fluree
            .stage(&handle)
            .update(
                &json!({"insert":[{"@id":format!("{EX}note"),format!("{EX}note"):"unrelated"}]}),
            )
            .execute()
            .await
            .unwrap();
        let unrelated = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
        fluree
            .stage(&handle)
            .update(&json!({
                "delete":[{"@id":format!("{EX}s{}", rows - 1), format!("{EX}n"):rows / 2 - 1}],
                "insert":[{"@id":format!("{EX}new"), format!("{EX}n"):rows / 2}]
            }))
            .execute()
            .await
            .unwrap();
        let novelty = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
        (indexed, unrelated, novelty)
    });
    let mut group = c.benchmark_group("numeric_range_count");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    for (state, view) in [
        ("indexed", &indexed),
        ("unrelated", &unrelated),
        ("novelty", &novelty),
    ] {
        for (form, query, expected) in [
            (
                "count_half",
                format!("SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}n> ?o FILTER(?o >= 0) }}"),
                json!([[rows / 2]]),
            ),
            (
                "sum_half",
                format!("SELECT (SUM(?o >= 0) AS ?n) WHERE {{ ?s <{EX}n> ?o }}"),
                json!([[rows / 2]]),
            ),
            (
                "count_all",
                format!(
                    "SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}n> ?o FILTER(?o > {}) }}",
                    -rows
                ),
                json!([[rows]]),
            ),
            (
                "min",
                format!("SELECT (MIN(?o) AS ?n) WHERE {{ ?s <{EX}n> ?o }}"),
                json!([[-rows / 2]]),
            ),
            (
                "strlen",
                format!("SELECT (SUM(STRLEN(?o)) AS ?n) WHERE {{ ?s <{EX}title> ?o }}"),
                json!([[10 * rows]]),
            ),
        ] {
            rt.block_on(run(&fluree, view, &query, &expected));
            group.bench_with_input(
                BenchmarkId::new(format!("{state}/{form}"), rows),
                &query,
                |b, query| {
                    b.to_async(&rt)
                        .iter(|| run(&fluree, view, query, &expected));
                },
            );
        }
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
