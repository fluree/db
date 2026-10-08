//! Repro probe for the bare `MATCH (n)` + property-read aggregation cliff.
//!
//! `MATCH (n) RETURN count(n), count(n.age)` and `MATCH (n) RETURN
//! min/max/avg(n.age)` measured ~0.5–15 ms on a freshly-reindexed (clean)
//! large ledger but cliffed to ~67 s once the ledger carried write-novelty.
//! This probe runs each statement CLEAN (right after reindex) and then DIRTY
//! (after appending a few un-reindexed nodes) so the novelty gate is isolated
//! in-process at a scale that shows the cliff without a full AWS run.
//!
//! ```bash
//! PROBE_USERS=200000 cargo run --release --example cypher_agg_cliff_probe -p fluree-db-api
//! ```

use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn time_query(fluree: &Fluree, db: &fluree_db_api::GraphDb, name: &str, text: &str) {
    // One warmup, then median of 3.
    let r = fluree.query_cypher(db, text).await.expect("warmup");
    let result = r.to_cypher_typed_table(db).await.expect("warmup fmt");
    let mut samples = Vec::new();
    for _ in 0..3 {
        let t0 = Instant::now();
        let r = fluree.query_cypher(db, text).await.expect("query");
        std::hint::black_box(r.to_cypher_typed_table(db).await.expect("fmt"));
        samples.push((Instant::now() - t0).as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let val: String = format!("{result:?}").chars().take(110).collect();
    eprintln!("  {name:<20} {:>10.3} ms   {val}", samples[1]);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    let users = env_usize("PROBE_USERS", 200_000);
    let single = std::env::var("PROBE_SINGLE").ok();

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let ledger = fluree.create_ledger("probe:main").await.expect("ledger");

    let graph: Vec<_> = (0..users)
        .map(|i| {
            json!({
                "@id": format!("u{i}"),
                "@type": "User",
                "id": i,
                "name": format!("user{i}"),
                "age": 18 + (i % 60),
                "knows": (1..=4u64).map(|k| {
                    let target = ((i as u64 + k).wrapping_mul(2_654_435_761)) % users as u64;
                    json!({"@id": format!("u{target}")})
                }).collect::<Vec<_>>()
            })
        })
        .collect();
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed");
    fluree
        .reindex("probe:main", ReindexOptions::default())
        .await
        .expect("reindex");

    let statements: &[(&str, &str)] = &[
        ("count", "MATCH (n) RETURN count(n), count(n.age)"),
        (
            "min_max_avg",
            "MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)",
        ),
        (
            "count_distinct_lbl",
            "MATCH (n:User) RETURN COUNT(DISTINCT n.age)",
        ),
        ("count_all_only", "MATCH (n) RETURN count(n)"),
    ];

    eprintln!("== CLEAN ledger (fresh reindex, {users} nodes) ==");
    let db = fluree.db("probe:main").await.expect("db");
    for (name, text) in statements {
        time_query(&fluree, &db, name, text).await;
    }

    // Append un-reindexed nodes → the ledger now carries novelty.
    let novelty = env_usize("PROBE_NOVELTY", 5);
    let extra: Vec<_> = (users..users + novelty)
        .map(|i| json!({"@id": format!("u{i}"), "@type": "User", "age": 42}))
        .collect();
    let ledger = fluree.ledger("probe:main").await.expect("ledger2");
    fluree
        .insert(ledger, &json!({"@graph": extra}))
        .await
        .expect("novelty insert");

    eprintln!("== DIRTY ledger (+5 un-reindexed nodes → novelty present) ==");
    let db = fluree.db("probe:main").await.expect("db2");
    for (name, text) in statements {
        if let Some(only) = &single {
            if name != only {
                continue;
            }
        }
        time_query(&fluree, &db, name, text).await;
    }
}
