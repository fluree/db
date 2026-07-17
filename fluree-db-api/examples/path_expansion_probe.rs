//! Probe the property-path batched raw-id lane: time multi-hop expansion
//! queries (fused anonymous-hop runs → exact-depth wildcard property paths)
//! over an **indexed** social graph.
//!
//! ```bash
//! cargo run --release --example path_expansion_probe -p fluree-db-api
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let users = env_usize("PROBE_USERS", 20_000);
    let degree = env_usize("PROBE_DEG", 10) as u64;
    let iters = env_usize("PROBE_ITERS", 10);
    println!("users={users} degree={degree} iters={iters}");

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let ledger = fluree.create_ledger("probe:main").await.expect("ledger");

    // Pokec-ish: User nodes with an id and 10 scattered knows edges
    // (multiplicative hash — BFS frontiers on real graphs are uniform over
    // the id space, not clustered).
    let graph: Vec<_> = (0..users)
        .map(|i| {
            json!({
                "@id": format!("u{i}"),
                "@type": "User",
                "id": i,
                "knows": (1..=degree).map(|k| {
                    let target = ((i as u64 + k).wrapping_mul(2_654_435_761)) % users as u64;
                    json!({"@id": format!("u{target}")})
                }).collect::<Vec<_>>()
            })
        })
        .collect();
    let t = Instant::now();
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed");
    println!("insert: {:?}", t.elapsed());
    let t = Instant::now();
    fluree
        .reindex("probe:main", ReindexOptions::default())
        .await
        .expect("reindex");
    println!("reindex: {:?}", t.elapsed());

    let queries: &[(&str, String)] = &[
        (
            "expansion_2",
            "MATCH (s:User {id: 0})-->()-->(n:User) RETURN DISTINCT n.id AS nid".into(),
        ),
        (
            "expansion_3",
            "MATCH (s:User {id: 0})-->()-->()-->(n:User) RETURN DISTINCT n.id AS nid".into(),
        ),
        (
            "expansion_4",
            "MATCH (s:User {id: 0})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id AS nid".into(),
        ),
    ];

    let view = fluree.db("probe:main").await.expect("view");
    for (name, query) in queries {
        // Warmup
        let rows = fluree
            .query_cypher(&view, query)
            .await
            .expect("warmup")
            .row_count();
        let mut times = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let r = fluree.query_cypher(&view, query).await.expect("query");
            times.push(t.elapsed());
            assert_eq!(r.row_count(), rows);
        }
        let mean = times.iter().sum::<std::time::Duration>() / times.len() as u32;
        let min = times.iter().min().unwrap();
        println!("{name}: rows={rows} mean={mean:?} min={min:?}");
    }
}
