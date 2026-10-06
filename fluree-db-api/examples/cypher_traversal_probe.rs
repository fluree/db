//! Characterization probe for the remaining large-scale traversal gaps:
//! `expansion_1`, `pattern_long`, and `shortest_path_with_filter`. Each is a
//! multi-hop neighbor expansion; the question is whether it uses the batched
//! galloping frontier expansion (the shortestPath raw-id lane) or per-node
//! dict-resolved reads. Runs against an indexed `knows`-graph and reports the
//! per-phase timings; set `RUST_LOG=fluree_db_query=debug` to see which
//! operators / lookups each plan uses.
//!
//! ```bash
//! FLUREE_CYPHER_ALLOW_FULL_SCAN=1 PROBE_USERS=200000 \
//!   cargo run --release --example cypher_traversal_probe -p fluree-db-api
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
    let r = fluree.query_cypher(db, text).await.expect("warmup");
    let result = r.to_cypher_typed_table(db).await.expect("warmup fmt");
    let mut samples = Vec::new();
    for _ in 0..10 {
        let t0 = Instant::now();
        let r = fluree.query_cypher(db, text).await.expect("query");
        std::hint::black_box(r.to_cypher_typed_table(db).await.expect("fmt"));
        samples.push((Instant::now() - t0).as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let shown: String = format!("{result:?}").chars().take(80).collect();
    eprintln!("  {name:<28} {:>9.3} ms   {shown}", samples[5]);
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

    // Users with ~8 outbound `knows` edges each (a connected-ish graph so the
    // multi-hop patterns and shortest paths resolve).
    let graph: Vec<_> = (0..users)
        .map(|i| {
            json!({
                "@id": format!("u{i}"),
                "@type": "User",
                "id": i,
                // Ages span 0..40 so ~45% are < 18 → the shortestPath filter
                // `all(node.age >= 18)` actually prunes candidate paths (as in
                // pokec), unlike a uniformly-adult graph.
                "age": i % 40,
                "knows": (1..=8u64).map(|k| {
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
    let db = fluree.db("probe:main").await.expect("db");

    let src = env_usize("PROBE_SRC", 4112) as u64;
    // A likely-reachable target within 15 hops on the hashed graph. Default is
    // an adult (id % 40 >= 18) so the `all(age >= 18)` filter can admit a path.
    let dst = env_usize("PROBE_DST", 10500) as u64;

    let statements: Vec<(&str, String)> = vec![
        (
            "expansion_1",
            format!("MATCH (s:User {{id: {src}}})-->(n:User) RETURN n.id"),
        ),
        (
            "expansion_1_filter",
            format!("MATCH (s:User {{id: {src}}})-->(n:User) WHERE n.age >= 18 RETURN n.id"),
        ),
        (
            "pattern_short",
            format!("MATCH (n:User {{id: {src}}})-[e]->(m) RETURN m LIMIT 1"),
        ),
        (
            "pattern_long",
            format!(
                "MATCH (n1:User {{id: {src}}})-[e1]->(n2)-[e2]->(n3)-[e3]->(n4)<-[e4]-(n5) \
                 RETURN n5 LIMIT 1"
            ),
        ),
        (
            "shortest_path",
            format!(
                "MATCH (n:User {{id: {src}}}), (m:User {{id: {dst}}}) WITH n, m \
                 MATCH p=shortestPath((n)-[*..15]->(m)) RETURN [x in nodes(p) | x.id] AS path"
            ),
        ),
        (
            "shortest_path_filter",
            format!(
                "MATCH (n:User {{id: {src}}}), (m:User {{id: {dst}}}) WITH n, m \
                 MATCH p=shortestPath((n)-[*..15]->(m)) \
                 WHERE all(x in nodes(p) WHERE x.age >= 18) RETURN [x in nodes(p) | x.id] AS path"
            ),
        ),
        (
            // Trivially-true filter: must return the SAME path as unfiltered
            // (guards against an early-exit bug in the constrained search).
            "shortest_path_filter_all",
            format!(
                "MATCH (n:User {{id: {src}}}), (m:User {{id: {dst}}}) WITH n, m \
                 MATCH p=shortestPath((n)-[*..15]->(m)) \
                 WHERE all(x in nodes(p) WHERE x.age >= 0) RETURN [x in nodes(p) | x.id] AS path"
            ),
        ),
    ];

    // Tight-loop mode for CPU sampling: PROBE_LOOP=<n> runs the single selected
    // statement n times (no timing/format noise from the table print).
    if let (Ok(loops), Some(only)) = (std::env::var("PROBE_LOOP"), &single) {
        let loops: usize = loops.parse().unwrap_or(200_000);
        let text = &statements
            .iter()
            .find(|(n, _)| n == only)
            .expect("PROBE_SINGLE")
            .1;
        eprintln!("looping {only} x{loops} (pid {})", std::process::id());
        for _ in 0..loops {
            let r = fluree.query_cypher(&db, text).await.expect("loop query");
            std::hint::black_box(r.to_cypher_typed_table(&db).await.expect("loop fmt"));
        }
        return;
    }

    eprintln!("== indexed {users}-user knows-graph ==");
    for (name, text) in &statements {
        if let Some(only) = &single {
            if name != only {
                continue;
            }
        }
        time_query(&fluree, &db, name, text).await;
    }
}
