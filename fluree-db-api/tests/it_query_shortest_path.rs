//! `shortestPath` / `allShortestPaths` JSON-LD clause + `nodes` / `pathPairs`
//! path-value functions.
//!
//! Graph (ex:knows): a→b→d, a→c→d (two 2-hop paths), a→e→f→d (a 3-hop path).
//! So the shortest a→d distance is 2 hops, reachable two ways.

mod support;

use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger, MemoryFluree};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

async fn seed_graph(fluree: &MemoryFluree, id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, id);
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:knows": [{"@id": "ex:b"}, {"@id": "ex:c"}, {"@id": "ex:e"}]},
            {"@id": "ex:b", "ex:knows": {"@id": "ex:d"}},
            {"@id": "ex:c", "ex:knows": {"@id": "ex:d"}},
            {"@id": "ex:e", "ex:knows": {"@id": "ex:f"}},
            {"@id": "ex:f", "ex:knows": {"@id": "ex:d"}}
        ]
    });
    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

async fn rows(
    fluree: &MemoryFluree,
    ledger: &fluree_db_api::LedgerState,
    q: &JsonValue,
) -> Vec<JsonValue> {
    let db = graphdb_from_ledger(ledger);
    let result = fluree.query(&db, q).await.expect("query");
    result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows")
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn first_i64(row: &JsonValue) -> i64 {
    match row {
        JsonValue::Array(c) => c[0].as_i64().expect("i64"),
        other => other.as_i64().expect("i64"),
    }
}

/// `shortestPath` finds one shortest a→d path; `nodes(p)` has 3 nodes (2 hops).
#[tokio::test]
async fn shortest_path_single_node_count() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:single").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?len"],
        "where": [
            ["shortestPath", {"from": "ex:a", "to": "ex:d", "via": "ex:knows", "bind": "?p"}],
            ["bind", "?len", "(size (nodes ?p))"]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    assert_eq!(r.len(), 1, "shortestPath returns a single path");
    assert_eq!(first_i64(&r[0]), 3, "2-hop path has 3 nodes");
}

/// `allShortestPaths` returns both 2-hop a→d paths (via b and via c).
#[tokio::test]
async fn all_shortest_paths_returns_both() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:all").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?len"],
        "where": [
            ["allShortestPaths", {"from": "ex:a", "to": "ex:d", "via": "ex:knows", "bind": "?p"}],
            ["bind", "?len", "(size (nodes ?p))"]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    assert_eq!(r.len(), 2, "two equal-length shortest paths");
    assert!(r.iter().all(|row| first_i64(row) == 3));
}

/// `pathPairs(p)` yields the consecutive node pairs — 2 edges for a 2-hop path.
#[tokio::test]
async fn shortest_path_pathpairs_edge_count() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:pairs").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?edges"],
        "where": [
            ["shortestPath", {"from": "ex:a", "to": "ex:d", "via": "ex:knows", "bind": "?p"}],
            ["bind", "?edges", "(size (path-pairs ?p))"]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    assert_eq!(first_i64(&r[0]), 2);
}

/// `nodes(p)` is a list — `unwind` re-expands it into one row per node; the
/// path endpoints are a and d.
#[tokio::test]
async fn shortest_path_nodes_unwound() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:nodes").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            ["shortestPath", {"from": "ex:a", "to": "ex:d", "via": "ex:knows", "bind": "?p"}],
            ["unwind", "?n", "(nodes ?p)"]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    let iris: Vec<String> = r
        .iter()
        .map(|row| match row {
            JsonValue::Array(c) => c[0].as_str().unwrap_or_default().to_string(),
            other => other.as_str().unwrap_or_default().to_string(),
        })
        .collect();
    assert_eq!(iris.len(), 3, "3 nodes along the 2-hop path");
    assert_eq!(iris.first().map(String::as_str), Some("ex:a"));
    assert_eq!(iris.last().map(String::as_str), Some("ex:d"));
}

/// `maxHops` shorter than the true distance → no path → no rows.
#[tokio::test]
async fn shortest_path_maxhops_too_small() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:maxhops").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": [
            ["shortestPath", {"from": "ex:a", "to": "ex:d", "via": "ex:knows", "maxHops": 1, "bind": "?p"}]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    assert_eq!(r.len(), 0, "no 1-hop a→d path exists");
}

/// An unknown predicate IRI yields no rows (no edges of that type exist).
#[tokio::test]
async fn shortest_path_unknown_predicate_no_rows() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_graph(&fluree, "it/sp:nopred").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": [
            ["shortestPath", {"from": "ex:a", "to": "ex:d", "via": "ex:nonexistent", "bind": "?p"}]
        ]
    });
    let r = rows(&fluree, &ledger, &q).await;
    assert_eq!(r.len(), 0);
}
