//! `collect` / `collect-distinct` JSON-LD select-aggregate tests.
//!
//! `collect` gathers every non-unbound value in a group into a first-class list
//! value (rendered as a JSON array). `collect-distinct` deduplicates. Being the
//! inverse of UNWIND, a `collect` result is a list value that `unwind` re-expands.
//!
//! Note: RDF triples are a set, so duplicate *identical triples* are stored
//! once — the difference between `collect` and `collect-distinct` only shows up
//! when the same value arrives on multiple *rows* (e.g. via a join). The data
//! below gives Alice two papers on the same subject so the join produces a
//! duplicate row.

mod support;

use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger, MemoryFluree};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

async fn seed_papers(fluree: &MemoryFluree, id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, id);
    // Alice wrote 3 papers — two on "AI", one on "ML"; Bob wrote 1 on "AI".
    // The author→paper→subject join yields "AI" twice for Alice.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice",
             "ex:authored": [{"@id": "ex:p1"}, {"@id": "ex:p2"}, {"@id": "ex:p3"}]},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:authored": {"@id": "ex:p4"}},
            {"@id": "ex:p1", "ex:subject": "AI"},
            {"@id": "ex:p2", "ex:subject": "AI"},
            {"@id": "ex:p3", "ex:subject": "ML"},
            {"@id": "ex:p4", "ex:subject": "AI"}
        ]
    });
    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

/// Sort a row's collected-list cell so assertions are order-independent.
fn sorted_list(cell: &JsonValue) -> Vec<String> {
    let mut v: Vec<String> = cell
        .as_array()
        .expect("list cell")
        .iter()
        .map(|x| x.as_str().expect("string elem").to_string())
        .collect();
    v.sort();
    v
}

/// `collect(?s)` gathers a group's values into a list, keeping row-level
/// duplicates (Alice's two "AI" papers contribute two "AI" entries).
#[tokio::test]
async fn collect_gathers_group_values_into_list() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_papers(&fluree, "it/collect:basic").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?name", "(as (collect ?s) ?subjects)"],
        "where": [
            {"@id": "?a", "ex:name": "?name", "ex:authored": "?paper"},
            {"@id": "?paper", "ex:subject": "?s"}
        ],
        "groupBy": ["?a", "?name"],
        "orderBy": ["?name"]
    });
    let result = fluree.query(&db, &q).await.expect("collect query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let arr = rows.as_array().expect("array");

    let alice = arr[0].as_array().expect("row");
    let bob = arr[1].as_array().expect("row");
    assert_eq!(alice[0].as_str(), Some("Alice"));
    assert_eq!(sorted_list(&alice[1]), vec!["AI", "AI", "ML"]);
    assert_eq!(bob[0].as_str(), Some("Bob"));
    assert_eq!(sorted_list(&bob[1]), vec!["AI"]);
}

/// `collect-distinct(?s)` deduplicates within the group (Alice's two "AI" rows
/// collapse to a single "AI").
#[tokio::test]
async fn collect_distinct_deduplicates() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_papers(&fluree, "it/collect:distinct").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?name", "(as (collect-distinct ?s) ?subjects)"],
        "where": [
            {"@id": "?a", "ex:name": "?name", "ex:authored": "?paper"},
            {"@id": "?paper", "ex:subject": "?s"}
        ],
        "groupBy": ["?a", "?name"],
        "orderBy": ["?name"]
    });
    let result = fluree.query(&db, &q).await.expect("collect-distinct query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let arr = rows.as_array().expect("array");
    let alice = arr[0].as_array().expect("row");
    assert_eq!(alice[0].as_str(), Some("Alice"));
    assert_eq!(
        sorted_list(&alice[1]),
        vec!["AI", "ML"],
        "duplicate 'AI' collapsed by collect-distinct"
    );
}

/// collect → unwind round-trip: a sub-select collects Alice's distinct subjects
/// into a list, and the outer UNWIND re-expands it to one row per subject.
#[tokio::test]
async fn collect_then_unwind_round_trip() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_papers(&fluree, "it/collect:roundtrip").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?subject"],
        "where": [
            ["query", {
                "@context": ctx(),
                "select": ["(as (collect-distinct ?s) ?subjects)"],
                "where": [
                    {"@id": "ex:alice", "ex:authored": "?paper"},
                    {"@id": "?paper", "ex:subject": "?s"}
                ]
            }],
            ["unwind", "?subject", "?subjects"]
        ],
        "orderBy": ["?subject"]
    });
    let result = fluree.query(&db, &q).await.expect("round-trip query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let subjects: Vec<String> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| match r {
            JsonValue::Array(c) => c[0].as_str().expect("str").to_string(),
            other => other.as_str().expect("str").to_string(),
        })
        .collect();
    assert_eq!(subjects, vec!["AI", "ML"]);
}
