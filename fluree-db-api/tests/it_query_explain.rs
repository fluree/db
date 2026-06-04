//! Explain API integration tests
//!
//! The native/statistics-backed tests live in `it_query_explain_native.rs`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, graphdb_from_ledger};

#[tokio::test]
async fn explain_no_stats_reports_none_and_reason() {
    // Scenario: explain-no-stats-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "no-stats:main");

    // Ensure the `ex` namespace is allocated (so query parsing can encode IRIs),
    // but do NOT run indexing so stats remain unavailable.
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?person"],
        "where": [{"@id":"?person","ex:name":"?name"}]
    });

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree.explain(&db, &q).await.expect("explain");
    assert_eq!(resp["plan"]["optimization"], "none");
    assert_eq!(resp["plan"]["reason"], "No statistics available");
    assert!(resp.get("query").is_some());
    assert!(resp["plan"].get("where-clause").is_some());
}

#[tokio::test]
async fn explain_sparql_no_stats_reports_none_and_reason() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "no-stats-sparql:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person WHERE { ?person ex:name ?name }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");
    assert_eq!(resp["plan"]["optimization"], "none");
    assert_eq!(resp["plan"]["reason"], "No statistics available");
    assert!(resp.get("query").is_some());
    // SPARQL explain does not include where-clause (that's a JSON-LD concept)
    assert!(resp["plan"].get("where-clause").is_none());
}

#[tokio::test]
async fn explain_logical_plan_preserves_compound_structure() {
    // The `logical` plan view is the compound-aware reorder_patterns order,
    // available even without stats. Verify a triple + OPTIONAL render as a
    // `triple` node and an `optional` node containing its inner triple.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "logical-compound:main");

    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@id":"ex:alice",
                "ex:name":"Alice",
                "ex:email":"alice@example.org"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?person ?email WHERE { ?person ex:name ?name . OPTIONAL { ?person ex:email ?email } }";

    let db = graphdb_from_ledger(&ledger);
    let resp = fluree
        .explain_sparql(&db, sparql)
        .await
        .expect("explain_sparql");

    let logical = resp["plan"]["logical"]
        .as_array()
        .expect("plan.logical is an array");
    assert!(!logical.is_empty(), "logical plan should not be empty");

    // Every node carries a kind + category.
    assert!(logical
        .iter()
        .all(|n| n.get("kind").is_some() && n.get("category").is_some()));

    // The OPTIONAL renders as an expander node holding its inner triple,
    // not flattened away.
    let optional = logical
        .iter()
        .find(|n| n["kind"] == "optional")
        .expect("optional node present in logical plan");
    assert_eq!(optional["category"], "expander");
    let inner = optional["patterns"]
        .as_array()
        .expect("optional has inner patterns");
    assert!(inner.iter().any(|n| n["kind"] == "triple"));

    // The required triple is a source.
    let triple = logical
        .iter()
        .find(|n| n["kind"] == "triple")
        .expect("required triple present");
    assert_eq!(triple["category"], "source");
}
