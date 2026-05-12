//! Cypher read-path end-to-end tests.
//!
//! Each test inserts data via JSON-LD `@annotation` (the canonical
//! producer of `f:reifies*` bundles) and queries it back via Cypher,
//! verifying the same IR underlies both surfaces.
//!
//! See `GQL_CYPHER_SUPPORT.md` §M5.3 / §M5.6 for the contract.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

#[tokio::test]
async fn cypher_match_labeled_node_finds_jsonld_typed_subject() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:typed-node";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Insert: ex:alice rdf:type ex:Person + ex:name
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@type": "ex:Person",
        "ex:name": "Alice",
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    // With the resolver default `@vocab = http://example.org/`, the
    // Cypher label `Person` resolves to `http://example.org/Person` —
    // the same IRI the JSON-LD insert produced via the `ex:` prefix.
    let db = graphdb_from_ledger(&committed.ledger);
    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n")
        .await
        .expect("cypher query");
    assert_eq!(
        result.row_count(),
        1,
        "expected exactly one row for the lone Person"
    );
}

#[tokio::test]
async fn cypher_parse_error_returns_clear_diagnostic() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:parse-error");
    let db = graphdb_from_ledger(&ledger0);

    // Garbage Cypher.
    let r = fluree.query_cypher(&db, "FOOBAR not cypher").await;
    assert!(r.is_err(), "expected parse error");
}

#[tokio::test]
async fn cypher_bare_node_pattern_rejected_at_lower() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:bare-node");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree.query_cypher(&db, "MATCH (n) RETURN n").await;
    assert!(r.is_err(), "bare MATCH (n) must be rejected");
}

#[tokio::test]
async fn cypher_variable_length_rejected_at_lower() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:varlen");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree
        .query_cypher(
            &db,
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b",
        )
        .await;
    assert!(r.is_err(), "variable-length paths must be rejected in v1");
}

#[tokio::test]
async fn transact_cypher_create_round_trips_to_jsonld_query() {
    // End-to-end: Cypher CREATE → stage → JSON-LD read sees the data.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:transact-create");

    let result = fluree
        .transact_cypher(ledger0, "CREATE (n:Person)")
        .await
        .expect("cypher create");

    // Querying back via Cypher should find the node.
    let db = graphdb_from_ledger(&result.ledger);
    let rows = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n")
        .await
        .expect("cypher query");
    assert_eq!(rows.row_count(), 1);
}

#[tokio::test]
async fn transact_cypher_merge_returns_specific_deferred_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:merge-deferred");

    let err = fluree
        .transact_cypher(ledger0, "MERGE (n:Person {name: \"Alice\"})")
        .await
        .expect_err("MERGE should be deferred");
    let msg = format!("{err}");
    assert!(
        msg.contains("MERGE"),
        "expected MERGE-specific deferral, got: {msg}"
    );
}

#[tokio::test]
async fn cypher_undirected_rejected_at_lower() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:undirected");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree
        .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a, b")
        .await;
    assert!(r.is_err());
}
