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
async fn cypher_property_accessor_in_where_filters_results() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:prop-accessor-where";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Three Person nodes with different ages.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:age": 25},
            {"@id": "ex:bob",   "@type": "ex:Person", "ex:age": 35},
            {"@id": "ex:carol", "@type": "ex:Person", "ex:age": 45},
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // Cypher property-accessor filter: only Bob and Carol are > 30.
    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age > 30 RETURN n")
        .await
        .expect("cypher property-accessor query");
    assert_eq!(
        result.row_count(),
        2,
        "expected exactly Bob and Carol (age > 30)"
    );
}

#[tokio::test]
async fn cypher_property_accessor_is_nullable_for_missing_property() {
    // Regression: WHERE n.missing IS NULL must match nodes that
    // lack the property. A mandatory-join lowering would
    // unconditionally drop them.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:nullable-prop";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Alice has an age; Bob doesn't.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:age": 25},
            {"@id": "ex:bob",   "@type": "ex:Person"},
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // IS NULL: only Bob.
    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age IS NULL RETURN n")
        .await
        .expect("cypher IS NULL query");
    assert_eq!(
        result.row_count(),
        1,
        "IS NULL on a missing property must match the node without it"
    );

    // RETURN n.name across sparse property: even with no names, we
    // get one row per Person — both Alice and Bob — with null name
    // for Alice (no name set in this seed) and null name for Bob.
    // The key contract is row preservation, not null surfacing.
    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n.name")
        .await
        .expect("cypher RETURN of sparse property");
    assert_eq!(
        result.row_count(),
        2,
        "RETURN of a sparse property must not drop rows for nodes lacking it"
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
async fn transact_cypher_set_property_replaces_old_value() {
    // End-to-end: seed via JSON-LD, MATCH … SET via Cypher, read back.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:set-prop");

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 25,
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) SET n.age = 42"#,
        )
        .await
        .expect("cypher set");

    let db = graphdb_from_ledger(&updated.ledger);
    // New value present.
    let hi = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age > 40 RETURN n")
        .await
        .expect("query new age");
    assert_eq!(hi.row_count(), 1, "age should now be 42");
    // Old value gone (single-valued, not accumulated).
    let lo = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age < 30 RETURN n")
        .await
        .expect("query old age");
    assert_eq!(lo.row_count(), 0, "old age 25 should have been retracted");
}

#[tokio::test]
async fn transact_cypher_set_label_adds_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:set-label");

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) SET n:Employee"#,
        )
        .await
        .expect("cypher set label");

    let db = graphdb_from_ledger(&updated.ledger);
    let rows = fluree
        .query_cypher(&db, "MATCH (n:Employee) RETURN n")
        .await
        .expect("query new label");
    assert_eq!(rows.row_count(), 1, "node should now carry the Employee label");
}

#[tokio::test]
async fn transact_cypher_remove_property_retracts_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:remove-prop");

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 25,
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) REMOVE n.age"#,
        )
        .await
        .expect("cypher remove");

    let db = graphdb_from_ledger(&updated.ledger);
    let nulls = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age IS NULL RETURN n")
        .await
        .expect("query removed prop");
    assert_eq!(nulls.row_count(), 1, "age should have been removed");
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
