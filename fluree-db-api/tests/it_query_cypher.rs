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
async fn transact_cypher_match_create_links_existing_nodes() {
    // MATCH binds Alice and Bob; CREATE links them with a new edge.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:match-create");

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
            {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let linked = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
               CREATE (a)-[:KNOWS]->(b)"#,
        )
        .await
        .expect("match-create");

    let db = graphdb_from_ledger(&linked.ledger);
    let rows = fluree
        .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
        .await
        .expect("query edge");
    assert_eq!(rows.row_count(), 1, "Alice KNOWS Bob should exist");
}

#[tokio::test]
async fn transact_cypher_match_create_mints_new_node_per_match() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:match-create-new");

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (a:Person {name: "Alice"})
               CREATE (a)-[:HAS_PET]->(p:Pet {name: "Rex"})"#,
        )
        .await
        .expect("match-create-new");

    let db = graphdb_from_ledger(&updated.ledger);
    let pets = fluree
        .query_cypher(&db, "MATCH (p:Pet) RETURN p")
        .await
        .expect("query pet");
    assert_eq!(pets.row_count(), 1, "a new Pet node should have been created");
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
async fn transact_cypher_set_null_removes_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:set-null");
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 25,
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) SET n.age = null"#,
        )
        .await
        .expect("cypher set null");

    let db = graphdb_from_ledger(&updated.ledger);
    let nulls = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age IS NULL RETURN n")
        .await
        .expect("query");
    assert_eq!(nulls.row_count(), 1, "SET age = null should remove it");
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
async fn cypher_query_with_parameter_filters() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:param-read");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
            {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let params = json!({ "name": "Alice" });
    let result = fluree
        .query_cypher_with_params(
            &db,
            "MATCH (n:Person {name: $name}) RETURN n",
            params.as_object(),
        )
        .await
        .expect("param query");
    assert_eq!(result.row_count(), 1, "only the matching name binds");
}

#[tokio::test]
async fn cypher_query_missing_parameter_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:param-missing");
    let db = graphdb_from_ledger(&ledger0);

    // No params supplied for `$name`.
    let r = fluree
        .query_cypher_with_params(&db, "MATCH (n:Person {name: $name}) RETURN n", None)
        .await;
    let err = format!("{}", r.expect_err("should error on missing param"));
    assert!(err.contains("name"), "error should name the param: {err}");
}

#[tokio::test]
async fn transact_cypher_with_parameters_creates_node() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:param-write");

    let params = json!({ "name": "Dana", "age": 27 });
    let result = fluree
        .transact_cypher_with_params(
            ledger0,
            "CREATE (n:Person {name: $name, age: $age})",
            params.as_object(),
        )
        .await
        .expect("param create");

    let db = graphdb_from_ledger(&result.ledger);
    let rows = fluree
        .query_cypher(&db, r#"MATCH (n:Person {name: "Dana"}) RETURN n"#)
        .await
        .expect("verify");
    assert_eq!(rows.row_count(), 1, "parameterized CREATE should persist");
}

#[tokio::test]
async fn transact_cypher_set_relationship_property() {
    // Bind a relationship variable in a write MATCH and update its metadata.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:set-rel-prop");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2000}]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    // Sanity: the edge has since=2000.
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS {since: 2000}]->(b) RETURN r")
            .await
            .expect("pre")
            .row_count(),
        1
    );

    // Update the relationship property via a bound relationship variable.
    let l = fluree
        .transact_cypher(l, "MATCH (a)-[r:KNOWS]->(b) SET r.since = 2020")
        .await
        .expect("set rel prop")
        .ledger;
    let db = graphdb_from_ledger(&l);

    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN r")
            .await
            .expect("post-new")
            .row_count(),
        1,
        "relationship now has since=2020"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS {since: 2000}]->(b) RETURN r")
            .await
            .expect("post-old")
            .row_count(),
        0,
        "old since=2000 retracted"
    );
}

#[tokio::test]
async fn transact_cypher_detach_delete_removes_node_and_both_directions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:detach-delete");

    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"CREATE (c:Person {name: "Carol"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
        r#"MATCH (c:Person {name: "Carol"}), (a:Person {name: "Alice"}) CREATE (c)-[:KNOWS]->(a)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    // Sanity: two KNOWS edges (Alice→Bob outbound, Carol→Alice inbound).
    let db = graphdb_from_ledger(&l);
    let edges = fluree
        .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
        .await
        .expect("edges");
    assert_eq!(edges.row_count(), 2, "two KNOWS edges before delete");

    // DETACH DELETE Alice — removes her node plus both directions.
    let l = fluree
        .transact_cypher(l, r#"MATCH (n:Person {name: "Alice"}) DETACH DELETE n"#)
        .await
        .expect("detach delete")
        .ledger;
    let db = graphdb_from_ledger(&l);

    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {name: "Alice"}) RETURN n"#)
            .await
            .expect("alice gone")
            .row_count(),
        0,
        "Alice's node should be gone"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
            .await
            .expect("edges gone")
            .row_count(),
        0,
        "both inbound and outbound KNOWS edges should be gone"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("survivors")
            .row_count(),
        2,
        "Bob and Carol should remain"
    );
}

#[tokio::test]
async fn transact_cypher_detach_delete_works_on_indexed_data() {
    // Same as above but the data is drained into the base index before the
    // delete, so the var-predicate scans and the reifier cascade run against
    // indexed flakes (not novelty/overlay).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:detach-delete-indexed";
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        std::sync::Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let mut l = genesis_ledger(&fluree, ledger_id);
            let mut last_t = 0;
            for stmt in [
                r#"CREATE (a:Person {name: "Alice"})"#,
                r#"CREATE (b:Person {name: "Bob"})"#,
                r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
            ] {
                let r = fluree.transact_cypher(l, stmt).await.expect(stmt);
                last_t = r.receipt.t;
                l = r.ledger;
            }

            // Drain novelty into the base index, then reload the indexed head.
            support::trigger_index_and_wait(&handle, ledger_id, last_t).await;
            let reloaded = fluree.ledger(ledger_id).await.expect("reload indexed");

            let after = fluree
                .transact_cypher(
                    reloaded,
                    r#"MATCH (n:Person {name: "Alice"}) DETACH DELETE n"#,
                )
                .await
                .expect("detach delete indexed")
                .ledger;
            let db = graphdb_from_ledger(&after);

            assert_eq!(
                fluree
                    .query_cypher(&db, r#"MATCH (n:Person {name: "Alice"}) RETURN n"#)
                    .await
                    .unwrap()
                    .row_count(),
                0,
                "Alice gone (indexed base)"
            );
            assert_eq!(
                fluree
                    .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
                    .await
                    .unwrap()
                    .row_count(),
                0,
                "KNOWS edge gone (indexed base)"
            );
            assert_eq!(
                fluree
                    .query_cypher(&db, "MATCH (n:Person) RETURN n")
                    .await
                    .unwrap()
                    .row_count(),
                1,
                "Bob remains"
            );
        })
        .await;
}

#[tokio::test]
async fn transact_cypher_merge_creates_then_is_a_noop() {
    // MERGE = find-or-create: the first run creates the node, the second run
    // finds the existing one and inserts nothing (single-Txn NOT EXISTS guard).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge");

    let l = fluree
        .transact_cypher(l, r#"MERGE (n:Person {name: "Alice"})"#)
        .await
        .expect("merge create")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("after first merge")
            .row_count(),
        1,
        "first MERGE creates the node"
    );

    // Second identical MERGE must not create a duplicate.
    let l = fluree
        .transact_cypher(l, r#"MERGE (n:Person {name: "Alice"})"#)
        .await
        .expect("merge match")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("after second merge")
            .row_count(),
        1,
        "second MERGE finds the existing node — no duplicate"
    );
}

#[tokio::test]
async fn transact_cypher_merge_on_create_set_fires_only_on_create() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-on-create");

    // Create Bob with role=admin via ON CREATE SET.
    let l = fluree
        .transact_cypher(
            l,
            r#"MERGE (n:Person {name: "Bob"}) ON CREATE SET n.role = "admin""#,
        )
        .await
        .expect("merge create")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {role: "admin"}) RETURN n"#)
            .await
            .expect("role admin")
            .row_count(),
        1,
        "ON CREATE SET applied on first create"
    );

    // Second MERGE with a different ON CREATE SET must NOT fire (Bob exists).
    let l = fluree
        .transact_cypher(
            l,
            r#"MERGE (n:Person {name: "Bob"}) ON CREATE SET n.role = "guest""#,
        )
        .await
        .expect("merge match")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {role: "guest"}) RETURN n"#)
            .await
            .expect("role guest")
            .row_count(),
        0,
        "ON CREATE SET must not fire when the node already exists"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {role: "admin"}) RETURN n"#)
            .await
            .expect("role still admin")
            .row_count(),
        1,
        "original role unchanged"
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
