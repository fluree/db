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
async fn cypher_var_length_bound_relationship_variable_rejected() {
    // Binding a variable to a variable-length relationship yields a list of
    // relationships, which needs list-valued bindings (deferred).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:varlen-bound");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree
        .query_cypher(&db, "MATCH (a:Person)-[r:KNOWS*1..3]->(b) RETURN b")
        .await;
    assert!(r.is_err(), "bound var-length relationship must be rejected");
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
    assert_eq!(
        pets.row_count(),
        1,
        "a new Pet node should have been created"
    );
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
    assert_eq!(
        rows.row_count(),
        1,
        "node should now carry the Employee label"
    );
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
async fn transact_cypher_bare_delete_removes_relationship_free_node() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:delete-clean");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");

    // Neither node has relationships → bare DELETE succeeds.
    let l = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) DELETE n"#,
        )
        .await
        .expect("bare delete")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1,
        "Alice removed, Bob remains"
    );
}

#[tokio::test]
async fn transact_cypher_bare_delete_errors_when_node_has_relationships() {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:delete-guarded");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    // Alice has an outbound relationship → bare DELETE must error.
    let err = fluree
        .transact_cypher(l.clone(), r#"MATCH (n:Person {name: "Alice"}) DELETE n"#)
        .await
        .expect_err("DELETE on a node with an outbound relationship should error");
    assert!(format!("{err}").contains("relationship"), "{err}");

    // Bob has an inbound relationship → bare DELETE must also error.
    let err = fluree
        .transact_cypher(l, r#"MATCH (n:Person {name: "Bob"}) DELETE n"#)
        .await
        .expect_err("DELETE on a node with an inbound relationship should error");
    assert!(format!("{err}").contains("relationship"), "{err}");
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
async fn transact_cypher_merge_on_match_set_fires_only_on_match() {
    // Conditional write: ON CREATE SET on first (absent) run, ON MATCH SET on
    // the second (present) run.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-on-match");

    let stmt = r#"MERGE (n:Person {name: "Alice"})
                  ON CREATE SET n.origin = "created"
                  ON MATCH  SET n.origin = "matched""#;

    // First run: node absent → create branch → origin = "created".
    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("merge create")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {origin: "created"}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "ON CREATE SET applied on first run"
    );

    // Second run: node present → on-match branch → origin = "matched".
    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("merge match")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1,
        "still exactly one node (no duplicate)"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {origin: "matched"}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "ON MATCH SET overwrote origin on the second run"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {origin: "created"}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        0,
        "old origin value was retracted"
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
async fn transact_cypher_delete_relationship_removes_edge() {
    // `DELETE r` retracts the relationship's base edge; the reifier cascade
    // clears the bundle. The endpoint nodes survive.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:delete-rel");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2000}]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS]->(b) RETURN r")
            .await
            .expect("pre")
            .row_count(),
        1,
        "edge present before delete"
    );

    let l = fluree
        .transact_cypher(l, "MATCH (a)-[r:KNOWS]->(b) DELETE r")
        .await
        .expect("delete rel")
        .ledger;
    let db = graphdb_from_ledger(&l);

    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS]->(b) RETURN r")
            .await
            .expect("post")
            .row_count(),
        0,
        "relationship removed"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("nodes")
            .row_count(),
        2,
        "both endpoint nodes survive"
    );
}

#[tokio::test]
async fn transact_cypher_delete_relationship_rejects_parallel_edges() {
    // Two KNOWS edges between the same pair share one base `(a,KNOWS,b)`
    // triple. Deleting one by retracting the base edge would disturb the
    // other, so `DELETE r` must reject when parallel siblings exist.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:delete-rel-parallel");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2000}]->(b)"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2010}]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS]->(b) RETURN r")
            .await
            .expect("pre")
            .row_count(),
        2,
        "two parallel KNOWS edges"
    );

    let err = fluree
        .transact_cypher(l, "MATCH (a)-[r:KNOWS]->(b) DELETE r")
        .await
        .expect_err("DELETE r on parallel relationships must error");
    assert!(format!("{err}").contains("parallel"), "{err}");
}

#[tokio::test]
async fn transact_cypher_delete_relationship_requires_named_endpoints() {
    // `DELETE r` needs both endpoints named so the parallel-edge probe can
    // group by them. An anonymous endpoint is rejected.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:delete-rel-anon");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let err = fluree
        .transact_cypher(l, "MATCH (a)-[r:KNOWS]->() DELETE r")
        .await
        .expect_err("DELETE r with an anonymous endpoint must error");
    assert!(format!("{err}").contains("endpoint"), "{err}");
}

#[tokio::test]
async fn transact_cypher_write_rejects_duplicate_relationship_variable() {
    // A relationship variable may bind only one edge per MATCH; reusing it
    // would make the parallel-edge probe (first occurrence) and the delete
    // lowering (last occurrence) disagree, so the write MATCH rejects it.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:dup-rel-var");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let err = fluree
        .transact_cypher(
            l,
            "MATCH (a)-[r:KNOWS]->(b), (c)-[r:LIKES]->(d) SET r.since = 2020",
        )
        .await
        .expect_err("reusing a relationship variable must be rejected");
    assert!(format!("{err}").contains("more than once"), "{err}");
}

#[tokio::test]
async fn transact_cypher_bare_delete_rejects_optional_only_target() {
    // A bare DELETE target bound only by OPTIONAL MATCH is rejected: the node
    // can be unbound on some rows, where the relationship probe would bind an
    // unrelated node and false-trigger the guard.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:delete-optional");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let err = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {name: "Alice"}) OPTIONAL MATCH (b:Person {name: "Bob"}) DELETE b"#,
        )
        .await
        .expect_err("bare DELETE of an OPTIONAL-only target must error");
    assert!(format!("{err}").contains("mandatory"), "{err}");
}

/// Seed a directed KNOWS chain Alice→Bob→Carol→Dave (plain edges).
async fn seed_knows_chain(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:KNOWS": {"@id": "ex:bob"}},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob",   "ex:KNOWS": {"@id": "ex:carol"}},
                    {"@id": "ex:carol", "@type": "ex:Person", "ex:name": "Carol", "ex:KNOWS": {"@id": "ex:dave"}},
                    {"@id": "ex:dave",  "@type": "ex:Person", "ex:name": "Dave"},
                ]
            }),
        )
        .await
        .expect("seed chain")
        .ledger
}

#[tokio::test]
async fn cypher_collect_gathers_values_into_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:collect");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
                     "ex:KNOWS": [{"@id": "ex:bob"}, {"@id": "ex:carol"}, {"@id": "ex:dave"}]},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
                    {"@id": "ex:carol", "@type": "ex:Person", "ex:name": "Carol"},
                    {"@id": "ex:dave",  "@type": "ex:Person", "ex:name": "Dave"},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let result = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f) RETURN collect(f.name) AS friends"#,
        )
        .await
        .expect("collect");
    // collect groups all of Alice's friends into a single row.
    assert_eq!(result.row_count(), 1, "one grouped row");

    let jsonld = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    // Shape: rows[0][col0] is the collected list.
    let list = jsonld[0][0]
        .as_array()
        .unwrap_or_else(|| panic!("expected a list column, got {jsonld}"));
    let mut names: Vec<&str> = list.iter().filter_map(|v| v.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["Bob", "Carol", "Dave"], "collected friend names");
}

#[tokio::test]
async fn cypher_collect_empty_input_returns_empty_list() {
    // Cypher: an implicit aggregation over zero matched rows still yields one
    // row; collect() of nothing is the empty list `[]`.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:collect-empty").await;
    let db = graphdb_from_ledger(&l);

    for q in [
        r#"MATCH (n:Nonexistent) RETURN collect(n) AS xs"#,
        r#"MATCH (n:Nonexistent) RETURN collect(DISTINCT n) AS xs"#,
    ] {
        let jsonld = fluree
            .query_cypher(&db, q)
            .await
            .expect("collect empty")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        assert_eq!(
            jsonld[0][0].as_array().map(Vec::len),
            Some(0),
            "empty collect is one row with []: {jsonld} ({q})"
        );
    }
}

#[tokio::test]
async fn cypher_order_by_collect_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:order-collect").await;
    let db = graphdb_from_ledger(&l);

    for q in [
        r#"MATCH (a:Person)-[:KNOWS]->(b) RETURN a, collect(b) AS bs ORDER BY bs"#,
        r#"MATCH (a:Person)-[:KNOWS]->(b) RETURN a, collect(b) ORDER BY collect(b)"#,
    ] {
        let err = fluree
            .query_cypher(&db, q)
            .await
            .expect_err("ORDER BY on a collect list must be rejected");
        assert!(format!("{err}").contains("ORDER BY"), "{err}");
    }
}

#[tokio::test]
async fn cypher_with_collect_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:with-collect").await;
    let db = graphdb_from_ledger(&l);

    let err = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b) AS bs RETURN a, bs"#,
        )
        .await
        .expect_err("collect() in WITH must be rejected");
    assert!(format!("{err}").contains("collect() in WITH"), "{err}");
}

#[tokio::test]
async fn cypher_collect_distinct_dedupes() {
    // Two friends share the name "Bob"; collect(DISTINCT) keeps one.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:collect-distinct");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
                     "ex:KNOWS": [{"@id": "ex:bob"}, {"@id": "ex:bob2"}]},
                    {"@id": "ex:bob",  "@type": "ex:Person", "ex:name": "Bob"},
                    {"@id": "ex:bob2", "@type": "ex:Person", "ex:name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let plain = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f) RETURN collect(f.name) AS names"#,
        )
        .await
        .expect("collect")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        plain[0][0].as_array().map(Vec::len),
        Some(2),
        "plain keeps duplicates: {plain}"
    );

    let distinct = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f) RETURN collect(DISTINCT f.name) AS names"#,
        )
        .await
        .expect("collect distinct")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        distinct[0][0].as_array().map(Vec::len),
        Some(1),
        "DISTINCT dedupes: {distinct}"
    );
}

#[tokio::test]
async fn cypher_undirected_relationship_matches_both_orientations() {
    // `-[:KNOWS]-` from Bob finds Alice (reverse: Alice KNOWS Bob, via Opst)
    // and Carol (forward: Bob KNOWS Carol).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:undirected").await;
    let db = graphdb_from_ledger(&l);

    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Bob"})-[:KNOWS]-(x) RETURN x"#,
        )
        .await
        .expect("undirected match");
    assert_eq!(
        rows.row_count(),
        2,
        "Bob's undirected neighbors: Alice, Carol"
    );
}

#[tokio::test]
async fn cypher_var_length_bounded_directed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:varlen-bounded").await;
    let db = graphdb_from_ledger(&l);

    // *1..2 from Alice → Bob (1 hop), Carol (2 hops).
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS*1..2]->(x) RETURN x"#,
        )
        .await
        .expect("*1..2");
    assert_eq!(rows.row_count(), 2, "Alice within 1..2 hops: Bob, Carol");

    // *1..3 from Alice → Bob, Carol, Dave.
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(x) RETURN x"#,
        )
        .await
        .expect("*1..3");
    assert_eq!(
        rows.row_count(),
        3,
        "Alice within 1..3 hops: Bob, Carol, Dave"
    );
}

#[tokio::test]
async fn cypher_var_length_unregistered_namespace_returns_no_rows() {
    // When the relationship type's *namespace* isn't registered in the ledger
    // (here: an empty genesis ledger), the predicate can't be encoded. An
    // unbounded path must then yield zero rows, not a query error — matching
    // how the bounded (string-IRI) path and absent labels behave.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/cypher:varlen-unregistered");
    let db = graphdb_from_ledger(&ledger);

    for path in ["*", "*0..", "*1..3", "*2"] {
        let rows = fluree
            .query_cypher(
                &db,
                &format!(r#"MATCH (a:Person)-[:KNOWS{path}]->(x) RETURN x"#),
            )
            .await
            .unwrap_or_else(|e| panic!("unregistered type `{path}` should not error: {e}"));
        assert_eq!(rows.row_count(), 0, "unregistered type with `{path}`");
    }
}

#[tokio::test]
async fn cypher_var_length_exact_hops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:varlen-exact").await;
    let db = graphdb_from_ledger(&l);

    // *2 from Alice → exactly Carol.
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS*2]->(x) RETURN x"#,
        )
        .await
        .expect("*2");
    assert_eq!(rows.row_count(), 1, "Alice at exactly 2 hops: Carol");
}

#[tokio::test]
async fn cypher_var_length_unbounded_transitive() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:varlen-unbounded").await;
    let db = graphdb_from_ledger(&l);

    // `*` = one-or-more (PropertyPath OneOrMore) from Alice → Bob, Carol, Dave.
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS*]->(x) RETURN x"#,
        )
        .await
        .expect("*");
    assert_eq!(
        rows.row_count(),
        3,
        "Alice transitive reach: Bob, Carol, Dave"
    );

    // `*0..` = zero-or-more (includes Alice herself).
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS*0..]->(x) RETURN x"#,
        )
        .await
        .expect("*0..");
    assert_eq!(
        rows.row_count(),
        4,
        "zero-or-more includes Alice: +Bob, Carol, Dave"
    );
}
