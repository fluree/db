// Cypher query strings are written as raw strings (`r#"..."#`) for consistency
// even when a given query has no inner quotes.
#![allow(clippy::needless_raw_string_hashes)]

//! Cypher read-path end-to-end tests.
//!
//! Each test inserts data via JSON-LD `@annotation` (the canonical
//! producer of `f:reifies*` bundles) and queries it back via Cypher,
//! verifying the same IR underlies both surfaces.
//!
//! See `docs/concepts/cypher.md` for the supported surface.

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
async fn cypher_var_length_unbounded_bound_relationship_variable_rejected() {
    // Binding a variable to an UNBOUNDED variable-length relationship needs path
    // enumeration the transitive operator doesn't provide (deferred). The bounded
    // form is supported — see `cypher_var_length_rel_and_path_binding`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:varlen-bound");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree
        .query_cypher(&db, "MATCH (a:Person)-[r:KNOWS*]->(b) RETURN b")
        .await;
    assert!(
        r.is_err(),
        "unbounded bound var-length relationship must be rejected"
    );
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
async fn transact_cypher_set_map_replace_preserves_labels_and_relationships() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:set-map-replace");

    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:age": 25,
                        "ex:KNOWS": {"@id": "ex:bob"}
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": "Bob",
                        "ex:age": 35
                    }
                ]
            }),
        )
        .await
        .expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person {name: "Alice"}) SET n = {name: "Alicia", city: "Paris"}"#,
        )
        .await
        .expect("set map replace");

    let db = graphdb_from_ledger(&updated.ledger);
    let replaced = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name: "Alicia", city: "Paris"}) RETURN n"#,
        )
        .await
        .expect("query replacement props");
    assert_eq!(replaced.row_count(), 1, "replacement properties inserted");

    let old_props = fluree
        .query_cypher(&db, r#"MATCH (n:Person {name: "Alice"}) RETURN n"#)
        .await
        .expect("query old name");
    assert_eq!(old_props.row_count(), 0, "old scalar properties removed");

    let old_age = fluree
        .query_cypher(&db, "MATCH (n:Person {age: 25}) RETURN n")
        .await
        .expect("query old age");
    assert_eq!(old_age.row_count(), 0, "omitted scalar properties removed");

    let relationship = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alicia"})-[:KNOWS]->(b:Person {name: "Bob"}) RETURN a, b"#,
        )
        .await
        .expect("query relationship");
    assert_eq!(relationship.row_count(), 1, "relationships are preserved");
}

#[tokio::test]
async fn transact_cypher_match_where_set_filters_target_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:match-where-set");

    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 25},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob",   "ex:age": 35},
                ]
            }),
        )
        .await
        .expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person) WHERE n.age > 30 SET n.status = "senior""#,
        )
        .await
        .expect("match where set");

    let db = graphdb_from_ledger(&updated.ledger);
    let rows = fluree
        .query_cypher(&db, r#"MATCH (n:Person {status: "senior"}) RETURN n"#)
        .await
        .expect("query status");
    assert_eq!(rows.row_count(), 1, "only Bob should be updated");
}

#[tokio::test]
async fn transact_cypher_match_where_is_null_set() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:match-where-null");

    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 25},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");

    let updated = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (n:Person) WHERE n.age IS NULL SET n.status = "missing-age""#,
        )
        .await
        .expect("match where is null set");

    let db = graphdb_from_ledger(&updated.ledger);
    let rows = fluree
        .query_cypher(&db, r#"MATCH (n:Person {status: "missing-age"}) RETURN n"#)
        .await
        .expect("query status");
    assert_eq!(rows.row_count(), 1, "only Bob lacks age");
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
async fn transact_cypher_match_where_create_links_existing_nodes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:match-where-create");

    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob"},
                    {"@id": "ex:eve",   "@type": "ex:Person", "ex:name": "Eve"},
                ]
            }),
        )
        .await
        .expect("seed");

    let linked = fluree
        .transact_cypher(
            committed.ledger,
            r#"MATCH (a:Person), (b:Person)
               WHERE a.name = "Alice" AND b.name STARTS WITH "B"
               CREATE (a)-[:KNOWS]->(b)"#,
        )
        .await
        .expect("match where create");

    let db = graphdb_from_ledger(&linked.ledger);
    let rows = fluree
        .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
        .await
        .expect("query edge");
    assert_eq!(rows.row_count(), 1, "only Alice KNOWS Bob should exist");
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
async fn transact_cypher_unwind_map_param_batches_node_inserts() {
    // The idiomatic driver batched insert: one parameter carrying N rows,
    // UNWIND, CREATE one node per row, commit once.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-map");

    let params = json!({
        "batch": [
            {"name": "Alice", "age": 30},
            {"name": "Bob",   "age": 41},
            {"name": "Carol", "age": 25},
        ]
    });
    let result = fluree
        .transact_cypher_with_params(
            ledger0,
            "UNWIND $batch AS row CREATE (n:Person {name: row.name, age: row.age})",
            params.as_object(),
        )
        .await
        .expect("unwind-map batched insert");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("count")
            .row_count(),
        3,
        "three distinct nodes created"
    );
    // Each row's properties land on its own node.
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) WHERE n.age > 28 RETURN n")
            .await
            .expect("filter")
            .row_count(),
        2,
        "Alice(30) + Bob(41); Carol(25) excluded"
    );
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (n:Person {name: "Bob"}) WHERE n.age = 41 RETURN n"#
            )
            .await
            .expect("bob")
            .row_count(),
        1,
        "Bob's name and age stayed on the same node"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_scalar_list_param_batches_inserts() {
    // Scalar-list UNWIND CREATE referencing the bare alias.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-scalar");

    let params = json!({ "ids": [1, 2, 3, 4] });
    let result = fluree
        .transact_cypher_with_params(
            ledger0,
            "UNWIND $ids AS id CREATE (n:Thing {ref: id})",
            params.as_object(),
        )
        .await
        .expect("unwind-scalar batched insert");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Thing) RETURN n")
            .await
            .expect("count")
            .row_count(),
        4,
        "four distinct nodes"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_empty_batch_errors_empty_transaction() {
    // An empty `$batch` unrolls to zero writes. Cypher would treat this as a
    // no-op success; today it surfaces the engine's EmptyTransaction guard.
    // Pinned here as a known limitation (graceful no-op is a follow-up).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-empty");

    let err = fluree
        .transact_cypher_with_params(
            ledger0,
            "UNWIND $batch AS row CREATE (n:Person {name: row.name})",
            json!({ "batch": [] }).as_object(),
        )
        .await
        .expect_err("empty batch currently errors (EmptyTransaction)");
    assert!(format!("{err:?}").contains("EmptyTransaction"), "{err:?}");
}

/// Seed three Person nodes carrying `ex:id` 1/2/3 for edge-batch tests.
async fn seed_nodes_with_ids(
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
                    {"@id": "ex:n1", "@type": "ex:Person", "ex:id": 1, "ex:name": "Alice"},
                    {"@id": "ex:n2", "@type": "ex:Person", "ex:id": 2, "ex:name": "Bob"},
                    {"@id": "ex:n3", "@type": "ex:Person", "ex:id": 3, "ex:name": "Carol"},
                ]
            }),
        )
        .await
        .expect("seed nodes")
        .ledger
}

#[tokio::test]
async fn transact_cypher_unwind_map_param_batches_edge_inserts() {
    // The edge-loading idiom: one parameter of {from,to} maps, matched against
    // existing nodes by id, one edge per row, committed once. Desugars to a
    // VALUES join.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:unwind-edges").await;

    let params = json!({ "pairs": [{"from": 1, "to": 2}, {"from": 2, "to": 3}] });
    let result = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $pairs AS p MATCH (a:Person {id: p.from}), (b:Person {id: p.to}) \
             CREATE (a)-[:KNOWS]->(b)",
            params.as_object(),
        )
        .await
        .expect("edge batch insert");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            .await
            .expect("edges")
            .row_count(),
        2,
        "two KNOWS edges created (1->2, 2->3)"
    );
    // The edges connect the right nodes.
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}) RETURN a"#,
            )
            .await
            .expect("alice->bob")
            .row_count(),
        1,
        "Alice(id 1) -> Bob(id 2)"
    );
}

#[tokio::test]
async fn transact_cypher_optional_match_before_create_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = fluree
        .transact_cypher(
            genesis_ledger(&fluree, "it/cypher:optional-create"),
            r#"CREATE (a:Person {name: "Alice"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let err = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {name: "Alice"}) OPTIONAL MATCH (b:Person {name: "Ghost"}) CREATE (a)-[:KNOWS]->(b)"#,
        )
        .await
        .expect_err("OPTIONAL MATCH before CREATE must be rejected");
    assert!(format!("{err}").contains("OPTIONAL MATCH"), "{err}");
}

#[tokio::test]
async fn transact_cypher_anonymous_create_reifies_for_named_read() {
    // Every Cypher relationship reifies (LPG identity), so an anonymous CREATE
    // is visible to a *named* read and carries identity.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:anon-create-reified");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS]->(b) RETURN r")
            .await
            .expect("named read")
            .row_count(),
        1,
        "anonymous CREATE reifies → named read sees it"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            .await
            .expect("anon read")
            .row_count(),
        1,
        "and the base triple is visible to anonymous reads"
    );
}

#[tokio::test]
async fn cypher_collect_inside_expression_rejected() {
    // collect() is list-valued: it can be a bare RETURN item or the argument of
    // a list function (`size(collect(x))`), but not nested in arithmetic /
    // comparison where it would silently evaluate to null.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:collect-in-expr").await;
    let db = graphdb_from_ledger(&l);

    for q in [
        "MATCH (n:Person) RETURN collect(n) + 1",
        "MATCH (n:Person) RETURN count(n) + collect(n)",
    ] {
        let err = fluree
            .query_cypher(&db, q)
            .await
            .expect_err("collect inside expression must be rejected");
        assert!(format!("{err}").contains("collect()"), "{err}: {q}");
    }
    // Bare collect still works.
    fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN collect(n) AS xs")
        .await
        .expect("bare collect still works");
}

#[tokio::test]
async fn cypher_collect_through_with() {
    // `collect()` projected by a WITH must flow out as a real list to the next
    // stage (it was previously deferred). Alice KNOWS Bob & Carol; Bob KNOWS Carol.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:collect-with").await;
    let db = graphdb_from_ledger(&l);

    // Raw list carried through the WITH boundary.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)-[:KNOWS]->(f:Person)
               WITH p, collect(f.name) AS friends
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("collect through WITH")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "Alice and Bob have outgoing KNOWS: {cj}");
    assert_eq!(data[0]["row"][0], json!("Alice"), "{cj}");
    let mut alice: Vec<String> = serde_json::from_value(data[0]["row"][1].clone()).expect("list");
    alice.sort();
    assert_eq!(
        alice,
        vec!["Bob", "Carol"],
        "Alice's collected friends: {cj}"
    );
    assert_eq!(data[1]["row"], json!(["Bob", ["Carol"]]), "{cj}");

    // The carried list feeds a downstream list function.
    let sized = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)-[:KNOWS]->(f:Person)
               WITH p, collect(f.name) AS friends
               RETURN p.name AS name, size(friends) AS n ORDER BY name"#,
        )
        .await
        .expect("size over WITH-collected list")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = sized["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data[0]["row"], json!(["Alice", 2]), "{sized}");
    assert_eq!(data[1]["row"], json!(["Bob", 1]), "{sized}");

    // The carried list feeds a downstream UNWIND (collect → unwind round-trip).
    let unwound = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})-[:KNOWS]->(f:Person)
               WITH p, collect(f.name) AS friends
               UNWIND friends AS fr
               RETURN fr ORDER BY fr"#,
        )
        .await
        .expect("unwind WITH-collected list")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let rows: Vec<_> = unwound["results"][0]["data"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["row"][0].clone())
        .collect();
    assert_eq!(rows, vec![json!("Bob"), json!("Carol")], "{unwound}");

    // ORDER BY directly on a collect() list in the same WITH is still rejected
    // (sorting a list value is unsound in v1).
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person)-[:KNOWS]->(f:Person)
                   WITH p, collect(f.name) AS friends ORDER BY friends
                   RETURN p.name, friends"#,
            )
            .await
            .is_err(),
        "ORDER BY on a collect() list in WITH is rejected"
    );
}

#[tokio::test]
async fn cypher_aggregate_composed_into_expression() {
    // Aggregates nested in a larger expression (IC3 total, IC10 score, IC14):
    // `count(*) * 2`, `count(n) + 1`, `count(*) + count(*)`.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:agg-expr").await; // 3 Person nodes
    let db = graphdb_from_ledger(&l);

    let doubled = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN count(*) * 2 AS doubled")
        .await
        .expect("count(*) * 2")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(doubled[0][0], json!(6), "3 persons * 2 = 6: {doubled}");

    let twice = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN count(*) + count(*) AS twice")
        .await
        .expect("count(*) + count(*)")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(twice[0][0], json!(6), "3 + 3 = 6: {twice}");

    let per_group = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) RETURN n.id AS id, count(n) + 1 AS c ORDER BY id",
        )
        .await
        .expect("count(n) + 1")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = per_group.as_array().expect("rows");
    assert_eq!(rows.len(), 3, "one row per id: {per_group}");
    for row in rows {
        assert_eq!(row[1], json!(2), "count(n) + 1 = 2 per group: {per_group}");
    }
}

#[tokio::test]
async fn cypher_aggregate_expression_argument() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:agg-expression-arg").await;
    let db = graphdb_from_ledger(&l);

    let result = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN sum(n.id * 2) AS total")
        .await
        .expect("sum expression arg")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(result[0][0], json!(12), "(1 + 2 + 3) * 2 = 12: {result}");
}

#[tokio::test]
async fn cypher_xor_expression_filters_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:xor").await;
    let db = graphdb_from_ledger(&l);

    let rows = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) WHERE n.id = 1 XOR n.id = 2 RETURN n ORDER BY n.id",
        )
        .await
        .expect("xor query");
    assert_eq!(rows.row_count(), 2, "ids 1 and 2 satisfy exactly one side");
}

#[tokio::test]
async fn cypher_modulus_expression_filters_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:modulus").await;
    let db = graphdb_from_ledger(&l);

    let rows = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) WHERE n.id % 2 = 1 RETURN n ORDER BY n.id",
        )
        .await
        .expect("modulus query");
    assert_eq!(rows.row_count(), 2, "ids 1 and 3 are odd");
}

#[tokio::test]
async fn cypher_with_star_carries_visible_vars_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:with-star").await;
    let db = graphdb_from_ledger(&l);

    let result = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) WHERE n.id > 1 WITH * RETURN * ORDER BY n.id",
        )
        .await
        .expect("WITH * query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = result.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "ids 2 and 3 survive the WITH boundary");
    for row in rows {
        let row = row.as_object().expect("wildcard row object");
        assert_eq!(
            row.len(),
            1,
            "WITH * should not expose synthetic property-accessor vars: {result}"
        );
        assert!(row.contains_key("n"), "WITH * should keep user variable n");
    }
}

#[tokio::test]
async fn cypher_labels_returns_rdf_type_strings() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:labels-fn");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                    {
                        "@id": "ex:bob",
                        "@type": ["ex:Person", "ex:Employee"],
                        "ex:name": "Bob"
                    },
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let jsonld = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) RETURN n.name AS name, labels(n) AS ls ORDER BY name",
        )
        .await
        .expect("labels query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");

    assert_eq!(jsonld.as_array().expect("rows").len(), 2, "Alice and Bob");
    let alice_labels: Vec<&str> = jsonld[0][1]
        .as_array()
        .expect("labels list")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert_eq!(alice_labels, ["Person"]);

    let mut bob_labels: Vec<&str> = jsonld[1][1]
        .as_array()
        .expect("labels list")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    bob_labels.sort_unstable();
    assert_eq!(bob_labels, ["Employee", "Person"]);
}

#[tokio::test]
async fn cypher_type_returns_named_relationship_type() {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:type-fn");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }
    let db = graphdb_from_ledger(&l);

    let jsonld = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person) RETURN type(r) AS t"#,
        )
        .await
        .expect("type query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");

    assert_eq!(jsonld.as_array().expect("rows").len(), 1);
    assert_eq!(jsonld[0][0].as_str(), Some("KNOWS"));
}

#[tokio::test]
async fn cypher_relationship_value_semantics() {
    // A bound relationship variable `r` (the reified edge) supports the full
    // relationship-value surface: type(r), startNode(r)/endNode(r), r.prop, and
    // properties(r).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:rel-value");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:RATED {stars: 5}]->(m:Movie {title: "Inception"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person)-[r:RATED]->(m:Movie)
               RETURN type(r) AS t, r.stars AS stars, properties(r) AS props,
                      startNode(r) AS sn, endNode(r) AS en, a AS aa, m AS mm"#,
        )
        .await
        .expect("relationship value query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!("RATED"), "type(r): {cj}");
    assert_eq!(row[1], json!(5), "r.stars: {cj}");
    assert_eq!(row[2], json!({"stars": 5}), "properties(r): {cj}");
    assert_eq!(row[3], row[5], "startNode(r) == a: {cj}");
    assert_eq!(row[4], row[6], "endNode(r) == m: {cj}");
}

#[tokio::test]
async fn cypher_order_by_property_accessor_grouping_key() {
    // ORDER BY a grouping key written as a property accessor (`f.id`, not its
    // alias) must work under aggregation — it should behave like ORDER BY the
    // alias, not mint a fresh post-grouping sort var.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:order-prop-group").await;
    let db = graphdb_from_ledger(&l);

    let via_accessor = fluree
        .query_cypher(
            &db,
            "MATCH (f:Person) RETURN f.id AS friendId, count(f) AS c ORDER BY f.id",
        )
        .await
        .expect("ORDER BY property accessor under aggregation");
    let via_alias = fluree
        .query_cypher(
            &db,
            "MATCH (f:Person) RETURN f.id AS friendId, count(f) AS c ORDER BY friendId",
        )
        .await
        .expect("ORDER BY alias");
    assert_eq!(via_accessor.row_count(), 3, "one row per distinct id");
    assert_eq!(
        via_accessor.row_count(),
        via_alias.row_count(),
        "accessor and alias forms agree"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_edge_with_property_batches() {
    // Edge batch carrying a per-row edge property: `p.d` is a VALUES-bound
    // column used in the relationship property map. The edge reifies, and each
    // row's reifier is a distinct (per-solution) blank node — so the two edges
    // get distinct `since` values without colliding.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:unwind-edge-props").await;

    let params = json!({
        "pairs": [
            {"from": 1, "to": 2, "d": 2020},
            {"from": 2, "to": 3, "d": 2021},
        ]
    });
    let result = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $pairs AS p MATCH (a:Person {id: p.from}), (b:Person {id: p.to}) \
             CREATE (a)-[:KNOWS {since: p.d}]->(b)",
            params.as_object(),
        )
        .await
        .expect("edge-with-property batch");

    let db = graphdb_from_ledger(&result.ledger);
    // Two reified edges (named read sees reified edges).
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS]->(b) RETURN r")
            .await
            .expect("edges")
            .row_count(),
        2,
        "two reified KNOWS edges"
    );
    // Each edge carries its own `since` — proving distinct per-row reifiers.
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN r")
            .await
            .expect("2020")
            .row_count(),
        1,
        "the 1->2 edge carries since=2020"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[r:KNOWS {since: 2021}]->(b) RETURN r")
            .await
            .expect("2021")
            .row_count(),
        1,
        "the 2->3 edge carries since=2021"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_edge_missing_id_drops_only_that_row() {
    // A row whose endpoint id matches nothing drops only itself — the rest of
    // the batch still commits (the value of the VALUES-join model over a
    // cross-product unroll).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:unwind-edges-missing").await;

    let params = json!({ "pairs": [{"from": 1, "to": 2}, {"from": 1, "to": 99}] });
    let result = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $pairs AS p MATCH (a:Person {id: p.from}), (b:Person {id: p.to}) \
             CREATE (a)-[:KNOWS]->(b)",
            params.as_object(),
        )
        .await
        .expect("partial edge batch");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            .await
            .expect("edges")
            .row_count(),
        1,
        "only the 1->2 edge; the 1->99 row found no target and dropped"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_optional_match_create_rejected() {
    // OPTIONAL MATCH endpoints could be unbound → a partial reifier bundle.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:unwind-optional").await;
    let err = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $pairs AS p OPTIONAL MATCH (a:Person {id: p.from}), (b:Person {id: p.to}) \
             CREATE (a)-[:KNOWS]->(b)",
            json!({ "pairs": [{"from": 1, "to": 2}] }).as_object(),
        )
        .await
        .expect_err("OPTIONAL MATCH in an UNWIND CREATE batch must be rejected");
    assert!(format!("{err}").contains("OPTIONAL MATCH"), "{err}");
}

#[tokio::test]
async fn transact_cypher_unwind_whole_row_value_rejected() {
    // Using the whole map element as a value (not a field) is deferred.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-whole");

    let params = json!({ "batch": [{"name": "Alice"}] });
    let err = fluree
        .transact_cypher_with_params(
            ledger0,
            "UNWIND $batch AS row CREATE (n:Person {data: row})",
            params.as_object(),
        )
        .await
        .expect_err("whole-map value must be rejected");
    assert!(format!("{err}").contains("whole UNWIND element"), "{err}");
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
        // Every Cypher relationship reifies (LPG identity), so the bare-DELETE
        // guard (which probes reified relationships) sees it.
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

/// A mixed-type relationship chain Alice -KNOWS-> Bob -FOLLOWS-> Carol -KNOWS->
/// Dave, every node a `:Person` with a `name` data property. Returns the ledger.
async fn untyped_path_chain(fluree: &support::MemoryFluree, name: &str) -> support::MemoryLedger {
    let l = genesis_ledger(fluree, name);
    fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
                      (b)-[:FOLLOWS]->(c:Person {name: "Carol"}),
                      (c)-[:KNOWS]->(d:Person {name: "Dave"})"#,
        )
        .await
        .expect("build chain")
        .ledger
}

async fn cypher_names(
    fluree: &support::MemoryFluree,
    l: &support::MemoryLedger,
    q: &str,
) -> JsonValue {
    let db = graphdb_from_ledger(l);
    fluree
        .query_cypher(&db, q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld")
}

#[tokio::test]
async fn cypher_untyped_path_bounded_follows_mixed_edge_types() {
    // `-[*1..2]->` from Alice follows KNOWS then FOLLOWS (mixed types), reaching
    // Bob (1 hop) and Carol (2 hops) — NOT Dave (3 hops, over the cap). Data
    // properties (`name`), `rdf:type` (the `:Person` class), and the reifier
    // sidecar are not edges, so they are never traversed.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = untyped_path_chain(&fluree, "it/cypher:untyped-bounded").await;
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "Alice"})-[*1..2]->(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(rows, json!([["Bob"], ["Carol"]]), "1..2 hops: {rows}");
}

#[tokio::test]
async fn cypher_untyped_path_unbounded_reaches_whole_chain() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = untyped_path_chain(&fluree, "it/cypher:untyped-unbounded").await;
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "Alice"})-[*]->(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(
        rows,
        json!([["Bob"], ["Carol"], ["Dave"]]),
        "unbounded reaches the whole chain: {rows}"
    );
}

#[tokio::test]
async fn cypher_untyped_path_diamond_lower_bound_is_consistent() {
    // Diamond: Alice -KNOWS-> Bob, and Alice -KNOWS-> Carol -KNOWS-> Bob.
    // `*2..2` from Alice must include Bob via the length-2 path Alice->Carol->Bob
    // even though Bob is ALSO reachable in 1 hop — the layered (node,depth) BFS
    // doesn't suppress the longer in-range path. Bound-unbound (RETURN x) and
    // bound-bound (RETURN exists) must agree.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:untyped-diamond");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
                      (a)-[:KNOWS]->(c:Person {name: "Carol"}),
                      (c)-[:KNOWS]->(b)"#,
        )
        .await
        .expect("seed")
        .ledger;

    // Bound-unbound: who is exactly 2 hops from Alice? Bob (via Carol).
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "Alice"})-[*2..2]->(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(
        rows,
        json!([["Bob"]]),
        "*2..2 reaches Bob via the length-2 path despite the 1-hop edge: {rows}"
    );

    // Bound-bound: the same query with Bob bound must also see the path.
    let db = graphdb_from_ledger(&l);
    let exists = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[*2..2]->(b:Person {name: "Bob"}) RETURN b"#,
        )
        .await
        .expect("bound-bound")
        .row_count();
    assert_eq!(exists, 1, "bound-bound agrees with bound-unbound");
}

#[tokio::test]
async fn cypher_untyped_path_unbounded_lower_bound_above_one_is_rejected() {
    // `-[*2..]->` (unbounded, lower bound > 1) can't be evaluated soundly — it
    // must be rejected with a clear error.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:untyped-unbounded-lo");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let err = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[*2..]->(x) RETURN x"#,
        )
        .await;
    assert!(err.is_err(), "unbounded *2.. should be rejected");
}

#[tokio::test]
async fn cypher_untyped_path_revisit_intermediate_bound_bound() {
    // A->B, A->C, C->B, B->D. `*3..3` from A reaches D only via A-C-B-D — which
    // requires revisiting B at depth 2. The bound-bound form (path_exists) must
    // agree with the bound-unbound form: both find D.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:untyped-revisit");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "A"})-[:R]->(b:Person {name: "B"}),
                      (a)-[:R]->(c:Person {name: "C"}),
                      (c)-[:R]->(b),
                      (b)-[:R]->(d:Person {name: "D"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Bound-unbound: who is exactly 3 hops from A? D (via A-C-B-D).
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "A"})-[*3..3]->(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(
        rows,
        json!([["D"]]),
        "*3..3 reaches D via the revisited B: {rows}"
    );

    // Bound-bound: the same with D bound must also see the path.
    let exists = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "A"})-[*3..3]->(d:Person {name: "D"}) RETURN d"#,
        )
        .await
        .expect("bound-bound")
        .row_count();
    assert_eq!(
        exists, 1,
        "bound-bound agrees with bound-unbound on the revisit path"
    );
}

#[tokio::test]
async fn cypher_untyped_path_lower_bound_excludes_near_nodes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = untyped_path_chain(&fluree, "it/cypher:untyped-lo").await;
    // `*2..3` from Alice: Carol (2) and Dave (3), but NOT Bob (1 hop).
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "Alice"})-[*2..3]->(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(rows, json!([["Carol"], ["Dave"]]), "2..3 hops: {rows}");
}

#[tokio::test]
async fn cypher_untyped_path_single_hop_excludes_rdf_type_class() {
    // Exactly one hop from Alice is just her relationship target (Bob). If the
    // wildcard scan followed `rdf:type` (a Ref to the `Person` class) it would
    // also surface the class node — proving the reserved-predicate exclusion.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = untyped_path_chain(&fluree, "it/cypher:untyped-1hop").await;
    let db = graphdb_from_ledger(&l);
    let count = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[*1..1]->(x) RETURN x"#,
        )
        .await
        .expect("query")
        .row_count();
    assert_eq!(count, 1, "exactly one 1-hop target (Bob), not the class");
}

#[tokio::test]
async fn cypher_untyped_path_incoming_direction() {
    // `<-[*1..2]-` into Dave: Carol (1 back) and Bob (2 back), not Alice (3).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = untyped_path_chain(&fluree, "it/cypher:untyped-incoming").await;
    let rows = cypher_names(
        &fluree,
        &l,
        r#"MATCH (a:Person {name: "Dave"})<-[*1..2]-(x) RETURN x.name AS n ORDER BY n"#,
    )
    .await;
    assert_eq!(rows, json!([["Bob"], ["Carol"]]), "incoming 1..2: {rows}");
}

#[tokio::test]
async fn cypher_map_literal_projection_renders_native_object() {
    // `RETURN {…}` builds a map value; cypher-json renders it as a native JSON
    // object with bare scalars (not RDF value-objects).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:map-literal");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice", age: 30})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               RETURN {name: p.name, age: p.age} AS person"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!({"name": "Alice", "age": 30}),
        "map literal → native object: {cj}"
    );
}

/// Run a Cypher read against a single seeded Person and return the first row's
/// columns as cypher-json native values.
async fn cypher_row(
    fluree: &support::MemoryFluree,
    l: &support::MemoryLedger,
    q: &str,
) -> JsonValue {
    let db = graphdb_from_ledger(l);
    fluree
        .query_cypher(&db, q)
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json")["results"][0]["data"][0]["row"]
        .clone()
}

#[tokio::test]
async fn cypher_pattern_comprehension() {
    // `[(a)-[:KNOWS]->(b) | b.name]` — a correlated subquery collecting a
    // projection per match, returned as a list per outer row.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:pattern-comp");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob", age: 40}),
                      (a)-[:KNOWS]->(c:Person {name: "Carol", age: 20})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // All of Alice's friends' names.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN [(a)-[:KNOWS]->(b:Person) | b.name] AS friends"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let mut friends: Vec<String> =
        serde_json::from_value(cj["results"][0]["data"][0]["row"][0].clone()).expect("list");
    friends.sort();
    assert_eq!(
        friends,
        vec!["Bob".to_string(), "Carol".to_string()],
        "{cj}"
    );

    // With an inner WHERE filter — only friends over 30.
    let filtered = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN [(a)-[:KNOWS]->(b:Person) WHERE b.age > 30 | b.name] AS older"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        filtered["results"][0]["data"][0]["row"][0],
        json!(["Bob"]),
        "inner WHERE filters the comprehension: {filtered}"
    );

    // Nested in another function: size of the comprehension.
    let count = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN size([(a)-[:KNOWS]->(b:Person) | b.name]) AS friendCount"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        count["results"][0]["data"][0]["row"][0],
        json!(2),
        "pattern comprehension nested in size(): {count}"
    );
}

#[tokio::test]
async fn cypher_pattern_comprehension_outer_var_and_nested_async() {
    // A pattern-comprehension projection can capture an OUTER variable that
    // never appears in the inner pattern, and can itself contain an async
    // subquery (EXISTS / a nested pattern comprehension). A chain
    // Alice->Bob->Carol plus a disconnected Zed.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:pattern-comp-outer");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
                      (b)-[:KNOWS]->(c:Person {name: "Carol"}),
                      (z:Person {name: "Zed"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Finding 1: projection references outer `z`, absent from the inner pattern.
    let outer = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}), (z:Person {name: "Zed"})
               RETURN [(a)-[:KNOWS]->(b:Person) | z.name] AS r"#,
        )
        .await
        .expect("outer-var projection")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        outer["results"][0]["data"][0]["row"][0],
        json!(["Zed"]),
        "outer var in projection survives dependency trimming: {outer}"
    );

    // Finding 2a: a nested EXISTS in the projection. Alice KNOWS Bob; Bob KNOWS
    // Carol, so the EXISTS holds for Bob.
    let nested_exists = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN [(a)-[:KNOWS]->(b:Person) | EXISTS { (b)-[:KNOWS]->(x:Person) }] AS r"#,
        )
        .await
        .expect("nested exists projection")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        nested_exists["results"][0]["data"][0]["row"][0],
        json!([true]),
        "nested EXISTS in projection is resolved per inner match: {nested_exists}"
    );

    // Finding 2b: a nested pattern comprehension in the projection.
    let nested_pc = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN [(a)-[:KNOWS]->(b:Person) | [(b)-[:KNOWS]->(c:Person) | c.name]] AS r"#,
        )
        .await
        .expect("nested pattern comprehension")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        nested_pc["results"][0]["data"][0]["row"][0],
        json!([["Carol"]]),
        "nested pattern comprehension is resolved per inner match: {nested_pc}"
    );

    // Finding 3: a parameter inside the inner pattern is substituted.
    let params = json!({ "bname": "Bob" });
    let with_param = fluree
        .query_cypher_with_params(
            &db,
            r#"MATCH (a:Person {name: "Alice"})
               RETURN [(a)-[:KNOWS]->(b:Person {name: $bname}) | b.name] AS r"#,
            params.as_object(),
        )
        .await
        .expect("param in inner pattern")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        with_param["results"][0]["data"][0]["row"][0],
        json!(["Bob"]),
        "param in the inner pattern is substituted: {with_param}"
    );
}

/// Seed Alice->Bob, Alice->Carol, Bob->Carol for the CALL subquery tests.
async fn seed_call_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let l = genesis_ledger(fluree, ledger_id);
    fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
                      (a)-[:KNOWS]->(c:Person {name: "Carol"}),
                      (b)-[:KNOWS]->(c)"#,
        )
        .await
        .expect("seed call graph")
        .ledger
}

#[tokio::test]
async fn cypher_call_subquery_uncorrelated_broadcasts() {
    // `CALL { … }` with no scope clause runs once; its single value is broadcast
    // to every outer row.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-uncorr").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"CALL { MATCH (x:Person) RETURN count(x) AS total }
               MATCH (p:Person)
               RETURN p.name AS name, total ORDER BY name"#,
        )
        .await
        .expect("uncorrelated call")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 3, "one row per person: {cj}");
    assert_eq!(data[0]["row"], json!(["Alice", 3]), "{cj}");
    assert_eq!(data[1]["row"], json!(["Bob", 3]), "{cj}");
    assert_eq!(data[2]["row"], json!(["Carol", 3]), "total broadcast: {cj}");
}

#[tokio::test]
async fn cypher_call_subquery_correlated_aggregate() {
    // `CALL (p) { … RETURN count(f) }` is grouped per imported `p`. Plain MATCH
    // inside drops a zero-match import (Carol has no outgoing KNOWS); OPTIONAL
    // MATCH retains it as 0.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-corr-agg").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS friends }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("correlated aggregate call")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "Carol (zero matches) drops out: {cj}");
    assert_eq!(data[0]["row"], json!(["Alice", 2]), "{cj}");
    assert_eq!(data[1]["row"], json!(["Bob", 1]), "{cj}");

    // OPTIONAL MATCH inside the CALL keeps the zero-match import as 0.
    let opt = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) { OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS friends }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("correlated optional aggregate call")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = opt["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 3, "OPTIONAL retains Carol: {opt}");
    assert_eq!(data[2]["row"], json!(["Carol", 0]), "{opt}");
}

#[tokio::test]
async fn cypher_call_subquery_correlated_row_expanding() {
    // A correlated CALL with no aggregate expands to one row per inner match.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-expand").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
               RETURN p.name AS name, friend ORDER BY friend"#,
        )
        .await
        .expect("row-expanding call")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "one row per friend: {cj}");
    assert_eq!(data[0]["row"], json!(["Alice", "Bob"]), "{cj}");
    assert_eq!(data[1]["row"], json!(["Alice", "Carol"]), "{cj}");
}

#[tokio::test]
async fn cypher_call_subquery_correlated_aggregate_join_mode() {
    // Soundness at scale: with >= 8 outer rows the SubqueryOperator picks
    // evaluate-once + hash-join (join-mode). The imports-as-GROUP-BY promotion
    // must still produce per-person counts, not a single global count broadcast
    // to every row. 12 people, each knowing exactly the next two (mod 12).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:call-joinmode");
    let n = 12;
    let people: Vec<JsonValue> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "@type": "ex:Person",
                "ex:name": format!("P{i:02}"),
                "ex:KNOWS": [
                    {"@id": format!("ex:p{}", (i + 1) % n)},
                    {"@id": format!("ex:p{}", (i + 2) % n)},
                ],
            })
        })
        .collect();
    let committed = fluree
        .insert(ledger0, &json!({"@context": ctx(), "@graph": people}))
        .await
        .expect("seed 12 people");
    let db = graphdb_from_ledger(&committed.ledger);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS friends }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("join-mode correlated aggregate")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), n, "one row per person: {cj}");
    for row in data {
        assert_eq!(
            row["row"][1],
            json!(2),
            "each person KNOWS exactly 2 — per-person count, not a global broadcast: {cj}"
        );
    }
}

#[tokio::test]
async fn cypher_call_subquery_union() {
    // `CALL { … UNION … }` — branches share a column shape; correlation flows
    // into each branch. Alice KNOWS Bob & Carol; Bob KNOWS Carol.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-union").await;
    let db = graphdb_from_ledger(&l);

    // Correlated UNION: per person, union two filtered branches.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) {
                 MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name STARTS WITH "B" RETURN f.name AS fn
                 UNION
                 MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name STARTS WITH "C" RETURN f.name AS fn
               }
               RETURN p.name AS name, fn ORDER BY name, fn"#,
        )
        .await
        .expect("correlated union call")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let rows: Vec<_> = cj["results"][0]["data"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["row"].clone())
        .collect();
    assert_eq!(
        rows,
        vec![
            json!(["Alice", "Bob"]),
            json!(["Alice", "Carol"]),
            json!(["Bob", "Carol"]),
        ],
        "correlated union per person: {cj}"
    );

    // UNION dedups; UNION ALL keeps duplicates. Two identical branches over
    // Alice's friends (Bob, Carol).
    let dedup = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               CALL (p) {
                 MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                 UNION
                 MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
               }
               RETURN fn ORDER BY fn"#,
        )
        .await
        .expect("union dedup")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        dedup["results"][0]["data"].as_array().expect("rows").len(),
        2,
        "UNION dedups identical branches: {dedup}"
    );

    let bag = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               CALL (p) {
                 MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                 UNION ALL
                 MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
               }
               RETURN fn ORDER BY fn"#,
        )
        .await
        .expect("union all bag")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        bag["results"][0]["data"].as_array().expect("rows").len(),
        4,
        "UNION ALL keeps duplicates: {bag}"
    );

    // Mixing UNION and UNION ALL in one CALL body is rejected.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person {name: "Alice"})
                   CALL (p) {
                     MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                     UNION
                     MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                     UNION ALL
                     MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                   }
                   RETURN fn"#,
            )
            .await
            .is_err(),
        "mixing UNION and UNION ALL in a CALL body is rejected"
    );

    // Branches must project the same columns.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person {name: "Alice"})
                   CALL (p) {
                     MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS fn
                     UNION
                     MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS other
                   }
                   RETURN fn"#,
            )
            .await
            .is_err(),
        "CALL UNION branches must project the same columns"
    );
}

#[tokio::test]
async fn cypher_call_subquery_import_all() {
    // `CALL (*)` imports the whole visible outer scope.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-star").await;
    let db = graphdb_from_ledger(&l);

    // (*) behaves like an explicit import of the referenced outer var `p`.
    let agg = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (*) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS friends }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("import-all aggregate")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = agg["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "Carol (zero matches) drops: {agg}");
    assert_eq!(data[0]["row"], json!(["Alice", 2]), "{agg}");
    assert_eq!(data[1]["row"], json!(["Bob", 1]), "{agg}");

    // (*) imports `x` too, so reusing its name inside is a correlated bound-bound
    // match (not a shadow error) — keeps only outer pairs where p KNOWS x.
    let pairs = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person), (x:Person)
               CALL (*) { MATCH (p)-[:KNOWS]->(x:Person) RETURN p.name AS hit }
               RETURN p.name AS pn, x.name AS xn ORDER BY pn, xn"#,
        )
        .await
        .expect("import-all correlated pair")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let rows: Vec<_> = pairs["results"][0]["data"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["row"].clone())
        .collect();
    assert_eq!(
        rows,
        vec![
            json!(["Alice", "Bob"]),
            json!(["Alice", "Carol"]),
            json!(["Bob", "Carol"]),
        ],
        "(*) imports x → bound-bound correlation keeps only KNOWS pairs: {pairs}"
    );

    // A RETURN re-binding an outer name is still rejected, even under (*).
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person), (q:Person)
                   CALL (*) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f AS q }
                   RETURN q.name"#,
            )
            .await
            .is_err(),
        "RETURN re-binding an outer name is rejected even under (*)"
    );
}

#[tokio::test]
async fn cypher_call_subquery_nested() {
    // A nested CALL sees the variables imported by its enclosing CALL.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-nested").await;
    let db = graphdb_from_ledger(&l);

    // Nested explicit import: the inner CALL (p) correlates on the outer CALL's
    // imported `p`.
    let nested = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) {
                 CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS c }
                 RETURN c AS friends
               }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("nested explicit import")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = nested["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "Carol (zero matches) drops: {nested}");
    assert_eq!(data[0]["row"], json!(["Alice", 2]), "{nested}");
    assert_eq!(data[1]["row"], json!(["Bob", 1]), "{nested}");

    // Nested CALL (*) must import the enclosing scope (incl. `p`), NOT silently
    // uncorrelate to a global count (which would broadcast 3 to every person).
    let star = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person)
               CALL (p) {
                 CALL (*) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS c }
                 RETURN c AS friends
               }
               RETURN p.name AS name, friends ORDER BY name"#,
        )
        .await
        .expect("nested import-all")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = star["results"][0]["data"].as_array().expect("rows");
    assert_eq!(
        data.len(),
        2,
        "nested CALL (*) correlates on p (not a global broadcast): {star}"
    );
    assert_eq!(data[0]["row"], json!(["Alice", 2]), "{star}");
    assert_eq!(data[1]["row"], json!(["Bob", 1]), "{star}");

    // A WITH inside the body narrows scope: after `WITH f` (which drops `p`), a
    // nested CALL (p) can no longer import `p`.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person)
                   CALL (p) {
                     MATCH (p)-[:KNOWS]->(f:Person)
                     WITH f
                     CALL (p) { MATCH (p)-[:KNOWS]->(g:Person) RETURN count(g) AS c }
                     RETURN c AS cc
                   }
                   RETURN p.name, cc"#,
            )
            .await
            .is_err(),
        "a WITH that drops the import narrows it out of a nested CALL's scope"
    );
}

#[tokio::test]
async fn cypher_call_subquery_rejections() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_call_graph(&fluree, "it/cypher:call-reject").await;
    let db = graphdb_from_ledger(&l);

    // A write inside CALL is deferred.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person) CALL (p) { CREATE (p)-[:SELF]->(p) RETURN p AS x } RETURN x"#,
            )
            .await
            .is_err(),
        "writes inside CALL are deferred"
    );

    // RETURN * inside CALL is rejected (opaque output schema).
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS]->(f) RETURN * } RETURN p.name"#,
            )
            .await
            .is_err(),
        "RETURN * inside CALL is rejected"
    );

    // A subquery RETURN that re-binds an imported name collides.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS]->(f) RETURN f AS p } RETURN p.name"#,
            )
            .await
            .is_err(),
        "returning an imported name collides"
    );

    // A subquery RETURN that re-binds a NON-import outer name also collides
    // (the executor would silently drop the subquery's value otherwise).
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person), (q:Person) CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f AS q } RETURN q.name"#,
            )
            .await
            .is_err(),
        "returning a name already bound elsewhere in the outer scope collides"
    );

    // An import that was never bound in the outer scope is rejected.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"CALL (p) { MATCH (p:Person) RETURN p.name AS name } RETURN name"#,
            )
            .await
            .is_err(),
        "importing a variable not bound outside is rejected"
    );

    // Strict shadowing: the body reuses an outer name (`x`) internally without
    // importing it — ambiguous, rejected until per-subquery scoping lands.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (p:Person), (x:Person)
                   CALL (p) { MATCH (p)-[:KNOWS]->(x:Person) RETURN count(x) AS c }
                   RETURN c"#,
            )
            .await
            .is_err(),
        "an un-imported outer name reused inside the body is rejected"
    );
}

#[tokio::test]
async fn cypher_scalar_functions_extended() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:scalar-ext");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {id: 1})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {id: 1})
               RETURN substring("hello", 1) AS sub1,
                      substring("hello", 1, 3) AS sub2,
                      left("hello", 3) AS lft,
                      right("hello", 2) AS rgt,
                      right("hi", 9) AS rgtclamp,
                      trim("  hi  ") AS t,
                      ltrim("  hi  ") AS lt,
                      rtrim("  hi  ") AS rt,
                      replace("a-b-a", "a", "X") AS rep,
                      split("a,b,c", ",") AS sp,
                      sqrt(16) AS sq,
                      sign(-5) AS sg,
                      sign(0) AS sg0,
                      log(1) AS lg,
                      2 ^ 10 AS pw,
                      2 ^ 3 ^ 2 AS pwassoc"#,
        )
        .await
        .expect("scalar functions")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!("ello"), "substring 2-arg: {cj}");
    assert_eq!(row[1], json!("ell"), "substring 3-arg: {cj}");
    assert_eq!(row[2], json!("hel"), "left: {cj}");
    assert_eq!(row[3], json!("lo"), "right: {cj}");
    assert_eq!(row[4], json!("hi"), "right clamps n>len: {cj}");
    assert_eq!(row[5], json!("hi"), "trim: {cj}");
    assert_eq!(row[6], json!("hi  "), "ltrim: {cj}");
    assert_eq!(row[7], json!("  hi"), "rtrim: {cj}");
    assert_eq!(row[8], json!("X-b-X"), "replace literal: {cj}");
    assert_eq!(row[9], json!(["a", "b", "c"]), "split: {cj}");
    assert_eq!(row[10], json!(4.0), "sqrt: {cj}");
    assert_eq!(row[11], json!(-1), "sign neg: {cj}");
    assert_eq!(row[12], json!(0), "sign zero: {cj}");
    assert_eq!(row[13], json!(0.0), "log(1)=0: {cj}");
    assert_eq!(row[14], json!(1024.0), "2^10: {cj}");
    assert_eq!(row[15], json!(512.0), "2^3^2 right-assoc = 2^9: {cj}");
}

#[tokio::test]
async fn cypher_id_function_returns_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:id-fn");
    let committed = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@id": "ex:zoe", "@type": "ex:Person", "ex:name": "Zoe"}),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let cj = fluree
        .query_cypher(&db, r#"MATCH (p:Person {name: "Zoe"}) RETURN id(p) AS id"#)
        .await
        .expect("id function")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!("http://example.org/zoe"),
        "id(n) returns the node's IRI string: {cj}"
    );
}

#[tokio::test]
async fn cypher_map_projection() {
    // `n{.key}` selectors, a `key: expr` entry, and `n{.*}` (all properties).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:map-projection");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice", age: 30})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Explicit selectors + a computed entry.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               RETURN p{.name, .age, nextYear: p.age + 1} AS person"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!({"name": "Alice", "age": 30, "nextYear": 31}),
        "explicit selectors + computed entry: {cj}"
    );

    // `.*` projects all data properties (like properties(n)).
    let star = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"}) RETURN p{.*} AS person"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        star["results"][0]["data"][0]["row"][0],
        json!({"name": "Alice", "age": 30}),
        "`.*` is all data properties: {star}"
    );
}

#[tokio::test]
async fn cypher_map_projection_mixed_star_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:map-projection-mixed");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let res = fluree
        .query_cypher(&db, r#"MATCH (p:Person) RETURN p{.*, extra: 1} AS person"#)
        .await;
    assert!(
        res.is_err(),
        "mixing .* with other selectors should be rejected"
    );
}

#[tokio::test]
async fn cypher_list_comprehension_arithmetic_and_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:listcomp");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    // map projection, WHERE filter, and both together.
    let row = cypher_row(
        &fluree,
        &l,
        r#"MATCH (p:Person)
           RETURN [x IN range(1, 4) | x * 2] AS doubled,
                  [x IN range(1, 6) WHERE x % 2 = 0] AS evens,
                  [x IN range(1, 5) WHERE x > 2 | x * 10] AS big"#,
    )
    .await;
    assert_eq!(row[0], json!([2, 4, 6, 8]), "map: {row}");
    assert_eq!(row[1], json!([2, 4, 6]), "filter: {row}");
    assert_eq!(row[2], json!([30, 40, 50]), "filter+map: {row}");
}

#[tokio::test]
async fn cypher_reduce_folds_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:reduce");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let row = cypher_row(
        &fluree,
        &l,
        r#"MATCH (p:Person)
           RETURN reduce(s = 0, x IN range(1, 4) | s + x) AS total,
                  reduce(s = 1, x IN [2, 3, 4] | s * x) AS product"#,
    )
    .await;
    assert_eq!(row[0], json!(10), "sum 1..4: {row}");
    assert_eq!(row[1], json!(24), "product: {row}");
}

#[tokio::test]
async fn cypher_list_predicates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:listpred");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let row = cypher_row(
        &fluree,
        &l,
        r#"MATCH (p:Person)
           RETURN all(x IN [2, 4, 6] WHERE x % 2 = 0) AS allEven,
                  any(x IN [1, 2, 3] WHERE x > 2) AS anyBig,
                  none(x IN [1, 2, 3] WHERE x > 5) AS noneBig,
                  single(x IN [1, 2, 3] WHERE x = 2) AS oneTwo,
                  all(x IN [] WHERE x > 0) AS emptyAll,
                  any(x IN [] WHERE x > 0) AS emptyAny"#,
    )
    .await;
    assert_eq!(row[0], json!(true), "all even: {row}");
    assert_eq!(row[1], json!(true), "any > 2: {row}");
    assert_eq!(row[2], json!(true), "none > 5: {row}");
    assert_eq!(row[3], json!(true), "single = 2: {row}");
    assert_eq!(row[4], json!(true), "empty all = true: {row}");
    assert_eq!(row[5], json!(false), "empty any = false: {row}");
}

#[tokio::test]
async fn cypher_comprehension_member_access_map_param() {
    // Loop-local member access on a map element ($people is a list of maps).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:listcomp-map");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let params: fluree_db_cypher::ParamMap = serde_json::from_value(json!({
        "people": [{"name": "Bob", "age": 30}, {"name": "Carol", "age": 40}]
    }))
    .expect("params");
    let cj = fluree
        .query_cypher_with_params(
            &db,
            r#"MATCH (p:Person)
               RETURN [row IN $people | row.name] AS names,
                      [row IN $people WHERE row.age > 35 | row.name] AS older"#,
            Some(&params),
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!(["Bob", "Carol"]), "map member access: {cj}");
    assert_eq!(row[1], json!(["Carol"]), "filter on map member: {cj}");
}

#[tokio::test]
async fn cypher_comprehension_member_access_node() {
    // Loop-local member access on a node element (collect → list of nodes).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:listcomp-node");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice", age: 30}),
                      (b:Person {name: "Bob", age: 40})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let names = cypher_row(
        &fluree,
        &l,
        r#"MATCH (p:Person)
           RETURN [x IN collect(p) | x.name] AS names"#,
    )
    .await;
    // Order follows collect(); compare as a set.
    let mut got: Vec<String> = serde_json::from_value(names[0].clone()).expect("list of names");
    got.sort();
    assert_eq!(
        got,
        vec!["Alice".to_string(), "Bob".to_string()],
        "node member: {names}"
    );
}

#[tokio::test]
async fn cypher_comprehension_null_and_nonlist_input() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:listcomp-null");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    // A non-existent property is null → comprehension over null is null (not []).
    let row = cypher_row(
        &fluree,
        &l,
        r#"MATCH (p:Person)
           RETURN [x IN p.missingList | x] AS over_null,
                  any(x IN p.missingList WHERE x > 0) AS any_null"#,
    )
    .await;
    assert_eq!(
        row[0],
        json!(null),
        "comprehension over null is null: {row}"
    );
    assert_eq!(row[1], json!(null), "predicate over null is null: {row}");
}

#[tokio::test]
async fn cypher_scalar_string_and_math_functions() {
    // The clean 1:1 scalar mappings: toUpper/toLower (string), round/floor/ceil
    // (math). `rand()` is wired but non-deterministic, so it's exercised in
    // a range check separately.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:scalar-fns");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice", score: 2.4})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               RETURN toUpper(p.name) AS up, toLower(p.name) AS down,
                      floor(p.score) AS fl, ceil(p.score) AS ce, round(p.score) AS rd"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!("ALICE"), "toUpper: {cj}");
    assert_eq!(row[1], json!("alice"), "toLower: {cj}");
    assert_eq!(row[2].as_f64(), Some(2.0), "floor: {cj}");
    assert_eq!(row[3].as_f64(), Some(3.0), "ceil: {cj}");
    assert_eq!(row[4].as_f64(), Some(2.0), "round: {cj}");
}

#[tokio::test]
async fn cypher_properties_and_keys() {
    // properties(n) → a map of all data properties; keys(n) → their names. Both
    // exclude the label (rdf:type) and any relationship edges.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:properties");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice", age: 30})-[:KNOWS]->(b:Person {name: "Bob"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // properties(a): name + age only — not the :Person label, not the KNOWS edge.
    let props = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}) RETURN properties(a) AS p"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        props["results"][0]["data"][0]["row"][0],
        json!({"name": "Alice", "age": 30}),
        "properties() is data-only: {props}"
    );

    // keys(a): the property names, sorted.
    let keys = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}) RETURN keys(a) AS k"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        keys["results"][0]["data"][0]["row"][0],
        json!(["age", "name"]),
        "keys() is the sorted property names: {keys}"
    );
}

#[tokio::test]
async fn cypher_map_value_reused_and_nested() {
    // A map-valued variable reused inside another value must survive the
    // round-trip (the `try_eval_to_binding` Map passthrough), and maps nest
    // maps/lists.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:map-nested");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice", age: 30})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Reuse a map var: `WITH properties(p) AS props RETURN {wrapped: props}`.
    let reused = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               WITH p, properties(p) AS props
               RETURN {name: p.name, props: props} AS row"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        reused["results"][0]["data"][0]["row"][0],
        json!({"name": "Alice", "props": {"name": "Alice", "age": 30}}),
        "map var reused inside a map literal: {reused}"
    );

    // Nested map + list literal in one shape.
    let nested = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name: "Alice"})
               RETURN {nums: [1, 2, 3], info: {city: "NYC"}} AS row"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        nested["results"][0]["data"][0]["row"][0],
        json!({"nums": [1, 2, 3], "info": {"city": "NYC"}}),
        "nested map + list: {nested}"
    );
}

#[tokio::test]
async fn cypher_properties_preserves_language_and_list_order() {
    // properties(n) must keep a `rdf:langString`'s @language (visible in JSON-LD
    // output) and render an `@list` property in its stored order.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:props-lang");
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@type": "ex:Person",
        "ex:greeting": {"@value": "Bonjour", "@language": "fr"},
        "ex:tags": {"@list": ["x", "y", "z"]},
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let jl = fluree
        .query_cypher(&db, r#"MATCH (n:Person) RETURN properties(n) AS p"#)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let props = &jl[0][0];
    assert_eq!(
        props["greeting"],
        json!({"@value": "Bonjour", "@language": "fr"}),
        "langString keeps @language: {jl}"
    );
    assert_eq!(
        props["tags"],
        json!(["x", "y", "z"]),
        "@list property keeps its order: {jl}"
    );
}

#[tokio::test]
async fn cypher_object_param_used_as_map_value() {
    // An object `$param` substitutes to a map value usable in a projection.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:map-param");
    let l = fluree
        .transact_cypher(l, r#"CREATE (p:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let params: fluree_db_cypher::ParamMap =
        serde_json::from_value(json!({"filter": {"city": "NYC", "zip": 10001}})).expect("params");
    let cj = fluree
        .query_cypher_with_params(
            &db,
            r#"MATCH (p:Person {name: "Alice"}) RETURN $filter AS f"#,
            Some(&params),
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!({"city": "NYC", "zip": 10001}),
        "object param → map value: {cj}"
    );
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
async fn transact_cypher_with_computed_alias_carries_into_set() {
    // WITH before a write: a computed projection (`a.birthYear + 30 AS adultAt`)
    // is carried into the SET and actually lands as a stored value.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:with-computed");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice", birthYear: 1990})"#)
        .await
        .expect("seed")
        .ledger;

    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {name: "Alice"})
               WITH a, a.birthYear + 30 AS adultAt
               SET a.adultAt = adultAt"#,
        )
        .await
        .expect("with+set")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let rows = fluree
        .query_cypher(&db, r#"MATCH (a:Person {name: "Alice"}) RETURN a.adultAt"#)
        .await
        .expect("read back")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rows,
        serde_json::json!([[2020]]),
        "computed value stored: {rows}"
    );
}

#[tokio::test]
async fn transact_cypher_with_filter_gates_a_write() {
    // WITH ... WHERE filters which matched rows reach the write.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:with-filter");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice", age: 40})
               CREATE (b:Person {name: "Bob", age: 20})"#,
        )
        .await
        .expect("seed")
        .ledger;

    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (p:Person)
               WITH p, p.age AS age WHERE age >= 30
               SET p.adult = true"#,
        )
        .await
        .expect("with+filter+set")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {adult: true}) RETURN p.name ORDER BY p.name"#,
        )
        .await
        .expect("read back")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rows,
        serde_json::json!([["Alice"]]),
        "only the over-30 person was flagged: {rows}"
    );
}

#[tokio::test]
async fn transact_cypher_with_before_delete_is_rejected_not_silent() {
    // `WITH a DELETE r` (r dropped by WITH) must error through the real
    // classifier→lowering path, not silently delete the out-of-scope edge.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:with-delete");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
        )
        .await
        .expect("seed")
        .ledger;

    let res = fluree
        .transact_cypher(
            l.clone(),
            r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WITH a DELETE r"#,
        )
        .await;
    assert!(res.is_err(), "WITH before DELETE must be rejected");

    // The edge is untouched — the rejection happened before any staging.
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
            .await
            .unwrap()
            .row_count(),
        1,
        "the KNOWS edge survives the rejected DELETE"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_creates_then_is_a_noop() {
    // Relationship MERGE = find-or-create the whole path. The first run mints
    // both endpoints and the edge; the second finds the path and inserts
    // nothing (one NOT EXISTS guard over the whole pattern).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel");

    let stmt = r#"MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#;
    let edge_q = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name";

    let l = fluree.transact_cypher(l, stmt).await.expect("merge").ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree.query_cypher(&db, edge_q).await.unwrap().row_count(),
        1,
        "first MERGE creates the Alice-KNOWS->Bob path"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        2,
        "two endpoints created"
    );

    // Re-running the identical MERGE finds the path → no duplicate edge / nodes.
    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("merge#2")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree.query_cypher(&db, edge_q).await.unwrap().row_count(),
        1,
        "second MERGE is a no-op — the path already exists"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        2,
        "still exactly two Person nodes"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_on_create_set_endpoint() {
    // ON CREATE SET targeting an endpoint node var fires only on the create run.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-on-create");

    let stmt = r#"MERGE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})
                  ON CREATE SET b.note = "fresh""#;
    let l = fluree.transact_cypher(l, stmt).await.expect("merge").ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (b:Person {note: "fresh"}) RETURN b"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "ON CREATE SET applied to the tail endpoint"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_bound_endpoints_is_per_row_find_or_create() {
    // Scope B: `MATCH (a),(b) MERGE (a)-[:KNOWS]->(b)` — the endpoints are bound
    // by the MATCH, so the MERGE runs per matched (a,b) row. Seed Alice, Bob,
    // and one existing Alice->Bob edge; then MERGE every Person→Person pair.
    // Existing edges are left alone; only the missing ones are created.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-bound");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
        )
        .await
        .expect("seed")
        .ledger;

    // MERGE every ordered distinct pair of Persons. Alice->Bob exists (no-op);
    // Bob->Alice is created. (Self-pairs are excluded by name inequality.)
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person), (b:Person) WHERE a.name <> b.name
               MERGE (a)-[:KNOWS]->(b)"#,
        )
        .await
        .expect("merge pairs")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Exactly two directed edges now: Alice->Bob (pre-existing) and Bob->Alice.
    let edges = fluree
        .query_cypher(
            &db,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS f, b.name AS t ORDER BY f",
        )
        .await
        .expect("edges")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        edges,
        serde_json::json!([["Alice", "Bob"], ["Bob", "Alice"]]),
        "the pre-existing edge is untouched; only the missing reverse edge is created: {edges}"
    );

    // Re-running the same MERGE is a no-op — both edges now exist.
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person), (b:Person) WHERE a.name <> b.name
               MERGE (a)-[:KNOWS]->(b)"#,
        )
        .await
        .expect("merge#2")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
            .await
            .unwrap()
            .row_count(),
        2,
        "still exactly two edges — no duplicates on the second MERGE"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_on_create_set_bound_head() {
    // ON CREATE SET targeting a MATCH-bound endpoint (the head) fires only when
    // the edge is created, and writes onto the existing bound node.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-oncreate-head");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"}) CREATE (b:Person {name: "Bob"})"#,
        )
        .await
        .expect("seed")
        .ledger;

    let stmt = r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
                  MERGE (a)-[:KNOWS]->(b)
                  ON CREATE SET a.linked = "yes""#;
    let l = fluree.transact_cypher(l, stmt).await.expect("merge").ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (a:Person {linked: "yes"}) RETURN a.name"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "ON CREATE SET wrote onto the bound head node"
    );

    // Second run: the edge already exists → ON CREATE SET does not fire again
    // (no second `linked` value — the property stays single-valued).
    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("merge#2")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (a:Person {linked: "yes"}) RETURN a"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "still exactly one match — ON CREATE SET did not re-fire"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_bound_head_new_tail() {
    // Mixed: bound head + a new tail node introduced by the MERGE. Per matched
    // Person, find-or-create a Pet named Rex.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-newtail");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;

    let stmt = r#"MATCH (a:Person {name: "Alice"})
                  MERGE (a)-[:HAS_PET]->(p:Pet {name: "Rex"})"#;
    let l = fluree.transact_cypher(l, stmt).await.expect("merge").ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a:Person)-[:HAS_PET]->(p:Pet) RETURN p.name")
            .await
            .unwrap()
            .row_count(),
        1,
        "first run creates the Pet + edge"
    );

    // Second run finds the existing Pet+edge → no new Pet.
    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("merge#2")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (p:Pet) RETURN p")
            .await
            .unwrap()
            .row_count(),
        1,
        "second run is a no-op — exactly one Pet"
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
        r"MATCH (n:Nonexistent) RETURN collect(n) AS xs",
        r"MATCH (n:Nonexistent) RETURN collect(DISTINCT n) AS xs",
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
        r"MATCH (a:Person)-[:KNOWS]->(b) RETURN a, collect(b) AS bs ORDER BY bs",
        r"MATCH (a:Person)-[:KNOWS]->(b) RETURN a, collect(b) ORDER BY collect(b)",
    ] {
        let err = fluree
            .query_cypher(&db, q)
            .await
            .expect_err("ORDER BY on a collect list must be rejected");
        assert!(format!("{err}").contains("ORDER BY"), "{err}");
    }
}

#[tokio::test]
async fn cypher_with_collect_carries_list() {
    // collect() projected by WITH now flows out as a real list (was deferred).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:with-collect").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b.name) AS bs RETURN a.name AS name, bs ORDER BY name",
        )
        .await
        .expect("collect() in WITH carries a list")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 3, "Alice, Bob, Carol each KNOW one: {cj}");
    assert_eq!(data[0]["row"], json!(["Alice", ["Bob"]]), "{cj}");
    assert_eq!(data[1]["row"], json!(["Bob", ["Carol"]]), "{cj}");
    assert_eq!(data[2]["row"], json!(["Carol", ["Dave"]]), "{cj}");
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
                &format!(r"MATCH (a:Person)-[:KNOWS{path}]->(x) RETURN x"),
            )
            .await
            .unwrap_or_else(|e| panic!("unregistered type `{path}` should not error: {e}"));
        assert_eq!(rows.row_count(), 0, "unregistered type with `{path}`");
    }
}

#[tokio::test]
async fn cypher_var_length_relationship_uniqueness_no_self_rows() {
    // Bounded var-length on a cyclic/undirected graph must not return spurious
    // self-rows from edge reuse (`a-b-a`). Graph: a(1)-knows-b(2)-knows-c(3).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:varlen-uniq");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Person", "ex:id": 1, "ex:KNOWS": {"@id": "ex:b"}},
                    {"@id": "ex:b", "@type": "ex:Person", "ex:id": 2, "ex:KNOWS": {"@id": "ex:c"}},
                    {"@id": "ex:c", "@type": "ex:Person", "ex:id": 3},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Undirected *2..2 from a: only c (id 3); NOT a itself (the a-b-a walk
    // reuses the a-b edge and is excluded).
    let rows = fluree
        .query_cypher(
            &db,
            "MATCH (a:Person {id: 1})-[:KNOWS*2..2]-(x) RETURN x.id AS id",
        )
        .await
        .expect("var-length uniqueness")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let ids: Vec<i64> = rows
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_i64())
        .collect();
    assert_eq!(ids, vec![3], "only c; no spurious self-row for a: {rows}");
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

#[tokio::test]
async fn cypher_shortest_path_length_directed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:sp-directed").await;
    let db = graphdb_from_ledger(&l);

    // Alice -> Bob -> Carol -> Dave; directed shortestPath Alice→Dave = 3 hops.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}), (d:Person {name: "Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("shortestPath length")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(3), "Alice→Dave is 3 KNOWS hops: {out}");
}

#[tokio::test]
async fn cypher_relationships_of_path() {
    // relationships(p) yields one relationship value per hop; type/startNode/
    // endNode work off each. Alice -> Bob -> Carol -> Dave.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:rels-of-path").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(c))
               RETURN [r IN relationships(p) | type(r)] AS types,
                      size(relationships(p)) AS n,
                      startNode(relationships(p)[0]) AS first_start,
                      endNode(relationships(p)[1]) AS last_end,
                      a AS aa, c AS cc"#,
        )
        .await
        .expect("relationships(p)")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!(["KNOWS", "KNOWS"]), "type per hop: {cj}");
    assert_eq!(row[1], json!(2), "Alice→Carol is 2 hops: {cj}");
    assert_eq!(row[2], row[4], "first hop start == Alice: {cj}");
    assert_eq!(row[3], row[5], "last hop end == Carol: {cj}");
}

#[tokio::test]
async fn cypher_var_length_rel_and_path_binding() {
    // Bounded var-length: bind a relationship variable as a rel list and a path
    // variable. Alice -> Bob -> Carol -> Dave.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:varlen-bind").await;
    let db = graphdb_from_ledger(&l);

    // `-[r:KNOWS*1..2]->` binds r to the list of relationships on each match.
    let rels = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[r:KNOWS*1..2]->(b:Person)
               RETURN b.name AS name, size(r) AS hops, [x IN r | type(x)] AS types
               ORDER BY name"#,
        )
        .await
        .expect("var-length rel binding")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = rels["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data.len(), 2, "Alice reaches Bob (1) and Carol (2): {rels}");
    assert_eq!(data[0]["row"], json!(["Bob", 1, ["KNOWS"]]), "{rels}");
    assert_eq!(
        data[1]["row"],
        json!(["Carol", 2, ["KNOWS", "KNOWS"]]),
        "{rels}"
    );

    // `MATCH p = (a)-[:KNOWS*1..2]->(b)` binds p as a path; relationships(p)
    // works over the bound path.
    let path = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[:KNOWS*1..2]->(b:Person)
               RETURN b.name AS name, length(p) AS len, size(relationships(p)) AS nrel
               ORDER BY name"#,
        )
        .await
        .expect("var-length path binding")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = path["results"][0]["data"].as_array().expect("rows");
    assert_eq!(data[0]["row"], json!(["Bob", 1, 1]), "{path}");
    assert_eq!(data[1]["row"], json!(["Carol", 2, 2]), "{path}");

    // Unbounded rel binding is deferred with a clear error.
    assert!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (a:Person {name: "Alice"})-[r:KNOWS*]->(b:Person) RETURN size(r)"#,
            )
            .await
            .is_err(),
        "unbounded var-length rel binding is deferred"
    );
}

#[tokio::test]
async fn cypher_shortest_path_length_undirected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:sp-undirected").await;
    let db = graphdb_from_ledger(&l);

    // Undirected search from the middle reaches Alice in 1 hop (Bob<-Alice).
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (b:Person {name: "Bob"}), (a:Person {name: "Alice"})
               MATCH p = shortestPath((b)-[:KNOWS*]-(a))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("undirected shortestPath")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(1), "Bob and Alice are adjacent: {out}");
}

#[tokio::test]
async fn cypher_shortest_path_no_path_drops_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:sp-nopath").await;
    let db = graphdb_from_ledger(&l);

    // Directed Dave→Alice has no path (chain is one-way). Mandatory MATCH
    // drops the row.
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (d:Person {name: "Dave"}), (a:Person {name: "Alice"})
               MATCH p = shortestPath((d)-[:KNOWS*]->(a))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("no-path shortestPath");
    assert_eq!(rows.row_count(), 0, "no directed Dave→Alice path");
}

#[tokio::test]
async fn cypher_shortest_path_optional_null_for_missing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:sp-optional").await;
    let db = graphdb_from_ledger(&l);

    // IC13 shape: OPTIONAL MATCH keeps the row with a null path when no path
    // exists; CASE maps that to -1.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (d:Person {name: "Dave"}), (a:Person {name: "Alice"})
               OPTIONAL MATCH p = shortestPath((d)-[:KNOWS*]->(a))
               RETURN CASE WHEN p IS NULL THEN -1 ELSE length(p) END AS len"#,
        )
        .await
        .expect("optional shortestPath")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(-1), "no path → -1 via CASE: {out}");
}

#[tokio::test]
async fn cypher_all_shortest_paths_returns_each_minimal_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:all-sp");
    // Diamond: a→b→d and a→c→d are two distinct 2-hop shortest paths a..d.
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Person", "ex:name": "A",
                     "ex:KNOWS": [{"@id": "ex:b"}, {"@id": "ex:c"}]},
                    {"@id": "ex:b", "@type": "ex:Person", "ex:name": "B", "ex:KNOWS": {"@id": "ex:d"}},
                    {"@id": "ex:c", "@type": "ex:Person", "ex:name": "C", "ex:KNOWS": {"@id": "ex:d"}},
                    {"@id": "ex:d", "@type": "ex:Person", "ex:name": "D"},
                ]
            }),
        )
        .await
        .expect("seed diamond")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "A"}), (d:Person {name: "D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]->(d))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("allShortestPaths")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = out.as_array().expect("rows");
    assert_eq!(
        rows.len(),
        2,
        "two distinct 2-hop paths a→b→d, a→c→d: {out}"
    );
    assert!(
        rows.iter().all(|r| r[0] == json!(2)),
        "both minimal paths are length 2: {out}"
    );
}

#[tokio::test]
async fn cypher_all_shortest_paths_honors_lower_hop_bound() {
    // A direct edge A→D (length 1) plus A→B→D (length 2). With `*2..` the
    // length-1 path is excluded, so the shortest qualifying length is 2 — the
    // distance-finalizing BFS would otherwise stop at the hidden length-1 path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:sp-minhops");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Person", "ex:name": "A",
                     "ex:KNOWS": [{"@id": "ex:b"}, {"@id": "ex:d"}]},
                    {"@id": "ex:b", "@type": "ex:Person", "ex:name": "B", "ex:KNOWS": {"@id": "ex:d"}},
                    {"@id": "ex:d", "@type": "ex:Person", "ex:name": "D"},
                ]
            }),
        )
        .await
        .expect("seed shortcut+detour")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "A"}), (d:Person {name: "D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*2..]->(d))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("allShortestPaths *2..")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = out.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "only the length-2 detour qualifies: {out}");
    assert_eq!(rows[0][0], json!(2), "A→B→D, not the excluded A→D: {out}");
}

#[tokio::test]
async fn cypher_shortest_path_single_honors_lower_hop_bound() {
    // Single shortestPath with `*2..` must also skip the length-1 shortcut.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:sp-single-minhops");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Person", "ex:name": "A",
                     "ex:KNOWS": [{"@id": "ex:b"}, {"@id": "ex:d"}]},
                    {"@id": "ex:b", "@type": "ex:Person", "ex:name": "B", "ex:KNOWS": {"@id": "ex:d"}},
                    {"@id": "ex:d", "@type": "ex:Person", "ex:name": "D"},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "A"}), (d:Person {name: "D"})
               MATCH p = shortestPath((a)-[:KNOWS*2..]->(d))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("shortestPath *2..")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(2),
        "shortest qualifying path is length 2: {out}"
    );
}

/// Seed 4 persons with `ex:id` 1..4 where person 1 KNOWS persons 3 and 4.
async fn seed_exists_graph(
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
                    {"@id": "ex:n1", "@type": "ex:Person", "ex:id": 1, "ex:name": "Alice",
                     "ex:KNOWS": [{"@id": "ex:n3"}, {"@id": "ex:n4"}]},
                    {"@id": "ex:n2", "@type": "ex:Person", "ex:id": 2, "ex:name": "Bob"},
                    {"@id": "ex:n3", "@type": "ex:Person", "ex:id": 3, "ex:name": "Carol"},
                    {"@id": "ex:n4", "@type": "ex:Person", "ex:id": 4, "ex:name": "Dave"},
                ]
            }),
        )
        .await
        .expect("seed exists graph")
        .ledger
}

#[tokio::test]
async fn cypher_exists_bare_pattern_form() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:exists-bare").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1}) WHERE EXISTS { (p)-[:KNOWS]-(x:Person) } RETURN p.id AS id",
        )
        .await
        .expect("exists bare")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(1), "person 1 has a KNOWS edge: {out}");
}

#[tokio::test]
async fn cypher_exists_subquery_match_form() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:exists-match").await;
    let db = graphdb_from_ledger(&l);

    // Subquery form with an explicit MATCH but no inner WHERE.
    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1}) WHERE EXISTS { MATCH (p)-[:KNOWS]-(x:Person) } RETURN p.id AS id",
        )
        .await
        .expect("exists match-form")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(1), "MATCH-form existence holds: {out}");
}

#[tokio::test]
async fn cypher_exists_subquery_inner_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:exists-inner-where").await;
    let db = graphdb_from_ledger(&l);

    // The IC4 shape: subquery form with an inner WHERE. Person 1 KNOWS 3 and 4
    // (both id > 2), so the filtered existence test holds.
    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1})
               WHERE EXISTS { MATCH (p)-[:KNOWS]-(x) WHERE x.id > 2 }
               RETURN p.id AS id",
        )
        .await
        .expect("exists inner-where")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(1),
        "person 1 has a friend with id > 2: {out}"
    );
}

#[tokio::test]
async fn cypher_exists_inner_where_excludes_when_unmet() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:exists-inner-where-neg").await;
    let db = graphdb_from_ledger(&l);

    // No friend with id > 100, so the filtered existence test fails and the
    // row is excluded.
    let rows = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1})
               WHERE EXISTS { MATCH (p)-[:KNOWS]-(x) WHERE x.id > 100 }
               RETURN p.id AS id",
        )
        .await
        .expect("exists inner-where unmet");
    assert_eq!(rows.row_count(), 0, "no friend with id > 100");
}

#[tokio::test]
async fn cypher_not_exists_subquery_inner_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:not-exists-inner-where").await;
    let db = graphdb_from_ledger(&l);

    // NOT EXISTS with an inner WHERE: person 1 has no friend with id > 100,
    // so NOT EXISTS holds and the row is kept.
    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1})
               WHERE NOT EXISTS { MATCH (p)-[:KNOWS]-(x) WHERE x.id > 100 }
               RETURN p.id AS id",
        )
        .await
        .expect("not exists inner-where")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(1),
        "no friend with id > 100 → NOT EXISTS holds: {out}"
    );
}

#[tokio::test]
async fn cypher_exists_in_map_projection_computed_entry() {
    // EXISTS as a computed entry inside a map projection / map literal must be
    // resolved per row (not fall through to a synchronous `false`). Person 1
    // KNOWS others → true; persons 2/3/4 have no outgoing KNOWS → false.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_exists_graph(&fluree, "it/cypher:exists-in-map").await;
    let db = graphdb_from_ledger(&l);

    // Map projection: `p{id: ..., hasFriends: EXISTS { ... }}`.
    let proj = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person)
               RETURN p{id: p.id, hasFriends: EXISTS { (p)-[:KNOWS]->(x:Person) }} AS info
               ORDER BY p.id",
        )
        .await
        .expect("map projection with EXISTS")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        proj["results"][0]["data"][0]["row"][0],
        json!({"id": 1, "hasFriends": true}),
        "person 1 has outgoing KNOWS → EXISTS true: {proj}"
    );
    assert_eq!(
        proj["results"][0]["data"][1]["row"][0],
        json!({"id": 2, "hasFriends": false}),
        "person 2 has no outgoing KNOWS → EXISTS false: {proj}"
    );

    // Bare map literal with a nested EXISTS must behave identically.
    let lit = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 1})
               RETURN {ok: EXISTS { (p)-[:KNOWS]->(x:Person) }} AS info",
        )
        .await
        .expect("map literal with EXISTS")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        lit["results"][0]["data"][0]["row"][0],
        json!({"ok": true}),
        "bare map literal resolves nested EXISTS: {lit}"
    );
}

#[tokio::test]
async fn cypher_create_list_valued_property_stores_each_element() {
    // IU1 (AddPerson) shape: a node with a list-valued literal property
    // (email[]) becomes a multi-valued RDF predicate — one flake per element.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:list-prop");
    let committed = fluree
        .transact_cypher(
            ledger0,
            r"CREATE (n:Person {id: 1, email: ['a@x.com', 'b@y.com']})",
        )
        .await
        .expect("list-valued create");
    let db = graphdb_from_ledger(&committed.ledger);

    // Both emails are stored; matching the property yields one row per value.
    let rows = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) RETURN n.email AS email ORDER BY email",
        )
        .await
        .expect("read emails")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let emails: Vec<&str> = rows
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        emails,
        vec!["a@x.com", "b@y.com"],
        "both list elements stored as separate values: {rows}"
    );
}

#[tokio::test]
async fn cypher_create_empty_list_property_stores_nothing() {
    // An empty list property stores no flake (like a null).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:empty-list-prop");
    let committed = fluree
        .transact_cypher(
            ledger0,
            r#"CREATE (n:Person {id: 1, name: "Alice", email: []})"#,
        )
        .await
        .expect("empty-list create");
    let db = graphdb_from_ledger(&committed.ledger);

    let rows = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) WHERE n.email IS NULL RETURN n.name AS name",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rows[0][0],
        json!("Alice"),
        "empty list stored no email: {rows}"
    );
}

#[tokio::test]
async fn cypher_iu8_friendship_with_edge_property() {
    // IU8 (AddFriendship): MATCH two persons, CREATE a KNOWS edge carrying a
    // creationDate property; read the edge property back.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:iu8").await;
    let committed = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {id: 1}), (b:Person {id: 2})
               CREATE (a)-[:KNOWS {creationDate: "2020-01-01"}]->(b)"#,
        )
        .await
        .expect("iu8 friendship create");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person {id: 1})-[k:KNOWS]->(b:Person {id: 2}) RETURN k.creationDate AS cd",
        )
        .await
        .expect("read friendship")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!("2020-01-01"),
        "edge property stored: {out}"
    );
}

#[tokio::test]
async fn cypher_iu1_inline_relationship_with_edge_property() {
    // IU1 (AddPerson) shape: a single CREATE joining new nodes with a typed
    // relationship that carries a property (studyAt classYear).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:iu1-inline-edge");
    let committed = fluree
        .transact_cypher(
            ledger0,
            r"CREATE (p:Person {id: 30})-[:STUDY_AT {classYear: 2011}]->(u:University {id: 40})",
        )
        .await
        .expect("inline edge-prop create");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 30})-[s:STUDY_AT]->(u) RETURN s.classYear AS y",
        )
        .await
        .expect("read studyAt")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(2011), "studyAt classYear stored: {out}");
}

#[tokio::test]
async fn cypher_multi_clause_create_builds_node_then_edges() {
    // IU1 builds a node then links it; verify multiple CREATE clauses in one
    // statement compose (node, node, then the relationship between them).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:multi-create");
    let committed = fluree
        .transact_cypher(
            ledger0,
            r"CREATE (p:Person {id: 10})
               CREATE (u:University {id: 20})
               CREATE (p)-[:STUDY_AT]->(u)",
        )
        .await
        .expect("multi-clause create");
    let db = graphdb_from_ledger(&committed.ledger);

    let rows = fluree
        .query_cypher(
            &db,
            r"MATCH (p:Person {id: 10})-[:STUDY_AT]->(u:University {id: 20}) RETURN u",
        )
        .await
        .expect("read multi-create relationship");
    assert_eq!(
        rows.row_count(),
        1,
        "node-node-edge chain across CREATE clauses"
    );
}

#[tokio::test]
async fn cypher_unwind_batch_list_valued_field() {
    // IU1 documented load shape: one list-of-maps param, an element field that
    // is itself a JSON array (email[]). The node unroller must accept it and
    // store one flake per element.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-list-field");
    let params = json!({
        "people": [
            {"id": 1, "email": ["a@x.com", "b@y.com"]},
            {"id": 2, "email": ["c@z.com"]},
        ]
    });
    let committed = fluree
        .transact_cypher_with_params(
            ledger0,
            "UNWIND $people AS row CREATE (n:Person {id: row.id, email: row.email})",
            params.as_object(),
        )
        .await
        .expect("unwind list-field create");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) RETURN n.email AS email ORDER BY email",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let emails: Vec<&str> = out
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        emails,
        vec!["a@x.com", "b@y.com"],
        "both batch emails stored: {out}"
    );
}

#[tokio::test]
async fn cypher_set_list_valued_property_replaces() {
    // SET n.prop = [...] replaces the multi-valued predicate.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:set-list");
    let l = fluree
        .transact_cypher(
            ledger0,
            r"CREATE (n:Person {id: 1, email: ['old1@x.com', 'old2@x.com']})",
        )
        .await
        .expect("create")
        .ledger;
    let committed = fluree
        .transact_cypher(
            l,
            r"MATCH (n:Person {id: 1}) SET n.email = ['new@x.com', 'also@x.com']",
        )
        .await
        .expect("set list");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) RETURN n.email AS email ORDER BY email",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let emails: Vec<&str> = out
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        emails,
        vec!["also@x.com", "new@x.com"],
        "old emails replaced by the new list: {out}"
    );
}

#[tokio::test]
async fn cypher_set_plus_equals_list_valued_property() {
    // SET n += {prop: [...]} also stores a multi-valued predicate.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:setpluseq-list");
    let l = fluree
        .transact_cypher(ledger0, r#"CREATE (n:Person {id: 1, name: "Alice"})"#)
        .await
        .expect("create")
        .ledger;
    let committed = fluree
        .transact_cypher(
            l,
            r"MATCH (n:Person {id: 1}) SET n += {speaks: ['en', 'fr', 'de']}",
        )
        .await
        .expect("set += list");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) RETURN n.speaks AS s ORDER BY s",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let langs: Vec<&str> = out
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        langs,
        vec!["de", "en", "fr"],
        "all three languages stored: {out}"
    );
}

#[tokio::test]
async fn cypher_merge_on_create_set_list_valued_property() {
    // MERGE ... ON CREATE SET n.prop = [...] stores a multi-valued predicate
    // when the node is created.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:merge-oncreate-list");
    let committed = fluree
        .transact_cypher(
            ledger0,
            r"MERGE (n:Person {id: 1}) ON CREATE SET n.email = ['a@x.com', 'b@y.com']",
        )
        .await
        .expect("merge on create set list");
    let db = graphdb_from_ledger(&committed.ledger);

    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id: 1}) RETURN n.email AS email ORDER BY email",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let emails: Vec<&str> = out
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        emails,
        vec!["a@x.com", "b@y.com"],
        "on-create list stored: {out}"
    );
}

/// Seed Alice KNOWS Bob, Carol, Dave (3 named friends) for list-function tests.
async fn seed_alice_friends(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id": "ex:a", "@type": "ex:Person", "ex:name": "Alice",
                 "ex:KNOWS": [{"@id": "ex:b"}, {"@id": "ex:c"}, {"@id": "ex:d"}]},
                {"@id": "ex:b", "@type": "ex:Person", "ex:name": "Bob"},
                {"@id": "ex:c", "@type": "ex:Person", "ex:name": "Carol"},
                {"@id": "ex:d", "@type": "ex:Person", "ex:name": "Dave"},
            ]}),
        )
        .await
        .expect("seed friends")
        .ledger
}

async fn list_fn_value(fluree: &fluree_db_api::Fluree, ledger_id: &str, query: &str) -> JsonValue {
    let l = seed_alice_friends(fluree, ledger_id).await;
    let db = graphdb_from_ledger(&l);
    let out = fluree
        .query_cypher(&db, query)
        .await
        .expect("list fn query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    out[0][0].clone()
}

#[tokio::test]
async fn cypher_size_of_collect() {
    let fluree = FlureeBuilder::memory().build_memory();
    let v = list_fn_value(
        &fluree,
        "it/cypher:size-collect",
        r#"MATCH (a:Person {name:"Alice"})-[:KNOWS]->(f) RETURN size(collect(f.name)) AS v"#,
    )
    .await;
    assert_eq!(v, json!(3), "Alice has 3 friends: {v}");
}

#[tokio::test]
async fn cypher_head_and_last_of_collect() {
    let fluree = FlureeBuilder::memory().build_memory();
    let h = list_fn_value(
        &fluree,
        "it/cypher:head-collect",
        r#"MATCH (a:Person {name:"Alice"})-[:KNOWS]->(f) RETURN head(collect(f.name)) AS v"#,
    )
    .await;
    assert_eq!(h, json!("Bob"), "first collected name: {h}");

    let last = list_fn_value(
        &fluree,
        "it/cypher:last-collect",
        r#"MATCH (a:Person {name:"Alice"})-[:KNOWS]->(f) RETURN last(collect(f.name)) AS v"#,
    )
    .await;
    assert_eq!(last, json!("Dave"), "last collected name: {last}");
}

#[tokio::test]
async fn cypher_reverse_and_tail_of_collect() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rev = list_fn_value(
        &fluree,
        "it/cypher:reverse-collect",
        r#"MATCH (a:Person {name:"Alice"})-[:KNOWS]->(f) RETURN reverse(collect(f.name)) AS v"#,
    )
    .await;
    assert_eq!(rev, json!(["Dave", "Carol", "Bob"]), "reversed list: {rev}");

    let tail = list_fn_value(
        &fluree,
        "it/cypher:tail-collect",
        r#"MATCH (a:Person {name:"Alice"})-[:KNOWS]->(f) RETURN tail(collect(f.name)) AS v"#,
    )
    .await;
    assert_eq!(tail, json!(["Carol", "Dave"]), "list without head: {tail}");
}

#[tokio::test]
async fn cypher_size_of_string() {
    // size() also works on a string (Cypher's list/string length).
    let fluree = FlureeBuilder::memory().build_memory();
    let v = list_fn_value(
        &fluree,
        "it/cypher:size-string",
        r#"MATCH (a:Person {name:"Alice"}) RETURN size(a.name) AS v"#,
    )
    .await;
    assert_eq!(v, json!(5), "len(\"Alice\") = 5: {v}");
}

#[tokio::test]
async fn cypher_list_literal_expression() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:list-literal").await;
    let db = graphdb_from_ledger(&l);

    // A list literal mixing a node id and name.
    let pair = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person {id:1}) RETURN [n.id, n.name] AS pair",
        )
        .await
        .expect("list literal")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        pair[0][0],
        json!([1, "Alice"]),
        "mixed-type list literal: {pair}"
    );

    // A bare scalar list literal.
    let nums = fluree
        .query_cypher(&db, r"MATCH (n:Person {id:1}) RETURN [1, 2, 3] AS nums")
        .await
        .expect("scalar list literal")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(nums[0][0], json!([1, 2, 3]), "scalar list literal: {nums}");
}

#[tokio::test]
async fn cypher_structured_collect_of_tuples() {
    // IC1's collect tier: collecting per-row tuples into a list of lists.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:struct-collect").await; // ids 1,2,3
    let db = graphdb_from_ledger(&l);

    let pairs = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person) RETURN collect([n.id, n.name]) AS pairs",
        )
        .await
        .expect("structured collect")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        pairs[0][0],
        json!([[1, "Alice"], [2, "Bob"], [3, "Carol"]]),
        "list of [id, name] tuples: {pairs}"
    );
}

#[tokio::test]
async fn cypher_size_of_structured_collect() {
    // List functions compose over a structured collect.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:size-struct-collect").await;
    let db = graphdb_from_ledger(&l);

    let n = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person) RETURN size(collect([n.id, n.name])) AS v",
        )
        .await
        .expect("size of structured collect")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(n[0][0], json!(3), "three tuples collected: {n}");
}

/// Seed a KNOWS chain Alice→Bob→Carol→Dave→Eve where Bob/Carol/Dave/Eve all
/// share fname "Friend" (distances 1..4 from Alice).
async fn seed_ic1_chain(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:fname":"Start","ex:KNOWS":{"@id":"ex:bob"}},
                {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob","ex:fname":"Friend","ex:KNOWS":{"@id":"ex:carol"}},
                {"@id":"ex:carol","@type":"ex:Person","ex:name":"Carol","ex:fname":"Friend","ex:KNOWS":{"@id":"ex:dave"}},
                {"@id":"ex:dave","@type":"ex:Person","ex:name":"Dave","ex:fname":"Friend","ex:KNOWS":{"@id":"ex:eve"}},
                {"@id":"ex:eve","@type":"ex:Person","ex:name":"Eve","ex:fname":"Friend"},
            ]}),
        )
        .await
        .expect("seed ic1 chain")
        .ledger
}

#[tokio::test]
async fn cypher_ic1_distance_ranking() {
    // IC1 core: friends bound by a (non-unique) property, ranked by shortest
    // KNOWS distance within 1..3 hops via length(shortestPath(...)). Eve (4
    // hops) is excluded; ordered by distance.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_ic1_chain(&fluree, "it/cypher:ic1-distance").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (p:Person {name:"Alice"}), (friend:Person {fname:"Friend"})
               WHERE p <> friend
               MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
               RETURN friend.name AS name, length(path) AS distance
               ORDER BY distance ASC, friend.name ASC
               LIMIT 20"#,
        )
        .await
        .expect("ic1 distance ranking")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["Bob", 1], ["Carol", 2], ["Dave", 3]]),
        "friends ranked by shortest distance, Eve (4 hops) excluded: {out}"
    );
}

#[tokio::test]
async fn cypher_order_by_expression_key() {
    // ORDER BY a general expression key (IC1's `toInteger(id)` tiebreaker).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:order-expr");
    let l = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"ex:n1","@type":"ex:Person","ex:sid":"10","ex:name":"A"},
                {"@id":"ex:n2","@type":"ex:Person","ex:sid":"2","ex:name":"B"},
                {"@id":"ex:n3","@type":"ex:Person","ex:sid":"30","ex:name":"C"},
            ]}),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // String ids "10","2","30" sort numerically as 2, 10, 30 via toInteger.
    let out = fluree
        .query_cypher(
            &db,
            r"MATCH (n:Person) RETURN n.name AS name ORDER BY toInteger(n.sid)",
        )
        .await
        .expect("order by toInteger")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let names: Vec<&str> = out
        .as_array()
        .expect("rows")
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        names,
        vec!["B", "A", "C"],
        "numeric id order (2,10,30): {out}"
    );
}

#[tokio::test]
async fn cypher_nodes_of_path_and_range() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_ic1_chain(&fluree, "it/cypher:nodes-range").await; // Alice→Bob→Carol→Dave→Eve
    let db = graphdb_from_ledger(&l);

    // nodes(path) returns the node sequence (as IRIs); a 3-hop path has 4 nodes.
    let ns = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               RETURN nodes(p) AS ns"#,
        )
        .await
        .expect("nodes(path)")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let nodes = ns[0][0].as_array().expect("node list");
    assert_eq!(nodes.len(), 4, "Alice→Bob→Carol→Dave = 4 nodes: {ns}");
    assert_eq!(nodes[0], json!("http://example.org/alice"));
    assert_eq!(nodes[3], json!("http://example.org/dave"));

    // size(nodes(path)) composes.
    let n = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d)) RETURN size(nodes(p)) AS n"#,
        )
        .await
        .expect("size(nodes)")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(n[0][0], json!(4), "node count: {n}");

    // range() builds an inclusive integer list, with an optional step.
    let r = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name:"Alice"}) RETURN range(1, 5) AS r"#,
        )
        .await
        .expect("range")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(r[0][0], json!([1, 2, 3, 4, 5]), "range(1,5): {r}");

    let r2 = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name:"Alice"}) RETURN range(0, 10, 2) AS r"#,
        )
        .await
        .expect("range step")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(r2[0][0], json!([0, 2, 4, 6, 8, 10]), "range(0,10,2): {r2}");
}

#[tokio::test]
async fn cypher_ic14_connection_paths_via_all_shortest() {
    // IC14 core: every shortest connection path between two persons, returned
    // as its node sequence. Diamond graph A→B→D and A→C→D → two 2-hop paths.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic14-paths");
    let l = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"ex:a","@type":"ex:Person","ex:name":"A","ex:KNOWS":[{"@id":"ex:b"},{"@id":"ex:c"}]},
                {"@id":"ex:b","@type":"ex:Person","ex:name":"B","ex:KNOWS":{"@id":"ex:d"}},
                {"@id":"ex:c","@type":"ex:Person","ex:name":"C","ex:KNOWS":{"@id":"ex:d"}},
                {"@id":"ex:d","@type":"ex:Person","ex:name":"D"},
            ]}),
        )
        .await
        .expect("seed diamond")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"}),(d:Person {name:"D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]->(d))
               RETURN nodes(p) AS pathNodes"#,
        )
        .await
        .expect("ic14 connection paths")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = out.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "two shortest connection paths: {out}");
    // Each path has 3 nodes (A, middle, D).
    assert!(
        rows.iter()
            .all(|r| r[0].as_array().map(std::vec::Vec::len) == Some(3)),
        "each path is A→mid→D = 3 nodes: {out}"
    );
}

#[tokio::test]
async fn cypher_unwind_runtime_list() {
    // UNWIND a runtime list expression (not a literal/param list) fans each
    // input row out over the elements.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_ic1_chain(&fluree, "it/cypher:unwind-runtime").await;
    let db = graphdb_from_ledger(&l);

    // UNWIND range(1,3).
    let xs = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name:"Alice"}) UNWIND range(1,3) AS x RETURN x"#,
        )
        .await
        .expect("unwind range")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(xs, json!([[1], [2], [3]]), "unwind range: {xs}");

    // UNWIND a path's nodes, then access a property of each element — the
    // property correlates with the unwound element (one name per node).
    let names = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               UNWIND nodes(p) AS pn
               RETURN pn.name AS nm"#,
        )
        .await
        .expect("unwind path nodes")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let got: Vec<&str> = names
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r[0].as_str())
        .collect();
    assert_eq!(
        got,
        vec!["Alice", "Bob", "Carol", "Dave"],
        "one name per path node: {names}"
    );
}

#[tokio::test]
async fn cypher_alternation_transitive_path() {
    // LDBC IC12 shape: `[:HAS_TYPE|IS_SUBCLASS_OF*0..]` — an alternation inside a
    // transitive path. The closure follows HAS_TYPE once, then IS_SUBCLASS_OF up
    // the class hierarchy. tagA -HAS_TYPE-> tc1 -IS_SUBCLASS_OF-> tc2 -> tcRoot.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:alt-transitive");
    let l = fluree
        .insert(ledger0, &json!({"@context": ctx(), "@graph": [
            {"@id":"ex:tagA","@type":"ex:Tag","ex:name":"A","ex:HAS_TYPE":{"@id":"ex:tc1"}},
            {"@id":"ex:tc1","@type":"ex:TagClass","ex:name":"C1","ex:IS_SUBCLASS_OF":{"@id":"ex:tc2"}},
            {"@id":"ex:tc2","@type":"ex:TagClass","ex:name":"C2","ex:IS_SUBCLASS_OF":{"@id":"ex:tcRoot"}},
            {"@id":"ex:tcRoot","@type":"ex:TagClass","ex:name":"Root"},
        ]}))
        .await
        .expect("seed tag hierarchy")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Every TagClass reachable from tagA via HAS_TYPE-then-IS_SUBCLASS_OF*.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (t:Tag {name:"A"})-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass)
               RETURN base.name AS cls ORDER BY cls"#,
        )
        .await
        .expect("alternation-transitive path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["C1"], ["C2"], ["Root"]]),
        "closure spans both predicates (HAS_TYPE then IS_SUBCLASS_OF*): {out}"
    );

    // A single branch alone cannot reach the class hierarchy: IS_SUBCLASS_OF*
    // from a Tag finds nothing (the first hop is HAS_TYPE, not IS_SUBCLASS_OF).
    let single = fluree
        .query_cypher(
            &db,
            r#"MATCH (t:Tag {name:"A"})-[:IS_SUBCLASS_OF*1..]->(base:TagClass)
               RETURN base.name AS cls ORDER BY cls"#,
        )
        .await
        .expect("single-branch path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        single,
        json!([]),
        "single predicate misses the alternation: {single}"
    );
}

#[tokio::test]
async fn cypher_path_pairs_and_list_indexing() {
    // pathPairs(p) explodes a path into consecutive node pairs; pair[0]/pair[1]
    // index each two-element pair. The building block for IC14 per-edge weight.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_ic1_chain(&fluree, "it/cypher:path-pairs").await; // Alice→Bob→Carol→Dave→Eve
    let db = graphdb_from_ledger(&l);

    // Alice→Bob→Carol→Dave = 3 edges → 3 pairs; index endpoints as IRIs.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               UNWIND pathPairs(p) AS pair
               RETURN pair[0] AS from, pair[1] AS to"#,
        )
        .await
        .expect("path pairs")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([
            ["http://example.org/alice", "http://example.org/bob"],
            ["http://example.org/bob", "http://example.org/carol"],
            ["http://example.org/carol", "http://example.org/dave"],
        ]),
        "consecutive node pairs along the path: {out}"
    );

    // An indexed pair element correlates as a node ref in a downstream property
    // accessor (the IC14 shape: pair[0]/pair[1] become MATCH endpoints).
    let names = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               UNWIND pathPairs(p) AS pair
               WITH pair[0] AS x, pair[1] AS y
               RETURN x.name AS from, y.name AS to"#,
        )
        .await
        .expect("path pair names")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        names,
        json!([["Alice", "Bob"], ["Bob", "Carol"], ["Carol", "Dave"]]),
        "indexed pair elements resolve as node refs: {names}"
    );

    // size(pathPairs(p)) = edge count = 3.
    let n = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               RETURN size(pathPairs(p)) AS n"#,
        )
        .await
        .expect("size pathPairs")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(n[0][0], json!(3), "3 edges → 3 pairs: {n}");

    // Negative index: list[-1] is the last element.
    let last = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name:"Alice"}) RETURN range(10, 40, 10)[-1] AS last"#,
        )
        .await
        .expect("negative index")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(last[0][0], json!(40), "range(10,40,10)[-1] = 40: {last}");
}

#[tokio::test]
async fn cypher_ic14_weighted_paths() {
    // IC14 weighted scoring (Option B): the per-edge `reduce` is decomposed into
    // unwind-pairs → OPTIONAL MATCH interaction → count → sum, grouped by path.
    // The path `p` is carried through the WITH boundaries (a node sequence
    // survives projection) and the final id list is a *terminal* collect grouped
    // by that path — together these sidestep the collect-in-WITH limitation.
    //
    // Diamond A→B→D / A→C→D (two 2-hop paths). Each "message" node m links a
    // sender (SENT_BY) to a receiver (RCVD_BY); the per-pair weight is count(m).
    //   pair (A,B): 2 msgs   pair (B,D): 1 msg   → path A→B→D weight 3
    //   pair (A,C): 0 msgs   pair (C,D): 5 msgs  → path A→C→D weight 5
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic14-weight");
    let mut graph = vec![
        json!({"@id":"ex:a","@type":"ex:Person","ex:name":"A","ex:KNOWS":[{"@id":"ex:b"},{"@id":"ex:c"}]}),
        json!({"@id":"ex:b","@type":"ex:Person","ex:name":"B","ex:KNOWS":{"@id":"ex:d"}}),
        json!({"@id":"ex:c","@type":"ex:Person","ex:name":"C","ex:KNOWS":{"@id":"ex:d"}}),
        json!({"@id":"ex:d","@type":"ex:Person","ex:name":"D"}),
    ];
    // Helper: n messages from `from` to `to`, with globally-unique message ids.
    let add_msgs = |from: &str, to: &str, n: usize, graph: &mut Vec<JsonValue>| {
        for i in 0..n {
            let mid = format!("ex:m_{from}_{to}_{i}");
            graph.push(json!({
                "@id": mid,
                "ex:SENT_BY": {"@id": format!("ex:{from}")},
                "ex:RCVD_BY": {"@id": format!("ex:{to}")},
            }));
        }
    };
    add_msgs("a", "b", 2, &mut graph);
    add_msgs("b", "d", 1, &mut graph);
    add_msgs("c", "d", 5, &mut graph);
    let l = fluree
        .insert(ledger0, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed interaction graph")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"}),(z:Person {name:"D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]->(z))
               UNWIND pathPairs(p) AS pair
               WITH p, pair[0] AS x, pair[1] AS y
               OPTIONAL MATCH (x)<-[:SENT_BY]-(m)-[:RCVD_BY]->(y)
               WITH p, x, y, count(m) AS pairWeight
               WITH p, sum(pairWeight) AS pathWeight
               RETURN pathWeight
               ORDER BY pathWeight DESC"#,
        )
        .await
        .expect("ic14 weight pipeline")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([[5], [3]]),
        "path weights, descending (A→C→D=5, A→B→D=3): {out}"
    );

    // Full IC14 shape: weight AND the per-path person list together.
    let full = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"}),(z:Person {name:"D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]->(z))
               UNWIND pathPairs(p) AS pair
               WITH p, pair[0] AS x, pair[1] AS y
               OPTIONAL MATCH (x)<-[:SENT_BY]-(m)-[:RCVD_BY]->(y)
               WITH p, x, y, count(m) AS pairWeight
               WITH p, sum(pairWeight) AS pathWeight
               UNWIND nodes(p) AS pn
               RETURN pathWeight, collect(pn.name) AS personsInPath
               ORDER BY pathWeight DESC"#,
        )
        .await
        .expect("ic14 full")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        full,
        json!([[5, ["A", "C", "D"]], [3, ["A", "B", "D"]]]),
        "weight + person list per path, descending: {full}"
    );
}

#[tokio::test]
async fn cypher_ic14_faithful_ldbc_weight() {
    // Faithful LDBC SNB IC14: bidirectional KNOWS shortest paths, weighted by
    // reply interactions between path-adjacent persons. A Comment replying to a
    // Post = 1.0; a Comment replying to a Comment = 0.5; both directions count.
    // The four interaction patterns per pair are independent OPTIONAL MATCHes —
    // count(DISTINCT c) avoids the cross-product over-count between them.
    //
    // KNOWS diamond (undirected): p0-p1-p3 and p0-p2-p3.
    //   (p0,p1): p0's Comment replies to p1's Post                  → 1.0
    //   (p1,p3): p1's Comment replies to p3's Comment (0.5) AND
    //            p3's Comment (base_p3) replies to p1's Post (1.0)  → 1.5
    //     path p0-p1-p3 weight = 2.5  → ranks first (bidirectional pair)
    //   (p0,p2): none                                               → 0.0
    //   (p2,p3): two of p3's Comments reply to p2's Posts           → 2.0
    //     path p0-p2-p3 weight = 2.0
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic14-faithful");
    let person = |id: &str, knows: JsonValue| json!({"@id": format!("ex:{id}"), "@type":"ex:Person", "ex:pid": id, "ex:KNOWS": knows});
    // Comment `c` by `creator` replying to message `target`.
    let comment = |c: &str, creator: &str, target: &str| {
        json!({"@id": format!("ex:{c}"), "@type":"ex:Comment",
               "ex:HAS_CREATOR":{"@id":format!("ex:{creator}")},
               "ex:REPLY_OF":{"@id":format!("ex:{target}")}})
    };
    let message = |m: &str, ty: &str, creator: &str| {
        json!({"@id": format!("ex:{m}"), "@type": format!("ex:{ty}"),
               "ex:HAS_CREATOR":{"@id":format!("ex:{creator}")}})
    };
    let graph = json!([
        person("p0", json!([{"@id":"ex:p1"},{"@id":"ex:p2"}])),
        person("p1", json!([{"@id":"ex:p3"}])),
        person("p2", json!([{"@id":"ex:p3"}])),
        person("p3", json!([])),
        // (p0,p1): p0 comment → p1 post  (1.0)
        message("post_p1", "Post", "p1"),
        comment("c_p0", "p0", "post_p1"),
        // (p1,p3): p1 comment → p3 comment (0.5)
        comment("base_p3", "p3", "post_p1"),
        comment("c_p1", "p1", "base_p3"),
        // (p2,p3): two of p3's comments → p2's posts (2.0)
        message("post_p2a", "Post", "p2"),
        message("post_p2b", "Post", "p2"),
        comment("c_p3a", "p3", "post_p2a"),
        comment("c_p3b", "p3", "post_p2b"),
    ]);
    let l = fluree
        .insert(ledger0, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed ldbc-ish graph")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {pid:"p0"}),(z:Person {pid:"p3"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]-(z))
               UNWIND pathPairs(p) AS pair
               WITH p, pair[0] AS x, pair[1] AS y
               OPTIONAL MATCH (x)<-[:HAS_CREATOR]-(cp1:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(y)
               OPTIONAL MATCH (x)<-[:HAS_CREATOR]-(cc1:Comment)-[:REPLY_OF]->(:Comment)-[:HAS_CREATOR]->(y)
               OPTIONAL MATCH (y)<-[:HAS_CREATOR]-(cp2:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(x)
               OPTIONAL MATCH (y)<-[:HAS_CREATOR]-(cc2:Comment)-[:REPLY_OF]->(:Comment)-[:HAS_CREATOR]->(x)
               WITH p, x, y,
                    count(DISTINCT cp1) * 1.0 + count(DISTINCT cc1) * 0.5 +
                    count(DISTINCT cp2) * 1.0 + count(DISTINCT cc2) * 0.5 AS pairWeight
               WITH p, sum(pairWeight) AS pathWeight
               UNWIND nodes(p) AS pn
               RETURN collect(pn.pid) AS personIdsInPath, pathWeight
               ORDER BY pathWeight DESC"#,
        )
        .await
        .expect("ic14 faithful")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([[["p0", "p1", "p3"], 2.5], [["p0", "p2", "p3"], 2.0],]),
        "LDBC IC14 weighted paths, descending: {out}"
    );
}

#[tokio::test]
async fn cypher_ic14_equal_weight_paths_stay_separate() {
    // Regression: when two distinct shortest paths score the SAME pathWeight,
    // the final `collect(pn.id)` must NOT merge them. Grouping by `pathWeight`
    // alone (the only non-aggregate key) fuses their node lists into one
    // concatenated row. Projecting the path `p` as an extra grouping key keeps
    // them separate. This is the shape validated against the real LDBC golden;
    // a distinct-weight fixture (cypher_ic14_faithful_ldbc_weight) can't catch
    // the fusion because the weights already separate the rows.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic14-equal-weight");
    let person = |id: &str, knows: JsonValue| json!({"@id": format!("ex:{id}"), "@type":"ex:Person", "ex:pid": id, "ex:KNOWS": knows});
    let comment = |c: &str, creator: &str, target: &str| {
        json!({"@id": format!("ex:{c}"), "@type":"ex:Comment",
               "ex:HAS_CREATOR":{"@id":format!("ex:{creator}")},
               "ex:REPLY_OF":{"@id":format!("ex:{target}")}})
    };
    let post = |m: &str, creator: &str| {
        json!({"@id": format!("ex:{m}"), "@type":"ex:Post",
               "ex:HAS_CREATOR":{"@id":format!("ex:{creator}")}})
    };
    // Diamond p0-p1-p3 / p0-p2-p3; each route scores exactly 1.0:
    //   (p0,p1): p0 comment → p1 post (1.0); (p1,p3): none  → path 1.0
    //   (p2,p3): p2 comment → p3 post (1.0); (p0,p2): none  → path 1.0
    let graph = json!([
        person("p0", json!([{"@id":"ex:p1"},{"@id":"ex:p2"}])),
        person("p1", json!([{"@id":"ex:p3"}])),
        person("p2", json!([{"@id":"ex:p3"}])),
        person("p3", json!([])),
        post("post_p1", "p1"),
        comment("c_p0", "p0", "post_p1"),
        post("post_p3", "p3"),
        comment("c_p2", "p2", "post_p3"),
    ]);
    let l = fluree
        .insert(ledger0, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed equal-weight diamond")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {pid:"p0"}),(z:Person {pid:"p3"})
               MATCH p = allShortestPaths((a)-[:KNOWS*0..]-(z))
               UNWIND pathPairs(p) AS pair
               WITH p, pair[0] AS x, pair[1] AS y
               OPTIONAL MATCH (x)<-[:HAS_CREATOR]-(cp1:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(y)
               OPTIONAL MATCH (y)<-[:HAS_CREATOR]-(cp2:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(x)
               WITH p, x, y, count(DISTINCT cp1) * 1.0 + count(DISTINCT cp2) * 1.0 AS pairWeight
               WITH p, sum(pairWeight) AS pathWeight
               UNWIND nodes(p) AS pn
               RETURN collect(pn.pid) AS personIdsInPath, pathWeight, p
               ORDER BY pathWeight DESC"#,
        )
        .await
        .expect("ic14 equal-weight")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    // Two separate 3-node paths, each weight 1.0 — NOT one fused 6-node row.
    let mut paths: Vec<Vec<String>> = out
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            assert_eq!(r[1], json!(1.0), "each path weight 1.0: {out}");
            r[0].as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap().to_string())
                .collect()
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            vec!["p0".to_string(), "p1".to_string(), "p3".to_string()],
            vec!["p0".to_string(), "p2".to_string(), "p3".to_string()],
        ],
        "equal-weight paths stay separate, not fused: {out}"
    );
}

#[tokio::test]
async fn cypher_ic14_paths_as_name_lists() {
    // IC14 core, full form: every shortest connection path between two persons,
    // returned as a list of the persons' names — `UNWIND nodes(p)` + per-path
    // `collect`, grouped by the path. Diamond A→B→D / A→C→D → two paths.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic14-name-lists");
    let l = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"ex:a","@type":"ex:Person","ex:name":"A","ex:KNOWS":[{"@id":"ex:b"},{"@id":"ex:c"}]},
                {"@id":"ex:b","@type":"ex:Person","ex:name":"B","ex:KNOWS":{"@id":"ex:d"}},
                {"@id":"ex:c","@type":"ex:Person","ex:name":"C","ex:KNOWS":{"@id":"ex:d"}},
                {"@id":"ex:d","@type":"ex:Person","ex:name":"D"},
            ]}),
        )
        .await
        .expect("seed diamond")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"}),(z:Person {name:"D"})
               MATCH p = allShortestPaths((a)-[:KNOWS*]->(z))
               UNWIND nodes(p) AS pn
               RETURN p, collect(pn.name) AS path_names"#,
        )
        .await
        .expect("ic14 name lists")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    // One row per shortest path; the name list is the second projected column.
    let mut lists: Vec<Vec<String>> = out
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            r[1].as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap().to_string())
                .collect()
        })
        .collect();
    lists.sort();
    assert_eq!(
        lists,
        vec![
            vec!["A".to_string(), "B".to_string(), "D".to_string()],
            vec!["A".to_string(), "C".to_string(), "D".to_string()],
        ],
        "two shortest paths, each as its person-name list: {out}"
    );
}

#[tokio::test]
async fn cypher_unwind_single_path_collect() {
    // A single shortest path collected into one name list (implicit aggregation).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_ic1_chain(&fluree, "it/cypher:unwind-single").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"Alice"}),(d:Person {name:"Dave"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(d))
               UNWIND nodes(p) AS pn
               RETURN collect(pn.name) AS path_names"#,
        )
        .await
        .expect("single-path collect")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(["Alice", "Bob", "Carol", "Dave"]),
        "the path as a name list: {out}"
    );
}

#[tokio::test]
async fn cypher_var_length_relationship_uniqueness_allows_cycle_closure() {
    // Directed triangle A→B→C→A. A 3-hop path back to A (A-B-C-A) reuses no
    // edge, so relationship-uniqueness allows it (Neo4j parity) — node-
    // uniqueness wrongly excluded it (revisits node A).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:triangle");
    let l = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"ex:a","@type":"ex:Person","ex:name":"A","ex:KNOWS":{"@id":"ex:b"}},
                {"@id":"ex:b","@type":"ex:Person","ex:name":"B","ex:KNOWS":{"@id":"ex:c"}},
                {"@id":"ex:c","@type":"ex:Person","ex:name":"C","ex:KNOWS":{"@id":"ex:a"}},
            ]}),
        )
        .await
        .expect("seed triangle")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // A 3-hop directed cycle returns A to itself.
    let cycle = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"})-[:KNOWS*3..3]->(a) RETURN a.name AS n"#,
        )
        .await
        .expect("cycle closure")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        cycle[0][0],
        json!("A"),
        "3-hop cycle A-B-C-A closes: {cycle}"
    );

    // But a 2-hop out-and-back over one edge reuses that edge → excluded.
    let back = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name:"A"})-[:KNOWS*2..2]-(a) RETURN a.name AS n"#,
        )
        .await
        .expect("out-and-back");
    assert_eq!(
        back.row_count(),
        0,
        "2-hop out-and-back reuses an edge → excluded"
    );
}

#[tokio::test]
async fn cypher_with_limit_then_match_truncates_and_drives_downstream() {
    // Regression: a non-final `WITH … LIMIT` (the canonical "top-N then expand"
    // pattern, LDBC IS2) used to silently break the following MATCH. The limited
    // WITH lowers to a subquery; the trailing MATCH re-produces the WITH's output
    // var, which `subquery_correlation_vars` mis-read as an external correlation
    // (a slice empties `self_produced`), deferring the WITH behind its own
    // consumer — so the MATCH ran first as an unseeded scan: empty results, or an
    // ignored limit. The fix restricts correlation inputs to PRECEDING siblings.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:with-limit-match");

    // hub KNOWS m1,m2,m3 ; each mN KNOWS exactly one xN.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:hub", "@type": "ex:Person", "ex:id": 0,
             "ex:KNOWS": [{"@id": "ex:m1"}, {"@id": "ex:m2"}, {"@id": "ex:m3"}]},
            {"@id": "ex:m1", "@type": "ex:Person", "ex:id": 1, "ex:KNOWS": {"@id": "ex:x1"}},
            {"@id": "ex:m2", "@type": "ex:Person", "ex:id": 2, "ex:KNOWS": {"@id": "ex:x2"}},
            {"@id": "ex:m3", "@type": "ex:Person", "ex:id": 3, "ex:KNOWS": {"@id": "ex:x3"}},
            {"@id": "ex:x1", "@type": "ex:Person", "ex:id": 11},
            {"@id": "ex:x2", "@type": "ex:Person", "ex:id": 12},
            {"@id": "ex:x3", "@type": "ex:Person", "ex:id": 13},
        ]
    });
    let l = fluree.insert(ledger0, &txn).await.expect("seed").ledger;
    let db = graphdb_from_ledger(&l);

    let rows = |q: &'static str| {
        let fluree = &fluree;
        let db = &db;
        async move {
            fluree
                .query_cypher(db, q)
                .await
                .expect("cypher")
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld")
        }
    };

    // Baseline (no limit): all three m's expand → 3 rows.
    let base = rows(
        "MATCH (:Person {id:0})-[:KNOWS]->(m) WITH m \
         MATCH (m)-[:KNOWS]->(x) RETURN m.id AS mid, x.id AS xid ORDER BY mid",
    )
    .await;
    assert_eq!(
        base,
        json!([[1, 11], [2, 12], [3, 13]]),
        "no-limit baseline expands every friend: {base}"
    );

    // ORDER BY + LIMIT 2: the two smallest-id friends drive the downstream MATCH.
    let limited = rows(
        "MATCH (:Person {id:0})-[:KNOWS]->(m) WITH m ORDER BY m.id LIMIT 2 \
         MATCH (m)-[:KNOWS]->(x) RETURN m.id AS mid, x.id AS xid ORDER BY mid",
    )
    .await;
    assert_eq!(
        limited,
        json!([[1, 11], [2, 12]]),
        "WITH ORDER BY LIMIT 2 truncates before the second MATCH: {limited}"
    );

    // Plain LIMIT (no ORDER BY): the limit still truncates to at most 2 driving
    // m's, and each row's downstream x is that m's real edge (m.id+10).
    let plain = rows(
        "MATCH (:Person {id:0})-[:KNOWS]->(m) WITH m LIMIT 2 \
         MATCH (m)-[:KNOWS]->(x) RETURN m.id AS mid, x.id AS xid",
    )
    .await;
    let plain_rows = plain.as_array().expect("rows");
    assert_eq!(plain_rows.len(), 2, "plain LIMIT 2 yields 2 rows: {plain}");
    for row in plain_rows {
        assert_eq!(
            row[1].as_i64().expect("xid"),
            row[0].as_i64().expect("mid") + 10,
            "each driven m joins to its own edge: {plain}"
        );
    }
}

#[tokio::test]
async fn cypher_var_length_then_with_distinct_multivar_drives_downstream() {
    // Regression (LDBC IC6): a variable-length traversal feeding a multi-var
    // `WITH DISTINCT friend, knownTag` whose outputs are consumed by a later
    // self-join under-counted (4 → 1). The reorder placed the cheap consuming
    // triples ahead of the var-length WITH (its cost estimate is high), turning
    // an uncorrelated producer into a per-row correlated subquery over its own
    // consumer and collapsing the consumer's bindings. The fix defers a consumer
    // of an uncorrelated subquery's output vars until after the subquery.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:ic6-var-length-with");

    // hub(0) reaches friend(100) via 3 KNOWS*1..2 paths (direct + via 1 + via 2).
    // friend authored 4 posts, each tagged Knot AND DavidFoster.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:p0", "@type": "ex:Person", "ex:id": 0,
             "ex:KNOWS": [{"@id": "ex:p100"}, {"@id": "ex:p1"}, {"@id": "ex:p2"}]},
            {"@id": "ex:p1", "@type": "ex:Person", "ex:id": 1, "ex:KNOWS": {"@id": "ex:p100"}},
            {"@id": "ex:p2", "@type": "ex:Person", "ex:id": 2, "ex:KNOWS": {"@id": "ex:p100"}},
            {"@id": "ex:p100", "@type": "ex:Person", "ex:id": 100},
            {"@id": "ex:tKnot", "@type": "ex:Tag", "ex:name": "Knot"},
            {"@id": "ex:tDF", "@type": "ex:Tag", "ex:name": "DavidFoster"},
            {"@id": "ex:m0", "@type": "ex:Post", "ex:id": 1000,
             "ex:HAS_CREATOR": {"@id": "ex:p100"}, "ex:HAS_TAG": [{"@id": "ex:tKnot"}, {"@id": "ex:tDF"}]},
            {"@id": "ex:m1", "@type": "ex:Post", "ex:id": 1001,
             "ex:HAS_CREATOR": {"@id": "ex:p100"}, "ex:HAS_TAG": [{"@id": "ex:tKnot"}, {"@id": "ex:tDF"}]},
            {"@id": "ex:m2", "@type": "ex:Post", "ex:id": 1002,
             "ex:HAS_CREATOR": {"@id": "ex:p100"}, "ex:HAS_TAG": [{"@id": "ex:tKnot"}, {"@id": "ex:tDF"}]},
            {"@id": "ex:m3", "@type": "ex:Post", "ex:id": 1003,
             "ex:HAS_CREATOR": {"@id": "ex:p100"}, "ex:HAS_TAG": [{"@id": "ex:tKnot"}, {"@id": "ex:tDF"}]},
        ]
    });
    let l = fluree.insert(ledger0, &txn).await.expect("seed").ledger;
    let db = graphdb_from_ledger(&l);

    let jsonld = fluree
        .query_cypher(
            &db,
            r#"MATCH (knownTag:Tag {name: "Knot"})
               MATCH (person:Person {id: 0})-[:KNOWS*1..2]-(friend) WHERE NOT friend = person
               WITH DISTINCT friend, knownTag
               MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(knownTag)
               MATCH (post)-[:HAS_TAG]->(commonTag) WHERE NOT commonTag = knownTag
               RETURN commonTag.name AS name, count(post) AS cnt ORDER BY cnt DESC, name"#,
        )
        .await
        .expect("ic6")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");

    // All 4 of friend's Knot-tagged posts also carry DavidFoster → count is 4.
    assert_eq!(
        jsonld,
        json!([["DavidFoster", 4]]),
        "var-length WITH output must drive the downstream count: {jsonld}"
    );
}
