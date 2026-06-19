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

// Known gap: the bare-DELETE guard probes a node's relationships via an
// untyped rel-var pattern `(n)-[r]->()`, which queries the `f:reifies*`
// sidecar — now hidden by the tightened edge-annotation read-side firewall, so
// the guard finds no relationship and allows the delete. Tracked for a focused
// fix in the per-edge-annotation probe path.
#[ignore = "bare-DELETE relationship guard vs edge-annotation firewall — tracked separately"]
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
async fn cypher_with_collect_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:with-collect").await;
    let db = graphdb_from_ledger(&l);

    let err = fluree
        .query_cypher(
            &db,
            r"MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b) AS bs RETURN a, bs",
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
