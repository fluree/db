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
use support::{genesis_ledger, graphdb_from_ledger, rebuild_and_publish_index};

fn ctx() -> JsonValue {
    json!({
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
        "@id": "alice",
        "@type": "Person",
        "name": "Alice",
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");

    // Bare names on both surfaces: with no `@vocab` configured, the
    // JSON-LD `@type` `Person` and the Cypher label `Person` are the
    // same namespace-0 name.
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
async fn cypher_untyped_single_hop_excludes_labels_and_data_properties() {
    // `-->` must follow only relationships: not `rdf:type` (the class node is
    // not a neighbor) and not data properties (literals are not nodes).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:untyped-hop-edge-set");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {
                        "@id": "alice",
                        "@type": "Person",
                        "name": "Alice",
                        "knows": {"@id": "bob"}
                    },
                    {"@id": "bob", "@type": "Person", "name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let jsonld = fluree
        .query_cypher(&db, r#"MATCH (n:Person {name: "Alice"})-->(m) RETURN m"#)
        .await
        .expect("untyped hop query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");

    let rows = jsonld.as_array().expect("rows");
    assert_eq!(
        rows.len(),
        1,
        "only the knows edge is a relationship: {jsonld}"
    );
    assert_eq!(rows[0][0].as_str(), Some("bob"), "{jsonld}");
}

#[tokio::test]
async fn cypher_untyped_undirected_hop_excludes_labels_and_data_properties() {
    // Same edge-set rule for the undirected `--` (forward ∪ reverse union).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:untyped-undirected-edge-set");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {
                        "@id": "alice",
                        "@type": "Person",
                        "name": "Alice",
                        "knows": {"@id": "bob"}
                    },
                    {
                        "@id": "carol",
                        "@type": "Person",
                        "name": "Carol",
                        "knows": {"@id": "alice"}
                    },
                    {"@id": "bob", "@type": "Person", "name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let jsonld = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person {name: "Alice"})--(m) RETURN m ORDER BY m"#,
        )
        .await
        .expect("untyped undirected hop query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");

    let rows = jsonld.as_array().expect("rows");
    let neighbors: Vec<&str> = rows.iter().filter_map(|r| r[0].as_str()).collect();
    assert_eq!(
        neighbors,
        ["bob", "carol"],
        "both edge orientations, no class node / literals: {jsonld}"
    );
}

#[tokio::test]
async fn cypher_value_only_rel_var_matches_unreified_edges() {
    // A bound relationship variable used only for its value surface
    // (`RETURN e`, `type(e)`) must match plain-RDF edges that carry no
    // `f:reifies*` bundle (typed-pattern matches on imported data).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:value-only-rel-var");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {
                        "@id": "alice",
                        "@type": "Person",
                        "name": "Alice",
                        "knows": {"@id": "bob"}
                    },
                    {"@id": "bob", "@type": "Person", "name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // Typed bound rel var.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[e:knows]->(b) RETURN type(e) AS t, b"#,
        )
        .await
        .expect("typed rel var over plain triple")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "plain edge must match: {cj}");
    assert_eq!(data[0]["row"][0], json!("knows"), "type(e): {cj}");

    // Untyped bound rel var: same edge-set rule as `-->` (no rdf:type /
    // data properties), and the synthesized value still answers type(e).
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[e]->(b) RETURN type(e) AS t"#,
        )
        .await
        .expect("untyped rel var over plain triple")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let data = cj["results"][0]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "only knows is a relationship: {cj}");
    assert_eq!(data[0]["row"][0], json!("knows"), "type(e): {cj}");
}

#[tokio::test]
async fn cypher_value_only_rel_var_keeps_parallel_reified_edges_distinct() {
    // Reified parallel edges (Cypher-created) share one base triple; a
    // value-only rel var must still yield one row per relationship.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:value-only-parallel");
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
            .expect("parallel edges")
            .row_count(),
        2,
        "one row per reified relationship"
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
            {"@id": "alice", "@type": "Person", "age": 25},
            {"@id": "bob",   "@type": "Person", "age": 35},
            {"@id": "carol", "@type": "Person", "age": 45},
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
            {"@id": "alice", "@type": "Person", "age": 25},
            {"@id": "bob",   "@type": "Person"},
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
async fn cypher_var_length_unbounded_bound_relationship_variable_enumerates() {
    // Binding a variable to an UNBOUNDED variable-length relationship
    // enumerates paths under relationship-uniqueness (Enumerate mode). On an
    // empty ledger the pattern matches nothing — the point is it no longer rejects.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:varlen-bound");
    let db = graphdb_from_ledger(&ledger0);

    let r = fluree
        .query_cypher(&db, "MATCH (a:Person)-[r:KNOWS*]->(b) RETURN b")
        .await
        .expect("unbounded rel binding is supported");
    assert_eq!(r.row_count(), 0, "empty ledger has no paths");
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
        "@id": "alice", "@type": "Person", "name": "Alice", "age": 25,
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
                        "@id": "alice",
                        "@type": "Person",
                        "name": "Alice",
                        "age": 25,
                        "KNOWS": {"@id": "bob"}
                    },
                    {
                        "@id": "bob",
                        "@type": "Person",
                        "name": "Bob",
                        "age": 35
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
                    {"@id": "alice", "@type": "Person", "name": "Alice", "age": 25},
                    {"@id": "bob",   "@type": "Person", "name": "Bob",   "age": 35},
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
                    {"@id": "alice", "@type": "Person", "name": "Alice", "age": 25},
                    {"@id": "bob",   "@type": "Person", "name": "Bob"},
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
            {"@id": "alice", "@type": "Person", "name": "Alice"},
            {"@id": "bob",   "@type": "Person", "name": "Bob"},
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
                    {"@id": "alice", "@type": "Person", "name": "Alice"},
                    {"@id": "bob",   "@type": "Person", "name": "Bob"},
                    {"@id": "eve",   "@type": "Person", "name": "Eve"},
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
        "@id": "alice", "@type": "Person", "name": "Alice",
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
        "@id": "alice", "@type": "Person", "name": "Alice",
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
        "@id": "alice", "@type": "Person", "name": "Alice", "age": 25,
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
        "@id": "alice", "@type": "Person", "name": "Alice", "age": 25,
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
            {"@id": "alice", "@type": "Person", "name": "Alice"},
            {"@id": "bob",   "@type": "Person", "name": "Bob"},
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
async fn transact_cypher_unwind_inline_range_batches_node_inserts() {
    // Inline constant UNWIND source on a write (`UNWIND range(1, 100) AS x`)
    // — desugars through the same path as `UNWIND $list`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-inline-range");

    let result = fluree
        .transact_cypher(
            ledger0,
            r#"UNWIND range(1, 100) AS x CREATE (n:L1:L2 {p1: true, p5: x})"#,
        )
        .await
        .expect("inline range batched insert");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:L1) RETURN n")
            .await
            .expect("count")
            .row_count(),
        100,
        "one node per range element"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:L2) WHERE n.p5 = 42 RETURN n")
            .await
            .expect("p5")
            .row_count(),
        1,
        "each node carries its own range value"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_inline_list_batches_node_inserts() {
    // Inline list-literal UNWIND on a write.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:unwind-inline-list");

    let result = fluree
        .transact_cypher(
            ledger0,
            r#"UNWIND ["Alice", "Bob", "Carol"] AS name CREATE (n:Person {name: name})"#,
        )
        .await
        .expect("inline list batched insert");

    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("count")
            .row_count(),
        3
    );
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {name: "Bob"}) RETURN n"#)
            .await
            .expect("bob")
            .row_count(),
        1
    );
}

#[tokio::test]
async fn transact_cypher_create_return_node() {
    // `CREATE (n:UserTemp {id: 1}) RETURN n` (single vertex write with
    // RETURN) — one row carrying the created node id.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:create-return-node");
    let (result, rows) = fluree
        .transact_cypher_returning(ledger0, r#"CREATE (n:UserTemp {id: 1}) RETURN n"#, None)
        .await
        .expect("create with RETURN");
    let rows = rows.expect("RETURN produces rows");
    assert_eq!(rows["results"][0]["columns"], json!(["n"]), "{rows}");
    let data = rows["results"][0]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "{rows}");
    let id = data[0]["row"][0].as_str().expect("node id string");
    assert!(id.starts_with("_:fdb-"), "skolemized node id: {rows}");

    // The returned identity is the committed node: it must have the label.
    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:UserTemp) RETURN n")
            .await
            .expect("verify")
            .row_count(),
        1
    );
}

#[tokio::test]
async fn transact_cypher_match_create_return_edge() {
    // `MATCH (a),(b) CREATE (a)-[e:Temp]->(b) RETURN e` (single edge write
    // with RETURN) — one row per WHERE solution.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:create-return-edge");
    for stmt in [
        r#"CREATE (a:User {id: 1})"#,
        r#"CREATE (b:User {id: 2})"#,
        r#"CREATE (c:User {id: 3})"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }

    let (result, rows) = fluree
        .transact_cypher_returning(
            l,
            r#"MATCH (a:User {id: 1}), (b:User {id: 2}) CREATE (a)-[e:Temp]->(b) RETURN e"#,
            None,
        )
        .await
        .expect("edge create with RETURN");
    let rows = rows.expect("RETURN produces rows");
    assert_eq!(rows["results"][0]["columns"], json!(["e"]), "{rows}");
    let data = rows["results"][0]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "one solution → one created edge: {rows}");
    assert!(
        data[0]["row"][0]
            .as_str()
            .expect("edge id")
            .contains("cy_rel_e"),
        "{rows}"
    );

    // Multi-solution: a broader MATCH creates one edge per pair and RETURN
    // reports each.
    let (_, rows) = fluree
        .transact_cypher_returning(
            result.ledger,
            r#"MATCH (a:User {id: 3}), (b:User) CREATE (a)-[e:Linked]->(b) RETURN e AS edge"#,
            None,
        )
        .await
        .expect("batch edge create with RETURN");
    let rows = rows.expect("rows");
    assert_eq!(rows["results"][0]["columns"], json!(["edge"]), "{rows}");
    assert_eq!(
        rows["results"][0]["data"].as_array().expect("data").len(),
        3,
        "three matched targets → three created edges: {rows}"
    );
}

#[tokio::test]
async fn transact_cypher_return_of_matched_var_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:write-return-matched");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:User {id: 1})"#)
        .await
        .expect("seed")
        .ledger;
    let err = fluree
        .transact_cypher_returning(l, r#"MATCH (a:User {id: 1}) SET a.x = 1 RETURN a"#, None)
        .await
        .expect_err("RETURN of a MATCH-bound var must be rejected");
    assert!(format!("{err}").contains("created"), "{err}");
}

#[tokio::test]
async fn transact_cypher_create_bare_anonymous_node() {
    // `CREATE ()` (anonymous vertex create) — an anonymous propertyless
    // node commits (via the hidden db:Node marker) and stays invisible to
    // labeled matches.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:create-bare");
    let result = fluree
        .transact_cypher(ledger0, "CREATE ()")
        .await
        .expect("bare CREATE ()");
    assert!(result.receipt.t >= 1, "committed");

    // `CREATE ()-[:TempEdge]->()` (anonymous pattern create).
    let result = fluree
        .transact_cypher(result.ledger, "CREATE ()-[:TempEdge]->()")
        .await
        .expect("bare CREATE pattern");
    let db = graphdb_from_ledger(&result.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (a)-[:TempEdge]->(b) RETURN a, b")
            .await
            .expect("edge query")
            .row_count(),
        1,
        "anonymous endpoints exist via the edge"
    );
}

#[tokio::test]
async fn transact_cypher_create_var_only_node_has_empty_labels() {
    // `CREATE (n)` — fresh node, no labels/props; labels(n) must hide the
    // db:Node existence marker.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:create-var-only");
    let l = fluree
        .transact_cypher(ledger0, "CREATE (n)")
        .await
        .expect("CREATE (n)")
        .ledger;
    let l = fluree
        .transact_cypher(l, r#"CREATE (m:Person {name: "Alice"})"#)
        .await
        .expect("labeled sibling")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // The labeled match must not see the bare node.
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .expect("person")
            .row_count(),
        1
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
                    {"@id": "n1", "@type": "Person", "id": 1, "name": "Alice"},
                    {"@id": "n2", "@type": "Person", "id": 2, "name": "Bob"},
                    {"@id": "n3", "@type": "Person", "id": 3, "name": "Carol"},
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
                    {"@id": "alice", "@type": "Person", "name": "Alice"},
                    {
                        "@id": "bob",
                        "@type": ["Person", "Employee"],
                        "name": "Bob"
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
                    {"@id": "alice", "@type": "Person", "name": "Alice"},
                    {"@id": "bob",   "@type": "Person", "name": "Bob"},
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
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
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
                "@id": format!("p{i}"),
                "@type": "Person",
                "name": format!("P{i:02}"),
                "KNOWS": [
                    {"@id": format!("p{}", (i + 1) % n)},
                    {"@id": format!("p{}", (i + 2) % n)},
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
            &json!({"@context": ctx(), "@id": "zoe", "@type": "Person", "name": "Zoe"}),
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
        json!("zoe"),
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
        "@id": "alice",
        "@type": "Person",
        "greeting": {"@value": "Bonjour", "@language": "fr"},
        "tags": {"@list": ["x", "y", "z"]},
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
async fn transact_cypher_merge_trailing_set_applies_on_both_branches() {
    // The upsert idiom: `MERGE (n {key}) SET …` — the SET runs after the
    // MERGE on the create AND the match branch.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-trailing-set");

    // First run: node absent → created, trailing SET applies.
    let l = fluree
        .transact_cypher(l, r#"MERGE (n:Person {name: "Carol"}) SET n.seen = 1"#)
        .await
        .expect("merge+set create branch")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {name: "Carol", seen: 1}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "trailing SET applied on the create branch"
    );

    // Second run: node present → matched, trailing SET overwrites.
    let l = fluree
        .transact_cypher(l, r#"MERGE (n:Person {name: "Carol"}) SET n.seen = 2"#)
        .await
        .expect("merge+set match branch")
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
            .query_cypher(&db, r#"MATCH (n:Person {seen: 2}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        1,
        "trailing SET applied on the match branch"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:Person {seen: 1}) RETURN n"#)
            .await
            .unwrap()
            .row_count(),
        0,
        "old value was retracted"
    );
}

#[tokio::test]
async fn transact_cypher_merge_on_create_and_trailing_set_combine() {
    // ON CREATE SET fires only on create; the trailing SET fires on both runs.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-oncreate-trailing");

    let l = fluree
        .transact_cypher(
            l,
            r#"MERGE (n:Item {sku: "a1"}) ON CREATE SET n.origin = "created" SET n.stamp = 7"#,
        )
        .await
        .expect("first run")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (n:Item {origin: "created", stamp: 7}) RETURN n"#
            )
            .await
            .unwrap()
            .row_count(),
        1,
        "both ON CREATE SET and trailing SET applied on create"
    );

    let l = fluree
        .transact_cypher(
            l,
            r#"MERGE (n:Item {sku: "a1"}) ON CREATE SET n.origin = "recreated" SET n.stamp = 9"#,
        )
        .await
        .expect("second run")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (n:Item {origin: "created", stamp: 9}) RETURN n"#
            )
            .await
            .unwrap()
            .row_count(),
        1,
        "trailing SET updated stamp; ON CREATE SET did not re-fire"
    );
}

#[tokio::test]
async fn transact_cypher_merge_trailing_set_map_merge_with_params() {
    // The canonical ETL statement: MERGE (n {key: $k}) SET n += $props.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-set-map");

    let params = json!({ "id": 42, "props": { "name": "Eve", "age": 30 } });
    let l = fluree
        .transact_cypher_with_params(
            l,
            "MERGE (n:User {id: $id}) SET n += $props",
            params.as_object(),
        )
        .await
        .expect("upsert create branch")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (n:User {id: 42, name: "Eve", age: 30}) RETURN n"#
            )
            .await
            .unwrap()
            .row_count(),
        1,
        "map merged into the created node"
    );

    // Re-run with changed props: same node, values updated.
    let params = json!({ "id": 42, "props": { "name": "Eve", "age": 31 } });
    let l = fluree
        .transact_cypher_with_params(
            l,
            "MERGE (n:User {id: $id}) SET n += $props",
            params.as_object(),
        )
        .await
        .expect("upsert match branch")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:User) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1,
        "no duplicate node"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:User {age: 31}) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1,
        "map merge updated the property on the match branch"
    );
}

#[tokio::test]
async fn transact_cypher_match_set_map_merge_param() {
    // `SET n += $map` after a plain MATCH (the same whole-map param the MERGE
    // upsert uses, through the ordinary MATCH … SET lowering).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:match-set-map-param");
    let l = fluree
        .transact_cypher(l, r#"CREATE (n:User {id: 7, name: "Ann"})"#)
        .await
        .expect("seed")
        .ledger;

    let params = json!({ "props": { "name": "Anne", "city": "Oslo" } });
    let l = fluree
        .transact_cypher_with_params(
            l,
            "MATCH (n:User {id: 7}) SET n += $props",
            params.as_object(),
        )
        .await
        .expect("set map param")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (n:User {id: 7, name: "Anne", city: "Oslo"}) RETURN n"#
            )
            .await
            .unwrap()
            .row_count(),
        1,
        "whole-map param merged via MATCH … SET"
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
async fn transact_cypher_merge_relationship_with_properties_is_per_value() {
    // `MERGE (a)-[:T {p: v}]->(b)` matches only an edge whose annotation
    // carries those values; a different value creates a parallel edge.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-props");

    let l = fluree
        .transact_cypher(l, r#"CREATE (:City {name: "X"}), (:Country {name: "Y"})"#)
        .await
        .expect("seed")
        .ledger;

    // Per-row form on bound endpoints: absent → creates the edge with props.
    let stmt_w3 = r#"MATCH (a:City {name: "X"}), (b:Country {name: "Y"})
                     MERGE (a)-[:IN {since: 2020}]->(b)"#;
    let l = fluree
        .transact_cypher(l, stmt_w3)
        .await
        .expect("create")
        .ledger;
    let t_after_create = l.t();

    // Same statement again: annotation matches → no-op.
    let l = fluree
        .transact_cypher(l, stmt_w3)
        .await
        .expect("noop")
        .ledger;
    assert_eq!(l.t(), t_after_create, "matching property MERGE is a no-op");

    // Different value → the guard misses → a parallel edge is created.
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:City {name: "X"}), (b:Country {name: "Y"})
               MERGE (a)-[:IN {since: 2021}]->(b)"#,
        )
        .await
        .expect("parallel create")
        .ledger;

    let db = graphdb_from_ledger(&l);
    let jsonld = fluree
        .query_cypher(
            &db,
            r#"MATCH (:City {name: "X"})-[r:IN]->(:Country {name: "Y"})
               RETURN r.since ORDER BY r.since"#,
        )
        .await
        .expect("read back")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        jsonld,
        json!([[2020], [2021]]),
        "two parallel edges by value"
    );

    // Exactly two City/Country nodes — per-row MERGE reused the endpoints.
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:City) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_on_match_set_updates_annotation() {
    // Standalone relationship MERGE with ON CREATE / ON MATCH SET resolves as
    // a conditional write: first run creates (ON CREATE fires on the rel
    // var), second run updates the annotation property.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-on-match");

    let stmt = r#"MERGE (a:City {name: "X"})-[r:IN]->(b:Country {name: "Y"})
                  ON CREATE SET r.checks = 1
                  ON MATCH  SET r.checks = 2"#;

    let l = fluree
        .transact_cypher(l, stmt)
        .await
        .expect("create")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let read = r#"MATCH (:City {name: "X"})-[r:IN]->(:Country {name: "Y"}) RETURN r.checks"#;
    let jsonld = fluree
        .query_cypher(&db, read)
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(jsonld, json!([[1]]), "ON CREATE SET fired on the rel var");

    let l = fluree.transact_cypher(l, stmt).await.expect("match").ledger;
    let db = graphdb_from_ledger(&l);
    let jsonld = fluree
        .query_cypher(&db, read)
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        jsonld,
        json!([[2]]),
        "ON MATCH SET updated the annotation property"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:City) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1,
        "no duplicate endpoints on the match branch"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_trailing_set_applies_on_both_branches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-trailing");

    let stmt =
        |year: i64| format!(r#"MERGE (a:U {{id: 1}})-[r:F]->(b:U {{id: 2}}) SET r.at = {year}"#);

    // Create branch: trailing SET lands on the created edge.
    let l = fluree
        .transact_cypher(l, &stmt(2020))
        .await
        .expect("create branch")
        .ledger;
    // Match branch: same edge, trailing SET overwrites.
    let l = fluree
        .transact_cypher(l, &stmt(2021))
        .await
        .expect("match branch")
        .ledger;

    let db = graphdb_from_ledger(&l);
    let jsonld = fluree
        .query_cypher(&db, "MATCH (:U {id: 1})-[r:F]->(:U {id: 2}) RETURN r.at")
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        jsonld,
        json!([[2021]]),
        "trailing SET applied on both branches"
    );
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:U) RETURN n")
            .await
            .unwrap()
            .row_count(),
        2,
        "exactly two endpoint nodes"
    );
}

// ============================================================================
// Multi-statement scripts (semicolon-separated, sequential autocommit)
// ============================================================================

#[tokio::test]
async fn transact_cypher_script_executes_statements_sequentially() {
    // cypher-shell semantics: one commit per statement, later statements see
    // earlier ones' effects (the MATCH in statement 3 binds nodes created by
    // statements 1–2).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:script");
    let t0 = l.t();

    let script = r#"
        CREATE (:Person {name: "Alice; the first"}); // ; inside a string
        CREATE (:Person {name: "Bob"});
        MATCH (a:Person {name: "Alice; the first"}), (b:Person {name: "Bob"})
        CREATE (a)-[:KNOWS {since: 2020}]->(b);
    "#;
    let l = fluree
        .transact_cypher(l, script)
        .await
        .expect("script")
        .ledger;
    assert_eq!(l.t(), t0 + 3, "one commit per statement");

    let db = graphdb_from_ledger(&l);
    let jsonld = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name"#,
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        jsonld,
        json!([["Alice; the first", 2020, "Bob"]]),
        "the semicolon inside the string did not split the statement"
    );
}

#[tokio::test]
async fn transact_cypher_accepts_trailing_semicolon() {
    // A single statement terminated with `;` (the cypher-shell habit) is one
    // statement, not a rejected multi-statement script.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:trailing-semi");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:Person {name: "Ada"});"#)
        .await
        .expect("trailing semicolon accepted")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1
    );
}

#[tokio::test]
async fn transact_cypher_script_return_on_last_statement_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:script-return");

    // RETURN on the final statement answers rows.
    let (result, rows) = fluree
        .transact_cypher_returning(
            l,
            r#"CREATE (:P {id: 1}); CREATE (n:P {id: 2}) RETURN n;"#,
            None,
        )
        .await
        .expect("script with final RETURN");
    assert!(rows.is_some(), "final RETURN answered");

    // A read anywhere before the end is rejected with the statement number.
    let msg = fluree
        .transact_cypher(
            result.ledger,
            r#"MATCH (n:P) RETURN n; CREATE (:P {id: 3});"#,
        )
        .await
        .expect_err("mid-script read")
        .to_string();
    assert!(
        msg.contains("statement 1") && msg.contains("read"),
        "error names the offending statement: {msg}"
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
                    {"@id": "alice", "@type": "Person", "name": "Alice", "KNOWS": {"@id": "bob"}},
                    {"@id": "bob",   "@type": "Person", "name": "Bob",   "KNOWS": {"@id": "carol"}},
                    {"@id": "carol", "@type": "Person", "name": "Carol", "KNOWS": {"@id": "dave"}},
                    {"@id": "dave",  "@type": "Person", "name": "Dave"},
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
                    {"@id": "alice", "@type": "Person", "name": "Alice",
                     "KNOWS": [{"@id": "bob"}, {"@id": "carol"}, {"@id": "dave"}]},
                    {"@id": "bob",   "@type": "Person", "name": "Bob"},
                    {"@id": "carol", "@type": "Person", "name": "Carol"},
                    {"@id": "dave",  "@type": "Person", "name": "Dave"},
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
                    {"@id": "alice", "@type": "Person", "name": "Alice",
                     "KNOWS": [{"@id": "bob"}, {"@id": "bob2"}]},
                    {"@id": "bob",  "@type": "Person", "name": "Bob"},
                    {"@id": "bob2", "@type": "Person", "name": "Bob"},
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
                    {"@id": "a", "@type": "Person", "id": 1, "KNOWS": {"@id": "b"}},
                    {"@id": "b", "@type": "Person", "id": 2, "KNOWS": {"@id": "c"}},
                    {"@id": "c", "@type": "Person", "id": 3},
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
async fn cypher_enumerate_path_relationship_uniqueness_traverses_cycle() {
    // Triangle 0→1→2→0 over :R. Cypher trail semantics (relationship-
    // uniqueness) enumerate the closure 0→1→2→0 — three distinct edges, node 0
    // revisited — which node-distinctness would wrongly drop. An unbounded typed
    // path binding (`p = (a)-[:R*]->(x)`) routes through Enumerate mode.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:enum-relunique");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "n0", "@type": "N", "id": 0, "R": {"@id": "n1"}},
                    {"@id": "n1", "@type": "N", "id": 1, "R": {"@id": "n2"}},
                    {"@id": "n2", "@type": "N", "id": 2, "R": {"@id": "n0"}},
                ]
            }),
        )
        .await
        .expect("seed triangle")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Enumerate from n0 to any node. Relationship-uniqueness yields three trails:
    // n0→n1 (len 1), n0→n1→n2 (len 2), n0→n1→n2→n0 (len 3, the closure). The
    // next hop would reuse edge n0→n1, so the walk stops there. Node-distinctness
    // would have dropped the closure, returning only the first two.
    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:N {id: 0})-[:R*]->(x) RETURN x.id AS dst, length(p) AS len"#,
        )
        .await
        .expect("enumerate")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let mut got: Vec<(i64, i64)> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| (r[0].as_i64().expect("dst"), r[1].as_i64().expect("len")))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![(0, 3), (1, 1), (2, 2)],
        "relationship-uniqueness enumerates the triangle closure n0→n1→n2→n0: {rows}"
    );
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
async fn cypher_path_enumeration_vs_reachability() {
    // Diamond: A→B, A→C, B→D, C→D — two distinct 2-hop paths A→D.
    // Bounded var-length ENUMERATES paths (2 rows); unbounded is REACHABILITY
    // (D reached once → 1 row). Documents the current semantic boundary.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:path-enum");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "A", "@type": "N", "name": "A", "R": [{"@id": "B"}, {"@id": "C"}]},
                    {"@id": "B", "@type": "N", "name": "B", "R": {"@id": "D"}},
                    {"@id": "C", "@type": "N", "name": "C", "R": {"@id": "D"}},
                    {"@id": "D", "@type": "N", "name": "D"},
                ]
            }),
        )
        .await
        .expect("seed diamond");
    let db = graphdb_from_ledger(&committed.ledger);

    let count = |q: &'static str| {
        let fluree = &fluree;
        let db = &db;
        async move {
            let out = fluree
                .query_cypher(db, q)
                .await
                .expect("query")
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld");
            out[0][0].as_i64().expect("count")
        }
    };

    // Bounded: one row per distinct 2-hop trail → 2.
    assert_eq!(
        count(r#"MATCH (a:N {name: "A"})-[:R*2..2]->(d:N {name: "D"}) RETURN count(*) AS c"#).await,
        2,
        "bounded var-length enumerates both A→B→D and A→C→D"
    );

    // Unbounded: reachability — D is reached, counted once → 1.
    assert_eq!(
        count(r#"MATCH (a:N {name: "A"})-[:R*]->(d:N {name: "D"}) RETURN count(*) AS c"#).await,
        1,
        "unbounded var-length is reachability (one row per reachable endpoint)"
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
async fn cypher_shortest_path_node_predicate_pushed_into_search() {
    // A path predicate over `nodes(p)` must be evaluated DURING the search, not
    // post-filtered on the unconstrained shortest path (Neo4j/openCypher
    // semantics). The unconstrained shortest a→z is 2 hops through `bob` (a
    // minor); the shortest ALL-ADULT path is 3 hops a→carol→dave→z. A
    // post-filter would find a→bob→z, reject it, and wrongly return empty.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:sp-node-pred");
    let l = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "a",     "@type": "Person", "role": "start", "age": 30, "KNOWS": [{"@id": "bob"}, {"@id": "carol"}]},
                    {"@id": "bob",   "@type": "Person", "age": 10, "KNOWS": {"@id": "z"}},
                    {"@id": "carol", "@type": "Person", "age": 30, "KNOWS": {"@id": "dave"}},
                    {"@id": "dave",  "@type": "Person", "age": 30, "KNOWS": {"@id": "z"}},
                    {"@id": "z",     "@type": "Person", "role": "end", "age": 30},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Unconstrained shortest is 2 hops (a→bob→z); the shortest ALL-ADULT path
    // is 3 hops (a→carol→dave→z). A correct search returns 3.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {role: "start"}), (z:Person {role: "end"})
               WITH a, z
               MATCH p = shortestPath((a)-[:KNOWS*..15]->(z))
               WHERE all(x IN nodes(p) WHERE x.age >= 18)
               RETURN length(p) AS len"#,
        )
        .await
        .expect("filtered shortestPath")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(3),
        "filtered shortestPath must find the 3-hop all-adult path, not post-filter to empty: {out}"
    );

    // Raising the bar past every intermediate leaves no qualifying path: the
    // search returns nothing (start/end still qualify, but no route does).
    let none = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {role: "start"}), (z:Person {role: "end"})
               WITH a, z
               MATCH p = shortestPath((a)-[:KNOWS*..15]->(z))
               WHERE all(x IN nodes(p) WHERE x.age >= 100)
               RETURN length(p) AS len"#,
        )
        .await
        .expect("no qualifying path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(none.as_array().map(Vec::len), Some(0), "no path: {none}");

    // Unfiltered shortestPath is unchanged: the 2-hop path through the minor.
    let two = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {role: "start"}), (z:Person {role: "end"})
               WITH a, z
               MATCH p = shortestPath((a)-[:KNOWS*..15]->(z))
               RETURN length(p) AS len"#,
        )
        .await
        .expect("unfiltered")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(two[0][0], json!(2), "unfiltered shortest is 2 hops: {two}");
}

#[tokio::test]
async fn cypher_shortest_path_batched_lane_respects_novelty() {
    // The raw-id shortestPath lane reads base index rows for clean nodes and
    // must fall back per node wherever novelty touches an expansion side:
    // novelty shortcut edges shorten the path, novelty retracts of base
    // edges break it, and novelty-only endpoints join the search.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:sp-novelty";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@graph": [
                    {"@id": "a", "@type": "Person", "name": "A", "KNOWS": {"@id": "b"}},
                    {"@id": "b", "@type": "Person", "name": "B", "KNOWS": {"@id": "c"}},
                    {"@id": "c", "@type": "Person", "name": "C", "KNOWS": {"@id": "d"}},
                    {"@id": "d", "@type": "Person", "name": "D", "KNOWS": {"@id": "e"}},
                    {"@id": "e", "@type": "Person", "name": "E"},
                ]
            }),
        )
        .await
        .expect("seed chain");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let l = fluree.ledger(ledger_id).await.expect("reload");

    let sp_len = |db: fluree_db_api::GraphDb, q: &'static str| {
        let fluree = &fluree;
        async move {
            fluree
                .query_cypher(&db, q)
                .await
                .expect("shortestPath query")
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld")[0][0]
                .clone()
        }
    };
    const A_TO_E: &str = r#"MATCH (a:Person {name: "A"}), (e:Person {name: "E"})
        MATCH p = shortestPath((a)-[:KNOWS*]->(e)) RETURN length(p) AS len"#;

    // Fully indexed, clean overlay: pure batched lane.
    assert_eq!(
        sp_len(graphdb_from_ledger(&l), A_TO_E).await,
        json!(4),
        "indexed chain A→E is 4 hops"
    );

    // Novelty shortcut B→D: B is now a dirty subject (its base out-edges are
    // incomplete), D a dirty object. Path must shorten through the fallback.
    let l = fluree
        .insert(l, &json!({"@id": "b", "KNOWS": {"@id": "d"}}))
        .await
        .expect("novelty shortcut")
        .ledger;
    assert_eq!(
        sp_len(graphdb_from_ledger(&l), A_TO_E).await,
        json!(3),
        "novelty shortcut B→D shortens A→E to 3"
    );

    // Same answer on the wildcard (untyped) expansion lanes.
    assert_eq!(
        sp_len(
            graphdb_from_ledger(&l),
            r#"MATCH (a:Person {name: "A"}), (e:Person {name: "E"})
               MATCH p = shortestPath((a)-[*..15]->(e)) RETURN length(p) AS len"#,
        )
        .await,
        json!(3),
        "wildcard shortestPath sees the novelty shortcut"
    );

    // Novelty retract of the BASE edge C→D: the original chain is broken,
    // so the only route is the shortcut.
    let l = fluree
        .update(
            l,
            &json!({
                "delete": {"@id": "c", "KNOWS": {"@id": "d"}}
            }),
        )
        .await
        .expect("retract base edge")
        .ledger;
    assert_eq!(
        sp_len(graphdb_from_ledger(&l), A_TO_E).await,
        json!(3),
        "retracting C→D leaves the shortcut route"
    );

    // Retract the shortcut too: no path remains — the match yields no row.
    let l = fluree
        .update(
            l,
            &json!({
                "delete": {"@id": "b", "KNOWS": {"@id": "d"}}
            }),
        )
        .await
        .expect("retract shortcut")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "A"}), (e:Person {name: "E"})
               MATCH p = shortestPath((a)-[:KNOWS*]->(e)) RETURN count(p) AS n"#,
        )
        .await
        .expect("count")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out[0][0],
        json!(0),
        "no path once both routes are retracted: {out}"
    );

    // Novelty-only endpoint: a brand-new node X hanging off E joins the
    // search without a persisted id. C→D (retracted from base above) is
    // re-asserted in novelty, so the C→X route crosses a re-asserted base
    // edge, base edges, and a novelty-only edge.
    let l = fluree
        .insert(
            l,
            &json!({"@graph": [
                {"@id": "c", "KNOWS": {"@id": "d"}},
                {"@id": "e", "KNOWS": {"@id": "x"}},
                {"@id": "x", "@type": "Person", "name": "X"}
            ]}),
        )
        .await
        .expect("novelty endpoint")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (c:Person {name: "C"}), (x:Person {name: "X"})
               MATCH p = shortestPath((c)-[:KNOWS*]->(x)) RETURN length(p) AS len"#,
        )
        .await
        .expect("novelty endpoint path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out[0][0], json!(3), "C→D→E→X spans base and novelty: {out}");
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
async fn cypher_relationships_incoming_direction() {
    // relationships(p) must report the STORED edge direction, not traversal
    // order. For an incoming path `(b)<-[:KNOWS]-(a)` the edge is a→b, so the
    // relationship's startNode is `a` even though `b` is the path's first node.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:rels-incoming").await;
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (b:Person {name: "Bob"})<-[:KNOWS*1..1]-(a:Person)
               RETURN startNode(relationships(p)[0]) AS s, endNode(relationships(p)[0]) AS e,
                      a AS aa, b AS bb"#,
        )
        .await
        .expect("incoming path relationships")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(
        row[0], row[2],
        "edge start is a (Alice), not the path's first node: {cj}"
    );
    assert_eq!(row[1], row[3], "edge end is b (Bob): {cj}");
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

    // Unbounded rel binding enumerates: one row per trail (relationship-unique).
    let unbounded = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[r:KNOWS*]->(b:Person)
               RETURN size(r) AS hops ORDER BY hops"#,
        )
        .await
        .expect("unbounded rel binding enumerates")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(unbounded, json!([[1], [2], [3]]), "one row per path length");
}

#[tokio::test]
async fn cypher_shortest_path_untyped_wildcard() {
    // Untyped shortestPath: follows any
    // relationship type, skipping rdf:type and data properties. The chain
    // mixes KNOWS and LIKES edges, so a single-type search can't reach Dave.
    let fluree = FlureeBuilder::memory().build_memory();
    let mut l = genesis_ledger(&fluree, "it/cypher:sp-untyped");
    for stmt in [
        r#"CREATE (a:Person {name: "Alice"})"#,
        r#"CREATE (b:Person {name: "Bob"})"#,
        r#"CREATE (c:Person {name: "Carol"})"#,
        r#"CREATE (d:Person {name: "Dave"})"#,
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
        r#"MATCH (b:Person {name: "Bob"}), (c:Person {name: "Carol"}) CREATE (b)-[:LIKES]->(c)"#,
        r#"MATCH (c:Person {name: "Carol"}), (d:Person {name: "Dave"}) CREATE (c)-[:KNOWS]->(d)"#,
    ] {
        l = fluree.transact_cypher(l, stmt).await.expect(stmt).ledger;
    }
    let db = graphdb_from_ledger(&l);

    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}), (d:Person {name: "Dave"})
               MATCH p = shortestPath((a)-[*..15]->(d))
               RETURN length(p) AS len, [r IN relationships(p) | type(r)] AS types"#,
        )
        .await
        .expect("untyped shortestPath")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!(3), "Alice→Dave is 3 mixed hops: {cj}");
    assert_eq!(
        row[1],
        json!(["KNOWS", "LIKES", "KNOWS"]),
        "per-hop types resolved post-hoc: {cj}"
    );

    // Typed search over the same pair finds nothing (LIKES breaks the chain).
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (a:Person {name: "Alice"}), (d:Person {name: "Dave"})
                   MATCH p = shortestPath((a)-[:KNOWS*..15]->(d))
                   RETURN length(p)"#,
            )
            .await
            .expect("typed control")
            .row_count(),
        0,
        "typed-only search must not cross the LIKES hop"
    );

    // allShortestPaths untyped.
    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (a:Person {name: "Alice"}), (d:Person {name: "Dave"})
                   MATCH p = allShortestPaths((a)-[*..15]->(d))
                   RETURN length(p)"#,
            )
            .await
            .expect("untyped allShortestPaths")
            .row_count(),
        1,
        "exactly one minimal path"
    );
}

#[tokio::test]
async fn cypher_shortest_path_untyped_skips_type_and_data_edges() {
    // The wildcard edge-set must not treat rdf:type or a data property as a
    // hop: two nodes sharing only a class have no path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:sp-untyped-edge-set");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "alice", "@type": "Person", "name": "Alice"},
                    {"@id": "bob", "@type": "Person", "name": "Bob"},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    assert_eq!(
        fluree
            .query_cypher(
                &db,
                r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
                   MATCH p = shortestPath((a)-[*..15]->(b))
                   RETURN length(p)"#,
            )
            .await
            .expect("no path via class node")
            .row_count(),
        0,
        "rdf:type must not connect nodes through their shared class"
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

// ============================================================================
// Path enumeration — free path values, unbounded var-length binding
// ============================================================================

#[tokio::test]
async fn cypher_path_enumeration_unbounded_free_end() {
    // `p = (a)-[:T*]->(b)` with b unbound: one row per trail (relationship-unique).
    // Chain Alice→Bob→Carol→Dave gives paths of length 1, 2, 3 from Alice.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:enum-free").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[:KNOWS*]->(b)
               RETURN b.name AS dst, length(p) AS len ORDER BY len"#,
        )
        .await
        .expect("enumerate")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["Bob", 1], ["Carol", 2], ["Dave", 3]]),
        "every path from Alice, end bound per path"
    );
}

#[tokio::test]
async fn cypher_path_enumeration_bound_end_filters() {
    // A bound end keeps only paths ending there; a diamond yields both.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:enum-diamond");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:N {name: "A"}), (b:N {name: "B"}), (c:N {name: "C"}), (d:N {name: "D"});
               MATCH (a:N {name: "A"}), (b:N {name: "B"}) CREATE (a)-[:E]->(b);
               MATCH (a:N {name: "A"}), (c:N {name: "C"}) CREATE (a)-[:E]->(c);
               MATCH (b:N {name: "B"}), (d:N {name: "D"}) CREATE (b)-[:E]->(d);
               MATCH (c:N {name: "C"}), (d:N {name: "D"}) CREATE (c)-[:E]->(d);"#,
        )
        .await
        .expect("seed diamond")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:N {name: "A"})-[:E*]->(d:N {name: "D"})
               RETURN [n IN nodes(p) | n.name] AS names ORDER BY names"#,
        )
        .await
        .expect("diamond paths")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([[["A", "B", "D"]], [["A", "C", "D"]]]),
        "both diamond arms enumerated"
    );
}

#[tokio::test]
async fn cypher_rel_var_binding_on_unbounded_path() {
    // `-[r:T*]->` binds the relationship list per enumerated path.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:enum-relvar").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[r:KNOWS*2..]->(b)
               RETURN b.name AS dst, size(r) AS hops, [x IN r | type(x)] AS types
               ORDER BY hops"#,
        )
        .await
        .expect("rel var enumerate")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([
            ["Carol", 2, ["KNOWS", "KNOWS"]],
            ["Dave", 3, ["KNOWS", "KNOWS", "KNOWS"]]
        ]),
        "lower bound 2 honored; rel list per path"
    );
}

#[tokio::test]
async fn cypher_path_enumeration_untyped_and_fixed_hop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:enum-untyped").await;
    let db = graphdb_from_ledger(&l);

    // Untyped wildcard with a path binding.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[*1..2]->(b)
               RETURN b.name AS dst, length(p) AS len ORDER BY len"#,
        )
        .await
        .expect("untyped enumerate")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([["Bob", 1], ["Carol", 2]]), "wildcard bounded");

    // Fixed single hop `p = (a)-[:T]->(b)` — a *1..1 path value.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[:KNOWS]->(b)
               RETURN length(p) AS len, [n IN nodes(p) | n.name] AS names,
                      [x IN relationships(p) | type(x)] AS types"#,
        )
        .await
        .expect("fixed hop path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([[1, ["Alice", "Bob"], ["KNOWS"]]]),
        "fixed hop builds a 1-hop path value"
    );
}

#[tokio::test]
async fn cypher_path_enumeration_zero_length_and_cycles() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:enum-zero-cycle");
    // A→B and B→A: a 2-cycle.
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:N {name: "A"})-[:E]->(b:N {name: "B"});
               MATCH (a:N {name: "A"}), (b:N {name: "B"}) CREATE (b)-[:E]->(a);"#,
        )
        .await
        .expect("seed cycle")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // `*0..` includes the zero-length path (end = start, no relationships).
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:N {name: "A"})-[:E*0..1]->(b)
               RETURN b.name AS dst, length(p) AS len ORDER BY len"#,
        )
        .await
        .expect("zero length")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["A", 0], ["B", 1]]),
        "zero-length path binds end to start"
    );

    // Unbounded enumeration on a cyclic graph terminates under relationship-
    // uniqueness (no edge reused): from A the trails are A→B and the closure
    // A→B→A (both edges distinct); A→B→A→B would reuse A→B, so the walk stops.
    // Matches Neo4j — a node may be revisited via a different edge.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:N {name: "A"})-[:E*]->(b)
               RETURN b.name AS dst, length(p) AS len ORDER BY len"#,
        )
        .await
        .expect("cycle safe")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["B", 1], ["A", 2]]),
        "relationship-uniqueness enumerates the 2-cycle closure A→B→A"
    );
}

#[tokio::test]
async fn cypher_path_enumeration_undirected_binding() {
    // Undirected var-length with a binding routes through enumeration.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:enum-undirected").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (b:Person {name: "Bob"})-[:KNOWS*1..1]-(x)
               RETURN x.name AS other ORDER BY other"#,
        )
        .await
        .expect("undirected enumerate")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["Alice"], ["Carol"]]),
        "both undirected neighbors of Bob"
    );
}

#[tokio::test]
async fn cypher_multi_hop_fixed_path_value() {
    // `p = (a)-[:T]->(b)-[:T]->(c)` — a multi-hop fixed chain builds the
    // path value from interleaved nodes and per-hop relationship values.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_knows_chain(&fluree, "it/cypher:multi-hop-path").await;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
               RETURN length(p) AS len,
                      [n IN nodes(p) | n.name] AS names,
                      [r IN relationships(p) | type(r)] AS types"#,
        )
        .await
        .expect("multi-hop path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([[2, ["Alice", "Bob", "Carol"], ["KNOWS", "KNOWS"]]]),
        "{out}"
    );

    // Incoming hops keep the STORED edge orientation in relationships(p).
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (c:Person {name: "Carol"})<-[:KNOWS]-(b)<-[:KNOWS]-(a)
               RETURN [n IN nodes(p) | n.name] AS names,
                      startNode(relationships(p)[0]) = b AS first_starts_at_b"#,
        )
        .await
        .expect("incoming multi-hop path")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([[["Carol", "Bob", "Alice"], true]]), "{out}");

    // A variable-length segment inside a multi-hop path value stays deferred.
    let msg = fluree
        .query_cypher(
            &db,
            r#"MATCH p = (a:Person {name: "Alice"})-[:KNOWS*1..2]->(b)-[:KNOWS]->(c) RETURN p"#,
        )
        .await
        .expect_err("mixed var-length multi-hop")
        .to_string();
    assert!(msg.contains("variable-length segment"), "{msg}");
}

#[tokio::test]
async fn cypher_var_length_property_filter_on_bounded_range() {
    // `-[:T*1..2 {p: v}]->` matches per hop on the edge annotation values.
    // A -[w:1]-> B -[w:1]-> C, plus A -[w:2]-> C directly.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:varlen-props");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (:V {name: "A"}), (:V {name: "B"}), (:V {name: "C"});
               MATCH (a:V {name: "A"}), (b:V {name: "B"}) CREATE (a)-[:E {w: 1}]->(b);
               MATCH (b:V {name: "B"}), (c:V {name: "C"}) CREATE (b)-[:E {w: 1}]->(c);
               MATCH (a:V {name: "A"}), (c:V {name: "C"}) CREATE (a)-[:E {w: 2}]->(c);"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Only w:1 hops qualify: A→B (1 hop) and A→B→C (2 hops); the direct
    // A→C edge (w:2) is filtered out.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:V {name: "A"})-[:E*1..2 {w: 1}]->(x)
               RETURN x.name AS dst ORDER BY dst"#,
        )
        .await
        .expect("filtered var-length")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([["B"], ["C"]]), "{out}");

    // Unbounded / binding combinations stay deferred with clear errors.
    let msg = fluree
        .query_cypher(&db, r#"MATCH (a:V)-[:E* {w: 1}]->(x) RETURN x"#)
        .await
        .expect_err("unbounded with props")
        .to_string();
    assert!(msg.contains("bounded"), "{msg}");
    let msg = fluree
        .query_cypher(&db, r#"MATCH (a:V)-[r:E*1..2 {w: 1}]->(x) RETURN x"#)
        .await
        .expect_err("binding with props")
        .to_string();
    assert!(msg.contains("WHERE"), "{msg}");
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
                    {"@id": "a", "@type": "Person", "name": "A",
                     "KNOWS": [{"@id": "b"}, {"@id": "c"}]},
                    {"@id": "b", "@type": "Person", "name": "B", "KNOWS": {"@id": "d"}},
                    {"@id": "c", "@type": "Person", "name": "C", "KNOWS": {"@id": "d"}},
                    {"@id": "d", "@type": "Person", "name": "D"},
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
                    {"@id": "a", "@type": "Person", "name": "A",
                     "KNOWS": [{"@id": "b"}, {"@id": "d"}]},
                    {"@id": "b", "@type": "Person", "name": "B", "KNOWS": {"@id": "d"}},
                    {"@id": "d", "@type": "Person", "name": "D"},
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
                    {"@id": "a", "@type": "Person", "name": "A",
                     "KNOWS": [{"@id": "b"}, {"@id": "d"}]},
                    {"@id": "b", "@type": "Person", "name": "B", "KNOWS": {"@id": "d"}},
                    {"@id": "d", "@type": "Person", "name": "D"},
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
                    {"@id": "n1", "@type": "Person", "id": 1, "name": "Alice",
                     "KNOWS": [{"@id": "n3"}, {"@id": "n4"}]},
                    {"@id": "n2", "@type": "Person", "id": 2, "name": "Bob"},
                    {"@id": "n3", "@type": "Person", "id": 3, "name": "Carol"},
                    {"@id": "n4", "@type": "Person", "id": 4, "name": "Dave"},
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
                {"@id": "a", "@type": "Person", "name": "Alice",
                 "KNOWS": [{"@id": "b"}, {"@id": "c"}, {"@id": "d"}]},
                {"@id": "b", "@type": "Person", "name": "Bob"},
                {"@id": "c", "@type": "Person", "name": "Carol"},
                {"@id": "d", "@type": "Person", "name": "Dave"},
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
                {"@id":"alice","@type":"Person","name":"Alice","fname":"Start","KNOWS":{"@id":"bob"}},
                {"@id":"bob","@type":"Person","name":"Bob","fname":"Friend","KNOWS":{"@id":"carol"}},
                {"@id":"carol","@type":"Person","name":"Carol","fname":"Friend","KNOWS":{"@id":"dave"}},
                {"@id":"dave","@type":"Person","name":"Dave","fname":"Friend","KNOWS":{"@id":"eve"}},
                {"@id":"eve","@type":"Person","name":"Eve","fname":"Friend"},
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
                {"@id":"n1","@type":"Person","sid":"10","name":"A"},
                {"@id":"n2","@type":"Person","sid":"2","name":"B"},
                {"@id":"n3","@type":"Person","sid":"30","name":"C"},
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
    assert_eq!(nodes[0], json!("alice"));
    assert_eq!(nodes[3], json!("dave"));

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
                {"@id":"a","@type":"Person","name":"A","KNOWS":[{"@id":"b"},{"@id":"c"}]},
                {"@id":"b","@type":"Person","name":"B","KNOWS":{"@id":"d"}},
                {"@id":"c","@type":"Person","name":"C","KNOWS":{"@id":"d"}},
                {"@id":"d","@type":"Person","name":"D"},
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
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id":"tagA","@type":"Tag","name":"A","HAS_TYPE":{"@id":"tc1"}},
                {"@id":"tc1","@type":"TagClass","name":"C1","IS_SUBCLASS_OF":{"@id":"tc2"}},
                {"@id":"tc2","@type":"TagClass","name":"C2","IS_SUBCLASS_OF":{"@id":"tcRoot"}},
                {"@id":"tcRoot","@type":"TagClass","name":"Root"},
            ]}),
        )
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
        json!([["alice", "bob"], ["bob", "carol"], ["carol", "dave"],]),
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
        json!({"@id":"a","@type":"Person","name":"A","KNOWS":[{"@id":"b"},{"@id":"c"}]}),
        json!({"@id":"b","@type":"Person","name":"B","KNOWS":{"@id":"d"}}),
        json!({"@id":"c","@type":"Person","name":"C","KNOWS":{"@id":"d"}}),
        json!({"@id":"d","@type":"Person","name":"D"}),
    ];
    // Helper: n messages from `from` to `to`, with globally-unique message ids.
    let add_msgs = |from: &str, to: &str, n: usize, graph: &mut Vec<JsonValue>| {
        for i in 0..n {
            let mid = format!("m_{from}_{to}_{i}");
            graph.push(json!({
                "@id": mid,
                "SENT_BY": {"@id": format!("{from}")},
                "RCVD_BY": {"@id": format!("{to}")},
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
    let person = |id: &str, knows: JsonValue| json!({"@id": format!("{id}"), "@type":"Person", "pid": id, "KNOWS": knows});
    // Comment `c` by `creator` replying to message `target`.
    let comment = |c: &str, creator: &str, target: &str| {
        json!({"@id": format!("{c}"), "@type":"Comment",
               "HAS_CREATOR":{"@id":format!("{creator}")},
               "REPLY_OF":{"@id":format!("{target}")}})
    };
    let message = |m: &str, ty: &str, creator: &str| {
        json!({"@id": format!("{m}"), "@type": format!("{ty}"),
               "HAS_CREATOR":{"@id":format!("{creator}")}})
    };
    let graph = json!([
        person("p0", json!([{"@id":"p1"},{"@id":"p2"}])),
        person("p1", json!([{"@id":"p3"}])),
        person("p2", json!([{"@id":"p3"}])),
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
    let person = |id: &str, knows: JsonValue| json!({"@id": format!("{id}"), "@type":"Person", "pid": id, "KNOWS": knows});
    let comment = |c: &str, creator: &str, target: &str| {
        json!({"@id": format!("{c}"), "@type":"Comment",
               "HAS_CREATOR":{"@id":format!("{creator}")},
               "REPLY_OF":{"@id":format!("{target}")}})
    };
    let post = |m: &str, creator: &str| {
        json!({"@id": format!("{m}"), "@type":"Post",
               "HAS_CREATOR":{"@id":format!("{creator}")}})
    };
    // Diamond p0-p1-p3 / p0-p2-p3; each route scores exactly 1.0:
    //   (p0,p1): p0 comment → p1 post (1.0); (p1,p3): none  → path 1.0
    //   (p2,p3): p2 comment → p3 post (1.0); (p0,p2): none  → path 1.0
    let graph = json!([
        person("p0", json!([{"@id":"p1"},{"@id":"p2"}])),
        person("p1", json!([{"@id":"p3"}])),
        person("p2", json!([{"@id":"p3"}])),
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
                {"@id":"a","@type":"Person","name":"A","KNOWS":[{"@id":"b"},{"@id":"c"}]},
                {"@id":"b","@type":"Person","name":"B","KNOWS":{"@id":"d"}},
                {"@id":"c","@type":"Person","name":"C","KNOWS":{"@id":"d"}},
                {"@id":"d","@type":"Person","name":"D"},
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
                {"@id":"a","@type":"Person","name":"A","KNOWS":{"@id":"b"}},
                {"@id":"b","@type":"Person","name":"B","KNOWS":{"@id":"c"}},
                {"@id":"c","@type":"Person","name":"C","KNOWS":{"@id":"a"}},
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
            {"@id": "hub", "@type": "Person", "id": 0,
             "KNOWS": [{"@id": "m1"}, {"@id": "m2"}, {"@id": "m3"}]},
            {"@id": "m1", "@type": "Person", "id": 1, "KNOWS": {"@id": "x1"}},
            {"@id": "m2", "@type": "Person", "id": 2, "KNOWS": {"@id": "x2"}},
            {"@id": "m3", "@type": "Person", "id": 3, "KNOWS": {"@id": "x3"}},
            {"@id": "x1", "@type": "Person", "id": 11},
            {"@id": "x2", "@type": "Person", "id": 12},
            {"@id": "x3", "@type": "Person", "id": 13},
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
            {"@id": "p0", "@type": "Person", "id": 0,
             "KNOWS": [{"@id": "p100"}, {"@id": "p1"}, {"@id": "p2"}]},
            {"@id": "p1", "@type": "Person", "id": 1, "KNOWS": {"@id": "p100"}},
            {"@id": "p2", "@type": "Person", "id": 2, "KNOWS": {"@id": "p100"}},
            {"@id": "p100", "@type": "Person", "id": 100},
            {"@id": "tKnot", "@type": "Tag", "name": "Knot"},
            {"@id": "tDF", "@type": "Tag", "name": "DavidFoster"},
            {"@id": "m0", "@type": "Post", "id": 1000,
             "HAS_CREATOR": {"@id": "p100"}, "HAS_TAG": [{"@id": "tKnot"}, {"@id": "tDF"}]},
            {"@id": "m1", "@type": "Post", "id": 1001,
             "HAS_CREATOR": {"@id": "p100"}, "HAS_TAG": [{"@id": "tKnot"}, {"@id": "tDF"}]},
            {"@id": "m2", "@type": "Post", "id": 1002,
             "HAS_CREATOR": {"@id": "p100"}, "HAS_TAG": [{"@id": "tKnot"}, {"@id": "tDF"}]},
            {"@id": "m3", "@type": "Post", "id": 1003,
             "HAS_CREATOR": {"@id": "p100"}, "HAS_TAG": [{"@id": "tKnot"}, {"@id": "tDF"}]},
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

#[tokio::test]
async fn cypher_power_binds_tighter_than_unary_minus() {
    // Regression: openCypher/Neo4j precedence puts `^` ABOVE unary `-`, so
    // `-2 ^ 2` = -(2^2) = -4, not (-2)^2 = 4. The right operand of `^` still
    // accepts a sign (`2 ^ -3` = 0.125), and `^` is right-associative
    // (`-2 ^ 2 ^ 2` = -(2^(2^2)) = -16).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_nodes_with_ids(&fluree, "it/cypher:power-precedence").await;
    let db = graphdb_from_ledger(&l);

    for (expr, want) in [("-2 ^ 2", -4.0), ("2 ^ -3", 0.125), ("-2 ^ 2 ^ 2", -16.0)] {
        let q = format!("MATCH (n:Person) RETURN {expr} AS x ORDER BY x");
        let out = fluree
            .query_cypher(&db, &q)
            .await
            .unwrap_or_else(|e| panic!("query `{expr}`: {e:?}"))
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        let got = out[0][0]
            .as_f64()
            .unwrap_or_else(|| panic!("`{expr}` produced non-numeric {out}"));
        assert!(
            (got - want).abs() < 1e-9,
            "`{expr}` evaluated to {got}, expected {want}"
        );
    }
}

#[tokio::test]
async fn cypher_explain_reports_sid_encoded_plan() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:explain");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "alice",
                "@type": "Person",
                "id": 7,
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let explain = fluree_db_api::explain::explain_cypher(
        &db.snapshot,
        "MATCH (n:Person {id: 7}) RETURN n",
        db.default_context.as_ref(),
    )
    .await
    .expect("explain");

    // The lowering must encode registered-namespace IRIs to SIDs (parity
    // with SPARQL) — Sid-gated planner analyses and batched join lanes
    // depend on it. The physical PropertyJoin renders Sid predicates in
    // `ns:name` form; an unencoded pattern would render the full IRI.
    let physical = serde_json::to_string(&explain["plan"]["physical"]).expect("physical");
    assert!(physical.contains("PropertyJoinOperator"), "{explain}");
    assert!(
        physical.contains("3:type"),
        "rdf:type not SID-encoded: {explain}"
    );
    assert!(
        physical.contains("0:id"),
        "bare `id` not SID-encoded under namespace 0: {explain}"
    );
}

#[tokio::test]
async fn cypher_with_where_property_equality_folds_to_seek() {
    // `WITH n WHERE n.id = k` lowers the accessor to OPTIONAL + FILTER; the
    // optional-filter fold must turn it into a required triple (seek) without
    // changing rows — including multi-valued properties and absent properties.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:with-where-fold");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "a", "@type": "Person", "id": 7, "age": [25, 30]},
                    {"@id": "b", "@type": "Person", "id": 8},
                    {"@id": "c", "@type": "Person"},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    let rows = |r: fluree_db_api::QueryResult| r.row_count();

    let r = fluree
        .query_cypher(&db, "MATCH (n:Person) WITH n WHERE n.id = 7 RETURN n")
        .await
        .expect("with-where eq");
    assert_eq!(rows(r), 1);

    // Absent property: no rows (c has no id) — the fold preserves this.
    let r = fluree
        .query_cypher(&db, "MATCH (n:Person) WITH n WHERE n.id = 99 RETURN n")
        .await
        .expect("no match");
    assert_eq!(rows(r), 0);

    // Multi-valued property through a range comparison: one row per passing
    // value.
    let r = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.age > 26 RETURN n, n.age")
        .await
        .expect("range");
    assert_eq!(rows(r), 1);

    // IS NULL keeps its OPTIONAL (bound-check is not error-rejecting):
    // only c lacks id.
    let r = fluree
        .query_cypher(&db, "MATCH (n:Person) WHERE n.id IS NULL RETURN n")
        .await
        .expect("is null");
    assert_eq!(rows(r), 1);
}

#[tokio::test]
async fn cypher_anonymous_hop_chain_fuses_to_reachability_under_distinct() {
    // Diamond + tail: a→b1→c, a→b2→c, c→d. Two 2-hop walks reach c.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:chain-fusion");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "a", "@type": "User", "id": 1,
                     "knows": [{"@id": "b1"}, {"@id": "b2"}]},
                    {"@id": "b1", "@type": "User", "knows": {"@id": "c"}},
                    {"@id": "b2", "@type": "User", "knows": {"@id": "c"}},
                    {"@id": "c", "@type": "User", "id": 3,
                     "knows": {"@id": "d"}},
                    {"@id": "d", "@type": "User", "id": 4},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // DISTINCT endpoints via 2 anonymous hops: just c (one row despite two
    // walks) — the fused frontier-BFS form must agree with join semantics
    // after dedup.
    let r = fluree
        .query_cypher(
            &db,
            "MATCH (s:User {id: 1})-->()-->(n:User) RETURN DISTINCT n.id",
        )
        .await
        .expect("fused distinct");
    assert_eq!(r.row_count(), 1);

    // WITHOUT DISTINCT the chain must keep one row per walk (2 rows) — the
    // fusion is gated off.
    let r = fluree
        .query_cypher(&db, "MATCH (s:User {id: 1})-->()-->(n:User) RETURN n.id")
        .await
        .expect("walk multiplicity");
    assert_eq!(r.row_count(), 2, "non-DISTINCT keeps per-walk rows");

    // Aggregates observe walk multiplicity: count(*) = 2 even with DISTINCT
    // elsewhere — gated off.
    let cj = fluree
        .query_cypher(
            &db,
            "MATCH (s:User {id: 1})-->()-->(n:User) RETURN count(*) AS c",
        )
        .await
        .expect("count walks")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(cj["results"][0]["data"][0]["row"][0], json!(2), "{cj}");

    // A named intermediate node is observable — not fused; DISTINCT (m, n)
    // pairs: (b1,c) and (b2,c).
    let r = fluree
        .query_cypher(
            &db,
            "MATCH (s:User {id: 1})-->(m)-->(n:User) RETURN DISTINCT m, n",
        )
        .await
        .expect("named middle");
    assert_eq!(r.row_count(), 2);

    // 3-hop chain: only d.
    let cj = fluree
        .query_cypher(
            &db,
            "MATCH (s:User {id: 1})-->()-->()-->(n:User) RETURN DISTINCT n.id",
        )
        .await
        .expect("3-hop")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(cj["results"][0]["data"][0]["row"][0], json!(4), "{cj}");
}

/// Class-anchored aggregate folds (`MATCH (n:C) …`): the histogram
/// (`RETURN n.age, COUNT(*)`) folds to a POST group count + null-group row,
/// and the scalar family reuses the whole-graph folds under the containment
/// proof. Indexed (fold) and novelty (pipeline) must agree.
#[tokio::test]
async fn cypher_class_anchored_histogram_and_scalars_match_pipeline() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:class-agg";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    // alice multi-valued: contributes to two histogram groups.
                    {"@id": "alice", "@type": "Person", "age": [25, 30]},
                    {"@id": "bob",   "@type": "Person"},
                    {"@id": "carol", "@type": "Person", "age": 30},
                    {"@id": "dave",  "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed");
    let novelty_db = graphdb_from_ledger(&committed.ledger);
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed_db = fluree.db(ledger_id).await.expect("indexed view");

    let histogram = |cj: &JsonValue| -> Vec<(JsonValue, JsonValue)> {
        let mut rows: Vec<(JsonValue, JsonValue)> = cj["results"][0]["data"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| (r["row"][0].clone(), r["row"][1].clone()))
            .collect();
        rows.sort_by_key(|(k, _)| k.to_string());
        rows
    };

    for db in [&novelty_db, &indexed_db] {
        let cj = fluree
            .query_cypher(db, "MATCH (n:Person) RETURN n.age, COUNT(*)")
            .await
            .expect("histogram")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        // Groups: 25→1 (alice), 30→2 (alice+carol), 40→1 (dave),
        // null→1 (bob).
        assert_eq!(
            histogram(&cj),
            vec![
                (json!(25), json!(1)),
                (json!(30), json!(2)),
                (json!(40), json!(1)),
                (json!(null), json!(1)),
            ],
            "{cj}"
        );

        let cj = fluree
            .query_cypher(
                db,
                "MATCH (n:Person) RETURN COUNT(DISTINCT n.age) AS d, count(n) AS c",
            )
            .await
            .expect("scalars")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        let row = &cj["results"][0]["data"][0]["row"];
        assert_eq!(row[0], json!(3), "distinct ages: {cj}");
        // count(n) over the left join: alice 2 rows + bob 1 + carol 1 + dave 1.
        assert_eq!(row[1], json!(5), "count rows: {cj}");
    }
}

/// The containment proof must fail when a non-class subject bears the
/// property — the fold defers and the pipeline restricts to the class.
#[tokio::test]
async fn cypher_class_anchored_fold_declines_without_containment() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:class-agg-decline";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 30},
                    // A Robot with an age: age is NOT contained in Person.
                    {"@id": "r2d2", "@type": "Robot", "age": 200},
                ]
            }),
        )
        .await
        .expect("seed");
    let novelty_db = graphdb_from_ledger(&committed.ledger);
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed_db = fluree.db(ledger_id).await.expect("indexed view");

    for db in [&novelty_db, &indexed_db] {
        let cj = fluree
            .query_cypher(db, "MATCH (n:Person) RETURN n.age, COUNT(*)")
            .await
            .expect("histogram")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        let rows = cj["results"][0]["data"].as_array().expect("rows");
        // Only alice's age — the Robot's 200 must not leak in.
        assert_eq!(rows.len(), 1, "{cj}");
        assert_eq!(rows[0]["row"], json!([30, 1]), "{cj}");

        let cj = fluree
            .query_cypher(db, "MATCH (n:Person) RETURN max(n.age) AS m")
            .await
            .expect("max")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        assert_eq!(cj["results"][0]["data"][0]["row"][0], json!(30), "{cj}");
    }
}

/// Filtered histogram: the WHERE predicate references only the group key, so
/// the fold evaluates it once per group (null group included) — identical to
/// the pipeline's per-row filtering.
#[tokio::test]
async fn cypher_class_anchored_filtered_histogram_matches_pipeline() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:class-agg-filtered";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": [15, 30]},
                    {"@id": "bob",   "@type": "Person"},
                    {"@id": "carol", "@type": "Person", "age": 30},
                    {"@id": "dave",  "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed");
    let novelty_db = graphdb_from_ledger(&committed.ledger);
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed_db = fluree.db(ledger_id).await.expect("indexed view");

    let rows_of = |cj: &JsonValue| -> Vec<(JsonValue, JsonValue)> {
        let mut rows: Vec<(JsonValue, JsonValue)> = cj["results"][0]["data"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| (r["row"][0].clone(), r["row"][1].clone()))
            .collect();
        rows.sort_by_key(|(k, _)| k.to_string());
        rows
    };

    for db in [&novelty_db, &indexed_db] {
        // Range filter: groups >= 18 only, no null group (unbound rejected).
        let cj = fluree
            .query_cypher(
                db,
                "MATCH (n:Person) WHERE n.age >= 18 RETURN n.age, COUNT(*)",
            )
            .await
            .expect("filtered histogram")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        assert_eq!(
            rows_of(&cj),
            vec![(json!(30), json!(2)), (json!(40), json!(1))],
            "{cj}"
        );

        // Filter that rejects every group: empty result, not an error.
        let cj = fluree
            .query_cypher(
                db,
                "MATCH (n:Person) WHERE n.age > 100 RETURN n.age, COUNT(*)",
            )
            .await
            .expect("all rejected")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        assert_eq!(
            cj["results"][0]["data"].as_array().expect("rows").len(),
            0,
            "{cj}"
        );

        // IS NULL keeps ONLY the null group (bound-check passes Unbound).
        let cj = fluree
            .query_cypher(
                db,
                "MATCH (n:Person) WHERE n.age IS NULL RETURN n.age, COUNT(*)",
            )
            .await
            .expect("null group only")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        assert_eq!(rows_of(&cj), vec![(json!(null), json!(1))], "{cj}");
    }
}

/// A specific-value lookup (`WHERE n.id = k`, equality object bounds) must
/// stay a point seek under live novelty on the same predicate — seeking the
/// index AND the overlay — and must decline the narrowed seek when the
/// predicate holds mixed numeric types (cross-type equality would miss rows).
#[tokio::test]
async fn cypher_equality_seek_correct_under_predicate_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:eq-seek-novelty";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ctx_json = json!({});
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx_json,
                "@graph": (1..=50).map(|i| json!({
                    "@id": format!("u{i}"), "@type": "User", "id": i
                })).collect::<Vec<_>>()
            }),
        )
        .await
        .expect("seed");
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Live novelty on the SAME predicate: a new subject with a new id value
    // and another with a duplicate of an existing value.
    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload indexed ledger");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx_json,
                "@graph": [
                    {"@id": "t1", "@type": "UserTemp", "id": 999},
                    {"@id": "t2", "@type": "UserTemp", "id": 7},
                ]
            }),
        )
        .await
        .expect("novelty writes")
        .ledger;
    let db = graphdb_from_ledger(&ledger);

    // Indexed value still found.
    let r = fluree
        .query_cypher(&db, "MATCH (n:User) WITH n WHERE n.id = 7 RETURN n")
        .await
        .expect("indexed value");
    assert_eq!(r.row_count(), 1);

    // Novelty-only value found through the narrowed seek (index ∪ overlay).
    let r = fluree
        .query_cypher(&db, "MATCH (n:UserTemp) WITH n WHERE n.id = 999 RETURN n")
        .await
        .expect("novelty value");
    assert_eq!(r.row_count(), 1);

    // The novelty duplicate of an indexed value is also visible.
    let r = fluree
        .query_cypher(&db, "MATCH (n:UserTemp) WHERE n.id = 7 RETURN n")
        .await
        .expect("novelty duplicate");
    assert_eq!(r.row_count(), 1);

    // Mixed numeric types on the predicate: cross-type equality must still
    // match (the narrowed seek declines; the decoded filter coerces).
    let ledger = fluree.ledger(ledger_id).await.expect("reload for mixed dt");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx_json,
                "@id": "t3", "@type": "UserTemp",
                "id": {"@value": "7", "@type": "http://www.w3.org/2001/XMLSchema#double"}
            }),
        )
        .await
        .expect("double id")
        .ledger;
    let db = graphdb_from_ledger(&ledger);
    let r = fluree
        .query_cypher(&db, "MATCH (n:UserTemp) WHERE n.id = 7 RETURN n")
        .await
        .expect("cross-type equality");
    assert_eq!(r.row_count(), 2, "double 7.0 must match integer equality");
}

#[tokio::test]
async fn cypher_vocab_context_resolves_bare_names_to_rdf_iris() {
    // RDF-compat mode: the view's default context supplies `@vocab`, so
    // bare Cypher identifiers resolve to full IRIs and reach RDF-style
    // data — both reads and the hydrated node identity.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:vocab-compat");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("seed");

    // Without @vocab (the default): bare `Person` is a namespace-0 name
    // and does NOT match the RDF-style data.
    let bare_db = graphdb_from_ledger(&committed.ledger);
    let bare = fluree
        .query_cypher(&bare_db, "MATCH (n:Person) RETURN n")
        .await
        .expect("bare query");
    assert_eq!(bare.row_count(), 0, "bare names must not match ex: IRIs");

    // With @vocab: `Person` → `http://example.org/Person`.
    let vocab_db = graphdb_from_ledger(&committed.ledger)
        .with_default_context(Some(json!({"@vocab": "http://example.org/"})));
    let result = fluree
        .query_cypher(&vocab_db, "MATCH (n:Person) RETURN n.name AS name")
        .await
        .expect("vocab query");
    assert_eq!(result.row_count(), 1);
    let rows = fluree
        .query_cypher(&vocab_db, "MATCH (n:Person) RETURN n")
        .await
        .expect("vocab node query");
    let (_, typed) = rows.to_cypher_typed_table(&vocab_db).await.expect("typed");
    let node = typed
        .iter()
        .flat_map(|r| r.iter())
        .find_map(|c| match c {
            fluree_db_api::format::cypher_typed::CypherCell::Node(n) => Some(n),
            _ => None,
        })
        .expect("node cell");
    assert_eq!(
        node.iri.as_ref(),
        "http://example.org/alice",
        "hydrated identity keeps the full IRI in RDF-compat mode"
    );
}

#[tokio::test]
async fn cypher_backticked_names_round_trip_without_splitting() {
    // The namespace-0 placement rule is "no colon → whole name": the IRI
    // splitter (`canonical_split`) must never cut a colon-free Cypher name
    // at `/`, `#`, a space, or an embedded `@` — the write path
    // (sid_for_iri) and the read path (the lowering's namespace-0 arm)
    // both keep it intact, so backticked exotic names round-trip.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:backtick-names");
    let committed = fluree
        .transact_cypher(
            ledger0,
            "CREATE (n:`Weird/Label` {`a/b`: 1, `a#b`: 2, `my prop`: 3, `user@host`: 4})",
        )
        .await
        .expect("create with exotic names");

    let db = graphdb_from_ledger(&committed.ledger);
    let result = fluree
        .query_cypher(
            &db,
            "MATCH (n:`Weird/Label`) \
             RETURN n.`a/b` AS s, n.`a#b` AS h, n.`my prop` AS sp, n.`user@host` AS at",
        )
        .await
        .expect("read exotic names back");
    let (columns, rows) = result.to_cypher_table(&db.snapshot).expect("table");
    assert_eq!(columns, ["s", "h", "sp", "at"]);
    assert_eq!(
        rows,
        vec![vec![json!(1), json!(2), json!(3), json!(4)]],
        "colon-free names must round-trip whole, never namespace-split"
    );

    // A colon-containing backticked name IS namespace-split (the RDF
    // escape hatch): it still round-trips through Cypher, and the same
    // data is reachable RDF-style through a SPARQL prefix.
    let committed = fluree
        .transact_cypher(committed.ledger, "CREATE (n:Coded {`ex:code`: 42})")
        .await
        .expect("create with prefixed name");
    let db = graphdb_from_ledger(&committed.ledger);
    let result = fluree
        .query_cypher(
            &db,
            "MATCH (n:Coded) WHERE n.`ex:code` = 42 RETURN n.`ex:code` AS c",
        )
        .await
        .expect("read prefixed name back");
    assert_eq!(
        result.row_count(),
        1,
        "colon names round-trip via the split"
    );
}

// ============================================================================
// Temporal constructors — date() / datetime() / time() / duration()
// ============================================================================

#[tokio::test]
async fn cypher_temporal_constructors_in_return_and_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:temporal-read");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:Thing {name: "x"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Constant constructors fold at lowering; temporal accessors read back.
    let res = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Thing)
               RETURN datetime('2020-05-06T10:00:00Z').year, date('2024-01-15').month"#,
        )
        .await
        .expect("constructor fold")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([[2020, 1]]), "{res}");
}

#[tokio::test]
async fn cypher_temporal_constructors_write_and_compare() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:temporal-write");

    // Constructors as property values in CREATE.
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (:Event {name: "a", at: datetime("2024-03-04T05:06:07Z"),
                               on: date("2024-03-04"), took: duration("PT2H")}),
                      (:Event {name: "b", at: datetime("2019-01-01T00:00:00Z")})"#,
        )
        .await
        .expect("create with temporal values")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Typed comparison against a constructor constant in WHERE.
    let res = fluree
        .query_cypher(
            &db,
            r#"MATCH (e:Event) WHERE e.at > datetime("2024-01-01T00:00:00Z")
               RETURN e.name, e.at.year"#,
        )
        .await
        .expect("where compare")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([["a", 2024]]), "{res}");

    // The duration value persisted and reads back.
    let took = fluree
        .query_cypher(&db, r#"MATCH (e:Event {name: "a"}) RETURN e.took"#)
        .await
        .expect("duration read");
    assert_eq!(took.row_count(), 1, "duration value present");

    // SET with a constructor value.
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (e:Event {name: "b"}) SET e.on = date("2019-01-01")"#,
        )
        .await
        .expect("set temporal")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let res = fluree
        .query_cypher(&db, r#"MATCH (e:Event {name: "b"}) RETURN e.on.year"#)
        .await
        .expect("set read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([[2019]]), "{res}");
}

#[tokio::test]
async fn cypher_zero_arg_datetime_and_date_write_statement_timestamp() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:temporal-now");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:Tick {n: 1, at: datetime(), on: date()})"#)
        .await
        .expect("create with now")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Both folded to real temporal values at write time.
    let res = fluree
        .query_cypher(
            &db,
            "MATCH (t:Tick) WHERE t.at.year >= 2026 AND t.on.year >= 2026 RETURN t.n",
        )
        .await
        .expect("read back now")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([[1]]), "{res}");

    // Zero-arg constructors also evaluate on the read path.
    let now_rows = fluree
        .query_cypher(&db, "MATCH (t:Tick) WHERE t.at <= datetime() RETURN t.n")
        .await
        .expect("read-side now");
    assert_eq!(now_rows.row_count(), 1, "stored instant is before now()");
}

#[tokio::test]
async fn cypher_temporal_component_map_constructors() {
    // Component maps fold to the same typed values the lexical forms build:
    // in reads (accessors + comparisons) and as write property values.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:temporal-components");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (:Event {name: "a", at: datetime({year: 2024, month: 3, day: 4,
                                                        hour: 5, minute: 6, second: 7}),
                               on: date({year: 2024, month: 3, day: 4}),
                               took: duration({hours: 2, minutes: 30})})"#,
        )
        .await
        .expect("create with component maps")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Component-map constant equals the lexical constant.
    let res = fluree
        .query_cypher(
            &db,
            r#"MATCH (e:Event)
               WHERE e.at = datetime("2024-03-04T05:06:07Z")
                 AND e.on = date("2024-03-04")
               RETURN e.name,
                      date({year: 2024, month: 2}).month,
                      duration({days: 3}) = duration("P3D"),
                      time({hour: 10, minute: 30}).hour"#,
        )
        .await
        .expect("component map read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([["a", 2, true, 10]]), "{res}");

    // Errors are actionable: unknown component / missing year.
    let msg = fluree
        .query_cypher(&db, r#"MATCH (e:Event) RETURN date({year: 2024, dayz: 3})"#)
        .await
        .expect_err("unknown component")
        .to_string();
    assert!(msg.contains("dayz"), "{msg}");
    let msg = fluree
        .query_cypher(&db, r#"MATCH (e:Event) RETURN date({month: 3})"#)
        .await
        .expect_err("missing year")
        .to_string();
    assert!(msg.contains("year"), "{msg}");
}

#[tokio::test]
async fn cypher_zero_arg_localdatetime_is_now() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:localdatetime-now");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:T {name: "x"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let res = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:T)
               RETURN localdatetime() >= datetime("2026-01-01T00:00:00Z")"#,
        )
        .await
        .expect("localdatetime()")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, serde_json::json!([[true]]), "{res}");
}

#[tokio::test]
async fn cypher_temporal_constructor_bad_literal_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:temporal-bad");
    let err = fluree
        .transact_cypher(l, r#"CREATE (:E {at: datetime("not-a-date")})"#)
        .await;
    let msg = format!("{err:?}");
    assert!(err.is_err(), "bad literal must error");
    assert!(
        msg.contains("datetime"),
        "error names the constructor: {msg}"
    );
}

// ============================================================================
// Schema DDL no-ops — CREATE/DROP INDEX|CONSTRAINT, SHOW INDEXES|CONSTRAINTS
// ============================================================================

#[tokio::test]
async fn cypher_schema_ddl_is_a_noop_write() {
    // Framework migrations (spring-data, neo4j-migrations) run index /
    // constraint DDL at startup. Fluree indexes everything: accept as no-ops.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:schema-ddl");
    let t0 = l.t();

    let l = fluree
        .transact_cypher(
            l,
            "CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)",
        )
        .await
        .expect("create index accepted")
        .ledger;
    let l = fluree
        .transact_cypher(
            l,
            "CREATE CONSTRAINT person_id_unique FOR (n:Person) REQUIRE n.id IS UNIQUE",
        )
        .await
        .expect("create constraint accepted")
        .ledger;
    let l = fluree
        .transact_cypher(l, "DROP INDEX person_name IF EXISTS")
        .await
        .expect("drop index accepted")
        .ledger;

    assert_eq!(l.t(), t0, "schema DDL commits nothing");

    // Data writes still work after the DDL no-ops.
    let l = fluree
        .transact_cypher(l, r#"CREATE (:Person {name: "Ada"})"#)
        .await
        .expect("data write")
        .ledger;
    let db = graphdb_from_ledger(&l);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        1
    );
}

#[tokio::test]
async fn cypher_show_indexes_and_constraints_answer_zero_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:show-schema");
    let db = graphdb_from_ledger(&l);

    for stmt in [
        "SHOW INDEXES",
        "SHOW CONSTRAINTS",
        "SHOW INDEXES YIELD name",
    ] {
        let res = fluree
            .query_cypher(&db, stmt)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e:?}"));
        assert_eq!(res.row_count(), 0, "{stmt} answers zero rows");
    }
}

// ============================================================================
// Procedure shims — CALL db.labels() / db.relationshipTypes() /
// db.propertyKeys() / db.schema.visualization() / dbms.components()
// ============================================================================

/// Seed a small property graph: two labels, one relationship, two data
/// properties — all bare ns-0 names, all still in novelty (no index build).
async fn seed_procedure_graph(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
) -> support::MemoryLedger {
    let l = genesis_ledger(fluree, ledger_id);
    fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Ada", age: 36})-[:KNOWS]->(b:Company {name: "Acme"})"#,
        )
        .await
        .expect("seed")
        .ledger
}

/// Flatten a single-column string result into a list (result order).
async fn string_column(
    fluree: &support::MemoryFluree,
    db: &fluree_db_api::GraphDb,
    stmt: &str,
) -> Vec<String> {
    let jsonld = fluree
        .query_cypher(db, stmt)
        .await
        .unwrap_or_else(|e| panic!("{stmt}: {e:?}"))
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    jsonld
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row[0].as_str().expect("string cell").to_string())
        .collect()
}

#[tokio::test]
async fn cypher_call_db_labels_lists_labels_from_novelty() {
    // Neo4j Browser's first act on connect. Data is novelty-only here —
    // the stats merge must see labels that have never been indexed.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_procedure_graph(&fluree, "it/cypher:proc-labels").await;
    let db = graphdb_from_ledger(&l);

    let labels = string_column(&fluree, &db, "CALL db.labels()").await;
    assert_eq!(labels, vec!["Company", "Person"], "sorted distinct labels");

    // YIELD + WHERE + RETURN compose like any read.
    let filtered = string_column(
        &fluree,
        &db,
        r#"CALL db.labels() YIELD label WHERE label STARTS WITH "P" RETURN label"#,
    )
    .await;
    assert_eq!(filtered, vec!["Person"]);

    // YIELD alias renames the visible column.
    let aliased = string_column(
        &fluree,
        &db,
        "CALL db.labels() YIELD label AS l RETURN l ORDER BY l DESC",
    )
    .await;
    assert_eq!(aliased, vec!["Person", "Company"]);
}

#[tokio::test]
async fn cypher_call_relationship_types_and_property_keys_split_by_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_procedure_graph(&fluree, "it/cypher:proc-types-keys").await;
    let db = graphdb_from_ledger(&l);

    // KNOWS is the only ref-object predicate (rdf:type is excluded — it is
    // the label edge, not a user relationship).
    let types = string_column(&fluree, &db, "CALL db.relationshipTypes()").await;
    assert_eq!(types, vec!["KNOWS"]);

    // name/age are literal-object predicates.
    let keys = string_column(&fluree, &db, "CALL db.propertyKeys()").await;
    assert_eq!(keys, vec!["age", "name"]);
}

#[tokio::test]
async fn cypher_call_procedures_answer_from_head_index_stats_too() {
    // Same answers once the data is indexed and novelty is empty.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:proc-indexed";
    seed_procedure_graph(&fluree, ledger_id).await;
    rebuild_and_publish_index(&fluree, ledger_id).await;
    let db = fluree.db(ledger_id).await.expect("indexed view");

    let labels = string_column(&fluree, &db, "CALL db.labels()").await;
    assert_eq!(labels, vec!["Company", "Person"]);
    let types = string_column(&fluree, &db, "CALL db.relationshipTypes()").await;
    assert_eq!(types, vec!["KNOWS"]);
    let keys = string_column(&fluree, &db, "CALL db.propertyKeys()").await;
    assert_eq!(keys, vec!["age", "name"]);
}

#[tokio::test]
async fn cypher_call_dbms_components_reports_compat_identity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:proc-components");
    let db = graphdb_from_ledger(&l);

    let jsonld = fluree
        .query_cypher(&db, "CALL dbms.components() YIELD name, versions, edition")
        .await
        .expect("components")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = jsonld.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{jsonld}");
    assert_eq!(rows[0][0].as_str(), Some("Neo4j Kernel"), "{jsonld}");
    let versions = rows[0][1].as_array().expect("versions list");
    assert_eq!(versions.len(), 1, "{jsonld}");
    assert!(
        rows[0][2].as_str().unwrap_or_default().contains("Fluree"),
        "edition carries Fluree attribution: {jsonld}"
    );
}

#[tokio::test]
async fn cypher_call_db_schema_visualization_returns_one_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_procedure_graph(&fluree, "it/cypher:proc-schema-viz").await;
    let db = graphdb_from_ledger(&l);

    let jsonld = fluree
        .query_cypher(&db, "CALL db.schema.visualization()")
        .await
        .expect("schema viz")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = jsonld.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{jsonld}");
    let nodes = rows[0][0].as_array().expect("nodes list");
    assert_eq!(nodes.len(), 2, "one entry per label: {jsonld}");
    let rels = rows[0][1].as_array().expect("relationships list");
    assert_eq!(rels.len(), 1, "one entry per rel type: {jsonld}");
}

#[tokio::test]
async fn cypher_call_apoc_meta_data_attributes_schema_per_label() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_procedure_graph(&fluree, "it/cypher:proc-meta-data").await;
    let db = graphdb_from_ledger(&l);

    let jsonld = fluree
        .query_cypher(
            &db,
            "CALL apoc.meta.data() YIELD label, property, count, type, other, elementType \
             RETURN label, property, type, other ORDER BY label, property",
        )
        .await
        .expect("meta.data")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let rows = jsonld.as_array().expect("rows");

    // Company: name (STRING). Person: age (INTEGER), name (STRING),
    // KNOWS (RELATIONSHIP → Company).
    let flat: Vec<(String, String, String)> = rows
        .iter()
        .map(|r| {
            (
                r[0].as_str().unwrap().to_string(),
                r[1].as_str().unwrap().to_string(),
                r[2].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        flat,
        vec![
            ("Company".into(), "name".into(), "STRING".into()),
            ("Person".into(), "KNOWS".into(), "RELATIONSHIP".into()),
            ("Person".into(), "age".into(), "INTEGER".into()),
            ("Person".into(), "name".into(), "STRING".into()),
        ],
        "{jsonld}"
    );
    let knows_other = rows[1][3].as_array().expect("other list");
    assert_eq!(knows_other, &[json!("Company")], "{jsonld}");
}

#[tokio::test]
async fn cypher_call_apoc_meta_data_answers_langchain_schema_queries() {
    // The three exact queries LangChain's Neo4jGraph issues to build its
    // schema description — the reason this shim exists.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = seed_procedure_graph(&fluree, "it/cypher:proc-langchain").await;
    let db = graphdb_from_ledger(&l);

    let run = |stmt: &'static str| {
        let fluree = &fluree;
        let db = &db;
        async move {
            fluree
                .query_cypher(db, stmt)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {e:?}"))
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld")
        }
    };

    // Node properties.
    let node_props = run("CALL apoc.meta.data() \
         YIELD label, other, elementType, type, property \
         WHERE NOT type = \"RELATIONSHIP\" AND elementType = \"node\" \
         WITH label AS nodeLabels, collect({property:property, type:type}) AS properties \
         RETURN {labels: nodeLabels, properties: properties} AS output")
    .await;
    let rows = node_props.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "one output per label: {node_props}");
    let person = rows
        .iter()
        .map(|r| &r[0])
        .find(|o| o["labels"] == json!("Person"))
        .unwrap_or_else(|| panic!("Person output missing: {node_props}"));
    let props = person["properties"].as_array().expect("properties");
    assert!(
        props.contains(&json!({"property": "age", "type": "INTEGER"}))
            && props.contains(&json!({"property": "name", "type": "STRING"})),
        "{node_props}"
    );

    // Relationship properties (edge-annotation attribution not emitted —
    // zero rows, which LangChain tolerates).
    let rel_props = run("CALL apoc.meta.data() \
         YIELD label, other, elementType, type, property \
         WHERE NOT type = \"RELATIONSHIP\" AND elementType = \"relationship\" \
         WITH label AS nodeLabels, collect({property:property, type:type}) AS properties \
         RETURN {type: nodeLabels, properties: properties} AS output")
    .await;
    assert_eq!(rel_props.as_array().expect("rows").len(), 0, "{rel_props}");

    // Relationships (start)-[type]->(end).
    let rels = run("CALL apoc.meta.data() \
         YIELD label, other, elementType, type, property \
         WHERE type = \"RELATIONSHIP\" AND elementType = \"node\" \
         UNWIND other AS other_node \
         RETURN {start: label, type: property, end: toString(other_node)} AS output")
    .await;
    let rows = rels.as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{rels}");
    assert_eq!(
        rows[0][0],
        json!({"start": "Person", "type": "KNOWS", "end": "Company"}),
        "{rels}"
    );
}

#[tokio::test]
async fn cypher_exists_in_projection_evaluates_per_row() {
    // EXISTS { pattern } in RETURN projection (IC7/IC10 shape) must evaluate
    // per row, not constant-false.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:exists-proj");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:P {name: "A"})-[:K]->(b:P {name: "B"}); CREATE (:P {name: "C"});"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:P) RETURN n.name AS name, EXISTS { (n)-[:K]->() } AS has ORDER BY name"#,
        )
        .await
        .expect("projected exists")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        out,
        json!([["A", true], ["B", false], ["C", false]]),
        "EXISTS evaluates per row in projection position"
    );
}

#[tokio::test]
async fn cypher_exists_in_projection_with_both_endpoints_bound() {
    // Regression: an EXISTS whose inner pattern correlates on MULTIPLE bound
    // vars used to be swallowed by the fused join block's synchronous eval,
    // collapsing to constant false (the IC7 shape). It must resolve per row —
    // bare, inside CASE, and in a WITH projection.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:exists-both-bound");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:P {name: "A"})-[:K]->(b:P {name: "B"}); CREATE (:P {name: "C"});"#,
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Directed and undirected, both endpoints bound.
    for pattern in ["(a)-[:K]->(x)", "(a)-[:K]-(x)"] {
        let stmt = format!(
            r#"MATCH (a:P {{name: "A"}}), (x:P) WHERE x.name <> "A"
               RETURN x.name AS name, EXISTS {{ {pattern} }} AS knows ORDER BY name"#
        );
        let out = fluree
            .query_cypher(&db, &stmt)
            .await
            .expect("both-bound exists")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        assert_eq!(out, json!([["B", true], ["C", false]]), "{pattern}");
    }

    // Inside CASE in projection.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:P {name: "A"}), (x:P) WHERE x.name <> "A"
               RETURN x.name AS name,
                      CASE WHEN EXISTS { (a)-[:K]-(x) } THEN 1 ELSE 0 END AS knows
               ORDER BY name"#,
        )
        .await
        .expect("case exists")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([["B", 1], ["C", 0]]));

    // In a WITH projection.
    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:P) WITH n.name AS name, EXISTS { (n)-[:K]->() } AS has
               RETURN name, has ORDER BY name"#,
        )
        .await
        .expect("with exists")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([["A", true], ["B", false], ["C", false]]));
}

#[tokio::test]
async fn cypher_foreach_unrolls_constant_lists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:foreach");

    // Pure CREATE body: one distinct node per element.
    let l = fluree
        .transact_cypher(l, r#"FOREACH (x IN [1, 2, 3] | CREATE (:N {v: x}))"#)
        .await
        .expect("foreach create")
        .ledger;
    // range() as the list; a MATCH-bound variable stays referenced in the body.
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (n:N {v: 1}) FOREACH (i IN range(1, 2) | CREATE (n)-[:T]->(:M {i: i}))"#,
        )
        .await
        .expect("foreach edges from bound node")
        .ledger;
    // SET body applies per element (last write wins on the same property).
    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (n:N {v: 2}) FOREACH (x IN ["a", "b"] | SET n.mark = x)"#,
        )
        .await
        .expect("foreach set")
        .ledger;

    let db = graphdb_from_ledger(&l);
    let counts = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:N) WITH count(n) AS nodes
               MATCH (:N {v: 1})-[:T]->(m:M)
               RETURN nodes, count(m) AS edges"#,
        )
        .await
        .expect("read back")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        counts,
        json!([[3, 2]]),
        "3 nodes; 2 edges from the bound node"
    );
    let mark = fluree
        .query_cypher(&db, r#"MATCH (n:N {v: 2}) RETURN n.mark"#)
        .await
        .expect("mark")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(mark, json!([["b"]]), "SET per element, last wins");

    // Runtime lists are deferred with a clear error.
    let msg = fluree
        .transact_cypher(l, r#"MATCH (n:N) FOREACH (x IN n.list | SET n.y = x)"#)
        .await
        .expect_err("runtime list")
        .to_string();
    assert!(msg.contains("FOREACH"), "{msg}");
}

#[tokio::test]
async fn cypher_null_literal_in_expressions() {
    // `null` is a first-class expression value: projected as JSON null,
    // never equal to anything, detected by IS NULL, skipped by coalesce.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:null-lit");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:P {name: "A"})"#)
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let out = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:P)
               RETURN null AS nothing,
                      coalesce(null, n.name) AS name,
                      CASE WHEN n.name = "Z" THEN 1 ELSE null END AS via_case,
                      null IS NULL AS yes"#,
        )
        .await
        .expect("null literal expressions")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([[null, "A", null, true]]), "{out}");

    // Comparison with null is never true: the row drops.
    assert_eq!(
        fluree
            .query_cypher(&db, r#"MATCH (n:P) WHERE n.name = null RETURN n"#)
            .await
            .expect("null comparison")
            .row_count(),
        0,
        "= null matches nothing"
    );

    // Null inside a list literal survives as a null element.
    let out = fluree
        .query_cypher(&db, r#"MATCH (n:P) RETURN [1, null, 2] AS xs"#)
        .await
        .expect("null in list")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(out, json!([[[1, null, 2]]]), "{out}");
}

#[tokio::test]
async fn cypher_call_procedure_errors_are_actionable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:proc-errors");
    let db = graphdb_from_ledger(&l);

    // Unknown procedure names the supported set.
    let msg = fluree
        .query_cypher(&db, "CALL apoc.load.json()")
        .await
        .expect_err("unknown procedure")
        .to_string();
    assert!(
        msg.contains("apoc.load.json") && msg.contains("db.labels"),
        "unknown-procedure error lists the shims: {msg}"
    );

    // Unknown YIELD column names the real columns.
    let msg = fluree
        .query_cypher(&db, "CALL db.labels() YIELD nope")
        .await
        .expect_err("unknown yield column")
        .to_string();
    assert!(
        msg.contains("nope") && msg.contains("label"),
        "yield error names the columns: {msg}"
    );

    // Args on a no-arg shim.
    let msg = fluree
        .query_cypher(&db, r#"CALL db.labels("x")"#)
        .await
        .expect_err("args rejected")
        .to_string();
    assert!(msg.contains("no arguments"), "{msg}");

    // Procedure calls only stand alone (first clause).
    let msg = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) CALL db.labels() YIELD label RETURN label",
        )
        .await
        .expect_err("mid-query procedure")
        .to_string();
    assert!(
        msg.contains("first clause"),
        "mid-query procedure is a clear parse error: {msg}"
    );
}

#[tokio::test]
async fn cypher_keyword_alias_names_output_column_and_binds_downstream() {
    // Keyword tokens (`count`, `end`, `order`, …) are accepted as binding
    // names — a deliberate leniency over strict openCypher. The alias must
    // both name the output column and remain referenceable downstream.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:keyword-alias");
    let l = fluree
        .insert(
            l,
            &json!({"@context": ctx(), "@id": "alice", "@type": "Person", "name": "Alice"}),
        )
        .await
        .expect("seed")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Terminal RETURN alias named with a keyword → that column name.
    let cj = fluree
        .query_cypher(&db, r#"MATCH (n:Person) RETURN n.name AS end"#)
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(cj["results"][0]["columns"], json!(["end"]), "{cj}");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!("Alice"),
        "{cj}"
    );

    // WITH-aliased keyword aggregate, referenced in a later WHERE and RETURN.
    let cj = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Person) WITH count(*) AS count WHERE count > 0 RETURN count"#,
        )
        .await
        .expect("query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(cj["results"][0]["columns"], json!(["count"]), "{cj}");
    assert_eq!(cj["results"][0]["data"][0]["row"][0], json!(1), "{cj}");
}

#[tokio::test]
async fn transact_cypher_merge_relationship_per_row_on_match_set() {
    // Per-row relationship MERGE where the rows split across both branches:
    // ON MATCH SET fires on the pre-existing edge, ON CREATE SET on the newly
    // created one — atomically, in a single commit.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-per-row-onmatch");

    // Seed Alice, Bob, Carol and one existing Alice-KNOWS->Bob edge.
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}),
                      (c:Person {name: "Carol"})"#,
        )
        .await
        .expect("seed")
        .ledger;
    let seed_t = l.t();

    // MERGE Alice→(everyone else): Alice->Bob already exists (ON MATCH), Alice->
    // Carol is missing (ON CREATE). One statement, one commit.
    let result = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {name: "Alice"}), (b:Person) WHERE b.name <> "Alice"
               MERGE (a)-[:KNOWS]->(b)
               ON CREATE SET b.tag = "created"
               ON MATCH SET b.tag = "matched""#,
        )
        .await
        .expect("per-row merge with on-match/on-create");

    assert_eq!(
        result.ledger.t(),
        seed_t + 1,
        "the two branches publish as a single commit (t advances by exactly one)"
    );

    let db = graphdb_from_ledger(&result.ledger);

    // Exactly two edges: the pre-existing Alice->Bob plus the created Alice->Carol.
    let edges = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person)
               RETURN b.name AS name, b.tag AS tag ORDER BY name"#,
        )
        .await
        .expect("edges")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        edges,
        json!([["Bob", "matched"], ["Carol", "created"]]),
        "ON MATCH tags the pre-existing edge's endpoint; ON CREATE tags the new one: {edges}"
    );

    // Alice was never a MERGE tail, so it carries no tag.
    let alice_tagged = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"}) WHERE a.tag IS NOT NULL RETURN a"#,
        )
        .await
        .expect("alice")
        .row_count();
    assert_eq!(
        alice_tagged, 0,
        "the head endpoint is untouched by either branch"
    );
}

#[tokio::test]
async fn transact_cypher_merge_relationship_per_row_on_match_only_all_existing() {
    // When every matched row already has the edge, only ON MATCH SET fires and
    // no new edges are created — still a single commit.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:merge-rel-per-row-onmatch-only");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
        )
        .await
        .expect("seed")
        .ledger;

    let l = fluree
        .transact_cypher(
            l,
            r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
               MERGE (a)-[:KNOWS]->(b)
               ON CREATE SET b.tag = "created"
               ON MATCH SET b.tag = "matched""#,
        )
        .await
        .expect("per-row merge all-existing")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let rows = fluree
        .query_cypher(
            &db,
            r#"MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person) RETURN b.tag AS tag"#,
        )
        .await
        .expect("rows")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(rows, json!([["matched"]]), "only ON MATCH fired: {rows}");
}

#[tokio::test]
async fn transact_cypher_unwind_batch_node_merge_upsert() {
    // The `LOAD CSV` shape: per-row node find-or-create keyed on a unique id,
    // with a trailing SET applied on both branches. Two passes prove upsert:
    // first pass creates, second pass updates without duplicating.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:unwind-node-merge");

    let batch1 = json!({ "batch": [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]});
    let stmt = "UNWIND $batch AS row \
                MERGE (n:Person {id: row.id}) \
                SET n.name = row.name";

    let seed_t = l.t();
    let res = fluree
        .transact_cypher_with_params(l, stmt, batch1.as_object())
        .await
        .expect("first upsert pass");
    assert_eq!(res.ledger.t(), seed_t + 1, "one batch → one commit");
    let db = graphdb_from_ledger(&res.ledger);
    assert_eq!(
        fluree
            .query_cypher(&db, "MATCH (n:Person) RETURN n")
            .await
            .unwrap()
            .row_count(),
        2,
        "first pass creates both nodes"
    );

    // Second pass: id 1 already exists (update name), id 3 is new (create).
    let batch2 = json!({ "batch": [
        {"id": "1", "name": "Alice2"},
        {"id": "3", "name": "Carol"},
    ]});
    let res = fluree
        .transact_cypher_with_params(res.ledger, stmt, batch2.as_object())
        .await
        .expect("second upsert pass");
    let db = graphdb_from_ledger(&res.ledger);

    let rows = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) RETURN n.id AS id, n.name AS name ORDER BY id",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rows,
        json!([["1", "Alice2"], ["2", "Bob"], ["3", "Carol"]]),
        "id 1 updated in place (no duplicate), id 3 created, id 2 untouched: {rows}"
    );
}

#[tokio::test]
async fn transact_cypher_unwind_batch_node_merge_on_create_on_match() {
    // Per-row node upsert with distinct ON CREATE vs ON MATCH branches.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cypher:unwind-node-merge-oncreate");
    // Seed id 1 so the next pass matches it and creates id 2.
    let l = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $batch AS row MERGE (n:Person {id: row.id})",
            json!({"batch": [{"id": "1"}]}).as_object(),
        )
        .await
        .expect("seed")
        .ledger;

    let l = fluree
        .transact_cypher_with_params(
            l,
            "UNWIND $batch AS row MERGE (n:Person {id: row.id}) \
             ON CREATE SET n.origin = 'created' \
             ON MATCH SET n.origin = 'matched'",
            json!({"batch": [{"id": "1"}, {"id": "2"}]}).as_object(),
        )
        .await
        .expect("upsert with on-create/on-match")
        .ledger;
    let db = graphdb_from_ledger(&l);
    let rows = fluree
        .query_cypher(
            &db,
            "MATCH (n:Person) RETURN n.id AS id, n.origin AS origin ORDER BY id",
        )
        .await
        .expect("read")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(
        rows,
        json!([["1", "matched"], ["2", "created"]]),
        "existing id 1 took ON MATCH; new id 2 took ON CREATE: {rows}"
    );
}
