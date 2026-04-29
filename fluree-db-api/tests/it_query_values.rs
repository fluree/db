//! VALUES query integration tests
//!
//! Uses explicit `@context` on every insert/query.
//!
//! Notes:
//! - Federated query behavior (`query-connection` + `:from`) is covered.
//! - VALUES inside multi-pattern OPTIONAL is supported.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_values_dataset(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Local explicit context: mirror the values-test usage (flur.ee + default context + ex).
    // For Rust, we keep it explicit and minimal for what these tests need.
    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:brian","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9],"ex:friend":[{"@id":"ex:brian"}]},
            {"@id":"ex:cam","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"}]},
            {"@id":"ex:liam","schema:name":"Liam","schema:email":"liam@example.org","schema:age":13,"ex:favNums":[42,11],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"},{"@id":"ex:cam"}]},
            {"@id":"ex:nikola",
             "schema:name":"Nikola",
             "ex:greeting":[{"@value":"Здраво","@language":"sb"},{"@value":"Hello","@language":"en"}],
             "ex:birthday":{"@value":"2000-01-01","@type":"xsd:datetime"},
             "ex:cool":true}
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn values_top_level_no_where_multiple_vars() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "select": ["?foo", "?bar"],
        "values": [["?foo", "?bar"],
                   [["foo1","bar1"],["foo2","bar2"],["foo3","bar3"]]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["foo1", "bar1"],
            ["foo2", "bar2"],
            ["foo3", "bar3"]
        ]))
    );
}

#[tokio::test]
async fn values_top_level_no_where_single_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "select": "?foo",
        "values": ["?foo", ["foo1","foo2","foo3"]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!(["foo1", "foo2", "foo3"]))
    );
}

#[tokio::test]
async fn values_top_level_iri_values_constrain_where() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": ["?name","?email"],
        "values": ["?s", [
            {"@value":"ex:brian","@type":"@id"},
            {"@value":"ex:cam","@type":"@id"}
        ]],
        "where": [
            {"@id":"?s","schema:name":"?name"},
            {"@id":"?s","schema:email":"?email"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "brian@example.org"],
            ["Cam", "cam@example.org"]
        ]))
    );
}

#[tokio::test]
async fn values_equivalent_iri_forms_var_in_id_map() {
    // Mirrors the three "equivalent syntactic forms" checks.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // baseline: IRI literal in pattern
    let q1 = json!({
        "@context": ctx,
        "where": [{"@id":"?s","ex:friend":{"@id":"ex:alice"}}],
        "select": "?s"
    });

    // variable via VALUES
    let q2 = json!({
        "@context": ctx,
        "values": ["?friend", [{"@value":"ex:alice","@type":"@id"}]],
        "where": [{"@id":"?s","ex:friend":"?friend"}],
        "select": "?s"
    });

    // variable inside id-map
    let q3 = json!({
        "@context": ctx,
        "values": ["?friend", [{"@value":"ex:alice","@type":"@id"}]],
        "where": [{"@id":"?s","ex:friend":{"@id":"?friend"}}],
        "select": "?s"
    });

    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let r3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    assert_eq!(
        normalize_rows(&r1),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
    assert_eq!(
        normalize_rows(&r2),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
    assert_eq!(
        normalize_rows(&r3),
        normalize_rows(&json!(["ex:cam", "ex:liam"]))
    );
}

#[tokio::test]
async fn values_where_clause_keyword_single_var() {
    // VALUES nested in WHERE: ["values", ["?s", [{@type:"@id",@value:"ex:cam"}, ...]]]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": ["?name","?email"],
        "where": [
            {"@id":"?s","schema:name":"?name"},
            {"@id":"?s","schema:email":"?email"},
            ["values", ["?s", [
                {"@type":"@id","@value":"ex:cam"},
                {"@type":"@id","@value":"ex:brian"}
            ]]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", "brian@example.org"],
            ["Cam", "cam@example.org"]
        ]))
    );
}

#[tokio::test]
async fn values_nested_under_optional_clause() {
    // Tests multi-pattern OPTIONAL containing both triple patterns and VALUES
    // The OPTIONAL contains: triple pattern + VALUES clause
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    // Query all users, optionally get the name and cool status for Nikola only
    let query = json!({
        "@context": ctx,
        "select": ["?s", "?name", "?cool"],
        "where": [
            {"@id": "?s", "schema:email": "?email"},
            ["optional",
                {"@id": "?s", "schema:name": "?name", "ex:cool": "?cool"},
                ["values", ["?s", [{"@type": "@id", "@value": "ex:nikola"}]]]
            ]
        ],
        "orderBy": "?s"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Only nikola matches the OPTIONAL (VALUES constrains to nikola, and nikola has ex:cool)
    // Other users have email so they match the required pattern, but don't match the OPTIONAL
    // because VALUES constrains ?s to nikola only
    assert_eq!(
        json_rows,
        json!([
            ["ex:alice", null, null],
            ["ex:brian", null, null],
            ["ex:cam", null, null],
            ["ex:liam", null, null]
        ])
    );
}

#[tokio::test]
async fn values_match_meta_language_tag() {
    // Scenario: match meta (language tag) => ["ex:nikola"]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": "?s",
        "where": [
            {"@id":"?s","ex:greeting":"?greet"},
            ["values", ["?greet", [{"@value":"Здраво","@language":"sb"}]]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(json_rows, json!(["ex:nikola"]));
}

#[tokio::test]
async fn values_with_empty_solution_seed() {
    // Scenario: VALUES first, then match by name.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_values_dataset(&fluree, "values-test:main").await;

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "where": [
            ["values", ["?name", ["Liam", "Cam"]]],
            {"@id":"?s","schema:name":"?name"}
        ],
        "select": ["?s","?name"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([["ex:cam", "Cam"], ["ex:liam", "Liam"]]))
    );
}

#[tokio::test]
async fn values_federated_query_connection_from_two_ledgers() {
    // Scenario: federated VALUES across two ledgers via query_connection.
    let fluree = FlureeBuilder::memory().build_memory();
    let _ = seed_values_dataset(&fluree, "values-test:main").await;

    // Seed second ledger with a single person.
    let other_ledger0 = genesis_ledger(&fluree, "other-ledger:main");
    let other_insert = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.com/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:khris","schema:name":"Khris"}
        ]
    });
    let _ = fluree
        .insert(other_ledger0, &other_insert)
        .await
        .expect("insert other-ledger");

    let ctx = json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "from": ["values-test:main", "other-ledger:main"],
        "select": "?name",
        "where": [
            {"@id":"?s","schema:name":"?name"},
            ["values", ["?s", [
                {"@type":"@id","@value":"ex:nikola"},
                {"@type":"@id","@value":"ex:khris"}
            ]]]
        ],
        "orderBy": "?name"
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("values-test:main").await.expect("ledger");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!(["Khris", "Nikola"]))
    );
}
