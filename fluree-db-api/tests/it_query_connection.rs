//! Connection-scoped query integration tests
//!
//! Focus:
//! - `query_connection` with `"from"` (combined datasets)
//! - `query_connection` with `"fromNamed"` + `["graph", ...]` patterns (separate named graphs)

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{
    assert_index_defaults, context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree,
    MemoryLedger,
};

fn ctx_schema() -> serde_json::Value {
    json!({
        "id": "@id",
        "type": "@type",
        "schema": "https://schema.org/"
    })
}

fn ctx_schema_value() -> serde_json::Value {
    json!([
        "https://schema.org",
        {
            "id": "@id",
            "type": "@type",
            "value": "@value",
            "schema": "https://schema.org/"
        }
    ])
}

async fn seed_federated_ledgers(fluree: &MemoryFluree) {
    // Authors
    let _ = fluree
        .insert(
            genesis_ledger(fluree, "test/authors:main"),
            &json!({
                "@context": ["https://schema.org", ctx_schema()],
                "@graph": [
                    {"@id":"https://www.wikidata.org/wiki/Q42","@type":"Person","name":"Douglas Adams"},
                    {"@id":"https://www.wikidata.org/wiki/Q173540","@type":"Person","name":"Margaret Mitchell"}
                ]
            }),
        )
        .await
        .expect("insert authors");

    // Books
    let _ = fluree
        .insert(
            genesis_ledger(fluree, "test/books:main"),
            &json!({
                "@context": ["https://schema.org", ctx_schema()],
                "@graph": [
                    {"@id":"https://www.wikidata.org/wiki/Q3107329","@type":["Book"],"name":"The Hitchhiker's Guide to the Galaxy","isbn":"0-330-25864-8","author":{"@id":"https://www.wikidata.org/wiki/Q42"}},
                    {"@id":"https://www.wikidata.org/wiki/Q2870","@type":["Book"],"name":"Gone with the Wind","isbn":"0-582-41805-4","author":{"@id":"https://www.wikidata.org/wiki/Q173540"}}
                ]
            }),
        )
        .await
        .expect("insert books");

    // Movies
    let _ = fluree
        .insert(
            genesis_ledger(fluree, "test/movies:main"),
            &json!({
                "@context": ["https://schema.org", ctx_schema()],
                "@graph": [
                    {"@id":"https://www.wikidata.org/wiki/Q836821","@type":["Movie"],"name":"The Hitchhiker's Guide to the Galaxy","isBasedOn":{"@id":"https://www.wikidata.org/wiki/Q3107329"}},
                    {"@id":"https://www.wikidata.org/wiki/Q2875","@type":["Movie"],"name":"Gone with the Wind","isBasedOn":{"@id":"https://www.wikidata.org/wiki/Q2870"}}
                ]
            }),
        )
        .await
        .expect("insert movies");
}

fn normalize_flat_results(v: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut items: Vec<serde_json::Value> = v.as_array().expect("expected JSON array").to_vec();
    items.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default())
    });
    items
}

async fn seed_people_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": context_ex_schema(),
        "@graph": [
            {"@id":"ex:alice","@type":"ex:Person","schema:name":"Alice","schema:age":30},
            {"@id":"ex:bob","@type":"ex:Person","schema:name":"Bob","schema:age":25}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger
}

async fn seed_people2_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": context_ex_schema(),
        "@graph": [
            {"@id":"ex:charlie","@type":"ex:Person","schema:name":"Charlie","schema:age":35},
            {"@id":"ex:diana","@type":"ex:Person","schema:name":"Diana","schema:age":28}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger
}

#[tokio::test]
async fn query_connection_from_combined_datasets_selecting_subgraphs_depth_3() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_federated_ledgers(&fluree).await;

    let q = json!({
        "@context": ctx_schema_value(),
        "from": ["test/authors:main", "test/books:main", "test/movies:main"],
        "select": { "?goneWithTheWind": ["*"] },
        "depth": 3,
        "where": {
            "@id": "?goneWithTheWind",
            "type": "Movie",
            "name": "Gone with the Wind"
        }
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let ledger = fluree.ledger("test/movies:main").await.expect("ledger");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([{
            "@id": "https://www.wikidata.org/wiki/Q2875",
            "@type": "Movie",
            "name": "Gone with the Wind",
            "isBasedOn": {
                "@id": "https://www.wikidata.org/wiki/Q2870",
                "@type": "Book",
                "name": "Gone with the Wind",
                "isbn": "0-582-41805-4",
                "author": {
                    "@id": "https://www.wikidata.org/wiki/Q173540",
                    "@type": "Person",
                    "name": "Margaret Mitchell"
                }
            }
        }]))
    );
}

#[tokio::test]
async fn query_connection_from_named_selecting_subgraphs_depth_3() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_federated_ledgers(&fluree).await;

    let q = json!({
        "@context": ctx_schema_value(),
        "fromNamed": ["test/authors:main", "test/books:main", "test/movies:main"],
        "select": { "?goneWithTheWind": ["*"] },
        "depth": 3,
        "where": [
            ["graph", "test/movies:main",
                {"@id": "?goneWithTheWind", "name": "Gone with the Wind"}
            ]
        ]
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let ledger = fluree.ledger("test/movies:main").await.expect("ledger");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([{
            "@id": "https://www.wikidata.org/wiki/Q2875",
            "@type": "Movie",
            "name": "Gone with the Wind",
            "isBasedOn": {
                "@id": "https://www.wikidata.org/wiki/Q2870",
                "@type": "Book",
                "name": "Gone with the Wind",
                "isbn": "0-582-41805-4",
                "author": {
                    "@id": "https://www.wikidata.org/wiki/Q173540",
                    "@type": "Person",
                    "name": "Margaret Mitchell"
                }
            }
        }]))
    );
}

#[tokio::test]
async fn query_connection_from_combined_datasets_direct_select_vars() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_federated_ledgers(&fluree).await;

    let q = json!({
        "@context": "https://schema.org",
        "from": ["test/authors:main", "test/books:main", "test/movies:main"],
        "select": ["?movieName", "?bookIsbn", "?authorName"],
        "where": {
            "type": "Movie",
            "name": "?movieName",
            "isBasedOn": {
                "isbn": "?bookIsbn",
                "author": {"name": "?authorName"}
            }
        }
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let ledger = fluree.ledger("test/movies:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["Gone with the Wind", "0-582-41805-4", "Margaret Mitchell"],
            [
                "The Hitchhiker's Guide to the Galaxy",
                "0-330-25864-8",
                "Douglas Adams"
            ]
        ]))
    );
}

#[tokio::test]
async fn query_connection_from_named_with_graph_patterns() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_federated_ledgers(&fluree).await;

    // Equivalent:
    // :fromNamed ["test/authors" "test/books" "test/movies"]
    // :where [[:graph "test/movies" {...}] [:graph "test/books" {...}] [:graph "test/authors" {...}]]
    //
    // Rust JSON-LD WHERE uses ["graph", graphNameOrVar, pattern1, pattern2...]
    let q = json!({
        "@context": "https://schema.org",
        "fromNamed": ["test/authors:main", "test/books:main", "test/movies:main"],
        "select": ["?movieName", "?bookIsbn", "?authorName"],
        "where": [
            ["graph", "test/movies:main",
                {"@id":"?movie","type":"Movie","name":"?movieName","isBasedOn":"?book"}
            ],
            ["graph", "test/books:main",
                {"@id":"?book","isbn":"?bookIsbn","author":"?author"}
            ],
            ["graph", "test/authors:main",
                {"@id":"?author","name":"?authorName"}
            ]
        ]
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let ledger = fluree.ledger("test/movies:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["Gone with the Wind", "0-582-41805-4", "Margaret Mitchell"],
            [
                "The Hitchhiker's Guide to the Galaxy",
                "0-330-25864-8",
                "Douglas Adams"
            ]
        ]))
    );
}

#[tokio::test]
async fn query_connection_single_ledger_from_top_level() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "from": "people:main",
        "select": "?name",
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection should succeed");

    // Use the ledger we loaded above for formatting
    let ledger = fluree.ledger("people:main").await.expect("ledger load");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

#[tokio::test]
async fn query_connection_multiple_default_graphs_union() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger1 = seed_people_ledger(&fluree, "people1:main").await;
    let _ledger2 = seed_people2_ledger(&fluree, "people2:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "from": ["people1:main", "people2:main"],
        "select": "?name",
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection should succeed");

    // Format using an arbitrary ledger's DB (encoding is consistent across these test ledgers)
    let ledger = fluree.ledger("people1:main").await.expect("ledger load");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob", "Charlie", "Diana"]))
    );
}

#[tokio::test]
async fn query_connection_uses_opts_ledger_fallback() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "opts": { "ledger": "people:main" },
        "select": "?name",
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection should succeed");

    let ledger = fluree.ledger("people:main").await.expect("ledger load");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_flat_results(&jsonld),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );
}

#[tokio::test]
async fn query_connection_missing_dataset_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let query = json!({
        "@context": context_ex_schema(),
        "select": ["?s"],
        "where": {"@id": "?s"}
    });

    let err = fluree.query_connection(&query).await.unwrap_err();
    assert!(
        err.to_string().contains("Missing ledger specification"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn query_connection_policy_identity_not_found_returns_empty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    // Use an identity that doesn't exist in the database.
    // With no inline default-allow, QueryConnectionOptions defaults to `default_allow: false`.
    // Unknown identity → empty restrictions → default-allow:false denies every row → 0 results.
    // The point of this test is that an unresolvable identity IRI does not surface as a 500;
    // it produces a clean empty result set.
    let query = json!({
        "@context": context_ex_schema(),
        "from": "people:main",
        "opts": { "identity": "ex:alice" },
        "select": ["?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query should succeed");
    // Unknown identity → no matching policies → default-allow: false → no rows returned
    let total_rows: usize = result.batches.iter().map(fluree_db_api::Batch::len).sum();
    assert_eq!(
        total_rows, 0,
        "unknown identity should return no results (fail-closed)"
    );
}

#[tokio::test]
async fn query_connection_sparql_uses_from_clause() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    let sparql = r"
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>
SELECT ?name
FROM <people:main>
WHERE {
  ?person a ex:Person ;
          schema:name ?name .
}
";

    let result = fluree
        .query_connection_sparql(sparql)
        .await
        .expect("query_connection_sparql should succeed");

    let ledger = fluree.ledger("people:main").await.expect("ledger load");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // SPARQL queries always return array-of-arrays, even for single-var selects.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Alice"], ["Bob"]]))
    );
}

#[tokio::test]
async fn query_connection_jsonld_tracked_single_ledger() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "from": "people:main",
        "select": "?name",
        "where": {
            "@id": "?person",
            "@type": "ex:Person",
            "schema:name": "?name"
        }
    });

    let tracked = fluree
        .query_connection_tracked(&query)
        .await
        .expect("query_connection_tracked should succeed");

    // Tracked response returns formatted result directly
    assert_eq!(
        normalize_flat_results(&tracked.result),
        normalize_flat_results(&json!(["Alice", "Bob"]))
    );

    // Verify response status is successful
    assert_eq!(tracked.status, 200);

    // Verify fuel tracking returns a positive decimal value
    assert!(
        tracked.fuel.is_some(),
        "fuel should be present in tracked response"
    );
    assert!(tracked.fuel.unwrap() > 0.0, "fuel should be positive");
}

#[tokio::test]
async fn query_connection_sparql_tracked_uses_from_clause() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_people_ledger(&fluree, "people:main").await;

    let sparql = r"
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>
SELECT ?name
FROM <people:main>
WHERE {
  ?person a ex:Person ;
          schema:name ?name .
}
";

    let tracked = fluree
        .query_connection_sparql_tracked(sparql, None)
        .await
        .expect("query_connection_sparql_tracked should succeed");

    // SPARQL tracked queries now return W3C SPARQL JSON format
    assert_eq!(tracked.status, 200);
    let head = tracked.result.get("head").expect("head");
    let vars = head
        .get("vars")
        .expect("vars")
        .as_array()
        .expect("vars array");
    assert_eq!(vars, &[json!("name")]);

    let bindings = tracked
        .result
        .get("results")
        .expect("results")
        .get("bindings")
        .expect("bindings")
        .as_array()
        .expect("bindings array");
    let mut names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob"]);

    // Verify fuel tracking returns a positive decimal value
    assert!(
        tracked.fuel.is_some(),
        "fuel should be present in tracked response"
    );
    assert!(tracked.fuel.unwrap() > 0.0, "fuel should be positive");
}
