//! Integration tests for JSON-LD `ask` boolean queries.
//!
//! Tests the `"ask": [...]` query form where the value of `ask` is the
//! where clause. Returns a bare boolean indicating whether the patterns
//! have any solution.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_people() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/ask:people");

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30 },
            { "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 25 }
        ]
    });

    let committed = fluree.insert(ledger, &tx).await.expect("insert people");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn ask_true_when_match_exists() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:name": "Alice" }
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));
}

#[tokio::test]
async fn ask_false_when_no_match() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:name": "Charlie" }
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_filter() {
    let (fluree, ledger) = seed_people().await;

    // Alice is 30, Bob is 25 — only Alice matches > 28
    let query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:age": "?age" },
            ["filter", "(> ?age 28)"]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));

    // Nobody is older than 50
    let query_no_match = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:age": "?age" },
            ["filter", "(> ?age 50)"]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query_no_match)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_type_pattern() {
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "@type": "ex:Person" }
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));

    // No Animals exist
    let query_no_match = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?x", "@type": "ex:Animal" }
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query_no_match)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(false));
}

#[tokio::test]
async fn ask_with_optional() {
    let (fluree, ledger) = seed_people().await;

    // Base pattern matches (Alice exists), OPTIONAL just adds more bindings
    let query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:name": "Alice" },
            ["optional", { "@id": "?person", "ex:email": "?email" }]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));
}

#[tokio::test]
async fn ask_object_shorthand() {
    let (fluree, ledger) = seed_people().await;

    // Single node-map object instead of array — shorthand form
    let query = json!({
        "@context": ctx(),
        "ask": { "@id": "?person", "ex:name": "Alice" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(json, JsonValue::Bool(true));
}

#[tokio::test]
async fn ask_rejects_non_pattern_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/ask:bad-values");

    // "ask": true — old syntax, no longer valid
    let result = support::query_jsonld(&fluree, &ledger, &json!({"ask": true})).await;
    assert!(result.is_err(), "ask: true should be rejected");

    // "ask": 1
    let result = support::query_jsonld(&fluree, &ledger, &json!({"ask": 1})).await;
    assert!(result.is_err(), "ask: 1 should be rejected");

    // "ask": "yes"
    let result = support::query_jsonld(&fluree, &ledger, &json!({"ask": "yes"})).await;
    assert!(result.is_err(), "ask: \"yes\" should be rejected");

    // "ask": false
    let result = support::query_jsonld(&fluree, &ledger, &json!({"ask": false})).await;
    assert!(result.is_err(), "ask: false should be rejected");

    // "ask": null
    let result = support::query_jsonld(&fluree, &ledger, &json!({"ask": null})).await;
    assert!(result.is_err(), "ask: null should be rejected");
}

#[tokio::test]
async fn ask_sparql_parity() {
    let (fluree, ledger) = seed_people().await;

    // JSON-LD ask
    let jsonld_query = json!({
        "@context": ctx(),
        "ask": [
            { "@id": "?person", "ex:name": "Alice" }
        ]
    });

    let jsonld_result = support::query_jsonld(&fluree, &ledger, &jsonld_query)
        .await
        .expect("json-ld query");
    let jsonld_json = jsonld_result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("json-ld format");

    // SPARQL ASK
    let sparql_result = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/ns/> ASK { ?person ex:name \"Alice\" }",
    )
    .await
    .expect("sparql query");

    // SPARQL ASK returns W3C envelope via to_sparql_json
    let sparql_json = sparql_result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql format");

    // Both should indicate true — JSON-LD as bare bool, SPARQL as W3C envelope
    assert_eq!(jsonld_json, JsonValue::Bool(true));
    assert_eq!(sparql_json["boolean"], true);
}
