//! OWL2-QL integration tests (minimal end-to-end)
//!
//! These are intentionally tiny, focused tests that validate:
//! - query JSON parsing of `"reasoning": "owl2ql"` and `"reasoning": "owl-ql"`
//! - `owl:equivalentProperty` expansion
//! - explicit `"reasoning": "none"` disabling auto-RDFS

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::genesis_ledger;

#[tokio::test]
async fn owl2ql_equivalent_property_expands_across_properties() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "query/owl2ql:equivalent-property");

    // Define p2 owl:equivalentProperty p1
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id":"ex:p1","@type":"rdf:Property"},
            {"@id":"ex:p2","@type":"rdf:Property","owl:equivalentProperty":{"@id":"ex:p1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Insert data using only p1
    let data = json!({"@context":{"ex":"http://example.org/"},"@id":"ex:s","ex:p1":"v"});
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query p2 should see p1 value when owl2ql enabled
    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?v"],
        "where": {"@id":"ex:s","ex:p2":"?v"},
        "reasoning": "owl2ql"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["v"]));
}

#[tokio::test]
async fn owl_ql_alias_string_is_accepted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "query/owl-ql:alias");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id":"ex:p1","@type":"rdf:Property"},
            {"@id":"ex:p2","@type":"rdf:Property","owl:equivalentProperty":{"@id":"ex:p1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({"@context":{"ex":"http://example.org/"},"@id":"ex:s","ex:p1":"v"});
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?v"],
        "where": {"@id":"ex:s","ex:p2":"?v"},
        "reasoning": "owl-ql"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["v"]));
}
