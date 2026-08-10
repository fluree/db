//! Regression tests for the whole-subject retraction shape used by R2RML
//! materialization deletion (`build_retract_doc`).
//!
//! The wildcard retraction MUST bind `?s` as a typed `@id` value. A bare-string
//! binding parses to a string *literal* that never joins a real subject Sid, so
//! the delete silently matches zero rows and retracts nothing. These tests run
//! the shape end-to-end through `Fluree::update` against a memory ledger and
//! assert the triples are actually gone — exactly what was missing when the
//! bare-string bug shipped.

use crate::support;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const NAME: &str = "https://www.w3.org/ns/activitystreams#name";

#[tokio::test]
async fn materialize_retract_shape_actually_retracts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "mat/retract:main");

    let seed = json!({
        "@graph": [
            {
                "@id": "https://example.org/Actor%2F1",
                "https://www.w3.org/ns/activitystreams#name": "Alice",
                "https://www.w3.org/ns/activitystreams#summary": "first"
            },
            {
                "@id": "https://example.org/Actor%2F2",
                "https://www.w3.org/ns/activitystreams#name": "Bob"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.unwrap().ledger;

    // Whole-subject retraction of Actor/1 using the CORRECT typed-@id values
    // shape (mirrors build_retract_doc).
    let retract = json!({
        "values": ["?s", [[{ "@type": "@id", "@value": "https://example.org/Actor%2F1" }]]],
        "where": { "@id": "?s", "?p": "?o" },
        "delete": { "@id": "?s", "?p": "?o" }
    });
    let ledger = fluree.update(ledger, &retract).await.unwrap().ledger;

    // Actor/1 is fully gone (all predicates); Actor/2 remains.
    let q = json!({
        "select": ["?s"],
        "where": { "@id": "?s", "https://www.w3.org/ns/activitystreams#name": "?n" }
    });
    let out = support::query_jsonld_formatted(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_string();
    assert!(
        !out.contains("Actor%2F1"),
        "Actor/1 should be retracted, got: {out}"
    );
    assert!(
        out.contains("Actor%2F2"),
        "Actor/2 should remain, got: {out}"
    );
}

#[tokio::test]
async fn materialize_retract_bare_string_is_noop() {
    // Control: the bare-string binding (the shipped bug) retracts nothing.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "mat/retract-bare:main");
    let seed = json!({
        "@graph": [
            { "@id": "https://example.org/Actor%2F1", "https://www.w3.org/ns/activitystreams#name": "Alice" }
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.unwrap().ledger;

    let retract = json!({
        "values": ["?s", [["https://example.org/Actor%2F1"]]],
        "where": { "@id": "?s", "?p": "?o" },
        "delete": { "@id": "?s", "?p": "?o" }
    });
    let ledger = fluree.update(ledger, &retract).await.unwrap().ledger;

    let q = json!({
        "select": ["?s"],
        "where": { "@id": "?s", "https://www.w3.org/ns/activitystreams#name": "?n" }
    });
    let out = support::query_jsonld_formatted(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_string();
    assert!(
        out.contains("Actor%2F1"),
        "bare-string retraction must be a no-op (subject still present), got: {out}"
    );
    let _ = NAME; // documents the predicate under test
}
