//! Per-level ordering and paging inside a hydration.
//!
//! `{"ex:friend": {"select": [...], "orderBy": [...], "limit": N}}` bounds how
//! many of *each subject's* values are shown. That is a different question from
//! how many rows the query returns, so the WHERE clause's `limit` cannot express
//! it — before this, a nested collection was all-or-nothing.

use crate::support::{genesis_ledger, query_jsonld_formatted, MemoryFluree};
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};

fn context() -> JsonValue {
    json!({ "ex": "http://example.org/" })
}

/// Alice knows four people with distinct names and ages; Bob knows nobody.
async fn seed(ledger_id: &str) -> (MemoryFluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:tag": ["red", "green", "blue", "amber"],
                        "ex:knows": [
                            { "@id": "ex:bob" }, { "@id": "ex:carol" },
                            { "@id": "ex:dave" }, { "@id": "ex:erin" }
                        ]
                    },
                    { "@id": "ex:bob", "ex:name": "Bob", "ex:age": 41 },
                    { "@id": "ex:carol", "ex:name": "Carol", "ex:age": 29 },
                    { "@id": "ex:dave", "ex:name": "Dave", "ex:age": 35 },
                    { "@id": "ex:erin", "ex:name": "Erin", "ex:age": 52 }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

async fn alice(fluree: &MemoryFluree, ledger: &LedgerState, friend_spec: JsonValue) -> JsonValue {
    let query = json!({
        "@context": context(),
        "select": { "ex:alice": ["@id", { "ex:knows": friend_spec }] }
    });
    let out = query_jsonld_formatted(fluree, ledger, &query)
        .await
        .expect("query");
    out.as_array().unwrap()[0].clone()
}

fn names(subject: &JsonValue) -> Vec<String> {
    subject["ex:knows"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|f| f["ex:name"].as_str().unwrap().to_string())
                .collect()
        })
        // A single remaining value is not wrapped in an array.
        .unwrap_or_else(|| vec![subject["ex:knows"]["ex:name"].as_str().unwrap().to_string()])
}

#[tokio::test]
async fn the_array_form_still_returns_everything() {
    let (fluree, ledger) = seed("nested-baseline").await;
    let mut got = names(&alice(&fluree, &ledger, json!(["ex:name"])).await);
    got.sort();
    assert_eq!(got, ["Bob", "Carol", "Dave", "Erin"]);
}

#[tokio::test]
async fn order_by_a_nested_property() {
    let (fluree, ledger) = seed("nested-order").await;

    let ascending = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name", "ex:age"], "orderBy": ["ex:age"] }),
    )
    .await;
    assert_eq!(names(&ascending), ["Carol", "Dave", "Bob", "Erin"]);

    let descending = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name", "ex:age"], "orderBy": [["desc", "ex:age"]] }),
    )
    .await;
    assert_eq!(names(&descending), ["Erin", "Bob", "Dave", "Carol"]);

    // Strings order lexically.
    let by_name = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name"], "orderBy": [["desc", "ex:name"]] }),
    )
    .await;
    assert_eq!(names(&by_name), ["Erin", "Dave", "Carol", "Bob"]);
}

#[tokio::test]
async fn limit_and_offset_cut_an_ordered_window() {
    let (fluree, ledger) = seed("nested-page").await;

    let first_two = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name", "ex:age"], "orderBy": ["ex:age"], "limit": 2 }),
    )
    .await;
    assert_eq!(names(&first_two), ["Carol", "Dave"]);

    let next_two = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name", "ex:age"], "orderBy": ["ex:age"], "offset": 2, "limit": 2 }),
    )
    .await;
    assert_eq!(names(&next_two), ["Bob", "Erin"]);

    // An offset past the end empties the field rather than erroring, so the key
    // simply does not appear.
    let past_end = alice(
        &fluree,
        &ledger,
        json!({ "select": ["ex:name"], "offset": 99 }),
    )
    .await;
    assert!(past_end.get("ex:knows").is_none(), "{past_end}");
}

#[tokio::test]
async fn a_literal_valued_property_needs_no_select() {
    let (fluree, ledger) = seed("nested-literal").await;
    let query = json!({
        "@context": context(),
        "select": {
            "ex:alice": ["@id", { "ex:tag": { "orderBy": ["@value"], "limit": 2 } }]
        }
    });
    let out = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query");
    assert_eq!(
        out.as_array().unwrap()[0],
        json!({ "@id": "ex:alice", "ex:tag": ["amber", "blue"] })
    );
}

#[tokio::test]
async fn modifiers_are_part_of_the_hydration_cache_key() {
    // Both columns expand the same subject through the same predicate, differing
    // only in `limit`. A cache keyed on the selection alone would serve the first
    // column's answer for the second.
    let (fluree, ledger) = seed("nested-cache").await;
    let query = json!({
        "@context": context(),
        "select": [
            { "ex:alice": [{ "ex:knows": { "select": ["ex:name"], "orderBy": ["ex:name"], "limit": 1 } }] },
            { "ex:alice": [{ "ex:knows": { "select": ["ex:name"], "orderBy": ["ex:name"], "limit": 3 } }] }
        ]
    });
    let out = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query");
    let row = &out.as_array().unwrap()[0];
    assert_eq!(names(&row[0]), ["Bob"]);
    assert_eq!(names(&row[1]), ["Bob", "Carol", "Dave"]);
}

#[tokio::test]
async fn malformed_nested_selections_are_rejected() {
    let (fluree, ledger) = seed("nested-errors").await;

    let cases = [
        (json!({ "limit": -1 }), "non-negative"),
        (json!({ "select": "ex:name" }), "must be an array"),
        (json!({ "nope": 1 }), "unknown key"),
        (json!({}), "at least one of"),
        (json!({ "orderBy": [["sideways", "ex:name"]] }), "direction"),
    ];
    for (spec, expected) in cases {
        let query = json!({
            "@context": context(),
            "select": { "ex:alice": [{ "ex:knows": spec }] }
        });
        let err = query_jsonld_formatted(&fluree, &ledger, &query)
            .await
            .expect_err(&format!("{spec} should be rejected"))
            .to_string();
        assert!(err.contains(expected), "for {spec}: {err}");
    }
}
