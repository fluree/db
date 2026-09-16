//! `rdf:JSON` literals are canonical, so one value is one fact.
//!
//! A literal's identity is its text. Before canonicalization, two spellings
//! of one value were two facts, and every comparison that followed worked on
//! the wrong unit. Retraction matching, upsert replace, and merge conflict
//! detection all missed.
//!
//! Regression coverage for #1781.

use crate::support;
use fluree_db_api::{ConflictStrategy, FlureeBuilder};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

/// `ex:config ex:items <value>`, with `value` given as a JSON document.
fn items(value: serde_json::Value) -> serde_json::Value {
    json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:items": {"@value": value, "@type": "@json"}
        }]
    })
}

/// Every `ex:items` value on the ledger, as stored.
async fn stored_items(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let q = json!({
        "@context": ctx(),
        "select": ["?v"],
        "where": {"@id": "ex:config", "ex:items": "?v"}
    });
    let result = support::query_jsonld(fluree, &ledger, &q).await.unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let mut out: Vec<String> = support::normalize_rows(&rows)
        .iter()
        .map(|row| {
            let value = row
                .as_array()
                .and_then(|a| a.first())
                .expect("row should hold one value");
            // The JSON-LD formatter hands a JSON literal back parsed. Both
            // shapes re-serialize to the stored lexical form, because
            // `serde_json` preserves member order as it parsed it.
            match value {
                serde_json::Value::String(s) => s.clone(),
                other => serde_json::to_string(other).expect("serializable"),
            }
        })
        .collect();
    out.sort();
    out
}

/// Member order in the written document does not change the stored term.
#[tokio::test]
async fn key_order_does_not_change_the_stored_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let a = fluree
        .insert(ledger, &items(json!([{"name": "alpha", "qty": 1}])))
        .await
        .unwrap()
        .ledger;

    assert_eq!(
        stored_items(&fluree, "mydb:main").await,
        vec![r#"[{"name":"alpha","qty":1}]"#]
    );

    // The same value, spelled the other way round: one fact, not two.
    fluree
        .insert(a, &items(json!([{"qty": 1, "name": "alpha"}])))
        .await
        .unwrap();
    assert_eq!(
        stored_items(&fluree, "mydb:main").await,
        vec![r#"[{"name":"alpha","qty":1}]"#],
        "re-asserting the same value in another key order must not add a second fact"
    );
}

/// A delete that names the value matches it however the writer spelled it.
#[tokio::test]
async fn delete_matches_the_same_value_spelled_differently() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let seeded = fluree
        .insert(ledger, &items(json!([{"name": "alpha", "qty": 1}])))
        .await
        .unwrap()
        .ledger;

    fluree
        .update(
            seeded,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:config",
                    "ex:items": {"@value": [{"qty": 1, "name": "alpha"}], "@type": "@json"}
                }
            }),
        )
        .await
        .unwrap();

    assert!(
        stored_items(&fluree, "mydb:main").await.is_empty(),
        "the delete should have matched the stored value"
    );
}

/// The merge from the issue: a branch replaces the value while the target
/// rewrites the same value in another key order. With one term per value the
/// branch's retraction matches, so the merge replaces rather than duplicates.
#[tokio::test]
async fn merge_after_a_reserialized_rewrite_does_not_duplicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &items(json!([{"name": "alpha", "qty": 1}])))
        .await
        .unwrap()
        .ledger;
    fluree
        .create_branch("mydb", "feat", None, None)
        .await
        .unwrap();

    // The branch replaces the value.
    let replace = |value: serde_json::Value| {
        json!({
            "@context": ctx(),
            "where": {"@id": "ex:config", "ex:items": "?old"},
            "delete": {"@id": "ex:config", "ex:items": "?old"},
            "insert": {
                "@id": "ex:config",
                "ex:items": {"@value": value, "@type": "@json"}
            }
        })
    };
    let feat = fluree.ledger("mydb:feat").await.unwrap();
    fluree
        .update(feat, &replace(json!([{"name": "beta", "qty": 2}])))
        .await
        .unwrap();

    // Main rewrites the same logical value with the members the other way
    // round, which used to mint a second term.
    fluree
        .update(main, &replace(json!([{"qty": 1, "name": "alpha"}])))
        .await
        .unwrap();

    fluree
        .merge_branch("mydb", "feat", None, ConflictStrategy::default())
        .await
        .expect("merge");

    assert_eq!(
        stored_items(&fluree, "mydb:main").await,
        vec![r#"[{"name":"beta","qty":2}]"#],
        "the branch's retraction must match main's value, leaving one value"
    );
}

/// Whitespace and nesting are canonicalized too, not just top-level order.
#[tokio::test]
async fn spacing_and_nested_order_do_not_change_the_stored_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    // A pre-serialized document, passed as a string, is canonicalized in
    // place rather than serialized again.
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [{
                    "@id": "ex:config",
                    "ex:items": {
                        "@value": "{ \"b\" : [1, 2], \"a\" : {\"y\": 2, \"x\": 3} }",
                        "@type": "@json"
                    }
                }]
            }),
        )
        .await
        .unwrap()
        .ledger;
    assert_eq!(
        stored_items(&fluree, "mydb:main").await,
        vec![r#"{"a":{"x":3,"y":2},"b":[1,2]}"#]
    );

    fluree
        .insert(ledger, &items(json!({"a": {"y": 2, "x": 3}, "b": [1, 2]})))
        .await
        .unwrap();
    assert_eq!(
        stored_items(&fluree, "mydb:main").await,
        vec![r#"{"a":{"x":3,"y":2},"b":[1,2]}"#],
        "the document and its pre-serialized twin are one fact"
    );
}
