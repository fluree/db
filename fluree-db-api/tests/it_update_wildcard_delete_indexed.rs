//! Regression: wildcard DELETE must retract indexed string facts.
//!
//! When a JSON-LD update uses wildcard predicates (`"?p": "?o"`) and the matched
//! facts are already in the binary index (post-reindex), the generated retraction
//! flakes must use the same datatype SIDs as the asserted facts. Otherwise the
//! retracts commit cleanly but are silently ineffective (fact identity includes `dt`).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn count_matches(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    where_obj: serde_json::Value,
) -> usize {
    let q = json!({
        "@context": ctx(),
        "select": ["?o"],
        "where": where_obj
    });
    let result = support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query");
    result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async")
        .as_array()
        .expect("array")
        .len()
}

#[tokio::test]
async fn update_wildcard_delete_retracts_indexed_string_boolean_integer_and_ref() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // Insert one entity with a mix of value kinds.
    let insert_txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "schema:name": "Alice",
        "schema:age": 42,
        "ex:active": true,
        "ex:friend": {"@id": "ex:bob"}
    });
    let receipt = fluree.insert(ledger0, &insert_txn).await.expect("insert");

    // Force indexing so the WHERE binding is reconstructed from the binary index.
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Sanity: all properties exist pre-delete.
    assert_eq!(
        count_matches(
            &fluree,
            &receipt.ledger,
            json!({"@id":"ex:alice","schema:name":"?o"})
        )
        .await,
        1
    );
    assert_eq!(
        count_matches(
            &fluree,
            &receipt.ledger,
            json!({"@id":"ex:alice","schema:age":"?o"})
        )
        .await,
        1
    );
    assert_eq!(
        count_matches(
            &fluree,
            &receipt.ledger,
            json!({"@id":"ex:alice","ex:active":"?o"})
        )
        .await,
        1
    );
    assert_eq!(
        count_matches(
            &fluree,
            &receipt.ledger,
            json!({"@id":"ex:alice","ex:friend":"?o"})
        )
        .await,
        1
    );

    // Wildcard DELETE of all predicates.
    let delete_txn = json!({
        "@context": ctx(),
        "where":  { "@id": "ex:alice", "?p": "?o" },
        "delete": { "@id": "ex:alice", "?p": "?o" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("update delete");

    // All previously asserted properties must be gone.
    assert_eq!(
        count_matches(
            &fluree,
            &out.ledger,
            json!({"@id":"ex:alice","schema:name":"?o"})
        )
        .await,
        0
    );
    assert_eq!(
        count_matches(
            &fluree,
            &out.ledger,
            json!({"@id":"ex:alice","schema:age":"?o"})
        )
        .await,
        0
    );
    assert_eq!(
        count_matches(
            &fluree,
            &out.ledger,
            json!({"@id":"ex:alice","ex:active":"?o"})
        )
        .await,
        0
    );
    assert_eq!(
        count_matches(
            &fluree,
            &out.ledger,
            json!({"@id":"ex:alice","ex:friend":"?o"})
        )
        .await,
        0
    );
}
