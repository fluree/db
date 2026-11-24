//! Nested lists: a list holding lists is stored as one tree, its values
//! values of the property that holds it, whatever syntax writes it.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n\
                      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";

async fn create(fluree: &fluree_db_api::Fluree, name: &str) -> String {
    let id = format!("it/nested-lists/{name}:main");
    fluree.create_ledger(&id).await.expect("create");
    id
}

async fn turtle(fluree: &fluree_db_api::Fluree, id: &str, ttl: &str) {
    fluree
        .graph(id)
        .transact()
        .insert_turtle(&format!("@prefix ex: <http://example.org/> .\n{ttl}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{ttl}"));
}

async fn update(fluree: &fluree_db_api::Fluree, id: &str, body: &str) {
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{body}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("update: {e}\n{body}"));
}

async fn insert_jsonld(fluree: &fluree_db_api::Fluree, id: &str, node: JsonValue) {
    let txn = json!({"@context": {"ex": "http://example.org/"}, "insert": node});
    fluree
        .graph(id)
        .transact()
        .update(&txn)
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{txn}"));
}

async fn load(fluree: &fluree_db_api::Fluree, id: &str) -> LedgerState {
    fluree.ledger(id).await.expect("load")
}

/// `ex:s`'s `ex:p`, as a JSON-LD crawl returns it.
async fn crawl(fluree: &fluree_db_api::Fluree, id: &str) -> JsonValue {
    let ledger = load(fluree, id).await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:s": ["*"]}
    });
    let out = support::query_jsonld_formatted(fluree, &ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("crawl: {e}"));
    out[0]["ex:p"].clone()
}

/// The rows of a SPARQL query, each a sorted array of its values.
async fn sparql(fluree: &fluree_db_api::Fluree, id: &str, text: &str) -> Vec<JsonValue> {
    let ledger = load(fluree, id).await;
    let text = format!("{PREFIX}{text}");
    let result = support::query_sparql(fluree, &ledger, &text)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

fn tree() -> JsonValue {
    json!({"@list": [{"@list": [1, 2]}, 3]})
}

/// Turtle (inserted or upserted), a SPARQL update and JSON-LD write
/// `((1 2) 3)` as one list: its
/// values are values of `ex:p`, and no `rdf:first` triple is stored.
#[tokio::test]
async fn every_syntax_stores_a_nested_list_as_one_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ttl = create(&fluree, "ttl").await;
    turtle(&fluree, &ttl, "ex:s ex:p ((1 2) 3) .").await;
    let sparql_update = create(&fluree, "sparql").await;
    update(
        &fluree,
        &sparql_update,
        "INSERT DATA { ex:s ex:p ((1 2) 3) }",
    )
    .await;
    let upsert = create(&fluree, "upsert").await;
    fluree
        .upsert_turtle(
            load(&fluree, &upsert).await,
            "@prefix ex: <http://example.org/> .\nex:s ex:p ((1 2) 3) .",
        )
        .await
        .expect("upsert");
    let jsonld = create(&fluree, "jsonld").await;
    insert_jsonld(&fluree, &jsonld, json!({"@id": "ex:s", "ex:p": tree()})).await;
    let spelled = create(&fluree, "spelled").await;
    turtle(
        &fluree,
        &spelled,
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:s ex:p _:c0 .\n\
         _:c0 rdf:first _:d0 ; rdf:rest _:c1 .\n\
         _:c1 rdf:first 3 ; rdf:rest rdf:nil .\n\
         _:d0 rdf:first 1 ; rdf:rest _:d1 .\n\
         _:d1 rdf:first 2 ; rdf:rest rdf:nil .",
    )
    .await;

    for id in [&ttl, &upsert, &sparql_update, &jsonld, &spelled] {
        assert_eq!(crawl(&fluree, id).await, tree(), "{id}");
        let values = sparql(&fluree, id, "SELECT ?o WHERE { ex:s ex:p ?o }").await;
        assert_eq!(values, [json!([1]), json!([2]), json!([3])], "{id}");
        let cells = sparql(&fluree, id, "SELECT ?c WHERE { ?c rdf:first ?o }").await;
        assert!(cells.is_empty(), "{id}: {cells:?}");
    }
}

/// Depth alone does not tell `((1 2))` from `((1) (2))`.
#[tokio::test]
async fn lists_side_by_side_stay_apart() {
    let fluree = FlureeBuilder::memory().build_memory();
    let one = create(&fluree, "one-inner").await;
    turtle(&fluree, &one, "ex:s ex:p ((1 2)) .").await;
    let two = create(&fluree, "two-inner").await;
    turtle(&fluree, &two, "ex:s ex:p ((1) (2)) .").await;
    assert_eq!(
        crawl(&fluree, &one).await,
        json!({"@list": [{"@list": [1, 2]}]})
    );
    assert_eq!(
        crawl(&fluree, &two).await,
        json!({"@list": [{"@list": [1]}, {"@list": [2]}]})
    );
}

/// Writing a nested list a property holds again restates it; one that
/// differs only in its shape is another list.
#[tokio::test]
async fn writing_a_nested_list_again_restates_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "restate").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    insert_jsonld(&fluree, &id, json!({"@id": "ex:s", "ex:p": tree()})).await;
    assert_eq!(crawl(&fluree, &id).await, tree());
    turtle(&fluree, &id, "ex:s ex:p ((1) 2 3) .").await;
    let lists = crawl(&fluree, &id).await;
    let lists = lists.as_array().expect("two lists");
    assert_eq!(lists.len(), 2, "{lists:?}");
    assert!(lists.contains(&tree()), "{lists:?}");
    assert!(
        lists.contains(&json!({"@list": [{"@list": [1]}, 2, 3]})),
        "{lists:?}"
    );
}

/// An empty inner list is `rdf:nil`, a value of the list.
#[tokio::test]
async fn an_empty_inner_list_is_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ttl = create(&fluree, "nil-ttl").await;
    turtle(&fluree, &ttl, "ex:s ex:p ((() 1) 2) .").await;
    let jsonld = create(&fluree, "nil-jsonld").await;
    insert_jsonld(
        &fluree,
        &jsonld,
        json!({"@id": "ex:s", "ex:p": {"@list": [{"@list": [{"@list": []}, 1]}, 2]}}),
    )
    .await;
    let want = crawl(&fluree, &ttl).await;
    assert_eq!(crawl(&fluree, &jsonld).await, want);
    let inner = &want["@list"][0]["@list"];
    assert_eq!(inner.as_array().map(Vec::len), Some(2), "{want}");
    assert_eq!(inner[1], json!(1), "{want}");
}

/// A JSON-LD list nested deeper than a tree can hold is refused.
#[tokio::test]
async fn a_list_nested_too_deep_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "too-deep").await;
    let mut deep = json!(1);
    for _ in 0..=16 {
        deep = json!({"@list": [deep]});
    }
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "insert": {"@id": "ex:s", "ex:p": deep}
    });
    let err = fluree
        .graph(&id)
        .transact()
        .update(&txn)
        .commit()
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("more than 15 deep"), "{err}");
}
