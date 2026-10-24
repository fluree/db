//! Two lists on one property stay two lists.
//!
//! A list is stored compactly as its members' positions on the owning
//! subject and predicate, and a position names no list: two lists stored that
//! way on one property merge their members into one list. Every list but one
//! on a property is therefore stored as an explicit `rdf:first`/`rdf:rest`
//! chain. A list with the same members, in order, as the compact list a
//! property holds is that list, restated. These tests read lists back through
//! the shared reader, which reads either shape, so they check the RDF a ledger
//! holds, not its encoding.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::json;

const EX: &str = "http://example.org/";

/// The lists `ex:{s} ex:{p}` holds, each as its members' integer values.
async fn lists(ledger: &LedgerState, s: &str, p: &str) -> Vec<Vec<i64>> {
    support::stored_int_lists(ledger, &format!("{EX}{s}"), &format!("{EX}{p}")).await
}

async fn turtle(fluree: &fluree_db_api::Fluree, ledger: LedgerState, ttl: &str) -> LedgerState {
    fluree
        .insert_turtle(ledger, &format!("@prefix ex: <{EX}> .\n{ttl}"))
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{ttl}"))
        .ledger
}

fn ctx() -> serde_json::Value {
    json!({"ex": EX})
}

#[tokio::test]
async fn two_lists_in_one_turtle_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/turtle");
    let ledger = turtle(
        &fluree,
        ledger,
        "ex:s ex:p (1 2), (3 4) ; ex:q (1 2), (1 3) .",
    )
    .await;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2], [3, 4]]);
    // A member shared at one position used to collapse to one fact.
    assert_eq!(lists(&ledger, "s", "q").await, [[1, 2], [1, 3]]);
}

#[tokio::test]
async fn two_lists_in_one_jsonld_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/jsonld");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:s",
                "ex:p": [{"@list": [1, 2]}, {"@list": [3, 4]}]
            }),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2], [3, 4]]);
}

/// Inserting a different list onto a property holding one adds a list, and
/// inserting the same list again restates it, as inserting a stored fact
/// does; before and over an index.
#[tokio::test]
async fn inserting_onto_a_stored_list_adds_a_different_one() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/stored:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = turtle(&fluree, ledger, "ex:s ex:p (1 2) .").await;
    let ledger = turtle(&fluree, ledger, "ex:s ex:p (1 2) .").await;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2]]);
    let ledger = turtle(&fluree, ledger, "ex:s ex:p (3 4) .").await;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2], [3, 4]]);

    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("load");
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:s", "ex:p": {"@list": [1, 2]}}),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2], [3, 4]]);
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:s", "ex:p": {"@list": [5]}}),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        lists(&ledger, "s", "p").await,
        vec![vec![1, 2], vec![3, 4], vec![5]]
    );
}

/// Upsert replaces a subject's values, so it is the idempotent way to load
/// a list: restating it commits nothing, and a new one replaces it.
#[tokio::test]
async fn upsert_replaces_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/upsert");
    let doc = |items: serde_json::Value| json!({"@context": ctx(), "@id": "ex:s", "ex:p": {"@list": items}});
    let ledger = fluree
        .upsert(ledger, &doc(json!([1, 2])))
        .await
        .expect("upsert")
        .ledger;
    let again = fluree
        .upsert(ledger, &doc(json!([1, 2])))
        .await
        .expect("upsert");
    assert_eq!(
        (again.receipt.assert_count, again.receipt.retract_count),
        (0, 0),
        "restating the list commits nothing"
    );
    assert_eq!(lists(&again.ledger, "s", "p").await, [[1, 2]]);
    let ledger = fluree
        .upsert(again.ledger, &doc(json!([5, 6])))
        .await
        .expect("upsert")
        .ledger;
    assert_eq!(lists(&ledger, "s", "p").await, [[5, 6]]);

    let ledger = fluree
        .upsert_turtle(
            ledger,
            &format!("@prefix ex: <{EX}> .\nex:s ex:p (7) , (8) ."),
        )
        .await
        .expect("upsert turtle")
        .ledger;
    assert_eq!(lists(&ledger, "s", "p").await, [[7], [8]]);
}

/// A list in an insert template is written in each WHERE solution: the same
/// list on one subject is that list again, and each subject gets its own.
#[tokio::test]
async fn a_template_list_is_a_list_per_solution() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/template");
    let ledger = turtle(&fluree, ledger, "ex:a ex:k 1 . ex:b ex:k 2 .").await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "?x", "ex:k": "?v"},
                "insert": [
                    {"@id": "ex:s", "ex:p": {"@list": [1, 2]}},
                    {"@id": "?x", "ex:p": {"@list": [3]}}
                ]
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2]]);
    assert_eq!(lists(&ledger, "a", "p").await, [[3]]);
    assert_eq!(lists(&ledger, "b", "p").await, [[3]]);
}

/// ADD merges a graph into another, so a list on a property where the
/// destination holds one is a second list there; COPY makes the destination
/// the source.
#[tokio::test]
async fn add_merges_a_second_list_copy_replaces() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/graphs:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = turtle(
        &fluree,
        ledger,
        "GRAPH <http://example.org/g1> { ex:s ex:p (1 2) . }
         GRAPH <http://example.org/g2> { ex:s ex:p (3 4) . }
         GRAPH <http://example.org/g3> { ex:s ex:p (5) . }",
    )
    .await;
    let graph = |ledger: &LedgerState, name: &str| {
        ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(&format!("{EX}{name}"))
            .expect("graph")
    };
    let in_graph = |ledger: &LedgerState, name: &str| {
        let g = graph(ledger, name);
        let ledger = ledger.clone();
        async move {
            support::stored_int_lists_in(&ledger, g, &format!("{EX}s"), &format!("{EX}p")).await
        }
    };
    assert_eq!(in_graph(&ledger, "g2").await, [[3, 4]]);

    let update = |sparql: &'static str| {
        let fluree = &fluree;
        async move {
            fluree
                .graph(id)
                .transact()
                .sparql_update(sparql)
                .commit()
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"))
        }
    };
    update("ADD <http://example.org/g1> TO <http://example.org/g2>").await;
    update("COPY <http://example.org/g1> TO <http://example.org/g3>").await;
    let ledger = fluree.ledger(id).await.expect("load");
    assert_eq!(in_graph(&ledger, "g2").await, [[1, 2], [3, 4]]);
    assert_eq!(in_graph(&ledger, "g3").await, [[1, 2]]);
    assert_eq!(in_graph(&ledger, "g1").await, [[1, 2]]);
}
