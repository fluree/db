//! Two lists on one property stay two lists.
//!
//! A list is stored compactly as its members' places on the owning subject
//! and predicate: which of the owner's lists each is in, and its position
//! there. A second list on a property takes the next list id. A list with the
//! same members, in order, as a list a property holds is that list, restated.
//! Most of these tests read lists back through the shared reader, which reads
//! either stored shape, so they check the RDF a ledger holds;
//! [`a_second_list_is_compact_under_its_own_id`] checks the encoding.

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

/// The places `ex:{s} ex:{p}`'s values are stored at, sorted; a value
/// stored as no list member (a chain's head) has none.
async fn places(ledger: &LedgerState, s: &str, p: &str) -> Vec<Option<(u32, i32)>> {
    use fluree_db_core::range::{RangeMatch, RangeTest};
    let snapshot = &ledger.snapshot;
    let s = snapshot.encode_iri(&format!("{EX}{s}")).expect("subject");
    let p = snapshot.encode_iri(&format!("{EX}{p}")).expect("predicate");
    let mut places: Vec<_> = ledger
        .as_graph_db_ref(0)
        .range(
            fluree_db_core::comparator::IndexType::Spot,
            RangeTest::Eq,
            RangeMatch {
                s: Some(s),
                p: Some(p),
                ..Default::default()
            },
        )
        .await
        .expect("range")
        .iter()
        .map(|f| f.m.as_ref().and_then(fluree_db_core::FlakeMeta::place))
        .collect();
    places.sort_unstable();
    places
}

/// A second list on a property is stored compactly, under its own list id,
/// in novelty and in the index; a member deleted by value leaves every list.
#[tokio::test]
async fn a_second_list_is_compact_under_its_own_id() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/compact:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = turtle(&fluree, ledger, "ex:s ex:p (1 2), (3 1) .").await;
    let both = [Some((0, 0)), Some((0, 1)), Some((1, 0)), Some((1, 1))];
    assert_eq!(places(&ledger, "s", "p").await, both);
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("load");
    assert_eq!(places(&ledger, "s", "p").await, both, "indexed");
    assert_eq!(lists(&ledger, "s", "p").await, [[1, 2], [3, 1]]);

    let ledger = turtle(&fluree, ledger, "ex:s ex:p (4) .").await;
    assert_eq!(places(&ledger, "s", "p").await.last(), Some(&Some((2, 0))));
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("DELETE DATA {{ <{EX}s> <{EX}p> 1 }}"))
        .commit()
        .await
        .expect("delete");
    let ledger = fluree.ledger(id).await.expect("load");
    assert_eq!(
        lists(&ledger, "s", "p").await,
        vec![vec![2], vec![3], vec![4]]
    );
}

#[tokio::test]
async fn two_lists_in_one_turtle_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/turtle").await;
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
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/jsonld").await;
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
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/upsert").await;
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
    let ledger = support::genesis_ledger(&fluree, "it/list-identity/template").await;
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

/// Two lists on a SHACL path term are two values, not one path: a shape whose
/// `sh:path`, `sh:alternativePath`, or path operator holds two lists is
/// malformed, as one with two `sh:path` values is, in novelty and in the
/// index. One list on `sh:path` is still a sequence.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn two_lists_on_a_shacl_path_term_are_two_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/shacl-path:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = support::enable_shacl(&fluree, ledger).await;
    // Every focus written below heads the chain `ex:a/ex:b/ex:c/ex:d`, which
    // reaches a value under any one path the two lists could be merged into.
    turtle(
        &fluree,
        ledger,
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        ex:Seq sh:targetClass ex:SeqThing ;
            sh:property [ sh:path (ex:a ex:b) ; sh:minCount 1 ] .
        ex:Path sh:targetClass ex:PathThing ;
            sh:property [ sh:path (ex:a ex:b), (ex:c ex:d) ; sh:minCount 1 ] .
        ex:Alt sh:targetClass ex:AltThing ; sh:property [
            sh:path [ sh:alternativePath (ex:a ex:b), (ex:c ex:d) ] ; sh:minCount 1 ] .
        ex:Op sh:targetClass ex:OpThing ; sh:property [
            sh:path [ sh:oneOrMorePath (ex:a ex:b), (ex:c ex:d) ] ; sh:minCount 1 ] .
        ex:n1 ex:b ex:n2 . ex:n2 ex:c ex:n3 . ex:n3 ex:d "end" ."#,
    )
    .await;
    let write = |focus: String, class: &'static str| {
        let fluree = &fluree;
        async move {
            let ledger = fluree.ledger(id).await.expect("load");
            let ttl = format!("@prefix ex: <{EX}> .\nex:{focus} a ex:{class} ; ex:a ex:n1 .");
            fluree
                .insert_turtle(ledger, &ttl)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    };

    for lane in ["novelty", "indexed"] {
        if lane == "indexed" {
            support::rebuild_and_publish_index(&fluree, id).await;
        }
        write(format!("seq-{lane}"), "SeqThing")
            .await
            .unwrap_or_else(|e| panic!("{lane}: one list is a sequence: {e}"));
        for class in ["PathThing", "AltThing", "OpThing"] {
            let err = write(format!("{class}-{lane}"), class)
                .await
                .expect_err(&format!("{lane}: {class}'s two lists made one path"));
            assert!(err.contains("multiple"), "{lane} {class}: {err}");
        }
    }
}
