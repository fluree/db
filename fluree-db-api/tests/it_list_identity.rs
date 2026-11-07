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

/// A list deleted — a `@list` in a JSON-LD delete, a collection in a SPARQL
/// `DELETE WHERE` — names a list as a delete names a value: the property's
/// lists that are it, the same members in the same order and no more, each
/// member the term written. Each is deleted whole. A prefix, a reordering, a
/// longer list, another datatype or plain values are not it and lose
/// nothing; before and over an index. A collection that matches no list
/// matches nothing, so the rest of its `DELETE WHERE` deletes nothing.
#[tokio::test]
async fn a_deleted_list_is_the_lists_that_are_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    for indexed in [false, true] {
        let id = format!("it/list-identity/delete-{indexed}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create");
        turtle(
            &fluree,
            ledger,
            "ex:s ex:p (1 9), (1 2), (1 2 3), (3 2 1) ; ex:q 1, 2 ; \
             ex:r (1 1 1 1 1 1 1 1), (1 1 1 1) . ex:t ex:p (1 2) .",
        )
        .await;
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let retracted_by = |update: serde_json::Value| {
            let (fluree, id) = (&fluree, &id);
            async move {
                let mut update = update;
                update["@context"] = json!({"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"});
                let result = fluree
                    .graph(id)
                    .transact()
                    .update(&update)
                    .commit()
                    .await
                    .unwrap_or_else(|e| panic!("{update}: {e}"));
                result.receipt.retract_count
            }
        };
        let sparql = |body: &str| {
            let (fluree, id) = (&fluree, &id);
            let text = format!("PREFIX ex: <{EX}>\n{body}");
            async move {
                fluree
                    .graph(id)
                    .transact()
                    .sparql_update(&text)
                    .commit()
                    .await
                    .map(|r| r.receipt.retract_count)
            }
        };
        let delete =
            |list: serde_json::Value| json!({"delete": {"@id": "ex:s", "ex:p": {"@list": list}}});
        let held = || async { lists(&fluree.ledger(&id).await.expect("load"), "s", "p").await };
        let all = [vec![1, 2], vec![1, 2, 3], vec![1, 9], vec![3, 2, 1]];
        let at = format!("indexed={indexed}");

        for named_nothing in [
            json!([1]),
            json!([2, 1]),
            json!([1, 2, 3, 4]),
            json!([{"@value": "1", "@type": "xsd:int"}, 2]),
        ] {
            assert_eq!(
                retracted_by(delete(named_nothing.clone())).await,
                0,
                "{named_nothing} {at}"
            );
        }
        assert_eq!(held().await, all, "{at}");

        assert_eq!(retracted_by(delete(json!([1, 2]))).await, 2, "{at}");
        assert_eq!(
            held().await,
            [vec![1, 2, 3], vec![1, 9], vec![3, 2, 1]],
            "{at}"
        );
        let ledger = fluree.ledger(&id).await.expect("load");
        assert_eq!(
            lists(&ledger, "t", "p").await,
            [[1, 2]],
            "another owner's list stays; {at}"
        );

        // A literal per solution: `?a` binds each value of the property.
        let per_row = json!({
            "where": {"@id": "ex:s", "ex:p": "?a"},
            "delete": {"@id": "ex:s", "ex:p": {"@list": ["?a", 9]}}
        });
        assert_eq!(retracted_by(per_row).await, 2, "{at}");
        assert_eq!(held().await, [vec![1, 2, 3], vec![3, 2, 1]], "{at}");

        for named_nothing in [
            "ex:s ex:p (1 2)",
            "ex:s ex:p (1 2) ; ex:q 1",
            "ex:s ex:p (3 2 1), (2 1)",
            "ex:s ex:p (2 3)",
        ] {
            assert_eq!(
                sparql(&format!("DELETE WHERE {{ {named_nothing} }}"))
                    .await
                    .expect("delete"),
                0,
                "{named_nothing} {at}"
            );
        }
        assert_eq!(held().await, [vec![1, 2, 3], vec![3, 2, 1]], "{at}");
        assert_eq!(
            sparql("DELETE WHERE { ex:s ex:p (3 2 1) }")
                .await
                .expect("delete"),
            3,
            "{at}"
        );
        assert_eq!(held().await, [[1, 2, 3]], "{at}");
        assert_eq!(
            sparql("DELETE WHERE { ex:s ex:r (1 1 1 1 1 1 1 1) }")
                .await
                .expect("repeated members"),
            8,
            "{at}"
        );
        let ledger = fluree.ledger(&id).await.expect("load");
        assert_eq!(lists(&ledger, "s", "r").await, [[1, 1, 1, 1]], "{at}");
        assert_eq!(
            sparql("DELETE WHERE { ?x ex:p (1 2) }")
                .await
                .expect("delete"),
            2,
            "ex:t's list alone; {at}"
        );
        let ledger = fluree.ledger(&id).await.expect("load");
        assert_eq!(
            lists(&ledger, "t", "p").await,
            Vec::<Vec<i64>>::new(),
            "{at}"
        );

        // Plain values are no list.
        assert_eq!(
            retracted_by(json!({"delete": {"@id": "ex:s", "ex:q": {"@list": [1, 2]}}})).await,
            0,
            "{at}"
        );
        assert_eq!(
            sparql("DELETE WHERE { ex:s ex:q (1 2) }")
                .await
                .expect("plain"),
            0,
            "{at}"
        );
        assert_eq!(places(&ledger, "s", "q").await, [None, None], "{at}");

        // Read as RDF by the pragma, a list's owner edge cannot be deleted.
        let err = sparql("# PRAGMA rdf-lists: true\nDELETE WHERE { ex:s ex:p (1 2 3) }")
            .await
            .expect_err("owner edge");
        assert_eq!(err.status_code(), 400, "{err}");
        assert_eq!(held().await, [[1, 2, 3]], "{at}");
        assert_eq!(retracted_by(delete(json!([1, 2, 4]))).await, 0, "{at}");

        // A member may be a variable, which the rest of the pattern reads as
        // any value: here `ex:q 1`.
        assert_eq!(
            sparql("DELETE WHERE { ex:s ex:p (?a 2 3) ; ex:q ?a }")
                .await
                .expect("variable member"),
            4,
            "{at}"
        );
        assert_eq!(held().await, Vec::<Vec<i64>>::new(), "{at}");
        let ledger = fluree.ledger(&id).await.expect("load");
        assert_eq!(places(&ledger, "s", "q").await, [None], "{at}");
    }
}

/// A collection in a SPARQL `DELETE WHERE` matches a list stored as its
/// triples (one whose cells carry other triples) as those triples, and
/// deletes them, the cells' other triples left; written as `( … )` or as the
/// chain, in the default graph and in a GRAPH block, before and over an
/// index. JSON-LD's `@list` names compact lists alone.
#[tokio::test]
async fn a_deleted_collection_matches_a_list_stored_as_its_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let prefixes =
        format!("PREFIX ex: <{EX}>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>");
    let chain = "ex:s ex:p _:h . _:h rdf:first 1 ; rdf:rest _:t ; ex:note 0 . \
                 _:t rdf:first 2 ; rdf:rest rdf:nil .";
    let patterns = [
        "ex:s ex:p (1 2)",
        "ex:s ex:p _:a . _:a rdf:first 1 ; rdf:rest _:b . _:b rdf:first 2 ; rdf:rest rdf:nil",
    ];
    for indexed in [false, true] {
        for graph in [false, true] {
            for (n, pattern) in patterns.into_iter().enumerate() {
                let id = format!("it/list-identity/delete-chain-{indexed}-{graph}-{n}:main");
                fluree.create_ledger(&id).await.expect("create");
                let in_graph = |body: &str| match graph {
                    true => format!("GRAPH ex:g {{ {body} }}"),
                    false => body.to_string(),
                };
                let update = |body: String| {
                    let (fluree, id) = (&fluree, &id);
                    let text = format!("{prefixes}\n{body}");
                    async move {
                        fluree
                            .graph(id)
                            .transact()
                            .sparql_update(&text)
                            .commit()
                            .await
                            .unwrap_or_else(|e| panic!("{text}: {e}"))
                            .receipt
                            .retract_count
                    }
                };
                let at = format!("{pattern} indexed={indexed} graph={graph}");
                update(format!("INSERT DATA {{ {} }}", in_graph(chain))).await;
                if indexed {
                    support::rebuild_and_publish_index(&fluree, &id).await;
                }
                if !graph {
                    let delete = json!({
                        "@context": {"ex": EX},
                        "delete": {"@id": "ex:s", "ex:p": {"@list": [1, 2]}}
                    });
                    let r = fluree.graph(&id).transact().update(&delete).commit().await;
                    assert_eq!(r.expect("delete").receipt.retract_count, 0, "{at}");
                }
                let longer = format!("DELETE WHERE {{ {} }}", in_graph("ex:s ex:p (1 2 3)"));
                assert_eq!(update(longer).await, 0, "{at}");
                let delete = format!("DELETE WHERE {{ {} }}", in_graph(pattern));
                assert_eq!(update(delete).await, 5, "{at}");
                let ledger = fluree.ledger(&id).await.expect("load");
                let select = format!(
                    "{prefixes}\nSELECT ?p ?o WHERE {{ {} }}",
                    in_graph("?x ?p ?o")
                );
                let left = support::query_sparql_formatted(&fluree, &ledger, &select)
                    .await
                    .expect("query");
                assert_eq!(
                    left,
                    json!([["ex:note", 0]]),
                    "the head's ex:note alone is left; {at}"
                );
            }
        }
    }
}

/// A collection's owner edge is any edge to its head but a cell's, on any
/// predicate: `rdf:first` and `rdf:rest` too.
#[tokio::test]
async fn a_deleted_collection_is_owned_on_any_predicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    for indexed in [false, true] {
        let id = format!("it/list-identity/delete-rdf-owner-{indexed}:main");
        fluree.create_ledger(&id).await.expect("create");
        let insert = json!({
            "@context": {"ex": EX, "rdf": rdf},
            "insert": {
                "@id": "ex:s",
                "rdf:first": {"@list": [1, 2]},
                "rdf:rest": {"@list": [3, 4]}
            }
        });
        fluree
            .graph(&id)
            .transact()
            .update(&insert)
            .commit()
            .await
            .expect("insert");
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        for (pattern, retracted) in [
            ("ex:s rdf:first (1 2 3)", 0),
            ("ex:s rdf:first (1 2)", 2),
            ("ex:s rdf:rest (3 4)", 2),
        ] {
            let text =
                format!("PREFIX ex: <{EX}>\nPREFIX rdf: <{rdf}>\nDELETE WHERE {{ {pattern} }}");
            let r = fluree
                .graph(&id)
                .transact()
                .sparql_update(&text)
                .commit()
                .await;
            assert_eq!(
                r.expect("delete").receipt.retract_count,
                retracted,
                "{pattern} indexed={indexed}"
            );
        }
        let ledger = fluree.ledger(&id).await.expect("load");
        let left = support::query_sparql_formatted(
            &fluree,
            &ledger,
            &format!("SELECT ?p ?o WHERE {{ <{EX}s> ?p ?o }}"),
        )
        .await
        .expect("query");
        assert_eq!(left, json!([]), "indexed={indexed}");
    }
}

/// An empty list, `()` or `{"@list": []}`, is the IRI `rdf:nil`, as SPARQL
/// and JSON-LD define it, and is stored and deleted as that value: with an
/// entry of a list that holds `rdf:nil`, as a delete naming any value is.
#[tokio::test]
async fn an_empty_list_deletes_as_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    for indexed in [false, true] {
        let id = format!("it/list-identity/delete-empty-{indexed}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create");
        turtle(
            &fluree,
            ledger,
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
             ex:s ex:p (rdf:nil 1) ; ex:tag 1 . ex:t ex:p (rdf:nil 2) ; ex:q () .",
        )
        .await;
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let at = format!("indexed={indexed}");
        for p in ["ex:p", "ex:q"] {
            let delete =
                json!({"@context": {"ex": EX}, "delete": {"@id": "ex:t", p: {"@list": []}}});
            let r = fluree.graph(&id).transact().update(&delete).commit().await;
            assert_eq!(r.expect("delete").receipt.retract_count, 1, "{p} {at}");
        }
        let text = format!("PREFIX ex: <{EX}>\nDELETE WHERE {{ ex:s ex:p () ; ex:tag 1 }}");
        let r = fluree
            .graph(&id)
            .transact()
            .sparql_update(&text)
            .commit()
            .await;
        assert_eq!(r.expect("delete").receipt.retract_count, 2, "{at}");
        let ledger = fluree.ledger(&id).await.expect("load");
        for s in ["s", "t"] {
            let left = places(&ledger, s, "p").await;
            assert!(
                matches!(left.as_slice(), [Some((_, 1))]),
                "{s}'s second entry alone is left; {at}: {left:?}"
            );
        }
        assert_eq!(places(&ledger, "t", "q").await, [], "{at}");
    }
}

/// A collection in a GRAPH block of a SPARQL `DELETE WHERE` deletes that
/// graph's lists that are it.
#[tokio::test]
async fn a_deleted_list_in_a_graph_block_is_that_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/delete-graph:main";
    fluree.create_ledger(id).await.expect("create");
    let update = |body: &str| {
        let text = format!("PREFIX ex: <{EX}>\n{body}");
        let fluree = &fluree;
        async move {
            fluree
                .graph(id)
                .transact()
                .sparql_update(&text)
                .commit()
                .await
                .unwrap_or_else(|e| panic!("{text}: {e}"))
                .receipt
                .retract_count
        }
    };
    update("INSERT DATA { ex:s ex:p (1 2) . GRAPH ex:g { ex:s ex:p (5), (1 2 3), (1 2) } }").await;
    assert_eq!(
        update("DELETE WHERE { GRAPH ex:g { ex:s ex:p (1 2) } }").await,
        2
    );
    assert_eq!(
        update("DELETE WHERE { GRAPH ex:g { ex:s ex:p (5), (1 2) } }").await,
        0,
        "a collection that matches no list matches nothing"
    );
    let ledger = fluree.ledger(id).await.expect("load");
    let g = ledger
        .snapshot
        .graph_registry
        .assigned_graph_id(&format!("{EX}g"))
        .expect("graph");
    let (s, p) = (format!("{EX}s"), format!("{EX}p"));
    assert_eq!(
        support::stored_int_lists_in(&ledger, g, &s, &p).await,
        [vec![1, 2, 3], vec![5]]
    );
    assert_eq!(
        support::stored_int_lists_in(&ledger, 0, &s, &p).await,
        [[1, 2]]
    );
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
            .assigned_graph_id(&format!("{EX}{name}"))
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

/// Inserts `ttl` into ledger `id` as it stands, or the refusal's text.
#[cfg(feature = "shacl")]
async fn insert_or_refusal(
    fluree: &fluree_db_api::Fluree,
    id: &str,
    ttl: &str,
) -> Result<(), String> {
    let ledger = fluree.ledger(id).await.expect("load");
    fluree
        .insert_turtle(ledger, &format!("@prefix ex: <{EX}> .\n{ttl}"))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
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
            insert_or_refusal(
                fluree,
                id,
                &format!("ex:{focus} a ex:{class} ; ex:a ex:n1 ."),
            )
            .await
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

/// Each `sh:or`, `sh:xone` or `sh:and` list on a shape is a constraint of its
/// own: `sh:or (ex:A ex:B), (ex:C ex:D)` needs one of A or B and one of C or
/// D, not one of the four, whether a list is compact or a stored chain (the
/// `ex:note` keeps `_:c` a chain). In novelty and in the index.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn each_logical_list_on_a_shape_is_its_own_constraint() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-identity/shacl-logical:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = support::enable_shacl(&fluree, ledger).await;
    turtle(
        &fluree,
        ledger,
        r"@prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:A a sh:NodeShape ; sh:property [ sh:path ex:a ; sh:minCount 1 ] .
        ex:B a sh:NodeShape ; sh:property [ sh:path ex:b ; sh:minCount 1 ] .
        ex:C a sh:NodeShape ; sh:property [ sh:path ex:c ; sh:minCount 1 ] .
        ex:D a sh:NodeShape ; sh:property [ sh:path ex:d ; sh:minCount 1 ] .
        ex:Or sh:targetClass ex:OrThing ; sh:or (ex:A ex:B), (ex:C ex:D) .
        ex:Xone sh:targetClass ex:XoneThing ; sh:xone (ex:A ex:B), (ex:C ex:D) .
        ex:And sh:targetClass ex:AndThing ; sh:and (ex:A ex:B), (ex:C ex:D) .
        ex:Mixed sh:targetClass ex:MixedThing ; sh:or (ex:A ex:B), _:c .
        _:c rdf:first ex:C ; rdf:rest _:d ; ex:note 0 .
        _:d rdf:first ex:D ; rdf:rest rdf:nil .",
    )
    .await;

    // A focus of each class, the properties it has, and whether it conforms.
    let cases = [
        ("OrThing", "a", false),
        ("OrThing", "a c", true),
        ("XoneThing", "a c", true),
        ("XoneThing", "a", false),
        ("AndThing", "a b c", false),
        ("AndThing", "a b c d", true),
        ("MixedThing", "a", false),
        ("MixedThing", "a c", true),
    ];
    for lane in ["novelty", "indexed"] {
        if lane == "indexed" {
            support::rebuild_and_publish_index(&fluree, id).await;
        }
        for (n, (class, props, conforms)) in cases.into_iter().enumerate() {
            let values: String = props.split(' ').map(|p| format!(" ; ex:{p} 1")).collect();
            let written =
                insert_or_refusal(&fluree, id, &format!("ex:{lane}{n} a ex:{class}{values} ."))
                    .await;
            assert_eq!(
                written.is_ok(),
                conforms,
                "{lane}: {class} with {props}: {written:?}"
            );
        }
    }
}

/// A `sh:or` value with no list may head a list whose cells another shapes
/// graph holds. Such heads resolve once every shapes graph is read, in
/// either order, and stay two lists: two disjunctions, not one.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn logical_list_heads_resolve_across_shapes_graphs() {
    use fluree_db_api::validate::{ShapesSource, ValidateOptions};
    let shapes = "ex:A a sh:NodeShape ; sh:property [ sh:path ex:a ; sh:minCount 1 ] .
        ex:B a sh:NodeShape ; sh:property [ sh:path ex:b ; sh:minCount 1 ] .
        ex:C a sh:NodeShape ; sh:property [ sh:path ex:c ; sh:minCount 1 ] .
        ex:D a sh:NodeShape ; sh:property [ sh:path ex:d ; sh:minCount 1 ] .
        ex:S sh:targetClass ex:T ; sh:or ex:l1, ex:l2 .";
    let cells =
        "ex:l1 rdf:first ex:A ; rdf:rest ex:l1b . ex:l1b rdf:first ex:B ; rdf:rest rdf:nil .
        ex:l2 rdf:first ex:C ; rdf:rest ex:l2b . ex:l2b rdf:first ex:D ; rdf:rest rdf:nil .";
    // The ledger's default graph is read first, the named graph second.
    for (order, default, named) in [
        ("shape-first", shapes, cells),
        ("cells-first", cells, shapes),
    ] {
        let fluree = FlureeBuilder::memory().build_memory();
        let id = format!("it/list-identity/shacl-graphs-{order}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create");
        let trig = format!(
            "@prefix ex: <{EX}> .
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            {default}
            ex:x a ex:T ; ex:a 1 .
            ex:y a ex:T ; ex:a 1 ; ex:c 1 .
            GRAPH <{EX}g> {{ {named} }}"
        );
        fluree
            .stage_owned(ledger)
            .upsert_turtle(&trig)
            .execute()
            .await
            .unwrap_or_else(|e| panic!("{order}: {e}"));
        let options = ValidateOptions {
            shapes: ShapesSource::Graph(format!("{EX}g")),
            include_attached: true,
            ..Default::default()
        };
        let report = fluree
            .validate_ledger(&id, &options)
            .await
            .expect("validate");
        let failing: Vec<_> = report
            .results
            .iter()
            .filter_map(|r| r.focus_node.as_str())
            .collect();
        assert_eq!(failing, [format!("{EX}x")], "{order}: {report:?}");
    }
}
