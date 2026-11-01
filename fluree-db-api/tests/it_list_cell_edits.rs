//! A write that reads lists as RDF (`# PRAGMA rdf-lists: true`, JSON-LD
//! `opts.rdfLists`) edits a compact list through its cells. A cell's
//! `rdf:first` replaced replaces its member at the same place, in the same
//! list, and nothing else is rewritten; every other edit naming a cell is
//! refused and changes nothing. Lists stored as explicit chains are triples,
//! edited as any triple is.

use crate::support;
use fluree_db_api::{policy_builder, FlureeBuilder, GovernanceOptions, LedgerState, PolicyContext};
use fluree_db_core::FlakeValue;
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";
const PREFIX: &str = "PREFIX ex: <http://example.org/>\n\
                      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
const CELLS: &str = "# PRAGMA rdf-lists: true\n";

async fn create(fluree: &fluree_db_api::Fluree, name: &str, ttl: &str) -> String {
    let id = format!("it/list-cell-edits/{name}:main");
    fluree.create_ledger(&id).await.expect("create");
    fluree
        .graph(&id)
        .transact()
        .insert_turtle(&format!("@prefix ex: <{EX}> .\n{ttl}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"));
    id
}

async fn load(fluree: &fluree_db_api::Fluree, id: &str) -> LedgerState {
    fluree.ledger(id).await.expect("load")
}

async fn update(
    fluree: &fluree_db_api::Fluree,
    id: &str,
    body: &str,
    policy: Option<PolicyContext>,
) -> fluree_db_api::Result<()> {
    let text = format!("{PREFIX}{body}");
    let graph = fluree.graph(id);
    let txn = graph.transact().sparql_update(&text);
    let txn = match policy {
        Some(policy) => txn.policy(policy),
        None => txn,
    };
    txn.commit().await.map(|_| ())
}

/// `body` in the RDF view of lists, committed.
async fn edit(fluree: &fluree_db_api::Fluree, id: &str, body: &str) {
    update(fluree, id, &format!("{CELLS}{body}"), None)
        .await
        .unwrap_or_else(|e| panic!("edit: {e}\n{body}"));
}

/// `body` in the RDF view of lists, refused with a message holding
/// `expected`, leaving what `ex:s ex:items` holds as it was.
async fn refused(fluree: &fluree_db_api::Fluree, id: &str, body: &str, expected: &str) {
    let before = held(&load(fluree, id).await, 0, "s", "items").await;
    let err = update(fluree, id, &format!("{CELLS}{body}"), None)
        .await
        .expect_err(body);
    assert!(err.to_string().contains(expected), "{body}: {err}");
    assert_eq!(err.status_code(), 400, "{body}: {err}");
    let after = held(&load(fluree, id).await, 0, "s", "items").await;
    assert_eq!(after, before, "{body}");
}

/// What `ex:{s} ex:{p}` holds in graph `g_id`, each value with its place,
/// in place order; values with no place last.
async fn held(
    ledger: &LedgerState,
    g_id: fluree_db_core::GraphId,
    s: &str,
    p: &str,
) -> Vec<(Option<(u32, i32)>, String)> {
    use fluree_db_core::range::{RangeMatch, RangeTest};
    let snapshot = &ledger.snapshot;
    let s = snapshot.encode_iri(&format!("{EX}{s}")).expect("subject");
    let p = snapshot.encode_iri(&format!("{EX}{p}")).expect("predicate");
    let mut held: Vec<_> = ledger
        .as_graph_db_ref(g_id)
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
        .map(|f| {
            let value = match &f.o {
                FlakeValue::String(s) => s.to_string(),
                FlakeValue::Long(n) => n.to_string(),
                other => format!("{other:?}"),
            };
            let value = match f.m.as_ref().and_then(|m| m.lang.as_deref()) {
                Some(lang) => format!("{value}@{lang}"),
                None => value,
            };
            (
                f.m.as_ref().and_then(fluree_db_core::FlakeMeta::place),
                value,
            )
        })
        .collect();
    held.sort_by_key(|(place, value)| (place.is_none(), *place, value.clone()));
    held
}

fn at(list: u32, position: i32, value: &str) -> (Option<(u32, i32)>, String) {
    (Some((list, position)), value.to_string())
}

fn plain(value: &str) -> (Option<(u32, i32)>, String) {
    (None, value.to_string())
}

async fn select(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    text: &str,
) -> Vec<JsonValue> {
    let result = support::query_sparql(fluree, ledger, &format!("{PREFIX}{text}"))
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

/// A cell's `rdf:first` replaced replaces its member in its list at its
/// position, whichever pattern reached the cell: the owner edge, a fixed
/// `rdf:rest` step, or `rdf:rest*`. Other lists of the property, its plain
/// value, and other owners' lists are left as they are; a `SELECT` under the
/// same view previews the WHERE. In novelty, then over an index.
#[tokio::test]
async fn a_cells_first_replaced_replaces_its_member_in_place() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "replace",
        r#"ex:alice ex:colors ("red" "green" "blue"), ("teal"), "plain" .
           ex:bob ex:colors ("red" "green") ."#,
    )
    .await;
    let heads = "SELECT ?old WHERE { ex:alice ex:colors ?head . ?head rdf:first ?old }";
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        select(&fluree, &ledger, &format!("{CELLS}{heads}")).await,
        [json!(["red"]), json!(["teal"])],
        "preview"
    );

    edit(
        &fluree,
        &id,
        r#"DELETE { ?head rdf:first ?old } INSERT { ?head rdf:first "navy" }
           WHERE { ex:alice ex:colors ?head . ?head rdf:first ?old FILTER(?old = "red") }"#,
    )
    .await;
    edit(
        &fluree,
        &id,
        r#"DELETE { ?c rdf:first ?old } INSERT { ?c rdf:first "lime" }
           WHERE { ex:alice ex:colors/rdf:rest ?c . ?c rdf:first ?old }"#,
    )
    .await;
    edit(
        &fluree,
        &id,
        r#"DELETE { ?c rdf:first "blue" } INSERT { ?c rdf:first "sky" }
           WHERE { ?s ex:colors/rdf:rest* ?c . ?c rdf:first "blue" }"#,
    )
    .await;
    let alice = [
        at(0, 0, "navy"),
        at(0, 1, "lime"),
        at(0, 2, "sky"),
        at(1, 0, "teal"),
        plain("plain"),
    ];
    let bob = [at(0, 0, "red"), at(0, 1, "green")];
    let ledger = load(&fluree, &id).await;
    assert_eq!(held(&ledger, 0, "alice", "colors").await, alice);
    assert_eq!(held(&ledger, 0, "bob", "colors").await, bob);

    support::rebuild_and_publish_index(&fluree, &id).await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(held(&ledger, 0, "alice", "colors").await, alice, "indexed");

    edit(
        &fluree,
        &id,
        r#"DELETE { ?h rdf:first ?old } INSERT { ?h rdf:first "türkis"@de }
           WHERE { ex:alice ex:colors ?h . ?h rdf:first "teal", ?old }"#,
    )
    .await;
    edit(
        &fluree,
        &id,
        r#"DELETE { ?c rdf:first ?old } INSERT { ?c rdf:first "olive" }
           WHERE { ex:bob ex:colors/rdf:rest ?c . ?c rdf:first ?old }"#,
    )
    .await;
    let ledger = load(&fluree, &id).await;
    let alice = [
        at(0, 0, "navy"),
        at(0, 1, "lime"),
        at(0, 2, "sky"),
        at(1, 0, "türkis@de"),
        plain("plain"),
    ];
    assert_eq!(
        held(&ledger, 0, "alice", "colors").await,
        alice,
        "over an index"
    );
    let bob = [at(0, 0, "red"), at(0, 1, "olive")];
    assert_eq!(
        held(&ledger, 0, "bob", "colors").await,
        bob,
        "over an index"
    );
    assert_eq!(
        select(&fluree, &ledger, "SELECT ?c WHERE { ex:bob ex:colors ?c }").await,
        [json!(["olive"]), json!(["red"])],
        "members"
    );
}

/// A cell names one member, so an edit reaches it among repeats; a list keeps
/// its identity when an edit leaves it equal to another list.
#[tokio::test]
async fn a_cell_edit_keeps_repeats_and_list_identity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "identity", "ex:s ex:items (1 1 2), (1 3 2) .").await;
    edit(
        &fluree,
        &id,
        "DELETE { ?c rdf:first 1 } INSERT { ?c rdf:first 9 }
         WHERE { ex:s ex:items ?h . ?h rdf:first 1 ; rdf:rest ?c . ?c rdf:first 1 }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [
            at(0, 0, "1"),
            at(0, 1, "9"),
            at(0, 2, "2"),
            at(1, 0, "1"),
            at(1, 1, "3"),
            at(1, 2, "2"),
        ]
    );
    edit(
        &fluree,
        &id,
        "DELETE { ?c rdf:first 3 } INSERT { ?c rdf:first 9 }
         WHERE { ex:s ex:items/rdf:rest ?c . ?c rdf:first 3 }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [
            at(0, 0, "1"),
            at(0, 1, "9"),
            at(0, 2, "2"),
            at(1, 0, "1"),
            at(1, 1, "9"),
            at(1, 2, "2"),
        ]
    );
}

/// The edits are taken together across solutions: a cell two solutions
/// replace alike is replaced once, and a cell left with its member is
/// unchanged.
#[tokio::test]
async fn solutions_that_agree_edit_a_cell_once() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "agree", "ex:s ex:items (1 2) .").await;
    edit(
        &fluree,
        &id,
        "DELETE { ?h rdf:first ?old } INSERT { ?h rdf:first 5 }
         WHERE { { ex:s ex:items ?h } UNION { ex:s ex:items ?h } ?h rdf:first ?old }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );

    let same = "DELETE { ?h rdf:first ?old } INSERT { ?h rdf:first ?old }
                WHERE { ex:s ex:items ?h . ?h rdf:first ?old }";
    update(&fluree, &id, &format!("{CELLS}{same}"), None)
        .await
        .expect("a write that changes nothing");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );
}

/// Every edit naming a cell other than its `rdf:first` replaced is refused,
/// and the write changes nothing.
#[tokio::test]
async fn other_cell_edits_are_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "refused", "ex:s ex:items (1 2), 9 .").await;
    let cases = [
        (
            "DELETE { ?h rdf:first ?o } WHERE { ex:s ex:items ?h . ?h rdf:first ?o }",
            "inserts none",
        ),
        (
            "INSERT { ?h rdf:first 7 } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "2 rdf:first values",
        ),
        (
            "DELETE { ?h rdf:first ?o } INSERT { ?h rdf:first ?n }
             WHERE { ex:s ex:items ?h . ?h rdf:first ?o VALUES ?n { 7 8 } }",
            "2 rdf:first values",
        ),
        (
            "DELETE { ?h rdf:rest ?r } INSERT { ?h rdf:rest rdf:nil }
             WHERE { ex:s ex:items ?h . ?h rdf:rest ?r }",
            "rdf:rest",
        ),
        (
            "DELETE { ex:s ex:items ?h } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a value",
        ),
        (
            "INSERT { ex:t ex:ref ?h } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a value",
        ),
        (
            "INSERT { ?h ex:note \"x\" } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "only rdf:first and rdf:rest",
        ),
        (
            "INSERT { ex:t ex:items ( ?h ) } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a value",
        ),
        (
            "INSERT { ex:t ?h 7 } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a property",
        ),
        (
            "DELETE { ex:t ?h 7 } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a property",
        ),
        (
            "INSERT { ex:t ex:says <<( ex:a ?h 1 )>> } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
            "not a value",
        ),
    ];
    for (body, expected) in cases {
        refused(&fluree, &id, body, expected).await;
    }
}

/// A cell's new member is in its list, so a list the same write states takes
/// the next id past it, or is that list when it holds the same members.
#[tokio::test]
async fn a_list_written_beside_a_cell_edit_counts_the_edited_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let replace = "DELETE { ?h rdf:first ?old } INSERT { ?h rdf:first ?new . ex:s ex:items ?list }
                   WHERE { ex:s ex:items ?h . ?h rdf:first ?old }";
    let cases = [
        ("(1)", "2", "(3)", vec![at(0, 0, "2"), at(1, 0, "3")]),
        ("(1)", "2", "(1)", vec![at(0, 0, "2"), at(1, 0, "1")]),
        ("(1)", "2", "(2)", vec![at(0, 0, "2")]),
    ];
    for (n, (stored, new, list, expected)) in cases.into_iter().enumerate() {
        let id = create(
            &fluree,
            &format!("beside-{n}"),
            &format!("ex:s ex:items {stored} ."),
        )
        .await;
        let body = replace
            .replace("?new", new)
            .replace("ex:s ex:items ?list", &format!("ex:s ex:items {list}"));
        edit(&fluree, &id, &body).await;
        assert_eq!(
            held(&load(&fluree, &id).await, 0, "s", "items").await,
            expected,
            "{body}"
        );
    }
}

/// Read as RDF, a list's members are its cells' `rdf:first`s, not values of
/// its property: a delete naming a value takes the plain value alone, however
/// it is written, and a list in a DELETE template is refused.
#[tokio::test]
async fn rdf_lists_deletes_a_propertys_plain_values_alone() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "plain-deletes", "ex:s ex:items (1 2), 1, 3 .").await;
    edit(
        &fluree,
        &id,
        "DELETE { ex:s ex:items 1 } WHERE { ex:s ex:items ?h . ?h rdf:first 1 }",
    )
    .await;
    let list = [at(0, 0, "1"), at(0, 1, "2")];
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [list[0].clone(), list[1].clone(), plain("3")]
    );
    edit(&fluree, &id, "DELETE DATA { ex:s ex:items 2 }").await;
    edit(
        &fluree,
        &id,
        "DELETE { ex:s ex:items ?v } WHERE { ex:s ex:items ?v FILTER(isLiteral(?v)) }",
    )
    .await;
    assert_eq!(held(&load(&fluree, &id).await, 0, "s", "items").await, list);

    let deleted_list = json!({
        "@context": {"ex": EX},
        "delete": {"@id": "ex:s", "ex:items": {"@list": [1, 2]}},
        "opts": {"rdfLists": true}
    });
    let err = fluree
        .graph(&id)
        .transact()
        .update(&deleted_list)
        .commit()
        .await
        .expect_err("a list deleted");
    assert!(
        err.to_string().contains("a list in a DELETE template"),
        "{err}"
    );
    assert_eq!(err.status_code(), 400, "{err}");
    assert_eq!(held(&load(&fluree, &id).await, 0, "s", "items").await, list);

    // Members are values without the view.
    update(&fluree, &id, "DELETE DATA { ex:s ex:items 1 }", None)
        .await
        .expect("member delete");
    assert_eq!(
        held(&load(&fluree, &id).await, 0, "s", "items").await,
        [at(0, 1, "2")]
    );
}

/// A cell is in the graph of its list: its `rdf:first` is replaced there, and
/// written in another graph is refused.
#[tokio::test]
async fn a_cell_is_edited_in_its_lists_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "graph", "GRAPH ex:g { ex:s ex:items (1 2) . }").await;
    let ledger = load(&fluree, &id).await;
    let g = ledger
        .snapshot
        .graph_registry
        .graph_id_for_iri(&format!("{EX}g"))
        .expect("graph");
    let err = update(
        &fluree,
        &id,
        &format!(
            "{CELLS}DELETE {{ GRAPH ex:g {{ ?h rdf:first ?o }} }} INSERT {{ ?h rdf:first 5 }}
             WHERE {{ GRAPH ex:g {{ ex:s ex:items ?h . ?h rdf:first ?o }} }}"
        ),
        None,
    )
    .await
    .expect_err("another graph");
    assert!(err.to_string().contains("graph of the list"), "{err}");

    edit(
        &fluree,
        &id,
        "DELETE { GRAPH ex:g { ?h rdf:first ?o } } INSERT { GRAPH ex:g { ?h rdf:first 5 } }
         WHERE { GRAPH ex:g { ex:s ex:items ?h . ?h rdf:first ?o } }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, g, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );
}

/// Without the RDF view a list's members are values, so a WHERE reaching for
/// a cell matches nothing and the write changes nothing.
#[tokio::test]
async fn without_rdf_lists_a_where_meets_no_cell() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "member-view", "ex:s ex:items (1 2) .").await;
    let body = "DELETE { ?h rdf:first ?old } INSERT { ?h rdf:first 5 }
                WHERE { ex:s ex:items ?h . ?h rdf:first ?old }";
    update(&fluree, &id, body, None)
        .await
        .expect("a write that changes nothing");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "1"), at(0, 1, "2")]
    );
}

/// A list stored as an explicit chain of named cells is triples: in the RDF
/// view its `rdf:first` and `rdf:rest` are written as any triple is.
#[tokio::test]
async fn an_explicit_chain_is_edited_as_triples() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "chain",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
         ex:s ex:items ex:c1 .
         ex:c1 rdf:first 1 ; rdf:rest ex:c2 .
         ex:c2 rdf:first 2 ; rdf:rest rdf:nil .",
    )
    .await;
    let lists = |ledger: LedgerState| async move {
        support::stored_int_lists(&ledger, &format!("{EX}s"), &format!("{EX}items")).await
    };
    edit(
        &fluree,
        &id,
        "DELETE { ?c rdf:first 2 } INSERT { ?c rdf:first 5 }
         WHERE { ex:s ex:items/rdf:rest ?c . ?c rdf:first 2 }",
    )
    .await;
    assert_eq!(lists(load(&fluree, &id).await).await, [[1, 5]]);
    edit(
        &fluree,
        &id,
        "DELETE { ex:c1 rdf:rest ?r } INSERT { ex:c1 rdf:rest rdf:nil } WHERE { ex:c1 rdf:rest ?r }",
    )
    .await;
    assert_eq!(lists(load(&fluree, &id).await).await, [[1]]);
}

async fn denying_modify_of(ledger: &LedgerState, property: &str) -> PolicyContext {
    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/deny",
            "f:required": true,
            "f:onProperty": [{"@id": property}],
            "f:action": "f:modify",
            "f:allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    };
    policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("policy")
}

/// A cell edit is a change to the property holding the list, and modify
/// policy judges it there, not on `rdf:first`.
#[tokio::test]
async fn modify_policy_judges_a_cell_edit_on_its_owners_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "policy", "ex:s ex:items (1 2) .").await;
    let body = format!(
        "{CELLS}DELETE {{ ?h rdf:first ?old }} INSERT {{ ?h rdf:first 5 }}
         WHERE {{ ex:s ex:items ?h . ?h rdf:first ?old }}"
    );
    let ledger = load(&fluree, &id).await;
    let items = denying_modify_of(&ledger, &format!("{EX}items")).await;
    update(&fluree, &id, &body, Some(items))
        .await
        .expect_err("ex:items may not change");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "1"), at(0, 1, "2")]
    );

    let first = denying_modify_of(&ledger, fluree_vocab::rdf::FIRST).await;
    update(&fluree, &id, &body, Some(first))
        .await
        .expect("rdf:first is not what changes");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );
}

/// The JSON-LD twin: `opts.rdfLists` reads and edits cells as the pragma
/// does, and a value that is not a boolean, or an upsert, is refused.
#[tokio::test]
async fn opts_rdf_lists_edits_cells_in_json_ld() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "jsonld", "ex:s ex:items (1 2) .").await;
    let txn = |opts: JsonValue| {
        json!({
            "@context": {"ex": EX, "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
            "where": [
                {"@id": "ex:s", "ex:items": "?h"},
                {"@id": "?h", "rdf:first": "?old"}
            ],
            "delete": {"@id": "?h", "rdf:first": "?old"},
            "insert": {"@id": "?h", "rdf:first": 5},
            "opts": opts
        })
    };
    fluree
        .graph(&id)
        .transact()
        .update(&txn(json!({"rdfLists": true})))
        .commit()
        .await
        .expect("update");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );

    let err = fluree
        .graph(&id)
        .transact()
        .update(&txn(json!({"rdfLists": "yes"})))
        .commit()
        .await
        .expect_err("not a boolean");
    assert!(err.to_string().contains("opts.rdfLists"), "{err}");

    // A list the write states holding a cell.
    let nested = json!({
        "@context": {"ex": EX, "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        "where": [{"@id": "ex:s", "ex:items": "?h"}, {"@id": "?h", "rdf:first": 5}],
        "insert": {"@id": "ex:t", "ex:items": {"@list": ["?h"]}},
        "opts": {"rdfLists": true}
    });
    let err = fluree
        .graph(&id)
        .transact()
        .update(&nested)
        .commit()
        .await
        .expect_err("a cell in a stated list");
    assert!(err.to_string().contains("names a list cell"), "{err}");

    // An upsert replaces the property's values, its lists' owner edges among
    // them.
    let upsert = json!({
        "@context": {"ex": EX},
        "@id": "ex:s",
        "ex:items": 9,
        "opts": {"rdfLists": true}
    });
    let err = fluree
        .graph(&id)
        .transact()
        .upsert(&upsert)
        .commit()
        .await
        .expect_err("an upsert");
    assert!(
        err.to_string().contains("does not apply to an upsert"),
        "{err}"
    );
    assert_eq!(err.status_code(), 400, "{err}");
    let ledger = load(&fluree, &id).await;
    assert_eq!(
        held(&ledger, 0, "s", "items").await,
        [at(0, 0, "5"), at(0, 1, "2")]
    );
}

/// An `sh:select` query reads lists as the write it judges does, so asking it
/// for cells is refused rather than ignored.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn an_sh_select_refuses_rdf_lists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/list-cell-edits/sh-select:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let ledger = support::enable_shacl(&fluree, ledger).await;
    let ledger = fluree
        .insert_turtle(
            ledger,
            &format!(
                r##"@prefix ex: <{EX}> . @prefix sh: <http://www.w3.org/ns/shacl#> .
                ex:Shape sh:targetClass ex:Thing ; sh:sparql ex:check .
                ex:check sh:select """# PRAGMA rdf-lists: true
                    SELECT $this WHERE {{ $this <{EX}items> ?o FILTER(false) }}""" ."##
            ),
        )
        .await
        .expect("shape")
        .ledger;
    let err = fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <{EX}> . ex:t a ex:Thing ; ex:items (1) ."),
        )
        .await
        .expect_err("refused");
    assert!(
        err.to_string().contains("rdf-lists does not apply"),
        "{err}"
    );
}
