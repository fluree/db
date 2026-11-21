//! A subject the index doesn't hold is looked up once.
//!
//! A bound subject is looked up in the subject dictionary by its `Sid`'s
//! parts, and on a miss by its IRI, whose key may differ (an IRI stored whole,
//! or a split the ledger no longer makes). Under canonical encoding the IRI's
//! split is usually the parts that just missed, and searching the same leaf
//! for the same key again used to follow every miss, up to three times: for
//! each new subject a write stages, and each absent IRI a query names. Each
//! test counts the subject dictionary's leaf lookups and checks the answer.
//! Every IRI looked up sorts between `ex:a` and `ex:z`, so its lookup reaches a
//! leaf and is counted.

use crate::support;
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState, QueryInput};
use fluree_db_binary_index::BinaryIndexStore;
use serde_json::{json, Value};

const PREFIXES: &str = "PREFIX ex: <http://example.org/>\n";

async fn indexed(fluree: &fluree_db_api::Fluree, id: &str) -> LedgerState {
    let ledger = fluree.create_ledger(id).await.expect("create");
    fluree
        .insert_turtle(
            ledger,
            "@prefix ex: <http://example.org/> .
             ex:a ex:name \"a\" .
             ex:b ex:name \"b\" .
             ex:z ex:name \"z\" .",
        )
        .await
        .expect("insert");
    support::rebuild_and_publish_index(fluree, id).await;
    fluree.ledger(id).await.expect("reload")
}

fn subject_lookups(ledger: &LedgerState) -> u64 {
    ledger
        .binary_store
        .as_ref()
        .expect("indexed ledger")
        .0
        .downcast_ref::<BinaryIndexStore>()
        .expect("binary store")
        .subject_reverse_leaf_lookups()
}

/// The query's rows, sorted, and the subject leaf lookups it made.
async fn sparql(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    body: &str,
) -> (Vec<Value>, u64) {
    let q = format!("{PREFIXES}{body}");
    let before = subject_lookups(ledger);
    let result = support::query_sparql(fluree, ledger, &q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let lookups = subject_lookups(ledger) - before;
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    (support::normalize_rows(&rows), lookups)
}

#[tokio::test]
async fn a_query_looks_an_absent_subject_up_once() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed(&fluree, "it/subject-lookup-misses/query:main").await;

    let (rows, lookups) =
        sparql(&fluree, &ledger, "SELECT ?o WHERE { ex:absent ex:name ?o }").await;
    assert!(rows.is_empty(), "{rows:?}");
    assert_eq!(lookups, 1, "an absent subject was looked up again");

    // Each row's IRI is looked up by the join and again by its scan, which
    // is not told the join's lookup missed: two each, no more.
    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?s ?o WHERE { VALUES ?s { ex:x1 ex:x2 ex:x3 } ?s ex:name ?o }",
    )
    .await;
    assert!(rows.is_empty(), "{rows:?}");
    assert_eq!(lookups, 6, "absent VALUES subjects were looked up again");

    let (rows, lookups) = sparql(
        &fluree,
        &ledger,
        "SELECT ?s ?o WHERE { VALUES ?s { ex:a ex:x1 } ?s ex:name ?o }",
    )
    .await;
    assert_eq!(rows, support::normalize_rows(&json!([["ex:a", "a"]])));
    assert_eq!(lookups, 3, "ex:a's scan takes the join's id");

    let (rows, lookups) = sparql(&fluree, &ledger, "SELECT ?o WHERE { ex:a ex:name ?o }").await;
    assert_eq!(rows, support::normalize_rows(&json!([["a"]])));
    assert_eq!(lookups, 1);
}

#[tokio::test]
async fn staging_looks_each_new_subject_up_once() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = indexed(&fluree, "it/subject-lookup-misses/stage:main").await;
    let before = subject_lookups(&ledger);
    let staged = fluree
        .stage_owned(ledger.clone())
        .upsert_turtle(
            "@prefix ex: <http://example.org/> .
             ex:n1 ex:name \"n1\" .
             ex:n2 ex:name \"n2\" .
             ex:n3 ex:name \"n3\" .",
        )
        .stage()
        .await
        .expect("stage");
    assert_eq!(
        subject_lookups(&ledger) - before,
        3,
        "a new subject was looked up again"
    );

    let preview = GraphDb::from_staged(&staged).expect("preview");
    let out = fluree
        .query(
            &preview,
            QueryInput::Sparql(&format!("{PREFIXES}SELECT ?s ?o WHERE {{ ?s ex:name ?o }}")),
        )
        .await
        .expect("query");
    let rows = out.to_jsonld(&preview.snapshot).expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&rows),
        support::normalize_rows(&json!([
            ["ex:a", "a"],
            ["ex:b", "b"],
            ["ex:n1", "n1"],
            ["ex:n2", "n2"],
            ["ex:n3", "n3"],
            ["ex:z", "z"],
        ])),
        "the upsert added the new subjects"
    );
}
