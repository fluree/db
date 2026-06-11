//! Regression test for fluree/db#1310: a `VALUES`-bound `rdf:type` pattern
//! must not resurface logically deleted (retracted) subjects.
//!
//! Repro shape: populate a ledger, wildcard-DELETE everything, re-insert a
//! smaller set. Counting class members with a constant class IRI and with the
//! class bound via `VALUES` must agree.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::json;

fn parts_graph(prefix: &str, n: usize) -> serde_json::Value {
    let nodes: Vec<serde_json::Value> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:{prefix}{i}"),
                "@type": "ex:Part",
                "ex:name": format!("{prefix} {i}")
            })
        })
        .collect();
    json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": nodes
    })
}

async fn count_constant(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> i64 {
    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s a ex:Part }
    ";
    extract_count(fluree, ledger, q).await
}

async fn count_values(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> i64 {
    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(DISTINCT ?s) AS ?n)
        WHERE { VALUES ?c { ex:Part } ?s a ?c }
    ";
    extract_count(fluree, ledger, q).await
}

async fn list_constant_subjects(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
) -> Vec<String> {
    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?s WHERE { ?s a ex:Part }
    ";
    list_subjects(fluree, ledger, q).await
}

async fn list_values_subjects(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> Vec<String> {
    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?s WHERE { ?s a ?c . VALUES ?c { ex:Part } }
    ";
    list_subjects(fluree, ledger, q).await
}

async fn list_subjects(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    q: &str,
) -> Vec<String> {
    let result = support::query_sparql(fluree, ledger, q)
        .await
        .expect("VALUES list query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let mut subjects: Vec<String> = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .iter()
        .map(|b| b["s"]["value"].as_str().expect("iri").to_string())
        .collect();
    subjects.sort();
    subjects
}

async fn extract_count(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, q: &str) -> i64 {
    let result = support::query_sparql(fluree, ledger, q)
        .await
        .expect("count query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let bindings = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(bindings.len(), 1, "single aggregate row: {sparql_json}");
    bindings[0]["n"]["value"]
        .as_str()
        .expect("count literal")
        .parse()
        .expect("count parses")
}

async fn wipe_ledger(fluree: &fluree_db_api::Fluree, ledger: LedgerState) -> LedgerState {
    let delete_all = json!({
        "@context": { "ex": "http://example.org/" },
        "where":  { "@id": "?s", "?p": "?o" },
        "delete": { "@id": "?s", "?p": "?o" }
    });
    fluree
        .update(ledger, &delete_all)
        .await
        .expect("wildcard delete")
        .ledger
}

async fn assert_shapes_agree(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    expected: i64,
    label: &str,
) {
    let constant = count_constant(fluree, ledger).await;
    let values = count_values(fluree, ledger).await;
    let listed_constant = list_constant_subjects(fluree, ledger).await;
    let listed_values = list_values_subjects(fluree, ledger).await;
    assert_eq!(constant, expected, "[{label}] constant-class count");
    assert_eq!(
        values, expected,
        "[{label}] VALUES-bound class count diverged (constant={constant}); \
         VALUES subjects: {listed_values:?}"
    );
    assert_eq!(
        listed_constant, listed_values,
        "[{label}] constant and VALUES shapes must list the same subjects"
    );
}

/// Novelty-only ledger (no binary index at any point).
#[tokio::test]
async fn values_bound_type_after_delete_novelty_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("values-deleted/novelty:main")
        .await
        .expect("create");

    let receipt = fluree
        .insert(ledger0, &parts_graph("old", 40))
        .await
        .expect("initial insert");
    assert_shapes_agree(&fluree, &receipt.ledger, 40, "pre-delete").await;

    let wiped = wipe_ledger(&fluree, receipt.ledger).await;
    assert_shapes_agree(&fluree, &wiped, 0, "post-delete").await;

    let receipt2 = fluree
        .insert(wiped, &parts_graph("new", 5))
        .await
        .expect("re-insert");
    assert_shapes_agree(&fluree, &receipt2.ledger, 5, "post-reinsert").await;
}

/// Binary index built from the initial population; delete + re-insert live in
/// novelty on top of the indexed (pre-delete) state.
#[tokio::test]
async fn values_bound_type_after_delete_indexed_before_delete() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let alias = "values-deleted/indexed-pre:main";
    let ledger0 = fluree.create_ledger(alias).await.expect("create");

    let receipt = fluree
        .insert(ledger0, &parts_graph("old", 40))
        .await
        .expect("initial insert");
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(alias).await.expect("reload");
    assert_shapes_agree(&fluree, &ledger, 40, "pre-delete indexed").await;

    let wiped = wipe_ledger(&fluree, ledger).await;
    assert_shapes_agree(&fluree, &wiped, 0, "post-delete").await;

    let receipt2 = fluree
        .insert(wiped, &parts_graph("new", 5))
        .await
        .expect("re-insert");
    let _ = receipt;
    assert_shapes_agree(&fluree, &receipt2.ledger, 5, "post-reinsert").await;
}

/// Binary index rebuilt after the delete + re-insert (fully indexed state).
#[tokio::test]
async fn values_bound_type_after_delete_indexed_after_reinsert() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let alias = "values-deleted/indexed-post:main";
    let ledger0 = fluree.create_ledger(alias).await.expect("create");

    let receipt = fluree
        .insert(ledger0, &parts_graph("old", 40))
        .await
        .expect("initial insert");

    let wiped = wipe_ledger(&fluree, receipt.ledger).await;
    let receipt2 = fluree
        .insert(wiped, &parts_graph("new", 5))
        .await
        .expect("re-insert");
    let _ = receipt2;

    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(alias).await.expect("reload");
    assert_shapes_agree(&fluree, &ledger, 5, "post-reinsert indexed").await;
}
