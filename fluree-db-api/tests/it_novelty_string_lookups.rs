//! A string only novelty holds is not looked up in the persisted tree.
//!
//! Overlay translation and object binding give each novelty string its
//! dictionary id. The novelty dictionary holds only strings the persisted
//! tree doesn't, so asking it first changes no answer, and spares each
//! novelty string the persisted lookup: for a long string, a SHA-256 and a
//! probe for a key the tree can't hold. The test counts the string tree's
//! leaf lookups over a query that reads novelty strings.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use fluree_db_binary_index::BinaryIndexStore;
use serde_json::json;

fn string_lookups(ledger: &LedgerState) -> u64 {
    ledger
        .binary_store
        .as_ref()
        .expect("indexed ledger")
        .0
        .downcast_ref::<BinaryIndexStore>()
        .expect("binary store")
        .string_reverse_leaf_lookups()
}

/// A note long enough to be keyed by hash, starting with `stem`.
fn note(stem: &str, i: usize) -> String {
    format!("{stem} note {i:03}: {}", "words ".repeat(16))
}

#[tokio::test]
async fn a_novelty_string_skips_the_persisted_tree() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/novelty-string-lookups:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    // Indexed strings on either side of the novelty ones, so a lookup of
    // those would reach a leaf and be counted.
    fluree
        .insert_turtle(
            ledger,
            &format!(
                "@prefix ex: <http://example.org/> .
                 ex:a ex:note \"{}\" .
                 ex:z ex:note \"{}\" .",
                note("aaa", 0),
                note("zzz", 0)
            ),
        )
        .await
        .expect("insert");
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload");

    let novelty: String = (0..20)
        .map(|i| format!("ex:m{i} ex:note \"{}\" .\n", note("mmm", i)))
        .collect();
    let ledger = fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <http://example.org/> .\n{novelty}"),
        )
        .await
        .expect("insert novelty")
        .ledger;

    let before = string_lookups(&ledger);
    let result = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/>
         SELECT (COUNT(?t) AS ?n) WHERE { ?s ex:note ?t FILTER(STRLEN(?t) > 10) }",
    )
    .await
    .expect("query");
    let lookups = string_lookups(&ledger) - before;
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        support::normalize_rows(&rows),
        support::normalize_rows(&json!([[22]]))
    );
    assert_eq!(
        lookups, 0,
        "novelty strings were looked up in the persisted tree"
    );
}
