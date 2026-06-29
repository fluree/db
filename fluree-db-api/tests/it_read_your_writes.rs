//! Regression test for issue #1330: `fluree.db()` must reflect commits made
//! through the owned-state API even when a handle was already cached by an
//! earlier `db()` call.
//!
//! The owned-state path (`insert`/`update`/`transact(ledger, ...)`) commits and
//! publishes to the nameservice but returns a new `LedgerState`; before the fix
//! it never wrote that state back into the `LedgerManager` cache, so a later
//! `db()` served the pre-commit view (a read-your-writes violation).

#![cfg(feature = "native")]

use crate::support;
use crate::support::genesis_ledger_for_fluree;
use fluree_db_api::{FlureeBuilder, QueryInput};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({ "ex": "http://example.org/ns/" })
}

async fn names(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<serde_json::Value> {
    let view = fluree.db(ledger_id).await.expect("db view");
    let query = r"PREFIX ex: <http://example.org/ns/>
        SELECT ?name WHERE { ?s ex:name ?name } ORDER BY ?name";
    let result = fluree
        .query(&view, QueryInput::Sparql(query))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    support::normalize_rows(&jsonld)
}

#[tokio::test]
async fn db_reflects_owned_commit_after_cached_read() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/read-your-writes:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    let receipt = fluree
        .insert(
            ledger,
            &json!({ "@context": ctx(), "@id": "ex:alice", "ex:name": "alice" }),
        )
        .await
        .expect("insert alice");

    // First read populates the ledger-manager cache.
    let after_first = names(&fluree, ledger_id).await;
    assert_eq!(
        after_first.len(),
        1,
        "alice should be visible: {after_first:?}"
    );

    // Commit again through the owned-state API: the new state must be written
    // back into the cache so the next db() observes it.
    let receipt = fluree
        .insert(
            receipt.ledger,
            &json!({ "@context": ctx(), "@id": "ex:bob", "ex:name": "bob" }),
        )
        .await
        .expect("insert bob");

    let after_second = names(&fluree, ledger_id).await;
    assert_eq!(
        after_second.len(),
        2,
        "db() must reflect the bob commit (read-your-writes); got {after_second:?}"
    );

    // A third commit on top — the cache must keep advancing, not stick at t=2.
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({ "@context": ctx(), "@id": "ex:carol", "ex:name": "carol" }),
        )
        .await
        .expect("insert carol");

    let after_third = names(&fluree, ledger_id).await;
    assert_eq!(
        after_third.len(),
        3,
        "db() must reflect the carol commit; got {after_third:?}"
    );
}
