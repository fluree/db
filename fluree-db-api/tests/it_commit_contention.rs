//! A writer that loses every commit race gets typed, retryable contention
//! (503), not an internal error, and nothing is committed.

use crate::race_nameservice::PausingNameService;
use fluree_db_api::{
    ApiError, CommitOpts, Fluree, FlureeBuilder, NameServiceMode, TransactionOptions, TxnOperation,
};
use serde_json::json;
use std::sync::Arc;

const LEDGER: &str = "it/contention:main";

/// A ledger at t=1 whose every later commit-head CAS loses.
async fn contended() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    let ns = Arc::new(PausingNameService::new(
        fluree.nameservice_mode().publisher_arc().unwrap(),
    ));
    let fluree = Fluree::from_backend(
        fluree.config().clone(),
        fluree.backend().clone(),
        NameServiceMode::ReadWrite(ns.clone()),
    );
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(&person("alice"))
        .execute()
        .await
        .expect("uncontended commit");
    ns.lose_commit_races();
    fluree
}

fn person(id: &str) -> serde_json::Value {
    json!({ "@context": { "ex": "http://example.org/" }, "@id": format!("ex:{id}"), "ex:name": id })
}

fn assert_contention(err: &ApiError) {
    assert!(
        matches!(err, ApiError::CommitContention { attempts: 16, last }
            if matches!(**last, ApiError::Transact(_))),
        "{err:?}"
    );
    assert_eq!(err.status_code(), 503, "{err}");
    assert_eq!(err.error_type(), fluree_vocab::errors::COMMIT_CONTENTION);
}

#[tokio::test(flavor = "current_thread")]
async fn a_staged_write_that_loses_every_race_is_commit_contention() {
    let (spans, _guard) = crate::support::span_capture::init_test_tracing();
    let fluree = contended().await;
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    let before = spans.find_spans("txn_attempts").len();

    let err = fluree
        .stage(&handle)
        .insert(&person("bob"))
        .execute()
        .await
        .expect_err("every commit attempt loses");

    assert_contention(&err);
    assert_eq!(handle.t().await, 1, "nothing was committed");
    let attempts = spans.find_spans("txn_attempts");
    assert_eq!(attempts.len(), before + 1, "one span for the retry loop");
    assert_eq!(
        attempts[before].fields.get("attempts").map(String::as_str),
        Some("16")
    );
}

#[tokio::test]
async fn a_transaction_that_loses_every_race_is_commit_contention() {
    let fluree = contended().await;
    let mut txn = fluree
        .begin_transaction(LEDGER, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(TxnOperation::Insert(person("bob")))
        .await
        .expect("stage");

    let err = txn
        .commit(CommitOpts::default())
        .await
        .expect_err("every commit attempt loses");

    assert_contention(&err);
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    assert_eq!(handle.t().await, 1, "nothing was committed");
}

/// The tracked write path answers a lost commit race with the status the
/// error carries, not a 500 of its own.
#[tokio::test]
async fn a_tracked_write_that_loses_its_race_keeps_the_conflict_status() {
    let fluree = contended().await;
    let mut txn = json!({
        "ledger": LEDGER,
        "@context": { "ex": "http://example.org/" },
        "insert": person("bob")
    });
    txn["insert"]
        .as_object_mut()
        .expect("object")
        .remove("@context");

    let err = fluree
        .update_with_ledger_tracked(&txn)
        .await
        .expect_err("the commit loses its race");

    assert_eq!(err.status_code(), 409, "{err}");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    assert_eq!(handle.t().await, 1, "nothing was committed");
}
