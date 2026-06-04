//! Regression: a cached writer whose in-memory `LedgerState` has fallen behind
//! the durable nameservice head must RECONCILE and commit, not wedge.
//!
//! Background (see memory `concurrent_update_wedge_and_ns_race`): a transaction
//! commits by CAS-publishing the new head to the nameservice and then updating
//! the cached in-memory state. If the cache ever lags the durable head (e.g. a
//! prior commit published but a post-publish bookkeeping step failed, or — as
//! modelled here — a *second* connection advanced the shared head), then
//! `verify_sequencing` returns `CommitConflict { expected_t, head_t }` with
//! `expected_t > head_t`. Before the fix this repeated forever (permanent
//! wedge). After the fix the writer `refresh()`-es the cache to the head and
//! retries, committing successfully.
//!
//! We model the lag with two independent `Fluree` connections over the SAME
//! on-disk storage + nameservice: connection B caches the ledger, connection A
//! advances the shared head, then B writes through its now-stale cached handle.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_concurrent_update_reconcile --features native

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

#[tokio::test]
async fn stale_cached_writer_reconciles_instead_of_wedging() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let ledger_id = "it/cu-reconcile:main";

    // --- Connection A: owns the durable head (no cache reliance) ---
    let fa = FlureeBuilder::file(path.clone()).build().expect("build A");

    // Genesis + first commit (t=1) via the owned path so it publishes to the
    // shared on-disk nameservice.
    let l0 = fa.create_ledger(ledger_id).await.expect("create");
    let tx_a1 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:a1", "ex:name": "a1" }]
    });
    let ra1 = fa.insert(l0, &tx_a1).await.expect("A insert t=1");
    assert_eq!(ra1.receipt.t, 1, "first commit is t=1");
    let la1 = ra1.ledger;

    // --- Connection B: cache the ledger at t=1 (independent manager) ---
    let fb = FlureeBuilder::file(path.clone()).build().expect("build B");
    let hb = fb.ledger_cached(ledger_id).await.expect("B cache");
    assert_eq!(hb.t().await, 1, "B cached at t=1");

    // --- Connection A advances the durable head to t=2 ---
    // B's cache is now STALE (still t=1) while the nameservice head is t=2.
    let tx_a2 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:a2", "ex:name": "a2" }]
    });
    let ra2 = fa.insert(la1, &tx_a2).await.expect("A insert t=2");
    assert_eq!(ra2.receipt.t, 2, "A advanced durable head to t=2");
    assert_eq!(hb.t().await, 1, "B's cache is still behind at t=1");

    // --- Connection B writes through its STALE cached handle ---
    // The optimistic commit path hits `CommitConflict` (cache t=1 vs head t=2),
    // reconciles via refresh (CommitCatchUp -> t=2), and retries -> commits t=3.
    let tx_b = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:b1", "ex:name": "b1" }]
    });
    let rb = fb
        .stage(&hb)
        .insert(&tx_b)
        .execute()
        .await
        .expect("stale-cache writer must reconcile and commit, not wedge");
    assert_eq!(
        rb.receipt.t, 3,
        "reconciled commit lands at t=3 (after catching up to the t=2 head)"
    );

    // The reconcile must have advanced B's cached state to the new head.
    assert_eq!(hb.t().await, 3, "B's cache reflects the committed head t=3");

    // A subsequent write through the same handle commits cleanly (no wedge).
    let tx_b2 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:b2", "ex:name": "b2" }]
    });
    let rb2 = fb
        .stage(&hb)
        .insert(&tx_b2)
        .execute()
        .await
        .expect("subsequent write after reconcile must succeed");
    assert_eq!(rb2.receipt.t, 4, "next write proceeds normally at t=4");
}
