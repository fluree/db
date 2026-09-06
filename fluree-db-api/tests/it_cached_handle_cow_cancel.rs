//! A cancelled commit must never expose the empty cache slot to readers.
//!
//! The cached-handle commit path empties the cache slot for the duration of a
//! commit, leaving a genesis placeholder under the ledger's write lock (see
//! `it_cached_handle_cow`). The lock is what makes the placeholder invisible —
//! so the one interleaving that can expose it is a cancellation, which drops
//! the commit future and releases the lock without the repair having landed.
//!
//! That is a reachable path, not a theoretical one: `LocalCommitter::transact`
//! awaits the commit inline inside the HTTP request future, and axum drops
//! handler futures when the client disconnects. Readers parked behind the
//! commit — the normal condition under load — would acquire the instant the
//! lock released and read an empty ledger at `t = 0`.
//!
//! The commit therefore runs on its own task, so a cancelled caller abandons
//! the *wait* rather than the commit. This test pins that: it parks a reader
//! behind an in-flight commit, cancels the caller, and asserts the reader never
//! sees `t = 0` and that the commit still lands.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerHandle};
use serde_json::json;
use std::time::{Duration, Instant};

fn person(id: &str) -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:{id}"),
        "ex:name": id
    })
}

/// Block until `handle` is write-locked, i.e. a commit is in flight.
///
/// Observing the lock from another task is a precise signal that the commit is
/// inside the detached window: the commit path takes the lock and reaches the
/// point where the cache slot is emptied without an intervening await, so the
/// first moment another task can run is after the slot is already empty.
async fn await_commit_in_flight(handle: &LedgerHandle) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !handle.is_locked() {
        assert!(
            Instant::now() < deadline,
            "the commit never took the ledger write lock"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn wait_for_t(handle: &LedgerHandle, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let t = handle.t().await;
        if t == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cached handle settled at t = {t}, expected {want}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_commit_never_exposes_the_empty_cache_slot() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");

    let ledger_id = "it/cow-cancel:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache the ledger");

    let alice = person("alice");
    fluree
        .stage(&handle)
        .insert(&alice)
        .execute()
        .await
        .expect("first commit");
    assert_eq!(handle.t().await, 1);

    // A second commit, on its own task so the caller can be cancelled the way
    // a client disconnect cancels an axum handler future.
    let committer = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            let bob = person("bob");
            fluree.stage(&handle).insert(&bob).execute().await
        })
    };
    await_commit_in_flight(&handle).await;

    // Park a reader behind the in-flight commit. It is queued before anything
    // the cancellation could schedule, so it is the first thing to observe the
    // cache slot once the write lock is released.
    let reader = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.t().await })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;

    committer.abort();

    let observed = reader.await.expect("reader task");
    assert_ne!(
        observed, 0,
        "a reader parked behind a cancelled commit observed the empty cache slot at t = 0; \
         the commit window is not shielded from cancellation"
    );

    // Cancelling the caller abandons the wait, not the commit: the shielded
    // task runs to completion and installs its state.
    wait_for_t(&handle, 2).await;

    let state = handle.snapshot().await.to_ledger_state();
    for id in ["ex:alice", "ex:bob"] {
        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "select": { id: ["*"] }
        });
        let rows = support::query_jsonld(&fluree, &state, &query)
            .await
            .expect("query after a cancelled commit");
        assert!(
            !rows.is_empty(),
            "{id} must be readable through the handle after a cancelled commit"
        );
    }

    // And the handle still writes.
    let carol = person("carol");
    let after = fluree
        .stage(&handle)
        .insert(&carol)
        .execute()
        .await
        .expect("commit after a cancelled one");
    assert_eq!(after.receipt.t, 3);
}
