//! Integration tests for `Fluree::refresh()` with `min_t` support.
//!
//! Verifies:
//! - Fast path: `min_t <= cached_t` returns immediately without hitting NS
//! - `AwaitTNotReached` error when `min_t` exceeds the ledger's `t`
//! - `LedgerManager::current_t()` returns correct values

#![cfg(feature = "native")]

mod support;

use fluree_db_api::ledger_manager::{NotifyResult, RefreshOpts, RefreshResult};
use fluree_db_api::{ApiError, FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

/// Helper: transact one insert and return the committed ledger state.
async fn insert_data(
    fluree: &support::MemoryFluree,
    ledger: fluree_db_api::LedgerState,
    label: &str,
) -> fluree_db_api::LedgerState {
    let txn = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{
            "@id": format!("ex:{label}"),
            "ex:name": label
        }]
    });
    fluree
        .insert_with_opts(
            ledger,
            &txn,
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert should succeed")
        .ledger
}

#[tokio::test]
async fn refresh_min_t_satisfied_returns_immediately() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/refresh-fast:main";

    // Create ledger and insert data so t >= 1
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let t = ledger1.t();
    assert!(t >= 1);

    // Load ledger into cache
    let _handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger_cached");

    // Refresh with min_t already satisfied — should return Current without NS lookup
    let result = fluree
        .refresh(ledger_id, RefreshOpts { min_t: Some(t) })
        .await
        .expect("refresh should succeed");

    assert_eq!(
        result,
        Some(RefreshResult {
            t,
            action: NotifyResult::Current,
        }),
        "should return Current when min_t is already satisfied by cache"
    );
}

#[tokio::test]
async fn refresh_min_t_below_cached_t_returns_immediately() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/refresh-below:main";

    // Create ledger and insert two items
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let ledger2 = insert_data(&fluree, ledger1, "item2").await;
    let t = ledger2.t();
    assert!(t >= 2);

    // Load into cache
    let _handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger_cached");

    // Refresh with min_t lower than cached t
    let result = fluree
        .refresh(ledger_id, RefreshOpts { min_t: Some(1) })
        .await
        .expect("refresh should succeed");

    assert_eq!(
        result,
        Some(RefreshResult {
            t,
            action: NotifyResult::Current,
        }),
        "should return Current when min_t < cached_t"
    );
}

#[tokio::test]
async fn refresh_min_t_not_reached_returns_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/refresh-not-reached:main";

    // Create ledger with one commit
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let t = ledger1.t();

    // Load into cache
    let _handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger_cached");

    // Request min_t far beyond what exists
    let unreachable_t = t + 100;
    let err = fluree
        .refresh(
            ledger_id,
            RefreshOpts {
                min_t: Some(unreachable_t),
            },
        )
        .await
        .expect_err("should fail when min_t is unreachable");

    match err {
        ApiError::AwaitTNotReached { requested, current } => {
            assert_eq!(requested, unreachable_t);
            assert_eq!(current, t);
        }
        other => panic!("expected AwaitTNotReached, got: {other:?}"),
    }
}

#[tokio::test]
async fn current_t_returns_none_when_not_cached() {
    let fluree = FlureeBuilder::memory().build_memory();
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    let t = manager.current_t("nonexistent:main").await;
    assert_eq!(t, None, "should return None for uncached ledger");
}

#[tokio::test]
async fn current_t_returns_correct_value_after_load() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/current-t:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // Create and commit data
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let expected_t = ledger1.t();

    // Load into cache
    let _handle = manager.get_or_load(ledger_id).await.expect("load");

    // current_t should reflect the cached state
    let t = manager.current_t(ledger_id).await;
    assert_eq!(
        t,
        Some(expected_t),
        "current_t should match the loaded ledger's t"
    );
}
