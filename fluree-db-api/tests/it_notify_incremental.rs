//! Integration tests for incremental LedgerManager::notify() paths.
//!
//! Verifies that notify() uses incremental updates (IndexOnly, CommitCatchUp)
//! instead of full reloads when the gap is small enough.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{
    ledger_manager::{LedgerManagerConfig, NotifyResult, NsNotify},
    FlureeBuilder, IndexConfig,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{genesis_ledger_for_fluree, start_background_indexer_local, trigger_index_and_wait};

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
                // Very large thresholds to prevent auto-indexing
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert should succeed")
        .ledger
}

#[tokio::test]
async fn notify_single_commit_uses_incremental_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/notify-incremental:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // Create ledger and insert initial data
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let t_after_first = ledger1.t();
    assert!(t_after_first >= 1);

    // Cache the ledger in the manager (simulates a prior request)
    let _handle = manager.get_or_load(ledger_id).await.expect("load");

    // Verify it's cached and current
    let result = manager
        .notify(NsNotify {
            ledger_id: ledger_id.to_string(),
            record: None,
        })
        .await
        .expect("notify");
    assert_eq!(
        result,
        NotifyResult::Current,
        "should be current after load"
    );

    // Transact one more commit (advances commit_t by 1)
    let _ledger2 = insert_data(&fluree, ledger1, "item2").await;

    // Notify — should use CommitCatchUp (gap=1), NOT reload
    let result = manager
        .notify(NsNotify {
            ledger_id: ledger_id.to_string(),
            record: None,
        })
        .await
        .expect("notify after second insert");

    assert!(
        matches!(result, NotifyResult::CommitsApplied { count: 1 }),
        "expected CommitsApplied {{ count: 1 }}, got: {result:?}"
    );

    // Verify the cached state now reflects the new commit
    let handle = manager.get_or_load(ledger_id).await.expect("re-load");
    let state = handle.snapshot().await;
    assert_eq!(
        state.t,
        t_after_first + 1,
        "cached ledger should be at t={} after incremental apply",
        t_after_first + 1
    );
}

#[tokio::test]
async fn notify_small_gap_uses_incremental_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/notify-gap:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // Create and commit initial data
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = insert_data(&fluree, ledger0, "item1").await;
    let t_initial = ledger1.t();

    // Cache the ledger
    let _handle = manager.get_or_load(ledger_id).await.expect("load");

    // Transact 3 more commits
    let ledger2 = insert_data(&fluree, ledger1, "item2").await;
    let ledger3 = insert_data(&fluree, ledger2, "item3").await;
    let _ledger4 = insert_data(&fluree, ledger3, "item4").await;

    // Notify — should catch up 3 commits incrementally
    let result = manager
        .notify(NsNotify {
            ledger_id: ledger_id.to_string(),
            record: None,
        })
        .await
        .expect("notify after 3 inserts");

    assert!(
        matches!(result, NotifyResult::CommitsApplied { count: 3 }),
        "expected CommitsApplied {{ count: 3 }}, got: {result:?}"
    );

    // Verify final t
    let handle = manager.get_or_load(ledger_id).await.expect("re-load");
    let state = handle.snapshot().await;
    assert_eq!(state.t, t_initial + 3);
}

#[tokio::test]
async fn notify_large_gap_falls_back_to_reload() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/notify-reload:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // Create and commit initial data
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let mut ledger = insert_data(&fluree, ledger0, "item0").await;

    // Cache the ledger
    let _handle = manager.get_or_load(ledger_id).await.expect("load");

    // Transact 6 more commits (exceeds MAX_INCREMENTAL_COMMITS = 5)
    for i in 1..=6 {
        ledger = insert_data(&fluree, ledger, &format!("item{i}")).await;
    }

    // Notify — gap is 6, should fall back to full reload
    let result = manager
        .notify(NsNotify {
            ledger_id: ledger_id.to_string(),
            record: None,
        })
        .await
        .expect("notify after 6 inserts");

    assert_eq!(
        result,
        NotifyResult::Reloaded,
        "expected Reloaded for gap > 5"
    );
}

#[tokio::test]
async fn notify_index_only_trims_novelty() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/notify-index-only:main";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // Start a background indexer
    let (local, indexer_handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async {
            // Create and commit data
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let mut graph = Vec::new();
            for i in 0..50u32 {
                graph.push(json!({
                    "@id": format!("ex:s{i}"),
                    "ex:name": format!("name-{i}"),
                    "ex:n": i
                }));
            }
            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": graph
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &IndexConfig {
                        reindex_min_bytes: 1_000_000_000,
                        reindex_max_bytes: 1_000_000_000,
                    },
                )
                .await
                .expect("insert");
            let commit_t = ledger1.ledger.t();

            // Cache the ledger (has novelty, no index yet)
            let handle = manager.get_or_load(ledger_id).await.expect("load");
            let state_before = handle.snapshot().await;
            // snapshot.t is the index_t (from the LedgerSnapshot)
            assert_eq!(
                state_before.snapshot.t, 0,
                "index_t should be 0 before indexing"
            );
            // Overall t includes novelty
            assert_eq!(state_before.t, commit_t, "overall t should reflect commits");

            // Trigger indexing — publishes new index root to nameservice
            trigger_index_and_wait(&indexer_handle, ledger_id, commit_t).await;

            // Notify — commit_t unchanged, index advanced → IndexOnly plan
            let result = manager
                .notify(NsNotify {
                    ledger_id: ledger_id.to_string(),
                    record: None,
                })
                .await
                .expect("notify after indexing");

            assert_eq!(
                result,
                NotifyResult::IndexUpdated,
                "expected IndexUpdated, got: {result:?}"
            );

            // Verify the cached state now has the index
            let handle = manager.get_or_load(ledger_id).await.expect("re-load");
            let state_after = handle.snapshot().await;
            assert_eq!(
                state_after.snapshot.t, commit_t,
                "index_t should match commit_t after IndexOnly update"
            );
            // Overall t should be unchanged
            assert_eq!(state_after.t, commit_t);
        })
        .await;
}

/// Catch-up across a branch fork point must not 404 on the parent commit.
///
/// Regression test for the bug where the commit-chain walker scoped its
/// content store to the branch's own namespace, causing reads of pre-fork
/// commits (which live under the source branch's prefix) to fail with
/// "Not found".
#[tokio::test]
async fn notify_branch_catch_up_resolves_pre_fork_parent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_name = "it/notify-branch";
    let main_id = "it/notify-branch:main";
    let dev_id = "it/notify-branch:dev";
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    // 1. Create main and seed one commit so commit_head_id is set
    //    (create_branch uses the source branch's commit head).
    fluree
        .create_ledger(ledger_name)
        .await
        .expect("create_ledger");
    let main_ledger = fluree.ledger(main_id).await.expect("open main");
    let _ = insert_data(&fluree, main_ledger, "seed").await;

    // 2. Create branch dev from main at t=1
    fluree
        .create_branch(ledger_name, "dev", None)
        .await
        .expect("create_branch");

    // 3. Cache the dev branch — its initial commit_t equals main's t at
    //    fork time. Critical: this is the local_t the catch-up walker
    //    will compare against when dev advances.
    let dev_ledger = fluree.ledger(dev_id).await.expect("open dev");
    let local_t_before = dev_ledger.t();
    let _handle = manager.get_or_load(dev_id).await.expect("cache dev");

    // 4. Transact on dev — this commit's `previous` points at the t=1
    //    commit that lives under main's namespace, not dev's.
    let _dev_after = insert_data(&fluree, dev_ledger, "dev-only").await;

    // 5. Notify on dev — small gap, takes the CommitCatchUp path.
    //    Without the branched-store fix, this fails with "Not found"
    //    when the walker tries to read the parent envelope from dev's
    //    own prefix. With the fix, BranchedContentStore falls through
    //    to main's namespace and resolves it.
    let result = manager
        .notify(NsNotify {
            ledger_id: dev_id.to_string(),
            record: None,
        })
        .await
        .expect("notify on dev branch");

    assert!(
        matches!(result, NotifyResult::CommitsApplied { count: 1 }),
        "expected CommitsApplied {{ count: 1 }} via branch-aware catch-up, got: {result:?}"
    );

    let handle = manager.get_or_load(dev_id).await.expect("re-load dev");
    let state = handle.snapshot().await;
    assert_eq!(
        state.t,
        local_t_before + 1,
        "cached dev branch should be at t={} after incremental apply",
        local_t_before + 1
    );
}

#[tokio::test]
async fn notify_returns_not_loaded_for_uncached_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let manager = fluree
        .ledger_manager()
        .expect("ledger_manager should be present");

    let result = manager
        .notify(NsNotify {
            ledger_id: "nonexistent:main".to_string(),
            record: None,
        })
        .await
        .expect("notify");

    assert_eq!(result, NotifyResult::NotLoaded);
}
