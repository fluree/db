//! Drop ledger integration tests
//!
//! Tests the `drop_ledger` API behavior.
//!
//! Note: `drop_graph_source` exists in Rust (`fluree-db-api/src/admin.rs`) but does not
//! yet have integration-test coverage here.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{DropMode, DropStatus, FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::address_path::ledger_id_to_path_prefix;
use fluree_db_core::LedgerSnapshot;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::start_background_indexer_local;
use tokio::time::{timeout, Duration};

/// Test that soft drop only retracts from nameservice and leaves files intact.
#[tokio::test]
async fn drop_ledger_soft_mode_retracts_only() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-soft-test:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });

    let result = fluree.insert(ledger, &tx).await.expect("insert");
    assert_eq!(result.receipt.t, 1);

    // Soft drop - should only retract, not delete files
    let report = fluree
        .drop_ledger(ledger_id, DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);
    assert_eq!(
        report.artifacts_deleted, 0,
        "Soft mode should not delete artifacts"
    );

    // Verify retracted in nameservice
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup");
    assert!(record.is_some(), "Record should still exist");
    assert!(record.unwrap().retracted, "Record should be retracted");

    // Files should still exist (commit prefix uses canonical storage path, no ':')
    let commit_prefix = format!(
        "fluree:file://{}/commit/",
        ledger_id_to_path_prefix(ledger_id).unwrap()
    );
    let files = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(!files.is_empty(), "Commit files should remain in soft mode");
}

/// Test that hard drop deletes all files and retracts from nameservice.
#[tokio::test]
async fn drop_ledger_hard_mode_deletes_files() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-hard-test:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });

    let result = fluree.insert(ledger, &tx).await.expect("insert");
    assert_eq!(result.receipt.t, 1);

    // Verify files exist before drop
    let commit_prefix = format!(
        "fluree:file://{}/commit/",
        ledger_id_to_path_prefix(ledger_id).unwrap()
    );
    let files_before = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(
        !files_before.is_empty(),
        "Should have commit files before drop"
    );

    // Hard drop - should delete files and retract
    let report = fluree
        .drop_ledger(ledger_id, DropMode::Hard)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);
    assert!(
        report.artifacts_deleted > 0,
        "Should have deleted artifacts"
    );

    // Verify nameservice purged (hard drop removes the record entirely,
    // allowing the alias to be reused — unlike soft drop which only retracts).
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup");
    assert!(
        record.is_none(),
        "Hard drop purges the record so it no longer exists"
    );

    // Verify commit files deleted
    let files_after = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(
        files_after.is_empty(),
        "Commit files should be deleted in hard mode"
    );
}

/// Test drop returns NotFound for non-existent ledger.
#[tokio::test]
async fn drop_ledger_not_found() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let report = fluree
        .drop_ledger("nonexistent:main", DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::NotFound);
}

/// Test drop is idempotent - second drop returns AlreadyRetracted.
#[tokio::test]
async fn drop_ledger_idempotent() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-idem-test:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });
    fluree.insert(ledger, &tx).await.expect("insert");

    // First drop
    let r1 = fluree
        .drop_ledger(ledger_id, DropMode::Soft)
        .await
        .expect("drop1");
    assert_eq!(r1.status, DropStatus::Dropped);

    // Second drop - should be idempotent
    let r2 = fluree
        .drop_ledger(ledger_id, DropMode::Soft)
        .await
        .expect("drop2");
    assert_eq!(r2.status, DropStatus::AlreadyRetracted);
}

/// Test that drop normalizes alias (adds :main if missing).
#[tokio::test]
async fn drop_ledger_normalizes_alias() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    // Create ledger with full alias
    let ledger_id = "normalize-test:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });
    fluree.insert(ledger, &tx).await.expect("insert");

    // Drop with short alias (should normalize to :main)
    let report = fluree
        .drop_ledger("normalize-test", DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);
    assert_eq!(report.ledger_id, "normalize-test:main");
}

/// Test that drop cancels pending indexing before deletion (the flake fix).
///
/// This test exercises the "drop while indexing is pending/in progress" scenario
/// to ensure cancel + wait_for_idle prevents race conditions.
#[tokio::test]
async fn drop_ledger_cancels_pending_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(&path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "drop-cancel-test:main";
            let db = LedgerSnapshot::genesis(ledger_id);
            let ledger = LedgerState::new(db, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000_000,
            };

            // Make commits to create indexing work
            let mut current = ledger;
            for i in 0..3 {
                let tx = json!({
                    "@context": {"ex": "http://example.org/"},
                    "@id": format!("ex:item{}", i),
                    "ex:value": i
                });
                let result = fluree
                    .insert_with_opts(
                        current,
                        &tx,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert");
                current = result.ledger;
            }

            // Trigger indexing but DON'T wait - immediately drop
            // This exercises the "drop while indexing is pending/in progress" scenario
            let _completion = handle.trigger(ledger_id, 3).await;

            // Immediately call drop_ledger - should cancel + wait_for_idle internally
            // This is the key test: drop should handle the race gracefully
            let report = timeout(
                Duration::from_secs(30),
                fluree.drop_ledger(ledger_id, DropMode::Hard),
            )
            .await
            .expect("drop timed out")
            .expect("drop failed");

            assert_eq!(report.status, DropStatus::Dropped);

            // Verify both commit and index files are deleted
            // Commits use raw alias: fluree:file://drop-cancel-test:main/commit/
            // Indexes use normalized: fluree:file://drop-cancel-test/main/index/
            let prefix = ledger_id_to_path_prefix(ledger_id).unwrap();
            let commit_prefix = format!("fluree:file://{prefix}/commit/");
            let index_prefix = format!("fluree:file://{prefix}/index/");

            let commit_files = fluree
                .admin_storage()
                .expect("managed backend")
                .list_prefix(&commit_prefix)
                .await
                .expect("list commit");
            let index_files = fluree
                .admin_storage()
                .expect("managed backend")
                .list_prefix(&index_prefix)
                .await
                .expect("list index");

            assert!(
                commit_files.is_empty(),
                "Commit files should be deleted after hard drop"
            );
            assert!(
                index_files.is_empty(),
                "Index files should be deleted after hard drop"
            );
        })
        .await;
}

/// Test that hard drop still attempts deletion even when ledger is already retracted.
#[tokio::test]
async fn drop_ledger_hard_mode_deletes_even_when_retracted() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-hard-retracted:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });
    fluree.insert(ledger, &tx).await.expect("insert");

    // First soft drop (retract only)
    let r1 = fluree
        .drop_ledger(ledger_id, DropMode::Soft)
        .await
        .expect("soft drop");
    assert_eq!(r1.status, DropStatus::Dropped);

    // Verify files still exist
    let commit_prefix = format!(
        "fluree:file://{}/commit/",
        ledger_id_to_path_prefix(ledger_id).unwrap()
    );
    let files_before = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(
        !files_before.is_empty(),
        "Files should exist after soft drop"
    );

    // Second hard drop (should still delete files)
    let r2 = fluree
        .drop_ledger(ledger_id, DropMode::Hard)
        .await
        .expect("hard drop");
    assert_eq!(r2.status, DropStatus::AlreadyRetracted);
    assert!(
        r2.artifacts_deleted > 0,
        "Hard drop should delete artifacts even when already retracted"
    );

    // Verify files deleted
    let files_after = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(
        files_after.is_empty(),
        "Files should be deleted after hard drop"
    );
}

/// Test that drop_ledger disconnects the ledger from cache.
///
/// This ensures dropped ledgers don't remain in the LedgerManager cache,
/// which could serve stale data for queries against a deleted ledger.
#[tokio::test]
async fn drop_ledger_disconnects_from_cache() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-cache-test:main";

    // Create a ledger (publishes to nameservice)
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    assert_eq!(ledger.t(), 0);

    // Cache the ledger by loading it through the manager
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache load");
    let snapshot = handle.snapshot().await;
    assert_eq!(snapshot.t, 0);

    // Verify it's in the cache
    let mgr = fluree.ledger_manager().expect("caching enabled");
    let cached_before = mgr.cached_aliases().await;
    assert!(
        cached_before.contains(&ledger_id.to_string()),
        "Ledger should be cached before drop"
    );

    // Drop the ledger (should disconnect from cache)
    let report = fluree
        .drop_ledger(ledger_id, DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);

    // Verify ledger is NO LONGER in the cache
    let cached_after = mgr.cached_aliases().await;
    assert!(
        !cached_after.contains(&ledger_id.to_string()),
        "Ledger should be evicted from cache after drop"
    );
}
