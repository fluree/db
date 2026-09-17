//! Drop ledger integration tests
//!
//! Tests the `drop_ledger` API behavior.
//!
//! Note: `drop_graph_source` exists in Rust (`fluree-db-api/src/admin.rs`) but does not
//! yet have integration-test coverage here.

#![cfg(feature = "native")]

use crate::support::start_background_indexer_local;
use fluree_db_api::{DropMode, DropStatus, FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::address_path::ledger_id_to_path_prefix;
use fluree_db_core::LedgerSnapshot;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use tokio::time::{timeout, Duration};

/// Test that soft drop only retracts from nameservice and leaves files intact.
#[tokio::test]
async fn drop_ledger_soft_mode_retracts_only() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-soft-test:main";
    let ledger_name = "drop-soft-test";
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
        .drop_ledger(ledger_name, DropMode::Soft)
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
    let ledger_name = "drop-hard-test";
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
        .drop_ledger(ledger_name, DropMode::Hard)
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
        .drop_ledger("nonexistent", DropMode::Soft)
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
    let ledger_name = "drop-idem-test";
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
        .drop_ledger(ledger_name, DropMode::Soft)
        .await
        .expect("drop1");
    assert_eq!(r1.status, DropStatus::Dropped);

    // Second drop - should be idempotent
    let r2 = fluree
        .drop_ledger(ledger_name, DropMode::Soft)
        .await
        .expect("drop2");
    assert_eq!(r2.status, DropStatus::AlreadyRetracted);
}

/// Test that drop normalizes alias (adds :main if missing).
#[tokio::test]
async fn drop_ledger_accepts_bare_name_and_default_suffix() {
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

    // Bare name is the canonical form; report.ledger_id is the ledger name.
    let report = fluree
        .drop_ledger("normalize-test", DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);
    assert_eq!(report.ledger_id, "normalize-test");
    assert!(
        report.warnings.is_empty(),
        "bare name should not warn: {:?}",
        report.warnings
    );
    assert_eq!(report.branch_reports.len(), 1);
    assert_eq!(report.branch_reports[0].ledger_id, "normalize-test:main");
}

/// `drop_ledger("name:main")` is rejected even though `:main` is the default
/// branch — callers must pass the bare ledger name. The error names
/// `drop_branch` so callers wanting a single-branch drop know where to go.
#[tokio::test]
async fn drop_ledger_rejects_main_suffix() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "suffix-test:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger = LedgerState::new(db, Novelty::new(0));
    let tx = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:x", "ex:n": 1});
    fluree.insert(ledger, &tx).await.expect("insert");

    let err = fluree
        .drop_ledger("suffix-test:main", DropMode::Soft)
        .await
        .expect_err("drop_ledger with :main suffix should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("branch suffix 'main'") && msg.contains("drop_branch"),
        "expected suffix-rejection error mentioning drop_branch, got: {msg}"
    );
}

/// `drop_ledger("name:non-default")` is rejected — callers must use
/// `drop_branch` for branch-scoped drops.
#[tokio::test]
async fn drop_ledger_rejects_non_default_branch_suffix() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let err = fluree
        .drop_ledger("mydb:dev", DropMode::Soft)
        .await
        .expect_err("non-default branch suffix should be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("drop_branch"), "msg={msg}");
}

/// Hard-drop of a multi-branch ledger: every branch is purged (including
/// retracted ones), per-branch reports are returned in leaf-first order,
/// and the cross-branch `@shared/dicts/` namespace is wiped at the end.
#[tokio::test]
async fn drop_ledger_hard_clears_every_branch_and_shared() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");

    // root (main) + two children
    let main = fluree.create_ledger("multi-drop").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(main, &txn).await.unwrap();

    fluree
        .create_branch("multi-drop", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("multi-drop", "feature-x", Some("dev"), None)
        .await
        .unwrap();

    // Drop `dev` while `feature-x` still references it. Because `dev` has a
    // live child, `drop_branch` retracts it (deferred=true) instead of
    // purging — that's the "retracted-but-not-purged" state we want to
    // exercise in the whole-ledger drop below.
    let dev_drop = fluree.drop_branch("multi-drop", "dev").await.unwrap();
    assert!(
        dev_drop.deferred,
        "dev should retract-as-deferred while feature-x lives"
    );
    let dev_record = fluree
        .nameservice()
        .lookup("multi-drop:dev")
        .await
        .unwrap()
        .expect("dev record still present");
    assert!(
        dev_record.retracted,
        "dev should be retracted before whole-ledger drop"
    );

    // Pre-condition: branches exist on disk
    let admin = fluree.admin_storage().expect("managed backend");
    let pre = admin
        .list_prefix("fluree:file://multi-drop/")
        .await
        .expect("list pre");
    assert!(
        !pre.is_empty(),
        "expected branch artifacts before drop, got: {pre:?}"
    );

    // Whole-ledger drop.
    let report = fluree
        .drop_ledger("multi-drop", DropMode::Hard)
        .await
        .expect("drop_ledger");
    assert_eq!(report.status, DropStatus::Dropped);
    assert_eq!(report.ledger_id, "multi-drop");
    assert!(
        report.branch_reports.len() >= 2,
        "expected per-branch reports, got: {:?}",
        report.branch_reports
    );

    // Leaf-first order: feature-x (which sourced from dev) must come before
    // dev, and dev before main, in the report.
    let order: Vec<&str> = report
        .branch_reports
        .iter()
        .map(|r| r.ledger_id.as_str())
        .collect();
    let pos = |id: &str| order.iter().position(|s| *s == id);
    if let (Some(fx), Some(dev), Some(m)) = (
        pos("multi-drop:feature-x"),
        pos("multi-drop:dev"),
        pos("multi-drop:main"),
    ) {
        assert!(fx < dev, "feature-x before dev, got order: {order:?}");
        assert!(dev < m, "dev before main, got order: {order:?}");
    }

    // Nameservice empty for this ledger name.
    assert!(fluree
        .nameservice()
        .list_branches("multi-drop")
        .await
        .unwrap()
        .is_empty());

    // Storage cleared of all branches AND @shared/dicts/.
    let post = admin
        .list_prefix("fluree:file://multi-drop/")
        .await
        .expect("list post");
    assert!(
        post.is_empty(),
        "expected no artifacts under multi-drop/ after hard drop, got: {post:?}"
    );

    // Alias is reusable.
    let _new = fluree.create_ledger("multi-drop").await.expect("recreate");
}

/// Dropping a fork releases the dictionary blobs only the fork's index chain
/// referenced and keeps every blob the surviving branch still reaches.
/// Dictionaries are ledger-wide, so the branch prefix delete alone would have
/// left the fork's own blobs behind for a sweep.
#[tokio::test]
async fn drop_branch_releases_dictionary_blobs_only_the_branch_referenced() {
    use crate::support::build_and_publish_index;
    use fluree_db_core::ContentStore;
    use fluree_db_indexer::{shared_refs_of_branches, BranchIndexHead};

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let main = fluree.create_ledger("fork-dicts").await.unwrap();
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(main, &seed).await.unwrap();
    build_and_publish_index(&fluree, "fork-dicts:main").await;

    fluree
        .create_branch("fork-dicts", "dev", None, None)
        .await
        .unwrap();

    // New subjects on the fork, then an index build: the fork's reverse
    // dictionary leaves are rewritten, so its chain references blobs main's
    // does not.
    let dev = fluree.ledger("fork-dicts:dev").await.unwrap();
    let subjects: Vec<_> = (0..50)
        .map(|i| json!({"@id": format!("ex:dev-{i}"), "ex:val": i}))
        .collect();
    let more = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": subjects
    });
    fluree.insert(dev, &more).await.unwrap();
    build_and_publish_index(&fluree, "fork-dicts:dev").await;

    async fn dict_refs(
        fluree: &fluree_db_api::Fluree,
        ledger_id: &str,
    ) -> std::collections::HashSet<fluree_db_core::ContentId> {
        let head = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .unwrap()
            .expect("record")
            .index_head_id;
        shared_refs_of_branches(
            fluree.backend(),
            &[BranchIndexHead {
                ledger_id: ledger_id.to_string(),
                index_head_id: head,
            }],
            None,
        )
        .await
        .unwrap()
    }
    let main_refs = dict_refs(&fluree, "fork-dicts:main").await;
    let dev_refs = dict_refs(&fluree, "fork-dicts:dev").await;
    let dev_only: Vec<_> = dev_refs.difference(&main_refs).cloned().collect();
    assert!(
        !dev_only.is_empty(),
        "the fork must reference dictionary blobs of its own for this test to mean anything"
    );
    assert!(
        !main_refs.is_empty(),
        "the surviving branch must reference dictionary blobs"
    );

    let report = fluree.drop_branch("fork-dicts", "dev").await.unwrap();
    assert_eq!(report.status, DropStatus::Dropped);
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);

    let store = fluree.content_store("fork-dicts:main");
    for cid in &main_refs {
        assert!(
            store.has(cid).await.unwrap(),
            "surviving branch's dictionary blob {cid} must remain"
        );
    }
    for cid in &dev_only {
        assert!(
            !store.has(cid).await.unwrap(),
            "fork-only dictionary blob {cid} must be released with the fork"
        );
    }
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "drop-cancel-test:main";
            let ledger_name = "drop-cancel-test";
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
                fluree.drop_ledger(ledger_name, DropMode::Hard),
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
    let ledger_name = "drop-hard-retracted";
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
        .drop_ledger(ledger_name, DropMode::Soft)
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
        .drop_ledger(ledger_name, DropMode::Hard)
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
    let ledger_name = "drop-cache-test";

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
        .drop_ledger(ledger_name, DropMode::Soft)
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

/// The collector and a fork drop, end to end on file storage and the file
/// nameservice with the worker running: real builds, passes releasing behind
/// them inside release windows, and nothing a surviving chain references lost.
#[tokio::test]
async fn collector_and_fork_drop_keep_every_referenced_dictionary_on_file_storage() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let mut fluree = FlureeBuilder::file(&path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        crate::support::collecting_indexer_config(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            crate::support::run_collector_and_fork_drop_scenario(&fluree, &handle, "gc-e2e").await;
        })
        .await;
}
