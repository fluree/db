//! Drop ledger integration tests
//!
//! Tests the `drop_ledger` API behavior.
//!
//! Note: `drop_graph_source` exists in Rust (`fluree-db-api/src/admin.rs`) but does not
//! yet have integration-test coverage here.

#![cfg(feature = "native")]

use crate::support::start_background_indexer_local;
use fluree_db_api::{DropMode, DropStatus, FlureeBuilder, IndexConfig};
use fluree_db_core::{ContentKind, StorageNamespace};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use tokio::time::{timeout, Duration};

/// A soft drop frees the name and keeps the files, for a restore or purge.
#[tokio::test]
async fn drop_ledger_soft_mode_keeps_files() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-soft-test:main";
    let ledger_name = "drop-soft-test";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });

    let result = fluree.insert(ledger, &tx).await.expect("insert");
    assert_eq!(result.receipt.t, 1);
    let namespace = result.ledger.storage_namespace();

    // Soft drop - frees the name, deletes no files
    let report = fluree
        .drop_ledger(ledger_name, DropMode::Soft)
        .await
        .expect("drop");
    assert_eq!(report.status, DropStatus::Dropped);
    assert_eq!(
        report.artifacts_deleted, 0,
        "Soft mode should not delete artifacts"
    );

    // The name is free, and the dropped ledger waits in the registry.
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup");
    assert!(record.is_none(), "the name is free");
    assert_eq!(
        fluree.list_dropped().await.expect("list dropped")[0].instance,
        report.instance.expect("instance")
    );

    // Files should still exist
    let commit_prefix = format!("fluree:file://{}/commit/", namespace.branch_prefix());
    let files = fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix(&commit_prefix)
        .await
        .expect("list");
    assert!(!files.is_empty(), "Commit files should remain in soft mode");
}

/// The legacy edge-annotation arenas have no storage layout of their own and
/// sit under the branch's `blob/` prefix; a hard drop must take them too.
/// Builds no longer write them, so the test places one where an older build
/// left it, in a ledger rooted at its name as an older build's is: an
/// instance root is purged whole.
#[tokio::test]
async fn hard_drop_removes_legacy_annotation_arenas_under_blob() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let ledger_id = "drop-blob-test:main";
    fluree_db_nameservice::testing::create_at_name_root(fluree.publisher().unwrap(), ledger_id)
        .await
        .expect("a ledger from before name bindings");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("insert");

    let storage = fluree.admin_storage().expect("managed backend");
    let namespace = StorageNamespace::parse_legacy(ledger_id).unwrap();
    let blob_prefix = format!("fluree:file://{}/blob/", namespace.branch_prefix());
    let arena = storage
        .content_write_bytes(ContentKind::AnnotationForwardLeaf, &namespace, b"EAFL1")
        .await
        .expect("write arena leaf");
    assert!(
        arena.address.starts_with(&blob_prefix),
        "an annotation arena leaf belongs under blob/: {}",
        arena.address
    );

    fluree
        .drop_ledger("drop-blob-test", DropMode::Hard)
        .await
        .expect("drop");
    let left = storage.list_prefix(&blob_prefix).await.expect("list");
    assert!(
        left.is_empty(),
        "hard drop left files under blob/: {left:?}"
    );
}

/// Test that hard drop deletes all files and retracts from nameservice.
#[tokio::test]
async fn drop_ledger_hard_mode_deletes_files() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-hard-test:main";
    let ledger_name = "drop-hard-test";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

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
        result.ledger.storage_namespace().branch_prefix()
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

/// A second drop finds nothing: the first freed the name.
#[tokio::test]
async fn drop_ledger_idempotent() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-idem-test:main";
    let ledger_name = "drop-idem-test";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

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

    // Second drop - nothing holds the name now
    let r2 = fluree
        .drop_ledger(ledger_name, DropMode::Soft)
        .await
        .expect("drop2");
    assert_eq!(r2.status, DropStatus::NotFound);
}

/// Test that drop normalizes alias (adds :main if missing).
#[tokio::test]
async fn drop_ledger_accepts_bare_name_and_default_suffix() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    // Create ledger with full alias
    let ledger_id = "normalize-test:main";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

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
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
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
        .branch_records("multi-drop")
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
        let record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .unwrap()
            .expect("record");
        shared_refs_of_branches(fluree.backend(), &[BranchIndexHead::of(&record)])
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

    let store = fluree.content_store(&fluree.storage_namespace("fork-dicts:main").await.unwrap());
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
            let ledger = fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");

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

            let namespace = current.storage_namespace();

            // Trigger indexing but DON'T wait - immediately drop
            // This exercises the "drop while indexing is pending/in progress" scenario
            let _completion = handle
                .trigger(&fluree_db_api::LedgerId::parse(ledger_id).unwrap(), 3)
                .await;

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
            let prefix = namespace.branch_prefix();
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

/// A soft-dropped ledger's files stay until it is purged, which deletes them.
#[tokio::test]
async fn purging_a_soft_dropped_ledger_deletes_its_files() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(&path).build().expect("build");

    let ledger_id = "drop-hard-retracted:main";
    let ledger_name = "drop-hard-retracted";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:test",
        "ex:name": "Test"
    });
    let namespace = fluree
        .insert(ledger, &tx)
        .await
        .expect("insert")
        .ledger
        .storage_namespace();

    // Soft drop keeps the files
    let r1 = fluree
        .drop_ledger(ledger_name, DropMode::Soft)
        .await
        .expect("soft drop");
    assert_eq!(r1.status, DropStatus::Dropped);

    // Verify files still exist
    let commit_prefix = format!("fluree:file://{}/commit/", namespace.branch_prefix());
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

    // Purging the dropped ledger deletes them
    let r2 = fluree
        .purge_dropped(r1.instance.expect("instance").as_str())
        .await
        .expect("purge");
    assert!(
        r2.artifacts_deleted > 0,
        "purge should delete the dropped ledger's artifacts"
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
        cached_before.contains(&fluree_db_api::LedgerId::parse(ledger_id).unwrap()),
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
        !cached_after.contains(&fluree_db_api::LedgerId::parse(ledger_id).unwrap()),
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

/// Ledger names may contain `/`. Hard-dropping `test/db` must reach only its
/// own `test/db/@shared/dicts/`, never ledger `test`'s `test/@shared/dicts/`:
/// the old path heuristic read a branchless `test/db` as `name/branch` and
/// wiped the sibling ledger's dictionaries.
#[tokio::test]
async fn hard_drop_of_nested_name_keeps_the_parent_named_ledgers_dicts() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": "a string for the dicts"}]
    });
    for name in ["test", "test/db"] {
        let ledger = fluree.create_ledger(name).await.expect("create");
        fluree.insert(ledger, &txn).await.expect("insert");
        fluree
            .reindex(name, fluree_db_api::ReindexOptions::default())
            .await
            .expect("reindex writes dict blobs");
    }

    let admin = fluree.admin_storage().expect("managed backend");
    let dicts_of = |id: &'static str| {
        let fluree = &fluree;
        async move {
            let root = fluree.storage_namespace(id).await.unwrap().root().clone();
            format!("fluree:file://{}/dicts/", root.shared_prefix())
        }
    };
    let parent_dicts = dicts_of("test:main").await;
    let nested_dicts = dicts_of("test/db:main").await;
    let before = admin.list_prefix(&parent_dicts).await.expect("list");
    assert!(!before.is_empty(), "fixture: `test` must have dict blobs");
    assert!(
        !admin
            .list_prefix(&nested_dicts)
            .await
            .expect("list")
            .is_empty(),
        "fixture: `test/db` must have dict blobs"
    );

    fluree
        .drop_ledger("test/db", DropMode::Hard)
        .await
        .expect("drop test/db");

    let after = admin.list_prefix(&parent_dicts).await.expect("list");
    assert_eq!(after, before, "dropping `test/db` deleted `test`'s dicts");
    let nested = admin.list_prefix(&nested_dicts).await.expect("list");
    assert!(
        nested.is_empty(),
        "`test/db`'s own dicts must be reclaimed: {nested:?}"
    );

    // `test` still loads and answers from its index.
    fluree.disconnect_ledger("test").await;
    let db = fluree.db("test").await.expect("load test");
    assert!(db.t > 0);
}

/// Two legacy ledgers from before name bindings, both rooted at their
/// names: `a`, and `a/main/index/child`, whose files therefore sit inside
/// `a:main`'s `index/` directory. With `child_dropped`, the child is
/// soft-dropped: its records move to the dropped-ledger registry, and its
/// files stay where they are. Returns the child's files and its instance
/// when dropped.
async fn nested_legacy_ledgers(
    child_dropped: bool,
) -> (
    tempfile::TempDir,
    fluree_db_api::Fluree,
    Vec<String>,
    Option<String>,
) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": "a string for the dicts"}]
    });
    // New names can no longer take a reserved layout segment, and new
    // ledgers get roots of their own, so both are built the way such
    // ledgers would already exist: without `create_ledger`.
    for id in ["a:main", "a/main/index/child:main"] {
        fluree_db_nameservice::testing::create_at_name_root(fluree.publisher().unwrap(), id)
            .await
            .expect("a ledger from before name bindings");
        let ledger = fluree.ledger(id).await.expect("load");
        fluree.insert(ledger, &txn).await.expect("insert");
        fluree
            .reindex(id, fluree_db_api::ReindexOptions::default())
            .await
            .expect("reindex writes dict blobs");
    }

    let admin = fluree.admin_storage().expect("managed backend");
    let child = admin
        .list_prefix("fluree:file://a/main/index/child/")
        .await
        .expect("list");
    assert!(
        child.iter().any(|f| f.contains("/@shared/dicts/"))
            && child.iter().any(|f| f.contains("/main/commit/")),
        "fixture: child must have commits and shared dicts: {child:?}"
    );
    let instance = if child_dropped {
        let report = fluree
            .drop_ledger("a/main/index/child", DropMode::Soft)
            .await
            .expect("soft-drop child");
        Some(report.instance.expect("instance").to_string())
    } else {
        None
    };
    (tmp, fluree, child, instance)
}

async fn files_of_child(fluree: &fluree_db_api::Fluree) -> Vec<String> {
    fluree
        .admin_storage()
        .expect("managed backend")
        .list_prefix("fluree:file://a/main/index/child/")
        .await
        .expect("list")
}

/// A hard drop of `a` keeps a nested legacy ledger's files, its branches'
/// and its `@shared/` dictionaries, whether that ledger is live or
/// soft-dropped and waiting in the registry.
#[tokio::test]
async fn hard_drop_keeps_a_nested_legacy_ledgers_files() {
    for child_dropped in [false, true] {
        let (_tmp, fluree, before, instance) = nested_legacy_ledgers(child_dropped).await;
        fluree
            .drop_ledger("a", DropMode::Hard)
            .await
            .expect("drop a");
        assert_eq!(
            files_of_child(&fluree).await,
            before,
            "dropping `a` deleted the nested ledger's files (dropped: {child_dropped})"
        );
        if let Some(instance) = instance {
            fluree
                .restore_dropped(&instance)
                .await
                .expect("restore child");
        }
        let child_id = "a/main/index/child:main";
        fluree.disconnect_ledger(child_id).await;
        assert!(fluree.db(child_id).await.expect("child loads").t > 0);
    }
}

/// An index sweep of `a` plans none of a nested legacy ledger's files as
/// orphans, live or soft-dropped.
#[tokio::test]
async fn index_sweep_keeps_a_nested_legacy_ledgers_files() {
    for child_dropped in [false, true] {
        let (_tmp, fluree, before, _) = nested_legacy_ledgers(child_dropped).await;
        fluree.sweep_index_storage("a").await.expect("sweep a");
        assert_eq!(
            files_of_child(&fluree).await,
            before,
            "sweeping `a` deleted the nested ledger's files (dropped: {child_dropped})"
        );
    }
}

async fn files_under(tier: &fluree_db_core::MemoryStorage, root: &str) -> Vec<String> {
    use fluree_db_core::StorageRead;
    tier.list_prefix(&format!("fluree:memory://{root}/"))
        .await
        .expect("list")
}

/// Purge lists an instance root once and on both storage tiers, so it also
/// removes files that no branch of the dropped ledger names.
#[tokio::test]
async fn purge_deletes_an_instance_root_from_both_storage_tiers() {
    use fluree_db_api::{ConnectionConfig, DroppedData, Fluree, NameServiceMode, TieredStorage};
    use fluree_db_core::MemoryStorage;
    use std::sync::Arc;

    let (commit, index) = (MemoryStorage::new(), MemoryStorage::new());
    let fluree = Fluree::new(
        ConnectionConfig::memory(),
        TieredStorage::new(commit.clone(), index.clone()),
        NameServiceMode::ReadWrite(Arc::new(
            fluree_db_nameservice::memory::MemoryNameService::new(),
        )),
    );
    let ledger = fluree.create_ledger("tiered:main").await.expect("create");
    let tx = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:name": "A"});
    let ledger = fluree.insert(ledger, &tx).await.expect("insert").ledger;
    fluree
        .set_default_context("tiered:main", &json!({"ex": "http://example.org/"}))
        .await
        .expect("context");
    let root = ledger.storage_namespace().root().to_string();
    assert!(!files_under(&commit, &root).await.is_empty(), "commits");
    assert!(!files_under(&index, &root).await.is_empty(), "config");

    let report = fluree
        .drop_ledger("tiered", DropMode::Soft)
        .await
        .expect("drop");
    // A branch created as the ledger was dropped left files behind.
    index.insert(
        format!("fluree:memory://{root}/late/index/x"),
        b"x".to_vec(),
    );

    let purged = fluree
        .purge_dropped(report.instance.expect("instance").as_str())
        .await
        .expect("purge");
    assert_eq!(purged.data, Some(DroppedData::Deleted), "{purged:?}");
    assert_eq!(files_under(&commit, &root).await, Vec::<String>::new());
    assert_eq!(files_under(&index, &root).await, Vec::<String>::new());
}

/// The orphan sweep deletes instance folders nothing references, such as
/// bytes a stale writer stored after its ledger was purged, and nothing
/// else: not live or soft-dropped ledgers, and not a name root's `@shared`.
#[tokio::test]
async fn orphan_sweep_deletes_only_unreferenced_instance_folders() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let admin = fluree.admin_storage().expect("managed backend");
    let tx = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:name": "A"});

    let mut roots = std::collections::HashMap::new();
    for name in ["live", "kept", "gone"] {
        let ledger = fluree
            .create_ledger(&format!("{name}:main"))
            .await
            .expect("create");
        let ledger = fluree.insert(ledger, &tx).await.expect("insert").ledger;
        roots.insert(name, ledger.storage_namespace().root().to_string());
    }
    let kept = fluree
        .drop_ledger("kept", DropMode::Soft)
        .await
        .expect("drop kept")
        .instance
        .expect("instance");
    fluree
        .drop_ledger("gone", DropMode::Hard)
        .await
        .expect("drop gone");

    // A writer that resolved `gone` before its drop stores a commit after
    // the purge listed the folder; a create that never finished left one.
    let late = format!("fluree:file://{}/main/commit/late.fcv2", roots["gone"]);
    let abandoned = "abandoned/@01JB8ZK4X5Y6Z7A8B9C0D1E2F3";
    let name_root = [
        "fluree:file://legacy/main/commit/c.fcv2",
        "fluree:file://legacy/@shared/dicts/d",
    ];
    for address in [
        late.as_str(),
        &format!("fluree:file://{abandoned}/main/txn/t"),
    ]
    .into_iter()
    .chain(name_root)
    {
        admin.write_bytes(address, b"x").await.expect("write");
    }
    let everything = |prefix: &str| {
        let prefix = format!("fluree:file://{prefix}/");
        async move {
            let mut files = admin.list_prefix(&prefix).await.expect("list");
            files.sort();
            files
        }
    };
    let live_files = everything(&roots["live"]).await;
    let kept_files = everything(&roots["kept"]).await;

    let dry = fluree.sweep_orphan_instances(true).await.expect("dry run");
    let mut found: Vec<String> = dry.orphans.iter().map(|o| o.root.to_string()).collect();
    found.sort();
    assert_eq!(found, vec![abandoned.to_string(), roots["gone"].clone()]);
    assert_eq!(dry.artifacts_deleted, 0);
    assert!(
        admin.exists(&late).await.expect("exists"),
        "a dry run deletes nothing"
    );

    let swept = fluree.sweep_orphan_instances(false).await.expect("sweep");
    assert_eq!(swept.orphans.len(), 2);
    assert_eq!(swept.artifacts_deleted, 2, "{swept:?}");
    assert!(everything(&roots["gone"]).await.is_empty());
    assert!(everything(abandoned).await.is_empty());
    // Background indexing may add files meanwhile; the sweep must remove none.
    for (root, before) in [("live", live_files), ("kept", kept_files)] {
        let after = everything(&roots[root]).await;
        let removed: Vec<_> = before.iter().filter(|f| !after.contains(f)).collect();
        assert!(removed.is_empty(), "the sweep deleted {root}'s {removed:?}");
    }
    for address in name_root {
        assert!(admin.exists(address).await.expect("exists"), "{address}");
    }

    fluree
        .restore_dropped(kept.as_str())
        .await
        .expect("the soft-dropped ledger still restores");
    assert!(fluree.db("kept:main").await.expect("load kept").t > 0);
}

/// A branch drop that cannot list ledgers keeps the branch's files, and its
/// listing, so nothing is left unreachable: dropping it again finishes.
#[tokio::test]
async fn drop_branch_keeps_its_files_when_ledgers_cannot_be_listed() {
    use fluree_db_api::{Fluree, NameServiceMode};
    use std::sync::Arc;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(&path).build().expect("build");
    let txn = |id: &str| {
        json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": id, "ex:val": 1}]
        })
    };
    let main = fluree.create_ledger("drop-listing").await.unwrap();
    fluree.insert(main, &txn("ex:a")).await.unwrap();
    fluree
        .create_branch("drop-listing", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("drop-listing:dev").await.unwrap();
    let dev_commits = tmp.path().join(format!(
        "{}/commit",
        dev.storage_namespace().branch_prefix()
    ));
    fluree.insert(dev, &txn("ex:b")).await.unwrap();
    assert!(dev_commits.read_dir().unwrap().next().is_some());

    let failing = Arc::new(crate::race_nameservice::PausingNameService::new(
        fluree.nameservice_mode().publisher_arc().unwrap(),
    ));
    failing.fail_listings();
    let dropper = Fluree::from_backend(
        fluree.config().clone(),
        fluree.backend().clone(),
        NameServiceMode::ReadWrite(failing),
    );
    let report = dropper
        .drop_branch("drop-listing", "dev")
        .await
        .expect("drop");
    assert!(!report.warnings.is_empty(), "{report:?}");
    assert!(dev_commits.read_dir().unwrap().next().is_some());

    let finished = fluree
        .drop_branch("drop-listing", "dev")
        .await
        .expect("drop again");
    assert!(finished.warnings.is_empty(), "{finished:?}");
    assert!(
        !dev_commits.exists() || dev_commits.read_dir().unwrap().next().is_none(),
        "dropping again deletes the files"
    );
}

/// Pauses the first listing of a prefix ending in `suffix`, once armed,
/// until resumed.
#[derive(Debug)]
struct PauseListing {
    suffix: &'static str,
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl crate::support::hooked_storage::StorageHooks for PauseListing {
    async fn before_list(&self, prefix: &str) {
        use std::sync::atomic::Ordering;
        if prefix.ends_with(self.suffix) && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
}

/// A branch drop finished by another run while this one was still deleting
/// leaves alone what the folder holds since: dropping the branch again
/// finishes it, as housekeeping resuming a slow drop would, and the branch
/// created again under its name reuses the folder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_branch_drop_finished_elsewhere_spares_the_branch_created_since() {
    use crate::support::hooked_storage::HookedStorage;
    use fluree_db_api::{NameServiceMode, ReindexOptions};
    use fluree_db_core::StorageRead;
    use std::sync::Arc;

    let storage = HookedStorage::new(PauseListing {
        suffix: "/dev/txn/",
        armed: false.into(),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let fluree = Arc::new(FlureeBuilder::memory().build_with(
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(
            fluree_db_nameservice::memory::MemoryNameService::new(),
        )),
    ));
    let main = fluree.create_ledger("pause").await.expect("create");
    let txn = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1});
    fluree.insert(main, &txn).await.expect("insert");
    fluree
        .reindex("pause", ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
        .create_branch("pause", "dev", None, None)
        .await
        .expect("create dev");
    let namespace = fluree.storage_namespace("pause:dev").await.unwrap();
    let dev_index = format!("fluree:memory://{}/index/", namespace.branch_prefix());

    storage
        .hooks()
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first = tokio::spawn({
        let fluree = Arc::clone(&fluree);
        async move { fluree.drop_branch("pause", "dev").await }
    });
    storage.hooks().entered.notified().await;

    fluree
        .drop_branch("pause", "dev")
        .await
        .expect("the second run finishes the drop");
    fluree
        .create_branch("pause", "dev", None, None)
        .await
        .expect("dev again");
    let recreated = storage.list_prefix(&dev_index).await.unwrap();
    assert!(
        !recreated.is_empty(),
        "fixture: the new dev must have an index in the folder"
    );

    storage.hooks().resume.notify_one();
    let report = first.await.unwrap().expect("first run");
    assert_eq!(
        storage.list_prefix(&dev_index).await.unwrap(),
        recreated,
        "the first run deleted the new dev's index"
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("finished elsewhere")),
        "{report:?}"
    );
    fluree.disconnect_ledger("pause:dev").await;
    assert!(fluree.db("pause:dev").await.expect("dev loads").t > 0);
}
