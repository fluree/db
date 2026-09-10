//! Background indexing wait integration test
//!
//! Common workflow:
//! - commit
//! - wait for indexing to complete
//! - reload/query/assert against the persisted index
//!
//! Rust equivalent:
//! - transact (capture `receipt.t`)
//! - `handle.trigger(alias, receipt.t)`
//! - `completion.wait().await`
//! - then load `LedgerSnapshot` from the index root and assert `db.t >= receipt.t`

#![cfg(feature = "native")]

use crate::support::start_background_indexer_local;
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::{load_ledger_snapshot, LedgerSnapshot};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn background_indexing_trigger_wait_then_load_index_root() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    // Build file-backed Fluree (so we can load the index root from storage).
    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    // Start background indexing worker + handle (LocalSet since worker may be !Send).
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
            // Genesis ledger state (uncommitted; nameservice record created on first commit).
            let ledger_id = "it/index-wait:main";
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = fluree_db_api::LedgerState::new(db0, fluree_db_api::Novelty::new(0));

            // Force indexing_needed=true for the test.
            // Must be large enough to allow the novelty write; we just want min_bytes=0
            // so background indexing is always triggered.
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };

            // 1) Transact
            let tx = json!({
                "@context": {"ex":"http://example.org/"},
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            });

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            let commit_t = result.receipt.t;
            assert!(commit_t >= 0);

            // 2) Trigger indexing predicate: index_t >= commit_t
            let completion = handle.trigger(result.ledger.ledger_id(), commit_t).await;

            // 3) Wait + assert we can load the persisted root
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed {
                    index_t, root_id, ..
                } => {
                    assert!(
                        index_t >= commit_t,
                        "index_t ({index_t}) should be >= commit_t ({commit_t})"
                    );
                    assert!(root_id.is_some(), "expected a root_id after indexing");

                    let root_cid = root_id.unwrap();
                    let loaded = load_ledger_snapshot(
                        &fluree
                            .backend()
                            .admin_storage_cloned()
                            .expect("test uses managed backend"),
                        &root_cid,
                        "it/index-wait:main",
                    )
                    .await
                    .expect("load_ledger_snapshot(root_cid)");
                    assert!(
                        loaded.t >= commit_t,
                        "loaded db.t ({}) should be >= commit_t ({})",
                        loaded.t,
                        commit_t
                    );
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }
        })
        .await;
}

#[tokio::test]
async fn cached_handle_applies_local_background_index_publish_without_refresh() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path)
        .with_indexing_thresholds(1_000_000, 10_000_000)
        .build()
        .expect("build file fluree");
    let indexer = fluree
        .indexer_handle()
        .expect("file builder should start background indexing")
        .clone();

    let ledger_id = "it/local-cache-index-refresh:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let cached = fluree.ledger_cached(ledger_id).await.expect("cache ledger");

    let tx = json!({
        "@context": {"ex":"http://example.org/"},
        "@id": "ex:alice",
        "ex:name": "Alice"
    });

    let result = fluree
        .stage(&cached)
        .insert(&tx)
        .execute()
        .await
        .expect("cached insert");
    let commit_t = result.receipt.t;

    let before = cached.snapshot().await;
    assert_eq!(before.t, commit_t);
    assert_eq!(before.index_t(), 0, "cached handle starts on genesis index");
    assert!(
        before.novelty.size > 0,
        "cached handle has unindexed novelty before background publish"
    );
    drop(before);

    let completion = indexer.trigger(ledger_id, commit_t).await;
    match completion.wait().await {
        fluree_db_api::IndexOutcome::Completed { index_t, .. } => {
            assert!(
                index_t >= commit_t,
                "background index_t ({index_t}) should cover commit_t ({commit_t})"
            );
        }
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    }

    let mut last_index_t = 0;
    let mut last_novelty_size = usize::MAX;
    for _ in 0..100 {
        let view = cached.snapshot().await;
        last_index_t = view.index_t();
        last_novelty_size = view.novelty.size;
        if last_index_t >= commit_t && last_novelty_size == 0 {
            return;
        }
        drop(view);
        sleep(Duration::from_millis(20)).await;
    }

    panic!(
        "cached handle did not apply local index event without refresh: index_t={last_index_t}, novelty_size={last_novelty_size}, commit_t={commit_t}"
    );
}

/// Wait until the cached handle has applied a publish covering `commit_t`,
/// returning the view that shows it.
async fn view_after_publish(
    cached: &fluree_db_api::LedgerHandle,
    commit_t: i64,
) -> fluree_db_api::LedgerView {
    for _ in 0..200 {
        let view = cached.snapshot().await;
        if view.index_t() >= commit_t && view.novelty.size == 0 {
            return view;
        }
        drop(view);
        sleep(Duration::from_millis(20)).await;
    }
    panic!("cached handle did not apply the publish covering t={commit_t}");
}

/// An incremental publish keeps nearly every artifact of the previous root.
/// The store installed for it must carry those over from the store it
/// replaces — every forward pack the two roots share is the same open
/// handle, and an unchanged namespace table is the same allocation — rather
/// than reopening the whole index on every publish.
#[tokio::test]
async fn an_index_publish_reuses_the_artifacts_of_the_store_it_replaces() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path)
        .with_indexing_thresholds(1_000_000, 10_000_000)
        .build()
        .expect("build file fluree");
    let indexer = fluree
        .indexer_handle()
        .expect("file builder should start background indexing")
        .clone();

    let ledger_id = "it/publish-reuse:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let cached = fluree.ledger_cached(ledger_id).await.expect("cache ledger");

    let publish = |round: usize| {
        let fluree = &fluree;
        let cached = &cached;
        let indexer = &indexer;
        async move {
            let tx = json!({
                "@context": {"ex": "http://example.org/"},
                // Same namespaces every round, so the namespace table of the
                // second root is exactly the first's.
                "@graph": (0..3).map(|i| json!({
                    "@id": format!("ex:thing-{round}-{i}"),
                    "ex:label": format!("round {round} thing {i}")
                })).collect::<Vec<_>>()
            });
            let commit_t = fluree
                .stage(cached)
                .insert(&tx)
                .execute()
                .await
                .expect("insert")
                .receipt
                .t;
            match indexer.trigger(ledger_id, commit_t).await.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                other => panic!("indexing did not complete: {other:?}"),
            }
            view_after_publish(cached, commit_t)
                .await
                .binary_store
                .expect("published index attaches a store")
        }
    };

    let first = publish(0).await;
    let second = publish(1).await;
    assert!(
        !Arc::ptr_eq(&first, &second),
        "a publish installs a new store"
    );

    let (shared, total) = second.forward_packs_shared_with(&first);
    let (_, before) = first.forward_packs_shared_with(&first);
    assert!(before > 0, "the first root has forward packs");
    assert_eq!(
        shared, before,
        "every pack of the previous root must be carried over, not reopened"
    );
    assert!(
        total > shared,
        "the second root appends packs for the new subjects and strings"
    );
    assert!(
        second.shares_namespace_tables_with(&first),
        "an unchanged namespace table must be shared, not rebuilt"
    );
}
