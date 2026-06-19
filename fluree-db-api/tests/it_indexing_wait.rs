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

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_core::{load_ledger_snapshot, LedgerSnapshot};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::start_background_indexer_local;
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
        .indexing_mode
        .handle()
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
