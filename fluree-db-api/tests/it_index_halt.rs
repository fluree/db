//! What lifts a halted ledger (`IndexStatusResult::halted`).

#![cfg(feature = "native")]

use crate::support::{start_background_indexer_local, trigger_index_and_wait_outcome};
use fluree_db_api::{FlureeBuilder, LedgerId, ReindexOptions};
use serde_json::json;

/// A reindex that succeeds builds the ledger's index, so a halt the
/// background indexer recorded for the ledger no longer holds.
#[tokio::test]
async fn a_reindex_that_succeeds_lifts_a_halt() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
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
            let ledger_id = "it/index-halt:main";
            fluree.create_ledger(ledger_id).await.expect("create");
            let data = json!({"@context": {"ex": "http://example.org/"},
                "@id": "ex:a", "ex:v": 1});
            let t = fluree
                .graph(ledger_id)
                .transact()
                .insert(&data)
                .commit()
                .await
                .expect("insert")
                .receipt
                .t;
            // Index first, so the halt is recorded at the index the reindex
            // republishes: nothing but the reindex itself can lift it.
            trigger_index_and_wait_outcome(&handle, ledger_id, t).await;

            let ledger = LedgerId::parse(ledger_id).expect("ledger id");
            handle
                .halt_for_test(&ledger, t, "commit x: bad value")
                .await;
            let status = fluree.index_status(ledger_id).await.expect("status");
            assert!(status.halted);
            assert_eq!(status.last_error.as_deref(), Some("commit x: bad value"));

            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .expect("reindex");
            let status = fluree.index_status(ledger_id).await.expect("status");
            assert!(!status.halted, "the reindex built the index");
            assert_eq!(status.last_error, None);
        })
        .await;
}
