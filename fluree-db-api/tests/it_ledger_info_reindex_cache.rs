//! Regression: ledger-info API cache must bust on reindex allow-equal.
//!
//! Reindex publishes a new index root CID at the same `index_t`. If ledger-info caching
//! keys only on `(commit_t, index_t, opts, ...)`, it can return stale JSON after reindex.

#![cfg(feature = "native")]

use crate::support::genesis_ledger_for_fluree;
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_indexer::IndexerConfig;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

#[tokio::test]
async fn ledger_info_cache_busts_on_reindex_allow_equal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ledger-info-reindex-cache:main";

    // Create a dataset large enough that different indexer configs produce
    // different physical index layouts (different root CID) at the same `t`.
    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let mut graph = Vec::new();
    for i in 0..400u32 {
        graph.push(json!({
            "@id": format!("ex:s{i}"),
            "ex:name": format!("name-{i}"),
            "ex:n": i
        }));
    }
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": graph
    });
    let _ledger1 = fluree
        .insert_with_opts(
            ledger0,
            &tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert")
        .ledger;

    // First reindex.
    fluree
        .reindex(
            ledger_id,
            ReindexOptions::default().with_indexer_config(
                IndexerConfig::default()
                    .with_leaflet_rows(10)
                    .with_leaflets_per_leaf(1),
            ),
        )
        .await
        .expect("reindex #1");

    // Warm the ledger-info cache.
    let info1 = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info #1");
    let index1 = info1["indexId"]
        .as_str()
        .expect("indexId string")
        .to_string();

    // Second reindex at the same commit_t/index_t but with a different indexer config.
    // This should publish a different root CID (same logical data, different physical layout).
    fluree
        .reindex(
            ledger_id,
            ReindexOptions::default().with_indexer_config(
                IndexerConfig::default()
                    .with_leaflet_rows(50)
                    .with_leaflets_per_leaf(4),
            ),
        )
        .await
        .expect("reindex #2");

    // If the cache key does not include the index CID, this would return the stale JSON
    // (with the old indexId). We require it to reflect the new index root.
    let info2 = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info #2");
    let index2 = info2["indexId"]
        .as_str()
        .expect("indexId string")
        .to_string();

    assert_ne!(
        index1, index2,
        "expected indexId to change after allow-equal reindex; got same indexId.\ninfo1={info1}\ninfo2={info2}"
    );
}

/// The HTTP metadata route builds from the cached state. Its commit metadata
/// must advance immediately even while the independently maintained index lags.
#[tokio::test]
async fn cached_ledger_info_tracks_commits_without_waiting_for_indexing() {
    use fluree_db_api::ledger_info::{build_ledger_info_for_connection, LedgerInfoOptions};

    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let ledger_id = "it/cached-commit-info:main";
    let initial = fluree.create_ledger(ledger_id).await.unwrap();
    fluree
        .insert(initial, &json!({"@id": "seed", "name": "seed"}))
        .await
        .unwrap();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let indexed = handle.snapshot().await.to_ledger_state();
    assert_eq!(indexed.t(), 1);
    assert_eq!(indexed.index_t(), 1);
    let baseline_record = indexed.ns_record.unwrap();

    for n in 2..=3 {
        let tx = json!({"@id": format!("node-{n}"), "name": format!("name-{n}")});
        let result = fluree.stage(&handle).insert(&tx).execute().await.unwrap();
        let receipt = result.receipt;
        assert_eq!(receipt.t, n);
        let cached = handle.snapshot().await.to_ledger_state();
        assert_eq!(cached.t(), n);
        assert_eq!(cached.index_t(), 1, "indexing is deliberately not running");
        assert_eq!(cached.head_commit_id.as_ref(), Some(&receipt.commit_id));

        let info =
            build_ledger_info_for_connection(&fluree, &cached, None, LedgerInfoOptions::default())
                .await
                .unwrap();
        assert_eq!(info["commitId"], receipt.commit_id.to_string());
        assert_eq!(info["ledger"]["commit-t"], n, "cached metadata: {info}");
        assert_eq!(info["ledger"]["index-t"], 1);
        assert_eq!(info["nameservice"]["f:t"], n);
        assert_eq!(
            info["nameservice"]["f:ledgerCommit"]["@id"],
            receipt.commit_id.to_string()
        );

        // All unrelated nameservice fields, especially the independent index
        // pointer, retain the loaded record's values.
        let mut expected = baseline_record.clone();
        expected.commit_t = n;
        expected.commit_head_id = Some(receipt.commit_id);
        assert_eq!(cached.ns_record.as_ref(), Some(&expected));
    }

    // A later background-equivalent index adoption cannot revert commit metadata.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    fluree
        .refresh(ledger_id, fluree_db_api::RefreshOpts::default())
        .await
        .unwrap();
    let caught_up = handle.snapshot().await.to_ledger_state();
    assert_eq!(caught_up.index_t(), 3);
    assert_eq!(caught_up.t(), 3);
    let info =
        build_ledger_info_for_connection(&fluree, &caught_up, None, LedgerInfoOptions::default())
            .await
            .unwrap();
    assert_eq!(info["ledger"]["commit-t"], 3);
    assert_eq!(info["ledger"]["index-t"], 3);
    assert_eq!(info["nameservice"]["f:t"], 3);
}
