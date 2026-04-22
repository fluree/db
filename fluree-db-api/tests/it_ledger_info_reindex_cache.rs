//! Regression: ledger-info API cache must bust on reindex allow-equal.
//!
//! Reindex publishes a new index root CID at the same `index_t`. If ledger-info caching
//! keys only on `(commit_t, index_t, opts, ...)`, it can return stale JSON after reindex.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_indexer::IndexerConfig;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

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
