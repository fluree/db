//! Regression: `ledger-info` must report non-zero `ledger.size` after reindex.
//!
//! We expect `ledger.size` to reflect total commit data size (bytes), as stored in
//! FIR6 root metadata and surfaced via `LedgerState::current_stats()`.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

#[tokio::test]
async fn ledger_info_size_is_not_reset_to_zero_by_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ledger-info-reindex-size:main";

    // Create a small commit chain (enough to have non-trivial commit bytes).
    let mut ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
    for i in 0..5u32 {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [
                { "@id": format!("ex:s{i}"), "ex:name": format!("name-{i}"), "ex:n": i }
            ]
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                TxnOpts::default(),
                CommitOpts::default(),
                &fluree_db_api::IndexConfig {
                    // Don't trigger background indexing; reindex will rebuild from commits.
                    reindex_min_bytes: 1_000_000_000,
                    reindex_max_bytes: 1_000_000_000,
                },
            )
            .await
            .expect("insert")
            .ledger;
    }

    // 1) Initial reindex build (creates an index).
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex (initial)");

    let info1 = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info after initial reindex");
    let size1 = info1["ledger"]["size"].as_u64().expect("ledger.size u64");
    let gsize1 = info1["stats"]["size"].as_u64().expect("stats.size u64");
    let named_graphs1 = info1["ledger"]["named-graphs"]
        .as_array()
        .expect("ledger.named-graphs array");
    assert!(
        named_graphs1.iter().any(|g| {
            g["g-id"].as_u64() == Some(0)
                && g.get("flakes")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && g.get("size").and_then(serde_json::Value::as_u64).is_some()
        }),
        "expected default graph entry in ledger.named-graphs to include flakes/size: {info1}"
    );
    assert!(
        size1 > 0,
        "expected ledger.size > 0 after initial reindex, got {size1}: {info1}"
    );
    assert!(
        gsize1 > 0,
        "expected graph-scoped stats.size > 0 after reindex (estimated), got {gsize1}: {info1}"
    );

    // 2) Force a second reindex at the same commit_t; size must remain non-zero.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex (second)");

    let info2 = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger_info after second reindex");
    let size2 = info2["ledger"]["size"].as_u64().expect("ledger.size u64");
    let gsize2 = info2["stats"]["size"].as_u64().expect("stats.size u64");
    let named_graphs2 = info2["ledger"]["named-graphs"]
        .as_array()
        .expect("ledger.named-graphs array");
    assert!(
        named_graphs2.iter().any(|g| {
            g["g-id"].as_u64() == Some(0)
                && g.get("flakes")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && g.get("size").and_then(serde_json::Value::as_u64).is_some()
        }),
        "expected default graph entry in ledger.named-graphs to include flakes/size: {info2}"
    );
    assert!(
        size2 > 0,
        "expected ledger.size > 0 after second reindex, got {size2}: {info2}"
    );
    assert!(
        gsize2 > 0,
        "expected graph-scoped stats.size > 0 after second reindex (estimated), got {gsize2}: {info2}"
    );
}
