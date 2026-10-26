//! A ledger dropped and created again under its name can reach the same
//! index `t` with different data. Its queries must be planned with its own
//! index stats, never the planner stats view cached for the dropped ledger.

#![cfg(feature = "native")]

use crate::support::{
    assert_index_defaults, genesis_ledger_for_fluree, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};
use fluree_db_api::{DropMode, FlureeBuilder, IndexConfig, LedgerManagerConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value};

/// The dropped ledger's `ex:a` and `ex:b` only ever held refs, which licenses
/// folding `FILTER(?x = ?y)` into a join on the term. The recreated ledger,
/// indexed to the same `t`, holds `1` and `1.0` there: equal values but
/// different terms, so the filter has to stay a value comparison.
#[tokio::test]
async fn a_recreated_ledger_is_planned_with_its_own_index_stats() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/stats-view-recreate:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let seed = |txs: Vec<Value>| {
                let fluree = &fluree;
                let handle = &handle;
                let index_cfg = &index_cfg;
                async move {
                    let mut ledger = genesis_ledger_for_fluree(fluree, ledger_id);
                    for tx in txs {
                        ledger = fluree
                            .insert_with_opts(
                                ledger,
                                &tx,
                                TxnOpts::default(),
                                CommitOpts::default(),
                                index_cfg,
                            )
                            .await
                            .expect("insert")
                            .ledger;
                    }
                    let _ = trigger_index_and_wait_outcome(handle, ledger_id, ledger.t()).await;
                    let status = fluree.index_status(ledger_id).await.expect("index_status");
                    assert_eq!(status.index_t, 2, "index should be at t=2");
                }
            };
            let equal_pairs = || {
                let fluree = &fluree;
                async move {
                    let query = json!({
                        "@context": {"ex": "http://example.org/"},
                        "select": "?s",
                        "where": [
                            {"@id": "?s", "ex:a": "?x", "ex:b": "?y"},
                            ["filter", "(= ?x ?y)"]
                        ]
                    });
                    let db = fluree.db(ledger_id).await.expect("db");
                    let result = fluree.query(&db, &query).await.expect("query");
                    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
                    result.to_jsonld(&ledger.snapshot).expect("to_jsonld")
                }
            };
            let ctx = json!({
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            });

            seed(vec![
                json!({"@context": ctx, "@id": "ex:s", "ex:a": {"@id": "ex:o"}}),
                json!({"@context": ctx, "@id": "ex:s", "ex:b": {"@id": "ex:o"}}),
            ])
            .await;
            assert_eq!(equal_pairs().await, json!(["ex:s"]));

            fluree
                .drop_ledger("it/stats-view-recreate", DropMode::Hard)
                .await
                .expect("drop");

            seed(vec![
                json!({"@context": ctx, "@id": "ex:s", "ex:a": {"@value": "1", "@type": "xsd:integer"}}),
                json!({"@context": ctx, "@id": "ex:s", "ex:b": {"@value": "1.0", "@type": "xsd:decimal"}}),
            ])
            .await;
            assert_eq!(
                equal_pairs().await,
                json!(["ex:s"]),
                "1 = 1.0 as values; a join on the term means the recreated ledger \
                 was planned with the dropped ledger's stats"
            );
        })
        .await;
}
