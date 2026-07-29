//! Upsert replace-semantics against an indexed ledger.
//!
//! Regression coverage for the upsert-deletion subject pre-check in
//! `generate_upsert_deletions`: subjects absent from both the persisted
//! subject dictionary and novelty skip their existing-value lookups entirely
//! (a bound-subject scan for an unresolvable subject degrades to a full PSOT
//! predicate-partition walk — ~8 ms/flake on real ledgers). These tests pin
//! that the skip never drops retractions for subjects that DO exist:
//! persisted (base index), novelty-only (committed after the last index), and
//! brand-new subjects all in one transaction.

#![cfg(feature = "native")]

use crate::support::{
    normalize_rows, query_jsonld_formatted, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

async fn name_values(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
) -> Vec<serde_json::Value> {
    let query = json!({
        "@context": ctx(),
        "where": {"@id": subject, "ex:name": "?n"},
        "select": ["?n"]
    });
    let rows = query_jsonld_formatted(fluree, ledger, &query)
        .await
        .expect("query");
    normalize_rows(&rows)
}

/// One upsert touching a persisted subject, a novelty-only subject, and a
/// brand-new subject must replace values for the first two and plainly insert
/// the third — before and after the next index build.
#[tokio::test]
async fn upsert_mixed_persisted_novelty_new_subjects() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

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
            let ledger_id = "it/upsert-indexed-mixed:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Persisted subject with a multi-valued property (all values must
            // be retracted on upsert, not just one).
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": "ex:a", "ex:name": ["A1a", "A1b"]},
                            {"@id": "ex:b", "ex:name": "B1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // Index + reload so ex:a / ex:b live in the binary index.
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger.snapshot.range_provider.is_some());

            // Novelty-only subject: committed after the index build.
            let r2 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:c",
                        "ex:name": "C1"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            // One upsert across all three subject classes.
            let r3 = fluree
                .upsert_with_opts(
                    r2.ledger,
                    &json!({
                        "@context": ctx(),
                        "@graph": [
                            {"@id": "ex:a", "ex:name": "A2"},
                            {"@id": "ex:c", "ex:name": "C2"},
                            {"@id": "ex:d", "ex:name": "D1"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            let expect = |v: &str| vec![json!([v])];
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:a").await,
                expect("A2"),
                "persisted subject: both old values replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:b").await,
                expect("B1"),
                "untouched subject keeps its value"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:c").await,
                expect("C2"),
                "novelty-only subject: old value replaced"
            );
            assert_eq!(
                name_values(&fluree, &r3.ledger, "ex:d").await,
                expect("D1"),
                "new subject inserted"
            );

            // Same assertions after the next index build (retractions must
            // have been staged, not merely masked by novelty).
            trigger_index_and_wait_outcome(&handle, ledger_id, r3.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.unwrap();
            assert_eq!(name_values(&fluree, &ledger, "ex:a").await, expect("A2"));
            assert_eq!(name_values(&fluree, &ledger, "ex:b").await, expect("B1"));
            assert_eq!(name_values(&fluree, &ledger, "ex:c").await, expect("C2"));
            assert_eq!(name_values(&fluree, &ledger, "ex:d").await, expect("D1"));
        })
        .await;
}
