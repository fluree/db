//! Indexing workflow integration tests
//!

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{
    Fluree, FlureeBuilder, IndexConfig, IndexingMode, LedgerState, Novelty, ReindexOptions,
    TriggerIndexOptions,
};
use fluree_db_core::LedgerSnapshot;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{assert_index_defaults, normalize_rows, start_background_indexer_local};

#[tokio::test]
async fn indexing_disabled_transaction_exposes_indexing_status_hints() {
    // Scenario: `manual-indexing-test` (transaction metadata)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-disabled-metadata:main";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice","ex:age":30},
            {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob","ex:age":25}
        ]
    });

    let result = fluree.insert(ledger0, &tx).await.expect("insert");

    assert_eq!(result.receipt.t, 1);
    assert!(
        !result.indexing.enabled,
        "indexing should be disabled by default"
    );
    assert!(
        !result.indexing.needed,
        "small novelty should not exceed default reindex_min_bytes"
    );
    assert!(
        result.indexing.novelty_size < 100_000,
        "novelty_size should be below default threshold"
    );
    assert_eq!(
        result.indexing.index_t, 0,
        "with indexing disabled, indexed state should remain at t=0"
    );

    // Scenario: "Trigger index API can be called" (trigger+wait succeeds).
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    local
        .run_until(async move {
            let completion = handle.trigger(ledger_id, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }
        })
        .await;
}

#[tokio::test]
async fn manual_indexing_disabled_mode_then_trigger_updates_nameservice_and_loads_indexed_ledger() {
    // Scenario: `manual-indexing-blocking-test` + `manual-indexing-updates-branch-state-test`
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-manual-trigger:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            for i in 0..10 {
                let tx = json!({
                    "@context": { "ex":"http://example.org/" },
                    "@id": format!("ex:person{i}"),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {i}"),
                    "ex:age": 20 + i,
                    "ex:email": format!("person{i}@example.com"),
                    "ex:description": format!("This is person {i} with extra text")
                });

                let r = fluree
                    .insert_with_opts(
                        ledger,
                        &tx,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert_with_opts");
                ledger = r.ledger;
            }

            let record = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("nameservice lookup")
                .expect("ns record");
            assert!(
                record.index_head_id.is_none(),
                "expected no index before manual trigger"
            );
            assert_eq!(record.commit_t, 10);

            let completion = handle.trigger(ledger_id, record.commit_t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let record2 = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("nameservice lookup")
                .expect("ns record");
            assert!(
                record2.index_head_id.is_some(),
                "expected index id after trigger"
            );
            assert!(
                record2.index_t >= record2.commit_t,
                "index_t should catch up"
            );

            let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
            assert_eq!(loaded.snapshot.t, 10, "loaded db should be at latest t");

            let query = json!({
                "@context": { "ex":"http://example.org/" },
                "select": ["?s"],
                "where": { "@id": "?s", "@type": "ex:Person" }
            });
            let result = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("query");
            let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
            assert_eq!(json_rows.as_array().map(std::vec::Vec::len), Some(10));
        })
        .await;
}

#[tokio::test]
async fn indexing_coalesces_multiple_commits_and_latest_root_is_queryable() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/indexing-workflow:main";
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let tx1 = json!({
                "@context": { "ex":"http://example.org/" },
                "@id":"ex:person0",
                "@type":"ex:Person",
                "ex:name":"Person 0",
                "ex:age":20
            });
            let r1 = fluree
                .insert_with_opts(
                    ledger0,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx1");

            let tx2 = json!({
                "@context": { "ex":"http://example.org/" },
                "@id":"ex:person1",
                "@type":"ex:Person",
                "ex:name":"Person 1",
                "ex:age":21
            });
            let r2 = fluree
                .insert_with_opts(
                    r1.ledger,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx2");

            let t1 = r1.receipt.t;
            let t2 = r2.receipt.t;
            assert!(t2 >= t1, "expected monotonic t");

            let c1 = handle.trigger(ledger_id, t1).await;
            let c2 = handle.trigger(ledger_id, t2).await;

            let (index_t2, _root_id2) = match c2.wait().await {
                fluree_db_api::IndexOutcome::Completed { index_t, root_id } => (index_t, root_id),
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            };
            assert!(index_t2 >= t2, "index_t should be >= latest commit t");

            match c1.wait().await {
                fluree_db_api::IndexOutcome::Completed { index_t, .. } => {
                    assert!(index_t >= t1, "index_t should be >= t1");
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Load via fluree.ledger() which attaches BinaryRangeProvider
            let ledger_loaded = fluree.ledger(ledger_id).await.expect("ledger load");
            assert!(
                ledger_loaded.t() >= index_t2,
                "loaded db.t should be >= indexed t"
            );

            let query = json!({
                "@context": { "ex":"http://example.org/" },
                "select": "?name",
                "where": { "@id": "?s", "@type": "ex:Person", "ex:name": "?name" }
            });

            let result = support::query_jsonld(&fluree, &ledger_loaded, &query)
                .await
                .expect("query");
            let json_rows = result.to_jsonld(&ledger_loaded.snapshot).expect("jsonld");

            assert_eq!(
                normalize_rows(&json_rows),
                normalize_rows(&json!(["Person 0", "Person 1"]))
            );
        })
        .await;
}

#[tokio::test]
async fn file_based_indexing_then_new_connection_loads_and_queries() {
    // Scenario: `file-based-indexing-test` (subset: new connection load)
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger_id = "it/indexing-file-load:main";
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            for i in 0..20 {
                let tx = json!({
                    "@context": { "ex":"http://example.org/" },
                    "@id": format!("ex:person{i}"),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {i}"),
                    "ex:age": 20 + i
                });
                let r = fluree
                    .insert_with_opts(
                        ledger,
                        &tx,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert_with_opts");
                ledger = r.ledger;
            }

            let completion = handle.trigger(ledger_id, ledger.t()).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let fluree2 = FlureeBuilder::file(path)
                .build()
                .expect("build file fluree2");
            let loaded = fluree2.ledger(ledger_id).await.expect("load ledger");
            assert_eq!(loaded.snapshot.t, 20);

            let query = json!({
                "@context": { "ex":"http://example.org/" },
                "select": ["?s"],
                "where": { "@id":"?s", "@type":"ex:Person" }
            });
            let result = support::query_jsonld(&fluree2, &loaded, &query)
                .await
                .expect("query");
            let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
            assert_eq!(json_rows.as_array().map(std::vec::Vec::len), Some(20));
        })
        .await;
}

#[tokio::test]
async fn automatic_indexing_disabled_mode_allows_novelty_to_accumulate_without_indexing() {
    // Scenario: `automatic-indexing-disabled-test`
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/indexing-disabled-accumulate:main";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));

    // Insert multiple transactions to build up novelty
    for i in 0..5 {
        let tx = json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": format!("ex:person{i}"),
                "@type": "ex:Person",
                "ex:name": format!("Person {i}"),
                "ex:age": 20 + i,
                "ex:email": format!("person{i}@example.com"),
                "ex:description": format!("Text for person {i} ").repeat(100)
            }]
        });

        let result = fluree.insert(ledger, &tx).await.expect("insert");
        ledger = result.ledger;
        // insert already commits, no need for separate commit call
    }

    // Verify state after multiple transactions
    // Note: ledger.t() returns max(novelty.t, db.t) - db.t stays at 0 until indexing
    assert_eq!(ledger.t(), 5, "Should be at t=5 after 5 transactions");
    assert!(
        ledger.novelty.size > 500,
        "Should have accumulated significant novelty"
    );
    // Note: In memory storage, stats may not be persisted the same way as file storage
    // The key test is that novelty accumulates without automatic indexing
}

// ---------------------------------------------------------------------------
// Index admin tests (formerly it_indexing_admin.rs)
// ---------------------------------------------------------------------------

fn admin_alias(name: &str) -> String {
    format!("it-admin-indexing-{name}:main")
}

async fn seed_some_commits(fluree: &Fluree, ledger_id: &str, n: usize) -> LedgerState {
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));

    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };

    for i in 0..n {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:person{i}"),
            "@type": "ex:Person",
            "ex:name": format!("Person {i}")
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &idx_cfg,
            )
            .await
            .expect("insert_with_opts")
            .ledger;
    }
    ledger
}

#[tokio::test]
async fn index_status_reports_commit_and_index_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("status");

    let _ledger = seed_some_commits(&fluree, &a, 3).await;

    let status = fluree.index_status(&a).await.expect("index_status");
    assert_eq!(status.commit_t, 3);
    // Indexing is disabled by default in FlureeBuilder.
    assert!(!status.indexing_enabled);
    assert_eq!(status.index_t, 0);
}

#[tokio::test]
async fn trigger_index_errors_when_indexing_disabled() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("trigger-disabled");
    let _ledger = seed_some_commits(&fluree, &a, 1).await;

    let err = fluree
        .trigger_index(&a, TriggerIndexOptions::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Indexing is disabled") || err.contains("IndexingDisabled"),
        "got: {err}"
    );
}

#[tokio::test]
async fn trigger_index_no_commit_ledger_returns_index_t_zero() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();

    // Enable background indexer
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    local
        .run_until(async move {
            let ledger = fluree
                .create_ledger("it/admin-indexing:no-commits")
                .await
                .expect("create_ledger");
            assert_eq!(ledger.t(), 0);

            let r = fluree
                .trigger_index(
                    "it/admin-indexing:no-commits",
                    TriggerIndexOptions::default(),
                )
                .await
                .expect("trigger_index");
            assert_eq!(r.index_t, 0);
            assert!(r.root_id.is_none());
        })
        .await;
}

#[tokio::test]
async fn trigger_index_builds_index_to_current_commit_t() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("trigger-ok");

    // Enable background indexer
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    local
        .run_until(async move {
            let _ledger = seed_some_commits(&fluree, &a, 5).await;

            let before = fluree.index_status(&a).await.expect("index_status");
            assert_eq!(before.commit_t, 5);
            assert_eq!(before.index_t, 0);
            assert!(before.indexing_enabled);

            let r = fluree
                .trigger_index(&a, TriggerIndexOptions::default().with_timeout(30_000))
                .await
                .expect("trigger_index");
            assert_eq!(r.index_t, 5);
            assert!(r.root_id.is_some(), "expected root_id");

            let after = fluree.index_status(&a).await.expect("index_status");
            assert_eq!(after.commit_t, 5);
            assert_eq!(after.index_t, 5);
        })
        .await;
}

#[tokio::test]
async fn trigger_index_times_out_if_worker_not_running() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("trigger-timeout");

    // Create a background handle but do NOT run the LocalSet.
    let (_local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let _ledger = seed_some_commits(&fluree, &a, 10).await;

    let err = fluree
        .trigger_index(&a, TriggerIndexOptions::default().with_timeout(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("IndexTimeout") || err.contains("timeout") || err.contains("timed out"),
        "got: {err}"
    );
}

#[tokio::test]
async fn reindex_rebuilds_and_publishes_index_at_current_commit_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-ok");

    let ledger = seed_some_commits(&fluree, &a, 4).await;
    assert_eq!(ledger.t(), 4);

    let r = fluree
        .reindex(&a, ReindexOptions::default())
        .await
        .expect("reindex");
    assert_eq!(r.index_t, 4);
    assert!(r.root_id.digest_hex().len() == 64);

    // Nameservice record should be updated
    let status = fluree.index_status(&a).await.expect("index_status");
    assert_eq!(status.commit_t, 4);
    assert_eq!(status.index_t, 4);

    // Sanity: load ledger and query at head
    let loaded = fluree.ledger(&a).await.expect("ledger load");
    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?name"],
        "where": {"@id":"?s","ex:name":"?name"}
    });
    let result = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(jsonld.as_array().expect("array").len(), 4);
}

/// reindex-basic-test
/// Verifies that reindex populates statistics (properties and classes)
#[tokio::test]
async fn reindex_populates_statistics() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-stats");

    // Create some structured data with types
    let db0 = LedgerSnapshot::genesis(&a);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));

    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };

    // Insert people with types
    let tx1 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice",
                "ex:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:name": "Bob",
                "ex:age": 25
            }
        ]
    });
    ledger = fluree
        .insert_with_opts(
            ledger,
            &tx1,
            Default::default(),
            Default::default(),
            &idx_cfg,
        )
        .await
        .expect("insert tx1")
        .ledger;

    // Add another person
    let tx2 = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:charlie",
        "@type": "ex:Person",
        "ex:name": "Charlie",
        "ex:age": 35
    });
    ledger = fluree
        .insert_with_opts(
            ledger,
            &tx2,
            Default::default(),
            Default::default(),
            &idx_cfg,
        )
        .await
        .expect("insert tx2")
        .ledger;

    assert_eq!(ledger.t(), 2, "Should be at t=2");

    // Before reindex: no index exists, so stats may be None
    let status_before = fluree.index_status(&a).await.expect("index_status");
    assert_eq!(status_before.index_t, 0, "No index before reindex");

    // Reindex
    let r = fluree
        .reindex(&a, ReindexOptions::default())
        .await
        .expect("reindex");
    assert_eq!(r.index_t, 2, "Should index to t=2");

    // After reindex: load the db and verify stats exist
    let loaded = fluree.ledger(&a).await.expect("ledger load");
    let stats = loaded
        .snapshot
        .stats
        .as_ref()
        .expect("db.stats should be Some after reindex");

    // Verify per-graph property stats exist (produced by IdStatsHook)
    assert!(stats.graphs.is_some(), "Should have per-graph statistics");
    let graphs = stats.graphs.as_ref().unwrap();
    assert!(!graphs.is_empty(), "Should have non-empty graph statistics");
    // Each graph entry should have property stats
    for g in graphs {
        assert!(
            !g.properties.is_empty(),
            "Graph {} should have property stats",
            g.g_id
        );
    }

    // Query should still work
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
    });
    let result = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld.as_array().expect("array").len(),
        3,
        "Should return 3 people"
    );
}

/// reindex-with-existing-index-test
/// Verifies that reindex works correctly when an existing index exists.
///
/// **Intentional divergence**: In Rust, indices are content-addressed.
/// If the same data is indexed with the same configuration, it produces the same
/// content hash. This is actually beneficial for deduplication - identical data
/// yields identical indices. Other implementations may produce different addresses due to
/// non-deterministic serialization or metadata differences.
#[tokio::test]
async fn reindex_with_existing_index_completes_successfully() {
    assert_index_defaults();
    let mut fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-existing");

    // Enable background indexer to create initial index
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    local
        .run_until(async move {
            // Create and seed the ledger
            let _ledger = seed_some_commits(&fluree, &a, 3).await;

            // Trigger initial indexing
            let initial = fluree
                .trigger_index(&a, TriggerIndexOptions::default().with_timeout(30_000))
                .await
                .expect("initial trigger_index");
            assert_eq!(initial.index_t, 3);
            let old_root_id = initial.root_id.clone();
            assert!(old_root_id.is_some(), "Should have initial root_id");

            // Verify index exists
            let status = fluree.index_status(&a).await.expect("index_status");
            assert_eq!(status.index_t, 3, "Index should be at t=3");

            // Now reindex - rebuilds from commit history
            let reindexed = fluree
                .reindex(&a, ReindexOptions::default())
                .await
                .expect("reindex");
            assert_eq!(reindexed.index_t, 3, "Reindex should still be at t=3");
            assert!(
                reindexed.root_id.digest_hex().len() == 64,
                "Should have valid root_id"
            );

            // NOTE: Content-addressed storage means identical data produces identical hashes.
            // This is an intentional divergence - in Rust, identical indices
            // at the same t with the same data will have the same content address.
            // The key verification is that reindex completed successfully.

            // Verify the nameservice record reflects the reindex
            let status_after = fluree
                .index_status(&a)
                .await
                .expect("index_status after reindex");
            assert_eq!(status_after.index_t, 3);

            // Query should work with the index
            let loaded = fluree.ledger(&a).await.expect("ledger load");
            let q = json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["?name"],
                "where": {"@id": "?s", "ex:name": "?name"}
            });
            let result = support::query_jsonld(&fluree, &loaded, &q)
                .await
                .expect("query");
            let jsonld = result.to_jsonld(&loaded.snapshot).expect("to_jsonld");
            assert_eq!(jsonld.as_array().expect("array").len(), 3);

            // Verify stats exist after reindex
            assert!(
                loaded.snapshot.stats.is_some(),
                "Should have stats after reindex"
            );
        })
        .await;
}

/// reindex-preserves-queries-test
/// Verifies that queries with filters work after reindex
#[tokio::test]
async fn reindex_preserves_filter_queries() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-filters");

    // Create ledger with salary data
    let db0 = LedgerSnapshot::genesis(&a);
    let ledger = LedgerState::new(db0, Novelty::new(0));

    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };

    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {
                "@id": "ex:emp1",
                "@type": "ex:Employee",
                "ex:name": "Alice",
                "ex:salary": 75000
            },
            {
                "@id": "ex:emp2",
                "@type": "ex:Employee",
                "ex:name": "Bob",
                "ex:salary": 65000
            }
        ]
    });
    let _ledger = fluree
        .insert_with_opts(
            ledger,
            &tx,
            Default::default(),
            Default::default(),
            &idx_cfg,
        )
        .await
        .expect("insert")
        .ledger;

    // Reindex
    let r = fluree
        .reindex(&a, ReindexOptions::default())
        .await
        .expect("reindex");
    assert_eq!(r.index_t, 1);

    let loaded = fluree.ledger(&a).await.expect("ledger load");

    // Basic select query
    let q1 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": {"@id": "?emp", "@type": "ex:Employee", "ex:name": "?name"}
    });
    let result1 = support::query_jsonld(&fluree, &loaded, &q1)
        .await
        .expect("query");
    let jsonld1 = result1.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld1.as_array().expect("array").len(),
        2,
        "Should return 2 employees"
    );

    // Query with filter (salary > 70000)
    // Note: Rust filter syntax uses ["filter", "(expr)"] array form
    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": [
            {"@id": "?emp", "@type": "ex:Employee", "ex:name": "?name", "ex:salary": "?salary"},
            ["filter", "(> ?salary 70000)"]
        ]
    });
    let result2 = support::query_jsonld(&fluree, &loaded, &q2)
        .await
        .expect("filter query");
    let jsonld2 = result2.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld2.as_array().expect("array").len(),
        1,
        "Should return 1 employee with salary > 70000"
    );
}

/// Verifies that reindex uses provided IndexerConfig
/// (Equivalent of batch-bytes affecting index structure)
#[tokio::test]
async fn reindex_uses_provided_indexer_config() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-config");

    let db0 = LedgerSnapshot::genesis(&a);
    let ledger = LedgerState::new(db0, Novelty::new(0));

    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };

    // Insert some data
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {"@id": "ex:a", "@type": "ex:Thing", "ex:val": 1},
            {"@id": "ex:b", "@type": "ex:Thing", "ex:val": 2},
            {"@id": "ex:c", "@type": "ex:Thing", "ex:val": 3}
        ]
    });
    let _ledger = fluree
        .insert_with_opts(
            ledger,
            &tx,
            Default::default(),
            Default::default(),
            &idx_cfg,
        )
        .await
        .expect("insert")
        .ledger;

    // Reindex with custom IndexerConfig (using small() preset)
    let custom_config = fluree_db_indexer::IndexerConfig::small();
    let r = fluree
        .reindex(
            &a,
            ReindexOptions::default().with_indexer_config(custom_config),
        )
        .await
        .expect("reindex with custom config");

    assert_eq!(r.index_t, 1, "Should index to t=1");
    assert!(
        r.root_id.digest_hex().len() == 64,
        "Should have valid root_id"
    );

    // Verify stats show the index was built
    assert!(r.stats.flake_count > 0, "Should have indexed flakes");

    // Query should work
    let loaded = fluree.ledger(&a).await.expect("ledger load");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?val"],
        "where": {"@id": "?s", "@type": "ex:Thing", "ex:val": "?val"}
    });
    let result = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld.as_array().expect("array").len(),
        3,
        "Should return 3 things"
    );
}

/// reindex-from-t-test
/// Verifies default from_t behavior (starts from t=1)
#[tokio::test]
async fn reindex_default_from_t_includes_all_data() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let a = admin_alias("reindex-from-t");

    let db0 = LedgerSnapshot::genesis(&a);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));

    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };

    // Insert 3 transactions
    for i in 0..3 {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:item{}", i),
            "@type": "ex:Item",
            "ex:label": format!("Item {}", i)
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &idx_cfg,
            )
            .await
            .unwrap_or_else(|_| panic!("insert tx{i}"))
            .ledger;
    }
    assert_eq!(ledger.t(), 3, "Should be at t=3 after 3 transactions");

    // Reindex with default options (from_t defaults to 1)
    let r = fluree
        .reindex(&a, ReindexOptions::default())
        .await
        .expect("reindex");
    assert_eq!(r.index_t, 3, "Should index to t=3");

    // Query should return all 3 items
    let loaded = fluree.ledger(&a).await.expect("ledger load");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?label"],
        "where": {"@id": "?item", "@type": "ex:Item", "ex:label": "?label"}
    });
    let result = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    assert_eq!(
        jsonld.as_array().expect("array").len(),
        3,
        "Should have all 3 items"
    );
}

/// Graph crawl select (`{"?s": ["*"]}`) must work against an indexed ledger.
///
/// Binary scan operators produce `EncodedSid` bindings for late materialization.
/// The graph crawl formatter must materialize these before subject property
/// lookup, otherwise every row is silently skipped and the result is `[]`.
#[tokio::test]
async fn graph_crawl_select_works_after_indexing() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-crawl-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Insert a few entities
            for i in 0..3 {
                let tx = json!({
                    "@context": { "ex":"http://example.org/" },
                    "@id": format!("ex:person{i}"),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {i}"),
                    "ex:age": 20 + i
                });

                let r = fluree
                    .insert_with_opts(
                        ledger,
                        &tx,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert_with_opts");
                ledger = r.ledger;
            }

            // Trigger indexing
            let record = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ns record");
            let completion = handle.trigger(ledger_id, record.commit_t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Load indexed ledger
            let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
            assert!(
                loaded.binary_store.is_some(),
                "loaded ledger should have binary index store"
            );

            // Graph crawl select: {"?s": ["*"]}
            let query = json!({
                "@context": { "ex":"http://example.org/" },
                "select": {"?s": ["*"]},
                "where": { "@id": "?s", "@type": "ex:Person" }
            });
            let result = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("query");
            let json_rows = result
                .to_jsonld_async(loaded.as_graph_db_ref(0))
                .await
                .expect("jsonld");
            let rows = json_rows.as_array().expect("should be array");

            assert_eq!(
                rows.len(),
                3,
                "graph crawl should return 3 persons, got: {json_rows}"
            );

            // Each row should be a JSON object with @id and properties
            for row in rows {
                assert!(row.is_object(), "each row should be a JSON object");
                assert!(row.get("@id").is_some(), "each row should have @id: {row}");
            }

            // Also test explicit property select: {"?s": ["@id", "ex:name"]}
            let query2 = json!({
                "@context": { "ex":"http://example.org/" },
                "select": {"?s": ["@id", "ex:name"]},
                "where": { "@id": "?s", "@type": "ex:Person" }
            });
            let result2 = support::query_jsonld(&fluree, &loaded, &query2)
                .await
                .expect("query2");
            let json_rows2 = result2
                .to_jsonld_async(loaded.as_graph_db_ref(0))
                .await
                .expect("jsonld2");
            let rows2 = json_rows2.as_array().expect("should be array");

            assert_eq!(
                rows2.len(),
                3,
                "explicit property select should return 3 persons, got: {json_rows2}"
            );

            for row in rows2 {
                assert!(row.is_object(), "each row should be a JSON object");
                assert!(row.get("@id").is_some(), "should have @id: {row}");
                assert!(row.get("ex:name").is_some(), "should have ex:name: {row}");
            }
        })
        .await;
}

/// CONSTRUCT queries must work against an indexed ledger.
///
/// When the binary index is active, scan operators may produce `EncodedSid` bindings.
/// The CONSTRUCT formatter must materialize these for subject/predicate positions,
/// otherwise it can silently omit triples.
#[tokio::test]
async fn construct_works_after_indexing() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/construct-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            for i in 0..3 {
                let tx = json!({
                    "@context": { "ex":"http://example.org/" },
                    "@id": format!("ex:person{i}"),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {i}")
                });
                let r = fluree
                    .insert_with_opts(
                        ledger,
                        &tx,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &index_cfg,
                    )
                    .await
                    .expect("insert_with_opts");
                ledger = r.ledger;
            }

            let record = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ns record");
            let completion = handle.trigger(ledger_id, record.commit_t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
            assert!(
                loaded.binary_store.is_some(),
                "loaded ledger should have binary index store"
            );

            let query = json!({
                "@context": { "ex":"http://example.org/" },
                "where": { "@id": "?s", "@type": "ex:Person", "ex:name": "?name" },
                "construct": [{ "@id": "?s", "ex:name": "?name" }]
            });

            let result = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("query");
            let constructed = result.to_construct(&loaded.snapshot).expect("to_construct");

            let graph = constructed
                .get("@graph")
                .and_then(|v| v.as_array())
                .expect("@graph array");
            assert_eq!(
                graph.len(),
                3,
                "expected 3 constructed nodes, got: {constructed}"
            );
        })
        .await;
}

#[tokio::test]
async fn new_namespace_after_indexing_is_queryable() {
    // Regression: when a transaction introduces a new namespace code that wasn't
    // present in the index root, queries should still resolve IRIs and format
    // results correctly. The DictOverlay delegates namespace_prefix() to the
    // BinaryIndexStore, which only knows about namespaces from the index root.
    // New namespace codes from novelty must also be available.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path)
        .build()
        .expect("build file fluree");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger_id = "it/new-ns-after-index:main";
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Step 1: Insert data with namespace "ex:" and trigger indexing.
            let tx1 = json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice"
            });
            let r1 = fluree
                .insert_with_opts(
                    ledger0,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert initial data");

            // Trigger indexing and wait — index root will contain namespace codes
            // for the built-in namespaces plus "http://example.org/" but NOT
            // "http://newprefix.org/".
            let completion = handle.trigger(ledger_id, r1.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Step 2: Insert data using a BRAND NEW namespace "np:" that was NOT
            // in the index. This creates a new namespace code in novelty.
            let tx2 = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "np": "http://newprefix.org/"
                },
                "@id": "np:bob",
                "@type": "ex:Person",
                "np:label": "Bob from new prefix"
            });
            // After indexing, reload the ledger so the second insert builds
            // on top of the indexed state (just like production code would).
            let post_index = fluree.ledger(ledger_id).await.expect("load post-index");
            assert!(
                post_index.binary_store.is_some(),
                "post-index state should have binary store"
            );

            let r2 = fluree
                .insert_with_opts(
                    post_index,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert with new namespace");
            assert_eq!(r2.receipt.t, 2);

            // Step 3: Reload the ledger (picks up the index + novelty overlay).
            let loaded = fluree.ledger(ledger_id).await.expect("ledger load");
            assert_eq!(
                loaded.t(),
                r2.receipt.t,
                "loaded ledger t() should be at latest commit t"
            );
            assert_eq!(
                loaded.snapshot.t, 1,
                "index time should still be 1 (only first commit was indexed)"
            );

            // Verify LedgerSnapshot namespace codes has the new prefix.
            assert!(
                loaded
                    .snapshot
                    .namespaces()
                    .values()
                    .any(|p| p == "http://newprefix.org/"),
                "LedgerSnapshot namespace codes should include the new prefix from novelty"
            );

            // Step 4: Query that forces resolution of a subject IRI using the new
            // namespace — this is where the bug manifested.
            //
            // Before the fix, DictOverlay.resolve_subject_iri() would fail with
            // "namespace code 13 not in index root" because the BinaryIndexStore
            // didn't know about namespace codes introduced after the last index build.
            //
            // Querying for all ex:Person subjects forces the result formatter to
            // resolve both "ex:alice" (in the index) and "np:bob" (in novelty,
            // with the new namespace code). Without the fix, resolving "np:bob"
            // would fail because its namespace code wasn't in the store.
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "np": "http://newprefix.org/"
                },
                "select": ["?s"],
                "where": { "@id": "?s", "@type": "ex:Person" }
            });
            let result = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("query with new ns");
            let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld format");
            let rows = normalize_rows(&json_rows);
            // Both Alice (from index) and Bob (from novelty with new namespace)
            // should be returned with properly resolved IRIs.
            assert_eq!(rows.len(), 2, "should find both Alice and Bob");
        })
        .await;
}
