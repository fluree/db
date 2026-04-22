//! Time travel + indexing integration tests
//!
//! Tests time travel queries (`@t:N`, `@iso:`, `@commit:`) across all 3 indexing scenarios:
//!
//! - Scenario (a): No index - all data in novelty only
//! - Scenario (b): Index current - index covers latest t
//! - Scenario (c): Index + novelty - index is behind, novelty has newer data
//!
//! This ensures time travel works correctly regardless of how data is stored.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    assert_index_defaults, genesis_ledger_for_fluree, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

type MemoryFluree = fluree_db_api::Fluree;
type MemoryLedger = LedgerState;

// =============================================================================
// Shared test setup
// =============================================================================

/// Insert 3 transactions with predictable data:
/// - t=1: Alice (age 30)
/// - t=2: Bob (age 25)
/// - t=3: Carol (age 28)
async fn seed_test_data(
    fluree: &MemoryFluree,
    ledger_id: &str,
    index_cfg: &IndexConfig,
) -> MemoryLedger {
    let mut ledger = genesis_ledger_for_fluree(fluree, ledger_id);

    let txns = [
        json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice",
            "@type": "ex:Person",
            "ex:name": "Alice",
            "ex:age": 30
        }),
        json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:bob",
            "@type": "ex:Person",
            "ex:name": "Bob",
            "ex:age": 25
        }),
        json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:carol",
            "@type": "ex:Person",
            "ex:name": "Carol",
            "ex:age": 28
        }),
    ];

    for tx in txns {
        let result = fluree
            .insert_with_opts(
                ledger,
                &tx,
                TxnOpts::default(),
                CommitOpts::default(),
                index_cfg,
            )
            .await
            .expect("insert");
        ledger = result.ledger;
    }

    assert_eq!(ledger.t(), 3, "should be at t=3 after seeding");
    ledger
}

/// Query names at a specific time using query_connection
async fn query_names_at(fluree: &MemoryFluree, from_spec: &str) -> Vec<String> {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": from_spec,
        "select": ["?name"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"},
        "orderBy": "?name"
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");

    // Load the ledger to get LedgerSnapshot for formatting (strip any @t: suffix and #fragment)
    let ledger_id = from_spec
        .split('@')
        .next()
        .unwrap_or(from_spec)
        .split('#')
        .next()
        .unwrap_or(from_spec);
    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("ledger for formatting");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    jsonld
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
}

// =============================================================================
// Scenario (a): No index - all data in novelty
// =============================================================================
// This is the existing behavior tested in it_query_time_travel.rs

#[tokio::test]
async fn time_travel_no_index_via_novelty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-no-index:main";

    // No indexer started - all data stays in novelty
    let index_cfg = IndexConfig {
        reindex_min_bytes: 1_000_000_000, // Very high threshold to prevent auto-indexing
        reindex_max_bytes: 10_000_000,
    };

    let _ledger = seed_test_data(&fluree, ledger_id, &index_cfg).await;

    // Verify no index exists
    let status = fluree.index_status(ledger_id).await.expect("index_status");
    assert_eq!(status.index_t, 0, "should have no index");
    assert_eq!(status.commit_t, 3, "should have commits");

    // Time travel queries should work via novelty
    let names_t1 = query_names_at(&fluree, &format!("{ledger_id}@t:1")).await;
    assert_eq!(names_t1, vec!["Alice"], "t=1 should have Alice only");

    let names_t2 = query_names_at(&fluree, &format!("{ledger_id}@t:2")).await;
    assert_eq!(names_t2, vec!["Alice", "Bob"], "t=2 should have Alice, Bob");

    let names_t3 = query_names_at(&fluree, &format!("{ledger_id}@t:3")).await;
    assert_eq!(
        names_t3,
        vec!["Alice", "Bob", "Carol"],
        "t=3 should have all three"
    );

    // Current query (no time spec) should return all
    let names_current = query_names_at(&fluree, ledger_id).await;
    assert_eq!(
        names_current,
        vec!["Alice", "Bob", "Carol"],
        "current should have all three"
    );
}

// =============================================================================
// Scenario (b): Index current - index covers latest t
// =============================================================================

#[tokio::test]
async fn time_travel_index_current() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-index-current:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = seed_test_data(&fluree, ledger_id, &index_cfg).await;

            // Trigger indexing to latest t and wait
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 3, "should index to t=3");
            }

            // Verify index is current
            let status = fluree.index_status(ledger_id).await.expect("index_status");
            assert_eq!(status.index_t, 3, "index should be at t=3");
            assert_eq!(status.commit_t, 3, "commit should be at t=3");

            // Time travel queries should work via indexed data
            let names_t1 = query_names_at(&fluree, &format!("{ledger_id}@t:1")).await;
            assert_eq!(
                names_t1,
                vec!["Alice"],
                "t=1 should have Alice only (indexed)"
            );

            let names_t2 = query_names_at(&fluree, &format!("{ledger_id}@t:2")).await;
            assert_eq!(
                names_t2,
                vec!["Alice", "Bob"],
                "t=2 should have Alice, Bob (indexed)"
            );

            let names_t3 = query_names_at(&fluree, &format!("{ledger_id}@t:3")).await;
            assert_eq!(
                names_t3,
                vec!["Alice", "Bob", "Carol"],
                "t=3 should have all three (indexed)"
            );

            // Current query should return all
            let names_current = query_names_at(&fluree, ledger_id).await;
            assert_eq!(
                names_current,
                vec!["Alice", "Bob", "Carol"],
                "current should have all three"
            );
        })
        .await;
}

// =============================================================================
// Scenario (c): Index + novelty - index is behind, novelty has newer data
// =============================================================================

#[tokio::test]
async fn time_travel_index_plus_novelty() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-index-novelty:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert first 2 transactions
            let tx1 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice",
                "ex:age": 30
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx1");
            ledger = result.ledger;
            eprintln!("After tx1: t={}", ledger.t());

            let tx2 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:name": "Bob",
                "ex:age": 25
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx2");
            ledger = result.ledger;
            eprintln!("After tx2: t={}", ledger.t());

            // Check nameservice before indexing
            let ns_record_before = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ns record");
            eprintln!(
                "Before indexing: commit_t={}, index_t={}",
                ns_record_before.commit_t, ns_record_before.index_t
            );

            // Index at t=2
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, 2).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 2, "should index to t=2");
            }

            // Now insert third transaction (this will be in novelty only)
            let tx3 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:carol",
                "@type": "ex:Person",
                "ex:name": "Carol",
                "ex:age": 28
            });
            let result3 = fluree
                .insert_with_opts(
                    ledger,
                    &tx3,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx3");
            eprintln!("After tx3: t={}", result3.ledger.t());

            // Check nameservice after tx3
            let ns_record_after = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ns record");
            eprintln!(
                "After tx3: commit_t={}, index_t={}, commit_addr={:?}",
                ns_record_after.commit_t, ns_record_after.index_t, ns_record_after.commit_head_id
            );

            // Verify index is at t=2, commits at t=3
            let status = fluree.index_status(ledger_id).await.expect("index_status");
            assert_eq!(status.index_t, 2, "index should be at t=2");
            assert_eq!(status.commit_t, 3, "commit should be at t=3");

            // Time travel queries:
            // t=1 and t=2 should come from index
            // t=3 should come from index + novelty overlay
            let names_t1 = query_names_at(&fluree, &format!("{ledger_id}@t:1")).await;
            assert_eq!(
                names_t1,
                vec!["Alice"],
                "t=1 should have Alice only (from index)"
            );

            let names_t2 = query_names_at(&fluree, &format!("{ledger_id}@t:2")).await;
            assert_eq!(
                names_t2,
                vec!["Alice", "Bob"],
                "t=2 should have Alice, Bob (from index)"
            );

            let names_t3 = query_names_at(&fluree, &format!("{ledger_id}@t:3")).await;
            assert_eq!(
                names_t3,
                vec!["Alice", "Bob", "Carol"],
                "t=3 should have all three (index + novelty)"
            );

            // Current query should return all
            let names_current = query_names_at(&fluree, ledger_id).await;
            assert_eq!(
                names_current,
                vec!["Alice", "Bob", "Carol"],
                "current should have all three"
            );
        })
        .await;
}

// =============================================================================
// Edge cases: Updates across scenarios
// =============================================================================

#[tokio::test]
async fn time_travel_updates_across_index_novelty_boundary() {
    // Test that updates work correctly when some data is indexed and some is in novelty
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-update-boundary:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            // t=1: Insert Alice with age 30
            let tx1 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice",
                "ex:age": 30
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx1");
            ledger = result.ledger;

            // Index at t=1
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, 1).await;

            // t=2: Update Alice's age to 31 (will be in novelty)
            // Use upsert to properly retract old age and assert new age
            let tx2 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:age": 31
            });
            let _result = fluree
                .upsert_with_opts(
                    ledger,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx2");

            // Verify index is at t=1, commits at t=2
            let status = fluree.index_status(ledger_id).await.expect("index_status");
            assert_eq!(status.index_t, 1, "index should be at t=1");
            assert_eq!(status.commit_t, 2, "commit should be at t=2");

            // Query age at t=1 (from index) - should be 30
            let query_t1 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": format!("{ledger_id}@t:1"),
                "select": ["?age"],
                "where": {"@id": "ex:alice", "ex:age": "?age"}
            });
            let result = fluree.query_connection(&query_t1).await.expect("query t=1");
            let ledger_for_fmt = fluree.ledger(ledger_id).await.expect("ledger");
            let jsonld = result
                .to_jsonld(&ledger_for_fmt.snapshot)
                .expect("to_jsonld");
            let ages: Vec<i64> = jsonld
                .as_array()
                .expect("array")
                .iter()
                .filter_map(serde_json::Value::as_i64)
                .collect();
            assert_eq!(ages, vec![30], "at t=1, Alice should be age 30");

            // Query age at t=2 (from index + novelty) - should be 31
            let query_t2 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": format!("{ledger_id}@t:2"),
                "select": ["?age"],
                "where": {"@id": "ex:alice", "ex:age": "?age"}
            });
            let result = fluree.query_connection(&query_t2).await.expect("query t=2");
            let jsonld = result
                .to_jsonld(&ledger_for_fmt.snapshot)
                .expect("to_jsonld");
            let ages: Vec<i64> = jsonld
                .as_array()
                .expect("array")
                .iter()
                .filter_map(serde_json::Value::as_i64)
                .collect();
            assert_eq!(ages, vec![31], "at t=2, Alice should be age 31");

            // Query current - should be 31
            let query_current = json!({
                "@context": {"ex": "http://example.org/"},
                "from": ledger_id,
                "select": ["?age"],
                "where": {"@id": "ex:alice", "ex:age": "?age"}
            });
            let result = fluree
                .query_connection(&query_current)
                .await
                .expect("query current");
            let jsonld = result
                .to_jsonld(&ledger_for_fmt.snapshot)
                .expect("to_jsonld");
            let ages: Vec<i64> = jsonld
                .as_array()
                .expect("array")
                .iter()
                .filter_map(serde_json::Value::as_i64)
                .collect();
            assert_eq!(ages, vec![31], "current Alice should be age 31");
        })
        .await;
}

#[tokio::test]
async fn time_travel_retraction_across_index_novelty_boundary() {
    // Test that retractions work correctly when indexed data is retracted in novelty
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-retract-boundary:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            // t=1: Insert Alice and Bob
            let tx1 = json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx1");
            ledger = result.ledger;

            // Index at t=1
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, 1).await;

            // t=2: Delete Bob (will be in novelty)
            let tx2 = json!({
                "@context": {"ex": "http://example.org/"},
                "delete": {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
            });
            let _result = fluree.update(ledger, &tx2).await.expect("tx2");

            // Verify index is at t=1, commits at t=2
            let status = fluree.index_status(ledger_id).await.expect("index_status");
            assert_eq!(status.index_t, 1, "index should be at t=1");
            assert_eq!(status.commit_t, 2, "commit should be at t=2");

            // Query at t=1 (from index) - should have both Alice and Bob
            let names_t1 = query_names_at(&fluree, &format!("{ledger_id}@t:1")).await;
            assert_eq!(
                names_t1,
                vec!["Alice", "Bob"],
                "t=1 should have both (from index)"
            );

            // Query at t=2 (from index + novelty) - should have only Alice
            let names_t2 = query_names_at(&fluree, &format!("{ledger_id}@t:2")).await;
            assert_eq!(
                names_t2,
                vec!["Alice"],
                "t=2 should have only Alice (Bob retracted in novelty)"
            );

            // Query current - should have only Alice
            let names_current = query_names_at(&fluree, ledger_id).await;
            assert_eq!(
                names_current,
                vec!["Alice"],
                "current should have only Alice"
            );
        })
        .await;
}

// =============================================================================
// Scenario comparisons - verify identical results across all scenarios
// =============================================================================

#[tokio::test]
async fn time_travel_consistent_results_across_scenarios() {
    // This test verifies that the same query returns identical results
    // whether data is in novelty, indexed, or both
    assert_index_defaults();

    // We'll create 3 ledgers with identical data but different indexing states
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg_low = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let index_cfg_high = IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 10_000_000,
            };

            // Ledger A: No indexing
            let ledger_id_a = "it/tt-compare-no-index:main";
            let _ledger_a = seed_test_data(&fluree, ledger_id_a, &index_cfg_high).await;

            // Ledger B: Fully indexed
            let ledger_id_b = "it/tt-compare-indexed:main";
            let ledger_b = seed_test_data(&fluree, ledger_id_b, &index_cfg_low).await;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id_b, ledger_b.t()).await;

            // Verify states
            let status_a = fluree.index_status(ledger_id_a).await.expect("status A");
            let status_b = fluree.index_status(ledger_id_b).await.expect("status B");
            assert_eq!(status_a.index_t, 0, "A should have no index");
            assert_eq!(status_b.index_t, 3, "B should be fully indexed");

            // Query all time points and compare
            for t in 1..=3 {
                let names_a = query_names_at(&fluree, &format!("{ledger_id_a}@t:{t}")).await;
                let names_b = query_names_at(&fluree, &format!("{ledger_id_b}@t:{t}")).await;

                assert_eq!(
                    names_a, names_b,
                    "Results at t={t} should be identical: novelty={names_a:?} indexed={names_b:?}"
                );
            }

            // Query current state
            let names_a_current = query_names_at(&fluree, ledger_id_a).await;
            let names_b_current = query_names_at(&fluree, ledger_id_b).await;
            assert_eq!(
                names_a_current, names_b_current,
                "Current results should be identical"
            );
        })
        .await;
}

// =============================================================================
// Multi-leaf overlay duplicate regression test
// =============================================================================

/// Query all person ages at a specific time, returning (name, age) pairs
async fn query_all_person_ages(fluree: &MemoryFluree, from_spec: &str) -> Vec<(String, i64)> {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": from_spec,
        "select": ["?name", "?age"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name", "ex:age": "?age"}
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");

    let ledger_id = from_spec.split('@').next().unwrap_or(from_spec);
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    jsonld
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|row| {
            let arr = row.as_array()?;
            let name = arr.first()?.as_str()?.to_string();
            let age = arr.get(1)?.as_i64()?;
            Some((name, age))
        })
        .collect()
}

/// Query a specific person's age
async fn query_person_age(fluree: &MemoryFluree, from_spec: &str, person_id: &str) -> Vec<i64> {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": from_spec,
        "select": ["?age"],
        "where": {"@id": person_id, "ex:age": "?age"}
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");

    let ledger_id = from_spec.split('@').next().unwrap_or(from_spec);
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    jsonld
        .as_array()
        .expect("array")
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect()
}

/// Test that overlay ops are not double-emitted when:
/// 1. Overlay ops are merged with one leaf and return results
/// 2. Subsequent leaves have no overlay (go through normal path)
/// 3. The overlay-only path incorrectly re-emits the same ops
///
/// This test creates many subjects to increase the chance of spanning multiple
/// leaves, then adds an overlay update and checks for duplicate results.
#[tokio::test]
async fn time_travel_no_duplicate_overlay_emission() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/tt-multi-leaf-overlay:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let db0 = LedgerSnapshot::genesis(ledger_id);
            let mut ledger = LedgerState::new(db0, Novelty::new(0));

            // t=1: Insert many subjects to potentially span multiple leaves
            // Each subject has a unique name and sequential ID
            let mut subjects = Vec::new();
            for i in 0..100 {
                subjects.push(json!({
                    "@id": format!("ex:person{}", i),
                    "@type": "ex:Person",
                    "ex:name": format!("Person{:03}", i),
                    "ex:age": 20 + (i % 50)
                }));
            }
            let tx1 = json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": subjects
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &tx1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx1");
            ledger = result.ledger;

            // Index at t=1
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, 1).await;

            // t=2: Update person0's age (will be in novelty)
            // Use upsert to create retraction + assertion
            let tx2 = json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:person0",
                "ex:age": 99
            });
            let _result = fluree
                .upsert_with_opts(
                    ledger,
                    &tx2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("tx2");

            // Verify index is at t=1, commits at t=2
            let status = fluree.index_status(ledger_id).await.expect("index_status");
            assert_eq!(status.index_t, 1, "index should be at t=1");
            assert_eq!(status.commit_t, 2, "commit should be at t=2");

            // Query all ages at t=2 - should have exactly 100 results (one per person)
            // If there's duplicate emission, we might get 101+ results
            let results = query_all_person_ages(&fluree, &format!("{ledger_id}@t:2")).await;

            // Count unique names to detect duplicates
            let mut names: Vec<String> = results.iter().map(|(n, _)| n.clone()).collect();
            names.sort();

            // Check for duplicates
            let mut seen = std::collections::HashSet::new();
            let mut duplicates = Vec::new();
            for name in &names {
                if !seen.insert(name.clone()) {
                    duplicates.push(name.clone());
                }
            }

            assert!(
                duplicates.is_empty(),
                "Found duplicate names (indicates double-emission bug): {duplicates:?}"
            );

            assert_eq!(
                names.len(),
                100,
                "Should have exactly 100 results, got {}",
                names.len()
            );

            // Verify person0's age is 99 (the updated value)
            let ages = query_person_age(&fluree, &format!("{ledger_id}@t:2"), "ex:person0").await;
            assert_eq!(ages, vec![99], "person0 should have age 99, got {ages:?}");
        })
        .await;
}
