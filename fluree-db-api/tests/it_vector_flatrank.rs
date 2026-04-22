#![cfg(feature = "vector")]
//! Vector flatrank integration tests
//
//! Tests vector search functionality with dot product, cosine similarity,
//! and euclidean distance scoring functions.
//!
//! ## Post-indexing tests
//!
//! The `vector_search_post_indexing_*` tests exercise the binary index path:
//! transact → index build → query from arena (not novelty).

use std::sync::Arc;
mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Integration test for basic vector search with dot product scoring
#[tokio::test]
async fn vector_search_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-score:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data with vectors
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"},
                "ex:age": 36
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"},
                "ex:age": "forever 10"
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Test query with dot product scoring
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Expected: [["ex:bart" 0.61 [0.1, 0.9]], ["ex:homer" 0.72 [0.6, 0.5]]]
    // Sort by score for consistent comparison
    let mut results: Vec<(String, f64, Vec<f64>)> = arr
        .iter()
        .map(|row| {
            let row_arr = row.as_array().unwrap();
            let id = row_arr[0].as_str().unwrap().to_string();
            let score = row_arr[1].as_f64().unwrap();
            let vec = row_arr[2]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect::<Vec<f64>>();
            (id, score, vec)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap()); // Sort by score descending

    assert_eq!(results[0].0, "ex:homer");
    assert!((results[0].1 - 0.72).abs() < 0.001);
    // @vector is f32 storage; returned values are f32-quantized.
    assert_eq!(results[0].2, vec![0.6f32 as f64, 0.5f32 as f64]);

    assert_eq!(results[1].0, "ex:bart");
    assert!((results[1].1 - 0.61).abs() < 0.001);
    assert_eq!(results[1].2, vec![0.1f32 as f64, 0.9f32 as f64]);
}

/// Test filtering results based on other properties
#[tokio::test]
async fn vector_search_with_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-score-filter:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"},
                "ex:age": 36
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"},
                "ex:age": "forever 10"
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with age filter
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:age": 36, "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should return only Homer (age 36)");

    let row = &arr[0];
    let row_arr = row.as_array().unwrap();
    assert_eq!(row_arr[0], "ex:homer");
    assert!((row_arr[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}

/// Test applying filters to score values
#[tokio::test]
async fn vector_search_score_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-score-threshold:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with score threshold filter
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]],
            ["filter", [">", "?score", 0.7]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should return only results with score > 0.7");

    let row = &arr[0];
    let row_arr = row.as_array().unwrap();
    assert_eq!(row_arr[0], "ex:homer");
    assert!(row_arr[1].as_f64().unwrap() > 0.7);
}

/// Test multi-cardinality vector values
#[tokio::test]
async fn vector_search_multi_cardinality() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-score-multi:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data with multiple vectors per entity
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": [
                    {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"},
                    {"@value": [0.2, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
                ]
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with dot product scoring - should return multiple results for Bart
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(
        arr.len(),
        3,
        "Should return 3 results (1 for Homer, 2 for Bart)"
    );

    // Expected order by score: [Bart(0.61), Bart(0.68), Homer(0.72)]
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:bart");
    assert!((row0[1].as_f64().unwrap() - 0.61).abs() < 0.001);

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:bart");
    assert!((row1[1].as_f64().unwrap() - 0.68).abs() < 0.001);

    let row2 = arr[2].as_array().unwrap();
    assert_eq!(row2[0], "ex:homer");
    assert!((row2[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}

/// Test cosine similarity scoring
#[tokio::test]
async fn vector_search_cosine_similarity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-cosine:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with cosine similarity
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["cosineSimilarity", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Results should be ordered by cosine similarity
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:bart");

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:homer");
}

/// Test euclidean distance scoring
#[tokio::test]
async fn vector_search_euclidean_distance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-euclidean:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query with euclidean distance
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["euclideanDistance", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2, "Should return 2 results");

    // Results should be ordered by euclidean distance (ascending)
    let row0 = arr[0].as_array().unwrap();
    assert_eq!(row0[0], "ex:homer"); // Homer should be closer

    let row1 = arr[1].as_array().unwrap();
    assert_eq!(row1[0], "ex:bart"); // Bart should be farther
}

/// Test mixed datatypes (vectors and non-vectors)
#[tokio::test]
async fn vector_search_mixed_datatypes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-mixed:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
    ]);

    // Insert test data with mixed datatypes
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
            },
            {
                "@id": "ex:lucy",
                "ex:xVec": "Not a Vector"
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query should handle mixed datatypes gracefully
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score", "?vec"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ],
        "orderBy": "?score"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(
        arr.len(),
        3,
        "Should return 3 results (including non-vector)"
    );

    // Lucy should have null score due to non-vector value
    let lucy_row = arr
        .iter()
        .find(|row| row.as_array().unwrap()[0] == "ex:lucy")
        .unwrap();
    let lucy_arr = lucy_row.as_array().unwrap();
    assert_eq!(lucy_arr[1], serde_json::Value::Null);
    assert_eq!(lucy_arr[2], "Not a Vector");

    // Vector results should be properly scored
    let homer_row = arr
        .iter()
        .find(|row| row.as_array().unwrap()[0] == "ex:homer")
        .unwrap();
    let homer_arr = homer_row.as_array().unwrap();
    assert!((homer_arr[1].as_f64().unwrap() - 0.72).abs() < 0.001);
}

// ============================================================================
// Post-indexing tests (vector arena on binary index path)
// ============================================================================

/// Insert vectors → force index build → query from binary index (arena path).
///
/// Verifies that vectors survive the full round-trip:
/// transact → commit → index build (vector arena shards) → load → query.
#[cfg(feature = "native")]
#[tokio::test]
async fn vector_search_post_indexing() {
    use fluree_db_api::{IndexConfig, LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use support::start_background_indexer_local;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-post-index:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let ctx = json!([
                support::default_context(),
                {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
            ]);

            let insert_txn = json!({
                "@context": ctx,
                "@graph": [
                    {
                        "@id": "ex:homer",
                        "ex:name": "Homer",
                        "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
                    },
                    {
                        "@id": "ex:bart",
                        "ex:name": "Bart",
                        "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
                    }
                ]
            });

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &insert_txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            // Trigger indexing and wait for completion
            let completion = handle.trigger(ledger_id, result.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { index_t, .. } => {
                    assert!(index_t >= result.receipt.t);
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Verify nameservice has index address
            let record = fluree
                .nameservice()
                .lookup(ledger_id)
                .await
                .expect("ns lookup")
                .expect("ns record");
            assert!(
                record.index_head_id.is_some(),
                "expected index id after indexing"
            );

            // Load indexed ledger and query
            let loaded = fluree.ledger(ledger_id).await.expect("load indexed ledger");

            let query = json!({
                "@context": ctx,
                "select": ["?x", "?score", "?vec"],
                "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?x", "ex:xVec": "?vec"},
                    ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
                ]
            });

            let qr = support::query_jsonld(&fluree, &loaded, &query).await.expect("query");
            let rows = qr.to_jsonld(&loaded.snapshot).expect("jsonld");
            let arr = rows.as_array().expect("array");

            assert_eq!(arr.len(), 2, "Should return 2 results from indexed path");

            let mut results: Vec<(String, f64)> = arr
                .iter()
                .map(|row| {
                    let r = row.as_array().unwrap();
                    (r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap())
                })
                .collect();
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            assert_eq!(results[0].0, "ex:homer");
            assert!((results[0].1 - 0.72).abs() < 0.001);
            assert_eq!(results[1].0, "ex:bart");
            assert!((results[1].1 - 0.61).abs() < 0.001);
        })
        .await;
}

/// Insert batch1 → index → insert batch2 (novelty) → query → both batches visible.
///
/// Verifies that novelty vectors and indexed arena vectors are merged correctly
/// in query results.
#[cfg(feature = "native")]
#[tokio::test]
async fn vector_search_novelty_plus_indexed() {
    use fluree_db_api::{IndexConfig, LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use support::start_background_indexer_local;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-novelty-plus-index:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let ctx = json!([
                support::default_context(),
                {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
            ]);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Batch 1: Homer
            let batch1 = json!({
                "@context": ctx,
                "@graph": [{
                    "@id": "ex:homer",
                    "ex:name": "Homer",
                    "ex:xVec": {"@value": [0.6, 0.5], "@type": "https://ns.flur.ee/db#embeddingVector"}
                }]
            });

            let r1 = fluree
                .insert_with_opts(
                    ledger0,
                    &batch1,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("batch1");

            // Index batch 1
            let completion = handle.trigger(ledger_id, r1.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Batch 2: Bart (novelty, not yet indexed)
            let batch2 = json!({
                "@context": ctx,
                "@graph": [{
                    "@id": "ex:bart",
                    "ex:name": "Bart",
                    "ex:xVec": {"@value": [0.1, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}
                }]
            });

            // Load the indexed ledger, then insert batch2 on top
            let indexed_ledger = fluree.ledger(ledger_id).await.expect("load indexed");
            let r2 = fluree
                .insert_with_opts(
                    indexed_ledger,
                    &batch2,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("batch2");

            // Query should see BOTH homer (indexed) and bart (novelty)
            let query = json!({
                "@context": ctx,
                "select": ["?x", "?score"],
                "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?x", "ex:xVec": "?vec"},
                    ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
                ]
            });

            let qr = support::query_jsonld(&fluree, &r2.ledger, &query).await.expect("query");
            let rows = qr.to_jsonld(&r2.ledger.snapshot).expect("jsonld");
            let arr = rows.as_array().expect("array");

            assert_eq!(
                arr.len(),
                2,
                "Should return both indexed and novelty vectors"
            );

            let ids: Vec<&str> = arr
                .iter()
                .map(|r| r.as_array().unwrap()[0].as_str().unwrap())
                .collect();
            assert!(ids.contains(&"ex:homer"), "indexed homer missing");
            assert!(ids.contains(&"ex:bart"), "novelty bart missing");
        })
        .await;
}

/// Transact vectors using `"@type": "@vector"` shorthand and verify behavior
/// is identical to the full IRI.
#[tokio::test]
async fn vector_at_type_shorthand() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-shorthand:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/"}
    ]);

    // Use @vector shorthand instead of full IRI
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "@vector"}
            },
            {
                "@id": "ex:bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "@vector"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query uses full IRI in values clause (query parser doesn't resolve
    // @vector shorthand in VALUES). The key assertion is that data inserted
    // with @vector shorthand is queryable and scores correctly.
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:xVec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(
        arr.len(),
        2,
        "Should return 2 results with @vector shorthand"
    );

    let mut results: Vec<(String, f64)> = arr
        .iter()
        .map(|row| {
            let r = row.as_array().unwrap();
            (r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap())
        })
        .collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    assert_eq!(results[0].0, "ex:homer");
    assert!((results[0].1 - 0.72).abs() < 0.001);
    assert_eq!(results[1].0, "ex:bart");
    assert!((results[1].1 - 0.61).abs() < 0.001);
}

/// Insert unit-normalized vectors → index → query with cosineSimilarity →
/// verify results match dotProduct within epsilon (the cosine→dot optimization).
#[cfg(feature = "native")]
#[tokio::test]
async fn vector_cosine_normalized_optimization() {
    use fluree_db_api::{IndexConfig, LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use support::start_background_indexer_local;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-cosine-norm:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let db0 = LedgerSnapshot::genesis(ledger_id);
            let ledger0 = LedgerState::new(db0, Novelty::new(0));

            let ctx = json!([
                support::default_context(),
                {"ex": "http://example.org/ns/", "fluree": "https://ns.flur.ee/db#"}
            ]);

            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Insert unit-normalized vectors (magnitude = 1.0)
            let inv_sqrt2 = 1.0f64 / 2.0f64.sqrt();
            let insert_txn = json!({
                "@context": ctx,
                "@graph": [
                    {
                        "@id": "ex:a",
                        "ex:xVec": {"@value": [inv_sqrt2, inv_sqrt2], "@type": "https://ns.flur.ee/db#embeddingVector"}
                    },
                    {
                        "@id": "ex:b",
                        "ex:xVec": {"@value": [1.0, 0.0], "@type": "https://ns.flur.ee/db#embeddingVector"}
                    }
                ]
            });

            let r = fluree
                .insert_with_opts(
                    ledger0,
                    &insert_txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");

            // Index
            let completion = handle.trigger(ledger_id, r.receipt.t).await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { .. } => {}
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            let loaded = fluree.ledger(ledger_id).await.expect("load");

            // Query with cosine similarity
            let cosine_query = json!({
                "@context": ctx,
                "select": ["?x", "?cosine"],
                "values": [["?targetVec"], [{"@value": [1.0, 0.0], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?x", "ex:xVec": "?vec"},
                    ["bind", "?cosine", ["cosineSimilarity", "?vec", "?targetVec"]]
                ]
            });

            // Query with dot product
            let dot_query = json!({
                "@context": ctx,
                "select": ["?x", "?dot"],
                "values": [["?targetVec"], [{"@value": [1.0, 0.0], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?x", "ex:xVec": "?vec"},
                    ["bind", "?dot", ["dotProduct", "?vec", "?targetVec"]]
                ]
            });

            let cos_result = support::query_jsonld(&fluree, &loaded, &cosine_query)
                .await
                .expect("cosine query");
            let cos_rows = cos_result.to_jsonld(&loaded.snapshot).expect("jsonld");
            let cos_arr = cos_rows.as_array().expect("array");

            let dot_result = support::query_jsonld(&fluree, &loaded, &dot_query)
                .await
                .expect("dot query");
            let dot_rows = dot_result.to_jsonld(&loaded.snapshot).expect("jsonld");
            let dot_arr = dot_rows.as_array().expect("array");

            assert_eq!(cos_arr.len(), 2);
            assert_eq!(dot_arr.len(), 2);

            // For unit-normalized vectors, cosine ≈ dot product.
            // Collect scores by id for comparison.
            let cos_scores: std::collections::HashMap<&str, f64> = cos_arr
                .iter()
                .map(|r| {
                    let a = r.as_array().unwrap();
                    (a[0].as_str().unwrap(), a[1].as_f64().unwrap())
                })
                .collect();

            let dot_scores: std::collections::HashMap<&str, f64> = dot_arr
                .iter()
                .map(|r| {
                    let a = r.as_array().unwrap();
                    (a[0].as_str().unwrap(), a[1].as_f64().unwrap())
                })
                .collect();

            for id in &["ex:a", "ex:b"] {
                let cos = cos_scores[id];
                let dot = dot_scores[id];
                assert!(
                    (cos - dot).abs() < 0.001,
                    "For unit vectors, cosine ({cos}) should ≈ dot ({dot}) for {id}"
                );
            }
        })
        .await;
}

/// Regression test: multi-property pattern with FILTER should use PropertyJoinOperator
/// and produce correct results. Previously, this combination fell back to NestedLoopJoin
/// and was ~12,000x slower. The fix allows PropertyJoinOperator with object bounds.
#[tokio::test]
async fn vector_search_with_date_filter_property_join() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-date-filter:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "fluree": "https://ns.flur.ee/db#"
        }
    ]);

    // Insert articles with vectors and dates.
    // homer: recent date (should pass filter), bart: old date (should be excluded)
    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "@vector"},
                "ex:publishedDate": {"@value": "2026-02-01", "@type": "xsd:date"}
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "@vector"},
                "ex:publishedDate": {"@value": "2025-01-15", "@type": "xsd:date"}
            },
            {
                "@id": "ex:marge",
                "ex:name": "Marge",
                "ex:xVec": {"@value": [0.9, 0.1], "@type": "@vector"},
                "ex:publishedDate": {"@value": "2026-01-20", "@type": "xsd:date"}
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &insert_txn).await.unwrap().ledger;

    // Query: filter to dates >= 2026-01-01, then score vectors.
    // This exercises the PropertyJoinOperator + object bounds path.
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score"],
        "values": [["?targetVec"], [{"@value": [0.7, 0.6], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:publishedDate": "?date", "ex:xVec": "?vec"},
            ["filter", [">=", "?date", "2026-01-01"]],
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]]
        ],
        "orderBy": [["desc", "?score"]]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // bart (2025-01-15) should be excluded by the date filter
    assert_eq!(
        arr.len(),
        2,
        "Only homer and marge should pass date filter >= 2026-01-01"
    );

    let mut results: Vec<(String, f64)> = arr
        .iter()
        .map(|r| {
            let a = r.as_array().unwrap();
            (a[0].as_str().unwrap().to_string(), a[1].as_f64().unwrap())
        })
        .collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // homer: dot([0.6,0.5], [0.7,0.6]) = 0.42 + 0.30 = 0.72
    // marge: dot([0.9,0.1], [0.7,0.6]) = 0.63 + 0.06 = 0.69
    assert_eq!(results[0].0, "ex:homer");
    assert!(
        (results[0].1 - 0.72).abs() < 0.01,
        "homer score ≈ 0.72, got {}",
        results[0].1
    );
    assert_eq!(results[1].0, "ex:marge");
    assert!(
        (results[1].1 - 0.69).abs() < 0.01,
        "marge score ≈ 0.69, got {}",
        results[1].1
    );
}

// ---------------------------------------------------------------------------
// SPARQL vector similarity function tests
// ---------------------------------------------------------------------------

/// Helper: insert vector test data and return the ledger state.
async fn seed_vector_data(fluree: &support::MemoryFluree) -> support::MemoryLedger {
    let ledger_id = "test/sparql-vector:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!([
        support::default_context(),
        {"ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#"}
    ]);

    let insert_txn = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:homer",
                "ex:name": "Homer",
                "ex:xVec": {"@value": [0.6, 0.5], "@type": "@vector"}
            },
            {
                "@id": "ex:bart",
                "ex:name": "Bart",
                "ex:xVec": {"@value": [0.1, 0.9], "@type": "@vector"}
            }
        ]
    });

    fluree.insert(ledger0, &insert_txn).await.unwrap().ledger
}

/// SPARQL dotProduct via BIND
#[tokio::test]
async fn sparql_vector_dot_product() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_vector_data(&fluree).await;

    let sparql = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX f: <https://ns.flur.ee/db#>
        SELECT ?name ?score
        WHERE {
            VALUES ?targetVec { "[0.7, 0.6]"^^f:embeddingVector }
            ?x ex:xVec ?vec ;
               ex:name ?name .
            BIND(dotProduct(?vec, ?targetVec) AS ?score)
        }
        ORDER BY DESC(?score)
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("SPARQL dotProduct query");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2);
    // Homer: 0.6*0.7 + 0.5*0.6 = 0.72
    assert_eq!(arr[0][0], "Homer");
    assert!((arr[0][1].as_f64().unwrap() - 0.72).abs() < 0.01);
    // Bart: 0.1*0.7 + 0.9*0.6 = 0.61
    assert_eq!(arr[1][0], "Bart");
    assert!((arr[1][1].as_f64().unwrap() - 0.61).abs() < 0.01);
}

/// SPARQL cosineSimilarity via BIND
#[tokio::test]
async fn sparql_vector_cosine_similarity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_vector_data(&fluree).await;

    let sparql = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX f: <https://ns.flur.ee/db#>
        SELECT ?name ?score
        WHERE {
            VALUES ?targetVec { "[0.7, 0.6]"^^f:embeddingVector }
            ?x ex:xVec ?vec ;
               ex:name ?name .
            BIND(cosineSimilarity(?vec, ?targetVec) AS ?score)
        }
        ORDER BY DESC(?score)
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("SPARQL cosineSimilarity query");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2);
    // Homer's vector is more aligned with target direction
    let homer_score = arr[0][1].as_f64().unwrap();
    let bart_score = arr[1][1].as_f64().unwrap();
    assert!(homer_score > bart_score, "Homer should rank higher");
    // Cosine similarity should be in [-1, 1]
    assert!((-1.0..=1.0).contains(&homer_score));
    assert!((-1.0..=1.0).contains(&bart_score));
}

/// SPARQL euclideanDistance via BIND
#[tokio::test]
async fn sparql_vector_euclidean_distance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_vector_data(&fluree).await;

    let sparql = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX f: <https://ns.flur.ee/db#>
        SELECT ?name ?dist
        WHERE {
            VALUES ?targetVec { "[0.7, 0.6]"^^f:embeddingVector }
            ?x ex:xVec ?vec ;
               ex:name ?name .
            BIND(euclideanDistance(?vec, ?targetVec) AS ?dist)
        }
        ORDER BY ?dist
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("SPARQL euclideanDistance query");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 2);
    // Homer is closer to target (lower distance first due to ASC order)
    assert_eq!(arr[0][0], "Homer");
    assert_eq!(arr[1][0], "Bart");
    let homer_dist = arr[0][1].as_f64().unwrap();
    let bart_dist = arr[1][1].as_f64().unwrap();
    assert!(homer_dist < bart_dist, "Homer should be closer");
    assert!(homer_dist >= 0.0, "distance must be non-negative");
}

/// SPARQL vector similarity with FILTER on score
#[tokio::test]
async fn sparql_vector_with_score_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_vector_data(&fluree).await;

    let sparql = r#"
        PREFIX ex: <http://example.org/ns/>
        PREFIX f: <https://ns.flur.ee/db#>
        SELECT ?name ?score
        WHERE {
            VALUES ?targetVec { "[0.7, 0.6]"^^f:embeddingVector }
            ?x ex:xVec ?vec ;
               ex:name ?name .
            BIND(dotProduct(?vec, ?targetVec) AS ?score)
            FILTER(?score > 0.65)
        }
    "#;

    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("SPARQL dotProduct with FILTER");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // Only Homer (0.72) passes the threshold; Bart (0.61) does not
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0][0], "Homer");
}
