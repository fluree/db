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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
            let completion = handle.trigger(&fluree_db_api::LedgerId::parse(ledger_id).unwrap(), result.receipt.t).await;
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
            let completion = handle.trigger(&fluree_db_api::LedgerId::parse(ledger_id).unwrap(), r1.receipt.t).await;
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
            let completion = handle.trigger(&fluree_db_api::LedgerId::parse(ledger_id).unwrap(), r.receipt.t).await;
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

// =============================================================================
// Perf smoke harness
// =============================================================================

/// Deterministic pseudo-random f64 in [-1, 1) (LCG; no process randomness so
/// the test can regenerate the exact vectors for scalar verification).
struct Lcg(u64);

impl Lcg {
    fn next_f64(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64) * 2.0 - 1.0
    }
}

/// Perf smoke harness for per-row flat vector scoring at scale (run manually):
///
/// ```sh
/// cargo test -p fluree-db-api --features vector --test it_vector_flatrank \
///     --release vector_flatrank_perf_50k -- --ignored --nocapture
/// ```
///
/// Seeds 50k entities with 256-dim vectors (novelty-only, so every scanned
/// row arrives as a materialized `Binding::Lit` — the production shape once
/// any unindexed commit exists), then times `bind ?score (dotProduct ...)`
/// + threshold filter. Results are verified against scalar recomputation of
/// the same LCG-generated vectors: exact hit count, and top-5 ids/scores
/// within 1e-9 — so before/after runs must agree on output, not just speed.
#[tokio::test]
#[ignore = "perf smoke — run manually with --ignored --nocapture"]
async fn vector_flatrank_perf_50k() {
    const N: usize = 50_000;
    const DIMS: usize = 256;
    const CHUNK: usize = 10_000;
    const THRESHOLD: f64 = 11.0;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-perf:main";
    let mut ledger = fluree.create_ledger(ledger_id).await.unwrap();

    // Generate all vectors once (kept for scalar verification below).
    let mut rng = Lcg(42);
    let vectors: Vec<Vec<f64>> = (0..N)
        .map(|_| (0..DIMS).map(|_| rng.next_f64()).collect())
        .collect();
    let target: Vec<f64> = (0..DIMS).map(|_| rng.next_f64()).collect();

    let seed_start = std::time::Instant::now();
    for chunk_start in (0..N).step_by(CHUNK) {
        let entities: Vec<serde_json::Value> = (chunk_start..(chunk_start + CHUNK).min(N))
            .map(|i| {
                json!({
                    "@id": format!("ex:v{i}"),
                    "ex:vec": {
                        "@value": vectors[i],
                        "@type": "https://ns.flur.ee/db#embeddingVector"
                    }
                })
            })
            .collect();
        let txn = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": entities
        });
        ledger = fluree
            .insert(ledger, &txn)
            .await
            .expect("seed chunk")
            .ledger;
    }
    println!(
        "vector_perf_50k: seeded {N} x {DIMS}-dim in {:?}",
        seed_start.elapsed()
    );

    // Scalar ground truth: expected hits and top-5 (id index, score).
    let scalar_dot = |a: &[f64], b: &[f64]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>();
    let mut expected: Vec<(usize, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i, scalar_dot(v, &target)))
        .filter(|(_, s)| *s > THRESHOLD)
        .collect();
    expected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    assert!(
        expected.len() > 100,
        "threshold should pass a meaningful subset, got {}",
        expected.len()
    );

    let query_dot = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?x", "?score"],
        "values": [["?targetVec"],
            [{"@value": target, "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:vec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?targetVec"]],
            ["filter", format!("(> ?score {THRESHOLD})")]
        ],
        "orderBy": [["desc", "?score"]]
    });

    for run in ["cold", "warm"] {
        let start = std::time::Instant::now();
        let result = support::query_jsonld(&fluree, &ledger, &query_dot)
            .await
            .expect("dotProduct query");
        let rows = result.to_jsonld(&ledger.snapshot).unwrap();
        let arr = rows.as_array().unwrap().clone();
        println!(
            "vector_perf_50k: dotProduct {run} run: {} hits in {:?}",
            arr.len(),
            start.elapsed()
        );

        assert_eq!(
            arr.len(),
            expected.len(),
            "{run}: hit count must match scalar recomputation"
        );
        for (rank, (exp_idx, exp_score)) in expected.iter().take(5).enumerate() {
            let row = arr[rank].as_array().unwrap();
            let id = row[0].as_str().unwrap();
            let score = row[1].as_f64().unwrap();
            assert_eq!(id, format!("ex:v{exp_idx}"), "{run}: rank {rank} id");
            assert!(
                (score - exp_score).abs() <= 1e-6 * exp_score.abs().max(1.0),
                "{run}: rank {rank} score {score} != scalar {exp_score}"
            );
        }
    }

    // Cosine + euclidean: one timed pass each, count-verified against scalar.
    for (func, name) in [
        ("cosineSimilarity", "cosine"),
        ("euclideanDistance", "euclidean"),
    ] {
        let (filter, expected_count) = match name {
            "cosine" => {
                let cnt = vectors
                    .iter()
                    .filter(|v| {
                        let dot = scalar_dot(v, &target);
                        let ma = scalar_dot(v, v).sqrt();
                        let mb = scalar_dot(&target, &target).sqrt();
                        dot / (ma * mb) > 0.2
                    })
                    .count();
                ("(> ?score 0.2)".to_string(), cnt)
            }
            _ => {
                let cnt = vectors
                    .iter()
                    .filter(|v| {
                        v.iter()
                            .zip(&target)
                            .map(|(x, y)| (x - y) * (x - y))
                            .sum::<f64>()
                            .sqrt()
                            < 12.0
                    })
                    .count();
                ("(< ?score 12.0)".to_string(), cnt)
            }
        };
        let query = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "select": ["?x", "?score"],
            "values": [["?targetVec"],
                [{"@value": target, "@type": "https://ns.flur.ee/db#embeddingVector"}]],
            "where": [
                {"@id": "?x", "ex:vec": "?vec"},
                ["bind", "?score", [func, "?vec", "?targetVec"]],
                ["filter", filter]
            ]
        });
        let start = std::time::Instant::now();
        let result = support::query_jsonld(&fluree, &ledger, &query)
            .await
            .expect("scored query");
        let rows = result.to_jsonld(&ledger.snapshot).unwrap();
        let got = rows.as_array().unwrap().len();
        println!(
            "vector_perf_50k: {name} run: {got} hits in {:?}",
            start.elapsed()
        );
        assert_eq!(
            got, expected_count,
            "{name}: hit count must match scalar recomputation"
        );
    }

    // ── Indexed phase ──────────────────────────────────────────────────────
    // Full reindex, then reload: novelty is empty (overlay epoch 0), so the
    // scan late-materializes and `?vec` reaches eval as EncodedLit VECTOR_ID
    // — the packed-f32-shard path, exercised with the same verification.
    let reindex_start = std::time::Instant::now();
    fluree
        .reindex(ledger_id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("reload indexed");
    println!(
        "vector_perf_50k: reindexed in {:?} (t={})",
        reindex_start.elapsed(),
        ledger.t()
    );

    let query_dot_pipeline = {
        let mut q = query_dot.clone();
        // Duplicate threshold filter: same semantics, but defeats the top-k
        // fast-path detection (≤1 filter) so the generic pipeline runs.
        q["where"]
            .as_array_mut()
            .unwrap()
            .push(json!(["filter", format!("(> ?score {THRESHOLD})")]));
        q
    };
    for run in ["indexed pipeline cold", "indexed pipeline warm"] {
        let start = std::time::Instant::now();
        let result = support::query_jsonld(&fluree, &ledger, &query_dot_pipeline)
            .await
            .expect("indexed pipeline query");
        let rows = result.to_jsonld(&ledger.snapshot).unwrap();
        println!(
            "vector_perf_50k: dotProduct {run} run: {} hits in {:?}",
            rows.as_array().unwrap().len(),
            start.elapsed()
        );
    }
    for run in ["indexed cold", "indexed warm"] {
        let start = std::time::Instant::now();
        let result = support::query_jsonld(&fluree, &ledger, &query_dot)
            .await
            .expect("indexed dotProduct query");
        let rows = result.to_jsonld(&ledger.snapshot).unwrap();
        let arr = rows.as_array().unwrap().clone();
        println!(
            "vector_perf_50k: dotProduct {run} run: {} hits in {:?}",
            arr.len(),
            start.elapsed()
        );

        assert_eq!(
            arr.len(),
            expected.len(),
            "{run}: hit count must match scalar recomputation"
        );
        for (rank, (exp_idx, exp_score)) in expected.iter().take(5).enumerate() {
            let row = arr[rank].as_array().unwrap();
            let id = row[0].as_str().unwrap();
            let score = row[1].as_f64().unwrap();
            assert_eq!(id, format!("ex:v{exp_idx}"), "{run}: rank {rank} id");
            assert!(
                (score - exp_score).abs() <= 1e-6 * exp_score.abs().max(1.0),
                "{run}: rank {rank} score {score} != scalar {exp_score}"
            );
        }
    }
}

// =============================================================================
// Flat-rank top-k fast-path parity
// =============================================================================

/// End-to-end parity for the flat-rank vector fast path against scalar
/// ground truth, across its lanes:
///
/// - indexed base (epoch 0): threshold + ORDER BY DESC + LIMIT top-k
/// - overlay lane: novelty assert (new top vector) + retraction of an
///   indexed vector via upsert — both must be reflected without reindexing
/// - multi-cardinality: a subject with two vectors yields two scored rows
/// - fallback shapes: an extra hydration triple and a projected `?vec`
///   must not detect (results still correct via the pipeline)
#[tokio::test]
async fn vector_topk_fast_path_parity() {
    use fluree_db_api::ReindexOptions;

    const DIMS: usize = 8;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-topk-parity:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Deterministic vectors; values chosen to be exactly f32-representable
    // is NOT required — ground truth below quantizes the same way ingest does.
    let mut rng = Lcg(7);
    let vectors: Vec<Vec<f64>> = (0..40)
        .map(|_| (0..DIMS).map(|_| rng.next_f64()).collect())
        .collect();
    let target: Vec<f64> = (0..DIMS).map(|_| rng.next_f64()).collect();
    // Give one subject (v39) a second vector (multi-cardinality).
    let second_vec: Vec<f64> = (0..DIMS).map(|_| rng.next_f64()).collect();

    let mut entities: Vec<serde_json::Value> = (0..40)
        .map(|i| {
            json!({
                "@id": format!("ex:v{i}"),
                "ex:vec": {"@value": vectors[i], "@type": "https://ns.flur.ee/db#embeddingVector"}
            })
        })
        .collect();
    entities.push(json!({
        "@id": "ex:v39",
        "ex:vec": {"@value": second_vec, "@type": "https://ns.flur.ee/db#embeddingVector"}
    }));
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": entities}),
        )
        .await
        .expect("seed")
        .ledger;
    let _ = ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("reload indexed");

    // Ground truth: stored vectors are f32-quantized at ingest; the query
    // VALUES vector is used at full precision (only *storage* quantizes).
    let q = |v: &[f64]| -> Vec<f64> { v.iter().map(|&x| (x as f32) as f64).collect() };
    let dot = |a: &[f64], b: &[f64]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>();
    let qt = target.clone();
    // (subject index, score); index 40 = v39's second vector.
    let mut facts: Vec<(usize, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i, dot(&q(v), &qt)))
        .collect();
    facts.push((39, dot(&q(&second_vec), &qt)));

    let run_topk = |ledger: support::MemoryLedger, k: usize| {
        let fluree = &fluree;
        let target = target.clone();
        async move {
            let query = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": ["?x", "?score"],
                "values": [["?t"], [{"@value": target, "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?x", "ex:vec": "?vec"},
                    ["bind", "?score", ["dotProduct", "?vec", "?t"]],
                    ["filter", "(> ?score 0)"]
                ],
                "orderBy": [["desc", "?score"]],
                "limit": k
            });
            let result = support::query_jsonld(fluree, &ledger, &query)
                .await
                .expect("topk query");
            let rows = result.to_jsonld(&ledger.snapshot).unwrap();
            let out: Vec<(String, f64)> = rows
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    let r = r.as_array().unwrap();
                    (r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap())
                })
                .collect();
            (ledger, out)
        }
    };

    let expect_topk = |facts: &[(usize, f64)], k: usize| -> Vec<(String, f64)> {
        let mut passing: Vec<(usize, f64)> =
            facts.iter().copied().filter(|(_, s)| *s > 0.0).collect();
        passing.sort_by(|a, b| b.1.total_cmp(&a.1));
        passing
            .into_iter()
            .take(k)
            .map(|(i, s)| (format!("ex:v{i}"), s))
            .collect()
    };

    let assert_rows = |got: &[(String, f64)], want: &[(String, f64)], label: &str| {
        assert_eq!(got.len(), want.len(), "{label}: row count");
        for (idx, ((gid, gs), (wid, ws))) in got.iter().zip(want).enumerate() {
            assert_eq!(
                gid, wid,
                "{label}: rank {idx} id (got {got:?} want {want:?})"
            );
            assert!(
                (gs - ws).abs() <= 1e-9 * ws.abs().max(1.0),
                "{label}: rank {idx} score {gs} != {ws}"
            );
        }
    };

    // ── Phase A: indexed base, top-5 ───────────────────────────────────────
    let (ledger, got) = run_topk(ledger, 5).await;
    assert_rows(&got, &expect_topk(&facts, 5), "indexed base");

    // ── Phase B: novelty — new top vector + retraction via upsert ─────────
    // New subject whose vector is target*2 → dominant top score.
    let big: Vec<f64> = target.iter().map(|x| x * 2.0).collect();
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:vNEW",
                "ex:vec": {"@value": big, "@type": "https://ns.flur.ee/db#embeddingVector"}
            }),
        )
        .await
        .expect("novelty insert")
        .ledger;
    // Retract the current best base vector by replacing it (upsert) with a
    // strongly negative one.
    let best_idx = facts
        .iter()
        .filter(|(i, _)| *i != 39) // v39 is multi-cardinality; upsert replaces BOTH its vectors
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .unwrap()
        .0;
    let neg: Vec<f64> = target.iter().map(|x| -x).collect();
    let ledger = fluree
        .upsert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": format!("ex:v{best_idx}"),
                "ex:vec": {"@value": neg, "@type": "https://ns.flur.ee/db#embeddingVector"}
            }),
        )
        .await
        .expect("retract via upsert")
        .ledger;

    // Update ground truth: drop best_idx's old fact, add its replacement and vNEW.
    facts.retain(|(i, _)| *i != best_idx);
    facts.push((best_idx, dot(&q(&neg), &qt)));
    let mut facts_with_new: Vec<(String, f64)> = facts
        .iter()
        .map(|(i, s)| (format!("ex:v{i}"), *s))
        .collect();
    facts_with_new.push(("ex:vNEW".to_string(), dot(&q(&big), &qt)));
    let mut want: Vec<(String, f64)> = facts_with_new
        .into_iter()
        .filter(|(_, s)| *s > 0.0)
        .collect();
    want.sort_by(|a, b| b.1.total_cmp(&a.1));
    want.truncate(5);
    assert_eq!(want[0].0, "ex:vNEW", "sanity: novelty vector must lead");

    let (ledger, got) = run_topk(ledger, 5).await;
    assert_rows(&got, &want, "overlay lane");

    // ── Phase C: fallback shapes stay correct ──────────────────────────────
    // Projecting ?vec must not detect (vector materialization is pipeline
    // work); the result set must be the same subjects/scores.
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?x", "?score", "?vec"],
        "values": [["?t"], [{"@value": target, "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:vec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?t"]],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 5
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("fallback query");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let got_fb: Vec<(String, f64)> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            let r = r.as_array().unwrap();
            (r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap())
        })
        .collect();
    assert_rows(&got_fb, &want, "fallback (?vec projected)");
}

/// A non-vector literal physically stored on the vector predicate passes
/// fast-path *detection* (the canonical `{?x, ?score}` shape), then trips the
/// runtime `Scorer::score → Err(Bail)` mid-scan. The whole fast path must defer
/// to the generic pipeline and return the pipeline's exact rows — never a
/// partial/garbage result.
#[tokio::test]
async fn vector_topk_fast_path_runtime_bail_defers_to_pipeline() {
    use fluree_db_api::ReindexOptions;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/vector-topk-runtime-bail:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let ctx = json!({"ex": "http://example.org/ns/"});
    // Real vectors plus one non-vector string on the SAME predicate.
    let real: [(&str, [f64; 2]); 4] = [
        ("ex:a", [0.9, 0.1]),
        ("ex:b", [0.5, 0.5]),
        ("ex:c", [0.1, 0.9]),
        ("ex:d", [0.8, 0.2]),
    ];
    let mut graph: Vec<serde_json::Value> = real
        .iter()
        .map(|(id, v)| {
            json!({
                "@id": id,
                "ex:vec": {"@value": [v[0], v[1]], "@type": "https://ns.flur.ee/db#embeddingVector"}
            })
        })
        .collect();
    graph.push(json!({"@id": "ex:bad", "ex:vec": "Not a Vector"}));

    let ledger = fluree
        .insert(ledger0, &json!({"@context": ctx, "@graph": graph}))
        .await
        .expect("seed")
        .ledger;
    let _ = ledger;
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("reload indexed");

    let target = [0.7_f64, 0.3_f64];
    let query = json!({
        "@context": ctx,
        "select": ["?x", "?score"],
        "values": [["?t"], [{"@value": [target[0], target[1]], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
        "where": [
            {"@id": "?x", "ex:vec": "?vec"},
            ["bind", "?score", ["dotProduct", "?vec", "?t"]],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]],
        "limit": 3
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("runtime-bail query");
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let got: Vec<(String, f64)> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            let r = r.as_array().unwrap();
            (r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap())
        })
        .collect();

    // Scalar ground truth over the real vectors only (f32-quantized at ingest;
    // the target stays full precision). The non-vector subject contributes
    // nothing, so it must be absent from the deferred pipeline's output.
    let q = |x: f64| (x as f32) as f64;
    let mut want: Vec<(String, f64)> = real
        .iter()
        .map(|(id, v)| (id.to_string(), q(v[0]) * target[0] + q(v[1]) * target[1]))
        .filter(|(_, s)| *s > 0.0)
        .collect();
    want.sort_by(|a, b| b.1.total_cmp(&a.1));
    want.truncate(3);

    assert_eq!(
        got.len(),
        want.len(),
        "runtime bail must defer, not emit partial: got {got:?}"
    );
    for (i, ((gid, gs), (wid, ws))) in got.iter().zip(&want).enumerate() {
        assert_eq!(gid, wid, "rank {i} id: got {got:?} want {want:?}");
        assert!(
            (gs - ws).abs() <= 1e-9 * ws.abs().max(1.0),
            "rank {i} score {gs} != {ws}"
        );
    }
    assert!(
        !got.iter().any(|(id, _)| id == "ex:bad"),
        "non-vector subject must not appear: {got:?}"
    );
}
