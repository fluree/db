#![cfg(feature = "vector")]
//! Integration tests for vector similarity search graph sources.
//!
//! These tests verify the end-to-end vector index lifecycle:
//! - create_vector_index
//! - load_vector_index / load_vector_index_at
//! - sync_vector_index / resync_vector_index
//! - check_vector_staleness
//! - drop_vector_index
//! - VectorIndexProvider search integration

mod support;

use fluree_db_api::{FlureeBuilder, FlureeIndexProvider, VectorCreateConfig};
use fluree_db_query::vector::DistanceMetric;
use serde_json::json;

/// Minimal end-to-end check that create_vector_index actually indexes documents
/// (not just publishes an empty snapshot).
#[tokio::test]
async fn vector_create_index_indexes_docs_and_is_loadable() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed a small ledger with embeddings (3-dimensional for simplicity)
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication of array elements
    let ledger_id = "vector/docs:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:title":"Hello world",
                "ex:embedding": { "@value": [0.1, 0.2, 0.3], "@type": "@vector" }
            },
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:title":"Hello rust",
                "ex:embedding": { "@value": [0.4, 0.5, 0.6], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Indexing query: root var is ?x (so execute_indexing_query can populate top-level @id)
    // Note: Don't bind embedding to a variable in WHERE - arrays would create multiple rows
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("vector-search", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert!(
        created.vector_count > 0,
        "expected index to include documents"
    );
    assert_eq!(created.vector_count, 2, "expected 2 vectors");
    assert!(created.index_id.is_some(), "expected persisted index id");

    // Load the index back via nameservice+storage
    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.len(), 2, "loaded index should include 2 vectors");
}

/// Test that vector search returns scored results with correct ordering
#[tokio::test]
async fn vector_search_returns_scored_results() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed ledger with documents - using 3D vectors for simplicity
    // doc1 and doc2 are similar (both have high first component)
    // doc3 is different (high third component)
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/search:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:title":"First document",
                "ex:embedding": { "@value": [0.9, 0.1, 0.05], "@type": "@vector" }
            },
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:title":"Second document",
                "ex:embedding": { "@value": [0.8, 0.2, 0.1], "@type": "@vector" }
            },
            {
                "@id":"ex:doc3",
                "@type":"ex:Doc",
                "ex:title":"Third document",
                "ex:embedding": { "@value": [0.1, 0.1, 0.9], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create vector index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("search-test", ledger_id, query, "ex:embedding", 3)
        .with_metric(DistanceMetric::Cosine);
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert_eq!(created.vector_count, 3);

    // Load and search - query vector similar to doc1/doc2
    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    let query_vector = [0.85, 0.15, 0.05];
    let results = idx.search(&query_vector, 10).unwrap();

    assert_eq!(results.len(), 3, "expected 3 results");

    // doc1 and doc2 should rank before doc3 (more similar to query)
    let ids: Vec<_> = results.iter().map(|r| r.iri.as_ref()).collect();

    let doc1_pos = ids.iter().position(|id| id.contains("doc1"));
    let doc2_pos = ids.iter().position(|id| id.contains("doc2"));
    let doc3_pos = ids.iter().position(|id| id.contains("doc3"));

    assert!(
        doc1_pos.is_some() && doc2_pos.is_some() && doc3_pos.is_some(),
        "expected all docs in results, got: {ids:?}"
    );

    // doc1 should rank first (most similar to query)
    assert_eq!(doc1_pos.unwrap(), 0, "expected doc1 to rank first");

    // doc3 should rank last (least similar)
    assert_eq!(doc3_pos.unwrap(), 2, "expected doc3 to rank last");
}

/// Test vector sync indexes new documents after initial creation
#[tokio::test]
async fn vector_sync_indexes_new_documents() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create initial ledger with one doc
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/sync:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx1 = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:title":"Initial document",
                "ex:embedding": { "@value": [0.5, 0.5, 0.0], "@type": "@vector" }
            }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;

    // Create vector index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("sync-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert_eq!(created.vector_count, 1, "expected 1 vector initially");

    // Add more documents
    let tx2 = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:title":"Second document",
                "ex:embedding": { "@value": [0.3, 0.3, 0.4], "@type": "@vector" }
            },
            {
                "@id":"ex:doc3",
                "@type":"ex:Doc",
                "ex:title":"Third document",
                "ex:embedding": { "@value": [0.1, 0.1, 0.8], "@type": "@vector" }
            }
        ]
    });
    let _ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    // Check staleness
    let staleness = fluree
        .check_vector_staleness(&created.graph_source_id)
        .await
        .unwrap();
    assert!(
        staleness.is_stale,
        "index should be stale after new commits"
    );
    assert!(staleness.lag > 0, "staleness lag should be > 0");

    // Sync the index
    let synced = fluree
        .sync_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(
        synced.was_full_resync || synced.new_watermark >= synced.old_watermark,
        "sync should update watermark"
    );

    // Verify by loading the index and checking vector count
    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.len(), 3, "loaded index should have 3 vectors");
}

/// Test vector index sync updates head snapshot (head-only, no time-travel)
#[tokio::test]
async fn vector_sync_updates_head_snapshot() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with initial doc
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/headonly:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx1 = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [1.0, 0.0, 0.0], "@type": "@vector" }
            }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;
    let t1 = ledger1.t();

    // Create vector index at t1
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("headonly-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert_eq!(created.vector_count, 1);
    assert_eq!(created.index_t, t1);

    // Load head — should have 1 vector
    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.len(), 1, "head index should have 1 vector after create");

    // Add more documents
    let tx2 = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.0, 1.0, 0.0], "@type": "@vector" }
            }
        ]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;
    let t2 = ledger2.t();

    // Sync to update head
    let synced = fluree
        .sync_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(synced.new_watermark, t2);

    // Load head again — should now have 2 vectors
    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.len(), 2, "head index should have 2 vectors after sync");
}

/// Test drop_vector_index marks graph source as retracted
#[tokio::test]
async fn vector_drop_index_marks_as_retracted() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create a minimal index
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/drop:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.5, 0.5, 0.0], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("drop-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();

    // Drop the index
    let dropped = fluree
        .drop_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(!dropped.was_already_retracted);

    // Trying to sync should fail
    let sync_result = fluree.sync_vector_index(&created.graph_source_id).await;
    assert!(sync_result.is_err(), "sync should fail on dropped index");

    // Dropping again should indicate already retracted
    let dropped_again = fluree
        .drop_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(dropped_again.was_already_retracted);
}

/// Test that documents with missing embeddings are skipped
#[tokio::test]
async fn vector_skips_documents_without_embeddings() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with some docs having embeddings and some without
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/skip:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:title":"Has embedding",
                "ex:embedding": { "@value": [0.1, 0.2, 0.3], "@type": "@vector" }
            },
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:title":"No embedding"
                // Note: no ex:embedding property
            },
            {
                "@id":"ex:doc3",
                "@type":"ex:Doc",
                "ex:title":"Also has embedding",
                "ex:embedding": { "@value": [0.4, 0.5, 0.6], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create index - should only index docs with embeddings
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("skip-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();

    // Should have 2 vectors (doc1 and doc3), skipped 1 (doc2)
    assert_eq!(created.vector_count, 2, "expected 2 vectors indexed");
    assert_eq!(created.skipped_count, 1, "expected 1 document skipped");
}

/// Test different distance metrics
#[tokio::test]
async fn vector_supports_different_metrics() {
    let fluree = FlureeBuilder::memory().build_memory();

    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/metrics:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [1.0, 0.0, 0.0], "@type": "@vector" }
            },
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.0, 1.0, 0.0], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    // Test with Dot product metric
    let cfg = VectorCreateConfig::new("metrics-dot", ledger_id, query.clone(), "ex:embedding", 3)
        .with_metric(DistanceMetric::Dot);
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert_eq!(created.vector_count, 2);

    let idx = fluree
        .load_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.metadata.metric, DistanceMetric::Dot);

    // Test with Euclidean metric
    let cfg2 = VectorCreateConfig::new(
        "metrics-euclidean",
        ledger_id,
        query.clone(),
        "ex:embedding",
        3,
    )
    .with_metric(DistanceMetric::Euclidean);
    let created2 = fluree.create_vector_index(cfg2).await.unwrap();
    assert_eq!(created2.vector_count, 2);

    let idx2 = fluree
        .load_vector_index(&created2.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx2.metadata.metric, DistanceMetric::Euclidean);
}

/// Test that FlureeIndexProvider implements VectorIndexProvider for query integration
#[tokio::test]
async fn vector_provider_integration() {
    use fluree_db_query::vector::{VectorIndexProvider, VectorSearchParams};

    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with vectors
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/provider:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.9, 0.1, 0.05], "@type": "@vector" }
            },
            {
                "@id":"ex:doc2",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.1, 0.9, 0.0], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create vector index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("provider-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();

    // Use FlureeIndexProvider to search
    let provider = FlureeIndexProvider::new(&fluree);
    let query_vector = [0.85, 0.15, 0.0];

    // Vector indexes are head-only (no time-travel), so as_of_t must be None
    let params = VectorSearchParams::new(&query_vector, DistanceMetric::Cosine, 10);

    let results = provider
        .search(&created.graph_source_id, params)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    // doc1 should rank first (more similar to query)
    assert!(
        results[0].iri.contains("doc1"),
        "expected doc1 first, got: {:?}",
        results[0].iri
    );
}

/// Test collection_exists check
#[tokio::test]
async fn vector_collection_exists() {
    use fluree_db_query::vector::VectorIndexProvider;

    let fluree = FlureeBuilder::memory().build_memory();

    // Create a ledger and index
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/exists:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex":"http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id":"ex:doc1",
                "@type":"ex:Doc",
                "ex:embedding": { "@value": [0.5, 0.5, 0.0], "@type": "@vector" }
            }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new("exists-test", ledger_id, query, "ex:embedding", 3);
    let created = fluree.create_vector_index(cfg).await.unwrap();

    let provider = FlureeIndexProvider::new(&fluree);

    // Should exist
    let exists = provider
        .collection_exists(&created.graph_source_id)
        .await
        .unwrap();
    assert!(exists, "collection should exist");

    // Non-existent should return false
    let not_exists = provider
        .collection_exists("nonexistent:main")
        .await
        .unwrap();
    assert!(!not_exists, "non-existent collection should not exist");

    // Drop and check again
    fluree
        .drop_vector_index(&created.graph_source_id)
        .await
        .unwrap();
    let after_drop = provider
        .collection_exists(&created.graph_source_id)
        .await
        .unwrap();
    assert!(!after_drop, "dropped collection should not exist");
}

/// End-to-end test for f:queryVector query syntax through the query pipeline.
///
/// This test verifies that vector search patterns in queries work correctly:
/// - Query parsing recognizes f:queryVector patterns
/// - VectorSearchOperator executes against the vector index
/// - Results include IDs and scores
#[tokio::test]
async fn vector_idx_query_syntax_e2e() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with vector embeddings
    // NOTE: Embeddings must use @type: @vector to avoid RDF deduplication
    let ledger_id = "vector/query-e2e:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:doc1",
                "@type": "ex:Doc",
                "ex:title": "Machine learning basics",
                "ex:embedding": { "@value": [0.9, 0.1, 0.05], "@type": "@vector" }
            },
            {
                "@id": "ex:doc2",
                "@type": "ex:Doc",
                "ex:title": "Database fundamentals",
                "ex:embedding": { "@value": [0.1, 0.9, 0.1], "@type": "@vector" }
            },
            {
                "@id": "ex:doc3",
                "@type": "ex:Doc",
                "ex:title": "Deep learning advanced",
                "ex:embedding": { "@value": [0.85, 0.15, 0.0], "@type": "@vector" }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create vector index
    let indexing_query = json!({
        "@context": { "ex": "http://example.org/" },
        "where": [{ "@id": "?x", "@type": "ex:Doc" }],
        "select": { "?x": ["@id", "ex:embedding"] }
    });

    let cfg = VectorCreateConfig::new(
        "query-e2e-idx",
        ledger_id,
        indexing_query,
        "ex:embedding",
        3,
    );
    let created = fluree.create_vector_index(cfg).await.unwrap();
    assert_eq!(created.vector_count, 3, "expected 3 vectors indexed");

    // Execute a query using f:queryVector syntax
    // Query for vectors similar to [0.85, 0.1, 0.05] - should match doc1 and doc3 best
    let search_query = json!({
        "@context": { "ex": "http://example.org/", "f": "https://ns.flur.ee/db#" },
        "from": ledger_id,
        "where": [
            {
                "f:graphSource": created.graph_source_id,
                "f:queryVector": [0.85, 0.1, 0.05],
                "f:distanceMetric": "cosine",
                "f:searchLimit": 10,
                "f:searchResult": {
                    "f:resultId": "?doc",
                    "f:resultScore": "?score"
                }
            }
        ],
        "select": ["?doc", "?score"]
    });

    let result = fluree
        .query_connection_with_bm25(&search_query)
        .await
        .unwrap();
    let formatted = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // Parse and verify results
    let results: Vec<serde_json::Value> = serde_json::from_value(formatted).unwrap();
    assert!(!results.is_empty(), "expected vector search results");

    // Results should be ordered by similarity score (highest first)
    // doc1 [0.9, 0.1, 0.05] and doc3 [0.85, 0.15, 0.0] should be most similar to query [0.85, 0.1, 0.05]
    // doc2 [0.1, 0.9, 0.1] should be least similar

    // Verify we have scores and IDs
    for (i, row) in results.iter().enumerate() {
        let row_arr = row.as_array().expect("result row should be array");
        assert_eq!(row_arr.len(), 2, "expected [doc, score] tuple");

        let doc = row_arr[0].as_str().expect("doc should be string");
        let score = row_arr[1].as_f64().expect("score should be number");

        assert!(doc.contains("doc"), "doc should contain 'doc': {doc}");
        // Cosine similarity can be in [-1, 1] (not [0, 1])
        // -1 = opposite, 0 = orthogonal, 1 = identical
        assert!(
            (-1.0..=1.0).contains(&score),
            "cosine score should be in [-1,1]: {score}"
        );

        // Log for debugging
        eprintln!("Result {i}: doc={doc}, score={score:.4}");
    }

    // Verify ordering: doc2 should be last (least similar)
    let last_row = results.last().unwrap().as_array().unwrap();
    let last_doc = last_row[0].as_str().unwrap();
    assert!(
        last_doc.contains("doc2"),
        "doc2 should be least similar (last), got: {last_doc}"
    );
}
