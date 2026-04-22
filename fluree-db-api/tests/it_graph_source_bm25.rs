mod support;

use fluree_db_api::{Bm25CreateConfig, FlureeBuilder, FlureeIndexProvider};
use fluree_db_nameservice::STORAGE_SEGMENT_GRAPH_SOURCES;
use fluree_db_query::bm25::{Analyzer, Bm25IndexProvider, Bm25Scorer};
use serde_json::json;

/// Minimal end-to-end check that create_full_text_index actually indexes documents
/// (not just publishes an empty snapshot).
#[tokio::test]
async fn bm25_create_full_text_index_indexes_docs_and_is_loadable() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed a small ledger
    let ledger_id = "bm25/docs:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Hello world" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Hello rust" }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Indexing query: root var is ?x (so execute_indexing_query can populate top-level @id)
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("bm25-search", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert!(created.doc_count > 0, "expected index to include documents");
    assert!(created.index_id.is_some(), "expected persisted index id");

    // Load the index back via nameservice+storage
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(idx.num_docs() > 0, "loaded index should include documents");
}

/// Test that BM25 search returns scored results
#[tokio::test]
async fn bm25_search_returns_scored_results() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed ledger with documents
    let ledger_id = "bm25/search:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Rust programming language" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Rust and systems programming" },
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"Python programming language" }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create BM25 index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("search-test", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();

    // Load and search using Bm25Scorer
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();

    // Analyze query and score
    let analyzer = Analyzer::english_default();
    let query_terms = analyzer.analyze_to_strings("rust programming");
    let term_refs: Vec<&str> = query_terms
        .iter()
        .map(std::string::String::as_str)
        .collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let results = scorer.top_k(10);

    // Should find all 3 docs (they all have "programming")
    assert!(!results.is_empty(), "expected search results");
    assert_eq!(results.len(), 3, "expected 3 results");

    // Rust documents (doc1, doc2) should rank higher than Python (doc3)
    // because they match "rust" in addition to "programming"
    let ids: Vec<_> = results
        .iter()
        .map(|(doc_key, _score)| doc_key.subject_iri.as_ref())
        .collect();

    // Find positions of each document
    let doc1_pos = ids.iter().position(|id| id.contains("doc1"));
    let doc2_pos = ids.iter().position(|id| id.contains("doc2"));
    let doc3_pos = ids.iter().position(|id| id.contains("doc3"));

    // Verify rust docs (doc1, doc2) rank before python doc (doc3)
    assert!(
        doc1_pos.is_some() && doc2_pos.is_some() && doc3_pos.is_some(),
        "expected all docs in results, got: {ids:?}"
    );
    assert!(
        doc1_pos.unwrap() < doc3_pos.unwrap(),
        "expected doc1 to rank before doc3 (rust before python)"
    );
    assert!(
        doc2_pos.unwrap() < doc3_pos.unwrap(),
        "expected doc2 to rank before doc3 (rust before python)"
    );
}

/// Test BM25 sync indexes new documents after initial creation
#[tokio::test]
async fn bm25_sync_indexes_new_documents() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create initial ledger with one doc
    let ledger_id = "bm25/sync:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx1 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Initial document" }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;

    // Create BM25 index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("sync-test", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert_eq!(created.doc_count, 1, "expected 1 doc initially");

    // Add more documents
    let tx2 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Second document" },
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"Third document" }
        ]
    });
    let _ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    // Sync the index (uses sync_bm25_index which does a full resync when stale)
    let synced = fluree
        .sync_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    // Verify sync was successful - new watermark should be > old
    assert!(synced.was_full_resync || synced.new_watermark >= synced.old_watermark);

    // Verify by loading the index and checking doc count
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.num_docs(), 3, "loaded index should have 3 docs");
}

/// Test BM25 snapshot history tracks versions for time-travel
#[tokio::test]
async fn bm25_snapshot_history_tracks_versions() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with doc
    let ledger_id = "bm25/history:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx1 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Version one" }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;
    let t1 = ledger1.t();

    // Create BM25 index (snapshot at t1)
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("history-test", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();

    // Add another doc and sync (snapshot at t2)
    let tx2 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Version two" }
        ]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;
    let t2 = ledger2.t();

    let synced = fluree
        .sync_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(synced.was_full_resync || synced.new_watermark >= synced.old_watermark);

    // Load at t1 should get 1 doc (returns tuple of (index, actual_t))
    let (idx_t1, actual_t1) = fluree
        .load_bm25_index_at(&created.graph_source_id, t1)
        .await
        .unwrap();
    assert_eq!(idx_t1.num_docs(), 1, "expected 1 doc at t1");
    assert_eq!(actual_t1, t1, "expected snapshot at exactly t1");

    // Load at t2 should get 2 docs
    let (idx_t2, actual_t2) = fluree
        .load_bm25_index_at(&created.graph_source_id, t2)
        .await
        .unwrap();
    assert_eq!(idx_t2.num_docs(), 2, "expected 2 docs at t2");
    assert_eq!(actual_t2, t2, "expected snapshot at exactly t2");
}

/// Test BM25 drop cleans up index and prevents further syncs
#[tokio::test]
async fn bm25_drop_full_text_index_cleans_up() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with doc
    let ledger_id = "bm25/drop:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Test document" }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create BM25 index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("drop-test", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert!(created.doc_count > 0);

    // Verify we can load it
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(idx.num_docs(), 1);

    // Drop the index
    let drop_result = fluree
        .drop_full_text_index(&created.graph_source_id)
        .await
        .unwrap();
    assert_eq!(drop_result.graph_source_id, created.graph_source_id);
    assert!(!drop_result.was_already_retracted);
    assert!(
        drop_result.deleted_snapshots >= 1,
        "expected at least 1 deleted snapshot"
    );

    // Sync should now fail
    let sync_result = fluree.sync_bm25_index(&created.graph_source_id).await;
    assert!(
        sync_result.is_err(),
        "sync should fail for dropped graph source"
    );

    // Add another doc to ledger
    let tx2 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Second doc" }
        ]
    });
    let _ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    // Sync should still fail
    let sync_result2 = fluree.sync_bm25_index(&created.graph_source_id).await;
    assert!(
        sync_result2.is_err(),
        "sync should still fail after ledger update"
    );

    // Drop again should be idempotent (was_already_retracted = true)
    let drop_result2 = fluree
        .drop_full_text_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(drop_result2.was_already_retracted);
    assert_eq!(drop_result2.deleted_snapshots, 0);
}

/// Test BM25 recreate index after drop: can create a new index with the same name after dropping
///
/// Scenario: drop a graph source
/// and then recreate it with the same name.
#[tokio::test]
async fn bm25_recreate_after_drop() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with docs
    let ledger_id = "bm25/recreate:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Original document one" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Original document two" }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create BM25 index with a specific name
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("recreate-test", ledger_id, query.clone());
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert_eq!(created.doc_count, 2, "initial index should have 2 docs");

    // Verify search works
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings("original");
    let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let results = scorer.top_k(10);
    assert_eq!(results.len(), 2, "search should find 2 original docs");

    // Drop the index
    let drop_result = fluree
        .drop_full_text_index(&created.graph_source_id)
        .await
        .unwrap();
    assert!(!drop_result.was_already_retracted);

    // Add new documents to ledger with a distinctive term
    let tx2 = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"Additional documentation three" }
        ]
    });
    let _ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    // Recreate index with SAME name
    let cfg2 = Bm25CreateConfig::new("recreate-test", ledger_id, query);
    let created2 = fluree.create_full_text_index(cfg2).await.unwrap();

    // New index should see all 3 documents
    assert_eq!(created2.doc_count, 3, "recreated index should have 3 docs");
    assert_eq!(
        created2.graph_source_id, created.graph_source_id,
        "should have same alias"
    );

    // Verify search works on new index
    let idx2 = fluree
        .load_bm25_index(&created2.graph_source_id)
        .await
        .unwrap();

    // Search for "additional" which only appears in the new doc
    let terms2 = analyzer.analyze_to_strings("additional");
    let term_refs2: Vec<&str> = terms2.iter().map(std::string::String::as_str).collect();
    let scorer2 = Bm25Scorer::new(&idx2, &term_refs2);
    let results2 = scorer2.top_k(10);
    assert_eq!(
        results2.len(),
        1,
        "search should find 1 new doc with 'additional'"
    );

    // Also verify "original" still finds 2 docs
    let terms3 = analyzer.analyze_to_strings("original");
    let term_refs3: Vec<&str> = terms3.iter().map(std::string::String::as_str).collect();
    let scorer3 = Bm25Scorer::new(&idx2, &term_refs3);
    let results3 = scorer3.top_k(10);
    assert_eq!(
        results3.len(),
        2,
        "search should still find 2 original docs"
    );
}

/// Test BM25 federated query: FlureeIndexProvider loads index and search works
///
/// This tests the full integration flow:
/// 1. FlureeIndexProvider implements Bm25IndexProvider trait
/// 2. Index can be loaded through the provider
/// 3. Search results can be used to look up documents in the ledger
#[tokio::test]
async fn bm25_federated_query_via_provider() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with documents
    let ledger_id = "bm25/federated:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Rust programming guide", "ex:author":"Alice" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Rust and WebAssembly", "ex:author":"Bob" },
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"Python for beginners", "ex:author":"Charlie" }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create BM25 index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("fed-search", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert_eq!(created.doc_count, 3);

    // Test 1: FlureeIndexProvider can load the index
    let provider = FlureeIndexProvider::new(&fluree);
    let idx = provider
        .bm25_index(&created.graph_source_id, Some(ledger.t()), false, None)
        .await
        .expect("provider should load BM25 index");

    // Test 2: Search for "rust programming" returns scored results
    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings("rust programming");
    let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let results = scorer.top_k(10);

    assert!(!results.is_empty(), "expected search results");

    // Test 3: Results contain valid document IRIs that match ledger data
    let mut found_rust_doc = false;
    let mut rust_scores = Vec::new();
    let mut python_score = None;

    for (doc_key, score) in &results {
        let iri = doc_key.subject_iri.as_ref();

        // Verify IRI is one of our documents
        if iri.contains("doc1") || iri.contains("doc2") {
            found_rust_doc = true;
            rust_scores.push(*score);
        } else if iri.contains("doc3") {
            python_score = Some(*score);
        }
    }

    assert!(found_rust_doc, "expected to find Rust documents in results");

    // Test 4: Rust docs score higher than Python doc (relevance ranking)
    if let Some(py_score) = python_score {
        let max_rust = rust_scores.iter().copied().fold(0.0_f64, f64::max);
        assert!(
            max_rust >= py_score,
            "Rust docs should score >= Python doc: rust_max={max_rust}, python={py_score}"
        );
    }

    // Test 5: Results can be used to query ledger for additional properties
    // (simulating what a federated query would do)
    for (doc_key, _score) in results.iter().take(2) {
        let iri = doc_key.subject_iri.as_ref();

        // Use the IRI to query ledger for author
        let doc_query = json!({
            "@context": { "ex":"http://example.org/" },
            "where": [{ "@id": iri, "ex:author":"?author" }],
            "select": ["?author"]
        });

        let result = support::query_jsonld(&fluree, &ledger, &doc_query)
            .await
            .unwrap();

        // Should have exactly one result (one author per doc)
        assert_eq!(result.batches.len(), 1, "expected one batch for doc query");
        let batch = &result.batches[0];
        assert!(!batch.is_empty(), "expected author result for {iri}");
    }
}

/// Test BM25 with file-backed storage: index persists to disk and search works
///
/// Scenario: filesystem BM25 which verifies:
/// - Index creation works with file storage
/// - Graph source directory exists on disk
/// - Search works with persisted index
#[tokio::test]
async fn bm25_file_backed_storage() {
    // Create temp directory for file storage
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let storage_path = tmp.path().to_string_lossy().to_string();

    // Build file-backed Fluree instance
    let fluree = FlureeBuilder::file(&storage_path)
        .build()
        .expect("build file fluree");

    // Create ledger with documents
    let ledger_id = "bm25/file:main";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:article1", "@type":"ex:Article", "ex:title":"Rust programming guide" },
            { "@id":"ex:article2", "@type":"ex:Article", "ex:title":"Python for beginners" },
            { "@id":"ex:article3", "@type":"ex:Article", "ex:title":"Systems programming in Rust" }
        ]
    });
    let ledger = fluree.insert(ledger, &tx).await.expect("insert").ledger;

    // Create BM25 index
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Article", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("article-search", ledger_id, query);
    let created = fluree
        .create_full_text_index(cfg)
        .await
        .expect("create index");
    assert_eq!(created.doc_count, 3, "expected 3 indexed docs");
    assert!(created.index_id.is_some(), "expected persisted index id");

    // Verify graph-sources directory exists
    let gs_dir = std::path::Path::new(&storage_path).join(STORAGE_SEGMENT_GRAPH_SOURCES);
    assert!(gs_dir.exists(), "graph-sources directory should exist");

    // Load and search using file-backed index
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .expect("load index");
    assert_eq!(idx.num_docs(), 3, "loaded index should have 3 docs");

    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings("rust");
    let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let results = scorer.top_k(10);

    // Should find 2 Rust-related articles
    assert_eq!(results.len(), 2, "expected 2 results for 'rust'");

    // Verify scores are numeric and in descending order
    let scores: Vec<f64> = results.iter().map(|(_, score)| *score).collect();
    assert!(
        scores[0] >= scores[1],
        "scores should be in descending order"
    );

    // Search for python should find 1 result
    let terms2 = analyzer.analyze_to_strings("python");
    let term_refs2: Vec<&str> = terms2.iter().map(std::string::String::as_str).collect();
    let scorer2 = Bm25Scorer::new(&idx, &term_refs2);
    let results2 = scorer2.top_k(10);
    assert_eq!(results2.len(), 1, "expected 1 result for 'python'");

    // Verify we can query ledger data alongside BM25 results (federated use case)
    let (doc_key, _score) = &results[0];
    let doc_query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id": doc_key.subject_iri.as_ref(), "ex:title":"?title" }],
        "select": ["?title"]
    });
    let query_result = support::query_jsonld(&fluree, &ledger, &doc_query)
        .await
        .expect("doc query");
    assert!(
        !query_result.batches.is_empty() && !query_result.batches[0].is_empty(),
        "should be able to query ledger for BM25 result doc"
    );

    // tmp directory will be cleaned up when `tmp` goes out of scope
}

/// Test BM25 query_connection_with_bm25 with f:* pattern syntax
///
/// This test verifies the full end-to-end integration:
/// - `query_connection_with_bm25` correctly wires the FlureeIndexProvider
/// - `f:*` patterns in where clauses resolve against BM25 graph source indexes
/// - Search results include document IRIs and scores
/// - Results can be joined with ledger data (via IriMatch bindings)
#[tokio::test]
async fn bm25_query_connection_with_idx_pattern() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with documents
    let ledger_id = "bm25/qc:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:doc1", "@type":"ex:Doc", "ex:title":"Rust programming guide", "ex:author":"Alice" },
            { "@id":"ex:doc2", "@type":"ex:Doc", "ex:title":"Rust and WebAssembly", "ex:author":"Bob" },
            { "@id":"ex:doc3", "@type":"ex:Doc", "ex:title":"Python for beginners", "ex:author":"Charlie" }
        ]
    });
    let _ledger = fluree.insert(ledger0, &tx).await.expect("insert failed");

    // Create BM25 index
    let index_query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Doc", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("qc-search", ledger_id, index_query);
    let created = fluree
        .create_full_text_index(cfg)
        .await
        .expect("create index failed");
    assert_eq!(created.doc_count, 3, "expected 3 indexed docs");

    // Test 1: Regular query via query_connection_with_bm25
    let regular_query = json!({
        "@context": { "ex":"http://example.org/" },
        "from": ledger_id,
        "where": [{"@id": "?doc", "@type": "ex:Doc", "ex:author": "?author"}],
        "select": ["?doc", "?author"]
    });

    let result = fluree
        .query_connection_with_bm25(&regular_query)
        .await
        .expect("query_connection_with_bm25 failed for regular query");
    let total_results: usize = result.batches.iter().map(fluree_db_api::Batch::len).sum();
    assert_eq!(total_results, 3, "expected 3 results from regular query");

    // Test 2: Execute f:* pattern through query_connection_with_bm25
    // This tests the full end-to-end flow:
    // - Pattern parsing (f:searchText, f:searchResult, f:searchLimit)
    // - BM25 operator execution
    // - IriMatch binding for cross-ledger joins
    // - Join with ledger data to get author
    let idx_query = json!({
        "@context": { "ex":"http://example.org/", "f": "https://ns.flur.ee/db#" },
        "from": ledger_id,
        "where": [
            // Search the BM25 index for "rust" FIRST - produces initial bindings
            {
                "f:graphSource": &created.graph_source_id,
                "f:searchText": "rust",
                "f:searchLimit": 10,
                "f:searchResult": {"f:resultId": "?doc", "f:resultScore": "?score"}
            },
            // Join with ledger data to get author
            { "@id": "?doc", "ex:author": "?author" }
        ],
        "select": ["?doc", "?score", "?author"]
    });

    let idx_result = fluree
        .query_connection_with_bm25(&idx_query)
        .await
        .expect("query_connection_with_bm25 failed for f:* query");

    // Should have results (2 rust docs)
    let idx_total: usize = idx_result
        .batches
        .iter()
        .map(fluree_db_api::Batch::len)
        .sum();
    assert!(
        idx_total >= 2,
        "expected at least 2 rust docs in f:* query results, got {idx_total}"
    );

    // Verify scores are present (VarRegistry uses "?score" with the ? prefix)
    let score_var_id = idx_result.vars.get("?score");
    assert!(
        score_var_id.is_some(),
        "expected ?score variable in results"
    );

    // Test 3: Verify FlureeIndexProvider can load the index directly
    let provider = FlureeIndexProvider::new(&fluree);
    let idx = provider
        .bm25_index(&created.graph_source_id, Some(1), false, None)
        .await
        .expect("FlureeIndexProvider should load BM25 index");
    assert_eq!(idx.num_docs(), 3, "index should have 3 docs");

    // Test 4: Direct search via provider (validates BM25 scorer)
    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings("rust");
    let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let search_results = scorer.top_k(10);
    assert_eq!(
        search_results.len(),
        2,
        "expected 2 rust docs via direct search"
    );
}

/// Test BM25 federated query with aggregation: search + join + groupBy/count
///
/// Scenario: federated BM25 aggregation scenarios.
/// Tests that BM25 results can be combined with ledger data for aggregation.
#[tokio::test]
async fn bm25_federated_query_with_aggregation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger with books having year and category metadata
    let ledger_id = "bm25/agg:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex":"http://example.org/" },
        "@graph": [
            { "@id":"ex:book1", "@type":"ex:Book", "ex:title":"Rust Systems Programming", "ex:year": 2020, "ex:category":"programming" },
            { "@id":"ex:book2", "@type":"ex:Book", "ex:title":"Learning Rust", "ex:year": 2021, "ex:category":"programming" },
            { "@id":"ex:book3", "@type":"ex:Book", "ex:title":"Advanced Rust Patterns", "ex:year": 2021, "ex:category":"programming" },
            { "@id":"ex:book4", "@type":"ex:Book", "ex:title":"Python Data Science", "ex:year": 2020, "ex:category":"data" },
            { "@id":"ex:book5", "@type":"ex:Book", "ex:title":"Database Design", "ex:year": 2019, "ex:category":"data" }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.unwrap().ledger;

    // Create BM25 index on title
    let query = json!({
        "@context": { "ex":"http://example.org/" },
        "where": [{ "@id":"?x", "@type":"ex:Book", "ex:title":"?title" }],
        "select": { "?x": ["@id", "ex:title"] }
    });

    let cfg = Bm25CreateConfig::new("book-search", ledger_id, query);
    let created = fluree.create_full_text_index(cfg).await.unwrap();
    assert_eq!(created.doc_count, 5, "expected 5 indexed books");

    // Load index and search for "rust" (should find 3 books)
    let idx = fluree
        .load_bm25_index(&created.graph_source_id)
        .await
        .unwrap();
    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings("rust");
    let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
    let scorer = Bm25Scorer::new(&idx, &term_refs);
    let search_results = scorer.top_k(10);
    assert_eq!(
        search_results.len(),
        3,
        "expected 3 rust books in search results"
    );

    // For each search result, query ledger for year to simulate aggregation
    // This mimics what a federated query with groupBy would do
    let mut year_counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();

    for (doc_key, _score) in &search_results {
        let iri = doc_key.subject_iri.as_ref();

        // Query ledger for year property
        let year_query = json!({
            "@context": { "ex":"http://example.org/" },
            "where": [{ "@id": iri, "ex:year":"?year" }],
            "select": ["?year"]
        });

        let result = support::query_jsonld(&fluree, &ledger, &year_query)
            .await
            .unwrap();
        if !result.batches.is_empty() && !result.batches[0].is_empty() {
            // Extract year from result
            let batch = &result.batches[0];
            if let Some(fluree_db_query::binding::Binding::Lit {
                val: fluree_db_core::FlakeValue::Long(year),
                ..
            }) = batch.column_by_idx(0).and_then(|col| col.first())
            {
                *year_counts.entry(*year).or_insert(0) += 1;
            }
        }
    }

    // Verify aggregation results: Rust books by year
    // - 2020: 1 book (Rust Systems Programming)
    // - 2021: 2 books (Learning Rust, Advanced Rust Patterns)
    assert_eq!(
        year_counts.get(&2020),
        Some(&1),
        "expected 1 rust book in 2020"
    );
    assert_eq!(
        year_counts.get(&2021),
        Some(&2),
        "expected 2 rust books in 2021"
    );
    assert!(
        !year_counts.contains_key(&2019),
        "expected no rust books in 2019"
    );
}
