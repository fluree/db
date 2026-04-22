mod support;

use fluree_db_api::{Bm25CreateConfig, Bm25DropResult, FlureeBuilder};
use serde_json::json;

/// Test creating BM25 graph sources via API
#[tokio::test]
async fn create_graph_source_test() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create test ledger
    let ledger = support::genesis_ledger(&fluree, "test-gs:main");
    let tx = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:article1", "ex:title": "Introduction to Fluree", "ex:content": "Fluree is a graph database"},
            {"@id": "ex:article2", "ex:title": "Advanced Queries", "ex:content": "Learn about complex queries"}
        ]
    });
    let _ledger = fluree.insert(ledger, &tx).await.unwrap().ledger;

    // Create BM25 full-text index
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": [{"@id": "?x", "@type": "ex:Article"}],
        "select": {"?x": ["@id", "ex:title", "ex:content"]}
    });

    let config = Bm25CreateConfig::new("article-search", "test-gs:main", query.clone())
        .with_k1(1.2)
        .with_b(0.75);

    let gs_obj = fluree.create_full_text_index(config).await.unwrap();
    let gs_name = &gs_obj.graph_source_id;

    // Graph source names are normalized with branch
    assert_eq!(gs_name, "article-search:main");

    // Verify we can retrieve the graph source record (must use full name with branch)
    let ns = fluree.nameservice();
    let gs_record = ns.lookup_graph_source(gs_name).await.unwrap().unwrap();
    assert_eq!(gs_record.name, "article-search");
    assert_eq!(gs_record.branch, "main");

    // Dependencies are stored in dependencies
    assert_eq!(gs_record.dependencies, vec!["test-gs:main"]);

    // Cannot create duplicate full-text index
    let duplicate_config = Bm25CreateConfig::new("article-search", "test-gs:main", query.clone());
    let result = fluree.create_full_text_index(duplicate_config).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));

    // List all graph sources
    let all_gs_records = ns.all_graph_source_records().await.unwrap();
    let sources: Vec<_> = all_gs_records
        .into_iter()
        .filter(|r| matches!(r.source_type, fluree_db_nameservice::GraphSourceType::Bm25))
        .collect();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].graph_source_id, "article-search:main");

    // Clean up
    let drop_result = fluree.drop_full_text_index(gs_name).await.unwrap();
    assert_eq!(drop_result.graph_source_id, *gs_name);
    assert!(!drop_result.was_already_retracted);
}

/// Test BM25 graph source receives updates when source ledger changes
#[tokio::test]
async fn bm25_index_updates_with_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create initial ledger with one article
    let ledger = support::genesis_ledger(&fluree, "articles:main");
    let tx1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:article1", "ex:title": "First Article", "ex:content": "This is the first article about databases"}]
    });
    let ledger1 = fluree.insert(ledger, &tx1).await.unwrap().ledger;

    // Create BM25 full-text index
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": [{"@id": "?x", "ex:title": "?title", "ex:content": "?content"}],
        "select": {"?x": ["@id", "ex:title", "ex:content"]}
    });

    let config = Bm25CreateConfig::new("article-search", "articles:main", query);
    let created = fluree.create_full_text_index(config).await.unwrap();
    assert_eq!(created.doc_count, 1); // One article initially

    // Insert NEW data into the source ledger
    let tx2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:article2", "ex:title": "Second Article", "ex:content": "This article discusses graph databases and queries"}]
    });
    let _ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    // Sync the graph source to pick up the new data
    let synced = fluree.sync_bm25_index("article-search:main").await.unwrap();
    assert!(synced.upserted >= 1); // At least one document added

    // Verify by loading the index and checking doc count
    let idx = fluree.load_bm25_index("article-search:main").await.unwrap();
    assert_eq!(idx.num_docs(), 2); // Now has 2 documents

    // Clean up
    let _drop_result: Bm25DropResult = fluree
        .drop_full_text_index("article-search:main")
        .await
        .unwrap();
}
