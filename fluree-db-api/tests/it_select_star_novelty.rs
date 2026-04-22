//! Regression tests for GitHub issue #95:
//! `select *` crawl returns empty properties for novelty-only entities when
//! `index_t < commit_t`.
//!
//! Reproduces the condition: bootstrap schema → reindex → insert entity
//! (novelty only) → query with `select *`.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, ReindexOptions};
use serde_json::{json, Value};

/// A minimal schema with enough triples to populate at least one indexed leaf.
/// Uses the `http://example.org/` namespace exclusively.
fn minimal_ontology() -> Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:Widget", "@type": "rdfs:Class", "rdfs:label": "Widget"},
            {"@id": "ex:Gadget", "@type": "rdfs:Class", "rdfs:label": "Gadget"},
            {"@id": "ex:Tool", "@type": "rdfs:Class", "rdfs:label": "Tool"},
            {"@id": "ex:Part", "@type": "rdfs:Class", "rdfs:label": "Part"},
            {"@id": "ex:Assembly", "@type": "rdfs:Class", "rdfs:label": "Assembly"},
            {"@id": "ex:Component", "@type": "rdfs:Class", "rdfs:label": "Component"},
            {"@id": "ex:Module", "@type": "rdfs:Class", "rdfs:label": "Module"},
            {"@id": "ex:name", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}, "rdfs:domain": {"@id": "ex:Widget"}},
            {"@id": "ex:enabled", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:boolean"}, "rdfs:domain": {"@id": "ex:Widget"}},
            {"@id": "ex:priority", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:integer"}, "rdfs:domain": {"@id": "ex:Widget"}},
            {"@id": "ex:category", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}, "rdfs:domain": {"@id": "ex:Widget"}},
            {"@id": "ex:weight", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:decimal"}, "rdfs:domain": {"@id": "ex:Part"}}
        ]
    })
}

/// Test entity uses a DIFFERENT namespace (`urn:test:`) from the ontology, so
/// DictNovelty assigns it an s_id in a namespace with watermark=0.
fn test_entity() -> Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "urn:test:widget-1",
        "@type": "ex:Widget",
        "ex:name": "Test Widget",
        "ex:enabled": true,
        "ex:priority": 42
    })
}

const IRI: &str = "urn:test:widget-1";

fn test_context() -> Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

fn assert_has_properties(node: &Value, label: &str) {
    let keys: Vec<&String> = node.as_object().unwrap().keys().collect();
    assert!(
        keys.len() > 1,
        "{label}: select * should return properties beyond @id, got only: {keys:?}\nfull: {node}"
    );
}

// =============================================================================
// Memory-backed test (baseline — should always pass)
// =============================================================================

#[tokio::test]
async fn memory_select_star_novelty_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");
    let ontology = minimal_ontology();
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_entity();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert");

    let ctx = test_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "test:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = fluree
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "memory select * novelty only");
}

// =============================================================================
// File-backed: index gap tests (the regression condition)
// =============================================================================

/// Primary regression test: reindex after ontology, then insert entity
/// (novelty only), then query with a fresh reader instance.
#[tokio::test]
async fn file_select_star_after_index_gap() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");

    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    let ontology = minimal_ontology();
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    // Force index at t=1.
    let _index_result = fluree.reindex("test:main", ReindexOptions::default()).await;

    // t=2: insert entity (novelty only, NOT indexed).
    let entity = test_entity();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert entity");

    // Fresh reader instance (simulates Query Lambda / separate process).
    drop(fluree);
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = test_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "test:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from select *");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * after index gap");
}

/// Same as above but writer and reader are separate Fluree instances
/// (closer to cross-process pattern).
#[tokio::test]
async fn file_separate_reader_index_gap() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    // Writer
    {
        let writer = FlureeBuilder::file(path).build().expect("build writer");
        let ledger0 = writer.create_ledger("test:main").await.expect("create");
        let ontology = minimal_ontology();
        let receipt = writer.insert(ledger0, &ontology).await.expect("bootstrap");

        let _index_result = writer.reindex("test:main", ReindexOptions::default()).await;

        let entity = test_entity();
        let _receipt = writer
            .insert(receipt.ledger, &entity)
            .await
            .expect("insert entity");
    }
    // Writer dropped — all state on disk.

    // Reader (separate instance).
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = test_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "test:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file separate reader with index gap");
}

/// Control: insert entity BEFORE reindex — should work regardless.
#[tokio::test]
async fn file_select_star_no_index_gap() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    let ontology = minimal_ontology();
    let receipt = fluree.insert(ledger0, &ontology).await.expect("bootstrap");

    let entity = test_entity();
    let _receipt = fluree
        .insert(receipt.ledger, &entity)
        .await
        .expect("insert entity");

    // Reindex AFTER entity insert — entity is in the index, not just novelty.
    let _index_result = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Fresh reader.
    drop(fluree);
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let ctx = test_context();
    let query = json!({"@context": ctx, "select": { IRI: ["*"] }, "from": "test:main"});
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&query)
        .format(config)
        .execute_tracked()
        .await
        .expect("query_from");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_has_properties(node, "file select * no index gap (control)");
}
