//! Regression tests for novelty retraction handling in JSON-LD graph crawl.
//!
//! When an entity is created and then updated (upserted) within novelty
//! (both transactions after the last index), the graph crawl `select *`
//! must only return the current (post-upsert) values, not both old and new.
//!
//! SPARQL SELECT handles this correctly because the query engine deduplicates
//! overlay facts. The JSON-LD graph crawl path goes through BinaryRangeProvider
//! which uses BinaryCursor overlay merge — and the cursor does not deduplicate
//! intra-overlay assert/retract pairs for the same fact.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, ReindexOptions};
use serde_json::{json, Value};

/// Minimal schema to populate the index before the test entity is introduced.
fn schema() -> Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:Task", "@type": "rdfs:Class", "rdfs:label": "Task"},
            {"@id": "ex:description", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}},
            {"@id": "ex:status", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}}
        ]
    })
}

fn test_context() -> Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

const TASK_IRI: &str = "urn:test:task-1";

fn assert_single_string_value(value: &Value, expected: &str, field: &str, node: &Value) {
    match value {
        Value::Array(arr) => {
            assert_eq!(
                arr.len(),
                1,
                "{field} should have exactly 1 value.\nGot: {value}\nFull node: {node}"
            );
            assert_single_string_value(&arr[0], expected, field, node);
        }
        Value::Object(obj) => {
            assert_eq!(
                obj.get("@value").and_then(Value::as_str),
                Some(expected),
                "{field} should match expected typed value.\nGot: {value}\nFull node: {node}"
            );
        }
        Value::String(s) => {
            assert_eq!(
                s, expected,
                "{field} should match expected string value.\nGot: {value}\nFull node: {node}"
            );
        }
        _ => panic!(
            "{field} should be a string, typed value, or single-element array.\nGot: {value}\nFull node: {node}"
        ),
    }
}

// =============================================================================
// Core regression test: upsert in novelty → graph crawl must show only new value
// =============================================================================

/// Reproduces the bug where JSON-LD `select *` returns both old and new values
/// after an upsert when both the original insert and the upsert are in novelty
/// (not yet indexed).
///
/// Scenario:
/// 1. Schema insert → reindex (creates binary index)
/// 2. Insert task with description "original" (novelty only)
/// 3. Upsert task with description "updated" (novelty only)
/// 4. JSON-LD `select {IRI: ["*"]}` should return ONLY "updated"
#[tokio::test]
async fn graph_crawl_applies_novelty_retractions() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // t=1: Insert schema
    let receipt = fluree.insert(ledger0, &schema()).await.expect("schema");

    // Force index at t=1 — all subsequent transactions are novelty-only.
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // t=2: Insert task entity (in novelty, not indexed).
    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree
        .insert(receipt.ledger, &insert_txn)
        .await
        .expect("insert task");

    // t=3: Upsert — update description (retract old + assert new in novelty).
    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert task");

    // -------------------------------------------------------------------------
    // SPARQL SELECT (control): should return only the updated value.
    // -------------------------------------------------------------------------
    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
        SELECT ?desc WHERE {{ <{TASK_IRI}> ex:description ?desc }}",
    );
    let sparql_result = support::query_sparql(&fluree, &receipt.ledger, &sparql)
        .await
        .expect("sparql query");
    let sparql_json = sparql_result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format sparql");

    let rows = sparql_json.as_array().expect("sparql rows");
    assert_eq!(
        rows.len(),
        1,
        "SPARQL should return exactly 1 row, got {}: {:?}",
        rows.len(),
        rows
    );
    // SPARQL always returns array-of-arrays (one array per binding row).
    assert_eq!(
        rows[0],
        json!(["updated description"]),
        "SPARQL should return only the updated description"
    );

    // -------------------------------------------------------------------------
    // JSON-LD graph crawl: select {IRI: ["*"]}
    // -------------------------------------------------------------------------
    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let crawl_result = fluree
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl query");

    let formatted = serde_json::to_value(&crawl_result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    // The description should be ONLY the updated value, not an array of both.
    assert_single_string_value(
        &node["ex:description"],
        "updated description",
        "description",
        node,
    );

    // Status should be unchanged.
    assert_single_string_value(&node["ex:status"], "pending", "status", node);
}

/// Scenario where the original value is already indexed (in the binary index),
/// and the update happens in novelty. This exercises the cursor merge path
/// (base rows + overlay retract/assert) rather than the overlay-only path.
#[tokio::test]
async fn graph_crawl_applies_novelty_retractions_for_indexed_base_rows() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // t=1: Insert schema and index it.
    let receipt = fluree.insert(ledger0, &schema()).await.expect("schema");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // t=2: Insert task and index it so the old value lives in the base index.
    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree
        .insert(receipt.ledger, &insert_txn)
        .await
        .expect("insert task");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // t=3: Upsert — update description in novelty (retract old + assert new).
    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert task");

    // SPARQL SELECT (control): should return only the updated value.
    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
        SELECT ?desc WHERE {{ <{TASK_IRI}> ex:description ?desc }}",
    );
    let sparql_result = support::query_sparql(&fluree, &receipt.ledger, &sparql)
        .await
        .expect("sparql query");
    let sparql_json = sparql_result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format sparql");
    let rows = sparql_json.as_array().expect("sparql rows");
    assert_eq!(
        rows.len(),
        1,
        "SPARQL should return exactly 1 row, got {}: {:?}",
        rows.len(),
        rows
    );
    assert_eq!(
        rows[0],
        json!(["updated description"]),
        "SPARQL should return only the updated description"
    );

    // JSON-LD graph crawl: select {IRI: ["*"]} should also return only the updated value.
    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let crawl_result = fluree
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl query");

    let formatted = serde_json::to_value(&crawl_result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_single_string_value(
        &node["ex:description"],
        "updated description",
        "description",
        node,
    );
}

/// Multi-valued property case: descriptions stored with list indices.
///
/// This is a closer match for real-world system properties where repeated
/// updates can result in list metadata (`@list` / array semantics). Retractions
/// must match the exact fact identity including list index, otherwise stale
/// values will leak from the base index.
#[tokio::test]
async fn graph_crawl_applies_novelty_retractions_for_list_indexed_values() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // Seed schema + index.
    let receipt = fluree.insert(ledger0, &schema()).await.expect("schema");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Insert two descriptions (array) and index them so list-indexed base rows exist.
    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": ["desc a", "desc b"],
        "ex:status": "pending"
    });
    let receipt = fluree
        .insert(receipt.ledger, &insert_txn)
        .await
        .expect("insert task");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Upsert with a single new description value.
    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "desc c"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert task");

    // SPARQL SELECT (control): should return only the updated value.
    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
        SELECT ?desc WHERE {{ <{TASK_IRI}> ex:description ?desc }}",
    );
    let sparql_result = support::query_sparql(&fluree, &receipt.ledger, &sparql)
        .await
        .expect("sparql query");
    let sparql_json = sparql_result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format sparql");
    let rows = sparql_json.as_array().expect("sparql rows");
    assert_eq!(
        rows,
        &vec![json!(["desc c"])],
        "SPARQL should return only the updated single description"
    );

    // JSON-LD graph crawl should also contain only one description value.
    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let crawl_result = fluree
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl query");
    let formatted = serde_json::to_value(&crawl_result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_single_string_value(&node["ex:description"], "desc c", "description", node);
}

/// Regression: graph crawl must not drop novelty assertions when V3 overlay translation
/// fails due to missing/invalid dictionary state.
///
/// This simulates a production failure mode where the reader has a binary index
/// but its `DictNovelty` is not populated with novelty-only strings. In that case
/// overlay translation of asserted string values can fail (`NotFound`) while
/// translation of retractions for indexed (old) string values still succeeds.
/// If we silently drop the failed assertion, the property disappears entirely.
#[tokio::test]
async fn graph_crawl_does_not_drop_overlay_assertions_when_dict_novelty_missing() {
    use std::sync::Arc;

    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    // Seed schema + index.
    let receipt = fluree.insert(ledger0, &schema()).await.expect("schema");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Insert task and index it so "original description" is in persisted dicts.
    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree
        .insert(receipt.ledger, &insert_txn)
        .await
        .expect("insert task");
    let _index = fluree.reindex("test:main", ReindexOptions::default()).await;

    // Upsert in novelty with a NEW string value that is not in the persisted string dict.
    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description (novelty-only string)"
    });
    let _receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert");

    // Load a fresh reader state so the binary index + range_provider are attached.
    let loaded = fluree.ledger("test:main").await.expect("load ledger");

    // Mutate the ledger snapshot to use a BinaryRangeProvider with an uninitialized DictNovelty.
    // This forces V3 overlay translation of the NEW asserted string value to fail.
    let mut ledger = loaded;
    let Some(provider) = ledger.snapshot.range_provider.as_ref() else {
        panic!("expected range_provider to be attached after reindex");
    };
    let brp = provider
        .as_ref()
        .as_any()
        .downcast_ref::<fluree_db_query::BinaryRangeProvider>()
        .expect("range_provider should be BinaryRangeProvider in native tests");

    let store = Arc::clone(brp.store());
    let bad_dn = Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized());
    let runtime_small_dicts = Arc::clone(brp.runtime_small_dicts());
    let ns_fallback = Some(Arc::new(ledger.snapshot.namespaces().clone()));
    ledger.snapshot.range_provider = Some(Arc::new(fluree_db_query::BinaryRangeProvider::new(
        store,
        bad_dn,
        runtime_small_dicts,
        ns_fallback,
    )));

    // Graph crawl should still return the updated value (correctness over speed).
    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();

    let db = support::graphdb_from_ledger(&ledger);
    let result = fluree.query(&db, &crawl_query).await.expect("query");
    let formatted = result
        .format_async(db.as_graph_db_ref(), &config)
        .await
        .expect("format graph crawl");

    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_single_string_value(
        &node["ex:description"],
        "updated description (novelty-only string)",
        "description",
        node,
    );
}

/// Same test but with a fresh reader instance (simulates Lambda/separate process).
/// This is closer to the production scenario where the reader loads state from storage.
#[tokio::test]
async fn graph_crawl_novelty_retractions_fresh_reader() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    // Writer: create schema, reindex, insert, upsert.
    {
        let writer = FlureeBuilder::file(path).build().expect("build writer");
        let ledger0 = writer.create_ledger("test:main").await.expect("create");

        let receipt = writer.insert(ledger0, &schema()).await.expect("schema");
        let _index = writer.reindex("test:main", ReindexOptions::default()).await;

        let insert_txn = json!({
            "@context": test_context(),
            "@id": TASK_IRI,
            "@type": "ex:Task",
            "ex:description": "original description",
            "ex:status": "pending"
        });
        let receipt = writer
            .insert(receipt.ledger, &insert_txn)
            .await
            .expect("insert");

        let upsert_txn = json!({
            "@context": test_context(),
            "@id": TASK_IRI,
            "ex:description": "updated description"
        });
        let _receipt = writer
            .upsert(receipt.ledger, &upsert_txn)
            .await
            .expect("upsert");
    }
    // Writer dropped.

    // Reader: fresh instance from storage.
    let reader = FlureeBuilder::file(path).build().expect("build reader");

    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = reader
        .query_from()
        .jsonld(&crawl_query)
        .format(config)
        .execute_tracked()
        .await
        .expect("graph crawl");

    let formatted = serde_json::to_value(&result.result).expect("serialize");
    let node = formatted
        .as_array()
        .and_then(|arr| arr.first())
        .expect("should return one result");

    assert_single_string_value(
        &node["ex:description"],
        "updated description",
        "description",
        node,
    );
}

/// Memory-backed baseline: same scenario without binary index.
/// This should always pass because the overlay-only path uses `remove_stale_flakes`.
#[tokio::test]
async fn memory_graph_crawl_novelty_retractions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("test:main").await.expect("create");

    let insert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "@type": "ex:Task",
        "ex:description": "original description",
        "ex:status": "pending"
    });
    let receipt = fluree.insert(ledger0, &insert_txn).await.expect("insert");

    let upsert_txn = json!({
        "@context": test_context(),
        "@id": TASK_IRI,
        "ex:description": "updated description"
    });
    let receipt = fluree
        .upsert(receipt.ledger, &upsert_txn)
        .await
        .expect("upsert");

    let crawl_query = json!({
        "@context": test_context(),
        "select": { TASK_IRI: ["*"] },
        "from": "test:main"
    });
    let result = support::query_jsonld(&fluree, &receipt.ledger, &crawl_query)
        .await
        .expect("query");
    let jsonld = result
        .to_jsonld_async(receipt.ledger.as_graph_db_ref(0))
        .await
        .expect("format");

    let node = jsonld
        .as_array()
        .and_then(|arr| arr.first())
        .expect("one result");

    assert_eq!(
        node["ex:description"],
        json!("updated description"),
        "Memory-backed graph crawl should return only the updated description.\n\
         Got: {}\nFull: {}",
        node["ex:description"],
        node
    );
}
