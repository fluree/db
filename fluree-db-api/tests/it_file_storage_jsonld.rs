//! File storage integration test: insert then JSON-LD query.
//!
//! Mirrors a minimal end-to-end pattern:
//! - connect-file (here: `FlureeBuilder::file`)
//! - create empty ledger (genesis)
//! - insert JSON-LD data
//! - query via JSON-LD query syntax
//! - reload from file-backed nameservice + storage and re-run the query

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::json;

#[tokio::test]
async fn file_storage_jsonld_insert_then_query_roundtrip() {
    // Use tmpdir-backed file storage (stable + hermetic).
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    support::assert_index_defaults();

    let ledger_id = "rust-port/jsonld-insert-query:main";

    // Create a brand-new ledger state (genesis).
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db, Novelty::new(0));

    // Insert JSON-LD data.
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:age": 42
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:age": 22
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed");

    assert_eq!(committed.receipt.t, 1, "first commit should advance t to 1");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    // Query against the returned ledger
    let result0 = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("query should succeed");
    let json0 = result0
        .to_jsonld(&committed.ledger.snapshot)
        .expect("format to jsonld");

    // Reload ledger from file-backed nameservice+storage and re-run query
    let loaded = fluree.ledger(ledger_id).await.expect("reload ledger");
    assert_eq!(loaded.t(), 1);

    let result1 = support::query_jsonld(&fluree, &loaded, &query)
        .await
        .expect("query after reload");
    let json1 = result1
        .to_jsonld(&loaded.snapshot)
        .expect("format to jsonld");

    fn extract_names(v: &serde_json::Value) -> Vec<String> {
        let mut names = vec![];
        let rows = v.as_array().expect("rows should be an array");
        for row in rows {
            // 1-col SELECTs are formatted as a flat array of values; older tests used `[["Alice"], ...]`.
            let name = if let Some(cols) = row.as_array() {
                cols.first()
                    .and_then(|x| x.as_str())
                    .expect("name should be a string")
            } else {
                row.as_str().expect("name should be a string")
            };
            names.push(name.to_string());
        }
        names.sort();
        names
    }

    assert_eq!(
        extract_names(&json0),
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    assert_eq!(
        extract_names(&json1),
        vec!["Alice".to_string(), "Bob".to_string()]
    );
}

/// @json values survive file-storage commit round-trip.
///
/// File storage uses binary v2 format which preserves `FlakeValue::Json` via
/// `OTag::Json`. This test validates the happy path. The actual serde bug
/// (FlakeValue::Json → FlakeValue::String after JSON deserialization) is
/// covered by unit tests in `fluree-db-core/src/serde/json.rs` and
/// the formatter defense-in-depth tests in `format/jsonld.rs`.
#[tokio::test]
async fn file_storage_json_datatype_survives_commit_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    support::assert_index_defaults();

    let ledger_id = "rust-port/json-dt-roundtrip:main";
    let db = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db, Novelty::new(0));

    // Insert data with @json-typed value
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {
                "@id": "ex:config1",
                "ex:data": {
                    "@value": {"paths": ["dev", "src"], "debug": true},
                    "@type": "@json"
                }
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert+commit should succeed");

    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?data"],
        "where": {"@id": "ex:config1", "ex:data": "?data"}
    });

    // Query in-memory
    let result_mem = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("in-memory query should succeed");
    let json_mem = result_mem
        .to_jsonld(&committed.ledger.snapshot)
        .expect("format in-memory result");

    // Cold-start: fresh Fluree instance, same directory
    drop(fluree);
    let fluree2 = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fresh file-backed Fluree");

    let loaded = fluree2.ledger(ledger_id).await.expect("reload ledger");
    assert_eq!(loaded.t(), 1);

    let result_disk = support::query_jsonld(&fluree2, &loaded, &query)
        .await
        .expect("query after reload should succeed");
    let json_disk = result_disk
        .to_jsonld(&loaded.snapshot)
        .expect("format reloaded result");

    let extract_data = |v: &serde_json::Value| -> serde_json::Value {
        let rows = v.as_array().expect("expected array");
        let first = rows.first().expect("non-empty result");
        if let Some(cols) = first.as_array() {
            cols.first().cloned().expect("non-empty row")
        } else {
            first.clone()
        }
    };

    let expected = json!({"paths": ["dev", "src"], "debug": true});
    assert_eq!(extract_data(&json_mem), expected, "in-memory @json");
    assert_eq!(extract_data(&json_disk), expected, "reloaded @json");
}
