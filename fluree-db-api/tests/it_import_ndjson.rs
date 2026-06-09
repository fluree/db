//! Integration tests for newline-delimited JSON-LD (ndjson / jsonl) bulk import.
//!
//! Exercises the full `fluree.create("db").import("data.jsonl").execute()` path:
//! the `.jsonl` file is streamed by `NdjsonReader` into standalone JSON-LD
//! chunks, parsed on the serial json-ld path, indexed, and queried back.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::io::Write;

fn write_file(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create file");
    f.write_all(content.as_bytes()).expect("write file");
    path
}

fn extract_sorted_strings(v: &serde_json::Value) -> Vec<String> {
    let mut out: Vec<String> = v
        .as_array()
        .expect("expected array")
        .iter()
        .map(|row| {
            if let Some(arr) = row.as_array() {
                arr.first()
                    .and_then(|v| v.as_str())
                    .expect("expected string in first column")
                    .to_string()
            } else {
                row.as_str().expect("expected string row").to_string()
            }
        })
        .collect();
    out.sort();
    out
}

/// ndjson whose first line is a shared `@context`; remaining lines are nodes.
#[tokio::test]
async fn import_ndjson_with_leading_context_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ndjson = concat!(
        "{\"@context\":{\"ex\":\"http://example.org/ns/\",\"schema\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"ex:alice\",\"@type\":\"ex:User\",\"schema:name\":\"Alice\"}\n",
        "{\"@id\":\"ex:bob\",\"@type\":\"ex:User\",\"schema:name\":\"Bob\"}\n",
        "{\"@id\":\"ex:cam\",\"@type\":\"ex:User\",\"schema:name\":\"Cam\"}\n",
    );
    let path = write_file(data_dir.path(), "people.jsonl", ndjson);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-ndjson:main")
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("ndjson import should succeed");

    assert!(result.t > 0, "should have at least one commit");
    assert!(result.flake_count > 0, "should have flakes");
    assert!(result.root_id.is_some(), "index should have been built");

    let ledger = fluree
        .ledger("test/import-ndjson:main")
        .await
        .expect("load ledger after import");

    let query = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    assert_eq!(extract_sorted_strings(&json), vec!["Alice", "Bob", "Cam"]);
}

/// ndjson with no shared context line — every node carries full IRIs. Exercises
/// the `{"@graph":[…]}` (no `@context`) chunk-wrapping path.
#[tokio::test]
async fn import_ndjson_without_context_full_iris() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ndjson = concat!(
        "{\"@id\":\"http://example.org/ns/x\",\"http://schema.org/name\":\"Xavier\"}\n",
        "{\"@id\":\"http://example.org/ns/y\",\"http://schema.org/name\":\"Yara\"}\n",
    );
    let path = write_file(data_dir.path(), "people.ndjson", ndjson);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-ndjson-nocontext:main")
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("ndjson import should succeed");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-ndjson-nocontext:main")
        .await
        .expect("load ledger after import");

    let query = json!({
        "@context": { "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    assert_eq!(extract_sorted_strings(&json), vec!["Xavier", "Yara"]);
}
