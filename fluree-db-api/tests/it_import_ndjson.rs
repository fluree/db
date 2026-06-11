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

/// Remote ndjson via `import_from_storage`: two objects in `MemoryStorage`,
/// each with its own leading `@context`, streamed (byte-range) and chained.
/// The second object uses a *different* prefix (`s:`) for schema.org, so a
/// correct result proves each object's own `@context` is applied (naive
/// concatenation would mis-resolve it).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_from_storage_ndjson_then_query() {
    use fluree_db_api::{RemoteObject, RemoteSource};
    use fluree_db_core::{MemoryStorage, StorageRead, StorageWrite};
    use std::sync::Arc;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let storage = Arc::new(MemoryStorage::new());

    let obj_a = concat!(
        "{\"@context\":{\"ex\":\"http://example.org/ns/\",\"schema\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"ex:alice\",\"schema:name\":\"Alice\"}\n",
        "{\"@id\":\"ex:bob\",\"schema:name\":\"Bob\"}\n",
    );
    let obj_b = concat!(
        "{\"@context\":{\"s\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"http://example.org/ns/cam\",\"s:name\":\"Cam\"}\n",
    );
    storage
        .write_bytes("imports/a.jsonl", obj_a.as_bytes())
        .await
        .unwrap();
    storage
        .write_bytes("imports/b.ndjson", obj_b.as_bytes())
        .await
        .unwrap();

    let objects = vec![
        RemoteObject {
            address: "imports/a.jsonl".to_string(),
            size_bytes: obj_a.len() as u64,
        },
        RemoteObject {
            address: "imports/b.ndjson".to_string(),
            size_bytes: obj_b.len() as u64,
        },
    ];

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let storage_dyn: Arc<dyn StorageRead> = storage.clone();
    let result = fluree
        .create("test/remote-ndjson:main")
        .import_from_storage(storage_dyn, RemoteSource::OrderedObjects(objects))
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("remote ndjson import should succeed");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/remote-ndjson:main")
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
    assert_eq!(extract_sorted_strings(&json), vec!["Alice", "Bob", "Cam"]);
}

/// A local DIRECTORY of `.jsonl` / `.ndjson` files, each carrying its OWN
/// `@context` on the first line. Exercises the directory dispatch in
/// `resolve_chunk_source` (all-ndjson dir → chained local producer) and proves
/// per-file contexts are preserved across the chain (file `1` uses a different
/// prefix and a fully-qualified IRI to reach the same `schema:name` property).
#[tokio::test]
async fn import_local_dir_of_ndjson_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    write_file(
        data_dir.path(),
        "0.jsonl",
        concat!(
            "{\"@context\":{\"ex\":\"http://example.org/ns/\",\"schema\":\"http://schema.org/\"}}\n",
            "{\"@id\":\"ex:alice\",\"schema:name\":\"Alice\"}\n",
        ),
    );
    write_file(
        data_dir.path(),
        "1.ndjson",
        concat!(
            "{\"@context\":{\"s\":\"http://schema.org/\"}}\n",
            "{\"@id\":\"http://example.org/ns/bob\",\"s:name\":\"Bob\"}\n",
        ),
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-ndjson-dir:main")
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("directory ndjson import should succeed");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-ndjson-dir:main")
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
    assert_eq!(extract_sorted_strings(&json), vec!["Alice", "Bob"]);
}

/// One file that is the concatenation of two ndjson files (`cat a.jsonl
/// b.jsonl`), each segment with its own leading `@context` using a different
/// prefix for schema.org. The mid-stream lone context must REPLACE the shared
/// context, so each segment's records resolve against their own context —
/// identical results to importing the two files separately.
#[tokio::test]
async fn import_concatenated_ndjson_segments_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ndjson = concat!(
        "{\"@context\":{\"schema\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"http://example.org/ns/ann\",\"schema:name\":\"Ann\"}\n",
        "{\"@context\":{\"s\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"http://example.org/ns/ben\",\"s:name\":\"Ben\"}\n",
    );
    let path = write_file(data_dir.path(), "concat.jsonl", ndjson);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let result = fluree
        .create("test/import-ndjson-concat:main")
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("concatenated ndjson import should succeed");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-ndjson-concat:main")
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
    assert_eq!(extract_sorted_strings(&json), vec!["Ann", "Ben"]);
}

/// A `.jsonl` source with no data lines (context-only) fails with a clear
/// up-front error — not an opaque "no commit head" storage error at the end
/// of the pipeline.
#[tokio::test]
async fn import_empty_ndjson_fails_with_clear_error() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let path = write_file(
        data_dir.path(),
        "empty.jsonl",
        "{\"@context\":{\"ex\":\"http://example.org/ns/\"}}\n",
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let err = fluree
        .create("test/import-ndjson-empty:main")
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect_err("context-only ndjson must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("no data records"),
        "expected clear empty-source error, got: {msg}"
    );
}

/// A gzip-compressed `.jsonl.gz` file streams through `open_decoded`
/// transparently.
#[tokio::test]
async fn import_gzipped_ndjson_then_query() {
    use std::io::Write as _;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ndjson = concat!(
        "{\"@context\":{\"ex\":\"http://example.org/ns/\",\"schema\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"ex:zed\",\"schema:name\":\"Zed\"}\n",
    );
    let path = data_dir.path().join("people.jsonl.gz");
    let f = std::fs::File::create(&path).expect("create gz file");
    let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    enc.write_all(ndjson.as_bytes()).expect("write gz");
    enc.finish().expect("finish gz");

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let result = fluree
        .create("test/import-ndjson-gz:main")
        .import(&path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("gzipped ndjson import should succeed");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-ndjson-gz:main")
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
    assert_eq!(extract_sorted_strings(&json), vec!["Zed"]);
}

/// Remote ndjson with `size_bytes: 0` (caller-supplied OrderedObjects without
/// sizes — the documented fallback when metadata listing is unsupported) must
/// fall back to a whole-object fetch and import the data, not silently treat
/// the object as empty. Runs on the default current_thread test runtime to
/// also exercise the serial loop's non-blocking chunk receive.
#[tokio::test]
async fn import_from_storage_ndjson_unknown_size_then_query() {
    use fluree_db_api::{RemoteObject, RemoteSource};
    use fluree_db_core::{MemoryStorage, StorageRead, StorageWrite};
    use std::sync::Arc;

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let storage = Arc::new(MemoryStorage::new());

    let obj = concat!(
        "{\"@context\":{\"schema\":\"http://schema.org/\"}}\n",
        "{\"@id\":\"http://example.org/ns/dana\",\"schema:name\":\"Dana\"}\n",
    );
    storage
        .write_bytes("imports/unsized.jsonl", obj.as_bytes())
        .await
        .unwrap();

    let objects = vec![RemoteObject {
        address: "imports/unsized.jsonl".to_string(),
        size_bytes: 0, // unknown — must not be treated as "empty object"
    }];

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let storage_dyn: Arc<dyn StorageRead> = storage.clone();
    let result = fluree
        .create("test/remote-ndjson-unsized:main")
        .import_from_storage(storage_dyn, RemoteSource::OrderedObjects(objects))
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("unknown-size remote ndjson import should succeed");
    assert!(result.flake_count > 0, "rows must not be silently dropped");

    let ledger = fluree
        .ledger("test/remote-ndjson-unsized:main")
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
    assert_eq!(extract_sorted_strings(&json), vec!["Dana"]);
}
