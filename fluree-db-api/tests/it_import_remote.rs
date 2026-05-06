//! Integration tests for remote-source bulk import (`import_from_storage`).
//!
//! These tests use [`MemoryStorage`] as the remote source so they exercise the
//! `Arc<dyn StorageRead>` path without requiring LocalStack/S3. They verify
//! that remote import produces the same query results as the equivalent local
//! `Files` import.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, RemoteObject, RemoteSource};
use fluree_db_core::{MemoryStorage, StorageRead, StorageWrite};
use serde_json::json;
use std::sync::Arc;

/// Helper: extract a sorted list of string values from a single-column JSON-LD result.
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

const TTL_PREFIX: &str = "@prefix ex: <http://example.org/ns/> .\n\
@prefix schema: <http://schema.org/> .\n\
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

fn chunk_a() -> String {
    format!(
        "{TTL_PREFIX}\n\
        ex:alice a ex:User ;\n\
            schema:name \"Alice\" ;\n\
            schema:age 42 .\n\
        \n\
        ex:bob a ex:User ;\n\
            schema:name \"Bob\" ;\n\
            schema:age 22 .\n"
    )
}

fn chunk_b() -> String {
    format!(
        "{TTL_PREFIX}\n\
        ex:cam a ex:User ;\n\
            schema:name \"Cam\" ;\n\
            schema:age 34 ;\n\
            ex:friend ex:alice, ex:bob .\n\
        \n\
        ex:dave a ex:User ;\n\
            schema:name \"Dave\" ;\n\
            schema:age 28 .\n"
    )
}

async fn populate_remote_storage() -> (Arc<MemoryStorage>, Vec<RemoteObject>) {
    let storage = Arc::new(MemoryStorage::new());
    let chunks = [
        ("imports/chunk_0000.ttl", chunk_a()),
        ("imports/chunk_0001.ttl", chunk_b()),
    ];
    let mut objects = Vec::new();
    for (addr, body) in &chunks {
        storage.write_bytes(addr, body.as_bytes()).await.unwrap();
        objects.push(RemoteObject {
            address: (*addr).to_string(),
            size_bytes: body.len() as u64,
        });
    }
    (storage, objects)
}

// ============================================================================
// OrderedObjects mode — the production-recommended path
// ============================================================================

#[tokio::test]
async fn import_from_storage_ordered_objects_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let (storage, objects) = populate_remote_storage().await;

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let storage_dyn: Arc<dyn StorageRead> = storage.clone();
    let result = fluree
        .create("test/remote-ordered:main")
        .import_from_storage(storage_dyn, RemoteSource::OrderedObjects(objects))
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("remote import (OrderedObjects) should succeed");

    assert_eq!(result.t, 2, "two remote objects => t=2");
    assert!(result.flake_count > 0);
    assert!(result.root_id.is_some());

    let ledger = fluree
        .ledger("test/remote-ordered:main")
        .await
        .expect("load ledger");

    let q = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query names");
    let names = extract_sorted_strings(&qr.to_jsonld(&ledger.snapshot).unwrap());
    assert_eq!(names, vec!["Alice", "Bob", "Cam", "Dave"]);
}

// ============================================================================
// Prefix mode — lex-sorted listing via list_prefix_with_metadata
// ============================================================================

#[tokio::test]
async fn import_from_storage_prefix_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let (storage, _objects) = populate_remote_storage().await;

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let storage_dyn: Arc<dyn StorageRead> = storage.clone();
    let result = fluree
        .create("test/remote-prefix:main")
        .import_from_storage(
            storage_dyn,
            RemoteSource::Prefix {
                prefix: "imports/".into(),
            },
        )
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("remote import (Prefix) should succeed");

    assert_eq!(result.t, 2);
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/remote-prefix:main")
        .await
        .expect("load ledger");

    let q = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &q).await.unwrap();
    let names = extract_sorted_strings(&qr.to_jsonld(&ledger.snapshot).unwrap());
    assert_eq!(names, vec!["Alice", "Bob", "Cam", "Dave"]);
}

// ============================================================================
// Result parity: remote vs local should produce identical flake counts
// ============================================================================

#[tokio::test]
async fn remote_import_matches_local_flake_count() {
    // Local baseline.
    let db_local = tempfile::tempdir().unwrap();
    let chunks_dir = tempfile::tempdir().unwrap();
    std::fs::write(chunks_dir.path().join("chunk_0000.ttl"), chunk_a()).unwrap();
    std::fs::write(chunks_dir.path().join("chunk_0001.ttl"), chunk_b()).unwrap();

    let local_fluree = FlureeBuilder::file(db_local.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let local_result = local_fluree
        .create("test/parity-local:main")
        .import(chunks_dir.path())
        .threads(2)
        .cleanup(false)
        .execute()
        .await
        .expect("local import");

    // Remote equivalent.
    let db_remote = tempfile::tempdir().unwrap();
    let (storage, objects) = populate_remote_storage().await;

    let remote_fluree = FlureeBuilder::file(db_remote.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let storage_dyn: Arc<dyn StorageRead> = storage;
    let remote_result = remote_fluree
        .create("test/parity-remote:main")
        .import_from_storage(storage_dyn, RemoteSource::OrderedObjects(objects))
        .threads(2)
        .cleanup(false)
        .execute()
        .await
        .expect("remote import");

    assert_eq!(
        remote_result.flake_count, local_result.flake_count,
        "remote and local imports must produce identical flake counts on the same data"
    );
    assert_eq!(remote_result.t, local_result.t);
}

// ============================================================================
// Error path: producer fetch failure surfaces as ImportError
// ============================================================================

#[tokio::test]
async fn import_from_storage_propagates_fetch_error() {
    let db_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(MemoryStorage::new());

    // Reference an address that doesn't exist — read_bytes will fail.
    let bogus = vec![RemoteObject {
        address: "imports/missing.ttl".into(),
        size_bytes: 100,
    }];

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    let storage_dyn: Arc<dyn StorageRead> = storage;
    let err = fluree
        .create("test/remote-error:main")
        .import_from_storage(storage_dyn, RemoteSource::OrderedObjects(bogus))
        .threads(1)
        .cleanup(false)
        .execute()
        .await
        .expect_err("import should fail when remote object is missing");

    let msg = err.to_string();
    assert!(
        msg.contains("imports/missing.ttl"),
        "error should reference the missing address, got: {msg}"
    );
}

// ============================================================================
// JSON-LD round-trip
// ============================================================================

#[tokio::test]
async fn import_from_storage_jsonld_then_query() {
    let db_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(MemoryStorage::new());

    let alice = r#"{
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@id": "ex:alice",
        "@type": "ex:Person",
        "schema:name": "Alice"
    }"#;
    let bob = r#"{
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@id": "ex:bob",
        "@type": "ex:Person",
        "schema:name": "Bob"
    }"#;

    storage
        .write_bytes("jsonld/01_alice.jsonld", alice.as_bytes())
        .await
        .unwrap();
    storage
        .write_bytes("jsonld/02_bob.jsonld", bob.as_bytes())
        .await
        .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    let storage_dyn: Arc<dyn StorageRead> = storage;
    let result = fluree
        .create("test/remote-jsonld:main")
        .import_from_storage(
            storage_dyn,
            RemoteSource::Prefix {
                prefix: "jsonld/".into(),
            },
        )
        .cleanup(false)
        .execute()
        .await
        .expect("remote JSON-LD import should succeed");

    assert_eq!(result.t, 2);
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/remote-jsonld:main")
        .await
        .expect("load ledger");

    let q = json!({
        "@context": { "ex": "http://example.org/ns/", "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &q).await.unwrap();
    let names = extract_sorted_strings(&qr.to_jsonld(&ledger.snapshot).unwrap());
    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ============================================================================
// .trig rejection — assert the explicit error so we don't silently start
// importing TriG into a half-broken state if the resolver changes.
// ============================================================================
//
// The underlying TriG-via-import path has a documented upstream limitation
// (see `import_trig_commit` in fluree-db-transact/src/import.rs). Until that
// is fixed, import_from_storage rejects .trig with an actionable error message.

#[tokio::test]
async fn import_from_storage_rejects_trig_with_upstream_explanation() {
    let db_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(MemoryStorage::new());
    storage
        .write_bytes("trig/data.trig", b"# trig data")
        .await
        .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    let storage_dyn: Arc<dyn StorageRead> = storage;
    let err = fluree
        .create("test/remote-trig:main")
        .import_from_storage(
            storage_dyn,
            RemoteSource::Prefix {
                prefix: "trig/".into(),
            },
        )
        .cleanup(false)
        .execute()
        .await
        .expect_err(".trig should be rejected with an upstream-limitation error");

    let msg = err.to_string();
    assert!(
        msg.contains(".trig") && msg.contains("not currently supported"),
        "expected explicit not-supported message, got: {msg}"
    );
}

// ============================================================================
// Mixed-format rejection: Turtle + JSON-LD => ImportError::MixedFormats
// ============================================================================

#[tokio::test]
async fn import_from_storage_rejects_mixed_formats() {
    let db_dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(MemoryStorage::new());
    storage
        .write_bytes("mix/a.ttl", chunk_a().as_bytes())
        .await
        .unwrap();
    storage.write_bytes("mix/b.jsonld", b"{}").await.unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();

    let storage_dyn: Arc<dyn StorageRead> = storage;
    let err = fluree
        .create("test/remote-mixed:main")
        .import_from_storage(
            storage_dyn,
            RemoteSource::Prefix {
                prefix: "mix/".into(),
            },
        )
        .threads(1)
        .cleanup(false)
        .execute()
        .await
        .expect_err("mixed formats must be rejected");

    let msg = err.to_string();
    assert!(
        msg.contains("Turtle") && msg.contains("JSON-LD"),
        "expected MixedFormats error, got: {msg}"
    );
}
