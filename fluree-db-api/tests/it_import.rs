//! Integration tests for the bulk import pipeline (Tier 2: spool/merge/remap).
//!
//! These tests exercise the full `fluree.create("db").import(path).execute()` path
//! end-to-end: write TTL → import → query the resulting indexed ledger.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::{LedgerSnapshot, Sid};
use serde_json::json;
use std::io::Write;

/// Write a TTL string to a temp file and return the path.
fn write_ttl(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create ttl file");
    f.write_all(content.as_bytes()).expect("write ttl");
    path
}

/// Helper: extract a sorted list of string values from a single-column JSON-LD query result.
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

/// Extract column `n` (0-based) from each row of a multi-column JSON-LD result,
/// keeping only string values, sorted.
fn extract_nth_column(v: &serde_json::Value, n: usize) -> Vec<String> {
    let mut out: Vec<String> = v
        .as_array()
        .expect("expected array")
        .iter()
        .filter_map(|row| row.as_array().and_then(|cols| cols.get(n)))
        .filter_map(|c| c.as_str().map(str::to_string))
        .collect();
    out.sort();
    out
}

// ============================================================================
// Single-file import (streaming split)
// ============================================================================

#[tokio::test]
async fn import_single_ttl_file_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:age 42 .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:age 22 .

ex:cam a ex:User ;
    schema:name "Cam" ;
    schema:age 34 ;
    ex:friend ex:alice, ex:bob .
"#;

    let ttl_path = write_ttl(data_dir.path(), "people.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-single:main")
        .import(&ttl_path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");

    assert!(result.t > 0, "should have at least one commit");
    assert!(result.flake_count > 0, "should have flakes");
    assert!(result.root_id.is_some(), "index should have been built");

    // Load the ledger and query it
    let ledger = fluree
        .ledger("test/import-single:main")
        .await
        .expect("load ledger after import");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Alice", "Bob", "Cam"]);
}

// ============================================================================
// Pre-split chunk files import
// ============================================================================

#[tokio::test]
async fn import_pre_split_chunks_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let chunks_dir = tempfile::tempdir().expect("chunks tmpdir");

    let prefix = r"@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
";

    // Chunk 0: two people
    let chunk0 = format!(
        "{prefix}\n\
        ex:alice a ex:User ;\n\
            schema:name \"Alice\" ;\n\
            schema:age 42 .\n\
        \n\
        ex:bob a ex:User ;\n\
            schema:name \"Bob\" ;\n\
            schema:age 22 .\n"
    );

    // Chunk 1: two more people with refs to chunk 0 entities
    let chunk1 = format!(
        "{prefix}\n\
        ex:cam a ex:User ;\n\
            schema:name \"Cam\" ;\n\
            schema:age 34 ;\n\
            ex:friend ex:alice, ex:bob .\n\
        \n\
        ex:dave a ex:User ;\n\
            schema:name \"Dave\" ;\n\
            schema:age 28 .\n"
    );

    write_ttl(chunks_dir.path(), "chunk_0000.ttl", &chunk0);
    write_ttl(chunks_dir.path(), "chunk_0001.ttl", &chunk1);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-chunks:main")
        .import(chunks_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");

    assert_eq!(result.t, 2, "two chunks => t=2");
    assert!(result.flake_count > 0);
    assert!(result.root_id.is_some());

    // Load and query
    let ledger = fluree
        .ledger("test/import-chunks:main")
        .await
        .expect("load ledger");

    // Query all names
    let query_names = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query_names)
        .await
        .expect("query names");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);
    assert_eq!(names, vec!["Alice", "Bob", "Cam", "Dave"]);

    // Query cross-chunk refs: who are Cam's friends?
    let query_friends = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?friendName"],
        "where": [
            { "@id": "ex:cam", "ex:friend": "?friend" },
            { "@id": "?friend", "schema:name": "?friendName" }
        ]
    });

    let qr2 = support::query_jsonld(&fluree, &ledger, &query_friends)
        .await
        .expect("query friends");
    let json2 = qr2.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let friends = extract_sorted_strings(&json2);
    assert_eq!(friends, vec!["Alice", "Bob"]);
}

// ============================================================================
// Stats helpers
// ============================================================================

/// Look up the count for a property IRI in the snapshot's stats.
fn property_count(snapshot: &LedgerSnapshot, iri: &str) -> Option<u64> {
    let stats = snapshot.stats.as_ref()?;
    let props = stats.properties.as_ref()?;
    for p in props {
        let sid = Sid::new(p.sid.0, &p.sid.1);
        if let Some(full) = snapshot.decode_sid(&sid) {
            if full == iri {
                return Some(p.count);
            }
        }
    }
    None
}

// ============================================================================
// Import with stats collection
// ============================================================================

#[tokio::test]
async fn import_collects_stats() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:age 42 .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:age 22 .
"#;

    let ttl_path = write_ttl(data_dir.path(), "stats_test.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/import-stats:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(256)
        .collect_id_stats(true)
        .cleanup(false)
        .execute()
        .await
        .expect("import with stats");

    assert!(result.t > 0);
    assert!(result.flake_count > 0);
    assert!(result.root_id.is_some());

    // Load and verify stats are populated
    let ledger = fluree.ledger("test/import-stats:main").await.expect("load");

    // Stats should be present in the loaded LedgerSnapshot
    assert!(
        ledger.snapshot.stats.is_some(),
        "stats should be populated after import with collect_id_stats=true"
    );
    let stats = ledger.snapshot.stats.as_ref().unwrap();
    assert!(stats.flakes > 0, "should have flake count in stats");

    // Regression: `stats.size` and per-graph `graphs[*].size` must be wired
    // from the IndexRoot's `total_commit_size`. Without the
    // `distribute_total_size_by_flakes` call in the import path, both
    // surface as 0 in `info` even though the commit blobs do have bytes.
    assert!(
        stats.size > 0,
        "stats.size should reflect total commit blob bytes, got 0"
    );
    let graphs = stats.graphs.as_ref().expect("graphs should be present");
    let default_graph_size = graphs
        .iter()
        .find(|g| g.g_id == 0)
        .map(|g| g.size)
        .unwrap_or(0);
    assert!(
        default_graph_size > 0,
        "default graph (g_id=0) size should be > 0"
    );

    // Property stats: schema:name should have count=2 (Alice, Bob)
    let name_count = property_count(&ledger.snapshot, "http://schema.org/name");
    assert_eq!(name_count, Some(2), "schema:name should have count=2");

    // Property stats: schema:age should have count=2
    let age_count = property_count(&ledger.snapshot, "http://schema.org/age");
    assert_eq!(age_count, Some(2), "schema:age should have count=2");

    // Class stats: currently disabled (see build_and_upload `if true` guard).
    // When re-enabled, ex:User should have count=2.
    // let user_count = class_count(&ledger.snapshot, "http://example.org/ns/User");
    // assert_eq!(user_count, Some(2), "ex:User class should have count=2");

    // Basic query still works
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let names = extract_sorted_strings(&json);
    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ============================================================================
// Import with multiple data types
// ============================================================================

#[tokio::test]
async fn import_handles_diverse_datatypes() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:age 42 ;
    schema:birthDate "1982-03-15"^^xsd:date ;
    ex:score 98.5 ;
    ex:active true .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:age 22 ;
    schema:birthDate "2002-07-04"^^xsd:date ;
    ex:score 75.0 ;
    ex:active false .
"#;

    let ttl_path = write_ttl(data_dir.path(), "datatypes.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/import-datatypes:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import diverse datatypes");

    assert!(result.t > 0);
    assert!(result.root_id.is_some());

    let ledger = fluree
        .ledger("test/import-datatypes:main")
        .await
        .expect("load");

    // Query names (string property)
    let query_names = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query_names)
        .await
        .expect("query names");
    let json = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let names = extract_sorted_strings(&json);
    assert_eq!(names, vec!["Alice", "Bob"]);

    // Query integer filter: people older than 30
    let query_age_filter = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": [
            { "schema:name": "?name", "schema:age": "?age" },
            ["filter", "(> ?age 30)"]
        ]
    });

    let qr2 = support::query_jsonld(&fluree, &ledger, &query_age_filter)
        .await
        .expect("query age filter");
    let json2 = qr2.to_jsonld(&ledger.snapshot).expect("jsonld");
    let older = extract_sorted_strings(&json2);
    assert_eq!(older, vec!["Alice"], "only Alice is older than 30");

    // Query boolean: active users
    let query_active = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": [
            { "schema:name": "?name", "ex:active": "?a" },
            ["filter", "(= ?a true)"]
        ]
    });

    let qr3 = support::query_jsonld(&fluree, &ledger, &query_active)
        .await
        .expect("query active");
    let json3 = qr3.to_jsonld(&ledger.snapshot).expect("jsonld");
    let active = extract_sorted_strings(&json3);
    assert_eq!(active, vec!["Alice"], "only Alice is active");

    // Query float comparison: high scorers (> 80)
    let query_score = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": [
            { "schema:name": "?name", "ex:score": "?s" },
            ["filter", "(> ?s 80)"]
        ]
    });

    let qr4 = support::query_jsonld(&fluree, &ledger, &query_score)
        .await
        .expect("query score");
    let json4 = qr4.to_jsonld(&ledger.snapshot).expect("jsonld");
    let high_scorers = extract_sorted_strings(&json4);
    assert_eq!(high_scorers, vec!["Alice"], "only Alice scores above 80");
}

// ============================================================================
// txn-meta graph queries after import
// ============================================================================

#[tokio::test]
async fn import_txn_meta_queryable() {
    // After bulk import, the txn-meta graph (g_id=1) should contain
    // commit metadata: db:address, db:time, db:t, db:size, db:asserts, db:retracts.
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice a ex:User ;
    schema:name "Alice" .

ex:bob a ex:User ;
    schema:name "Bob" .
"#;

    let ttl_path = write_ttl(data_dir.path(), "people.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-txn-meta:main")
        .import(&ttl_path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");

    assert!(result.t > 0, "should have at least one commit");
    assert!(result.root_id.is_some(), "index should have been built");

    // Query the txn-meta graph via db (same path as CLI: fluree.db("alias#txn-meta"))
    let view = fluree
        .db("test/import-txn-meta:main#txn-meta")
        .await
        .expect("load txn-meta view");

    assert_eq!(view.graph_id, 1, "txn-meta should use g_id=1");
    assert!(
        view.binary_store().is_some(),
        "binary store should be loaded"
    );

    // Query all triples in the txn-meta graph
    let sparql = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    let qr = fluree.query(&view, sparql).await.expect("query txn-meta");

    assert!(
        qr.row_count() > 0,
        "txn-meta graph should have commit metadata rows, got 0"
    );

    // Each chunk produces one commit subject with db:t, db:address, etc.
    // Small TTL = 1 chunk = at least 6 properties (db:t, db:address, db:time, db:size, db:asserts, db:retracts)
    assert!(
        qr.row_count() >= 6,
        "expected >= 6 txn-meta triples, got {}",
        qr.row_count()
    );
}

// ============================================================================
// Directory import without chunk_ prefix
// ============================================================================

#[tokio::test]
async fn import_directory_without_chunk_prefix() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let prefix = "@prefix ex: <http://example.org/ns/> .\n\
                  @prefix schema: <http://schema.org/> .\n";

    let file_a = format!(
        "{prefix}\n\
         ex:alice a ex:User ;\n\
             schema:name \"Alice\" .\n"
    );
    let file_b = format!(
        "{prefix}\n\
         ex:bob a ex:User ;\n\
             schema:name \"Bob\" .\n"
    );

    write_ttl(data_dir.path(), "a_people.ttl", &file_a);
    write_ttl(data_dir.path(), "b_people.ttl", &file_b);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-noprefix:main")
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed without chunk_ prefix");

    assert_eq!(result.t, 2, "two files => t=2");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-noprefix:main")
        .await
        .expect("load ledger");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ============================================================================
// JSON-LD import tests
// ============================================================================

#[tokio::test]
async fn import_jsonld_directory_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    std::fs::write(
        data_dir.path().join("01_alice.jsonld"),
        r#"{
            "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
            "@id": "ex:alice",
            "@type": "ex:Person",
            "schema:name": "Alice"
        }"#,
    )
    .unwrap();
    std::fs::write(
        data_dir.path().join("02_bob.jsonld"),
        r#"{
            "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
            "@id": "ex:bob",
            "@type": "ex:Person",
            "schema:name": "Bob"
        }"#,
    )
    .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-jsonld-dir:main")
        .import(data_dir.path())
        .cleanup(false)
        .execute()
        .await
        .expect("JSON-LD directory import should succeed");

    assert_eq!(result.t, 2, "two .jsonld files => t=2");
    assert!(result.flake_count > 0, "expected flakes, got 0");

    let ledger = fluree
        .ledger("test/import-jsonld-dir:main")
        .await
        .expect("load ledger");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after JSON-LD import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn import_single_jsonld_file_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let file_path = data_dir.path().join("people.jsonld");
    std::fs::write(
        &file_path,
        r#"{
            "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
            "@graph": [
                {"@id": "ex:carol", "@type": "ex:Person", "schema:name": "Carol"},
                {"@id": "ex:dave", "@type": "ex:Person", "schema:name": "Dave"}
            ]
        }"#,
    )
    .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-jsonld-single:main")
        .import(&file_path)
        .cleanup(false)
        .execute()
        .await
        .expect("single JSON-LD file import should succeed");

    assert_eq!(result.t, 1, "single file => t=1");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-jsonld-single:main")
        .await
        .expect("load ledger");

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after single JSON-LD import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Carol", "Dave"]);
}

/// Regression: serial Turtle path (threads=0) must produce queryable results.
#[tokio::test]
async fn import_serial_turtle_then_query() {
    let db_dir = tempfile::tempdir().expect("db");
    let data_dir = tempfile::tempdir().expect("data");

    write_ttl(
        data_dir.path(),
        "a.ttl",
        "@prefix ex: <http://example.org/ns/> .\n@prefix schema: <http://schema.org/> .\nex:alice a ex:Person ; schema:name \"Alice\" .\n",
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/serial-ttl:main")
        .import(data_dir.path())
        .threads(0) // Force serial path
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    assert_eq!(result.t, 1);
    assert!(result.flake_count > 0);

    let ledger = fluree.ledger("test/serial-ttl:main").await.expect("load");
    let query = json!({
        "@context": { "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });
    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_result = qr.to_jsonld(&ledger.snapshot).expect("format");
    let names = extract_sorted_strings(&json_result);
    assert_eq!(names, vec!["Alice"]);
}

/// Regression: after directory import, a subsequent insert with a custom namespace
/// predicate must be queryable by full IRI in SPARQL. Previously, the predicate
/// filter was silently dropped (acting as a wildcard) because the overlay-only
/// bounds code used `store.sid_to_p_id()` which only checks the persisted index,
/// returning None for novelty-only predicates and widening the scan to all p_ids.
#[tokio::test]
async fn import_then_insert_custom_ns_predicate_matches_sparql() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // Phase 1: Create import data with a custom namespace
    std::fs::write(
        data_dir.path().join("01_schema.jsonld"),
        r#"{
            "@context": {
                "skos": "http://www.w3.org/2004/02/skos/core#",
                "sh": "http://www.w3.org/ns/shacl#",
                "cust": "https://taxo.cbcrc.ca/ns/"
            },
            "@graph": [
                {
                    "@id": "cust:shape/ConceptShape",
                    "@type": "sh:NodeShape",
                    "sh:targetClass": {"@id": "skos:Concept"}
                }
            ]
        }"#,
    )
    .unwrap();
    std::fs::write(
        data_dir.path().join("02_data.jsonld"),
        r#"{
            "@context": {"skos": "http://www.w3.org/2004/02/skos/core#"},
            "@graph": [
                {"@id": "http://example.org/c1", "@type": "skos:Concept", "skos:prefLabel": "One"},
                {"@id": "http://example.org/c2", "@type": "skos:Concept", "skos:prefLabel": "Two"}
            ]
        }"#,
    )
    .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Phase 2: Import directory
    let import_result = fluree
        .create("test/import-ns-bug:main")
        .import(data_dir.path())
        .cleanup(false)
        .execute()
        .await
        .expect("directory import should succeed");

    assert!(import_result.flake_count > 0);

    // Phase 3: Insert data with a custom namespace predicate
    let ledger = fluree
        .ledger("test/import-ns-bug:main")
        .await
        .expect("load ledger after import");

    let insert_data = json!({
        "@context": {"cust": "https://taxo.cbcrc.ca/ns/"},
        "@id": "http://example.org/assoc1",
        "@type": "cust:CoveragePackage",
        "cust:packageType": "test-pkg"
    });
    let insert_result = fluree.insert(ledger, &insert_data).await.expect("insert");
    assert!(insert_result.receipt.flake_count > 0);

    // Phase 4: Reload ledger and query with SPARQL using the full predicate IRI.
    // The predicate <https://taxo.cbcrc.ca/ns/packageType> must match ONLY the
    // packageType triple, not all triples for the subject.
    let ledger = fluree
        .ledger("test/import-ns-bug:main")
        .await
        .expect("reload ledger");

    let sparql = r"SELECT ?o WHERE {
        <http://example.org/assoc1> <https://taxo.cbcrc.ca/ns/packageType> ?o
    }";

    let qr = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("SPARQL query with custom namespace predicate");

    let json = qr.to_sparql_json(&ledger.snapshot).expect("format json");
    let bindings = json["results"]["bindings"]
        .as_array()
        .expect("bindings array");

    // Must return exactly 1 row (the packageType triple), not all triples for the subject
    assert_eq!(
        bindings.len(),
        1,
        "Expected 1 binding for packageType, got {}: {:?}",
        bindings.len(),
        bindings
    );

    let value = bindings[0]["o"]["value"]
        .as_str()
        .expect("binding value string");
    assert_eq!(value, "test-pkg");
}

// ============================================================================
// Negative: malformed JSON-LD in directory import
// ============================================================================

/// A directory containing a valid `.jsonld` alongside a malformed one must
/// fail with a clear error rather than silently skipping or panicking.
#[tokio::test]
async fn import_jsonld_directory_with_malformed_file_errors() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // Valid file
    std::fs::write(
        data_dir.path().join("01_valid.jsonld"),
        r#"{
            "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
            "@id": "ex:alice",
            "@type": "ex:Person",
            "schema:name": "Alice"
        }"#,
    )
    .unwrap();

    // Malformed: not valid JSON at all
    std::fs::write(
        data_dir.path().join("02_bad.jsonld"),
        r"{ this is not valid json @@@ ",
    )
    .unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let err = fluree
        .create("test/import-jsonld-bad:main")
        .import(data_dir.path())
        .cleanup(false)
        .execute()
        .await
        .expect_err("import of directory with malformed JSON-LD should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("transact")
            || msg.contains("parse")
            || msg.contains("JSON")
            || msg.contains("json"),
        "expected a parse/transact error for malformed JSON-LD, got: {msg}"
    );
}

/// A single malformed `.jsonld` file (not in a directory) must also fail cleanly.
#[tokio::test]
async fn import_single_malformed_jsonld_file_errors() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let bad_path = data_dir.path().join("bad.jsonld");
    std::fs::write(&bad_path, r"{ not json !!!").unwrap();

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let err = fluree
        .create("test/import-jsonld-single-bad:main")
        .import(&bad_path)
        .cleanup(false)
        .execute()
        .await
        .expect_err("import of malformed single JSON-LD file should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("transact")
            || msg.contains("parse")
            || msg.contains("JSON")
            || msg.contains("json"),
        "expected a parse/transact error for malformed JSON-LD, got: {msg}"
    );
}

// ============================================================================
// N-Triples (.nt) import tests
//
// N-Triples is a strict subset of Turtle, so `.nt` files dispatch to the same
// Turtle parser. These verify the import pipeline discovers and parses `.nt`
// for both single-file and directory imports.
// ============================================================================

/// Write a string to a temp file with the given name and return the path.
fn write_data(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create data file");
    f.write_all(content.as_bytes()).expect("write data");
    path
}

#[tokio::test]
async fn import_single_nt_file_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // N-Triples: full IRIs, one triple per line.
    let nt = "\
<http://example.org/ns/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns/User> .
<http://example.org/ns/alice> <http://schema.org/name> \"Alice\" .
<http://example.org/ns/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/ns/User> .
<http://example.org/ns/bob> <http://schema.org/name> \"Bob\" .
";

    let nt_path = write_data(data_dir.path(), "people.nt", nt);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-nt-single:main")
        .import(&nt_path)
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("single .nt import should succeed");

    assert!(result.flake_count > 0, "should have flakes");
    assert!(result.root_id.is_some(), "index should have been built");

    let ledger = fluree
        .ledger("test/import-nt-single:main")
        .await
        .expect("load ledger after .nt import");

    let query = json!({
        "@context": { "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after .nt import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn import_nt_directory_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // Two .nt files with disjoint subjects. Directory discovery must recognize
    // the `.nt` extension (previously only .ttl/.trig/.jsonld were discovered).
    write_data(
        data_dir.path(),
        "a_people.nt",
        "<http://example.org/ns/alice> <http://schema.org/name> \"Alice\" .\n",
    );
    write_data(
        data_dir.path(),
        "b_people.nt",
        "<http://example.org/ns/bob> <http://schema.org/name> \"Bob\" .\n",
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/import-nt-dir:main")
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("directory .nt import should succeed");

    assert_eq!(result.t, 2, "two .nt files => t=2");
    assert!(result.flake_count > 0);

    let ledger = fluree
        .ledger("test/import-nt-dir:main")
        .await
        .expect("load ledger");

    let query = json!({
        "@context": { "schema": "http://schema.org/" },
        "select": ["?name"],
        "where": { "schema:name": "?name" }
    });

    let qr = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query after .nt directory import");
    let json = qr.to_jsonld(&ledger.snapshot).expect("format jsonld");
    let names = extract_sorted_strings(&json);

    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ============================================================================
// TriG named-graph bulk import
//
// Verifies that named-graph (GRAPH block) data imported via the *bulk* path
// (create().import(.trig)) is spooled into the Tier-2 index and is queryable
// via the `#<graph-iri>` fragment — not just default-graph triples.
// ============================================================================

#[tokio::test]
async fn import_trig_named_graph_is_queryable() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let trig = r#"@prefix ex: <http://example.org/> .
@prefix schema: <http://schema.org/> .

ex:alice schema:name "Alice" .

GRAPH <http://example.org/graphs/audit> {
    ex:event1 schema:description "User login" .
    ex:event2 schema:description "User logout" .
}
"#;

    let path = data_dir.path().join("data.trig");
    {
        let mut f = std::fs::File::create(&path).expect("create trig");
        f.write_all(trig.as_bytes()).expect("write trig");
    }

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/trig-named:main")
        .import(&path)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("trig import should succeed");
    assert!(result.flake_count >= 3, "default + 2 named-graph flakes");

    let ledger = fluree
        .ledger("test/trig-named:main")
        .await
        .expect("load ledger");

    // Default graph: broad triple scan must include Alice's name.
    let q_default = json!({
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let qr = support::query_jsonld(&fluree, &ledger, &q_default)
        .await
        .expect("default-graph query");
    let json_default = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let default_objs = extract_nth_column(&json_default, 2);
    assert!(
        default_objs.contains(&"Alice".to_string()),
        "default graph must contain Alice; got {json_default}"
    );

    // Named graph: the audit events are queryable via the #<iri> fragment.
    // This is the behavior the fix enables — previously this errored with
    // "Unknown named graph" because named-graph flakes never reached the index.
    let named_alias = "test/trig-named:main#http://example.org/graphs/audit";
    let q_named = json!({
        "from": named_alias,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let qr = fluree
        .query_connection(&q_named)
        .await
        .expect("named-graph query should resolve and return data");
    let json_named = qr.to_jsonld(&ledger.snapshot).expect("jsonld");
    let named_objs = extract_nth_column(&json_named, 2);
    assert_eq!(
        named_objs,
        vec!["User login", "User logout"],
        "named-graph data must be indexed and queryable after bulk import; got {json_named}"
    );
}

// ============================================================================
// N-Quads (.nq) import
//
// N-Quads is converted to TriG and dispatched through the named-graph-aware
// TriG path. Default-graph quads and named-graph (4th-term) quads must both
// import and be queryable.
// ============================================================================

#[tokio::test]
async fn import_nquads_default_and_named_graph() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // N-Quads: 3-term lines = default graph; 4-term lines = named graph.
    let nq = "\
<http://example.org/alice> <http://schema.org/name> \"Alice\" .
<http://example.org/event1> <http://schema.org/description> \"User login\" <http://example.org/graphs/audit> .
<http://example.org/event2> <http://schema.org/description> \"User logout\" <http://example.org/graphs/audit> .
";

    let nq_path = write_data(data_dir.path(), "data.nq", nq);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/nq:main")
        .import(&nq_path)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("single .nq import should succeed");
    assert!(result.flake_count >= 3, "1 default + 2 named-graph quads");

    let ledger = fluree.ledger("test/nq:main").await.expect("load ledger");

    // Default graph contains Alice.
    let q_default = json!({
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let qr = support::query_jsonld(&fluree, &ledger, &q_default)
        .await
        .expect("default-graph query");
    assert!(
        extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2)
            .contains(&"Alice".to_string()),
        "default graph must contain Alice"
    );

    // Named graph (the 4th-term graph label) is queryable via the #<iri> fragment.
    let named_alias = "test/nq:main#http://example.org/graphs/audit";
    let q_named = json!({
        "from": named_alias,
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let qr = fluree
        .query_connection(&q_named)
        .await
        .expect("named-graph query should resolve and return data");
    assert_eq!(
        extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2),
        vec!["User login", "User logout"],
        "N-Quads named-graph data must be indexed and queryable"
    );
}

// ============================================================================
// Compressed inputs (.gz / .zst)
//
// Transparent decompression for `.ttl.gz`, `.nt.gz`, `.nq.gz`, `.ttl.zst`, etc.
// The streaming-split path is exercised by the large-file test; small files
// run through the `Files` single-chunk path.
// ============================================================================

fn write_gzipped(dir: &std::path::Path, name: &str, content: &[u8]) -> std::path::PathBuf {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    let path = dir.join(name);
    let f = std::fs::File::create(&path).expect("create gz file");
    let mut enc = GzEncoder::new(f, Compression::default());
    std::io::Write::write_all(&mut enc, content).expect("gz encode");
    enc.finish().expect("gz finish");
    path
}

fn write_zstd(dir: &std::path::Path, name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create zst file");
    zstd::stream::copy_encode(content, &mut f, 3).expect("zstd encode");
    path
}

#[tokio::test]
async fn import_gzipped_ttl_small_file() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = b"@prefix ex: <http://example.org/ns/> .\n\
                @prefix schema: <http://schema.org/> .\n\
                ex:alice schema:name \"Alice\" .\n\
                ex:bob schema:name \"Bob\" .\n";

    let gz_path = write_gzipped(data_dir.path(), "people.ttl.gz", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create("test/ttl-gz:main")
        .import(&gz_path)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect(".ttl.gz import should succeed");
    assert!(result.flake_count >= 2);

    let ledger = fluree
        .ledger("test/ttl-gz:main")
        .await
        .expect("load ledger");
    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": ["?s", "?p", "?o"], "where": {"@id": "?s", "?p": "?o"}}),
    )
    .await
    .expect("query");
    let objs = extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2);
    assert!(objs.contains(&"Alice".to_string()));
    assert!(objs.contains(&"Bob".to_string()));
}

#[tokio::test]
async fn import_gzipped_nt_directory() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // Directory of `.nt.gz` files — discovery must accept the compressed
    // extension via `effective_extension`.
    write_gzipped(
        data_dir.path(),
        "a.nt.gz",
        b"<http://example.org/alice> <http://schema.org/name> \"Alice\" .\n",
    );
    write_gzipped(
        data_dir.path(),
        "b.nt.gz",
        b"<http://example.org/bob> <http://schema.org/name> \"Bob\" .\n",
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/nt-gz-dir:main")
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("directory .nt.gz import should succeed");
    assert_eq!(result.t, 2, "two .nt.gz files => t=2");

    let ledger = fluree
        .ledger("test/nt-gz-dir:main")
        .await
        .expect("load ledger");
    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": ["?s", "?p", "?o"], "where": {"@id": "?s", "?p": "?o"}}),
    )
    .await
    .expect("query");
    let objs = extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2);
    assert!(objs.contains(&"Alice".to_string()) && objs.contains(&"Bob".to_string()));
}

#[tokio::test]
async fn import_zstd_ttl_small_file() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = b"@prefix ex: <http://example.org/ns/> .\n\
                @prefix schema: <http://schema.org/> .\n\
                ex:carol schema:name \"Carol\" .\n";

    let zst_path = write_zstd(data_dir.path(), "people.ttl.zst", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/ttl-zst:main")
        .import(&zst_path)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect(".ttl.zst import should succeed");
    assert!(result.flake_count >= 1);

    let ledger = fluree.ledger("test/ttl-zst:main").await.expect("load");
    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": ["?s", "?p", "?o"], "where": {"@id": "?s", "?p": "?o"}}),
    )
    .await
    .expect("query");
    let objs = extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2);
    assert!(objs.contains(&"Carol".to_string()));
}

#[tokio::test]
async fn import_gzipped_ttl_streaming_split() {
    // Force the streaming (large-file) path: write enough TTL that the
    // compressed file exceeds the chunk threshold so the splitter activates.
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let mut ttl = String::from(
        "@prefix ex: <http://example.org/ns/> .\n\
         @prefix schema: <http://schema.org/> .\n",
    );
    // ~5 MB of synthetic triples — comfortably above the 1 MB chunk threshold
    // we configure below, even after compression.
    for i in 0..40_000 {
        use std::fmt::Write as _;
        writeln!(ttl, "ex:s{i} schema:name \"name-{i}\" .").unwrap();
    }
    let gz_path = write_gzipped(data_dir.path(), "big.ttl.gz", ttl.as_bytes());

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/ttl-gz-stream:main")
        .import(&gz_path)
        .threads(2)
        .chunk_size_mb(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("streaming .ttl.gz import should succeed");
    assert!(result.flake_count as usize >= 40_000);

    let ledger = fluree
        .ledger("test/ttl-gz-stream:main")
        .await
        .expect("load");
    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": ["?s", "?p", "?o"], "where": {"@id": "?s", "?p": "?o"}}),
    )
    .await
    .expect("query");
    let objs = extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2);
    // Spot-check a handful of values are queryable post-index.
    assert!(objs.contains(&"name-0".to_string()));
    assert!(objs.contains(&"name-39999".to_string()));
}

#[tokio::test]
async fn import_nt_streaming_split() {
    // Force a large *plain* `.nt` file through the streaming-split path (the
    // path that overflowed u32 token offsets on >4 GiB inputs). We can't write
    // 4 GiB in a test, but a chunk_size_mb(1) threshold over a multi-MB file
    // exercises the same StreamingTurtleReader chunking and per-chunk parse the
    // big file uses, with every offset relative to a small per-chunk string.
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // ~5 MB of N-Triples: full IRIs, one triple per line, no prelude.
    let mut nt = String::new();
    for i in 0..40_000 {
        use std::fmt::Write as _;
        writeln!(
            nt,
            "<http://example.org/ns/s{i}> <http://schema.org/name> \"name-{i}\" ."
        )
        .unwrap();
    }
    let nt_path = write_data(data_dir.path(), "big.nt", &nt);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let result = fluree
        .create("test/nt-stream:main")
        .import(&nt_path)
        .threads(2)
        .chunk_size_mb(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("streaming .nt import should succeed");
    assert!(result.flake_count as usize >= 40_000);

    let ledger = fluree.ledger("test/nt-stream:main").await.expect("load");
    let qr = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": ["?s", "?p", "?o"], "where": {"@id": "?s", "?p": "?o"}}),
    )
    .await
    .expect("query");
    let objs = extract_nth_column(&qr.to_jsonld(&ledger.snapshot).unwrap(), 2);
    // Boundary values across the chunk splits must all be queryable post-index.
    assert!(objs.contains(&"name-0".to_string()));
    assert!(objs.contains(&"name-39999".to_string()));
}
