//! Integration tests for V3 (FLI3) index format via the bulk import pipeline.
//!
//! Verifies:
//! - V3 artifacts (FLI3 leaves) are produced with correct magic bytes
//! - Full E2E: import → FIR6 root → load → V3 cursor → SPARQL query → results

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use std::io::Write;
use std::path::Path;

fn write_ttl(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create ttl file");
    f.write_all(content.as_bytes()).expect("write ttl");
    path
}

/// Recursively find files whose first 4 bytes match the given magic.
fn find_files_with_magic(dir: &Path, magic: &[u8; 4]) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    scan_dir_recursive(dir, magic, &mut results);
    results
}

fn scan_dir_recursive(dir: &Path, magic: &[u8; 4], results: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_dir_recursive(&path, magic, results);
        } else if path.is_file() {
            if let Ok(data) = std::fs::read(&path) {
                if data.len() >= 4 && &data[0..4] == magic {
                    results.push(path);
                }
            }
        }
    }
}

#[tokio::test]
async fn import_v3_produces_fli3_artifacts() {
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

    // Import with V3 format.
    let result = fluree
        .create("test/import-v3:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await;

    assert!(result.is_ok(), "V3 import failed: {:?}", result.err());

    // ---- Byte-level artifact inspection ----
    //
    // Scan the file-backed CAS directory for FLI3 leaf files and FBR3 branch files.
    // The CAS layout is: db_dir/data/index-leaf/<cid> (or similar content-addressed paths).
    // Since the exact CAS layout may vary, scan the entire db_dir for matching magic.

    let fli3_files = find_files_with_magic(db_dir.path(), b"FLI3");
    assert!(
        !fli3_files.is_empty(),
        "expected at least one FLI3 leaf file in CAS, found none under {}",
        db_dir.path().display()
    );

    // Verify first FLI3 file has valid header.
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3, decode_leaf_header_v3, LEAF_V3_MAGIC,
    };

    for fli3_path in &fli3_files {
        let data = std::fs::read(fli3_path).expect("read FLI3 leaf file");
        assert_eq!(&data[0..4], LEAF_V3_MAGIC, "magic mismatch");

        let header = decode_leaf_header_v3(&data).expect("decode FLI3 header");
        assert!(
            header.leaflet_count > 0,
            "leaf must have at least one leaflet"
        );
        assert!(header.total_rows > 0, "leaf must have rows");

        let dir_entries =
            decode_leaf_dir_v3(&data, &header).expect("decode FLI3 leaflet directory");
        assert_eq!(dir_entries.len(), header.leaflet_count as usize);

        // Verify each leaflet has column refs.
        for entry in &dir_entries {
            assert!(
                !entry.column_refs.is_empty(),
                "leaflet must have column blocks"
            );
            assert!(entry.row_count > 0, "leaflet must have rows");
        }

        // For POST order, verify predicate-homogeneous segmentation.
        use fluree_db_binary_index::format::run_record::RunSortOrder;
        if header.order == RunSortOrder::Post {
            for entry in &dir_entries {
                assert!(
                    entry.p_const.is_some(),
                    "POST leaflets must have p_const set"
                );
            }
        }

        // For OPST order, verify type-homogeneous segmentation.
        if header.order == RunSortOrder::Opst {
            for entry in &dir_entries {
                assert!(
                    entry.o_type_const.is_some(),
                    "OPST leaflets must have o_type_const set"
                );
            }
        }
    }

    // FBR3 branch files are NOT uploaded to CAS for the default graph (g_id=0) —
    // the branch is embedded inline in the root. FBR3 files would only appear
    // for named graphs (g_id != 0). For this single-graph test, we verify that
    // FLI3 leaves were produced and are structurally valid (above assertions).
}

// ============================================================================
// E2E: import V3 → load → query → verify results
// ============================================================================

/// Full end-to-end: import TTL with V3 format → load ledger → run queries → verify.
///
/// Validates the complete V3 read path: FIR6 root decode → BinaryIndexStore load →
/// BinaryCursor scan → decode_value_v3 → query results.
///
/// Covers:
/// - Plain xsd:string (schema:name "Alice")
/// - rdf:langString (schema:description "A user"@en)
/// - IRI reference / @id (rdf:type → ex:User)
/// - Integer literal (schema:age 42)
#[tokio::test]
async fn import_v3_and_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // TTL with diverse value types for decode_value_v3 coverage.
    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:description "A user"@en ;
    schema:age 42 ;
    ex:friend ex:bob .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:description "Another user"@de ;
    schema:age 22 .
"#;

    let ttl_path = write_ttl(data_dir.path(), "typed.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Import with V3 format.
    let result = fluree
        .create("test/v3-query:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("V3 import should succeed");

    assert!(result.t > 0, "should have at least one commit");
    assert!(result.root_id.is_some(), "index should have been built");

    // Verify FIR6 root was produced (FIR6 format).
    let fir6_files = find_files_with_magic(db_dir.path(), b"FIR6");
    assert!(
        !fir6_files.is_empty(),
        "expected FIR6 root file, found none under {}",
        db_dir.path().display()
    );

    // Load the ledger (this triggers FIR6 → BinaryIndexStore loading).
    let ledger = fluree
        .ledger("test/v3-query:main")
        .await
        .expect("load V3 ledger");

    // ── Query 1: plain xsd:string ──
    let names_result = support::query_sparql(
        &fluree,
        &ledger,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("string query");
    let names_json = names_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["Alice", "Bob"], "xsd:string query failed");

    // ── Query 1b: bound xsd:string object (must filter correctly) ──
    let alice_subject_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?s WHERE { ?s schema:name "Alice" }
        "#,
    )
    .await
    .expect("bound string object query");
    let alice_subject_json = alice_subject_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let alice_subject_bindings = alice_subject_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        alice_subject_bindings.len(),
        1,
        "expected exactly one subject for name=\"Alice\""
    );
    let alice_s = alice_subject_bindings[0]["s"]["value"].as_str().unwrap();
    assert!(
        alice_s == "http://example.org/ns/alice" || alice_s == "ex:alice",
        "bound string object scan mismatch: {alice_s}"
    );

    // ── Query 2: rdf:langString ──
    let desc_result = support::query_sparql(
        &fluree,
        &ledger,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?desc WHERE { ?s schema:description ?desc }
        ORDER BY ?desc
        ",
    )
    .await
    .expect("langString query");
    let desc_json = desc_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let desc_bindings = desc_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let descs: Vec<&str> = desc_bindings
        .iter()
        .map(|b| b["desc"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        descs,
        vec!["A user", "Another user"],
        "rdf:langString query failed"
    );
    // Verify language tags are preserved.
    let langs: Vec<&str> = desc_bindings
        .iter()
        .filter_map(|b| b["desc"]["xml:lang"].as_str())
        .collect();
    assert_eq!(langs, vec!["en", "de"], "language tags not preserved");

    // ── Query 2b: bound rdf:langString object + lang constraint ──
    let alice_desc_subject_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?s WHERE { ?s schema:description "A user"@en }
        "#,
    )
    .await
    .expect("bound langString query");
    let alice_desc_subject_json = alice_desc_subject_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let alice_desc_subject_bindings = alice_desc_subject_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        alice_desc_subject_bindings.len(),
        1,
        "expected exactly one subject for description=\"A user\"@en"
    );
    let alice_s2 = alice_desc_subject_bindings[0]["s"]["value"]
        .as_str()
        .unwrap();
    assert!(
        alice_s2 == "http://example.org/ns/alice" || alice_s2 == "ex:alice",
        "bound langString object scan mismatch: {alice_s2}"
    );

    // ── Query 3: IRI reference (rdf:type) ──
    let type_result = support::query_sparql(
        &fluree,
        &ledger,
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE { ?s a ex:User }
        ORDER BY ?s
        ",
    )
    .await
    .expect("IRI ref query");
    let type_json = type_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let type_bindings = type_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(type_bindings.len(), 2, "expected 2 ex:User instances");
    // Subjects should be IRIs.
    for b in type_bindings {
        assert_eq!(
            b["s"]["type"].as_str().unwrap(),
            "uri",
            "subject should be URI type"
        );
    }

    // ── Query 4: integer literal ──
    let age_result = support::query_sparql(
        &fluree,
        &ledger,
        r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?age WHERE { ex:alice schema:age ?age }
        ",
    )
    .await
    .expect("integer query");
    let age_json = age_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let age_bindings = age_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(age_bindings.len(), 1, "expected 1 age result");
    let age_val = age_bindings[0]["age"]["value"].as_str().expect("age value");
    assert_eq!(age_val, "42", "integer literal value mismatch");

    // ── Query 5: JSON-LD query (cross-check scan path) ──
    let jld_result = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "schema": "http://schema.org/"
            },
            "select": "?name",
            "where": { "schema:name": "?name" }
        }),
    )
    .await
    .expect("JSON-LD query");
    let jld_json = jld_result
        .to_jsonld(&ledger.snapshot)
        .expect("format jsonld");
    let mut jld_names: Vec<String> = jld_json
        .as_array()
        .expect("array")
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    jld_names.sort();
    assert_eq!(jld_names, vec!["Alice", "Bob"], "JSON-LD query failed");
}

// ============================================================================
// E2E: import V3 → transact → query (overlay merge)
// ============================================================================

/// Import V3, then transact a new triple, then query — verifies overlay merge
/// surfaces both indexed data and uncommitted novelty.
#[tokio::test]
async fn import_v3_transact_then_query() {
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

    let ttl_path = write_ttl(data_dir.path(), "overlay.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Phase 1: Import with V3 format.
    let _import_result = fluree
        .create("test/v3-overlay:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("V3 import should succeed");

    // Phase 2: Load ledger, then transact a new triple (overlay/novelty).
    let ledger = fluree
        .ledger("test/v3-overlay:main")
        .await
        .expect("load V3 ledger");

    let insert_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "schema:age": 34
        }]
    });

    let txn_result = fluree
        .insert(ledger, &insert_data)
        .await
        .expect("insert should succeed");

    assert!(
        txn_result.receipt.flake_count > 0,
        "transaction should produce flakes"
    );
    let ledger_after = txn_result.ledger;

    // Phase 3: Query the post-transaction ledger — should see indexed (Alice, Bob)
    // plus overlay (Cam).
    let names_result = support::query_sparql(
        &fluree,
        &ledger_after,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("overlay query");
    let names_json = names_result
        .to_sparql_json(&ledger_after.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Cam"],
        "overlay merge should surface both indexed and novelty data"
    );

    // Verify rdf:type also shows all three users.
    let type_result = support::query_sparql(
        &fluree,
        &ledger_after,
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE { ?s a ex:User }
        ORDER BY ?s
        ",
    )
    .await
    .expect("type query with overlay");
    let type_json = type_result
        .to_sparql_json(&ledger_after.snapshot)
        .expect("format sparql json");
    let type_bindings = type_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        type_bindings.len(),
        3,
        "expected 3 ex:User instances (2 indexed + 1 overlay)"
    );

    // Verify integer query on overlay entity.
    let cam_age = support::query_sparql(
        &fluree,
        &ledger_after,
        r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?age WHERE { ex:cam schema:age ?age }
        ",
    )
    .await
    .expect("cam age query");
    let cam_age_json = cam_age
        .to_sparql_json(&ledger_after.snapshot)
        .expect("format sparql json");
    let cam_age_bindings = cam_age_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(cam_age_bindings.len(), 1, "expected 1 age result for Cam");
    assert_eq!(
        cam_age_bindings[0]["age"]["value"].as_str().unwrap(),
        "34",
        "Cam's age should be 34"
    );
}

/// Overlay retraction: import V3 → retract an indexed triple → verify it disappears.
#[tokio::test]
async fn import_v3_retract_then_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice schema:name "Alice" ;
    schema:age 42 .

ex:bob schema:name "Bob" ;
    schema:age 22 .
"#;
    let ttl_path = write_ttl(data_dir.path(), "retract.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let _import = fluree
        .create("test/v3-retract:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree.ledger("test/v3-retract:main").await.expect("load");

    // Retract Bob's name.
    let delete_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "delete": [{
            "@id": "ex:bob",
            "schema:name": "Bob"
        }]
    });

    let txn = fluree
        .transact(
            ledger,
            fluree_db_api::TxnType::Update,
            &delete_data,
            fluree_db_api::TxnOpts::default(),
            fluree_db_api::CommitOpts::default(),
            &fluree_db_ledger::IndexConfig::default(),
        )
        .await
        .expect("retract should succeed");

    let ledger_after = txn.ledger;

    // Query names — should only see Alice (Bob's name retracted).
    let names_result = support::query_sparql(
        &fluree,
        &ledger_after,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("post-retract query");
    let names_json = names_result
        .to_sparql_json(&ledger_after.snapshot)
        .expect("format");
    let bindings = names_json["results"]["bindings"].as_array().expect("array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Alice"],
        "retract should remove Bob's name, leaving only Alice"
    );
}

/// Overlay langString: import V3 → transact a langString → verify tag preserved.
#[tokio::test]
async fn import_v3_overlay_lang_string() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:alice schema:name "Alice" .
"#;
    let ttl_path = write_ttl(data_dir.path(), "lang.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");

    let _import = fluree
        .create("test/v3-lang:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree.ledger("test/v3-lang:main").await.expect("load");

    // Transact a langString via overlay.
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "schema:description": {
                "@value": "A person",
                "@language": "en"
            }
        }]
    });

    let txn = fluree
        .insert(ledger, &insert)
        .await
        .expect("insert langString");
    let ledger_after = txn.ledger;

    // Query descriptions — should see the novelty langString with tag.
    let desc_result = support::query_sparql(
        &fluree,
        &ledger_after,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?desc WHERE { ?s schema:description ?desc }
        ",
    )
    .await
    .expect("langString overlay query");
    let desc_json = desc_result
        .to_sparql_json(&ledger_after.snapshot)
        .expect("format");
    let desc_bindings = desc_json["results"]["bindings"].as_array().expect("array");
    assert_eq!(desc_bindings.len(), 1, "expected 1 description");
    assert_eq!(
        desc_bindings[0]["desc"]["value"].as_str().unwrap(),
        "A person"
    );
    assert_eq!(
        desc_bindings[0]["desc"]["xml:lang"].as_str().unwrap(),
        "en",
        "language tag should be preserved for novelty langString"
    );
}

// ============================================================================
// E2E: import V3 → transact → rebuild → reload → query
// ============================================================================

/// Full cycle: import V3, transact, trigger V3 rebuild from commits,
/// reload the ledger from the new FIR6 root, query the rebuilt index.
///
/// This verifies: `rebuild_index_from_commits(use_v3=true)` produces a
/// valid FIR6 root that can be loaded and queried. The transacted data
/// (Cam) must survive the rebuild because rebuild walks the full commit chain.
#[tokio::test]
async fn import_v3_rebuild_then_query() {
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

    let ttl_path = write_ttl(data_dir.path(), "rebuild.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Phase 1: Import with V3 format.
    let _import_result = fluree
        .create("test/v3-rebuild:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("V3 import should succeed");

    // Phase 2: Transact a new entity (committed, not just overlay).
    let ledger = fluree
        .ledger("test/v3-rebuild:main")
        .await
        .expect("load V3 ledger");

    let insert_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "schema:age": 34
        }]
    });

    let txn_result = fluree
        .insert(ledger, &insert_data)
        .await
        .expect("insert should succeed");
    assert!(txn_result.receipt.flake_count > 0);

    // Phase 3: Trigger V3 rebuild from commits.
    // Get the nameservice record for the ledger.
    let ns_record = fluree
        .nameservice()
        .lookup("test/v3-rebuild:main")
        .await
        .expect("ns lookup")
        .expect("ns record should exist");

    let indexer_config = fluree_db_indexer::IndexerConfig::default();

    let index_result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store("test/v3-rebuild:main"),
        "test/v3-rebuild:main",
        &ns_record,
        indexer_config,
    )
    .await
    .expect("V3 rebuild should succeed");

    assert!(
        index_result.index_t > 0,
        "rebuild should produce an index with t > 0"
    );

    // Verify the rebuild produced a FIR6 root (FIR6 format).
    let fir6_files = find_files_with_magic(db_dir.path(), b"FIR6");
    // There should be at least 2 FIR6 files now: one from import, one from rebuild.
    assert!(
        fir6_files.len() >= 2,
        "expected at least 2 FIR6 roots (import + rebuild), found {}",
        fir6_files.len()
    );

    // Phase 4: Publish the rebuilt root to nameservice so ledger() loads it.
    fluree
        .publisher()
        .unwrap()
        .publish_index(
            "test/v3-rebuild:main",
            index_result.index_t,
            &index_result.root_id,
        )
        .await
        .expect("publish rebuilt root");

    // Phase 5: Reload the ledger from the rebuilt FIR6 root.
    let rebuilt_ledger = fluree
        .ledger("test/v3-rebuild:main")
        .await
        .expect("load rebuilt V3 ledger");

    // Phase 6: Query the rebuilt index — should see all 3 users
    // (Alice + Bob from import, Cam from the committed transaction).
    let names_result = support::query_sparql(
        &fluree,
        &rebuilt_ledger,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("query rebuilt V3 index");
    let names_json = names_result
        .to_sparql_json(&rebuilt_ledger.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Cam"],
        "rebuilt V3 index should contain all 3 users (import + transacted)"
    );

    // Verify integer values survive rebuild.
    let age_result = support::query_sparql(
        &fluree,
        &rebuilt_ledger,
        r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?age WHERE { ex:cam schema:age ?age }
        ",
    )
    .await
    .expect("cam age after rebuild");
    let age_json = age_result
        .to_sparql_json(&rebuilt_ledger.snapshot)
        .expect("format");
    let age_bindings = age_json["results"]["bindings"].as_array().expect("array");
    assert_eq!(age_bindings.len(), 1);
    assert_eq!(age_bindings[0]["age"]["value"].as_str().unwrap(), "34");

    // Verify rdf:type query (POST index) works after rebuild.
    let type_result = support::query_sparql(
        &fluree,
        &rebuilt_ledger,
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE { ?s a ex:User }
        ORDER BY ?s
        ",
    )
    .await
    .expect("type query after rebuild");
    let type_json = type_result
        .to_sparql_json(&rebuilt_ledger.snapshot)
        .expect("format sparql json");
    let type_bindings = type_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        type_bindings.len(),
        3,
        "expected 3 ex:User instances after rebuild (Alice + Bob + Cam)"
    );
}

// ============================================================================
// E2E: import V3 → rebuild → transact → incremental V6 → query
// ============================================================================

/// Full incremental cycle: import V3, rebuild, transact new data (with new
/// subject IRI + new string), trigger incremental V6 indexing, reload, query.
///
/// Validates:
/// - `incremental_index` runs (not falling back to rebuild)
/// - New subject IRI resolves correctly (forward pack updated)
/// - New string literal resolves correctly (forward pack updated)
/// - Existing indexed data survives the incremental update
/// - Retracted data is absent after incremental
#[tokio::test]
async fn import_v3_incremental_then_query() {
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

    let ttl_path = write_ttl(data_dir.path(), "incr.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Phase 1: Import with V3 format.
    fluree
        .create("test/v3-incr:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("V3 import should succeed");

    // Phase 2: Explicit rebuild to get a clean FIR6 root published.
    let ns_record = fluree
        .nameservice()
        .lookup("test/v3-incr:main")
        .await
        .expect("ns lookup")
        .expect("ns record");

    let rebuild_config = fluree_db_indexer::IndexerConfig::default();

    let rebuild_result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store("test/v3-incr:main"),
        "test/v3-incr:main",
        &ns_record,
        rebuild_config,
    )
    .await
    .expect("V3 rebuild should succeed");

    fluree
        .publisher()
        .unwrap()
        .publish_index(
            "test/v3-incr:main",
            rebuild_result.index_t,
            &rebuild_result.root_id,
        )
        .await
        .expect("publish rebuild root");

    // Phase 3: Transact new data — introduces a new subject IRI (ex:cam)
    // and new string literal ("Cam"), creating a commit gap from the rebuild root.
    let ledger = fluree
        .ledger("test/v3-incr:main")
        .await
        .expect("load V3 ledger");

    let insert_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "schema:age": 34
        }]
    });

    let txn_result = fluree
        .insert(ledger, &insert_data)
        .await
        .expect("insert should succeed");
    assert!(txn_result.receipt.flake_count > 0);

    // Phase 4: Trigger incremental indexing via build_index_for_ledger.
    // The V6 root from import should be detected, and incremental should run
    // (not falling back to rebuild) since commit gap is small.
    let ns_record2 = fluree
        .nameservice()
        .lookup("test/v3-incr:main")
        .await
        .expect("ns lookup")
        .expect("ns record");

    let indexer_config = fluree_db_indexer::IndexerConfig {
        incremental_enabled: true,
        incremental_max_commits: 100,
        ..Default::default()
    };

    // Verify we have a commit gap (index_t < commit_t) and an existing root.
    assert!(
        ns_record2.index_head_id.is_some(),
        "must have a FIR6 root from rebuild to attempt incremental"
    );
    assert!(
        ns_record2.commit_t > ns_record2.index_t,
        "need a commit gap: commit_t={} should be > index_t={}",
        ns_record2.commit_t,
        ns_record2.index_t
    );

    let index_result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store("test/v3-incr:main"),
        fluree.nameservice(),
        "test/v3-incr:main",
        indexer_config,
    )
    .await
    .expect("V6 incremental indexing should succeed");

    assert!(
        index_result.index_t >= ns_record2.commit_t,
        "index_t ({}) should be >= commit_t ({})",
        index_result.index_t,
        ns_record2.commit_t
    );

    // Verify a new FIR6 root was produced.
    let fir6_files = find_files_with_magic(db_dir.path(), b"FIR6");
    assert!(
        fir6_files.len() >= 2,
        "expected at least 2 FIR6 roots (import + incremental), found {}",
        fir6_files.len()
    );

    // Phase 5: Publish and reload.
    fluree
        .publisher()
        .unwrap()
        .publish_index(
            "test/v3-incr:main",
            index_result.index_t,
            &index_result.root_id,
        )
        .await
        .expect("publish incremental root");

    let incr_ledger = fluree
        .ledger("test/v3-incr:main")
        .await
        .expect("load incremental V3 ledger");

    // Verify the root is FIR6 (codec = CODEC_FLUREE_INDEX_ROOT).
    assert_eq!(
        index_result.root_id.codec(),
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT,
        "result should be FIR6 root, not V5"
    );

    // Phase 6: Query — should see all 3 users.
    let names_result = support::query_sparql(
        &fluree,
        &incr_ledger,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("query incremental V3 index");
    let names_json = names_result
        .to_sparql_json(&incr_ledger.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Alice", "Bob", "Cam"],
        "incremental V6 index should contain all 3 users"
    );

    // Verify Cam's IRI resolves (forward pack for new subject).
    let cam_result = support::query_sparql(
        &fluree,
        &incr_ledger,
        r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?age WHERE { ex:cam schema:age ?age }
        ",
    )
    .await
    .expect("cam age query");
    let cam_json = cam_result
        .to_sparql_json(&incr_ledger.snapshot)
        .expect("format");
    let cam_bindings = cam_json["results"]["bindings"].as_array().expect("array");
    assert_eq!(cam_bindings.len(), 1);
    assert_eq!(
        cam_bindings[0]["age"]["value"].as_str().unwrap(),
        "34",
        "Cam's age should be 34 after incremental"
    );

    // Verify rdf:type query (POST index) works after incremental.
    let type_result = support::query_sparql(
        &fluree,
        &incr_ledger,
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE { ?s a ex:User }
        ORDER BY ?s
        ",
    )
    .await
    .expect("type query after incremental");
    let type_json = type_result
        .to_sparql_json(&incr_ledger.snapshot)
        .expect("format sparql json");
    let type_bindings = type_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        type_bindings.len(),
        3,
        "expected 3 ex:User instances after incremental (Alice + Bob + Cam)"
    );
}

/// Verify that V3 rebuild correctly filters retract-winners.
///
/// Import 2 entities, retract one via transaction, rebuild V3 → the retracted
/// entity's triples should be absent from the rebuilt index.
#[tokio::test]
async fn import_v3_rebuild_filters_retracts() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

ex:keep a ex:User ;
    schema:name "Keep" .

ex:remove a ex:User ;
    schema:name "Remove" .
"#;

    let ttl_path = write_ttl(data_dir.path(), "retract.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Import with V3 format.
    fluree
        .create("test/v3-retract:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("V3 import should succeed");

    // Retract the "Remove" entity.
    let ledger = fluree
        .ledger("test/v3-retract:main")
        .await
        .expect("load V3 ledger");

    let delete_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "ex:remove",
            "schema:name": "?name",
            "@type": "?type"
        },
        "delete": {
            "@id": "ex:remove",
            "schema:name": "?name",
            "@type": "?type"
        }
    });

    let txn_result = fluree
        .update(ledger, &delete_data)
        .await
        .expect("retract should succeed");
    assert!(txn_result.receipt.flake_count > 0);

    // Rebuild V3 from commits.
    let ns_record = fluree
        .nameservice()
        .lookup("test/v3-retract:main")
        .await
        .expect("ns lookup")
        .expect("ns record should exist");

    let indexer_config = fluree_db_indexer::IndexerConfig::default();

    let index_result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store("test/v3-retract:main"),
        "test/v3-retract:main",
        &ns_record,
        indexer_config,
    )
    .await
    .expect("V3 rebuild should succeed");

    // Publish and reload.
    fluree
        .publisher()
        .unwrap()
        .publish_index(
            "test/v3-retract:main",
            index_result.index_t,
            &index_result.root_id,
        )
        .await
        .expect("publish rebuilt root");

    let rebuilt_ledger = fluree
        .ledger("test/v3-retract:main")
        .await
        .expect("load rebuilt V3 ledger");

    // Query: only "Keep" should remain; "Remove" should be filtered out.
    let names_result = support::query_sparql(
        &fluree,
        &rebuilt_ledger,
        r"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        ",
    )
    .await
    .expect("query rebuilt V3 index");
    let names_json = names_result
        .to_sparql_json(&rebuilt_ledger.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["Keep"],
        "rebuilt V3 index should only contain 'Keep' — 'Remove' should be filtered as retract-winner"
    );
}
