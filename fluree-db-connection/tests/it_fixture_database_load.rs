//! Fixture database load integration tests.
//!
//! Run with:
//! `cargo test -p fluree-db-connection --test it_fixture_database_load -- --ignored --nocapture`

use fluree_db_connection::{connect, ConnectionHandle, StorageType};
use fluree_db_core::{ContentId, ContentKind, GraphDbRef, NoOverlay};
use fluree_db_query::{execute_pattern, Ref, RowAccess, Term, TriplePattern, VarRegistry};
use serde_json::json;
use std::path::{Path, PathBuf};

fn get_test_db_path() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-database");

    if path.exists() {
        Some(path)
    } else {
        None
    }
}

/// Ledger alias embedded in the test database path layout.
const TEST_LEDGER_ID: &str = "test/range-scan:main";

fn find_root_file(test_db_path: &Path) -> Option<ContentId> {
    let root_dir = test_db_path.join("test/range-scan/index/root");

    if !root_dir.exists() {
        return None;
    }

    let entry = std::fs::read_dir(&root_dir)
        .ok()?
        .filter_map(std::result::Result::ok)
        .find(|e| {
            e.path()
                .extension()
                .map(|x| x == "fir6" || x == "json")
                .unwrap_or(false)
        })?;

    let bytes = std::fs::read(entry.path()).ok()?;
    Some(ContentId::new(ContentKind::IndexRoot, &bytes))
}

#[tokio::test]
#[ignore = "Requires external test-database/ directory"]
async fn loads_fixture_database_and_scans_triples() {
    let test_db_path = match get_test_db_path() {
        Some(p) => p,
        None => {
            eprintln!("Test database not found at ../test-database/, skipping");
            return;
        }
    };

    let test_db_str = test_db_path.to_str().unwrap();

    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@id": "file",
        "@graph": [
            {
                "@id": "fileStorage",
                "@type": "Storage",
                "filePath": test_db_str
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 4,
                "cacheMaxMb": 1000,
                "indexStorage": {"@id": "fileStorage"},
                "commitStorage": {"@id": "fileStorage"}
            }
        ]
    });

    let conn = connect(&config).expect("Failed to parse JSON-LD config");

    let parsed_config = conn.config();
    assert_eq!(parsed_config.parallelism, 4);
    assert_eq!(parsed_config.cache.max_mb, 1000);
    assert!(matches!(
        parsed_config.index_storage.storage_type,
        StorageType::File
    ));

    let root_id = match find_root_file(&test_db_path) {
        Some(r) => r,
        None => {
            eprintln!("No root file found, skipping");
            return;
        }
    };

    let db = match &conn {
        ConnectionHandle::File { storage, .. } => {
            let fresh = fluree_db_core::FileStorage::new(storage.base_path());
            fluree_db_core::load_ledger_snapshot(&fresh, &root_id, TEST_LEDGER_ID)
                .await
                .unwrap()
        }
        _ => panic!("Expected File connection"),
    };

    // Execute query via execute_pattern (bypasses JSON query parser)
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let p = vars.get_or_insert("?p");
    let o = vars.get_or_insert("?o");

    let pattern = TriplePattern::new(Ref::Var(s), Ref::Var(p), Term::Var(o));
    let no_overlay = NoOverlay;
    let db_ref = GraphDbRef::new(&db, 0, &no_overlay, db.t);
    let batches = execute_pattern(db_ref, &vars, pattern).await.unwrap();

    assert!(!batches.is_empty(), "Should have at least one batch");
    let total: usize = batches.iter().map(fluree_db_query::Batch::len).sum();
    assert!(total > 0, "Should have results");

    if let Some(batch) = batches.first() {
        if let Some(row) = batch.row_view(0) {
            assert!(row.get(s).is_some(), "Subject should be bound");
            assert!(row.get(p).is_some(), "Predicate should be bound");
            assert!(row.get(o).is_some(), "Object should be bound");
        }
    }
}
