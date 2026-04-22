//! File-backed integration tests for fluree-db-api
//!
//! These tests are intentionally `#[ignore]` because they require an external
//! `test-database/` directory (outside the repo root).

#![cfg(feature = "native")]

use fluree_db_api::fluree_file;
use serde_json::json;
use std::path::PathBuf;

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

/// End-to-end test using the fluree-db-api with a real file-backed database.
///
/// Run with:
/// `cargo test -p fluree-db-api --test it_file_backed -- --ignored --nocapture`
#[tokio::test]
#[ignore = "Requires external test-database/ directory"]
async fn file_backed_query_smoke_test() {
    let test_db_path = match get_test_db_path() {
        Some(p) => p,
        None => return, // skip silently when directory absent
    };

    let fluree = fluree_file(test_db_path.to_str().unwrap()).expect("create Fluree (file)");
    let db = fluree
        .ledger("test/range-scan:main")
        .await
        .expect("load db");

    let query = json!({
        "select": ["?s", "?type"],
        "where": { "@id": "?s", "@type": "?type" }
    });

    let result = fluree
        .query(&fluree_db_api::GraphDb::from_ledger_state(&db), &query)
        .await
        .expect("query");
    assert!(!result.is_empty(), "expected results from test database");
}

/// Benchmark-style test.
///
/// Run with:
/// `cargo test -p fluree-db-api --test it_file_backed -- --ignored --nocapture`
#[tokio::test]
#[ignore = "Benchmark: requires external test-database/ directory"]
async fn file_backed_query_benchmark() {
    use std::time::Instant;

    let test_db_path = match get_test_db_path() {
        Some(p) => p,
        None => return,
    };

    let fluree = fluree_file(test_db_path.to_str().unwrap()).expect("create Fluree (file)");
    let db = fluree
        .ledger("test/range-scan:main")
        .await
        .expect("load db");

    let query = json!({
        "select": ["?s", "?type"],
        "where": { "@id": "?s", "@type": "?type" }
    });

    // Warmup
    for _ in 0..10 {
        let _ = fluree
            .query(&fluree_db_api::GraphDb::from_ledger_state(&db), &query)
            .await
            .unwrap();
    }

    let iterations = 100;
    let start = Instant::now();
    let mut total_rows = 0;

    for _ in 0..iterations {
        let result = fluree
            .query(&fluree_db_api::GraphDb::from_ledger_state(&db), &query)
            .await
            .unwrap();
        total_rows += result.row_count();
    }

    let elapsed = start.elapsed();
    let _rows_per_iter = total_rows / iterations;
    let _queries_per_sec = iterations as f64 / elapsed.as_secs_f64();
}
