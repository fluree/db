//! Ledger lifecycle integration tests
//!
//! Tests the full ledger lifecycle: creation, name validation, existence checking,
//! basic querying, fuel tracking, and duplicate prevention.
//!
//! Merged from: it_api_create.rs, it_db.rs

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

// =============================================================================
// Ledger creation and name validation (from it_api_create.rs)
// =============================================================================

/// Test ledger creation name validation
#[tokio::test]
async fn create_ledger_name_validation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Explicit branch form (name:branch) is allowed
    let ledger = fluree.create_ledger("explicit:branch").await.unwrap();
    assert_eq!(ledger.ledger_id(), "explicit:branch");

    // Reject multiple colons (invalid alias format)
    let result = fluree.create_ledger("test:feature:v2").await;
    assert!(result.is_err(), "Should reject name with multiple colons");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid ledger ID format"));

    // Test accepting valid ledger names
    let ledger = fluree.create_ledger("valid-name").await.unwrap();
    assert_eq!(ledger.ledger_id(), "valid-name:main");

    let ledger = fluree.create_ledger("valid_name").await.unwrap();
    assert_eq!(ledger.ledger_id(), "valid_name:main");

    let ledger = fluree.create_ledger("tenant/database").await.unwrap();
    assert_eq!(ledger.ledger_id(), "tenant/database:main");

    let ledger = fluree.create_ledger("my-ledger-2024").await.unwrap();
    assert_eq!(ledger.ledger_id(), "my-ledger-2024:main");

    // Test automatically appending ':main' branch to valid names
    let ledger = fluree.create_ledger("auto-branch-test").await.unwrap();
    assert_eq!(ledger.ledger_id(), "auto-branch-test:main");
}

/// Test edge cases for ledger name validation
#[tokio::test]
async fn edge_case_validation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Test empty colon cases
    let result = fluree.create_ledger(":").await;
    assert!(result.is_err(), "Should reject single colon");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid ledger ID format"));

    let result = fluree.create_ledger(":branch").await;
    assert!(result.is_err(), "Should reject name starting with colon");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid ledger ID format"));

    let result = fluree.create_ledger("ledger:").await;
    assert!(result.is_err(), "Should reject name ending with colon");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid ledger ID format"));

    // Test special characters that ARE allowed
    let ledger = fluree.create_ledger("ledger.with.dots").await.unwrap();
    assert_eq!(ledger.ledger_id(), "ledger.with.dots:main");

    let ledger = fluree.create_ledger("ledger-with-dashes").await.unwrap();
    assert_eq!(ledger.ledger_id(), "ledger-with-dashes:main");

    let ledger = fluree
        .create_ledger("ledger_with_underscores")
        .await
        .unwrap();
    assert_eq!(ledger.ledger_id(), "ledger_with_underscores:main");

    let ledger = fluree
        .create_ledger("org/department/project")
        .await
        .unwrap();
    assert_eq!(ledger.ledger_id(), "org/department/project:main");
}

/// Test that duplicate ledger creation is prevented
#[tokio::test]
async fn duplicate_ledger_creation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_name = "unique-test";

    // First creation should succeed
    let _ledger = fluree.create_ledger(ledger_name).await.unwrap();

    // Second creation with same name should fail
    let result = fluree.create_ledger(ledger_name).await;
    assert!(result.is_err(), "Duplicate creation should fail");

    // Trying with explicit :main should also fail (same normalized alias)
    let result = fluree.create_ledger(&format!("{ledger_name}:main")).await;
    assert!(
        result.is_err(),
        "Duplicate creation should fail for explicit branch too"
    );
}

// =============================================================================
// General DB functionality (from it_db.rs)
// =============================================================================

/// Test ledger existence checking
#[tokio::test]
async fn exists_test() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger_id = "testledger";

    // Test: returns false before creation
    let result = fluree.ledger(ledger_id).await;
    assert!(result.is_err(), "Ledger should not exist before creation");

    // Create the ledger
    let _ledger = fluree.create_ledger(ledger_id).await.unwrap();

    // Test: returns true after creation
    let result = fluree.ledger(ledger_id).await;
    assert!(result.is_ok(), "Ledger should exist after creation");

    // Test: still returns true after committing data
    let ledger = result.unwrap();
    let txn = json!({
        "@context": support::default_context(),
        "@graph": [{
            "@id": "https://ns.flur.ee/me",
            "@type": "https://schema.org/Person",
            "https://schema.org/name": "Me"
        }]
    });
    let _updated_ledger = fluree.insert(ledger, &txn).await.unwrap();

    let result = fluree.ledger(ledger_id).await;
    assert!(
        result.is_ok(),
        "Ledger should still exist after committing data"
    );

    // Test: returns false for non-existent ledger
    let result = fluree.ledger("notaledger").await;
    assert!(result.is_err(), "Non-existent ledger should not be found");
}

/// Integration test for basic query functionality
#[tokio::test]
async fn query_integration_test() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load people data
    let ledger_id = support::load_people(&fluree).await.unwrap();

    // Get the ledger
    let ledger = fluree.ledger(&ledger_id).await.unwrap();

    // Query for all users with their names
    let query = json!({
        "@context": [
            support::default_context(),
            {"ex": "http://example.org/ns/"}
        ],
        "select": ["?person", "?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // Expected results: [[:ex/alice "Alice"], [:ex/brian "Brian"], [:ex/cam "Cam"], [:ex/liam "Liam"]]
    assert_eq!(arr.len(), 4, "Should return 4 users");

    // Sort by name for consistent comparison
    let mut results: Vec<(String, String)> = arr
        .iter()
        .map(|row| {
            let row_arr = row.as_array().unwrap();
            let person = row_arr[0].as_str().unwrap().to_string();
            let name = row_arr[1].as_str().unwrap().to_string();
            (person, name)
        })
        .collect();

    results.sort_by(|a, b| a.1.cmp(&b.1));

    // Verify expected results
    assert_eq!(results[0], ("ex:alice".to_string(), "Alice".to_string()));
    assert_eq!(results[1], ("ex:brian".to_string(), "Brian".to_string()));
    assert_eq!(results[2], ("ex:cam".to_string(), "Cam".to_string()));
    assert_eq!(results[3], ("ex:liam".to_string(), "Liam".to_string()));
}

/// Integration test for fuel tracking functionality
#[tokio::test]
async fn fuel_integration_test() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Load people data first
    let ledger_id = support::load_people(&fluree).await.unwrap();
    let ledger = fluree.ledger(&ledger_id).await.unwrap();

    // =========================================================================
    // Test queries with fuel tracking
    // =========================================================================

    // Query without metadata should work but not report fuel
    let query_basic = json!({
        "@context": [
            support::default_context(),
            {"ex": "http://example.org/ns/"}
        ],
        "select": ["?person", "?name"],
        "where": {
            "@id": "?person",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let basic_result = support::query_jsonld(&fluree, &ledger, &query_basic)
        .await
        .unwrap();
    let rows = basic_result.to_jsonld(&ledger.snapshot).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        4,
        "Basic query should return 4 users"
    );

    // Query with metadata should report fuel
    let query_with_meta = json!({
        "@context": support::default_context(),
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"},
        "opts": {"meta": true}
    });

    let tracked_result = support::query_jsonld_tracked(&fluree, &ledger, &query_with_meta)
        .await
        .unwrap();
    let query_fuel = tracked_result
        .fuel
        .expect("Query with meta should report fuel");

    // Fuel should be a positive decimal representing computational cost
    assert!(
        query_fuel > 0.0,
        "Query fuel should be greater than 0, got: {query_fuel}"
    );

    // Fuel should roughly correspond to the number of flakes traversed
    // (may not be exact due to query optimization differences). Per-row charge
    // is 1 fuel each in this step.
    let total_flakes = ledger.current_stats().flakes as f64;
    // Some in-memory fixtures don't materialize stats; avoid asserting against zero.
    if total_flakes > 0.0 {
        assert!(
            query_fuel <= total_flakes * 2.0, // Allow some overhead for query processing
            "Query fuel ({query_fuel}) should be reasonable compared to total flakes ({total_flakes})"
        );
    }

    // =========================================================================
    // Test fuel limits (short-circuiting)
    // =========================================================================

    // Query with very low fuel limit should fail. The fixture is in-memory
    // (genesis, no binary index), so this exercises the overlay path which
    // charges 1 micro-fuel per row; a 0.001-fuel limit (1 micro-fuel) trips
    // on the second row.
    let query_with_limit = json!({
        "@context": support::default_context(),
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"},
        "opts": {"maxFuel": 0.001}
    });

    let limited_result = support::query_jsonld(&fluree, &ledger, &query_with_limit).await;
    assert!(
        limited_result.is_err(),
        "Query with very low maxFuel should fail"
    );
    let err_msg = limited_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Fuel limit exceeded") || err_msg.contains("fuel"),
        "Error should mention fuel limit exceeded, got: {err_msg}"
    );
}
