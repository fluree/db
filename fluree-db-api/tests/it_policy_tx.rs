//! Policy transaction (modify) enforcement tests.
//!
//! Scenario reference: `fluree.snapshot.policy.tx-test`
//!
//! Tests modify-policy enforcement including:
//! - f:onProperty modify policies with f:query
//! - View-only policies blocking all modifications
//! - Custom error messages via f:exMessage

mod support;

use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, IndexConfig, QueryConnectionOptions, TrackedTransactionInput,
    TxnOpts, TxnType,
};
use serde_json::json;
use std::collections::HashMap;
use support::{assert_index_defaults, genesis_ledger};

/// Helper to seed test data with users.
async fn seed_users(fluree: &support::MemoryFluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "http://example.org/ns/alice",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "Alice",
                "http://schema.org/email": "alice@flur.ee",
                "http://schema.org/ssn": "111-11-1111"
            },
            {
                "@id": "http://example.org/ns/john",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "John",
                "http://schema.org/email": "john@flur.ee",
                "http://schema.org/ssn": "888-88-8888"
            },
            // Identity subjects linking to users
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            {
                "@id": "http://example.org/ns/johnIdentity",
                "http://example.org/ns/user": {"@id": "http://example.org/ns/john"}
            }
        ]
    });

    fluree.insert(ledger0, &txn).await.unwrap().ledger
}

/// Test: User can modify their own email (property policy allows)
///
/// property-policy-tx-enforcement (john-allowed case)
#[tokio::test]
async fn modify_policy_allows_own_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_users(&fluree, "modify_policy_allows_own").await;

    // Policy: only identity's own user can modify their email
    let policy = json!([
        {
            "@id": "ex:emailRestriction",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/email"}],
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Only users can update their own emails.",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        },
        {
            "@id": "ex:defaultAllow",
            "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
            "f:allow": true
        }
    ]);

    // Build policy context with John as identity
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/johnIdentity"}),
        )])),
        default_allow: false,
        ..Default::default()
    };

    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // John updating his own email - should succeed
    let update_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "delete": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "insert": {
            "@id": "ex:john",
            "schema:email": "updated@flur.ee"
        }
    });

    // This should succeed - John is updating his own email
    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_ok(),
        "John should be allowed to update his own email: {:?}",
        result.err()
    );

    // Verify the update happened
    let (tx_result, _tally) = result.unwrap();
    let query = json!({
        "select": "?email",
        "where": {
            "@id": "http://example.org/ns/john",
            "http://schema.org/email": "?email"
        }
    });

    let query_result = support::query_jsonld(&fluree, &tx_result.ledger, &query)
        .await
        .unwrap();
    let rows = query_result.to_jsonld(&tx_result.ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should have 1 row, got: {arr:?}");
    assert_eq!(
        arr[0], "updated@flur.ee",
        "Email should be updated, got: {arr:?}"
    );
}

/// Test: User cannot modify another user's email (property policy denies)
///
/// property-policy-tx-enforcement (alice-not-allowed case)
#[tokio::test]
async fn modify_policy_denies_other_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_users(&fluree, "modify_policy_denies_other").await;

    // Policy: only identity's own user can modify their email
    let policy = json!([
        {
            "@id": "ex:emailRestriction",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/email"}],
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Only users can update their own emails.",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        },
        {
            "@id": "ex:defaultAllow",
            "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
            "f:allow": true
        }
    ]);

    // Build policy context with Alice as identity
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/aliceIdentity"}),
        )])),
        default_allow: false,
        ..Default::default()
    };

    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // Alice trying to update John's email - should fail
    let update_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "delete": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "insert": {
            "@id": "ex:john",
            "schema:email": "hacked@evil.com"
        }
    });

    // This should fail - Alice is not allowed to update John's email
    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_err(),
        "Alice should NOT be allowed to update John's email"
    );

    let err = result.unwrap_err();

    // Should contain the custom error message
    assert_eq!(
        err.error, "Only users can update their own emails.",
        "Error should be the custom f:exMessage"
    );
}

/// Test: View-only policy blocks all modifications
///
/// view-only-policy-restricts-tx (first test case)
#[tokio::test]
async fn view_only_policy_blocks_modify() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "view_only_blocks");

    // Insert some data
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:alice",
            "ex:name": "Alice"
        }
    });

    let ledger = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Policy: only allows view, no modify
    let policy = json!([{
        "@id": "ex:viewOnly",
        "f:action": [{"@id": "f:view"}],
        "f:allow": true
    }]);

    // Build policy context (no modify policies)
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: false,
        ..Default::default()
    };

    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // Try to insert new data - should fail
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:john",
            "ex:name": "John"
        }
    });

    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_err(),
        "View-only policy should block modifications, but got: {result:?}"
    );

    let err = result.unwrap_err();
    // "Database policy denies all modifications."
    // Rust uses: "Policy enforcement prevents modification."
    assert!(
        err.error.contains("denied")
            || err.error.contains("Policy")
            || err.error.contains("modification"),
        "Error should indicate policy denial: {}",
        err.error
    );
}

/// Test: Always-false modify query denies with custom message
///
/// view-only-policy-restricts-tx (second test case)
#[tokio::test]
async fn modify_query_always_false_denies() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "modify_always_false");

    // Insert some data
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:alice",
            "ex:name": "Alice"
        }
    });

    let ledger = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Policy: modify query always returns false (impossible pattern)
    let policy = json!([
        {
            "@id": "ex:alwaysFalseModify",
            "f:required": true,
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Sample policy always returns false - denied!",
            "f:query": serde_json::to_string(&json!({
                "where": {"http://nonexistent.org/blah": "?$this"}
            })).unwrap()
        },
        {
            "@id": "ex:viewAll",
            "f:action": [{"@id": "f:view"}],
            "f:allow": true
        }
    ]);

    // Build policy context
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: false,
        ..Default::default()
    };

    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // Try to insert new data - should fail with custom message
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:john",
            "ex:name": "John"
        }
    });

    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(result.is_err(), "Always-false modify query should deny");

    let err = result.unwrap_err();
    // The custom message should be in the error
    assert_eq!(
        err.error, "Sample policy always returns false - denied!",
        "Error should be the custom f:exMessage"
    );
}
