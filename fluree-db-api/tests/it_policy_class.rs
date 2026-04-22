//! Policy class-based query tests.
//!
//! Scenario reference: `fluree.snapshot.policy.policy-class-test`
//!
//! Tests policy class lookup where policies are stored in the database
//! and loaded via f:policyClass references.

mod support;

use fluree_db_api::policy_builder;
use fluree_db_api::{FlureeBuilder, QueryConnectionOptions};
use serde_json::json;
use std::collections::HashMap;
use support::{assert_index_defaults, genesis_ledger};

/// Test: Policy class restricts SSN visibility to own user
///
/// class-policy-query (first test case)
#[tokio::test]
async fn policy_class_restricts_ssn_to_own_user() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_class_ssn");

    // Create users, identity with policyClass, and policies stored in DB
    let setup = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            // Users
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
            // Product (for testing non-User queries still work)
            {
                "@id": "http://example.org/ns/widget",
                "@type": "http://example.org/ns/Product",
                "http://schema.org/name": "Widget",
                "http://schema.org/price": 99.99
            },
            // Identity with policyClass assignment
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "https://ns.flur.ee/db#policyClass": [{"@id": "http://example.org/ns/EmployeePolicy"}],
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            // SSN restriction policy - stored in DB with type EmployeePolicy
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            // Default allow policy for other properties
            {
                "@id": "http://example.org/ns/defaultAllowView",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({})).unwrap()
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &setup).await.unwrap().ledger;

    // Build policy context using policy_class option with Alice's identity
    let qc_opts = QueryConnectionOptions {
        policy_class: Some(vec!["http://example.org/ns/EmployeePolicy".to_string()]),
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

    // Query for all SSNs - should only see Alice's
    let query = json!({
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "http://example.org/ns/User",
            "http://schema.org/ssn": "?ssn"
        }
    });

    let result = support::query_jsonld_with_policy(&fluree, &ledger, &query, &policy_ctx)
        .await
        .expect("query with policy");

    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // Should only have one result (Alice's SSN)
    assert_eq!(arr.len(), 1, "Should only see Alice's SSN, got: {arr:?}");

    // The result should contain Alice's SSN
    let row = arr[0].as_array().unwrap();
    let ssn = row[1].as_str().unwrap();
    assert_eq!(ssn, "111-11-1111", "Should see Alice's SSN");
}

/// Test: Policy class allows viewing non-restricted properties
///
/// class-policy-query (in a graph crawl restricts)
#[tokio::test]
async fn policy_class_allows_non_restricted_properties() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_class_non_restricted");

    // Same setup as above
    let setup = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
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
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "https://ns.flur.ee/db#policyClass": [{"@id": "http://example.org/ns/EmployeePolicy"}],
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            {
                "@id": "http://example.org/ns/defaultAllowView",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({})).unwrap()
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &setup).await.unwrap().ledger;

    let qc_opts = QueryConnectionOptions {
        policy_class: Some(vec!["http://example.org/ns/EmployeePolicy".to_string()]),
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

    // Query for names - should see both Alice and John (name is not restricted)
    let query = json!({
        "select": ["?s", "?name"],
        "where": {
            "@id": "?s",
            "@type": "http://example.org/ns/User",
            "http://schema.org/name": "?name"
        }
    });

    let result = support::query_jsonld_with_policy(&fluree, &ledger, &query, &policy_ctx)
        .await
        .expect("query with policy");

    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // Should see both users' names
    assert_eq!(arr.len(), 2, "Should see both users' names, got: {arr:?}");
}

/// Test: Policy blocks query for another user's restricted SSN
///
/// class-policy-query (with where-clause match of restricted data)
#[tokio::test]
async fn policy_class_blocks_other_user_ssn_in_where() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_class_blocks_other");

    let setup = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "http://example.org/ns/alice",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "Alice",
                "http://schema.org/ssn": "111-11-1111"
            },
            {
                "@id": "http://example.org/ns/john",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "John",
                "http://schema.org/ssn": "888-88-8888"
            },
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "https://ns.flur.ee/db#policyClass": [{"@id": "http://example.org/ns/EmployeePolicy"}],
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            {
                "@id": "http://example.org/ns/defaultAllowView",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({})).unwrap()
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &setup).await.unwrap().ledger;

    let qc_opts = QueryConnectionOptions {
        policy_class: Some(vec!["http://example.org/ns/EmployeePolicy".to_string()]),
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

    // Query for user with John's SSN - should return empty (Alice can't see John's SSN)
    let query = json!({
        "select": ["?s"],
        "where": {
            "@id": "?s",
            "http://schema.org/ssn": "888-88-8888"
        }
    });

    let result = support::query_jsonld_with_policy(&fluree, &ledger, &query, &policy_ctx)
        .await
        .expect("query with policy");

    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    // John's SSN should not be visible - results should be empty
    assert_eq!(
        arr.len(),
        0,
        "Query for John's SSN should return empty, got: {arr:?}"
    );
}
