//! Identity-based policy integration tests
//!
//! Tests identity-based access control and policy restrictions.

mod support;

use fluree_db_api::{wrap_identity_policy_view, FlureeBuilder};
use serde_json::json;
use support::{assert_index_defaults, genesis_ledger};

/// Test inline policy with ?$identity binding.
///
/// This test verifies the policy-values pattern for binding ?$identity:
/// 1. Direct select binding restricts results based on identity
/// 2. Where-clause match of restricted data returns empty
#[tokio::test]
async fn inline_policy_with_identity_binding() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = genesis_ledger(&fluree, "policy/inline-policy-identity:main");

    // Use HTTP IRI for identity (avoid DID encoding issues)
    let alice_identity = "http://example.org/identity/alice";

    // Insert test data with users and identity link (no policies in DB)
    let test_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            // Users with sensitive SSN data
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:ssn": "111-11-1111"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:ssn": "888-88-8888"
            },
            // Identity with link to ex:alice
            {
                "@id": alice_identity,
                "ex:user": {"@id": "ex:alice"}
            }
        ]
    });

    let _ledger = fluree
        .insert(ledger0, &test_data)
        .await
        .expect("insert test data")
        .ledger;

    // Get a reference to the ledger for to_jsonld conversion
    let ledger = fluree
        .ledger("policy/inline-policy-identity:main")
        .await
        .expect("ledger");

    // Test 1: Query with inline policy should only return Alice's SSN
    let query1 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline-policy-identity:main",
        "opts": {
            "default-allow": true,
            "policy": [{
                "@id": "inline-ssn-policy",
                "f:required": true,
                "f:onProperty": [{"@id": "http://schema.org/ssn"}],
                "f:action": "f:view",
                "f:query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            }],
            "policy-values": {
                "?$identity": {"@id": alice_identity}
            }
        },
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:ssn": "?ssn"
        }
    });

    let result1 = fluree.query_connection(&query1).await.expect("query1");
    let result1_json = result1.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should only return Alice's SSN
    assert_eq!(
        result1_json,
        json!([["ex:alice", "111-11-1111"]]),
        "Test 1: Only Alice's SSN should be visible"
    );

    // Test 2: Query for John's SSN directly should return empty
    let query2 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline-policy-identity:main",
        "opts": {
            "default-allow": true,
            "policy": [{
                "@id": "inline-ssn-policy",
                "f:required": true,
                "f:onProperty": [{"@id": "http://schema.org/ssn"}],
                "f:action": "f:view",
                "f:query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            }],
            "policy-values": {
                "?$identity": {"@id": alice_identity}
            }
        },
        "select": "?s",
        "where": {
            "@id": "?s",
            "schema:ssn": "888-88-8888"
        }
    });

    let result2 = fluree.query_connection(&query2).await.expect("query2");
    let result2_json = result2.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return empty (John's SSN is not accessible to Alice)
    assert_eq!(
        result2_json,
        json!([]),
        "Test 2: John's SSN should not be accessible"
    );
}

/// Test identity-based policy lookup via f:policyClass.
///
/// This test verifies that policies stored in the database can be loaded
/// via the identity's f:policyClass property.
#[tokio::test]
async fn identity_based_policy_lookup() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = genesis_ledger(&fluree, "policy/identity-lookup:main");

    // Use HTTP IRI for identity
    let alice_identity = "http://example.org/identity/alice";

    // Insert test data with users, identity, and policies stored in DB
    let test_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            // Users with sensitive SSN data
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:ssn": "111-11-1111"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:ssn": "888-88-8888"
            },
            // Identity with f:policyClass and link to ex:alice
            {
                "@id": alice_identity,
                "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
                "ex:user": {"@id": "ex:alice"}
            },
            // SSN restriction policy stored in DB
            // Uses f:query to check if identity.ex:user = ?$this
            {
                "@id": "ex:ssnRestriction",
                "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                "f:required": true,
                "f:onProperty": [{"@id": "schema:ssn"}],
                "f:action": {"@id": "f:view"},
                "f:query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            // Default allow policy for all other properties
            {
                "@id": "ex:defaultAllowView",
                "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                "f:action": {"@id": "f:view"},
                "f:allow": true
            }
        ]
    });

    let _ledger = fluree
        .insert(ledger0, &test_data)
        .await
        .expect("insert test data")
        .ledger;

    // Get a reference to the ledger for to_jsonld conversion
    let ledger = fluree
        .ledger("policy/identity-lookup:main")
        .await
        .expect("ledger");

    // Test: Query with identity-based policy should only return Alice's SSN
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/identity-lookup:main",
        "opts": {
            "identity": alice_identity,
            "default-allow": true
        },
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree.query_connection(&query).await.expect("query");
    let result_json = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should only return Alice's SSN (John's is restricted by policy)
    assert_eq!(
        result_json,
        json!([["ex:alice", "111-11-1111"]]),
        "Identity-based policy should only show Alice's SSN"
    );

    // Test 2: Query for John's SSN directly should return empty
    let query2 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/identity-lookup:main",
        "opts": {
            "identity": alice_identity,
            "default-allow": true
        },
        "select": "?s",
        "where": {
            "@id": "?s",
            "schema:ssn": "888-88-8888"
        }
    });

    let result2 = fluree.query_connection(&query2).await.expect("query2");
    let result2_json = result2.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should return empty (John's SSN is not accessible via policy)
    assert_eq!(
        result2_json,
        json!([]),
        "John's SSN should not be accessible via identity-based policy"
    );
}

/// Test wrap_identity_policy_view API helper function.
///
/// This test verifies the `wrap_identity_policy_view` convenience function
/// that mirrors the legacy `wrap-identity-policy` helper.
#[tokio::test]
async fn wrap_identity_policy_view_api() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = genesis_ledger(&fluree, "policy/wrap-identity:main");

    // Use HTTP IRI for identity
    let alice_identity = "http://example.org/identity/alice";

    // Insert test data with users, identity, and policies stored in DB
    let test_data = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            // Users with sensitive SSN data
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:ssn": "111-11-1111"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:ssn": "888-88-8888"
            },
            // Identity with f:policyClass and link to ex:alice
            {
                "@id": alice_identity,
                "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
                "ex:user": {"@id": "ex:alice"}
            },
            // SSN restriction policy stored in DB
            {
                "@id": "ex:ssnRestriction",
                "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                "f:required": true,
                "f:onProperty": [{"@id": "schema:ssn"}],
                "f:action": {"@id": "f:view"},
                "f:query": serde_json::to_string(&json!({
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            // Default allow policy for all other properties
            {
                "@id": "ex:defaultAllowView",
                "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                "f:action": {"@id": "f:view"},
                "f:allow": true
            }
        ]
    });

    let ledger = fluree
        .insert(ledger0, &test_data)
        .await
        .expect("insert test data")
        .ledger;

    // Use wrap_identity_policy_view API to create a policy-wrapped view
    let wrapped = wrap_identity_policy_view(&ledger, alice_identity, true)
        .await
        .expect("wrap_identity_policy_view");

    // Verify the wrapped view has the expected properties
    assert!(!wrapped.is_root(), "Should not be root policy");

    // Query using query_with_policy and the policy context from the wrapped view
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:ssn": "?ssn"
        }
    });

    let result = support::query_jsonld_with_policy(&fluree, &ledger, &query, wrapped.policy())
        .await
        .expect("query_with_policy");
    let result_json = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Should only return Alice's SSN (John's is restricted by policy)
    assert_eq!(
        result_json,
        json!([["ex:alice", "111-11-1111"]]),
        "wrap_identity_policy_view should restrict to Alice's SSN only"
    );
}
