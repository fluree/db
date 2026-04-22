//! Policy f:query integration tests
//!
//! Tests f:query policy evaluation using the main query parser/IR.
//!
//! These tests verify that the refactored policy query execution:
//! 1. Uses the main query parser (not a bespoke policy parser)
//! 2. Supports FILTER expressions
//! 3. Properly injects ?$this and ?$identity

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{assert_index_defaults, genesis_ledger, normalize_rows, MemoryFluree};

/// Diagnostic test: verify basic policy mechanism works with f:allow: true
#[tokio::test]
async fn policy_baseline_allow_true() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = genesis_ledger(&fluree, "policy/baseline:main");
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            { "@id": "ex:item1", "@type": "ex:Item", "ex:name": "Item One" }
        ]
    });
    let _ = fluree.insert(ledger0, &txn).await.expect("insert");

    // Policy with f:allow: true - should allow everything
    let policy = json!([{
        "@id": "ex:allowPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:allow": true
    }]);

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": "policy/baseline:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("policy/baseline:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Item One"])),
        "f:allow: true should allow all items"
    );
}

/// Helper to seed simple classified data
async fn seed_classified_data(fluree: &MemoryFluree, ledger_id: &str) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:item-public",
                "@type": "ex:Item",
                "ex:name": "Public Item",
                "ex:level": 0
            },
            {
                "@id": "ex:item-secret",
                "@type": "ex:Item",
                "ex:name": "Secret Item",
                "ex:level": 5
            },
            {
                "@id": "ex:item-topsecret",
                "@type": "ex:Item",
                "ex:name": "Top Secret Item",
                "ex:level": 10
            }
        ]
    });

    let _ = fluree
        .insert(ledger0, &txn)
        .await
        .expect("seed should succeed");
}

/// Tests that f:query with a simple WHERE pattern works.
///
/// This is a baseline test that the main parser is being used for f:query.
/// The policy allows viewing items where ?$this has ex:level = 0.
#[tokio::test]
async fn policy_fquery_simple_where_pattern() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_classified_data(&fluree, "policy/fquery-simple:main").await;

    // Policy that allows only items with level = 0
    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        // Simple f:query - only allow if ?$this has level 0
        "f:query": {
            "@type": "@json",
            "@value": {
                "@context": { "ex": "http://example.org/ns/" },
                "where": [{ "@id": "?$this", "ex:level": 0 }]
            }
        }
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-simple:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/fquery-simple:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Only "Public Item" (level=0) should be visible
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item"])),
        "Only items with level=0 should be visible"
    );
}

/// Tests that f:query with FILTER expressions works.
///
/// Key test: FILTER should work in policy queries
/// because we now use the main query parser instead of a bespoke policy parser.
#[tokio::test]
async fn policy_fquery_with_filter_expression() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_classified_data(&fluree, "policy/fquery-filter:main").await;

    // Policy that allows items where level < 3 (using FILTER)
    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        // f:query with FILTER - allow if ?$this.level < 3
        "f:query": {
            "@type": "@json",
            "@value": {
                "@context": { "ex": "http://example.org/ns/" },
                "where": [
                    { "@id": "?$this", "ex:level": "?level" },
                    ["filter", "(< ?level 3)"]
                ]
            }
        }
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-filter:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/fquery-filter:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Only "Public Item" (level=0) should be visible; level < 3
    // "Secret Item" (level=5) and "Top Secret Item" (level=10) should be filtered out
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item"])),
        "Only items with level < 3 should be visible (FILTER in f:query)"
    );
}

/// Tests that default-allow works when NO policy applies.
///
/// default-allow only applies when NO policies apply to a flake.
/// When policies DO apply but their f:query returns no results, that's a deny.
#[tokio::test]
async fn policy_fquery_default_allow_fallback() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_classified_data(&fluree, "policy/fquery-default:main").await;

    // Policy with f:query that will never match (level = 999) and NO targeting.
    // Since there's no targeting (f:onProperty, f:onSubject, f:onClass), this is
    // a "default" policy that APPLIES to ALL flakes. The f:query just won't return results.
    let policy_no_targeting = json!([{
        "@id": "ex:impossiblePolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:query": {
            "@type": "@json",
            "@value": {
                "@context": { "ex": "http://example.org/ns/" },
                "where": [{ "@id": "?$this", "ex:level": 999 }]
            }
        }
    }]);

    // Query with default-allow: true BUT policy DOES apply (just returns false)
    // Expected: DENY because the policy applies and its f:query returned false
    let query_with_applicable_policy = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-default:main",
        "opts": {
            "policy": policy_no_targeting,
            "default-allow": true
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query_with_applicable_policy)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/fquery-default:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // The policy APPLIED (it ran its f:query), but the query returned false.
    // Therefore the policy didn't "allow", and default-allow doesn't override this.
    // Semantics: default-allow only applies when NO policies apply.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([])),
        "When a policy applies but its f:query returns false, items are denied (default-allow doesn't override)"
    );

    // Now test with a policy that has TARGETING that won't match our items' properties.
    // This policy only targets property "ex:nonexistent", so it won't apply to ex:name flakes.
    // NOTE: We use full IRIs in policy targeting because the policy builder doesn't expand
    // compact IRIs from the query context.
    let policy_with_targeting = json!([{
        "@id": "http://example.org/ns/targetedPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onProperty": { "@id": "http://example.org/ns/nonexistent" },
        "f:query": {
            "@type": "@json",
            "@value": {
                "@context": { "ex": "http://example.org/ns/" },
                "where": [{ "@id": "?$this", "ex:level": 999 }]
            }
        }
    }]);

    // Query with default-allow: true AND policy that doesn't apply
    // Expected: ALLOW (default-allow) because the policy doesn't target ex:name
    let query_no_applicable_policy = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-default:main",
        "opts": {
            "policy": policy_with_targeting.clone(),
            "default-allow": true
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result_allow = fluree
        .query_connection(&query_no_applicable_policy)
        .await
        .expect("query_connection");
    let jsonld_allow = result_allow.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // The policy doesn't apply (targets ex:nonexistent, not ex:name), so default-allow kicks in
    assert_eq!(
        normalize_rows(&jsonld_allow),
        normalize_rows(&json!(["Public Item", "Secret Item", "Top Secret Item"])),
        "When NO policy applies, default-allow: true allows all items"
    );

    // Same policy with default-allow: false
    let query_deny = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-default:main",
        "opts": {
            "policy": policy_with_targeting,
            "default-allow": false
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result_deny = fluree
        .query_connection(&query_deny)
        .await
        .expect("query_connection");
    let jsonld_deny = result_deny.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // The policy doesn't apply, and default-allow is false
    assert_eq!(
        normalize_rows(&jsonld_deny),
        normalize_rows(&json!([])),
        "When NO policy applies and default-allow: false, items are denied"
    );
}

/// Tests that empty f:query {} allows access (matches anything).
///
/// An empty WHERE clause in f:query should succeed, allowing access.
/// This is used as a "default allow view" pattern.
#[tokio::test]
async fn policy_fquery_empty_where_allows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_classified_data(&fluree, "policy/fquery-empty:main").await;

    // Policy with empty f:query - should allow everything
    let policy = json!([{
        "@id": "ex:allowAllPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        // Empty f:query - should succeed for everything
        "f:query": {
            "@type": "@json",
            "@value": {}
        }
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "from": "policy/fquery-empty:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/fquery-empty:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Empty f:query should allow all items
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item", "Secret Item", "Top Secret Item"])),
        "Empty f:query should allow all items"
    );
}
