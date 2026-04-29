//! Policy + query-connection integration tests
//!
//! Focus:
//! - identity-based policy loading via `f:policyClass` on the identity subject
//! - view policy enforcement on direct selects and graph crawl formatting

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{assert_index_defaults, normalize_rows, seed_people_with_ssn};

#[tokio::test]
async fn policy_inline_denies_restricted_property_in_direct_select() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/inline:main").await;

    // Inline policy: deny viewing `schema:ssn` for everyone.
    //
    // We set `default-allow: true` so other properties remain visible:
    // default_allow only applies when *no* policies apply for a flake).
    // NOTE: Rust `opts.policy` expects **a policy object or array of policy objects**,
    // not a JSON-LD wrapper like `{"@graph":[...]}`.
    let policy = json!([{
        "@id": "ex:ssnRestriction",
        "f:required": true,
        // Use fully-expanded IRI here to avoid any namespace/term-resolution ambiguity.
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("policy/inline:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Denying schema:ssn removes all solutions to a query that requires schema:ssn.
    assert_eq!(jsonld, json!([]));
}

#[tokio::test]
async fn policy_inline_denies_restricted_property_in_graph_crawl() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/inline:main").await;

    // NOTE: Rust `opts.policy` expects **a policy object or array of policy objects**,
    // not a JSON-LD wrapper like `{"@graph":[...]}`.
    let policy = json!([{
        "@id": "ex:ssnRestriction",
        "f:required": true,
        // Use fully-expanded IRI here to avoid any namespace/term-resolution ambiguity.
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "@type": "ex:User" }
    });

    // Sanity check: flat selects should still work (default-allow allows all non-SSN predicates).
    let sanity = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": query["opts"]["policy"].clone(),
            "default-allow": true
        },
        "select": "?name",
        "where": { "@id": "?s", "@type": "ex:User", "schema:name": "?name" }
    });
    let sanity_result = fluree
        .query_connection(&sanity)
        .await
        .expect("sanity query_connection");
    let ledger = fluree.ledger("policy/inline:main").await.expect("ledger");
    let sanity_jsonld = sanity_result
        .to_jsonld(&ledger.snapshot)
        .expect("sanity to_jsonld");
    assert_eq!(
        normalize_rows(&sanity_jsonld),
        normalize_rows(&json!(["Alice", "John"]))
    );

    // Use the tracked connection query entrypoint, which performs **policy-aware**
    // graph crawl formatting.
    let tracked = fluree
        .query_connection_tracked(&query)
        .await
        .expect("query_connection_tracked");
    let jsonld = tracked.result;

    // In a crawl, `schema:ssn` is removed everywhere, while other fields remain.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@flur.ee",
                "schema:birthDate": "2022-08-17"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:email": "john@flur.ee",
                "schema:birthDate": "2021-08-17"
            }
        ]))
    );
}

#[tokio::test]
async fn policy_per_source_override_takes_precedence_over_global() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/per-source:main").await;

    // Query with global policy (default-allow: false) but per-source override (default-allow: true).
    // The per-source policy should take precedence, allowing data visibility.
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": {
            "@id": "policy/per-source:main",
            "policy": {
                "default-allow": true
            }
        },
        "opts": {
            "default-allow": false
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/per-source:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per-source policy (default-allow: true) should allow data visibility
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "John"]))
    );
}

#[tokio::test]
async fn policy_per_source_override_denies_when_global_allows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_people_with_ssn(&fluree, "policy/per-source-deny:main").await;

    // Per-source policy with an explicit deny rule for schema:name.
    // Global policy uses default-allow: true, but per-source has a deny rule.
    // The per-source policy should take precedence, denying the specific property.
    let deny_name_policy = json!([{
        "@id": "ex:nameRestriction",
        "f:required": true,
        "f:onProperty": [{"@id": "http://schema.org/name"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "from": {
            "@id": "policy/per-source-deny:main",
            "policy": {
                "policy": deny_name_policy,
                "default-allow": true
            }
        },
        "opts": {
            "default-allow": true
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/per-source-deny:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per-source policy denies schema:name, so query returns empty
    assert_eq!(jsonld, json!([]));
}
