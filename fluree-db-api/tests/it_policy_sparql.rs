//! SPARQL policy query integration tests
//!
//! `f:query` policy conditions can be written in SPARQL by storing the
//! literal with the `f:sparql` datatype (or passing
//! `{"@type": "f:sparql", "@value": "ASK ..."}` inline). These tests verify:
//! 1. Stored policies with SPARQL ASK conditions enforce correctly
//! 2. Inline (request-opts) SPARQL policies enforce correctly
//! 3. Special variables bind SHACL-SPARQL style (`$this` / `$identity`)
//! 4. Invalid SPARQL fails closed (deny), never open

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::policy_builder;
use fluree_db_api::{FlureeBuilder, GovernanceOptions};
use serde_json::json;
use std::collections::HashMap;

/// Stored policy with a SPARQL ASK condition: SSN visible only to the
/// owning user, mirroring the JSON-LD version in `it_policy_class.rs`.
#[tokio::test]
async fn sparql_policy_stored_ask_restricts_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_sparql_ssn");

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
            // SSN restriction policy - SPARQL ASK condition
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "ASK { $identity <http://example.org/ns/user> $this }"
                }
            },
            // Default allow policy for other properties (empty JSON-LD query)
            {
                "@id": "http://example.org/ns/defaultAllowView",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({})).unwrap()
            }
        ]
    });

    let ledger = fluree.insert(ledger0, &setup).await.unwrap().ledger;

    let qc_opts = GovernanceOptions {
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

    assert_eq!(
        arr.len(),
        1,
        "SPARQL ASK policy should only reveal Alice's SSN, got: {arr:?}"
    );
    let row = arr[0].as_array().unwrap();
    assert_eq!(row[1].as_str().unwrap(), "111-11-1111");
}

/// Helper to seed items with a numeric classification level.
async fn seed_leveled_data(fluree: &support::MemoryFluree, ledger_id: &str) {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let txn = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            { "@id": "ex:item-public", "@type": "ex:Item", "ex:name": "Public Item", "ex:level": 0 },
            { "@id": "ex:item-secret", "@type": "ex:Item", "ex:name": "Secret Item", "ex:level": 5 }
        ]
    });
    let _ = fluree.insert(ledger0, &txn).await.expect("seed");
}

/// Inline (request-opts) policy with a SPARQL ASK condition.
#[tokio::test]
async fn sparql_policy_inline_ask() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_leveled_data(&fluree, "policy/sparql-inline:main").await;

    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:query": {
            "@type": "f:sparql",
            "@value": "ASK { $this <http://example.org/ns/level> 0 }"
        }
    }]);

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": "policy/sparql-inline:main",
        "opts": { "policy": policy, "default-allow": false },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/sparql-inline:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item"])),
        "SPARQL policy should only allow level-0 items"
    );
}

/// A SPARQL policy source that fails to parse (or is not ASK/SELECT) must
/// fail closed: the policy becomes a deny, revealing nothing.
#[tokio::test]
async fn sparql_policy_invalid_fails_closed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_leveled_data(&fluree, "policy/sparql-invalid:main").await;

    for bad_source in [
        "THIS IS NOT SPARQL",
        // Valid SPARQL, but not ASK/SELECT — rejected at policy build
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    ] {
        let policy = json!([{
            "@id": "ex:badPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:query": { "@type": "f:sparql", "@value": bad_source }
        }]);

        let query = json!({
            "@context": { "ex": "http://example.org/ns/" },
            "from": "policy/sparql-invalid:main",
            "opts": { "policy": policy, "default-allow": false },
            "select": "?name",
            "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
        });

        let result = fluree
            .query_connection(&query)
            .await
            .expect("query_connection");
        let ledger = fluree
            .ledger("policy/sparql-invalid:main")
            .await
            .expect("ledger");
        let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

        assert_eq!(
            jsonld.as_array().map(Vec::len),
            Some(0),
            "invalid SPARQL policy ({bad_source:?}) must deny everything, got: {jsonld:?}"
        );
    }
}

/// SPARQL SELECT form is also accepted for policy conditions.
#[tokio::test]
async fn sparql_policy_select_form() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_leveled_data(&fluree, "policy/sparql-select:main").await;

    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:query": {
            "@type": "f:sparql",
            "@value": "SELECT ?this WHERE { $this <http://example.org/ns/level> ?lvl FILTER(?lvl < 3) }"
        }
    }]);

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": "policy/sparql-select:main",
        "opts": { "policy": policy, "default-allow": false },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/sparql-select:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item"])),
        "SPARQL SELECT policy with FILTER should only allow level < 3"
    );
}

/// Positional `$identity` condition, anonymous request: the canonical stored
/// idiom `ASK { $identity <ex:user> $this }` binds `$identity` in *subject
/// position*, not a FILTER comparison. Seeding an unbound identity as UNDEF
/// would make it an unconstrained variable that matches any row (VALUES-UNDEF
/// is compatible with anything), so the identity constraint would vanish and
/// leak every SSN. The never-match sentinel makes it fail closed instead.
#[tokio::test]
async fn sparql_policy_positional_identity_unbound_fails_closed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_sparql_unbound_identity");

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
                "https://ns.flur.ee/db#query": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "ASK { $identity <http://example.org/ns/user> $this }"
                }
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

    // Anonymous request: policy class selected, but NO identity provided.
    let anon_opts = GovernanceOptions {
        policy_class: Some(vec!["http://example.org/ns/EmployeePolicy".to_string()]),
        default_allow: false,
        ..Default::default()
    };
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &anon_opts,
        &[0],
    )
    .await
    .expect("build policy context");

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
    let rendered = result.to_jsonld(&ledger.snapshot).unwrap().to_string();

    assert!(
        !rendered.contains("111-11-1111") && !rendered.contains("888-88-8888"),
        "unbound identity must not satisfy a positional $identity condition; \
         no SSN may leak, got: {rendered}"
    );
}

/// The bound-identity path must still work after the unbound fix: Alice's
/// identity resolves and the positional condition holds for her row. Pins
/// that the sentinel change did not break legitimate positional binding.
#[tokio::test]
async fn sparql_policy_positional_identity_bound_still_allows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_sparql_bound_identity");

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
                "https://ns.flur.ee/db#query": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "ASK { $identity <http://example.org/ns/user> $this }"
                }
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

    let qc_opts = GovernanceOptions {
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

    assert_eq!(arr.len(), 1, "only Alice's SSN should be visible: {arr:?}");
    assert_eq!(
        arr[0].as_array().unwrap()[1].as_str().unwrap(),
        "111-11-1111"
    );
}
