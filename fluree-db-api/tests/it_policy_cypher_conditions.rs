//! Cypher policy condition integration tests
//!
//! `f:query` policy conditions can be written in openCypher by storing the
//! literal with the `f:cypher` datatype (or passing
//! `{"@type": "f:cypher", "@value": "MATCH ..."}` inline). These tests verify:
//! 1. Inline Cypher conditions enforce correctly ($this as a parameter)
//! 2. Stored Cypher conditions enforce correctly ($identity as a parameter)
//! 3. Write / non-query statements fail closed (deny), never open
//! 4. An unbound identity never satisfies an identity-referencing condition

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::policy_builder;
use fluree_db_api::{FlureeBuilder, GovernanceOptions};
use serde_json::json;
use std::collections::HashMap;

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

/// Inline (request-opts) policy with a Cypher condition: `$this` arrives as
/// a Cypher parameter carrying the subject IRI, compared via `id()`.
#[tokio::test]
async fn cypher_policy_inline_property_condition() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_leveled_data(&fluree, "policy/cypher-inline:main").await;

    let policy = json!([{
        "@id": "ex:levelPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:query": {
            "@type": "f:cypher",
            "@value": "MATCH (t {`http://example.org/ns/level`: 0}) WHERE id(t) = $this RETURN t"
        }
    }]);

    let query = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "from": "policy/cypher-inline:main",
        "opts": { "policy": policy, "default-allow": false },
        "select": "?name",
        "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/cypher-inline:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Public Item"])),
        "Cypher policy should only allow level-0 items"
    );
}

/// Stored policy with a Cypher relationship condition: SSN visible only to
/// the owning user, mirroring the SPARQL version in `it_policy_sparql.rs`.
#[tokio::test]
async fn cypher_policy_stored_relationship_condition() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy_cypher_ssn");

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
            // SSN restriction policy - Cypher relationship condition
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/EmployeePolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": {
                    "@type": "https://ns.flur.ee/db#cypher",
                    "@value": "MATCH (i)-[:`http://example.org/ns/user`]->(t) WHERE id(i) = $identity AND id(t) = $this RETURN i"
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
        "Cypher condition should only reveal Alice's SSN, got: {arr:?}"
    );
    let row = arr[0].as_array().unwrap();
    assert_eq!(row[1].as_str().unwrap(), "111-11-1111");

    // Unbound identity: the same stored policy evaluated without an
    // identity binding must never hold ($identity substitutes as null).
    let anon_opts = GovernanceOptions {
        policy_class: Some(vec!["http://example.org/ns/EmployeePolicy".to_string()]),
        default_allow: false,
        ..Default::default()
    };
    let anon_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &anon_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    match support::query_jsonld_with_policy(&fluree, &ledger, &query, &anon_ctx).await {
        Err(_) => {} // a condition error is fail-closed
        Ok(result) => {
            let rendered = result.to_jsonld(&ledger.snapshot).unwrap().to_string();
            assert!(
                !rendered.contains("111-11-1111") && !rendered.contains("888-88-8888"),
                "unbound identity must not satisfy the condition: {rendered}"
            );
        }
    }
}

/// A Cypher policy source that fails to parse — or is not a read-only query —
/// must fail closed: the policy becomes a deny, revealing nothing.
#[tokio::test]
async fn cypher_policy_invalid_fails_closed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed_leveled_data(&fluree, "policy/cypher-invalid:main").await;

    for bad_source in [
        "THIS IS NOT CYPHER",
        // Valid Cypher, but a write — rejected at policy build
        "CREATE (n:Backdoor {open: true})",
        "MATCH (n) SET n.level = 0",
    ] {
        let policy = json!([{
            "@id": "ex:badPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:query": { "@type": "f:cypher", "@value": bad_source }
        }]);

        let query = json!({
            "@context": { "ex": "http://example.org/ns/" },
            "from": "policy/cypher-invalid:main",
            "opts": { "policy": policy, "default-allow": false },
            "select": "?name",
            "where": [{ "@id": "?item", "@type": "ex:Item", "ex:name": "?name" }]
        });

        let result = fluree
            .query_connection(&query)
            .await
            .expect("query_connection");
        let ledger = fluree
            .ledger("policy/cypher-invalid:main")
            .await
            .expect("ledger");
        let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

        assert_eq!(
            jsonld.as_array().map(Vec::len),
            Some(0),
            "invalid Cypher policy ({bad_source:?}) must deny everything, got: {jsonld:?}"
        );
    }
}
