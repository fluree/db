//! Regression tests: policy enforcement survives binary indexing + ledger reload.
//!
//! Root cause: after binary indexing, `BinaryScanOperator` used late
//! materialization (epoch=0) returning `Binding::EncodedSid` that
//! `load_policies_by_identity` silently dropped via `binding.as_sid()`,
//! disabling identity-based policy enforcement.
//!
//! The fix adds `GraphDbRef::eager()` which forces resolved bindings.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::policy_builder;
use fluree_db_api::{FlureeBuilder, IndexConfig, QueryConnectionOptions};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{start_background_indexer_local, trigger_index_and_wait_outcome};

/// Identity-based `f:policyClass` enforcement must survive binary indexing.
///
/// Before the fix, `load_policies_by_identity` used `execute_pattern_with_overlay_at`
/// without `GraphDbRef::eager()`, so after indexing (epoch=0), the `EncodedSid` bindings
/// from the binary scan were silently dropped by `as_sid()`, causing
/// `FoundNoPolicies` instead of `FoundWithPolicies` — effectively disabling
/// access control.
#[tokio::test]
async fn policy_class_survives_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/policy-survives-index:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Seed users, identity with policyClass, and policies
            let setup = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "schema": "http://schema.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:User",
                        "schema:name": "Alice",
                        "schema:ssn": "111-11-1111"
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:User",
                        "schema:name": "Bob",
                        "schema:ssn": "222-22-2222"
                    },
                    // Identity with policyClass
                    {
                        "@id": "ex:aliceIdentity",
                        "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
                        "ex:user": {"@id": "ex:alice"}
                    },
                    // SSN restriction: only visible if ?$identity.ex:user == ?$this
                    {
                        "@id": "ex:ssnRestriction",
                        "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                        "f:required": true,
                        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
                        "f:action": {"@id": "f:view"},
                        "f:query": {
                            "@value": "{\"@context\":{\"ex\":\"http://example.org/ns/\"},\"where\":{\"@id\":\"?$identity\",\"ex:user\":{\"@id\":\"?$this\"}}}",
                            "@type": "http://www.w3.org/2001/XMLSchema#string"
                        }
                    },
                    // Default allow for all other properties
                    {
                        "@id": "ex:defaultAllow",
                        "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                        "f:action": {"@id": "f:view"},
                        "f:query": {
                            "@value": "{}",
                            "@type": "http://www.w3.org/2001/XMLSchema#string"
                        }
                    }
                ]
            });

            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &setup,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .unwrap();

            let alice_opts = QueryConnectionOptions {
                identity: Some("http://example.org/ns/aliceIdentity".to_string()),
                default_allow: false,
                ..Default::default()
            };

            // Pre-indexing: verify policy restricts SSN to Alice only
            {
                let policy_ctx = policy_builder::build_policy_context_from_opts(
                    &r1.ledger.snapshot,
                    r1.ledger.novelty.as_ref(),
                    None,
                    r1.ledger.t(),
                    &alice_opts,
                    &[0],
                )
                .await
                .expect("build policy pre-index");

                let query = json!({
                    "select": ["?name", "?ssn"],
                    "where": {
                        "@id": "?u",
                        "@type": "http://example.org/ns/User",
                        "http://schema.org/name": "?name",
                        "http://schema.org/ssn": "?ssn"
                    }
                });

                let result = support::query_jsonld_with_policy(
                    &fluree, &r1.ledger, &query, &policy_ctx,
                )
                .await
                .expect("query pre-index");
                let jsonld = result
                    .to_jsonld(&r1.ledger.snapshot)
                    .expect("to_jsonld pre-index");
                let rows = jsonld.as_array().expect("array");

                assert_eq!(
                    rows.len(),
                    1,
                    "pre-index: only Alice's SSN should be visible, got: {jsonld:#?}"
                );

                let row_str = rows[0].to_string();
                assert!(
                    row_str.contains("111-11-1111"),
                    "pre-index: returned row should contain Alice's SSN (111-11-1111), got: {row_str}"
                );
                assert!(
                    !row_str.contains("222-22-2222"),
                    "pre-index: returned row must NOT contain Bob's SSN (222-22-2222), got: {row_str}"
                );
            }

            // Index + reload
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger_indexed.snapshot.range_provider.is_some(),
                "ledger should have binary index after indexing"
            );

            // Post-indexing: verify policy STILL restricts SSN to Alice only
            {
                let policy_ctx = policy_builder::build_policy_context_from_opts(
                    &ledger_indexed.snapshot,
                    ledger_indexed.novelty.as_ref(),
                    None,
                    ledger_indexed.t(),
                    &alice_opts,
                    &[0],
                )
                .await
                .expect("build policy post-index");

                let query = json!({
                    "select": ["?name", "?ssn"],
                    "where": {
                        "@id": "?u",
                        "@type": "http://example.org/ns/User",
                        "http://schema.org/name": "?name",
                        "http://schema.org/ssn": "?ssn"
                    }
                });

                let result = support::query_jsonld_with_policy(
                    &fluree, &ledger_indexed, &query, &policy_ctx,
                )
                .await
                .expect("query post-index");
                let jsonld = result
                    .to_jsonld(&ledger_indexed.snapshot)
                    .expect("to_jsonld post-index");
                let rows = jsonld.as_array().expect("array");

                assert_eq!(
                    rows.len(),
                    1,
                    "post-index: policy enforcement must survive indexing — only Alice's SSN should be visible, got: {jsonld:#?}"
                );

                let row_str = rows[0].to_string();
                assert!(
                    row_str.contains("111-11-1111"),
                    "post-index: returned row should contain Alice's SSN (111-11-1111), got: {row_str}"
                );
                assert!(
                    !row_str.contains("222-22-2222"),
                    "post-index: returned row must NOT contain Bob's SSN (222-22-2222), got: {row_str}"
                );
            }
        })
        .await;
}
