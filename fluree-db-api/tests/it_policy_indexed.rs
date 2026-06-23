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
use fluree_db_api::{FlureeBuilder, GovernanceOptions, IndexConfig};
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

            let alice_opts = GovernanceOptions {
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

/// Tripwire: the batched join lanes read raw leaflets and never run the
/// per-leaf `filter_flakes` policy filtering — the probe lane plans must
/// decline under a restrictive policy even on a fully indexed, novelty-free
/// ledger (they used to short-circuit to `Clean` on `overlay_free` before
/// checking the policy). This harness's routing currently keeps policy
/// queries off the batched lanes upstream, so the assertion guards against
/// any future routing change exposing the raw path.
#[tokio::test]
async fn policy_batched_join_lane_declines_index_only() {
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
            let ledger_id = "it/policy-batched-lane:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

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
                    {
                        "@id": "ex:aliceIdentity",
                        "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
                        "ex:user": {"@id": "ex:alice"}
                    },
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

            // Fully index — no novelty tail remains.
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger_indexed.snapshot.range_provider.is_some(),
                "ledger should have binary index after indexing"
            );

            let alice_opts = GovernanceOptions {
                identity: Some("http://example.org/ns/aliceIdentity".to_string()),
                default_allow: false,
                ..Default::default()
            };
            let policy_ctx = policy_builder::build_policy_context_from_opts(
                &ledger_indexed.snapshot,
                ledger_indexed.novelty.as_ref(),
                None,
                ledger_indexed.t(),
                &alice_opts,
                &[0],
            )
            .await
            .expect("build policy");

            // Two-pattern star: the planner gives this to the nested-loop
            // join, whose batched subject lane reads raw leaflets when not
            // declined — independent of the property-join driver capture the
            // 3-pattern test exercises.
            let query = json!({
                "select": ["?name", "?ssn"],
                "where": {
                    "@id": "?u",
                    "http://schema.org/name": "?name",
                    "http://schema.org/ssn": "?ssn"
                }
            });
            let result =
                support::query_jsonld_with_policy(&fluree, &ledger_indexed, &query, &policy_ctx)
                    .await
                    .expect("query");
            let jsonld = result
                .to_jsonld(&ledger_indexed.snapshot)
                .expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "index-only policy view: only Alice's SSN should be visible, got: {jsonld:#?}"
            );
            assert!(
                rows[0].to_string().contains("111-11-1111")
                    && !rows[0].to_string().contains("222-22-2222"),
                "index-only policy view must hide Bob's SSN, got: {jsonld:#?}"
            );
        })
        .await;
}

/// O1 plan-time uncovered-predicate skip: under a non-root view policy, a
/// single-predicate COUNT over a predicate the policy provably cannot restrict
/// keeps the metadata/cursor fast path instead of bailing to the filtered scan,
/// and yields the same result the per-flake evaluator would.
///
/// Setup: the only policy is an `f:onProperty schema:ssn` owner-gate (no default
/// or subject policy), so `schema:name` is uncovered while `schema:ssn` is
/// covered. Verified end-to-end on a fully indexed ledger:
/// - uncovered `:name`, `default_allow=true`  => Allow   => full count (2)
/// - covered   `:ssn`,  `default_allow=true`  => Decline => filtered (1, alice's own)
/// - uncovered `:name`, `default_allow=false` => Empty   => 0
#[tokio::test]
async fn policy_count_respects_predicate_coverage() {
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
            let ledger_id = "it/policy-count-coverage:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // alice + bob each have a visible name and a restricted ssn. The ONLY
            // policy is an f:onProperty ssn owner-gate — no default/subject policy
            // — so schema:name is provably uncovered, schema:ssn is covered.
            let setup = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "schema": "http://schema.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:ssn": "111-11-1111"},
                    {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "schema:ssn": "222-22-2222"},
                    {"@id": "ex:aliceIdentity", "f:policyClass": [{"@id": "ex:EmployeePolicy"}], "ex:user": {"@id": "ex:alice"}},
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
                    }
                ]
            });
            let r1 = fluree
                .upsert_with_opts(ledger, &setup, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(
                ledger_indexed.snapshot.range_provider.is_some(),
                "ledger should have binary index after indexing"
            );

            let name_count = json!({
                "select": "(count ?o)",
                "where": {"@id": "?s", "http://schema.org/name": "?o"}
            });
            let ssn_count = json!({
                "select": "(count ?o)",
                "where": {"@id": "?s", "http://schema.org/ssn": "?o"}
            });

            let opts_allow = GovernanceOptions {
                identity: Some("http://example.org/ns/aliceIdentity".to_string()),
                default_allow: true,
                ..Default::default()
            };
            let opts_deny = GovernanceOptions {
                identity: Some("http://example.org/ns/aliceIdentity".to_string()),
                default_allow: false,
                ..Default::default()
            };

            // default_allow = true: uncovered :name keeps the fast path (counts
            // both rows); covered :ssn declines to the filtered scan (alice's own).
            let ctx_allow = policy_builder::build_policy_context_from_opts(
                &ledger_indexed.snapshot,
                ledger_indexed.novelty.as_ref(),
                None,
                ledger_indexed.t(),
                &opts_allow,
                &[0],
            )
            .await
            .expect("build policy (allow)");
            let name_allow = support::query_jsonld_with_policy(&fluree, &ledger_indexed, &name_count, &ctx_allow)
                .await
                .expect("name count (allow)");
            assert_eq!(
                name_allow.to_jsonld(&ledger_indexed.snapshot).expect("jsonld"),
                json!([2]),
                "uncovered :name with default_allow=true must count all rows via the fast path"
            );
            let ssn_allow = support::query_jsonld_with_policy(&fluree, &ledger_indexed, &ssn_count, &ctx_allow)
                .await
                .expect("ssn count (allow)");
            assert_eq!(
                ssn_allow.to_jsonld(&ledger_indexed.snapshot).expect("jsonld"),
                json!([1]),
                "covered :ssn must filter to alice's own row (no leak of Bob's ssn into the count)"
            );

            // default_allow = false: uncovered :name is wholly hidden => 0.
            let ctx_deny = policy_builder::build_policy_context_from_opts(
                &ledger_indexed.snapshot,
                ledger_indexed.novelty.as_ref(),
                None,
                ledger_indexed.t(),
                &opts_deny,
                &[0],
            )
            .await
            .expect("build policy (deny)");
            let name_deny = support::query_jsonld_with_policy(&fluree, &ledger_indexed, &name_count, &ctx_deny)
                .await
                .expect("name count (deny)");
            assert_eq!(
                name_deny.to_jsonld(&ledger_indexed.snapshot).expect("jsonld"),
                json!([0]),
                "uncovered :name with default_allow=false must short-circuit to an empty count"
            );
        })
        .await;
}

/// StatsCountByPredicate §2.4 leak: `SELECT ?p (COUNT ?s) WHERE { ?s ?p ?o }
/// GROUP BY ?p` answers from whole-index StatsView with no per-flake filtering.
/// Under a non-root view policy the operator must delegate to the generic
/// (scan-based) GROUP BY so per-predicate counts reflect the policy, not the
/// raw index. Here `schema:ssn` is owner-gated: alice sees only her own.
#[tokio::test]
async fn policy_stats_count_by_predicate_uses_filtered_fallback() {
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
            let ledger_id = "it/policy-stats-count:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();
            let setup = json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "schema": "http://schema.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:ssn": "111-11-1111"},
                    {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "schema:ssn": "222-22-2222"},
                    {"@id": "ex:aliceIdentity", "f:policyClass": [{"@id": "ex:EmployeePolicy"}], "ex:user": {"@id": "ex:alice"}},
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
                    }
                ]
            });
            let r1 = fluree
                .upsert_with_opts(ledger, &setup, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r1.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger_indexed.snapshot.range_provider.is_some(), "expected binary index");

            // SELECT ?p (COUNT ?s) WHERE { ?s ?p ?o } GROUP BY ?p
            let query = json!({
                "select": ["?p", "(as (count ?s) ?count)"],
                "where": {"@id": "?s", "?p": "?o"},
                "groupBy": ["?p"]
            });

            // Extract the count for the schema:ssn predicate from the (?p,?count) rows.
            let ssn_count = |jsonld: &serde_json::Value| -> Option<i64> {
                jsonld.as_array()?.iter().find_map(|row| {
                    let arr = row.as_array()?;
                    let pred = arr.first()?.as_str()?;
                    if pred.contains("ssn") {
                        arr.get(1)?.as_i64()
                    } else {
                        None
                    }
                })
            };

            // Control: no policy => raw stats count for ssn = 2 (alice + bob).
            let control = support::query_jsonld(&fluree, &ledger_indexed, &query)
                .await
                .expect("control query")
                .to_jsonld(&ledger_indexed.snapshot)
                .expect("jsonld");
            assert_eq!(
                ssn_count(&control),
                Some(2),
                "control: both ssn flakes counted; got {control:#?}"
            );

            // Policy: alice can view only her own ssn => count 1 (filtered fallback),
            // not the raw index count of 2.
            let alice_opts = GovernanceOptions {
                identity: Some("http://example.org/ns/aliceIdentity".to_string()),
                default_allow: true,
                ..Default::default()
            };
            let policy_ctx = policy_builder::build_policy_context_from_opts(
                &ledger_indexed.snapshot,
                ledger_indexed.novelty.as_ref(),
                None,
                ledger_indexed.t(),
                &alice_opts,
                &[0],
            )
            .await
            .expect("build policy");
            let filtered = support::query_jsonld_with_policy(&fluree, &ledger_indexed, &query, &policy_ctx)
                .await
                .expect("policy query")
                .to_jsonld(&ledger_indexed.snapshot)
                .expect("jsonld");
            assert_eq!(
                ssn_count(&filtered),
                Some(1),
                "policy: stats count-by-predicate must reflect the view policy (alice's own ssn only), \
                 not the raw index count; got {filtered:#?}"
            );
        })
        .await;
}
