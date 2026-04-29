//! Regression tests for https://github.com/fluree/db-r/issues/127
//!
//! Root cause: after binary indexing + ledger reload, the config graph (g_id=2)
//! queries returned late-materialized bindings (`EncodedSid`/`EncodedLit`) that
//! the config resolver couldn't extract, silently disabling all config-graph-
//! driven features including `f:enforceUnique` enforcement.
//!
//! These tests require the `native` feature for file-backed storage + indexing.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    query_jsonld_formatted, query_sparql, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

/// Build the config-graph IRI for a ledger.
fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Helper: write enforce-unique config via TriG.
fn unique_config_trig(ledger_id: &str) -> String {
    let config_iri = config_graph_iri(ledger_id);
    format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:transactDefaults <urn:config:transact> .
            <urn:config:transact> f:uniqueEnabled true .
        }}
    "
    )
}

/// Helper: set up file-backed Fluree with background indexer.
struct IndexedTestHarness {
    index_cfg: IndexConfig,
}

impl IndexedTestHarness {
    fn new() -> Self {
        Self {
            index_cfg: IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            },
        }
    }
}

/// Config graph data must survive binary indexing + ledger reload.
///
/// Before this fix, the BinaryScanOperator used late materialization (epoch=0)
/// which returned `EncodedSid` bindings that `find_instances_of_type` silently
/// dropped via `binding.as_sid()`. The fix uses `GraphDbRef::eager()` to force
/// resolved bindings for infrastructure queries.
#[tokio::test]
async fn config_graph_survives_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let h = IndexedTestHarness::new();

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/config-survives-index:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Insert data + f:enforceUnique annotation
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": {
                            "ex": "http://example.org/",
                            "f": "https://ns.flur.ee/db#"
                        },
                        "@graph": [
                            {"@id": "ex:email", "f:enforceUnique": true},
                            {"@id": "ex:alice", "ex:email": "alice@example.com"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await
                .unwrap();

            // Write config
            let r2 = fluree
                .stage_owned(r1.ledger)
                .upsert_turtle(&unique_config_trig(ledger_id))
                .index_config(h.index_cfg.clone())
                .execute()
                .await
                .unwrap();

            // Pre-index: unique constraint works
            let err_pre = fluree
                .insert_with_opts(
                    r2.ledger.clone(),
                    &json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:bob",
                        "ex:email": "alice@example.com"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await;
            assert!(
                err_pre.is_err(),
                "unique constraint should work BEFORE indexing"
            );

            // Index + reload
            trigger_index_and_wait_outcome(&handle, ledger_id, r2.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();
            assert!(ledger_indexed.snapshot.range_provider.is_some());

            // Post-index: unique constraint must STILL work
            let err_post = fluree
                .insert_with_opts(
                    ledger_indexed,
                    &json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:charlie",
                        "ex:email": "alice@example.com"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await;
            assert!(
                err_post.is_err(),
                "unique constraint should still work AFTER indexing + reload"
            );
        })
        .await;
}

/// Self-upsert on `f:enforceUnique` property after indexing must succeed.
#[tokio::test]
async fn upsert_enforce_unique_self_conflict_after_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let h = IndexedTestHarness::new();

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/unique-upsert-indexed:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Insert ontology + entity
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": {
                            "ex": "http://example.org/",
                            "f": "https://ns.flur.ee/db#"
                        },
                        "@graph": [
                            {"@id": "ex:userId", "f:enforceUnique": true},
                            {
                                "@id": "ex:user1",
                                "@type": "ex:User",
                                "ex:userId": "u-001",
                                "ex:displayName": "alice"
                            }
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await
                .unwrap();

            // Enable config
            let r2 = fluree
                .stage_owned(r1.ledger)
                .upsert_turtle(&unique_config_trig(ledger_id))
                .index_config(h.index_cfg.clone())
                .execute()
                .await
                .unwrap();

            // Index + reload
            trigger_index_and_wait_outcome(&handle, ledger_id, r2.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();

            // Self-upsert: same entity, same unique value
            let result = fluree
                .upsert_with_opts(
                    ledger_indexed,
                    &json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:user1",
                        "@type": "ex:User",
                        "ex:userId": "u-001",
                        "ex:displayName": "alice-updated"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await;

            assert!(
                result.is_ok(),
                "self-upsert with same unique value should succeed after indexing: {:?}",
                result.err()
            );

            // Verify entity appears exactly once
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "where": {"@id": "?u", "@type": "ex:User", "ex:userId": "u-001"},
                "select": {"?u": ["*"]}
            });
            let rows = query_jsonld_formatted(&fluree, &result.unwrap().ledger, &query)
                .await
                .unwrap();
            let arr = rows.as_array().expect("array");
            assert_eq!(arr.len(), 1, "entity should appear exactly once: {arr:#?}");
        })
        .await;
}

/// URN-style IRIs (Solo's `urn:fsys:user:<uuid>` pattern) + indexing.
#[tokio::test]
async fn upsert_enforce_unique_urn_iris_after_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let h = IndexedTestHarness::new();

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/unique-urn-indexed:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            let ctx = json!({
                "fsys": "https://ns.flur.ee/system#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "schema": "http://schema.org/",
                "f": "https://ns.flur.ee/db#"
            });

            // Insert ontology + entity (Solo-style)
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": ctx,
                        "@graph": [
                            {
                                "@id": "fsys:userId",
                                "@type": "rdf:Property",
                                "rdfs:range": {"@id": "xsd:string"},
                                "f:enforceUnique": true
                            },
                            {
                                "@id": "urn:fsys:user:e4b8a468-80b1-709f-c2eb-9627f8e7c24a",
                                "@type": "fsys:User",
                                "fsys:userId": "e4b8a468-80b1-709f-c2eb-9627f8e7c24a",
                                "fsys:displayName": "ajohnson"
                            }
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await
                .unwrap();

            // Enable config + index
            let r2 = fluree
                .stage_owned(r1.ledger)
                .upsert_turtle(&unique_config_trig(ledger_id))
                .index_config(h.index_cfg.clone())
                .execute()
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r2.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();

            // Self-upsert (exact bug trigger from issue #127)
            let result = fluree
                .upsert_with_opts(
                    ledger_indexed,
                    &json!({
                        "@context": ctx,
                        "@id": "urn:fsys:user:e4b8a468-80b1-709f-c2eb-9627f8e7c24a",
                        "@type": "fsys:User",
                        "fsys:userId": "e4b8a468-80b1-709f-c2eb-9627f8e7c24a",
                        "fsys:displayName": "ajohnson"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await;

            assert!(
                result.is_ok(),
                "self-upsert with same unique value should succeed (issue #127): {:?}",
                result.err()
            );

            // Verify no duplication
            let sparql = r#"
                PREFIX fsys: <https://ns.flur.ee/system#>
                SELECT (COUNT(*) AS ?c)
                WHERE {
                    ?u a fsys:User ;
                       fsys:userId "e4b8a468-80b1-709f-c2eb-9627f8e7c24a" .
                }
            "#;
            let count = query_sparql(&fluree, &result.unwrap().ledger, sparql)
                .await
                .unwrap()
                .to_jsonld(&fluree.ledger(ledger_id).await.unwrap().snapshot)
                .unwrap();
            assert_eq!(count, json!([[1]]), "entity should appear exactly once");
        })
        .await;
}

/// Failed unique violation must not corrupt state for subsequent txns.
#[tokio::test]
async fn failed_unique_violation_no_corruption_after_indexing() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let h = IndexedTestHarness::new();

    let mut fluree = FlureeBuilder::file(path).build().expect("build");

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/unique-no-corrupt-indexed:main";
            let ledger = fluree.create_ledger(ledger_id).await.unwrap();

            // Seed data
            let r1 = fluree
                .upsert_with_opts(
                    ledger,
                    &json!({
                        "@context": {
                            "ex": "http://example.org/",
                            "f": "https://ns.flur.ee/db#"
                        },
                        "@graph": [
                            {"@id": "ex:email", "f:enforceUnique": true},
                            {"@id": "ex:alice", "ex:email": "alice@example.com", "ex:name": "Alice"},
                            {"@id": "ex:bob", "ex:email": "bob@example.com", "ex:name": "Bob"}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await
                .unwrap();

            // Enable config + index
            let r2 = fluree
                .stage_owned(r1.ledger)
                .upsert_turtle(&unique_config_trig(ledger_id))
                .index_config(h.index_cfg.clone())
                .execute()
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, ledger_id, r2.receipt.t).await;
            let ledger_indexed = fluree.ledger(ledger_id).await.unwrap();

            // Trigger genuine unique violation
            let err = fluree
                .insert_with_opts(
                    ledger_indexed.clone(),
                    &json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:mallory",
                        "ex:email": "alice@example.com"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await;
            assert!(err.is_err(), "duplicate unique value should be rejected");

            // Subsequent insert must succeed and not corrupt state
            let r3 = fluree
                .upsert_with_opts(
                    ledger_indexed,
                    &json!({
                        "@context": {"ex": "http://example.org/"},
                        "@id": "ex:charlie",
                        "ex:email": "charlie@example.com",
                        "ex:name": "Charlie"
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &h.index_cfg,
                )
                .await
                .expect("subsequent insert should succeed");

            // Verify no duplication
            let sparql = r"
                PREFIX ex: <http://example.org/>
                SELECT ?u ?email
                WHERE { ?u ex:email ?email }
                ORDER BY ?email
            ";
            let result = query_sparql(&fluree, &r3.ledger, sparql)
                .await
                .unwrap()
                .to_jsonld(&r3.ledger.snapshot)
                .unwrap();
            let rows = result.as_array().expect("array");
            assert_eq!(rows.len(), 3, "expected 3 entities: {result:#?}");
        })
        .await;
}
