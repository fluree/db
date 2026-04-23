//! Cross-graph isolation regression tests.
//!
//! These tests verify that class attribution, SHACL validation, and policy
//! enforcement respect named graph boundaries. Specifically, `rdf:type`
//! assertions in graph A must NOT be visible when querying graph B.
//!
//! ## Background
//!
//! Before the per-graph fixes, all transact-time lookups (SHACL validation,
//! policy class cache, list-index meta hydration) hardcoded `g_id = 0`,
//! meaning a subject's `rdf:type` in *any* graph was visible everywhere.
//! This caused incorrect SHACL targeting and wrong class-based policy
//! decisions for subjects split across multiple named graphs.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{
    ExportCommitsRequest, FlureeBuilder, IndexConfig, LedgerManagerConfig, PushCommitsRequest,
    QueryConnectionOptions,
};
use serde_json::json;
use support::{genesis_ledger, start_background_indexer_local, trigger_index_and_wait};

/// Regression: rdf:type in graph A is invisible when querying graph B.
///
/// Scenario:
///   1. Insert `ex:alice rdf:type ex:Person` in named graph `ex:types`
///   2. Insert `ex:alice schema:name "Alice"` in named graph `ex:data`
///   3. Index both
///   4. Query `ex:alice`'s rdf:type from graph `ex:data` → should be empty
///   5. Query `ex:alice`'s rdf:type from graph `ex:types` → should find ex:Person
///
/// The old code (g_id=0 everywhere) would have returned ex:Person from
/// any graph because the binary index lookup always hit the default graph.
/// With per-graph range queries, each graph is isolated.
#[tokio::test]
async fn rdf_type_isolated_across_named_graphs() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/graph-isolation-type:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Insert rdf:type in graph A, data property in graph B.
            // Both share the same subject (ex:alice).
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

                GRAPH <http://example.org/graphs/types> {
                    ex:alice rdf:type ex:Person .
                }

                GRAPH <http://example.org/graphs/data> {
                    ex:alice schema:name "Alice" .
                    ex:alice schema:email "alice@example.org" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("initial import should succeed");

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query rdf:type from graph "types" → should find ex:Person
            let types_alias = format!("{ledger_id}#http://example.org/graphs/types");
            let query = json!({
                "@context": {"ex": "http://example.org/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
                "from": &types_alias,
                "select": ["?type"],
                "where": {"@id": "ex:alice", "rdf:type": "?type"}
            });

            let results = fluree.query_connection(&query).await.expect("query types graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                !arr.is_empty(),
                "should find ex:Person in the types graph"
            );

            // Query rdf:type from graph "data" → should be EMPTY
            let data_alias = format!("{ledger_id}#http://example.org/graphs/data");
            let query = json!({
                "@context": {"ex": "http://example.org/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
                "from": &data_alias,
                "select": ["?type"],
                "where": {"@id": "ex:alice", "rdf:type": "?type"}
            });

            let results = fluree.query_connection(&query).await.expect("query data graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "rdf:type should NOT bleed from types graph to data graph, got: {arr:?}"
            );

            // Also verify default graph has no types for alice
            let query = json!({
                "@context": {"ex": "http://example.org/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
                "from": ledger_id,
                "select": ["?type"],
                "where": {"@id": "ex:alice", "rdf:type": "?type"}
            });

            let results = fluree.query_connection(&query).await.expect("query default graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "default graph should have no rdf:type for alice, got: {arr:?}"
            );
        })
        .await;
}

/// Regression: class cache population uses the correct graph.
///
/// This test verifies that when a subject has rdf:type in one graph
/// but data in another, querying the subject's properties from the
/// data graph does NOT include class-based data from the type graph.
///
/// Scenario:
///   1. Insert `ex:alice rdf:type ex:Person` + `schema:name "Alice"` in graph A
///   2. Insert `ex:alice ex:score 42` in graph B (no rdf:type in B)
///   3. Index
///   4. Query graph B for all of alice's properties → only ex:score
///   5. Query graph A for all of alice's properties → rdf:type + schema:name
#[tokio::test]
async fn subject_properties_isolated_per_graph() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/graph-isolation-props:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

                GRAPH <http://example.org/graphs/core> {
                    ex:alice rdf:type ex:Person .
                    ex:alice schema:name "Alice" .
                }

                GRAPH <http://example.org/graphs/scores> {
                    ex:alice ex:score "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query all properties of alice from the scores graph
            let scores_alias = format!("{ledger_id}#http://example.org/graphs/scores");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &scores_alias,
                "select": ["?p", "?o"],
                "where": {"@id": "ex:alice", "?p": "?o"}
            });

            let results = fluree.query_connection(&query).await.expect("query scores");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Should only see ex:score, NOT rdf:type or schema:name from core graph
            let predicates: Vec<&str> = arr
                .iter()
                .filter_map(|row| row.as_array().and_then(|a| a[0].as_str()))
                .collect();
            assert!(
                !predicates.iter().any(|p| p.contains("type")),
                "rdf:type from core graph should not appear in scores graph, predicates: {predicates:?}"
            );
            assert!(
                !predicates.iter().any(|p| p.contains("name")),
                "schema:name from core graph should not appear in scores graph, predicates: {predicates:?}"
            );

            // Query all properties of alice from the core graph
            let core_alias = format!("{ledger_id}#http://example.org/graphs/core");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &core_alias,
                "select": ["?p", "?o"],
                "where": {"@id": "ex:alice", "?p": "?o"}
            });

            let results = fluree.query_connection(&query).await.expect("query core");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Should see rdf:type and schema:name, NOT ex:score
            let predicates: Vec<&str> = arr
                .iter()
                .filter_map(|row| row.as_array().and_then(|a| a[0].as_str()))
                .collect();
            assert!(
                predicates.iter().any(|p| p.contains("type")),
                "core graph should contain rdf:type, predicates: {predicates:?}"
            );
            assert!(
                predicates.iter().any(|p| p.contains("name")),
                "core graph should contain schema:name, predicates: {predicates:?}"
            );
            assert!(
                !predicates.iter().any(|p| p.contains("score")),
                "core graph should not contain ex:score from scores graph, predicates: {predicates:?}"
            );
        })
        .await;
}

/// Regression: `@type` query filters respect graph boundaries.
///
/// Scenario:
///   1. Insert `ex:alice rdf:type ex:Employee` + `schema:name "Alice"` in graph A (HR)
///   2. Insert `ex:alice schema:salary "100000"` in graph B (payroll, NO rdf:type)
///   3. Index
///   4. Query graph B: `?s a ex:Employee` → empty (alice is not Employee in payroll)
///   5. Query graph A: `?s a ex:Employee` → finds alice
///
/// The old code with g_id=0 would find alice as an Employee in any graph
/// because the overlay/index lookup used the default graph regardless.
#[tokio::test]
async fn type_filter_query_respects_graph_boundaries() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/graph-isolation-typefilter:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

                GRAPH <http://example.org/graphs/hr> {
                    ex:alice rdf:type ex:Employee .
                    ex:alice schema:name "Alice" .
                }

                GRAPH <http://example.org/graphs/payroll> {
                    ex:alice schema:salary "100000" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query payroll graph for employees → should be empty
            let payroll_alias = format!("{ledger_id}#http://example.org/graphs/payroll");
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "from": &payroll_alias,
                "select": ["?s"],
                "where": {"@id": "?s", "rdf:type": {"@id": "ex:Employee"}}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query payroll");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "payroll graph should have no Employees, got: {arr:?}"
            );

            // Query HR graph for employees → should find alice
            let hr_alias = format!("{ledger_id}#http://example.org/graphs/hr");
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "from": &hr_alias,
                "select": ["?s"],
                "where": {"@id": "?s", "rdf:type": {"@id": "ex:Employee"}}
            });

            let results = fluree.query_connection(&query).await.expect("query hr");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                !arr.is_empty(),
                "HR graph should find alice as Employee, got: {arr:?}"
            );
        })
        .await;
}

/// Pre-index transact-time isolation: upsert in one named graph must not
/// see or retract data from another named graph at genesis (no binary index).
///
/// This test ensures that the novelty-scanning fallback in
/// `generate_upsert_deletions` correctly filters by graph Sid, and that
/// transact-time operations (SHACL, staging) don't bleed across graphs
/// when no binary index exists.
///
/// Scenario:
///   1. Insert `ex:alice schema:score "10"` in graph A and `ex:alice schema:score "20"` in graph B
///   2. Upsert `ex:alice schema:score "99"` in graph A only (NO indexing in between)
///   3. Verify: graph A should have score "99" (retracted "10"), graph B should still have "20"
///   4. Index and query to confirm isolation persisted through indexing
#[tokio::test]
async fn pre_index_upsert_isolates_named_graphs() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/graph-isolation-preindex:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Commit 1: Same subject, same predicate, different values in different graphs.
            let trig1 = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

                GRAPH <http://example.org/graphs/alpha> {
                    ex:alice rdf:type ex:Person .
                    ex:alice schema:score "10" .
                }

                GRAPH <http://example.org/graphs/beta> {
                    ex:alice rdf:type ex:Robot .
                    ex:alice schema:score "20" .
                }
            "#;

            let result1 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig1)
                .execute()
                .await
                .expect("commit 1: initial data in two named graphs");
            assert_eq!(result1.receipt.t, 1);

            // Commit 2: Upsert score in graph alpha ONLY — should retract "10", assert "99".
            // Graph beta's score ("20") must NOT be retracted.
            // This runs at genesis (no binary index): the upsert resolution path
            // must use novelty scanning with graph-scoped filtering.
            let trig2 = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/alpha> {
                    ex:alice schema:score "99" .
                }
            "#;

            let result2 = fluree
                .stage_owned(result1.ledger)
                .upsert_turtle(trig2)
                .execute()
                .await
                .expect("commit 2: upsert in alpha graph at genesis");
            assert_eq!(result2.receipt.t, 2);

            // Now index and verify via queries that the upsert was graph-scoped.
            trigger_index_and_wait(&handle, ledger_id, 2).await;
            let _ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query alpha graph for score → should be "99"
            let alpha_alias = format!("{ledger_id}#http://example.org/graphs/alpha");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &alpha_alias,
                "select": "?score",
                "where": {"@id": "ex:alice", "schema:score": "?score"}
            });

            let results = fluree.query_connection(&query).await.expect("query alpha");
            let results = results.to_jsonld(&_ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            let scores: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            assert!(
                scores.contains(&"99"),
                "alpha graph should have score 99, got: {arr:?}"
            );
            assert!(
                !scores.contains(&"10"),
                "alpha graph should NOT have old score 10, got: {arr:?}"
            );

            // Query beta graph for score → should still be "20"
            let beta_alias = format!("{ledger_id}#http://example.org/graphs/beta");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &beta_alias,
                "select": "?score",
                "where": {"@id": "ex:alice", "schema:score": "?score"}
            });

            let results = fluree.query_connection(&query).await.expect("query beta");
            let results = results.to_jsonld(&_ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            let scores: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            assert!(
                scores.contains(&"20"),
                "beta graph should still have score 20, got: {arr:?}"
            );
            assert!(
                !scores.contains(&"99"),
                "beta graph should NOT have alpha's score 99, got: {arr:?}"
            );

            // Also verify rdf:type isolation persists through indexing.
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "from": &alpha_alias,
                "select": "?type",
                "where": {"@id": "ex:alice", "rdf:type": "?type"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query alpha types");
            let results = results.to_jsonld(&_ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            let has_person = arr
                .iter()
                .any(|v| v.as_str().map(|s| s.contains("Person")).unwrap_or(false));
            let has_robot = arr
                .iter()
                .any(|v| v.as_str().map(|s| s.contains("Robot")).unwrap_or(false));
            assert!(has_person, "alpha should have Person type, got: {arr:?}");
            assert!(
                !has_robot,
                "alpha should NOT have Robot type (from beta), got: {arr:?}"
            );
        })
        .await;
}

/// Push roundtrip: export commits with named-graph retractions, push to a fresh ledger.
///
/// This exercises the full push pipeline with named-graph flakes:
/// - `derive_graph_routing` builds the GraphId → Sid mapping
/// - `is_currently_asserted` queries the correct binary index partition
/// - `stage_commit_flakes` passes graph_sids to `stage_flakes`
/// - SHACL validation receives per-graph routing
///
/// Scenario:
///   1. Source: Insert data in two named graphs (HR + payroll)
///   2. Source: Upsert salary in payroll graph (retraction of old value + assertion of new)
///   3. Export both commits from source
///   4. Create fresh target ledger, push both commits
///   5. Index the target, then query to verify:
///      - HR graph has alice as Employee with name "Alice"
///      - Payroll graph has the updated salary (75000, not 50000)
#[tokio::test]
async fn push_roundtrip_named_graph_retractions() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            // --- Source ledger ---

            let src_id = "it/push-ng-src:main";
            let src_ledger = fluree.create_ledger(src_id).await.expect("create source");

            // Commit 1: Insert data in two named graphs.
            let trig1 = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

                GRAPH <http://example.org/graphs/hr> {
                    ex:alice rdf:type ex:Employee .
                    ex:alice schema:name "Alice" .
                }

                GRAPH <http://example.org/graphs/payroll> {
                    ex:alice schema:salary "50000" .
                }
            "#;

            let result1 = fluree
                .stage_owned(src_ledger)
                .upsert_turtle(trig1)
                .execute()
                .await
                .expect("source commit 1");
            assert_eq!(result1.receipt.t, 1, "first commit should be t=1");

            // Commit 2: Upsert salary in payroll graph → retracts "50000", asserts "75000".
            let trig2 = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/payroll> {
                    ex:alice schema:salary "75000" .
                }
            "#;

            let result2 = fluree
                .stage_owned(result1.ledger)
                .upsert_turtle(trig2)
                .execute()
                .await
                .expect("source commit 2 (retraction + assertion in named graph)");
            assert_eq!(result2.receipt.t, 2, "second commit should be t=2");

            // --- Export commits from source ---

            let src_handle = fluree.ledger_cached(src_id).await.expect("source handle");
            let export = fluree
                .export_commit_range(
                    &src_handle,
                    &ExportCommitsRequest {
                        cursor: None,
                        cursor_id: None,
                        limit: Some(100),
                    },
                )
                .await
                .expect("export commits");

            assert_eq!(export.count, 2, "should export 2 commits");
            assert_eq!(export.newest_t, 2);
            assert_eq!(export.oldest_t, 1);

            // --- Target ledger: create fresh, push exported commits ---

            let tgt_id = "it/push-ng-tgt:main";
            let _tgt_ledger = fluree.create_ledger(tgt_id).await.expect("create target");
            let tgt_handle = fluree.ledger_cached(tgt_id).await.expect("target handle");

            // Export returns newest → oldest; push needs oldest → newest.
            let mut push_commits = export.commits;
            push_commits.reverse();

            let push_req = PushCommitsRequest {
                commits: push_commits,
                blobs: export.blobs,
            };

            let push_result = fluree
                .push_commits_with_handle(
                    &tgt_handle,
                    push_req,
                    &QueryConnectionOptions::default(),
                    &IndexConfig::default(),
                )
                .await
                .expect("push should succeed with named-graph retractions");

            assert_eq!(push_result.accepted, 2, "both commits accepted");
            assert_eq!(push_result.head.t, 2, "target head should be at t=2");

            // --- Index the target ledger so named-graph queries work ---

            trigger_index_and_wait(&handle, tgt_id, 2).await;

            // Evict the cached handle so the next query loads fresh (with binary index).
            fluree.disconnect_ledger(tgt_id).await;
            let tgt_state = fluree.ledger(tgt_id).await.expect("load target");

            // --- Query target to verify data ---

            // Query HR graph for Employee type — should find alice.
            let hr_alias = format!("{tgt_id}#http://example.org/graphs/hr");
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "schema": "http://schema.org/"
                },
                "from": &hr_alias,
                "select": "?name",
                "where": {
                    "@id": "?s",
                    "rdf:type": {"@id": "ex:Employee"},
                    "schema:name": "?name"
                }
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query target HR graph");
            let results = results.to_jsonld(&tgt_state.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                !arr.is_empty(),
                "target HR graph should find alice as Employee"
            );
            // Should contain "Alice"
            let names: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            assert!(
                names.contains(&"Alice"),
                "should find Alice in HR graph, got: {arr:?}"
            );

            // Query payroll graph for salary — should see "75000" (not "50000").
            let payroll_alias = format!("{tgt_id}#http://example.org/graphs/payroll");
            let query = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "from": &payroll_alias,
                "select": "?salary",
                "where": {
                    "@id": "ex:alice",
                    "schema:salary": "?salary"
                }
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query target payroll graph");
            let results = results.to_jsonld(&tgt_state.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                !arr.is_empty(),
                "target payroll graph should have salary data"
            );
            // Should be "75000" (the retraction removed "50000", assertion added "75000").
            let salaries: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            assert!(
                salaries.contains(&"75000"),
                "salary should be 75000 after retraction, got: {arr:?}"
            );
            assert!(
                !salaries.contains(&"50000"),
                "old salary 50000 should be retracted, got: {arr:?}"
            );
        })
        .await;
}
