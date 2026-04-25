//! Named graph integration tests
//!
//! Tests the full pipeline from TriG/JSON-LD with named graphs → commit → indexing → query.
//!
//! These tests verify that:
//! - Named graphs are parsed correctly from TriG GRAPH blocks
//! - Graph IRIs are encoded in the commit's graph_delta field
//! - Indexed data is queryable via the #<graph-iri> fragment
//!
//! Named graphs use g_id 2+ (0 = default, 1 = txn-meta).

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, LedgerManagerConfig};
use serde_json::json;
use support::{genesis_ledger, start_background_indexer_local, trigger_index_and_wait};

// =============================================================================
// TriG named graph parsing tests
// =============================================================================

#[tokio::test]
async fn test_trig_named_graph_basic() {
    // Insert TriG with a GRAPH block containing named graph data.
    // Verify that the data is stored in the named graph.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-basic:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // TriG with a named graph block - use upsert_turtle which processes GRAPH blocks
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                # Default graph data
                ex:alice schema:name "Alice" .

                # Named graph data
                GRAPH <http://example.org/graphs/audit> {
                    ex:event1 schema:description "User login" .
                    ex:event1 ex:timestamp "2025-01-01T00:00:00Z" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");
            assert_eq!(result.receipt.t, 1);

            // Trigger indexing and wait
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Query the default graph - should see Alice
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": ledger_id,
                "select": "?name",
                "where": {"@id": "ex:alice", "schema:name": "?name"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query default");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find Alice in default graph");
            assert_eq!(arr[0], "Alice");

            // Query the named graph via fragment - should see the event
            let named_graph_alias = format!("{ledger_id}#http://example.org/graphs/audit");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &named_graph_alias,
                "select": "?desc",
                "where": {"@id": "ex:event1", "schema:description": "?desc"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query named graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find event in named graph");
            assert_eq!(arr[0], "User login");
        })
        .await;
}

#[tokio::test]
async fn test_trig_named_graph_typed_literal_without_prefix_errors() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger = genesis_ledger(&fluree, "it/named-graph-missing-dt-prefix:main");

    let trig = r#"
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/graphs/audit> {
            ex:event1 ex:label "User login"^^xsd:string .
        }
    "#;

    let err = fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect_err("TriG typed literal without xsd prefix should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("Undefined prefix: xsd") || msg.contains("undefined prefix: xsd"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn test_trig_multiple_named_graphs() {
    // Insert TriG with multiple GRAPH blocks.
    // Verify each graph is isolated and queryable.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-multi:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // TriG with multiple named graphs
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/users> {
                    ex:alice schema:name "Alice" .
                    ex:bob schema:name "Bob" .
                }

                GRAPH <http://example.org/graphs/products> {
                    ex:prod1 schema:name "Widget" .
                    ex:prod1 ex:price 99 .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query users graph
            let users_alias = format!("{ledger_id}#http://example.org/graphs/users");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &users_alias,
                "select": ["?s", "?name"],
                "where": {"@id": "?s", "schema:name": "?name"}
            });

            let results = fluree.query_connection(&query).await.expect("query users");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            // Should have 2 names (Alice, Bob)
            assert_eq!(arr.len(), 2, "should find 2 users: {arr:?}");

            // Query products graph
            let products_alias = format!("{ledger_id}#http://example.org/graphs/products");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &products_alias,
                "select": "?name",
                "where": {"@id": "ex:prod1", "schema:name": "?name"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query products");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find product");
            assert_eq!(arr[0], "Widget");
        })
        .await;
}

#[tokio::test]
async fn test_unknown_named_graph_error() {
    // Attempting to query a non-existent named graph should error.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-unknown:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Just insert some data
            let tx = json!({
                "@context": {"ex": "http://example.org/"},
                "insert": [{"@id": "ex:alice", "ex:name": "Alice"}]
            });
            let result = fluree.update(ledger, &tx).await.expect("update");

            // Trigger indexing
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Query a non-existent named graph - should error
            let unknown_alias = format!("{ledger_id}#http://example.org/nonexistent");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &unknown_alias,
                "select": ["?s"],
                "where": {"@id": "?s", "ex:name": "?name"}
            });

            let result = fluree.query_connection(&query).await;
            assert!(result.is_err(), "should error on unknown named graph");
            let err_msg = format!("{}", result.unwrap_err());
            assert!(
                err_msg.contains("Unknown named graph"),
                "error should mention unknown graph: {err_msg}"
            );
        })
        .await;
}

#[tokio::test]
async fn test_update_default_graph_and_template_graph_sugar() {
    // JSON-LD update graph scoping:
    // - top-level "graph" scopes default-graph WHERE patterns and template triples
    // - insert/delete allow ["graph", <graph-iri>, <pattern>] template sugar
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/update-graph-scope:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Seed "old" into a named graph using template sugar.
            let seed = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "insert": [[
                    "graph",
                    "http://example.org/graphs/audit",
                    { "@id": "ex:event1", "schema:description": "old" }
                ]]
            });

            let result = fluree.update(ledger, &seed).await.expect("seed update");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Now UPDATE with a transaction-level default graph.
            // The WHERE has no explicit graph wrapper, so it should be scoped to the named graph.
            let update = json!({
                "@context": {
                    "ex": "http://example.org/",
                    "schema": "http://schema.org/"
                },
                "graph": "http://example.org/graphs/audit",
                "where": { "@id": "ex:event1", "schema:description": "?old" },
                "delete": { "@id": "ex:event1", "schema:description": "?old" },
                "insert": { "@id": "ex:event1", "schema:description": "new" }
            });

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let result = fluree.update(ledger, &update).await.expect("scoped update");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Query the named graph - should see "new"
            let named_graph_alias = format!("{ledger_id}#http://example.org/graphs/audit");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &named_graph_alias,
                "select": "?desc",
                "where": { "@id": "ex:event1", "schema:description": "?desc" }
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query named graph");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert_eq!(arr.len(), 1, "expected single description: {arr:?}");
            assert_eq!(arr[0], "new");

            // Query default graph - should not see the event (it lives in the named graph)
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": ledger_id,
                "select": "?desc",
                "where": { "@id": "ex:event1", "schema:description": "?desc" }
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query default graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(arr.is_empty(), "expected no default-graph results: {arr:?}");
        })
        .await;
}

#[tokio::test]
async fn test_update_from_scopes_where_default_graph() {
    // `from.graph` scopes WHERE evaluation to a named graph (USING equivalent),
    // while `graph` (top-level) controls the default target graph for templates (WITH equivalent).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/update-from-scopes-where:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Seed a value in g1, and ensure g2 is initially empty for the copied predicate.
            let seed = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "insert": [
                    ["graph", "http://example.org/g1", { "@id": "ex:s", "schema:description": "g1-old" }]
                ]
            });
            let result = fluree.update(ledger, &seed).await.expect("seed");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Read from g1 (WHERE scoped by from.graph) and write to g2 (templates defaulted by graph).
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let update = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "graph": "http://example.org/g2",
                "from": { "graph": "http://example.org/g1" },
                "where": { "@id": "ex:s", "schema:description": "?d" },
                "insert": [
                    { "@id": "ex:s", "schema:copyFromG1": "?d" }
                ]
            });
            let result = fluree.update(ledger, &update).await.expect("update");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let named_g2 = format!("{ledger_id}#http://example.org/g2");
            let query = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "from": &named_g2,
                "select": "?d",
                "where": { "@id": "ex:s", "schema:copyFromG1": "?d" }
            });
            let results = fluree.query_connection(&query).await.expect("query g2 copy");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert_eq!(arr, &vec![json!("g1-old")]);
        })
        .await;
}

#[tokio::test]
async fn test_update_from_multiple_default_graphs_merge_where() {
    // JSON-LD update `from` can specify multiple default graphs. Default-graph WHERE patterns
    // see a merged graph (USING multiple graphs equivalent).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/update-from-multiple-default-graphs:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Seed ex:a in g1 with ex:p "1" and in g2 with ex:q "2".
            let seed = json!({
                "@context": { "ex": "http://example.org/" },
                "insert": [
                    ["graph", "http://example.org/g1", { "@id": "ex:a", "ex:p": "1" }],
                    ["graph", "http://example.org/g2", { "@id": "ex:a", "ex:q": "2" }]
                ]
            });
            let result = fluree.update(ledger, &seed).await.expect("seed");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // WHERE needs to see both triples, but they live in different graphs; `from: [g1,g2]`
            // makes them visible as one merged default graph.
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let update = json!({
                "@context": { "ex": "http://example.org/" },
                "graph": "http://example.org/g1",
                "from": ["http://example.org/g1", "http://example.org/g2"],
                "where": [
                    { "@id": "ex:a", "ex:p": "1" },
                    { "@id": "ex:a", "ex:q": "2" }
                ],
                "insert": [
                    { "@id": "ex:a", "ex:marker": "ok" }
                ]
            });
            let result = fluree.update(ledger, &update).await.expect("update");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let named_g1 = format!("{ledger_id}#http://example.org/g1");
            let query = json!({
                "@context": { "ex": "http://example.org/" },
                "from": &named_g1,
                "select": "?m",
                "where": { "@id": "ex:a", "ex:marker": "?m" }
            });
            let results = fluree
                .query_connection(&query)
                .await
                .expect("query g1 marker");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert_eq!(arr, &vec![json!("ok")]);
        })
        .await;
}

#[tokio::test]
async fn test_update_from_named_alias_usable_in_templates() {
    // Ensure `fromNamed.alias` can be used consistently in UPDATE templates
    // (not just in WHERE graph patterns).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/update-from-named-alias-templates:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Insert into g2 using the fromNamed alias as the template graph selector.
            let insert = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "fromNamed": [
                    { "alias": "g2", "graph": "http://example.org/g2" }
                ],
                "values": ["?x", [1]],
                "insert": [
                    ["graph", "g2", { "@id": "ex:s", "schema:description": "via-alias" }]
                ]
            });
            let result = fluree
                .update(ledger, &insert)
                .await
                .expect("insert via alias");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let named_g2 = format!("{ledger_id}#http://example.org/g2");
            let query = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "from": &named_g2,
                "select": "?d",
                "where": { "@id": "ex:s", "schema:description": "?d" }
            });
            let results = fluree.query_connection(&query).await.expect("query g2");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            assert!(
                ledger
                    .snapshot
                    .graph_registry
                    .graph_id_for_iri("http://example.org/g2")
                    .is_some(),
                "expected g2 IRI to be registered in graph_registry"
            );
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert_eq!(arr, &vec![json!("via-alias")]);
        })
        .await;
}

#[tokio::test]
async fn test_default_graph_isolation() {
    // Data in named graphs should not appear in default graph queries.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-isolation:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // TriG with data only in a named graph
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/private> {
                    ex:secret schema:value "confidential" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query default graph - should NOT find the secret
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": ledger_id,
                "select": "?val",
                "where": {"@id": "ex:secret", "schema:value": "?val"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query default");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "default graph should not contain named graph data: {arr:?}"
            );

            // Query named graph - should find the secret
            let private_alias = format!("{ledger_id}#http://example.org/graphs/private");
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": &private_alias,
                "select": "?val",
                "where": {"@id": "ex:secret", "schema:value": "?val"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query private");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find secret in named graph");
            assert_eq!(arr[0], "confidential");
        })
        .await;
}

#[tokio::test]
async fn test_txn_meta_and_named_graph_coexist() {
    // TriG can have both txn-meta GRAPH and user named graphs.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-coexist:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // TriG with txn-meta and a user named graph
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                # Default graph
                ex:alice schema:name "Alice" .

                # txn-meta graph
                GRAPH <#txn-meta> {
                    <fluree:commit:this> ex:batchId "batch-123" .
                }

                # User named graph
                GRAPH <http://example.org/graphs/audit> {
                    ex:log1 ex:action "user created" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("import");

            // Trigger indexing
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query default graph
            let query = json!({
                "@context": {"ex": "http://example.org/", "schema": "http://schema.org/"},
                "from": ledger_id,
                "select": "?name",
                "where": {"@id": "ex:alice", "schema:name": "?name"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query default");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find Alice in default graph");
            assert_eq!(arr[0], "Alice");

            // Query txn-meta graph
            let meta_alias = format!("{ledger_id}#txn-meta");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &meta_alias,
                "select": "?batch",
                "where": {"@id": "?commit", "ex:batchId": "?batch"}
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query txn-meta");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find batch in txn-meta");
            assert_eq!(arr[0], "batch-123");

            // Query audit graph
            let audit_alias = format!("{ledger_id}#http://example.org/graphs/audit");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &audit_alias,
                "select": "?action",
                "where": {"@id": "ex:log1", "ex:action": "?action"}
            });

            let results = fluree.query_connection(&query).await.expect("query audit");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(!arr.is_empty(), "should find action in audit graph");
            assert_eq!(arr[0], "user created");
        })
        .await;
}

// =============================================================================
// Named graph update + time travel tests
// =============================================================================
//
// These tests cover multi-transaction correctness, time travel, and JSON-LD
// `@graph`-scoped deletes for named graphs.

#[tokio::test]
async fn test_named_graph_update_and_query_current() {
    // Test multiple updates to a named graph and querying current state.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-update:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Transaction 1: Initial data in named graph
            let trig1 = r"
                @prefix ex: <http://example.org/> .
                @prefix schema: <http://schema.org/> .

                GRAPH <http://example.org/graphs/inventory> {
                    ex:widget ex:stock 100 .
                    ex:gadget ex:stock 50 .
                }
            ";

            let result1 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig1)
                .execute()
                .await
                .expect("tx1");
            assert_eq!(result1.receipt.t, 1);

            // Index transaction 1
            trigger_index_and_wait(&handle, ledger_id, result1.receipt.t).await;

            // Transaction 2: Update stock levels using graph().transact() API
            let trig2 = r"
                @prefix ex: <http://example.org/> .

                GRAPH <http://example.org/graphs/inventory> {
                    ex:widget ex:stock 75 .
                    ex:gadget ex:stock 60 .
                    ex:gizmo ex:stock 25 .
                }
            ";

            let result2 = fluree
                .graph(ledger_id)
                .transact()
                .upsert_turtle(trig2)
                .commit()
                .await
                .expect("tx2");
            assert_eq!(result2.receipt.t, 2);

            // Index transaction 2
            trigger_index_and_wait(&handle, ledger_id, result2.receipt.t).await;

            // Query current state (t=2) - should see updated values
            let inv_alias = format!("{ledger_id}#http://example.org/graphs/inventory");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &inv_alias,
                "select": ["?item", "?stock"],
                "where": {"@id": "?item", "ex:stock": "?stock"},
                "orderBy": "?item"
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query current");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            // Should have 3 items with updated stock
            assert_eq!(arr.len(), 3, "should have 3 items: {arr:?}");

            // Check widget has updated stock (75, not 100)
            let widget_row = arr.iter().find(|r| {
                r.as_array()
                    .map(|a| {
                        let s = a.first().and_then(|v| v.as_str()).unwrap_or("");
                        s == "http://example.org/widget" || s == "ex:widget"
                    })
                    .unwrap_or(false)
            });
            assert!(widget_row.is_some(), "should find widget");
            let widget_stock = widget_row
                .unwrap()
                .as_array()
                .and_then(|a| a.get(1))
                .and_then(serde_json::Value::as_i64);
            assert_eq!(widget_stock, Some(75), "widget should have updated stock");

            // Check gizmo exists (added in tx2)
            let gizmo_row = arr.iter().find(|r| {
                r.as_array()
                    .map(|a| a.first().and_then(|v| v.as_str()) == Some("http://example.org/gizmo"))
                    .unwrap_or(false)
            });
            let gizmo_row = gizmo_row.or_else(|| {
                arr.iter().find(|r| {
                    r.as_array()
                        .map(|a| a.first().and_then(|v| v.as_str()) == Some("ex:gizmo"))
                        .unwrap_or(false)
                })
            });
            assert!(gizmo_row.is_some(), "should find gizmo (added in tx2)");
        })
        .await;
}

#[tokio::test]
async fn test_named_graph_time_travel() {
    // Test time travel queries on named graphs.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-time-travel:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Transaction 1: Initial prices
            let trig1 = r"
                @prefix ex: <http://example.org/> .

                GRAPH <http://example.org/graphs/pricing> {
                    ex:product1 ex:price 100 .
                    ex:product2 ex:price 200 .
                }
            ";

            let result1 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig1)
                .execute()
                .await
                .expect("tx1");
            assert_eq!(result1.receipt.t, 1);

            trigger_index_and_wait(&handle, ledger_id, result1.receipt.t).await;

            // Transaction 2: Price updates using graph().transact() API
            let trig2 = r"
                @prefix ex: <http://example.org/> .

                GRAPH <http://example.org/graphs/pricing> {
                    ex:product1 ex:price 150 .
                    ex:product2 ex:price 175 .
                }
            ";

            let result2 = fluree
                .graph(ledger_id)
                .transact()
                .upsert_turtle(trig2)
                .commit()
                .await
                .expect("tx2");
            assert_eq!(result2.receipt.t, 2);
            eprintln!("DEBUG tx2 flake_count: {}", result2.receipt.flake_count);

            trigger_index_and_wait(&handle, ledger_id, result2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Debug: Query current state via graph fragment syntax
            let query_debug = json!({
                "@context": {"ex": "http://example.org/"},
                "from": format!("{}#http://example.org/graphs/pricing", ledger_id),
                "select": ["?product", "?price"],
                "where": {"@id": "?product", "ex:price": "?price"},
                "orderBy": "?product"
            });
            let results_debug = fluree
                .query_connection(&query_debug)
                .await
                .expect("query debug");
            let results_debug = results_debug
                .to_jsonld(&ledger.snapshot)
                .expect("to_jsonld debug");
            eprintln!(
                "DEBUG current via fragment: {}",
                serde_json::to_string_pretty(&results_debug).unwrap()
            );

            // Debug: Query current state via structured from (no t)
            let query_debug2 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": {"@id": ledger_id, "graph": "http://example.org/graphs/pricing"},
                "select": ["?product", "?price"],
                "where": {"@id": "?product", "ex:price": "?price"},
                "orderBy": "?product"
            });
            let results_debug2 = fluree
                .query_connection(&query_debug2)
                .await
                .expect("query debug2");
            let results_debug2 = results_debug2
                .to_jsonld(&ledger.snapshot)
                .expect("to_jsonld debug2");
            eprintln!(
                "DEBUG current via structured: {}",
                serde_json::to_string_pretty(&results_debug2).unwrap()
            );

            // Query at t=1 (original prices) using structured from object
            let query_t1 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": {
                    "@id": ledger_id,
                    "t": 1,
                    "graph": "http://example.org/graphs/pricing"
                },
                "select": ["?product", "?price"],
                "where": {"@id": "?product", "ex:price": "?price"},
                "orderBy": "?product"
            });

            let results = fluree.query_connection(&query_t1).await.expect("query t=1");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            assert_eq!(arr.len(), 2, "should have 2 products at t=1");

            // product1 should have original price 100
            // Note: results may use prefixed form "ex:product1" due to @context in query
            let p1_row = arr.iter().find(|r| {
                r.as_array()
                    .map(|a| {
                        let s = a.first().and_then(|v| v.as_str()).unwrap_or("");
                        s == "http://example.org/product1" || s == "ex:product1"
                    })
                    .unwrap_or(false)
            });
            let p1_price = p1_row
                .and_then(|r| r.as_array())
                .and_then(|a| a.get(1))
                .and_then(serde_json::Value::as_i64);
            assert_eq!(p1_price, Some(100), "product1 at t=1 should be 100");

            // Query at t=2 (updated prices)
            let query_t2 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": {
                    "@id": ledger_id,
                    "t": 2,
                    "graph": "http://example.org/graphs/pricing"
                },
                "select": ["?product", "?price"],
                "where": {"@id": "?product", "ex:price": "?price"},
                "orderBy": "?product"
            });

            let results = fluree.query_connection(&query_t2).await.expect("query t=2");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            eprintln!(
                "DEBUG query_t2 results: {}",
                serde_json::to_string_pretty(&results).unwrap()
            );
            let arr = results.as_array().expect("array");

            // product1 should have updated price 150
            let p1_row = arr.iter().find(|r| {
                r.as_array()
                    .map(|a| {
                        let s = a.first().and_then(|v| v.as_str()).unwrap_or("");
                        s == "http://example.org/product1" || s == "ex:product1"
                    })
                    .unwrap_or(false)
            });
            let p1_price = p1_row
                .and_then(|r| r.as_array())
                .and_then(|a| a.get(1))
                .and_then(serde_json::Value::as_i64);
            assert_eq!(p1_price, Some(150), "product1 at t=2 should be 150");

            // Query current (should match t=2)
            let query_current = json!({
                "@context": {"ex": "http://example.org/"},
                "from": {
                    "@id": ledger_id,
                    "graph": "http://example.org/graphs/pricing"
                },
                "select": ["?product", "?price"],
                "where": {"@id": "?product", "ex:price": "?price"},
                "orderBy": "?product"
            });

            let results = fluree
                .query_connection(&query_current)
                .await
                .expect("query current");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            eprintln!(
                "DEBUG query_current results: {}",
                serde_json::to_string_pretty(&results).unwrap()
            );
            let arr = results.as_array().expect("array");

            let p1_row = arr.iter().find(|r| {
                r.as_array()
                    .map(|a| {
                        let s = a.first().and_then(|v| v.as_str()).unwrap_or("");
                        s == "http://example.org/product1" || s == "ex:product1"
                    })
                    .unwrap_or(false)
            });
            let p1_price = p1_row
                .and_then(|r| r.as_array())
                .and_then(|a| a.get(1))
                .and_then(serde_json::Value::as_i64);
            assert_eq!(p1_price, Some(150), "current product1 should be 150");
        })
        .await;
}

#[tokio::test]
async fn test_named_graph_retraction() {
    // Test that retractions work correctly in named graphs.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/named-graph-retract:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Transaction 1: Add data to named graph
            let trig1 = r"
                @prefix ex: <http://example.org/> .

                GRAPH <http://example.org/graphs/users> {
                    ex:alice ex:active true .
                    ex:bob ex:active true .
                    ex:carol ex:active true .
                }
            ";

            let result1 = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig1)
                .execute()
                .await
                .expect("tx1");

            trigger_index_and_wait(&handle, ledger_id, result1.receipt.t).await;

            // Transaction 2: Delete bob from the named graph
            // Use JSON-LD delete with graph selector
            let delete_tx = json!({
                "@context": {"ex": "http://example.org/"},
                "delete": [{
                    "@id": "ex:bob",
                    "@graph": "http://example.org/graphs/users",
                    "ex:active": true
                }]
            });

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let result2 = fluree.update(ledger, &delete_tx).await.expect("tx2");
            assert_eq!(result2.receipt.t, 2);

            trigger_index_and_wait(&handle, ledger_id, result2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Query current - should have alice and carol, but NOT bob
            let users_alias = format!("{ledger_id}#http://example.org/graphs/users");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &users_alias,
                "select": "?user",
                "where": {"@id": "?user", "ex:active": true},
                "orderBy": "?user"
            });

            let results = fluree
                .query_connection(&query)
                .await
                .expect("query current");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            assert_eq!(
                arr.len(),
                2,
                "should have 2 active users after retraction: {arr:?}"
            );

            let user_ids: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            let has_user = |full: &str, prefixed: &str| {
                user_ids.contains(&full) || user_ids.contains(&prefixed)
            };
            assert!(
                has_user("http://example.org/alice", "ex:alice"),
                "alice should be active"
            );
            assert!(
                has_user("http://example.org/carol", "ex:carol"),
                "carol should be active"
            );
            assert!(
                !has_user("http://example.org/bob", "ex:bob"),
                "bob should NOT be active"
            );

            // Query at t=1 - should have all three
            let query_t1 = json!({
                "@context": {"ex": "http://example.org/"},
                "from": {
                    "@id": ledger_id,
                    "t": 1,
                    "graph": "http://example.org/graphs/users"
                },
                "select": "?user",
                "where": {"@id": "?user", "ex:active": true}
            });

            let results = fluree.query_connection(&query_t1).await.expect("query t=1");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");

            assert_eq!(arr.len(), 3, "should have 3 active users at t=1: {arr:?}");
        })
        .await;
}
