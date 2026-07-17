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

use crate::support::{
    self, genesis_ledger, start_background_indexer_local, trigger_index_and_wait,
};
use fluree_db_api::{FlureeBuilder, LedgerManagerConfig};
use fluree_db_transact::Txn;
use serde_json::json;

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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
async fn test_trig_compact_named_graph_block() {
    // Issue #1278: the W3C-compliant compact graph block form `<iri> { ... }`
    // (no `GRAPH` keyword) must be accepted and behave identically to the
    // keyword form. Stock RDF tooling (rdflib, Jena, RDF4J) emits this form.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/trig-compact-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Compact form: graph label `<urn:graph:test>` with NO GRAPH keyword,
            // plus a default-graph triple. This is exactly the payload from the
            // issue's reproducer that rdflib/Jena/RDF4J emit by default.
            let trig = r#"
                @prefix ex: <http://example.org/> .

                ex:alice ex:name "Alice" .

                <urn:graph:test> {
                    ex:event1 ex:description "User login" .
                }
            "#;

            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("compact-form TriG should be accepted");
            assert_eq!(result.receipt.t, 1);

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Default graph triple landed.
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": ledger_id,
                "select": "?name",
                "where": {"@id": "ex:alice", "ex:name": "?name"}
            });
            let results = fluree
                .query_connection(&query)
                .await
                .expect("query default");
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert_eq!(arr[0], "Alice", "default-graph triple should be present");

            // Named graph triple landed under the compact label.
            let named_graph_alias = format!("{ledger_id}#urn:graph:test");
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": &named_graph_alias,
                "select": "?desc",
                "where": {"@id": "ex:event1", "ex:description": "?desc"}
            });
            let results = fluree
                .query_connection(&query)
                .await
                .expect("query named graph");
            let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
            let arr = results.as_array().expect("array");
            assert!(
                !arr.is_empty(),
                "compact-form named graph should be queryable"
            );
            assert_eq!(arr[0], "User login");
        })
        .await;
}

/// Helper: run a raw SPARQL UPDATE string through the parse → lower → stage
/// pipeline (the same path the server's `/v1/fluree/update` endpoint uses).
async fn run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> fluree_db_api::TransactResult {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
}

#[tokio::test]
async fn test_sparql_insert_data_named_graph() {
    // Issue #1288: INSERT DATA { GRAPH <g> { ... } } must land triples in the
    // named graph (RDF4J's SPARQLConnection.add(stmts, ctx) emits this).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/sparql-insert-data-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let sparql = r#"
                INSERT DATA {
                    <https://example.org/s/0> <https://example.org/p> "default" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }
            "#;
            let result = run_sparql_update(&fluree, ledger, sparql).await;
            assert_eq!(result.receipt.t, 1);

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Default-graph triple present.
            let q_default = json!({
                "from": ledger_id,
                "select": "?o",
                "where": {"@id": "https://example.org/s/0", "https://example.org/p": "?o"}
            });
            let results = fluree
                .query_connection(&q_default)
                .await
                .expect("query default");
            let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
            let arr = arr.as_array().expect("array");
            assert_eq!(arr[0], "default", "default-graph triple should be present");

            // Named-graph triple queryable via the #<iri> fragment.
            let named_alias = format!("{ledger_id}#https://example.org/g/1");
            let q_named = json!({
                "from": &named_alias,
                "select": "?o",
                "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
            });
            let results = fluree
                .query_connection(&q_named)
                .await
                .expect("query named graph should resolve");
            let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
            let arr = arr.as_array().expect("array");
            assert!(!arr.is_empty(), "named-graph triple should be queryable");
            assert_eq!(arr[0], "v");
        })
        .await;
}

#[tokio::test]
async fn test_sparql_delete_data_named_graph() {
    // Issue #1288 (symmetric): DELETE DATA { GRAPH <g> { ... } } retracts a
    // triple from the named graph and leaves default-graph data untouched.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/sparql-delete-data-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Default-graph triple + a named-graph triple (distinct subjects).
            let insert = r#"
                INSERT DATA {
                    <https://example.org/s/0> <https://example.org/p> "keep" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }
            "#;
            let r1 = run_sparql_update(&fluree, ledger, insert).await;
            trigger_index_and_wait(&handle, ledger_id, r1.receipt.t).await;

            // Delete the named-graph triple.
            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
            let delete = r#"
                DELETE DATA {
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }
            "#;
            let r2 = run_sparql_update(&fluree, ledger, delete).await;
            assert!(r2.receipt.t > r1.receipt.t, "delete should bump t");
            trigger_index_and_wait(&handle, ledger_id, r2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");

            // Named graph no longer has the triple.
            let named_alias = format!("{ledger_id}#https://example.org/g/1");
            let q_named = json!({
                "from": &named_alias,
                "select": "?o",
                "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
            });
            let results = fluree
                .query_connection(&q_named)
                .await
                .expect("query named graph");
            let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
            let arr = arr.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "named-graph triple should be deleted, got {arr:?}"
            );

            // Default-graph data is untouched.
            let q_default = json!({
                "from": ledger_id,
                "select": "?o",
                "where": {"@id": "https://example.org/s/0", "https://example.org/p": "?o"}
            });
            let results = fluree
                .query_connection(&q_default)
                .await
                .expect("query default");
            let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
            let arr = arr.as_array().expect("array");
            assert_eq!(arr[0], "keep", "default-graph triple should survive");
        })
        .await;
}

#[tokio::test]
async fn test_sparql_delete_where_named_graph_block() {
    // W3C dawg-delete-where-02/04/06 shape: DELETE WHERE { GRAPH <g> { ... } }
    // matches inside the named graph and retracts only there. Routed through
    // the same Modify-with-GRAPH lowering as DELETE/INSERT ... WHERE.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/sparql-delete-where-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Same subject/predicate in the default graph, g1 (two values),
            // and g2 — only g1's matches may be deleted.
            let insert = r#"
                INSERT DATA {
                    <https://example.org/a> <https://example.org/knows> "default" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/a> <https://example.org/knows> "b" .
                        <https://example.org/a> <https://example.org/knows> "c" .
                        <https://example.org/a> <https://example.org/name> "Alice" .
                    }
                    GRAPH <https://example.org/g/2> {
                        <https://example.org/a> <https://example.org/knows> "d" .
                    }
                }
            "#;
            let r1 = run_sparql_update(&fluree, ledger, insert).await;
            trigger_index_and_wait(&handle, ledger_id, r1.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
            let delete = r"
                DELETE WHERE {
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/a> <https://example.org/knows> ?b
                    }
                }
            ";
            let r2 = run_sparql_update(&fluree, ledger, delete).await;
            assert!(r2.receipt.t > r1.receipt.t, "delete should bump t");
            trigger_index_and_wait(&handle, ledger_id, r2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");

            let values = |from: &str, pred: &str| {
                let q = json!({
                    "from": from,
                    "select": "?o",
                    "where": {"@id": "https://example.org/a", pred: "?o"}
                });
                let fluree = &fluree;
                let ledger = &ledger;
                async move {
                    let results = fluree.query_connection(&q).await.expect("query");
                    let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
                    arr.as_array().expect("array").clone()
                }
            };

            // g1: both `knows` triples retracted, unrelated predicate kept.
            let g1 = format!("{ledger_id}#https://example.org/g/1");
            let knows_g1 = values(&g1, "https://example.org/knows").await;
            assert!(
                knows_g1.is_empty(),
                "g1 `knows` triples should be deleted, got {knows_g1:?}"
            );
            let name_g1 = values(&g1, "https://example.org/name").await;
            assert_eq!(name_g1, vec!["Alice"], "non-matching g1 triple survives");

            // g2 and the default graph are untouched.
            let g2 = format!("{ledger_id}#https://example.org/g/2");
            let knows_g2 = values(&g2, "https://example.org/knows").await;
            assert_eq!(knows_g2, vec!["d"], "g2 must not be affected");
            let knows_default = values(ledger_id, "https://example.org/knows").await;
            assert_eq!(
                knows_default,
                vec!["default"],
                "default graph must not be affected"
            );
        })
        .await;
}

#[tokio::test]
async fn test_jsonld_delete_where_named_graph_scoped() {
    // JSON-LD parity for DELETE WHERE { GRAPH <g> { ... } } (three-surface
    // rule: the graph-scoped delete-where capability must be expressible and
    // guarded on the JSON-LD surface too). Top-level "graph" scopes both the
    // WHERE match and the delete template to the named graph.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/jsonld-delete-where-graph:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let insert = r#"
                INSERT DATA {
                    <https://example.org/a> <https://example.org/knows> "default" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/a> <https://example.org/knows> "b" .
                        <https://example.org/a> <https://example.org/knows> "c" .
                    }
                    GRAPH <https://example.org/g/2> {
                        <https://example.org/a> <https://example.org/knows> "d" .
                    }
                }
            "#;
            let r1 = run_sparql_update(&fluree, ledger, insert).await;
            trigger_index_and_wait(&handle, ledger_id, r1.receipt.t).await;

            // JSON-LD analogue of DELETE WHERE { GRAPH <g1> { :a :knows ?b } }.
            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
            let update = json!({
                "graph": "https://example.org/g/1",
                "where": { "@id": "https://example.org/a", "https://example.org/knows": "?b" },
                "delete": { "@id": "https://example.org/a", "https://example.org/knows": "?b" }
            });
            let r2 = fluree
                .update(ledger, &update)
                .await
                .expect("graph-scoped JSON-LD delete");
            assert!(r2.receipt.t > r1.receipt.t, "delete should bump t");
            trigger_index_and_wait(&handle, ledger_id, r2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");

            let values = |from: &str| {
                let q = json!({
                    "from": from,
                    "select": "?o",
                    "where": {"@id": "https://example.org/a", "https://example.org/knows": "?o"}
                });
                let fluree = &fluree;
                let ledger = &ledger;
                async move {
                    let results = fluree.query_connection(&q).await.expect("query");
                    let arr = results.to_jsonld(&ledger.snapshot).expect("jsonld");
                    arr.as_array().expect("array").clone()
                }
            };

            let g1 = format!("{ledger_id}#https://example.org/g/1");
            let knows_g1 = values(&g1).await;
            assert!(
                knows_g1.is_empty(),
                "g1 `knows` triples should be deleted, got {knows_g1:?}"
            );
            let g2 = format!("{ledger_id}#https://example.org/g/2");
            assert_eq!(values(&g2).await, vec!["d"], "g2 must not be affected");
            assert_eq!(
                values(ledger_id).await,
                vec!["default"],
                "default graph must not be affected"
            );
        })
        .await;
}

#[tokio::test]
async fn test_insert_data_same_triple_default_and_named_one_txn() {
    // Graph-scoping regression: the SAME (s,p,o) asserted in both the default
    // graph and a named graph within ONE transaction must produce TWO distinct
    // flakes. Previously the graph-blind FlakeAccumulator collapsed them.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/repro-same-triple-one-txn:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);
            let sparql = r#"
                INSERT DATA {
                    <https://example.org/s/1> <https://example.org/p> "v" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }
            "#;
            let r = run_sparql_update(&fluree, ledger, sparql).await;
            trigger_index_and_wait(&handle, ledger_id, r.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            let q_default = json!({
                "from": ledger_id,
                "select": "?o",
                "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
            });
            let d = fluree
                .query_connection(&q_default)
                .await
                .expect("q default");
            let d = d.to_jsonld(&ledger.snapshot).expect("jsonld");
            let d = d.as_array().expect("array");

            let named_alias = format!("{ledger_id}#https://example.org/g/1");
            let q_named = json!({
                "from": &named_alias,
                "select": "?o",
                "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
            });
            let n = fluree.query_connection(&q_named).await.expect("q named");
            let n = n.to_jsonld(&ledger.snapshot).expect("jsonld");
            let n = n.as_array().expect("array");

            assert_eq!(
                d.first(),
                Some(&json!("v")),
                "default-graph copy must exist"
            );
            assert_eq!(n.first(), Some(&json!("v")), "named-graph copy must exist");
        })
        .await;
}

#[tokio::test]
async fn test_delete_data_graph_scoped_with_indexed_default_copy() {
    // Graph-scoping regression (across transactions): default copy and named
    // copy committed+indexed separately; DELETE DATA { GRAPH <g> { .. } } must
    // remove ONLY the named copy.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/repro-delete-indexed-default:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // txn1: default copy
            let r1 = run_sparql_update(
                &fluree,
                ledger,
                r#"INSERT DATA { <https://example.org/s/1> <https://example.org/p> "v" }"#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, r1.receipt.t).await;

            // txn2: named copy
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            let r2 = run_sparql_update(
                &fluree,
                ledger,
                r#"INSERT DATA { GRAPH <https://example.org/g/1> { <https://example.org/s/1> <https://example.org/p> "v" } }"#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, r2.receipt.t).await;

            // txn3: delete only the named copy
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            let r3 = run_sparql_update(
                &fluree,
                ledger,
                r#"DELETE DATA { GRAPH <https://example.org/g/1> { <https://example.org/s/1> <https://example.org/p> "v" } }"#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, r3.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            let named_alias = format!("{ledger_id}#https://example.org/g/1");
            let n = fluree
                .query_connection(&json!({
                    "from": &named_alias,
                    "select": "?o",
                    "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
                }))
                .await
                .expect("q named");
            let n = n.to_jsonld(&ledger.snapshot).expect("jsonld");
            let n = n.as_array().expect("array");

            let d = fluree
                .query_connection(&json!({
                    "from": ledger_id,
                    "select": "?o",
                    "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
                }))
                .await
                .expect("q default");
            let d = d.to_jsonld(&ledger.snapshot).expect("jsonld");
            let d = d.as_array().expect("array");

            assert!(n.is_empty(), "named-graph copy must be deleted, got {n:?}");
            assert_eq!(d.first(), Some(&json!("v")), "default-graph copy must survive");
        })
        .await;
}

#[tokio::test]
async fn test_delete_data_same_triple_two_graphs_one_txn() {
    // Graph-scoping regression (DELETE side, within one transaction): a single
    // DELETE DATA retracting the SAME (s,p,o) from both the default graph and a
    // named graph must remove BOTH. Previously the two retractions collapsed in
    // the graph-blind FlakeAccumulator and only one graph was retracted.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/delete-same-triple-two-graphs:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Seed the same triple in both graphs (one txn; relies on the
            // INSERT-side fix to keep both copies).
            let r1 = run_sparql_update(
                &fluree,
                ledger,
                r#"INSERT DATA {
                    <https://example.org/s/1> <https://example.org/p> "v" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }"#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, r1.receipt.t).await;

            // One DELETE DATA retracting the triple from BOTH graphs.
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            let r2 = run_sparql_update(
                &fluree,
                ledger,
                r#"DELETE DATA {
                    <https://example.org/s/1> <https://example.org/p> "v" .
                    GRAPH <https://example.org/g/1> {
                        <https://example.org/s/1> <https://example.org/p> "v" .
                    }
                }"#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, r2.receipt.t).await;

            let ledger = fluree.ledger(ledger_id).await.expect("reload");

            let d = fluree
                .query_connection(&json!({
                    "from": ledger_id,
                    "select": "?o",
                    "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
                }))
                .await
                .expect("q default");
            let d = d.to_jsonld(&ledger.snapshot).expect("jsonld");
            let d = d.as_array().expect("array");

            let named_alias = format!("{ledger_id}#https://example.org/g/1");
            let n = fluree
                .query_connection(&json!({
                    "from": &named_alias,
                    "select": "?o",
                    "where": {"@id": "https://example.org/s/1", "https://example.org/p": "?o"}
                }))
                .await
                .expect("q named");
            let n = n.to_jsonld(&ledger.snapshot).expect("jsonld");
            let n = n.as_array().expect("array");

            assert!(
                d.is_empty(),
                "default-graph copy must be deleted, got {d:?}"
            );
            assert!(n.is_empty(), "named-graph copy must be deleted, got {n:?}");
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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

/// Count the (s, p, o) triples visible in a single named graph, addressed by
/// its composite `<ledger_id>#<graph-iri>` key. Borrows `fluree` so it can be
/// called repeatedly between updates without moving it out of the test scope.
async fn count_named_graph_triples(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    named_graph_from: &str,
) -> usize {
    let query = json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "from": named_graph_from,
        "select": ["?s", "?p", "?o"],
        "where": { "@id": "?s", "?p": "?o" }
    });
    let results = fluree
        .query_connection(&query)
        .await
        .expect("named-graph count query");
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    let results = results.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    results.as_array().expect("array").len()
}

#[tokio::test]
async fn test_update_delete_where_graph_block_restricted_to_from_named() {
    // JSON-LD parity for the SPARQL USING + explicit-GRAPH over-delete fix
    // (W3C dawg-delete-using-02a/06a, #1441). `fromNamed` is the JSON-LD
    // `USING NAMED` equivalent: it defines the set of named graphs visible to
    // WHERE evaluation exactly. An explicit `["graph", <g>, ...]` block in the
    // WHERE must therefore match nothing when `<g>` is NOT in `fromNamed` — it
    // must not "override" the dataset scoping and over-reach into `<g>`. This
    // exercises the same shared runtime-dataset named-graph restriction in
    // `stream_where_into_accumulator` that SPARQL `USING` now routes through.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/update-delete-where-graph-restricted:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // g2 holds Chris (name + email) and Eve (name); g3 holds Dan.
            let seed = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "insert": [
                    ["graph", "http://example.org/g2", { "@id": "ex:c", "schema:name": "Chris" }],
                    ["graph", "http://example.org/g2", { "@id": "ex:c", "schema:email": "chris@example.org" }],
                    ["graph", "http://example.org/g2", { "@id": "ex:e", "schema:name": "Eve" }],
                    ["graph", "http://example.org/g3", { "@id": "ex:d", "schema:name": "Dan" }]
                ]
            });
            let result = fluree.update(ledger, &seed).await.expect("seed");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            // Count the triples currently in g2 (queried via the composite key).
            let named_g2 = format!("{ledger_id}#http://example.org/g2");

            assert_eq!(
                count_named_graph_triples(&fluree, ledger_id, &named_g2).await,
                3,
                "g2 should start with 3 triples (Chris name+email, Eve name)"
            );

            // Restricted case: fromNamed lists ONLY g3, so the explicit
            // `["graph", g2, ...]` WHERE block must match nothing even though g2
            // physically contains a matching `schema:name "Chris"` row. Nothing
            // is deleted — the graph block does not override the fromNamed scope.
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let restricted = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "fromNamed": [ { "graph": "http://example.org/g3" } ],
                "delete": [ ["graph", "http://example.org/g2", { "@id": "?s", "?p": "?o" }] ],
                "where":  [ ["graph", "http://example.org/g2", { "@id": "?s", "schema:name": "Chris", "?p": "?o" }] ]
            });
            let result = fluree
                .update(ledger, &restricted)
                .await
                .expect("restricted delete-where");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            assert_eq!(
                count_named_graph_triples(&fluree, ledger_id, &named_g2).await,
                3,
                "g2 must be UNCHANGED: the graph-scoped WHERE block on g2 was \
                 scoped out by fromNamed=[g3] and must not over-delete"
            );

            // Positive control: with g2 IN fromNamed, the identical delete-where
            // now sees g2 and removes exactly Chris's two triples, leaving Eve.
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
            let in_scope = json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "fromNamed": [ { "graph": "http://example.org/g2" } ],
                "delete": [ ["graph", "http://example.org/g2", { "@id": "?s", "?p": "?o" }] ],
                "where":  [ ["graph", "http://example.org/g2", { "@id": "?s", "schema:name": "Chris", "?p": "?o" }] ]
            });
            let result = fluree
                .update(ledger, &in_scope)
                .await
                .expect("in-scope delete-where");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;

            assert_eq!(
                count_named_graph_triples(&fluree, ledger_id, &named_g2).await,
                1,
                "with g2 in fromNamed the delete-where fires: Chris's name+email \
                 are removed, leaving only Eve's name"
            );
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
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

// =============================================================================
// PR-U3 — graph-management query-surface parity (transact builder / Txn IR)
// =============================================================================

/// Count the triples currently visible in named graph `iri` of `ledger`.
async fn count_in_graph(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    iri: &str,
) -> usize {
    let sparql = format!("SELECT ?s ?p ?o WHERE {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}");
    let result = support::query_sparql(fluree, ledger, &sparql)
        .await
        .expect("graph count query");
    let v = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    match v.as_array() {
        Some(rows) => rows.len(),
        None => 0,
    }
}

/// PR-U3 query-surface parity (compliance case 2): the SPARQL 1.1 Update
/// graph-management verbs are a genuinely new *capability* (retract-all /
/// copy a whole graph), so they are exposed on the non-SPARQL transact surface
/// too — `Txn::clear_graph`/`drop_graph`/`copy_graph` (shared by the JSON-LD
/// and FQL transact paths, since all lower to the one `Txn` IR + staging).
/// This test drives that builder API directly (no SPARQL text) and asserts the
/// outcome is identical to the equivalent SPARQL `CLEAR`/`COPY GRAPH`.
#[tokio::test]
async fn test_graph_mgmt_transact_builder_parity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let g1 = "http://example.org/g1";
    let g2 = "http://example.org/g2";
    let g3 = "http://example.org/g3";

    let seed = format!(
        r#"INSERT DATA {{
            GRAPH <{g1}> {{ <http://example.org/s1> <http://example.org/p> "in-g1" }}
            GRAPH <{g2}> {{ <http://example.org/s2> <http://example.org/p> "in-g2" }}
        }}"#
    );

    // --- Builder-API path: clear_graph + copy_graph via the Txn IR ---
    let ledger = genesis_ledger(&fluree, "it/graph-mgmt-parity-builder:main");
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;
    assert_eq!(count_in_graph(&fluree, &ledger, g1).await, 1);
    assert_eq!(count_in_graph(&fluree, &ledger, g2).await, 1);

    // copy_graph(g1 -> g3): g3 gains g1's content.
    let ledger = fluree
        .stage_owned(ledger)
        .txn(Txn::copy_graph(g1, g3))
        .execute()
        .await
        .expect("Txn::copy_graph")
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger, g3).await,
        1,
        "copy_graph copied g1 into g3"
    );

    // clear_graph(g1): g1 emptied, g2 and the g3 copy untouched.
    let ledger = fluree
        .stage_owned(ledger)
        .txn(Txn::clear_graph(g1))
        .execute()
        .await
        .expect("Txn::clear_graph")
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger, g1).await,
        0,
        "clear_graph emptied g1"
    );
    assert_eq!(
        count_in_graph(&fluree, &ledger, g2).await,
        1,
        "g2 untouched"
    );
    assert_eq!(
        count_in_graph(&fluree, &ledger, g3).await,
        1,
        "g3 copy kept"
    );

    // --- SPARQL path: identical outcome for CLEAR GRAPH (parity) ---
    let ledger_s = genesis_ledger(&fluree, "it/graph-mgmt-parity-sparql:main");
    let ledger_s = run_sparql_update(&fluree, ledger_s, &seed).await.ledger;
    let ledger_s = run_sparql_update(&fluree, ledger_s, &format!("CLEAR GRAPH <{g1}>"))
        .await
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger_s, g1).await,
        0,
        "SPARQL CLEAR GRAPH matches the builder clear_graph"
    );
    assert_eq!(count_in_graph(&fluree, &ledger_s, g2).await, 1);
}

/// Like [`run_sparql_update`] but returns the staging `Result` (mapped to its
/// error string) instead of `expect`-ing success — for negative tests that
/// assert an operation is *rejected*.
async fn try_run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> std::result::Result<(), String> {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// B2: graph-management (CLEAR/DROP/COPY/MOVE/ADD) must reject the reserved
/// system graphs — `#config` (g_id 2, seeds SHACL/uniqueness governance and
/// cross-ledger rules) and `#txn-meta` (g_id 1, commit metadata) — by IRI, the
/// same way the `Named`/`All` scope already filters them out by g_id. On
/// `burndown/wave-3` these operations silently retract governance / shred
/// commit metadata (CLEAR/DROP) or inject flakes into them (COPY/MOVE/ADD dest);
/// here every one must error. (Remove the guards and this test fails: the ops
/// succeed and mutate the reserved graphs.)
#[tokio::test]
async fn test_graph_mgmt_rejects_reserved_graph_targets() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-mgmt-reserved:main";
    let config_iri = fluree_db_core::config_graph_iri(ledger_id);
    let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let g1 = "http://example.org/g1";

    // Seed one user graph so COPY/MOVE/ADD have a valid non-reserved end.
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = format!(
        r#"INSERT DATA {{ GRAPH <{g1}> {{ <http://example.org/s> <http://example.org/p> "v" }} }}"#
    );
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;

    // Reserved graph as CLEAR/DROP target, and as COPY/MOVE/ADD source AND
    // destination — every one must be refused. (`ledger.clone()` is a cheap
    // Arc bump; `stage_owned` consumes it, so each attempt gets its own.)
    let reserved_cases = [
        format!("CLEAR GRAPH <{config_iri}>"),
        format!("DROP GRAPH <{config_iri}>"),
        format!("CLEAR GRAPH <{txn_meta_iri}>"),
        format!("DROP GRAPH <{txn_meta_iri}>"),
        format!("COPY <{g1}> TO <{config_iri}>"), // reserved destination
        format!("MOVE <{g1}> TO <{txn_meta_iri}>"), // reserved destination
        format!("COPY <{config_iri}> TO <{g1}>"), // reserved source
        format!("ADD <{txn_meta_iri}> TO <{g1}>"), // reserved source
    ];
    for sparql in reserved_cases {
        let err = try_run_sparql_update(&fluree, ledger.clone(), &sparql)
            .await
            .expect_err(&format!("reserved-graph op must be rejected: {sparql}"));
        assert!(
            err.contains("reserved system graph"),
            "expected a reserved-graph rejection for `{sparql}`, got: {err}"
        );
    }

    // Control: the same verbs against a normal user graph still succeed, so the
    // guard rejects the reserved graphs specifically, not graph-management.
    try_run_sparql_update(&fluree, ledger.clone(), &format!("CLEAR GRAPH <{g1}>"))
        .await
        .expect("CLEAR of a user graph must still succeed");
}

/// O5: COPY/MOVE/ADD of a named graph that contains edge annotations
/// (`f:reifies*` flakes) must fail loud. Re-homing rewrites only each flake's
/// `g`, desyncing the `f:reifiesGraph` anchor, so on `burndown/wave-3` the
/// annotation is silently dropped at read time (both JSON-LD hydration and the
/// attachment indexer skip a `GraphMismatch` bundle). Here the transfer must
/// error instead of silently losing the annotation. (Remove the guard and this
/// test fails: the COPY/MOVE/ADD succeeds and the annotation is lost.)
#[tokio::test]
async fn test_graph_mgmt_rejects_annotation_bearing_transfer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-mgmt-annotation:main";
    let g2 = "http://example.org/g2";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed an annotated edge: the base triple AND its `f:reifies*` bundle land
    // in the default graph. SPARQL UPDATE only accepts annotation tails in the
    // default graph in v1 (the JSON-LD surface can place them in a named graph),
    // but the guard fires on `f:reifies*` flakes in ANY source graph, so a
    // default-graph source exercises the same path — and DEFAULT -> named is
    // itself an orphaning case: the re-homed bundle would need an
    // `f:reifiesGraph` anchor the source never had.
    let seed = r#"PREFIX ex: <http://example.org/>
INSERT DATA {
  ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
}"#;
    let ledger = run_sparql_update(&fluree, ledger, seed).await.ledger;

    for verb in ["COPY", "MOVE", "ADD"] {
        let sparql = format!("{verb} DEFAULT TO <{g2}>");
        let err = try_run_sparql_update(&fluree, ledger.clone(), &sparql)
            .await
            .expect_err(&format!("annotation-bearing {verb} must be rejected"));
        assert!(
            err.contains("edge annotations"),
            "expected an annotation-transfer rejection for `{sparql}`, got: {err}"
        );
    }

    // Control: COPY of an annotation-free graph still succeeds.
    let plain_iri = "http://example.org/plain";
    let plain = format!(
        r#"INSERT DATA {{ GRAPH <{plain_iri}> {{ <http://example.org/s> <http://example.org/p> "v" }} }}"#
    );
    let ledger = run_sparql_update(&fluree, ledger, &plain).await.ledger;
    try_run_sparql_update(
        &fluree,
        ledger.clone(),
        &format!("COPY <{plain_iri}> TO <http://example.org/plain-copy>"),
    )
    .await
    .expect("COPY of an annotation-free graph must still succeed");
}

/// Graph management runs the SAME enforce_modify_policies as any other
/// transaction — the policy is NOT bypassed on the whole-graph scan/re-home
/// path (stage.rs:1270-1275, a reviewer-praised load-bearing invariant).
/// Regression-lock for the new path: no prior test exercised a graph-mgmt verb
/// under a modify PolicyContext (it_policy_named_graphs covers only query/insert).
/// Passes on both wave-3 and this branch (the gate already exists); this pins it.
#[tokio::test]
async fn test_graph_mgmt_honors_modify_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-mgmt-policy:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // One non-schema flake in the default graph (schema flakes bypass modify
    // policy via is_schema_flake, so use a plain triple).
    let ledger = run_sparql_update(
        &fluree,
        ledger,
        r#"INSERT DATA { <http://example.org/s> <http://example.org/p> "v" }"#,
    )
    .await
    .ledger;

    // View-only, default-deny policy: modifying any flake is forbidden.
    let policy = json!([{
        "@id": "ex:viewOnly",
        "f:action": [{"@id": "f:view"}],
        "f:allow": true
    }]);
    let qc_opts = fluree_db_api::GovernanceOptions {
        policy: Some(policy),
        default_allow: false,
        ..Default::default()
    };
    let policy_ctx = fluree_db_api::policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // CLEAR DEFAULT retracts the seeded flake; enforce_modify_policies must
    // REJECT it under the view-only policy (policy not bypassed on graph-mgmt).
    let result = fluree
        .stage_owned(ledger.clone())
        .txn(Txn::clear_default_graph())
        .policy(policy_ctx)
        .execute()
        .await;
    assert!(
        result.is_err(),
        "CLEAR under a view-only modify policy must be rejected, not bypassed"
    );

    // The rejected CLEAR did not commit — the flake is intact.
    let survived = support::query_sparql(
        &fluree,
        &ledger,
        "SELECT ?p WHERE { <http://example.org/s> ?p ?o }",
    )
    .await
    .expect("post-clear query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    let surviving_rows = match survived.as_array() {
        Some(rows) => rows.len(),
        None => 0,
    };
    assert_eq!(
        surviving_rows, 1,
        "policy-rejected CLEAR must leave the flake intact, got: {survived:?}"
    );
}

/// O3: a graph-management transfer (ADD/COPY/MOVE) whose SOURCE graph was never
/// registered — a typo'd or never-written IRI — must error (SPARQL 1.1 Update
/// §3.2), not silently empty the destination. On `burndown/wave-3` the missing
/// source resolves to `None`, scans as empty, and COPY/MOVE clear the entire
/// destination and copy nothing back in, so `COPY <typo> TO <important>`
/// silently destroys `<important>` (data loss). Here every non-SILENT transfer
/// from a missing source is refused and the destination is preserved.
///
/// The additive-only registry (roadmap D-6) keeps a never-registered source
/// (`None` → error) distinguishable from an emptied-but-registered source
/// (`Some(g_id)` → a legitimate empty source that proceeds); the control at the
/// end pins that distinction, so the guard rejects typos specifically, not
/// every empty source.
///
/// (Remove the source-resolution guard and this test fails: the non-SILENT
/// COPY/MOVE/ADD succeed and the destination is emptied — the wave-3 bug.)
#[tokio::test]
async fn test_graph_mgmt_missing_source_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-mgmt-missing-source:main";
    let dest = "http://example.org/important";
    let missing = "http://example.org/typo"; // never registered

    // Seed only the destination; the source IRI is never written, so it is
    // never entered into the graph registry (resolves to `None` at staging).
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = format!(
        r#"INSERT DATA {{ GRAPH <{dest}> {{
            <http://example.org/s1> <http://example.org/p> "a" .
            <http://example.org/s2> <http://example.org/p> "b"
        }} }}"#
    );
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;
    let before = count_in_graph(&fluree, &ledger, dest).await;
    assert_eq!(before, 2, "destination seeded with two triples");

    // Every non-SILENT transfer verb from the missing source must error, and
    // the destination must be left intact — the rejected txn never commits, so
    // the pre-txn snapshot still holds the data. (`ledger.clone()` is a cheap
    // Arc bump; `stage_owned` consumes it, so each attempt gets its own.)
    for verb in ["COPY", "MOVE", "ADD"] {
        let sparql = format!("{verb} <{missing}> TO <{dest}>");
        let err = try_run_sparql_update(&fluree, ledger.clone(), &sparql)
            .await
            .expect_err(&format!("{verb} from a missing source must be rejected"));
        assert!(
            err.contains("does not exist"),
            "expected a missing-source rejection for `{sparql}`, got: {err}"
        );
        assert_eq!(
            count_in_graph(&fluree, &ledger, dest).await,
            before,
            "destination must be preserved after the rejected `{sparql}`"
        );
    }

    // SILENT opts into the clear-and-copy-nothing behavior: no error, and the
    // destination is emptied (COPY overwrites it with the missing source's
    // empty contents). The user asked to ignore the missing source.
    let ledger_silent = run_sparql_update(
        &fluree,
        ledger.clone(),
        &format!("COPY SILENT <{missing}> TO <{dest}>"),
    )
    .await
    .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger_silent, dest).await,
        0,
        "COPY SILENT from a missing source clears the destination (opted in)"
    );

    // Control: a source graph that WAS registered and then emptied (CLEAR keeps
    // it in the additive-only registry, D-6) is a legitimate empty source, so
    // COPY from it must NOT error — distinguishing it from the never-registered
    // case above and proving the guard rejects typos specifically, not every
    // empty source. Uses an independent ledger so the committed SILENT case
    // above does not advance this scenario's head.
    let src = "http://example.org/src";
    let ledger_ctl = genesis_ledger(&fluree, "it/graph-mgmt-empty-source:main");
    let seed_ctl = format!(
        r#"INSERT DATA {{
            GRAPH <{src}> {{ <http://example.org/x> <http://example.org/p> "seed" }}
            GRAPH <{dest}> {{ <http://example.org/s1> <http://example.org/p> "a" }}
        }}"#
    );
    let ledger_ctl = run_sparql_update(&fluree, ledger_ctl, &seed_ctl)
        .await
        .ledger;
    // Empty the source but keep it registered.
    let ledger_ctl = run_sparql_update(&fluree, ledger_ctl, &format!("CLEAR GRAPH <{src}>"))
        .await
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger_ctl, src).await,
        0,
        "source graph emptied but still registered"
    );
    // Registered-but-empty source: COPY proceeds without error (run_sparql_update
    // `expect`s staging success, so a spurious rejection would panic here) and
    // overwrites the destination with the empty source (dest: 1 -> 0).
    let ledger_ctl = run_sparql_update(&fluree, ledger_ctl, &format!("COPY <{src}> TO <{dest}>"))
        .await
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger_ctl, dest).await,
        0,
        "COPY from a legitimately empty (registered) source clears the destination without error"
    );
}

/// Count the DEFAULT-graph triples visible to a plain (ambient) query.
async fn count_in_default(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
) -> usize {
    let result = support::query_sparql(fluree, ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .await
        .expect("default-graph count query");
    let v = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    match v.as_array() {
        Some(rows) => rows.len(),
        None => 0,
    }
}

/// SPARQL 1.1 §13.2.1 (via Update §3.1.3): `USING NAMED` without a plain
/// `USING` gives the WHERE dataset an EMPTY default graph — a default-scoped
/// WHERE pattern binds nothing. Before the fix, default-graph selection fell
/// through to the ledger's real default graph, so
/// `DELETE { ?s ?p ?o } USING NAMED <h> WHERE { ?s ?p ?o }` deleted the whole
/// default graph (the same over-reach class as #1441, one clause over).
/// (Remove the `where_default_is_empty` arm in stage.rs and the first
/// assertion fails: the default graph is emptied.)
#[tokio::test]
async fn test_using_named_only_where_default_graph_is_empty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/using-named-only:main";
    let h = "http://example.org/h";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = format!(
        r#"INSERT DATA {{
            <http://example.org/s1> <http://example.org/p> "a" .
            <http://example.org/s2> <http://example.org/p> "b" .
            GRAPH <{h}> {{ <http://example.org/s3> <http://example.org/p> "c" }}
        }}"#
    );
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;
    assert_eq!(count_in_default(&fluree, &ledger).await, 2);
    assert_eq!(count_in_graph(&fluree, &ledger, h).await, 1);

    // USING NAMED only + default-scoped WHERE: binds nothing, deletes nothing.
    let ledger = run_sparql_update(
        &fluree,
        ledger,
        &format!("DELETE {{ ?s ?p ?o }} USING NAMED <{h}> WHERE {{ ?s ?p ?o }}"),
    )
    .await
    .ledger;
    assert_eq!(
        count_in_default(&fluree, &ledger).await,
        2,
        "USING NAMED-only WHERE must see an EMPTY default graph, not the real one"
    );
    assert_eq!(count_in_graph(&fluree, &ledger, h).await, 1);

    // The named set is still visible: an explicit GRAPH block over the USING
    // NAMED graph matches and deletes from it.
    let ledger = run_sparql_update(
        &fluree,
        ledger,
        &format!(
            "DELETE {{ GRAPH <{h}> {{ ?s ?p ?o }} }} USING NAMED <{h}> \
             WHERE {{ GRAPH <{h}> {{ ?s ?p ?o }} }}"
        ),
    )
    .await
    .ledger;
    assert_eq!(count_in_graph(&fluree, &ledger, h).await, 0);
    assert_eq!(count_in_default(&fluree, &ledger).await, 2);

    // Control: the no-USING ambient path is untouched — a plain DELETE WHERE
    // over the default graph still matches it.
    let ledger = run_sparql_update(&fluree, ledger, "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }")
        .await
        .ledger;
    assert_eq!(count_in_default(&fluree, &ledger).await, 0);
}

/// Builder↔SPARQL parity for the two transfer verbs the builder previously
/// lacked: `Txn::move_graph` ≡ `MOVE <from> TO <to>` (source retracted) and
/// `Txn::add_graph` ≡ `ADD <from> TO <to>` (destination contents kept).
#[tokio::test]
async fn test_graph_mgmt_builder_move_add_parity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let g1 = "http://example.org/g1";
    let g2 = "http://example.org/g2";
    let seed = format!(
        r#"INSERT DATA {{
            GRAPH <{g1}> {{ <http://example.org/s1> <http://example.org/p> "in-g1" }}
            GRAPH <{g2}> {{ <http://example.org/s2> <http://example.org/p> "in-g2" }}
        }}"#
    );

    // --- Builder path ---
    let ledger = genesis_ledger(&fluree, "it/graph-mgmt-moveadd-builder:main");
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;

    // add_graph(g1 -> g2): g2 keeps its own triple AND gains g1's; g1 intact.
    let ledger = fluree
        .stage_owned(ledger)
        .txn(Txn::add_graph(g1, g2))
        .execute()
        .await
        .expect("Txn::add_graph")
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger, g1).await,
        1,
        "ADD keeps source"
    );
    assert_eq!(
        count_in_graph(&fluree, &ledger, g2).await,
        2,
        "ADD merges into dest"
    );

    // move_graph(g1 -> g2): dest replaced by source; source gone.
    let ledger = fluree
        .stage_owned(ledger)
        .txn(Txn::move_graph(g1, g2))
        .execute()
        .await
        .expect("Txn::move_graph")
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger, g1).await,
        0,
        "MOVE retracts source"
    );
    assert_eq!(
        count_in_graph(&fluree, &ledger, g2).await,
        1,
        "MOVE replaces dest"
    );

    // --- SPARQL path: identical outcomes ---
    let ledger_s = genesis_ledger(&fluree, "it/graph-mgmt-moveadd-sparql:main");
    let ledger_s = run_sparql_update(&fluree, ledger_s, &seed).await.ledger;
    let ledger_s = run_sparql_update(&fluree, ledger_s, &format!("ADD <{g1}> TO <{g2}>"))
        .await
        .ledger;
    assert_eq!(count_in_graph(&fluree, &ledger_s, g1).await, 1);
    assert_eq!(count_in_graph(&fluree, &ledger_s, g2).await, 2);
    let ledger_s = run_sparql_update(&fluree, ledger_s, &format!("MOVE <{g1}> TO <{g2}>"))
        .await
        .ledger;
    assert_eq!(count_in_graph(&fluree, &ledger_s, g1).await, 0);
    assert_eq!(count_in_graph(&fluree, &ledger_s, g2).await, 1);
}

/// SPARQL 1.1 §3.2.3-3.2.5: COPY/MOVE/ADD of a graph onto itself is a no-op —
/// "no operation will be performed and the data will be left as it was." The
/// guard (`from == to` short-circuit in stage_graph_mgmt) is the only thing
/// between a refactor and MOVE's clear_dest destroying the graph, so pin it
/// for all three verbs on both the SPARQL and builder surfaces.
#[tokio::test]
async fn test_graph_mgmt_same_graph_transfer_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let g = "http://example.org/g";
    let ledger = genesis_ledger(&fluree, "it/graph-mgmt-same-graph:main");
    let seed = format!(
        r#"INSERT DATA {{ GRAPH <{g}> {{
            <http://example.org/s1> <http://example.org/p> "a" .
            <http://example.org/s2> <http://example.org/p> "b"
        }} }}"#
    );
    let mut ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;
    assert_eq!(count_in_graph(&fluree, &ledger, g).await, 2);

    for verb in ["COPY", "MOVE", "ADD"] {
        ledger = run_sparql_update(&fluree, ledger.clone(), &format!("{verb} <{g}> TO <{g}>"))
            .await
            .ledger;
        assert_eq!(
            count_in_graph(&fluree, &ledger, g).await,
            2,
            "{verb} <g> TO <g> must leave the graph exactly as it was"
        );
    }

    // Builder surface too (same Txn IR, same guard).
    for txn in [
        Txn::copy_graph(g, g),
        Txn::move_graph(g, g),
        Txn::add_graph(g, g),
    ] {
        ledger = fluree
            .stage_owned(ledger.clone())
            .txn(txn)
            .execute()
            .await
            .expect("same-graph builder transfer is a no-op, not an error")
            .ledger;
        assert_eq!(count_in_graph(&fluree, &ledger, g).await, 2);
    }

    // A reserved system graph is refused even in the same-graph shape — the
    // no-op must not read as accepting `#config` as a transfer target.
    let config_iri = fluree_db_core::config_graph_iri("it/graph-mgmt-same-graph:main");
    let err = try_run_sparql_update(
        &fluree,
        ledger.clone(),
        &format!("COPY <{config_iri}> TO <{config_iri}>"),
    )
    .await
    .expect_err("same-graph COPY of a reserved graph must be refused");
    assert!(
        err.contains("reserved system graph"),
        "expected the reserved-graph rejection, got: {err}"
    );
}

/// A multi-operation request mixing a DATA op with a graph-management op:
/// each op stages sequentially in ONE atomic commit (§3.1 / D-10), so the
/// COPY/CLEAR must observe the graph its predecessor just created — including
/// the g_id registered earlier in the SAME request (the sequential-staging
/// novelty view + provisional graph registration working together).
#[tokio::test]
async fn test_multi_op_update_mixes_data_and_graph_mgmt() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger("it/multiop-graph-mgmt:main")
        .await
        .expect("create ledger");
    let g = "http://example.org/g";
    let h = "http://example.org/h";

    // Op 1 creates <g>; op 2 copies the just-created graph into <h>.
    let insert_then_copy = format!(
        r#"INSERT DATA {{ GRAPH <{g}> {{ <http://example.org/s> <http://example.org/p> "v" }} }} ;
           COPY <{g}> TO <{h}>"#
    );
    fluree
        .graph("it/multiop-graph-mgmt:main")
        .transact()
        .sparql_update(&insert_then_copy)
        .commit()
        .await
        .expect("INSERT DATA ; COPY executes as one atomic commit");
    let ledger = fluree
        .ledger("it/multiop-graph-mgmt:main")
        .await
        .expect("ledger");
    assert_eq!(
        count_in_graph(&fluree, &ledger, g).await,
        1,
        "op 1's graph survives (COPY keeps its source)"
    );
    assert_eq!(
        count_in_graph(&fluree, &ledger, h).await,
        1,
        "COPY must see the graph op 1 created in the same request"
    );

    // Insert-then-CLEAR of the same graph in one request nets to empty —
    // CLEAR sees the same-request insert through the sequential novelty view.
    let g2 = "http://example.org/g2";
    let insert_then_clear = format!(
        r#"INSERT DATA {{ GRAPH <{g2}> {{ <http://example.org/s2> <http://example.org/p> "w" }} }} ;
           CLEAR GRAPH <{g2}>"#
    );
    fluree
        .graph("it/multiop-graph-mgmt:main")
        .transact()
        .sparql_update(&insert_then_clear)
        .commit()
        .await
        .expect("INSERT DATA ; CLEAR executes as one atomic commit");
    let ledger = fluree
        .ledger("it/multiop-graph-mgmt:main")
        .await
        .expect("ledger");
    assert_eq!(
        count_in_graph(&fluree, &ledger, g2).await,
        0,
        "CLEAR must retract the same-request insert"
    );
}

/// CLEAR of an annotation-bearing graph retracts the whole reification bundle
/// (base edge + f:reifies* anchors) cleanly: the graph reads empty, and
/// re-inserting the SAME annotated edge afterwards succeeds — a leftover
/// (orphaned) anchor would collide with the re-insert's bundle instead.
#[tokio::test]
async fn test_clear_of_annotation_bearing_graph_is_clean() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/clear-annotations:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = r#"PREFIX ex: <http://example.org/>
INSERT DATA { ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} . }"#;
    let ledger = run_sparql_update(&fluree, ledger, seed).await.ledger;
    assert!(
        count_in_default(&fluree, &ledger).await >= 1,
        "annotated edge seeded"
    );

    // CLEAR DEFAULT retracts base edge AND annotation bundle.
    let ledger = run_sparql_update(&fluree, ledger, "CLEAR DEFAULT")
        .await
        .ledger;
    assert_eq!(
        count_in_default(&fluree, &ledger).await,
        0,
        "CLEAR DEFAULT must leave nothing behind (no orphaned anchors)"
    );

    // Re-inserting the identical annotated edge succeeds cleanly — a leftover
    // anchor from an incomplete retraction would corrupt this bundle.
    let ledger = run_sparql_update(&fluree, ledger, seed).await.ledger;
    assert!(
        count_in_default(&fluree, &ledger).await >= 1,
        "re-insert after CLEAR must succeed with a clean bundle"
    );
}

/// CREATE registers the graph in the additive registry, so a subsequent
/// non-SILENT COPY/MOVE/ADD from it is a legitimate EMPTY source — not the O3
/// "source graph does not exist" error, which contradicted CREATE's reported
/// success. Covers both the cross-request and the same-request (multi-op)
/// flow; the never-CREATEd control still errors.
#[tokio::test]
async fn test_create_registers_graph_as_transfer_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/create-registers:main";
    let dest = "http://example.org/dest";
    let fresh = "http://example.org/fresh";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = format!(
        r#"INSERT DATA {{ GRAPH <{dest}> {{ <http://example.org/s> <http://example.org/p> "v" }} }}"#
    );
    let ledger = run_sparql_update(&fluree, ledger, &seed).await.ledger;

    // Cross-request: CREATE then COPY. The COPY must succeed (empty source
    // overwrites dest → dest emptied), not raise SourceGraphNotFound.
    let ledger = run_sparql_update(&fluree, ledger, &format!("CREATE GRAPH <{fresh}>"))
        .await
        .ledger;
    let ledger = run_sparql_update(&fluree, ledger, &format!("COPY <{fresh}> TO <{dest}>"))
        .await
        .ledger;
    assert_eq!(
        count_in_graph(&fluree, &ledger, dest).await,
        0,
        "COPY from a CREATEd (registered, empty) source must proceed"
    );

    // Control: a never-CREATEd source still errors non-SILENTLY.
    let err = try_run_sparql_update(
        &fluree,
        ledger.clone(),
        &format!("COPY <http://example.org/never> TO <{dest}>"),
    )
    .await
    .expect_err("COPY from a never-registered source must still error");
    assert!(err.contains("does not exist"), "got: {err}");

    // Same-request (multi-op): CREATE ; ADD in one atomic commit — the second
    // op must see the first's provisional registration.
    fluree
        .create_ledger("it/create-registers-multi:main")
        .await
        .expect("create ledger");
    fluree
        .graph("it/create-registers-multi:main")
        .transact()
        .sparql_update(
            "CREATE GRAPH <http://example.org/g1> ; \
             ADD <http://example.org/g1> TO <http://example.org/g2>",
        )
        .commit()
        .await
        .expect("CREATE ; ADD must commit (registered empty source)");
}

/// O7 (transact-template twin): an anonymous `[]` in an INSERT template mints
/// a non-lexable `_:[]{n}` label, so it can never collide with a hand-written
/// `_:bN` in the same template. Before the fix the first anon minted `_:b0`,
/// fusing with a user's `_:b0` into ONE node per solution.
#[tokio::test]
async fn test_insert_template_anon_blank_never_merges_with_labeled() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/template-anon-blanks:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let seed = r#"INSERT DATA { <http://example.org/s> <http://example.org/handle> "h1" }"#;
    let ledger = run_sparql_update(&fluree, ledger, seed).await.ledger;

    // One solution row; the template mints `[]` AND user-labeled `_:b0` —
    // they must become TWO distinct blank nodes.
    let ledger = run_sparql_update(
        &fluree,
        ledger,
        r"PREFIX ex: <http://example.org/>
INSERT { [] ex:tagQ ?h . _:b0 ex:tagP ?h }
WHERE { ?s ex:handle ?h }",
    )
    .await
    .ledger;

    // No single subject may carry BOTH tag predicates (the fusion signature).
    let fused = support::query_sparql(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/> \
         SELECT ?b WHERE { ?b ex:tagQ ?h . ?b ex:tagP ?h }",
    )
    .await
    .expect("fusion probe")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        fused.as_array().map(Vec::len).unwrap_or(0),
        0,
        "anon [] and user _:b0 template blanks fused into one node: {fused}"
    );

    // And both tags landed (two distinct blank subjects exist).
    for tag in ["tagQ", "tagP"] {
        let rows = support::query_sparql(
            &fluree,
            &ledger,
            &format!("PREFIX ex: <http://example.org/> SELECT ?b WHERE {{ ?b ex:{tag} ?h }}"),
        )
        .await
        .expect("tag probe")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
        assert_eq!(
            rows.as_array().map(Vec::len).unwrap_or(0),
            1,
            "expected exactly one {tag} subject: {rows}"
        );
    }
}

/// N3 contract pin: schema flakes (rdfs:Class / rdfs:subClassOf …) are exempt
/// from modify policy (`is_schema_flake`), so a view-only, default-deny
/// identity CAN `CLEAR DEFAULT` a schema-only default graph — the documented
/// (policy-unblockable) ontology-wipe footgun. The companion test above
/// proves the SAME identity is rejected for non-schema flakes, so this pins
/// the boundary rather than a policy hole: if the always-allow schema
/// exemption is ever narrowed, this test flips and the change is deliberate.
#[tokio::test]
async fn test_clear_default_schema_flakes_bypass_modify_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-mgmt-schema-policy:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Schema-only default graph: one class declaration.
    let ledger = run_sparql_update(
        &fluree,
        ledger,
        r"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
INSERT DATA { <http://example.org/MyClass> a rdfs:Class }",
    )
    .await
    .ledger;
    assert_eq!(count_in_default(&fluree, &ledger).await, 1);

    // Same view-only, default-deny policy the non-schema test uses.
    let policy = json!([{
        "@id": "ex:viewOnly",
        "f:action": [{"@id": "f:view"}],
        "f:allow": true
    }]);
    let qc_opts = fluree_db_api::GovernanceOptions {
        policy: Some(policy),
        default_allow: false,
        ..Default::default()
    };
    let policy_ctx = fluree_db_api::policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // CLEAR DEFAULT succeeds — the schema retraction is not policy-blockable.
    let result = fluree
        .stage_owned(ledger.clone())
        .txn(Txn::clear_default_graph())
        .policy(policy_ctx)
        .execute()
        .await
        .expect("schema-only CLEAR DEFAULT bypasses modify policy (N3 contract)");
    assert_eq!(
        count_in_default(&fluree, &result.ledger).await,
        0,
        "the ontology was wiped by a view-only identity — the pinned N3 contract"
    );
}
