//! SPARQL query regression tests exercising the **binary index** path.
//!
//! These tests mirror the memory-only regression tests in `it_query_sparql.rs`
//! but trigger indexing so queries go through `BinaryScanOperator` instead of
//! `RangeScanOperator`. This is important because the CLI/server always use the
//! binary path once data is indexed.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{
    FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, QueryInput, QueryResult,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    assert_index_defaults, genesis_ledger_for_fluree, normalize_rows, normalize_sparql_bindings,
    start_background_indexer_local, trigger_index_and_wait_outcome,
};

type MemoryFluree = fluree_db_api::Fluree;
type MemoryLedger = LedgerState;

// =============================================================================
// Shared seeding
// =============================================================================

/// Insert custom-namespace data with forced indexing after each commit.
async fn seed_custom_ns_indexed(
    fluree: &MemoryFluree,
    ledger_id: &str,
    index_cfg: &IndexConfig,
) -> MemoryLedger {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);

    let insert = json!({
        "@context": {
            "cust": "http://example.org/custom/",
            "skos": "http://www.w3.org/2008/05/skos#"
        },
        "@graph": [
            {
                "@id": "cust:pkg1",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-1",
                "skos:broader": {"@id": "cust:pkg2"}
            },
            {
                "@id": "cust:pkg2",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-2",
                "skos:broader": {"@id": "cust:pkg3"}
            },
            {
                "@id": "cust:pkg3",
                "@type": "cust:CoveragePackage",
                "cust:anchor": "anchor-value-3"
            }
        ]
    });

    let result = fluree
        .insert_with_opts(
            ledger,
            &insert,
            TxnOpts::default(),
            CommitOpts::default(),
            index_cfg,
        )
        .await
        .expect("insert");
    let ledger = result.ledger;

    assert_eq!(ledger.t(), 1, "should be at t=1 after seeding");
    ledger
}

// =============================================================================
// Bug regression: custom namespace predicate without rdf:type (indexed path)
// =============================================================================

/// Regression: `SELECT ?s ?o WHERE { ?s cust:anchor ?o }` must return rows
/// when the data is in the binary index (not just novelty).
///
/// The memory-only test passes because `RangeScanOperator` works correctly.
/// This test catches bugs that only manifest in the `BinaryScanOperator` path
/// which is what the CLI and server use after indexing.
#[tokio::test]
async fn indexed_sparql_custom_predicate_without_type_returns_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-custom-pred-idx:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = seed_custom_ns_indexed(&fluree, ledger_id, &index_cfg).await;

            // Trigger indexing and wait
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Load the indexed view (GraphDb) — properly threads binary_store
            let view = fluree.db(ledger_id).await.expect("load view");

            // Baseline: query WITH rdf:type pattern
            let with_type = r"
                PREFIX cust: <http://example.org/custom/>
                SELECT ?s ?o
                WHERE { ?s a cust:CoveragePackage ; cust:anchor ?o . }
                ORDER BY ?o
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(with_type))
                .await
                .expect("query with type");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (with type)");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([
                    ["cust:pkg1", "anchor-value-1"],
                    ["cust:pkg2", "anchor-value-2"],
                    ["cust:pkg3", "anchor-value-3"]
                ])),
                "baseline: query WITH rdf:type should return 3 rows (indexed)"
            );

            // Bug: query WITHOUT rdf:type pattern
            let without_type = r"
                PREFIX cust: <http://example.org/custom/>
                SELECT ?s ?o
                WHERE { ?s cust:anchor ?o . }
                ORDER BY ?o
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(without_type))
                .await
                .expect("query without type");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (without type)");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([
                    ["cust:pkg1", "anchor-value-1"],
                    ["cust:pkg2", "anchor-value-2"],
                    ["cust:pkg3", "anchor-value-3"]
                ])),
                "BUG: query WITHOUT rdf:type should also return 3 rows (indexed path)"
            );

            // Also verify standard namespace predicate works (control)
            let std_pred = r"
                PREFIX cust: <http://example.org/custom/>
                PREFIX skos: <http://www.w3.org/2008/05/skos#>
                SELECT ?s ?o
                WHERE { ?s skos:broader ?o . }
                ORDER BY ?s
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(std_pred))
                .await
                .expect("query std predicate");
            let jsonld = result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld (std pred)");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([
                    ["cust:pkg1", "cust:pkg2"],
                    ["cust:pkg2", "cust:pkg3"]
                ])),
                "standard namespace predicate should work without rdf:type (indexed)"
            );
        })
        .await;
}

// =============================================================================
// Bug regression: UNION with partially-bound SELECT variable (indexed path)
// =============================================================================

/// Regression: UNION with a SELECT variable only bound in one branch should
/// produce null/unbound rows, not a "Variable not found" error — via indexed path.
#[tokio::test]
async fn indexed_sparql_union_partial_select_var() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-union-idx:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = seed_custom_ns_indexed(&fluree, ledger_id, &index_cfg).await;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree.db(ledger_id).await.expect("load view");

            let query = r#"
                PREFIX cust: <http://example.org/custom/>
                PREFIX skos: <http://www.w3.org/2008/05/skos#>
                SELECT ?s ?val ?role
                WHERE {
                  {
                    ?s cust:anchor ?val .
                    BIND("anchor" AS ?role)
                  }
                  UNION
                  {
                    ?s skos:broader ?val .
                  }
                }
                ORDER BY ?s ?val
            "#;

            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("UNION with partially-bound SELECT var should not error (indexed)");

            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("should be array of rows");

            // 3 anchor rows + 2 broader rows = 5 total
            assert!(
                rows.len() >= 5,
                "expected at least 5 rows from both UNION branches (indexed), got {}",
                rows.len()
            );
        })
        .await;
}

// =============================================================================
// Bug 1 repro: novelty-only custom NS predicate after index (user's exact path)
// =============================================================================

/// The user's scenario: import + index → insert with custom NS predicates →
/// query. The custom predicates exist only in novelty, not the binary index.
/// `BinaryScanOperator::translate_range` returned None because `sid_to_p_id`
/// only checks the persisted index, missing novelty-only predicates.
#[tokio::test]
async fn indexed_then_insert_novelty_custom_pred_returns_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-novelty-pred:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed baseline data and index it
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": {
                    "skos": "http://www.w3.org/2004/02/skos/core#",
                    "ex": "http://example.org/ns/"
                },
                "@graph": [
                    {"@id": "ex:concept1", "@type": "skos:Concept", "skos:prefLabel": "One"},
                    {"@id": "ex:concept2", "@type": "skos:Concept", "skos:prefLabel": "Two"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            // Index baseline
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 2: Insert data with a NEW custom NS predicate (only in novelty)
            let custom_insert = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "@graph": [{
                    "@id": "cbc:assoc/coverage-001",
                    "@type": "cust:CoveragePackage",
                    "cust:anchor": {"@id": "cbc:e9235fd0"},
                    "cust:member": [{"@id": "cbc:5b33544d"}, {"@id": "cbc:0476a33f"}]
                }]
            });
            let result = fluree
                .insert(ledger, &custom_insert)
                .await
                .expect("custom ns insert");
            let _ledger = result.ledger;

            // Phase 3: Query via view (same path as CLI)
            let view = fluree.db(ledger_id).await.expect("load view");

            // Bug 1: custom NS predicate without rdf:type returns 0 rows
            let query = r"
                PREFIX cust: <https://taxo.cbcrc.ca/ns/>
                SELECT ?s ?o
                WHERE { ?s cust:anchor ?o . }
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("query novelty-only custom pred");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "novelty-only custom NS predicate should return 1 row; got: {jsonld:?}"
            );
        })
        .await;
}

// =============================================================================
// Bug 2 repro: expansion empty for custom NS type after index
// =============================================================================

/// After index + insert with a custom namespace rdf:type, expansion returns
/// only `{"@id": "..."}` with no properties. The `decode_batch_to_flakes_filtered`
/// function in `binary_range.rs` only handled REF_ID values through DictOverlay
/// fallback, missing DictOverlay-assigned string value IDs.
#[tokio::test]
async fn indexed_then_insert_expansion_custom_type_returns_properties() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/regress-crawl-type:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed baseline and index
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": {
                    "skos": "http://www.w3.org/2004/02/skos/core#",
                    "ex": "http://example.org/ns/"
                },
                "@graph": [
                    {"@id": "ex:concept1", "@type": "skos:Concept", "skos:prefLabel": "One"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 2: Insert with custom NS type (only in novelty)
            let custom_insert = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "@graph": [{
                    "@id": "cbc:assoc/coverage-001",
                    "@type": "cust:CoveragePackage",
                    "cust:associationType": {"@id": "cust:assocType/coverage"},
                    "cust:anchor": {"@id": "cbc:e9235fd0"}
                }]
            });
            let result = fluree
                .insert(ledger, &custom_insert)
                .await
                .expect("custom type insert");
            let _ledger = result.ledger;

            // Phase 3: Graph crawl via view
            let view = fluree.db(ledger_id).await.expect("load view");

            let query = json!({
                "@context": {
                    "cust": "https://taxo.cbcrc.ca/ns/",
                    "cbc": "https://taxo.cbcrc.ca/id/"
                },
                "select": {"?s": ["*"]},
                "values": ["?s", [{"@id": "cbc:assoc/coverage-001"}]]
            });
            let result: QueryResult = fluree.query(&view, &query).await.expect("expansion");
            let jsonld = result
                .to_jsonld_async(view.as_graph_db_ref())
                .await
                .expect("to_jsonld_async");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(rows.len(), 1, "should find 1 entity");
            let obj = rows[0].as_object().expect("object");
            assert!(
                obj.len() > 1,
                "expansion should return properties, not just @id; got: {obj:?}"
            );
        })
        .await;
}

// Regression: repeated vars in a triple pattern must not create duplicate schema
// =============================================================================

/// Regression: indexed/binary-scan path must handle repeated variables in a single triple pattern
/// (e.g. `?x ex:self ?x` or `?x ?x ?o`) without producing a Batch schema containing duplicate VarIds.
#[tokio::test]
async fn indexed_repeated_vars_in_triple_pattern_do_not_duplicate_schema() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-repeated-vars:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Seed and index.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    // Used for ?x ex:self ?x
                    {"@id": "ex:a", "ex:self": {"@id": "ex:a"}},
                    // Used for ?x ?x ?o (predicate IRI equals subject IRI)
                    {"@id": "ex:a", "ex:a": {"@id": "ex:b"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            // 1) subject==object repeated var
            let q1 = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?x WHERE { ?x ex:self ?x }
            ";
            let r1 = fluree
                .query(&view, QueryInput::Sparql(q1))
                .await
                .expect("query 1 should succeed");
            let jsonld1 = r1.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld1),
                normalize_rows(&json!([["ex:a"]])),
                "expected ?x=ex:a"
            );

            // 2) subject==predicate repeated var
            let q2 = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?x ?o WHERE { ?x ?x ?o }
            ";
            let r2 = fluree
                .query(&view, QueryInput::Sparql(q2))
                .await
                .expect("query 2 should succeed");
            let jsonld2 = r2.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld2),
                normalize_rows(&json!([["ex:a", "ex:b"]])),
                "expected (?x,?o)=(ex:a,ex:b)"
            );
        })
        .await;
}

/// Regression: a two-pattern join that shares both subject and object variables
/// (e.g. `?s p1 ?o . ?s p2 ?o`) must not be planned as a PropertyJoinOperator
/// (which assumes distinct object vars) and must execute without duplicate schema.
#[tokio::test]
async fn indexed_multicolumn_join_shared_object_var_executes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-multicolumn-join:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:s1", "ex:p1": {"@id": "ex:o1"}, "ex:p2": {"@id": "ex:o1"}},
                    {"@id": "ex:s2", "ex:p1": {"@id": "ex:o2"}, "ex:p2": {"@id": "ex:o2"}},
                    {"@id": "ex:s3", "ex:p1": {"@id": "ex:o3"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o . ?s ex:p2 ?o . }
            ";
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("multicolumn join query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "expected two matching (s,o) pairs"
            );
        })
        .await;
}

/// Regression: the multicolumn `?s p1 ?o . ?s p2 ?o` COUNT(*) must count matching
/// `(s,o)` PAIRS (composite-key intersection), not the product of per-subject
/// object counts. With a subject carrying several objects under each predicate,
/// a subject-keyed star join would multiply (e.g. 3×3=9) — the composite-key
/// merge must instead intersect on `(s,o)`.
#[tokio::test]
async fn indexed_multicolumn_join_counts_pairs_not_product() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-multicolumn-pairs:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            // s1: p1 -> {o1,o2,o3}, p2 -> {o1,o2,o4}  → shared pairs (s1,o1),(s1,o2) = 2
            // s2: p1 -> {o5},        p2 -> {o5}        → shared pair  (s2,o5)         = 1
            // Total matching (s,o) pairs = 3.
            // A subject-keyed star multiply would give s1: 3×3=9, s2: 1×1=1 → 10.
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {
                        "@id": "ex:s1",
                        "ex:p1": [{"@id": "ex:o1"}, {"@id": "ex:o2"}, {"@id": "ex:o3"}],
                        "ex:p2": [{"@id": "ex:o1"}, {"@id": "ex:o2"}, {"@id": "ex:o4"}]
                    },
                    {
                        "@id": "ex:s2",
                        "ex:p1": {"@id": "ex:o5"},
                        "ex:p2": {"@id": "ex:o5"}
                    }
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o . ?s ex:p2 ?o . }
            ";
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("multicolumn join query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "expected three matching (s,o) pairs, not the per-subject product"
            );
        })
        .await;
}

// =============================================================================
// Overlay correctness: COUNT fast paths must incorporate novelty
// =============================================================================

/// Regression: COUNT queries should return correct results when the binary index
/// is present but changes are in novelty (overlay), including retraction of an
/// indexed fact and re-assertion in novelty.
#[tokio::test]
async fn indexed_overlay_count_reflects_retract_and_reassert() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-count:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed and index 4 ex:Person facts.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "@type": "ex:Person"},
                    {"@id": "ex:p2", "@type": "ex:Person"},
                    {"@id": "ex:p3", "@type": "ex:Person"},
                    {"@id": "ex:p4", "@type": "ex:Person"}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?p a ex:Person . }
            ";

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[4]])),
                "baseline count should be 4"
            );

            // Phase 2: Retract one indexed fact in novelty (overlay).
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:p1", "@type": "ex:Person"}
                ]
            });
            let ledger2 = fluree
                .update(ledger1, &retract)
                .await
                .expect("retract in novelty")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "count should reflect novelty retraction"
            );

            // Phase 3: Re-assert the same fact in novelty.
            let reassert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "@type": "ex:Person"}
                ]
            });
            let ledger3 = fluree
                .insert(ledger2, &reassert)
                .await
                .expect("re-assert in novelty")
                .ledger;

            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("load view at t=3");
            let result = fluree
                .query(&view3, QueryInput::Sparql(query))
                .await
                .expect("count at t=3");
            let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[4]])),
                "count should reflect novelty re-assertion"
            );
        })
        .await;
}

/// Regression (Tier-2): a filterless `COUNT(*)` over a single bound-predicate
/// triple must track novelty even though the metadata count planner bails on
/// overlay.
///
/// Under overlay, `count_plan`/`count_rows` (strategy a) decline and the query
/// falls back to the generic `GroupAggregate(COUNT(*))` over a scan. That
/// pushdown now fires through `DatasetOperator`/`BinaryScanOperator::drain_count`
/// — the count-only "clean lane" sums the overlay-merged cursor's rows without
/// materializing bindings. This verifies the lane reflects both an overlay
/// assertion and an overlay retraction of `ex:knows` edges.
#[tokio::test]
async fn indexed_overlay_count_star_drain_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-count-drain:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed and index 3 ex:knows edges (a→b, a→c, b→c).
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:knows": [{"@id": "ex:b"}, {"@id": "ex:c"}]},
                    {"@id": "ex:b", "ex:knows": {"@id": "ex:c"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Bound predicate, unbound subject + object, no FILTER => the scan is
            // `count_only_eligible` and drain_count counts the cursor directly.
            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?s ex:knows ?o . }
            ";

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "indexed-only count should be 3"
            );

            // Phase 2: Assert a new edge in novelty (c→a).
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:c", "ex:knows": {"@id": "ex:a"}} ]
            });
            let ledger2 = fluree
                .insert(ledger1, &assert)
                .await
                .expect("assert in novelty")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[4]])),
                "count should reflect the overlay assertion"
            );

            // Phase 3: Retract an indexed edge in novelty (a→b).
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:a", "ex:knows": {"@id": "ex:b"}} ]
            });
            let ledger3 = fluree
                .update(ledger2, &retract)
                .await
                .expect("retract in novelty")
                .ledger;

            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("load view at t=3");
            let result = fluree
                .query(&view3, QueryInput::Sparql(query))
                .await
                .expect("count at t=3");
            let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "count should reflect the overlay retraction"
            );
        })
        .await;
}

/// Regression (Tier-2): the encoded-prefilter `COUNT` path (`CountRowsOperator`)
/// must track novelty. It now gates on `allow_cursor_fast_path` (strategy b)
/// instead of `fast_path_store`, so its overlay-merging `DatasetOperator::scan`
/// `fast_child` runs under overlay rather than bailing to the generic fallback.
/// Exercised via `FILTER(?s = ?o)` (an encoded `SubjectEqObjectRef` prefilter).
#[tokio::test]
async fn indexed_overlay_count_encoded_filter_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-count-encoded-filter:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed two self-loops (a→a, b→b) and one non-loop (c→d).
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:rel": {"@id": "ex:a"}},
                    {"@id": "ex:b", "ex:rel": {"@id": "ex:b"}},
                    {"@id": "ex:c", "ex:rel": {"@id": "ex:d"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?s ex:rel ?o . FILTER(?s = ?o) }
            ";

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "indexed-only self-loop count should be 2"
            );

            // Phase 2: Assert a new self-loop in novelty (e→e).
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:e", "ex:rel": {"@id": "ex:e"}} ]
            });
            let ledger2 = fluree
                .insert(ledger1, &assert)
                .await
                .expect("assert in novelty")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "encoded-filter count should reflect the overlay self-loop assertion"
            );

            // Phase 3: Retract an indexed self-loop in novelty (a→a).
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:a", "ex:rel": {"@id": "ex:a"}} ]
            });
            let ledger3 = fluree
                .update(ledger2, &retract)
                .await
                .expect("retract in novelty")
                .ledger;

            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("load view at t=3");
            let result = fluree
                .query(&view3, QueryInput::Sparql(query))
                .await
                .expect("count at t=3");
            let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "encoded-filter count should reflect the overlay self-loop retraction"
            );
        })
        .await;
}

/// Regression: the `<S> <p>+ ?o` COUNT(*) fast path must incorporate novelty.
///
/// Property-path+ COUNT used to gate on `fast_path_store`, which bails the
/// instant an uncommitted overlay is present — so its (overlay-aware) PSOT
/// cursor was dead code. It now gates on `allow_cursor_fast_path` like the
/// transitive-path operator, building adjacency from the overlay-merged cursor.
/// This verifies the count tracks both an overlay assertion and an overlay
/// retraction of `knows` edges.
#[tokio::test]
async fn indexed_overlay_property_path_plus_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-path-plus-count:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed and index a knows-chain p1 -> p2 -> p3 -> p4.
            // Reachable from p1 via knows+ = {p2, p3, p4} = 3.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "ex:knows": {"@id": "ex:p2"}},
                    {"@id": "ex:p2", "ex:knows": {"@id": "ex:p3"}},
                    {"@id": "ex:p3", "ex:knows": {"@id": "ex:p4"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ex:p1 ex:knows+ ?o . }
            ";

            // Baseline (no overlay): fast path runs against the binary index only.
            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "baseline knows+ reachable count from p1 should be 3"
            );

            // Phase 2: Assert p4 -> p5 in novelty (overlay).
            // Reachable from p1 via knows+ = {p2, p3, p4, p5} = 4.
            let extend = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p4", "ex:knows": {"@id": "ex:p5"}}
                ]
            });
            let ledger2 = fluree
                .insert(ledger1, &extend)
                .await
                .expect("extend in novelty")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[4]])),
                "count should reflect the overlay-asserted knows edge"
            );

            // Phase 3: Retract p2 -> p3 in novelty. The effective graph is now
            // p1 -> p2, p3 -> p4, p4 -> p5, so only {p2} is reachable from p1.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:p2", "ex:knows": {"@id": "ex:p3"}}
                ]
            });
            let ledger3 = fluree
                .update(ledger2, &retract)
                .await
                .expect("retract in novelty")
                .ledger;

            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("load view at t=3");
            let result = fluree
                .query(&view3, QueryInput::Sparql(query))
                .await
                .expect("count at t=3");
            let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[1]])),
                "count should reflect the overlay-retracted knows edge"
            );
        })
        .await;
}

/// Regression: `<S> <p>+ ?o` COUNT(*) where the fixed start subject `<S>` exists
/// ONLY in novelty (overlay), not the persisted index. The fast path resolves the
/// fixed subject via the persisted dictionary; an overlay-only subject can't be
/// resolved there, so the operator must bail to the generic pipeline rather than
/// undercount to 0.
#[tokio::test]
async fn indexed_overlay_property_path_plus_count_subject_in_overlay_only() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-path-plus-subject-novelty:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed and index a knows-chain p1 -> p2 -> p3 (no ex:new).
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:p1", "ex:knows": {"@id": "ex:p2"}},
                    {"@id": "ex:p2", "ex:knows": {"@id": "ex:p3"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 2: Introduce a NEW subject ex:new only in novelty, linking it
            // into the indexed chain: ex:new -> p1. Reachable from ex:new via
            // knows+ = {p1, p2, p3} = 3.
            let extend = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:new", "ex:knows": {"@id": "ex:p1"}}
                ]
            });
            let ledger2 = fluree
                .insert(ledger1, &extend)
                .await
                .expect("insert new subject in novelty")
                .ledger;

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ex:new ex:knows+ ?o . }
            ";

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "overlay-only start subject must not undercount (expected 3 reachable)"
            );
        })
        .await;
}

/// Regression: GROUP BY + COUNT top-k should reflect novelty deltas even when
/// the binary index is present and overlay introduces retractions/assertions.
#[tokio::test]
async fn indexed_overlay_group_by_count_topk_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-group-count-topk:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed and index baseline policyState distribution:
            // CA = 3, WA = 2
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:policyState": "CA"},
                    {"@id": "ex:b", "ex:policyState": "CA"},
                    {"@id": "ex:c", "ex:policyState": "CA"},
                    {"@id": "ex:d", "ex:policyState": "WA"},
                    {"@id": "ex:e", "ex:policyState": "WA"}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?o (COUNT(?s) AS ?cnt)
                WHERE { ?s ex:policyState ?o . }
                GROUP BY ?o
                ORDER BY DESC(?cnt)
                LIMIT 2
            ";

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load view at t=1");
            let result = fluree
                .query(&view1, QueryInput::Sparql(query))
                .await
                .expect("group count at t=1");
            let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["CA", 3], ["WA", 2]])),
                "baseline group counts should be CA=3, WA=2"
            );

            // Phase 2: Overlay changes:
            // - Move ex:e from WA -> CA (retract WA, assert CA)
            // - Insert a new subject ex:f with CA (assert-only)
            // Final: CA = 4, WA = 1
            let overlay_tx = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:e", "ex:policyState": "WA"}
                ],
                "insert": [
                    {"@id": "ex:e", "ex:policyState": "CA"},
                    {"@id": "ex:f", "ex:policyState": "CA"}
                ]
            });
            let ledger2 = fluree
                .update(ledger1, &overlay_tx)
                .await
                .expect("overlay update")
                .ledger;

            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load view at t=2");
            let result = fluree
                .query(&view2, QueryInput::Sparql(query))
                .await
                .expect("group count at t=2");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["CA", 5], ["WA", 1]])),
                "group counts should reflect overlay deltas"
            );
        })
        .await;
}

// =============================================================================
// Regression: novelty-only subject/string/ref equality after index
// =============================================================================
//
// These tests exercise `binary_range_eq_v3` when a match component (subject,
// string object, ref object) exists only in novelty — not in the persisted
// binary index. Before the fix, DictNovelty would resolve a novelty ID, the
// cursor filter would be set to that ID, but no persisted leaflet contained it,
// so the cursor produced zero batches and overlay ops were silently dropped.
//
// The correct behavior is: when the persisted store can't resolve a value,
// `overlay_only_flakes` must be used instead of the cursor path.
// =============================================================================

/// Regression: querying a novelty-only subject by IRI must return its data.
///
/// Exercises the SPOT path in `binary_range_eq_v3` where `store.sid_to_s_id()`
/// returns None and the code must fall through to `overlay_only_flakes`.
#[tokio::test]
async fn indexed_novelty_only_subject_returns_data() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/novelty-only-subject:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed baseline data and index it.
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice", "ex:age": 30}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            // Phase 2: Insert a NEW subject (novelty-only, not in index).
            let novelty_insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:bob", "ex:name": "Bob", "ex:age": 25}
                ]
            });
            let result = fluree
                .insert(ledger, &novelty_insert)
                .await
                .expect("novelty insert");
            let _ledger = result.ledger;

            // Phase 3: Query the novelty-only subject by IRI.
            let view = fluree.db(ledger_id).await.expect("load view");

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?p ?o
                WHERE { ex:bob ?p ?o . }
                ORDER BY ?p
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("query novelty-only subject");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");

            // ex:bob has ex:age, ex:name, and rdf:type — at least 2 explicit triples.
            assert!(
                rows.len() >= 2,
                "novelty-only subject ex:bob should return data; got: {jsonld:?}"
            );

            // Verify the indexed subject (ex:alice) still works too.
            let query_alice = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?p ?o
                WHERE { ex:alice ?p ?o . }
                ORDER BY ?p
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(query_alice))
                .await
                .expect("query indexed subject");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert!(
                rows.len() >= 2,
                "indexed subject ex:alice should still return data; got: {jsonld:?}"
            );
        })
        .await;
}

/// Regression: filtering by a novelty-only string value must return matches.
///
/// Exercises the string-object path in `binary_range_eq_v3` where
/// `store.find_string_id()` returns None and the code must fall through to
/// `overlay_only_flakes`.
#[tokio::test]
async fn indexed_novelty_only_string_object_returns_data() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/novelty-only-string:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed baseline and index.
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice"},
                    {"@id": "ex:bob", "ex:name": "Bob"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            // Phase 2: Insert data with a NEW string value (novelty-only).
            let novelty_insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:charlie", "ex:name": "Charlie"}
                ]
            });
            let result = fluree
                .insert(ledger, &novelty_insert)
                .await
                .expect("novelty insert");
            let _ledger = result.ledger;

            // Phase 3: Query filtering on the novelty-only string value.
            let view = fluree.db(ledger_id).await.expect("load view");

            let query = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?s
                WHERE { ?s ex:name "Charlie" . }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("query novelty-only string value");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "novelty-only string 'Charlie' should match 1 subject; got: {jsonld:?}"
            );

            // Verify indexed string value still works.
            let query_indexed = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?s
                WHERE { ?s ex:name "Alice" . }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(query_indexed))
                .await
                .expect("query indexed string value");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "indexed string 'Alice' should still match; got: {jsonld:?}"
            );
        })
        .await;
}

/// Regression: SPARQL string functions must work on indexed strings and
/// novelty-overlay strings after the binary index is attached.
///
/// Equality on encoded strings already has dedicated indexed coverage. This
/// test specifically exercises function evaluation, which forces late decode /
/// materialization of `Binding::EncodedLit` values.
#[tokio::test]
async fn indexed_string_functions_work_for_indexed_and_overlay_strings() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-string-functions:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice Adams"},
                    {"@id": "ex:bob", "ex:name": "Bob Builder"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let overlay_insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:brian", "ex:name": "Brian Platz"}
                ]
            });
            let result = fluree
                .insert(ledger, &overlay_insert)
                .await
                .expect("overlay insert");
            let _ledger = result.ledger;

            let view = fluree.db(ledger_id).await.expect("load view");

            let indexed_contains = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(CONTAINS(?name, "Alice"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(indexed_contains))
                .await
                .expect("indexed CONTAINS query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Alice Adams"]]))
            );

            let indexed_strstarts = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(STRSTARTS(?name, "Ali"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(indexed_strstarts))
                .await
                .expect("indexed STRSTARTS query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Alice Adams"]]))
            );

            let indexed_regex_prefix = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(REGEX(?name, "^Ali"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(indexed_regex_prefix))
                .await
                .expect("indexed regex-prefix query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Alice Adams"]]))
            );

            let overlay_equality = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(?name = "Brian Platz")
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_equality))
                .await
                .expect("overlay equality query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz"]]))
            );

            let overlay_contains = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(CONTAINS(?name, "Brian"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_contains))
                .await
                .expect("overlay CONTAINS query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz"]]))
            );

            let overlay_regex = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(REGEX(?name, "brian", "i"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_regex))
                .await
                .expect("overlay REGEX query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz"]]))
            );

            let overlay_regex_prefix = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(REGEX(?name, "^Brian"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_regex_prefix))
                .await
                .expect("overlay REGEX prefix query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz"]]))
            );

            let overlay_strstarts = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name
                WHERE {
                  ?s ex:name ?name .
                  FILTER(STRSTARTS(?name, "Brian"))
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_strstarts))
                .await
                .expect("overlay STRSTARTS query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz"]]))
            );

            let overlay_strlen = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?name (STRLEN(?name) AS ?len)
                WHERE {
                  ?s ex:name ?name .
                  FILTER(?name = "Brian Platz")
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_strlen))
                .await
                .expect("overlay STRLEN query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([["Brian Platz", 11]]))
            );

            let overlay_lcase = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT (LCASE(?name) AS ?lower)
                WHERE {
                  ?s ex:name ?name .
                  FILTER(?name = "Brian Platz")
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(overlay_lcase))
                .await
                .expect("overlay LCASE query");
            let sparql_json = result
                .to_sparql_json(&view.snapshot)
                .expect("to_sparql_json");
            let bindings = normalize_sparql_bindings(&sparql_json);
            assert_eq!(bindings.len(), 1, "LCASE should bind one row");
            assert_eq!(bindings[0]["lower"]["value"], json!("brian platz"));
        })
        .await;
}

#[tokio::test]
async fn indexed_count_with_lang_filter_counts_matching_lang_tag_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-count-lang-filter:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:label": {"@value": "Alpha", "@language": "en"}},
                    {"@id": "ex:b", "ex:label": {"@value": "Bravo", "@language": "en"}},
                    {"@id": "ex:c", "ex:label": {"@value": "Bonjour", "@language": "fr"}},
                    {"@id": "ex:d", "ex:label": "Plain"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let view = fluree
                .db_at_t(ledger_id, ledger.t())
                .await
                .expect("load indexed-only view");
            let query = r#"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(?s) AS ?count)
                WHERE {
                  ?s ex:label ?o .
                  FILTER(LANG(?o) = "en")
                }
            "#;
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("indexed LANG count query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "indexed COUNT with LANG filter should count only en-tagged literals"
            );
        })
        .await;
}

#[tokio::test]
async fn indexed_numeric_sum_fast_paths_work_for_identity_and_add_self() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-numeric-sum-fast-paths:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:n": 1},
                    {"@id": "ex:b", "ex:n": 2},
                    {"@id": "ex:c", "ex:n": 3}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let view = fluree
                .db_at_t(ledger_id, ledger.t())
                .await
                .expect("load indexed-only view");

            let baseline_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (SUM(?o) AS ?sum)
                WHERE { ?s ex:n ?o }
            ";
            let baseline_result = fluree
                .query(&view, QueryInput::Sparql(baseline_query))
                .await
                .expect("indexed SUM(?o) query");
            let baseline_json = baseline_result
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&baseline_json),
                normalize_rows(&json!([[6]])),
                "indexed SUM(?o) should add integer values directly"
            );

            let add_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (SUM(?o + ?o) AS ?sum)
                WHERE { ?s ex:n ?o }
            ";
            let add_result = fluree
                .query(&view, QueryInput::Sparql(add_query))
                .await
                .expect("indexed SUM(?o + ?o) query");
            let add_json = add_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&add_json),
                normalize_rows(&json!([[12]])),
                "indexed SUM(?o + ?o) should double each integer value"
            );
        })
        .await;
}

#[tokio::test]
async fn indexed_numeric_count_fast_path_handles_threshold_filters() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-numeric-count-threshold:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:n": 1},
                    {"@id": "ex:b", "ex:n": 2},
                    {"@id": "ex:c", "ex:n": 3},
                    {"@id": "ex:d", "ex:n": 3}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let view = fluree
                .db_at_t(ledger_id, ledger.t())
                .await
                .expect("load indexed-only view");

            let ge_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(?s) AS ?count)
                WHERE {
                  ?s ex:n ?o .
                  FILTER (?o >= 2)
                }
            ";
            let ge_result = fluree
                .query(&view, QueryInput::Sparql(ge_query))
                .await
                .expect("indexed COUNT(?s) with >= query");
            let ge_json = ge_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&ge_json),
                normalize_rows(&json!([[3]])),
                "indexed COUNT with ?o >= 2 should count qualifying rows"
            );

            let gt_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(?s) AS ?count)
                WHERE {
                  ?s ex:n ?o .
                  FILTER (?o > 2)
                }
            ";
            let gt_result = fluree
                .query(&view, QueryInput::Sparql(gt_query))
                .await
                .expect("indexed COUNT(?s) with > query");
            let gt_json = gt_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&gt_json),
                normalize_rows(&json!([[2]])),
                "indexed COUNT with ?o > 2 should honor exclusive thresholds"
            );
        })
        .await;
}

#[tokio::test]
async fn indexed_numeric_avg_min_max_fast_paths_work() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-numeric-avg-min-max:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:n": 1},
                    {"@id": "ex:b", "ex:n": 2},
                    {"@id": "ex:c", "ex:n": 3},
                    {"@id": "ex:d", "ex:n": 4}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let view = fluree
                .db_at_t(ledger_id, ledger.t())
                .await
                .expect("load indexed-only view");

            let avg_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (AVG(?o) AS ?avg)
                WHERE { ?s ex:n ?o }
            ";
            let avg_result = fluree
                .query(&view, QueryInput::Sparql(avg_query))
                .await
                .expect("indexed AVG(?o) query");
            let avg_json = avg_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&avg_json),
                normalize_rows(&json!([[2.5]])),
                "indexed AVG(?o) should average numeric values directly"
            );

            let min_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (MIN(?o) AS ?min)
                WHERE { ?s ex:n ?o }
            ";
            let min_result = fluree
                .query(&view, QueryInput::Sparql(min_query))
                .await
                .expect("indexed MIN(?o) query");
            let min_json = min_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&min_json),
                normalize_rows(&json!([[1]])),
                "indexed MIN(?o) should use numeric leaflet boundaries"
            );

            let max_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (MAX(?o) AS ?max)
                WHERE { ?s ex:n ?o }
            ";
            let max_result = fluree
                .query(&view, QueryInput::Sparql(max_query))
                .await
                .expect("indexed MAX(?o) query");
            let max_json = max_result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&max_json),
                normalize_rows(&json!([[4]])),
                "indexed MAX(?o) should use numeric leaflet boundaries"
            );
        })
        .await;
}

#[tokio::test]
async fn indexed_strstarts_sum_counts_prefix_matches() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-strstarts-sum:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice Adams"},
                    {"@id": "ex:ann", "ex:name": "Ann Arbor"},
                    {"@id": "ex:bob", "ex:name": "Bob Builder"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            let indexed = fluree
                .db_at_t(ledger_id, ledger.t())
                .await
                .expect("load indexed-only view");
            let query = r#"
                PREFIX ex: <http://example.org/ns/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT (SUM(xsd:integer(STRSTARTS(?name, "A"))) AS ?count)
                WHERE {
                  ?s ex:name ?name .
                }
            "#;
            let result = fluree
                .query(&indexed, QueryInput::Sparql(query))
                .await
                .expect("indexed STRSTARTS SUM query");
            let jsonld = result.to_jsonld(&indexed.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "indexed SUM(xsd:integer(STRSTARTS(...))) should count matching rows"
            );
        })
        .await;
}

/// Regression: filtering by a novelty-only ref object must return matches.
///
/// Exercises the ref-object path in `binary_range_eq_v3` where
/// `store.sid_to_s_id()` for the object returns None and the code must fall
/// through to `overlay_only_flakes`.
#[tokio::test]
async fn indexed_novelty_only_ref_object_returns_data() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/novelty-only-ref:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            // Phase 1: Seed baseline and index.
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
                    {"@id": "ex:bob", "ex:name": "Bob"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert");
            let ledger = result.ledger;

            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1);
            }

            // Phase 2: Insert a reference to a NEW subject (novelty-only ref target).
            let novelty_insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:charlie", "ex:knows": {"@id": "ex:diana"}},
                    {"@id": "ex:diana", "ex:name": "Diana"}
                ]
            });
            let result = fluree
                .insert(ledger, &novelty_insert)
                .await
                .expect("novelty insert");
            let _ledger = result.ledger;

            // Phase 3: Query filtering on the novelty-only ref object.
            let view = fluree.db(ledger_id).await.expect("load view");

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?s
                WHERE { ?s ex:knows ex:diana . }
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("query novelty-only ref object");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "novelty-only ref ex:diana should match 1 subject; got: {jsonld:?}"
            );

            // Verify indexed ref object still works.
            let query_indexed = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?s
                WHERE { ?s ex:knows ex:bob . }
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(query_indexed))
                .await
                .expect("query indexed ref object");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("array");
            assert_eq!(
                rows.len(),
                1,
                "indexed ref ex:bob should still match; got: {jsonld:?}"
            );
        })
        .await;
}

// =============================================================================
// IRI_REF and BLANK_NODE datatype resolution through binary index
// =============================================================================

/// Verify that IRI references and blank nodes resolve correctly through the
/// binary index path (via `resolve_datatype_sid` mapping IRI_REF/BLANK_NODE
/// to `jsonld:@id`).
///
/// Previously these OTypes returned `None` from `resolve_datatype_sid`, which
/// could cause callers to skip or use a fallback. This test ensures both
/// IRI-valued bindings (`skos:broader ?o` → IRI) and blank node subjects
/// (`isBlank(?s)`) work correctly through the indexed path.
#[tokio::test]
async fn indexed_iri_ref_and_blank_node_resolve_correctly() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/iri-bnode-resolve-idx:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

            // Insert data with both IRI references and blank nodes.
            // The blank node (no @id) will get a system-generated blank node ID.
            let insert = json!({
                "@context": {
                    "ex": "http://example.org/ns/"
                },
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "ex:name": "Alice",
                        "ex:knows": {"@id": "ex:bob"}
                    },
                    {
                        "@id": "ex:bob",
                        "ex:name": "Bob",
                        "ex:knows": {
                            "ex:name": "Anonymous"
                        }
                    }
                ]
            });

            let result = fluree
                .insert_with_opts(
                    ledger,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;

            // Trigger indexing
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            let view = fluree.db(ledger_id).await.expect("load view");

            // Test 1: IRI-valued bindings resolve correctly through binary index.
            // `ex:knows` points to IRIs (stored as OType::IRI_REF), including both
            // named IRIs (ex:bob) and blank nodes (the anonymous friend).
            let iri_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?s ?friend
                WHERE {
                    ?s ex:knows ?friend .
                }
                ORDER BY ?s
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(iri_query))
                .await
                .expect("IRI ref query should succeed through indexed path");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("should be array");
            assert_eq!(
                rows.len(),
                2,
                "should find 2 ex:knows bindings (alice->bob, bob->bnode); got: {jsonld:?}"
            );
            // Verify named IRI reference resolves correctly
            let alice_row = rows
                .iter()
                .find(|r| r[0].as_str() == Some("ex:alice"))
                .expect("should find alice row");
            assert_eq!(
                alice_row[1].as_str().unwrap(),
                "ex:bob",
                "IRI ref should resolve to ex:bob"
            );
            // Verify blank node reference also resolves (starts with _:)
            let bob_row = rows
                .iter()
                .find(|r| r[0].as_str() == Some("ex:bob"))
                .expect("should find bob row");
            assert!(
                bob_row[1].as_str().unwrap().starts_with("_:"),
                "blank node ref should start with _:, got: {}",
                bob_row[1]
            );

            // Test 2: Blank node subjects are queryable through binary index.
            // Bob's anonymous friend has a blank node ID.
            let bnode_query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?bnode ?name
                WHERE {
                    ?bnode ex:name ?name .
                    FILTER(isBlank(?bnode))
                }
            ";
            let result = fluree
                .query(&view, QueryInput::Sparql(bnode_query))
                .await
                .expect("blank node query should succeed through indexed path");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            let rows = jsonld.as_array().expect("should be array");
            assert_eq!(
                rows.len(),
                1,
                "should find 1 blank node subject with ex:name; got: {jsonld:?}"
            );
            assert_eq!(
                rows[0][1].as_str().unwrap(),
                "Anonymous",
                "blank node's name should be 'Anonymous'"
            );
        })
        .await;
}

/// POST overlay cursor coverage: `SUM(?o)`, `AVG(?o)`, and `COUNT(DISTINCT ?o)`
/// over a single predicate must reflect uncommitted novelty (overlay) — both
/// asserts and retracts — not only the persisted index.
///
/// The no-overlay baseline takes the leaflet-metadata scan; once novelty is
/// present the same `AggState` folds over a POST overlay cursor
/// (`build_post_cursor_for_predicate`). Numeric values exercise SUM/AVG; the
/// IRI-ref `ex:tag` predicate exercises adjacent-dedup COUNT(DISTINCT), including
/// a retract that drops an object's only edge (distinct count must shrink).
#[tokio::test]
async fn indexed_overlay_scalar_agg_reflects_assert_and_retract() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-scalar-agg:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };

            let sum_q = r"PREFIX ex: <http://example.org/ns/>
                SELECT (SUM(?o) AS ?v) WHERE { ?s ex:n ?o }";
            let avg_q = r"PREFIX ex: <http://example.org/ns/>
                SELECT (AVG(?o) AS ?v) WHERE { ?s ex:n ?o }";
            let cd_q = r"PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(DISTINCT ?o) AS ?v) WHERE { ?s ex:tag ?o }";

            // Phase 1: seed + index. n: a=10, b=20, c=30; tag: a→X, b→Y, c→X.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:n": 10, "ex:tag": {"@id": "ex:X"}},
                    {"@id": "ex:b", "ex:n": 20, "ex:tag": {"@id": "ex:Y"}},
                    {"@id": "ex:c", "ex:n": 30, "ex:tag": {"@id": "ex:X"}}
                ]
            });
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &baseline,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("baseline insert")
                .ledger;
            let outcome = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            if let fluree_db_api::IndexOutcome::Completed { index_t, .. } = outcome {
                assert_eq!(index_t, 1, "should index to t=1");
            }

            // Phase 1 — baseline (no overlay → leaflet-metadata lane):
            // SUM=60, AVG=20, distinct={X,Y}=2.
            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("view t=1");
            for (q, expected) in [
                (sum_q, json!([[60]])),
                (avg_q, json!([[20.0]])),
                (cd_q, json!([[2]])),
            ] {
                let result = fluree
                    .query(&view1, QueryInput::Sparql(q))
                    .await
                    .expect("baseline query");
                let jsonld = result.to_jsonld(&view1.snapshot).expect("to_jsonld");
                assert_eq!(
                    normalize_rows(&jsonld),
                    normalize_rows(&expected),
                    "baseline {q}"
                );
            }

            // Phase 2 — assert d (n=40, tag→Z) in novelty (overlay → POST cursor lane):
            // SUM=100, AVG=25, distinct={X,Y,Z}=3.
            let extend = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:d", "ex:n": 40, "ex:tag": {"@id": "ex:Z"}} ]
            });
            let ledger2 = fluree
                .insert(ledger1, &extend)
                .await
                .expect("overlay assert")
                .ledger;
            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("view t=2");
            for (q, expected) in [
                (sum_q, json!([[100]])),
                (avg_q, json!([[25.0]])),
                (cd_q, json!([[3]])),
            ] {
                let result = fluree
                    .query(&view2, QueryInput::Sparql(q))
                    .await
                    .expect("post-assert query");
                let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
                assert_eq!(
                    normalize_rows(&jsonld),
                    normalize_rows(&expected),
                    "after assert {q}"
                );
            }

            // Phase 3 — retract a's n=10 and b's only tag edge (→Y) in novelty:
            // SUM=20+30+40=90, AVG=30, distinct={X,Z}=2 (Y dropped).
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [
                    {"@id": "ex:a", "ex:n": 10},
                    {"@id": "ex:b", "ex:tag": {"@id": "ex:Y"}}
                ]
            });
            let ledger3 = fluree
                .update(ledger2, &retract)
                .await
                .expect("overlay retract")
                .ledger;
            let view3 = fluree
                .db_at_t(ledger_id, ledger3.t())
                .await
                .expect("view t=3");
            for (q, expected) in [
                (sum_q, json!([[90]])),
                (avg_q, json!([[30.0]])),
                (cd_q, json!([[2]])),
            ] {
                let result = fluree
                    .query(&view3, QueryInput::Sparql(q))
                    .await
                    .expect("post-retract query");
                let jsonld = result.to_jsonld(&view3.snapshot).expect("to_jsonld");
                assert_eq!(
                    normalize_rows(&jsonld),
                    normalize_rows(&expected),
                    "after retract {q}"
                );
            }
        })
        .await;
}
