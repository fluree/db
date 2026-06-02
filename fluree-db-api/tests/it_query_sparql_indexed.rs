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
// SUM(?o <cmp> K) -> COUNT bridge (numeric-compare fast path)
// =============================================================================

/// `SUM(?o > K)` is `COUNT` of the rows where the comparison holds. On an indexed
/// homogeneous numeric predicate it must route to the directory-skipping
/// numeric-compare count and return the exact matching-row count.
///
/// favNums = {3,7,42,99} ∪ {23} ∪ {8,6,7,5,3,0,9}; values > 10 are 42, 99, 23 => 3.
#[tokio::test]
async fn indexed_sum_compare_as_count_matches_value() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-sum-compare:main";

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
                "@context": { "person": "http://example.org/Person#" },
                "@graph": [
                    {"@id": "person:jbob", "person:favNums": [3, 7, 42, 99]},
                    {"@id": "person:jdoe", "person:favNums": [23]},
                    {"@id": "person:bbob", "person:favNums": [8, 6, 7, 5, 3, 0, 9]}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX person: <http://example.org/Person#>
                SELECT (SUM(?favNums > 10) AS ?count)
                WHERE { ?person person:favNums ?favNums . }
            ";
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("SUM(compare) query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "SUM(?o>10) over an indexed predicate must equal the count of matching rows"
            );

            // Parity with the equivalent COUNT(FILTER) form on the same indexed data.
            let q_count = r"
                PREFIX person: <http://example.org/Person#>
                SELECT (COUNT(?favNums) AS ?count)
                WHERE { ?person person:favNums ?favNums . FILTER(?favNums > 10) }
            ";
            let r_count = fluree
                .query(&view, QueryInput::Sparql(q_count))
                .await
                .expect("COUNT(FILTER) query should succeed");
            let jsonld_count = r_count.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&jsonld_count),
                "SUM(?o>10) must equal COUNT(?o){{FILTER(?o>10)}}"
            );
        })
        .await;
}

/// `SUM` over an **empty** input is Unbound (not 0). For an absent predicate the
/// fast path must defer to the general pipeline. We assert the indexed result
/// matches the memory (general-pipeline) result so we don't depend on the exact
/// Unbound serialization — only that the fast path doesn't substitute `0`.
#[tokio::test]
async fn indexed_sum_compare_empty_predicate_matches_general() {
    assert_index_defaults();

    let seed = || {
        json!({
            "@context": { "person": "http://example.org/Person#" },
            "@graph": [
                {"@id": "person:jbob", "person:favNums": [3, 7, 42, 99]}
            ]
        })
    };
    // `person:absent` is never asserted, so the WHERE matches zero rows and SUM is Unbound.
    let q = r"
        PREFIX person: <http://example.org/Person#>
        SELECT (SUM(?v > 0) AS ?count)
        WHERE { ?person person:absent ?v . }
    ";

    // Memory reference (always the general pipeline).
    let mem = FlureeBuilder::memory().build_memory();
    let mem_ledger = {
        let l0 = genesis_ledger_for_fluree(&mem, "it/sum-empty-mem:main");
        mem.insert_with_opts(
            l0,
            &seed(),
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            },
        )
        .await
        .expect("mem seed")
        .ledger
    };
    let mem_view = mem
        .db_at_t("it/sum-empty-mem:main", mem_ledger.t())
        .await
        .expect("mem view");
    let mem_rows = normalize_rows(
        &mem.query(&mem_view, QueryInput::Sparql(q))
            .await
            .expect("mem query")
            .to_jsonld(&mem_view.snapshot)
            .expect("to_jsonld"),
    );

    // Indexed path: the SUM-compare detector fires, the operator sees an absent
    // predicate, and must defer to the same general result.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-sum-empty:main";
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
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &seed(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");
            let idx_rows = normalize_rows(
                &fluree
                    .query(&view, QueryInput::Sparql(q))
                    .await
                    .expect("indexed query")
                    .to_jsonld(&view.snapshot)
                    .expect("to_jsonld"),
            );
            assert_eq!(
                idx_rows, mem_rows,
                "SUM over an absent predicate must yield the general Unbound result, not 0"
            );
        })
        .await;
}

/// GATE check: on a regular (non-bulk) index, `lex_sorted_string_ids` is false, so
/// the rdf:type-star metadata fold is disabled and the join uses the MERGE — which
/// reads current-state leaflets, not the (incrementally-stale) class-property
/// stats. ?s rdf:type ?o1 . ?s ex:p ?o2: initially 2*3 + 1 = 7; after retracting
/// one of ex:a's p values and re-indexing, 2*2 + 1 = 5. (The class-property fold
/// would wrongly stay 7 here — see bulk_import_rdf_type_star_count_from_class_stats
/// for the fold path, which only fires on bulk imports where the stats are exact.)
#[tokio::test]
async fn indexed_rdf_type_star_count_uses_merge_not_stale_stats() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-typestar:main";
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
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [
                            {"@id": "ex:a", "@type": ["ex:C1", "ex:C2"], "ex:p": [1, 2, 3]},
                            {"@id": "ex:b", "@type": "ex:C1", "ex:p": [4]},
                            {"@id": "ex:c", "@type": "ex:C2"},
                            {"@id": "ex:d", "ex:p": [5]}
                        ]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT (COUNT(*) AS ?count) WHERE { ?s rdf:type ?o1 . ?s ex:p ?o2 }
            ";
            let view1 = fluree.db_at_t(ledger_id, ledger1.t()).await.expect("view1");
            assert_eq!(
                normalize_rows(
                    &fluree
                        .query(&view1, QueryInput::Sparql(q))
                        .await
                        .expect("typestar q1")
                        .to_jsonld(&view1.snapshot)
                        .expect("to_jsonld")
                ),
                normalize_rows(&json!([[7]])),
                "Σ_C count(C, ex:p) = C1:(3+1) + C2:(3) = 7"
            );

            // Retract one of ex:a's p values, re-index: 2*2 + 1 = 5.
            let ledger2 = fluree
                .update(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [{"@id": "ex:a", "ex:p": 1}]
                    }),
                )
                .await
                .expect("delete")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger2.t()).await;
            let view2 = fluree.db_at_t(ledger_id, ledger2.t()).await.expect("view2");
            assert_eq!(
                normalize_rows(
                    &fluree
                        .query(&view2, QueryInput::Sparql(q))
                        .await
                        .expect("typestar q2")
                        .to_jsonld(&view2.snapshot)
                        .expect("to_jsonld")
                ),
                normalize_rows(&json!([[5]])),
                "after retracting ex:a ex:p 1: C1:(2+1) + C2:(2) = 5 (current-state, not history)"
            );
        })
        .await;
}

/// `number-of-predicates` (`COUNT(DISTINCT ?p) WHERE { ?s ?p ?o }`) served from
/// the per-graph index stats (count of properties with positive current count),
/// no leaf scan. Asserts the indexed (stats) result equals the memory
/// (general-pipeline) result — so we don't have to know Fluree's internal
/// predicate count — and that it survives a retraction of a whole predicate.
#[tokio::test]
async fn indexed_number_of_predicates_from_stats_matches_general() {
    assert_index_defaults();

    let seed = || {
        json!({
            "@context": { "ex": "http://example.org/ns/" },
            "@graph": [
                {"@id": "ex:a", "ex:p0": 1, "ex:p1": 2, "ex:p2": 3},
                {"@id": "ex:b", "ex:p1": 4, "ex:p3": 5},
                {"@id": "ex:c", "ex:p4": 6, "ex:p0": 7}
            ]
        })
    };
    let q = r"SELECT (COUNT(DISTINCT ?p) AS ?count) WHERE { ?s ?p ?o }";

    // Memory reference (general pipeline).
    let mem = FlureeBuilder::memory().build_memory();
    let mem_ledger = mem
        .insert_with_opts(
            genesis_ledger_for_fluree(&mem, "it/nop-mem:main"),
            &seed(),
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            },
        )
        .await
        .expect("mem seed")
        .ledger;
    let mem_view = mem
        .db_at_t("it/nop-mem:main", mem_ledger.t())
        .await
        .expect("mem view");
    let mem_rows = normalize_rows(
        &mem.query(&mem_view, QueryInput::Sparql(q))
            .await
            .expect("mem query")
            .to_jsonld(&mem_view.snapshot)
            .expect("to_jsonld"),
    );

    // Indexed path (stats).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/nop-indexed:main";
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
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &seed(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("indexed seed")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("indexed view");
            let idx_rows = normalize_rows(
                &fluree
                    .query(&view, QueryInput::Sparql(q))
                    .await
                    .expect("indexed query")
                    .to_jsonld(&view.snapshot)
                    .expect("to_jsonld"),
            );
            assert_eq!(
                idx_rows, mem_rows,
                "number-of-predicates from stats must equal the general-pipeline count"
            );
        })
        .await;
}

/// Parallel partitioned inner-star COUNT(*): seed enough rows (>50k, the parallel
/// threshold) with VARYING per-subject multiplicity so the count is sensitive to
/// any partition-boundary bug (a misattributed subject changes the product sum).
/// The result must equal the serially-computed expected sum exactly.
#[tokio::test]
async fn indexed_parallel_star_join_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-star:main";

    // Tiny leaflets/leaves so the modest test data spans many leaves (the real
    // benchmark predicate has thousands) and the parallel partitioner engages.
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // subject i: p1 has (i%3)+1 values, p2 has (i%2)+1 values.
            // total rows ~ 3.5*N; N=16000 => ~56k rows > 50k parallel threshold.
            let n: i64 = 16_000;
            let mut expected: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                let c1 = (i % 3) + 1;
                let c2 = (i % 2) + 1;
                expected += c1 * c2;
                let p1: Vec<serde_json::Value> = (0..c1).map(|j| json!(i * 100 + j)).collect();
                let p2: Vec<serde_json::Value> =
                    (0..c2).map(|j| json!(i * 100 + 50 + j)).collect();
                nodes.push(json!({"@id": format!("ex:s{i}"), "ex:p1": p1, "ex:p2": p2}));
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o1 . ?s ex:p2 ?o2 }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel star join query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "parallel partitioned star-join count must equal Σ_s c1(s)*c2(s) = {expected}"
            );
        })
        .await;
}

/// Parallel partitioned OPTIONAL COUNT(*): `?s p1 ?o1 OPTIONAL { ?s p2 ?o2 }` =
/// Σ_s count_p1(s)·max(1, count_p2(s)). Seeds >50k rows (the parallel threshold)
/// with varying optional multiplicity (incl. subjects with NO p2, factor 1) so the
/// count is sensitive to any partition-boundary or optional-multiplier bug; the
/// result must equal the serially-computed expected sum exactly.
#[tokio::test]
async fn indexed_parallel_optional_join_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-optional:main";
    // Tiny leaflets so the data spans many leaves and the partitioner engages.
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // subject i: required p1 has 2 values; optional p2 has (i%3) values
            // (0 => no p2 => multiplier 1). total rows ~ 2N + Σ(i%3) ~ 3N;
            // N=20000 => ~60k > 50k threshold.
            let n: i64 = 20_000;
            let mut expected: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                let c2 = i % 3; // 0, 1, or 2 optional values
                expected += 2 * c2.max(1);
                let mut node = json!({"@id": format!("ex:s{i}"), "ex:p1": [i * 10, i * 10 + 1]});
                if c2 > 0 {
                    let p2: Vec<serde_json::Value> =
                        (0..c2).map(|j| json!(i * 10 + 5 + j)).collect();
                    node["ex:p2"] = json!(p2);
                }
                nodes.push(node);
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o1 OPTIONAL { ?s ex:p2 ?o2 } }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel optional join query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "parallel optional count must equal Σ_s count_p1(s)*max(1,count_p2(s)) = {expected}"
            );
        })
        .await;
}

/// Parallel partitioned constrained-UNION COUNT(*):
///   { ?s p1 ?o } UNION { ?s p2 ?o } . ?s e ?o2
///   = Σ_s (count_p1(s)+count_p2(s))·count_e(s) for s with e AND (p1 OR p2).
/// Seeds >50k rows with varying multiplicity plus union-only and constraint-only
/// subjects (which must contribute 0), so the result is sensitive to both the
/// union OR-sum, the constraint AND-join, and partition boundaries.
#[tokio::test]
async fn indexed_parallel_union_constraint_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-union-constraint:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // subject i: p1 = (i%2)+1 values, e = 1 value, p2 = (i%3) values.
            // contributes (count_p1 + count_p2) * count_e = ((i%2+1)+(i%3))*1.
            let n: i64 = 16_000;
            let mut expected: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::new();
            for i in 0..n {
                let c1 = (i % 2) + 1;
                let cp2 = i % 3;
                expected += c1 + cp2;
                let mut node = json!({"@id": format!("ex:s{i}"), "ex:e": 1});
                node["ex:p1"] = json!((0..c1).map(|j| json!(i * 10 + j)).collect::<Vec<_>>());
                if cp2 > 0 {
                    node["ex:p2"] =
                        json!((0..cp2).map(|j| json!(i * 10 + 5 + j)).collect::<Vec<_>>());
                }
                nodes.push(node);
            }
            // union-only (no constraint e) and constraint-only (no union) => contribute 0.
            for i in 0..500 {
                nodes.push(json!({"@id": format!("ex:u{i}"), "ex:p1": [i]}));
                nodes.push(json!({"@id": format!("ex:c{i}"), "ex:e": 2}));
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { { ?s ex:p1 ?o } UNION { ?s ex:p2 ?o } ?s ex:e ?o2 }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel union-constraint query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "parallel union-constraint count must equal Σ_s (c_p1+c_p2)·c_e = {expected}"
            );
        })
        .await;
}

/// Parallel partitioned MINUS over a multi-predicate inner block (the "3-star"
/// shape): `?s outer ?o1 MINUS { ?s in1 ?a . ?s in2 ?b }`
///   = Σ_s count_outer(s) for s with outer AND s ∉ (in1 ∩ in2).
/// The inner block compiles to `IntersectSorted([in1, in2])`, so the asymmetric
/// seek bails and `try_modifier_intersect_parallel` drives the keyset build +
/// outer scan in parallel. >50k total rows with varying outer multiplicity and an
/// inner intersection (`i%6==0`) that is a strict subset of the outer subjects, so
/// the result is sensitive to both the intersection and the partition boundaries.
#[tokio::test]
async fn indexed_parallel_minus_intersect_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-minus-intersect:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // subject i: outer = (i%2)+1 values; in1 present iff i%2==0; in2 iff
            // i%3==0. inner intersection (in1 AND in2) = i%6==0. total rows ~ 1.5N
            // + 0.5N + 0.33N; N=24000 => ~56k > 50k threshold.
            let n: i64 = 24_000;
            let mut expected_minus: i64 = 0;
            let mut expected_exists: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                let c_outer = (i % 2) + 1;
                let in1 = i % 2 == 0;
                let in2 = i % 3 == 0;
                if in1 && in2 {
                    expected_exists += c_outer;
                } else {
                    expected_minus += c_outer;
                }
                let mut node = json!({"@id": format!("ex:s{i}")});
                node["ex:outer"] =
                    json!((0..c_outer).map(|j| json!(i * 10 + j)).collect::<Vec<_>>());
                if in1 {
                    node["ex:in1"] = json!(i * 10 + 100);
                }
                if in2 {
                    node["ex:in2"] = json!(i * 10 + 200);
                }
                nodes.push(node);
            }
            let _ = expected_exists;
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:outer ?o1 MINUS { ?s ex:in1 ?a . ?s ex:in2 ?b } }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel minus-intersect query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected_minus]])),
                "parallel minus-intersect count must equal Σ_s∉(in1∩in2) count_outer(s) = {expected_minus}"
            );
        })
        .await;
}

/// Parallel partitioned FILTER EXISTS over a multi-predicate inner block (the
/// "3-star" shape): `?s outer ?o1 FILTER EXISTS { ?s in1 ?a . ?s in2 ?b }`
///   = Σ_s count_outer(s) for s with outer AND s ∈ (in1 ∩ in2).
/// Same data as the MINUS test; EXISTS is its complement over the outer subjects.
#[tokio::test]
async fn indexed_parallel_exists_intersect_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-exists-intersect:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            let n: i64 = 24_000;
            let mut expected_exists: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                let c_outer = (i % 2) + 1;
                let in1 = i % 2 == 0;
                let in2 = i % 3 == 0;
                if in1 && in2 {
                    expected_exists += c_outer;
                }
                let mut node = json!({"@id": format!("ex:s{i}")});
                node["ex:outer"] =
                    json!((0..c_outer).map(|j| json!(i * 10 + j)).collect::<Vec<_>>());
                if in1 {
                    node["ex:in1"] = json!(i * 10 + 100);
                }
                if in2 {
                    node["ex:in2"] = json!(i * 10 + 200);
                }
                nodes.push(node);
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:outer ?o1 FILTER EXISTS { ?s ex:in1 ?a . ?s ex:in2 ?b } }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel exists-intersect query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected_exists]])),
                "parallel exists-intersect count must equal Σ_s∈(in1∩in2) count_outer(s) = {expected_exists}"
            );
        })
        .await;
}

/// Parallel encoded-filter row count: `COUNT(?s) { ?s rdf:type ?o FILTER(?s != ?o) }`.
/// The `?s != ?o` filter compiles to a `SubjectNeObjectRef` encoded pre-filter, so
/// at HEAD the rows are counted in parallel across leaf chunks (no binding
/// materialization). Seeds >50k typed subjects (s != o) plus a few self-typed nodes
/// (`ex:self_k a ex:self_k`, s == o) which the filter must exclude.
#[tokio::test]
async fn indexed_parallel_encoded_filter_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-encoded-filter:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // N normal typed subjects (s != o) + M self-typed (s == o, excluded).
            let n: i64 = 60_000;
            let m: i64 = 50;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity((n + m) as usize);
            for i in 0..n {
                nodes.push(json!({"@id": format!("ex:s{i}"), "@type": format!("ex:C{}", i % 50)}));
            }
            for j in 0..m {
                // A node typed as itself => (s == o) row, excluded by FILTER(?s != ?o).
                nodes.push(json!({"@id": format!("ex:self{j}"), "@type": format!("ex:self{j}")}));
            }
            let expected = n; // self-typed rows excluded
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT (COUNT(?s) AS ?count)
                WHERE { ?s rdf:type ?o . FILTER (?s != ?o) }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel encoded-filter query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "parallel encoded-filter count must exclude (s == o) rows = {expected}"
            );
        })
        .await;
}

/// Parallel numeric-compare count: `SUM(?o > 0) { ?s ex:num ?o }` over >50k integer
/// rows. The POST leaf slice is split across cores; per chunk, fully-matching /
/// fully-excluded leaflets short-circuit and boundary leaflets binary-search.
#[tokio::test]
async fn indexed_parallel_numeric_compare_count_matches_serial() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-parallel-numeric-compare:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    idx_config.leaf_target_bytes = 8_000;
    idx_config.leaf_max_bytes = 16_000;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // value = (i%5) - 2 in {-2,-1,0,1,2}; > 0 iff (i%5) in {3,4} => 2/5.
            let n: i64 = 60_000;
            let mut expected: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                let v = (i % 5) - 2;
                if v > 0 {
                    expected += 1;
                }
                nodes.push(json!({"@id": format!("ex:s{i}"), "ex:num": v}));
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (SUM(?o > 0) AS ?sum)
                WHERE { ?s ex:num ?o }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("parallel numeric-compare query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "parallel numeric-compare count must equal #rows with ?o > 0 = {expected}"
            );
        })
        .await;
}

/// Mixed XSD_INTEGER + XSD_DOUBLE objects under one predicate: the per-leaflet
/// threshold encoding must count each numeric otype against its own encoded
/// threshold (POST sorts by o_type, so int and double leaflets are disjoint). This
/// locks the behavior change from the prior "bail on mixed otype" path.
#[tokio::test]
async fn indexed_numeric_compare_mixed_int_double_counts_correctly() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-numeric-compare-mixed:main";
    let mut idx_config = fluree_db_indexer::IndexerConfig::small();
    idx_config.leaflet_rows = 100;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        idx_config,
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 80_000_000,
            };
            // Even i: integer (i%5)-2; odd i: double (i%5) as f64 - 1.5.
            let n: i64 = 600;
            let mut expected: i64 = 0;
            let mut nodes: Vec<serde_json::Value> = Vec::with_capacity(n as usize);
            for i in 0..n {
                if i % 2 == 0 {
                    let v = (i % 5) - 2; // {-2,-1,0,1,2}, >0 => {1,2}
                    if v > 0 {
                        expected += 1;
                    }
                    nodes.push(json!({"@id": format!("ex:s{i}"), "ex:num": v}));
                } else {
                    let v = (i % 5) as f64 - 1.5; // {-1.5,-0.5,0.5,1.5,2.5}, >0 => last three
                    if v > 0.0 {
                        expected += 1;
                    }
                    nodes.push(json!({"@id": format!("ex:s{i}"), "ex:num": v}));
                }
            }
            let ledger1 = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, ledger_id),
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (SUM(?o > 0) AS ?sum)
                WHERE { ?s ex:num ?o }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("mixed numeric-compare query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[expected]])),
                "mixed int/double compare count must equal #rows with ?o > 0 = {expected}"
            );
        })
        .await;
}

/// GROUP BY ?object COUNT(?subject) ORDER BY DESC LIMIT via the indexed POST
/// path (`group_count_v6`, run-length + top-K). A high-multiplicity object's run
/// (2000 rows) spans multiple leaflets, exercising the boundary-equality /
/// cross-leaflet run accumulation; LIMIT 3 must keep the three highest counts in
/// descending order.
#[tokio::test]
async fn indexed_group_by_object_count_topk_run_spanning() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-groupby-topk:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 50_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            // object 10 -> 2000 subjects, 20 -> 400, 30 -> 80, 40 -> 20.
            let mut nodes: Vec<serde_json::Value> = Vec::new();
            for (val, n) in [(10, 2000), (20, 400), (30, 80), (40, 20)] {
                for i in 0..n {
                    nodes.push(json!({"@id": format!("ex:s{val}_{i}"), "ex:p": val}));
                }
            }
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT ?o (COUNT(?s) AS ?c)
                WHERE { ?s ex:p ?o }
                GROUP BY ?o ORDER BY DESC(?c) LIMIT 3
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("group-by topk query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[10, 2000], [20, 400], [30, 80]])),
                "top-3 object groups by count desc; the 2000-row run spans leaflets"
            );
        })
        .await;
}

/// `count_rows_for_predicate_psot` reads interior-leaf counts from the branch
/// manifest (`LeafEntry.row_count`). That count must reflect **latest state** —
/// indexed retractions must be excluded, matching the per-leaflet directory sum.
/// Insert 2000 rows of one predicate (enough to span whole/interior leaves),
/// index, delete 800, re-index, then COUNT(*) at HEAD must be 1200.
#[tokio::test]
async fn indexed_predicate_count_excludes_indexed_retractions() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-retract-count:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 50_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let nodes: Vec<serde_json::Value> = (0..2000)
                .map(|i| json!({"@id": format!("ex:s{i}"), "ex:p": i}))
                .collect();
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "@graph": nodes }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            // Delete 2000 of the rows, then index so the retractions land in the base.
            let dels: Vec<serde_json::Value> = (0..800)
                .map(|i| json!({"@id": format!("ex:s{i}"), "ex:p": i}))
                .collect();
            let ledger2 = fluree
                .update(
                    ledger1,
                    &json!({ "@context": { "ex": "http://example.org/ns/" }, "delete": dels }),
                )
                .await
                .expect("delete")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger2.t()).await;

            let view = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("load indexed view");
            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count) WHERE { ?s ex:p ?o }
            ";
            let jsonld = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("count query")
                .to_jsonld(&view.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[1200]])),
                "manifest interior-leaf count must exclude indexed retractions (2000-800)"
            );
        })
        .await;
}

// =============================================================================
// Asymmetric (leapfrog) seek strategy for skewed inner-star count joins
// =============================================================================

/// `?s p1 ?o1 . ?s p2 ?o2` COUNT(*) where p1 is tiny and p2 is large enough to
/// trip the seek threshold (`driver_rows * SEEK_STAR_DRIVER_FACTOR < probe_rows`).
/// Validates that the seek strategy yields the same count as the merge: per-subject
/// multiplicity (ex:a => 2×3 = 6) and that a driver subject absent from the probe
/// (ex:c has p1 but not p2) contributes 0.
#[tokio::test]
async fn indexed_star_join_seek_strategy_counts_correctly() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-star-seek:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 50_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            // Driver p1 rows = 3 (ex:a:2, ex:c:1). Probe p2 rows = 3 + 24_600 filler
            // = 24_603 > 3 * 8192 = 24_576, so the seek strategy fires.
            let filler: Vec<serde_json::Value> =
                (0..24_600).map(|n| json!(n)).collect();
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [10, 20], "ex:p2": [100, 200, 300]},
                    {"@id": "ex:c", "ex:p1": [30]},
                    {"@id": "ex:filler", "ex:p2": filler}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { ?s ex:p1 ?o1 . ?s ex:p2 ?o2 . }
            ";
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("star join seek query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[6]])),
                "seek strategy: ex:a contributes 2×3=6; ex:c (no p2) contributes 0"
            );
        })
        .await;
}

/// EXISTS/MINUS asymmetric seek, all four branches from one skewed dataset:
///   ex:a: p1{1}, p2{100,200,300}   ex:b: p1{2}   ex:filler: p2[0..16400]
///   p1 rows = 2, p2 rows = 16_403 (skew triggers the seek both directions).
/// - `p1 EXISTS p2`  (drive A=p1, seek B=p2): only ex:a has p2 => count_p1(a)=1
/// - `p1 MINUS  p2`  (drive A=p1, seek B=p2): only ex:b lacks p2 => count_p1(b)=1
/// - `p2 EXISTS p1`  (drive B=p1, seek A=p2): matched = count_p2(a)=3
/// - `p2 MINUS  p1`  (drive B=p1, seek A=p2): total(p2)-matched = 16_403-3 = 16_400
#[tokio::test]
async fn indexed_modifier_seek_exists_minus_counts_correctly() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-modifier-seek:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 50_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let filler: Vec<serde_json::Value> = (0..16_400).map(|n| json!(n)).collect();
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [1], "ex:p2": [100, 200, 300]},
                    {"@id": "ex:b", "ex:p1": [2]},
                    {"@id": "ex:filler", "ex:p2": filler}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let cases = [
                (
                    "p1 EXISTS p2 (drive A)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p1 ?o1 FILTER EXISTS { ?s ex:p2 ?o2 } }",
                    1i64,
                ),
                (
                    "p1 MINUS p2 (drive A)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p1 ?o1 MINUS { ?s ex:p2 ?o2 } }",
                    1,
                ),
                (
                    "p2 EXISTS p1 (drive B)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p2 ?o1 FILTER EXISTS { ?s ex:p1 ?o2 } }",
                    3,
                ),
                (
                    "p2 MINUS p1 (drive B)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p2 ?o1 MINUS { ?s ex:p1 ?o2 } }",
                    16_400,
                ),
            ];

            for (label, q, expected) in cases {
                let r = fluree
                    .query(&view, QueryInput::Sparql(q))
                    .await
                    .unwrap_or_else(|e| panic!("{label} query failed: {e:?}"));
                let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
                assert_eq!(
                    normalize_rows(&jsonld),
                    normalize_rows(&json!([[expected]])),
                    "{label}"
                );
            }
        })
        .await;
}

/// OPTIONAL asymmetric seek, both drive directions from one skewed dataset:
///   ex:a: p1{1,5}, p2{100,200,300}   ex:b: p1{2}   ex:filler: p2[0..24600]
///   p1 rows = 3, p2 rows = 24_603.
/// - `p1 OPTIONAL p2` (drive A=p1): Σ count_p1(s)·max(1,count_p2(s))
///     = ex:a 2·3 + ex:b 1·1 = 7
/// - `p2 OPTIONAL p1` (drive B=p1): total(p2) + Σ count_p2(s)·(count_p1(s)-1)
///     = 24_603 + ex:a 3·(2-1) = 24_606
#[tokio::test]
async fn indexed_optional_seek_counts_correctly() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-optional-seek:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 50_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let filler: Vec<serde_json::Value> = (0..24_600).map(|n| json!(n)).collect();
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [1, 5], "ex:p2": [100, 200, 300]},
                    {"@id": "ex:b", "ex:p1": [2]},
                    {"@id": "ex:filler", "ex:p2": filler}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let cases = [
                (
                    "p1 OPTIONAL p2 (drive A)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p1 ?o1 OPTIONAL { ?s ex:p2 ?o2 } }",
                    7i64,
                ),
                (
                    "p2 OPTIONAL p1 (drive B)",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p2 ?o1 OPTIONAL { ?s ex:p1 ?o2 } }",
                    24_606,
                ),
            ];

            for (label, q, expected) in cases {
                let r = fluree
                    .query(&view, QueryInput::Sparql(q))
                    .await
                    .unwrap_or_else(|e| panic!("{label} query failed: {e:?}"));
                let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
                assert_eq!(
                    normalize_rows(&jsonld),
                    normalize_rows(&json!([[expected]])),
                    "{label}"
                );
            }
        })
        .await;
}

// =============================================================================
// UNION COUNT(*) pure-sum metadata collapse (AllRows, no extra constraint)
// =============================================================================

/// `{ ?s p1 ?o } UNION { ?s p2 ?o }` under COUNT(*) is `count(p1)+count(p2)` under
/// bag semantics. At HEAD (no overlay/time-travel) this is answered from leaflet
/// directory row counts. Subjects present under BOTH predicates must be counted
/// twice (bag semantics), so the answer is 6, not a distinct-subject count.
#[tokio::test]
async fn indexed_union_count_all_rows_metadata_lane() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-union-count:main";

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
            // p1 rows: a{1,2}, b{3} => 3.  p2 rows: a{1}, c{4,5} => 3.
            // a appears under both predicates and must be counted in each branch.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let insert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [1, 2], "ex:p2": [1]},
                    {"@id": "ex:b", "ex:p1": [3]},
                    {"@id": "ex:c", "ex:p2": [4, 5]}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;
            let view = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("load indexed view");

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { { ?s ex:p1 ?o } UNION { ?s ex:p2 ?o } }
            ";
            let r = fluree
                .query(&view, QueryInput::Sparql(q))
                .await
                .expect("union count query should succeed");
            let jsonld = r.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[6]])),
                "UNION COUNT(*) = count(p1)+count(p2) = 6 under bag semantics"
            );
        })
        .await;
}

/// The metadata collapse must be gated to HEAD: under overlay (novelty present)
/// the UNION count must still incorporate the overlay rows by falling through to
/// the cursor path. baseline = 6; after novelty adds one p1 row and one p2 row = 8.
#[tokio::test]
async fn indexed_overlay_union_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-union-count:main";

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
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [1, 2], "ex:p2": [1]},
                    {"@id": "ex:b", "ex:p1": [3]},
                    {"@id": "ex:c", "ex:p2": [4, 5]}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { { ?s ex:p1 ?o } UNION { ?s ex:p2 ?o } }
            ";

            let view1 = fluree
                .db_at_t(ledger_id, ledger1.t())
                .await
                .expect("view t=1");
            let jsonld1 = fluree
                .query(&view1, QueryInput::Sparql(q))
                .await
                .expect("count t=1")
                .to_jsonld(&view1.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld1),
                normalize_rows(&json!([[6]])),
                "indexed-only union count = 6"
            );

            // Novelty: add one p1 row (ex:d) and one p2 row (ex:b) => total 8.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:d", "ex:p1": [10]},
                    {"@id": "ex:b", "ex:p2": [11]}
                ]
            });
            let ledger2 = fluree
                .insert(ledger1, &assert)
                .await
                .expect("novelty insert")
                .ledger;
            let view2 = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("view t=2");
            let jsonld2 = fluree
                .query(&view2, QueryInput::Sparql(q))
                .await
                .expect("count t=2")
                .to_jsonld(&view2.snapshot)
                .expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld2),
                normalize_rows(&json!([[8]])),
                "union count must include the two overlay rows (cursor fall-through)"
            );
        })
        .await;
}

/// Time-travel gate: the metadata collapse reads the latest indexed directory, so
/// it must NOT fire when `to_t < max_t`. Index to t=2 (HEAD count = 2), then query
/// at to_t=1 — the result must be the historical count (1), not the current 2.
#[tokio::test]
async fn indexed_union_count_time_travel_uses_cursor_path() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/indexed-union-timetravel:main";

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
            let ledger1 = fluree
                .insert_with_opts(
                    ledger0,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [{"@id": "ex:a", "ex:p1": [1]}]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert t=1")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let ledger2 = fluree
                .insert_with_opts(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [{"@id": "ex:b", "ex:p1": [2]}]
                    }),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert t=2")
                .ledger;
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger2.t()).await;

            let q = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?count)
                WHERE { { ?s ex:p1 ?o } UNION { ?s ex:p2 ?o } }
            ";

            // HEAD: count = 2.
            let head = fluree
                .db_at_t(ledger_id, ledger2.t())
                .await
                .expect("view HEAD");
            assert_eq!(
                normalize_rows(
                    &fluree
                        .query(&head, QueryInput::Sparql(q))
                        .await
                        .expect("count HEAD")
                        .to_jsonld(&head.snapshot)
                        .expect("to_jsonld")
                ),
                normalize_rows(&json!([[2]])),
                "HEAD union count = 2"
            );

            // Time-travel to t=1: count must be the historical 1, not 2.
            let past = fluree.db_at_t(ledger_id, 1).await.expect("view t=1");
            assert_eq!(
                normalize_rows(
                    &fluree
                        .query(&past, QueryInput::Sparql(q))
                        .await
                        .expect("count t=1")
                        .to_jsonld(&past.snapshot)
                        .expect("to_jsonld")
                ),
                normalize_rows(&json!([[1]])),
                "time-travel union count must be the historical 1, not the current 2"
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

/// Regression: a star-join `COUNT(*)` (`?s p1 ?o1 . ?s p2 ?o2`) must track
/// novelty. This shape is handled only by the generic `count_plan`, which used
/// to bail entirely on overlay. `count_plan_exec` now has an overlay lane:
/// subject-count streams come from the overlay-merging PSOT cursor instead of
/// base-leaflet metadata, and the star multiply runs over those. Verifies the
/// count tracks an overlay assertion (a subject gaining the second predicate)
/// and an overlay retraction (a subject losing the first).
#[tokio::test]
async fn indexed_overlay_star_join_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-star-count:main";

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

            // Phase 1: a,b have both age+name; c has age only. Star count = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:age": 30, "ex:name": "A"},
                    {"@id": "ex:b", "ex:age": 40, "ex:name": "B"},
                    {"@id": "ex:c", "ex:age": 50}
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

            // Star: COUNT(*) = Σ_s count_age(s) × count_name(s) over s with both.
            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?s ex:age ?a . ?s ex:name ?n . }
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
                "indexed-only star count should be 2 (a, b)"
            );

            // Phase 2: c gains a name in novelty → now has both → star count = 3.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:c", "ex:name": "C"} ]
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
                "star count should reflect c gaining a name in novelty"
            );

            // Phase 3: retract a's age in novelty → a drops out → star count = 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:a", "ex:age": 30} ]
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
                "star count should reflect a losing its age in novelty (b, c)"
            );
        })
        .await;
}

/// Regression: a subject `MINUS` `COUNT(*)` (`?s p ?a . MINUS { ?s q ?r }`)
/// must track novelty. `count_plan` lowers this to `Sum(AntiJoin { source, excluded })`;
/// the overlay lane now draws both the source subject-count stream and the
/// excluded subject keyset from the overlay-merging PSOT cursor. Verifies the
/// count tracks an overlay assertion and retraction of the excluded predicate.
#[tokio::test]
async fn indexed_overlay_minus_subject_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-minus-count:main";

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

            // a,c are retired; b,d are not. All four have age. MINUS retired => {b,d} = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:age": 30, "ex:retired": true},
                    {"@id": "ex:b", "ex:age": 40},
                    {"@id": "ex:c", "ex:age": 50, "ex:retired": true},
                    {"@id": "ex:d", "ex:age": 60}
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
                WHERE { ?s ex:age ?a . MINUS { ?s ex:retired ?r } }
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
                "indexed-only MINUS count should be 2 (b, d)"
            );

            // Assert d retired in novelty → excluded → {b} = 1.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:d", "ex:retired": true} ]
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
                normalize_rows(&json!([[1]])),
                "MINUS count should reflect d becoming retired (b)"
            );

            // Retract a's retired in novelty → a re-included → {a, b} = 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:a", "ex:retired": true} ]
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
                "MINUS count should reflect a losing retired (a, b)"
            );
        })
        .await;
}

/// Regression: a subject `FILTER EXISTS` `COUNT(*)` must track novelty.
/// `count_plan` lowers `?s p ?a . FILTER EXISTS { ?s q ?x }` to
/// `Sum(SemiJoin { source, filter })`; the overlay lane draws both from the
/// overlay-merging PSOT cursor. Verifies tracking of an overlay assertion and
/// retraction of the EXISTS predicate.
#[tokio::test]
async fn indexed_overlay_exists_subject_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-exists-count:main";

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

            // a,c are active; all four have age. EXISTS active => {a,c} = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:age": 30, "ex:active": true},
                    {"@id": "ex:b", "ex:age": 40},
                    {"@id": "ex:c", "ex:age": 50, "ex:active": true},
                    {"@id": "ex:d", "ex:age": 60}
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
                WHERE { ?s ex:age ?a . FILTER EXISTS { ?s ex:active ?x } }
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
                "indexed-only EXISTS count should be 2 (a, c)"
            );

            // Assert b active in novelty → included → {a, b, c} = 3.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:b", "ex:active": true} ]
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
                "EXISTS count should reflect b becoming active (a, b, c)"
            );

            // Retract c's active in novelty → c excluded → {a, b} = 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:c", "ex:active": true} ]
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
                "EXISTS count should reflect c losing active (a, b)"
            );
        })
        .await;
}

/// Regression: a subject `OPTIONAL` `COUNT(*)` must track novelty, including
/// the OPTIONAL multiplicity. `count_plan` lowers `?s p ?a . OPTIONAL { ?s q ?t }`
/// to `Sum(OptionalJoin { required, optional_groups })` with the per-subject
/// formula `count_p(s) × max(1, count_q(s))`. The overlay lane draws both the
/// required and optional subject-count streams from the overlay-merging PSOT
/// cursor. Verifies the count tracks an overlay assertion that raises a
/// multiplier and a retraction that lowers one.
#[tokio::test]
async fn indexed_overlay_optional_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-optional-count:main";

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

            // a: 2 tags, b: 1 tag, c: 0 tags. Each has one age.
            // COUNT(*) = 1×2 + 1×1 + 1×max(1,0) = 4.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:age": 30, "ex:tag": ["m", "n"]},
                    {"@id": "ex:b", "ex:age": 40, "ex:tag": "m"},
                    {"@id": "ex:c", "ex:age": 50}
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
                WHERE { ?s ex:age ?a . OPTIONAL { ?s ex:tag ?t } }
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
                "indexed-only OPTIONAL count should be 4 (2 + 1 + 1)"
            );

            // c gains two tags in novelty → c: 1×2 → total = 2 + 1 + 2 = 5.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:c", "ex:tag": ["x", "y"]} ]
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
                normalize_rows(&json!([[5]])),
                "OPTIONAL count should reflect c gaining two tags (2+1+2)"
            );

            // Retract one of a's tags in novelty → a: 1×1 → total = 1 + 1 + 2 = 4.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:a", "ex:tag": "n"} ]
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
                normalize_rows(&json!([[4]])),
                "OPTIONAL count should reflect a losing a tag (1+1+2)"
            );
        })
        .await;
}

/// Regression: a composite (s,o)-join `COUNT(*)` (`?s p1 ?o . ?s p2 ?o`, sharing
/// both subject and object var) must track novelty. `count_plan` lowers this to
/// `CompositeJoinPairCount`, a merge-intersection on `(s_id, o_type, o_key)`.
/// The overlay lane streams both relations' `(s, o)` rows from the
/// overlay-merging PSOT cursor. Verifies tracking of an overlay assertion that
/// creates a shared pair and a retraction that removes one.
#[tokio::test]
async fn indexed_overlay_composite_join_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-composite-count:main";

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

            // a likes {x,y} owns {x,z} → shared {x}; b likes {p} owns {p} → shared {p}.
            // COUNT(*) over (?s likes ?o . ?s owns ?o) = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {
                        "@id": "ex:a",
                        "ex:likes": [{"@id": "ex:x"}, {"@id": "ex:y"}],
                        "ex:owns": [{"@id": "ex:x"}, {"@id": "ex:z"}]
                    },
                    {"@id": "ex:b", "ex:likes": {"@id": "ex:p"}, "ex:owns": {"@id": "ex:p"}}
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
                WHERE { ?s ex:likes ?o . ?s ex:owns ?o }
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
                "indexed-only composite-join count should be 2 ((a,x), (b,p))"
            );

            // a owns y in novelty → shared {x,y} → total = 2 + 1 = 3.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:a", "ex:owns": {"@id": "ex:y"}} ]
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
                "composite-join count should reflect a owning y ((a,x),(a,y),(b,p))"
            );

            // Retract b owns p in novelty → b shared {} → total = 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:b", "ex:owns": {"@id": "ex:p"}} ]
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
                "composite-join count should reflect b losing owns p ((a,x),(a,y))"
            );
        })
        .await;
}

/// Regression: an object-var `FILTER EXISTS` `COUNT(*)`
/// (`?s p ?o . FILTER EXISTS { ?o q ?x }`) must track novelty. `count_plan`
/// lowers this to `PostObjectFilteredSum { pred, object_filter }`; the overlay
/// lane streams POST(`p`) from the overlay-merging POST cursor and matches each
/// ref object against the (overlay-aware) subject keyset of `q`.
#[tokio::test]
async fn indexed_overlay_object_exists_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-object-exists:main";

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

            // a knows {b,c,d}; b,c active. EXISTS active on object => (a,b),(a,c) = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:knows": [{"@id": "ex:b"}, {"@id": "ex:c"}, {"@id": "ex:d"}]},
                    {"@id": "ex:b", "ex:active": true},
                    {"@id": "ex:c", "ex:active": true}
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
                WHERE { ?s ex:knows ?o . FILTER EXISTS { ?o ex:active ?x } }
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
                "indexed-only object-EXISTS count should be 2 ((a,b),(a,c))"
            );

            // d becomes active in novelty → (a,d) qualifies → 3.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:d", "ex:active": true} ]
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
                "object-EXISTS count should reflect d becoming active"
            );

            // b loses active in novelty → (a,b) drops → 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:b", "ex:active": true} ]
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
                "object-EXISTS count should reflect b losing active ((a,c),(a,d))"
            );
        })
        .await;
}

/// Regression: an object-var `MINUS` `COUNT(*)`
/// (`?s p ?o . MINUS { ?o q ?x }`) must track novelty. `count_plan` lowers this
/// to `TotalMinusPostObjectFilteredSum`: `count(p) - postObjectFilteredSum(p, subjects(q))`.
/// The overlay lane uses the cursor for both the total and the filtered sum.
#[tokio::test]
async fn indexed_overlay_object_minus_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-object-minus:main";

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

            // a knows {b,c,d} (3 edges); b active. MINUS active object => not-active = c,d = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:knows": [{"@id": "ex:b"}, {"@id": "ex:c"}, {"@id": "ex:d"}]},
                    {"@id": "ex:b", "ex:active": true}
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
                WHERE { ?s ex:knows ?o . MINUS { ?o ex:active ?x } }
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
                "indexed-only object-MINUS count should be 2 (3 - 1)"
            );

            // c becomes active in novelty → in_set = {b,c} → 3 - 2 = 1.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:c", "ex:active": true} ]
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
                normalize_rows(&json!([[1]])),
                "object-MINUS count should reflect c becoming active (3 - 2)"
            );

            // b loses active in novelty → in_set = {c} → 3 - 1 = 2.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:b", "ex:active": true} ]
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
                "object-MINUS count should reflect b losing active (3 - 1)"
            );
        })
        .await;
}

/// Regression: an object-chain `FILTER EXISTS` `COUNT(*)`
/// (`?s p ?o . FILTER EXISTS { ?o q ?m . ?m r ?t }`) must track novelty. The
/// EXISTS block is a 2-hop chain, so `count_plan` lowers it to
/// `PostObjectFilteredSum { p, SubjectsWithObjectIn { q, SubjectSet(r) } }`.
/// This exercises the overlay lane for BOTH the POST-object filtered sum and the
/// object-chain `subjects_with_object_in` keyset.
#[tokio::test]
async fn indexed_overlay_object_chain_exists_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-object-chain:main";

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

            // a knows {x,y,z}; x,z like m; y likes n; m tasty.
            // Qualifying objects (like something tasty): x, z. Edges (a,x),(a,z) => 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:knows": [{"@id": "ex:x"}, {"@id": "ex:y"}, {"@id": "ex:z"}]},
                    {"@id": "ex:x", "ex:likes": {"@id": "ex:m"}},
                    {"@id": "ex:y", "ex:likes": {"@id": "ex:n"}},
                    {"@id": "ex:z", "ex:likes": {"@id": "ex:m"}},
                    {"@id": "ex:m", "ex:tasty": true}
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
                WHERE {
                  ?s ex:knows ?o .
                  FILTER EXISTS { ?o ex:likes ?m . ?m ex:tasty ?t }
                }
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
                "indexed-only object-chain EXISTS count should be 2 ((a,x),(a,z))"
            );

            // n becomes tasty in novelty → y qualifies → (a,y) → 3.
            let assert = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [ {"@id": "ex:n", "ex:tasty": true} ]
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
                "object-chain EXISTS count should reflect n becoming tasty (a,x),(a,y),(a,z)"
            );

            // m loses tasty in novelty → x,z drop, only y qualifies → (a,y) → 1.
            let retract = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "delete": [ {"@id": "ex:m", "ex:tasty": true} ]
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
                "object-chain EXISTS count should reflect m losing tasty ((a,y))"
            );
        })
        .await;
}

/// Regression: a 2-hop chain `COUNT(*)` (`?a p1 ?b . ?b p2 ?c`) must track
/// novelty. `count_plan` lowers this to a `ChainFold`; the overlay lane folds
/// per-hop weight maps over the overlay-merging PSOT cursor (no seek cursor).
/// Count = Σ over p1 edges of out-degree(b in p2).
#[tokio::test]
async fn indexed_overlay_chain2_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-chain2:main";

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

            // a p1 {b,e}; b p2 {c,d}; e p2 {f}. Paths = od(b)+od(e) = 2+1 = 3.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": [{"@id": "ex:b"}, {"@id": "ex:e"}]},
                    {"@id": "ex:b", "ex:p2": [{"@id": "ex:c"}, {"@id": "ex:d"}]},
                    {"@id": "ex:e", "ex:p2": {"@id": "ex:f"}}
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
                WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c }
            ";

            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[3]])),
                "indexed-only 2-hop chain count should be 3"
            );

            // b gains a p2 object in novelty → od(b)=3 → total = 3+1 = 4.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:b", "ex:p2": {"@id": "ex:g"}} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[4]])),
                "chain count should reflect b gaining a p2 object"
            );

            // retract e's only p2 object → od(e)=0 → total = 3+0 = 3.
            let ledger3 = fluree
                .update(
                    ledger2,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [ {"@id": "ex:e", "ex:p2": {"@id": "ex:f"}} ]
                    }),
                )
                .await
                .expect("retract")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger3.t()).await),
                normalize_rows(&json!([[3]])),
                "chain count should reflect e losing its p2 object"
            );
        })
        .await;
}

/// Regression: a 3-hop chain `COUNT(*)` (`?a p1 ?b . ?b p2 ?c . ?c p3 ?d`) must
/// track novelty. Exercises the overlay chain fold (right-to-left through p2).
#[tokio::test]
async fn indexed_overlay_chain3_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-chain3:main";

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

            // a p1 b; b p2 {c,x}; c p3 {d,e}; x p3 {}. Paths = comp2[b] = od(c)+od(x) = 2+0 = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": {"@id": "ex:b"}},
                    {"@id": "ex:b", "ex:p2": [{"@id": "ex:c"}, {"@id": "ex:x"}]},
                    {"@id": "ex:c", "ex:p3": [{"@id": "ex:d"}, {"@id": "ex:e"}]}
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
                WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?c ex:p3 ?d }
            ";

            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[2]])),
                "indexed-only 3-hop chain count should be 2"
            );

            // x gains a p3 object in novelty → comp2[b] = od(c)+od(x) = 2+1 = 3.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:x", "ex:p3": {"@id": "ex:y"}} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[3]])),
                "3-hop chain count should reflect x gaining a p3 object"
            );

            // retract one of c's p3 objects → od(c)=1 → comp2[b] = 1+1 = 2.
            let ledger3 = fluree
                .update(
                    ledger2,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [ {"@id": "ex:c", "ex:p3": {"@id": "ex:d"}} ]
                    }),
                )
                .await
                .expect("retract")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger3.t()).await),
                normalize_rows(&json!([[2]])),
                "3-hop chain count should reflect c losing a p3 object"
            );
        })
        .await;
}

/// Regression: a chain with a tail `FILTER EXISTS` modifier
/// (`?a p1 ?b . ?b p2 ?c . FILTER EXISTS { ?c p3 ?d }`) must track novelty.
/// Exercises the overlay `ChainWeight::InSet` rightmost build.
#[tokio::test]
async fn indexed_overlay_chain_tail_exists_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-chain-tail-exists:main";

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

            // a p1 b; b p2 {c,x}; c p3 d. Tail EXISTS p3 → only c qualifies → 1.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": {"@id": "ex:b"}},
                    {"@id": "ex:b", "ex:p2": [{"@id": "ex:c"}, {"@id": "ex:x"}]},
                    {"@id": "ex:c", "ex:p3": {"@id": "ex:d"}}
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
                WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . FILTER EXISTS { ?c ex:p3 ?d } }
            ";

            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[1]])),
                "indexed-only tail-EXISTS chain count should be 1 (only c has p3)"
            );

            // x gains p3 in novelty → both c,x qualify → 2.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:x", "ex:p3": {"@id": "ex:e"}} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[2]])),
                "tail-EXISTS chain count should reflect x gaining p3"
            );

            // retract c's p3 → only x qualifies → 1.
            let ledger3 = fluree
                .update(
                    ledger2,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [ {"@id": "ex:c", "ex:p3": {"@id": "ex:d"}} ]
                    }),
                )
                .await
                .expect("retract")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger3.t()).await),
                normalize_rows(&json!([[1]])),
                "tail-EXISTS chain count should reflect c losing p3"
            );
        })
        .await;
}

/// Regression: a chain with a tail `MINUS` modifier
/// (`?a p1 ?b . ?b p2 ?c . MINUS { ?c p3 ?d }`) must track novelty. Exercises
/// the overlay `ChainWeight::NotInSet` rightmost build.
#[tokio::test]
async fn indexed_overlay_chain_tail_minus_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-chain-tail-minus:main";

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

            // a p1 b; b p2 {c,x}; c p3 d. Tail MINUS p3 → only x (no p3) qualifies → 1.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:p1": {"@id": "ex:b"}},
                    {"@id": "ex:b", "ex:p2": [{"@id": "ex:c"}, {"@id": "ex:x"}]},
                    {"@id": "ex:c", "ex:p3": {"@id": "ex:d"}}
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
                WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . MINUS { ?c ex:p3 ?d } }
            ";

            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[1]])),
                "indexed-only tail-MINUS chain count should be 1 (only x lacks p3)"
            );

            // x gains p3 in novelty → neither qualifies → 0.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:x", "ex:p3": {"@id": "ex:e"}} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[0]])),
                "tail-MINUS chain count should reflect x gaining p3"
            );

            // retract c's p3 → c qualifies again (x still has p3) → 1.
            let ledger3 = fluree
                .update(
                    ledger2,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [ {"@id": "ex:c", "ex:p3": {"@id": "ex:d"}} ]
                    }),
                )
                .await
                .expect("retract")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger3.t()).await),
                normalize_rows(&json!([[1]])),
                "tail-MINUS chain count should reflect c losing p3 (only c qualifies)"
            );
        })
        .await;
}

/// Regression: optional chain-head `COUNT(*)`
/// (`?a p1 ?b . OPTIONAL { ?b p2 ?c . ?c p3 ?d }`). This shape was a standalone
/// fast path (now folded into `count_plan` as `OptionalChainHead`). Phase 1
/// (indexed, no overlay) exercises the metadata lane; phases 2-3 exercise the
/// new overlay lane (the old operator bailed on overlay).
/// Count = Σ over p1 edges of max(1, Σ_{c ∈ p2(b)} count_p3(c)).
#[tokio::test]
async fn indexed_overlay_optional_chain_head_count_reflects_overlay() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-opt-chain-head:main";

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

            // a follows {b,e}; b likes {c,f}; e likes {}; c rates {x,y}; f rates {z}.
            // comp2(b)=rates(c)+rates(f)=2+1=3 → (a,b)→max(1,3)=3; comp2(e)=0 → (a,e)→1. Total 4.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:follows": [{"@id": "ex:b"}, {"@id": "ex:e"}]},
                    {"@id": "ex:b", "ex:likes": [{"@id": "ex:c"}, {"@id": "ex:f"}]},
                    {"@id": "ex:c", "ex:rates": [{"@id": "ex:x"}, {"@id": "ex:y"}]},
                    {"@id": "ex:f", "ex:rates": {"@id": "ex:z"}}
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
                WHERE { ?a ex:follows ?b . OPTIONAL { ?b ex:likes ?c . ?c ex:rates ?d } }
            ";

            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            // Phase 1: metadata lane (no overlay).
            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[4]])),
                "indexed-only optional chain-head count should be 4 (3 + 1)"
            );

            // Phase 2: e gains a like of c in novelty → comp2(e)=rates(c)=2 → (a,e)→2 → total 5.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:e", "ex:likes": {"@id": "ex:c"}} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[5]])),
                "optional chain-head count should reflect e liking c (3 + 2)"
            );

            // Phase 3: retract one of c's rates → rates(c)=1.
            // comp2(b)=1+1=2 → (a,b)→2; comp2(e)=1 → (a,e)→1. Total 3.
            let ledger3 = fluree
                .update(
                    ledger2,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "delete": [ {"@id": "ex:c", "ex:rates": {"@id": "ex:x"}} ]
                    }),
                )
                .await
                .expect("retract")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger3.t()).await),
                normalize_rows(&json!([[3]])),
                "optional chain-head count should reflect c losing a rating (2 + 1)"
            );
        })
        .await;
}

/// Regression (OPTIONAL chain-head, non-IRI `p1` object): a literal `?b` object
/// of the head predicate still contributes 1 to the count — the OPTIONAL keeps
/// the `?a p1 ?b` solution even though a literal can't be a `p2` subject. The
/// metadata lane (no overlay) must defer mixed-type `p1` to the generic
/// pipeline (its IRI-only POST iterator would otherwise drop/terminate); the
/// overlay lane must count non-ref `?b` with multiplier 1.
#[tokio::test]
async fn indexed_optional_chain_head_literal_p1_object_counts_once() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/opt-chain-literal-p1:main";
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
            // a follows {b (IRI), "lit1", "lit2"}; b likes c; c rates {x,y}.
            // comp2(b) = rates(c) = 2 → (a,b) → max(1,2) = 2; each literal → 1. Total 4.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:follows": [{"@id": "ex:b"}, "lit1", "lit2"]},
                    {"@id": "ex:b", "ex:likes": {"@id": "ex:c"}},
                    {"@id": "ex:c", "ex:rates": [{"@id": "ex:x"}, {"@id": "ex:y"}]}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?a ex:follows ?b . OPTIONAL { ?b ex:likes ?c . ?c ex:rates ?d } }
            ";
            let run = |t| {
                let fluree = &fluree;
                async move {
                    let view = fluree.db_at_t(ledger_id, t).await.expect("load view");
                    let result = fluree
                        .query(&view, QueryInput::Sparql(query))
                        .await
                        .expect("count");
                    result.to_jsonld(&view.snapshot).expect("to_jsonld")
                }
            };

            // Indexed, no overlay → metadata lane defers mixed-type p1 to generic.
            assert_eq!(
                normalize_rows(&run(ledger1.t()).await),
                normalize_rows(&json!([[4]])),
                "literal follows-objects each count once (2 + 1 + 1)"
            );

            // Novelty adds `a follows \"lit3\"` → overlay lane; non-ref ?b ⇒ +1 ⇒ 5.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:a", "ex:follows": "lit3"} ]
                    }),
                )
                .await
                .expect("assert")
                .ledger;
            assert_eq!(
                normalize_rows(&run(ledger2.t()).await),
                normalize_rows(&json!([[5]])),
                "overlay lane counts non-ref ?b with multiplier 1 (2 + 1 + 1 + 1)"
            );
        })
        .await;
}

/// Regression (OPTIONAL chain-head, absent `p2`): when the first inner-chain
/// predicate has no data, the OPTIONAL can never match, so every `?a p1 ?b`
/// row contributes exactly 1. The metadata lane previously returned 0 here.
#[tokio::test]
async fn indexed_optional_chain_head_absent_p2_counts_all_p1_rows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/opt-chain-absent-p2:main";
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
            // a follows {b, e} (IRI). `ex:rates` exists so p3 resolves; `ex:missing`
            // is never used so p2 is absent. Inner chain can never match ⇒ count 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:follows": [{"@id": "ex:b"}, {"@id": "ex:e"}]},
                    {"@id": "ex:c", "ex:rates": {"@id": "ex:x"}}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?a ex:follows ?b . OPTIONAL { ?b ex:missing ?c . ?c ex:rates ?d } }
            ";
            let view = fluree.db_at_t(ledger_id, ledger1.t()).await.expect("view");
            let result = fluree
                .query(&view, QueryInput::Sparql(query))
                .await
                .expect("count");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[2]])),
                "absent p2 ⇒ every p1 row counts once (was incorrectly 0)"
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

/// Regression for MEDIUM-1: overlay COUNT projection invariant on a **cache-less**
/// store.
///
/// The count-plan overlay cursors project NARROW column subsets (here SId+OType+
/// OKey via `cursor_projection_sid_otype_okey` → `ColumnSet(13)`), but
/// `merge_overlay_into_batch` compares each base row against the overlay ops on
/// the FULL V3 identity (s_id, p_id, o_type, o_key, o_i). With
/// `LedgerManagerConfig::default()` the store has **no leaflet cache**
/// (`leaflet_cache: None`), so the narrow projection is what actually loads — a
/// missing identity column would read as `AbsentDefault` (p_id→0, o_i→u32::MAX)
/// and corrupt the merge, mis-counting under overlay. (Production is masked only
/// because an injected cache always loads `ColumnProjection::all()`.)
///
/// `build_overlay_cursor_for_predicate` now forces the identity columns into the
/// projection's `internal` set, and `BinaryCursor::set_overlay_ops` debug-asserts
/// their presence. Without the fix this exact query panics that assert (and would
/// mis-count in release). This test locks the end-to-end count on a no-cache store.
#[tokio::test]
async fn indexed_overlay_count_no_cache_projection_invariant() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/overlay-count-projection-invariant:main";

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

            // Baseline: a,c active; all four have age. EXISTS(active) over the
            // age subjects drives a subject-count fold through the narrow overlay
            // cursor. Indexed-only => {a, c} = 2.
            let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
            let baseline = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:age": 30, "ex:active": true},
                    {"@id": "ex:b", "ex:age": 40},
                    {"@id": "ex:c", "ex:age": 50, "ex:active": true},
                    {"@id": "ex:d", "ex:age": 60}
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
            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger1.t()).await;

            let query = r"
                PREFIX ex: <http://example.org/ns/>
                SELECT (COUNT(*) AS ?cnt)
                WHERE { ?s ex:age ?a . FILTER EXISTS { ?s ex:active ?x } }
            ";

            // Novelty assert: b becomes active → overlay-merged count = {a,b,c} = 3.
            // This is the case that miscounts if the narrow projection drops the
            // identity columns the overlay merge needs.
            let ledger2 = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": { "ex": "http://example.org/ns/" },
                        "@graph": [ {"@id": "ex:b", "ex:active": true} ]
                    }),
                )
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
                .expect("overlay count");
            let jsonld = result.to_jsonld(&view2.snapshot).expect("to_jsonld");
            assert_eq!(
                normalize_rows(&jsonld),
                normalize_rows(&json!([[3]])),
                "no-cache overlay EXISTS count must be 3 (a, b, c) with b asserted in novelty"
            );
        })
        .await;
}
