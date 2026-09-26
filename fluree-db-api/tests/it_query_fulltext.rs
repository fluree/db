//! Integration tests for fulltext scoring via `fulltext()` function.
//!
//! These tests exercise the full pipeline: transact `@fulltext` data →
//! build binary index (including FTA1 fulltext arenas) → query with
//! `fulltext(?var, "query")` in bind expressions → verify BM25 scoring.
//!
//! Tests cover:
//! - Basic arena-based BM25 scoring (positive scores for matching docs)
//! - Non-matching documents produce score 0
//! - Multi-document ranking (more/better matches → higher scores)
//! - Retraction removes documents from the arena
//! - Multiple predicates produce independent arenas
//!
//! These tests require the binary index to be built, so they use the native feature.

#![cfg(feature = "native")]

use crate::support;
use crate::support::start_background_indexer_local;
use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Value as JsonValue};

fn fulltext_context() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#"
    })
}

/// Helper to insert a document with @fulltext content.
async fn insert_doc(
    fluree: &support::MemoryFluree,
    ledger: support::MemoryLedger,
    id: &str,
    title: &str,
    content: &str,
) -> support::MemoryLedger {
    let tx = json!({
        "@context": fulltext_context(),
        "@id": id,
        "ex:title": title,
        "ex:content": {
            "@value": content,
            "@type": "@fulltext"
        }
    });

    fluree.insert(ledger, &tx).await.expect("insert doc").ledger
}

/// Helper to run a fulltext query and return (title, score) pairs ordered by score desc.
async fn query_fulltext(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    query_text: &str,
) -> Vec<(String, f64)> {
    let bind_expr = format!("(fulltext ?content \"{query_text}\")");

    let query = json!({
        "@context": fulltext_context(),
        "select": ["?title", "?score"],
        "where": [
            { "@id": "?doc", "ex:content": "?content", "ex:title": "?title" },
            ["bind", "?score", bind_expr],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]]
    });

    let result = support::query_jsonld(fluree, ledger, &query).await;
    match result {
        Ok(r) => {
            let json_rows = r.to_jsonld(&ledger.snapshot).expect("jsonld");
            json_rows
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|row| {
                            let arr = row.as_array()?;
                            let title = arr.first()?.as_str()?.to_string();
                            let score = arr.get(1)?.as_f64()?;
                            Some((title, score))
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        Err(e) => {
            panic!("Fulltext query failed: {e}");
        }
    }
}

/// Trigger indexing and wait for completion.
async fn index_and_load(
    fluree: &support::MemoryFluree,
    handle: &fluree_db_indexer::IndexerHandle,
    alias: &str,
    t: i64,
) -> LedgerState {
    let completion = handle
        .trigger(&fluree_db_api::LedgerId::parse(alias).unwrap(), t)
        .await;
    match completion.wait().await {
        fluree_db_api::IndexOutcome::Completed { .. } => {}
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    }
    fluree.ledger(alias).await.expect("load ledger")
}

// =============================================================================
// Basic scoring tests
// =============================================================================

#[tokio::test]
async fn fulltext_basic_scoring_returns_positive_for_matching_doc() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-basic:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Rust Guide",
                "Rust is a systems programming language focused on safety and performance",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            let results = query_fulltext(&fluree, &loaded, "Rust programming").await;

            assert!(
                !results.is_empty(),
                "Should find at least one matching document"
            );
            assert_eq!(results[0].0, "Rust Guide");
            assert!(
                results[0].1 > 0.0,
                "Matching doc should have positive score: {}",
                results[0].1
            );
        })
        .await;
}

#[tokio::test]
async fn fulltext_non_matching_query_excluded_by_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-nomatch:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Rust Guide",
                "Rust is a systems programming language",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Query for terms not in the document
            let results = query_fulltext(&fluree, &loaded, "cooking recipes").await;

            assert!(
                results.is_empty(),
                "Non-matching query should return no results (filtered by > 0)"
            );
        })
        .await;
}

// =============================================================================
// Ranking tests
// =============================================================================

#[tokio::test]
async fn fulltext_ranking_more_relevant_doc_scores_higher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-ranking:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Doc 1: mentions "database" once
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Intro",
                "This guide covers database fundamentals and design patterns",
            )
            .await;

            // Doc 2: mentions "database" multiple times
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "Deep Dive",
                "Database indexing strategies for database performance optimization in database systems",
            )
            .await;

            // Doc 3: no match
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc3",
                "Unrelated",
                "Cooking recipes for pasta and bread",
            )
            .await;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            let results = query_fulltext(&fluree, &loaded, "database").await;

            assert_eq!(
                results.len(),
                2,
                "Should find exactly two matching docs, got: {results:?}"
            );

            // The doc with more occurrences of "database" should rank higher
            assert_eq!(
                results[0].0, "Deep Dive",
                "Doc with higher TF should rank first"
            );
            assert_eq!(
                results[1].0, "Intro",
                "Doc with lower TF should rank second"
            );
            assert!(
                results[0].1 > results[1].1,
                "Higher TF doc should have higher score: {} vs {}",
                results[0].1,
                results[1].1
            );
        })
        .await;
}

// =============================================================================
// Retraction tests
// =============================================================================

#[tokio::test]
async fn fulltext_retraction_removes_doc_from_results() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-retract:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert two documents
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Keeper",
                "Rust programming language guide",
            )
            .await;
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "ToRemove",
                "Rust compiler optimization techniques",
            )
            .await;

            // Index after initial inserts
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;
            let results_before = query_fulltext(&fluree, &loaded, "Rust").await;
            assert_eq!(
                results_before.len(),
                2,
                "Should find both docs before retraction"
            );

            // Retract the content of doc2 by updating it to a non-fulltext value
            let retract_tx = json!({
                "@context": fulltext_context(),
                "where": {
                    "@id": "ex:doc2",
                    "ex:content": "?old"
                },
                "delete": {
                    "@id": "ex:doc2",
                    "ex:content": "?old"
                }
            });
            let ledger = fluree
                .update(loaded, &retract_tx)
                .await
                .expect("retract")
                .ledger;

            // Re-index after retraction
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;
            let results_after = query_fulltext(&fluree, &loaded, "Rust").await;

            assert_eq!(
                results_after.len(),
                1,
                "Should find only one doc after retraction, got: {results_after:?}"
            );
            assert_eq!(results_after[0].0, "Keeper");
        })
        .await;
}

// =============================================================================
// Novelty overlay test
// =============================================================================

#[tokio::test]
async fn fulltext_novelty_docs_scored_when_arena_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-novelty:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert two @fulltext docs (arena will exist for ex:content)
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc1",
                "Indexed Doc",
                "Rust programming language systems performance safety",
            )
            .await;
            let ledger = insert_doc(
                &fluree,
                ledger,
                "ex:doc2",
                "Also Indexed",
                "Rust compiler optimization techniques for fast builds",
            )
            .await;

            // Seed a *persisted* string dict entry that is NOT indexed as @fulltext.
            //
            // This ensures the novelty doc below reuses an existing string_id that:
            // - is <= persisted string watermark (so it will be emitted as EncodedLit)
            // - is NOT present in the fulltext arena (no DocBoW), reproducing the bug
            //   that previously forced arena BM25 scoring to 0.0.
            let seeded_plain_text = "Rust async runtime tokio concurrent programming patterns";
            let seed_tx = json!({
                "@context": fulltext_context(),
                "@id": "ex:seed",
                "ex:title": "Seed Plain",
                // Plain string (NOT @fulltext) — should not be indexed into the arena.
                "ex:content": seeded_plain_text
            });
            let ledger = fluree
                .insert(ledger, &seed_tx)
                .await
                .expect("seed insert")
                .ledger;

            // Index → arenas are built for docs 1 and 2 (but not for the seeded plain string)
            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Verify both indexed docs are found
            let results = query_fulltext(&fluree, &loaded, "Rust").await;
            assert_eq!(
                results.len(),
                2,
                "Should find both indexed docs before novelty insert"
            );

            // Now insert a THIRD doc WITHOUT re-indexing (this is in novelty).
            // IMPORTANT: it reuses the seeded string value so the string_id is persisted,
            // but the doc is not present in the arena (novelty assertion).
            let ledger =
                insert_doc(&fluree, loaded, "ex:doc3", "Novelty Doc", seeded_plain_text).await;

            // Query the ledger with novelty — should find all 3 docs
            let results = query_fulltext(&fluree, &ledger, "Rust").await;
            assert_eq!(
                results.len(),
                3,
                "Should find indexed AND novelty docs, got: {results:?}"
            );

            // The novelty doc should appear with a positive score
            let novelty_result = results.iter().find(|(title, _)| title == "Novelty Doc");
            assert!(
                novelty_result.is_some(),
                "Novelty doc should appear in results"
            );
            assert!(
                novelty_result.unwrap().1 > 0.0,
                "Novelty doc should have positive score: {}",
                novelty_result.unwrap().1
            );
        })
        .await;
}

// =============================================================================
// Multiple predicates test
// =============================================================================

#[tokio::test]
async fn fulltext_multiple_predicates_independent_arenas() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "it/fulltext-multi-pred:main";

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
            let db0 = LedgerSnapshot::genesis(alias);
            let ledger = LedgerState::new(db0, Novelty::new(0));

            // Insert a doc with two different @fulltext predicates
            let tx = json!({
                "@context": fulltext_context(),
                "@id": "ex:doc1",
                "ex:title": "Multi-field Doc",
                "ex:content": {
                    "@value": "Rust programming language guide for beginners",
                    "@type": "@fulltext"
                },
                "ex:summary": {
                    "@value": "A comprehensive overview of Rust fundamentals",
                    "@type": "@fulltext"
                }
            });
            let ledger = fluree.insert(ledger, &tx).await.expect("insert").ledger;

            let loaded = index_and_load(&fluree, &handle, alias, ledger.snapshot.t).await;

            // Query against ex:content
            let results_content = query_fulltext(&fluree, &loaded, "programming").await;

            // Query against ex:summary using a custom query
            let bind_expr = "(fulltext ?summary \"comprehensive overview\")";
            let query = json!({
                "@context": fulltext_context(),
                "select": ["?title", "?score"],
                "where": [
                    { "@id": "?doc", "ex:summary": "?summary", "ex:title": "?title" },
                    ["bind", "?score", bind_expr],
                    ["filter", "(> ?score 0)"]
                ]
            });
            let result = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("query summary");
            let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
            let results_summary: Vec<(String, f64)> = json_rows
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|row| {
                            let arr = row.as_array()?;
                            let title = arr.first()?.as_str()?.to_string();
                            let score = arr.get(1)?.as_f64()?;
                            Some((title, score))
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Both predicates should return results
            assert!(
                !results_content.is_empty(),
                "ex:content query should find results"
            );
            assert!(
                !results_summary.is_empty(),
                "ex:summary query should find results"
            );
        })
        .await;
}

// =============================================================================
// Configured-property path (`f:fullTextDefaults`)
// =============================================================================
//
// These tests exercise the non-`@fulltext` path: plain-string values on a
// property declared in `f:fullTextDefaults` flow through the BM25 arena
// after a reindex that reads the config.

/// Helper: score a plain-string property via `fulltext(?title, "query")`.
async fn query_fulltext_plain(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    query_text: &str,
) -> Vec<(String, f64)> {
    let bind_expr = format!("(fulltext ?title \"{query_text}\")");
    let query = json!({
        "@context": fulltext_context(),
        "select": ["?id", "?score"],
        "where": [
            { "@id": "?id", "ex:title": "?title" },
            ["bind", "?score", bind_expr],
            ["filter", "(> ?score 0)"]
        ],
        "orderBy": [["desc", "?score"]]
    });

    let result = support::query_jsonld(fluree, ledger, &query).await;
    match result {
        Ok(r) => {
            let json_rows = r.to_jsonld(&ledger.snapshot).expect("jsonld");
            json_rows
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|row| {
                            let arr = row.as_array()?;
                            let id = arr.first()?.as_str()?.to_string();
                            let score = arr.get(1)?.as_f64()?;
                            Some((id, score))
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        Err(e) => panic!("Fulltext query failed: {e}"),
    }
}

/// A plain-string `ex:title` property isn't scored by `fulltext(...)` by
/// default. Once `f:fullTextDefaults` adds `ex:title` and a reindex happens,
/// the same query returns positive scores. This covers the full round-trip
/// of the config path: api resolves config → indexer pre-registers IRIs →
/// `FulltextHook` collects plain-string values → arena built → query side
/// finds the arena under the bucket's `lang_id`.
#[tokio::test]
async fn fulltext_configured_property_indexed_after_reindex() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-reindex:main";
    let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);

    // Suppress auto-reindex so we can control when indexing happens.
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // 1) Write `f:fullTextDefaults` enabling `ex:title` FIRST, while we
    //    still have a live LedgerState to stage against. Then insert the
    //    documents. This ordering mirrors a realistic flow where config
    //    lives alongside the data rather than being bolted on after.
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .
        }}
    "
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write fulltext config");

    // 2) Initial reindex so the config graph itself is indexed + queryable.
    //    At this point the reindex also pre-registers `ex:title` via the
    //    config helper, so the fulltext arena is built in this pass.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("initial reindex to index the config graph");

    // 3) Insert plain-string titles on ex:title (no @fulltext tag). Using
    //    `no_auto` so the incremental path doesn't kick in — we'll force a
    //    final reindex below that picks up everything in one shot.
    let tx_docs = json!({
        "@context": fulltext_context(),
        "@graph": [
            { "@id": "ex:doc1", "ex:title": "Rust programming language guide" },
            { "@id": "ex:doc2", "ex:title": "Cooking recipes for pasta" },
            { "@id": "ex:doc3", "ex:title": "Advanced Rust macros and traits" },
        ]
    });
    let mut ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload after reindex");
    ledger = fluree
        .insert_with_opts(
            ledger,
            &tx_docs,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert docs")
        .ledger;
    let _ = ledger;

    // 4) Reindex — full rebuild now walks every commit (config + docs) and
    //    the admin path reads `f:fullTextDefaults` from the existing index
    //    to seed `IndexerConfig.fulltext_configured_properties` before
    //    building the new one.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex after config + docs");

    let loaded = fluree.ledger(ledger_id).await.expect("load after reindex");

    // 4) Query — plain strings on ex:title should now be scored via BM25.
    let results = query_fulltext_plain(&fluree, &loaded, "Rust").await;
    let hits: std::collections::HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        hits.contains("ex:doc1"),
        "doc1 (mentions 'Rust') should be returned: {results:?}"
    );
    assert!(
        hits.contains("ex:doc3"),
        "doc3 (mentions 'Rust') should be returned: {results:?}"
    );
    assert!(
        !hits.contains("ex:doc2"),
        "doc2 (no Rust) should NOT be returned: {results:?}"
    );
    assert!(
        results.iter().all(|(_, score)| *score > 0.0),
        "all configured-property hits should have positive scores: {results:?}"
    );
}

/// When `f:fullTextDefaults` is NOT configured, plain-string values on
/// `ex:title` do not score — `fulltext(?title, ...)` returns unbound and
/// the `> 0` filter drops every row. This is the pre-config baseline;
/// the test above asserts that enabling config flips this behavior.
#[tokio::test]
async fn fulltext_unconfigured_plain_string_returns_empty() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-unconfigured:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": fulltext_context(),
                "@graph": [
                    { "@id": "ex:doc1", "ex:title": "Rust programming language guide" },
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert")
        .ledger;
    let _ = ledger;
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex without config");

    let loaded = fluree.ledger(ledger_id).await.expect("load");
    let results = query_fulltext_plain(&fluree, &loaded, "Rust").await;
    assert!(
        results.is_empty(),
        "plain-string ex:title must not score without `f:fullTextDefaults`: {results:?}"
    );
}

/// Regression for Finding 2: after a reindex picks up `f:fullTextDefaults`,
/// subsequent non-reindex index builds (the path used by the background
/// indexer and CLI `fluree index`) must continue to collect configured
/// plain-string values. Previously, only `reindex()` and the rebase helper
/// refreshed the configured-property set — follow-up incremental runs would
/// silently stop routing new commits' values into BM25 arenas.
///
/// This test exercises `build_index_for_ledger` directly, which is what the
/// CLI and background worker use, with the api-side
/// `FulltextConfigProvider` attached.
#[tokio::test]
async fn fulltext_configured_property_picked_up_by_build_index_for_ledger() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-steady-state:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // 1) Write config + trigger the initial indexing pass via reindex.
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .
        }}
    "
    );
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;
    let _ = ledger;
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("initial reindex");

    // 2) Add NEW docs AFTER the initial reindex. Without the provider the
    //    configured-property set would still be empty for this build.
    let mut ledger = fluree.ledger(ledger_id).await.expect("load after reindex");
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": fulltext_context(),
                "@graph": [
                    { "@id": "ex:new1", "ex:title": "Advanced Rust systems" },
                    { "@id": "ex:new2", "ex:title": "Cooking pasta recipes" },
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert new docs")
        .ledger;
    let _ = ledger;

    // 3) Invoke the same indexing entry point the CLI / background worker
    //    use — `build_index_for_ledger` — with a provider-attached config.
    let idx_config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(ledger_id),
        fluree.nameservice(),
        ledger_id,
        idx_config,
    )
    .await
    .expect("build_index_for_ledger");

    // Publish the new index so `fluree.ledger()` can load it.
    fluree
        .nameservice_mode()
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");

    // 4) Query — the new docs on `ex:title` should be scored even though
    //    the run that indexed them was NOT `reindex()`.
    let loaded = fluree
        .ledger(ledger_id)
        .await
        .expect("load after incremental");
    let results = query_fulltext_plain(&fluree, &loaded, "Rust").await;
    let hits: std::collections::HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        hits.contains("ex:new1"),
        "steady-state build_index_for_ledger must pick up configured properties: {results:?}"
    );
    assert!(
        !hits.contains("ex:new2"),
        "non-matching title should not score: {results:?}"
    );
}

/// Reproducer for the Solo bug report (2026-04-23): first-ever index build via
/// the background / provider path must pick up `f:fullTextDefaults` from
/// novelty. Before the fix, `ApiFulltextConfigProvider::resolve()` called
/// `resolve_ledger_config(snapshot, novelty, snapshot.t)` with
/// `snapshot.t == 0` (genesis), which filtered out all novelty flakes and
/// returned an empty configured-property list.
#[tokio::test]
async fn fulltext_configured_property_first_build_via_provider() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-first-build:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // 1) Write config (commit t=1).
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .
        }}
    "
    );
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;

    // 2) Insert data (commit t=2) on the configured predicate.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": fulltext_context(),
                "@graph": [
                    { "@id": "ex:doc1", "ex:title": "Advanced Rust systems" },
                    { "@id": "ex:doc2", "ex:title": "Cooking pasta recipes" },
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert docs")
        .ledger;
    let _ = ledger;

    // 3) First-ever index build via the provider path — same code path as
    //    the background worker on a freshly-committed ledger with no prior
    //    index. Uses `build_index_for_ledger`.
    let idx_config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(ledger_id),
        fluree.nameservice(),
        ledger_id,
        idx_config,
    )
    .await
    .expect("build_index_for_ledger");

    fluree
        .nameservice_mode()
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");

    // 4) Query — plain-string `ex:title` should score via BM25 because config
    //    enabled it, even on the first-ever indexing pass.
    let loaded = fluree.ledger(ledger_id).await.expect("load after build");
    let results = query_fulltext_plain(&fluree, &loaded, "Rust").await;
    let hits: std::collections::HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        hits.contains("ex:doc1"),
        "first-ever build must pick up configured properties from novelty: {results:?}"
    );
}

/// Reproducer for the residual Solo finding against 9d239d6: arena is built,
/// but `fulltext(?lit, "…")` still returns null for a language-tagged value
/// on a configured predicate. Matches the `skosxl:literalForm
/// "Competencies"@en` shape from the bug report.
#[tokio::test]
async fn fulltext_configured_langtagged_literal_scores_via_arena() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-langtag:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // Config: make skosxl:literalForm a fulltext-configured predicate.
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix skosxl: <http://www.w3.org/2008/05/skos-xl#> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:defaultLanguage "en" .
            <urn:config:ft> f:property <urn:config:ft:litform> .
            <urn:config:ft:litform> rdf:type f:FullTextProperty .
            <urn:config:ft:litform> f:target skosxl:literalForm .
        }}
    "#
    );
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;

    // Data: language-tagged literal on the configured predicate.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {
                    "skosxl": "http://www.w3.org/2008/05/skos-xl#",
                    "ex": "http://example.org/"
                },
                "@id": "ex:l1",
                "@type": "skosxl:Label",
                "skosxl:literalForm": {"@value": "Competencies", "@language": "en"}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert label")
        .ledger;
    let _ = ledger;

    // Build index via the provider path (same as background worker).
    let idx_config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(ledger_id),
        fluree.nameservice(),
        ledger_id,
        idx_config,
    )
    .await
    .expect("build_index_for_ledger");
    fluree
        .nameservice_mode()
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");

    // Query: exact shape from the bug report.
    let loaded = fluree.ledger(ledger_id).await.expect("load after build");
    let query = json!({
        "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#"},
        "select": ["?lit", "?score"],
        "where": [
            {"@id": "?ln", "skosxl:literalForm": "?lit"},
            ["bind", "?score", "(fulltext ?lit \"competencies\")"]
        ]
    });
    let result = support::query_jsonld(&fluree, &loaded, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
    let rows = json_rows.as_array().expect("rows");
    assert!(!rows.is_empty(), "expected at least one row: {rows:?}");
    let score = rows[0].as_array().and_then(|r| r.get(1)).cloned();
    assert!(
        score
            .as_ref()
            .is_some_and(|s| s.as_f64().unwrap_or(0.0) > 0.0),
        "configured lang-tagged value must score via arena, got {score:?} in rows {rows:?}"
    );
}

/// Reproducer for the Solo bug "Still broken 2": incremental indexing after a
/// new commit on a configured predicate should extend the arena, but no
/// activity is logged and the new value never scores.
#[tokio::test]
async fn fulltext_configured_incremental_adds_to_arena() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-incremental:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix skosxl: <http://www.w3.org/2008/05/skos-xl#> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:litform> .
            <urn:config:ft:litform> rdf:type f:FullTextProperty .
            <urn:config:ft:litform> f:target skosxl:literalForm .
        }}
    "
    );
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;

    // First labelled doc.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#", "ex": "http://example.org/"},
                "@id": "ex:l1",
                "@type": "skosxl:Label",
                "skosxl:literalForm": {"@value": "Competencies", "@language": "en"}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert first label")
        .ledger;
    let _ = ledger;

    // Initial full build.
    let idx_config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(ledger_id),
        fluree.nameservice(),
        ledger_id,
        idx_config,
    )
    .await
    .expect("initial build");
    fluree
        .nameservice_mode()
        .publisher()
        .expect("publisher")
        .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish initial");

    // New commit post-index with a NEW value on the configured predicate.
    let mut ledger = fluree.ledger(ledger_id).await.expect("reload");
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#", "ex": "http://example.org/"},
                "@id": "ex:l2",
                "@type": "skosxl:Label",
                "skosxl:literalForm": {"@value": "Performance Management", "@language": "en"}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert second label")
        .ledger;
    let _ = ledger;

    // Incremental build — same provider, should route the new value through
    // the fulltext hook and extend the arena.
    let idx_config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(ledger_id),
        fluree.nameservice(),
        ledger_id,
        idx_config,
    )
    .await
    .expect("incremental build");
    fluree
        .nameservice_mode()
        .publisher()
        .expect("publisher")
        .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish incremental");

    // Query: the new value ("Performance Management") must be queryable by
    // fulltext(...) — same arena, same bucket.
    let loaded = fluree.ledger(ledger_id).await.expect("load final");
    let query = json!({
        "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#"},
        "select": ["?lit", "?score"],
        "where": [
            {"@id": "?ln", "skosxl:literalForm": "?lit"},
            ["bind", "?score", "(fulltext ?lit \"performance\")"],
            ["filter", "(> ?score 0)"]
        ]
    });
    let result = support::query_jsonld(&fluree, &loaded, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
    let rows = json_rows.as_array().expect("rows");
    assert!(
        !rows.is_empty(),
        "incremental indexing must add new configured values to arena: {rows:?}"
    );

    // The pre-existing value from the initial build must still score — an
    // arena-extend that stomps prior docs would pass the "performance" check
    // above but regress this one.
    let prior_query = json!({
        "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#"},
        "select": ["?lit", "?score"],
        "where": [
            {"@id": "?ln", "skosxl:literalForm": "?lit"},
            ["bind", "?score", "(fulltext ?lit \"competencies\")"],
            ["filter", "(> ?score 0)"]
        ]
    });
    let prior_result = support::query_jsonld(&fluree, &loaded, &prior_query)
        .await
        .expect("prior-value query");
    let prior_json = prior_result
        .to_jsonld(&loaded.snapshot)
        .expect("jsonld prior");
    let prior_rows = prior_json.as_array().expect("prior rows");
    assert!(
        !prior_rows.is_empty(),
        "incremental indexing must preserve prior arena docs: {prior_rows:?}"
    );
}

/// Reproducer for c3000-04 finding: two configured-predicate assertions
/// across two separate commits with other commits in between. Arena should
/// have docs=2, terms>=2. On S3 the user reports docs=1 after full rebuild.
#[tokio::test]
async fn fulltext_configured_two_commits_two_values_full_rebuild() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-two-commits:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // Commit 1 (t=1): config.
    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix skosxl: <http://www.w3.org/2008/05/skos-xl#> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:litform> .
            <urn:config:ft:litform> rdf:type f:FullTextProperty .
            <urn:config:ft:litform> f:target skosxl:literalForm .
        }}
    "
    );
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;

    // Commit 2 (t=2): first Label with "Competencies"@en on a URN-style subject.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {
                    "skosxl": "http://www.w3.org/2008/05/skos-xl#",
                    "tm": "https://ns.flur.ee/cust/tm/model/"
                },
                "@id": "tm:concept-1",
                "@type": "skosxl:Label",
                "skosxl:literalForm": {"@value": "Competencies", "@language": "en"}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert first label")
        .ledger;

    // Commit 3 (t=3): unrelated data to advance t.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:unrelated",
                "ex:note": "spacer commit"
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert unrelated")
        .ledger;

    // Commit 4 (t=4): second Label with "Performance Management"@en on a
    // different-namespace subject.
    ledger = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {
                    "skosxl": "http://www.w3.org/2008/05/skos-xl#",
                    "ex": "http://example.org/"
                },
                "@id": "ex:l2",
                "@type": "skosxl:Label",
                "skosxl:literalForm": {"@value": "Performance Management", "@language": "en"}
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert second label")
        .ledger;
    let _ = ledger;

    // Single full reindex — same code path as `fluree reindex` on the server.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // Query BOTH values individually. Both must score.
    let loaded = fluree.ledger(ledger_id).await.expect("load");
    for (term, expect_match) in [("competencies", true), ("performance", true)] {
        let bind = format!("(fulltext ?lit \"{term}\")");
        let query = json!({
            "@context": {"skosxl": "http://www.w3.org/2008/05/skos-xl#"},
            "select": ["?lit", "?score"],
            "where": [
                {"@id": "?ln", "skosxl:literalForm": "?lit"},
                ["bind", "?score", bind],
                ["filter", "(> ?score 0)"]
            ]
        });
        let result = support::query_jsonld(&fluree, &loaded, &query)
            .await
            .expect("query");
        let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
        let rows = json_rows.as_array().expect("rows");
        if expect_match {
            assert!(
                !rows.is_empty(),
                "query for '{term}' must return at least one row; both configured assertions must be in arena after full rebuild: {rows:?}"
            );
        }
    }
}

/// Persisted-backend reproducer for the Solo/S3 bug report (2026-07-03):
/// arena builds correctly at index time, but `fulltext(?v, "…")` returns
/// unbound for every value when the query runs in a *separate process* that
/// loads the index from persisted storage.
///
/// All prior reproducers in this file use `FlureeBuilder::memory()` where the
/// indexer and query side share one process. This test mirrors the field
/// deployment shape: instance A (the "indexing Lambda") writes config + data
/// and publishes a full index to file storage, then is dropped; instance B
/// (the "query Lambda") is a fresh `Fluree` over the same directory that must
/// load the FTA1 arena via the FIR6 root and score both an untagged
/// `xsd:string` and an `@en`-tagged value on the configured predicate.
#[tokio::test]
async fn fulltext_configured_persisted_reload_scores_plain_and_langtagged() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/fulltext-persisted:main";
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // ── Instance A: writer + indexer (the "indexing Lambda") ──────────────
    {
        let fluree = FlureeBuilder::file(&path).build().expect("file fluree");
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");

        // Config: rdfs:label is a fulltext-configured predicate (ncit shape).
        let config_iri = format!("urn:fluree:{ledger_id}#config");
        let config_trig = format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            GRAPH <{config_iri}> {{
                <urn:config:main> rdf:type f:LedgerConfig .
                <urn:config:main> f:fullTextDefaults <urn:config:ft> .
                <urn:config:ft> rdf:type f:FullTextDefaults .
                <urn:config:ft> f:property <urn:config:ft:label> .
                <urn:config:ft:label> rdf:type f:FullTextProperty .
                <urn:config:ft:label> f:target rdfs:label .
            }}
        "
        );
        let ledger = fluree
            .stage_owned(ledger)
            .upsert_turtle(&config_trig)
            .execute()
            .await
            .expect("write config")
            .ledger;

        // Data: both probe shapes from the bug report — untagged xsd:string
        // and @en-tagged — plus filler so the arena has corpus stats.
        let ledger = fluree
            .insert_with_opts(
                ledger,
                &json!({
                    "@context": {
                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                        "ex": "http://ex.test/"
                    },
                    "@graph": [
                        {"@id": "ex:ZZTESTPLAIN", "rdfs:label": "Qjklm Marker Syndrome"},
                        {"@id": "ex:ZZTESTEN",
                         "rdfs:label": {"@value": "Zqxwv Marker Syndrome", "@language": "en"}},
                        {"@id": "ex:OTHER1", "rdfs:label": "Unrelated cardiac disorder"},
                        {"@id": "ex:OTHER2", "rdfs:label": "Another unrelated entry"}
                    ]
                }),
                TxnOpts::default(),
                CommitOpts::default(),
                &no_auto,
            )
            .await
            .expect("insert probes")
            .ledger;
        let _ = ledger;

        // Full reindex — same admin-reindex path the field report used.
        fluree
            .reindex(ledger_id, ReindexOptions::default())
            .await
            .expect("reindex");
    }

    // ── Instance B: fresh process (the "query Lambda") ─────────────────────
    let fluree = FlureeBuilder::file(&path).build().expect("re-open fluree");
    let loaded = fluree.ledger(ledger_id).await.expect("load ledger");

    for (probe, expect_id) in [("qjklm", "ex:ZZTESTPLAIN"), ("zqxwv", "ex:ZZTESTEN")] {
        let bind = format!("(fulltext ?label \"{probe}\")");
        let query = json!({
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "ex": "http://ex.test/"
            },
            "select": ["?s", "?label", "?score"],
            "where": [
                {"@id": "?s", "rdfs:label": "?label"},
                ["bind", "?score", bind],
                ["filter", "(> ?score 0)"]
            ]
        });
        let result = support::query_jsonld(&fluree, &loaded, &query)
            .await
            .expect("query");
        let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
        let rows = json_rows.as_array().cloned().unwrap_or_default();
        let hit = rows.iter().any(|row| {
            row.as_array()
                .and_then(|r| r.first())
                .and_then(|v| v.as_str())
                .is_some_and(|id| id == expect_id)
        });
        assert!(
            hit,
            "persisted reload: fulltext(\"{probe}\") must score {expect_id} > 0 \
             when the arena is loaded from disk in a fresh process, got rows: {rows:?}"
        );
    }
}

/// Shared setup for the persisted-backend tests below: a file-backed Fluree
/// over `path` with `rdfs:label` fulltext-configured and the given docs
/// committed, followed by a full reindex. The instance is dropped before
/// return so a fresh instance sees only persisted state.
async fn persisted_setup_with_labels(path: &str, ledger_id: &str, docs: &JsonValue) {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };
    let fluree = FlureeBuilder::file(path).build().expect("file fluree");
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");

    let config_iri = format!("urn:fluree:{ledger_id}#config");
    let config_trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:property <urn:config:ft:label> .
            <urn:config:ft:label> rdf:type f:FullTextProperty .
            <urn:config:ft:label> f:target rdfs:label .
        }}
    "
    );
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config_trig)
        .execute()
        .await
        .expect("write config")
        .ledger;

    let ledger = fluree
        .insert_with_opts(
            ledger,
            docs,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_auto,
        )
        .await
        .expect("insert docs")
        .ledger;
    let _ = ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
}

/// Run a `fulltext(?label, probe)` query on `rdfs:label` and return the set
/// of matching subject ids (compacted against the `ex:` prefix).
async fn persisted_query_label_hits(
    fluree: &support::MemoryFluree,
    loaded: &support::MemoryLedger,
    probe: &str,
) -> Vec<String> {
    let bind = format!("(fulltext ?label \"{probe}\")");
    let query = json!({
        "@context": {
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "ex": "http://ex.test/"
        },
        "select": ["?s", "?score"],
        "where": [
            {"@id": "?s", "rdfs:label": "?label"},
            ["bind", "?score", bind],
            ["filter", "(> ?score 0)"]
        ]
    });
    let result = support::query_jsonld(fluree, loaded, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&loaded.snapshot).expect("jsonld");
    json_rows
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    row.as_array()?
                        .first()?
                        .as_str()
                        .map(std::string::ToString::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Persisted incremental indexing: after a full reindex + process restart,
/// a new commit on the configured predicate is picked up by the incremental
/// indexer (prior FTA1 arena fetched from persisted storage, extended, and
/// re-published), and a further fresh process scores both old and new values.
#[tokio::test]
async fn fulltext_configured_persisted_incremental_extends_arena() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/fulltext-persisted-incr:main";
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    persisted_setup_with_labels(
        &path,
        ledger_id,
        &json!({
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "ex": "http://ex.test/"
            },
            "@graph": [
                {"@id": "ex:OLD", "rdfs:label": "Qjklm Marker Syndrome"},
                {"@id": "ex:FILLER", "rdfs:label": "Unrelated cardiac disorder"}
            ]
        }),
    )
    .await;

    // Fresh process: commit a new value, then incremental index (prior index
    // exists, so build_index_for_ledger takes the incremental path and must
    // fetch + extend the persisted arena).
    {
        let fluree = FlureeBuilder::file(&path).build().expect("re-open");
        let ledger = fluree.ledger(ledger_id).await.expect("load");
        let ledger = fluree
            .insert_with_opts(
                ledger,
                &json!({
                    "@context": {
                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                        "ex": "http://ex.test/"
                    },
                    "@id": "ex:NEW",
                    "rdfs:label": "Wvbnm Novel Finding"
                }),
                TxnOpts::default(),
                CommitOpts::default(),
                &no_auto,
            )
            .await
            .expect("insert new label")
            .ledger;
        let _ = ledger;

        let idx_config = fluree_db_indexer::IndexerConfig::default()
            .with_fulltext_config_provider(fluree.fulltext_config_provider());
        let result = fluree_db_indexer::build_index_for_ledger(
            fluree.content_store(ledger_id),
            fluree.nameservice(),
            ledger_id,
            idx_config,
        )
        .await
        .expect("incremental build");
        fluree
            .nameservice_mode()
            .publisher()
            .expect("publisher")
            .publish_index_allow_equal(ledger_id, result.index_t, &result.root_id)
            .await
            .expect("publish incremental");
    }

    // Another fresh process: both the pre-existing and the incrementally
    // added value must score from the persisted arena.
    let fluree = FlureeBuilder::file(&path).build().expect("re-open 2");
    let loaded = fluree.ledger(ledger_id).await.expect("load final");
    let old_hits = persisted_query_label_hits(&fluree, &loaded, "qjklm").await;
    assert!(
        old_hits.iter().any(|id| id == "ex:OLD"),
        "incremental index must preserve prior persisted arena docs, got {old_hits:?}"
    );
    let new_hits = persisted_query_label_hits(&fluree, &loaded, "wvbnm").await;
    assert!(
        new_hits.iter().any(|id| id == "ex:NEW"),
        "incremental index must extend the persisted arena with new docs, got {new_hits:?}"
    );
}

/// Persisted novelty/overlay: a commit made AFTER the last index build (and
/// never indexed) must still score via the novelty-delta path when queried
/// from a fresh process — both for the unindexed value itself and without
/// breaking scores for indexed values.
#[tokio::test]
async fn fulltext_configured_persisted_novelty_scores_unindexed_commit() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/fulltext-persisted-novelty:main";
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    persisted_setup_with_labels(
        &path,
        ledger_id,
        &json!({
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "ex": "http://ex.test/"
            },
            "@graph": [
                {"@id": "ex:INDEXED", "rdfs:label": "Qjklm Marker Syndrome"},
                {"@id": "ex:FILLER", "rdfs:label": "Unrelated cardiac disorder"}
            ]
        }),
    )
    .await;

    // Fresh process: commit a new value but do NOT index it.
    {
        let fluree = FlureeBuilder::file(&path).build().expect("re-open");
        let ledger = fluree.ledger(ledger_id).await.expect("load");
        let ledger = fluree
            .insert_with_opts(
                ledger,
                &json!({
                    "@context": {
                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                        "ex": "http://ex.test/"
                    },
                    "@id": "ex:NOVELTY",
                    "rdfs:label": "Xplqr Overlay Finding"
                }),
                TxnOpts::default(),
                CommitOpts::default(),
                &no_auto,
            )
            .await
            .expect("insert novelty label")
            .ledger;
        let _ = ledger;
    }

    // Fresh process: the unindexed (novelty) value must score, and the
    // indexed value must keep scoring.
    let fluree = FlureeBuilder::file(&path).build().expect("re-open 2");
    let loaded = fluree.ledger(ledger_id).await.expect("load with novelty");
    let novelty_hits = persisted_query_label_hits(&fluree, &loaded, "xplqr").await;
    assert!(
        novelty_hits.iter().any(|id| id == "ex:NOVELTY"),
        "unindexed novelty value on a configured predicate must score, got {novelty_hits:?}"
    );
    let indexed_hits = persisted_query_label_hits(&fluree, &loaded, "qjklm").await;
    assert!(
        indexed_hits.iter().any(|id| id == "ex:INDEXED"),
        "indexed value must keep scoring with novelty present, got {indexed_hits:?}"
    );
}

/// Persisted language buckets: language-tagged values on a configured
/// predicate build language-specific arenas — a French value is analyzed
/// with the French stemmer in its own `(g_id, p_id, lang_id)` bucket, and
/// English/untagged values live in the English bucket. Verified across a
/// process restart so the buckets round-trip through the FIR6 root.
#[tokio::test]
async fn fulltext_configured_persisted_language_buckets() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/fulltext-persisted-lang:main";

    persisted_setup_with_labels(
        &path,
        ledger_id,
        &json!({
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "ex": "http://ex.test/"
            },
            "@graph": [
                {"@id": "ex:FR",
                 "rdfs:label": {"@value": "Maladies cardiaques chroniques", "@language": "fr"}},
                {"@id": "ex:EN",
                 "rdfs:label": {"@value": "Chronic heart diseases", "@language": "en"}},
                {"@id": "ex:PLAIN", "rdfs:label": "Chronic kidney disease"}
            ]
        }),
    )
    .await;

    let fluree = FlureeBuilder::file(&path).build().expect("re-open");
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    // French bucket: "maladie" (singular) must match "Maladies" via the
    // French stemmer — this only works if the @fr value was analyzed with
    // the French analyzer in its own arena bucket.
    let fr_hits = persisted_query_label_hits(&fluree, &loaded, "maladie").await;
    assert!(
        fr_hits.iter().any(|id| id == "ex:FR"),
        "@fr value must score via its language-specific arena (French stemming), got {fr_hits:?}"
    );

    // English bucket: "diseases" matches the @en value; the untagged value
    // shares the English bucket, so "disease" matches it too.
    let en_hits = persisted_query_label_hits(&fluree, &loaded, "diseases").await;
    assert!(
        en_hits.iter().any(|id| id == "ex:EN"),
        "@en value must score via the English bucket, got {en_hits:?}"
    );
    assert!(
        en_hits.iter().any(|id| id == "ex:PLAIN"),
        "untagged value must share the English bucket, got {en_hits:?}"
    );

    // Cross-bucket isolation: a French-only term must not surface the
    // English-bucket docs.
    assert!(
        !fr_hits.iter().any(|id| id == "ex:EN" || id == "ex:PLAIN"),
        "French query terms must not match English-bucket docs, got {fr_hits:?}"
    );
}

/// Perf smoke harness for per-row `fulltext()` cost at scale (run manually):
///
/// ```sh
/// cargo test -p fluree-db-api --test grp_query --release \
///     fulltext_perf_50k_labels -- --ignored --nocapture
/// ```
///
/// Seeds 50k configured labels, full-reindexes, then commits ONE extra label
/// WITHOUT indexing so the overlay epoch is non-zero — forcing the eager
/// materialization / `Binding::Lit` path for every row, which is the
/// production norm (the Solo/S3 27s report shape). Prints wall time for
/// cold and warm runs; asserts only correctness (hit counts), not timing.
#[tokio::test]
#[ignore = "perf smoke — run manually with --ignored --nocapture"]
async fn fulltext_perf_50k_labels() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    const N: usize = 50_000;
    let words = [
        "disease",
        "syndrome",
        "carcinoma",
        "gene",
        "protein",
        "receptor",
        "chronic",
        "acute",
        "cardiac",
        "renal",
        "hepatic",
        "neural",
        "benign",
        "malignant",
        "primary",
        "secondary",
        "infection",
        "deficiency",
        "disorder",
        "lesion",
    ];

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/fulltext-perf:main";
    let no_auto = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    {
        let fluree = FlureeBuilder::file(&path).build().expect("file fluree");
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");

        let config_iri = format!("urn:fluree:{ledger_id}#config");
        let config_trig = format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            GRAPH <{config_iri}> {{
                <urn:config:main> rdf:type f:LedgerConfig .
                <urn:config:main> f:fullTextDefaults <urn:config:ft> .
                <urn:config:ft> rdf:type f:FullTextDefaults .
                <urn:config:ft> f:property <urn:config:ft:label> .
                <urn:config:ft:label> rdf:type f:FullTextProperty .
                <urn:config:ft:label> f:target rdfs:label .
            }}
        "
        );
        let mut ledger = fluree
            .stage_owned(ledger)
            .upsert_turtle(&config_trig)
            .execute()
            .await
            .expect("write config")
            .ledger;

        // Bulk-seed N labels via turtle (3 pseudo-random pool words each).
        let mut turtle = String::from("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n");
        for i in 0..N {
            let a = words[i % words.len()];
            let b = words[(i / words.len() + 3) % words.len()];
            let c = words[(i * 7 + 11) % words.len()];
            turtle.push_str(&format!(
                "<http://ex.test/C{i}> rdfs:label \"{a} {b} {c} entry {i}\" .\n"
            ));
        }
        ledger = fluree
            .stage_owned(ledger)
            .upsert_turtle(&turtle)
            .execute()
            .await
            .expect("seed labels")
            .ledger;
        let _ = ledger;

        fluree
            .reindex(ledger_id, ReindexOptions::default())
            .await
            .expect("reindex");

        // One unindexed commit → overlay epoch != 0 → eager materialization.
        let ledger = fluree.ledger(ledger_id).await.expect("reload");
        let _ = fluree
            .insert_with_opts(
                ledger,
                &json!({
                    "@context": {
                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                        "ex": "http://ex.test/"
                    },
                    "@id": "ex:NOVELTY",
                    "rdfs:label": "xplqr overlay entry"
                }),
                TxnOpts::default(),
                CommitOpts::default(),
                &no_auto,
            )
            .await
            .expect("novelty commit");
    }

    let fluree = FlureeBuilder::file(&path).build().expect("re-open");
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    // "disease" appears in a large fraction of the pool-generated labels.
    for run in ["cold", "warm"] {
        let start = std::time::Instant::now();
        let hits = persisted_query_label_hits(&fluree, &loaded, "disease").await;
        let elapsed = start.elapsed();
        println!(
            "fulltext_perf_50k: {run} run: {} hits in {elapsed:?}",
            hits.len()
        );
        assert!(
            hits.len() > 1000,
            "{run}: expected thousands of 'disease' hits, got {}",
            hits.len()
        );
    }
}
