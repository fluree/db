//! Regression test: a cyclic BGP whose edge predicate exists ONLY in novelty
//! (committed after the last index) must not return zero rows.
//!
//! The CyclicBgpOperator's relation loaders used to treat a predicate absent
//! from the base index dictionary (`sid_to_p_id` -> None) as an empty
//! relation. With an active overlay the predicate's facts can live in
//! novelty, so the empty relation zeroed the whole BGP via the
//! empty-driver early exit while the fallback operator tree (whose cursors
//! merge the overlay) found the matches. The loaders now decline the fast
//! path in that case, routing to the fallback.
//!
//! Env mutation lives in ONE test fn (and this file is its own test binary)
//! so parallel test threads can't race on process-global state.

#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    genesis_ledger_for_fluree, normalize_rows, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

#[tokio::test]
async fn cyclic_bgp_novelty_only_predicate_matches_fallback() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/cyclic-bgp-novelty-pred:main";

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

            // Phase 1: p1/p2 legs of two directed triangles plus dangling
            // edges. The closing predicate ex:np is deliberately absent so it
            // never enters the base index dictionary.
            let baseline = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:n1", "ex:p1": {"@id": "ex:n2"}},
                    {"@id": "ex:n2", "ex:p2": {"@id": "ex:n3"}},
                    {"@id": "ex:n4", "ex:p1": {"@id": "ex:n5"}},
                    {"@id": "ex:n5", "ex:p2": {"@id": "ex:n6"}},
                    {"@id": "ex:n10", "ex:p1": {"@id": "ex:n11"}},
                    {"@id": "ex:n12", "ex:p2": {"@id": "ex:n13"}}
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

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;

            // Phase 2: close the triangles with ex:np — novelty only, no
            // reindex. Includes a shortcut edge for the EncodedObject-mode
            // query and a dangler that must not join.
            let novelty = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:n3", "ex:np": {"@id": "ex:n1"}},
                    {"@id": "ex:n6", "ex:np": {"@id": "ex:n4"}},
                    {"@id": "ex:n1", "ex:np": {"@id": "ex:n3"}},
                    {"@id": "ex:n14", "ex:np": {"@id": "ex:n15"}}
                ]
            });
            let result = fluree
                .insert(ledger, &novelty)
                .await
                .expect("novelty insert");
            let _ledger = result.ledger;

            let view = fluree.db(ledger_id).await.expect("load view");

            // Directed triangle exercises RefOnly mode (every object var is
            // also a subject var); the shortcut exercises EncodedObject mode
            // (?c is object-only).
            let queries = [
                (
                    "directed-triangle",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT ?a ?b ?c
                      WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?c ex:np ?a }
                      ORDER BY ?a ?b ?c",
                ),
                (
                    "shortcut-triangle",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT ?a ?b ?c
                      WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?a ex:np ?c }
                      ORDER BY ?a ?b ?c",
                ),
            ];

            for (name, query) in queries {
                // Ground truth: fallback operator tree (overlay-merging cursors).
                std::env::set_var("FLUREE_CYCLIC_BGP", "0");
                let expected = run_query(&fluree, &view, query).await;
                assert!(!expected.is_empty(), "{name}: fallback should produce rows");

                // Cyclic operator enabled (default): the novelty-only closing
                // predicate must decline the fast path, not zero the BGP.
                std::env::remove_var("FLUREE_CYCLIC_BGP");
                let actual = run_query(&fluree, &view, query).await;
                assert_eq!(
                    actual, expected,
                    "{name}: cyclic operator under novelty-only predicate != fallback"
                );
            }
        })
        .await;
}

async fn run_query(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    query: &str,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query(view, QueryInput::Sparql(query))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}
