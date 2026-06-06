//! Synthetic reproduction for the Fluree-vs-Others COUNT(*) divergence on
//! sequence paths `?s p1/p2+ ?o`  (DBLP-KG: subStream/relatedStream+).
//!
//! Goal: isolate whether Fluree counts DISTINCT (?s,?o) pairs vs. the full
//! BGP-join cardinality that binds the intermediate `?mid`. We run the SAME
//! queries on BOTH the in-memory novelty path (generic PropertyPathOperator)
//! AND the indexed/binary path (transitive-path+ COUNT(*) fast-path) so any
//! divergence between the two engines inside Fluree is exposed directly.
//!
//! Graph construction (ex: = http://example.org/):
//!   p1 = ex:p1 (plain predicate, the "subStream" analog)
//!   p2 = ex:p2 (transitive predicate, the "relatedStream+" analog)
//!
//!   s --p1--> m1 , s --p1--> m2     (two distinct intermediates from one subject)
//!   m1 --p2--> o , m1 --p2--> a
//!   m2 --p2--> o
//!   a  --p2--> b
//!   b  --p2--> o , b  --p2--> a      (CYCLE a<->b)
//!
//! Hand-computed p2+ reachability (one-or-more, deduped per source):
//!   reach+(m1) = { o, a, b }     reach+(m2) = { o }
//!   reach+(a)  = { b, o, a }     reach+(b)  = { a, o, b }   reach+(o) = { }
//!
//! Two counting units for the sequence path { s p1/p2+ ?o }:
//!   distinct-(?s,?o) pairs : union over intermediates = {o,a,b} => 3  (the OLD bug)
//!   bind-?mid BGP join     : reach+(m1)=3 + reach+(m2)=1     => 4  (spec-correct)
//! These DISTINGUISH the semantics. Per SPARQL 1.1 §18.2.2.4 the sequence lowers to a
//! Join over the fresh path-internal variable ?mid, which PRESERVES multiplicity, so
//! COUNT(*) = 4 (an ?o reachable via two ?mid is counted twice). This test now ASSERTS
//! the bind-?mid count on BOTH the in-memory novelty path AND the indexed fast path —
//! a regression guard for:
//!   - Defect 1: the transitive-path+ COUNT(*) fast path counted distinct (?s,?o) pairs.
//!   - Defect 2: the generic PropertyPathOperator failed to resolve `Binding::EncodedSid`
//!     intermediates (bound-subject + indexed), pairing each ?mid with the full closure.

#![cfg(feature = "native")]

mod support;

use std::sync::Arc;

use fluree_db_api::{FlureeBuilder, GraphDb, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_core::LedgerSnapshot;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    genesis_ledger, genesis_ledger_for_fluree, start_background_indexer_local,
    trigger_index_and_wait_outcome, MemoryFluree, MemoryLedger,
};

const PREFIX: &str = "PREFIX ex: <http://example.org/>";

fn graph() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:s",  "ex:p1": [{"@id": "ex:m1"}, {"@id": "ex:m2"}]},
            {"@id": "ex:m1", "ex:p2": [{"@id": "ex:o"}, {"@id": "ex:a"}]},
            {"@id": "ex:m2", "ex:p2": {"@id": "ex:o"}},
            {"@id": "ex:a",  "ex:p2": {"@id": "ex:b"}},
            {"@id": "ex:b",  "ex:p2": [{"@id": "ex:o"}, {"@id": "ex:a"}]}
        ]
    })
}

async fn seed_novelty(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree.insert(ledger0, &graph()).await.unwrap().ledger
}

/// Read a single COUNT scalar out of the `[[n]]` JSON-LD result shape.
fn count_of(jsonld: &serde_json::Value) -> i64 {
    jsonld
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|cols| cols.first())
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("unexpected count shape: {jsonld}"))
}

/// The six queries we probe. Returns (label, sparql, distinct_pair, bind_mid).
fn probes() -> Vec<(&'static str, String, i64, i64)> {
    vec![
        (
            "A1 m1 p2+",
            format!("{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ex:m1 ex:p2+ ?o }}"),
            3,
            3,
        ),
        (
            "A2 m2 p2+",
            format!("{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ex:m2 ex:p2+ ?o }}"),
            1,
            1,
        ),
        (
            "B  ?x p2+ ?o",
            format!("{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ?x ex:p2+ ?o }}"),
            10,
            10,
        ),
        (
            "C  s p1/p2+ ?o",
            format!("{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ex:s ex:p1/ex:p2+ ?o }}"),
            3,
            4,
        ),
        (
            "D  s p1 ?mid . ?mid p2+ ?o",
            format!(
                "{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ex:s ex:p1 ?mid . ?mid ex:p2+ ?o }}"
            ),
            3,
            4,
        ),
        (
            "F  ?s p1/p2+ ?o",
            format!("{PREFIX}\nSELECT (COUNT(*) AS ?c) WHERE {{ ?s ex:p1/ex:p2+ ?o }}"),
            3,
            4,
        ),
    ]
}

async fn run_all(label: &str, fluree: &MemoryFluree, db: &GraphDb, snapshot: &LedgerSnapshot) {
    for (name, q, dp, bm) in probes() {
        let result = fluree
            .query(db, QueryInput::Sparql(&q))
            .await
            .unwrap_or_else(|e| panic!("[{label}] query `{name}` failed: {e}"));
        let jsonld = result.to_jsonld(snapshot).expect("to_jsonld");
        let actual = count_of(&jsonld);
        // COUNT(*) must equal the spec-correct bind-?mid join cardinality on BOTH the
        // novelty (generic operator) and indexed (fast path) engines.
        assert_eq!(
            actual, bm,
            "[{label}] `{name}`: COUNT(*) must equal the bind-?mid join cardinality \
             {bm} (got {actual}); sequence p1/p2+ preserves multiplicity over the \
             path-internal variable per SPARQL 1.1 §18.2.2.4"
        );
        // Where the two units differ, prove we did NOT regress to the old distinct-pair
        // count (Defect 1) or to the full-closure garbage (Defect 2).
        if dp != bm {
            assert_ne!(
                actual, dp,
                "[{label}] `{name}`: regressed to the old distinct-(?s,?o) count {dp}"
            );
        }
    }
}

#[tokio::test]
async fn seq_path_count_unit_novelty_vs_indexed() {
    // ---------- 1) In-memory novelty path (generic operators) ----------
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "seqpath/novelty:main").await;
    let db = support::graphdb_from_ledger(&ledger);

    // Sanity: enumerate reach+(m1)/reach+(m2) to anchor the hand computation.
    for (subj, expect) in [("ex:m1", "[ex:a, ex:b, ex:o]"), ("ex:m2", "[ex:o]")] {
        let rows = fluree
            .query(
                &db,
                QueryInput::Sparql(&format!(
                    "{PREFIX}\nSELECT ?o WHERE {{ {subj} ex:p2+ ?o }} ORDER BY ?o"
                )),
            )
            .await
            .unwrap()
            .to_jsonld(&ledger.snapshot)
            .unwrap();
        let set: Vec<String> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r[0].as_str().unwrap().to_string())
            .collect();
        println!("reach+({subj}) = {set:?}  (expected {expect})");
    }

    run_all(
        "NOVELTY (in-memory generic path)",
        &fluree,
        &db,
        &ledger.snapshot,
    )
    .await;

    // ---------- 2) Indexed / binary path (fast-path operators) ----------
    let fluree2 = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "seqpath/indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree2.backend().clone(),
        Arc::new(fluree2.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger0 = genesis_ledger_for_fluree(&fluree2, ledger_id);
            let res = fluree2
                .insert_with_opts(
                    ledger0,
                    &graph(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert (indexed)");
            let ledger_i = res.ledger;

            let _ = trigger_index_and_wait_outcome(&handle, ledger_id, ledger_i.t()).await;

            // Load the indexed view so binary_store is threaded -> fast path eligible.
            let view = fluree2.db(ledger_id).await.expect("load indexed view");

            run_all(
                "INDEXED (binary fast-path)",
                &fluree2,
                &view,
                &view.snapshot,
            )
            .await;

            // Enumerate the ACTUAL solution rows the indexed engine produces for the
            // fixed-subject sequence path, to explain the COUNT(*) number.
            println!("\n---- INDEXED enumeration: ex:s ex:p1/ex:p2+ ?o (SELECT ?o) ----");
            let rows = fluree2
                .query(
                    &view,
                    QueryInput::Sparql(&format!(
                        "{PREFIX}\nSELECT ?o WHERE {{ ex:s ex:p1/ex:p2+ ?o }} ORDER BY ?o"
                    )),
                )
                .await
                .unwrap()
                .to_jsonld(&view.snapshot)
                .unwrap();
            println!("  rows ({}) = {rows}", rows.as_array().map_or(0, Vec::len));

            println!(
                "\n---- INDEXED enumeration: ex:s ex:p1 ?mid . ?mid ex:p2+ ?o (SELECT ?mid ?o) ----"
            );
            let rows2 = fluree2
                .query(
                    &view,
                    QueryInput::Sparql(&format!(
                        "{PREFIX}\nSELECT ?mid ?o WHERE {{ ex:s ex:p1 ?mid . ?mid ex:p2+ ?o }} ORDER BY ?mid ?o"
                    )),
                )
                .await
                .unwrap()
                .to_jsonld(&view.snapshot)
                .unwrap();
            println!(
                "  rows ({}) = {rows2}",
                rows2.as_array().map_or(0, Vec::len)
            );

            // SELECT-DISTINCT variants to see what the engine considers distinct.
            println!("\n---- INDEXED: SELECT DISTINCT ?o WHERE {{ ex:s ex:p1/ex:p2+ ?o }} ----");
            let rows3 = fluree2
                .query(
                    &view,
                    QueryInput::Sparql(&format!(
                        "{PREFIX}\nSELECT DISTINCT ?o WHERE {{ ex:s ex:p1/ex:p2+ ?o }} ORDER BY ?o"
                    )),
                )
                .await
                .unwrap()
                .to_jsonld(&view.snapshot)
                .unwrap();
            println!(
                "  rows ({}) = {rows3}",
                rows3.as_array().map_or(0, Vec::len)
            );
        })
        .await;
}
