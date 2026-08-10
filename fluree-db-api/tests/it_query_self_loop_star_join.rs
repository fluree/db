//! Regression pin: a self-loop triple joined to a second pattern on the same
//! subject must execute, not fail with `Duplicate VarId`.
//!
//! `{ ?x <rel> ?x . ?x a ?c }` returned
//! `Query error: Batch error: Duplicate VarId VarId(0) in schema` — a hard
//! query failure on every lane, with or without an aggregate. The scan layer
//! has always handled a repeated variable correctly: `schema_from_pattern_with_emit`
//! folds both positions into one output column and `within_row_var_equality_ok`
//! enforces the implied equality per row. `NestedLoopJoinOperator` built its
//! right-side output vars position by position instead, so a right pattern that
//! names one variable twice contributed two columns with the same `VarId`, and
//! `Batch::new` rejects that schema outright.
//!
//! `{ ?x <rel> ?x . ?x <rel> ?y }` was unaffected, which is why the failure
//! looked arbitrary: the same-predicate shape plans to a different operator and
//! never asks the nested-loop join to widen a repeated variable.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;

/// `rel` has 5 triples of which 2 are self-loops (n1, n2); all four nodes are
/// typed `ex:C`; n1/n2 carry a `score`.
fn fixture() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://ex/"},
        "@graph": [
            {"@id": "ex:n1", "@type": "ex:C", "ex:score": 10,
             "ex:rel": [{"@id": "ex:n1"}, {"@id": "ex:n2"}]},
            {"@id": "ex:n2", "@type": "ex:C", "ex:score": 20,
             "ex:rel": [{"@id": "ex:n2"}, {"@id": "ex:n3"}]},
            {"@id": "ex:n3", "@type": "ex:C", "ex:rel": {"@id": "ex:n4"}},
            {"@id": "ex:n4", "@type": "ex:C"}
        ]
    })
}

/// `(query, expected row count)`. Only n1 and n2 are self-related, and both are
/// typed and scored, so every star join over the self-loop yields 2 — except
/// the same-predicate control, where the second leg is free and n1/n2 have two
/// `rel` objects each.
const CASES: &[(&str, usize)] = &[
    ("SELECT ?x ?c WHERE { ?x <http://ex/rel> ?x . ?x a ?c }", 2),
    (
        "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x . ?x a ?c }",
        1,
    ),
    (
        "SELECT ?x ?v WHERE { ?x <http://ex/rel> ?x . ?x <http://ex/score> ?v }",
        2,
    ),
    // Reversed leg order: the self-loop is now the RIGHT side of the join with
    // a non-empty left schema.
    ("SELECT ?x ?c WHERE { ?x a ?c . ?x <http://ex/rel> ?x }", 2),
    // Repeated variable across subject and PREDICATE, joined to a second leg.
    ("SELECT ?x ?c WHERE { ?x ?x ?o . ?x a ?c }", 0),
    // Control: same predicate on both legs — this shape always worked.
    (
        "SELECT ?x ?y WHERE { ?x <http://ex/rel> ?x . ?x <http://ex/rel> ?y }",
        4,
    ),
];

/// The aggregate case above projects one row; assert its value too, so a fix
/// that merely stops erroring (by dropping the equality constraint) still fails.
const SELF_LOOP_STAR_COUNT: &str =
    "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x . ?x a ?c }";

async fn seed_memory(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(ledger, &fixture())
        .await
        .expect("insert")
        .ledger
}

/// Pull the count out of a one-row, one-column result. `to_jsonld` renders a
/// projected SELECT as positional arrays; accept the object shape too so a
/// formatter change surfaces as a failed assertion rather than a panic here.
fn single_count(rows: &[serde_json::Value]) -> i64 {
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one aggregate row: {rows:?}"
    );
    let cell = match &rows[0] {
        serde_json::Value::Array(a) => a.first(),
        other => other.get("n"),
    };
    cell.and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("no integer count in {rows:?}"))
}

async fn run(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    q: &str,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query(view, QueryInput::Sparql(q))
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}

#[tokio::test(flavor = "current_thread")]
async fn self_loop_star_join_novelty_lane() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_memory(&fluree, "selfloop/star-novelty:main").await;

    for (q, expected) in CASES {
        let view = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let rows = run(&fluree, &view, q).await;
        assert_eq!(rows.len(), *expected, "novelty lane: {q}");
    }

    let view = fluree_db_api::GraphDb::from_ledger_state(&ledger);
    let rows = run(&fluree, &view, SELF_LOOP_STAR_COUNT).await;
    assert_eq!(
        single_count(&rows),
        2,
        "novelty lane: only n1 and n2 are self-related"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn self_loop_star_join_indexed_lane() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "selfloop/star-indexed:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &fixture(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;

            support::trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            support::wait_for_index_application(&fluree, ledger_id, ledger.t()).await;

            for (q, expected) in CASES {
                let view = fluree.db(ledger_id).await.expect("indexed view");
                let rows = run(&fluree, &view, q).await;
                assert_eq!(rows.len(), *expected, "indexed lane: {q}");
            }

            let view = fluree.db(ledger_id).await.expect("indexed view");
            let rows = run(&fluree, &view, SELF_LOOP_STAR_COUNT).await;
            assert_eq!(
                single_count(&rows),
                2,
                "indexed lane: only n1 and n2 are self-related"
            );
        })
        .await;
}
