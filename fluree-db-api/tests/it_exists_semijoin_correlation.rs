//! Regression pin: the EXISTS semijoin fast path must not answer a
//! *correlated* object variable.
//!
//! `ExistsSemijoinKey` is `(subject_var, predicate)` — a cache entry answers
//! "does this subject have any object for this predicate". That is the truth
//! value of `EXISTS { ?s <p> ?o }` only when `?o` is genuinely free. The
//! eligibility gates tested `Term::is_bound()`, which is `!is_var()` — a
//! plan-time syntactic test that rejects a constant object but is blind to a
//! variable the incoming row has already bound. So
//! `NOT EXISTS { ?s2 :member ?x }` was answered as
//! `NOT EXISTS { ?s2 :member ?anything }`, and W3C `sparql11/negation/subset-02`
//! returned 25 solutions instead of 11.
//!
//! Two conditions are needed to observe it, and both are load-bearing here:
//!
//! * The NOT EXISTS must survive lowering as an `Expression::Exists`. A
//!   *standalone* `FILTER (NOT EXISTS {...})` becomes a `Pattern::NotExists`
//!   and routes to `ExistsOperator`, which seeds the subquery from the row and
//!   is correct. Only a NOT EXISTS inside a compound expression (here, one arm
//!   of a `||`) reaches `FilterOperator` and is offered to the semijoin cache.
//!   The MINUS in subset-02 is incidental — `disjunctive_not_exists_*` below is
//!   the same defect with no MINUS at all.
//! * The read must be on an **indexed, overlay-free** view. With live novelty
//!   `fast_path_store` declines and the generic per-row seeded path answers
//!   correctly, which is why `testsuite-sparql` — every ledger of which is
//!   `FlureeBuilder::memory()` — has a green negation register and could never
//!   have caught this.
//!
//! The second condition is why every indexed-lane read here is preceded by a
//! *lane probe*: an uncorrelated EXISTS of the same shape, which must stamp
//! `exists_semijoin` → `proceed`. Without it a slow background indexer turns
//! the whole file green while testing only the novelty lane — and the probe
//! doubles as the must-fire assertion, so a "fix" that stops building the
//! cache altogether fails here.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    genesis_ledger_for_fluree, span_capture, start_background_indexer_local,
    trigger_index_and_wait_outcome, wait_for_index_application,
};

/// `fast-path outcome` site stamped by `build_exists_semijoin_cache`.
const SEMIJOIN_SITE: &str = "exists_semijoin";

/// W3C `sparql11/negation/subset-02.rq`, verbatim.
const SUBSET_02: &str = r"PREFIX :    <http://example/>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (?s1 AS ?subset) (?s2 AS ?superset)
WHERE
{
    ?s2 rdf:type :Set .
    ?s1 rdf:type :Set .

    MINUS {
        ?s1 rdf:type :Set .
        ?s2 rdf:type :Set .
        ?s1 :member ?x .
        FILTER ( ?s1 = ?s2 || NOT EXISTS { ?s2 :member ?x . } )
    }
    MINUS {
        ?s1 rdf:type :Set .
        ?s2 rdf:type :Set .
        FILTER ( NOT EXISTS { ?s1 :member ?y . } )
        FILTER ( NOT EXISTS { ?s2 :member ?y . } )
    }
}";

/// `subset-02.srx`, as (subset, superset) short names: every `(empty, X)` pair
/// except `(empty, empty)`, plus the six proper-subset pairs. Before the fix
/// the indexed lane returned 25 — all 20 off-diagonal pairs plus the five
/// `(empty, X)`.
const SUBSET_02_EXPECTED: &[(&str, &str)] = &[
    ("b", "d"),
    ("c", "a"),
    ("c", "e"),
    ("d", "b"),
    ("e", "a"),
    ("e", "c"),
    ("empty", "a"),
    ("empty", "b"),
    ("empty", "c"),
    ("empty", "d"),
    ("empty", "e"),
];

/// The same correlated NOT EXISTS at the TOP LEVEL — no MINUS, no disjunction.
/// Lowering turns this into a `Pattern::NotExists`, so it never reaches the
/// semijoin; it is the control proving the defect is the *expression* form.
const PLAIN_NOT_EXISTS: &str = r"PREFIX :    <http://example/>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s1 ?x ?s2
WHERE {
    ?s1 :member ?x .
    ?s2 rdf:type :Set .
    FILTER ( NOT EXISTS { ?s2 :member ?x . } )
}";

/// The minimal user-facing repro: no MINUS. The only difference from
/// `PLAIN_NOT_EXISTS` is that NOT EXISTS sits inside a `||`, so it stays an
/// `Expression::Exists` and is offered to the semijoin. 11 solutions satisfy
/// `?s1 = ?s2`, 27 satisfy the correlated NOT EXISTS, and the two are
/// disjoint; the indexed lane returned 22 before the fix.
const DISJUNCTIVE_NOT_EXISTS: &str = r"PREFIX :    <http://example/>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s1 ?x ?s2
WHERE {
    ?s1 :member ?x .
    ?s2 rdf:type :Set .
    FILTER ( ?s1 = ?s2 || NOT EXISTS { ?s2 :member ?x . } )
}";

/// Lane probe / must-fire control: `?free` occurs nowhere but inside the
/// EXISTS, so the object position really is free and the semijoin is entitled
/// to serve it. 6 pairs where `?s1 = ?s2`, plus the 6 where `?s2` is the
/// memberless set, less the one they share.
const UNCORRELATED_EXISTS: &str = r"PREFIX :    <http://example/>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?s1 ?s2
WHERE {
    ?s1 rdf:type :Set .
    ?s2 rdf:type :Set .
    FILTER ( ?s1 = ?s2 || NOT EXISTS { ?s2 :member ?free . } )
}";

const UNCORRELATED_EXPECTED_ROWS: usize = 11;

/// W3C `negation/set-data.ttl` as JSON-LD: five sets over `{1,2,3,9}` plus one
/// empty set.
fn set_data() -> serde_json::Value {
    json!({
        "@context": {"": "http://example/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
        "@graph": [
            {"@id": "http://example/a", "@type": "http://example/Set",
             "http://example/member": [1, 2, 3]},
            {"@id": "http://example/b", "@type": "http://example/Set",
             "http://example/member": [1, 9]},
            {"@id": "http://example/c", "@type": "http://example/Set",
             "http://example/member": [1, 2]},
            {"@id": "http://example/d", "@type": "http://example/Set",
             "http://example/member": [1, 9]},
            {"@id": "http://example/e", "@type": "http://example/Set",
             "http://example/member": [1, 2]},
            {"@id": "http://example/empty", "@type": "http://example/Set"}
        ]
    })
}

/// Reduce solutions to sorted (subset, superset) short-name pairs. Values
/// arrive either as full IRIs or as compacted CURIEs, and rows either as
/// positional arrays or as objects, depending on the formatter.
fn pairs(rows: &[serde_json::Value]) -> Vec<(String, String)> {
    let short = |v: Option<&serde_json::Value>| -> String {
        match v {
            None => "<unbound>".to_string(),
            Some(serde_json::Value::String(s)) => {
                let tail = s.rsplit('/').next().unwrap_or(s);
                tail.trim_start_matches(':').to_string()
            }
            Some(other) => other.to_string(),
        }
    };
    let mut out: Vec<(String, String)> = rows
        .iter()
        .map(|r| match r {
            serde_json::Value::Array(a) => (short(a.first()), short(a.get(1))),
            _ => {
                let obj = r.as_object();
                let get = |k: &str| obj.and_then(|o| o.get(k));
                (short(get("subset")), short(get("superset")))
            }
        })
        .collect();
    out.sort();
    out
}

fn expected_pairs(spec: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = spec
        .iter()
        .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
        .collect();
    out.sort();
    out
}

async fn run(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    q: &str,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query(view, QueryInput::Sparql(q))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    support::normalize_rows(&jsonld)
}

/// Sites that stamped `proceed` after index `from` of the capture log.
fn proceeded_sites(store: &span_capture::SpanStore, from: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[from..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// Assert the view is genuinely on the indexed fast-path lane by running an
/// uncorrelated EXISTS of the same shape and requiring the semijoin to fire.
///
/// Without this the background indexer losing a race silently downgrades every
/// assertion in this file to a novelty-lane assertion, which passes on the
/// unfixed engine.
async fn assert_semijoin_lane_live(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    store: &span_capture::SpanStore,
) {
    // The kill switch does not currently reach this fast path, but if that ever
    // changes the must-fire assertion below would pass vacuously.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — unset it; this test asserts a \
         fast path fires."
    );
    let before = store.find_events("fast-path outcome").len();
    let rows = run(fluree, view, UNCORRELATED_EXISTS).await;
    let sites = proceeded_sites(store, before);
    assert_eq!(
        rows.len(),
        UNCORRELATED_EXPECTED_ROWS,
        "lane probe: uncorrelated NOT EXISTS = 6 self-pairs + 6 memberless-superset \
         pairs, less the one they share"
    );
    assert!(
        sites.iter().any(|s| s == SEMIJOIN_SITE),
        "expected `{SEMIJOIN_SITE}` to proceed for a free object var on this view. \
         Either the view is not on the indexed fast-path lane (so every assertion \
         in this test would be vacuous), or the correlation guard is over-broad \
         and has disabled the semijoin outright. Proceeded: {sites:?}"
    );
}

/// Insert `set_data`, read `query` on the novelty lane, index, then read it
/// again on the indexed lane — checking `expect` on both and requiring the
/// semijoin to decline the (correlated) query on the indexed lane.
async fn on_both_lanes<F>(ledger_id: &'static str, query: &'static str, expect: F)
where
    F: Fn(&'static str, &[serde_json::Value]) + Send + 'static,
{
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();

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
            // `reindex_min_bytes: 0` makes the six-subject fixture index-eligible.
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &set_data(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;

            let view = fluree.db(ledger_id).await.expect("novelty view");
            expect("novelty", &run(&fluree, &view, query).await);

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("indexed view");

            let (store, _tracing_guard) = span_capture::init_test_tracing();
            assert_semijoin_lane_live(&fluree, &view, &store).await;

            let before = store.find_events("fast-path outcome").len();
            let rows = run(&fluree, &view, query).await;
            let sites = proceeded_sites(&store, before);
            assert!(
                !sites.iter().any(|s| s == SEMIJOIN_SITE),
                "`{SEMIJOIN_SITE}` must decline a correlated EXISTS object var; \
                 proceeded: {sites:?}"
            );
            expect("indexed", &rows);
        })
        .await;
}

/// W3C `sparql11/negation/subset-02`: 11 solutions, and the same 11 whether the
/// read is served from novelty or from the merged index.
#[tokio::test(flavor = "current_thread")]
async fn subset_02_returns_eleven_on_both_lanes() {
    let expected = expected_pairs(SUBSET_02_EXPECTED);
    on_both_lanes("audit48/subset02:main", SUBSET_02, move |lane, rows| {
        assert_eq!(
            pairs(rows),
            expected,
            "{lane} lane: subset-02 must return the 11 solutions of subset-02.srx"
        );
    })
    .await;
}

/// Control: a *standalone* `FILTER NOT EXISTS` lowers to `Pattern::NotExists`
/// and never reaches the semijoin. It was correct before the fix and must stay
/// correct — 27 solutions on both lanes.
#[tokio::test(flavor = "current_thread")]
async fn plain_top_level_not_exists_unchanged() {
    on_both_lanes(
        "audit48/plain-notexists:main",
        PLAIN_NOT_EXISTS,
        |lane, rows| {
            assert_eq!(
                rows.len(),
                27,
                "{lane} lane: top-level FILTER NOT EXISTS must return 27 solutions"
            );
        },
    )
    .await;
}

/// The defect with no MINUS involved: NOT EXISTS inside a `||` stays an
/// expression, reaches the semijoin, and returned 22 instead of 38 on the
/// indexed lane.
#[tokio::test(flavor = "current_thread")]
async fn disjunctive_not_exists_returns_thirty_eight_on_both_lanes() {
    on_both_lanes(
        "audit48/disjunctive:main",
        DISJUNCTIVE_NOT_EXISTS,
        |lane, rows| {
            assert_eq!(
                rows.len(),
                38,
                "{lane} lane: `?s1 = ?s2 || NOT EXISTS {{ ?s2 :member ?x }}` must \
                 return 38 solutions"
            );
        },
    )
    .await;
}

/// The must-fire direction on its own ledger: with a free object var the
/// semijoin still serves the query and still returns the right answer. (Every
/// test above runs the same probe against its own view; this one names the
/// contract.)
#[tokio::test(flavor = "current_thread")]
async fn uncorrelated_exists_keeps_semijoin() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "audit48/uncorrelated:main";

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
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &set_data(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;

            let view = fluree.db(ledger_id).await.expect("novelty view");
            let novelty = run(&fluree, &view, UNCORRELATED_EXISTS).await;
            assert_eq!(novelty.len(), UNCORRELATED_EXPECTED_ROWS, "novelty lane");

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("indexed view");

            let (store, _tracing_guard) = span_capture::init_test_tracing();
            assert_semijoin_lane_live(&fluree, &view, &store).await;
        })
        .await;
}
