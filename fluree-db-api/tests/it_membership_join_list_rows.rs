//! Membership-join lane at or above its driving-size gate (#1687).
//!
//! `MembershipJoinOperator` answers a right triple that binds nothing new as
//! a hash keep/drop, on the premise that a fully-ground triple matches at
//! most once. RDF list rows break that premise inside a single graph: the
//! same `(s, p, o)` recurs at multiple `o_i` positions, so the generic join
//! emits one row per matching flake where keep/drop emits one per driving
//! row. Below `MEMBERSHIP_JOIN_MIN_DRIVING` (256) the planner already
//! declines; at or above it the lane used to answer the shape as a semi-join
//! and silently diverge from the generic pipeline (3 vs 5 on the #1652
//! fixture — a scale-dependent wrong answer).
//!
//! Two cases, both sized past the gate so the planner genuinely admits the
//! lane, with routing asserted via the lane's `membership-join` stamps so
//! neither can silently degrade into a below-gate case:
//!
//! * **List rows** — the build drain detects duplicate ground rows, routes
//!   every row through the exact per-row fallback, and the lane's answer
//!   equals the generic pipeline's. Must NOT stamp `proceed`; must stamp
//!   `fallback:gate_declined` (proof the lane was reached at all).
//! * **Ordinary multi-valued rows** — the perf guard: the decline is
//!   surgical, so a non-list shape of the same size keeps the hash path.
//!   Must stamp `proceed`.
//!
//! Phase 2 reruns both queries under `FLUREE_DISABLE_QUERY_FAST_PATHS`
//! (which now reaches this lane) and requires identical rows — the
//! join-equivalence pin the lane's module docs claim.
//!
//! Own test binary: toggles the process-global kill switch AND asserts
//! fast-path routing via span capture, so it must not share a process with
//! other tests.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, FormatterConfig};
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";
const SITE: &str = "membership-join";
const QUERY: &str = "SELECT ?s ?o WHERE { ?s ex:createdBy ?o . ?s ex:authoredBy ?o }";

const N_DUP: usize = 128;
const N_PLAIN: usize = 20;
const N_ORDINARY: usize = 140;

/// List ledger: every subject carries 2-element `@list` values on both
/// predicates, so the driving side is `(N_DUP + N_PLAIN) × 2 = 296` rows —
/// past the planner's 256-row gate.
///
/// * `s-dup-i`: `createdBy [v-i, v-i]`, `authoredBy [v-i, v-i]`. The generic
///   join pairs each of the 2 driving rows with each of the 2 matching
///   flakes → 4 rows per subject; keep/drop would emit 2.
/// * `s-pln-i`: `createdBy [a-i, b-i]`, `authoredBy [a-i, c-i]` — one shared
///   value, all distinct within each list → 1 row per subject either way.
///
/// Generic (correct) total: 128×4 + 20×1 = 532. The old semi-join answer
/// was 128×2 + 20×1 = 276.
fn list_graph() -> Value {
    let mut nodes = Vec::new();
    for i in 0..N_DUP {
        nodes.push(json!({
            "@id": format!("ex:s-dup-{i:03}"),
            "ex:createdBy": [format!("v-{i}"), format!("v-{i}")],
            "ex:authoredBy": [format!("v-{i}"), format!("v-{i}")]
        }));
    }
    for i in 0..N_PLAIN {
        nodes.push(json!({
            "@id": format!("ex:s-pln-{i:03}"),
            "ex:createdBy": [format!("a-{i}"), format!("b-{i}")],
            "ex:authoredBy": [format!("a-{i}"), format!("c-{i}")]
        }));
    }
    json!({
        "@context": [
            {"ex": "http://example.org/ns/"},
            {"ex:createdBy": {"@container": "@list"},
             "ex:authoredBy": {"@container": "@list"}}
        ],
        "@graph": nodes
    })
}

/// Ordinary ledger: the same size and join shape with plain multi-valued
/// (non-list) predicates — every ground triple matches at most once, so the
/// hash path is join-equivalent and must keep firing. Driving side is
/// `140 × 2 = 280` rows; each subject shares exactly one value across the
/// two predicates → 140 result rows.
fn ordinary_graph() -> Value {
    let nodes: Vec<Value> = (0..N_ORDINARY)
        .map(|i| {
            json!({
                "@id": format!("ex:t-{i:03}"),
                "ex:createdBy": [format!("x-{i}"), format!("shared-{i}")],
                "ex:authoredBy": [format!("shared-{i}"), format!("y-{i}")]
            })
        })
        .collect();
    json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": nodes})
}

/// Insert + reindex so the ledger has statistics — `membership_join_key_vars`
/// requires stats to bound the build side, so an unindexed ledger would keep
/// the lane out of the plan and every assertion below would be vacuous.
async fn setup(alias: &str, data: &Value) -> (tempfile::TempDir, Fluree) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let ledger = fluree.create_ledger(alias).await.expect("create_ledger");
    fluree.insert(ledger, data).await.expect("insert");
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    (dir, fluree)
}

async fn run_query(fluree: &Fluree, alias: &str) -> Vec<String> {
    let full = format!("{PREFIX}{QUERY}");
    let snapshot = fluree.graph(alias).load().await.expect("load");
    let rows = snapshot
        .query()
        .sparql(&full)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("query");
    let mut out: Vec<String> = rows
        .as_array()
        .expect("array of rows")
        .iter()
        .map(|r| serde_json::to_string(r).expect("serialize row"))
        .collect();
    out.sort();
    out
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn membership_join_agrees_with_generic_on_list_rows_and_keeps_firing() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast phase below would \
         run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let (_d1, list) = setup("mj1687:list", &list_graph()).await;
    let (_d2, ordinary) = setup("mj1687:ordinary", &ordinary_graph()).await;

    // Phase 1 — fast paths on, under span capture so engagement is proven,
    // not assumed (a fixture that quietly lands below the driving-size gate
    // would otherwise pass without ever exercising the lane).
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);

    let before = store.find_events("fast-path outcome").len();
    let list_fast = run_query(&list, "mj1687:list").await;
    let list_outcomes: Vec<String> = store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(SITE))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect();

    let before = store.find_events("fast-path outcome").len();
    let ordinary_fast = run_query(&ordinary, "mj1687:ordinary").await;
    let ordinary_outcomes: Vec<String> = store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(SITE))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect();
    drop(tracing_guard);

    // Phase 2 — the generic reference: the kill switch keeps the lane out
    // of the plan, so the nested-loop join answers.
    set_fast_paths_disabled(true);
    let list_generic = run_query(&list, "mj1687:list").await;
    let ordinary_generic = run_query(&ordinary, "mj1687:ordinary").await;
    set_fast_paths_disabled(false);

    // List rows: identical answers, with the lane engaged and declining.
    assert_eq!(
        list_fast.len(),
        N_DUP * 4 + N_PLAIN,
        "list rows: the generic bag join pairs each duplicate driving row \
         with each matching flake (got {} rows)",
        list_fast.len()
    );
    assert_eq!(
        list_fast, list_generic,
        "list rows: the membership lane's answer diverged from the generic \
         pipeline's — its join-equivalence invariant is broken again"
    );
    assert!(
        list_outcomes.iter().any(|o| o == "fallback:gate_declined"),
        "list rows: expected the lane to be reached and decline its hash \
         path — no `{SITE}` gate_declined stamp means the fixture no longer \
         engages the lane and this test pins nothing [outcomes: {list_outcomes:?}]"
    );
    assert!(
        !list_outcomes.iter().any(|o| o == "proceed"),
        "list rows: the hash keep/drop path served a shape whose ground \
         triples match more than once [outcomes: {list_outcomes:?}]"
    );

    // Ordinary rows: the perf guard — the decline must be surgical.
    assert_eq!(
        ordinary_fast.len(),
        N_ORDINARY,
        "ordinary rows: one shared value per subject (got {} rows)",
        ordinary_fast.len()
    );
    assert_eq!(
        ordinary_fast, ordinary_generic,
        "ordinary rows: fast and generic answers must agree"
    );
    assert!(
        ordinary_outcomes.iter().any(|o| o == "proceed"),
        "ordinary rows: expected the hash membership path to keep firing on \
         a non-list shape of the same size — the list decline must not \
         disable the lane [outcomes: {ordinary_outcomes:?}]"
    );
}
