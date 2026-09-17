//! A threshold on an annotation body must reduce scan work.
//!
//! `expand_edge_annotation_patterns` wraps the expanded chain in
//! `Pattern::DefaultGraphSource`, and `collect_inner_join_block` stops at any
//! non-triple pattern. A `FILTER` written beside the annotation therefore
//! started a block with no triples in it, `extract_bounds_from_filters` never
//! saw the body's object variable, and the threshold ran as a `FilterOperator`
//! *above* the wrapper — after every annotation had been read and
//! materialized. Filtering by confidence cost exactly what not filtering cost:
//! on a 60k-edge ledger, `?c > 0.7` (18,317 rows) and `?c > 0.97` (1,148 rows)
//! both burned 321.03 fuel, the same as no filter at all.
//!
//! Two stamps, because a test that only checked row counts would pass against
//! the bug it is meant to pin — the bug never changed an answer.
//!
//! 1. **Structural**: `--explain` must report `body-filters: 1` on the
//!    `DefaultGraphSourceOperator`, i.e. the threshold is *inside* the chain
//!    where the block builder can see it. Before the rewrite this is 0.
//! 2. **Effect**: with the lane pinned to `chain`, a tighter threshold must
//!    burn strictly less fuel than no threshold. Fuel charges rows the scan
//!    emits and never charges rows an encoded pre-filter drops inside the
//!    cursor (`binary_scan.rs`), so it measures the thing under test, is
//!    bit-identical across runs, and cannot be moved by load on the build box.
//!    Before the rewrite these were equal to the last decimal place.
//!
//! The lane is pinned deliberately. The `arena` lane the planner currently
//! prefers drives from the base edge and probes per row, and does not exploit
//! object bounds on the resulting bound-subject body lookup — a separate
//! defect, tracked with the lane-selection issue. Pinning `chain` runs the
//! generic path, which plans the whole chain as one block — that is where the
//! bounds bite. Note the naming: `ChainLane::Chain` means "neither arena nor
//! enumerate" and the engine reports the execution as `generic`, so seeing
//! `lane="generic"` after pinning `chain` is expected and not a demotion.
//! Pinning is what keeps this test from passing by silently taking a lane that
//! never had the problem — and the fired-lane assertion is what keeps the
//! pinning itself honest.
//!
//! Which check guards what, since three of them look redundant and are not:
//! stamp 1 pins that the threshold is planned inside the chain; stamp 2
//! (`pinned_kept_fuel < pinned_all_fuel`) catches a dropped override, because a
//! demoted arm cannot show the threshold reducing scan work; and the fired-lane
//! assertion in the loop is the only one that certifies *which* lane ran.
//!
//! Twin surfaces: SPARQL and JSON-LD share the IR, and the rewrite lives in
//! `fluree-db-query`, so both are covered here.

#![cfg(feature = "native")]

use crate::support;
use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value as JsonValue};

/// Edges in the fixture. Large enough that the per-row charges the pushdown
/// removes dominate the fixed leaflet charge that it does not.
const EDGES: usize = 3_000;

/// `?c > THRESHOLD` keeps `ex:p{i}` for `i/10000 > 0.25`, i.e. `i >= 2501`:
/// 499 of the 3,000 edges. Hand-computed, not read back from the engine.
const THRESHOLD: f64 = 0.25;
const KEPT: usize = 499;

/// `?c > 0.25 && ?o != ex:p2600` — edge `i` has object `ex:p{i+1}`, so this
/// excludes exactly `i == 2599`, whose confidence 0.2599 is above the
/// threshold. 499 - 1. Hand-computed.
const KEPT_EXCLUDING_ONE_OBJECT: usize = 498;

fn ctx() -> JsonValue {
    json!({ "ex": "http://example.org/" })
}

/// `ex:p{i} ex:knows ex:p{i+1} {| ex:confidence i/10000 |}` for i in 0..EDGES.
/// Confidence rises monotonically with the subject index so the kept set is a
/// contiguous tail and the expected count is arithmetic, not empirical.
fn seed_graph() -> JsonValue {
    let rows: Vec<JsonValue> = (0..EDGES)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "ex:knows": {
                    "@id": format!("ex:p{}", i + 1),
                    "@annotation": {
                        "@id": format!("ex:claim{i}"),
                        "ex:confidence": { "@value": i as f64 / 10_000.0, "@type": "xsd:double" }
                    }
                }
            })
        })
        .collect();
    json!({
        "@context": { "ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#" },
        "@graph": rows
    })
}

fn annotated_count_sparql(filter: Option<f64>) -> String {
    let f = filter.map_or(String::new(), |t| format!("FILTER(?c > {t})"));
    format!(
        "PREFIX ex: <http://example.org/>
         SELECT (COUNT(*) AS ?n) WHERE {{
           ?s ex:knows ?o {{| ex:confidence ?c |}} {f}
         }}"
    )
}

/// The same COUNT, with the base **object** variable also in the filter. This
/// is the shape that couples the sink to `elide_redundant_chain`: `?o` is not
/// projected and not read outside the wrapper, so elision is free to drop the
/// `f:reifiesObject` lookup — and `?o` survives only because `collect_var_stats`
/// walks `Pattern::Filter` and puts it back in the referenced set. If that walk
/// ever goes away, `?o` unbinds inside the chain and every row drops silently.
fn annotated_count_with_object_var_sparql() -> String {
    format!(
        "PREFIX ex: <http://example.org/>
         SELECT (COUNT(*) AS ?n) WHERE {{
           ?s ex:knows ?o {{| ex:confidence ?c |}}
           FILTER(?c > {THRESHOLD} && ?o != ex:p2600)
         }}"
    )
}

fn annotated_rows_jsonld(filter: Option<f64>) -> JsonValue {
    let mut where_clause = vec![json!({
        "@id": "?s",
        "ex:knows": { "@id": "?o", "@annotation": { "ex:confidence": "?c" } }
    })];
    if let Some(t) = filter {
        where_clause.push(json!(["filter", format!("(> ?c {t})")]));
    }
    json!({
        "@context": ctx(),
        "select": ["?s", "?o", "?c"],
        "where": where_clause
    })
}

/// COUNT and fuel for one SPARQL query, through the tracked builder.
async fn sparql_count_and_fuel(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> (u64, f64) {
    let db = support::graphdb_from_ledger(ledger);
    let tracked = db
        .query(fluree)
        .sparql(sparql)
        .execute_tracked()
        .await
        .expect("tracked sparql query");
    let n = tracked.result["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .expect("COUNT binding")
        .parse::<u64>()
        .expect("COUNT is an integer");
    (n, tracked.fuel.expect("fuel must be tracked"))
}

/// Row count and fuel for one JSON-LD query.
async fn jsonld_rows_and_fuel(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &JsonValue,
) -> (usize, f64) {
    let tracked = support::query_jsonld_tracked(fluree, ledger, query)
        .await
        .expect("tracked jsonld query");
    let n = tracked
        .result
        .as_array()
        .expect("select rows are an array")
        .len();
    (n, tracked.fuel.expect("fuel must be tracked"))
}

#[tokio::test]
async fn annotation_body_threshold_reduces_scan_work_on_both_surfaces() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/annotation-filter-pushdown:threshold";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after = fluree
                .insert(ledger0, &seed_graph())
                .await
                .expect("annotated insert");
            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");
            support::trigger_index_and_wait(&handle, ledger_id, after.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;
            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert!(
                post.snapshot.annotation_index.is_some(),
                "fixture must have a sealed arena, or this exercises a different lane"
            );

            // ---- correctness first -------------------------------------
            let (all, _) =
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(None)).await;
            let (kept, _) =
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                    .await;
            assert_eq!(all as usize, EDGES, "unfiltered count");
            assert_eq!(kept as usize, KEPT, "thresholded count");

            // The same answer with every fast path disabled. This is a weak
            // oracle on its own (a defect in the plan both lanes share is
            // invisible to it), which is why the fuel assertion below is the
            // one that pins the behaviour.
            let generic = {
                let _g = DisableFastPaths::set();
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                    .await
                    .0
            };
            assert_eq!(generic, kept, "fast-path-disabled lane must agree");

            // ---- stamp 1: the threshold is INSIDE the chain -------------
            // `body-filters` counts `Pattern::Filter`s in the chain body. It
            // is 0 without the rewrite: the FILTER stays a sibling of the
            // wrapper, where no block builder can reach it.
            let db = support::graphdb_from_ledger(&post);
            let explained = fluree
                .explain(&db, &annotated_rows_jsonld(Some(THRESHOLD)))
                .await
                .expect("explain");
            let wrapper = find_op(&explained["plan"]["physical"], "DefaultGraphSourceOperator")
                .expect("the annotated BGP must appear as a DefaultGraphSourceOperator");
            assert_eq!(
                wrapper["details"]["kind"], "edge-annotation",
                "explain must name the chain: {explained}"
            );
            assert_eq!(
                wrapper["details"]["body-filters"], 1,
                "the threshold must be planned inside the chain body: {explained}"
            );

            // ---- stamp 2: it reaches the scan ---------------------------
            // Pinned to `chain` (which executes as `generic`); module header for why.
            let (pinned_all, pinned_all_fuel, pinned_kept, pinned_kept_fuel) = {
                let _lane = LanePin::chain();
                let (a, af) =
                    sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(None)).await;
                let (k, kf) =
                    sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                        .await;
                (a, af, k, kf)
            };
            assert_eq!(pinned_all as usize, EDGES, "chain pin, unfiltered count");
            assert_eq!(pinned_kept as usize, KEPT, "chain pin, thresholded count");
            assert!(
                pinned_kept_fuel < pinned_all_fuel,
                "a threshold keeping {KEPT}/{EDGES} rows must cut scan work, not just \
                 rows in the answer: unfiltered {pinned_all_fuel}, filtered \
                 {pinned_kept_fuel} (these were EQUAL before the rewrite)"
            );

            // ---- the cross-module invariant the sink creates ------------
            // A filter naming the base OBJECT variable passes the sink gate,
            // because `?o` is produced by the base triple inside the wrapper.
            // It then lands in a chain whose `f:reifiesObject` lookup is an
            // elision candidate: with a pure COUNT, `?o` is in neither the
            // projection nor `needed_outside`, so `elide_redundant_chain`
            // (`default_graph_source.rs`) may drop that lookup. `?o` stays
            // bound only because `collect_var_stats` (`where_plan.rs`) walks
            // `Pattern::Filter` into the referenced set.
            //
            // That traversal predates this rewrite and nothing else connects
            // the two modules, so delete it and every row here drops silently
            // with no other test going red. This pins it, on every lane.
            // (pin, the lane the engine actually reports for that pin).
            //
            // `chain` maps to `generic`, and that is not a demotion: there is no
            // execution branch keyed on `ChainLane::Chain`. The variant means only
            // "neither arena nor enumerate", after which control reaches the hash
            // branch (admitted at >= 256 driving rows) and otherwise the generic
            // chain. Pinning `chain` therefore cannot produce a `chain`-labelled
            // execution on any shape. Encoding that here rather than in prose is
            // the point: if it ever changes, this fails.
            let (store, _tracing_guard) = support::span_capture::init_test_tracing();
            for (lane, expected_fired) in [
                ("arena", "arena"),
                ("enumerate", "enumerate"),
                ("chain", "generic"),
            ] {
                let _pin = LanePin::lane(lane);
                let before = store.find_events("annotation delegate lane").len();
                let (n, obj_fuel) = sparql_count_and_fuel(
                    &fluree,
                    &post,
                    &annotated_count_with_object_var_sparql(),
                )
                .await;
                assert_eq!(
                    n as usize, KEPT_EXCLUDING_ONE_OBJECT,
                    "lane={lane}: the base object variable must stay bound inside \
                     the chain — 0 here means `f:reifiesObject` was elided while \
                     the sunk filter still reads `?o`"
                );
                // The plain threshold on the same lane, so `enumerate` (which
                // neither the default lane nor the chain pin exercises) has a
                // filter-carrying correctness assertion too.
                let (plain, _) =
                    sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                        .await;
                assert_eq!(plain as usize, KEPT, "lane={lane}: thresholded count");

                // The lane that actually executed, from the engine's own event.
                // This is what distinguishes "three lanes ran" from "one lane ran
                // three times" — pairwise-distinct fuel cannot, because a pin that
                // falls through to a fourth path with its own cost still yields
                // three distinct numbers and a mislabelled cell.
                let fired: Vec<String> = store.find_events("annotation delegate lane")[before..]
                    .iter()
                    .filter_map(|e| e.fields.get("lane").cloned())
                    .collect();
                assert!(
                    !fired.is_empty(),
                    "lane={lane}: no `annotation delegate lane` event was captured, so \
                     nothing here establishes which lane ran. An absent marker must \
                     fail rather than pass — if the event moved, this assertion is \
                     what needs updating, not deleting."
                );
                assert!(
                    fired.iter().all(|f| f == expected_fired),
                    "lane={lane}: expected the engine to report `{expected_fired}`, got \
                     {fired:?} (fuel {obj_fuel}). Either a runtime gate demoted the \
                     pin, or the FLUREE_ANNOTATION_LANE override never reached the \
                     query process, or the pin-to-lane mapping changed."
                );
            }

            // ---- twin surface: JSON-LD ---------------------------------
            let (jl_all, _) =
                jsonld_rows_and_fuel(&fluree, &post, &annotated_rows_jsonld(None)).await;
            let (jl_kept, _) =
                jsonld_rows_and_fuel(&fluree, &post, &annotated_rows_jsonld(Some(THRESHOLD))).await;
            assert_eq!(jl_all, EDGES, "unfiltered JSON-LD rows");
            assert_eq!(jl_kept, KEPT, "thresholded JSON-LD rows");
            // The structural stamp above was taken on the JSON-LD plan, so the
            // shared-IR claim is already pinned on both surfaces.
        })
        .await;
}

/// First node with this `op` in a rendered physical plan, depth-first.
fn find_op<'a>(node: &'a JsonValue, op: &str) -> Option<&'a JsonValue> {
    if node["op"] == op {
        return Some(node);
    }
    node["children"]
        .as_array()?
        .iter()
        .find_map(|edge| find_op(&edge["node"], op))
}

/// Scoped `FLUREE_ANNOTATION_LANE`, restored on drop, serialized against any
/// other test in this binary that pins a lane. Results are lane-invariant, so
/// a concurrent annotation test seeing a pinned lane still gets its answer;
/// only a test asserting a *lane* would be disturbed, and this is the only one.
struct LanePin {
    _guard: std::sync::MutexGuard<'static, ()>,
    prev: Option<String>,
}

impl LanePin {
    fn chain() -> Self {
        Self::lane("chain")
    }

    fn lane(name: &str) -> Self {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        let guard = LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::var("FLUREE_ANNOTATION_LANE").ok();
        std::env::set_var("FLUREE_ANNOTATION_LANE", name);
        Self {
            _guard: guard,
            prev,
        }
    }
}

impl Drop for LanePin {
    fn drop(&mut self) {
        match self.prev.take() {
            Some(v) => std::env::set_var("FLUREE_ANNOTATION_LANE", v),
            None => std::env::remove_var("FLUREE_ANNOTATION_LANE"),
        }
    }
}

/// Scoped `FLUREE_DISABLE_QUERY_FAST_PATHS`, restored on drop.
struct DisableFastPaths(Option<String>);

impl DisableFastPaths {
    fn set() -> Self {
        let prev = std::env::var("FLUREE_DISABLE_QUERY_FAST_PATHS").ok();
        std::env::set_var("FLUREE_DISABLE_QUERY_FAST_PATHS", "1");
        Self(prev)
    }
}

impl Drop for DisableFastPaths {
    fn drop(&mut self) {
        match self.0.take() {
            Some(v) => std::env::set_var("FLUREE_DISABLE_QUERY_FAST_PATHS", v),
            None => std::env::remove_var("FLUREE_DISABLE_QUERY_FAST_PATHS"),
        }
    }
}
