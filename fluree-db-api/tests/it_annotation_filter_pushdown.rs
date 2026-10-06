//! A threshold on an annotation body must reduce scan work.
//!
//! `expand_edge_annotation_patterns` wraps an annotated edge (the base triple,
//! the reifier's `rdf:reifies` link and its term components) in
//! `Pattern::DefaultGraphSource`, and `collect_inner_join_block` breaks on
//! that wrapper. A `FILTER` written beside the annotation therefore started a
//! block with no triples in it, `extract_bounds_from_filters` returned early,
//! and the threshold ran *above* the wrapper — after every annotation had been
//! read. On a 60k-edge ledger `?c > 0.7` and `?c > 0.97` both burned the same
//! fuel as no filter at all. The expansion now copies such a filter into the
//! wrapper (pinned structurally by the `sink_*` unit tests in `where_plan.rs`).
//!
//! The effect is pinned here: a tighter threshold must burn strictly less
//! fuel. Fuel charges rows the scan emits and never rows an encoded pre-filter
//! drops inside the cursor (`binary_scan.rs`), so it measures the thing under
//! test, is bit-identical across runs, and cannot be moved by load. A test that
//! only checked row counts would pass against the bug: it never changed an
//! answer.
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

/// The same COUNT, with the base **object** variable also in the filter: `?o`
/// is not projected and not read outside the wrapper, and survives only because
/// `collect_var_stats` walks `Pattern::Filter` and puts it back in the
/// referenced set.
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

    let (local, handle) = support::start_background_indexer_for(&fluree, IndexerConfig::small());

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
            // ---- correctness first -------------------------------------
            let (all, all_fuel) =
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(None)).await;
            let (kept, kept_fuel) =
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                    .await;
            assert_eq!(all as usize, EDGES, "unfiltered count");
            assert_eq!(kept as usize, KEPT, "thresholded count");

            // The same answer with every fast path disabled. A weak oracle on
            // its own (a defect in the plan both lanes share is invisible to
            // it), which is why the fuel assertion below is the one that pins
            // the behaviour.
            let generic = {
                let _g = DisableFastPaths::set();
                sparql_count_and_fuel(&fluree, &post, &annotated_count_sparql(Some(THRESHOLD)))
                    .await
                    .0
            };
            assert_eq!(generic, kept, "fast-path-disabled lane must agree");

            // ---- the effect: it reaches the scan ------------------------
            assert!(
                kept_fuel < all_fuel,
                "a threshold keeping {KEPT}/{EDGES} rows must cut scan work, not just \
                 rows in the answer: unfiltered {all_fuel}, filtered {kept_fuel}"
            );

            // A filter naming the base OBJECT variable passes the sink gate,
            // because `?o` is produced inside the wrapper. With a pure COUNT,
            // `?o` is in neither the projection nor `needed_outside`, so it
            // stays bound only because `collect_var_stats` (`where_plan.rs`)
            // walks `Pattern::Filter` into the referenced set; without that,
            // every row here drops silently.
            let (n, _) =
                sparql_count_and_fuel(&fluree, &post, &annotated_count_with_object_var_sparql())
                    .await;
            assert_eq!(
                n as usize, KEPT_EXCLUDING_ONE_OBJECT,
                "the base object variable must stay bound inside the wrapper"
            );

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

/// Scoped planner-fast-path disable, restored on drop.
///
/// Programmatic rather than `FLUREE_DISABLE_QUERY_FAST_PATHS`, because a scoped
/// env-var helper is a lie whenever the *reader* caches. `fast_paths_disabled()`
/// (`operator_tree.rs`) latches its env read in a `OnceLock` on first call, so:
///
/// - set the var after any query has run and it does nothing — the assertion
///   then compares a query against itself and passes vacuously;
/// - set it before any query has run and it latches `true` for the entire test
///   binary, which `Drop` cannot undo — `it_join_batched_overlay` is a module of
///   the same `grp_misc` binary and asserts specific lanes fired.
///
/// Which of those two happens is decided by test scheduling.
/// `set_fast_paths_disabled` is an `AtomicBool`, so it is both effective and
/// reversible; its own doc comment names this footgun.
///
/// The mutex is there because this is process-wide state in a parallel binary.
///
/// # Residual, and the full remedy if it ever bites
///
/// The mutex serialises this helper against *itself*; it cannot stop an
/// unrelated test in the same binary from observing fast paths off while the
/// guard is held. That is currently harmless because the switch reaches only
/// the fused/aggregate detectors (`operator_tree.rs`, `fast_paths_globally_disabled`),
/// the membership join (`where_plan.rs`) and the range semijoin — and no module
/// in `grp_misc` asserts any of them. Checked across all 39, not assumed. Note
/// the switch's own doc: batched joins and cursor selection are explicitly
/// unaffected, so `it_join_batched_overlay`'s `used_spot_star_walk` assertion
/// is not in scope for it.
///
/// **If a test asserting a fused fast path, membership join or range semijoin
/// is ever added to `grp_misc`, that is the moment to move this module to its
/// own `[[test]]` binary** — which is what `it_datetime_component_presence`
/// does, and why its header says "Own test binary: toggles the process-global
/// fast-path kill switch."
struct DisableFastPaths {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl DisableFastPaths {
    fn set() -> Self {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        let guard = LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The env var ORs into `fast_paths_disabled()` and cannot be cleared
        // programmatically, so with it set the fast-paths-ON phase would run
        // generically and the differential would pass without comparing
        // anything. Fail loudly instead.
        assert!(
            std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
            "FLUREE_DISABLE_QUERY_FAST_PATHS must be unset: it latches \
             `fast_paths_disabled()` on for the process and makes this \
             differential vacuous"
        );
        assert!(
            !fluree_db_api::fast_paths_disabled(),
            "fast paths already disabled on entry: something else in this binary \
             left the switch on, and this guard's Drop would clear it for them"
        );
        fluree_db_api::set_fast_paths_disabled(true);
        // Value-shaped, not presence-shaped: assert the switch is actually ON
        // rather than that we called the setter. This is the check whose
        // absence made the previous env-var version pass without ever
        // disabling anything.
        assert!(
            fluree_db_api::fast_paths_disabled(),
            "the kill switch must be on inside this guard"
        );
        Self { _guard: guard }
    }
}

impl Drop for DisableFastPaths {
    fn drop(&mut self) {
        fluree_db_api::set_fast_paths_disabled(false);
    }
}
