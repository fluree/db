//! Regression pin: the whole-graph `COUNT(DISTINCT ?o)` fast path must decline
//! a graph whose objects include a NumBig arena handle.
//!
//! `SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }` is answered from OPST
//! leaflet directory metadata by grouping on the 10-byte lead `o_type(2) +
//! o_key(8)`, which stops immediately before `p_id` at bytes `[14..18]`. For
//! every object kind but one that lead is a faithful graph-wide identity. The
//! exception is `OType::NUM_BIG_OVERFLOW`, whose `o_key` is a handle into a
//! **per-predicate** arena allocated from 0 within each `(g_id, p_id)`: the
//! first big value under one predicate and the first under another are both
//! handle `0` and collapse into one group.
//!
//! Two consequences shape this file. Every `xsd:decimal` lands in that arena
//! unconditionally (the resolver has no inline branch), and so does any
//! `xsd:integer` too large for `i64` — so the scope is not "decimals" but "the
//! NumBig arena". And because handles are sequential per predicate, the result
//! was `max` over predicates of their distinct big-value counts rather than the
//! size of the union: a silent undercount that can never over-report, which is
//! why nothing downstream ever tripped on it.
//!
//! Everything here runs on **bulk-imported, fully indexed** ledgers. A
//! `FlureeBuilder::memory()` ledger declines every one of these fast paths and
//! would make the whole file pass vacuously.
//!
//! One test function, because the kill switch is process-global and phase 1
//! toggles it; `cargo test` runs a binary's tests on parallel threads.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder, IndexConfig, LedgerManagerConfig};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value};
use std::io::Write;
use support::span_capture;
use tempfile::TempDir;

/// Operator label the whole-graph distinct-object count stamps.
const OBJECT_SITE: &str = "distinct object COUNT";
/// Whole-graph distinct-**subject** count — must be untouched by the gate.
const SUBJECT_SITE: &str = "distinct subject COUNT";
/// Bound-predicate distinct-object count, served from POST whose 14-byte lead
/// starts with `p_id` and therefore scopes the arena handle correctly.
const PREDICATE_SITE: &str = "COUNT(DISTINCT)";

const Q_COUNT: &str = "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }";

/// The reported fixture: two different decimals under two different predicates.
const F_REPORTED: &str = r#"
<http://valuenet/ontop/treatments/treatment_id=11> <http://valuenet/ontop/treatments#cost_of_treatment> "514.0000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://valuenet/ontop/charges/charge_id=3> <http://valuenet/ontop/charges#charge_amount> "640.0000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// Two decimals under ONE predicate: handles 0 and 1 in a single arena, so this
/// answered correctly even unfixed. It still declines now — the gate is on the
/// presence of a NumBig row, not on a collision it cannot see from metadata.
const F_ONE_PRED: &str = r#"
<http://ex/s1> <http://ex/p1> "514.0000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/s2> <http://ex/p1> "640.0000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// The same decimal VALUE under two predicates: two handles, one term. Correct
/// by accident before the fix (both are handle 0).
const F_SAME_VALUE: &str = r#"
<http://ex/s1> <http://ex/p1> "5.5000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/s2> <http://ex/p2> "5.5000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// 3 distinct decimals under p1, 1 disjoint under p2: union 4, `max` 3.
const F_MAXPRED: &str = r#"
<http://ex/a1> <http://ex/p1> "1.1000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a2> <http://ex/p1> "2.2000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a3> <http://ex/p1> "3.3000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b1> <http://ex/p2> "9.9000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// 3 and 3, all disjoint: union 6, `max` 3. Also the ledger the must-fire
/// guards below run on.
const F_LOSS3: &str = r#"
<http://ex/a1> <http://ex/p1> "1.1000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a2> <http://ex/p1> "2.2000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a3> <http://ex/p1> "3.3000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b1> <http://ex/p2> "4.4000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b2> <http://ex/p2> "5.5000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b3> <http://ex/p2> "6.6000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// Decimals across predicates PLUS strings and small integers, so the correctly
/// counted non-NumBig slice is exercised alongside the broken one.
const F_MIXED: &str = r#"
<http://ex/a1> <http://ex/p1> "1.1000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a2> <http://ex/p1> "2.2000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b1> <http://ex/p2> "3.3000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b2> <http://ex/p2> "4.4000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/c1> <http://ex/p3> "alpha" .
<http://ex/c2> <http://ex/p3> "beta" .
<http://ex/d1> <http://ex/p4> 7 .
<http://ex/d2> <http://ex/p4> 8 .
"#;

/// 5 decimals under p1 and 4 under p2 of which 1 is shared: union 8, `max` 5 —
/// the reported production shape, a loss of exactly 3. Values are inserted in
/// non-numeric order so the extrema and ORDER BY pins below are meaningful:
/// arena handles follow insertion order, so the global min and max sit at
/// non-extreme handle positions.
const F_LIFECYCLE: &str = r#"
<http://ex/a1> <http://ex/p1> "5.5000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a2> <http://ex/p1> "6.6000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a3> <http://ex/p1> "7.7000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a4> <http://ex/p1> "8.8000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/a5> <http://ex/p1> "9.9000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b1> <http://ex/p2> "9.9000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b2> <http://ex/p2> "1.1000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b3> <http://ex/p2> "2.2000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<http://ex/b4> <http://ex/p2> "3.3000"^^<http://www.w3.org/2001/XMLSchema#decimal> .
"#;

/// `xsd:integer` beyond `i64`: the same per-predicate arena as decimals. The
/// reported scope said integers were unaffected — true only up to `i64::MAX`.
const F_BIGINT_CROSS: &str = r#"
<http://ex/s1> <http://ex/p1> "170141183460469231731687303715884105727"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/s2> <http://ex/p2> "170141183460469231731687303715884105999"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

/// `xsd:integer` within `i64`: an inline, value-faithful `o_key`.
const F_INT_CROSS: &str = r#"
<http://ex/s1> <http://ex/p1> "514"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/s2> <http://ex/p2> "640"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const F_DOUBLE_CROSS: &str = r#"
<http://ex/s1> <http://ex/p1> "514.0"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://ex/s2> <http://ex/p2> "640.0"^^<http://www.w3.org/2001/XMLSchema#double> .
"#;

const F_STR_CROSS: &str = r#"
<http://ex/s1> <http://ex/p1> "514.0000" .
<http://ex/s2> <http://ex/p2> "640.0000" .
"#;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Routing {
    /// The graph holds a NumBig object: the whole-graph object count must fall
    /// through to the general pipeline.
    MustDecline,
    /// No NumBig object anywhere: the fast path must still serve it, so an
    /// over-broad gate (or a blanket disable) fails here.
    MustProceed,
}

struct Case {
    name: &'static str,
    ttl: &'static str,
    /// Hand-computed distinct object terms in the whole graph.
    truth: usize,
    routing: Routing,
}

const CASES: &[Case] = &[
    Case {
        name: "reported: two decimals, two predicates",
        ttl: F_REPORTED,
        truth: 2,
        routing: Routing::MustDecline,
    },
    Case {
        name: "two decimals under one predicate",
        ttl: F_ONE_PRED,
        truth: 2,
        routing: Routing::MustDecline,
    },
    Case {
        name: "same decimal value under two predicates",
        ttl: F_SAME_VALUE,
        truth: 1,
        routing: Routing::MustDecline,
    },
    Case {
        name: "decimals 3 and 1 disjoint",
        ttl: F_MAXPRED,
        truth: 4,
        routing: Routing::MustDecline,
    },
    Case {
        name: "decimals 3 and 3 disjoint",
        ttl: F_LOSS3,
        truth: 6,
        routing: Routing::MustDecline,
    },
    Case {
        name: "decimals 5 and 4 sharing one",
        ttl: F_LIFECYCLE,
        truth: 8,
        routing: Routing::MustDecline,
    },
    Case {
        name: "decimals mixed with strings and small integers",
        ttl: F_MIXED,
        truth: 8,
        routing: Routing::MustDecline,
    },
    Case {
        name: "overflow xsd:integer across two predicates",
        ttl: F_BIGINT_CROSS,
        truth: 2,
        routing: Routing::MustDecline,
    },
    Case {
        name: "CTRL i64 integers across two predicates",
        ttl: F_INT_CROSS,
        truth: 2,
        routing: Routing::MustProceed,
    },
    Case {
        name: "CTRL doubles across two predicates",
        ttl: F_DOUBLE_CROSS,
        truth: 2,
        routing: Routing::MustProceed,
    },
    Case {
        name: "CTRL strings across two predicates",
        ttl: F_STR_CROSS,
        truth: 2,
        routing: Routing::MustProceed,
    },
];

/// Bulk import builds a fully persisted binary index with no trailing novelty,
/// so `fast_path_store` holds and the detectors actually fire. The `TempDir`s
/// are returned so the caller keeps them alive for the ledger's lifetime.
async fn build_indexed(ttl: &str, slug: &str) -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");
    let path = data_dir.path().join("00-fixture.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(ttl.as_bytes()).expect("write ttl");

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    // Case names double as ledger names, so fold anything the ledger-id parser
    // rejects (spaces, colons, parentheses) into `-`.
    let sanitized: String = slug
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let ledger_id = format!("numbig/{sanitized}:main");
    fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    (db_dir, data_dir, fluree, ledger_id)
}

/// Query an already-resolved view, as `Fluree::db` hands back.
async fn run_view(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) -> Value {
    fluree
        .query(view, sparql)
        .await
        .unwrap_or_else(|e| panic!("query {sparql}: {e}"))
        .to_sparql_json(&view.snapshot)
        .unwrap_or_else(|e| panic!("to_sparql_json {sparql}: {e}"))
}

async fn run(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> Value {
    let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
    fluree
        .query(&db, sparql)
        .await
        .unwrap_or_else(|e| panic!("query {sparql}: {e}"))
        .to_sparql_json(&ledger.snapshot)
        .unwrap_or_else(|e| panic!("to_sparql_json {sparql}: {e}"))
}

fn scalar(v: &Value) -> String {
    v["results"]["bindings"][0]["n"]["value"]
        .as_str()
        .unwrap_or("<none>")
        .to_string()
}

fn nrows(v: &Value) -> usize {
    v["results"]["bindings"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
}

/// Values of `?o`, in result order.
fn o_values(v: &Value) -> Vec<String> {
    v["results"]["bindings"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|b| b["o"]["value"].as_str().unwrap_or("?").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn proceeded(store: &span_capture::SpanStore, from: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[from..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn distinct_object_count_declines_numbig_object_keys() {
    // The kill switch OR's with this env var, so with it set the fast lane
    // would run generically and every assertion below would be vacuous.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast lane of this test \
         would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;
    let mut failures: Vec<String> = Vec::new();

    // ---- Phase 1: value + routing, per fixture ---------------------------
    for case in CASES {
        let (_db, _data, fluree, ledger_id) = build_indexed(case.ttl, case.name).await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load ledger");
        let truth = case.truth.to_string();

        let (store, tracing_guard) = span_capture::init_test_tracing();
        set_fast_paths_disabled(false);
        let fast = scalar(&run(&fluree, &ledger, Q_COUNT).await);
        let sites = proceeded(&store, 0);
        drop(tracing_guard);

        // The general pipeline is ground truth: it agrees with the
        // hand-computed answer on every fixture, which is what makes a
        // divergence attributable to the fast path rather than to a semantics
        // disagreement.
        set_fast_paths_disabled(true);
        let generic = scalar(&run(&fluree, &ledger, Q_COUNT).await);
        set_fast_paths_disabled(false);

        if fast != truth {
            failures.push(format!(
                "{}: fast lane COUNT(DISTINCT ?o) = {fast}, expected {truth} \
                 (generic {generic}) [proceeded: {sites:?}]",
                case.name
            ));
        }
        if generic != truth {
            failures.push(format!(
                "{}: generic pipeline = {generic}, expected {truth} — the \
                 hand-computed answer is wrong or the general pipeline regressed",
                case.name
            ));
        }
        let did_proceed = sites.iter().any(|s| s == OBJECT_SITE);
        match case.routing {
            Routing::MustDecline if did_proceed => failures.push(format!(
                "{}: `{OBJECT_SITE}` proceeded on a graph holding a NumBig \
                 object key, whose (o_type, o_key) lead is per-predicate",
                case.name
            )),
            Routing::MustProceed if !did_proceed => failures.push(format!(
                "{}: expected `{OBJECT_SITE}` to proceed — no NumBig object \
                 here, so the gate must narrow the fast path, not remove it \
                 [proceeded: {sites:?}]",
                case.name
            )),
            _ => {}
        }
    }

    // ---- Phase 2: the over-broad-fix guards, on a decimal-bearing ledger --
    // Only the whole-graph OPST object arm is unsound. Declining its siblings
    // would be a gratuitous perf regression, so both must still fire on the
    // very ledger that makes the object arm decline.
    {
        let (_db, _data, fluree, ledger_id) = build_indexed(F_LOSS3, "must-fire-guards").await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load ledger");
        let guards: &[(&str, &str, &str, usize)] = &[
            (
                "COUNT(DISTINCT ?s) whole-graph",
                "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }",
                SUBJECT_SITE,
                6,
            ),
            (
                "COUNT(DISTINCT ?o) bound predicate",
                "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s <http://ex/p1> ?o }",
                PREDICATE_SITE,
                3,
            ),
        ];
        for (name, q, site, truth) in guards {
            let (store, tracing_guard) = span_capture::init_test_tracing();
            set_fast_paths_disabled(false);
            let got = scalar(&run(&fluree, &ledger, q).await);
            let sites = proceeded(&store, 0);
            drop(tracing_guard);
            if got != truth.to_string() {
                failures.push(format!("{name}: got {got}, expected {truth}"));
            }
            if !sites.iter().any(|s| s == site) {
                failures.push(format!(
                    "{name}: expected `{site}` to proceed on a decimal-bearing \
                     ledger — only the whole-graph object arm is unsound \
                     [proceeded: {sites:?}]"
                ));
            }
        }
    }

    // ---- Phase 3: the neighbourhood, which the gate must not perturb ------
    // These decode the value through the arena instead of comparing raw keys,
    // so they were correct before the fix and document its boundary. The
    // same-value fixture is the mirror risk: a key that DOES carry p_id would
    // split one decimal term into two.
    {
        let (_db, _data, fluree, ledger_id) = build_indexed(F_LOSS3, "neighbourhood").await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load ledger");
        let rows: &[(&str, &str, usize)] = &[
            (
                "GROUP BY ?o",
                "SELECT ?o (COUNT(*) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?o",
                6,
            ),
            ("DISTINCT ?o", "SELECT DISTINCT ?o WHERE { ?s ?p ?o }", 6),
        ];
        for (name, q, truth) in rows {
            let got = nrows(&run(&fluree, &ledger, q).await);
            if got != *truth {
                failures.push(format!("{name}: {got} rows, expected {truth}"));
            }
        }

        for (name, q, truth) in [
            (
                "MIN(?o) whole-graph",
                "SELECT (MIN(?o) AS ?n) WHERE { ?s ?p ?o }",
                "1.1",
            ),
            (
                "MAX(?o) whole-graph",
                "SELECT (MAX(?o) AS ?n) WHERE { ?s ?p ?o }",
                "6.6",
            ),
        ] {
            let got = scalar(&run(&fluree, &ledger, q).await);
            if got != truth {
                failures.push(format!("{name}: got {got}, expected {truth}"));
            }
        }
    }
    {
        // Insertion order is deliberately not numeric order, so a path that
        // read OPST/handle order as value order would fail here.
        let (_db, _data, fluree, ledger_id) = build_indexed(F_LIFECYCLE, "ordering").await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load ledger");
        let asc =
            o_values(&run(&fluree, &ledger, "SELECT ?o WHERE { ?s ?p ?o } ORDER BY ?o").await);
        let mut sorted = asc.clone();
        sorted.sort_by(|a, b| {
            a.parse::<f64>()
                .unwrap_or(f64::NAN)
                .partial_cmp(&b.parse::<f64>().unwrap_or(f64::NAN))
                .expect("decimal fixture parses")
        });
        if asc != sorted {
            failures.push(format!("ORDER BY ?o ASC is not numeric: {asc:?}"));
        }
        let desc = o_values(
            &run(
                &fluree,
                &ledger,
                "SELECT ?o WHERE { ?s ?p ?o } ORDER BY DESC(?o)",
            )
            .await,
        );
        let mut rev = sorted.clone();
        rev.reverse();
        if desc != rev {
            failures.push(format!("ORDER BY ?o DESC is not numeric: {desc:?}"));
        }
    }
    {
        let (_db, _data, fluree, ledger_id) = build_indexed(F_SAME_VALUE, "mirror").await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load ledger");
        let mirror: &[(&str, &str, usize)] = &[
            (
                "GROUP BY ?o on one value under two predicates",
                "SELECT ?o (COUNT(*) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?o",
                1,
            ),
            (
                "DISTINCT ?o on one value under two predicates",
                "SELECT DISTINCT ?o WHERE { ?s ?p ?o }",
                1,
            ),
            (
                "cross-predicate value join",
                "SELECT ?a ?b WHERE { ?a <http://ex/p1> ?v . ?b <http://ex/p2> ?v }",
                1,
            ),
            (
                "cross-predicate FILTER equality",
                "SELECT ?a ?b WHERE { ?a <http://ex/p1> ?v1 . ?b <http://ex/p2> ?v2 \
                 FILTER(?v1 = ?v2) }",
                1,
            ),
        ];
        for (name, q, truth) in mirror {
            let got = nrows(&run(&fluree, &ledger, q).await);
            if got != *truth {
                failures.push(format!("{name}: {got} rows, expected {truth}"));
            }
        }
    }

    // ---- Phase 4: composition with the retracted-partition rebuild ---------
    failures.extend(gate_clears_after_retracting_every_decimal().await);

    assert!(
        failures.is_empty(),
        "distinct-object NumBig gate failures:\n  {}",
        failures.join("\n  ")
    );
}

/// The gate and the rebuild that drops retracted partitions have to compose:
/// once every NumBig row is retracted and the index rebuilt, the graph no
/// longer holds a non-identifying object key, so the fast path must come back.
///
/// The load-bearing detail is an ordering one. `count_distinct_lead_groups_inner`
/// skips a directory entry on `row_count == 0 || lead_group_count == 0` *before*
/// it consults `leaflet_may_hold_non_identifying_o_key`, so an emptied leaflet
/// whose key range still spans `NUM_BIG_OVERFLOW` cannot trip the gate. Move
/// the gate check above that skip and every ledger that has ever held a decimal
/// declines forever, which no value assertion elsewhere in this file would
/// catch — they all run on freshly imported ledgers with nothing retracted.
async fn gate_clears_after_retracting_every_decimal() -> Vec<String> {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "numbig/retracted-decimals:main";

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
            let mut failures: Vec<String> = Vec::new();
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            // Two decimals (NumBig) plus four objects that are not: distinct
            // objects = 6 while the decimals are live, 4 once they are gone.
            let seed = json!({
                "@context": {"ex": "http://ex/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
                "@graph": [
                    {"@id": "ex:a", "ex:cost": {"@value": "1.1", "@type": "xsd:decimal"}},
                    {"@id": "ex:b", "ex:cost": {"@value": "2.2", "@type": "xsd:decimal"}},
                    {"@id": "ex:c", "ex:label": "alpha"},
                    {"@id": "ex:d", "ex:label": "beta"},
                    {"@id": "ex:e", "ex:n": 7},
                    {"@id": "ex:f", "ex:n": 8}
                ]
            });
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let seeded = fluree
                .insert_with_opts(
                    ledger,
                    &seed,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = seeded.ledger;
            support::trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            support::wait_for_index_application(&fluree, ledger_id, ledger.t()).await;

            // While the decimals are live the gate declines, and the general
            // pipeline answers 6. This half also proves the ledger really is on
            // the indexed lane, so the post-retraction half is not vacuous.
            let (store, tracing_guard) = span_capture::init_test_tracing();
            let view = fluree.db(ledger_id).await.expect("indexed view");
            let before = scalar(&run_view(&fluree, &view, Q_COUNT).await);
            let before_sites = proceeded(&store, 0);
            if before != "6" {
                failures.push(format!(
                    "with decimals live: COUNT(DISTINCT ?o) = {before}, expected 6"
                ));
            }
            if before_sites.iter().any(|s| s == OBJECT_SITE) {
                failures.push(format!(
                    "with decimals live: `{OBJECT_SITE}` proceeded on a graph \
                     holding NumBig objects [proceeded: {before_sites:?}]"
                ));
            }

            let retracted = fluree
                .update(
                    ledger,
                    &json!({
                        "@context": {"ex": "http://ex/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
                        "delete": [
                            {"@id": "ex:a", "ex:cost": {"@value": "1.1", "@type": "xsd:decimal"}},
                            {"@id": "ex:b", "ex:cost": {"@value": "2.2", "@type": "xsd:decimal"}}
                        ]
                    }),
                )
                .await
                .expect("retract every decimal")
                .ledger;
            support::trigger_index_and_wait_outcome(&handle, ledger_id, retracted.t()).await;
            support::wait_for_index_application(&fluree, ledger_id, retracted.t()).await;

            let mark = store.find_events("fast-path outcome").len();
            let view = fluree.db(ledger_id).await.expect("rebuilt view");
            let after = scalar(&run_view(&fluree, &view, Q_COUNT).await);
            let after_sites = proceeded(&store, mark);
            drop(tracing_guard);

            if after != "4" {
                failures.push(format!(
                    "after retracting every decimal: COUNT(DISTINCT ?o) = {after}, \
                     expected 4 (two strings and two integers remain)"
                ));
            }
            if !after_sites.iter().any(|s| s == OBJECT_SITE) {
                failures.push(format!(
                    "after retracting every decimal: expected `{OBJECT_SITE}` to \
                     proceed again — an emptied leaflet whose key range still \
                     spans NUM_BIG_OVERFLOW must be skipped on row_count before \
                     the gate sees it [proceeded: {after_sites:?}]"
                ));
            }
            failures
        })
        .await
}
