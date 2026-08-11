//! Regression pin: an aggregate fast path must decline a triple pattern whose
//! positions repeat a variable.
//!
//! `{ ?x <rel> ?x }` carries an implicit equality join. The metadata-backed
//! aggregate fast paths answer from per-predicate index metadata — a PSOT
//! leaflet row count, a distinct-subject count, a whole-permutation triple
//! count — and never compare the subject to the object, so every one of them
//! returned the predicate's cardinality: `COUNT(*) WHERE { ?x <rel> ?x }` was 5
//! where SPARQL requires 2, and `COUNT(*) WHERE { ?a a ?a }` was 4 where SPARQL
//! requires 0. The engine's own `count_plan.rs` has rejected this shape since
//! the v4 baseline (`test_self_loop_rejected`); the older per-shape detectors
//! in `execute::operator_tree` never got the same guard.
//!
//! Two structural notes about why this survived, both of which shape this test:
//!
//! * It is an **indexed-lane** defect. A memory-backed ledger declines every
//!   fast path and answers all 20+ shapes below correctly even unfixed, so a
//!   novelty-only test is vacuous — which is exactly why `testsuite-sparql`
//!   (`FlureeBuilder::memory()` throughout) never caught it. The ledger here is
//!   bulk-imported so the fast paths really fire.
//! * A value assertion alone cannot tell "the guard declined" from "the
//!   detector silently stopped matching". Each case therefore also asserts the
//!   engine's own `fast-path outcome` stamps: no guarded site may `proceed` for
//!   a repeated-variable shape, and the CTRL shapes — where predicate
//!   cardinality *is* the right answer — must still `proceed` on their named
//!   site, so the fix cannot be a blanket disable.
//!
//! Own test binary: the kill switch is process-global, and this file both
//! toggles it and asserts fast-path routing.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::Value;
use std::io::Write;
use tempfile::TempDir;

/// The reported fixture: `rel` has 5 triples of which 2 are self-loops (n1,
/// n2); `rdf:type` has 4 triples and no self-type. `score` and `label` add
/// literal-object predicates, where a self-loop is impossible outright.
const DATA: &str = r#"
@prefix ex: <http://ex/> .

ex:n1 a ex:C ; ex:rel ex:n1, ex:n2 ; ex:score 10 ; ex:label "alpha" .
ex:n2 a ex:C ; ex:rel ex:n2, ex:n3 ; ex:score 20 ; ex:label "beta" .
ex:n3 a ex:C ; ex:rel ex:n4 .
ex:n4 a ex:C .
"#;

/// Two datatypes under one predicate, so a datatype constraint on the object
/// selects a strict subset of the column: 10 of 30.
const DTC_DATA: &str = r#"
<http://ex/i1> <http://ex/amount> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/i2> <http://ex/amount> "20"^^<http://www.w3.org/2001/XMLSchema#positiveInteger> .
"#;

/// Every fast-path site that answers from per-predicate or whole-permutation
/// metadata, and therefore may serve only a triple whose subject and object are
/// distinct free variables. Names are the `FastPathOperator` labels the engine
/// stamps.
const GUARDED_SITES: &[&str] = &[
    "COUNT rows",
    "COUNT rows numeric compare",
    "COUNT(DISTINCT)",
    "COUNT blank nodes",
    "triples COUNT",
    "distinct subject COUNT",
    "AVG numeric",
    "MIN/MAX string",
    "SUM(?o)",
    "SUM(ABS)",
];

/// What the engine's fast-path stamps must show for a case.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Routing {
    /// No site in `GUARDED_SITES` may `proceed`.
    NoGuardedSite,
    /// This site must `proceed` — the shape is one where predicate-level
    /// metadata is the correct answer, and it must keep its fast path.
    MustFire(&'static str),
}

struct Case {
    name: &'static str,
    sparql: &'static str,
    /// Hand-computed answer as `summarize` renders it, or `"=generic"` when the
    /// value under test is the empty-group rendering (unbound MAX, AVG/SUM of
    /// no rows) rather than a number worth pinning by hand — there the contract
    /// is that the fast lane and the generic pipeline agree.
    expected: &'static str,
    routing: Routing,
}

fn cases() -> Vec<Case> {
    use Routing::{MustFire, NoGuardedSite};
    vec![
        // ---- the reported shapes -------------------------------------------
        Case {
            name: "COUNT(*) {?x rel ?x}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(?x) {?x rel ?x}",
            sparql: "SELECT (COUNT(?x) AS ?n) WHERE { ?x <http://ex/rel> ?x }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(*) {?a a ?a}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?a a ?a }",
            expected: "n=0",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(DISTINCT ?x) {?x rel ?x}",
            sparql: "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { ?x <http://ex/rel> ?x }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        // ---- repeated var with a VARIABLE predicate: the two detectors that
        // destructure the triple inline instead of calling
        // `validate_simple_triple`, so they need their own pairwise check.
        Case {
            name: "COUNT(*) {?x ?p ?x}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x ?p ?x }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(?x) {?x ?x ?o} (subject var == predicate var)",
            sparql: "SELECT (COUNT(?x) AS ?n) WHERE { ?x ?x ?o }",
            expected: "n=0",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(DISTINCT ?x) {?x ?p ?x}",
            sparql: "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { ?x ?p ?x }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        // ---- literal-object predicate: subject is an IRI and object is a
        // literal, so the self-loop is impossible and every answer is empty.
        Case {
            name: "COUNT(*) {?x score ?x} (literal obj)",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/score> ?x }",
            expected: "n=0",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(DISTINCT ?x) {?x score ?x}",
            sparql: "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { ?x <http://ex/score> ?x }",
            expected: "n=0",
            routing: NoGuardedSite,
        },
        Case {
            name: "AVG(?x) {?x score ?x}",
            sparql: "SELECT (AVG(?x) AS ?n) WHERE { ?x <http://ex/score> ?x }",
            expected: "=generic",
            routing: NoGuardedSite,
        },
        Case {
            name: "MAX(?x) {?x label ?x}",
            sparql: "SELECT (MAX(?x) AS ?n) WHERE { ?x <http://ex/label> ?x }",
            expected: "=generic",
            routing: NoGuardedSite,
        },
        Case {
            name: "SUM(?x) {?x score ?x}",
            sparql: "SELECT (SUM(?x) AS ?n) WHERE { ?x <http://ex/score> ?x }",
            expected: "=generic",
            routing: NoGuardedSite,
        },
        // ---- CONSTANT SUBJECT. `detect_fused_scan_sum_i64` inspected only the
        // predicate and object, so it also fired on a bound subject and summed
        // the predicate across the whole ledger — "total spend for this
        // customer" answering with everyone's. The AVG / MIN-MAX / COUNT
        // siblings route through `validate_simple_triple` and were always
        // correct here, which is why only the SUM rows were wrong.
        Case {
            name: "SUM(?v) {<n1> score ?v} (constant subject)",
            sparql: "SELECT (SUM(?v) AS ?n) WHERE { <http://ex/n1> <http://ex/score> ?v }",
            expected: "n=10",
            routing: NoGuardedSite,
        },
        Case {
            name: "SUM(?v) {<n2> score ?v} (constant subject)",
            sparql: "SELECT (SUM(?v) AS ?n) WHERE { <http://ex/n2> <http://ex/score> ?v }",
            expected: "n=20",
            routing: NoGuardedSite,
        },
        Case {
            name: "SUM(ABS(?v)) {<n1> score ?v} (constant subject, BIND arm)",
            sparql: "SELECT (SUM(ABS(?v)) AS ?n) WHERE { <http://ex/n1> <http://ex/score> ?v }",
            expected: "n=10",
            routing: NoGuardedSite,
        },
        Case {
            name: "AVG(?v) {<n1> score ?v} (constant subject)",
            sparql: "SELECT (AVG(?v) AS ?n) WHERE { <http://ex/n1> <http://ex/score> ?v }",
            expected: "n=10",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(*) {<n1> score ?v} (constant subject)",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { <http://ex/n1> <http://ex/score> ?v }",
            expected: "n=1",
            routing: NoGuardedSite,
        },
        // ---- shapes that were already correct via unrelated gates and must
        // stay correct (the fix must not perturb them).
        Case {
            name: "rows {?x rel ?x}",
            sparql: "SELECT ?x WHERE { ?x <http://ex/rel> ?x }",
            expected: "rows=2",
            routing: NoGuardedSite,
        },
        Case {
            name: "rows {?a a ?a}",
            sparql: "SELECT ?a WHERE { ?a a ?a }",
            expected: "rows=0",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(*) {?x rel ?x} + FILTER",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x \
                     FILTER(?x != <http://ex/n1>) }",
            expected: "n=1",
            routing: NoGuardedSite,
        },
        Case {
            name: "GROUP BY ?x COUNT(*) {?x rel ?x}",
            sparql: "SELECT ?x (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x } GROUP BY ?x",
            expected: "rows=2",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(*) {?x rel ?x . ?x rel ?y}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x . \
                     ?x <http://ex/rel> ?y }",
            expected: "n=4",
            routing: NoGuardedSite,
        },
        Case {
            name: "COUNT(*) {?x rel ?x} OPTIONAL {?x a ?c}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?x <http://ex/rel> ?x \
                     OPTIONAL { ?x a ?c } }",
            expected: "n=2",
            routing: NoGuardedSite,
        },
        // ---- controls: distinct variables, where predicate-level metadata IS
        // the right answer. These must keep their fast path.
        Case {
            name: "CTRL COUNT(*) {?s rel ?o}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s <http://ex/rel> ?o }",
            expected: "n=5",
            routing: MustFire("COUNT rows"),
        },
        Case {
            name: "CTRL COUNT(*) {?s a ?o}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s a ?o }",
            expected: "n=4",
            routing: MustFire("COUNT rows"),
        },
        Case {
            name: "CTRL COUNT(*) {?s ?p ?o}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
            expected: "n=13",
            routing: MustFire("triples COUNT"),
        },
        Case {
            name: "CTRL COUNT(DISTINCT ?s) {?s ?p ?o}",
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }",
            expected: "n=4",
            routing: MustFire("distinct subject COUNT"),
        },
        Case {
            name: "CTRL SUM(?o) {?s score ?o}",
            sparql: "SELECT (SUM(?o) AS ?n) WHERE { ?s <http://ex/score> ?o }",
            expected: "n=30",
            routing: MustFire("SUM(?o)"),
        },
        Case {
            name: "CTRL SUM(ABS(?o)) {?s score ?o}",
            sparql: "SELECT (SUM(ABS(?o)) AS ?n) WHERE { ?s <http://ex/score> ?o }",
            expected: "n=30",
            routing: MustFire("SUM(ABS)"),
        },
    ]
}

/// Canonical one-line rendering: a single-row aggregate renders `n=<value>`,
/// everything else renders `rows=<count>`.
fn summarize(results: &Value) -> String {
    let bindings = results["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("no results.bindings in {results}"));
    if bindings.len() == 1 {
        if let Some(v) = bindings[0].get("n").and_then(|c| c["value"].as_str()) {
            return format!("n={v}");
        }
    }
    format!("rows={}", bindings.len())
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

async fn indexed_ledger(dir: &TempDir, name: &str, ttl: &str) -> (fluree_db_api::Fluree, String) {
    let data_dir = TempDir::new().expect("data tmpdir");
    let path = data_dir.path().join("00-fixture.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(ttl.as_bytes()).expect("write ttl");

    // Bulk import builds a fully persisted binary index with no trailing
    // novelty, so `fast_path_store` holds and the detectors actually fire.
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = format!("test/{name}:main");
    fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    (fluree, ledger_id)
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_variable_patterns_decline_the_aggregate_fast_paths() {
    // The kill switch OR's with this env var, so with it set the fast-path
    // phase would run generically and every assertion below would be vacuous.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let db_dir = TempDir::new().expect("db tmpdir");
    let (fluree, ledger_id) = indexed_ledger(&db_dir, "repeated-var-guards", DATA).await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");
    let cases = cases();

    // Phase 1 — fast paths on, under span capture so each answer can be
    // attributed to the site that served it.
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut fast: Vec<String> = Vec::new();
    let mut proceeded: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let before = store.find_events("fast-path outcome").len();
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let got = match fluree.query(&db, c.sparql).await {
            Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                Ok(json) => summarize(&json),
                Err(e) => format!("ERR:{e}"),
            },
            Err(e) => format!("ERR:{e}"),
        };
        proceeded.push(
            store.find_events("fast-path outcome")[before..]
                .iter()
                .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                .filter_map(|e| e.fields.get("site").cloned())
                .collect(),
        );
        fast.push(got);
    }
    drop(tracing_guard);

    // Phase 2 — generic pipeline. It agrees with the hand-computed SPARQL
    // answer on every shape, which is what makes any divergence attributable to
    // a fast path rather than to a semantics disagreement.
    set_fast_paths_disabled(true);
    let mut generic: Vec<String> = Vec::new();
    for c in &cases {
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let got = match fluree.query(&db, c.sparql).await {
            Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                Ok(json) => summarize(&json),
                Err(e) => format!("ERR:{e}"),
            },
            Err(e) => format!("ERR:{e}"),
        };
        generic.push(got);
    }
    set_fast_paths_disabled(false);

    let mut failures: Vec<String> = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        if c.expected == "=generic" {
            if fast[i] != generic[i] {
                failures.push(format!(
                    "{}: fast {} != generic {} [proceeded: {:?}]",
                    c.name, fast[i], generic[i], proceeded[i]
                ));
            }
        } else {
            if fast[i] != c.expected {
                failures.push(format!(
                    "{}: fast lane returned {}, expected {} [proceeded: {:?}]",
                    c.name, fast[i], c.expected, proceeded[i]
                ));
            }
            if generic[i] != c.expected {
                failures.push(format!(
                    "{}: generic pipeline returned {}, expected {} — the \
                     hand-computed answer is wrong or the general pipeline \
                     regressed",
                    c.name, generic[i], c.expected
                ));
            }
        }

        match c.routing {
            Routing::NoGuardedSite => {
                let bad: Vec<&String> = proceeded[i]
                    .iter()
                    .filter(|s| GUARDED_SITES.contains(&s.as_str()))
                    .collect();
                if !bad.is_empty() {
                    failures.push(format!(
                        "{}: metadata fast path {bad:?} proceeded on a pattern it \
                         is not entitled to answer",
                        c.name
                    ));
                }
            }
            Routing::MustFire(site) => {
                if !proceeded[i].iter().any(|s| s == site) {
                    failures.push(format!(
                        "{}: expected site `{site}` to proceed — the guard must \
                         narrow the gate, not disable the detector \
                         [proceeded: {:?}]",
                        c.name, proceeded[i]
                    ));
                }
            }
        }
    }

    // ---- The datatype-constrained SUM --------------------------------------
    // Routing `detect_fused_scan_sum_i64` through `validate_simple_triple` also
    // inherits that helper's `tp.dtc.is_some()` decline, which the SUM detector
    // never had of its own: it read only the predicate and the object var, so a
    // datatype-constrained SUM took the fused scan and summed the predicate's
    // whole object column across every datatype under it.
    //
    // A datatype constraint alongside a *variable* object has no SPARQL
    // surface — it is the JSON-LD `{"@value": "?v", "@type": …}` form — so this
    // runs on its own tiny ledger with two datatypes under one predicate.
    // `xsd:positiveInteger` is chosen because it survives numeric-datatype
    // normalization; `xsd:long` and friends fold into `xsd:integer` and would
    // make the constraint a no-op.
    {
        let db_dir = TempDir::new().expect("db tmpdir");
        let (fluree, ledger_id) = indexed_ledger(&db_dir, "dtc-sum", DTC_DATA).await;
        let ledger = fluree.ledger(&ledger_id).await.expect("load");

        let constrained = serde_json::json!({
            "@context": {"ex": "http://ex/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "select": ["(as (sum ?v) ?n)"],
            "where": {"@id": "?s", "ex:amount": {"@value": "?v", "@type": "xsd:integer"}}
        });

        let (store, tracing_guard) = span_capture::init_test_tracing();
        set_fast_paths_disabled(false);
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let fast = match fluree.query(&db, &constrained).await {
            Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                Ok(json) => summarize(&json),
                Err(e) => format!("ERR:{e}"),
            },
            Err(e) => format!("ERR:{e}"),
        };
        let dtc_sites: Vec<String> = store
            .find_events("fast-path outcome")
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect();

        // Must-fire control on the same ledger: drop the constraint and the
        // fused scan is entitled to the whole column again. Without it, a fix
        // that disabled the detector outright would pass everything above.
        let before = store.find_events("fast-path outcome").len();
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let unconstrained = match fluree
            .query(
                &db,
                "SELECT (SUM(?v) AS ?n) WHERE { ?s <http://ex/amount> ?v }",
            )
            .await
        {
            Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                Ok(json) => summarize(&json),
                Err(e) => format!("ERR:{e}"),
            },
            Err(e) => format!("ERR:{e}"),
        };
        let plain_sites: Vec<String> = store.find_events("fast-path outcome")[before..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect();
        drop(tracing_guard);

        if fast != "n=10" {
            failures.push(format!(
                "datatype-constrained SUM: got {fast}, expected n=10 — the \
                 constraint selects only the xsd:integer row [proceeded: \
                 {dtc_sites:?}]"
            ));
        }
        let bad: Vec<&String> = dtc_sites
            .iter()
            .filter(|s| GUARDED_SITES.contains(&s.as_str()))
            .collect();
        if !bad.is_empty() {
            failures.push(format!(
                "datatype-constrained SUM: metadata fast path {bad:?} proceeded \
                 on a triple carrying a datatype constraint it cannot honour"
            ));
        }
        if unconstrained != "n=30" {
            failures.push(format!(
                "CTRL unconstrained SUM on the same ledger: got {unconstrained}, \
                 expected n=30"
            ));
        }
        if !plain_sites.iter().any(|s| s == "SUM(?o)") {
            failures.push(format!(
                "CTRL unconstrained SUM: expected site `SUM(?o)` to proceed — \
                 the dtc decline must narrow the detector, not disable it \
                 [proceeded: {plain_sites:?}]"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "repeated-variable fast-path guard failures:\n  {}",
        failures.join("\n  ")
    );
}
