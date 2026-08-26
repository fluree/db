//! Regression pin (#1700): a star join whose object variable nothing
//! downstream reads must still produce one row per solution, not one row per
//! subject.
//!
//! `PropertyJoinOperator` demotes a predicate to an existence-only (semijoin)
//! constraint when its object var is absent from the block's needed-vars set —
//! it records that the subject has *some* object and drops the per-subject
//! object list. That is a set operation. It was applied unconditionally, so on
//! `{ ?s a ex:Gadget . ?s ex:tag ?o }` a subject with three tags contributed
//! one row instead of three, and `COUNT(*)` returned the number of qualifying
//! **subjects**.
//!
//! Three structural notes, each of which shapes this test:
//!
//! * **A single-object-per-subject fixture cannot tell the two answers
//!   apart.** Every fixture below is deliberately multiplicity-bearing, and
//!   each shape is pinned alongside its distinct-subject count so the two
//!   numbers differ. Flatten the fixture and the `subjects` assertions fail
//!   rather than the test quietly going vacuous.
//! * **It is an indexed-lane defect.** The block only reaches
//!   `PropertyJoinOperator` with a bound-object anchor on a real index, so the
//!   ledger here is bulk-imported.
//! * **Both lanes are pinned against the hand-computed SPARQL answer, never
//!   against each other.** `FLUREE_DISABLE_QUERY_FAST_PATHS` is the repo's
//!   differential oracle, and for this shape the oracle was the broken side:
//!   the `COUNT(*)` fast path answered 5 from index metadata while the generic
//!   pipeline it is checked against answered 2. A fast-vs-generic comparison
//!   would have called the correct lane the regression. So the expectations
//!   below are arithmetic on the fixture, and the lanes are checked against
//!   that — not against one another.
//!
//! Own test binary: the kill switch is process-global.

#![cfg(feature = "native")]

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

/// Multiplicity-bearing by construction — every class below has more
/// `(subject, object)` pairs than it has subjects.
///
/// * `ex:Gadget` / `ex:tag`  — 5 pairs over 2 tagged subjects (3 + 2).
/// * `ex:Gadget` / `ex:code` — 3 pairs over 2 subjects (2 + 1); crossed with
///   `ex:tag` it gives 3x2 + 2x1 = 8, which no single-predicate count produces.
/// * `ex:g3` carries the type but no tag and no code, so an OPTIONAL tail has
///   a non-matching subject to preserve.
/// * `ex:Widget` / `ex:name` — 4 pairs over 3 subjects, the issue's second
///   fixture: a shape where rows and subjects differ by one.
const DATA: &str = r#"
@prefix ex: <http://example.org/> .

ex:g1 a ex:Gadget ; ex:tag "a" , "b" , "c" ; ex:code "x" , "y" .
ex:g2 a ex:Gadget ; ex:tag "d" , "e" ; ex:code "z" .
ex:g3 a ex:Gadget .

ex:w1 a ex:Widget ; ex:name "one" , "two" .
ex:w2 a ex:Widget ; ex:name "three" .
ex:w3 a ex:Widget ; ex:name "four" .
"#;

const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

struct Case {
    name: &'static str,
    sparql: &'static str,
    /// Hand-computed from `DATA`, as `summarize` renders it.
    expected: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        // ---- the reported shape ---------------------------------------------
        // g1 has 3 tags, g2 has 2 => 5 solutions over 2 subjects. Before the
        // fix the generic pipeline answered 2 here and the fast path 5.
        Case {
            name: "COUNT(*) {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "n=5",
        },
        // The discriminator: 2 != 5, so a fixture with one tag per subject
        // could not tell a passing run from a failing one.
        Case {
            name: "COUNT(DISTINCT ?s) {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "n=2",
        },
        // COUNT(?o) was already right, for an unrelated reason: naming ?o in
        // the aggregate puts it in the needed set, so the demotion never fired.
        // Pinned so a fix cannot regress it.
        Case {
            name: "COUNT(?o) {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT (COUNT(?o) AS ?n) WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "n=5",
        },
        // ---- the same collapse, read as rows rather than as a count ---------
        // Not an aggregate defect: the rows were already gone by the time the
        // aggregate ran. SPARQL projects a bag, so this is 5 rows, not 2.
        Case {
            name: "rows ?s {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "rows=5",
        },
        Case {
            name: "rows ?s ?o {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT ?s ?o WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "rows=5",
        },
        // ---- grouped COUNT(*): wrong on BOTH lanes before the fix -----------
        // g1 -> 3, g2 -> 2. This one the kill-switch oracle could never have
        // caught: the fast and generic lanes agreed on 1 and 1.
        Case {
            name: "GROUP BY ?s COUNT(*) {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT ?s (COUNT(*) AS ?n) WHERE { ?s a ex:Gadget . ?s ex:tag ?o } \
                     GROUP BY ?s ORDER BY ?s",
            expected: "rows=2 n=[2,3]",
        },
        // ---- two variable objects: a real cartesian, not just a fanout ------
        // g1: 3 tags x 2 codes = 6; g2: 2 x 1 = 2; total 8 over 2 subjects.
        // 8 is not the row count of either predicate alone, so this cannot be
        // satisfied by accidentally counting one column.
        Case {
            name: "COUNT(*) {?s a Gadget . ?s tag ?o . ?s code ?c}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE \
                     { ?s a ex:Gadget . ?s ex:tag ?o . ?s ex:code ?c }",
            expected: "n=8",
        },
        Case {
            name: "COUNT(DISTINCT ?s) {?s a Gadget . ?s tag ?o . ?s code ?c}",
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE \
                     { ?s a ex:Gadget . ?s ex:tag ?o . ?s ex:code ?c }",
            expected: "n=2",
        },
        Case {
            name: "rows {?s a Gadget . ?s tag ?o . ?s code ?c}",
            sparql: "SELECT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o . ?s ex:code ?c }",
            expected: "rows=8",
        },
        // ---- OPTIONAL tail folded into the same block -----------------------
        // g1 -> 3, g2 -> 2, g3 -> 1 unmatched row = 6.
        Case {
            name: "COUNT(*) {?s a Gadget} OPTIONAL {?s tag ?o}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE \
                     { ?s a ex:Gadget OPTIONAL { ?s ex:tag ?o } }",
            expected: "n=6",
        },
        // ---- the issue's second fixture: rows and subjects differ by one ----
        Case {
            name: "COUNT(*) {?s a Widget . ?s name ?n2}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s a ex:Widget . ?s ex:name ?n2 }",
            expected: "n=4",
        },
        Case {
            name: "COUNT(DISTINCT ?s) {?s a Widget . ?s name ?n2}",
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s a ex:Widget . ?s ex:name ?n2 }",
            expected: "n=3",
        },
        // ---- the pruning must SURVIVE where it is licensed ------------------
        // These consumers cannot observe row multiplicity, so the existence-only
        // demotion is still sound and must still happen. If the fix were a
        // blanket disable these would keep passing, but the unit tests in
        // `where_plan.rs` pin the plan shape; here they guard the answers.
        Case {
            name: "DISTINCT ?s {?s a Gadget . ?s tag ?o}",
            sparql: "SELECT DISTINCT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o }",
            expected: "rows=2",
        },
        Case {
            name: "DISTINCT ?s {?s a Gadget . ?s tag ?o . ?s code ?c}",
            sparql: "SELECT DISTINCT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o . ?s ex:code ?c }",
            expected: "rows=2",
        },
        // ---- a bound object carries multiplicity 1 and must stay collapsed --
        // `?s ex:tag "a"` matches at most one flake per subject, so this is 1
        // regardless of how many other tags g1 has.
        Case {
            name: "COUNT(*) {?s a Gadget . ?s tag \"a\"}",
            sparql: "SELECT (COUNT(*) AS ?n) WHERE { ?s a ex:Gadget . ?s ex:tag \"a\" }",
            expected: "n=1",
        },
    ]
}

/// The JSON-LD twin of each SPARQL shape above, per the three-surface parity
/// rule: both surfaces lower to the same IR, so a WHERE-planner defect reaches
/// both and a fix owes a regression test on both.
fn jsonld_cases() -> Vec<(&'static str, Value, &'static str)> {
    let ctx = json!({"ex": "http://example.org/"});
    vec![
        (
            "jsonld COUNT(*) gadget/tag",
            json!({
                "@context": ctx,
                "select": ["(as (count *) ?n)"],
                "where": {"@id": "?s", "@type": "ex:Gadget", "ex:tag": "?o"}
            }),
            "n=5",
        ),
        (
            "jsonld COUNT(DISTINCT ?s) gadget/tag",
            json!({
                "@context": ctx,
                "select": ["(as (count-distinct ?s) ?n)"],
                "where": {"@id": "?s", "@type": "ex:Gadget", "ex:tag": "?o"}
            }),
            "n=2",
        ),
        (
            "jsonld rows ?s gadget/tag",
            json!({
                "@context": ctx,
                "select": ["?s"],
                "where": {"@id": "?s", "@type": "ex:Gadget", "ex:tag": "?o"}
            }),
            "rows=5",
        ),
        (
            "jsonld COUNT(*) gadget/tag x code",
            json!({
                "@context": ctx,
                "select": ["(as (count *) ?n)"],
                "where": {"@id": "?s", "@type": "ex:Gadget", "ex:tag": "?o", "ex:code": "?c"}
            }),
            "n=8",
        ),
        (
            "jsonld COUNT(*) widget/name",
            json!({
                "@context": ctx,
                "select": ["(as (count *) ?n)"],
                "where": {"@id": "?s", "@type": "ex:Widget", "ex:name": "?n2"}
            }),
            "n=4",
        ),
        (
            "jsonld selectDistinct ?s gadget/tag",
            json!({
                "@context": ctx,
                "selectDistinct": ["?s"],
                "where": {"@id": "?s", "@type": "ex:Gadget", "ex:tag": "?o"}
            }),
            "rows=2",
        ),
    ]
}

/// Canonical one-line rendering. A single-row aggregate renders `n=<value>`; a
/// multi-row result whose every row carries `?n` renders its sorted counts too,
/// so a grouped `COUNT(*)` is pinned by its per-group values and not merely by
/// its row count.
fn summarize(results: &Value) -> String {
    let bindings = results["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("no results.bindings in {results}"));
    if bindings.len() == 1 {
        if let Some(v) = bindings[0].get("n").and_then(|c| c["value"].as_str()) {
            return format!("n={v}");
        }
    }
    let mut ns: Vec<String> = bindings
        .iter()
        .filter_map(|b| {
            b.get("n")
                .and_then(|c| c["value"].as_str())
                .map(str::to_string)
        })
        .collect();
    if !ns.is_empty() && ns.len() == bindings.len() {
        ns.sort();
        return format!("rows={} n=[{}]", bindings.len(), ns.join(","));
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
    // novelty. Required: on a never-indexed ledger the block never reaches the
    // property-join plan and every shape below is already correct, so a
    // memory-backed fixture would pin nothing.
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
async fn count_star_counts_joined_rows_not_distinct_subjects() {
    // The kill switch OR's with this env var, so with it set the fast-path
    // phase would run generically and half of every assertion below would be
    // pinning the same lane twice.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let db_dir = TempDir::new().expect("db tmpdir");
    let (fluree, ledger_id) = indexed_ledger(&db_dir, "count-star-multiplicity", DATA).await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");
    let cases = cases();
    let jsonld = jsonld_cases();

    let mut failures: Vec<String> = Vec::new();

    for (lane, disabled) in [("fast", false), ("generic", true)] {
        set_fast_paths_disabled(disabled);

        for c in &cases {
            let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
            let sparql = format!("{PREFIX}{}", c.sparql);
            let got = match fluree.query(&db, sparql.as_str()).await {
                Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                    Ok(json) => summarize(&json),
                    Err(e) => format!("ERR:{e}"),
                },
                Err(e) => format!("ERR:{e}"),
            };
            if got != c.expected {
                failures.push(format!(
                    "[{lane}] {}: got {got}, expected {} (hand-computed from the fixture)",
                    c.name, c.expected
                ));
            }
        }

        for (name, query, expected) in &jsonld {
            let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
            let got = match fluree.query(&db, query).await {
                Ok(r) => match r.to_sparql_json(&ledger.snapshot) {
                    Ok(json) => summarize(&json),
                    Err(e) => format!("ERR:{e}"),
                },
                Err(e) => format!("ERR:{e}"),
            };
            if got != *expected {
                failures.push(format!(
                    "[{lane}] {name}: got {got}, expected {expected} \
                     (SPARQL twin of the same shape)"
                ));
            }
        }
    }

    set_fast_paths_disabled(false);

    assert!(
        failures.is_empty(),
        "COUNT(*) row-multiplicity failures:\n  {}",
        failures.join("\n  ")
    );
}
