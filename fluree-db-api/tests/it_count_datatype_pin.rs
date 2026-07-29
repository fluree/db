//! Machine-enforced pin: every SPARQL COUNT carries exactly `xsd:integer`.
//!
//! SPARQL COUNT yields `xsd:integer` (§18.5.1.6). The COUNT fast paths tag the
//! datatype themselves rather than routing through `aggregate::finalize`, so a
//! single wrong `WellKnownDatatypes` field re-types the count while leaving its
//! value right — which is what `stats_query` (per-predicate COUNT) and
//! `fast_group_count_firsts` (GROUP BY object top-k COUNT) both did until
//! `45d6009bf`, and what the merge that reconciled this branch with `main`
//! nearly reverted.
//!
//! Nothing already in the tree catches that regression:
//!
//! * The W3C suite would (`result_comparison::terms_match` compares datatype
//!   IRIs strictly, before any numeric value-equality fallback), but it builds
//!   every ledger with `FlureeBuilder::memory()`, so `fast_path_store` declines
//!   and the generic pipeline — which tags `xsd:integer` correctly — answers.
//!   `agg02` therefore passes on a memory ledger no matter what the fast path
//!   tags.
//! * The differential harness (`it_differential_fastpath.rs`) does exercise an
//!   indexed ledger and does compare fast against generic, but it formats with
//!   `FormatterConfig::jsonld()`, which renders an integer literal as a bare
//!   JSON number with no datatype — so a re-typed count compares equal there
//!   too.
//!
//! This test closes the gap: an indexed (bulk-imported) ledger so the fast
//! paths actually fire, `to_sparql_json` so the datatype is on the wire, and a
//! STRICT string comparison of the datatype IRI — never numeric value
//! equality. Where a fast path stamps a per-detector verdict it also asserts
//! the path fired, so a detector that stops matching degrades to a loud
//! failure instead of a vacuous generic-vs-generic pass.
//!
//! Its own test binary: the kill switch is process-global, so a test that
//! toggles it must not share a process with tests that assert fast-path
//! routing (see the hazard note in `it_differential_fastpath.rs`).

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::Value;
use std::io::Write;
use tempfile::TempDir;

/// The one datatype a SPARQL COUNT may carry. Compared as an exact string —
/// `xsd:long` holds the same numeric value and must still fail.
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .

ex:a a ex:Thing ; ex:kind "red"  ; ex:n 1 .
ex:b a ex:Thing ; ex:kind "red"  ; ex:n 2 .
ex:c a ex:Thing ; ex:kind "blue" ; ex:n 3 .
ex:d a ex:Other ; ex:kind "blue" .
ex:e a ex:Other ; ex:kind "green" .
"#;

/// A COUNT query plus the variable its count is bound to.
struct CountQuery {
    name: &'static str,
    count_var: &'static str,
    sparql: &'static str,
}

/// One shape per COUNT-emitting fast path that tags its own datatype.
fn count_queries() -> Vec<CountQuery> {
    vec![
        // stats_query::stats_count_by_predicate_operator — the site the
        // main-reconcile merge conflicted on.
        CountQuery {
            name: "count_by_predicate",
            count_var: "c",
            sparql: "SELECT ?p (COUNT(?s) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p",
        },
        // detect_predicate_group_by_object_count_topk (PredicateGroupCountFirsts).
        CountQuery {
            name: "group_by_object_count_topk",
            count_var: "c",
            sparql: "PREFIX ex: <http://example.org/ns/>\n\
                     SELECT ?o (COUNT(?s) AS ?c) WHERE { ?s ex:kind ?o } \
                     GROUP BY ?o ORDER BY DESC(?c) LIMIT 10",
        },
        // detect_group_by_object_star_topk — a DISTINCT operator from the one
        // above, and the only route to `compute_group_by_object_star_topk`,
        // which is the second datatype site 45d6009bf corrected. It needs the
        // same-subject star shape (a second triple pattern on ?s), so the
        // single-pattern query above does not reach it.
        CountQuery {
            name: "group_by_object_star_topk",
            count_var: "c",
            sparql: "PREFIX ex: <http://example.org/ns/>\n\
                     SELECT ?o (COUNT(?s) AS ?c) WHERE { ?s ex:kind ?o . ?s ex:n ?x } \
                     GROUP BY ?o ORDER BY DESC(?c) LIMIT 10",
        },
        // detect_predicate_object_count (bound-object COUNT via POST FIRSTs).
        CountQuery {
            name: "count_class",
            count_var: "c",
            sparql: "PREFIX ex: <http://example.org/ns/>\n\
                     SELECT (COUNT(?s) AS ?c) WHERE { ?s a ex:Thing }",
        },
        // detect_predicate_count_rows.
        CountQuery {
            name: "count_predicate_rows",
            count_var: "c",
            sparql: "PREFIX ex: <http://example.org/ns/>\n\
                     SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:kind ?o }",
        },
        // detect_count_triples.
        CountQuery {
            name: "count_all_triples",
            count_var: "c",
            sparql: "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
        },
        // detect_count_distinct_position.
        CountQuery {
            name: "count_distinct_subjects",
            count_var: "c",
            sparql: "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }",
        },
    ]
}

/// Assert every binding of `count_var` is a literal typed exactly `xsd:integer`.
///
/// Strict: the datatype IRI is compared as a string, so `xsd:long "5"` fails
/// even though it is numerically equal to `xsd:integer "5"`.
fn assert_count_is_xsd_integer(results: &Value, count_var: &str, label: &str) {
    let bindings = results["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("{label}: no results.bindings array in {results}"));
    assert!(
        !bindings.is_empty(),
        "{label}: query returned no rows, so the datatype contract went untested"
    );
    for row in bindings {
        let cell = &row[count_var];
        assert_eq!(
            cell["type"].as_str(),
            Some("literal"),
            "{label}: ?{count_var} must be a literal, got {cell}"
        );
        assert_eq!(
            cell["datatype"].as_str(),
            Some(XSD_INTEGER),
            "{label}: SPARQL COUNT must be tagged {XSD_INTEGER} (§18.5.1.6), got {cell}"
        );
    }
}

/// Canonical, key-order-independent rendering of a SPARQL-JSON result set,
/// sorted so unordered result sets compare as multisets.
fn canonical_rows(results: &Value) -> Vec<String> {
    let mut rows: Vec<String> = results["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("no results.bindings array in {results}"))
        .iter()
        .map(|row| {
            let obj = row
                .as_object()
                .unwrap_or_else(|| panic!("binding row is not an object: {row}"));
            let mut vars: Vec<&String> = obj.keys().collect();
            vars.sort();
            vars.iter()
                .map(|var| {
                    let cell = &obj[*var];
                    format!(
                        "{var}={}|{}|{}|{}",
                        cell["type"].as_str().unwrap_or(""),
                        cell["value"].as_str().unwrap_or(""),
                        cell["datatype"].as_str().unwrap_or(""),
                        cell["xml:lang"].as_str().unwrap_or(""),
                    )
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    rows.sort();
    rows
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn count_results_are_tagged_xsd_integer() {
    // The kill switch OR's with this env var, so with it set the "fast paths
    // on" phase below would silently run generically and pin nothing.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pass vacuously. Unset it."
    );
    let _guard = FastPathGuard;

    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");
    let path = data_dir.path().join("00-counts.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(DATA.as_bytes()).expect("write ttl");

    // Bulk import builds a fully persisted index with no trailing novelty, so
    // `fast_path_store` holds (binary store present, query at max_t, root
    // policy, single ledger). A memory-backed ledger would decline every fast
    // path and make this test vacuous — which is exactly why the W3C suite
    // never caught the datatype bug.
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "test/count-datatype:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.expect("load");

    let queries = count_queries();

    // Phase 1 — fast paths on, under span capture so we can prove they fired.
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut fast_results = Vec::new();
    for q in &queries {
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let json = fluree
            .query(&db, q.sparql)
            .await
            .unwrap_or_else(|e| panic!("[fast] {}: {e}", q.name))
            .to_sparql_json(&ledger.snapshot)
            .unwrap_or_else(|e| panic!("[fast] {}: to_sparql_json: {e}", q.name));
        assert_count_is_xsd_integer(&json, q.count_var, &format!("fast/{}", q.name));
        fast_results.push(json);
    }

    // The two sites 45d6009bf corrected must actually have served these
    // queries; otherwise the assertions above only re-tested the generic
    // pipeline. `stamp_fast_path` emits `site` + `outcome` on the
    // `fluree::fastpath` target — `stats_count_by_predicate` /
    // `group_by_object_count_topk` are the plan-time stamps (the detector
    // matched), `COUNT by predicate (directory)` is the runtime stamp
    // (`FastPathOperator::open` computed a batch rather than declining).
    let outcomes: Vec<(String, String)> = store
        .find_events("fast-path outcome")
        .iter()
        .filter_map(|e| {
            Some((
                e.fields.get("site")?.clone(),
                e.fields.get("outcome")?.clone(),
            ))
        })
        .collect();
    // `group_by_object_star_topk` is deliberately absent: it sits on the fused
    // chain, which stamps one aggregate `fused_chain` verdict rather than a
    // per-detector one (per-detector verdicts are the PR-3 TODO in
    // `fast_path_outcome`). Its `count_datatype` coverage was verified by
    // mutation instead — retyping `compute_group_by_object_star_topk`'s
    // `dt_count` to xsd:long turns the `group_by_object_star_topk` case below
    // red. Add it here once that site stamps.
    for site in [
        "stats_count_by_predicate",
        "group_by_object_count_topk",
        "COUNT by predicate (directory)",
    ] {
        assert!(
            outcomes.iter().any(|(s, o)| s == site && o == "proceed"),
            "expected fast-path site {site} to proceed on an indexed ledger, so the \
             xsd:integer assertions above actually covered it; observed: {outcomes:?}"
        );
    }
    drop(tracing_guard);

    // Phase 2 — generic pipeline. Same strict datatype contract, and the two
    // pipelines must be indistinguishable on the wire (datatype included).
    set_fast_paths_disabled(true);
    for (q, fast) in queries.iter().zip(&fast_results) {
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
        let generic = fluree
            .query(&db, q.sparql)
            .await
            .unwrap_or_else(|e| panic!("[generic] {}: {e}", q.name))
            .to_sparql_json(&ledger.snapshot)
            .unwrap_or_else(|e| panic!("[generic] {}: to_sparql_json: {e}", q.name));
        assert_count_is_xsd_integer(&generic, q.count_var, &format!("generic/{}", q.name));
        assert_eq!(
            canonical_rows(fast),
            canonical_rows(&generic),
            "{}: fast and generic pipelines disagree on the wire",
            q.name
        );
    }
    set_fast_paths_disabled(false);
}
