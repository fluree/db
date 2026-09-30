//! #1978 on a bulk-imported, indexed ledger, with the fast paths on and off.
//!
//! A grouped SELECT expression now rides in the grouping phase as a per-group
//! `Extend`, including under a dedup-only `GROUP BY` that has no aggregation
//! stage. A fast-path detector that matched the grouping without looking at its
//! binds would answer without the expression. A `FlureeBuilder::memory()`
//! ledger declines every index-backed fast path, so this runs on an indexed
//! ledger, once per lane. The oracle is the hand-derived answer, not agreement
//! between the lanes.
//!
//! Routing is stamped on the one fast path these shapes can reach, the
//! `GROUP BY ?o` count top-k: it must serve the key-only query (MustFire, which
//! also shows the ledger is index-backed) and decline the same query with a
//! SELECT expression (MustNotFire), whose binds it cannot evaluate. The other
//! cases reach no per-detector fast path on this fixture.
//!
//! Own binary: it toggles the process-global fast-path kill switch.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

const FIXTURE: &str = r#"@prefix ex: <http://example.org/> .
ex:e1 ex:area "Net" .
ex:e2 ex:area "Net" .
ex:e3 ex:area "Net" .
ex:e4 ex:area "Local" .
ex:e5 ex:area "Local" .
ex:e6 ex:area "Remote" .
"#;

/// Restores the fast paths when the test ends, panicking or not.
struct FastPathsGuard;

impl Drop for FastPathsGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

fn sorted(rows: &Value) -> Vec<Value> {
    let mut rows = rows.as_array().expect("rows").clone();
    rows.sort_by_key(std::string::ToString::to_string);
    rows
}

/// Run one SPARQL `body` (prefix added) and render its rows as JSON-LD.
async fn run(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
    ledger: &fluree_db_api::LedgerState,
    body: &str,
) -> Value {
    let query = format!("PREFIX ex: <http://example.org/>\n{body}");
    fluree
        .query(db, query.as_str())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

#[tokio::test]
async fn grouped_select_expression_on_an_indexed_ledger_in_both_lanes() {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");
    std::fs::File::create(data_dir.path().join("00-areas.ttl"))
        .expect("create ttl")
        .write_all(FIXTURE.as_bytes())
        .expect("write ttl");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "grouped-projection/lanes:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);

    let seg = r#"(IF(?a = "Net", "network", "other") AS ?seg)"#;
    let cases = [
        (
            format!("SELECT {seg} (COUNT(?e) AS ?n) WHERE {{ ?e ex:area ?a }} GROUP BY ?a"),
            json!([["network", 3], ["other", 2], ["other", 1]]),
        ),
        (
            format!("SELECT {seg} WHERE {{ ?e ex:area ?a }} GROUP BY ?a"),
            json!([["network"], ["other"], ["other"]]),
        ),
        (
            format!(
                "SELECT ?seg (SUM(?n) AS ?t) WHERE {{ \
                   {{ SELECT {seg} (COUNT(?e) AS ?n) WHERE {{ ?e ex:area ?a }} GROUP BY ?a }} \
                 }} GROUP BY ?seg"
            ),
            json!([["network", 3], ["other", 3]]),
        ),
        (
            format!(
                "SELECT {seg} (COUNT(?e) AS ?n) WHERE {{ ?e ex:area ?a }} GROUP BY ?a \
                 ORDER BY DESC(?n) LIMIT 2"
            ),
            json!([["network", 3], ["other", 2]]),
        ),
    ];

    // The top-k fast path serves the key-only form (the canary) and must
    // decline the same query with the SELECT expression (the last case).
    let canary = "SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a } GROUP BY ?a \
                  ORDER BY DESC(?n) LIMIT 2";
    let with_expression = cases.last().expect("cases").0.as_str();
    const SITE: &str = "group_by_object_count_topk";

    let _guard = FastPathsGuard;
    let (store, tracing_guard) = span_capture::init_test_tracing();
    let proceeded = |before: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[before..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect()
    };

    set_fast_paths_disabled(false);
    for (body, must_fire) in [(canary, true), (with_expression, false)] {
        let before = store.find_events("fast-path outcome").len();
        run(&fluree, &db, &ledger, body).await;
        let sites = proceeded(before);
        assert_eq!(
            sites.iter().any(|s| s == SITE),
            must_fire,
            "`{SITE}` must {}proceed [proceeded: {sites:?}]\n{body}",
            if must_fire { "" } else { "not " }
        );
    }
    drop(tracing_guard);

    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        for (body, expected) in &cases {
            let rows = run(&fluree, &db, &ledger, body).await;
            assert_eq!(
                sorted(&rows),
                sorted(expected),
                "fast paths disabled = {disabled}\n{body}"
            );
        }
    }
}
