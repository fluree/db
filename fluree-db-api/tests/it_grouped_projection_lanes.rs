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
//! Routing is stamped as canary pairs, one per aggregate fast path that gates
//! on the grouping's binds: the key-only query must take the fast path
//! (MustFire, which also shows the fixture reaches it on an index-backed
//! ledger) and the same query with a SELECT expression must not (MustNotFire),
//! since the fast path cannot evaluate the expression. The `GROUP BY ?o` count
//! top-k and the per-predicate directory count decline it on their binds gate
//! alone; the others also decline it because their projection check refuses
//! the expression's column.
//!
//! A trailing VALUES clause joins after the WHERE tree, which no fast path
//! reads: each such query must decline the fast path its WHERE and grouping
//! would otherwise take, and answer with the VALUES applied.
//!
//! Own binary: it toggles the process-global fast-path kill switch.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder};
use serde_json::{json, Value};
use std::io::Write;
use tempfile::TempDir;

/// Six entities in three areas, each with a kind; four with an age.
const FIXTURE: &str = r#"@prefix ex: <http://example.org/> .
ex:e1 ex:area "Net" ; ex:kind "k1" ; ex:age 30 .
ex:e2 ex:area "Net" ; ex:kind "k2" ; ex:age 40 .
ex:e3 ex:area "Net" ; ex:kind "k1" .
ex:e4 ex:area "Local" ; ex:kind "k2" ; ex:age 25 .
ex:e5 ex:area "Local" ; ex:kind "k1" .
ex:e6 ex:area "Remote" ; ex:kind "k2" ; ex:age 50 .
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

/// A fast path's routing pair: the stamp `site` its operator records when it
/// answers, the key-only query it must answer, and the same query with a
/// grouping bind (a SELECT expression) it must decline. Each query carries
/// its hand-derived rows.
struct CanaryPair {
    site: &'static str,
    key_only: (String, Value),
    with_bind: (String, Value),
}

fn canary_pairs(seg: &str) -> Vec<CanaryPair> {
    vec![
        CanaryPair {
            site: "group_by_object_count_topk",
            key_only: (
                "SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a } GROUP BY ?a \
                 ORDER BY DESC(?n) LIMIT 2"
                    .to_string(),
                json!([["Net", 3], ["Local", 2]]),
            ),
            with_bind: (
                format!(
                    "SELECT {seg} (COUNT(?e) AS ?n) WHERE {{ ?e ex:area ?a }} GROUP BY ?a \
                     ORDER BY DESC(?n) LIMIT 2"
                ),
                json!([["network", 3], ["other", 2]]),
            ),
        },
        CanaryPair {
            site: "group_by_object_star_topk",
            key_only: (
                "SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a ; ex:kind ?k } GROUP BY ?a \
                 ORDER BY DESC(?n) LIMIT 2"
                    .to_string(),
                json!([["Net", 3], ["Local", 2]]),
            ),
            with_bind: (
                "SELECT ?a (COUNT(?e) AS ?n) (?n * 10 AS ?t) \
                 WHERE { ?e ex:area ?a ; ex:kind ?k } GROUP BY ?a ORDER BY DESC(?n) LIMIT 2"
                    .to_string(),
                json!([["Net", 3, 30], ["Local", 2, 20]]),
            ),
        },
        CanaryPair {
            site: "COUNT by predicate (directory)",
            key_only: (
                "SELECT ?p (COUNT(?o) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p".to_string(),
                json!([["ex:age", 4], ["ex:area", 6], ["ex:kind", 6]]),
            ),
            with_bind: (
                "SELECT ?p (COUNT(?o) AS ?n) (STR(?p) AS ?ps) WHERE { ?s ?p ?o } GROUP BY ?p"
                    .to_string(),
                json!([
                    ["ex:age", 4, "http://example.org/age"],
                    ["ex:area", 6, "http://example.org/area"],
                    ["ex:kind", 6, "http://example.org/kind"]
                ]),
            ),
        },
        CanaryPair {
            site: "COUNT rows",
            key_only: (
                "SELECT (COUNT(*) AS ?n) WHERE { ?e ex:area ?a }".to_string(),
                json!([[6]]),
            ),
            with_bind: (
                "SELECT (COUNT(*) AS ?n) (?n * 10 AS ?m) WHERE { ?e ex:area ?a }".to_string(),
                json!([[6, 60]]),
            ),
        },
        CanaryPair {
            site: "SUM(?o)",
            key_only: (
                "SELECT (SUM(?g) AS ?s) WHERE { ?e ex:age ?g }".to_string(),
                json!([[145]]),
            ),
            with_bind: (
                "SELECT (SUM(?g) AS ?s) (?s + 1 AS ?t) WHERE { ?e ex:age ?g }".to_string(),
                json!([[145, 146]]),
            ),
        },
        CanaryPair {
            site: "whole-graph scalar aggregates",
            key_only: (
                "SELECT (COUNT(?e) AS ?n) (MAX(?age) AS ?m) \
                 WHERE { { SELECT DISTINCT ?e WHERE { ?e ?p ?o } } OPTIONAL { ?e ex:age ?age } }"
                    .to_string(),
                json!([[6, 50]]),
            ),
            with_bind: (
                "SELECT (COUNT(?e) AS ?n) (MAX(?age) AS ?m) (?n + ?m AS ?s) \
                 WHERE { { SELECT DISTINCT ?e WHERE { ?e ?p ?o } } OPTIONAL { ?e ex:age ?age } }"
                    .to_string(),
                json!([[6, 50, 56]]),
            ),
        },
    ]
}

/// Queries with a trailing VALUES clause, each shaped for the fast path
/// `site`, which must not take it: the fast path would ignore the VALUES (each
/// answered as if it were absent before the fix).
fn trailing_values_cases() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "COUNT rows",
            r#"SELECT (COUNT(*) AS ?n) WHERE { ?e ex:area ?a } VALUES ?a { "Net" }"#,
            json!([[3]]),
        ),
        (
            "group_by_object_count_topk",
            r#"SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a } GROUP BY ?a
               ORDER BY DESC(?n) LIMIT 2 VALUES ?a { "Local" }"#,
            json!([["Local", 2]]),
        ),
        (
            "group_by_object_star_topk",
            r#"SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a ; ex:kind ?k } GROUP BY ?a
               ORDER BY DESC(?n) LIMIT 2 VALUES ?k { "k1" }"#,
            json!([["Net", 2], ["Local", 1]]),
        ),
        (
            "COUNT by predicate (directory)",
            "SELECT ?p (COUNT(?o) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p VALUES ?s { ex:e1 }",
            json!([["ex:age", 1], ["ex:area", 1], ["ex:kind", 1]]),
        ),
        (
            "SUM(?o)",
            "SELECT (SUM(?g) AS ?s) WHERE { ?e ex:age ?g } VALUES ?e { ex:e1 }",
            json!([[30]]),
        ),
    ]
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
    let mut cases = vec![
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
    ];
    let pairs = canary_pairs(seg);
    for pair in &pairs {
        cases.push(pair.key_only.clone());
        cases.push(pair.with_bind.clone());
    }
    let values_cases = trailing_values_cases();
    for (_, body, expected) in &values_cases {
        cases.push(((*body).to_string(), expected.clone()));
    }

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
    let mut misrouted: Vec<String> = Vec::new();
    for pair in &pairs {
        for ((body, _), must_fire) in [(&pair.key_only, true), (&pair.with_bind, false)] {
            let before = store.find_events("fast-path outcome").len();
            run(&fluree, &db, &ledger, body).await;
            let sites = proceeded(before);
            if sites.iter().any(|s| s == pair.site) != must_fire {
                misrouted.push(format!(
                    "`{}` must {}proceed [proceeded: {sites:?}]\n{body}",
                    pair.site,
                    if must_fire { "" } else { "not " }
                ));
            }
        }
    }
    for (site, body, _) in &values_cases {
        let before = store.find_events("fast-path outcome").len();
        run(&fluree, &db, &ledger, body).await;
        let sites = proceeded(before);
        if sites.iter().any(|s| s == site) {
            misrouted.push(format!(
                "`{site}` must not proceed with a trailing VALUES [proceeded: {sites:?}]\n{body}"
            ));
        }
    }
    drop(tracing_guard);
    assert!(misrouted.is_empty(), "\n{}", misrouted.join("\n\n"));

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
