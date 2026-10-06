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
//! A JSON-LD projection of a per-group list next to the key and the count is
//! also a canary pair for the two count fast paths, which output the key and
//! the count only. The list's IRIs are encoded on this ledger, so the same
//! queries pin that the JSON-LD formatters, DOM and streaming, materialize an
//! encoded binding inside a per-group list.
//!
//! A second indexed ledger holds overflow integers (beyond i64), which leave
//! the scan as encoded bindings whose dt_id says xsd:decimal: through grouping,
//! aggregates, per-group lists and the count top-k they must stay xsd:integer.
//!
//! Own binary: it toggles the process-global fast-path kill switch.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::format::format_results_string;
use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder, FormatterConfig};
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

/// [`sorted`], with each list cell (a per-group list) sorted too: the order of
/// a group's members is the order its rows reached the grouping.
fn sorted_lists(rows: &Value) -> Vec<Value> {
    let rows: Vec<Value> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let cells = row.as_array().expect("row").iter().map(|cell| match cell {
                Value::Array(list) => {
                    let mut list = list.clone();
                    list.sort_by_key(std::string::ToString::to_string);
                    Value::Array(list)
                }
                other => other.clone(),
            });
            Value::Array(cells.collect())
        })
        .collect();
    sorted(&Value::Array(rows))
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

/// Run one JSON-LD `query` and render its rows through both JSON-LD
/// formatters, the DOM one and the streaming one, which must agree.
async fn run_jsonld(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
    ledger: &fluree_db_api::LedgerState,
    query: &Value,
) -> Value {
    let result = fluree
        .query(db, query)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    let dom = result
        .to_jsonld(&ledger.snapshot)
        .unwrap_or_else(|e| panic!("to_jsonld: {e}\n{query}"));
    let streamed = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::jsonld(),
    )
    .unwrap_or_else(|e| panic!("format_results_string: {e}\n{query}"));
    let streamed: Value = serde_json::from_str(&streamed).expect("streamed JSON");
    assert_eq!(dom, streamed, "DOM and streaming JSON-LD differ\n{query}");
    dom
}

/// A JSON-LD routing pair for a count fast path: the key-and-count query it
/// must answer, and the same query projecting a per-group list it must
/// decline (it answered `null` for the list, or failed with "Projected
/// variable not in child schema"). Each query carries its hand-derived rows.
struct JsonLdPair {
    site: &'static str,
    key_only: (Value, Value),
    with_list: (Value, Value),
}

fn jsonld_list_pairs() -> Vec<JsonLdPair> {
    let ctx = json!({"ex": "http://example.org/"});
    vec![
        JsonLdPair {
            site: "group_by_object_count_topk",
            key_only: (
                json!({
                    "@context": ctx,
                    "select": ["?a", "(as (count ?e) ?n)"],
                    "where": {"@id": "?e", "ex:area": "?a"},
                    "groupBy": "?a", "orderBy": "(desc ?n)", "limit": 2
                }),
                json!([["Net", 3], ["Local", 2]]),
            ),
            with_list: (
                json!({
                    "@context": ctx,
                    "select": ["?a", "?e", "(as (count ?e) ?n)"],
                    "where": {"@id": "?e", "ex:area": "?a"},
                    "groupBy": "?a", "orderBy": "(desc ?n)", "limit": 2
                }),
                json!([
                    ["Net", ["ex:e1", "ex:e2", "ex:e3"], 3],
                    ["Local", ["ex:e4", "ex:e5"], 2]
                ]),
            ),
        },
        JsonLdPair {
            site: "COUNT by predicate (directory)",
            key_only: (
                json!({
                    "@context": ctx,
                    "select": ["?p", "(as (count ?o) ?n)"],
                    "where": {"@id": "?s", "?p": "?o"},
                    "groupBy": "?p"
                }),
                json!([["ex:age", 4], ["ex:area", 6], ["ex:kind", 6]]),
            ),
            with_list: (
                json!({
                    "@context": ctx,
                    "select": ["?p", "?s", "(as (count ?o) ?n)"],
                    "where": {"@id": "?s", "?p": "?o"},
                    "groupBy": "?p"
                }),
                json!([
                    ["ex:age", ["ex:e1", "ex:e2", "ex:e4", "ex:e6"], 4],
                    [
                        "ex:area",
                        ["ex:e1", "ex:e2", "ex:e3", "ex:e4", "ex:e5", "ex:e6"],
                        6
                    ],
                    [
                        "ex:kind",
                        ["ex:e1", "ex:e2", "ex:e3", "ex:e4", "ex:e5", "ex:e6"],
                        6
                    ]
                ]),
            ),
        },
    ]
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
        (
            "whole-graph scalar aggregates",
            "SELECT (COUNT(?e) AS ?n) (MAX(?age) AS ?m) \
             WHERE { { SELECT DISTINCT ?e WHERE { ?e ?p ?o } } OPTIONAL { ?e ex:age ?age } } \
             VALUES ?e { ex:e1 ex:e4 }",
            json!([[2, 30]]),
        ),
    ]
}

/// A file-backed Fluree holding `ledger_id`, bulk-imported (so indexed) from
/// the Turtle `ttl`. The directories live as long as the returned handles.
async fn indexed_ledger(
    ttl: &str,
    ledger_id: &str,
) -> (
    [TempDir; 2],
    fluree_db_api::Fluree,
    fluree_db_api::LedgerState,
) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");
    std::fs::File::create(data_dir.path().join("00-data.ttl"))
        .expect("create ttl")
        .write_all(ttl.as_bytes())
        .expect("write ttl");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
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
    ([db_dir, data_dir], fluree, ledger)
}

/// The fast-path sites that recorded `proceed` since event `before`.
fn proceeded(store: &span_capture::SpanStore, before: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect()
}

/// A query no fast path may answer runs the generic pipeline, whose only
/// stamp is the fused chain it passed through (a sibling fast path, such as
/// the count planner, would add its own).
fn generic_only(sites: &[String]) -> bool {
    sites.iter().all(|s| s == "fused_chain")
}

#[tokio::test]
async fn grouped_select_expression_on_an_indexed_ledger_in_both_lanes() {
    let (_dirs, fluree, ledger) = indexed_ledger(FIXTURE, "grouped-projection/lanes:main").await;
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

    set_fast_paths_disabled(false);
    let mut misrouted: Vec<String> = Vec::new();
    for pair in &pairs {
        for ((body, _), must_fire) in [(&pair.key_only, true), (&pair.with_bind, false)] {
            let before = store.find_events("fast-path outcome").len();
            run(&fluree, &db, &ledger, body).await;
            let sites = proceeded(&store, before);
            if sites.iter().any(|s| s == pair.site) != must_fire {
                misrouted.push(format!(
                    "`{}` must {}proceed [proceeded: {sites:?}]\n{body}",
                    pair.site,
                    if must_fire { "" } else { "not " }
                ));
            }
            if !must_fire && !generic_only(&sites) {
                misrouted.push(format!(
                    "no fast path may answer [proceeded: {sites:?}]\n{body}"
                ));
            }
        }
    }
    let jsonld_pairs = jsonld_list_pairs();
    for pair in &jsonld_pairs {
        for ((query, _), must_fire) in [(&pair.key_only, true), (&pair.with_list, false)] {
            let before = store.find_events("fast-path outcome").len();
            run_jsonld(&fluree, &db, &ledger, query).await;
            let sites = proceeded(&store, before);
            if sites.iter().any(|s| s == pair.site) != must_fire {
                misrouted.push(format!(
                    "`{}` must {}proceed [proceeded: {sites:?}]\n{query}",
                    pair.site,
                    if must_fire { "" } else { "not " }
                ));
            }
            if !must_fire && !generic_only(&sites) {
                misrouted.push(format!(
                    "no fast path may answer [proceeded: {sites:?}]\n{query}"
                ));
            }
        }
    }
    for (site, body, _) in &values_cases {
        let before = store.find_events("fast-path outcome").len();
        run(&fluree, &db, &ledger, body).await;
        let sites = proceeded(&store, before);
        if sites.iter().any(|s| s == site) || !generic_only(&sites) {
            misrouted.push(format!(
                "`{site}` (or any fast path) must not proceed with a trailing VALUES \
                 [proceeded: {sites:?}]\n{body}"
            ));
        }
    }
    drop(tracing_guard);
    assert!(misrouted.is_empty(), "\n{}", misrouted.join("\n\n"));

    // A per-group list of IRIs with no fast path in reach: the formatters'
    // case alone.
    let ctx = json!({"ex": "http://example.org/"});
    let mut jsonld_cases = vec![(
        json!({
            "@context": ctx,
            "select": ["?a", "?e"],
            "where": {"@id": "?e", "ex:area": "?a"},
            "groupBy": "?a"
        }),
        json!([
            ["Local", ["ex:e4", "ex:e5"]],
            ["Net", ["ex:e1", "ex:e2", "ex:e3"]],
            ["Remote", ["ex:e6"]]
        ]),
    )];
    for pair in &jsonld_pairs {
        jsonld_cases.push(pair.key_only.clone());
        jsonld_cases.push(pair.with_list.clone());
    }

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
        for (query, expected) in &jsonld_cases {
            let rows = run_jsonld(&fluree, &db, &ledger, query).await;
            assert_eq!(
                sorted_lists(&rows),
                sorted_lists(expected),
                "fast paths disabled = {disabled}\n{query}"
            );
        }
    }

    // In the same test: the kill switch is process-global.
    overflow_integers_through_grouping().await;
}

/// b1–b3 have the same overflow integer size (beyond i64), b4 size 7; b1, b3
/// and b4 are tagged t1, b2 t2.
const OVERFLOW_FIXTURE: &str = r#"@prefix ex: <http://example.org/> .
ex:b1 ex:size 123456789012345678901234567890 ; ex:tag "t1" .
ex:b2 ex:size 123456789012345678901234567890 ; ex:tag "t2" .
ex:b3 ex:size 123456789012345678901234567890 ; ex:tag "t1" .
ex:b4 ex:size 7 ; ex:tag "t1" .
"#;
const BIG: &str = "123456789012345678901234567890";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// Run one SPARQL `body` and render each row's cells, in the order of `vars`,
/// from SPARQL JSON: a typed literal as `[value, datatype]`, anything else as
/// its value.
async fn run_typed(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
    ledger: &fluree_db_api::LedgerState,
    vars: &[&str],
    body: &str,
) -> Value {
    let query = format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{body}"
    );
    let json = fluree
        .query(db, query.as_str())
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"))
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let rows = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|binding| {
            let cells = vars.iter().map(|var| {
                let cell = &binding[*var];
                match cell.get("datatype") {
                    Some(dt) => json!([cell["value"], dt]),
                    None => cell["value"].clone(),
                }
            });
            Value::Array(cells.collect())
        })
        .collect();
    Value::Array(rows)
}

/// Every `@type` in a typed-JSON rendering, sorted.
fn typed_json_types(value: &Value) -> Vec<String> {
    fn walk(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(dt)) = map.get("@type") {
                    out.push(dt.clone());
                }
                map.values().for_each(|v| walk(v, out));
            }
            Value::Array(items) => items.iter().for_each(|v| walk(v, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(value, &mut out);
    out.sort();
    out
}

/// An overflow integer on an indexed ledger leaves the scan as an encoded
/// binding whose dt_id says xsd:decimal; only the decoded value says
/// xsd:integer. Through the grouping it must stay xsd:integer, in both lanes:
/// as a GROUP BY key read by a per-group SELECT expression and by HAVING, as
/// an aggregate input (MIN, MAX, SAMPLE, and the implicit SAMPLE of a non-key
/// variable HAVING reads), inside a JSON-LD per-group list, and as the key of
/// the count top-k, which must take the key-and-count query and decline the
/// same query with a per-group list or a SELECT expression.
async fn overflow_integers_through_grouping() {
    let (_dirs, fluree, ledger) =
        indexed_ledger(OVERFLOW_FIXTURE, "grouped-projection/overflow:main").await;
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);
    let big = json!([BIG, XSD_INTEGER]);
    let int = |n: i64| json!([n.to_string(), XSD_INTEGER]);

    const TOPK: &str = "WHERE { ?b ex:size ?z } GROUP BY ?z ORDER BY DESC(?n) LIMIT 2";
    let topk_key_only = format!("SELECT ?z (COUNT(?b) AS ?n) {TOPK}");
    let topk_with_bind = format!("SELECT ?z (COUNT(?b) AS ?n) (DATATYPE(?z) AS ?dt) {TOPK}");
    let sparql_cases: Vec<(&[&str], String, Value)> = vec![
        (
            &["z", "n"],
            topk_key_only.clone(),
            json!([[big, int(3)], [int(7), int(1)]]),
        ),
        (
            &["z", "n", "dt"],
            topk_with_bind.clone(),
            json!([[big, int(3), XSD_INTEGER], [int(7), int(1), XSD_INTEGER]]),
        ),
        (
            &["z", "n"],
            "SELECT ?z (COUNT(?b) AS ?n) WHERE { ?b ex:size ?z } GROUP BY ?z \
             HAVING (DATATYPE(?z) = xsd:integer)"
                .to_string(),
            json!([[big, int(3)], [int(7), int(1)]]),
        ),
        (
            &["t", "mn", "mx"],
            "SELECT ?t (MIN(?z) AS ?mn) (MAX(?z) AS ?mx) \
             WHERE { ?b ex:tag ?t ; ex:size ?z } GROUP BY ?t"
                .to_string(),
            json!([["t1", int(7), big], ["t2", big, big]]),
        ),
        (
            &["t", "s"],
            "SELECT ?t (SAMPLE(?z) AS ?s) \
             WHERE { ?b ex:tag ?t ; ex:size ?z FILTER(?t = \"t2\") } GROUP BY ?t"
                .to_string(),
            json!([["t2", big]]),
        ),
        (
            &["t"],
            "SELECT ?t WHERE { ?b ex:tag ?t ; ex:size ?z } GROUP BY ?t \
             HAVING (DATATYPE(?z) = xsd:integer)"
                .to_string(),
            json!([["t1"], ["t2"]]),
        ),
    ];

    let ctx = json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"});
    let jsonld_topk = |select: Value| {
        json!({
            "@context": ctx, "select": select, "where": {"@id": "?b", "ex:size": "?z"},
            "groupBy": "?z", "orderBy": "(desc ?n)", "limit": 2
        })
    };
    let jsonld_key_only = jsonld_topk(json!(["?z", "(as (count ?b) ?n)"]));
    let jsonld_with_list = jsonld_topk(json!(["?z", "?b", "(as (count ?b) ?n)"]));
    let jsonld_cases = vec![
        (jsonld_key_only.clone(), json!([[BIG, 3], [7, 1]])),
        (
            jsonld_with_list.clone(),
            json!([[BIG, ["ex:b1", "ex:b2", "ex:b3"], 3], [7, ["ex:b4"], 1]]),
        ),
        (
            jsonld_topk(json!([
                "?z",
                "(as (count ?b) ?n)",
                "(as (datatype ?z) ?dt)"
            ])),
            json!([[BIG, 3, "xsd:integer"], [7, 1, "xsd:integer"]]),
        ),
        (
            json!({
                "@context": ctx, "select": ["?t", "?z"],
                "where": {"@id": "?b", "ex:tag": "?t", "ex:size": "?z"}, "groupBy": "?t"
            }),
            json!([["t1", [BIG, BIG, 7]], ["t2", [BIG]]]),
        ),
        (
            json!({
                "@context": ctx, "select": ["?t", "(as (min ?z) ?mn)", "(as (max ?z) ?mx)"],
                "where": {"@id": "?b", "ex:tag": "?t", "ex:size": "?z"}, "groupBy": "?t"
            }),
            json!([["t1", 7, BIG], ["t2", BIG, BIG]]),
        ),
    ];

    // Routing: the count top-k takes the key-and-count query on an overflow
    // key and declines it with a SELECT expression or a per-group list.
    let _guard = FastPathsGuard;
    set_fast_paths_disabled(false);
    let (store, tracing_guard) = span_capture::init_test_tracing();
    let mut misrouted: Vec<String> = Vec::new();
    for (query, must_fire) in [
        (json!(topk_key_only), true),
        (json!(topk_with_bind), false),
        (jsonld_key_only, true),
        (jsonld_with_list, false),
    ] {
        let before = store.find_events("fast-path outcome").len();
        match &query {
            Value::String(body) => {
                run_typed(&fluree, &db, &ledger, &[], body).await;
            }
            _ => {
                run_jsonld(&fluree, &db, &ledger, &query).await;
            }
        }
        let sites = proceeded(&store, before);
        let fired = sites.iter().any(|s| s == "group_by_object_count_topk");
        if fired != must_fire || (!must_fire && !generic_only(&sites)) {
            misrouted.push(format!(
                "`group_by_object_count_topk` must {}proceed [proceeded: {sites:?}]\n{query}",
                if must_fire { "" } else { "not " }
            ));
        }
    }
    drop(tracing_guard);
    assert!(misrouted.is_empty(), "\n{}", misrouted.join("\n\n"));

    for disabled in [false, true] {
        set_fast_paths_disabled(disabled);
        for (vars, body, expected) in &sparql_cases {
            let rows = run_typed(&fluree, &db, &ledger, vars, body).await;
            assert_eq!(
                sorted(&rows),
                sorted(expected),
                "fast paths disabled = {disabled}\n{body}"
            );
        }
        for (query, expected) in &jsonld_cases {
            let rows = run_jsonld(&fluree, &db, &ledger, query).await;
            assert_eq!(
                sorted_lists(&rows),
                sorted_lists(expected),
                "fast paths disabled = {disabled}\n{query}"
            );
        }
        // JSON-LD renders an overflow integer as a bare string whatever its
        // datatype; typed JSON shows it. Every size in the per-group lists and
        // every MIN / MAX is an xsd:integer.
        for (query, sizes) in [(&jsonld_cases[3].0, 4), (&jsonld_cases[4].0, 4)] {
            let result = fluree
                .query(&db, query)
                .await
                .unwrap_or_else(|e| panic!("{e}\n{query}"));
            let typed = format_results_string(
                &result,
                &result.context,
                &ledger.snapshot,
                &FormatterConfig::typed_json(),
            )
            .unwrap_or_else(|e| panic!("typed json: {e}\n{query}"));
            let typed: Value = serde_json::from_str(&typed).expect("typed JSON");
            let types: Vec<String> = typed_json_types(&typed)
                .into_iter()
                .filter(|dt| dt != "xsd:string")
                .collect();
            assert_eq!(
                types,
                vec!["xsd:integer".to_string(); sizes],
                "fast paths disabled = {disabled}\n{query}\n{typed:#}"
            );
        }
    }
}
