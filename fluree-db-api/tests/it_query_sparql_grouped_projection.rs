//! SELECT expressions of a grouped query level (#1978).
//!
//! SPARQL 1.1 §18.2.4.4: a SELECT expression of a grouped level is an
//! `Extend` over the group rows, evaluated once per group after HAVING, in
//! SELECT order. It used to be lowered as a per-solution `BIND` before
//! grouping, so the grouping stage carried it as a per-group list and the
//! SPARQL-results formatters expanded that list into one row per solution.
//!
//! The fixture maps three areas onto two labels (`IF(?a = "Net", "network",
//! "other")`), so a lowering that grouped by the expression's value instead of
//! the key would return two rows where the spec returns three.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::format::format_results_string;
use fluree_db_api::{FlureeBuilder, FormatterConfig, QueryResult};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";
const W: &str = "WHERE { ?e ex:area ?a }";
const SEG: &str = r#"(IF(?a = "Net", "network", "other") AS ?seg)"#;

/// e1–e3 Net, e4–e5 Local, e6 Remote: 6 solutions, 3 groups, 2 labels.
async fn seed_areas(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:e1", "ex:area": "Net"},
            {"@id": "ex:e2", "ex:area": "Net"},
            {"@id": "ex:e3", "ex:area": "Net"},
            {"@id": "ex:e4", "ex:area": "Local"},
            {"@id": "ex:e5", "ex:area": "Local"},
            {"@id": "ex:e6", "ex:area": "Remote"}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("seed").ledger
}

async fn run(fluree: &MemoryFluree, ledger: &MemoryLedger, body: &str) -> QueryResult {
    let query = format!("{PREFIX}{body}");
    support::query_sparql(fluree, ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"))
}

/// One SPARQL-JSON binding as `var → lexical value`.
type Row = BTreeMap<String, String>;

/// SPARQL-JSON bindings, in result order.
fn sparql_rows(result: &QueryResult, ledger: &MemoryLedger) -> Vec<Row> {
    let sj = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    sj["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            b.as_object()
                .expect("binding object")
                .iter()
                .map(|(k, v)| (k.clone(), v["value"].as_str().expect("value").to_string()))
                .collect()
        })
        .collect()
}

/// Expected rows written as `json!([{"var": "value"}, …])`, in order.
fn rows(expected: &JsonValue) -> Vec<Row> {
    expected
        .as_array()
        .expect("expected rows")
        .iter()
        .map(|r| {
            r.as_object()
                .expect("expected row object")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("string value").to_string()))
                .collect()
        })
        .collect()
}

/// Sorted, for unordered comparison.
fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

/// Assert both renderings of one query: the SPARQL-JSON rows (the surface
/// that expanded lists) and the JSON-LD rows (which showed the lists).
async fn assert_rows(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    body: &str,
    sparql: JsonValue,
    jsonld: JsonValue,
) {
    let result = run(fluree, ledger, body).await;
    assert_eq!(
        sorted(sparql_rows(&result, ledger)),
        sorted(rows(&sparql)),
        "SPARQL-JSON rows for\n{body}"
    );
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&jsonld),
        "JSON-LD rows for\n{body}"
    );
}

#[tokio::test]
async fn grouped_select_expression_one_row_per_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/rows:main").await;

    // #1978 itself: 6 rows today (one per solution).
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
        json!([
            {"seg": "network", "n": "3"},
            {"seg": "other", "n": "2"},
            {"seg": "other", "n": "1"}
        ]),
        json!([["network", 3], ["other", 2], ["other", 1]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT (CONCAT(?a, "!") AS ?x) (COUNT(?e) AS ?n) {W} GROUP BY ?a"#),
        json!([
            {"x": "Net!", "n": "3"},
            {"x": "Local!", "n": "2"},
            {"x": "Remote!", "n": "1"}
        ]),
        json!([["Net!", 3], ["Local!", 2], ["Remote!", 1]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT (?a AS ?b) (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
        json!([
            {"b": "Net", "n": "3"},
            {"b": "Local", "n": "2"},
            {"b": "Remote", "n": "1"}
        ]),
        json!([["Net", 3], ["Local", 2], ["Remote", 1]]),
    )
    .await;
    // Two expression columns: the per-group product (Σ k² = 14 rows today).
    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT {SEG} (CONCAT(?a, "!") AS ?x) (COUNT(?e) AS ?n) {W} GROUP BY ?a"#),
        json!([
            {"seg": "network", "x": "Net!", "n": "3"},
            {"seg": "other", "x": "Local!", "n": "2"},
            {"seg": "other", "x": "Remote!", "n": "1"}
        ]),
        json!([
            ["network", "Net!", 3],
            ["other", "Local!", 2],
            ["other", "Remote!", 1]
        ]),
    )
    .await;
    // Implicit grouping via a SELECT aggregate.
    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT ("x" AS ?c) (COUNT(*) AS ?n) {W}"#),
        json!([{"c": "x", "n": "6"}]),
        json!([["x", 6]]),
    )
    .await;
    // Dedup-only GROUP BY: no aggregation stage, still one row per group.
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} {W} GROUP BY ?a"),
        json!([{"seg": "network"}, {"seg": "other"}, {"seg": "other"}]),
        json!([["network"], ["other"], ["other"]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT DISTINCT {SEG} {W} GROUP BY ?a"),
        json!([{"seg": "network"}, {"seg": "other"}]),
        json!([["network"], ["other"]]),
    )
    .await;
    // DISTINCT runs on the group rows, which are already distinct.
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT DISTINCT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
        json!([
            {"seg": "network", "n": "3"},
            {"seg": "other", "n": "2"},
            {"seg": "other", "n": "1"}
        ]),
        json!([["network", 3], ["other", 2], ["other", 1]]),
    )
    .await;
    // HAVING filters the groups before the Extend runs.
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (COUNT(?e) > 1)"),
        json!([{"seg": "network", "n": "3"}, {"seg": "other", "n": "2"}]),
        json!([["network", 3], ["other", 2]]),
    )
    .await;
    // The `GROUP BY (LCASE(?a))` shortcut: the key's own expression (#1333).
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT (LCASE(?a) AS ?k) (COUNT(?e) AS ?n) {W} GROUP BY ?a (LCASE(?a))"),
        json!([
            {"k": "net", "n": "3"},
            {"k": "local", "n": "2"},
            {"k": "remote", "n": "1"}
        ]),
        json!([["net", 3], ["local", 2], ["remote", 1]]),
    )
    .await;
}

/// Alias chains: one Extend list in SELECT order, so an expression over a
/// compound aggregate's alias (or over an alias of an aggregate alias) sees its
/// input. Both used to leave `?t` unbound.
#[tokio::test]
async fn grouped_select_expression_alias_chains() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/chains:main").await;
    for body in [
        format!(
            "SELECT ?a (COUNT(?e) AS ?n) ((COUNT(?e) + 1) AS ?np1) ((?np1 * 10) AS ?t) {W} \
             GROUP BY ?a"
        ),
        format!(
            "SELECT ?a (COUNT(?e) AS ?n) ((?n + 1) AS ?np1) ((?np1 * 10) AS ?t) {W} GROUP BY ?a"
        ),
    ] {
        assert_rows(
            &fluree,
            &ledger,
            &body,
            json!([
                {"a": "Net", "n": "3", "np1": "4", "t": "40"},
                {"a": "Local", "n": "2", "np1": "3", "t": "30"},
                {"a": "Remote", "n": "1", "np1": "2", "t": "20"}
            ]),
            json!([["Net", 3, 4, 40], ["Local", 2, 3, 30], ["Remote", 1, 2, 20]]),
        )
        .await;
    }
}

/// ORDER BY and LIMIT run on the group rows, and an ORDER BY may read the
/// SELECT alias (it used to be rejected as a grouped variable).
#[tokio::test]
async fn grouped_select_expression_order_by_and_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/order:main").await;

    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a ORDER BY ?seg ?n"),
    )
    .await;
    assert_eq!(
        sparql_rows(&result, &ledger),
        rows(&json!([
            {"seg": "network", "n": "3"},
            {"seg": "other", "n": "1"},
            {"seg": "other", "n": "2"}
        ]))
    );

    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a ORDER BY DESC(?n) LIMIT 1"),
    )
    .await;
    assert_eq!(
        sparql_rows(&result, &ledger),
        rows(&json!([{"seg": "network", "n": "3"}]))
    );
}

/// A sub-SELECT's grouped expression reaches the enclosing query as one scalar
/// per group: an outer FILTER, GROUP BY and aggregate all read scalars. (A
/// debug-build panic, a merged group and an empty GROUP_CONCAT before.)
#[tokio::test]
async fn sub_select_grouped_expression_is_a_scalar_outside() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/subselect:main").await;
    let inner = format!("{{ SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a }}");

    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT ?n WHERE {{ {inner} FILTER(?seg = "network") }}"#),
        json!([{"n": "3"}]),
        json!([[3]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?seg (SUM(?n) AS ?t) WHERE {{ {inner} }} GROUP BY ?seg"),
        json!([{"seg": "network", "t": "3"}, {"seg": "other", "t": "3"}]),
        json!([["network", 3], ["other", 3]]),
    )
    .await;

    let result = run(
        &fluree,
        &ledger,
        &format!(
            r#"SELECT (COUNT(?seg) AS ?c) (GROUP_CONCAT(?seg; SEPARATOR="|") AS ?g) WHERE {{ {inner} }}"#
        ),
    )
    .await;
    let found = sparql_rows(&result, &ledger);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0]["c"], "3");
    let mut parts: Vec<&str> = found[0]["g"].split('|').collect();
    parts.sort_unstable();
    assert_eq!(parts, vec!["network", "other", "other"]);
}

/// §18.2.4.2: HAVING runs before the SELECT expressions, so it reads a SELECT
/// alias as unbound — no row qualifies, and nothing panics (a debug-build panic
/// in `eval` before).
#[tokio::test]
async fn having_reads_a_select_alias_as_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/having-alias:main").await;
    for having in [r#"(?seg = "network")"#, "(?nosuch = 1)"] {
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING {having}"),
        )
        .await;
        assert_eq!(result.row_count(), 0, "HAVING {having}");
    }
}

/// A non-deterministic expression runs once per group: one UUID per group,
/// three distinct UUIDs.
#[tokio::test]
async fn grouped_struuid_runs_once_per_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/struuid:main").await;
    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT ?a (STRUUID() AS ?u) (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
    )
    .await;
    let found = sparql_rows(&result, &ledger);
    assert_eq!(found.len(), 3, "{found:?}");
    let mut uuids: Vec<&str> = found.iter().map(|r| r["u"].as_str()).collect();
    uuids.sort_unstable();
    uuids.dedup();
    assert_eq!(uuids.len(), 3, "{found:?}");
}

/// An implicit group over no solutions is still one group (§18.2.4.1): the
/// constant and `COUNT(*) = 0`. The empty per-group list used to drop the row
/// from SPARQL JSON.
#[tokio::test]
async fn implicit_group_over_empty_input_keeps_its_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/empty:main").await;
    assert_rows(
        &fluree,
        &ledger,
        r#"SELECT ("x" AS ?c) (COUNT(*) AS ?n) WHERE { ?e ex:nope ?a }"#,
        json!([{"c": "x", "n": "0"}]),
        json!([["x", 0]]),
    )
    .await;
}

/// `SELECT *` with an aggregate only in HAVING groups implicitly and projects
/// nothing (§18.2.4.4): one empty row, not one row per solution (36 today).
/// As a sub-SELECT it exports nothing, so the enclosing join sees one empty
/// solution instead of per-group lists (a join panic before).
#[tokio::test]
async fn select_star_under_implicit_grouping_projects_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/star:main").await;
    let star = format!("SELECT * {W} HAVING (STRLEN(GROUP_CONCAT(?a)) > 1)");

    let result = run(&fluree, &ledger, &star).await;
    let sj = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    assert_eq!(sj["head"]["vars"], json!([]), "{sj}");
    assert_eq!(sj["results"]["bindings"], json!([{}]), "{sj}");

    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT ?a ?x WHERE {{ {{ {star} }} ?x ex:area ?a }}"),
    )
    .await;
    assert_eq!(sparql_rows(&result, &ledger).len(), 6);
}

/// Every SPARQL-results writer sees one row per group.
#[tokio::test]
async fn grouped_select_expression_every_results_format() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/formats:main").await;
    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
    )
    .await;

    let xml = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::sparql_xml(),
    )
    .expect("sparql xml");
    assert_eq!(xml.matches("<result>").count(), 3, "{xml}");

    let csv = result.to_csv(&ledger.snapshot).expect("csv");
    assert_eq!(csv.lines().count(), 4, "header + 3 rows:\n{csv}");
    assert!(!csv.contains(';'), "no list cells:\n{csv}");
    let tsv = result.to_tsv(&ledger.snapshot).expect("tsv");
    assert_eq!(tsv.lines().count(), 4, "header + 3 rows:\n{tsv}");

    let typed = result.to_typed_json(&ledger.snapshot).expect("typed json");
    let typed_rows = typed.as_array().expect("typed rows");
    assert_eq!(typed_rows.len(), 3, "{typed}");
    assert!(
        typed_rows.iter().all(|r| !r["?seg"].is_array()),
        "scalar cells: {typed}"
    );
}

/// §18.2.4.1: in a grouped level, HAVING and ORDER BY read a non-key variable
/// as `SAMPLE(?v)`. The HAVING below is true for any sample, so every group
/// survives. With COUNT it returned no rows (the streaming lane read `?e` as
/// unbound); with GROUP_CONCAT (the traditional lane) it panicked a debug build.
#[tokio::test]
async fn having_on_a_non_key_variable_reads_a_sample() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/having-sample:main").await;
    for aggregate in ["(COUNT(?e) AS ?n)", "(GROUP_CONCAT(STR(?e)) AS ?n)"] {
        let result = run(
            &fluree,
            &ledger,
            &format!(
                r#"SELECT ?a {aggregate} {W} GROUP BY ?a
                   HAVING (STRSTARTS(STR(?e), "http://example.org/e"))"#
            ),
        )
        .await;
        let keys: Vec<String> = sorted(sparql_rows(&result, &ledger))
            .into_iter()
            .map(|r| r["a"].clone())
            .collect();
        assert_eq!(keys, vec!["Local", "Net", "Remote"], "{aggregate}");
    }
}

/// ORDER BY a non-key variable sorts by a sample of it. The groups own disjoint
/// IRI ranges (e1–e3, e4–e5, e6), so every sample orders them alike. Both used
/// to be rejected ("Sort variable … not found", "ORDER BY expression references
/// variable …").
#[tokio::test]
async fn order_by_a_non_key_variable_reads_a_sample() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/order-sample:main").await;
    for (order_by, expected) in [
        ("?e", ["Net", "Local", "Remote"]),
        ("DESC(STR(?e))", ["Remote", "Local", "Net"]),
    ] {
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a ORDER BY {order_by}"),
        )
        .await;
        let keys: Vec<String> = sparql_rows(&result, &ledger)
            .into_iter()
            .map(|r| r["a"].clone())
            .collect();
        assert_eq!(keys, expected, "ORDER BY {order_by}");
    }
}

/// An aggregate in HAVING or ORDER BY groups the level (§18.2.4.1), so a
/// projected non-key variable is the same V4 error as under an explicit GROUP
/// BY. The projected expression used to expand into one row per solution, and
/// the projected variable used to become an implicit GROUP BY key.
#[tokio::test]
async fn implicit_grouping_via_having_checks_the_projection() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/v4-having:main").await;
    for body in [
        format!("SELECT {SEG} {W} HAVING (COUNT(*) > 1)"),
        format!("SELECT ?a {W} HAVING (COUNT(*) > 1)"),
    ] {
        let query = format!("{PREFIX}{body}");
        let err = support::query_sparql(&fluree, &ledger, &query)
            .await
            .expect_err("a projected non-key variable of a grouped level");
        let msg = err.to_string();
        assert!(
            msg.contains("?a is projected but is neither a GROUP BY key nor aggregated"),
            "{body}: {msg}"
        );
    }
}

/// An aggregate over an alias of the same SELECT clause reads it before the
/// SELECT's Extend binds it; per the spec `COUNT(?seg)` would be 0 for every
/// group. It used to count solutions (and expand `?seg`); it is now a named
/// error.
#[tokio::test]
async fn aggregate_over_a_same_level_select_alias_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/agg-alias:main").await;
    for body in [
        format!("SELECT ?a {SEG} (COUNT(?seg) AS ?c) {W} GROUP BY ?a"),
        format!("SELECT ?a {SEG} {W} GROUP BY ?a HAVING (COUNT(?seg) > 1)"),
    ] {
        let query = format!("{PREFIX}{body}");
        let err = support::query_sparql(&fluree, &ledger, &query)
            .await
            .expect_err("an aggregate over a same-level SELECT alias");
        let msg = err.to_string();
        assert!(
            msg.contains(
                "?seg is assigned by this SELECT clause, after aggregation, so it cannot \
                 be aggregated at the same level"
            ),
            "{body}: {msg}"
        );
    }
}

/// §18.2.4.2: HAVING on a level that does not group is a Filter over its
/// solutions (it used to be ignored), and it cannot see the SELECT expressions,
/// so `BOUND(?s)` is false for every solution.
#[tokio::test]
async fn having_without_grouping_filters() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/having-filter:main").await;
    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT ?a {W} HAVING (?a = "Net")"#),
        json!([{"a": "Net"}, {"a": "Net"}, {"a": "Net"}]),
        json!([["Net"], ["Net"], ["Net"]]),
    )
    .await;
    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT ?a (STR(?a) AS ?s) {W} HAVING (BOUND(?s))"),
    )
    .await;
    assert_eq!(result.row_count(), 0);
}
