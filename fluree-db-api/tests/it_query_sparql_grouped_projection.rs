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

/// HAVING reads the SELECT clause's aliases: an aggregate's alias, and an
/// expression's alias, whose expression runs before HAVING, once per group
/// (`Grouping::binds_before_having`), through a chain of aliases too. SPARQL 1.1
/// evaluates SELECT expressions after HAVING (§18.2.4), so this is a Fluree
/// extension; the W3C aggregate tests always repeat the aggregate in HAVING
/// instead. A variable nothing binds is still unbound: no group qualifies.
#[tokio::test]
async fn having_reads_a_select_alias() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/having-alias:main").await;
    assert_rows(
        &fluree,
        &ledger,
        &format!(r#"SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (?seg = "network")"#),
        json!([{"seg": "network", "n": "3"}]),
        json!([["network", 3]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) + 0 AS ?n) {W} GROUP BY ?a HAVING (?n > 1)"),
        json!([{"a": "Local", "n": "2"}, {"a": "Net", "n": "3"}]),
        json!([["Local", 2], ["Net", 3]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) AS ?n) (?n * 10 AS ?t) {W} GROUP BY ?a HAVING (?t > 15)"),
        json!([{"a": "Local", "n": "2", "t": "20"}, {"a": "Net", "n": "3", "t": "30"}]),
        json!([["Local", 2, 20], ["Net", 3, 30]]),
    )
    .await;
    let result = run(
        &fluree,
        &ledger,
        &format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (?nosuch = 1)"),
    )
    .await;
    assert_eq!(result.row_count(), 0, "HAVING (?nosuch = 1)");
}

/// A sub-SELECT's grouped SELECT expression is one value per group, so an
/// outer ORDER BY on its alias sorts the groups. Evaluated per solution, it
/// was a per-group list: the outer sort compared two lists (a `debug_assert!`
/// in the sort comparator; equal, so no order, in release builds).
#[tokio::test]
async fn subselect_grouped_expression_sorts_in_the_outer_query() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/subselect-order-by:main").await;
    for (body, expected) in [
        (
            "SELECT ?a ?len WHERE { { SELECT ?a (STRLEN(?a) AS ?len) \
             WHERE { ?e ex:area ?a } GROUP BY ?a } } ORDER BY DESC(?len)",
            json!([["Remote", 6], ["Local", 5], ["Net", 3]]),
        ),
        (
            "SELECT ?e ?k WHERE { { SELECT ?e (CONCAT(?a, STR(?e)) AS ?k) \
             WHERE { ?e ex:area ?a } GROUP BY ?e ?a } } ORDER BY ?k LIMIT 3",
            json!([
                ["ex:e4", "Localhttp://example.org/e4"],
                ["ex:e5", "Localhttp://example.org/e5"],
                ["ex:e1", "Nethttp://example.org/e1"]
            ]),
        ),
        (
            "SELECT ?a ?d WHERE { { SELECT ?a (COUNT(?e) * 2 AS ?d) \
             WHERE { ?e ex:area ?a } GROUP BY ?a } } ORDER BY ?d",
            json!([["Remote", 2], ["Local", 4], ["Net", 6]]),
        ),
    ] {
        let query = format!("{PREFIX}{body}");
        let rows = support::query_sparql(&fluree, &ledger, &query)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{query}"))
            .to_jsonld(&ledger.snapshot)
            .expect("to_jsonld");
        assert_eq!(rows, expected, "{query}");
    }
}

/// An aggregate over a variable nothing binds is a plan error naming the
/// variable, a 400 on both query paths; it printed `Aggregate input variable
/// VarId(n) not found in schema`, a 500 on the tracked path.
#[tokio::test]
async fn aggregate_over_a_variable_nothing_binds_is_a_named_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/aggregate-nosuch:main").await;
    for body in [
        format!("SELECT (SUM(?nosuch) AS ?s) {W}"),
        format!("SELECT ?a {W} GROUP BY ?a HAVING (SUM(?nosuch) > 0)"),
    ] {
        let query = format!("{PREFIX}{body}");
        let Err(err) = support::query_sparql(&fluree, &ledger, &query).await else {
            panic!("an aggregate over a variable nothing binds must fail: {body}");
        };
        let message = err.to_string();
        assert!(
            message.contains("an aggregate reads variable ?nosuch, which is unbound"),
            "{body}: {message}"
        );
        assert!(!message.contains("VarId("), "{body}: {message}");
        assert_eq!(err.status_code(), 400, "{body}: {message}");

        let Err(tracked) = support::graphdb_from_ledger(&ledger)
            .query(&fluree)
            .sparql(&query)
            .execute_tracked()
            .await
        else {
            panic!("the tracked query must fail too: {body}");
        };
        assert_eq!(tracked.status, 400, "{body}: {}", tracked.error);
        assert!(
            tracked
                .error
                .contains("an aggregate reads variable ?nosuch, which is unbound"),
            "{body}: {}",
            tracked.error
        );
    }
}

/// Cypher's aggregate of a sibling aggregate's output (`count(f) AS c, sum(c)`)
/// is the same plan error, named; it printed `VarId(n)`.
#[tokio::test]
async fn cypher_aggregate_of_a_sibling_aggregate_is_a_named_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "grouped-projection/cypher-sibling-aggregate:main").await;
    let db = cypher_db(&ledger);
    for q in [
        "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c, sum(c) AS s RETURN p.name, s",
        "MATCH (p:P)-[:knows]->(f) RETURN p.name, count(f) AS c, sum(c) AS s",
    ] {
        let Err(err) = fluree.query_cypher(&db, q).await else {
            panic!("an aggregate of a sibling aggregate must fail: {q}");
        };
        let message = err.to_string();
        assert!(
            message.contains("an aggregate reads variable c, which is unbound"),
            "{q}: {message}"
        );
        assert!(!message.contains("VarId("), "{q}: {message}");
        assert_eq!(err.status_code(), 400, "{q}: {message}");
    }
}

/// The value HAVING tests is the value the alias returns: the expression runs
/// once per group, before HAVING, never again. Over 40 groups a
/// non-deterministic alias splits them, and every returned value passes.
#[tokio::test]
async fn having_tests_the_value_an_alias_returns() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "grouped-projection/having-alias-once:main");
    let graph: Vec<JsonValue> = (0..40)
        .map(|i| json!({"@id": format!("ex:e{i}"), "ex:area": format!("A{i}")}))
        .collect();
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
        )
        .await
        .expect("seed")
        .ledger;
    for (alias, having, passes) in [
        (
            "(RAND() AS ?x)",
            "(?x < 0.5)",
            (|x: &str| x.parse::<f64>().expect("double") < 0.5) as fn(&str) -> bool,
        ),
        ("(STRUUID() AS ?x)", r#"(?x < "8")"#, |x: &str| x < "8"),
    ] {
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT ?a {alias} (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING {having}"),
        )
        .await;
        let found = sparql_rows(&result, &ledger);
        assert!(
            !found.is_empty() && found.len() < 40,
            "{alias}: {} of 40 groups kept",
            found.len()
        );
        for row in &found {
            assert!(
                passes(&row["x"]),
                "{alias}: returned {} fails {having}",
                row["x"]
            );
        }
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

/// A sort key nothing binds is unbound in every solution, so it orders nothing
/// (an unbound key sorts the same in every row): the solutions come back,
/// ordered by the keys that are bound, at the top level, under grouping and in
/// a sub-SELECT. It was a 500 naming an internal id ("Sort variable VarId(n)
/// not found in query schema").
#[tokio::test]
async fn order_by_a_variable_nothing_binds_orders_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/order-nosuch:main").await;
    let result = run(&fluree, &ledger, &format!("SELECT ?e {W} ORDER BY ?nosuch")).await;
    assert_eq!(result.row_count(), 6, "ungrouped ORDER BY ?nosuch");

    let order = |body: String| {
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            let result = run(fluree, ledger, &body).await;
            sparql_rows(&result, ledger)
                .into_iter()
                .map(|r| r["a"].clone())
                .collect::<Vec<String>>()
        }
    };
    assert_eq!(
        order(format!(
            "SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a ORDER BY ?nosuch DESC(?n)"
        ))
        .await,
        ["Net", "Local", "Remote"],
        "grouped ORDER BY ?nosuch DESC(?n)"
    );
    assert_eq!(
        order(format!(
            "SELECT ?a WHERE {{ {{ SELECT DISTINCT ?a {W} ORDER BY ?nosuch DESC(?a) LIMIT 2 }} }}"
        ))
        .await
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>(),
        ["Net".to_string(), "Remote".to_string()].into(),
        "sub-SELECT ORDER BY ?nosuch DESC(?a) LIMIT 2"
    );
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

/// The SPARQL twin of `jsonld_key_only_alias_read_per_solution_stays_a_list`
/// stays an error: a SELECT expression of a grouped level reading a non-key
/// variable is V4 (SPARQL has no per-group lists), whatever earlier alias it
/// also reads.
#[tokio::test]
async fn select_expression_reading_a_non_key_variable_is_v4() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/v4-moved-alias:main").await;
    let query = format!(
        "{PREFIX}SELECT ?a (STRLEN(?a) AS ?len) ((?len + STRLEN(STR(?e))) AS ?x) \
         {W} GROUP BY ?a"
    );
    let err = support::query_sparql(&fluree, &ledger, &query)
        .await
        .expect_err("a SELECT expression reading a non-key variable");
    let msg = err.to_string();
    assert!(
        msg.contains("?e is projected but is neither a GROUP BY key nor aggregated"),
        "{msg}"
    );
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

/// A trailing VALUES clause joins right after the WHERE, before grouping, as
/// it did before this change (AJ-27): it restricts the aggregates' input, so
/// the "VALUES as a parameter" idiom keeps its answers. SPARQL 1.1 §18.2.4.3
/// joins it after HAVING instead; that is a deliberate, documented deviation.
///
/// HAVING reads a VALUES variable the WHERE does not bind as unbound, as it
/// would after HAVING: `HAVING (?v = 1) VALUES ?v { 1 }` keeps nothing,
/// grouped or not, as before this change. The implicit SAMPLE must not reach
/// it (that kept every group).
#[tokio::test]
async fn trailing_values_join_before_grouping() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/trailing-values:main").await;
    for body in [
        format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (?v = 1) VALUES ?v {{ 1 }}"),
        format!("SELECT ?a {W} HAVING (?v = 1) VALUES ?v {{ 1 }}"),
    ] {
        let result = run(&fluree, &ledger, &body).await;
        assert_eq!(result.row_count(), 0, "{body}");
    }
    // VALUES as a parameter: it restricts what the aggregates count.
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a VALUES ?e {{ ex:e1 }}"),
        json!([{"a": "Net", "n": "1"}]),
        json!([["Net", 1]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT (COUNT(?e) AS ?n) {W} VALUES ?e {{ ex:e1 ex:e4 }}"),
        json!([{"n": "2"}]),
        json!([[2]]),
    )
    .await;
    // A VALUES variable the WHERE does not bind multiplies the aggregates'
    // input (the spec would pair each group row with each VALUES row).
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a VALUES ?x {{ 1 2 }}"),
        json!([
            {"a": "Net", "n": "6"},
            {"a": "Local", "n": "4"},
            {"a": "Remote", "n": "2"}
        ]),
        json!([["Net", 6], ["Local", 4], ["Remote", 2]]),
    )
    .await;
    // Bound before grouping, a VALUES variable is one ORDER BY can read, as a
    // SAMPLE like any other non-key variable (HAVING alone reads it unbound).
    let body = format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a ORDER BY ?v VALUES ?v {{ 1 }}");
    assert_eq!(run(&fluree, &ledger, &body).await.row_count(), 3, "{body}");
}

/// e1 and e3 have `ex:n 1`, e2 has `ex:n 20`.
async fn seed_numbers(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:e1", "ex:n": 1},
            {"@id": "ex:e2", "ex:n": 20},
            {"@id": "ex:e3", "ex:n": 1}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("seed").ledger
}

/// The binds a level generates before grouping (an aggregate's input
/// expression, a GROUP BY expression, a SELECT expression of a level that does
/// not group) run after the trailing VALUES join, so they read its variables,
/// at the top level as in a sub-SELECT (whose VALUES is spliced right after
/// its WHERE). At the top level they used to run before the join and read the
/// VALUES variables as unbound: `SUM(?n * ?v)` was 0, the SELECT expression
/// unbound, and the GROUP BY expression made one unbound group.
#[tokio::test]
async fn generated_binds_read_trailing_values_at_both_levels() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_numbers(&fluree, "grouped-projection/values-binds:main").await;
    const N: &str = "WHERE { ?e ex:n ?n }";
    let both_levels = |select: &str, outer: &str, rest: &str| {
        [
            format!("SELECT {select} {N} {rest}"),
            format!("SELECT {outer} WHERE {{ {{ SELECT {select} {N} {rest} }} }}"),
        ]
    };
    for body in both_levels("(SUM(?n * ?v) AS ?s)", "?s", "VALUES ?v { 2 }") {
        assert_rows(&fluree, &ledger, &body, json!([{"s": "44"}]), json!([[44]])).await;
    }
    // Two VALUES rows: each solution pairs with each, before the aggregate.
    for body in both_levels("(SUM(?n * ?v) AS ?s)", "?s", "VALUES ?v { 1 2 }") {
        assert_rows(&fluree, &ledger, &body, json!([{"s": "66"}]), json!([[66]])).await;
    }
    for body in both_levels("?e (?n * ?v AS ?p)", "?e ?p", "VALUES ?v { 2 }") {
        assert_rows(
            &fluree,
            &ledger,
            &body,
            json!([
                {"e": "http://example.org/e1", "p": "2"},
                {"e": "http://example.org/e2", "p": "40"},
                {"e": "http://example.org/e3", "p": "2"}
            ]),
            json!([["ex:e1", 2], ["ex:e2", 40], ["ex:e3", 2]]),
        )
        .await;
    }
    for body in both_levels(
        "?k (COUNT(?e) AS ?c)",
        "?k ?c",
        "GROUP BY (?n * ?v AS ?k) VALUES ?v { 2 }",
    ) {
        assert_rows(
            &fluree,
            &ledger,
            &body,
            json!([{"k": "2", "c": "2"}, {"k": "40", "c": "1"}]),
            json!([[2, 2], [40, 1]]),
        )
        .await;
    }
}

/// The sub-SELECT twin: its trailing VALUES also joins before grouping, and its
/// HAVING also reads the VALUES variables the WHERE does not bind as unbound,
/// grouped or not. The grouped HAVING used to read them as a SAMPLE of the
/// VALUES column (every group kept), and the ungrouped one as the joined
/// value. A VALUES variable the level binds before HAVING (here the key `?a`)
/// is still read, and VALUES as a parameter still restricts the aggregates.
#[tokio::test]
async fn sub_select_having_reads_trailing_values_as_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/sub-select-values:main").await;
    for (inner, rows) in [
        (
            format!(
                "SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (?v = 1) VALUES ?v {{ 1 }}"
            ),
            0,
        ),
        (
            format!("SELECT ?a {W} HAVING (?v = 1) VALUES ?v {{ 1 }}"),
            0,
        ),
        (
            format!("SELECT ?a {W} HAVING (!BOUND(?v)) VALUES ?v {{ 1 }}"),
            6,
        ),
    ] {
        let body = format!("SELECT * WHERE {{ {{ {inner} }} }}");
        let result = run(&fluree, &ledger, &body).await;
        assert_eq!(result.row_count(), rows, "{body}");
    }
    assert_rows(
        &fluree,
        &ledger,
        &format!(
            "SELECT * WHERE {{ {{ SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a \
             HAVING (?a = \"Net\") VALUES ?a {{ \"Net\" \"Local\" }} }} }}"
        ),
        json!([{"a": "Net", "n": "3"}]),
        json!([{"?a": "Net", "?n": 3}]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!(
            "SELECT * WHERE {{ {{ SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a \
             VALUES ?e {{ ex:e1 }} }} }}"
        ),
        json!([{"a": "Net", "n": "1"}]),
        json!([{"?a": "Net", "?n": 1}]),
    )
    .await;
}

/// GROUP BY, HAVING and an aggregate ORDER BY group an ASK or CONSTRUCT level
/// too (§18.2.4.1): ASK is true when some group passes HAVING, and CONSTRUCT
/// instantiates its template once per such group. A template variable that is
/// not a GROUP BY key is unbound in the group solution, so the triples that
/// read it are skipped. These used to be refused (and before that, dropped:
/// `ASK { … } HAVING (?a = "Nope")` was true). DESCRIBE still refuses them.
#[tokio::test]
async fn ask_and_construct_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/ask-construct:main").await;
    let ask = |body: String| {
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            run(fluree, ledger, &body)
                .await
                .to_sparql_json(&ledger.snapshot)
                .expect("json")["boolean"]
                .clone()
        }
    };
    for (body, expected) in [
        (format!("ASK {W}"), true),
        (format!(r#"ASK {W} HAVING (?a = "Nope")"#), false),
        (format!("ASK {W} GROUP BY ?a HAVING (COUNT(?e) > 2)"), true),
        (format!("ASK {W} GROUP BY ?a HAVING (COUNT(?e) > 3)"), false),
        (format!("ASK {W} HAVING (COUNT(?e) > 5)"), true),
        (format!("ASK {W} HAVING (COUNT(?e) > 6)"), false),
        // One implicit group, even over no solutions.
        (format!("ASK {W} ORDER BY COUNT(?e)"), true),
        (
            "ASK { ?e ex:nosuch ?a } HAVING (COUNT(?e) = 0)".to_string(),
            true,
        ),
        // A non-key variable in an EXISTS body is free over the group row, in
        // an ASK as in a SELECT: EXISTS holds for every group, NOT EXISTS for
        // none.
        (
            format!(r#"ASK {W} GROUP BY ?a HAVING (EXISTS {{ ?e ex:area "Net" }})"#),
            true,
        ),
        (
            format!(r#"ASK {W} GROUP BY ?a HAVING (NOT EXISTS {{ ?e ex:area "Net" }})"#),
            false,
        ),
    ] {
        assert_eq!(ask(body.clone()).await, json!(expected), "{body}");
    }

    let construct = format!(
        "CONSTRUCT {{ ex:summary ex:bigArea ?a . ?e ex:inBigArea ?a }} {W} \
         GROUP BY ?a HAVING (COUNT(?e) > 1)"
    );
    let graph = run(&fluree, &ledger, &construct)
        .await
        .to_construct(&ledger.snapshot)
        .expect("to_construct")["@graph"]
        .clone();
    let nodes = graph.as_array().expect("@graph");
    assert_eq!(
        nodes.len(),
        1,
        "only the summary node, no ?e triples: {graph}"
    );
    assert_eq!(nodes[0]["@id"], "ex:summary", "{graph}");
    let mut areas: Vec<String> = nodes[0]["ex:bigArea"]
        .as_array()
        .expect("values")
        .iter()
        .map(|v| v.as_str().expect("string").to_string())
        .collect();
    areas.sort();
    assert_eq!(areas, ["Local", "Net"], "{graph}");

    let body = format!("DESCRIBE ?a {W} GROUP BY ?a");
    let err = support::query_sparql(&fluree, &ledger, &format!("{PREFIX}{body}"))
        .await
        .expect_err("DESCRIBE with GROUP BY");
    assert!(
        err.to_string().contains("DESCRIBE with GROUP BY/HAVING"),
        "{body}: {err}"
    );
}

/// An EXISTS in a grouped HAVING is evaluated per group row, with the row's
/// keys and aggregate outputs bound, by the same evaluator as FILTER. HAVING
/// used a separate evaluator that never resolved EXISTS, so `HAVING (EXISTS
/// …)` kept no group and `HAVING (NOT EXISTS …)` kept every group.
///
/// A non-key variable inside the EXISTS is free in its body: the group row does
/// not bind it (`exists_body_variables_are_free_over_the_group_row`).
#[tokio::test]
async fn having_exists_is_evaluated_per_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "grouped-projection/having-exists:main").await;
    let exists = "EXISTS { ?x ex:area ?a FILTER(?x = ex:e4) }";
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING ({exists})"),
        json!([{"a": "Local", "n": "2"}]),
        json!([["Local", 2]]),
    )
    .await;
    assert_rows(
        &fluree,
        &ledger,
        &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (NOT {exists})"),
        json!([{"a": "Net", "n": "3"}, {"a": "Remote", "n": "1"}]),
        json!([["Net", 3], ["Remote", 1]]),
    )
    .await;
    // ?e is not a key, so the group row does not bind it: it is free in the
    // body, and some entity is in Net, so every group is kept (§18.2.4.1
    // samples the expression's variables, not a pattern's).
    assert_rows(
        &fluree,
        &ledger,
        &format!(
            r#"SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (EXISTS {{ ?e ex:area "Net" }})"#
        ),
        json!([
            {"a": "Local", "n": "2"},
            {"a": "Net", "n": "3"},
            {"a": "Remote", "n": "1"}
        ]),
        json!([["Local", 2], ["Net", 3], ["Remote", 1]]),
    )
    .await;
}

/// A variable only an EXISTS body mentions is free over the group row, so the
/// answer does not depend on which solution SAMPLE would pick: with one Net
/// entity flagged, `EXISTS { ?e ex:flag true }` holds for every group, whichever
/// entity carries the flag, and `NOT EXISTS` for none. The same holds on the
/// `GroupByOperator` lane (a dedup-only GROUP BY, and GROUP_CONCAT) and for an
/// EXISTS SELECT expression, which runs once per group.
#[tokio::test]
async fn exists_body_variables_are_free_over_the_group_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    for flagged in ["ex:e1", "ex:e2"] {
        let ledger_id = format!("grouped-projection/exists-free-{}:main", &flagged[3..]);
        let ledger = seed_areas(&fluree, &ledger_id).await;
        let ledger = fluree
            .insert(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@id": flagged,
                    "ex:flag": true
                }),
            )
            .await
            .expect("flag")
            .ledger;
        let all = json!([
            {"a": "Local", "n": "2"},
            {"a": "Net", "n": "3"},
            {"a": "Remote", "n": "1"}
        ]);
        let exists = "EXISTS { ?e ex:flag true }";
        let found = |result: &QueryResult| sorted(sparql_rows(result, &ledger));
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING ({exists})"),
        )
        .await;
        assert_eq!(
            found(&result),
            sorted(rows(&all)),
            "{flagged}: HAVING EXISTS"
        );
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT ?a (COUNT(?e) AS ?n) {W} GROUP BY ?a HAVING (NOT {exists})"),
        )
        .await;
        assert_eq!(result.row_count(), 0, "{flagged}: HAVING NOT EXISTS");
        for body in [
            format!("SELECT ?a {W} GROUP BY ?a HAVING ({exists})"),
            format!("SELECT ?a (GROUP_CONCAT(STR(?e)) AS ?g) {W} GROUP BY ?a HAVING ({exists})"),
        ] {
            let result = run(&fluree, &ledger, &body).await;
            assert_eq!(result.row_count(), 3, "{flagged}: {body}");
        }
        let result = run(
            &fluree,
            &ledger,
            &format!("SELECT ?a ({exists} AS ?f) (COUNT(?e) AS ?n) {W} GROUP BY ?a"),
        )
        .await;
        assert_eq!(
            found(&result),
            sorted(rows(&json!([
                {"a": "Local", "f": "true", "n": "2"},
                {"a": "Net", "f": "true", "n": "3"},
                {"a": "Remote", "f": "true", "n": "1"}
            ]))),
            "{flagged}: EXISTS SELECT expression"
        );
    }
}

/// The area fixture with `ex:E` types, for Cypher's label match.
async fn seed_typed_areas(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:e1", "@type": "ex:E", "ex:area": "Net"},
                    {"@id": "ex:e2", "@type": "ex:E", "ex:area": "Net"},
                    {"@id": "ex:e3", "@type": "ex:E", "ex:area": "Net"},
                    {"@id": "ex:e4", "@type": "ex:E", "ex:area": "Local"},
                    {"@id": "ex:e5", "@type": "ex:E", "ex:area": "Local"},
                    {"@id": "ex:e6", "@type": "ex:E", "ex:area": "Remote"}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

/// A view that resolves Cypher's bare names (`E`, `area`) in `ex:`.
fn cypher_db(ledger: &MemoryLedger) -> fluree_db_api::GraphDb {
    support::graphdb_from_ledger(ledger)
        .with_default_context(Some(json!({"@vocab": "http://example.org/"})))
}

/// A grouped-read error does not print an internal variable. A Cypher
/// property access (`e.name`) is a synthetic variable (`?#__prop_e_name`);
/// ORDER BY reading it after an aggregating WITH that does not project `e`
/// (so `e` is out of scope there) used to be reported by that name.
#[tokio::test]
async fn cypher_grouped_read_error_names_no_internal_variable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_areas(&fluree, "grouped-projection/cypher-internal:main").await;
    let db = cypher_db(&ledger);
    let query = "MATCH (e:E) WITH e.area AS a, count(*) AS c ORDER BY e.name RETURN c";
    let Err(err) = fluree.query_cypher(&db, query).await else {
        panic!("ORDER BY a property of a node the WITH does not project must fail: {query}");
    };
    let message = err.to_string();
    assert!(
        message.contains("an ORDER BY key is neither a GROUP BY key nor an aggregate result"),
        "{message}"
    );
    for internal in ["?#", "?__", "VarId("] {
        assert!(!message.contains(internal), "{internal} in: {message}");
    }
}

/// People who know each other, typed `ex:P`, with ages. Alice also likes Carol.
async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {
                        "@id": "ex:alice", "@type": "ex:P", "ex:age": 40,
                        "ex:knows": [{"@id": "ex:bob"}, {"@id": "ex:carol"}],
                        "ex:likes": {"@id": "ex:carol"}
                    },
                    {"@id": "ex:bob", "@type": "ex:P", "ex:age": 25, "ex:knows": {"@id": "ex:carol"}},
                    {"@id": "ex:carol", "@type": "ex:P", "ex:age": 35},
                    {
                        "@id": "ex:dave", "@type": "ex:P", "ex:age": 50,
                        "ex:knows": [{"@id": "ex:alice"}, {"@id": "ex:bob"}, {"@id": "ex:carol"}]
                    }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

/// After an aggregating `WITH` or `RETURN`, a property of a node the clause
/// projects is readable in the `WITH`'s `WHERE` and in either clause's
/// `ORDER BY`: it is read after the aggregation, as a following `WITH p, c
/// WHERE p.age > 30` would read it. So is an expression sort key over an
/// aggregate (`ORDER BY c + 1`). A composite alias (`count(f) + 0 AS c`) is
/// visible to the `WITH`'s `WHERE`, and the variables of an `exists { … }`
/// there are its own, not the aggregated `f`.
#[tokio::test]
async fn cypher_reads_a_key_nodes_property_after_grouping() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "grouped-projection/cypher-key-props:main").await;
    let db = cypher_db(&ledger);
    let cypher = |query: &'static str| {
        let (fluree, db) = (&fluree, &db);
        async move {
            let rows = fluree
                .query_cypher(db, query)
                .await
                .unwrap_or_else(|e| panic!("{e}\n{query}"))
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld");
            normalize_rows(&rows)
        }
    };
    for (query, expected) in [
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c WHERE c >= 1 AND p.age > 30 \
             RETURN p.age, c",
            json!([[40, 2], [50, 3]]),
        ),
        (
            "MATCH (e:P) WITH e, count(*) AS c WHERE e.age > 30 RETURN e.age, c",
            json!([[40, 1], [35, 1], [50, 1]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c ORDER BY p.age LIMIT 2 \
             RETURN p.age, c",
            json!([[25, 1], [40, 2]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c ORDER BY p.age DESC LIMIT 1 \
             RETURN p.age, c",
            json!([[50, 3]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) + 0 AS c WHERE c > 1 RETURN p.age, c",
            json!([[40, 2], [50, 3]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c ORDER BY c + 1 DESC LIMIT 2 \
             RETURN p.age, c",
            json!([[50, 3], [40, 2]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c ORDER BY -c LIMIT 1 \
             RETURN p.age, c",
            json!([[50, 3]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) RETURN p.age AS a, count(f) AS c ORDER BY c + 1 LIMIT 1",
            json!([[25, 1]]),
        ),
        (
            "MATCH (p:P)-[:knows]->(f) WITH p, count(f) AS c \
             WHERE exists { (p)-[:likes]->(f) } RETURN p.age, c",
            json!([[40, 2]]),
        ),
    ] {
        assert_eq!(cypher(query).await, normalize_rows(&expected), "{query}");
    }

    // An aggregating RETURN orders by a key node's property.
    for (query, count) in [
        (
            "MATCH (p:P)-[:knows]->(f) RETURN p, count(f) AS c ORDER BY p.age DESC LIMIT 1",
            3,
        ),
        (
            "MATCH (p:P)-[:knows]->(f) RETURN p, count(f) AS c ORDER BY p.age LIMIT 1",
            1,
        ),
    ] {
        let cj = fluree
            .query_cypher(&db, query)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{query}"))
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        let data = cj["results"][0]["data"].as_array().expect("rows");
        assert_eq!(data.len(), 1, "{query}: {cj}");
        assert_eq!(data[0]["row"][1], json!(count), "{query}: {cj}");
    }
}

/// People with names and scores, typed `ex:P`; Alice has two ages (40 and 41).
async fn seed_people_two_ages(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {
                        "@id": "ex:alice", "@type": "ex:P", "ex:name": "Alice",
                        "ex:age": [40, 41], "ex:score": 1,
                        "ex:knows": [{"@id": "ex:bob"}, {"@id": "ex:carol"}]
                    },
                    {
                        "@id": "ex:bob", "@type": "ex:P", "ex:name": "Bob", "ex:age": 25,
                        "ex:score": 10, "ex:knows": {"@id": "ex:carol"}
                    },
                    {"@id": "ex:carol", "@type": "ex:P", "ex:name": "Carol", "ex:age": 35, "ex:score": 100},
                    {
                        "@id": "ex:dave", "@type": "ex:P", "ex:name": "Dave", "ex:age": 50,
                        "ex:score": 1000,
                        "ex:knows": [{"@id": "ex:alice"}, {"@id": "ex:bob"}, {"@id": "ex:carol"}]
                    }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

/// A property of an output node, read in an aggregating `WITH`'s `WHERE` or
/// in an aggregating `WITH`'s or `RETURN`'s `ORDER BY`, is read after the
/// aggregation, so a property with several values does not change what the
/// aggregates see. Read before it (an accessor joined in the aggregation's
/// body), Alice's two ages doubled her group: `count` 4, `sum` 220, `collect`
/// with every friend twice. Alice (40 and 41) knows Bob (score 10) and Carol
/// (100); Dave (50) knows Alice (1), Bob and Carol.
///
/// The second stage reads every value of the property, as a following `WITH`
/// does, so a filter that both of Alice's ages pass keeps her row twice, each
/// with the same aggregates, exactly as `MATCH (p:P) WHERE p.age > 30` returns
/// her twice.
#[tokio::test]
async fn cypher_output_node_properties_are_read_after_aggregation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_two_ages(&fluree, "grouped-projection/cypher-two-ages:main").await;
    let db = cypher_db(&ledger);
    let cypher = |query: &'static str| {
        let (fluree, db) = (&fluree, &db);
        async move {
            let rows = fluree
                .query_cypher(db, query)
                .await
                .unwrap_or_else(|e| panic!("{e}\n{query}"))
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld");
            // `collect` order is unspecified: sort each list cell.
            let rows: Vec<JsonValue> = rows
                .as_array()
                .expect("rows")
                .iter()
                .map(|row| {
                    let cells = row.as_array().expect("row").iter().map(|cell| match cell {
                        JsonValue::Array(list) => {
                            let mut list = list.clone();
                            list.sort_by_key(std::string::ToString::to_string);
                            JsonValue::Array(list)
                        }
                        other => other.clone(),
                    });
                    JsonValue::Array(cells.collect())
                })
                .collect();
            normalize_rows(&JsonValue::Array(rows))
        }
    };
    const M: &str = "MATCH (p:P)-[:knows]->(f) ";
    for (rest, expected) in [
        (
            "WITH p, count(f) AS c WHERE p.age > 40 RETURN p.name, c",
            json!([["Alice", 2], ["Dave", 3]]),
        ),
        (
            "WITH p, count(f) AS c, sum(f.score) AS s WHERE p.age > 40 RETURN p.name, c, s",
            json!([["Alice", 2, 110], ["Dave", 3, 111]]),
        ),
        (
            "WITH p, collect(f.name) AS fs WHERE p.age > 40 RETURN p.name, fs",
            json!([
                ["Alice", ["Bob", "Carol"]],
                ["Dave", ["Alice", "Bob", "Carol"]]
            ]),
        ),
        (
            "WITH p AS q, count(f) AS c WHERE c >= 1 AND q.age > 40 RETURN q.name, c",
            json!([["Alice", 2], ["Dave", 3]]),
        ),
        (
            "WITH p, count(f) AS c, sum(f.score) AS s, collect(f.name) AS fs \
             ORDER BY p.age DESC SKIP 1 LIMIT 1 RETURN p.name, c, s, fs",
            json!([["Alice", 2, 110, ["Bob", "Carol"]]]),
        ),
        (
            "WITH p, count(f) AS c ORDER BY p.age + 1 DESC SKIP 1 LIMIT 1 RETURN p.name, c",
            json!([["Alice", 2]]),
        ),
        (
            "RETURN p.name AS n, count(f) AS c, sum(f.score) AS s, collect(f.name) AS fs, p \
             ORDER BY p.age DESC SKIP 1 LIMIT 1",
            json!([[
                "Alice",
                2,
                110,
                ["Bob", "Carol"],
                "http://example.org/alice"
            ]]),
        ),
        (
            "WITH p, count(f) AS c WHERE p.age > 30 RETURN p.name, c",
            json!([["Alice", 2], ["Alice", 2], ["Dave", 3]]),
        ),
        (
            "WITH p, count(f) AS c WITH p, c WHERE p.age > 30 RETURN p.name, c",
            json!([["Alice", 2], ["Alice", 2], ["Dave", 3]]),
        ),
    ] {
        let query: &'static str = Box::leak(format!("{M}{rest}").into_boxed_str());
        assert_eq!(cypher(query).await, normalize_rows(&expected), "{query}");
    }

    // The second stage reads every value of the property, as `MATCH … WHERE`
    // and an explicit following `WITH` do. An unsliced sort on Alice's two
    // ages lists her row once per age, `DISTINCT` keeps both copies (the sort
    // key is projected with them), and a later aggregate counts each copy.
    // A read inside an aggregate's argument is still joined before grouping,
    // so Alice's two ages repeat her group's rows for every aggregate of the
    // clause (`count(f)` is 4). These pin today's per-value model in row
    // order: a change to it (one row per record, as openCypher has no
    // multi-valued properties) flips them on purpose.
    for (rest, expected) in [
        (
            "WITH p, count(f) AS c ORDER BY p.age RETURN p.name, c",
            json!([["Bob", 1], ["Alice", 2], ["Alice", 2], ["Dave", 3]]),
        ),
        (
            "WITH DISTINCT p, count(f) AS c ORDER BY p.age RETURN p.name, c",
            json!([["Bob", 1], ["Alice", 2], ["Alice", 2], ["Dave", 3]]),
        ),
        (
            "RETURN p.name AS n, count(f) AS c, p ORDER BY p.age DESC",
            json!([
                ["Dave", 3, "http://example.org/dave"],
                ["Alice", 2, "http://example.org/alice"],
                ["Alice", 2, "http://example.org/alice"],
                ["Bob", 1, "http://example.org/bob"]
            ]),
        ),
        (
            "WITH p, count(f) AS c WHERE p.age > 30 RETURN count(*) AS n",
            json!([[3]]),
        ),
        (
            "WITH p, count(f) AS c WHERE p.age > 30 RETURN sum(c) AS s",
            json!([[7]]),
        ),
        (
            "WITH p, count(f) AS c, collect(p.age) AS ages \
             RETURN p.name, c, size(ages) AS k ORDER BY c",
            json!([["Bob", 1, 1], ["Dave", 3, 3], ["Alice", 4, 4]]),
        ),
    ] {
        let query = format!("{M}{rest}");
        let rows = fluree
            .query_cypher(&db, &query)
            .await
            .unwrap_or_else(|e| panic!("{e}\n{query}"))
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        assert_eq!(rows, expected, "{query}");
    }

    // A node the clause does not project is out of scope after it.
    for rest in [
        "WITH p, count(f) AS c WHERE f.age > 30 RETURN p.name, c",
        "WITH p, count(f) AS c ORDER BY f.age RETURN p.name, c",
    ] {
        let query = format!("{M}{rest}");
        assert!(fluree.query_cypher(&db, &query).await.is_err(), "{query}");
    }
}

/// Cypher parity. Cypher makes a non-aggregate RETURN expression a grouping
/// key, so `RETURN CASE … AS seg, count(e)` groups by the label — SPARQL's
/// `GROUP BY (IF(…) AS ?seg)`. Grouping by the area first and mapping it after
/// (`WITH e.area AS a, count(e) AS n RETURN CASE … AS seg, n`) is what a
/// SPARQL SELECT expression over `GROUP BY ?a` means. The mapping is
/// non-injective (three areas, two labels), so the two readings differ.
#[tokio::test]
async fn grouped_select_expression_matches_cypher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_areas(&fluree, "grouped-projection/cypher:main").await;
    let db = cypher_db(&ledger);
    let cypher = |query: &'static str| {
        let (fluree, db) = (&fluree, &db);
        async move {
            let rows = fluree
                .query_cypher(db, query)
                .await
                .unwrap_or_else(|e| panic!("{e}\n{query}"))
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld");
            normalize_rows(&rows)
        }
    };
    let sparql = |body: String| {
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            let rows = run(fluree, ledger, &body)
                .await
                .to_jsonld(&ledger.snapshot)
                .expect("jsonld");
            normalize_rows(&rows)
        }
    };

    let per_area = json!([["network", 3], ["other", 2], ["other", 1]]);
    assert_eq!(
        cypher(
            "MATCH (e:E) WITH e.area AS a, count(e) AS n \
             RETURN CASE WHEN a = 'Net' THEN 'network' ELSE 'other' END AS seg, n"
        )
        .await,
        normalize_rows(&per_area)
    );
    assert_eq!(
        sparql(format!("SELECT {SEG} (COUNT(?e) AS ?n) {W} GROUP BY ?a")).await,
        normalize_rows(&per_area)
    );

    let per_label = json!([["network", 3], ["other", 3]]);
    assert_eq!(
        cypher(
            "MATCH (e:E) \
             RETURN CASE WHEN e.area = 'Net' THEN 'network' ELSE 'other' END AS seg, \
             count(e) AS n"
        )
        .await,
        normalize_rows(&per_label)
    );
    assert_eq!(
        sparql(format!(
            r#"SELECT ?seg (COUNT(?e) AS ?n) {W} GROUP BY (IF(?a = "Net", "network", "other") AS ?seg)"#
        ))
        .await,
        normalize_rows(&per_label)
    );
}

/// Must-not-change guards: grouped shapes that fluree/solo runs today, which
/// project only keys, aggregates and expressions of aggregates. Their answers
/// are unchanged by the grouped-projection work.
#[tokio::test]
async fn solo_grouped_sparql_shapes_are_unchanged() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "grouped-projection/solo-sparql:main");
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:g1", "ex:link": [{"@id": "ex:l1"}, {"@id": "ex:l2"}, {"@id": "ex:l3"}]},
                    {"@id": "ex:g2", "ex:link": [{"@id": "ex:l4"}, {"@id": "ex:l5"}, {"@id": "ex:l6"}]},
                    {"@id": "ex:g3", "ex:link": [{"@id": "ex:l7"}]},
                    {"@id": "ex:s1", "ex:doc": {"@id": "ex:d1"}, "ex:score": 0.9},
                    {"@id": "ex:s2", "ex:doc": {"@id": "ex:d1"}, "ex:score": 0.5},
                    {"@id": "ex:s3", "ex:doc": {"@id": "ex:d2"}, "ex:score": 0.85}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

    // Nested grouped sub-SELECT (solo `golden.ts` clusterSizes): entities per
    // link count, and the links they hold.
    assert_rows(
        &fluree,
        &ledger,
        "SELECT ?n (COUNT(?e) AS ?entities) (SUM(?n) AS ?records) WHERE {
           { SELECT ?e (COUNT(?l) AS ?n) WHERE { ?e ex:link ?l } GROUP BY ?e }
         } GROUP BY ?n",
        json!([
            {"n": "3", "entities": "2", "records": "6"},
            {"n": "1", "entities": "1", "records": "1"}
        ]),
        json!([[3, 2, 6], [1, 1, 1]]),
    )
    .await;

    // SUM(IF(…)) buckets per key (solo `golden.ts` evidenceScoreBuckets).
    assert_rows(
        &fluree,
        &ledger,
        "SELECT ?d (SUM(IF(?score >= 0.8, 1, 0)) AS ?high) (SUM(IF(?score < 0.8, 1, 0)) AS ?low)
         WHERE { ?s ex:doc ?d ; ex:score ?score } GROUP BY ?d",
        json!([
            {"d": "http://example.org/d1", "high": "1", "low": "1"},
            {"d": "http://example.org/d2", "high": "1", "low": "0"}
        ]),
        json!([["ex:d1", 1, 1], ["ex:d2", 1, 0]]),
    )
    .await;

    // Two aggregate-only sub-SELECTs under an ungrouped SELECT * (the pattern
    // solo's chat prompt teaches, `prose.rs`).
    assert_rows(
        &fluree,
        &ledger,
        "SELECT * WHERE {
           { SELECT (COUNT(?l) AS ?links) (COUNT(DISTINCT ?e) AS ?entities) WHERE { ?e ex:link ?l } }
           { SELECT (COUNT(?s) AS ?scores) WHERE { ?s ex:score ?score } }
         }",
        json!([{"links": "7", "entities": "3", "scores": "3"}]),
        json!([{"?links": 7, "?entities": 3, "?scores": 3}]),
    )
    .await;
}
