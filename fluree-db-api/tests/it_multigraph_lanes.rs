//! Fixture A for the lanes that read several graphs of one ledger, checked
//! before any lane leans on it. In every scope of design §5's table and every
//! fixture state, the answer to each of the six shapes, with fast paths off
//! (the general plan) and on, equals the answer written by hand in
//! `support/multigraph.rs`, and the query's `index scope` stamp is the one the
//! scope must get.
//!
//! Lane tests (C, D, E, B, A) belong in this binary: they run their shape
//! through `multigraph::check` and pin their sites with
//! `multigraph::assert_routing`.

#[path = "support/span_capture.rs"]
mod span_capture;

#[path = "support/multigraph.rs"]
mod multigraph;

use multigraph::{
    assert_routing, check, Fixture, Graphs, Kind, Rows, Run, Scope, Shape, State, Surface, SCOPES,
    SHAPES,
};

/// Every shape, scope and surface over fixture A in `state`.
async fn every_scope_answers_by_hand(state: State) {
    let fx = Fixture::build(state).await;
    let mut failures = Vec::new();
    let mut runs = 0;
    for &shape in SHAPES {
        for &scope in SCOPES {
            if !scope.applies(state) {
                continue;
            }
            for surface in [Surface::Sparql, Surface::JsonLd] {
                if scope.supports(surface) {
                    check(&fx, scope, shape, surface, &mut failures).await;
                    runs += 1;
                }
            }
        }
    }
    assert!(runs > 0, "no scope ran");
    assert!(
        failures.is_empty(),
        "{} mismatches over {runs} runs:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fixture_a_indexed() {
    every_scope_answers_by_hand(State::Indexed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixture_a_with_novelty_in_g2() {
    every_scope_answers_by_hand(State::Novelty).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixture_a_read_below_its_index() {
    every_scope_answers_by_hand(State::Indexed2).await;
}

// ============================================================================
// Lane A: the grouped count over a default graph of several graphs
// ============================================================================

/// Lane A's site over several graphs or none, and the one-graph partitioned
/// GROUP BY's site.
const A_UNION: &str = "partitioned_group_by_union";
const A_ONE: &str = "partitioned_group_by";

/// Whether `run` stamped `outcome` at lane A's union site.
fn a_declined(run: &Run, outcome: &str, failures: &mut Vec<String>) {
    let decline = (A_UNION.to_string(), outcome.to_string());
    if !run.fell_back.contains(&decline) {
        failures.push(format!(
            "{}: no `{outcome}` at {A_UNION} (fell back: {:?})",
            run.label, run.fell_back
        ));
    }
}

/// Lane A's shape (`COUNT(*)` in SPARQL, `COUNT(?s)` in JSON-LD) in every
/// scope of fixture A in `state`: the answer by hand with fast paths on and
/// off, and the routing. The union lane answers a current scope of several
/// graphs, one of them an empty default graph or not, and the scope of none;
/// a scope with novelty or read below the index declines it with `Novelty`;
/// the one-graph lane answers one graph; neither answers a GRAPH scope (not
/// the shape) or two ledgers.
async fn lane_a(state: State) {
    let fx = Fixture::build(state).await;
    let mut failures = Vec::new();
    let mut runs = 0;
    for &scope in SCOPES {
        if !scope.applies(state) {
            continue;
        }
        for surface in [Surface::Sparql, Surface::JsonLd] {
            if !scope.supports(surface) {
                continue;
            }
            let run = check(&fx, scope, Shape::GroupByObject, surface, &mut failures).await;
            runs += 1;
            let current = state != State::Novelty && scope != Scope::TimeTravel;
            match scope.kind() {
                // A member the index holds nothing for (the empty default
                // graph) contributes nothing; it is no reason to decline.
                Kind::Union if current => {
                    assert_routing(&run, &[A_UNION], &[A_ONE], &mut failures);
                }
                Kind::Union => {
                    assert_routing(&run, &[], &[A_UNION, A_ONE], &mut failures);
                    a_declined(&run, "fallback:novelty", &mut failures);
                }
                Kind::Empty => assert_routing(&run, &[A_UNION], &[A_ONE], &mut failures),
                // A GRAPH scope groups a pattern inside GRAPH: not the shape.
                Kind::One if matches!(scope.graphs(), Graphs::G0 | Graphs::G1) => {
                    assert_routing(&run, &[A_ONE], &[A_UNION], &mut failures);
                }
                Kind::One | Kind::None => {
                    assert_routing(&run, &[], &[A_UNION, A_ONE], &mut failures);
                }
            }
        }
    }
    assert!(runs > 0, "no scope ran");
    assert!(
        failures.is_empty(),
        "{} failures over {runs} runs:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn lane_a_indexed() {
    lane_a(State::Indexed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_a_with_novelty_in_g2() {
    lane_a(State::Novelty).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_a_read_below_its_index() {
    lane_a(State::Indexed2).await;
}

/// The SPARQL rows of `result`, each value with its datatype's local name
/// when it has one (`2^^integer`), so terms of one value and two datatypes
/// stay apart.
fn typed_rows(result: &serde_json::Value, vars: &[&str]) -> Rows {
    let bindings = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("SPARQL results: {result}"));
    let mut out: Rows = bindings
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| match b.get(*v) {
                    Some(term) => {
                        let value = multigraph::short(term["value"].as_str().unwrap_or_default());
                        match term["datatype"].as_str() {
                            Some(dt) => {
                                let local = dt.rsplit(['#', '/']).next().unwrap_or(dt);
                                format!("{value}^^{local}")
                            }
                            None => value,
                        }
                    }
                    None => "-".to_string(),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// One of lane A's own queries over fixture A: its SPARQL (`{ds}` for the
/// dataset clause) and, where it has one, its JSON-LD twin; the columns; the
/// rows by hand; and whether the union lane answers it, or which outcome it
/// declines with.
struct GroupCase {
    label: &'static str,
    sparql: &'static str,
    jsonld: Option<serde_json::Value>,
    columns: &'static [&'static str],
    /// Compare values with their datatypes (SPARQL only).
    typed: bool,
    want: Rows,
    fires: bool,
    decline: Option<&'static str>,
    /// JSON-LD's answer where it differs from SPARQL's: over a list, SPARQL
    /// sees the RDF list view (the owner edge to the list's first cell,
    /// `crate::list_view`), which the planner answers outside the scan, and
    /// JSON-LD sees the members as values.
    jsonld_view: Option<JsonLdView>,
}

/// A [`GroupCase`]'s JSON-LD rows, whether the union lane answers them, and
/// which outcome it declines with.
struct JsonLdView {
    want: Rows,
    fires: bool,
    decline: Option<&'static str>,
}

/// `case` over `graphs` (a `FROM` clause each; `L` names the ledger's own
/// default graph) on both surfaces, checked by hand, against the general
/// plan, and for routing.
async fn group_case(fx: &Fixture, graphs: &[&str], case: &GroupCase, failures: &mut Vec<String>) {
    use multigraph::{jsonld_rows, run_query, sparql_rows, EX, LEDGER};
    let from: String = graphs
        .iter()
        .map(|g| match *g {
            "L" => format!("FROM <{LEDGER}> "),
            g => format!("FROM <{g}> "),
        })
        .collect();
    let sparql = format!("PREFIX ex: <{EX}>\n{}", case.sparql.replace("{ds}", &from));
    let mut surfaces: Vec<(&str, multigraph::Run)> = Vec::new();
    let view = fx.view(LEDGER).await;
    let label = format!("A: {} / {graphs:?} / SPARQL", case.label);
    let run = run_query(label, || async {
        let json = view
            .query(&fx.fluree)
            .sparql(&sparql)
            .execute_formatted()
            .await
            .map_err(|e| format!("{sparql}: {e}"))?;
        Ok(if case.typed {
            typed_rows(&json, case.columns)
        } else {
            sparql_rows(&json, case.columns)
        })
    })
    .await;
    surfaces.push(("SPARQL", run));
    if let Some(query) = &case.jsonld {
        let mut query = query.clone();
        let from: Vec<String> = graphs
            .iter()
            .map(|g| match *g {
                "L" => LEDGER.to_string(),
                g => format!("{LEDGER}#{g}"),
            })
            .collect();
        query["from"] = serde_json::json!(from);
        let label = format!("A: {} / {graphs:?} / JSON-LD", case.label);
        let run = run_query(label, || async {
            let result = fx
                .fluree
                .query_connection(&query)
                .await
                .map_err(|e| format!("{query}: {e}"))?;
            let ledger = fx.fluree.ledger(LEDGER).await.map_err(|e| e.to_string())?;
            let json = result
                .to_jsonld_async(ledger.as_graph_db_ref(0))
                .await
                .map_err(|e| e.to_string())?;
            Ok(jsonld_rows(&json))
        })
        .await;
        surfaces.push(("JSON-LD", run));
    }
    for (surface, run) in surfaces {
        let (want, fires, decline) = match (&case.jsonld_view, surface) {
            (Some(view), "JSON-LD") => (&view.want, view.fires, view.decline),
            _ => (&case.want, case.fires, case.decline),
        };
        for (lane, got) in [
            ("fast paths on", &run.fast),
            ("fast paths off", &run.generic),
        ] {
            match got {
                Ok(rows) if rows == want => {}
                Ok(rows) => {
                    failures.push(format!("{}: {lane}: {rows:?}, want {want:?}", run.label));
                }
                Err(e) => failures.push(format!("{}: {lane}: failed: {e}", run.label)),
            }
        }
        if fires {
            assert_routing(&run, &[A_UNION], &[A_ONE], failures);
        } else {
            assert_routing(&run, &[], &[A_UNION, A_ONE], failures);
        }
        match decline {
            Some(outcome) => a_declined(&run, outcome, failures),
            // The list view's decline is named only for the lane's shape.
            None if run
                .fell_back
                .contains(&(A_UNION.to_string(), "fallback:list_view".to_string())) =>
            {
                failures.push(format!("{}: `fallback:list_view` off the shape", run.label));
            }
            None => {}
        }
    }
}

/// Lane A beyond the shape, over fixture A's `FROM` scopes: grouping by the
/// subject, two counts, modifiers above the count, a list's repeated value,
/// one value under three datatypes, a predicate one member holds; and what it
/// leaves to the general plan: a predicate no member holds, a count of the
/// grouped key itself, another aggregate beside the count, a FILTER, and big
/// numbers (an arena handle per graph and predicate).
///
/// A list reads two ways. SPARQL sees the RDF list view: `?s ex:items ?v` is
/// the owner edge to the list's first cell (`_:l0-0`), one row, planned as a
/// list-view triple, so the union lane stands down with `ListView` where the
/// query is its shape. JSON-LD sees the members as values, two facts by
/// their list positions, and the lane answers them.
#[tokio::test(flavor = "current_thread")]
async fn lane_a_group_counts_over_from_graphs() {
    use multigraph::{rows, G1, G2};
    use serde_json::json;
    let fx = Fixture::build(State::Indexed).await;
    let ctx = || json!({"ex": multigraph::EX});
    let g1g2: &[&str] = &[G1, G2];
    let g0g1g2: &[&str] = &["L", G1, G2];
    let cases: Vec<(&[&str], GroupCase)> = vec![
        (
            g1g2,
            GroupCase {
                label: "COUNT(*) by subject",
                sparql: "SELECT ?s (COUNT(*) AS ?k) {ds} WHERE { ?s ex:n ?v } GROUP BY ?s",
                jsonld: Some(json!({
                    "@context": ctx(),
                    "select": ["?s", "(as (count ?v) ?k)"],
                    "where": [{"@id": "?s", "ex:n": "?v"}],
                    "groupBy": ["?s"]
                })),
                columns: &["s", "k"],
                typed: false,
                want: rows(&[&["s1", "2"], &["s2", "1"], &["s3", "1"]]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g0g1g2,
            GroupCase {
                label: "COUNT(*) by subject, three graphs",
                sparql: "SELECT ?s (COUNT(*) AS ?k) {ds} WHERE { ?s ex:n ?v } GROUP BY ?s",
                jsonld: None,
                columns: &["s", "k"],
                typed: false,
                want: rows(&[
                    &["a1", "1"],
                    &["a2", "1"],
                    &["s1", "2"],
                    &["s2", "1"],
                    &["s3", "1"],
                ]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "COUNT(*) and COUNT(?s) by object",
                sparql: "SELECT ?v (COUNT(*) AS ?k) (COUNT(?s) AS ?j) {ds} \
                         WHERE { ?s ex:n ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k", "j"],
                typed: false,
                want: rows(&[&["1", "1", "1"], &["2", "2", "2"], &["3", "1", "1"]]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            // The plan copies a grouped key that an aggregate reads into a
            // column of its own (a BIND), which the lane does not read through.
            GroupCase {
                label: "COUNT of the grouped key",
                sparql: "SELECT ?v (COUNT(?v) AS ?k) {ds} WHERE { ?s ex:n ?v } GROUP BY ?v",
                jsonld: Some(json!({
                    "@context": ctx(),
                    "select": ["?v", "(as (count ?v) ?k)"],
                    "where": [{"@id": "?s", "ex:n": "?v"}],
                    "groupBy": ["?v"]
                })),
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["1", "1"], &["2", "2"], &["3", "1"]]),
                fires: false,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "HAVING, ORDER BY and LIMIT above the count",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:n ?v } GROUP BY ?v \
                         HAVING (COUNT(*) < 3) ORDER BY DESC(?v) LIMIT 2",
                jsonld: None,
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["2", "2"], &["3", "1"]]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "a list's repeated value, by object",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:items ?v } GROUP BY ?v",
                jsonld: Some(json!({
                    "@context": ctx(),
                    "select": ["?v", "(as (count ?s) ?k)"],
                    "where": [{"@id": "?s", "ex:items": "?v"}],
                    "groupBy": ["?v"]
                })),
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["l0-0", "1"]]),
                fires: false,
                decline: Some("fallback:list_view"),
                jsonld_view: Some(JsonLdView {
                    want: rows(&[&["v", "2"]]),
                    fires: true,
                    decline: None,
                }),
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "a list's repeated value, by subject",
                sparql: "SELECT ?s (COUNT(*) AS ?k) {ds} WHERE { ?s ex:items ?v } GROUP BY ?s",
                jsonld: Some(json!({
                    "@context": ctx(),
                    "select": ["?s", "(as (count ?v) ?k)"],
                    "where": [{"@id": "?s", "ex:items": "?v"}],
                    "groupBy": ["?s"]
                })),
                columns: &["s", "k"],
                typed: false,
                want: rows(&[&["L", "1"]]),
                fires: false,
                decline: Some("fallback:list_view"),
                jsonld_view: Some(JsonLdView {
                    want: rows(&[&["L", "2"]]),
                    fires: true,
                    decline: None,
                }),
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "one value under three datatypes",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:m ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k"],
                typed: true,
                want: rows(&[
                    &["2.0E0^^double", "2^^integer"],
                    &["2^^integer", "2^^integer"],
                    &["2^^short", "1^^integer"],
                ]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "a predicate only g2 holds",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:only ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["7", "1"]]),
                fires: true,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "a predicate no graph holds",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:none ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k"],
                typed: false,
                want: rows(&[]),
                fires: false,
                decline: Some("fallback:gate_declined"),
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "COUNT(DISTINCT ?s) beside the count",
                sparql: "SELECT ?v (COUNT(*) AS ?k) (COUNT(DISTINCT ?s) AS ?d) {ds} \
                         WHERE { ?s ex:n ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k", "d"],
                typed: false,
                want: rows(&[&["1", "1", "1"], &["2", "2", "2"], &["3", "1", "1"]]),
                fires: false,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g1g2,
            // A list repeats `"v"` for one subject: two rows, one subject.
            GroupCase {
                label: "COUNT(DISTINCT ?s) of a list's repeated value",
                sparql: "SELECT ?v (COUNT(*) AS ?k) (COUNT(DISTINCT ?s) AS ?d) {ds} \
                         WHERE { ?s ex:items ?v } GROUP BY ?v",
                jsonld: Some(json!({
                    "@context": ctx(),
                    "select": ["?v", "(as (count ?s) ?k)", "(as (count-distinct ?s) ?d)"],
                    "where": [{"@id": "?s", "ex:items": "?v"}],
                    "groupBy": ["?v"]
                })),
                columns: &["v", "k", "d"],
                typed: false,
                want: rows(&[&["l0-0", "1", "1"]]),
                fires: false,
                decline: None,
                jsonld_view: Some(JsonLdView {
                    want: rows(&[&["v", "2", "1"]]),
                    fires: false,
                    decline: None,
                }),
            },
        ),
        (
            g1g2,
            GroupCase {
                label: "a FILTER on the grouped object",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:n ?v FILTER(?v > 1) } \
                         GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["2", "2"], &["3", "1"]]),
                fires: false,
                decline: None,
                jsonld_view: None,
            },
        ),
        (
            g0g1g2,
            GroupCase {
                label: "big numbers, by object",
                sparql: "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:big ?v } GROUP BY ?v",
                jsonld: None,
                columns: &["v", "k"],
                typed: false,
                want: rows(&[&["1.5", "1"], &["2.5", "1"]]),
                fires: false,
                decline: Some("fallback:arena_type"),
                jsonld_view: None,
            },
        ),
        (
            g0g1g2,
            GroupCase {
                label: "big numbers, by subject",
                sparql: "SELECT ?s (COUNT(*) AS ?k) {ds} WHERE { ?s ex:big ?v } GROUP BY ?s",
                jsonld: None,
                columns: &["s", "k"],
                typed: false,
                want: rows(&[&["b", "2"]]),
                fires: false,
                decline: Some("fallback:arena_type"),
                jsonld_view: None,
            },
        ),
    ];
    let mut failures = Vec::new();
    for (graphs, case) in &cases {
        group_case(&fx, graphs, case, &mut failures).await;
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Under the kill switch the union lane stands down with `KillSwitch`, and
/// the general plan answers.
#[tokio::test(flavor = "current_thread")]
async fn lane_a_answers_to_the_kill_switch() {
    use fluree_db_api::set_fast_paths_disabled;
    let fx = Fixture::build(State::Indexed).await;
    let mut failures = Vec::new();
    for (scope, surface) in [
        (Scope::FromG1G2, Surface::Sparql),
        (Scope::UnionPragma, Surface::Sparql),
        (Scope::FromG1G2, Surface::JsonLd),
    ] {
        let _switch = multigraph::switch().lock().await;
        set_fast_paths_disabled(true);
        let (store, guard) = span_capture::init_test_tracing();
        let got = multigraph::answer(&fx, scope, Shape::GroupByObject, surface).await;
        drop(guard);
        set_fast_paths_disabled(false);
        let label = format!("{} / {surface:?}", scope.name());
        let want = multigraph::expected(
            Shape::GroupByObject,
            scope.graphs(),
            State::Indexed,
            surface,
        );
        match got {
            Ok(rows) if rows == want => {}
            Ok(rows) => failures.push(format!("{label}: {rows:?}, want {want:?}")),
            Err(e) => failures.push(format!("{label}: failed: {e}")),
        }
        let outcomes = multigraph::outcomes(&store);
        if !outcomes.contains(&(A_UNION.to_string(), "fallback:kill_switch".to_string())) {
            failures.push(format!("{label}: no kill-switch decline: {outcomes:?}"));
        }
        if outcomes.iter().any(|(s, o)| s == A_UNION && o == "proceed") {
            failures.push(format!("{label}: {A_UNION} proceeded under the switch"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Cypher over fixture A's ledger whose default graph is the union of its
/// graphs (`f:unionDefaultGraph`): a count per related node, grouped by the
/// object and by the subject, reaches the union lane and answers by hand;
/// over the plain ledger's default graph alone the one-graph lane answers.
#[tokio::test(flavor = "current_thread")]
async fn lane_a_cypher_over_the_union_default_graph() {
    use multigraph::{jsonld_rows, rows, EMPTY_G0_UNION_LEDGER, LEDGER, UNION_LEDGER};
    let fx = Fixture::build(State::Indexed).await;
    let kind = "`http://example.org/kind`";
    let knows = "`http://example.org/knows`";
    let cases: Vec<(&str, String, Rows)> = vec![
        (
            UNION_LEDGER,
            format!("MATCH (s)-[:{kind}]->(k) RETURN k, count(s) AS c"),
            rows(&[&["T1", "4"]]),
        ),
        (
            UNION_LEDGER,
            format!("MATCH (s)-[:{knows}]->(o) RETURN s, count(o) AS c"),
            rows(&[
                &["t1a", "1"],
                &["t1b", "1"],
                &["t1c", "1"],
                &["t2a", "1"],
                &["t2b", "1"],
                &["t2c", "1"],
                &["t3a", "1"],
                &["t3b", "1"],
                &["t3c", "1"],
            ]),
        ),
        // A union whose default graph holds nothing: g1 and g2.
        (
            EMPTY_G0_UNION_LEDGER,
            format!("MATCH (s)-[:{kind}]->(k) RETURN k, count(s) AS c"),
            rows(&[&["T1", "3"]]),
        ),
        (
            LEDGER,
            format!("MATCH (s)-[:{kind}]->(k) RETURN k, count(s) AS c"),
            rows(&[&["T1", "1"]]),
        ),
    ];
    let mut failures = Vec::new();
    for (ledger, cypher, want) in cases {
        let db = fx.view(ledger).await;
        let label = format!("A: Cypher on {ledger}: {cypher}");
        let run = multigraph::run_query(label, || async {
            let result = fx
                .fluree
                .query_cypher(&db, &cypher)
                .await
                .map_err(|e| format!("{cypher}: {e}"))?;
            let json = result
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .map_err(|e| e.to_string())?;
            Ok(jsonld_rows(&json))
        })
        .await;
        for (lane, got) in [
            ("fast paths on", &run.fast),
            ("fast paths off", &run.generic),
        ] {
            match got {
                Ok(rows) if *rows == want => {}
                Ok(rows) => {
                    failures.push(format!("{}: {lane}: {rows:?}, want {want:?}", run.label));
                }
                Err(e) => failures.push(format!("{}: {lane}: failed: {e}", run.label)),
            }
        }
        if ledger != LEDGER {
            assert_routing(&run, &[A_UNION], &[A_ONE], &mut failures);
        } else {
            assert_routing(&run, &[A_ONE], &[A_UNION], &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
