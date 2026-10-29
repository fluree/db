//! Fixture A for the lanes that read several graphs of one ledger, checked
//! before any lane leans on it. In every scope of the scope table and every
//! fixture state, the answer to each of the six shapes, with fast paths off
//! (the general plan) and on, equals the answer written by hand in
//! `support/multigraph.rs`, and the query's `index scope` stamp is the one the
//! scope must get.
//!
//! Lane tests (C, D, E, B, A) belong in this binary: they run their shape
//! through `multigraph::check` and pin their sites with
//! `multigraph::assert_routing`. Lane F (the nested-loop join's batched
//! probes over a union) runs on fixture F (`support/lane_f.rs`), whose facts
//! carry the fact-identity cases its probes must keep apart.

#[path = "support/span_capture.rs"]
mod span_capture;

#[path = "support/multigraph.rs"]
mod multigraph;

#[path = "support/lane_f.rs"]
mod lane_f;

use multigraph::{
    assert_routing, check, Fixture, Graphs, Kind, ListCase, Rows, Run, Scope, Shape, State,
    Surface, G1, G2, SCOPES, SHAPES,
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
// Union lanes with a one-graph site and a union site (C, D)
// ============================================================================

/// Which of a lane's two sites must run for a query.
#[derive(Clone, Copy, Debug, PartialEq)]
enum LaneRoute {
    /// The one-graph site answers; the union site stamps nothing.
    One,
    /// The union site answers; the one-graph site does not.
    Union,
    /// The union site declines with this outcome; neither answers.
    UnionDeclines(&'static str),
    /// Neither site answers, and the union site stamps nothing.
    Neither,
}

/// The route of a union lane in `scope` and `state` (the scope table), by
/// what the index scope makes of the scope's default graph. The union sites
/// take a default graph of several graphs of one ledger, an empty member
/// among them, and of none; novelty and a read below the index decline them.
/// Novelty is ledger-wide, so it also turns the one-graph sites off in every
/// scope.
fn union_lane_route(scope: Scope, state: State) -> LaneRoute {
    match scope.kind() {
        Kind::None => LaneRoute::Neither,
        // A default graph of none answers nothing, novelty or not.
        Kind::Empty => LaneRoute::Union,
        Kind::Union if scope == Scope::TimeTravel || state == State::Novelty => {
            LaneRoute::UnionDeclines("fallback:novelty")
        }
        Kind::Union => LaneRoute::Union,
        Kind::One if state == State::Novelty => LaneRoute::Neither,
        Kind::One => LaneRoute::One,
    }
}

/// Check one run's route: whether the one-graph site answered
/// (`one_answered`, as the lane defines it), whether the union site ran, and
/// what it declined with.
fn check_lane_route(
    run: &Run,
    route: LaneRoute,
    one_answered: bool,
    sites: (&str, &str),
    failures: &mut Vec<String>,
) {
    let (one_site, union_site) = sites;
    let union_ran = run.proceeded.iter().any(|s| s == union_site);
    let declines: Vec<String> = run
        .fell_back
        .iter()
        .filter(|(site, _)| site == union_site)
        .map(|(_, outcome)| outcome.clone())
        .collect();
    let want = match route {
        LaneRoute::One => (true, false, vec![]),
        LaneRoute::Union => (false, true, vec![]),
        LaneRoute::UnionDeclines(reason) => (false, false, vec![reason.to_string()]),
        LaneRoute::Neither => (false, false, vec![]),
    };
    let got = (one_answered, union_ran, declines);
    if got != want {
        failures.push(format!(
            "{}: (`{one_site}` answered, `{union_site}` ran, its declines) = {got:?}, want {want:?}",
            run.label
        ));
    }
}

/// Run `shape` in every scope and surface over fixture A in `state`: both
/// answers by hand, and the route `one_answered` and the sites say it took,
/// against the lane's `route`.
async fn union_lane_routes(
    state: State,
    shape: Shape,
    sites: (&str, &str),
    one_answered: fn(&Run, &str) -> bool,
    route: fn(Scope, State) -> LaneRoute,
) {
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
            let run = check(&fx, scope, shape, surface, &mut failures).await;
            let answered = one_answered(&run, sites.0);
            check_lane_route(&run, route(scope, state), answered, sites, &mut failures);
            runs += 1;
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

// ============================================================================
// Lane C: the EXISTS semijoin cache
// ============================================================================

const EXISTS_SITES: (&str, &str) = ("exists_semijoin", "exists_semijoin_union");

/// The cache stamps its site only when it builds, so having proceeded is
/// having answered.
fn exists_cache_answered(run: &Run, site: &str) -> bool {
    run.proceeded.iter().any(|s| s == site)
}

#[tokio::test(flavor = "current_thread")]
async fn lane_c_exists_cache_indexed() {
    union_lane_routes(
        State::Indexed,
        Shape::ExistsInExpression,
        EXISTS_SITES,
        exists_cache_answered,
        union_lane_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_c_exists_cache_with_novelty_in_g2() {
    union_lane_routes(
        State::Novelty,
        Shape::ExistsInExpression,
        EXISTS_SITES,
        exists_cache_answered,
        union_lane_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_c_exists_cache_read_below_its_index() {
    union_lane_routes(
        State::Indexed2,
        Shape::ExistsInExpression,
        EXISTS_SITES,
        exists_cache_answered,
        union_lane_route,
    )
    .await;
}

// ============================================================================
// Lane D: the bound-object count
// ============================================================================

const OBJECT_COUNT_SITES: (&str, &str) = ("predicate_object_count", "predicate_object_count_union");

/// The one-graph count stamps `proceed` when it is planned and a decline at
/// open when it does not answer: it answered when it proceeded and declined
/// nothing.
fn object_count_answered(run: &Run, site: &str) -> bool {
    run.proceeded.iter().any(|s| s == site) && !run.fell_back.iter().any(|(s, _)| s == site)
}

/// The count's route: [`union_lane_route`], except inside `GRAPH`, where the
/// count is no single triple pattern and neither lane is planned.
fn object_count_route(scope: Scope, state: State) -> LaneRoute {
    match scope {
        Scope::GraphG2 | Scope::GraphVar => LaneRoute::Neither,
        _ => union_lane_route(scope, state),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_object_count_indexed() {
    union_lane_routes(
        State::Indexed,
        Shape::BoundObjectCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_object_count_with_novelty_in_g2() {
    union_lane_routes(
        State::Novelty,
        Shape::BoundObjectCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_object_count_read_below_its_index() {
    union_lane_routes(
        State::Indexed2,
        Shape::BoundObjectCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_numeric_count_indexed() {
    union_lane_routes(
        State::Indexed,
        Shape::BoundNumericCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_numeric_count_with_novelty_in_g2() {
    union_lane_routes(
        State::Novelty,
        Shape::BoundNumericCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_d_numeric_count_read_below_its_index() {
    union_lane_routes(
        State::Indexed2,
        Shape::BoundNumericCount,
        OBJECT_COUNT_SITES,
        object_count_answered,
        object_count_route,
    )
    .await;
}

// ============================================================================
// Lane E: the per-predicate count over a default graph of several graphs
// ============================================================================

/// Lane E's site over several graphs or none; the one-graph lane's site as
/// it runs, and as it is planned. The planned site stamps at plan time
/// whenever the one-graph lane is planned, also where its open() then
/// declines (two ledgers, novelty), so only the run site says it answered;
/// a union scope plans neither.
const E_UNION: &str = "stats_count_by_predicate_union";
const E_ONE: &str = "COUNT by predicate (directory)";
const E_ONE_PLANNED: &str = "stats_count_by_predicate";

/// Lane E's shape in every scope of fixture A in `state`: the answer by hand
/// with fast paths on and off, and the routing. The union lane answers a
/// current scope of several graphs, also one with a default graph the index
/// holds no leaf for, and the empty one; a scope with novelty or read below
/// the index declines it with `Novelty`; the one-graph lane answers one
/// current graph; nothing answers a GRAPH scope (not the shape) or two
/// ledgers.
///
/// Fixture A holds a list, so SPARQL reads `?s ?p ?o` through its view of
/// RDF lists (#2159): the owner edge and the cells' `rdf:first`/`rdf:rest`
/// in place of the members. The union lane counts stored rows, so over
/// several graphs or none it stands down at plan time with `ListView`, in
/// every state, and the general plan answers; the one-graph lane corrects
/// its counts from the list census. JSON-LD reads the members as values,
/// and the union lane answers it.
async fn lane_e(state: State) {
    let fx = Fixture::build(state).await;
    let mut failures = Vec::new();
    let mut runs = 0;
    let all = [E_UNION, E_ONE, E_ONE_PLANNED];
    for &scope in SCOPES {
        if !scope.applies(state) {
            continue;
        }
        for surface in [Surface::Sparql, Surface::JsonLd] {
            if !scope.supports(surface) {
                continue;
            }
            let run = check(&fx, scope, Shape::PerPredicateCount, surface, &mut failures).await;
            runs += 1;
            // Novelty belongs to the ledger, so a write to g2 leaves no
            // member of any scope current.
            let current = state != State::Novelty && scope != Scope::TimeTravel;
            match scope.kind() {
                Kind::Union | Kind::Empty if surface == Surface::Sparql => {
                    assert_routing(&run, &[], &all, &mut failures);
                    let list_view = (E_UNION.to_string(), "fallback:list_view".to_string());
                    if !run.fell_back.contains(&list_view) {
                        failures.push(format!(
                            "{}: no ListView decline (fell back: {:?})",
                            run.label, run.fell_back
                        ));
                    }
                }
                Kind::Union if current => {
                    assert_routing(&run, &[E_UNION], &[E_ONE, E_ONE_PLANNED], &mut failures);
                }
                Kind::Union => {
                    assert_routing(&run, &[], &all, &mut failures);
                    let novelty = (E_UNION.to_string(), "fallback:novelty".to_string());
                    if !run.fell_back.contains(&novelty) {
                        failures.push(format!(
                            "{}: no Novelty decline (fell back: {:?})",
                            run.label, run.fell_back
                        ));
                    }
                }
                Kind::Empty => {
                    assert_routing(&run, &[E_UNION], &[E_ONE, E_ONE_PLANNED], &mut failures);
                }
                // In a GRAPH scope the pattern is not the lane's shape.
                Kind::One if current && matches!(scope.graphs(), Graphs::G0 | Graphs::G1) => {
                    assert_routing(&run, &[E_ONE], &[E_UNION], &mut failures);
                }
                Kind::One | Kind::None => {
                    assert_routing(&run, &[], &[E_UNION, E_ONE], &mut failures);
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
async fn lane_e_indexed() {
    lane_e(State::Indexed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_e_with_novelty_in_g2() {
    lane_e(State::Novelty).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lane_e_read_below_its_index() {
    lane_e(State::Indexed2).await;
}

/// Big numbers in the per-predicate count over several graphs. A big number
/// is a handle into an arena per graph and predicate, so the count keys each
/// by its value: `1.5` and `1.50` are one value; a big integer is keyed by
/// value whatever XSD subtype wrote it; a decimal and an integer of equal
/// magnitude are two values; a value in one member stays its own. The
/// ledger's default graph is empty, so the union default graph has a member
/// the index holds nothing for, which the count leaves out.
const NUMBIG_LEDGER: &str = "it/mg-lanes-numbig:main";
const NUMBIG_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
GRAPH <urn:g1> {
  ex:x ex:dec 1.5 ;
       ex:big "100000000000000000000"^^xsd:integer ;
       ex:mag "100000000000000000000"^^xsd:decimal ;
       ex:one 2.25 .
}
GRAPH <urn:g2> {
  ex:x ex:dec 1.50 ;
       ex:big "100000000000000000000"^^xsd:nonNegativeInteger ;
       ex:mag "100000000000000000000"^^xsd:integer .
}
"#;

/// The per-predicate counts of `dataset` over the big-number ledger, as
/// sorted `local=count` strings, through SPARQL or JSON-LD, with the
/// fast-path sites that proceeded.
async fn numbig_counts(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    graphs: &[&str],
    jsonld: bool,
) -> (Vec<String>, Vec<String>) {
    use serde_json::json;
    let (store, guard) = span_capture::init_test_tracing();
    // No graphs: the union default graph.
    let union_default = graphs.is_empty();
    let rows: Vec<(String, String)> = if jsonld {
        let from = if union_default {
            json!(NUMBIG_LEDGER)
        } else {
            json!(graphs
                .iter()
                .map(|g| format!("{NUMBIG_LEDGER}#{g}"))
                .collect::<Vec<_>>())
        };
        let mut q = json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?p", "(count ?s)"],
            "where": [{"@id": "?s", "?p": "?o"}],
            "groupBy": ["?p"]
        });
        if union_default {
            q["opts"] = json!({"unionDefaultGraph": true});
        }
        let result = fluree.query_connection(&q).await.expect("JSON-LD query");
        let ledger = fluree.ledger(NUMBIG_LEDGER).await.expect("ledger");
        let out = result
            .to_jsonld_async(ledger.as_graph_db_ref(0))
            .await
            .expect("format");
        out.as_array()
            .expect("rows")
            .iter()
            .map(|r| (r[0].to_string(), r[1].to_string()))
            .collect()
    } else {
        let from: String = graphs.iter().map(|g| format!("FROM <{g}> ")).collect();
        let pragma = if union_default {
            "# PRAGMA union-default-graph: true\n"
        } else {
            ""
        };
        let q =
            format!("{pragma}SELECT ?p (COUNT(?s) AS ?c) {from} WHERE {{ ?s ?p ?o }} GROUP BY ?p");
        let out = view
            .query(fluree)
            .sparql(&q)
            .execute_formatted()
            .await
            .expect("SPARQL query");
        out["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| {
                (
                    b["p"]["value"].as_str().unwrap().to_string(),
                    b["c"]["value"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    drop(guard);
    let mut rows: Vec<String> = rows
        .into_iter()
        .map(|(p, c)| {
            let p = p.trim_matches('"');
            let local = p.rsplit(['/', ':']).next().unwrap_or(p);
            format!("{local}={}", c.trim_matches('"'))
        })
        .collect();
    rows.sort();
    let proceeded = store
        .all_events()
        .iter()
        .filter(|e| e.message() == "fast-path outcome")
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect();
    (rows, proceeded)
}

#[tokio::test(flavor = "current_thread")]
async fn lane_e_keys_big_numbers_by_value() {
    use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder, GraphDb, ReindexOptions};
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    fluree
        .create_ledger(NUMBIG_LEDGER)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(NUMBIG_LEDGER).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(NUMBIG_TRIG)
        .execute()
        .await
        .expect("seed");
    fluree
        .reindex(NUMBIG_LEDGER, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(NUMBIG_LEDGER).await;
    let handle = fluree.ledger_cached(NUMBIG_LEDGER).await.expect("reload");
    let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());

    let want = |rows: &[&str]| -> Vec<String> { rows.iter().map(|r| (*r).to_string()).collect() };
    let cases: [(&[&str], Vec<String>, bool); 3] = [
        (
            &["urn:g1"],
            want(&["big=1", "dec=1", "mag=1", "one=1"]),
            false,
        ),
        (
            &["urn:g1", "urn:g2"],
            want(&["big=1", "dec=1", "mag=2", "one=1"]),
            true,
        ),
        // The union default graph: g0 (empty: the index holds nothing for
        // it), g1 and g2.
        (&[], want(&["big=1", "dec=1", "mag=2", "one=1"]), true),
    ];
    let _switch = multigraph::switch().lock().await;
    let mut failures = Vec::new();
    for (graphs, expected, union) in cases {
        for jsonld in [false, true] {
            set_fast_paths_disabled(false);
            let (fast, proceeded) = numbig_counts(&fluree, &view, graphs, jsonld).await;
            set_fast_paths_disabled(true);
            let (generic, _) = numbig_counts(&fluree, &view, graphs, jsonld).await;
            set_fast_paths_disabled(false);
            let label = format!("{graphs:?} jsonld={jsonld}");
            if fast != expected {
                failures.push(format!("{label}: fast {fast:?}, want {expected:?}"));
            }
            if generic != expected {
                failures.push(format!("{label}: generic {generic:?}, want {expected:?}"));
            }
            if union && !proceeded.iter().any(|s| s == E_UNION) {
                failures.push(format!("{label}: {E_UNION} did not proceed: {proceeded:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// ---------------------------------------------------------------------------
// Lane B: the cyclic join (`Shape::Triangle`).

const CYCLIC: &str = "cyclic_bgp";
const CYCLIC_UNION: &str = "cyclic_bgp_union";

/// The triangle in every scope and surface of fixture A in `state`, answered
/// by hand and by the general plan, and routed by scope: the union variant
/// answers a default graph of several graphs (an empty default graph among
/// them included: a member that holds nothing adds no edges), and of none;
/// the one-graph lane answers one graph; neither runs inside `GRAPH` (its
/// inner plan is seeded, and the planner builds the cyclic operator only for
/// an unseeded pattern list) or over two ledgers.
async fn triangle_routes_by_scope(state: State) {
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
            let run = check(&fx, scope, Shape::Triangle, surface, &mut failures).await;
            let (must_fire, must_not_fire): (&[&str], &[&str]) = match (scope, scope.kind()) {
                // The planner builds the cyclic operator only for an unseeded
                // pattern list, and GRAPH seeds its inner plan.
                (Scope::GraphG2 | Scope::GraphVar, _) | (_, Kind::None) => {
                    (&[], &[CYCLIC, CYCLIC_UNION])
                }
                // Several graphs, an empty default graph among them included,
                // and a default graph of none.
                (_, Kind::Union | Kind::Empty) => (&[CYCLIC_UNION], &[CYCLIC]),
                (_, Kind::One) => (&[CYCLIC], &[CYCLIC_UNION]),
            };
            assert_routing(&run, must_fire, must_not_fire, &mut failures);
            runs += 1;
        }
    }
    assert!(runs > 0, "no scope ran");
    assert!(
        failures.is_empty(),
        "{} of {runs} runs failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn triangle_lanes_indexed() {
    triangle_routes_by_scope(State::Indexed).await;
}

#[tokio::test(flavor = "current_thread")]
async fn triangle_lanes_with_novelty_in_g2() {
    triangle_routes_by_scope(State::Novelty).await;
}

#[tokio::test(flavor = "current_thread")]
async fn triangle_lanes_read_below_the_index() {
    triangle_routes_by_scope(State::Indexed2).await;
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

const UNION_SCOPES: &[(&str, &str, &str)] = &[
    ("FROM g1 FROM g2", "", "FROM <urn:g1> FROM <urn:g2>"),
    (
        "union default graph",
        "# PRAGMA union-default-graph: true\n",
        "",
    ),
];

/// `run`'s answers against `want`, and its routing: `must_fire` proceeded,
/// `must_not_fire` did not.
fn check_lane_f(
    label: &str,
    run: &lane_f::Run,
    want: &[String],
    must_fire: &[&str],
    must_not_fire: &[&str],
    failures: &mut Vec<String>,
) {
    for (lane, got) in [
        ("fast paths on", &run.fast),
        ("fast paths off", &run.generic),
    ] {
        match got {
            Ok(rows) if rows == want => {}
            Ok(rows) => failures.push(format!("{label}: {lane}: {rows:?}, want {want:?}")),
            Err(e) => failures.push(format!("{label}: {lane}: {e}")),
        }
    }
    for site in must_fire {
        if !run.proceeded.iter().any(|s| s == site) {
            failures.push(format!(
                "{label}: `{site}` did not proceed (proceeded {:?}, fell back {:?})",
                run.proceeded, run.fell_back
            ));
        }
    }
    for site in must_not_fire {
        if run.proceeded.iter().any(|s| s == site) {
            failures.push(format!("{label}: `{site}` proceeded"));
        }
    }
}

/// Lane F over fixture F: under a union of g1 and g2, every shape answers as
/// the union's facts do and as the one-graph floor does, with fast paths on
/// and off, and the union probe answers it; over g1 alone the one-graph
/// lanes answer and the union site stays off. The membership join keeps its
/// keep/drop path under the union, and goes exact where a list position
/// repeats a key.
///
/// On JSON-LD a list's members are values, so every shape runs through its
/// lane. On SPARQL a predicate holding lists is read through the list view,
/// a list both graphs hold is two lists (its cells are blank nodes local to
/// each graph), and no batched lane joins that predicate; its list-free twin
/// on the same ledger is joined by the lane.
#[tokio::test(flavor = "current_thread")]
async fn lane_f_union_probes_answer_by_fact() {
    use lane_f::{Shape, Surface};
    let fx = lane_f::Fixture::build().await;
    let mut failures = Vec::new();
    for &surface in lane_f::SURFACES {
        for (ledger, floor, lists) in [
            (lane_f::LEDGER, lane_f::FLOOR, false),
            (lane_f::LEDGER_LISTS, lane_f::FLOOR_LISTS, true),
        ] {
            let facts = lane_f::facts(lists);
            let g1_facts: Vec<_> = facts
                .iter()
                .filter(|f| f.g == lane_f::G1)
                .cloned()
                .collect();
            let floor_facts = lane_f::union_of(&facts);
            for &shape in lane_f::SHAPES {
                let label =
                    |scope: &str| format!("{surface:?} / {shape:?} / {scope} / lists {lists}");
                let want_floor = lane_f::expected_on(surface, shape, &floor_facts);
                match fx.on(surface, floor, "", "", shape).await {
                    Ok(rows) if rows == want_floor => {}
                    other => failures.push(format!(
                        "{}: floor {other:?}, want {want_floor:?}",
                        label("floor")
                    )),
                }
                let want = lane_f::expected_on(surface, shape, &facts);
                let list_view = surface == Surface::Sparql && lane_f::probes_a_list(shape, &facts);
                // The membership join's key repeats where its predicate's list
                // holds one member twice (`y20 q (x20 x20)`).
                let repeats = lists && lane_f::probes_a_list(shape, &facts);
                let union_site = shape.site();
                for (name, prologue, dataset) in UNION_SCOPES {
                    let run = lane_f::run_on(&fx, surface, ledger, prologue, dataset, shape).await;
                    let (fire, not_fire): (&[&str], &[&str]) = match shape.base() {
                        _ if list_view => (&[], &[union_site, "nested-loop-batched"]),
                        // A list position repeating a key sends keep/drop exact.
                        Shape::Membership if repeats => (&[], &["membership-join"]),
                        Shape::Membership => (&["membership-join"], &[]),
                        _ => (&[union_site], &["nested-loop-batched"]),
                    };
                    check_lane_f(&label(name), &run, &want, fire, not_fire, &mut failures);
                    // The union probe names why it stood down on the list view.
                    if list_view
                        && !run.fell_back.iter().any(|(site, outcome)| {
                            site == "nested-loop-batched-union" && outcome == "fallback:list_view"
                        })
                    {
                        failures.push(format!(
                            "{}: no `fallback:list_view` at the union site ({:?})",
                            label(name),
                            run.fell_back
                        ));
                    }
                }
                // Over one graph the one-graph operators answer: the batched
                // probes for the subject and existence joins (the planner picks
                // another join for the object shape there), never the union's.
                let want_g1 = lane_f::expected_on(surface, shape, &g1_facts);
                let run = lane_f::run_on(&fx, surface, ledger, "", "FROM <urn:g1>", shape).await;
                let (fire, not_fire): (&[&str], &[&str]) = match shape.base() {
                    _ if list_view => (&[], &["nested-loop-batched", "nested-loop-batched-union"]),
                    Shape::Membership if repeats => (&[], &["membership-join"]),
                    Shape::Membership => (&["membership-join"], &[]),
                    Shape::Object => (&[], &["nested-loop-batched-union"]),
                    _ => (&["nested-loop-batched"], &["nested-loop-batched-union"]),
                };
                check_lane_f(
                    &label("FROM g1"),
                    &run,
                    &want_g1,
                    fire,
                    not_fire,
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Lane F follows each member's own novelty, per graph and per predicate
/// (lane B's rule). Novelty outside the union, or on another predicate,
/// leaves the union probe running. A member the index has no branch for,
/// whose novelty holds the probed predicate, stands it down (`Novelty`);
/// for now so does a member with a branch and such novelty. Every answer is
/// the union's. JSON-LD, where the probed predicate's list members are
/// values and the probe runs (on SPARQL the list view joins it).
#[tokio::test(flavor = "current_thread")]
async fn lane_f_union_probes_follow_each_members_novelty() {
    use lane_f::{fact, Obj, Shape, G2, LEDGER};
    let fx = lane_f::Fixture::build().await;
    let mut facts = lane_f::facts(false);
    let mut failures = Vec::new();
    let in_g1_g2 = |facts: &[lane_f::Fact]| -> Vec<lane_f::Fact> {
        facts
            .iter()
            .filter(|f| f.g == lane_f::G1 || f.g == G2)
            .cloned()
            .collect()
    };
    let (from, pragma) = (UNION_SCOPES[0], UNION_SCOPES[1]);
    let union = "nested-loop-batched-union";
    let json = lane_f::Surface::JsonLd;

    // Novelty in g2 on another predicate: g2 holds nothing new for `v`.
    let other = vec![fact(G2, "z", "other", Obj::Int(1))];
    fx.write(LEDGER, &other).await;
    facts.extend(other);
    let run = lane_f::run_on(&fx, json, LEDGER, from.1, from.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &in_g1_g2(&facts));
    check_lane_f("other predicate", &run, &want, &[union], &[], &mut failures);

    // Novelty for `v` in the default graph, which the index has no branch
    // for: outside `FROM g1 FROM g2`, a member of the union default graph.
    let default_graph = vec![fact("", "y2", "v", Obj::Str("d".into()))];
    fx.write(LEDGER, &default_graph).await;
    facts.extend(default_graph);
    let run = lane_f::run_on(&fx, json, LEDGER, from.1, from.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &in_g1_g2(&facts));
    check_lane_f("novelty outside", &run, &want, &[union], &[], &mut failures);
    let run = lane_f::run_on(&fx, json, LEDGER, pragma.1, pragma.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &facts);
    check_lane_f(
        "branchless member",
        &run,
        &want,
        &[],
        &[union],
        &mut failures,
    );
    if !run
        .fell_back
        .iter()
        .any(|(s, o)| s == union && o == "fallback:novelty")
    {
        failures.push(format!(
            "branchless member: no `{union}` novelty decline ({:?})",
            run.fell_back
        ));
    }

    // Novelty for `v` in g2, which has a branch.
    let g2 = vec![fact(G2, "y0", "v", Obj::Dec("4.5"))];
    fx.write(LEDGER, &g2).await;
    facts.extend(g2);
    let run = lane_f::run_on(&fx, json, LEDGER, from.1, from.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &in_g1_g2(&facts));
    check_lane_f(
        "member with novelty",
        &run,
        &want,
        &[],
        &[union],
        &mut failures,
    );

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A one-graph join whose probed graph has no PSOT branch: the default graph
/// (fixture F's facts are all in g1 and g2) and a named graph first written
/// after the index, each holding left rows in novelty only. The probed
/// predicate has no novelty there, so the graph holds nothing for the probe:
/// the join answers with no rows, rather than failing to find the branch.
#[tokio::test(flavor = "current_thread")]
async fn a_probe_over_a_graph_with_no_branch_finds_nothing() {
    use lane_f::{fact, Obj, LEDGER};
    let fx = lane_f::Fixture::build().await;
    fx.write(
        LEDGER,
        &[
            fact("", "a", "sel", Obj::Iri("y0".into())),
            fact("urn:g3", "b", "sel", Obj::Iri("y0".into())),
        ],
    )
    .await;
    let mut failures = Vec::new();
    let body = lane_f::Shape::Subject.body();
    let in_g3 = format!("GRAPH <urn:g3> {{ {body} }}");
    let body = body.as_str();
    for (name, dataset, body) in [
        ("default graph", "", body),
        ("FROM g3", "FROM <urn:g3>", body),
        ("GRAPH g3", "", in_g3.as_str()),
    ] {
        let run = lane_f::run_where(&fx, LEDGER, "", dataset, body, &["y", "o"]).await;
        check_lane_f(name, &run, &[], &[], &[], &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Union rows stay encoded: an arena literal (a big number) leaves its member
/// with the graph it was read from, so every consumer reads the value its own
/// graph holds. Fixture F's trap on `vf` (no lists, so SPARQL reads it as
/// values): `y0 vf` is 1.5 in g1 and 2.5 in g2, both the first big-number
/// handle of their graph; `y1 vf 3.5` is in both (one fact of the union);
/// `y10 vf 7.5` is in g2. Each consumer that decodes or keys a binding (scan
/// output, ORDER BY, FILTER, DISTINCT, GROUP BY) answers by value under both
/// union spellings, with fast paths on and off, on the index and again with
/// novelty in one member (which then binds decoded beside the encoded other).
#[tokio::test(flavor = "current_thread")]
async fn encoded_union_rows_keep_each_arena_literal_in_its_graph() {
    use lane_f::{fact, Obj, G2, LEDGER};
    let fx = lane_f::Fixture::build().await;
    let numeric = "?s ex:vf ?o FILTER(isNumeric(?o))";
    // (label, WHERE body, projected vars, rows on the index, rows added by the
    // novelty write of `y0 vf 4.5` in g2)
    type Case<'a> = (&'a str, String, &'a [&'a str], Vec<&'a str>, Vec<&'a str>);
    let cases: Vec<Case<'_>> = vec![
        (
            "scan",
            "ex:y0 ex:vf ?o".into(),
            &["o"],
            vec!["1.5^^decimal", "2.5^^decimal"],
            vec!["4.5^^decimal"],
        ),
        (
            "ORDER BY, lowest",
            format!("{{ SELECT ?s ?o WHERE {{ {numeric} }} ORDER BY ?o LIMIT 1 }}"),
            &["s", "o"],
            vec!["y0 1.5^^decimal"],
            vec![],
        ),
        (
            "ORDER BY, highest",
            format!("{{ SELECT ?s ?o WHERE {{ {numeric} }} ORDER BY DESC(?o) LIMIT 1 }}"),
            &["s", "o"],
            vec!["y10 7.5^^decimal"],
            vec![],
        ),
        (
            "FILTER > 2",
            "?s ex:vf ?o FILTER(isNumeric(?o) && ?o > 2)".into(),
            &["s", "o"],
            vec!["y0 2.5^^decimal", "y1 3.5^^decimal", "y10 7.5^^decimal"],
            vec!["y0 4.5^^decimal"],
        ),
        (
            "DISTINCT",
            format!("{{ SELECT DISTINCT ?o WHERE {{ {numeric} }} }}"),
            &["o"],
            vec![
                "1.5^^decimal",
                "2.5^^decimal",
                "3.5^^decimal",
                "7.5^^decimal",
            ],
            vec!["4.5^^decimal"],
        ),
        (
            "GROUP BY",
            format!("{{ SELECT ?o (COUNT(*) AS ?n) WHERE {{ {numeric} }} GROUP BY ?o }}"),
            &["o", "n"],
            vec![
                "1.5^^decimal 1^^integer",
                "2.5^^decimal 1^^integer",
                "3.5^^decimal 1^^integer",
                "7.5^^decimal 1^^integer",
            ],
            vec!["4.5^^decimal 1^^integer"],
        ),
    ];
    let mut failures = Vec::new();
    for novelty in [false, true] {
        if novelty {
            fx.write(LEDGER, &[fact(G2, "y0", "vf", Obj::Dec("4.5"))])
                .await;
        }
        let phase = if novelty { "novelty in g2" } else { "indexed" };
        for (scope, prologue, dataset) in UNION_SCOPES {
            for (label, body, vars, base, added) in &cases {
                let mut want: Vec<String> = base.iter().map(|s| (*s).to_string()).collect();
                if novelty {
                    want.extend(added.iter().map(|s| (*s).to_string()));
                    want.sort();
                }
                let run = lane_f::run_where(&fx, LEDGER, prologue, dataset, body, vars).await;
                check_lane_f(
                    &format!("{phase} / {scope} / {label}"),
                    &run,
                    &want,
                    &[],
                    &[],
                    &mut failures,
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ============================================================================
// The list fixtures (#2159): every union lane over lists in two members, and
// over a list only novelty holds
// ============================================================================

/// The shapes the list fixtures run, and the union site of the lane each
/// exercises (M2's bound is pushed into each member's scan: it has no union
/// site, and is checked by its answer alone).
const LIST_SHAPES: &[(Shape, Option<&str>)] = &[
    (Shape::ExistsInExpression, Some(EXISTS_SITES.1)),
    (Shape::BoundObjectCount, Some(OBJECT_COUNT_SITES.1)),
    (Shape::BoundNumericCount, Some(OBJECT_COUNT_SITES.1)),
    (Shape::PerPredicateCount, Some(E_UNION)),
    (Shape::Triangle, Some(CYCLIC_UNION)),
    (Shape::GroupByObject, Some(A_UNION)),
    (Shape::SubjectProbe, Some("nested-loop-batched-union")),
    (Shape::StringBound, None),
];

/// Every lane shape over a list fixture, in every scope whose default graph
/// is several graphs or none, on both surfaces: the answer with fast paths
/// on equals the general plan's. Each lane's predicate holds lists, so under
/// SPARQL's list view each union site either answers as the general plan
/// does or stands down, and every lane names `fallback:list_view` at least
/// once; JSON-LD reads members as values, and over lists in two indexed
/// members every lane's union site answers at least once.
async fn lanes_over_lists(case: ListCase) {
    let fx = Fixture::build_lists(case).await;
    let mut failures = Vec::new();
    let mut runs = 0;
    // Per (shape, surface): the outcomes its lane's union site stamped.
    let mut seen: Vec<(Shape, Surface, String)> = Vec::new();
    for &(shape, site) in LIST_SHAPES {
        for &scope in SCOPES {
            if !scope.applies(fx.state) || !matches!(scope.kind(), Kind::Union | Kind::Empty) {
                continue;
            }
            for surface in [Surface::Sparql, Surface::JsonLd] {
                if !scope.supports(surface) {
                    continue;
                }
                let run = multigraph::run(&fx, scope, shape, surface).await;
                runs += 1;
                match (&run.fast, &run.generic) {
                    (Ok(fast), Ok(generic)) if fast == generic => {}
                    (fast, generic) => failures.push(format!(
                        "{}: fast paths on {fast:?}, off {generic:?}",
                        run.label
                    )),
                }
                if let Some(site) = site {
                    let outcomes = run
                        .proceeded
                        .iter()
                        .filter(|s| *s == site)
                        .map(|_| "proceed".to_string())
                        .chain(
                            run.fell_back
                                .iter()
                                .filter(|(s, _)| s == site)
                                .map(|(_, o)| o.clone()),
                        );
                    for outcome in outcomes {
                        seen.push((shape, surface, outcome));
                    }
                }
            }
        }
    }
    for &(shape, _) in LIST_SHAPES {
        for surface in [Surface::Sparql, Surface::JsonLd] {
            let mut outcomes: Vec<&str> = seen
                .iter()
                .filter(|(sh, su, _)| *sh == shape && *su == surface)
                .map(|(_, _, o)| o.as_str())
                .collect();
            outcomes.sort_unstable();
            outcomes.dedup();
            eprintln!("{case:?} {shape:?} {surface:?}: {outcomes:?}");
        }
    }
    // Each lane was reached: under SPARQL it named its decline, and over
    // indexed lists its union site answered JSON-LD.
    for &(shape, site) in LIST_SHAPES {
        if site.is_none() {
            continue;
        }
        let stamped = |surface, outcome: &str| {
            seen.iter()
                .any(|(sh, su, o)| *sh == shape && *su == surface && o == outcome)
        };
        if !stamped(Surface::Sparql, "fallback:list_view") {
            failures.push(format!("{case:?}: {shape:?} never declined with list_view"));
        }
        if case == ListCase::TwoMembers && !stamped(Surface::JsonLd, "proceed") {
            failures.push(format!(
                "{case:?}: {shape:?}'s union site never answered JSON-LD"
            ));
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
async fn lanes_over_lists_in_two_members() {
    lanes_over_lists(ListCase::TwoMembers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lanes_over_a_list_only_novelty_holds() {
    lanes_over_lists(ListCase::NoveltyOnly).await;
}

/// SPARQL's list view sees a list only novelty holds: over the union, `L`'s
/// `items` is the list's head, one blank node, not its two members.
#[tokio::test(flavor = "current_thread")]
async fn the_list_view_reads_a_list_only_novelty_holds() {
    let fx = Fixture::build_lists(ListCase::NoveltyOnly).await;
    let query = format!(
        "PREFIX ex: <{}>\nSELECT ?o FROM <{G1}> FROM <{G2}> WHERE {{ ex:L ex:items ?o }}",
        multigraph::EX
    );
    let json = fx
        .view(multigraph::LEDGER)
        .await
        .query(&fx.fluree)
        .sparql(&query)
        .execute_formatted()
        .await
        .expect("query");
    let bindings = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "one owner edge: {bindings:?}");
    assert_eq!(
        bindings[0]["o"]["type"], "bnode",
        "the list's head: {bindings:?}"
    );
}
