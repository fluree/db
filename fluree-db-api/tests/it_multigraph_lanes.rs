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
    assert_routing, check, Fixture, Graphs, Kind, Run, Scope, Shape, State, Surface, SCOPES, SHAPES,
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

/// The route of a union lane in `scope` and `state` (design §5's table), by
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
