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

use multigraph::{check, Fixture, Run, Scope, Shape, State, Surface, SCOPES, SHAPES};

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

/// The route of a union lane in `scope` and `state` (design §5's table). The
/// union sites take a default graph of several graphs of one ledger, and of
/// none; novelty and a read below the index decline them. Novelty is
/// ledger-wide, so it also turns the one-graph sites off in every scope.
fn union_lane_route(scope: Scope, state: State) -> LaneRoute {
    let union = matches!(
        scope,
        Scope::FromG1G2
            | Scope::FromG1G2Connection
            | Scope::FromG0G1G2
            | Scope::UnionPragma
            | Scope::UnionOpts
            | Scope::UnionConfig
    );
    match scope {
        Scope::TwoLedgers => LaneRoute::Neither,
        // A default graph of none answers nothing, novelty or not.
        Scope::FromNamedOnly => LaneRoute::Union,
        Scope::TimeTravel => LaneRoute::UnionDeclines("fallback:novelty"),
        _ if state == State::Novelty && union => LaneRoute::UnionDeclines("fallback:novelty"),
        _ if state == State::Novelty => LaneRoute::Neither,
        _ if union => LaneRoute::Union,
        _ => LaneRoute::One,
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
/// answers by hand, and the route `one_answered` and the sites say it took.
async fn union_lane_routes(
    state: State,
    shape: Shape,
    sites: (&str, &str),
    one_answered: fn(&Run, &str) -> bool,
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
            check_lane_route(
                &run,
                union_lane_route(scope, state),
                answered,
                sites,
                &mut failures,
            );
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
    )
    .await;
}
