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

use multigraph::{assert_routing, check, Fixture, Graphs, Shape, State, Surface, SCOPES, SHAPES};

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

// ---------------------------------------------------------------------------
// Lane B: the cyclic join (`Shape::Triangle`).

const CYCLIC: &str = "cyclic_bgp";
const CYCLIC_UNION: &str = "cyclic_bgp_union";

/// The triangle in every scope and surface of fixture A in `state`, answered
/// by hand and by the general plan, and routed by scope: the union variant
/// answers a default graph of several graphs, and of none; the one-graph
/// lane answers one graph; neither runs inside `GRAPH` (its inner plan is
/// seeded, and the planner builds the cyclic operator only for an unseeded
/// pattern list) or over two ledgers.
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
            let (must_fire, must_not_fire): (&[&str], &[&str]) = match scope.graphs() {
                Graphs::G1G2 | Graphs::G0G1G2 | Graphs::Empty => (&[CYCLIC_UNION], &[CYCLIC]),
                Graphs::G0 | Graphs::G1 => (&[CYCLIC], &[CYCLIC_UNION]),
                Graphs::G2 | Graphs::PerNamedGraph | Graphs::G0Other => {
                    (&[], &[CYCLIC, CYCLIC_UNION])
                }
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
