//! Fixture A for the lanes that read several graphs of one ledger, checked
//! before any lane leans on it. In every scope of design §5's table and every
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

use multigraph::{check, Fixture, State, Surface, SCOPES, SHAPES};

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
#[tokio::test(flavor = "current_thread")]
async fn lane_f_union_probes_answer_by_fact() {
    let fx = lane_f::Fixture::build().await;
    let mut failures = Vec::new();
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
        for &shape in lane_f::SHAPES {
            let want = lane_f::expected(shape, &facts);
            let label = |scope: &str| format!("{shape:?} / {scope} / lists {lists}");
            match fx.sparql(floor, "", "", shape).await {
                Ok(rows) if rows == want => {}
                other => failures.push(format!(
                    "{}: floor {other:?}, want {want:?}",
                    label("floor")
                )),
            }
            let union_site = shape.site();
            for (name, prologue, dataset) in UNION_SCOPES {
                let run = lane_f::run(&fx, ledger, prologue, dataset, shape).await;
                let (fire, not_fire): (&[&str], &[&str]) = match shape {
                    // A list position repeating a key sends keep/drop exact.
                    lane_f::Shape::Membership if lists => (&[], &["membership-join"]),
                    lane_f::Shape::Membership => (&["membership-join"], &[]),
                    _ => (&[union_site], &["nested-loop-batched"]),
                };
                check_lane_f(&label(name), &run, &want, fire, not_fire, &mut failures);
            }
            // Over one graph the one-graph operators answer: the batched
            // probes for the subject and existence joins (the planner picks
            // another join for the object shape there), never the union's.
            let want_g1 = lane_f::expected(shape, &g1_facts);
            let run = lane_f::run(&fx, ledger, "", "FROM <urn:g1>", shape).await;
            let (fire, not_fire): (&[&str], &[&str]) = match shape {
                lane_f::Shape::Membership if lists => (&[], &["membership-join"]),
                lane_f::Shape::Membership => (&["membership-join"], &[]),
                lane_f::Shape::Object => (&[], &["nested-loop-batched-union"]),
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
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Lane F follows each member's own novelty, per graph and per predicate
/// (lane B's rule). Novelty outside the union, or on another predicate,
/// leaves the union probe running. A member the index has no branch for,
/// whose novelty holds the probed predicate, stands it down (`Novelty`);
/// for now so does a member with a branch and such novelty. Every answer is
/// the union's.
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

    // Novelty in g2 on another predicate: g2 holds nothing new for `v`.
    let other = vec![fact(G2, "z", "other", Obj::Int(1))];
    fx.write(LEDGER, &other).await;
    facts.extend(other);
    let run = lane_f::run(&fx, LEDGER, from.1, from.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &in_g1_g2(&facts));
    check_lane_f("other predicate", &run, &want, &[union], &[], &mut failures);

    // Novelty for `v` in the default graph, which the index has no branch
    // for: outside `FROM g1 FROM g2`, a member of the union default graph.
    let default_graph = vec![fact("", "y2", "v", Obj::Str("d".into()))];
    fx.write(LEDGER, &default_graph).await;
    facts.extend(default_graph);
    let run = lane_f::run(&fx, LEDGER, from.1, from.2, Shape::Subject).await;
    let want = lane_f::expected(Shape::Subject, &in_g1_g2(&facts));
    check_lane_f("novelty outside", &run, &want, &[union], &[], &mut failures);
    let run = lane_f::run(&fx, LEDGER, pragma.1, pragma.2, Shape::Subject).await;
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
    let run = lane_f::run(&fx, LEDGER, from.1, from.2, Shape::Subject).await;
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
