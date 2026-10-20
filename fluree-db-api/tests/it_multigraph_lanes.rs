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
    assert_routing, check, Fixture, Graphs, Kind, Scope, Shape, State, Surface, SCOPES, SHAPES,
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
