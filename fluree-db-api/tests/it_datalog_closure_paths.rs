//! Transitive-closure rules evaluated as one property path.
//!
//! A recursive group of one rule shaped as a transitive closure runs once,
//! as a body with a one-or-more path, instead of iterating. Every shape must
//! derive exactly the facts the whole-program reference derives, on a graph
//! with a cycle, a self-loop, branching, asserted and literal-valued facts of
//! the closure predicate, and edges in a named graph that the default graph
//! must not see. Rules one step away from a closure keep iterating, and so
//! does a left-linear closure while its path predicate has a literal object.

use crate::it_datalog_rule_schedule::{
    chain_data, fact_set, jsonld_rules, ledger_with, run, sparql_rules,
};
use crate::it_datalog_semi_naive::{both_schedules, derived};
use crate::support::{genesis_ledger, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_query::datalog_rules::{run_fixpoint_with_schedule, RuleExecutionEnv, RuleSchedule};
use fluree_db_query::reasoning::ReasoningBudget;
use serde_json::json;
use std::time::Duration;

const BASE: &str = "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:next ?y }";

const DATA: &str = r#"
@prefix ex: <http://example.org/> .
ex:a ex:next ex:b . ex:b ex:next ex:c . ex:c ex:next ex:a .
ex:c ex:next ex:d . ex:d ex:next ex:d . ex:d ex:next ex:e . ex:e ex:next ex:f .
ex:b ex:next ex:g . ex:g ex:next ex:h .
ex:h ex:reach ex:a .
ex:h ex:reach "tag" .
GRAPH ex:side { ex:f ex:next ex:a . ex:f ex:reach ex:g . }
"#;

async fn trig_ledger(ledger_id: &str, trig: &str) -> MemoryLedger {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("seed")
        .ledger
}

fn step(body: &str) -> String {
    format!("CONSTRUCT {{ ?x ex:reach ?z }} WHERE {{ {body} }}")
}

#[tokio::test]
async fn each_closure_shape_runs_as_one_path_and_matches_iteration() {
    let shapes = [
        ("right-linear", "?x ex:next ?y . ?y ex:reach ?z"),
        ("right-linear-inverse", "?y ex:next ?x . ?y ex:reach ?z"),
        ("left-linear", "?x ex:reach ?y . ?y ex:next ?z"),
        ("left-linear-inverse", "?x ex:reach ?y . ?z ex:next ?y"),
        ("nonlinear", "?x ex:reach ?y . ?y ex:reach ?z"),
    ];
    for (shape, body) in shapes {
        let ledger = trig_ledger(&format!("datalog/closure-{shape}:main"), DATA).await;
        let (scheduled, reference) = both_schedules(&ledger, &[BASE.to_string(), step(body)]).await;
        // The base rule's pass, then the closure's single pass.
        assert_eq!(scheduled.diagnostics.iterations, 2, "{shape}");
        assert_eq!(scheduled.rule_body_runs, 2, "{shape}");
        assert!(reference.diagnostics.iterations > 3, "{shape}");
    }
}

#[tokio::test]
async fn a_closure_path_keeps_cycles_self_loops_and_graph_scope() {
    let ledger = trig_ledger("datalog/closure-semantics:main", DATA).await;
    let (scheduled, _) = both_schedules(
        &ledger,
        &[BASE.to_string(), step("?x ex:next ?y . ?y ex:reach ?z")],
    )
    .await;
    let reach = derived(&scheduled, &ledger, "reach");
    let has = |s: &str, o: &str| reach.contains(&(s.to_string(), o.to_string()));
    assert!(has("a", "a"), "a reaches itself around the cycle");
    assert!(has("d", "d"), "d reaches itself through its self-loop");
    assert!(has("g", "a"), "g reaches a through the asserted h reach a");
    assert!(
        has("g", "String(\"tag\")"),
        "the literal-valued reach extends"
    );
    assert!(
        !has("e", "a") && !has("f", "g"),
        "edges in the named graph are not part of the default graph"
    );
}

/// A closure far larger than the budget stops at the budget instead of
/// producing its whole closure first. (A path with both ends free, which
/// computes all its pairs before returning a row, iterates instead once it
/// passes what the budget has room for; the path operator's tests cover that
/// limit.)
#[tokio::test]
async fn a_closure_larger_than_the_budget_stops_at_it() {
    const LEN: usize = 400;
    const MAX_FACTS: usize = 2_000;
    let shapes = [
        ("right-linear", "?x ex:parent ?y . ?y ex:ancestor ?z"),
        ("nonlinear", "?x ex:ancestor ?y . ?y ex:ancestor ?z"),
        ("left-linear", "?x ex:ancestor ?y . ?y ex:parent ?z"),
    ];
    for (shape, body) in shapes {
        let ledger = ledger_with(
            &format!("datalog/closure-budget-{shape}:main"),
            chain_data(LEN),
        )
        .await;
        let rules = sparql_rules(
            &ledger,
            &[
                "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
                format!("CONSTRUCT {{ ?x ex:ancestor ?z }} WHERE {{ {body} }}"),
            ],
        );
        let result = run_fixpoint_with_schedule(
            ledger.as_graph_db_ref(0),
            &rules,
            100,
            &ReasoningBudget::new(Duration::from_secs(600), MAX_FACTS, usize::MAX),
            RuleExecutionEnv::default(),
            RuleSchedule::Dependencies,
        )
        .await
        .expect("fixpoint");

        assert_eq!(
            result.diagnostics.capped_reason.as_deref(),
            Some("facts"),
            "{shape}"
        );
        assert_eq!(result.derived_flakes.len(), MAX_FACTS, "{shape}");
        assert!(
            result.body_rows < 4 * MAX_FACTS,
            "{shape}: {} body rows of a {}-pair closure for a budget of {MAX_FACTS} facts",
            result.body_rows,
            LEN * (LEN - 1) / 2
        );
    }
}

/// The left-linear path ends at the head's object, and a path follows only
/// IRI-valued objects. While `ex:next` has a literal object the rule
/// iterates, so the facts ending at that literal are derived.
#[tokio::test]
async fn a_left_linear_closure_iterates_while_its_path_has_literal_objects() {
    let data = format!("{DATA}\nex:f ex:next \"end\" .\n");
    let ledger = trig_ledger("datalog/closure-literal:main", &data).await;
    let (scheduled, _) = both_schedules(
        &ledger,
        &[BASE.to_string(), step("?x ex:reach ?y . ?y ex:next ?z")],
    )
    .await;
    assert!(scheduled.diagnostics.iterations > 2);
    assert!(derived(&scheduled, &ledger, "reach")
        .contains(&("a".to_string(), "String(\"end\")".to_string())));
}

/// Bodies one step away from a closure keep iterating, and must still
/// derive exactly what iteration derives.
#[tokio::test]
async fn near_closure_rules_iterate_and_match_iteration() {
    let near = [
        // Head reversed.
        "CONSTRUCT { ?z ex:reach ?x } WHERE { ?x ex:next ?y . ?y ex:reach ?z }",
        // Recursive pattern reversed.
        "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:next ?y . ?z ex:reach ?y }",
        // Second recursive pattern reversed.
        "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:reach ?y . ?z ex:reach ?y }",
        // Head object is the step's end, not the recursion's.
        "CONSTRUCT { ?y ex:reach ?x } WHERE { ?x ex:next ?y . ?x ex:reach ?z }",
        // Step does not touch the recursive pattern's free end.
        "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:next ?w . ?y ex:reach ?z }",
    ];
    for (i, rule) in near.iter().enumerate() {
        let ledger = trig_ledger(&format!("datalog/closure-near-{i}:main"), DATA).await;
        let (scheduled, _) = both_schedules(&ledger, &[BASE.to_string(), rule.to_string()]).await;
        assert!(scheduled.diagnostics.iterations > 2, "{rule}");
    }
}

/// The policy contract's ancestor rule nests its recursive read in JSON-LD,
/// which lowers to the nonlinear shape, so those tests read a path-derived
/// closure.
#[tokio::test]
async fn a_nested_json_ld_closure_runs_as_one_path() {
    let ledger = trig_ledger("datalog/closure-jsonld:main", DATA).await;
    let ctx = json!({"ex": "http://example.org/"});
    let rule_set = jsonld_rules(
        &ledger,
        vec![
            json!({
                "@context": ctx,
                "where": {"@id": "?x", "ex:next": "?y"},
                "insert": {"@id": "?x", "ex:reach": {"@id": "?y"}}
            }),
            json!({
                "@context": ctx,
                "where": {"@id": "?x", "ex:reach": {"ex:reach": "?z"}},
                "insert": {"@id": "?x", "ex:reach": {"@id": "?z"}}
            }),
        ],
    );
    let scheduled = run(&ledger, &rule_set, RuleSchedule::Dependencies).await;
    let reference = run(&ledger, &rule_set, RuleSchedule::WholeProgram).await;
    assert_eq!(fact_set(&scheduled), fact_set(&reference));
    assert_eq!(scheduled.diagnostics.iterations, 2);
}
