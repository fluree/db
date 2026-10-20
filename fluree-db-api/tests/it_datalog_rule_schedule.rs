//! The dependency schedule against the whole-program reference.
//!
//! Under the dependency schedule a rule that cannot observe its own
//! conclusions runs once, after the rules it reads from; recursive groups
//! iterate on their own. The whole-program schedule re-runs every rule every
//! round. Both must derive the same facts for repeatable rules; the body-run
//! count proves which schedule actually ran.

use crate::it_datalog_semi_naive::derived;
use crate::support::{genesis_ledger, normalize_rows, query_jsonld, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::Sid;
use fluree_db_query::datalog_rules::{
    parse_query_time_rule, run_fixpoint_with_schedule, DatalogExecutionResult, DatalogRuleSet,
    RuleExecutionEnv, RuleSchedule,
};
use fluree_db_query::reasoning::ReasoningBudget;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_ITERATIONS: usize = 100;

pub(crate) async fn ledger_with(ledger_id: &str, data: Value) -> MemoryLedger {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    fluree.insert(ledger, &data).await.expect("seed").ledger
}

pub(crate) fn sparql_rules(ledger: &MemoryLedger, rules: &[String]) -> DatalogRuleSet {
    let mut set = DatalogRuleSet::new();
    for (i, construct) in rules.iter().enumerate() {
        let rule = json!({
            "@id": format!("ex:rule{i:02}"),
            "f:rule": {
                "@type": "f:sparql",
                "@value": format!("PREFIX ex: <http://example.org/>\n{construct}")
            }
        });
        let parsed = parse_query_time_rule(&rule, &ledger.snapshot, i)
            .unwrap_or_else(|e| panic!("rule {construct} failed to parse: {e}"));
        set.add_rule(parsed).expect("unique ids");
    }
    set
}

/// JSON-LD rules (`where`/`insert`, with their own `@context`).
pub(crate) fn jsonld_rules(ledger: &MemoryLedger, rules: Vec<Value>) -> DatalogRuleSet {
    let mut set = DatalogRuleSet::new();
    for (i, rule) in rules.into_iter().enumerate() {
        let stored = json!({"@id": format!("ex:rule{i:02}"), "f:rule": rule});
        let parsed = parse_query_time_rule(&stored, &ledger.snapshot, i)
            .unwrap_or_else(|e| panic!("rule {i} failed to parse: {e}"));
        set.add_rule(parsed).expect("unique ids");
    }
    set
}

pub(crate) async fn run(
    ledger: &MemoryLedger,
    rules: &DatalogRuleSet,
    schedule: RuleSchedule,
) -> DatalogExecutionResult {
    run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        rules,
        MAX_ITERATIONS,
        &ReasoningBudget::unlimited(),
        RuleExecutionEnv::default(),
        schedule,
    )
    .await
    .expect("fixpoint")
}

pub(crate) fn fact_set(result: &DatalogExecutionResult) -> BTreeSet<(Sid, Sid, String)> {
    result
        .derived_flakes
        .iter()
        .map(|f| (f.s.clone(), f.p.clone(), format!("{:?}", f.o)))
        .collect()
}

pub(crate) fn chain_data(len: usize) -> Value {
    let graph: Vec<Value> = (0..len - 1)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:parent": {"@id": format!("ex:n{}", i + 1)}}))
        .chain(std::iter::once(
            json!({"@id": format!("ex:n{}", len - 1), "@type": "ex:Root"}),
        ))
        .collect();
    json!({"@context": {"ex": "http://example.org/"}, "@graph": graph})
}

#[tokio::test]
async fn downstream_rules_run_once_after_the_recursion_converges() {
    const DOWNSTREAM: usize = 6;
    let ledger = ledger_with("datalog/schedule-once:main", chain_data(8)).await;
    let mut rules = vec![
        "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
        "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:parent ?y . ?y ex:ancestor ?z }".to_string(),
    ];
    for k in 0..DOWNSTREAM {
        rules.push(format!(
            "CONSTRUCT {{ ?x ex:reaches{k} ?y }} WHERE {{ ?x ex:ancestor ?y . ?y a ex:Root }}"
        ));
    }
    let rules = sparql_rules(&ledger, &rules);

    let scheduled = run(&ledger, &rules, RuleSchedule::Dependencies).await;
    let reference = run(&ledger, &rules, RuleSchedule::WholeProgram).await;
    assert!(!scheduled.diagnostics.capped && !reference.diagnostics.capped);
    assert_eq!(fact_set(&scheduled), fact_set(&reference));

    // One pass for the base rule, the recursive step's passes, and one pass
    // in which every downstream rule runs exactly once.
    let recursive_passes = scheduled.diagnostics.iterations - 2;
    assert_eq!(
        scheduled.rule_body_runs,
        1 + recursive_passes + DOWNSTREAM,
        "each non-recursive rule must run once"
    );
    assert_eq!(
        reference.rule_body_runs,
        reference.diagnostics.iterations * rules.len(),
        "the reference re-runs every rule every round"
    );
    assert!(scheduled.rule_body_runs < reference.rule_body_runs);
}

/// A rule calling `STRUUID()` derives a different literal every time it
/// runs. Run once, after its inputs are complete, it derives one id per
/// subject; re-run every round, it never converges.
#[tokio::test]
async fn a_non_recursive_volatile_rule_runs_once() {
    let ledger = ledger_with("datalog/schedule-volatile:main", chain_data(4)).await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:caseId ?id } WHERE { ?x ex:parent ?y BIND(STRUUID() AS ?id) }"
                .to_string(),
        ],
    );

    let scheduled = run(&ledger, &rules, RuleSchedule::Dependencies).await;
    assert!(!scheduled.diagnostics.capped);
    assert_eq!(scheduled.derived_flakes.len(), 3, "one id per child");

    let reference = run(&ledger, &rules, RuleSchedule::WholeProgram).await;
    assert_eq!(
        reference.diagnostics.capped_reason.as_deref(),
        Some("iterations"),
        "re-running a STRUUID() rule every round never converges"
    );
}

/// A single non-recursive rule gets one pass, so the deadline must be
/// checked while its body runs, not only before it starts: otherwise a body
/// that outlasts the budget reports a completed fixpoint.
#[tokio::test]
async fn the_time_budget_caps_a_rule_that_outlasts_it() {
    const NODES: usize = 200;
    let graph: Vec<Value> = (0..NODES)
        .map(|i| json!({"@id": format!("ex:a{i}"), "ex:kind": {"@id": "ex:k"}}))
        .collect();
    let ledger = ledger_with(
        "datalog/schedule-deadline:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &["CONSTRUCT { ?x ex:sameKind ?y } WHERE { ?x ex:kind ?k . ?y ex:kind ?k }".to_string()],
    );
    let budget = ReasoningBudget::new(Duration::from_millis(2), usize::MAX, usize::MAX);

    let result = run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        MAX_ITERATIONS,
        &budget,
        RuleExecutionEnv::default(),
        RuleSchedule::Dependencies,
    )
    .await
    .expect("fixpoint");
    assert_eq!(
        result.diagnostics.capped_reason.as_deref(),
        Some("time"),
        "a {}-fact derivation cannot finish in 2ms; got {} facts reported complete",
        NODES * NODES,
        result.derived_flakes.len()
    );
    assert!(result.derived_flakes.len() < NODES * NODES);
}

/// A rule's heads are instantiated as its body's rows arrive, so a body
/// matching far more rows than the fact budget allows stops when the budget
/// is reached instead of producing every row first.
#[tokio::test]
async fn the_fact_budget_stops_a_body_that_matches_far_more_rows() {
    const NODES: usize = 300;
    const MAX_FACTS: usize = 100;
    let graph: Vec<Value> = (0..NODES)
        .map(|i| json!({"@id": format!("ex:a{i}"), "ex:kind": {"@id": "ex:k"}}))
        .collect();
    let ledger = ledger_with(
        "datalog/schedule-fact-budget:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &["CONSTRUCT { ?x ex:sameKind ?y } WHERE { ?x ex:kind ?k . ?y ex:kind ?k }".to_string()],
    );
    let budget = ReasoningBudget::new(Duration::from_secs(600), MAX_FACTS, usize::MAX);

    let result = run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        MAX_ITERATIONS,
        &budget,
        RuleExecutionEnv::default(),
        RuleSchedule::Dependencies,
    )
    .await
    .expect("fixpoint");
    assert_eq!(result.diagnostics.capped_reason.as_deref(), Some("facts"));
    assert_eq!(result.derived_flakes.len(), MAX_FACTS);
    assert!(
        result.body_rows < NODES * NODES / 4,
        "the body produced {} of its {} rows for a budget of {MAX_FACTS} facts",
        result.body_rows,
        NODES * NODES
    );
}

/// Derived facts carry the query's `t`. At the `t` of a commit that retracted
/// the stored copy of a fact the rules derive again, the retraction must not
/// hide the derived fact: not from rule bodies that read it while the
/// fixpoint runs, and not from the query.
#[tokio::test]
async fn a_fact_retracted_at_the_query_t_is_still_derived() {
    const NODES: usize = 7;
    let shapes = [
        ("right-linear", "?x ex:next ?y . ?y ex:reach ?z"),
        ("left-linear", "?x ex:reach ?y . ?y ex:next ?z"),
        ("nonlinear", "?x ex:reach ?y . ?y ex:reach ?z"),
    ];
    for (shape, body) in shapes {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = genesis_ledger(&fluree, &format!("datalog/retracted-at-t-{shape}:main"));
        let mut graph: Vec<Value> = (0..NODES - 1)
            .map(|i| json!({"@id": format!("ex:n{i}"), "ex:next": {"@id": format!("ex:n{}", i + 1)}}))
            .collect();
        let asserted = [("ex:n0", "ex:n2"), ("ex:n1", "ex:n3")];
        for (s, o) in asserted {
            graph.push(json!({"@id": s, "ex:reach": {"@id": o}}));
        }
        let ctx = json!({"ex": "http://example.org/"});
        let ledger = fluree
            .insert(ledger, &json!({"@context": ctx, "@graph": graph}))
            .await
            .expect("seed")
            .ledger;
        let retract: Vec<Value> = asserted
            .iter()
            .map(|(s, o)| json!({"@id": s, "ex:reach": {"@id": o}}))
            .collect();
        let ledger = fluree
            .update(ledger, &json!({"@context": ctx, "delete": retract}))
            .await
            .expect("retract")
            .ledger;

        let rules = [
            "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:next ?y }".to_string(),
            format!("CONSTRUCT {{ ?x ex:reach ?z }} WHERE {{ {body} }}"),
        ];
        let rule_set = sparql_rules(&ledger, &rules);
        let scheduled = run(&ledger, &rule_set, RuleSchedule::Dependencies).await;
        let reference = run(&ledger, &rule_set, RuleSchedule::WholeProgram).await;
        let closure = NODES * (NODES - 1) / 2;
        assert_eq!(scheduled.derived_flakes.len(), closure, "{shape}");
        assert_eq!(
            fact_set(&scheduled),
            fact_set(&reference),
            "{shape}: the schedules disagree"
        );

        let query = json!({
            "@context": ctx,
            "select": ["?x", "?y"],
            "where": {"@id": "?x", "ex:reach": "?y"},
            "rules": rules.iter().map(|rule| json!({
                "@type": "https://ns.flur.ee/db#sparql",
                "@value": format!("PREFIX ex: <http://example.org/>\n{rule}")
            })).collect::<Vec<_>>()
        });
        let rows = query_jsonld(&fluree, &ledger, &query)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("rows");
        let rows = normalize_rows(&rows);
        assert_eq!(rows.len(), closure, "{shape}: {rows:?}");
        for (s, o) in asserted {
            assert!(
                rows.contains(&json!([s, o])),
                "{shape}: {s} {o} missing from {rows:?}"
            );
        }
    }
}

/// A rule whose sequence path reads another rule's facts at a later step runs
/// after that rule, even when it is listed first.
#[tokio::test]
async fn a_sequence_path_reads_each_of_its_steps() {
    let ledger = ledger_with(
        "datalog/schedule-sequence-path:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": [
            {"@id": "ex:a", "ex:owns": {"@id": "ex:b"}},
            {"@id": "ex:b", "ex:likes": {"@id": "ex:c"}},
            {"@id": "ex:c", "ex:owns": {"@id": "ex:d"}},
            {"@id": "ex:d", "ex:likes": {"@id": "ex:e"}}
        ]}),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:q ?y } WHERE { ?x (ex:owns/ex:watched)+ ?y }".to_string(),
            "CONSTRUCT { ?x ex:watched ?y } WHERE { ?x ex:likes ?y }".to_string(),
        ],
    );
    let scheduled = run(&ledger, &rules, RuleSchedule::Dependencies).await;
    let reference = run(&ledger, &rules, RuleSchedule::WholeProgram).await;
    assert_eq!(fact_set(&scheduled), fact_set(&reference));
    let pair = |s: &str, o: &str| (s.to_string(), o.to_string());
    assert_eq!(
        derived(&scheduled, &ledger, "q"),
        vec![pair("a", "c"), pair("a", "e"), pair("c", "e")]
    );
}

/// Random positive programs over a small graph with one literal-valued fact
/// and a few facts retracted in the latest commit: chains of joins (often
/// transitive closures, linear or not, once their predicates are derived),
/// class reads and writes, variable predicates, repeated variables and
/// constant subjects, reads nested in UNION and EXISTS, one-or-more paths
/// over a predicate or a sequence, zero-length paths, and heads that
/// introduce fresh terms. Every rule is repeatable, so both schedules must
/// derive exactly the same facts.
#[tokio::test]
async fn random_programs_derive_the_same_facts_under_both_schedules() {
    const PREDICATES: usize = 4;
    const CLASSES: usize = 2;
    const NODES: usize = 7;

    let mut rng = StdRng::seed_from_u64(0x00da_7a10);
    for program in 0..120 {
        let mut graph = Vec::new();
        for _ in 0..12 {
            let p = rng.gen_range(0..PREDICATES);
            graph.push(json!({
                "@id": format!("ex:n{}", rng.gen_range(0..NODES)),
                format!("ex:p{p}"): {"@id": format!("ex:n{}", rng.gen_range(0..NODES))}
            }));
        }
        for _ in 0..3 {
            graph.push(json!({
                "@id": format!("ex:n{}", rng.gen_range(0..NODES)),
                "@type": format!("ex:C{}", rng.gen_range(0..CLASSES))
            }));
        }
        // A literal object ends any chain through its predicate.
        let p = rng.gen_range(0..PREDICATES);
        graph.push(json!({
            "@id": format!("ex:n{}", rng.gen_range(0..NODES)),
            format!("ex:p{p}"): "end"
        }));
        // Retracting asserted facts in the latest commit puts a retraction
        // at the query's `t`, which a fact the rules derive again must
        // survive.
        let retract: Vec<Value> = (0..4)
            .map(|_| graph[rng.gen_range(0..12)].clone())
            .collect();
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = genesis_ledger(&fluree, &format!("datalog/schedule-random-{program}:main"));
        let ctx = json!({"ex": "http://example.org/"});
        let ledger = fluree
            .insert(ledger, &json!({"@context": ctx, "@graph": graph}))
            .await
            .expect("seed")
            .ledger;
        let ledger = fluree
            .update(ledger, &json!({"@context": ctx, "delete": retract}))
            .await
            .expect("retract")
            .ledger;

        let rule_count = rng.gen_range(2..7);
        let rules: Vec<String> = (0..rule_count)
            .map(|_| random_rule(&mut rng, PREDICATES, CLASSES))
            .collect();
        let rule_set = sparql_rules(&ledger, &rules);

        let scheduled = run(&ledger, &rule_set, RuleSchedule::Dependencies).await;
        let reference = run(&ledger, &rule_set, RuleSchedule::WholeProgram).await;
        assert!(
            !scheduled.diagnostics.capped && !reference.diagnostics.capped,
            "program {program} capped: {rules:#?}"
        );
        assert_eq!(
            fact_set(&scheduled),
            fact_set(&reference),
            "program {program} derived different facts:\n{rules:#?}"
        );
    }
}

fn random_rule(rng: &mut StdRng, predicates: usize, classes: usize) -> String {
    let p = |rng: &mut StdRng| format!("ex:p{}", rng.gen_range(0..predicates));
    let class = |rng: &mut StdRng| format!("ex:C{}", rng.gen_range(0..classes));
    let body = match rng.gen_range(0..14) {
        0 => format!("?x {} ?y", p(rng)),
        1 => format!("?x {} ?m . ?m {} ?y", p(rng), p(rng)),
        2 => format!("?x {} ?y . ?y a {}", p(rng), class(rng)),
        3 => format!("?x {}+ ?y", p(rng)),
        4 => format!("?x {}* ?y . ?x a {}", p(rng), class(rng)),
        5 => format!("?x {}* ?y", p(rng)),
        6 => format!("?x {} ?m . ?m {} ?n . ?n {} ?y", p(rng), p(rng), p(rng)),
        7 => format!("?x ?q ?y . ?y a {}", class(rng)),
        8 => format!("?x {} ?x . ?x {} ?y", p(rng), p(rng)),
        9 => format!("ex:n0 {} ?x . ?x {} ?y", p(rng), p(rng)),
        10 => format!("{{ ?x {} ?y }} UNION {{ ?y {} ?x }}", p(rng), p(rng)),
        11 => format!("?x {} ?y . FILTER EXISTS {{ ?y {} ?w }}", p(rng), p(rng)),
        12 => format!("?x ({}/{})+ ?y", p(rng), p(rng)),
        _ => format!("?y {} ?x", p(rng)),
    };
    // A constant head object introduces a term no asserted triple mentions,
    // which only a zero-length path can observe through another predicate.
    let head = match rng.gen_range(0..8) {
        0 | 1 => format!("?y a {}", class(rng)),
        2 => format!("?x {} ex:fresh{}", p(rng), rng.gen_range(0..2)),
        _ => format!("?x {} ?y", p(rng)),
    };
    format!("CONSTRUCT {{ {head} }} WHERE {{ {body} }}")
}
