//! Semi-naive re-evaluation inside recursive groups.
//!
//! After its first pass, a recursive rule re-reads only what its group
//! derived in the previous pass, once per top-level triple pattern that reads
//! the group's facts. These tests pin that the work shrinks, that every
//! recursive pattern of a nonlinear rule is seeded, that a rule reading its
//! group's facts anywhere else re-runs its whole body, and that patterns the
//! seed cannot stand in for still match exactly. The whole-program schedule,
//! which always runs whole bodies, is the reference.
//!
//! A one-rule transitive closure runs as a property path instead (see
//! `it_datalog_closure_paths`); the recursive rules here carry a FILTER that
//! is always true on their data to keep them iterating.

use crate::it_datalog_rule_schedule::{chain_data, fact_set, ledger_with, run, sparql_rules};
use fluree_db_core::Sid;
use fluree_db_query::datalog_rules::{DatalogExecutionResult, RuleSchedule};
use serde_json::{json, Value};

fn graph(nodes: Vec<Value>) -> Value {
    json!({"@context": {"ex": "http://example.org/"}, "@graph": nodes})
}

fn edges(predicate: &str, pairs: &[(&str, &str)]) -> Vec<Value> {
    pairs
        .iter()
        .map(|(s, o)| json!({"@id": format!("ex:{s}"), predicate: {"@id": format!("ex:{o}")}}))
        .collect()
}

/// Derived facts with `predicate`, as `(subject, object)` local names.
pub(crate) fn derived(
    result: &DatalogExecutionResult,
    ledger: &crate::support::MemoryLedger,
    predicate: &str,
) -> Vec<(String, String)> {
    let p = ledger
        .snapshot
        .encode_iri(&format!("http://example.org/{predicate}"))
        .expect("predicate encodes");
    let local = |sid: &Sid| {
        let iri = ledger.snapshot.decode_sid(sid).expect("decodes");
        iri.trim_start_matches("http://example.org/").to_string()
    };
    let mut pairs: Vec<(String, String)> = result
        .derived_flakes
        .iter()
        .filter(|f| f.p == p)
        .map(|f| {
            let o = match &f.o {
                fluree_db_core::value::FlakeValue::Ref(sid) => local(sid),
                other => format!("{other:?}"),
            };
            (local(&f.s), o)
        })
        .collect();
    pairs.sort();
    pairs
}

pub(crate) async fn both_schedules(
    ledger: &crate::support::MemoryLedger,
    rules: &[String],
) -> (DatalogExecutionResult, DatalogExecutionResult) {
    let rule_set = sparql_rules(ledger, rules);
    let scheduled = run(ledger, &rule_set, RuleSchedule::Dependencies).await;
    let reference = run(ledger, &rule_set, RuleSchedule::WholeProgram).await;
    assert!(!scheduled.diagnostics.capped && !reference.diagnostics.capped);
    assert_eq!(
        fact_set(&scheduled),
        fact_set(&reference),
        "the schedules derived different facts for {rules:#?}"
    );
    (scheduled, reference)
}

#[tokio::test]
async fn a_linear_recursion_derives_each_fact_from_one_body_row() {
    const LEN: usize = 24;
    let ledger = ledger_with("datalog/semi-naive-linear:main", chain_data(LEN)).await;
    let (scheduled, reference) = both_schedules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:parent ?y . ?y ex:ancestor ?z \
             FILTER(?x != ?z) }"
                .to_string(),
        ],
    )
    .await;
    assert_eq!(scheduled.derived_flakes.len(), LEN * (LEN - 1) / 2);

    // On a chain each ancestor pair has exactly one derivation. Reading only
    // the previous pass finds each pair once; re-running the whole body
    // finds every known pair again on every pass.
    assert_eq!(scheduled.body_rows, scheduled.derived_flakes.len());
    assert!(
        reference.body_rows > 5 * scheduled.body_rows,
        "whole bodies returned {} rows, seeded bodies {}",
        reference.body_rows,
        scheduled.body_rows
    );
}

#[tokio::test]
async fn a_nonlinear_rule_is_seeded_at_each_recursive_pattern() {
    const LEN: usize = 24;
    let ledger = ledger_with("datalog/semi-naive-nonlinear:main", chain_data(LEN)).await;
    let (scheduled, _) = both_schedules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:ancestor ?y . ?y ex:ancestor ?z \
             FILTER(?x != ?z) }"
                .to_string(),
        ],
    )
    .await;
    assert_eq!(scheduled.derived_flakes.len(), LEN * (LEN - 1) / 2);

    // The base rule runs once. The recursive group's first pass runs the
    // whole body; each later pass runs it once with each pattern seeded.
    let group_passes = scheduled.diagnostics.iterations - 1;
    assert_eq!(scheduled.rule_body_runs, 1 + 1 + 2 * (group_passes - 1));
}

/// `ex:seen` is derived once, in the group's first pass, while `ex:reach`
/// keeps growing — but the third rule reads `ex:reach` only inside a nested
/// pattern. Seeding just its top-level `ex:seen` pattern would stop the
/// closure after one step; the rule has to re-run its whole body.
#[tokio::test]
async fn a_rule_reading_its_group_inside_a_nested_pattern_reruns_whole() {
    const LEN: usize = 10;
    let nested = [
        (
            "union",
            "?s ex:seen ex:yes . { ?s ex:reach ?y . ?y ex:parent ?z } UNION { ?s ex:parent ?z }",
        ),
        (
            "exists",
            "?s ex:seen ex:yes . ?y ex:parent ?z . FILTER EXISTS { ?s ex:reach ?y }",
        ),
        (
            "subquery",
            "?s ex:seen ex:yes . { SELECT ?s ?z WHERE { ?s ex:reach ?y . ?y ex:parent ?z } }",
        ),
        (
            "path",
            "?s ex:seen ex:yes . ?s ex:reach? ?y . ?y ex:parent ?z",
        ),
    ];
    for (shape, body) in nested {
        let ledger = ledger_with(
            &format!("datalog/semi-naive-nested-{shape}:main"),
            chain_data(LEN),
        )
        .await;
        let (scheduled, _) = both_schedules(
            &ledger,
            &[
                "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:parent ?y }".to_string(),
                "CONSTRUCT { ?x ex:seen ex:yes } WHERE { ?x ex:reach ?y }".to_string(),
                format!("CONSTRUCT {{ ?s ex:reach ?z }} WHERE {{ {body} }}"),
            ],
        )
        .await;
        assert_eq!(
            derived(&scheduled, &ledger, "reach").len(),
            LEN * (LEN - 1) / 2,
            "{shape}: the closure must be complete"
        );
    }
}

/// A literal in a recursive pattern is matched by the pattern, which stays
/// in the body behind its seed: a level-2 fact must not seed the level-1
/// rule.
#[tokio::test]
async fn a_literal_in_a_recursive_pattern_still_matches_exactly() {
    let mut nodes = edges(
        "ex:next",
        &[("a0", "a1"), ("a1", "a2"), ("b0", "b1"), ("b1", "b2")],
    );
    nodes.push(json!({"@id": "ex:a0", "ex:level": 1}));
    nodes.push(json!({"@id": "ex:b0", "ex:level": 2}));
    let ledger = ledger_with("datalog/semi-naive-literal:main", graph(nodes)).await;
    let (scheduled, _) = both_schedules(
        &ledger,
        &[
            "CONSTRUCT { ?y ex:level 1 } WHERE { ?x ex:level 1 . ?x ex:next ?y }".to_string(),
            "CONSTRUCT { ?y ex:level 2 } WHERE { ?x ex:level 2 . ?x ex:next ?y }".to_string(),
        ],
    )
    .await;
    let levels = derived(&scheduled, &ledger, "level");
    let expected: Vec<(String, String)> = [("a1", "1"), ("a2", "1"), ("b1", "2"), ("b2", "2")]
        .iter()
        .map(|(s, level)| (s.to_string(), format!("Long({level})")))
        .collect();
    assert_eq!(levels, expected);
}

/// `?x ex:reach ?x` matches only self-reaching facts. The seed replaces the
/// pattern, so it must bind `?x` only from a fact whose subject and object
/// agree: `e0`'s new reach facts must not mark its successor.
#[tokio::test]
async fn a_repeated_variable_in_a_recursive_pattern_binds_only_agreeing_facts() {
    let nodes = edges(
        "ex:next",
        &[
            ("c0", "c1"),
            ("c1", "c2"),
            ("c2", "c0"),
            ("c2", "d0"),
            ("d0", "d1"),
            ("e0", "e1"),
            ("e1", "e2"),
        ],
    );
    let ledger = ledger_with("datalog/semi-naive-repeated:main", graph(nodes)).await;
    let (scheduled, _) = both_schedules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:next ?y }".to_string(),
            "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:reach ?y . ?y ex:next ?z }".to_string(),
            "CONSTRUCT { ?y ex:reach ?y } WHERE { ?x ex:reach ?x . ?x ex:next ?y }".to_string(),
        ],
    )
    .await;
    let self_reaching: Vec<String> = derived(&scheduled, &ledger, "reach")
        .into_iter()
        .filter(|(s, o)| s == o)
        .map(|(s, _)| s)
        .collect();
    assert_eq!(self_reaching, ["c0", "c1", "c2", "d0", "d1"]);
}
