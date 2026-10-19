//! Stratified negation in rule bodies.
//!
//! A rule may use `NOT EXISTS` (JSON-LD `not-exists`, SPARQL
//! `FILTER NOT EXISTS`, or `!EXISTS`) over facts that are complete before it
//! runs: base data, OWL entailments, and the conclusions of rules it does not
//! depend on through the negation. Dependency order runs each negating rule
//! after everything it negates. A program that negates its own conclusions
//! through a cycle is rejected, naming the cycle. A run whose budget stops
//! before a negating rule finishes fails instead of answering partially.

use crate::it_datalog_rule_schedule::{fact_set, jsonld_rules, ledger_with, run, sparql_rules};
use crate::it_datalog_semi_naive::derived;
use crate::support::{normalize_rows, query_jsonld, query_sparql, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_query::datalog_rules::{
    run_fixpoint_with_schedule, DatalogRuleSet, RuleExecutionEnv, RuleSchedule,
};
use fluree_db_query::reasoning::ReasoningBudget;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

fn graph(nodes: Vec<Value>) -> Value {
    json!({"@context": {"ex": "http://example.org/"}, "@graph": nodes})
}

fn edge(s: &str, p: &str, o: &str) -> Value {
    json!({"@id": format!("ex:{s}"), format!("ex:{p}"): {"@id": format!("ex:{o}")}})
}

fn typed(s: &str, class: &str) -> Value {
    json!({"@id": format!("ex:{s}"), "@type": format!("ex:{class}")})
}

/// Subjects of derived `rdf:type ex:{class}` facts, as local names.
fn members(
    result: &fluree_db_query::datalog_rules::DatalogExecutionResult,
    ledger: &MemoryLedger,
    class: &str,
) -> BTreeSet<String> {
    let class = ledger
        .snapshot
        .encode_iri(&format!("http://example.org/{class}"))
        .expect("class encodes");
    result
        .derived_flakes
        .iter()
        .filter(|f| f.o == fluree_db_core::value::FlakeValue::Ref(class.clone()))
        .map(|f| {
            let iri = ledger.snapshot.decode_sid(&f.s).expect("decodes");
            iri.trim_start_matches("http://example.org/").to_string()
        })
        .collect()
}

async fn dependencies_run(
    ledger: &MemoryLedger,
    rules: &DatalogRuleSet,
    budget: &ReasoningBudget,
) -> fluree_db_query::Result<fluree_db_query::datalog_rules::DatalogExecutionResult> {
    run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        rules,
        100,
        budget,
        RuleExecutionEnv::default(),
        RuleSchedule::Dependencies,
    )
    .await
}

/// Reachability takes many passes to converge; the negation over it must
/// read the finished closure. A node reachable only at the end of the chain
/// is reachable, not "not yet reached".
#[tokio::test]
async fn negation_reads_a_recursive_relation_only_once_it_is_complete() {
    const CHAIN: usize = 12;
    let mut nodes: Vec<Value> = (0..CHAIN)
        .map(|i| edge(&format!("n{i}"), "edge", &format!("n{}", i + 1)))
        .collect();
    nodes.extend([edge("c0", "edge", "c1"), edge("c1", "edge", "c0")]);
    let all: Vec<String> = (0..=CHAIN)
        .map(|i| format!("n{i}"))
        .chain(["c0", "c1", "lone"].map(String::from))
        .collect();
    nodes.extend(all.iter().map(|n| typed(n, "Node")));
    let ledger = ledger_with("datalog/negation-closure:main", graph(nodes)).await;

    let rules = sparql_rules(
        &ledger,
        &[
            // Listed first, so order cannot be what makes it right.
            "CONSTRUCT { ?n a ex:Unreachable } WHERE { ?n a ex:Node \
             FILTER NOT EXISTS { ex:n0 ex:reach ?n } }"
                .to_string(),
            "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:edge ?y }".to_string(),
            // The FILTER keeps the recursion iterating instead of running as
            // one path.
            "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:reach ?y . ?y ex:edge ?z \
             FILTER(?x != ?z) }"
                .to_string(),
        ],
    );
    let result = dependencies_run(&ledger, &rules, &ReasoningBudget::unlimited())
        .await
        .expect("fixpoint");
    assert!(!result.diagnostics.capped);
    let expected: BTreeSet<String> = ["n0", "c0", "c1", "lone"].map(String::from).into();
    assert_eq!(members(&result, &ledger, "Unreachable"), expected);
}

async fn invoice_ledger(ledger_id: &str) -> (fluree_db_api::Fluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = crate::support::genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &graph(vec![
                typed("inv1", "Invoice"),
                typed("inv2", "Invoice"),
                typed("inv3", "Invoice"),
                edge("inv1", "paidBy", "pay1"),
                edge("inv3", "waivedBy", "mgr"),
            ]),
        )
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

fn sorted(rows: &Value) -> Vec<Value> {
    let mut rows = normalize_rows(rows);
    rows.sort_by_key(ToString::to_string);
    rows
}

/// The JSON-LD spelling, through a query: an invoice with neither a payment
/// nor a waiver is open.
#[tokio::test]
async fn json_ld_not_exists_derives_open_invoices() {
    let (fluree, ledger) = invoice_ledger("datalog/negation-jsonld:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?i",
        "where": {"@id": "?i", "@type": "ex:Open"},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?i", "@type": "ex:Invoice"},
                ["not-exists", {"@id": "?i", "ex:paidBy": "?p"}],
                ["not-exists", {"@id": "?i", "ex:waivedBy": "?w"}]
            ],
            "insert": {"@id": "?i", "@type": "ex:Open"}
        }]
    });
    let rows = query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    assert_eq!(sorted(&rows), vec![json!("ex:inv2")]);
}

/// The SPARQL twin, with the rule stored: `FILTER NOT EXISTS`.
#[tokio::test]
async fn sparql_filter_not_exists_derives_open_invoices() {
    let (fluree, ledger) = invoice_ledger("datalog/negation-sparql:main").await;
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/openRule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?i a ex:Open } WHERE { ?i a ex:Invoice \
                       FILTER NOT EXISTS { ?i ex:paidBy ?p } \
                       FILTER NOT EXISTS { ?i ex:waivedBy ?w } }"
        }
    });
    let ledger = fluree.insert(ledger, &rule).await.expect("rule").ledger;
    let rows = query_sparql(
        &fluree,
        &ledger,
        "# PRAGMA reasoning: datalog\nPREFIX ex: <http://example.org/>\n\
         SELECT ?i WHERE { ?i a ex:Open }",
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert_eq!(sorted(&rows), vec![json!(["ex:inv2"])]);
}

/// Three strata, written top stratum first: managers are derived, staff are
/// employees who are not managers, and a team with no staff is all managers.
#[tokio::test]
async fn negation_stacks_across_three_strata() {
    let ledger = ledger_with(
        "datalog/negation-strata:main",
        graph(vec![
            typed("ann", "Employee"),
            typed("bo", "Employee"),
            typed("cy", "Employee"),
            edge("ann", "manages", "bo"),
            edge("cy", "manages", "bo"),
            edge("red", "member", "ann"),
            edge("red", "member", "cy"),
            edge("blue", "member", "ann"),
            edge("blue", "member", "bo"),
        ]),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?t a ex:AllManagers } WHERE { ?t ex:member ?m \
             FILTER NOT EXISTS { ?t ex:member ?s . ?s a ex:Staff } }"
                .to_string(),
            "CONSTRUCT { ?x a ex:Staff } WHERE { ?x a ex:Employee \
             FILTER NOT EXISTS { ?x a ex:Manager } }"
                .to_string(),
            "CONSTRUCT { ?x a ex:Manager } WHERE { ?x ex:manages ?y }".to_string(),
        ],
    );
    let result = dependencies_run(&ledger, &rules, &ReasoningBudget::unlimited())
        .await
        .expect("fixpoint");
    let set = |names: &[&str]| {
        names
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(members(&result, &ledger, "Manager"), set(&["ann", "cy"]));
    assert_eq!(members(&result, &ledger, "Staff"), set(&["bo"]));
    assert_eq!(members(&result, &ledger, "AllManagers"), set(&["red"]));
}

/// A recursive rule may negate a lower stratum. Its NOT EXISTS reads nothing
/// the recursion writes, so the recursion still re-reads only new facts.
#[tokio::test]
async fn a_recursive_rule_may_negate_a_lower_stratum() {
    let edges = [
        ("a", "b"),
        ("b", "c"),
        ("c", "d"),
        ("b", "x"),
        ("x", "y"),
        ("d", "a"),
    ];
    let mut nodes: Vec<Value> = edges.iter().map(|(s, o)| edge(s, "edge", o)).collect();
    nodes.push(typed("x", "Blocked"));
    let ledger = ledger_with("datalog/negation-recursive:main", graph(nodes)).await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:safe ?y } WHERE { ?x ex:edge ?y \
             FILTER NOT EXISTS { ?y a ex:Blocked } }"
                .to_string(),
            "CONSTRUCT { ?x ex:safe ?z } WHERE { ?x ex:safe ?y . ?y ex:edge ?z \
             FILTER NOT EXISTS { ?z a ex:Blocked } }"
                .to_string(),
        ],
    );
    let result = dependencies_run(&ledger, &rules, &ReasoningBudget::unlimited())
        .await
        .expect("fixpoint");

    // Reachability over the edges whose target is not blocked.
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for (s, o) in edges.iter().filter(|(_, o)| *o != "x") {
        adjacency.entry(s).or_default().push(o);
    }
    let mut expected = BTreeSet::new();
    for &start in adjacency.keys() {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack = adjacency[start].clone();
        while let Some(n) = stack.pop() {
            if seen.insert(n) {
                expected.insert((start.to_string(), n.to_string()));
                stack.extend(adjacency.get(n).cloned().unwrap_or_default());
            }
        }
    }
    let actual: BTreeSet<(String, String)> =
        derived(&result, &ledger, "safe").into_iter().collect();
    assert_eq!(actual, expected);
}

/// Accepting a candidate negates its rejection, and a rejection is derived
/// from acceptances. No evaluation order is sound, so the program is
/// refused with the cycle spelled out.
#[tokio::test]
async fn negation_through_a_cycle_is_rejected_with_the_cycle() {
    let ledger = ledger_with(
        "datalog/negation-cycle:main",
        graph(vec![
            typed("a", "Candidate"),
            edge("a", "conflictsWith", "b"),
        ]),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x a ex:Accepted } WHERE { ?x a ex:Candidate \
             FILTER NOT EXISTS { ?x a ex:Rejected } }"
                .to_string(),
            "CONSTRUCT { ?x a ex:Rejected } WHERE { ?x a ex:Accepted . ?x ex:conflictsWith ?y }"
                .to_string(),
        ],
    );
    let err = dependencies_run(&ledger, &rules, &ReasoningBudget::unlimited())
        .await
        .expect_err("no stratification exists");
    let message = err.to_string();
    assert!(
        message.contains("ex:rule00 -not-> ex:rule01 -> ex:rule00"),
        "{message}"
    );
}

/// A negation over a read of every predicate (here a `*` path) observes the
/// rule's own head, so it is refused. The error says what read every
/// predicate and which facts close the cycle, rather than naming only the
/// rule's head as if the rule negated it.
#[tokio::test]
async fn negation_over_a_read_of_every_predicate_names_the_read() {
    let ledger = ledger_with(
        "datalog/negation-any-read:main",
        graph(vec![typed("n1", "Node"), edge("n0", "edge", "n1")]),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &["CONSTRUCT { ?n a ex:Flag } WHERE { ?n a ex:Node \
           FILTER NOT EXISTS { ex:n0 (ex:edge/ex:link)* ?n } }"
            .to_string()],
    );
    let err = dependencies_run(&ledger, &rules, &ReasoningBudget::unlimited())
        .await
        .expect_err("a negation over every predicate observes its own head");
    let message = err.to_string();
    assert!(
        message.contains(
            "negates facts of any predicate (a variable predicate, a `*` or `?` path, a \
             negated property set, or another read the analysis cannot narrow), among them \
             rdf:type http://example.org/Flag facts"
        ),
        "{message}"
    );
}

/// A variable a negation shares with the rest of the body must be bound by a
/// positive pattern; here `?y` is mentioned only inside two negations.
#[tokio::test]
async fn a_negated_variable_must_be_bound_positively() {
    let ledger = ledger_with("datalog/negation-unsafe:main", graph(vec![typed("a", "A")])).await;
    let rule = json!({
        "@id": "ex:unsafe",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?x a ex:Flagged } WHERE { ?x a ex:A \
                       FILTER NOT EXISTS { ?x ex:p ?y } FILTER NOT EXISTS { ?y ex:q ?z } }"
        }
    });
    let err = fluree_db_query::datalog_rules::parse_query_time_rule(&rule, &ledger.snapshot, 0)
        .expect_err("?y is never bound");
    assert!(err.to_string().contains("?y"), "{err}");
}

/// MINUS stays out of rule bodies; the error points at NOT EXISTS.
#[tokio::test]
async fn minus_points_at_not_exists() {
    let ledger = ledger_with("datalog/negation-minus:main", graph(vec![typed("a", "A")])).await;
    let rule = json!({
        "@id": "ex:minus",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?x a ex:B } WHERE { ?x a ex:A MINUS { ?x ex:p ?y } }"
        }
    });
    let err = fluree_db_query::datalog_rules::parse_query_time_rule(&rule, &ledger.snapshot, 0)
        .expect_err("MINUS is rejected");
    assert!(err.to_string().contains("NOT EXISTS"), "{err}");
}

/// When the budget runs out before a negating rule finishes, the run fails
/// instead of answering from partial conclusions. The same program without
/// the negating rule reports a capped result as before.
#[tokio::test]
async fn a_budget_that_stops_before_a_negating_rule_fails_the_run() {
    const LEN: usize = 12;
    let nodes: Vec<Value> = (0..LEN)
        .map(|i| edge(&format!("n{i}"), "edge", &format!("n{}", i + 1)))
        .collect();
    let ledger = ledger_with("datalog/negation-capped:main", graph(nodes)).await;
    let positive = [
        "CONSTRUCT { ?x ex:reach ?y } WHERE { ?x ex:edge ?y }".to_string(),
        "CONSTRUCT { ?x ex:reach ?z } WHERE { ?x ex:reach ?y . ?y ex:edge ?z FILTER(?x != ?z) }"
            .to_string(),
    ];
    let budget = ReasoningBudget::new(Duration::from_secs(60), 20, usize::MAX);

    let capped = dependencies_run(&ledger, &sparql_rules(&ledger, &positive), &budget)
        .await
        .expect("a positive program still answers capped");
    assert_eq!(capped.diagnostics.capped_reason.as_deref(), Some("facts"));

    let mut with_negation = positive.to_vec();
    with_negation.push(
        "CONSTRUCT { ?x a ex:Sink } WHERE { ?x ex:reach ?y FILTER NOT EXISTS { ?y ex:edge ?z } }"
            .to_string(),
    );
    let err = dependencies_run(&ledger, &sparql_rules(&ledger, &with_negation), &budget)
        .await
        .expect_err("the negating rule cannot run over a complete closure");
    let message = err.to_string();
    assert!(
        message.contains("ex:rule02") && message.contains("negation"),
        "{message}"
    );
}

/// OWL entailments are a finished input to datalog. When OWL 2 RL is capped,
/// a negating rule would read the missing entailments as absent, so the
/// query fails.
#[tokio::test]
async fn a_capped_owl_closure_fails_a_negating_program() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = crate::support::genesis_ledger(&fluree, "datalog/negation-owl:main");
    let mut nodes = vec![json!({
        "@id": "ex:partOf",
        "@type": "http://www.w3.org/2002/07/owl#TransitiveProperty"
    })];
    nodes.extend((0..8).map(|i| edge(&format!("p{i}"), "partOf", &format!("p{}", i + 1))));
    let ledger = fluree
        .insert(ledger, &graph(nodes))
        .await
        .expect("seed")
        .ledger;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Top"},
        "reasoning": ["owl2rl", "datalog"],
        "reasoningBudget": {"maxFacts": 3},
        "rules": [{
            "@id": "ex:topRule",
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?x", "ex:partOf": "?y"},
                ["not-exists", {"@id": "?z", "ex:partOf": {"@id": "?x"}}]
            ],
            "insert": {"@id": "?x", "@type": "ex:Top"}
        }]
    });
    let err = query_jsonld(&fluree, &ledger, &query)
        .await
        .expect_err("the negation would read an incomplete closure");
    let message = err.to_string();
    assert!(
        message.contains("OWL 2 RL") && message.contains("ex:topRule"),
        "{message}"
    );
}

/// The whole-program reference evaluates every rule from the first round,
/// before what a negation reads is complete, so it refuses negation.
#[tokio::test]
async fn the_whole_program_reference_refuses_negation() {
    let ledger = ledger_with(
        "datalog/negation-reference:main",
        graph(vec![typed("a", "A")]),
    )
    .await;
    let rules = jsonld_rules(
        &ledger,
        vec![json!({
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?x", "@type": "ex:A"},
                ["not-exists", {"@id": "?x", "@type": "ex:B"}]
            ],
            "insert": {"@id": "?x", "@type": "ex:C"}
        })],
    );
    let scheduled = run(&ledger, &rules, RuleSchedule::Dependencies).await;
    assert_eq!(fact_set(&scheduled).len(), 1);
    let err = run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        100,
        &ReasoningBudget::unlimited(),
        RuleExecutionEnv::default(),
        RuleSchedule::WholeProgram,
    )
    .await
    .expect_err("the reference cannot evaluate negation");
    assert!(err.to_string().contains("negation"), "{err}");
}

/// `ex:Manager rdfs:subClassOf ex:Employee`. Ann is stored as a manager; bob
/// becomes one through `ex:managerRule`. Both work at acme.
async fn staffing_ledger(ledger_id: &str) -> (fluree_db_api::Fluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = crate::support::genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &graph(vec![
                json!({
                    "@id": "ex:Manager",
                    "http://www.w3.org/2000/01/rdf-schema#subClassOf": {"@id": "ex:Employee"}
                }),
                typed("ann", "Manager"),
                edge("ann", "worksAt", "acme"),
                edge("bob", "manages", "team1"),
                edge("bob", "worksAt", "acme"),
            ]),
        )
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

fn manager_rule() -> Value {
    json!({
        "@id": "ex:managerRule",
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?x", "ex:manages": "?t"},
        "insert": {"@id": "?x", "@type": "ex:Manager"}
    })
}

/// Anyone working somewhere who is not an employee is a contractor.
fn contractor_rule() -> Value {
    json!({
        "@id": "ex:contractorRule",
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?x", "ex:worksAt": "?c"},
            ["not-exists", {"@id": "?x", "@type": "ex:Employee"}]
        ],
        "insert": {"@id": "?x", "@type": "ex:Contractor"}
    })
}

fn staffing_query(reasoning: Value, rules: Vec<Value>, classes: &[&str]) -> Value {
    let where_: Vec<Value> = classes
        .iter()
        .map(|class| json!({"@id": "?x", "@type": format!("ex:{class}")}))
        .collect();
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": where_,
        "reasoning": reasoning,
        "rules": rules
    })
}

fn assert_refused_under(message: &str, mode: &str) {
    assert!(
        message.contains("ex:contractorRule")
            && message.contains("negation")
            && message.contains(&format!("{mode} reasoning is on without owl2rl")),
        "{message}"
    );
}

/// `rdfs` and `owl2ql` widen only the query's patterns, so a rule body reads
/// ann, a stored manager, and bob, a derived one, as non-employees while the
/// query reads both as employees. A program that negates is refused rather
/// than answer that both are contractors because they are not employees. The
/// cache key leaves those modes out, so a cached datalog-only run of the same
/// rules must not answer for them.
#[tokio::test]
async fn negation_under_rdfs_or_owl2ql_without_owl2rl_is_refused() {
    let (fluree, ledger) = staffing_ledger("datalog/negation-rewrite-modes:main").await;
    let rules = vec![manager_rule(), contractor_rule()];
    let rows = query_jsonld(
        &fluree,
        &ledger,
        &staffing_query(json!("datalog"), rules.clone(), &["Contractor"]),
    )
    .await
    .expect("datalog alone runs")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert_eq!(sorted(&rows), vec![json!("ex:ann"), json!("ex:bob")]);

    for mode in ["rdfs", "owl2ql"] {
        let query = staffing_query(json!([mode, "datalog"]), rules.clone(), &["Contractor"]);
        let err = query_jsonld(&fluree, &ledger, &query)
            .await
            .expect_err("a negation under a query rewrite");
        assert_refused_under(&err.to_string(), mode);
    }
}

/// With OWL 2 RL the entailments are facts that rule bodies read too, and the
/// rewrite steps aside, so no one comes out both a contractor and an employee.
#[tokio::test]
async fn negation_under_rdfs_with_owl2rl_agrees_with_the_query() {
    let (fluree, ledger) = staffing_ledger("datalog/negation-rewrite-owl2rl:main").await;
    let rules = vec![manager_rule(), contractor_rule()];
    let reasoning = json!(["rdfs", "owl2rl", "datalog"]);
    let both = query_jsonld(
        &fluree,
        &ledger,
        &staffing_query(
            reasoning.clone(),
            rules.clone(),
            &["Contractor", "Employee"],
        ),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert_eq!(sorted(&both), Vec::<Value>::new());
    let contractors = query_jsonld(
        &fluree,
        &ledger,
        &staffing_query(reasoning, rules, &["Contractor"]),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    assert!(
        !sorted(&contractors).contains(&json!("ex:ann")),
        "{contractors}"
    );
}

/// A positive program under `rdfs` still runs: it can only derive less than
/// the query would, never contradict it.
#[tokio::test]
async fn a_positive_program_under_rdfs_still_runs() {
    let (fluree, ledger) = staffing_ledger("datalog/positive-rdfs:main").await;
    let query = staffing_query(
        json!(["rdfs", "datalog"]),
        vec![manager_rule()],
        &["Employee"],
    );
    let rows = query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    assert_eq!(sorted(&rows), vec![json!("ex:ann"), json!("ex:bob")]);
}

/// The SPARQL twin, with the rule stored.
#[tokio::test]
async fn sparql_negation_under_rdfs_without_owl2rl_is_refused() {
    let (fluree, ledger) = staffing_ledger("datalog/negation-rewrite-sparql:main").await;
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "http://example.org/contractorRule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?x a ex:Contractor } WHERE { ?x ex:worksAt ?c \
                       FILTER NOT EXISTS { ?x a ex:Employee } }"
        }
    });
    let ledger = fluree.insert(ledger, &rule).await.expect("rule").ledger;
    let err = query_sparql(
        &fluree,
        &ledger,
        "# PRAGMA reasoning: rdfs datalog\nPREFIX ex: <http://example.org/>\n\
         SELECT ?x WHERE { ?x a ex:Contractor }",
    )
    .await
    .expect_err("a negation under a query rewrite");
    let message = err.to_string();
    assert!(
        message.contains("http://example.org/contractorRule")
            && message.contains("rdfs reasoning is on without owl2rl"),
        "{message}"
    );
}

/// The ledger's reasoning defaults reach a query that names no modes, and
/// stored rules under a default `rdfs` are refused the same way.
#[tokio::test]
async fn negation_under_a_ledger_default_rdfs_is_refused() {
    let ledger_id = "datalog/negation-rewrite-default:main";
    let (fluree, ledger) = staffing_ledger(ledger_id).await;
    let rules = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@graph": [
            {
                "@id": "http://example.org/managerRule",
                "f:rule": {
                    "@type": "f:sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { ?x a ex:Manager } WHERE { ?x ex:manages ?t }"
                }
            },
            {
                "@id": "http://example.org/contractorRule",
                "f:rule": {
                    "@type": "f:sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { ?x a ex:Contractor } WHERE { ?x ex:worksAt ?c \
                               FILTER NOT EXISTS { ?x a ex:Employee } }"
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger, &rules).await.expect("rules").ledger;
    let config = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <urn:fluree:{ledger_id}#config> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs, f:datalog .
        }}
        "
    );
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&config)
        .execute()
        .await
        .expect("reasoning config")
        .ledger;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Contractor"}
    });
    let err = query_jsonld(&fluree, &ledger, &query)
        .await
        .expect_err("a negation under the ledger's default rdfs");
    let message = err.to_string();
    assert!(
        message.contains("http://example.org/contractorRule")
            && message.contains("rdfs reasoning is on without owl2rl"),
        "{message}"
    );
}

/// At the `t` of a commit that retracted the stored copy of a fact the rules
/// derive again, the derived fact still holds: a negation over it does not
/// match, and a count includes it. The asserted copy sits in novelty or in a
/// published index; the retraction is in novelty.
#[tokio::test]
async fn a_fact_retracted_at_the_query_t_is_still_negated_and_counted() {
    use crate::support::{instances_sharing_storage, rebuild_and_publish_index};

    let ctx = json!({"ex": "http://example.org/"});
    for indexed in [false, true] {
        let ledger_id = format!("datalog/negation-retracted-at-t-{indexed}:main");
        let (writer, reader) = instances_sharing_storage();
        writer
            .insert(
                crate::support::genesis_ledger(&writer, &ledger_id),
                &graph(vec![
                    typed("a", "Person"),
                    typed("c", "Person"),
                    edge("a", "friend", "b"),
                    edge("a", "knows", "b"),
                ]),
            )
            .await
            .expect("seed");
        if indexed {
            rebuild_and_publish_index(&writer, &ledger_id).await;
        }
        let ledger = reader.ledger(&ledger_id).await.expect("reader load");
        let ledger = reader
            .update(
                ledger,
                &json!({"@context": ctx, "delete": edge("a", "knows", "b")}),
            )
            .await
            .expect("retract")
            .ledger;

        let rules = json!([
            {
                "@context": ctx,
                "where": {"@id": "?x", "ex:friend": "?y"},
                "insert": {"@id": "?x", "ex:knows": {"@id": "?y"}}
            },
            {
                "@context": ctx,
                "where": [
                    {"@id": "?x", "@type": "ex:Person"},
                    ["not-exists", {"@id": "?x", "ex:knows": "?y"}]
                ],
                "insert": {"@id": "?x", "@type": "ex:Loner"}
            },
            {
                "@context": ctx,
                "where": [["query", {
                    "@context": ctx,
                    "select": ["?x", "(as (count ?y) ?n)"],
                    "where": {"@id": "?x", "ex:knows": "?y"},
                    "groupBy": ["?x"]
                }]],
                "insert": {"@id": "?x", "ex:knowsCount": "?n"}
            }
        ]);
        let rows = |select: Value, where_: Value| {
            let query = json!({
                "@context": ctx,
                "select": select,
                "where": where_,
                "reasoning": "datalog",
                "rules": rules.clone()
            });
            let (reader, ledger) = (&reader, &ledger);
            async move {
                let rows = query_jsonld(reader, ledger, &query)
                    .await
                    .expect("query")
                    .to_jsonld(&ledger.snapshot)
                    .expect("format");
                sorted(&rows)
            }
        };
        assert_eq!(
            rows(json!("?x"), json!({"@id": "?x", "@type": "ex:Loner"})).await,
            vec![json!("ex:c")],
            "indexed: {indexed}"
        );
        assert_eq!(
            rows(
                json!(["?x", "?n"]),
                json!({"@id": "?x", "ex:knowsCount": "?n"})
            )
            .await,
            vec![json!(["ex:a", 1])],
            "indexed: {indexed}"
        );
    }
}
