//! What a datalog run promises about time, budgets and incomplete results.
//!
//! - Every rule body in a run evaluates `NOW()` and `TODAY()` at one
//!   evaluation time: the commit time of the queried snapshot, or the time the
//!   query requests. A snapshot without a commit time needs a requested one.
//! - The time budget interrupts a rule body that outlasts it.
//! - A capped run names the rules it cut short and the rules it never ran.

use crate::it_datalog_rule_schedule::{chain_data, ledger_with, sparql_rules};
use crate::support::{genesis_ledger, normalize_rows, query_jsonld, query_sparql, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::temporal::DateTime;
use fluree_db_core::{QueryCancellation, QueryCancellationReason};
use fluree_db_query::datalog_rules::{
    run_fixpoint_with_schedule, DatalogExecutionResult, DatalogRuleSet, RuleExecutionEnv,
    RuleSchedule,
};
use fluree_db_query::execute::QueryRequest;
use fluree_db_query::reasoning::ReasoningBudget;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn fixpoint(
    ledger: &MemoryLedger,
    rules: &DatalogRuleSet,
    max_iterations: usize,
    budget: &ReasoningBudget,
    evaluation_time: Option<&DateTime>,
) -> fluree_db_query::Result<DatalogExecutionResult> {
    run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        rules,
        max_iterations,
        budget,
        RuleExecutionEnv {
            evaluation_time,
            ..RuleExecutionEnv::default()
        },
        RuleSchedule::Dependencies,
    )
    .await
}

/// `ex:stampedAt` objects, as their canonical lexical forms.
fn stamps(result: &DatalogExecutionResult) -> Vec<String> {
    result
        .derived_flakes
        .iter()
        .map(|f| match &f.o {
            fluree_db_core::value::FlakeValue::DateTime(dt) => dt.canonical(),
            other => panic!("expected a dateTime stamp, got {other:?}"),
        })
        .collect()
}

/// A recursive rule that calls `NOW()` derives a fresh stamp for every match
/// it re-runs. With one evaluation time per run the stamps repeat, so the
/// recursion converges with exactly one stamp per node.
#[tokio::test]
async fn now_is_one_evaluation_time_for_the_whole_run() {
    const LEN: usize = 6;
    let ledger = ledger_with("datalog/contract-now:main", chain_data(LEN)).await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:stampedAt ?t } WHERE { ?x a ex:Root BIND(NOW() AS ?t) }".to_string(),
            "CONSTRUCT { ?x ex:stampedAt ?t } WHERE { ?x ex:parent ?y . ?y ex:stampedAt ?s \
             BIND(NOW() AS ?t) }"
                .to_string(),
        ],
    );
    let requested = DateTime::parse("2026-03-01T12:00:00Z").unwrap();
    let result = fixpoint(
        &ledger,
        &rules,
        100,
        &ReasoningBudget::unlimited(),
        Some(&requested),
    )
    .await
    .expect("fixpoint");
    assert!(!result.diagnostics.capped);
    assert_eq!(stamps(&result), vec![requested.canonical(); LEN]);
    assert_eq!(
        result.diagnostics.evaluation_time.as_deref(),
        Some(requested.canonical().as_str())
    );
}

/// Without a requested time, the run evaluates at the commit time of the
/// snapshot it reads: inside the commit's window, and unchanged when the run
/// is repeated later.
#[tokio::test]
async fn the_default_evaluation_time_is_the_commit_time() {
    let before = chrono::Utc::now();
    let ledger = ledger_with("datalog/contract-commit-time:main", chain_data(3)).await;
    let after = chrono::Utc::now();
    let rules = sparql_rules(
        &ledger,
        &["CONSTRUCT { ?x ex:stampedAt ?t } WHERE { ?x a ex:Root BIND(NOW() AS ?t) }".to_string()],
    );

    let first = fixpoint(&ledger, &rules, 100, &ReasoningBudget::unlimited(), None)
        .await
        .expect("fixpoint");
    let time = first.diagnostics.evaluation_time.clone().expect("a clock");
    let instant = DateTime::parse(&time).unwrap().instant();
    // Commit times carry millisecond precision.
    assert!(
        instant >= before - chrono::Duration::milliseconds(1) && instant <= after,
        "{time} is outside the commit window {before}..{after}"
    );
    assert_eq!(stamps(&first), vec![time.clone()]);

    std::thread::sleep(Duration::from_millis(20));
    let again = fixpoint(&ledger, &rules, 100, &ReasoningBudget::unlimited(), None)
        .await
        .expect("fixpoint");
    assert_eq!(again.diagnostics.evaluation_time, Some(time));
}

/// An empty ledger has no commit to take a time from, so a program that reads
/// the clock needs a requested time instead of silently using the wall clock.
#[tokio::test]
async fn a_snapshot_without_a_commit_time_needs_a_requested_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "datalog/contract-genesis:main").await;
    let rules = sparql_rules(
        &ledger,
        &["CONSTRUCT { ?x ex:stampedAt ?t } WHERE { ?x a ex:Root BIND(NOW() AS ?t) }".to_string()],
    );
    let err = fixpoint(&ledger, &rules, 100, &ReasoningBudget::unlimited(), None)
        .await
        .expect_err("no clock to evaluate at");
    assert!(err.to_string().contains("reasoningTime"), "{err}");

    let requested = DateTime::parse("2026-03-01T12:00:00Z").unwrap();
    fixpoint(
        &ledger,
        &rules,
        100,
        &ReasoningBudget::unlimited(),
        Some(&requested),
    )
    .await
    .expect("a requested time is enough");
}

async fn overdue_ledger(ledger_id: &str) -> (fluree_db_api::Fluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id).await;
    let rule = json!({
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?x", "ex:due": "?d"},
            ["filter", "(< ?d (now))"]
        ],
        "insert": {"@id": "?x", "@type": "ex:Overdue"}
    });
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id": "ex:overdueRule", "f:rule": {"@type": "@json", "@value": rule}},
            {"@id": "ex:early", "ex:due": {"@value": "2026-01-01T00:00:00Z", "@type": "xsd:dateTime"}},
            {"@id": "ex:late", "ex:due": {"@value": "2027-01-01T00:00:00Z", "@type": "xsd:dateTime"}}
        ]
    });
    let ledger = fluree.insert(ledger, &data).await.expect("seed").ledger;
    (fluree, ledger)
}

fn sorted(rows: &Value) -> Vec<Value> {
    let mut rows = normalize_rows(rows);
    rows.sort_by_key(ToString::to_string);
    rows
}

/// The requested time decides what `NOW()` returns, and two requested times
/// are cached apart: answering the second from the first's materialization
/// would serve the wrong clock.
#[tokio::test]
async fn a_requested_time_decides_the_answer_and_is_cached_apart() {
    let (fluree, ledger) = overdue_ledger("datalog/contract-requested-time:main").await;
    let overdue_at = |time: &str| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?x",
            "where": {"@id": "?x", "@type": "ex:Overdue"},
            "reasoning": "datalog",
            "reasoningTime": time
        })
    };
    let ask = |query: Value| {
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            let rows = query_jsonld(fluree, ledger, &query)
                .await
                .expect("query")
                .to_jsonld(&ledger.snapshot)
                .expect("format");
            sorted(&rows)
        }
    };
    assert_eq!(
        ask(overdue_at("2026-06-01T00:00:00Z")).await,
        vec![json!("ex:early")]
    );
    assert_eq!(
        ask(overdue_at("2027-06-01T00:00:00Z")).await,
        vec![json!("ex:early"), json!("ex:late")]
    );
    assert_eq!(
        ask(overdue_at("2026-06-01T00:00:00Z")).await,
        vec![json!("ex:early")]
    );
}

/// The SPARQL twin: `# PRAGMA reasoning-time:` sets the same clock.
#[tokio::test]
async fn the_reasoning_time_pragma_sets_the_clock() {
    let (fluree, ledger) = overdue_ledger("datalog/contract-pragma-time:main").await;
    let at = |time: &str| {
        format!(
            "# PRAGMA reasoning: datalog\n# PRAGMA reasoning-time: {time}\n\
             PREFIX ex: <http://example.org/>\nSELECT ?x WHERE {{ ?x a ex:Overdue }}"
        )
    };
    let ask = |sparql: String| {
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            let rows = query_sparql(fluree, ledger, &sparql)
                .await
                .expect("query")
                .to_jsonld(&ledger.snapshot)
                .expect("format");
            sorted(&rows)
        }
    };
    assert_eq!(ask(at("2026-06-01T00:00:00Z")).await.len(), 1);
    assert_eq!(ask(at("2027-06-01T00:00:00Z")).await.len(), 2);

    let err = query_sparql(&fluree, &ledger, &at("not-a-time"))
        .await
        .expect_err("an unparseable time is rejected");
    assert!(err.to_string().contains("reasoning-time"), "{err}");
}

/// A body that runs far longer than the budget stops at its next checkpoint
/// once the deadline passes, instead of running to completion first. Its
/// FILTER rejects every pair, so the body is all work and no rows.
#[tokio::test]
async fn the_deadline_interrupts_a_running_rule_body() {
    const NODES: usize = 3_000;
    let graph: Vec<Value> = (0..NODES)
        .map(|i| json!({"@id": format!("ex:a{i}"), "ex:kind": {"@id": "ex:k"}}))
        .collect();
    let ledger = ledger_with(
        "datalog/contract-deadline:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:sameKind ?y } WHERE { ?x ex:kind ?k . ?y ex:kind ?k \
           FILTER(STRLEN(STR(?x)) + STRLEN(STR(?y)) < 0) }"
                .to_string(),
        ],
    );
    let budget = ReasoningBudget::new(Duration::from_millis(20), usize::MAX, usize::MAX);

    let started = Instant::now();
    let result = fixpoint(&ledger, &rules, 100, &budget, None)
        .await
        .expect("fixpoint");
    let elapsed = started.elapsed();
    assert_eq!(result.diagnostics.capped_reason.as_deref(), Some("time"));
    assert!(
        elapsed < Duration::from_secs(2),
        "a 20ms budget ran {elapsed:?}: the {}-pair body was not interrupted",
        NODES * NODES
    );
}

/// A request cancelled while a body runs ends the run with the cancellation,
/// not a capped result: the deadline caps a run, the request's cancellation
/// ends it.
#[tokio::test]
async fn a_cancelled_request_ends_the_run() {
    const NODES: usize = 3_000;
    let graph: Vec<Value> = (0..NODES)
        .map(|i| json!({"@id": format!("ex:a{i}"), "ex:kind": {"@id": "ex:k"}}))
        .collect();
    let ledger = ledger_with(
        "datalog/contract-cancelled:main",
        json!({"@context": {"ex": "http://example.org/"}, "@graph": graph}),
    )
    .await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:sameKind ?y } WHERE { ?x ex:kind ?k . ?y ex:kind ?k \
           FILTER(STRLEN(STR(?x)) + STRLEN(STR(?y)) < 0) }"
                .to_string(),
        ],
    );
    let handle = QueryCancellation::new();
    let canceller = handle.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        canceller.cancel_with(QueryCancellationReason::ClientDisconnected);
    });
    let budget = ReasoningBudget::new(Duration::from_secs(20), usize::MAX, usize::MAX);

    let result = run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        100,
        &budget,
        RuleExecutionEnv {
            request: QueryRequest {
                cancellation: Some(&handle),
                tracker: None,
            },
            ..RuleExecutionEnv::default()
        },
        RuleSchedule::Dependencies,
    )
    .await;
    match result {
        Err(fluree_db_query::QueryError::Cancelled {
            reason: QueryCancellationReason::ClientDisconnected,
        }) => {}
        other => panic!(
            "expected the cancellation, got {:?}",
            other.map(|r| r.diagnostics)
        ),
    }
}

/// The iteration cap fires inside the recursion. The recursive rule is named
/// as cut short and the rule downstream of it as never run; the base rule
/// finished and is named nowhere.
#[tokio::test]
async fn a_capped_run_names_the_rules_it_cut_short_and_those_it_never_ran() {
    let ledger = ledger_with("datalog/contract-attribution:main", chain_data(10)).await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:ancestor ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { ?x ex:ancestor ?z } WHERE { ?x ex:parent ?y . ?y ex:ancestor ?z \
             FILTER(?x != ?z) }"
                .to_string(),
            "CONSTRUCT { ?x ex:reachesRoot true } WHERE { ?x ex:ancestor ?r . ?r a ex:Root }"
                .to_string(),
        ],
    );
    let result = fixpoint(&ledger, &rules, 2, &ReasoningBudget::unlimited(), None)
        .await
        .expect("fixpoint");
    let diagnostics = &result.diagnostics;
    assert_eq!(diagnostics.capped_reason.as_deref(), Some("iterations"));
    let name = |i: usize| format!("ex:rule{i:02}");
    assert_eq!(diagnostics.capped_rules, vec![name(1)]);
    assert_eq!(diagnostics.pending_rules, vec![name(2)]);
}

/// The same attribution reaches a tracked query's `reasoning` block.
#[tokio::test]
async fn a_tracked_query_reports_which_rules_did_not_finish() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "datalog/contract-tracked:main";
    let ledger = genesis_ledger(&fluree, ledger_id).await;
    fluree.insert(ledger, &chain_data(10)).await.expect("seed");
    let rule = |id: &str, where_: Value, insert: Value| json!({"@id": id, "@context": {"ex": "http://example.org/"}, "where": where_, "insert": insert});
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": "?x",
        "where": {"@id": "?x", "ex:reachesRoot": true},
        "reasoning": "datalog",
        "reasoningBudget": {"maxFacts": 5},
        "rules": [
            rule("ex:base", json!({"@id": "?x", "ex:parent": "?y"}),
                 json!({"@id": "?x", "ex:ancestor": {"@id": "?y"}})),
            rule("ex:downstream",
                 json!([{"@id": "?x", "ex:ancestor": "?r"}, {"@id": "?r", "@type": "ex:Root"}]),
                 json!({"@id": "?x", "ex:reachesRoot": true}))
        ]
    });
    let resp = fluree
        .query_from()
        .jsonld(&query)
        .track_all()
        .execute_tracked()
        .await
        .expect("execute_tracked");
    let reasoning = resp.reasoning.expect("a reasoning block");
    assert_eq!(reasoning.capped_reason.as_deref(), Some("facts"));
    assert_eq!(reasoning.capped_rules, vec!["ex:base"]);
    assert_eq!(reasoning.pending_rules, vec!["ex:downstream"]);
}

/// A query of an earlier snapshot evaluates at that snapshot's commit time,
/// not the latest commit's, so asking about the past answers as of then.
#[tokio::test]
async fn a_historical_query_evaluates_at_its_own_commit_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "datalog/contract-historical-time:main";
    let ledger = genesis_ledger(&fluree, ledger_id).await;
    let before = chrono::Utc::now();
    let ledger = fluree
        .insert(ledger, &chain_data(3))
        .await
        .expect("seed")
        .ledger;
    let after = chrono::Utc::now();
    std::thread::sleep(Duration::from_millis(20));
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:later", "ex:p": 1}),
        )
        .await
        .expect("second commit");

    let stamped = |from: String| {
        let fluree = &fluree;
        async move {
            let query = json!({
                "@context": {"ex": "http://example.org/"},
                "from": from,
                "select": "?t",
                "where": {"@id": "?x", "ex:stampedAt": "?t"},
                "reasoning": "datalog",
                "rules": [{
                    "@context": {"ex": "http://example.org/"},
                    "where": [{"@id": "?x", "@type": "ex:Root"}, ["bind", "?t", "(now)"]],
                    "insert": {"@id": "?x", "ex:stampedAt": "?t"}
                }]
            });
            let resp = fluree
                .query_from()
                .jsonld(&query)
                .track_all()
                .execute_tracked()
                .await
                .expect("execute_tracked");
            let time = resp
                .reasoning
                .and_then(|r| r.evaluation_time)
                .expect("an evaluation time");
            (time, resp.result)
        }
    };
    let instant = |time: &str| DateTime::parse(time).unwrap().instant();

    let (historical, rows) = stamped(format!("{ledger_id}@t:1")).await;
    assert_eq!(
        rows,
        json!([{"@value": historical, "@type": "http://www.w3.org/2001/XMLSchema#dateTime"}]),
        "NOW() in the rule body is the evaluation time"
    );
    assert!(
        instant(&historical) >= before - chrono::Duration::milliseconds(1)
            && instant(&historical) <= after,
        "{historical} is outside the first commit's window {before}..{after}"
    );
    let (latest, _) = stamped(ledger_id.to_string()).await;
    assert!(
        instant(&latest) > after,
        "{latest} is not the second commit's time"
    );
}

/// A requested time at the top level beside a mode in `opts` runs the mode
/// at that time. Any top-level reasoning key used to make the top level the
/// only source, dropping the mode in silence (fluree/db#1883).
#[tokio::test]
async fn a_top_level_time_beside_an_opts_mode_runs_that_mode() {
    let (fluree, ledger) = overdue_ledger("datalog/contract-time-opts-mode:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Overdue"},
        "reasoningTime": "2026-06-01T00:00:00Z",
        "opts": {"reasoning": "datalog"}
    });
    let rows = query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    assert_eq!(sorted(&rows), vec![json!("ex:early")]);
}
