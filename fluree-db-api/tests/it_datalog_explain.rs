//! Explanations of derived facts, over the recent-activity program: which
//! rule derived a fact, from which facts, which negations found nothing, and
//! which solutions a count counted.

use crate::it_datalog_program_assessment::{commit, ctx, load, Storage, Surface, T0};
use crate::support::{graphdb_from_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::{Explanation, GovernanceOptions};
use serde_json::{json, Value};

const EX: &str = "http://example.org/";

fn iri(local: &str) -> Value {
    json!({"@id": format!("{EX}{local}")})
}

async fn explain(fluree: &MemoryFluree, ledger: &MemoryLedger, node: Value) -> Explanation {
    fluree
        .explain_derived(
            &graphdb_from_ledger(ledger),
            &json!({
                "@context": ctx(),
                "reasoning": ["owl2rl", "datalog"],
                "reasoningTime": T0,
                "explain": node
            }),
        )
        .await
        .expect("explain")
}

/// A candidate is explained by the assessing rule's row: the count and the
/// threshold it read, and the exception it looked for and did not find.
#[tokio::test]
async fn a_candidate_is_explained_by_its_premises_and_the_exception_it_did_not_find() {
    for surface in [Surface::JsonLd, Surface::Sparql] {
        let ledger_id = format!("datalog/explain-candidate-{surface:?}:main").to_lowercase();
        let (fluree, ledger) = load(&ledger_id, surface, Storage::NoveltyOnly).await;
        let explanation = explain(
            &fluree,
            &ledger,
            json!({"@id": "ex:acct2", "@type": "ex:Candidate"}),
        )
        .await;
        assert!(explanation.complete);
        assert!(explanation
            .evaluation_time
            .as_deref()
            .is_some_and(|t| t.starts_with("2026-06-30T00:00:00")));
        assert_eq!(explanation.facts.len(), 1, "{surface:?}");
        let fact = &explanation.facts[0];
        assert_eq!(fact.subject, iri("acct2"));
        assert_eq!(fact.object, iri("Candidate"));
        assert!(!fact.asserted);
        assert_eq!(fact.derivations.len(), 1);

        let derivation = &fact.derivations[0];
        assert_eq!(derivation.rule, format!("{EX}assess"));
        assert_eq!(derivation.bindings["?a"], iri("acct2"));
        assert_eq!(derivation.bindings["?n"], json!(3));
        let premises: Vec<(Value, Value, bool)> = derivation
            .premises
            .iter()
            .map(|p| (p.predicate.clone(), p.object.clone(), p.derived))
            .collect();
        assert!(
            premises.contains(&(iri("recentCount"), json!(3), true)),
            "{premises:?}"
        );
        assert!(
            premises.contains(&(iri("minCount"), json!(3), false)),
            "{premises:?}"
        );
        assert_eq!(derivation.absences.len(), 1);
        assert_eq!(
            derivation.absences[0].patterns,
            vec![[iri("acct2"), iri("coveredBy"), json!("?x")]]
        );
        assert_eq!(derivation.absences[0].graph, None);
    }
}

/// A count is explained by its group: the transactions inside the window,
/// and not the one outside it.
#[tokio::test]
async fn a_count_is_explained_by_the_solutions_in_its_group() {
    let (fluree, ledger) = load(
        "datalog/explain-count:main",
        Surface::JsonLd,
        Storage::Indexed,
    )
    .await;
    let explanation = explain(
        &fluree,
        &ledger,
        json!({"@id": "ex:acct1", "ex:recentCount": "?n"}),
    )
    .await;
    assert_eq!(explanation.facts.len(), 1);
    let fact = &explanation.facts[0];
    assert_eq!(fact.object, json!(3));
    let derivation = &fact.derivations[0];
    assert_eq!(derivation.rule, format!("{EX}recentActivity"));
    assert_eq!(derivation.groups.len(), 1);
    let group = &derivation.groups[0];
    assert_eq!(group.key.get("?a"), Some(&iri("acct1")));
    let mut counted: Vec<Value> = group.inputs.iter().map(|row| row["?t"].clone()).collect();
    counted.sort_by_key(ToString::to_string);
    assert_eq!(counted, vec![iri("t1b"), iri("t1c"), iri("t1d")]);
}

/// Coverage is explained one step at a time down the ownership chain, and a
/// premise OWL entailed is marked derived like one a rule derived.
#[tokio::test]
async fn coverage_is_explained_step_by_step_down_the_ownership_chain() {
    let (fluree, ledger) = load(
        "datalog/explain-chain:main",
        Surface::JsonLd,
        Storage::NoveltyOnly,
    )
    .await;
    let ledger = commit(
        &fluree,
        ledger,
        json!({"insert": {"@id": "ex:parent", "ex:exception": {"@id": "ex:waiver1"}}}),
    )
    .await;

    let covered = explain(
        &fluree,
        &ledger,
        json!({"@id": "ex:acct1", "ex:coveredBy": {"@id": "ex:waiver1"}}),
    )
    .await;
    let derivation = &covered.facts[0].derivations[0];
    assert_eq!(derivation.rule, format!("{EX}coveredAbove"));
    let premises: Vec<(Value, Value, Value, bool)> = derivation
        .premises
        .iter()
        .map(|p| {
            (
                p.subject.clone(),
                p.predicate.clone(),
                p.object.clone(),
                p.derived,
            )
        })
        .collect();
    assert_eq!(
        premises,
        vec![
            (iri("acct1"), iri("holder"), iri("opco"), false),
            (iri("opco"), iri("within"), iri("parent"), true),
            (iri("parent"), iri("exception"), iri("waiver1"), false),
        ]
    );

    // The derived premise in turn: two steps up, through an OWL entailment.
    let within = explain(
        &fluree,
        &ledger,
        json!({"@id": "ex:opco", "ex:within": {"@id": "ex:parent"}}),
    )
    .await;
    let derivation = &within.facts[0].derivations[0];
    assert_eq!(derivation.rule, format!("{EX}withinChain"));
    let premises: Vec<(Value, Value, bool)> = derivation
        .premises
        .iter()
        .map(|p| (p.subject.clone(), p.object.clone(), p.derived))
        .collect();
    assert_eq!(
        premises,
        vec![
            (iri("opco"), iri("holdco"), true),
            (iri("holdco"), iri("parent"), true),
        ],
        "holdco ownedBy parent is an OWL entailment of subsidiaryOf"
    );
}

/// A fact the rules do not derive has nothing to explain.
#[tokio::test]
async fn an_underived_fact_has_no_explanation() {
    let (fluree, ledger) = load(
        "datalog/explain-none:main",
        Surface::JsonLd,
        Storage::NoveltyOnly,
    )
    .await;
    let explanation = explain(
        &fluree,
        &ledger,
        json!({"@id": "ex:acct4", "@type": "ex:Candidate"}),
    )
    .await;
    assert!(explanation.facts.is_empty());
}

/// An explanation shows premises a policy may hide, so a restricted view
/// cannot ask for one.
#[tokio::test]
async fn a_restricted_view_cannot_ask_for_an_explanation() {
    let (fluree, ledger) = load(
        "datalog/explain-policy:main",
        Surface::JsonLd,
        Storage::NoveltyOnly,
    )
    .await;
    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/hideExceptions",
            "https://ns.flur.ee/db#required": true,
            "https://ns.flur.ee/db#onProperty": [{"@id": format!("{EX}exception")}],
            "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
            "https://ns.flur.ee/db#allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    };
    let restricted = fluree
        .wrap_policy(graphdb_from_ledger(&ledger), &opts)
        .await
        .expect("wrap policy");
    let err = fluree
        .explain_derived(
            &restricted,
            &json!({
                "@context": ctx(),
                "reasoning": "datalog",
                "explain": {"@id": "ex:acct2", "@type": "ex:Candidate"}
            }),
        )
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("unrestricted"), "{err}");
}

async fn ledger_with(fluree: &MemoryFluree, ledger_id: &str, data: Value) -> MemoryLedger {
    let ledger = crate::support::genesis_ledger(fluree, ledger_id);
    fluree.insert(ledger, &data).await.expect("seed").ledger
}

async fn explain_with(fluree: &MemoryFluree, ledger: &MemoryLedger, request: Value) -> Explanation {
    fluree
        .explain_derived(&graphdb_from_ledger(ledger), &request)
        .await
        .expect("explain")
}

fn ex_ctx() -> Value {
    json!({"ex": EX})
}

/// A capped run explains only what it reached: the stage it derived, not
/// the conclusion a replay of the next rule would produce.
#[tokio::test]
async fn a_conclusion_the_run_did_not_reach_is_not_explained() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "datalog/explain-capped:main",
        json!({"@context": ex_ctx(), "@id": "ex:x", "@type": "ex:Seed"}),
    )
    .await;
    let request = |explain: Value| {
        json!({
            "@context": ex_ctx(),
            "reasoning": "datalog",
            "reasoningBudget": {"maxFacts": 1},
            "rules": [
                {"@context": ex_ctx(), "@id": "ex:stage",
                 "where": {"@id": "?x", "@type": "ex:Seed"},
                 "insert": {"@id": "?x", "@type": "ex:Stage"}},
                {"@context": ex_ctx(), "@id": "ex:conclude",
                 "where": {"@id": "?x", "@type": "ex:Stage"},
                 "insert": {"@id": "?x", "@type": "ex:Conclusion"}}
            ],
            "explain": explain
        })
    };

    let conclusion = explain_with(
        &fluree,
        &ledger,
        request(json!({"@id": "ex:x", "@type": "ex:Conclusion"})),
    )
    .await;
    assert!(!conclusion.complete);
    assert!(conclusion.facts.is_empty(), "{:?}", conclusion.facts);

    let stage = explain_with(
        &fluree,
        &ledger,
        request(json!({"@id": "ex:x", "@type": "ex:Stage"})),
    )
    .await;
    assert_eq!(stage.facts.len(), 1);
    assert_eq!(stage.facts[0].derivations.len(), 1);
}

/// A rule whose body mints a new value on every run cannot be replayed to
/// the row that derived a fact; the fact is listed and the rule named.
#[tokio::test]
async fn a_rule_that_cannot_be_replayed_is_named() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "datalog/explain-volatile:main",
        json!({"@context": ex_ctx(), "@id": "ex:x", "@type": "ex:Seed"}),
    )
    .await;
    let explanation = explain_with(
        &fluree,
        &ledger,
        json!({
            "@context": ex_ctx(),
            "reasoning": "datalog",
            "rules": [{
                "@context": ex_ctx(), "@id": "ex:mint",
                "where": [{"@id": "?x", "@type": "ex:Seed"}, ["bind", "?u", "(struuid)"]],
                "insert": {"@id": "?x", "ex:token": "?u"}
            }],
            "explain": {"@id": "ex:x", "ex:token": "?u"}
        }),
    )
    .await;
    assert_eq!(explanation.facts.len(), 1);
    assert!(explanation.facts[0].derivations.is_empty());
    assert_eq!(explanation.not_replayed, vec!["ex:mint".to_string()]);
}

/// A count taken inside a named graph is explained by the solutions in that
/// graph, not by matches in the default graph.
#[tokio::test]
async fn an_aggregate_in_a_named_graph_is_explained_from_that_graph() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .stage_owned(crate::support::genesis_ledger(
            &fluree,
            "datalog/explain-graph-count:main",
        ))
        .upsert_turtle(
            "@prefix ex: <http://example.org/> .\n\
             ex:i3 a ex:Item .\n\
             GRAPH ex:g { ex:i1 a ex:Item . ex:i2 a ex:Item . }\n",
        )
        .execute()
        .await
        .expect("seed")
        .ledger;
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": format!("{EX}countItems"),
                "f:rule": {"@type": "f:sparql", "@value":
                    "PREFIX ex: <http://example.org/> \
                     CONSTRUCT { ex:box ex:count ?n } WHERE { GRAPH ex:g { \
                     { SELECT (COUNT(?i) AS ?n) WHERE { ?i a ex:Item } } } }"}
            }),
        )
        .await
        .expect("rule")
        .ledger;
    let explanation = explain_with(
        &fluree,
        &ledger,
        json!({
            "@context": ex_ctx(),
            "reasoning": "datalog",
            "explain": {"@id": "ex:box", "ex:count": "?n"}
        }),
    )
    .await;
    assert_eq!(explanation.facts.len(), 1);
    assert_eq!(explanation.facts[0].object, json!(2));
    let group = &explanation.facts[0].derivations[0].groups[0];
    assert_eq!(group.graph.as_deref(), Some(format!("{EX}g").as_str()));
    let mut items: Vec<Value> = group.inputs.iter().map(|row| row["?i"].clone()).collect();
    items.sort_by_key(ToString::to_string);
    assert_eq!(items, vec![iri("i1"), iri("i2")]);
}

/// Facts that differ only in language tag are distinct facts, each with its
/// own derivation, and an asserted twin in another language does not make
/// one asserted.
#[tokio::test]
async fn language_tagged_facts_stay_distinct() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = ledger_with(
        &fluree,
        "datalog/explain-lang:main",
        json!({
            "@context": ex_ctx(),
            "@id": "ex:x",
            "ex:labelFr": {"@value": "chat", "@language": "fr"},
            "ex:labelEn": {"@value": "chat", "@language": "en"},
            "ex:label": {"@value": "chat", "@language": "en"}
        }),
    )
    .await;
    let explanation = explain_with(
        &fluree,
        &ledger,
        json!({
            "@context": ex_ctx(),
            "reasoning": "datalog",
            "rules": [
                {"@context": ex_ctx(), "@id": "ex:fromFr",
                 "where": {"@id": "?x", "ex:labelFr": "?l"},
                 "insert": {"@id": "?x", "ex:label": "?l"}},
                {"@context": ex_ctx(), "@id": "ex:fromEn",
                 "where": {"@id": "?x", "ex:labelEn": "?l"},
                 "insert": {"@id": "?x", "ex:label": "?l"}}
            ],
            "explain": {"@id": "ex:x", "ex:label": "?l"}
        }),
    )
    .await;
    let mut facts: Vec<(Value, bool, Vec<String>, bool)> = explanation
        .facts
        .iter()
        .map(|f| {
            (
                f.object.clone(),
                f.asserted,
                f.derivations.iter().map(|d| d.rule.clone()).collect(),
                f.derivations[0].premises[0].derived,
            )
        })
        .collect();
    facts.sort_by_key(|f| f.0.to_string());
    assert_eq!(
        facts,
        vec![
            (
                json!({"@value": "chat", "@language": "en"}),
                true,
                vec!["ex:fromEn".to_string()],
                false
            ),
            (
                json!({"@value": "chat", "@language": "fr"}),
                false,
                vec!["ex:fromFr".to_string()],
                false
            ),
        ]
    );
}

/// A 400-node chain whose closure materializes as one traversal (tens of
/// thousands of facts), while replaying the closing rule's body joins the
/// whole closure with itself (millions of rows).
async fn chain_closure_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let graph: Vec<Value> = (0..399)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:next": {"@id": format!("ex:n{}", i + 1)}}))
        .collect();
    ledger_with(
        fluree,
        ledger_id,
        json!({"@context": ex_ctx(), "@graph": graph}),
    )
    .await
}

fn chain_closure_rules() -> Value {
    json!([
        {"@context": ex_ctx(), "@id": "ex:step",
         "where": {"@id": "?x", "ex:next": "?y"},
         "insert": {"@id": "?x", "ex:reach": {"@id": "?y"}}},
        {"@context": ex_ctx(), "@id": "ex:close",
         "where": [{"@id": "?x", "ex:reach": "?y"}, {"@id": "?y", "ex:reach": "?z"}],
         "insert": {"@id": "?x", "ex:reach": {"@id": "?z"}}}
    ])
}

fn chain_closure_request(max_seconds: u64) -> Value {
    json!({
        "@context": ex_ctx(),
        "reasoning": "datalog",
        "reasoningBudget": {"maxSeconds": max_seconds},
        "rules": chain_closure_rules(),
        "explain": {"@id": "ex:n0", "ex:reach": {"@id": "ex:n399"}}
    })
}

/// Replays run under the reasoning budget as the run's rule bodies do: one
/// that outlasts it stops, and the explanation says it is incomplete.
#[tokio::test]
async fn a_replay_stops_at_the_reasoning_deadline() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = chain_closure_ledger(&fluree, "datalog/explain-deadline:main").await;
    let started = std::time::Instant::now();
    let explanation = explain_with(&fluree, &ledger, chain_closure_request(2)).await;
    let elapsed = started.elapsed();
    assert_eq!(explanation.facts.len(), 1, "the run derives the fact");
    assert!(!explanation.complete, "the replay was stopped");
    assert!(
        elapsed < std::time::Duration::from_secs(8),
        "a 2s budget, the explanation ran {elapsed:?}"
    );
}

/// A host's cancellation reaches the replays: an explanation cancelled while
/// it replays fails with the cancellation rather than finishing.
#[tokio::test]
async fn a_cancelled_explanation_stops_its_replays() {
    use fluree_db_api::{QueryCancellation, QueryCancellationReason, QueryExecutionOptions};

    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = chain_closure_ledger(&fluree, "datalog/explain-cancel:main").await;
    // Materialize first, so the explanation reads the cached run and the
    // cancellation lands while it replays.
    crate::support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": ex_ctx(),
            "select": "?y",
            "where": {"@id": "ex:n0", "ex:reach": "?y"},
            "reasoning": "datalog",
            "reasoningBudget": {"maxSeconds": 60},
            "rules": chain_closure_rules()
        }),
    )
    .await
    .expect("materialize");

    let handle = QueryCancellation::new();
    let options = QueryExecutionOptions::new().with_cancellation(handle.clone());
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(300));
        handle.cancel_with(QueryCancellationReason::ClientDisconnected);
    });
    let started = std::time::Instant::now();
    let err = fluree
        .explain_derived_with_options(
            &graphdb_from_ledger(&ledger),
            &chain_closure_request(60),
            &options,
            None,
        )
        .await
        .expect_err("the explanation was cancelled");
    let elapsed = started.elapsed();
    assert!(err.to_string().contains("client_disconnected"), "{err}");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "cancelled after 300ms, the explanation ran {elapsed:?}"
    );
}

/// The ledger's configured default policy counts as a restriction: a view
/// from `fluree.db` carries no policy wrapper of its own, yet an explanation
/// on it is refused as on a wrapped one.
#[tokio::test]
async fn a_ledger_default_policy_refuses_an_explanation() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger_id = "datalog/explain-config-policy:main";
    let ledger = ledger_with(
        &fluree,
        ledger_id,
        json!({"@context": ex_ctx(), "@id": "ex:acct1", "ex:risk": "high"}),
    )
    .await;
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <urn:fluree:{ledger_id}#config> {{
                <urn:config:main> rdf:type f:LedgerConfig .
                <urn:config:main> f:policyDefaults <urn:config:policy> .
                <urn:config:policy> f:defaultAllow false .
            }}
            "
        ))
        .execute()
        .await
        .expect("config");
    let view = fluree.db(ledger_id).await.expect("view");
    let err = fluree
        .explain_derived(
            &view,
            &json!({
                "@context": ex_ctx(),
                "reasoning": "datalog",
                "rules": [{"@context": ex_ctx(),
                           "where": {"@id": "?a", "ex:risk": "high"},
                           "insert": {"@id": "?a", "@type": "ex:Flagged"}}],
                "explain": {"@id": "ex:acct1", "@type": "ex:Flagged"}
            }),
        )
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("unrestricted"), "{err}");
}
