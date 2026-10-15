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
