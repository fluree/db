//! Integration tests for the OWL2-RL materialization budget and its
//! surfacing in query response metadata.
//!
//! The budget is a correctness control: a capped materialization is an
//! incomplete closure, so it must be (a) configurable per ledger
//! (`f:reasoningMaxFacts` / `f:reasoningMaxSeconds` in `f:reasoningDefaults`)
//! and per query (`"reasoningBudget"`), and (b) loud — visible in the
//! tracked response's `reasoning` block, not only in server logs.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::genesis_ledger;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

async fn apply_trig(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    trig: &str,
) -> fluree_db_api::LedgerState {
    fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("trig stage should succeed")
        .ledger
}

/// Seed a transitive-property chain that derives 3 facts under OWL2-RL:
/// `a partOf b partOf c partOf d` ⇒ `a-c`, `b-d`, `a-d`.
async fn seed_transitive_chain(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let ledger = genesis_ledger(fluree, ledger_id);
    let _ = apply_trig(
        fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:partOf rdf:type owl:TransitiveProperty .
        ex:a ex:partOf ex:b .
        ex:b ex:partOf ex:c .
        ex:c ex:partOf ex:d .
        ",
    )
    .await;
}

/// Add a `f:reasoningDefaults` budget block to the ledger's config graph.
async fn configure_budget(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    max_facts: u64,
    override_control: Option<&str>,
) {
    let config_iri = config_graph_iri(ledger_id);
    let override_line = override_control
        .map(|oc| format!("<urn:config:reasoning> f:overrideControl {oc} ."))
        .unwrap_or_default();
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningMaxFacts {max_facts} .
            {override_line}
        }}
        "
    );
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let _ = apply_trig(fluree, ledger, &trig).await;
}

fn reasoning_query(ledger_id: &str) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?x"],
        "where": {"@id": "?x", "ex:partOf": {"@id": "ex:d"}},
        "reasoning": "owl2rl"
    })
}

async fn run_tracked(
    fluree: &fluree_db_api::Fluree,
    query: &serde_json::Value,
) -> fluree_db_api::TrackedQueryResponse {
    let resp = fluree
        .query_from()
        .jsonld(query)
        .track_all()
        .execute_tracked()
        .await
        .expect("execute_tracked");
    assert_eq!(resp.status, 200, "query failed: {resp:?}");
    resp
}

// ============================================================================
// Uncapped baseline: full closure, reasoning block reports complete
// ============================================================================

#[tokio::test]
async fn uncapped_reasoning_reports_complete_in_tracking() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-uncapped:main";
    seed_transitive_chain(&fluree, ledger_id).await;

    let resp = run_tracked(&fluree, &reasoning_query(ledger_id)).await;

    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "full closure should not be capped: {reasoning:?}"
    );
    assert!(reasoning.capped_reason.is_none());
    assert_eq!(
        reasoning.derived_facts, 3,
        "a 4-node transitive chain derives exactly 3 facts"
    );

    // All of a, b, c reach d in the closure.
    let rows = resp.result;
    let n = rows.as_array().map(std::vec::Vec::len).unwrap_or(0);
    assert_eq!(n, 3, "expected a, b, c to reach d; got {rows:?}");
}

// ============================================================================
// Per-query budget caps the closure and the cap is visible to the client
// ============================================================================

#[tokio::test]
async fn query_budget_caps_materialization_and_surfaces_in_tracking() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-query:main";
    seed_transitive_chain(&fluree, ledger_id).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 1});

    let resp = run_tracked(&fluree, &query).await;

    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        reasoning.capped,
        "maxFacts=1 must cap a 3-fact closure: {reasoning:?}"
    );
    assert!(
        reasoning.capped_reason.is_some(),
        "capped result carries a reason"
    );
}

// ============================================================================
// Ledger-config budget applies even when the query brings its own modes
// ============================================================================

#[tokio::test]
async fn config_budget_applies_to_query_requested_reasoning() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-config:main";
    seed_transitive_chain(&fluree, ledger_id).await;
    // Budget only — no default modes. Queries opt into reasoning themselves
    // but run under the configured cap.
    configure_budget(&fluree, ledger_id, 1, None).await;

    let resp = run_tracked(&fluree, &reasoning_query(ledger_id)).await;

    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        reasoning.capped,
        "config f:reasoningMaxFacts=1 must cap the closure: {reasoning:?}"
    );
}

// ============================================================================
// Override control: a query-supplied budget loses to a forced config budget
// ============================================================================

#[tokio::test]
async fn forced_config_budget_discards_query_budget() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-forced:main";
    seed_transitive_chain(&fluree, ledger_id).await;
    configure_budget(&fluree, ledger_id, 1, Some("f:OverrideNone")).await;

    // The query tries to raise the budget past the closure size.
    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 1_000_000});

    let resp = run_tracked(&fluree, &query).await;

    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        reasoning.capped,
        "OverrideNone config budget must win over the query budget: {reasoning:?}"
    );
}

// ============================================================================
// Config-default modes + query-only budget: the budget survives the
// wrapper-mode replacement
// ============================================================================

#[tokio::test]
async fn query_budget_survives_config_default_modes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-mode-default:main";
    seed_transitive_chain(&fluree, ledger_id).await;

    // Config supplies default modes (owl2rl) but no budget.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:owl2rl .
        }}
        "
    );
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let _ = apply_trig(&fluree, ledger, &trig).await;

    // The query requests no modes (config default applies) but caps the budget.
    let mut query = reasoning_query(ledger_id);
    query.as_object_mut().unwrap().remove("reasoning");
    query["reasoningBudget"] = json!({"maxFacts": 1});

    let resp = run_tracked(&fluree, &query).await;

    let reasoning = resp
        .reasoning
        .expect("config-default reasoning reports a reasoning block");
    assert!(
        reasoning.capped,
        "query budget must survive config-default mode application: {reasoning:?}"
    );
}

// ============================================================================
// Permissive override control: the query budget wins over config
// ============================================================================

#[tokio::test]
async fn query_budget_overrides_permissive_config_budget() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-permissive:main";
    seed_transitive_chain(&fluree, ledger_id).await;
    // Default override control is AllowAll.
    configure_budget(&fluree, ledger_id, 1, None).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 1_000_000});

    let resp = run_tracked(&fluree, &query).await;

    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "AllowAll lets the query raise the budget: {reasoning:?}"
    );
    assert_eq!(reasoning.derived_facts, 3);
}
