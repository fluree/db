//! Integration tests for the OWL2-RL materialization budget and its
//! surfacing in query response metadata.
//!
//! The budget is a correctness control: a capped materialization is an
//! incomplete closure, so it must be (a) configurable per ledger
//! (`f:reasoningMaxFacts` / `f:reasoningMaxSeconds` in `f:reasoningDefaults`)
//! and per query (`"reasoningBudget"`), and (b) loud — visible in the
//! tracked response's `reasoning` block, not only in server logs.

use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

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

// ============================================================================
// Datalog rules honour the same budget and surface `capped` the same way
// ============================================================================

/// Seed a few plain entities (no OWL) for datalog rules to scan.
async fn seed_entities(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let ledger = genesis_ledger(fluree, ledger_id);
    let _ = apply_trig(
        fluree,
        ledger,
        r#"
        @prefix ex: <http://example.org/> .
        ex:a ex:name "a" .
        ex:b ex:name "b" .
        ex:c ex:name "c" .
        "#,
    )
    .await;
}

/// A datalog query whose one rule has an ALL-UNBOUND leading `where` pattern
/// `{?s ?p ?o}` — the full-ledger-scan shape the budget must bound (issue
/// #1541 review) — deriving `ex:touched true` for every subject.
fn datalog_fullscan_query(ledger_id: &str) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?s"],
        "where": {"@id": "?s", "ex:touched": true},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "?s", "?p": "?o"},
            "insert": {"@id": "?s", "ex:touched": true}
        }]
    })
}

#[tokio::test]
async fn datalog_uncapped_reports_complete_in_tracking() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/datalog-budget-uncapped:main";
    seed_entities(&fluree, ledger_id).await;

    let resp = run_tracked(&fluree, &datalog_fullscan_query(ledger_id)).await;

    let reasoning = resp
        .reasoning
        .expect("tracked datalog query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "unbounded budget must not cap: {reasoning:?}"
    );
    assert!(reasoning.capped_reason.is_none());
    assert!(
        reasoning.derived_facts >= 3,
        "every seeded subject is touched: {reasoning:?}"
    );
}

#[tokio::test]
async fn datalog_query_budget_caps_fixpoint_and_surfaces_in_tracking() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/datalog-budget-query:main";
    seed_entities(&fluree, ledger_id).await;

    let mut query = datalog_fullscan_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 1});

    let resp = run_tracked(&fluree, &query).await;

    let reasoning = resp
        .reasoning
        .expect("tracked datalog query reports a reasoning block");
    assert!(
        reasoning.capped,
        "maxFacts=1 must cap a multi-subject full-scan rule: {reasoning:?}"
    );
    assert!(
        reasoning.capped_reason.is_some(),
        "capped result carries a reason"
    );
}

// ============================================================================
// The fact cap holds INSIDE a round, not only between rounds
// ============================================================================

/// A 40-node `partOf` chain under a transitive property has a 780-fact
/// closure. With `maxFacts: 50` the materialization must stop inside the
/// round that crosses the cap; before the in-round check the whole round ran
/// to completion (a dense 100k-edge graph derived 21M facts under a 1M cap).
#[tokio::test]
async fn fact_cap_holds_within_a_single_round() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-in-round:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let mut trig = String::from(
        "@prefix ex: <http://example.org/> .\n\
         @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:partOf rdf:type owl:TransitiveProperty .\n",
    );
    for i in 0..40 {
        trig.push_str(&format!("ex:n{i} ex:partOf ex:n{} .\n", i + 1));
    }
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 50});
    let resp = run_tracked(&fluree, &query).await;
    let reasoning = resp.reasoning.expect("reasoning block");
    assert!(
        reasoning.capped,
        "maxFacts=50 must cap a 780-fact closure: {reasoning:?}"
    );
    assert_eq!(reasoning.capped_reason.as_deref(), Some("facts"));
    assert!(
        reasoning.derived_facts <= 100,
        "the cap must hold within the round (at most one delta fact's fan-out \
         past 50), got {} derived facts",
        reasoning.derived_facts
    );
}

// ============================================================================
// Memory budget: enforced, operator-settable, and scoped to derived facts
// ============================================================================

/// Seed a transitive chain of `n` nodes. The closure is `n*(n-1)/2` facts, so
/// the size is a tuning dial for budget tests.
async fn seed_chain_of(fluree: &fluree_db_api::Fluree, ledger_id: &str, n: usize) {
    let ledger = genesis_ledger(fluree, ledger_id);
    let mut trig = String::from(
        "@prefix ex: <http://example.org/> .\n\
         @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:partOf rdf:type owl:TransitiveProperty .\n",
    );
    for i in 0..n - 1 {
        trig.push_str(&format!("ex:n{i} ex:partOf ex:n{}.\n", i + 1));
    }
    let _ = apply_trig(fluree, ledger, &trig).await;
}

#[tokio::test]
async fn memory_budget_caps_materialization_and_names_memory_as_the_reason() {
    // The memory ceiling was inert before this PR — nothing read it — so this
    // is the first test anywhere that trips it. A 150-node chain closes to
    // 11,175 facts, comfortably past a 1 MB ceiling at roughly 200 bytes a
    // fact, while staying far below the default fact cap so the reason is
    // unambiguously memory and not facts.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-memory:main";
    seed_chain_of(&fluree, ledger_id, 150).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxMemoryMb": 1});

    let resp = run_tracked(&fluree, &query).await;
    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(reasoning.capped, "a 1 MB ceiling must cap: {reasoning:?}");
    assert_eq!(
        reasoning.capped_reason.as_deref(),
        Some("memory"),
        "the cap must name memory, not facts or time: {reasoning:?}"
    );
}

#[tokio::test]
async fn the_default_memory_ceiling_does_not_bind_before_the_fact_ceiling() {
    // End-to-end guard that an ordinary closure is not capped by anything
    // under default settings. The coherence property itself — that the
    // default memory ceiling exceeds what `max_facts` ordinary flakes cost,
    // which the flat 100 MB default did not — is asserted directly in
    // `fluree_db_reasoner::cache::tests::test_budget_defaults`, because
    // reproducing it end-to-end would need a closure of half a million
    // facts.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-memory-default:main";
    seed_chain_of(&fluree, ledger_id, 150).await;

    let resp = run_tracked(&fluree, &reasoning_query(ledger_id)).await;
    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "an 11k-fact closure must complete under the default budget: {reasoning:?}"
    );
    // The transitive closure of a 150-node chain is 11,175 pairs, of which
    // the 149 direct edges are seed facts rather than derived ones.
    assert_eq!(reasoning.derived_facts, 150 * 149 / 2 - 149);
}

#[tokio::test]
async fn memory_budget_ignores_seed_facts() {
    // `derived_len` excludes re-added base facts from the FACT budget, and
    // `into_derived_flakes` drops them from the overlay the memory budget
    // bounds — but the byte total charged for them anyway. The whole seed
    // delta merges into the derived set at the end of round one, so from
    // round two on the memory cap was being spent on exactly the base data
    // the fact cap ignores.
    //
    // A seed that dwarfs its own closure makes the difference visible: 8,000
    // seed edges are well past a 1 MB ceiling at roughly 200 bytes a fact,
    // while the closure they produce is 3 facts. Charging the seed capped
    // this; scoping the total to derived facts does not.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-memory-seed:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let mut trig = String::from(
        "@prefix ex: <http://example.org/> .\n\
         @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:partOf rdf:type owl:TransitiveProperty .\n\
         ex:a ex:partOf ex:b .\n\
         ex:b ex:partOf ex:c .\n\
         ex:c ex:partOf ex:d .\n",
    );
    // Unrelated `partOf` edges: seed for the rule, but each is its own
    // 2-node chain, so none of them derives anything.
    for i in 0..8_000 {
        trig.push_str(&format!("ex:p{i} ex:partOf ex:q{i} .\n"));
    }
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxMemoryMb": 1});

    let resp = run_tracked(&fluree, &query).await;
    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "a 3-fact closure must not be capped by the seed it read: {reasoning:?}"
    );
    assert_eq!(reasoning.derived_facts, 3);
}

#[tokio::test]
async fn already_stored_entailments_do_not_trip_the_fact_cap() {
    // Round 1 registers the seed facts in `base_keys` but not in `seen`, so
    // `DerivedSet::contains` answered false for them and every rule that
    // RE-derived an already-stored fact pushed it into the round's delta,
    // where the in-round cap counted it. A ledger that already stores its own
    // entailments — the ordinary DBpedia shape, where a `Student` is also
    // typed `Person`, or symmetric edges stored both ways — could therefore
    // cap on facts it derives nothing new from, and the truncated closure is
    // then cached.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-stored-entailments:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let mut trig = String::from(
        "@prefix ex: <http://example.org/> .\n\
         @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:partOf rdf:type owl:SymmetricProperty .\n",
    );
    // Both directions already stored: the symmetric rule re-derives each one
    // and adds nothing.
    for i in 0..400 {
        trig.push_str(&format!(
            "ex:a{i} ex:partOf ex:b{i} .\nex:b{i} ex:partOf ex:a{i} .\n"
        ));
    }
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let mut query = reasoning_query(ledger_id);
    query["reasoningBudget"] = json!({"maxFacts": 100});

    let resp = run_tracked(&fluree, &query).await;
    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert!(
        !reasoning.capped,
        "re-deriving 800 already-stored facts must not trip a 100-fact cap: {reasoning:?}"
    );
    assert_eq!(
        reasoning.derived_facts, 0,
        "nothing is genuinely new: {reasoning:?}"
    );
}

#[tokio::test]
async fn query_memory_budget_survives_config_default_modes() {
    // When ledger-config modes apply, the query's own reasoning modes are
    // REPLACED by the config's, and each budget field the query set has to be
    // carried across that replacement by hand. `maxFacts` and `maxSeconds`
    // were; `maxMemoryMb` was not, so on any ledger with `f:reasoningDefaults`
    // a query asking for a memory ceiling silently got the default instead.
    // The other memory tests use a ledger with no config, so none of them
    // reach this path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-budget-memory-config-modes:main";
    seed_chain_of(&fluree, ledger_id, 150).await;

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

    let mut query = reasoning_query(ledger_id);
    query.as_object_mut().unwrap().remove("reasoning");
    query["reasoningBudget"] = json!({"maxMemoryMb": 1});

    let resp = run_tracked(&fluree, &query).await;
    let reasoning = resp
        .reasoning
        .expect("tracked reasoning query reports a reasoning block");
    assert_eq!(
        reasoning.capped_reason.as_deref(),
        Some("memory"),
        "the query's own memory ceiling must survive config mode replacement: {reasoning:?}"
    );
}
