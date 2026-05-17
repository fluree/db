//! End-to-end cross-ledger datalog rule routing.
//!
//! Data ledger D's `#config` declares `f:rulesSource` with
//! `f:ledger` pointing at model ledger M's rules graph. At query
//! time, `view/query.rs::attach_cross_ledger_rules` resolves M's
//! rules into JSON bodies (via the cross-ledger resolver,
//! `ArtifactKind::Rules`) and feeds them into
//! `executable.reasoning.modes.rules`. The existing datalog
//! evaluator parses each rule against D's snapshot and runs the
//! fixpoint on D's data.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

fn config_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

#[tokio::test]
async fn data_ledger_query_pulls_rules_from_model_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();

    // --- M (model ledger): stash a grandparent rule in a named graph.
    let model_id = "test/cross-ledger-rules/model:main";
    let rules_graph_iri = "http://example.org/governance/rules";
    let model = genesis_ledger(&fluree, model_id);

    let rule_tx = json!({
        "@context": {
            "ex": "http://example.org/",
            "f":  "https://ns.flur.ee/db#"
        },
        "insert": [
            ["graph", rules_graph_iri, {
                "@id": "ex:grandparentRule",
                "f:rule": {
                    "@type":  "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where":    {"@id": "?p", "ex:parent": {"ex:parent": "?g"}},
                        "insert":   {"@id": "?p", "ex:grandparent": {"@id": "?g"}}
                    }
                }
            }]
        ]
    });
    fluree
        .update(model, &rule_tx)
        .await
        .expect("seed M with grandparent rule");

    // --- D (data ledger): wire f:rulesSource → M's rules graph.
    let data_id = "test/cross-ledger-rules/data:main";
    let data = genesis_ledger(&fluree, data_id);

    let cfg = config_iri(data_id);
    let cfg_trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{cfg}> {{
            <urn:cfg:main>     rdf:type          f:LedgerConfig .
            <urn:cfg:main>     f:datalogDefaults <urn:cfg:datalog> .
            <urn:cfg:datalog>  f:datalogEnabled  true .
            <urn:cfg:datalog>  f:rulesSource     <urn:cfg:rules-ref> .
            <urn:cfg:rules-ref> rdf:type         f:GraphRef ;
                                f:graphSource    <urn:cfg:rules-src> .
            <urn:cfg:rules-src> f:ledger         <{model_id}> ;
                                f:graphSelector  <{rules_graph_iri}> .
        }}
    "
    );
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed D config with cross-ledger f:rulesSource");
    let data = r.ledger;

    // --- D's data: alice → bob → charlie family chain.
    let family = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob",   "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    fluree
        .insert(data, &family)
        .await
        .expect("insert family data into D");

    // --- Query D for alice's grandparent with datalog reasoning.
    //     Must load D's view via `fluree.db(...)` so the resolved
    //     config (and the cross-ledger dispatch) actually runs.
    let view = fluree.db(data_id).await.expect("load D with config");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?grandparent",
        "where":  {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });
    let data = fluree.ledger(data_id).await.expect("reload D ledger");
    let rows = fluree
        .query(&view, &q)
        .await
        .expect("query D with cross-ledger rule")
        .to_jsonld(&data.snapshot)
        .expect("to_jsonld");
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:charlie")),
        "M's grandparent rule (resolved cross-ledger) must derive \
         charlie as alice's grandparent on D; got: {results:?}"
    );
}

#[tokio::test]
async fn missing_model_ledger_surfaces_cross_ledger_error() {
    // f:rulesSource → a model ledger that doesn't exist. The
    // resolver must return CrossLedgerError::ModelLedgerMissing
    // (mapped to ApiError::CrossLedger), not silently fall back
    // to "no rules".
    let fluree = FlureeBuilder::memory().build_memory();

    let data_id = "test/cross-ledger-rules/missing-model:main";
    let data = genesis_ledger(&fluree, data_id);

    let cfg = config_iri(data_id);
    let cfg_trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{cfg}> {{
            <urn:cfg:main>      rdf:type          f:LedgerConfig .
            <urn:cfg:main>      f:datalogDefaults <urn:cfg:datalog> .
            <urn:cfg:datalog>   f:datalogEnabled  true .
            <urn:cfg:datalog>   f:rulesSource     <urn:cfg:rules-ref> .
            <urn:cfg:rules-ref> rdf:type          f:GraphRef ;
                                f:graphSource     <urn:cfg:rules-src> .
            <urn:cfg:rules-src> f:ledger          <does-not-exist:main> ;
                                f:graphSelector   <http://example.org/rules> .
        }}
    "
    );
    fluree
        .stage_owned(data)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed D config");

    let view = fluree.db(data_id).await.expect("load D");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?g",
        "where":  {"@id": "ex:alice", "ex:grandparent": "?g"},
        "reasoning": "datalog"
    });
    let err = fluree
        .query(&view, &q)
        .await
        .expect_err("missing model ledger must surface as an error");

    assert!(
        matches!(err, fluree_db_api::ApiError::CrossLedger(_)),
        "expected ApiError::CrossLedger for missing model ledger, got: {err:?}"
    );
}
