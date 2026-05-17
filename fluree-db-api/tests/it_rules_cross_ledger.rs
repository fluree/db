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
async fn cross_ledger_rules_with_default_graph_selector() {
    // Cross-ledger f:rulesSource with `f:graphSelector f:defaultGraph`
    // must resolve to M's g_id=0 (not collide with the named-graph
    // registry lookup). Without the fix, this returned
    // GraphMissingAtT because `https://ns.flur.ee/db#defaultGraph`
    // isn't a registered graph IRI on M.
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-rules/default-graph-model:main";
    let model = genesis_ledger(&fluree, model_id);

    // Insert the rule into M's *default* graph (no GRAPH wrapper).
    let rule_doc = json!({
        "@context": {
            "ex": "http://example.org/",
            "f":  "https://ns.flur.ee/db#"
        },
        "@id": "ex:grandparentRule",
        "f:rule": {
            "@type":  "@json",
            "@value": {
                "@context": {"ex": "http://example.org/"},
                "where":    {"@id": "?p", "ex:parent": {"ex:parent": "?g"}},
                "insert":   {"@id": "?p", "ex:grandparent": {"@id": "?g"}}
            }
        }
    });
    fluree
        .insert(model, &rule_doc)
        .await
        .expect("seed M default graph with rule");

    // D's config points at M's default graph via f:defaultGraph.
    let data_id = "test/cross-ledger-rules/default-graph-data:main";
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
            <urn:cfg:rules-src> f:ledger          <{model_id}> ;
                                f:graphSelector   f:defaultGraph .
        }}
    "
    );
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed D config (cross-ledger + f:defaultGraph)");
    let data = r.ledger;

    let family = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob",   "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    fluree.insert(data, &family).await.expect("seed D family");

    let view = fluree.db(data_id).await.expect("load D");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?g",
        "where":  {"@id": "ex:alice", "ex:grandparent": "?g"},
        "reasoning": "datalog"
    });
    let data = fluree.ledger(data_id).await.expect("reload D");
    let rows = fluree
        .query(&view, &q)
        .await
        .expect("cross-ledger rule via f:defaultGraph must work")
        .to_jsonld(&data.snapshot)
        .expect("to_jsonld");
    let results = normalize_rows(&rows);
    assert!(
        results.contains(&json!("ex:charlie")),
        "expected charlie derived via cross-ledger f:defaultGraph; got: {results:?}"
    );
}

/// Defense-in-depth: `RulesArtifactWire::parsed_rules` must
/// `Err(CrossLedgerError::TranslationFailed)` on any malformed JSON
/// body rather than silently dropping the entry. Cross-ledger
/// governance is admin-authored; silently weakening the reasoning
/// model is the worst failure mode.
///
/// Normal write paths (Turtle/JSON-LD insert) reject malformed JSON
/// literals at the storage boundary — so this is structurally
/// unreachable through user code today. The guard remains because
/// the wire boundary can be reached by future formats, repairs, or
/// out-of-band index writes; the unit test pins the behaviour.
#[test]
fn parsed_rules_fails_closed_on_malformed_json() {
    use fluree_db_api::cross_ledger::{RulesArtifactWire, WireOrigin};

    let wire = RulesArtifactWire {
        origin: WireOrigin {
            model_ledger_id: "test/m:main".into(),
            graph_iri: "http://example.org/rules".into(),
            resolved_t: 1,
        },
        rules: vec!["{ valid: false".into()],
    };

    let err = wire
        .parsed_rules()
        .expect_err("malformed JSON must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("malformed cross-ledger rule"),
        "expected explicit malformed-rule diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn cross_ledger_f_txn_meta_graph_selector_rejected() {
    // `f:txnMetaGraph` is a reserved sentinel — the model
    // ledger's txn-meta graph is never a legitimate cross-ledger
    // target (it carries commit-time provenance). The selector
    // helper must surface ReservedGraphSelected *before* any
    // storage I/O on M.
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-rules/txn-meta-selector-model:main";
    let model = genesis_ledger(&fluree, model_id);
    // Commit one trivial fact so M is published to the nameservice;
    // the resolver looks up M by id before reaching the selector
    // check, and an unpublished M short-circuits as ModelLedgerMissing.
    fluree
        .insert(
            model,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id":     "ex:bootstrap",
                "ex:tag":  "init"
            }),
        )
        .await
        .expect("publish M");

    let data_id = "test/cross-ledger-rules/txn-meta-selector-data:main";
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
            <urn:cfg:rules-src> f:ledger          <{model_id}> ;
                                f:graphSelector   f:txnMetaGraph .
        }}
    "
    );
    fluree
        .stage_owned(data)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed D config selecting f:txnMetaGraph");

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
        .expect_err("f:txnMetaGraph selector must surface ReservedGraphSelected");

    let msg = err.to_string();
    assert!(
        matches!(err, fluree_db_api::ApiError::CrossLedger(_)) && msg.contains("txnMetaGraph"),
        "expected ApiError::CrossLedger naming the reserved sentinel, got: {err:?}"
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
