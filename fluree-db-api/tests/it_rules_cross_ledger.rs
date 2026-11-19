//! End-to-end cross-ledger datalog rule routing.
//!
//! Data ledger D's `#config` declares `f:rulesSource` with
//! `f:ledger` pointing at model ledger M's rules graph. At query
//! time, `view/query.rs::attach_cross_ledger_rules` reads M's
//! `f:rule` values (via the cross-ledger resolver,
//! `ArtifactKind::Rules`) the way D's own extractor reads D's, and
//! attaches them, with the ledger, graph and `t` they were read at,
//! as `executable.reasoning.imported_rules`. The datalog evaluator
//! parses each as a stored rule against D's snapshot and runs the
//! fixpoint on D's data; the tracked `reasoning` block reports the
//! rule set and where it came from.

#![cfg(feature = "native")]

use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

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

/// Defense-in-depth: `RulesArtifactWire::imported_rules` must
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
fn imported_rules_fail_closed_on_malformed_json() {
    use fluree_db_api::cross_ledger::{ImportedRule, RuleLanguage, RulesArtifactWire, WireOrigin};

    let wire = RulesArtifactWire {
        origin: WireOrigin {
            model_ledger_id: "test/m:main".into(),
            graph_iri: "http://example.org/rules".into(),
            resolved_t: 1,
            commit_id: None,
        },
        rules: vec![ImportedRule {
            id: "http://example.org/broken".into(),
            language: RuleLanguage::JsonLd,
            source: "{ valid: false".into(),
        }],
    };

    let err = wire
        .imported_rules()
        .expect_err("malformed JSON must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("malformed cross-ledger rule <http://example.org/broken>"),
        "expected explicit malformed-rule diagnostic naming the rule, got: {msg}"
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

/// Dataset-path parity: cross-ledger `f:rulesSource` must engage when
/// the query takes the multi-ledger dataset path, which previously
/// attached neither `rules_source_g_id` nor cross-ledger rules. The
/// second source is an unrelated ledger — NOT M — so the rule can only
/// arrive via the cross-ledger resolver on D's config.
#[tokio::test]
async fn dataset_query_pulls_rules_from_model_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-rules/model-ds:main";
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

    let data_id = "test/cross-ledger-rules/data-ds:main";
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

    let other_id = "test/cross-ledger-rules/other-ds:main";
    let other = genesis_ledger(&fluree, other_id);
    fluree
        .insert(
            other,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:unrelated",
                "ex:note": "no rules here"
            }),
        )
        .await
        .expect("seed unrelated ledger");

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "from": [data_id, other_id],
        "select": "?grandparent",
        "where":  {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("dataset query with cross-ledger rule");
    let data = fluree.ledger(data_id).await.expect("reload D ledger");
    let rows = result.to_jsonld(&data.snapshot).expect("to_jsonld");
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:charlie")),
        "M's grandparent rule (resolved cross-ledger) must derive \
         charlie on the dataset path; got: {results:?}"
    );
}

// ---------------------------------------------------------------------------
// What runs, and what the response says ran
// ---------------------------------------------------------------------------

const SCORED_RULES_GRAPH: &str = "http://example.org/rules";

/// M holds `ex:r1` (`@json`: a score over 10 makes a candidate) and `ex:r2`
/// (`f:sparql`: anything scored is a SPARQL candidate) in `ex:rules`. D scores
/// alice 5 and bob 50, and its config reads its rules from M's `ex:rules`.
async fn scored_ledgers(fluree: &fluree_db_api::Fluree, model_id: &str, data_id: &str) {
    let model = genesis_ledger(fluree, model_id);
    fluree
        .update(
            model,
            &json!({
                "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
                "insert": [
                    ["graph", SCORED_RULES_GRAPH,
                     {"@id": "ex:r1", "f:rule": {"@type": "@json", "@value": {
                         "@context": {"ex": "http://example.org/"},
                         "where": [{"@id": "?a", "ex:score": "?s"}, ["filter", "(> ?s 10)"]],
                         "insert": {"@id": "?a", "@type": "ex:Candidate"}
                     }}}],
                    ["graph", SCORED_RULES_GRAPH,
                     {"@id": "ex:r2", "f:rule": {"@type": "f:sparql", "@value":
                         "PREFIX ex: <http://example.org/> \
                          CONSTRUCT { ?a a ex:SparqlCandidate } WHERE { ?a ex:score ?s }"}}]
                ]
            }),
        )
        .await
        .expect("seed M with rules");

    let data = genesis_ledger(fluree, data_id);
    let cfg = config_iri(data_id);
    let data = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            GRAPH <{cfg}> {{
                <urn:cfg:main>      rdf:type          f:LedgerConfig ;
                                    f:datalogDefaults <urn:cfg:datalog> .
                <urn:cfg:datalog>   f:datalogEnabled  true ;
                                    f:rulesSource     <urn:cfg:rules-ref> .
                <urn:cfg:rules-ref> rdf:type          f:GraphRef ;
                                    f:graphSource     <urn:cfg:rules-src> .
                <urn:cfg:rules-src> f:ledger          <{model_id}> ;
                                    f:graphSelector   <{SCORED_RULES_GRAPH}> .
            }}"
        ))
        .execute()
        .await
        .expect("seed D config")
        .ledger;
    fluree
        .insert(
            data,
            &json!({"@context": {"ex": "http://example.org/"}, "@graph": [
                {"@id": "ex:alice", "@type": "ex:Account", "ex:score": 5},
                {"@id": "ex:bob", "@type": "ex:Account", "ex:score": 50}
            ]}),
        )
        .await
        .expect("seed D data");
}

/// Add a rule to M's rules graph.
async fn add_model_rule(fluree: &fluree_db_api::Fluree, model_id: &str, rule: serde_json::Value) {
    let model = fluree.ledger(model_id).await.expect("load M");
    fluree
        .update(
            model,
            &json!({
                "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
                "insert": [["graph", SCORED_RULES_GRAPH, rule]]
            }),
        )
        .await
        .expect("add rule to M");
}

/// `(subject, type)` pairs a tracked datalog query over D returns, with the
/// tracked `reasoning` block.
async fn typed_subjects(
    fluree: &fluree_db_api::Fluree,
    data_id: &str,
) -> std::result::Result<
    (Vec<serde_json::Value>, fluree_db_api::ReasoningTally),
    fluree_db_api::TrackedErrorResponse,
> {
    let view = fluree.db(data_id).await.expect("load D with config");
    let response = view
        .query(fluree)
        .jsonld(&json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?a", "?c"],
            "where": {"@id": "?a", "@type": "?c"},
            "reasoning": "datalog",
            "opts": {"meta": true}
        }))
        .execute_tracked()
        .await?;
    let mut rows = normalize_rows(&response.result);
    rows.sort_by_key(ToString::to_string);
    let tally = response
        .reasoning
        .expect("a reasoning query reports its reasoning");
    Ok((rows, tally))
}

/// The issue's repro: an `f:sparql` rule stored in the rules ledger runs, as
/// it does stored in the data ledger, and the tracked response names the
/// rules ledger, the `t` it was read at, and the rule set by content. Adding
/// a rule changes the hash and the `t`, and (the rules being read at the
/// rules ledger's head) what the query derives.
#[tokio::test]
async fn cross_ledger_sparql_rules_run_and_the_response_says_what_ran() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-rules/scored-model:main";
    let data_id = "test/cross-ledger-rules/scored-data:main";
    scored_ledgers(&fluree, model_id, data_id).await;

    let (rows, tally) = typed_subjects(&fluree, data_id).await.expect("query");
    for (subject, class) in [
        ("ex:bob", "ex:Candidate"),
        ("ex:alice", "ex:SparqlCandidate"),
        ("ex:bob", "ex:SparqlCandidate"),
    ] {
        assert!(
            rows.contains(&json!([subject, class])),
            "{subject} {class} missing: {rows:?}"
        );
    }
    assert!(
        !rows.contains(&json!(["ex:alice", "ex:Candidate"])),
        "{rows:?}"
    );

    let source = tally.rules_source.clone().expect("rules source reported");
    assert_eq!(source.ledger, model_id);
    assert_eq!(source.graph, SCORED_RULES_GRAPH);
    let model_t = fluree.ledger(model_id).await.expect("M").t();
    assert_eq!(source.t, model_t);
    let first_hash = tally.rule_set.clone().expect("rule set reported");
    assert_eq!(first_hash.len(), 16, "{first_hash}");
    assert_eq!(
        tally.rules_fired.get("http://example.org/r2"),
        Some(&2),
        "the SPARQL rule is named by its IRI: {:?}",
        tally.rules_fired
    );

    // The same query again is served from the reasoning cache and reports the
    // same rule set and source.
    let (_, cached) = typed_subjects(&fluree, data_id)
        .await
        .expect("cached query");
    assert_eq!(cached.rule_set, tally.rule_set);
    assert_eq!(cached.rules_source, tally.rules_source);

    add_model_rule(
        &fluree,
        model_id,
        json!({"@id": "ex:r3", "f:rule": {"@type": "@json", "@value": {
            "@context": {"ex": "http://example.org/"},
            "where": [{"@id": "?a", "ex:score": "?s"}, ["filter", "(> ?s 1)"]],
            "insert": {"@id": "?a", "@type": "ex:Candidate"}
        }}}),
    )
    .await;
    let (rows, tally) = typed_subjects(&fluree, data_id)
        .await
        .expect("query after r3");
    assert!(
        rows.contains(&json!(["ex:alice", "ex:Candidate"])),
        "{rows:?}"
    );
    let source = tally.rules_source.expect("rules source reported");
    assert!(source.t > model_t, "{} > {model_t}", source.t);
    assert_ne!(tally.rule_set.expect("rule set reported"), first_hash);
}

/// An `f:rule` value of any other datatype fails the query naming the rule,
/// rather than being skipped, as it does in the ledger that stores it.
#[tokio::test]
async fn a_cross_ledger_rule_of_another_datatype_fails_the_query() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-rules/typed-model:main";
    let data_id = "test/cross-ledger-rules/typed-data:main";
    scored_ledgers(&fluree, model_id, data_id).await;
    add_model_rule(
        &fluree,
        model_id,
        json!({"@id": "ex:r4", "f:rule":
            "PREFIX ex: <http://example.org/> CONSTRUCT { ?a a ex:Plain } WHERE { ?a ex:score ?s }"}),
    )
    .await;

    let err = typed_subjects(&fluree, data_id)
        .await
        .expect_err("a plain-string rule is refused");
    assert!(
        err.error
            .contains("datalog rule <http://example.org/r4> has datatype")
            && err.error.contains("f:sparql-typed CONSTRUCT"),
        "{}",
        err.error
    );
}

/// A rule that does not parse fails the query naming it and the ledger it
/// came from (it used to be an anonymous `_:query_rule_N`).
#[tokio::test]
async fn an_invalid_cross_ledger_rule_is_named_with_its_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-rules/invalid-model:main";
    let data_id = "test/cross-ledger-rules/invalid-data:main";
    scored_ledgers(&fluree, model_id, data_id).await;
    add_model_rule(
        &fluree,
        model_id,
        json!({"@id": "ex:r5", "f:rule": {"@type": "@json", "@value": {
            "@context": {"ex": "http://example.org/"},
            "where": [{"@id": "?a", "ex:score": "?s"},
                      ["optional", {"@id": "?a", "ex:note": "?n"}]],
            "insert": {"@id": "?a", "@type": "ex:Noted"}
        }}}),
    )
    .await;

    let err = typed_subjects(&fluree, data_id)
        .await
        .expect_err("an OPTIONAL rule is refused");
    assert_eq!(err.status, 400, "{}", err.error);
    assert!(
        err.error.contains(&format!(
            "datalog rule <http://example.org/r5> from ledger '{model_id}'"
        )) && err.error.contains("OPTIONAL"),
        "{}",
        err.error
    );
}

/// Rules from another ledger are stored rules: re-read on every query, with
/// no author present, so a check that depends on what D holds warns rather
/// than refusing. A filter operand in a namespace D has never seen would
/// refuse a query-time rule.
#[tokio::test]
async fn cross_ledger_rules_are_checked_as_stored_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-rules/stored-model:main";
    let data_id = "test/cross-ledger-rules/stored-data:main";
    scored_ledgers(&fluree, model_id, data_id).await;
    add_model_rule(
        &fluree,
        model_id,
        json!({"@id": "ex:r6", "f:rule": {"@type": "f:sparql", "@value":
            "PREFIX ex: <http://example.org/> CONSTRUCT { ?a a ex:Kept } \
             WHERE { ?a ex:score ?s FILTER(?a != <http://unseen.example/x>) }"}}),
    )
    .await;

    let (rows, _) = typed_subjects(&fluree, data_id).await.expect("query");
    assert!(rows.contains(&json!(["ex:alice", "ex:Kept"])), "{rows:?}");
    assert!(rows.contains(&json!(["ex:bob", "ex:Kept"])), "{rows:?}");
}
