//! Same-ledger `f:rulesSource` routing.
//!
//! Datalog rules live in a non-default named graph; `f:rulesSource`
//! on the config graph points the rule extractor at that graph. The
//! extractor must scan the configured graph rather than the default
//! graph, and the resulting rules must execute against the query
//! graph as usual.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

fn config_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Seed a non-default graph with a grandparent datalog rule and
/// configure `f:rulesSource` to point at that graph. Returns the
/// post-tx ledger.
async fn seed_rules_in_named_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    rules_graph_iri: &str,
) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);

    // JSON-LD update with `["graph", <iri>, {…}]` sugar so the
    // rule flake lands in the configured rules graph (rather than
    // the default graph). The rule body keeps `@type: @json` so it
    // stores as `FlakeValue::Json` — which `extract_datalog_rules`
    // is the only path that picks up.
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
    let r = fluree
        .update(ledger, &rule_tx)
        .await
        .expect("seed rule into named graph");
    let ledger = r.ledger;

    // Wire f:rulesSource → rules_graph_iri via the config graph.
    let cfg = config_iri(ledger_id);
    let cfg_trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{cfg}> {{
            <urn:cfg:main>     rdf:type           f:LedgerConfig .
            <urn:cfg:main>     f:datalogDefaults  <urn:cfg:datalog> .
            <urn:cfg:datalog>  f:datalogEnabled   true .
            <urn:cfg:datalog>  f:rulesSource      <urn:cfg:rules-ref> .
            <urn:cfg:rules-ref> rdf:type          f:GraphRef ;
                                f:graphSource    <urn:cfg:rules-src> .
            <urn:cfg:rules-src> f:graphSelector  <{rules_graph_iri}> .
        }}
    "
    );
    let r = fluree
        .stage_owned(ledger)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed config with f:rulesSource");
    r.ledger
}

async fn insert_family_data(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
) -> fluree_db_api::LedgerState {
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob",   "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    fluree
        .insert(ledger, &data)
        .await
        .expect("insert family data")
        .ledger
}

async fn query_grandparent(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    ledger_id: &str,
) -> Vec<serde_json::Value> {
    // Use `fluree.db(...)` instead of `GraphDb::from_ledger_state`
    // so the view has `resolved_config` applied — the f:rulesSource
    // routing only takes effect once `apply_config_datalog` runs,
    // which requires the resolved config.
    let view = fluree.db(ledger_id).await.expect("load db with config");
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?grandparent",
        "where":  {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    normalize_rows(&rows)
}

#[tokio::test]
async fn rules_source_in_named_graph_is_honored() {
    // Rules live in a named graph; config wires f:rulesSource at it;
    // the rule must fire and derive `ex:grandparent`.
    let fluree = FlureeBuilder::memory().build_memory();
    let rules_iri = "http://example.org/governance/rules";
    let ledger_id = "rules-src/honored:main";
    let ledger = seed_rules_in_named_graph(&fluree, ledger_id, rules_iri).await;
    let ledger = insert_family_data(&fluree, ledger).await;

    let results = query_grandparent(&fluree, &ledger, ledger_id).await;
    assert!(
        results.contains(&json!("ex:charlie")),
        "f:rulesSource → named graph must route rule extraction; \
         expected charlie derived as alice's grandparent, got: {results:?}"
    );
}

#[tokio::test]
async fn unknown_rules_source_graph_iri_fails_loudly() {
    // Misconfiguration: f:rulesSource points at a graph IRI that
    // doesn't exist in this ledger. Must surface as a config error
    // at db() load time, not silently fall back to "no rules".
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "rules-src/unknown-graph:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let cfg = config_iri(ledger_id);
    let bad_iri = "http://example.org/this-graph-does-not-exist";
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
            <urn:cfg:rules-src> f:graphSelector   <{bad_iri}> .
        }}
    "
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed misconfigured rulesSource");

    let err = fluree
        .db(ledger_id)
        .await
        .expect_err("unknown rulesSource graph IRI must fail loudly");
    let msg = err.to_string();
    assert!(
        msg.contains("f:rulesSource") && msg.contains("not found"),
        "expected explicit f:rulesSource error, got: {msg}"
    );
}

#[tokio::test]
async fn rules_source_with_unsupported_at_t_fails_loudly() {
    // f:atT on f:rulesSource is reserved (Phase 3+). Must surface
    // as a config error rather than silently disabling rules.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "rules-src/unsupported-at-t:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let cfg = config_iri(ledger_id);
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
            <urn:cfg:rules-src> f:graphSelector   f:defaultGraph ;
                                f:atT             5 .
        }}
    "
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&cfg_trig)
        .execute()
        .await
        .expect("seed config with reserved f:atT");

    let err = fluree
        .db(ledger_id)
        .await
        .expect_err("f:atT on f:rulesSource must surface as a config error");
    let msg = err.to_string();
    assert!(
        msg.contains("f:atT"),
        "expected f:atT mention in error, got: {msg}"
    );
}

#[tokio::test]
async fn rule_in_named_graph_without_rules_source_is_ignored() {
    // Negative control: same rule, same named graph, but NO
    // f:rulesSource configured. The extractor defaults to the
    // query graph (g_id=0); the rule isn't visible there, so no
    // grandparent fact is derived. This proves the wiring above
    // is load-bearing, not a coincidence.
    let fluree = FlureeBuilder::memory().build_memory();
    let rules_iri = "http://example.org/governance/rules";
    let ledger_id = "rules-src/no-config:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Same rule, same named graph — but no `f:rulesSource` config.
    let rule_tx = json!({
        "@context": {
            "ex": "http://example.org/",
            "f":  "https://ns.flur.ee/db#"
        },
        "insert": [
            ["graph", rules_iri, {
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
    let r = fluree.update(ledger, &rule_tx).await.expect("seed rule");
    let ledger = insert_family_data(&fluree, r.ledger).await;

    let results = query_grandparent(&fluree, &ledger, ledger_id).await;
    assert!(
        !results.contains(&json!("ex:charlie")),
        "without f:rulesSource the named-graph rule must not be \
         discovered; got an unexpected derived charlie: {results:?}"
    );
}
