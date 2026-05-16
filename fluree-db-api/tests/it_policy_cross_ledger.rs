//! End-to-end cross-ledger policy enforcement.
//!
//! Data ledger D's `#config` declares `f:policySource` with `f:ledger`
//! pointing at model ledger M and `f:graphSelector` pointing at a
//! named graph in M that holds policy rules. A query against D under
//! `db_with_policy` must enforce M's rules — the data D's own graphs
//! never see the policy IRIs, the rules live exclusively in M.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, QueryConnectionOptions};
use serde_json::json;
use support::genesis_ledger;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Data ledger D references a deny-on-class policy in model ledger M.
/// A query that targets the denied class against D must return empty;
/// a query that targets unrelated data must return normally. M's rules
/// govern D without any policy IRIs ever being written into D.
#[tokio::test]
async fn data_ledger_query_enforces_model_ledger_deny_policy() {
    let fluree = FlureeBuilder::memory().build_memory();

    // --- model ledger M: holds a deny-on-class policy in a named graph
    let model_id = "test/cross-ledger-e2e/model:main";
    let model = genesis_ledger(&fluree, model_id);

    let policy_graph_iri = "http://example.org/m-policies";
    let m_trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{policy_graph_iri}> {{
            ex:denyUsers
                rdf:type    f:AccessPolicy ;
                f:action    f:view ;
                f:onClass   ex:User ;
                f:allow     false .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&m_trig)
        .execute()
        .await
        .expect("seed M policy graph");

    // --- data ledger D: holds user data plus a cross-ledger config
    let data_id = "test/cross-ledger-e2e/data:main";
    let data = genesis_ledger(&fluree, data_id);

    // First write the actual user data into D's default graph.
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice"},
                    {"@id": "ex:doc1",  "@type": "ex:Doc",  "ex:title": "Spec"}
                ]
            }),
        )
        .await
        .expect("seed D user + doc");
    let data = r1.ledger;

    // Then write D's #config: defaultAllow=false, policy_class points
    // at f:AccessPolicy so the cross-ledger restriction's policy_types
    // intersection passes, and f:policySource carries f:ledger so the
    // resolver routes to M.
    let config_iri = config_graph_iri(data_id);
    let d_config = format!(
        r"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
            <urn:cfg:policy> f:defaultAllow false .
            <urn:cfg:policy> f:policyClass f:AccessPolicy .
            <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
            <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                 f:graphSource <urn:cfg:policy-src> .
            <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                 f:graphSelector <{policy_graph_iri}> .
        }}
    "
    );
    fluree
        .stage_owned(data)
        .upsert_turtle(&d_config)
        .execute()
        .await
        .expect("seed D cross-ledger config");

    // --- query D under cross-ledger policy
    let opts = QueryConnectionOptions::default();
    let wrapped = fluree
        .db_with_policy(data_id, &opts)
        .await
        .expect("db_with_policy must route cross-ledger config through M");

    // Query 1: target the denied class (ex:User). M's deny rule must
    // apply against D's data — alice is filtered out.
    let q_users = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": "?u",
        "where": {"@id": "?u", "@type": "ex:User"}
    });
    let users = fluree
        .query(&wrapped, &q_users)
        .await
        .expect("query ex:User under cross-ledger policy");
    let users_jsonld = users
        .to_jsonld(&wrapped.snapshot)
        .expect("jsonld users");
    assert_eq!(
        users_jsonld,
        json!([]),
        "M's deny-on-ex:User must filter alice out of D's results, got {users_jsonld}"
    );

    // Query 2: target an unrelated class (ex:Doc). M's rule doesn't
    // touch ex:Doc, but the config's defaultAllow=false means every
    // class must be governed by an explicit allow. Phase 1a's
    // f:AccessPolicy filter only loaded the deny rule; ex:Doc has
    // no allow policy, so it should also be denied under
    // defaultAllow=false. This verifies the cross-ledger policies
    // are the only ones in play (no silent fallthrough to default-
    // graph rules in D).
    let q_docs = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": "?d",
        "where": {"@id": "?d", "@type": "ex:Doc"}
    });
    let docs = fluree
        .query(&wrapped, &q_docs)
        .await
        .expect("query ex:Doc under cross-ledger policy");
    let docs_jsonld = docs.to_jsonld(&wrapped.snapshot).expect("jsonld docs");
    assert_eq!(
        docs_jsonld,
        json!([]),
        "defaultAllow=false with only M's deny rule must also block ex:Doc, got {docs_jsonld}"
    );
}

/// Combining `opts.identity` with cross-ledger `f:policySource` is
/// a fail-closed config error in Phase 1a: the model ledger
/// contributes policy rules, the data ledger contributes identity
/// binding, and mixing them via identity-mode would attribute
/// policies ambiguously across ledger boundaries.
#[tokio::test]
async fn cross_ledger_plus_identity_mode_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-e2e/id-model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/id-policies";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:rule1 rdf:type f:AccessPolicy ; f:action f:view ; f:allow true .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M");

    let data_id = "test/cross-ledger-e2e/id-data:main";
    let data = genesis_ledger(&fluree, data_id);

    let config_iri = config_graph_iri(data_id);
    fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");

    let opts = QueryConnectionOptions {
        identity: Some("http://example.org/users/alice".into()),
        ..Default::default()
    };

    let err = fluree
        .db_with_policy(data_id, &opts)
        .await
        .expect_err("identity + cross-ledger must fail closed");

    let msg = err.to_string();
    assert!(
        msg.contains("identity") && msg.contains("cross-ledger"),
        "expected fail-closed diagnostic mentioning both, got: {msg}"
    );
}
