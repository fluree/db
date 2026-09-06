//! End-to-end cross-ledger policy enforcement.
//!
//! Data ledger D's `#config` declares `f:policySource` with `f:ledger`
//! pointing at model ledger M and `f:graphSelector` pointing at a
//! named graph in M that holds policy rules. A query against D under
//! `db_with_policy` must enforce M's rules — the data D's own graphs
//! never see the policy IRIs, the rules live exclusively in M.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{FlureeBuilder, GovernanceOptions};
use serde_json::json;

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
        r"
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
    "
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
    let opts = GovernanceOptions::default();
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
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld users");
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

// =============================================================================
// Class-filter contract — exact-IRI resolution at the wire→Sid boundary.
//
// The cross-ledger contract says: the data ledger's configured
// `f:policyClass` set is intersected against each wire restriction's
// `policy_types` by exact IRI match (no subclass entailment). The three
// tests below pin both sides of that filter:
//
//   1. A custom-typed policy in M is enforced iff D's config names the
//      exact same class IRI in `f:policyClass`.
//   2. The same policy is silently skipped (no enforcement) when D's
//      config names any other class — even if M holds a perfectly valid
//      policy resource and D would otherwise allow alice to be seen.
//   3. Direct `f:AccessPolicy` remains the baseline / default —
//      configurations targeting it work the same way they would in a
//      same-ledger setup.
// =============================================================================

/// Custom-class enforcement: a policy typed as `ex:OrgPolicy` in M is
/// enforced against D iff D's `f:policyClass` config lists exactly
/// `ex:OrgPolicy`. This is the "include the class → enforcement
/// applies" side of the filter contract.
#[tokio::test]
async fn custom_class_policy_enforced_when_data_ledger_includes_class() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-filter/custom-included/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/custom-policies";

    // Policy typed as ex:OrgPolicy (NOT f:AccessPolicy).
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:orgDenyUsers
                    rdf:type    ex:OrgPolicy ;
                    f:action    f:view ;
                    f:onClass   ex:User ;
                    f:allow     false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M custom-typed policy");

    let data_id = "test/cross-ledger-filter/custom-included/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:name": "Alice"
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

    // D config: defaultAllow=true so absence-of-deny means visible;
    // policyClass names ex:OrgPolicy *exactly* — the same IRI M
    // typed the policy with.
    let config_iri = config_graph_iri(data_id);
    fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass ex:OrgPolicy .
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
        .expect("seed D config including ex:OrgPolicy");

    let wrapped = fluree
        .db_with_policy(data_id, &GovernanceOptions::default())
        .await
        .expect("db_with_policy");

    let users = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?u",
                "where": {"@id": "?u", "@type": "ex:User"}
            }),
        )
        .await
        .expect("query ex:User");
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld");
    assert_eq!(
        users_jsonld,
        json!([]),
        "policyClass = ex:OrgPolicy must include the custom-typed rule and deny ex:User, got {users_jsonld}"
    );
}

/// Filter exclusion: the same custom-class policy in M is *not*
/// enforced when D's `f:policyClass` config names a different class.
/// Exact IRI matching, no subclass entailment — `f:AccessPolicy` does
/// not subsume `ex:OrgPolicy`, even though both are policy types.
#[tokio::test]
async fn custom_class_policy_skipped_when_data_ledger_omits_class() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-filter/custom-omitted/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/custom-policies";

    // Identical M setup to the previous test — custom-typed policy.
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:orgDenyUsers
                    rdf:type    ex:OrgPolicy ;
                    f:action    f:view ;
                    f:onClass   ex:User ;
                    f:allow     false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M custom-typed policy");

    let data_id = "test/cross-ledger-filter/custom-omitted/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:name": "Alice"
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

    // D config: defaultAllow=true; policyClass names f:AccessPolicy,
    // which is NOT what M's policy is typed as. The wire restriction
    // must be filtered out at translation time and no deny applies.
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
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass f:AccessPolicy .
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
        .expect("seed D config naming f:AccessPolicy");

    let wrapped = fluree
        .db_with_policy(data_id, &GovernanceOptions::default())
        .await
        .expect("db_with_policy");

    let users = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?u",
                "where": {"@id": "?u", "@type": "ex:User"}
            }),
        )
        .await
        .expect("query ex:User");
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld");
    // alice MUST be visible — the policy was typed ex:OrgPolicy, the
    // config asked only for f:AccessPolicy, the filter rejects it,
    // defaultAllow=true governs and the data is returned. If the
    // filter were absent or subsumptive, alice would be hidden.
    let rendered = users_jsonld.to_string();
    assert!(
        rendered.contains("alice"),
        "policyClass = f:AccessPolicy must NOT pull in ex:OrgPolicy-typed rules; \
         alice should remain visible, got {users_jsonld}"
    );
}

/// A configured cross-ledger `f:policySource` whose `f:policyClass`
/// selects **zero** model-ledger rules must NOT collapse to a root
/// (unrestricted) context. With `defaultAllow=false` the empty
/// restriction set has to deny — a configured policy source that
/// happens to match no rules is still "policy is in effect," not
/// "no policy." Regression for the fail-open where an empty
/// cross-ledger restriction set on an anonymous request made
/// `is_root=true` and dropped enforcement entirely.
#[tokio::test]
async fn empty_cross_ledger_restrictions_fail_closed_under_default_deny() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-filter/empty-deny/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/empty-policies";

    // M holds a custom-typed (ex:OrgPolicy) rule — nothing typed
    // f:AccessPolicy lives here.
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:orgDenyUsers
                    rdf:type    ex:OrgPolicy ;
                    f:action    f:view ;
                    f:onClass   ex:User ;
                    f:allow     false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M custom-typed policy");

    let data_id = "test/cross-ledger-filter/empty-deny/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:name": "Alice"
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

    // D config: defaultAllow=false; NO f:policyClass at all. For an
    // anonymous request the resolver applies its default {f:AccessPolicy}
    // filter, which matches NONE of M's rules (typed ex:OrgPolicy), so
    // the restriction set is empty — but `effective_opts.policy_class`
    // stays None, so nothing marks the request as policy-bearing except
    // the configured cross-ledger source itself. The built context must
    // still enforce (fall back to defaultAllow=false) rather than
    // treating the empty set as root.
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
                <urn:cfg:policy> f:defaultAllow false .
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
        .expect("seed D config with cross-ledger source, no policyClass");

    let wrapped = fluree
        .db_with_policy(data_id, &GovernanceOptions::default())
        .await
        .expect("db_with_policy");

    let users = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?u",
                "where": {"@id": "?u", "@type": "ex:User"}
            }),
        )
        .await
        .expect("query ex:User");
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld");
    // alice MUST be hidden: the cross-ledger source is configured, the
    // class filter selected no rules, and defaultAllow=false denies.
    // If the empty restriction set collapsed to root, alice would leak.
    assert_eq!(
        users_jsonld,
        json!([]),
        "configured cross-ledger source with an empty restriction set must \
         fail closed under defaultAllow=false, got {users_jsonld}"
    );
}

/// Baseline: direct `f:AccessPolicy` typing is the canonical policy
/// class, and a configuration that names it enforces the rule. This
/// is the same shape as the data-ledger-deny test at the top of the
/// file, but with `defaultAllow=true` and a non-deny query so the
/// assertion isolates "rule applied" from "default policy applied."
#[tokio::test]
async fn baseline_access_policy_class_enforced() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-filter/baseline/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/access-policies";

    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
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
        "
        ))
        .execute()
        .await
        .expect("seed M baseline policy");

    let data_id = "test/cross-ledger-filter/baseline/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:name": "Alice"
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

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
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass f:AccessPolicy .
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
        .expect("seed D config with f:AccessPolicy class");

    let wrapped = fluree
        .db_with_policy(data_id, &GovernanceOptions::default())
        .await
        .expect("db_with_policy");

    let users = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?u",
                "where": {"@id": "?u", "@type": "ex:User"}
            }),
        )
        .await
        .expect("query ex:User");
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld");
    assert_eq!(
        users_jsonld,
        json!([]),
        "policyClass = f:AccessPolicy must enforce M's deny on ex:User, got {users_jsonld}"
    );
}

/// When D's config sets `f:policySource` cross-ledger but omits
/// `f:policyClass` entirely, the filter must default to
/// `{f:AccessPolicy}` — NOT "every rule in M's policy graph." A
/// model graph containing both an `f:AccessPolicy` and an
/// `ex:OrgPolicy` rule must enforce only the former; the operator
/// has to opt into custom-class enforcement explicitly.
#[tokio::test]
async fn omitted_policy_class_defaults_to_access_policy_only() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-filter/default-class/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/mixed-policies";

    // M holds two policies: one canonical, one custom-typed. Both
    // are deny-on-class — the canonical one targets ex:User, the
    // custom one targets ex:Doc. The custom one MUST NOT be picked
    // up under default-class filtering.
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:denyUsers
                    rdf:type    f:AccessPolicy ;
                    f:action    f:view ;
                    f:onClass   ex:User ;
                    f:allow     false .

                ex:orgDenyDocs
                    rdf:type    ex:OrgPolicy ;
                    f:action    f:view ;
                    f:onClass   ex:Doc ;
                    f:allow     false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M with mixed policy classes");

    let data_id = "test/cross-ledger-filter/default-class/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User"},
                    {"@id": "ex:doc1", "@type": "ex:Doc"}
                ]
            }),
        )
        .await
        .unwrap();
    let data = r1.ledger;

    // D config: defaultAllow=true; no f:policyClass set at all;
    // f:policySource points at M.
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
                <urn:cfg:policy> f:defaultAllow true .
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
        .expect("seed D config (no policy_class)");

    let wrapped = fluree
        .db_with_policy(data_id, &GovernanceOptions::default())
        .await
        .expect("db_with_policy");

    // ex:User: canonical f:AccessPolicy rule must enforce → empty.
    let users = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?u",
                "where": {"@id": "?u", "@type": "ex:User"}
            }),
        )
        .await
        .expect("query users");
    let users_jsonld = users.to_jsonld(&wrapped.snapshot).expect("jsonld users");
    assert_eq!(
        users_jsonld,
        json!([]),
        "default f:AccessPolicy filter must enforce the canonical-typed rule; got {users_jsonld}"
    );

    // ex:Doc: only the custom-typed ex:OrgPolicy rule denies it. With
    // no f:policyClass set, the default filter is {f:AccessPolicy},
    // so the custom rule is dropped at translation. defaultAllow=true
    // governs and ex:doc1 must remain visible.
    let docs = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": "?d",
                "where": {"@id": "?d", "@type": "ex:Doc"}
            }),
        )
        .await
        .expect("query docs");
    let docs_rendered = docs
        .to_jsonld(&wrapped.snapshot)
        .expect("jsonld docs")
        .to_string();
    assert!(
        docs_rendered.contains("doc1"),
        "omitted f:policyClass must NOT pick up ex:OrgPolicy-typed rules; \
         ex:doc1 should remain visible, got {docs_rendered}"
    );
}

/// Identity + cross-ledger with a policy class available (from D's
/// config): rules load from M via the class filter and the identity
/// binds `?$identity` against D, driving M's f:query rules. The
/// owner-only view rule hides bob's email from aliceIdentity while
/// alice's own email stays visible — proving both that the request
/// no longer fails closed and that the binding is live.
#[tokio::test]
async fn identity_with_policy_class_engages_cross_ledger_rules() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-e2e/id-bind-model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/id-bind-policies";
    // Full IRIs inside f:query — it executes against D.
    let owner_query =
        r#"{"where": {"@id": "?$identity", "http://example.org/ns/user": {"@id": "?$this"}}}"#;
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:ownerEmailOnly
                    rdf:type     f:AccessPolicy ;
                    f:required   true ;
                    f:onProperty ex:email ;
                    f:action     f:view ;
                    f:query      """{owner_query}""" .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M owner-only view rule");

    let data_id = "test/cross-ledger-e2e/id-bind-data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice", "ex:email": "alice@flur.ee"},
                    {"@id": "ex:bob",   "@type": "ex:User", "ex:name": "Bob",   "ex:email": "bob@flur.ee"},
                    {"@id": "ex:aliceIdentity", "ex:user": {"@id": "ex:alice"}}
                ]
            }),
        )
        .await
        .expect("seed D users + identity");
    let data = r1.ledger;

    let config_iri = config_graph_iri(data_id);
    fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass f:AccessPolicy .
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
        .expect("seed D cross-ledger config with policyClass");

    // Identity-carrying request — previously a hard config error.
    let opts = GovernanceOptions {
        identity: Some("http://example.org/ns/aliceIdentity".into()),
        ..Default::default()
    };
    let wrapped = fluree
        .db_with_policy(data_id, &opts)
        .await
        .expect("identity + config policyClass must not fail closed");

    let emails = fluree
        .query(
            &wrapped,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": ["?who", "?email"],
                "where": {"@id": "?who", "ex:email": "?email"}
            }),
        )
        .await
        .expect("query emails under cross-ledger identity binding");
    let rendered = emails
        .to_jsonld(&wrapped.snapshot)
        .expect("jsonld")
        .to_string();
    assert!(
        rendered.contains("alice@flur.ee"),
        "aliceIdentity must see its own user's email via ?$identity binding, got {rendered}"
    );
    assert!(
        !rendered.contains("bob@flur.ee"),
        "aliceIdentity must NOT see bob's email — M's owner-only rule must filter it, got {rendered}"
    );
}

/// Combining `opts.identity` with cross-ledger `f:policySource` is
/// a fail-closed config error when no policy class is available
/// anywhere (request or config): the identity is bind-only and can't
/// select rules, so the operator must name which classes govern.
#[tokio::test]
async fn cross_ledger_plus_identity_mode_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-e2e/id-model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/id-policies";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:rule1 rdf:type f:AccessPolicy ; f:action f:view ; f:allow true .
            }}
        "
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

    let opts = GovernanceOptions {
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

/// Two class-targeted `f:query` rules in M govern the same class in D.
/// Whichever the materializer loads first, an identity that satisfies
/// either rule must see the instance, and the answer must be stable across
/// re-materializations (each M commit produces a fresh governance-cache
/// key). Before the fix, the evaluator denied on the first failing targeted
/// f:query and the load order came from a `HashSet`, so roughly half of the
/// M versions denied one identity or the other.
#[tokio::test]
async fn multiple_targeted_query_rules_any_allow_grants_across_materializations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ctx = json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"});

    let model_id = "test/cross-ledger-e2e/multi-query-model:main";
    let mut model = fluree
        .insert(
            genesis_ledger(&fluree, model_id),
            &json!({"@context": ctx, "@graph": [
                {"@id": "ex:one-hop", "@type": "f:AccessPolicy", "f:action": {"@id": "f:view"},
                 "f:onClass": [{"@id": "ex:Line"}],
                 "f:query": "{\"where\":{\"@id\":\"?$this\",\"http://example.org/supplier\":{\"@id\":\"?$identity\"}}}"},
                {"@id": "ex:two-hop", "@type": "f:AccessPolicy", "f:action": {"@id": "f:view"},
                 "f:onClass": [{"@id": "ex:Line"}],
                 "f:query": "{\"where\":{\"@id\":\"?$this\",\"http://example.org/supplier\":{\"@id\":\"?rec\",\"http://example.org/canonical\":{\"@id\":\"?$identity\"}}}}"}
            ]}),
        )
        .await
        .expect("seed M rules")
        .ledger;

    let data_id = "test/cross-ledger-e2e/multi-query-data:main";
    let data = fluree
        .insert(
            genesis_ledger(&fluree, data_id),
            &json!({"@context": ctx, "@graph": [
                {"@id": "ex:acme", "@type": "ex:Supplier"},
                {"@id": "ex:acme-rome", "@type": "ex:SupplierRecord", "ex:canonical": {"@id": "ex:acme"}},
                {"@id": "ex:line1", "@type": "ex:Line", "ex:supplier": {"@id": "ex:acme-rome"}},
                {"@id": "ex:line2", "@type": "ex:Line", "ex:supplier": {"@id": "ex:nobody"}}
            ]}),
        )
        .await
        .expect("seed D")
        .ledger;
    let config_iri = config_graph_iri(data_id);
    fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow false ; f:policyClass f:AccessPolicy ;
                                 f:policySource <urn:cfg:ref> .
                <urn:cfg:ref> rdf:type f:GraphRef ; f:graphSource <urn:cfg:src> .
                <urn:cfg:src> f:ledger <{model_id}> ; f:graphSelector f:defaultGraph .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");

    let lines = |identity: &str| {
        let opts = GovernanceOptions {
            identity: Some(identity.into()),
            ..Default::default()
        };
        let fluree = &fluree;
        async move {
            let wrapped = fluree
                .db_with_policy(data_id, &opts)
                .await
                .expect("db_with_policy");
            fluree
                .query(
                    &wrapped,
                    &json!({"@context": {"ex": "http://example.org/"}, "select": ["?l"],
                            "where": {"@id": "?l", "@type": "ex:Line"}}),
                )
                .await
                .expect("query")
                .to_jsonld(&wrapped.snapshot)
                .expect("jsonld")
        }
    };

    for round in 0..16 {
        // Unrelated M commit: new resolved_t, fresh materialization.
        model = fluree
            .insert(
                model,
                &json!({"@context": ctx, "@id": "ex:tick", "ex:n": round}),
            )
            .await
            .expect("tick")
            .ledger;
        assert_eq!(
            lines("http://example.org/acme").await,
            json!([["ex:line1"]]),
            "round {round}: two-hop rule must grant acme"
        );
        assert_eq!(
            lines("http://example.org/acme-rome").await,
            json!([["ex:line1"]]),
            "round {round}: one-hop rule must grant acme-rome"
        );
    }
}
