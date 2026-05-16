//! Cross-ledger resolver integration tests.
//!
//! End-to-end coverage that the Phase 1a resolver actually reads
//! policy rules from a *different* ledger than the data ledger and
//! returns them in term-neutral form. The translator-to-PolicySet
//! step (slice 5) and enforcement against a real query (slice 9) are
//! covered separately; this file's job is the wire-artifact contract.

mod support;

use fluree_db_api::cross_ledger::{
    resolve_graph_ref, ArtifactKind, GovernanceArtifact, ResolveCtx,
};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::ledger_config::GraphSourceRef;
use fluree_db_policy::TargetMode;
use serde_json::json;
use support::genesis_ledger;

/// Build a cross-ledger GraphSourceRef.
fn cross_ref(ledger: &str, graph: &str) -> GraphSourceRef {
    GraphSourceRef {
        ledger: Some(ledger.into()),
        graph_selector: Some(graph.into()),
        at_t: None,
        trust_policy: None,
        rollback_guard: None,
    }
}

/// Resolve a cross-ledger ref to a model ledger M holding one
/// f:AccessPolicy in a named graph, against an unrelated data
/// ledger D on the same Fluree instance. The wire artifact returned
/// must be IRI-form, attributed to M, and capture the rule's targets
/// in M's IRI space — D's term space is untouched.
#[tokio::test]
async fn resolves_policy_graph_from_model_ledger_into_term_neutral_wire() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Model ledger M: holds one policy in a named graph.
    let model_id = "test/cross-ledger/model:main";
    let model = genesis_ledger(&fluree, model_id);

    // Use TriG to drop the policy directly into a named graph (the
    // simplest way to populate a non-default graph in one transaction).
    let policy_graph_iri = "http://example.org/policy-graph";
    let trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{policy_graph_iri}> {{
            ex:denyUsers
                rdf:type        f:AccessPolicy ;
                f:action        f:view ;
                f:onClass       ex:User ;
                f:allow         false .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write policy into model ledger named graph");

    // Data ledger D: an unrelated ledger on the same instance. Its
    // contents don't matter for the resolver — only the resolver's
    // ability to find and read M without touching D.
    let data_id = "test/cross-ledger/data:main";
    let _data = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let graph_ref = cross_ref(model_id, policy_graph_iri);

    let resolved = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx)
        .await
        .expect("cross-ledger resolution");

    // Origin must point at M, not D.
    assert_eq!(resolved.model_ledger_id, model_id);
    assert_eq!(resolved.graph_iri, policy_graph_iri);
    assert!(resolved.resolved_t > 0, "must resolve to a real commit t");

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        panic!("expected PolicyRules artifact for ArtifactKind::PolicyRules, got {:?}", resolved.artifact);
    };
    assert_eq!(wire.origin.model_ledger_id, model_id);
    assert_eq!(wire.origin.graph_iri, policy_graph_iri);
    assert_eq!(wire.origin.resolved_t, resolved.resolved_t);

    // Exactly one restriction — `ex:denyUsers`.
    assert_eq!(wire.restrictions.len(), 1, "expected one wire restriction");
    let r = &wire.restrictions[0];

    // The restriction's identifying IRI must round-trip from M's
    // term space as a full IRI, not a curie.
    assert!(
        r.id.contains("denyUsers"),
        "restriction id {:?} should reference denyUsers",
        r.id
    );

    // Target mode is OnClass and the class IRI is in M's IRI space.
    assert_eq!(r.target_mode, TargetMode::OnClass);
    assert!(r.targets.is_empty());
    assert_eq!(
        r.for_classes,
        vec!["http://example.org/ns/User".to_string()],
        "for_classes must hold the expanded ex:User IRI, not a curie or Sid"
    );
    assert!(r.class_policy);

    // Effect = Deny, action filtered correctly via the wire mirror.
    assert!(
        matches!(r.value, fluree_db_policy::WirePolicyValue::Deny),
        "value must be Deny, got {:?}",
        r.value
    );

    // policy_types must carry the rule's rdf:type set in IRI form —
    // this is what the translation step intersects against the data
    // ledger's configured f:policyClass set.
    assert_eq!(
        r.policy_types,
        vec!["https://ns.flur.ee/db#AccessPolicy".to_string()],
        "policy_types must record the rule's rdf:type IRIs, got {:?}",
        r.policy_types
    );

    // The wire artifact is also memoized — a second resolve of the
    // same (M, graph, t) tuple within the same context must return
    // the same Arc without re-materializing.
    let resolved2 = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx)
        .await
        .expect("second resolve");
    assert!(
        std::sync::Arc::ptr_eq(&resolved, &resolved2),
        "second resolve must be a memo hit, not a fresh materialization"
    );
}

/// Structural detection: a subject that has `f:allow` but declares a
/// **non-`f:AccessPolicy`** rdf:type is still materialized. The
/// translation step (slice 5) is where the data ledger's configured
/// f:policyClass set gets intersected against the rule's policy_types.
/// The wire materializer must not pre-filter by class.
#[tokio::test]
async fn structural_detection_picks_up_custom_typed_policies() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger/custom-typed:main";
    let model = genesis_ledger(&fluree, model_id);

    let policy_graph_iri = "http://example.org/custom-policy-graph";
    // Policy is typed as ex:OrgPolicy, NOT f:AccessPolicy. It has
    // f:allow set so structural detection must include it. policy_types
    // must record `ex:OrgPolicy` so slice 5 can filter appropriately.
    let trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{policy_graph_iri}> {{
            ex:orgRule
                rdf:type    ex:OrgPolicy ;
                f:action    f:view ;
                f:onClass   ex:Document ;
                f:allow     false .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write custom-typed policy");

    let data_id = "test/cross-ledger/custom-typed-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let graph_ref = cross_ref(model_id, policy_graph_iri);

    let resolved = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx)
        .await
        .expect("cross-ledger resolution with custom-typed policy");

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        panic!("expected PolicyRules artifact for ArtifactKind::PolicyRules, got {:?}", resolved.artifact);
    };
    assert_eq!(
        wire.restrictions.len(),
        1,
        "structural detection must pick up the f:allow-bearing subject regardless of class"
    );
    let r = &wire.restrictions[0];
    assert!(r.id.contains("orgRule"));
    assert_eq!(
        r.policy_types,
        vec!["http://example.org/ns/OrgPolicy".to_string()],
        "policy_types must record ex:OrgPolicy, NOT f:AccessPolicy: got {:?}",
        r.policy_types
    );
}

/// A policy that declares **multiple** rdf:type values must surface
/// every type in policy_types. The translation step uses set
/// intersection against the data ledger's policy_class config; a
/// rule typed as both `f:AccessPolicy` and `ex:OrgPolicy` must be
/// selectable via either configured class.
#[tokio::test]
async fn multiple_rdf_types_are_all_captured_in_policy_types() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger/multi-typed:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/multi-typed-graph";
    let trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{policy_graph_iri}> {{
            ex:dualTyped
                rdf:type    f:AccessPolicy , ex:OrgPolicy ;
                f:action    f:view ;
                f:onClass   ex:Doc ;
                f:allow     true .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write dual-typed policy");

    let data_id = "test/cross-ledger/multi-typed-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let resolved = resolve_graph_ref(
        &cross_ref(model_id, policy_graph_iri),
        ArtifactKind::PolicyRules,
        &mut ctx,
    )
    .await
    .expect("dual-typed resolves");

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        panic!("expected PolicyRules artifact for ArtifactKind::PolicyRules, got {:?}", resolved.artifact);
    };
    let r = &wire.restrictions[0];

    // Both types must be present (order-independent).
    assert_eq!(r.policy_types.len(), 2);
    assert!(r.policy_types.contains(&"https://ns.flur.ee/db#AccessPolicy".to_string()));
    assert!(r.policy_types.contains(&"http://example.org/ns/OrgPolicy".to_string()));
}

/// Cross-ledger schema materialization picks up the whitelisted
/// ontology axiom triples in M's schema graph and projects them
/// into IRI form. Reasoner-level enforcement is exercised
/// separately; this test pins the wire artifact contract — the
/// materializer hits the right whitelist, decodes M's Sids to
/// IRIs that survive transport, and origin metadata reflects M.
#[tokio::test]
async fn cross_ledger_schema_materializes_whitelisted_axioms() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger/schema:main";
    let model = genesis_ledger(&fluree, model_id);
    let schema_graph_iri = "http://example.org/ontology/core";

    // M's schema graph: a small class hierarchy + a property
    // hierarchy that should both surface in the wire artifact.
    let trig = format!(
        r#"
        @prefix owl:  <http://www.w3.org/2002/07/owl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{schema_graph_iri}> {{
            ex:Animal  rdf:type           owl:Class .
            ex:Dog     rdf:type           owl:Class ;
                       rdfs:subClassOf    ex:Animal .

            ex:knows   rdf:type           owl:ObjectProperty .
            ex:friend  rdf:type           owl:ObjectProperty ;
                       rdfs:subPropertyOf ex:knows .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed M schema graph");

    let data_id = "test/cross-ledger/schema-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let resolved = resolve_graph_ref(
        &cross_ref(model_id, schema_graph_iri),
        ArtifactKind::SchemaClosure,
        &mut ctx,
    )
    .await
    .expect("cross-ledger schema resolution");

    assert_eq!(resolved.model_ledger_id, model_id);
    assert_eq!(resolved.graph_iri, schema_graph_iri);

    let GovernanceArtifact::SchemaClosure(wire) = &resolved.artifact else {
        panic!(
            "expected SchemaClosure artifact for ArtifactKind::SchemaClosure, \
             got {:?}",
            resolved.artifact
        );
    };
    assert_eq!(wire.origin.model_ledger_id, model_id);
    assert_eq!(wire.origin.graph_iri, schema_graph_iri);

    // The wire must include the rdfs:subClassOf axiom in IRI form.
    let triples = &wire.triples;
    let subclass_axiom = triples.iter().find(|t| {
        t.s == "http://example.org/ns/Dog"
            && t.p == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
            && t.o == "http://example.org/ns/Animal"
    });
    assert!(
        subclass_axiom.is_some(),
        "wire must include ex:Dog rdfs:subClassOf ex:Animal in IRI form, got: {triples:?}"
    );

    // ...and the rdfs:subPropertyOf axiom.
    let subproperty_axiom = triples.iter().find(|t| {
        t.s == "http://example.org/ns/friend"
            && t.p == "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
            && t.o == "http://example.org/ns/knows"
    });
    assert!(
        subproperty_axiom.is_some(),
        "wire must include ex:friend rdfs:subPropertyOf ex:knows, got: {triples:?}"
    );

    // ...and the rdf:type owl:Class declarations.
    let class_decl = triples.iter().find(|t| {
        t.s == "http://example.org/ns/Animal"
            && t.p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            && t.o == "http://www.w3.org/2002/07/owl#Class"
    });
    assert!(
        class_decl.is_some(),
        "wire must include ex:Animal rdf:type owl:Class, got: {triples:?}"
    );
}

/// A cross-ledger schema reference against a model ledger that
/// holds no schema triples (or no whitelisted axioms) yields a wire
/// artifact with zero triples — not an error.
#[tokio::test]
async fn cross_ledger_schema_empty_graph_yields_empty_wire() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger/schema-empty:main";
    let model = genesis_ledger(&fluree, model_id);

    // Non-schema data in a named graph so the graph EXISTS.
    let graph_iri = "http://example.org/non-schema";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix ex: <http://example.org/ns/> .

            GRAPH <{graph_iri}> {{
                ex:alice ex:name "Alice" .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed non-schema data");

    let data_id = "test/cross-ledger/schema-empty-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let resolved = resolve_graph_ref(
        &cross_ref(model_id, graph_iri),
        ArtifactKind::SchemaClosure,
        &mut ctx,
    )
    .await
    .expect("cross-ledger schema resolution against non-schema graph");

    let GovernanceArtifact::SchemaClosure(wire) = &resolved.artifact else {
        panic!("expected SchemaClosure");
    };
    assert!(
        wire.triples.is_empty(),
        "non-schema data in graph must yield zero whitelisted triples"
    );
}

/// A canonically-typed policy (`rdf:type f:AccessPolicy`) that has
/// NEITHER `f:allow` nor `f:query` is treated as `Deny` by the same-
/// ledger materializer (with a warning) — so the cross-ledger
/// materializer must surface the same subject. Structural detection
/// on `f:allow ∪ f:query` alone would miss it; the additional
/// `rdf:type f:AccessPolicy` scan closes that gap.
#[tokio::test]
async fn missing_effect_on_typed_policy_is_picked_up_as_deny() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger/missing-effect:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/missing-effect-graph";

    // Policy is canonically typed but declares no f:allow / f:query.
    // It does declare f:onClass so target_mode is OnClass — without
    // the rdf:type-driven structural scan this subject would never
    // be discovered, and the deny would silently disappear cross-
    // ledger while still applying same-ledger.
    let trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{policy_graph_iri}> {{
            ex:incompleteDeny
                rdf:type    f:AccessPolicy ;
                f:action    f:view ;
                f:onClass   ex:User .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write canonical-typed effect-less policy");

    let data_id = "test/cross-ledger/missing-effect-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let resolved = resolve_graph_ref(
        &cross_ref(model_id, policy_graph_iri),
        ArtifactKind::PolicyRules,
        &mut ctx,
    )
    .await
    .expect("missing-effect typed policy resolves");

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        panic!("expected PolicyRules artifact for ArtifactKind::PolicyRules, got {:?}", resolved.artifact);
    };
    assert_eq!(
        wire.restrictions.len(),
        1,
        "canonical-typed policy must be surfaced even without f:allow / f:query"
    );
    let r = &wire.restrictions[0];
    assert!(r.id.contains("incompleteDeny"));
    assert!(
        matches!(r.value, fluree_db_policy::WirePolicyValue::Deny),
        "missing-effect on canonical type must materialize as Deny (mirroring same-ledger); got {:?}",
        r.value
    );
}

/// Distinct-namespace-codes canary.
///
/// The design doc treats this as a mandatory cross-cutting test:
/// term translation must operate on IRIs end-to-end, never on M's
/// internal Sids. If a future change makes the wire form leak Sids
/// from M's namespace map, this test would fail — D's lookup of
/// the policy's class IRI would land on a different Sid than M
/// embedded, and the enforcement would silently miss.
///
/// To make the canary non-trivial, D is pre-seeded with an
/// unrelated namespace before its policy-relevant data lands, so
/// D's `http://example.org/ns/` prefix is registered at a
/// different `ns_code` than M's. The test asserts the codes
/// actually diverge, then runs the end-to-end enforcement and
/// confirms M's deny rule applies against D's data.
#[tokio::test]
async fn distinct_namespace_codes_canary_term_translation_still_works() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Build M first with the policy graph using ex:User.
    let model_id = "test/cross-ledger/canary/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/canary-policy";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
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
        ))
        .execute()
        .await
        .expect("seed M");

    // D pre-seeded with an UNRELATED namespace before its
    // ex:-prefixed data lands. The unrelated insert allocates
    // an ns_code first, so when ex:User finally lands, its prefix
    // ends up at a different code than M assigned.
    let data_id = "test/cross-ledger/canary/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"unrelated": "http://unrelated.example.org/v1/"},
                "@graph": [
                    {"@id": "unrelated:seed-a", "unrelated:tag": "first"},
                    {"@id": "unrelated:seed-b", "unrelated:tag": "second"}
                ]
            }),
        )
        .await
        .expect("seed unrelated namespace into D");
    let data = r1.ledger;

    let r2 = fluree
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
        .expect("seed ex:User data into D");
    let data = r2.ledger;

    // Inspect both ledgers' namespace maps. If allocation happens
    // to align (e.g., a future change to ns_code assignment) the
    // canary degenerates into a regular enforcement test rather
    // than asserting divergence — flag that in the message so a
    // failure here is actionable.
    let m_snapshot = fluree.db(model_id).await.expect("open M");
    let d_snapshot = &data.snapshot;
    let m_user_sid = m_snapshot
        .snapshot
        .encode_iri("http://example.org/ns/User")
        .expect("ex:User should resolve in M");
    let d_user_sid = d_snapshot
        .encode_iri("http://example.org/ns/User")
        .expect("ex:User should resolve in D");

    // Hard assertion: a CI run where these happen to align would
    // report the canary green without actually exercising the
    // cross-namespace-code path. The seed order above (M seeded
    // with ex: first, D seeded with an unrelated namespace before
    // ex:) is constructed specifically to force divergence — if a
    // future ns-allocation change ever makes these collide, the
    // test needs to be re-seeded, not silently tolerated.
    assert_ne!(
        m_user_sid.namespace_code, d_user_sid.namespace_code,
        "canary seeding must produce distinct ns_codes for ex:User on M vs D \
         (got M={:?}, D={:?}); adjust the seed order until they diverge",
        m_user_sid.namespace_code, d_user_sid.namespace_code,
    );

    // Write D's cross-ledger config pointing at M.
    let config_iri = format!("urn:fluree:{data_id}#config");
    let r3 = fluree
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
        .expect("seed D cross-ledger config");
    let _ = r3;

    // End-to-end: query D for ex:User. The materializer produced M's
    // class IRI as a string ("http://example.org/ns/User"); the
    // translator must re-intern that against D's namespace map and
    // arrive at d_user_sid. If translation broke (Sids leaked from M
    // into D's evaluation), the deny would miss and alice would be
    // visible.
    let wrapped = fluree
        .db_with_policy(data_id, &fluree_db_api::QueryConnectionOptions::default())
        .await
        .expect("db_with_policy under cross-ledger");

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
        "cross-ledger deny must apply against D's data even when D's \
         ns_code for the class IRI differs from M's; got {users_jsonld}"
    );
}

/// Single-resolution-t.
///
/// Within one request (one `ResolveCtx`), every cross-ledger
/// reference to the same model ledger M must use the same
/// `resolved_t` even if M advances in the middle. The design doc
/// makes this property mandatory so policy / shapes / schema can
/// never disagree about which version of M they're enforcing for
/// a given request.
#[tokio::test]
async fn single_resolution_t_is_stable_within_a_request() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger/stable-t/model:main";
    let model = genesis_ledger(&fluree, model_id);

    // M holds two policy graphs from the start so both resolutions
    // can succeed without M needing to advance for them to exist.
    let graph_a_iri = "http://example.org/policy-a";
    let graph_b_iri = "http://example.org/policy-b";
    let r1 = fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{graph_a_iri}> {{
                ex:ruleA rdf:type f:AccessPolicy ; f:action f:view ; f:allow true .
            }}
            GRAPH <{graph_b_iri}> {{
                ex:ruleB rdf:type f:AccessPolicy ; f:action f:view ; f:allow false .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M with two policy graphs");
    let model = r1.ledger;

    let data_id = "test/cross-ledger/stable-t/data:main";
    let _ = genesis_ledger(&fluree, data_id);

    // Single ResolveCtx = single request. Resolve A, advance M, then
    // resolve B. B's resolved_t must equal A's — captured once at the
    // first reference, not re-read between resolutions.
    let mut ctx = ResolveCtx::new(data_id, &fluree);

    let resolved_a = resolve_graph_ref(
        &cross_ref(model_id, graph_a_iri),
        ArtifactKind::PolicyRules,
        &mut ctx,
    )
    .await
    .expect("resolve A");
    let t_at_first_resolution = resolved_a.resolved_t;

    // Advance M between the two resolutions. Without single-
    // resolution-t, the second resolve would pick up this new head.
    fluree
        .insert(
            model,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:filler",
                "ex:val": "advance M"
            }),
        )
        .await
        .expect("advance M's commit_t");

    let resolved_b = resolve_graph_ref(
        &cross_ref(model_id, graph_b_iri),
        ArtifactKind::PolicyRules,
        &mut ctx,
    )
    .await
    .expect("resolve B");

    assert_eq!(
        resolved_b.resolved_t, t_at_first_resolution,
        "second resolution within the same request must reuse the captured \
         resolved_t (got {} after advancing M; first capture was {})",
        resolved_b.resolved_t, t_at_first_resolution,
    );

    // Verify resolved_ts cached the head exactly once. A new
    // request would re-capture against the advanced M; the cache
    // is per-request, not per-instance.
    assert_eq!(ctx.resolved_ts.len(), 1);
    assert_eq!(
        ctx.resolved_ts.get(model_id),
        Some(&t_at_first_resolution),
        "resolved_ts must store the captured t once per canonical model id"
    );

    // Sanity: a new ResolveCtx against the same Fluree DOES re-capture
    // against M's current head.
    let mut fresh_ctx = ResolveCtx::new(data_id, &fluree);
    let resolved_a_fresh = resolve_graph_ref(
        &cross_ref(model_id, graph_a_iri),
        ArtifactKind::PolicyRules,
        &mut fresh_ctx,
    )
    .await
    .expect("resolve A in fresh ctx");
    assert!(
        resolved_a_fresh.resolved_t > t_at_first_resolution,
        "a fresh request should see M's advanced head (got {}, expected > {})",
        resolved_a_fresh.resolved_t, t_at_first_resolution
    );
}

/// The per-instance governance cache makes the same (M, graph, t)
/// reusable across requests and across every data ledger on the
/// instance. Two independent ResolveCtxs (simulating two requests)
/// resolving the same key must end up sharing one Arc — the second
/// resolution is a cache hit and never re-materializes.
#[tokio::test]
async fn governance_cache_short_circuits_repeated_resolutions_across_contexts() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger/cache:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/cache-policy";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:rule rdf:type f:AccessPolicy ; f:action f:view ; f:allow true .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M");

    // Two data ledgers — same Fluree instance, different identities.
    // Both resolve the same (M, policy_graph, t). The second one
    // must hit the per-instance governance cache.
    let data_a = "test/cross-ledger/cache/d-a:main";
    let _ = genesis_ledger(&fluree, data_a);
    let data_b = "test/cross-ledger/cache/d-b:main";
    let _ = genesis_ledger(&fluree, data_b);

    let graph_ref = cross_ref(model_id, policy_graph_iri);

    assert_eq!(
        fluree.governance_cache().entry_count(),
        0,
        "cache must start empty"
    );

    let mut ctx_a = ResolveCtx::new(data_a, &fluree);
    let resolved_a = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx_a)
        .await
        .expect("first resolve populates cache");

    // After the first resolve, the cache must have exactly one
    // entry for this (kind, M, graph, t). Moka's entry_count is
    // best-effort; allow the rare zero-count race by also asserting
    // a direct get hit.
    let _ = fluree.governance_cache(); // touch
    // (sync_for_test would be ideal but Moka doesn't expose one;
    // the get below is the load-bearing check.)

    let mut ctx_b = ResolveCtx::new(data_b, &fluree);
    let resolved_b = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx_b)
        .await
        .expect("second resolve hits cache");

    assert!(
        std::sync::Arc::ptr_eq(&resolved_a, &resolved_b),
        "second resolve in a fresh ResolveCtx must return the cached Arc, \
         not a freshly materialized one"
    );

    // A new context against a third data ledger sees the same hit.
    let data_c = "test/cross-ledger/cache/d-c:main";
    let _ = genesis_ledger(&fluree, data_c);
    let mut ctx_c = ResolveCtx::new(data_c, &fluree);
    let resolved_c = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx_c)
        .await
        .expect("third resolve also a cache hit");
    assert!(std::sync::Arc::ptr_eq(&resolved_a, &resolved_c));
}

/// A cross-ledger ref to a graph IRI that doesn't exist on the model
/// ledger surfaces as GraphMissingAtT — not silently empty, not
/// TranslationFailed. The fail-closed contract names this failure
/// distinctly for audit.
#[tokio::test]
async fn unknown_graph_on_model_ledger_surfaces_graph_missing_at_t() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger/empty-model:main";
    let model = genesis_ledger(&fluree, model_id);

    // Transact something into the default graph so the model has a
    // commit t, but never touch any named graph.
    fluree
        .insert(
            model,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:seed",
                "ex:val": 1
            }),
        )
        .await
        .expect("seed model ledger");

    let data_id = "test/cross-ledger/empty-data:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let graph_ref = cross_ref(model_id, "http://example.org/never-created");

    let err = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx)
        .await
        .expect_err("missing graph on model must fail");

    match err {
        fluree_db_api::cross_ledger::CrossLedgerError::GraphMissingAtT {
            ledger_id,
            graph_iri,
            ..
        } => {
            assert_eq!(ledger_id, model_id);
            assert_eq!(graph_iri, "http://example.org/never-created");
        }
        other => panic!("expected GraphMissingAtT, got {other:?}"),
    }
}

/// A cross-ledger ref where the named graph exists on the model
/// ledger but contains no f:AccessPolicy-typed subjects yields a
/// wire artifact with zero restrictions. This is a valid result —
/// "no policies configured here" is not a failure.
#[tokio::test]
async fn empty_policy_graph_yields_empty_wire_artifact() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger/no-policies:main";
    let model = genesis_ledger(&fluree, model_id);

    // Put NON-policy data in the named graph so the graph exists.
    let graph_iri = "http://example.org/data-only";
    let trig = format!(
        r#"
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{graph_iri}> {{
            ex:alice ex:name "Alice" .
        }}
    "#
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write non-policy data");

    let data_id = "test/cross-ledger/no-policies-d:main";
    let _ = genesis_ledger(&fluree, data_id);

    let mut ctx = ResolveCtx::new(data_id, &fluree);
    let graph_ref = cross_ref(model_id, graph_iri);

    let resolved = resolve_graph_ref(&graph_ref, ArtifactKind::PolicyRules, &mut ctx)
        .await
        .expect("empty graph resolves successfully");

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact else {
        panic!("expected PolicyRules artifact for ArtifactKind::PolicyRules, got {:?}", resolved.artifact);
    };
    assert!(
        wire.restrictions.is_empty(),
        "non-policy data in graph must yield zero restrictions, got {}",
        wire.restrictions.len()
    );
}
