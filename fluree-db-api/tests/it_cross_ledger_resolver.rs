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

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact;
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

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact;
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

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact;
    let r = &wire.restrictions[0];

    // Both types must be present (order-independent).
    assert_eq!(r.policy_types.len(), 2);
    assert!(r.policy_types.contains(&"https://ns.flur.ee/db#AccessPolicy".to_string()));
    assert!(r.policy_types.contains(&"http://example.org/ns/OrgPolicy".to_string()));
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

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact;
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

    let GovernanceArtifact::PolicyRules(wire) = &resolved.artifact;
    assert!(
        wire.restrictions.is_empty(),
        "non-policy data in graph must yield zero restrictions, got {}",
        wire.restrictions.len()
    );
}
