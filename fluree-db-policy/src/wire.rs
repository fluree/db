//! Term-neutral wire form for cross-ledger policy artifacts.
//!
//! See `docs/design/cross-ledger-model-enforcement.md`. The wire form
//! is what a cross-ledger resolver produces and what the per-instance
//! governance-artifact cache stores: IRIs throughout, no Sids. It is
//! shareable across every data ledger that references the same model
//! graph at the same model-t.
//!
//! Translation to a Sid-form [`PolicySet`] happens at request time
//! against the data ledger's term space via
//! [`build_policy_set_from_wire`]. The translation function takes a
//! closure for IRI resolution so this crate stays free of
//! `LedgerSnapshot` references; production passes a closure wrapping
//! `LedgerSnapshot::encode_iri`.

use crate::error::Result;
use crate::index::build_policy_set;
use crate::types::{
    PolicyAction, PolicyQuery, PolicyRestriction, PolicySet, PolicyValue, TargetMode,
};
use fluree_db_core::{IndexStats, Sid};
use std::collections::HashSet;

/// Term-neutral policy artifact materialized from a model ledger.
///
/// Cached by the API layer keyed on `(origin.model_ledger_id,
/// origin.graph_iri, origin.resolved_t)`.
#[derive(Debug, Clone)]
pub struct PolicyArtifactWire {
    /// Provenance for diagnostics and cache key derivation.
    pub origin: WireOrigin,
    /// Policy rules in the order they were read from the model graph.
    pub restrictions: Vec<WireRestriction>,
}

/// Provenance for a wire artifact.
#[derive(Debug, Clone)]
pub struct WireOrigin {
    /// Canonical model ledger id (`NsRecord.ledger_id`), never a
    /// user-typed alias.
    pub model_ledger_id: String,
    /// Graph IRI within the model ledger whose triples produced this
    /// artifact.
    pub graph_iri: String,
    /// Model ledger `t` at which the artifact was materialized.
    pub resolved_t: i64,
}

/// IRI-form mirror of [`PolicyRestriction`].
///
/// `targets` and `for_classes` are `Vec<String>` (not `HashSet`)
/// because ordering is irrelevant to PolicySet construction and Vec
/// serializes more efficiently. They are rebuilt into HashSets during
/// translation.
///
/// `policy_types` carries the rule subject's `rdf:type` IRIs so the
/// translation step can filter by the data ledger's configured
/// `f:policyClass` set via exact IRI intersection. This keeps the
/// cross-ledger cache shareable across every data ledger that
/// references the same model graph — different policy-class
/// configurations don't produce different wire artifacts.
#[derive(Debug, Clone)]
pub struct WireRestriction {
    /// Policy rule IRI (subject in the source graph).
    pub id: String,
    /// Every `rdf:type` value declared on the rule subject in the
    /// source graph (decoded to IRI form). Used by the translator to
    /// intersect with the data ledger's configured `f:policyClass`
    /// set. Phase 1a uses exact IRI matching only; subclass
    /// entailment is a later enhancement.
    pub policy_types: Vec<String>,
    /// Targeting mode (`f:onSubject` / `f:onProperty` / `f:onClass` /
    /// default).
    pub target_mode: TargetMode,
    /// Target IRIs — subjects, properties, or classes per `target_mode`.
    pub targets: Vec<String>,
    /// `f:action`.
    pub action: PolicyAction,
    /// `f:allow` / `f:query` effect.
    pub value: WirePolicyValue,
    /// `f:required` flag.
    pub required: bool,
    /// Optional `f:exMessage`.
    pub message: Option<String>,
    /// `f:onClass` was set (the restriction targets class instances).
    pub class_policy: bool,
    /// Target class IRIs for `f:onClass` policies.
    pub for_classes: Vec<String>,
}

/// IRI-form mirror of [`PolicyValue`].
///
/// `Query` stores its JSON payload verbatim — it is already
/// term-neutral; the policy query executor re-interns IRIs at
/// execution time against the data ledger's term space.
#[derive(Debug, Clone)]
pub enum WirePolicyValue {
    Allow,
    Deny,
    Query(String),
}

/// Translate a wire artifact into a Sid-form [`PolicySet`] against a
/// data ledger's term space.
///
/// `resolve_iri` is the term-translation hook. Production wraps
/// `LedgerSnapshot::encode_iri`. Tests can pass an in-memory stub.
///
/// IRIs that fail to resolve are dropped from their target set: the
/// restriction is still included in the result with the unresolvable
/// targets removed (its `id` remains observable for diagnostics).
/// Whether unresolved IRIs should instead be interned on demand
/// against D is a separate decision that lands with the resolver
/// itself (a later slice); this function preserves whatever the
/// closure returns and applies no policy of its own.
pub fn build_policy_set_from_wire<R>(
    wire: &PolicyArtifactWire,
    resolve_iri: R,
    stats: Option<&IndexStats>,
    action_filter: PolicyAction,
) -> Result<PolicySet>
where
    R: Fn(&str) -> Option<Sid>,
{
    let mut restrictions = Vec::with_capacity(wire.restrictions.len());
    for w in &wire.restrictions {
        let targets: HashSet<Sid> = w.targets.iter().filter_map(|iri| resolve_iri(iri)).collect();
        let for_classes: HashSet<Sid> = w
            .for_classes
            .iter()
            .filter_map(|iri| resolve_iri(iri))
            .collect();
        let value = match &w.value {
            WirePolicyValue::Allow => PolicyValue::Allow,
            WirePolicyValue::Deny => PolicyValue::Deny,
            WirePolicyValue::Query(json) => PolicyValue::Query(PolicyQuery { json: json.clone() }),
        };
        restrictions.push(PolicyRestriction {
            id: w.id.clone(),
            target_mode: w.target_mode,
            targets,
            action: w.action,
            value,
            required: w.required,
            message: w.message.clone(),
            class_policy: w.class_policy,
            for_classes,
            // `class_check_needed` is recomputed by `build_policy_set`
            // from the (for_classes, property, stats) triple.
            class_check_needed: false,
        });
    }
    Ok(build_policy_set(restrictions, stats, action_filter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PropertyPolicyEntry;
    use std::collections::HashMap;

    /// Build an IRI→Sid resolver from a static map.
    fn stub_resolver(entries: &[(&str, Sid)]) -> impl Fn(&str) -> Option<Sid> {
        let map: HashMap<String, Sid> = entries
            .iter()
            .map(|(iri, sid)| ((*iri).to_string(), sid.clone()))
            .collect();
        move |iri| map.get(iri).cloned()
    }

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn wire_origin() -> WireOrigin {
        WireOrigin {
            model_ledger_id: "test/model:main".into(),
            graph_iri: "http://example.org/policy".into(),
            resolved_t: 42,
        }
    }

    #[test]
    fn empty_wire_produces_empty_policy_set() {
        let wire = PolicyArtifactWire {
            origin: wire_origin(),
            restrictions: vec![],
        };
        let set = build_policy_set_from_wire(&wire, stub_resolver(&[]), None, PolicyAction::View)
            .expect("translate empty wire");
        assert!(set.restrictions.is_empty());
        assert!(set.by_property.is_empty());
        assert!(set.by_subject.is_empty());
        assert!(set.defaults.is_empty());
    }

    #[test]
    fn on_property_restriction_indexed_under_translated_sid() {
        let name_iri = "http://example.org/name";
        let name_sid = sid(100, "name");

        let wire = PolicyArtifactWire {
            origin: wire_origin(),
            restrictions: vec![WireRestriction {
                id: "http://example.org/rules/allow-name".into(),
                policy_types: vec!["https://ns.flur.ee/db#AccessPolicy".into()],
                target_mode: TargetMode::OnProperty,
                targets: vec![name_iri.into()],
                action: PolicyAction::View,
                value: WirePolicyValue::Allow,
                required: false,
                message: None,
                class_policy: false,
                for_classes: vec![],
            }],
        };

        let set = build_policy_set_from_wire(
            &wire,
            stub_resolver(&[(name_iri, name_sid.clone())]),
            None,
            PolicyAction::View,
        )
        .expect("translate property-target wire");

        assert_eq!(set.restrictions.len(), 1);
        let entries = set
            .by_property
            .get(&name_sid)
            .expect("name should be indexed");
        assert_eq!(entries.len(), 1);
        let PropertyPolicyEntry { idx, .. } = entries[0];
        assert_eq!(set.restrictions[idx].id, "http://example.org/rules/allow-name");
    }

    #[test]
    fn unresolved_target_iri_is_dropped_from_set_but_restriction_preserved() {
        // Two targets: one resolvable, one not. The restriction must
        // still appear in the result so its presence is observable;
        // only the unresolvable IRI is dropped from the index.
        let name_iri = "http://example.org/name";
        let unknown_iri = "http://example.org/never-seen";
        let name_sid = sid(100, "name");

        let wire = PolicyArtifactWire {
            origin: wire_origin(),
            restrictions: vec![WireRestriction {
                id: "http://example.org/rules/partial".into(),
                policy_types: vec!["https://ns.flur.ee/db#AccessPolicy".into()],
                target_mode: TargetMode::OnProperty,
                targets: vec![name_iri.into(), unknown_iri.into()],
                action: PolicyAction::View,
                value: WirePolicyValue::Deny,
                required: false,
                message: None,
                class_policy: false,
                for_classes: vec![],
            }],
        };

        let set = build_policy_set_from_wire(
            &wire,
            stub_resolver(&[(name_iri, name_sid.clone())]),
            None,
            PolicyAction::View,
        )
        .expect("translate partial-resolution wire");

        assert_eq!(set.restrictions.len(), 1, "restriction must be preserved");
        let restriction = &set.restrictions[0];
        assert_eq!(restriction.targets.len(), 1);
        assert!(restriction.targets.contains(&name_sid));
        assert!(
            set.by_property.contains_key(&name_sid),
            "resolvable target must be indexed"
        );
    }

    #[test]
    fn behavior_matches_direct_local_construction() {
        // Build the same policy two ways: (a) directly as a
        // PolicyRestriction in the local Sid-form (the same-ledger
        // materializer's output shape), and (b) through the wire
        // form using equivalent IRIs and the same Sid bindings.
        // Then exercise both PolicySets via the public
        // restrictions_for_flake API and assert the decisions match.
        // Asserts behavior, not byte-equality — HashSet iteration
        // order and incidental diagnostic strings are not
        // load-bearing.

        let name_iri = "http://example.org/name";
        let name_sid = sid(100, "name");
        let alice_sid = sid(100, "alice");

        // (a) Direct local construction.
        let local_restriction = PolicyRestriction {
            id: "http://example.org/rules/r1".into(),
            target_mode: TargetMode::OnProperty,
            targets: [name_sid.clone()].into_iter().collect(),
            action: PolicyAction::View,
            value: PolicyValue::Allow,
            required: false,
            message: None,
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        };
        let local_set = build_policy_set(vec![local_restriction], None, PolicyAction::View);

        // (b) Wire form translated against the same Sid bindings.
        let wire = PolicyArtifactWire {
            origin: wire_origin(),
            restrictions: vec![WireRestriction {
                id: "http://example.org/rules/r1".into(),
                policy_types: vec!["https://ns.flur.ee/db#AccessPolicy".into()],
                target_mode: TargetMode::OnProperty,
                targets: vec![name_iri.into()],
                action: PolicyAction::View,
                value: WirePolicyValue::Allow,
                required: false,
                message: None,
                class_policy: false,
                for_classes: vec![],
            }],
        };
        let wire_set = build_policy_set_from_wire(
            &wire,
            stub_resolver(&[(name_iri, name_sid.clone())]),
            None,
            PolicyAction::View,
        )
        .expect("wire translate");

        // Both must produce the same lookup result for a representative
        // flake (alice's name) and the same empty result for an
        // unrelated property.
        let unrelated = sid(100, "age");

        let local_hits = local_set.restrictions_for_flake(&alice_sid, &name_sid);
        let wire_hits = wire_set.restrictions_for_flake(&alice_sid, &name_sid);
        assert_eq!(local_hits.len(), wire_hits.len());
        assert_eq!(local_hits[0].id, wire_hits[0].id);
        assert!(matches!(local_hits[0].value, PolicyValue::Allow));
        assert!(matches!(wire_hits[0].value, PolicyValue::Allow));

        let local_miss = local_set.restrictions_for_flake(&alice_sid, &unrelated);
        let wire_miss = wire_set.restrictions_for_flake(&alice_sid, &unrelated);
        assert_eq!(local_miss.len(), 0);
        assert_eq!(wire_miss.len(), 0);
    }

    #[test]
    fn query_value_passes_through_verbatim() {
        // Query JSON is term-neutral by construction (the query engine
        // re-interns IRIs at execution time), so translation must not
        // touch it.
        let json_payload = r#"{"@id": "ex:Bob"}"#;
        let wire = PolicyArtifactWire {
            origin: wire_origin(),
            restrictions: vec![WireRestriction {
                id: "http://example.org/rules/cond".into(),
                policy_types: vec!["https://ns.flur.ee/db#AccessPolicy".into()],
                target_mode: TargetMode::Default,
                targets: vec![],
                action: PolicyAction::View,
                value: WirePolicyValue::Query(json_payload.into()),
                required: false,
                message: None,
                class_policy: false,
                for_classes: vec![],
            }],
        };
        let set = build_policy_set_from_wire(&wire, stub_resolver(&[]), None, PolicyAction::View)
            .expect("translate query-value wire");

        assert_eq!(set.restrictions.len(), 1);
        match &set.restrictions[0].value {
            PolicyValue::Query(q) => assert_eq!(q.json, json_payload),
            other => panic!("expected Query, got {other:?}"),
        }
    }
}
