//! Cross-ledger PolicyRules materialization.
//!
//! Reads a model ledger's policy graph at `resolved_t` and projects it
//! into a term-neutral [`PolicyArtifactWire`] keyed under the model
//! ledger's canonical id. The wire artifact is consumed by the API
//! layer's `build_policy_set_from_wire` against the data ledger's
//! snapshot to produce a Sid-form `PolicySet` (slice 5).
//!
//! ## Phase 1a scope
//!
//! **Structural detection of policy subjects.** A subject is treated
//! as a policy if it has at least one `f:allow` or `f:query` triple
//! in the configured graph. This is intentionally schema-agnostic —
//! the rule's `rdf:type` is recorded on the wire as
//! `WireRestriction.policy_types` and the translation step
//! intersects those against the data ledger's configured
//! `f:policyClass` set. This keeps the cross-ledger cache shareable
//! across every data ledger that references the same model graph,
//! regardless of policy-class configuration.
//!
//! Exact IRI matching only. Subclass entailment
//! (`rdfs:subClassOf` chains rooted at the configured policy class)
//! is **not** applied — the data ledger's policy class IRI must
//! appear verbatim in the rule subject's `rdf:type` list. This
//! mirrors the same-ledger `load_policies_by_class` semantics, which
//! also matches `rdf:type` directly without entailment.
//!
//! ## Failure handling
//!
//! - Snapshot construction fails → `TranslationFailed` (with the
//!   underlying error in `detail`).
//! - Graph IRI absent from M's registry → `GraphMissingAtT`.
//! - System IRI (`f:allow`, `f:query`, `rdf:type`) absent from M's
//!   namespace map → `TranslationFailed`. These IRIs come from
//!   default namespaces pre-registered at genesis, so this only
//!   surfaces on corrupted M.
//! - The same-ledger `load_policy_restriction` returns `None` for a
//!   subject (malformed — has the value predicate but the action /
//!   target combination doesn't add up) → the subject is **skipped**
//!   from the wire artifact, matching the existing same-ledger
//!   behavior. The skip is silent because the same-ledger loader's
//!   `None` return is itself an "ignore this subject" signal; if
//!   that's wrong it's a same-ledger correctness bug, not something
//!   cross-ledger should diverge on.
//! - A Sid that doesn't decode against M's namespace map →
//!   `TranslationFailed` (no silent drop; dropping a target would
//!   produce a structurally weaker policy than authored).

use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::{FlakeValue, IndexType, LedgerSnapshot, RangeMatch, RangeTest, Sid};
use fluree_vocab::policy_iris;

/// Canonical policy class IRI. Subjects typed exactly as this are
/// included in the structural detection set even when they're missing
/// `f:allow` and `f:query`, mirroring the same-ledger
/// `load_policy_restriction` behavior that maps such "missing-effect"
/// policies to `Deny` (fail-closed). Without this scan, a canonically-
/// typed policy with no effect would be silently absent cross-ledger
/// while still being enforced as Deny same-ledger — a divergence the
/// design doc forbids.
const ACCESS_POLICY_IRI: &str = "https://ns.flur.ee/db#AccessPolicy";
use fluree_db_policy::{
    PolicyArtifactWire, PolicyRestriction, PolicyValue, WireOrigin, WirePolicyValue,
    WireRestriction,
};
use std::collections::HashSet;

/// Materialize the policy graph at `graph_iri` in model ledger `M` at
/// `resolved_t` into a `PolicyArtifactWire`.
#[tracing::instrument(
    name = "cross_ledger.policy.materialize",
    level = "debug",
    skip(fluree),
    fields(
        model_ledger = canonical_model_ledger_id,
        graph_iri = graph_iri,
        resolved_t = resolved_t,
    ),
)]
pub(super) async fn materialize_policy_rules(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<PolicyArtifactWire, CrossLedgerError> {
    // 1. Open M at resolved_t.
    let m_db = fluree
        .load_graph_db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    // 2. Resolve graph_iri → g_id in M's graph registry (handling
    //    `f:defaultGraph` as g_id=0).
    let g_id = super::resolve_selector_g_id(&m_db.snapshot, graph_iri)?.ok_or_else(|| {
        CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        }
    })?;

    // 3. Encode the predicate IRIs we need to scan against M's
    //    namespace map. These come from default namespaces
    //    (fluree, rdf) which are pre-registered at genesis, so a
    //    failure here means M is corrupt.
    let allow_sid = encode_system_iri(
        &m_db.snapshot,
        policy_iris::ALLOW,
        canonical_model_ledger_id,
        graph_iri,
    )?;
    let query_sid = encode_system_iri(
        &m_db.snapshot,
        policy_iris::QUERY,
        canonical_model_ledger_id,
        graph_iri,
    )?;
    let rdf_type_sid = encode_system_iri(
        &m_db.snapshot,
        rdf_type_iri(),
        canonical_model_ledger_id,
        graph_iri,
    )?;

    let m_view =
        fluree_db_core::GraphDbRef::new(&m_db.snapshot, g_id, m_db.overlay.as_ref(), m_db.t);

    // 4. Structural detection — find every subject that has at least
    //    one f:allow or f:query triple OR is canonically typed as
    //    f:AccessPolicy. The latter scan covers the case where a
    //    canonically-typed policy is missing both effect predicates;
    //    same-ledger load_policy_restriction maps that to Deny
    //    (fail-closed), and the cross-ledger flow must agree.
    let mut policy_subjects: HashSet<Sid> = HashSet::new();
    for pred_sid in [allow_sid, query_sid] {
        let flakes = m_view
            .range(
                IndexType::Post,
                RangeTest::Eq,
                RangeMatch::predicate(pred_sid),
            )
            .await
            .map_err(|e| CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!("range scan for policy-effect predicate failed: {e}"),
            })?;
        for flake in flakes.into_iter().filter(|f| f.op) {
            policy_subjects.insert(flake.s);
        }
    }

    // Union in subjects typed exactly as f:AccessPolicy. We don't
    // attempt subclass entailment or scan for arbitrary types — the
    // canonical class is the only structural baseline; custom-typed
    // policies still need an explicit effect predicate to be picked
    // up cross-ledger. That's a documented Phase 1a limitation.
    if let Some(access_policy_sid) = m_db.snapshot.encode_iri(ACCESS_POLICY_IRI) {
        let flakes = m_view
            .range(
                IndexType::Post,
                RangeTest::Eq,
                RangeMatch::predicate_object(
                    rdf_type_sid.clone(),
                    FlakeValue::Ref(access_policy_sid),
                ),
            )
            .await
            .map_err(|e| CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!("range scan for rdf:type f:AccessPolicy failed: {e}"),
            })?;
        for flake in flakes.into_iter().filter(|f| f.op) {
            policy_subjects.insert(flake.s);
        }
    }

    // 5. For each candidate policy subject, fan out via the same-
    //    ledger loader (against M's snapshot) and read its rdf:type
    //    set. Subjects the loader returns `None` for are silently
    //    skipped — that's the existing local-side semantics for
    //    malformed rules.
    let policy_graphs = [g_id];
    let mut wire_restrictions = Vec::with_capacity(policy_subjects.len());
    for policy_sid in policy_subjects {
        let restriction = crate::policy_builder::load_policy_restriction(
            &m_db.snapshot,
            m_db.overlay.as_ref(),
            m_db.t,
            &policy_sid,
            &policy_graphs,
        )
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("per-policy load failed for {policy_sid:?}: {e}"),
        })?;

        let Some(restriction) = restriction else {
            continue; // malformed; matches same-ledger semantics
        };

        let policy_types = read_rdf_types(
            &m_view,
            &policy_sid,
            &rdf_type_sid,
            &m_db.snapshot,
            canonical_model_ledger_id,
            graph_iri,
        )
        .await?;

        wire_restrictions.push(restriction_to_wire(
            &restriction,
            policy_types,
            &m_db.snapshot,
            canonical_model_ledger_id,
            graph_iri,
        )?);
    }

    Ok(PolicyArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        },
        restrictions: wire_restrictions,
    })
}

fn rdf_type_iri() -> &'static str {
    // Avoid a dep cycle / extra const — the IRI is fixed by the W3C
    // and pre-registered in every Fluree ledger via the RDF
    // namespace code.
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
}

fn encode_system_iri(
    snapshot: &LedgerSnapshot,
    iri: &str,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<Sid, CrossLedgerError> {
    snapshot
        .encode_iri(iri)
        .ok_or_else(|| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!(
                "system IRI '{iri}' is not in the model ledger's namespace map; \
                 this usually indicates the model ledger is corrupted or did \
                 not initialize default namespaces"
            ),
        })
}

/// Read every `rdf:type` value for `subject` in the policy graph and
/// decode each to its IRI form.
async fn read_rdf_types(
    m_view: &fluree_db_core::GraphDbRef<'_>,
    subject: &Sid,
    rdf_type_sid: &Sid,
    snapshot: &LedgerSnapshot,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<Vec<String>, CrossLedgerError> {
    let flakes = m_view
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(subject.clone(), rdf_type_sid.clone()),
        )
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("rdf:type scan for {subject:?} failed: {e}"),
        })?;

    let mut out = Vec::new();
    for flake in flakes.into_iter().filter(|f| f.op) {
        let FlakeValue::Ref(class_sid) = flake.o else {
            continue; // non-Ref rdf:type isn't valid; skip silently
        };
        let iri =
            snapshot
                .decode_sid(&class_sid)
                .ok_or_else(|| CrossLedgerError::TranslationFailed {
                    ledger_id: canonical_model_ledger_id.to_string(),
                    graph_iri: graph_iri.to_string(),
                    detail: format!(
                        "could not decode rdf:type Sid {class_sid:?} on policy {subject:?}"
                    ),
                })?;
        out.push(iri);
    }
    Ok(out)
}

/// Decode a Sid-form `PolicyRestriction` into an IRI-form
/// `WireRestriction`, attaching the rule's `rdf:type` set.
///
/// A Sid that doesn't round-trip through `decode_sid` surfaces as
/// `TranslationFailed` rather than being silently dropped — silent
/// drop would produce a policy structurally weaker than authored.
fn restriction_to_wire(
    r: &PolicyRestriction,
    policy_types: Vec<String>,
    snapshot: &LedgerSnapshot,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<WireRestriction, CrossLedgerError> {
    let decode_set = |sids: &HashSet<Sid>, field: &str| -> Result<Vec<String>, CrossLedgerError> {
        let mut out = Vec::with_capacity(sids.len());
        for sid in sids {
            let iri =
                snapshot
                    .decode_sid(sid)
                    .ok_or_else(|| CrossLedgerError::TranslationFailed {
                        ledger_id: canonical_model_ledger_id.to_string(),
                        graph_iri: graph_iri.to_string(),
                        detail: format!(
                            "could not decode Sid {sid:?} on field '{field}' of policy {}",
                            r.id,
                        ),
                    })?;
            out.push(iri);
        }
        Ok(out)
    };

    let targets = decode_set(&r.targets, "targets")?;
    let for_classes = decode_set(&r.for_classes, "for_classes")?;

    let value = match &r.value {
        PolicyValue::Allow => WirePolicyValue::Allow,
        PolicyValue::Deny => WirePolicyValue::Deny,
        PolicyValue::Query(q) => WirePolicyValue::Query(q.json.clone()),
    };

    Ok(WireRestriction {
        id: r.id.clone(),
        policy_types,
        target_mode: r.target_mode,
        targets,
        action: r.action,
        value,
        required: r.required,
        message: r.message.clone(),
        class_policy: r.class_policy,
        for_classes,
    })
}
