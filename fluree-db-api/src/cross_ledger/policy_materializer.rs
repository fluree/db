//! Cross-ledger PolicyRules materialization.
//!
//! Reads a model ledger's policy graph at `resolved_t` and projects it
//! into a term-neutral [`PolicyArtifactWire`] keyed under the model
//! ledger's canonical id. The wire artifact is consumed by the API
//! layer's `build_policy_set_from_wire` against the data ledger's
//! snapshot to produce a Sid-form `PolicySet` (slice 5).
//!
//! Phase 1a scope:
//!
//! - Loads only policies with `rdf:type f:AccessPolicy`. Subclass
//!   entailment (`?p rdfs:subClassOf f:AccessPolicy`) is **not**
//!   applied — policies in M must declare `f:AccessPolicy` directly.
//!   The cache key stays free of policy-class context, which keeps a
//!   single wire artifact shareable across every data ledger that
//!   references the same model graph.
//! - No `f:atT` pin — resolver passes M's current head `t`. Pinned
//!   refs are accepted by the resolver but treated as the current
//!   head for materialization; honoring pins lands in a later slice.
//!
//! Reuses the same-ledger `policy_builder::load_policies_by_class`
//! against M's snapshot, then decodes the resulting Sid-form
//! `PolicyRestriction` list into IRI-form `WireRestriction`s by
//! routing every Sid through M's `decode_sid`.

use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::LedgerSnapshot;
use fluree_db_policy::{
    PolicyArtifactWire, PolicyRestriction, PolicyValue, WireOrigin, WirePolicyValue,
    WireRestriction,
};

/// IRI of the canonical policy class root.
///
/// Phase 1a scope: subclass entailment is not applied. Policies in
/// the model ledger must declare `rdf:type f:AccessPolicy` directly.
const ACCESS_POLICY_IRI: &str = "https://ns.flur.ee/db#AccessPolicy";

/// Materialize the policy graph at `graph_iri` in model ledger `M` at
/// `resolved_t` into a `PolicyArtifactWire`.
///
/// Failure modes:
///
/// - The model ledger's snapshot cannot be opened at `resolved_t` —
///   surfaces as [`CrossLedgerError::TranslationFailed`]. The
///   resolver's nameservice lookup already covered the
///   "ledger doesn't exist" case before reaching us; this is
///   a snapshot-construction failure (storage error, etc.).
/// - The graph IRI is not in `M`'s graph registry at `resolved_t` —
///   [`CrossLedgerError::GraphMissingAtT`].
/// - The same-ledger loader fails (system IRI encoding, query
///   execution) — [`CrossLedgerError::TranslationFailed`] with the
///   underlying error in `detail`.
/// - A Sid in the loaded restrictions cannot be decoded back to an
///   IRI against M's namespace map (dictionary loss / corruption) —
///   [`CrossLedgerError::TranslationFailed`].
pub(super) async fn materialize_policy_rules(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<PolicyArtifactWire, CrossLedgerError> {
    // 1. Open M at resolved_t. The resolver already canonicalized the
    //    ledger id via nameservice.lookup, so this should succeed
    //    unless the snapshot itself can't be built (e.g., index
    //    storage error).
    let m_db = fluree
        .db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    // 2. Resolve graph_iri → g_id in M's graph registry.
    let g_id = m_db
        .snapshot
        .graph_registry
        .graph_id_for_iri(graph_iri)
        .ok_or_else(|| CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        })?;

    // 3. Load all f:AccessPolicy-typed subjects in that graph via the
    //    same-ledger loader, but applied to M's snapshot. The loader
    //    handles the rdf:type scan, per-policy predicate fan-out, and
    //    target-mode determination.
    let restrictions = crate::policy_builder::load_policies_by_class(
        &m_db.snapshot,
        m_db.overlay.as_ref(),
        m_db.t,
        &[ACCESS_POLICY_IRI.to_string()],
        &[g_id],
    )
    .await
    .map_err(|e| CrossLedgerError::TranslationFailed {
        ledger_id: canonical_model_ledger_id.to_string(),
        graph_iri: graph_iri.to_string(),
        detail: format!("policy loader failed against model ledger: {e}"),
    })?;

    // 4. Decode each PolicyRestriction's Sids back to IRIs against M's
    //    namespace map. The resulting WireRestrictions are term-neutral
    //    — consumers re-intern them against their own data ledger's
    //    dictionary in build_policy_set_from_wire.
    let mut wire_restrictions = Vec::with_capacity(restrictions.len());
    for r in &restrictions {
        wire_restrictions.push(restriction_to_wire(
            r,
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

/// Decode a Sid-form `PolicyRestriction` (produced against M's
/// snapshot) into an IRI-form `WireRestriction`.
///
/// Every Sid that the same-ledger loader assembled must round-trip
/// through `decode_sid`; failure to decode indicates a corrupt or
/// truncated namespace map and is surfaced as `TranslationFailed`
/// rather than silently dropping the target. Silent drop would
/// produce a policy that's structurally weaker than what was
/// authored — exactly the failure mode the design doc forbids.
fn restriction_to_wire(
    r: &PolicyRestriction,
    snapshot: &LedgerSnapshot,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<WireRestriction, CrossLedgerError> {
    let decode_set = |sids: &std::collections::HashSet<fluree_db_core::Sid>,
                      field: &str|
     -> Result<Vec<String>, CrossLedgerError> {
        let mut out = Vec::with_capacity(sids.len());
        for sid in sids {
            let iri = snapshot.decode_sid(sid).ok_or_else(|| {
                CrossLedgerError::TranslationFailed {
                    ledger_id: canonical_model_ledger_id.to_string(),
                    graph_iri: graph_iri.to_string(),
                    detail: format!(
                        "could not decode Sid {sid:?} on field '{field}' \
                         of policy {} against model ledger's namespace map",
                        r.id,
                    ),
                }
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
