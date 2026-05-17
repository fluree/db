//! Cross-ledger constraints materialization.
//!
//! Reads a model ledger's constraints graph at `resolved_t` and
//! projects every property carrying `f:enforceUnique true` into a
//! term-neutral [`ConstraintsArtifactWire`]. The translator at the
//! data ledger side encodes each IRI against D's namespace map to
//! produce the property Sid set that the existing
//! `enforce_unique_constraints` flow consumes.
//!
//! The artifact is the simplest of the cross-ledger materializers —
//! a list of property IRIs, no per-rule body, no class targets, no
//! values.

use super::types::{ConstraintsArtifactWire, WireOrigin};
use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::{FlakeValue, IndexType, LedgerSnapshot, RangeMatch, RangeTest, Sid};
use fluree_vocab::config_iris;

/// Materialize the constraints graph at `graph_iri` in model
/// ledger `M` at `resolved_t` into a `ConstraintsArtifactWire`.
pub(super) async fn materialize_constraints(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<ConstraintsArtifactWire, CrossLedgerError> {
    // 1. Open M at resolved_t.
    let m_db = fluree
        .db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    // 2. Resolve graph_iri → g_id in M's graph registry.
    let g_id = super::resolve_selector_g_id(&m_db.snapshot, graph_iri)?.ok_or_else(|| {
        CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        }
    })?;

    // 3. Encode the system IRIs we need to scan against M. These
    //    come from default namespaces pre-registered at genesis,
    //    so failure here means M is corrupted.
    let enforce_unique_sid = encode_system_iri(
        &m_db.snapshot,
        config_iris::ENFORCE_UNIQUE,
        canonical_model_ledger_id,
        graph_iri,
    )?;
    let xsd_boolean_sid = encode_system_iri(
        &m_db.snapshot,
        "http://www.w3.org/2001/XMLSchema#boolean",
        canonical_model_ledger_id,
        graph_iri,
    )?;

    // 4. Scan POST for ?prop f:enforceUnique true. The matching
    //    subjects ARE the property IRIs we want.
    let m_view =
        fluree_db_core::GraphDbRef::new(&m_db.snapshot, g_id, m_db.overlay.as_ref(), m_db.t);
    let match_val = RangeMatch::predicate_object(enforce_unique_sid, FlakeValue::Boolean(true))
        .with_datatype(xsd_boolean_sid);

    let flakes = m_view
        .range(IndexType::Post, RangeTest::Eq, match_val)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("range scan for f:enforceUnique annotations failed: {e}"),
        })?;

    // 5. Decode each property Sid back to its IRI. A failure to
    //    decode means M's namespace map is missing the entry —
    //    structural corruption, not something a fail-open silent
    //    drop should hide.
    let mut property_iris = Vec::new();
    for flake in flakes.into_iter().filter(|f| f.op) {
        let iri = m_db.snapshot.decode_sid(&flake.s).ok_or_else(|| {
            CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!(
                    "could not decode property Sid {:?} declared \
                     f:enforceUnique against model ledger's namespace map",
                    flake.s
                ),
            }
        })?;
        property_iris.push(iri);
    }

    Ok(ConstraintsArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        },
        property_iris,
    })
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
                "system IRI '{iri}' is not in the model ledger's namespace \
                 map; this usually indicates the model ledger is corrupted \
                 or did not initialize default namespaces"
            ),
        })
}
