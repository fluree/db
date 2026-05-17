//! Cross-ledger datalog rules materialization.
//!
//! Reads M's rules graph at `resolved_t` and projects every
//! `f:rule` flake whose object is `FlakeValue::Json` into the
//! cross-ledger wire form. Unlike the schema / shapes / constraints
//! paths, rules need no term-space translation: the JSON body
//! references IRIs that
//! [`fluree_db_query::datalog_rules::parse_query_time_rule`] resolves
//! against the data ledger's snapshot at query time, the same way
//! `opts.rules` are handled.
//!
//! Non-JSON values on `f:rule` are silently skipped — the local
//! extractor (`fluree_db_query::datalog_rules::extract_datalog_rules`)
//! does the same. A future variant could surface these as a warn
//! when authors mistype the rule literal.

use super::types::{RulesArtifactWire, WireOrigin};
use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::{FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest};
use fluree_vocab::fluree::RULE;

#[tracing::instrument(
    name = "cross_ledger.rules.materialize",
    level = "debug",
    skip(fluree),
    fields(
        model_ledger = canonical_model_ledger_id,
        graph_iri = graph_iri,
        resolved_t = resolved_t,
    ),
)]
pub(super) async fn materialize_rules(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    resolved_t: i64,
    fluree: &Fluree,
) -> Result<RulesArtifactWire, CrossLedgerError> {
    let m_db = fluree
        .load_graph_db_at_t(canonical_model_ledger_id, resolved_t)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    let g_id = super::resolve_selector_g_id(&m_db.snapshot, graph_iri)?.ok_or_else(|| {
        CrossLedgerError::GraphMissingAtT {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        }
    })?;

    // No `f:rule` predicate in M's namespace map → M never authored
    // rules. Treat the same as an empty rule set so the caller's
    // downstream merge is a no-op.
    let Some(rule_pred_sid) = m_db.snapshot.encode_iri(RULE) else {
        return Ok(RulesArtifactWire {
            origin: WireOrigin {
                model_ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
            },
            rules: Vec::new(),
        });
    };

    let opts = RangeOptions::default().with_to_t(m_db.t);
    let flakes = fluree_db_core::range_with_overlay(
        &m_db.snapshot,
        g_id,
        m_db.overlay.as_ref(),
        IndexType::Psot,
        RangeTest::Eq,
        RangeMatch::predicate(rule_pred_sid),
        opts,
    )
    .await
    .map_err(|e| CrossLedgerError::TranslationFailed {
        ledger_id: canonical_model_ledger_id.to_string(),
        graph_iri: graph_iri.to_string(),
        detail: format!("f:rule predicate scan failed: {e}"),
    })?;

    let mut rules: Vec<String> = Vec::new();
    for f in flakes.into_iter().filter(|f| f.op) {
        if let FlakeValue::Json(json_str) = &f.o {
            rules.push(json_str.clone());
        }
        // Non-Json values are silently skipped, mirroring the
        // same-ledger extractor's `if let FlakeValue::Json(...)`
        // guard.
    }

    Ok(RulesArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
        },
        rules,
    })
}
