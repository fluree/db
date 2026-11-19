//! Cross-ledger datalog rules materialization.
//!
//! Reads M's rules graph at `resolved_t` and projects every `f:rule`
//! value into the cross-ledger wire form: the IRI of the subject it is
//! on, its language and its source text. Rules need no term-space
//! translation: their text names terms by IRI, and the query crate
//! parses it against the data ledger's snapshot when the rule set loads.
//!
//! Each value is read by
//! [`fluree_db_query::datalog_rules::stored_rule_source`], the one
//! reading the data ledger's own extractor
//! (`fluree_db_query::datalog_rules::extract_datalog_rules`) uses: an
//! `@json` value is a JSON-LD rule, an `f:sparql`-typed string a SPARQL
//! rule, and any other datatype fails the request naming the rule, as it
//! fails a query over the ledger that stores it.

use super::types::{ModelHead, RulesArtifactWire, WireOrigin};
use super::CrossLedgerError;
use super::ResolveCtx;
use fluree_db_core::{IndexType, RangeMatch, RangeOptions, RangeTest};
use fluree_db_query::datalog_rules::{stored_rule_source, unsupported_rule_datatype, ImportedRule};
use fluree_vocab::fluree::RULE;

#[tracing::instrument(
    name = "cross_ledger.rules.materialize",
    level = "debug",
    skip(ctx),
    fields(
        model_ledger = canonical_model_ledger_id,
        graph_iri = graph_iri,
        resolved_t = head.t,
    ),
)]
pub(super) async fn materialize_rules(
    canonical_model_ledger_id: &str,
    graph_iri: &str,
    head: &ModelHead,
    ctx: &ResolveCtx<'_>,
) -> Result<RulesArtifactWire, CrossLedgerError> {
    let resolved_t = head.t;
    let m_db = ctx
        .open_model_db(canonical_model_ledger_id, head)
        .await
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            detail: format!("failed to open model ledger snapshot at t={resolved_t}: {e}"),
        })?;

    let g_id =
        super::resolve_selector_g_id(&m_db.snapshot, graph_iri, m_db.t)?.ok_or_else(|| {
            CrossLedgerError::GraphMissingAtT {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
            }
        })?;

    // No `f:rule` predicate in M's namespace map → M never authored
    // rules. Treat the same as an empty rule set so the caller's
    // downstream merge is a no-op.
    let Some(rule_pred_sid) = m_db.snapshot.encode_iri_strict(RULE) else {
        return Ok(RulesArtifactWire {
            origin: WireOrigin {
                model_ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                resolved_t,
                commit_id: head.commit_id.clone(),
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

    let mut rules: Vec<ImportedRule> = Vec::new();
    for f in flakes.into_iter().filter(|f| f.op) {
        let id = m_db
            .snapshot
            .decode_sid(&f.s)
            .unwrap_or_else(|| f.s.to_string());
        let Some((language, source)) = stored_rule_source(&f.o, &f.dt) else {
            let datatype = m_db
                .snapshot
                .decode_sid(&f.dt)
                .unwrap_or_else(|| f.dt.to_string());
            return Err(CrossLedgerError::TranslationFailed {
                ledger_id: canonical_model_ledger_id.to_string(),
                graph_iri: graph_iri.to_string(),
                detail: format!(
                    "datalog rule <{id}> {}",
                    unsupported_rule_datatype(&datatype)
                ),
            });
        };
        rules.push(ImportedRule {
            id,
            language,
            source: source.to_string(),
        });
    }

    Ok(RulesArtifactWire {
        origin: WireOrigin {
            model_ledger_id: canonical_model_ledger_id.to_string(),
            graph_iri: graph_iri.to_string(),
            resolved_t,
            commit_id: head.commit_id.clone(),
        },
        rules,
    })
}
