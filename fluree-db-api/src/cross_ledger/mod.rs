//! Cross-ledger model enforcement.
//!
//! Resolution of `f:GraphRef` whose `f:ledger` targets a different
//! ledger on the same instance: the model ledger holds governance
//! artifacts (policy / shapes / schema / rules / constraints) that
//! are applied to requests against the data ledger.
//!
//! Contract and semantics: see
//! `docs/design/cross-ledger-model-enforcement.md`.

mod cache;
mod constraints_materializer;
pub mod error;
mod policy_materializer;
mod resolver;
mod rules_materializer;
mod schema_materializer;
mod shapes_materializer;
mod types;

pub use cache::GovernanceCache;
pub use error::CrossLedgerError;
pub use resolver::resolve_graph_ref;
pub use types::{
    ArtifactKind, ConstraintsArtifactWire, GovernanceArtifact, ResolveCtx, ResolvedGraph,
    RulesArtifactWire, SchemaArtifactWire, ShapesArtifactWire, WireObject, WireOrigin, WireTriple,
};

/// Resolve a `f:graphSelector` IRI against a model ledger snapshot.
///
/// - `f:defaultGraph` → `Ok(Some(0))`.
/// - `f:txnMetaGraph` → `Err(ReservedGraphSelected)`. The
///   txn-meta graph carries commit-time provenance and is never a
///   legitimate cross-ledger target; rejecting the sentinel here
///   matches the per-canonical-ledger reserved-graph guard
///   ([`crate::cross_ledger::types::reject_if_reserved_graph`])
///   and surfaces the dedicated error variant instead of letting
///   the request leak to a `GraphMissingAtT` after touching M.
/// - Named graph IRI present in the snapshot's registry →
///   `Ok(Some(g_id))`.
/// - Otherwise → `Ok(None)`; callers map to
///   [`CrossLedgerError::GraphMissingAtT`] with the full context
///   (ledger id, resolved_t) that this helper doesn't carry.
pub(crate) fn resolve_selector_g_id(
    snapshot: &fluree_db_core::LedgerSnapshot,
    graph_iri: &str,
) -> Result<Option<fluree_db_core::GraphId>, CrossLedgerError> {
    if graph_iri == fluree_vocab::config_iris::DEFAULT_GRAPH {
        return Ok(Some(0u16));
    }
    if graph_iri == fluree_vocab::config_iris::TXN_META_GRAPH {
        return Err(CrossLedgerError::ReservedGraphSelected {
            graph_iri: graph_iri.to_string(),
        });
    }
    Ok(snapshot.graph_registry.graph_id_for_iri(graph_iri))
}

/// Encode a fixed system IRI (e.g. `rdf:type`, `f:allow`) against a
/// model-ledger snapshot, returning a structured error if the IRI's
/// namespace is not registered.
///
/// Uses `encode_iri_strict` so a missing well-known prefix surfaces as
/// [`CrossLedgerError::TranslationFailed`] rather than silently
/// falling back to an EMPTY-namespace Sid that would match nothing in
/// M's flakes (silent governance downgrade).
pub(crate) fn encode_system_iri(
    snapshot: &fluree_db_core::LedgerSnapshot,
    iri: &str,
    canonical_model_ledger_id: &str,
    graph_iri: &str,
) -> Result<fluree_db_core::Sid, CrossLedgerError> {
    snapshot
        .encode_iri_strict(iri)
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

/// Resolve `f:reasoningDefaults`' `f:schemaSource` when it points at a
/// model ledger, translating the ontology wire against the data ledger's
/// snapshot. Returns `None` when no cross-ledger schema source is
/// configured. Resolution is t-cached (GovernanceCache): an unchanged model
/// head is an Arc clone, not a re-query.
///
/// Shared by SHACL enforcement (transaction path) and policy-context
/// construction so both merge the model ledger's subclass/subproperty edges
/// into their entailment hierarchy.
pub(crate) async fn resolve_schema_closure_bundle(
    reasoning: &fluree_db_core::ledger_config::ReasoningDefaults,
    snapshot: &fluree_db_core::LedgerSnapshot,
    ctx: &mut ResolveCtx<'_>,
) -> Result<
    Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>,
    CrossLedgerError,
> {
    let Some(schema_source) = reasoning.schema_source.as_ref() else {
        return Ok(None);
    };
    if schema_source.ledger.is_none() {
        return Ok(None);
    }
    // The cross-ledger materializer resolves a single graph and does not
    // walk `owl:imports`, so `f:followOwlImports` cannot be honored here.
    // Skip the bundle (local-only hierarchy) rather than erroring: this
    // resolver runs inside every transaction's enforcement setup, so a hard
    // failure would reject every subsequent write on the ledger — including
    // the config repair itself. The loud fail-closed rejection lives on the
    // reasoning-query path (`resolve_configured_schema_bundle`), which is
    // where an incomplete closure would actually change entailment results;
    // enforcement merely falls back to the pre-feature local hierarchy.
    if reasoning.follow_owl_imports.unwrap_or(false) {
        tracing::warn!(
            model_ledger = schema_source.ledger.as_deref().unwrap_or_default(),
            graph_selector = schema_source.graph_selector.as_deref().unwrap_or_default(),
            "`f:followOwlImports` is not supported with a cross-ledger \
             `f:schemaSource`; skipping cross-ledger enforcement entailment \
             (local hierarchy only). Reasoning queries against this config \
             fail closed."
        );
        return Ok(None);
    }
    let resolved = resolve_graph_ref(schema_source, ArtifactKind::SchemaClosure, ctx).await?;
    let GovernanceArtifact::SchemaClosure(wire) = &resolved.artifact else {
        return Err(CrossLedgerError::TranslationFailed {
            ledger_id: resolved.model_ledger_id.clone(),
            graph_iri: resolved.graph_iri.clone(),
            detail: "resolver returned a non-SchemaClosure artifact".into(),
        });
    };
    let bundle = wire
        .translate_to_schema_bundle_flakes(snapshot)
        .map_err(|e| CrossLedgerError::TranslationFailed {
            ledger_id: resolved.model_ledger_id.clone(),
            graph_iri: resolved.graph_iri.clone(),
            detail: format!("schema wire translation failed: {e}"),
        })?;
    Ok(Some(bundle))
}
