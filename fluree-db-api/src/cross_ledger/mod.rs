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
