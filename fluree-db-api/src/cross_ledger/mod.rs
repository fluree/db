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

/// Resolve a `f:graphSelector` IRI against a model ledger snapshot,
/// honoring `f:defaultGraph` as `g_id = 0`. Used by every
/// materializer to translate the selector IRI carried on the wire
/// into a concrete graph id. Returns `None` when the IRI is neither
/// `f:defaultGraph` nor present in the snapshot's `graph_registry`;
/// callers map `None` to [`CrossLedgerError::GraphMissingAtT`].
pub(crate) fn resolve_selector_g_id(
    snapshot: &fluree_db_core::LedgerSnapshot,
    graph_iri: &str,
) -> Option<fluree_db_core::GraphId> {
    if graph_iri == fluree_vocab::config_iris::DEFAULT_GRAPH {
        return Some(0u16);
    }
    snapshot.graph_registry.graph_id_for_iri(graph_iri)
}
