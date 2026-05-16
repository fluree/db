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
mod types;

pub use cache::GovernanceCache;
pub use error::CrossLedgerError;
pub use resolver::resolve_graph_ref;
pub use types::{
    ArtifactKind, ConstraintsArtifactWire, GovernanceArtifact, ResolveCtx, ResolvedGraph,
    WireOrigin,
};
