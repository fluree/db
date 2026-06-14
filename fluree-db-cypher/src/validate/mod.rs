//! Capability-driven validation for Cypher ASTs.
//!
//! v1 capability set is minimal — the parser already rejects most
//! deferred shapes with specific errors. This module exists as a
//! placeholder that mirrors the SPARQL crate's surface.

use crate::ast::CypherAst;
use crate::diag::Diagnostic;

#[derive(Clone, Debug, Default)]
pub struct Capabilities {
    /// If true, allow `MATCH (n)` to scan all subjects (defaults false in v1).
    pub allow_bare_node_pattern: bool,
}

pub fn validate(_ast: &CypherAst, _caps: &Capabilities) -> Vec<Diagnostic> {
    Vec::new()
}
