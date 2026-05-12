//! Cypher AST → fluree-db-query IR.
//!
//! Implemented in M5.3 (read path). For now, this is a stub that
//! returns a `LowerError::NotImplemented`.

use thiserror::Error;

use crate::ast::CypherAst;

#[derive(Debug, Error)]
pub enum LowerError {
    #[error("Cypher lowering is not yet implemented")]
    NotImplemented,
    #[error("lowering error: {0}")]
    Other(String),
}

/// Lower a Cypher AST into a query/transaction IR.
///
/// This is a stub. The concrete lowering will be split into a read
/// path (returning `fluree_db_query::ir::Query`) and a write path
/// (returning `fluree_db_transact::Txn`) once those crates are
/// wired in.
pub fn lower_cypher(_ast: &CypherAst) -> Result<(), LowerError> {
    Err(LowerError::NotImplemented)
}
