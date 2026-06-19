//! AST types for openCypher 9.
//!
//! Pure data types with no DB access. The parser builds this; the lower
//! step (feature `lowering`) translates it into `fluree_db_query::ir::Query`
//! for reads and `fluree_db_transact::Txn` for writes.

mod expr;
mod pattern;
mod stmt;

pub use expr::*;
pub use pattern::*;
pub use stmt::*;

use crate::span::SourceSpan;

/// Top-level parsed Cypher input.
#[derive(Clone, Debug, PartialEq)]
pub struct CypherAst {
    pub statement: Statement,
    pub span: SourceSpan,
}
