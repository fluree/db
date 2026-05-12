//! Cypher AST → fluree-db-query IR.
//!
//! See `GQL_CYPHER_SUPPORT.md` "M5.3 — Lower (read path)" for the
//! lowering rules. This module is read-path only; write-path lowering
//! lives in `fluree-db-transact/src/lower_cypher_update.rs`.

mod context;
mod expr;
mod pattern;
mod stmt;

pub use context::LoweringContext;

use thiserror::Error;

use crate::ast::{CypherAst, Statement};

use fluree_db_query::ir::Query;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarRegistry;

#[derive(Debug, Error)]
pub enum LowerError {
    #[error("{message}")]
    Generic { message: String },
    #[error("Cypher writes are lowered via fluree-db-transact, not the query path")]
    WriteOnQueryPath,
    #[error("unsupported in v1: {0}")]
    Unsupported(String),
    #[error("unresolved identifier: {0}")]
    UnresolvedIri(String),
    #[error("attempt to use reserved Fluree system predicate: {0}")]
    ReservedPredicate(String),
    #[error("bare node pattern `({0})` rejected — see GQL_CYPHER_SUPPORT.md \"Node existence model\"")]
    BareNodePattern(String),
}

impl LowerError {
    pub fn unsupported(msg: impl Into<String>) -> Self {
        LowerError::Unsupported(msg.into())
    }

    pub fn generic(msg: impl Into<String>) -> Self {
        LowerError::Generic {
            message: msg.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, LowerError>;

/// Lower a Cypher AST to a `Query`. Only valid for `Statement::Query`
/// shapes; write statements must use `fluree_db_transact::lower_cypher_update`.
pub fn lower_cypher<E: IriEncoder>(
    ast: &CypherAst,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Query> {
    match &ast.statement {
        Statement::Query(q) => {
            let mut ctx = LoweringContext::new(encoder, vars);
            stmt::lower_query(&mut ctx, q)
        }
        Statement::Update(_) => Err(LowerError::WriteOnQueryPath),
    }
}
