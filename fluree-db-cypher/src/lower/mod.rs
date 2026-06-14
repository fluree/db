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
    #[error(
        "bare node pattern `({0})` rejected — see GQL_CYPHER_SUPPORT.md \"Node existence model\""
    )]
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

/// Lower a Cypher AST to a `Query` with the default lowering context
/// (`@vocab` = `http://example.org/`, no overrides). Useful for tests
/// and for callers that don't have a ledger context to apply.
///
/// Most callers should use [`lower_cypher_with_context`] and pass a
/// `LoweringContext` configured with the ledger's `@vocab` and
/// term overrides.
pub fn lower_cypher<E: IriEncoder>(
    ast: &CypherAst,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Query> {
    let mut ctx = LoweringContext::new(encoder, vars);
    lower_with_context(ast, &mut ctx)
}

/// Lower a Cypher AST to a `Query` using a caller-provided
/// `LoweringContext`. This is the entry point API callers should use
/// to apply ledger-context `@vocab` and term mappings to bare Cypher
/// identifiers.
pub fn lower_cypher_with_context<E: IriEncoder>(
    ast: &CypherAst,
    ctx: &mut LoweringContext<'_, E>,
) -> Result<Query> {
    lower_with_context(ast, ctx)
}

fn lower_with_context<E: IriEncoder>(
    ast: &CypherAst,
    ctx: &mut LoweringContext<'_, E>,
) -> Result<Query> {
    match &ast.statement {
        Statement::Query(q) => stmt::lower_query(ctx, q),
        Statement::Update(_) => Err(LowerError::WriteOnQueryPath),
    }
}
