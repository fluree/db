//! Cypher AST → fluree-db-query IR.
//!
//! This module is read-path only; write-path lowering lives in
//! `fluree-db-transact/src/lower_cypher_update.rs`. See
//! `docs/concepts/cypher.md` for how Cypher maps onto the RDF model.

mod annotation_use;
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
        "bare node pattern `({0})` is not supported — a node must be constrained by a label, a property, or a relationship"
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
/// (no `@vocab`: bare identifiers stay namespace-0 names; no
/// overrides). Useful for tests and for callers that don't have a
/// ledger context to apply.
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
        Statement::Query(q) => {
            ctx.set_annotation_dependent(annotation_use::annotation_dependent_vars(q));
            let mut query = stmt::lower_query(ctx, q)?;
            absorb_shortest_path_node_filters(&mut query.patterns);
            Ok(query)
        }
        Statement::Update(_) => Err(LowerError::WriteOnQueryPath),
        Statement::Schema(cmd) => match cmd.kind {
            // Fluree has no user-managed index/constraint catalog: SHOW
            // answers zero rows (with the shared Neo4j column heads) so
            // migration tooling iterating the result proceeds cleanly.
            crate::ast::SchemaCommandKind::ShowSchema => {
                let q = schema_show_query(cmd.span);
                stmt::lower_query(ctx, &q)
            }
            _ => Err(LowerError::WriteOnQueryPath),
        },
        // Procedure shims are resolved at the API layer (they need ledger
        // stats to answer); by the time lowering runs they have been
        // rewritten to a constant-rows Query. Reaching here means a caller
        // skipped that rewrite.
        Statement::CallProcedure(call) => Err(LowerError::unsupported(format!(
            "CALL {}() must be resolved against a ledger before lowering",
            call.name
        ))),
    }
}

/// Zero-row, fixed-column read for `SHOW INDEXES` / `SHOW CONSTRAINTS`:
/// an empty `InlineRows` over the column set the two SHOW forms share.
fn schema_show_query(span: crate::span::SourceSpan) -> crate::ast::Query {
    use crate::ast::{Expr, ProjectionItem, Query, ReadClause, ReturnClause, Variable};
    let cols = [
        "id",
        "name",
        "type",
        "entityType",
        "labelsOrTypes",
        "properties",
    ];
    let vars: Vec<Variable> = cols
        .iter()
        .map(|c| Variable {
            name: (*c).to_string(),
            span,
        })
        .collect();
    Query {
        clauses: vec![ReadClause::InlineRows {
            vars: vars.clone(),
            rows: Vec::new(),
        }],
        return_clause: ReturnClause {
            items: vars
                .into_iter()
                .map(|v| ProjectionItem {
                    expr: Expr::Var(v),
                    alias: None,
                    span,
                })
                .collect(),
            distinct: false,
            order_by: Vec::new(),
            skip: None,
            limit: None,
            span,
        },
        union_tail: None,
        span,
    }
}

use fluree_db_query::ir::expression::ListPredicateKind;
use fluree_db_query::ir::{Expression, Function, PathNodeFilter, Pattern};
use fluree_db_query::var_registry::VarId;

/// Push a trailing `WHERE all(x IN nodes(p) WHERE …)` node predicate into the
/// preceding `shortestPath` pattern so the search finds the shortest
/// *qualifying* path, rather than post-filtering the unconstrained shortest
/// path (which returns empty whenever that one path violates the predicate —
/// wrong per openCypher). Recurses into subqueries and optionals.
fn absorb_shortest_path_node_filters(patterns: &mut Vec<Pattern>) {
    for p in patterns.iter_mut() {
        match p {
            Pattern::Subquery(sq) => absorb_shortest_path_node_filters(&mut sq.patterns),
            Pattern::Optional(inner) => absorb_shortest_path_node_filters(inner),
            _ => {}
        }
    }

    let path_vars: Vec<VarId> = patterns
        .iter()
        .filter_map(|p| match p {
            Pattern::ShortestPath(sp) => Some(sp.path_var),
            _ => None,
        })
        .collect();

    for pv in path_vars {
        // Extract every `all(x IN nodes(pv) WHERE …)` filter, AND-combining
        // their predicates (a path node must satisfy all of them).
        let mut combined: Option<PathNodeFilter> = None;
        let mut i = 0;
        while i < patterns.len() {
            let matched = match &patterns[i] {
                Pattern::Filter(expr) => match_all_over_path_nodes(expr, pv),
                _ => None,
            };
            match matched {
                Some((var, predicate)) => {
                    combined = Some(match combined {
                        None => PathNodeFilter { var, predicate },
                        Some(mut acc) => {
                            let mut predicate = predicate;
                            predicate.substitute_var(var, acc.var);
                            acc.predicate = Expression::Call {
                                func: Function::And,
                                args: vec![acc.predicate, predicate],
                            };
                            acc
                        }
                    });
                    patterns.remove(i);
                }
                None => i += 1,
            }
        }
        if let Some(nf) = combined {
            for p in patterns.iter_mut() {
                if let Pattern::ShortestPath(sp) = p {
                    if sp.path_var == pv {
                        sp.node_filter = Some(nf);
                        break;
                    }
                }
            }
        }
    }
}

/// If `expr` is `all(var IN nodes(path_var) WHERE predicate)` and `predicate`
/// references only `var` (so it is safe to evaluate per node with no other
/// bindings), return `(var, predicate)`.
fn match_all_over_path_nodes(expr: &Expression, path_var: VarId) -> Option<(VarId, Expression)> {
    let Expression::ListPredicate {
        kind: ListPredicateKind::All,
        var,
        list,
        predicate,
    } = expr
    else {
        return None;
    };
    let Expression::Call {
        func: Function::Nodes,
        args,
    } = list.as_ref()
    else {
        return None;
    };
    match args.as_slice() {
        [Expression::Var(v)] if *v == path_var => {}
        _ => return None,
    }
    if predicate.referenced_vars().iter().any(|r| r != var) {
        return None;
    }
    Some((*var, (**predicate).clone()))
}
