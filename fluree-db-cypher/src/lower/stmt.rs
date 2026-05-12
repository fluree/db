//! Statement-level read-path lowering.

use fluree_db_query::ir::{Pattern, Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::sort::{SortDirection, SortSpec};
use fluree_db_query::var_registry::VarId;
use fluree_graph_json_ld::ParsedContext;

use crate::ast::{Expr, OrderDirection, ProjectionItem, ReadClause, ReturnClause};

use super::context::LoweringContext;
use super::expr::lower_expr;
use super::pattern::lower_pattern;
use super::{LowerError, Result};

pub fn lower_query<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    q: &crate::ast::Query,
) -> Result<Query> {
    let mut patterns: Vec<Pattern> = Vec::new();
    for clause in &q.clauses {
        match clause {
            ReadClause::Match(m) => {
                let mut p = lower_pattern(ctx, &m.pattern)?;
                patterns.append(&mut p);
                if let Some(w) = &m.where_clause {
                    let f = lower_expr(ctx, w)?;
                    patterns.push(Pattern::Filter(f));
                }
            }
            ReadClause::OptionalMatch(m) => {
                let mut inner = lower_pattern(ctx, &m.pattern)?;
                if let Some(w) = &m.where_clause {
                    let f = lower_expr(ctx, w)?;
                    inner.push(Pattern::Filter(f));
                }
                patterns.push(Pattern::Optional(inner));
            }
            ReadClause::With(_) => {
                return Err(LowerError::unsupported(
                    "WITH (subquery boundary) is deferred in v1 lowering — initial slice covers single-MATCH queries",
                ));
            }
            ReadClause::Unwind(_) => {
                return Err(LowerError::unsupported(
                    "UNWIND is deferred in v1 lowering — initial slice covers MATCH/WHERE/RETURN",
                ));
            }
        }
    }

    let (output, ordering, limit, offset) = lower_return(ctx, &q.return_clause, &patterns)?;

    Ok(Query {
        context: ParsedContext::new(),
        orig_context: None,
        output,
        patterns,
        grouping: None,
        ordering,
        limit,
        offset,
        reasoning: Default::default(),
        post_values: None,
        // System-fact filter ON — hides f:reifies* from untyped relationship matches.
        include_system_facts: false,
    })
}

type LoweredReturn = (QueryOutput, Vec<SortSpec>, Option<usize>, Option<usize>);

fn lower_return<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    r: &ReturnClause,
    _patterns: &[Pattern],
) -> Result<LoweredReturn> {
    // Project items — only Var and Var-as-alias supported in v1. `*`
    // becomes Wildcard.
    let mut vars: Vec<VarId> = Vec::new();
    let mut saw_star = false;
    for item in &r.items {
        match &item.expr {
            Expr::Var(v) if v.name == "*" => {
                saw_star = true;
            }
            Expr::Var(v) => {
                if item.alias.is_some() {
                    // RETURN ... AS alias is parsed but the v1 lower
                    // path doesn't yet rename the projected column —
                    // returning the original variable would silently
                    // discard the alias in the result schema.
                    return Err(LowerError::unsupported(
                        "RETURN ... AS alias is deferred in v1 — drop the alias for now",
                    ));
                }
                vars.push(ctx.intern_var(&v.name));
            }
            other => {
                return Err(LowerError::unsupported(format!(
                    "RETURN of `{:?}` is deferred — v1 covers RETURN of variables only",
                    other_kind(other)
                )));
            }
        }
    }

    let output = if saw_star {
        if r.distinct {
            QueryOutput::wildcard_distinct()
        } else {
            QueryOutput::wildcard()
        }
    } else if r.distinct {
        QueryOutput::select_distinct(vars)
    } else {
        QueryOutput::select_all(vars)
    };

    let limit = const_usize(&r.limit)?;
    let offset = const_usize(&r.skip)?;

    let ordering = lower_order_by(ctx, &r.order_by)?;

    Ok((output, ordering, limit, offset))
}

fn lower_order_by<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    items: &[crate::ast::OrderItem],
) -> Result<Vec<SortSpec>> {
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        // v1 only supports `ORDER BY <var>`. Expression-keyed ordering
        // would need a pre-BIND, which we defer.
        let var = match &it.expr {
            Expr::Var(v) => ctx.intern_var(&v.name),
            _ => {
                return Err(LowerError::unsupported(
                    "ORDER BY accepts only a variable in v1 — use `WITH expr AS alias ORDER BY alias` once WITH lands",
                ));
            }
        };
        let direction = match it.direction {
            OrderDirection::Ascending => SortDirection::Ascending,
            OrderDirection::Descending => SortDirection::Descending,
        };
        out.push(SortSpec { var, direction });
    }
    Ok(out)
}

fn const_usize(e: &Option<Expr>) -> Result<Option<usize>> {
    match e {
        None => Ok(None),
        Some(Expr::Lit(crate::ast::Literal::Integer(n, _))) => Ok(Some((*n).max(0) as usize)),
        Some(_) => Err(LowerError::unsupported(
            "non-literal SKIP/LIMIT is deferred — write a literal integer",
        )),
    }
}

fn other_kind(e: &Expr) -> &'static str {
    match e {
        Expr::Lit(_) => "literal",
        Expr::Call(_) => "function call",
        Expr::Prop(_, _, _) => "property accessor",
        Expr::BinOp(_, _, _, _) => "binary op",
        Expr::UnaryOp(_, _, _) => "unary op",
        Expr::List(_, _) => "list literal",
        Expr::Case(_) => "CASE",
        Expr::Exists(_, _) => "EXISTS",
        Expr::In(_, _, _) => "IN",
        Expr::IsNull(_, _) | Expr::IsNotNull(_, _) => "null test",
        Expr::StartsWith(_, _, _) | Expr::EndsWith(_, _, _) | Expr::Contains(_, _, _) => {
            "string predicate"
        }
        Expr::Param(_) => "parameter",
        Expr::Var(_) => "variable",
    }
}

// Convenience aliases so the unused-import linter relaxes on items
// we plan to use in the next slice.
#[allow(dead_code)]
fn _retain(_p: &ProjectionItem) {}
