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

    let (output, ordering, limit, offset) = lower_return(ctx, &q.return_clause, &mut patterns)?;

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
    patterns: &mut Vec<Pattern>,
) -> Result<LoweredReturn> {
    let mut vars: Vec<VarId> = Vec::new();
    let mut saw_star = false;
    let mut alias_counter = 0u32;
    for item in &r.items {
        // `RETURN *` is the wildcard projection.
        if let Expr::Var(v) = &item.expr {
            if v.name == "*" {
                saw_star = true;
                continue;
            }
            // Bare variable without alias — project directly.
            if item.alias.is_none() {
                vars.push(ctx.intern_var(&v.name));
                continue;
            }
        }

        // Any other shape — including a bare Var with an alias, a
        // computed expression with or without an alias — lowers via a
        // `Bind` pattern that introduces a fresh (or aliased) VarId.
        let lowered = lower_expr(ctx, &item.expr)?;
        let alias_id = match &item.alias {
            Some(alias) => ctx.intern_var(&alias.name),
            None => {
                let name = format!("?#__ret_{alias_counter}");
                alias_counter += 1;
                ctx.intern_var(&name)
            }
        };
        patterns.push(Pattern::Bind {
            var: alias_id,
            expr: lowered,
        });
        vars.push(alias_id);
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

// Silence the unused-import linter for items reused by other slices.
#[allow(dead_code)]
fn _retain(_p: &ProjectionItem) {}
