//! Statement-level read-path lowering.

use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::grouping::{AggregateFn, AggregateSpec, Grouping};
use fluree_db_query::ir::{Pattern, Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::sort::{SortDirection, SortSpec};
use fluree_db_query::var_registry::VarId;
use fluree_graph_json_ld::ParsedContext;
use fluree_vocab::namespaces::XSD;
use fluree_vocab::xsd_names;

use crate::ast::{
    Expr, FuncCall, Literal, OrderDirection, ProjectionItem, ReadClause, ReturnClause, UnwindClause,
};

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
            ReadClause::Unwind(u) => {
                patterns.push(lower_unwind(ctx, u)?);
            }
        }
    }

    let (output, ordering, limit, offset, aggregates) =
        lower_return(ctx, &q.return_clause, &mut patterns)?;

    // v1 has no GROUP BY surface, so any aggregates lift into an
    // implicit single-group `Grouping`.
    let grouping = Grouping::assemble(Vec::new(), aggregates, Vec::new(), None);

    Ok(Query {
        context: ParsedContext::new(),
        orig_context: None,
        output,
        patterns,
        grouping,
        ordering,
        limit,
        offset,
        reasoning: Default::default(),
        post_values: None,
        // System-fact filter ON — hides f:reifies* from untyped relationship matches.
        include_system_facts: false,
    })
}

type LoweredReturn = (
    QueryOutput,
    Vec<SortSpec>,
    Option<usize>,
    Option<usize>,
    Vec<AggregateSpec>,
);

fn lower_return<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    r: &ReturnClause,
    patterns: &mut Vec<Pattern>,
) -> Result<LoweredReturn> {
    let mut vars: Vec<VarId> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
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

        // Aggregate function calls in projection position lift into
        // `AggregateSpec` entries and project the aggregate's output
        // variable. No `Bind` is emitted — the planner inserts an
        // aggregate stage.
        if let Expr::Call(call) = &item.expr {
            if let Some(agg_fn) = aggregate_fn(&call.name, call.distinct) {
                let output_var = aggregate_output_var(ctx, &item.alias, &mut alias_counter);
                let input_var = aggregate_input_var(ctx, call, &agg_fn)?;
                aggregates.push(AggregateSpec {
                    function: agg_fn,
                    input_var,
                    output_var,
                    // The dedicated `CountDistinct` variant handles
                    // distinct semantics for COUNT internally; for
                    // SUM/AVG/etc. we forward the user's `DISTINCT`
                    // marker.
                    distinct: call.distinct
                        && !matches!(call.name.to_ascii_lowercase().as_str(), "count"),
                });
                vars.push(output_var);
                continue;
            }
        }

        // Any other shape — including a bare Var with an alias, a
        // computed expression with or without an alias — lowers via a
        // `Bind` pattern that introduces a fresh (or aliased) VarId.
        let lowered = lower_expr(ctx, &item.expr)?;
        let alias_id = aggregate_output_var(ctx, &item.alias, &mut alias_counter);
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

    Ok((output, ordering, limit, offset, aggregates))
}

/// Map a Cypher function name to an `AggregateFn`, if it is one of
/// the v1-supported aggregates. Returns `None` for non-aggregate
/// functions (which fall through to the scalar Bind lowering).
fn aggregate_fn(name: &str, distinct: bool) -> Option<AggregateFn> {
    Some(match name.to_ascii_lowercase().as_str() {
        "count" if distinct => AggregateFn::CountDistinct,
        "count" => AggregateFn::Count,
        "sum" => AggregateFn::Sum,
        "avg" => AggregateFn::Avg,
        "min" => AggregateFn::Min,
        "max" => AggregateFn::Max,
        _ => return None,
    })
}

/// Resolve the aggregate's input variable: `None` for `count(*)`,
/// a bare-variable VarId otherwise. Expression-valued aggregate
/// arguments (e.g. `sum(n.age * 2)`) are deferred — they need a
/// pre-aggregation `Bind`.
fn aggregate_input_var<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    call: &FuncCall,
    agg_fn: &AggregateFn,
) -> Result<Option<VarId>> {
    if call.args.is_empty() {
        // count(*) — no input variable
        if !matches!(agg_fn, AggregateFn::Count) {
            return Err(LowerError::unsupported(format!(
                "{}(*) is not supported — only count(*) takes no argument",
                call.name
            )));
        }
        return Ok(None);
    }
    if call.args.len() != 1 {
        return Err(LowerError::unsupported(format!(
            "{}() takes exactly one argument in v1",
            call.name
        )));
    }
    match &call.args[0] {
        Expr::Var(v) => Ok(Some(ctx.intern_var(&v.name))),
        _ => Err(LowerError::unsupported(format!(
            "{}() argument must be a bare variable in v1 — expression arguments are deferred",
            call.name
        ))),
    }
}

/// Mint the output VarId for a projection item: the user's alias if
/// provided, otherwise a fresh synthetic.
fn aggregate_output_var<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    alias: &Option<crate::ast::Variable>,
    counter: &mut u32,
) -> VarId {
    match alias {
        Some(a) => ctx.intern_var(&a.name),
        None => {
            let name = format!("?#__ret_{counter}");
            *counter += 1;
            ctx.intern_var(&name)
        }
    }
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

/// Lower `UNWIND <list> AS x` to a `Pattern::Values` with one row per
/// list element. v1 supports inline literal lists; `$param`-bound
/// lists need API-layer parameter substitution and are deferred.
fn lower_unwind<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    u: &UnwindClause,
) -> Result<Pattern> {
    let items = match &u.expr {
        Expr::List(items, _) => items,
        Expr::Param(_) => {
            return Err(LowerError::unsupported(
                "UNWIND $param requires API-layer parameter substitution; submit pre-substituted UNWIND [a, b, c] AS x in v1",
            ));
        }
        _ => {
            return Err(LowerError::unsupported(
                "UNWIND accepts an inline list literal `[...]` in v1",
            ));
        }
    };

    let alias = ctx.intern_var(&u.alias.name);
    let mut rows: Vec<Vec<Binding>> = Vec::with_capacity(items.len());
    for item in items {
        let binding = literal_to_binding(item)?;
        rows.push(vec![binding]);
    }
    Ok(Pattern::Values {
        vars: vec![alias],
        rows,
    })
}

fn literal_to_binding(e: &Expr) -> Result<Binding> {
    let Expr::Lit(lit) = e else {
        return Err(LowerError::unsupported(
            "UNWIND list elements must be literals in v1 (no nested expressions yet)",
        ));
    };
    Ok(match lit {
        Literal::Integer(n, _) => {
            Binding::lit(FlakeValue::Long(*n), Sid::new(XSD, xsd_names::LONG))
        }
        Literal::Float(f, _) => {
            Binding::lit(FlakeValue::Double(*f), Sid::new(XSD, xsd_names::DOUBLE))
        }
        Literal::String(s, _) => Binding::lit(
            FlakeValue::String(s.clone()),
            Sid::new(XSD, xsd_names::STRING),
        ),
        Literal::Bool(b, _) => {
            Binding::lit(FlakeValue::Boolean(*b), Sid::new(XSD, xsd_names::BOOLEAN))
        }
        Literal::Null(_) => Binding::Unbound,
    })
}

// Silence the unused-import linter for items reused by other slices.
#[allow(dead_code)]
fn _retain(_p: &ProjectionItem, _d: &DatatypeConstraint) {}
