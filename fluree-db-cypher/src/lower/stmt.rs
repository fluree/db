//! Statement-level read-path lowering.

use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::grouping::{AggregateFn, AggregateSpec, Grouping};
use fluree_db_query::ir::{Pattern, Query, QueryOutput, SubqueryPattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::sort::{SortDirection, SortSpec};
use fluree_db_query::var_registry::VarId;
use fluree_graph_json_ld::ParsedContext;
use fluree_vocab::namespaces::XSD;
use fluree_vocab::xsd_names;

use crate::ast::{
    Expr, FuncCall, Literal, OrderDirection, ProjectionItem, ReadClause, ReturnClause,
    UnwindClause, WithClause,
};

use super::context::LoweringContext;
use super::expr::lower_expr;
use super::pattern::lower_pattern;
use super::{LowerError, Result};

pub fn lower_query<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    q: &crate::ast::Query,
) -> Result<Query> {
    if q.union_tail.is_some() {
        return lower_union_query(ctx, q);
    }

    let SingleBranch {
        patterns,
        output,
        ordering,
        limit,
        offset,
        grouping,
    } = lower_single_branch(ctx, q)?;

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

/// Lower a chain of UNION-connected queries into a single top-level
/// `Query` whose WHERE is a `Pattern::Union` of one branch per query.
/// Each branch is wrapped in a `Subquery` so it can carry its own
/// solution modifiers and aggregates.
///
/// Cypher requires every branch to project the same column names; we
/// enforce this on the projected `VarId` lists.
fn lower_union_query<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    q: &crate::ast::Query,
) -> Result<Query> {
    let mut branches: Vec<Vec<Pattern>> = Vec::new();
    let mut all = false; // any UNION ALL in the chain → ALL semantics overall
    let mut projected_vars: Option<Vec<VarId>> = None;

    let mut cursor = q;
    loop {
        let branch = lower_single_branch(ctx, cursor)?;
        let branch_vars = branch.projected_vars();
        match &projected_vars {
            None => projected_vars = Some(branch_vars),
            Some(existing) if existing != &branch_vars => {
                return Err(LowerError::unsupported(
                    "UNION branches must project the same columns in the same order (Cypher's column-name-match rule)",
                ));
            }
            _ => {}
        }
        branches.push(vec![
            branch.into_subquery_pattern(projected_vars.clone().unwrap())
        ]);

        match &cursor.union_tail {
            Some(tail) => {
                all = all || tail.all;
                cursor = &tail.right;
            }
            None => break,
        }
    }

    let projected = projected_vars.expect("at least one branch");
    let output = if all {
        QueryOutput::select_all(projected)
    } else {
        QueryOutput::select_distinct(projected)
    };

    Ok(Query {
        context: ParsedContext::new(),
        orig_context: None,
        output,
        patterns: vec![Pattern::Union(branches)],
        grouping: None,
        ordering: Vec::new(),
        limit: None,
        offset: None,
        reasoning: Default::default(),
        post_values: None,
        include_system_facts: false,
    })
}

struct SingleBranch {
    patterns: Vec<Pattern>,
    output: QueryOutput,
    ordering: Vec<SortSpec>,
    limit: Option<usize>,
    offset: Option<usize>,
    grouping: Option<Grouping>,
}

impl SingleBranch {
    /// The variables this branch projects, in order. Used to check
    /// column-name compatibility across UNION branches.
    fn projected_vars(&self) -> Vec<VarId> {
        self.output.projected_vars().unwrap_or_default()
    }

    /// Wrap this branch into a `Pattern::Subquery` for use as a UNION
    /// branch. The select list pins which variables surface out of
    /// the branch into the outer Union.
    fn into_subquery_pattern(self, select: Vec<VarId>) -> Pattern {
        let mut sq = SubqueryPattern::new(select, self.patterns);
        if let Some(limit) = self.limit {
            sq = sq.with_limit(limit);
        }
        if let Some(offset) = self.offset {
            sq = sq.with_offset(offset);
        }
        if !self.ordering.is_empty() {
            sq = sq.with_ordering(self.ordering);
        }
        if let Some(g) = self.grouping {
            sq = sq.with_grouping(g);
        }
        Pattern::Subquery(sq)
    }
}

fn lower_single_branch<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    q: &crate::ast::Query,
) -> Result<SingleBranch> {
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
            ReadClause::With(w) => {
                let subq = lower_with(ctx, w, std::mem::take(&mut patterns))?;
                patterns.push(Pattern::Subquery(subq));
            }
            ReadClause::Unwind(u) => {
                patterns.push(lower_unwind(ctx, u)?);
            }
        }
    }

    let (output, ordering, limit, offset, aggregates) =
        lower_return(ctx, &q.return_clause, &mut patterns)?;

    let grouping = Grouping::assemble(Vec::new(), aggregates, Vec::new(), None);

    Ok(SingleBranch {
        patterns,
        output,
        ordering,
        limit,
        offset,
        grouping,
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
    let mut projection = ProjectionState::new();
    for item in &r.items {
        projection.add_item(ctx, patterns, item)?;
    }

    let output = if projection.saw_star {
        if r.distinct {
            QueryOutput::wildcard_distinct()
        } else {
            QueryOutput::wildcard()
        }
    } else if r.distinct {
        QueryOutput::select_distinct(projection.vars)
    } else {
        QueryOutput::select_all(projection.vars)
    };

    let limit = const_usize(&r.limit)?;
    let offset = const_usize(&r.skip)?;

    let ordering = lower_order_by(ctx, &r.order_by)?;

    Ok((output, ordering, limit, offset, projection.aggregates))
}

/// Shared state used while lowering a projection list (RETURN, WITH).
///
/// Each item either: marks `saw_star`, is a bare-var projection
/// (push to `vars`), is an aggregate (push to `aggregates` + project
/// the aggregate's output VarId), or is a general expression (emit a
/// `Bind` pattern + project the bound VarId).
struct ProjectionState {
    vars: Vec<VarId>,
    aggregates: Vec<AggregateSpec>,
    saw_star: bool,
    alias_counter: u32,
}

impl ProjectionState {
    fn new() -> Self {
        Self {
            vars: Vec::new(),
            aggregates: Vec::new(),
            saw_star: false,
            alias_counter: 0,
        }
    }

    fn add_item<E: IriEncoder>(
        &mut self,
        ctx: &mut LoweringContext<'_, E>,
        patterns: &mut Vec<Pattern>,
        item: &ProjectionItem,
    ) -> Result<()> {
        if let Expr::Var(v) = &item.expr {
            if v.name == "*" {
                self.saw_star = true;
                return Ok(());
            }
            if item.alias.is_none() {
                self.vars.push(ctx.intern_var(&v.name));
                return Ok(());
            }
        }
        if let Expr::Call(call) = &item.expr {
            if let Some(agg_fn) = aggregate_fn(&call.name, call.distinct) {
                let output_var = aggregate_output_var(ctx, &item.alias, &mut self.alias_counter);
                let input_var = aggregate_input_var(ctx, call, &agg_fn)?;
                self.aggregates.push(AggregateSpec {
                    function: agg_fn,
                    input_var,
                    output_var,
                    distinct: call.distinct
                        && !matches!(call.name.to_ascii_lowercase().as_str(), "count"),
                });
                self.vars.push(output_var);
                return Ok(());
            }
        }
        let lowered = lower_expr(ctx, &item.expr)?;
        let alias_id = aggregate_output_var(ctx, &item.alias, &mut self.alias_counter);
        patterns.push(Pattern::Bind {
            var: alias_id,
            expr: lowered,
        });
        self.vars.push(alias_id);
        Ok(())
    }
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

/// Lower a `WITH` clause boundary to a `Pattern::Subquery`. All
/// previously-accumulated patterns become the subquery body. The
/// WITH's projection items become the subquery's select list.
/// WITH-induced modifiers (WHERE, ORDER BY, SKIP, LIMIT) and
/// aggregates apply inside the subquery.
fn lower_with<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    w: &WithClause,
    mut inner_patterns: Vec<Pattern>,
) -> Result<SubqueryPattern> {
    let mut projection = ProjectionState::new();
    for item in &w.items {
        projection.add_item(ctx, &mut inner_patterns, item)?;
    }
    if projection.saw_star {
        return Err(LowerError::unsupported(
            "WITH * is deferred in v1 — list the variables explicitly",
        ));
    }

    // WITH WHERE filters the subquery's solution stream.
    if let Some(where_expr) = &w.where_clause {
        let filter = lower_expr(ctx, where_expr)?;
        inner_patterns.push(Pattern::Filter(filter));
    }

    let grouping = Grouping::assemble(Vec::new(), projection.aggregates, Vec::new(), None);
    let mut sq = SubqueryPattern::new(projection.vars, inner_patterns);

    let ordering = lower_order_by(ctx, &w.order_by)?;
    if !ordering.is_empty() {
        sq = sq.with_ordering(ordering);
    }
    if let Some(limit) = const_usize(&w.limit)? {
        sq = sq.with_limit(limit);
    }
    if let Some(offset) = const_usize(&w.skip)? {
        sq = sq.with_offset(offset);
    }
    if w.distinct {
        sq = sq.with_distinct();
    }
    if let Some(g) = grouping {
        sq = sq.with_grouping(g);
    }
    Ok(sq)
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
