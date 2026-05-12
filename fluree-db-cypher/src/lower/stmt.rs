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
/// Cypher rules enforced here:
/// - **Column-name match.** Every branch must project the same VarIds
///   in the same order. Wildcard projection (`RETURN *`) is rejected
///   in UNION branches because the projected-vars list is opaque
///   (it expands at execution time to whatever is bound).
/// - **Uniform variant.** The openCypher spec disallows mixing
///   `UNION` and `UNION ALL` in the same chain — every join in the
///   chain must use the same variant. We enforce this rather than
///   silently collapsing to a single global bag/set choice.
fn lower_union_query<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    q: &crate::ast::Query,
) -> Result<Query> {
    let mut branches: Vec<Vec<Pattern>> = Vec::new();
    let mut union_variant: Option<bool> = None; // Some(true) = UNION ALL, Some(false) = UNION
    let mut projected_vars: Option<Vec<VarId>> = None;

    let mut cursor = q;
    loop {
        // Reject RETURN * in branches — wildcard's projected-vars
        // list is empty at lower time, so column-name compatibility
        // can't be checked. Users must enumerate columns.
        if uses_wildcard_return(cursor) {
            return Err(LowerError::unsupported(
                "RETURN * is rejected in UNION branches — list columns explicitly so column-name compatibility can be checked at lower time",
            ));
        }

        let branch = lower_single_branch(ctx, cursor)?;
        let branch_vars = branch.projected_vars();
        if branch_vars.is_empty() {
            return Err(LowerError::unsupported(
                "UNION branches must project at least one column",
            ));
        }
        match &projected_vars {
            None => projected_vars = Some(branch_vars.clone()),
            Some(existing) if existing != &branch_vars => {
                return Err(LowerError::unsupported(
                    "UNION branches must project the same columns in the same order (Cypher's column-name-match rule)",
                ));
            }
            _ => {}
        }
        branches.push(vec![branch.into_subquery_pattern(branch_vars)]);

        match &cursor.union_tail {
            Some(tail) => {
                match union_variant {
                    None => union_variant = Some(tail.all),
                    Some(prev) if prev != tail.all => {
                        return Err(LowerError::unsupported(
                            "mixing `UNION` and `UNION ALL` in the same chain is not allowed — every join in a UNION chain must use the same variant",
                        ));
                    }
                    _ => {}
                }
                cursor = &tail.right;
            }
            None => break,
        }
    }

    let projected = projected_vars.expect("at least one branch");
    let all = union_variant.unwrap_or(false);
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

/// Augment a subquery's select list with any VarIds referenced by
/// its ORDER BY specs that aren't already in the list.
///
/// SubqueryOperator's pipeline applies `ProjectOperator(select)`
/// BEFORE `SortOperator(ordering)`, so a sort key absent from
/// `select` is dropped before sorting can read it — silently
/// un-sorting the subquery's output. The synthetic property-accessor
/// vars (`?#__prop_*`) emitted by `lower_order_by` for `ORDER BY n.age`
/// are exactly this case.
///
/// Surfacing the extra vars to the outer scope is safe because the
/// `?#`-prefix convention causes the wildcard formatter to filter
/// them out of `RETURN *` output (see `fluree-db-api/src/format/mod.rs`).
fn augment_select_with_sort_vars(mut select: Vec<VarId>, ordering: &[SortSpec]) -> Vec<VarId> {
    for spec in ordering {
        if !select.contains(&spec.var) {
            select.push(spec.var);
        }
    }
    select
}

/// True if the query's RETURN uses `*`. Walks only the head query
/// (the caller iterates the chain).
fn uses_wildcard_return(q: &crate::ast::Query) -> bool {
    q.return_clause
        .items
        .iter()
        .any(|item| matches!(&item.expr, Expr::Var(v) if v.name == "*"))
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
        // SubqueryOperator applies ProjectOperator BEFORE SortOperator,
        // so any sort key that isn't in `select` is dropped before
        // sorting runs. Augment `select` with vars referenced by
        // ordering. The synthetic property-accessor vars use the
        // `?#__prop_*` naming convention which the wildcard
        // formatter already filters from `RETURN *` output, so
        // surfacing them into the outer scope is invisible to users.
        let augmented_select = augment_select_with_sort_vars(select, &self.ordering);
        let mut sq = SubqueryPattern::new(augmented_select, self.patterns);
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
                    // Property accessors and other side-effecting
                    // sub-expressions append auxiliary triples
                    // before the Filter.
                    let f = lower_expr(ctx, w, &mut patterns)?;
                    patterns.push(Pattern::Filter(f));
                }
            }
            ReadClause::OptionalMatch(m) => {
                let mut inner = lower_pattern(ctx, &m.pattern)?;
                if let Some(w) = &m.where_clause {
                    let f = lower_expr(ctx, w, &mut inner)?;
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

    let (output, ordering, limit, offset, group_keys, aggregates) =
        lower_return(ctx, &q.return_clause, &mut patterns)?;

    // When the projection mixes aggregates with non-aggregate items,
    // the non-aggregates become GROUP BY keys (Cypher's implicit
    // grouping rule). RETURN has no WHERE/HAVING — those live on WITH.
    let grouping = Grouping::assemble(group_keys, aggregates, Vec::new(), None);

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
    Vec<VarId>,
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

    let ordering = lower_order_by(ctx, &r.order_by, patterns)?;

    // GROUP BY keys are only meaningful when aggregates exist; if not,
    // pass an empty list so Grouping::assemble produces None.
    let group_keys = if projection.aggregates.is_empty() {
        Vec::new()
    } else {
        projection.group_keys
    };

    Ok((
        output,
        ordering,
        limit,
        offset,
        group_keys,
        projection.aggregates,
    ))
}

/// Shared state used while lowering a projection list (RETURN, WITH).
///
/// Each item either: marks `saw_star`, is a bare-var projection
/// (push to `vars` and `group_keys`), is an aggregate (push to
/// `aggregates` + project the aggregate's output VarId), or is a
/// general expression (emit a `Bind` + project the bound VarId,
/// which then participates in GROUP BY as a non-aggregate key).
///
/// `group_keys` holds the non-aggregate projected VarIds. When the
/// projection mixes aggregates with non-aggregate items, Cypher
/// semantics implicitly group by every non-aggregate projection.
/// `Grouping::assemble` consumes `group_keys` as the GROUP BY list,
/// producing `Grouping::Explicit` if both lists are populated and
/// `Grouping::Implicit` for aggregates-only.
struct ProjectionState {
    vars: Vec<VarId>,
    group_keys: Vec<VarId>,
    aggregates: Vec<AggregateSpec>,
    saw_star: bool,
    alias_counter: u32,
}

impl ProjectionState {
    fn new() -> Self {
        Self {
            vars: Vec::new(),
            group_keys: Vec::new(),
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
                let id = ctx.intern_var(&v.name);
                self.vars.push(id);
                self.group_keys.push(id);
                return Ok(());
            }
        }
        if let Expr::Call(call) = &item.expr {
            if let Some(agg_fn) = aggregate_fn(&call.name, call.distinct) {
                let output_var = aggregate_output_var(ctx, &item.alias, &mut self.alias_counter);
                let input_var = aggregate_input_var(ctx, call, &agg_fn, patterns)?;
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
        let lowered = lower_expr(ctx, &item.expr, patterns)?;
        let alias_id = aggregate_output_var(ctx, &item.alias, &mut self.alias_counter);
        patterns.push(Pattern::Bind {
            var: alias_id,
            expr: lowered,
        });
        self.vars.push(alias_id);
        self.group_keys.push(alias_id);
        Ok(())
    }

    /// Returns the set of VarIds produced by aggregate stages. Used
    /// by the WITH lowering to decide whether a WHERE expression
    /// references aggregate outputs (= HAVING) or only pre-aggregation
    /// bindings (= pre-aggregation Filter).
    fn aggregate_output_vars(&self) -> std::collections::HashSet<VarId> {
        self.aggregates.iter().map(|a| a.output_var).collect()
    }
}

/// True if `expr` references any of the given VarIds.
fn expression_references_any(
    expr: &fluree_db_query::ir::Expression,
    vars: &std::collections::HashSet<VarId>,
) -> bool {
    use fluree_db_query::ir::Expression;
    match expr {
        Expression::Var(v) => vars.contains(v),
        Expression::Const(_) => false,
        Expression::Call { args, .. } => args.iter().any(|a| expression_references_any(a, vars)),
        Expression::Exists { .. } => false,
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
/// a bare-variable VarId for `count(n)` / `sum(n)` / etc., or a
/// property-accessor's synthetic VarId for `sum(n.age)`. Other
/// expression-valued arguments (`sum(n.age * 2)`) are deferred —
/// they need a pre-aggregation `Bind`.
fn aggregate_input_var<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    call: &FuncCall,
    agg_fn: &AggregateFn,
    aux: &mut Vec<Pattern>,
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
        Expr::Prop(target, key, _) => {
            let var = crate::lower::expr::resolve_property_accessor(ctx, target, key, aux)?;
            Ok(Some(var))
        }
        _ => Err(LowerError::unsupported(format!(
            "{}() argument must be a bare variable or property accessor in v1 — other expressions are deferred",
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
    aux: &mut Vec<Pattern>,
) -> Result<Vec<SortSpec>> {
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        // ORDER BY accepts a bare variable or a property accessor
        // (`ORDER BY n.age`). The latter resolves through the same
        // helper that handles `n.age` in WHERE/RETURN, emitting the
        // property triple into `aux` so the ordering key is bound
        // before SortSpec evaluates.
        let var = match &it.expr {
            Expr::Var(v) => ctx.intern_var(&v.name),
            Expr::Prop(target, key, _) => {
                crate::lower::expr::resolve_property_accessor(ctx, target, key, aux)?
            }
            _ => {
                return Err(LowerError::unsupported(
                    "ORDER BY accepts a variable or a property accessor in v1 (e.g., `ORDER BY n` or `ORDER BY n.age`) — richer expression-keyed ordering needs an explicit `WITH expr AS alias ORDER BY alias`",
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

    // WITH WHERE routing:
    //
    // - If no aggregates are projected, the WHERE filters the
    //   subquery's pre-projection solution stream (regular Filter).
    // - If aggregates are projected, the WHERE is HAVING-shaped: it
    //   evaluates on per-group output rows. We don't try to split a
    //   composite expression into pre-aggregation Filter + HAVING in
    //   v1; the entire WITH WHERE becomes HAVING when aggregates are
    //   present. References to non-aggregate group keys still
    //   evaluate correctly because those bindings survive aggregation.
    //
    // The earlier broken behavior pushed the WHERE into `inner_patterns`
    // as a Filter, which ran before aggregation and made any
    // reference to an aggregate output variable (e.g. `WHERE c > 0`
    // after `count(*) AS c`) silently match zero rows.
    let having = if let Some(where_expr) = &w.where_clause {
        // Auxiliary triples (property accessors) emitted by the
        // WHERE expression go into the subquery body before any
        // Filter — they bind the values the WHERE needs to read.
        let lowered = lower_expr(ctx, where_expr, &mut inner_patterns)?;
        let agg_outputs = projection.aggregate_output_vars();
        let references_aggregate =
            !agg_outputs.is_empty() && expression_references_any(&lowered, &agg_outputs);
        if !projection.aggregates.is_empty() && references_aggregate {
            Some(lowered)
        } else if projection.aggregates.is_empty() {
            inner_patterns.push(Pattern::Filter(lowered));
            None
        } else {
            // Aggregates exist but the WHERE doesn't reference any
            // aggregate output — keep it as HAVING too. Pre-grouping
            // filters belong in a MATCH WHERE clause that runs before
            // the WITH, not after.
            Some(lowered)
        }
    } else {
        None
    };

    let group_keys = if projection.aggregates.is_empty() {
        Vec::new()
    } else {
        projection.group_keys
    };

    let grouping = Grouping::assemble(group_keys, projection.aggregates, Vec::new(), having);

    // ORDER BY may also reference property accessors; emit any
    // resulting auxiliary triples into the subquery body before we
    // hand patterns to SubqueryPattern.
    let ordering = lower_order_by(ctx, &w.order_by, &mut inner_patterns)?;

    // SubqueryOperator runs Project BEFORE Sort. Sort keys not in
    // `select` are dropped before the sort can see them, silently
    // un-sorting the result. Augment `select` with any vars the
    // ordering references (the synthetic `?#__prop_*` names stay
    // hidden from `RETURN *` via the wildcard formatter filter).
    let augmented_select = augment_select_with_sort_vars(projection.vars, &ordering);
    let mut sq = SubqueryPattern::new(augmented_select, inner_patterns);

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
