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
        order_binds: Vec::new(),
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
        order_binds: Vec::new(),
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
            ReadClause::InlineRows { vars, rows } => {
                patterns.push(lower_inline_rows(ctx, vars, rows)?);
            }
        }
    }

    let (output, ordering, limit, offset, group_keys, aggregates, post_binds) =
        lower_return(ctx, &q.return_clause, &mut patterns)?;

    // When the projection mixes aggregates with non-aggregate items,
    // the non-aggregates become GROUP BY keys (Cypher's implicit
    // grouping rule). RETURN has no WHERE/HAVING — those live on WITH.
    // `post_binds` carry aggregate-composite expressions (`count(a)+count(b)`).
    let grouping = Grouping::assemble(group_keys, aggregates, post_binds, None);

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
    Vec<(VarId, fluree_db_query::ir::Expression)>,
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

    let order_by = align_order_by_with_projection(&r.items, &r.order_by);
    reject_order_by_on_list(ctx, &order_by, &projection.list_outputs)?;
    let ordering = lower_order_by(ctx, &order_by, patterns)?;

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
        projection.post_binds,
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
    /// Output vars bound to `collect()` — list-valued. The list carrier
    /// (`Binding::Grouped`) is an internal value the sort/join/group key paths
    /// don't handle, so these vars must not reach ORDER BY (or flow out of a
    /// WITH into a downstream join/sort/group). Tracked here to reject those.
    list_outputs: std::collections::HashSet<VarId>,
    /// Post-aggregation binds: `output_var = <expr over aggregate outputs and
    /// literals>`, for aggregates composed into a larger expression
    /// (`count(a) + count(b)`, `count(m) + 1`, `sum(a) / count(b)`). Fire after
    /// every aggregate is computed, before HAVING.
    post_binds: Vec<(VarId, fluree_db_query::ir::Expression)>,
    /// Counter for synthetic per-aggregate output vars lifted out of composite
    /// expressions (`?#__agg_N`).
    agg_name_counter: u32,
}

impl ProjectionState {
    fn new() -> Self {
        Self {
            vars: Vec::new(),
            group_keys: Vec::new(),
            aggregates: Vec::new(),
            saw_star: false,
            alias_counter: 0,
            list_outputs: std::collections::HashSet::new(),
            post_binds: Vec::new(),
            agg_name_counter: 0,
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
            if is_aggregate(&call.name) {
                let output_var =
                    aggregate_output_var(ctx, &item.alias, &item.expr, &mut self.alias_counter);
                let input_var = aggregate_input_var(ctx, call, patterns)?;
                let function = build_aggregate_fn(&call.name, call.distinct, input_var)?;
                if call.name.eq_ignore_ascii_case("collect") {
                    self.list_outputs.insert(output_var);
                }
                self.aggregates.push(AggregateSpec {
                    function,
                    output_var,
                });
                self.vars.push(output_var);
                return Ok(());
            }
        }
        // An aggregate composed into a larger expression (`count(m) + 1`,
        // `count(a) + count(b)`, `sum(a) / count(b)`): lift each aggregate to
        // its own spec, then evaluate the surrounding expression as a
        // post-aggregation bind referencing those outputs.
        if expr_has_aggregate(&item.expr) {
            return self.add_aggregate_composite(ctx, patterns, item);
        }
        let lowered = lower_expr(ctx, &item.expr, patterns)?;
        let alias_id = aggregate_output_var(ctx, &item.alias, &item.expr, &mut self.alias_counter);
        patterns.push(Pattern::Bind {
            var: alias_id,
            expr: lowered,
        });
        self.vars.push(alias_id);
        self.group_keys.push(alias_id);
        Ok(())
    }

    /// Lower a projection item whose expression *contains* aggregates but isn't
    /// a bare aggregate call. Each aggregate sub-expression is lifted into a
    /// spec with a synthetic `?#__agg_N` output; the rewritten expression
    /// (aggregate outputs + literals) becomes a post-aggregation bind to the
    /// item's output variable.
    fn add_aggregate_composite<E: IriEncoder>(
        &mut self,
        ctx: &mut LoweringContext<'_, E>,
        patterns: &mut Vec<Pattern>,
        item: &ProjectionItem,
    ) -> Result<()> {
        let mut rewritten = item.expr.clone();
        extract_aggregates(
            ctx,
            &mut rewritten,
            patterns,
            &mut self.aggregates,
            &mut self.agg_name_counter,
        )?;
        if composite_references_grouping_value(&rewritten) {
            return Err(LowerError::unsupported(
                "an aggregate expression may combine aggregates with literals (e.g. \
                 `count(a) + count(b)`, `count(m) + 1`); referencing a grouping key inside the \
                 expression is deferred — project it separately and reference its alias",
            ));
        }
        let lowered = lower_expr(ctx, &rewritten, patterns)?;
        let output_var =
            aggregate_output_var(ctx, &item.alias, &item.expr, &mut self.alias_counter);
        self.post_binds.push((output_var, lowered));
        self.vars.push(output_var);
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

/// Rewrite ORDER BY keys that are written as an expression already projected
/// under an alias (e.g. `RETURN f.id AS friendId … ORDER BY f.id`) to reference
/// that alias. Under aggregation the sort runs post-grouping, where only group
/// keys and aggregate outputs survive; re-lowering the bare `f.id` would mint a
/// fresh pre-grouping variable absent from the sorted stream ("Sort variable
/// not found"). Aliasing makes `ORDER BY f.id` behave like `ORDER BY friendId`.
fn align_order_by_with_projection(
    items: &[ProjectionItem],
    order_by: &[crate::ast::OrderItem],
) -> Vec<crate::ast::OrderItem> {
    order_by
        .iter()
        .map(|oi| match projection_alias_for(items, &oi.expr) {
            Some(alias) => crate::ast::OrderItem {
                expr: Expr::Var(alias),
                direction: oi.direction,
            },
            None => oi.clone(),
        })
        .collect()
}

/// The alias a projection item gives to `e`, if any item projects exactly that
/// expression under an alias (bare variable or property accessor only).
fn projection_alias_for(items: &[ProjectionItem], e: &Expr) -> Option<crate::ast::Variable> {
    items.iter().find_map(|it| {
        let alias = it.alias.as_ref()?;
        expr_matches_ignoring_span(&it.expr, e).then(|| alias.clone())
    })
}

/// Structural equality for the projection-key shapes shared by a projection and
/// an ORDER BY — bare variables and property accessors — ignoring source spans
/// (the two occurrences are at different positions).
fn expr_matches_ignoring_span(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Var(x), Expr::Var(y)) => x.name == y.name,
        (Expr::Prop(xi, xf, _), Expr::Prop(yi, yf, _)) => {
            xf == yf && expr_matches_ignoring_span(xi, yi)
        }
        _ => false,
    }
}

/// Reject ORDER BY keys that reference a `collect()` list. The list carrier
/// (`Binding::Grouped`) is treated as an internal value by the sort comparator
/// (and the join/group key paths), so ordering by it is unsound until real
/// list semantics land. ORDER BY on *other* keys while a list is merely
/// projected is fine — the sort never inspects the list column.
fn reject_order_by_on_list<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    order_by: &[crate::ast::OrderItem],
    list_outputs: &std::collections::HashSet<VarId>,
) -> Result<()> {
    for item in order_by {
        if expr_touches_list(ctx, &item.expr, list_outputs) {
            return Err(LowerError::unsupported(
                "ORDER BY on a collect() list is not supported in v1 (list ordering is deferred)",
            ));
        }
    }
    Ok(())
}

/// True if `e` references a `collect()` list output (directly as a `collect()`
/// call, or via a variable bound to one).
fn expr_touches_list<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    e: &Expr,
    list_outputs: &std::collections::HashSet<VarId>,
) -> bool {
    match e {
        Expr::Var(v) => list_outputs.contains(&ctx.intern_var(&v.name)),
        Expr::Call(c) => {
            c.name.eq_ignore_ascii_case("collect")
                || c.args
                    .iter()
                    .any(|a| expr_touches_list(ctx, a, list_outputs))
        }
        Expr::BinOp(_, a, b, _)
        | Expr::In(a, b, _)
        | Expr::StartsWith(a, b, _)
        | Expr::EndsWith(a, b, _)
        | Expr::Contains(a, b, _) => {
            expr_touches_list(ctx, a, list_outputs) || expr_touches_list(ctx, b, list_outputs)
        }
        Expr::UnaryOp(_, a, _)
        | Expr::IsNull(a, _)
        | Expr::IsNotNull(a, _)
        | Expr::Prop(a, _, _) => expr_touches_list(ctx, a, list_outputs),
        _ => false,
    }
}

/// True if `e` contains an aggregate function call anywhere.
fn expr_has_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Call(c) => is_aggregate(&c.name) || c.args.iter().any(expr_has_aggregate),
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _)
        | Expr::Index(l, r, _) => expr_has_aggregate(l) || expr_has_aggregate(r),
        Expr::UnaryOp(_, x, _)
        | Expr::IsNull(x, _)
        | Expr::IsNotNull(x, _)
        | Expr::Prop(x, _, _) => expr_has_aggregate(x),
        Expr::List(items, _) => items.iter().any(expr_has_aggregate),
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) | Expr::Case(_) | Expr::Exists(_, _, _) => {
            false
        }
    }
}

/// Replace every aggregate call in `e` with a synthetic `?#__agg_N` variable,
/// pushing the corresponding [`AggregateSpec`] (its input lowered into
/// `patterns`, pre-grouping). After this, `e` references aggregate outputs,
/// literals, and any grouping values left in place.
fn extract_aggregates<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    e: &mut Expr,
    patterns: &mut Vec<Pattern>,
    aggregates: &mut Vec<AggregateSpec>,
    counter: &mut u32,
) -> Result<()> {
    // `collect()` is list-valued, so it can only be nested inside a *list*
    // function (`size`/`head`/…) — never in arithmetic / comparison, where it
    // would silently evaluate to null. The flag tracks whether the current
    // position is a direct argument of a list function (propagated through
    // nested list functions like `size(reverse(collect(x)))`).
    extract_aggregates_inner(ctx, e, patterns, aggregates, counter, false)
}

fn extract_aggregates_inner<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    e: &mut Expr,
    patterns: &mut Vec<Pattern>,
    aggregates: &mut Vec<AggregateSpec>,
    counter: &mut u32,
    collect_allowed: bool,
) -> Result<()> {
    match e {
        Expr::Call(call) if is_aggregate(&call.name) => {
            if call.name.eq_ignore_ascii_case("collect") && !collect_allowed {
                return Err(LowerError::unsupported(
                    "collect() inside an expression is only supported as the argument of a \
                     list function (size/head/last/tail/reverse) — e.g. `size(collect(x))`",
                ));
            }
            let name = format!("?#__agg_{counter}");
            *counter += 1;
            let output_var = ctx.intern_var(&name);
            let input_var = aggregate_input_var(ctx, call, patterns)?;
            let function = build_aggregate_fn(&call.name, call.distinct, input_var)?;
            aggregates.push(AggregateSpec {
                function,
                output_var,
            });
            *e = Expr::Var(crate::ast::Variable {
                name,
                span: call.span,
            });
            Ok(())
        }
        Expr::Call(call) => {
            // A list function's direct arguments may contain a collect; any
            // other function's arguments may not.
            let allow = is_list_function(&call.name);
            for a in &mut call.args {
                extract_aggregates_inner(ctx, a, patterns, aggregates, counter, allow)?;
            }
            Ok(())
        }
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _)
        | Expr::Index(l, r, _) => {
            extract_aggregates_inner(ctx, l, patterns, aggregates, counter, false)?;
            extract_aggregates_inner(ctx, r, patterns, aggregates, counter, false)
        }
        Expr::UnaryOp(_, x, _)
        | Expr::IsNull(x, _)
        | Expr::IsNotNull(x, _)
        | Expr::Prop(x, _, _) => {
            extract_aggregates_inner(ctx, x, patterns, aggregates, counter, false)
        }
        Expr::List(items, _) => {
            for it in items {
                extract_aggregates_inner(ctx, it, patterns, aggregates, counter, false)?;
            }
            Ok(())
        }
        Expr::Case(_) | Expr::Exists(_, _, _) => Err(LowerError::unsupported(
            "aggregates inside CASE / EXISTS are not supported in v1",
        )),
        Expr::Var(_) | Expr::Lit(_) | Expr::Param(_) => Ok(()),
    }
}

/// Cypher list functions that consume a list value (and may therefore wrap a
/// `collect()` in an expression).
fn is_list_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "size" | "head" | "last" | "tail" | "reverse"
    )
}

/// After [`extract_aggregates`], `e` should reference only aggregate outputs
/// (`?#__agg_*`) and literals. A remaining bare variable or property accessor
/// means the expression mixes a grouping value in — not yet supported.
fn composite_references_grouping_value(e: &Expr) -> bool {
    match e {
        Expr::Var(v) => !v.name.starts_with("?#__agg_"),
        Expr::Prop(_, _, _) => true,
        Expr::BinOp(_, l, r, _)
        | Expr::In(l, r, _)
        | Expr::StartsWith(l, r, _)
        | Expr::EndsWith(l, r, _)
        | Expr::Contains(l, r, _)
        | Expr::Index(l, r, _) => {
            composite_references_grouping_value(l) || composite_references_grouping_value(r)
        }
        Expr::UnaryOp(_, x, _) | Expr::IsNull(x, _) | Expr::IsNotNull(x, _) => {
            composite_references_grouping_value(x)
        }
        Expr::Call(c) => c.args.iter().any(composite_references_grouping_value),
        Expr::List(items, _) => items.iter().any(composite_references_grouping_value),
        Expr::Lit(_) | Expr::Param(_) | Expr::Case(_) | Expr::Exists(_, _, _) => false,
    }
}

/// True if `name` is one of the v1-supported Cypher aggregate
/// functions. Non-aggregate functions fall through to the scalar Bind
/// lowering.
fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "collect"
    )
}

/// Build the IR `AggregateFn` for a v1-supported Cypher aggregate,
/// embedding the resolved input variable and DISTINCT semantics. The
/// input var is `None` only for `count(*)` (→ `CountAll`).
fn build_aggregate_fn(name: &str, distinct: bool, input_var: Option<VarId>) -> Result<AggregateFn> {
    use fluree_db_query::ir::InputSemantics;
    let semantics = if distinct {
        InputSemantics::Set
    } else {
        InputSemantics::List
    };
    let lname = name.to_ascii_lowercase();
    Ok(match (lname.as_str(), input_var) {
        ("count", None) => AggregateFn::CountAll,
        ("count", Some(v)) if distinct => AggregateFn::CountDistinct(v),
        ("count", Some(v)) => AggregateFn::Count(v),
        ("sum", Some(v)) => AggregateFn::Sum(v, semantics),
        ("avg", Some(v)) => AggregateFn::Avg(v, semantics),
        ("min", Some(v)) => AggregateFn::Min(v),
        ("max", Some(v)) => AggregateFn::Max(v),
        ("collect", Some(v)) => AggregateFn::Collect(v, semantics),
        // sum/avg/min/max without an argument is already rejected in
        // aggregate_input_var; this arm is unreachable in practice.
        (other, _) => {
            return Err(LowerError::unsupported(format!(
                "{other}() is not a supported aggregate in v1"
            )))
        }
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
    aux: &mut Vec<Pattern>,
) -> Result<Option<VarId>> {
    if call.args.is_empty() {
        // count(*) — no input variable; only count takes no argument.
        if !call.name.eq_ignore_ascii_case("count") {
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
        // A list-literal argument (structured `collect([a, b])`) lowers to a
        // pre-aggregation Bind that builds the per-row list, then the aggregate
        // gathers those list values into a list of tuples.
        Expr::List(..) => {
            let expr = crate::lower::expr::lower_expr(ctx, &call.args[0], aux)?;
            let var = ctx.fresh_synth();
            aux.push(Pattern::Bind { var, expr });
            Ok(Some(var))
        }
        _ => Err(LowerError::unsupported(format!(
            "{}() argument must be a bare variable, property accessor, or list literal in v1 — other expressions are deferred",
            call.name
        ))),
    }
}

/// Mint the output VarId for a projection item: the user's alias if
/// provided, otherwise the projected expression's surface text so the
/// rendered column reads as Neo4j does (`a.name`, `count(m)`). The
/// surface label always contains a `.`, `(`, or operator and so can never
/// collide with a user variable (a bare identifier). Falls back to a
/// synthetic `?#__ret_N` only for expressions we can't render — those
/// still project correctly (the formatter emits every explicit column),
/// they just carry an opaque label.
fn aggregate_output_var<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    alias: &Option<crate::ast::Variable>,
    expr: &Expr,
    counter: &mut u32,
) -> VarId {
    match alias {
        Some(a) => ctx.intern_var(&a.name),
        None => match projection_label(expr) {
            Some(label) => ctx.intern_var(&label),
            None => {
                let name = format!("?#__ret_{counter}");
                *counter += 1;
                ctx.intern_var(&name)
            }
        },
    }
}

/// Render a projected expression to its Neo4j column label. Covers the
/// surface forms LDBC / openCypher actually project unaliased; returns
/// `None` for shapes with no obvious textual label.
fn projection_label(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Var(v) => Some(v.name.clone()),
        Expr::Prop(target, key, _) => Some(format!("{}.{}", projection_label(target)?, key)),
        Expr::Call(call) => {
            let args = call
                .args
                .iter()
                .map(|a| projection_label(a).unwrap_or_else(|| "…".to_string()))
                .collect::<Vec<_>>()
                .join(", ");
            let distinct = if call.distinct { "DISTINCT " } else { "" };
            Some(format!("{}({distinct}{args})", call.name))
        }
        _ => None,
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
            // An aggregate ORDER BY key must reference a projected alias (the
            // aggregate is computed by grouping, not re-derivable in a sort
            // Bind). `align_order_by_with_projection` already rewrites a key
            // that matches a projected item to its alias.
            other if expr_has_aggregate(other) => {
                return Err(LowerError::unsupported(
                    "ORDER BY over an aggregate must reference its projected alias \
                     (e.g. `RETURN count(x) AS c ORDER BY c`)",
                ));
            }
            // A general expression key (`ORDER BY toInteger(n.id)`, arithmetic,
            // etc.): lower it to a synthetic pre-sort Bind and order by that var.
            // Property accessors inside it emit their own aux patterns.
            other => {
                let expr = crate::lower::expr::lower_expr(ctx, other, aux)?;
                let var = ctx.fresh_synth();
                aux.push(Pattern::Bind { var, expr });
                var
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
    if !projection.list_outputs.is_empty() {
        // A `collect()` projected by WITH flows out as a `Binding::List`. List
        // functions over it work in the final RETURN (`RETURN size(collect(x))`),
        // but projecting the raw list *through* the WITH subquery boundary
        // currently nulls it (the subquery result projection drops the List —
        // a separate fix). Rather than return a silent null, keep collect() in
        // WITH deferred with a clear error.
        return Err(LowerError::unsupported(
            "collect() in WITH is deferred in v1 — wrap it in the final RETURN, \
             e.g. `RETURN size(collect(x))`",
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

    let grouping = Grouping::assemble(
        group_keys,
        projection.aggregates,
        projection.post_binds,
        having,
    );

    // ORDER BY may also reference property accessors; emit any
    // resulting auxiliary triples into the subquery body before we
    // hand patterns to SubqueryPattern. Align accessor/var keys that match an
    // aliased projection item to that alias (so they survive grouping).
    let order_by = align_order_by_with_projection(&w.items, &w.order_by);
    let ordering = lower_order_by(ctx, &order_by, &mut inner_patterns)?;

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
    let alias = ctx.intern_var(&u.alias.name);

    // A constant inline list lowers to `Values` (one row per element). Any
    // other (runtime) expression — `UNWIND nodes(path) AS n`, `UNWIND range(..)`
    // — lowers to a `Pattern::Unwind` that the operator explodes per input row.
    match &u.expr {
        Expr::List(items, _) if items.iter().all(|i| matches!(i, Expr::Lit(_))) => {
            let mut rows: Vec<Vec<Binding>> = Vec::with_capacity(items.len());
            for item in items {
                rows.push(vec![literal_to_binding(item)?]);
            }
            Ok(Pattern::Values {
                vars: vec![alias],
                rows,
            })
        }
        Expr::Param(_) => Err(LowerError::unsupported(
            "UNWIND $param requires API-layer parameter substitution; submit pre-substituted UNWIND [a, b, c] AS x in v1",
        )),
        other => {
            let mut aux = Vec::new();
            let list = crate::lower::expr::lower_expr(ctx, other, &mut aux)?;
            // Any auxiliary patterns the list expression needs (e.g. property
            // accessors) must run before the UNWIND.
            aux.push(Pattern::Unwind { var: alias, list });
            // A single pattern is expected by the caller; wrap multiple in the
            // natural sequence via a Union-of-one is wrong, so return the last
            // and prepend aux through the caller. We only ever produce aux for
            // property accessors here, which is rare for an UNWIND source.
            if aux.len() == 1 {
                Ok(aux.pop().unwrap())
            } else {
                Err(LowerError::unsupported(
                    "UNWIND over an expression that needs auxiliary patterns is deferred",
                ))
            }
        }
    }
}

/// Lower a desugared `InlineRows` (constant multi-column row set, produced by
/// the `UNWIND $listOfMaps` → VALUES rewrite) to a `Pattern::Values`.
fn lower_inline_rows<E: IriEncoder>(
    ctx: &mut LoweringContext<'_, E>,
    vars: &[crate::ast::Variable],
    rows: &[Vec<Expr>],
) -> Result<Pattern> {
    let col_vars: Vec<VarId> = vars.iter().map(|v| ctx.intern_var(&v.name)).collect();
    let mut out_rows: Vec<Vec<Binding>> = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != col_vars.len() {
            return Err(LowerError::unsupported(
                "internal: InlineRows row width does not match its column count",
            ));
        }
        let mut cells = Vec::with_capacity(row.len());
        for cell in row {
            cells.push(literal_to_binding(cell)?);
        }
        out_rows.push(cells);
    }
    Ok(Pattern::Values {
        vars: col_vars,
        rows: out_rows,
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
