//! SELECT clause, solution modifiers, and subquery lowering.
//!
//! Handles lowering of SELECT variables, DISTINCT, LIMIT, OFFSET, ORDER BY,
//! GROUP BY, HAVING, and subquery patterns.

use crate::ast::expr::Expression as AstExpression;
use crate::ast::pattern::SubSelect;
use crate::ast::query::{
    GroupCondition, OrderCondition, OrderDirection, OrderExpr, SelectClause, SelectModifier,
    SelectVariable, SelectVariables, SolutionModifiers,
};
use crate::span::SourceSpan;

use fluree_db_query::ir::AggregateSpec;
use fluree_db_query::ir::{Expression, FlakeValue, Grouping, Pattern, SubqueryPattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::sort::{SortDirection, SortSpec};
use fluree_db_query::var_registry::VarId;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{LoweringContext, Result};

/// Result of lowering SELECT expression binds.
pub(super) struct SelectBinds {
    /// BIND patterns to apply before grouping/aggregation
    pub pre: Vec<Pattern>,
    /// Post-aggregation binds (var, expr) to apply after GROUP BY
    pub post: Vec<(VarId, Expression)>,
}

/// LIMIT / OFFSET / ORDER BY values produced by `lower_base_modifiers`.
/// Each lives on `Query` directly, so the lowering helper just hands them
/// back as a bundle for the caller to attach.
pub(super) struct BaseModifiers {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub ordering: Vec<SortSpec>,
    /// Synthetic `(var, expr)` binds produced by aggregate-free expression
    /// ORDER BY conditions (e.g. `ORDER BY DESC(?a / ?b)`), lowered eagerly.
    /// Each `SortSpec` in `ordering` that came from an expression references the
    /// matching synthetic var here (or in `deferred_order_exprs`).
    pub order_binds: Vec<(VarId, Expression)>,
    /// Expression ORDER BY conditions that contain an inline aggregate
    /// (e.g. `ORDER BY DESC(COUNT(?x))`). They cannot be lowered until aggregate
    /// hoisting has produced the alias map, so `lower_base_modifiers` stashes the
    /// synthetic sort var + raw AST expression here. `lower_solution_modifiers`
    /// (SELECT) hoists their aggregates and lowers them into `order_binds`;
    /// CONSTRUCT/DESCRIBE reject a non-empty list (no aggregation stage there).
    pub deferred_order_exprs: Vec<(VarId, AstExpression)>,
}

/// Result of lowering solution modifiers.
pub(super) struct LoweredModifiers {
    /// LIMIT, OFFSET, ORDER BY — lifted onto `Query` by the caller.
    pub base: BaseModifiers,
    /// Whether the SELECT carried `DISTINCT`. Lifted into the resulting
    /// [`QueryOutput::Select::restriction`] by the caller.
    pub distinct: bool,
    /// GROUP BY variables. Empty when the surface SELECT had no `GROUP BY`
    /// and no implied grouping was derived. Lifted into `Query.grouping`
    /// by the caller.
    pub group_by: Vec<VarId>,
    /// Aggregate specs computed per group (or once if `group_by` is empty
    /// and `aggregates` is non-empty — implicit single-group aggregation).
    pub aggregates: Vec<AggregateSpec>,
    /// HAVING expression (post-lift — aggregate calls have been hoisted into
    /// `aggregates` with synthetic output variables, and this references them).
    pub having: Option<Expression>,
    /// Pre-GROUP-BY BIND patterns for expression-based GROUP BY conditions.
    /// These must be injected into the WHERE pattern list before query building.
    pub pre_group_binds: Vec<Pattern>,
}

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower SELECT clause to a list of VarIds.
    pub(super) fn lower_select_clause(&mut self, clause: &SelectClause) -> Result<Vec<VarId>> {
        match &clause.variables {
            SelectVariables::Star => {
                // SELECT * — return user-visible registered variables.
                //
                // Hide three categories:
                // - `?__*` — planner / aggregate / property-path synthetics.
                // - `?#*`  — annotation-reifier synthetics
                //   (see `annotation::INTERNAL_VAR_PREFIX`).
                // - `_:*`  — SPARQL blank-node variables. Per SPARQL §4.1.4
                //   these are non-distinguished and not in SELECT scope, so
                //   they don't appear in `SELECT *` results. Hiding them
                //   here also covers blank-node-labelled reifiers
                //   (`~ _:ann`, `_:ann rdf:reifies …`).
                Ok(self
                    .vars
                    .iter()
                    .filter(|(name, _)| {
                        !name.starts_with("?__")
                            && !name.starts_with("?#")
                            && !name.starts_with("_:")
                    })
                    .map(|(_, id)| id)
                    .collect())
            }
            SelectVariables::Explicit(vars) => {
                let mut result = Vec::with_capacity(vars.len());
                for var in vars {
                    match var {
                        SelectVariable::Var(v) => {
                            result.push(self.register_var(v));
                        }
                        SelectVariable::Expr { alias, .. } => {
                            // For now, just register the alias variable
                            // The expression is handled via BIND in the pattern
                            result.push(self.register_var(alias));
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    pub(super) fn collect_aggregate_alias_names(&self, clause: &SelectClause) -> HashSet<Arc<str>> {
        let mut names = HashSet::new();
        if let SelectVariables::Explicit(vars) = &clause.variables {
            for var in vars {
                if let SelectVariable::Expr { expr, alias, .. } = var {
                    if matches!(expr, AstExpression::Aggregate { .. }) {
                        names.insert(alias.name.clone());
                    }
                }
            }
        }
        names
    }

    /// Lower non-aggregate SELECT expressions to BIND patterns (pre or post aggregation).
    pub(super) fn lower_select_expression_binds(
        &mut self,
        clause: &SelectClause,
        aggregate_aliases: &HashSet<Arc<str>>,
    ) -> Result<SelectBinds> {
        let mut pre_binds = Vec::new();
        let mut post_binds = Vec::new();

        if let SelectVariables::Explicit(vars) = &clause.variables {
            for var in vars {
                if let SelectVariable::Expr { expr, alias, .. } = var {
                    if matches!(expr, AstExpression::Aggregate { .. }) {
                        continue;
                    }
                    let filter_expr = self.lower_expression(expr)?;
                    let var_id = self.register_var(alias);
                    if self.expr_references_vars(expr, aggregate_aliases) {
                        post_binds.push((var_id, filter_expr));
                    } else {
                        pre_binds.push(Pattern::Bind {
                            var: var_id,
                            expr: filter_expr,
                        });
                    }
                }
            }
        }

        Ok(SelectBinds {
            pre: pre_binds,
            post: post_binds,
        })
    }

    /// Lower solution modifiers (DISTINCT, LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING)
    pub(super) fn lower_solution_modifiers(
        &mut self,
        modifiers: &SolutionModifiers,
        select: &SelectClause,
    ) -> Result<LoweredModifiers> {
        let distinct = select.modifier == Some(SelectModifier::Distinct);
        let mut group_by: Vec<VarId> = Vec::new();
        let mut having: Option<Expression> = None;
        let mut pre_group_binds = Vec::new();

        // LIMIT, OFFSET, ORDER BY. Aggregate-bearing ORDER BY expressions are
        // stashed in `base.deferred_order_exprs` and lowered below, after the
        // aggregate alias map exists.
        let mut base = self.lower_base_modifiers(modifiers)?;

        // GROUP BY — supports both variables and expressions.
        // Expression GROUP BY like `GROUP BY (expr AS ?alias)` desugars to
        // a pre-group BIND pattern + GROUP BY on the alias variable.
        if let Some(ref group_by_clause) = modifiers.group_by {
            let mut group_vars = Vec::with_capacity(group_by_clause.conditions.len());
            for cond in &group_by_clause.conditions {
                let (var_id, bind_pattern) = self.lower_group_condition(cond)?;
                group_vars.push(var_id);
                if let Some(pattern) = bind_pattern {
                    pre_group_binds.push(pattern);
                }
            }
            group_by = group_vars;
        }

        // Aggregate-alias map shared by HAVING and aggregate-bearing ORDER BY
        // hoisting. Seeded with SELECT aggregate aliases so both reuse them, and
        // so the synthetic `?__inline_agg_N` names stay unique across both (the
        // names are keyed off the shared map's length).
        let needs_alias_map = modifiers.having.is_some() || !base.deferred_order_exprs.is_empty();
        let mut aggregate_aliases: HashMap<String, VarId> = if needs_alias_map {
            self.build_aggregate_aliases(select)?
        } else {
            HashMap::new()
        };
        // Aggregates hoisted out of HAVING and/or ORDER BY expressions.
        let mut hoisted_aggregates: Vec<AggregateSpec> = Vec::new();

        // HAVING (may reference aggregate expressions)
        if let Some(ref having_clause) = modifiers.having {
            let mut having_pre_binds: Vec<Pattern> = Vec::new();
            for cond in &having_clause.conditions {
                self.collect_inline_aggregates(
                    cond,
                    &mut aggregate_aliases,
                    &mut hoisted_aggregates,
                    &mut having_pre_binds,
                )?;
            }
            self.aggregate_aliases = Some(aggregate_aliases.clone());
            // Combine all HAVING conditions with AND
            let filter = self.lower_having_conditions(&having_clause.conditions)?;
            having = Some(filter);
            self.aggregate_aliases = None;
            pre_group_binds.extend(having_pre_binds);
        }

        // Deferred ORDER BY expressions containing inline aggregates
        // (e.g. `ORDER BY DESC(COUNT(?x))`). Hoist their aggregates into the same
        // map, then lower the expression with the alias map in scope so the
        // inline aggregate resolves to its synthetic output var. The resulting
        // order bind is applied by the operator tree's post-grouping stage.
        if !base.deferred_order_exprs.is_empty() {
            let deferred = std::mem::take(&mut base.deferred_order_exprs);
            let mut order_pre_binds: Vec<Pattern> = Vec::new();
            for (_, ast_expr) in &deferred {
                self.collect_inline_aggregates(
                    ast_expr,
                    &mut aggregate_aliases,
                    &mut hoisted_aggregates,
                    &mut order_pre_binds,
                )?;
            }
            self.aggregate_aliases = Some(aggregate_aliases.clone());
            for (var_id, ast_expr) in &deferred {
                let lowered = self.lower_expression(ast_expr)?;
                base.order_binds.push((*var_id, lowered));
            }
            self.aggregate_aliases = None;
            pre_group_binds.extend(order_pre_binds);
        }

        // Extract aggregates from SELECT clause, then append any aggregates
        // lifted out of HAVING / ORDER BY.
        let (mut aggregates, select_agg_binds) = self.extract_aggregates(select)?;
        pre_group_binds.extend(select_agg_binds);
        aggregates.extend(hoisted_aggregates);

        // Auto-populate GROUP BY when aggregates present but no explicit GROUP BY
        // Per SPARQL semantics, all non-aggregated SELECT variables must be in GROUP BY
        if !aggregates.is_empty() && group_by.is_empty() {
            group_by = self.collect_non_aggregate_select_vars(select);
        }

        Ok(LoweredModifiers {
            base,
            distinct,
            group_by,
            aggregates,
            having,
            pre_group_binds,
        })
    }

    /// Lower LIMIT, OFFSET, and ORDER BY modifiers (shared by SELECT and
    /// CONSTRUCT). Each rides on `Query` directly; the caller attaches them.
    pub(super) fn lower_base_modifiers(
        &mut self,
        modifiers: &SolutionModifiers,
    ) -> Result<BaseModifiers> {
        let limit = modifiers.limit.as_ref().map(|clause| clause.value as usize);
        let offset = modifiers
            .offset
            .as_ref()
            .map(|clause| clause.value as usize);
        let mut order_binds: Vec<(VarId, Expression)> = Vec::new();
        let mut deferred_order_exprs: Vec<(VarId, AstExpression)> = Vec::new();
        let ordering = match &modifiers.order_by {
            Some(order_by) => order_by
                .conditions
                .iter()
                .map(|cond| {
                    self.lower_order_condition(cond, &mut order_binds, &mut deferred_order_exprs)
                })
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };

        Ok(BaseModifiers {
            limit,
            offset,
            ordering,
            order_binds,
            deferred_order_exprs,
        })
    }

    /// Lower an ORDER BY condition to a [`SortSpec`].
    ///
    /// Bare variables (including `ASC(?var)` / `DESC((?var))`) sort directly on
    /// that variable. A non-trivial expression (`ORDER BY DESC(?a / ?b)`) is
    /// desugared to a synthetic `BIND(expr AS ?__order_by_N)`: the expression is
    /// evaluated once per solution into the synthetic var, which becomes the
    /// sort key (sorting an expression inside the comparator would re-evaluate it
    /// O(n log n) times).
    ///
    /// An expression that contains an inline aggregate (`ORDER BY DESC(COUNT(?x))`)
    /// cannot be lowered yet — the aggregate alias map does not exist until
    /// hoisting runs — so it is stashed in `deferred_order_exprs` and lowered
    /// later by [`Self::lower_solution_modifiers`].
    fn lower_order_condition(
        &mut self,
        cond: &OrderCondition,
        order_binds: &mut Vec<(VarId, Expression)>,
        deferred_order_exprs: &mut Vec<(VarId, AstExpression)>,
    ) -> Result<SortSpec> {
        let direction = match cond.direction {
            OrderDirection::Asc => SortDirection::Ascending,
            OrderDirection::Desc => SortDirection::Descending,
        };

        match &cond.expr {
            OrderExpr::Var(var) => {
                let var_id = self.register_var(var);
                Ok(SortSpec {
                    var: var_id,
                    direction,
                })
            }
            // Handle ASC(?var) / DESC(?var) / ASC((?var)) which parses as Expr
            // Unwrap any bracketed expressions first
            OrderExpr::Expr(expr) => match expr.unwrap_bracketed() {
                AstExpression::Var(var) => {
                    let var_id = self.register_var(var);
                    Ok(SortSpec {
                        var: var_id,
                        direction,
                    })
                }
                _ => {
                    // Expression-based ORDER BY: sort on a synthetic var bound to
                    // the expression.
                    let name = format!("?__order_by_{}", self.order_counter);
                    self.order_counter += 1;
                    let var_id = self.vars.get_or_insert(&name);
                    if self.expr_contains_aggregate(expr) {
                        // Defer: needs aggregate hoisting before it can be lowered.
                        deferred_order_exprs.push((var_id, expr.clone()));
                    } else {
                        let lowered = self.lower_expression(expr)?;
                        order_binds.push((var_id, lowered));
                    }
                    Ok(SortSpec {
                        var: var_id,
                        direction,
                    })
                }
            },
        }
    }

    /// Lower a GROUP BY condition to a variable ID and optional pre-GROUP-BY BIND.
    ///
    /// Returns `(var_id, Option<Pattern::Bind>)`:
    /// - `GROUP BY ?x`              → variable reference, no BIND needed
    /// - `GROUP BY (?x)`            → parenthesized variable, unwrapped to plain variable
    /// - `GROUP BY (expr AS ?alias)` → desugared to BIND(expr AS ?alias) + GROUP BY ?alias
    /// - `GROUP BY (expr)`          → same, but with a synthetic `?__group_expr_N` alias
    fn lower_group_condition(&mut self, cond: &GroupCondition) -> Result<(VarId, Option<Pattern>)> {
        match cond {
            GroupCondition::Var(var) => Ok((self.register_var(var), None)),
            GroupCondition::Expr { expr, alias, .. } => {
                // Try unwrapping brackets to see if it's just a variable
                match expr.unwrap_bracketed() {
                    AstExpression::Var(var) => Ok((self.register_var(var), None)),
                    _ => {
                        // Expression-based GROUP BY: desugar to BIND + GROUP BY alias
                        let lowered = self.lower_expression(expr)?;
                        let var_id = if let Some(alias_var) = alias {
                            self.register_var(alias_var)
                        } else {
                            // No alias — generate a synthetic variable
                            let name = format!("?__group_expr_{}", self.vars.len());
                            self.vars.get_or_insert(&name)
                        };
                        Ok((
                            var_id,
                            Some(Pattern::Bind {
                                var: var_id,
                                expr: lowered,
                            }),
                        ))
                    }
                }
            }
        }
    }

    /// Lower HAVING conditions to a single Expression (ANDed together)
    fn lower_having_conditions(&mut self, conditions: &[AstExpression]) -> Result<Expression> {
        if conditions.is_empty() {
            // Should not happen - HAVING requires at least one condition
            return Ok(Expression::Const(FlakeValue::Boolean(true)));
        }

        let mut exprs: Vec<Expression> = Vec::with_capacity(conditions.len());
        for cond in conditions {
            exprs.push(self.lower_expression(cond)?);
        }

        // Combine with AND if multiple conditions
        if exprs.len() == 1 {
            Ok(exprs.pop().unwrap())
        } else {
            Ok(Expression::and(exprs))
        }
    }

    /// Lower a SPARQL subquery (SubSelect) to the IR.
    ///
    /// Subqueries have the form: `{ SELECT ?vars WHERE { ... } GROUP BY ?v
    /// HAVING (..) ORDER BY (..) LIMIT n }`. This mirrors the top-level SELECT
    /// lowering — SELECT-expression binds, GROUP BY / aggregates / HAVING, and
    /// expression/aggregate ORDER BY all go through the same shared helpers
    /// (`lower_select_expression_binds`, `lower_solution_modifiers`) — so a
    /// subquery inherits exactly the same modifier semantics as a top-level
    /// query. The resulting `SubqueryPattern` is executed per correlated parent
    /// row by `SubqueryOperator`, which applies the shared solution-modifier
    /// tail (`apply_solution_modifiers`).
    pub(super) fn lower_subselect(
        &mut self,
        subselect: &SubSelect,
        _span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        // Aggregate-input-expression CSE (`agg_expr_binds`) is scoped to a single
        // WHERE clause. A subquery is its own execution scope, so the synthetic
        // `?__agg_expr_N` bind for an aggregate over an expression (e.g.
        // `AVG(xsd:float(?n))`) must live in THIS subquery's patterns. Without
        // resetting the cache, two sibling subqueries sharing the same aggregate
        // input expression would dedup to one synthetic var that is bound only in
        // the first subquery's scope, leaving the second's aggregate input
        // unbound (benchmark-db bug #4). Save here, restore before returning.
        let saved_agg_expr_binds = std::mem::take(&mut self.agg_expr_binds);

        // Lower WHERE patterns (mut: SELECT-expression / GROUP BY / aggregate-
        // input BINDs are appended below, just as in the top-level pipeline).
        let mut patterns = self.lower_graph_pattern(&subselect.pattern)?;

        // Build a SelectClause so the shared SELECT/modifier lowering applies.
        // REDUCED is treated as DISTINCT (handled when assembling the pattern).
        let select_clause = SelectClause {
            modifier: if subselect.distinct {
                Some(SelectModifier::Distinct)
            } else if subselect.reduced {
                Some(SelectModifier::Reduced)
            } else {
                None
            },
            variables: subselect.variables.clone(),
            span: subselect.span,
        };

        // Projected variable list.
        //
        // IMPORTANT: In the query engine, an empty select list does NOT mean
        // "SELECT *" — it means "select no variables". For SPARQL `SELECT *` we
        // approximate the spec by selecting all variables produced by the
        // (just-lowered) WHERE patterns, in stable encounter order.
        let select: Vec<VarId> = match &subselect.variables {
            SelectVariables::Star => {
                let mut seen: HashSet<VarId> = HashSet::new();
                let mut select: Vec<VarId> = Vec::new();
                for p in &patterns {
                    for v in p.produced_vars() {
                        if seen.insert(v) {
                            select.push(v);
                        }
                    }
                }
                select
            }
            SelectVariables::Explicit(vars) => {
                let mut result = Vec::with_capacity(vars.len());
                for var in vars {
                    match var {
                        SelectVariable::Var(v) => result.push(self.register_var(v)),
                        SelectVariable::Expr { alias, .. } => result.push(self.register_var(alias)),
                    }
                }
                result
            }
        };

        // SELECT-expression binds: pre-aggregation ones append to WHERE; post-
        // aggregation ones (referencing an aggregate alias) ride in the grouping.
        let aggregate_aliases = self.collect_aggregate_alias_names(&select_clause);
        let select_binds =
            self.lower_select_expression_binds(&select_clause, &aggregate_aliases)?;
        patterns.extend(select_binds.pre);

        // Solution modifiers (GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET /
        // aggregates) through the same path as a top-level SELECT. This lowers
        // HAVING, hoists inline aggregates from HAVING / ORDER BY, and produces
        // expression-ORDER-BY binds — all previously dropped or rejected here.
        let lowered = self.lower_solution_modifiers(&subselect.modifiers, &select_clause)?;
        patterns.extend(lowered.pre_group_binds);
        let BaseModifiers {
            limit,
            offset,
            ordering,
            order_binds,
            // Consumed by `lower_solution_modifiers` (lowered into `order_binds`
            // after aggregate hoisting); always empty here.
            deferred_order_exprs: _,
        } = lowered.base;

        // Assemble the SubqueryPattern. Post-aggregation SELECT binds ride inside
        // the grouping's aggregation stage; expression/aggregate ORDER BY binds
        // ride on `order_binds` (a dedicated post-grouping stage in the shared
        // modifier tail) so they evaluate uniformly with or without grouping.
        let mut sq = SubqueryPattern::new(select, patterns);
        if let Some(grouping) = Grouping::assemble(
            lowered.group_by,
            lowered.aggregates,
            select_binds.post,
            lowered.having,
        ) {
            sq = sq.with_grouping(grouping);
        }
        sq = sq.with_order_binds(order_binds);
        if !ordering.is_empty() {
            sq = sq.with_ordering(ordering);
        }
        if let Some(limit) = limit {
            sq = sq.with_limit(limit);
        }
        if let Some(offset) = offset {
            sq = sq.with_offset(offset);
        }
        // DISTINCT (REDUCED is treated as DISTINCT).
        if lowered.distinct || subselect.reduced {
            sq = sq.with_distinct();
        }

        // Restore the enclosing scope's aggregate-expression CSE cache (an early
        // `?` error abandons the whole lowering, so success-path restore is enough).
        self.agg_expr_binds = saved_agg_expr_binds;

        Ok(vec![Pattern::Subquery(sq)])
    }
}
