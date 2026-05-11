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

use std::collections::HashSet;
use std::sync::Arc;

use super::{LowerError, LoweringContext, Result};

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
                // SELECT * - return all variables in the WHERE clause
                // For now, return what we have registered
                Ok(self.vars.iter().map(|(_, id)| id).collect())
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

        // LIMIT, OFFSET, ORDER BY
        let base = self.lower_base_modifiers(modifiers)?;

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

        // HAVING (may reference aggregate expressions)
        let mut having_aggregates: Vec<AggregateSpec> = Vec::new();
        if let Some(ref having_clause) = modifiers.having {
            let mut aggregate_aliases = self.build_aggregate_aliases(select)?;
            let mut having_pre_binds: Vec<Pattern> = Vec::new();
            for cond in &having_clause.conditions {
                self.collect_having_aggregates(
                    cond,
                    &mut aggregate_aliases,
                    &mut having_aggregates,
                    &mut having_pre_binds,
                )?;
            }
            self.aggregate_aliases = Some(aggregate_aliases);
            // Combine all HAVING conditions with AND
            let filter = self.lower_having_conditions(&having_clause.conditions)?;
            having = Some(filter);
            self.aggregate_aliases = None;
            pre_group_binds.extend(having_pre_binds);
        }

        // Extract aggregates from SELECT clause, then append any aggregates
        // lifted out of HAVING.
        let (mut aggregates, select_agg_binds) = self.extract_aggregates(select)?;
        pre_group_binds.extend(select_agg_binds);
        aggregates.extend(having_aggregates);

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
        let ordering = match &modifiers.order_by {
            Some(order_by) => order_by
                .conditions
                .iter()
                .map(|cond| self.lower_order_condition(cond))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };

        Ok(BaseModifiers {
            limit,
            offset,
            ordering,
        })
    }

    /// Lower an ORDER BY condition (vars-only MVP)
    fn lower_order_condition(&mut self, cond: &OrderCondition) -> Result<SortSpec> {
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
                    // Complex expression-based ORDER BY not yet supported
                    Err(LowerError::unsupported_order_by_expr(cond.span))
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
    /// Subqueries have the form: `{ SELECT ?vars WHERE { ... } GROUP BY ?v LIMIT n }`
    /// Supports aggregate expressions like `(COUNT(?x) AS ?count)` in the SELECT clause.
    pub(super) fn lower_subselect(
        &mut self,
        subselect: &SubSelect,
        _span: SourceSpan,
    ) -> Result<Vec<Pattern>> {
        // Lower WHERE patterns (mut: expression GROUP BY may append pre-group BINDs)
        let mut patterns = self.lower_graph_pattern(&subselect.pattern)?;

        // Build a temporary SelectClause so we can reuse extract_aggregates /
        // collect_non_aggregate_select_vars which operate on SelectClause.
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

        // Lower SELECT variables
        //
        // IMPORTANT: In the query engine, an empty select list does NOT mean "SELECT *".
        // It means "select no variables", which yields no output schema and therefore no rows.
        //
        // For SPARQL `SELECT *`, we approximate the spec by selecting all variables referenced
        // by the subquery's WHERE patterns (in stable encounter order).
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
                        SelectVariable::Var(v) => {
                            result.push(self.register_var(v));
                        }
                        SelectVariable::Expr { alias, .. } => {
                            result.push(self.register_var(alias));
                        }
                    }
                }
                result
            }
        };

        // Extract aggregates from SELECT clause (e.g. COUNT(?x) AS ?count)
        let (aggregates, agg_binds) = self.extract_aggregates(&select_clause)?;

        // Lower GROUP BY (expression GROUP BY produces pre-group BINDs)
        let mut group_vars = Vec::new();
        if let Some(ref group_by) = subselect.group_by {
            for cond in &group_by.conditions {
                let (var_id, bind_pattern) = self.lower_group_condition(cond)?;
                group_vars.push(var_id);
                if let Some(pattern) = bind_pattern {
                    patterns.push(pattern);
                }
            }
        }

        // Aggregate expression inputs (e.g. SUM(YEAR(?o))) are desugared to
        // pre-aggregation BIND patterns + aggregate over the synthetic var.
        patterns.extend(agg_binds);

        // Build SubqueryPattern (after injecting any pre-group BINDs into patterns)
        let mut sq = SubqueryPattern::new(select, patterns);

        // Auto-populate GROUP BY when aggregates present but no explicit GROUP BY.
        // Per SPARQL semantics, all non-aggregated SELECT variables must be grouped.
        if !aggregates.is_empty() && group_vars.is_empty() {
            group_vars = self.collect_non_aggregate_select_vars(&select_clause);
        }

        // Lift GROUP BY / aggregates into the SubqueryPattern's grouping
        // phase. Subselect HAVING isn't lowered here (its surface syntax is
        // captured upstream and would require its own lowering); same for
        // post-aggregation binds.
        if let Some(grouping) = Grouping::assemble(group_vars, aggregates, Vec::new(), None) {
            sq = sq.with_grouping(grouping);
        }

        // Apply LIMIT
        if let Some(limit) = subselect.limit {
            sq = sq.with_limit(limit as usize);
        }

        // Apply OFFSET
        if let Some(offset) = subselect.offset {
            sq = sq.with_offset(offset as usize);
        }

        // Apply DISTINCT
        if subselect.distinct {
            sq = sq.with_distinct();
        }

        // Note: REDUCED is treated as DISTINCT for simplicity
        if subselect.reduced {
            sq = sq.with_distinct();
        }

        // Apply ORDER BY
        if !subselect.order_by.is_empty() {
            let mut sort_specs = Vec::with_capacity(subselect.order_by.len());
            for order in &subselect.order_by {
                let var_id = self.register_var(&order.var);
                let direction = if order.descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                };
                sort_specs.push(SortSpec {
                    var: var_id,
                    direction,
                });
            }
            sq = sq.with_ordering(sort_specs);
        }

        Ok(vec![Pattern::Subquery(sq)])
    }
}
