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

use fluree_db_query::ir::pattern::produced_vars_of;
use fluree_db_query::ir::AggregateSpec;
use fluree_db_query::ir::{
    having_as_filter, read_as_unbound, sample_ungrouped_reads, Expression, FlakeValue, Grouping,
    Pattern, SelectExprPlacement, SelectExprPlacer, SubqueryPattern,
};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::sort::{SortDirection, SortSpec};
use fluree_db_query::var_registry::VarId;

use std::collections::{HashMap, HashSet};

use super::{LowerError, LoweringContext, Result};

/// The variables a query level binds before grouping: its WHERE, its trailing
/// VALUES (joined there, see [`LoweringContext::lower_select_level`]) and the
/// binds its GROUP BY keys and aggregate inputs lowered to.
///
/// Collected only by a stage that reads it — a HAVING or ORDER BY to sample, a
/// SELECT expression to place — so the common level with none of those
/// (`SELECT (COUNT(?s) AS ?n) WHERE { … }`) never collects it.
fn pre_group_vars(
    where_patterns: &[Pattern],
    trailing_values: Option<&Pattern>,
    pre_group_binds: &[(VarId, Expression)],
) -> HashSet<VarId> {
    let mut vars = produced_vars_of(where_patterns);
    vars.extend(trailing_values.into_iter().flat_map(Pattern::produced_vars));
    vars.extend(pre_group_binds.iter().map(|(var, _)| *var));
    vars
}

/// The SELECT expressions of one query level, placed.
pub(super) struct SelectExtends {
    /// Binds before grouping, after the level's WHERE and trailing VALUES:
    /// every SELECT expression of a level that does not group, and a grouping
    /// level's expression whose alias is itself a group key.
    pub pre: Vec<(VarId, Expression)>,
    /// Per-group `Extend`s (SPARQL 1.1 §18.2.4.4), in SELECT order: every
    /// other SELECT expression of a grouping level, compound aggregate items
    /// included. The ones HAVING reads run before it, the rest after it
    /// (`Grouping::binds_before_having`).
    pub extends: Vec<(VarId, Expression)>,
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
    /// Binds before grouping: expression GROUP BY conditions and aggregate
    /// inputs, including those of aggregates hoisted from SELECT, HAVING and
    /// ORDER BY expressions.
    pub pre_group_binds: Vec<(VarId, Expression)>,
    /// Compound-aggregate SELECT items (e.g. `((MAX(?u) - MIN(?u)) AS
    /// ?spread)`), keyed by alias. Each inner aggregate has been hoisted into
    /// `aggregates`, and the expression reads those synthetic output vars.
    /// [`LoweringContext::lower_select_extends`] places them in SELECT order.
    pub compound_select_exprs: HashMap<VarId, Expression>,
}

impl LoweredModifiers {
    /// Whether the level groups (SPARQL 1.1 §18.2.4.1): a GROUP BY, or an
    /// aggregate anywhere in SELECT, HAVING or ORDER BY. Every aggregate,
    /// wherever it was written, has been hoisted into `aggregates` by now.
    pub fn groups(&self) -> bool {
        !self.group_by.is_empty() || !self.aggregates.is_empty()
    }
}

/// One SELECT level (top level or sub-SELECT), lowered past its WHERE: its
/// solution modifiers, grouping phase and SELECT-expression placement. A
/// HAVING-as-Filter is already on its patterns; the binds it generated are in
/// `binds`, for the caller to place after the level's trailing VALUES.
pub(super) struct LoweredSelectLevel {
    /// LIMIT, OFFSET, ORDER BY — lifted onto the query / sub-query.
    pub base: BaseModifiers,
    /// Whether the SELECT carried `DISTINCT`.
    pub distinct: bool,
    /// The grouping phase, when the level groups.
    pub grouping: Option<Grouping>,
    /// `SELECT *` of a grouping level: its user-visible GROUP BY keys (none
    /// under implicit grouping). `None` when the level has no `*` or does
    /// not group.
    pub star_projection: Option<Vec<VarId>>,
    /// The binds the level generated, evaluated in order after its WHERE and
    /// its trailing VALUES, before grouping: SELECT expressions placed before
    /// grouping, then expression GROUP BY conditions and aggregate inputs.
    pub binds: Vec<(VarId, Expression)>,
}

impl LoweredSelectLevel {
    /// The level's generated binds as WHERE patterns.
    pub fn bind_patterns(binds: Vec<(VarId, Expression)>) -> impl Iterator<Item = Pattern> {
        binds
            .into_iter()
            .map(|(var, expr)| Pattern::Bind { var, expr })
    }
}

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// The registered variables a user can see, in registration order.
    ///
    /// This is what `*` denotes — both in `SELECT *` and in
    /// `COUNT(DISTINCT *)`, which counts distinct solution *mappings* and so
    /// must range over the same variables. Three categories are hidden:
    /// - `?__*` — planner / aggregate / property-path synthetics.
    /// - `?#*`  — annotation-reifier synthetics
    ///   (see `annotation::INTERNAL_VAR_PREFIX`).
    /// - `_:*`  — SPARQL blank-node variables. Per SPARQL §4.1.4 these are
    ///   non-distinguished and not in SELECT scope, so they don't appear in
    ///   `SELECT *` results. Hiding them here also covers blank-node-labelled
    ///   reifiers (`~ _:ann`, `_:ann rdf:reifies …`).
    ///
    /// The registry spans the whole query, so this can name variables from
    /// sibling or nested scopes; consumers intersect it with the scope they
    /// actually operate on.
    pub(super) fn user_visible_vars(&self) -> Vec<VarId> {
        self.vars
            .iter()
            .filter(|(name, _)| !fluree_db_query::var_registry::is_internal_var_name(name))
            .map(|(_, id)| id)
            .collect()
    }

    /// Lower SELECT clause to a list of VarIds.
    pub(super) fn lower_select_clause(&mut self, clause: &SelectClause) -> Result<Vec<VarId>> {
        match &clause.variables {
            SelectVariables::Star => Ok(self.user_visible_vars()),
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

    /// Lower one SELECT level past its WHERE, in the order SPARQL 1.1
    /// §18.2.4 gives the query level: grouping and aggregates, HAVING, then
    /// the SELECT expressions. Shared by the top level and sub-SELECTs.
    ///
    /// `patterns` are the level's WHERE patterns; a HAVING-as-Filter is
    /// appended to them. `trailing_values` is the level's trailing VALUES
    /// clause, which the caller joins right after the WHERE, before grouping,
    /// where it restricts the aggregates' input (a deliberate deviation from
    /// §18.2.4.3, which joins it after HAVING), and then places the returned
    /// `binds`. Its variables are therefore bound before grouping, except to
    /// HAVING: HAVING reads the ones the WHERE does not bind as unbound, as it
    /// would after HAVING.
    pub(super) fn lower_select_level(
        &mut self,
        select: &SelectClause,
        modifiers: &SolutionModifiers,
        patterns: &mut Vec<Pattern>,
        trailing_values: Option<&Pattern>,
    ) -> Result<LoweredSelectLevel> {
        let mut lowered =
            self.lower_solution_modifiers(modifiers, select, patterns, trailing_values)?;
        // One definition of "groups": validation (V4) reads it off the AST,
        // lowering off the lowered keys and aggregates.
        debug_assert_eq!(
            lowered.groups(),
            modifiers.level_groups(&select.variables),
            "lowering and validation disagree on whether the level groups"
        );
        let extends = self.lower_select_extends(select, &mut lowered, patterns, trailing_values)?;

        // HAVING on a level that does not group: a Filter over its solutions
        // (§18.2.4.2), which cannot see the SELECT expressions.
        if !lowered.groups() {
            if let Some(having) = lowered.having.take() {
                let aliases: HashSet<VarId> = match &select.variables {
                    SelectVariables::Explicit(items) => items
                        .iter()
                        .filter_map(|item| match item {
                            SelectVariable::Expr { alias, .. } => Some(self.register_var(alias)),
                            SelectVariable::Var(_) => None,
                        })
                        .collect(),
                    SelectVariables::Star => HashSet::new(),
                };
                let vars = &mut self.vars;
                patterns.push(having_as_filter(having, &aliases, &mut |_| {
                    vars.get_or_insert(&format!("?__having_unbound_{}", vars.len()))
                }));
            }
        }
        let mut binds = extends.pre;
        binds.append(&mut lowered.pre_group_binds);

        let star_projection = (matches!(select.variables, SelectVariables::Star)
            && lowered.groups())
        .then(|| self.grouped_star_projection(&lowered.group_by));
        let grouping = Grouping::assemble(
            lowered.group_by,
            lowered.aggregates,
            extends.extends,
            lowered.having,
        )
        .map_err(|e| LowerError::InvalidGrouping {
            message: e.to_string(),
            span: select.span,
        })?;
        Ok(LoweredSelectLevel {
            base: lowered.base,
            distinct: lowered.distinct,
            grouping,
            star_projection,
            binds,
        })
    }

    /// `SELECT *` of a grouping level: the user-visible GROUP BY keys
    /// (§18.2.4.4 restricts the projection to them). Implicit grouping has
    /// none, so it projects nothing. (An explicit GROUP BY with `SELECT *` is
    /// a validation error; this is what an unvalidated entry point gets.)
    pub(super) fn grouped_star_projection(&self, group_by: &[VarId]) -> Vec<VarId> {
        let visible: HashSet<VarId> = self.user_visible_vars().into_iter().collect();
        group_by
            .iter()
            .copied()
            .filter(|v| visible.contains(v))
            .collect()
    }

    /// Place the SELECT expressions of one query level, in SELECT order
    /// (SPARQL 1.1 §18.2.4.4), after its solution modifiers are lowered —
    /// whether the level groups is read from the lowered keys and aggregates,
    /// not re-derived from the AST.
    ///
    /// A bare aggregate (`(COUNT(?x) AS ?n)`) needs nothing here: its
    /// `AggregateSpec` binds the alias. A compound aggregate item was lowered
    /// by [`Self::lower_solution_modifiers`] and always runs per group. Every
    /// other expression goes where [`SelectExprPlacer`] puts it: in a
    /// grouping level, per group after HAVING, except an expression whose
    /// alias is a group key (the `GROUP BY (LCASE(?a))` shortcut), which has
    /// to exist before grouping.
    ///
    /// `where_patterns` and `trailing_values` are the level's WHERE and
    /// trailing VALUES (see [`Self::lower_select_level`]).
    pub(super) fn lower_select_extends(
        &mut self,
        select: &SelectClause,
        lowered: &mut LoweredModifiers,
        where_patterns: &[Pattern],
        trailing_values: Option<&Pattern>,
    ) -> Result<SelectExtends> {
        let mut pre = Vec::new();
        let mut extends = Vec::new();
        let SelectVariables::Explicit(items) = &select.variables else {
            return Ok(SelectExtends { pre, extends });
        };
        // (alias, expression, contains an aggregate), in SELECT order.
        let mut computed: Vec<(VarId, Expression, bool)> = Vec::new();
        for item in items {
            let SelectVariable::Expr { expr, alias, .. } = item else {
                continue;
            };
            if matches!(expr, AstExpression::Aggregate { .. }) {
                continue;
            }
            let var = self.register_var(alias);
            if let Some(compound) = lowered.compound_select_exprs.remove(&var) {
                computed.push((var, compound, true));
                continue;
            }
            computed.push((var, self.lower_expression(expr)?, false));
        }
        if computed.is_empty() {
            return Ok(SelectExtends { pre, extends });
        }
        let placer = if lowered.groups() {
            SelectExprPlacer::grouped(
                lowered.group_by.iter().copied(),
                &lowered.aggregates,
                pre_group_vars(where_patterns, trailing_values, &lowered.pre_group_binds),
            )
        } else {
            SelectExprPlacer::ungrouped()
        };
        let placements = placer.place_all(
            &computed
                .iter()
                .map(|(var, expr, aggregate)| (*var, expr, *aggregate))
                .collect::<Vec<_>>(),
        );
        for ((var, expr, _), placement) in computed.into_iter().zip(placements) {
            match placement {
                SelectExprPlacement::PostGroup => extends.push((var, expr)),
                SelectExprPlacement::PreGroup => pre.push((var, expr)),
            }
        }
        Ok(SelectExtends { pre, extends })
    }

    /// Lower solution modifiers (DISTINCT, LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING)
    ///
    /// `where_patterns` and `trailing_values` are the level's WHERE and
    /// trailing VALUES, which joins before grouping (see
    /// [`Self::lower_select_level`]).
    pub(super) fn lower_solution_modifiers(
        &mut self,
        modifiers: &SolutionModifiers,
        select: &SelectClause,
        where_patterns: &[Pattern],
        trailing_values: Option<&Pattern>,
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
            // Map of structural key → alias var for non-aggregate SELECT
            // expressions, so an unaliased `GROUP BY (expr)` whose expression is
            // also projected as `(expr AS ?k)` can group on ?k directly rather
            // than on a fresh synthetic var (SPARQL 1.1 §11.2 — a projected
            // grouping expression yields the group value).
            let select_expr_aliases = self.select_expr_alias_map(select);
            let mut group_vars = Vec::with_capacity(group_by_clause.conditions.len());
            for cond in &group_by_clause.conditions {
                let (var_id, bind) = self.lower_group_condition(cond, &select_expr_aliases)?;
                group_vars.push(var_id);
                if let Some(expr) = bind {
                    pre_group_binds.push((var_id, expr));
                }
            }
            group_by = group_vars;
        }

        // Compound-aggregate SELECT items (`(MAX(?u) - MIN(?u) AS ?spread)`):
        // these need the alias map too, so their inner aggregates can be
        // hoisted and the outer expression lowered as a post-aggregation bind.
        let compound_aggregate_select_items: Vec<(VarId, AstExpression)> =
            self.collect_compound_aggregate_select_items(select);

        // Aggregate-alias map shared by HAVING, aggregate-bearing ORDER BY, and
        // compound-aggregate SELECT items. Seeded with bare-Aggregate SELECT
        // aliases so all three reuse them, and so the synthetic
        // `?__inline_agg_N` names stay unique across them (keyed off the shared
        // map's length).
        let needs_alias_map = modifiers.having.is_some()
            || !base.deferred_order_exprs.is_empty()
            || !compound_aggregate_select_items.is_empty();
        let mut aggregate_aliases: HashMap<String, VarId> = if needs_alias_map {
            self.build_aggregate_aliases(select)?
        } else {
            HashMap::new()
        };
        // Aggregates hoisted out of compound SELECT items, HAVING, and/or
        // ORDER BY expressions.
        let mut hoisted_aggregates: Vec<AggregateSpec> = Vec::new();

        // Compound-aggregate SELECT items: hoist their inner aggregates, then
        // lower each outer expression against the synthetic alias vars. They
        // are placed, in SELECT order, by `lower_select_extends`.
        let mut compound_select_exprs: HashMap<VarId, Expression> = HashMap::new();
        if !compound_aggregate_select_items.is_empty() {
            let mut select_pre_binds = Vec::new();
            for (_, ast_expr) in &compound_aggregate_select_items {
                self.collect_inline_aggregates(
                    ast_expr,
                    &mut aggregate_aliases,
                    &mut hoisted_aggregates,
                    &mut select_pre_binds,
                )?;
            }
            self.aggregate_aliases = Some(aggregate_aliases.clone());
            for (var_id, ast_expr) in &compound_aggregate_select_items {
                let lowered = self.lower_expression(ast_expr)?;
                compound_select_exprs.insert(*var_id, lowered);
            }
            self.aggregate_aliases = None;
            pre_group_binds.extend(select_pre_binds);
        }

        // HAVING (may reference aggregate expressions)
        if let Some(ref having_clause) = modifiers.having {
            let mut having_pre_binds = Vec::new();
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
            let mut order_pre_binds = Vec::new();
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

        // In the spec a trailing VALUES clause joins after HAVING (§18.2.4), so
        // HAVING reads its variables as unbound unless the level binds them
        // itself: in the WHERE (the caller left those out), as a key or as an
        // aggregate. This runs before the SAMPLE rewrite below, which they
        // would otherwise reach: Fluree joins VALUES before grouping.
        if let (Some(having), Some(values)) = (having.as_mut(), trailing_values) {
            let where_bound = produced_vars_of(where_patterns);
            let unbound: HashSet<VarId> = values
                .produced_vars()
                .into_iter()
                .filter(|v| {
                    !where_bound.contains(v)
                        && !group_by.contains(v)
                        && !aggregates.iter().any(|spec| spec.output_var == *v)
                })
                .collect();
            let vars = &mut self.vars;
            read_as_unbound(having, &unbound, &mut |_| {
                vars.get_or_insert(&format!("?__having_unbound_{}", vars.len()))
            });
        }

        // In a grouping level, a HAVING / ORDER BY read of a non-key variable
        // means SAMPLE(?v) (§18.2.4.1). The level's variables bound before
        // grouping include the GROUP BY / aggregate-input binds lowered above.
        if !group_by.is_empty() || !aggregates.is_empty() {
            let vars = &mut self.vars;
            sample_ungrouped_reads(
                &group_by,
                &mut aggregates,
                having.as_mut(),
                &mut base.order_binds,
                &mut base.ordering,
                || pre_group_vars(where_patterns, trailing_values, &pre_group_binds),
                &mut |_| vars.get_or_insert(&format!("?__sample_{}", vars.len())),
            );
        }

        Ok(LoweredModifiers {
            base,
            distinct,
            group_by,
            aggregates,
            having,
            pre_group_binds,
            compound_select_exprs,
        })
    }

    /// Walk the SELECT clause for compound expressions that *contain*
    /// aggregates (e.g. `(MAX(?u) - MIN(?u) AS ?spread)`) — bare aggregates
    /// (`MAX(?u) AS ?hi`) are handled separately by `extract_aggregates`.
    /// Returns each as `(alias VarId, AST expression)` for hoisting + post-bind
    /// lowering in `lower_solution_modifiers`. Alias VarIds were registered
    /// upstream by `lower_select_clause`.
    fn collect_compound_aggregate_select_items(
        &mut self,
        select: &SelectClause,
    ) -> Vec<(VarId, AstExpression)> {
        let SelectVariables::Explicit(vars) = &select.variables else {
            return Vec::new();
        };
        let mut items = Vec::new();
        for var in vars {
            if let SelectVariable::Expr { expr, alias, .. } = var {
                if matches!(expr, AstExpression::Aggregate { .. }) {
                    continue;
                }
                if self.expr_contains_aggregate(expr) {
                    items.push((self.register_var(alias), expr.clone()));
                }
            }
        }
        items
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

    /// Build a map from structural expression key → alias `VarId` for every
    /// non-aggregate `(expr AS ?alias)` in the SELECT clause.
    ///
    /// Used to recognize when an unaliased `GROUP BY (expr)` groups on an
    /// expression that the SELECT also projects, so both can share one variable.
    /// The alias vars are already registered by `lower_select_clause`;
    /// `register_var` returns the existing id. The shared variable is a group
    /// key, so `lower_select_extends` keeps its expression a WHERE bind.
    fn select_expr_alias_map(&mut self, select: &SelectClause) -> HashMap<String, VarId> {
        let mut map = HashMap::new();
        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                if let SelectVariable::Expr { expr, alias, .. } = var {
                    if matches!(expr, AstExpression::Aggregate { .. }) {
                        continue;
                    }
                    let key = Self::expr_key_no_span(expr);
                    let var_id = self.register_var(alias);
                    map.entry(key).or_insert(var_id);
                }
            }
        }
        map
    }

    /// Lower a GROUP BY condition to a variable ID and optional pre-GROUP-BY BIND.
    ///
    /// Returns `(var_id, Option<expr>)`, where `expr` binds `var_id`:
    /// - `GROUP BY ?x`              → variable reference, no BIND needed
    /// - `GROUP BY (?x)`            → parenthesized variable, unwrapped to plain variable
    /// - `GROUP BY (expr AS ?alias)` → desugared to BIND(expr AS ?alias) + GROUP BY ?alias
    /// - `GROUP BY (expr)` projected as `(expr AS ?k)` → group on ?k (bound by
    ///   the SELECT expression, which stays a WHERE bind because ?k is a
    ///   key), no new BIND
    /// - `GROUP BY (expr)`          → otherwise, a synthetic `?__group_expr_N` alias
    fn lower_group_condition(
        &mut self,
        cond: &GroupCondition,
        select_expr_aliases: &HashMap<String, VarId>,
    ) -> Result<(VarId, Option<Expression>)> {
        match cond {
            GroupCondition::Var(var) => Ok((self.register_var(var), None)),
            GroupCondition::Expr { expr, alias, .. } => {
                // Try unwrapping brackets to see if it's just a variable
                match expr.unwrap_bracketed() {
                    AstExpression::Var(var) => Ok((self.register_var(var), None)),
                    _ => {
                        // Explicit `GROUP BY (expr AS ?alias)`: desugar to
                        // BIND(expr AS ?alias) + GROUP BY ?alias.
                        if let Some(alias_var) = alias {
                            let lowered = self.lower_expression(expr)?;
                            let var_id = self.register_var(alias_var);
                            return Ok((var_id, Some(lowered)));
                        }

                        // Unaliased `GROUP BY (expr)`: if the SELECT projects the
                        // same expression as `(expr AS ?k)`, group on ?k. The
                        // SELECT expression computes `?k = expr` as a WHERE bind
                        // (a key's expression stays before grouping), so no new
                        // BIND is needed and the projected variable equals the
                        // group value. Otherwise synthesize a fresh group var +
                        // BIND.
                        let key = Self::expr_key_no_span(expr);
                        if let Some(&alias_var) = select_expr_aliases.get(&key) {
                            return Ok((alias_var, None));
                        }

                        let lowered = self.lower_expression(expr)?;
                        let name = format!("?__group_expr_{}", self.vars.len());
                        let var_id = self.vars.get_or_insert(&name);
                        Ok((var_id, Some(lowered)))
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
    /// lowering — SELECT expressions, GROUP BY / aggregates / HAVING, and
    /// expression/aggregate ORDER BY all go through the same shared helpers
    /// (`lower_solution_modifiers`, `lower_select_extends`) — so a subquery
    /// inherits exactly the same modifier semantics as a top-level query. The
    /// resulting `SubqueryPattern` is executed per correlated parent row by
    /// `SubqueryOperator`, which applies the shared solution-modifier tail
    /// (`apply_solution_modifiers`).
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

        // Trailing VALUES clause (`SubSelect ::= … SolutionModifier
        // ValuesClause`): joined with the subquery's WHERE result by placing
        // the VALUES table right after the WHERE patterns — i.e. before this
        // subquery's projection/modifiers, as at the top level. For a
        // modifier-free subquery this is exactly the spec's
        // `M := Join(M, ToMultiSet(data))` insertion point (§18.2.4.3, which
        // applies VALUES before Project); with GROUP BY it joins before
        // grouping rather than after HAVING (see `lower_select_level`). Its
        // variables are in scope of `SELECT *`.
        let mut trailing_values: Vec<Pattern> = match &subselect.values {
            Some(values) => self.lower_graph_pattern(values)?,
            None => Vec::new(),
        };

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
                for p in patterns.iter().chain(&trailing_values) {
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

        // Solution modifiers, SELECT expressions and HAVING through the same
        // path as a top-level SELECT.
        let where_len = patterns.len();
        let level = match trailing_values.as_slice() {
            [values @ Pattern::Values { .. }] => self.lower_select_level(
                &select_clause,
                &subselect.modifiers,
                &mut patterns,
                Some(values),
            )?,
            _ => {
                // Not a single VALUES table (never produced for a trailing
                // VALUES clause): join it with the WHERE like any pattern.
                patterns.append(&mut trailing_values);
                self.lower_select_level(&select_clause, &subselect.modifiers, &mut patterns, None)?
            }
        };
        patterns.splice(where_len..where_len, trailing_values);
        patterns.extend(LoweredSelectLevel::bind_patterns(level.binds));

        // `SELECT *` of a grouping level projects its keys; implicit grouping
        // has none, so the sub-SELECT exports nothing and keeps its row count.
        let select = level.star_projection.unwrap_or(select);

        let BaseModifiers {
            limit,
            offset,
            ordering,
            order_binds,
            // Consumed by `lower_solution_modifiers` (lowered into `order_binds`
            // after aggregate hoisting); always empty here.
            deferred_order_exprs: _,
        } = level.base;

        // Assemble the SubqueryPattern. SELECT Extends ride in the grouping
        // phase; expression/aggregate ORDER BY binds ride on `order_binds` (a
        // dedicated post-grouping stage in the shared modifier tail) so they
        // evaluate uniformly with or without grouping.
        // SPARQL sub-SELECTs are uncorrelated (§18.2): evaluated independently
        // of the enclosing pattern, then joined.
        let mut sq = SubqueryPattern::new(select, patterns).with_uncorrelated();
        if let Some(grouping) = level.grouping {
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
        if level.distinct || subselect.reduced {
            sq = sq.with_distinct();
        }

        // Restore the enclosing scope's aggregate-expression CSE cache (an early
        // `?` error abandons the whole lowering, so success-path restore is enough).
        self.agg_expr_binds = saved_agg_expr_binds;

        Ok(vec![Pattern::Subquery(sq)])
    }
}
