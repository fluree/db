//! Filter operator
//!
//! This module provides the FilterOperator which wraps a child operator
//! and filters rows based on a predicate expression.
//!
//! # Filter Evaluation Semantics
//!
//! This uses **two-valued logic** (true/false), not SQL 3-valued NULL logic:
//!
//! - **Unbound variables**: Comparisons involving unbound vars yield `false`
//! - **Type mismatches**: Comparisons between incompatible types yield `false`
//!   (except `!=` which yields `true` for mismatched types)
//! - **NaN**: Comparisons involving NaN yield `false` (except `!=` → `true`)
//! - **Logical operators**: Standard boolean logic (AND, OR, NOT)
//!
//! Note: `NOT(unbound_comparison)` evaluates to `true` because the inner
//! comparison returns `false`, which is then negated. This differs from
//! SQL NULL semantics where NULL comparisons propagate.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::eval::PreparedBoolExpression;
use crate::execute::build_where_operators_seeded;
use crate::ir::triple::Ref;
use crate::ir::{Expression, FlakeValue, Pattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::var_registry::VarId;
use async_trait::async_trait;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashSet;
use std::sync::Arc;

use crate::fast_path_common::{
    collect_subjects_for_predicate_set, fast_path_store, try_normalize_pred_sid,
};
use fluree_db_core::Sid;

/// Filter rows from a batch using two-valued logic.
///
/// Evaluates `expr` for each row in `batch`. Rows where the expression evaluates
/// to `true` are kept; rows that evaluate to `false` or encounter an error
/// (type mismatch, unbound variable) are filtered out.
///
/// Returns `None` if no rows pass the filter.
pub fn filter_batch(
    batch: &Batch,
    expr: &PreparedBoolExpression,
    schema: &Arc<[VarId]>,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Batch>> {
    let mut keep_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.len() {
        let Some(row) = batch.row_view(row_idx) else {
            continue;
        };
        if expr.eval_to_bool_non_strict(&row, Some(ctx))? {
            keep_indices.push(row_idx);
        }
    }

    if keep_indices.is_empty() {
        return Ok(None);
    }

    let columns: Vec<Vec<Binding>> = (0..schema.len())
        .map(|col_idx| {
            let src_col = batch
                .column_by_idx(col_idx)
                .expect("batch schema must match operator schema");
            keep_indices
                .iter()
                .map(|&row_idx| src_col[row_idx].clone())
                .collect()
        })
        .collect();

    Ok(Some(Batch::new(schema.clone(), columns)?))
}

/// Check if an expression tree contains any `Expression::Exists` nodes.
pub fn contains_exists(expr: &Expression) -> bool {
    match expr {
        Expression::Exists { .. } => true,
        Expression::Call { args, .. } => args.iter().any(contains_exists),
        Expression::Var(_) | Expression::Const(_) => false,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ExistsSemijoinKey {
    subject_var: VarId,
    pred: Sid,
}

/// Cached subject sets for simple correlated EXISTS patterns.
///
/// The cache maps (subject_var, p_id) -> set of matching subject IDs (s_id).
#[derive(Default)]
struct ExistsSemijoinCache {
    subjects_by_key: FxHashMap<ExistsSemijoinKey, FxHashSet<u64>>,
}

fn allow_exists_semijoin_fast_path(ctx: &ExecutionContext<'_>) -> bool {
    fast_path_store(ctx).is_some()
}

fn collect_simple_exists_keys(expr: &Expression, out: &mut Vec<(VarId, Ref)>) {
    match expr {
        Expression::Exists {
            patterns,
            negated: _,
        } => {
            // Only handle EXISTS { ?s <p> ?o } (single triple) here; more complex
            // inner patterns keep the generic per-row seeded execution.
            if patterns.len() != 1 {
                return;
            }
            let Pattern::Triple(tp) = &patterns[0] else {
                return;
            };
            let Ref::Var(sv) = &tp.s else {
                return;
            };
            if !tp.p_bound() {
                return;
            }
            if tp.o.is_bound() {
                return;
            }
            if tp.dtc.is_some() {
                return;
            }
            out.push((*sv, tp.p.clone()));
        }
        Expression::Call { func: _, args } => {
            for a in args {
                collect_simple_exists_keys(a, out);
            }
        }
        Expression::Var(_) | Expression::Const(_) => {}
    }
}

fn build_exists_semijoin_cache(
    expr: &Expression,
    schema: &[VarId],
    ctx: &ExecutionContext<'_>,
) -> Result<Option<ExistsSemijoinCache>> {
    if !allow_exists_semijoin_fast_path(ctx) {
        return Ok(None);
    }
    let Some(store) = ctx.binary_store.as_ref() else {
        return Ok(None);
    };

    let mut exists_nodes: Vec<(VarId, Ref)> = Vec::new();
    collect_simple_exists_keys(expr, &mut exists_nodes);
    if exists_nodes.is_empty() {
        return Ok(None);
    }

    // Only cache EXISTS nodes whose correlated var is actually present in the batch schema.
    let schema_vars: HashSet<VarId> = schema.iter().copied().collect();

    let mut cache = ExistsSemijoinCache::default();
    for (sv, pred_ref) in exists_nodes {
        if !schema_vars.contains(&sv) {
            continue;
        }
        let Some(pred_sid) = try_normalize_pred_sid(store, &pred_ref) else {
            continue;
        };
        let key = ExistsSemijoinKey {
            subject_var: sv,
            pred: pred_sid.clone(),
        };
        if cache.subjects_by_key.contains_key(&key) {
            continue;
        }

        // Resolve p_id once, then scan PSOT to collect matching subjects.
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            cache.subjects_by_key.insert(key, FxHashSet::default());
            continue;
        };

        let subjects = collect_subjects_for_predicate_set(store, ctx.binary_g_id, p_id)?;
        cache.subjects_by_key.insert(key, subjects);
    }

    if cache.subjects_by_key.is_empty() {
        Ok(None)
    } else {
        Ok(Some(cache))
    }
}

/// Check if an EXISTS subquery is uncorrelated with respect to a batch schema.
///
/// An EXISTS is uncorrelated when its pattern variables share no variables with
/// the batch schema — meaning the result is the same regardless of the row.
fn is_uncorrelated_exists(patterns: &[Pattern], batch_schema: &[VarId]) -> bool {
    let schema_vars: HashSet<VarId> = batch_schema.iter().copied().collect();
    let pattern_vars: HashSet<VarId> = patterns
        .iter()
        .flat_map(super::ir::Pattern::referenced_vars)
        .collect();
    pattern_vars.is_disjoint(&schema_vars)
}

/// Evaluate an EXISTS subquery once (uncorrelated) using an empty seed.
async fn eval_exists_uncorrelated(
    patterns: &[Pattern],
    negated: bool,
    ctx: &ExecutionContext<'_>,
    planning: &crate::temporal_mode::PlanningContext,
) -> Result<bool> {
    #[expect(clippy::box_default)]
    let seed: BoxedOperator = Box::new(EmptyOperator::new());
    let mut exists_op = build_where_operators_seeded(Some(seed), patterns, None, None, planning)?;

    exists_op.open(ctx).await?;

    let has_match = loop {
        match exists_op.next_batch(ctx).await? {
            Some(b) if !b.is_empty() => break true,
            Some(_) => continue,
            None => break false,
        }
    };

    exists_op.close();
    Ok(if negated { !has_match } else { has_match })
}

/// Evaluate an EXISTS subquery for a given row (correlated).
///
/// Seeds the subquery with the current row's bindings and checks if any
/// result is produced.
async fn eval_exists_for_row(
    patterns: &[Pattern],
    negated: bool,
    batch: &Batch,
    row_idx: usize,
    ctx: &ExecutionContext<'_>,
    planning: &crate::temporal_mode::PlanningContext,
) -> Result<bool> {
    let seed = SeedOperator::from_batch_row(batch, row_idx);
    let mut exists_op =
        build_where_operators_seeded(Some(Box::new(seed)), patterns, None, None, planning)?;

    exists_op.open(ctx).await?;

    let has_match = loop {
        match exists_op.next_batch(ctx).await? {
            Some(b) if !b.is_empty() => break true,
            Some(_) => continue,
            None => break false,
        }
    };

    exists_op.close();
    Ok(if negated { !has_match } else { has_match })
}

/// Pre-evaluate all uncorrelated EXISTS nodes in an expression tree.
///
/// Called once per batch (not per row). Uncorrelated EXISTS subexpressions
/// are evaluated against an empty seed and replaced with boolean constants.
/// Correlated EXISTS nodes are left in place for per-row evaluation.
fn pre_resolve_uncorrelated<'a>(
    expr: &'a Expression,
    batch_schema: &'a [VarId],
    ctx: &'a ExecutionContext<'a>,
    planning: &'a crate::temporal_mode::PlanningContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Expression>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expression::Exists { patterns, negated } => {
                if is_uncorrelated_exists(patterns, batch_schema) {
                    let result =
                        eval_exists_uncorrelated(patterns, *negated, ctx, planning).await?;
                    Ok(Expression::Const(FlakeValue::Boolean(result)))
                } else {
                    Ok(expr.clone())
                }
            }
            Expression::Call { func, args } => {
                let mut resolved_args = Vec::with_capacity(args.len());
                for arg in args {
                    resolved_args
                        .push(pre_resolve_uncorrelated(arg, batch_schema, ctx, planning).await?);
                }
                Ok(Expression::Call {
                    func: func.clone(),
                    args: resolved_args,
                })
            }
            _ => Ok(expr.clone()),
        }
    })
}

fn try_eval_simple_exists_semijoin(
    patterns: &[Pattern],
    negated: bool,
    batch: &Batch,
    row_idx: usize,
    ctx: &ExecutionContext<'_>,
    cache: &ExistsSemijoinCache,
) -> Result<Option<bool>> {
    let Some(store) = ctx.binary_store.as_ref() else {
        return Ok(None);
    };
    if patterns.len() != 1 {
        return Ok(None);
    }
    let Pattern::Triple(tp) = &patterns[0] else {
        return Ok(None);
    };
    let Ref::Var(subject_var) = &tp.s else {
        return Ok(None);
    };
    if tp.dtc.is_some() {
        return Ok(None);
    }
    if !tp.p_bound() {
        return Ok(None);
    }
    if tp.o.is_bound() {
        return Ok(None);
    }
    let Some(pred_sid) = try_normalize_pred_sid(store, &tp.p) else {
        return Ok(None);
    };

    let key = ExistsSemijoinKey {
        subject_var: *subject_var,
        pred: pred_sid,
    };
    let Some(subjects) = cache.subjects_by_key.get(&key) else {
        return Ok(None);
    };

    let Some(binding) = batch.get(row_idx, *subject_var) else {
        return Ok(Some(false));
    };
    let Binding::Sid { sid, .. } = binding else {
        // Only handle the common single-ledger SID binding here.
        return Ok(None);
    };

    let s_id = store
        .find_subject_id_by_parts(sid.namespace_code, sid.name.as_ref())
        .map_err(|e| crate::error::QueryError::Internal(format!("sid->s_id: {e}")))?;

    let has_match = s_id.is_some_and(|id| subjects.contains(&id));
    Ok(Some(if negated { !has_match } else { has_match }))
}

/// Replace remaining (correlated) `Expression::Exists` nodes with per-row constants.
///
/// Called once per row. Evaluates correlated EXISTS subqueries seeded with
/// the current row's bindings and replaces them with `Const(Bool(result))`.
fn resolve_exists_for_row<'a>(
    expr: &'a Expression,
    batch: &'a Batch,
    row_idx: usize,
    ctx: &'a ExecutionContext<'a>,
    cache: Option<&'a ExistsSemijoinCache>,
    planning: &'a crate::temporal_mode::PlanningContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Expression>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expression::Exists { patterns, negated } => {
                if let Some(c) = cache {
                    if let Some(result) =
                        try_eval_simple_exists_semijoin(patterns, *negated, batch, row_idx, ctx, c)?
                    {
                        return Ok(Expression::Const(FlakeValue::Boolean(result)));
                    }
                }

                let result =
                    eval_exists_for_row(patterns, *negated, batch, row_idx, ctx, planning).await?;
                Ok(Expression::Const(FlakeValue::Boolean(result)))
            }
            Expression::Call { func, args } => {
                let mut resolved_args = Vec::with_capacity(args.len());
                for arg in args {
                    resolved_args.push(
                        resolve_exists_for_row(arg, batch, row_idx, ctx, cache, planning).await?,
                    );
                }
                Ok(Expression::Call {
                    func: func.clone(),
                    args: resolved_args,
                })
            }
            // Var and Const have no EXISTS nodes — already resolved or irrelevant
            _ => Ok(expr.clone()),
        }
    })
}

/// Filter a batch using an expression that contains EXISTS subexpressions.
///
/// Two-phase evaluation:
/// 1. Pre-resolve uncorrelated EXISTS once per batch (O(1) per subexpression).
/// 2. For each row, resolve remaining correlated EXISTS per-row, then evaluate.
///
/// If ALL EXISTS subexpressions are uncorrelated, phase 2 skips async work
/// entirely and uses the fast synchronous `filter_batch` path.
async fn filter_batch_with_exists(
    batch: &Batch,
    expr: &Expression,
    schema: &Arc<[VarId]>,
    ctx: &ExecutionContext<'_>,
    cache: Option<&ExistsSemijoinCache>,
    planning: &crate::temporal_mode::PlanningContext,
) -> Result<Option<Batch>> {
    // Phase 1: resolve uncorrelated EXISTS once for the whole batch
    let partially_resolved = pre_resolve_uncorrelated(expr, batch.schema(), ctx, planning).await?;

    // If no EXISTS nodes remain, we can use the fast synchronous path
    if !contains_exists(&partially_resolved) {
        let prepared = PreparedBoolExpression::new(partially_resolved);
        return filter_batch(batch, &prepared, schema, ctx);
    }

    // Phase 2: resolve remaining correlated EXISTS per-row
    let mut keep_indices: Vec<usize> = Vec::new();

    for row_idx in 0..batch.len() {
        let resolved_expr =
            resolve_exists_for_row(&partially_resolved, batch, row_idx, ctx, cache, planning)
                .await?;
        let Some(row) = batch.row_view(row_idx) else {
            continue;
        };
        let pass = resolved_expr.eval_to_bool_non_strict(&row, Some(ctx))?;
        if pass {
            keep_indices.push(row_idx);
        }
    }

    if keep_indices.is_empty() {
        return Ok(None);
    }

    let columns: Vec<Vec<Binding>> = (0..schema.len())
        .map(|col_idx| {
            let src_col = batch
                .column_by_idx(col_idx)
                .expect("batch schema must match operator schema");
            keep_indices
                .iter()
                .map(|&row_idx| src_col[row_idx].clone())
                .collect()
        })
        .collect();

    Ok(Some(Batch::new(schema.clone(), columns)?))
}

/// Filter operator - applies a predicate to each row from child
///
/// Rows where the filter evaluates to `false` or encounters an error
/// (type mismatch, unbound var) are filtered out.
pub struct FilterOperator {
    /// Child operator providing input rows
    child: BoxedOperator,
    /// Filter expression to evaluate
    expr: Expression,
    prepared_expr: PreparedBoolExpression,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Whether the expression contains EXISTS subexpressions (cached)
    has_exists: bool,
    /// Optional semijoin caches for simple correlated EXISTS patterns.
    exists_semijoin: Option<ExistsSemijoinCache>,
    /// Planning context captured at planner-time for FILTER EXISTS subplans.
    planning: crate::temporal_mode::PlanningContext,
}

impl FilterOperator {
    /// Create a new filter operator with current-state planning context.
    ///
    /// Construction sites that have a captured [`PlanningContext`] should call
    /// [`FilterOperator::new_with_planning`] instead so that FILTER EXISTS
    /// subplans inherit the same temporal mode.
    pub fn new(child: BoxedOperator, expr: Expression) -> Self {
        Self::new_with_planning(
            child,
            expr,
            crate::temporal_mode::PlanningContext::current(),
        )
    }

    /// Create a new filter operator that captures a planning context for any
    /// FILTER EXISTS subplans.
    pub fn new_with_planning(
        child: BoxedOperator,
        expr: Expression,
        planning: crate::temporal_mode::PlanningContext,
    ) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        let has_exists = contains_exists(&expr);
        let prepared_expr = PreparedBoolExpression::new(expr.clone());
        Self {
            child,
            expr,
            prepared_expr,
            schema,
            state: OperatorState::Created,
            has_exists,
            exists_semijoin: None,
            planning,
        }
    }

    /// Get the filter expression
    pub fn expr(&self) -> &Expression {
        &self.expr
    }
}

#[async_trait]
impl Operator for FilterOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        if self.has_exists {
            self.exists_semijoin = build_exists_semijoin_cache(&self.expr, &self.schema, ctx)?;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if batch.is_empty() {
                continue;
            }

            let filtered = if self.has_exists {
                filter_batch_with_exists(
                    &batch,
                    &self.expr,
                    &self.schema,
                    ctx,
                    self.exists_semijoin.as_ref(),
                    &self.planning,
                )
                .await?
            } else {
                filter_batch(&batch, &self.prepared_expr, &self.schema, ctx)?
            };

            if let Some(filtered) = filtered {
                return Ok(Some(filtered));
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: child rows * selectivity (assume 50% for now)
        self.child.estimated_rows().map(|r| r / 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::FlakeValue;
    use fluree_db_core::Sid;

    #[test]
    fn contains_exists_false_for_var() {
        assert!(!contains_exists(&Expression::Var(VarId(0))));
    }

    #[test]
    fn contains_exists_false_for_const() {
        assert!(!contains_exists(&Expression::Const(FlakeValue::Boolean(
            true
        ))));
    }

    #[test]
    fn contains_exists_true_for_direct() {
        let expr = Expression::Exists {
            patterns: vec![],
            negated: false,
        };
        assert!(contains_exists(&expr));
    }

    #[test]
    fn contains_exists_true_for_negated() {
        let expr = Expression::Exists {
            patterns: vec![],
            negated: true,
        };
        assert!(contains_exists(&expr));
    }

    #[test]
    fn contains_exists_true_nested_in_call() {
        // FILTER(?x = ?y || NOT EXISTS { ... })
        let exists = Expression::Exists {
            patterns: vec![],
            negated: true,
        };
        let eq = Expression::eq(Expression::Var(VarId(0)), Expression::Var(VarId(1)));
        let or = Expression::or(vec![eq, exists]);
        assert!(contains_exists(&or));
    }

    #[test]
    fn contains_exists_false_for_plain_call() {
        let eq = Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(42)),
        );
        assert!(!contains_exists(&eq));
    }

    #[test]
    fn contains_exists_deeply_nested() {
        // AND(OR(true, EXISTS{}), ?x > 5)
        let exists = Expression::Exists {
            patterns: vec![],
            negated: false,
        };
        let inner_or = Expression::or(vec![Expression::Const(FlakeValue::Boolean(true)), exists]);
        let gt = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(5)),
        );
        let and = Expression::and(vec![inner_or, gt]);
        assert!(contains_exists(&and));
    }

    #[test]
    fn uncorrelated_exists_no_shared_vars() {
        // EXISTS { ?z :p ?w } where batch schema is [?x, ?y]
        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(10)),
            Ref::Sid(Sid::new(100, "p")),
            Term::Var(VarId(11)),
        ))];
        let schema = &[VarId(0), VarId(1)];
        assert!(is_uncorrelated_exists(&patterns, schema));
    }

    #[test]
    fn correlated_exists_shared_vars() {
        // EXISTS { ?x :p ?w } where batch schema is [?x, ?y]
        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)), // shared with schema
            Ref::Sid(Sid::new(100, "p")),
            Term::Var(VarId(11)),
        ))];
        let schema = &[VarId(0), VarId(1)];
        assert!(!is_uncorrelated_exists(&patterns, schema));
    }
}
