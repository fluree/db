//! SemijoinOperator — hash-based EXISTS / NOT EXISTS filter.
//!
//! Replaces per-row correlated subquery evaluation with a build-probe approach:
//!
//! 1. **Build phase** (`open`): Execute inner patterns once (uncorrelated), collect
//!    distinct key tuples (the correlation variables) into a `HashSet`.
//! 2. **Probe phase** (`next_batch`): For each outer row, extract key var values and
//!    probe the set. EXISTS keeps matches; NOT EXISTS keeps non-matches.
//!
//! **Partial-binding correctness:** When any key var is Unbound or Poisoned in an
//! outer row, the hash probe is not valid (SPARQL substitution leaves unbound vars
//! free in the inner query). These rows fall back to per-row correlated evaluation
//! via [`any_solution`], as `ExistsOperator` does.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::exists::any_solution;
use crate::group_aggregate::CompositeGroupKey;
use crate::ir::Pattern;
use crate::object_binding::{equality_norm, EqualityNorm};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use rustc_hash::FxHashSet;
use std::sync::Arc;

pub struct SemijoinOperator {
    /// Child operator providing outer solutions.
    child: BoxedOperator,
    /// Inner EXISTS/NOT EXISTS patterns.
    inner_patterns: Vec<Pattern>,
    /// Correlation variables (intersection of outer schema and inner free vars),
    /// in child schema order for stable extraction.
    key_vars: Vec<VarId>,
    /// If true, this is NOT EXISTS (anti-semijoin).
    negated: bool,
    /// Output schema (same as child — EXISTS doesn't add variables).
    schema: Arc<[VarId]>,
    state: OperatorState,
    /// Hash set of distinct key tuples from the inner side, built in `open()`.
    key_set: FxHashSet<CompositeGroupKey>,
    /// Column indices of key_vars within child.schema(), computed in `open()`.
    key_col_indices: Vec<usize>,
    /// Stats for nested query building.
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time for the inner subplan.
    planning: PlanningContext,
    /// Store for normalizing decoded bindings to encoded form on both sides,
    /// so mixed-representation rows key identically. `None` outside
    /// single-ledger binary execution.
    norm: Option<EqualityNorm>,
}

impl SemijoinOperator {
    pub fn new(
        child: BoxedOperator,
        inner_patterns: Vec<Pattern>,
        key_vars: Vec<VarId>,
        negated: bool,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            inner_patterns,
            key_vars,
            negated,
            schema,
            state: OperatorState::Created,
            key_set: FxHashSet::default(),
            norm: None,
            key_col_indices: Vec::new(),
            stats,
            planning,
        }
    }

    /// Check if all key vars are bound (not Unbound or Poisoned) in a row.
    fn all_keys_bound(&self, batch: &Batch, row_idx: usize) -> bool {
        self.key_col_indices.iter().all(|&ci| {
            !matches!(
                batch.get_by_col(row_idx, ci),
                Binding::Unbound | Binding::Poisoned
            )
        })
    }

    /// Per-row keep flags: a hash probe when every key var is bound,
    /// otherwise a per-row correlated evaluation.
    async fn keep_mask(&self, ctx: &ExecutionContext<'_>, batch: &Batch) -> Result<Vec<bool>> {
        let mut keep = Vec::with_capacity(batch.len());
        for row_idx in 0..batch.len() {
            let has_match = if self.all_keys_bound(batch, row_idx) {
                let key = row_key(batch, row_idx, &self.key_col_indices, &self.norm);
                self.key_set.contains(&key)
            } else {
                let seed = SeedOperator::from_batch_row(batch, row_idx);
                any_solution(
                    Box::new(seed),
                    &self.inner_patterns,
                    self.stats.clone(),
                    &self.planning,
                    ctx,
                )
                .await?
            };
            keep.push(if self.negated { !has_match } else { has_match });
        }
        Ok(keep)
    }
}

/// Composite key over the columns `cols` of one row.
fn row_key(
    batch: &Batch,
    row_idx: usize,
    cols: &[usize],
    norm: &Option<EqualityNorm>,
) -> CompositeGroupKey {
    CompositeGroupKey::normalized(cols.iter().map(|&ci| batch.get_by_col(row_idx, ci)), norm)
}

#[async_trait]
impl Operator for SemijoinOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "SemijoinOperator::open() called in invalid state".into(),
            ));
        }
        if self.norm.is_none() {
            self.norm = equality_norm(ctx);
        }

        // Build phase: execute inner patterns once, collect distinct key tuples.
        #[allow(clippy::box_default)]
        let seed: BoxedOperator = Box::new(EmptyOperator::new());
        let mut inner_op = build_where_operators_seeded(
            Some(seed),
            &self.inner_patterns,
            self.stats.clone(),
            Some(&self.key_vars),
            &self.planning,
        )?;

        // Compute column indices for key vars within the inner operator's schema.
        let inner_schema = inner_op.schema().to_vec();
        let inner_key_col_indices: Vec<usize> = self
            .key_vars
            .iter()
            .map(|kv| {
                inner_schema.iter().position(|v| v == kv).ok_or_else(|| {
                    QueryError::Internal(format!("key var {kv:?} not found in inner schema"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        inner_op.open(ctx).await?;

        while let Some(batch) = inner_op.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            for row_idx in 0..batch.len() {
                let key = row_key(&batch, row_idx, &inner_key_col_indices, &self.norm);
                self.key_set.insert(key);
            }
        }
        inner_op.close();

        // Compute key column indices for the child (outer) schema.
        let child_schema = self.child.schema().to_vec();
        self.key_col_indices = self
            .key_vars
            .iter()
            .map(|kv| {
                child_schema.iter().position(|v| v == kv).ok_or_else(|| {
                    QueryError::Internal(format!("key var {kv:?} not found in child schema"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            let keep = self.keep_mask(ctx, &input_batch).await?;
            if let Some(kept) = input_batch.filter_rows(&keep) {
                return Ok(Some(kept));
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.key_set.clear();
        self.state = OperatorState::Closed;
    }

    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        let mut count: u64 = 0;
        loop {
            match self.child.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => {
                    let keep = self.keep_mask(ctx, &batch).await?;
                    let kept = keep.iter().filter(|&&k| k).count() as u64;
                    count = count.checked_add(kept).ok_or_else(|| {
                        QueryError::execution("COUNT(*) overflow in semijoin drain_count")
                    })?;
                }
                Some(_) => continue,
                None => break,
            }
        }
        self.state = OperatorState::Exhausted;
        Ok(Some(count))
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child.estimated_rows()
    }
}
