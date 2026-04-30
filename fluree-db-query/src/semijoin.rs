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
//! via the same `SeedOperator` + `build_where_operators_seeded` pattern used by
//! `ExistsOperator`.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::group_aggregate::{binding_to_group_key_owned, CompositeGroupKey};
use crate::ir::Pattern;
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
            key_col_indices: Vec::new(),
            stats,
            planning,
        }
    }

    /// Extract a composite key from a batch row at the given key column indices.
    fn extract_key(&self, batch: &Batch, row_idx: usize) -> CompositeGroupKey {
        let keys = self
            .key_col_indices
            .iter()
            .map(|&ci| binding_to_group_key_owned(batch.get_by_col(row_idx, ci)))
            .collect();
        CompositeGroupKey(keys)
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

    /// Per-row correlated evaluation fallback for rows with unbound key vars.
    /// Uses the same SeedOperator pattern as ExistsOperator::has_match.
    async fn per_row_has_match(
        &self,
        ctx: &ExecutionContext<'_>,
        input_batch: &Batch,
        row_idx: usize,
    ) -> Result<bool> {
        let seed = SeedOperator::from_batch_row(input_batch, row_idx);
        let mut inner_op = build_where_operators_seeded(
            Some(Box::new(seed)),
            &self.inner_patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        inner_op.open(ctx).await?;
        let has_result = loop {
            match inner_op.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => break true,
                Some(_) => continue,
                None => break false,
            }
        };
        inner_op.close();
        Ok(has_result)
    }
}

#[async_trait]
impl Operator for SemijoinOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "SemijoinOperator::open() called in invalid state".into(),
            ));
        }

        // Build phase: execute inner patterns once, collect distinct key tuples.
        let key_var_slice: Vec<VarId> = self.key_vars.clone();
        #[allow(clippy::box_default)]
        let seed: BoxedOperator = Box::new(EmptyOperator::new());
        let mut inner_op = build_where_operators_seeded(
            Some(seed),
            &self.inner_patterns,
            self.stats.clone(),
            Some(&key_var_slice),
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
            for row_idx in 0..batch.len() {
                let key = inner_key_col_indices
                    .iter()
                    .map(|&ci| binding_to_group_key_owned(batch.get_by_col(row_idx, ci)))
                    .collect();
                self.key_set.insert(CompositeGroupKey(key));
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

            let mut keep_rows: Vec<bool> = Vec::with_capacity(input_batch.len());

            for row_idx in 0..input_batch.len() {
                let has_match = if self.all_keys_bound(&input_batch, row_idx) {
                    // Fast path: all key vars bound → probe hash set.
                    let key = self.extract_key(&input_batch, row_idx);
                    self.key_set.contains(&key)
                } else {
                    // Slow path: partial/no binding → per-row correlated evaluation.
                    self.per_row_has_match(ctx, &input_batch, row_idx).await?
                };

                let keep = if self.negated { !has_match } else { has_match };
                keep_rows.push(keep);
            }

            let kept_count = keep_rows.iter().filter(|&&k| k).count();
            if kept_count == 0 {
                continue;
            }
            if kept_count == input_batch.len() {
                return Ok(Some(input_batch));
            }

            // Build filtered batch with only kept rows.
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(kept_count))
                .collect();
            for (row_idx, keep) in keep_rows.iter().enumerate() {
                if *keep {
                    for (col_idx, var) in self.schema.iter().enumerate() {
                        if let Some(col) = input_batch.column(*var) {
                            columns[col_idx].push(col[row_idx].clone());
                        }
                    }
                }
            }
            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
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
                    for row_idx in 0..batch.len() {
                        let has_match = if self.all_keys_bound(&batch, row_idx) {
                            let key = self.extract_key(&batch, row_idx);
                            self.key_set.contains(&key)
                        } else {
                            self.per_row_has_match(ctx, &batch, row_idx).await?
                        };
                        let keep = if self.negated { !has_match } else { has_match };
                        if keep {
                            count = count.checked_add(1).ok_or_else(|| {
                                QueryError::execution("COUNT(*) overflow in semijoin drain_count")
                            })?;
                        }
                    }
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
