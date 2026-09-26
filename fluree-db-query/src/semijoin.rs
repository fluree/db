//! SemijoinOperator — hash-based EXISTS / NOT EXISTS filter.
//!
//! Replaces per-row correlated subquery evaluation with a build-probe approach:
//!
//! 1. **Build phase** (`open`): Execute inner patterns once (uncorrelated), collect
//!    distinct key tuples (the correlation variables) into a `HashSet`.
//! 2. **Probe phase** (`next_batch`): For each outer row, extract key var values and
//!    probe the set. EXISTS keeps matches; NOT EXISTS keeps non-matches.
//!
//! **Partial-binding correctness:** An Unbound key stays free inside EXISTS.
//! For a body made entirely of triple patterns, project the built keys onto
//! the outer row's bound variables and probe that smaller set. Cache a bounded
//! number of these projections per execution. Expressions, compound patterns,
//! and Poisoned keys retain seeded evaluation via [`any_solution`].

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::exists::any_solution;
use crate::group_aggregate::{CompositeGroupKey, GroupKeyOwned};
use crate::ir::Pattern;
use crate::object_binding::{equality_norm, EqualityNorm};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;

/// Avoid retaining every one of the exponentially many binding masks. Further
/// masks use the existing seeded evaluation; cached masks remain reusable.
const MAX_PARTIAL_KEY_SETS: usize = 4;

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
    /// Projection is equivalent to substitution for a conjunction of triples:
    /// all key variables are bound in each inner solution, and unbound outer
    /// variables impose no constraint. Other pattern kinds need their original
    /// seeded semantics (e.g. BIND, OPTIONAL, MINUS and subquery scope).
    partial_keys_safe: bool,
    /// Key positions (not batch columns) -> projected, normalized inner keys.
    partial_key_sets: FxHashMap<Vec<usize>, FxHashSet<CompositeGroupKey>>,
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
        let partial_keys_safe = !inner_patterns.is_empty()
            && inner_patterns
                .iter()
                .all(|p| matches!(p, Pattern::Triple(_)));
        Self {
            child,
            inner_patterns,
            key_vars,
            negated,
            schema,
            state: OperatorState::Created,
            key_set: FxHashSet::default(),
            partial_keys_safe,
            partial_key_sets: FxHashMap::default(),
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

    /// Probe only the bound key positions. `None` means the row must use seeded
    /// evaluation. A scratch vector avoids allocating a binding mask per row.
    fn partial_has_match(
        &mut self,
        ctx: &ExecutionContext<'_>,
        batch: &Batch,
        row_idx: usize,
        positions: &mut Vec<usize>,
    ) -> Result<Option<bool>> {
        if !self.partial_keys_safe {
            return Ok(None);
        }
        positions.clear();
        for (position, &col) in self.key_col_indices.iter().enumerate() {
            match batch.get_by_col(row_idx, col) {
                Binding::Unbound => {}
                // Poisoned is not a free variable and must not be rebound.
                Binding::Poisoned => return Ok(None),
                _ => positions.push(position),
            }
        }
        if positions.is_empty() {
            return Ok(Some(!self.key_set.is_empty()));
        }
        if !self.partial_key_sets.contains_key(positions.as_slice()) {
            if self.partial_key_sets.len() >= MAX_PARTIAL_KEY_SETS {
                return Ok(None);
            }
            let mut projected = FxHashSet::default();
            let entry_bytes = std::mem::size_of::<CompositeGroupKey>()
                + positions.len() * std::mem::size_of::<GroupKeyOwned>();
            let mut charged_rows = 0;
            for (i, key) in self.key_set.iter().enumerate() {
                if i.is_multiple_of(1024) {
                    ctx.record_alloc((projected.len() - charged_rows) * entry_bytes);
                    charged_rows = projected.len();
                    ctx.checkpoint()?;
                }
                projected.insert(CompositeGroupKey(
                    positions.iter().map(|&p| key.0[p].clone()).collect(),
                ));
            }
            ctx.record_alloc((projected.len() - charged_rows) * entry_bytes);
            ctx.checkpoint()?;
            tracing::debug!(
                bound_keys = positions.len(),
                source_keys = self.key_set.len(),
                projected_keys = projected.len(),
                "semijoin partial-key lookup built"
            );
            self.partial_key_sets.insert(positions.clone(), projected);
        }
        let key = CompositeGroupKey::normalized(
            positions
                .iter()
                .map(|&p| batch.get_by_col(row_idx, self.key_col_indices[p])),
            &self.norm,
        );
        Ok(Some(
            self.partial_key_sets[positions.as_slice()].contains(&key),
        ))
    }

    /// Per-row keep flags: full or projected hash probes when sound, otherwise
    /// the existing correlated evaluation.
    async fn keep_mask(&mut self, ctx: &ExecutionContext<'_>, batch: &Batch) -> Result<Vec<bool>> {
        let mut keep = Vec::with_capacity(batch.len());
        let mut positions = Vec::new();
        let mut projected_rows = 0usize;
        let mut correlated_rows = 0usize;
        for row_idx in 0..batch.len() {
            let has_match = if self.all_keys_bound(batch, row_idx) {
                let key = row_key(batch, row_idx, &self.key_col_indices, &self.norm);
                self.key_set.contains(&key)
            } else if let Some(found) =
                self.partial_has_match(ctx, batch, row_idx, &mut positions)?
            {
                projected_rows += 1;
                found
            } else {
                correlated_rows += 1;
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
        if projected_rows + correlated_rows > 0 {
            tracing::debug!(
                projected_rows,
                correlated_rows,
                "semijoin partial-key probes"
            );
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
        self.partial_key_sets.clear();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::seed::BatchSeedOperator;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::{LedgerSnapshot, QueryCancellation};

    fn batch(rows: Vec<Vec<Binding>>) -> Batch {
        let schema = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        Batch::new(
            schema,
            (0..3)
                .map(|col| rows.iter().map(|r| r[col].clone()).collect())
                .collect(),
        )
        .unwrap()
    }

    fn triple() -> Pattern {
        Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Var(VarId(1)),
            Term::Var(VarId(2)),
        ))
    }

    /// Isolate the projection/probe from the scan, whose visibility and encoding
    /// are covered by the indexed/overlay/history integration regression.
    fn semijoin(patterns: Vec<Pattern>) -> SemijoinOperator {
        let keys = batch(vec![
            vec![
                Binding::encoded_sid(1),
                Binding::encoded_sid(2),
                Binding::encoded_sid(3),
            ],
            vec![
                Binding::encoded_sid(1),
                Binding::encoded_sid(4),
                Binding::encoded_sid(5),
            ],
        ]);
        let child = Box::new(BatchSeedOperator::from_batch(keys.clone()));
        let mut op = SemijoinOperator::new(
            child,
            patterns,
            vec![VarId(0), VarId(1), VarId(2)],
            false,
            None,
            PlanningContext::current(),
        );
        op.key_col_indices = vec![0, 1, 2];
        for row in 0..keys.len() {
            op.key_set.insert(row_key(&keys, row, &[0, 1, 2], &None));
        }
        op
    }

    #[test]
    fn projected_keys_preserve_bound_constraints_and_handle_empty_keys() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let mut op = semijoin(vec![triple()]);
        let rows = batch(vec![
            vec![
                Binding::encoded_sid(1),
                Binding::Unbound,
                Binding::encoded_sid(5),
            ],
            vec![
                Binding::encoded_sid(1),
                Binding::Unbound,
                Binding::encoded_sid(9),
            ],
            vec![
                Binding::encoded_sid(9),
                Binding::Unbound,
                Binding::encoded_sid(5),
            ],
            vec![Binding::Unbound, Binding::Unbound, Binding::Unbound],
            vec![Binding::encoded_sid(1), Binding::Poisoned, Binding::Unbound],
        ]);
        let mut positions = Vec::new();
        for (row, expected) in [Some(true), Some(false), Some(false), Some(true), None]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                op.partial_has_match(&ctx, &rows, row, &mut positions)
                    .unwrap(),
                expected
            );
        }
        assert_eq!(op.partial_key_sets.len(), 1);
        op.close();
        assert!(op.partial_key_sets.is_empty());
        assert_eq!(
            op.partial_has_match(&ctx, &rows, 3, &mut positions)
                .unwrap(),
            Some(false)
        );
    }

    #[test]
    fn projected_key_cache_is_bounded_and_keeps_existing_masks() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let mut op = semijoin(vec![triple()]);
        let mut positions = Vec::new();
        // Three single-column and three two-column projections; only four may
        // be cached. Additional masks decline, rather than evicting/rebuilding.
        for (i, mask) in [1, 2, 4, 3, 5, 6].into_iter().enumerate() {
            let rows = batch(vec![(0..3)
                .map(|p| {
                    if mask & (1 << p) == 0 {
                        Binding::Unbound
                    } else {
                        Binding::encoded_sid(p + 1)
                    }
                })
                .collect()]);
            let found = op
                .partial_has_match(&ctx, &rows, 0, &mut positions)
                .unwrap();
            assert_eq!(
                found,
                if i < MAX_PARTIAL_KEY_SETS {
                    Some(true)
                } else {
                    None
                }
            );
        }
        assert_eq!(op.partial_key_sets.len(), MAX_PARTIAL_KEY_SETS);
        let rows = batch(vec![vec![
            Binding::encoded_sid(1),
            Binding::Unbound,
            Binding::Unbound,
        ]]);
        assert_eq!(
            op.partial_has_match(&ctx, &rows, 0, &mut positions)
                .unwrap(),
            Some(true)
        );
    }

    #[test]
    fn compound_inner_patterns_keep_seeded_evaluation() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let rows = batch(vec![vec![
            Binding::encoded_sid(1),
            Binding::Unbound,
            Binding::Unbound,
        ]]);
        for patterns in [
            vec![],
            vec![Pattern::Optional(vec![triple()])],
            vec![Pattern::Union(vec![vec![triple()]])],
            vec![triple(), Pattern::Minus(vec![triple()])],
        ] {
            let mut op = semijoin(patterns);
            assert_eq!(
                op.partial_has_match(&ctx, &rows, 0, &mut Vec::new())
                    .unwrap(),
                None
            );
            assert!(op.partial_key_sets.is_empty());
        }
    }

    #[test]
    fn projected_lookup_honors_memory_budget_and_cancellation() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let cancellation = QueryCancellation::new();
        cancellation.set_memory_limit(1);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let mut op = semijoin(vec![triple()]);
        let rows = batch(vec![vec![
            Binding::encoded_sid(1),
            Binding::Unbound,
            Binding::Unbound,
        ]]);
        let err = op
            .partial_has_match(&ctx, &rows, 0, &mut Vec::new())
            .unwrap_err();
        assert!(
            matches!(err, QueryError::MemoryBudgetExceeded { .. }),
            "{err:?}"
        );
        assert!(op.partial_key_sets.is_empty());
        let cancellation = QueryCancellation::new();
        cancellation.cancel_with(fluree_db_core::QueryCancellationReason::Timeout);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let err = op
            .partial_has_match(&ctx, &rows, 0, &mut Vec::new())
            .unwrap_err();
        assert!(matches!(err, QueryError::Cancelled { .. }), "{err:?}");
        assert!(op.partial_key_sets.is_empty());
    }
}
