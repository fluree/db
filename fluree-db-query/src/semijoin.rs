//! SemijoinOperator — hash-based EXISTS / NOT EXISTS filter.
//!
//! Replaces per-row correlated subquery evaluation with a build-probe approach:
//!
//! 1. **Build phase** (`open`): Execute inner patterns once, collect distinct key
//!    tuples (the correlation variables) into a `HashSet`. When the outer side
//!    is small against the inner relation, the build is instead seeded by the
//!    outer keys, one bounded chunk of outer rows at a time, so the cost
//!    follows the outer side rather than the whole inner relation.
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
use crate::seed::{BatchSeedOperator, EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;
use std::sync::Arc;

/// Avoid retaining every one of the exponentially many binding masks. Further
/// masks use the existing seeded evaluation; cached masks remain reusable.
const MAX_PARTIAL_KEY_SETS: usize = 4;

/// Distinct outer keys per seeded build; a larger outer side is seeded one
/// chunk of this many keys at a time.
const SEEDED_BUILD_MAX_KEYS: usize = 1024;

/// Bound on the outer rows buffered per chunk, whatever their keys.
const SEEDED_BUILD_MAX_ROWS: usize = 64 * 1024;

/// A seeded key costs an index lookup where the unseeded build costs a scan
/// row: seeding wins while the outer side is under this fraction of the
/// inner relation.
const SEEDED_KEY_COST: f64 = 16.0;

/// Approximate retained key storage, shared by the base and projected sets.
/// Counts the tuple and its cells; excludes table slack and shared payloads.
fn key_entry_bytes(width: usize) -> usize {
    std::mem::size_of::<CompositeGroupKey>() + width * std::mem::size_of::<GroupKeyOwned>()
}

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
    /// The current chunk's outer rows, which `key_set` was seeded for.
    buffered: VecDeque<Batch>,
    /// Query-budget bytes charged for `buffered`, released as it drains.
    buffered_bytes: usize,
    /// Query-budget bytes charged for a seeded `key_set`, released per chunk.
    key_set_bytes: usize,
    /// The child returned `None` while buffering.
    child_exhausted: bool,
    /// Builds are seeded per chunk of outer rows; otherwise one unseeded build
    /// precedes opening the child, so the two never hold memory together.
    seeded: bool,
    /// Outer keys seeded so far, across chunks.
    seeded_keys: usize,
    /// Planner estimates of the outer and inner row counts, when stats exist.
    estimates: Option<(f64, f64)>,
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
            buffered: VecDeque::new(),
            buffered_bytes: 0,
            key_set_bytes: 0,
            child_exhausted: false,
            seeded: false,
            seeded_keys: 0,
            estimates: None,
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
            let entry_bytes = key_entry_bytes(positions.len());
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
            if self.seeded {
                self.key_set_bytes += projected.len() * entry_bytes;
            }
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

impl SemijoinOperator {
    /// Planner estimates of the outer side's and the inner body's row counts,
    /// which choose between seeded and unseeded builds.
    pub fn with_estimates(mut self, outer_rows: f64, inner_rows: f64) -> Self {
        self.estimates = Some((outer_rows, inner_rows));
        self
    }

    /// Seed only a conjunction of triples (a seeded solution of it is a
    /// solution of the unseeded body), and only an outer side estimated small
    /// against the body. Without estimates, try: the first chunk decides.
    fn seeds(&self) -> bool {
        self.partial_keys_safe
            && self
                .estimates
                .is_none_or(|(outer, inner)| outer * SEEDED_KEY_COST < inner)
    }

    /// Seeded keys have passed the point where an unseeded build is cheaper:
    /// the outer estimate was low. Without estimates, seed only an outer side
    /// one chunk holds.
    fn seeding_outgrown(&self) -> bool {
        match self.estimates {
            Some((_, inner)) => self.seeded_keys as f64 * SEEDED_KEY_COST >= inner,
            None => !self.child_exhausted,
        }
    }

    /// The current chunk's rows first; once it drains, the next chunk under a
    /// seeded build, otherwise the child's.
    async fn next_child_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.buffered.is_empty() && self.seeded && !self.child_exhausted {
            self.load_chunk(ctx).await?;
        }
        if let Some(batch) = self.buffered.pop_front() {
            let bytes = batch_bytes(&batch).min(self.buffered_bytes);
            ctx.release(bytes);
            self.buffered_bytes -= bytes;
            return Ok(Some(batch));
        }
        if self.child_exhausted || self.seeded {
            return Ok(None);
        }
        self.child.next_batch(ctx).await
    }

    /// Buffer the next chunk of outer rows and seed `key_set` with its keys,
    /// or, once seeding is outgrown, build unseeded and stop chunking.
    async fn load_chunk(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let mut seen: FxHashSet<CompositeGroupKey> = FxHashSet::default();
        let mut seed_rows: Vec<Vec<Binding>> = Vec::new();
        let mut rows = 0usize;
        while seen.len() <= SEEDED_BUILD_MAX_KEYS && rows <= SEEDED_BUILD_MAX_ROWS {
            let Some(batch) = self.child.next_batch(ctx).await? else {
                self.child_exhausted = true;
                break;
            };
            for row_idx in 0..batch.len() {
                // An unbound key seeds as a free variable, so the build holds
                // every inner solution the row could match. A poisoned row
                // keeps its seeded per-row evaluation.
                if self
                    .key_col_indices
                    .iter()
                    .any(|&ci| matches!(batch.get_by_col(row_idx, ci), Binding::Poisoned))
                {
                    continue;
                }
                let key = row_key(&batch, row_idx, &self.key_col_indices, &self.norm);
                if seen.insert(key) {
                    seed_rows.push(
                        self.key_col_indices
                            .iter()
                            .map(|&ci| batch.get_by_col(row_idx, ci).clone())
                            .collect(),
                    );
                }
            }
            rows += batch.len();
            let bytes = batch_bytes(&batch);
            ctx.record_alloc(bytes);
            self.buffered_bytes += bytes;
            ctx.checkpoint()?;
            self.buffered.push_back(batch);
        }
        drop(seen);
        if self.buffered.is_empty() {
            return Ok(());
        }
        self.key_set.clear();
        self.partial_key_sets.clear();
        ctx.release(self.key_set_bytes);
        self.key_set_bytes = 0;
        self.seeded_keys += seed_rows.len();
        if self.seeding_outgrown() {
            self.seeded = false;
            tracing::debug!(seeded_keys = self.seeded_keys, "semijoin build unseeded");
            return self.build(ctx, None).await;
        }
        let schema: Arc<[VarId]> = Arc::from(self.key_vars.clone().into_boxed_slice());
        let columns = (0..self.key_vars.len())
            .map(|col| seed_rows.iter().map(|r| r[col].clone()).collect())
            .collect();
        drop(seed_rows);
        let before = ctx.mem_used();
        self.build(ctx, Some(Batch::new(schema, columns)?)).await?;
        self.key_set_bytes = ctx.mem_used().saturating_sub(before);
        Ok(())
    }

    /// Execute the inner patterns, seeded by `seed` when given, into `key_set`.
    async fn build(&mut self, ctx: &ExecutionContext<'_>, seed: Option<Batch>) -> Result<()> {
        tracing::debug!(
            seeded = seed.is_some(),
            seed_keys = seed.as_ref().map_or(0, Batch::len),
            "semijoin build"
        );
        #[allow(clippy::box_default)]
        let seed: BoxedOperator = match seed {
            Some(batch) => Box::new(BatchSeedOperator::from_batch(batch)),
            None => Box::new(EmptyOperator::new()),
        };
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

        let build_result: Result<()> = async {
            ctx.checkpoint()?;
            let entry_bytes = key_entry_bytes(self.key_vars.len());
            while let Some(batch) = inner_op.next_batch(ctx).await? {
                ctx.checkpoint()?;
                let previous_keys = self.key_set.len();
                for row_idx in 0..batch.len() {
                    let key = row_key(&batch, row_idx, &inner_key_col_indices, &self.norm);
                    self.key_set.insert(key);
                }
                // Charge only new retained keys, not duplicate inner solutions.
                ctx.record_alloc((self.key_set.len() - previous_keys) * entry_bytes);
                ctx.checkpoint()?;
            }
            Ok(())
        }
        .await;
        // Also close the inner plan when its build exceeds the budget.
        inner_op.close();
        build_result
    }
}

/// Query-budget estimate for a buffered outer batch.
fn batch_bytes(batch: &Batch) -> usize {
    batch.len() * batch.schema().len() * crate::context::BINDING_EST_BYTES
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

        self.seeded = self.seeds();
        if self.seeded {
            self.child.open(ctx).await?;
            self.load_chunk(ctx).await?;
        } else {
            // Build before opening the child, so state the child holds once
            // open (a hash table, OPTIONAL buckets) never coexists with it.
            self.build(ctx, None).await?;
            self.child.open(ctx).await?;
        }

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            let input_batch = match self.next_child_batch(ctx).await? {
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
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.key_set_bytes = 0;
        self.state = OperatorState::Closed;
    }

    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        let mut count: u64 = 0;
        loop {
            match self.next_child_batch(ctx).await? {
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
        let snapshot = LedgerSnapshot::genesis("test:main");
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
        let snapshot = LedgerSnapshot::genesis("test:main");
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
        let snapshot = LedgerSnapshot::genesis("test:main");
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
        let snapshot = LedgerSnapshot::genesis("test:main");
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

    /// An outer side delivered in batches of `size` rows, recording the query
    /// memory in use when it is opened.
    struct Batches {
        batches: VecDeque<Batch>,
        schema: Arc<[VarId]>,
        mem_at_open: Arc<std::sync::Mutex<Option<usize>>>,
    }

    impl Batches {
        fn new(rows: Vec<Vec<Binding>>, size: usize) -> Self {
            let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)]);
            Self {
                batches: rows
                    .chunks(size)
                    .map(|chunk| batch(chunk.to_vec()))
                    .collect(),
                schema,
                mem_at_open: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl Operator for Batches {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
        async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
            *self.mem_at_open.lock().unwrap() = Some(ctx.mem_used());
            Ok(())
        }
        async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(self.batches.pop_front())
        }
        fn close(&mut self) {}
    }

    /// A conjunction of triples seeds its build from the outer keys (unbound
    /// keys included), a chunk at a time, unless the outer side is estimated
    /// large against the body or seeded keys outgrow that estimate; another
    /// body shape builds the whole body before the outer side is read.
    #[tokio::test]
    async fn build_is_seeded_per_chunk_for_a_small_outer_side_over_triples() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let rows = |n: u64| {
            (0..n)
                .map(|i| {
                    vec![
                        Binding::encoded_sid(i),
                        Binding::encoded_sid(1),
                        if i % 2 == 0 {
                            Binding::Unbound
                        } else {
                            Binding::encoded_sid(2)
                        },
                    ]
                })
                .collect::<Vec<_>>()
        };
        let values = Pattern::Values {
            vars: vec![VarId(0), VarId(1), VarId(2)],
            rows: vec![],
        };
        let keys = SEEDED_BUILD_MAX_KEYS as u64;
        let few = Some((10.0, 1e6));
        // (outer rows, body, estimates, seeded once open, seeded keys at the end)
        for (n, body, estimates, seeded, seeded_keys) in [
            (keys, triple(), None, true, keys),
            // Without estimates, an outer side past one chunk builds unseeded.
            (3 * keys, triple(), None, false, keys + 256),
            (3 * keys, triple(), few, true, 3 * keys),
            (3 * keys, triple(), Some((1e6, 1e6)), false, 0),
            // Seeding stops once its keys reach 1/16 of the body's estimate.
            (
                3 * keys,
                triple(),
                Some((10.0, 32.0 * keys as f64)),
                true,
                2 * (keys + 256),
            ),
            (2, values, None, false, 0),
        ] {
            let mut op = SemijoinOperator::new(
                Box::new(Batches::new(rows(n), 256)),
                vec![body],
                vec![VarId(0), VarId(1), VarId(2)],
                false,
                None,
                PlanningContext::current(),
            );
            op.estimates = estimates;
            op.open(&ctx).await.unwrap();
            assert_eq!(op.seeded, seeded, "{n} keys, {estimates:?}");
            let mut replayed = 0;
            while let Some(batch) = op.next_child_batch(&ctx).await.unwrap() {
                replayed += batch.len();
            }
            assert_eq!(replayed as u64, n, "every outer row is replayed");
            assert_eq!(
                op.seeded_keys as u64, seeded_keys,
                "{n} keys, {estimates:?}"
            );
            assert_eq!(op.buffered_bytes, 0);
            op.close();
        }
    }

    /// An unseeded build runs before the outer side is opened, so the two
    /// never hold memory at once.
    #[tokio::test]
    async fn unseeded_build_precedes_opening_the_outer_side() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let vars = VarRegistry::new();
        let cancellation = QueryCancellation::new();
        cancellation.set_memory_limit(usize::MAX);
        let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
        let child = Batches::new(
            vec![vec![
                Binding::encoded_sid(1),
                Binding::encoded_sid(2),
                Binding::Unbound,
            ]],
            1,
        );
        let mem_at_open = Arc::clone(&child.mem_at_open);
        let mut op = SemijoinOperator::new(
            Box::new(child),
            vec![Pattern::Values {
                vars: vec![VarId(0), VarId(1)],
                rows: vec![vec![Binding::encoded_sid(1), Binding::encoded_sid(2)]],
            }],
            vec![VarId(0), VarId(1)],
            false,
            None,
            PlanningContext::current(),
        );
        op.open(&ctx).await.unwrap();
        assert!(!op.seeded);
        assert_eq!(*mem_at_open.lock().unwrap(), Some(key_entry_bytes(2)));
        assert_eq!(op.next_batch(&ctx).await.unwrap().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn base_lookup_charges_only_distinct_keys_and_enforces_budget() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let vars = VarRegistry::new();
        let expected_bytes = 2 * key_entry_bytes(2);
        for budget in [expected_bytes, expected_bytes - 1] {
            let cancellation = QueryCancellation::new();
            cancellation.set_memory_limit(budget);
            let ctx = ExecutionContext::new(&snapshot, &vars).with_cancellation(cancellation);
            let child = Box::new(BatchSeedOperator::from_batch(batch(vec![vec![
                Binding::encoded_sid(1),
                Binding::encoded_sid(2),
                Binding::Unbound,
            ]])));
            let mut op = SemijoinOperator::new(
                child,
                vec![Pattern::Values {
                    vars: vec![VarId(0), VarId(1)],
                    rows: vec![
                        vec![Binding::encoded_sid(1), Binding::encoded_sid(2)],
                        vec![Binding::encoded_sid(1), Binding::encoded_sid(2)],
                        vec![Binding::encoded_sid(2), Binding::encoded_sid(3)],
                    ],
                }],
                vec![VarId(0), VarId(1)],
                false,
                None,
                PlanningContext::current(),
            );
            let result = op.open(&ctx).await;
            assert_eq!(ctx.mem_used(), expected_bytes);
            assert_eq!(op.key_set.len(), 2);
            if budget == expected_bytes {
                result.unwrap();
                assert_eq!(op.next_batch(&ctx).await.unwrap().unwrap().len(), 1);
            } else {
                assert!(matches!(
                    result,
                    Err(QueryError::MemoryBudgetExceeded { .. })
                ));
                assert_eq!(op.state, OperatorState::Created);
            }
            op.close();
        }
    }
}
