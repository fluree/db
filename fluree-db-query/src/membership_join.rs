//! MembershipJoinOperator — hash-membership evaluation of a join whose
//! right-side triple binds NOTHING new.
//!
//! When every variable of a join's right triple already appears in the left
//! schema, the "join" is a pure existence filter: over a single graph in
//! current-state mode a fully-ground triple matches at most once, so each
//! left row is kept or dropped. `NestedLoopJoinOperator` evaluates that
//! shape by re-opening a scan per driving row (~ms each — the KB
//! `o:INDEXED` membership check spent ~30 s over 21k rows this way). This
//! operator instead drains the triple ONCE into a hash set of composite
//! keys (same normalization as [`crate::semijoin::SemijoinOperator`]) and
//! probes per row.
//!
//! **The "matches at most once" premise is graph-scoped.** Across a
//! multi-graph active scope the same ground triple can exist in more than
//! one graph, and the nested loop emits one row per matching graph where a
//! hash set collapses them to one — a cardinality change, not just a
//! reordering. Rather than assume what that duplication should be, the
//! operator checks the active graph count at `open()` and routes EVERY row
//! through the exact per-row fallback below when more than one graph is in
//! scope. So the lane is join-equivalent unconditionally, and the hash fast
//! path engages only where the premise provably holds.
//!
//! **Rows with an unbound key var keep exact join semantics** — a join
//! against a partially-ground pattern EXTENDS the row (possibly multiplying
//! it), which a keep/drop semijoin cannot reproduce. Such rows fall back to
//! a per-row seeded evaluation of the triple, emitting one output row per
//! match with the produced binding filled in. The output schema equals the
//! left schema either way (every triple var is already a left column).
//!
//! Chosen by `build_scan_or_join` only when: no object bounds / inline ops,
//! not a history query (ground triples match once per (t, op) there), at
//! least one variable, the triple's standalone cardinality estimate is
//! within [`MEMBERSHIP_JOIN_MAX_BUILD`](crate::execute::where_plan) — the
//! build side is a bounded single-predicate/class scan, never a world scan —
//! and the driving side is at least
//! [`MEMBERSHIP_JOIN_MIN_DRIVING`](crate::execute::where_plan) rows, so the
//! one-shot drain is cheaper than the per-row probes it replaces.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::group_aggregate::{binding_to_group_key_normalized, CompositeGroupKey};
use crate::ir::{Pattern, TriplePattern};
use crate::object_binding::{equality_norm, EqualityNorm};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use rustc_hash::FxHashSet;
use std::sync::Arc;

pub struct MembershipJoinOperator {
    child: BoxedOperator,
    /// The right-side triple (all vars ∈ child schema).
    pattern: TriplePattern,
    /// The triple's variables, in child schema order.
    key_vars: Vec<VarId>,
    /// Output schema (same as child — the join adds no variables).
    schema: Arc<[VarId]>,
    state: OperatorState,
    /// Distinct key tuples from one unseeded drain of `pattern`. Built
    /// lazily on the first fully-bound row, so a stream that never grounds
    /// the pattern (or is empty) never pays for the scan.
    key_set: Option<FxHashSet<CompositeGroupKey>>,
    /// Set at `open()` when more than one graph is in the active scope: the
    /// hash set is never built and every row takes the exact per-row join
    /// (see the multi-graph note in the module docs).
    exact_only: bool,
    planning: PlanningContext,
    norm: Option<EqualityNorm>,
    /// Probe-outcome telemetry for the exhaustion debug line.
    probed_rows: usize,
    kept_rows: usize,
    fallback_rows: usize,
}

impl MembershipJoinOperator {
    pub(crate) fn new(
        child: BoxedOperator,
        pattern: TriplePattern,
        key_vars: Vec<VarId>,
        planning: PlanningContext,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            pattern,
            key_vars,
            schema,
            state: OperatorState::Created,
            key_set: None,
            exact_only: false,
            planning,
            norm: None,
            probed_rows: 0,
            kept_rows: 0,
            fallback_rows: 0,
        }
    }

    /// Key lookups go through the batch's OWN schema (`Batch::get`), never
    /// positional indices precomputed from the child's declared schema: an
    /// operator like `DefaultGraphSourceOperator`'s single-graph delegate
    /// emits batches whose column order comes from the reordered inner
    /// chain, which need not match the declared schema order.
    fn all_keys_bound(&self, batch: &Batch, row_idx: usize) -> bool {
        self.key_vars.iter().all(|v| {
            !matches!(
                batch.get(row_idx, *v),
                None | Some(Binding::Unbound | Binding::Poisoned)
            )
        })
    }

    fn extract_key(&self, batch: &Batch, row_idx: usize) -> CompositeGroupKey {
        let keys = self
            .key_vars
            .iter()
            .map(|v| {
                let (store, gv) = EqualityNorm::parts(&self.norm);
                let binding = batch.get(row_idx, *v).unwrap_or(&Binding::Unbound);
                binding_to_group_key_normalized(binding, store, gv)
            })
            .collect();
        CompositeGroupKey(keys)
    }

    /// Drain the triple once (unseeded planned scan — overlay-merged and
    /// policy-filtered) into the key set.
    async fn build_key_set(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let key_var_slice: Vec<VarId> = self.key_vars.clone();
        #[allow(clippy::box_default)]
        let seed: BoxedOperator = Box::new(EmptyOperator::new());
        let mut inner = build_where_operators_seeded(
            Some(seed),
            std::slice::from_ref(&Pattern::Triple(self.pattern.clone())),
            None,
            Some(&key_var_slice),
            &self.planning,
        )?;
        let mut set = FxHashSet::default();
        inner.open(ctx).await?;
        while let Some(batch) = inner.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            for row_idx in 0..batch.len() {
                let key = self
                    .key_vars
                    .iter()
                    .map(|v| {
                        let (store, gv) = EqualityNorm::parts(&self.norm);
                        let binding = batch.get(row_idx, *v).unwrap_or(&Binding::Unbound);
                        binding_to_group_key_normalized(binding, store, gv)
                    })
                    .collect();
                set.insert(CompositeGroupKey(key));
            }
        }
        inner.close();
        tracing::debug!(
            keys = set.len(),
            pattern = ?self.pattern,
            "membership join key set built"
        );
        self.key_set = Some(set);
        Ok(())
    }

    /// Exact join fallback for a row that does NOT fully ground the
    /// pattern: evaluate the triple seeded with the row and emit one
    /// output row per match, with produced bindings filled in.
    async fn per_row_join(
        &self,
        ctx: &ExecutionContext<'_>,
        input_batch: &Batch,
        row_idx: usize,
        columns: &mut [Vec<Binding>],
    ) -> Result<()> {
        let seed = SeedOperator::from_batch_row(input_batch, row_idx);
        let mut inner = build_where_operators_seeded(
            Some(Box::new(seed)),
            std::slice::from_ref(&Pattern::Triple(self.pattern.clone())),
            None,
            None,
            &self.planning,
        )?;
        inner.open(ctx).await?;
        while let Some(batch) = inner.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            for r in 0..batch.len() {
                for (col_idx, var) in self.schema.iter().enumerate() {
                    let binding = batch.get(r, *var).cloned().unwrap_or_else(|| {
                        input_batch
                            .get(row_idx, *var)
                            .cloned()
                            .unwrap_or(Binding::Unbound)
                    });
                    columns[col_idx].push(binding);
                }
            }
        }
        inner.close();
        Ok(())
    }
}

#[async_trait]
impl Operator for MembershipJoinOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.norm.is_none() {
            self.norm = equality_norm(ctx);
        }
        // Keep/drop is only join-equivalent over a single graph; with more
        // than one in scope a ground triple can match once per graph.
        self.exact_only = match ctx.active_graphs() {
            crate::dataset::ActiveGraphs::Single => false,
            crate::dataset::ActiveGraphs::Many(graphs) => graphs.len() > 1,
        };
        self.child.open(ctx).await?;
        self.key_set = None;
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
                    tracing::debug!(
                        pattern = ?self.pattern,
                        probed = self.probed_rows,
                        kept = self.kept_rows,
                        fallback = self.fallback_rows,
                        "membership join exhausted"
                    );
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            let mut columns: Vec<Vec<Binding>> =
                (0..self.schema.len()).map(|_| Vec::new()).collect();
            for row_idx in 0..input_batch.len() {
                if !self.exact_only && self.all_keys_bound(&input_batch, row_idx) {
                    if self.key_set.is_none() {
                        self.build_key_set(ctx).await?;
                    }
                    self.probed_rows += 1;
                    let key = self.extract_key(&input_batch, row_idx);
                    let keep = self
                        .key_set
                        .as_ref()
                        .expect("key set built above")
                        .contains(&key);
                    if keep {
                        self.kept_rows += 1;
                        for (col_idx, var) in self.schema.iter().enumerate() {
                            let binding = input_batch
                                .get(row_idx, *var)
                                .cloned()
                                .unwrap_or(Binding::Unbound);
                            columns[col_idx].push(binding);
                        }
                    }
                } else {
                    self.fallback_rows += 1;
                    self.per_row_join(ctx, &input_batch, row_idx, &mut columns)
                        .await?;
                }
            }

            if columns.first().is_none_or(std::vec::Vec::is_empty) {
                continue;
            }
            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.key_set = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // A filter: at most the child's cardinality (ground rows match ≤ once).
        self.child.estimated_rows()
    }
}
