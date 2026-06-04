//! `HashJoinOperator` — build/probe inner join for object→subject "path" joins.
//!
//! This is the cure for the BSBM-BI "small + large" two-pattern join slowdown
//! (see `docs/troubleshooting/performance-tracing.md` and the benchmark report
//! `bsbm-bi-fluree-100m-join-scaling.md`). The minimal repro:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?c) WHERE {
//!   ?review  rev:reviewer ?reviewer .            # LARGE: full predicate scan
//!   ?reviewer bsbm:country <US> .                 # SMALL: selective bound object
//! }
//! ```
//!
//! The planner correctly drives from the selective bound-object (country) side,
//! which makes the large `rev:reviewer` pattern a right scan whose **object** is
//! bound from the left. The default `NestedLoopJoinOperator` then resolves it via
//! the batched-OBJECT path (`flush_batched_object_accumulator_binary`), seeking the
//! **object-major global OPST index** once per distinct driving object. Because a
//! single predicate's triples are scattered across the whole OPST key space, that
//! degrades superlinearly (≈47 s at 100M for ~61.8K driving objects).
//!
//! `HashJoinOperator` instead:
//!   1. **Build** a hash table from the small (driving) side, keyed by the join var.
//!   2. **Probe** by scanning the large predicate's *contiguous* PSOT/POST partition
//!      exactly once and looking each row's object up in the table.
//!
//! This turns N scattered object seeks into one sequential predicate scan + hash
//! probes (the large `rev:reviewer` scan alone is ~75 ms at 100M), which is what
//! cardinality-aware engines do for this shape.
//!
//! ## Correctness of the join key
//!
//! Ref bindings can appear as late-materialised `EncodedSid` *or* materialised `Sid`
//! for the *same* entity, and `binding_to_group_key_owned` hashes those to different
//! keys. To make build- and probe-side keys comparable we normalise every resolvable
//! ref to its `u64` subject id via the binary store (mirroring the batched-object
//! path in `join.rs`); unresolvable/non-ref values fall back to the group key, which
//! is consistent across sides in store-less (memory) mode.
//!
//! ## Selection
//!
//! [`HashJoinPlanner`] (below) chooses this operator with a cost model: the shape
//! must match (single shared var = the bound object, fixed predicate, new subject
//! var, no object bounds/inline ops) AND the probe predicate must be large enough
//! that scattered seeks dominate. `FLUREE_HASH_JOIN` is a force-override only
//! (`On`/`Off`). `where_plan` threads one planner through a join block; at `open()`,
//! if the query is a true multi-graph dataset (ledger-local key normalisation would
//! be wrong), the operator falls back to a `NestedLoopJoinOperator` over the same
//! inputs.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use rustc_hash::FxHashMap;

use fluree_db_core::{ObjectBounds, StatsView};

use crate::binary_scan::EmitMask;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::group_aggregate::{binding_to_group_key_owned, GroupKeyOwned};
use crate::ir::triple::TriplePattern;
use crate::join::NestedLoopJoinOperator;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;

/// Target number of output rows per produced batch.
const OUTPUT_BATCH_TARGET: usize = 4096;

// ── Planning: when to use the hash join ──────────────────────────────────────
//
// `HashJoinPlanner` is threaded through a WHERE join block by `where_plan`. It owns
// the force mode, the `StatsView`, and the running cardinality of the driving chain,
// and decides per pattern whether the object→subject join should use this operator.

/// Force-override for the cost-based object→subject hash-join decision.
///
/// `FLUREE_HASH_JOIN`: unset/other => `Auto` (the cost model decides), `1`/`true`
/// => `On` (force the hash join whenever the shape matches, ignoring cost — keeps
/// force-on usable on stats-less ledgers), `0`/`false` => `Off` (never hash-join).
#[derive(Clone, Copy, PartialEq, Eq)]
enum HashJoinForce {
    Auto,
    On,
    Off,
}

fn hash_join_force() -> HashJoinForce {
    match std::env::var("FLUREE_HASH_JOIN") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => HashJoinForce::On,
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("false") => HashJoinForce::Off,
        _ => HashJoinForce::Auto,
    }
}

/// Probe-predicate floor: below this, scattered OPST object seeks stay cheap and the
/// hash join is a wash (the BSBM crossover is between 55.7k @1M and 560k @10M).
const HASH_JOIN_PROBE_MIN: u64 = 250_000;
/// Don't scan a probe predicate more than this many times the driving-set size —
/// a guard against the pathological "scan a huge predicate for a handful of driving
/// rows" case. It is deliberately loose: the alternative we replace is the scattered
/// OPST object seek, measured at ~760 µs/seek (47 s / 61.8K driving rows at 100M)
/// versus ~26 ns per sequential probe row, so the *true* break-even ratio is ≈29,000×.
/// We sit far below that (room for the build-side hash + memory), but well above the
/// estimate error from skewed objects: `driving_est` for a bound object is the AVERAGE
/// object size (`count/ndv`), which undershoots a popular value like BSBM `country=US`
/// (avg ~28K vs actual ~61.8K), so a tight cap (the old 64×) wrongly rejected the very
/// join this operator exists for. See the BI-1 case in `hash_join_cost_wins` tests.
const HASH_JOIN_MAX_SCAN_RATIO: f64 = 1024.0;

/// The hash join wins when the probe predicate is large enough that scattered OPST
/// seeks dominate, but not so large relative to the driving set that the single
/// contiguous scan is wasteful. `false` when probe stats are absent (=> the safe
/// `NestedLoopJoinOperator` default). Constants tuned to BSBM 1M/10M/100M; heuristic.
fn hash_join_cost_wins(probe_count: Option<u64>, driving_est: Option<f64>) -> bool {
    let Some(pc) = probe_count else { return false };
    let drive = driving_est.unwrap_or(0.0).max(1.0);
    pc >= HASH_JOIN_PROBE_MIN && (pc as f64) <= drive * HASH_JOIN_MAX_SCAN_RATIO
}

/// Probe-predicate row count from stats, keyed by SID or — for the un-encoded IRI
/// predicates that non-reasoning queries carry — by IRI. Mirrors the planner's
/// `property_stats` fallback so the cost model sees the same numbers reorder did.
fn predicate_count(stats: &StatsView, pred: &crate::ir::triple::Ref) -> Option<u64> {
    if let Some(sid) = pred.as_sid() {
        return stats.get_property(sid).map(|p| p.count);
    }
    if let Some(iri) = pred.as_iri() {
        return stats.get_property_by_iri(iri).map(|p| p.count);
    }
    None
}

/// Eligibility for the object→subject hash join. The right pattern `tp` must be a
/// single fixed-predicate scan whose OBJECT is the (only) variable shared with the
/// left side (the join key), whose SUBJECT is a new variable, with no object bounds,
/// no datatype constraint, and no inline ops. Returns the shared join variable.
///
/// This is exactly the shape that `NestedLoopJoinOperator` would otherwise run via
/// the batched-OBJECT (OPST) path — the slow case the hash join replaces.
fn hash_join_object_join_var(
    left_schema: &[VarId],
    tp: &TriplePattern,
    has_bounds: bool,
    inline_ops_empty: bool,
) -> Option<VarId> {
    if has_bounds || !inline_ops_empty || tp.dtc.is_some() {
        return None;
    }
    // Predicate must be fixed — a SID or an IRI — so the probe scans one contiguous
    // partition. Non-reasoning SPARQL/JSON-LD queries reach planning with IRI
    // predicates (only reasoning queries are pre-encoded to SIDs in runner.rs), so a
    // SID-only check here silently excluded the exact BSBM joins this operator targets.
    if tp.p.as_var().is_some() {
        return None;
    }
    // Object must be a var already bound from the left (the join key).
    let o_var = tp.o.as_var()?;
    if !left_schema.contains(&o_var) {
        return None;
    }
    // Subject must be a brand-new var (not shared with the left).
    let s_var = tp.s.as_var()?;
    if left_schema.contains(&s_var) {
        return None;
    }
    Some(o_var)
}

/// Per-WHERE-block planning state for the object→subject hash join.
///
/// One instance is threaded through a join block so `build_triple_operators` and
/// `build_sequential_join_block` share the same cost state — the running driving-set
/// cardinality, the resolved force mode, and stats — instead of each re-deriving it
/// and widening every helper signature.
pub(crate) struct HashJoinPlanner<'a> {
    stats: Option<&'a StatsView>,
    force: HashJoinForce,
    /// Running product of per-pattern estimates for the chain built so far.
    driving_est: f64,
    /// `driving_est` snapshot from the most recent [`before_step`](Self::before_step)
    /// — the driving-set size the next probe is weighed against. `None` until
    /// `before_step` runs with stats present, so single-pattern / stats-less callers
    /// never auto-fire (force-`On` still does).
    step_est: Option<f64>,
}

impl<'a> HashJoinPlanner<'a> {
    pub(crate) fn new(stats: Option<&'a StatsView>) -> Self {
        Self {
            stats,
            force: hash_join_force(),
            driving_est: 1.0,
            step_est: None,
        }
    }

    /// Advance the running driving cardinality past one pattern. Call once per
    /// pattern, in chain order, BEFORE building its operator. Snapshots the product
    /// of the patterns to the LEFT of `tp` (this becomes `step_est`, the size the
    /// probe is weighed against), then folds `tp`'s own estimate in for later steps.
    /// The snapshot excludes `tp` on purpose: as a probe its object is bound from the
    /// left, so estimating it would misclassify as a selective BoundObject scan.
    /// `bound` is the set of variables bound by the chain to the left of `tp`.
    pub(crate) fn before_step(&mut self, tp: &TriplePattern, bound: &HashSet<VarId>) {
        self.step_est = self.stats.map(|stats| {
            let snapshot = self.driving_est;
            self.driving_est *= crate::planner::estimate_triple_row_count(tp, bound, Some(stats));
            snapshot
        });
    }

    /// Decide whether the join for the current pattern should use the object→subject
    /// hash join, returning the shared join variable when it should (and `None` to
    /// keep the `NestedLoopJoinOperator` default). Honors the force mode; under `Auto`
    /// it weighs the probe predicate's size against the latest `before_step` snapshot.
    pub(crate) fn choose_object_hash_join(
        &self,
        left_schema: &[VarId],
        tp: &TriplePattern,
        has_bounds: bool,
        inline_ops_empty: bool,
    ) -> Option<VarId> {
        if self.force == HashJoinForce::Off {
            return None;
        }
        let join_var = hash_join_object_join_var(left_schema, tp, has_bounds, inline_ops_empty)?;
        let use_hash = match self.force {
            HashJoinForce::On => true,
            HashJoinForce::Off => false,
            HashJoinForce::Auto => {
                let probe_count = self.stats.and_then(|s| predicate_count(s, &tp.p));
                hash_join_cost_wins(probe_count, self.step_est)
            }
        };
        use_hash.then_some(join_var)
    }
}

/// A join key that is comparable across build/probe sides regardless of whether a
/// ref was delivered as `EncodedSid` or materialised `Sid`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKey {
    /// A resolved subject id (ledger-local; valid because the operator is
    /// single-store / native).
    Ref(u64),
    /// Fallback for literals or refs that could not be resolved to a subject id.
    Other(GroupKeyOwned),
}

/// Build a comparable join key for a binding, normalising refs to a `u64` s_id when
/// a store is available. Returns `None` for Unbound/Poisoned (cannot participate in
/// an inner join).
fn join_key(binding: &Binding, store: Option<&BinaryIndexStore>) -> Option<JoinKey> {
    match binding {
        Binding::EncodedSid { s_id, .. } => Some(JoinKey::Ref(*s_id)),
        Binding::Sid { sid, .. } => Some(
            store
                .and_then(|s| {
                    s.find_subject_id_by_parts(sid.namespace_code, &sid.name)
                        .ok()
                        .flatten()
                })
                .map(JoinKey::Ref)
                .unwrap_or_else(|| JoinKey::Other(binding_to_group_key_owned(binding))),
        ),
        Binding::IriMatch { primary_sid, .. } => Some(
            store
                .and_then(|s| {
                    s.find_subject_id_by_parts(primary_sid.namespace_code, &primary_sid.name)
                        .ok()
                        .flatten()
                })
                .map(JoinKey::Ref)
                .unwrap_or_else(|| JoinKey::Other(binding_to_group_key_owned(binding))),
        ),
        Binding::Unbound | Binding::Poisoned => None,
        other => Some(JoinKey::Other(binding_to_group_key_owned(other))),
    }
}

/// Inner hash join: build a table from `build` (the small side), probe with `probe`
/// (a contiguous scan of the large predicate), joining on a single shared variable.
pub struct HashJoinOperator {
    /// Small (driving) side. Consumed in `open()` — drained into the hash table on
    /// the hash path, or handed to the nested-loop on the multi-graph fallback path.
    build: Option<BoxedOperator>,
    /// Large side — streamed once during `next_batch()` / `drain_count()`. Dropped
    /// unused when the fallback path is taken.
    probe: Option<BoxedOperator>,
    /// Probe pattern + params, kept so `open()` can build a `NestedLoopJoinOperator`
    /// fallback when the hash path is not safe (true multi-graph dataset, where the
    /// ledger-local key normalisation would be wrong).
    right_pattern: TriplePattern,
    nl_bounds: Option<ObjectBounds>,
    nl_mode: TemporalMode,
    nl_downstream: Option<Arc<[VarId]>>,
    /// Set in `open()` when the hash path is unsafe; all output is delegated to it.
    fallback: Option<BoxedOperator>,
    /// `build.schema()` captured at construction (column order of build rows).
    build_schema: Arc<[VarId]>,
    /// Full output schema: `build_schema` ++ probe vars not already in build_schema.
    full_schema: Arc<[VarId]>,
    /// Trimmed output schema when downstream only needs a subset.
    out_schema: Option<Arc<[VarId]>>,
    /// Column of the join var within `build_schema`.
    build_key_col: usize,
    /// Column of the join var within `probe.schema()`.
    probe_key_col: usize,
    /// Columns of `probe.schema()` appended to the output (probe vars not in build).
    probe_emit_cols: Vec<usize>,
    /// Hash table: join key → all build rows with that key (each row aligned to
    /// `build_schema`). Multiplicity is preserved for correct COUNT semantics.
    table: FxHashMap<JoinKey, Vec<Vec<Binding>>>,
    /// Build rows whose join key is Unbound/Poisoned. The nested-loop join treats an
    /// unbound shared var as unconstrained — the right side fills it — so these rows
    /// must match EVERY probe row (with the join var taking the probe value), not be
    /// dropped. Empty in the common case (join var always bound), so zero overhead.
    wildcard_rows: Vec<Vec<Binding>>,
    /// Current probe batch being consumed and the next row to process.
    cur_probe: Option<Batch>,
    cur_probe_row: usize,
    state: OperatorState,
}

impl HashJoinOperator {
    /// Construct from a build (small) side, a probe (large) scan, and the single
    /// shared join variable. `downstream_vars` trims the output when provided.
    ///
    /// Caller (`build_scan_or_join`) guarantees `join_var` is present in both
    /// schemas via its eligibility check, so column lookups cannot fail.
    pub fn new(
        build: BoxedOperator,
        probe: BoxedOperator,
        join_var: VarId,
        downstream_vars: Option<&[VarId]>,
        right_pattern: TriplePattern,
        nl_bounds: Option<ObjectBounds>,
        nl_mode: TemporalMode,
    ) -> Self {
        let build_schema: Arc<[VarId]> = Arc::from(build.schema().to_vec().into_boxed_slice());
        let probe_schema: Vec<VarId> = probe.schema().to_vec();

        let build_key_col = build_schema
            .iter()
            .position(|v| *v == join_var)
            .expect("hash join: join var must be in build schema");
        let probe_key_col = probe_schema
            .iter()
            .position(|v| *v == join_var)
            .expect("hash join: join var must be in probe schema");

        // Output = build columns, then probe vars not already produced by build.
        let mut full = build_schema.to_vec();
        let mut probe_emit_cols = Vec::new();
        for (i, v) in probe_schema.iter().enumerate() {
            if !build_schema.contains(v) {
                probe_emit_cols.push(i);
                full.push(*v);
            }
        }
        let full_schema: Arc<[VarId]> = Arc::from(full.into_boxed_slice());
        let out_schema = compute_trimmed_vars(&full_schema, downstream_vars);
        let nl_downstream: Option<Arc<[VarId]>> =
            downstream_vars.map(|d| Arc::from(d.to_vec().into_boxed_slice()));

        Self {
            build: Some(build),
            probe: Some(probe),
            right_pattern,
            nl_bounds,
            nl_mode,
            nl_downstream,
            fallback: None,
            build_schema,
            full_schema,
            out_schema,
            build_key_col,
            probe_key_col,
            probe_emit_cols,
            table: FxHashMap::default(),
            wildcard_rows: Vec::new(),
            cur_probe: None,
            cur_probe_row: 0,
            state: OperatorState::Created,
        }
    }

    /// Drain `build` into the hash table. Called once in `open()` on the hash path.
    async fn build_table(
        &mut self,
        ctx: &ExecutionContext<'_>,
        mut build: BoxedOperator,
    ) -> Result<()> {
        let store = ctx.binary_store.as_deref();
        let ncols = self.build_schema.len();
        build.open(ctx).await?;
        while let Some(batch) = build.next_batch(ctx).await? {
            for row in 0..batch.len() {
                let row_vals: Vec<Binding> = (0..ncols)
                    .map(|c| batch.get_by_col(row, c).clone())
                    .collect();
                match join_key(batch.get_by_col(row, self.build_key_col), store) {
                    Some(key) => self.table.entry(key).or_default().push(row_vals),
                    // Unbound/Poisoned join var: unconstrained, matches every probe row.
                    None => self.wildcard_rows.push(row_vals),
                }
            }
        }
        build.close();
        Ok(())
    }
}

#[async_trait]
impl Operator for HashJoinOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        let mut v = Vec::new();
        if let Some(b) = self.build.as_deref() {
            v.push(crate::plan_node::PlanChild::child(b));
        }
        if let Some(p) = self.probe.as_deref() {
            v.push(crate::plan_node::PlanChild::child(p));
        }
        v
    }
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert(
            "probe".into(),
            crate::explain::format_pattern(&self.right_pattern).into(),
        );
        m
    }
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.full_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "HashJoinOperator::open() called in invalid state".into(),
            ));
        }
        let build = self.build.take().expect("hash join build side");
        // Join keys normalise refs to ledger-local subject ids, which would collide
        // across ledgers in a true multi-graph dataset (Many with >1 graph). Rather
        // than error (the cost planner can auto-select us), fall back to a nested-loop
        // join over the same driving side + probe pattern, which is graph-correct.
        let multi_graph =
            matches!(ctx.active_graphs(), ActiveGraphs::Many(graphs) if graphs.len() > 1);
        if multi_graph {
            let mut nl = NestedLoopJoinOperator::new(
                build,
                Arc::clone(&self.build_schema),
                self.right_pattern.clone(),
                self.nl_bounds.clone(),
                Vec::new(),
                EmitMask::ALL,
                self.nl_mode,
            )
            .with_out_schema(self.nl_downstream.as_deref());
            nl.open(ctx).await?;
            self.fallback = Some(Box::new(nl));
        } else {
            self.build_table(ctx, build).await?;
            self.probe
                .as_mut()
                .expect("hash join probe")
                .open(ctx)
                .await?;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if let Some(fallback) = self.fallback.as_mut() {
            return fallback.next_batch(ctx).await;
        }
        let store = ctx.binary_store.as_deref();
        let ncols = self.full_schema.len();
        let build_cols = self.build_schema.len();
        let probe = self.probe.as_mut().expect("hash join probe");

        loop {
            // Ensure we have a probe batch to consume.
            if self.cur_probe.is_none() {
                match probe.next_batch(ctx).await? {
                    Some(b) if !b.is_empty() => {
                        self.cur_probe = Some(b);
                        self.cur_probe_row = 0;
                    }
                    Some(_) => continue,
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            let pb = self.cur_probe.as_ref().unwrap();
            let mut out_cols: Vec<Vec<Binding>> = (0..ncols).map(|_| Vec::new()).collect();
            let mut produced = 0usize;

            while self.cur_probe_row < pb.len() {
                let row = self.cur_probe_row;
                self.cur_probe_row += 1;

                let Some(key) = join_key(pb.get_by_col(row, self.probe_key_col), store) else {
                    continue;
                };
                if let Some(matches) = self.table.get(&key) {
                    for build_row in matches {
                        for (c, b) in build_row.iter().enumerate() {
                            out_cols[c].push(b.clone());
                        }
                        for (i, &pc) in self.probe_emit_cols.iter().enumerate() {
                            out_cols[build_cols + i].push(pb.get_by_col(row, pc).clone());
                        }
                        produced += 1;
                    }
                }
                // Unbound-key build rows match every probe row; the join var takes the
                // probe value (the nested-loop "take the right side" semantics).
                if !self.wildcard_rows.is_empty() {
                    let probe_key = pb.get_by_col(row, self.probe_key_col);
                    for wild in &self.wildcard_rows {
                        for (c, b) in wild.iter().enumerate() {
                            out_cols[c].push(if c == self.build_key_col {
                                probe_key.clone()
                            } else {
                                b.clone()
                            });
                        }
                        for (i, &pc) in self.probe_emit_cols.iter().enumerate() {
                            out_cols[build_cols + i].push(pb.get_by_col(row, pc).clone());
                        }
                        produced += 1;
                    }
                }
                if produced >= OUTPUT_BATCH_TARGET {
                    break;
                }
            }

            if self.cur_probe_row >= pb.len() {
                self.cur_probe = None;
            }
            if produced == 0 {
                continue;
            }
            let batch = Batch::new(Arc::clone(&self.full_schema), out_cols)
                .map_err(|e| QueryError::Internal(format!("hash join batch: {e}")))?;
            return Ok(trim_batch(&self.out_schema, batch));
        }
    }

    fn close(&mut self) {
        if let Some(b) = self.build.as_mut() {
            b.close();
        }
        if let Some(p) = self.probe.as_mut() {
            p.close();
        }
        if let Some(f) = self.fallback.as_mut() {
            f.close();
        }
        self.table.clear();
        self.wildcard_rows.clear();
        self.cur_probe = None;
        self.state = OperatorState::Closed;
    }

    /// COUNT(*) fast path: stream the probe side and sum build-side multiplicity per
    /// matching key, without materialising any output bindings.
    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if let Some(fallback) = self.fallback.as_mut() {
            return fallback.drain_count(ctx).await;
        }
        let store = ctx.binary_store.as_deref();
        let probe = self.probe.as_mut().expect("hash join probe");
        let mut count: u64 = 0;
        loop {
            match probe.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => {
                    for row in 0..batch.len() {
                        let Some(key) = join_key(batch.get_by_col(row, self.probe_key_col), store)
                        else {
                            continue;
                        };
                        // Each probe row matches its keyed build rows plus every
                        // wildcard (unbound-key) build row.
                        let matched =
                            self.table.get(&key).map_or(0, Vec::len) + self.wildcard_rows.len();
                        count = count.checked_add(matched as u64).ok_or_else(|| {
                            QueryError::execution("COUNT(*) overflow in hash join drain_count")
                        })?;
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
        // Output is bounded by the probe side's matching subset; use the probe
        // estimate as a coarse upper bound for downstream sizing.
        if let Some(f) = self.fallback.as_ref() {
            return f.estimated_rows();
        }
        self.probe.as_ref().and_then(|p| p.estimated_rows())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term};

    #[test]
    fn iri_predicate_is_eligible_like_a_sid() {
        // Non-reasoning SPARQL keeps predicates as IRIs (not pre-encoded to SIDs).
        // The probe `?review <rev:reviewer> ?reviewer` with ?reviewer bound from the
        // left must be eligible — a SID-only check here was why the BSBM BI-1 join
        // never reached the cost gate.
        let review = VarId(0);
        let reviewer = VarId(1);
        let left_schema = [reviewer];
        let iri_tp = TriplePattern::new(
            Ref::Var(review),
            Ref::Iri(Arc::from("http://purl.org/stuff/rev#reviewer")),
            Term::Var(reviewer),
        );
        assert_eq!(
            hash_join_object_join_var(&left_schema, &iri_tp, false, true),
            Some(reviewer),
            "IRI-predicate probe must be eligible for the object→subject hash join"
        );

        // A variable predicate is still rejected (can't scan one contiguous partition).
        let var_pred_tp =
            TriplePattern::new(Ref::Var(review), Ref::Var(VarId(2)), Term::Var(reviewer));
        assert_eq!(
            hash_join_object_join_var(&left_schema, &var_pred_tp, false, true),
            None
        );
    }

    #[test]
    fn cost_gate_fires_for_skewed_bound_object_bi1() {
        // BSBM BI-1: probe rev:reviewer (2.85M rows) vs a driving `country=US` set.
        // The cost model estimates the driving side as the AVERAGE country size
        // (count/ndv ≈ 28k), not US's actual 61.8k. The 1024× cap must accept this
        // (28k×1024 = 28.6M ≥ 2.85M); the old 64× cap rejected it (28k×64 = 1.79M <
        // 2.85M), which is the regression this fixes.
        assert!(hash_join_cost_wins(Some(2_848_260), Some(28_000.0)));
        // Also passes against US's true (un-averaged) selectivity.
        assert!(hash_join_cost_wins(Some(2_848_260), Some(61_847.0)));
    }

    #[test]
    fn cost_gate_still_guards_pathological_and_small_probes() {
        // Huge probe for a handful of driving rows is still rejected (ratio ≫ 1024×).
        assert!(!hash_join_cost_wins(Some(2_848_260), Some(100.0)));
        // Probe below the floor never qualifies, regardless of ratio.
        assert!(!hash_join_cost_wins(Some(100_000), Some(10.0)));
        // No probe stats => fall back to the safe nested-loop default.
        assert!(!hash_join_cost_wins(None, Some(1_000_000.0)));
    }
}
