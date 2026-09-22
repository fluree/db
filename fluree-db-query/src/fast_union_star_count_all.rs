//! Fast-path: `COUNT(*)` for UNION-of-triples optionally constrained by same-subject star joins.
//!
//! Targets benchmark-style queries like:
//!
//! 1) UNION + self-loop filter:
//! ```sparql
//! SELECT (COUNT(*) AS ?count) WHERE {
//!   { ?s p1 ?o } UNION { ?s p2 ?o }
//!   FILTER (?s = ?o)
//! }
//! ```
//! Bag semantics: duplicates across branches are counted twice.
//! Answer = count_{p1}(s=o) + count_{p2}(s=o)
//!
//! 2) UNION + additional same-subject predicate(s):
//! ```sparql
//! SELECT (COUNT(*) AS ?count) WHERE {
//!   { ?s p1 ?o1 } UNION { ?s p2 ?o1 }
//!   ?s p3 ?o2
//! }
//! ```
//! Answer = Σ_s (count_{p1}(s)+count_{p2}(s)) * count_{p3}(s)
//! (and generalizes to multiple `p3`-like predicates as a product).
//!
//! This operator avoids materializing UNION results and avoids downstream joins by working with
//! per-subject multiplicity streams from PSOT.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, build_overlay_cursor_for_subject_range, build_psot_cursor_for_predicate,
    cached_overlay_ops, count_predicate_overlay_delta, count_rows_for_predicate_psot, count_to_i64,
    cursor_projection_sid_only, cursor_projection_sid_otype_okey, leaf_entries_for_predicate,
    normalize_pred_sid, slice_overlay_ops_by_subject, CursorSubjectCountStream, GroupStream,
    InnerMergeHeads, PsotSubjectCountIter, SharedOverlayOps, UnionMergeHeads,
};
use crate::ir::triple::Ref;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::{BinaryCursor, RunSortOrder};
use fluree_db_core::o_type::OType;
use fluree_db_core::QueryCancellation;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnionCountMode {
    /// Count all rows for each union predicate.
    AllRows,
    /// Count only rows where `?s = ?o` (ref-only self-loops).
    SubjectEqObject,
}

pub struct UnionStarCountAllOperator {
    union_preds: Vec<Ref>,
    extra_preds: Vec<Ref>,
    mode: UnionCountMode,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    emitted: bool,
    result: Option<i64>,
}

impl UnionStarCountAllOperator {
    pub fn new(
        union_preds: Vec<Ref>,
        extra_preds: Vec<Ref>,
        mode: UnionCountMode,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            union_preds,
            extra_preds,
            mode,
            out_var,
            state: OperatorState::Created,
            fallback,
            emitted: false,
            result: None,
        }
    }
}

#[async_trait]
impl Operator for UnionStarCountAllOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|fb| vec![crate::plan_node::PlanChild::fallback(fb)])
            .unwrap_or_default()
    }
    fn schema(&self) -> &[VarId] {
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // History mode is filtered at the planner — see
        // `execute::operator_tree::build_operator_tree_inner` — so this gate
        // doesn't duplicate that check.
        let allow_fast =
            !ctx.is_multi_ledger() && !ctx.is_history_range() && ctx.allow_unfiltered();
        if allow_fast {
            if let Some(store) = ctx.binary_store.as_ref() {
                let started = fluree_db_core::clock::Instant::now();
                let Some(n) = count_union_star(
                    store,
                    ctx,
                    ctx.binary_g_id,
                    &self.union_preds,
                    &self.extra_preds,
                    self.mode,
                )?
                else {
                    // Fast-path unavailable under this execution context (e.g., overlay requires fallback).
                    // Fall through to the provided fallback operator.
                    let Some(fallback) = &mut self.fallback else {
                        return Err(QueryError::Internal(
                            "UNION-star COUNT(*) fast-path unavailable and no fallback provided"
                                .into(),
                        ));
                    };
                    fallback.open(ctx).await?;
                    self.state = OperatorState::Open;
                    return Ok(());
                };
                tracing::debug!(
                    count = n,
                    mode = ?self.mode,
                    elapsed_us = started.elapsed().as_micros() as u64,
                    "union-star count fast path executed"
                );
                self.result = Some(count_to_i64(n, "COUNT(*) UNION-star")?);
                self.emitted = false;
                self.state = OperatorState::Open;
                self.fallback = None;
                return Ok(());
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "UNION-star COUNT(*) fast-path unavailable and no fallback provided".into(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if let Some(fb) = &mut self.fallback {
            return fb.next_batch(ctx).await;
        }

        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        let n = self.result.unwrap_or(0);
        let b = build_count_batch(self.out_var, n)?;
        self.emitted = true;
        Ok(Some(b))
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
        self.emitted = false;
        self.result = None;
    }
}

/// Stream of `(s_id, count_self_loops)` for a predicate, where self-loop means ref-only `s_id == o_key`.
struct SubjectSelfLoopCountStreamV6 {
    cursor: BinaryCursor,
    current: Option<fluree_db_binary_index::ColumnBatch>,
    row: usize,
    cur_s: Option<u64>,
    cur_count: u64,
    iri_ref: u16,
    bnode: u16,
}

impl SubjectSelfLoopCountStreamV6 {
    fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
            cur_s: None,
            cur_count: 0,
            iri_ref: OType::IRI_REF.as_u16(),
            bnode: OType::BLANK_NODE.as_u16(),
        }
    }

    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.current.is_none() {
                self.current = self
                    .cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                self.row = 0;
                if self.current.is_none() {
                    if let Some(s) = self.cur_s.take() {
                        let n = std::mem::take(&mut self.cur_count);
                        if n > 0 {
                            return Ok(Some((s, n)));
                        }
                    }
                    return Ok(None);
                }
            }

            let batch = self.current.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.current = None;
                continue;
            }
            let s = batch.s_id.get(self.row);
            if self.cur_s.is_none() {
                self.cur_s = Some(s);
                self.cur_count = 0;
            } else if self.cur_s != Some(s) {
                let out_s = self.cur_s.replace(s).unwrap();
                let out_n = std::mem::replace(&mut self.cur_count, 0);
                if out_n > 0 {
                    return Ok(Some((out_s, out_n)));
                }
                // else skip emitting empty group and continue without advancing row
                continue;
            }

            let ot = batch.o_type.get(self.row);
            if (ot == self.iri_ref || ot == self.bnode) && batch.o_key.get(self.row) == s {
                self.cur_count += 1;
            }
            self.row += 1;
        }
    }
}

impl GroupStream for SubjectSelfLoopCountStreamV6 {
    #[inline]
    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        SubjectSelfLoopCountStreamV6::next_group(self)
    }
}

/// Subjects present in ALL constraint (extra) streams, each with the product of
/// its counts; subjects missing from any constraint predicate are skipped.
struct ExtraMatches {
    /// `None` once a stream has run out: no further subject can be in all of them.
    heads: Option<InnerMergeHeads>,
    key: u64,
    product: u64,
}

impl ExtraMatches {
    fn prime<S: GroupStream>(streams: &mut [S]) -> Result<Self> {
        Ok(Self {
            heads: InnerMergeHeads::prime(streams)?,
            key: 0,
            product: 0,
        })
    }

    /// Take the next subject in every stream into `key` / `product`. `false`
    /// when there is none.
    #[inline(always)]
    fn next<S: GroupStream>(&mut self, streams: &mut [S]) -> Result<bool> {
        loop {
            let Some(heads) = self.heads.as_mut() else {
                return Ok(false);
            };
            let aligned = heads.aligned();
            if aligned {
                self.key = heads.key();
                self.product = heads.count_product().min(u64::MAX as u128) as u64;
            }
            if !heads.advance(streams)? {
                self.heads = None;
            }
            if aligned {
                return Ok(true);
            }
        }
    }
}

/// `Σ_s (Σ_b count_b(s)) × (Π_e count_e(s))` over subjects in any union branch
/// AND all extra streams, keeping only subjects `owned` by the caller's range.
#[inline(always)]
fn count_union_with_extras<U: GroupStream, E: GroupStream>(
    union_streams: &mut [U],
    extra_streams: &mut [E],
    owned: impl Fn(u64) -> bool,
) -> Result<u128> {
    let mut union = UnionMergeHeads::prime(union_streams)?;
    let mut extra = ExtraMatches::prime(extra_streams)?;
    let mut total: u128 = 0;
    let mut more = union.next(union_streams)? && extra.next(extra_streams)?;
    while more {
        let (us, es) = (union.key(), extra.key);
        more = match us.cmp(&es) {
            std::cmp::Ordering::Less => union.next(union_streams)?,
            std::cmp::Ordering::Greater => extra.next(extra_streams)?,
            std::cmp::Ordering::Equal => {
                if owned(us) {
                    let rows = (union.count_sum() as u128).saturating_mul(extra.product as u128);
                    total = total.saturating_add(rows);
                }
                union.next(union_streams)? && extra.next(extra_streams)?
            }
        };
    }
    Ok(total)
}

/// `Σ_s Σ_b count_b(s)`: the union's row count with no constraint streams.
fn sum_union<U: GroupStream>(union_streams: &mut [U]) -> Result<u64> {
    let mut union = UnionMergeHeads::prime(union_streams)?;
    let mut total: u64 = 0;
    while union.next(union_streams)? {
        total = total.saturating_add(union.count_sum());
    }
    Ok(total)
}

/// Per-partition partial for `(UNION of union_pids) ⋈ (AND of extra_pids)` COUNT(*)
/// over `[lo, hi)`: `Σ_s (Σ_b count_b(s)) × (Π_e count_e(s))` for subjects in any
/// union branch AND all extra predicates. BASE index only.
fn merge_union_constraint_count_range(
    store: &fluree_db_binary_index::BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    union_pids: &[u32],
    extra_pids: &[u32],
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut u_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(union_pids.len());
    for &p in union_pids {
        u_iters.push(
            PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?
                .with_cancellation(cancellation),
        );
    }
    let mut e_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(extra_pids.len());
    for &p in extra_pids {
        e_iters.push(
            PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?
                .with_cancellation(cancellation),
        );
    }
    count_union_with_extras(&mut u_iters, &mut e_iters, |_| true)
}

/// Parallel partitioned constrained-union count. Resolves predicate ids, picks the
/// partition driver, and dispatches to the shared harness. Returns `Ok(None)` to
/// defer to the cursor merge when a predicate is absent or there are too few rows.
/// Caller ensures `AllRows`, non-empty `extra_preds`, and HEAD (no overlay/time-travel).
fn try_union_constraint_parallel(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    union_preds: &[Ref],
    extra_preds: &[Ref],
    cancellation: &QueryCancellation,
) -> Result<Option<u64>> {
    let mut union_pids: Vec<u32> = Vec::with_capacity(union_preds.len());
    let mut extra_pids: Vec<u32> = Vec::with_capacity(extra_preds.len());
    let mut total_rows: u64 = 0;
    // Absent predicate (union or extra) => defer to the cursor merge, which handles
    // the empty-union / empty-join semantics.
    for p in union_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        union_pids.push(p_id);
        total_rows = total_rows.saturating_add(count_rows_for_predicate_psot(store, g_id, p_id)?);
    }
    for p in extra_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        extra_pids.push(p_id);
        total_rows = total_rows.saturating_add(count_rows_for_predicate_psot(store, g_id, p_id)?);
    }
    if union_pids.is_empty() || extra_pids.is_empty() {
        return Ok(None);
    }
    // Partition driver = the predicate (union or extra) with the most leaves.
    let driver_p = union_pids
        .iter()
        .chain(extra_pids.iter())
        .copied()
        .max_by_key(|&p| leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p).len())
        .unwrap();

    crate::count_plan_exec::parallel_partition_count(
        store,
        g_id,
        driver_p,
        total_rows,
        cancellation,
        |lo, hi| {
            merge_union_constraint_count_range(
                store,
                g_id,
                &union_pids,
                &extra_pids,
                cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Overlay/time-travel variant of `merge_union_constraint_count_range` for one
/// subject partition: `Σ_s (Σ_b count_b(s)) × (Π_e count_e(s))`, every predicate read
/// through a bounded overlay cursor (its `[lo,hi)` leaves + its novelty ops sliced to
/// that range). `union_ops`/`extra_ops` mirror `union_pids`/`extra_pids`. A
/// `s ∈ [lo,hi)` guard on the matched subject counts boundary subjects once.
#[allow(clippy::too_many_arguments)]
fn merge_union_constraint_count_range_overlay(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    union_pids: &[u32],
    union_ops: &[SharedOverlayOps],
    extra_pids: &[u32],
    extra_ops: &[SharedOverlayOps],
    to_t: i64,
    epoch: u64,
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let build = |p_id: u32, ops: &[fluree_db_binary_index::read::types::OverlayOp]| {
        let sliced = slice_overlay_ops_by_subject(ops, lo, hi);
        build_overlay_cursor_for_subject_range(
            store,
            g_id,
            p_id,
            cursor_projection_sid_only(),
            lo,
            hi,
            sliced,
            to_t,
            epoch,
        )
        .map(|c| CursorSubjectCountStream::new(c).with_cancellation(cancellation))
    };

    let mut u_streams: Vec<CursorSubjectCountStream> = Vec::with_capacity(union_pids.len());
    for (i, &p) in union_pids.iter().enumerate() {
        let Some(s) = build(p, &union_ops[i]) else {
            return Ok(0);
        };
        u_streams.push(s);
    }
    let mut e_streams: Vec<CursorSubjectCountStream> = Vec::with_capacity(extra_pids.len());
    for (i, &p) in extra_pids.iter().enumerate() {
        let Some(s) = build(p, &extra_ops[i]) else {
            return Ok(0);
        };
        e_streams.push(s);
    }
    count_union_with_extras(&mut u_streams, &mut e_streams, |s| s >= lo && s < hi)
}

/// Overlay/time-travel parallel constrained-union count: like
/// `try_union_constraint_parallel` but folds novelty per partition (bounded overlay
/// cursors). Collects each predicate's resolved ops once. Returns `Ok(None)` to defer
/// to the serial cursor merge for an absent predicate, a translation failure, or too
/// few rows.
fn try_union_constraint_overlay_parallel(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    union_preds: &[Ref],
    extra_preds: &[Ref],
) -> Result<Option<u64>> {
    type ResolvedPreds = (Vec<u32>, Vec<fluree_db_core::Sid>, u64);
    let resolve = |preds: &[Ref]| -> Result<Option<ResolvedPreds>> {
        let mut pids = Vec::with_capacity(preds.len());
        let mut sids = Vec::with_capacity(preds.len());
        let mut rows = 0u64;
        for p in preds {
            let sid = normalize_pred_sid(store, p)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(None);
            };
            rows = rows.saturating_add(count_rows_for_predicate_psot(store, g_id, p_id)?);
            pids.push(p_id);
            sids.push(sid);
        }
        Ok(Some((pids, sids, rows)))
    };
    let Some((union_pids, union_sids, ur)) = resolve(union_preds)? else {
        return Ok(None);
    };
    let Some((extra_pids, extra_sids, er)) = resolve(extra_preds)? else {
        return Ok(None);
    };
    if union_pids.is_empty() || extra_pids.is_empty() {
        return Ok(None);
    }
    let total_rows = ur.saturating_add(er);

    // Pre-gate before walking novelty: the serial cursor-merge fallback re-collects
    // these ops, so collecting them here only to fail the parallel gate is double work.
    if !crate::count_plan_exec::parallel_count_gate_open(total_rows) {
        return Ok(None);
    }

    let collect = |sids: &[fluree_db_core::Sid]| -> Result<Option<Vec<SharedOverlayOps>>> {
        let mut out = Vec::with_capacity(sids.len());
        for sid in sids {
            let Some(ops) = cached_overlay_ops(ctx, store, g_id, RunSortOrder::Psot, sid)? else {
                return Ok(None);
            };
            out.push(ops);
        }
        Ok(Some(out))
    };
    let Some(union_ops) = collect(&union_sids)? else {
        return Ok(None);
    };
    let Some(extra_ops) = collect(&extra_sids)? else {
        return Ok(None);
    };

    let to_t = ctx.to_t;
    let epoch = ctx.overlay.as_ref().map(|o| o.epoch()).unwrap_or(0);
    let driver_p = union_pids
        .iter()
        .chain(extra_pids.iter())
        .copied()
        .max_by_key(|&p| leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p).len())
        .unwrap();

    let (union_pids, union_ops, extra_pids, extra_ops) =
        (&union_pids, &union_ops, &extra_pids, &extra_ops);
    crate::count_plan_exec::parallel_partition_count(
        store,
        g_id,
        driver_p,
        total_rows,
        &ctx.cancellation,
        move |lo, hi| {
            merge_union_constraint_count_range_overlay(
                store,
                g_id,
                union_pids,
                union_ops,
                extra_pids,
                extra_ops,
                to_t,
                epoch,
                &ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

fn count_union_star(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    union_preds: &[Ref],
    extra_preds: &[Ref],
    mode: UnionCountMode,
) -> Result<Option<u64>> {
    let overlay_has_rows = ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0;
    if union_preds.is_empty() {
        return Ok(Some(0));
    }

    // Metadata fast lane: `{ ?s p1 ?o } UNION { ?s p2 ?o }` under COUNT(*) with no
    // extra constraint reduces, under bag semantics, to `Σ_p count_rows(p)` — a sum
    // of leaflet-directory row counts with NO row decode. (count(p1)+count(p2)
    // double-counts subjects present under both predicates, which is exactly correct
    // for UNION bag semantics.) Only valid at HEAD with no overlay/time-travel, where
    // base-leaflet directory counts are exact; otherwise fall through to the
    // overlay-merging cursor path below.
    //
    // Gate matches `count_plan_exec`: epoch != 0 OR to_t != max_t.
    let time_travel = ctx.to_t != store.max_t();
    if matches!(mode, UnionCountMode::AllRows)
        && extra_preds.is_empty()
        && !overlay_has_rows
        && !time_travel
    {
        let mut total: u64 = 0;
        for p in union_preds {
            let sid = normalize_pred_sid(store, p)?;
            // Absent predicate contributes 0. Safe here: no overlay means there are
            // no overlay-only rows a missing `p_id` could hide.
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                continue;
            };
            total = total.saturating_add(count_rows_for_predicate_psot(store, g_id, p_id)?);
        }
        return Ok(Some(total));
    }

    // Novelty at HEAD (no time-travel), no-constraint UNION: bag-semantics sum of
    // each branch's metadata base count + a novelty delta over only the touched
    // leaves, instead of a full cursor scan of every branch.
    if matches!(mode, UnionCountMode::AllRows)
        && extra_preds.is_empty()
        && overlay_has_rows
        && ctx.to_t >= store.max_t()
    {
        let mut total: u64 = 0;
        let mut all_ok = true;
        for p in union_preds {
            let sid = normalize_pred_sid(store, p)?;
            // A union branch present only in novelty (no base id), or a translation
            // failure, defers the whole count to the cursor merge below.
            match store.sid_to_p_id(&sid) {
                Some(p_id) => match count_predicate_overlay_delta(ctx, store, g_id, sid, p_id)? {
                    Some(n) => total = total.saturating_add(n),
                    None => {
                        all_ok = false;
                        break;
                    }
                },
                None => {
                    all_ok = false;
                    break;
                }
            }
        }
        if all_ok {
            return Ok(Some(total));
        }
    }

    // Parallel partitioned merge for the constrained `AllRows` case:
    // `{ ?s p1 ?o } UNION { ?s p2 ?o } . ?s e1 ?o2 …` COUNT(*) over large
    // predicates. HEAD-only (no overlay/time-travel); else the cursor merge below.
    if matches!(mode, UnionCountMode::AllRows)
        && !extra_preds.is_empty()
        && !overlay_has_rows
        && !time_travel
    {
        if let Some(total) =
            try_union_constraint_parallel(store, g_id, union_preds, extra_preds, &ctx.cancellation)?
        {
            return Ok(Some(total));
        }
    }

    // Overlay/time-travel constrained-UNION: parallelize the base scan and fold
    // novelty per partition. Falls through to the serial cursor merge below for
    // absent predicates, translation failures, or too few rows.
    if matches!(mode, UnionCountMode::AllRows)
        && !extra_preds.is_empty()
        && (overlay_has_rows || time_travel)
    {
        if let Some(total) =
            try_union_constraint_overlay_parallel(store, ctx, g_id, union_preds, extra_preds)?
        {
            return Ok(Some(total));
        }
    }

    // Build union streams.
    let mut union_streams_all: Vec<CursorSubjectCountStream> = Vec::new();
    let mut union_streams_eq: Vec<SubjectSelfLoopCountStreamV6> = Vec::new();

    for p in union_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            if overlay_has_rows {
                return Ok(None);
            }
            continue;
        };

        let projection = match mode {
            UnionCountMode::AllRows => cursor_projection_sid_only(),
            UnionCountMode::SubjectEqObject => cursor_projection_sid_otype_okey(),
        };

        let Some(cursor) =
            build_psot_cursor_for_predicate(ctx, store, g_id, sid, p_id, projection)?
        else {
            return Ok(None);
        };
        match mode {
            UnionCountMode::AllRows => {
                union_streams_all.push(
                    CursorSubjectCountStream::new(cursor).with_cancellation(&ctx.cancellation),
                );
            }
            UnionCountMode::SubjectEqObject => {
                union_streams_eq.push(SubjectSelfLoopCountStreamV6::new(cursor));
            }
        }
    }

    // If no union predicates exist in the index, result is empty.
    if matches!(mode, UnionCountMode::AllRows) && union_streams_all.is_empty() {
        return Ok(Some(0));
    }
    if matches!(mode, UnionCountMode::SubjectEqObject) && union_streams_eq.is_empty() {
        return Ok(Some(0));
    }

    // If no extra predicates, total is just Σ_s union_sum(s).
    if extra_preds.is_empty() {
        let total = match mode {
            UnionCountMode::AllRows => sum_union(&mut union_streams_all)?,
            UnionCountMode::SubjectEqObject => sum_union(&mut union_streams_eq)?,
        };
        return Ok(Some(total));
    }

    // Build extra streams (per-subject counts).
    let mut extra_streams: Vec<CursorSubjectCountStream> = Vec::new();
    for p in extra_preds {
        let sid = normalize_pred_sid(store, p)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            // Required predicate absent => empty join.
            return if overlay_has_rows {
                Ok(None)
            } else {
                Ok(Some(0))
            };
        };
        let Some(cursor) = build_psot_cursor_for_predicate(
            ctx,
            store,
            g_id,
            sid,
            p_id,
            cursor_projection_sid_only(),
        )?
        else {
            return Ok(None);
        };
        extra_streams
            .push(CursorSubjectCountStream::new(cursor).with_cancellation(&ctx.cancellation));
    }
    // Merge-join union_sum(s) with product_extra(s).
    let total = match mode {
        UnionCountMode::AllRows => {
            count_union_with_extras(&mut union_streams_all, &mut extra_streams, |_| true)?
        }
        UnionCountMode::SubjectEqObject => {
            count_union_with_extras(&mut union_streams_eq, &mut extra_streams, |_| true)?
        }
    };
    Ok(Some(total.min(u64::MAX as u128) as u64))
}

#[cfg(test)]
mod group_merge_tests {
    use super::*;
    use fluree_db_core::storage::residency::{FetchKind, NeedFetch};
    use fluree_db_core::{ContentId, ContentKind};
    use std::collections::VecDeque;

    /// Yields its groups, then fails with a typed residency miss if it has one.
    struct Groups {
        groups: VecDeque<(u64, u64)>,
        miss: Option<QueryError>,
    }

    impl Groups {
        fn of(groups: &[(u64, u64)]) -> Self {
            Self {
                groups: groups.iter().copied().collect(),
                miss: None,
            }
        }
    }

    impl GroupStream for Groups {
        fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
            match self.groups.pop_front() {
                Some(group) => Ok(Some(group)),
                None => self.miss.take().map_or(Ok(None), Err),
            }
        }
    }

    #[test]
    fn union_times_extras_counts_bag_union_against_every_constraint() {
        // Union (bag): s=1 -> 2, s=3 -> 3+4, s=4 -> 5, s=9 -> 1.
        let mut union = [
            Groups::of(&[(1, 2), (3, 3), (9, 1)]),
            Groups::of(&[(3, 4), (4, 5)]),
        ];
        // In both constraints: s=3 -> 2*5, s=9 -> 3*7. s=4 is in one only.
        let mut extras = [
            Groups::of(&[(3, 2), (4, 1), (9, 3)]),
            Groups::of(&[(1, 9), (3, 5), (9, 7), (12, 1)]),
        ];
        let total = count_union_with_extras(&mut union, &mut extras, |_| true).unwrap();
        assert_eq!(total, 7 * 10 + 21);

        // A partition owns only its own subjects: drop s=9.
        let mut union = [
            Groups::of(&[(1, 2), (3, 3), (9, 1)]),
            Groups::of(&[(3, 4), (4, 5)]),
        ];
        let mut extras = [
            Groups::of(&[(3, 2), (4, 1), (9, 3)]),
            Groups::of(&[(1, 9), (3, 5), (9, 7), (12, 1)]),
        ];
        let total = count_union_with_extras(&mut union, &mut extras, |s| s < 9).unwrap();
        assert_eq!(total, 7 * 10);

        let mut union = [Groups::of(&[(1, 2), (3, 3)]), Groups::of(&[(3, 4)])];
        assert_eq!(sum_union(&mut union).unwrap(), 9);
    }

    #[test]
    fn group_errors_preserve_typed_residency_misses() {
        for failing_union in [true, false] {
            let cid = ContentId::new(ContentKind::IndexLeaf, b"missing group leaf");
            let miss = NeedFetch::new(cid.clone(), FetchKind::IndexLeaf);
            let expected = miss.to_string();
            let failing = Groups {
                groups: VecDeque::from([(7, 2)]),
                miss: Some(QueryError::from_io("group leaf", miss.into_io_error())),
            };
            let healthy = Groups::of(&[(7, 3), (8, 1)]);
            let (mut union, mut extras) = if failing_union {
                ([failing], [healthy])
            } else {
                ([healthy], [failing])
            };
            let err = count_union_with_extras(&mut union, &mut extras, |_| true)
                .expect_err("the source must fail");
            assert_eq!(err.to_string(), expected);
            assert!(!err.can_demote_in_expression());
            assert!(!err.demotes_to_unbound_in_extend());
            match err {
                QueryError::NeedFetch(miss) => {
                    assert_eq!(miss.cid, cid);
                    assert_eq!(miss.kind, FetchKind::IndexLeaf);
                }
                other => panic!("lost typed residency miss: {other}"),
            }
        }
    }
}
