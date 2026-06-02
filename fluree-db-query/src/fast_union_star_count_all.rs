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
    build_count_batch, build_psot_cursor_for_predicate, count_rows_for_predicate_psot, count_to_i64,
    cursor_projection_sid_only, cursor_projection_sid_otype_okey, leaf_entries_for_predicate,
    normalize_pred_sid, CursorSubjectCountStream, PsotSubjectCountIter,
};
use crate::ir::triple::Ref;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::{BinaryCursor, RunSortOrder};
use fluree_db_core::o_type::OType;
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
        let allow_fast = !ctx.is_multi_ledger()
            && ctx.from_t.is_none()
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root());
        if allow_fast {
            if let Some(store) = ctx.binary_store.as_ref() {
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

/// Min-merge over union-branch iterators: returns `(s_min, Σ counts at s_min)` and
/// advances the iterators at `s_min`. Bag semantics — a subject under multiple
/// branches sums their counts.
fn next_union_group(
    iters: &mut [PsotSubjectCountIter<'_>],
    cur: &mut [Option<(u64, u64)>],
) -> Result<Option<(u64, u64)>> {
    if cur.iter().all(std::option::Option::is_none) {
        return Ok(None);
    }
    let s_min = cur.iter().filter_map(|c| c.map(|(s, _)| s)).min().unwrap();
    let mut sum: u64 = 0;
    for (i, it) in iters.iter_mut().enumerate() {
        if let Some((s, n)) = cur[i] {
            if s == s_min {
                sum = sum.saturating_add(n);
                cur[i] = it.next_group()?;
            }
        }
    }
    Ok(Some((s_min, sum)))
}

/// Max-merge over the constraint (extra) iterators: returns the next subject present
/// in ALL of them with the product of their counts; subjects missing from any
/// constraint predicate are skipped.
fn next_extra_product_group(
    iters: &mut [PsotSubjectCountIter<'_>],
    cur: &mut [Option<(u64, u64)>],
) -> Result<Option<(u64, u64)>> {
    loop {
        if cur.iter().any(std::option::Option::is_none) {
            return Ok(None);
        }
        let target = cur.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        let mut advanced = false;
        for (i, it) in iters.iter_mut().enumerate() {
            while let Some((s, _)) = cur[i] {
                if s < target {
                    cur[i] = it.next_group()?;
                    advanced = true;
                    if cur[i].is_none() {
                        return Ok(None);
                    }
                } else {
                    break;
                }
            }
        }
        if advanced {
            continue;
        }
        let mut prod: u64 = 1;
        for c in cur.iter() {
            prod = prod.saturating_mul(c.unwrap().1);
        }
        for (i, it) in iters.iter_mut().enumerate() {
            cur[i] = it.next_group()?;
        }
        return Ok(Some((target, prod)));
    }
}

/// Per-partition partial for `(UNION of union_pids) ⋈ (AND of extra_pids)` COUNT(*)
/// over `[lo, hi)`: `Σ_s (Σ_b count_b(s)) × (Π_e count_e(s))` for subjects in any
/// union branch AND all extra predicates. BASE index only.
fn merge_union_constraint_count_range(
    store: &fluree_db_binary_index::BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    union_pids: &[u32],
    extra_pids: &[u32],
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut u_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(union_pids.len());
    for &p in union_pids {
        u_iters.push(PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?);
    }
    let mut u_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(u_iters.len());
    for it in &mut u_iters {
        u_cur.push(it.next_group()?);
    }
    let mut e_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(extra_pids.len());
    for &p in extra_pids {
        e_iters.push(PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?);
    }
    let mut e_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(e_iters.len());
    for it in &mut e_iters {
        e_cur.push(it.next_group()?);
    }

    let mut u = next_union_group(&mut u_iters, &mut u_cur)?;
    let mut e = next_extra_product_group(&mut e_iters, &mut e_cur)?;
    let mut total: u128 = 0;
    while let (Some((us, usum)), Some((es, eprod))) = (u, e) {
        if us < es {
            u = next_union_group(&mut u_iters, &mut u_cur)?;
            continue;
        }
        if es < us {
            e = next_extra_product_group(&mut e_iters, &mut e_cur)?;
            continue;
        }
        total = total.saturating_add((usum as u128).saturating_mul(eprod as u128));
        u = next_union_group(&mut u_iters, &mut u_cur)?;
        e = next_extra_product_group(&mut e_iters, &mut e_cur)?;
    }
    Ok(total)
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

    crate::count_plan_exec::parallel_partition_count(store, g_id, driver_p, total_rows, |lo, hi| {
        merge_union_constraint_count_range(store, g_id, &union_pids, &extra_pids, lo, hi)
    })
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

    // Parallel partitioned merge for the constrained `AllRows` case:
    // `{ ?s p1 ?o } UNION { ?s p2 ?o } . ?s e1 ?o2 …` COUNT(*) over large
    // predicates. HEAD-only (no overlay/time-travel); else the cursor merge below.
    if matches!(mode, UnionCountMode::AllRows) && !extra_preds.is_empty() && !overlay_has_rows && !time_travel {
        if let Some(total) = try_union_constraint_parallel(store, g_id, union_preds, extra_preds)? {
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
                union_streams_all.push(CursorSubjectCountStream::new(cursor));
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

    // Helper: yield next `(s, sum)` for the UNION block.
    let mut union_curr_all: Vec<Option<(u64, u64)>> = Vec::new();
    let mut union_curr_eq: Vec<Option<(u64, u64)>> = Vec::new();
    match mode {
        UnionCountMode::AllRows => {
            for s in &mut union_streams_all {
                union_curr_all.push(s.next_group()?);
            }
        }
        UnionCountMode::SubjectEqObject => {
            for s in &mut union_streams_eq {
                union_curr_eq.push(s.next_group()?);
            }
        }
    }

    let mut next_union = || -> Result<Option<(u64, u64)>> {
        match mode {
            UnionCountMode::AllRows => {
                if union_curr_all.iter().all(std::option::Option::is_none) {
                    return Ok(None);
                }
                let s_min = union_curr_all
                    .iter()
                    .filter_map(|c| c.map(|(s, _)| s))
                    .min()
                    .unwrap();
                let mut sum = 0u64;
                for (i, st) in union_streams_all.iter_mut().enumerate() {
                    if let Some((s, n)) = union_curr_all[i] {
                        if s == s_min {
                            sum = sum.saturating_add(n);
                            union_curr_all[i] = st.next_group()?;
                        }
                    }
                }
                Ok(Some((s_min, sum)))
            }
            UnionCountMode::SubjectEqObject => {
                if union_curr_eq.iter().all(std::option::Option::is_none) {
                    return Ok(None);
                }
                let s_min = union_curr_eq
                    .iter()
                    .filter_map(|c| c.map(|(s, _)| s))
                    .min()
                    .unwrap();
                let mut sum = 0u64;
                for (i, st) in union_streams_eq.iter_mut().enumerate() {
                    if let Some((s, n)) = union_curr_eq[i] {
                        if s == s_min {
                            sum = sum.saturating_add(n);
                            union_curr_eq[i] = st.next_group()?;
                        }
                    }
                }
                Ok(Some((s_min, sum)))
            }
        }
    };

    // If no extra predicates, total is just Σ_s union_sum(s).
    if extra_preds.is_empty() {
        let mut total: u64 = 0;
        while let Some((_s, u)) = next_union()? {
            total = total.saturating_add(u);
        }
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
        extra_streams.push(CursorSubjectCountStream::new(cursor));
    }
    let mut extra_curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(extra_streams.len());
    for s in &mut extra_streams {
        extra_curr.push(s.next_group()?);
    }

    // Helper: next `(s, product)` for subjects that have all extra predicates.
    let mut next_extra_product = || -> Result<Option<(u64, u64)>> {
        loop {
            if extra_curr.iter().any(std::option::Option::is_none) {
                return Ok(None);
            }
            let target = extra_curr.iter().map(|c| c.unwrap().0).max().unwrap();
            let mut any_advanced = false;
            for (i, st) in extra_streams.iter_mut().enumerate() {
                while let Some((s_id, _)) = extra_curr[i] {
                    if s_id < target {
                        extra_curr[i] = st.next_group()?;
                        any_advanced = true;
                        if extra_curr[i].is_none() {
                            return Ok(None);
                        }
                    } else {
                        break;
                    }
                }
            }
            if any_advanced {
                continue;
            }
            let s = target;
            let mut prod: u64 = 1;
            for c in &extra_curr {
                prod = prod.saturating_mul(c.unwrap().1);
            }
            for (i, st) in extra_streams.iter_mut().enumerate() {
                extra_curr[i] = st.next_group()?;
            }
            return Ok(Some((s, prod)));
        }
    };

    // Merge-join union_sum(s) with product_extra(s).
    let mut u_cur = next_union()?;
    let mut e_cur = next_extra_product()?;
    let mut total: u128 = 0;
    while let (Some((us, u)), Some((es, eprod))) = (u_cur, e_cur) {
        if us < es {
            u_cur = next_union()?;
            continue;
        }
        if es < us {
            e_cur = next_extra_product()?;
            continue;
        }
        let add = (u as u128).saturating_mul(eprod as u128);
        total = total.saturating_add(add);
        u_cur = next_union()?;
        e_cur = next_extra_product()?;
    }
    Ok(Some(total.min(u64::MAX as u128) as u64))
}
