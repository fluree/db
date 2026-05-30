//! Count-only plan executor — evaluates a `CountPlan` against a `BinaryIndexStore`.
//!
//! The executor wraps as a `FastPathOperator` closure. During `open()`:
//! 1. Gate on `allow_cursor_fast_path(ctx)` + a binary-index store — `Ok(None)`
//!    (triggers fallback) otherwise. Unlike `fast_path_store`, this does NOT
//!    bail on overlay/`to_t`: an overlay/time-travel lane reads the
//!    novelty-merged PSOT cursor instead of base-leaflet metadata.
//! 2. Resolve all `Ref` predicates to `p_id`s
//! 3. Recursively evaluate the plan tree. Nodes without an overlay lane are
//!    rejected by `plan_overlay_supported` so they fall back under overlay.
//! 4. Return single count batch
//!
//! See `count_plan.rs` for the IR definition and planner.

use crate::context::ExecutionContext;
use crate::count_plan::{
    ChainFold, CountPlan, CountPlanRoot, KeySetNode, ScalarNode, StreamNode, TailWeight,
};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_count_batch, build_post_cursor_for_predicate,
    build_psot_cursor_for_predicate, collect_subjects_for_predicate_set,
    collect_subjects_for_predicate_sorted, collect_subjects_with_object_in_set,
    count_rows_for_predicate_psot, cursor_projection_otype_okey, cursor_projection_sid_only,
    cursor_projection_sid_otype_okey, intersect_many_sorted, leaf_entries_for_predicate,
    normalize_pred_sid, projection_sid_otype_okey, sum_post_object_counts_filtered,
    FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter, PsotObjectFilterCountIter,
    PsotSubjectCountIter, PsotSubjectWeightedSumIter,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use fluree_db_binary_index::{BinaryCursor, BinaryIndexStore, RunSortOrder};
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::sync::Arc;

/// Per-execution bundle threaded through the plan evaluator.
///
/// Carries the store + graph plus whether an **overlay lane** is required:
/// novelty is present (`overlay.epoch() != 0`) or the query is time-travel
/// (`to_t < max_t`). When `overlay` is false the metadata (base-leaflet)
/// primitives are exact and used as before; when true the subject-keyed nodes
/// route through the overlay-merging PSOT cursor instead. Nodes not yet
/// overlay-aware are rejected up-front by [`plan_overlay_supported`], so the
/// `overlay` lane is only entered for shapes that support it.
struct ExecCtx<'a, 'c> {
    ctx: &'a ExecutionContext<'c>,
    store: &'a Arc<BinaryIndexStore>,
    g_id: GraphId,
    overlay: bool,
}

/// Create a `FastPathOperator` that executes a `CountPlan`.
pub(crate) fn count_plan_operator(
    plan: CountPlan,
    fallback: Option<BoxedOperator>,
) -> BoxedOperator {
    let out_var = plan.out_var;
    Box::new(FastPathOperator::new(
        out_var,
        move |ctx| {
            // Strategy (b) gate: the subject-keyed shapes route through the
            // overlay-merging cursor, so we no longer bail on overlay/`to_t`
            // here (unlike `fast_path_store`). Multi-ledger / `from_t` / non-root
            // policy still bail.
            if !allow_cursor_fast_path(ctx) {
                return Ok(None);
            }
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None);
            };
            let g_id = ctx.binary_g_id;

            // Overlay lane needed when novelty is present or the query is
            // time-travel (`to_t < max_t`) — in both cases the base-leaflet
            // metadata primitives are not exact.
            let overlay = ctx
                .overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                != 0
                || ctx.to_t != store.max_t();

            // Only some node types have an overlay lane so far; any other node
            // under overlay must bail to the (correct, slower) generic fallback
            // rather than read stale base-only counts.
            if overlay && !plan_overlay_supported(&plan.root) {
                return Ok(None);
            }

            let ec = ExecCtx {
                ctx,
                store,
                g_id,
                overlay,
            };

            match execute_plan(&plan.root, &ec)? {
                Some(count) => {
                    let count_i64 = i64::try_from(count)
                        .map_err(|_| QueryError::execution("COUNT(*) exceeds i64 in count plan"))?;
                    Ok(Some(build_count_batch(out_var, count_i64)?))
                }
                None => Ok(None), // Fall through to general pipeline.
            }
        },
        fallback,
        "count-plan",
    ))
}

// ===========================================================================
// Plan evaluation
// ===========================================================================

fn execute_plan(root: &CountPlanRoot, ec: &ExecCtx<'_, '_>) -> Result<Option<u64>> {
    match root {
        CountPlanRoot::Scalar(scalar) => execute_scalar(scalar, ec),
        // Chain is metadata-only (rejected under overlay by plan_overlay_supported).
        CountPlanRoot::Chain(chain) => execute_chain(chain, ec.store, ec.g_id),
    }
}

// ---------------------------------------------------------------------------
// Overlay-lane support
// ---------------------------------------------------------------------------

/// Whether every node in the plan has an overlay-aware execution lane.
///
/// When false and an overlay/time-travel lane is required, the executor bails
/// to the generic fallback (correct, just not metadata-fast) rather than
/// reading stale base-only counts. So far only the subject-keyed scalar/star
/// shapes are covered; object/POST/chain shapes are added incrementally.
fn plan_overlay_supported(root: &CountPlanRoot) -> bool {
    match root {
        CountPlanRoot::Scalar(s) => scalar_overlay_supported(s),
        CountPlanRoot::Chain(_) => false,
    }
}

fn scalar_overlay_supported(node: &ScalarNode) -> bool {
    match node {
        ScalarNode::TotalRowCount { .. } | ScalarNode::CompositeJoinPairCount { .. } => true,
        ScalarNode::Sum { source } => stream_overlay_supported(source),
        ScalarNode::PostObjectFilteredSum { object_filter, .. } => {
            keyset_overlay_supported(object_filter)
        }
        ScalarNode::TotalMinusPostObjectFilteredSum {
            excluded_objects, ..
        } => keyset_overlay_supported(excluded_objects),
        // Dead variants (never built by the planner); keep bailing.
        ScalarNode::SumExcluding { .. } | ScalarNode::SumFiltered { .. } => false,
    }
}

fn stream_overlay_supported(node: &StreamNode) -> bool {
    match node {
        StreamNode::SubjectCountScan { .. } => true,
        StreamNode::StarJoin { children } => children.iter().all(stream_overlay_supported),
        StreamNode::AntiJoin { source, excluded } => {
            stream_overlay_supported(source) && keyset_overlay_supported(excluded)
        }
        StreamNode::SemiJoin { source, filter } => {
            stream_overlay_supported(source) && keyset_overlay_supported(filter)
        }
        StreamNode::OptionalJoin {
            required,
            optional_groups,
        } => {
            stream_overlay_supported(required)
                && optional_groups
                    .iter()
                    .all(|grp| grp.iter().all(stream_overlay_supported))
        }
    }
}

/// Subject-keyed keysets have an overlay lane; the object-keyed
/// `SubjectsWithObjectIn` does not (yet).
fn keyset_overlay_supported(node: &KeySetNode) -> bool {
    match node {
        KeySetNode::SubjectSet { .. } | KeySetNode::SubjectsSorted { .. } => true,
        KeySetNode::IntersectSorted { children } => children.iter().all(keyset_overlay_supported),
        KeySetNode::SubjectsWithObjectIn { .. } => false,
    }
}

/// A subject-keyed `(s_id, count)` group stream — metadata or overlay lane.
enum SubjectGroups<'a> {
    /// Genuinely empty (predicate absent from the base index, no overlay).
    Empty,
    /// Base-leaflet metadata lane.
    Meta(PsotSubjectCountIter<'a>),
    /// Overlay-merging PSOT cursor lane.
    Cursor(CursorSubjectGroups),
}

impl SubjectGroups<'_> {
    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        match self {
            SubjectGroups::Empty => Ok(None),
            SubjectGroups::Meta(it) => it.next_group(),
            SubjectGroups::Cursor(c) => c.next_group(),
        }
    }
}

/// Streams `(s_id, edge_count)` groups from an overlay-merged PSOT cursor.
///
/// Mirrors `SubjectCountStreamV6` in `fast_union_star_count_all`; kept local to
/// the count-plan overlay lane. The cursor yields rows in PSOT order, so a
/// running group-by on `s_id` produces the same `(subject, count)` pairs that
/// `PsotSubjectCountIter` derives from leaflet metadata — but over the
/// novelty-merged row stream.
struct CursorSubjectGroups {
    cursor: BinaryCursor,
    current: Option<fluree_db_binary_index::ColumnBatch>,
    row: usize,
    cur_s: Option<u64>,
    cur_count: u64,
}

impl CursorSubjectGroups {
    fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
            cur_s: None,
            cur_count: 0,
        }
    }

    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.current.is_none() {
                self.current = self
                    .cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?;
                self.row = 0;
                if self.current.is_none() {
                    if let Some(s) = self.cur_s.take() {
                        let n = std::mem::take(&mut self.cur_count);
                        return Ok(Some((s, n)));
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
                // Don't advance row; reprocess it into the new group.
                return Ok(Some((out_s, out_n)));
            }
            self.cur_count += 1;
            self.row += 1;
        }
    }
}

/// Build a subject-count group stream for `pred`, choosing the metadata lane
/// (base leaflets) when no overlay/time-travel is in effect, else the
/// overlay-merging PSOT cursor lane.
///
/// `Ok(None)` signals the whole plan must bail to the fallback: the predicate
/// is absent from the base index while novelty is present (it may carry
/// overlay-only rows a `p_id` cursor cannot reach), or an overlay flake failed
/// to translate. `Ok(Some(Empty))` is a genuinely empty stream.
fn subject_groups<'a>(ec: &ExecCtx<'a, '_>, pred: &Ref) -> Result<Option<SubjectGroups<'a>>> {
    let store: &'a Arc<BinaryIndexStore> = ec.store;
    let sid = normalize_pred_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return Ok(if ec.overlay {
            None
        } else {
            Some(SubjectGroups::Empty)
        });
    };
    if ec.overlay {
        let Some(cursor) = build_psot_cursor_for_predicate(
            ec.ctx,
            store,
            ec.g_id,
            sid,
            p_id,
            cursor_projection_sid_only(),
        )?
        else {
            return Ok(None);
        };
        Ok(Some(SubjectGroups::Cursor(CursorSubjectGroups::new(
            cursor,
        ))))
    } else {
        Ok(Some(SubjectGroups::Meta(PsotSubjectCountIter::new(
            store, ec.g_id, p_id,
        )?)))
    }
}

/// Distinct subjects for `pred`, ascending — metadata (no overlay) or the
/// overlay-merged PSOT cursor. The PSOT cursor yields rows in `(p, s, …)` order,
/// so consecutive-subject grouping produces distinct subjects already sorted.
/// `Ok(None)` bails the plan.
fn subject_keys_sorted(ec: &ExecCtx<'_, '_>, pred: &Ref) -> Result<Option<Vec<u64>>> {
    if ec.overlay {
        let Some(mut groups) = subject_groups(ec, pred)? else {
            return Ok(None);
        };
        let mut out: Vec<u64> = Vec::new();
        while let Some((s, _)) = groups.next_group()? {
            // One group per distinct subject, emitted in ascending order.
            out.push(s);
        }
        Ok(Some(out))
    } else {
        let sid = normalize_pred_sid(ec.store, pred)?;
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(Some(Vec::new()));
        };
        Ok(Some(collect_subjects_for_predicate_sorted(
            ec.store, ec.g_id, p_id,
        )?))
    }
}

/// Total row count for a predicate — metadata leaflet sum (no overlay) or a
/// row count over the overlay-merged PSOT cursor. `Ok(None)` bails the plan.
fn total_row_count(ec: &ExecCtx<'_, '_>, pred: &Ref) -> Result<Option<u64>> {
    let sid = normalize_pred_sid(ec.store, pred)?;
    let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
        return Ok(if ec.overlay { None } else { Some(0) });
    };
    if ec.overlay {
        let Some(mut cursor) = build_psot_cursor_for_predicate(
            ec.ctx,
            ec.store,
            ec.g_id,
            sid,
            p_id,
            cursor_projection_sid_only(),
        )?
        else {
            return Ok(None);
        };
        let mut total: u64 = 0;
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?
        {
            total = total
                .checked_add(batch.row_count as u64)
                .ok_or_else(|| QueryError::execution("COUNT(*) overflow in count plan"))?;
        }
        Ok(Some(total))
    } else {
        Ok(Some(count_rows_for_predicate_psot(
            ec.store, ec.g_id, p_id,
        )?))
    }
}

/// Sum of POST(`pred`) rows whose IRI_REF object key is in `allowed_sorted` —
/// metadata leaflet scan (no overlay) or the overlay-merging POST cursor.
///
/// Both lanes bail (`Ok(None)`) the instant a non-`IRI_REF` object is seen: this
/// fast path only applies when `pred`'s objects are all node references (the
/// object-var EXISTS/MINUS join requires `?o` to be a node). Mixed-type
/// predicates fall back to the generic pipeline, which skips literal objects.
fn post_object_filtered_sum(
    ec: &ExecCtx<'_, '_>,
    pred: &Ref,
    allowed_sorted: &[u64],
) -> Result<Option<u64>> {
    let sid = normalize_pred_sid(ec.store, pred)?;
    let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
        return Ok(if ec.overlay { None } else { Some(0) });
    };
    if !ec.overlay {
        return sum_post_object_counts_filtered(ec.store, ec.g_id, p_id, allowed_sorted);
    }

    let Some(mut cursor) = build_post_cursor_for_predicate(
        ec.ctx,
        ec.store,
        ec.g_id,
        sid,
        p_id,
        cursor_projection_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    // POST order over a ref-only predicate is `(o_key, …)` ascending, so a single
    // monotonic pointer into the sorted filter is O(rows + |filter|).
    let iri_ref = OType::IRI_REF.as_u16();
    let mut allowed_idx: usize = 0;
    let mut total: u64 = 0;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?
    {
        for row in 0..batch.row_count {
            if batch.o_type.get(row) != iri_ref {
                return Ok(None);
            }
            let o_key = batch.o_key.get(row);
            while allowed_idx < allowed_sorted.len() && allowed_sorted[allowed_idx] < o_key {
                allowed_idx += 1;
            }
            if allowed_idx < allowed_sorted.len() && allowed_sorted[allowed_idx] == o_key {
                total = total.saturating_add(1);
            }
        }
    }
    Ok(Some(total))
}

// ---------------------------------------------------------------------------
// Scalar evaluation
// ---------------------------------------------------------------------------

fn execute_scalar(node: &ScalarNode, ec: &ExecCtx<'_, '_>) -> Result<Option<u64>> {
    match node {
        ScalarNode::TotalRowCount { pred } => total_row_count(ec, pred),

        ScalarNode::CompositeJoinPairCount { pred1, pred2 } => {
            count_composite_join_pairs(ec, pred1, pred2)
        }

        ScalarNode::Sum { source } => sum_stream(source, ec, None, None),

        ScalarNode::SumExcluding { source, excluded } => {
            let exclude_sorted = execute_keyset_as_sorted(excluded, ec)?;
            let exclude_sorted = match exclude_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            sum_stream(source, ec, Some(&exclude_sorted), None)
        }

        ScalarNode::SumFiltered { source, filter } => {
            let filter_sorted = execute_keyset_as_sorted(filter, ec)?;
            let filter_sorted = match filter_sorted {
                Some(s) => s,
                None => return Ok(None),
            };
            sum_stream(source, ec, None, Some(&filter_sorted))
        }

        ScalarNode::PostObjectFilteredSum {
            pred,
            object_filter,
        } => {
            let filter_sorted = match execute_keyset_as_sorted(object_filter, ec)? {
                Some(s) => s,
                None => return Ok(None),
            };
            if filter_sorted.is_empty() {
                return Ok(Some(0));
            }
            post_object_filtered_sum(ec, pred, &filter_sorted)
        }

        ScalarNode::TotalMinusPostObjectFilteredSum {
            pred,
            excluded_objects,
        } => {
            let Some(total) = total_row_count(ec, pred)? else {
                return Ok(None);
            };
            let excluded_sorted = match execute_keyset_as_sorted(excluded_objects, ec)? {
                Some(s) => s,
                None => return Ok(None),
            };
            if excluded_sorted.is_empty() {
                return Ok(Some(total));
            }
            let Some(in_set) = post_object_filtered_sum(ec, pred, &excluded_sorted)? else {
                return Ok(None);
            };
            Ok(Some(total.saturating_sub(in_set)))
        }
    }
}

// ---------------------------------------------------------------------------
// Stream evaluation — produces (key, count) pairs, summed with optional filters
// ---------------------------------------------------------------------------

/// Sum all `(key, count)` from a stream, optionally filtering by exclude/include sets.
///
/// For MINUS/EXISTS modifiers on star/single-triple shapes, we pre-compute a sorted
/// exclusion or inclusion list and use a running index pointer for O(1) amortized
/// per-subject filtering (matching `fast_minus_count_all.rs` and `fast_exists_count_all.rs`).
fn sum_stream(
    node: &StreamNode,
    ec: &ExecCtx<'_, '_>,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    match node {
        StreamNode::SubjectCountScan { pred } => {
            let Some(mut groups) = subject_groups(ec, pred)? else {
                return Ok(None); // overlay bail
            };
            let mut excl_idx: usize = 0;
            let mut incl_idx: usize = 0;
            let mut total: u128 = 0;
            while let Some((s, count)) = groups.next_group()? {
                if is_excluded(s, exclude_sorted, &mut excl_idx) {
                    continue;
                }
                if !is_included(s, include_sorted, &mut incl_idx) {
                    continue;
                }
                total = total.saturating_add(count as u128);
            }
            Ok(Some(total.min(u64::MAX as u128) as u64))
        }

        StreamNode::StarJoin { children } => {
            sum_star_join(children, ec, exclude_sorted, include_sorted)
        }

        StreamNode::OptionalJoin {
            required,
            optional_groups,
        } => sum_optional_join(
            required,
            optional_groups,
            ec,
            exclude_sorted,
            include_sorted,
        ),

        StreamNode::AntiJoin { source, excluded } => {
            let exclude_list = execute_keyset_as_sorted(excluded, ec)?;
            let exclude_list = match exclude_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Merge with any existing sorted exclusion list.
            let merged = merge_sorted_lists(exclude_sorted, &exclude_list);
            sum_stream(source, ec, Some(&merged), include_sorted)
        }

        StreamNode::SemiJoin { source, filter } => {
            let filter_list = execute_keyset_as_sorted(filter, ec)?;
            let filter_list = match filter_list {
                Some(s) => s,
                None => return Ok(None),
            };
            // Intersect with any existing sorted inclusion list.
            let merged = match include_sorted {
                Some(existing) => intersect_sorted_pair(existing, &filter_list),
                None => filter_list,
            };
            sum_stream(source, ec, exclude_sorted, Some(&merged))
        }
    }
}

/// Check if `key` is in the sorted exclusion list, advancing the index pointer.
#[inline]
fn is_excluded(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return false };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Check if `key` is in the sorted inclusion list, advancing the index pointer.
/// Returns true if there is no inclusion list (no filter) or key is present.
#[inline]
fn is_included(key: u64, sorted: Option<&[u64]>, idx: &mut usize) -> bool {
    let Some(list) = sorted else { return true };
    while *idx < list.len() && list[*idx] < key {
        *idx += 1;
    }
    *idx < list.len() && list[*idx] == key
}

/// Merge two sorted lists into a deduplicated sorted union.
fn merge_sorted_lists(existing: Option<&[u64]>, new: &[u64]) -> Vec<u64> {
    let Some(existing) = existing else {
        return new.to_vec();
    };
    let mut result = Vec::with_capacity(existing.len() + new.len());
    let (mut i, mut j) = (0, 0);
    while i < existing.len() && j < new.len() {
        match existing[i].cmp(&new[j]) {
            std::cmp::Ordering::Less => {
                result.push(existing[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(new[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(existing[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&existing[i..]);
    result.extend_from_slice(&new[j..]);
    result
}

/// Intersect two sorted lists into a sorted intersection.
fn intersect_sorted_pair(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// N-way merge-join on subject count iterators, multiplying counts per key.
///
/// Uses sorted-list-based exclusion/inclusion with running index pointers for O(1)
/// amortized per-subject filtering (matching `fast_minus_count_all::count_property_join_all`).
///
/// Formula: `Σ_{s in all, not excluded, included} Π_i count_i(s)`
fn sum_star_join(
    children: &[StreamNode],
    ec: &ExecCtx<'_, '_>,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // All children must be SubjectCountScan for the streaming N-way merge.
    let mut iters: Vec<SubjectGroups<'_>> = Vec::with_capacity(children.len());
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        // Absent predicate (no overlay) yields an empty stream → the N-way
        // intersection is empty → total 0; absent under overlay bails (None).
        let Some(groups) = subject_groups(ec, pred)? else {
            return Ok(None);
        };
        iters.push(groups);
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(std::option::Option::is_none) {
            break;
        }

        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();

        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                total = total.saturating_add(product);
            }

            for (i, it) in iters.iter_mut().enumerate() {
                curr[i] = it.next_group()?;
            }
        } else {
            for (i, it) in iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = curr[i] {
                    if s_id < max_s {
                        curr[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

/// Fully streaming merge-join with OPTIONAL semantics.
///
/// Interleaves optional group cursor advancement with the required N-way merge,
/// matching the star-join + OPTIONAL multiplicity algorithm.
/// No HashMap materialization for required or optional streams.
///
/// Formula: `Σ_s req_product(s) × Π_g max(1, Π_i opt_gi(s))`
fn sum_optional_join(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    ec: &ExecCtx<'_, '_>,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // Collect required iterators (single scan or star join children). An absent
    // required predicate yields an `Empty` stream (no overlay) → no subjects →
    // total 0; absent under overlay bails (`None`).
    let mut req_iters: Vec<SubjectGroups<'_>> = Vec::new();
    match required {
        StreamNode::SubjectCountScan { pred } => {
            let Some(groups) = subject_groups(ec, pred)? else {
                return Ok(None);
            };
            req_iters.push(groups);
        }
        StreamNode::StarJoin { children } => {
            for child in children {
                let StreamNode::SubjectCountScan { pred } = child else {
                    return Ok(None);
                };
                let Some(groups) = subject_groups(ec, pred)? else {
                    return Ok(None);
                };
                req_iters.push(groups);
            }
        }
        _ => return Ok(None),
    }

    // Optional groups: each group is a same-subject star; multiplier is max(1, Π counts).
    // An optional predicate that is absent in the store makes the entire group `always_one`.
    struct OptGroup<'a> {
        always_one: bool,
        iters: Vec<SubjectGroups<'a>>,
        cur: Vec<Option<(u64, u64)>>,
    }

    let mut opt_groups: Vec<OptGroup<'_>> = Vec::with_capacity(optional_groups.len());
    for grp in optional_groups {
        let mut always_one = false;
        let mut iters: Vec<SubjectGroups<'_>> = Vec::with_capacity(grp.len());
        for node in grp {
            let StreamNode::SubjectCountScan { pred } = node else {
                return Ok(None);
            };
            let Some(groups) = subject_groups(ec, pred)? else {
                return Ok(None);
            };
            if matches!(groups, SubjectGroups::Empty) {
                // Absent optional predicate (no overlay) => group never matches
                // => multiplier 1. (Under overlay an absent predicate bails
                // above instead, since it may carry overlay-only rows.)
                always_one = true;
                iters.clear();
                break;
            }
            iters.push(groups);
        }
        let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
        for it in &mut iters {
            cur.push(it.next_group()?);
        }
        opt_groups.push(OptGroup {
            always_one,
            iters,
            cur,
        });
    }

    // Prime required cursors.
    let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_iters.len());
    for it in &mut req_iters {
        req_cur.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if req_cur.iter().any(std::option::Option::is_none) {
            break;
        }

        let max_s = req_cur
            .iter()
            .filter_map(|c| c.map(|(s, _)| s))
            .max()
            .unwrap();

        if req_cur.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let skip = is_excluded(max_s, exclude_sorted, &mut excl_idx)
                || !is_included(max_s, include_sorted, &mut incl_idx);

            if !skip {
                // Required product at this subject.
                let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();

                // Multiply OPTIONAL group factors for this subject (streaming).
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    let mut g_prod: u128 = 1;
                    for i in 0..g.iters.len() {
                        // Advance optional cursor to >= max_s.
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        let c = match g.cur[i] {
                            Some((sid2, c)) if sid2 == max_s => {
                                g.cur[i] = g.iters[i].next_group()?;
                                c
                            }
                            _ => 0u64,
                        };
                        if c == 0 {
                            g_prod = 0;
                            break;
                        }
                        g_prod = g_prod.saturating_mul(c as u128);
                    }
                    let mult = if g_prod == 0 { 1u128 } else { g_prod };
                    product = product.saturating_mul(mult);
                }

                total = total.saturating_add(product);
            } else {
                // Still need to advance optional cursors past this subject
                // to keep them in sync even when the required subject is skipped.
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    for i in 0..g.iters.len() {
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        if let Some((sid2, _)) = g.cur[i] {
                            if sid2 == max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                            }
                        }
                    }
                }
            }

            // Advance required iterators.
            for (i, it) in req_iters.iter_mut().enumerate() {
                req_cur[i] = it.next_group()?;
            }
        } else {
            // Advance smaller required subjects up to the current max.
            for (i, it) in req_iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = req_cur[i] {
                    if s_id < max_s {
                        req_cur[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

// ---------------------------------------------------------------------------
// KeySet evaluation — produces materialized sorted lists or hash sets
// ---------------------------------------------------------------------------

/// Primary keyset evaluator: returns a sorted `Vec<u64>`.
///
/// All callers in Phase B use sorted lists for streaming merge-skip/merge-keep.
fn execute_keyset_as_sorted(node: &KeySetNode, ec: &ExecCtx<'_, '_>) -> Result<Option<Vec<u64>>> {
    let store = ec.store;
    let g_id = ec.g_id;
    match node {
        KeySetNode::SubjectsSorted { pred } | KeySetNode::SubjectSet { pred } => {
            subject_keys_sorted(ec, pred)
        }
        KeySetNode::SubjectsWithObjectIn { pred, object_set } => {
            // Object-keyed; no overlay lane yet (rejected up-front by
            // `keyset_overlay_supported`, so this only runs in the metadata lane).
            if ec.overlay {
                return Ok(None);
            }
            // Need a hash set for the object filter, then sort the result.
            let obj_set = execute_keyset_as_hash_set(object_set, ec)?;
            let obj_set = match obj_set {
                Some(s) => s,
                None => return Ok(None),
            };
            let sid = normalize_pred_sid(store, pred)?;
            let Some(p_id) = store.sid_to_p_id(&sid) else {
                return Ok(Some(Vec::new()));
            };
            let Some(mut subjects) =
                collect_subjects_with_object_in_set(store, g_id, p_id, &obj_set)?
            else {
                return Ok(None);
            };
            subjects.sort_unstable();
            subjects.dedup();
            Ok(Some(subjects))
        }
        KeySetNode::IntersectSorted { children } => {
            let mut lists: Vec<Vec<u64>> = Vec::with_capacity(children.len());
            for child in children {
                let sorted = execute_keyset_as_sorted(child, ec)?;
                let sorted = match sorted {
                    Some(s) => s,
                    None => return Ok(None),
                };
                lists.push(sorted);
            }
            Ok(Some(intersect_many_sorted(lists)))
        }
    }
}

/// Hash-set evaluator for keysets — used only by `SubjectsWithObjectIn` which
/// needs `collect_subjects_with_object_in_set(&FxHashSet)`.
fn execute_keyset_as_hash_set(
    node: &KeySetNode,
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<FxHashSet<u64>>> {
    match node {
        KeySetNode::SubjectSet { pred } => {
            if ec.overlay {
                let Some(v) = subject_keys_sorted(ec, pred)? else {
                    return Ok(None);
                };
                return Ok(Some(v.into_iter().collect()));
            }
            let sid = normalize_pred_sid(ec.store, pred)?;
            let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
                return Ok(Some(FxHashSet::default()));
            };
            Ok(Some(collect_subjects_for_predicate_set(
                ec.store, ec.g_id, p_id,
            )?))
        }
        _ => {
            // Fall back: get sorted, convert to set.
            let sorted = execute_keyset_as_sorted(node, ec)?;
            match sorted {
                Some(v) => Ok(Some(v.into_iter().collect())),
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Chain evaluation
// ---------------------------------------------------------------------------

/// Execute a chain fold: right-to-left accumulation over a linear chain.
///
/// For a chain `?v0 <p1> ?v1 . ?v1 <p2> ?v2 . ... ?v_{N-1} <pN> ?vN`:
///
/// **Step 1** — Build initial weights keyed by `v_{N-1}` (subjects of pN).
///   - `TailWeight::None`: `weights[v_{N-1}] = count_pN(v_{N-1})`
///   - `TailWeight::Optional { tail_pred }`: For each `v_{N-1}`, compute
///     `Σ_{vN in pN(v_{N-1})} max(1, count_tail(vN))` via `PsotSubjectWeightedSumIter`.
///   - `TailWeight::Minus { tail_pred }`: Count only pN objects NOT in `subjects(tail_pred)`
///     via `PsotObjectFilterCountIter`.
///   - `TailWeight::Exists { tail_pred }`: Count only pN objects IN `subjects(tail_pred)`
///     via `PsotObjectFilterCountIter`.
///
/// **Step 2** — Fold right-to-left through `p_{N-1}` … `p2` via `PsotSubjectWeightedSumIter`.
///
/// **Step 3** — Final merge: `POST(p1)` objects × weights.
fn execute_chain(
    chain: &ChainFold,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
) -> Result<Option<u64>> {
    assert!(
        chain.predicates.len() >= 2,
        "chain must have at least 2 predicates"
    );

    let n = chain.predicates.len();

    // Resolve all predicate Refs to p_ids.
    let mut p_ids: Vec<u32> = Vec::with_capacity(n);
    for pred in &chain.predicates {
        let sid = normalize_pred_sid(store, pred)?;
        let Some(p_id) = store.sid_to_p_id(&sid) else {
            // Missing predicate in an inner join chain → 0.
            return Ok(Some(0));
        };
        p_ids.push(p_id);
    }

    // Generic chain speed trick (this is what made the 2026-03-09 numbers good):
    //
    // Do NOT scan PSOT(p2) across all subjects. Instead:
    // - Stream POST(p1) grouped by object b (these are the only b values that matter)
    // - For each b, *seek* into PSOT(p2) to sum weights over its objects
    //
    // This is especially important when p2 is huge (e.g. rdf:type) but p1's object
    // domain is much smaller.

    enum P2WeightMode<'a> {
        /// Each edge counts as 1 (used when the chain ends at p2 with no tail modifier).
        CountEdges { non_iri_weight: u64 },
        /// Weight is looked up by object key, with a default for missing/non-IRI objects.
        LookupRef {
            weights: &'a FxHashMap<u64, u64>,
            default_weight: u64,
            non_iri_weight: u64,
        },
        /// Owned version of [`LookupRef`] (used for 2-hop tail modifiers).
        LookupOwned {
            weights: FxHashMap<u64, u64>,
            default_weight: u64,
            non_iri_weight: u64,
        },
        /// Owned version of [`InSetRef`] (used for 2-hop tail modifiers).
        InSetOwned {
            set: FxHashSet<u64>,
            non_iri_weight: u64,
        },
        /// Owned version of [`NotInSetRef`] (used for 2-hop tail modifiers).
        NotInSetOwned {
            set: FxHashSet<u64>,
            non_iri_weight: u64,
        },
    }

    impl P2WeightMode<'_> {
        #[inline]
        fn weight_row(&self, o_type: u16, o_key: u64) -> u64 {
            let is_iri = o_type == fluree_db_core::o_type::OType::IRI_REF.as_u16();
            match self {
                P2WeightMode::CountEdges { non_iri_weight } => {
                    if is_iri {
                        1
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::LookupRef {
                    weights,
                    default_weight,
                    non_iri_weight,
                } => {
                    if is_iri {
                        weights.get(&o_key).copied().unwrap_or(*default_weight)
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::LookupOwned {
                    weights,
                    default_weight,
                    non_iri_weight,
                } => {
                    if is_iri {
                        weights.get(&o_key).copied().unwrap_or(*default_weight)
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::InSetOwned {
                    set,
                    non_iri_weight,
                } => {
                    if is_iri && set.contains(&o_key) {
                        1
                    } else {
                        *non_iri_weight
                    }
                }
                P2WeightMode::NotInSetOwned {
                    set,
                    non_iri_weight,
                } => {
                    if is_iri {
                        (!set.contains(&o_key)) as u64
                    } else {
                        *non_iri_weight
                    }
                }
            }
        }
    }

    struct PsotSeekSumCursor<'a, 'm> {
        store: &'a BinaryIndexStore,
        p_id: u32,
        leaves: &'a [fluree_db_binary_index::format::branch::LeafEntry],
        leaf_pos: usize,
        leaflet_idx: usize,
        row: usize,
        handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
        batch: Option<fluree_db_binary_index::ColumnBatch>,
        mixed: bool,
        /// True when the current leaflet is a pure non-IRI_REF leaflet —
        /// every row gets the `non_iri_weight` without checking `o_key`.
        all_non_iri: bool,
        mode: P2WeightMode<'m>,
    }

    impl<'a, 'm> PsotSeekSumCursor<'a, 'm> {
        fn new(
            store: &'a BinaryIndexStore,
            g_id: GraphId,
            p_id: u32,
            mode: P2WeightMode<'m>,
        ) -> Self {
            let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
            Self {
                store,
                p_id,
                leaves,
                leaf_pos: 0,
                leaflet_idx: 0,
                row: 0,
                handle: None,
                batch: None,
                mixed: false,
                all_non_iri: false,
                mode,
            }
        }

        fn load_next_batch(&mut self, target_b: u64) -> Result<Option<()>> {
            use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
            loop {
                if self.handle.is_none() {
                    if self.leaf_pos >= self.leaves.len() {
                        return Ok(None);
                    }
                    let leaf_entry = &self.leaves[self.leaf_pos];
                    self.leaf_pos += 1;
                    self.leaflet_idx = 0;
                    self.row = 0;
                    self.batch = None;
                    self.handle = Some(
                        self.store
                            .open_leaf_handle(
                                &leaf_entry.leaf_cid,
                                leaf_entry.sidecar_cid.as_ref(),
                                false,
                            )
                            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                    );
                }

                let handle = self.handle.as_ref().unwrap();
                let dir = handle.dir();

                while self.leaflet_idx < dir.entries.len() {
                    let entry = &dir.entries[self.leaflet_idx];
                    let idx = self.leaflet_idx;
                    self.leaflet_idx += 1;
                    if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                        continue;
                    }

                    // Skip leaflets that cannot contain the target subject by inspecting last key.
                    let last = read_ordered_key_v2(RunSortOrder::Psot, &entry.last_key);
                    let last_b = last.s_id.as_u64();
                    if last_b < target_b {
                        continue;
                    }

                    let mixed = entry.o_type_const.is_none();
                    let iri_only =
                        entry.o_type_const == Some(fluree_db_core::o_type::OType::IRI_REF.as_u16());
                    let non_iri_only = !mixed && !iri_only;

                    // Choose projection based on whether we need per-row o_type.
                    // - mixed: need o_type
                    // - non-IRI homogeneous: we don't need o_type, but we also don't need o_key (kept simple)
                    let projection = if mixed {
                        crate::fast_path_common::projection_sid_otype_okey()
                    } else {
                        crate::fast_path_common::projection_sid_okey()
                    };

                    let batch = if let Some(cache) = self.store.leaflet_cache() {
                        use fluree_db_binary_index::read::column_loader::load_columns_cached_via_handle;
                        let idx_u32: u32 = idx.try_into().map_err(|_| {
                            QueryError::Internal("leaflet idx exceeds u32".to_string())
                        })?;
                        load_columns_cached_via_handle(
                            handle.as_ref(),
                            idx,
                            RunSortOrder::Psot,
                            cache,
                            handle.leaf_id(),
                            idx_u32,
                        )
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                    } else {
                        handle
                            .load_columns(idx, &projection, RunSortOrder::Psot)
                            .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                    };

                    self.row = 0;
                    self.batch = Some(batch);
                    self.mixed = mixed;
                    self.all_non_iri = non_iri_only;
                    return Ok(Some(()));
                }

                self.handle = None;
            }
        }

        /// Return `Some(sum)` if `target_b` exists, otherwise `None`.
        fn seek_sum_for_subject(&mut self, target_b: u64) -> Result<Option<u64>> {
            let mut found = false;
            let mut sum: u64 = 0;

            loop {
                if self.batch.is_none() && self.load_next_batch(target_b)?.is_none() {
                    return Ok(found.then_some(sum));
                }
                let batch = self.batch.as_ref().unwrap();

                if self.row >= batch.row_count {
                    self.batch = None;
                    continue;
                }

                if !found {
                    // Advance to first row with s_id >= target_b.
                    while self.row < batch.row_count && batch.s_id.get(self.row) < target_b {
                        let cur = batch.s_id.get(self.row);
                        while self.row < batch.row_count && batch.s_id.get(self.row) == cur {
                            self.row += 1;
                        }
                    }
                    if self.row >= batch.row_count {
                        self.batch = None;
                        continue;
                    }
                    let b = batch.s_id.get(self.row);
                    if b > target_b {
                        return Ok(None);
                    }
                    found = true;
                } else if batch.s_id.get(self.row) > target_b {
                    return Ok(Some(sum));
                }

                // We are at `target_b`.
                while self.row < batch.row_count && batch.s_id.get(self.row) == target_b {
                    let (o_type, o_key) = if self.all_non_iri {
                        // Any non-IRI type will do; weight_row will take the non-iri branch.
                        (fluree_db_core::o_type::OType::XSD_STRING.as_u16(), 0)
                    } else if self.mixed {
                        (batch.o_type.get(self.row), batch.o_key.get(self.row))
                    } else {
                        (
                            fluree_db_core::o_type::OType::IRI_REF.as_u16(),
                            batch.o_key.get(self.row),
                        )
                    };

                    let w = self.mode.weight_row(o_type, o_key);
                    if w > 0 {
                        sum = sum.checked_add(w).ok_or_else(|| {
                            QueryError::execution("COUNT(*) overflow in chain join")
                        })?;
                    }
                    self.row += 1;
                }

                if self.row < batch.row_count {
                    return Ok(Some(sum));
                }

                // Subject group may continue in the next leaflet/batch.
                self.batch = None;
            }
        }
    }

    // 1) Build weights for v2 (object of p2), by folding the tail (p3..pN).
    let mut v2_weights: FxHashMap<u64, u64> = FxHashMap::default();
    if n == 2 {
        // No tail fold needed: p2 is the last predicate, and we handle tail_weight directly.
    } else {
        // Step 1: initial weights keyed by v_{N-1} (subjects of pN), with tail modifier.
        match &chain.tail_weight {
            TailWeight::None => {
                let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                while let Some((s, count)) = iter.next_group()? {
                    if count > 0 {
                        v2_weights.insert(s, count);
                    }
                }
            }
            TailWeight::Optional { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let mut mult_map: FxHashMap<u64, u64> = FxHashMap::default();
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?;
                    while let Some((c, count)) = iter.next_group()? {
                        mult_map.insert(c, count.max(1));
                    }
                    let Some(mut ws_iter) =
                        PsotSubjectWeightedSumIter::new(store, g_id, p_ids[n - 1], &mult_map, 1)?
                    else {
                        return Ok(None);
                    };
                    while let Some((s, sum)) = ws_iter.next_group()? {
                        if sum > 0 {
                            v2_weights.insert(s, sum);
                        }
                    }
                } else {
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                }
            }
            TailWeight::Minus { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let excluded = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                    let Some(mut iter) = PsotObjectFilterCountIter::new(
                        store,
                        g_id,
                        p_ids[n - 1],
                        &excluded,
                        ObjectFilterMode::NotInSet,
                    )?
                    else {
                        return Ok(None);
                    };
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                } else {
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?;
                    while let Some((s, count)) = iter.next_group()? {
                        if count > 0 {
                            v2_weights.insert(s, count);
                        }
                    }
                }
            }
            TailWeight::Exists { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) else {
                    return Ok(Some(0));
                };
                let included = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                if included.is_empty() {
                    return Ok(Some(0));
                }
                let Some(mut iter) = PsotObjectFilterCountIter::new(
                    store,
                    g_id,
                    p_ids[n - 1],
                    &included,
                    ObjectFilterMode::InSet,
                )?
                else {
                    return Ok(None);
                };
                while let Some((s, count)) = iter.next_group()? {
                    if count > 0 {
                        v2_weights.insert(s, count);
                    }
                }
            }
        }

        if v2_weights.is_empty() {
            return Ok(Some(0));
        }

        // Fold right-to-left through predicates p_{N-1}..p3, producing weights keyed by v2.
        // (Critically: we stop at i==2; p2 is handled via sparse seeks driven by POST(p1).)
        for i in (2..n - 1).rev() {
            let Some(mut iter) =
                PsotSubjectWeightedSumIter::new(store, g_id, p_ids[i], &v2_weights, 0)?
            else {
                return Ok(None);
            };
            let mut new_weights: FxHashMap<u64, u64> = FxHashMap::default();
            while let Some((b, sum)) = iter.next_group()? {
                if sum > 0 {
                    new_weights.insert(b, sum);
                }
            }
            if new_weights.is_empty() {
                return Ok(Some(0));
            }
            v2_weights = new_weights;
        }
    }

    // 2) Stream POST(p1) grouped by object `b`, and for each `b` seek PSOT(p2) to compute w(b).
    let Some(mut it1) = PostObjectGroupCountIter::new(store, g_id, p_ids[0])? else {
        return Ok(None);
    };

    // Configure how each p2 edge contributes to w(b).
    let p2_mode: P2WeightMode<'_> = if n == 2 {
        match &chain.tail_weight {
            TailWeight::None => P2WeightMode::CountEdges { non_iri_weight: 1 },
            TailWeight::Optional { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let mut mult_map: FxHashMap<u64, u64> = FxHashMap::default();
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?;
                    while let Some((c, count)) = iter.next_group()? {
                        mult_map.insert(c, count.max(1));
                    }
                    P2WeightMode::LookupOwned {
                        weights: mult_map,
                        default_weight: 1,
                        non_iri_weight: 1,
                    }
                } else {
                    P2WeightMode::CountEdges { non_iri_weight: 1 }
                }
            }
            TailWeight::Minus { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                if let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) {
                    let excluded = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                    P2WeightMode::NotInSetOwned {
                        set: excluded,
                        non_iri_weight: 1,
                    }
                } else {
                    P2WeightMode::CountEdges { non_iri_weight: 1 }
                }
            }
            TailWeight::Exists { tail_pred } => {
                let tail_sid = normalize_pred_sid(store, tail_pred)?;
                let Some(tail_p_id) = store.sid_to_p_id(&tail_sid) else {
                    return Ok(Some(0));
                };
                let included = collect_subjects_for_predicate_set(store, g_id, tail_p_id)?;
                if included.is_empty() {
                    return Ok(Some(0));
                }
                P2WeightMode::InSetOwned {
                    set: included,
                    non_iri_weight: 0,
                }
            }
        }
    } else {
        P2WeightMode::LookupRef {
            weights: &v2_weights,
            default_weight: 0,
            non_iri_weight: 0,
        }
    };

    let mut p2_cursor = PsotSeekSumCursor::new(store, g_id, p_ids[1], p2_mode);

    let mut total: u64 = 0;
    while let Some((b, n1)) = it1.next_group()? {
        let Some(w) = p2_cursor.seek_sum_for_subject(b)? else {
            continue;
        };
        if w == 0 {
            continue;
        }
        let add = n1
            .checked_mul(w)
            .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
        total = total
            .checked_add(add)
            .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
    }

    Ok(Some(total))
}

// ---------------------------------------------------------------------------
// Multicolumn (s,o)-join pair count: `?s <p1> ?o . ?s <p2> ?o`
// ---------------------------------------------------------------------------

/// Composite key `(s_id, o_type, o_key)` for the multicolumn merge-join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SoKey {
    s: u64,
    o_type: u16,
    o_key: u64,
}

impl Ord for SoKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.s
            .cmp(&other.s)
            .then_with(|| self.o_type.cmp(&other.o_type))
            .then_with(|| self.o_key.cmp(&other.o_key))
    }
}

impl PartialOrd for SoKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Streams a predicate's rows as `(s_id, o_type, o_key)` in PSOT order, which is
/// exactly ascending `SoKey` order for a fixed predicate.
struct PsotSoIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
    projection: fluree_db_binary_index::ColumnProjection,
}

impl<'a> PsotSoIter<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Self {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            projection: projection_sid_otype_okey(),
        }
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let batch = handle
                    .load_columns(idx, &self.projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    fn next_row(&mut self) -> Result<Option<SoKey>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            let key = SoKey {
                s: batch.s_id.get(self.row),
                o_type: batch.o_type.get(self.row),
                o_key: batch.o_key.get(self.row),
            };
            self.row += 1;
            return Ok(Some(key));
        }
    }
}

/// Streams `SoKey { s, o_type, o_key }` rows from an overlay-merged PSOT cursor,
/// in `(s, o_type, o_key)` order (matching `SoKey`'s ordering and `PsotSoIter`).
struct CursorSoIter {
    cursor: BinaryCursor,
    current: Option<fluree_db_binary_index::ColumnBatch>,
    row: usize,
}

impl CursorSoIter {
    fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
        }
    }

    fn next_row(&mut self) -> Result<Option<SoKey>> {
        loop {
            if self.current.is_none() {
                self.current = self
                    .cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?;
                self.row = 0;
                if self.current.is_none() {
                    return Ok(None);
                }
            }
            let batch = self.current.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.current = None;
                continue;
            }
            let key = SoKey {
                s: batch.s_id.get(self.row),
                o_type: batch.o_type.get(self.row),
                o_key: batch.o_key.get(self.row),
            };
            self.row += 1;
            return Ok(Some(key));
        }
    }
}

/// A `(s, o_type, o_key)` row stream — metadata or overlay lane.
enum SoRows<'a> {
    /// Genuinely empty (predicate absent from the base index, no overlay).
    Empty,
    Meta(PsotSoIter<'a>),
    Cursor(CursorSoIter),
}

impl SoRows<'_> {
    fn next_row(&mut self) -> Result<Option<SoKey>> {
        match self {
            SoRows::Empty => Ok(None),
            SoRows::Meta(it) => it.next_row(),
            SoRows::Cursor(c) => c.next_row(),
        }
    }
}

/// Build an `(s, o_type, o_key)` row stream for `pred` — metadata leaflet scan
/// (no overlay) or the overlay-merging PSOT cursor. `Ok(None)` bails the plan.
fn so_rows<'a>(ec: &ExecCtx<'a, '_>, pred: &Ref) -> Result<Option<SoRows<'a>>> {
    let store: &'a Arc<BinaryIndexStore> = ec.store;
    let sid = normalize_pred_sid(store, pred)?;
    let Some(p_id) = store.sid_to_p_id(&sid) else {
        return Ok(if ec.overlay {
            None
        } else {
            Some(SoRows::Empty)
        });
    };
    if ec.overlay {
        let Some(cursor) = build_psot_cursor_for_predicate(
            ec.ctx,
            store,
            ec.g_id,
            sid,
            p_id,
            cursor_projection_sid_otype_okey(),
        )?
        else {
            return Ok(None);
        };
        Ok(Some(SoRows::Cursor(CursorSoIter::new(cursor))))
    } else {
        Ok(Some(SoRows::Meta(PsotSoIter::new(store, ec.g_id, p_id))))
    }
}

/// Count `(s, o)` pairs present in BOTH predicate relations via a streaming
/// merge-join on the composite `(s_id, o_type, o_key)` key. Each shared pair is
/// counted once (intersection cardinality), NOT the product of per-subject counts.
/// `Ok(None)` bails the plan (overlay present but a predicate is absent from the
/// base index, or an overlay flake failed to translate).
fn count_composite_join_pairs(ec: &ExecCtx<'_, '_>, p1: &Ref, p2: &Ref) -> Result<Option<u64>> {
    let Some(mut it1) = so_rows(ec, p1)? else {
        return Ok(None);
    };
    let Some(mut it2) = so_rows(ec, p2)? else {
        return Ok(None);
    };

    let mut a = it1.next_row()?;
    let mut b = it2.next_row()?;
    let mut count: u64 = 0;

    while let (Some(ka), Some(kb)) = (a, b) {
        match ka.cmp(&kb) {
            Ordering::Less => a = it1.next_row()?,
            Ordering::Greater => b = it2.next_row()?,
            Ordering::Equal => {
                count = count.saturating_add(1);
                a = it1.next_row()?;
                b = it2.next_row()?;
            }
        }
    }

    Ok(Some(count))
}
