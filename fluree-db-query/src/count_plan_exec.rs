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
    allow_cursor_fast_path, build_count_batch, build_overlay_cursor_for_subject_range,
    build_post_cursor_for_predicate, build_psot_cursor_for_predicate, cached_overlay_ops,
    collect_subjects_for_predicate_set, collect_subjects_for_predicate_sorted,
    collect_subjects_with_object_in_set, count_rows_for_predicate_psot,
    cursor_projection_otype_okey, cursor_projection_sid_only, cursor_projection_sid_otype_okey,
    intersect_many_sorted, leaf_entries_for_predicate, normalize_pred_sid,
    projection_sid_otype_okey, slice_overlay_ops_by_subject, sum_post_object_counts_filtered,
    CursorSubjectCountStream, FastPathOperator, ObjectFilterMode, PostObjectGroupCountIter,
    PsotObjectFilterCountIter, PsotSubjectCountIter, PsotSubjectSeek, PsotSubjectWeightedSumIter,
    SharedOverlayOps,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use fluree_db_binary_index::{BinaryCursor, BinaryIndexStore, RunSortOrder};
use fluree_db_core::o_type::OType;
use fluree_db_core::{GraphId, QueryCancellation, Sid};
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
                tracing::debug!(
                    multi_ledger = ctx.is_multi_ledger(),
                    has_from_t = ctx.from_t.is_some(),
                    "count plan declined: cursor fast path gate"
                );
                return Ok(None);
            }
            let Some(store) = ctx.binary_store.as_ref() else {
                tracing::debug!("count plan declined: no binary store");
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
                tracing::debug!("count plan declined: overlay lane unsupported for plan shape");
                return Ok(None);
            }

            let ec = ExecCtx {
                ctx,
                store,
                g_id,
                overlay,
            };

            let started = std::time::Instant::now();
            match execute_plan(&plan.root, &ec)? {
                Some(count) => {
                    tracing::debug!(
                        overlay,
                        count,
                        elapsed_us = started.elapsed().as_micros() as u64,
                        plan_root = ?plan.root,
                        "count plan executed"
                    );
                    let count_i64 = i64::try_from(count)
                        .map_err(|_| QueryError::execution("COUNT(*) exceeds i64 in count plan"))?;
                    Ok(Some(build_count_batch(out_var, count_i64)?))
                }
                None => {
                    // Fall through to general pipeline.
                    tracing::debug!(
                        overlay,
                        plan_root = ?plan.root,
                        "count plan declined: executor returned no result"
                    );
                    Ok(None)
                }
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
        CountPlanRoot::Chain(chain) => {
            if ec.overlay {
                execute_chain_overlay(chain, ec)
            } else {
                execute_chain(chain, ec.store, ec.g_id, &ec.ctx.cancellation)
            }
        }
        CountPlanRoot::OptionalChainHead { p1, p2, p3 } => {
            if ec.overlay {
                execute_optional_chain_head_overlay(ec, p1, p2, p3)
            } else {
                execute_optional_chain_head(ec, p1, p2, p3)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Overlay-lane support
// ---------------------------------------------------------------------------

/// Whether every node in the plan has an overlay-aware execution lane.
///
/// When false and an overlay/time-travel lane is required, the executor bails
/// to the generic fallback (correct, just not metadata-fast) rather than
/// reading stale base-only counts. Every `CountPlan` node type has an overlay
/// lane, so this only returns false when a nested keyset is object-keyed in a
/// way not yet covered — currently never, in practice.
fn plan_overlay_supported(root: &CountPlanRoot) -> bool {
    match root {
        CountPlanRoot::Scalar(s) => scalar_overlay_supported(s),
        // execute_chain_overlay / execute_optional_chain_head_overlay handle any
        // shape (they bail at runtime on an absent predicate under novelty).
        CountPlanRoot::Chain(_) | CountPlanRoot::OptionalChainHead { .. } => true,
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

/// All keyset node types have an overlay lane (the object-keyed
/// `SubjectsWithObjectIn` is supported when its inner object set is).
fn keyset_overlay_supported(node: &KeySetNode) -> bool {
    match node {
        KeySetNode::SubjectSet { .. } | KeySetNode::SubjectsSorted { .. } => true,
        KeySetNode::IntersectSorted { children } => children.iter().all(keyset_overlay_supported),
        KeySetNode::SubjectsWithObjectIn { object_set, .. } => keyset_overlay_supported(object_set),
    }
}

/// A subject-keyed `(s_id, count)` group stream — metadata or overlay lane.
enum SubjectGroups<'a> {
    /// Genuinely empty (predicate absent from the base index, no overlay).
    Empty,
    /// Base-leaflet metadata lane.
    Meta(PsotSubjectCountIter<'a>),
    /// Overlay-merging PSOT cursor lane (shared `CursorSubjectCountStream`).
    Cursor(CursorSubjectCountStream),
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
        Ok(Some(SubjectGroups::Cursor(
            CursorSubjectCountStream::new(cursor).with_cancellation(&ec.ctx.cancellation),
        )))
    } else {
        Ok(Some(SubjectGroups::Meta(
            PsotSubjectCountIter::new(store, ec.g_id, p_id)?
                .with_cancellation(&ec.ctx.cancellation),
        )))
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
            ec.ctx.check_cancelled()?;
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
        ec.ctx.check_cancelled()?;
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

/// Subjects of `pred` that have at least one IRI_REF object in `object_set`
/// (sorted ascending, distinct) — metadata leaflet scan (no overlay) or the
/// overlay-merging PSOT cursor. Both lanes bail on any non-IRI_REF object,
/// matching the metadata path. `Ok(None)` bails the plan.
fn subjects_with_object_in(
    ec: &ExecCtx<'_, '_>,
    pred: &Ref,
    object_set: &FxHashSet<u64>,
) -> Result<Option<Vec<u64>>> {
    let sid = normalize_pred_sid(ec.store, pred)?;
    let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
        return Ok(if ec.overlay { None } else { Some(Vec::new()) });
    };
    if !ec.overlay {
        return collect_subjects_with_object_in_set(ec.store, ec.g_id, p_id, object_set);
    }

    let Some(mut cursor) = build_psot_cursor_for_predicate(
        ec.ctx,
        ec.store,
        ec.g_id,
        sid,
        p_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    let iri_ref = OType::IRI_REF.as_u16();
    let mut out: Vec<u64> = Vec::new();
    let mut cur_s: Option<u64> = None;
    let mut cur_ok = false;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?
    {
        ec.ctx.check_cancelled()?;
        for row in 0..batch.row_count {
            if batch.o_type.get(row) != iri_ref {
                return Ok(None);
            }
            let s = batch.s_id.get(row);
            if cur_s != Some(s) {
                if let Some(cs) = cur_s {
                    if cur_ok {
                        out.push(cs);
                    }
                }
                cur_s = Some(s);
                cur_ok = false;
            }
            if object_set.contains(&batch.o_key.get(row)) {
                cur_ok = true;
            }
        }
    }
    if let Some(cs) = cur_s {
        if cur_ok {
            out.push(cs);
        }
    }
    Ok(Some(out))
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
            // Asymmetric seek (HEAD, outermost modifier only): drive from the
            // smaller of outer/inner and seek the other, avoiding a full scan or
            // keyset-build of the large side.
            if !ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
                if let Some(n) = try_modifier_seek(source, excluded, true, ec)? {
                    return Ok(Some(n));
                }
                // Both-large (and multi-predicate inner): parallelize the keyset
                // build + outer scan instead of building the keyset serially.
                if let Some(n) = try_modifier_intersect_parallel(source, excluded, true, ec)? {
                    return Ok(Some(n));
                }
            }
            // Overlay/time-travel: parallelize the base scan + fold novelty per
            // partition; falls through to the serial keyset path otherwise.
            if ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
                if let Some(n) =
                    try_modifier_intersect_overlay_parallel(source, excluded, true, ec)?
                {
                    return Ok(Some(n));
                }
            }
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
            if !ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
                if let Some(n) = try_modifier_seek(source, filter, false, ec)? {
                    return Ok(Some(n));
                }
                // Both-large (and multi-predicate inner): parallelize the keyset
                // build + outer scan instead of building the keyset serially.
                if let Some(n) = try_modifier_intersect_parallel(source, filter, false, ec)? {
                    return Ok(Some(n));
                }
            }
            // Overlay/time-travel: parallelize the base scan + fold novelty per
            // partition; falls through to the serial keyset path otherwise.
            if ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
                if let Some(n) = try_modifier_intersect_overlay_parallel(source, filter, false, ec)?
                {
                    return Ok(Some(n));
                }
            }
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

/// Asymmetric seek strategy for a single-predicate EXISTS/MINUS over a
/// single-predicate outer: `?s A ?o1 {FILTER EXISTS | MINUS} { ?s B ?o2 }`.
///
/// Drives from whichever of A/B has fewer rows and seeks into the other, avoiding
/// a full scan or keyset-build of the large side:
/// - EXISTS  = `Σ_{s ∈ A ∧ s ∈ B} count_A(s)`
/// - MINUS   = `Σ_{s ∈ A ∧ s ∉ B} count_A(s)` = `total(A) − Σ_{s ∈ A ∩ B} count_A(s)`
///
/// (`is_anti` = true for MINUS / NOT EXISTS, false for EXISTS.) Returns `Ok(None)`
/// to defer to the keyset+scan path when the shape is not single-predicate on both
/// sides, or the sides are not skewed enough for the seek to win. BASE index only:
/// the caller must ensure `!ec.overlay` and that no outer exclude/include filter is
/// already active.
fn try_modifier_seek(
    source: &StreamNode,
    keyset: &KeySetNode,
    is_anti: bool,
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let StreamNode::SubjectCountScan { pred: pred_a } = source else {
        return Ok(None);
    };
    let pred_b = match keyset {
        KeySetNode::SubjectSet { pred } | KeySetNode::SubjectsSorted { pred } => pred,
        _ => return Ok(None),
    };

    let sid_a = normalize_pred_sid(ec.store, pred_a)?;
    let sid_b = normalize_pred_sid(ec.store, pred_b)?;

    // Absent-predicate semantics:
    // - A absent  => outer empty => 0 for both EXISTS and MINUS.
    // - B absent  => EXISTS: 0 (no subject has B); MINUS: total(A) (nothing excluded).
    let Some(p_a) = ec.store.sid_to_p_id(&sid_a) else {
        return Ok(Some(0));
    };
    let rows_a = count_rows_for_predicate_psot(ec.store, ec.g_id, p_a)?;
    let Some(p_b) = ec.store.sid_to_p_id(&sid_b) else {
        return Ok(Some(if is_anti { rows_a } else { 0 }));
    };
    let rows_b = count_rows_for_predicate_psot(ec.store, ec.g_id, p_b)?;

    // Only worthwhile when one side is much smaller than the other; otherwise the
    // existing keyset+scan path is competitive — defer to it.
    let (min_rows, max_rows) = (rows_a.min(rows_b), rows_a.max(rows_b));
    if min_rows.saturating_mul(SEEK_STAR_DRIVER_FACTOR) >= max_rows {
        return Ok(None);
    }

    let total: u128 = if rows_a <= rows_b {
        // Drive from A (smaller): iterate (s, count_A) and probe B for existence.
        let Some(mut a_groups) = subject_groups(ec, pred_a)? else {
            return Ok(None);
        };
        let mut b_seek =
            PsotSubjectSeek::new(ec.store, ec.g_id, p_b).with_cancellation(&ec.ctx.cancellation);
        let mut acc: u128 = 0;
        while let Some((s, count_a)) = a_groups.next_group()? {
            let present = b_seek.subject_present(s)?;
            // EXISTS keeps present subjects; MINUS keeps absent ones.
            if present != is_anti {
                acc = acc.saturating_add(count_a as u128);
            }
        }
        acc
    } else {
        // Drive from B (smaller): iterate B's distinct subjects, seek A's count.
        // `matched` = Σ_{s ∈ A ∩ B} count_A(s) (absent-in-A subjects seek to None).
        let Some(mut b_groups) = subject_groups(ec, pred_b)? else {
            return Ok(None);
        };
        let mut a_seek =
            PsotSubjectSeek::new(ec.store, ec.g_id, p_a).with_cancellation(&ec.ctx.cancellation);
        let mut matched: u128 = 0;
        while let Some((s, _)) = b_groups.next_group()? {
            if let Some(count_a) = a_seek.count_for_subject(s)? {
                matched = matched.saturating_add(count_a as u128);
            }
        }
        if is_anti {
            (rows_a as u128).saturating_sub(matched)
        } else {
            matched
        }
    };

    Ok(Some(total.min(u64::MAX as u128) as u64))
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
/// Driver/probe cost heuristic for the asymmetric seek strategy: a rough
/// rows-per-leaflet proxy. The seek strategy is chosen only when the smallest
/// child's row count times this factor is still below the largest child's row
/// count — i.e. the driver is small enough that probing it into the large side
/// (≈ one leaf-leapfrog pass) is cheaper than decoding the large side in full.
/// A wrong choice only affects speed, never the count, so an overestimate keeps
/// the strategy conservative (no regression vs the symmetric merge).
const SEEK_STAR_DRIVER_FACTOR: u64 = 8192;

/// Minimum total rows across the star's predicates before the parallel
/// partitioned merge is worth its thread-spawn overhead. Below this the serial
/// merge is used. (A wrong choice only affects speed, never the count.)
const PARALLEL_STAR_MIN_ROWS: u64 = 50_000;
/// Cap on partitions regardless of core count.
const PARALLEL_STAR_MAX_PARTITIONS: usize = 16;

/// Cheap pre-gate for the partitioned-merge fast paths: true only when there are
/// enough cores and rows to make parallelism worthwhile. The overlay-parallel
/// wrappers call this *before* collecting/resolving novelty ops, so that small or
/// single-core inputs skip that walk entirely and fall straight through to the
/// serial cursor-merge (which would otherwise redo the same overlay collection).
/// `parallel_partition_count` re-checks the identical condition, so the two never
/// disagree.
pub(crate) fn parallel_count_gate_open(total_rows: u64) -> bool {
    let ncpu = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    ncpu >= 2 && total_rows >= PARALLEL_STAR_MIN_ROWS
}

/// N-way merge-join COUNT over the subject range `[lo, hi)` of `p_ids`, using
/// bounded base-leaflet iterators. Returns `Σ_{s ∈ [lo,hi)} Π_i count_i(s)` —
/// the partial count for one partition. BASE index only (no overlay).
fn merge_count_range(
    store: &BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    p_ids: &[u32],
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(p_ids.len());
    for &p_id in p_ids {
        iters.push(
            PsotSubjectCountIter::new_bounded(store, g_id, p_id, lo, hi)?
                .with_cancellation(cancellation),
        );
    }
    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }
    let mut total: u128 = 0;
    loop {
        if curr.iter().any(std::option::Option::is_none) {
            break;
        }
        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
            total = total.saturating_add(product);
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
    Ok(total)
}

/// Per-partition partial for `?s REQ… OPTIONAL { ?s OPT… } …` over `[lo, hi)`:
/// `Σ_{s ∈ [lo,hi), s in all REQ} req_product(s) × Π_g max(1, Π_i opt_gi(s))`.
///
/// `opt_groups` lists each optional group's present predicate ids; an empty group
/// is `always_one` (multiplier 1 — an absent optional predicate). BASE index only
/// (no overlay), no exclude/include (the caller gates that).
fn merge_optional_count_range(
    store: &BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    req_pids: &[u32],
    opt_groups: &[Vec<u32>],
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut req_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(req_pids.len());
    for &p in req_pids {
        req_iters.push(
            PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?
                .with_cancellation(cancellation),
        );
    }

    struct OptG<'a> {
        always_one: bool,
        iters: Vec<PsotSubjectCountIter<'a>>,
        cur: Vec<Option<(u64, u64)>>,
    }
    let mut opt: Vec<OptG<'_>> = Vec::with_capacity(opt_groups.len());
    for grp in opt_groups {
        if grp.is_empty() {
            opt.push(OptG {
                always_one: true,
                iters: Vec::new(),
                cur: Vec::new(),
            });
            continue;
        }
        let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(grp.len());
        for &p in grp {
            iters.push(
                PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?
                    .with_cancellation(cancellation),
            );
        }
        let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
        for it in &mut iters {
            cur.push(it.next_group()?);
        }
        opt.push(OptG {
            always_one: false,
            iters,
            cur,
        });
    }

    let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_iters.len());
    for it in &mut req_iters {
        req_cur.push(it.next_group()?);
    }
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
            // Required inner-join product at this subject.
            let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();
            // Multiply each optional group's max(1, Π count) factor (streaming;
            // cursors lazily catch up to max_s).
            for g in &mut opt {
                if g.always_one {
                    continue;
                }
                let mut g_prod: u128 = 1;
                for i in 0..g.iters.len() {
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
            for (i, it) in req_iters.iter_mut().enumerate() {
                req_cur[i] = it.next_group()?;
            }
        } else {
            for (i, it) in req_iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = req_cur[i] {
                    if s_id < max_s {
                        req_cur[i] = it.next_group()?;
                    }
                }
            }
        }
    }
    Ok(total)
}

/// Ascending candidate subject boundaries for partitioning a predicate's scan.
///
/// When the predicate has at least `k` leaves, returns each leaf's first subject
/// straight from the in-memory manifest (no leaf opens) — the huge-predicate case
/// (e.g. `rdf:type`, thousands of leaves). Otherwise opens the few leaves and
/// returns each matching leaflet's first subject, giving finer boundaries for a
/// predicate that fits in one/few leaves but many leaflets. The returned values
/// are non-decreasing (leaves and leaflets are subject-sorted).
fn driver_subject_boundaries(
    store: &BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    p_id: u32,
    k: usize,
) -> Result<Vec<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    if leaves.len() >= k {
        return Ok(leaves.iter().map(|e| e.first_key.s_id.as_u64()).collect());
    }
    use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
    let mut bounds: Vec<u64> = Vec::new();
    for leaf in leaves {
        let handle = store
            .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        for entry in &handle.dir().entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let first = read_ordered_key_v2(RunSortOrder::Psot, &entry.first_key);
            bounds.push(first.s_id.as_u64());
        }
    }
    Ok(bounds)
}

/// Parallel partitioned variant of the inner-star count merge.
///
/// Partitions the subject space into K contiguous ranges at leaf boundaries and
/// runs the N-way merge per range on its own thread, then sums the partials.
/// Because partition boundaries are subject VALUES and every subject's rows are
/// contiguous within a predicate, each subject lands in exactly one partition —
/// so the partials sum exactly. Parallelizes both leaflet decompression AND the
/// merge itself, rather than keeping the merge serial. Returns `Ok(None)`
/// to defer to the serial merge when not applicable (not all `SubjectCountScan`,
/// a predicate absent, too few rows/leaves, or a single core). BASE index only:
/// Generic parallel-partitioned count harness.
///
/// Partitions the subject space into K contiguous ranges at `driver_p`'s
/// leaf/leaflet boundaries and runs `reducer(lo, hi)` per range on its own thread,
/// summing the partials. The reducer computes the per-partition partial count for
/// whatever per-subject aggregation is being parallelized (inner star, optional,
/// union, …). Because partition boundaries are subject VALUES and every subject's
/// rows are contiguous within a predicate, each subject lands in exactly one
/// partition — the partials sum exactly. Returns `Ok(None)` to defer to the serial
/// path when there aren't enough rows/leaves/cores to be worth parallelizing.
/// BASE index only: the caller must ensure no overlay/time-travel.
pub(crate) fn parallel_partition_count<F>(
    store: &BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    driver_p: u32,
    total_rows: u64,
    cancellation: &QueryCancellation,
    reducer: F,
) -> Result<Option<u64>>
where
    F: Fn(u64, u64) -> Result<u128> + Sync,
{
    if !parallel_count_gate_open(total_rows) {
        return Ok(None);
    }
    let ncpu = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    let k = ncpu.min(PARALLEL_STAR_MAX_PARTITIONS);
    // Candidate ascending subject boundaries from the driver: leaf first-subjects
    // when there are enough leaves (manifest only, no leaf opens — the huge-
    // predicate case), else leaflet first-subjects (opens the few driver leaves,
    // which the reducer scans anyway — the medium single-leaf-many-leaflet case).
    let candidates = driver_subject_boundaries(store, g_id, driver_p, k)?;
    if candidates.len() < 2 {
        return Ok(None);
    }
    // Subsample to ~K strictly-increasing boundaries: 0 = b_0 < … < b_K = MAX.
    let mut bounds: Vec<u64> = vec![0];
    for j in 1..k {
        let b = candidates[j * candidates.len() / k];
        if b > *bounds.last().unwrap() {
            bounds.push(b);
        }
    }
    bounds.push(u64::MAX);
    if bounds.len() < 3 {
        // Fewer than two real partitions — not worth parallelizing.
        return Ok(None);
    }
    let ranges: Vec<(u64, u64)> = bounds.windows(2).map(|w| (w[0], w[1])).collect();
    tracing::debug!(
        partitions = ranges.len(),
        total_rows,
        "count-plan: parallel partitioned count"
    );

    // Run partitions on the shared global rayon pool (not a per-query
    // `thread::scope`), so concurrent queries don't each spawn a fresh fan-out of
    // worker threads. See `fast_path_common::parallel_map_pooled`.
    let partials: Vec<Result<u128>> =
        crate::fast_path_common::parallel_map_pooled(ranges, |(lo, hi)| {
            crate::fast_path_common::bail_if_cancelled(cancellation)?;
            reducer(lo, hi)
        });

    let mut total: u128 = 0;
    for partial in partials {
        total = total.saturating_add(partial?);
    }
    Ok(Some(total.min(u64::MAX as u128) as u64))
}

fn sum_star_join_parallel(children: &[StreamNode], ec: &ExecCtx<'_, '_>) -> Result<Option<u64>> {
    if children.len() < 2 {
        return Ok(None);
    }
    let mut p_ids: Vec<u32> = Vec::with_capacity(children.len());
    let mut total_rows: u64 = 0;
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        p_ids.push(p_id);
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
    }

    // Partition driver = the predicate with the most leaves (finest boundaries).
    let driver_p = *p_ids
        .iter()
        .max_by_key(|&&p| {
            leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len()
        })
        .unwrap();

    parallel_partition_count(
        ec.store,
        ec.g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        |lo, hi| merge_count_range(ec.store, ec.g_id, &p_ids, &ec.ctx.cancellation, lo, hi),
    )
}

/// Overlay/time-travel variant of `merge_count_range` for one subject partition:
/// the N-way inner-star merge over `[lo, hi)`, but each predicate is read through a
/// *bounded overlay cursor* (its leaves in `[lo, hi)` plus its novelty ops sliced to
/// that range) rather than a base-only iterator, so novelty is folded in. A
/// `max_s ∈ [lo, hi)` guard keeps subjects in boundary leaves (shared with an
/// adjacent partition) counted by exactly one partition. `ops_per_pred[i]` are the
/// resolved overlay ops for `p_ids[i]`, collected once by the caller.
#[allow(clippy::too_many_arguments)]
fn merge_count_range_overlay(
    store: &Arc<BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    p_ids: &[u32],
    ops_per_pred: &[SharedOverlayOps],
    to_t: i64,
    epoch: u64,
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut streams: Vec<CursorSubjectCountStream> = Vec::with_capacity(p_ids.len());
    for (i, &p_id) in p_ids.iter().enumerate() {
        let sliced = slice_overlay_ops_by_subject(&ops_per_pred[i], lo, hi);
        let Some(cursor) = build_overlay_cursor_for_subject_range(
            store,
            g_id,
            p_id,
            cursor_projection_sid_only(),
            lo,
            hi,
            sliced,
            to_t,
            epoch,
        ) else {
            return Ok(0); // PSOT branch absent => empty intersection
        };
        streams.push(CursorSubjectCountStream::new(cursor).with_cancellation(cancellation));
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(streams.len());
    for s in &mut streams {
        curr.push(s.next_group()?);
    }
    let mut total: u128 = 0;
    loop {
        if curr.iter().any(std::option::Option::is_none) {
            break;
        }
        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            if max_s >= lo && max_s < hi {
                let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                total = total.saturating_add(product);
            }
            for (i, s) in streams.iter_mut().enumerate() {
                curr[i] = s.next_group()?;
            }
        } else {
            for (i, s) in streams.iter_mut().enumerate() {
                if let Some((s_id, _)) = curr[i] {
                    if s_id < max_s {
                        curr[i] = s.next_group()?;
                    }
                }
            }
        }
    }
    Ok(total)
}

/// Overlay/time-travel parallel inner-star `COUNT(*)`: like `sum_star_join_parallel`
/// but folds novelty. Collects each predicate's resolved overlay ops once, then runs
/// the bounded-overlay N-way merge per subject partition on the shared pool. Returns
/// `Ok(None)` to defer to the serial cursor merge for any non-plain shape, an absent
/// predicate (novelty-only or missing — serial/general handles it), an
/// overlay-translation failure, or too few rows.
fn sum_star_join_overlay_parallel(
    children: &[StreamNode],
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    if children.len() < 2 {
        return Ok(None);
    }
    let mut p_ids: Vec<u32> = Vec::with_capacity(children.len());
    let mut sids = Vec::with_capacity(children.len());
    let mut total_rows: u64 = 0;
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(None); // absent in base => defer (serial bails to general)
        };
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
        p_ids.push(p_id);
        sids.push(sid);
    }

    // Pre-gate: if the partitioned merge wouldn't fire anyway (too few rows/cores),
    // bail before walking novelty — the serial cursor-merge fallback re-collects the
    // same overlay ops, so doing it here would be pure double work (a regression).
    if !parallel_count_gate_open(total_rows) {
        return Ok(None);
    }

    // Collect + resolve each predicate's overlay ops once (serial; novelty is small).
    let mut ops_per_pred: Vec<SharedOverlayOps> = Vec::with_capacity(sids.len());
    for sid in &sids {
        match cached_overlay_ops(ec.ctx, ec.store, ec.g_id, RunSortOrder::Psot, sid)? {
            Some(ops) => ops_per_pred.push(ops),
            None => return Ok(None),
        }
    }
    let to_t = ec.ctx.to_t;
    let epoch = ec.ctx.overlay.as_ref().map(|o| o.epoch()).unwrap_or(0);

    let driver_p = *p_ids
        .iter()
        .max_by_key(|&&p| {
            leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len()
        })
        .unwrap();

    let store = ec.store;
    let g_id = ec.g_id;
    let p_ids_ref = &p_ids;
    let ops_ref = &ops_per_pred;
    parallel_partition_count(
        store,
        g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        move |lo, hi| {
            merge_count_range_overlay(
                store,
                g_id,
                p_ids_ref,
                ops_ref,
                to_t,
                epoch,
                &ec.ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Parallel partitioned variant of `sum_optional_join` for the all-present-
/// predicate shape (single/star required + optional groups of `SubjectCountScan`s).
/// Partitions by the required side and runs the optional merge per range. Returns
/// `Ok(None)` to defer to the serial merge for any other shape, an absent required
/// predicate, or too few rows. BASE index only: caller ensures `!ec.overlay` and no
/// exclude/include.
fn sum_optional_join_parallel(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let req_preds: &[StreamNode] = match required {
        StreamNode::SubjectCountScan { .. } => std::slice::from_ref(required),
        StreamNode::StarJoin { children } => children,
        _ => return Ok(None),
    };
    let mut req_pids: Vec<u32> = Vec::with_capacity(req_preds.len());
    let mut total_rows: u64 = 0;
    for node in req_preds {
        let StreamNode::SubjectCountScan { pred } = node else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        // Absent required predicate => empty inner join; let the serial path
        // produce the (correct) 0.
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        req_pids.push(p_id);
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
    }

    // Resolve optional groups; an absent optional predicate makes its group
    // `always_one` (multiplier 1), represented as an empty id list.
    let mut opt_groups: Vec<Vec<u32>> = Vec::with_capacity(optional_groups.len());
    for grp in optional_groups {
        let mut pids: Vec<u32> = Vec::with_capacity(grp.len());
        let mut absent = false;
        for node in grp {
            let StreamNode::SubjectCountScan { pred } = node else {
                return Ok(None);
            };
            let sid = normalize_pred_sid(ec.store, pred)?;
            match ec.store.sid_to_p_id(&sid) {
                Some(p_id) => {
                    pids.push(p_id);
                    total_rows = total_rows
                        .saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
                }
                None => {
                    absent = true;
                    break;
                }
            }
        }
        opt_groups.push(if absent { Vec::new() } else { pids });
    }

    // Drive partitioning from the required predicate with the most leaves.
    let driver_p = *req_pids
        .iter()
        .max_by_key(|&&p| {
            leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len()
        })
        .unwrap();

    parallel_partition_count(
        ec.store,
        ec.g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        |lo, hi| {
            merge_optional_count_range(
                ec.store,
                ec.g_id,
                &req_pids,
                &opt_groups,
                &ec.ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Overlay/time-travel variant of `merge_optional_count_range` for one subject
/// partition: `Σ_{s ∈ [lo,hi), s in all REQ} req_product(s) × Π_g max(1, Π opt)`,
/// every predicate read through a bounded overlay cursor (its `[lo,hi)` leaves + its
/// novelty ops sliced to that range). `req_ops`/`opt_ops` mirror `req_pids`/
/// `opt_groups`. A `max_s ∈ [lo,hi)` guard counts boundary subjects once; unlike the
/// base path there are no `always_one` groups — the caller bails when any predicate
/// is absent in base (it could be novelty-only), so all groups are present here.
#[allow(clippy::too_many_arguments)]
fn merge_optional_count_range_overlay(
    store: &Arc<BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    req_pids: &[u32],
    req_ops: &[SharedOverlayOps],
    opt_groups: &[Vec<u32>],
    opt_ops: &[Vec<SharedOverlayOps>],
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

    let mut req_streams: Vec<CursorSubjectCountStream> = Vec::with_capacity(req_pids.len());
    for (i, &p) in req_pids.iter().enumerate() {
        let Some(s) = build(p, &req_ops[i]) else {
            return Ok(0);
        };
        req_streams.push(s);
    }

    struct OptG {
        streams: Vec<CursorSubjectCountStream>,
        cur: Vec<Option<(u64, u64)>>,
    }
    let mut opt: Vec<OptG> = Vec::with_capacity(opt_groups.len());
    for (gi, grp) in opt_groups.iter().enumerate() {
        let mut streams: Vec<CursorSubjectCountStream> = Vec::with_capacity(grp.len());
        for (i, &p) in grp.iter().enumerate() {
            let Some(s) = build(p, &opt_ops[gi][i]) else {
                return Ok(0);
            };
            streams.push(s);
        }
        let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(streams.len());
        for s in &mut streams {
            cur.push(s.next_group()?);
        }
        opt.push(OptG { streams, cur });
    }

    let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_streams.len());
    for s in &mut req_streams {
        req_cur.push(s.next_group()?);
    }
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
            if max_s >= lo && max_s < hi {
                let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();
                for g in &mut opt {
                    let mut g_prod: u128 = 1;
                    for i in 0..g.streams.len() {
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.streams[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        let c = match g.cur[i] {
                            Some((sid2, c)) if sid2 == max_s => {
                                g.cur[i] = g.streams[i].next_group()?;
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
            }
            for (i, s) in req_streams.iter_mut().enumerate() {
                req_cur[i] = s.next_group()?;
            }
        } else {
            for (i, s) in req_streams.iter_mut().enumerate() {
                if let Some((s_id, _)) = req_cur[i] {
                    if s_id < max_s {
                        req_cur[i] = s.next_group()?;
                    }
                }
            }
        }
    }
    Ok(total)
}

/// Overlay/time-travel parallel `OPTIONAL` join COUNT(*): like
/// `sum_optional_join_parallel` but folds novelty per partition. Bails (defers to
/// the serial cursor merge) if any required or optional predicate is absent in base
/// — under overlay it could be novelty-only, which the serial/general path handles.
fn sum_optional_join_overlay_parallel(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let req_preds: &[StreamNode] = match required {
        StreamNode::SubjectCountScan { .. } => std::slice::from_ref(required),
        StreamNode::StarJoin { children } => children,
        _ => return Ok(None),
    };
    let resolve = |node: &StreamNode| -> Result<Option<(u32, Sid)>> {
        let StreamNode::SubjectCountScan { pred } = node else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        Ok(ec.store.sid_to_p_id(&sid).map(|p| (p, sid)))
    };

    let mut req_pids: Vec<u32> = Vec::with_capacity(req_preds.len());
    let mut req_sids: Vec<Sid> = Vec::with_capacity(req_preds.len());
    let mut total_rows: u64 = 0;
    for node in req_preds {
        let Some((p_id, sid)) = resolve(node)? else {
            return Ok(None);
        };
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
        req_pids.push(p_id);
        req_sids.push(sid);
    }

    let mut opt_groups: Vec<Vec<u32>> = Vec::with_capacity(optional_groups.len());
    let mut opt_sids: Vec<Vec<Sid>> = Vec::with_capacity(optional_groups.len());
    for grp in optional_groups {
        let mut pids: Vec<u32> = Vec::with_capacity(grp.len());
        let mut sids: Vec<Sid> = Vec::with_capacity(grp.len());
        for node in grp {
            // Absent optional predicate under overlay may be novelty-only — bail.
            let Some((p_id, sid)) = resolve(node)? else {
                return Ok(None);
            };
            total_rows =
                total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
            pids.push(p_id);
            sids.push(sid);
        }
        opt_groups.push(pids);
        opt_sids.push(sids);
    }

    // Pre-gate before walking novelty: the serial fallback re-collects these ops, so
    // collecting them here only to fail the parallel gate would be double work.
    if !parallel_count_gate_open(total_rows) {
        return Ok(None);
    }

    let collect = |sid: &Sid| -> Result<Option<SharedOverlayOps>> {
        cached_overlay_ops(ec.ctx, ec.store, ec.g_id, RunSortOrder::Psot, sid)
    };
    let mut req_ops = Vec::with_capacity(req_sids.len());
    for sid in &req_sids {
        let Some(ops) = collect(sid)? else {
            return Ok(None);
        };
        req_ops.push(ops);
    }
    let mut opt_ops: Vec<Vec<SharedOverlayOps>> = Vec::with_capacity(opt_sids.len());
    for grp in &opt_sids {
        let mut group_ops = Vec::with_capacity(grp.len());
        for sid in grp {
            let Some(ops) = collect(sid)? else {
                return Ok(None);
            };
            group_ops.push(ops);
        }
        opt_ops.push(group_ops);
    }

    let to_t = ec.ctx.to_t;
    let epoch = ec.ctx.overlay.as_ref().map(|o| o.epoch()).unwrap_or(0);
    let driver_p = *req_pids
        .iter()
        .max_by_key(|&&p| {
            leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len()
        })
        .unwrap();

    let store = ec.store;
    let g_id = ec.g_id;
    let (req_pids, opt_groups, req_ops, opt_ops) = (&req_pids, &opt_groups, &req_ops, &opt_ops);
    parallel_partition_count(
        store,
        g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        move |lo, hi| {
            merge_optional_count_range_overlay(
                store,
                g_id,
                req_pids,
                req_ops,
                opt_groups,
                opt_ops,
                to_t,
                epoch,
                &ec.ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Per-partition partial for a MINUS/EXISTS whose inner block is an inner-join of
/// one or more single-predicate subject sets, over a single-predicate outer:
/// `?s OUTER ?o {MINUS | FILTER EXISTS} { ?s IN1 ?a . ?s IN2 ?b . … }`.
///
/// Streams the outer iterator and every inner predicate's bounded subject iterator
/// together (all ascending) and, per outer subject, tests membership in the inner
/// intersection (present in *every* inner predicate). MINUS keeps subjects absent
/// from the intersection; EXISTS keeps those present:
///
/// `Σ_{s ∈ OUTER ∩ [lo,hi)} [keep(s)] · count_OUTER(s)` where
/// `keep(s) = is_anti ? s ∉ ⋂_i IN_i : s ∈ ⋂_i IN_i`.
///
/// O(1) memory — no keyset is materialized; the streaming merge parallelizes the
/// decompression of the outer *and* every inner predicate across partitions (the
/// inner keyset build is the dominant cost for a multi-predicate inner). BASE index
/// only (no overlay), no exclude/include (the caller gates that).
#[allow(clippy::too_many_arguments)]
fn merge_modifier_intersect_range(
    store: &BinaryIndexStore,
    g_id: fluree_db_core::GraphId,
    outer_pid: u32,
    inner_pids: &[u32],
    is_anti: bool,
    cancellation: &QueryCancellation,
    lo: u64,
    hi: u64,
) -> Result<u128> {
    let mut outer = PsotSubjectCountIter::new_bounded(store, g_id, outer_pid, lo, hi)?
        .with_cancellation(cancellation);
    let mut inner: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(inner_pids.len());
    for &p in inner_pids {
        inner.push(
            PsotSubjectCountIter::new_bounded(store, g_id, p, lo, hi)?
                .with_cancellation(cancellation),
        );
    }
    let mut i_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(inner.len());
    for it in &mut inner {
        i_cur.push(it.next_group()?);
    }

    let mut total: u128 = 0;
    while let Some((os, ocount)) = outer.next_group()? {
        // In the inner intersection iff present in every inner predicate. Each
        // inner cursor advances monotonically to the first subject >= os; since os
        // is non-decreasing, cursors skipped by an early break catch up lazily.
        let mut in_inner = true;
        for (k, it) in inner.iter_mut().enumerate() {
            while let Some((is_, _)) = i_cur[k] {
                if is_ < os {
                    i_cur[k] = it.next_group()?;
                } else {
                    break;
                }
            }
            if !matches!(i_cur[k], Some((is_, _)) if is_ == os) {
                in_inner = false;
                break;
            }
        }
        let keep = if is_anti { !in_inner } else { in_inner };
        if keep {
            total = total.saturating_add(ocount as u128);
        }
    }
    Ok(total)
}

/// Resolve a MINUS/EXISTS keyset into the `p_id`s of its inner predicates for the
/// parallel modifier-intersect path. Handles a single-predicate set
/// (`SubjectSet`/`SubjectsSorted`) and an inner-join intersection of such sets
/// (`IntersectSorted`). Returns `Ok(None)` to defer to the serial keyset path for
/// any other shape (e.g. object-chain `SubjectsWithObjectIn`) or if any predicate
/// is absent — the serial path applies the correct absent-predicate semantics.
fn resolve_keyset_pids(keyset: &KeySetNode, ec: &ExecCtx<'_, '_>) -> Result<Option<Vec<u32>>> {
    fn single_pid(node: &KeySetNode, ec: &ExecCtx<'_, '_>) -> Result<Option<u32>> {
        let pred = match node {
            KeySetNode::SubjectSet { pred } | KeySetNode::SubjectsSorted { pred } => pred,
            _ => return Ok(None),
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        Ok(ec.store.sid_to_p_id(&sid))
    }
    match keyset {
        KeySetNode::SubjectSet { .. } | KeySetNode::SubjectsSorted { .. } => {
            Ok(single_pid(keyset, ec)?.map(|p| vec![p]))
        }
        KeySetNode::IntersectSorted { children } => {
            let mut pids = Vec::with_capacity(children.len());
            for child in children {
                match single_pid(child, ec)? {
                    Some(p) => pids.push(p),
                    None => return Ok(None),
                }
            }
            if pids.is_empty() {
                return Ok(None);
            }
            Ok(Some(pids))
        }
        _ => Ok(None),
    }
}

/// Parallel-partitioned MINUS/EXISTS over a single-predicate outer and an inner
/// block that is a single predicate or an inner-join of predicates. Partitions the
/// subject space and runs `merge_modifier_intersect_range` per range, parallelizing
/// the inner keyset build (the dominant cost for a multi-predicate inner) together
/// with the outer scan. Returns `Ok(None)` to defer to the serial keyset+scan path
/// for any other shape, an absent predicate, or too few rows. BASE index only:
/// caller ensures `!ec.overlay` and no active exclude/include.
fn try_modifier_intersect_parallel(
    source: &StreamNode,
    keyset: &KeySetNode,
    is_anti: bool,
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let StreamNode::SubjectCountScan { pred: outer_pred } = source else {
        return Ok(None);
    };
    let outer_sid = normalize_pred_sid(ec.store, outer_pred)?;
    // Absent outer => empty outer => 0; let the serial path produce it.
    let Some(outer_pid) = ec.store.sid_to_p_id(&outer_sid) else {
        return Ok(None);
    };
    let Some(inner_pids) = resolve_keyset_pids(keyset, ec)? else {
        return Ok(None);
    };

    let mut total_rows = count_rows_for_predicate_psot(ec.store, ec.g_id, outer_pid)?;
    for &p in &inner_pids {
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p)?);
    }

    // Partition driver = the predicate (outer or inner) with the most leaves.
    let driver_p = std::iter::once(outer_pid)
        .chain(inner_pids.iter().copied())
        .max_by_key(|&p| leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len())
        .unwrap();

    parallel_partition_count(
        ec.store,
        ec.g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        |lo, hi| {
            merge_modifier_intersect_range(
                ec.store,
                ec.g_id,
                outer_pid,
                &inner_pids,
                is_anti,
                &ec.ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Like [`resolve_keyset_pids`] but also returns each inner predicate's `Sid`, so the
/// overlay-parallel path can collect that predicate's novelty ops.
fn resolve_keyset_pids_sids(
    keyset: &KeySetNode,
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<Vec<(u32, Sid)>>> {
    fn one(node: &KeySetNode, ec: &ExecCtx<'_, '_>) -> Result<Option<(u32, Sid)>> {
        let pred = match node {
            KeySetNode::SubjectSet { pred } | KeySetNode::SubjectsSorted { pred } => pred,
            _ => return Ok(None),
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        Ok(ec.store.sid_to_p_id(&sid).map(|p| (p, sid)))
    }
    match keyset {
        KeySetNode::SubjectSet { .. } | KeySetNode::SubjectsSorted { .. } => {
            Ok(one(keyset, ec)?.map(|x| vec![x]))
        }
        KeySetNode::IntersectSorted { children } => {
            let mut out = Vec::with_capacity(children.len());
            for child in children {
                match one(child, ec)? {
                    Some(x) => out.push(x),
                    None => return Ok(None),
                }
            }
            if out.is_empty() {
                return Ok(None);
            }
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

/// Overlay/time-travel variant of `merge_modifier_intersect_range` for one subject
/// partition: drives the outer through a bounded overlay cursor and tests each outer
/// subject for membership in the inner intersection (each inner predicate also a
/// bounded overlay cursor) — MINUS keeps absent, EXISTS keeps present. Novelty is
/// folded per predicate; an outer subject outside `[lo, hi)` (boundary leaf) is
/// skipped so each is counted by exactly one partition.
#[allow(clippy::too_many_arguments)]
fn merge_modifier_intersect_range_overlay(
    store: &Arc<BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    outer_pid: u32,
    outer_ops: &[fluree_db_binary_index::read::types::OverlayOp],
    inner_pids: &[u32],
    inner_ops: &[SharedOverlayOps],
    is_anti: bool,
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

    let Some(mut outer) = build(outer_pid, outer_ops) else {
        return Ok(0);
    };
    let mut inner: Vec<CursorSubjectCountStream> = Vec::with_capacity(inner_pids.len());
    for (i, &p) in inner_pids.iter().enumerate() {
        let Some(s) = build(p, &inner_ops[i]) else {
            return Ok(0);
        };
        inner.push(s);
    }
    let mut i_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(inner.len());
    for s in &mut inner {
        i_cur.push(s.next_group()?);
    }

    let mut total: u128 = 0;
    while let Some((os, ocount)) = outer.next_group()? {
        if os < lo {
            continue; // boundary subject below the partition; owned by a lower one
        }
        if os >= hi {
            break; // ascending => the rest are all out of range
        }
        let mut in_inner = true;
        for (k, s) in inner.iter_mut().enumerate() {
            while let Some((is_, _)) = i_cur[k] {
                if is_ < os {
                    i_cur[k] = s.next_group()?;
                } else {
                    break;
                }
            }
            if !matches!(i_cur[k], Some((is_, _)) if is_ == os) {
                in_inner = false;
                break;
            }
        }
        let keep = if is_anti { !in_inner } else { in_inner };
        if keep {
            total = total.saturating_add(ocount as u128);
        }
    }
    Ok(total)
}

/// Overlay/time-travel parallel MINUS/EXISTS: like `try_modifier_intersect_parallel`
/// but folds novelty per partition (bounded overlay cursors). Collects the outer and
/// inner predicates' resolved ops once. Returns `Ok(None)` to defer to the serial
/// keyset path for a non-plain shape, an absent predicate, a translation failure, or
/// too few rows.
fn try_modifier_intersect_overlay_parallel(
    source: &StreamNode,
    keyset: &KeySetNode,
    is_anti: bool,
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let StreamNode::SubjectCountScan { pred: outer_pred } = source else {
        return Ok(None);
    };
    let outer_sid = normalize_pred_sid(ec.store, outer_pred)?;
    let Some(outer_pid) = ec.store.sid_to_p_id(&outer_sid) else {
        return Ok(None);
    };
    let Some(inner) = resolve_keyset_pids_sids(keyset, ec)? else {
        return Ok(None);
    };
    let inner_pids: Vec<u32> = inner.iter().map(|(p, _)| *p).collect();

    let mut total_rows = count_rows_for_predicate_psot(ec.store, ec.g_id, outer_pid)?;
    for &p in &inner_pids {
        total_rows =
            total_rows.saturating_add(count_rows_for_predicate_psot(ec.store, ec.g_id, p)?);
    }

    // Pre-gate before walking novelty: the serial keyset fallback re-collects these
    // ops, so collecting them here only to fail the parallel gate is double work.
    if !parallel_count_gate_open(total_rows) {
        return Ok(None);
    }

    let Some(outer_ops) =
        cached_overlay_ops(ec.ctx, ec.store, ec.g_id, RunSortOrder::Psot, &outer_sid)?
    else {
        return Ok(None);
    };
    let mut inner_ops: Vec<SharedOverlayOps> = Vec::with_capacity(inner.len());
    for (_, sid) in &inner {
        let Some(ops) = cached_overlay_ops(ec.ctx, ec.store, ec.g_id, RunSortOrder::Psot, sid)?
        else {
            return Ok(None);
        };
        inner_ops.push(ops);
    }

    let to_t = ec.ctx.to_t;
    let epoch = ec.ctx.overlay.as_ref().map(|o| o.epoch()).unwrap_or(0);
    let driver_p = std::iter::once(outer_pid)
        .chain(inner_pids.iter().copied())
        .max_by_key(|&p| leaf_entries_for_predicate(ec.store, ec.g_id, RunSortOrder::Psot, p).len())
        .unwrap();

    let store = ec.store;
    let g_id = ec.g_id;
    let (inner_pids, inner_ops, outer_ops) = (&inner_pids, &inner_ops, &outer_ops);
    parallel_partition_count(
        store,
        g_id,
        driver_p,
        total_rows,
        &ec.ctx.cancellation,
        move |lo, hi| {
            merge_modifier_intersect_range_overlay(
                store,
                g_id,
                outer_pid,
                outer_ops,
                inner_pids,
                inner_ops,
                is_anti,
                to_t,
                epoch,
                &ec.ctx.cancellation,
                lo,
                hi,
            )
        },
    )
}

/// Metadata-only fold for the rdf:type inner-star count:
///
/// `COUNT(*)` of `?s rdf:type ?o1 . ?s P ?o2` = `Σ_C Σ_dt classStat[C][P].count`.
///
/// The per-`(class, property)` flake count attributes each `P`-flake on a
/// `k`-typed subject once per class, so summing over all classes yields
/// `Σ_{s typed} count_rdftype(s)·count_P(s)` — exactly the join's product-sum.
/// Reads the per-graph class stats straight from the snapshot (no index scan).
///
/// The per-(class,property) DATATYPE counts this fold sums are current-state-exact
/// on BOTH the bulk-import and incremental paths: the incremental class-stat merge
/// applies retraction and re-type deltas via base-vs-net attribution plus a
/// base-index re-scan of re-typed subjects (issue #1266). So the fold runs for any
/// index, not just bulk imports — there is no longer a `lex_sorted_string_ids`
/// gate here. (Ref-class edge counts are not consumed by this fold.)
///
/// Returns `Ok(None)` to defer to the merge when: the shape isn't exactly one
/// rdf:type leg plus one non-type leg, the graph class stats are absent, or the
/// fold is 0 (a genuinely-empty join — the merge handles it).
fn try_type_star_pred_fold(children: &[StreamNode], ec: &ExecCtx<'_, '_>) -> Result<Option<u64>> {
    if children.len() != 2 {
        return Ok(None);
    }
    let mut sids = Vec::with_capacity(2);
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        sids.push(normalize_pred_sid(ec.store, pred)?);
    }
    // Exactly one rdf:type leg; the other is the value predicate P.
    let value_sid = match (
        fluree_db_core::is_rdf_type(&sids[0]),
        fluree_db_core::is_rdf_type(&sids[1]),
    ) {
        (true, false) => &sids[1],
        (false, true) => &sids[0],
        _ => return Ok(None),
    };

    let Some(stats) = ec.ctx.active_snapshot.stats.as_ref() else {
        return Ok(None);
    };
    let Some(graphs) = stats.graphs.as_ref() else {
        return Ok(None);
    };
    let Some(graph) = graphs.iter().find(|g| g.g_id == ec.g_id) else {
        return Ok(None);
    };
    let Some(classes) = graph.classes.as_ref() else {
        return Ok(None);
    };

    let mut total: u128 = 0;
    for class in classes {
        if let Some(prop) = class
            .properties
            .iter()
            .find(|p| &p.property_sid == value_sid)
        {
            for &(_dt, count) in &prop.datatypes {
                total = total.saturating_add(count as u128);
            }
        }
    }
    if total == 0 {
        // Genuinely-empty join: let the merge confirm it.
        return Ok(None);
    }
    Ok(Some(total.min(u64::MAX as u128) as u64))
}

fn sum_star_join(
    children: &[StreamNode],
    ec: &ExecCtx<'_, '_>,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    // rdf:type inner-star (?s rdf:type ?o1 . ?s P ?o2) COUNT(*): answer from the
    // per-(class,property) datatype counts in the index stats — zero scan, instant.
    // The counts are current-state-exact on both bulk-import and incremental
    // indexes (issue #1266), so this runs for any base-index read; overlay reads
    // still defer to the merge below.
    if !ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
        if let Some(total) = try_type_star_pred_fold(children, ec)? {
            return Ok(Some(total));
        }
    }

    // Asymmetric (leapfrog) strategy: when one predicate is much smaller than the
    // others, drive from it and SEEK per-subject into the large side instead of a
    // full symmetric scan of every predicate. BASE index only, so HEAD-gated;
    // under overlay the cursor-merge path below stays correct.
    if !ec.overlay {
        if let Some(total) = try_sum_star_join_seek(children, ec, exclude_sorted, include_sorted)? {
            return Ok(Some(total));
        }
        // Both-large plain star (no modifiers): partition the subject space and
        // run the merge across cores. Parallelizes decompression + merge.
        if exclude_sorted.is_none() && include_sorted.is_none() {
            if let Some(total) = sum_star_join_parallel(children, ec)? {
                return Ok(Some(total));
            }
        }
    }

    // Overlay/time-travel plain star: parallelize the base scan and fold novelty
    // per partition (bounded overlay cursors). Falls through to the serial cursor
    // merge below for small inputs, absent predicates, or translation failures.
    if ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
        if let Some(total) = sum_star_join_overlay_parallel(children, ec)? {
            return Ok(Some(total));
        }
    }

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

/// Asymmetric (leapfrog) seek strategy for an inner subject-star count join.
///
/// Returns `Ok(None)` to defer to the symmetric merge when: fewer than two
/// children, a child is not a plain `SubjectCountScan`, a predicate is absent, or
/// the driver is not small enough to beat a full scan. Otherwise computes
/// `Σ_s driver_count(s) × Π_probe count_probe(s)` over the (ascending) driver
/// subjects that pass the exclude/include filters and exist in every probe —
/// driving from the smallest predicate and seeking into the others.
///
/// BASE index only: the caller must ensure `!ec.overlay`.
fn try_sum_star_join_seek(
    children: &[StreamNode],
    ec: &ExecCtx<'_, '_>,
    exclude_sorted: Option<&[u64]>,
    include_sorted: Option<&[u64]>,
) -> Result<Option<u64>> {
    if children.len() < 2 {
        return Ok(None);
    }

    // Resolve each child to (p_id, row_count). Bail to the merge if any child is
    // not a plain SubjectCountScan or its predicate is absent — the merge path
    // already handles those cases correctly.
    let mut child_pids: Vec<u32> = Vec::with_capacity(children.len());
    let mut child_rows: Vec<u64> = Vec::with_capacity(children.len());
    for child in children {
        let StreamNode::SubjectCountScan { pred } = child else {
            return Ok(None);
        };
        let sid = normalize_pred_sid(ec.store, pred)?;
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        child_pids.push(p_id);
        child_rows.push(count_rows_for_predicate_psot(ec.store, ec.g_id, p_id)?);
    }

    // Driver = smallest by row count; probes = the rest.
    let driver_idx = child_rows
        .iter()
        .enumerate()
        .min_by_key(|(_, r)| **r)
        .map(|(i, _)| i)
        .expect("children non-empty");
    let driver_rows = child_rows[driver_idx];
    let max_probe_rows = child_rows
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != driver_idx)
        .map(|(_, r)| *r)
        .max()
        .unwrap_or(0);

    // Only seek when the driver is much smaller than the largest probe; otherwise
    // a full symmetric scan is competitive (or cheaper) — defer to the merge.
    if driver_rows.saturating_mul(SEEK_STAR_DRIVER_FACTOR) >= max_probe_rows {
        return Ok(None);
    }
    if driver_rows == 0 {
        // Empty driver => empty inner join.
        return Ok(Some(0));
    }

    // Driver subject→count groups (ascending; Meta lane since `!ec.overlay`), and
    // one forward-only seek cursor per probe predicate.
    let StreamNode::SubjectCountScan { pred: driver_pred } = &children[driver_idx] else {
        return Ok(None);
    };
    let Some(mut driver) = subject_groups(ec, driver_pred)? else {
        return Ok(None);
    };
    let mut probes: Vec<PsotSubjectSeek<'_>> = child_pids
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != driver_idx)
        .map(|(_, &p_id)| {
            PsotSubjectSeek::new(ec.store, ec.g_id, p_id).with_cancellation(&ec.ctx.cancellation)
        })
        .collect();

    let mut excl_idx: usize = 0;
    let mut incl_idx: usize = 0;
    let mut total: u128 = 0;

    while let Some((s, driver_count)) = driver.next_group()? {
        if is_excluded(s, exclude_sorted, &mut excl_idx)
            || !is_included(s, include_sorted, &mut incl_idx)
        {
            continue;
        }
        let mut product: u128 = driver_count as u128;
        for probe in &mut probes {
            match probe.count_for_subject(s)? {
                Some(c) => product = product.saturating_mul(c as u128),
                None => {
                    // Subject absent from this probe => not in the inner join.
                    product = 0;
                    break;
                }
            }
        }
        total = total.saturating_add(product);
    }

    Ok(Some(total.min(u64::MAX as u128) as u64))
}

/// Asymmetric seek strategy for `?s A ?o1 OPTIONAL { ?s B ?o2 }` COUNT(*):
///
/// `Σ_s count_A(s) × max(1, count_B(s))` = `total(A) + Σ_{s ∈ A ∩ B} count_A(s)·(count_B(s) − 1)`.
///
/// Drives from the smaller of required-A / optional-B and seeks the other. When B
/// is the smaller side, `total(A)` is a directory sum and only the (few) B subjects
/// seek into A — avoiding a full scan of the large required side. Returns `Ok(None)`
/// to defer to the streaming merge for any other shape (multi-predicate required,
/// multiple/multi-triple optional groups) or when the sides are not skewed enough.
/// BASE index only: caller ensures `!ec.overlay` and no active exclude/include.
fn try_optional_seek(
    required: &StreamNode,
    optional_groups: &[Vec<StreamNode>],
    ec: &ExecCtx<'_, '_>,
) -> Result<Option<u64>> {
    let StreamNode::SubjectCountScan { pred: pred_a } = required else {
        return Ok(None);
    };
    if optional_groups.len() != 1 || optional_groups[0].len() != 1 {
        return Ok(None);
    }
    let StreamNode::SubjectCountScan { pred: pred_b } = &optional_groups[0][0] else {
        return Ok(None);
    };

    let sid_a = normalize_pred_sid(ec.store, pred_a)?;
    let Some(p_a) = ec.store.sid_to_p_id(&sid_a) else {
        // Required absent => no rows.
        return Ok(Some(0));
    };
    let rows_a = count_rows_for_predicate_psot(ec.store, ec.g_id, p_a)?;
    let sid_b = normalize_pred_sid(ec.store, pred_b)?;
    let Some(p_b) = ec.store.sid_to_p_id(&sid_b) else {
        // Optional absent => every required row contributes factor 1 => total(A).
        return Ok(Some(rows_a));
    };
    let rows_b = count_rows_for_predicate_psot(ec.store, ec.g_id, p_b)?;

    let (min_rows, max_rows) = (rows_a.min(rows_b), rows_a.max(rows_b));
    if min_rows.saturating_mul(SEEK_STAR_DRIVER_FACTOR) >= max_rows {
        return Ok(None);
    }

    let total: u128 = if rows_a <= rows_b {
        // Drive required A (smaller): Σ count_A(s) × max(1, count_B(s)).
        let Some(mut a_groups) = subject_groups(ec, pred_a)? else {
            return Ok(None);
        };
        let mut b_seek =
            PsotSubjectSeek::new(ec.store, ec.g_id, p_b).with_cancellation(&ec.ctx.cancellation);
        let mut acc: u128 = 0;
        while let Some((s, count_a)) = a_groups.next_group()? {
            let mult = b_seek.count_for_subject(s)?.unwrap_or(0).max(1);
            acc = acc.saturating_add((count_a as u128).saturating_mul(mult as u128));
        }
        acc
    } else {
        // Drive optional B (smaller): total(A) + Σ_{s ∈ B} count_A(s)·(count_B(s) − 1).
        let Some(mut b_groups) = subject_groups(ec, pred_b)? else {
            return Ok(None);
        };
        let mut a_seek =
            PsotSubjectSeek::new(ec.store, ec.g_id, p_a).with_cancellation(&ec.ctx.cancellation);
        let mut bonus: u128 = 0;
        while let Some((s, count_b)) = b_groups.next_group()? {
            // count_B == 1 yields no bonus; skip the seek (targets stay ascending).
            if count_b <= 1 {
                continue;
            }
            if let Some(count_a) = a_seek.count_for_subject(s)? {
                bonus =
                    bonus.saturating_add((count_a as u128).saturating_mul((count_b - 1) as u128));
            }
        }
        (rows_a as u128).saturating_add(bonus)
    };

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
    // Asymmetric seek (HEAD, outermost modifier only): for the single-required +
    // single-optional-predicate shape, drive from the smaller side and seek the
    // other instead of scanning both in full.
    if !ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
        if let Some(n) = try_optional_seek(required, optional_groups, ec)? {
            return Ok(Some(n));
        }
        // Both-large optional (no modifiers): partition the subject space and run
        // the optional merge across cores.
        if let Some(n) = sum_optional_join_parallel(required, optional_groups, ec)? {
            return Ok(Some(n));
        }
    }

    // Overlay/time-travel optional: parallelize the base scan and fold novelty per
    // partition (bounded overlay cursors). Falls through to the serial cursor merge
    // for small inputs, absent predicates, or translation failures.
    if ec.overlay && exclude_sorted.is_none() && include_sorted.is_none() {
        if let Some(n) = sum_optional_join_overlay_parallel(required, optional_groups, ec)? {
            return Ok(Some(n));
        }
    }

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
    match node {
        KeySetNode::SubjectsSorted { pred } | KeySetNode::SubjectSet { pred } => {
            subject_keys_sorted(ec, pred)
        }
        KeySetNode::SubjectsWithObjectIn { pred, object_set } => {
            // Need a hash set for the object filter, then sort the result.
            let obj_set = match execute_keyset_as_hash_set(object_set, ec)? {
                Some(s) => s,
                None => return Ok(None),
            };
            let Some(mut subjects) = subjects_with_object_in(ec, pred, &obj_set)? else {
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
    cancellation: &QueryCancellation,
) -> Result<Option<u64>> {
    // Matches the overlay twin `execute_chain_overlay`: a malformed chain is a
    // planner bug, not a runtime condition — never panic a release query path.
    debug_assert!(
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
                            cache,
                            fluree_db_binary_index::read::column_loader::LeafletDecodeSpec {
                                leaf_id: handle.leaf_id(),
                                leaflet_idx: idx_u32,
                                order: RunSortOrder::Psot,
                                decode_set: fluree_db_binary_index::ColumnSet::ALL,
                            },
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
                let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?
                    .with_cancellation(cancellation);
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
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?
                        .with_cancellation(cancellation);
                    while let Some((c, count)) = iter.next_group()? {
                        mult_map.insert(c, count.max(1));
                    }
                    let Some(mut ws_iter) =
                        PsotSubjectWeightedSumIter::new(store, g_id, p_ids[n - 1], &mult_map, 1)?
                            .map(|it| it.with_cancellation(cancellation))
                    else {
                        return Ok(None);
                    };
                    while let Some((s, sum)) = ws_iter.next_group()? {
                        if sum > 0 {
                            v2_weights.insert(s, sum);
                        }
                    }
                } else {
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?
                        .with_cancellation(cancellation);
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
                    let mut iter = PsotSubjectCountIter::new(store, g_id, p_ids[n - 1])?
                        .with_cancellation(cancellation);
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
                    .map(|it| it.with_cancellation(cancellation))
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
                    let mut iter = PsotSubjectCountIter::new(store, g_id, tail_p_id)?
                        .with_cancellation(cancellation);
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
// Chain evaluation — overlay lane
// ---------------------------------------------------------------------------

/// Per-object weight for one chain hop. Mirrors the metadata `P2WeightMode`
/// semantics exactly (verified against `PsotSubjectCountIter`,
/// `PsotSubjectWeightedSumIter`, and `PsotObjectFilterCountIter`):
/// - `CountEdges`: every edge weighs 1 (rightmost `TailWeight::None`).
/// - `Lookup`: ref object → `weights.get(o_key).unwrap_or(default)`; non-ref → `default`.
/// - `InSet`: ref object in set → 1, else 0; non-ref → 0 (EXISTS tail).
/// - `NotInSet`: ref object not in set → 1, else 0; non-ref → 1 (MINUS tail).
enum ChainWeight<'a> {
    CountEdges,
    Lookup {
        weights: &'a FxHashMap<u64, u64>,
        default: u64,
    },
    /// `max(1, weights.get(o))` for ref objects, `1` for non-ref — the
    /// OPTIONAL-chain head multiplier. `?b` keeps its solution even with no
    /// inner chain; a non-node `?b` (literal) can never be a `p2` subject, so
    /// its inner chain has 0 completions ⇒ OPTIONAL multiplier 1 (not 0).
    LookupMaxOne {
        weights: &'a FxHashMap<u64, u64>,
    },
    InSet {
        set: &'a FxHashSet<u64>,
    },
    NotInSet {
        set: &'a FxHashSet<u64>,
    },
}

impl ChainWeight<'_> {
    #[inline]
    fn weight(&self, o_type: u16, o_key: u64) -> u64 {
        let is_iri = o_type == OType::IRI_REF.as_u16();
        match self {
            ChainWeight::CountEdges => 1,
            ChainWeight::Lookup { weights, default } => {
                if is_iri {
                    weights.get(&o_key).copied().unwrap_or(*default)
                } else {
                    *default
                }
            }
            ChainWeight::LookupMaxOne { weights } => {
                if is_iri {
                    weights.get(&o_key).copied().unwrap_or(0).max(1)
                } else {
                    // Non-node `?b` can't extend the chain, but the OPTIONAL
                    // still keeps its `?a p1 ?b` solution: multiplier 1.
                    1
                }
            }
            ChainWeight::InSet { set } => u64::from(is_iri && set.contains(&o_key)),
            ChainWeight::NotInSet { set } => {
                if is_iri {
                    u64::from(!set.contains(&o_key))
                } else {
                    1
                }
            }
        }
    }
}

/// Stream PSOT(`pred`) from the overlay-merging cursor and return a map
/// `subject_id -> Σ weight(o)` (non-zero only). `Ok(None)` bails the plan.
fn psot_weighted_subject_sums(
    ec: &ExecCtx<'_, '_>,
    pred_sid: &Sid,
    p_id: u32,
    weight: &ChainWeight<'_>,
) -> Result<Option<FxHashMap<u64, u64>>> {
    let Some(mut cursor) = build_psot_cursor_for_predicate(
        ec.ctx,
        ec.store,
        ec.g_id,
        pred_sid.clone(),
        p_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    let mut out: FxHashMap<u64, u64> = FxHashMap::default();
    let mut cur_s: Option<u64> = None;
    let mut cur_sum: u64 = 0;
    let overflow = || QueryError::execution("COUNT(*) overflow in chain join");
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?
    {
        for row in 0..batch.row_count {
            let s = batch.s_id.get(row);
            let w = weight.weight(batch.o_type.get(row), batch.o_key.get(row));
            if cur_s == Some(s) {
                cur_sum = cur_sum.checked_add(w).ok_or_else(overflow)?;
            } else {
                if let Some(cs) = cur_s {
                    if cur_sum > 0 {
                        out.insert(cs, cur_sum);
                    }
                }
                cur_s = Some(s);
                cur_sum = w;
            }
        }
    }
    if let Some(cs) = cur_s {
        if cur_sum > 0 {
            out.insert(cs, cur_sum);
        }
    }
    Ok(Some(out))
}

/// Stream PSOT(`pred`) from the overlay-merging cursor and return `Σ weight(o)`
/// over all rows (ungrouped). `Ok(None)` bails the plan.
fn psot_weighted_total(
    ec: &ExecCtx<'_, '_>,
    pred_sid: &Sid,
    p_id: u32,
    weight: &ChainWeight<'_>,
) -> Result<Option<u64>> {
    let Some(mut cursor) = build_psot_cursor_for_predicate(
        ec.ctx,
        ec.store,
        ec.g_id,
        pred_sid.clone(),
        p_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    let mut total: u64 = 0;
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("count-plan cursor batch: {e}")))?
    {
        for row in 0..batch.row_count {
            let w = weight.weight(batch.o_type.get(row), batch.o_key.get(row));
            total = total
                .checked_add(w)
                .ok_or_else(|| QueryError::execution("COUNT(*) overflow in chain join"))?;
        }
    }
    Ok(Some(total))
}

/// Overlay set of subjects for a predicate. `Ok(None)` bails the plan.
fn subject_set(ec: &ExecCtx<'_, '_>, pred: &Ref) -> Result<Option<FxHashSet<u64>>> {
    match subject_keys_sorted(ec, pred)? {
        Some(v) => Ok(Some(v.into_iter().collect())),
        None => Ok(None),
    }
}

/// Build the rightmost chain weights (keyed by subjects of `pN`) under overlay,
/// applying the tail modifier. `Ok(None)` bails the plan.
fn build_chain_rightmost_overlay(
    ec: &ExecCtx<'_, '_>,
    tail: &TailWeight,
    pn_sid: &Sid,
    pn_id: u32,
) -> Result<Option<FxHashMap<u64, u64>>> {
    match tail {
        TailWeight::None => psot_weighted_subject_sums(ec, pn_sid, pn_id, &ChainWeight::CountEdges),
        TailWeight::Optional { tail_pred } => {
            // mult_map[c] = max(1, count_tail(c)); objects with no tail edge → 1.
            let Some(mut groups) = subject_groups(ec, tail_pred)? else {
                return Ok(None);
            };
            let mut mult: FxHashMap<u64, u64> = FxHashMap::default();
            while let Some((c, count)) = groups.next_group()? {
                mult.insert(c, count.max(1));
            }
            psot_weighted_subject_sums(
                ec,
                pn_sid,
                pn_id,
                &ChainWeight::Lookup {
                    weights: &mult,
                    default: 1,
                },
            )
        }
        TailWeight::Minus { tail_pred } => {
            let Some(set) = subject_set(ec, tail_pred)? else {
                return Ok(None);
            };
            psot_weighted_subject_sums(ec, pn_sid, pn_id, &ChainWeight::NotInSet { set: &set })
        }
        TailWeight::Exists { tail_pred } => {
            let Some(set) = subject_set(ec, tail_pred)? else {
                return Ok(None);
            };
            if set.is_empty() {
                return Ok(Some(FxHashMap::default()));
            }
            psot_weighted_subject_sums(ec, pn_sid, pn_id, &ChainWeight::InSet { set: &set })
        }
    }
}

/// Overlay lane for a chain fold. Equivalent to the metadata `execute_chain`
/// but built on overlay-merging PSOT cursors: a uniform right-to-left fold of
/// per-hop weight maps (no `PsotSeekSumCursor`).
///
/// `comp[v_k]` = number of chain completions from `v_k` through `p_{k+1}..pN`
/// (+ tail). Built rightmost-first, folded through `p_{N-1}..p2`, then summed
/// over `p1`'s edges. `Ok(None)` bails to the generic fallback (a predicate is
/// absent from the base index while novelty is present, or an overlay flake
/// failed to translate).
fn execute_chain_overlay(chain: &ChainFold, ec: &ExecCtx<'_, '_>) -> Result<Option<u64>> {
    let n = chain.predicates.len();
    debug_assert!(n >= 2, "chain must have at least 2 predicates");

    // Resolve predicates. An absent predicate may carry overlay-only rows a
    // p_id cursor can't reach, so bail rather than undercount.
    let mut p_sids: Vec<Sid> = Vec::with_capacity(n);
    let mut p_ids: Vec<u32> = Vec::with_capacity(n);
    for pred in &chain.predicates {
        let sid = normalize_pred_sid(ec.store, pred)?;
        let Some(p_id) = ec.store.sid_to_p_id(&sid) else {
            return Ok(None);
        };
        p_sids.push(sid);
        p_ids.push(p_id);
    }

    // Rightmost weights keyed by subjects of pN (= v_{N-1}).
    let Some(mut comp) =
        build_chain_rightmost_overlay(ec, &chain.tail_weight, &p_sids[n - 1], p_ids[n - 1])?
    else {
        return Ok(None);
    };
    if comp.is_empty() {
        return Ok(Some(0));
    }

    // Fold p_{N-1} … p2 (indices n-2 … 1). Empty range for n == 2.
    for idx in (1..=n.saturating_sub(2)).rev() {
        let new_comp = {
            let mode = ChainWeight::Lookup {
                weights: &comp,
                default: 0,
            };
            match psot_weighted_subject_sums(ec, &p_sids[idx], p_ids[idx], &mode)? {
                Some(m) => m,
                None => return Ok(None),
            }
        };
        comp = new_comp;
        if comp.is_empty() {
            return Ok(Some(0));
        }
    }

    // Head: Σ over p1 edges of comp[object].
    let head_mode = ChainWeight::Lookup {
        weights: &comp,
        default: 0,
    };
    psot_weighted_total(ec, &p_sids[0], p_ids[0], &head_mode)
}

// ---------------------------------------------------------------------------
// Optional chain-head: `?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d }`
// total = Σ_b count_p1(b) × max(1, Σ_{c ∈ p2(b)} count_p3(c))
// ---------------------------------------------------------------------------

/// True iff every stored object of predicate `p_id` is an IRI reference.
///
/// Cheap directory prepass (no row decode): inspects each POST leaflet's
/// `o_type_const`. A `None` (mixed leaflet) or any non-`IRI_REF` homogeneous
/// type disqualifies. An empty predicate is vacuously all-IRI (count 0).
fn predicate_objects_all_iri(store: &BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<bool> {
    let iri_ref = OType::IRI_REF.as_u16();
    for leaf_entry in leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id) {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        for entry in &handle.dir().entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(iri_ref) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Metadata lane — consolidates the former `fast_optional_chain_head_count_all`,
/// parameterized over `ExecCtx`. Runs only when `!ec.overlay`. Differs from the
/// old standalone operator by (a) deferring non-all-IRI `p1` to the generic
/// pipeline and (b) treating an absent `p2` (not just `p3`) as "inner chain
/// never matches ⇒ multiplier 1".
fn execute_optional_chain_head(
    ec: &ExecCtx<'_, '_>,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let store = ec.store;
    let g_id = ec.g_id;
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        // No `p1` data at all ⇒ no `?a p1 ?b` solutions.
        return Ok(Some(0));
    };

    // This lane drives the IRI-only `PostObjectGroupCountIter`, which terminates
    // on a homogeneous non-IRI leaflet (and POST orders such leaflets before
    // `IRI_REF`). A literal-valued `?b` still survives the OPTIONAL with
    // multiplier 1, so rather than undercount we defer any non-all-IRI `p1` to
    // the generic pipeline.
    if !predicate_objects_all_iri(store, g_id, p1_id)? {
        return Ok(None);
    }

    let p2_id = store.sid_to_p_id(&sid2);
    let p3_id = store.sid_to_p_id(&sid3);

    // If either inner predicate is absent, the OPTIONAL chain can never match,
    // so every `?a p1 ?b` row contributes exactly 1 (multiplier 1 for all b).
    let (Some(p2_id), Some(p3_id)) = (p2_id, p3_id) else {
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
        )?;
        let mut total = 0u64;
        while let Some((_b, w)) = it1.next_group()? {
            total += w;
        }
        return Ok(Some(total));
    };

    // Precompute n3(c) = count_{p3}(c).
    let mut n3: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 =
        PsotSubjectCountIter::new(store, g_id, p3_id)?.with_cancellation(&ec.ctx.cancellation);
    while let Some((c, n)) = it3.next_group()? {
        n3.insert(c, n);
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
    )?;
    // default_weight=0: objects not in n3 contribute nothing to the sum
    let mut it2 = PsotSubjectWeightedSumIter::new(store, g_id, p2_id, &n3, 0)?
        .map(|it| it.with_cancellation(&ec.ctx.cancellation))
        .ok_or(QueryError::Internal(
            "optional chain-head: PSOT iterator unavailable".into(),
        ))?;

    let mut p2_cur = it2.next_group()?;
    let mut total = 0u64;

    while let Some((b, w)) = it1.next_group()? {
        while let Some((b2, _)) = p2_cur {
            if b2 < b {
                p2_cur = it2.next_group()?;
                continue;
            }
            break;
        }
        let sum_n3 = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        let mult = if sum_n3 == 0 { 1 } else { sum_n3 };
        total = total.saturating_add(w.saturating_mul(mult));
    }

    Ok(Some(total))
}

/// Overlay lane — reuses the chain weight-map primitives. `n3[c] = count_p3(c)`;
/// `comp2[b] = Σ_{c ∈ p2(b)} n3[c]`; total = Σ over p1 edges of `max(1, comp2[b])`
/// (the `LookupMaxOne` head). An absent predicate under novelty bails.
fn execute_optional_chain_head_overlay(
    ec: &ExecCtx<'_, '_>,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let s1 = normalize_pred_sid(ec.store, p1)?;
    let Some(id1) = ec.store.sid_to_p_id(&s1) else {
        return Ok(None);
    };
    let s2 = normalize_pred_sid(ec.store, p2)?;
    let Some(id2) = ec.store.sid_to_p_id(&s2) else {
        return Ok(None);
    };
    let s3 = normalize_pred_sid(ec.store, p3)?;
    let Some(id3) = ec.store.sid_to_p_id(&s3) else {
        return Ok(None);
    };

    let Some(n3) = psot_weighted_subject_sums(ec, &s3, id3, &ChainWeight::CountEdges)? else {
        return Ok(None);
    };
    let comp2 = {
        let mode = ChainWeight::Lookup {
            weights: &n3,
            default: 0,
        };
        match psot_weighted_subject_sums(ec, &s2, id2, &mode)? {
            Some(m) => m,
            None => return Ok(None),
        }
    };
    let head = ChainWeight::LookupMaxOne { weights: &comp2 };
    psot_weighted_total(ec, &s1, id1, &head)
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
    Meta(Box<PsotSoIter<'a>>),
    /// Boxed: embeds a `BinaryCursor`, much larger than the other variants.
    Cursor(Box<CursorSoIter>),
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
        Ok(Some(SoRows::Cursor(Box::new(CursorSoIter::new(cursor)))))
    } else {
        Ok(Some(SoRows::Meta(Box::new(PsotSoIter::new(
            store, ec.g_id, p_id,
        )))))
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
