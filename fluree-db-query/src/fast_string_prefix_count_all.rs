//! Fast-path: `COUNT(*)` for single-predicate string-prefix filters.
//!
//! Targets shapes like:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p> ?o FILTER REGEX(?o, "^Com") }`
//! and
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p> ?o FILTER STRSTARTS(?o, "Com") }`
//!
//! When the index root marks string IDs as lexicographically sorted, all string
//! dictionary IDs matching a given prefix form one or more contiguous ranges.
//! POST leaflets wholly inside those ranges are counted from their directories;
//! only boundary leaflets and leaflets touched by matching novelty are decoded.

use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    bail_if_cancelled, build_count_batch, build_i64_singleton_batch, build_range_cursor,
    cached_overlay_ops, charge_leaflet, contiguous_id_range, cursor_fast_path_for_predicate,
    load_leaflet_projection, normalize_pred_sid, overlay_has_novelty, FastPathOperator,
    PredicateFastPath,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf::{LeafletDirEntryV3, LiveEdges};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::read::types::{overlay_window_for_range, OverlayOp};
use fluree_db_binary_index::{BinaryFilter, ColumnProjection, ColumnSet};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::GraphId;
use std::sync::Arc;

pub fn string_prefix_count_all_operator(
    pred: Ref,
    prefix: Arc<str>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(count) = prefix_match_count(ctx, &pred, prefix.as_ref())? else {
                return Ok(None);
            };
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in string-prefix fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "string-prefix COUNT(*)",
    )
}

pub fn string_prefix_sum_strstarts_operator(
    pred: Ref,
    prefix: Arc<str>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(sum) = prefix_match_count(ctx, &pred, prefix.as_ref())? else {
                return Ok(None);
            };
            let sum_i64 = i64::try_from(sum).map_err(|_| {
                QueryError::execution("SUM exceeds i64 in string-prefix STRSTARTS fast-path")
            })?;
            Ok(Some(build_i64_singleton_batch(out_var, sum_i64, "sum")?))
        },
        fallback,
        "string-prefix SUM(STRSTARTS)",
    )
}

fn prefix_match_count(ctx: &ExecutionContext<'_>, pred: &Ref, prefix: &str) -> Result<Option<u64>> {
    let Some(store) = ctx.binary_store.as_ref() else {
        return Ok(None);
    };
    let pred_sid = normalize_pred_sid(store, pred)?;
    // The directories describe the indexed state, not a time-travel replay.
    // Novelty at or above it is reconciled below, with the same single-graph
    // and predicate-policy admission as the overlay-aware cursor lanes.
    if !ctx.has_binary_store()
        || ctx.to_t < store.max_t()
        || !store.lex_sorted_string_ids()
        || !matches!(
            cursor_fast_path_for_predicate(ctx, &pred_sid),
            PredicateFastPath::Allow
        )
    {
        return Ok(None);
    }
    let string_ids = store
        .find_strings_by_prefix(prefix)
        .map_err(|e| QueryError::Internal(format!("find_strings_by_prefix: {e}")))?;
    let Ok(id_ranges) = contiguous_id_range(&string_ids) else {
        return Ok(None);
    };

    // Resolved ops have one final state per full fact identity. New strings
    // have novelty IDs outside the lexicographic base range, so classify
    // their text separately rather than extending that range over them.
    let mut matching_ops = Vec::new();
    if overlay_has_novelty(ctx) {
        let Some(ops) =
            cached_overlay_ops(ctx, store, ctx.binary_g_id, RunSortOrder::Post, &pred_sid)?
        else {
            return Ok(None);
        };
        let mut previous = None;
        let mut matches = false;
        for op in ops.iter() {
            if op.o_type != OType::XSD_STRING.as_u16()
                && !OType::from_u16(op.o_type).is_lang_string()
            {
                continue;
            }
            if previous != Some(op.o_key) {
                let Ok(id) = u32::try_from(op.o_key) else {
                    return Ok(None);
                };
                matches = match ctx
                    .dict_novelty
                    .as_ref()
                    .and_then(|dn| dn.strings.resolve_string(id))
                {
                    Some(value) => value.starts_with(prefix),
                    None => id_ranges.iter().any(|&(lo, hi)| id >= lo && id <= hi),
                };
                previous = Some(op.o_key);
            }
            if matches {
                matching_ops.push(*op);
            }
        }
    }
    let asserted = matching_ops.iter().filter(|op| op.op).count() as u64;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        // This predicate exists only in novelty: no base row can overlap.
        return Ok(Some(asserted));
    };
    if store
        .dedicated_branch(ctx.binary_g_id, RunSortOrder::Post)
        .is_none()
    {
        // Keep the existing OPST lane for indexes configured without POST.
        return if matching_ops.is_empty() {
            count_prefix_rows_opst(
                store,
                ctx.binary_g_id,
                p_id,
                &id_ranges,
                ctx.to_t,
                &ctx.tracker,
            )
            .map(Some)
        } else {
            Ok(None)
        };
    }
    let counted = count_prefix_rows_post(ctx, store, p_id, &id_ranges, &matching_ops)?;
    // Start with every matching base row and novelty assertion. A base row
    // touched by either kind of op is removed once: a retraction deletes it;
    // a reassertion replaces the copy already included in `asserted`.
    // Unmatched retractions do nothing. No blind +/- delta can provide this.
    let total = counted
        .rows
        .checked_sub(counted.overlap)
        .and_then(|n| n.checked_add(asserted))
        .ok_or_else(|| QueryError::execution("COUNT(*) overflow in string-prefix scan"))?;
    tracing::debug!(
        metadata_leaflets = counted.metadata_leaflets,
        scanned_leaflets = counted.scanned_leaflets,
        novelty_ops = matching_ops.len(),
        "string-prefix block count"
    );
    Ok(Some(total))
}

#[derive(Default)]
struct PrefixRows {
    rows: u64,
    overlap: u64,
    metadata_leaflets: u64,
    scanned_leaflets: u64,
}

/// Object bounds within one POST predicate. Live edges stay exact even when
/// retained history widens routing keys. Legacy bounds are conservative;
/// if they cross predicates, only the row scan can settle this leaflet.
fn post_object_bounds(entry: &LeafletDirEntryV3, p_id: u32) -> Option<((u16, u64), (u16, u64))> {
    match entry.live_edges {
        LiveEdges::Exact { first, last } => Some((
            crate::fast_lead_walk::post_group_object(&first),
            crate::fast_lead_walk::post_group_object(&last),
        )),
        LiveEdges::Empty => None,
        LiveEdges::Legacy => {
            let first = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Post, &entry.last_key);
            (first.p_id == p_id && last.p_id == p_id)
                .then_some(((first.o_type, first.o_key), (last.o_type, last.o_key)))
        }
    }
}

fn count_prefix_rows_post(
    ctx: &ExecutionContext<'_>,
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    p_id: u32,
    id_ranges: &[(u32, u32)],
    ops: &[OverlayOp],
) -> Result<PrefixRows> {
    let order = RunSortOrder::Post;
    let branch = store
        .dedicated_branch(ctx.binary_g_id, order)
        .expect("POST checked above");
    let mut counted = PrefixRows::default();
    let types = std::iter::once(OType::XSD_STRING.as_u16())
        .chain((1..=store.language_tag_count()).map(|id| OType::lang_string(id).as_u16()));
    for o_type in types {
        for &(lo, hi) in id_ranges {
            let min = RunRecordV2 {
                s_id: SubjectId(0),
                o_key: u64::from(lo),
                p_id,
                t: 0,
                o_i: 0,
                o_type,
                g_id: ctx.binary_g_id,
            };
            let max = RunRecordV2 {
                s_id: SubjectId(u64::MAX),
                o_key: u64::from(hi),
                t: u32::MAX,
                o_i: LIST_INDEX_NONE,
                ..min
            };
            let (start, end) = overlay_window_for_range(ops, &min, &max, order);
            let range_ops = &ops[start..end];
            let leaves = branch.find_leaves_in_range(&min, &max, cmp_v2_for_order(order));
            for leaf in &branch.leaves[leaves] {
                bail_if_cancelled(&ctx.cancellation)?;
                let dir = store
                    .open_leaf_dir(&leaf.leaf_cid)
                    .map_err(|e| QueryError::from_io("leaf dir open", e))?;
                let mut handle = None;
                for (idx, entry) in dir.entries.iter().enumerate() {
                    if entry.row_count == 0
                        || entry.p_const != Some(p_id)
                        || entry.o_type_const.is_some_and(|ot| ot != o_type)
                    {
                        continue;
                    }
                    let lower = (o_type, u64::from(lo));
                    let upper = (o_type, u64::from(hi));
                    let bounds = post_object_bounds(entry, p_id);
                    if bounds.is_some_and(|(first, last)| last < lower || first > upper) {
                        continue;
                    }
                    // Locate ops by the full routing keys, including subject
                    // and list place; an object may span multiple leaflets.
                    let touched = if range_ops.is_empty() {
                        range_ops
                    } else {
                        let first = read_ordered_key_v2(order, &entry.first_key);
                        let last = read_ordered_key_v2(order, &entry.last_key);
                        let (start, end) =
                            overlay_window_for_range(range_ops, &first, &last, order);
                        &range_ops[start..end]
                    };
                    let whole = bounds.is_some_and(|(first, last)| first >= lower && last <= upper);
                    // Count every visited leaflet as an index touch, including
                    // a metadata answer, as the previous cursor lane did.
                    if whole {
                        counted.rows += u64::from(entry.row_count);
                        counted.metadata_leaflets += 1;
                        if touched.is_empty() {
                            charge_leaflet(&ctx.tracker)?;
                            continue;
                        }
                    }
                    if handle.is_none() {
                        handle = Some(
                            store
                                .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
                                .map_err(|e| QueryError::from_io("leaf open", e))?,
                        );
                    }
                    let mut columns = ColumnSet::single(ColumnId::OKey);
                    columns.insert(ColumnId::OType);
                    if !touched.is_empty() {
                        columns.insert(ColumnId::SId);
                        columns.insert(ColumnId::OI);
                    }
                    let batch = load_leaflet_projection(
                        store,
                        handle.as_deref().expect("opened above"),
                        idx,
                        &ColumnProjection {
                            output: columns,
                            internal: ColumnSet::EMPTY,
                        },
                        order,
                        &ctx.tracker,
                    )?;
                    counted.scanned_leaflets += 1;
                    counted.overlap += crate::fast_path_common::count_post_overlay_overlap(
                        &batch,
                        entry.o_type_const,
                        touched,
                    );
                    if !whole {
                        for row in 0..batch.row_count {
                            let ot = batch.o_type.get(row);
                            let key = batch.o_key.get(row);
                            if ot == o_type && key >= u64::from(lo) && key <= u64::from(hi) {
                                counted.rows += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(counted)
}

fn count_prefix_rows_opst(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    id_ranges: &[(u32, u32)],
    to_t: i64,
    tracker: &fluree_db_core::Tracker,
) -> Result<u64> {
    let projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: ColumnSet::single(ColumnId::OKey),
    };

    let mut total = 0u64;
    let mut string_otypes = Vec::with_capacity(store.language_tag_count() as usize + 1);
    string_otypes.push(OType::XSD_STRING.as_u16());
    for lang_id in 1..=store.language_tag_count() {
        string_otypes.push(OType::lang_string(lang_id).as_u16());
    }

    for &o_type in &string_otypes {
        for &(start_id, end_id) in id_ranges {
            let min_key = RunRecordV2 {
                s_id: SubjectId(0),
                o_key: start_id as u64,
                p_id,
                t: 0,
                o_i: 0,
                o_type,
                g_id,
            };
            let max_key = RunRecordV2 {
                s_id: SubjectId(u64::MAX),
                o_key: end_id as u64,
                p_id,
                t: u32::MAX,
                o_i: LIST_INDEX_NONE,
                o_type,
                g_id,
            };
            let filter = BinaryFilter {
                p_id: Some(p_id),
                o_type: Some(o_type),
                ..Default::default()
            };
            let Some(cursor) = build_range_cursor(
                store,
                g_id,
                RunSortOrder::Opst,
                &min_key,
                &max_key,
                filter,
                projection,
            ) else {
                return Ok(total);
            };
            let mut cursor = cursor.with_tracker(tracker.clone());
            cursor.set_to_t(to_t);

            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::from_io("binary cursor", e))?
            {
                let mut batch_matches = 0u64;
                for row_idx in 0..batch.row_count {
                    let o_key = batch.o_key.get_or(row_idx, 0);
                    if o_key >= u64::from(start_id) && o_key <= u64::from(end_id) {
                        batch_matches += 1;
                    }
                }
                total = total.checked_add(batch_matches).ok_or_else(|| {
                    QueryError::execution("COUNT(*) overflow in string-prefix scan")
                })?;
            }
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_path_common::contiguous_id_range;
    use fluree_db_binary_index::format::leaflet::LiveGroup;
    use fluree_db_binary_index::format::run_record_v2::{
        write_ordered_key_v2, ORDERED_KEY_V2_SIZE,
    };

    #[test]
    fn post_bounds_use_live_groups_and_conservative_legacy_keys() {
        let ot = OType::XSD_STRING.as_u16();
        let key = |p_id, o_key| {
            let row = RunRecordV2 {
                s_id: SubjectId(1),
                p_id,
                o_key,
                o_type: ot,
                t: 1,
                g_id: 0,
                o_i: LIST_INDEX_NONE,
            };
            let mut key = [0; ORDERED_KEY_V2_SIZE];
            write_ordered_key_v2(RunSortOrder::Post, &row, &mut key);
            key
        };
        // History stretches the routing range to 1..100, but the only live
        // objects are 10..20: a prefix covering them can count row_count.
        let mut entry = LeafletDirEntryV3 {
            row_count: 11,
            lead_group_count: 11,
            first_key: key(2, 1),
            last_key: key(2, 100),
            live_edges: LiveEdges::Exact {
                first: LiveGroup::of_key(RunSortOrder::Post, &key(2, 10)),
                last: LiveGroup::of_key(RunSortOrder::Post, &key(2, 20)),
            },
            p_const: Some(2),
            o_type_const: Some(ot),
            flags: 0,
            payload_offset: 0,
            payload_len: 0,
            column_refs: Vec::new(),
            history_offset: 0,
            history_len: 0,
            history_min_t: 0,
            history_max_t: 0,
        };
        assert_eq!(post_object_bounds(&entry, 2), Some(((ot, 10), (ot, 20))));
        entry.live_edges = LiveEdges::Legacy;
        assert_eq!(post_object_bounds(&entry, 2), Some(((ot, 1), (ot, 100))));
        // Legacy history may route a predicate boundary into its neighbor.
        entry.first_key = key(1, 900);
        assert_eq!(post_object_bounds(&entry, 2), None);
        entry.live_edges = LiveEdges::Empty;
        assert_eq!(post_object_bounds(&entry, 2), None);
    }

    #[test]
    fn prefix_ids_accept_single_contiguous_run() {
        let ranges = contiguous_id_range(&[10, 11, 12]).expect("contiguous ids");
        assert_eq!(ranges, vec![(10, 12)]);
    }

    #[test]
    fn prefix_ids_accept_reordered_runs_and_u32_boundaries() {
        for (ids, expected) in [
            (vec![], vec![]),
            (vec![0], vec![(0, 0)]),
            (vec![12, 10, 13, 11], vec![(10, 13)]),
            (
                vec![u32::MAX, u32::MAX - 2, u32::MAX - 1],
                vec![(u32::MAX - 2, u32::MAX)],
            ),
        ] {
            assert_eq!(contiguous_id_range(&ids).unwrap(), expected);
        }
        assert!(contiguous_id_range(&[u32::MAX, 0]).is_err());
    }

    #[test]
    fn prefix_ids_reject_gapped_runs() {
        let err = contiguous_id_range(&[10, 12]).expect_err("gapped ids should reject");
        assert!(
            err.to_string().contains("not contiguous"),
            "unexpected error: {err}"
        );
    }
}
