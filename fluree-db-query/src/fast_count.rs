//! Consolidated fast-path COUNT operators.
//!
//! This module groups the `fast_count_*` family into one place to reduce sprawl.
//! All operators here emit a single-row count batch via `FastPathOperator`
//! when `fast_path_store(ctx)` is available, otherwise they fall back to a planned
//! operator tree for correctness.

use crate::binary_scan::{compile_encoded_pre_filters_and_prune_inline_ops, EncodedPreFilter};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_count_batch, count_predicate_overlay_delta,
    count_rows_for_predicate_psot, count_to_i64, fast_path_store, leaf_entries_for_predicate,
    normalize_pred_sid, parallel_leaf_chunk_count, parallel_overlay_psot_filter_count,
    projection_okey_only, projection_otype_only, projection_sid_only, projection_sid_otype_okey,
    FastPathOperator,
};
use crate::ir::triple::{Ref, TriplePattern};
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::{FlakeValue, GraphId};
use fluree_vocab::namespaces;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// 1) COUNT(*) / COUNT(?x) for single predicate `?s <p> ?o`
// ---------------------------------------------------------------------------

/// Fast-path: `COUNT(*)` / `COUNT(?x)` for a single triple `?s <p> ?o`.
pub fn count_rows_operator(
    predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            // HEAD: metadata-only predicate row count (instant).
            if let Some(store) = fast_path_store(ctx) {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                let count = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
                return Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(count, "COUNT rows")?,
                )?));
            }
            // Novelty at HEAD (no time-travel): metadata base count + a novelty delta
            // that rescans only the leaves novelty touches, instead of the whole
            // predicate. Time-travel (to_t < max_t) needs base replay — defer.
            if allow_cursor_fast_path(ctx) {
                if let Some(store) = ctx.binary_store.as_ref() {
                    // to_t >= max_t: at-or-above the indexed head (novelty folds on
                    // top). Time-travel BELOW max_t needs base replay — defer.
                    if ctx.to_t >= store.max_t() {
                        let pred_sid = normalize_pred_sid(store, &predicate)?;
                        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                            return Ok(None); // novelty-only predicate => defer
                        };
                        if let Some(count) = count_predicate_overlay_delta(
                            ctx,
                            store,
                            ctx.binary_g_id,
                            pred_sid,
                            p_id,
                        )? {
                            return Ok(Some(build_count_batch(
                                out_var,
                                count_to_i64(count, "COUNT rows")?,
                            )?));
                        }
                    }
                }
            }
            Ok(None)
        },
        fallback,
        "COUNT rows",
    )
}

/// Fast-path: `COUNT(*)` of `?s rdf:type <Class> . ?s P ?o` — the number of `P`
/// flakes on instances of a single bound class. Answered from the per-(class,
/// property) stats: `classStat[g][Class][P]` = Σ of its datatype counts (which
/// already include the reference total via the `JSON_LD_ID` tag). One metadata
/// lookup, no scan/join.
///
/// The per-class datatype stats are current-state-exact on any base index (#1266),
/// so this runs on bulk and incremental indexes alike. HEAD-only (via
/// [`fast_path_store`]); under overlay it defers to the fallback (stats exclude
/// uncommitted novelty). `COUNT(DISTINCT …)` is a different statistic (distinct
/// subjects/objects, not the flake count) and is excluded by the detector. Returns
/// `Ok(None)` to defer when the bound-object leg isn't rdf:type, stats are absent,
/// or the count is 0 (a genuinely-empty join the general path confirms cheaply).
pub fn class_property_count_operator(
    type_pred: Ref,
    class_obj: Ref,
    property: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            // The bound-object leg must be rdf:type for the class stats to apply.
            let type_sid = normalize_pred_sid(store, &type_pred)?;
            if !fluree_db_core::is_rdf_type(&type_sid) {
                return Ok(None);
            }
            let class_sid = normalize_pred_sid(store, &class_obj)?;
            let property_sid = normalize_pred_sid(store, &property)?;

            let Some(stats) = ctx.active_snapshot.stats.as_ref() else {
                return Ok(None);
            };
            let Some(graphs) = stats.graphs.as_ref() else {
                return Ok(None);
            };
            let Some(graph) = graphs.iter().find(|g| g.g_id == ctx.binary_g_id) else {
                return Ok(None);
            };
            let Some(classes) = graph.classes.as_ref() else {
                return Ok(None);
            };
            let total: u64 = classes
                .iter()
                .find(|c| c.class_sid == class_sid)
                .and_then(|c| c.properties.iter().find(|p| p.property_sid == property_sid))
                .map(|p| p.datatypes.iter().map(|&(_dt, c)| c).sum())
                .unwrap_or(0);
            if total == 0 {
                // Genuinely-empty (or stats-incomplete) — let the general path confirm.
                return Ok(None);
            }
            Ok(Some(build_count_batch(
                out_var,
                count_to_i64(total, "COUNT class-property")?,
            )?))
        },
        fallback,
        "COUNT class-property",
    )
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NumericCompareOp {
    Gt,
    Ge,
    Lt,
    Le,
}

/// Fast-path: `COUNT(?s)` / `COUNT(*)` for a single triple `?s <p> ?o`
/// with a single numeric comparison filter on `?o`.
pub fn count_rows_numeric_compare_operator(
    predicate: Ref,
    compare: NumericCompareOp,
    threshold: FlakeValue,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            // HEAD lane: directory shortcut + binary-search over base POST leaflets.
            if let Some(store) = fast_path_store(ctx) {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                let count = count_rows_for_predicate_numeric_compare_post(
                    store,
                    ctx.binary_g_id,
                    p_id,
                    compare,
                    &threshold,
                )?;
                return match count {
                    Some(count) => Ok(Some(build_count_batch(
                        out_var,
                        count_to_i64(count, "COUNT rows numeric compare")?,
                    )?)),
                    None => Ok(None),
                };
            }
            // Overlay / time-travel lane: per-row compare over the merged cursor.
            if allow_cursor_fast_path(ctx) {
                if let Some(store) = ctx.binary_store.as_ref() {
                    let pred_sid = normalize_pred_sid(store, &predicate)?;
                    // Absent in base => COUNT is 0 only if also absent in novelty; a
                    // novelty-only predicate has no base id, so defer to the fallback.
                    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                        return Ok(None);
                    };
                    if let Some(matches) = count_numeric_compare_overlay_parallel(
                        ctx,
                        store,
                        ctx.binary_g_id,
                        pred_sid,
                        p_id,
                        compare,
                        &threshold,
                    )? {
                        return Ok(Some(build_count_batch(
                            out_var,
                            count_to_i64(matches, "COUNT rows numeric compare")?,
                        )?));
                    }
                }
            }
            Ok(None)
        },
        fallback,
        "COUNT rows numeric compare",
    )
}

/// Fast-path: `SUM(?o <cmp> K)` over a single triple `?s <p> ?o`.
///
/// `SUM` of a boolean comparison is algebraically `COUNT` of the rows where the
/// comparison holds (true→1, false→0), so this reuses the directory-skipping
/// numeric-compare count.
///
/// SEMANTICS GUARD: `SUM` over an empty multiset is **Unbound** (SPARQL), whereas
/// `COUNT` would be `0`. The two diverge only when the predicate feeds the
/// aggregate *no rows at all* (absent predicate, or every row retracted). In that
/// case we return `Ok(None)` to defer to the general aggregate pipeline, which
/// emits the correct Unbound result. When at least one row exists, `SUM(?o cmp K)`
/// == `COUNT(rows where ?o cmp K)` exactly (a non-empty input with zero matches
/// sums to bound `0`, which `COUNT` also yields).
pub fn sum_compare_as_count_operator(
    predicate: Ref,
    compare: NumericCompareOp,
    threshold: FlakeValue,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            // HEAD lane.
            if let Some(store) = fast_path_store(ctx) {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    // Absent predicate => empty input => SUM is Unbound (not 0).
                    return Ok(None);
                };
                // Empty input (all rows retracted) => SUM is Unbound; defer to fallback.
                let total = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
                if total == 0 {
                    return Ok(None);
                }
                let count = count_rows_for_predicate_numeric_compare_post(
                    store,
                    ctx.binary_g_id,
                    p_id,
                    compare,
                    &threshold,
                )?;
                return match count {
                    Some(count) => Ok(Some(build_count_batch(
                        out_var,
                        count_to_i64(count, "SUM(compare) as count")?,
                    )?)),
                    None => Ok(None),
                };
            }
            // Overlay / time-travel lane: the merged cursor count gives both the
            // matches and the total rows; an empty (base+novelty) input is SUM
            // Unbound, so defer to the fallback which emits Unbound.
            if allow_cursor_fast_path(ctx) {
                if let Some(store) = ctx.binary_store.as_ref() {
                    let pred_sid = normalize_pred_sid(store, &predicate)?;
                    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                        return Ok(None);
                    };
                    if let Some(matches) = count_numeric_compare_overlay_parallel(
                        ctx,
                        store,
                        ctx.binary_g_id,
                        pred_sid,
                        p_id,
                        compare,
                        &threshold,
                    )? {
                        // matches > 0 ⇒ the input is non-empty ⇒ SUM is the bound
                        // count. matches == 0 can't distinguish empty (Unbound) from
                        // non-empty-with-no-matches (bound 0), so defer to the
                        // fallback, which resolves the SPARQL semantics correctly.
                        if matches > 0 {
                            return Ok(Some(build_count_batch(
                                out_var,
                                count_to_i64(matches, "SUM(compare) as count")?,
                            )?));
                        }
                    }
                }
            }
            Ok(None)
        },
        fallback,
        "SUM(compare) as count",
    )
}

fn count_rows_for_predicate_numeric_compare_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    compare: NumericCompareOp,
    threshold: &FlakeValue,
) -> Result<Option<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);

    // Global metadata shortcut: POST sorts by (p_id, o_type, o_key), so the
    // predicate's whole o_key extent is [global_min, global_max]. If that extent
    // lies entirely on one side of the threshold, the answer is `total` (all match)
    // or `0` (all excluded) with no per-leaflet scan — opening at most the ≤2
    // boundary leaves whose manifest key belongs to an adjacent predicate. For
    // `?o > 0` over an all-positive predicate this collapses the whole scan to a
    // manifest read.
    if let Some((min_ot, min_ok, max_ot, max_ok)) =
        predicate_post_global_extent(store, p_id, leaves)?
    {
        // Same o_type at both ends ⇒ uniform o_type (POST sorts o_type before o_key).
        if min_ot == max_ot {
            let otype = OType::from_u16(min_ot);
            if matches!(otype, OType::XSD_INTEGER | OType::XSD_DOUBLE) {
                if let Some(threshold_key) = encode_numeric_threshold_for_otype(otype, threshold)? {
                    if leaflet_fully_excluded(compare, min_ok, max_ok, threshold_key) {
                        return Ok(Some(0));
                    }
                    if leaflet_fully_matches(compare, min_ok, max_ok, threshold_key) {
                        return Ok(Some(count_rows_for_predicate_psot(store, g_id, p_id)?));
                    }
                }
            }
        }
    }

    // Partial overlap (or mixed/non-numeric o_type): each POST leaflet is
    // type-homogeneous and counted independently, so the leaf slice is split across
    // cores (the per-leaflet reducer is stateless). The partition decision uses the
    // predicate's total row count.
    let total_rows = count_rows_for_predicate_psot(store, g_id, p_id)?;
    parallel_leaf_chunk_count(leaves, total_rows, |chunk| {
        count_numeric_compare_in_leaf_slice(store, p_id, compare, threshold, chunk)
    })
}

/// The predicate's global `(min_o_type, min_o_key, max_o_type, max_o_key)` in POST
/// order — the extent of its rows. Read straight from the branch manifest
/// (`LeafEntry.first_key`/`last_key` are decoded `RunRecordV2`), opening only the
/// ≤2 boundary leaves whose manifest key belongs to an adjacent predicate (so their
/// per-leaflet directory must be consulted for this predicate's first/last key).
/// Returns `None` if there are no leaves (an empty predicate — the caller's total is
/// 0) or, defensively, if a boundary leaf yields no matching leaflet.
fn predicate_post_global_extent(
    store: &BinaryIndexStore,
    p_id: u32,
    leaves: &[LeafEntry],
) -> Result<Option<(u16, u64, u16, u64)>> {
    let (Some(first_leaf), Some(last_leaf)) = (leaves.first(), leaves.last()) else {
        return Ok(None);
    };

    // Global minimum: the first p_id row in POST order. When the first leaf already
    // starts at this predicate the manifest key is it; otherwise the leaf opens with
    // an earlier predicate and we read its first p_id leaflet's first key.
    let (min_ot, min_ok) = if first_leaf.first_key.p_id == p_id {
        (first_leaf.first_key.o_type, first_leaf.first_key.o_key)
    } else {
        match boundary_leaf_pid_extent(store, p_id, first_leaf, false)? {
            Some(v) => v,
            None => return Ok(None),
        }
    };

    // Global maximum: the last p_id row in POST order.
    let (max_ot, max_ok) = if last_leaf.last_key.p_id == p_id {
        (last_leaf.last_key.o_type, last_leaf.last_key.o_key)
    } else {
        match boundary_leaf_pid_extent(store, p_id, last_leaf, true)? {
            Some(v) => v,
            None => return Ok(None),
        }
    };

    Ok(Some((min_ot, min_ok, max_ot, max_ok)))
}

/// Open a boundary leaf and read the `(o_type, o_key)` of this predicate's first
/// (`last = false`) or last (`last = true`) row, from the matching leaflet directory
/// entry. Returns `None` if no leaflet in the leaf belongs to `p_id`.
fn boundary_leaf_pid_extent(
    store: &BinaryIndexStore,
    p_id: u32,
    leaf: &LeafEntry,
    last: bool,
) -> Result<Option<(u16, u64)>> {
    let handle = store
        .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
    let entries = &handle.dir().entries;
    let mut indices = (0..entries.len()).collect::<Vec<_>>();
    if last {
        indices.reverse();
    }
    for i in indices {
        let entry = &entries[i];
        if entry.row_count == 0 || entry.p_const != Some(p_id) {
            continue;
        }
        let raw = if last {
            &entry.last_key
        } else {
            &entry.first_key
        };
        let key = read_ordered_key_v2(RunSortOrder::Post, raw);
        return Ok(Some((key.o_type, key.o_key)));
    }
    Ok(None)
}

/// Count rows of `p_id` matching `?o <compare> threshold` over one contiguous slice
/// of POST leaves. Each leaflet is type-homogeneous (`o_type_const`), so the
/// threshold is encoded per-leaflet for that leaflet's numeric otype — this both
/// removes the cross-leaflet shared state (making the slice safe to run on its own
/// thread) and correctly handles a predicate carrying both XSD_INTEGER and
/// XSD_DOUBLE objects (POST sorts by o_type, so int and double leaflets are
/// disjoint and each is counted against its own encoded threshold). Returns
/// `Ok(None)` if any leaflet is non-numeric or the threshold can't be encoded for
/// its otype — deferring the whole count to the general aggregate pipeline.
fn count_numeric_compare_in_leaf_slice(
    store: &BinaryIndexStore,
    p_id: u32,
    compare: NumericCompareOp,
    threshold: &FlakeValue,
    leaves: &[LeafEntry],
) -> Result<Option<u64>> {
    let projection = projection_okey_only();
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let Some(raw_otype) = entry.o_type_const else {
                return Ok(None);
            };
            let otype = OType::from_u16(raw_otype);
            if !matches!(otype, OType::XSD_INTEGER | OType::XSD_DOUBLE) {
                return Ok(None);
            }
            let threshold_key = match encode_numeric_threshold_for_otype(otype, threshold)? {
                Some(key) => key,
                None => return Ok(None),
            };

            let first = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Post, &entry.last_key);

            if first.o_type != raw_otype || last.o_type != raw_otype {
                return Ok(None);
            }

            if leaflet_fully_matches(compare, first.o_key, last.o_key, threshold_key) {
                total = total.saturating_add(entry.row_count as u64);
                continue;
            }
            if leaflet_fully_excluded(compare, first.o_key, last.o_key, threshold_key) {
                continue;
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            total = total.saturating_add(count_matching_rows_in_sorted_batch(
                &batch,
                compare,
                threshold_key,
            ) as u64);
        }
    }

    Ok(Some(total))
}

fn encode_numeric_threshold_for_otype(otype: OType, threshold: &FlakeValue) -> Result<Option<u64>> {
    let key = match (otype, threshold) {
        (OType::XSD_INTEGER, FlakeValue::Long(n)) => ObjKey::encode_i64(*n).as_u64(),
        (OType::XSD_DOUBLE, FlakeValue::Long(n)) => ObjKey::encode_f64(*n as f64)
            .map_err(|_| QueryError::execution("cannot encode f64 threshold".to_string()))?
            .as_u64(),
        (OType::XSD_DOUBLE, FlakeValue::Double(d)) => ObjKey::encode_f64(*d)
            .map_err(|_| QueryError::execution("cannot encode f64 threshold".to_string()))?
            .as_u64(),
        _ => return Ok(None),
    };
    Ok(Some(key))
}

fn leaflet_fully_matches(compare: NumericCompareOp, first: u64, last: u64, threshold: u64) -> bool {
    match compare {
        NumericCompareOp::Gt => first > threshold,
        NumericCompareOp::Ge => first >= threshold,
        NumericCompareOp::Lt => last < threshold,
        NumericCompareOp::Le => last <= threshold,
    }
}

fn leaflet_fully_excluded(
    compare: NumericCompareOp,
    first: u64,
    last: u64,
    threshold: u64,
) -> bool {
    match compare {
        NumericCompareOp::Gt => last <= threshold,
        NumericCompareOp::Ge => last < threshold,
        NumericCompareOp::Lt => first >= threshold,
        NumericCompareOp::Le => first > threshold,
    }
}

fn count_matching_rows_in_sorted_batch(
    batch: &fluree_db_binary_index::ColumnBatch,
    compare: NumericCompareOp,
    threshold: u64,
) -> usize {
    let lower = lower_bound_okey(batch, threshold);
    let upper = upper_bound_okey(batch, threshold);
    match compare {
        NumericCompareOp::Gt => batch.row_count.saturating_sub(upper),
        NumericCompareOp::Ge => batch.row_count.saturating_sub(lower),
        NumericCompareOp::Lt => lower,
        NumericCompareOp::Le => upper,
    }
}

fn lower_bound_okey(batch: &fluree_db_binary_index::ColumnBatch, threshold: u64) -> usize {
    let mut lo = 0usize;
    let mut hi = batch.row_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if batch.o_key.get(mid) < threshold {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound_okey(batch: &fluree_db_binary_index::ColumnBatch, threshold: u64) -> usize {
    let mut lo = 0usize;
    let mut hi = batch.row_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if batch.o_key.get(mid) <= threshold {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Scalar form of the numeric comparison on the order-preserving `o_key` encoding
/// (the same encoding the binary-search bounds rely on).
#[inline]
fn okey_matches(compare: NumericCompareOp, o_key: u64, threshold: u64) -> bool {
    match compare {
        NumericCompareOp::Gt => o_key > threshold,
        NumericCompareOp::Ge => o_key >= threshold,
        NumericCompareOp::Lt => o_key < threshold,
        NumericCompareOp::Le => o_key <= threshold,
    }
}

/// Overlay/time-travel lane for the numeric-compare count: counts rows of `p_id`
/// matching `?o <compare> threshold` over the novelty-merged stream, parallelized
/// across the subject space (PSOT) just like the HEAD path.
///
/// The comparison is per-row and order-independent, so PSOT order is fine — the POST
/// directory shortcut + binary search only apply to the base scan. A non-numeric (or
/// non-encodable) object is treated as a non-match: `?o <cmp> K` on a non-number
/// would error and so contributes nothing, which equals what the general aggregate
/// pipeline counts (and avoids the base path's whole-query bail on mixed types).
/// Returns the match count, or `Ok(None)` to defer (overlay flake failed to
/// translate). BASE+novelty: caller gates on [`allow_cursor_fast_path`].
fn count_numeric_compare_overlay_parallel(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: fluree_db_core::Sid,
    p_id: u32,
    compare: NumericCompareOp,
    threshold: &FlakeValue,
) -> Result<Option<u64>> {
    let tk_int = encode_numeric_threshold_for_otype(OType::XSD_INTEGER, threshold)?;
    let tk_dbl = encode_numeric_threshold_for_otype(OType::XSD_DOUBLE, threshold)?;
    parallel_overlay_psot_filter_count(
        ctx,
        store,
        g_id,
        pred_sid,
        p_id,
        move |_s, o_type, o_key| {
            let tk = match OType::from_u16(o_type) {
                OType::XSD_INTEGER => tk_int,
                OType::XSD_DOUBLE => tk_dbl,
                _ => return false, // non-numeric object: comparison errors => not a match
            };
            match tk {
                Some(tk) => okey_matches(compare, o_key, tk),
                None => false,
            }
        },
    )
}

/// Fast-path: parallel `COUNT(?s)` / `COUNT(*)` for a single predicate `?s <p> ?o`
/// with FILTERs that compile to encoded pre-filters (no value decoding), e.g.
/// `FILTER(?s != ?o)`, `FILTER(?s = ?o)`, `FILTER(ISBLANK(?o))`,
/// `FILTER(LANG(?o) = "en")`.
///
/// At HEAD (via [`fast_path_store`]) the predicate's PSOT leaves are split across
/// cores and each chunk counts the rows passing *every* encoded pre-filter — reading
/// only the (s_id, o_type, o_key) columns and bypassing the serial
/// `BinaryScanOperator` row materialization entirely. Returns `Ok(None)` — deferring
/// to `fallback` (the serial scan-count) — when not at HEAD, the predicate is absent
/// already handled as 0, or any filter can't be pushed down to encoded columns
/// (it would need value decoding).
pub fn count_rows_encoded_filters_operator(
    pattern: TriplePattern,
    inline_ops: Vec<InlineOperator>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let g_id = ctx.binary_g_id;
            // HEAD lane: parallel base-leaflet scan applying the encoded filters.
            if let Some(store) = fast_path_store(ctx) {
                let pred_sid = normalize_pred_sid(store, &pattern.p)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    // Absent predicate => no rows => COUNT is 0.
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                // Compile the FILTERs to encoded pre-filters. If any can't be pushed
                // down (needs value decoding), defer to the serial scan-count fallback.
                let (encoded, pruned) = compile_encoded_pre_filters_and_prune_inline_ops(
                    &inline_ops,
                    &pattern,
                    store,
                    true,
                );
                if !pruned.is_empty() || encoded.is_empty() {
                    return Ok(None);
                }
                let total_rows = count_rows_for_predicate_psot(store, g_id, p_id)?;
                let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
                let count = parallel_leaf_chunk_count(leaves, total_rows, |chunk| {
                    count_rows_matching_encoded_filters_in_leaf_slice(store, p_id, &encoded, chunk)
                })?;
                return match count {
                    Some(c) => Ok(Some(build_count_batch(
                        out_var,
                        count_to_i64(c, "COUNT rows encoded filters")?,
                    )?)),
                    None => Ok(None),
                };
            }
            // Overlay / time-travel lane: apply the same encoded filters per row over
            // the merged cursor. A novelty-only predicate has no base id => defer.
            if allow_cursor_fast_path(ctx) {
                if let Some(store) = ctx.binary_store.as_ref() {
                    let pred_sid = normalize_pred_sid(store, &pattern.p)?;
                    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                        return Ok(None);
                    };
                    let (encoded, pruned) = compile_encoded_pre_filters_and_prune_inline_ops(
                        &inline_ops,
                        &pattern,
                        store,
                        true,
                    );
                    if !pruned.is_empty() || encoded.is_empty() {
                        return Ok(None);
                    }
                    if let Some(count) = parallel_overlay_psot_filter_count(
                        ctx,
                        store,
                        g_id,
                        pred_sid,
                        p_id,
                        move |s_id, o_type, o_key| {
                            encoded.iter().all(|f| f.eval_row(s_id, o_type, o_key))
                        },
                    )? {
                        return Ok(Some(build_count_batch(
                            out_var,
                            count_to_i64(count, "COUNT rows encoded filters")?,
                        )?));
                    }
                }
            }
            Ok(None)
        },
        fallback,
        "COUNT rows encoded filters (parallel)",
    )
}

/// Count rows of `p_id` passing every encoded pre-filter over one contiguous slice
/// of PSOT leaves. Loads (s_id, o_type, o_key) per leaflet and applies the filters
/// on the encoded columns — no term decoding, no binding materialization. Always
/// returns `Ok(Some(_))` (the encoded filters are total functions on every row).
fn count_rows_matching_encoded_filters_in_leaf_slice(
    store: &BinaryIndexStore,
    p_id: u32,
    filters: &[EncodedPreFilter],
    leaves: &[LeafEntry],
) -> Result<Option<u64>> {
    let projection = projection_sid_otype_okey();
    let mut total: u64 = 0;
    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for r in 0..batch.row_count {
                let s_id = batch.s_id.get(r);
                let o_type = batch.o_type.get(r);
                let o_key = batch.o_key.get(r);
                if filters.iter().all(|f| f.eval_row(s_id, o_type, o_key)) {
                    total = total.saturating_add(1);
                }
            }
        }
    }
    Ok(Some(total))
}

/// Fast-path: `COUNT(*)` / `COUNT(?x)` for a single triple `?s <p> ?o`
/// with `FILTER(LANG(?o) = "<tag>")`.
pub fn count_rows_lang_filter_operator(
    predicate: Ref,
    lang_tag: String,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            // HEAD lane: base-leaflet scan with the per-leaflet o_type_const shortcut.
            if let Some(store) = fast_path_store(ctx) {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                let Some(lang_id) = store.resolve_lang_id(&lang_tag) else {
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                let required_otype = OType::lang_string(lang_id).as_u16();
                let count = count_rows_for_predicate_lang_psot(
                    store,
                    ctx.binary_g_id,
                    p_id,
                    required_otype,
                )?;
                return Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(count, "COUNT rows lang filter")?,
                )?));
            }
            // Overlay / time-travel lane: count merged rows whose o_type is the
            // lang-string type. A predicate or lang present only in novelty has no
            // base id, so defer to the fallback (it handles novelty-only terms).
            if allow_cursor_fast_path(ctx) {
                if let Some(store) = ctx.binary_store.as_ref() {
                    let pred_sid = normalize_pred_sid(store, &predicate)?;
                    let (Some(p_id), Some(lang_id)) = (
                        store.sid_to_p_id(&pred_sid),
                        store.resolve_lang_id(&lang_tag),
                    ) else {
                        return Ok(None);
                    };
                    let required_otype = OType::lang_string(lang_id).as_u16();
                    if let Some(count) = parallel_overlay_psot_filter_count(
                        ctx,
                        store,
                        ctx.binary_g_id,
                        pred_sid,
                        p_id,
                        move |_s, o_type, _o_key| o_type == required_otype,
                    )? {
                        return Ok(Some(build_count_batch(
                            out_var,
                            count_to_i64(count, "COUNT rows lang filter")?,
                        )?));
                    }
                }
            }
            Ok(None)
        },
        fallback,
        "COUNT rows lang filter",
    )
}

fn count_rows_for_predicate_lang_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    required_otype: u16,
) -> Result<u64> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_otype_only();
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if let Some(ot) = entry.o_type_const {
                if ot == required_otype {
                    total += entry.row_count as u64;
                }
                continue;
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                if batch.o_type.get(row) == required_otype {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

// COUNT(DISTINCT ?o) for a single predicate now lives in the consolidated
// `fast_predicate_scalar_agg` module alongside SUM/AVG (shared POST-scan driver).

// ---------------------------------------------------------------------------
// COUNT(*) / COUNT(?x) for `?s ?p ?o` and COUNT(DISTINCT ?lead)
// ---------------------------------------------------------------------------

/// Fast-path: count total triples across all patterns.
pub fn count_triples_operator(out_var: VarId, fallback: Option<BoxedOperator>) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_triples_from_branch_manifest(store, ctx.binary_g_id)?;
            let count_i64 = count_to_i64(count, "COUNT triples")?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "triples COUNT",
    )
}

fn count_triples_from_branch_manifest(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    // Any permutation's leaf `row_count` sums to the total number of triples.
    // Prefer PSOT (commonly present and predicate-segmented).
    let order_preference = [
        RunSortOrder::Psot,
        RunSortOrder::Spot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];
    for order in order_preference {
        if let Some(branch) = store.branch_for_order(g_id, order) {
            return Ok(branch.leaves.iter().map(|l| l.row_count).sum());
        }
    }
    Ok(0)
}

/// Which triple position a `COUNT(DISTINCT ?v)` over `?s ?p ?o` targets.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DistinctPosition {
    Subjects,
    Predicates,
    Objects,
}

/// Fast-path: count distinct subjects / predicates / objects across all triples.
///
/// Subjects (SPOT) and objects (OPST) are answered metadata-only from leaflet
/// `lead_group_count` with a boundary-overlap correction; predicates use PSOT
/// `p_const` transitions (PSOT leaflets are predicate-homogeneous). The two
/// dedup algorithms genuinely differ, so they stay distinct behind the
/// `position` branch — only the operator/dispatch shell is unified.
pub fn count_distinct_position_operator(
    position: DistinctPosition,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let label = match position {
        DistinctPosition::Subjects => "distinct subject COUNT",
        DistinctPosition::Predicates => "distinct predicate COUNT",
        DistinctPosition::Objects => "distinct object COUNT",
    };
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let (count, overflow_label) = match position {
                // SPOT key layout: s_id(8) + p_id(4) + o_type(2) + o_key(8) + o_i(4).
                // Distinct subjects = lead bytes [0..8].
                DistinctPosition::Subjects => (
                    count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Spot, 8)?,
                    "COUNT(DISTINCT) subjects",
                ),
                DistinctPosition::Predicates => (
                    // Prefer the in-memory per-graph stats (number of predicates
                    // with a positive current count) — O(#predicates), no leaf
                    // opens. Falls back to the PSOT `p_const` scan when the graph
                    // stats are unavailable.
                    match distinct_predicates_from_graph_stats(ctx) {
                        Some(c) => c,
                        None => count_distinct_predicates_psot(store, ctx.binary_g_id)?,
                    },
                    "COUNT(DISTINCT) predicates",
                ),
                // OPST key layout: o_type(2) + o_key(8) + o_i(4) + p_id(4) + s_id(8).
                // Distinct objects = lead bytes [0..10].
                DistinctPosition::Objects => (
                    count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Opst, 10)?,
                    "COUNT(DISTINCT) objects",
                ),
            };
            let count_i64 = count_to_i64(count, overflow_label)?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        label,
    )
}

/// Current-state distinct-predicate count from the per-graph index stats: the
/// number of properties with a positive current flake count.
///
/// `GraphPropertyStatEntry.count` is "after dedup; retractions decrement", so a
/// predicate has a current PSOT leaflet iff its count is `> 0` — this matches
/// `count_distinct_predicates_psot` exactly but reads the in-memory stats instead
/// of opening every leaf. Returns `None` when the graph stats are unavailable
/// (older index / not computed). Only correct at HEAD with no overlay; the caller
/// gates that via `fast_path_store`.
fn distinct_predicates_from_graph_stats(ctx: &ExecutionContext<'_>) -> Option<u64> {
    let graphs = ctx.active_snapshot.stats.as_ref()?.graphs.as_ref()?;
    let g = graphs.iter().find(|g| g.g_id == ctx.binary_g_id)?;
    Some(g.properties.iter().filter(|p| p.count > 0).count() as u64)
}

/// Count distinct lead groups across all leaflets in a given sort order.
///
/// Uses `lead_group_count` from leaflet directory entries, deduplicating groups
/// that span leaflet boundaries by comparing lead key prefixes.
///
/// `lead_len` is the number of leading key bytes that define the grouping:
/// - SPOT distinct subjects: 8 bytes (s_id)
/// - OPST distinct objects: 10 bytes (o_type + o_key)
fn count_distinct_lead_groups(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
    lead_len: usize,
) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, order) else {
        return Ok(0);
    };

    let mut prev_lead_last: Vec<u8> = Vec::new();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.lead_group_count == 0 {
                continue;
            }

            let lead_first = entry.first_key.get(..lead_len).ok_or_else(|| {
                QueryError::execution("leaflet key shorter than expected lead_len")
            })?;
            let lead_last = entry.last_key.get(..lead_len).ok_or_else(|| {
                QueryError::execution("leaflet key shorter than expected lead_len")
            })?;

            total += u64::from(entry.lead_group_count);
            if !prev_lead_last.is_empty() && prev_lead_last == lead_first {
                total = total.saturating_sub(1);
            }
            prev_lead_last.clear();
            prev_lead_last.extend_from_slice(lead_last);
        }
    }

    Ok(total)
}

/// Distinct predicates uses p_const metadata rather than lead_group_count,
/// since PSOT leaflets are predicate-homogeneous.
fn count_distinct_predicates_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };

    let mut prev_p: Option<u32> = None;
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }

            let p_id = match entry.p_const {
                Some(id) => id,
                None => {
                    let bytes: [u8; 4] = entry
                        .first_key
                        .get(..4)
                        .and_then(|s| s.try_into().ok())
                        .ok_or_else(|| {
                        QueryError::execution("PSOT leaflet key shorter than 4 bytes")
                    })?;
                    u32::from_be_bytes(bytes)
                }
            };

            if prev_p != Some(p_id) {
                total += 1;
                prev_p = Some(p_id);
            }
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// 4) Specialized global counts: literals and blank-node subjects
// ---------------------------------------------------------------------------

/// Fast-path: count triples with literal objects.
pub fn count_literal_objects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_literal_rows_psot(store, ctx.binary_g_id)?;
            let count_i64 = count_to_i64(count, "COUNT literals")?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "literal COUNT",
    )
}

fn is_literal_otype(ot_u16: u16) -> bool {
    let ot = OType::from_u16(ot_u16);
    !ot.is_node_ref()
}

fn count_literal_rows_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };
    let projection = projection_otype_only();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if let Some(ot) = entry.o_type_const {
                if is_literal_otype(ot) {
                    total += entry.row_count as u64;
                }
                continue;
            }

            // Mixed types: decode OType column only.
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                if is_literal_otype(batch.o_type.get(row)) {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// Fast-path: count triples with blank-node subjects.
pub fn count_blank_node_subjects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_blank_subject_rows_spot(store, ctx.binary_g_id)?;
            let count_i64 = count_to_i64(count, "COUNT blank nodes")?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "blank-node COUNT",
    )
}

fn blank_subject_range() -> (u64, u64) {
    let ns = namespaces::BLANK_NODE;
    let min = SubjectId::new(ns, 0).as_u64();
    let max = SubjectId::new(ns, 0x0000_FFFF_FFFF_FFFF).as_u64();
    (min, max)
}

fn count_blank_subject_rows_spot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Spot) else {
        return Ok(0);
    };
    let (s_min, s_max) = blank_subject_range();

    let min_key = RunRecordV2 {
        s_id: SubjectId(s_min),
        o_key: 0,
        p_id: 0,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(s_max),
        o_key: u64::MAX,
        p_id: u32::MAX,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let cmp = cmp_v2_for_order(RunSortOrder::Spot);
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    let leaves = &branch.leaves[leaf_range];

    let projection = projection_sid_only();
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            let first = read_ordered_key_v2(RunSortOrder::Spot, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Spot, &entry.last_key);
            let first_s = first.s_id.as_u64();
            let last_s = last.s_id.as_u64();

            if last_s < s_min || first_s > s_max {
                continue;
            }

            if first_s >= s_min && last_s <= s_max {
                total += entry.row_count as u64;
                continue;
            }

            // Boundary leaflet: count exact rows by scanning SId column only.
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Spot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if (s_min..=s_max).contains(&sid) {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

// (Removed) Regex anchored-prefix COUNT fast path: was parked behind
// `#[expect(dead_code)]` and not wired. If we revisit this, we should implement
// a correctness-first detector + a plan that doesn't require enumerating large
// string-id sets for common prefixes.
