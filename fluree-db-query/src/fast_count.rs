//! Consolidated fast-path COUNT operators.
//!
//! This module groups the `fast_count_*` family into one place to reduce sprawl.
//! All operators here emit a single-row count batch via `FastPathOperator`
//! when `fast_path_store(ctx)` is available, otherwise they fall back to a planned
//! operator tree for correctness.

use crate::binary_scan::{compile_encoded_pre_filters_and_prune_inline_ops, EncodedPreFilter};
use crate::error::{QueryError, Result};
use crate::fast_lead_walk::{distinct_lead_walk, LeadWalk, Role};
use crate::fast_path_common::{
    build_count_batch, count_predicate_overlay_delta, count_rows_for_predicate_psot, count_to_i64,
    cursor_fast_path_for_predicate, fast_path_store, fast_path_store_policy_cleared,
    leaf_entries_for_predicate, normalize_pred_sid, parallel_leaf_chunk_count,
    parallel_leaf_chunk_reduce, parallel_overlay_psot_filter_count, projection_okey_only,
    projection_otype_okey, projection_otype_only, projection_sid_only, projection_sid_otype_okey,
    FastPathOperator, PredicateFastPath,
};
use crate::ir::triple::{Ref, TriplePattern};
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::arena::numbig::{NumBigArena, NumBigRepr, StoredBigValue};
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::leaflet::LiveGroup;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ValueTypeTag};
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
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None); // no binary store => general/filtered fallback
            };
            let pred_sid = normalize_pred_sid(store, &predicate)?;

            // O1: under a non-root view policy, a single-predicate count can keep
            // the fast path when the scanned predicate is provably uncovered by
            // the policy. `Decline` (covered, multi-ledger, or historical) falls
            // back to the filtered scan; an uncovered default-deny predicate is
            // wholly hidden, so its count is 0. With no policy / root this is
            // always `Allow`, preserving the original behavior.
            match cursor_fast_path_for_predicate(ctx, &pred_sid) {
                PredicateFastPath::Decline => return Ok(None),
                PredicateFastPath::Empty => return Ok(Some(build_count_batch(out_var, 0)?)),
                PredicateFastPath::Allow => {}
            }

            // Lane 1 — metadata-only predicate row count (instant) when there is
            // no novelty overlay and the target is the indexed head. Here, and
            // only here, a predicate missing from the persisted dictionary really
            // does mean zero rows: with no overlay, nothing else can contribute.
            let overlay_free = !crate::fast_path_common::overlay_has_novelty(ctx);
            if overlay_free && ctx.to_t == store.max_t() {
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(Some(build_count_batch(out_var, 0)?)); // predicate absent => 0
                };
                let count = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
                return Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(count, "COUNT rows")?,
                )?));
            }

            // Lane 2 — base metadata count + a novelty delta that rescans only the
            // leaves novelty touches, when the target is at or above the indexed
            // head. Time-travel below the head needs base replay — defer.
            //
            // A predicate living only in the overlay (uncommitted novelty, or a
            // datalog / OWL2-RL materialization) has no persisted `p_id` to
            // range-bound the delta cursor, so the base count is not the answer —
            // defer, exactly as the numeric-compare and encoded-filter twins below
            // do. Reading the miss as 0 here was #1863.
            if ctx.to_t >= store.max_t() {
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(None);
                };
                if let Some(count) =
                    count_predicate_overlay_delta(ctx, store, ctx.binary_g_id, pred_sid, p_id)?
                {
                    return Ok(Some(build_count_batch(
                        out_var,
                        count_to_i64(count, "COUNT rows")?,
                    )?));
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
            // O1: under a non-root view policy, keep the fast path when the scanned
            // predicate is provably uncovered. An uncovered default-deny predicate
            // is wholly hidden, so the count is 0.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                match cursor_fast_path_for_predicate(ctx, &pred_sid) {
                    PredicateFastPath::Decline => return Ok(None),
                    PredicateFastPath::Empty => return Ok(Some(build_count_batch(out_var, 0)?)),
                    PredicateFastPath::Allow => {}
                }
            }
            // HEAD lane: directory shortcut + binary-search over base POST leaflets.
            if let Some(store) = fast_path_store_policy_cleared(ctx) {
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
            // The O1 gate above already cleared single-ledger / no-from_t / policy.
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
/// SEMANTICS: `SUM` over an empty multiset is the identity `"0"^^xsd:integer`
/// (SPARQL 1.1 §18.5.1.3) — the same value `COUNT` yields — so `SUM(?o cmp K)`
/// equals `COUNT(rows where ?o cmp K)` for *every* input, including the empty and
/// no-match cases. The empty / absent-predicate branches below still return
/// `Ok(None)` to defer to the general aggregate pipeline; that is now a
/// conservative choice (the fallback yields the same `0`), not a correctness
/// requirement.
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
            // O1: keep the fast path when the scanned predicate is provably
            // uncovered. An uncovered default-deny predicate feeds the aggregate
            // *no rows* (empty SUM = identity 0) — defer to the fallback, which
            // emits that 0; this matches the absent/empty-input handling below.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                match cursor_fast_path_for_predicate(ctx, &pred_sid) {
                    PredicateFastPath::Decline | PredicateFastPath::Empty => return Ok(None),
                    PredicateFastPath::Allow => {}
                }
            }
            // HEAD lane.
            if let Some(store) = fast_path_store_policy_cleared(ctx) {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    // Absent predicate => empty input => SUM is identity 0; defer.
                    return Ok(None);
                };
                // Empty input (all rows retracted) => SUM is identity 0; defer.
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
            // matches and the total rows. The O1 gate above already cleared
            // single-ledger / no-from_t / policy.
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
                    // count. matches == 0 is the empty-or-no-match case (both now
                    // sum to 0); defer to the fallback rather than special-casing
                    // it here.
                    if matches > 0 {
                        return Ok(Some(build_count_batch(
                            out_var,
                            count_to_i64(matches, "SUM(compare) as count")?,
                        )?));
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
            if !otype_okey_order_comparable(otype) {
                // Uniformly not o_key-comparable (e.g. arena NUM_BIG): the leaf
                // scan below would bail on its first leaflet anyway — defer
                // now without opening any leaves.
                return Ok(None);
            }
            let Some(interval) = threshold_key_interval(otype, compare, threshold) else {
                // Threshold not encodable for the uniform o_type (e.g. a
                // decimal constant over integer rows): same doomed scan.
                return Ok(None);
            };
            if leaflet_fully_excluded(min_ok, max_ok, interval) {
                return Ok(Some(0));
            }
            if leaflet_fully_matches(min_ok, max_ok, interval) {
                return Ok(Some(count_rows_for_predicate_psot(store, g_id, p_id)?));
            }
        } else if otype_unsupported_numeric(min_ot) || otype_unsupported_numeric(max_ot) {
            // Mixed-type predicate whose extent BOUNDARY is an unsupported
            // numeric (e.g. integer rows + decimals: NUM_BIG sorts last among
            // the numerics, so it lands on the max boundary): those rows are
            // guaranteed present and the scan below is doomed — defer now.
            // An unsupported numeric strictly interior to the extent still
            // falls through and is caught by the per-leaflet bail.
            return Ok(None);
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
/// o_types whose `o_key` order equals numeric order, so a `?o <cmp> K` scan can
/// compare encoded keys directly:
/// - **all inline integer subtypes** (`is_integer`): every inline integer is
///   `encode_i64`-ordered; values that overflow `i64` carry the arena
///   `NUM_BIG_OVERFLOW` o_type instead, so an integer-subtype o_type guarantees
///   an inline, order-preserving key.
/// - **`xsd:double` / `xsd:float`**: `encode_f64` is total-order.
/// - **inline decimals** (`XSD_DECIMAL_INLINE`): order-preserving base-10 float.
///
/// Arena `NUM_BIG_OVERFLOW` is numeric but equality-only, so it is excluded.
pub(crate) fn otype_okey_order_comparable(ot: OType) -> bool {
    ot.is_integer()
        || ot == OType::XSD_DOUBLE
        || ot == OType::XSD_FLOAT
        || ot == OType::XSD_DECIMAL_INLINE
}

/// True if this o_type is numeric but NOT o_key-order-comparable, so rows of it
/// force the numeric-COUNT lanes to defer. With all inline integer subtypes,
/// `xsd:double`/`xsd:float`, and inline decimals now comparable
/// ([`otype_okey_order_comparable`]), this is the arena `NUM_BIG_OVERFLOW` lane
/// (equality-only) and the dormant lossy-f64 `XSD_DECIMAL` lane.
fn otype_unsupported_numeric(raw: u16) -> bool {
    let ot = OType::from_u16(raw);
    (ot.is_numeric() || ot == OType::NUM_BIG_OVERFLOW || ot == OType::XSD_DECIMAL_INLINE)
        && !otype_okey_order_comparable(ot)
}

/// The single `o_type` shared by every row of `p_id` in POST order, or `None`
/// if the predicate is empty or has mixed o_types. Read from the leaf manifest
/// (plus ≤2 boundary leaves) — cheap, no full scan. A uniform result in an
/// order-preserving numeric type (any inline integer subtype, double/float, or
/// inline decimal — see [`otype_okey_order_comparable`]) means every value
/// shares that type with no arena spill and no other types, which is the base
/// precondition for narrowing a numeric range scan by `o_key`. (The caller must
/// additionally ensure no overlay, since novelty can add a cross-type value.)
pub(crate) fn predicate_uniform_o_type(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Option<u16> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    match predicate_post_global_extent(store, p_id, leaves).ok()? {
        Some((min_ot, _, max_ot, _)) if min_ot == max_ot => Some(min_ot),
        _ => None,
    }
}

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
    let dir = store
        .open_leaf_dir(&leaf.leaf_cid)
        .map_err(|e| QueryError::from_io("leaf dir open", e))?;
    let entries = &dir.entries;
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
            .map_err(|e| QueryError::from_io("leaf open", e))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let Some(raw_otype) = entry.o_type_const else {
                return Ok(None);
            };
            let otype = OType::from_u16(raw_otype);
            if !otype_okey_order_comparable(otype) {
                return Ok(None);
            }
            let Some(interval) = threshold_key_interval(otype, compare, threshold) else {
                return Ok(None);
            };

            let first = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Post, &entry.last_key);

            if first.o_type != raw_otype || last.o_type != raw_otype {
                return Ok(None);
            }

            if leaflet_fully_matches(first.o_key, last.o_key, interval) {
                total = total.saturating_add(entry.row_count as u64);
                continue;
            }
            if leaflet_fully_excluded(first.o_key, last.o_key, interval) {
                continue;
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            total =
                total.saturating_add(count_matching_rows_in_sorted_batch(&batch, interval) as u64);
        }
    }

    Ok(Some(total))
}

/// `threshold`'s key in `otype`'s order-preserving `o_key` space, or `None`
/// when it has none there. All inline integer subtypes share `encode_i64`'s
/// order, doubles and floats `encode_f64`'s, and inline decimals their own
/// codec, so an integer or decimal threshold and a numerically equal stored
/// decimal encode identically. A threshold that doesn't fit inline, or a double
/// threshold against decimal rows, has no key.
pub(crate) fn encode_numeric_threshold_for_otype(
    otype: OType,
    threshold: &FlakeValue,
) -> Option<u64> {
    use bigdecimal::BigDecimal;
    match (otype, threshold) {
        (ot, FlakeValue::Long(n)) if ot.is_integer() => Some(ObjKey::encode_i64(*n).as_u64()),
        (OType::XSD_DOUBLE | OType::XSD_FLOAT, FlakeValue::Long(n)) => {
            Some(ObjKey::encode_f64(*n as f64).as_u64())
        }
        (OType::XSD_DOUBLE | OType::XSD_FLOAT, FlakeValue::Double(d)) => {
            Some(ObjKey::encode_f64(*d).as_u64())
        }
        (OType::XSD_DECIMAL_INLINE, FlakeValue::Decimal(d)) => {
            ObjKey::encode_decimal(d).map(ObjKey::as_u64)
        }
        (OType::XSD_DECIMAL_INLINE, FlakeValue::Long(n)) => {
            ObjKey::encode_decimal(&BigDecimal::from(*n)).map(ObjKey::as_u64)
        }
        (OType::XSD_DECIMAL_INLINE, FlakeValue::BigInt(b)) => {
            ObjKey::encode_decimal(&BigDecimal::from(b.as_ref().clone())).map(ObjKey::as_u64)
        }
        _ => None,
    }
}

/// The rows of one o_type that `?o <compare> threshold` selects, as an
/// inclusive `o_key` interval (keys are order-preserving per o_type).
///
/// - `None`: the threshold is not comparable by key for this o_type (e.g. a
///   decimal threshold over integer rows); the caller defers.
/// - `Some(None)`: the comparison selects no row of this o_type.
/// - `Some(Some((lo, hi)))`: it selects exactly the keys in `lo..=hi`.
///
/// Every comparison with NaN is false (XPath F&O 3.1 §4.3.2): a NaN threshold
/// selects nothing, and a double interval stops at `+INF`'s key, below NaN's.
pub(crate) type KeyInterval = Option<(u64, u64)>;

pub(crate) fn threshold_key_interval(
    otype: OType,
    compare: NumericCompareOp,
    threshold: &FlakeValue,
) -> Option<KeyInterval> {
    let (compare, key, max) = match (otype, threshold) {
        (OType::XSD_DOUBLE | OType::XSD_FLOAT, FlakeValue::Long(n)) => {
            let (compare, d) = long_threshold_as_double(compare, *n);
            (
                compare,
                ObjKey::encode_f64(d).as_u64(),
                ObjKey::F64_COMPARABLE_MAX.as_u64(),
            )
        }
        (OType::XSD_DOUBLE | OType::XSD_FLOAT, FlakeValue::Double(d)) => {
            if d.is_nan() {
                return Some(None);
            }
            (
                compare,
                ObjKey::encode_f64(*d).as_u64(),
                ObjKey::F64_COMPARABLE_MAX.as_u64(),
            )
        }
        _ => (
            compare,
            encode_numeric_threshold_for_otype(otype, threshold)?,
            u64::MAX,
        ),
    };
    let interval = match compare {
        NumericCompareOp::Gt => key.checked_add(1).map(|lo| (lo, max)),
        NumericCompareOp::Ge => Some((key, max)),
        NumericCompareOp::Lt => key.checked_sub(1).map(|hi| (0, hi)),
        NumericCompareOp::Le => Some((0, key)),
    };
    Some(interval.filter(|(lo, hi)| lo <= hi))
}

/// `?v compare n`, for a double `?v`, as a comparison with a double: `n`
/// itself when it is one, else the double `n` rounds to, with the comparison
/// adjusted to its side of `n`. No double lies strictly between `n` and that
/// neighbour, so the comparison selects exactly the doubles the exact one
/// does (Fluree compares numbers exactly).
fn long_threshold_as_double(compare: NumericCompareOp, n: i64) -> (NumericCompareOp, f64) {
    use std::cmp::Ordering;
    use NumericCompareOp::{Ge, Gt, Le, Lt};
    let d = n as f64;
    // Every double at or beyond 2^53 is an integer, and `d` is at most 2^63,
    // so this comparison is exact.
    let compare = match (d as i128).cmp(&i128::from(n)) {
        Ordering::Equal => compare,
        // `d` is above `n`: `> n` and `>= n` are `>= d`; `< n` and `<= n` are `< d`.
        Ordering::Greater => match compare {
            Gt | Ge => Ge,
            Lt | Le => Lt,
        },
        // `d` is below `n`: `> n` and `>= n` are `> d`; `< n` and `<= n` are `<= d`.
        Ordering::Less => match compare {
            Gt | Ge => Gt,
            Lt | Le => Le,
        },
    };
    (compare, d)
}

fn leaflet_fully_matches(first: u64, last: u64, interval: KeyInterval) -> bool {
    interval.is_some_and(|(lo, hi)| first >= lo && last <= hi)
}

fn leaflet_fully_excluded(first: u64, last: u64, interval: KeyInterval) -> bool {
    interval.is_none_or(|(lo, hi)| last < lo || first > hi)
}

fn count_matching_rows_in_sorted_batch(
    batch: &fluree_db_binary_index::ColumnBatch,
    interval: KeyInterval,
) -> usize {
    interval.map_or(0, |(lo, hi)| {
        upper_bound_okey(batch, hi).saturating_sub(lower_bound_okey(batch, lo))
    })
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

/// Scalar form of the numeric comparison on the order-preserving `o_key`
/// encoding (the same intervals the binary-search bounds use).
#[inline]
fn okey_matches(o_key: u64, interval: KeyInterval) -> bool {
    interval.is_some_and(|(lo, hi)| lo <= o_key && o_key <= hi)
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
/// translate). BASE+novelty: caller gates on the O1
/// [`cursor_fast_path_for_predicate`] verdict (single-ledger, no `from_t`, and
/// the view policy does not cover the scanned predicate).
fn count_numeric_compare_overlay_parallel(
    ctx: &crate::context::ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: fluree_db_core::Sid,
    p_id: u32,
    compare: NumericCompareOp,
    threshold: &FlakeValue,
) -> Result<Option<u64>> {
    // One interval per order-preserving family: all integer subtypes share the
    // integer order, doubles and floats the double order, inline decimals their
    // own. `None` means the threshold has no key in that family.
    let tk_int = threshold_key_interval(OType::XSD_INTEGER, compare, threshold);
    let tk_dbl = threshold_key_interval(OType::XSD_DOUBLE, compare, threshold);
    let tk_dec = threshold_key_interval(OType::XSD_DECIMAL_INLINE, compare, threshold);

    // Map a row o_type to its family's interval: `Some(tk)` if the type is
    // o_key-comparable (tk may itself be `None` if the threshold has no key in
    // that family), `None` if the type isn't comparable at all.
    let tk_for = |ot: OType| -> Option<Option<KeyInterval>> {
        if ot.is_integer() {
            Some(tk_int)
        } else if ot == OType::XSD_DOUBLE || ot == OType::XSD_FLOAT {
            Some(tk_dbl)
        } else if ot == OType::XSD_DECIMAL_INLINE {
            Some(tk_dec)
        } else {
            None
        }
    };

    // Pre-check the base predicate's POST extent: if the base rows are uniformly
    // an o_type we can't compare by o_key (e.g. arena NUM_BIG), or the threshold
    // can't encode for the uniform supported family, the full scan below is
    // doomed — defer immediately instead of scanning every partition first.
    // (Unsupported values arriving only via novelty are still caught by the
    // per-row flag; novelty is small, so that residual pass is bounded.)
    let post_leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    if let Some((min_ot, _, max_ot, _)) = predicate_post_global_extent(store, p_id, post_leaves)? {
        if min_ot == max_ot {
            let ot = OType::from_u16(min_ot);
            match tk_for(ot) {
                Some(Some(_)) => {}            // comparable, threshold encodes → proceed
                Some(None) => return Ok(None), // comparable family but threshold didn't encode
                None if ot.is_numeric() || ot == OType::NUM_BIG_OVERFLOW => return Ok(None),
                None => {} // non-numeric uniform → every row a non-match, fine
            }
        } else if otype_unsupported_numeric(min_ot) || otype_unsupported_numeric(max_ot) {
            // Mixed base with an unsupported-numeric boundary (e.g. integer
            // rows + arena NUM_BIG): doomed regardless of novelty — defer.
            return Ok(None);
        }
    }

    // Numeric o_types this lane can't compare by o_key (arena-keyed NUM_BIG,
    // which has no value order) must defer the whole count: treating them as
    // non-matches would silently undercount. All inline integer subtypes,
    // doubles/floats, and inline decimals ARE comparable. Mirrors the base
    // lane's per-leaflet Ok(None) bail.
    let saw_unsupported_numeric = std::sync::atomic::AtomicBool::new(false);
    let count = parallel_overlay_psot_filter_count(
        ctx,
        store,
        g_id,
        pred_sid,
        p_id,
        |_s, o_type, o_key| {
            let ot = OType::from_u16(o_type);
            match tk_for(ot) {
                Some(Some(interval)) => okey_matches(o_key, interval),
                Some(None) => {
                    // Comparable family but the threshold has no key in it
                    // (e.g. a decimal threshold against integer rows): defer.
                    saw_unsupported_numeric.store(true, std::sync::atomic::Ordering::Relaxed);
                    false
                }
                None if ot.is_numeric() || ot == OType::NUM_BIG_OVERFLOW => {
                    saw_unsupported_numeric.store(true, std::sync::atomic::Ordering::Relaxed);
                    false
                }
                // Genuinely non-numeric object: comparison errors => not a match.
                None => false,
            }
        },
    )?;
    if saw_unsupported_numeric.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(None);
    }
    Ok(count)
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
            // O1: keep the fast path when the scanned predicate is provably
            // uncovered; an uncovered default-deny predicate has all rows hidden
            // => COUNT is 0.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &pattern.p)?;
                match cursor_fast_path_for_predicate(ctx, &pred_sid) {
                    PredicateFastPath::Decline => return Ok(None),
                    PredicateFastPath::Empty => return Ok(Some(build_count_batch(out_var, 0)?)),
                    PredicateFastPath::Allow => {}
                }
            }
            // HEAD lane: parallel base-leaflet scan applying the encoded filters.
            if let Some(store) = fast_path_store_policy_cleared(ctx) {
                let pred_sid = normalize_pred_sid(store, &pattern.p)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    // Absent predicate => no rows => COUNT is 0.
                    return Ok(Some(build_count_batch(out_var, 0)?));
                };
                // Compile the FILTERs to encoded pre-filters. If any can't be pushed
                // down (needs value decoding), defer to the serial scan-count fallback.
                let (encoded, pruned, _) = compile_encoded_pre_filters_and_prune_inline_ops(
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
            // The O1 gate above already cleared single-ledger / no-from_t / policy.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &pattern.p)?;
                let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                    return Ok(None);
                };
                let (encoded, pruned, _) = compile_encoded_pre_filters_and_prune_inline_ops(
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
            .map_err(|e| QueryError::from_io("leaf open", e))?;
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
            // O1: keep the fast path when the scanned predicate is provably
            // uncovered; an uncovered default-deny predicate has all rows hidden
            // => COUNT is 0.
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                match cursor_fast_path_for_predicate(ctx, &pred_sid) {
                    PredicateFastPath::Decline => return Ok(None),
                    PredicateFastPath::Empty => return Ok(Some(build_count_batch(out_var, 0)?)),
                    PredicateFastPath::Allow => {}
                }
            }
            // HEAD lane: base-leaflet scan with the per-leaflet o_type_const shortcut.
            if let Some(store) = fast_path_store_policy_cleared(ctx) {
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
            // The O1 gate above already cleared single-ledger / no-from_t / policy.
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
            .map_err(|e| QueryError::from_io("leaf open", e))?;
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

/// Fast-path: count total triples across all patterns. With `lists`, the
/// count is of SPARQL's view of RDF lists (`crate::list_view`): each list's
/// members give way to its owner edge and two triples per cell, so the
/// stored count gains the members and lists of the index's list census.
pub fn count_triples_operator(
    out_var: VarId,
    lists: bool,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            // Leaf row counts include the legacy `f:reifies*` rows the
            // variable-predicate scan hides.
            if crate::fast_whole_graph_agg::graph_has_scan_hidden_predicates(ctx, store)? {
                return Ok(None);
            }
            let mut count = count_triples_from_branch_manifest(store, ctx.binary_g_id)?;
            if lists {
                let census = store
                    .list_census(ctx.binary_g_id)
                    .map_err(|e| QueryError::from_io("list census", e))?;
                count += census.values().map(|c| c.lists + c.members).sum::<u64>();
            }
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
        if let Some(branch) = store.dedicated_branch(g_id, order) {
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
            // The variable-predicate scan hides legacy `f:reifies*` facts, but
            // the SPOT/PSOT/OPST directories these folds read include them — a
            // graph holding a legacy bundle would over-count in every position.
            // Decline to the general pipeline when any such predicate has rows.
            if crate::fast_whole_graph_agg::graph_has_scan_hidden_predicates(ctx, store)? {
                return Ok(None);
            }
            let (count, overflow_label) = match position {
                DistinctPosition::Subjects => {
                    let Some(count) =
                        count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Spot)?
                    else {
                        return Ok(None);
                    };
                    (count, "COUNT(DISTINCT) subjects")
                }
                DistinctPosition::Predicates => (
                    // Prefer the O(#predicates) in-memory stats count; fall back
                    // to the PSOT `p_const` directory scan when stats are absent
                    // (older index). Both are gated above to HEAD/no-overlay with
                    // no scan-hidden predicates, so the stats count equals the
                    // scan exactly.
                    match distinct_predicates_from_graph_stats(ctx) {
                        Some(c) => c,
                        None => count_distinct_predicates_psot(store, ctx.binary_g_id)?,
                    },
                    "COUNT(DISTINCT) predicates",
                ),
                // OPST key layout: o_type(2) + o_key(8) + o_i(4) + p_id(4) + s_id(8).
                // Distinct objects = lead bytes [0..10] — which excludes p_id,
                // and so is not an object identity for a NumBig arena handle.
                // Those terms are counted exactly from the in-memory arenas
                // (see `count_distinct_numbig_objects`); declining is reserved
                // for an inconsistent index or the explicit kill switch.
                DistinctPosition::Objects => {
                    let Some(count) = count_distinct_objects(store, ctx.binary_g_id)? else {
                        return Ok(None);
                    };
                    (count, "COUNT(DISTINCT) objects")
                }
            };
            let count_i64 = count_to_i64(count, overflow_label)?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        label,
    )
}

/// Current-state distinct-predicate count from the per-graph index stats:
/// properties with a positive current flake count. O(#predicates), no leaf
/// opens — the in-memory equivalent of `count_distinct_predicates_psot`.
///
/// `GraphPropertyStatEntry.count` is post-dedup and retraction-decremented, so
/// `count > 0` ⇔ the predicate has a live PSOT leaflet, matching the scan
/// exactly. Safe only at HEAD with no overlay and no scan-hidden predicates —
/// both are caller-gated (`fast_path_store`, `graph_has_scan_hidden_predicates`).
/// Current-state-exact after an incremental index only because the build now
/// carries base per-graph property stats forward (they were previously dropped,
/// net-zero churn pinned at 0). Returns `None` when stats are absent (older
/// index) so the caller falls back to the PSOT scan.
fn distinct_predicates_from_graph_stats(ctx: &crate::context::ExecutionContext<'_>) -> Option<u64> {
    let graphs = ctx.active_snapshot.stats.as_ref()?.graphs.as_ref()?;
    let g = graphs.iter().find(|g| g.g_id == ctx.binary_g_id)?;
    Some(g.properties.iter().filter(|p| p.count > 0).count() as u64)
}

/// Distinct groups of `order` across the graph's dedicated tree, from leaflet
/// directories alone (see [`crate::fast_lead_walk`]): distinct subjects in
/// SPOT, distinct `(o_type, o_key)` in OPST.
///
/// `None` when a leaflet with rows predates `lead_group_count`: the caller
/// falls back to the general pipeline.
pub(crate) fn count_distinct_lead_groups(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
) -> Result<Option<u64>> {
    Ok(count_distinct_lead_groups_inner(store, g_id, order, false)?.map(|w| w.count))
}

/// `fast-path outcome` site for the exact NumBig branch, distinct from the
/// operator's own label so span capture can tell "counted from metadata alone"
/// from "counted the arena slice exactly".
const NUMBIG_EXACT_SITE: &str = "distinct object COUNT (numbig exact)";

/// Kill switch for the exact NumBig branch: when set, a graph whose NumBig
/// arenas hold more entries than this in total declines the whole count to
/// the general pipeline. Unset means no cap.
///
/// There is deliberately no default. The branch reads resident arena entries
/// and directory metadata, decoding mixed-type or legacy leaflets and scanning
/// NumBig rows for predicates with stale handles. Declining instead scans and
/// materializes every object in the graph. The predecessor of this
/// branch capped on NumBig *rows* (25M) and declined above it, which on a
/// Wikidata-scale ledger turned a 20 ms count into a full scan that timed out
/// at 300 s.
fn numbig_exact_max_entries() -> Option<u64> {
    std::env::var("FLUREE_NUMBIG_EXACT_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
}

/// Whole-graph distinct **objects** from OPST leaflet directories, or `None`
/// when the count cannot be established without the general pipeline.
///
/// The 10-byte OPST lead is `o_type(2) + o_key(8)` and stops immediately before
/// `p_id` (bytes `[14..18]`). For a `NUM_BIG_OVERFLOW` row that lead is a
/// per-predicate arena handle, so the first big value under one predicate and
/// the first under another share a lead and collapse into one group — see
/// [`OType::o_key_is_globally_identifying`]. Because handles are allocated from
/// 0 within each predicate, the result would be the *max* over predicates of
/// their distinct big-value counts rather than the size of the union: a silent
/// undercount, never an over-report.
///
/// The metadata walk therefore counts only the identifying part of the graph and
/// reports the NumBig slice's row count separately. When that slice is non-empty
/// its distinct terms come from [`count_distinct_numbig_objects`], which reads
/// the arenas rather than the rows.
///
/// The per-predicate distinct-object count is unaffected and stays on its fast
/// path: its 14-byte POST lead starts with `p_id`, which scopes the handle
/// correctly. So is the distinct-subject count, whose SPOT lead is `s_id`.
pub(crate) fn count_distinct_objects(
    store: &BinaryIndexStore,
    g_id: GraphId,
) -> Result<Option<u64>> {
    let Some(walk) = count_distinct_lead_groups_inner(store, g_id, RunSortOrder::Opst, true)?
    else {
        return Ok(None);
    };
    if walk.tally == 0 {
        return Ok(Some(walk.count));
    }
    let Some(exact) = count_distinct_numbig_objects(store, g_id)? else {
        return Ok(None);
    };
    crate::fast_path_outcome::stamp_fast_path(
        NUMBIG_EXACT_SITE,
        crate::fast_path_outcome::FastPathOutcome::Proceed,
    );
    Ok(Some(walk.count.saturating_add(exact)))
}

/// Exact distinct-term count over the graph's `NUM_BIG_OVERFLOW` objects,
/// answered from the arenas rather than the rows.
///
/// Every NumBig arena is resident in memory from store open and keyed on a
/// normalized value repr, so the union of the arenas' entries *is* the set of
/// distinct big-numeric terms the index has ever held — with one catch: an
/// arena keeps a handle for a value whose rows have since been retracted (a
/// full rebuild replays retractions through the same insert path), so arena
/// membership alone over-counts. Liveness is proved per predicate from POST
/// directory metadata: the number of distinct live handles under `p` is the
/// predicate's lead-group count over its NumBig leaflets, and when that equals
/// the arena's size every entry is live and nothing is read beyond directories.
/// Only a predicate with stale handles pays for a column read, and only over
/// its own NumBig rows, to learn *which* handles survive.
///
/// Dedup keys on the normalized repr, exactly as the general pipeline keys
/// these rows (normalized decimal lexical, BigInt separate from BigDecimal), so
/// the same value under two predicates — two handles in two arenas — and a
/// legacy arena's scale-variant duplicates each count once. A BigInt entry that
/// happens to fit `i64` is keyed as a big value here and as `Long` by the
/// general pipeline; the resolver never routes such a value to the arena, so
/// this cannot arise on a written index.
///
/// Returns `None` when the index is inconsistent (NumBig rows with no arena to
/// decode them) or when [`numbig_exact_max_entries`] is set and exceeded.
///
/// Keys are built and sorted on every execution; the count is not cached.
/// Before cross-predicate dedup, the vector holds one key per live arena
/// entry, including repeated values across predicates. On typical 64-bit targets
/// each key occupies 48 bytes, plus allocations for wide values and temporary
/// buffers. Legacy arenas also pay for normalization during key construction.
fn count_distinct_numbig_objects(store: &BinaryIndexStore, g_id: GraphId) -> Result<Option<u64>> {
    use rayon::prelude::*;

    let arenas: Vec<(u32, &NumBigArena)> = store
        .numbig_arenas(g_id)
        .filter(|(_, arena)| !arena.is_empty())
        .collect();
    if arenas.is_empty() {
        return Ok(None);
    }
    let total_entries: u64 = arenas.iter().map(|(_, a)| a.len() as u64).sum();
    if numbig_exact_max_entries().is_some_and(|cap| total_entries > cap) {
        return Ok(None);
    }

    let mut keys: Vec<NumBigDistinctKey> = Vec::new();
    for (p_id, arena) in arenas {
        let normalized = arena.values_are_normalized();
        let live = count_live_numbig_handles(store, g_id, p_id)?;
        if live == 0 {
            continue;
        }
        if live == arena.len() as u64 {
            keys.par_extend(
                arena
                    .values()
                    .par_iter()
                    .map(|v| NumBigDistinctKey::from_stored(v, normalized)),
            );
            continue;
        }
        let handles = live_numbig_handles(store, g_id, p_id)?;
        let mapped: Result<Vec<NumBigDistinctKey>> = handles
            .par_iter()
            .map(|&h| {
                arena
                    .get_by_handle(h)
                    .map(|v| NumBigDistinctKey::from_stored(v, normalized))
                    .ok_or_else(|| {
                        QueryError::Internal(format!(
                            "NumBig handle {h} beyond arena for g_id={g_id}, p_id={p_id}"
                        ))
                    })
            })
            .collect();
        keys.extend(mapped?);
    }
    keys.par_sort_unstable();
    keys.dedup();
    Ok(Some(keys.len() as u64))
}

/// Compact, totally ordered form of [`NumBigRepr`] for sort-based dedup.
///
/// Canonical signed little-endian bytes of at most 16 bytes fold into an
/// `i128`; anything wider keeps its bytes. The encoding is minimal, so a value
/// that fits `i128` never appears in the wide arm and the mapping is injective.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NumBigDistinctKey {
    Int(i128),
    IntWide(Vec<u8>),
    Dec(i128, i64),
    DecWide(Vec<u8>, i64),
}

impl NumBigDistinctKey {
    /// Only bypass normalization when the arena proves all stored values are
    /// normalized. Inline keys borrow bytes without allocating; wide keys own
    /// a copy so they can be sorted together across arenas.
    fn from_stored(value: &StoredBigValue, normalized: bool) -> Self {
        if !normalized {
            return Self::from_repr(value.normalized_repr());
        }
        match value {
            StoredBigValue::BigInt(bytes) => match i128_from_signed_le(bytes) {
                Some(v) => Self::Int(v),
                None => Self::IntWide(bytes.clone()),
            },
            StoredBigValue::BigDec { unscaled, scale } => match i128_from_signed_le(unscaled) {
                Some(v) => Self::Dec(v, *scale),
                None => Self::DecWide(unscaled.clone(), *scale),
            },
        }
    }

    fn from_repr(repr: NumBigRepr) -> Self {
        match repr {
            NumBigRepr::BigIntBytes(bytes) => match i128_from_signed_le(&bytes) {
                Some(v) => Self::Int(v),
                None => Self::IntWide(bytes),
            },
            NumBigRepr::BigDecBytes { unscaled, scale } => match i128_from_signed_le(&unscaled) {
                Some(v) => Self::Dec(v, scale),
                None => Self::DecWide(unscaled, scale),
            },
        }
    }
}

fn i128_from_signed_le(bytes: &[u8]) -> Option<i128> {
    if bytes.len() > 16 {
        return None;
    }
    let negative = bytes.last().is_some_and(|b| b & 0x80 != 0);
    let mut buf = [if negative { 0xFF } else { 0x00 }; 16];
    buf[..bytes.len()].copy_from_slice(bytes);
    Some(i128::from_le_bytes(buf))
}

/// How one predicate's POST leaflet relates to the `NUM_BIG_OVERFLOW` range.
enum NumBigLeaflet {
    /// Holds no NumBig row, or belongs to another predicate.
    Outside,
    /// Every row is NumBig: `lead_group_count` is its distinct-handle count.
    Pure,
    /// Straddles the NumBig range (or predates `lead_group_count`): rows
    /// must be decoded to find its NumBig handles.
    Decode,
}

fn classify_numbig_leaflet(
    entry: &fluree_db_binary_index::format::leaf::LeafletDirEntryV3,
    p_id: u32,
) -> NumBigLeaflet {
    if entry.row_count == 0 || entry.p_const != Some(p_id) {
        return NumBigLeaflet::Outside;
    }
    let numbig = OType::NUM_BIG_OVERFLOW.as_u16();
    match entry.o_type_const {
        Some(o_type) if o_type == numbig => {
            return if entry.lead_group_count > 0 {
                NumBigLeaflet::Pure
            } else {
                NumBigLeaflet::Decode
            };
        }
        Some(_) => return NumBigLeaflet::Outside,
        None => {}
    }
    // POST keys sort by `(p_id, o_type, o_key, …)`, so the entry's routing
    // interval bounds every o_type its rows hold.
    let lo = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key).o_type;
    let hi = read_ordered_key_v2(RunSortOrder::Post, &entry.last_key).o_type;
    if lo <= numbig && numbig <= hi {
        NumBigLeaflet::Decode
    } else {
        NumBigLeaflet::Outside
    }
}

/// The POST group of NumBig handle `handle` under `p_id`.
fn numbig_group(p_id: u32, handle: u64) -> LiveGroup {
    let rec = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: handle,
        p_id,
        t: 0,
        o_i: LIST_INDEX_NONE,
        o_type: OType::NUM_BIG_OVERFLOW.as_u16(),
        g_id: 0,
    };
    let mut key = [0u8; fluree_db_binary_index::format::run_record_v2::ORDERED_KEY_V2_SIZE];
    fluree_db_binary_index::format::run_record_v2::write_ordered_key_v2(
        RunSortOrder::Post,
        &rec,
        &mut key,
    );
    LiveGroup::of_key(RunSortOrder::Post, &key)
}

/// Distinct NumBig handles in one decoded POST leaflet: `(count, first, last)`
/// over its NumBig rows only, or `None` when it holds none. Rows are sorted by
/// `(o_type, o_key, …)` within the predicate, so equal handles are adjacent.
fn decode_numbig_handle_run(
    handle: &dyn fluree_db_binary_index::read::leaf_access::LeafHandle,
    leaflet_idx: usize,
    o_type_const: Option<u16>,
) -> Result<Option<(u64, u64, u64)>> {
    let numbig = OType::NUM_BIG_OVERFLOW.as_u16();
    let batch = handle
        .load_columns(leaflet_idx, &projection_otype_okey(), RunSortOrder::Post)
        .map_err(|e| QueryError::Internal(format!("leaflet columns: {e}")))?;
    let o_type_default = o_type_const.unwrap_or(0);
    let mut count = 0u64;
    let mut first = None;
    let mut prev = None;
    for row in 0..batch.row_count {
        if batch.o_type.get_or(row, o_type_default) != numbig {
            continue;
        }
        let k = batch.o_key.get(row);
        if prev != Some(k) {
            count += 1;
            prev = Some(k);
            first.get_or_insert(k);
        }
    }
    Ok(first.zip(prev).map(|(f, l)| (count, f, l)))
}

/// Number of distinct NumBig handles with a live row under `p_id`, from POST
/// leaflet directories: `lead_group_count` over type-pure NumBig leaflets with
/// seam dedup on the handle, decoding only a leaflet that straddles the NumBig
/// range or predates `lead_group_count`. Compared against the arena's size, this
/// says whether every arena entry is still live without reading a row.
fn count_live_numbig_handles(store: &BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<u64> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let walk = distinct_lead_walk(store, RunSortOrder::Post, leaves, |leaf, idx, entry| {
        Ok(match classify_numbig_leaflet(entry, p_id) {
            NumBigLeaflet::Outside => Role::Skip,
            NumBigLeaflet::Pure => Role::Groups,
            NumBigLeaflet::Decode => {
                let handle = store
                    .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
                    .map_err(|e| QueryError::from_io("leaf open", e))?;
                match decode_numbig_handle_run(handle.as_ref(), idx, entry.o_type_const)? {
                    Some((count, first, last)) => Role::Run {
                        count,
                        first: numbig_group(p_id, first),
                        last: numbig_group(p_id, last),
                    },
                    None => Role::Skip,
                }
            }
        })
    })?;
    // A leaflet without `lead_group_count` is decoded, so the walk never
    // declines. Were it to, fewer live handles than arena entries sends the
    // caller to the exact column read, which is correct either way.
    Ok(walk.map_or(0, |w| w.count))
}

/// The set of NumBig handles with a live row under `p_id`, sorted and
/// deduplicated. Reads the `o_key` column (plus `o_type` for a straddling
/// leaflet) over the predicate's NumBig leaflets only — the fallback for a
/// predicate whose arena holds retracted values.
fn live_numbig_handles(store: &BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Vec<u32>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    if leaves.is_empty() {
        return Ok(Vec::new());
    }
    let numbig = OType::NUM_BIG_OVERFLOW.as_u16();

    let map = |chunk: &[LeafEntry]| -> Result<Option<Vec<u32>>> {
        let mut handles: Vec<u32> = Vec::new();
        for leaf_entry in chunk {
            let dir = store
                .open_leaf_dir(&leaf_entry.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
            let wanted: Vec<usize> = dir
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    !matches!(classify_numbig_leaflet(e, p_id), NumBigLeaflet::Outside)
                })
                .map(|(i, _)| i)
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let handle = store
                .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
                .map_err(|e| QueryError::from_io("leaf open", e))?;
            for idx in wanted {
                let entry = &dir.entries[idx];
                let batch = handle
                    .load_columns(idx, &projection_otype_okey(), RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("leaflet columns: {e}")))?;
                let o_type_default = entry.o_type_const.unwrap_or(0);
                for row in 0..batch.row_count {
                    if batch.o_type.get_or(row, o_type_default) != numbig {
                        continue;
                    }
                    let h = batch.o_key.get(row) as u32;
                    if handles.last() != Some(&h) {
                        handles.push(h);
                    }
                }
            }
        }
        Ok(Some(handles))
    };

    let parallel = leaves.len() >= crate::fast_path_common::parallel_dir_walk_min_leaves();
    let mut handles = parallel_leaf_chunk_reduce(leaves, parallel, map, |mut l, r| {
        l.extend(r);
        l
    })?
    .unwrap_or_default();
    handles.sort_unstable();
    handles.dedup();
    Ok(handles)
}

/// Distinct groups of `order` over the graph's dedicated tree. With
/// `require_identifying_o_key`, leaflets that can hold a non-identifying
/// `o_key` are left out of the count and their rows tallied instead. They take
/// no part in boundary dedup either, which is sound because a NumBig group can
/// never equal a non-NumBig one (the o_type differs in its leading two bytes).
fn count_distinct_lead_groups_inner(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
    require_identifying_o_key: bool,
) -> Result<Option<LeadWalk>> {
    let Some(branch) = store.dedicated_branch(g_id, order) else {
        return Ok(Some(LeadWalk { count: 0, tally: 0 }));
    };
    distinct_lead_walk(store, order, &branch.leaves, |_, _, entry| {
        Ok(if entry.row_count == 0 {
            Role::Skip
        } else if require_identifying_o_key && leaflet_may_hold_non_identifying_o_key(entry) {
            Role::Tally(u64::from(entry.row_count))
        } else {
            Role::Groups
        })
    })
}

/// Whether an OPST leaflet can hold an object key that is not a graph-wide
/// identity.
///
/// An OPST leaflet's `o_type_const`, taken over its rows, says so without
/// opening a column. Without one, the routing keys' `o_type` interval bounds
/// every row's `o_type` (they bound the rows, widened or not). Which o_types
/// are the exceptions is the `OType` vocabulary's business, not this walk's,
/// so a new non-identifying o_type needs no edit here.
fn leaflet_may_hold_non_identifying_o_key(
    entry: &fluree_db_binary_index::format::leaf::LeafletDirEntryV3,
) -> bool {
    if let Some(o_type) = entry.o_type_const {
        return !OType::from_u16(o_type).o_key_is_globally_identifying();
    }
    let o_type_at = |k: &[u8]| -> OType {
        OType::from_u16(u16::from_be_bytes([
            k.first().copied().unwrap_or(0),
            k.get(1).copied().unwrap_or(0),
        ]))
    };
    let lo = o_type_at(&entry.first_key);
    let hi = o_type_at(&entry.last_key);
    if lo == hi {
        return !lo.o_key_is_globally_identifying();
    }
    OType::range_holds_non_globally_identifying(lo, hi)
}

/// Count distinct objects `(o_type, o_key)` for one predicate from POST
/// leaflet directories only: a POST group is the predicate and the object.
pub(crate) fn count_distinct_objects_for_predicate(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Option<u64>> {
    count_lead_groups_for_predicate(store, g_id, p_id, RunSortOrder::Post)
}

/// Count distinct subjects for one predicate from PSOT leaflet directories
/// only: a PSOT group is the predicate and the subject.
pub(crate) fn count_distinct_subjects_for_predicate(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Option<u64>> {
    count_lead_groups_for_predicate(store, g_id, p_id, RunSortOrder::Psot)
}

/// Count distinct groups for one predicate's leaf range, reading only the
/// leaflet directories (see [`crate::fast_lead_walk`]). Distinctness is
/// order-independent, so no `lex_sorted_string_ids` gate is needed and every
/// object type is supported.
///
/// Returns `Ok(None)` when a leaflet with rows has no `lead_group_count`
/// (pre-§3.2 leaves): the caller must fall back to a row scan.
fn count_lead_groups_for_predicate(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    order: RunSortOrder,
) -> Result<Option<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, order, p_id);
    let walk = distinct_lead_walk(store, order, leaves, |_, _, entry| {
        Ok(if entry.row_count == 0 || entry.p_const != Some(p_id) {
            Role::Skip
        } else {
            Role::Groups
        })
    })?;
    Ok(walk.map(|w| w.count))
}

/// Distinct predicates uses p_const metadata rather than lead_group_count,
/// since PSOT leaflets are predicate-homogeneous.
fn count_distinct_predicates_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.dedicated_branch(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };

    let mut prev_p: Option<u32> = None;
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let dir = store
            .open_leaf_dir(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;

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
///
/// First tries a metadata-only fold over the per-graph property datatype stats
/// (zero leaf I/O); when the stats can't attribute every row exactly it falls
/// back to the PSOT leaflet walk.
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
            let stats_fold = ctx
                .active_snapshot
                .stats
                .as_ref()
                .and_then(|stats| count_literal_rows_from_stats(stats, ctx.binary_g_id));
            let count = match stats_fold {
                Some(count) => {
                    tracing::debug!(count, "literal COUNT answered from datatype stats");
                    count
                }
                None => count_literal_rows_psot(store, ctx.binary_g_id)?,
            };
            let count_i64 = count_to_i64(count, "COUNT literals")?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "literal COUNT",
    )
}

/// Metadata-only literal-row count from per-graph property datatype stats:
/// literal rows = Σ over properties of every non-`JSON_LD_ID` datatype count
/// (IRI and blank-node refs both carry `JSON_LD_ID`; every other tag is a
/// literal).
///
/// Returns `None` (caller falls back to the leaflet walk) unless the stats
/// provably attribute every row in the graph:
/// - an `UNKNOWN` bucket means unattributable rows — the index-build mapping
///   sends both blank-node objects (not literals) and NumBig overflow values
///   (literals) to `UNKNOWN`;
/// - each property's datatype counts must sum to its row count, and property
///   counts must sum to the graph's flake total (guards stale/partial stats);
/// - a zero result defers so the leaflet walk (cheap when empty) confirms it.
fn count_literal_rows_from_stats(stats: &fluree_db_core::IndexStats, g_id: GraphId) -> Option<u64> {
    let graph = stats.graphs.as_ref()?.iter().find(|g| g.g_id == g_id)?;

    let ref_tag = ValueTypeTag::JSON_LD_ID.as_u8();
    let unknown_tag = ValueTypeTag::UNKNOWN.as_u8();
    let mut literals: u64 = 0;
    let mut attributed: u64 = 0;
    for prop in &graph.properties {
        let mut prop_total: u64 = 0;
        for &(tag, count) in &prop.datatypes {
            if tag == unknown_tag && count > 0 {
                return None;
            }
            prop_total = prop_total.checked_add(count)?;
            if tag != ref_tag {
                literals = literals.checked_add(count)?;
            }
        }
        if prop_total != prop.count {
            return None;
        }
        attributed = attributed.checked_add(prop_total)?;
    }
    if attributed != graph.flakes {
        return None;
    }
    (literals > 0).then_some(literals)
}

/// A triple term is neither a node reference nor a literal.
fn is_literal_otype(ot_u16: u16) -> bool {
    let ot = OType::from_u16(ot_u16);
    !ot.is_node_ref() && ot != OType::TRIPLE_TERM
}

fn count_literal_rows_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.dedicated_branch(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };
    let projection = projection_otype_only();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::from_io("leaf open", e))?;
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
    let Some(branch) = store.dedicated_branch(g_id, RunSortOrder::Spot) else {
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
        o_i: LIST_INDEX_NONE,
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
            .map_err(|e| QueryError::from_io("leaf open", e))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::index_stats::{GraphPropertyStatEntry, GraphStatsEntry};
    use fluree_db_core::IndexStats;

    /// An integer threshold selects exactly the doubles the generic lane's
    /// exact comparison does, around and beyond 2^53 where an `i64` has no
    /// double of its own.
    #[test]
    fn a_long_threshold_selects_doubles_exactly() {
        use std::cmp::Ordering;
        // `v` against `n`, exactly.
        fn exact(v: f64, n: i64) -> Ordering {
            let floor = v.floor();
            match (floor as i128).cmp(&i128::from(n)) {
                Ordering::Equal if v > floor => Ordering::Greater,
                other => other,
            }
        }
        let p53 = 1_i64 << 53;
        for n in [
            0,
            7,
            p53 - 1,
            p53,
            p53 + 1,
            p53 + 3,
            -(p53 + 1),
            i64::MAX,
            i64::MIN,
        ] {
            let d = n as f64;
            let near = [
                d,
                d.next_up(),
                d.next_down(),
                d.next_up().next_up(),
                d.next_down().next_down(),
            ];
            for op in [
                NumericCompareOp::Gt,
                NumericCompareOp::Ge,
                NumericCompareOp::Lt,
                NumericCompareOp::Le,
            ] {
                let interval = threshold_key_interval(OType::XSD_DOUBLE, op, &FlakeValue::Long(n))
                    .expect("an integer threshold is comparable with doubles");
                for v in near {
                    let key = ObjKey::encode_f64(v).as_u64();
                    let selected = interval.is_some_and(|(lo, hi)| lo <= key && key <= hi);
                    let expected = match op {
                        NumericCompareOp::Gt => exact(v, n) == Ordering::Greater,
                        NumericCompareOp::Ge => exact(v, n) != Ordering::Less,
                        NumericCompareOp::Lt => exact(v, n) == Ordering::Less,
                        NumericCompareOp::Le => exact(v, n) != Ordering::Greater,
                    };
                    assert_eq!(selected, expected, "{v:e} {op:?} {n}");
                }
            }
        }
    }

    #[test]
    fn numbig_signed_keys_preserve_boundaries() {
        use num_bigint::BigInt;
        for value in [
            0,
            127,
            128,
            -128,
            -129,
            i64::MIN as i128,
            i64::MAX as i128,
            i128::MIN,
            i128::MAX,
        ] {
            let bytes = BigInt::from(value).to_signed_bytes_le();
            assert_eq!(i128_from_signed_le(&bytes), Some(value));
            assert_eq!(
                NumBigDistinctKey::from_stored(&StoredBigValue::BigInt(bytes), true),
                NumBigDistinctKey::Int(value)
            );
        }
        for value in [BigInt::from(i128::MIN) - 1u8, BigInt::from(i128::MAX) + 1u8] {
            let bytes = value.to_signed_bytes_le();
            assert_eq!(i128_from_signed_le(&bytes), None);
            let int = NumBigDistinctKey::from_stored(&StoredBigValue::BigInt(bytes.clone()), true);
            let dec = NumBigDistinctKey::from_stored(
                &StoredBigValue::BigDec {
                    unscaled: bytes.clone(),
                    scale: 0,
                },
                true,
            );
            assert_eq!(int, NumBigDistinctKey::IntWide(bytes.clone()));
            assert_eq!(dec, NumBigDistinctKey::DecWide(bytes, 0));
            assert_ne!(int, dec);
        }
    }

    #[test]
    fn numbig_stored_keys_match_normalized_keys() {
        use fluree_db_binary_index::arena::numbig::{
            read_numbig_arena_from_bytes, write_numbig_arena_to_bytes,
        };
        let mut arena = NumBigArena::new();
        for lexical in [
            "1.50",
            "-1.500",
            "0.000",
            "1000.00",
            "170141183460469231731687303715884105728.1",
        ] {
            arena.get_or_insert_bigdec(&lexical.parse().unwrap());
        }
        arena.get_or_insert_bigint(&"170141183460469231731687303715884105728".parse().unwrap());
        let loaded =
            read_numbig_arena_from_bytes(&write_numbig_arena_to_bytes(&arena).unwrap()).unwrap();
        for arena in [&arena, &loaded] {
            assert!(arena.values_are_normalized());
            for value in arena.values() {
                assert_eq!(
                    NumBigDistinctKey::from_stored(value, arena.values_are_normalized()),
                    NumBigDistinctKey::from_repr(value.normalized_repr())
                );
            }
        }
        for (unscaled, scale, expected) in [
            (150i128, 2, NumBigDistinctKey::Dec(15, 1)),
            (0, 3, NumBigDistinctKey::Dec(0, 0)),
            (-1000, 2, NumBigDistinctKey::Dec(-1, -1)),
        ] {
            let value = StoredBigValue::BigDec {
                unscaled: num_bigint::BigInt::from(unscaled).to_signed_bytes_le(),
                scale,
            };
            assert_eq!(NumBigDistinctKey::from_stored(&value, false), expected);
        }
        assert_ne!(NumBigDistinctKey::Dec(15, 1), NumBigDistinctKey::Int(15));
    }

    fn prop(p_id: u32, datatypes: Vec<(u8, u64)>) -> GraphPropertyStatEntry {
        let count = datatypes.iter().map(|&(_, c)| c).sum();
        GraphPropertyStatEntry {
            p_id,
            count,
            ndv_values: 0,
            ndv_subjects: 0,
            last_modified_t: 1,
            observed_datatypes: fluree_db_core::PropertyStatEntry::tags_of(&datatypes),
            historical_datatypes: vec![],
            datatypes,
        }
    }

    fn stats_with_graph(g_id: GraphId, properties: Vec<GraphPropertyStatEntry>) -> IndexStats {
        let flakes = properties.iter().map(|p| p.count).sum();
        IndexStats {
            flakes,
            size: 0,
            properties: None,
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id,
                flakes,
                size: 0,
                properties,
                classes: None,
            }]),
            historical_since_t: None,
            links: None,
        }
    }

    const REF: u8 = 16; // ValueTypeTag::JSON_LD_ID
    const STRING: u8 = 1;
    const INTEGER: u8 = 2;
    const UNKNOWN: u8 = 255;

    #[test]
    fn double_key_intervals_follow_numeric_comparison() {
        use NumericCompareOp::{Ge, Gt, Le, Lt};
        let values = [f64::NEG_INFINITY, -1.5, 0.0, 2.5, f64::INFINITY, f64::NAN];
        let keys: Vec<u64> = values
            .iter()
            .map(|d| ObjKey::encode_f64(*d).as_u64())
            .collect();
        let selected = |compare: NumericCompareOp, threshold: FlakeValue| -> Vec<usize> {
            let interval = threshold_key_interval(OType::XSD_DOUBLE, compare, &threshold)
                .expect("a double threshold is comparable by key");
            (0..keys.len())
                .filter(|&i| okey_matches(keys[i], interval))
                .collect()
        };
        // XPath comparisons (F&O 3.1 §4.3.2): +INF is above and -INF below
        // every other number, and NaN satisfies none of them.
        assert_eq!(selected(Gt, FlakeValue::Double(0.0)), [3, 4]);
        assert_eq!(selected(Gt, FlakeValue::Long(0)), [3, 4]);
        assert_eq!(selected(Lt, FlakeValue::Long(0)), [0, 1]);
        assert_eq!(
            selected(Ge, FlakeValue::Double(f64::NEG_INFINITY)),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            selected(Le, FlakeValue::Double(f64::INFINITY)),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            selected(Lt, FlakeValue::Double(f64::INFINITY)),
            [0, 1, 2, 3]
        );
        assert!(selected(Gt, FlakeValue::Double(f64::INFINITY)).is_empty());
        for compare in [Gt, Ge, Lt, Le] {
            assert!(selected(compare, FlakeValue::Double(f64::NAN)).is_empty());
        }
    }

    #[test]
    fn literal_fold_sums_non_ref_tags() {
        let stats = stats_with_graph(
            0,
            vec![
                prop(1, vec![(STRING, 100), (REF, 40)]),
                prop(2, vec![(INTEGER, 7)]),
                prop(3, vec![(REF, 9)]),
            ],
        );
        assert_eq!(count_literal_rows_from_stats(&stats, 0), Some(107));
    }

    #[test]
    fn literal_fold_declines_on_unknown_bucket() {
        // UNKNOWN holds both blank-node refs and NumBig literals — unattributable.
        let stats = stats_with_graph(0, vec![prop(1, vec![(STRING, 5), (UNKNOWN, 1)])]);
        assert_eq!(count_literal_rows_from_stats(&stats, 0), None);
    }

    #[test]
    fn literal_fold_declines_on_datatype_count_mismatch() {
        let mut stats = stats_with_graph(0, vec![prop(1, vec![(STRING, 5)])]);
        stats.graphs.as_mut().unwrap()[0].properties[0].count = 6;
        stats.graphs.as_mut().unwrap()[0].flakes = 6;
        assert_eq!(count_literal_rows_from_stats(&stats, 0), None);
    }

    #[test]
    fn literal_fold_declines_on_graph_total_mismatch() {
        let mut stats = stats_with_graph(0, vec![prop(1, vec![(STRING, 5)])]);
        stats.graphs.as_mut().unwrap()[0].flakes = 7;
        assert_eq!(count_literal_rows_from_stats(&stats, 0), None);
    }

    #[test]
    fn literal_fold_defers_on_zero_and_missing() {
        let all_refs = stats_with_graph(0, vec![prop(1, vec![(REF, 9)])]);
        assert_eq!(count_literal_rows_from_stats(&all_refs, 0), None);

        let stats = stats_with_graph(0, vec![prop(1, vec![(STRING, 5)])]);
        assert_eq!(count_literal_rows_from_stats(&stats, 2), None);

        let no_graphs = IndexStats {
            flakes: 5,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
            historical_since_t: None,
            links: None,
        };
        assert_eq!(count_literal_rows_from_stats(&no_graphs, 0), None);
    }
}
