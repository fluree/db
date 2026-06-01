//! Consolidated fast-path COUNT operators.
//!
//! This module groups the `fast_count_*` family into one place to reduce sprawl.
//! All operators here emit a single-row count batch via `FastPathOperator`
//! when `fast_path_store(ctx)` is available, otherwise they fall back to a planned
//! operator tree for correctness.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, count_rows_for_predicate_psot, count_to_i64, fast_path_store,
    leaf_entries_for_predicate, normalize_pred_sid, projection_okey_only, projection_otype_only,
    projection_sid_only, FastPathOperator,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
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
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                return Ok(Some(build_count_batch(out_var, 0)?));
            };
            let count = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
            Ok(Some(build_count_batch(
                out_var,
                count_to_i64(count, "COUNT rows")?,
            )?))
        },
        fallback,
        "COUNT rows",
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
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
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
            match count {
                Some(count) => Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(count, "COUNT rows numeric compare")?,
                )?)),
                None => Ok(None),
            }
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
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
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
            match count {
                Some(count) => Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(count, "SUM(compare) as count")?,
                )?)),
                None => Ok(None),
            }
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
    let projection = projection_okey_only();
    let mut total: u64 = 0;
    let mut required_otype: Option<OType> = None;
    let mut threshold_key: Option<u64> = None;

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

            match required_otype {
                Some(existing) if existing != otype => return Ok(None),
                Some(_) => {}
                None => {
                    required_otype = Some(otype);
                    threshold_key = Some(
                        match encode_numeric_threshold_for_otype(otype, threshold)? {
                            Some(key) => key,
                            None => return Ok(None),
                        },
                    );
                }
            }

            let threshold_key = threshold_key.expect("set with required_otype");
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
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                return Ok(Some(build_count_batch(out_var, 0)?));
            };
            let Some(lang_id) = store.resolve_lang_id(&lang_tag) else {
                return Ok(Some(build_count_batch(out_var, 0)?));
            };
            let required_otype = OType::lang_string(lang_id).as_u16();
            let count =
                count_rows_for_predicate_lang_psot(store, ctx.binary_g_id, p_id, required_otype)?;
            Ok(Some(build_count_batch(
                out_var,
                count_to_i64(count, "COUNT rows lang filter")?,
            )?))
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
                    count_distinct_predicates_psot(store, ctx.binary_g_id)?,
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
