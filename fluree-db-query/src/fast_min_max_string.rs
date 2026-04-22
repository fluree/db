//! Fast-path: scalar `MIN(?o)` / `MAX(?o)` / `AVG(?o)` for a single triple `?s <p> ?o`.
//!
//! QLever answers these kinds of aggregates by exploiting permutation order and metadata
//! to avoid scanning all rows. For Fluree's V3 index, we can do something similar:
//! - for homogeneous numeric predicates, use each POST leaflet's first/last key as the
//!   MIN/MAX candidate and scan only `o_key` for AVG
//! - for homogeneous string-dict predicates, use each leaflet's first/last key as the
//!   MIN/MAX candidate and compare string dictionary values lexicographically
//!
//! This reduces work from O(rows) decode/materialization to O(leaflets) for MIN/MAX and
//! O(rows) over `o_key` only for AVG.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    fast_path_store, leaf_entries_for_predicate, normalize_pred_sid, projection_okey_only,
    FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::{FlakeValue, GraphId, Sid};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MinMaxMode {
    Min,
    Max,
}

/// Create a fused operator that outputs a single-row batch containing the MIN/MAX result.
pub fn predicate_min_max_string_operator(
    predicate: Ref,
    mode: MinMaxMode,
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
                // Predicate absent -> empty input -> aggregate result is unbound.
                let batch = Batch::single_row(
                    Arc::from(vec![out_var].into_boxed_slice()),
                    vec![Binding::Unbound],
                )
                .map_err(|e| QueryError::execution(format!("min/max batch build: {e}")))?;
                return Ok(Some(batch));
            };

            if let Some(b) = minmax_numeric_post(store, ctx.binary_g_id, p_id, mode)? {
                let batch = Batch::single_row(Arc::from(vec![out_var].into_boxed_slice()), vec![b])
                    .map_err(|e| QueryError::execution(format!("min/max batch build: {e}")))?;
                return Ok(Some(batch));
            }

            if let Some(b) = minmax_string_dict_post(store, ctx.binary_g_id, p_id, mode)? {
                let batch = Batch::single_row(Arc::from(vec![out_var].into_boxed_slice()), vec![b])
                    .map_err(|e| QueryError::execution(format!("min/max batch build: {e}")))?;
                return Ok(Some(batch));
            }
            // Unsupported at runtime (mixed non-string objects) — fall through to planned pipeline.
            Ok(None)
        },
        fallback,
        "MIN/MAX string",
    )
}

/// Create a fused operator that outputs a single-row batch containing AVG(?o).
pub fn predicate_avg_numeric_operator(
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
                let batch = Batch::single_row(
                    Arc::from(vec![out_var].into_boxed_slice()),
                    vec![Binding::Unbound],
                )
                .map_err(|e| QueryError::execution(format!("avg batch build: {e}")))?;
                return Ok(Some(batch));
            };

            if let Some(b) = avg_numeric_post(store, ctx.binary_g_id, p_id)? {
                let batch = Batch::single_row(Arc::from(vec![out_var].into_boxed_slice()), vec![b])
                    .map_err(|e| QueryError::execution(format!("avg batch build: {e}")))?;
                return Ok(Some(batch));
            }

            Ok(None)
        },
        fallback,
        "AVG numeric",
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EncodedStringIdentity {
    /// String dictionary ID (NOT lexicographically ordered).
    str_id: u32,
    /// Datatype identity for ordering/equality (xsd:string vs rdf:langString vs fulltext).
    dt_id: u16,
    /// Language identity for rdf:langString (0 for non-langString).
    lang_id: u16,
}

fn encoded_lit_from_otype(
    o_type: u16,
    o_key: u64,
    p_id: u32,
) -> Option<(EncodedStringIdentity, Binding)> {
    let ot = OType::from_u16(o_type);
    if ot.decode_kind() != fluree_db_core::o_type::DecodeKind::StringDict {
        return None;
    }
    let str_id = u32::try_from(o_key).ok()?;

    let (dt_id, lang_id) = if ot.is_lang_string() {
        (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
    } else if o_type == OType::FULLTEXT.as_u16() {
        (DatatypeDictId::FULL_TEXT.as_u16(), 0)
    } else {
        // Default string dict values to xsd:string to match late-materialization behavior.
        (DatatypeDictId::STRING.as_u16(), 0)
    };

    let ident = EncodedStringIdentity {
        str_id,
        dt_id,
        lang_id,
    };
    let b = Binding::EncodedLit {
        o_kind: ObjKind::LEX_ID.as_u8(),
        o_key,
        p_id,
        dt_id,
        lang_id,
        i_val: i32::MIN,
        t: 0,
    };
    Some((ident, b))
}

/// Compute MIN/MAX candidate for a predicate by scanning POST leaflets and considering
/// only directory keys (first/last key per leaflet).
///
/// Returns `None` when leaflets contain non-string objects (to avoid semantic surprises).
fn minmax_string_dict_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    mode: MinMaxMode,
) -> Result<Option<Binding>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);

    let mut best: Option<(EncodedStringIdentity, Binding)> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let rr = match mode {
                MinMaxMode::Min => read_ordered_key_v2(RunSortOrder::Post, &entry.first_key),
                MinMaxMode::Max => read_ordered_key_v2(RunSortOrder::Post, &entry.last_key),
            };
            let Some(candidate) = encoded_lit_from_otype(rr.o_type, rr.o_key, p_id) else {
                return Ok(None);
            };

            match &best {
                None => best = Some(candidate),
                Some((best_id, _)) => {
                    // We can compare lexicographically without materialization *only*
                    // when both candidates share the same datatype+lang identity.
                    if candidate.0.dt_id != best_id.dt_id || candidate.0.lang_id != best_id.lang_id
                    {
                        return Ok(None);
                    }
                    let ord = store
                        .compare_string_lex(candidate.0.str_id, best_id.str_id)
                        .map_err(|e| QueryError::Internal(format!("compare string lex: {e}")))?;
                    let better = match mode {
                        MinMaxMode::Min => ord.is_lt(),
                        MinMaxMode::Max => ord.is_gt(),
                    };
                    if better {
                        best = Some(candidate);
                    }
                }
            }
        }
    }

    Ok(best.map(|(_, b)| b))
}

fn minmax_numeric_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    mode: MinMaxMode,
) -> Result<Option<Binding>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let mut best: Option<(u16, u64)> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let rr = match mode {
                MinMaxMode::Min => read_ordered_key_v2(RunSortOrder::Post, &entry.first_key),
                MinMaxMode::Max => read_ordered_key_v2(RunSortOrder::Post, &entry.last_key),
            };
            let ot = OType::from_u16(rr.o_type);
            if !ot.is_numeric() {
                return Ok(None);
            }

            match best {
                None => best = Some((rr.o_type, rr.o_key)),
                Some((best_ot, best_key)) => {
                    if best_ot != rr.o_type {
                        return Ok(None);
                    }
                    let better = match mode {
                        MinMaxMode::Min => rr.o_key < best_key,
                        MinMaxMode::Max => rr.o_key > best_key,
                    };
                    if better {
                        best = Some((rr.o_type, rr.o_key));
                    }
                }
            }
        }
    }

    Ok(best.map(|(o_type, o_key)| numeric_binding_from_otype_okey(store, o_type, o_key)))
}

/// Compute AVG(?o) over a numeric predicate by scanning POST leaflets.
///
/// # Precision
///
/// Uses Kahan compensated summation to reduce floating-point rounding error when
/// accumulating many values. Naive `sum += x` can lose low-order bits as the
/// accumulator grows large; Kahan summation maintains a separate compensation
/// term `c` that captures the lost low-order bits each iteration, keeping
/// relative error near machine epsilon rather than growing with row count.
fn avg_numeric_post(store: &BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Option<Binding>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let projection = projection_okey_only();
    let mut required_otype: Option<u16> = None;
    // Kahan compensated summation state
    let mut sum = 0.0f64;
    let mut compensation = 0.0f64;
    let mut count: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let Some(o_type) = entry.o_type_const else {
                return Ok(None);
            };
            let ot = OType::from_u16(o_type);
            if !ot.is_numeric() {
                return Ok(None);
            }
            match required_otype {
                None => required_otype = Some(o_type),
                Some(existing) if existing != o_type => return Ok(None),
                Some(_) => {}
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let val = decode_numeric_as_f64(o_type, batch.o_key.get(row))?;
                // Kahan summation: compensate for lost low-order bits
                let y = val - compensation;
                let t = sum + y;
                compensation = (t - sum) - y;
                sum = t;
            }
            count = count.saturating_add(batch.row_count as u64);
        }
    }

    if count == 0 {
        return Ok(Some(Binding::Unbound));
    }
    Ok(Some(Binding::lit(
        FlakeValue::Double(sum / count as f64),
        Sid::xsd_double(),
    )))
}

fn numeric_binding_from_otype_okey(store: &BinaryIndexStore, o_type: u16, o_key: u64) -> Binding {
    let ot = OType::from_u16(o_type);
    let dt = store
        .resolve_datatype_sid(o_type)
        .unwrap_or_else(Sid::xsd_integer);
    match ot.decode_kind() {
        DecodeKind::I64 => Binding::lit(FlakeValue::Long(ObjKey::from_u64(o_key).decode_i64()), dt),
        DecodeKind::F64 => {
            Binding::lit(FlakeValue::Double(ObjKey::from_u64(o_key).decode_f64()), dt)
        }
        _ => Binding::Unbound,
    }
}

fn decode_numeric_as_f64(o_type: u16, o_key: u64) -> Result<f64> {
    let ot = OType::from_u16(o_type);
    let key = ObjKey::from_u64(o_key);
    match ot.decode_kind() {
        DecodeKind::I64 => Ok(key.decode_i64() as f64),
        DecodeKind::F64 => Ok(key.decode_f64()),
        _ => Err(QueryError::execution(format!(
            "unsupported numeric decode kind for AVG fast-path: {ot:?}"
        ))),
    }
}
