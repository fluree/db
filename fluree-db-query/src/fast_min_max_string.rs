//! Fast-path: scalar `MIN(?o)` / `MAX(?o)` for a single triple `?s <p> ?o`.
//!
//! These aggregates can be answered by exploiting permutation order and metadata
//! to avoid scanning all rows. For Fluree's V3 index, we do this as follows:
//! - for numeric predicates, use each POST leaflet's first/last key as the
//!   MIN/MAX candidate (numeric `o_key` encodings are order-preserving)
//! - for string-dict predicates on lex-sorted indexes (`lex_sorted_string_ids`,
//!   i.e. bulk imports), reduce leaflet boundary keys across all datatype/language
//!   groups by raw `(str_id, dt_id, lang_id)` — the same order the fallback
//!   aggregate applies to `EncodedLit` bindings
//!
//! Boundary keys are only a leaflet's extreme within its own `o_type` when the
//! leaflet is `o_type`-homogeneous; the few leaflets containing an `o_type`
//! region edge are column-scanned so an extreme hidden mid-leaflet is not missed.
//!
//! This reduces work from O(rows) decode/materialization to O(leaflets) — only leaflet
//! directory keys are read. The row-scanning aggregates (`SUM`/`AVG`/`COUNT(DISTINCT)`)
//! live in [`crate::fast_predicate_scalar_agg`]; routing MIN/MAX through that per-row
//! cursor would regress them to O(rows).

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    cursor_fast_path_for_predicate, fast_path_store_policy_cleared, leaf_entries_for_predicate,
    normalize_pred_sid, projection_otype_okey, FastPathOperator, PredicateFastPath,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
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
            // O1: keep the fast path only when the scanned predicate is provably
            // uncovered by the view policy; otherwise defer to the fallback, which
            // computes the correct aggregate over the policy-filtered input (MIN/MAX
            // of an empty input is Unbound).
            if let Some(store) = ctx.binary_store.as_ref() {
                let pred_sid = normalize_pred_sid(store, &predicate)?;
                if !matches!(
                    cursor_fast_path_for_predicate(ctx, &pred_sid),
                    PredicateFastPath::Allow
                ) {
                    return Ok(None);
                }
            }
            let Some(store) = fast_path_store_policy_cleared(ctx) else {
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

/// Candidate identity, ordered to match the fallback aggregate's `EncodedLit`
/// comparison: `(o_key, dt_id, lang_id)`. On lex-sorted indexes (the only place
/// the string path runs) `str_id` order equals UTF-8 byte order of the values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct EncodedStringIdentity {
    /// String dictionary ID.
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

/// Compute MIN/MAX candidate for a string-dict predicate from POST leaflet metadata.
///
/// Sound only when string dictionary IDs are lexicographically assigned
/// (`lex_sorted_string_ids`): a homogeneous leaflet's boundary key is then its
/// lexicographic extreme, and raw [`EncodedStringIdentity`] comparison across
/// datatype/language groups matches the fallback aggregate's ordering. Without
/// that property the true extreme can sit mid-leaflet where directory keys never
/// surface it, so we decline.
///
/// `o_type`-heterogeneous leaflets (a datatype/language region edge falls inside —
/// at most one leaflet per region) are column-scanned; every row is a valid
/// candidate, so no per-region bookkeeping is needed.
///
/// Returns `None` when any object is not string-dict-backed.
fn minmax_string_dict_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    mode: MinMaxMode,
) -> Result<Option<Binding>> {
    if !store.lex_sorted_string_ids() {
        return Ok(None);
    }
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);

    let mut best: Option<(EncodedStringIdentity, Binding)> = None;

    for leaf_entry in leaves {
        // Directory-only prefix read (cached); the full leaf blob is opened
        // lazily and only for o_type-heterogeneous leaflets.
        let dir = store
            .open_leaf_dir(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
        let mut handle = None;

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const.is_some() {
                let rr = match mode {
                    MinMaxMode::Min => read_ordered_key_v2(RunSortOrder::Post, &entry.first_key),
                    MinMaxMode::Max => read_ordered_key_v2(RunSortOrder::Post, &entry.last_key),
                };
                if !consider_string_candidate(&mut best, rr.o_type, rr.o_key, p_id, mode) {
                    return Ok(None);
                }
            } else {
                if handle.is_none() {
                    handle = Some(
                        store
                            .open_leaf_handle(
                                &leaf_entry.leaf_cid,
                                leaf_entry.sidecar_cid.as_ref(),
                                false,
                            )
                            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                    );
                }
                let batch = handle
                    .as_ref()
                    .expect("handle opened above")
                    .load_columns(leaflet_idx, &projection_otype_okey(), RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                for row in 0..batch.row_count {
                    let o_type = batch.o_type.get_or(row, 0);
                    let o_key = batch.o_key.get(row);
                    if !consider_string_candidate(&mut best, o_type, o_key, p_id, mode) {
                        return Ok(None);
                    }
                }
            }
        }
    }

    Ok(best.map(|(_, b)| b))
}

/// Fold one `(o_type, o_key)` candidate into `best`.
///
/// Returns `false` for non-string-dict objects: the caller must decline the
/// fast path and defer to the planned pipeline.
fn consider_string_candidate(
    best: &mut Option<(EncodedStringIdentity, Binding)>,
    o_type: u16,
    o_key: u64,
    p_id: u32,
    mode: MinMaxMode,
) -> bool {
    let Some(candidate) = encoded_lit_from_otype(o_type, o_key, p_id) else {
        return false;
    };
    match best {
        None => *best = Some(candidate),
        Some((best_id, _)) => {
            let better = match mode {
                MinMaxMode::Min => candidate.0 < *best_id,
                MinMaxMode::Max => candidate.0 > *best_id,
            };
            if better {
                *best = Some(candidate);
            }
        }
    }
    true
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
        // Directory-only prefix read (cached); this path never loads columns.
        let dir = store
            .open_leaf_dir(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            // A heterogeneous leaflet can hide another o_type's extreme
            // mid-leaflet where boundary keys never surface it; the numeric
            // path only supports a single o_type anyway, so decline.
            if entry.o_type_const.is_none() {
                return Ok(None);
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
