//! Fast-path for `SUM(STRLEN(GROUP_CONCAT(...)))` over a single predicate.
//!
//! Targets benchmark-style nested queries like:
//!
//! ```sparql
//! SELECT (SUM(STRLEN(?cat)) AS ?sum) {
//!   { SELECT (GROUP_CONCAT(?o; SEPARATOR=" ") AS ?cat)
//!     { ?s <p> ?o . }
//!     GROUP BY ?s
//!   }
//! }
//! ```
//!
//! The generic pipeline materializes large concatenated strings per subject group,
//! then computes STRLEN, then SUM — dominated by allocation and copying.
//!
//! This operator computes the same result without building concatenated strings:
//! for each subject group,
//!
//! `strlen(group_concat(values, sep)) = sum(strlen(v_i)) + (n-1) * strlen(sep)` (when n>0).
//!
//! It scans PSOT for the predicate, groups by `s_id`, and accumulates UTF-8 codepoint
//! lengths directly from the string dictionary.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    fast_path_store, leaf_entries_for_predicate, normalize_pred_sid, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnProjection, ColumnSet};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::{FlakeValue, GraphId, Sid};
use std::sync::Arc;

/// Create a fused operator for SUM(STRLEN(GROUP_CONCAT(...))) over a single predicate.
pub fn sum_strlen_group_concat_operator(
    predicate: Ref,
    separator: Arc<str>,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let outcome =
                sum_strlen_group_concat_psot(store, ctx.binary_g_id, &predicate, &separator)?;
            match outcome {
                FastSumStrlenGroupConcat::Supported(sum) => {
                    let schema = Arc::from(vec![out_var].into_boxed_slice());
                    let col = vec![match sum {
                        Some(v) => Binding::lit(FlakeValue::Long(v), Sid::xsd_integer()),
                        None => Binding::Unbound,
                    }];
                    let batch = Batch::new(schema, vec![col]).map_err(|e| {
                        QueryError::execution(format!("sum strlen group_concat batch build: {e}"))
                    })?;
                    Ok(Some(batch))
                }
                FastSumStrlenGroupConcat::Unsupported => Ok(None),
            }
        },
        fallback,
        "SUM(STRLEN(GROUP_CONCAT))",
    )
}

fn utf8_codepoint_count(bytes: &[u8]) -> usize {
    // Count non-continuation bytes (valid UTF-8 assumption).
    bytes
        .iter()
        .filter(|b| (**b & 0b1100_0000) != 0b1000_0000)
        .count()
}

enum FastSumStrlenGroupConcat {
    /// Fast path is supported; value is `Some(sum)` when at least one row exists,
    /// otherwise `None` for empty input (SPARQL aggregate over empty).
    Supported(Option<i64>),
    /// Fast path is not supported (e.g., mixed non-string object types); caller must fall back.
    Unsupported,
}

fn sum_strlen_group_concat_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
    separator: &str,
) -> Result<FastSumStrlenGroupConcat> {
    let p_sid = normalize_pred_sid(store, predicate)?;
    let p_id = store.sid_to_p_id(&p_sid).ok_or_else(|| {
        QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
    })?;

    let sep_len = separator.chars().count() as i64;

    // Projection: only load SId, OType, OKey for PSOT.
    let projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
    };

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);

    let mut scratch: Vec<u8> = Vec::new();
    let mut have_any = false;

    let mut current_s: Option<u64> = None;
    let mut group_count: i64 = 0;
    let mut group_sum: i64 = 0;

    let mut total_sum: i64 = 0;

    let flush_group = |total_sum: &mut i64, group_count: i64, group_sum: i64, sep_len: i64| {
        if group_count > 0 {
            *total_sum += group_sum + (group_count - 1) * sep_len;
        }
    };

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

            for row in 0..batch.row_count {
                let s_id = batch.s_id.get(row);
                let o_type = batch.o_type.get_or(row, 0);
                let o_key = batch.o_key.get(row);

                if current_s != Some(s_id) {
                    if current_s.is_some() {
                        flush_group(&mut total_sum, group_count, group_sum, sep_len);
                    }
                    current_s = Some(s_id);
                    group_count = 0;
                    group_sum = 0;
                }

                // Only support string-dict backed values (xsd:string, rdf:langString, etc.).
                // If other types appear, fall back to the generic pipeline for correctness.
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::StringDict {
                    return Ok(FastSumStrlenGroupConcat::Unsupported);
                }
                let str_id: u32 = o_key
                    .try_into()
                    .map_err(|_| QueryError::execution("string o_key out of range"))?;

                scratch.clear();
                let found = store
                    .string_lookup_into(str_id, &mut scratch)
                    .map_err(|e| QueryError::execution(format!("string_lookup_into: {e}")))?;
                if !found {
                    return Err(QueryError::execution("string id not found"));
                }

                have_any = true;
                group_count += 1;
                group_sum += utf8_codepoint_count(&scratch) as i64;
            }
        }
    }

    if current_s.is_some() {
        flush_group(&mut total_sum, group_count, group_sum, sep_len);
    }

    Ok(FastSumStrlenGroupConcat::Supported(
        have_any.then_some(total_sum),
    ))
}
