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
//! Per subject group, `strlen(group_concat(values, sep)) = sum(strlen(v_i)) +
//! (n-1)·strlen(sep)`; summed over all groups the per-subject bookkeeping
//! cancels out entirely:
//!
//! `total = Σ strlen(o) + (N_rows − N_subjects)·strlen(sep)`
//!
//! so the answer assembles from three primitives that never touch per-row
//! subject IDs: the parallel per-distinct STRLEN fold over POST
//! ([`crate::fast_string_fold`]), the metadata row count, and the PSOT
//! lead-group distinct-subject directory walk. The previous implementation
//! scanned PSOT serially with one random-order dictionary lookup per row,
//! which timed out at billions of rows.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_count::count_distinct_subjects_for_predicate;
use crate::fast_path_common::{
    count_rows_for_predicate_psot, count_to_i64, cursor_fast_path_for_predicate,
    fast_path_store_policy_cleared, normalize_pred_sid, FastPathOperator, PredicateFastPath,
};
use crate::fast_string_fold::sum_strlen_any_string_dict;
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::BinaryIndexStore;
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
            // O1: keep the fast path only when the scanned predicate is provably
            // uncovered by the view policy; otherwise defer to the fallback, which
            // computes the correct aggregate over the policy-filtered input.
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
            let outcome = sum_strlen_group_concat_fold(
                store,
                ctx.binary_g_id,
                &predicate,
                &separator,
                &ctx.cancellation,
            )?;
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

enum FastSumStrlenGroupConcat {
    /// Fast path is supported; value is `Some(sum)` when at least one row exists,
    /// otherwise `None` for empty input (SPARQL aggregate over empty).
    Supported(Option<i64>),
    /// Fast path is not supported (e.g., mixed non-string object types); caller must fall back.
    Unsupported,
}

fn sum_strlen_group_concat_fold(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    predicate: &Ref,
    separator: &str,
    cancellation: &fluree_db_core::QueryCancellation,
) -> Result<FastSumStrlenGroupConcat> {
    let p_sid = normalize_pred_sid(store, predicate)?;
    let Some(p_id) = store.sid_to_p_id(&p_sid) else {
        // Predicate absent -> empty input -> aggregate over nothing.
        return Ok(FastSumStrlenGroupConcat::Supported(None));
    };

    let total_rows = count_rows_for_predicate_psot(store, g_id, p_id)?;
    if total_rows == 0 {
        return Ok(FastSumStrlenGroupConcat::Supported(None));
    }
    let Some(subjects) = count_distinct_subjects_for_predicate(store, g_id, p_id)? else {
        return Ok(FastSumStrlenGroupConcat::Unsupported);
    };
    let Some(strlen_sum) = sum_strlen_any_string_dict(store, g_id, p_id, cancellation)? else {
        return Ok(FastSumStrlenGroupConcat::Unsupported);
    };

    let strlen_sum = count_to_i64(strlen_sum, "SUM(STRLEN(GROUP_CONCAT)) values")?;
    let separators = count_to_i64(
        total_rows.saturating_sub(subjects),
        "SUM(STRLEN(GROUP_CONCAT)) separators",
    )?;
    let sep_len = separator.chars().count() as i64;
    let total = strlen_sum.saturating_add(separators.saturating_mul(sep_len));

    Ok(FastSumStrlenGroupConcat::Supported(Some(total)))
}
