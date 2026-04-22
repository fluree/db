//! Fast-path: `COUNT(*)` for single-predicate string-prefix filters.
//!
//! Targets shapes like:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p> ?o FILTER REGEX(?o, "^Com") }`
//! and
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p> ?o FILTER STRSTARTS(?o, "Com") }`
//!
//! When the index root marks string IDs as lexicographically sorted, all string
//! dictionary IDs matching a given prefix form one or more contiguous ranges.
//! We can then count matching rows by scanning bounded OPST slices instead of
//! scanning the whole predicate partition and evaluating the string function per row.

use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, build_i64_singleton_batch, contiguous_id_range, fast_path_store,
    ref_to_p_id, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{BinaryCursor, BinaryFilter, ColumnProjection, ColumnSet};
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
    let Some(store) = fast_path_store(ctx) else {
        return Ok(None);
    };

    // This fast path currently counts persisted index rows only.
    // `ExecutionContext` usually carries an overlay provider even when there
    // is no novelty (`NoOverlay` has epoch 0), so only fall back when the
    // overlay actually contains delta rows.
    if ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0
    {
        return Ok(None);
    }
    if !store.lex_sorted_string_ids() {
        return Ok(None);
    }

    let p_id = ref_to_p_id(ctx, store.as_ref(), pred)?;
    let string_ids = store
        .find_strings_by_prefix(prefix)
        .map_err(|e| QueryError::Internal(format!("find_strings_by_prefix: {e}")))?;

    if string_ids.is_empty() {
        return Ok(Some(0));
    }

    let Ok(id_ranges) = contiguous_id_range(&string_ids) else {
        return Ok(None);
    };
    count_prefix_rows_opst(store, ctx.binary_g_id, p_id, &id_ranges, ctx.to_t).map(Some)
}

fn count_prefix_rows_opst(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    id_ranges: &[(u32, u32)],
    to_t: i64,
) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Opst) else {
        return Ok(0);
    };
    let branch = Arc::clone(branch);
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
                o_i: u32::MAX,
                o_type,
                g_id,
            };
            let filter = BinaryFilter {
                p_id: Some(p_id),
                o_type: Some(o_type),
                ..Default::default()
            };
            let mut cursor = BinaryCursor::new(
                Arc::clone(store),
                RunSortOrder::Opst,
                Arc::clone(&branch),
                &min_key,
                &max_key,
                filter,
                projection,
            );
            cursor.set_to_t(to_t);

            while let Some(batch) = cursor
                .next_batch()
                .map_err(|e| QueryError::Internal(format!("binary cursor: {e}")))?
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
    use crate::fast_path_common::contiguous_id_range;

    #[test]
    fn prefix_ids_accept_single_contiguous_run() {
        let ranges = contiguous_id_range(&[10, 11, 12]).expect("contiguous ids");
        assert_eq!(ranges, vec![(10, 12)]);
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
