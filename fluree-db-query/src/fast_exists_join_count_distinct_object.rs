//! Fast-path for `COUNT(DISTINCT ?o)` with an existence-only join on the same subject.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(DISTINCT ?o1) AS ?count)
//! WHERE { ?s <p_count> ?o1 . ?s <p_exists> ?o2 . }
//! ```
//!
//! When `?o2` is not otherwise needed, the second triple is an existence constraint
//! on `?s`. The generic pipeline would materialize bindings and hash decoded
//! values for distinctness.
//!
//! This operator instead:
//! - builds a subject set for `<p_exists>` by scanning PSOT (SId column only)
//! - scans POST for `<p_count>` and streams through sorted `(o_key, s_id)` rows
//! - increments the distinct counter once per object group that has any subject in the set
//! - never decodes subject/object values

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store,
    leaf_entries_for_predicate, normalize_pred_sid, projection_sid_okey, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::ir::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;

pub fn exists_join_count_distinct_object_operator(
    count_predicate: Ref,
    exists_predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_distinct_object_with_exists_subject_post(
                store,
                ctx.binary_g_id,
                &count_predicate,
                &exists_predicate,
            )? {
                Some(count) => Ok(Some(build_count_batch(out_var, count as i64)?)),
                None => Ok(None),
            }
        },
        fallback,
        "EXISTS-join COUNT(DISTINCT ?o)",
    )
}

/// COUNT DISTINCT objects for a bound predicate by scanning POST, restricted by a subject set.
///
/// Returns `None` when the fast-path cannot guarantee correctness (e.g., mixed o_type).
fn count_distinct_object_with_exists_subject_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    count_predicate: &Ref,
    exists_predicate: &Ref,
) -> Result<Option<u64>> {
    let count_sid = normalize_pred_sid(store, count_predicate)?;
    let exists_sid = normalize_pred_sid(store, exists_predicate)?;

    let Some(p_count) = store.sid_to_p_id(&count_sid) else {
        return Ok(Some(0));
    };
    let Some(p_exists) = store.sid_to_p_id(&exists_sid) else {
        return Ok(Some(0));
    };

    let subjects = collect_subjects_for_predicate_set(store, g_id, p_exists)?;
    if subjects.is_empty() {
        return Ok(Some(0));
    }

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_count);

    // For now: only handle IRI_REF objects (benchmark predicates like dblp:bibtexType).
    let required_o_type = OType::IRI_REF.as_u16();

    let projection = projection_sid_okey();

    let mut distinct: u64 = 0;
    let mut current_okey: Option<u64> = None;
    let mut group_has_match = false;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_count) {
                continue;
            }
            if entry.o_type_const != Some(required_o_type) {
                return Ok(None);
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let okey = batch.o_key.get(row);
                if current_okey != Some(okey) {
                    if current_okey.is_some() && group_has_match {
                        distinct += 1;
                    }
                    current_okey = Some(okey);
                    group_has_match = false;
                }

                let s_id = batch.s_id.get(row);
                if subjects.contains(&s_id) {
                    group_has_match = true;
                }
            }
        }
    }

    if current_okey.is_some() && group_has_match {
        distinct += 1;
    }

    Ok(Some(distinct))
}
