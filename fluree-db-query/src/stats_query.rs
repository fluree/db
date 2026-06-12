//! Fast-path operator for per-predicate count queries.
//!
//! Answers `SELECT ?p (COUNT(?s) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p`
//! with **exact** counts read from POST leaf-directory metadata —
//! `O(leaflets)` directory reads, decoding only the rare mixed-predicate
//! leaflet — instead of scanning all triples.
//!
//! ## Why not answer from `IndexStats` / `StatsView`?
//!
//! A previous version of this operator returned `StatsView` property
//! counts directly. The differential harness
//! (`fluree-db-api/tests/it_differential_fastpath.rs`, FD-3) showed those
//! numbers are planner *estimates*, not current-state fact counts: they
//! count duplicate re-asserts that set-semantics novelty application
//! dedups, track `rdf:type` asymmetrically between indexer-built and
//! novelty-accumulated stats, and apply novelty deltas inconsistently per
//! predicate. Selectivity estimation tolerates all of that; an exact
//! `COUNT` answer does not.
//!
//! V3 leaflets store current-state rows only (history lives in the
//! sidecar), so per-predicate row counts from leaf directories are exact —
//! but only for the persisted index, which is why this path is gated by
//! [`fast_path_store`] (binary store present, no overlay epoch, query at
//! the store's `max_t`, root policy, single ledger). Anything else returns
//! `Ok(None)` and the planned fallback (the generic scan + group + count
//! pipeline) runs instead.

use crate::binding::{Batch, Binding};
use crate::context::WellKnownDatatypes;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{fast_path_store, FastPathOperator};
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnProjection, ColumnSet};
use fluree_db_core::{FlakeValue, GraphId};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Build the fused per-predicate count operator. Emits `(pred, count)`
/// rows when the exact directory-count gate holds; defers to `fallback`
/// otherwise.
pub fn stats_count_by_predicate_operator(
    pred_var: VarId,
    count_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    let schema: Arc<[VarId]> = Arc::from(vec![pred_var, count_var].into_boxed_slice());
    let batch_schema = schema.clone();
    FastPathOperator::with_schema(
        schema,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let Some(counts) = exact_count_by_predicate_post(store, ctx.binary_g_id)? else {
                return Ok(None);
            };

            let dt = WellKnownDatatypes::new().xsd_long;
            let mut pred_col: Vec<Binding> = Vec::with_capacity(counts.len());
            let mut count_col: Vec<Binding> = Vec::with_capacity(counts.len());
            for (p_id, count) in counts {
                // Directory p_ids are persisted-store IDs by construction;
                // an unresolvable one means the store and its directories
                // disagree — fall back rather than emit a partial result.
                let Some(iri) = store.resolve_predicate_iri(p_id) else {
                    return Ok(None);
                };
                pred_col.push(Binding::sid(store.encode_iri(iri)));
                count_col.push(Binding::lit(FlakeValue::Long(count), dt.clone()));
            }
            let batch = Batch::new(batch_schema.clone(), vec![pred_col, count_col])
                .map_err(|e| QueryError::execution(format!("stats count batch: {e}")))?;
            Ok(Some(batch))
        },
        fallback,
        "COUNT by predicate (directory)",
    )
}

/// Exact per-predicate row counts from POST leaf directories.
///
/// Homogeneous leaflets (`p_const = Some(p)`) contribute `row_count`
/// without any payload read; mixed-predicate leaflets decode only the
/// `PId` column. Returns `Ok(None)` when the graph has no POST branch —
/// the caller falls back rather than asserting emptiness from absence.
fn exact_count_by_predicate_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
) -> Result<Option<Vec<(u32, i64)>>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
        return Ok(None);
    };

    let pid_projection = ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::PId);
            s
        },
    };

    let mut counts: BTreeMap<u32, i64> = BTreeMap::new();
    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if let Some(p) = entry.p_const {
                *counts.entry(p).or_insert(0) += i64::from(entry.row_count);
            } else {
                let batch = handle
                    .load_columns(leaflet_idx, &pid_projection, RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                for row in 0..batch.row_count {
                    *counts.entry(batch.p_id.get(row)).or_insert(0) += 1;
                }
            }
        }
    }

    Ok(Some(counts.into_iter().collect()))
}
