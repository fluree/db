//! Fast-path: `COUNT(*)` for a single transitive property path (`+`) with a fixed endpoint.
//!
//! Targets queries like:
//! - `SELECT (COUNT(*) AS ?count) WHERE { <S> <p>+ ?o }`
//! - (future) `SELECT (COUNT(*) AS ?count) WHERE { ?s <p>+ <O> }`
//!
//! This avoids the generic `PropertyPathOperator`'s repeated range scans by:
//! - scanning PSOT(p) once to build an adjacency map of ref-only edges
//! - running a BFS/visited traversal from the fixed seed and counting unique reachable endpoints
//!
//! Semantics for `+` (one-or-more):
//! - does NOT include the start node unless there is a non-zero-length cycle back to it
//! - traverses only IRI_REF edges (ref-only), matching existing property path behavior

use crate::error::Result;
use crate::fast_path_common::{
    build_count_batch, build_iri_adjacency_from_cursor, build_psot_cursor_for_predicate,
    count_to_i64, cursor_projection_sid_otype_okey, fast_path_store, reach_count_plus,
    reach_count_plus_multi, subject_ref_to_s_id, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::ir::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_core::o_type::OType;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

/// Create a fused operator that outputs a single-row batch with the COUNT(*) result.
pub fn property_path_plus_count_all_operator(
    predicate: fluree_db_core::Sid,
    subject: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_reachable_plus_from_fixed_subject(
                store,
                ctx,
                ctx.binary_g_id,
                &predicate,
                &subject,
            )?;
            match count {
                Some(n) => Ok(Some(build_count_batch(
                    out_var,
                    count_to_i64(n, "COUNT(*) property-path+")?,
                )?)),
                None => Ok(None),
            }
        },
        fallback,
        "property-path+ COUNT(*)",
    )
}

fn count_reachable_plus_from_fixed_subject(
    store: &std::sync::Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &crate::context::ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    pred_sid: &fluree_db_core::Sid,
    subj: &Ref,
) -> Result<Option<u64>> {
    let Some(p_id) = store.sid_to_p_id(pred_sid) else {
        return if ctx.overlay.is_some() {
            Ok(None)
        } else {
            Ok(Some(0))
        };
    };

    let projection = cursor_projection_sid_otype_okey();
    let Some(mut cursor) =
        build_psot_cursor_for_predicate(ctx, store, g_id, pred_sid.clone(), p_id, projection)?
    else {
        return Ok(None);
    };

    if let Some(seed) = subject_ref_to_s_id(ctx.active_snapshot, store, subj)? {
        let adj = build_iri_adjacency_from_cursor(&mut cursor)?;
        return Ok(Some(reach_count_plus(&adj, seed)));
    }

    let target_iri = match subj {
        Ref::Iri(iri) => Some(iri.to_string()),
        Ref::Sid(sid) => ctx
            .active_snapshot
            .decode_sid(sid)
            .or_else(|| store.sid_to_iri(sid)),
        Ref::Var(_) => None,
    };
    let Some(target_iri) = target_iri else {
        return Ok(None);
    };

    let iri_ref = OType::IRI_REF.as_u16();
    let mut adj: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
    let mut starts: Vec<u64> = Vec::new();
    let mut seen_starts: HashSet<u64> = HashSet::new();
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| crate::error::QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for i in 0..batch.row_count {
            if batch.o_type.get(i) != iri_ref {
                continue;
            }
            let s = batch.s_id.get(i);
            let o = batch.o_key.get(i);
            adj.entry(s).or_default().push(o);
            if seen_starts.contains(&s) {
                continue;
            }
            if store.resolve_subject_iri(s).ok().as_deref() == Some(target_iri.as_str()) {
                seen_starts.insert(s);
                starts.push(s);
            }
        }
    }
    Ok(Some(reach_count_plus_multi(&adj, &starts)))
}
