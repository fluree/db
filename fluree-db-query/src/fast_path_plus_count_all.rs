//! Fast-path `COUNT(*)` operators for property-path (`+`) shapes.
//!
//! Two related shapes share the same machinery — a PSOT(p) cursor folded into an
//! IRI adjacency map plus a reachability count — so they live together:
//!
//! - **Single transitive path with a fixed endpoint**
//!   `SELECT (COUNT(*) AS ?c) WHERE { <S> <p>+ ?o }`
//!   (`property_path_plus_count_all_operator`)
//! - **Two-step path whose second step is transitive**
//!   `SELECT (COUNT(*) AS ?c) WHERE { ?s <p1> ?x . ?x <p2>+ ?o }`
//!   (`transitive_path_plus_count_all_operator`)
//!
//! Both build adjacency once via [`build_psot_cursor_for_predicate`] +
//! [`build_iri_adjacency_from_cursor`] and count unique reachable endpoints with
//! [`reach_count_plus`] / [`reach_count_plus_multi`], avoiding the generic
//! `PropertyPathOperator`'s repeated range scans / closure materialization.
//!
//! Both gate on [`allow_cursor_fast_path`] (strategy (b)): the PSOT cursor folds
//! the novelty overlay in and honors `to_t`, so these paths stay correct under an
//! uncommitted overlay and at `to_t < max_t` — they only require single-ledger,
//! no `from_t`, and root (or no) policy.
//!
//! Semantics for `+` (one-or-more):
//! - does NOT include the start node unless there is a non-zero-length cycle back to it
//! - traverses only IRI_REF edges (ref-only), matching existing property path behavior

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, build_count_batch, build_iri_adjacency_from_cursor,
    build_psot_cursor_for_predicate, count_to_i64, cursor_projection_sid_otype_okey,
    normalize_pred_sid, reach_count_plus, reach_count_plus_multi, subject_ref_to_s_id,
    FastPathOperator,
};
use crate::ir::triple::Ref;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_core::o_type::OType;
use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// 1) Single transitive path with a fixed endpoint: `<S> <p>+ ?o`
// ---------------------------------------------------------------------------

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
            if !allow_cursor_fast_path(ctx) {
                return Ok(None);
            }
            let Some(store) = ctx.binary_store.as_ref() else {
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
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
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

    // The fixed subject was not found in the persisted dictionary. The fallback
    // below matches it by scanning `resolve_subject_iri`, which is persisted-only:
    // under an uncommitted overlay the start subject may exist solely in novelty
    // (e.g. `ex:new` inserted but not yet indexed), so it would never match and we
    // would undercount to 0. Bail to the (correct) generic pipeline in that case.
    if ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0
    {
        return Ok(None);
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

// ---------------------------------------------------------------------------
// 2) Two-step path whose second step is transitive: `?s <p1> ?x . ?x <p2>+ ?o`
// ---------------------------------------------------------------------------

/// Create a fused operator for COUNT(*) over a 2-step path with transitive `+`.
pub fn transitive_path_plus_count_all_operator(
    p1: Ref,
    p2: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            if !allow_cursor_fast_path(ctx) {
                return Ok(None);
            }
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None);
            };
            let Some(count) = count_p1_then_p2_plus(store, ctx, ctx.binary_g_id, &p1, &p2)? else {
                return Ok(None);
            };
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in transitive-path+ fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "transitive-path+ COUNT(*)",
    )
}

fn count_p1_then_p2_plus(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &crate::context::ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    p1: &Ref,
    p2: &Ref,
) -> Result<Option<u64>> {
    let overlay_has_rows = ctx
        .overlay
        .map(fluree_db_core::OverlayProvider::epoch)
        .unwrap_or(0)
        != 0;
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(0))
        };
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return if overlay_has_rows {
            Ok(None)
        } else {
            Ok(Some(0))
        };
    };

    // Build adjacency from PSOT(p2).
    let Some(mut cursor2) = build_psot_cursor_for_predicate(
        ctx,
        store,
        g_id,
        p2_sid,
        p2_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };
    let adj = build_iri_adjacency_from_cursor(&mut cursor2)?;

    // Stream p1 grouped by subject and union endpoints across multiple start nodes if needed.
    let Some(mut cursor1) = build_psot_cursor_for_predicate(
        ctx,
        store,
        g_id,
        p1_sid,
        p1_id,
        cursor_projection_sid_otype_okey(),
    )?
    else {
        return Ok(None);
    };

    let iri_ref = OType::IRI_REF.as_u16();
    let mut memo: FxHashMap<u64, u64> = FxHashMap::default();
    let mut total: u64 = 0;

    let mut cur_s: Option<u64> = None;
    let mut cur_starts: Vec<u64> = Vec::new();

    let mut flush_group = |starts: &mut Vec<u64>| -> u64 {
        match starts.len() {
            0 => 0,
            1 => {
                let x = starts[0];
                if let Some(&v) = memo.get(&x) {
                    v
                } else {
                    let v = reach_count_plus(&adj, x);
                    memo.insert(x, v);
                    v
                }
            }
            _ => reach_count_plus_multi(&adj, starts),
        }
    };

    while let Some(batch) = cursor1
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for i in 0..batch.row_count {
            let s = batch.s_id.get(i);
            if cur_s != Some(s) {
                if cur_s.is_some() {
                    let add = flush_group(&mut cur_starts);
                    total = total.saturating_add(add);
                }
                cur_s = Some(s);
                cur_starts.clear();
            }
            if batch.o_type.get(i) != iri_ref {
                continue;
            }
            let x = batch.o_key.get(i);
            if !cur_starts.contains(&x) {
                cur_starts.push(x);
            }
        }
    }

    if cur_s.is_some() {
        let add = flush_group(&mut cur_starts);
        total = total.saturating_add(add);
    }

    Ok(Some(total))
}
