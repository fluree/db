//! Fast-path: `COUNT(*)` for a 2-step path where the second step is transitive `+`.
//!
//! Targets benchmark-style query:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?s <p1> ?x . ?x <p2>+ ?o }`
//!
//! In SPARQL, property paths have set-like semantics for the reached endpoints per start node.
//! This fast-path avoids materializing `(s, o)` rows by:
//! - building the `p2` adjacency once from PSOT(p2) (ref-only edges)
//! - streaming PSOT(p1) grouped by subject and computing:
//!   - if there is exactly one distinct `x`: `reach_count_plus(x)` (memoized)
//!   - if there are multiple `x` values: `|⋃ reach_plus(x_i)|` via a multi-source BFS
//! - summing per-subject reachable endpoint counts.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, build_iri_adjacency_from_cursor, build_psot_cursor_for_predicate,
    cursor_projection_sid_otype_okey, normalize_pred_sid, reach_count_plus, reach_count_plus_multi,
    FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_core::o_type::OType;
use rustc_hash::FxHashMap;
use std::sync::Arc;

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
            // Cursor-based path is overlay-aware, but still requires single-ledger,
            // non-history, root-policy execution.
            let allow_fast = !ctx.is_multi_ledger()
                && !ctx.history_mode
                && ctx.from_t.is_none()
                && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root());
            if !allow_fast {
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
