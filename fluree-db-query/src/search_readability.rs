//! Shared view-policy readability check for index-search operators
//! (BM25, vector, geo, S2).
//!
//! A search hit is a matched *subject*, not a flake, so search operators bypass
//! the per-flake policy filtering that scan operators apply. To keep search
//! results consistent with the rest of the engine, each hit is re-checked here
//! against the standard per-flake evaluator on the subject's *searched-property*
//! flakes — the content that could have produced the match. If every searched
//! flake of the subject is hidden, the hit is dropped.
//!
//! Semantics (inherited from the standard evaluator, see
//! [`BinaryScanOperator::filter_flakes_by_policy`]):
//! - property / class / `f:query` policies all apply exactly as they do to any
//!   flake (class membership via the populated class cache);
//! - permissive **any-viewable**: the hit survives if *any one* searched flake of
//!   the subject is viewable;
//! - no-op (visible) for root / no policy.
//!
//! Two entry points are provided: [`search_hit_readable`] for operators that
//! carry the subject/predicate as IRIs (BM25, vector, S2), and
//! [`search_hit_readable_sids`] for operators that already hold them as `Sid`s in
//! the active graph's space (geo). Both fold into the active graph's per-graph
//! context first, because in dataset mode the policy enforcer lives on the
//! `GraphRef`, not the top-level context that search operators run in.

use crate::binary_scan::BinaryScanOperator;
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::Result;
use fluree_db_core::{range_with_overlay, IndexType, RangeMatch, RangeOptions, RangeTest, Sid};

/// Readability for a hit identified by IRIs. See module docs.
pub(crate) async fn search_hit_readable(
    ctx: &ExecutionContext<'_>,
    subject_iri: &str,
    searched_pred_iris: &[&str],
) -> Result<bool> {
    match ctx.active_graphs() {
        ActiveGraphs::Single => visible_iris(ctx, subject_iri, searched_pred_iris).await,
        ActiveGraphs::Many(graphs) => {
            if graphs.len() == 1 {
                let graph_ctx = ctx.with_graph_ref(graphs[0]);
                visible_iris(&graph_ctx, subject_iri, searched_pred_iris).await
            } else {
                conservative_multi_graph(&graphs)
            }
        }
    }
}

/// Readability for a hit whose subject and searched predicates are already
/// `Sid`s in the active graph's namespace (e.g. geo, which resolves them while
/// scanning). See module docs.
pub(crate) async fn search_hit_readable_sids(
    ctx: &ExecutionContext<'_>,
    subject_sid: &Sid,
    searched_pred_sids: &[Sid],
) -> Result<bool> {
    match ctx.active_graphs() {
        ActiveGraphs::Single => visible_sids(ctx, subject_sid, searched_pred_sids).await,
        ActiveGraphs::Many(graphs) => {
            if graphs.len() == 1 {
                let graph_ctx = ctx.with_graph_ref(graphs[0]);
                visible_sids(&graph_ctx, subject_sid, searched_pred_sids).await
            } else {
                conservative_multi_graph(&graphs)
            }
        }
    }
}

/// A multi-graph (>1) scope can't be checked per-flake here, so hide
/// conservatively when any active graph enforces a policy, else allow.
fn conservative_multi_graph(graphs: &[&crate::dataset::GraphRef<'_>]) -> Result<bool> {
    Ok(!graphs.iter().any(|g| g.has_policy()))
}

async fn visible_iris(
    ctx: &ExecutionContext<'_>,
    subject_iri: &str,
    searched_pred_iris: &[&str],
) -> Result<bool> {
    if ctx.allow_unfiltered() {
        return Ok(true);
    }
    let Some(subject_sid) = ctx.encode_iri(subject_iri) else {
        // Subject not resolvable in this graph => cannot verify => hide.
        return Ok(false);
    };
    let pred_sids: Vec<Sid> = searched_pred_iris
        .iter()
        .filter_map(|iri| ctx.encode_iri(iri))
        .collect();
    any_visible_flake(ctx, &subject_sid, &pred_sids).await
}

async fn visible_sids(
    ctx: &ExecutionContext<'_>,
    subject_sid: &Sid,
    searched_pred_sids: &[Sid],
) -> Result<bool> {
    if ctx.allow_unfiltered() {
        return Ok(true);
    }
    any_visible_flake(ctx, subject_sid, searched_pred_sids).await
}

/// Reads the subject's flakes on each searched predicate (against a single-graph
/// `ctx` already carrying the graph's enforcer) and runs them through the
/// standard per-flake filter; true on the first viewable one.
async fn any_visible_flake(
    ctx: &ExecutionContext<'_>,
    subject_sid: &Sid,
    searched_pred_sids: &[Sid],
) -> Result<bool> {
    let snapshot = ctx.active_snapshot;
    let overlay = ctx.overlay();
    let to_t = ctx.to_t;
    for pred_sid in searched_pred_sids {
        let range_match = RangeMatch::new()
            .with_subject(subject_sid.clone())
            .with_predicate(pred_sid.clone());
        let flakes = range_with_overlay(
            snapshot,
            ctx.binary_g_id,
            overlay,
            IndexType::Spot,
            RangeTest::Eq,
            range_match,
            RangeOptions::new().with_to_t(to_t),
        )
        .await?;
        let visible = BinaryScanOperator::filter_flakes_by_policy(
            ctx,
            snapshot,
            overlay,
            to_t,
            ctx.binary_g_id,
            flakes,
        )
        .await?;
        if !visible.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}
