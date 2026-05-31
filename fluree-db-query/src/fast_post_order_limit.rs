//! Fast-path: `ORDER BY DESC(?o) LIMIT k` over a single bound predicate's
//! object, optionally filtered by `?s a <Class>`.
//!
//! Targets "latest feed" shapes like:
//! `SELECT ?c ?d WHERE { ?c a <Conversation> ; <dateModified> ?d } ORDER BY DESC(?d) LIMIT 5`
//!
//! The POST index is sorted `(p_id, o_type, o_key, o_i, s_id)`, so for a fixed
//! predicate whose objects share one **order-preserving** `o_type` (numeric /
//! temporal / boolean — see [`is_post_desc_orderable`]), the physical tail of
//! the predicate's POST range IS the `ORDER BY DESC(?o)` top-k. Instead of
//! draining the whole predicate into a top-k `SortOperator`, we walk the POST
//! leaf entries from the tail, decode only the rows we keep, and stop once we
//! have `OFFSET + LIMIT` survivors.
//!
//! ## Two lanes
//!
//! Both lanes require single-ledger, no `from_t`, root (or no) policy, and
//! `to_t >= store.max_t()` — i.e. `to_t` at or after the persisted index point
//! (`index_t`). Querying *before* the index point needs the history sidecar and
//! defers to the generic pipeline. They split on whether novelty is present:
//!
//! - **Base lane** (`epoch == 0`): the persisted index is exact, so a plain
//!   reverse leaf-walk suffices.
//! - **Overlay lane** (`epoch != 0`): the same reverse leaf-walk is merged with
//!   the predicate's resolved novelty ops. This is a *row-set* merge (skip base
//!   rows retracted by overlay; add overlay asserts; dedup by fact identity), so
//!   it sidesteps the "base + asserts − retracts" arithmetic pitfall that only
//!   afflicts overlay-aware *counting*. `rdf:type` class membership is likewise
//!   evaluated overlay-correctly (persisted base ± overlay type asserts/retracts)
//!   rather than via the persisted-only `batched_lookup_predicate_refs` alone.
//!
//! ## Correctness gates (enforced at runtime in the `compute` closure)
//!
//! - **Single order-preserving `o_type`** — proved up front by a directory
//!   prepass over *all* of the predicate's POST leaflets (base lane:
//!   [`single_orderable_o_type`]; overlay lane: [`combined_orderable_o_type`],
//!   which also folds the overlay ops' types). A mixed-leaflet, multi-`o_type`,
//!   or non-order-preserving predicate (strings/refs/arena) bails to the generic
//!   top-k. The full prepass (rather than a streaming check) is required because
//!   the tail walk stops after `OFFSET + LIMIT` rows.
//! - **Embedded-scalar object materialization** — the kept objects are decoded
//!   via `BinaryIndexStore::decode_value_v3` and wrapped by
//!   [`materialized_object_binding`]; the embedded numeric/temporal/boolean
//!   kinds are exactly those `late_materialized_object_binding` declines.
//! - **Absent anchor predicate / class IRI** ⇒ empty result with no overlay; a
//!   bail to the generic pipeline when overlay is present (a novelty-only
//!   predicate/subject would otherwise be silently missed).
//! - **Profitability budget.** If the scan inspects more than [`SCAN_BUDGET`]
//!   rows without filling `OFFSET + LIMIT` survivors (e.g. a highly selective
//!   class filter), we bail to the generic top-k rather than walk the whole
//!   predicate.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    allow_cursor_fast_path, collect_resolved_overlay_ops, empty_batch, is_post_desc_orderable,
    leaf_entries_for_predicate, normalize_pred_sid, projection_sid_okey, projection_sid_okey_oi,
    term_to_ref_s_id,
};
use crate::ir::triple::{Ref, Term};
use crate::object_binding::materialized_object_binding;
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use fluree_db_binary_index::batched_lookup_predicate_refs;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::read::types::OverlayOp;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::{GraphId, Sid};
use rustc_hash::FxHashSet;
use std::sync::Arc;

/// Maximum POST rows the bounded tail scan may inspect before giving up and
/// deferring to the generic top-k. Bounds worst-case wasted work when a class
/// filter is far more selective than the ordering predicate is broad.
const SCAN_BUDGET: usize = 200_000;

/// A collected result row: `(o_type, o_key, s_id)` in descending value order.
type TopKRow = (u16, u64, u64);

/// Overlay-lane working row: the full V3 fact identity (within a single proven
/// `o_type`). `o_i` is carried so dedup/retract operate on whole facts —
/// repeated/list values share `(s_id, o_key)` but differ in `o_i`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct OvRow {
    o_key: u64,
    s_id: u64,
    o_i: u32,
}

impl OvRow {
    /// Sort key for descending merge order, matching the physical POST tie-order
    /// within one `o_type`: `(o_key, o_i, s_id)`.
    #[inline]
    fn cmp_desc_key(&self) -> (u64, u32, u64) {
        (self.o_key, self.o_i, self.s_id)
    }
}

/// Build the reverse-POST `ORDER BY DESC(?o) LIMIT k` fast-path operator.
///
/// `projected` is the SELECT var list (a subset of `{subject_var, object_var}`,
/// in projection order) and becomes the operator's output schema.
#[allow(clippy::too_many_arguments)]
pub fn post_order_desc_limit_operator(
    projected: Vec<VarId>,
    subject_var: VarId,
    object_var: VarId,
    anchor_pred: Ref,
    class_term: Option<Term>,
    distinct: bool,
    limit: usize,
    offset: usize,
    fallback: Option<BoxedOperator>,
) -> BoxedOperator {
    let schema: Arc<[VarId]> = Arc::from(projected.into_boxed_slice());
    let emit_subject = schema.contains(&subject_var);
    Box::new(crate::fast_path_common::FastPathOperator::with_schema(
        Arc::clone(&schema),
        move |ctx| {
            // Single-ledger, no from_t, root policy. (Overlay & to_t are handled
            // below: this gate, unlike `fast_path_store`, permits novelty.)
            let Some(store) = ctx.binary_store.as_ref() else {
                return Ok(None);
            };
            if !allow_cursor_fast_path(ctx) {
                return Ok(None);
            }
            // `store.max_t()` is the persisted index point (`index_t`). The base
            // index plus the novelty overlay (filtered to `to_t`) reconstructs any
            // `to_t >= index_t`: `to_t == index_t` with no novelty is the base
            // lane; `to_t > index_t` carries novelty (overlay lane). Querying
            // *before* the index point needs the history sidecar — defer.
            if ctx.to_t < store.max_t() {
                return Ok(None);
            }
            let overlay_present = ctx
                .overlay
                .map(fluree_db_core::OverlayProvider::epoch)
                .unwrap_or(0)
                != 0;

            let g_id: GraphId = ctx.binary_g_id;
            let need = offset.saturating_add(limit);
            if need == 0 {
                return Ok(Some(empty_batch(schema.clone())?));
            }

            let _span = tracing::debug_span!(
                "fast_post_order_desc_limit",
                anchor = ?anchor_pred,
                class = ?class_term,
                limit,
                offset,
                distinct,
                overlay = overlay_present,
            )
            .entered();

            // Resolve the anchor predicate.
            let anchor_sid = normalize_pred_sid(store.as_ref(), &anchor_pred)?;
            let p_id = match store.sid_to_p_id(&anchor_sid) {
                Some(p) => p,
                None => {
                    // No base rows for the predicate. With overlay, novelty-only
                    // rows may exist (no p_id to reach them) ⇒ defer; otherwise
                    // the (ordered, limited) result is simply empty.
                    if overlay_present {
                        return Ok(None);
                    }
                    return Ok(Some(empty_batch(schema.clone())?));
                }
            };

            // Resolve the optional `?s a <Class>` constraint. A persisted-dict
            // miss means "no members" only when there is no overlay; under
            // overlay the class IRI / rdf:type may be novelty-only, so we defer.
            let class: Option<(u32, u64)> = match &class_term {
                Some(t) => {
                    let Some(rdf_type_p_id) = store.find_predicate_id(fluree_vocab::rdf::TYPE)
                    else {
                        if overlay_present {
                            return Ok(None);
                        }
                        return Ok(Some(empty_batch(schema.clone())?));
                    };
                    let Some(class_s_id) = term_to_ref_s_id(ctx, store.as_ref(), t)? else {
                        if overlay_present {
                            return Ok(None);
                        }
                        return Ok(Some(empty_batch(schema.clone())?));
                    };
                    Some((rdf_type_p_id, class_s_id))
                }
                None => None,
            };

            let rows_opt = if overlay_present {
                collect_post_desc_topk_overlay(
                    ctx,
                    store,
                    g_id,
                    p_id,
                    &anchor_sid,
                    class,
                    distinct,
                    emit_subject,
                    need,
                )?
            } else {
                let Some(o_type) = single_orderable_o_type(store, g_id, p_id)? else {
                    return Ok(None);
                };
                collect_post_desc_topk(
                    store,
                    g_id,
                    p_id,
                    o_type,
                    class,
                    ctx.to_t,
                    distinct,
                    emit_subject,
                    need,
                )?
            };
            let Some(rows) = rows_opt else {
                return Ok(None);
            };

            // `rows` is descending and length <= need; apply OFFSET.
            let out_rows: &[TopKRow] = if offset >= rows.len() {
                &[]
            } else {
                &rows[offset..]
            };

            // Emitted on the fast-path success path (after every bail point);
            // observable in a trace to confirm the fast path served the query.
            tracing::debug!(
                returned = out_rows.len(),
                "fast-path: post-order-desc-limit emitted"
            );

            // Overlay-lane subjects may be novelty-only and thus absent from the
            // persisted dictionary, so `Binding::encoded_sid` (resolved against
            // the persisted dict at serialization) would fail. Materialize them
            // through a novelty-aware graph view instead. The base lane keeps the
            // lazy encoded form (all subjects are persisted there).
            let view = if overlay_present {
                Some(ctx.graph_view().ok_or_else(|| {
                    QueryError::Internal("graph view unavailable for overlay subjects".into())
                })?)
            } else {
                None
            };

            let mut cols: Vec<Vec<Binding>> = Vec::with_capacity(schema.len());
            for var in schema.iter() {
                let mut col: Vec<Binding> = Vec::with_capacity(out_rows.len());
                if *var == subject_var {
                    for (_, _, s_id) in out_rows {
                        let b = match &view {
                            Some(gv) => Binding::sid(
                                gv.resolve_subject_sid(*s_id)
                                    .map_err(|e| QueryError::from_io("resolve_subject_sid", e))?,
                            ),
                            None => Binding::encoded_sid(*s_id),
                        };
                        col.push(b);
                    }
                } else if *var == object_var {
                    // Embedded scalar o_types (numeric/temporal/boolean) decode
                    // from `o_key`; `materialized_object_binding` then attaches the
                    // datatype. (`late_materialized_object_binding` only covers
                    // dict/ref/arena kinds and returns `None` for these.)
                    for (o_type, o_key, _) in out_rows {
                        let val = store
                            .decode_value_v3(*o_type, *o_key, p_id, g_id)
                            .map_err(|e| QueryError::from_io("decode_value", e))?;
                        col.push(materialized_object_binding(
                            store.as_ref(),
                            *o_type,
                            p_id,
                            val,
                            None,
                            None,
                        ));
                    }
                } else {
                    // Schema vars are restricted to {subject_var, object_var} by
                    // the detector; anything else is a planner bug — be safe.
                    return Ok(None);
                }
                cols.push(col);
            }
            Ok(Some(Batch::new(schema.clone(), cols)?))
        },
        fallback,
        "post-order-desc-limit",
    ))
}

/// Directory prepass: `Some(o_type)` iff every stored object of `p_id` shares
/// one order-preserving o_type (numeric/temporal/boolean — see
/// [`is_post_desc_orderable`]). `None` for an empty, mixed-leaflet, multi-type,
/// or non-order-preserving predicate, signaling the caller to defer to the
/// generic top-k. Inspects only leaflet `o_type_const` (no row decode).
fn single_orderable_o_type(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
) -> Result<Option<u16>> {
    let found = base_predicate_o_type(store, g_id, p_id)?;
    match found {
        OneOType::Single(ot) if is_post_desc_orderable(ot) => Ok(Some(ot)),
        _ => Ok(None),
    }
}

/// Like [`single_orderable_o_type`] but also folds the predicate's resolved
/// overlay ops, so a novelty-only or novelty-extended predicate is proven
/// single-typed too. Used by the overlay lane.
fn combined_orderable_o_type(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    ops: &[OverlayOp],
) -> Result<Option<u16>> {
    let mut found = base_predicate_o_type(store, g_id, p_id)?;
    for op in ops {
        found = match found {
            OneOType::Mixed => return Ok(None),
            OneOType::Empty => OneOType::Single(op.o_type),
            OneOType::Single(prev) if prev == op.o_type => OneOType::Single(prev),
            OneOType::Single(_) => return Ok(None),
        };
    }
    match found {
        OneOType::Single(ot) if is_post_desc_orderable(ot) => Ok(Some(ot)),
        _ => Ok(None),
    }
}

/// Outcome of the base-leaflet o_type prepass.
enum OneOType {
    /// No stored base rows for the predicate.
    Empty,
    /// Every base leaflet is homogeneous in this single o_type.
    Single(u16),
    /// A mixed leaflet or more than one o_type — not single-typed.
    Mixed,
}

/// Scan a predicate's POST leaflets and classify its base object o_type(s).
fn base_predicate_o_type(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
) -> Result<OneOType> {
    let mut found = OneOType::Empty;
    for leaf in leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id) {
        let handle = store
            .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        for entry in &handle.dir().entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let Some(ot) = entry.o_type_const else {
                return Ok(OneOType::Mixed); // mixed-type leaflet
            };
            found = match found {
                OneOType::Empty => OneOType::Single(ot),
                OneOType::Single(prev) if prev == ot => OneOType::Single(prev),
                OneOType::Single(_) => return Ok(OneOType::Mixed),
                OneOType::Mixed => return Ok(OneOType::Mixed),
            };
        }
    }
    Ok(found)
}

/// Base lane: bounded reverse-POST tail walk over the persisted index (no
/// overlay). Returns descending `(o_type, o_key, s_id)` rows (class-filtered,
/// DISTINCT-deduped) truncated to `need`, or `Ok(None)` to bail to the generic
/// pipeline.
#[allow(clippy::too_many_arguments)]
fn collect_post_desc_topk(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    expected_o_type: u16,
    class: Option<(u32, u64)>,
    to_t: i64,
    distinct: bool,
    dedup_includes_subject: bool,
    need: usize,
) -> Result<Option<Vec<TopKRow>>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    if leaves.is_empty() {
        return Ok(Some(Vec::new()));
    }

    // Note: the base lane emits one row per physical POST row and does NOT dedup
    // by fact identity, so it needs no `o_i` (unlike the overlay lane). Repeated
    // values of a subject (same `(s,o_key)`, different `o_i`) are distinct facts:
    // for non-DISTINCT they are separate solutions (matching the generic
    // SortOperator's multiset semantics), and DISTINCT collapses them by the
    // projected `(s?, o)` below — neither needs `o_i`.
    let projection = projection_sid_okey();
    let mut results: Vec<TopKRow> = Vec::with_capacity(need);
    // DISTINCT dedup key: (subject-or-0, o_type, o_key). When DISTINCT is set the
    // detector guarantees the object var is projected, so the object component is
    // always meaningful; the subject component is included only when projected.
    let mut seen: FxHashSet<(u64, u16, u64)> = FxHashSet::default();
    let mut scanned: usize = 0;

    // Walk POST leaf entries from the tail (highest object value first).
    for leaf in leaves.iter().rev() {
        let handle = store
            .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        // Collect this leaf's candidate rows in descending order.
        let mut leaf_cands: Vec<TopKRow> = Vec::new();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate().rev() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            // `single_orderable_o_type` already proved the whole predicate is one
            // order-preserving o_type; this is a defensive guard against an
            // inconsistent leaflet (a stale prepass) — bail rather than misorder.
            if entry.o_type_const != Some(expected_o_type) {
                return Ok(None);
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            scanned += batch.row_count;
            for row in (0..batch.row_count).rev() {
                leaf_cands.push((expected_o_type, batch.o_key.get(row), batch.s_id.get(row)));
            }
        }

        // Class membership filter (base lane: overlay-free, persisted == full).
        if let Some((rdf_type_p_id, class_s_id)) = class {
            if !leaf_cands.is_empty() {
                let mut uniq: Vec<u64> = leaf_cands.iter().map(|(_, _, s)| *s).collect();
                uniq.sort_unstable();
                uniq.dedup();
                let type_map =
                    batched_lookup_predicate_refs(store, g_id, rdf_type_p_id, &uniq, to_t)
                        .map_err(|e| {
                            QueryError::Internal(format!("batched_lookup_predicate_refs: {e}"))
                        })?;
                leaf_cands.retain(|(_, _, s)| {
                    type_map
                        .get(s)
                        .is_some_and(|classes| classes.binary_search(&class_s_id).is_ok())
                });
            }
        }

        for (o_type, o_key, s_id) in leaf_cands {
            if distinct {
                let s_key = if dedup_includes_subject { s_id } else { 0 };
                if !seen.insert((s_key, o_type, o_key)) {
                    continue;
                }
            }
            results.push((o_type, o_key, s_id));
            if results.len() >= need {
                return Ok(Some(results));
            }
        }

        if scanned > SCAN_BUDGET && results.len() < need {
            // Class filter (or sparsity) is too selective for a bounded tail
            // scan to pay off; defer to the generic top-k.
            return Ok(None);
        }
    }

    // Exhausted the predicate: `results` is the exact answer (possibly < need).
    Ok(Some(results))
}

/// Overlay lane: the base reverse-POST tail walk merged, in descending value
/// order, with the predicate's resolved novelty ops.
///
/// **Why a row-set merge is correct here** (and the "base + asserts − retracts"
/// arithmetic that's wrong for *counting* is not a problem): we produce the
/// actual live rows, deduping by fact identity `(s_id, o_key, o_i)` (within the
/// single proven `o_type`):
/// - an overlay assert of a brand-new fact → emitted;
/// - an overlay assert equal to a base row (re-assertion) → deduped to one row;
/// - an overlay retract that hits a base row → that base row is skipped;
/// - an overlay retract that misses → no matching base row, so it's a no-op;
/// - a value change (retract old + assert new, distinct fact keys) → old base
///   row skipped, new value emitted.
///
/// `resolve_overlay_ops` (inside [`collect_resolved_overlay_ops`]) guarantees at
/// most one op per fact key, so asserts and retracts are disjoint fact sets.
#[allow(clippy::too_many_arguments)]
fn collect_post_desc_topk_overlay(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    anchor_sid: &Sid,
    class: Option<(u32, u64)>,
    distinct: bool,
    dedup_includes_subject: bool,
    need: usize,
) -> Result<Option<Vec<TopKRow>>> {
    // Resolved novelty ops for the anchor predicate (POST order). `Ok(None)`
    // means a flake failed to translate ⇒ defer to the generic pipeline.
    let Some(ops) = collect_resolved_overlay_ops(ctx, store, g_id, RunSortOrder::Post, anchor_sid)?
    else {
        return Ok(None);
    };

    // Single order-preserving o_type across base leaflets AND overlay ops.
    let Some(o_type) = combined_orderable_o_type(store, g_id, p_id, &ops)? else {
        return Ok(None);
    };

    // Split ops into descending assert candidates and a retract fact-set, keyed
    // by the full V3 fact identity `(o_key, s_id, o_i)` (`o_i` distinguishes
    // repeated/list values of the same subject+object — collapsing on `(s,o_key)`
    // alone would drop live rows or let one `o_i`'s retract suppress all of them).
    // (All ops share `o_type` by the proof above.)
    let mut asserts: Vec<OvRow> = Vec::new();
    let mut retracts: FxHashSet<OvRow> = FxHashSet::default();
    for op in &ops {
        let row = OvRow {
            o_key: op.o_key,
            s_id: op.s_id,
            o_i: op.o_i,
        };
        if op.op {
            asserts.push(row);
        } else {
            retracts.insert(row);
        }
    }
    // Descending by the physical POST tie-order within an o_type: (o_key, o_i, s_id).
    asserts.sort_unstable_by_key(|r| std::cmp::Reverse(r.cmp_desc_key()));

    // Overlay-correct class membership context (built once).
    let class_filter = match class {
        Some((rdf_type_p_id, class_s_id)) => {
            let Some(f) = build_overlay_class_filter(ctx, store, g_id, rdf_type_p_id, class_s_id)?
            else {
                return Ok(None);
            };
            Some(f)
        }
        None => None,
    };

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let projection = projection_sid_okey_oi();

    let mut results: Vec<TopKRow> = Vec::with_capacity(need);
    let mut emitted: FxHashSet<OvRow> = FxHashSet::default(); // full-fact dedup
    let mut distinct_seen: FxHashSet<(u64, u64)> = FxHashSet::default(); // (s_or0, o_key)
    let mut assert_idx = 0usize;
    let mut scanned = 0usize;

    // Walk base leaves tail-first; merge each leaf's value-window of asserts.
    // `upper` is the exclusive high bound (the previous, higher leaf's low value).
    let mut upper: Option<u64> = None;
    for leaf in leaves.iter().rev() {
        let leaf_lo = leaf.first_key.o_key;
        let handle = store
            .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        let mut leaf_base: Vec<OvRow> = Vec::new(); // DESC by (o_key, o_i, s_id)
        for (leaflet_idx, entry) in dir.entries.iter().enumerate().rev() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(o_type) {
                return Ok(None); // defensive: stale single-type proof
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            scanned += batch.row_count;
            for row in (0..batch.row_count).rev() {
                leaf_base.push(OvRow {
                    o_key: batch.o_key.get(row),
                    s_id: batch.s_id.get(row),
                    // `u32::MAX` is the V3 "no list index" sentinel (matches the
                    // cursor's `merge_overlay_into_batch` and overlay-op translation),
                    // so identity dedup/retract align even if `OI` weren't loaded.
                    o_i: batch.o_i.get_or(row, u32::MAX),
                });
            }
        }

        // Asserts in this leaf's value window: o_key in [leaf_lo, upper). Because
        // we descend leaf-by-leaf and asserts are sorted DESC, the next
        // `>= leaf_lo` slice is exactly this window (anything `>= upper` was
        // consumed by higher leaves). The first leaf has `upper = None` (+inf),
        // capturing asserts above the base maximum (the newest novelty).
        let window_end = assert_idx
            + asserts[assert_idx..]
                .iter()
                .take_while(|r| r.o_key >= leaf_lo)
                .count();
        let window_asserts = &asserts[assert_idx..window_end];
        assert_idx = window_end;
        debug_assert!(upper.is_none_or(|u| window_asserts.iter().all(|r| r.o_key < u)));

        let merged = merge_desc(window_asserts, &leaf_base);
        let members = batch_members(class_filter.as_ref(), store, g_id, ctx.to_t, &merged)?;
        if emit_merged(
            &merged,
            o_type,
            &retracts,
            members.as_ref(),
            distinct,
            dedup_includes_subject,
            need,
            &mut results,
            &mut emitted,
            &mut distinct_seen,
        )? {
            return Ok(Some(results));
        }

        if scanned > SCAN_BUDGET && results.len() < need {
            return Ok(None);
        }
        upper = Some(leaf_lo);
    }

    // Asserts below every base row (and all asserts when the base is empty).
    let tail_asserts: Vec<OvRow> = asserts[assert_idx..].to_vec();
    let merged = merge_desc(&tail_asserts, &[]);
    let members = batch_members(class_filter.as_ref(), store, g_id, ctx.to_t, &merged)?;
    let _ = emit_merged(
        &merged,
        o_type,
        &retracts,
        members.as_ref(),
        distinct,
        dedup_includes_subject,
        need,
        &mut results,
        &mut emitted,
        &mut distinct_seen,
    )?;

    Ok(Some(results))
}

/// Merge a descending slice of overlay asserts with a descending slice of base
/// rows into one descending sequence tagged `is_base`. Both inputs are ordered
/// descending by `(o_key, o_i, s_id)`.
fn merge_desc(asserts: &[OvRow], base: &[OvRow]) -> Vec<(OvRow, bool)> {
    let mut out = Vec::with_capacity(asserts.len() + base.len());
    let (mut ai, mut bi) = (0usize, 0usize);
    while ai < asserts.len() && bi < base.len() {
        // Base sorts first when strictly greater in `(o_key, o_i, s_id)` descending.
        if base[bi].cmp_desc_key() > asserts[ai].cmp_desc_key() {
            out.push((base[bi], true));
            bi += 1;
        } else {
            out.push((asserts[ai], false));
            ai += 1;
        }
    }
    out.extend(base[bi..].iter().map(|r| (*r, true)));
    out.extend(asserts[ai..].iter().map(|r| (*r, false)));
    out
}

/// Resolve overlay-correct class membership for a merged batch's subjects, or
/// `None` when there is no class constraint.
fn batch_members(
    class_filter: Option<&OverlayClassFilter>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    to_t: i64,
    merged: &[(OvRow, bool)],
) -> Result<Option<FxHashSet<u64>>> {
    match class_filter {
        Some(f) => {
            let subjects: Vec<u64> = merged.iter().map(|(r, _)| r.s_id).collect();
            Ok(Some(f.members(store, g_id, to_t, &subjects)?))
        }
        None => Ok(None),
    }
}

/// Apply retract-suppression, fact dedup, class membership, and DISTINCT to a
/// descending merged batch, pushing survivors into `results`. Returns `true`
/// once `need` rows are reached.
///
/// `members` is the precomputed class-membership set for this batch's subjects
/// ([`batch_members`]), or `None` when unconstrained — kept out of this function
/// so the identity/retract/dedup logic is store-free and unit-testable.
#[allow(clippy::too_many_arguments)]
fn emit_merged(
    merged: &[(OvRow, bool)],
    o_type: u16,
    retracts: &FxHashSet<OvRow>,
    members: Option<&FxHashSet<u64>>,
    distinct: bool,
    dedup_includes_subject: bool,
    need: usize,
    results: &mut Vec<TopKRow>,
    emitted: &mut FxHashSet<OvRow>,
    distinct_seen: &mut FxHashSet<(u64, u64)>,
) -> Result<bool> {
    for (row, is_base) in merged.iter().copied() {
        // Skip base rows retracted by overlay (asserts are never in `retracts`,
        // as resolved ops are disjoint by fact key). Keyed by full fact identity.
        if is_base && retracts.contains(&row) {
            continue;
        }
        // Dedup by full fact identity: an overlay re-assertion of an existing base
        // row must not double it; distinct list entries (same `(s,o)`, different
        // `o_i`) are NOT collapsed here.
        if !emitted.insert(row) {
            continue;
        }
        if let Some(members) = members {
            if !members.contains(&row.s_id) {
                continue;
            }
        }
        if distinct {
            // DISTINCT collapses by the *projected* value `(s?, o)`, independent
            // of `o_i` — repeated list values are one distinct solution.
            let s_key = if dedup_includes_subject { row.s_id } else { 0 };
            if !distinct_seen.insert((s_key, row.o_key)) {
                continue;
            }
        }
        results.push((o_type, row.o_key, row.s_id));
        if results.len() >= need {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Overlay-correct `rdf:type = <Class>` membership: persisted base membership
/// (via `batched_lookup_predicate_refs`) adjusted by the novelty's `rdf:type`
/// asserts/retracts for this class.
struct OverlayClassFilter {
    rdf_type_p_id: u32,
    class_s_id: u64,
    /// Subjects that gained `rdf:type = class` in novelty.
    asserts: FxHashSet<u64>,
    /// Subjects that lost `rdf:type = class` in novelty.
    retracts: FxHashSet<u64>,
}

impl OverlayClassFilter {
    /// Members among `subjects`: `(base ∧ ¬overlay-retract) ∨ overlay-assert`.
    fn members(
        &self,
        store: &Arc<BinaryIndexStore>,
        g_id: GraphId,
        to_t: i64,
        subjects: &[u64],
    ) -> Result<FxHashSet<u64>> {
        let mut uniq: Vec<u64> = subjects.to_vec();
        uniq.sort_unstable();
        uniq.dedup();
        let type_map = batched_lookup_predicate_refs(store, g_id, self.rdf_type_p_id, &uniq, to_t)
            .map_err(|e| QueryError::Internal(format!("batched_lookup_predicate_refs: {e}")))?;
        let mut out: FxHashSet<u64> = FxHashSet::default();
        for s in uniq {
            let base_member = type_map
                .get(&s)
                .is_some_and(|classes| classes.binary_search(&self.class_s_id).is_ok());
            if (base_member && !self.retracts.contains(&s)) || self.asserts.contains(&s) {
                out.insert(s);
            }
        }
        Ok(out)
    }
}

/// Build the overlay class filter from the novelty's `rdf:type` ops.
/// `Ok(None)` on a translation failure ⇒ caller defers to the generic pipeline.
fn build_overlay_class_filter(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    rdf_type_p_id: u32,
    class_s_id: u64,
) -> Result<Option<OverlayClassFilter>> {
    let rdf_type_sid = store.encode_iri(fluree_vocab::rdf::TYPE);
    let Some(ops) =
        collect_resolved_overlay_ops(ctx, store, g_id, RunSortOrder::Psot, &rdf_type_sid)?
    else {
        return Ok(None);
    };
    let iri_ref = OType::IRI_REF.as_u16();
    let mut asserts: FxHashSet<u64> = FxHashSet::default();
    let mut retracts: FxHashSet<u64> = FxHashSet::default();
    for op in &ops {
        if op.o_type == iri_ref && op.o_key == class_s_id {
            if op.op {
                asserts.insert(op.s_id);
            } else {
                retracts.insert(op.s_id);
            }
        }
    }
    Ok(Some(OverlayClassFilter {
        rdf_type_p_id,
        class_s_id,
        asserts,
        retracts,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u16 = 0x0010; // any o_type; emit_merged just passes it through

    fn ov(o_key: u64, s_id: u64, o_i: u32) -> OvRow {
        OvRow { o_key, s_id, o_i }
    }

    /// Drive `emit_merged` with no class filter and collect the result rows.
    fn emit(
        merged: &[(OvRow, bool)],
        retracts: &[OvRow],
        distinct: bool,
        dedup_subject: bool,
        need: usize,
    ) -> Vec<TopKRow> {
        let retr: FxHashSet<OvRow> = retracts.iter().copied().collect();
        let mut results = Vec::new();
        let mut emitted = FxHashSet::default();
        let mut distinct_seen = FxHashSet::default();
        emit_merged(
            merged,
            T,
            &retr,
            None,
            distinct,
            dedup_subject,
            need,
            &mut results,
            &mut emitted,
            &mut distinct_seen,
        )
        .unwrap();
        results
    }

    // Repeated/list values share (s_id, o_key) but differ in o_i. Non-DISTINCT
    // must keep each as its own row (collapsing on (s,o_key) would drop one).
    #[test]
    fn list_values_with_distinct_o_i_are_not_collapsed() {
        let merged = vec![
            (ov(5, 1, 0), true),
            (ov(5, 1, 1), true),
            (ov(3, 1, 2), true),
        ];
        assert_eq!(
            emit(&merged, &[], false, true, 10),
            vec![(T, 5, 1), (T, 5, 1), (T, 3, 1)]
        );
    }

    // A retract for one o_i must suppress only that fact, not every base row that
    // happens to share (s_id, o_key).
    #[test]
    fn retract_of_one_o_i_suppresses_only_that_fact() {
        let merged = vec![(ov(5, 1, 0), true), (ov(5, 1, 1), true)];
        // Retract only o_i=0; the o_i=1 fact survives ⇒ exactly one row.
        assert_eq!(
            emit(&merged, &[ov(5, 1, 0)], false, true, 10),
            vec![(T, 5, 1)]
        );
    }

    // An overlay re-assertion of an existing base fact (identical identity) is
    // deduped to a single row.
    #[test]
    fn reassertion_of_same_fact_is_deduped() {
        let merged = vec![(ov(5, 1, 0), true), (ov(5, 1, 0), false)];
        assert_eq!(emit(&merged, &[], false, true, 10), vec![(T, 5, 1)]);
    }

    // DISTINCT collapses by the projected value (s?, o) regardless of o_i, so two
    // list entries of the same value are one solution.
    #[test]
    fn distinct_collapses_repeated_values_ignoring_o_i() {
        let merged = vec![(ov(5, 1, 0), true), (ov(5, 1, 1), true)];
        assert_eq!(emit(&merged, &[], true, true, 10), vec![(T, 5, 1)]);
    }

    // merge_desc yields a descending (o_key, o_i, s_id) sequence.
    #[test]
    fn merge_desc_is_descending_by_full_key() {
        // Both inputs must already be descending by (o_key, o_i, s_id), as the
        // reverse leaf-walk and the assert sort produce.
        let asserts = vec![ov(5, 2, 0), ov(4, 1, 0)];
        let base = vec![ov(5, 1, 1), ov(5, 1, 0), ov(3, 9, 0)];
        let merged = merge_desc(&asserts, &base);
        let keys: Vec<(u64, u32, u64)> = merged.iter().map(|(r, _)| r.cmp_desc_key()).collect();
        for w in keys.windows(2) {
            assert!(w[0] >= w[1], "merge not descending: {keys:?}");
        }
    }
}
