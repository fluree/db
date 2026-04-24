//! Indexed history-range scan operator.
//!
//! Wraps `BinaryScanOperator` and, when `ctx.history_mode` is set and a
//! binary store is attached, replaces its cursor walk with a
//! three-source merge:
//!
//! 1. **History sidecar entries** (`HistEntryV2`) for each matching
//!    leaflet — assert + retract events with explicit op.
//! 2. **Base leaflet rows** whose `t` falls in `[from_t, index_t]` —
//!    emitted as `op = assert`.
//! 3. **Overlay / novelty flakes** in `(index_t, to_t]` — carry their
//!    own `flake.op`.
//!
//! The merged flake list is handed to the existing
//! `flakes_to_bindings` pipeline via `prime_history_flakes`, which is
//! already history-aware (line ~704 in `binary_scan.rs` copies
//! `flake.op` onto the emitted `Binding::Lit`).
//!
//! ## Narrowing
//!
//! Leaves and leaflets are narrowed by the pattern's bound components:
//! - subject bound → `find_leaves_for_subject(s_id)` on a SPOT branch
//! - predicate-only bound → PSOT branch (currently still walks all
//!   leaves; predicate-constant leaflet skip applies)
//! - leaflet-level skip via `p_const`, `o_type_const`, and
//!   `history_max_t < from_t`
//!
//! Non-history queries and queries with no binary store fall through
//! to `BinaryScanOperator::open` unchanged.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::{BinaryGraphView, BinaryIndexStore, ColumnProjection, RunSortOrder};
use fluree_db_core::{
    flake_matches_range_eq, Flake, FlakeMeta, FlakeValue, GraphId, IndexType, NoOverlay,
    ObjectBounds, OverlayProvider, RangeMatch, Sid,
};

use crate::binary_scan::{BinaryScanOperator, EmitMask};
use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::inline::InlineOperator;
use crate::operator::Operator;
use crate::triple::TriplePattern;
use crate::var_registry::VarId;

/// Scan operator that activates a dedicated history-range walk when
/// `ctx.history_mode` is true.
///
/// When history mode is not active (or no binary store is available),
/// this wrapper transparently delegates to `BinaryScanOperator` — the
/// regular scan path is unchanged.
pub struct BinaryHistoryScanOperator {
    inner: BinaryScanOperator,
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    index_hint: Option<IndexType>,
}

impl BinaryHistoryScanOperator {
    /// Construct with default emit mask and no index hint.
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
    ) -> Self {
        Self::new_with_emit_and_index(pattern, object_bounds, inline_ops, EmitMask::ALL, None)
    }

    /// Construct with an explicit emit mask and index hint.
    pub fn new_with_emit_and_index(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
    ) -> Self {
        let inner = BinaryScanOperator::new_with_emit_and_index(
            pattern.clone(),
            object_bounds.clone(),
            inline_ops,
            emit,
            index_hint,
        );
        Self {
            inner,
            pattern,
            object_bounds,
            index_hint,
        }
    }

    /// Pick an index order that minimises leaf touches for the bound components.
    ///
    /// `index_hint` (set by the planner) wins if present. Otherwise:
    /// - subject bound → SPOT
    /// - predicate bound (no subject) → PSOT
    /// - object bound (no subject, no predicate) → OPST
    /// - none bound → SPOT (default)
    fn pick_order(&self, filter: &fluree_db_binary_index::BinaryFilter) -> RunSortOrder {
        if let Some(hint) = self.index_hint {
            return crate::binary_scan::index_type_to_sort_order(hint);
        }
        if filter.s_id.is_some() {
            RunSortOrder::Spot
        } else if filter.p_id.is_some() {
            RunSortOrder::Psot
        } else if filter.o_type.is_some() || filter.o_key.is_some() {
            RunSortOrder::Opst
        } else {
            RunSortOrder::Spot
        }
    }

    /// Collect the full history-range event stream for this pattern.
    ///
    /// Walks matching leaves + leaflets, loads sidecar segments and
    /// in-range base rows, merges with overlay/novelty events, and
    /// returns flakes with explicit `op`.
    async fn collect_history_flakes(&self, ctx: &ExecutionContext<'_>) -> Result<Vec<Flake>> {
        let store = ctx
            .binary_store
            .clone()
            .ok_or_else(|| QueryError::Internal("history scan: no binary store".into()))?;
        let snapshot = ctx.active_snapshot;
        let no_overlay = NoOverlay;
        let overlay: &dyn OverlayProvider = ctx.overlay.unwrap_or(&no_overlay);
        let g_id: GraphId = ctx.binary_g_id;

        let index_t = store.max_t();
        let from_t = ctx.from_t.unwrap_or(0);
        let to_t = ctx.to_t;
        // Persisted-side upper bound is clamped to index_t; anything above
        // lives in novelty and is merged below.
        let persisted_to_t = to_t.min(index_t);

        // Encode bound pattern components through the snapshot to match
        // the persisted ID space.
        let (s_sid, p_sid, o_val_opt) =
            BinaryScanOperator::extract_bound_terms_snapshot(snapshot, &self.pattern);
        let filter = BinaryScanOperator::build_filter_from_snapshot_sids(
            snapshot,
            &self.pattern,
            store.as_ref(),
            &s_sid,
            &p_sid,
        )
        .map_err(|e| QueryError::Internal(format!("history scan: build_filter: {e}")))?;

        let order = self.pick_order(&filter);

        let mut flakes: Vec<Flake> = Vec::new();

        // ---- Persisted sources (sidecar + base rows in range) ----
        if from_t <= persisted_to_t {
            if let Some(branch) = store.branch_for_order(g_id, order) {
                let leaf_indices = leaf_index_range(branch, &filter, order);
                let view = BinaryGraphView::with_novelty(
                    Arc::clone(&store),
                    g_id,
                    ctx.dict_novelty.clone(),
                );

                let from_t_u32 = clamp_t_u32(from_t);
                let persisted_to_u32 = clamp_t_u32(persisted_to_t);

                for leaf_idx in leaf_indices {
                    let entry = &branch.leaves[leaf_idx];
                    let handle = store
                        .open_leaf_handle(&entry.leaf_cid, entry.sidecar_cid.as_ref(), true)
                        .map_err(|e| QueryError::from_io("history scan open_leaf_handle", e))?;
                    let dir = handle.dir();

                    for (leaflet_idx, leaflet) in dir.entries.iter().enumerate() {
                        if !leaflet_matches_filter(leaflet, &filter) {
                            continue;
                        }

                        // Sidecar: only load when the segment's t range can
                        // overlap [from_t, persisted_to_t].
                        let sidecar_in_range = leaflet.history_len > 0
                            && leaflet.history_max_t >= from_t_u32
                            && leaflet.history_min_t <= persisted_to_u32;
                        if sidecar_in_range {
                            let segment =
                                handle.load_sidecar_segment(leaflet_idx).map_err(|e| {
                                    QueryError::from_io("history scan load_sidecar_segment", e)
                                })?;
                            for entry_v2 in &segment {
                                let s_id = entry_v2.s_id.as_u64();
                                if !filter.matches(
                                    s_id,
                                    entry_v2.p_id,
                                    entry_v2.o_type,
                                    entry_v2.o_key,
                                    entry_v2.o_i,
                                ) {
                                    continue;
                                }
                                let t = entry_v2.t as i64;
                                if t < from_t || t > persisted_to_t {
                                    continue;
                                }
                                let op = entry_v2.op == 1;
                                if let Some(flake) = decode_event_to_flake(
                                    &view,
                                    store.as_ref(),
                                    s_id,
                                    entry_v2.p_id,
                                    entry_v2.o_type,
                                    entry_v2.o_key,
                                    entry_v2.o_i,
                                    t,
                                    op,
                                )? {
                                    flakes.push(flake);
                                }
                            }
                        }

                        // Base rows: emit rows whose t falls in range as asserts.
                        if leaflet.row_count > 0 {
                            let projection = ColumnProjection::all();
                            let batch = handle
                                .load_columns(leaflet_idx, &projection, order)
                                .map_err(|e| QueryError::from_io("history scan load_columns", e))?;
                            for i in 0..batch.row_count {
                                let s_id = batch.s_id.get(i);
                                let p_id = batch.p_id.get_or(i, 0);
                                let o_type = batch.o_type.get_or(i, 0);
                                let o_key = batch.o_key.get(i);
                                let o_i = batch.o_i.get_or(i, u32::MAX);
                                let t_u32 = batch.t.get_or(i, 0);
                                let t = t_u32 as i64;

                                if !filter.matches(s_id, p_id, o_type, o_key, o_i) {
                                    continue;
                                }
                                if t < from_t || t > persisted_to_t {
                                    continue;
                                }
                                if let Some(flake) = decode_event_to_flake(
                                    &view,
                                    store.as_ref(),
                                    s_id,
                                    p_id,
                                    o_type,
                                    o_key,
                                    o_i,
                                    t,
                                    true,
                                )? {
                                    flakes.push(flake);
                                }
                            }
                        }
                    }
                }
            }
        }

        // ---- Novelty events in (index_t, to_t] ----
        if to_t > index_t {
            // Build a RangeMatch for pattern-level filtering.
            let match_val = RangeMatch {
                s: s_sid.clone(),
                p: p_sid.clone(),
                o: o_val_opt.clone(),
                dt: self.pattern.dtc.as_ref().map(|d| d.datatype().clone()),
                t: None,
            };

            let mut novelty: Vec<Flake> = Vec::new();
            overlay.for_each_overlay_flake(
                g_id,
                self.inner_index(),
                None,
                None,
                true,
                to_t,
                &mut |f| {
                    if f.t > index_t && f.t <= to_t && f.t >= from_t {
                        novelty.push(f.clone());
                    }
                },
            );
            novelty.retain(|f| flake_matches_range_eq(f, &match_val));
            if let Some(bounds) = &self.object_bounds {
                novelty.retain(|f| bounds.matches(&f.o));
            }
            flakes.extend(novelty);
        }

        // ---- Policy enforcement ----
        let flakes =
            BinaryScanOperator::filter_flakes_by_policy(ctx, snapshot, overlay, to_t, g_id, flakes)
                .await?;

        Ok(flakes)
    }

    /// Wrap `self.pattern` to derive the same `IndexType` the inner
    /// scan would have picked. Kept private to keep the ctor uniform.
    fn inner_index(&self) -> IndexType {
        let s_bound = self.pattern.s_bound();
        let p_bound = self.pattern.p_bound();
        let o_bound = self.pattern.o_bound();
        let o_is_ref = self.pattern.o_is_ref();
        self.index_hint
            .unwrap_or_else(|| IndexType::for_query(s_bound, p_bound, o_bound, o_is_ref))
    }
}

#[async_trait]
impl Operator for BinaryHistoryScanOperator {
    fn schema(&self) -> &[VarId] {
        self.inner.schema()
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !ctx.history_mode || ctx.binary_store.is_none() {
            // Non-history queries, or queries with no binary index store,
            // go through the unchanged scan path.
            return self.inner.open(ctx).await;
        }
        // Policy enforcement via per-flake async checks is honored below
        // in `collect_history_flakes` (via `filter_flakes_by_policy`).
        let flakes = self.collect_history_flakes(ctx).await?;
        self.inner.prime_history_flakes(ctx, flakes)
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        self.inner.next_batch(ctx).await
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Range of leaf indices whose key range can contain a matching row.
///
/// - Subject bound (SPOT order) → narrow by `s_id`.
/// - Otherwise → all leaves.
fn leaf_index_range(
    branch: &fluree_db_binary_index::format::branch::BranchManifest,
    filter: &fluree_db_binary_index::BinaryFilter,
    order: RunSortOrder,
) -> Range<usize> {
    if let (Some(s_id), RunSortOrder::Spot) = (filter.s_id, order) {
        branch.find_leaves_for_subject(s_id)
    } else {
        0..branch.leaves.len()
    }
}

/// Quick leaflet-level filter skip using directory constants.
fn leaflet_matches_filter(
    leaflet: &fluree_db_binary_index::format::leaf::LeafletDirEntryV3,
    filter: &fluree_db_binary_index::BinaryFilter,
) -> bool {
    if let (Some(filter_p), Some(const_p)) = (filter.p_id, leaflet.p_const) {
        if filter_p != const_p {
            return false;
        }
    }
    if let (Some(filter_ot), Some(const_ot)) = (filter.o_type, leaflet.o_type_const) {
        if filter_ot != const_ot {
            return false;
        }
    }
    true
}

/// Clamp an i64 `t` to the u32 range used by the sidecar/base format.
#[inline]
fn clamp_t_u32(t: i64) -> u32 {
    if t < 0 {
        0
    } else if t > u32::MAX as i64 {
        u32::MAX
    } else {
        t as u32
    }
}

/// Decode a single history event (from sidecar or base column) into
/// a `Flake` with explicit `op`.
///
/// Returns `Ok(None)` when subject resolution fails (caller should
/// skip — shouldn't happen for well-formed indices).
#[allow(clippy::too_many_arguments)]
fn decode_event_to_flake(
    view: &BinaryGraphView,
    store: &BinaryIndexStore,
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
    t: i64,
    op: bool,
) -> Result<Option<Flake>> {
    let s_sid = view
        .resolve_subject_sid(s_id)
        .map_err(|e| QueryError::from_io("history decode resolve_subject_sid", e))?;
    let p_iri = match store.resolve_predicate_iri(p_id) {
        Some(iri) => iri,
        None => return Ok(None),
    };
    let p_sid = store.encode_iri(p_iri);
    let o_val: FlakeValue = view
        .decode_value(o_type, o_key, p_id)
        .map_err(|e| QueryError::from_io("history decode decode_value", e))?;
    let dt = store
        .resolve_datatype_sid(o_type)
        .unwrap_or_else(|| Sid::new(0, ""));
    let lang = store
        .resolve_lang_tag(o_type)
        .map(std::string::ToString::to_string);
    let meta = if lang.is_some() || o_i != u32::MAX {
        Some(FlakeMeta {
            lang,
            i: if o_i != u32::MAX {
                Some(o_i as i32)
            } else {
                None
            },
        })
    } else {
        None
    };
    Ok(Some(Flake {
        g: None,
        s: s_sid,
        p: p_sid,
        o: o_val,
        dt,
        t,
        op,
        m: meta,
    }))
}
