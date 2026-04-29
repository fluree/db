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
//! Non-history queries delegate to `BinaryScanOperator::open` unchanged.
//! History queries always go through `collect_history_flakes`, even when
//! no binary store is attached: in the no-store case the persisted pass
//! is skipped (`index_t = -1`) and the novelty walk is the whole event
//! stream. This is deliberate — the core `range_with_overlay` genesis
//! path calls `remove_stale_flakes` which drops retracts, and that's
//! wrong for history mode.
//!
//! ## Cost model
//!
//! All heavy lifting happens in `open()` (three-source collection into
//! a `Vec<Flake>`), so that's where the fuel guardrail lives.
//!
//! - **Persisted pass:** `1000 + sidecar_rows + base_rows` micro-fuel per
//!   leaflet, charged from the directory entry before any sidecar segment
//!   or column load — overrun short-circuits before I/O. The 1000 base
//!   matches `BinaryCursor::next_batch` for non-history scans; the
//!   row-count terms account for sidecar replay + base-row decode work
//!   that the non-history path doesn't perform. Overcharges on rows
//!   rejected by `filter.matches()`; that's the guardrail tradeoff.
//! - **Novelty pass:** 1 micro-fuel per matched novelty flake, charged
//!   during the walk. A captured `FuelExceededError` short-circuits the
//!   walk on the next callback invocation.
//! - **Per-flake emit:** 1 micro-fuel per flake is charged downstream in
//!   `flakes_to_bindings` when `next_batch` drains the collected vec.
//!
//! Together these keep a broad unindexed or index-lagged history query
//! from doing unbounded eager work before the caller sees the first
//! batch.
//!
//! ## Known follow-ups
//!
//! - **Per-leaf sidecar pruning.** `open_leaf_handle(..., need_replay=true)`
//!   fetches the whole sidecar blob up front. For leaves whose directory
//!   doesn't yet reveal any leaflet with `history_max_t >= from_t`, this
//!   is wasted I/O on local/cached reads. Fixing this cleanly needs
//!   either a two-pass open (dir first, sidecar on demand) or leaf-level
//!   `history_max_t` on `LeafEntry` so we can prune without opening.
//!   Tracked for a follow-up.
//! - **Streaming emit.** `collect_history_flakes` materialises the full
//!   matched event set before `next_batch` drains it. Bound-subject /
//!   bound-predicate queries match a tiny set so this is cheap. Broad
//!   queries (`?s ?p ?o` across a wide range) can benefit from a
//!   leaflet-at-a-time streaming cursor — also tracked as a follow-up.

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
/// When history mode is not active, this wrapper transparently delegates
/// to `BinaryScanOperator::open` — the regular scan path is unchanged.
/// When history mode is active, the operator runs its own three-source
/// merge (sidecar + base + novelty) regardless of whether a binary store
/// is attached: an unindexed ledger just skips the persisted pass and
/// takes the full event stream from novelty.
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
        let snapshot = ctx.active_snapshot;
        let no_overlay = NoOverlay;
        let overlay: &dyn OverlayProvider = ctx.overlay.unwrap_or(&no_overlay);
        let g_id: GraphId = ctx.binary_g_id;

        // Index / novelty boundary. When no binary store is attached
        // (e.g. a ledger that's never been indexed), treat everything as
        // novelty: `index_t = -1` makes the persisted pass a no-op and
        // the novelty merge picks up all in-range overlay events.
        let (store_opt, index_t) = match ctx.binary_store.clone() {
            Some(store) => {
                let t = store.max_t();
                (Some(store), t)
            }
            None => (None, -1_i64),
        };
        let from_t = ctx.from_t.unwrap_or(0);
        let to_t = ctx.to_t;
        let persisted_to_t = to_t.min(index_t);

        // Encode bound pattern components through the snapshot to match
        // the persisted ID space. (`build_filter_from_snapshot_sids`
        // needs a store for s_id/p_id resolution in the persisted pass;
        // novelty-only doesn't need the filter since we apply
        // `flake_matches_range_eq` on decoded flakes.)
        let (s_sid, p_sid, o_val_opt) =
            BinaryScanOperator::extract_bound_terms_snapshot(snapshot, &self.pattern);

        let mut flakes: Vec<Flake> = Vec::new();

        // ---- Persisted sources (sidecar + base rows in range) ----
        if let Some(store) = store_opt.as_ref() {
            if from_t <= persisted_to_t {
                let filter = BinaryScanOperator::build_filter_from_snapshot_sids(
                    snapshot,
                    &self.pattern,
                    store.as_ref(),
                    &s_sid,
                    &p_sid,
                )
                .map_err(|e| QueryError::Internal(format!("history scan: build_filter: {e}")))?;
                let order = self.pick_order(&filter);
                if let Some(branch) = store.branch_for_order(g_id, order) {
                    let leaf_indices = leaf_index_range(branch, &filter, order);
                    let view = BinaryGraphView::with_novelty(
                        Arc::clone(store),
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

                            // Scale per-leaflet fuel by rows we'll actually iterate.
                            // Both counts come from the directory entry — no I/O
                            // needed to charge — so we short-circuit before the
                            // sidecar segment / column load on overrun. Overcharges
                            // on filter-rejected rows; that's the guardrail tradeoff.
                            let sidecar_rows =
                                if sidecar_in_range { leaflet.history_len as u64 } else { 0 };
                            let base_rows = leaflet.row_count as u64;
                            ctx.tracker.consume_fuel(
                                1000u64.saturating_add(sidecar_rows).saturating_add(base_rows),
                            )?;

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
                                    .map_err(|e| {
                                        QueryError::from_io("history scan load_columns", e)
                                    })?;
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
                } // end `if let Some(branch)`
            } // end `if from_t <= persisted_to_t`
        } // end `if let Some(store)`

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

            // Cost model: charge 1 fuel per matched novelty flake, **during**
            // collection. Without this, an unindexed ledger with large
            // novelty or a broad history query could do unbounded eager
            // work in `open()` before the caller sees the first batch.
            // We apply the full pattern filter (s/p/o + object bounds)
            // inside the walk so we only charge for flakes we actually
            // retain — otherwise a wide predicate like `?s ?p ?o` on a
            // crowded novelty slice would charge for every flake touched
            // even when our pattern only matches a sliver. `for_each_overlay_flake`'s
            // callback cannot return a Result, so we capture any
            // `FuelExceededError` and surface it after the walk.
            let object_bounds = self.object_bounds.as_ref();
            let mut novelty: Vec<Flake> = Vec::new();
            let mut fuel_err: Option<fluree_db_core::FuelExceededError> = None;
            overlay.for_each_overlay_flake(
                g_id,
                self.inner_index(),
                None,
                None,
                true,
                to_t,
                &mut |f| {
                    if fuel_err.is_some() {
                        return;
                    }
                    if f.t <= index_t || f.t > to_t || f.t < from_t {
                        return;
                    }
                    if !flake_matches_range_eq(f, &match_val) {
                        return;
                    }
                    if let Some(bounds) = object_bounds {
                        if !bounds.matches(&f.o) {
                            return;
                        }
                    }
                    if let Err(e) = ctx.tracker.consume_fuel(1) {
                        fuel_err = Some(e);
                        return;
                    }
                    novelty.push(f.clone());
                },
            );
            if let Some(e) = fuel_err {
                return Err(e.into());
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
        if !ctx.history_mode {
            // Non-history queries go through the unchanged scan path.
            return self.inner.open(ctx).await;
        }
        // History mode: we always collect flakes ourselves (with explicit op
        // preservation) rather than going through `BinaryScanOperator::open`.
        // The non-history path in the core `range_with_overlay` genesis
        // fallback calls `remove_stale_flakes`, which drops retracts — fine
        // for current-state queries but wrong for history. Running our own
        // collector handles both the indexed and novelty-only cases
        // correctly. Policy enforcement is applied in
        // `collect_history_flakes` via `filter_flakes_by_policy`.
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
