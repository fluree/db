//! Binary cursor: iterates FLI3 leaves and produces `ColumnBatch` output.
//!
//! Yields **one `ColumnBatch` per leaflet** (not per leaf). This avoids the
//! broken batch-concat problem entirely and provides leaflet-at-a-time iteration.
//!
//! Overlay merge: two-pointer merge of indexed leaflet rows with sorted
//! `OverlayOp` values. The four merge cases:
//! row-only, overlay-only, same-identity (replace/retract), different-identity.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use fluree_db_core::Tracker;

use crate::format::branch::BranchManifest;
use crate::format::run_record::RunSortOrder;
use crate::format::run_record_v2::cmp_v2_for_order;
use crate::format::run_record_v2::{read_ordered_key_v2, RunRecordV2};
use crate::read::types::cmp_overlay_vs_record;
use crate::read::types::{cmp_row_vs_overlay, OverlayOp};

use super::binary_index_store::BinaryIndexStore;
use super::column_loader::load_columns_cached_via_handle;
use super::column_types::{BinaryFilter, ColumnBatch, ColumnData, ColumnProjection};
use super::replay::replay_leaflet;

// ============================================================================
// BinaryCursor
// ============================================================================

/// V3 columnar cursor: iterates leaflets across leaves in a branch manifest.
///
/// Yields one `ColumnBatch` per leaflet per `next_batch()` call.
/// Leaf bytes are fetched and decoded on demand when advancing to a new leaf.
pub struct BinaryCursor {
    store: Arc<BinaryIndexStore>,
    order: RunSortOrder,
    branch: Arc<BranchManifest>,
    leaf_range: Range<usize>,
    current_leaf_idx: usize,
    filter: BinaryFilter,
    projection: ColumnProjection,
    /// Decoded state for the currently-open leaf.
    current_leaf: Option<OpenLeaf>,
    /// Index of the next leaflet within the current leaf.
    current_leaflet_idx: usize,
    exhausted: bool,
    /// Overlay ops sorted by this cursor's sort order.
    overlay_ops: Vec<OverlayOp>,
    /// Start position in overlay_ops for the current leaf (set per-leaf via slicing).
    overlay_pos: usize,
    /// Exclusive end position in overlay_ops for the current leaf.
    /// Ops beyond this belong to a later leaf and must not be consumed.
    leaf_overlay_end: usize,
    /// Overlay epoch for cache key differentiation.
    epoch: u64,
    /// Time bound for overlay ops (only emit ops with t <= to_t).
    to_t: i64,
    /// Optional fuel tracker. When set, charges 1 fuel per leaflet returned.
    tracker: Option<Tracker>,
}

/// State for a leaf that's been opened via `LeafHandle`.
struct OpenLeaf {
    handle: Box<dyn super::leaf_access::LeafHandle>,
}

impl BinaryCursor {
    /// Create a new cursor over a range of leaves in a branch manifest.
    pub fn new(
        store: Arc<BinaryIndexStore>,
        order: RunSortOrder,
        branch: Arc<BranchManifest>,
        min_key: &crate::format::run_record_v2::RunRecordV2,
        max_key: &crate::format::run_record_v2::RunRecordV2,
        filter: BinaryFilter,
        projection: ColumnProjection,
    ) -> Self {
        let cmp = cmp_v2_for_order(order);
        let leaf_range = branch.find_leaves_in_range(min_key, max_key, cmp);
        Self {
            store,
            order,
            branch,
            leaf_range: leaf_range.clone(),
            current_leaf_idx: leaf_range.start,
            filter,
            projection,
            current_leaf: None,
            current_leaflet_idx: 0,
            // Don't mark exhausted when leaf_range is empty — overlay-only path
            // may still have ops to emit.
            exhausted: false,
            overlay_ops: Vec::new(),
            overlay_pos: 0,
            leaf_overlay_end: 0,
            epoch: 0,
            to_t: i64::MAX,
            tracker: None,
        }
    }

    /// Create a cursor that scans ALL leaves in the branch.
    pub fn scan_all(
        store: Arc<BinaryIndexStore>,
        order: RunSortOrder,
        branch: Arc<BranchManifest>,
        filter: BinaryFilter,
        projection: ColumnProjection,
    ) -> Self {
        let leaf_count = branch.leaves.len();
        Self {
            store,
            order,
            branch,
            leaf_range: 0..leaf_count,
            current_leaf_idx: 0,
            filter,
            projection,
            current_leaf: None,
            current_leaflet_idx: 0,
            exhausted: false,
            overlay_ops: Vec::new(),
            overlay_pos: 0,
            leaf_overlay_end: 0,
            epoch: 0,
            to_t: i64::MAX,
            tracker: None,
        }
    }

    /// Attach a fuel tracker. Charges 1 fuel (1000 micro-fuel) per leaflet
    /// returned by `next_batch` (regardless of cache hit/miss).
    pub fn with_tracker(mut self, tracker: Tracker) -> Self {
        if tracker.is_enabled() {
            self.tracker = Some(tracker);
        }
        self
    }

    /// Set overlay ops.
    ///
    /// **Contract:** ops must be pre-sorted by this cursor's sort order AND
    /// assert/retract lifecycles must be resolved (at most one op per fact key).
    /// Use [`sort_overlay_ops`] then [`resolve_overlay_ops`] before calling.
    pub fn set_overlay_ops(&mut self, ops: Vec<OverlayOp>) {
        debug_assert!(
            ops.windows(2).all(|w| w[0].fact_key() != w[1].fact_key()),
            "overlay ops contain duplicate fact keys — caller must resolve \
             assert/retract lifecycles via resolve_overlay_ops() before set_overlay_ops()"
        );
        let len = ops.len();
        self.overlay_ops = ops;
        self.overlay_pos = 0;
        self.leaf_overlay_end = len; // default: all ops visible (refined per-leaf)
    }

    /// Set the overlay epoch for cache key differentiation.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Set the time bound for overlay ops.
    pub fn set_to_t(&mut self, to_t: i64) {
        self.to_t = to_t;
    }

    /// Whether overlay ops remain for the current leaf.
    fn has_overlay(&self) -> bool {
        self.overlay_pos < self.leaf_overlay_end
    }

    /// Whether time-travel replay is needed (to_t < index_t).
    fn need_replay(&self) -> bool {
        self.to_t < self.store.max_t()
    }

    /// Whether any overlay ops remain globally (for overlay-only path).
    fn has_any_overlay(&self) -> bool {
        self.overlay_pos < self.overlay_ops.len()
    }

    /// Slice overlay ops for the leaf at `leaf_idx` using branch manifest keys.
    /// Sets `overlay_pos` and `leaf_overlay_end` for this leaf.
    fn slice_overlay_for_leaf(&mut self, leaf_idx: usize) {
        let ops = &self.overlay_ops[self.overlay_pos..];
        if ops.is_empty() {
            self.leaf_overlay_end = self.overlay_pos;
            return;
        }

        let leaf_entry = &self.branch.leaves[leaf_idx];
        let next_first = if leaf_idx + 1 < self.branch.leaves.len() {
            Some(&self.branch.leaves[leaf_idx + 1].first_key)
        } else {
            None
        };

        let is_first = leaf_idx == self.leaf_range.start;
        let (start_offset, end_offset) =
            compute_overlay_window(ops, &leaf_entry.first_key, next_first, self.order, is_first);

        self.overlay_pos += start_offset;
        self.leaf_overlay_end = self.overlay_pos + (end_offset - start_offset);
    }

    /// Advance to the next non-empty leaflet and return its `ColumnBatch`.
    ///
    /// Returns `None` when all leaflets in all leaves are exhausted
    /// (and overlay-only ops have been emitted).
    pub fn next_batch(&mut self) -> io::Result<Option<ColumnBatch>> {
        loop {
            if self.exhausted {
                return Ok(None);
            }

            // If we have an open leaf, try the next leaflet in it.
            // We take the leaf temporarily to avoid borrow conflicts with &mut self.
            if let Some(leaf) = self.current_leaf.take() {
                while self.current_leaflet_idx < leaf.handle.dir().entries.len() {
                    let entry = &leaf.handle.dir().entries[self.current_leaflet_idx];
                    self.current_leaflet_idx += 1;

                    // Pre-skip by directory metadata (only when no overlay —
                    // overlay merge may add rows to otherwise-skippable leaflets).
                    let has_ov = self.has_overlay();
                    if !has_ov && self.filter.skip_leaflet(entry.p_const, entry.o_type_const) {
                        continue;
                    }
                    if entry.row_count == 0 && !has_ov {
                        continue;
                    }

                    // Load columns via LeafHandle (cached when LeafletCache is available).
                    let mut batch = if entry.row_count > 0 {
                        let leaflet_idx = self.current_leaflet_idx - 1;
                        if let Some(cache) = self.store.leaflet_cache() {
                            load_columns_cached_via_handle(
                                leaf.handle.as_ref(),
                                leaflet_idx,
                                self.order,
                                cache,
                                leaf.handle.leaf_id(),
                                u32::try_from(leaflet_idx).map_err(|_| {
                                    std::io::Error::other(format!(
                                        "leaflet index {leaflet_idx} exceeds u32::MAX"
                                    ))
                                })?,
                            )?
                        } else {
                            leaf.handle
                                .load_columns(leaflet_idx, &self.projection, self.order)?
                        }
                    } else {
                        ColumnBatch::empty()
                    };

                    // Time-travel replay: if to_t < index_t, reconstruct leaflet state
                    // at to_t using the history sidecar.
                    if self.need_replay() {
                        // Quick-skip: if this leaflet's history doesn't extend past to_t,
                        // and no base rows have t > to_t, replay is unnecessary.
                        let to_t_u32 = u32::try_from(self.to_t).unwrap_or(u32::MAX);
                        let needs_leaflet_replay = entry.history_max_t > to_t_u32
                            || batch_has_rows_above_t(&batch, to_t_u32);

                        if needs_leaflet_replay && entry.history_len > 0 {
                            let history = leaf
                                .handle
                                .load_sidecar_segment(self.current_leaflet_idx - 1)?;
                            if !history.is_empty() {
                                if let Some(replayed) =
                                    replay_leaflet(&batch, &history, self.to_t, self.order)
                                {
                                    batch = replayed;
                                }
                            }
                        } else if needs_leaflet_replay {
                            // No sidecar but base rows have t > to_t: filter them out.
                            if let Some(replayed) =
                                replay_leaflet(&batch, &[], self.to_t, self.order)
                            {
                                batch = replayed;
                            }
                        }
                    }

                    // Apply row-level filter.
                    let batch = if self.filter.is_empty() || batch.is_empty() {
                        batch
                    } else {
                        filter_batch(&self.filter, &batch)
                    };

                    // Apply overlay merge if we have overlay ops.
                    let batch = if has_ov {
                        // Drain overlay ops only up to this leaflet's key range.
                        // Draining past the leaflet boundary can emit an overlay assert
                        // before the base row appears in a later leaflet, yielding duplicates.
                        let leaflet_last_key: RunRecordV2 =
                            read_ordered_key_v2(self.order, &entry.last_key);
                        self.merge_overlay_into_batch(batch, &leaflet_last_key)
                    } else {
                        batch
                    };

                    if batch.is_empty() {
                        continue;
                    }

                    // Charge 1 fuel per leaflet returned (per-touch, regardless
                    // of cache state). Caller can downcast the io::Error to
                    // recover the original FuelExceededError.
                    if let Some(tracker) = &self.tracker {
                        if let Err(e) = tracker.consume_fuel(1000) {
                            return Err(io::Error::other(e));
                        }
                    }

                    // Put the leaf back before returning.
                    self.current_leaf = Some(leaf);
                    return Ok(Some(batch));
                }
                // Exhausted all leaflets in this leaf — drop it (already taken).
            }

            // Open the next leaf.
            if self.current_leaf_idx >= self.leaf_range.end {
                // All indexed leaves exhausted. Try overlay-only path.
                // Reset leaf_overlay_end to cover all remaining ops.
                self.leaf_overlay_end = self.overlay_ops.len();
                if self.has_any_overlay() {
                    let batch = self.emit_overlay_only();
                    self.exhausted = true;
                    if !batch.is_empty() {
                        return Ok(Some(batch));
                    }
                }
                self.exhausted = true;
                return Ok(None);
            }

            let leaf_idx = self.current_leaf_idx;
            let leaf_cid = self.branch.leaves[leaf_idx].leaf_cid.clone();
            let sidecar_cid = self.branch.leaves[leaf_idx].sidecar_cid.clone();
            self.current_leaf_idx += 1;

            // Slice overlay ops for this leaf (binary search on branch keys).
            if !self.overlay_ops.is_empty() {
                self.slice_overlay_for_leaf(leaf_idx);
            }

            // Open leaf via LeafHandle (auto-selects local vs range-read path).
            let handle =
                self.store
                    .open_leaf_handle(&leaf_cid, sidecar_cid.as_ref(), self.need_replay())?;
            self.current_leaf = Some(OpenLeaf { handle });
            self.current_leaflet_idx = 0;
        }
    }

    // ========================================================================
    // Overlay merge
    // ========================================================================

    /// Two-pointer merge of a base batch with overlay ops at the current position.
    ///
    /// Consumes overlay ops that fall within this batch's sort-order range.
    /// Returns a new batch with merged rows.
    fn merge_overlay_into_batch(
        &mut self,
        base: ColumnBatch,
        leaflet_last_key: &RunRecordV2,
    ) -> ColumnBatch {
        let order = self.order;
        let to_t = self.to_t;

        let mut out_s_id: Vec<u64> = Vec::new();
        let mut out_o_key: Vec<u64> = Vec::new();
        let mut out_p_id: Vec<u32> = Vec::new();
        let mut out_o_type: Vec<u16> = Vec::new();
        let mut out_o_i: Vec<u32> = Vec::new();
        let mut out_t: Vec<u32> = Vec::new();

        let mut ri = 0usize;
        let row_count = base.row_count;

        let ov_end = self.leaf_overlay_end;

        while ri < row_count || self.overlay_pos < ov_end {
            // Determine which side to advance.
            if ri >= row_count {
                // Rows exhausted — drain overlay asserts that sort within this leaflet's key range.
                if self.overlay_pos >= ov_end {
                    break;
                }
                let ov = &self.overlay_ops[self.overlay_pos];

                // Stop if the overlay op sorts AFTER this leaflet.
                if cmp_overlay_vs_record(ov, leaflet_last_key, order) == std::cmp::Ordering::Greater
                {
                    break;
                }

                if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                    push_overlay_row(
                        ov,
                        &mut out_s_id,
                        &mut out_o_key,
                        &mut out_p_id,
                        &mut out_o_type,
                        &mut out_o_i,
                        &mut out_t,
                    );
                }
                self.overlay_pos += 1;
                continue;
            }

            if self.overlay_pos >= ov_end {
                // Overlay exhausted for this leaf — drain remaining rows.
                push_batch_row(
                    &base,
                    ri,
                    &mut out_s_id,
                    &mut out_o_key,
                    &mut out_p_id,
                    &mut out_o_type,
                    &mut out_o_i,
                    &mut out_t,
                );
                ri += 1;
                continue;
            }

            // Both sides have elements — compare.
            let ov = &self.overlay_ops[self.overlay_pos];
            let r_s = base.s_id.get(ri);
            let r_p = base.p_id.get_or(ri, 0);
            let r_ot = base.o_type.get_or(ri, 0);
            let r_ok = base.o_key.get(ri);
            let r_oi = base.o_i.get_or(ri, u32::MAX);

            match cmp_row_vs_overlay(r_s, r_p, r_ot, r_ok, r_oi, ov, order) {
                std::cmp::Ordering::Less => {
                    // Row sorts before overlay → emit row.
                    push_batch_row(
                        &base,
                        ri,
                        &mut out_s_id,
                        &mut out_o_key,
                        &mut out_p_id,
                        &mut out_o_type,
                        &mut out_o_i,
                        &mut out_t,
                    );
                    ri += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Overlay sorts before row → emit assert, skip retract.
                    if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                        push_overlay_row(
                            ov,
                            &mut out_s_id,
                            &mut out_o_key,
                            &mut out_p_id,
                            &mut out_o_type,
                            &mut out_o_i,
                            &mut out_t,
                        );
                    }
                    self.overlay_pos += 1;
                }
                std::cmp::Ordering::Equal => {
                    // Same sort position — check full identity (all 5 fields).
                    let same_identity = r_s == ov.s_id
                        && r_p == ov.p_id
                        && r_ot == ov.o_type
                        && r_ok == ov.o_key
                        && r_oi == ov.o_i;

                    if same_identity {
                        // Same fact: assert → overlay replaces row; retract → omit row.
                        if ov.op && ov.t <= to_t && self.filter_overlay(ov) {
                            push_overlay_row(
                                ov,
                                &mut out_s_id,
                                &mut out_o_key,
                                &mut out_p_id,
                                &mut out_o_type,
                                &mut out_o_i,
                                &mut out_t,
                            );
                        }
                        // Both consumed.
                        ri += 1;
                        self.overlay_pos += 1;
                    } else {
                        // Sort-position tie but different identity — emit row, retry overlay.
                        push_batch_row(
                            &base,
                            ri,
                            &mut out_s_id,
                            &mut out_o_key,
                            &mut out_p_id,
                            &mut out_o_type,
                            &mut out_o_i,
                            &mut out_t,
                        );
                        ri += 1;
                    }
                }
            }
        }

        ColumnBatch {
            row_count: out_s_id.len(),
            s_id: ColumnData::Block(out_s_id.into()),
            o_key: ColumnData::Block(out_o_key.into()),
            p_id: ColumnData::Block(out_p_id.into()),
            o_type: ColumnData::Block(out_o_type.into()),
            o_i: ColumnData::Block(out_o_i.into()),
            t: ColumnData::Block(out_t.into()),
        }
    }

    /// Emit remaining overlay ops as a batch (overlay-only path).
    ///
    /// Called when all indexed leaves are exhausted but overlay ops remain.
    /// These represent facts that exist only in novelty (e.g., new subjects).
    fn emit_overlay_only(&mut self) -> ColumnBatch {
        let mut out_s_id: Vec<u64> = Vec::new();
        let mut out_o_key: Vec<u64> = Vec::new();
        let mut out_p_id: Vec<u32> = Vec::new();
        let mut out_o_type: Vec<u16> = Vec::new();
        let mut out_o_i: Vec<u32> = Vec::new();
        let mut out_t: Vec<u32> = Vec::new();

        while self.overlay_pos < self.overlay_ops.len() {
            let ov = &self.overlay_ops[self.overlay_pos];
            self.overlay_pos += 1;

            if !ov.op || ov.t > self.to_t {
                continue;
            }
            if !self.filter_overlay(ov) {
                continue;
            }

            push_overlay_row(
                ov,
                &mut out_s_id,
                &mut out_o_key,
                &mut out_p_id,
                &mut out_o_type,
                &mut out_o_i,
                &mut out_t,
            );
        }

        ColumnBatch {
            row_count: out_s_id.len(),
            s_id: ColumnData::Block(out_s_id.into()),
            o_key: ColumnData::Block(out_o_key.into()),
            p_id: ColumnData::Block(out_p_id.into()),
            o_type: ColumnData::Block(out_o_type.into()),
            o_i: ColumnData::Block(out_o_i.into()),
            t: ColumnData::Block(out_t.into()),
        }
    }

    /// Check if an overlay op passes the current filter.
    #[inline]
    fn filter_overlay(&self, ov: &OverlayOp) -> bool {
        self.filter
            .matches(ov.s_id, ov.p_id, ov.o_type, ov.o_key, ov.o_i)
    }
}

// ============================================================================
// Helpers
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn push_batch_row(
    batch: &ColumnBatch,
    i: usize,
    s_id: &mut Vec<u64>,
    o_key: &mut Vec<u64>,
    p_id: &mut Vec<u32>,
    o_type: &mut Vec<u16>,
    o_i: &mut Vec<u32>,
    t: &mut Vec<u32>,
) {
    s_id.push(batch.s_id.get(i));
    o_key.push(batch.o_key.get(i));
    p_id.push(batch.p_id.get_or(i, 0));
    o_type.push(batch.o_type.get_or(i, 0));
    o_i.push(batch.o_i.get_or(i, u32::MAX));
    t.push(batch.t.get_or(i, 0));
}

fn push_overlay_row(
    ov: &OverlayOp,
    s_id: &mut Vec<u64>,
    o_key: &mut Vec<u64>,
    p_id: &mut Vec<u32>,
    o_type: &mut Vec<u16>,
    o_i: &mut Vec<u32>,
    t: &mut Vec<u32>,
) {
    s_id.push(ov.s_id);
    o_key.push(ov.o_key);
    p_id.push(ov.p_id);
    o_type.push(ov.o_type);
    o_i.push(ov.o_i);
    t.push(u32::try_from(ov.t.max(0)).unwrap_or_else(|_| {
        tracing::warn!(overlay_t = ov.t, "overlay t does not fit u32, clamping");
        u32::MAX
    }));
}

// ============================================================================
// Filtering
// ============================================================================

/// Apply the filter to a batch, returning only matching rows.
/// Returns the batch unchanged if all rows match (avoids copy).
/// Check if any row in the batch has `t > t_target`.
fn batch_has_rows_above_t(batch: &ColumnBatch, t_target: u32) -> bool {
    match &batch.t {
        ColumnData::Block(ts) => ts.iter().any(|&t| t > t_target),
        ColumnData::Const(t) => *t > t_target,
        ColumnData::AbsentDefault => false,
    }
}

fn filter_batch(filter: &BinaryFilter, batch: &ColumnBatch) -> ColumnBatch {
    let mut matching: Vec<usize> = Vec::new();
    for i in 0..batch.row_count {
        let s_id = batch.s_id.get(i); // always present
        let o_key = batch.o_key.get(i); // always present
        let p_id = batch.p_id.get_or(i, 0);
        let o_type = batch.o_type.get_or(i, 0);
        let o_i = batch.o_i.get_or(i, u32::MAX);
        if filter.matches(s_id, p_id, o_type, o_key, o_i) {
            matching.push(i);
        }
    }

    if matching.len() == batch.row_count {
        return batch.clone();
    }

    gather_batch(batch, &matching)
}

/// Gather rows at the given indices from a batch into a new batch.
fn gather_batch(src: &ColumnBatch, indices: &[usize]) -> ColumnBatch {
    ColumnBatch {
        row_count: indices.len(),
        s_id: gather_column(&src.s_id, indices),
        o_key: gather_column(&src.o_key, indices),
        p_id: gather_column(&src.p_id, indices),
        o_type: gather_column(&src.o_type, indices),
        o_i: gather_column(&src.o_i, indices),
        t: gather_column(&src.t, indices),
    }
}

fn gather_column<T: Copy>(col: &ColumnData<T>, indices: &[usize]) -> ColumnData<T> {
    match col {
        ColumnData::Block(arr) => {
            let gathered: Vec<T> = indices.iter().map(|&i| arr[i]).collect();
            ColumnData::Block(gathered.into())
        }
        ColumnData::Const(v) => ColumnData::Const(*v),
        ColumnData::AbsentDefault => ColumnData::AbsentDefault,
    }
}

/// Compute the overlay op window `[start, end)` for a given leaf.
///
/// For the first leaf in the scan range (`is_first_leaf = true`), includes
/// all pre-leaf overlay ops so the merge loop can emit novelty-only rows
/// that sort before the first indexed data. Without this, subjects whose
/// sort keys precede all leaflets are silently dropped. See issue #95.
pub(crate) fn compute_overlay_window(
    ops: &[OverlayOp],
    leaf_first_key: &crate::format::run_record_v2::RunRecordV2,
    next_leaf_first_key: Option<&crate::format::run_record_v2::RunRecordV2>,
    order: crate::format::run_record::RunSortOrder,
    is_first_leaf: bool,
) -> (usize, usize) {
    use super::types::cmp_overlay_vs_record;
    use std::cmp::Ordering;

    if ops.is_empty() {
        return (0, 0);
    }

    // Start: skip ops that sort before this leaf's first_key.
    // Exception: for the FIRST leaf in the scan range, include all pre-leaf
    // overlay ops. The merge loop handles them correctly via the "overlay
    // sorts before row" case (emits asserts, skips retracts).
    let start = if is_first_leaf {
        0
    } else {
        ops.partition_point(|ov| cmp_overlay_vs_record(ov, leaf_first_key, order) == Ordering::Less)
    };

    // End: find first op >= next leaf's first_key (or all remaining if last leaf).
    let end = match next_leaf_first_key {
        Some(next_key) => {
            ops.partition_point(|ov| cmp_overlay_vs_record(ov, next_key, order) == Ordering::Less)
        }
        None => ops.len(),
    };

    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::run_record::RunSortOrder;
    use crate::format::run_record_v2::RunRecordV2;
    use crate::read::types::OverlayOp;
    use fluree_db_core::subject_id::SubjectId;

    fn make_op(s_id: u64, p_id: u32) -> OverlayOp {
        OverlayOp {
            s_id,
            p_id,
            o_type: 0,
            o_key: 0,
            o_i: u32::MAX,
            t: 1,
            op: true,
        }
    }

    fn make_key(s_id: u64, p_id: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id: 0,
        }
    }

    #[test]
    fn first_leaf_includes_pre_leaf_ops() {
        // Overlay ops with s_id=5 sort BEFORE leaf first_key s_id=10.
        let ops = vec![make_op(5, 1), make_op(5, 2), make_op(5, 3)];
        let leaf_key = make_key(10, 0);
        let next_key = make_key(20, 0);

        // First leaf: should include pre-leaf ops (start=0).
        let (start, end) =
            compute_overlay_window(&ops, &leaf_key, Some(&next_key), RunSortOrder::Spot, true);
        assert_eq!(start, 0, "first leaf should NOT skip pre-leaf ops");
        assert_eq!(end, 3, "all ops sort before next leaf");

        // Non-first leaf: should skip pre-leaf ops.
        let (start, end) =
            compute_overlay_window(&ops, &leaf_key, Some(&next_key), RunSortOrder::Spot, false);
        assert_eq!(start, 3, "non-first leaf should skip pre-leaf ops");
        assert_eq!(end, 3, "no ops remain for this leaf");
    }

    #[test]
    fn end_boundary_with_next_leaf() {
        // Ops spanning two leaves: s_ids 5, 15, 25.
        // Leaf covers [10, 20), so only s_id=15 belongs to it.
        let ops = vec![make_op(5, 0), make_op(15, 0), make_op(25, 0)];
        let leaf_key = make_key(10, 0);
        let next_key = make_key(20, 0);

        // Non-first leaf: skip pre-leaf op (s_id=5), include s_id=15, exclude s_id=25.
        let (start, end) =
            compute_overlay_window(&ops, &leaf_key, Some(&next_key), RunSortOrder::Spot, false);
        assert_eq!(start, 1, "skip 1 pre-leaf op");
        assert_eq!(end, 2, "include s_id=15, exclude s_id=25");
    }

    #[test]
    fn last_leaf_includes_all_remaining() {
        let ops = vec![make_op(50, 0), make_op(100, 0), make_op(200, 0)];
        let leaf_key = make_key(40, 0);

        let (start, end) = compute_overlay_window(
            &ops,
            &leaf_key,
            None, // last leaf — no next
            RunSortOrder::Spot,
            false,
        );
        assert_eq!(start, 0, "no ops sort before this leaf");
        assert_eq!(end, 3, "last leaf gets all remaining ops");
    }

    #[test]
    fn empty_ops() {
        let ops: Vec<OverlayOp> = vec![];
        let leaf_key = make_key(10, 0);

        let (start, end) = compute_overlay_window(&ops, &leaf_key, None, RunSortOrder::Spot, true);
        assert_eq!((start, end), (0, 0));
    }
}
