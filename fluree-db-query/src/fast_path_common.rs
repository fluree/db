//! Shared primitives for fast-path query operators.
//!
//! All fast-path operators (fused scan + aggregate) share common building blocks:
//! predicate resolution, leaf range scanning, subject collection, and operator plumbing.
//! This module consolidates them to avoid ~1,100 lines of duplication across 9 files.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::triple::Ref;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::read::column_loader::load_columns_cached_via_handle;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryIndexStore, ColumnBatch, ColumnProjection, ColumnSet,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// 0. Shared string-ID range helpers
// ---------------------------------------------------------------------------

/// Sort a list of dictionary string IDs and verify they form a single contiguous range.
///
/// Returns `[(start, end)]` on success. Errors if the IDs are not contiguous.
///
/// Used by both `fast_string_prefix_count_all` and `BinaryScanOperator::build_prefix_id_ranges`.
pub fn contiguous_id_range(ids: &[u32]) -> Result<Vec<(u32, u32)>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    let start = sorted[0];
    let end = *sorted.last().unwrap_or(&start);
    let span_len = u64::from(end) - u64::from(start) + 1;
    if span_len != sorted.len() as u64 {
        return Err(QueryError::execution(
            "prefix string ids are not contiguous; refusing range pushdown",
        ));
    }
    Ok(vec![(start, end)])
}

// ---------------------------------------------------------------------------
// 1. Predicate resolution
// ---------------------------------------------------------------------------

/// Resolve a predicate `Ref` to its `Sid`, returning an error for variables.
pub fn normalize_pred_sid(store: &BinaryIndexStore, pred: &Ref) -> Result<Sid> {
    Ok(match pred {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast-path requires bound predicates".to_string(),
            ))
        }
    })
}

/// Like [`normalize_pred_sid`] but returns `None` for variables instead of an error.
pub fn try_normalize_pred_sid(store: &BinaryIndexStore, pred: &Ref) -> Option<Sid> {
    match pred {
        Ref::Sid(s) => Some(s.clone()),
        Ref::Iri(i) => Some(store.encode_iri(i)),
        Ref::Var(_) => None,
    }
}

// ---------------------------------------------------------------------------
// 2. Column projection helpers
// ---------------------------------------------------------------------------

/// Projection that loads only the SId column (internal, not output).
#[inline]
pub fn projection_sid_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s
        },
    }
}

/// Projection that loads only the OKey column (internal, not output).
#[inline]
pub fn projection_okey_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads SId + OKey columns (internal, not output).
#[inline]
pub fn projection_sid_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads SId + OType + OKey columns (internal, not output).
#[inline]
pub fn projection_sid_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads OType + OKey columns (internal, not output).
#[inline]
pub fn projection_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads only the OType column (internal, not output).
#[inline]
pub fn projection_otype_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OType);
            s
        },
    }
}

/// Projection for `BinaryCursor` that outputs SId + OType + OKey columns.
///
/// Unlike `projection_sid_otype_okey` (which uses `internal` for raw leaf access),
/// this places columns in `output` as required by `BinaryCursor`.
#[inline]
pub fn cursor_projection_sid_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
        internal: ColumnSet::EMPTY,
    }
}

/// Projection for `BinaryCursor` that outputs only the SId column.
#[inline]
pub fn cursor_projection_sid_only() -> ColumnProjection {
    ColumnProjection {
        output: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s
        },
        internal: ColumnSet::EMPTY,
    }
}

// ---------------------------------------------------------------------------
// 3. Leaf range scanning
// ---------------------------------------------------------------------------

/// Construct min/max `RunRecordV2` keys spanning all rows for a predicate.
#[inline]
fn predicate_range_keys(p_id: u32, g_id: GraphId) -> (RunRecordV2, RunRecordV2) {
    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    (min_key, max_key)
}

/// Find leaf entries for a predicate in a given sort order.
///
/// Returns an empty slice if the branch does not exist.
pub fn leaf_entries_for_predicate(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
    p_id: u32,
) -> &[LeafEntry] {
    let Some(branch) = store.branch_for_order(g_id, order) else {
        return &[];
    };
    let cmp = cmp_v2_for_order(order);
    let (min_key, max_key) = predicate_range_keys(p_id, g_id);
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    &branch.leaves[leaf_range]
}

// ---------------------------------------------------------------------------
// 4. Subject collection
// ---------------------------------------------------------------------------

/// Collect distinct subject IDs from PSOT for a predicate as a sorted `Vec<u64>`.
///
/// PSOT guarantees subjects are emitted in sorted order within a fixed predicate,
/// so deduplication is a simple consecutive check.
pub fn collect_subjects_for_predicate_sorted(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Vec<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_only();

    let mut out: Vec<u64> = Vec::new();
    let mut prev: Option<u64> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if prev != Some(sid) {
                    out.push(sid);
                    prev = Some(sid);
                }
            }
        }
    }
    Ok(out)
}

/// Collect distinct subject IDs from PSOT for a predicate as an `FxHashSet<u64>`.
///
/// Preferred when the caller needs O(1) membership tests rather than merge-join.
pub fn collect_subjects_for_predicate_set(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<FxHashSet<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_only();

    let mut out: FxHashSet<u64> = FxHashSet::default();
    let mut prev: Option<u64> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if prev != Some(sid) {
                    out.insert(sid);
                    prev = Some(sid);
                }
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 5. Sorted set operations
// ---------------------------------------------------------------------------

/// Two-pointer intersection of two sorted, deduplicated `u64` slices.
pub fn intersect_sorted(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (av, bv) = (a[i], b[j]);
        match av.cmp(&bv) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(av);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// N-way intersection of sorted `u64` lists. Sorts by length and folds pairwise.
pub fn intersect_many_sorted(mut lists: Vec<Vec<u64>>) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    lists.sort_by_key(std::vec::Vec::len);
    let mut acc = lists.remove(0);
    for next in lists {
        if acc.is_empty() {
            break;
        }
        acc = intersect_sorted(&acc, &next);
    }
    acc
}

// ---------------------------------------------------------------------------
// 6. Merge-count
// ---------------------------------------------------------------------------

/// Count total rows for a predicate by summing PSOT leaflet directory `row_count`.
///
/// This is the fastest possible implementation of:
/// `SELECT (COUNT(*) AS ?c) WHERE { ?s <p> ?o }`
/// (and also `COUNT(?s)` / `COUNT(?o)` for the same single-triple pattern),
/// because every solution binding has all vars bound.
///
/// Assumes PSOT leaflets have `p_const` set (so we can filter without loading columns).
pub fn count_rows_for_predicate_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<u64> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_id) {
                continue;
            }
            total += entry.row_count as u64;
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// 7. Streaming PSOT subject-count iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over PSOT leaflets for a predicate that yields
/// `(subject_id, row_count)` groups in sorted subject order.
pub struct PsotSubjectCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    /// Accumulated subject for a group that may span leaflet boundaries.
    cur_s: Option<u64>,
    cur_count: u64,
}

impl<'a> PsotSubjectCountIter<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Self> {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Ok(Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_s: None,
            cur_count: 0,
        })
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = projection_sid_only();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let batch = if let Some(cache) = self.store.leaflet_cache() {
                    let idx_u32: u32 = idx
                        .try_into()
                        .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?;
                    load_columns_cached_via_handle(
                        handle.as_ref(),
                        idx,
                        RunSortOrder::Psot,
                        cache,
                        handle.leaf_id(),
                        idx_u32,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                } else {
                    handle
                        .load_columns(idx, &projection, RunSortOrder::Psot)
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                };
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(subject_id, count)` group.
    ///
    /// Groups span leaflet boundaries — a subject that straddles two leaflets
    /// will NOT be split across two calls (the group accumulates until the
    /// subject changes).
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            // Load a batch if needed. If there are no more batches, flush any accumulated group.
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                if let Some(s) = self.cur_s.take() {
                    let n = std::mem::take(&mut self.cur_count);
                    return Ok(Some((s, n)));
                }
                return Ok(None);
            }

            let batch = self.batch.as_ref().unwrap();

            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let sid = batch.s_id.get(self.row);

            match self.cur_s {
                None => {
                    self.cur_s = Some(sid);
                    self.cur_count = 0;
                }
                Some(cur) if cur != sid => {
                    // New subject starts; emit previous group without consuming this row.
                    let out_s = self.cur_s.take().expect("checked: cur_s is Some");
                    let out_n = std::mem::take(&mut self.cur_count);
                    return Ok(Some((out_s, out_n)));
                }
                Some(_) => {}
            }

            // Accumulate current subject (may span batches).
            self.cur_count += 1;
            self.row += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// 7b. Streaming POST object-group-count iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over POST leaflets for a predicate that yields
/// `(object_key, row_count)` groups in POST order, restricted to IRI_REF objects.
///
/// Returns `Ok(None)` from `next_group` if a non-IRI_REF leaflet is encountered
/// (unless it's a mixed-type leaflet, in which case non-IRI rows are skipped).
pub struct PostObjectGroupCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    mixed: bool,
}

impl<'a> PostObjectGroupCountIter<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            mixed: false,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let proj_okey = projection_okey_only();
        let proj_otype_okey = projection_otype_okey();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let mixed = entry.o_type_const.is_none();
                if !mixed && entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                    return Ok(None);
                }
                let batch = if let Some(cache) = self.store.leaflet_cache() {
                    let idx_u32: u32 = idx
                        .try_into()
                        .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?;
                    load_columns_cached_via_handle(
                        handle.as_ref(),
                        idx,
                        RunSortOrder::Post,
                        cache,
                        handle.leaf_id(),
                        idx_u32,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                } else {
                    handle
                        .load_columns(
                            idx,
                            if mixed { &proj_otype_okey } else { &proj_okey },
                            RunSortOrder::Post,
                        )
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                };
                self.row = 0;
                self.batch = Some(batch);
                self.mixed = mixed;
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(object_key, count)` group.
    ///
    /// Only counts IRI_REF objects. Mixed-type leaflets are handled by filtering
    /// to IRI rows. Returns `None` when exhausted or if a non-IRI homogeneous
    /// leaflet is encountered.
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            if !self.mixed {
                let b = batch.o_key.get(self.row);
                let mut count: u64 = 0;
                while self.row < batch.row_count && batch.o_key.get(self.row) == b {
                    count += 1;
                    self.row += 1;
                }
                return Ok(Some((b, count)));
            }

            // Mixed-type leaflet: skip non-IRI_REF rows and group by o_key.
            while self.row < batch.row_count
                && batch.o_type.get(self.row) != OType::IRI_REF.as_u16()
            {
                self.row += 1;
            }
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b = batch.o_key.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count
                && batch.o_type.get(self.row) == OType::IRI_REF.as_u16()
                && batch.o_key.get(self.row) == b
            {
                count += 1;
                self.row += 1;
            }
            return Ok(Some((b, count)));
        }
    }
}

// ---------------------------------------------------------------------------
// 7c. PSOT subject-weighted-sum iterator (for OPTIONAL chain patterns)
// ---------------------------------------------------------------------------

/// Streaming iterator over PSOT leaflets for a predicate that yields
/// `(subject_id, weighted_sum)` groups, where the weight of each object is
/// looked up in a `FxHashMap<u64, u64>`.
///
/// Used by OPTIONAL chain patterns:
/// - **Head** (`default_weight = 0`): `Σ_{c in p2(b)} n3(c)` where missing c → 0
/// - **Tail** (`default_weight = 1`): `Σ_{c in p2(b)} max(1, n3(c))` where missing c → 1
///
/// Requires IRI_REF objects (o_key is a subject ID). Mixed-type leaflets are
/// handled by treating non-IRI rows as weight 0.
pub struct PsotSubjectWeightedSumIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    weights: &'a FxHashMap<u64, u64>,
    default_weight: u64,
    /// Optional allowlist of subject IDs to emit groups for.
    /// When present, the iterator will **skip entire subject groups** that are
    /// not in this sorted list, and will stop early once the list is exhausted.
    allowed_subjects: Option<&'a [u64]>,
    allowed_pos: usize,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    cur_b: Option<u64>,
    cur_sum: u64,
    mixed: bool,
    /// True when the current batch is a pure non-IRI_REF leaflet —
    /// every row gets `default_weight` without looking up `o_key` in `weights`.
    all_default: bool,
}

impl<'a> PsotSubjectWeightedSumIter<'a> {
    /// Create a new iterator. `default_weight` is used for objects not in `weights`.
    pub fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        weights: &'a FxHashMap<u64, u64>,
        default_weight: u64,
    ) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            weights,
            default_weight,
            allowed_subjects: None,
            allowed_pos: 0,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_b: None,
            cur_sum: 0,
            mixed: false,
            all_default: false,
        }))
    }

    /// Create a new iterator that only emits groups for subjects in `allowed_subjects`.
    ///
    /// `allowed_subjects` must be sorted ascending and must not contain duplicates.
    // Kept for: filtered-subject weighted-sum fast path (e.g., COUNT with WHERE filter).
    // Use when: fast-path COUNT(*) adds subject-filtering support.
    #[expect(dead_code)]
    pub fn new_filtered_subjects(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        weights: &'a FxHashMap<u64, u64>,
        default_weight: u64,
        allowed_subjects: &'a [u64],
    ) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            weights,
            default_weight,
            allowed_subjects: Some(allowed_subjects),
            allowed_pos: 0,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_b: None,
            cur_sum: 0,
            mixed: false,
            all_default: false,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let proj_sid_okey = projection_sid_okey();
        let proj_sid_otype_okey = projection_sid_otype_okey();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let mixed = entry.o_type_const.is_none();
                let iri_only = entry.o_type_const == Some(OType::IRI_REF.as_u16());
                let non_iri_only = !mixed && !iri_only;

                if non_iri_only && self.default_weight == 0 {
                    // Every row would get weight 0 — skip without terminating.
                    continue;
                }

                let batch = if let Some(cache) = self.store.leaflet_cache() {
                    let idx_u32: u32 = idx
                        .try_into()
                        .map_err(|_| QueryError::Internal("leaflet idx exceeds u32".to_string()))?;
                    load_columns_cached_via_handle(
                        handle.as_ref(),
                        idx,
                        RunSortOrder::Psot,
                        cache,
                        handle.leaf_id(),
                        idx_u32,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                } else {
                    handle
                        .load_columns(
                            idx,
                            if mixed {
                                &proj_sid_otype_okey
                            } else {
                                &proj_sid_okey
                            },
                            RunSortOrder::Psot,
                        )
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?
                };
                self.row = 0;
                self.batch = Some(batch);
                self.mixed = mixed;
                self.all_default = non_iri_only;
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(subject_id, weighted_sum)` group with non-zero sum,
    /// or `None` when exhausted.
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        if self.allowed_subjects.is_none() {
            return self.next_group_unfiltered();
        }
        self.next_group_filtered()
    }

    fn next_group_unfiltered(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                if let Some(b) = self.cur_b.take() {
                    let n = std::mem::take(&mut self.cur_sum);
                    // We are flushing the final allowed subject group at exhaustion.
                    // Advancing keeps `allowed_pos` consistent even though we will
                    // immediately return `None` on the next call.
                    self.allowed_pos += 1;
                    return Ok(Some((b, n)));
                }
                return Ok(None);
            }

            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b = batch.s_id.get(self.row);

            let w = if self.all_default {
                // Pure non-IRI_REF leaflet — o_key can't be a subject ID.
                self.default_weight
            } else if self.mixed && batch.o_type.get(self.row) != OType::IRI_REF.as_u16() {
                // Non-IRI_REF row in mixed leaflet — o_key can't be a subject ID.
                self.default_weight
            } else {
                let c = batch.o_key.get(self.row);
                self.weights.get(&c).copied().unwrap_or(self.default_weight)
            };

            match self.cur_b {
                None => {
                    self.cur_b = Some(b);
                    self.cur_sum = 0;
                }
                Some(cur) if cur != b => {
                    let out_b = self.cur_b.replace(b).expect("checked: cur_b is Some");
                    let out_n = std::mem::replace(&mut self.cur_sum, w);
                    self.row += 1;
                    return Ok(Some((out_b, out_n)));
                }
                Some(_) => {}
            }

            self.cur_sum += w;
            self.row += 1;
        }
    }

    fn next_group_filtered(&mut self) -> Result<Option<(u64, u64)>> {
        let allowed = self
            .allowed_subjects
            .expect("checked: allowed_subjects is Some");
        loop {
            if self.allowed_pos >= allowed.len() {
                // All requested subjects were processed — stop early.
                if let Some(b) = self.cur_b.take() {
                    let n = std::mem::take(&mut self.cur_sum);
                    return Ok(Some((b, n)));
                }
                return Ok(None);
            }

            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                if let Some(b) = self.cur_b.take() {
                    let n = std::mem::take(&mut self.cur_sum);
                    return Ok(Some((b, n)));
                }
                return Ok(None);
            }

            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b = batch.s_id.get(self.row);

            // If we are mid-group, do not skip.
            if self.cur_b.is_none() {
                while self.allowed_pos < allowed.len() && allowed[self.allowed_pos] < b {
                    self.allowed_pos += 1;
                }
                if self.allowed_pos >= allowed.len() {
                    return Ok(None);
                }
                let target = allowed[self.allowed_pos];
                if b < target {
                    // Skip this entire subject group quickly.
                    let skip_b = b;
                    while self.row < batch.row_count && batch.s_id.get(self.row) == skip_b {
                        self.row += 1;
                    }
                    continue;
                }
                // b == target → allow group, but don't advance allowed_pos until the group ends.
            }

            // If we reached a new subject, emit the previous group (if any) before consuming this row.
            if let Some(cur) = self.cur_b {
                if cur != b {
                    let out_b = self.cur_b.take().expect("checked: cur_b is Some");
                    let out_n = std::mem::take(&mut self.cur_sum);
                    // Completed one allowed subject group.
                    self.allowed_pos += 1;
                    return Ok(Some((out_b, out_n)));
                }
            } else {
                self.cur_b = Some(b);
                self.cur_sum = 0;
            }

            let w = if self.all_default
                || (self.mixed && batch.o_type.get(self.row) != OType::IRI_REF.as_u16())
            {
                self.default_weight
            } else {
                let c = batch.o_key.get(self.row);
                self.weights.get(&c).copied().unwrap_or(self.default_weight)
            };

            self.cur_sum += w;
            self.row += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// 7d. PSOT object-filtered group-count iterator (unified Include/Exclude)
// ---------------------------------------------------------------------------

/// Whether to count objects that ARE in the set or are NOT in the set.
#[derive(Clone, Copy)]
pub enum ObjectFilterMode {
    /// Count objects whose `o_key` IS in the set (EXISTS / join semantics).
    InSet,
    /// Count objects whose `o_key` is NOT in the set (MINUS / anti-join semantics).
    NotInSet,
}

/// Streaming iterator over PSOT leaflets for a predicate that yields
/// `(subject_id, filtered_count)` groups — counting objects that either appear
/// in or do not appear in a reference set, depending on [`ObjectFilterMode`].
///
/// Requires `o_type_const == IRI_REF` so that `o_key` is a subject ID.
/// Returns `Ok(None)` from the constructor or `next_group` if a non-IRI_REF
/// leaflet is encountered.
pub struct PsotObjectFilterCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    reference_set: &'a FxHashSet<u64>,
    mode: ObjectFilterMode,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    /// True when the current batch is a pure non-IRI_REF leaflet in `NotInSet` mode —
    /// all rows count without checking the reference set.
    all_count: bool,
    /// True when the current batch is a mixed leaflet (o_type_const is None) —
    /// each row must be checked for IRI_REF type before set membership lookup.
    mixed: bool,
}

impl<'a> PsotObjectFilterCountIter<'a> {
    pub fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        reference_set: &'a FxHashSet<u64>,
        mode: ObjectFilterMode,
    ) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            reference_set,
            mode,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            all_count: false,
            mixed: false,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let proj_sid_okey = projection_sid_okey();
        let proj_sid_otype_okey = projection_sid_otype_okey();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }

                let iri_only = entry.o_type_const == Some(OType::IRI_REF.as_u16());
                let mixed = entry.o_type_const.is_none();
                let non_iri_only = !iri_only && !mixed;

                if non_iri_only {
                    // Non-IRI_REF objects can never be in the reference set (which
                    // contains subject IDs). InSet → no rows match, skip leaflet.
                    // NotInSet → all rows match, count every row per subject.
                    if matches!(self.mode, ObjectFilterMode::InSet) {
                        continue;
                    }
                    // NotInSet: load s_id + o_key, mark all_count so next_group
                    // counts every row without checking the reference set.
                    let batch = handle
                        .load_columns(idx, &proj_sid_okey, RunSortOrder::Psot)
                        .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                    self.row = 0;
                    self.batch = Some(batch);
                    self.all_count = true;
                    self.mixed = false;
                    return Ok(Some(()));
                }

                let projection = if mixed {
                    &proj_sid_otype_okey
                } else {
                    &proj_sid_okey
                };
                let batch = handle
                    .load_columns(idx, projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                self.all_count = false;
                self.mixed = mixed;
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(subject_id, count)` group with non-zero count.
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            let b_id = batch.s_id.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count && batch.s_id.get(self.row) == b_id {
                let counts = if self.all_count {
                    // Pure non-IRI_REF leaflet + NotInSet: all rows count.
                    true
                } else if self.mixed {
                    // Mixed leaflet: check o_type per row.
                    let is_iri = batch.o_type.get(self.row) == OType::IRI_REF.as_u16();
                    if is_iri {
                        let c_id = batch.o_key.get(self.row);
                        let in_set = self.reference_set.contains(&c_id);
                        match self.mode {
                            ObjectFilterMode::InSet => in_set,
                            ObjectFilterMode::NotInSet => !in_set,
                        }
                    } else {
                        // Non-IRI_REF: can't be in the IRI reference set.
                        matches!(self.mode, ObjectFilterMode::NotInSet)
                    }
                } else {
                    // Pure IRI_REF leaflet: check set membership.
                    let c_id = batch.o_key.get(self.row);
                    let in_set = self.reference_set.contains(&c_id);
                    match self.mode {
                        ObjectFilterMode::InSet => in_set,
                        ObjectFilterMode::NotInSet => !in_set,
                    }
                };
                if counts {
                    count += 1;
                }
                self.row += 1;
            }
            if count > 0 {
                return Ok(Some((b_id, count)));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 7e. Object-in-set subject collection and POST filtered counting
// ---------------------------------------------------------------------------

/// Collect subjects from PSOT(p_id) where any object `o_key` is in `object_set`.
///
/// Returns a sorted `Vec<u64>` of qualifying subject IDs, or `None` if any
/// leaflet has a non-IRI_REF `o_type_const`.
pub fn collect_subjects_with_object_in_set(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    object_set: &FxHashSet<u64>,
) -> Result<Option<Vec<u64>>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_okey();
    let mut out: Vec<u64> = Vec::new();

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                return Ok(None);
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            let mut i = 0usize;
            while i < batch.row_count {
                let b_id = batch.s_id.get(i);
                let mut ok = false;
                while i < batch.row_count && batch.s_id.get(i) == b_id {
                    let c_id = batch.o_key.get(i);
                    if object_set.contains(&c_id) {
                        ok = true;
                    }
                    i += 1;
                }
                if ok {
                    out.push(b_id);
                }
            }
        }
    }

    Ok(Some(out))
}

/// Sum row counts from POST(p_id) for object groups whose `o_key` is in
/// `allowed_objects_sorted` (must be pre-sorted ascending).
///
/// Uses a merge-scan between sorted POST groups and the sorted allowed list.
/// Returns `None` if any leaflet has non-IRI_REF `o_type_const`.
pub fn sum_post_object_counts_filtered(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    allowed_objects_sorted: &[u64],
) -> Result<Option<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let projection = projection_okey_only();

    let mut allowed_idx: usize = 0;
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                return Ok(None);
            }

            let batch: ColumnBatch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            let mut i = 0usize;
            while i < batch.row_count {
                let b_id = batch.o_key.get(i);
                let mut count: u64 = 0;
                while i < batch.row_count && batch.o_key.get(i) == b_id {
                    count += 1;
                    i += 1;
                }

                while allowed_idx < allowed_objects_sorted.len()
                    && allowed_objects_sorted[allowed_idx] < b_id
                {
                    allowed_idx += 1;
                }
                if allowed_idx < allowed_objects_sorted.len()
                    && allowed_objects_sorted[allowed_idx] == b_id
                {
                    total = total.saturating_add(count);
                }
            }
        }
    }

    Ok(Some(total))
}

// ---------------------------------------------------------------------------
// 8. BinaryCursor construction
// ---------------------------------------------------------------------------

/// Build a `BinaryCursor` for a single predicate in PSOT order with overlay support.
///
/// This is the cursor-based counterpart of the leaf-entry iterators in sections 7/7b.
/// It supports overlay merging (uncommitted flakes), so it works even when
/// `ctx.overlay` is set — unlike the raw leaf-entry scan which requires `fast_path_store`.
///
/// Returns `None` if the PSOT branch does not exist for the given graph.
pub fn build_psot_cursor_for_predicate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(None);
    };
    let branch = Arc::clone(branch);

    let (min_key, max_key) = predicate_range_keys(p_id, g_id);

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Psot,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );
    cursor.set_to_t(ctx.to_t);

    // Overlay merge — pre-filter by predicate.
    if ctx.overlay.is_some() {
        use std::collections::HashMap;
        let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
            Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
        });
        let mut ephemeral_preds = HashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();
        let mut translate_failed = false;
        let mut translate_fail_count: u32 = 0;

        ctx.overlay().for_each_overlay_flake(
            g_id,
            fluree_db_core::IndexType::Psot,
            None,
            None,
            true,
            ctx.to_t,
            &mut |flake| {
                if flake.p != pred_sid {
                    return;
                }
                match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    Some(&dn),
                    ctx.runtime_small_dicts,
                    &mut ephemeral_preds,
                    &mut next_ep,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        translate_failed = true;
                        translate_fail_count = translate_fail_count.saturating_add(1);
                        if translate_fail_count == 1 {
                            tracing::warn!(
                                error = %e,
                                s = %flake.s,
                                p = %flake.p,
                                t = flake.t,
                                op = flake.op,
                                "fast-path cursor: overlay flake translation failed; disabling fast path for correctness"
                            );
                        }
                    }
                }
            },
        );
        if translate_failed {
            tracing::debug!(
                failures = translate_fail_count,
                "fast-path cursor: falling back due to overlay translation failures"
            );
            return Ok(None);
        }

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, RunSortOrder::Psot);
            fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
            cursor.set_overlay_ops(ops);
        }
        cursor.set_epoch(ctx.overlay().epoch());
    }

    Ok(Some(cursor))
}

/// Resolve a bound `Ref` (Iri or Sid) to its internal `s_id` (u64).
///
/// Returns `Ok(None)` for `Ref::Var` or if the subject is not found in the store.
pub fn subject_ref_to_s_id(
    snapshot: &fluree_db_core::LedgerSnapshot,
    store: &BinaryIndexStore,
    r: &Ref,
) -> Result<Option<u64>> {
    match r {
        Ref::Iri(iri) => Ok(store
            .find_subject_id(iri)
            .map_err(|e| QueryError::Internal(format!("find_subject_id: {e}")))?),
        Ref::Sid(sid) => {
            if let Some(s_id) = store
                .find_subject_id_by_parts(sid.namespace_code, &sid.name)
                .map_err(|e| QueryError::Internal(format!("find_subject_id_by_parts: {e}")))?
            {
                return Ok(Some(s_id));
            }
            if let Some(iri) = snapshot.decode_sid(sid).or_else(|| store.sid_to_iri(sid)) {
                Ok(store
                    .find_subject_id(&iri)
                    .map_err(|e| QueryError::Internal(format!("find_subject_id: {e}")))?)
            } else {
                Ok(None)
            }
        }
        Ref::Var(_) => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// 9. Graph reachability (transitive property paths)
// ---------------------------------------------------------------------------

/// BFS one-or-more reachability count from a single start node.
///
/// Returns the number of unique nodes reachable from `start` via one or more
/// hops in the adjacency map. The start node itself is only counted if a cycle
/// leads back to it.
pub fn reach_count_plus(adj: &FxHashMap<u64, Vec<u64>>, start: u64) -> u64 {
    let mut visited: FxHashSet<u64> = FxHashSet::default();
    let mut q: VecDeque<u64> = VecDeque::new();
    let mut count: u64 = 0;
    let mut added_start_via_cycle = false;

    visited.insert(start);
    q.push_back(start);

    while let Some(cur) = q.pop_front() {
        if let Some(nexts) = adj.get(&cur) {
            for &n in nexts {
                if n == start && !added_start_via_cycle {
                    count = count.saturating_add(1);
                    added_start_via_cycle = true;
                    continue;
                }
                if visited.insert(n) {
                    count = count.saturating_add(1);
                    q.push_back(n);
                }
            }
        }
    }

    count
}

/// BFS one-or-more reachability count from multiple start nodes (union semantics).
///
/// Returns `|⋃ reach_plus(s_i)|` — the number of unique nodes reachable from
/// *any* start node via one or more hops. Start nodes themselves are counted
/// only if a cycle leads back to them.
pub fn reach_count_plus_multi(adj: &FxHashMap<u64, Vec<u64>>, starts: &[u64]) -> u64 {
    if starts.is_empty() {
        return 0;
    }
    if starts.len() == 1 {
        return reach_count_plus(adj, starts[0]);
    }

    let mut starts_set: FxHashSet<u64> = FxHashSet::default();
    let mut counted_starts: FxHashSet<u64> = FxHashSet::default();
    let mut visited: FxHashSet<u64> = FxHashSet::default();
    let mut q: VecDeque<u64> = VecDeque::new();
    let mut count: u64 = 0;

    for &s in starts {
        starts_set.insert(s);
        visited.insert(s);
        q.push_back(s);
    }

    while let Some(cur) = q.pop_front() {
        if let Some(nexts) = adj.get(&cur) {
            for &n in nexts {
                if starts_set.contains(&n) {
                    if counted_starts.insert(n) {
                        count = count.saturating_add(1);
                    }
                    continue;
                }
                if visited.insert(n) {
                    count = count.saturating_add(1);
                    q.push_back(n);
                }
            }
        }
    }

    count
}

/// Build an IRI-ref-only adjacency map from a PSOT cursor.
///
/// Scans all batches from `cursor`, collecting `(s_id -> [o_key])` edges
/// where `o_type == IRI_REF`. Used by transitive property path operators.
pub fn build_iri_adjacency_from_cursor(
    cursor: &mut BinaryCursor,
) -> Result<FxHashMap<u64, Vec<u64>>> {
    let iri_ref = OType::IRI_REF.as_u16();
    let mut adj: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
    while let Some(batch) = cursor
        .next_batch()
        .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?
    {
        for i in 0..batch.row_count {
            if batch.o_type.get(i) != iri_ref {
                continue;
            }
            let s = batch.s_id.get(i);
            let o = batch.o_key.get(i);
            adj.entry(s).or_default().push(o);
        }
    }
    Ok(adj)
}

// ---------------------------------------------------------------------------
// 10. Operator plumbing
// ---------------------------------------------------------------------------

/// Tiny helper operator: yields exactly one precomputed batch, then exhausts.
///
/// Starts in `Open` state since the batch is pre-computed at construction time.
struct PrecomputedSingleBatchOperator {
    batch: Option<Batch>,
    state: OperatorState,
}

impl PrecomputedSingleBatchOperator {
    fn new(batch: Batch) -> Self {
        Self {
            batch: Some(batch),
            state: OperatorState::Open,
        }
    }
}

#[async_trait]
impl Operator for PrecomputedSingleBatchOperator {
    fn schema(&self) -> &[VarId] {
        self.batch
            .as_ref()
            .map(super::binding::Batch::schema)
            .unwrap_or(&[])
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        let out = self.batch.take();
        if out.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.batch = None;
        self.state = OperatorState::Closed;
    }
}

/// Build a single-row batch containing an `xsd:integer` value.
pub fn build_i64_singleton_batch(out_var: VarId, value: i64, label: &str) -> Result<Batch> {
    let schema: Arc<[VarId]> = Arc::from(vec![out_var].into_boxed_slice());
    let col = vec![Binding::lit(FlakeValue::Long(value), Sid::xsd_integer())];
    Batch::new(schema, vec![col])
        .map_err(|e| QueryError::execution(format!("fast-path {label} batch build: {e}")))
}

/// Build a single-row batch containing a count value (`xsd:integer`).
pub fn build_count_batch(out_var: VarId, count: i64) -> Result<Batch> {
    build_i64_singleton_batch(out_var, count, "count")
}

/// Convert a non-negative count to `i64`, erroring on overflow instead of silently capping.
pub fn count_to_i64(count: u64, label: &'static str) -> Result<i64> {
    i64::try_from(count).map_err(|_| QueryError::execution(format!("{label} exceeds i64")))
}

/// Build an empty batch (zero rows) with the given schema.
pub fn empty_batch(schema: Arc<[VarId]>) -> Result<Batch> {
    let cols: Vec<Vec<Binding>> = (0..schema.len()).map(|_| Vec::new()).collect();
    Batch::new(schema, cols).map_err(Into::into)
}

/// Resolve a bound predicate `Ref` to its binary-index predicate ID (`u32`).
///
/// Uses `ExecutionContext` to decode `Sid` variants. Returns an error for `Ref::Var`.
pub fn ref_to_p_id(ctx: &ExecutionContext<'_>, store: &BinaryIndexStore, r: &Ref) -> Result<u32> {
    let iri: Arc<str> = match r {
        Ref::Sid(sid) => ctx
            .decode_sid(sid)
            .map(Arc::from)
            .ok_or_else(|| QueryError::execution("failed to decode predicate SID".to_string()))?,
        Ref::Iri(i) => Arc::clone(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast-path requires bound predicates".to_string(),
            ))
        }
    };
    store.find_predicate_id(iri.as_ref()).ok_or_else(|| {
        QueryError::execution(format!("predicate not found in binary index dict: {iri}"))
    })
}

/// Resolve a bound `Term` (IRI or Sid) to its internal subject ID (`u64`).
///
/// Uses `ExecutionContext` to decode `Sid` variants. Returns `Ok(None)` for
/// non-IRI terms or if the subject is not found in the store.
pub fn term_to_ref_s_id(
    ctx: &ExecutionContext<'_>,
    store: &BinaryIndexStore,
    t: &crate::ir::triple::Term,
) -> Result<Option<u64>> {
    let iri: Arc<str> = match t {
        crate::ir::triple::Term::Sid(sid) => match ctx.decode_sid(sid) {
            Some(i) => Arc::from(i),
            None => return Ok(None),
        },
        crate::ir::triple::Term::Iri(i) => Arc::clone(i),
        _ => return Ok(None),
    };
    let sid = store.encode_iri(iri.as_ref());
    store
        .find_subject_id_by_parts(sid.namespace_code, &sid.name)
        .map_err(|e| QueryError::execution(format!("find_subject_id_by_parts: {e}")))
}

/// Check whether the execution context allows fast-path operators.
///
/// Fast paths are only valid when single-ledger, no `from_t`, root (or no)
/// policy, and no uncommitted overlay. History mode is filtered at the
/// planner level (in `execute::operator_tree::build_operator_tree_inner`),
/// so this runtime gate doesn't repeat that check.
#[inline]
fn allow_fast_path(ctx: &ExecutionContext<'_>) -> bool {
    // Fast paths rely on a single binary index + single-ledger semantics for encoded IDs.
    // Dataset (multi-ledger) execution can span multiple ledgers/graphs, so disable fast
    // paths for correctness unless/until they are made dataset-aware.
    !ctx.is_multi_ledger()
        && ctx.from_t.is_none()
        && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
        && ctx
            .overlay
            .map(fluree_db_core::OverlayProvider::epoch)
            .unwrap_or(0)
            == 0
}

/// Combined fast-path eligibility: [`allow_fast_path`] + binary store present + `to_t == max_t`.
///
/// Returns the store reference if the fast path can proceed, `None` otherwise.
#[inline]
pub fn fast_path_store<'a>(ctx: &'a ExecutionContext<'_>) -> Option<&'a Arc<BinaryIndexStore>> {
    if !allow_fast_path(ctx) {
        return None;
    }
    let store = ctx.binary_store.as_ref()?;
    if ctx.to_t != store.max_t() {
        return None;
    }
    Some(store)
}

// ---------------------------------------------------------------------------
// 11. Generic fast-path operator
// ---------------------------------------------------------------------------

/// Generic fast-path operator that eliminates per-operator boilerplate.
///
/// Each fast-path file provides a constructor function that captures domain-specific
/// data into a closure. `FastPathOperator` handles all lifecycle plumbing:
/// state transitions, fallback delegation, and single-batch yielding.
///
/// The `compute` closure is called once during `open()`:
/// - `Ok(Some(batch))` → fast path succeeded; that batch is yielded then exhausted
/// - `Ok(None)` → fall through to the fallback operator tree
/// - `Err(_)` → propagated as-is
// Boxed closure that computes the fast-path result during `open()`.
type FastPathCompute =
    Box<dyn FnOnce(&ExecutionContext<'_>) -> Result<Option<Batch>> + Send + Sync>;

pub struct FastPathOperator {
    schema: FastPathSchema,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    compute: Option<FastPathCompute>,
    label: &'static str,
}

enum FastPathSchema {
    Single(VarId),
    Multi(Arc<[VarId]>),
}

impl FastPathOperator {
    /// Create a fast-path operator with a single output variable.
    pub fn new(
        out_var: VarId,
        compute: impl FnOnce(&ExecutionContext<'_>) -> Result<Option<Batch>> + Send + Sync + 'static,
        fallback: Option<BoxedOperator>,
        label: &'static str,
    ) -> Self {
        Self {
            schema: FastPathSchema::Single(out_var),
            state: OperatorState::Created,
            fallback,
            compute: Some(Box::new(compute)),
            label,
        }
    }

    /// Create a fast-path operator with a multi-variable output schema.
    pub fn with_schema(
        schema: Arc<[VarId]>,
        compute: impl FnOnce(&ExecutionContext<'_>) -> Result<Option<Batch>> + Send + Sync + 'static,
        fallback: Option<BoxedOperator>,
        label: &'static str,
    ) -> Self {
        Self {
            schema: FastPathSchema::Multi(schema),
            state: OperatorState::Created,
            fallback,
            compute: Some(Box::new(compute)),
            label,
        }
    }
}

#[async_trait]
impl Operator for FastPathOperator {
    fn schema(&self) -> &[VarId] {
        match &self.schema {
            FastPathSchema::Single(v) => std::slice::from_ref(v),
            FastPathSchema::Multi(v) => v,
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        if let Some(compute) = self.compute.take() {
            if let Some(batch) = compute(ctx)? {
                self.state = OperatorState::Open;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(batch)));
                return Ok(());
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(format!(
                "{} fast-path unavailable and no fallback provided",
                self.label
            )));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let Some(fallback) = &mut self.fallback else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        let b = fallback.next_batch(ctx).await?;
        if b.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(b)
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
    }
}
