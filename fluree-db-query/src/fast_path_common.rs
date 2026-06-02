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

/// Projection that loads SId + OKey + OI columns (internal, not output).
///
/// `OI` (the value/list index) is part of the V3 fact identity
/// `(s_id, p_id, o_type, o_key, o_i)`, so paths that dedup or retract by fact
/// identity (e.g. the overlay-merge tail scan) must carry it.
#[inline]
pub fn projection_sid_okey_oi() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OKey);
            s.insert(ColumnId::OI);
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

/// Projection for `BinaryCursor` that outputs OType + OKey columns.
///
/// Object-only counterpart of [`cursor_projection_sid_otype_okey`] for paths
/// that fold over object values without needing the subject (e.g. POST-ordered
/// scalar aggregates). Columns are in `output` as required by `BinaryCursor`.
#[inline]
pub fn cursor_projection_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
        internal: ColumnSet::EMPTY,
    }
}

/// True when an object `o_type` is **order-preserving in `o_key`**: the raw
/// `u64` `o_key` byte order equals the value's semantic order, so a scan of a
/// single such `o_type` yields rows in value order.
///
/// This is the gate for the reverse-POST `ORDER BY DESC(?o) LIMIT k` fast path
/// (see [`crate::fast_post_order_limit`]). It admits the embedded numeric,
/// temporal, and boolean types whose encodings are documented order-preserving
/// in `fluree_db_core::value_id`, and EXCLUDES:
/// - dict-backed strings/IRIs (`LEX_ID`/`IRI_REF`, tag `10`): ids are assigned
///   by insertion order, not lexicographic value order;
/// - lang strings (tag `11`);
/// - `GEO_POINT` (packed lat/long — not a linear value order) and `BLANK_NODE`;
/// - overflow big numerics / JSON / vector arena handles (equality-only).
///
/// Within one `o_type`, this equals the SPARQL `ORDER BY` order; mixing
/// `o_type`s under one predicate is rejected by the operator at runtime.
#[inline]
pub const fn is_post_desc_orderable(o_type: u16) -> bool {
    let ot = OType::from_u16(o_type);
    // XSD_BOOLEAN (0x0002), the signed/unsigned/constrained integers and floats
    // (is_numeric: 0x0003..=0x0012), and the temporal + duration range
    // (is_temporal: XSD_DATE 0x0013..=XSD_DURATION 0x001D). Excludes GEO_POINT
    // (0x001E), BLANK_NODE (0x001F), and every dict-backed/lang/arena type.
    o_type == OType::XSD_BOOLEAN.as_u16() || ot.is_numeric() || ot.is_temporal()
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

/// Minimum total predicate rows before a parallel leaf-chunk scan is worth its
/// thread-spawn overhead; below this the reducer runs serially on the whole slice.
const PARALLEL_LEAF_SCAN_MIN_ROWS: u64 = 50_000;
/// Cap on leaf-chunk worker count regardless of core count.
const PARALLEL_LEAF_SCAN_MAX_CHUNKS: usize = 16;

/// Run `f` over `items` on the shared global rayon thread pool, preserving order and
/// the current tracing span, returning each item's result.
///
/// This is the multi-tenant-safe replacement for a per-call `std::thread::scope`.
/// A `scope` spawns fresh OS threads on every invocation, so under concurrent query
/// load N queries each running a partitioned count/scan spawn up to N×K worker
/// threads — thrashing a multi-tenant server's scheduler and memory. The global
/// rayon pool is sized once (≈ logical cores) and shared across every query, so the
/// total worker-thread count stays bounded no matter how many queries run at once
/// (matching the pool `fluree-db-novelty` already uses). Like `thread::scope` — and
/// unlike `thread::spawn` / `spawn_blocking` — rayon's parallel iterator is fully
/// structured: it blocks until every task finishes, so `f` may borrow non-`'static`
/// data (the store, the reducer) exactly as the old scoped threads did. A panic in a
/// worker is converted to an error for that item instead of unwinding the query.
pub(crate) fn parallel_map_pooled<T, R, F>(items: Vec<T>, f: F) -> Vec<Result<R>>
where
    T: Send,
    R: Send,
    F: Fn(T) -> Result<R> + Sync + Send,
{
    use rayon::prelude::*;
    let span = tracing::Span::current();
    items
        .into_par_iter()
        .map(|item| {
            let _guard = span.enter();
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(item))) {
                Ok(result) => result,
                Err(_) => Err(QueryError::execution("parallel worker panicked")),
            }
        })
        .collect()
}

/// Partition a predicate's `leaves` into up to `PARALLEL_LEAF_SCAN_MAX_CHUNKS`
/// contiguous chunks (~one per core) and run `reducer(chunk)` per chunk on the
/// shared global rayon pool (via [`parallel_map_pooled`]), summing the partials.
///
/// Every row lives in exactly one leaflet of one leaf, so counting rows per chunk
/// and summing is exact for ANY index order — unlike the per-subject
/// [`crate::count_plan_exec::parallel_partition_count`] (which must partition on
/// subject boundaries because a subject's rows span leaves), this counts
/// independent rows and so can split purely on leaf index. `reducer` returns
/// `Ok(None)` to signal the whole count must defer to the general pipeline (an
/// unsupported shape, e.g. a non-numeric leaflet); any `None` short-circuits the
/// result to `Ok(None)`.
///
/// When there aren't enough rows/leaves/cores to be worth parallelizing, runs
/// `reducer` once on the whole slice (identical to a serial scan). BASE index only:
/// the caller must reach here via [`fast_path_store`] (HEAD, no overlay), so the
/// base leaflets already reflect current state.
pub fn parallel_leaf_chunk_count<F>(
    leaves: &[LeafEntry],
    total_rows: u64,
    reducer: F,
) -> Result<Option<u64>>
where
    F: Fn(&[LeafEntry]) -> Result<Option<u64>> + Sync + Send,
{
    let ncpu = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let k = ncpu.min(PARALLEL_LEAF_SCAN_MAX_CHUNKS).min(leaves.len());
    if ncpu < 2 || total_rows < PARALLEL_LEAF_SCAN_MIN_ROWS || k < 2 {
        // Not worth parallelizing: run the reducer serially over the whole slice.
        return reducer(leaves);
    }

    // Contiguous, near-equal leaf chunks (`chunks()` yields ceil(len/per) slices).
    let per = leaves.len().div_ceil(k);
    let chunks: Vec<&[LeafEntry]> = leaves.chunks(per).collect();
    tracing::debug!(
        chunks = chunks.len(),
        leaves = leaves.len(),
        total_rows,
        "fast-path: parallel leaf-chunk scan"
    );

    let partials: Vec<Result<Option<u64>>> = parallel_map_pooled(chunks, reducer);

    let mut total: u64 = 0;
    for partial in partials {
        match partial? {
            Some(n) => total = total.saturating_add(n),
            None => return Ok(None),
        }
    }
    Ok(Some(total))
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

/// Count total rows for a predicate from the PSOT branch manifest.
///
/// This is the fastest possible implementation of:
/// `SELECT (COUNT(*) AS ?c) WHERE { ?s <p> ?o }`
/// (and also `COUNT(?s)` / `COUNT(?o)` for the same single-triple pattern),
/// because every solution binding has all vars bound.
///
/// A leaf whose `first_key` and `last_key` both belong to `p_id` is *interior* to
/// the predicate: PSOT order (p_id, s_id, …) means every row in it is `p_id`, so
/// `LeafEntry.row_count` (the manifest's latest-state row count, which equals the
/// sum of that leaf's leaflet `row_count`s) IS the predicate's contribution — no
/// leaf open. Only the at-most-two *boundary* leaves (where the predicate range
/// starts or ends mid-leaf, mixing predicates) need a directory walk. For a large
/// predicate (e.g. `rdf:type`, thousands of leaves) this turns thousands of leaf
/// opens into ~two.
pub fn count_rows_for_predicate_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<u64> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        // Interior leaf: entirely this predicate → use the manifest count, no open.
        if leaf_entry.first_key.p_id == p_id && leaf_entry.last_key.p_id == p_id {
            total += leaf_entry.row_count;
            continue;
        }
        // Boundary leaf: may mix predicates → open and sum the matching leaflets.
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
    /// Half-open subject range `[lo, hi)` this iterator emits. Subjects below
    /// `lo` are skipped; the first subject `>= hi` ends the stream. Used to
    /// partition one predicate's subjects across parallel workers.
    lo: u64,
    hi: u64,
}

impl<'a> PsotSubjectCountIter<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Self> {
        Self::new_bounded(store, g_id, p_id, 0, u64::MAX)
    }

    /// Iterate only the subjects in `[lo, hi)`. Leaves entirely below `lo` are
    /// skipped via a binary search on the predicate's leaf slice (so a partition
    /// only opens its own leaves), and iteration ends at the first subject `>= hi`.
    pub fn new_bounded(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        lo: u64,
        hi: u64,
    ) -> Result<Self> {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        // Leaf leapfrog to `lo`: skip leaves whose last subject (for THIS predicate)
        // is below `lo`. Guarded by `last_key.p_id == p_id` because a boundary
        // leaf's `last_key` can belong to a higher predicate (see PsotSubjectSeek).
        let leaf_pos = leaves.partition_point(|e| e.last_key.p_id == p_id && e.last_key.s_id.as_u64() < lo);
        Ok(Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_s: None,
            cur_count: 0,
            lo,
            hi,
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

            // Below the partition's range: skip whole sub-`lo` subjects. Only
            // possible before the first in-range subject (rows are subject-sorted),
            // so `cur_s` is None here.
            if sid < self.lo {
                self.row += 1;
                continue;
            }
            // At/above the partition's end: no more in-range subjects (sorted), so
            // flush any accumulated group and end the stream. The `>= hi` row is
            // left unconsumed (it belongs to the next partition).
            if sid >= self.hi {
                if let Some(s) = self.cur_s.take() {
                    let n = std::mem::take(&mut self.cur_count);
                    return Ok(Some((s, n)));
                }
                return Ok(None);
            }

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
// 7a-seek. Forward-only per-subject PSOT seek (asymmetric-join probe)
// ---------------------------------------------------------------------------

/// Forward-only monotonic per-subject seek over a predicate's PSOT leaves.
///
/// Given **strictly non-decreasing** target subjects, returns the row count for
/// each (any object datatype) — the probe side of an asymmetric subject join
/// (drive from the small predicate, seek into the large one). Cost is driven
/// sub-linear in the probe predicate two ways:
/// - **leaf leapfrog**: a binary search over the predicate's leaf slice skips
///   whole leaves — and their blob reads — that cannot contain the target;
/// - **leaflet skip**: leaflets whose `last_key` subject is below the target are
///   skipped without decoding their columns.
///
/// Subjects whose rows span leaflet/leaf boundaries are handled. Because the
/// cursor only ever advances, the total work across an ascending target sequence
/// is one monotonic pass.
///
/// PRECONDITION: targets must be non-decreasing across calls (the cursor never
/// rewinds). BASE index only — callers MUST gate to HEAD (no overlay,
/// `to_t == max_t`); novelty / time-travel are not merged here.
pub struct PsotSubjectSeek<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaves: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
}

impl<'a> PsotSubjectSeek<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Self {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Self {
            store,
            p_id,
            leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        }
    }

    fn load_next_batch(&mut self, target_s: u64) -> Result<Option<()>> {
        use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
        loop {
            if self.handle.is_none() {
                // Leaf leapfrog: skip leaves that provably cannot contain target_s.
                //
                // A leaf is skippable only when its `last_key` belongs to THIS
                // predicate AND its subject is below the target. At the predicate's
                // upper boundary a leaf's `last_key` may belong to a higher predicate
                // (our rows are a prefix of that leaf); such a leaf must NOT be
                // skipped by its foreign subject. `leaf_entries_for_predicate`
                // guarantees `last_key.p_id >= self.p_id`, so the skip predicate is
                // a monotone leading run and `partition_point` is valid.
                let skip = self.leaves[self.leaf_pos..].partition_point(|e| {
                    e.last_key.p_id == self.p_id && e.last_key.s_id.as_u64() < target_s
                });
                self.leaf_pos += skip;
                if self.leaf_pos >= self.leaves.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaves[self.leaf_pos];
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
                // Leaflet skip: cannot contain target_s if its last subject is below it.
                let last = read_ordered_key_v2(RunSortOrder::Psot, &entry.last_key);
                if last.s_id.as_u64() < target_s {
                    continue;
                }
                let projection = projection_sid_only();
                let batch = if let Some(cache) = self.store.leaflet_cache() {
                    let idx_u32: u32 = idx.try_into().map_err(|_| {
                        QueryError::Internal("leaflet idx exceeds u32".to_string())
                    })?;
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

    /// Row count for `target_s` (any object datatype), or `None` if the subject is
    /// absent. Targets MUST be non-decreasing across calls.
    pub fn count_for_subject(&mut self, target_s: u64) -> Result<Option<u64>> {
        let mut found = false;
        let mut count: u64 = 0;
        loop {
            if self.batch.is_none() && self.load_next_batch(target_s)?.is_none() {
                return Ok(found.then_some(count));
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            if !found {
                // Advance to the first row with s_id >= target_s. Within a leaflet
                // s_id is sorted, so binary-search the unconsumed suffix instead of
                // skipping row-by-row — cheap when the target is far in, e.g. when a
                // sparse driver seeks past a high-multiplicity subject's run.
                let (mut lo, mut hi) = (self.row, batch.row_count);
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if batch.s_id.get(mid) < target_s {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                self.row = lo;
                if self.row >= batch.row_count {
                    self.batch = None;
                    continue;
                }
                if batch.s_id.get(self.row) > target_s {
                    // Cursor is now parked at the first subject above target_s, ready
                    // for the next (larger) target. Subject is absent.
                    return Ok(None);
                }
                found = true;
            } else if batch.s_id.get(self.row) > target_s {
                return Ok(Some(count));
            }
            // At target_s: count its rows (the group may span batches).
            while self.row < batch.row_count && batch.s_id.get(self.row) == target_s {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| QueryError::execution("COUNT(*) overflow in subject seek"))?;
                self.row += 1;
            }
            if self.row < batch.row_count {
                return Ok(Some(count));
            }
            // Subject group may continue into the next leaflet/leaf.
            self.batch = None;
        }
    }

    /// Whether `target_s` has any row. Targets MUST be non-decreasing across calls.
    pub fn subject_present(&mut self, target_s: u64) -> Result<bool> {
        Ok(self.count_for_subject(target_s)?.is_some())
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

/// Construct a `BinaryCursor` over an explicit key range + filter in `order`,
/// without overlay merging or `to_t` filtering.
///
/// Lowest-level shared cursor constructor: looks up the branch for `order` and,
/// on success, builds the cursor. Callers are responsible for `set_to_t` (and
/// any overlay wiring) afterwards. Returns `None` when the branch for `order`
/// does not exist for `g_id`.
///
/// Used directly by object-/subject-range-bounded fast paths
/// (`fast_string_prefix_count_all`, `fast_star_const_order_topk`) and as the
/// base of [`build_overlay_cursor_for_predicate`].
pub fn build_range_cursor(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    order: RunSortOrder,
    min_key: &RunRecordV2,
    max_key: &RunRecordV2,
    filter: BinaryFilter,
    projection: ColumnProjection,
) -> Option<BinaryCursor> {
    let branch = Arc::clone(store.branch_for_order(g_id, order)?);
    Some(BinaryCursor::new(
        Arc::clone(store),
        order,
        branch,
        min_key,
        max_key,
        filter,
        projection,
    ))
}

/// Collect the novelty-overlay ops for a single predicate, translated into the
/// binary-index `OverlayOp` representation and sorted/resolved for `order`.
///
/// Returns `Ok(Some(ops))` on success (`ops` may be empty), or `Ok(None)` when
/// any flake fails to translate — in which case the caller must disable the
/// fast path for correctness. Only meaningful when an overlay carrying novelty
/// is present (`epoch != 0`).
pub fn collect_resolved_overlay_ops(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    order: RunSortOrder,
    pred_sid: &Sid,
) -> Result<Option<Vec<fluree_db_binary_index::read::types::OverlayOp>>> {
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
        crate::binary_scan::sort_order_to_index_type(order),
        None,
        None,
        true,
        ctx.to_t,
        &mut |flake| {
            if flake.p != *pred_sid {
                return;
            }
            match crate::binary_scan::translate_one_flake_v3_pub(
                flake,
                store,
                Some(&dn),
                ctx.runtime_small_dicts,
                &mut ephemeral_preds,
                &mut next_ep,
                g_id,
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
        fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
        fluree_db_binary_index::read::types::resolve_overlay_ops(&mut ops);
    }
    Ok(Some(ops))
}

/// Build a per-predicate `BinaryCursor` in `order`, folding in the novelty
/// overlay and honoring `to_t`.
///
/// `order` must be a *predicate-bounded* order — [`RunSortOrder::Psot`] or
/// [`RunSortOrder::Post`] — because both place `p_id` first in their key, so
/// [`predicate_range_keys`] bounds the scan to one predicate. `Opst` is
/// object-keyed (its `p_id` is not a primary key component) and is intentionally
/// unsupported here; object-ordered cursors must be range-bounded by object
/// instead (see `fast_string_prefix_count_all::count_prefix_rows_opst`).
///
/// Unlike the raw leaf-entry scans, this folds uncommitted overlay flakes into
/// the cursor, so it stays correct when `ctx.overlay` carries novelty or when
/// `ctx.to_t < max_t` — operators using it should gate on
/// [`allow_cursor_fast_path`], not [`fast_path_store`]. Returns `None` if the
/// branch for `order` is absent, or if an overlay flake fails to translate (fast
/// path disabled for correctness).
pub fn build_overlay_cursor_for_predicate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    order: RunSortOrder,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    debug_assert!(
        matches!(order, RunSortOrder::Psot | RunSortOrder::Post),
        "build_overlay_cursor_for_predicate requires a p_id-primary order \
         (Psot or Post); got {order:?}"
    );

    // When an overlay merge can run, `merge_overlay_into_batch` needs the full
    // V3 identity (s_id, p_id, o_type, o_key, o_i) on every base row. Narrow
    // count projections (SId-only, OType+OKey) omit some, which would read as
    // `AbsentDefault` and corrupt the identity compare. Force the identity
    // columns into `internal` so they're loaded for the merge but dropped before
    // the returned batch — the output shape the count operators see is
    // unchanged. (Production masks this via the cache's `all()` load; a
    // cache-less store would miscount. See `BinaryCursor::set_overlay_ops`.)
    let overlay_active = ctx.overlay.is_some() && ctx.overlay().epoch() != 0;
    let projection = if overlay_active {
        let identity = ColumnSet::CORE.union(ColumnSet::single(ColumnId::OI));
        ColumnProjection {
            output: projection.output,
            // Don't duplicate columns already materialized in `output`.
            internal: ColumnSet(projection.internal.union(identity).0 & !projection.output.0),
        }
    } else {
        projection
    };

    let (min_key, max_key) = predicate_range_keys(p_id, g_id);
    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };
    let Some(mut cursor) =
        build_range_cursor(store, g_id, order, &min_key, &max_key, filter, projection)
    else {
        return Ok(None);
    };
    cursor.set_to_t(ctx.to_t);

    // Fold the novelty overlay in. Skip the walk entirely when there is no
    // novelty (epoch 0): the persisted index alone is then exact.
    if ctx.overlay.is_some() {
        let epoch = ctx.overlay().epoch();
        if epoch != 0 {
            match collect_resolved_overlay_ops(ctx, store, g_id, order, &pred_sid)? {
                Some(ops) => {
                    if !ops.is_empty() {
                        cursor.set_overlay_ops(ops);
                    }
                }
                None => return Ok(None),
            }
        }
        cursor.set_epoch(epoch);
    }

    Ok(Some(cursor))
}

/// Build a per-predicate PSOT overlay cursor (subject-ordered within the
/// predicate). Thin wrapper over [`build_overlay_cursor_for_predicate`].
#[inline]
pub fn build_psot_cursor_for_predicate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    build_overlay_cursor_for_predicate(
        ctx,
        store,
        g_id,
        RunSortOrder::Psot,
        pred_sid,
        p_id,
        projection,
    )
}

/// Build a per-predicate POST overlay cursor (object-ordered within the
/// predicate). Thin wrapper over [`build_overlay_cursor_for_predicate`].
///
/// POST groups rows by `(o_type, o_key)` within the predicate, so adjacent rows
/// share an object — the natural shape for object-folding aggregates and
/// distinct-object counts that must stay overlay-correct.
#[inline]
pub fn build_post_cursor_for_predicate(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    pred_sid: Sid,
    p_id: u32,
    projection: ColumnProjection,
) -> Result<Option<BinaryCursor>> {
    build_overlay_cursor_for_predicate(
        ctx,
        store,
        g_id,
        RunSortOrder::Post,
        pred_sid,
        p_id,
        projection,
    )
}

/// Streams `(s_id, edge_count)` groups from an overlay-merging PSOT cursor.
///
/// The cursor yields rows in PSOT order, so a running group-by on `s_id`
/// produces the same `(subject, count)` pairs that the metadata
/// [`PsotSubjectCountIter`] derives from leaflet headers — but over the
/// novelty-merged row stream. Shared by `fast_union_star_count_all` and the
/// `count_plan_exec` overlay lane.
///
/// Use `cursor_projection_sid_only()` (or any projection that includes `s_id`)
/// when building the cursor.
pub struct CursorSubjectCountStream {
    cursor: BinaryCursor,
    current: Option<ColumnBatch>,
    row: usize,
    cur_s: Option<u64>,
    cur_count: u64,
}

impl CursorSubjectCountStream {
    pub fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
            cur_s: None,
            cur_count: 0,
        }
    }

    /// Next `(subject_id, row_count)` group, or `None` when exhausted.
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.current.is_none() {
                self.current = self
                    .cursor
                    .next_batch()
                    .map_err(|e| QueryError::Internal(format!("cursor batch: {e}")))?;
                self.row = 0;
                if self.current.is_none() {
                    if let Some(s) = self.cur_s.take() {
                        let n = std::mem::take(&mut self.cur_count);
                        return Ok(Some((s, n)));
                    }
                    return Ok(None);
                }
            }

            let batch = self.current.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.current = None;
                continue;
            }
            let s = batch.s_id.get(self.row);
            if self.cur_s.is_none() {
                self.cur_s = Some(s);
                self.cur_count = 0;
            } else if self.cur_s != Some(s) {
                let out_s = self.cur_s.replace(s).unwrap();
                let out_n = std::mem::replace(&mut self.cur_count, 0);
                // Don't advance row; reprocess it into the new group.
                return Ok(Some((out_s, out_n)));
            }
            self.cur_count += 1;
            self.row += 1;
        }
    }
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

/// Cursor-flavored fast-path gate (strategy (b)).
///
/// Unlike [`allow_fast_path`], this does **not** reject uncommitted overlay or
/// `to_t < max_t`: operators using this gate read through a [`BinaryCursor`]
/// (built by [`build_psot_cursor_for_predicate`]) that folds the novelty overlay
/// in and honors `to_t`, so those cases stay correct. It still requires
/// single-ledger, no `from_t`, and root (or no) policy; History mode is filtered
/// at the planner level.
#[inline]
pub fn allow_cursor_fast_path(ctx: &ExecutionContext<'_>) -> bool {
    !ctx.is_multi_ledger()
        && ctx.from_t.is_none()
        && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
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
