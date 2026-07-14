//! Batched PSOT lookup for ref-valued predicate objects across many subjects.
//!
//! Used by the incremental stats pipeline to efficiently query base-root
//! class membership (rdf:type assertions) for subjects touched by novelty.
//! Instead of one PSOT cursor per subject, this module scans contiguous
//! subject-ID ranges in bulk, filtering to the requested subject set
//! in-memory.
//!
//! Mirrors the legacy `batched-get-subject-classes` strategy.

use super::binary_cursor::BinaryCursor;
use super::binary_index_store::BinaryIndexStore;
use super::column_types::{BinaryFilter, ColumnProjection, ColumnSet};
use crate::format::column_block::ColumnId;
use crate::format::run_record::RunSortOrder;
use crate::format::run_record_v2::RunRecordV2;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::GraphId;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Batched PSOT lookup for ref-valued predicate objects across many subjects.
///
/// Returns `HashMap<sid64_subject, Vec<sid64_class>>` of current assertions
/// from the persisted index at `to_t`. No overlay/novelty merge -- caller
/// applies novelty deltas separately.
///
/// Mirrors the legacy `batched-get-subject-classes` strategy: one streaming
/// pass over PSOT bounded by the subject range, filtering to the requested
/// subject set in-memory.
///
/// For sparse subject ranges, chunks the scan to avoid scanning large gaps.
pub fn batched_lookup_predicate_refs(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    subjects: &[u64],
    to_t: i64,
) -> io::Result<HashMap<u64, Vec<u64>>> {
    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    let started_all = Instant::now();

    let mut sorted_subjects = subjects.to_vec();
    sorted_subjects.sort_unstable();
    sorted_subjects.dedup();

    let mut out: HashMap<u64, Vec<u64>> = HashMap::new();

    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(out);
    };
    let branch = Arc::clone(branch);

    let iri_ref = OType::IRI_REF.as_u16();

    const MAX_SPAN: u64 = 100_000;
    const MAX_CHUNK: usize = 1000;
    let chunks = chunk_subjects(&sorted_subjects, MAX_SPAN, MAX_CHUNK);

    let min_s = *sorted_subjects.first().unwrap_or(&0);
    let max_s = *sorted_subjects.last().unwrap_or(&min_s);
    tracing::debug!(
        g_id,
        p_id,
        subjects = sorted_subjects.len(),
        chunks = chunks.len(),
        min_s_id = min_s,
        max_s_id = max_s,
        span = max_s.saturating_sub(min_s),
        to_t,
        heartbeat_secs = HEARTBEAT_INTERVAL.as_secs(),
        "batched_lookup_predicate_refs: starting"
    );

    // Only need s_id, o_type, o_key columns for class lookup.
    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::SId);
    needed.insert(ColumnId::OType);
    needed.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    let scanned_batches = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let scanned_rows = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let current_chunk_idx = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let subjects_with_hits = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Heartbeat thread: emits progress even if we stall inside cursor.next_batch().
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let hb_scanned_batches = Arc::clone(&scanned_batches);
    let hb_scanned_rows = Arc::clone(&scanned_rows);
    let hb_chunk_idx = Arc::clone(&current_chunk_idx);
    let hb_hits = Arc::clone(&subjects_with_hits);
    let hb_started = started_all;
    let hb = std::thread::spawn(move || loop {
        match stop_rx.recv_timeout(HEARTBEAT_INTERVAL) {
            Ok(()) => return,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let b = hb_scanned_batches.load(std::sync::atomic::Ordering::Relaxed);
                let r = hb_scanned_rows.load(std::sync::atomic::Ordering::Relaxed);
                let c = hb_chunk_idx.load(std::sync::atomic::Ordering::Relaxed);
                let h = hb_hits.load(std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(
                    g_id,
                    p_id,
                    chunk_idx = c,
                    scanned_batches = b,
                    scanned_rows = r,
                    subjects_with_hits = h,
                    elapsed_ms = hb_started.elapsed().as_millis() as u64,
                    "batched_lookup_predicate_refs: heartbeat"
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        }
    });

    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        current_chunk_idx.store(chunk_idx as u64, std::sync::atomic::Ordering::Relaxed);
        let min_s = chunk[0];
        let max_s = *chunk.last().unwrap();

        tracing::debug!(
            g_id,
            p_id,
            chunk_idx,
            chunk_subjects = chunk.len(),
            min_s_id = min_s,
            max_s_id = max_s,
            span = max_s.saturating_sub(min_s),
            "batched_lookup_predicate_refs: scanning chunk"
        );

        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(min_s),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(max_s),
            o_key: u64::MAX,
            p_id,
            t: 0,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };

        let filter = BinaryFilter {
            p_id: Some(p_id),
            ..Default::default()
        };

        let mut cursor = BinaryCursor::new(
            Arc::clone(store),
            RunSortOrder::Psot,
            Arc::clone(&branch),
            &min_key,
            &max_key,
            filter,
            projection,
        );
        cursor.set_to_t(to_t);

        // The cursor's p_id filter pins PSOT's leading key, so s_id is
        // non-decreasing across the returned rows — gallop the chunk's
        // wanted ids instead of testing every row. Spillover rows for a
        // neighboring chunk's subjects are excluded by construction.
        while let Some(batch) = cursor.next_batch()? {
            scanned_batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            scanned_rows.fetch_add(batch.row_count as u64, std::sync::atomic::Ordering::Relaxed);
            for_each_subject_run(&batch, chunk, |s_id, i, batch| {
                let ot = batch.o_type.get_or(i, 0);
                if ot != iri_ref {
                    return;
                }
                out.entry(s_id).or_default().push(batch.o_key.get(i));
            });
            subjects_with_hits.store(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    for classes in out.values_mut() {
        classes.sort_unstable();
        classes.dedup();
    }

    // Stop heartbeat.
    let _ = stop_tx.send(());
    let _ = hb.join();

    tracing::debug!(
        g_id,
        p_id,
        subjects_with_hits = out.len(),
        scanned_batches = scanned_batches.load(std::sync::atomic::Ordering::Relaxed),
        scanned_rows = scanned_rows.load(std::sync::atomic::Ordering::Relaxed),
        elapsed_ms = started_all.elapsed().as_millis() as u64,
        "batched_lookup_predicate_refs: completed"
    );

    Ok(out)
}

/// Per-subject live property flakes from a base-index scan: each subject's
/// sid64 maps to a list of `(p_id, o_type, o_key)` for its current assertions.
pub type SubjectPropertyFlakes = HashMap<u64, Vec<(u32, u16, u64)>>;

/// Batched SPOT lookup of all live property flakes for a set of subjects from
/// the persisted index at `to_t`.
///
/// Returns the current (non-history) property assertions for each requested
/// subject. No overlay/novelty merge; the caller applies novelty deltas
/// separately.
///
/// Used by the incremental class-stat merge to re-attribute the existing
/// properties of a subject whose class membership changed this batch (re-type,
/// add/remove a type, or delete) from the classes it left onto the classes it
/// joined (issue #1266). Mirrors [`batched_lookup_predicate_refs`] but scans
/// SPOT (subject-major) across all predicates instead of one PSOT predicate.
pub fn batched_lookup_subject_properties(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    subjects: &[u64],
    to_t: i64,
) -> io::Result<SubjectPropertyFlakes> {
    let mut out: HashMap<u64, Vec<(u32, u16, u64)>> = HashMap::new();
    if subjects.is_empty() {
        return Ok(out);
    }

    let mut sorted_subjects = subjects.to_vec();
    sorted_subjects.sort_unstable();
    sorted_subjects.dedup();

    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Spot) else {
        return Ok(out);
    };
    let branch = Arc::clone(branch);

    const MAX_SPAN: u64 = 100_000;
    const MAX_CHUNK: usize = 1000;
    let chunks = chunk_subjects(&sorted_subjects, MAX_SPAN, MAX_CHUNK);

    // Need s_id (to filter), p_id, o_type, o_key (to reconstruct datatype/lang/ref).
    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::SId);
    needed.insert(ColumnId::PId);
    needed.insert(ColumnId::OType);
    needed.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    for chunk in &chunks {
        let min_s = chunk[0];
        let max_s = *chunk.last().unwrap();

        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(min_s),
            o_key: 0,
            p_id: 0,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(max_s),
            o_key: u64::MAX,
            p_id: u32::MAX,
            t: 0,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };

        let mut cursor = BinaryCursor::new(
            Arc::clone(store),
            RunSortOrder::Spot,
            Arc::clone(&branch),
            &min_key,
            &max_key,
            BinaryFilter::default(),
            projection,
        );
        cursor.set_to_t(to_t);

        // Batches are leaflet-granular and spill far past the wanted
        // subjects, so testing every row is the dominant cost for small
        // subject sets (a single-node hydration paid a full-leaflet
        // membership scan). SPOT's primary sort key is `s_id`, so gallop
        // instead: binary-search each wanted subject's contiguous run and
        // copy only those rows. Spillover rows for a neighboring chunk's
        // subjects are excluded by construction (only this chunk's ids are
        // searched), preserving the no-double-collect invariant the
        // per-chunk membership set used to provide.
        while let Some(batch) = cursor.next_batch()? {
            for_each_subject_run(&batch, chunk, |s_id, i, batch| {
                let p_id = batch.p_id.get_or(i, 0);
                let o_type = batch.o_type.get_or(i, 0);
                let o_key = batch.o_key.get(i);
                out.entry(s_id).or_default().push((p_id, o_type, o_key));
            });
        }
    }

    Ok(out)
}

/// Visit every row of `batch` whose `s_id` is in the sorted id list
/// `wanted`, in row order, via sorted merge with galloping binary search.
///
/// Requires the batch's `s_id` column to be non-decreasing over the visited
/// range — true for SPOT-order leaflets (subject is the primary sort key),
/// including time-travel replayed ones, and for PSOT batches after the
/// cursor's `p_id` filter pinned the leading key.
fn for_each_subject_run(
    batch: &super::column_types::ColumnBatch,
    wanted: &[u64],
    mut visit: impl FnMut(u64, usize, &super::column_types::ColumnBatch),
) {
    let n = batch.row_count;
    for_each_wanted_run(&batch.s_id, 0, n, wanted, |target, i| {
        visit(target, i, batch);
    });
}

/// Sorted-merge core: visit every row index in `[lo, hi)` whose value in
/// `keys` is in the sorted list `wanted`. `keys` must be non-decreasing
/// over `[lo, hi)`.
fn for_each_wanted_run(
    keys: &super::column_types::ColumnData<u64>,
    lo: usize,
    hi: usize,
    wanted: &[u64],
    mut visit: impl FnMut(u64, usize),
) {
    let mut row = lo;
    let mut w = 0usize;
    while row < hi && w < wanted.len() {
        let target = wanted[w];
        // First row with key >= target (binary search over [row, hi)).
        let (mut a, mut b) = (row, hi);
        while a < b {
            let mid = a + (b - a) / 2;
            if keys.get(mid) < target {
                a = mid + 1;
            } else {
                b = mid;
            }
        }
        row = a;
        if row >= hi {
            break;
        }
        let k = keys.get(row);
        if k > target {
            // No rows for `target` here; skip wanted ids below `k`.
            w += wanted[w..].partition_point(|&x| x < k);
            continue;
        }
        while row < hi && keys.get(row) == target {
            visit(target, row);
            row += 1;
        }
        w += 1;
    }
}

/// The contiguous `[start, end)` row range of `batch` whose `o_type` column
/// equals `ot` (OPST leaflets sort by `(o_type, o_key, ...)`, so the range
/// is well-defined and `o_key` ascends within it).
fn o_type_run(batch: &super::column_types::ColumnBatch, ot: u16) -> (usize, usize) {
    use super::column_types::ColumnData;
    match &batch.o_type {
        ColumnData::Block(arr) => {
            let start = arr.partition_point(|&x| x < ot);
            let end = arr.partition_point(|&x| x <= ot);
            (start, end)
        }
        ColumnData::Const(c) if *c == ot => (0, batch.row_count),
        _ => (0, 0),
    }
}

/// Batched OPST lookup: all inbound `IRI_REF` edges pointing at each requested
/// object, from the persisted index at `to_t`. No overlay/novelty merge.
///
/// Returns `HashMap<object_sid64, Vec<(p_id, subject_sid64)>>`.
///
/// Mirrors [`batched_lookup_predicate_refs`] but scans `RunSortOrder::Opst`
/// (object-major: `(o_type, o_key, o_i, p_id, s_id)`). The scan range pins
/// `o_type = IRI_REF`, so every yielded row is a reference edge — no per-row
/// o_type check is required. Used by the incremental class-stat merge to move a
/// re-typed object's inbound ref-class edges from its old class bucket to its
/// new one (issue #1266).
pub fn batched_lookup_inbound_refs(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    objects: &[u64],
    to_t: i64,
) -> io::Result<HashMap<u64, Vec<(u32, u64)>>> {
    let mut out: HashMap<u64, Vec<(u32, u64)>> = HashMap::new();
    if objects.is_empty() {
        return Ok(out);
    }

    let mut sorted = objects.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Opst) else {
        return Ok(out);
    };
    let branch = Arc::clone(branch);
    let iri_ref = OType::IRI_REF.as_u16();

    const MAX_SPAN: u64 = 100_000;
    const MAX_CHUNK: usize = 1000;
    // chunk_subjects operates on a sorted &[u64] span — identical logic applies
    // to object o_key spans, so reuse it verbatim.
    let chunks = chunk_subjects(&sorted, MAX_SPAN, MAX_CHUNK);

    // Need s_id (inbound subject), p_id, o_key (filter to set), o_type (ref check).
    let mut needed = ColumnSet::EMPTY;
    needed.insert(ColumnId::SId);
    needed.insert(ColumnId::PId);
    needed.insert(ColumnId::OType);
    needed.insert(ColumnId::OKey);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    for chunk in &chunks {
        let min_o = chunk[0];
        let max_o = *chunk.last().unwrap();

        // OPST order = (o_type, o_key, o_i, p_id, s_id); pin o_type = IRI_REF.
        let min_key = RunRecordV2 {
            s_id: SubjectId::from_u64(0),
            o_key: min_o,
            p_id: 0,
            t: 0,
            o_i: 0,
            o_type: iri_ref,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId::from_u64(u64::MAX),
            o_key: max_o,
            p_id: u32::MAX,
            t: 0,
            o_i: u32::MAX,
            o_type: iri_ref,
            g_id,
        };

        let mut cursor = BinaryCursor::new(
            Arc::clone(store),
            RunSortOrder::Opst,
            Arc::clone(&branch),
            &min_key,
            &max_key,
            BinaryFilter::default(),
            projection,
        );
        cursor.set_to_t(to_t);

        // OPST sorts by (o_type, o_key, ...): binary-search the IRI_REF
        // o_type run (boundary leaflets can carry neighboring o_types),
        // then gallop the chunk's wanted o_keys within it. Spillover rows
        // for a neighboring chunk's objects are excluded by construction.
        while let Some(batch) = cursor.next_batch()? {
            let (lo, hi) = o_type_run(&batch, iri_ref);
            for_each_wanted_run(&batch.o_key, lo, hi, chunk, |o_key, i| {
                out.entry(o_key)
                    .or_default()
                    .push((batch.p_id.get_or(i, 0), batch.s_id.get(i)));
            });
        }
    }

    for v in out.values_mut() {
        v.sort_unstable();
        v.dedup();
    }
    Ok(out)
}

/// Break sorted subjects into chunks where each chunk spans at most
/// `max_span` IDs and contains at most `max_chunk` subjects.
fn chunk_subjects(sorted: &[u64], max_span: u64, max_chunk: usize) -> Vec<&[u64]> {
    // Gap threshold: split only when the id gap is wide enough that the
    // leaflets it crosses cost more to visit than a fresh cursor descent.
    // With shared leaf mmaps and cached leaflet decodes, crossing a leaflet
    // that holds none of the wanted ids costs ~3 µs (decode-cache hit +
    // galloped no-op) and a descent ~4 µs; at ~1k subjects per leaflet the
    // break-even gap is a few thousand ids. An aggressive threshold (64)
    // measurably hurt dense-ish sets (node-emit hydration ~2x: ~8 µs
    // descent per near-point-seek, nothing saved); no threshold at all makes a
    // scattered BFS frontier sweep every leaflet between its members.
    const MAX_GAP: u64 = 4096;

    if sorted.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    for i in 1..sorted.len() {
        let span = sorted[i] - sorted[start];
        let gap = sorted[i] - sorted[i - 1];
        let size = i - start;
        if span > max_span || gap > MAX_GAP || size >= max_chunk {
            chunks.push(&sorted[start..i]);
            start = i;
        }
    }
    chunks.push(&sorted[start..]);
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_subjects_empty() {
        let result = chunk_subjects(&[], 100, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn chunk_subjects_single() {
        let subjects = [42];
        let result = chunk_subjects(&subjects, 100, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], &[42]);
    }

    #[test]
    fn chunk_subjects_dense() {
        let subjects: Vec<u64> = (100..110).collect();
        let result = chunk_subjects(&subjects, 100, 1000);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);
    }

    #[test]
    fn chunk_subjects_sparse() {
        // Two clusters far apart
        let mut subjects = vec![100, 101, 102, 200_000, 200_001, 200_002];
        subjects.sort_unstable();
        let result = chunk_subjects(&subjects, 1_000, 1000);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], &[100, 101, 102]);
        assert_eq!(result[1], &[200_000, 200_001, 200_002]);
    }

    #[test]
    fn chunk_subjects_max_chunk_size() {
        // 5 subjects, max_chunk=2
        let subjects: Vec<u64> = (0..5).collect();
        let result = chunk_subjects(&subjects, u64::MAX, 2);
        assert_eq!(result.len(), 3); // [0,1], [2,3], [4]
        assert_eq!(result[0], &[0, 1]);
        assert_eq!(result[1], &[2, 3]);
        assert_eq!(result[2], &[4]);
    }
}
