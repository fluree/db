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
use std::collections::{HashMap, HashSet};
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

    let s_id_set: HashSet<u64> = sorted_subjects.iter().copied().collect();
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

        while let Some(batch) = cursor.next_batch()? {
            scanned_batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            scanned_rows.fetch_add(batch.row_count as u64, std::sync::atomic::Ordering::Relaxed);
            for i in 0..batch.row_count {
                let s_id = batch.s_id.get(i);
                if !s_id_set.contains(&s_id) {
                    continue;
                }
                let ot = batch.o_type.get_or(i, 0);
                if ot != iri_ref {
                    continue;
                }
                out.entry(s_id).or_default().push(batch.o_key.get(i));
            }
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

/// Break sorted subjects into chunks where each chunk spans at most
/// `max_span` IDs and contains at most `max_chunk` subjects.
fn chunk_subjects(sorted: &[u64], max_span: u64, max_chunk: usize) -> Vec<&[u64]> {
    if sorted.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    for i in 1..sorted.len() {
        let span = sorted[i] - sorted[start];
        let size = i - start;
        if span > max_span || size >= max_chunk {
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
