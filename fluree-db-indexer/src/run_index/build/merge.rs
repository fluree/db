//! V2 k-way merge engine for `RunRecordV2` streams.
//!
//! Same min-heap architecture as V1 `KWayMerge`, but typed for `RunRecordV2`
//! with V2 identity semantics: `(s_id, p_id, o_type, o_key, o_i)`.
//! No conditional `lang_id`/`i` logic — identity is always the same five fields.

use crate::run_index::runs::streaming_reader::MergeSource;
use fluree_db_binary_index::format::run_record_v2::{same_identity_v2, RunRecordV2};
use std::cmp::Ordering;
use std::io;

struct HeapEntry {
    record: RunRecordV2,
    op: u8,
    stream_idx: usize,
}

/// K-way merge for V2 run record streams.
///
/// Records are dequeued in sort order determined by the comparator `F`.
/// Deduplication uses `same_identity_v2` (always the same 5 fields).
pub struct KWayMerge<T: MergeSource, F: Fn(&RunRecordV2, &RunRecordV2) -> Ordering> {
    heap: Vec<HeapEntry>,
    streams: Vec<T>,
    cmp: F,
}

impl<T: MergeSource, F: Fn(&RunRecordV2, &RunRecordV2) -> Ordering> KWayMerge<T, F> {
    /// Create a new k-way merge from the given streams.
    pub fn new(streams: Vec<T>, cmp: F) -> io::Result<Self> {
        let mut heap = Vec::with_capacity(streams.len());
        for (idx, stream) in streams.iter().enumerate() {
            if let Some(rec) = stream.peek() {
                heap.push(HeapEntry {
                    record: *rec,
                    op: stream.peek_op(),
                    stream_idx: idx,
                });
            }
        }

        let mut merge = Self { heap, streams, cmp };

        // Build heap from bottom up.
        if merge.heap.len() > 1 {
            let last_internal = merge.heap.len() / 2;
            for i in (0..last_internal).rev() {
                merge.sift_down(i);
            }
        }

        Ok(merge)
    }

    #[inline]
    fn heap_less(&self, i: usize, j: usize) -> bool {
        let ord = (self.cmp)(&self.heap[i].record, &self.heap[j].record);
        match ord {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => self.heap[i].stream_idx < self.heap[j].stream_idx,
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        let len = self.heap.len();
        loop {
            let left = 2 * pos + 1;
            if left >= len {
                break;
            }
            let right = left + 1;
            let smallest = if right < len && self.heap_less(right, left) {
                right
            } else {
                left
            };
            if self.heap_less(pos, smallest) {
                break;
            }
            self.heap.swap(pos, smallest);
            pos = smallest;
        }
    }

    /// Dequeue the next record in sort order (no dedup).
    ///
    /// Returns `(record, op)` where `op` is the operation byte
    /// (`1` = assert, `0` = retract). For import-path sources that
    /// carry no op, `op` defaults to `1`.
    pub fn next_record(&mut self) -> io::Result<Option<(RunRecordV2, u8)>> {
        if self.heap.is_empty() {
            return Ok(None);
        }

        let winner = self.heap[0].record;
        let winner_op = self.heap[0].op;
        let stream_idx = self.heap[0].stream_idx;

        // Advance the winning stream.
        self.streams[stream_idx].advance()?;

        if let Some(next) = self.streams[stream_idx].peek() {
            self.heap[0] = HeapEntry {
                record: *next,
                op: self.streams[stream_idx].peek_op(),
                stream_idx,
            };
            self.sift_down(0);
        } else {
            // Stream exhausted — remove from heap.
            let last = self.heap.len() - 1;
            self.heap.swap(0, last);
            self.heap.pop();
            if !self.heap.is_empty() {
                self.sift_down(0);
            }
        }

        Ok(Some((winner, winner_op)))
    }

    /// Dequeue the next record with deduplication.
    ///
    /// When multiple records share the same identity, keeps the one with
    /// the highest `t` (merge tie-breaking). Non-winners are discarded.
    /// The op byte of the winning record is preserved.
    ///
    /// For the import-only milestone, dedup is rarely needed (each fact
    /// appears once). This is kept for correctness in chunk-overlap edge cases.
    pub fn next_deduped(&mut self) -> io::Result<Option<(RunRecordV2, u8)>> {
        let (mut winner, mut winner_op) = match self.next_record()? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        // Consume consecutive duplicates.
        while let Some(peeked) = self.heap.first().map(|e| &e.record) {
            if !same_identity_v2(&winner, peeked) {
                break;
            }
            let (dup, dup_op) = self.next_record()?.unwrap();
            if dup.t > winner.t {
                winner = dup;
                winner_op = dup_op;
            }
        }

        Ok(Some((winner, winner_op)))
    }

    /// Like `next_deduped`, but also returns non-winning entries as history.
    ///
    /// Returns `(winner, winner_op, history)` where `history` contains all
    /// non-winning duplicates (same identity, lower t) as `(RunRecordV2, u8)` pairs.
    /// These are the entries that should become history sidecar entries.
    #[allow(clippy::type_complexity)]
    pub fn next_deduped_with_history(
        &mut self,
    ) -> io::Result<Option<(RunRecordV2, u8, Vec<(RunRecordV2, u8)>)>> {
        let (mut winner, mut winner_op) = match self.next_record()? {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let mut history: Vec<(RunRecordV2, u8)> = Vec::new();

        // Consume consecutive duplicates, keeping the highest-t as winner.
        while let Some(peeked) = self.heap.first().map(|e| &e.record) {
            if !same_identity_v2(&winner, peeked) {
                break;
            }
            let (dup, dup_op) = self.next_record()?.unwrap();
            if dup.t > winner.t {
                // The old winner becomes history.
                history.push((winner, winner_op));
                winner = dup;
                winner_op = dup_op;
            } else {
                // The dup is history.
                history.push((dup, dup_op));
            }
        }

        Ok(Some((winner, winner_op, history)))
    }

    pub fn is_exhausted(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    /// In-memory merge source for testing.
    struct VecSource {
        records: Vec<RunRecordV2>,
        pos: usize,
    }

    impl VecSource {
        fn new(records: Vec<RunRecordV2>) -> Self {
            Self { records, pos: 0 }
        }
    }

    impl MergeSource for VecSource {
        fn peek(&self) -> Option<&RunRecordV2> {
            self.records.get(self.pos)
        }
        fn advance(&mut self) -> io::Result<()> {
            self.pos += 1;
            Ok(())
        }
        fn is_exhausted(&self) -> bool {
            self.pos >= self.records.len()
        }
    }

    /// In-memory merge source with explicit op bytes for testing.
    struct VecSourceWithOp {
        records: Vec<RunRecordV2>,
        ops: Vec<u8>,
        pos: usize,
    }

    impl VecSourceWithOp {
        fn new(records: Vec<RunRecordV2>, ops: Vec<u8>) -> Self {
            debug_assert_eq!(records.len(), ops.len());
            Self {
                records,
                ops,
                pos: 0,
            }
        }
    }

    impl MergeSource for VecSourceWithOp {
        fn peek(&self) -> Option<&RunRecordV2> {
            self.records.get(self.pos)
        }
        fn advance(&mut self) -> io::Result<()> {
            self.pos += 1;
            Ok(())
        }
        fn is_exhausted(&self) -> bool {
            self.pos >= self.records.len()
        }
        fn peek_op(&self) -> u8 {
            self.ops.get(self.pos).copied().unwrap_or(1)
        }
    }

    fn make_rec(s_id: u64, p_id: u32, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type: OType::XSD_INTEGER.as_u16(),
            g_id: 0,
        }
    }

    #[test]
    fn merge_two_streams() {
        let s1 = VecSource::new(vec![make_rec(1, 1, 10, 1), make_rec(3, 1, 30, 1)]);
        let s2 = VecSource::new(vec![make_rec(2, 1, 20, 1), make_rec(4, 1, 40, 1)]);

        let mut merge = KWayMerge::new(vec![s1, s2], cmp_v2_spot).unwrap();

        let mut results = Vec::new();
        while let Some((rec, op)) = merge.next_record().unwrap() {
            assert_eq!(op, 1); // default op for VecSource
            results.push(rec.s_id.as_u64());
        }
        assert_eq!(results, vec![1, 2, 3, 4]);
    }

    #[test]
    fn dedup_keeps_highest_t() {
        // Same identity (s=1, p=1, o_type=INT, o_key=10, o_i=MAX) at t=1 and t=5.
        let s1 = VecSource::new(vec![make_rec(1, 1, 10, 1)]);
        let s2 = VecSource::new(vec![make_rec(1, 1, 10, 5)]);

        let mut merge = KWayMerge::new(vec![s1, s2], cmp_v2_spot).unwrap();
        let (winner, op) = merge.next_deduped().unwrap().unwrap();
        assert_eq!(winner.t, 5); // higher t wins
        assert_eq!(op, 1); // default op
        assert!(merge.next_deduped().unwrap().is_none());
    }

    #[test]
    fn different_o_type_not_deduped() {
        let mut r1 = make_rec(1, 1, 10, 1);
        r1.o_type = OType::XSD_INTEGER.as_u16();
        let mut r2 = make_rec(1, 1, 10, 2);
        r2.o_type = OType::XSD_LONG.as_u16();

        // Sort them.
        let mut recs = vec![r1, r2];
        recs.sort_by(cmp_v2_spot);

        let s = VecSource::new(recs);
        let mut merge = KWayMerge::new(vec![s], cmp_v2_spot).unwrap();

        let (a, _) = merge.next_deduped().unwrap().unwrap();
        let (b, _) = merge.next_deduped().unwrap().unwrap();
        assert_ne!(a.o_type, b.o_type); // both emitted, not deduped
    }

    #[test]
    fn empty_streams() {
        let s1 = VecSource::new(vec![]);
        let s2 = VecSource::new(vec![]);
        let mut merge = KWayMerge::new(vec![s1, s2], cmp_v2_spot).unwrap();
        assert!(merge.next_record().unwrap().is_none());
        assert!(merge.is_exhausted());
    }

    #[test]
    fn dedup_preserves_op_of_winner() {
        // Two sources with the same identity: s1 has t=1, s2 has t=5.
        // s1 op=0 (retract), s2 op=1 (assert). Winner should be t=5 with op=1.
        let s1 = VecSourceWithOp::new(vec![make_rec(1, 1, 10, 1)], vec![0]);
        let s2 = VecSourceWithOp::new(vec![make_rec(1, 1, 10, 5)], vec![1]);

        let mut merge = KWayMerge::new(vec![s1, s2], cmp_v2_spot).unwrap();
        let (winner, op) = merge.next_deduped().unwrap().unwrap();
        assert_eq!(winner.t, 5);
        assert_eq!(op, 1); // winner's op
        assert!(merge.next_deduped().unwrap().is_none());
    }

    #[test]
    fn dedup_preserves_retract_op_of_winner() {
        // Two sources with the same identity: s1 has t=1 op=1, s2 has t=5 op=0.
        // Winner should be t=5 with op=0 (retract wins because higher t).
        let s1 = VecSourceWithOp::new(vec![make_rec(1, 1, 10, 1)], vec![1]);
        let s2 = VecSourceWithOp::new(vec![make_rec(1, 1, 10, 5)], vec![0]);

        let mut merge = KWayMerge::new(vec![s1, s2], cmp_v2_spot).unwrap();
        let (winner, op) = merge.next_deduped().unwrap().unwrap();
        assert_eq!(winner.t, 5);
        assert_eq!(op, 0); // retract-winner
        assert!(merge.next_deduped().unwrap().is_none());
    }
}
