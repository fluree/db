//! Novelty merge for the V3 index format.
//!
//! Merges sorted novelty `RunRecordV2` operations into a decoded V3 leaflet
//! (`ColumnBatch`), producing updated `RunRecordV2` rows and `HistEntryV2`
//! entries for the history sidecar.
//!
//! The algorithm is a two-pointer walk identical in structure to `novelty_merge.rs`
//! (V5), but uses the V3 identity model:
//!
//! - **Fact identity**: `(s_id, p_id, o_type, o_key, o_i)`
//! - **Sort orders**: SPOT/PSOT/POST/OPST — all include `o_i` in the comparator
//! - **History**: `HistEntryV2` (fixed-size, 31 bytes) for the sidecar, not inline Region 3

use fluree_db_binary_index::format::history_sidecar::HistEntryV2;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::read::column_types::ColumnBatch;
use fluree_db_core::subject_id::SubjectId;
use std::cmp::Ordering;

/// Sentinel for o_i when no list index is present.
const OI_NONE: u32 = u32::MAX;

// ============================================================================
// MergeInput / MergeOutput
// ============================================================================

/// Input to the V3 novelty merge.
pub struct MergeInput<'a> {
    /// Decoded V3 leaflet columns (from `load_leaflet_columns`).
    pub batch: &'a ColumnBatch,
    /// Existing history entries for this leaflet (from sidecar decode, carry forward).
    /// Already in reverse chronological order (t descending).
    pub existing_history: &'a [HistEntryV2],
    /// Sorted novelty operations to apply (must be sorted by `order`).
    pub novelty: &'a [RunRecordV2],
    /// Novelty ops — parallel array, same length as `novelty`.
    /// 1 = assert, 0 = retract.
    pub novelty_ops: &'a [u8],
    /// Sort order (determines merge comparison).
    pub order: RunSortOrder,
}

/// Output of the V3 novelty merge.
pub struct MergeOutput {
    /// Merged latest-state rows (asserted facts surviving the merge).
    pub records: Vec<RunRecordV2>,
    /// History entries: new merge events ++ existing history, reverse chronological.
    pub history: Vec<HistEntryV2>,
}

// ============================================================================
// Batch row extraction
// ============================================================================

/// Extract a `RunRecordV2` from a `ColumnBatch` at the given row index.
///
/// This reconstitutes a full record from the columnar representation.
/// `g_id` is set to 0 (not relevant for incremental merge; the caller
/// knows which graph this leaflet belongs to).
#[inline]
fn batch_row_to_record(batch: &ColumnBatch, row: usize) -> RunRecordV2 {
    RunRecordV2 {
        s_id: SubjectId(batch.s_id.get(row)),
        o_key: batch.o_key.get(row),
        p_id: batch.p_id.get(row),
        t: batch.t.get_or(row, 0),
        o_i: batch.o_i.get_or(row, OI_NONE),
        o_type: batch.o_type.get(row),
        g_id: 0,
    }
}

/// Convert a `RunRecordV2` + op into a `HistEntryV2`.
#[inline]
fn record_to_hist_entry(rec: &RunRecordV2, op: u8) -> HistEntryV2 {
    HistEntryV2 {
        s_id: rec.s_id,
        p_id: rec.p_id,
        o_type: rec.o_type,
        o_key: rec.o_key,
        o_i: rec.o_i,
        t: rec.t,
        op,
    }
}

/// Build a `HistEntryV2` from decoded batch row columns.
#[inline]
fn batch_row_to_hist_entry(batch: &ColumnBatch, row: usize, op: u8) -> HistEntryV2 {
    HistEntryV2 {
        s_id: SubjectId(batch.s_id.get(row)),
        p_id: batch.p_id.get(row),
        o_type: batch.o_type.get(row),
        o_key: batch.o_key.get(row),
        o_i: batch.o_i.get_or(row, OI_NONE),
        t: batch.t.get_or(row, 0),
        op,
    }
}

// ============================================================================
// Sort-order comparison: batch row vs RunRecordV2
// ============================================================================

/// Compare a decoded batch row against a `RunRecordV2` by the given sort order.
///
/// Compares identity columns only: `(s_id, p_id, o_type, o_key, o_i)` in
/// order-specific priority. Does **not** compare `t` or `op`.
///
/// In V3, sort-order position and fact identity use the same 5 columns,
/// so `Equal` always means same fact identity (no lang/i edge case like V5).
fn cmp_batch_row_vs_record(
    batch: &ColumnBatch,
    row: usize,
    rec: &RunRecordV2,
    order: RunSortOrder,
) -> Ordering {
    let s_id = batch.s_id.get(row);
    let p_id = batch.p_id.get(row);
    let o_type = batch.o_type.get(row);
    let o_key = batch.o_key.get(row);
    let o_i = batch.o_i.get_or(row, OI_NONE);

    let rec_s_id = rec.s_id.as_u64();
    match order {
        RunSortOrder::Spot => s_id
            .cmp(&rec_s_id)
            .then(p_id.cmp(&rec.p_id))
            .then(o_type.cmp(&rec.o_type))
            .then(o_key.cmp(&rec.o_key))
            .then(o_i.cmp(&rec.o_i)),
        RunSortOrder::Psot => p_id
            .cmp(&rec.p_id)
            .then(s_id.cmp(&rec_s_id))
            .then(o_type.cmp(&rec.o_type))
            .then(o_key.cmp(&rec.o_key))
            .then(o_i.cmp(&rec.o_i)),
        RunSortOrder::Post => p_id
            .cmp(&rec.p_id)
            .then(o_type.cmp(&rec.o_type))
            .then(o_key.cmp(&rec.o_key))
            .then(o_i.cmp(&rec.o_i))
            .then(s_id.cmp(&rec_s_id)),
        RunSortOrder::Opst => o_type
            .cmp(&rec.o_type)
            .then(o_key.cmp(&rec.o_key))
            .then(o_i.cmp(&rec.o_i))
            .then(p_id.cmp(&rec.p_id))
            .then(s_id.cmp(&rec_s_id)),
    }
}

// ============================================================================
// History assembly
// ============================================================================

/// Fact identity key for history dedup (V3).
#[derive(PartialEq, Eq)]
struct FactKeyV3 {
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
}

impl FactKeyV3 {
    #[inline]
    fn from_hist(e: &HistEntryV2) -> Self {
        Self {
            s_id: e.s_id.as_u64(),
            p_id: e.p_id,
            o_type: e.o_type,
            o_key: e.o_key,
            o_i: e.o_i,
        }
    }
}

/// Assemble the final history list from new merge entries + existing history.
///
/// - `new_history` is sorted by `t` descending, then deduplicated for adjacent
///   duplicate asserts (same FactKey).
/// - Concatenated as `new_history ++ existing_history` with boundary dedup.
fn assemble_history(
    mut new_history: Vec<HistEntryV2>,
    existing: &[HistEntryV2],
) -> Vec<HistEntryV2> {
    // Sort new entries by t descending (newest first).
    new_history.sort_unstable_by(|a, b| {
        b.t.cmp(&a.t).then_with(|| {
            // Tie-break: retracts before asserts (op=0 < op=1)
            a.op.cmp(&b.op)
        })
    });

    // Adjacent dedup within new_history.
    dedup_adjacent_asserts(&mut new_history);

    // Boundary dedup: at the new ++ existing seam, if both sides have an
    // assert for the same fact key, keep the newer one (higher t).
    // In the concat, new_history entries are generally newer than existing.
    let skip_first_old = match (new_history.last(), existing.first()) {
        (Some(last_new), Some(first_old))
            if FactKeyV3::from_hist(last_new) == FactKeyV3::from_hist(first_old)
                && last_new.op == 1
                && first_old.op == 1 =>
        {
            if last_new.t >= first_old.t {
                // last_new is newer or equal — keep last_new, skip first_old
                true
            } else {
                // first_old is newer — keep first_old, drop last_new
                new_history.pop();
                false
            }
        }
        _ => false,
    };

    if skip_first_old {
        new_history.extend_from_slice(&existing[1..]);
    } else {
        new_history.extend_from_slice(existing);
    }
    new_history
}

/// Remove adjacent duplicate asserts within a reverse-chronological history list.
///
/// For adjacent entries with the same FactKey and both asserts, the older one
/// (later index = lower t) is dropped; the newest one is kept. This preserves
/// the most recent event in the log.
fn dedup_adjacent_asserts(entries: &mut Vec<HistEntryV2>) {
    if entries.len() < 2 {
        return;
    }
    let mut write = 0;
    for read in 1..entries.len() {
        let is_dup = FactKeyV3::from_hist(&entries[write]) == FactKeyV3::from_hist(&entries[read])
            && entries[write].op == 1
            && entries[read].op == 1;
        if is_dup {
            // Keep newer (at write position, higher t), drop older (at read).
            // Don't advance write — next iteration compares against the kept entry.
        } else {
            write += 1;
            entries[write] = entries[read];
        }
    }
    entries.truncate(write + 1);
}

// ============================================================================
// Merge algorithm
// ============================================================================

/// Merge sorted novelty operations into a decoded V3 leaflet.
///
/// Walks the existing batch rows and novelty cursors together in sort order:
///
/// - **Existing < Novelty**: Emit existing row unchanged.
/// - **Novelty < Existing**:
///   - Assert: Emit novelty to output; record in history.
///   - Retract: Skip (retract of non-existent fact); still record in history.
/// - **Same identity** (Equal):
///   - Assert: Emit novelty (update); record retraction of old + new assert in history.
///   - Retract: Omit from output; record retraction in history.
///
/// After the walk, assembles history as `new_history ++ existing_history`
/// (newest first), with adjacent duplicate-assert dedup.
pub fn merge_novelty(input: &MergeInput<'_>) -> MergeOutput {
    let existing_len = input.batch.row_count;
    let novelty_len = input.novelty.len();
    debug_assert_eq!(
        novelty_len,
        input.novelty_ops.len(),
        "novelty and novelty_ops must have same length"
    );

    let mut out: Vec<RunRecordV2> = Vec::with_capacity(existing_len + novelty_len);
    let mut new_history: Vec<HistEntryV2> = Vec::with_capacity(novelty_len * 2);

    let mut ei = 0usize; // existing row index
    let mut ni = 0usize; // novelty index

    while ei < existing_len && ni < novelty_len {
        let nov = &input.novelty[ni];
        let op = input.novelty_ops[ni];
        let cmp = cmp_batch_row_vs_record(input.batch, ei, nov, input.order);

        match cmp {
            Ordering::Less => {
                // Existing row comes first — emit unchanged.
                out.push(batch_row_to_record(input.batch, ei));
                ei += 1;
            }
            Ordering::Greater => {
                // Novelty comes first (not in existing data).
                if op == 1 {
                    out.push(*nov);
                }
                new_history.push(record_to_hist_entry(nov, op));
                ni += 1;
            }
            Ordering::Equal => {
                // Same fact identity.
                if op == 1 {
                    // Assert (update): emit novelty, record retraction of old + new assert.
                    out.push(*nov);
                    // Retraction of old value: same identity columns as the old row,
                    // but t = the novelty's t (the retraction occurs at the transaction
                    // that caused it, not the old row's assertion time).
                    let mut retract_entry = batch_row_to_hist_entry(input.batch, ei, 0);
                    retract_entry.t = nov.t;
                    new_history.push(retract_entry);
                }
                // Record the novelty operation itself.
                new_history.push(record_to_hist_entry(nov, op));
                ei += 1;
                ni += 1;
            }
        }
    }

    // Drain remaining existing rows.
    while ei < existing_len {
        out.push(batch_row_to_record(input.batch, ei));
        ei += 1;
    }

    // Drain remaining novelty.
    while ni < novelty_len {
        let nov = &input.novelty[ni];
        let op = input.novelty_ops[ni];
        if op == 1 {
            out.push(*nov);
        }
        new_history.push(record_to_hist_entry(nov, op));
        ni += 1;
    }

    let history = assemble_history(new_history, input.existing_history);
    MergeOutput {
        records: out,
        history,
    }
}

/// Reconstitute `Vec<RunRecordV2>` from a `ColumnBatch`.
///
/// Useful when the incremental leaf path needs to feed passthrough
/// leaflet data through `LeafWriter` after a merge.
pub fn column_batch_to_records(batch: &ColumnBatch) -> Vec<RunRecordV2> {
    let mut records = Vec::with_capacity(batch.row_count);
    for i in 0..batch.row_count {
        records.push(batch_row_to_record(batch, i));
    }
    records
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::read::column_types::ColumnData;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::ObjKey;
    use std::sync::Arc;

    /// Helper: build a RunRecordV2 for testing (integer value).
    fn rec2(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key: ObjKey::encode_i64(val).as_u64(),
            p_id,
            t,
            o_i: OI_NONE,
            o_type: OType::XSD_INTEGER.as_u16(),
            g_id: 0,
        }
    }

    /// Helper: build a ColumnBatch from RunRecordV2 slice.
    fn batch_from_records(records: &[RunRecordV2]) -> ColumnBatch {
        if records.is_empty() {
            return ColumnBatch::empty();
        }
        let s_ids: Arc<[u64]> = records.iter().map(|r| r.s_id.as_u64()).collect();
        let o_keys: Arc<[u64]> = records.iter().map(|r| r.o_key).collect();
        let p_ids: Arc<[u32]> = records.iter().map(|r| r.p_id).collect();
        let o_types: Arc<[u16]> = records.iter().map(|r| r.o_type).collect();
        let o_is: Arc<[u32]> = records.iter().map(|r| r.o_i).collect();
        let ts: Arc<[u32]> = records.iter().map(|r| r.t).collect();

        let has_non_sentinel_oi = o_is.iter().any(|&v| v != OI_NONE);

        ColumnBatch {
            row_count: records.len(),
            s_id: ColumnData::Block(s_ids),
            o_key: ColumnData::Block(o_keys),
            p_id: ColumnData::Block(p_ids),
            o_type: ColumnData::Block(o_types),
            o_i: if has_non_sentinel_oi {
                ColumnData::Block(o_is)
            } else {
                ColumnData::AbsentDefault
            },
            t: ColumnData::Block(ts),
        }
    }

    /// Helper: make a MergeInput from existing records + novelty records + ops.
    fn make_input<'a>(
        batch: &'a ColumnBatch,
        novelty: &'a [RunRecordV2],
        ops: &'a [u8],
        existing_history: &'a [HistEntryV2],
        order: RunSortOrder,
    ) -> MergeInput<'a> {
        MergeInput {
            batch,
            existing_history,
            novelty,
            novelty_ops: ops,
            order,
        }
    }

    #[test]
    fn test_merge_empty_novelty() {
        let existing = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1)];
        let batch = batch_from_records(&existing);
        let input = make_input(&batch, &[], &[], &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 2);
        assert!(out.history.is_empty());
    }

    #[test]
    fn test_merge_assert_new_fact() {
        let existing = vec![rec2(1, 1, 10, 1), rec2(3, 1, 30, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(2, 1, 20, 5)];
        let ops = vec![1u8]; // assert
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 3);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 2);
        assert_eq!(out.records[2].s_id.as_u64(), 3);
        assert_eq!(out.records[1].t, 5);

        assert_eq!(out.history.len(), 1);
        assert_eq!(out.history[0].op, 1); // assert
        assert_eq!(out.history[0].s_id.as_u64(), 2);
    }

    #[test]
    fn test_merge_retract_existing_fact() {
        let existing = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1), rec2(3, 1, 30, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(2, 1, 20, 5)];
        let ops = vec![0u8]; // retract
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 3);

        // History: retraction entry
        assert_eq!(out.history.len(), 1);
        assert_eq!(out.history[0].op, 0);
        assert_eq!(out.history[0].s_id.as_u64(), 2);
    }

    #[test]
    fn test_merge_update_existing_fact() {
        let existing = vec![rec2(1, 1, 10, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(1, 1, 10, 5)]; // same identity, newer t
        let ops = vec![1u8]; // assert (update)
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].t, 5); // updated

        // History: retract at t=5 + assert at t=5 (both at novelty's t).
        // Sorted by t desc, then op asc (retracts before asserts at same t).
        assert_eq!(out.history.len(), 2);
        assert_eq!(out.history[0].t, 5);
        assert_eq!(out.history[0].op, 0); // retract first (same t, op=0 < op=1)
        assert_eq!(out.history[1].t, 5);
        assert_eq!(out.history[1].op, 1); // then assert
    }

    #[test]
    fn test_merge_retract_nonexistent() {
        let existing = vec![rec2(1, 1, 10, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(0, 1, 5, 5)]; // before s=1
        let ops = vec![0u8]; // retract
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].s_id.as_u64(), 1);

        // History still records the retraction
        assert_eq!(out.history.len(), 1);
        assert_eq!(out.history[0].op, 0);
    }

    #[test]
    fn test_merge_append_after() {
        let existing = vec![rec2(1, 1, 10, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(5, 1, 50, 5), rec2(6, 1, 60, 5)];
        let ops = vec![1u8, 1];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 3);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 5);
        assert_eq!(out.records[2].s_id.as_u64(), 6);
    }

    #[test]
    fn test_merge_prepend_before() {
        let existing = vec![rec2(5, 1, 50, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(1, 1, 10, 5), rec2(2, 1, 20, 5)];
        let ops = vec![1u8, 1];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 3);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 2);
        assert_eq!(out.records[2].s_id.as_u64(), 5);
    }

    #[test]
    fn test_merge_preserves_existing_history() {
        let existing = vec![rec2(1, 1, 10, 3)];
        let batch = batch_from_records(&existing);

        let old_history = vec![HistEntryV2 {
            s_id: SubjectId(1),
            p_id: 1,
            o_type: OType::XSD_INTEGER.as_u16(),
            o_key: ObjKey::encode_i64(5).as_u64(),
            o_i: OI_NONE,
            t: 2,
            op: 0, // retraction at t=2
        }];

        let novelty = vec![rec2(2, 1, 20, 5)];
        let ops = vec![1u8];
        let input = make_input(&batch, &novelty, &ops, &old_history, RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 2);

        // History: new assert (t=5) then old retraction (t=2)
        assert_eq!(out.history.len(), 2);
        assert_eq!(out.history[0].t, 5);
        assert_eq!(out.history[0].s_id.as_u64(), 2);
        assert_eq!(out.history[1].t, 2);
        assert_eq!(out.history[1].s_id.as_u64(), 1);
    }

    #[test]
    fn test_merge_mixed_operations() {
        let existing = vec![
            rec2(1, 1, 10, 1),
            rec2(2, 1, 20, 1),
            rec2(3, 1, 30, 1),
            rec2(5, 1, 50, 1),
        ];
        let batch = batch_from_records(&existing);
        let novelty = vec![
            rec2(2, 1, 20, 5), // retract s=2
            rec2(3, 1, 30, 5), // update s=3
            rec2(4, 1, 40, 5), // insert s=4
        ];
        let ops = vec![0u8, 1, 1]; // retract, assert, assert
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 4);
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[1].s_id.as_u64(), 3);
        assert_eq!(out.records[2].s_id.as_u64(), 4);
        assert_eq!(out.records[3].s_id.as_u64(), 5);
        assert_eq!(out.records[1].t, 5); // s=3 updated
    }

    #[test]
    fn test_merge_into_empty_leaflet() {
        let batch = ColumnBatch::empty();
        let novelty = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1)];
        let ops = vec![1u8, 1];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.history.len(), 2);
    }

    #[test]
    fn test_merge_retract_all() {
        // Retract every fact → empty latest-state, but history retained.
        let existing = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 1)];
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(1, 1, 10, 5), rec2(2, 1, 20, 5)];
        let ops = vec![0u8, 0]; // both retracts
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 0);
        // History: 2 retraction entries
        assert_eq!(out.history.len(), 2);
        assert!(out.history.iter().all(|h| h.op == 0));
    }

    #[test]
    fn test_merge_retract_all_with_existing_history() {
        // All facts retracted, but existing history is preserved.
        // This produces an empty leaflet with valid history linkage.
        let existing = vec![rec2(1, 1, 10, 3)];
        let batch = batch_from_records(&existing);

        let old_history = vec![HistEntryV2 {
            s_id: SubjectId(1),
            p_id: 1,
            o_type: OType::XSD_INTEGER.as_u16(),
            o_key: ObjKey::encode_i64(10).as_u64(),
            o_i: OI_NONE,
            t: 1,
            op: 1, // original assert at t=1
        }];

        let novelty = vec![rec2(1, 1, 10, 5)];
        let ops = vec![0u8]; // retract
        let input = make_input(&batch, &novelty, &ops, &old_history, RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 0); // empty latest-state
                                          // History: new retraction (t=5) + old assert (t=1)
        assert_eq!(out.history.len(), 2);
        assert_eq!(out.history[0].t, 5);
        assert_eq!(out.history[0].op, 0); // retract
        assert_eq!(out.history[1].t, 1);
        assert_eq!(out.history[1].op, 1); // old assert
    }

    #[test]
    fn test_column_batch_to_records() {
        let records = vec![rec2(1, 1, 10, 1), rec2(2, 1, 20, 2)];
        let batch = batch_from_records(&records);
        let reconstituted = column_batch_to_records(&batch);

        assert_eq!(reconstituted.len(), 2);
        assert_eq!(reconstituted[0].s_id.as_u64(), 1);
        assert_eq!(reconstituted[0].o_key, records[0].o_key);
        assert_eq!(reconstituted[1].s_id.as_u64(), 2);
    }

    #[test]
    fn test_merge_psot_order() {
        // Test with PSOT ordering: (p_id, s_id, o_type, o_key, o_i)
        let existing = vec![
            rec2(1, 1, 10, 1), // p=1, s=1
            rec2(2, 1, 20, 1), // p=1, s=2
            rec2(1, 2, 30, 1), // p=2, s=1
        ];
        let batch = batch_from_records(&existing);
        // Insert p=1,s=3 (between s=2 and p=2 in PSOT order)
        let novelty = vec![rec2(3, 1, 40, 5)]; // p=1, s=3
        let ops = vec![1u8];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Psot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 4);
        // PSOT order: (p=1,s=1), (p=1,s=2), (p=1,s=3), (p=2,s=1)
        assert_eq!(out.records[0].s_id.as_u64(), 1);
        assert_eq!(out.records[0].p_id, 1);
        assert_eq!(out.records[1].s_id.as_u64(), 2);
        assert_eq!(out.records[1].p_id, 1);
        assert_eq!(out.records[2].s_id.as_u64(), 3);
        assert_eq!(out.records[2].p_id, 1);
        assert_eq!(out.records[3].s_id.as_u64(), 1);
        assert_eq!(out.records[3].p_id, 2);
    }

    #[test]
    fn test_dedup_adjacent_asserts() {
        let o_type = OType::XSD_INTEGER.as_u16();
        let o_key = ObjKey::encode_i64(10).as_u64();

        let mut entries = vec![
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 5,
                op: 1,
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 3,
                op: 1,
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t, 5); // newer survives
    }

    #[test]
    fn test_dedup_keeps_retract_assert_pair() {
        let o_type = OType::XSD_INTEGER.as_u16();
        let o_key = ObjKey::encode_i64(10).as_u64();

        let mut entries = vec![
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 5,
                op: 0, // retract
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 3,
                op: 1, // assert
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 2); // both kept
    }

    #[test]
    fn test_boundary_dedup_new_is_newer() {
        let existing = vec![rec2(1, 1, 10, 1)];
        let batch = batch_from_records(&existing);

        let old_history = vec![HistEntryV2 {
            s_id: SubjectId(1),
            p_id: 1,
            o_type: OType::XSD_INTEGER.as_u16(),
            o_key: ObjKey::encode_i64(10).as_u64(),
            o_i: OI_NONE,
            t: 3,
            op: 1, // assert
        }];

        // Update same fact at t=10 → produces assert in new_history
        let novelty = vec![rec2(1, 1, 10, 10)];
        let ops = vec![1u8];
        let input = make_input(&batch, &novelty, &ops, &old_history, RunSortOrder::Spot);

        let out = merge_novelty(&input);
        // The newest assert (t=10) should survive boundary dedup
        let assert_entries: Vec<_> = out
            .history
            .iter()
            .filter(|e| e.op == 1 && e.s_id.as_u64() == 1)
            .collect();
        assert!(
            assert_entries.iter().any(|e| e.t == 10),
            "newest assert (t=10) should survive boundary dedup"
        );
    }

    /// Regression: update retraction must record at the novelty's t, not the old row's t.
    #[test]
    fn test_update_retraction_uses_novelty_t() {
        let existing = vec![rec2(1, 1, 10, 1)]; // old row at t=1
        let batch = batch_from_records(&existing);
        let novelty = vec![rec2(1, 1, 10, 5)]; // update at t=5
        let ops = vec![1u8];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].t, 5);

        // History: retract at t=5 (NOT t=1), then assert at t=5
        let retract = out.history.iter().find(|h| h.op == 0).unwrap();
        assert_eq!(
            retract.t, 5,
            "retraction event must occur at the novelty's t (5), not the old row's t (1)"
        );
        let assert_entry = out.history.iter().find(|h| h.op == 1).unwrap();
        assert_eq!(assert_entry.t, 5);
    }

    /// Regression: dedup_adjacent_asserts keeps newest (higher t), drops older.
    #[test]
    fn test_dedup_keeps_newest_assert() {
        let o_type = OType::XSD_INTEGER.as_u16();
        let o_key = ObjKey::encode_i64(10).as_u64();

        let mut entries = vec![
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 7,
                op: 1,
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 5,
                op: 1,
            },
            HistEntryV2 {
                s_id: SubjectId(1),
                p_id: 1,
                o_type,
                o_key,
                o_i: OI_NONE,
                t: 3,
                op: 1,
            },
        ];
        dedup_adjacent_asserts(&mut entries);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].t, 7, "newest assert (t=7) must survive");
    }

    #[test]
    fn test_const_column_batch() {
        // Test with Const columns (as would be produced for POST/PSOT leaflets)
        let batch = ColumnBatch {
            row_count: 3,
            s_id: ColumnData::Block(Arc::from(vec![1u64, 2, 3])),
            o_key: ColumnData::Block(Arc::from(vec![
                ObjKey::encode_i64(10).as_u64(),
                ObjKey::encode_i64(20).as_u64(),
                ObjKey::encode_i64(30).as_u64(),
            ])),
            p_id: ColumnData::Const(42), // constant p_id for POST
            o_type: ColumnData::Const(OType::XSD_INTEGER.as_u16()),
            o_i: ColumnData::AbsentDefault,
            t: ColumnData::Block(Arc::from(vec![1u32, 1, 1])),
        };

        // Insert s=4 with same p_id=42
        let novelty = vec![RunRecordV2 {
            s_id: SubjectId(4),
            o_key: ObjKey::encode_i64(40).as_u64(),
            p_id: 42,
            t: 5,
            o_i: OI_NONE,
            o_type: OType::XSD_INTEGER.as_u16(),
            g_id: 0,
        }];
        let ops = vec![1u8];
        let input = make_input(&batch, &novelty, &ops, &[], RunSortOrder::Spot);

        let out = merge_novelty(&input);
        assert_eq!(out.records.len(), 4);
        // All should have p_id=42
        assert!(out.records.iter().all(|r| r.p_id == 42));
        assert_eq!(out.records[3].s_id.as_u64(), 4);
    }
}
