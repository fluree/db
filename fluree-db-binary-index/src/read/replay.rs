//! Time-travel replay: reconstruct leaflet state at `t_target` using
//! base columns (latest-state) and history sidecar entries.
//!
//! - **Fact identity**: `(s_id, p_id, o_type, o_key, o_i)`
//! - **History entries**: `HistEntryV2` from FHS1 sidecar
//! - **Input/output**: `ColumnBatch`
//!
//! ## Algorithm
//!
//! 0. Build current-state membership: `FactKeyV3 → t` from base `ColumnBatch`
//! 1. Collect undo events with `t > t_target` from both base rows and history
//! 2. Apply undo events in reverse-chronological order to derive final state
//! 3. Derive exclude/include sets and three-way merge into output `ColumnBatch`

use std::collections::HashMap;
use std::sync::Arc;

use crate::format::history_sidecar::HistEntryV2;
use crate::format::run_record::RunSortOrder;
use crate::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use crate::read::column_types::{ColumnBatch, ColumnData};

/// Sentinel for o_i when no list index is present.
const OI_NONE: u32 = u32::MAX;

// ============================================================================
// V3 Fact Key (identity for replay)
// ============================================================================

#[derive(Clone, Hash, Eq, PartialEq)]
struct FactKeyV3 {
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
}

impl FactKeyV3 {
    #[inline]
    fn from_batch(batch: &ColumnBatch, row: usize) -> Self {
        Self {
            s_id: batch.s_id.get(row),
            p_id: batch.p_id.get(row),
            o_type: batch.o_type.get(row),
            o_key: batch.o_key.get(row),
            o_i: batch.o_i.get_or(row, OI_NONE),
        }
    }

    #[inline]
    fn from_hist(entry: &HistEntryV2) -> Self {
        Self {
            s_id: entry.s_id.as_u64(),
            p_id: entry.p_id,
            o_type: entry.o_type,
            o_key: entry.o_key,
            o_i: entry.o_i,
        }
    }
}

// ============================================================================
// Undo event
// ============================================================================

struct UndoEvent {
    abs_t: u32,
    is_assert: bool,
    key: FactKeyV3,
    /// Source entry for materialization when undoing a retract (restore the fact).
    source: Option<HistEntryV2>,
}

// ============================================================================
// Replay output state per fact key
// ============================================================================

struct FactState {
    /// Whether the fact should be present at `t_target`.
    present: bool,
    /// Source entry to materialize from (for include/restore).
    include_src: Option<HistEntryV2>,
}

// ============================================================================
// Public API
// ============================================================================

/// Replay a V3 leaflet to reconstruct its state at `t_target`.
///
/// Returns `Some(ColumnBatch)` with the reconstructed rows, or `None` if
/// no replay is needed (no events with `t > t_target`).
///
/// # Inputs
///
/// - `batch`: decoded base columns (latest-state) for the leaflet
/// - `history`: decoded history entries from the sidecar segment (sorted by t desc)
/// - `t_target`: the transaction time to reconstruct (query `AS OF`)
/// - `order`: sort order (for maintaining output ordering)
pub fn replay_leaflet(
    batch: &ColumnBatch,
    history: &[HistEntryV2],
    t_target: i64,
    order: RunSortOrder,
) -> Option<ColumnBatch> {
    // Clamp t_target to u32 range. All on-disk t values are u32.
    // t_target < 0 → "before genesis" → everything is undone → empty batch.
    // t_target > u32::MAX → all t values are ≤ target → no replay needed.
    if t_target > u32::MAX as i64 {
        return None;
    }
    if t_target < 0 {
        // Before any transaction — return empty batch.
        return Some(ColumnBatch::empty());
    }
    let t_target_u32 = t_target as u32;

    // ---- Step 0: Build current-state membership ----
    // Map FactKeyV3 → (t, row_index) for all base rows.
    let mut membership: HashMap<FactKeyV3, (u32, usize)> = HashMap::with_capacity(batch.row_count);
    for i in 0..batch.row_count {
        let key = FactKeyV3::from_batch(batch, i);
        let t = batch.t.get_or(i, 0);
        membership.insert(key, (t, i));
    }

    // ---- Step 1: Collect undo events with t > t_target ----
    let mut events: Vec<UndoEvent> = Vec::new();

    // From history entries.
    for entry in history {
        if entry.t <= t_target_u32 {
            // History is sorted by t descending; once we hit ≤ target, stop.
            break;
        }
        events.push(UndoEvent {
            abs_t: entry.t,
            is_assert: entry.op == 1,
            key: FactKeyV3::from_hist(entry),
            source: Some(*entry),
        });
    }

    // From base rows with t > t_target (synthetic assert events).
    for i in 0..batch.row_count {
        let t = batch.t.get_or(i, 0);
        if t > t_target_u32 {
            events.push(UndoEvent {
                abs_t: t,
                is_assert: true,
                key: FactKeyV3::from_batch(batch, i),
                source: None,
            });
        }
    }

    // Early return if no events need undoing.
    if events.is_empty() {
        return None;
    }

    // ---- Step 2: Apply undo events in reverse-chronological order ----
    // Sort by t descending, then asserts before retracts at same t.
    events.sort_unstable_by(|a, b| {
        b.abs_t
            .cmp(&a.abs_t)
            .then_with(|| b.is_assert.cmp(&a.is_assert))
    });

    // Per-fact state tracker.
    let mut fact_states: HashMap<FactKeyV3, FactState> = HashMap::new();

    for event in &events {
        let state = fact_states.entry(event.key.clone()).or_insert(FactState {
            present: membership.contains_key(&event.key),
            include_src: None,
        });

        if event.is_assert {
            // Undo assert → mark absent.
            state.present = false;
        } else {
            // Undo retract → mark present, remember source for materialization.
            state.present = true;
            if state.include_src.is_none() {
                state.include_src = event.source;
            }
        }
    }

    // ---- Step 3: Derive exclude/include sets ----
    // Exclude: base rows that shouldn't exist at t_target.
    let mut exclude_indices: Vec<usize> = Vec::new();
    // Include: facts to restore from history.
    let mut includes: Vec<RunRecordV2> = Vec::new();

    for (key, state) in &fact_states {
        let base_entry = membership.get(key); // O(1) lookup
        let in_base = base_entry.is_some();

        if in_base && !state.present {
            // Was in base, should be absent → exclude.
            let (_, row_idx) = base_entry.unwrap();
            exclude_indices.push(*row_idx);
        } else if !in_base && state.present {
            // Was NOT in base, should be present → include from source.
            if let Some(src) = &state.include_src {
                includes.push(hist_entry_to_record(src));
            }
        } else if in_base && state.present {
            // Was in base and should remain, but check if base row's t > t_target.
            // If so, we need to swap it with the older version from history.
            let (base_t, row_idx) = base_entry.unwrap();
            if *base_t > t_target_u32 {
                // Exclude the base row (too new).
                exclude_indices.push(*row_idx);
                // Include the older version from history (find the assert with t ≤ t_target).
                if let Some(src) = find_base_assert_at_target(key, history, t_target_u32) {
                    includes.push(hist_entry_to_record(&src));
                } else if let Some(src) = &state.include_src {
                    includes.push(hist_entry_to_record(src));
                }
            }
        }
    }

    // Sort excludes for efficient skip during merge.
    exclude_indices.sort_unstable();
    exclude_indices.dedup();

    // Sort includes by the leaflet's sort order for ordered merge.
    let cmp = cmp_v2_for_order(order);
    includes.sort_unstable_by(cmp);

    // ---- Step 4: Three-way merge ----
    // Merge (base rows minus excludes) with includes, maintaining sort order.
    let exclude_set: std::collections::HashSet<usize> = exclude_indices.iter().copied().collect();

    let mut out_s: Vec<u64> = Vec::new();
    let mut out_p: Vec<u32> = Vec::new();
    let mut out_otype: Vec<u16> = Vec::new();
    let mut out_okey: Vec<u64> = Vec::new();
    let mut out_oi: Vec<u32> = Vec::new();
    let mut out_t: Vec<u32> = Vec::new();

    let mut bi = 0usize; // base row index
    let mut ii = 0usize; // include index

    // Helper: push a base row to output.
    let push_base = |batch: &ColumnBatch,
                     row: usize,
                     s: &mut Vec<u64>,
                     p: &mut Vec<u32>,
                     ot: &mut Vec<u16>,
                     ok: &mut Vec<u64>,
                     oi: &mut Vec<u32>,
                     t: &mut Vec<u32>| {
        s.push(batch.s_id.get(row));
        p.push(batch.p_id.get(row));
        ot.push(batch.o_type.get(row));
        ok.push(batch.o_key.get(row));
        oi.push(batch.o_i.get_or(row, OI_NONE));
        t.push(batch.t.get_or(row, 0));
    };

    // Helper: push an include record to output.
    let push_include = |rec: &RunRecordV2,
                        s: &mut Vec<u64>,
                        p: &mut Vec<u32>,
                        ot: &mut Vec<u16>,
                        ok: &mut Vec<u64>,
                        oi: &mut Vec<u32>,
                        t: &mut Vec<u32>| {
        s.push(rec.s_id.as_u64());
        p.push(rec.p_id);
        ot.push(rec.o_type);
        ok.push(rec.o_key);
        oi.push(rec.o_i);
        t.push(rec.t);
    };

    // Convert base row to RunRecordV2 for comparison.
    let batch_row_as_rec = |batch: &ColumnBatch, row: usize| -> RunRecordV2 {
        RunRecordV2 {
            s_id: fluree_db_core::subject_id::SubjectId(batch.s_id.get(row)),
            o_key: batch.o_key.get(row),
            p_id: batch.p_id.get(row),
            t: batch.t.get_or(row, 0),
            o_i: batch.o_i.get_or(row, OI_NONE),
            o_type: batch.o_type.get(row),
            g_id: 0,
        }
    };

    while bi < batch.row_count && ii < includes.len() {
        // Skip excluded base rows.
        if exclude_set.contains(&bi) {
            bi += 1;
            continue;
        }

        let base_rec = batch_row_as_rec(batch, bi);
        let inc = &includes[ii];
        let ord = cmp(&base_rec, inc);

        match ord {
            std::cmp::Ordering::Less => {
                push_base(
                    batch,
                    bi,
                    &mut out_s,
                    &mut out_p,
                    &mut out_otype,
                    &mut out_okey,
                    &mut out_oi,
                    &mut out_t,
                );
                bi += 1;
            }
            std::cmp::Ordering::Greater => {
                push_include(
                    inc,
                    &mut out_s,
                    &mut out_p,
                    &mut out_otype,
                    &mut out_okey,
                    &mut out_oi,
                    &mut out_t,
                );
                ii += 1;
            }
            std::cmp::Ordering::Equal => {
                // Same position — prefer include (it's the historically correct version).
                push_include(
                    inc,
                    &mut out_s,
                    &mut out_p,
                    &mut out_otype,
                    &mut out_okey,
                    &mut out_oi,
                    &mut out_t,
                );
                bi += 1;
                ii += 1;
            }
        }
    }

    // Drain remaining base rows (skipping excludes).
    while bi < batch.row_count {
        if !exclude_set.contains(&bi) {
            push_base(
                batch,
                bi,
                &mut out_s,
                &mut out_p,
                &mut out_otype,
                &mut out_okey,
                &mut out_oi,
                &mut out_t,
            );
        }
        bi += 1;
    }

    // Drain remaining includes.
    while ii < includes.len() {
        push_include(
            &includes[ii],
            &mut out_s,
            &mut out_p,
            &mut out_otype,
            &mut out_okey,
            &mut out_oi,
            &mut out_t,
        );
        ii += 1;
    }

    let row_count = out_s.len();

    // Check if o_i is all sentinel (can use AbsentDefault).
    let has_non_sentinel_oi = out_oi.iter().any(|&v| v != OI_NONE);

    Some(ColumnBatch {
        row_count,
        s_id: ColumnData::Block(Arc::from(out_s)),
        o_key: ColumnData::Block(Arc::from(out_okey)),
        p_id: ColumnData::Block(Arc::from(out_p)),
        o_type: ColumnData::Block(Arc::from(out_otype)),
        o_i: if has_non_sentinel_oi {
            ColumnData::Block(Arc::from(out_oi))
        } else {
            ColumnData::AbsentDefault
        },
        t: ColumnData::Block(Arc::from(out_t)),
    })
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a `HistEntryV2` to a `RunRecordV2` for sort comparison and output.
fn hist_entry_to_record(entry: &HistEntryV2) -> RunRecordV2 {
    RunRecordV2 {
        s_id: entry.s_id,
        o_key: entry.o_key,
        p_id: entry.p_id,
        t: entry.t,
        o_i: entry.o_i,
        o_type: entry.o_type,
        g_id: 0,
    }
}

/// Find the most recent assert entry for `key` with `t ≤ t_target` in history.
///
/// Used for the "base row has `t > t_target` but fact should be present" case:
/// we need to find the version of the fact that was valid at `t_target`.
fn find_base_assert_at_target(
    key: &FactKeyV3,
    history: &[HistEntryV2],
    t_target: u32,
) -> Option<HistEntryV2> {
    // History is sorted by t descending. Find the first assert with t ≤ t_target.
    for entry in history {
        if entry.t <= t_target && entry.op == 1 && FactKeyV3::from_hist(entry) == *key {
            return Some(*entry);
        }
    }
    None
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::ObjKey;

    fn make_batch(records: &[(u64, u32, i64, u32)]) -> ColumnBatch {
        if records.is_empty() {
            return ColumnBatch::empty();
        }
        let s_ids: Arc<[u64]> = records.iter().map(|r| r.0).collect();
        let p_ids: Arc<[u32]> = records.iter().map(|_| 1u32).collect();
        let o_types: Arc<[u16]> = records
            .iter()
            .map(|_| OType::XSD_INTEGER.as_u16())
            .collect();
        let o_keys: Arc<[u64]> = records
            .iter()
            .map(|r| ObjKey::encode_i64(r.2).as_u64())
            .collect();
        let ts: Arc<[u32]> = records.iter().map(|r| r.3).collect();

        ColumnBatch {
            row_count: records.len(),
            s_id: ColumnData::Block(s_ids),
            o_key: ColumnData::Block(o_keys),
            p_id: ColumnData::Block(p_ids),
            o_type: ColumnData::Block(o_types),
            o_i: ColumnData::AbsentDefault,
            t: ColumnData::Block(ts),
        }
    }

    fn make_hist(s_id: u64, val: i64, t: u32, op: u8) -> HistEntryV2 {
        HistEntryV2 {
            s_id: SubjectId(s_id),
            p_id: 1,
            o_type: OType::XSD_INTEGER.as_u16(),
            o_key: ObjKey::encode_i64(val).as_u64(),
            o_i: OI_NONE,
            t,
            op,
        }
    }

    #[test]
    fn no_replay_needed() {
        // All rows have t ≤ t_target, no history events above target.
        let batch = make_batch(&[(1, 1, 10, 1), (2, 1, 20, 1)]);
        let result = replay_leaflet(&batch, &[], 5, RunSortOrder::Spot);
        assert!(result.is_none(), "no replay needed when all t ≤ t_target");
    }

    #[test]
    fn exclude_row_above_target() {
        // Row at t=5 should be excluded when t_target=3.
        let batch = make_batch(&[(1, 1, 10, 1), (2, 1, 20, 5)]);
        let result = replay_leaflet(&batch, &[], 3, RunSortOrder::Spot);
        assert!(result.is_some());
        let replayed = result.unwrap();
        assert_eq!(replayed.row_count, 1);
        assert_eq!(replayed.s_id.get(0), 1);
    }

    #[test]
    fn restore_retracted_fact() {
        // Base has s=1 at t=3. History shows s=2 was retracted at t=5.
        // At t_target=4, s=2 should be restored.
        let batch = make_batch(&[(1, 1, 10, 3)]);
        let history = vec![
            make_hist(2, 20, 5, 0), // retract s=2 at t=5
            make_hist(2, 20, 2, 1), // assert s=2 at t=2
        ];
        let result = replay_leaflet(&batch, &history, 4, RunSortOrder::Spot);
        assert!(result.is_some());
        let replayed = result.unwrap();
        assert_eq!(replayed.row_count, 2);
        // Should have both s=1 and s=2 (restored).
        let s_ids: Vec<u64> = (0..replayed.row_count)
            .map(|i| replayed.s_id.get(i))
            .collect();
        assert!(s_ids.contains(&1));
        assert!(s_ids.contains(&2));
    }

    #[test]
    fn swap_newer_base_row_with_older_version() {
        // Base has s=1 with val=20 at t=5. History has assert of val=10 at t=2.
        // At t_target=3, s=1 should have val=10 (the older version).
        let batch = make_batch(&[(1, 1, 20, 5)]);
        let history = vec![
            make_hist(1, 20, 5, 1), // assert val=20 at t=5
            make_hist(1, 10, 5, 0), // retract val=10 at t=5
            make_hist(1, 10, 2, 1), // assert val=10 at t=2
        ];
        let result = replay_leaflet(&batch, &history, 3, RunSortOrder::Spot);
        assert!(result.is_some());
        let replayed = result.unwrap();
        assert_eq!(replayed.row_count, 1);
        assert_eq!(replayed.s_id.get(0), 1);
        // The value should be val=10 (the older version).
        assert_eq!(
            replayed.o_key.get(0),
            ObjKey::encode_i64(10).as_u64(),
            "should have older value at t_target"
        );
    }

    #[test]
    fn negative_t_target_returns_empty() {
        let batch = make_batch(&[(1, 1, 10, 1), (2, 1, 20, 2)]);
        let result = replay_leaflet(&batch, &[], -1, RunSortOrder::Spot);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().row_count,
            0,
            "t_target < 0 means before genesis"
        );
    }

    #[test]
    fn huge_t_target_returns_none() {
        let batch = make_batch(&[(1, 1, 10, 1)]);
        let result = replay_leaflet(&batch, &[], i64::MAX, RunSortOrder::Spot);
        assert!(
            result.is_none(),
            "t_target > u32::MAX means no replay needed"
        );
    }
}
