//! V3 query-facing types: fact identity, decoded rows, overlay operations.
//!
//! These parallel the V2 types in `types.rs` (`DecodedRow`, `OverlayOp`,
//! `sort_overlay_ops`) but use the V3 identity model:
//! `(s_id, p_id, o_type, o_key, o_i)` instead of `(s_id, p_id, o_kind, o_key, dt)`.

use crate::format::run_record::RunSortOrder;
use fluree_db_core::same_t::supersedes;
use fluree_db_core::GraphId;
use std::cmp::Ordering;

// ============================================================================
// FactKeyV3 — fact identity
// ============================================================================

/// Fact identity key for the V3 format.
///
/// Two facts with the same `FactKeyV3` are the same fact (differ only in `t`).
/// Used for dedup, overlay merge, and replay membership sets.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FactKeyV3 {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
}

// ============================================================================
// DecodedRowV3
// ============================================================================

/// A single decoded row from V3 columnar storage.
#[derive(Debug, Clone, Copy)]
pub struct DecodedRowV3 {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
    pub t: i64,
}

impl DecodedRowV3 {
    /// Extract the fact identity key (without `t`).
    #[inline]
    pub fn fact_key(&self) -> FactKeyV3 {
        FactKeyV3 {
            s_id: self.s_id,
            p_id: self.p_id,
            o_type: self.o_type,
            o_key: self.o_key,
            o_i: self.o_i,
        }
    }
}

// ============================================================================
// OverlayOp
// ============================================================================

/// An overlay operation translated to V3 integer-ID space.
///
/// Produced by translating `Flake` overlay ops via reverse dictionary lookups.
/// Sorted by the cursor's sort order for streaming merge with decoded
/// leaflet columns.
#[derive(Debug, Clone, Copy)]
pub struct OverlayOp {
    pub s_id: u64,
    pub p_id: u32,
    pub o_type: u16,
    pub o_key: u64,
    pub o_i: u32,
    pub t: i64,
    /// true = assert, false = retract.
    pub op: bool,
    /// Graph of the fact. Compared only by pooled orders; dedicated trees hold
    /// one graph and leave it 0.
    pub g_id: GraphId,
}

impl OverlayOp {
    /// Extract the fact identity key (without `t` and `op`).
    #[inline]
    pub fn fact_key(&self) -> FactKeyV3 {
        FactKeyV3 {
            s_id: self.s_id,
            p_id: self.p_id,
            o_type: self.o_type,
            o_key: self.o_key,
            o_i: self.o_i,
        }
    }
}

// ============================================================================
// Sort / comparison helpers
// ============================================================================

/// The sort-key fields of a row, overlay op or routing key.
#[derive(Clone, Copy)]
struct KeyFields {
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
    g_id: GraphId,
}

/// Compare by `order`'s sort key. Pooled orders compare `g_id` last, except
/// `Gspo`, which leads with it.
#[inline]
fn cmp_key_fields(a: KeyFields, b: KeyFields, order: RunSortOrder) -> Ordering {
    let base = match order.base() {
        RunSortOrder::Spot => a
            .s_id
            .cmp(&b.s_id)
            .then(a.p_id.cmp(&b.p_id))
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i)),
        RunSortOrder::Psot => a
            .p_id
            .cmp(&b.p_id)
            .then(a.s_id.cmp(&b.s_id))
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i)),
        RunSortOrder::Post => a
            .p_id
            .cmp(&b.p_id)
            .then(a.o_type.cmp(&b.o_type))
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i))
            .then(a.s_id.cmp(&b.s_id)),
        _ => a
            .o_type
            .cmp(&b.o_type)
            .then(a.o_key.cmp(&b.o_key))
            .then(a.o_i.cmp(&b.o_i))
            .then(a.p_id.cmp(&b.p_id))
            .then(a.s_id.cmp(&b.s_id)),
    };
    match order {
        RunSortOrder::Gspo => a.g_id.cmp(&b.g_id).then(base),
        o if o.is_pooled() => base.then(a.g_id.cmp(&b.g_id)),
        _ => base,
    }
}

impl OverlayOp {
    #[inline]
    fn key_fields(&self) -> KeyFields {
        KeyFields {
            s_id: self.s_id,
            p_id: self.p_id,
            o_type: self.o_type,
            o_key: self.o_key,
            o_i: self.o_i,
            g_id: self.g_id,
        }
    }
}

/// Compare two overlay ops by the V3 sort order (no `t` in sort order).
fn cmp_overlay_v3(a: &OverlayOp, b: &OverlayOp, order: RunSortOrder) -> Ordering {
    cmp_key_fields(a.key_fields(), b.key_fields(), order)
}

/// Sort overlay ops by the given V3 sort order.
pub fn sort_overlay_ops(ops: &mut [OverlayOp], order: RunSortOrder) {
    ops.sort_unstable_by(|a, b| cmp_overlay_v3(a, b, order));
}

/// Stable, run-adaptive variant of [`sort_overlay_ops`].
///
/// Rust's stable sort detects already-sorted input runs and merges them in
/// ~O(n log k) rather than re-sorting from scratch. Use this when the input is
/// a **concatenation of K already-sorted runs** (e.g. per-segment translated op
/// runs assembled by the segment-aware overlay path): here it acts as a k-way
/// merge of those runs, not a full re-sort.
pub fn sort_overlay_ops_stable(ops: &mut [OverlayOp], order: RunSortOrder) {
    ops.sort_by(|a, b| cmp_overlay_v3(a, b, order));
}

/// Resolve assert/retract lifecycles within overlay ops.
///
/// When the same fact (same `FactKeyV3`) has both an assertion and a retraction
/// in the overlay — e.g., an insert at t=N followed by an upsert at t=N+1 that
/// retracts the old value — only the latest operation (highest `t`) should
/// survive. This collapses each fact's lifecycle to its current state. A
/// retraction and an assertion at one `t` resolve to the assertion
/// ([`fluree_db_core::same_t`]), as every index build resolves them.
///
/// The `BinaryCursor` merge assumes each fact appears at most once in the overlay
/// ops. Without this resolution, both the stale assertion and the retraction are
/// processed independently, causing the stale value to leak into query results.
///
/// **Must be called after [`sort_overlay_ops`]** so that ops with the same fact
/// key are adjacent.
pub fn resolve_overlay_ops(ops: &mut Vec<OverlayOp>) {
    if ops.len() < 2 {
        return;
    }
    // Walk backwards: for each run of adjacent ops with the same fact key,
    // keep only the one with the highest `t`.
    let mut write = 0;
    let mut read = 0;
    while read < ops.len() {
        // Start of a new fact-key group. Find the op with max t in this group.
        let mut best = read;
        let key = ops[read].fact_key();
        read += 1;
        while read < ops.len() && ops[read].fact_key() == key {
            // Keep the op that holds: the highest `t`, and at one `t` the
            // assertion. Decided by rule, not by sort position: commits written
            // before the staging fix (and merges before the net-change fold)
            // hold same-`t` pairs, and the segment-aware assembly merges runs
            // across segments.
            if supersedes((ops[read].t, ops[read].op), (ops[best].t, ops[best].op)) {
                best = read;
            }
            read += 1;
        }
        ops[write] = ops[best];
        write += 1;
    }
    ops.truncate(write);
}

/// Each fact's state at `to_t` among `events`, which are sorted by `order`'s
/// key (a fact's events adjacent, in any `t` order): its latest event at or
/// before `to_t`, the assertion holding at one `t` (`fluree_db_core::same_t`).
/// Unlike [`resolve_overlay_ops`], a fact's identity includes its graph, so
/// the same triple in two pooled graphs stays two facts.
pub fn resolve_events_at(events: &[OverlayOp], to_t: i64) -> Vec<OverlayOp> {
    let mut out: Vec<OverlayOp> = Vec::new();
    let mut i = 0;
    while i < events.len() {
        let key = (events[i].fact_key(), events[i].g_id);
        let mut best: Option<OverlayOp> = None;
        while i < events.len() && (events[i].fact_key(), events[i].g_id) == key {
            let e = events[i];
            i += 1;
            if e.t > to_t {
                continue;
            }
            if best.is_none_or(|b| supersedes((e.t, e.op), (b.t, b.op))) {
                best = Some(e);
            }
        }
        out.extend(best);
    }
    out
}

/// Merge two resolved op lists sorted by `order`'s key into one; where both
/// hold a fact, `newer` wins.
pub fn merge_resolved_ops(
    older: &[OverlayOp],
    newer: &[OverlayOp],
    order: RunSortOrder,
) -> Vec<OverlayOp> {
    let mut out = Vec::with_capacity(older.len() + newer.len());
    let (mut i, mut j) = (0, 0);
    while i < older.len() && j < newer.len() {
        match cmp_overlay_v3(&older[i], &newer[j], order) {
            Ordering::Less => {
                out.push(older[i]);
                i += 1;
            }
            Ordering::Greater => {
                out.push(newer[j]);
                j += 1;
            }
            Ordering::Equal => {
                if older[i].fact_key() == newer[j].fact_key() && older[i].g_id == newer[j].g_id {
                    out.push(newer[j]);
                    i += 1;
                    j += 1;
                } else {
                    out.push(older[i]);
                    i += 1;
                }
            }
        }
    }
    out.extend_from_slice(&older[i..]);
    out.extend_from_slice(&newer[j..]);
    out
}

/// Compare a decoded row (from a `ColumnBatch`) against an overlay op
/// using the V3 sort order. Used by the two-pointer merge in `BinaryCursor`.
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn cmp_row_vs_overlay(
    s_id: u64,
    p_id: u32,
    o_type: u16,
    o_key: u64,
    o_i: u32,
    g_id: GraphId,
    ov: &OverlayOp,
    order: RunSortOrder,
) -> Ordering {
    let row = KeyFields {
        s_id,
        p_id,
        o_type,
        o_key,
        o_i,
        g_id,
    };
    cmp_key_fields(row, ov.key_fields(), order)
}

/// Compare an overlay op against a `RunRecordV2` branch key.
///
/// Used for per-leaf overlay slicing: binary-search overlay ops against
/// leaf `first_key`/`last_key` in the branch manifest.
#[inline]
pub fn cmp_overlay_vs_record(
    ov: &OverlayOp,
    rec: &crate::format::run_record_v2::RunRecordV2,
    order: RunSortOrder,
) -> Ordering {
    let rec = KeyFields {
        s_id: rec.s_id.as_u64(),
        p_id: rec.p_id,
        o_type: rec.o_type,
        o_key: rec.o_key,
        o_i: rec.o_i,
        g_id: rec.g_id,
    };
    cmp_key_fields(ov.key_fields(), rec, order)
}

/// Binary-search the window of `ops` (sorted by `order`) that intersects the
/// inclusive key range `[min_key, max_key]`.
///
/// Ops outside the window can never match a cursor bounded by those keys, so
/// a range-bounded cursor should be given only this window — otherwise every
/// probe pays an O(overlay) merge walk and loses leaflet pre-skips (the
/// first-leaf overlay window deliberately includes all preceding ops).
pub fn overlay_window_for_range(
    ops: &[OverlayOp],
    min_key: &crate::format::run_record_v2::RunRecordV2,
    max_key: &crate::format::run_record_v2::RunRecordV2,
    order: RunSortOrder,
) -> (usize, usize) {
    let start =
        ops.partition_point(|ov| cmp_overlay_vs_record(ov, min_key, order) == Ordering::Less);
    let end =
        ops.partition_point(|ov| cmp_overlay_vs_record(ov, max_key, order) != Ordering::Greater);
    (start, end.max(start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fact_key_equality() {
        let k1 = FactKeyV3 {
            s_id: 1,
            p_id: 2,
            o_type: 3,
            o_key: 4,
            o_i: u32::MAX,
        };
        let k2 = k1;
        assert_eq!(k1, k2);
    }

    #[test]
    fn overlay_sort_spot() {
        let mut ops = vec![
            OverlayOp {
                s_id: 2,
                p_id: 1,
                o_type: 0,
                o_key: 0,
                o_i: u32::MAX,
                t: 1,
                op: true,
                g_id: 0,
            },
            OverlayOp {
                s_id: 1,
                p_id: 1,
                o_type: 0,
                o_key: 0,
                o_i: u32::MAX,
                t: 1,
                op: true,
                g_id: 0,
            },
        ];
        sort_overlay_ops(&mut ops, RunSortOrder::Spot);
        assert_eq!(ops[0].s_id, 1);
        assert_eq!(ops[1].s_id, 2);
    }

    #[test]
    fn overlay_sort_post() {
        let mut ops = vec![
            OverlayOp {
                s_id: 1,
                p_id: 2,
                o_type: 0,
                o_key: 10,
                o_i: u32::MAX,
                t: 1,
                op: true,
                g_id: 0,
            },
            OverlayOp {
                s_id: 1,
                p_id: 2,
                o_type: 0,
                o_key: 5,
                o_i: u32::MAX,
                t: 1,
                op: true,
                g_id: 0,
            },
        ];
        sort_overlay_ops(&mut ops, RunSortOrder::Post);
        assert_eq!(ops[0].o_key, 5);
        assert_eq!(ops[1].o_key, 10);
    }
}
