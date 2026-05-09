//! Pure builder helpers for the edge-annotation arenas.
//!
//! The builder is split into two stages so callers can interleave the
//! CAS write between them:
//!
//! 1. **`build_*_leaves`** — chunk pre-sorted rows into leaves and
//!    return one encoded blob per chunk along with its routing summary.
//!    The caller writes each blob to CAS and collects the resulting
//!    [`ContentId`] for that leaf.
//! 2. **`build_*_branch`** — given leaf summaries paired with their CIDs,
//!    encode the branch manifest. The caller writes that blob to CAS
//!    too and stores the branch CID in [`AnnotationIndexRoot`].
//!
//! This module contains no I/O — it owns sort-respect, chunking, and
//! aggregate stats computation only. The indexer-side glue
//! (`fluree-db-indexer/src/build/annotation_arena.rs` in slice 3b)
//! handles bundle reconstruction from `f:reifies*` flakes and CAS
//! writes.
//!
//! ## Sort invariants
//!
//! Callers MUST pass rows sorted by:
//! - Forward: `(edge, ann, t, op)` ascending.
//! - Reverse: `(ann, edge, t, op)` ascending.
//!
//! Debug builds assert this; release builds trust the caller.

use super::format::{
    AnnotationForwardBranch, AnnotationForwardBranchEntry, AnnotationForwardLeaf,
    AnnotationForwardRow, AnnotationReverseBranch, AnnotationReverseBranchEntry,
    AnnotationReverseLeaf, AnnotationReverseRow,
};
use fluree_db_core::{AnnotationStats, ContentId, EdgeKey, Sid};
use std::collections::HashSet;

/// Default target rows per leaf. Picked to keep the postcard-encoded
/// leaf blob in the low-MB range for typical 100-byte rows. Builders
/// can override via [`build_forward_leaves`] / [`build_reverse_leaves`]
/// when sizing for a known workload.
pub const DEFAULT_TARGET_ROWS_PER_LEAF: usize = 4096;

/// Routing key bounds for a single forward-arena leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardLeafSummary {
    pub first_edge: EdgeKey,
    pub first_ann: Sid,
    pub last_edge: EdgeKey,
    pub last_ann: Sid,
    pub row_count: u64,
}

/// Routing key bounds for a single reverse-arena leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReverseLeafSummary {
    pub first_ann: Sid,
    pub first_edge: EdgeKey,
    pub last_ann: Sid,
    pub last_edge: EdgeKey,
    pub row_count: u64,
}

/// Compute aggregate stats over a sorted forward-row slice.
///
/// Returns `(max_t, stats)`. `max_t` is `0` for empty input — the
/// caller (typically the indexer) is responsible for substituting the
/// snapshot's `index_t` if it wants the arena root to advertise the
/// snapshot's `t` even with zero rows.
///
/// `distinct_edges` and `distinct_annotations` count **live**
/// attachments only: a `(edge, ann)` pair contributes to the totals
/// iff the final row in its history group has `op = true`. This
/// matches the field documentation on [`AnnotationStats`] and gives
/// the cost-based planner a true "currently attached" snapshot
/// rather than an over-count of historical churn. Implementation
/// relies on the `(edge, ann, t, op)` sort: the last row of each
/// `(edge, ann)` run is the latest event for that pair.
pub fn forward_arena_stats(rows: &[AnnotationForwardRow]) -> (i64, AnnotationStats) {
    if rows.is_empty() {
        return (0, AnnotationStats::default());
    }
    let mut max_t: i64 = i64::MIN;
    let mut live_edges: HashSet<EdgeKey> = HashSet::new();
    let mut live_anns: HashSet<Sid> = HashSet::new();
    for i in 0..rows.len() {
        if rows[i].t > max_t {
            max_t = rows[i].t;
        }
        let last_in_group = i + 1 == rows.len()
            || rows[i].edge != rows[i + 1].edge
            || rows[i].ann != rows[i + 1].ann;
        // The final event in each (edge, ann) run determines whether
        // the pair is currently live. Sort tie-breaker on `op` is
        // `false < true`, so an assert at the same `t` as a retract
        // correctly wins.
        if last_in_group && rows[i].op {
            live_edges.insert(rows[i].edge.clone());
            live_anns.insert(rows[i].ann.clone());
        }
    }
    let stats = AnnotationStats {
        forward_rows: rows.len() as u64,
        // Reverse rows mirror forward rows when the indexer emits both
        // arenas from the same source set. The caller fills in its own
        // count if the two are not symmetric.
        reverse_rows: rows.len() as u64,
        distinct_edges: live_edges.len() as u64,
        distinct_annotations: live_anns.len() as u64,
    };
    (max_t, stats)
}

/// Encode the forward-arena leaves from a sorted row slice.
///
/// Output is one `(summary, blob)` tuple per leaf. The blobs carry the
/// `EAFL1` magic and are ready to write to CAS as
/// [`fluree_db_core::ContentKind::AnnotationForwardLeaf`].
///
/// Empty input produces an empty Vec — the caller decides whether to
/// emit a zero-leaf branch or omit the section entirely (per
/// `EDGE_ANNOTATIONS.md` Sidecar Artifacts, omission is only legal when
/// the snapshot has zero `f:reifies*` bundles).
///
/// **Routing-key cohesion.** Chunks are extended past
/// `target_rows_per_leaf` whenever splitting would cut a `(edge, ann)`
/// group across two leaves. The branch holds inclusive `[first, last]`
/// `(edge, ann)` bounds per leaf; if a single hot routing key spilled
/// into two leaves, both leaves would advertise the same `(edge, ann)`
/// in their bounds and a `partition_point` lookup would only see the
/// first one — silently dropping the rest of the history. The
/// post-`target` overshoot keeps every history row for one
/// `(edge, ann)` co-located.
pub fn build_forward_leaves(
    rows: &[AnnotationForwardRow],
    target_rows_per_leaf: usize,
) -> Vec<(ForwardLeafSummary, Vec<u8>)> {
    let target = target_rows_per_leaf.max(1);
    debug_assert!(
        rows.windows(2)
            .all(|w| (&w[0].edge, &w[0].ann, w[0].t, w[0].op)
                <= (&w[1].edge, &w[1].ann, w[1].t, w[1].op)),
        "build_forward_leaves: rows must be sorted by (edge, ann, t, op)"
    );

    let mut out: Vec<(ForwardLeafSummary, Vec<u8>)> = Vec::new();
    let mut start = 0usize;
    while start < rows.len() {
        let mut end = (start + target).min(rows.len());
        // Extend `end` so we never split a `(edge, ann)` group across
        // two leaves. The routing key for forward leaves is
        // `(edge, ann)`; identical values must live in one leaf.
        while end < rows.len() {
            let prev = &rows[end - 1];
            let next = &rows[end];
            if prev.edge == next.edge && prev.ann == next.ann {
                end += 1;
            } else {
                break;
            }
        }
        let chunk = &rows[start..end];
        let first = chunk.first().expect("non-empty chunk");
        let last = chunk.last().expect("non-empty chunk");
        let summary = ForwardLeafSummary {
            first_edge: first.edge.clone(),
            first_ann: first.ann.clone(),
            last_edge: last.edge.clone(),
            last_ann: last.ann.clone(),
            row_count: chunk.len() as u64,
        };
        let leaf = AnnotationForwardLeaf {
            rows: chunk.to_vec(),
        };
        out.push((summary, leaf.encode()));
        start = end;
    }
    out
}

/// Encode the forward-arena branch from leaf summaries paired with
/// their CAS-written CIDs. Order must match `build_forward_leaves`'
/// output (which preserves the input row order).
pub fn build_forward_branch(leaves: &[(ForwardLeafSummary, ContentId)]) -> Vec<u8> {
    let entries = leaves
        .iter()
        .map(|(s, cid)| AnnotationForwardBranchEntry {
            first_edge: s.first_edge.clone(),
            first_ann: s.first_ann.clone(),
            last_edge: s.last_edge.clone(),
            last_ann: s.last_ann.clone(),
            row_count: s.row_count,
            leaf_cid: cid.clone(),
        })
        .collect();
    AnnotationForwardBranch { leaves: entries }.encode()
}

/// Encode the reverse-arena leaves from a sorted row slice.
///
/// Same routing-key cohesion guarantee as
/// [`build_forward_leaves`]: a single `(ann, edge)` group never
/// straddles two leaves.
pub fn build_reverse_leaves(
    rows: &[AnnotationReverseRow],
    target_rows_per_leaf: usize,
) -> Vec<(ReverseLeafSummary, Vec<u8>)> {
    let target = target_rows_per_leaf.max(1);
    debug_assert!(
        rows.windows(2)
            .all(|w| (&w[0].ann, &w[0].edge, w[0].t, w[0].op)
                <= (&w[1].ann, &w[1].edge, w[1].t, w[1].op)),
        "build_reverse_leaves: rows must be sorted by (ann, edge, t, op)"
    );

    let mut out: Vec<(ReverseLeafSummary, Vec<u8>)> = Vec::new();
    let mut start = 0usize;
    while start < rows.len() {
        let mut end = (start + target).min(rows.len());
        while end < rows.len() {
            let prev = &rows[end - 1];
            let next = &rows[end];
            if prev.ann == next.ann && prev.edge == next.edge {
                end += 1;
            } else {
                break;
            }
        }
        let chunk = &rows[start..end];
        let first = chunk.first().expect("non-empty chunk");
        let last = chunk.last().expect("non-empty chunk");
        let summary = ReverseLeafSummary {
            first_ann: first.ann.clone(),
            first_edge: first.edge.clone(),
            last_ann: last.ann.clone(),
            last_edge: last.edge.clone(),
            row_count: chunk.len() as u64,
        };
        let leaf = AnnotationReverseLeaf {
            rows: chunk.to_vec(),
        };
        out.push((summary, leaf.encode()));
        start = end;
    }
    out
}

/// Encode the reverse-arena branch from leaf summaries + CIDs.
pub fn build_reverse_branch(leaves: &[(ReverseLeafSummary, ContentId)]) -> Vec<u8> {
    let entries = leaves
        .iter()
        .map(|(s, cid)| AnnotationReverseBranchEntry {
            first_ann: s.first_ann.clone(),
            first_edge: s.first_edge.clone(),
            last_ann: s.last_ann.clone(),
            last_edge: s.last_edge.clone(),
            row_count: s.row_count,
            leaf_cid: cid.clone(),
        })
        .collect();
    AnnotationReverseBranch { leaves: entries }.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentKind, FlakeValue};
    use fluree_vocab::xsd;

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn edge(idx: u8) -> EdgeKey {
        EdgeKey {
            g: None,
            s: sid(11, &format!("s{idx}")),
            p: sid(12, &format!("p{idx}")),
            o: FlakeValue::Ref(sid(11, &format!("o{idx}"))),
            dt: Sid::new(0, xsd::ANY_URI),
            lang: None,
            list_i: None,
        }
    }

    fn fwd_row(edge_idx: u8, ann: &str, t: i64, op: bool) -> AnnotationForwardRow {
        AnnotationForwardRow {
            edge: edge(edge_idx),
            ann: sid(20, ann),
            t,
            op,
        }
    }

    fn rev_row(ann: &str, edge_idx: u8, t: i64, op: bool) -> AnnotationReverseRow {
        AnnotationReverseRow {
            ann: sid(20, ann),
            edge: edge(edge_idx),
            t,
            op,
        }
    }

    fn cid_for(blob: &[u8], kind: ContentKind) -> ContentId {
        ContentId::new(kind, blob)
    }

    #[test]
    fn forward_stats_empty_returns_default() {
        let (max_t, stats) = forward_arena_stats(&[]);
        assert_eq!(max_t, 0);
        assert_eq!(stats, AnnotationStats::default());
    }

    #[test]
    fn forward_stats_distinct_excludes_retracted_pairs() {
        // (edge_0, ann_a) attached then retracted → not live;
        // (edge_1, ann_b) attached → live. Stats should reflect only
        // currently-attached pairs, not historical churn.
        let rows = vec![
            fwd_row(0, "ann_a", 1, true),
            fwd_row(0, "ann_a", 2, false),
            fwd_row(1, "ann_b", 3, true),
        ];
        let (max_t, stats) = forward_arena_stats(&rows);
        assert_eq!(max_t, 3);
        assert_eq!(stats.forward_rows, 3, "all events still counted");
        assert_eq!(
            stats.distinct_edges, 1,
            "edge_0 was retracted; only edge_1 is live"
        );
        assert_eq!(stats.distinct_annotations, 1, "only ann_b is live");
    }

    #[test]
    fn forward_stats_assert_at_same_t_as_retract_wins() {
        // Same `t`, both ops on the same pair. Sort tie-break on
        // `op` puts `false` before `true`, so the assert is the
        // final row → pair is live.
        let rows = vec![fwd_row(0, "ann_a", 5, false), fwd_row(0, "ann_a", 5, true)];
        let (_, stats) = forward_arena_stats(&rows);
        assert_eq!(stats.distinct_edges, 1);
        assert_eq!(stats.distinct_annotations, 1);
    }

    #[test]
    fn forward_stats_counts_distinct_edges_and_annotations() {
        // Two edges; first has two annotations and a retract event;
        // second shares one annotation with the first.
        let rows = vec![
            fwd_row(0, "ann_a", 1, true),
            fwd_row(0, "ann_a", 2, false),
            fwd_row(0, "ann_b", 3, true),
            fwd_row(1, "ann_a", 4, true),
        ];
        let (max_t, stats) = forward_arena_stats(&rows);
        assert_eq!(max_t, 4);
        assert_eq!(stats.forward_rows, 4);
        assert_eq!(stats.distinct_edges, 2);
        assert_eq!(stats.distinct_annotations, 2, "ann_a + ann_b across edges");
    }

    #[test]
    fn forward_round_trip_single_leaf() {
        let rows = vec![
            fwd_row(0, "ann_a", 1, true),
            fwd_row(0, "ann_b", 2, true),
            fwd_row(1, "ann_c", 3, true),
        ];
        let leaves = build_forward_leaves(&rows, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(leaves.len(), 1);
        let (summary, blob) = &leaves[0];
        assert_eq!(summary.row_count, 3);
        assert_eq!(summary.first_edge, edge(0));
        assert_eq!(summary.last_edge, edge(1));

        let decoded = AnnotationForwardLeaf::decode(blob).unwrap();
        assert_eq!(decoded.rows, rows);

        let cid = cid_for(blob, ContentKind::AnnotationForwardLeaf);
        let branch_bytes = build_forward_branch(&[(summary.clone(), cid.clone())]);
        let branch = AnnotationForwardBranch::decode(&branch_bytes).unwrap();
        assert_eq!(branch.leaves.len(), 1);
        assert_eq!(branch.leaves[0].leaf_cid, cid);
        assert_eq!(branch.leaves[0].first_edge, edge(0));
        assert_eq!(branch.leaves[0].last_edge, edge(1));
        assert_eq!(branch.leaves[0].row_count, 3);
    }

    #[test]
    fn forward_chunking_keeps_routing_key_groups_together() {
        // 5 events for the same `(edge_0, ann_a)` history followed by
        // a singleton `(edge_1, ann_b)`. With `target = 2`, naive
        // row-count chunking would split `(edge_0, ann_a)` across two
        // leaves, leaving overlapping inclusive bounds in the branch.
        // Routing-key cohesion must extend the first chunk past the
        // target so the hot key stays in one leaf — otherwise a
        // `partition_point` lookup in the branch would only find the
        // first leaf and silently drop the rest of the history.
        let rows: Vec<AnnotationForwardRow> = (1..=5)
            .map(|t| fwd_row(0, "ann_a", t, true))
            .chain(std::iter::once(fwd_row(1, "ann_b", 6, true)))
            .collect();
        let leaves = build_forward_leaves(&rows, 2);
        assert_eq!(leaves.len(), 2);
        assert_eq!(
            leaves[0].0.row_count, 5,
            "all 5 (edge_0, ann_a) rows colocated"
        );
        assert_eq!(leaves[0].0.first_ann, sid(20, "ann_a"));
        assert_eq!(leaves[0].0.last_ann, sid(20, "ann_a"));
        assert_eq!(leaves[1].0.row_count, 1);
        // Branch entries must have non-overlapping `(edge, ann)` bounds.
        let entries: Vec<_> = leaves
            .iter()
            .map(|(s, b)| (s.clone(), cid_for(b, ContentKind::AnnotationForwardLeaf)))
            .collect();
        let branch_bytes = build_forward_branch(&entries);
        let branch = AnnotationForwardBranch::decode(&branch_bytes).unwrap();
        for w in branch.leaves.windows(2) {
            assert!(
                (&w[0].last_edge, &w[0].last_ann) < (&w[1].first_edge, &w[1].first_ann),
                "leaf bounds must not overlap on routing key"
            );
        }
    }

    #[test]
    fn reverse_chunking_keeps_routing_key_groups_together() {
        // Same scenario for the reverse arena: 5 events for the same
        // `(ann_a, edge_0)` then a singleton.
        let rows: Vec<AnnotationReverseRow> = (1..=5)
            .map(|t| rev_row("ann_a", 0, t, true))
            .chain(std::iter::once(rev_row("ann_b", 1, 6, true)))
            .collect();
        let leaves = build_reverse_leaves(&rows, 2);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].0.row_count, 5);
        assert_eq!(leaves[0].0.first_ann, sid(20, "ann_a"));
        assert_eq!(leaves[0].0.last_ann, sid(20, "ann_a"));
    }

    #[test]
    fn forward_round_trip_chunked_into_multiple_leaves() {
        // 5 rows with target=2 should produce 3 leaves of sizes 2, 2, 1.
        let rows = vec![
            fwd_row(0, "ann_a", 1, true),
            fwd_row(0, "ann_b", 2, true),
            fwd_row(1, "ann_a", 3, true),
            fwd_row(1, "ann_b", 4, true),
            fwd_row(2, "ann_a", 5, true),
        ];
        let leaves = build_forward_leaves(&rows, 2);
        assert_eq!(leaves.len(), 3);
        assert_eq!(leaves[0].0.row_count, 2);
        assert_eq!(leaves[1].0.row_count, 2);
        assert_eq!(leaves[2].0.row_count, 1);
        // Boundary keys must be inclusive on both sides.
        assert_eq!(leaves[0].0.first_ann, sid(20, "ann_a"));
        assert_eq!(leaves[0].0.last_ann, sid(20, "ann_b"));
        assert_eq!(leaves[1].0.first_edge, edge(1));
        assert_eq!(leaves[2].0.first_edge, edge(2));

        // Concatenating decoded leaves must equal the original input.
        let mut roundtrip: Vec<AnnotationForwardRow> = Vec::new();
        let mut entries: Vec<(ForwardLeafSummary, ContentId)> = Vec::new();
        for (summary, blob) in &leaves {
            let decoded = AnnotationForwardLeaf::decode(blob).unwrap();
            roundtrip.extend(decoded.rows);
            entries.push((
                summary.clone(),
                cid_for(blob, ContentKind::AnnotationForwardLeaf),
            ));
        }
        assert_eq!(roundtrip, rows);

        // Branch entries must be in the same order as the leaves.
        let branch_bytes = build_forward_branch(&entries);
        let branch = AnnotationForwardBranch::decode(&branch_bytes).unwrap();
        assert_eq!(branch.leaves.len(), 3);
        for (i, entry) in branch.leaves.iter().enumerate() {
            assert_eq!(entry.row_count, entries[i].0.row_count);
            assert_eq!(entry.leaf_cid, entries[i].1);
        }
    }

    #[test]
    fn forward_empty_input_produces_zero_leaves() {
        let leaves = build_forward_leaves(&[], DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(leaves.len(), 0);
        // The caller can still emit an empty branch.
        let branch_bytes = build_forward_branch(&[]);
        let branch = AnnotationForwardBranch::decode(&branch_bytes).unwrap();
        assert!(branch.leaves.is_empty());
    }

    #[test]
    fn reverse_round_trip_chunked() {
        let rows = vec![
            rev_row("ann_a", 0, 1, true),
            rev_row("ann_a", 1, 2, true),
            rev_row("ann_b", 0, 3, true),
        ];
        let leaves = build_reverse_leaves(&rows, 2);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].0.first_ann, sid(20, "ann_a"));
        assert_eq!(leaves[0].0.last_ann, sid(20, "ann_a"));
        assert_eq!(leaves[1].0.first_ann, sid(20, "ann_b"));
        assert_eq!(leaves[1].0.last_ann, sid(20, "ann_b"));

        let mut entries: Vec<(ReverseLeafSummary, ContentId)> = Vec::new();
        let mut roundtrip = Vec::new();
        for (summary, blob) in &leaves {
            let decoded = AnnotationReverseLeaf::decode(blob).unwrap();
            roundtrip.extend(decoded.rows);
            entries.push((
                summary.clone(),
                cid_for(blob, ContentKind::AnnotationReverseLeaf),
            ));
        }
        assert_eq!(roundtrip, rows);

        let branch_bytes = build_reverse_branch(&entries);
        let branch = AnnotationReverseBranch::decode(&branch_bytes).unwrap();
        assert_eq!(branch.leaves.len(), 2);
        assert_eq!(branch.leaves[0].first_ann, sid(20, "ann_a"));
        assert_eq!(branch.leaves[1].last_edge, edge(0));
    }

    #[test]
    #[should_panic(expected = "rows must be sorted")]
    fn forward_debug_assert_catches_unsorted() {
        // Out of order on `(edge, ann)` — debug builds must catch.
        let rows = vec![fwd_row(1, "ann_a", 1, true), fwd_row(0, "ann_a", 2, true)];
        let _ = build_forward_leaves(&rows, DEFAULT_TARGET_ROWS_PER_LEAF);
    }

    #[test]
    #[should_panic(expected = "rows must be sorted")]
    fn reverse_debug_assert_catches_unsorted() {
        let rows = vec![rev_row("ann_b", 0, 1, true), rev_row("ann_a", 0, 2, true)];
        let _ = build_reverse_leaves(&rows, DEFAULT_TARGET_ROWS_PER_LEAF);
    }

    #[test]
    fn target_rows_zero_is_treated_as_one() {
        // Defensive: zero target would otherwise infinite-loop on
        // chunks. We clamp to 1.
        let rows = vec![fwd_row(0, "ann_a", 1, true), fwd_row(1, "ann_a", 2, true)];
        let leaves = build_forward_leaves(&rows, 0);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].0.row_count, 1);
        assert_eq!(leaves[1].0.row_count, 1);
    }
}
