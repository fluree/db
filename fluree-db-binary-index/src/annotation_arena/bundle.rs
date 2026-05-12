//! Reconstruct edge-annotation rows from a slab of `f:reifies*` flakes.
//!
//! The indexer collects every `f:reifies*` fact reachable from the
//! snapshot's commit chain and hands them to [`build_arenas_from_flakes`].
//! This module:
//!
//! 1. Groups the flakes by `(ann_subject, t, op)` so each group is a
//!    single attachment-event bundle.
//! 2. Calls [`EdgeKey::from_reifies_facts`] to validate the bundle and
//!    materialize an [`EdgeKey`].
//! 3. Emits one forward and one reverse row per validated bundle.
//! 4. Sorts each list, runs the pure builder
//!    ([`super::builder`]), and returns blobs ready for CAS writes
//!    plus the [`AnnotationStats`] / `max_t` to seal into
//!    [`AnnotationIndexRoot`].
//!
//! Malformed bundles (missing `f:reifiesSubject`, datatype mismatch,
//! list-index facts in v1, …) are skipped with a `tracing::warn` and
//! counted; the rest of the snapshot indexes normally. This mirrors
//! the replay-validator behavior described in
//! `docs/design/edge-annotations.md` ("the on-disk arena never
//! contains rows from malformed bundles").
//!
//! ## Inputs
//!
//! Callers pass `&[Flake]` containing **only** `f:reifies*` flakes —
//! the filter is the caller's responsibility because they have direct
//! access to predicate-SID information (the global predicate dict in
//! the indexer, or `is_reserved_reifies_predicate` for ad-hoc cases).
//! Passing non-`f:reifies*` flakes is a no-op (they're ignored by the
//! per-bundle decoder), but increases bundle-grouping cost.
//!
//! ## What this module is not
//!
//! - It does not write to CAS. The caller takes the leaf blobs from
//!   the [`ArenaBuildOutput`], writes each one, then calls
//!   [`super::build_forward_branch`] / [`super::build_reverse_branch`]
//!   with `(summary, cid)` pairs.
//! - It does not build [`AnnotationIndexRoot`]. The caller fills in
//!   `forward_branch_cid` / `reverse_branch_cid` after writing the
//!   branches.

use super::builder::{
    build_forward_leaves, build_reverse_leaves, ForwardLeafSummary, ReverseLeafSummary,
};
use super::format::{AnnotationForwardRow, AnnotationReverseRow};
use fluree_db_core::{AnnotationStats, EdgeKey, Flake, Sid};
use std::collections::HashMap;

/// Output of [`build_arenas_from_flakes`].
///
/// `forward_leaves` and `reverse_leaves` carry encoded blobs ready
/// for CAS writes. The caller writes each blob, collects the resulting
/// `ContentId`, then calls
/// [`super::build_forward_branch`] / [`super::build_reverse_branch`]
/// with the `(summary, cid)` pairs to encode the branch manifest.
#[derive(Debug)]
pub struct ArenaBuildOutput {
    pub forward_leaves: Vec<(ForwardLeafSummary, Vec<u8>)>,
    pub reverse_leaves: Vec<(ReverseLeafSummary, Vec<u8>)>,
    /// Highest `t` observed across all valid bundles. `0` when zero
    /// valid bundles were produced.
    pub max_t: i64,
    /// Aggregate stats over the validated rows. Independent counters
    /// for forward / reverse (always equal in the current builder, but
    /// kept distinct so future filters can diverge).
    pub stats: AnnotationStats,
    /// Number of malformed bundles skipped. Surfaced so callers can
    /// emit a single rolled-up telemetry counter rather than one
    /// per-bundle.
    pub skipped_bundles: u64,
}

/// Reconstruct + sort + chunk forward / reverse arena rows from a slab
/// of `f:reifies*` flakes.
///
/// `target_rows_per_leaf` controls leaf chunking; pass
/// [`DEFAULT_TARGET_ROWS_PER_LEAF`] when in doubt.
pub fn build_arenas_from_flakes(flakes: &[Flake], target_rows_per_leaf: usize) -> ArenaBuildOutput {
    // Group by (flake_graph, ann_sid, t, op) — each group is one
    // bundle. The flake-level graph is part of the key because the
    // writer convention is that `f:reifies*` flakes for an edge in
    // graph G are themselves asserted in graph G; folding two graphs
    // together at this stage would let a pathological cross-graph
    // collision merge into a single (wrong-graph) bundle. `ann_sid`
    // is the *subject* of every `f:reifies*` flake — the annotation's
    // id, not the reified edge's subject.
    let mut groups: HashMap<(Option<Sid>, Sid, i64, bool), Vec<Flake>> = HashMap::new();
    for f in flakes {
        groups
            .entry((f.g.clone(), f.s.clone(), f.t, f.op))
            .or_default()
            .push(f.clone());
    }

    let mut forward_rows: Vec<AnnotationForwardRow> = Vec::with_capacity(groups.len());
    let mut reverse_rows: Vec<AnnotationReverseRow> = Vec::with_capacity(groups.len());
    let mut skipped: u64 = 0;
    let mut max_t: i64 = 0;

    for ((bundle_g, ann_sid, t, op), bundle) in groups {
        match EdgeKey::from_reifies_facts(&bundle) {
            Ok(edge) => {
                // Cross-check: the graph the bundle was *asserted in*
                // (flake-level `g`) must match the graph the bundle
                // *reifies* (`EdgeKey.g`, derived from the optional
                // `f:reifiesGraph` flake). Mismatches indicate either
                // a malformed bundle (e.g. `f:reifiesGraph` missing on
                // a named-graph edge) or tampered history. Either way,
                // the safe move is the replay-validator pattern: skip
                // + count, never silently file the bundle under the
                // wrong graph in the arena.
                if edge.g != bundle_g {
                    skipped += 1;
                    tracing::warn!(
                        ann_sid = ?ann_sid,
                        t,
                        op,
                        bundle_graph = ?bundle_g,
                        edge_graph = ?edge.g,
                        "skipping bundle: f:reifiesGraph disagrees with flake-level graph"
                    );
                    continue;
                }
                if t > max_t {
                    max_t = t;
                }
                forward_rows.push(AnnotationForwardRow {
                    edge: edge.clone(),
                    ann: ann_sid.clone(),
                    t,
                    op,
                });
                reverse_rows.push(AnnotationReverseRow {
                    ann: ann_sid,
                    edge,
                    t,
                    op,
                });
            }
            Err(err) => {
                skipped += 1;
                // The replay validator pattern: log + count, never fail
                // the index build over a single malformed bundle.
                // Surrounding non-`f:reifies` metadata stays visible as
                // ordinary RDF (just without the attachment binding).
                tracing::warn!(
                    ?err,
                    ?ann_sid,
                    t,
                    op,
                    "skipping malformed f:reifies* bundle during arena build"
                );
            }
        }
    }

    forward_rows.sort_unstable_by(|a, b| {
        a.edge
            .cmp(&b.edge)
            .then_with(|| a.ann.cmp(&b.ann))
            .then_with(|| a.t.cmp(&b.t))
            .then_with(|| a.op.cmp(&b.op))
    });
    reverse_rows.sort_unstable_by(|a, b| {
        a.ann
            .cmp(&b.ann)
            .then_with(|| a.edge.cmp(&b.edge))
            .then_with(|| a.t.cmp(&b.t))
            .then_with(|| a.op.cmp(&b.op))
    });

    let stats = compute_stats(&forward_rows, &reverse_rows);

    let forward_leaves = build_forward_leaves(&forward_rows, target_rows_per_leaf);
    let reverse_leaves = build_reverse_leaves(&reverse_rows, target_rows_per_leaf);

    ArenaBuildOutput {
        forward_leaves,
        reverse_leaves,
        max_t,
        stats,
        skipped_bundles: skipped,
    }
}

/// `distinct_edges` and `distinct_annotations` count **live**
/// attachments only — `(edge, ann)` pairs whose latest event is
/// `op = true`. The forward slice is already sorted by
/// `(edge, ann, t, op)` (caller of [`build_arenas_from_flakes`]
/// guarantees this), so the last row in each `(edge, ann)` run is
/// the latest event for that pair. Counting "any row" would
/// overstate live state after retractions; see the field docs on
/// [`AnnotationStats`].
/// Build forward + reverse arenas from a stream of pre-decoded
/// attachment events.
///
/// Bypasses the bundle-reconstruction step that
/// [`build_arenas_from_flakes`] performs — callers that already
/// hold `(EdgeKey, ann_sid, t, op)` tuples (e.g.
/// `AttachmentNovelty.iter_*`, or any source that has already
/// validated `f:reifiesGraph` agreement) hand them in directly. Each
/// input event maps to one forward row + one reverse row.
///
/// Sort, chunk, and stats handling match the [`build_arenas_from_flakes`]
/// path. The output is ready for the same CAS-write + branch-encode
/// flow.
pub fn build_arenas_from_event_pairs(
    events: impl IntoIterator<Item = (EdgeKey, Sid, i64, bool)>,
    target_rows_per_leaf: usize,
) -> ArenaBuildOutput {
    let mut forward_rows: Vec<AnnotationForwardRow> = Vec::new();
    let mut reverse_rows: Vec<AnnotationReverseRow> = Vec::new();
    let mut max_t: i64 = 0;
    for (edge, ann, t, op) in events {
        if t > max_t {
            max_t = t;
        }
        forward_rows.push(AnnotationForwardRow {
            edge: edge.clone(),
            ann: ann.clone(),
            t,
            op,
        });
        reverse_rows.push(AnnotationReverseRow { ann, edge, t, op });
    }

    forward_rows.sort_unstable_by(|a, b| {
        a.edge
            .cmp(&b.edge)
            .then_with(|| a.ann.cmp(&b.ann))
            .then_with(|| a.t.cmp(&b.t))
            .then_with(|| a.op.cmp(&b.op))
    });
    reverse_rows.sort_unstable_by(|a, b| {
        a.ann
            .cmp(&b.ann)
            .then_with(|| a.edge.cmp(&b.edge))
            .then_with(|| a.t.cmp(&b.t))
            .then_with(|| a.op.cmp(&b.op))
    });

    let stats = compute_stats(&forward_rows, &reverse_rows);
    let forward_leaves = build_forward_leaves(&forward_rows, target_rows_per_leaf);
    let reverse_leaves = build_reverse_leaves(&reverse_rows, target_rows_per_leaf);

    ArenaBuildOutput {
        forward_leaves,
        reverse_leaves,
        max_t,
        stats,
        skipped_bundles: 0,
    }
}

fn compute_stats(
    forward: &[AnnotationForwardRow],
    reverse: &[AnnotationReverseRow],
) -> AnnotationStats {
    use fluree_db_core::FlakeValue;
    use std::collections::HashSet;

    // Walk the rows once. For each live `(edge, ann)` pair (the
    // last-in-group row where `op == true`), count one row per
    // optional-slot the edge carries — multiple annotations on the
    // same named-graph edge contribute separate `f:reifiesGraph`
    // rows. Distinct-value counters dedupe via `HashSet`.
    let mut live_edges: HashSet<&EdgeKey> = HashSet::new();
    let mut live_anns: HashSet<&Sid> = HashSet::new();
    let mut subjects: HashSet<&Sid> = HashSet::new();
    let mut predicates: HashSet<&Sid> = HashSet::new();
    let mut objects: HashSet<&FlakeValue> = HashSet::new();
    let mut graphs: HashSet<&Sid> = HashSet::new();
    let mut langs: HashSet<&str> = HashSet::new();
    let mut list_indices: HashSet<i32> = HashSet::new();
    let mut graph_rows: u64 = 0;
    let mut lang_rows: u64 = 0;
    let mut list_index_rows: u64 = 0;
    let mut live_pairs: u64 = 0;
    // Per-optional-slot ann-SID sets — the right denominator for
    // `<known_ann> f:reifies<slot> ?v` BoundSubject estimates.
    // Under the v1 invariant each set's size equals the slot's row
    // count; under multi-target it can be smaller.
    let mut graph_anns: HashSet<&Sid> = HashSet::new();
    let mut lang_anns: HashSet<&Sid> = HashSet::new();
    let mut list_index_anns: HashSet<&Sid> = HashSet::new();

    for i in 0..forward.len() {
        let last_in_group = i + 1 == forward.len()
            || forward[i].edge != forward[i + 1].edge
            || forward[i].ann != forward[i + 1].ann;
        if !(last_in_group && forward[i].op) {
            continue;
        }
        let edge = &forward[i].edge;
        let ann = &forward[i].ann;
        live_edges.insert(edge);
        live_anns.insert(ann);
        live_pairs += 1;

        // Distinct values are per-edge (HashSet dedupes natural
        // duplicates from parallel annotations).
        subjects.insert(&edge.s);
        predicates.insert(&edge.p);
        objects.insert(&edge.o);
        if let Some(g) = &edge.g {
            graphs.insert(g);
        }
        if let Some(lang) = &edge.lang {
            langs.insert(lang.as_str());
        }
        if let Some(i) = edge.list_i {
            list_indices.insert(i);
        }

        // Row counts for optional slots are per `(edge, ann)` pair
        // — each annotation on a named-graph edge contributes its
        // own `f:reifiesGraph` row. Counting per distinct edge would
        // under-report when parallel annotations share an endpoint.
        // The per-slot ann-SID sets dedupe naturally for the
        // multi-target anomaly case.
        if edge.g.is_some() {
            graph_rows += 1;
            graph_anns.insert(ann);
        }
        if edge.lang.is_some() {
            lang_rows += 1;
            lang_anns.insert(ann);
        }
        if edge.list_i.is_some() {
            list_index_rows += 1;
            list_index_anns.insert(ann);
        }
    }

    // `f:reifiesDatatype` is *not* synthesized from the arena. The
    // arena reconstructs `EdgeKey.dt` from the flake-level dt of
    // the `f:reifiesObject` row, so the on-wire `f:reifiesDatatype`
    // predicate may have zero rows (JSON-LD-compatible cascade) or
    // one-per-annotation (full bundle path) and we cannot tell
    // which from the arena state alone. Reporting a synthesized
    // count would let `merge_annotation_stats` overwrite the real
    // `IndexStats.properties` HLL with a phantom value. Leave the
    // datatype counters at zero so the planner falls back to the
    // HLL.

    AnnotationStats {
        forward_rows: forward.len() as u64,
        reverse_rows: reverse.len() as u64,
        distinct_edges: live_edges.len() as u64,
        distinct_annotations: live_anns.len() as u64,
        live_attachment_pairs: live_pairs,
        distinct_reified_subjects: subjects.len() as u64,
        distinct_reified_predicates: predicates.len() as u64,
        distinct_reified_objects: objects.len() as u64,
        reifies_graph_rows: graph_rows,
        distinct_reified_graphs: graphs.len() as u64,
        distinct_graph_anns: graph_anns.len() as u64,
        reifies_datatype_rows: 0,
        distinct_reified_datatypes: 0,
        reifies_lang_rows: lang_rows,
        distinct_reified_langs: langs.len() as u64,
        distinct_lang_anns: lang_anns.len() as u64,
        reifies_list_index_rows: list_index_rows,
        distinct_reified_list_indices: list_indices.len() as u64,
        distinct_list_index_anns: list_index_anns.len() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::super::builder::DEFAULT_TARGET_ROWS_PER_LEAF;
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};
    use fluree_vocab::db as db_predicates;

    fn ns_fluree_db() -> u16 {
        fluree_vocab::namespaces::FLUREE_DB
    }

    fn ann_sid(name: &str) -> Sid {
        Sid::new(20, name)
    }

    fn ref_sid(name: &str) -> Sid {
        Sid::new(11, name)
    }

    fn p_reifies(suffix: &str) -> Sid {
        Sid::new(ns_fluree_db(), suffix)
    }

    fn id_dt() -> Sid {
        fluree_db_core::id_datatype_sid()
    }

    /// Build a JSON-LD-compatible 3-flake bundle (subject, predicate,
    /// object) for ann `ann_name` reifying edge `(ref_sid("alice"),
    /// ref_sid("worksFor"), ref_sid("acme"))` at `(t, op)`.
    fn make_bundle(ann_name: &str, t: i64, op: bool) -> Vec<Flake> {
        let ann = ann_sid(ann_name);
        vec![
            Flake::new(
                ann.clone(),
                p_reifies(db_predicates::REIFIES_SUBJECT),
                FlakeValue::Ref(ref_sid("alice")),
                id_dt(),
                t,
                op,
                None,
            ),
            Flake::new(
                ann.clone(),
                p_reifies(db_predicates::REIFIES_PREDICATE),
                FlakeValue::Ref(ref_sid("worksFor")),
                id_dt(),
                t,
                op,
                None,
            ),
            Flake::new(
                ann,
                p_reifies(db_predicates::REIFIES_OBJECT),
                FlakeValue::Ref(ref_sid("acme")),
                id_dt(),
                t,
                op,
                None,
            ),
        ]
    }

    #[test]
    fn empty_input_produces_zero_rows() {
        let out = build_arenas_from_flakes(&[], DEFAULT_TARGET_ROWS_PER_LEAF);
        assert!(out.forward_leaves.is_empty());
        assert!(out.reverse_leaves.is_empty());
        assert_eq!(out.max_t, 0);
        assert_eq!(out.stats, AnnotationStats::default());
        assert_eq!(out.skipped_bundles, 0);
    }

    #[test]
    fn single_assert_bundle_produces_one_forward_and_one_reverse_row() {
        let flakes = make_bundle("ann_1", 5, true);
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.forward_leaves.len(), 1);
        assert_eq!(out.reverse_leaves.len(), 1);
        assert_eq!(out.forward_leaves[0].0.row_count, 1);
        assert_eq!(out.reverse_leaves[0].0.row_count, 1);
        assert_eq!(out.max_t, 5);
        assert_eq!(out.stats.forward_rows, 1);
        assert_eq!(out.stats.distinct_edges, 1);
        assert_eq!(out.stats.distinct_annotations, 1);
        assert_eq!(out.skipped_bundles, 0);
    }

    #[test]
    fn assert_then_retract_same_ann_emits_two_rows_but_zero_live() {
        // Two events on the same (edge, ann): attached then retracted.
        // Both rows survive (history queries need them), but the pair
        // is no longer live and must not contribute to live-state
        // stats.
        let mut flakes = make_bundle("ann_1", 5, true);
        flakes.extend(make_bundle("ann_1", 7, false));
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.stats.forward_rows, 2);
        assert_eq!(out.stats.reverse_rows, 2);
        assert_eq!(
            out.stats.distinct_edges, 0,
            "edge attached then retracted is no longer live"
        );
        assert_eq!(
            out.stats.distinct_annotations, 0,
            "ann_1 attached then retracted is no longer live"
        );
        assert_eq!(out.max_t, 7);
    }

    #[test]
    fn distinct_stats_count_only_currently_live_pairs() {
        // ann_a attached then retracted → not live;
        // ann_b attached → live. Stats reflect live state, not history.
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", 1, true));
        flakes.extend(make_bundle("ann_a", 2, false));
        flakes.extend(make_bundle("ann_b", 3, true));
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.stats.forward_rows, 3, "all events kept for history");
        assert_eq!(out.stats.distinct_edges, 1, "edge with ann_b is live");
        assert_eq!(out.stats.distinct_annotations, 1, "only ann_b is live");
    }

    #[test]
    fn bundle_with_mismatched_flake_graph_is_skipped() {
        // Manually build a bundle whose flakes are asserted in a
        // named graph but whose `f:reifiesGraph` is omitted (so
        // `EdgeKey::from_reifies_facts` infers default-graph). This
        // is the malformed shape the cross-check guards against —
        // accepting it would file the annotation under the wrong
        // graph in the arena.
        let ann = ann_sid("ann_x");
        let bundle_graph = Some(ref_sid("graph_a"));
        let mk = |p: &str, o: FlakeValue, dt: Sid| {
            // `Flake::new_in_graph` to mark these as living in
            // graph_a.
            Flake::new_in_graph(
                bundle_graph.clone().unwrap(),
                ann.clone(),
                p_reifies(p),
                o,
                dt,
                1,
                true,
                None,
            )
        };
        let flakes = vec![
            mk(
                db_predicates::REIFIES_SUBJECT,
                FlakeValue::Ref(ref_sid("alice")),
                id_dt(),
            ),
            mk(
                db_predicates::REIFIES_PREDICATE,
                FlakeValue::Ref(ref_sid("worksFor")),
                id_dt(),
            ),
            mk(
                db_predicates::REIFIES_OBJECT,
                FlakeValue::Ref(ref_sid("acme")),
                id_dt(),
            ),
            // Note: NO `f:reifiesGraph` flake. EdgeKey.g will decode
            // as None (default graph), but the bundle was asserted in
            // graph_a. Cross-check must reject.
        ];
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.skipped_bundles, 1);
        assert_eq!(out.stats.forward_rows, 0);
        assert!(out.forward_leaves.is_empty());
        assert!(out.reverse_leaves.is_empty());
    }

    #[test]
    fn bundle_with_matching_named_graph_is_accepted() {
        // Same as above, but with the `f:reifiesGraph` flake present
        // and pointing to graph_a. Cross-check passes; the row lands
        // in the arena with `EdgeKey.g = Some(graph_a)`.
        let ann = ann_sid("ann_y");
        let g = ref_sid("graph_a");
        let mk = |p: &str, o: FlakeValue, dt: Sid| {
            Flake::new_in_graph(g.clone(), ann.clone(), p_reifies(p), o, dt, 1, true, None)
        };
        let flakes = vec![
            mk(
                db_predicates::REIFIES_GRAPH,
                FlakeValue::Ref(g.clone()),
                id_dt(),
            ),
            mk(
                db_predicates::REIFIES_SUBJECT,
                FlakeValue::Ref(ref_sid("alice")),
                id_dt(),
            ),
            mk(
                db_predicates::REIFIES_PREDICATE,
                FlakeValue::Ref(ref_sid("worksFor")),
                id_dt(),
            ),
            mk(
                db_predicates::REIFIES_OBJECT,
                FlakeValue::Ref(ref_sid("acme")),
                id_dt(),
            ),
        ];
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.skipped_bundles, 0);
        assert_eq!(out.stats.forward_rows, 1);
        assert_eq!(out.stats.distinct_edges, 1);
        // Recovered EdgeKey carries the named graph.
        let leaf_blob = &out.forward_leaves[0].1;
        let leaf =
            crate::annotation_arena::format::AnnotationForwardLeaf::decode(leaf_blob).unwrap();
        assert_eq!(leaf.rows[0].edge.g, Some(g));
    }

    #[test]
    fn malformed_bundle_skipped_with_counter() {
        // Bundle missing f:reifiesSubject. Surrounding good bundle
        // must still produce a row.
        let good = make_bundle("ann_good", 1, true);
        let bad = vec![Flake::new(
            ann_sid("ann_bad"),
            p_reifies(db_predicates::REIFIES_PREDICATE),
            FlakeValue::Ref(ref_sid("worksFor")),
            id_dt(),
            1,
            true,
            None,
        )];
        let mut all = good;
        all.extend(bad);
        let out = build_arenas_from_flakes(&all, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.skipped_bundles, 1);
        assert_eq!(out.stats.forward_rows, 1, "good bundle still emitted");
    }

    #[test]
    fn multiple_bundles_sort_into_arena_order() {
        // Three bundles, distinct edges. The forward arena sort key is
        // (edge, ann, t, op); reverse is (ann, edge, t, op). We assert
        // the sort by inspecting the first/last keys of the produced
        // single leaf each.
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_3", 1, true));
        flakes.extend(make_bundle("ann_1", 2, true));
        flakes.extend(make_bundle("ann_2", 3, true));
        // make_bundle reuses the same edge for every annotation, so
        // forward arena rows differ only in the annotation Sid.
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);
        assert_eq!(out.forward_leaves.len(), 1);
        let summary = &out.forward_leaves[0].0;
        // Sorted ascending by ann_sid: ann_1 < ann_2 < ann_3.
        assert_eq!(summary.first_ann, ann_sid("ann_1"));
        assert_eq!(summary.last_ann, ann_sid("ann_3"));

        let rev = &out.reverse_leaves[0].0;
        assert_eq!(rev.first_ann, ann_sid("ann_1"));
        assert_eq!(rev.last_ann, ann_sid("ann_3"));
    }

    #[test]
    fn rows_chunk_into_multiple_leaves_when_exceeding_target() {
        // 5 distinct annotations against the same edge → 5 forward
        // rows; with target=2 → 3 leaves.
        let mut flakes = Vec::new();
        for i in 0..5 {
            flakes.extend(make_bundle(&format!("ann_{i}"), i64::from(i) + 1, true));
        }
        let out = build_arenas_from_flakes(&flakes, 2);
        assert_eq!(out.forward_leaves.len(), 3);
        let total: u64 = out.forward_leaves.iter().map(|(s, _)| s.row_count).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn forward_and_reverse_blobs_decode_back_to_original_rows() {
        use crate::annotation_arena::format::{AnnotationForwardLeaf, AnnotationReverseLeaf};

        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", 1, true));
        flakes.extend(make_bundle("ann_b", 2, true));
        flakes.extend(make_bundle("ann_a", 3, false));
        let out = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);

        let mut decoded_forward = Vec::new();
        for (_, blob) in &out.forward_leaves {
            decoded_forward.extend(AnnotationForwardLeaf::decode(blob).unwrap().rows);
        }
        let mut decoded_reverse = Vec::new();
        for (_, blob) in &out.reverse_leaves {
            decoded_reverse.extend(AnnotationReverseLeaf::decode(blob).unwrap().rows);
        }
        assert_eq!(decoded_forward.len(), 3);
        assert_eq!(decoded_reverse.len(), 3);

        // Cross-check: every forward (edge, ann, t, op) appears in
        // reverse with the same fields.
        for fwd in &decoded_forward {
            let found = decoded_reverse.iter().any(|rev| {
                rev.edge == fwd.edge && rev.ann == fwd.ann && rev.t == fwd.t && rev.op == fwd.op
            });
            assert!(found, "forward row missing from reverse: {fwd:?}");
        }
    }

    #[test]
    fn event_pairs_path_matches_flakes_path() {
        // Building from pre-decoded events must produce the same
        // arena rows as the bundle-driven path. Same input data, two
        // forms.
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", 1, true));
        flakes.extend(make_bundle("ann_b", 2, true));
        let from_flakes = build_arenas_from_flakes(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF);

        // Equivalent pre-decoded events. `make_bundle` reuses the
        // same edge for each annotation, so we reconstruct it from
        // the bundle once.
        let edge = EdgeKey::from_reifies_facts(&make_bundle("ann_a", 1, true)).unwrap();
        let events = vec![
            (edge.clone(), ann_sid("ann_a"), 1, true),
            (edge, ann_sid("ann_b"), 2, true),
        ];
        let from_events = build_arenas_from_event_pairs(events, DEFAULT_TARGET_ROWS_PER_LEAF);

        assert_eq!(from_flakes.stats, from_events.stats);
        assert_eq!(from_flakes.max_t, from_events.max_t);
        assert_eq!(
            from_flakes.forward_leaves.len(),
            from_events.forward_leaves.len()
        );
    }

    #[test]
    fn event_pairs_empty_input_produces_empty_arena() {
        let out = build_arenas_from_event_pairs(std::iter::empty(), DEFAULT_TARGET_ROWS_PER_LEAF);
        assert!(out.forward_leaves.is_empty());
        assert!(out.reverse_leaves.is_empty());
        assert_eq!(out.max_t, 0);
        assert_eq!(out.skipped_bundles, 0);
    }
}
