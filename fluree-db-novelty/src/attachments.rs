//! Edge-annotation attachment overlay (M1 — novelty only).
//!
//! Mirrors flake-level [`Novelty`](crate::Novelty) for the
//! attachment side: an in-memory, derived index of which annotations
//! are attached to which base edges. Populated by observing
//! `f:reifies*` system flakes that flow through the novelty pipeline,
//! either at apply-commit time (live transactions) or warmup
//! (rehydrating a snapshot from prior commits).
//!
//! `AttachmentNovelty` is **derived state** — never primary truth. The
//! durable encoding lives in the seven `f:reifies*` flakes themselves
//! (see `fluree_db_core::edge::EdgeKey::to_reifies_facts`). If the
//! attachment overlay disagrees with the underlying flakes, the
//! flakes win. M2 will replace this in-memory map with a binary
//! arena, keeping the durable encoding unchanged.
//!
//! Two indexes are maintained in parallel:
//!
//! - `forward: EdgeKey -> Vec<ForwardRow>` — for edge-rooted lookups.
//!   "Given this base edge, which annotations point at it?"
//! - `reverse: Sid -> Vec<ReverseRow>` — for annotation-rooted
//!   lookups. "Given this annotation subject, which edge does it
//!   reify?"
//!
//! Each row carries `(t, op)` so history queries can replay the
//! attachment lifecycle without re-decoding the flakes.

use fluree_db_core::edge::EdgeKey;
use fluree_db_core::namespaces::is_reserved_reifies_predicate;
use fluree_db_core::{Flake, Sid};
use std::collections::BTreeMap;

use crate::error::Result;

/// One forward-direction row: an annotation attached to an edge.
///
/// Stored under [`AttachmentNovelty::forward`] keyed by the edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForwardRow {
    /// The annotation subject SID.
    pub ann: Sid,
    /// Transaction time of this attachment event.
    pub t: i64,
    /// `true` = attachment asserted, `false` = retracted.
    pub op: bool,
}

/// One reverse-direction row: an edge an annotation reifies.
///
/// Stored under [`AttachmentNovelty::reverse`] keyed by the annotation
/// SID. Carries the full `EdgeKey` (graph + s + p + o + dt + lang) so
/// downstream operators can re-probe the base fact indexes for
/// visibility checks without an additional lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReverseRow {
    /// The reified base edge.
    pub edge: EdgeKey,
    /// Transaction time of this attachment event.
    pub t: i64,
    /// `true` = attachment asserted, `false` = retracted.
    pub op: bool,
}

/// In-memory attachment overlay paralleling [`Novelty`](crate::Novelty).
///
/// Updated by [`Self::observe_flakes`] from the same flake stream that
/// `Novelty::apply_commit` accepts (post-dedup). Caches the
/// `has_annotations` gate so cascade-retract paths can short-circuit
/// without walking the maps when the ledger has never seen an
/// annotation.
#[derive(Clone, Debug, Default)]
pub struct AttachmentNovelty {
    forward: BTreeMap<EdgeKey, Vec<ForwardRow>>,
    reverse: BTreeMap<Sid, Vec<ReverseRow>>,
    /// `true` once *any* `f:reifies*` bundle has been observed (asserted
    /// or retracted, doesn't matter — the cascade gate cares about
    /// "could this snapshot ever have annotations").
    has_annotations: bool,
    /// Cumulative count of malformed `f:reifies*` bundles observed by
    /// [`Self::observe_flakes`] over the lifetime of this overlay.
    /// Per the design contract, malformed bundles are skipped + warned
    /// rather than erroring out so a single corrupt event in replay
    /// can't block the rest of the ledger from loading. Operators
    /// scrape this to detect data-corruption / replay-anomaly
    /// signals; a non-zero value indicates either a software bug in
    /// the writer or an externally-tampered commit history.
    observed_malformed_bundles: u64,
}

impl AttachmentNovelty {
    /// Create an empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// True iff at least one `f:reifies*` flake has been observed.
    ///
    /// Cascade fast-path: when both `Novelty::attachments.has_annotations()`
    /// and the indexed arena both report `false`, plain edge retracts
    /// can skip the attachment-cascade lookup entirely.
    #[inline]
    pub fn has_annotations(&self) -> bool {
        self.has_annotations
    }

    /// Cumulative count of malformed `f:reifies*` bundles observed
    /// over this overlay's lifetime. See the field docs on
    /// `observed_malformed_bundles` for the operational signal.
    /// Always `0` on a healthy ledger.
    #[inline]
    pub fn observed_malformed_bundle_count(&self) -> u64 {
        self.observed_malformed_bundles
    }

    /// Iterator over annotation SIDs **currently attached** to `edge`,
    /// where "current" is evaluated against `as_of_t`: only events
    /// with `t <= as_of_t` are considered, and the latest such event
    /// for each annotation must be `op == true`.
    ///
    /// This is the time-travel-correct read used by query and
    /// hydration paths — the formatter passes `self.db.t` so a
    /// historical view sees the attachment state as of that
    /// transaction, not the live latest.
    pub fn current_annotations_for_at<'a>(
        &'a self,
        edge: &'a EdgeKey,
        as_of_t: i64,
    ) -> impl Iterator<Item = Sid> + 'a {
        self.forward
            .get(edge)
            .map(|rows| latest_assertions_at::<_, _>(rows.iter(), |r| (&r.ann, r.t, r.op), as_of_t))
            .into_iter()
            .flatten()
            .cloned()
    }

    /// Iterator over annotation SIDs currently attached to `edge`
    /// against the **live** overlay state (i.e., the latest event
    /// over all `t`).
    ///
    /// Used by transactor staging where the relevant state is
    /// always "everything committed before this transaction" — the
    /// novelty's attachment rows only carry post-commit events with
    /// `t <= ledger.t()` by construction, so the live and as-of
    /// reads coincide.
    ///
    /// **Read paths must use [`Self::current_annotations_for_at`]
    /// with an explicit `as_of_t`** — this method is for write-side
    /// callers only.
    pub fn current_annotations_for<'a>(
        &'a self,
        edge: &'a EdgeKey,
    ) -> impl Iterator<Item = Sid> + 'a {
        self.current_annotations_for_at(edge, i64::MAX)
    }

    /// Time-travel-correct counterpart of [`Self::current_targets_for`].
    pub fn current_targets_for_at<'a>(
        &'a self,
        ann: &'a Sid,
        as_of_t: i64,
    ) -> impl Iterator<Item = EdgeKey> + 'a {
        self.reverse
            .get(ann)
            .map(|rows| {
                latest_assertions_at::<_, _>(rows.iter(), |r| (&r.edge, r.t, r.op), as_of_t)
            })
            .into_iter()
            .flatten()
            .cloned()
    }

    /// Iterator over base [`EdgeKey`]s currently reified by
    /// annotation `ann` against the live overlay state. See
    /// [`Self::current_annotations_for`] for the time-travel-vs-
    /// live distinction.
    pub fn current_targets_for<'a>(&'a self, ann: &'a Sid) -> impl Iterator<Item = EdgeKey> + 'a {
        self.current_targets_for_at(ann, i64::MAX)
    }

    /// Iterator over the *full* attachment history of `ann` —
    /// every `(EdgeKey, t, op)` event, in row-stored order. Used by
    /// history-range queries that explicitly want to see attachment
    /// lifecycle alongside flake history.
    pub fn target_history<'a>(&'a self, ann: &'a Sid) -> impl Iterator<Item = &'a ReverseRow> + 'a {
        self.reverse
            .get(ann)
            .into_iter()
            .flat_map(|rows| rows.iter())
    }

    /// Iterator over the full attachment history of `edge` — every
    /// `(ann, t, op)` event for this edge, in row-stored order.
    pub fn forward_history<'a>(
        &'a self,
        edge: &'a EdgeKey,
    ) -> impl Iterator<Item = &'a ForwardRow> + 'a {
        self.forward
            .get(edge)
            .into_iter()
            .flat_map(|rows| rows.iter())
    }

    /// Collect every overlay event for `edge` as `(ann, t, op)` triples.
    ///
    /// Shaped to match the input format of
    /// `fluree_db_binary_index::annotation_arena::AnnotationArenaReader::current_annotations_merged`
    /// so callers in higher-layer crates can pass the slice directly.
    /// Empty when the overlay has no rows for the given edge.
    pub fn collect_forward_events(&self, edge: &EdgeKey) -> Vec<(Sid, i64, bool)> {
        self.forward
            .get(edge)
            .map(|rows| rows.iter().map(|r| (r.ann.clone(), r.t, r.op)).collect())
            .unwrap_or_default()
    }

    /// Collect every overlay event for `ann` as `(edge, t, op)` triples.
    /// Counterpart of [`Self::collect_forward_events`] for the reverse
    /// arena.
    pub fn collect_reverse_events(&self, ann: &Sid) -> Vec<(EdgeKey, i64, bool)> {
        self.reverse
            .get(ann)
            .map(|rows| rows.iter().map(|r| (r.edge.clone(), r.t, r.op)).collect())
            .unwrap_or_default()
    }

    /// Iterator over every overlay event as `(EdgeKey, ann, t, op)`
    /// tuples — the input shape of
    /// `fluree_db_binary_index::annotation_arena::build_arenas_from_event_pairs`.
    ///
    /// Used by the indexer when sealing a new arena: collect the full
    /// overlay state (or merge with the previous arena's events first)
    /// and feed straight into the arena builder. Walks `forward` so
    /// each `(edge, ann)` pair is yielded together with all its
    /// history rows in row-stored order.
    pub fn iter_event_pairs(&self) -> impl Iterator<Item = (EdgeKey, Sid, i64, bool)> + '_ {
        self.forward.iter().flat_map(|(edge, rows)| {
            rows.iter()
                .map(move |r| (edge.clone(), r.ann.clone(), r.t, r.op))
        })
    }

    /// Observe a slice of accepted flakes and update the overlay.
    ///
    /// Filters down to `f:reifies*` flakes, groups them by
    /// `(ann_sid, t, op)`, and decodes each group via
    /// [`EdgeKey::from_reifies_facts`]. A malformed bundle (missing
    /// required predicate, duplicate, or deferred shape) is **skipped
    /// with a `tracing::warn!` and counted** in
    /// `observed_malformed_bundles`. The non-`f:reifies*` flakes for
    /// the same annotation subject remain visible as ordinary RDF —
    /// only the attachment binding is dropped.
    ///
    /// **Why skip rather than error:** the contract in
    /// `EDGE_ANNOTATIONS_IMPL_PLAN.md` says replay validation should
    /// "Reject (skip + telemetry counter) any ann_sid that has a
    /// partial bundle." A single malformed bundle in commit replay
    /// (e.g. legacy data or a tampered commit) would otherwise block
    /// the ledger from loading. Operators detect data corruption
    /// post-hoc via [`Self::observed_malformed_bundle_count`].
    ///
    /// Caller contract: pass the **post-dedup** flake set that
    /// `Novelty::apply_commit` ultimately stored in the arena.
    /// Observing a deduped duplicate would create a phantom row.
    pub fn observe_flakes(&mut self, flakes: &[Flake]) -> Result<()> {
        if flakes.is_empty() {
            return Ok(());
        }

        // Group `f:reifies*` flakes into bundles keyed by
        // `(flake_graph, ann_sid, t, op)`. The flake-level graph is
        // part of the key because the writer convention is that
        // `f:reifies*` flakes for an edge in graph G are themselves
        // asserted in graph G; folding two graphs together at this
        // stage would let a pathological cross-graph collision merge
        // into a single (wrong-graph) bundle. Mirrors the arena
        // builder's grouping in
        // `fluree_db_binary_index::annotation_arena::bundle::build_arenas_from_flakes`
        // so the two paths agree on what counts as a malformed
        // bundle.
        let mut bundles: BTreeMap<(Option<Sid>, Sid, i64, bool), Vec<Flake>> = BTreeMap::new();
        for f in flakes {
            if !is_reserved_reifies_predicate(&f.p) {
                continue;
            }
            bundles
                .entry((f.g.clone(), f.s.clone(), f.t, f.op))
                .or_default()
                .push(f.clone());
        }

        if bundles.is_empty() {
            return Ok(());
        }

        for ((bundle_g, ann, t, op), bundle) in bundles {
            let edge = match EdgeKey::from_reifies_facts(&bundle) {
                Ok(edge) => edge,
                Err(e) => {
                    self.observed_malformed_bundles =
                        self.observed_malformed_bundles.saturating_add(1);
                    tracing::warn!(
                        ?ann,
                        t,
                        op,
                        error = %e,
                        cumulative_skipped = self.observed_malformed_bundles,
                        "skipping malformed f:reifies* bundle in novelty observer"
                    );
                    continue;
                }
            };

            // Cross-check: the graph the bundle was *asserted in*
            // (flake-level `g`) must match the graph the bundle
            // *reifies* (`EdgeKey.g`, derived from the optional
            // `f:reifiesGraph` flake). Mismatches indicate a
            // malformed bundle (e.g. `f:reifiesGraph` missing on a
            // named-graph edge, or a tampered commit) — file under
            // the wrong graph and the cascade fast-path can't find
            // the bundle on a base-edge retract.
            //
            // **Belt-and-suspenders.** The invariant now lives in
            // the decoder itself: `EdgeKey::from_reifies_facts`
            // returns `EdgeKeyDecodeError::GraphMismatch` when
            // `f:reifiesGraph` disagrees with the bundle's
            // flake-level `g`, and `MixedFlakeGraphs` when the
            // bundle's flakes don't share one graph. Since this
            // observer groups by `(g, ann, t, op)` before calling
            // the decoder, the inner slice is graph-uniform and
            // the decoder's `GraphMismatch` check already covers
            // every case this external check could fire on. The
            // external check is kept as defense-in-depth: if a
            // future refactor changes the decoder semantics or
            // skips the reconciliation, this branch catches the
            // regression with a structured warn that names both
            // graphs.
            if edge.g != bundle_g {
                self.observed_malformed_bundles = self.observed_malformed_bundles.saturating_add(1);
                tracing::warn!(
                    ?ann,
                    t,
                    op,
                    bundle_graph = ?bundle_g,
                    edge_graph = ?edge.g,
                    cumulative_skipped = self.observed_malformed_bundles,
                    "skipping bundle: f:reifiesGraph disagrees with flake-level graph"
                );
                continue;
            }

            self.forward
                .entry(edge.clone())
                .or_default()
                .push(ForwardRow {
                    ann: ann.clone(),
                    t,
                    op,
                });
            self.reverse
                .entry(ann)
                .or_default()
                .push(ReverseRow { edge, t, op });
            self.has_annotations = true;
        }

        Ok(())
    }

    /// Iterator over every `(edge, rows)` pair in the forward map.
    /// Diagnostic / test use — walks the entire overlay so callers
    /// must keep the cost in mind (linear in distinct edges).
    pub fn iter_forward(&self) -> impl Iterator<Item = (&EdgeKey, &Vec<ForwardRow>)> {
        self.forward.iter()
    }

    /// Iterator over every `(ann_sid, rows)` pair in the reverse map.
    /// Diagnostic / test counterpart of [`Self::iter_forward`].
    pub fn iter_reverse(&self) -> impl Iterator<Item = (&fluree_db_core::Sid, &Vec<ReverseRow>)> {
        self.reverse.iter()
    }

    /// Total number of forward rows across all edges. Diagnostic /
    /// telemetry-only — not a hot-path metric.
    pub fn forward_row_count(&self) -> usize {
        self.forward.values().map(Vec::len).sum()
    }

    /// Total number of reverse rows across all annotations. Diagnostic
    /// / telemetry-only.
    pub fn reverse_row_count(&self) -> usize {
        self.reverse.values().map(Vec::len).sum()
    }

    /// Number of distinct edges with at least one attachment row.
    pub fn distinct_edges(&self) -> usize {
        self.forward.len()
    }

    /// Number of distinct annotation subjects with at least one row.
    pub fn distinct_annotations(&self) -> usize {
        self.reverse.len()
    }
}

/// Walk a row sequence and yield each "other" position whose latest
/// `(t, op)` event with `t <= as_of_t` is currently asserted.
///
/// Generic over both row types so both `current_annotations_for_at`
/// and `current_targets_for_at` share the same implementation. Rows
/// with `t > as_of_t` are ignored entirely (so a future retract is
/// invisible to a past view, and vice-versa). Stable: when the same
/// `(other, t)` appears twice (impossible in practice but not
/// enforced by the type), the *last-encountered* `op` wins.
fn latest_assertions_at<'a, R, T>(
    rows: impl Iterator<Item = &'a R>,
    extract: impl Fn(&'a R) -> (&'a T, i64, bool),
    as_of_t: i64,
) -> impl Iterator<Item = &'a T>
where
    R: 'a,
    T: 'a + Ord + Clone,
{
    // Build a small map of "latest visible (t, op)" per `other`. A
    // BTreeMap keyed on the `other` side gives a deterministic
    // iteration order (good for tests and replay determinism).
    let mut latest: BTreeMap<&'a T, (i64, bool)> = BTreeMap::new();
    for row in rows {
        let (other, t, op) = extract(row);
        if t > as_of_t {
            continue;
        }
        latest
            .entry(other)
            .and_modify(|cur| {
                if t >= cur.0 {
                    *cur = (t, op);
                }
            })
            .or_insert((t, op));
    }
    latest
        .into_iter()
        .filter_map(|(other, (_t, op))| if op { Some(other) } else { None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::edge::EdgeKey;
    use fluree_db_core::{FlakeMeta, FlakeValue, Sid};

    fn sample_edge() -> EdgeKey {
        EdgeKey::from_flake(&Flake::new(
            Sid::new(13, "alice"),
            Sid::new(13, "worksFor"),
            FlakeValue::Ref(Sid::new(13, "acme")),
            fluree_db_core::edge::id_datatype_sid(),
            42,
            true,
            None,
        ))
    }

    fn ann_sid(name: &str) -> Sid {
        Sid::new(13, name)
    }

    #[test]
    fn empty_overlay_reports_no_annotations() {
        let overlay = AttachmentNovelty::new();
        assert!(!overlay.has_annotations());
        let edge = sample_edge();
        assert!(overlay.current_annotations_for(&edge).next().is_none());
    }

    #[test]
    fn observe_assertion_makes_attachment_visible() {
        let edge = sample_edge();
        let ann = ann_sid("ann1");
        let bundle = edge.to_reifies_facts(&ann, 5, true);

        let mut overlay = AttachmentNovelty::new();
        overlay.observe_flakes(&bundle).unwrap();
        assert!(overlay.has_annotations());

        let attached: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        assert_eq!(attached, vec![ann.clone()]);

        let targets: Vec<EdgeKey> = overlay.current_targets_for(&ann).collect();
        assert_eq!(targets, vec![edge]);
    }

    #[test]
    fn assert_then_retract_clears_current_attachment_but_keeps_history() {
        let edge = sample_edge();
        let ann = ann_sid("ann1");

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 7, false))
            .unwrap();

        assert!(
            overlay.current_annotations_for(&edge).next().is_none(),
            "retraction at t=7 must hide the t=5 attachment from current view"
        );
        // History still sees both events.
        let history: Vec<(i64, bool)> = overlay
            .target_history(&ann)
            .map(|row| (row.t, row.op))
            .collect();
        assert_eq!(history, vec![(5, true), (7, false)]);
        // has_annotations remains sticky — the index has been touched.
        assert!(overlay.has_annotations());
    }

    #[test]
    fn current_annotations_for_at_respects_as_of_t() {
        // Time-travel correctness: an annotation asserted at t=5 and
        // retracted at t=7 must be visible to a view at t=5 or t=6,
        // hidden at t=7+, and not yet visible at t=4.
        let edge = sample_edge();
        let ann = ann_sid("ann_a");

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 7, false))
            .unwrap();

        // Before any event: no rows visible.
        let before: Vec<Sid> = overlay.current_annotations_for_at(&edge, 4).collect();
        assert!(before.is_empty(), "view at t=4 must not see t=5 assertion");

        // At t=5 and t=6: assertion visible.
        let at5: Vec<Sid> = overlay.current_annotations_for_at(&edge, 5).collect();
        assert_eq!(at5, vec![ann.clone()], "view at t=5 must see the assertion");
        let at6: Vec<Sid> = overlay.current_annotations_for_at(&edge, 6).collect();
        assert_eq!(at6, vec![ann.clone()], "view at t=6 must still see it");

        // At t=7+: retract takes effect.
        let at7: Vec<Sid> = overlay.current_annotations_for_at(&edge, 7).collect();
        assert!(at7.is_empty(), "view at t=7 must see retraction");
        let at_max: Vec<Sid> = overlay
            .current_annotations_for_at(&edge, i64::MAX)
            .collect();
        assert!(at_max.is_empty(), "live view sees retraction too");

        // Live `current_annotations_for` agrees with as-of MAX.
        let live: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        assert!(live.is_empty());
    }

    #[test]
    fn current_targets_for_at_respects_as_of_t() {
        // Counterpart for the reverse map.
        let edge = sample_edge();
        let ann = ann_sid("ann_a");

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();

        let at_4: Vec<EdgeKey> = overlay.current_targets_for_at(&ann, 4).collect();
        assert!(at_4.is_empty());
        let at_5: Vec<EdgeKey> = overlay.current_targets_for_at(&ann, 5).collect();
        assert_eq!(at_5, vec![edge]);
    }

    #[test]
    fn parallel_annotations_on_one_edge_both_visible() {
        // Two distinct annotation subjects attached to the same edge.
        let edge = sample_edge();
        let ann_a = ann_sid("ann_A");
        let ann_b = ann_sid("ann_B");

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann_a, 5, true))
            .unwrap();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann_b, 6, true))
            .unwrap();

        let mut attached: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        attached.sort();
        assert_eq!(attached, vec![ann_a, ann_b]);
    }

    #[test]
    fn observe_ignores_non_reifies_flakes_silently() {
        let edge = sample_edge();
        let ann = ann_sid("ann1");

        let mut overlay = AttachmentNovelty::new();
        // Annotation metadata flake (e.g. `ann ex:role "Engineer"`)
        // accompanies the bundle but must not affect the overlay.
        let mut all_flakes = edge.to_reifies_facts(&ann, 5, true);
        all_flakes.push(Flake::new(
            ann.clone(),
            Sid::new(13, "role"),
            FlakeValue::String("Engineer".into()),
            Sid::new(2, "string"),
            5,
            true,
            None,
        ));
        overlay.observe_flakes(&all_flakes).unwrap();

        // The metadata flake should be silently ignored — only the
        // bundle drives the overlay.
        assert_eq!(overlay.distinct_edges(), 1);
        assert_eq!(overlay.distinct_annotations(), 1);
    }

    #[test]
    fn observe_no_op_on_empty_input() {
        let mut overlay = AttachmentNovelty::new();
        overlay.observe_flakes(&[]).unwrap();
        assert!(!overlay.has_annotations());
    }

    #[test]
    fn observe_skips_and_counts_graph_mismatch_bundle() {
        // A bundle whose *flake-level* graph (the graph the
        // f:reifies* flakes were asserted in) doesn't match the
        // bundle's *decoded* graph (from the optional f:reifiesGraph
        // flake) is malformed. Without this cross-check, a cross-
        // graph collision could merge into a single (wrong-graph)
        // bundle and file the attachment under the wrong edge —
        // breaking the cascade fast-path on base-edge retract.
        // Mirrors the arena builder's guard in
        // `fluree_db_binary_index::annotation_arena::bundle::build_arenas_from_flakes`.
        let edge = sample_edge(); // default-graph edge
        let ann = ann_sid("ann_x");
        // Build a default-graph bundle (correct shape: edge.g ==
        // None, no f:reifiesGraph flake), then re-graph every flake
        // to graph G_a — so flake-level g = Some(G_a) but decoded
        // EdgeKey.g = None. Mismatch.
        let g_a = Sid::new(13, "graph_a");
        let bundle_default: Vec<Flake> = edge.to_reifies_facts(&ann, 5, true);
        let bundle_mismatch: Vec<Flake> = bundle_default
            .into_iter()
            .map(|f| Flake::new_in_graph(g_a.clone(), f.s, f.p, f.o, f.dt, f.t, f.op, f.m))
            .collect();

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&bundle_mismatch)
            .expect("graph-mismatch bundle skipped, not an error");
        assert!(!overlay.has_annotations(), "no rows landed");
        assert_eq!(
            overlay.observed_malformed_bundle_count(),
            1,
            "graph-mismatch bundle counts as malformed"
        );
    }

    #[test]
    fn observe_skips_and_counts_malformed_bundle() {
        // Strip a required predicate from the bundle — decoder rejects.
        // Per the design contract, observe_flakes now SKIPS + warns +
        // counts rather than erroring out, so a single corrupt event
        // in replay can't block the rest of the ledger from loading.
        let edge = sample_edge();
        let ann = ann_sid("ann1");
        let mut bundle = edge.to_reifies_facts(&ann, 5, true);
        bundle.retain(|f| !fluree_db_core::namespaces::is_reifies_subject(&f.p));

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&bundle)
            .expect("observe_flakes skips malformed bundles instead of erroring");
        // Overlay's attachment maps stay empty — only the malformed
        // bundle was in the input.
        assert!(!overlay.has_annotations());
        // The cumulative counter ticks so operators can detect the
        // signal post-hoc.
        assert_eq!(overlay.observed_malformed_bundle_count(), 1);
    }

    #[test]
    fn collect_forward_events_returns_arena_reader_input_shape() {
        // Two events on the same edge: ann_a attached at t=5, retracted
        // at t=7. `collect_forward_events` returns both as
        // (ann, t, op) triples in row-stored order — ready to hand
        // straight to AnnotationArenaReader::current_annotations_merged.
        let edge = sample_edge();
        let ann = ann_sid("ann_a");
        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 7, false))
            .unwrap();

        let events = overlay.collect_forward_events(&edge);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], (ann.clone(), 5, true));
        assert_eq!(events[1], (ann, 7, false));
    }

    #[test]
    fn collect_reverse_events_mirrors_forward_collector() {
        let edge = sample_edge();
        let ann = ann_sid("ann_a");
        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();

        let events = overlay.collect_reverse_events(&ann);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, edge);
        assert_eq!(events[0].1, 5);
        assert!(events[0].2);
    }

    #[test]
    fn collect_events_empty_for_unknown_keys() {
        let overlay = AttachmentNovelty::new();
        assert!(overlay.collect_forward_events(&sample_edge()).is_empty());
        assert!(overlay
            .collect_reverse_events(&ann_sid("never_seen"))
            .is_empty());
    }

    #[test]
    fn observe_handles_named_graph_and_lang_bundles() {
        let mut base = Flake::new(
            Sid::new(13, "alice"),
            Sid::new(13, "label"),
            FlakeValue::String("Engineer".into()),
            Sid::new(2, "string"),
            42,
            true,
            None,
        );
        base.g = Some(Sid::new(13, "graph_a"));
        base.m = Some(FlakeMeta {
            lang: Some("fr".into()),
            i: None,
        });
        let edge = EdgeKey::from_flake(&base);
        let ann = ann_sid("ann_named");

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&edge.to_reifies_facts(&ann, 5, true))
            .unwrap();

        let attached: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        assert_eq!(attached, vec![ann]);
    }

    #[test]
    fn malformed_bundle_skipped_with_warn_and_counter_bump() {
        // A bundle missing the required `f:reifiesSubject` flake
        // (decoder rejects with `EdgeKeyDecodeError::Missing`) used
        // to error out the caller's commit. Per the design contract
        // it now skips + warns + bumps the cumulative counter so a
        // single corrupt event in replay can't block the rest of
        // the ledger from loading.
        use fluree_db_core::edge::id_datatype_sid;
        use fluree_vocab::db as p;
        use fluree_vocab::namespaces::FLUREE_DB;

        let ann = ann_sid("ann_bad");
        let id_dt = id_datatype_sid();

        // Bundle with f:reifiesPredicate + f:reifiesObject only —
        // missing f:reifiesSubject. `EdgeKey::from_reifies_facts`
        // returns `Missing("f:reifiesSubject")`.
        let malformed: Vec<Flake> = vec![
            Flake::new(
                ann.clone(),
                Sid::new(FLUREE_DB, p::REIFIES_PREDICATE),
                FlakeValue::Ref(Sid::new(13, "worksFor")),
                id_dt.clone(),
                7,
                true,
                None,
            ),
            Flake::new(
                ann.clone(),
                Sid::new(FLUREE_DB, p::REIFIES_OBJECT),
                FlakeValue::Ref(Sid::new(13, "acme")),
                id_dt,
                7,
                true,
                None,
            ),
        ];

        // A second, well-formed bundle for a different annotation in
        // the same call so we can assert "the malformed one is
        // skipped, the well-formed one still applies."
        let edge = sample_edge();
        let good_ann = ann_sid("ann_good");
        let mut all = malformed;
        all.extend(edge.to_reifies_facts(&good_ann, 7, true));

        let mut overlay = AttachmentNovelty::new();
        overlay
            .observe_flakes(&all)
            .expect("observe_flakes must NOT error on malformed bundle — skip + count");

        assert_eq!(
            overlay.observed_malformed_bundle_count(),
            1,
            "exactly one malformed bundle was observed"
        );
        // The well-formed bundle still landed.
        let attached: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        assert_eq!(attached, vec![good_ann]);
        // The malformed bundle's annotation has no live target.
        assert!(overlay.current_targets_for(&ann).next().is_none());

        // A second observe with a fresh malformed bundle bumps the
        // counter again; it's cumulative across calls.
        let another_bad: Vec<Flake> = vec![Flake::new(
            ann_sid("ann_bad2"),
            Sid::new(FLUREE_DB, p::REIFIES_OBJECT),
            FlakeValue::Ref(Sid::new(13, "acme")),
            fluree_db_core::edge::id_datatype_sid(),
            8,
            true,
            None,
        )];
        overlay.observe_flakes(&another_bad).unwrap();
        assert_eq!(overlay.observed_malformed_bundle_count(), 2);
    }
}
