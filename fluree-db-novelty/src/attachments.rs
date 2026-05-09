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

use crate::error::{NoveltyError, Result};

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

    /// Iterator over annotation SIDs **currently attached** to `edge`.
    ///
    /// Walks the row history and returns one Sid per annotation whose
    /// most-recent event for this edge has `op == true`. Annotations
    /// that were asserted then retracted produce no row.
    pub fn current_annotations_for<'a>(
        &'a self,
        edge: &'a EdgeKey,
    ) -> impl Iterator<Item = Sid> + 'a {
        self.forward
            .get(edge)
            .map(|rows| latest_assertions::<_, _>(rows.iter(), |r| (&r.ann, r.t, r.op)))
            .into_iter()
            .flatten()
            .map(|sid| sid.clone())
    }

    /// Iterator over base [`EdgeKey`]s **currently reified** by
    /// annotation `ann`. Same `(t, op)` filter as
    /// [`Self::current_annotations_for`].
    ///
    /// Note: at v1 stage time we enforce "exactly one current target
    /// per annotation SID" — so this iterator usually produces zero or
    /// one element. We still return an iterator because (a) M2 indexed
    /// arenas may surface legacy multi-target rows, and (b)
    /// policy-filtered visibility checks downstream may drop the only
    /// candidate.
    pub fn current_targets_for<'a>(
        &'a self,
        ann: &'a Sid,
    ) -> impl Iterator<Item = EdgeKey> + 'a {
        self.reverse
            .get(ann)
            .map(|rows| latest_assertions::<_, _>(rows.iter(), |r| (&r.edge, r.t, r.op)))
            .into_iter()
            .flatten()
            .cloned()
    }

    /// Iterator over the *full* attachment history of `ann` —
    /// every `(EdgeKey, t, op)` event, in row-stored order. Used by
    /// history-range queries that explicitly want to see attachment
    /// lifecycle alongside flake history.
    pub fn target_history<'a>(
        &'a self,
        ann: &'a Sid,
    ) -> impl Iterator<Item = &'a ReverseRow> + 'a {
        self.reverse.get(ann).into_iter().flat_map(|rows| rows.iter())
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

    /// Observe a slice of accepted flakes and update the overlay.
    ///
    /// Filters down to `f:reifies*` flakes, groups them by
    /// `(ann_sid, t, op)`, and decodes each group via
    /// [`EdgeKey::from_reifies_facts`]. A malformed bundle (missing
    /// required predicate, duplicate, or deferred shape) is skipped
    /// with the structured decode error returned to the caller — the
    /// caller may log + telemetry-count and continue, since the
    /// non-`f:reifies*` flakes for the same annotation subject remain
    /// visible as ordinary RDF.
    ///
    /// Caller contract: pass the **post-dedup** flake set that
    /// `Novelty::apply_commit` ultimately stored in the arena.
    /// Observing a deduped duplicate would create a phantom row.
    pub fn observe_flakes(&mut self, flakes: &[Flake]) -> Result<()> {
        if flakes.is_empty() {
            return Ok(());
        }

        // Group `f:reifies*` flakes into bundles keyed by
        // `(ann_sid, t, op)`. Within a single transaction, a complete
        // attach- or detach-bundle for an annotation subject shares t
        // and op by construction.
        let mut bundles: BTreeMap<(Sid, i64, bool), Vec<Flake>> = BTreeMap::new();
        for f in flakes {
            if !is_reserved_reifies_predicate(&f.p) {
                continue;
            }
            bundles
                .entry((f.s.clone(), f.t, f.op))
                .or_default()
                .push(f.clone());
        }

        if bundles.is_empty() {
            return Ok(());
        }

        for ((ann, t, op), bundle) in bundles {
            let edge = EdgeKey::from_reifies_facts(&bundle).map_err(|e| {
                NoveltyError::InvalidGraph(format!(
                    "malformed f:reifies* bundle for annotation {ann:?} at t={t}: {e}"
                ))
            })?;

            self.forward
                .entry(edge.clone())
                .or_default()
                .push(ForwardRow {
                    ann: ann.clone(),
                    t,
                    op,
                });
            self.reverse.entry(ann).or_default().push(ReverseRow {
                edge,
                t,
                op,
            });
            self.has_annotations = true;
        }

        Ok(())
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
/// `(t, op)` event is currently asserted.
///
/// Generic over both row types so both `current_annotations_for` and
/// `current_targets_for` share the same implementation. Stable: when
/// the same `(other, t)` appears twice (impossible in practice but
/// not enforced by the type), the *last-encountered* `op` wins.
fn latest_assertions<'a, R, T>(
    rows: impl Iterator<Item = &'a R>,
    extract: impl Fn(&'a R) -> (&'a T, i64, bool),
) -> impl Iterator<Item = &'a T>
where
    R: 'a,
    T: 'a + Ord + Clone,
{
    // Build a small map of "latest (t, op)" per `other`. A BTreeMap
    // keyed on the `other` side gives a deterministic iteration order
    // (good for tests and replay determinism).
    let mut latest: BTreeMap<&'a T, (i64, bool)> = BTreeMap::new();
    for row in rows {
        let (other, t, op) = extract(row);
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
        overlay.observe_flakes(&edge.to_reifies_facts(&ann, 5, true)).unwrap();
        overlay.observe_flakes(&edge.to_reifies_facts(&ann, 7, false)).unwrap();

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
    fn parallel_annotations_on_one_edge_both_visible() {
        // Two distinct annotation subjects attached to the same edge.
        let edge = sample_edge();
        let ann_a = ann_sid("ann_A");
        let ann_b = ann_sid("ann_B");

        let mut overlay = AttachmentNovelty::new();
        overlay.observe_flakes(&edge.to_reifies_facts(&ann_a, 5, true)).unwrap();
        overlay.observe_flakes(&edge.to_reifies_facts(&ann_b, 6, true)).unwrap();

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
    fn observe_returns_error_on_malformed_bundle() {
        // Strip a required predicate from the bundle — decoder rejects.
        let edge = sample_edge();
        let ann = ann_sid("ann1");
        let mut bundle = edge.to_reifies_facts(&ann, 5, true);
        bundle.retain(|f| {
            !fluree_db_core::namespaces::is_reifies_subject(&f.p)
        });

        let mut overlay = AttachmentNovelty::new();
        let err = overlay.observe_flakes(&bundle).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("malformed") && msg.contains("reifies"),
            "error should describe the malformed bundle: {msg}"
        );
        // Overlay should remain untouched after the error.
        assert!(!overlay.has_annotations());
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
        overlay.observe_flakes(&edge.to_reifies_facts(&ann, 5, true)).unwrap();

        let attached: Vec<Sid> = overlay.current_annotations_for(&edge).collect();
        assert_eq!(attached, vec![ann]);
    }
}
