//! Reification links for commits the index has not covered.
//!
//! The index derives `_:r rdf:reifies <<( s p o )>>` from each commit's
//! `f:reifies*` slot ops; novelty derives the same links for its own commits,
//! so a link query sees an annotation as soon as it commits. Deriving a
//! reifier's link needs its attachment before the commit: the index's slots
//! (an [`AttachmentBase`]) plus the reifier's earlier novelty ops.
//!
//! The base is known once the ledger has attached its index (or knows it has
//! none). Until then commits apply without links and `links_through` stays
//! behind; [`Novelty::set_attachment_base`] derives the missing links. A trim
//! past the base's `t` drops the base, since the trimmed ops then live only in
//! the next index.

use crate::error::{NoveltyError, Result};
use crate::{Novelty, Segment};
use fluree_db_core::flake::FlakeMeta;
use fluree_db_core::link::{is_attachment_slot, replay_links, AttachmentBase, AttachmentSlots};
use fluree_db_core::{Flake, FlakeValue, GraphId, IndexType, Sid};
use std::collections::BTreeMap;
use std::sync::Arc;

/// An index's attachments, and the `t` it covers.
#[derive(Clone, Debug)]
pub struct LinkBase {
    base: Arc<dyn AttachmentBase>,
    t: i64,
}

/// The base of a ledger whose index holds no attachments.
#[derive(Debug)]
struct NoAttachments;

impl AttachmentBase for NoAttachments {
    fn attachments(
        &self,
        _g_id: GraphId,
        reifiers: &[Sid],
    ) -> std::io::Result<Vec<AttachmentSlots>> {
        Ok(vec![AttachmentSlots::default(); reifiers.len()])
    }
}

impl LinkBase {
    /// The attachments `base` holds, as of index `t`.
    pub fn new(base: Arc<dyn AttachmentBase>, t: i64) -> Self {
        Self { base, t }
    }

    /// A base with no attachments, as of index `t`: no index, or one that
    /// never held an annotation.
    pub fn empty(t: i64) -> Self {
        Self::new(Arc::new(NoAttachments), t)
    }
}

/// Reifiers with attachment ops, keyed `(g_id, reifier)`, each with the ops a
/// batch brings that novelty does not hold yet.
pub(crate) type Touched<'a> = BTreeMap<(GraphId, Sid), Vec<&'a Flake>>;

/// Group a batch's attachment ops by reifier.
pub(crate) fn touched_reifiers<'a>(
    batch: impl IntoIterator<Item = (GraphId, &'a Flake)>,
) -> Touched<'a> {
    let mut touched = Touched::new();
    for (g_id, flake) in batch {
        if is_attachment_slot(&flake.p) {
            touched
                .entry((g_id, flake.s.clone()))
                .or_default()
                .push(flake);
        }
    }
    touched
}

impl Novelty {
    /// Install the index's attachments and derive the links of every commit
    /// applied without them. Returns the derived links, which the caller adds
    /// to its dictionary novelty.
    pub fn set_attachment_base(&mut self, base: LinkBase) -> Result<Vec<Flake>> {
        let derived = if self.links_through < self.t {
            let mut touched = Touched::new();
            for g_id in 0..self.graphs.len() as GraphId {
                for flake in self.graph_flakes(g_id) {
                    if flake.t > self.links_through && is_attachment_slot(&flake.p) {
                        touched.entry((g_id, flake.s.clone())).or_default();
                    }
                }
            }
            self.derive_links(&base, &touched, self.links_through)?
        } else {
            Vec::new()
        };
        self.link_base = Some(base);
        self.links_through = self.t;
        if derived.is_empty() {
            return Ok(Vec::new());
        }
        let mut per_graph: BTreeMap<GraphId, Vec<Flake>> = BTreeMap::new();
        for (g_id, flake) in &derived {
            per_graph.entry(*g_id).or_default().push(flake.clone());
        }
        for (g_id, batch) in per_graph {
            self.check_segment_capacity(g_id)?;
            for flake in &batch {
                self.fact_state.record(g_id, flake);
            }
            self.push_segment(g_id, Segment::build(batch, false));
        }
        self.epoch += 1;
        self.refresh_content_version();
        Ok(derived.into_iter().map(|(_, f)| f).collect())
    }

    /// The base links are derived from, when every earlier commit has its
    /// links; `None` leaves the next commit's links pending.
    pub(crate) fn current_link_base(&self) -> Option<&LinkBase> {
        self.link_base
            .as_ref()
            .filter(|_| self.links_through == self.t)
    }

    /// Links for each touched reifier: its index slots, replayed through every
    /// op novelty holds for it and then its new ops. Only ops after
    /// `emit_after` produce links.
    pub(crate) fn derive_links(
        &self,
        base: &LinkBase,
        touched: &Touched<'_>,
        emit_after: i64,
    ) -> Result<Vec<(GraphId, Flake)>> {
        let mut out: Vec<(GraphId, Flake)> = Vec::new();
        let mut links: Vec<Flake> = Vec::new();
        let mut graphs: BTreeMap<GraphId, Vec<&Sid>> = BTreeMap::new();
        for (g_id, reifier) in touched.keys() {
            graphs.entry(*g_id).or_default().push(reifier);
        }
        for (g_id, reifiers) in graphs {
            let owned: Vec<Sid> = reifiers.iter().map(|s| (*s).clone()).collect();
            let slots = base
                .base
                .attachments(g_id, &owned)
                .map_err(|e| NoveltyError::Storage(format!("index attachments: {e}")))?;
            if slots.len() != owned.len() {
                return Err(NoveltyError::Storage(format!(
                    "index attachments: {} answers for {} reifiers",
                    slots.len(),
                    owned.len()
                )));
            }
            for (reifier, mut state) in owned.into_iter().zip(slots) {
                let mut ops = self.reifier_slot_history(g_id, &reifier);
                ops.extend(touched[&(g_id, reifier)].iter().copied());
                replay_links(&mut state, &mut ops, emit_after, &mut links);
                out.extend(links.drain(..).map(|f| (g_id, f)));
            }
        }
        Ok(out)
    }

    /// Every slot op novelty holds for `reifier` in `g_id`.
    fn reifier_slot_history(&self, g_id: GraphId, reifier: &Sid) -> Vec<&Flake> {
        let first = Flake::new(
            reifier.clone(),
            Sid::min(),
            FlakeValue::min(),
            Sid::min(),
            i64::MIN,
            false,
            None,
        );
        let rhs = Flake::new(
            reifier.clone(),
            Sid::max(),
            FlakeValue::max(),
            Sid::max(),
            i64::MAX,
            true,
            Some(FlakeMeta::max()),
        );
        let mut ops = Vec::new();
        if let Some(Some(segs)) = self.graphs.get(g_id as usize) {
            for seg in segs {
                if !seg.may_overlap(IndexType::Spot, Some(&first), Some(&rhs), false) {
                    continue;
                }
                for &local in seg.range(IndexType::Spot, Some(&first), Some(&rhs), false) {
                    let flake = &seg.flakes[local as usize];
                    if flake.s == *reifier && is_attachment_slot(&flake.p) {
                        ops.push(flake);
                    }
                }
            }
        }
        ops
    }

    /// Every flake of `g_id`, segment by segment.
    fn graph_flakes(&self, g_id: GraphId) -> impl Iterator<Item = &Flake> {
        self.graphs
            .get(g_id as usize)
            .and_then(Option::as_ref)
            .into_iter()
            .flatten()
            .flat_map(|seg| seg.flakes.iter())
    }

    /// Drop the base once a trim passes it: the trimmed ops now live only in
    /// a newer index, which the ledger installs next.
    pub(crate) fn trim_link_base(&mut self, cutoff_t: i64) {
        if self.link_base.as_ref().is_some_and(|b| b.t < cutoff_t) {
            self.link_base = None;
        }
    }
}
