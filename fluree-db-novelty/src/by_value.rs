//! Retractions of big integers from commits staged by value.
//!
//! A commit an earlier release wrote, or one written against such a
//! release's index, retracts an integer too large for 64 bits by value: that
//! index held the value under every XSD subtype as one fact, read back as
//! `xsd:integer`. Its retraction retracts every term of the value stored at
//! its slot just before the commit ([`fluree_db_core::Commit::num_big_typing`]).
//!
//! What those terms are depends on the index novelty overlays, which is not
//! known while a ledger loads its commits. Novelty holds such a commit's
//! flakes as written, and remembers its ops on big integers here: its
//! retractions, and its assertions, whose terms hold on its `t` whatever it
//! retracts.
//! Once the ledger has its index, the retractions of the other terms are
//! worked out and added at the commit's `t`
//! ([`Novelty::add_by_value_retractions`]). They are worked out again when
//! the index changes how it types big integers, the first time an index that
//! keeps subtypes replaces an earlier release's.

use crate::error::Result;
use crate::{Novelty, Segment};
use fluree_db_core::link::is_attachment_slot;
use fluree_db_core::{Flake, GraphId, NumBigTyping, Sid};
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// An op on a big integer from a commit staged by value, and the typing of
/// the index the retractions that go with the commit were worked out against
/// (`None` until they are).
#[derive(Clone, Debug)]
pub struct ByValueOp {
    pub flake: Flake,
    pub resolved_under: Option<NumBigTyping>,
}

impl Novelty {
    /// Remember the ops on big integers in `flakes`, from a commit staged by
    /// value that novelty holds. `resolved_under` is the typing the
    /// retractions applied alongside its own were worked out under, if they
    /// were.
    pub fn note_by_value_ops<'f>(
        &mut self,
        flakes: impl IntoIterator<Item = &'f Flake>,
        resolved_under: Option<NumBigTyping>,
    ) {
        self.by_value.extend(
            flakes
                .into_iter()
                .filter(|f| f.o.is_big_integer())
                .map(|f| ByValueOp {
                    flake: f.clone(),
                    resolved_under,
                }),
        );
    }

    /// The ops on big integers novelty holds from commits staged by value.
    pub fn by_value_ops(&self) -> &[ByValueOp] {
        &self.by_value
    }

    /// Whether some commit staged by value that novelty holds has not had
    /// its retractions worked out against an index typed `typing`.
    pub fn by_value_unresolved(&self, typing: NumBigTyping) -> bool {
        self.by_value
            .iter()
            .any(|r| !r.flake.op && r.resolved_under != Some(typing))
    }

    /// Add `retractions`, worked out under `typing` for the commits staged by
    /// value that novelty holds, each with its graph, and mark every one of
    /// those commits' retractions worked out under `typing`. A retraction
    /// novelty already holds at its `t` is not added again.
    ///
    /// A retraction of an attachment slot changes its reifier's link, from
    /// its `t` on: returns the links that adds, which the caller's
    /// dictionary novelty must register.
    pub fn add_by_value_retractions(
        &mut self,
        retractions: Vec<(GraphId, Flake)>,
        typing: NumBigTyping,
    ) -> Result<Vec<Flake>> {
        for r in &mut self.by_value {
            r.resolved_under = Some(typing);
        }
        let mut per_graph: BTreeMap<GraphId, Vec<Flake>> = BTreeMap::new();
        for (g_id, flake) in retractions {
            per_graph.entry(g_id).or_default().push(flake);
        }
        let mut added = false;
        let mut reifiers: BTreeSet<(GraphId, Sid)> = BTreeSet::new();
        for (g_id, mut batch) in per_graph {
            let present: HashSet<usize> = {
                let slots: HashSet<(&Sid, &Sid, i64)> =
                    batch.iter().map(|f| (&f.s, &f.p, f.t)).collect();
                self.graph_flakes(g_id)
                    .filter(|f| !f.op && slots.contains(&(&f.s, &f.p, f.t)))
                    .filter_map(|held| batch.iter().position(|f| f == held && f.t == held.t))
                    .collect()
            };
            let mut at = 0;
            batch.retain(|_| {
                at += 1;
                !present.contains(&(at - 1))
            });
            if batch.is_empty() {
                continue;
            }
            self.check_segment_capacity(g_id)?;
            for flake in &batch {
                self.fact_state.record(g_id, flake);
                self.note_list_meta(flake);
                if is_attachment_slot(&flake.p) {
                    reifiers.insert((g_id, flake.s.clone()));
                }
            }
            self.push_segment(g_id, Segment::build(batch, false));
            added = true;
        }
        let links = self.missing_links(reifiers)?;
        let mut per_graph: BTreeMap<GraphId, Vec<Flake>> = BTreeMap::new();
        for (g_id, link) in &links {
            per_graph.entry(*g_id).or_default().push(link.clone());
        }
        for (g_id, batch) in per_graph {
            self.check_segment_capacity(g_id)?;
            for flake in &batch {
                self.fact_state.record(g_id, flake);
            }
            self.push_segment(g_id, Segment::build(batch, false));
        }
        if added {
            self.epoch += 1;
            self.refresh_content_version();
        }
        Ok(links.into_iter().map(|(_, link)| link).collect())
    }

    /// Forget the ops of commits at or before `cutoff_t`, which the index now
    /// holds.
    pub(crate) fn trim_by_value(&mut self, cutoff_t: i64) {
        self.by_value.retain(|r| r.flake.t > cutoff_t);
    }
}
