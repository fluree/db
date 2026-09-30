//! Current facts: the one read every staged retraction comes from.
//!
//! **Invariant.** A retraction staged for commit names a fact that is
//! currently stored: its `(g, s, p, o, dt, m)` is read from storage (the
//! persisted index plus novelty, lifecycle-resolved), never rebuilt from a
//! query binding, a template, or an edge key. An intent that matches no
//! stored fact stages nothing.
//!
//! The second sentence is what makes the accumulator's cancellation sound.
//! It cancels a retraction against a same-transaction assertion of the same
//! fact, which implements SPARQL 1.1 Update's `(DS − D) ∪ I` only if every
//! retraction cancels a stored fact: a retraction of an absent `x` would
//! cancel an `INSERT x` in the same transaction and `x` would never be
//! written.
//!
//! The types carry the invariant. [`CurrentFacts`] is the only producer of
//! [`StoredFact`], whose fields are private, and [`StoredFact::retract`] is
//! the way staging turns one into a [`Retraction`] — the only item type
//! [`FlakeAccumulator::push_retractions`](crate::generate::FlakeAccumulator::push_retractions)
//! accepts.
//!
//! ## The read
//!
//! Per `(graph, subject, predicate)` slot (or subject): one seek into the
//! persisted index through `NoOverlay` (no novelty translation, no shared
//! LRU entry), plus a Sid-space seek into novelty bounded by the slot's
//! sentinels, then [`resolve_current_flakes`]: the newest op per fact wins
//! and retractions drop out. Index rows come back without their graph, so
//! every survivor is stamped with the slot's graph Sid.
//!
//! When a graph's novelty is small relative to the number of slots asked
//! about in it, one filtered walk over that graph's novelty answers every
//! slot at once instead (see [`NOVELTY_WALK_RATIO`]).
//!
//! Not view-policy filtered, matching the whole-graph scans of sync and
//! CLEAR: modify policy still runs on the staged flakes.

use crate::error::{Result, TransactError};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::resolve_current_flakes;
use fluree_db_core::{
    range_with_overlay, Flake, GraphId, NoOverlay, OverlayProvider, RangeMatch, RangeOptions,
    RangeTest, Sid, Tracker,
};
use fluree_db_ledger::LedgerState;
use std::collections::{HashMap, HashSet};

/// Seek-versus-walk break-even for a graph's novelty. Slots are answered by
/// one filtered walk over the graph's novelty when
/// `slots × segments × NOVELTY_WALK_RATIO > novelty flakes`, and by one
/// bounded seek per slot otherwise. A seek costs `O(segments · log N)`
/// comparisons, a walk `O(N)` hash probes; 16 puts the switch where a
/// seek's log factor and per-call overhead catch up with a probe per flake.
pub const NOVELTY_WALK_RATIO: usize = 16;

/// Where a stored fact's surviving op was read from.
///
/// Index rows are decodes, and some decodes are lossy — a big integer's
/// declared XSD subtype is not stored, so it decodes as `xsd:integer` or
/// `xsd:decimal`. Recording the origin keeps any value-only identity rule
/// for those rows in this one type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The persisted index.
    Index,
    /// Novelty (commits since the last index).
    Novelty,
}

/// A fact as currently stored in its graph, graph Sid stamped (`None` for
/// the default graph). Built only by [`CurrentFacts`].
#[derive(Clone, Debug)]
pub struct StoredFact {
    flake: Flake,
    origin: Origin,
}

impl StoredFact {
    /// The stored fact (an assertion; `t` is the transaction that wrote it).
    pub fn flake(&self) -> &Flake {
        &self.flake
    }

    /// Where the fact was read from.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// Retract this fact at `t`. The one way staging retracts a stored fact.
    pub fn retract(self, t: i64) -> Retraction {
        Retraction(Flake {
            t,
            op: false,
            ..self.flake
        })
    }
}

/// A retraction of a currently stored fact.
///
/// The field is private to this module: a `Retraction` comes from
/// [`StoredFact::retract`] or from a WHERE row proven to be the decode of a
/// stored fact (see `resolve_intents`), and from nowhere else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retraction(Flake);

impl Retraction {
    /// The retraction flake.
    pub fn flake(&self) -> &Flake {
        &self.0
    }

    /// Unwrap into the retraction flake, for the staged flake set.
    pub fn into_flake(self) -> Flake {
        self.0
    }
}

/// A `(graph, subject, predicate)` slot to read.
#[derive(Clone, Debug)]
pub struct Slot {
    /// Ledger graph id to read (0 = default graph).
    pub g_id: GraphId,
    /// Graph Sid stamped on every fact read (`None` = default graph).
    pub g_sid: Option<Sid>,
    /// Subject.
    pub s: Sid,
    /// Predicate.
    pub p: Sid,
}

/// Reads a ledger's current facts as of its current `t`.
pub struct CurrentFacts<'l> {
    ledger: &'l LedgerState,
    to_t: i64,
}

impl<'l> CurrentFacts<'l> {
    /// Current facts of `ledger` as of `ledger.t()`.
    pub fn new(ledger: &'l LedgerState) -> Self {
        Self {
            ledger,
            to_t: ledger.t(),
        }
    }

    /// The current facts of every slot, in slot order: one `Vec` per slot,
    /// each in SPOT order.
    ///
    /// Duplicate slots are answered once and cloned. Slots of one graph
    /// share one decision between per-slot novelty seeks and a single
    /// filtered novelty walk.
    pub async fn of_slots(&self, slots: &[Slot]) -> Result<Vec<Vec<StoredFact>>> {
        let novelty = NoveltyPlan::for_slots(self.ledger, slots, self.to_t);
        let mut out = Vec::with_capacity(slots.len());
        for slot in slots {
            let rm = RangeMatch::subject_predicate(slot.s.clone(), slot.p.clone());
            let mut flakes = self.base(slot.g_id, rm).await?;
            novelty.collect_slot(self.ledger, slot, self.to_t, &mut flakes);
            out.push(self.resolve(flakes, slot.g_sid.as_ref()));
        }
        Ok(out)
    }

    /// The current facts of one subject in graph `g_id`, in SPOT order.
    pub async fn of_subject(
        &self,
        g_id: GraphId,
        g_sid: Option<&Sid>,
        s: &Sid,
    ) -> Result<Vec<StoredFact>> {
        let mut flakes = self
            .base(g_id, RangeMatch::new().with_subject(s.clone()))
            .await?;
        let lo = Flake::min_for_subject(s.clone());
        let hi = Flake::max_for_subject(s.clone());
        self.ledger.novelty.for_each_overlay_flake(
            g_id,
            IndexType::Spot,
            Some(&lo),
            Some(&hi),
            false,
            self.to_t,
            &mut |f| flakes.push(f.clone()),
        );
        Ok(self.resolve(flakes, g_sid))
    }

    /// Every current fact of graph `g_id`, graph Sid stamped.
    ///
    /// The whole-graph verbs (graph sync, CLEAR, DROP, COPY, MOVE) read
    /// through this. `limit` bounds what is materialized, not just what is
    /// returned: the provider's drain loop stops at `limit + 1` and the scan
    /// then fails with [`TransactError::WholeGraphScanTooLarge`].
    pub async fn whole_graph(
        &self,
        g_id: GraphId,
        g_sid: Option<&Sid>,
        limit: Option<usize>,
        tracker: Option<&Tracker>,
    ) -> Result<Vec<StoredFact>> {
        let db_ref = match tracker {
            Some(t) => self.ledger.as_graph_db_ref(g_id).with_tracker(t),
            None => self.ledger.as_graph_db_ref(g_id),
        };
        // `Eq` with an empty match is the whole-graph scan on both range
        // paths: the V3 provider treats "nothing bound" as a full-index
        // cursor and rejects every other `RangeTest`, and the genesis
        // (overlay-only) path matches an empty `Eq` against every flake.
        let opts = RangeOptions {
            flake_limit: limit.map(|l| l.saturating_add(1)),
            ..Default::default()
        };
        let flakes = db_ref
            .range_with_opts(IndexType::Spot, RangeTest::Eq, RangeMatch::new(), opts)
            .await
            .map_err(|e| TransactError::FlakeGeneration(format!("graph scan failed: {e}")))?;
        if let Some(l) = limit {
            if flakes.len() > l {
                return Err(TransactError::WholeGraphScanTooLarge { limit: l });
            }
        }
        // Already lifecycle-resolved by the range read.
        let index_t = self.ledger.snapshot.t;
        Ok(flakes
            .into_iter()
            .map(|flake| stored(flake, g_sid, index_t))
            .collect())
    }

    /// Persisted-index rows matching `rm` in graph `g_id`, read through
    /// `NoOverlay`: one leaf seek, no novelty translation. At genesis (no
    /// index yet) there is no provider and the answer is empty.
    async fn base(&self, g_id: GraphId, rm: RangeMatch) -> Result<Vec<Flake>> {
        Ok(range_with_overlay(
            &self.ledger.snapshot,
            g_id,
            &NoOverlay,
            IndexType::Spot,
            RangeTest::Eq,
            rm,
            RangeOptions::new().with_to_t(self.to_t),
        )
        .await?)
    }

    /// Resolve base rows plus novelty ops to the current facts, stamping
    /// the graph and the origin.
    fn resolve(&self, flakes: Vec<Flake>, g_sid: Option<&Sid>) -> Vec<StoredFact> {
        if flakes.is_empty() {
            return Vec::new();
        }
        let index_t = self.ledger.snapshot.t;
        resolve_current_flakes(flakes, IndexType::Spot)
            .into_iter()
            .map(|flake| stored(flake, g_sid, index_t))
            .collect()
    }
}

/// Wrap a resolved flake: stamp the graph (index rows decode with `g:
/// None`, whatever graph they live in) and record where it came from.
/// Novelty holds only commits after the index, so a fact whose surviving op
/// is at or below the index `t` was read from the index.
fn stored(mut flake: Flake, g_sid: Option<&Sid>, index_t: i64) -> StoredFact {
    flake.g = g_sid.cloned();
    let origin = if flake.t <= index_t {
        Origin::Index
    } else {
        Origin::Novelty
    };
    StoredFact { flake, origin }
}

/// How each graph's novelty is read for a batch of slots.
struct NoveltyPlan {
    /// Graphs whose novelty was walked once, keyed `(graph, s, p)`.
    walked: HashMap<(GraphId, Sid, Sid), Vec<Flake>>,
    walked_graphs: HashSet<GraphId>,
    /// Graphs with no novelty at all: nothing to read.
    empty_graphs: HashSet<GraphId>,
}

impl NoveltyPlan {
    fn for_slots(ledger: &LedgerState, slots: &[Slot], to_t: i64) -> Self {
        let novelty = ledger.novelty.as_ref();
        let mut per_graph: HashMap<GraphId, usize> = HashMap::new();
        for slot in slots {
            *per_graph.entry(slot.g_id).or_default() += 1;
        }
        let mut plan = NoveltyPlan {
            walked: HashMap::new(),
            walked_graphs: HashSet::new(),
            empty_graphs: HashSet::new(),
        };
        for (g_id, slot_count) in per_graph {
            let flakes = novelty.overlay_flake_count(g_id).unwrap_or(usize::MAX);
            if flakes == 0 {
                plan.empty_graphs.insert(g_id);
                continue;
            }
            let segments = novelty.segment_count(g_id).max(1);
            if slot_count
                .saturating_mul(segments)
                .saturating_mul(NOVELTY_WALK_RATIO)
                <= flakes
            {
                continue; // per-slot seeks
            }
            let wanted: HashSet<(&Sid, &Sid)> = slots
                .iter()
                .filter(|slot| slot.g_id == g_id)
                .map(|slot| (&slot.s, &slot.p))
                .collect();
            let walked = &mut plan.walked;
            novelty.for_each_overlay_flake(
                g_id,
                IndexType::Spot,
                None,
                None,
                true,
                to_t,
                &mut |f| {
                    if wanted.contains(&(&f.s, &f.p)) {
                        walked
                            .entry((g_id, f.s.clone(), f.p.clone()))
                            .or_default()
                            .push(f.clone());
                    }
                },
            );
            plan.walked_graphs.insert(g_id);
        }
        plan
    }

    /// Append the novelty ops of `slot` to `out`.
    fn collect_slot(&self, ledger: &LedgerState, slot: &Slot, to_t: i64, out: &mut Vec<Flake>) {
        if self.empty_graphs.contains(&slot.g_id) {
            return;
        }
        if self.walked_graphs.contains(&slot.g_id) {
            if let Some(ops) = self
                .walked
                .get(&(slot.g_id, slot.s.clone(), slot.p.clone()))
            {
                out.extend(ops.iter().cloned());
            }
            return;
        }
        let lo = Flake::min_for_subject_predicate(slot.s.clone(), slot.p.clone());
        let hi = Flake::max_for_subject_predicate(slot.s.clone(), slot.p.clone());
        ledger.novelty.for_each_overlay_flake(
            slot.g_id,
            IndexType::Spot,
            Some(&lo),
            Some(&hi),
            false,
            to_t,
            &mut |f| out.push(f.clone()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeMeta, FlakeValue, LedgerSnapshot};
    use fluree_db_novelty::Novelty;

    fn sid(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn string_dt() -> Sid {
        Sid::new(fluree_vocab::namespaces::XSD, "string")
    }

    fn lang_dt() -> Sid {
        Sid::new(fluree_vocab::namespaces::RDF, "langString")
    }

    fn flake(s: &str, p: &str, o: &str, t: i64, op: bool, m: Option<FlakeMeta>) -> Flake {
        let dt = if m.as_ref().is_some_and(|m| m.lang.is_some()) {
            lang_dt()
        } else {
            string_dt()
        };
        Flake::new(
            sid(s),
            sid(p),
            FlakeValue::String(o.to_string()),
            dt,
            t,
            op,
            m,
        )
    }

    fn in_graph(mut f: Flake, g: &Sid) -> Flake {
        f.g = Some(g.clone());
        f
    }

    /// A genesis ledger (nothing indexed) whose novelty holds `commits`, one
    /// commit per inner vector at t = 1, 2, …
    fn ledger(commits: Vec<Vec<Flake>>, reverse_graph: &HashMap<Sid, GraphId>) -> LedgerState {
        let mut novelty = Novelty::new(0);
        for (i, flakes) in commits.into_iter().enumerate() {
            novelty
                .apply_commit(flakes, i as i64 + 1, reverse_graph)
                .expect("apply commit");
        }
        LedgerState::new(LedgerSnapshot::genesis("test:main"), novelty)
    }

    fn slot(g_id: GraphId, g_sid: Option<&Sid>, s: &str, p: &str) -> Slot {
        Slot {
            g_id,
            g_sid: g_sid.cloned(),
            s: sid(s),
            p: sid(p),
        }
    }

    fn objects(facts: &[StoredFact]) -> Vec<(String, Option<FlakeMeta>)> {
        facts
            .iter()
            .map(|f| match &f.flake().o {
                FlakeValue::String(s) => (s.clone(), f.flake().m.clone()),
                other => (format!("{other:?}"), f.flake().m.clone()),
            })
            .collect()
    }

    /// Lang-tagged and list values come back as stored — with their tag and
    /// position — and a value retracted in a later commit is gone.
    #[tokio::test]
    async fn slot_facts_keep_lang_and_list_metadata_and_drop_retracted_values() {
        let en = FlakeMeta::with_lang("en");
        let fr = FlakeMeta::with_lang("fr");
        let ledger = ledger(
            vec![
                vec![
                    flake("a", "label", "x", 1, true, Some(en.clone())),
                    flake("a", "label", "x", 1, true, Some(fr.clone())),
                    flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(0))),
                    flake("a", "items", "v", 1, true, Some(FlakeMeta::with_index(1))),
                    flake("a", "items", "w", 1, true, Some(FlakeMeta::with_index(2))),
                    flake("a", "other", "o", 1, true, None),
                ],
                vec![flake("a", "items", "w", 2, false, Some(FlakeMeta::with_index(2)))],
            ],
            &HashMap::new(),
        );
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(0, None, "a", "label"), slot(0, None, "a", "items")])
            .await
            .unwrap();
        let mut labels = objects(&facts[0]);
        labels.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            labels,
            vec![("x".to_string(), Some(en)), ("x".to_string(), Some(fr))]
        );
        assert_eq!(
            objects(&facts[1]),
            vec![
                ("v".to_string(), Some(FlakeMeta::with_index(0))),
                ("v".to_string(), Some(FlakeMeta::with_index(1))),
            ],
            "list duplicates stay separate facts; the retracted entry is gone"
        );
        assert!(facts
            .iter()
            .flatten()
            .all(|f| f.origin() == Origin::Novelty && f.flake().g.is_none()));
    }

    /// A slot in a named graph reads that graph only, and every fact is
    /// stamped with the graph Sid the caller gives, which is what lets the
    /// retraction cancel a same-transaction assertion in that graph.
    #[tokio::test]
    async fn named_graph_slots_read_their_graph_and_stamp_it() {
        let g1 = sid("g1");
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let ledger = ledger(
            vec![vec![
                flake("a", "p", "default", 1, true, None),
                in_graph(flake("a", "p", "named", 1, true, None), &g1),
            ]],
            &reverse,
        );
        let facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(3, Some(&g1), "a", "p"), slot(0, None, "a", "p")])
            .await
            .unwrap();
        assert_eq!(objects(&facts[0]), vec![("named".to_string(), None)]);
        assert_eq!(facts[0][0].flake().g.as_ref(), Some(&g1));
        assert_eq!(objects(&facts[1]), vec![("default".to_string(), None)]);
        assert_eq!(facts[1][0].flake().g, None);
    }

    /// The per-slot seek and the single filtered walk must give the same
    /// answer: which one runs depends only on how many slots a graph gets
    /// relative to its novelty size.
    #[tokio::test]
    async fn seek_and_walk_agree() {
        let mut commit = Vec::new();
        for i in 0..40 {
            commit.push(flake(&format!("s{i:02}"), "p", &format!("v{i}"), 1, true, None));
            commit.push(flake(&format!("s{i:02}"), "q", &format!("w{i}"), 1, true, None));
        }
        let retract = vec![flake("s07", "p", "v7", 2, false, None)];
        let ledger = ledger(vec![commit, retract], &HashMap::new());
        let facts = CurrentFacts::new(&ledger);

        // One slot against 81 novelty flakes: seek.
        let one = facts.of_slots(&[slot(0, None, "s07", "q")]).await.unwrap();
        // Every slot: walk.
        let all_slots: Vec<Slot> = (0..40)
            .flat_map(|i| {
                [
                    slot(0, None, &format!("s{i:02}"), "p"),
                    slot(0, None, &format!("s{i:02}"), "q"),
                ]
            })
            .collect();
        let all = facts.of_slots(&all_slots).await.unwrap();
        assert_eq!(objects(&one[0]), vec![("w7".to_string(), None)]);
        assert_eq!(objects(&all[15]), vec![("w7".to_string(), None)]);
        assert!(all[14].is_empty(), "s07 p was retracted");
        assert_eq!(
            all.iter().map(Vec::len).sum::<usize>(),
            79,
            "80 values minus the retracted one"
        );
    }

    #[tokio::test]
    async fn subject_facts_cover_every_predicate() {
        let ledger = ledger(
            vec![vec![
                flake("a", "p", "1", 1, true, None),
                flake("a", "q", "2", 1, true, None),
                flake("b", "p", "3", 1, true, None),
            ]],
            &HashMap::new(),
        );
        let facts = CurrentFacts::new(&ledger)
            .of_subject(0, None, &sid("a"))
            .await
            .unwrap();
        assert_eq!(
            objects(&facts),
            vec![("1".to_string(), None), ("2".to_string(), None)]
        );
    }

    /// `retract` keeps the stored identity — graph, datatype, tag, list
    /// position — and changes only `t` and the op.
    #[tokio::test]
    async fn retract_keeps_the_stored_identity() {
        let g1 = sid("g1");
        let reverse: HashMap<Sid, GraphId> = [(g1.clone(), 3)].into_iter().collect();
        let stored_flake = in_graph(
            flake(
                "a",
                "label",
                "x",
                1,
                true,
                FlakeMeta::from_parts(Some("en"), Some(4)),
            ),
            &g1,
        );
        let ledger = ledger(vec![vec![stored_flake.clone()]], &reverse);
        let mut facts = CurrentFacts::new(&ledger)
            .of_slots(&[slot(3, Some(&g1), "a", "label")])
            .await
            .unwrap();
        let retraction = facts.remove(0).remove(0).retract(9).into_flake();
        // `Flake`'s equality ignores `g`, `t` and `op`, so check them apart.
        assert_eq!(retraction, stored_flake);
        assert_eq!((retraction.t, retraction.op), (9, false));
        assert_eq!(retraction.g, stored_flake.g);
        assert_eq!(retraction.m, stored_flake.m);
        assert_eq!(retraction.dt, stored_flake.dt);
    }
}
