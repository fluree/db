//! Delta computation for rebase conflict detection
//!
//! Walks a commit chain and collects (subject, predicate, graph) tuples
//! modified between two points, producing a set of [`ConflictKey`]s that
//! can be checked against branch commits to detect overlapping changes.

use crate::{trace_first_parent_commits_by_id, Result};
use fluree_db_core::same_t::supersedes;
use fluree_db_core::{
    load_commit_by_id, ConflictKey, ContentId, ContentStore, Flake, FlakeValue, Sid,
};
use futures::TryStreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};

/// Full identity of a fact — every [`Flake`] component except `t` and `op`,
/// and the datatype of a value the index keys by value alone
/// ([`FlakeValue::index_keys_by_value`]): storage holds an integer too large
/// for 64 bits as one fact whatever XSD subtype each op writes it with, so
/// netting it per term would replay a retraction under one subtype and an
/// assertion under another at one `t`, which a commit may not hold.
///
/// This is deliberately NOT `Flake`'s own `Eq`/`Hash` (which ignore `g` by
/// design — flake identity is graph-independent for index purposes). Netting
/// must treat the same triple in two graphs as two distinct facts, and must
/// keep language-tagged strings and list positions apart, so `g`, `lang`,
/// and `i` are all part of the key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FactKey {
    g: Option<Sid>,
    s: Sid,
    p: Sid,
    o: FlakeValue,
    dt: Option<Sid>,
    lang: Option<String>,
    i: Option<i32>,
}

impl FactKey {
    /// The identity of `flake`'s fact, independent of `t` and `op`.
    pub fn of(flake: &Flake) -> Self {
        Self {
            g: flake.g.clone(),
            s: flake.s.clone(),
            p: flake.p.clone(),
            o: flake.o.clone(),
            dt: (!flake.o.index_keys_by_value()).then(|| flake.dt.clone()),
            lang: flake.m.as_ref().and_then(|m| m.lang.clone()),
            i: flake.m.as_ref().and_then(|m| m.i),
        }
    }
}

/// Accumulates the net effect of a commit range on each distinct fact.
///
/// Feed flakes in strictly **newest-first** order (reverse-chronological).
/// Per fact the newest op wins: it is the fact's state at the range head,
/// which is what replaying the range has to reproduce. Within one commit
/// (one `t`) a retraction and an assertion of one fact resolve to the
/// assertion ([`fluree_db_core::same_t`]), whatever their order in the
/// commit; commits written before the staging fix hold such pairs.
///
/// The rule deliberately infers nothing about whether the fact existed
/// before the range, because a commit stream cannot support that inference:
/// re-asserting a value the ledger already holds writes a real assert flake,
/// and only [`Novelty::apply_commit`](crate::Novelty::apply_commit) discards
/// it at apply time. Reading "assert … retract" as created-then-destroyed
/// would therefore drop a genuine deletion whenever the range happened to
/// re-assert the value first, which idempotent loads and upserts do
/// routinely.
///
/// What this emits instead is at worst redundant, never wrong: a redundant
/// assert is deduplicated on apply, and a retract of a fact the target does
/// not hold is a no-op for reads, reindexing and later writes.
#[derive(Default)]
pub struct NetChangeAccumulator {
    map: FxHashMap<FactKey, Flake>,
    /// Facts whose deciding commit (the first to arrive) both retracted and
    /// asserted them at one `t`: see [`Self::finish_with_pairs`].
    paired: FxHashSet<FactKey>,
}

impl NetChangeAccumulator {
    /// Record one flake. Flakes must arrive newest-first, so the first
    /// occurrence of a fact is the one that decides its net op, except that
    /// a later occurrence at the same `t` replaces it when it holds there.
    pub fn push_newest_first(&mut self, flake: &Flake) {
        match self.map.entry(FactKey::of(flake)) {
            Entry::Vacant(e) => {
                e.insert(flake.clone());
            }
            Entry::Occupied(mut e) => {
                let cur = e.get();
                if flake.t != cur.t {
                    return;
                }
                if flake.op != cur.op {
                    self.paired.insert(e.key().clone());
                }
                if supersedes((flake.t, flake.op), (cur.t, cur.op)) {
                    e.insert(flake.clone());
                }
            }
        }
    }

    /// Every fact the range touched, represented by its newest in-range
    /// flake, so `op` is the fact's state at the range head. Unordered.
    pub fn finish(self) -> Vec<Flake> {
        self.map.into_values().collect()
    }

    /// [`Self::finish`], with the positions of the facts whose deciding
    /// commit held a same-`t` retraction and assertion of them. Such a pair
    /// resolves to its assertion, but what the commit changed depends on
    /// whether the fact was present before it, which only the state can say:
    /// an undo of the range resolves these against it.
    pub fn finish_with_pairs(self) -> (Vec<Flake>, Vec<usize>) {
        let Self { map, paired } = self;
        if paired.is_empty() {
            return (map.into_values().collect(), Vec::new());
        }
        let mut flakes = Vec::with_capacity(map.len());
        let mut pairs = Vec::with_capacity(paired.len());
        for (key, flake) in map {
            if paired.contains(&key) {
                pairs.push(flakes.len());
            }
            flakes.push(flake);
        }
        (flakes, pairs)
    }
}

/// The (subject, predicate, graph) tuples these commits changed, oldest
/// commit first.
///
/// Branch clocks are not comparable, so a range that crosses a merge cannot
/// be cut by `t`. Callers that compare two branches name the commits
/// instead.
pub async fn delta_keys_of<C: ContentStore + ?Sized>(
    store: &C,
    cids: &[ContentId],
) -> Result<FxHashSet<ConflictKey>> {
    let mut keys = FxHashSet::default();
    for cid in cids {
        let commit = load_commit_by_id(store, cid).await?;
        keys.extend(
            commit
                .flakes
                .iter()
                .map(|flake| ConflictKey::new(flake.s.clone(), flake.p.clone(), flake.g.clone())),
        );
    }
    Ok(keys)
}

/// Walk the first-parent lineage from `head_id` back to `stop_at_t`,
/// collecting the (subject, predicate, graph) tuples those commits changed
/// and netting their flakes into the aggregate change set the range applies
/// (see [`NetChangeAccumulator`] for the netting contract).
///
/// One walk serves every output, so callers that need conflict keys *and*
/// the change set (merge preview) replay the source chain once instead of
/// twice. The third element is the union of the range's namespace deltas,
/// earliest commit winning on a code collision: the codes a consumer needs
/// to make the change set's terms encodable before the range is committed.
/// The fourth is every named graph the range's commits list, by IRI: the
/// graphs a consumer must route before staging the change set elsewhere.
pub async fn compute_delta_keys_and_changes<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> Result<(
    FxHashSet<ConflictKey>,
    Vec<Flake>,
    HashMap<u16, String>,
    BTreeSet<String>,
)> {
    let stream = trace_first_parent_commits_by_id(store, head_id, stop_at_t);
    futures::pin_mut!(stream);

    let mut keys = FxHashSet::default();
    let mut acc = NetChangeAccumulator::default();
    let mut namespace_delta: HashMap<u16, String> = HashMap::new();
    let mut graph_iris: BTreeSet<String> = BTreeSet::new();

    while let Some(commit) = stream.try_next().await? {
        graph_iris.extend(commit.graph_delta.into_values());
        // Commits stream newest-first, so a plain insert leaves the oldest
        // commit's prefix in place for a colliding code.
        for (code, prefix) in commit.namespace_delta {
            namespace_delta.insert(code, prefix);
        }
        // Commits stream newest-first; iterate each commit's flakes in
        // reverse so the accumulator sees a strictly reverse-chronological
        // sequence even when one commit touches the same fact twice.
        for flake in commit.flakes.iter().rev() {
            keys.insert(ConflictKey::new(
                flake.s.clone(),
                flake.p.clone(),
                flake.g.clone(),
            ));
            acc.push_newest_first(flake);
        }
    }

    Ok((keys, acc.finish(), namespace_delta, graph_iris))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::FlakeMeta;

    fn sid(name: &str) -> Sid {
        Sid::new(100, name)
    }

    fn flake(s: &str, o: &str, t: i64, op: bool) -> Flake {
        Flake::new(
            sid(s),
            sid("pred"),
            FlakeValue::String(o.to_string()),
            sid("string"),
            t,
            op,
            None,
        )
    }

    /// Feed chronological flakes to the accumulator (which expects
    /// newest-first) and return the netted result.
    fn net(chronological: &[Flake]) -> Vec<Flake> {
        let mut acc = NetChangeAccumulator::default();
        for f in chronological.iter().rev() {
            acc.push_newest_first(f);
        }
        acc.finish()
    }

    #[test]
    fn lone_assert_survives() {
        let out = net(&[flake("a", "v", 1, true)]);
        assert_eq!(out.len(), 1);
        assert!(out[0].op);
    }

    #[test]
    fn lone_retract_survives() {
        let out = net(&[flake("a", "v", 1, false)]);
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
    }

    #[test]
    fn assert_then_retract_nets_to_the_retract() {
        // The assert may be redundant with state the range inherited, so the
        // retract is the only thing that can be known: the fact is gone at
        // the range head.
        let out = net(&[flake("a", "v", 1, true), flake("a", "v", 2, false)]);
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
        assert_eq!(out[0].t, 2);
    }

    #[test]
    fn retract_then_assert_nets_to_the_assert() {
        let out = net(&[flake("a", "v", 1, false), flake("a", "v", 2, true)]);
        assert_eq!(out.len(), 1);
        assert!(out[0].op);
        assert_eq!(out[0].t, 2);
    }

    #[test]
    fn assert_retract_assert_survives_as_assert() {
        let out = net(&[
            flake("a", "v", 1, true),
            flake("a", "v", 2, false),
            flake("a", "v", 3, true),
        ]);
        assert_eq!(out.len(), 1);
        assert!(out[0].op);
        assert_eq!(out[0].t, 3, "representative flake is the newest");
    }

    #[test]
    fn update_keeps_both_sides() {
        // Retract old value + assert new value = two distinct facts, both net.
        let out = net(&[flake("a", "old", 2, false), flake("a", "new", 2, true)]);
        assert_eq!(out.len(), 2);
    }

    /// A fact both retracted and asserted by its deciding commit is reported
    /// as paired; one changed at another `t` after that, or by one op, is not.
    #[test]
    fn a_same_t_pair_in_the_deciding_commit_is_reported() {
        let mut acc = NetChangeAccumulator::default();
        for f in [
            flake("a", "v", 2, false),
            flake("a", "v", 2, true),
            flake("b", "w", 2, true),
            flake("c", "x", 3, true),
            flake("c", "x", 2, false),
            flake("c", "x", 2, true),
        ] {
            acc.push_newest_first(&f);
        }
        let (flakes, pairs) = acc.finish_with_pairs();
        let paired: Vec<String> = pairs
            .iter()
            .map(|&n| match &flakes[n].o {
                FlakeValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(paired, vec!["v".to_string()], "{flakes:?}");
    }

    /// Both ops of one fact at the same `t` net to the assertion
    /// (`fluree_db_core::same_t`), whatever their order in the commit.
    #[test]
    fn same_commit_pair_nets_to_the_assert_in_either_order() {
        for pair in [
            [flake("a", "v", 2, true), flake("a", "v", 2, false)],
            [flake("a", "v", 2, false), flake("a", "v", 2, true)],
        ] {
            let out = net(&pair);
            assert_eq!(out.len(), 1);
            assert!(out[0].op);
        }
        // A newer commit still decides: a retract at t=3 after the pair.
        let out = net(&[
            flake("a", "v", 2, false),
            flake("a", "v", 2, true),
            flake("a", "v", 3, false),
        ]);
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
        assert_eq!(out[0].t, 3);
    }

    #[test]
    fn graph_scoping_keeps_facts_distinct() {
        let mut in_graph = flake("a", "v", 1, true);
        in_graph.g = Some(sid("g1"));
        let out = net(&[flake("a", "v", 1, true), in_graph]);
        assert_eq!(
            out.len(),
            2,
            "same triple in default + named graph must not collapse"
        );
    }

    #[test]
    fn lang_tags_keep_facts_distinct() {
        let mut en = flake("a", "chat", 1, true);
        en.m = Some(FlakeMeta::with_lang("en"));
        let mut fr = flake("a", "chat", 2, false);
        fr.m = Some(FlakeMeta::with_lang("fr"));
        let out = net(&[en, fr]);
        assert_eq!(
            out.len(),
            2,
            "differing language tags must not cancel each other"
        );
    }

    #[test]
    fn list_positions_keep_facts_distinct() {
        let mut p0 = flake("a", "v", 1, true);
        p0.m = Some(FlakeMeta::with_index(0));
        let mut p1 = flake("a", "v", 1, true);
        p1.m = Some(FlakeMeta::with_index(1));
        let out = net(&[p0, p1]);
        assert_eq!(out.len(), 2);
    }

    /// An integer too large for 64 bits is one fact whatever subtype each op
    /// writes it with, so a delete under one subtype and a write under
    /// another net to the write alone. A value that fits 64 bits keeps its
    /// datatype in the key.
    #[test]
    fn a_big_integer_nets_by_value_across_subtypes() {
        let big = |dt: &str, t: i64, op: bool| {
            Flake::new(
                sid("a"),
                sid("pred"),
                FlakeValue::BigInt(Box::new(
                    "123456789012345678901234567890".parse().expect("big"),
                )),
                sid(dt),
                t,
                op,
                None,
            )
        };
        let out = net(&[big("integer", 1, false), big("nonNegativeInteger", 2, true)]);
        assert_eq!(out.len(), 1);
        assert!(out[0].op);
        assert_eq!(out[0].dt, sid("nonNegativeInteger"));

        let long = |dt: &str, t: i64, op: bool| {
            Flake::new(
                sid("a"),
                sid("pred"),
                FlakeValue::Long(7),
                sid(dt),
                t,
                op,
                None,
            )
        };
        let out = net(&[long("integer", 1, false), long("int", 2, true)]);
        assert_eq!(out.len(), 2);
    }
}
