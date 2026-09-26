//! Delta computation for rebase conflict detection
//!
//! Walks a commit chain and collects (subject, predicate, graph) tuples
//! modified between two points, producing a set of [`ConflictKey`]s that
//! can be checked against branch commits to detect overlapping changes.

use crate::{trace_first_parent_commits_by_id, Result};
use fluree_db_core::{ConflictKey, ContentId, ContentStore, Flake, FlakeValue, Sid};
use futures::TryStreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{BTreeSet, HashMap};

/// Walk the first-parent lineage from `head_id` back to `stop_at_t` and
/// collect all (subject, predicate, graph) tuples modified in those commits.
///
/// This produces the "source delta" — the set of data points changed on
/// the source branch since the branch point. During rebase, branch commits
/// whose flakes overlap with this set are flagged as conflicts.
///
/// # Arguments
///
/// * `store` - Content store for loading commits by CID
/// * `head_id` - CID of the source branch's current HEAD commit
/// * `stop_at_t` - Stop when `commit.t <= stop_at_t` (the branch point t)
pub async fn compute_delta_keys<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> Result<FxHashSet<ConflictKey>> {
    let stream = trace_first_parent_commits_by_id(store, head_id, stop_at_t);
    futures::pin_mut!(stream);

    let mut keys = FxHashSet::default();

    while let Some(commit) = stream.try_next().await? {
        for flake in &commit.flakes {
            keys.insert(ConflictKey::new(
                flake.s.clone(),
                flake.p.clone(),
                flake.g.clone(),
            ));
        }
    }

    Ok(keys)
}

/// Full identity of a fact — every [`Flake`] component except `t` and `op`.
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
    dt: Sid,
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
            dt: flake.dt.clone(),
            lang: flake.m.as_ref().and_then(|m| m.lang.clone()),
            i: flake.m.as_ref().and_then(|m| m.i),
        }
    }
}

/// Accumulates the net effect of a commit range on each distinct fact.
///
/// Feed flakes in strictly **newest-first** order (reverse-chronological).
/// Per fact the newest op wins: it is the fact's state at the range head,
/// which is what replaying the range has to reproduce.
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
}

impl NetChangeAccumulator {
    /// Record one flake. Flakes must arrive newest-first, so the first
    /// occurrence of a fact is the one that decides its net op.
    pub fn push_newest_first(&mut self, flake: &Flake) {
        self.map
            .entry(FactKey::of(flake))
            .or_insert_with(|| flake.clone());
    }

    /// Every fact the range touched, represented by its newest in-range
    /// flake, so `op` is the fact's state at the range head. Unordered.
    pub fn finish(self) -> Vec<Flake> {
        self.map.into_values().collect()
    }
}

/// Like [`compute_delta_keys`], but additionally nets the range's flakes
/// into the aggregate change set the range applies (see
/// [`NetChangeAccumulator`] for the netting contract).
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

    #[test]
    fn same_commit_pair_nets_to_the_retract_via_reverse_iteration() {
        // Both ops at the same t, chronological order assert-then-retract.
        let out = net(&[flake("a", "v", 2, true), flake("a", "v", 2, false)]);
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
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
}
