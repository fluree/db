//! Fold a sequence of commits into the flakes and `namespace_delta` /
//! `graph_delta` of ONE new commit.
//!
//! Used by the merge and revert paths to bundle multiple source commits into
//! a single commit. Because that commit lands at a single `t`, and novelty
//! resolves a same-`t` assert and retract of one fact as "retracted", the
//! range's flakes are **netted per fact** first: a fact the range asserted,
//! retracted, and asserted again folds to one assert; a fact it replaced and
//! then restored folds to nothing. The netting contract is
//! [`NetChangeAccumulator`]'s.

use fluree_db_core::{Commit, ConflictKey, ContentId, Flake};
use fluree_db_novelty::NetChangeAccumulator;
use rustc_hash::FxHashSet;
use std::collections::{HashMap, HashSet};

/// Which flakes on one side of a branch comparison are that side's own
/// changes, tracked as its commits are walked oldest-first.
///
/// A merge whose merged-in history the other side already holds is not one
/// of the side's own commits. Its flakes on keys this side never changed are
/// copies of the other side's changes. Applying them again brings back
/// values the other side has since replaced, and comparing them reports
/// conflicts on keys this side never touched. Its flakes on keys this side
/// did change are how that merge resolved the overlap, and dropping those
/// would undo the resolution.
pub(crate) struct OwnChanges<'a> {
    own: HashSet<&'a ContentId>,
    keys: FxHashSet<ConflictKey>,
}

impl<'a> OwnChanges<'a> {
    /// `own` is the side's own commits, as `BranchSide::own` reports them.
    pub(crate) fn new(own: &'a [ContentId]) -> Self {
        Self {
            own: own.iter().collect(),
            keys: FxHashSet::default(),
        }
    }

    /// Whether the side made commit `cid` itself.
    pub(crate) fn is_own(&self, cid: &ContentId) -> bool {
        self.own.contains(cid)
    }

    /// Record `cid`'s changes, dropping the flakes that copy the other
    /// side's. Commits must arrive oldest-first, because a merge's
    /// resolution covers only keys the side changed before it.
    pub(crate) fn retain_changes(&mut self, cid: &ContentId, flakes: &mut Vec<Flake>) {
        if self.is_own(cid) {
            self.keys.extend(flakes.iter().map(key_of));
        } else {
            flakes.retain(|flake| self.keys.contains(&key_of(flake)));
        }
    }

    /// Every key the side has changed so far.
    pub(crate) fn into_keys(self) -> FxHashSet<ConflictKey> {
        self.keys
    }
}

/// The (subject, predicate, graph) key `flake` changes.
pub(crate) fn key_of(flake: &Flake) -> ConflictKey {
    ConflictKey::new(flake.s.clone(), flake.p.clone(), flake.g.clone())
}

/// Flakes and metadata accumulated from a sequence of commits.
#[derive(Default)]
pub(crate) struct CollectedCommitData {
    /// The range's net change, one flake per surviving fact. Unordered.
    pub(crate) flakes: Vec<Flake>,
    /// Union of namespace deltas; earlier commits win on key collisions.
    pub(crate) namespace_delta: HashMap<u16, String>,
    /// Union of graph deltas; earlier commits win on key collisions.
    pub(crate) graph_delta: HashMap<u16, String>,
}

/// Which direction the new commit applies the range in.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fold {
    /// Apply the commits as they were (merge): the newest commit's flakes
    /// are the newest change.
    Replay,
    /// Apply the commits' inverses (revert): undoing the range means the
    /// oldest commit's inverse is applied last, so it is the newest change.
    Undo,
}

/// Fold `commits` into a [`CollectedCommitData`].
///
/// Commits must be supplied in **oldest-first** order so that earlier
/// commits take precedence on namespace and graph delta keys (matches the
/// historical `or_insert` semantics in `merge.rs::collect_commit_data`).
///
/// Flake `t` is not a concern here: `StagedLedger::new` restamps every
/// staged flake.
pub(crate) fn collect_from_commits<I>(commits: I, fold: Fold) -> CollectedCommitData
where
    I: IntoIterator<Item = Commit>,
{
    let mut data = CollectedCommitData::default();
    let mut ordered: Vec<Vec<Flake>> = Vec::new();
    for commit in commits {
        ordered.push(commit.flakes);
        for (code, prefix) in commit.namespace_delta {
            data.namespace_delta.entry(code).or_insert(prefix);
        }
        for (g_id, iri) in commit.graph_delta {
            data.graph_delta.entry(g_id).or_insert(iri);
        }
    }

    // The accumulator wants the range newest-change-first. For a replay
    // that is the newest commit, last flake first. For an undo the inverse
    // of the oldest commit is applied last, so the oldest commit's inverse
    // comes first and each commit's own flakes keep their order.
    let mut acc = NetChangeAccumulator::default();
    match fold {
        Fold::Replay => {
            for flakes in ordered.iter().rev() {
                for flake in flakes.iter().rev() {
                    acc.push_newest_first(flake);
                }
            }
        }
        Fold::Undo => {
            for flakes in &ordered {
                for flake in flakes {
                    acc.push_newest_first(&flake.invert());
                }
            }
        }
    }
    data.flakes = acc.finish();
    data
}
