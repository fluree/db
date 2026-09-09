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

use fluree_db_core::{Commit, Flake};
use fluree_db_novelty::NetChangeAccumulator;
use std::collections::HashMap;

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
