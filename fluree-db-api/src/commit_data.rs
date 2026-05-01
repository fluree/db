//! Accumulate flakes and `namespace_delta` / `graph_delta` from a sequence
//! of commits into one [`CollectedCommitData`].
//!
//! Used by the merge and revert paths to bundle multiple source commits into
//! a single new commit. The two paths differ only in how each commit's
//! flakes are transformed before they're appended (identity for merge,
//! `flake.invert_at(0)` for revert), so the loop body — and especially the
//! `or_insert` semantics for the namespace and graph deltas — is shared.

use fluree_db_core::{Commit, Flake};
use std::collections::HashMap;

/// Flakes and metadata accumulated from a sequence of commits.
#[derive(Default)]
pub(crate) struct CollectedCommitData {
    /// All flakes from the input commits, in order, after `flake_transform`.
    pub(crate) flakes: Vec<Flake>,
    /// Union of namespace deltas; earlier commits win on key collisions.
    pub(crate) namespace_delta: HashMap<u16, String>,
    /// Union of graph deltas; earlier commits win on key collisions.
    pub(crate) graph_delta: HashMap<u16, String>,
}

/// Fold `commits` into a [`CollectedCommitData`].
///
/// Commits must be supplied in **oldest-first** order so that earlier
/// commits take precedence on namespace and graph delta keys (matches the
/// historical `or_insert` semantics in `merge.rs::collect_commit_data`).
///
/// `flake_transform` is applied to every flake before it's appended. Use
/// [`std::convert::identity`] to keep flakes as-is (merge), or
/// `|f| f.invert_at(0)` to flip assertions ⇄ retractions (revert).
pub(crate) fn collect_from_commits<I, F>(commits: I, mut flake_transform: F) -> CollectedCommitData
where
    I: IntoIterator<Item = Commit>,
    F: FnMut(Flake) -> Flake,
{
    let mut data = CollectedCommitData::default();
    for commit in commits {
        data.flakes
            .extend(commit.flakes.into_iter().map(&mut flake_transform));
        for (code, prefix) in commit.namespace_delta {
            data.namespace_delta.entry(code).or_insert(prefix);
        }
        for (g_id, iri) in commit.graph_delta {
            data.graph_delta.entry(g_id).or_insert(iri);
        }
    }
    data
}
