//! Delta computation for rebase conflict detection
//!
//! Walks a commit chain and collects (subject, predicate, graph) tuples
//! modified between two points, producing a set of [`ConflictKey`]s that
//! can be checked against branch commits to detect overlapping changes.

use crate::{trace_commits_by_id, Result};
use fluree_db_core::{ConflictKey, ContentId, ContentStore};
use futures::TryStreamExt;
use rustc_hash::FxHashSet;

/// Walk the commit chain from `head_id` back to `stop_at_t` and collect
/// all (subject, predicate, graph) tuples modified in those commits.
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
    let stream = trace_commits_by_id(store, head_id, stop_at_t);
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
