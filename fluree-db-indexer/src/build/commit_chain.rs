//! Commit chain walking helpers.
//!
//! Provides `walk_commit_chain_full` to walk the commit chain backward from
//! HEAD to genesis and return CIDs in chronological (genesis-first) order.

use fluree_db_core::storage::ContentStore;
use fluree_db_core::ContentId;

use crate::error::Result;

/// Walk the commit chain backward from `head` to genesis, returning CIDs
/// in chronological order (genesis first).
// Kept for: convenience wrapper over collect_dag_cids for callers that need
// only CIDs in chronological order without (t, cid) pairs.
// Use when: any indexer pipeline needs a simple genesis-first CID list.
#[expect(dead_code)]
pub(crate) async fn walk_commit_chain_full(
    content_store: &dyn ContentStore,
    head_commit_id: &ContentId,
) -> Result<Vec<ContentId>> {
    // stop_at_t=0 collects all commits (t starts at 1).
    let dag = fluree_db_core::collect_dag_cids(content_store, head_commit_id, 0).await?;
    // collect_dag_cids returns (t, cid) sorted by t descending; reverse for chronological order.
    let cids: Vec<ContentId> = dag.into_iter().rev().map(|(_, cid)| cid).collect();
    Ok(cids)
}
