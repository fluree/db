//! Ledger integrity verification: walk the commit chain and confirm every
//! object it references actually exists in storage.
//!
//! Ledger *state* never depends on anything outside the commit blobs (flakes
//! live in the commit), so a missing referenced object is not corruption of
//! the data — but it breaks replication paths (clone, pack, push, merge) and
//! loses provenance. This walk turns "clone mysteriously fails" into a
//! one-command diagnosis that names the commit, its `t`, and the missing CID.

use crate::error::{ApiError, Result};
use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::{ContentId, ContentStore};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// One integrity problem found during verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VerifyProblem {
    /// A commit named as a parent is not in storage. The chain is broken
    /// below `referenced_by`; anything older is unreachable.
    MissingCommit {
        commit_id: ContentId,
        referenced_by: ContentId,
        referenced_by_t: i64,
    },
    /// The commit the nameservice points at as the head is not in storage.
    /// Nothing references it from within the chain, so it has no
    /// `referenced_by` — the whole chain is unreachable.
    MissingHead { commit_id: ContentId },
    /// A commit blob exists but cannot be decoded.
    UnreadableCommit { commit_id: ContentId, error: String },
    /// A commit references a raw-transaction blob that is not in storage.
    /// State is unaffected; provenance for that commit is lost and
    /// replication paths must tolerate the gap.
    MissingTxnBlob {
        t: i64,
        commit_id: ContentId,
        txn_id: ContentId,
    },
    /// A commit's `t` is not its primary parent's `t + 1`.
    TGap {
        commit_id: ContentId,
        t: i64,
        parent_id: ContentId,
        parent_t: i64,
    },
    /// The nameservice points at an index root that is not in storage.
    MissingIndexRoot { index_id: ContentId, index_t: i64 },
}

/// How badly a problem compromises the ledger.
///
/// The split is the one this crate's replication paths already make: a
/// missing txn blob costs provenance but every replication path tolerates
/// it (export, pack, push, merge, clone all warn and continue), while a
/// broken chain or a missing index root stops them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifySeverity {
    /// No problems found.
    Healthy,
    /// Provenance is incomplete; state and replication are intact.
    Provenance,
    /// The commit chain or the index root is broken.
    Chain,
}

impl VerifyProblem {
    /// Classify this problem. See [`VerifySeverity`].
    pub fn severity(&self) -> VerifySeverity {
        match self {
            // State is intact and every replication path tolerates it.
            VerifyProblem::MissingTxnBlob { .. } => VerifySeverity::Provenance,
            VerifyProblem::MissingCommit { .. }
            | VerifyProblem::MissingHead { .. }
            | VerifyProblem::UnreadableCommit { .. }
            | VerifyProblem::TGap { .. }
            // Clone requests the index in its pack by default, so a missing
            // root breaks replication, not just local query performance.
            | VerifyProblem::MissingIndexRoot { .. } => VerifySeverity::Chain,
        }
    }
}

/// Result of [`crate::Fluree::verify_ledger`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerVerifyReport {
    pub ledger_id: String,
    pub head_commit_id: Option<ContentId>,
    pub head_t: i64,
    pub index_id: Option<ContentId>,
    pub index_t: i64,
    /// Commits actually read during the walk.
    pub commits_checked: usize,
    /// Commits in the chain that reference a txn blob.
    pub txn_refs_checked: usize,
    /// `true` when the walk stopped at `max_commits` before reaching genesis.
    pub truncated: bool,
    pub problems: Vec<VerifyProblem>,
}

impl LedgerVerifyReport {
    pub fn is_healthy(&self) -> bool {
        self.problems.is_empty()
    }

    /// The worst severity among the problems found — what an automation
    /// gate should branch on.
    pub fn severity(&self) -> VerifySeverity {
        self.problems
            .iter()
            .map(VerifyProblem::severity)
            .max()
            .unwrap_or(VerifySeverity::Healthy)
    }
}

/// The child that led the walk to a commit: `(child_id, child_t, via_primary_parent)`.
type ChildEdge = (ContentId, i64, bool);

/// Walk the commit DAG from `head`, checking that each commit decodes, its
/// parents exist, `t` is contiguous along the primary parent, and every
/// referenced txn blob is present.
pub async fn verify_commit_chain<C: ContentStore + ?Sized>(
    store: &C,
    head: &ContentId,
    max_commits: Option<usize>,
) -> Result<(usize, usize, bool, Vec<VerifyProblem>)> {
    let mut problems = Vec::new();
    let mut frontier: Vec<(ContentId, Option<ChildEdge>)> = vec![(head.clone(), None)];
    let mut visited: HashSet<ContentId> = HashSet::new();
    let mut commits_checked = 0usize;
    let mut txn_refs_checked = 0usize;
    let mut truncated = false;

    while let Some((cid, referenced_by)) = frontier.pop() {
        if !visited.insert(cid.clone()) {
            continue;
        }
        if max_commits.is_some_and(|max| commits_checked >= max) {
            truncated = true;
            break;
        }

        let bytes = match store.get(&cid).await {
            Ok(bytes) => bytes,
            Err(fluree_db_core::Error::NotFound(_)) => {
                problems.push(match referenced_by {
                    Some((id, t, _)) => VerifyProblem::MissingCommit {
                        commit_id: cid,
                        referenced_by: id,
                        referenced_by_t: t,
                    },
                    // Only the head enters the walk unreferenced.
                    None => VerifyProblem::MissingHead { commit_id: cid },
                });
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        commits_checked += 1;

        let env = match read_commit_envelope(&bytes) {
            Ok(env) => env,
            Err(e) => {
                problems.push(VerifyProblem::UnreadableCommit {
                    commit_id: cid,
                    error: e.to_string(),
                });
                continue;
            }
        };

        if let Some(txn_id) = &env.txn {
            txn_refs_checked += 1;
            if !store.has(txn_id).await? {
                problems.push(VerifyProblem::MissingTxnBlob {
                    t: env.t,
                    commit_id: cid.clone(),
                    txn_id: txn_id.clone(),
                });
            }
        }

        // Contiguity is checked against the child that led here, and only
        // along the primary-parent edge — merge parents legitimately sit at
        // an arbitrary older t.
        if let Some((child_id, child_t, primary)) = &referenced_by {
            if *primary && child_t - 1 != env.t {
                problems.push(VerifyProblem::TGap {
                    commit_id: child_id.clone(),
                    t: *child_t,
                    parent_id: cid.clone(),
                    parent_t: env.t,
                });
            }
        }
        // Pushed in reverse so the primary parent is popped first: the
        // frontier is a stack, and walking the primary lineage first keeps
        // `--limit` on the newest-first path and reaches a shared ancestor
        // by its primary edge (where the `t` contiguity check applies)
        // rather than marking it visited via a merge edge.
        for (idx, parent) in env.parents.iter().enumerate().rev() {
            frontier.push((parent.clone(), Some((cid.clone(), env.t, idx == 0))));
        }
    }

    Ok((commits_checked, txn_refs_checked, truncated, problems))
}

impl crate::Fluree {
    /// Verify a ledger's commit chain and referenced objects.
    ///
    /// Read-only. Uses a branch-aware content store so the walk crosses
    /// fork points. `max_commits` bounds the walk for very long chains.
    pub async fn verify_ledger(
        &self,
        ledger_id: &str,
        max_commits: Option<usize>,
    ) -> Result<LedgerVerifyReport> {
        let record = self
            .nameservice()
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;

        let store = fluree_db_nameservice::branched_content_store_for_record(
            self.backend(),
            self.nameservice(),
            &record,
        )
        .await?;

        let mut problems = Vec::new();
        let (commits_checked, txn_refs_checked, truncated) = match &record.commit_head_id {
            Some(head) => {
                let (c, t, truncated, p) = verify_commit_chain(&store, head, max_commits).await?;
                problems.extend(p);
                (c, t, truncated)
            }
            None => (0, 0, false),
        };

        if let Some(index_id) = &record.index_head_id {
            if !store.has(index_id).await? {
                problems.push(VerifyProblem::MissingIndexRoot {
                    index_id: index_id.clone(),
                    index_t: record.index_t,
                });
            }
        }

        Ok(LedgerVerifyReport {
            ledger_id: record.ledger_id.clone(),
            head_commit_id: record.commit_head_id.clone(),
            head_t: record.commit_t,
            index_id: record.index_head_id.clone(),
            index_t: record.index_t,
            commits_checked,
            txn_refs_checked,
            truncated,
            problems,
        })
    }
}
