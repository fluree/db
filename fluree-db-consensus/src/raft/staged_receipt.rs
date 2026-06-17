//! Per-leader side channel carrying staged-time receipt details from
//! the [`CommitWorker`](super::commit_worker::CommitWorker) to the
//! [`QueuedTransactor`](super::queued_transactor::QueuedTransactor)
//! that registered the waiter.
//!
//! The state machine only persists what's canonical to "did this
//! submission succeed and what head did it produce" — the
//! [`ApplyRecord`](super::state_machine::ApplyRecord) it caches under
//! `state.idempotency` has the head, t, and the request CID. Anything
//! that's only meaningful at stage time (conflict counts, replay
//! counts, indexing snapshots, etc.) is not replicated; it lives here
//! and is delivered to the awaiting transactor through the waiter
//! map on the way up.
//!
//! Scope is per-process. A leader transition strands stashed
//! receipts on the former leader, in which case the apply on the new
//! leader resolves the waiter with [`AppliedReceipt::Minimal`] and
//! callers fall back to conservative defaults — same recovery shape
//! as the [`WaiterMap`](super::waiter::WaiterMap) itself.

use dashmap::DashMap;
use fluree_db_api::{ConflictStrategy, IndexingStatus, TrackingTally};
use fluree_db_core::{CommitId, ContentId};

/// Typed receipt the worker stashes after staging and before
/// proposing [`ApplyHead`](super::state_machine::Command::ApplyHead).
///
/// One variant per queue-mediated `Committer` method, plus
/// [`Minimal`](Self::Minimal) for the fallback case where the
/// adapter resolves a waiter without finding a stashed receipt.
#[derive(Debug)]
pub enum AppliedReceipt {
    Transact(TransactApplied),
    Push(PushApplied),
    Revert(RevertApplied),
    Merge(MergeApplied),
    Rebase(RebaseApplied),
    /// Adapter resolved the waiter from an apply whose worker
    /// didn't stash side-channel data — most commonly a leader
    /// transition where the receipt is stranded on the former
    /// leader. Callers fall back to conservative defaults.
    Minimal { commit_id: ContentId, commit_t: i64 },
}

impl AppliedReceipt {
    /// Borrow the head identity that came out of the apply. Useful
    /// for callers that only need the head and don't care about
    /// per-op detail.
    pub fn commit_id(&self) -> &ContentId {
        match self {
            AppliedReceipt::Transact(r) => &r.commit_id,
            AppliedReceipt::Push(r) => &r.commit_id,
            AppliedReceipt::Revert(r) => &r.commit_id,
            AppliedReceipt::Merge(r) => &r.commit_id,
            AppliedReceipt::Rebase(r) => &r.commit_id,
            AppliedReceipt::Minimal { commit_id, .. } => commit_id,
        }
    }

    pub fn commit_t(&self) -> i64 {
        match self {
            AppliedReceipt::Transact(r) => r.commit_t,
            AppliedReceipt::Push(r) => r.commit_t,
            AppliedReceipt::Revert(r) => r.commit_t,
            AppliedReceipt::Merge(r) => r.commit_t,
            AppliedReceipt::Rebase(r) => r.commit_t,
            AppliedReceipt::Minimal { commit_t, .. } => *commit_t,
        }
    }
}

#[derive(Debug)]
pub struct TransactApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub tally: Option<TrackingTally>,
}

#[derive(Debug)]
pub struct PushApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub accepted: usize,
    pub indexing: IndexingStatus,
}

#[derive(Debug)]
pub struct RevertApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub reverted_commits: Vec<CommitId>,
    pub conflict_count: usize,
    pub strategy: ConflictStrategy,
}

#[derive(Debug)]
pub struct MergeApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub fast_forward: bool,
    pub commits_copied: usize,
    pub conflict_count: usize,
    pub strategy: ConflictStrategy,
}

#[derive(Debug)]
pub struct RebaseApplied {
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub fast_forward: bool,
    pub replayed: usize,
    pub skipped: usize,
    pub conflicts: usize,
    pub failures: usize,
    pub total_commits: usize,
    pub source_head_t: i64,
    pub source_head_id: ContentId,
    pub strategy: ConflictStrategy,
}

/// Concurrent map from queue_id → stashed [`AppliedReceipt`].
///
/// Held jointly by the state-machine adapter (consumer) and the
/// commit worker (producer) via `Arc`.
#[derive(Default)]
pub struct StagedReceiptMap {
    receipts: DashMap<u64, AppliedReceipt>,
}

impl StagedReceiptMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Park a receipt under `queue_id`. Called by the worker after
    /// staging and before proposing `ApplyHead`.
    pub fn stash(&self, queue_id: u64, receipt: AppliedReceipt) {
        self.receipts.insert(queue_id, receipt);
    }

    /// Remove and return the receipt for `queue_id`. The state-
    /// machine adapter calls this when resolving a waiter on the
    /// matching apply; the worker also calls it for cleanup when a
    /// propose fails after a stash.
    pub fn take(&self, queue_id: u64) -> Option<AppliedReceipt> {
        self.receipts.remove(&queue_id).map(|(_, v)| v)
    }

    /// Number of stashed receipts. Test-only.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.receipts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    #[test]
    fn stash_then_take_returns_the_receipt() {
        let map = StagedReceiptMap::new();
        map.stash(
            7,
            AppliedReceipt::Transact(TransactApplied {
                commit_id: cid(1),
                commit_t: 10,
                tally: None,
            }),
        );
        match map.take(7) {
            Some(AppliedReceipt::Transact(r)) => {
                assert_eq!(r.commit_id, cid(1));
                assert_eq!(r.commit_t, 10);
            }
            other => panic!("expected Transact, got {other:?}"),
        }
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn take_on_unknown_queue_id_is_none() {
        let map = StagedReceiptMap::new();
        assert!(map.take(9_999).is_none());
    }

    #[test]
    fn commit_id_and_commit_t_accessors_match_variant_payload() {
        let receipt = AppliedReceipt::Merge(MergeApplied {
            commit_id: cid(7),
            commit_t: 42,
            fast_forward: false,
            commits_copied: 3,
            conflict_count: 0,
            strategy: ConflictStrategy::TakeBoth,
        });
        assert_eq!(receipt.commit_id(), &cid(7));
        assert_eq!(receipt.commit_t(), 42);
    }
}
