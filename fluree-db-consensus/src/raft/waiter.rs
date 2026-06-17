//! Per-process bridge between `EnqueueCommand` proposers and the
//! state-machine adapter that observes the entry's terminal apply.
//!
//! When the [`QueuedTransactor`](super::queued_transactor::QueuedTransactor)
//! proposes an `EnqueueCommand` and receives back a `queue_id`, it
//! registers a [`oneshot::Sender`] keyed by that id. When the
//! state-machine adapter then applies an `ApplyHead` /
//! `PoisonQueueEntry`, or a head-mutating admin command that cleared
//! the queue, it looks the id up here and sends the outcome back to
//! the awaiting transactor.
//!
//! Scope is per-process — a leader transition strands waiters from
//! the former leader (the new leader's adapter doesn't know they
//! exist). The transactor recovers via a per-call timeout plus
//! idempotency-keyed re-issue (see the design doc, "Leader
//! transition mid-flight").

use crate::raft::state_machine::{PoisonReason, RefKey};
use dashmap::DashMap;
use fluree_db_core::ContentId;
use tokio::sync::oneshot;

/// Outcome the state-machine adapter sends back through the channel
/// the transactor parked on.
///
/// `Applied` is the success path — the head advanced under the
/// queue_id the transactor handed in. `Aborted` covers every way
/// the entry left the queue without a head advance (poison + admin
/// preemption).
#[derive(Debug)]
pub enum WaiterOutcome {
    Applied { commit_id: ContentId, commit_t: i64 },
    Aborted(AbortReason),
}

/// Why a queued entry resolved without advancing the head.
///
/// The variants line up with the state-machine commands that strand
/// queue entries: `PoisonQueueEntry` produces `Poisoned`; the head-
/// mutating admin commands (`DropBranch`, `PurgeLedger`, `ResetHead`)
/// produce the matching branch-level variant for every pending
/// queue_id on the affected branch.
#[derive(Debug, Clone)]
pub enum AbortReason {
    BranchDropped,
    BranchPurged,
    BranchHeadReset,
    Poisoned(PoisonReason),
}

struct WaiterSlot {
    ref_key: RefKey,
    sender: oneshot::Sender<WaiterOutcome>,
}

/// Concurrent map from queue_id → parked waiter.
///
/// Held by the state-machine adapter and shared with the
/// transactor (via `Arc`). Registrations insert; the adapter
/// resolves on apply.
#[derive(Default)]
pub struct WaiterMap {
    waiters: DashMap<u64, WaiterSlot>,
}

impl WaiterMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Park a waiter on `queue_id` and return the receiver the
    /// caller awaits. `ref_key` is recorded alongside so admin
    /// commands can sweep every waiter for a branch in one pass.
    ///
    /// If the same queue_id is already registered (a stale
    /// registration from a former leader, say) the prior sender is
    /// dropped — its receiver gets a [`oneshot::error::RecvError`],
    /// which the caller treats as "the waiter is gone, restart by
    /// re-issuing the idempotency key."
    pub fn register(
        &self,
        queue_id: u64,
        ref_key: RefKey,
    ) -> oneshot::Receiver<WaiterOutcome> {
        let (sender, receiver) = oneshot::channel();
        self.waiters.insert(queue_id, WaiterSlot { ref_key, sender });
        receiver
    }

    /// Resolve `queue_id` with the head advance the worker landed.
    /// No-op if no waiter is registered (e.g. the caller timed out).
    pub fn resolve_applied(&self, queue_id: u64, commit_id: ContentId, commit_t: i64) {
        if let Some((_, slot)) = self.waiters.remove(&queue_id) {
            let _ = slot
                .sender
                .send(WaiterOutcome::Applied { commit_id, commit_t });
        }
    }

    /// Resolve `queue_id` with an abort outcome. No-op if no waiter
    /// is registered.
    pub fn resolve_aborted(&self, queue_id: u64, reason: AbortReason) {
        if let Some((_, slot)) = self.waiters.remove(&queue_id) {
            let _ = slot.sender.send(WaiterOutcome::Aborted(reason));
        }
    }

    /// Abort every waiter parked on the given branch with the same
    /// reason. Called when head-mutating admin commands (Drop /
    /// Purge / ResetHead) clear the per-branch queue.
    pub fn abort_all_for_branch(&self, ref_key: &RefKey, reason: AbortReason) {
        let to_resolve: Vec<u64> = self
            .waiters
            .iter()
            .filter(|entry| &entry.value().ref_key == ref_key)
            .map(|entry| *entry.key())
            .collect();
        for queue_id in to_resolve {
            self.resolve_aborted(queue_id, reason.clone());
        }
    }

    /// Number of parked waiters. Used by tests; not part of the
    /// stable surface.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.waiters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn ref_key(branch: &str) -> RefKey {
        RefKey::new("test/db", branch)
    }

    #[tokio::test]
    async fn register_then_resolve_applied_delivers_head() {
        let map = WaiterMap::new();
        let rx = map.register(7, ref_key("main"));
        map.resolve_applied(7, cid(42), 10);

        match rx.await.expect("receive") {
            WaiterOutcome::Applied { commit_id, commit_t } => {
                assert_eq!(commit_id, cid(42));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn register_then_resolve_aborted_delivers_reason() {
        let map = WaiterMap::new();
        let rx = map.register(7, ref_key("main"));
        map.resolve_aborted(7, AbortReason::Poisoned(PoisonReason::BodyMalformed {
            error: "bad turtle".into(),
        }));

        match rx.await.expect("receive") {
            WaiterOutcome::Aborted(AbortReason::Poisoned(PoisonReason::BodyMalformed {
                error,
            })) => assert_eq!(error, "bad turtle"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn resolve_on_unknown_queue_id_is_noop() {
        let map = WaiterMap::new();
        map.resolve_applied(9_999, cid(1), 1);
        map.resolve_aborted(9_999, AbortReason::BranchDropped);
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn abort_all_for_branch_only_touches_matching_branch() {
        let map = WaiterMap::new();
        let main_rx = map.register(1, ref_key("main"));
        let feature_rx_a = map.register(2, ref_key("feature"));
        let feature_rx_b = map.register(3, ref_key("feature"));

        map.abort_all_for_branch(&ref_key("feature"), AbortReason::BranchDropped);

        // The two `feature` waiters drained; `main` is untouched.
        assert_eq!(map.len(), 1);
        assert!(matches!(
            feature_rx_a.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
        assert!(matches!(
            feature_rx_b.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
        // The `main` waiter's receiver is still parked.
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            main_rx,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn duplicate_register_drops_the_old_sender() {
        let map = WaiterMap::new();
        let stale_rx = map.register(7, ref_key("main"));
        let fresh_rx = map.register(7, ref_key("main"));

        map.resolve_applied(7, cid(42), 10);

        // The stale receiver sees the channel closed (no outcome).
        assert!(stale_rx.await.is_err());
        // The fresh one gets the outcome.
        assert!(matches!(
            fresh_rx.await.unwrap(),
            WaiterOutcome::Applied { .. }
        ));
    }
}
