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

use crate::raft::staged_receipt::AppliedReceipt;
use crate::raft::state_machine::{PoisonReason, RefKey};
use dashmap::DashMap;
use tokio::sync::oneshot;

/// Outcome the state-machine adapter sends back through the channel
/// the transactor parked on.
///
/// `Applied` is the success path — the head advanced under the
/// queue_id the transactor handed in. The carried [`AppliedReceipt`]
/// gives the transactor the per-op staging detail it needs to build
/// a faithful receipt (commit count, conflict count, etc.); it
/// falls back to [`AppliedReceipt::Minimal`] when the side-channel
/// stash was lost (typically a former-leader scenario).
///
/// `Aborted` covers every way the entry left the queue without a
/// head advance (poison + admin preemption).
#[derive(Debug)]
pub enum WaiterOutcome {
    Applied(AppliedReceipt),
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
    /// The branch was soft-dropped via `RetractLedger`. The flag
    /// flip drains the queue alongside it (see the comment on
    /// [`ClearReason::BranchRetracted`](crate::raft::state_machine::ClearReason::BranchRetracted)),
    /// so in-flight waiters from before the retract get this
    /// reason instead of a head-mutating `BranchHeadReset`.
    BranchRetracted,
    /// The state machine was rebuilt from an install_snapshot, so
    /// every locally-tracked queue_id (parked or buffered) is
    /// abandoned: the entry may or may not exist in the new state,
    /// and the prior leader's local outcome is no longer
    /// authoritative.
    SnapshotInstalled,
    Poisoned(PoisonReason),
}

/// Per-queue_id slot. Either a parked sender (the proposer made it
/// here before the worker resolved) or a buffered outcome (the worker
/// resolved before the proposer parked its waiter — the race the
/// pre-buffer design fixes).
enum WaiterSlot {
    /// Proposer parked first. The next `resolve_*` call sends through
    /// `sender` and removes the slot.
    Parked {
        ref_key: RefKey,
        sender: oneshot::Sender<WaiterOutcome>,
    },
    /// Worker resolved first. The next `register` call pulls the
    /// outcome out, delivers it on the new sender, and removes the
    /// slot.
    ///
    /// Buffered slots have no `ref_key` because the resolve already
    /// happened — there's no waiter for an admin clear to sweep, only
    /// a value to hand off to the eventual register.
    Resolved(WaiterOutcome),
}

/// Concurrent map from queue_id → parked waiter or buffered outcome.
///
/// Held by the state-machine adapter and shared with the transactor
/// (via `Arc`). Designed to be order-agnostic: a `resolve_*` call that
/// lands before the matching `register` buffers the outcome, and the
/// late-arriving `register` picks it up and completes immediately.
/// Without that buffering, a fast leader (worker proposes `ApplyHead`
/// before the original `EnqueueCommand` response reaches the
/// transactor) would resolve to a nonexistent slot and the transactor
/// would time out on the still-empty receiver — silently re-proposing
/// under a fresh `request_cid` and producing a duplicate commit on
/// retry.
///
/// Buffered slots leak only when a transactor's `client_write` is
/// dropped between commit and resolve (rare network failure mid-RPC).
/// The footprint is one [`WaiterOutcome`] per leaked slot; if a
/// production load exposes this we can add a TTL sweep.
#[derive(Default)]
pub struct WaiterMap {
    slots: DashMap<u64, WaiterSlot>,
}

impl WaiterMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Park a waiter on `queue_id` and return the receiver the caller
    /// awaits. `ref_key` is recorded alongside so admin commands can
    /// sweep every waiter for a branch in one pass.
    ///
    /// If a [`WaiterSlot::Resolved`] is already buffered for this id
    /// (the worker beat the caller back from `client_write`) the
    /// buffered outcome is delivered to the new sender immediately
    /// and the slot is removed.
    ///
    /// If a [`WaiterSlot::Parked`] is already there (a stale
    /// registration from a former leader) the prior sender is dropped
    /// — its receiver gets a [`oneshot::error::RecvError`], which the
    /// caller treats as "the waiter is gone, restart by re-issuing
    /// the idempotency key."
    pub fn register(&self, queue_id: u64, ref_key: RefKey) -> oneshot::Receiver<WaiterOutcome> {
        let (sender, receiver) = oneshot::channel();
        use dashmap::mapref::entry::Entry;
        match self.slots.entry(queue_id) {
            Entry::Vacant(v) => {
                v.insert(WaiterSlot::Parked { ref_key, sender });
            }
            Entry::Occupied(mut o) => {
                if matches!(o.get(), WaiterSlot::Resolved(_)) {
                    // Pre-buffered outcome — deliver and remove.
                    if let WaiterSlot::Resolved(outcome) = o.remove() {
                        let _ = sender.send(outcome);
                    }
                } else {
                    // Duplicate register; the prior sender drops.
                    *o.get_mut() = WaiterSlot::Parked { ref_key, sender };
                }
            }
        }
        receiver
    }

    /// Resolve `queue_id` with the head advance the worker landed.
    ///
    /// If a [`WaiterSlot::Parked`] is registered, send the outcome to
    /// the parked sender and remove the slot. If no slot exists yet
    /// (the worker beat the proposer's `register` call), buffer the
    /// outcome on the slot — the eventual `register` will pick it up.
    pub fn resolve_applied(&self, queue_id: u64, receipt: AppliedReceipt) {
        self.resolve_with(queue_id, WaiterOutcome::Applied(receipt));
    }

    /// Resolve `queue_id` with an abort outcome. Same buffering rule
    /// as [`Self::resolve_applied`] — buffered if no waiter is
    /// registered yet.
    pub fn resolve_aborted(&self, queue_id: u64, reason: AbortReason) {
        self.resolve_with(queue_id, WaiterOutcome::Aborted(reason));
    }

    fn resolve_with(&self, queue_id: u64, outcome: WaiterOutcome) {
        use dashmap::mapref::entry::Entry;
        match self.slots.entry(queue_id) {
            Entry::Vacant(v) => {
                // Race: resolve arrived before register. Buffer for
                // late-arriving register.
                v.insert(WaiterSlot::Resolved(outcome));
            }
            Entry::Occupied(mut o) => {
                if matches!(o.get(), WaiterSlot::Parked { .. }) {
                    if let WaiterSlot::Parked { sender, .. } = o.remove() {
                        let _ = sender.send(outcome);
                    }
                } else {
                    // Already resolved — latest wins. This shouldn't
                    // happen in normal operation (one queue_id maps
                    // to one apply outcome) but is defended against
                    // here so a duplicate adapter call doesn't drop
                    // either outcome on the floor silently.
                    *o.get_mut() = WaiterSlot::Resolved(outcome);
                }
            }
        }
    }

    /// Abort every waiter parked on the given branch with the same
    /// reason. Called when head-mutating admin commands (Drop /
    /// Purge / ResetHead) clear the per-branch queue.
    ///
    /// Only [`WaiterSlot::Parked`] slots are swept — buffered
    /// resolutions are left alone because their underlying queue
    /// entry already completed before the admin clear; the
    /// late-arriving `register` should still see the success.
    pub fn abort_all_for_branch(&self, ref_key: &RefKey, reason: AbortReason) {
        let to_resolve: Vec<u64> = self
            .slots
            .iter()
            .filter(|entry| {
                matches!(
                    entry.value(),
                    WaiterSlot::Parked { ref_key: rk, .. } if rk == ref_key
                )
            })
            .map(|entry| *entry.key())
            .collect();
        for queue_id in to_resolve {
            self.resolve_aborted(queue_id, reason.clone());
        }
    }

    /// Resolve every parked slot with `reason` and drop every
    /// buffered slot. Called by the state-machine adapter on
    /// install_snapshot: the in-memory queue ids tracked here belong
    /// to the pre-snapshot state, and neither parked proposers nor
    /// buffered outcomes can be trusted once the local state has
    /// been replaced by a snapshot the prior leader didn't produce.
    ///
    /// Distinct from [`Self::abort_all_for_branch`], which
    /// intentionally preserves buffered resolutions because their
    /// underlying entry actually applied — that invariant does not
    /// hold across a snapshot install.
    pub fn drain_all_with(&self, reason: AbortReason) {
        let queue_ids: Vec<u64> = self.slots.iter().map(|entry| *entry.key()).collect();
        for queue_id in queue_ids {
            if let Some((_, slot)) = self.slots.remove(&queue_id) {
                if let WaiterSlot::Parked { sender, .. } = slot {
                    let _ = sender.send(WaiterOutcome::Aborted(reason.clone()));
                }
            }
        }
    }

    /// Number of slots (parked + buffered). Used by tests; not part
    /// of the stable surface.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True when no slots are populated. Test-only; paired with
    /// [`Self::len`] so clippy's `len_without_is_empty` doesn't flag
    /// the helper.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind};

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn ref_key(branch: &str) -> RefKey {
        RefKey::new("test/db", branch)
    }

    fn minimal(seed: u8, commit_t: i64) -> AppliedReceipt {
        AppliedReceipt::Minimal {
            commit_id: cid(seed),
            commit_t,
        }
    }

    #[tokio::test]
    async fn register_then_resolve_applied_delivers_head() {
        let map = WaiterMap::new();
        let rx = map.register(7, ref_key("main"));
        map.resolve_applied(7, minimal(42, 10));

        match rx.await.expect("receive") {
            WaiterOutcome::Applied(AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            }) => {
                assert_eq!(commit_id, cid(42));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected Applied(Minimal), got {other:?}"),
        }
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn register_then_resolve_aborted_delivers_reason() {
        let map = WaiterMap::new();
        let rx = map.register(7, ref_key("main"));
        map.resolve_aborted(
            7,
            AbortReason::Poisoned(PoisonReason::BodyMalformed {
                error: "bad turtle".into(),
            }),
        );

        match rx.await.expect("receive") {
            WaiterOutcome::Aborted(AbortReason::Poisoned(PoisonReason::BodyMalformed {
                error,
            })) => assert_eq!(error, "bad turtle"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn resolve_on_unknown_queue_id_buffers_for_late_register() {
        let map = WaiterMap::new();
        // Worker resolves before the proposer's register call lands.
        map.resolve_applied(9_999, minimal(1, 1));
        assert_eq!(
            map.len(),
            1,
            "buffered slot stays until register picks it up"
        );

        // Late-arriving register pulls the buffered outcome out and
        // delivers it on the new receiver.
        let rx = map.register(9_999, ref_key("main"));
        match rx.await.expect("buffered outcome delivered") {
            WaiterOutcome::Applied(AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            }) => {
                assert_eq!(commit_id, cid(1));
                assert_eq!(commit_t, 1);
            }
            other => panic!("expected Applied(Minimal), got {other:?}"),
        }
        assert_eq!(map.len(), 0, "slot drained after delivery");
    }

    #[tokio::test]
    async fn resolve_aborted_before_register_also_buffers() {
        let map = WaiterMap::new();
        map.resolve_aborted(7, AbortReason::BranchDropped);
        let rx = map.register(7, ref_key("main"));
        assert!(matches!(
            rx.await.expect("buffered abort"),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn abort_all_does_not_disturb_buffered_resolves() {
        let map = WaiterMap::new();
        // Worker resolved first, buffered.
        map.resolve_applied(7, minimal(42, 10));
        // Admin clear fires before the proposer registers — should
        // not touch the buffered Applied (the work completed before
        // the clear was proposed).
        map.abort_all_for_branch(&ref_key("main"), AbortReason::BranchDropped);
        // Register still picks up the Applied outcome.
        let rx = map.register(7, ref_key("main"));
        assert!(matches!(
            rx.await.expect("buffered applied"),
            WaiterOutcome::Applied(_)
        ));
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
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), main_rx,)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn drain_all_resolves_parked_and_drops_buffered() {
        let map = WaiterMap::new();
        // Parked: a proposer waiting on queue_id 1.
        let parked_rx = map.register(1, ref_key("main"));
        // Buffered: worker resolved 2 before the proposer registered;
        // unlike abort_all_for_branch this drain MUST drop the
        // buffered slot too because the post-snapshot state can't
        // honor it.
        map.resolve_applied(2, minimal(42, 10));
        assert_eq!(map.len(), 2);

        map.drain_all_with(AbortReason::SnapshotInstalled);

        assert!(matches!(
            parked_rx.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::SnapshotInstalled)
        ));
        assert!(map.is_empty(), "buffered slot must be dropped, not retained");
    }

    #[tokio::test]
    async fn duplicate_register_drops_the_old_sender() {
        let map = WaiterMap::new();
        let stale_rx = map.register(7, ref_key("main"));
        let fresh_rx = map.register(7, ref_key("main"));

        map.resolve_applied(7, minimal(42, 10));

        // The stale receiver sees the channel closed (no outcome).
        assert!(stale_rx.await.is_err());
        // The fresh one gets the outcome.
        assert!(matches!(fresh_rx.await.unwrap(), WaiterOutcome::Applied(_)));
    }
}
