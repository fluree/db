//! Per-process bridge between `EnqueueCommand` proposers and the
//! state-machine adapter that observes the entry's terminal apply.
//!
//! ## Interest, not registration
//!
//! A proposer does not know its `queue_id` until the `EnqueueCommand`
//! has already applied — the state machine assigns it. The obvious
//! design, "register once `client_write` returns," therefore has a race:
//! on a fast leader the worker can propose and land `ApplyHead` before
//! the enqueue response gets back to the proposer, resolving against a
//! slot that does not exist yet.
//!
//! Buffering the outcome for a late registration closes that race, but
//! it cannot tell a late proposer from an absent one — and on a
//! follower the proposer is always absent, because
//! [`QueuedTransactor`](super::queued_transactor::QueuedTransactor)
//! refuses submissions there. Every terminal apply on every non-leader
//! node then buffers an outcome nobody will ever collect.
//!
//! So interest is armed **before** proposing, keyed by the submission's
//! `request_cid` — which the proposer knows and the command carries.
//! When this node applies that `EnqueueCommand`, the adapter binds the
//! interest to the `queue_id` the state machine just assigned. Because
//! the binding happens during apply, it strictly precedes any later
//! `ApplyHead` for the same id, so the race closes structurally and
//! there is nothing left to buffer.
//!
//! A resolve for a `queue_id` with no bound waiter is simply dropped:
//! on a follower that is every terminal apply, and it costs nothing.
//!
//! ## Scope
//!
//! Per-process, but not leader-only: `ApplyHead` replicates to every
//! node, so a waiter bound on a former leader still resolves when the
//! new leader's worker finishes the entry. What strands a waiter is the
//! entry leaving the replicated queue without a terminal apply this
//! node observes (a snapshot install, a partition). The transactor
//! parks on the ticket in probe intervals: a timeout that finds the
//! entry still queued keeps waiting; one that finds it gone waits a
//! short grace for the outcome, then spends a retry attempt on an
//! idempotency-keyed re-issue (see `QueuedTransactor`).

use crate::raft::staged_receipt::AppliedReceipt;
use crate::raft::state_machine::{PoisonReason, RefKey};
use dashmap::DashMap;
use fluree_db_core::ContentId;
use std::sync::{Arc, OnceLock};
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
    /// every locally-tracked waiter is abandoned: the entry may or
    /// may not exist in the new state, and the prior leader's local
    /// outcome is no longer authoritative.
    SnapshotInstalled,
    Poisoned(PoisonReason),
}

/// A waiter whose `queue_id` is not known yet.
struct Interest {
    ref_key: RefKey,
    sender: oneshot::Sender<WaiterOutcome>,
    /// Set by [`WaiterMap::bind`]; lets the ticket clean up the right
    /// entry on drop without knowing in advance which map it landed in.
    bound: Arc<OnceLock<u64>>,
}

/// A waiter bound to a `queue_id`, awaiting its terminal apply.
struct Waiter {
    ref_key: RefKey,
    sender: oneshot::Sender<WaiterOutcome>,
    /// The `Arc` this waiter's ticket holds. Both maps are keyed by
    /// something a second submission can collide on — `request_cid` for
    /// interests, `queue_id` for waiters — so a displaced ticket must
    /// compare identity before removing, or its `Drop` deletes the
    /// entry that displaced it.
    bound: Arc<OnceLock<u64>>,
}

/// Handle a proposer holds while it waits.
///
/// Dropping it removes whichever entry the waiter currently occupies,
/// so abandoning a submission — timeout, cancellation, a panic on the
/// propose path — cannot leave anything behind.
pub struct WaiterTicket {
    map: Arc<WaiterMap>,
    request_cid: ContentId,
    bound: Arc<OnceLock<u64>>,
    receiver: Option<oneshot::Receiver<WaiterOutcome>>,
}

/// Why a wait ended without an outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum WaitError {
    /// No terminal apply arrived in time. The ticket stays valid — the
    /// waiter is still bound, so a retry that rejoins the same queue
    /// entry can await it again.
    TimedOut,
    /// Another submission bound to this `queue_id` and took the slot,
    /// or the waiter was drained. Retry under the idempotency key.
    Displaced,
}

impl WaiterTicket {
    /// Await the terminal outcome, giving up after `timeout`.
    ///
    /// Takes `&mut self` and borrows the receiver rather than consuming
    /// it, so a timed-out ticket can be awaited again: the retry loop
    /// re-proposes the same `request_cid`, rejoins the same queue entry
    /// (`InFlight`), and waits on the binding it already has.
    pub async fn wait(&mut self, timeout: std::time::Duration) -> Result<WaiterOutcome, WaitError> {
        let Some(rx) = self.receiver.as_mut() else {
            return Err(WaitError::Displaced);
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_)) => {
                // Sender dropped: displaced by a later bind, or drained.
                self.receiver = None;
                Err(WaitError::Displaced)
            }
            Err(_elapsed) => Err(WaitError::TimedOut),
        }
    }

    /// The `queue_id` this ticket bound to, once the local node has
    /// applied the enqueue. `None` until then.
    pub fn queue_id(&self) -> Option<u64> {
        self.bound.get().copied()
    }
}

impl Drop for WaiterTicket {
    fn drop(&mut self) {
        // Remove only this ticket's own entry. A displaced ticket still
        // names the slot it briefly held — the same `queue_id`, or the
        // same `request_cid` — so removing by key alone would delete the
        // binding the *current* holder is waiting on, and its outcome
        // would be dropped on the floor when the terminal apply lands.
        // The `bound` `Arc` is shared between a ticket and whichever
        // entry it owns, so pointer identity settles it.
        match self.bound.get() {
            Some(queue_id) => {
                self.map
                    .waiters
                    .remove_if(queue_id, |_, w| Arc::ptr_eq(&w.bound, &self.bound));
            }
            None => {
                self.map
                    .interests
                    .remove_if(&self.request_cid, |_, i| Arc::ptr_eq(&i.bound, &self.bound));
            }
        }
    }
}

/// Per-process registry of local proposers awaiting terminal applies.
///
/// Held by the state-machine adapter and shared with the transactor via
/// `Arc`. Only entries a *local* proposer armed are ever tracked, so a
/// follower's map stays empty no matter how much the cluster commits.
#[derive(Default)]
pub struct WaiterMap {
    /// `request_cid` → armed interest, before a `queue_id` exists.
    interests: DashMap<ContentId, Interest>,
    /// `queue_id` → bound waiter.
    waiters: DashMap<u64, Waiter>,
}

impl WaiterMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm interest in the submission identified by `request_cid`,
    /// **before** proposing it.
    ///
    /// Arming first is what closes the race: by the time this node
    /// applies the enqueue there is already somewhere to bind, so no
    /// terminal apply can arrive with nowhere to go.
    pub fn arm(self: &Arc<Self>, request_cid: ContentId, ref_key: RefKey) -> WaiterTicket {
        let (sender, receiver) = oneshot::channel();
        let bound = Arc::new(OnceLock::new());
        self.interests.insert(
            request_cid.clone(),
            Interest {
                ref_key,
                sender,
                bound: Arc::clone(&bound),
            },
        );
        WaiterTicket {
            map: Arc::clone(self),
            request_cid,
            bound,
            receiver: Some(receiver),
        }
    }

    /// Bind an armed interest to the `queue_id` the state machine
    /// assigned. Called by the adapter when *this node* applies the
    /// matching `EnqueueCommand`.
    ///
    /// No-op when no local proposer armed this `request_cid` — which is
    /// the ordinary case on every follower, and the reason a follower's
    /// map never grows.
    ///
    /// A duplicate submission joining an in-flight entry (`InFlight`)
    /// binds to the same `queue_id` and displaces the earlier waiter,
    /// whose receiver then errors; the caller retries under its
    /// idempotency key.
    pub fn bind(&self, request_cid: &ContentId, queue_id: u64) {
        let Some((_, interest)) = self.interests.remove(request_cid) else {
            return;
        };
        // Publish before inserting: a ticket dropped concurrently must
        // find the waiter entry rather than the (now absent) interest.
        let _ = interest.bound.set(queue_id);
        self.waiters.insert(
            queue_id,
            Waiter {
                ref_key: interest.ref_key,
                sender: interest.sender,
                bound: interest.bound,
            },
        );
    }

    /// Resolve `queue_id` with the head advance the worker landed.
    ///
    /// Dropped when no local waiter is bound — on a follower that is
    /// every terminal apply.
    pub fn resolve_applied(&self, queue_id: u64, receipt: AppliedReceipt) {
        self.resolve_with(queue_id, WaiterOutcome::Applied(receipt));
    }

    /// Resolve `queue_id` with an abort outcome. Same "no waiter, no
    /// work" rule as [`Self::resolve_applied`].
    pub fn resolve_aborted(&self, queue_id: u64, reason: AbortReason) {
        self.resolve_with(queue_id, WaiterOutcome::Aborted(reason));
    }

    fn resolve_with(&self, queue_id: u64, outcome: WaiterOutcome) {
        if let Some((_, waiter)) = self.waiters.remove(&queue_id) {
            let _ = waiter.sender.send(outcome);
        }
    }

    /// Abort every waiter bound to `ref_key`. Called when head-mutating
    /// admin commands (Drop / Purge / ResetHead) clear the queue.
    ///
    /// Unbound interests are left alone: their `EnqueueCommand` has not
    /// applied yet, so it will land against the post-clear state and
    /// resolve on its own terms.
    pub fn abort_all_for_branch(&self, ref_key: &RefKey, reason: AbortReason) {
        let ids: Vec<u64> = self
            .waiters
            .iter()
            .filter(|entry| &entry.value().ref_key == ref_key)
            .map(|entry| *entry.key())
            .collect();
        for queue_id in ids {
            self.resolve_aborted(queue_id, reason.clone());
        }
    }

    /// Abandon every local waiter, bound or not.
    ///
    /// Called on install_snapshot: the state machine has been replaced
    /// wholesale, so neither a bound waiter's entry nor an armed
    /// interest's pending enqueue can be trusted to exist in the new
    /// state.
    pub fn drain_all_with(&self, reason: AbortReason) {
        let ids: Vec<u64> = self.waiters.iter().map(|entry| *entry.key()).collect();
        for queue_id in ids {
            self.resolve_aborted(queue_id, reason.clone());
        }
        let cids: Vec<ContentId> = self.interests.iter().map(|e| e.key().clone()).collect();
        for cid in cids {
            if let Some((_, interest)) = self.interests.remove(&cid) {
                let _ = interest.sender.send(WaiterOutcome::Aborted(reason.clone()));
            }
        }
    }

    /// Arm and bind in one step, for tests that exercise the resolve
    /// path directly rather than running a real propose.
    #[cfg(test)]
    pub fn arm_bound(
        self: &Arc<Self>,
        request_cid: ContentId,
        ref_key: RefKey,
        queue_id: u64,
    ) -> WaiterTicket {
        let ticket = self.arm(request_cid.clone(), ref_key);
        self.bind(&request_cid, queue_id);
        ticket
    }

    /// Number of tracked entries — armed interests plus bound waiters.
    /// Tests only; not part of the public contract.
    pub fn len(&self) -> usize {
        self.interests.len() + self.waiters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{ContentId as ApiContentId, ContentKind};

    fn key(name: &str, branch: &str) -> RefKey {
        RefKey::new(name, branch)
    }

    fn cid(seed: u8) -> ContentId {
        ApiContentId::new(ContentKind::Commit, &[seed])
    }

    fn receipt() -> AppliedReceipt {
        AppliedReceipt::Minimal {
            commit_id: cid(200),
            commit_t: 1,
        }
    }

    #[tokio::test]
    async fn bind_then_resolve_delivers_the_outcome() {
        let map = Arc::new(WaiterMap::new());
        let mut ticket = map.arm(cid(1), key("db", "main"));
        assert_eq!(ticket.queue_id(), None, "unbound until the enqueue applies");

        map.bind(&cid(1), 7);
        assert_eq!(ticket.queue_id(), Some(7));

        map.resolve_applied(7, receipt());
        assert!(matches!(
            ticket
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("outcome delivered"),
            WaiterOutcome::Applied(_)
        ));
    }

    #[tokio::test]
    async fn bind_then_resolve_aborted_delivers_the_reason() {
        let map = Arc::new(WaiterMap::new());
        let mut ticket = map.arm(cid(2), key("db", "main"));
        map.bind(&cid(2), 9);
        map.resolve_aborted(9, AbortReason::BranchDropped);
        assert!(matches!(
            ticket
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("outcome delivered"),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
    }

    /// The leak this design replaces: a node with no local proposer —
    /// every follower — must accumulate nothing, however much the
    /// cluster commits.
    #[tokio::test]
    async fn a_follower_accumulates_nothing() {
        let map = Arc::new(WaiterMap::new());
        for queue_id in 0..1_000 {
            // A follower applies the enqueue (nothing armed locally),
            // then its terminal command.
            map.bind(&cid(1), queue_id);
            map.resolve_applied(queue_id, receipt());
        }
        assert_eq!(
            map.len(),
            0,
            "a node with no local interest must track nothing",
        );
    }

    /// The race that used to require buffering. Arming before the
    /// propose means the binding happens during apply, which strictly
    /// precedes any later terminal apply for the same id.
    #[tokio::test]
    async fn arming_before_propose_closes_the_resolve_race() {
        let map = Arc::new(WaiterMap::new());
        let mut ticket = map.arm(cid(3), key("db", "main"));

        // Enqueue and ApplyHead land back-to-back, before the proposer
        // ever looks at its receiver.
        map.bind(&cid(3), 42);
        map.resolve_applied(42, receipt());

        assert!(matches!(
            ticket
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("outcome still delivered"),
            WaiterOutcome::Applied(_)
        ));
    }

    #[tokio::test]
    async fn dropping_a_ticket_releases_an_unbound_interest() {
        let map = Arc::new(WaiterMap::new());
        let ticket = map.arm(cid(4), key("db", "main"));
        assert_eq!(map.len(), 1);
        drop(ticket);
        assert_eq!(map.len(), 0, "an abandoned interest must not linger");
    }

    #[tokio::test]
    async fn dropping_a_ticket_releases_a_bound_waiter() {
        let map = Arc::new(WaiterMap::new());
        let ticket = map.arm(cid(5), key("db", "main"));
        map.bind(&cid(5), 11);
        assert_eq!(map.len(), 1);
        drop(ticket);
        assert_eq!(map.len(), 0, "an abandoned waiter must not linger");

        // And a later resolve for that id is simply dropped.
        map.resolve_applied(11, receipt());
        assert_eq!(map.len(), 0);
    }

    #[tokio::test]
    async fn abort_all_for_branch_only_touches_matching_waiters() {
        let map = Arc::new(WaiterMap::new());
        let mut main = map.arm(cid(6), key("db", "main"));
        let mut feature = map.arm(cid(7), key("db", "feature"));
        map.bind(&cid(6), 1);
        map.bind(&cid(7), 2);

        map.abort_all_for_branch(&key("db", "main"), AbortReason::BranchPurged);

        assert!(matches!(
            main.wait(std::time::Duration::from_secs(5))
                .await
                .expect("main aborted"),
            WaiterOutcome::Aborted(AbortReason::BranchPurged)
        ));
        assert_eq!(map.len(), 1, "the other branch's waiter must survive");

        map.resolve_applied(2, receipt());
        assert!(matches!(
            feature
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("feature resolved"),
            WaiterOutcome::Applied(_)
        ));
    }

    /// An admin clear cannot see an interest whose enqueue has not
    /// applied yet — that submission will land against the post-clear
    /// state and resolve on its own terms.
    #[tokio::test]
    async fn abort_all_for_branch_leaves_unbound_interests_alone() {
        let map = Arc::new(WaiterMap::new());
        let _pending = map.arm(cid(8), key("db", "main"));
        map.abort_all_for_branch(&key("db", "main"), AbortReason::BranchDropped);
        assert_eq!(
            map.len(),
            1,
            "an unbound interest is not the clear's to sweep"
        );
    }

    #[tokio::test]
    async fn drain_all_abandons_bound_and_unbound_alike() {
        let map = Arc::new(WaiterMap::new());
        let mut bound = map.arm(cid(9), key("db", "main"));
        let mut unbound = map.arm(cid(10), key("db", "main"));
        map.bind(&cid(9), 3);

        map.drain_all_with(AbortReason::SnapshotInstalled);

        assert!(matches!(
            bound
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("bound waiter told"),
            WaiterOutcome::Aborted(AbortReason::SnapshotInstalled)
        ));
        assert!(matches!(
            unbound
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("unbound interest told"),
            WaiterOutcome::Aborted(AbortReason::SnapshotInstalled)
        ));
        assert_eq!(map.len(), 0);
    }

    /// A duplicate submission that joins an in-flight entry displaces
    /// the earlier waiter; its receiver errors and the caller retries
    /// under its idempotency key.
    #[tokio::test]
    async fn binding_a_second_interest_to_one_queue_id_displaces_the_first() {
        let map = Arc::new(WaiterMap::new());
        let mut first = map.arm(cid(11), key("db", "main"));
        map.bind(&cid(11), 5);
        let mut second = map.arm(cid(12), key("db", "main"));
        map.bind(&cid(12), 5);

        assert!(
            matches!(
                first.wait(std::time::Duration::from_millis(50)).await,
                Err(WaitError::Displaced)
            ),
            "displaced waiter must report it",
        );

        // The displaced ticket is bound to the same `queue_id` as the
        // ticket that displaced it. Its cleanup must not take the live
        // binding with it, or the current holder's outcome is dropped
        // on the floor and it times out on a commit that applied.
        drop(first);
        assert_eq!(map.len(), 1, "second's binding must survive first's drop");

        map.resolve_applied(5, receipt());
        assert!(matches!(
            second
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("current waiter resolved"),
            WaiterOutcome::Applied(_)
        ));
    }

    /// Same identity rule on the interests map: re-arming a
    /// `request_cid` before either ticket binds displaces the first,
    /// whose drop must leave the second's armed interest in place —
    /// otherwise nothing is left to bind when the enqueue applies and
    /// the terminal apply has nowhere to go.
    #[tokio::test]
    async fn dropping_a_displaced_interest_leaves_the_live_one_armed() {
        let map = Arc::new(WaiterMap::new());
        let first = map.arm(cid(21), key("db", "main"));
        let mut second = map.arm(cid(21), key("db", "main"));

        drop(first);
        assert_eq!(map.len(), 1, "second's interest must survive first's drop");

        map.bind(&cid(21), 9);
        map.resolve_applied(9, receipt());
        assert!(matches!(
            second
                .wait(std::time::Duration::from_secs(5))
                .await
                .expect("current interest bound and resolved"),
            WaiterOutcome::Applied(_)
        ));
    }
}
