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
//! envelope, branch, and idempotency identity, all carried by the command.
//! Content identity alone is insufficient: distinct submissions may have
//! identical envelopes, including absent or identical timestamps.
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
use crate::IdempotencyCacheKey;
use fluree_db_core::ContentId;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
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
#[derive(Debug, Clone)]
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

/// Exact correlation identity available before the queue assigns an ID.
/// Content addressing identifies bytes, not a unique submission.
#[derive(Clone, PartialEq, Eq, Hash)]
struct InterestKey {
    request_cid: ContentId,
    ref_key: RefKey,
    idempotency: Option<IdempotencyCacheKey>,
}

struct Waiter {
    ref_key: RefKey,
    sender: oneshot::Sender<WaiterOutcome>,
    bound: Arc<OnceLock<u64>>,
}

/// Handle owned by one local proposer. Cancellation removes only this handle.
pub struct WaiterTicket {
    map: Arc<WaiterMap>,
    key: InterestKey,
    bound: Arc<OnceLock<u64>>,
    receiver: Option<oneshot::Receiver<WaiterOutcome>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WaitError {
    TimedOut,
    /// The sender was abandoned without a terminal outcome.
    Displaced,
}

impl WaiterTicket {
    pub async fn wait(&mut self, timeout: std::time::Duration) -> Result<WaiterOutcome, WaitError> {
        let Some(rx) = self.receiver.as_mut() else {
            return Err(WaitError::Displaced);
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_)) => {
                self.receiver = None;
                Err(WaitError::Displaced)
            }
            Err(_) => Err(WaitError::TimedOut),
        }
    }

    pub fn queue_id(&self) -> Option<u64> {
        self.bound.get().copied()
    }
}

impl Drop for WaiterTicket {
    fn drop(&mut self) {
        // Binding and cancellation use the same short synchronous lock, so
        // cancellation cannot fall between removal of an interest and insertion
        // of its bound waiter. Never hold this lock across an await.
        let mut state = self.map.state.lock().unwrap();
        if let Some(id) = self.bound.get() {
            if let Some(waiters) = state.waiters.get_mut(id) {
                waiters.retain(|w| !Arc::ptr_eq(&w.bound, &self.bound));
                if waiters.is_empty() {
                    state.waiters.remove(id);
                }
            }
        } else if let Some(interests) = state.interests.get_mut(&self.key) {
            interests.retain(|w| !Arc::ptr_eq(&w.bound, &self.bound));
            if interests.is_empty() {
                state.interests.remove(&self.key);
            }
        }
    }
}

#[derive(Default)]
struct WaiterState {
    interests: HashMap<InterestKey, VecDeque<Waiter>>,
    waiters: HashMap<u64, Vec<Waiter>>,
}

/// Local proposer interests only. Followers retain no unsolicited outcomes.
#[derive(Default)]
pub struct WaiterMap {
    state: Mutex<WaiterState>,
}

impl WaiterMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arm_submission(
        self: &Arc<Self>,
        request_cid: ContentId,
        ref_key: RefKey,
        idempotency: Option<IdempotencyCacheKey>,
    ) -> WaiterTicket {
        let key = InterestKey {
            request_cid,
            ref_key: ref_key.clone(),
            idempotency,
        };
        let (sender, receiver) = oneshot::channel();
        let bound = Arc::new(OnceLock::new());
        self.state
            .lock()
            .unwrap()
            .interests
            .entry(key.clone())
            .or_default()
            .push_back(Waiter {
                ref_key,
                sender,
                bound: Arc::clone(&bound),
            });
        WaiterTicket {
            map: Arc::clone(self),
            key,
            bound,
            receiver: Some(receiver),
        }
    }

    /// Bind using the identity of the command that actually applied.
    /// Keyed duplicates share one queue entry and all receive its outcome.
    /// Unkeyed identical submissions each create an entry: bind one equivalent
    /// interest per apply, never collapse them into one transaction.
    pub fn bind_submission(
        &self,
        request_cid: &ContentId,
        ref_key: &RefKey,
        idempotency: Option<&IdempotencyCacheKey>,
        queue_id: u64,
    ) {
        let key = InterestKey {
            request_cid: request_cid.clone(),
            ref_key: ref_key.clone(),
            idempotency: idempotency.cloned(),
        };
        let mut state = self.state.lock().unwrap();
        let Some(interests) = state.interests.get_mut(&key) else {
            return;
        };
        let bound: Vec<_> = if idempotency.is_some() {
            interests.drain(..).collect()
        } else {
            interests.pop_front().into_iter().collect()
        };
        if interests.is_empty() {
            state.interests.remove(&key);
        }
        let waiters = state.waiters.entry(queue_id).or_default();
        for interest in bound {
            let _ = interest.bound.set(queue_id);
            waiters.push(interest);
        }
    }

    pub fn resolve_applied(&self, queue_id: u64, receipt: AppliedReceipt) {
        self.resolve_with(queue_id, WaiterOutcome::Applied(receipt));
    }

    pub fn resolve_aborted(&self, queue_id: u64, reason: AbortReason) {
        self.resolve_with(queue_id, WaiterOutcome::Aborted(reason));
    }

    fn resolve_with(&self, queue_id: u64, outcome: WaiterOutcome) {
        let waiters = self.state.lock().unwrap().waiters.remove(&queue_id);
        if let Some(waiters) = waiters {
            for waiter in waiters {
                let _ = waiter.sender.send(outcome.clone());
            }
        }
    }

    pub fn abort_all_for_branch(&self, ref_key: &RefKey, reason: AbortReason) {
        let mut state = self.state.lock().unwrap();
        state.waiters.retain(|_, waiters| {
            if waiters.first().is_some_and(|w| &w.ref_key == ref_key) {
                for waiter in waiters.drain(..) {
                    let _ = waiter.sender.send(WaiterOutcome::Aborted(reason.clone()));
                }
                false
            } else {
                true
            }
        });
    }

    pub fn drain_all_with(&self, reason: AbortReason) {
        let mut state = self.state.lock().unwrap();
        for (_, waiters) in state.waiters.drain() {
            for waiter in waiters {
                let _ = waiter.sender.send(WaiterOutcome::Aborted(reason.clone()));
            }
        }
        for (_, interests) in state.interests.drain() {
            for interest in interests {
                let _ = interest.sender.send(WaiterOutcome::Aborted(reason.clone()));
            }
        }
    }

    #[cfg(test)]
    pub fn arm(self: &Arc<Self>, request_cid: ContentId, ref_key: RefKey) -> WaiterTicket {
        self.arm_submission(request_cid, ref_key, None)
    }

    // Existing single-interest fixtures do not need to spell out correlation.
    #[cfg(test)]
    pub fn bind(&self, request_cid: &ContentId, queue_id: u64) {
        let key = self
            .state
            .lock()
            .unwrap()
            .interests
            .keys()
            .find(|k| &k.request_cid == request_cid)
            .cloned();
        if let Some(key) = key {
            self.bind_submission(
                request_cid,
                &key.ref_key,
                key.idempotency.as_ref(),
                queue_id,
            );
        }
    }

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

    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.interests.values().map(VecDeque::len).sum::<usize>()
            + state.waiters.values().map(Vec::len).sum::<usize>()
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

    fn idem(value: &str) -> IdempotencyCacheKey {
        IdempotencyCacheKey::new("db:main", crate::IdempotencyKey::new(value).unwrap())
    }

    async fn assert_t(ticket: &mut WaiterTicket, expected: i64) {
        match ticket
            .wait(std::time::Duration::from_secs(1))
            .await
            .unwrap()
        {
            WaiterOutcome::Applied(AppliedReceipt::Minimal { commit_t, .. }) => {
                assert_eq!(commit_t, expected);
            }
            outcome => panic!("unexpected {outcome:?}"),
        }
    }

    fn receipt_at(t: i64) -> AppliedReceipt {
        AppliedReceipt::Minimal {
            commit_id: cid(t as u8),
            commit_t: t,
        }
    }

    #[tokio::test]
    async fn identical_envelopes_with_distinct_keys_receive_their_own_receipts() {
        let map = Arc::new(WaiterMap::new());
        let branch = key("db", "main");
        let a = idem("a");
        let b = idem("b");
        let mut first = map.arm_submission(cid(1), branch.clone(), Some(a.clone()));
        let mut second = map.arm_submission(cid(1), branch.clone(), Some(b.clone()));
        // Apply in reverse arm order; byte identity must not cross-wire receipts.
        map.bind_submission(&cid(1), &branch, Some(&b), 2);
        map.bind_submission(&cid(1), &branch, Some(&a), 1);
        map.resolve_applied(2, receipt_at(22));
        map.resolve_applied(1, receipt_at(11));
        assert_t(&mut first, 11).await;
        assert_t(&mut second, 22).await;
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn duplicate_key_waiters_fan_out_and_cancellation_does_not_displace_peers() {
        let map = Arc::new(WaiterMap::new());
        let branch = key("db", "main");
        let id = idem("a");
        let mut first = map.arm_submission(cid(1), branch.clone(), Some(id.clone()));
        let abandoned = map.arm_submission(cid(1), branch.clone(), Some(id.clone()));
        let mut second = map.arm_submission(cid(1), branch.clone(), Some(id.clone()));
        drop(abandoned);
        map.bind_submission(&cid(1), &branch, Some(&id), 1);
        // A retry may serialize different transient context but join the same queue ID.
        let mut third = map.arm_submission(cid(2), branch.clone(), Some(id.clone()));
        map.bind_submission(&cid(2), &branch, Some(&id), 1);
        let abandoned = map.arm_submission(cid(3), branch.clone(), Some(id.clone()));
        map.bind_submission(&cid(3), &branch, Some(&id), 1);
        drop(abandoned);
        assert_eq!(map.len(), 3);
        map.resolve_applied(1, receipt_at(11));
        assert_t(&mut first, 11).await;
        assert_t(&mut second, 11).await;
        assert_t(&mut third, 11).await;
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn identical_unkeyed_requests_are_not_coalesced_and_branches_are_isolated() {
        let map = Arc::new(WaiterMap::new());
        let branch = key("db", "main");
        let other = key("db", "other");
        let mut a = map.arm_submission(cid(1), branch.clone(), None);
        let mut b = map.arm_submission(cid(1), branch.clone(), None);
        let mut c = map.arm_submission(cid(1), other.clone(), None);
        map.bind_submission(&cid(1), &other, None, 3);
        map.bind_submission(&cid(1), &branch, None, 1);
        map.bind_submission(&cid(1), &branch, None, 2);
        map.resolve_applied(1, receipt_at(11));
        map.resolve_applied(2, receipt_at(22));
        map.resolve_applied(3, receipt_at(33));
        assert_t(&mut a, 11).await;
        assert_t(&mut b, 22).await;
        assert_t(&mut c, 33).await;
        assert!(map.is_empty());
    }
}
