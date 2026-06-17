//! openraft state-machine adapter.
//!
//! Holds the [`NameServiceState`] plus the bookkeeping openraft
//! requires (last-applied log id, current membership) and routes
//! `apply` through our pure [`state_machine::apply`]. Snapshot
//! persistence happens via the [`RaftStorage::snapshots`] handle.
//!
//! The post-apply release processing (driving content-store deletes
//! from `Response::Applied.release`) is a separate concern that lives
//! at the wrapper above this adapter — see phase 4 of the design.
//! For now apply just returns the `Response` as-is.

use crate::raft::log_adapter::{from_openraft_log_id, to_openraft_log_id};
use crate::raft::state_machine::{self, Command, NameServiceState, RefKey, Response};
use crate::raft::storage::{
    RaftSnapshotStore, RaftStorage, SnapshotId as OurSnapshotId, SnapshotMeta as OurSnapshotMeta,
};
use crate::raft::staged_receipt::{AppliedReceipt, StagedReceiptMap};
use crate::raft::waiter::{AbortReason, WaiterMap};
use crate::raft::{ClusterNode, NodeId, TypeConfig};
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::ContentId;
use fluree_db_nameservice::{LedgerEventBus, NameServiceEvent};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handle to the replicated nameservice state used by the apply
/// path. Cloning is cheap (`Arc` clone). Sharing this with a
/// [`RaftNameService`](crate::raft::nameservice::RaftNameService)
/// lets read-only consumers see committed state without going
/// through openraft's RPC surface.
pub type SharedState = Arc<RwLock<NameServiceState>>;

fn io_err<S: ToString>(
    verb: ErrorVerb,
    subject: ErrorSubject<NodeId>,
    source: S,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::error(source.to_string())),
    }
}

fn read_state_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Read, ErrorSubject::StateMachine, source)
}

fn write_state_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Write, ErrorSubject::StateMachine, source)
}

fn snapshot_err<S: ToString>(verb: ErrorVerb, source: S) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Snapshot(None),
            verb,
            AnyError::error(source.to_string()),
        ),
    }
}

/// openraft state-machine adapter wrapping an `Arc<S: RaftStorage>`.
///
/// Holds the in-memory [`NameServiceState`] (shared via [`SharedState`]
/// so a [`RaftNameService`](crate::raft::nameservice::RaftNameService)
/// can read the same committed state) plus the bookkeeping openraft
/// needs (last-applied log id, current membership). Snapshot
/// reads/writes go through `S::SnapshotStore`.
pub struct StateMachineAdapter<S>
where
    S: RaftStorage,
{
    state: SharedState,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    storage: Arc<S>,
    /// Monotonic counter for snapshot ids — combined with the
    /// last-applied index for uniqueness across rebuilds.
    snapshot_counter: AtomicU64,
    /// When set, the adapter emits [`NameServiceEvent`]s on this bus
    /// after each successful apply.
    event_bus: Option<Arc<LedgerEventBus>>,
    /// When set, the adapter resolves queue waiters parked on this
    /// node after each successful queue-related apply. Per-process
    /// scope — leader transitions strand waiters from the prior
    /// leader, which the proposer recovers from with timeout +
    /// idempotency-keyed retry.
    waiter_map: Option<Arc<WaiterMap>>,
    /// Companion to [`waiter_map`](Self::waiter_map): the worker
    /// stashes per-op staging detail here before proposing
    /// `ApplyHead`; the adapter takes it during waiter resolution
    /// and sends it through as part of [`AppliedReceipt`]. Absent
    /// entry → fall back to [`AppliedReceipt::Minimal`] (the
    /// stranded-on-former-leader path).
    staged_receipts: Option<Arc<StagedReceiptMap>>,
}

impl<S> StateMachineAdapter<S>
where
    S: RaftStorage,
{
    /// Construct an adapter with a freshly-allocated [`SharedState`].
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_state(storage, Arc::new(RwLock::new(NameServiceState::default())))
    }

    /// Construct an adapter sharing the provided state handle. Use
    /// this when the same state must be visible to a `RaftNameService`
    /// constructed alongside the adapter.
    pub fn with_state(storage: Arc<S>, state: SharedState) -> Self {
        Self {
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            storage,
            snapshot_counter: AtomicU64::new(0),
            event_bus: None,
            waiter_map: None,
            staged_receipts: None,
        }
    }

    /// Set the [`LedgerEventBus`] this adapter emits commit/index
    /// events on.
    pub fn with_event_bus(mut self, event_bus: Arc<LedgerEventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Set the [`WaiterMap`] this adapter resolves after each
    /// queue-related apply. Pair it with the same handle the
    /// [`QueuedTransactor`](super::queued_transactor::QueuedTransactor)
    /// registers waiters on.
    pub fn with_waiter_map(mut self, waiter_map: Arc<WaiterMap>) -> Self {
        self.waiter_map = Some(waiter_map);
        self
    }

    /// Set the [`StagedReceiptMap`] the adapter reads from when
    /// constructing an [`AppliedReceipt`] for a resolved waiter.
    /// Pair it with the same handle the
    /// [`CommitWorker`](super::commit_worker::CommitWorker) stashes
    /// per-op staging detail into.
    pub fn with_staged_receipts(mut self, staged_receipts: Arc<StagedReceiptMap>) -> Self {
        self.staged_receipts = Some(staged_receipts);
        self
    }

    /// Borrow the shared state handle. Cheap clone (`Arc`).
    pub fn shared_state(&self) -> SharedState {
        Arc::clone(&self.state)
    }
}

/// What the waiter-map should do for a single `(Command, Response)`
/// pair. Computed under the apply lock, executed after it drops so
/// subscribers (the parked waiters' senders' receivers) can't
/// reenter apply.
enum WaiterResolution {
    /// `ApplyHead` advanced the head — wake the parked transactor
    /// with the new head identity.
    Applied {
        queue_id: u64,
        commit_id: ContentId,
        commit_t: i64,
    },
    /// `PoisonQueueEntry` recorded a terminal failure — wake the
    /// transactor with the abort reason.
    Aborted { queue_id: u64, reason: AbortReason },
    /// A head-mutating admin command cleared the per-branch queue —
    /// wake every parked transactor on that branch with the
    /// matching abort reason.
    AbortBranch { ref_key: RefKey, reason: AbortReason },
}

/// Translate an apply-path `(Command, Response)` pair into the
/// matching waiter resolution, if any. Returns `None` for pairs that
/// don't terminate a queue entry (every non-queue command, plus
/// `QueueDesync` — the waiter for that queue_id has already been
/// resolved by whichever earlier event popped it).
fn waiter_resolution_for(cmd: &Command, response: &Response) -> Option<WaiterResolution> {
    match (cmd, response) {
        (
            Command::ApplyHead(args),
            Response::HeadApplied {
                commit_id, commit_t, ..
            },
        ) => Some(WaiterResolution::Applied {
            queue_id: args.queue_id,
            commit_id: commit_id.clone(),
            commit_t: *commit_t,
        }),
        (Command::PoisonQueueEntry(_), Response::Poisoned { queue_id, reason, .. }) => {
            Some(WaiterResolution::Aborted {
                queue_id: *queue_id,
                reason: AbortReason::Poisoned(reason.clone()),
            })
        }
        (Command::DropBranch { ledger_id, branch }, Response::BranchDropped { .. }) => {
            Some(WaiterResolution::AbortBranch {
                ref_key: RefKey::new(ledger_id, branch),
                reason: AbortReason::BranchDropped,
            })
        }
        (Command::PurgeLedger { ledger_id, branch }, Response::Purged { .. }) => {
            Some(WaiterResolution::AbortBranch {
                ref_key: RefKey::new(ledger_id, branch),
                reason: AbortReason::BranchPurged,
            })
        }
        (
            Command::ResetHead {
                ledger_id, branch, ..
            },
            Response::HeadReset { .. },
        ) => Some(WaiterResolution::AbortBranch {
            ref_key: RefKey::new(ledger_id, branch),
            reason: AbortReason::BranchHeadReset,
        }),
        _ => None,
    }
}

/// Translate an apply-path `(Command, Response)` pair into the
/// matching [`NameServiceEvent`]. Returns `None` for pairs that
/// don't advance head state — desyncs, no-ops, idempotency hits.
fn event_for(cmd: &Command, response: &Response) -> Option<NameServiceEvent> {
    match (cmd, response) {
        (
            Command::ApplyHead(args),
            Response::HeadApplied {
                commit_id,
                commit_t,
                ..
            },
        ) => Some(NameServiceEvent::LedgerCommitPublished {
            ledger_id: format_ledger_id(&args.ledger_id, &args.branch),
            commit_id: commit_id.clone(),
            commit_t: *commit_t,
        }),
        (Command::AdvanceIndexHead(args), Response::IndexAdvanced { index_t, index_head }) => {
            Some(NameServiceEvent::LedgerIndexPublished {
                ledger_id: format_ledger_id(&args.ledger_id, &args.branch),
                index_id: index_head.clone(),
                index_t: *index_t,
            })
        }
        (Command::RetractLedger { .. }, Response::Retracted { ledger_id })
        | (Command::PurgeLedger { .. }, Response::Purged { ledger_id })
        | (Command::DropBranch { .. }, Response::BranchDropped { ledger_id, .. }) => {
            Some(NameServiceEvent::LedgerRetracted {
                ledger_id: ledger_id.clone(),
            })
        }
        (
            Command::CreateBranch(_),
            Response::BranchCreated {
                ledger_id,
                head,
                t,
            },
        ) => Some(NameServiceEvent::LedgerCommitPublished {
            ledger_id: ledger_id.clone(),
            commit_id: head.clone(),
            commit_t: *t,
        }),
        _ => None,
    }
}

impl<S> RaftStateMachine<TypeConfig> for StateMachineAdapter<S>
where
    S: RaftStorage,
{
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, ClusterNode>), StorageError<NodeId>>
    {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut events = Vec::new();
        let mut resolutions = Vec::new();
        {
            let mut state = self.state.write().await;
            for entry in entries {
                let log_id = entry.log_id;
                self.last_applied = Some(log_id);
                match entry.payload {
                    EntryPayload::Blank => responses.push(Response::NoOp),
                    EntryPayload::Normal(cmd) => {
                        let response = state_machine::apply(&mut state, cmd.clone(), log_id.index);
                        if let Some(event) = event_for(&cmd, &response) {
                            events.push(event);
                        }
                        if let Some(resolution) = waiter_resolution_for(&cmd, &response) {
                            resolutions.push(resolution);
                        }
                        responses.push(response);
                    }
                    EntryPayload::Membership(m) => {
                        self.last_membership = StoredMembership::new(Some(log_id), m);
                        responses.push(Response::NoOp);
                    }
                }
            }
        }
        // Emit after the state write lock drops so subscribers
        // can't block apply progress.
        if let Some(bus) = self.event_bus.as_ref() {
            for event in events {
                bus.notify(event);
            }
        }
        if let Some(waiters) = self.waiter_map.as_ref() {
            for resolution in resolutions {
                match resolution {
                    WaiterResolution::Applied {
                        queue_id,
                        commit_id,
                        commit_t,
                    } => {
                        let receipt = self
                            .staged_receipts
                            .as_ref()
                            .and_then(|s| s.take(queue_id))
                            .unwrap_or(AppliedReceipt::Minimal {
                                commit_id,
                                commit_t,
                            });
                        waiters.resolve_applied(queue_id, receipt);
                    }
                    WaiterResolution::Aborted { queue_id, reason } => {
                        waiters.resolve_aborted(queue_id, reason)
                    }
                    WaiterResolution::AbortBranch { ref_key, reason } => {
                        waiters.abort_all_for_branch(&ref_key, reason)
                    }
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let counter = self.snapshot_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let state_clone = self.state.read().await.clone();
        SnapshotBuilder {
            state: state_clone,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            storage: Arc::clone(&self.storage),
            counter,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let new_state = NameServiceState::from_snapshot(&bytes).map_err(read_state_err)?;
        let membership_bytes =
            postcard::to_allocvec(&meta.last_membership).map_err(write_state_err)?;

        self.storage
            .snapshots()
            .write(
                &OurSnapshotMeta {
                    id: OurSnapshotId::new(&meta.snapshot_id),
                    last_applied: meta.last_log_id.map(from_openraft_log_id),
                    membership: membership_bytes,
                },
                bytes,
            )
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Write, e))?;

        *self.state.write().await = new_state;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let current = self
            .storage
            .snapshots()
            .current()
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Read, e))?;
        let Some((our_meta, data)) = current else {
            return Ok(None);
        };
        let last_membership: StoredMembership<NodeId, ClusterNode> =
            postcard::from_bytes(&our_meta.membership).map_err(read_state_err)?;
        let meta = SnapshotMeta {
            last_log_id: our_meta.last_applied.map(to_openraft_log_id),
            last_membership,
            snapshot_id: our_meta.id.as_str().to_string(),
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

/// Snapshot builder produced by
/// [`StateMachineAdapter::get_snapshot_builder`]. Carries a cloned
/// view of state-machine state so the snapshot reflects the moment
/// the builder was obtained, not "now."
pub struct SnapshotBuilder<S>
where
    S: RaftStorage,
{
    state: NameServiceState,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    storage: Arc<S>,
    counter: u64,
}

impl<S> RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder<S>
where
    S: RaftStorage,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let bytes = self.state.to_snapshot().map_err(write_state_err)?;
        let last_index = self.last_applied.map(|id| id.index).unwrap_or(0);
        let snapshot_id = format!("snap-{}-{}", last_index, self.counter);

        let membership_bytes =
            postcard::to_allocvec(&self.last_membership).map_err(write_state_err)?;
        self.storage
            .snapshots()
            .write(
                &OurSnapshotMeta {
                    id: OurSnapshotId::new(&snapshot_id),
                    last_applied: self.last_applied.map(from_openraft_log_id),
                    membership: membership_bytes,
                },
                bytes.clone(),
            )
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Write, e))?;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::CreateLedgerArgs;
    use crate::raft::storage::memory::MemoryRaftStorage;
    use crate::raft::Command as RaftCommand;
    use fluree_db_api::{ContentId, ContentKind};
    use openraft::{CommittedLeaderId, LogId};

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn create_ledger_entry(index: u64, ledger_id: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::CreateLedger(CreateLedgerArgs {
                ledger_id: ledger_id.into(),
                branch: "main".into(),
                created_at_millis: 1_000,
            })),
        }
    }


    #[tokio::test]
    async fn apply_routes_create_ledger_to_state_machine() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage);
        let responses = sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], Response::Created { .. }));
        assert!(sm.shared_state().read().await.ledgers.contains_key("test/db"));
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn blank_entry_is_noop_but_advances_last_applied() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage);
        let blank = Entry {
            log_id: log_id(1, 5),
            payload: EntryPayload::Blank,
        };
        let responses = sm.apply([blank]).await.unwrap();
        assert_eq!(responses, vec![Response::NoOp]);
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 5)));
    }

    /// Direct head seed used by event-bus tests that need a populated
    /// branch head as setup, but don't care which mechanism set it
    /// (and don't want a queue-path `LedgerCommitPublished` event to
    /// leak into the test's bus draining).
    async fn seed_branch_head<S: super::RaftStorage>(
        sm: &StateMachineAdapter<S>,
        ledger_id: &str,
        branch: &str,
        head: ContentId,
        t: i64,
    ) {
        let mut guard = sm.shared_state().write_owned().await;
        if let Some(ledger) = guard.ledgers.get_mut(ledger_id) {
            if !ledger.branches.iter().any(|b| b == branch) {
                ledger.branches.push(branch.to_string());
            }
        }
        let ref_key = crate::raft::state_machine::RefKey::new(ledger_id, branch);
        let (prior_index, prior_source, prior_branches) = guard
            .refs
            .get(&ref_key)
            .map(|r| (r.index.clone(), r.source_branch.clone(), r.branches))
            .unwrap_or_default();
        guard.refs.insert(
            ref_key,
            crate::raft::state_machine::RefEntry {
                head,
                t,
                last_advanced_at_millis: 2_000,
                last_advanced_index: 1,
                index: prior_index,
                source_branch: prior_source,
                branches: prior_branches,
            },
        );
    }

    #[tokio::test]
    async fn snapshot_build_persists_and_get_current_round_trips() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(Arc::clone(&storage));
        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id, Some(log_id(1, 1)));

        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.snapshot_id, snap.meta.snapshot_id);
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_fresh_retract() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("retracted event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:main");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_nothing_on_already_retracted() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            }),
        }])
        .await
        .unwrap();
        let _ = sub.receiver.try_recv().expect("first retract emits");

        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            }),
        }])
        .await
        .unwrap();
        assert!(
            sub.receiver.try_recv().is_err(),
            "idempotent retract should not emit"
        );
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_purge_of_known_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::PurgeLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("purge event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:main");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_nothing_on_purge_of_missing_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(RaftCommand::PurgeLedger {
                ledger_id: "ghost".into(),
                branch: "main".into(),
            }),
        }])
        .await
        .unwrap();
        assert!(
            sub.receiver.try_recv().is_err(),
            "purge of unknown branch should not emit"
        );
    }

    #[tokio::test]
    async fn apply_emits_commit_event_on_create_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        seed_branch_head(&sm, "test/db", "main", cid(7), 10).await;

        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::CreateBranch(
                crate::raft::state_machine::CreateBranchArgs {
                    ledger_id: "test/db".into(),
                    branch: "feature".into(),
                    source_branch: "main".into(),
                    at_commit: None,
                    applied_at_millis: 3_000,
                },
            )),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("create-branch event") {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_id,
                commit_t,
            } => {
                assert_eq!(ledger_id, "test/db:feature");
                assert_eq!(commit_id, cid(7));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected LedgerCommitPublished, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_drop_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(storage).with_event_bus(Arc::clone(&bus));
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        seed_branch_head(&sm, "test/db", "main", cid(7), 10).await;
        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::CreateBranch(
                crate::raft::state_machine::CreateBranchArgs {
                    ledger_id: "test/db".into(),
                    branch: "feature".into(),
                    source_branch: "main".into(),
                    at_commit: None,
                    applied_at_millis: 3_000,
                },
            )),
        }])
        .await
        .unwrap();
        // Drain the create-branch event.
        let _ = sub.receiver.try_recv().expect("create-branch event");

        sm.apply([Entry {
            log_id: log_id(1, 4),
            payload: EntryPayload::Normal(RaftCommand::DropBranch {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("drop-branch event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:feature");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn install_snapshot_replaces_state_and_persists() {
        let source_storage = Arc::new(MemoryRaftStorage::new());
        let mut source = StateMachineAdapter::new(Arc::clone(&source_storage));
        source
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        let target_storage = Arc::new(MemoryRaftStorage::new());
        let mut target = StateMachineAdapter::new(Arc::clone(&target_storage));
        target
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert!(target
            .shared_state()
            .read()
            .await
            .ledgers
            .contains_key("test/db"));
        let (applied, _) = target.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1)));
    }

    // ====================================================================
    // Waiter-map resolution
    // ====================================================================

    use crate::raft::state_machine::{
        ApplyHeadArgs, BodyKind, EnqueueCommandArgs, PoisonQueueEntryArgs, PoisonReason,
        ResetHeadSnapshot,
    };
    use crate::raft::waiter::{AbortReason, WaiterMap, WaiterOutcome};

    fn enqueue_entry(index: u64, ledger_id: &str, branch: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::EnqueueCommand(EnqueueCommandArgs {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                idempotency: None,
                request_cid: cid(0),
                body_kind: BodyKind::JsonLdInsert,
                applied_at_millis: 1_500,
            })),
        }
    }

    fn apply_head_entry(
        index: u64,
        ledger_id: &str,
        branch: &str,
        queue_id: u64,
        commit: ContentId,
        commit_t: i64,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::ApplyHead(ApplyHeadArgs {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                queue_id,
                commit_id: commit,
                commit_t,
                applied_at_millis: 2_000,
            })),
        }
    }

    fn poison_entry(
        index: u64,
        ledger_id: &str,
        branch: &str,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::PoisonQueueEntry(PoisonQueueEntryArgs {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                queue_id,
                reason,
                applied_at_millis: 2_000,
            })),
        }
    }

    fn drop_branch_entry(index: u64, ledger_id: &str, branch: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::DropBranch {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
            }),
        }
    }

    async fn adapter_with_waiters() -> (
        StateMachineAdapter<MemoryRaftStorage>,
        Arc<WaiterMap>,
    ) {
        let storage = Arc::new(MemoryRaftStorage::new());
        let waiter_map = Arc::new(WaiterMap::new());
        let adapter = StateMachineAdapter::new(storage).with_waiter_map(Arc::clone(&waiter_map));
        (adapter, waiter_map)
    }

    #[tokio::test]
    async fn apply_head_resolves_waiter_with_applied_outcome() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        adapter.apply([enqueue_entry(2, "test/db", "main")]).await.unwrap();

        let rx = waiters.register(0, RefKey::new("test/db", "main"));
        adapter
            .apply([apply_head_entry(3, "test/db", "main", 0, cid(42), 10)])
            .await
            .unwrap();

        // No StagedReceiptMap is configured on the adapter, so the
        // resolution falls back to Minimal — confirming the absent-
        // entry path delivers commit_id / commit_t without panicking.
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
    }

    #[tokio::test]
    async fn apply_head_reads_stashed_receipt_when_present() {
        use crate::raft::staged_receipt::{StagedReceiptMap, TransactApplied};
        let storage = Arc::new(MemoryRaftStorage::new());
        let waiter_map = Arc::new(WaiterMap::new());
        let staged = Arc::new(StagedReceiptMap::new());
        let mut adapter = StateMachineAdapter::new(storage)
            .with_waiter_map(Arc::clone(&waiter_map))
            .with_staged_receipts(Arc::clone(&staged));
        adapter.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        adapter.apply([enqueue_entry(2, "test/db", "main")]).await.unwrap();

        let rx = waiter_map.register(0, RefKey::new("test/db", "main"));
        staged.stash(
            0,
            AppliedReceipt::Transact(TransactApplied {
                commit_id: cid(42),
                commit_t: 10,
                tally: None,
            }),
        );
        adapter
            .apply([apply_head_entry(3, "test/db", "main", 0, cid(42), 10)])
            .await
            .unwrap();

        match rx.await.expect("receive") {
            WaiterOutcome::Applied(AppliedReceipt::Transact(r)) => {
                assert_eq!(r.commit_id, cid(42));
                assert_eq!(r.commit_t, 10);
            }
            other => panic!("expected Applied(Transact), got {other:?}"),
        }
        assert_eq!(staged.len(), 0, "adapter must take from the map");
    }

    #[tokio::test]
    async fn poison_resolves_waiter_with_aborted_poisoned() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        adapter.apply([enqueue_entry(2, "test/db", "main")]).await.unwrap();

        let rx = waiters.register(0, RefKey::new("test/db", "main"));
        adapter
            .apply([poison_entry(
                3,
                "test/db",
                "main",
                0,
                PoisonReason::BodyMalformed {
                    error: "bad turtle".into(),
                },
            )])
            .await
            .unwrap();

        match rx.await.expect("receive") {
            WaiterOutcome::Aborted(AbortReason::Poisoned(PoisonReason::BodyMalformed {
                error,
            })) => assert_eq!(error, "bad turtle"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drop_branch_resolves_every_pending_waiter_on_that_branch() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        adapter.apply([enqueue_entry(2, "test/db", "main")]).await.unwrap();
        adapter.apply([enqueue_entry(3, "test/db", "main")]).await.unwrap();

        let rx_a = waiters.register(0, RefKey::new("test/db", "main"));
        let rx_b = waiters.register(1, RefKey::new("test/db", "main"));

        adapter
            .apply([drop_branch_entry(4, "test/db", "main")])
            .await
            .unwrap();

        assert!(matches!(
            rx_a.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
        assert!(matches!(
            rx_b.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
    }

    #[tokio::test]
    async fn reset_head_resolves_waiter_with_branch_head_reset() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        adapter.apply([enqueue_entry(2, "test/db", "main")]).await.unwrap();

        let rx = waiters.register(0, RefKey::new("test/db", "main"));
        adapter
            .apply([Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Normal(RaftCommand::ResetHead {
                    ledger_id: "test/db".into(),
                    branch: "main".into(),
                    snapshot: ResetHeadSnapshot {
                        commit_head_id: None,
                        commit_t: 0,
                        index_head_id: None,
                        index_t: 0,
                    },
                }),
            }])
            .await
            .unwrap();

        assert!(matches!(
            rx.await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchHeadReset)
        ));
    }

    #[tokio::test]
    async fn apply_without_waiter_map_is_silent() {
        // No waiter_map configured — the adapter should still apply
        // and respond normally without trying to resolve anything.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut adapter = StateMachineAdapter::new(storage);
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        // No assertions beyond "didn't panic" — the absence of a
        // waiter handle should be benign.
    }
}
