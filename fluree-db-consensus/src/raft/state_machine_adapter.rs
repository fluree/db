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
use crate::raft::state_machine::{self, NameServiceState, Response};
use crate::raft::storage::{
    RaftSnapshotStore, RaftStorage, SnapshotId as OurSnapshotId, SnapshotMeta as OurSnapshotMeta,
};
use crate::raft::{ClusterNode, NodeId, TypeConfig};
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
        }
    }

    /// Borrow the shared state handle. Cheap clone (`Arc`).
    pub fn shared_state(&self) -> SharedState {
        Arc::clone(&self.state)
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
        let mut state = self.state.write().await;
        for entry in entries {
            let log_id = entry.log_id;
            self.last_applied = Some(log_id);
            match entry.payload {
                EntryPayload::Blank => responses.push(Response::NoOp),
                EntryPayload::Normal(cmd) => {
                    responses.push(state_machine::apply(&mut state, cmd, log_id.index));
                }
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(log_id), m);
                    responses.push(Response::NoOp);
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
    use crate::raft::state_machine::{AdvanceRefArgs, CreateLedgerArgs};
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
                initial_branch: "main".into(),
                initial_head: cid(0),
                initial_t: 0,
                governance: cid(0xAA),
                created_at_millis: 1_000,
            })),
        }
    }

    fn advance_entry(
        index: u64,
        ledger_id: &str,
        prev: Option<ContentId>,
        new: ContentId,
        t: i64,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::AdvanceRef(AdvanceRefArgs {
                ledger_id: ledger_id.into(),
                branch: "main".into(),
                expected_prev: prev,
                new_head: new,
                t,
                applied_at_millis: 2_000,
                idempotency: None,
                release: Vec::new(),
                tally: None,
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
    async fn apply_runs_advance_ref_after_create() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage);
        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        let responses = sm
            .apply([advance_entry(2, "test/db", Some(cid(0)), cid(1), 1)])
            .await
            .unwrap();
        assert!(matches!(
            responses[0],
            Response::Applied { accepted: 1, .. }
        ));
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
}
