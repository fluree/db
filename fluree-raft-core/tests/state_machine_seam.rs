//! End-to-end exercise of the [`AppStateMachine`] /
//! [`StateMachineObserver`] seam, using a counter as the application.
//!
//! The counter is deliberately trivial. What is being tested is the
//! seam: that a real implementation can be written against it, that the
//! adapter honours the contract the conformance fixture describes, and
//! that the two properties the design turns on actually hold —
//! effects are published *after* the state lock drops, and a live
//! snapshot install is distinguishable from a boot restore.

#![cfg(feature = "testing")]

#[path = "support/counter.rs"]
mod counter;

use counter::{
    Counter, CounterCommand, CounterConfig, CounterResponse, CounterState, CounterStateV1,
    SNAPSHOT_V2,
};
use fluree_raft_core::node::{ClusterNode, NodeId};
use fluree_raft_core::state_machine::{
    codec, AppStateMachine, MembershipView, ReadOnlyState, SnapshotCodecError, SnapshotLoad,
    StateMachineAdapter, StateMachineObserver,
};
use fluree_raft_core::storage::memory::{
    MemoryRaftLogStore, MemoryRaftSnapshotStore, MemoryRaftStorage,
};
use fluree_raft_core::storage::{
    RaftSnapshotStore, RaftStorage, SnapshotId, SnapshotMeta, StorageError,
};
use fluree_raft_core::testing::{run_all, ConformanceHarness};
use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

// ============================================================================
// The observer
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum Effect {
    Applied { value: i64, at_index: u64 },
    MembershipChanged { voters: usize },
    SnapshotLoaded(SnapshotLoad),
}

#[derive(Default)]
struct Recorder {
    /// Effects seen by `publish`, in order.
    published: Mutex<Vec<Effect>>,
    /// Whether every `publish` call was able to take a read lock on the
    /// state — i.e. the apply-path write lock had genuinely been
    /// released before publishing.
    lock_free_at_publish: Mutex<Vec<bool>>,
    /// Set after construction so `publish` can probe the same handle
    /// the adapter writes through.
    state: Mutex<Option<ReadOnlyState<Counter>>>,
}

impl Recorder {
    fn effects(&self) -> Vec<Effect> {
        self.published.lock().unwrap().clone()
    }
}

struct RecordingObserver(Arc<Recorder>);

impl StateMachineObserver<Counter> for RecordingObserver {
    type Effect = Effect;

    fn on_command(
        &self,
        state: &CounterState,
        command: &CounterCommand,
        response: &mut CounterResponse,
        log_index: u64,
        out: &mut Vec<Self::Effect>,
    ) {
        // The observer sees the command, the resulting state, and the
        // response together — the combination the nameservice needs to
        // decide which waiter to wake and which event to emit.
        assert!(
            matches!(response, CounterResponse::Value { value, .. } if *value == state.value),
            "observer must see the response that this command produced",
        );
        let _ = command;
        out.push(Effect::Applied {
            value: state.value,
            at_index: log_index,
        });
    }

    fn on_membership(
        &self,
        state: &CounterState,
        membership: &MembershipView,
        _log_index: u64,
        out: &mut Vec<Self::Effect>,
    ) {
        assert_eq!(
            state.voters, membership.voters,
            "on_membership must run after apply_membership",
        );
        out.push(Effect::MembershipChanged {
            voters: membership.voters.len(),
        });
    }

    fn on_snapshot_loaded(
        &self,
        _state: &CounterState,
        load: SnapshotLoad,
        out: &mut Vec<Self::Effect>,
    ) {
        out.push(Effect::SnapshotLoaded(load));
    }

    fn publish(&self, effects: Vec<Self::Effect>) {
        // The load-bearing property: by the time publish runs, the apply
        // path must have released the state write lock. If it had not,
        // this try_read fails — and a real observer that touched the
        // state here would deadlock or stall apply.
        if let Some(state) = self.0.state.lock().unwrap().as_ref() {
            self.0
                .lock_free_at_publish
                .lock()
                .unwrap()
                .push(state.try_read().is_ok());
        }
        self.0.published.lock().unwrap().extend(effects);
    }
}

// ============================================================================
// Helpers
// ============================================================================

type Adapter = StateMachineAdapter<Counter, RecordingObserver, MemoryRaftStorage>;

async fn adapter_with(recorder: Arc<Recorder>, storage: Arc<MemoryRaftStorage>) -> Adapter {
    let sm = StateMachineAdapter::open(storage, RecordingObserver(Arc::clone(&recorder)))
        .await
        .expect("adapter opens");
    *recorder.state.lock().unwrap() = Some(sm.shared_state());
    sm
}

fn log_id(term: u64, index: u64) -> LogId<NodeId> {
    LogId {
        leader_id: CommittedLeaderId::new(term, 0),
        index,
    }
}

fn add(index: u64, n: i64) -> Entry<CounterConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(CounterCommand::Add(n)),
    }
}

// ============================================================================
// Conformance
// ============================================================================

/// Memory storage whose snapshot writes can be made to fail.
///
/// Needed by the persist-before-swap check: a swap-first install is
/// indistinguishable from a persist-first one unless the persist can be
/// made to fail while the snapshot itself stays perfectly valid.
struct FaultySnapshotStore {
    inner: MemoryRaftSnapshotStore,
    fail_writes: bool,
}

#[async_trait::async_trait]
impl RaftSnapshotStore for FaultySnapshotStore {
    async fn write(&self, meta: &SnapshotMeta, data: Vec<u8>) -> Result<(), StorageError> {
        if self.fail_writes {
            return Err(StorageError::io("injected snapshot write failure"));
        }
        self.inner.write(meta, data).await
    }
    async fn read(&self, id: &SnapshotId) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.read(id).await
    }
    async fn current(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, StorageError> {
        self.inner.current().await
    }
}

struct FaultyStorage {
    log: MemoryRaftLogStore,
    snapshots: FaultySnapshotStore,
}

impl FaultyStorage {
    fn new(fail_writes: bool) -> Self {
        Self {
            log: MemoryRaftLogStore::new(),
            snapshots: FaultySnapshotStore {
                inner: MemoryRaftSnapshotStore::new(),
                fail_writes,
            },
        }
    }
}

impl RaftStorage for FaultyStorage {
    type LogStore = MemoryRaftLogStore;
    type SnapshotStore = FaultySnapshotStore;

    fn log(&self) -> &Self::LogStore {
        &self.log
    }
    fn snapshots(&self) -> &Self::SnapshotStore {
        &self.snapshots
    }
}

struct CounterHarness;

impl ConformanceHarness for CounterHarness {
    type Config = CounterConfig;
    type Adapter = StateMachineAdapter<Counter, RecordingObserver, FaultyStorage>;
    type Storage = FaultyStorage;

    fn storage(&self) -> Arc<Self::Storage> {
        Arc::new(FaultyStorage::new(false))
    }

    fn failing_snapshot_storage(&self) -> Option<Arc<Self::Storage>> {
        Some(Arc::new(FaultyStorage::new(true)))
    }

    async fn open(&self, storage: Arc<Self::Storage>) -> Self::Adapter {
        StateMachineAdapter::open(storage, RecordingObserver(Arc::new(Recorder::default())))
            .await
            .expect("adapter opens")
    }

    fn command(&self, n: u64) -> CounterCommand {
        CounterCommand::Add(n as i64)
    }

    async fn probe(&self, adapter: &Self::Adapter) -> u64 {
        probe_state(&*adapter.shared_state().read().await)
    }

    fn probe_snapshot(&self, bytes: &[u8]) -> u64 {
        probe_state(&Counter::decode_snapshot(bytes).expect("snapshot decodes"))
    }
}

/// Folds value and apply count, so a state differing in either probes
/// differently.
fn probe_state(state: &CounterState) -> u64 {
    (state.value as u64)
        .wrapping_mul(31)
        .wrapping_add(state.applies)
}

#[tokio::test]
async fn counter_passes_adapter_conformance() {
    run_all(&CounterHarness).await;
}

// ============================================================================
// The seam's own properties
// ============================================================================

#[tokio::test]
async fn effects_publish_after_the_state_lock_is_released() {
    let recorder = Arc::new(Recorder::default());
    let mut sm = adapter_with(Arc::clone(&recorder), Arc::new(MemoryRaftStorage::new())).await;

    sm.apply(vec![add(1, 5), add(2, 7)]).await.expect("apply");

    let probes = recorder.lock_free_at_publish.lock().unwrap().clone();
    assert!(!probes.is_empty(), "publish must have run");
    assert!(
        probes.iter().all(|ok| *ok),
        "publish ran while the state write lock was still held",
    );
}

#[tokio::test]
async fn effects_arrive_batched_in_log_order() {
    let recorder = Arc::new(Recorder::default());
    let mut sm = adapter_with(Arc::clone(&recorder), Arc::new(MemoryRaftStorage::new())).await;

    sm.apply(vec![add(1, 5), add(2, 7), add(3, -2)])
        .await
        .expect("apply");

    assert_eq!(
        recorder.effects(),
        vec![
            Effect::Applied {
                value: 5,
                at_index: 1
            },
            Effect::Applied {
                value: 12,
                at_index: 2
            },
            Effect::Applied {
                value: 10,
                at_index: 3
            },
        ],
        "effects must arrive in log order",
    );
}

#[tokio::test]
async fn membership_entries_reach_both_state_and_observer() {
    let recorder = Arc::new(Recorder::default());
    let mut sm = adapter_with(Arc::clone(&recorder), Arc::new(MemoryRaftStorage::new())).await;

    let nodes: std::collections::BTreeMap<NodeId, ClusterNode> = [1u64, 2, 3]
        .into_iter()
        .map(|id| {
            (
                id,
                ClusterNode::new(format!("http://n{id}/raft"), format!("http://n{id}")),
            )
        })
        .collect();
    sm.apply(vec![Entry {
        log_id: log_id(1, 1),
        payload: EntryPayload::Membership(Membership::new(
            vec![[1u64, 2, 3].into_iter().collect()],
            nodes,
        )),
    }])
    .await
    .expect("apply");

    assert_eq!(
        sm.shared_state().read().await.voters,
        [1u64, 2, 3].into_iter().collect::<BTreeSet<_>>(),
        "apply_membership must mirror the voter set into app state",
    );
    assert_eq!(
        recorder.effects(),
        vec![Effect::MembershipChanged { voters: 3 }],
    );
}

/// The distinction the nameservice needs: on a live install, in-flight
/// local work is unresolvable; on a boot restore there is none.
#[tokio::test]
async fn live_install_and_boot_restore_are_distinguishable() {
    let source_storage = Arc::new(MemoryRaftStorage::new());
    let (meta, bytes) = {
        let mut sm = adapter_with(Arc::new(Recorder::default()), Arc::clone(&source_storage)).await;
        sm.apply(vec![add(1, 42)]).await.expect("apply");
        let mut b = sm.get_snapshot_builder().await;
        let snap = openraft::storage::RaftSnapshotBuilder::build_snapshot(&mut b)
            .await
            .expect("build");
        (snap.meta.clone(), snap.snapshot.into_inner())
    };

    // Live install on a lagging node.
    let target_storage = Arc::new(MemoryRaftStorage::new());
    let recorder = Arc::new(Recorder::default());
    let mut target = adapter_with(Arc::clone(&recorder), Arc::clone(&target_storage)).await;
    target
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .expect("install");
    assert_eq!(
        recorder.effects(),
        vec![Effect::SnapshotLoaded(SnapshotLoad::LiveInstall)],
    );
    assert_eq!(target.shared_state().read().await.value, 42);

    // Restart over the same storage: same state, different reason.
    let restart_recorder = Arc::new(Recorder::default());
    let restarted = adapter_with(Arc::clone(&restart_recorder), target_storage).await;
    assert_eq!(
        restart_recorder.effects(),
        vec![Effect::SnapshotLoaded(SnapshotLoad::BootRestore)],
    );
    assert_eq!(restarted.shared_state().read().await.value, 42);
}

// ============================================================================
// Snapshot codec
// ============================================================================

#[tokio::test]
async fn snapshot_migrates_from_an_older_format_version() {
    let old = codec::encode(1, &CounterStateV1 { value: 17 }).expect("encode v1");
    let migrated = Counter::decode_snapshot(&old).expect("v1 snapshot must still be readable");
    assert_eq!(migrated.value, 17);

    let current = Counter::encode_snapshot(&CounterState {
        value: 17,
        applies: 3,
        voters: BTreeSet::new(),
    })
    .expect("encode v2");
    assert_eq!(codec::peek_version(&current).unwrap(), SNAPSHOT_V2);
    assert_eq!(Counter::decode_snapshot(&current).unwrap().applies, 3);
}

#[test]
fn unversioned_or_truncated_snapshots_are_rejected_distinctly() {
    // A bare postcard blob — what a pre-versioning build would have
    // written — must not be mistaken for a valid one.
    let bare = postcard::to_allocvec(&CounterState::default()).unwrap();
    assert!(matches!(
        codec::peek_version(&bare),
        Err(SnapshotCodecError::BadMagic | SnapshotCodecError::Truncated(..))
    ));

    assert!(matches!(
        codec::peek_version(b"FRC"),
        Err(SnapshotCodecError::Truncated(3, 6))
    ));

    let future = codec::encode(999, &CounterState::default()).unwrap();
    assert!(matches!(
        Counter::decode_snapshot(&future),
        Err(SnapshotCodecError::UnsupportedVersion { found: 999, .. })
    ));
}
