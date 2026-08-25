//! The nameservice's state machine, held to the adapter contract.
//!
//! The nameservice reduces through `fluree_raft_core`'s generic
//! `StateMachineAdapter`, so most of what openraft depends on — persist
//! before swap, restore before replay, one response per entry — is
//! generic code the counter group already exercises. What is *not*
//! generic is this application's own contribution: the reduction in
//! `NameServiceApp`, its membership mirroring, and above all its
//! snapshot codec, which is bare postcard rather than the versioned
//! envelope the counter uses.
//!
//! `fluree_raft_core::testing::run_all` is the shared fixture that
//! pins those. Running the bespoke adapter through it is what makes
//! "the two adapters agree" a checked claim rather than an assertion
//! about code that was read once.
//!
//! Nameservice-specific behavior — waiter resolution, receipt stashing,
//! ledger-cache watermarks — is not in scope here; it stays in the
//! adapter's own unit tests.

#![cfg(all(feature = "raft", feature = "testing"))]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fluree_db_consensus::raft::state_machine::{Command, NameServiceState, NewLedger};
use fluree_db_consensus::raft::state_machine_adapter::{NameServiceObserver, StateMachineAdapter};
use fluree_db_consensus::raft::TypeConfig;
use fluree_raft_core::storage::memory::{MemoryRaftLogStore, MemoryRaftSnapshotStore};
use fluree_raft_core::storage::{
    RaftSnapshotStore, RaftStorage, SnapshotId, SnapshotMeta, StorageError,
};
use fluree_raft_core::testing::{run_all, ConformanceHarness};

/// Memory storage whose snapshot writes can be made to fail.
///
/// Required by the persist-before-swap check: a swap-first install is
/// indistinguishable from a persist-first one on the success path, so
/// the only way to tell them apart is to fail the persist while the
/// snapshot itself stays perfectly valid.
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

struct NameServiceHarness;

impl ConformanceHarness for NameServiceHarness {
    type Config = TypeConfig;
    type Adapter = StateMachineAdapter<FaultyStorage>;
    type Storage = FaultyStorage;

    fn storage(&self) -> Arc<Self::Storage> {
        Arc::new(FaultyStorage::new(false))
    }

    fn failing_snapshot_storage(&self) -> Option<Arc<Self::Storage>> {
        Some(Arc::new(FaultyStorage::new(true)))
    }

    async fn open(&self, storage: Arc<Self::Storage>) -> Self::Adapter {
        StateMachineAdapter::open(storage, NameServiceObserver::new())
            .await
            .expect("adapter opens")
    }

    /// `CreateLedger` is the smallest command with a durable, ordered,
    /// observable effect: it adds one entry to `ledgers` plus the
    /// genesis ref, and it is not idempotency-cached on a fresh state,
    /// so distinct `n` yield distinct states.
    fn command(&self, n: u64) -> Command {
        Command::CreateLedger(NewLedger {
            ledger_id: format!("conformance/{n}"),
            branch: "main".into(),
            created_at_millis: 1_000 + n,
        })
    }

    async fn probe(&self, adapter: &Self::Adapter) -> u64 {
        probe_state(&*adapter.shared_state().read().await)
    }

    fn probe_snapshot(&self, bytes: &[u8]) -> u64 {
        probe_state(&NameServiceState::from_snapshot(bytes).expect("snapshot decodes"))
    }
}

/// Order-insensitive digest of the parts `CreateLedger` touches.
///
/// `NameServiceState` is `HashMap`-backed, so anything derived from
/// iteration order would compare unequal to itself. Ledger names and
/// per-ref `t` go through sorted collections first.
fn probe_state(state: &NameServiceState) -> u64 {
    let ledgers: BTreeSet<&str> = state.ledgers.keys().map(String::as_str).collect();
    let refs: BTreeMap<String, i64> = state
        .refs
        .iter()
        .map(|(key, entry)| (key.ledger_id(), entry.t))
        .collect();

    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut mix = |bytes: &[u8]| {
        for b in bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    };
    for name in ledgers {
        mix(name.as_bytes());
    }
    for (id, t) in refs {
        mix(id.as_bytes());
        mix(&t.to_le_bytes());
    }
    hash
}

#[tokio::test]
async fn nameservice_adapter_passes_adapter_conformance() {
    run_all(&NameServiceHarness).await;
}

/// The fixture's precondition: "two states that probe equal must be
/// interchangeable." A degenerate probe would silently weaken every
/// check above rather than fail one, so pin that distinct commands
/// reach distinct probes — through live state and through a snapshot.
#[tokio::test]
async fn distinct_commands_reach_distinct_probes() {
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    let h = NameServiceHarness;
    let mut sm = h.open(h.storage()).await;
    let mut seen = BTreeSet::new();

    for index in 1..=3u64 {
        sm.apply(vec![Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(1, 0),
                index,
            },
            payload: EntryPayload::Normal(h.command(index)),
        }])
        .await
        .expect("apply");

        let live = h.probe(&sm).await;
        let snap = sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build snapshot");
        assert_eq!(
            h.probe_snapshot(snap.snapshot.get_ref()),
            live,
            "a snapshot of the live state must probe the same as the live state",
        );
        assert!(
            seen.insert(live),
            "command {index} left the state probing the same as an earlier one",
        );
    }
}
