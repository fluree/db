//! End-to-end test: a single-node Raft<TypeConfig> built from our
//! adapters processes a Command::CreateLedger through openraft and
//! produces a Response::Created.
//!
//! Stub network — single-node mode never has peers, so the RPC
//! methods are wired to panic. If openraft ever calls one in this
//! configuration, that's a real bug to investigate, and a panic in
//! a test is louder than a silent unimplemented!().

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluree_db_api::{ContentId, ContentKind};
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{Config, Raft, ServerState};

use fluree_db_consensus::raft::log_adapter::LogAdapter;
use fluree_db_consensus::raft::nameservice::RaftNameService;
use fluree_db_consensus::raft::state_machine::{
    AdvanceRefArgs, Command as SmCommand, CreateLedgerArgs, RefKey, Response,
};
use fluree_db_consensus::raft::state_machine_adapter::StateMachineAdapter;
use fluree_db_consensus::raft::storage::memory::MemoryRaftStorage;
use fluree_db_consensus::raft::{ClusterNode, NodeId, TypeConfig};
use fluree_db_nameservice::{
    IndexPublisher, LedgerEventBus, LedgerLifecycle, NameServiceError, NameServiceEvent,
    NameServiceLookup, SubscriptionScope,
};

struct StubFactory;
struct StubNetwork;

impl RaftNetworkFactory<TypeConfig> for StubFactory {
    type Network = StubNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &ClusterNode) -> Self::Network {
        StubNetwork
    }
}

impl RaftNetwork<TypeConfig> for StubNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        panic!("single-node Raft should never invoke append_entries");
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        panic!("single-node Raft should never invoke install_snapshot");
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        panic!("single-node Raft should never invoke vote");
    }
}

fn cid(seed: u8) -> ContentId {
    ContentId::new(ContentKind::Commit, &[seed])
}

#[tokio::test]
async fn single_node_create_ledger_round_trip() {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage));

    // Tight timing so the test doesn't dawdle.
    let config = Config {
        cluster_name: "single-node-test".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let raft = Raft::new(1, config, StubFactory, log, sm).await.unwrap();

    // Bootstrap as a single-member cluster.
    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    // Wait for self-election. With one node and a configured timeout,
    // this should happen well within a second.
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    let cmd = SmCommand::CreateLedger(CreateLedgerArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        created_at_millis: 1_000,
    });
    let resp = raft.client_write(cmd).await.unwrap();
    match resp.data {
        Response::Created { ref ledger_id } => assert_eq!(ledger_id, "test/db:main"),
        other => panic!("expected Created, got {other:?}"),
    }

    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_raft_index_publisher_round_trip() {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage));
    let shared_state = sm.shared_state();

    let config = Config {
        cluster_name: "single-node-index-publisher".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let raft = Raft::new(1, config, StubFactory, log, sm).await.unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    // Bootstrap the ledger + a commit so the index publish has
    // something to attach to.
    raft.client_write(SmCommand::CreateLedger(CreateLedgerArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        created_at_millis: 1_000,
    }))
    .await
    .unwrap();

    raft.client_write(SmCommand::AdvanceRef(AdvanceRefArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        expected_prev: None,
        new_head: cid(7),
        t: 10,
        applied_at_millis: 2_000,
        idempotency: None,
        release: Vec::new(),
        tally: None,
    }))
    .await
    .unwrap();

    // Publish through the combined RaftNameService.
    let raft_arc = Arc::new(raft);
    let ns = RaftNameService::new(shared_state.clone(), Arc::clone(&raft_arc));
    ns.publish_index("test/db:main", 10, &cid(42))
        .await
        .expect("publish_index ok");

    // The state machine's RefEntry should now carry the index.
    {
        let state = shared_state.read().await;
        let entry = state
            .refs
            .get(&RefKey::new("test/db", "main"))
            .expect("ref entry");
        let index = entry.index.as_ref().expect("index populated");
        assert_eq!(index.head, cid(42));
        assert_eq!(index.t, 10);
    }

    // The same handle's `lookup` observes the new index head — the
    // combined type unifies reads and writes.
    let record = ns
        .lookup("test/db:main")
        .await
        .expect("lookup ok")
        .expect("record");
    assert_eq!(record.index_head_id, Some(cid(42)));
    assert_eq!(record.index_t, 10);

    // A second publish at the same t is treated as stale and
    // surfaces as Ok — the cluster's view is unchanged.
    ns.publish_index("test/db:main", 10, &cid(99))
        .await
        .expect("stale publish is ok");
    {
        let state = shared_state.read().await;
        let entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        let index = entry.index.as_ref().unwrap();
        // Original head preserved — second publish was stale.
        assert_eq!(index.head, cid(42));
        assert_eq!(index.t, 10);
    }

    raft_arc.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_apply_emits_commit_event_on_bus() {
    // Wires the state-machine adapter to a LedgerEventBus and
    // proves that going through the full openraft pipeline (propose
    // → quorum → apply) results in a `LedgerCommitPublished` event on
    // the bus — exactly the path the indexer worker subscribes to.

    let storage = Arc::new(MemoryRaftStorage::new());
    let event_bus = Arc::new(LedgerEventBus::new(16));
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm =
        StateMachineAdapter::new(Arc::clone(&storage)).with_event_bus(Arc::clone(&event_bus));

    let config = Config {
        cluster_name: "single-node-event-bus".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let raft = Raft::new(1, config, StubFactory, log, sm).await.unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    // Subscribe BEFORE the AdvanceRef proposal so the event lands in
    // the receiver's buffer when apply emits it.
    let mut sub = event_bus.subscribe(SubscriptionScope::All);

    raft.client_write(SmCommand::CreateLedger(CreateLedgerArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        created_at_millis: 1_000,
    }))
    .await
    .unwrap();
    // CreateLedger doesn't carry a published-commit semantic — the
    // bus stays quiet.
    assert!(
        sub.receiver.try_recv().is_err(),
        "CreateLedger should not emit a commit event"
    );

    raft.client_write(SmCommand::AdvanceRef(AdvanceRefArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        expected_prev: None,
        new_head: cid(7),
        t: 10,
        applied_at_millis: 2_000,
        idempotency: None,
        release: Vec::new(),
        tally: None,
    }))
    .await
    .unwrap();

    // The AdvanceRef Applied response should have triggered an
    // emission. Try-recv to keep the test deterministic — the event
    // is already on the broadcast buffer by the time client_write
    // returns (apply emits before returning the Response).
    match sub.receiver.try_recv().expect("commit event present") {
        NameServiceEvent::LedgerCommitPublished {
            ledger_id,
            commit_id,
            commit_t,
        } => {
            assert_eq!(ledger_id, "test/db:main");
            assert_eq!(commit_id, cid(7));
            assert_eq!(commit_t, 10);
        }
        other => panic!("expected LedgerCommitPublished, got {other:?}"),
    }

    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_ledger_lifecycle_round_trip() {
    // init → retract → purge → init (alias reusable) — driven
    // entirely through the LedgerLifecycle trait surface on
    // RaftNameService, so the test exercises the same path
    // production HTTP routes will.

    let storage = Arc::new(MemoryRaftStorage::new());
    let bus = Arc::new(LedgerEventBus::new(16));
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm =
        StateMachineAdapter::new(Arc::clone(&storage)).with_event_bus(Arc::clone(&bus));
    let shared_state = sm.shared_state();

    let config = Config {
        cluster_name: "single-node-lifecycle".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let raft = Arc::new(Raft::new(1, config, StubFactory, log, sm).await.unwrap());

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    let ns = RaftNameService::new(shared_state.clone(), Arc::clone(&raft));
    let mut sub = bus.subscribe(SubscriptionScope::All);

    // init registers the branch unborn.
    ns.init("test/db:main").await.expect("init ok");
    let record = ns.lookup("test/db:main").await.unwrap().expect("record");
    assert_eq!(record.ledger_id, "test/db:main");
    assert_eq!(record.commit_head_id, None);
    assert!(!record.retracted);

    // Duplicate init returns the typed LedgerAlreadyExists error.
    match ns.init("test/db:main").await {
        Err(NameServiceError::LedgerAlreadyExists(id)) => {
            assert_eq!(id, "test/db:main");
        }
        other => panic!("expected LedgerAlreadyExists, got {other:?}"),
    }

    // retract flips the record to retracted; lookup keeps returning
    // it (with the flag) until purge clears it.
    ns.retract("test/db:main").await.expect("retract ok");
    let record = ns.lookup("test/db:main").await.unwrap().expect("record");
    assert!(record.retracted);

    // The first event on the bus is the retract.
    match sub.receiver.try_recv().expect("event present") {
        NameServiceEvent::LedgerRetracted { ledger_id } => {
            assert_eq!(ledger_id, "test/db:main");
        }
        other => panic!("expected LedgerRetracted, got {other:?}"),
    }

    // Idempotent retract is Ok and emits nothing.
    ns.retract("test/db:main").await.expect("retract idempotent");
    assert!(sub.receiver.try_recv().is_err());

    // Init still refuses while the record is retracted.
    match ns.init("test/db:main").await {
        Err(NameServiceError::LedgerAlreadyExists(_)) => {}
        other => panic!("expected LedgerAlreadyExists, got {other:?}"),
    }

    // purge clears the alias. Emits LedgerRetracted again since the
    // branch transitioned from "present" to "absent".
    ns.purge("test/db:main").await.expect("purge ok");
    assert!(ns.lookup("test/db:main").await.unwrap().is_none());
    match sub.receiver.try_recv().expect("event present") {
        NameServiceEvent::LedgerRetracted { ledger_id } => {
            assert_eq!(ledger_id, "test/db:main");
        }
        other => panic!("expected LedgerRetracted, got {other:?}"),
    }

    // Idempotent purge of an already-purged branch is Ok and silent.
    ns.purge("test/db:main").await.expect("purge idempotent");
    assert!(sub.receiver.try_recv().is_err());

    // The alias is reusable now.
    ns.init("test/db:main").await.expect("init after purge");

    raft.shutdown().await.unwrap();
}
