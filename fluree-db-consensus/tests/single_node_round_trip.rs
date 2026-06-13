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

use fluree_db_consensus::raft::index_publisher::RaftIndexPublisher;
use fluree_db_consensus::raft::log_adapter::LogAdapter;
use fluree_db_consensus::raft::state_machine::{
    AdvanceRefArgs, Command as SmCommand, CreateLedgerArgs, RefKey, Response,
};
use fluree_db_consensus::raft::state_machine_adapter::StateMachineAdapter;
use fluree_db_consensus::raft::storage::memory::MemoryRaftStorage;
use fluree_db_consensus::raft::{ClusterNode, NodeId, TypeConfig};
use fluree_db_nameservice::IndexPublisher;

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
        initial_branch: "main".into(),
        initial_head: cid(0),
        initial_t: 0,
        governance: cid(0xAA),
        created_at_millis: 1_000,
    });
    let resp = raft.client_write(cmd).await.unwrap();
    match resp.data {
        Response::Created { ref ledger_id } => assert_eq!(ledger_id, "test/db"),
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
        initial_branch: "main".into(),
        initial_head: cid(0),
        initial_t: 0,
        governance: cid(0xAA),
        created_at_millis: 1_000,
    }))
    .await
    .unwrap();

    raft.client_write(SmCommand::AdvanceRef(AdvanceRefArgs {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        expected_prev: Some(cid(0)),
        new_head: cid(7),
        t: 10,
        applied_at_millis: 2_000,
        idempotency: None,
        release: Vec::new(),
        tally: None,
    }))
    .await
    .unwrap();

    // Publish through the IndexPublisher trait, end-to-end.
    let publisher = RaftIndexPublisher::new(Arc::new(raft));
    publisher
        .publish_index("test/db:main", 10, &cid(42))
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

    // A second publish at the same t is treated as stale and
    // surfaces as Ok — the cluster's view is unchanged.
    publisher
        .publish_index("test/db:main", 10, &cid(99))
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

    publisher.raft().shutdown().await.unwrap();
}
