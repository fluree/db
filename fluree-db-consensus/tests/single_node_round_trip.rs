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
use fluree_db_consensus::raft::state_machine::{Command as SmCommand, CreateLedgerArgs, Response};
use fluree_db_consensus::raft::state_machine_adapter::StateMachineAdapter;
use fluree_db_consensus::raft::storage::memory::MemoryRaftStorage;
use fluree_db_consensus::raft::{ClusterNode, NodeId, TypeConfig};

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
