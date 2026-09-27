//! The nameservice lifecycle conformance suite against the raft-replicated
//! nameservice: each case on a fresh single-node cluster.

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{Config, Raft, ServerState};

use fluree_db_consensus::raft::log_adapter::LogAdapter;
use fluree_db_consensus::raft::nameservice::RaftNameService;
use fluree_db_consensus::raft::state_machine_adapter::{NameServiceObserver, StateMachineAdapter};
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
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        panic!("single-node raft never replicates");
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        panic!("single-node raft never installs snapshots");
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        panic!("single-node raft never requests votes");
    }
}

async fn single_node_nameservice() -> RaftNameService {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage), NameServiceObserver::new());
    let state = sm.shared_state();
    let config = Config {
        cluster_name: "lifecycle-conformance".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let raft = Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        StubFactory,
        log,
        sm,
    )
    .await
    .unwrap();
    raft.initialize(BTreeMap::from([(1u64, ClusterNode::default())]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();
    RaftNameService::new(state, Arc::new(raft))
}

#[tokio::test]
async fn raft_lifecycle_conformance() {
    fluree_db_nameservice::conformance::run_all(single_node_nameservice).await;
}
