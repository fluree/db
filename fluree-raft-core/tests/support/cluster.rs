//! Standing up a real multi-node group, generic over the application.
//!
//! Shared so a second app does not fork the bootstrap dance, and so
//! anything learned about forming a cluster (single-voter bootstrap,
//! then grow) is learned once.

use fluree_raft_core::admin::NodeAddrs;
use fluree_raft_core::group::GroupId;
use fluree_raft_core::node::{ClusterNode, NodeId};
use fluree_raft_core::runtime::{RaftGroup, RaftGroupConfig};
use fluree_raft_core::state_machine::{AppStateMachine, NoObserver};
use fluree_raft_core::storage::fs::FsRaftStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub struct Node<A: AppStateMachine> {
    pub id: NodeId,
    pub group: Arc<RaftGroup<A>>,
    pub addr: ClusterNode,
    /// Held so the storage directory outlives the node.
    _dir: tempfile::TempDir,
}

impl<A: AppStateMachine> Node<A> {
    pub fn addrs(&self) -> NodeAddrs {
        NodeAddrs {
            raft_addr: self.addr.raft_addr.clone(),
            client_addr: self.addr.client_addr.clone(),
        }
    }
}

/// Bring up one node: storage in a temp dir, a group, and an axum
/// server exposing its two routers under the group prefix.
pub async fn start_node<A: AppStateMachine>(id: NodeId, group_id: &GroupId) -> Node<A> {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = RaftGroupConfig::new(group_id.clone(), id, dir.path());
    let storage = Arc::new(
        FsRaftStorage::open(config.group_storage_root())
            .await
            .expect("storage opens"),
    );
    let group = Arc::new(
        RaftGroup::<A>::bootstrap(config, storage, NoObserver::default())
            .await
            .expect("group bootstraps"),
    );

    // Group-prefixed mounts — the multi-group shape. A single-group
    // deployment nests the same routers at bare /raft and /cluster,
    // which is how an existing group keeps the addresses already
    // recorded in its replicated membership.
    let app = axum::Router::new()
        .nest(&format!("/raft/{group_id}"), group.raft_router())
        .nest(&format!("/cluster/{group_id}"), group.admin_router());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Node {
        id,
        group,
        addr: ClusterNode::new(
            format!("http://127.0.0.1:{port}/raft/{group_id}"),
            format!("http://127.0.0.1:{port}"),
        ),
        _dir: dir,
    }
}

/// Poll an async predicate until it holds, or panic after ~10s.
pub async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..1000 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Single-voter bootstrap on the first node, then grow to the full set.
pub async fn form_cluster<A: AppStateMachine>(nodes: &[Node<A>]) {
    nodes[0]
        .group
        .admin
        .initialize(BTreeMap::from([(nodes[0].id, nodes[0].addrs())]))
        .await
        .expect("initialize");
    eventually("the first node to elect itself", || async {
        nodes[0].group.is_leader()
    })
    .await;

    for n in &nodes[1..] {
        nodes[0]
            .group
            .admin
            .add_learner(n.id, n.addrs(), true)
            .await
            .unwrap_or_else(|e| panic!("add learner {}: {e}", n.id));
    }
    nodes[0]
        .group
        .admin
        .change_membership(nodes.iter().map(|n| n.id).collect(), false)
        .await
        .expect("change membership");
}

/// The node currently leading, waiting for one if the election is
/// still in flight.
pub async fn leader<A: AppStateMachine>(nodes: &[Node<A>]) -> &Node<A> {
    eventually("a leader to emerge", || async {
        nodes.iter().any(|n| n.group.is_leader())
    })
    .await;
    nodes
        .iter()
        .find(|n| n.group.is_leader())
        .expect("a leader exists")
}
