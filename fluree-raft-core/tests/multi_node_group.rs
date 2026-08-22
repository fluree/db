//! A real three-node cluster of the toy counter group, over HTTP.
//!
//! This is the acceptance bar for the generic stack: everything here is
//! `fluree-raft-core` only — no nameservice, no `fluree-db-*` crate — so
//! it demonstrates that a second consumer can stand up a group without
//! forking anything.
//!
//! What it drives: filesystem storage per node, the generic state
//! machine, the HTTP transport and admin routers nested under a group-id
//! prefix, single-voter bootstrap growing to three by add-learner plus
//! change-membership, replication, the live `LeaderView` impl the
//! forwarder depends on, and the leader-task lifecycle.

#![cfg(feature = "testing")]

mod support;

use fluree_raft_core::admin::NodeAddrs;
use fluree_raft_core::forward::LeaderView;
use fluree_raft_core::group::GroupId;
use fluree_raft_core::node::{ClusterNode, NodeId};
use fluree_raft_core::runtime::{
    run_periodic, spawn_leader_watcher, RaftGroup, RaftGroupConfig, DEFAULT_LEADER_TASK_GRACE,
};
use fluree_raft_core::state_machine::NoObserver;
use fluree_raft_core::storage::fs::FsRaftStorage;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::{Counter, CounterCommand};
use tokio_util::sync::CancellationToken;

type Group = RaftGroup<Counter>;

struct Node {
    id: NodeId,
    group: Arc<Group>,
    addr: ClusterNode,
    /// Held so the storage directory outlives the node.
    _dir: tempfile::TempDir,
}

impl Node {
    fn addrs(&self) -> NodeAddrs {
        NodeAddrs {
            raft_addr: self.addr.raft_addr.clone(),
            client_addr: self.addr.client_addr.clone(),
        }
    }

    async fn value(&self) -> i64 {
        self.group.state.read().await.value
    }
}

/// Bring up one node: storage in a temp dir, a group, and an axum
/// server exposing its two routers under the group prefix.
async fn start_node(id: NodeId, group_id: &GroupId) -> Node {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = RaftGroupConfig::new(group_id.clone(), id, dir.path());
    let storage = Arc::new(
        FsRaftStorage::open(config.group_storage_root())
            .await
            .expect("storage opens"),
    );
    let group = Arc::new(
        Group::bootstrap(config, storage, NoObserver::default())
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
async fn eventually<F, Fut>(what: &str, mut check: F)
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

/// Single-voter bootstrap on node 1, then grow to the full set.
async fn form_cluster(nodes: &[Node]) {
    nodes[0]
        .group
        .admin
        .initialize(BTreeMap::from([(nodes[0].id, nodes[0].addrs())]))
        .await
        .expect("initialize");
    eventually("node 1 to elect itself", || async {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_counter_cluster_replicates_and_tracks_membership() {
    let group_id = GroupId::new("counter").expect("valid group id");
    let nodes = vec![
        start_node(1, &group_id).await,
        start_node(2, &group_id).await,
        start_node(3, &group_id).await,
    ];
    form_cluster(&nodes).await;

    for n in [5i64, 7, -2] {
        nodes[0]
            .group
            .raft
            .client_write(CounterCommand::Add(n))
            .await
            .expect("client_write");
    }

    for node in &nodes {
        let id = node.id;
        eventually(&format!("node {id} to converge on 10"), || async {
            node.value().await == 10
        })
        .await;
    }

    // The membership-derived field. No `Command` carries this — it can
    // only arrive through `apply_membership`, so a node holding the
    // right voter set proves that path ran on every replica.
    let expected: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
    for node in &nodes {
        assert_eq!(
            node.group.state.read().await.voters,
            expected,
            "node {} must mirror the voter set into app state",
            node.id,
        );
    }
}

/// The forwarder's decision logic is unit-tested against a stub; what a
/// stub cannot cover is whether the real `Raft<C>` reports leadership
/// and membership addresses the way the forwarder expects.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_raft_satisfies_the_leader_view_the_forwarder_needs() {
    let group_id = GroupId::new("view").expect("valid group id");
    let nodes = vec![
        start_node(1, &group_id).await,
        start_node(2, &group_id).await,
        start_node(3, &group_id).await,
    ];
    form_cluster(&nodes).await;

    let follower = nodes
        .iter()
        .find(|n| !n.group.is_leader())
        .expect("a follower exists");

    // Every node agrees who leads, including from a follower's view —
    // that is the answer the forwarder routes on.
    eventually("the follower to see a leader", || async {
        LeaderView::current_leader(&*follower.group.raft)
            .await
            .is_some()
    })
    .await;
    let leader_id = LeaderView::current_leader(&*follower.group.raft)
        .await
        .expect("leader known");
    assert!(
        nodes
            .iter()
            .any(|n| n.id == leader_id && n.group.is_leader()),
        "the follower's leader id must name the node that thinks it leads",
    );

    // And the follower can resolve that leader's *client* address from
    // replicated membership alone — the reason ClusterNode carries an
    // address pair rather than just an RPC URL.
    let seen = LeaderView::membership_nodes(&*follower.group.raft);
    assert_eq!(seen.len(), 3, "membership must list every node");
    let leader_client_addr = seen
        .iter()
        .find(|(id, _)| *id == leader_id)
        .map(|(_, node)| node.client_addr.clone())
        .expect("leader has a membership entry");
    let expected = nodes
        .iter()
        .find(|n| n.id == leader_id)
        .expect("leader node")
        .addr
        .client_addr
        .clone();
    assert_eq!(
        leader_client_addr, expected,
        "the client address a follower resolves must be the leader's own",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_tasks_start_on_election_and_stop_on_shutdown() {
    let group_id = GroupId::new("ticker").expect("valid group id");
    let node = start_node(1, &group_id).await;
    node.group
        .admin
        .initialize(BTreeMap::from([(node.id, node.addrs())]))
        .await
        .expect("initialize");

    let ticks = Arc::new(AtomicU64::new(0));
    let watcher = {
        let ticks = Arc::clone(&ticks);
        spawn_leader_watcher(
            Arc::clone(&node.group.raft),
            node.id,
            DEFAULT_LEADER_TASK_GRACE,
            move |cancel: CancellationToken| {
                let ticks = Arc::clone(&ticks);
                vec![tokio::spawn(async move {
                    run_periodic(Duration::from_millis(20), cancel, || {
                        let ticks = Arc::clone(&ticks);
                        async move {
                            ticks.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                    .await;
                })]
            },
        )
    };

    eventually("the leader task to tick", || async {
        ticks.load(Ordering::Relaxed) > 0
    })
    .await;

    // `shutdown` returns only once the task has actually stopped, so
    // the count cannot move afterwards. If it did, a leader flap would
    // be able to run two generations of "leader-only" tasks at once.
    watcher.shutdown().await;
    let settled = ticks.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        settled,
        "leader tasks must be stopped once shutdown returns",
    );
}

/// The bounded-abort half of shutdown: a leader task that ignores its
/// cancellation token must not be able to hang the watcher.
///
/// Without the abort, `shutdown` would await such a task forever and a
/// node could never relinquish leadership cleanly. Without the grace
/// period, a well-behaved task would be killed mid-cleanup. This pins
/// both halves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_ignores_cancellation_is_aborted_after_the_grace_period() {
    const GRACE: Duration = Duration::from_millis(200);

    let group_id = GroupId::new("straggler").expect("valid group id");
    let node = start_node(1, &group_id).await;
    node.group
        .admin
        .initialize(BTreeMap::from([(node.id, node.addrs())]))
        .await
        .expect("initialize");

    let ticks = Arc::new(AtomicU64::new(0));
    let watcher = {
        let ticks = Arc::clone(&ticks);
        spawn_leader_watcher(
            Arc::clone(&node.group.raft),
            node.id,
            GRACE,
            move |_cancel: CancellationToken| {
                // Deliberately never checks the token.
                let ticks = Arc::clone(&ticks);
                vec![tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        ticks.fetch_add(1, Ordering::Relaxed);
                    }
                })]
            },
        )
    };

    eventually("the straggler to start ticking", || async {
        ticks.load(Ordering::Relaxed) > 0
    })
    .await;

    let started = Instant::now();
    watcher.shutdown().await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= GRACE,
        "shutdown returned in {elapsed:?}, before the {GRACE:?} grace period — a \
         well-behaved task would be killed mid-cleanup",
    );
    assert!(
        elapsed < GRACE * 5,
        "shutdown took {elapsed:?}; a task ignoring cancellation must be aborted, not waited on",
    );

    let settled = ticks.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        settled,
        "an aborted straggler must actually be stopped once shutdown returns",
    );
}
