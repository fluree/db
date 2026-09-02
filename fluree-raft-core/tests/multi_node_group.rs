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

#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/counter.rs"]
mod counter;

use cluster::{eventually, form_cluster, leader, start_node, Node};

type CounterNode = Node<Counter>;
use counter::{Counter, CounterCommand};
use fluree_raft_core::forward::LeaderView;
use fluree_raft_core::group::GroupId;
use fluree_raft_core::node::NodeId;
use fluree_raft_core::runtime::{run_periodic, spawn_leader_watcher, DEFAULT_LEADER_TASK_GRACE};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_counter_cluster_replicates_and_tracks_membership() {
    let group_id = GroupId::new("counter").expect("valid group id");
    let nodes: Vec<CounterNode> = vec![
        start_node(1, &group_id, |_| {}).await,
        start_node(2, &group_id, |_| {}).await,
        start_node(3, &group_id, |_| {}).await,
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
            node.group.state.read().await.value == 10
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
    let nodes: Vec<CounterNode> = vec![
        start_node(1, &group_id, |_| {}).await,
        start_node(2, &group_id, |_| {}).await,
        start_node(3, &group_id, |_| {}).await,
    ];
    form_cluster(&nodes).await;

    // Wait for the election before classifying anyone: without this,
    // "not the leader" is also true of every node during a campaign.
    let elected = leader(&nodes).await.id;
    let follower = nodes
        .iter()
        .find(|n| n.id != elected)
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
    let node: CounterNode = start_node(1, &group_id, |_| {}).await;
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
    let node: CounterNode = start_node(1, &group_id, |_| {}).await;
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

/// Any node can accept a proposal: a FOLLOWER's `propose_via_leader`
/// relays the command to the leader's `/propose` endpoint, the apply
/// replicates, and the follower gets the application response back —
/// the contract that lets a load balancer spread writes across a
/// group's nodes instead of pinning them to whoever leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_propose_relays_to_the_leader_and_applies() {
    let group_id = GroupId::new("counter").expect("valid group id");
    let nodes: Vec<CounterNode> = vec![
        start_node(1, &group_id, |_| {}).await,
        start_node(2, &group_id, |_| {}).await,
        start_node(3, &group_id, |_| {}).await,
    ];
    form_cluster(&nodes).await;

    let leader_id = leader(&nodes).await.id;
    let follower = nodes
        .iter()
        .find(|n| n.id != leader_id)
        .expect("two followers exist");

    // Direct client_write on the follower refuses — the baseline the
    // relay exists to fix.
    let direct = follower
        .group
        .raft
        .client_write(CounterCommand::Add(1))
        .await;
    assert!(
        direct.is_err(),
        "a follower's direct client_write must refuse"
    );

    // The relayed propose lands.
    fluree_raft_core::forward::propose_via_leader(&follower.group.raft, CounterCommand::Add(41))
        .await
        .expect("relayed propose applies");

    for node in &nodes {
        let id = node.id;
        eventually(&format!("node {id} to converge on 41"), || async {
            node.group.state.read().await.value == 41
        })
        .await;
    }
}
