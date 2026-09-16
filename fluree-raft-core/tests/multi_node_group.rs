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
use fluree_raft_core::forward::{LeaderView, ProposeError};
use fluree_raft_core::group::GroupId;
use fluree_raft_core::node::NodeId;
use fluree_raft_core::runtime::{run_periodic, spawn_leader_watcher, DEFAULT_LEADER_TASK_GRACE};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
enum ProposeFault {
    LostHeaders,
    TruncatedBody,
    NotLeader,
    ServerError,
    Bare503,
    InvalidJson,
}

/// Forward to the real leader, then damage only the first proposal's
/// response. Other RPCs pass through, so elections and replication stay real.
async fn relay_fault_case(fault: ProposeFault) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let group_id = GroupId::new("relay-fault").unwrap();
    let mut nodes: Vec<CounterNode> = vec![
        start_node(1, &group_id, |_| {}).await,
        start_node(2, &group_id, |_| {}).await,
        start_node(3, &group_id, |_| {}).await,
    ];
    let upstream = nodes[0].addr.client_addr.clone();
    let follower_upstream = nodes[1].addr.client_addr.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    nodes[0].addr.raft_addr = format!("http://{}/raft/{group_id}", listener.local_addr().unwrap());
    let proposals = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&proposals);
    let proxy = tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let client = client.clone();
            let upstream = upstream.clone();
            let follower_upstream = follower_upstream.clone();
            let observed = Arc::clone(&observed);
            tokio::spawn(async move {
                let mut request = Vec::new();
                loop {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                    assert!(request.len() < 64 * 1024);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let headers = String::from_utf8(request).unwrap();
                let path = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                let first =
                    path.ends_with("/propose") && observed.fetch_add(1, Ordering::SeqCst) == 0;
                // Exercise the real peer rejection wire response by sending
                // the first proposal to a follower instead of the leader.
                let rejected = first && matches!(fault, ProposeFault::NotLeader);
                let upstream = if rejected {
                    follower_upstream
                } else {
                    upstream
                };
                let response = client
                    .post(format!("{upstream}{path}"))
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                let mut status = response.status();
                let mut bytes = response.bytes().await.unwrap();
                if rejected {
                    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                        serde_json::json!({"error": "not_leader"})
                    );
                } else if first {
                    assert!(status.is_success(), "leader must apply before fault");
                    if matches!(fault, ProposeFault::LostHeaders) {
                        return;
                    }
                    match fault {
                        ProposeFault::ServerError => {
                            status = reqwest::StatusCode::INTERNAL_SERVER_ERROR;
                            bytes = "propose response encode error".into();
                        }
                        ProposeFault::Bare503 => {
                            status = reqwest::StatusCode::SERVICE_UNAVAILABLE;
                            bytes = "upstream unavailable".into();
                        }
                        ProposeFault::InvalidJson => bytes = "{".into(),
                        _ => {}
                    }
                }
                let headers = format!(
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status,
                    bytes.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                if first && matches!(fault, ProposeFault::TruncatedBody) {
                    assert!(bytes.len() > 1);
                    socket.write_all(&bytes[..1]).await.unwrap();
                    // Give the relay time to receive headers before EOF.
                    tokio::task::yield_now().await;
                    return;
                }
                socket.write_all(&bytes).await.unwrap();
            });
        }
    });
    form_cluster(&nodes).await;
    eventually("follower to recognize leader 1", || async {
        LeaderView::current_leader(&*nodes[1].group.raft).await == Some(1)
    })
    .await;

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        fluree_raft_core::forward::propose_via_leader(&nodes[1].group.raft, CounterCommand::Add(1)),
    )
    .await
    .expect("relay must finish");
    let state = nodes[0].group.state.read().await.clone();
    proxy.abort();
    eprintln!(
        "{fault:?}: result={result:?}, proposals={}, value={}, applies={}",
        proposals.load(Ordering::SeqCst),
        state.value,
        state.applies
    );
    assert_eq!(state.value, 1, "one logical proposal must increment once");
    assert_eq!(state.applies, 1);
    if matches!(fault, ProposeFault::NotLeader) {
        assert!(result.is_ok());
        assert_eq!(proposals.load(Ordering::SeqCst), 2);
    } else {
        assert!(
            matches!(result, Err(ProposeError::Unknown(_))),
            "lost response must report an ambiguous outcome"
        );
        assert_eq!(proposals.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_does_not_reapply_after_losing_response_headers() {
    relay_fault_case(ProposeFault::LostHeaders).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_does_not_reapply_after_truncated_response_body() {
    relay_fault_case(ProposeFault::TruncatedBody).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_retries_explicit_rejection_before_apply() {
    relay_fault_case(ProposeFault::NotLeader).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_reports_unknown_after_post_apply_server_error() {
    relay_fault_case(ProposeFault::ServerError).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_does_not_retry_bare_503_after_apply() {
    relay_fault_case(ProposeFault::Bare503).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_reports_unknown_after_invalid_success_json() {
    relay_fault_case(ProposeFault::InvalidJson).await;
}

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
