//! Multi-node Raft integration tests.
//!
//! Spins up a real 5-node cluster over the HTTP layer (one ephemeral
//! public listener + one ephemeral raft private listener per node),
//! bootstraps the membership through `/cluster/initialize` +
//! `/cluster/add-learner` + `/cluster/change-membership`, and then
//! exercises the write/query/failover paths end-to-end.
//!
//! Each test pays the cluster-spawn cost in full (~1–3s on a quiet
//! machine) so the scenarios stay independent — a failure in one test
//! never contaminates another's cluster state.

#![cfg(feature = "raft")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_consensus::raft::admin::{
    AddLearnerRequest, ChangeMembershipRequest, ClusterStatus, InitializeRequest, NodeAddrs,
};
use fluree_db_consensus::NodeId;
use fluree_db_server::raft::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_server::{AppState, FlureeServerBuilder};
use reqwest::StatusCode;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const CLUSTER_SIZE: u64 = 5;

/// Best-effort tracing init for debug runs. Honors `RUST_LOG`; safe
/// to call from every test (a second call is a no-op).
fn init_test_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

/// Cap on how long any wait_for_* poll can run. Generous so slow CI
/// machines don't flake; tests that need a tighter bound override it
/// at the call site.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

/// A live raft node in the test cluster.
struct TestNode {
    node_id: NodeId,
    public_url: String,
    raft_url: String,
    /// Shared with the routers; held here so the leader watcher and
    /// other background tasks keep running for the lifetime of the
    /// node (the public router's State extractor also holds it, so
    /// dropping a node by aborting just its listeners is enough).
    _state: Arc<AppState>,
    /// JoinHandles for the two listener tasks. Aborting both takes
    /// the node off the network entirely (other nodes' heartbeats and
    /// client requests both fail).
    public_task: JoinHandle<()>,
    raft_task: JoinHandle<()>,
    /// Per-node raft log directory. Kept on the node so it's cleaned
    /// up when the node is dropped, not before. (The data storage
    /// tree is shared across nodes — owned by `TestCluster`.)
    _raft_tmp: TempDir,
}

impl TestNode {
    /// True when the node's listener tasks are still running.
    /// `shutdown` flips this by aborting both tasks.
    fn is_alive(&self) -> bool {
        !self.public_task.is_finished() && !self.raft_task.is_finished()
    }

    /// Take the node off the cluster. Stops the raft loop (so it
    /// stops sending heartbeats to peers) and aborts both listener
    /// tasks (so peers and clients can't reach it). The combination
    /// is what triggers a fresh election on the surviving quorum.
    async fn shutdown(&mut self) {
        if let Some(raft) = self._state.raft.as_ref() {
            // shutdown() returns Err if the raft loop already exited;
            // either way the post-condition (raft no longer running)
            // holds, so ignore.
            let _ = raft.raft.shutdown().await;
        }
        self.public_task.abort();
        self.raft_task.abort();
    }
}

struct TestCluster {
    nodes: Vec<TestNode>,
    client: reqwest::Client,
    /// All nodes share a single data storage tree (commit blobs,
    /// index roots, etc.). Raft replicates nameservice state across
    /// nodes; commit bodies live in this shared directory so
    /// followers can read what the leader wrote. Held on the cluster
    /// so it lives at least as long as the nodes.
    _shared_data_tmp: TempDir,
}

impl TestCluster {
    /// Spawn `count` raft nodes on ephemeral ports. The cluster is
    /// not yet bootstrapped — call [`Self::bootstrap`] next.
    async fn spawn(count: u64) -> Self {
        assert!(count >= 1, "cluster must have at least one node");

        // Single shared data directory across all nodes — the
        // commit worker (leader-only) writes blobs here; every node
        // (leader + followers) reads them back. The raft log itself
        // stays per-node so each node has its own persisted state.
        let shared_data_tmp = TempDir::new().expect("shared data tempdir");

        // Bind one public + one raft listener per node up front and
        // hold them across the build → serve handoff. Reserving the
        // port and dropping the listener (the "discover port" trick)
        // races against concurrent tests under `cargo test`'s default
        // parallelism — a sibling test snatches the port in the
        // drop-then-rebind window and the cluster fails to start.
        let mut listeners = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let public = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind public listener");
            let raft = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind raft listener");
            listeners.push((public, raft));
        }

        let mut nodes = Vec::with_capacity(count as usize);
        for (i, (public_listener, raft_listener)) in listeners.into_iter().enumerate() {
            let node_id = (i as u64) + 1;
            nodes.push(
                spawn_node(
                    node_id,
                    public_listener,
                    raft_listener,
                    shared_data_tmp.path(),
                )
                .await,
            );
        }

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client");

        Self {
            nodes,
            client,
            _shared_data_tmp: shared_data_tmp,
        }
    }

    /// Bootstrap the cluster: initialize node 1 alone, add the rest
    /// as learners, then promote to voters. Waits for a stable
    /// leader before returning.
    async fn bootstrap(&self) {
        let leader = &self.nodes[0];

        // 1. Single-node initialize on node 1.
        let init_body = InitializeRequest {
            members: BTreeMap::from([(leader.node_id, leader.node_addrs())]),
        };
        let resp = self
            .client
            .post(format!("{}/cluster/initialize", leader.raft_url))
            .json(&init_body)
            .send()
            .await
            .expect("initialize request");
        assert!(
            resp.status().is_success(),
            "initialize failed: {}",
            resp.text().await.unwrap_or_default()
        );

        // Node 1 should win the immediate single-node election.
        let initial_leader = self.wait_for_leader(DEFAULT_TIMEOUT).await;
        assert_eq!(
            initial_leader, leader.node_id,
            "single-node bootstrap should elect node 1"
        );

        // 2. Add every other node as a learner. The leader replicates
        //    the existing log to each before returning.
        for node in self.nodes.iter().skip(1) {
            let body = AddLearnerRequest {
                node_id: node.node_id,
                addrs: node.node_addrs(),
                blocking: true,
            };
            let resp = self
                .client
                .post(format!("{}/cluster/add-learner", leader.raft_url))
                .json(&body)
                .send()
                .await
                .expect("add-learner request");
            assert!(
                resp.status().is_success(),
                "add-learner({}) failed: {}",
                node.node_id,
                resp.text().await.unwrap_or_default()
            );
        }

        // 3. Promote every learner to a voter.
        let voters: BTreeSet<NodeId> = self.nodes.iter().map(|n| n.node_id).collect();
        let body = ChangeMembershipRequest {
            members: voters.clone(),
            retain: false,
        };
        let resp = self
            .client
            .post(format!("{}/cluster/change-membership", leader.raft_url))
            .json(&body)
            .send()
            .await
            .expect("change-membership request");
        assert!(
            resp.status().is_success(),
            "change-membership failed: {}",
            resp.text().await.unwrap_or_default()
        );

        // Wait for every live node to agree on the same leader and
        // see the full voter set — guards against tests that look at
        // a node before it has caught up to membership.
        self.wait_for_cluster_consensus(&voters, DEFAULT_TIMEOUT)
            .await;
    }

    /// Poll `/cluster/status` on the first live node and return its
    /// current_leader, if any.
    async fn current_leader(&self) -> Option<NodeId> {
        for node in self.nodes.iter().filter(|n| n.is_alive()) {
            if let Ok(status) = self.cluster_status(node).await {
                if status.current_leader.is_some() {
                    return status.current_leader;
                }
            }
        }
        None
    }

    /// Poll `current_leader` until non-None or `timeout` elapses.
    async fn wait_for_leader(&self, timeout: Duration) -> NodeId {
        let deadline = Instant::now() + timeout;
        let mut last = None;
        while Instant::now() < deadline {
            if let Some(id) = self.current_leader().await {
                return id;
            }
            last = Some("none yet");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for a leader (last: {last:?})");
    }

    /// Poll until every live node reports the same leader id and the
    /// expected voter set, or the timeout elapses.
    async fn wait_for_cluster_consensus(
        &self,
        expected_voters: &BTreeSet<NodeId>,
        timeout: Duration,
    ) {
        let deadline = Instant::now() + timeout;
        loop {
            let mut leaders = BTreeSet::new();
            let mut all_have_voters = true;
            for node in self.nodes.iter().filter(|n| n.is_alive()) {
                match self.cluster_status(node).await {
                    Ok(status) => {
                        if let Some(id) = status.current_leader {
                            leaders.insert(id);
                        } else {
                            all_have_voters = false;
                        }
                        if &status.voters != expected_voters {
                            all_have_voters = false;
                        }
                    }
                    Err(_) => {
                        all_have_voters = false;
                    }
                }
            }
            if all_have_voters && leaders.len() == 1 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for consensus (leaders={leaders:?}, expected_voters={expected_voters:?})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn cluster_status(&self, node: &TestNode) -> reqwest::Result<ClusterStatus> {
        self.client
            .get(format!("{}/cluster/status", node.raft_url))
            .send()
            .await?
            .error_for_status()?
            .json::<ClusterStatus>()
            .await
    }

    /// Find a node id that is not the current leader (any follower).
    /// Panics if the cluster has no leader or only one live node.
    async fn pick_follower(&self) -> NodeId {
        let leader = self.current_leader().await.expect("leader present");
        self.nodes
            .iter()
            .find(|n| n.is_alive() && n.node_id != leader)
            .map(|n| n.node_id)
            .expect("at least one follower")
    }

    /// Public URL for a specific node id.
    fn public_url(&self, node_id: NodeId) -> &str {
        &self
            .nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .expect("node exists")
            .public_url
    }

    /// Shutdown a node by id — stops its raft loop and aborts its
    /// listeners so peers see it as gone.
    async fn shutdown_node(&mut self, node_id: NodeId) {
        let node = self
            .nodes
            .iter_mut()
            .find(|n| n.node_id == node_id)
            .expect("node exists");
        node.shutdown().await;
    }

    /// Create a ledger by posting to the chosen node. Follower-forward
    /// middleware on write routes relays to the leader transparently,
    /// so any live node works.
    async fn create_ledger(&self, via_node: NodeId, ledger: &str) {
        let url = format!("{}/v1/fluree/create", self.public_url(via_node));
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(json!({ "ledger": ledger }).to_string())
            .send()
            .await
            .expect("create request");
        let status = resp.status();
        assert!(
            status == StatusCode::CREATED || status.is_success(),
            "create({ledger}) via node {via_node} returned {status}: {}",
            resp.text().await.unwrap_or_default()
        );
    }

    /// Insert a single `{subject, "ex:name", literal}` triple via
    /// the chosen node.
    async fn insert_subject(&self, via_node: NodeId, ledger: &str, subject: &str, name: &str) {
        let url = format!("{}/v1/fluree/insert", self.public_url(via_node));
        let body = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:{subject}"),
            "ex:name": name,
        });
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("fluree-ledger", ledger)
            .body(body.to_string())
            .send()
            .await
            .expect("insert request");
        assert!(
            resp.status().is_success(),
            "insert({subject}) via node {via_node} returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    /// SPARQL-style JSON query against the chosen node. Returns the
    /// raw `?name` bindings as a sorted Vec for stable equality.
    async fn query_names(&self, via_node: NodeId, ledger: &str) -> Vec<String> {
        let url = format!("{}/v1/fluree/query", self.public_url(via_node));
        let body = json!({
            "@context": { "ex": "http://example.org/" },
            "from": ledger,
            "select": ["?name"],
            "where": { "@id": "?s", "ex:name": "?name" }
        });
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .expect("query request");
        assert!(
            resp.status().is_success(),
            "query via node {via_node} returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
        let json: serde_json::Value = resp.json().await.expect("query json");
        let mut names: Vec<String> = json
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        row.get("name")
                            .or_else(|| row.get(0))
                            .and_then(|v| v.as_str().map(str::to_string))
                    })
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Poll `query_names` on `via_node` until it returns `expected`
    /// (sorted) or `timeout` elapses. Used to mask the bounded delay
    /// between a leader commit and a follower's apply.
    async fn wait_for_names(
        &self,
        via_node: NodeId,
        ledger: &str,
        expected: &[&str],
        timeout: Duration,
    ) {
        let want: Vec<String> = {
            let mut v: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
            v.sort();
            v
        };
        let deadline = Instant::now() + timeout;
        let mut last = Vec::new();
        while Instant::now() < deadline {
            last = self.query_names(via_node, ledger).await;
            if last == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for node {via_node} to see {want:?} on {ledger}; last={last:?}");
    }
}

impl TestNode {
    fn node_addrs(&self) -> NodeAddrs {
        NodeAddrs {
            raft_addr: format!("{}/raft", self.raft_url),
            client_addr: self.public_url.clone(),
        }
    }
}

/// Build a single raft-enabled `FlureeServer`, extract its routers,
/// and spawn both listeners. Caller provides the pre-bound listeners
/// so the public/raft addresses can be discovered ahead of building
/// the server (the server needs to know its self-addrs) without ever
/// dropping the listener (which would race with concurrent tests
/// for the same kernel-assigned port).
async fn spawn_node(
    node_id: NodeId,
    public_listener: TcpListener,
    raft_listener: TcpListener,
    shared_data_path: &std::path::Path,
) -> TestNode {
    let public_addr = public_listener.local_addr().expect("public local_addr");
    let raft_addr = raft_listener.local_addr().expect("raft local_addr");

    let raft_tmp = TempDir::new().expect("raft tempdir");

    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(
            node_id,
            raft_tmp.path().to_path_buf(),
        ))
        .await
        .expect("raft bootstrap"),
    );

    // Build the server with its self-address baked in. All nodes
    // point at the same data directory so a follower can read commit
    // blobs the leader wrote. Indexing stays on so the indexer keeps
    // the index roots in shared storage current — the alternative
    // (indexing off) leaves the follower's query path hunting for
    // index roots that nobody has built.
    let server = FlureeServerBuilder::file(shared_data_path)
        .listen_addr(public_addr)
        .cors_enabled(false)
        .with_raft(Arc::clone(&integration), raft_addr)
        .build()
        .await
        .expect("server build");

    let state = Arc::clone(server.state());
    let public_router = server.router();
    let raft_router = integration.private_router();
    // Drop `server` here: that detaches `raft_leader_watcher` (still
    // running on the runtime) and drops the unused `raft_listener`
    // field. We serve both routers ourselves below so the
    // `shutdown_node` path can abort just the raft side without
    // losing the public side (or vice versa).
    drop(server);

    let public_task = tokio::spawn(async move {
        let _ = axum::serve(public_listener, public_router).await;
    });
    let raft_task = tokio::spawn(async move {
        let _ = axum::serve(raft_listener, raft_router).await;
    });

    let public_url = format!("http://{public_addr}");
    let raft_url = format!("http://{raft_addr}");

    TestNode {
        node_id,
        public_url,
        raft_url,
        _state: state,
        public_task,
        raft_task,
        _raft_tmp: raft_tmp,
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Diagnostic: single-node cluster, post directly to the leader.
/// Confirms the test harness produces a working HTTP path before the
/// multi-node forwarding tests run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_round_trip_via_http() {
    let cluster = TestCluster::spawn(1).await;
    cluster.bootstrap().await;

    let leader = cluster.current_leader().await.expect("leader");
    let ledger = "raft:smoke";
    cluster.create_ledger(leader, ledger).await;
    cluster
        .insert_subject(leader, ledger, "smoke", "Smoke")
        .await;
    cluster
        .wait_for_names(leader, ledger, &["Smoke"], DEFAULT_TIMEOUT)
        .await;
}

/// Writes sent to a follower are forwarded to the leader, replicated,
/// and visible on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_path_follower_forwards_to_leader() {
    init_test_tracing();
    let cluster = TestCluster::spawn(CLUSTER_SIZE).await;
    cluster.bootstrap().await;

    let follower = cluster.pick_follower().await;
    let ledger = "raft:happy";

    cluster.create_ledger(follower, ledger).await;
    cluster
        .insert_subject(follower, ledger, "alice", "Alice")
        .await;
    cluster.insert_subject(follower, ledger, "bob", "Bob").await;

    // Verify all five nodes see both writes (with bounded wait for
    // followers to apply the committed log).
    for node in &cluster.nodes {
        cluster
            .wait_for_names(node.node_id, ledger, &["Alice", "Bob"], DEFAULT_TIMEOUT)
            .await;
    }
}

/// Many transactions fan out to every node in parallel; the final
/// state matches the union of all writes and every node sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_across_all_nodes() {
    init_test_tracing();
    let cluster = TestCluster::spawn(CLUSTER_SIZE).await;
    cluster.bootstrap().await;

    let ledger = "raft:concurrent";
    cluster
        .create_ledger(cluster.nodes[0].node_id, ledger)
        .await;

    // 5 writes per node, each to a distinct subject so there's no
    // intra-ledger CAS contention to muddy what we're testing.
    const PER_NODE: usize = 5;
    let mut handles = Vec::new();
    let client = cluster.client.clone();
    let urls: Vec<(NodeId, String)> = cluster
        .nodes
        .iter()
        .map(|n| (n.node_id, n.public_url.clone()))
        .collect();
    for (node_id, url) in urls {
        for i in 0..PER_NODE {
            let client = client.clone();
            let url = format!("{url}/v1/fluree/insert");
            let subject = format!("n{node_id}s{i}");
            let name = format!("Person-{node_id}-{i}");
            let body = json!({
                "@context": { "ex": "http://example.org/" },
                "@id": format!("ex:{subject}"),
                "ex:name": name,
            });
            handles.push(tokio::spawn(async move {
                let resp = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .header("fluree-ledger", ledger)
                    .body(body.to_string())
                    .send()
                    .await
                    .expect("insert request");
                assert!(
                    resp.status().is_success(),
                    "insert via node {node_id} returned {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }));
        }
    }
    for h in handles {
        h.await.expect("insert task");
    }

    let mut expected: Vec<String> = (0..CLUSTER_SIZE)
        .flat_map(|n| (0..PER_NODE).map(move |i| format!("Person-{}-{}", n + 1, i)))
        .collect();
    expected.sort();
    let expected_refs: Vec<&str> = expected.iter().map(String::as_str).collect();

    for node in &cluster.nodes {
        cluster
            .wait_for_names(node.node_id, ledger, &expected_refs, DEFAULT_TIMEOUT)
            .await;
    }
}

/// A write through node A is visible from a query against node B
/// after the bounded apply delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_your_write_across_nodes() {
    init_test_tracing();
    let cluster = TestCluster::spawn(CLUSTER_SIZE).await;
    cluster.bootstrap().await;

    let ledger = "raft:ryw";
    let writer = cluster.nodes[0].node_id;
    cluster.create_ledger(writer, ledger).await;
    cluster
        .insert_subject(writer, ledger, "carol", "Carol")
        .await;

    // Every other node should converge on the same view; each gets
    // its own bounded poll so a slow follower doesn't block the rest.
    for node in cluster.nodes.iter().filter(|n| n.node_id != writer) {
        cluster
            .wait_for_names(node.node_id, ledger, &["Carol"], DEFAULT_TIMEOUT)
            .await;
    }
}

/// Kill the current leader mid-workload, wait for a new election,
/// then resume writes through any surviving node and verify the full
/// history is intact on every surviving node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_resumes_writes() {
    let mut cluster = TestCluster::spawn(CLUSTER_SIZE).await;
    cluster.bootstrap().await;

    let ledger = "raft:failover";
    cluster
        .create_ledger(cluster.nodes[0].node_id, ledger)
        .await;
    cluster
        .insert_subject(cluster.nodes[0].node_id, ledger, "dave", "Dave")
        .await;

    let old_leader = cluster.current_leader().await.expect("leader present");

    // Wait briefly so every node has a chance to apply the first
    // write before we kill the leader — failure here would mean
    // we're testing apply-after-leader-loss, not failover proper.
    cluster
        .wait_for_names(old_leader, ledger, &["Dave"], DEFAULT_TIMEOUT)
        .await;

    cluster.shutdown_node(old_leader).await;

    // Quorum survives losing one of five nodes; a new leader should
    // emerge within a few election timeouts. The bound here is
    // generous so a slow CI tick doesn't flake the test — the
    // raft default election timeout is on the order of hundreds of
    // milliseconds.
    let new_leader = {
        let deadline = Instant::now() + DEFAULT_TIMEOUT;
        let mut current = old_leader;
        while Instant::now() < deadline {
            if let Some(id) = cluster.current_leader().await {
                if id != old_leader {
                    current = id;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_ne!(
            current, old_leader,
            "no new leader elected within {DEFAULT_TIMEOUT:?}"
        );
        current
    };
    assert_ne!(new_leader, old_leader);

    // Resume writes through a surviving non-leader to also exercise
    // the new leader's forward path.
    let surviving_follower = cluster
        .nodes
        .iter()
        .find(|n| n.is_alive() && n.node_id != new_leader)
        .map(|n| n.node_id)
        .expect("surviving follower");
    cluster
        .insert_subject(surviving_follower, ledger, "erin", "Erin")
        .await;

    // Every surviving node should see both writes.
    for node in cluster.nodes.iter().filter(|n| n.is_alive()) {
        cluster
            .wait_for_names(node.node_id, ledger, &["Dave", "Erin"], DEFAULT_TIMEOUT)
            .await;
    }
}
