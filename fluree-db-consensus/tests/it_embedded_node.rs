//! A Raft-replicated nameservice node stood up by an embedding process.
//!
//! This is the contract `fluree_db_consensus::raft::embedded` exists to
//! keep: everything `fluree-db-server` does to wire an engine to Raft
//! is reachable without it. Nothing here imports `fluree_db_server`.
//! If that import ever becomes necessary to make this test pass, the
//! embedding story has regressed.
//!
//! Single node, real Raft, real HTTP routers mounted by the host at a
//! prefix of its choosing, one write through the committer, and the
//! proof that it went through consensus rather than the local writer:
//! the replicated nameservice's head moved, and the engine's ledger
//! cache — fed only by the adapter's watermark — agrees.

#![cfg(feature = "raft")]

use fluree_db_api::{FlureeBuilder, NameServiceMode};
use fluree_db_consensus::raft::embedded::{EmbeddedRaftConfig, EmbeddedRaftNode};
use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_consensus::{TransactionBody, TransactionRequest};
use fluree_db_nameservice::{NameServiceLookup, NameServicePublisher};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..500 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_embedder_can_stand_up_a_raft_node_and_write_through_it() {
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let data_dir = tempfile::tempdir().expect("data dir");

    // 1. Consensus half. Node id and storage root are plain arguments.
    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(7, raft_dir.path()))
            .await
            .expect("bootstrap"),
    );

    // 2. The host mounts the routers where it likes — here under a
    //    group name, beside whatever else it serves on that listener.
    let app = axum::Router::new()
        .nest("/raft/nameservice", integration.raft_rpc_router())
        .nest("/cluster/nameservice", integration.cluster_admin_router());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // 3. Engine half: one bus, one nameservice, shared with the adapter,
    //    and a log of the node's own on the payload store.
    let ns: Arc<dyn NameServicePublisher> = integration.nameservice();
    let fluree = Arc::new(
        FlureeBuilder::file(data_dir.path().to_string_lossy().as_ref())
            .with_event_bus(Arc::clone(&integration.event_bus))
            .with_storage_wal_owner("node-7")
            .build_client_with_nameservice(NameServiceMode::ReadWrite(Arc::clone(&ns)))
            .await
            .expect("engine builds"),
    );

    // 4. Attach. Thresholds come from the engine so staging and
    //    backpressure cannot disagree.
    let node = EmbeddedRaftNode::attach(
        Arc::clone(&integration),
        Arc::clone(&fluree),
        EmbeddedRaftConfig::for_engine(&fluree),
    )
    .await;

    // 5. Form a single-voter cluster, the same admin call an operator
    //    makes. The raft address is the prefix the host chose.
    let mut members = BTreeMap::new();
    members.insert(
        7u64,
        fluree_db_consensus::raft::ClusterNode::new(
            format!("http://127.0.0.1:{port}/raft/nameservice"),
            format!("http://127.0.0.1:{port}"),
        ),
    );
    integration
        .raft
        .initialize(members)
        .await
        .expect("initialize");
    eventually("self-election", async || {
        integration.raft.current_leader().await == Some(7)
    })
    .await;

    // 6. Create through the engine (creation is a nameservice op that
    //    goes through the replicated publisher), then WRITE THROUGH THE
    //    COMMITTER — never the engine handle.
    fluree
        .create_ledger("embedded/db")
        .await
        .expect("create ledger");
    let log_before = integration.raft.metrics().borrow().last_applied;
    let receipt = node
        .committer
        .transact(TransactionRequest {
            idempotency_key: None,
            ledger_id: "embedded/db:main".into(),
            body: TransactionBody::JsonLdInsert(serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice"
            })),
            txn_opts: Default::default(),
            commit_opts: Default::default(),
            tracking: None,
            governance: Default::default(),
        })
        .await
        .expect("transact through consensus");
    assert!(
        receipt.commit.t >= 1,
        "the commit must carry a t: {receipt:?}"
    );

    // 7. Proof it went through the log, not the local writer: the Raft
    //    log advanced (enqueue + head-apply are both entries), and the
    //    replicated nameservice's head is the commit the receipt names.
    let log_after = integration.raft.metrics().borrow().last_applied;
    assert!(
        log_after.map(|id| id.index) > log_before.map(|id| id.index),
        "a committer write must append to the Raft log: {log_before:?} -> {log_after:?}",
    );
    let record = ns
        .lookup("embedded/db:main")
        .await
        .expect("lookup")
        .expect("the ledger is registered");
    assert_eq!(
        record.commit_t, receipt.commit.t,
        "the replicated head must be the committed t",
    );

    // 8. And the engine's own cache agrees — fed by the adapter's
    //    synchronous watermark on apply, which is what `attach` wired.
    let manager = fluree.ledger_manager().expect("ledger manager");
    let cached_id = fluree_db_api::LedgerId::parse("embedded/db:main").unwrap();
    eventually(
        "the ledger cache to reach the replicated head",
        async || manager.current_t(&cached_id).await == Some(receipt.commit.t),
    )
    .await;

    // 9. Teardown in the order the node owns.
    node.shutdown().await;
}

/// Nodes share one payload store. Each engine, built as `attach` asks, opens
/// it beside the others instead of being refused its lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_built_per_the_contract_share_one_payload_store() {
    let data_dir = tempfile::tempdir().expect("data dir");
    let mut engines = Vec::new();
    for node_id in [1u64, 2] {
        let raft_dir = tempfile::tempdir().expect("raft dir");
        let integration =
            RaftIntegration::bootstrap(RaftBootstrapConfig::new(node_id, raft_dir.path()))
                .await
                .expect("bootstrap");
        let fluree = FlureeBuilder::file(data_dir.path().to_string_lossy().as_ref())
            .with_event_bus(Arc::clone(&integration.event_bus))
            .with_storage_wal_owner(format!("node-{node_id}"))
            .build_client_with_nameservice(NameServiceMode::ReadWrite(integration.nameservice()))
            .await
            .unwrap_or_else(|e| panic!("node {node_id} builds on the shared store: {e}"));
        engines.push((fluree, integration, raft_dir));
    }
}

/// Stand up a single-voter embedded node with `configure` applied to the
/// attach config, and return the pieces a test drives.
async fn stand_up(
    node_id: u64,
    configure: impl FnOnce(EmbeddedRaftConfig) -> EmbeddedRaftConfig,
) -> (
    Arc<RaftIntegration>,
    Arc<fluree_db_api::Fluree>,
    EmbeddedRaftNode,
    (tempfile::TempDir, tempfile::TempDir),
) {
    stand_up_with(node_id, |builder| builder, configure).await
}

/// [`stand_up`], with the engine's builder adjusted by `build`.
async fn stand_up_with(
    node_id: u64,
    build: impl FnOnce(FlureeBuilder) -> FlureeBuilder,
    configure: impl FnOnce(EmbeddedRaftConfig) -> EmbeddedRaftConfig,
) -> (
    Arc<RaftIntegration>,
    Arc<fluree_db_api::Fluree>,
    EmbeddedRaftNode,
    (tempfile::TempDir, tempfile::TempDir),
) {
    let data_dir = tempfile::tempdir().expect("data dir");
    let (integration, fluree, node, raft_dir, address) =
        start_node(node_id, data_dir.path(), build, configure).await;
    integration
        .raft
        .initialize(BTreeMap::from([(node_id, address)]))
        .await
        .expect("initialize");
    eventually("self-election", async || {
        integration.raft.current_leader().await == Some(node_id)
    })
    .await;
    (integration, fluree, node, (raft_dir, data_dir))
}

/// A node on the storage at `data_dir`, serving its Raft routes, attached
/// but in no cluster yet, and the address it is reached at.
async fn start_node(
    node_id: u64,
    data_dir: &std::path::Path,
    build: impl FnOnce(FlureeBuilder) -> FlureeBuilder,
    configure: impl FnOnce(EmbeddedRaftConfig) -> EmbeddedRaftConfig,
) -> (
    Arc<RaftIntegration>,
    Arc<fluree_db_api::Fluree>,
    EmbeddedRaftNode,
    tempfile::TempDir,
    fluree_db_consensus::raft::ClusterNode,
) {
    start_node_with(node_id, data_dir, build, configure, |app| app).await
}

/// [`start_node`], its routes served through `wrap`.
async fn start_node_with(
    node_id: u64,
    data_dir: &std::path::Path,
    build: impl FnOnce(FlureeBuilder) -> FlureeBuilder,
    configure: impl FnOnce(EmbeddedRaftConfig) -> EmbeddedRaftConfig,
    wrap: impl FnOnce(axum::Router) -> axum::Router,
) -> (
    Arc<RaftIntegration>,
    Arc<fluree_db_api::Fluree>,
    EmbeddedRaftNode,
    tempfile::TempDir,
    fluree_db_consensus::raft::ClusterNode,
) {
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(node_id, raft_dir.path()))
            .await
            .expect("bootstrap"),
    );
    let app = wrap(
        axum::Router::new()
            .nest("/raft/nameservice", integration.raft_rpc_router())
            .nest("/cluster/nameservice", integration.cluster_admin_router()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let ns: Arc<dyn NameServicePublisher> = integration.nameservice();
    let builder = FlureeBuilder::file(data_dir.to_string_lossy().as_ref())
        .with_event_bus(Arc::clone(&integration.event_bus))
        .with_storage_wal_owner(format!("node-{node_id}"));
    let fluree = Arc::new(
        build(builder)
            .build_client_with_nameservice(NameServiceMode::ReadWrite(ns))
            .await
            .expect("engine builds"),
    );
    let node = EmbeddedRaftNode::attach(
        Arc::clone(&integration),
        Arc::clone(&fluree),
        configure(EmbeddedRaftConfig::for_engine(&fluree)),
    )
    .await;
    let address = fluree_db_consensus::raft::ClusterNode::new(
        format!("http://127.0.0.1:{port}/raft/nameservice"),
        format!("http://127.0.0.1:{port}"),
    );
    (integration, fluree, node, raft_dir, address)
}

/// A transaction large enough that staging plus the Raft round trip
/// always outlasts a probe interval measured in milliseconds.
fn bulk_request(ledger_id: &str, nodes: usize) -> TransactionRequest {
    bulk_request_round(ledger_id, nodes, 0)
}

/// [`bulk_request`] with subjects unique to `round`, so consecutive
/// submissions to one ledger each assert new data.
fn bulk_request_round(ledger_id: &str, nodes: usize, round: usize) -> TransactionRequest {
    let graph: Vec<serde_json::Value> = (0..nodes)
        .map(|i| {
            serde_json::json!({
                "@id": format!("ex:round{round}-item{i}"),
                "@type": "ex:Item",
                "ex:name": format!("Item {i}"),
                "ex:rank": i as i64
            })
        })
        .collect();
    TransactionRequest {
        idempotency_key: None,
        ledger_id: ledger_id.into(),
        body: TransactionBody::JsonLdInsert(serde_json::json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": graph
        })),
        txn_opts: Default::default(),
        commit_opts: Default::default(),
        tracking: None,
        governance: Default::default(),
    }
}

/// The waiter's probe interval bounds leader-transition recovery, not
/// commit latency: a submission whose stage outlasts the interval — by a
/// wide margin here, with a 1 ms probe and a single attempt — stays
/// parked while its entry is queued and comes back with a receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_submission_outlives_the_probe_interval_while_its_entry_is_queued() {
    let (_integration, fluree, node, _dirs) = stand_up(11, |config| {
        config.with_submit_wait(Duration::from_millis(1), 1)
    })
    .await;
    fluree
        .create_ledger("embedded/slow")
        .await
        .expect("create ledger");

    let receipt = node
        .committer
        .transact(bulk_request("embedded/slow:main", 2_000))
        .await
        .expect("a slow but live submission must not be reported stranded");
    assert!(
        receipt.commit.t >= 1,
        "the commit must carry a t: {receipt:?}"
    );
    assert_eq!(receipt.commit.flake_count, 2_000 * 3);
}

/// The ceiling is the only clock that turns a live, still-queued entry
/// into a 504 — and it says so, naming the outcome as unknown rather
/// than failed, because the worker commits regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_wait_ceiling_bounds_a_live_submission_and_reports_it_unknown() {
    let (integration, fluree, node, _dirs) = stand_up(12, |config| {
        config
            .with_submit_wait(Duration::from_millis(1), 1)
            .with_submit_max_wait(Duration::from_millis(1))
    })
    .await;
    fluree
        .create_ledger("embedded/ceiling")
        .await
        .expect("create ledger");

    let err = node
        .committer
        .transact(bulk_request("embedded/ceiling:main", 2_000))
        .await
        .expect_err("a 1 ms ceiling cannot outlast a 2,000-node stage");
    let text = format!("{err:?}");
    assert!(
        text.contains("504") && text.contains("outcome is unknown"),
        "ceiling must surface as a 504 with the outcome unknown: {text}"
    );

    // The submission the caller gave up on still commits.
    let ns: Arc<dyn NameServiceLookup> = integration.nameservice();
    eventually("the abandoned submission commits anyway", async || {
        ns.lookup("embedded/ceiling:main")
            .await
            .ok()
            .flatten()
            .is_some_and(|record| record.commit_t >= 1)
    })
    .await;
}

/// A ceiling shorter than the probe must wake a live waiter on its own.
/// Hold the ledger write lock so completion cannot race the ceiling, and
/// release it after the caller times out to prove the queued write still
/// commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_wait_ceiling_bounds_a_submission_before_its_next_probe() {
    use fluree_db_consensus::raft::state_machine::RefKey;

    let (integration, fluree, node, _dirs) = stand_up(14, |config| {
        config
            .with_submit_wait(Duration::from_secs(10), 1)
            .with_submit_max_wait(Duration::from_millis(20))
    })
    .await;
    let ledger_id = "embedded/short-ceiling:main";
    fluree
        .create_ledger("embedded/short-ceiling")
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_manager()
        .expect("ledger manager")
        .get_or_load(&fluree_db_api::LedgerId::parse(ledger_id).unwrap())
        .await
        .expect("load ledger");
    let guard = handle.lock_for_write().await;
    let committer = Arc::clone(&node.committer);
    let mut submission =
        tokio::spawn(async move { committer.transact(bulk_request(ledger_id, 1)).await });

    eventually("the blocked submission is queued", async || {
        integration
            .shared_state
            .read()
            .await
            .queues
            .get(&RefKey::new("embedded/short-ceiling", "main"))
            .is_some_and(|queue| !queue.is_empty())
    })
    .await;

    // Allow scheduling slack, but stay well below the 10-second probe.
    let result = tokio::time::timeout(Duration::from_secs(3), &mut submission).await;
    drop(guard);
    let err = result
        .expect("the 20 ms ceiling must wake the waiter before its 10 s probe")
        .expect("submission task")
        .expect_err("a blocked worker must reach the ceiling");
    assert!(
        matches!(err, fluree_db_consensus::SubmissionError::Execution { status: 504, ref message }
            if message.contains("outcome is unknown")),
        "ceiling must report a 504 with the outcome unknown: {err:?}"
    );

    let ns: Arc<dyn NameServiceLookup> = integration.nameservice();
    eventually(
        "the timed-out write commits after the lock is released",
        async || {
            ns.lookup(ledger_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|record| record.commit_t >= 1)
        },
    )
    .await;
    node.shutdown().await;
}

/// The entry leaves the replicated queue under the state lock, but its
/// terminal apply resolves the waiter only after that lock drops. A probe
/// that reads in between sees the entry gone while the receipt is moments
/// from landing, and must wait for that receipt rather than report the
/// submission stranded. One submission at a 1 ms probe hits the window
/// roughly one time in six; twenty in a row make missing it the exception.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_that_finds_the_entry_just_popped_still_returns_the_receipt() {
    let (_integration, fluree, node, _dirs) = stand_up(13, |config| {
        config.with_submit_wait(Duration::from_millis(1), 1)
    })
    .await;
    fluree
        .create_ledger("embedded/popped")
        .await
        .expect("create ledger");

    let mut last_t = 0;
    for round in 0..20 {
        let receipt = node
            .committer
            .transact(bulk_request_round("embedded/popped:main", 200, round))
            .await
            .unwrap_or_else(|err| {
                panic!("round {round}: a receipt landing right behind the probe must not be reported stranded: {err:?}")
            });
        assert!(
            receipt.commit.t > last_t,
            "round {round}: head must advance: {receipt:?}"
        );
        last_t = receipt.commit.t;
    }
}

/// A fast-forward merge lands on a queue-backed nameservice.
///
/// The regression this pins: `apply_merge`'s fast-forward arm used to call
/// `publish_commit`, whose contract on the replicated data plane is "apply
/// the staged queue front". A fast-forward stages nothing on the target —
/// that is what makes it a fast-forward — so it failed with "per-branch
/// queue is empty — nothing staged for this branch" on precisely the merges
/// that need the least work, while a divergent merge (which stages a merge
/// commit) succeeded.
///
/// It has to run here rather than beside the other merge tests: those build
/// `FlureeBuilder::memory()`, whose `publish_commit` is already a plain ref
/// write, so every one of them passed throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_forward_merge_lands_through_consensus() {
    use fluree_db_api::ConflictStrategy;

    let (integration, fluree, node, _dirs) = stand_up(15, |config| config).await;
    fluree.create_ledger("ffdb").await.expect("create ledger");

    // One commit on main, so the branch has somewhere to fork from.
    node.committer
        .transact(TransactionRequest {
            idempotency_key: None,
            ledger_id: "ffdb:main".into(),
            body: TransactionBody::JsonLdInsert(serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:base", "ex:name": "base"
            })),
            txn_opts: Default::default(),
            commit_opts: Default::default(),
            tracking: None,
            governance: Default::default(),
        })
        .await
        .expect("seed main");

    fluree
        .create_branch("ffdb", "feature", None, None)
        .await
        .expect("create branch");

    // Two commits on the branch and NONE on main — the shape that failed.
    for i in 0..2 {
        node.committer
            .transact(TransactionRequest {
                idempotency_key: None,
                ledger_id: "ffdb:feature".into(),
                body: TransactionBody::JsonLdInsert(serde_json::json!({
                    "@context": {"ex": "http://example.org/"},
                    "@id": format!("ex:item{i}"), "ex:name": format!("item {i}")
                })),
                txn_opts: Default::default(),
                commit_opts: Default::default(),
                tracking: None,
                governance: Default::default(),
            })
            .await
            .expect("commit on branch");
    }

    let report = fluree
        .merge_branch("ffdb", "feature", None, ConflictStrategy::default())
        .await
        .expect("a fast-forward merge must land on a queue-backed nameservice");

    assert!(
        report.fast_forward,
        "main never advanced, so this must be a fast-forward: {report:?}"
    );
    assert_eq!(
        report.conflict_count, 0,
        "nothing diverged, so nothing can conflict: {report:?}"
    );
    assert_eq!(
        report.commits_copied, 2,
        "main gains both of the branch's commits: {report:?}"
    );

    // The head actually moved — a merge that reports success without
    // advancing the ref would leave the proposal unlanded.
    let record = integration
        .nameservice()
        .lookup("ffdb:main")
        .await
        .expect("lookup")
        .expect("main is registered");
    assert_eq!(
        record.commit_t, report.new_head_t,
        "main's replicated head must be the merged head: {report:?}"
    );

    node.shutdown().await;
}

/// An erasure activation replaces the view a branch's writes are made from.
/// A publication from the view before it is refused, and the committer's
/// next transaction, staged on the branch as it is now, lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_from_before_an_erasure_activation_are_refused_and_later_ones_land() {
    use fluree_db_consensus::raft::state_machine::{
        holder_state, ActivateErasure, Command, ErasureCommand, RefKey, Response,
    };
    use fluree_db_nameservice::{
        GraphSourcePublisher, GraphSourceType, IndexPublisher, NameServiceError,
    };

    let (integration, fluree, node, (_raft_dir, data_dir)) = stand_up(9, |config| config).await;
    fluree.create_ledger("erased/db").await.expect("create");
    // A manifest erasing nothing: what the activation installs.
    let manifest = fluree_db_core::sha256_hex(b"[]");
    let manifests = data_dir.path().join("@retention/erasure/manifest");
    std::fs::create_dir_all(&manifests).expect("manifest folder");
    std::fs::write(manifests.join(format!("{manifest}.json")), b"[]").expect("manifest");
    node.committer
        .transact(bulk_request_round("erased/db:main", 3, 0))
        .await
        .expect("first transaction");
    let ns = integration.nameservice();
    let before = ns
        .lookup("erased/db:main")
        .await
        .expect("lookup")
        .expect("registered");
    assert_eq!(before.erasure_generation, 0);
    ns.publish_graph_source(
        "search",
        "main",
        GraphSourceType::Bm25,
        "{}",
        &["erased/db:main".to_string()],
    )
    .await
    .expect("graph source");

    let key = RefKey::new("erased/db", "main");
    integration
        .raft
        .client_write(Command::Erasure(ErasureCommand::Track))
        .await
        .expect("track");
    let (holder, holder_set) = {
        let state = integration.shared_state.read().await;
        (holder_state(&state, &key), state.erasure.holder_set)
    };
    let activated = integration
        .raft
        .client_write(Command::Erasure(ErasureCommand::Activate(
            ActivateErasure {
                manifest,
                holder_set,
                holders: vec![holder],
                applied_at_millis: 0,
            },
        )))
        .await
        .expect("activate");
    assert!(
        matches!(activated.data, Response::ErasureActivated { .. }),
        "{:?}",
        activated.data
    );
    // The node installs the activation, then reports that it has.
    eventually("the node to install the activation", async || {
        ns.lookup("erased/db:main")
            .await
            .ok()
            .flatten()
            .is_some_and(|record| record.erasure_generation == 1)
    })
    .await;
    eventually("the node's readiness report", async || {
        let state = integration.shared_state.read().await;
        state.erasure.installed.get(&9).is_some_and(|installed| {
            installed.through == 1 && installed.incarnation == integration.erasure.incarnation()
        })
    })
    .await;

    let index = fluree_db_core::ContentId::new(fluree_db_core::ContentKind::IndexRoot, b"stale");
    let stale = ns
        .publish_index_fenced(
            "erased/db:main",
            before.write_basis(),
            before.commit_t,
            &index,
        )
        .await;
    assert!(
        matches!(stale, Err(NameServiceError::ApplyLagged(_))),
        "{stale:?}"
    );

    // A search index presents the generation of the source view it was
    // built from.
    let source = |g| [("erased/db:main".to_string(), g)];
    let stale = ns
        .publish_graph_source_index("search", "main", &index, 1, &source(0))
        .await;
    assert!(
        matches!(stale, Err(NameServiceError::ApplyLagged(_))),
        "{stale:?}"
    );
    ns.publish_graph_source_index("search", "main", &index, 1, &source(1))
        .await
        .expect("an index from the current view publishes");

    let receipt = tokio::time::timeout(
        Duration::from_secs(30),
        node.committer
            .transact(bulk_request_round("erased/db:main", 3, 1)),
    )
    .await
    .expect("a transaction after the activation lands in time")
    .expect("a transaction after the activation lands");
    assert!(receipt.commit.t > before.commit_t);
    node.shutdown().await;
}

/// A member transfers none of a ledger's history (a clone's pack, a page of
/// commits, a commit object) until it has installed the erasure activations
/// it applied: before, it would send what they erase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_transfers_wait_until_the_member_installs_its_erasures() {
    use fluree_db_consensus::raft::state_machine::{
        holder_state, ActivateErasure, Command, ErasureCommand, RefKey,
    };

    let (integration, fluree, node, (_raft_dir, data_dir)) = stand_up(9, |config| config).await;
    let ledger = "transfer/db:main";
    fluree.create_ledger("transfer/db").await.expect("create");
    node.committer
        .transact(bulk_request_round(ledger, 3, 0))
        .await
        .expect("transaction");
    // Activated before its manifest is stored, the erasure cannot be
    // installed yet.
    let manifest = fluree_db_core::sha256_hex(b"[]");
    integration
        .raft
        .client_write(Command::Erasure(ErasureCommand::Track))
        .await
        .expect("track");
    let (holder, holder_set) = {
        let state = integration.shared_state.read().await;
        (
            holder_state(&state, &RefKey::new("transfer/db", "main")),
            state.erasure.holder_set,
        )
    };
    integration
        .raft
        .client_write(Command::Erasure(ErasureCommand::Activate(
            ActivateErasure {
                manifest: manifest.clone(),
                holder_set,
                holders: vec![holder],
                applied_at_millis: 0,
            },
        )))
        .await
        .expect("activate");

    let handle = fluree.ledger_cached(ledger).await.expect("handle");
    let head = handle
        .snapshot()
        .await
        .head_commit_id
        .clone()
        .expect("head");
    let pack = async || {
        let request = fluree_db_api::pack::full_ledger_pack_request(&handle, false)
            .await
            .expect("pack request");
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let stats = fluree_db_api::pack::stream_pack(&fluree, &handle, &request, tx).await;
        let mut bytes = Vec::new();
        while let Some(chunk) = rx.recv().await {
            bytes.extend(chunk.expect("chunk"));
        }
        (
            stats.commits_sent,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    };
    let (sent, stream) = pack().await;
    assert_eq!(sent, 0, "{stream}");
    assert!(stream.contains("not yet installed"), "{stream}");
    let page = fluree
        .export_commit_range(&handle, &Default::default())
        .await;
    assert!(
        matches!(page, Err(fluree_db_api::ApiError::ReplicaNotCurrent(_))),
        "{page:?}"
    );
    let object = fluree.erased_object(ledger, &head).await;
    assert!(
        matches!(object, Err(fluree_db_api::ApiError::ReplicaNotCurrent(_))),
        "{:?}",
        object.err()
    );

    let manifests = data_dir.path().join("@retention/erasure/manifest");
    std::fs::create_dir_all(&manifests).expect("manifest folder");
    std::fs::write(manifests.join(format!("{manifest}.json")), b"[]").expect("manifest");
    eventually("the node to install the activation", async || {
        fluree.erased_object(ledger, &head).await.is_ok()
    })
    .await;
    let (sent, stream) = pack().await;
    assert!(sent >= 1, "{stream}");
    fluree
        .export_commit_range(&handle, &Default::default())
        .await
        .expect("a page of commits");
    node.shutdown().await;
}

/// A cluster takes no push of history holding permanently erased commits
/// yet: neither erasures the push carries nor those of the history it
/// builds on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_of_erased_history_is_refused() {
    let (_integration, fluree, node, _dirs) = stand_up(9, |config| config).await;
    fluree.create_ledger("pushed/db").await.expect("create");
    let push = |erasure_frames, base_erasures| fluree_db_consensus::PushRequest {
        idempotency_key: None,
        ledger_id: "pushed/db:main".to_string(),
        commits: Vec::new(),
        blobs: Default::default(),
        merged_commits: Vec::new(),
        commit_ids: Vec::new(),
        merged_commit_ids: Vec::new(),
        governance: Default::default(),
        erasure_frames,
        base_erasures,
        retention: None,
        authority: Default::default(),
    };
    let base = fluree_db_api::BaseErasure {
        commit: fluree_db_core::ContentId::new(fluree_db_core::ContentKind::Commit, b"erased"),
        digest: "0".repeat(64),
        steps: 1,
    };
    for request in [
        push(vec![b"frame".to_vec()], Vec::new()),
        push(Vec::new(), vec![base]),
    ] {
        match node.committer.push(request).await {
            Err(fluree_db_consensus::SubmissionError::Execution { status: 501, .. }) => {}
            other => panic!("{other:?}"),
        }
    }
    node.shutdown().await;
}

/// A leader with a learner, both proving natively on one storage, and an
/// erasure of a value the leader's ledger asserted, accepted.
async fn erasing_pair() -> Pair {
    erasing_pair_with(|app| app).await
}

/// [`erasing_pair`], the learner's routes served through `wrap`.
async fn erasing_pair_with(wrap: impl FnOnce(axum::Router) -> axum::Router) -> Pair {
    use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
    use fluree_db_api::retention::FactSelector;

    let data_dir = tempfile::tempdir().expect("data dir");
    let provers = [
        Arc::new(fluree_db_erasure::testing::NativeProver::default()),
        Arc::new(fluree_db_erasure::testing::NativeProver::default()),
    ];
    let erasing = |prover: &Arc<fluree_db_erasure::testing::NativeProver>| {
        let prover = Arc::clone(prover);
        move |builder: FlureeBuilder| {
            builder.with_erasure(ErasureConfig {
                prover,
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
        }
    };
    let (integration, fluree, node, leader_raft, address) =
        start_node(61, data_dir.path(), erasing(&provers[0]), |config| config).await;
    integration
        .raft
        .initialize(BTreeMap::from([(61, address)]))
        .await
        .expect("initialize");
    eventually("self-election", async || {
        integration.raft.current_leader().await == Some(61)
    })
    .await;
    let (member_integration, member, member_node, member_raft, address) = start_node_with(
        62,
        data_dir.path(),
        erasing(&provers[1]),
        |config| config,
        wrap,
    )
    .await;
    integration
        .raft
        .add_learner(62, address, true)
        .await
        .expect("learner added");

    let id = "proved/db:main";
    fluree.create_ledger("proved/db").await.expect("create");
    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::JsonLdInsert(serde_json::json!({
        "@id": "http://example.org/alice",
        "http://example.org/email": "a@secret"
    }));
    node.committer.transact(request).await.expect("insert");
    let writer = fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
        std::sync::OnceLock::from(Arc::clone(&node.committer)),
    ));
    let erasure = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(serde_json::json!("a@secret")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences_with(&writer, id, erasure, None)
        .await
        .expect("accepted");
    Pair {
        integration,
        fluree,
        node,
        member_integration,
        member,
        member_node,
        provers,
        _dirs: (leader_raft, member_raft, data_dir),
    }
}

struct Pair {
    integration: Arc<RaftIntegration>,
    fluree: Arc<fluree_db_api::Fluree>,
    node: EmbeddedRaftNode,
    member_integration: Arc<RaftIntegration>,
    member: Arc<fluree_db_api::Fluree>,
    member_node: EmbeddedRaftNode,
    /// The leader's, then the member's.
    provers: [Arc<fluree_db_erasure::testing::NativeProver>; 2],
    _dirs: (tempfile::TempDir, tempfile::TempDir, tempfile::TempDir),
}

impl Pair {
    async fn job(&self) -> fluree_db_api::retention::erasure::ErasureJob {
        self.fluree
            .retention_status("proved/db:main")
            .await
            .expect("status")
            .registration
            .expect("registered")
            .erasures
            .remove(0)
    }

    async fn shutdown(self) {
        self.member_node.shutdown().await;
        self.node.shutdown().await;
    }
}

/// A leader has its proving steps proved by another member with a prover,
/// and records what that member proved; its own prover proves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_member_proves_the_leaders_steps() {
    let pair = erasing_pair().await;
    let report = pair
        .fluree
        .run_erasure_pass("proved/db:main")
        .await
        .expect("pass");
    let job = pair.job().await;
    assert_eq!(report.proved, job.commits.len(), "{report:?}");
    assert!(pair.provers[0].proved().is_empty());
    assert_eq!(pair.provers[1].proved().len(), job.commits.len());
    assert!(job.commits.iter().all(|c| c.proved.is_some()), "{job:?}");

    // A step ends with its leader's leadership: a member that does not
    // lead has none to wait out.
    use fluree_db_api::retention::erasure::ProvingMembers;
    tokio::time::timeout(
        Duration::from_secs(5),
        pair.member_integration.nameservice().deposed(),
    )
    .await
    .expect("a learner leads nothing");
    assert!(tokio::time::timeout(
        Duration::from_millis(300),
        pair.integration.nameservice().deposed()
    )
    .await
    .is_err());
    pair.shutdown().await;
}

/// A step a member proved for a leader since gone is not proved again: the
/// next leader finds what it stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_proved_for_an_earlier_leader_is_not_proved_again() {
    use fluree_db_api::retention::erasure::{MemberProving, ProofStep};

    let pair = erasing_pair().await;
    let job = pair.job().await;
    for task in &job.commits {
        let step = ProofStep {
            branch: "proved/db:main".to_string(),
            request_id: job.request_id.clone(),
            commit: task.commit.clone(),
            branches: task.branches.clone(),
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match pair.member.prove_for_leader(step.clone(), 62) {
                MemberProving::Done(outcome) => {
                    assert!(
                        matches!(
                            outcome,
                            fluree_db_api::retention::erasure::StepOutcome::Proved(_)
                        ),
                        "{outcome:?}"
                    );
                    break;
                }
                MemberProving::Proving => {}
                other => panic!("{other:?}"),
            }
            assert!(std::time::Instant::now() < deadline, "the member proves");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    // Gone from the cluster, the member is asked nothing.
    pair.integration
        .raft
        .change_membership(
            openraft::ChangeMembers::RemoveNodes(std::collections::BTreeSet::from([62])),
            false,
        )
        .await
        .expect("member removed");
    let report = pair
        .fluree
        .run_erasure_pass("proved/db:main")
        .await
        .expect("pass");
    assert_eq!(report.proved, job.commits.len(), "{report:?}");
    assert!(pair.provers[0].proved().is_empty());
    assert_eq!(pair.provers[1].proved().len(), job.commits.len());
    pair.shutdown().await;
}

/// Proves natively once let through, counting the proofs begun and the
/// most under way at once.
#[derive(Debug)]
struct GatedProver {
    inner: fluree_db_erasure::testing::NativeProver,
    gate: tokio::sync::Semaphore,
    begun: std::sync::atomic::AtomicUsize,
    running: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

impl GatedProver {
    fn closed() -> Arc<Self> {
        Arc::new(Self {
            inner: Default::default(),
            gate: tokio::sync::Semaphore::new(0),
            begun: Default::default(),
            running: Default::default(),
            most: Default::default(),
        })
    }

    fn open(&self) {
        self.gate.add_permits(1_000);
    }

    fn begun(&self) -> usize {
        self.begun.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn most(&self) -> usize {
        self.most.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl fluree_db_erasure::Prover for GatedProver {
    async fn prove(
        &self,
        request: &fluree_db_erasure::ProveRequest,
    ) -> Result<fluree_db_erasure::ProveResponse, fluree_db_erasure::ProverError> {
        use std::sync::atomic::Ordering::SeqCst;
        struct Running<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, SeqCst);
            }
        }
        self.begun.fetch_add(1, SeqCst);
        let now = self.running.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        let _running = Running(&self.running);
        let _permit = self.gate.acquire().await.expect("gate");
        self.inner.prove(request).await
    }
}

fn erasing_with(prover: &Arc<GatedProver>) -> impl FnOnce(FlureeBuilder) -> FlureeBuilder {
    let prover = Arc::clone(prover);
    move |builder| {
        builder.with_erasure(fluree_db_api::retention::erasure::ErasureConfig {
            prover,
            verifier: Arc::new(
                fluree_db_api::retention::erasure::Verifier::accepting_dev_receipts(),
            ),
            retry_ms: 10,
        })
    }
}

/// Insert a value on `id` and accept an erasure of it through `node`.
async fn insert_and_erase(fluree: &fluree_db_api::Fluree, node: &EmbeddedRaftNode, id: &str) {
    use fluree_db_api::retention::erasure::ErasureRequest;
    use fluree_db_api::retention::FactSelector;

    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::JsonLdInsert(serde_json::json!({
        "@id": "http://example.org/alice",
        "http://example.org/email": "a@secret"
    }));
    node.committer.transact(request).await.expect("insert");
    let writer = fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
        std::sync::OnceLock::from(Arc::clone(&node.committer)),
    ));
    let erasure = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(serde_json::json!("a@secret")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences_with(&writer, id, erasure, None)
        .await
        .expect("accepted");
}

async fn first_job(
    fluree: &fluree_db_api::Fluree,
    id: &str,
) -> fluree_db_api::retention::erasure::ErasureJob {
    fluree
        .retention_status(id)
        .await
        .expect("status")
        .registration
        .expect("registered")
        .erasures
        .remove(0)
}

/// A host proves one step at a time, whether for its leader or for itself:
/// a member still proving a step handed to it before it led waits for that
/// proof when it comes to the step itself, and finds it stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_proves_one_step_at_a_time() {
    use fluree_db_api::retention::erasure::{MemberProving, ProofStep};

    let prover = GatedProver::closed();
    let (_integration, fluree, node, _dirs) =
        stand_up_with(73, erasing_with(&prover), |config| config).await;
    let id = "gated/db:main";
    fluree.create_ledger("gated/db").await.expect("create");
    insert_and_erase(&fluree, &node, id).await;
    let job = first_job(&fluree, id).await;
    let step = ProofStep {
        branch: id.to_string(),
        request_id: job.request_id.clone(),
        commit: job.commits[0].commit.clone(),
        branches: job.commits[0].branches.clone(),
    };
    assert_eq!(fluree.prove_for_leader(step, 73), MemberProving::Proving);
    eventually("the handed proof to begin", async || prover.begun() == 1).await;

    let pass = tokio::spawn({
        let fluree = Arc::clone(&fluree);
        async move { fluree.run_erasure_pass(id).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(prover.begun(), 1, "a second proof began beside the first");
    prover.open();
    let report = pass.await.expect("pass task").expect("pass");
    assert_eq!(report.proved, job.commits.len(), "{report:?}");
    assert_eq!(prover.begun(), job.commits.len(), "a step was proved twice");
    assert_eq!(prover.most(), 1);
    node.shutdown().await;
}

/// A member whose first answer to the leader is lost has taken the step: the
/// leader asks again rather than proving it elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_answer_does_not_hand_the_step_on() {
    let lost = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let pair = erasing_pair_with(move |app| {
        app.layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let lost = Arc::clone(&lost);
                async move {
                    let prove = request.uri().path().ends_with("/erasure_prove");
                    let response = next.run(request).await;
                    if prove && lost.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        return axum::response::IntoResponse::into_response("garbled");
                    }
                    response
                }
            },
        ))
    })
    .await;
    let report = pair
        .fluree
        .run_erasure_pass("proved/db:main")
        .await
        .expect("pass");
    let job = pair.job().await;
    assert_eq!(report.proved, job.commits.len(), "{report:?}");
    assert!(pair.provers[0].proved().is_empty(), "the leader proved too");
    assert_eq!(pair.provers[1].proved().len(), job.commits.len());
    pair.shutdown().await;
}

/// A leader that loses its leadership mid-proof stops waiting on the member
/// proving its step, and the next leader, another member, asks that member
/// first and takes the proof under way over rather than proving it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_leader_adopts_a_proof_under_way() {
    adopted(false).await;
}

/// [`the_next_leader_adopts_a_proof_under_way`], led next by the member
/// proving the step: it waits for its own proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_leads_next_adopts_its_own_proof() {
    adopted(true).await;
}

async fn adopted(next_is_prover: bool) {
    use fluree_db_consensus::raft::ownership::owner;
    use fluree_db_consensus::raft::state_machine::RefKey;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    let data_dir = tempfile::tempdir().expect("data dir");
    let ids = [81u64, 82, 83];
    let provers = [
        GatedProver::closed(),
        GatedProver::closed(),
        GatedProver::closed(),
    ];
    let asks: [Arc<AtomicUsize>; 3] = Default::default();
    let mut nodes = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let counted = Arc::clone(&asks[i]);
        let started = start_node_with(
            *id,
            data_dir.path(),
            erasing_with(&provers[i]),
            |config| config,
            move |app| {
                app.layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        if request.uri().path().ends_with("/erasure_prove") {
                            counted.fetch_add(1, SeqCst);
                        }
                        next.run(request)
                    },
                ))
            },
        )
        .await;
        nodes.push(started);
    }
    let leader = &nodes[0].0;
    leader
        .raft
        .initialize(BTreeMap::from([(81, nodes[0].4.clone())]))
        .await
        .expect("initialize");
    eventually("self-election", async || {
        leader.raft.current_leader().await == Some(81)
    })
    .await;
    for i in [1, 2] {
        leader
            .raft
            .add_learner(ids[i], nodes[i].4.clone(), true)
            .await
            .expect("learner added");
    }
    leader
        .raft
        .change_membership(
            openraft::ChangeMembers::AddVoterIds(std::collections::BTreeSet::from([82, 83])),
            false,
        )
        .await
        .expect("voters");

    // A ledger a follower owns: the leader hands its step to the other, and
    // a new leader asking by preference alone would ask the old leader.
    let name = (0..)
        .map(|n| format!("adopted-{n}/db"))
        .find(|name| owner(&RefKey::new(name, "main"), &ids) != Some(81))
        .expect("a name");
    let owning = owner(&RefKey::new(&name, "main"), &ids).expect("an owner");
    let proving = if owning == 82 { 2 } else { 1 };
    let other = 3 - proving;
    let id = format!("{name}:main");
    let first = &nodes[0].1;
    first.create_ledger(&name).await.expect("create");
    insert_and_erase(first, &nodes[0].2, &id).await;
    let steps = first_job(first, &id).await.commits.len();

    let housekeeping = |i: usize| {
        let writer: Arc<dyn fluree_db_api::retention::RetentionWriter> = Arc::new(
            fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
                std::sync::OnceLock::from(Arc::clone(&nodes[i].2.committer)),
            )),
        );
        nodes[i].1.retention_housekeeping(
            Some(writer),
            fluree_db_api::retention::RetentionOptions {
                interval: Duration::from_millis(200),
                ..Default::default()
            },
        )
    };
    let leading = housekeeping(0);
    leading.tick().await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while provers[proving].begun() == 0 {
        assert!(std::time::Instant::now() < deadline, "the member to begin");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(provers[other].begun(), 0);

    // The leader goes quiet, and the member meant to lead next is elected.
    let next = if next_is_prover { proving } else { other };
    leader.raft.runtime_config().elect(false);
    leader.raft.runtime_config().heartbeat(false);
    nodes[3 - next].0.raft.runtime_config().elect(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let led = nodes[next].0.raft.metrics().borrow().current_leader;
        let deposed = leader.raft.metrics().borrow().current_leader != Some(81);
        if led == Some(ids[next]) && deposed {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "a new leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // As when a leader's tasks are aborted.
    drop(leading);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let before = asks[proving].load(SeqCst);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        asks[proving].load(SeqCst) <= before + 1,
        "the deposed leader still waits on its step"
    );

    let led = housekeeping(next);
    led.tick().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    for prover in &provers {
        prover.open();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let job = first_job(&nodes[next].1, &id).await;
        if job.commits.iter().all(|c| c.proved.is_some()) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{job:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let begun: usize = provers.iter().map(|p| p.begun()).sum();
    assert_eq!(begun, steps, "a step was proved twice");
    drop(led);
    for (_, _, node, _, _) in nodes {
        node.shutdown().await;
    }
}

/// A leader that loses its leadership while its purge pass has planned
/// stops the pass there, deleting nothing, and the next leader purges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_planned_by_a_deposed_leader_is_done_by_the_next() {
    purge_across_a_leader_change(fluree_db_api::retention::erasure::purge_hooks::Hold::Planned)
        .await;
}

/// [`a_purge_planned_by_a_deposed_leader_is_done_by_the_next`], deposed
/// once the pass erased the holders' indexes and recorded it: the next
/// leader goes on from that record rather than erasing them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_resumes_from_what_a_deposed_leader_recorded() {
    purge_across_a_leader_change(fluree_db_api::retention::erasure::purge_hooks::Hold::Rebuilt)
        .await;
}

async fn purge_across_a_leader_change(at: fluree_db_api::retention::erasure::purge_hooks::Hold) {
    use fluree_db_api::retention::erasure::purge_hooks::PurgeHold;
    use fluree_db_api::retention::erasure::ErasureStage;

    let data_dir = tempfile::tempdir().expect("data dir");
    let ids = [91u64, 92, 93];
    let provers = [
        GatedProver::closed(),
        GatedProver::closed(),
        GatedProver::closed(),
    ];
    let mut nodes = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        provers[i].open();
        nodes.push(
            start_node_with(
                *id,
                data_dir.path(),
                erasing_with(&provers[i]),
                |config| config,
                |app| app,
            )
            .await,
        );
    }
    let leader = &nodes[0].0;
    leader
        .raft
        .initialize(BTreeMap::from([(91, nodes[0].4.clone())]))
        .await
        .expect("initialize");
    eventually("self-election", async || {
        leader.raft.current_leader().await == Some(91)
    })
    .await;
    for i in [1, 2] {
        leader
            .raft
            .add_learner(ids[i], nodes[i].4.clone(), true)
            .await
            .expect("learner added");
    }
    leader
        .raft
        .change_membership(
            openraft::ChangeMembers::AddVoterIds(std::collections::BTreeSet::from([92, 93])),
            false,
        )
        .await
        .expect("voters");

    let name = format!("purge-{}/db", format!("{at:?}").to_lowercase());
    let id = format!("{name}:main");
    let first = &nodes[0].1;
    first.create_ledger(&name).await.expect("create");
    insert_and_erase(first, &nodes[0].2, &id).await;
    first.run_erasure_pass(&id).await.expect("proved");
    first
        .reindex(&id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("indexed");
    let published = first.run_erasure_pass(&id).await.expect("published");
    assert_eq!(published.published, 1, "{published:?}");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        first.run_erasure_pass(&id).await.expect("pass");
        let job = first_job(first, &id).await;
        if job
            .cluster
            .as_ref()
            .is_some_and(|c| c.effective_ms.is_some())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "every member to install: {job:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The request's own idempotency record names its envelope until it
    // goes, which a purge waits for.
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 1;
    leader
        .raft
        .client_write(
            fluree_db_consensus::raft::state_machine::Command::EvictIdempotency {
                cutoff_millis: now,
                marker_cutoff_millis: now,
            },
        )
        .await
        .expect("records evicted, as their time-to-live would");

    let record = leader
        .nameservice()
        .lookup(&id)
        .await
        .expect("lookup")
        .expect("registered");
    let originals: Vec<std::path::PathBuf> = first_job(first, &id)
        .await
        .commits
        .iter()
        .map(|task| {
            let cid: fluree_db_core::ContentId = task.commit.parse().expect("a commit id");
            let address = fluree_db_core::content_address(
                "file",
                fluree_db_core::ContentKind::Commit,
                &record.storage_namespace(),
                &cid.address_digest(),
            );
            data_dir
                .path()
                .join(address.trim_start_matches("fluree:file://"))
        })
        .collect();
    assert!(!originals.is_empty());
    assert!(originals.iter().all(|o| o.exists()), "{originals:?}");

    let housekeeping = |i: usize| {
        let writer: Arc<dyn fluree_db_api::retention::RetentionWriter> = Arc::new(
            fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
                std::sync::OnceLock::from(Arc::clone(&nodes[i].2.committer)),
            )),
        );
        nodes[i].1.retention_housekeeping(
            Some(writer),
            fluree_db_api::retention::RetentionOptions {
                interval: Duration::from_millis(200),
                purge_grace: Duration::ZERO,
                ..Default::default()
            },
        )
    };
    let hold = PurgeHold::set(at, &name);
    let leading = housekeeping(0);
    leading.tick().await;
    tokio::time::timeout(Duration::from_secs(60), hold.reached())
        .await
        .expect("the leader's background pass reaches the hold");
    let held = first_job(first, &id).await.purge;

    // The leader goes quiet and another member is elected.
    let next = 1;
    leader.raft.runtime_config().elect(false);
    leader.raft.runtime_config().heartbeat(false);
    nodes[2].0.raft.runtime_config().elect(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let led = nodes[next].0.raft.metrics().borrow().current_leader;
        let deposed = leader.raft.metrics().borrow().current_leader != Some(91);
        if led == Some(ids[next]) && deposed {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "a new leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Its housekeeping still runs, but the pass it began ended with its
    // leadership: let go now, it does not go on.
    hold.clear();
    hold.proceed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(hold.resumed(), 0, "the deposed leader's pass went on");
    assert!(originals.iter().all(|o| o.exists()), "{originals:?}");
    let job = first_job(&nodes[next].1, &id).await;
    assert!(
        job.purge.as_ref().is_none_or(|p| p.storage_ms.is_none()),
        "{job:?}"
    );
    drop(leading);

    // The next leader purges, from what the first recorded.
    let led = housekeeping(next);
    led.tick().await;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let job = loop {
        let job = first_job(&nodes[next].1, &id).await;
        if job.stage == ErasureStage::Purged {
            break job;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "not purged: {:?}",
            job.purge
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let purge = job.purge.expect("purge progress");
    assert!(purge.blocked.is_none(), "{purge:?}");
    assert!(originals.iter().all(|o| !o.exists()), "{originals:?}");
    if at == fluree_db_api::retention::erasure::purge_hooks::Hold::Rebuilt {
        let recorded = held.and_then(|p| p.rebuilt_ms);
        assert!(recorded.is_some(), "held before the rebuild was recorded");
        assert_eq!(purge.rebuilt_ms, recorded, "the indexes were erased again");
    }
    drop(led);
    for (_, _, node, _, _) in nodes {
        node.shutdown().await;
    }
}

/// An erasure reaches only the branches whose history holds its commits:
/// a branch made before the erased commit keeps its generation, and its
/// writes go on from the views they began with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erasure_reaches_only_the_branches_holding_its_commits() {
    use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
    use fluree_db_api::retention::FactSelector;
    use fluree_db_consensus::raft::state_machine::{generation, RefKey};

    let (integration, fluree, node, _dirs) = stand_up_with(
        71,
        |builder| {
            builder.with_erasure(ErasureConfig {
                prover: Arc::new(fluree_db_erasure::testing::NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
        },
        |config| config,
    )
    .await;
    let id = "reached/db:main";
    fluree.create_ledger("reached/db").await.expect("create");
    node.committer
        .transact(bulk_request_round(id, 1, 0))
        .await
        .expect("first transaction");
    fluree
        .create_branch("reached/db", "before", None, None)
        .await
        .expect("branch before");
    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::JsonLdInsert(serde_json::json!({
        "@id": "http://example.org/alice",
        "http://example.org/email": "a@secret"
    }));
    node.committer.transact(request).await.expect("insert");
    fluree
        .create_branch("reached/db", "after", None, None)
        .await
        .expect("branch after");

    let writer = fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
        std::sync::OnceLock::from(Arc::clone(&node.committer)),
    ));
    let erasure = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(serde_json::json!("a@secret")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences_with(&writer, id, erasure, None)
        .await
        .expect("accepted");
    fluree.run_erasure_pass(id).await.expect("proved");
    for branch in [id, "reached/db:after"] {
        fluree
            .reindex(branch, fluree_db_api::ReindexOptions::default())
            .await
            .expect("indexed");
    }
    let report = fluree.run_erasure_pass(id).await.expect("published");
    assert_eq!(report.published, 1, "{report:?}");
    {
        let state = integration.shared_state.read().await;
        assert_eq!(generation(&state, &RefKey::new("reached/db", "main")), 1);
        assert_eq!(generation(&state, &RefKey::new("reached/db", "after")), 1);
        assert_eq!(generation(&state, &RefKey::new("reached/db", "before")), 0);
    }
    node.shutdown().await;
}

/// A branch made before the erased commits that merged them since holds
/// them: the erasure reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erasure_reaches_a_branch_that_merged_its_commits() {
    use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
    use fluree_db_api::retention::FactSelector;
    use fluree_db_api::ConflictStrategy;
    use fluree_db_consensus::raft::state_machine::{generation, RefKey};
    use fluree_db_consensus::MergeRequest;

    let (integration, fluree, node, _dirs) = stand_up_with(
        72,
        |builder| {
            builder.with_erasure(ErasureConfig {
                prover: Arc::new(fluree_db_erasure::testing::NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
        },
        |config| config,
    )
    .await;
    let id = "merged/db:main";
    fluree.create_ledger("merged/db").await.expect("create");
    node.committer
        .transact(bulk_request_round(id, 1, 0))
        .await
        .expect("first transaction");
    fluree
        .create_branch("merged/db", "side", None, None)
        .await
        .expect("branch");
    node.committer
        .transact(bulk_request_round("merged/db:side", 1, 1))
        .await
        .expect("the side's own change");
    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::JsonLdInsert(serde_json::json!({
        "@id": "http://example.org/alice",
        "http://example.org/email": "a@secret"
    }));
    node.committer.transact(request).await.expect("insert");
    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::Sparql(
        "DELETE DATA { <http://example.org/alice> <http://example.org/email> \"a@secret\" }"
            .to_string(),
    );
    node.committer.transact(request).await.expect("retract");
    let merged = node
        .committer
        .merge(MergeRequest {
            idempotency_key: None,
            ledger_name: "merged/db".into(),
            source_branch: "main".into(),
            target_branch: Some("side".into()),
            strategy: ConflictStrategy::default(),
        })
        .await
        .expect("merged");
    assert!(!merged.fast_forward, "{merged:?}");

    let writer = fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
        std::sync::OnceLock::from(Arc::clone(&node.committer)),
    ));
    let erasure = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(serde_json::json!("a@secret")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences_with(&writer, id, erasure, None)
        .await
        .expect("accepted");
    fluree.run_erasure_pass(id).await.expect("proved");
    for branch in [id, "merged/db:side"] {
        fluree
            .reindex(branch, fluree_db_api::ReindexOptions::default())
            .await
            .expect("indexed");
    }
    let report = fluree.run_erasure_pass(id).await.expect("published");
    assert_eq!(report.published, 1, "{report:?}");
    {
        let state = integration.shared_state.read().await;
        assert_eq!(generation(&state, &RefKey::new("merged/db", "main")), 1);
        assert_eq!(generation(&state, &RefKey::new("merged/db", "side")), 1);
    }
    node.shutdown().await;
}

/// An erasure made on a cluster publishes through Raft: the leader
/// activates a manifest of it, the member installs the manifest and reports
/// it, reads of the erased commit return its replacement, writes resume on
/// the new generation, and the job records the activation and when every
/// member had installed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_erasure_publishes_through_an_activation_every_member_installs() {
    use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
    use fluree_db_api::retention::FactSelector;
    use fluree_db_consensus::raft::state_machine::{generation, RefKey};
    use fluree_db_core::commit::codec::replacement::is_replacement;
    use fluree_db_core::ContentStore;

    let (integration, fluree, node, dirs) = stand_up_with(
        16,
        |builder| {
            builder.with_erasure(ErasureConfig {
                prover: Arc::new(fluree_db_erasure::testing::NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
        },
        |config| config,
    )
    .await;
    let id = "erasing/db:main";
    // It proves erasures; it takes none by a transfer yet.
    assert!(fluree.can_prove_erasures());
    assert!(!fluree.can_hold_erasures());
    fluree.create_ledger("erasing/db").await.expect("create");
    let mut request = bulk_request(id, 1);
    request.body = TransactionBody::JsonLdInsert(serde_json::json!({
        "@id": "http://example.org/alice",
        "http://example.org/email": "a@secret"
    }));
    let asserted_t = node
        .committer
        .transact(request)
        .await
        .expect("insert")
        .commit
        .t;
    let ns = integration.nameservice();
    let record = ns.lookup(id).await.expect("lookup").expect("registered");
    let store = fluree.content_store(&record.storage_namespace());
    let asserted = fluree_db_core::collect_first_parent_cids(
        store.as_ref(),
        record.commit_head_id.as_ref().expect("head"),
        0,
    )
    .await
    .expect("chain")
    .into_iter()
    .find(|(t, _)| *t == asserted_t)
    .expect("asserting commit")
    .1;

    let writer = fluree_db_consensus::retention::CommitterRetentionWriter::new(Arc::new(
        std::sync::OnceLock::from(Arc::clone(&node.committer)),
    ));
    let erasure = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(serde_json::json!("a@secret")),
        }],
        ..Default::default()
    };
    // Refused while a member does not run this release.
    integration
        .raft
        .add_learner(
            18,
            fluree_db_consensus::raft::ClusterNode::new(
                "http://127.0.0.1:9/raft/nameservice",
                "http://127.0.0.1:9",
            ),
            false,
        )
        .await
        .expect("learner added");
    let refused = fluree
        .erase_occurrences_with(&writer, id, erasure.clone(), None)
        .await;
    assert!(refused.is_err(), "{refused:?}");
    integration
        .raft
        .change_membership(
            openraft::ChangeMembers::RemoveNodes(std::collections::BTreeSet::from([18])),
            false,
        )
        .await
        .expect("learner removed");
    fluree
        .erase_occurrences_with(&writer, id, erasure, None)
        .await
        .expect("accepted");
    fluree.run_erasure_pass(id).await.expect("proved");
    fluree
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .expect("indexed");
    let report = fluree.run_erasure_pass(id).await.expect("published");
    assert_eq!(report.published, 1, "{report:?}");

    let job = fluree
        .retention_status(id)
        .await
        .expect("status")
        .registration
        .expect("registered")
        .erasures
        .remove(0);
    let publication = job.cluster.expect("published through the cluster");
    assert_eq!(publication.activation, Some(0));
    {
        let state = integration.shared_state.read().await;
        assert_eq!(state.erasure.activations[&0].manifest, publication.manifest);
        assert_eq!(generation(&state, &RefKey::new("erasing/db", "main")), 1);
    }
    eventually("the member's readiness report", async || {
        let state = integration.shared_state.read().await;
        state
            .erasure
            .installed
            .get(&16)
            .is_some_and(|installed| installed.through == 1)
    })
    .await;
    assert!(is_replacement(&store.get(&asserted).await.expect("read")));
    // A branch made from the erased history takes its erasure, and the
    // source's index with it.
    fluree
        .create_branch("erasing/db", "after", None, None)
        .await
        .expect("branch");
    let after = ns
        .lookup("erasing/db:after")
        .await
        .expect("lookup")
        .expect("branch record");
    assert_eq!(after.erasure_generation, 1);
    assert!(
        after.index_head_id.is_some(),
        "the source's index was copied"
    );
    eventually("the job's effective state", async || {
        fluree.run_erasure_pass(id).await.expect("pass");
        fluree
            .retention_status(id)
            .await
            .expect("status")
            .registration
            .is_some_and(|r| {
                r.erasures[0]
                    .cluster
                    .as_ref()
                    .is_some_and(|c| c.effective_ms.is_some())
            })
    })
    .await;

    tokio::time::timeout(
        Duration::from_secs(30),
        node.committer.transact(bulk_request_round(id, 3, 1)),
    )
    .await
    .expect("a transaction after the activation lands in time")
    .expect("a transaction after the activation lands");

    // Then the leader purges the shared storage, the requests' garbage
    // collected through the cluster's intents, and leaves its members' own
    // copies to them.
    let original = dirs.1.path().join(
        fluree_db_core::content_address(
            "file",
            fluree_db_core::ContentKind::Commit,
            &record.storage_namespace(),
            &asserted.address_digest(),
        )
        .trim_start_matches("fluree:file://"),
    );
    assert!(original.exists(), "{}", original.display());
    let status = async || {
        fluree
            .retention_status(id)
            .await
            .expect("status")
            .registration
            .expect("registered")
            .erasures
            .remove(0)
    };
    // The erasure's own control commit was keyed: until its idempotency
    // record goes, its envelope is named, and the purge waits.
    let waiting = fluree
        .run_purge_pass(id, Duration::ZERO)
        .await
        .expect("purge pass");
    assert!(waiting.waiting, "{waiting:?}");
    assert!(status()
        .await
        .purge
        .is_some_and(|p| p.envelopes_ms.is_none()));
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 1;
    integration
        .raft
        .client_write(
            fluree_db_consensus::raft::state_machine::Command::EvictIdempotency {
                cutoff_millis: now,
                marker_cutoff_millis: now,
            },
        )
        .await
        .expect("records evicted, as their time-to-live would");
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let job = loop {
        let report = fluree
            .run_purge_pass(id, Duration::ZERO)
            .await
            .expect("purge pass");
        assert!(report.blocked.is_none(), "{report:?}");
        let job = status().await;
        if job.purge.as_ref().is_some_and(|p| p.storage_ms.is_some()) {
            break job;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the shared storage was not purged: {report:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        job.stage,
        fluree_db_api::retention::erasure::ErasureStage::Purging
    );
    assert!(!original.exists(), "the original is deleted");

    // A member stopped with a write still in its log, of a request's
    // envelope holding the erased value, collected since: a replay brings it
    // back, and it is collected again before the job completes.
    use fluree_db_core::StorageWrite as _;
    let envelope_bytes = br#"{"resurrected":"a@secret"}"#;
    let envelope = fluree_db_core::content_address(
        "file",
        fluree_db_core::ContentKind::Txn,
        &record.storage_namespace(),
        &fluree_db_core::ContentId::new(fluree_db_core::ContentKind::Txn, envelope_bytes)
            .address_digest(),
    );
    let envelope_file = dirs
        .1
        .path()
        .join(envelope.trim_start_matches("fluree:file://"));
    let stopped = fluree_db_core::FileStorage::new(dirs.1.path())
        .with_durability(fluree_db_core::Durability::Wal)
        .with_wal_owner("node-99");
    stopped.recover_wal().unwrap();
    stopped.hold_wal_segments_for_test().unwrap();
    stopped
        .write_bytes(&envelope, envelope_bytes)
        .await
        .unwrap();
    stopped.simulate_crash_for_test();
    drop(stopped);
    std::fs::remove_file(&envelope_file).unwrap();

    // Its members clean their own copies, then it is purged: a member that
    // has not reported holds it, named.
    integration
        .raft
        .add_learner(
            17,
            fluree_db_consensus::raft::ClusterNode::new(
                "http://127.0.0.1:9/raft/nameservice",
                "http://127.0.0.1:9",
            ),
            false,
        )
        .await
        .expect("learner added");
    let held = fluree
        .run_purge_pass(id, Duration::ZERO)
        .await
        .expect("purge pass");
    assert!(held.waiting, "{held:?}");
    let blocked = status().await.purge.and_then(|p| p.blocked);
    assert!(
        blocked.as_deref().is_some_and(|why| why.contains("17")),
        "{blocked:?}"
    );
    integration
        .raft
        .change_membership(
            openraft::ChangeMembers::RemoveNodes(std::collections::BTreeSet::from([17])),
            false,
        )
        .await
        .expect("learner removed");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let job = loop {
        fluree
            .run_purge_pass(id, Duration::ZERO)
            .await
            .expect("purge pass");
        let job = status().await;
        if job.stage == fluree_db_api::retention::erasure::ErasureStage::Purged {
            break job;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "not purged: {:?}",
            job.purge
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert!(job
        .purge
        .is_some_and(|p| p.purged_ms.is_some() && p.blocked.is_none()));
    assert!(
        !envelope_file.exists(),
        "the envelope a replay brought back"
    );
    assert!(integration
        .shared_state
        .read()
        .await
        .erasure_cleanup
        .cleaned
        .contains_key(&16));
    assert!(!original.exists());
    node.shutdown().await;
}

/// Seed `ledger_id` with one commit through the cluster.
async fn seed(node: &EmbeddedRaftNode, ledger_id: &str) {
    node.committer
        .transact(TransactionRequest {
            idempotency_key: None,
            ledger_id: ledger_id.into(),
            body: TransactionBody::JsonLdInsert(serde_json::json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:base", "ex:name": "base"
            })),
            txn_opts: Default::default(),
            commit_opts: Default::default(),
            tracking: None,
            governance: Default::default(),
        })
        .await
        .expect("seed");
}

/// Condemn an intent naming `addresses`, its object written now.
async fn condemn(
    integration: &RaftIntegration,
    fluree: &fluree_db_api::Fluree,
    addresses: &[String],
) -> String {
    use fluree_db_api::collect::IntentEntry;
    use fluree_db_consensus::raft::state_machine::{CollectCommand, Command};
    let entries = addresses
        .iter()
        .map(|address| IntentEntry {
            address: address.clone(),
            reason: "test".into(),
            ledger: None,
            record: None,
        })
        .collect();
    let hash = fluree.put_intent(entries).await.expect("intent stored");
    let membership = integration.shared_state.read().await.membership_index;
    for command in [
        CollectCommand::Enable { membership },
        CollectCommand::Condemn {
            intent: hash.clone(),
        },
    ] {
        integration
            .raft
            .client_write(Command::Collect(command))
            .await
            .expect("condemned");
    }
    hash
}

/// A submission storing an address an intent condemned spares it, so the
/// intent's deletion leaves it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_storing_a_condemned_address_spares_it() {
    use fluree_db_api::ConflictStrategy;
    use fluree_db_consensus::{QueuedRebase, QueuedRequest, RebaseRequest};
    use fluree_db_core::{content_address, ContentId, ContentKind};

    let (integration, fluree, node, _dirs) = stand_up(31, |config| config).await;
    fluree.create_ledger("spare").await.expect("create ledger");
    seed(&node, "spare:main").await;
    fluree
        .create_branch("spare", "dev", None, None)
        .await
        .expect("create branch");

    // A rebase's envelope holds only its strategy, so its address is known.
    let strategy = ConflictStrategy::default();
    let bytes = QueuedRequest::Rebase(QueuedRebase { strategy })
        .to_bytes()
        .unwrap();
    let namespace = fluree.storage_namespace("spare:dev").await.unwrap();
    let address = content_address(
        "file",
        ContentKind::Txn,
        &namespace,
        &ContentId::new(ContentKind::Txn, &bytes).address_digest(),
    );
    condemn(&integration, &fluree, std::slice::from_ref(&address)).await;
    let condemned = fluree.condemned().expect("managed storage");
    eventually("the intent loads", async || condemned.loaded() == 1).await;

    node.committer
        .rebase(RebaseRequest {
            idempotency_key: None,
            ledger_name: "spare".into(),
            branch: "dev".into(),
            strategy,
        })
        .await
        .expect("rebase");
    let spared = integration.shared_state.read().await.collect.intents[&0]
        .spared
        .clone();
    assert!(spared.contains(&address), "{spared:?}");
    node.shutdown().await;
}

/// A write that stored an address before its node loaded the intent
/// condemning it, refused a spare because that intent's deletion has
/// begun, is refused when it publishes; one begun after loading the intent
/// publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_begun_before_a_deleting_intent_is_refused() {
    use fluree_db_api::collect::{IntentEntry, IntentObject};
    use fluree_db_consensus::raft::state_machine::{CollectCommand, Command};
    use fluree_db_nameservice::{ConfigLookup, ConfigPublisher, ConfigValue, NameServiceError};

    let (integration, fluree, node, _dirs) = stand_up(32, |config| config).await;
    fluree.create_ledger("fenced").await.expect("create ledger");
    let ns = integration.nameservice();
    let push = async || {
        let fence = ns.lookup("fenced:main").await.unwrap().unwrap().fence;
        let current = ns.get_config("fenced:main").await.unwrap();
        let v = current.as_ref().map_or(0, |c| c.v);
        ns.push_config_fenced(
            "fenced:main",
            fence,
            current.as_ref(),
            &ConfigValue::new(v + 1, None),
        )
        .await
    };
    let condemned = fluree.condemned().expect("managed storage");
    let storage = fluree.admin_storage().unwrap();
    let address = "fluree:file://fenced/main/txn/stray.json".to_string();
    let intent = IntentObject {
        nonce: String::new(),
        entries: vec![IntentEntry {
            address: address.clone(),
            reason: "test".into(),
            ledger: None,
            record: None,
        }],
    };
    let bytes = serde_json::to_vec(&intent).unwrap();
    let hash = fluree_db_core::sha256_hex(&bytes);

    let membership = integration.shared_state.read().await.membership_index;
    let refused = condemned
        .scoped(async {
            storage.write_bytes(&address, b"stray").await.unwrap();
            // Deleting before this node can load the intent: its object is
            // written only after.
            for command in [
                CollectCommand::Enable { membership },
                CollectCommand::Condemn {
                    intent: hash.clone(),
                },
                CollectCommand::Delete { number: 0 },
            ] {
                integration
                    .raft
                    .client_write(Command::Collect(command))
                    .await
                    .unwrap();
            }
            storage
                .write_bytes(
                    &fluree.intent_address(storage.storage_method(), &hash),
                    &bytes,
                )
                .await
                .unwrap();
            eventually("the intent loads", async || condemned.loaded() == 1).await;
            push().await
        })
        .await;
    assert!(
        matches!(refused, Err(NameServiceError::ApplyLagged(_))),
        "{refused:?}"
    );
    condemned.scoped(push()).await.expect("begun after loading");
    push().await.expect("outside any scope");
    node.shutdown().await;
}

/// Enable the cluster's collection, as its first erasure request does.
async fn enable_collection(integration: &RaftIntegration) {
    use fluree_db_api::collect::Collector;
    assert!(integration.nameservice().enable().await.unwrap());
}

/// The leader collects blobs nothing references through an intent: the
/// pass that finds them condemns them, the next deletes them and closes the
/// intent, and writes carry on. The seed's envelope, released when it
/// applied, is among them: no node deletes it at apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_collects_unreferenced_blobs_through_an_intent() {
    use fluree_db_core::{content_address, ContentKind, ContentStore as _};

    let (integration, fluree, node, _dirs) = stand_up(33, |config| config).await;
    enable_collection(&integration).await;
    fluree.create_ledger("swept").await.expect("create ledger");
    seed(&node, "swept:main").await;
    let namespace = fluree.storage_namespace("swept:main").await.unwrap();
    let stray = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"stray\":true}")
        .await
        .unwrap();
    let address = content_address(
        "file",
        ContentKind::Txn,
        &namespace,
        &stray.address_digest(),
    );
    let storage = fluree.admin_storage().unwrap();
    fluree.reconcile_garbage("swept").await.unwrap().unwrap();

    let first = fluree
        .collect_garbage_settled(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(first.condemned, 2, "{first:?}");
    assert!(
        storage.exists(&address).await.unwrap(),
        "condemned, not deleted"
    );
    let second = fluree
        .collect_garbage_settled(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(second.deleted, 2, "{second:?}");
    assert!(!storage.exists(&address).await.unwrap());
    {
        let state = integration.shared_state.read().await;
        assert!(state.collect.intents.is_empty(), "closed");
        assert_eq!(state.collect.deleted_through, 1);
    }
    let condemned = fluree.condemned().unwrap();
    eventually("the close is loaded", async || condemned.open().is_empty()).await;
    seed(&node, "swept:main").await;
    node.shutdown().await;
}

/// The cluster collects only once something enables collection (an
/// erasure request), and only once every member checks its writes: a member
/// a release behind could not decode collection's commands. A garbage pass
/// enables nothing, and until a learner answers that it checks, enabling is
/// refused and the leader condemns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collection_waits_until_every_member_checks_its_writes() {
    use fluree_db_api::collect::Collector;
    use fluree_db_consensus::raft::nameservice::COLLECT_READY_PATH;
    use fluree_db_core::{ContentKind, ContentStore as _};
    use std::sync::atomic::{AtomicBool, Ordering};

    let (integration, fluree, node, _dirs) = stand_up(35, |config| config).await;
    fluree.create_ledger("gated").await.expect("create ledger");
    seed(&node, "gated:main").await;
    let namespace = fluree.storage_namespace("gated:main").await.unwrap();
    fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"stray\":true}")
        .await
        .unwrap();
    // Nothing is recorded as garbage before the cluster collects.
    assert!(fluree.reconcile_garbage("gated").await.unwrap().is_none());

    // A learner that does not check its writes until told to.
    let checks = Arc::new(AtomicBool::new(false));
    let app = axum::Router::new().route(
        &format!("/raft/nameservice{COLLECT_READY_PATH}"),
        axum::routing::post({
            let checks = Arc::clone(&checks);
            move || async move {
                if checks.load(Ordering::SeqCst) {
                    // Slowly: a membership change can land meanwhile.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Ok(postcard::to_allocvec(&36u64).unwrap())
                } else {
                    Err(axum::http::StatusCode::NOT_FOUND)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    integration
        .raft
        .add_learner(
            36,
            fluree_db_consensus::raft::ClusterNode::new(
                format!("http://127.0.0.1:{port}/raft/nameservice"),
                format!("http://127.0.0.1:{port}"),
            ),
            false,
        )
        .await
        .expect("learner added");

    let ns = integration.nameservice();
    assert!(!ns.enable().await.unwrap());
    let waiting = fluree
        .collect_garbage_settled(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(waiting.condemned, 0, "{waiting:?}");
    assert!(!integration.shared_state.read().await.collect.enabled);

    // A member of an earlier release joining while the others are asked
    // was never asked: collection is not enabled.
    checks.store(true, Ordering::SeqCst);
    let joins = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        integration
            .raft
            .add_learner(
                37,
                fluree_db_consensus::raft::ClusterNode::new(
                    "http://127.0.0.1:9/raft/nameservice",
                    "http://127.0.0.1:9",
                ),
                false,
            )
            .await
            .expect("learner added");
    };
    let (raced, ()) = tokio::join!(ns.enable(), joins);
    assert!(!raced.unwrap());
    assert!(!integration.shared_state.read().await.collect.enabled);
    integration
        .raft
        .change_membership(
            openraft::ChangeMembers::RemoveNodes(std::collections::BTreeSet::from([37])),
            false,
        )
        .await
        .expect("learner removed");

    // Every member checks now, but a garbage pass alone does not enable.
    let idle = fluree
        .collect_garbage_settled(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(idle.condemned, 0, "{idle:?}");
    assert!(!integration.shared_state.read().await.collect.enabled);

    assert!(ns.enable().await.unwrap());
    fluree.reconcile_garbage("gated").await.unwrap().unwrap();
    let enabled = fluree
        .collect_garbage_settled(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert!(enabled.condemned > 0, "{enabled:?}");
    assert!(integration.shared_state.read().await.collect.enabled);

    // From now on a commit head set outside a write scope is refused: what
    // it names was not checked. A drop of a named graph, which stores its
    // commit and sets the head in one scope, lands.
    use fluree_db_nameservice::{NameServiceError, RefKind, RefPublisher, RefValue};
    let ns = integration.nameservice();
    let record = ns.lookup("gated:main").await.unwrap().unwrap();
    let unscoped = ns
        .compare_and_set_ref_fenced(
            "gated:main",
            record.write_basis(),
            RefKind::CommitHead,
            None,
            &RefValue {
                id: record.commit_head_id.clone(),
                t: record.commit_t + 1,
            },
        )
        .await;
    assert!(
        matches!(unscoped, Err(NameServiceError::ApplyRejected(_))),
        "{unscoped:?}"
    );
    node.committer
        .transact(TransactionRequest {
            idempotency_key: None,
            ledger_id: "gated:main".into(),
            body: TransactionBody::TrigUpsert(
                "GRAPH <http://example.org/graphs/alpha> { \
                 <http://example.org/a> <http://example.org/p> \"v\" . }"
                    .into(),
            ),
            txn_opts: Default::default(),
            commit_opts: Default::default(),
            tracking: None,
            governance: Default::default(),
        })
        .await
        .expect("named graph written");
    fluree
        .drop_named_graph("gated:main", "http://example.org/graphs/alpha")
        .await
        .expect("a scoped drop lands");
    // As does a restore, which stores the archive's commits and sets the
    // heads naming them.
    let mut archive = Vec::new();
    fluree
        .archive_ledger("gated", false, &mut archive)
        .await
        .expect("archived");
    fluree
        .restore_ledger("restored", &mut archive.as_slice())
        .await
        .expect("a scoped restore lands");
    node.shutdown().await;
}

/// Identical requests share one envelope address: a merge's envelope holds
/// only its branches and strategy. A second submission stored its envelope
/// before the first applied, and is enqueued after: the first's release
/// must not take the envelope from it, or the second fails reading it.
/// (Enqueued directly here, keyed so its outcome is kept: two identical
/// submissions through one committer also share a waiter, a separate
/// matter.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identical_request_keeps_the_envelope_the_first_released() {
    use fluree_db_api::ConflictStrategy;
    use fluree_db_consensus::raft::state_machine::{
        ApplyOutcome, BodyKind, Command, QueueSubmission,
    };
    use fluree_db_consensus::{
        IdempotencyCacheKey, IdempotencyKey, MergeRequest, QueuedMerge, QueuedRequest,
    };
    use fluree_db_core::{content_address, ContentId, ContentKind};

    let (integration, fluree, node, _dirs) = stand_up(34, |config| config).await;
    enable_collection(&integration).await;
    fluree.create_ledger("twice").await.expect("create ledger");
    seed(&node, "twice:main").await;
    fluree
        .create_branch("twice", "dev", None, None)
        .await
        .expect("create branch");
    seed(&node, "twice:dev").await;

    let strategy = ConflictStrategy::default();
    let envelope = QueuedRequest::Merge(QueuedMerge {
        source_branch: "dev".into(),
        target_branch: Some("main".into()),
        strategy,
    });
    let request_cid = ContentId::new(ContentKind::Txn, &envelope.to_bytes().unwrap());
    let body_cid = ContentId::new(ContentKind::Txn, &envelope.canonical_body_bytes().unwrap());
    let namespace = fluree.storage_namespace("twice:main").await.unwrap();
    let address = content_address(
        "file",
        ContentKind::Txn,
        &namespace,
        &request_cid.address_digest(),
    );
    let storage = fluree.admin_storage().unwrap();

    node.committer
        .merge(MergeRequest {
            idempotency_key: None,
            ledger_name: "twice".into(),
            source_branch: "dev".into(),
            target_branch: Some("main".into()),
            strategy,
        })
        .await
        .expect("the first lands");
    // Given the release time to land, if it deletes.
    for _ in 0..50 {
        if !storage.exists(&address).await.unwrap() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let key = IdempotencyCacheKey::new("twice:main", IdempotencyKey::new("second").unwrap());
    // As a writer presents it while the cluster collects: having seen no
    // intent.
    integration
        .raft
        .client_write(Command::Seen {
            seen: 0,
            command: Box::new(Command::EnqueueCommand(QueueSubmission {
                ledger_id: "twice".into(),
                branch: "main".into(),
                idempotency: Some(key.clone()),
                request_cid,
                body_cid,
                body_kind: BodyKind::Merge,
                applied_at_millis: 0,
            })),
        })
        .await
        .unwrap();
    eventually("the second's outcome", async || {
        integration
            .shared_state
            .read()
            .await
            .idempotency
            .contains_key(&key)
    })
    .await;
    let outcome = integration.shared_state.read().await.idempotency[&key].clone();
    assert!(matches!(outcome, ApplyOutcome::Applied(_)), "{outcome:?}");
    node.shutdown().await;
}

static WORKER_SPAWNS: std::sync::Mutex<BTreeMap<String, usize>> =
    std::sync::Mutex::new(BTreeMap::new());

/// Count the supervisor's `commit worker spawned` events per ledger, for
/// every test in this binary from the first call on.
fn count_worker_spawns() {
    static COUNTING: std::sync::Once = std::sync::Once::new();
    COUNTING.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Layer;
        let commit_worker = tracing_subscriber::filter::filter_fn(|meta| {
            meta.target() == "fluree_db_consensus::raft::commit_worker"
        });
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(SpawnCounter.with_filter(commit_worker)),
        )
        .expect("the only global subscriber");
    });
}

fn worker_spawns(ledger: &str) -> usize {
    WORKER_SPAWNS
        .lock()
        .unwrap()
        .get(ledger)
        .copied()
        .unwrap_or(0)
}

struct SpawnCounter;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpawnCounter {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        #[derive(Default)]
        struct Spawn {
            spawned: bool,
            ledger: Option<String>,
        }
        impl tracing::field::Visit for Spawn {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "message" => self.spawned = format!("{value:?}") == "commit worker spawned",
                    "ledger_id" => self.ledger = Some(format!("{value:?}")),
                    _ => {}
                }
            }
        }
        let mut spawn = Spawn::default();
        event.record(&mut spawn);
        if let (true, Some(ledger)) = (spawn.spawned, spawn.ledger) {
            *WORKER_SPAWNS.lock().unwrap().entry(ledger).or_default() += 1;
        }
    }
}

/// A branch whose queue each write drains keeps its worker: applying the
/// `ApplyHead` that pops the last entry removes the queue while the
/// worker still awaits that propose's response, and tearing the worker
/// down then would drop the response and skip installing the commit.
/// Writes spaced past the supervisor's poll each drain the queue, and
/// one worker stages them all. Kept, it idles until the supervisor wakes
/// it: left asleep, a write would wait out the worker's idle exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_keeps_its_worker_across_drained_queues() {
    count_worker_spawns();
    let (_integration, fluree, node, _dirs) = stand_up(62, |config| config).await;
    fluree
        .create_ledger("drained/db")
        .await
        .expect("create ledger");
    for round in 0..4 {
        let write = node
            .committer
            .transact(bulk_request_round("drained/db:main", 1, round));
        tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("a kept worker is woken for its branch's next write")
            .expect("transact through consensus");
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    assert_eq!(worker_spawns("drained/db"), 1);
}
