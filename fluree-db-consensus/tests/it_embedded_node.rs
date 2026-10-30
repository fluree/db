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
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let data_dir = tempfile::tempdir().expect("data dir");
    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(node_id, raft_dir.path()))
            .await
            .expect("bootstrap"),
    );
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
    let ns: Arc<dyn NameServicePublisher> = integration.nameservice();
    let builder = FlureeBuilder::file(data_dir.path().to_string_lossy().as_ref())
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
    let mut members = BTreeMap::new();
    members.insert(
        node_id,
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
        integration.raft.current_leader().await == Some(node_id)
    })
    .await;
    (integration, fluree, node, (raft_dir, data_dir))
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

    let (integration, fluree, node, _dirs) = stand_up_with(
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
    // Refused, as long as a cluster cannot purge what it publishes.
    let refused = fluree
        .erase_occurrences_with(&writer, id, erasure.clone(), None)
        .await;
    assert!(refused.is_err(), "{refused:?}");
    fluree
        .erase_occurrences_in_cluster(&writer, id, erasure, None)
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
    let purge = fluree.run_purge_pass(id, Duration::ZERO).await;
    assert!(purge.is_err(), "a cluster purged: {purge:?}");
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
    node.shutdown().await;
}
