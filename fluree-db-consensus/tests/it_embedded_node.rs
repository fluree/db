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

    // 3. Engine half: one bus, one nameservice, shared with the adapter.
    let ns: Arc<dyn NameServicePublisher> = integration.nameservice();
    let fluree = Arc::new(
        FlureeBuilder::file(data_dir.path().to_string_lossy().as_ref())
            .with_event_bus(Arc::clone(&integration.event_bus))
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
    eventually(
        "the ledger cache to reach the replicated head",
        async || manager.current_t("embedded/db:main").await == Some(receipt.commit.t),
    )
    .await;

    // 9. Teardown in the order the node owns.
    node.shutdown().await;
}
