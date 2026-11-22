//! Writes one of the v4 Raft fixtures beside this file. It is not built
//! here: it runs as a test inside an exported v4 source tree.
//!
//! ```text
//! git archive v4.2.9 | tar -x -C /tmp/v4.2.9
//! cp generate.rs /tmp/v4.2.9/fluree-db-consensus/tests/gen_raft_v4_fixture.rs
//! cd /tmp/v4.2.9 && CARGO_TARGET_DIR=/tmp/target-v4.2.9 FIXTURE_OUT=/tmp/out \
//!   cargo test -p fluree-db-consensus --features raft --test gen_raft_v4_fixture -- --nocapture
//! ```
//!
//! The node is a single voter, so it reopens without its peers. What it
//! leaves behind, in `raft/` (the Raft log and snapshot) and `data/` (the
//! ledgers' storage):
//!
//! - ledger `fixture/db`: Alice under key `fixture-alice` before the
//!   snapshot, Bob under key `fixture-bob` after it, so the log has a tail
//!   to replay over the snapshot;
//! - a submission under key `fixture-malformed` whose body fails to stage:
//!   a cached failure;
//! - ledger `fixture/unborn`, created with no commit, and a no-op update to
//!   it under key `fixture-noop`, which v4 poisons as `WorkerPanic`.
//!
//! With `FIXTURE_PURGE=1` the log is also purged through the snapshot
//! before Bob's write, as openraft does on a long-running node: a purge
//! marker, and no entries at or below it.
//!
//! `expected.json` records the heads and outcomes a reader checks.

#![cfg(feature = "raft")]

use fluree_db_api::{FlureeBuilder, NameServiceMode};
use fluree_db_consensus::raft::embedded::{EmbeddedRaftConfig, EmbeddedRaftNode};
use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_consensus::{IdempotencyKey, TransactionBody, TransactionRequest};
use fluree_db_nameservice::{NameServiceLookup, NameServicePublisher};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

const NODE: u64 = 1;

async fn eventually(what: &str, check: impl AsyncFn() -> bool) {
    for _ in 0..1000 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn request(key: &str, ledger_id: &str, body: TransactionBody) -> TransactionRequest {
    TransactionRequest {
        idempotency_key: Some(IdempotencyKey::new(key).expect("key")),
        ledger_id: ledger_id.into(),
        body,
        txn_opts: Default::default(),
        commit_opts: Default::default(),
        tracking: None,
        governance: Default::default(),
    }
}

fn insert(name: &str) -> TransactionBody {
    TransactionBody::JsonLdInsert(serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "@id": format!("ex:{}", name.to_lowercase()),
        "ex:name": name
    }))
}

#[tokio::test(flavor = "multi_thread")]
async fn generate() {
    let out = std::path::PathBuf::from(std::env::var("FIXTURE_OUT").expect("FIXTURE_OUT"));
    let raft_dir = out.join("raft");
    let data_dir = out.join("data");
    for dir in [&raft_dir, &data_dir] {
        assert!(!dir.exists(), "{} already exists", dir.display());
        std::fs::create_dir_all(dir).expect("create dir");
    }

    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(NODE, &raft_dir))
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
    let fluree = Arc::new(
        FlureeBuilder::file(data_dir.to_string_lossy().as_ref())
            .with_event_bus(Arc::clone(&integration.event_bus))
            .build_client_with_nameservice(NameServiceMode::ReadWrite(Arc::clone(&ns)))
            .await
            .expect("engine builds"),
    );
    let node = EmbeddedRaftNode::attach(
        Arc::clone(&integration),
        Arc::clone(&fluree),
        EmbeddedRaftConfig::for_engine(&fluree),
    )
    .await;
    integration
        .raft
        .initialize(BTreeMap::from([(
            NODE,
            fluree_db_consensus::raft::ClusterNode::new(
                format!("http://127.0.0.1:{port}/raft/nameservice"),
                format!("http://127.0.0.1:{port}"),
            ),
        )]))
        .await
        .expect("initialize");
    eventually("self-election", async || {
        integration.raft.current_leader().await == Some(NODE)
    })
    .await;

    fluree.create_ledger("fixture/db").await.expect("create db");
    fluree
        .create_ledger("fixture/unborn")
        .await
        .expect("create unborn");

    let alice = node
        .committer
        .transact(request("fixture-alice", "fixture/db:main", insert("Alice")))
        .await
        .expect("alice");
    let malformed = node
        .committer
        .transact(request(
            "fixture-malformed",
            "fixture/db:main",
            TransactionBody::JsonLdInsert(serde_json::json!(42)),
        ))
        .await
        .expect_err("a body that cannot stage");
    let noop = node
        .committer
        .transact(request(
            "fixture-noop",
            "fixture/unborn:main",
            TransactionBody::JsonLdUpdate(serde_json::json!({
                "where": {"@id": "?s", "http://example.org/p": "?o"},
                "delete": {"@id": "?s", "http://example.org/p": "?o"}
            })),
        ))
        .await;

    integration.raft.trigger().snapshot().await.expect("snapshot");
    eventually("the snapshot", async || {
        integration.raft.metrics().borrow().snapshot.is_some()
    })
    .await;
    let snapshot = integration.raft.metrics().borrow().snapshot;
    let purge = std::env::var("FIXTURE_PURGE").is_ok_and(|v| v == "1");
    if purge {
        let through = snapshot.expect("a snapshot").index;
        integration
            .raft
            .trigger()
            .purge_log(through)
            .await
            .expect("purge");
        eventually("the purge", async || {
            integration
                .raft
                .metrics()
                .borrow()
                .purged
                .is_some_and(|id| id.index >= through)
        })
        .await;
    }
    let purged = integration.raft.metrics().borrow().purged;

    let bob = node
        .committer
        .transact(request("fixture-bob", "fixture/db:main", insert("Bob")))
        .await
        .expect("bob");
    let last_applied = integration.raft.metrics().borrow().last_applied;
    let record = ns
        .lookup("fixture/db:main")
        .await
        .expect("lookup")
        .expect("registered");
    assert_eq!(record.commit_t, bob.commit.t);

    node.shutdown().await;

    let expected = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "node_id": NODE,
        "db": {
            "ledger_id": "fixture/db:main",
            "alice_t": alice.commit.t,
            "head_t": bob.commit.t,
            "names": ["Alice", "Bob"],
        },
        "unborn_ledger_id": "fixture/unborn:main",
        "malformed_error": malformed.to_string(),
        "noop_outcome": format!("{noop:?}"),
        "snapshot_index": snapshot.map(|id| id.index),
        "purged_index": purged.map(|id| id.index),
        "last_applied_index": last_applied.map(|id| id.index),
    });
    std::fs::write(
        out.join("expected.json"),
        serde_json::to_string_pretty(&expected).expect("json"),
    )
    .expect("write expected.json");
    println!("{}", serde_json::to_string_pretty(&expected).unwrap());
}
