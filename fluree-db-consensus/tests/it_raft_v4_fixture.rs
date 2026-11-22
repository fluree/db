//! A node of this release reopens the Raft log, snapshot and storage a v4
//! node left behind.
//!
//! The fixtures under `tests/fixtures/raft-v4/` were written by v4 binaries
//! (`generate.rs` there says how): v4.2.0, the last release whose log kept
//! one file per entry, and v4.2.9, the last v4 release, whose log is in
//! segments. Each comes as a log never purged and, under `-purged`, one
//! purged through its snapshot, as a long-running node's is. Each is a
//! single voter, so it reopens without its peers.
//! Everything else in this crate writes and reads with this release's code,
//! so this is the test that notices a change to the replicated encoding
//! (commands, snapshot state, idempotency outcomes) that v4 data cannot
//! decode under.

#![cfg(feature = "raft")]

use fluree_db_api::{FlureeBuilder, NameServiceMode};
use fluree_db_consensus::raft::embedded::{EmbeddedRaftConfig, EmbeddedRaftNode};
use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_consensus::{IdempotencyKey, TransactionBody, TransactionRequest};
use fluree_db_nameservice::{NameServiceLookup, NameServicePublisher};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..1000 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read fixture dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
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
    TransactionBody::JsonLdInsert(json!({
        "@context": {"ex": "http://example.org/"},
        "@id": format!("ex:{}", name.to_lowercase()),
        "ex:name": name
    }))
}

/// A node of this release on `raft_dir` and `data_dir`, leading the
/// single-voter cluster the fixture's node formed.
async fn reopen(
    node_id: u64,
    raft_dir: &Path,
    data_dir: &Path,
) -> (
    Arc<RaftIntegration>,
    Arc<fluree_db_api::Fluree>,
    EmbeddedRaftNode,
) {
    let integration = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(node_id, raft_dir))
            .await
            .expect("a v4 Raft directory opens"),
    );
    let ns: Arc<dyn NameServicePublisher> = integration.nameservice();
    let fluree = Arc::new(
        FlureeBuilder::file(data_dir.to_string_lossy().as_ref())
            .with_event_bus(Arc::clone(&integration.event_bus))
            .with_storage_wal_owner(format!("node-{node_id}"))
            .build_client_with_nameservice(NameServiceMode::ReadWrite(ns))
            .await
            .expect("engine builds"),
    );
    let node = EmbeddedRaftNode::attach(
        Arc::clone(&integration),
        Arc::clone(&fluree),
        EmbeddedRaftConfig::for_engine(&fluree),
    )
    .await;
    eventually("the reopened node to lead", async || {
        integration.raft.current_leader().await == Some(node_id)
    })
    .await;
    (integration, fluree, node)
}

async fn names(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Option<Vec<String>> {
    let rows = fluree
        .query_from()
        .jsonld(&json!({
            "@context": {"ex": "http://example.org/"},
            "from": ledger_id,
            "select": "?name",
            "where": {"@id": "?s", "ex:name": "?name"}
        }))
        .execute_formatted()
        .await
        .ok()?;
    let mut names: Vec<String> = rows
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    names.sort();
    Some(names)
}

async fn reopens(version: &str) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/raft-v4")
        .join(version);
    let expected: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("expected.json")).expect("expected.json"),
    )
    .expect("expected.json parses");
    let release = version.trim_start_matches('v');
    assert_eq!(
        expected["version"],
        release.strip_suffix("-purged").unwrap_or(release)
    );
    let node_id = expected["node_id"].as_u64().expect("node_id");
    let db = expected["db"]["ledger_id"].as_str().expect("db ledger id");
    let unborn = expected["unborn_ledger_id"].as_str().expect("unborn id");
    let alice_t = expected["db"]["alice_t"].as_i64().expect("alice_t");
    let head_t = expected["db"]["head_t"].as_i64().expect("head_t");

    let dir = tempfile::tempdir().expect("tempdir");
    let (raft_dir, data_dir) = (dir.path().join("raft"), dir.path().join("data"));
    copy_dir(&fixture.join("raft"), &raft_dir);
    copy_dir(&fixture.join("data"), &data_dir);

    let (integration, fluree, node) = reopen(node_id, &raft_dir, &data_dir).await;

    // The snapshot restored, and the log tail past it replayed.
    let metrics = integration.raft.metrics().borrow().clone();
    assert_eq!(
        metrics.snapshot.map(|id| id.index),
        expected["snapshot_index"].as_u64(),
        "{version}: the v4 snapshot is the node's snapshot"
    );
    assert_eq!(
        metrics.purged.map(|id| id.index),
        expected["purged_index"].as_u64(),
        "{version}: the v4 purge marker"
    );
    assert!(
        metrics.last_applied.map(|id| id.index) >= expected["last_applied_index"].as_u64(),
        "{version}: the log tail replayed: {:?}",
        metrics.last_applied
    );
    // The new leader binds the ledgers v4 registered before name bindings.
    let ns = integration.nameservice();
    eventually("the v4 ledgers to be bound", async || {
        ns.lookup(db).await.ok().flatten().is_some()
            && ns.lookup(unborn).await.ok().flatten().is_some()
    })
    .await;
    let record = ns.lookup(db).await.expect("lookup").expect("registered");
    assert_eq!(record.commit_t, head_t, "{version}: the replicated head");
    eventually("the v4 ledger to answer queries", async || {
        names(&fluree, db).await == Some(vec!["Alice".into(), "Bob".into()])
    })
    .await;

    // v4's idempotency outcomes answer retries: its receipts, and its
    // failures, which are not re-run.
    let alice = node
        .committer
        .transact(request("fixture-alice", db, insert("Alice")))
        .await
        .expect("v4's receipt answers the retry");
    assert_eq!(alice.commit.t, alice_t, "{version}");
    let bob = node
        .committer
        .transact(request("fixture-bob", db, insert("Bob")))
        .await
        .expect("v4's receipt answers the retry");
    assert_eq!(bob.commit.t, head_t, "{version}");
    node.committer
        .transact(request(
            "fixture-malformed",
            db,
            TransactionBody::JsonLdInsert(json!(42)),
        ))
        .await
        .expect_err("v4's recorded failure answers the retry");
    if expected["noop_outcome"]
        .as_str()
        .is_some_and(|o| o.starts_with("Err"))
    {
        node.committer
            .transact(request(
                "fixture-noop",
                unborn,
                TransactionBody::JsonLdUpdate(json!({
                    "where": {"@id": "?s", "http://example.org/p": "?o"},
                    "delete": {"@id": "?s", "http://example.org/p": "?o"}
                })),
            ))
            .await
            .expect_err("v4's recorded failure answers the retry");
    }

    // The upgraded node writes, and reopens what it wrote over v4's.
    let carol = node
        .committer
        .transact(request("fixture-carol", db, insert("Carol")))
        .await
        .expect("a new write");
    assert_eq!(carol.commit.t, head_t + 1, "{version}");
    integration
        .raft
        .trigger()
        .snapshot()
        .await
        .expect("snapshot");
    eventually("a snapshot of this release", async || {
        integration
            .raft
            .metrics()
            .borrow()
            .snapshot
            .map(|id| id.index)
            > expected["snapshot_index"].as_u64()
    })
    .await;
    node.shutdown().await;
    drop(fluree);
    drop(integration);

    let (integration, fluree, node) = reopen(node_id, &raft_dir, &data_dir).await;
    eventually("the reopened node to answer queries", async || {
        names(&fluree, db).await == Some(vec!["Alice".into(), "Bob".into(), "Carol".into()])
    })
    .await;
    let record = integration
        .nameservice()
        .lookup(db)
        .await
        .expect("lookup")
        .expect("registered");
    assert_eq!(record.commit_t, head_t + 1, "{version}");
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_v4_2_0_node_reopens() {
    reopens("v4.2.0").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_v4_2_9_node_reopens() {
    reopens("v4.2.9").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_purged_v4_2_0_node_reopens() {
    reopens("v4.2.0-purged").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_purged_v4_2_9_node_reopens() {
    reopens("v4.2.9-purged").await;
}
