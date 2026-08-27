//! The transition boot: a store that lived in the single-node FILE
//! posture moves into raft, and `adopt_file_registry` carries every
//! registry record — existence, both heads, config, status — into the
//! replicated nameservice. Also pins the once-only marker semantics
//! and the crash-retry idempotence (a re-run over a partially adopted
//! machine converges instead of conflicting).

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_core::{ContentId, ContentKind};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::{
    CasResult, LedgerLifecycle, NameServiceLookup, RefKind, RefLookup, RefPublisher, RefValue,
};

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

fn cid(kind: ContentKind, seed: u8) -> ContentId {
    ContentId::new(kind, &[seed])
}

/// Seed a file registry the way a life in the file posture would have:
/// through the FileNameService's own publisher surface.
async fn seed_file_registry(root: &std::path::Path) {
    let ns = FileNameService::new(root);
    for (ledger, commit_seed, commit_t, index_seed, index_t) in [
        ("adopted/one", 1u8, 5i64, 2u8, 3i64),
        ("adopted/two", 3u8, 12i64, 4u8, 12i64),
    ] {
        ns.init(ledger).await.expect("init");
        let head = RefValue {
            id: Some(cid(ContentKind::Commit, commit_seed)),
            t: commit_t,
        };
        assert!(matches!(
            ns.fast_forward_commit(ledger, &head, 3)
                .await
                .expect("commit head"),
            CasResult::Updated
        ));
        let idx = RefValue {
            id: Some(cid(ContentKind::IndexRoot, index_seed)),
            t: index_t,
        };
        let current = ns.get_ref(ledger, RefKind::IndexHead).await.expect("read");
        assert!(matches!(
            ns.compare_and_set_ref(ledger, RefKind::IndexHead, current.as_ref(), &idx)
                .await
                .expect("index head"),
            CasResult::Updated
        ));
    }
    // A tombstone must NOT carry.
    ns.init("adopted/gone").await.expect("init tombstone");
    ns.retract("adopted/gone").await.expect("retract");
}

async fn boot_initialized(
    node_id: u64,
    raft_dir: &std::path::Path,
    store_dir: &std::path::Path,
) -> Arc<RaftIntegration> {
    let integration = Arc::new(
        RaftIntegration::bootstrap(
            RaftBootstrapConfig::new(node_id, raft_dir).with_file_registry_adoption(store_dir),
        )
        .await
        .expect("bootstrap"),
    );
    let mut members = BTreeMap::new();
    members.insert(
        node_id,
        fluree_db_consensus::raft::ClusterNode::new(
            "http://127.0.0.1:1/raft/nameservice".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
    );
    integration
        .raft
        .initialize(members)
        .await
        .expect("initialize");
    let raft = Arc::clone(&integration.raft);
    let id = node_id;
    eventually("self-election", async || {
        raft.current_leader().await == Some(id)
    })
    .await;
    integration
}

#[tokio::test]
async fn a_file_registry_adopts_once_and_survives_a_partial_retry() {
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let store_dir = tempfile::tempdir().expect("store dir");
    seed_file_registry(store_dir.path()).await;

    let integration = boot_initialized(7, raft_dir.path(), store_dir.path()).await;

    // The replay carries the two live records and skips the tombstone.
    let adopted = integration.adopt_file_registry().await.expect("adoption");
    assert_eq!(adopted, 2);
    let ns = integration.nameservice();
    let one = ns
        .lookup("adopted/one:main")
        .await
        .expect("lookup")
        .expect("adopted/one present");
    assert_eq!(one.commit_t, 5);
    assert_eq!(one.index_t, 3);
    assert_eq!(one.commit_head_id, Some(cid(ContentKind::Commit, 1)));
    assert_eq!(one.index_head_id, Some(cid(ContentKind::IndexRoot, 2)));
    let two = ns
        .lookup("adopted/two:main")
        .await
        .expect("lookup")
        .expect("adopted/two present");
    assert_eq!(two.commit_t, 12);
    assert!(ns
        .lookup("adopted/gone:main")
        .await
        .expect("lookup")
        .is_none_or(|r| r.retracted));

    // The marker makes a second call a no-op.
    assert_eq!(integration.adopt_file_registry().await.expect("re-run"), 0);

    // Crash between replay and marker: delete the marker and re-run —
    // the conflict-tolerant replay converges over its own prior writes.
    std::fs::remove_file(raft_dir.path().join("file-registry-adopted")).expect("drop marker");
    assert_eq!(
        integration
            .adopt_file_registry()
            .await
            .expect("retry over a partially adopted machine"),
        2
    );
    assert_eq!(
        ns.lookup("adopted/one:main")
            .await
            .expect("lookup")
            .expect("still present")
            .commit_t,
        5
    );

    integration.raft.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn no_registry_and_no_config_both_answer_zero() {
    // Configured path with no ns@v2 underneath: marker written, zero carried.
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let store_dir = tempfile::tempdir().expect("store dir");
    let integration = boot_initialized(9, raft_dir.path(), store_dir.path()).await;
    assert_eq!(integration.adopt_file_registry().await.expect("empty"), 0);
    assert!(raft_dir.path().join("file-registry-adopted").is_file());

    // No adoption configured at all: permanent no-op, no marker.
    let raft_dir2 = tempfile::tempdir().expect("raft dir");
    let bare = Arc::new(
        RaftIntegration::bootstrap(RaftBootstrapConfig::new(11, raft_dir2.path()))
            .await
            .expect("bootstrap"),
    );
    let mut members = BTreeMap::new();
    members.insert(
        11,
        fluree_db_consensus::raft::ClusterNode::new(
            "http://127.0.0.1:1/raft/nameservice".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
    );
    bare.raft.initialize(members).await.expect("initialize");
    let raft = Arc::clone(&bare.raft);
    eventually("self-election", async || {
        raft.current_leader().await == Some(11)
    })
    .await;
    assert_eq!(bare.adopt_file_registry().await.expect("unconfigured"), 0);
    assert!(!raft_dir2.path().join("file-registry-adopted").is_file());

    integration.raft.shutdown().await.expect("shutdown");
    bare.raft.shutdown().await.expect("shutdown");
}
