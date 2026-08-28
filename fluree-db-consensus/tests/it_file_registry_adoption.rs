//! The transition boot: a store that lived in the single-node FILE
//! posture moves into raft, and `adopt_file_registry` carries every
//! registry record — existence, both heads, config, status — into the
//! replicated nameservice. Also pins the once-only marker semantics,
//! the crash-retry idempotence (a re-run over a partially adopted
//! machine converges instead of conflicting), and the two cases that
//! must NOT write the marker: a registry root with no `ns@v2` subtree,
//! and a record the replay cannot read.

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluree_db_consensus::raft::integration::{RaftBootstrapConfig, RaftIntegration};
use fluree_db_core::{ContentId, ContentKind};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::{
    CasResult, ConfigCasResult, ConfigLookup, ConfigPayload, ConfigPublisher, ConfigValue,
    LedgerLifecycle, NameServiceLookup, RefKind, RefLookup, RefPublisher, RefValue,
    StatusCasResult, StatusLookup, StatusPayload, StatusPublisher, StatusValue,
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

        // Config and status are part of a record's live state and
        // must carry too. The file backend reads an unset config as
        // `unborn` and an unset status as `initial`, so the seeding
        // CAS expects exactly that.
        let config = ConfigValue::new(
            4,
            Some(ConfigPayload::with_default_context(cid(
                ContentKind::Commit,
                commit_seed.wrapping_add(40),
            ))),
        );
        assert!(matches!(
            ns.push_config(ledger, Some(&ConfigValue::unborn()), &config)
                .await
                .expect("seed config"),
            ConfigCasResult::Updated
        ));
        let status = StatusValue::new(6, StatusPayload::new(format!("seeded-{commit_seed}")));
        assert!(matches!(
            ns.push_status(ledger, Some(&StatusValue::initial()), &status)
                .await
                .expect("seed status"),
            StatusCasResult::Updated
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

    // Config and status carry VERBATIM — watermark and payload both.
    // The machine reads an unset value as `unborn` / `initial`, so
    // these assertions fail against a replay that pushed with a `None`
    // expectation (which can never match) as well as against one that
    // dropped the values outright.
    for (ledger, commit_seed) in [("adopted/one:main", 1u8), ("adopted/two:main", 3u8)] {
        let config = ns
            .get_config(ledger)
            .await
            .expect("config read")
            .expect("config present");
        assert_eq!(config.v, 4, "{ledger} config watermark");
        assert_eq!(
            config.payload.expect("config payload").default_context,
            Some(cid(ContentKind::Commit, commit_seed.wrapping_add(40))),
            "{ledger} config payload"
        );
        let status = ns
            .get_status(ledger)
            .await
            .expect("status read")
            .expect("status present");
        assert_eq!(status.v, 6, "{ledger} status watermark");
        assert_eq!(status.payload.state, format!("seeded-{commit_seed}"));
    }

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
    // The re-run tolerated its own prior config/status write rather
    // than diverging on the unchanged watermark.
    assert_eq!(
        ns.get_config("adopted/one:main")
            .await
            .expect("config read")
            .expect("config present")
            .v,
        4
    );
    assert_eq!(
        ns.get_status("adopted/one:main")
            .await
            .expect("status read")
            .expect("status present")
            .v,
        6
    );

    integration.raft.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn no_registry_and_no_config_both_answer_zero() {
    // Configured path with no ns@v2 underneath: zero carried and NO
    // marker, so a mistyped registry root doesn't burn the one-shot
    // adoption before the corrected config gets a turn.
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let store_dir = tempfile::tempdir().expect("store dir");
    let integration = boot_initialized(9, raft_dir.path(), store_dir.path()).await;
    assert_eq!(integration.adopt_file_registry().await.expect("empty"), 0);
    assert!(!raft_dir.path().join("file-registry-adopted").is_file());

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

#[tokio::test]
async fn an_unreadable_record_aborts_before_the_marker_is_written() {
    // Adoption is once-only, so nothing it cannot READ may be skipped
    // past: the run aborts and withholds the marker, leaving the
    // operator a signal AND a second chance. Without that, a ledger
    // arrives in raft with state silently absent and the marker
    // makes every later call `Ok(0)`.
    let raft_dir = tempfile::tempdir().expect("raft dir");
    let store_dir = tempfile::tempdir().expect("store dir");
    seed_file_registry(store_dir.path()).await;

    // Truncate one record the way an unclean shutdown would, leaving
    // the file present (so `all_records` still yields it) but its JSON
    // unparseable.
    let mut truncated = None;
    for entry in walk(&store_dir.path().join("ns@v2")) {
        if entry.to_string_lossy().contains("one") {
            std::fs::write(&entry, b"{\"ledger\": ").expect("truncate");
            truncated = Some(entry);
            break;
        }
    }
    let truncated = truncated.expect("a record file for adopted/one");

    let integration = boot_initialized(13, raft_dir.path(), store_dir.path()).await;
    let err = integration
        .adopt_file_registry()
        .await
        .expect_err("a truncated record must not adopt silently");
    // Surfaced by the registry read here; a corruption that survives
    // `all_records` and fails a per-record read lands on the same
    // abort-before-`finish` path through `Replay`.
    assert!(
        matches!(
            err,
            fluree_db_consensus::raft::integration::FileRegistryAdoptionError::Registry(_)
                | fluree_db_consensus::raft::integration::FileRegistryAdoptionError::Replay { .. }
        ),
        "unreadable record surfaces as a read failure, not a marker write: {err}"
    );
    assert!(
        !raft_dir.path().join("file-registry-adopted").is_file(),
        "marker withheld so the operator can repair {} and re-run",
        truncated.display()
    );

    integration.raft.shutdown().await.expect("shutdown");
}

/// Every file under `root`, recursively.
fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}
