use fluree_db_core::{ContentKind, ContentStore as _};
use serde_json::json;

use super::{Blob, GarbageReason};
use crate::{Fluree, FlureeBuilder};

async fn file_fluree(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build_async()
        .await
        .unwrap()
}

/// A reconciliation checks one slice of the digest space per step, keeping
/// the time it started, until every file is checked once; one that must start
/// later starts over.
#[tokio::test]
async fn reconciliation_resumes_where_its_last_step_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("steps").await.unwrap();
    for name in ["alice", "bob", "carol"] {
        let ledger = fluree.ledger("steps:main").await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@id": format!("http://example.org/{name}"), "http://example.org/name": name}),
            )
            .await
            .unwrap();
    }
    let namespace = fluree.storage_namespace("steps:main").await.unwrap();
    for n in 0..3 {
        fluree
            .content_store(&namespace)
            .put(ContentKind::Txn, format!("{{\"stray\":{n}}}").as_bytes())
            .await
            .unwrap();
    }

    let mut steps = Vec::new();
    for _ in 0..10 {
        let step = fluree
            .reconcile_step("steps", 2, None, &mut None)
            .await
            .unwrap()
            .unwrap();
        steps.push(step.clone());
        if step.complete {
            break;
        }
    }
    assert!(steps.last().is_some_and(|s| s.complete), "{steps:?}");
    // Six files in slices of about two: four slices.
    assert_eq!(steps.len(), 4, "{steps:?}");
    assert!(steps.iter().all(|s| s.listed == 6), "{steps:?}");
    assert_eq!(
        steps.iter().map(|s| s.checked).sum::<usize>(),
        6,
        "{steps:?}"
    );
    assert_eq!(
        steps.iter().map(|s| s.recorded).sum::<usize>(),
        3,
        "{steps:?}"
    );

    // A step that may not resume progress from before `not_before` checks
    // everything again.
    let first = fluree
        .reconcile_step("steps", 2, None, &mut None)
        .await
        .unwrap()
        .unwrap();
    assert!(!first.complete);
    let restarted = fluree
        .reconcile_step("steps", 100, Some(i64::MAX), &mut None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restarted.checked, 6, "{restarted:?}");
    assert!(restarted.complete);

    // Steps sharing a listing list once, and again once the branches change.
    let mut listing = None;
    let first = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    let second = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first.listed, second.listed),
        (6, 0),
        "{first:?} {second:?}"
    );
    fluree
        .create_branch("steps", "dev", None, None)
        .await
        .unwrap();
    let third = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.listed, 6, "{third:?}");
}

/// A blob a reconciliation recorded stays unresolved while a writer pins
/// it, even once a failed attempt records it again.
#[tokio::test]
async fn recording_a_blob_again_keeps_it_unresolved() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("again").await.unwrap();
    let ledger = fluree.ledger("again:main").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@id": "http://example.org/alice", "http://example.org/name": "alice"}),
        )
        .await
        .unwrap();
    let namespace = fluree.storage_namespace("again:main").await.unwrap();
    let stray = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"stray\":true}")
        .await
        .unwrap();
    let report = fluree.reconcile_garbage("again").await.unwrap().unwrap();
    assert_eq!(report.recorded, 1);

    // A retry stores the same body, holds it, and fails again later.
    let pin = fluree.blob_pinner(&namespace).unwrap().pin(&stray).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    fluree
        .record_garbage(
            &[Blob::new(namespace.clone(), stray.clone())],
            GarbageReason::Abandoned,
        )
        .await
        .unwrap();
    assert!(!fluree.resolve_garbage("again", 0).await.unwrap());
    assert!(fluree.content_store(&namespace).has(&stray).await.unwrap());

    drop(pin);
    assert!(fluree.resolve_garbage("again", 0).await.unwrap());
    assert!(!fluree.content_store(&namespace).has(&stray).await.unwrap());
}

/// A ledger that cannot be reconciled yet, such as one with no commit, does
/// not keep the next ledger from its turn.
#[tokio::test]
async fn an_unsettled_ledger_does_not_hold_up_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("a-empty").await.unwrap();
    fluree.create_ledger("b-full").await.unwrap();
    let ledger = fluree.ledger("b-full:main").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@id": "http://example.org/alice", "http://example.org/name": "alice"}),
        )
        .await
        .unwrap();

    let pass = fluree
        .reconcile_due(
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(
        pass.ledgers
            .iter()
            .any(|(name, report)| name == "b-full" && report.complete),
        "{pass:?}"
    );
}

/// A pass out of time leaves the ledgers due, and says how long the
/// longest-waiting has gone unreconciled.
#[tokio::test]
async fn a_pass_out_of_time_leaves_its_ledgers_due() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    for name in ["one", "two"] {
        fluree.create_ledger(name).await.unwrap();
        commit_raw(&fluree, &format!("{name}:main"), name).await;
    }
    let none = std::time::Duration::ZERO;
    let pass = fluree.reconcile_due(none, none).await.unwrap();
    assert!(pass.ledgers.is_empty(), "{pass:?}");
    assert_eq!((pass.due, pass.oldest_ms), (2, None), "{pass:?}");

    let before = super::now_ms();
    fluree.reconcile_garbage("one").await.unwrap().unwrap();
    let pass = fluree.reconcile_due(none, none).await.unwrap();
    assert_eq!(pass.due, 2, "{pass:?}");
    assert!(pass.oldest_ms.is_some_and(|ms| ms >= before), "{pass:?}");

    let pass = fluree
        .reconcile_due(
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(pass.ledgers.len(), 1, "{pass:?}");
    assert_eq!(pass.due, 0, "{pass:?}");
}

async fn commit_raw(fluree: &Fluree, ledger_id: &str, name: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .txn_opts(fluree_db_transact::TxnOpts::default().store_raw_txn(true))
        .update(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "insert": {"@id": format!("ex:{name}"), "ex:name": name}
        }))
        .commit()
        .await
        .unwrap();
}

/// Over every file listed, the slice-at-a-time check a reconciliation makes
/// agrees with the one-blob check collection makes: indexed history and
/// novelty, a branch reading its parent's folder, the same body stored in
/// two folders, commits a merge copied in, and strays.
#[tokio::test]
async fn reconciliation_and_collection_agree_on_every_file() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("agree").await.unwrap();
    for name in ["a", "b", "c"] {
        commit_raw(&fluree, "agree:main", name).await;
    }
    fluree
        .reindex("agree:main", crate::ReindexOptions::default())
        .await
        .unwrap();
    commit_raw(&fluree, "agree:main", "d").await;
    fluree
        .create_branch("agree", "dev", None, None)
        .await
        .unwrap();
    for name in ["e", "f"] {
        commit_raw(&fluree, "agree:dev", name).await;
    }
    commit_raw(&fluree, "agree:main", "g").await;
    // Each branch reads its own copy of the same body.
    commit_raw(&fluree, "agree:dev", "same").await;
    commit_raw(&fluree, "agree:main", "same").await;
    fluree
        .merge_branch("agree", "dev", None, crate::ConflictStrategy::default())
        .await
        .unwrap();
    commit_raw(&fluree, "agree:dev", "h").await;

    let main = fluree.storage_namespace("agree:main").await.unwrap();
    let dev = fluree.storage_namespace("agree:dev").await.unwrap();
    // The body of `a`, which dev inherited, stored again in dev's folder:
    // dev reads that copy, main its own.
    let a_body = serde_json::to_vec(&json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:a", "ex:name": "a"}
    }))
    .unwrap();
    fluree
        .content_store(&dev)
        .put(ContentKind::Txn, &a_body)
        .await
        .unwrap();
    // The body of `h`, only dev's, stored again in main's folder: dev reads
    // its own copy, so nothing reads that one.
    let h_body = serde_json::to_vec(&json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:h", "ex:name": "h"}
    }))
    .unwrap();
    fluree
        .content_store(&main)
        .put(ContentKind::Txn, &h_body)
        .await
        .unwrap();
    for (namespace, stray) in [(&main, "main"), (&dev, "dev")] {
        fluree
            .content_store(namespace)
            .put(
                ContentKind::Txn,
                format!("{{\"stray\":\"{stray}\"}}").as_bytes(),
            )
            .await
            .unwrap();
    }

    let binding = fluree
        .publisher()
        .unwrap()
        .get_binding("agree")
        .await
        .unwrap()
        .unwrap()
        .value;
    let holders = fluree
        .load_holders("agree", &binding)
        .await
        .unwrap()
        .expect("settled");
    let listing = fluree.list_ledger(&binding, holders).await.unwrap();
    let listed = super::check::Listed::new(listing.slice(1, 0));
    let holders = &listing.holders;
    let read = holders.read(&listed).await.unwrap();
    let mut unread = Vec::new();
    for (blob, read) in listed.blobs.iter().zip(read) {
        let referenced = holders.references(&fluree, blob).await.unwrap();
        assert_eq!(read, referenced, "{blob:?}");
        if !read {
            unread.push(blob.clone());
        }
    }
    assert_eq!(unread.len(), 3, "{unread:?}");
    assert!(listed.blobs.len() > 20, "{}", listed.blobs.len());
}
