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

/// A reconciliation taken in steps resumes after the last file it checked,
/// keeping the time it started; one that must start later starts over.
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
            .reconcile_step("steps", 2, None)
            .await
            .unwrap()
            .unwrap();
        steps.push(step.clone());
        if step.complete {
            break;
        }
    }
    assert!(steps.last().is_some_and(|s| s.complete), "{steps:?}");
    assert_eq!(
        steps.iter().map(|s| s.listed).sum::<usize>(),
        6,
        "{steps:?}"
    );
    assert_eq!(
        steps.iter().map(|s| s.recorded).sum::<usize>(),
        3,
        "{steps:?}"
    );
    assert_eq!(steps.len(), 3, "{steps:?}");

    // A step that may not resume progress from before `not_before` checks
    // everything again.
    let first = fluree
        .reconcile_step("steps", 2, None)
        .await
        .unwrap()
        .unwrap();
    assert!(!first.complete);
    let restarted = fluree
        .reconcile_step("steps", 100, Some(i64::MAX))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restarted.listed, 6, "{restarted:?}");
    assert!(restarted.complete);
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

    let (name, report) = fluree
        .reconcile_next(std::time::Duration::ZERO)
        .await
        .unwrap()
        .expect("a ledger reconciled");
    assert_eq!(name, "b-full");
    assert!(report.complete);
}
