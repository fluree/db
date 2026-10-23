use fluree_db_core::{ContentKind, ContentStore as _};
use serde_json::json;

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
