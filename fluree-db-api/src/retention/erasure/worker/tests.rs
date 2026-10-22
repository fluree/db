//! The worker's coordination with branch creation, and a step proved on an
//! erasure published since.

use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::commit::codec::replacement::is_replacement;
use fluree_db_core::{collect_first_parent_cids, ContentId};
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

use super::Stepped;
use crate::retention::erasure::{ErasureConfig, ErasureRequest, ErasureStage, Verifier};
use crate::retention::FactSelector;
use crate::{Fluree, FlureeBuilder, ReindexOptions};

fn erasing(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(ErasureConfig {
            prover: Arc::new(NativeProver::default()),
            verifier: Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build()
        .unwrap()
}

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .unwrap()
        .receipt
        .t
}

async fn reindex(fluree: &Fluree, ledger_id: &str) {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
}

fn email(subject: &str, value: &str) -> ErasureRequest {
    ErasureRequest {
        matching: vec![FactSelector {
            subject: format!("http://example.org/{subject}"),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!(value)),
        }],
        ..Default::default()
    }
}

async fn commit_at(fluree: &Fluree, ledger_id: &str, t: i64) -> ContentId {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let store = fluree.content_store(&record.storage_namespace());
    collect_first_parent_cids(store.as_ref(), record.commit_head_id.as_ref().unwrap(), 0)
        .await
        .unwrap()
        .into_iter()
        .find(|(ct, _)| *ct == t)
        .unwrap()
        .1
}

/// Publication and branch creation exclude each other: neither proceeds
/// while the other holds admission.
#[tokio::test]
async fn publication_and_branch_creation_wait_for_admission() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(erasing(&dir));
    fluree.create_ledger("t/admission").await.unwrap();
    let id = "t/admission:main";
    let t1 = insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(&fluree, id, t1).await;
    fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    reindex(&fluree, id).await;

    let runtime = fluree.erasure_runtime().unwrap().clone();
    let held = runtime.admission.lock().await;
    let pass = {
        let fluree = fluree.clone();
        tokio::spawn(async move { fluree.run_erasure_pass(id).await })
    };
    let branch = {
        let fluree = fluree.clone();
        tokio::spawn(async move {
            fluree
                .create_branch("t/admission", "late", None, None)
                .await
                .map(|_| ())
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        runtime.index.erased(&asserted).await.unwrap().is_none(),
        "published while admission was held"
    );
    assert!(
        !branch.is_finished(),
        "a branch was made while admission was held"
    );
    drop(held);
    assert_eq!(pass.await.unwrap().unwrap().published, 1);
    branch.await.unwrap().unwrap();
}

/// A later request's step proved on the original, before the earlier
/// request published its own: publishing it would make a chain that does
/// not link, so it is proved again on the published erasure.
#[tokio::test]
async fn a_step_proved_on_a_superseded_erasure_is_proved_again() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/superseded").await.unwrap();
    let id = "t/superseded:main";
    let t1 = insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/carol", "http://example.org/email": "c@secret"}
        ]}),
    )
    .await;
    let shared = commit_at(&fluree, id, t1).await.to_string();
    let first = fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let second = fluree
        .erase_occurrences(id, email("carol", "c@secret"), None)
        .await
        .unwrap();

    // The later request's step on the shared commit, out of turn.
    let stepped = fluree
        .step_and_record(id, &second.request_id, &shared, 10)
        .await;
    assert!(matches!(stepped, Stepped::Proved));
    let job = fluree.job(id, &first.request_id).await.unwrap();
    for task in &job.commits {
        fluree
            .step_and_record(id, &first.request_id, &task.commit, 10)
            .await;
    }
    reindex(&fluree, id).await;
    let job = fluree.job(id, &first.request_id).await.unwrap();
    fluree.publish_when_indexed(id, &job).await.unwrap();
    let first_job = fluree.job(id, &first.request_id).await.unwrap();
    assert_eq!(first_job.stage, ErasureStage::Published);

    let job = fluree.job(id, &second.request_id).await.unwrap();
    fluree.publish_when_indexed(id, &job).await.unwrap();
    let job = fluree.job(id, &second.request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Proving);
    let task = job.commits.iter().find(|c| c.commit == shared).unwrap();
    assert!(task.proved.is_none(), "{task:?}");

    let report = fluree.run_erasure_pass(id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let job = fluree.job(id, &second.request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Published);
    let base = &first_job
        .commits
        .iter()
        .find(|c| c.commit == shared)
        .unwrap()
        .proved
        .as_ref()
        .unwrap()
        .replacement;
    let task = job.commits.iter().find(|c| c.commit == shared).unwrap();
    assert_eq!(task.proved.as_ref().unwrap().base.as_ref(), Some(base));
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let raw = fluree
        .content_store(&record.storage_namespace())
        .get(&shared.parse().unwrap())
        .await
        .unwrap()
        .into_vec();
    let contains = |needle: &str| raw.windows(needle.len()).any(|w| w == needle.as_bytes());
    assert!(is_replacement(&raw) && !contains("a@secret") && !contains("c@secret"));
}
