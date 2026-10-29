//! The worker's coordination with branch creation, and a step proved on an
//! erasure published since.

use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::commit::codec::replacement::is_replacement;
use fluree_db_core::{collect_first_parent_cids, ContentId};
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

use super::Stepped;
use crate::retention::erasure::index::{ErasedCommit, ErasureStep};
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

/// Publication, branch creation and restores exclude each other: none
/// proceeds while another holds admission.
#[tokio::test]
async fn publication_branch_creation_and_restores_wait_for_admission() {
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

    // With its index, so the copy does not hold publication back.
    let mut archive = Vec::new();
    fluree.archive_ledger(id, true, &mut archive).await.unwrap();
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
    let restore = {
        let fluree = fluree.clone();
        tokio::spawn(async move {
            fluree
                .restore_ledger("t/admission-copy", &mut archive.as_slice())
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
    assert!(
        !restore.is_finished(),
        "a copy was restored while admission was held"
    );
    drop(held);
    assert_eq!(pass.await.unwrap().unwrap().published, 1);
    branch.await.unwrap().unwrap();
    restore.await.unwrap().unwrap();
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

/// A ledger dropped while its erasure waits for an index hides its
/// branches; publishing then would skip their indexes, which a restore
/// brings back. Nothing is published while it is dropped.
#[tokio::test]
async fn nothing_is_published_while_the_ledger_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/dropped").await.unwrap();
    let id = "t/dropped:main";
    let t1 = insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(&fluree, id, t1).await;
    let receipt = fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let report = fluree.run_erasure_pass(id).await.unwrap();
    assert_eq!((report.proved, report.published), (2, 0), "{report:?}");
    let job = fluree.job(id, &receipt.request_id).await.unwrap();

    fluree
        .drop_ledger("t/dropped", crate::DropMode::Soft)
        .await
        .unwrap();
    // As a background pass holding the job would.
    fluree.publish_when_indexed(id, &job).await.unwrap();
    let runtime = fluree.erasure_runtime().unwrap();
    assert!(runtime.index.erased(&asserted).await.unwrap().is_none());
}

/// Published before a crash lost the job's update: the next pass finds the
/// erasure published and completes the job without proving it again, and
/// reports it only once another instance on the storage reads it.
#[tokio::test]
async fn a_publication_whose_job_update_was_lost_completes() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/lost-update").await.unwrap();
    let id = "t/lost-update:main";
    let t1 = insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(&fluree, id, t1).await;
    let receipt = fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    fluree.run_erasure_pass(id).await.unwrap();
    reindex(&fluree, id).await;
    let reader = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(ErasureConfig {
            prover: Arc::new(NativeProver::default()),
            verifier: Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build_client_with_nameservice(crate::NameServiceMode::ReadOnly(Arc::new(
            fluree_db_nameservice::file::FileNameService::new(dir.path()),
        )))
        .await
        .unwrap();
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let read = reader.content_store(&record.storage_namespace());
    assert!(!is_replacement(&read.get(&asserted).await.unwrap()));

    // The maps written as publication writes them; the job left as it was.
    let job = fluree.job(id, &receipt.request_id).await.unwrap();
    let published = job
        .commits
        .iter()
        .map(|task| {
            let proved = task.proved.as_ref().unwrap();
            ErasedCommit {
                commit: task.commit.clone(),
                ledger_id: task.ledger_id.clone(),
                txn: None,
                steps: vec![ErasureStep {
                    request_id: job.request_id.clone(),
                    slots: proved.slots.clone(),
                    replacement: proved.replacement.clone(),
                    proof: proved.proof.clone(),
                }],
                replaced_at: Vec::new(),
            }
        })
        .collect();
    let runtime = fluree.erasure_runtime().unwrap();
    runtime.index.publish(published).await.unwrap();
    drop(fluree);

    let restarted = erasing(&dir);
    let report = restarted.run_erasure_pass(id).await.unwrap();
    assert_eq!((report.proved, report.published), (0, 1), "{report:?}");
    let job = restarted.job(id, &receipt.request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Published);
    assert!(is_replacement(&read.get(&asserted).await.unwrap()));
}

/// Panics on its first step, then proves as `NativeProver` does.
#[derive(Debug, Default)]
struct PanicsOnce {
    panicked: std::sync::atomic::AtomicBool,
    inner: NativeProver,
}

#[async_trait::async_trait]
impl fluree_db_erasure::Prover for PanicsOnce {
    async fn prove(
        &self,
        request: &fluree_db_erasure::ProveRequest,
    ) -> std::result::Result<fluree_db_erasure::ProveResponse, fluree_db_erasure::ProverError> {
        if !self
            .panicked
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("the prover panicked");
        }
        self.inner.prove(request).await
    }
}

/// A background step that panics is retried like a failed one; it does not
/// leave background proving stopped until a restart.
#[tokio::test]
async fn a_panicking_background_step_is_retried() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .without_indexing()
            .with_erasure(ErasureConfig {
                prover: Arc::new(PanicsOnce::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
            .build()
            .unwrap(),
    );
    fluree.create_ledger("t/panic").await.unwrap();
    let id = "t/panic:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let receipt = fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();

    let weak = Arc::downgrade(&fluree);
    let ledgers = [id.to_string()];
    let mut proved = 0;
    for _ in 0..200 {
        fluree
            .erasure_sweep(&weak, &ledgers, Duration::from_secs(3600))
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let job = fluree.job(id, &receipt.request_id).await.unwrap();
        proved = job.commits.iter().filter(|c| c.proved.is_some()).count();
        if proved == job.commits.len() {
            break;
        }
    }
    let job = fluree.job(id, &receipt.request_id).await.unwrap();
    assert_eq!(proved, job.commits.len(), "{job:?}");
}

/// A job in a later release's stage keeps that release's fields, at the
/// top and nested, through the work this release does on the registration
/// around it: a pass, and another request's job.
#[tokio::test]
async fn a_later_release_job_keeps_its_fields_through_this_releases_work() {
    use crate::retention::registry;

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/later-stage").await.unwrap();
    let id = "t/later-stage:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/bob", "http://example.org/email": "b@secret"}
        ]}),
    )
    .await;
    fluree
        .erase_occurrences(id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let ns = fluree.publisher().unwrap();
    let (_, read) = registry::read(ns, id).await.unwrap();
    let mut stored = serde_json::to_value(read.unwrap()).unwrap();
    let job = &mut stored["erasures"][0];
    job["stage"] = "remoteQueued".into();
    job["remoteTask"] = json!({"worker": "w1", "lease": 5});
    job["commits"][0]["remoteSlot"] = 3.into();
    let later = job.clone();
    let registration: registry::Registration = serde_json::from_value(stored).unwrap();
    registry::update(ns, id, |_| Some(registration.clone()))
        .await
        .unwrap();

    fluree.run_erasure_pass(id).await.unwrap();
    fluree
        .erase_occurrences(id, email("bob", "b@secret"), None)
        .await
        .unwrap();
    fluree.run_erasure_pass(id).await.unwrap();

    let config = ns.get_config(id).await.unwrap().unwrap();
    let jobs = config.payload.unwrap().extra["retention"]["erasures"].clone();
    assert_eq!(jobs.as_array().map(Vec::len), Some(2), "{jobs}");
    assert!(jobs.as_array().unwrap().contains(&later), "{jobs}");
}
