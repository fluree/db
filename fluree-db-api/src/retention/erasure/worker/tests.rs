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
    let held = runtime.admission.write().await;
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

/// What only adds branches shares admission: a branch created, or a dropped
/// ledger restored, while another branch is being created, is not held up
/// by it; a publication is.
#[tokio::test]
async fn branch_creations_and_restores_do_not_wait_for_one_another() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(erasing(&dir));
    for name in ["t/shared", "t/shared-dropped"] {
        fluree.create_ledger(name).await.unwrap();
        insert(
            &fluree,
            &format!("{name}:main"),
            json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
        )
        .await;
    }
    let dropped = fluree
        .drop_ledger("t/shared-dropped", crate::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .expect("a soft drop keeps the instance");
    fluree
        .erase_occurrences("t/shared:main", email("alice", "a@secret"), None)
        .await
        .unwrap();
    reindex(&fluree, "t/shared:main").await;

    let runtime = fluree.erasure_runtime().unwrap().clone();
    // As a branch creation under way holds it.
    let creating = runtime.admission.read().await;
    async fn within<T>(what: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), what)
            .await
            .expect("not held up")
    }
    within(fluree.create_branch("t/shared", "other", None, None))
        .await
        .unwrap();
    within(fluree.restore_dropped(dropped.as_str()))
        .await
        .unwrap();
    let pass = {
        let fluree = fluree.clone();
        tokio::spawn(async move { fluree.run_erasure_pass("t/shared:main").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!pass.is_finished(), "published during a branch creation");
    drop(creating);
    pass.await.unwrap().unwrap();
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
                locations_known: false,
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
    let (_, read) = registry::read(ns, fluree.registry_objects(), id)
        .await
        .unwrap();
    let mut stored = serde_json::to_value(read.unwrap()).unwrap();
    let job = &mut stored["erasures"][0];
    job["stage"] = "remoteQueued".into();
    job["remoteTask"] = json!({"worker": "w1", "lease": 5});
    job["commits"][0]["remoteSlot"] = 3.into();
    let later = job.clone();
    let registration: registry::Registration = serde_json::from_value(stored).unwrap();
    registry::update(ns, fluree.registry_objects(), id, |_| {
        Some(registration.clone())
    })
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

/// A cluster's activations as the worker meets them: the first `refusals`
/// are refused, each later one applies. At each, the manifest holds the
/// erasures and nothing reads them yet.
#[derive(Debug, Default)]
struct Cluster {
    index: std::sync::OnceLock<Arc<crate::retention::erasure::index::ErasureIndex>>,
    refusals: std::sync::atomic::AtomicUsize,
    /// The next activation applies, but its response is lost.
    lose_response: std::sync::atomic::AtomicBool,
    calls: parking_lot::Mutex<Vec<&'static str>>,
    applied: parking_lot::Mutex<Vec<String>>,
}

impl Cluster {
    fn on(fluree: &Fluree, refusals: usize) -> Arc<Self> {
        let cluster = Arc::new(Cluster {
            refusals: refusals.into(),
            ..Default::default()
        });
        let index = fluree.erasure_runtime().unwrap().index.clone();
        cluster.index.set(index).unwrap();
        fluree.set_erasure_activator(cluster.clone());
        cluster
    }

    fn calls(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.calls.lock())
    }
}

#[async_trait::async_trait]
impl crate::retention::erasure::ErasureActivator for Cluster {
    async fn holder_set(&self) -> crate::Result<Option<u64>> {
        self.calls.lock().push("holder_set");
        Ok(Some(1))
    }

    async fn hold(
        &self,
        holders: &[fluree_db_nameservice::NsRecord],
        _: Duration,
    ) -> crate::Result<()> {
        assert!(!holders.is_empty());
        self.calls.lock().push("hold");
        Ok(())
    }

    async fn release(&self, _: &[fluree_db_nameservice::NsRecord]) -> crate::Result<()> {
        self.calls.lock().push("release");
        Ok(())
    }

    async fn activate(
        &self,
        manifest: &str,
        _: u64,
        holders: &[fluree_db_nameservice::NsRecord],
    ) -> crate::Result<Option<u64>> {
        assert!(!holders.is_empty());
        self.calls.lock().push("activate");
        let index = self.index.get().unwrap();
        let erased = index.manifest(manifest).await.unwrap();
        assert!(!erased.is_empty());
        for e in &erased {
            let cid: ContentId = e.commit.parse().unwrap();
            assert!(
                index.erased(&cid).await.unwrap().is_none(),
                "read as erased before its activation"
            );
        }
        if self.refusals.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            self.refusals
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return Ok(None);
        }
        let number = {
            let mut applied = self.applied.lock();
            applied.push(manifest.to_string());
            applied.len() as u64 - 1
        };
        if self
            .lose_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::ApiError::internal("the response was lost"));
        }
        Ok(Some(number))
    }

    async fn applied(&self, manifest: &str) -> Option<u64> {
        self.applied
            .lock()
            .iter()
            .position(|m| m == manifest)
            .map(|n| n as u64)
    }

    async fn installed_everywhere(&self, _: u64) -> bool {
        true
    }

    async fn record_purged(&self, _: u64) -> crate::Result<u64> {
        Ok(0)
    }

    async fn not_cleaned(&self, _: u64) -> Vec<u64> {
        Vec::new()
    }
}

/// A requested erasure, proved and indexed, ready to publish.
async fn ready_to_publish(fluree: &Fluree, name: &str) -> (String, String, ContentId) {
    fluree.create_ledger(name).await.unwrap();
    let id = format!("{name}:main");
    let t1 = insert(
        fluree,
        &id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(fluree, &id, t1).await;
    let receipt = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    fluree.run_erasure_pass(&id).await.unwrap();
    reindex(fluree, &id).await;
    (id, receipt.request_id, asserted)
}

async fn map_objects(fluree: &Fluree) -> Vec<String> {
    let runtime = fluree.erasure_runtime().unwrap();
    runtime
        .index
        .list_all()
        .await
        .unwrap()
        .into_iter()
        .filter(|a| a.contains("/erasure/map/"))
        .collect()
}

async fn reads_replacement(fluree: &Fluree, id: &str, commit: &ContentId) -> bool {
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let store = fluree.content_store(&record.storage_namespace());
    is_replacement(&store.get(commit).await.unwrap())
}

/// On a cluster, publication is the activation of a manifest: nothing
/// reads the erasures before it applies, one refused holds the holders and
/// prepares again, and the map objects are written once it has applied.
#[tokio::test]
async fn a_cluster_publishes_through_an_activation_of_its_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let cluster = Cluster::on(&fluree, 1);
    let (id, request_id, asserted) = ready_to_publish(&fluree, "t/cluster").await;
    cluster.calls();

    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    assert_eq!(
        cluster.calls(),
        [
            "holder_set",
            "activate",
            "hold",
            "holder_set",
            "activate",
            "release"
        ]
    );
    let job = fluree.job(&id, &request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Published);
    let publication = job.cluster.unwrap();
    assert_eq!(publication.activation, Some(0));
    assert_eq!(
        *cluster.applied.lock(),
        std::slice::from_ref(&publication.manifest)
    );
    assert!(publication.effective_ms.is_some());
    let maps = map_objects(&fluree).await;
    assert_eq!(maps.len(), 1, "{maps:?}");
    assert!(maps[0].ends_with(&format!("/{}.json", publication.manifest)));
    assert!(reads_replacement(&fluree, &id, &asserted).await);
}

/// Refused again under the hold, the activation lifts it and publishes
/// nothing; a later pass prepares and activates afresh.
#[tokio::test]
async fn an_activation_refused_under_the_hold_releases_it() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let cluster = Cluster::on(&fluree, 2);
    let (id, request_id, asserted) = ready_to_publish(&fluree, "t/cluster-refused").await;
    cluster.calls();

    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 0, "{report:?}");
    assert_eq!(
        cluster.calls(),
        [
            "holder_set",
            "activate",
            "hold",
            "holder_set",
            "activate",
            "release"
        ]
    );
    let job = fluree.job(&id, &request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Indexing);
    assert_eq!(job.cluster.as_ref().and_then(|c| c.activation), None);
    assert!(map_objects(&fluree).await.is_empty());
    assert!(!reads_replacement(&fluree, &id, &asserted).await);

    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    assert!(reads_replacement(&fluree, &id, &asserted).await);
}

/// An activation that applied but whose response was lost is found applied
/// once the member has installed its manifest, which it reads by: its map
/// object is written then, and the job records it.
#[tokio::test]
async fn an_activation_whose_response_was_lost_completes() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let cluster = Cluster::on(&fluree, 0);
    cluster
        .lose_response
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (id, request_id, asserted) = ready_to_publish(&fluree, "t/cluster-lost").await;

    assert!(fluree.run_erasure_pass(&id).await.is_err());
    let manifest = cluster.applied.lock()[0].clone();
    assert!(map_objects(&fluree).await.is_empty());
    assert!(!reads_replacement(&fluree, &id, &asserted).await);
    // This member's installer installs what the cluster applied.
    fluree
        .install_erasures(&manifest, std::slice::from_ref(&id))
        .await
        .unwrap();
    assert!(reads_replacement(&fluree, &id, &asserted).await);
    assert!(map_objects(&fluree).await.is_empty());

    cluster.calls();
    fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(cluster.calls(), ["holder_set"], "nothing activated again");
    let job = fluree.job(&id, &request_id).await.unwrap();
    assert_eq!(job.stage, ErasureStage::Published);
    let publication = job.cluster.unwrap();
    assert_eq!(
        (publication.manifest.as_str(), publication.activation),
        (manifest.as_str(), Some(0))
    );
    assert_eq!(map_objects(&fluree).await.len(), 1);
}

/// A cluster member that, the first time acceptance confirms it is current,
/// finds another member made a branch meanwhile: from the head before the
/// removal, with its source's journal as it was before it held it.
struct BranchedMeanwhile {
    publisher: Arc<dyn fluree_db_nameservice::NameServicePublisher>,
    at: (ContentId, i64),
    made: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::retention::replicas::ReplicaSync for BranchedMeanwhile {
    async fn sync_reads(&self) -> std::result::Result<(), String> {
        if !self.made.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let name = fluree_db_core::LedgerName::parse("t/meanwhile").unwrap();
            fluree_db_nameservice::lifecycle::create_branch_with(
                self.publisher.as_ref(),
                &name,
                "late",
                "main",
                Some(self.at.clone()),
                |_| async { Ok(()) },
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn await_replicas(&self, _: Duration) -> Vec<String> {
        Vec::new()
    }
}

/// On a cluster, a branch another member makes while a request is accepted,
/// which neither the listing nor its copied journal covers, is removed from
/// too: it would otherwise keep the erased value as current state.
#[tokio::test]
async fn acceptance_removes_from_a_branch_another_member_made_meanwhile() {
    use crate::retention::{RetentionBatch, RetentionWriter, SubmitOutcome};

    struct Direct<'a>(&'a Fluree);
    #[async_trait::async_trait]
    impl RetentionWriter for Direct<'_> {
        async fn submit(
            &self,
            ledger_id: &str,
            batch: RetentionBatch,
        ) -> crate::Result<SubmitOutcome> {
            crate::retention::writer::outcome_of(
                self.0.commit_retention_batch(ledger_id, batch).await,
            )
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/meanwhile").await.unwrap();
    let id = "t/meanwhile:main";
    let t1 = insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(&fluree, id, t1).await;
    fluree.set_retention_replicas(Arc::new(BranchedMeanwhile {
        publisher: fluree.nameservice_mode().publisher_arc().unwrap(),
        at: (asserted, t1),
        made: false.into(),
    }));

    let receipt = fluree
        .accept_erasure(&Direct(&fluree), id, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let late = "t/meanwhile:late";
    assert!(
        receipt.removals.iter().any(|r| r.ledger_id == late),
        "{:?}",
        receipt.removals
    );
    let results = fluree
        .query_connection(&json!({
            "from": late,
            "select": "?o",
            "where": {"@id": "http://example.org/alice", "http://example.org/email": "?o"}
        }))
        .await
        .unwrap();
    let ledger = fluree.ledger(late).await.unwrap();
    let emails = results.to_jsonld(&ledger.snapshot).unwrap();
    assert_eq!(
        emails,
        json!([]),
        "the value stays current on the late branch"
    );
}

/// A branch holds an erased commit it reaches only through a merge's other
/// parent, whose `t` is the source branch's: higher than the merge's, and
/// than the branch's head, whatever its head's `t` becomes.
#[tokio::test]
async fn a_branch_reaching_an_erased_commit_only_through_a_merge_holds_it() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let name = "holders/merge-clock";
    let main = "holders/merge-clock:main";
    let side = "holders/merge-clock:side";
    fluree.create_ledger(name).await.unwrap();
    insert(
        &fluree,
        main,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "original"}),
    )
    .await;
    fluree
        .create_branch(name, "side", None, None)
        .await
        .unwrap();
    for n in 2..=5 {
        insert(
            &fluree,
            main,
            json!({"@id": format!("http://example.org/p{n}"), "http://example.org/name": n}),
        )
        .await;
    }
    let update = |value: &str| {
        json!({
            "where": {"@id": "http://example.org/alice", "http://example.org/email": "?old"},
            "delete": {"@id": "http://example.org/alice", "http://example.org/email": "?old"},
            "insert": {"@id": "http://example.org/alice", "http://example.org/email": value}
        })
    };
    let source_t = fluree
        .graph(main)
        .transact()
        .update(&update("a@secret"))
        .commit()
        .await
        .unwrap()
        .receipt
        .t;
    let source = commit_at(&fluree, main, source_t).await;
    fluree
        .graph(side)
        .transact()
        .update(&update("public"))
        .commit()
        .await
        .unwrap();
    // The side keeps its own value: its merge holds none of the secret.
    let merged = fluree
        .merge_branch(
            name,
            "main",
            Some("side"),
            crate::ConflictStrategy::TakeBranch,
        )
        .await
        .unwrap();
    assert!(!merged.fast_forward);
    let record = fluree.nameservice().lookup(side).await.unwrap().unwrap();
    assert!(
        record.commit_t < source_t,
        "{} vs {source_t}",
        record.commit_t
    );
    let store = fluree
        .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
        .await
        .unwrap();
    let head = record.commit_head_id.as_ref().unwrap();
    let merge = fluree_db_core::load_commit_by_id(store.as_ref(), head)
        .await
        .unwrap();
    assert!(merge.parents.contains(&source));
    assert!(!merge
        .flakes
        .iter()
        .any(|f| f.o == fluree_db_core::FlakeValue::String("a@secret".into())));
    assert!(!collect_first_parent_cids(store.as_ref(), head, 0)
        .await
        .unwrap()
        .iter()
        .any(|(_, cid)| cid == &source));

    let receipt = fluree
        .erase_occurrences(main, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let job = fluree.job(main, &receipt.request_id).await.unwrap();
    let holds = async |fluree: &Fluree| {
        fluree
            .holders(main, &job)
            .await
            .unwrap()
            .unwrap()
            .iter()
            .any(|r| r.ledger_id == side)
    };
    assert!(
        holds(&fluree).await,
        "while the side's head is below the source's t"
    );
    for n in 0..6 {
        insert(
            &fluree,
            side,
            json!({"@id": format!("http://example.org/s{n}"), "http://example.org/name": n}),
        )
        .await;
    }
    assert!(holds(&fluree).await, "once the side's head is above it");
}

/// A set keeps the heads its branches had at the last walk: one a branch has
/// moved past is dropped.
#[tokio::test]
async fn an_erased_set_keeps_only_its_branches_current_heads() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let name = "holders/kept-heads";
    let main = "holders/kept-heads:main";
    fluree.create_ledger(name).await.unwrap();
    insert(
        &fluree,
        main,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let receipt = fluree
        .erase_occurrences(main, email("alice", "a@secret"), None)
        .await
        .unwrap();
    let job = fluree.job(main, &receipt.request_id).await.unwrap();
    for n in 0..4 {
        insert(
            &fluree,
            main,
            json!({"@id": format!("http://example.org/s{n}"), "http://example.org/name": n}),
        )
        .await;
        let holders = fluree.holders(main, &job).await.unwrap().unwrap();
        assert!(holders.iter().any(|r| r.ledger_id == main));
    }
    let kept: Vec<usize> = fluree
        .erasure_runtime()
        .unwrap()
        .reached_heads
        .lock()
        .values()
        .map(std::collections::HashMap::len)
        .collect();
    assert_eq!(kept, vec![1]);
}

/// A step whose proof exposed the original's signatures, under an
/// authorization asking it to verify them, is not read in place of the
/// original: the replacement does not verify.
#[tokio::test]
async fn an_exposed_step_authorized_to_verify_does_not_read() {
    use crate::retention::erasure::SignatureMode;
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/exposed").await.unwrap();
    let id = "t/exposed:main";
    let key = Arc::new(ed25519_dalek::SigningKey::from_bytes(&[7; 32]));
    let t1 = fluree
        .graph(id)
        .transact()
        .insert(&json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}))
        .commit_opts(crate::CommitOpts::default().with_signing_key(key))
        .commit()
        .await
        .unwrap()
        .receipt
        .t;
    let original = commit_at(&fluree, id, t1).await;
    let receipt = fluree
        .erase_occurrences(
            id,
            ErasureRequest {
                signatures: SignatureMode::Expose,
                ..email("alice", "a@secret")
            },
            None,
        )
        .await
        .unwrap();
    reindex(&fluree, id).await;
    let report = fluree.run_erasure_pass(id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let runtime = fluree.erasure_runtime().unwrap();
    let mut authorization = runtime
        .index
        .authorization(&receipt.request_id)
        .await
        .unwrap()
        .expect("stored at publication");
    assert_eq!(authorization.signatures, SignatureMode::Expose);
    assert!(reads_replacement(&fluree, id, &original).await);

    authorization.signatures = SignatureMode::Verify;
    runtime
        .index
        .overwrite_authorization(&authorization)
        .await
        .unwrap();
    drop(fluree);
    let restarted = erasing(&dir);
    let record = restarted.nameservice().lookup(id).await.unwrap().unwrap();
    let store = restarted.content_store(&record.storage_namespace());
    assert!(store.get(&original).await.is_err());
}
