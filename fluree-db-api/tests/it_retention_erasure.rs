//! Permanent erasure through proof-verified replacements: a request removes
//! the occurrences on every branch that holds them, records its
//! authorization, and once its proofs verify and its commits are indexed,
//! every read of an affected commit reads its replacement — across restarts,
//! reindexing and raw retrieval — while every other commit and CID stays as
//! it was.

use std::sync::Arc;

use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, ErasureStage, Verifier};
use fluree_db_api::retention::FactSelector;
use fluree_db_api::{ApiError, Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_core::commit::codec::replacement::is_replacement;
use fluree_db_core::{collect_first_parent_cids, load_commit_by_id, ContentId, ContentStore};
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/"})
}

fn erasing(dir: &tempfile::TempDir, prover: Arc<NativeProver>) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(ErasureConfig {
            prover,
            verifier: Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build()
        .unwrap()
}

async fn ledger(fluree: &Fluree, name: &str) -> String {
    fluree.create_ledger(name).await.expect("create");
    format!("{name}:main")
}

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .expect("insert")
        .receipt
        .t
}

async fn objects(fluree: &Fluree, ledger_id: &str, s: &str, p: &str) -> Vec<JsonValue> {
    let query = json!({
        "@context": ctx(),
        "from": ledger_id,
        "select": "?o",
        "where": {"@id": s, p: "?o"}
    });
    let results = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    results
        .to_jsonld(&ledger.snapshot)
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default()
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

async fn store_of(fluree: &Fluree, ledger_id: &str) -> Arc<dyn ContentStore> {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    fluree.content_store(&record.storage_namespace())
}

/// The ledger's commits, head first.
async fn chain(fluree: &Fluree, ledger_id: &str) -> Vec<(i64, ContentId)> {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let store = store_of(fluree, ledger_id).await;
    collect_first_parent_cids(store.as_ref(), record.commit_head_id.as_ref().unwrap(), 0)
        .await
        .unwrap()
}

async fn commit_at(fluree: &Fluree, ledger_id: &str, t: i64) -> ContentId {
    chain(fluree, ledger_id)
        .await
        .into_iter()
        .find(|(ct, _)| *ct == t)
        .unwrap()
        .1
}

async fn reindex(fluree: &Fluree, ledger_id: &str) {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
}

/// The file under `root` holding commit `cid`.
fn commit_file(root: &std::path::Path, cid: &ContentId) -> Option<std::path::PathBuf> {
    let name = format!("{}.fcv2", cid.address_digest());
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name().is_some_and(|n| n == name.as_str()) {
                return Some(path);
            }
        }
    }
    None
}

fn contains(bytes: &[u8], needle: &str) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
}

/// History entries in the default graph's index sidecars, read straight
/// from storage, past any serving check.
async fn indexed_history_entries(fluree: &Fluree, ledger_id: &str) -> usize {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let root_id = record.index_head_id.clone().expect("indexed");
    let store = fluree.content_store(&record.storage_namespace());
    let root_bytes = store.get(&root_id).await.unwrap();
    let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&root_bytes).unwrap();
    let mut entries = 0;
    for order in &root.default_graph_orders {
        for leaf in &order.leaves {
            let Some(cid) = &leaf.sidecar_cid else {
                continue;
            };
            let bytes = store.get(cid).await.unwrap();
            let mut pos = 8;
            while pos + 4 <= bytes.len() {
                let n = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                entries += n;
                pos += 4 + n * 31;
            }
        }
    }
    entries
}

#[tokio::test]
async fn an_erasure_replaces_its_commits_everywhere_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let prover = Arc::new(NativeProver::default());
    let fluree = erasing(&dir, prover.clone());
    let id = ledger(&fluree, "it/erase").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "alice@secret.example", "ex:name": "Alice"},
            {"@id": "ex:bob", "ex:email": "bob@kept.example"}
        ]}),
    )
    .await;
    let original_t1 = commit_at(&fluree, &id, t1).await;

    let receipt = fluree
        .erase_occurrences(
            &id,
            email("alice", "alice@secret.example"),
            Some("did:example:officer".into()),
        )
        .await
        .expect("erase");
    // The assertion, and the retraction the removal made of it.
    assert_eq!(receipt.manifest.len(), 2, "{receipt:?}");
    assert_eq!(receipt.commits, 2);
    assert_eq!(receipt.stage, ErasureStage::Proving);
    // Its authorization is a control commit naming the request and its
    // requester, and no value.
    let store = store_of(&fluree, &id).await;
    let authorization = load_commit_by_id(
        store.as_ref(),
        &commit_at(&fluree, &id, receipt.authorized_t).await,
    )
    .await
    .unwrap();
    assert!(authorization.flakes.is_empty());
    let meta = format!("{:?}", authorization.txn_meta);
    assert!(meta.contains(&receipt.request_id) && meta.contains("did:example:officer"));
    assert!(!meta.contains("secret"));
    // Removed from queries at once.
    assert!(objects(&fluree, &id, "ex:alice", "ex:email")
        .await
        .is_empty());
    let heads_before = chain(&fluree, &id).await;

    // Proved and verified, but not published before an index covers it.
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!((report.proved, report.published), (2, 0), "{report:?}");
    assert!(!is_replacement(&store.get(&original_t1).await.unwrap()));
    reindex(&fluree, &id).await;
    assert!(
        indexed_history_entries(&fluree, &id).await > 0,
        "the index built from the originals holds the removed email's history"
    );
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let status = fluree.retention_status(&id).await.unwrap();
    let job = &status.registration.as_ref().unwrap().erasures[0];
    assert_eq!(job.stage, ErasureStage::Published);
    assert!(job.commits.iter().all(|c| c.proved.is_some()));

    // Every read of the commit is its replacement: raw bytes included.
    let raw = store.get(&original_t1).await.unwrap().into_vec();
    assert!(is_replacement(&raw));
    assert!(!contains(&raw, "alice@secret.example"));
    assert!(contains(&raw, "bob@kept.example"));
    let decoded = load_commit_by_id(store.as_ref(), &original_t1)
        .await
        .unwrap();
    assert_eq!(
        decoded.id.as_ref(),
        Some(&original_t1),
        "it keeps its identity"
    );
    assert!(decoded.redaction.is_some());
    // No CID changed, and the chain walks as before.
    assert_eq!(chain(&fluree, &id).await, heads_before);
    // Current state, as before.
    assert_eq!(
        objects(&fluree, &id, "ex:bob", "ex:email").await,
        vec![json!("bob@kept.example")]
    );

    // A restarted instance, with no prover, reads the replacements; an
    // index rebuilt from them holds no history of the erased value.
    drop(fluree);
    let restarted = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let store = store_of(&restarted, &id).await;
    // Its verifier pins the production images, so it refuses this test's
    // fake receipts rather than read the original.
    assert!(store.get(&original_t1).await.is_err());
    drop(restarted);
    let restarted = erasing(&dir, Arc::new(NativeProver::default()));
    let store = store_of(&restarted, &id).await;
    assert!(is_replacement(&store.get(&original_t1).await.unwrap()));
    reindex(&restarted, &id).await;
    assert_eq!(indexed_history_entries(&restarted, &id).await, 0);
    assert!(objects(&restarted, &id, "ex:alice", "ex:email")
        .await
        .is_empty());
    assert_eq!(
        objects(&restarted, &id, "ex:alice", "ex:name").await,
        vec![json!("Alice")]
    );
    assert_eq!(prover.proved().len(), 2);
}

#[tokio::test]
async fn a_failed_step_is_retried_and_a_restart_resumes_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let prover = Arc::new(NativeProver::default());
    let fluree = erasing(&dir, prover.clone());
    let id = ledger(&fluree, "it/erase-retry").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    prover.fail_next(1);
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.failed, 1, "{report:?}");
    let status = fluree.retention_status(&id).await.unwrap();
    let job = status.registration.unwrap().erasures[0].clone();
    assert_eq!((job.stage, job.retries), (ErasureStage::Proving, 1));
    assert!(job.last_error.is_some() && job.retry_at_ms.is_some());

    // Restarted before the retry: the job is where it was left.
    drop(fluree);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!((report.proved, report.published), (2, 1), "{report:?}");
    // Asked again, nothing of it is left to erase.
    let again = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await;
    assert!(
        matches!(again, Err(ApiError::Http { status: 404, .. })),
        "{again:?}"
    );
}

#[tokio::test]
async fn a_second_erasure_of_a_commit_builds_on_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-twice").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:carol", "ex:email": "c@secret"},
            {"@id": "ex:bob", "ex:email": "b@kept"}
        ]}),
    )
    .await;
    let original = commit_at(&fluree, &id, t1).await;
    for who in [("alice", "a@secret"), ("carol", "c@secret")] {
        fluree
            .erase_occurrences(&id, email(who.0, who.1), None)
            .await
            .expect("erase");
        reindex(&fluree, &id).await;
        let report = fluree.run_erasure_pass(&id).await.unwrap();
        assert_eq!(report.published, 1, "{report:?}");
    }
    // A fresh instance verifies the two-step chain before reading it.
    drop(fluree);
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let store = store_of(&fluree, &id).await;
    let raw = store.get(&original).await.unwrap().into_vec();
    assert!(!contains(&raw, "a@secret") && !contains(&raw, "c@secret"));
    assert!(contains(&raw, "b@kept"));
    let decoded = load_commit_by_id(store.as_ref(), &original).await.unwrap();
    assert_eq!(decoded.flakes.len(), 1);
}

#[tokio::test]
async fn an_erasure_reaches_every_branch_holding_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-branches").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    fluree
        .create_branch("it/erase-branches", "dev", None, None)
        .await
        .expect("branch");
    let dev = "it/erase-branches:dev";
    let shared = commit_at(&fluree, &id, t1).await;

    let receipt = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    assert_eq!(receipt.removals.len(), 2, "both branches: {receipt:?}");
    // The shared commit once, and each branch's own retraction.
    assert_eq!(receipt.commits, 3, "{receipt:?}");
    assert!(objects(&fluree, dev, "ex:alice", "ex:email")
        .await
        .is_empty());

    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 0, "dev is not indexed yet: {report:?}");
    reindex(&fluree, dev).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let dev_store = store_of(&fluree, dev).await;
    assert!(is_replacement(&dev_store.get(&shared).await.unwrap()));
}

#[tokio::test]
async fn an_erased_commit_is_neither_written_back_nor_transferred() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-transfer").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let cid = commit_at(&fluree, &id, t1).await;
    let store = store_of(&fluree, &id).await;
    let original = store.get(&cid).await.unwrap().into_vec();
    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    reindex(&fluree, &id).await;
    assert_eq!(fluree.run_erasure_pass(&id).await.unwrap().published, 1);

    // Its original gone (as a purge will leave it), writing it back (an
    // import, a restore) writes nothing.
    let file = commit_file(dir.path(), &cid).expect("the original's file");
    std::fs::remove_file(&file).unwrap();
    store.put_with_id(&cid, &original).await.unwrap();
    assert!(!file.exists(), "the original was written back");
    assert!(is_replacement(&store.get(&cid).await.unwrap()));
    // An archive would carry the replacement as the commit: refused.
    let mut archive = Vec::new();
    let refused = fluree.archive_ledger(&id, false, &mut archive).await;
    assert!(
        matches!(&refused, Err(e) if e.to_string().contains("permanently erased")),
        "{refused:?}"
    );
}

#[tokio::test]
async fn erasure_is_refused_outside_its_scope() {
    let request = || email("alice", "a@secret");
    // Memory storage.
    let memory = FlureeBuilder::memory().build_memory();
    let id = ledger(&memory, "it/erase-memory").await;
    insert(
        &memory,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let refused = memory.erase_occurrences(&id, request(), None).await;
    assert!(
        matches!(refused, Err(ApiError::Http { status: 501, .. })),
        "{refused:?}"
    );
    // File storage with no prover.
    let dir = tempfile::tempdir().unwrap();
    let file = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let id = ledger(&file, "it/erase-noprover").await;
    insert(
        &file,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let refused = file.erase_occurrences(&id, request(), None).await;
    assert!(
        matches!(refused, Err(ApiError::Http { status: 501, .. })),
        "{refused:?}"
    );
    // Nothing was removed by a refused request.
    assert_eq!(
        objects(&file, &id, "ex:alice", "ex:email").await,
        vec![json!("a@secret")]
    );
}

/// A named assertion takes the retraction that ended it (and a named
/// retraction the assertion it ended): both carry the value, and erasing
/// one alone would change what replaying the other states.
#[tokio::test]
async fn a_named_occurrence_takes_the_one_bounding_its_span() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-span").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@old"}),
    )
    .await;
    let t2 = fluree
        .graph(&id)
        .transact()
        .upsert(&json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@new"}))
        .commit()
        .await
        .unwrap()
        .receipt
        .t;
    let store = store_of(&fluree, &id).await;
    let asserted = commit_at(&fluree, &id, t1).await;
    let retracted = commit_at(&fluree, &id, t2).await;
    let commit = load_commit_by_id(store.as_ref(), &asserted).await.unwrap();
    let slot = commit
        .occurrences()
        .find(|(_, f)| f.op && format!("{:?}", f.o).contains("a@old"))
        .unwrap()
        .0;
    let receipt = fluree
        .erase_occurrences(
            &id,
            ErasureRequest {
                occurrences: vec![fluree_db_api::retention::OccurrenceRef {
                    commit: asserted.to_string(),
                    slot,
                }],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("erase");
    let commits: Vec<&str> = receipt.manifest.iter().map(|o| o.commit.as_str()).collect();
    assert_eq!(receipt.manifest.len(), 2, "{receipt:?}");
    assert!(commits.contains(&asserted.to_string().as_str()));
    assert!(commits.contains(&retracted.to_string().as_str()));
    // The current value is not part of it.
    assert_eq!(
        objects(&fluree, &id, "ex:alice", "ex:email").await,
        vec![json!("a@new")]
    );
}

/// A branch made while an erasure is proved holds its commits above that
/// branch's index: publication waits for the branch's index too, or its
/// indexed assertion would replay without the erased retraction.
#[tokio::test]
async fn a_branch_made_while_proving_is_indexed_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-late-branch").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    reindex(&fluree, &id).await;
    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    fluree
        .create_branch("it/erase-late-branch", "late", None, None)
        .await
        .expect("branch");
    let late = "it/erase-late-branch:late";
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(
        (report.proved, report.published),
        (2, 0),
        "late is not indexed yet: {report:?}"
    );
    reindex(&fluree, late).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    assert!(objects(&fluree, late, "ex:alice", "ex:email")
        .await
        .is_empty());
    drop(fluree);
    let restarted = erasing(&dir, Arc::new(NativeProver::default()));
    assert!(objects(&restarted, late, "ex:alice", "ex:email")
        .await
        .is_empty());
}

/// A branch made while a request is accepted is made after it, holding its
/// removal; made before the removal, the request would miss it.
#[tokio::test]
async fn a_branch_made_during_acceptance_holds_the_removal() {
    use fluree_db_api::retention::{outcome_of, RetentionBatch, RetentionWriter, SubmitOutcome};
    type Branching = tokio::task::JoinHandle<fluree_db_api::Result<()>>;
    /// Starts making a branch as the removal commits.
    struct Brancher(Arc<Fluree>, std::sync::Mutex<Option<Branching>>);
    #[async_trait::async_trait]
    impl RetentionWriter for Brancher {
        async fn submit(
            &self,
            ledger_id: &str,
            batch: RetentionBatch,
        ) -> fluree_db_api::Result<SubmitOutcome> {
            if !batch.retractions.is_empty() {
                let fluree = self.0.clone();
                let mut branching = tokio::spawn(async move {
                    fluree
                        .create_branch("it/erase-admission", "late", None, None)
                        .await
                        .map(|_| ())
                });
                // Made now unless it waits for the request.
                let _ = tokio::time::timeout(std::time::Duration::from_millis(300), &mut branching)
                    .await;
                *self.1.lock().unwrap() = Some(branching);
            }
            outcome_of(self.0.commit_retention_batch(ledger_id, batch).await)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(erasing(&dir, Arc::new(NativeProver::default())));
    let id = ledger(&fluree, "it/erase-admission").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    reindex(&fluree, &id).await;
    let writer = Brancher(fluree.clone(), std::sync::Mutex::new(None));
    fluree
        .erase_occurrences_with(&writer, &id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    let branching = writer.1.lock().unwrap().take().expect("branching");
    branching.await.unwrap().expect("branch");
    let late = "it/erase-admission:late";
    assert!(objects(&fluree, late, "ex:alice", "ex:email")
        .await
        .is_empty());
    reindex(&fluree, &id).await;
    reindex(&fluree, late).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    drop(writer);
    let fluree = Arc::into_inner(fluree).expect("sole owner");
    drop(fluree);
    let restarted = erasing(&dir, Arc::new(NativeProver::default()));
    assert!(objects(&restarted, late, "ex:alice", "ex:email")
        .await
        .is_empty());
}

/// The removal's own retraction carries the value: it is erased however
/// far the head has moved by the time the removal looks for it.
#[tokio::test]
async fn the_removals_retraction_is_erased_after_the_head_moves_on() {
    use fluree_db_api::retention::{outcome_of, RetentionBatch, RetentionWriter, SubmitOutcome};
    /// Commits another transaction right after each retraction.
    struct Busy<'a>(&'a Fluree);
    #[async_trait::async_trait]
    impl RetentionWriter for Busy<'_> {
        async fn submit(
            &self,
            ledger_id: &str,
            batch: RetentionBatch,
        ) -> fluree_db_api::Result<SubmitOutcome> {
            let retracting = !batch.retractions.is_empty();
            let outcome = outcome_of(self.0.commit_retention_batch(ledger_id, batch).await);
            if retracting && outcome.is_ok() {
                insert(
                    self.0,
                    ledger_id,
                    json!({"@context": ctx(), "@id": "ex:other", "ex:name": "Other"}),
                )
                .await;
            }
            outcome
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-busy").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let receipt = fluree
        .erase_occurrences_with(&Busy(&fluree), &id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    let retraction = commit_at(&fluree, &id, t1 + 1).await;
    assert_eq!(receipt.commits, 2, "{receipt:?}");
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let raw = store_of(&fluree, &id)
        .await
        .get(&retraction)
        .await
        .unwrap()
        .into_vec();
    assert!(is_replacement(&raw) && !contains(&raw, "a@secret"));
}

/// Another instance open on the same storage, a query peer, reads the
/// replacement once the erasure reports published.
#[tokio::test]
async fn a_reader_open_before_publication_reads_the_replacement() {
    use fluree_db_api::NameServiceMode;
    use fluree_db_nameservice::file::FileNameService;
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-reader").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let cid = commit_at(&fluree, &id, t1).await;
    let reader = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(ErasureConfig {
            prover: Arc::new(NativeProver::default()),
            verifier: Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build_client_with_nameservice(NameServiceMode::ReadOnly(Arc::new(FileNameService::new(
            dir.path(),
        ))))
        .await
        .unwrap();
    let record = fluree.nameservice().lookup(&id).await.unwrap().unwrap();
    let store = reader.content_store(&record.storage_namespace());
    assert!(!is_replacement(&store.get(&cid).await.unwrap()));

    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let raw = store.get(&cid).await.unwrap().into_vec();
    assert!(is_replacement(&raw) && !contains(&raw, "a@secret"));
}

/// A request overlapping one still in progress completes only once the
/// published erasures remove all it asks for.
#[tokio::test]
async fn an_overlapping_request_completes_with_the_one_ahead_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let prover = Arc::new(NativeProver::default());
    let fluree = erasing(&dir, prover.clone());
    let id = ledger(&fluree, "it/erase-overlap").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:carol", "ex:email": "c@secret"}
        ]}),
    )
    .await;
    let original = commit_at(&fluree, &id, t1).await;
    let mut both = email("alice", "a@secret");
    both.matching.extend(email("carol", "c@secret").matching);
    fluree
        .erase_occurrences(&id, both, None)
        .await
        .expect("erase");
    let second = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    assert_eq!(
        (second.stage, second.commits),
        (ErasureStage::Proving, 2),
        "{second:?}"
    );

    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    // The first request's steps remove what the second asks for.
    assert_eq!((report.proved, report.published), (2, 2), "{report:?}");
    let status = fluree.retention_status(&id).await.unwrap();
    let jobs = status.registration.unwrap().erasures;
    assert!(jobs.iter().all(|j| j.stage == ErasureStage::Published));
    let raw = store_of(&fluree, &id)
        .await
        .get(&original)
        .await
        .unwrap()
        .into_vec();
    assert!(!contains(&raw, "a@secret") && !contains(&raw, "c@secret"));
}

/// When the request ahead of it fails, an overlapping request proves what
/// it asks for itself.
#[tokio::test]
async fn an_overlapping_request_proves_what_a_failed_one_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let prover = Arc::new(NativeProver::default());
    let fluree = erasing(&dir, prover.clone());
    let id = ledger(&fluree, "it/erase-overlap-failed").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:carol", "ex:email": "c@secret"}
        ]}),
    )
    .await;
    let original = commit_at(&fluree, &id, t1).await;
    let mut both = email("alice", "a@secret");
    both.matching.extend(email("carol", "c@secret").matching);
    let first = fluree
        .erase_occurrences(&id, both, None)
        .await
        .expect("erase");
    let second = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");

    prover.refuse_next(1);
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(
        (report.failed, report.proved, report.published),
        (1, 2, 1),
        "{report:?}"
    );
    let status = fluree.retention_status(&id).await.unwrap();
    let jobs = status.registration.unwrap().erasures;
    let stage = |id: &str| jobs.iter().find(|j| j.request_id == id).unwrap().stage;
    assert_eq!(stage(&first.request_id), ErasureStage::Failed);
    assert_eq!(stage(&second.request_id), ErasureStage::Published);
    let raw = store_of(&fluree, &id)
        .await
        .get(&original)
        .await
        .unwrap()
        .into_vec();
    assert!(!contains(&raw, "a@secret") && contains(&raw, "c@secret"));
}

/// A signed commit, through the whole workflow in either signature mode:
/// the proof verifies the original's signatures, or exposes them for the
/// verifier, and the replacement reads, replays and reindexes like any
/// other.
#[tokio::test]
async fn a_signed_commit_is_erased_in_either_signature_mode() {
    use fluree_db_api::retention::erasure::SignatureMode;
    for (n, mode) in [SignatureMode::Verify, SignatureMode::Expose]
        .into_iter()
        .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        let prover = Arc::new(NativeProver::default());
        let fluree = erasing(&dir, prover.clone());
        let id = ledger(&fluree, &format!("it/erase-signed-{n}")).await;
        let key = Arc::new(ed25519_dalek::SigningKey::from_bytes(&[7; 32]));
        let t1 = fluree
            .graph(&id)
            .transact()
            .insert(&json!({"@context": ctx(), "@graph": [
                {"@id": "ex:alice", "ex:email": "a@secret"},
                {"@id": "ex:bob", "ex:email": "b@kept"}
            ]}))
            .commit_opts(fluree_db_api::CommitOpts::default().with_signing_key(key))
            .commit()
            .await
            .unwrap()
            .receipt
            .t;
        let original = commit_at(&fluree, &id, t1).await;
        let store = store_of(&fluree, &id).await;
        let signed = load_commit_by_id(store.as_ref(), &original).await.unwrap();
        assert_eq!(signed.commit_signatures.len(), 1);

        fluree
            .erase_occurrences(
                &id,
                ErasureRequest {
                    signatures: mode,
                    ..email("alice", "a@secret")
                },
                None,
            )
            .await
            .expect("erase");
        reindex(&fluree, &id).await;
        let report = fluree.run_erasure_pass(&id).await.unwrap();
        assert_eq!(
            (report.proved, report.published),
            (2, 1),
            "{mode:?}: {report:?}"
        );
        let proved = prover.proved();
        let step = proved
            .iter()
            .find(|r| commit_signed(&r.input))
            .expect("the signed commit's step");
        assert_eq!(step.signatures, mode.into());

        drop(fluree);
        let restarted = erasing(&dir, Arc::new(NativeProver::default()));
        let raw = store_of(&restarted, &id)
            .await
            .get(&original)
            .await
            .unwrap()
            .into_vec();
        assert!(is_replacement(&raw) && !contains(&raw, "a@secret") && contains(&raw, "b@kept"));
        reindex(&restarted, &id).await;
        assert!(objects(&restarted, &id, "ex:alice", "ex:email")
            .await
            .is_empty());
        assert_eq!(
            objects(&restarted, &id, "ex:bob", "ex:email").await,
            vec![json!("b@kept")]
        );
    }
}

/// Whether a commit blob carries a commit signature.
fn commit_signed(bytes: &[u8]) -> bool {
    fluree_db_core::commit::codec::read_commit(bytes).is_ok_and(|c| !c.commit_signatures.is_empty())
}

/// A ledger's erasure jobs survive its drop and restore, across a restart:
/// a published one stays published, and a pending one resumes.
#[tokio::test]
async fn erasures_survive_a_drop_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-restored").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:carol", "ex:email": "c@secret"}
        ]}),
    )
    .await;
    let shared = commit_at(&fluree, &id, t1).await;
    let first = fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    reindex(&fluree, &id).await;
    assert_eq!(fluree.run_erasure_pass(&id).await.unwrap().published, 1);
    let second = fluree
        .erase_occurrences(&id, email("carol", "c@secret"), None)
        .await
        .expect("erase");
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!((report.proved, report.published), (2, 0), "{report:?}");

    let dropped = fluree
        .drop_ledger("it/erase-restored", fluree_db_api::DropMode::Soft)
        .await
        .unwrap();
    drop(fluree);
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    fluree
        .restore_dropped(&dropped.instance.unwrap().to_string())
        .await
        .unwrap();
    let status = fluree.retention_status(&id).await.unwrap();
    let jobs = status.registration.expect("registration").erasures;
    let stage = |request: &str| jobs.iter().find(|j| j.request_id == request).unwrap().stage;
    assert_eq!(stage(&first.request_id), ErasureStage::Published);
    assert!(jobs
        .iter()
        .any(|j| j.request_id == second.request_id && j.pending()));

    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
    let raw = store_of(&fluree, &id)
        .await
        .get(&shared)
        .await
        .unwrap()
        .into_vec();
    assert!(!contains(&raw, "a@secret") && !contains(&raw, "c@secret"));
}

/// After a restore, the retention worker resumes the ledger's pending
/// erasure without waiting for its periodic rescan of registrations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_worker_resumes_a_restored_ledgers_erasure() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        Arc::new(
            FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                .with_indexer_catchup_interval(Duration::from_millis(200))
                .with_erasure(ErasureConfig {
                    prover: Arc::new(NativeProver::default()),
                    verifier: Arc::new(Verifier::accepting_dev_receipts()),
                    retry_ms: 10,
                })
                .build()
                .unwrap(),
        )
    };
    let fluree = open();
    let id = ledger(&fluree, "it/erase-worker-restored").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let asserted = commit_at(&fluree, &id, t1).await;
    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    // Indexed through its head, so no build after the restore registers it.
    reindex(&fluree, &id).await;
    let dropped = fluree
        .drop_ledger("it/erase-worker-restored", fluree_db_api::DropMode::Soft)
        .await
        .unwrap();
    drop(fluree);

    // Restarted, the worker's first rescan finds no such ledger; its next
    // is minutes away.
    let fluree = open();
    assert!(fluree.start_maintenance(
        None,
        Some(fluree_db_api::retention::RetentionOptions {
            interval: Duration::from_millis(100),
            ..Default::default()
        })
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    fluree
        .restore_dropped(&dropped.instance.unwrap().to_string())
        .await
        .unwrap();

    let mut stage = ErasureStage::Proving;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = fluree.retention_status(&id).await.unwrap();
        stage = status.registration.unwrap().erasures[0].stage;
        if stage == ErasureStage::Published {
            break;
        }
    }
    assert_eq!(stage, ErasureStage::Published);
    let raw = store_of(&fluree, &id)
        .await
        .get(&asserted)
        .await
        .unwrap()
        .into_vec();
    assert!(is_replacement(&raw));
}

/// The whole workflow with the real prover and the production verifier, on a
/// signed commit of 1,000 operations whose signature the proof verifies: run
/// with `FLUREE_TEST_PROVER` naming a `fluree-prover` built from this tree.
#[tokio::test]
#[ignore = "needs a built fluree-prover (FLUREE_TEST_PROVER)"]
async fn the_real_prover_erases_under_a_pinned_image() {
    let command = std::env::var_os("FLUREE_TEST_PROVER").expect("FLUREE_TEST_PROVER");
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let config = || {
        ErasureConfig::subprocess(fluree_db_api::retention::erasure::ProverConfig {
            command: command.clone().into(),
            scratch_dir: scratch.path().to_path_buf(),
            threads: None,
            niceness: 10,
            timeout: std::time::Duration::from_secs(3_600),
        })
    };
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(config())
        .build()
        .unwrap();
    let id = ledger(&fluree, "it/erase-real").await;
    let people: Vec<JsonValue> = (0..500)
        .map(|i| json!({"@id": format!("ex:p{i}"), "ex:email": format!("p{i}@secret.example"), "ex:name": format!("Person {i}")}))
        .collect();
    let key = Arc::new(ed25519_dalek::SigningKey::from_bytes(&[7; 32]));
    let t1 = fluree
        .graph(&id)
        .transact()
        .insert(&json!({"@context": ctx(), "@graph": people}))
        .commit_opts(fluree_db_api::CommitOpts::default().with_signing_key(key))
        .commit()
        .await
        .unwrap()
        .receipt
        .t;
    let original = commit_at(&fluree, &id, t1).await;
    fluree
        .erase_occurrences(&id, email("p0", "p0@secret.example"), None)
        .await
        .expect("erase");
    reindex(&fluree, &id).await;
    let started = std::time::Instant::now();
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    eprintln!(
        "proved and published in {:?}: {report:?}",
        started.elapsed()
    );
    assert_eq!((report.proved, report.published), (2, 1), "{report:?}");
    let status = fluree.retention_status(&id).await.unwrap();
    for task in &status.registration.unwrap().erasures[0].commits {
        eprintln!("{}: {:?}", task.commit, task.proved);
    }
    // Scratch is gone, and a restart verifies the real receipts again.
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    drop(fluree);
    let restarted = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let raw = store_of(&restarted, &id)
        .await
        .get(&original)
        .await
        .unwrap()
        .into_vec();
    assert!(is_replacement(&raw) && !contains(&raw, "p0@secret.example"));
    assert!(contains(&raw, "p1@secret.example"));
}

/// A copy of the ledger in the same storage, restored from an archive,
/// holds the erased commits too and reads their replacements: the request
/// removes the value there, and publication waits for the copy's index, or
/// the copy would go on serving the value it indexed.
#[tokio::test]
async fn an_erasure_reaches_a_copy_restored_from_an_archive() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/erase-copied").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("it/erase-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&fluree, &copy).await;

    fluree
        .erase_occurrences(&id, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    assert!(objects(&fluree, &copy, "ex:alice", "ex:email")
        .await
        .is_empty());
    reindex(&fluree, &id).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(
        report.published, 0,
        "the copy is not indexed yet: {report:?}"
    );
    reindex(&fluree, &copy).await;
    let report = fluree.run_erasure_pass(&id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");

    drop(fluree);
    let restarted = erasing(&dir, Arc::new(NativeProver::default()));
    assert!(objects(&restarted, &copy, "ex:alice", "ex:email")
        .await
        .is_empty());
    let past = restarted
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{copy}@t:{t1}"),
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
}
