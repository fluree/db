//! Commit and transaction garbage: what a failed writer leaves is recorded,
//! checked against every branch, and deleted; what any branch references
//! stays.

use std::sync::Arc;
use std::time::Duration;

use fluree_db_api::{
    CommitOpts, ConflictStrategy, Fluree, FlureeBuilder, GarbageReport, IndexConfig,
    NameServiceMode,
};
use fluree_db_core::{ContentId, ContentKind, ContentStore as _};
use fluree_db_transact::{ir::TxnType, TxnOpts};
use serde_json::{json, Value as JsonValue};

async fn file_fluree(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build_async()
        .await
        .expect("build")
}

fn insert(name: &str) -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": format!("ex:{name}"), "ex:name": name}
    })
}

/// Fails while staging, after its body is stored.
fn failing() -> JsonValue {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "insert": {
            "@id": "ex:x",
            "ex:age": {"@value": "not a number", "@type": "xsd:integer"}
        }
    })
}

/// Transact `body` on `ledger_id`, storing it raw.
async fn transact(
    fluree: &Fluree,
    ledger_id: &str,
    body: &JsonValue,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    let ledger = fluree.ledger(ledger_id).await?;
    fluree
        .transact(
            ledger,
            TxnType::Update,
            body,
            TxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
}

fn body_id(body: &JsonValue) -> ContentId {
    ContentId::new(ContentKind::Txn, &serde_json::to_vec(body).unwrap())
}

async fn stored(fluree: &Fluree, ledger_id: &str, id: &ContentId) -> bool {
    let namespace = fluree.storage_namespace(ledger_id).await.unwrap();
    fluree.content_store(&namespace).has(id).await.unwrap()
}

/// Collect with no grace period until `done` holds of the summed reports;
/// an abandoned upload reaches the collector from a background task.
async fn collect_until(fluree: &Fluree, done: impl Fn(&GarbageReport) -> bool) -> GarbageReport {
    let mut total = GarbageReport::default();
    for _ in 0..100 {
        let report = fluree.collect_garbage(Duration::ZERO).await.unwrap();
        total.recorded += report.recorded;
        total.deleted += report.deleted;
        total.resolved += report.resolved;
        total.deferred += report.deferred;
        if done(&total) {
            return total;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("collection never got there: {total:?}");
}

#[tokio::test]
async fn a_failed_transactions_body_is_collected() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    let kept = insert("alice");
    transact(&fluree, "garbage:main", &kept).await.unwrap();
    transact(&fluree, "garbage:main", &failing())
        .await
        .expect_err("staging fails");

    let report = collect_until(&fluree, |r| r.deleted == 1).await;
    assert_eq!(report.recorded, 1);
    assert!(!stored(&fluree, "garbage:main", &body_id(&failing())).await);
    assert!(stored(&fluree, "garbage:main", &body_id(&kept)).await);
}

#[tokio::test]
async fn a_body_a_published_commit_names_outlives_a_repeat_that_failed() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    let body = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {"@id": "ex:alice", "ex:name": "alice"},
        "delete": {"@id": "ex:alice", "ex:name": "alice"},
        "insert": {"@id": "ex:alice", "ex:name": "Alice"}
    });
    let first = transact(&fluree, "garbage:main", &body).await.unwrap();
    // The same body again matches nothing, so it commits nothing and its
    // upload is abandoned.
    let again = transact(&fluree, "garbage:main", &body).await.unwrap();
    assert_eq!(again.receipt.t, first.receipt.t);

    let report = collect_until(&fluree, |r| r.resolved == 1).await;
    assert_eq!(report.deleted, 0);
    assert!(stored(&fluree, "garbage:main", &body_id(&body)).await);
}

#[tokio::test]
async fn reconciliation_records_a_blob_no_branch_references() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    let kept = insert("alice");
    transact(&fluree, "garbage:main", &kept).await.unwrap();
    // As a crash between storing a body and abandoning it leaves one.
    let namespace = fluree.storage_namespace("garbage:main").await.unwrap();
    let orphan = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"crashed\":true}")
        .await
        .unwrap();

    let report = fluree
        .reconcile_garbage("garbage")
        .await
        .unwrap()
        .expect("settled");
    assert_eq!(report.recorded, 1);
    // Recorded files are not recorded again.
    let again = fluree.reconcile_garbage("garbage").await.unwrap().unwrap();
    assert_eq!(again.recorded, 0);

    collect_until(&fluree, |r| r.deleted == 1).await;
    assert!(!stored(&fluree, "garbage:main", &orphan).await);
    assert!(stored(&fluree, "garbage:main", &body_id(&kept)).await);
}

#[tokio::test]
async fn commits_a_merge_copied_in_stay_after_their_branch_goes() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    fluree
        .create_branch("garbage", "dev", None, None)
        .await
        .unwrap();
    let dev = [insert("bob"), insert("carol")];
    for body in &dev {
        transact(&fluree, "garbage:dev", body).await.unwrap();
    }
    transact(&fluree, "garbage:main", &insert("dave"))
        .await
        .unwrap();
    let merged = fluree
        .merge_branch("garbage", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!merged.fast_forward);
    fluree.drop_branch("garbage", "dev").await.unwrap();

    // The merge above the index, then within it.
    let report = fluree
        .reconcile_garbage("garbage")
        .await
        .unwrap()
        .expect("settled");
    assert_eq!(report.recorded, 0, "{report:?}");
    fluree
        .reindex("garbage:main", fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let report = fluree
        .reconcile_garbage("garbage")
        .await
        .unwrap()
        .expect("settled");
    assert_eq!(report.recorded, 0, "{report:?}");
    for body in &dev {
        assert!(stored(&fluree, "garbage:main", &body_id(body)).await);
    }
    let verified = fluree.verify_ledger("garbage:main", None).await.unwrap();
    assert!(verified.problems.is_empty(), "{:?}", verified.problems);
}

/// The commit files in `ledger_id`'s own folder.
async fn commit_files(fluree: &Fluree, ledger_id: &str) -> Vec<String> {
    let namespace = fluree.storage_namespace(ledger_id).await.unwrap();
    let storage = fluree.admin_storage().unwrap();
    let mut files = storage
        .list_prefix(&format!(
            "fluree:{}://{}/commit/",
            storage.storage_method(),
            namespace.branch_prefix()
        ))
        .await
        .unwrap();
    files.sort();
    files
}

#[tokio::test]
async fn commits_that_lost_their_publish_race_are_collected() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    let before = commit_files(&fluree, "garbage:main").await;
    // Another writer over the same storage, whose every publish loses.
    let racing = Arc::new(crate::race_nameservice::PausingNameService::new(
        fluree.nameservice_mode().publisher_arc().unwrap(),
    ));
    racing.lose_commit_races();
    let losing = Fluree::from_backend(
        fluree.config().clone(),
        fluree.backend().clone(),
        NameServiceMode::ReadWrite(racing),
    );
    let handle = losing.ledger_cached("garbage:main").await.unwrap();
    losing
        .stage(&handle)
        .insert(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:bob",
            "ex:name": "bob"
        }))
        .execute()
        .await
        .expect_err("every publish loses");
    assert!(commit_files(&fluree, "garbage:main").await.len() > before.len());

    collect_until(&fluree, |r| r.deleted > 0).await;
    assert_eq!(commit_files(&fluree, "garbage:main").await, before);
    let verified = fluree.verify_ledger("garbage:main", None).await.unwrap();
    assert!(verified.problems.is_empty(), "{:?}", verified.problems);
}

#[tokio::test]
async fn a_rebase_leaves_the_line_it_replaced_to_the_collector() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    fluree
        .create_branch("garbage", "dev", None, None)
        .await
        .unwrap();
    let dev = [insert("bob"), insert("carol")];
    let mut replaced = Vec::new();
    for body in &dev {
        let result = transact(&fluree, "garbage:dev", body).await.unwrap();
        replaced.push(result.receipt.commit_id);
    }
    transact(&fluree, "garbage:main", &insert("dave"))
        .await
        .unwrap();
    // Through another instance over the same storage, which does not
    // collect but records for the one that does.
    let other = Fluree::from_backend(
        fluree.config().clone(),
        fluree.backend().clone(),
        fluree.nameservice_mode().clone(),
    );
    let report = other
        .rebase_branch("garbage", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    assert_eq!(report.replayed, 2);

    // The replaced commits and their transactions; the replays carry none.
    collect_until(&fluree, |r| r.deleted == 4).await;
    for id in &replaced {
        assert!(!stored(&fluree, "garbage:dev", id).await);
    }
    for body in &dev {
        assert!(!stored(&fluree, "garbage:dev", &body_id(body)).await);
    }
    for ledger in ["garbage:dev", "garbage:main"] {
        let verified = fluree.verify_ledger(ledger, None).await.unwrap();
        assert!(
            verified.problems.is_empty(),
            "{ledger}: {:?}",
            verified.problems
        );
    }
}

#[tokio::test]
async fn what_a_merge_copied_before_losing_its_race_is_collected() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    fluree
        .create_branch("garbage", "dev", None, None)
        .await
        .unwrap();
    let dev = [insert("bob"), insert("carol")];
    for body in &dev {
        transact(&fluree, "garbage:dev", body).await.unwrap();
    }
    transact(&fluree, "garbage:main", &insert("dave"))
        .await
        .unwrap();
    let before = commit_files(&fluree, "garbage:main").await;
    let racing = Arc::new(crate::race_nameservice::PausingNameService::new(
        fluree.nameservice_mode().publisher_arc().unwrap(),
    ));
    racing.lose_commit_races();
    let losing = Fluree::from_backend(
        fluree.config().clone(),
        fluree.backend().clone(),
        NameServiceMode::ReadWrite(racing),
    );
    losing
        .merge_branch("garbage", "dev", None, ConflictStrategy::default())
        .await
        .expect_err("the merge's publish loses");
    assert!(commit_files(&fluree, "garbage:main").await.len() > before.len());

    collect_until(&fluree, |r| r.deleted >= 5).await;
    assert_eq!(commit_files(&fluree, "garbage:main").await, before);
    let namespace = fluree.storage_namespace("garbage:main").await.unwrap();
    for body in &dev {
        // Copied in, and gone again; dev keeps its own.
        assert!(!fluree
            .content_store(&namespace)
            .has(&body_id(body))
            .await
            .unwrap());
        assert!(stored(&fluree, "garbage:dev", &body_id(body)).await);
    }
}

#[tokio::test]
async fn a_commit_a_branch_reads_from_its_parents_folder_stays() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    let before = fluree
        .nameservice()
        .lookup("garbage:main")
        .await
        .unwrap()
        .unwrap();
    let bob = insert("bob");
    let forked_at = transact(&fluree, "garbage:main", &bob).await.unwrap();
    fluree
        .create_branch("garbage", "dev", None, None)
        .await
        .unwrap();
    // Main forgets its last commit; dev still reads it from main's folder.
    fluree
        .branch_admin()
        .unwrap()
        .reset_head_fenced(
            "garbage:main",
            before.fence,
            fluree_db_nameservice::NsRecordSnapshot::from_record(&before),
        )
        .await
        .unwrap();

    let report = fluree.reconcile_garbage("garbage").await.unwrap().unwrap();
    assert_eq!(report.recorded, 0, "{report:?}");

    fluree.drop_branch("garbage", "dev").await.unwrap();
    let report = fluree.reconcile_garbage("garbage").await.unwrap().unwrap();
    assert_eq!(report.recorded, 2, "{report:?}");
    collect_until(&fluree, |r| r.deleted == 2).await;
    assert!(!stored(&fluree, "garbage:main", &forked_at.receipt.commit_id).await);
    assert!(!stored(&fluree, "garbage:main", &body_id(&bob)).await);
}

/// Commit latency with writers pinning their blobs and without, and with
/// the collector and reconciliation running throughout. Every tenth
/// transaction fails, so garbage keeps arriving. Run with
/// `cargo test --release -p fluree-db-api --test grp_ledger
/// it_garbage::commit_latency -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn commit_latency_with_and_without_pins() {
    use fluree_db_core::Durability;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    const WRITERS: usize = 8;
    const COMMITS: usize = 200;
    const ROUNDS: usize = 3;

    async fn lane(durability: Durability, locked: bool, collecting: bool) -> Vec<Duration> {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_storage_durability(durability);
        if !locked {
            builder = builder.without_storage_lock();
        }
        let fluree = Arc::new(builder.build_async().await.unwrap());
        for w in 0..WRITERS {
            fluree.create_ledger(&format!("perf{w}")).await.unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let collector = collecting.then(|| {
            let fluree = Arc::clone(&fluree);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    fluree.collect_garbage(Duration::ZERO).await.unwrap();
                    for w in 0..WRITERS {
                        fluree.reconcile_garbage(&format!("perf{w}")).await.unwrap();
                    }
                }
            })
        });
        let mut writers = Vec::new();
        for w in 0..WRITERS {
            let fluree = Arc::clone(&fluree);
            writers.push(tokio::spawn(async move {
                let id = format!("perf{w}:main");
                let mut latencies = Vec::with_capacity(COMMITS);
                for i in 0..COMMITS {
                    let body = if i % 10 == 9 {
                        json!({
                            "@context": {"ex": "http://example.org/ns/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
                            "insert": {"@id": format!("ex:f{w}-{i}"), "ex:age": {"@value": "x", "@type": "xsd:integer"}}
                        })
                    } else {
                        insert(&format!("w{w}-{i}"))
                    };
                    let started = Instant::now();
                    let result = fluree
                        .graph(&id)
                        .transact()
                        .txn_opts(TxnOpts::default().store_raw_txn(true))
                        .update(&body)
                        .commit()
                        .await;
                    if result.is_ok() && i >= 20 {
                        latencies.push(started.elapsed());
                    }
                }
                latencies
            }));
        }
        let mut latencies = Vec::new();
        for writer in writers {
            latencies.extend(writer.await.unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        if let Some(collector) = collector {
            collector.await.unwrap();
        }
        latencies
    }

    fn summary(label: &str, mut latencies: Vec<Duration>) {
        latencies.sort();
        let at = |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize];
        let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        println!(
            "{label:<32} n={:<5} mean={:>9.1?} p50={:>9.1?} p95={:>9.1?} p99={:>9.1?}",
            latencies.len(),
            mean,
            at(0.50),
            at(0.95),
            at(0.99)
        );
    }

    for durability in [Durability::PageCache, Durability::Wal] {
        let lanes = [
            ("unpinned", false, false),
            ("pinned", true, false),
            ("pinned, collecting", true, true),
        ];
        let mut results: Vec<Vec<Duration>> = vec![Vec::new(); lanes.len()];
        for _ in 0..ROUNDS {
            for (i, (_, locked, collecting)) in lanes.iter().enumerate() {
                results[i].extend(lane(durability, *locked, *collecting).await);
            }
        }
        for ((label, _, _), latencies) in lanes.iter().zip(results) {
            summary(&format!("{durability:?} {label}"), latencies);
        }
    }
}
