//! What a purge captures, and from which bytes.

use std::sync::Arc;

use fluree_db_core::FlakeValue;
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

use crate::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
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

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
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

async fn erase(fluree: &Fluree, ledger_id: &str, request: ErasureRequest) {
    fluree
        .erase_occurrences(ledger_id, request, None)
        .await
        .unwrap();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    assert_eq!(
        fluree.run_erasure_pass(ledger_id).await.unwrap().published,
        1
    );
}

fn holds(captured: &[super::Captured], value: &str) -> bool {
    captured
        .iter()
        .any(|c| matches!(&c.flake.o, FlakeValue::String(s) if s == value))
}

/// A transaction document written back after a purge's scan, as a stopped
/// member's log replays it, is deleted with the originals, and found once
/// they are gone: by the folders the purge recorded deleting them from.
#[tokio::test]
async fn a_document_written_back_after_the_scan_is_purged() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/replayed").await.unwrap();
    let id = "t/replayed:main";
    fluree
        .graph(id)
        .transact()
        .txn_opts(fluree_db_transact::TxnOpts::default().store_raw_txn(true))
        .insert(&json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}))
        .commit()
        .await
        .unwrap();
    erase(&fluree, id, email("alice", "a@secret")).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    assert_eq!(plan.transactions.len(), 1, "{plan:?}");
    let document = dir
        .path()
        .join(plan.transactions[0].strip_prefix("fluree:file://").unwrap());
    let bytes = std::fs::read(&document).unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("a@secret"));

    // The pass scanned and deleted the originals; then the document came
    // back.
    let request = fluree
        .retention_status(id)
        .await
        .unwrap()
        .registration
        .unwrap()
        .erasures[0]
        .request_id
        .clone();
    fluree
        .update_job(id, &request, |j| {
            j.stage = super::super::ErasureStage::Purging;
            let p = j.purge.get_or_insert_with(Default::default);
            p.rebuilt_ms = Some(1);
            p.scanned_ms = Some(1);
        })
        .await
        .unwrap();
    let runtime = fluree.erasure_runtime().unwrap();
    runtime
        .index
        .record_replaced_at(&plan.originals)
        .await
        .unwrap();
    for address in plan.originals.iter().chain(&plan.transactions) {
        runtime.index.delete_raw(address).await.unwrap();
    }
    std::fs::write(&document, &bytes).unwrap();
    assert_eq!(
        fluree.plan_purge(id).await.unwrap().transactions,
        plan.transactions
    );

    let report = fluree
        .run_purge_pass(id, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert!(!document.exists());
}

/// On a cluster a purge deletes only within its leader's lease: deposed
/// after it began, it deletes nothing more, a new leader's indexes among
/// what it would have.
#[tokio::test]
async fn a_deposed_leaders_purge_deletes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/deposed").await.unwrap();
    let id = "t/deposed:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    erase(&fluree, id, email("alice", "a@secret")).await;
    let request = fluree
        .retention_status(id)
        .await
        .unwrap()
        .registration
        .unwrap()
        .erasures[0]
        .request_id
        .clone();
    fluree
        .update_job(id, &request, |j| {
            j.cluster = Some(super::super::ClusterPublication {
                manifest: "m".into(),
                activation: Some(0),
                effective_ms: Some(1),
            });
        })
        .await
        .unwrap();
    let plan = fluree.plan_purge(id).await.unwrap();
    let original = dir
        .path()
        .join(plan.originals[0].strip_prefix("fluree:file://").unwrap());
    fn index(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    index(&path)
                } else {
                    usize::from(path.to_string_lossy().contains("/index/"))
                }
            })
            .sum()
    }
    let indexed = index(dir.path());
    // Leading when it starts, deposed as its lease runs out.
    let cluster = crate::garbage::tests::cluster::Cluster::with_leases(1);
    *cluster.lease_ms.lock() = Some(0);
    fluree.set_collector(Arc::new(cluster));
    let pass = fluree.run_purge_pass(id, std::time::Duration::ZERO).await;
    assert!(pass.is_err(), "{pass:?}");
    assert!(original.exists());
    assert!(index(dir.path()) >= indexed, "an index file was deleted");
}

/// A cluster member's cleanup evicts the cached state of the purge's
/// holders: one loaded before the purge's rewrite holds the older index.
#[tokio::test]
async fn a_members_cleanup_evicts_the_holders_cached_state() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/cached").await.unwrap();
    let id = "t/cached:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    fluree.ledger_cached(id).await.unwrap();
    let manager = fluree.ledger_manager().unwrap();
    let ledger = crate::LedgerId::parse(id).unwrap();
    assert!(manager.get_loaded_handle(&ledger).await.is_some());
    fluree.clean_after_purge(&[id.to_string()]).await.unwrap();
    assert!(manager.get_loaded_handle(&ledger).await.is_none());
}

/// On a cluster a purge plans only the jobs every member has installed: a
/// member that has not still reads the originals, and may write them back.
#[tokio::test]
async fn a_cluster_plans_only_the_jobs_every_member_installed() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/gated").await.unwrap();
    let id = "t/gated:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/bob", "http://example.org/email": "b@secret"}),
    )
    .await;
    erase(&fluree, id, email("alice", "a@secret")).await;
    erase(&fluree, id, email("bob", "b@secret")).await;
    fluree.set_collector(Arc::new(crate::garbage::tests::cluster::Cluster::default()));
    let jobs = fluree
        .retention_status(id)
        .await
        .unwrap()
        .registration
        .unwrap()
        .erasures;
    assert_eq!(jobs.len(), 2);
    for (n, job) in jobs.iter().enumerate() {
        fluree
            .update_job(id, &job.request_id, |j| {
                j.cluster = Some(super::super::ClusterPublication {
                    manifest: format!("m{n}"),
                    activation: Some(n as u64),
                    effective_ms: (n == 0).then_some(1),
                });
            })
            .await
            .unwrap();
    }
    let plan = fluree.plan_purge(id).await.unwrap();
    assert_eq!(plan.requests, [jobs[0].request_id.clone()]);
    assert_eq!(
        plan.commits,
        jobs[0]
            .commits
            .iter()
            .map(|c| c.commit.clone())
            .collect::<Vec<_>>()
    );
}

/// Capture reads the erased facts from the originals, which reads no longer
/// return; once the originals are gone, from the earliest replacement a
/// later erasure superseded, which still holds what that erasure removed.
#[tokio::test]
async fn capture_reads_the_oldest_bytes_still_stored() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/capture").await.unwrap();
    let id = "t/capture:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/carol", "http://example.org/email": "c@secret"}
        ]}),
    )
    .await;
    erase(&fluree, id, email("alice", "a@secret")).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    assert!(plan.superseded.is_empty(), "{plan:?}");
    let captured = fluree.capture_erased(&plan).await.unwrap();
    assert!(holds(&captured, "a@secret") && !holds(&captured, "c@secret"));

    erase(&fluree, id, email("carol", "c@secret")).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    assert_eq!(plan.superseded.len(), 1, "{plan:?}");
    let captured = fluree.capture_erased(&plan).await.unwrap();
    assert!(holds(&captured, "a@secret") && holds(&captured, "c@secret"));

    // As an earlier purge leaves it: the originals gone.
    for address in &plan.originals {
        let path = address.strip_prefix("fluree:file://").unwrap();
        std::fs::remove_file(dir.path().join(path)).unwrap();
    }
    let captured = fluree.capture_erased(&plan).await.unwrap();
    assert!(!holds(&captured, "a@secret") && holds(&captured, "c@secret"));
}

/// An index built before publication still holds the erased occurrences
/// and their values; one rebuilt from the replacements holds neither, except
/// a value a surviving occurrence uses. A commit file no history reaches is
/// found once reconciliation records it.
#[tokio::test]
async fn a_scan_finds_what_still_holds_a_captured_value() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/scan").await.unwrap();
    let id = "t/scan:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/carol", "http://example.org/email": "c@shared"}
        ]}),
    )
    .await;
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/dave", "http://example.org/email": "c@shared"}),
    )
    .await;
    let both = ErasureRequest {
        matching: [email("alice", "a@secret"), email("carol", "c@shared")]
            .into_iter()
            .flat_map(|r| r.matching)
            .collect(),
        ..Default::default()
    };
    erase(&fluree, id, both).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();

    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    assert_eq!(scan.indexed, vec![id.to_string()], "{scan:?}");
    assert_eq!(scan.dictionary, vec![id.to_string()], "{scan:?}");
    assert_eq!(scan.in_use, 1, "{scan:?}");

    fluree.reindex(id, ReindexOptions::default()).await.unwrap();
    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    assert!(scan.clean(), "{scan:?}");
    assert_eq!(scan.in_use, 1, "{scan:?}");

    // A commit no history reaches, as a rebase leaves one.
    fluree.create_ledger("t/elsewhere").await.unwrap();
    insert(
        &fluree,
        "t/elsewhere:main",
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    let elsewhere = fluree
        .nameservice()
        .lookup("t/elsewhere:main")
        .await
        .unwrap()
        .unwrap();
    let stray = elsewhere.commit_head_id.as_ref().unwrap().address_digest();
    let folder = |record: &fluree_db_nameservice::NsRecord| {
        dir.path()
            .join(record.storage_namespace().branch_prefix())
            .join("commit")
    };
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    std::fs::copy(
        folder(&elsewhere).join(format!("{stray}.fcv2")),
        folder(&record).join(format!("{stray}.fcv2")),
    )
    .unwrap();
    assert!(fluree
        .scan_purge(&plan, &captured)
        .await
        .unwrap()
        .unreferenced
        .is_empty());
    fluree.reconcile_garbage("t/scan").await.unwrap().unwrap();
    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    assert_eq!(scan.unreferenced.len(), 1, "{scan:?}");
    assert!(scan.unreferenced[0].contains(&stray), "{scan:?}");
}

/// A merge copies the merged branch's commits into the target's folder.
/// Once that branch is dropped only the merge's second parent reaches them,
/// and a value one of them still uses is in use, not unreferenced.
#[tokio::test]
async fn a_scan_follows_every_parent_of_a_merge() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/merged").await.unwrap();
    let id = "t/merged:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    fluree
        .create_branch("t/merged", "dev", None, None)
        .await
        .unwrap();
    insert(
        &fluree,
        "t/merged:dev",
        json!({"@id": "http://example.org/dave", "http://example.org/email": "a@secret"}),
    )
    .await;
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/bob", "http://example.org/name": "Bob"}),
    )
    .await;
    fluree
        .merge_branch("t/merged", "dev", None, crate::ConflictStrategy::default())
        .await
        .unwrap();
    fluree.drop_branch("t/merged", "dev").await.unwrap();

    erase(&fluree, id, email("alice", "a@secret")).await;
    fluree.reindex(id, ReindexOptions::default()).await.unwrap();
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();
    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    assert!(scan.clean(), "{scan:?}");
    assert_eq!(scan.in_use, 1, "{scan:?}");
}

/// Transaction documents no commit names are reported once reconciliation
/// records them, whatever they hold; a surviving commit's own document is
/// not. A TTL schedule still holding an erased fact is found too.
#[tokio::test]
async fn a_scan_finds_recorded_garbage_and_schedules_holding_a_captured_value() {
    use sha2::{Digest, Sha256};

    use crate::retention::state::{Entry, FactKey, Schedule, Source};

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/documents").await.unwrap();
    let id = "t/documents:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    // A surviving use of the value, whose own document holds it.
    fluree
        .graph(id)
        .transact()
        .txn_opts(fluree_db_transact::TxnOpts::default().store_raw_txn(true))
        .insert(&json!({"@id": "http://example.org/dave", "http://example.org/email": "a@secret"}))
        .commit()
        .await
        .unwrap();
    erase(&fluree, id, email("alice", "a@secret")).await;
    fluree.reindex(id, ReindexOptions::default()).await.unwrap();
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();
    assert!(fluree.scan_purge(&plan, &captured).await.unwrap().clean());

    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let folder = dir.path().join(record.storage_namespace().branch_prefix());
    let write = |kind: &str, ext: &str, bytes: &[u8]| {
        let hex = hex::encode(Sha256::digest(bytes));
        std::fs::create_dir_all(folder.join(kind)).unwrap();
        std::fs::write(folder.join(kind).join(format!("{hex}.{ext}")), bytes).unwrap();
        hex
    };
    let documents = [
        json!({"insert": {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}}),
        json!({"insert": {"http://example.org/email": "b@other"}}),
    ]
    .map(|doc| write("txn", "json", doc.to_string().as_bytes()));
    let asserted = captured.iter().find(|c| c.flake.op).unwrap();
    let mut schedule = Schedule::starting_after(0);
    schedule.insert(
        FactKey::of(&asserted.flake),
        Entry {
            due_ms: 0,
            occurrence_t: asserted.flake.t,
            occurrence_slot: 0,
            source: Source::Property,
            generation_t: asserted.flake.t,
            reifier: None,
        },
    );
    let scheduled = write("retention", "frs", &schedule.encode().unwrap());

    fluree
        .reconcile_garbage("t/documents")
        .await
        .unwrap()
        .unwrap();
    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    for hex in &documents {
        assert!(
            scan.unreferenced.iter().any(|a| a.contains(hex.as_str())),
            "{hex}: {scan:?}"
        );
    }
    assert_eq!(scan.unreferenced.len(), 2, "{scan:?}");
    assert_eq!(scan.schedules.len(), 1, "{scan:?}");
    assert!(scan.schedules[0].contains(&scheduled), "{scan:?}");
}

/// A ledger's current TTL schedule holding an erased fact is rewritten
/// without it, and the old one deleted.
#[tokio::test]
async fn a_purge_rewrites_a_schedule_holding_an_erased_fact() {
    use fluree_db_core::{ContentKind, ContentStore as _};

    use crate::retention::registry;
    use crate::retention::state::{Entry, FactKey, Schedule, Source};

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/scheduled").await.unwrap();
    let id = "t/scheduled:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/bob", "http://example.org/email": "b@kept"}
        ]}),
    )
    .await;
    erase(&fluree, id, email("alice", "a@secret")).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();
    let erased = FactKey::of(&captured.iter().find(|c| c.flake.op).unwrap().flake);

    // A schedule naming the erased fact and a surviving one.
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let state = fluree.ledger(id).await.unwrap();
    let bob = fluree_db_core::range(
        &state.snapshot,
        0,
        fluree_db_core::IndexType::Spot,
        fluree_db_core::RangeTest::Eq,
        fluree_db_core::RangeMatch::subject(
            state.snapshot.encode_iri("http://example.org/bob").unwrap(),
        ),
        Default::default(),
    )
    .await
    .unwrap();
    let kept = FactKey::of(&bob[0]);
    let entry = |t| Entry {
        due_ms: i64::MAX,
        occurrence_t: t,
        occurrence_slot: 0,
        source: Source::Property,
        generation_t: t,
        reifier: None,
    };
    let mut schedule = Schedule::starting_after(0);
    schedule.insert(erased.clone(), entry(1));
    schedule.insert(kept.clone(), entry(1));
    let store = fluree.content_store(&record.storage_namespace());
    let blob = store
        .put(ContentKind::RetentionState, &schedule.encode().unwrap())
        .await
        .unwrap();
    let ns = fluree.publisher().unwrap();
    registry::update(ns, fluree.registry_objects(), id, |r| {
        Some(registry::Registration {
            schedule: Some(blob.to_string()),
            ..r?.clone()
        })
    })
    .await
    .unwrap();

    let report = fluree
        .run_purge_pass(id, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    let current = registry::read(ns, fluree.registry_objects(), id)
        .await
        .unwrap()
        .1
        .unwrap()
        .schedule
        .unwrap();
    assert_ne!(current, blob.to_string());
    let rewritten = Schedule::decode(&store.get(&current.parse().unwrap()).await.unwrap()).unwrap();
    assert!(!rewritten.contains(&erased) && rewritten.contains(&kept));
    assert!(!store.has(&blob).await.unwrap());
}

/// A file nothing references, holding an erased value, is collected before
/// the purge completes: the purge reconciles the ledger first.
#[tokio::test]
async fn a_purge_collects_an_unreferenced_file_holding_an_erased_value() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/stray").await.unwrap();
    let id = "t/stray:main";
    insert(
        &fluree,
        id,
        json!({"@id": "http://example.org/alice", "http://example.org/email": "a@secret"}),
    )
    .await;
    // A failed transaction's body, left by a crash before it was abandoned.
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let stray = fluree
        .content_store(&record.storage_namespace())
        .put(
            fluree_db_core::ContentKind::Txn,
            json!({"insert": {"http://example.org/email": "a@secret"}})
                .to_string()
                .as_bytes(),
        )
        .await
        .unwrap();
    erase(&fluree, id, email("alice", "a@secret")).await;

    let report = fluree
        .run_purge_pass(id, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert!(!fluree
        .content_store(&record.storage_namespace())
        .has(&stray)
        .await
        .unwrap());
}

/// Erasing leaf by leaf leaves an index that reads as a full rebuild from
/// the replacements does: every subject the same as of every `t`, for a
/// value in a dictionary and a value embedded in the key alike.
#[tokio::test]
async fn erasing_leaf_by_leaf_reads_as_a_rebuild() {
    use fluree_db_core::{range, IndexType, RangeMatch, RangeOptions, RangeTest};

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/leafwise").await.unwrap();
    let id = "t/leafwise:main";
    let ex = |s: &str| format!("http://example.org/{s}");
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": ex("alice"), ex("email"): "a@secret", ex("nick"): "ally", ex("age"): 41},
            {"@id": ex("bob"), ex("email"): "b@kept", ex("age"): 30}
        ]}),
    )
    .await;
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": ex("carol"), ex("name"): "Carol", ex("score"): 3.5},
            // The same string, kept: its dictionary entry stays.
            {"@id": ex("dave"), ex("email"): "a@secret"}
        ]}),
    )
    .await;
    fluree
        .graph(id)
        .transact()
        .update(&json!({
            "delete": {"@id": ex("bob"), ex("age"): 30},
            "insert": {"@id": ex("bob"), ex("age"): 31}
        }))
        .commit()
        .await
        .unwrap();
    let request = ErasureRequest {
        matching: vec![
            FactSelector {
                subject: ex("alice"),
                predicate: ex("email"),
                graph: None,
                object: None,
            },
            FactSelector {
                subject: ex("alice"),
                predicate: ex("age"),
                graph: None,
                object: None,
            },
            // A string nothing else uses: its dictionary entry goes.
            FactSelector {
                subject: ex("alice"),
                predicate: ex("nick"),
                graph: None,
                object: None,
            },
        ],
        ..Default::default()
    };
    erase(&fluree, id, request).await;
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();
    assert!(captured.len() >= 4, "{captured:?}");

    let reads = |fluree: &Fluree| {
        let fluree = fluree.clone();
        async move {
            let state = fluree.ledger(id).await.unwrap();
            let mut out = Vec::new();
            for t in 1..=state.t() {
                for s in ["alice", "bob", "carol", "dave"] {
                    let sid = state.snapshot.encode_iri(&ex(s)).unwrap();
                    let mut flakes: Vec<String> = range(
                        &state.snapshot,
                        0,
                        IndexType::Spot,
                        RangeTest::Eq,
                        RangeMatch::subject(sid),
                        RangeOptions {
                            to_t: Some(t),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|f| format!("{} {} {:?} {}", f.s, f.p, f.o, f.op))
                    .collect();
                    flakes.sort();
                    out.push((t, s, flakes));
                }
            }
            out
        }
    };

    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    assert_eq!(
        fluree.erase_from_index(&record, &captured).await.unwrap(),
        super::erase::Erased::Leafwise
    );
    let scan = fluree.scan_purge(&plan, &captured).await.unwrap();
    assert!(
        scan.indexed.is_empty() && scan.dictionary.is_empty(),
        "{scan:?}"
    );
    let leafwise = reads(&fluree).await;

    fluree.reindex(id, ReindexOptions::default()).await.unwrap();
    let rebuilt = reads(&fluree).await;
    assert_eq!(leafwise, rebuilt);
}

/// Erasing leaf by leaf holds the branch against indexing, as a reindex
/// does, so no build begun from the root being erased publishes over it:
/// with the branch held by another maintenance operation, it refuses.
#[tokio::test]
async fn erasing_leaf_by_leaf_holds_the_branch_against_indexing() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_erasure(ErasureConfig {
            prover: Arc::new(NativeProver::default()),
            verifier: Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build()
        .unwrap();
    fluree.create_ledger("t/held").await.unwrap();
    let id = "t/held:main";
    let ex = |s: &str| format!("http://example.org/{s}");
    insert(
        &fluree,
        id,
        json!({"@id": ex("alice"), ex("email"): "a@secret", ex("name"): "Alice"}),
    )
    .await;
    erase(
        &fluree,
        id,
        ErasureRequest {
            matching: vec![FactSelector {
                subject: ex("alice"),
                predicate: ex("email"),
                graph: None,
                object: None,
            }],
            ..Default::default()
        },
    )
    .await;
    let plan = fluree.plan_purge(id).await.unwrap();
    let captured = fluree.capture_erased(&plan).await.unwrap();
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let crate::IndexingMode::Background(handle) = &fluree.indexing_mode else {
        panic!("background indexing");
    };
    let held = handle.hold_quiesced(&record.ledger_id).await.unwrap();
    let refused = fluree.erase_from_index(&record, &captured).await;
    assert!(
        matches!(refused, Err(crate::ApiError::BranchConflict(_))),
        "{refused:?}"
    );
    drop(held);
    assert_eq!(
        fluree.erase_from_index(&record, &captured).await.unwrap(),
        super::erase::Erased::Leafwise
    );
}

/// A sync's files are leftovers until it publishes: a purge that releases
/// one meanwhile leaves the sync a manifest naming a file gone, which it
/// does not publish.
#[tokio::test]
async fn a_sync_does_not_publish_a_file_a_release_took() {
    use std::time::Duration;

    use fluree_db_core::ContentKind;
    use fluree_db_nameservice::NameServiceError;
    use fluree_db_query::bm25::Bm25SnapshotEntry;

    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/release-race").await.unwrap();
    let id = "t/release-race:main";
    let ctx = json!({"ex": "http://example.org/"});
    insert(
        &fluree,
        id,
        json!({"@context": ctx, "@type": "ex:Person", "@id": "ex:alice", "ex:email": "zephyr"}),
    )
    .await;
    fluree
        .create_full_text_index(crate::Bm25CreateConfig::new(
            "t-release-race",
            id,
            json!({
                "@context": ctx,
                "where": [{"@id": "?x", "@type": "ex:Person", "ex:email": "?email"}],
                "select": {"?x": ["@id", "ex:email"]}
            }),
        ))
        .await
        .unwrap();
    erase(&fluree, id, email("alice", "zephyr")).await;
    let report = fluree
        .run_purge_pass(id, Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(report.waiting, "{report:?}");

    // A sync on the rebuilt head: its snapshot written, not yet published.
    let source = "t-release-race:main";
    let mut manifest = fluree.load_or_create_bm25_manifest(source).await.unwrap();
    let written = fluree
        .graph_source_store(source)
        .unwrap()
        .put(ContentKind::GraphSourceSnapshot, b"a sync's snapshot")
        .await
        .unwrap();
    manifest.append(Bm25SnapshotEntry::new(i64::MAX, written));
    let head = fluree
        .nameservice()
        .lookup_graph_source(source)
        .await
        .unwrap();

    let report = fluree.run_purge_pass(id, Duration::ZERO).await.unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    let refused = fluree
        .publish_bm25_manifest(source, &manifest, 2, &[])
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::ApiError::NameService(NameServiceError::Conflict(_)))
        ),
        "{refused:?}"
    );
    assert_eq!(
        fluree
            .nameservice()
            .lookup_graph_source(source)
            .await
            .unwrap(),
        head
    );
}

/// The files under `root` holding `needle`.
fn holding(root: &std::path::Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if std::fs::read(&path)
                .unwrap()
                .windows(needle.len())
                .any(|w| w == needle.as_bytes())
            {
                found.push(path);
            }
        }
    }
    found
}

/// What `subject` has for `ex:email` on `ledger_id`.
async fn emails(fluree: &Fluree, ledger_id: &str, subject: &str) -> crate::Result<Vec<JsonValue>> {
    let results = fluree
        .query_connection(&json!({
            "from": ledger_id,
            "select": "?o",
            "where": {"@id": format!("http://example.org/{subject}"), "http://example.org/email": "?o"}
        }))
        .await?;
    let ledger = fluree.ledger(ledger_id).await?;
    Ok(results
        .to_jsonld(&ledger.snapshot)?
        .as_array()
        .cloned()
        .unwrap_or_default())
}

/// Purge passes over `id` until none has a job left to purge.
async fn purge(fluree: &Fluree, id: &str) {
    for _ in 0..5 {
        let report = fluree
            .run_purge_pass(id, std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(report.blocked.is_none(), "{report:?}");
        if report.requests.is_empty() {
            return;
        }
    }
    panic!("{id} was not purged");
}

/// Runs `during` once a pass over `id` reaches `at` for `name`, for at most
/// two seconds, then lets the pass go on. Returns what `during` returned,
/// if it finished.
async fn while_held<T>(
    fluree: &Fluree,
    id: &str,
    at: super::hooks::Hold,
    name: &str,
    during: impl std::future::Future<Output = T>,
) -> Option<T> {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let (reached, proceed) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *super::hooks::HOLD.lock() = Some((at, name.to_string(), reached.clone(), proceed.clone()));
    let held = async {
        reached.notified().await;
        let done = tokio::time::timeout(std::time::Duration::from_secs(2), during)
            .await
            .ok();
        proceed.notify_one();
        done
    };
    let (pass, done) = tokio::join!(fluree.run_purge_pass(id, std::time::Duration::ZERO), held);
    *super::hooks::HOLD.lock() = None;
    pass.unwrap();
    done
}

/// A restore of a soft-dropped holder never runs within a purge pass: run
/// once the release has marked the ledger and before it deleted the
/// ledger's index files, the release would delete the index the restore
/// rebuilt.
#[tokio::test]
async fn a_restore_of_a_dropped_holder_waits_for_a_purge_pass() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/restore-race").await.unwrap();
    let id = "t/restore-race:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/bob", "http://example.org/email": "b@kept"}
        ]}),
    )
    .await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(id, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("t/restore-race-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    fluree
        .reindex(&copy, ReindexOptions::default())
        .await
        .unwrap();
    let instance = fluree
        .drop_ledger("t/restore-race-copy", crate::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    erase(&fluree, id, email("alice", "a@secret")).await;

    let restore = fluree.restore_dropped(instance.as_str());
    let during = while_held(
        &fluree,
        id,
        super::hooks::Hold::Marked,
        "t/restore-race-copy",
        restore,
    )
    .await;
    if during.is_none() {
        fluree.restore_dropped(instance.as_str()).await.unwrap();
    }
    purge(&fluree, id).await;
    assert_eq!(
        emails(&fluree, &copy, "bob").await.expect("the copy reads"),
        vec![json!("b@kept")]
    );
    assert!(emails(&fluree, &copy, "alice").await.unwrap().is_empty());
    assert_eq!(
        holding(dir.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );
}

/// A branch is never made within a purge pass: made from a holder once the
/// pass has planned and before it erased the holder's index, the branch
/// would copy that index with the value, and no pass would look at it.
#[tokio::test]
async fn a_branch_of_a_holder_waits_for_a_purge_pass() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    fluree.create_ledger("t/branch-race").await.unwrap();
    let id = "t/branch-race:main";
    insert(
        &fluree,
        id,
        json!({"@graph": [
            {"@id": "http://example.org/alice", "http://example.org/email": "a@secret"},
            {"@id": "http://example.org/bob", "http://example.org/email": "b@kept"}
        ]}),
    )
    .await;
    erase(&fluree, id, email("alice", "a@secret")).await;

    let branch = fluree.create_branch("t/branch-race", "dev", None, None);
    let during = while_held(
        &fluree,
        id,
        super::hooks::Hold::Planned,
        "t/branch-race",
        branch,
    )
    .await;
    if during.is_none() {
        fluree
            .create_branch("t/branch-race", "dev", None, None)
            .await
            .unwrap();
    }
    purge(&fluree, id).await;
    let dev = "t/branch-race:dev";
    assert!(emails(&fluree, dev, "alice").await.unwrap().is_empty());
    assert_eq!(
        emails(&fluree, dev, "bob").await.unwrap(),
        vec![json!("b@kept")]
    );
    assert_eq!(
        holding(dir.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );
}
