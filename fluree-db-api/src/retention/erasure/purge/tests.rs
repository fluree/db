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
/// found by the value it holds.
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
