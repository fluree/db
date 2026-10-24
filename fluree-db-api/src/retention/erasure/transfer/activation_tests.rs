//! Activations of received erasures stopped where a crash could stop them,
//! and transactions racing them.

use std::sync::Arc;
use std::time::Duration;

use fluree_db_core::commit::codec::replacement::is_replacement;
use fluree_db_core::{collect_first_parent_cids, ContentId};
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

use super::hooks::{Stop, CHECKED, STOP};
use crate::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
use crate::retention::FactSelector;
use crate::{ApiError, Fluree, FlureeBuilder, ReindexOptions};

/// The hooks are process-wide: one test at a time sets them.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/"})
}

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

fn receiving(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure_verifier(Arc::new(Verifier::accepting_dev_receipts()))
        .build()
        .unwrap()
}

async fn reindex(fluree: &Fluree, ledger_id: &str) {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
}

async fn emails(fluree: &Fluree, ledger_id: &str) -> Vec<JsonValue> {
    let results = fluree
        .query_connection(&json!({
            "@context": ctx(),
            "from": ledger_id,
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await
        .unwrap();
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    results
        .to_jsonld(&ledger.snapshot)
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default()
}

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

/// Archives of a ledger from before and after alice's email is erased, and
/// the commit holding it.
async fn archives(dir: &tempfile::TempDir) -> (Vec<u8>, Vec<u8>, ContentId, i64) {
    let source = erasing(dir);
    source.create_ledger("t/activation").await.unwrap();
    let id = "t/activation:main";
    let t1 = source
        .graph(id)
        .transact()
        .insert(&json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}))
        .commit()
        .await
        .unwrap()
        .receipt
        .t;
    source
        .graph(id)
        .transact()
        .insert(&json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@kept"}))
        .commit()
        .await
        .unwrap();
    let record = source.nameservice().lookup(id).await.unwrap().unwrap();
    let store = source.content_store(&record.storage_namespace());
    let shared = collect_first_parent_cids(store.as_ref(), &record.commit_head_id.unwrap(), 0)
        .await
        .unwrap()
        .into_iter()
        .find(|(t, _)| *t == t1)
        .unwrap()
        .1;
    let mut before = Vec::new();
    source.archive_ledger(id, false, &mut before).await.unwrap();
    let request = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!("a@secret")),
        }],
        ..Default::default()
    };
    source.erase_occurrences(id, request, None).await.unwrap();
    reindex(&source, id).await;
    assert_eq!(source.run_erasure_pass(id).await.unwrap().published, 1);
    let mut after = Vec::new();
    source.archive_ledger(id, false, &mut after).await.unwrap();
    (before, after, shared, t1)
}

/// A receiver holding the original, indexed.
async fn holder(here: &tempfile::TempDir, before: &[u8]) -> (Fluree, String) {
    let receiver = receiving(here);
    let held = receiver
        .restore_ledger("t/activation-held", &mut &before[..])
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &held).await;
    (receiver, held)
}

/// An activation stopped before any holder changes, after one did, or after
/// publication, leaves its record; resuming completes it, from whichever
/// point, and the holder's purge then leaves no file holding the value.
#[tokio::test]
async fn a_stopped_activation_completes_on_resume() {
    let _serial = SERIAL.lock().await;
    let source = tempfile::tempdir().unwrap();
    let (before, after, shared, t1) = archives(&source).await;
    for stop in [Stop::BeforeHolders, Stop::AfterHolder, Stop::AfterPublish] {
        let here = tempfile::tempdir().unwrap();
        let (receiver, held) = holder(&here, &before).await;
        *STOP.lock() = Some(stop);
        let stopped = receiver
            .restore_ledger("t/activation-stopped", &mut after.as_slice())
            .await;
        *STOP.lock() = None;
        let stopped = stopped.expect_err("stopped");
        assert!(stopped.to_string().contains("stopped"), "{stopped}");
        let runtime = receiver.erasure_runtime().unwrap();
        assert_eq!(runtime.index.pending().await.unwrap().len(), 1, "{stop:?}");
        if stop == Stop::BeforeHolders {
            assert_eq!(emails(&receiver, &held).await, [json!("a@secret")]);
        }

        assert_eq!(receiver.resume_received_erasures().await.unwrap(), 1);
        assert!(
            runtime.index.pending().await.unwrap().is_empty(),
            "{stop:?}"
        );
        assert!(emails(&receiver, &held).await.is_empty(), "{stop:?}");
        let store = receiver.content_store(
            &receiver
                .nameservice()
                .lookup(&held)
                .await
                .unwrap()
                .unwrap()
                .storage_namespace(),
        );
        assert!(is_replacement(&store.get(&shared).await.unwrap()));
        let past = receiver
            .query_connection(&json!({
                "@context": ctx(),
                "from": format!("{held}@t:{t1}"),
                "select": "?o",
                "where": {"@id": "ex:alice", "ex:email": "?o"}
            }))
            .await;
        assert!(
            matches!(past, Err(ApiError::HistoryUnavailable(_))),
            "{stop:?}: {past:?}"
        );
        let report = receiver
            .run_purge_pass(&held, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(report.purged, 1, "{stop:?}: {report:?}");
        assert!(
            holding(here.path(), "a@secret").is_empty(),
            "{stop:?}: {:?}",
            holding(here.path(), "a@secret")
        );
    }
}

/// A transaction on a holder while a restore checks and changes it waits
/// for the holder's write lock, and then finds the value gone: it cannot
/// retract it in a commit only a prover could erase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_waits_for_the_holder_and_finds_the_value_gone() {
    let _serial = SERIAL.lock().await;
    let source = tempfile::tempdir().unwrap();
    let (before, after, _, _) = archives(&source).await;
    let here = tempfile::tempdir().unwrap();
    let (receiver, held) = holder(&here, &before).await;
    let receiver = Arc::new(receiver);
    let checked = Arc::new(tokio::sync::Notify::new());
    let proceed = Arc::new(tokio::sync::Notify::new());
    *CHECKED.lock() = Some((checked.clone(), proceed.clone()));

    let restoring = {
        let receiver = receiver.clone();
        tokio::spawn(async move {
            receiver
                .restore_ledger("t/activation-raced", &mut after.as_slice())
                .await
        })
    };
    checked.notified().await;
    *CHECKED.lock() = None;
    let mut deleting = {
        let receiver = receiver.clone();
        let held = held.clone();
        tokio::spawn(async move {
            receiver
                .graph(&held)
                .transact()
                .update(&json!({
                    "@context": ctx(),
                    "delete": {"@id": "ex:alice", "ex:email": "a@secret"}
                }))
                .commit()
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut deleting)
            .await
            .is_err(),
        "the delete committed while the holder was checked"
    );
    proceed.notify_one();
    restoring.await.unwrap().unwrap();
    let _ = deleting.await.unwrap();

    assert!(emails(&receiver, &held).await.is_empty());
    let report = receiver
        .run_purge_pass(&held, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert!(
        holding(here.path(), "a@secret").is_empty(),
        "{:?}",
        holding(here.path(), "a@secret")
    );
}
