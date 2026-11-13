//! Which erased commits a ledger's history holds: found without walking a
//! history that can hold none, and walking only what is new since.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use fluree_db_core::ContentId;
use fluree_db_erasure::testing::NativeProver;
use serde_json::json;

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

async fn insert(fluree: &Fluree, ledger_id: &str, subject: &str, email: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&json!({
            "@id": format!("http://example.org/{subject}"),
            "http://example.org/email": email
        }))
        .commit()
        .await
        .unwrap();
}

async fn erase_published(fluree: &Fluree, ledger_id: &str, subject: &str, email: &str) {
    let request = ErasureRequest {
        matching: vec![FactSelector {
            subject: format!("http://example.org/{subject}"),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!(email)),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences(ledger_id, request, None)
        .await
        .unwrap();
    fluree.run_erasure_pass(ledger_id).await.unwrap();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .unwrap();
    let report = fluree.run_erasure_pass(ledger_id).await.unwrap();
    assert_eq!(report.published, 1, "{report:?}");
}

async fn reached(fluree: &Fluree, ledger_id: &str) -> Vec<ContentId> {
    let head = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap()
        .commit_head_id
        .unwrap();
    let store = fluree.branched_content_store(ledger_id).await.unwrap();
    fluree
        .erased_reached(ledger_id, store.as_ref(), &head)
        .await
        .unwrap()
        .into_iter()
        .map(|(cid, _)| cid)
        .collect()
}

#[tokio::test]
async fn only_a_history_that_can_hold_an_erasure_is_walked() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir);
    let other = "t/reach-other:main";
    fluree.create_ledger("t/reach-other").await.unwrap();
    for i in 0..8 {
        insert(&fluree, other, &format!("p{i}"), "p@kept").await;
    }
    let erased = "t/reach-erased:main";
    fluree.create_ledger("t/reach-erased").await.unwrap();
    insert(&fluree, erased, "alice", "a@secret").await;
    insert(&fluree, erased, "bob", "b@kept").await;
    erase_published(&fluree, erased, "alice", "a@secret").await;
    let runtime = fluree.erasure_runtime().unwrap();
    let reads = || runtime.reach_reads.load(Ordering::Relaxed);
    let probes = || runtime.index.probes.load(Ordering::Relaxed);

    // A ledger whose folders hold no erased commit is not walked, and
    // probes no unrelated commit.

    assert!(reached(&fluree, other).await.is_empty());
    assert_eq!(reads(), 0);
    assert_eq!(probes(), 0);
    assert!(reached(&fluree, other).await.is_empty());
    assert_eq!(probes(), 0);

    // One that does is walked once, then only for what is new.
    assert!(!reached(&fluree, erased).await.is_empty());
    let walked = reads();
    assert!(walked > 0);
    let first = reached(&fluree, erased).await;
    assert_eq!(reads(), walked);
    insert(&fluree, erased, "carol", "c@kept").await;
    assert_eq!(reached(&fluree, erased).await, first);
    assert_eq!(reads(), walked + 1);

    // Another ledger's erasure walks neither again, and the first probes
    // none of its commits.
    let third = "t/reach-third:main";
    fluree.create_ledger("t/reach-third").await.unwrap();
    insert(&fluree, third, "dave", "d@secret").await;
    erase_published(&fluree, third, "dave", "d@secret").await;
    let before = reads();
    let probed = probes();
    assert!(reached(&fluree, other).await.is_empty());
    assert_eq!(probes() - probed, 0);
    assert_eq!(reached(&fluree, erased).await, first);
    assert_eq!(reads(), before);
    assert!(!reached(&fluree, third).await.is_empty());
}
