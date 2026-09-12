//! Many clients writing to one ledger at once all get through.
//!
//! The optimistic commit path stages against a snapshot and restages when
//! the ledger moved before it took the lock. Under steady contention it
//! always has: the lock is FIFO and some commit is in flight at snapshot
//! time. Losing that race sixteen times in a row is what a client saw as
//! "transaction commit retry limit exceeded"; taking the lock before the
//! second attempt is what makes every commit land.

#![cfg(all(feature = "native", unix))]

use fluree_db_api::FlureeBuilder;
use serde_json::json;

const LEDGER: &str = "it/concurrent-writers:main";

/// File storage, so each commit holds the lock for a real flush and the
/// contention that starved clients is the contention this test creates.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_optimistic_insert_lands_under_contention() {
    const CLIENTS: usize = 16;
    const PER_CLIENT: usize = 40;

    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");

    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let fluree = fluree.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let mut failures = Vec::new();
                for i in 0..PER_CLIENT {
                    let n = c * PER_CLIENT + i;
                    let result = fluree
                        .stage(&handle)
                        .insert(&json!({
                            "@context": { "ex": "http://example.org/" },
                            "@id": format!("ex:person-{n}"),
                            "ex:name": format!("Person {n}"),
                        }))
                        .execute()
                        .await;
                    if let Err(e) = result {
                        failures.push(e.to_string());
                    }
                }
                failures
            })
        })
        .collect();
    let mut failures = Vec::new();
    for task in tasks {
        failures.extend(task.await.expect("client task"));
    }
    assert!(
        failures.is_empty(),
        "{} of {} commits failed; first: {}",
        failures.len(),
        CLIENTS * PER_CLIENT,
        failures[0]
    );

    let view = handle.snapshot().await;
    assert_eq!(
        view.t as usize,
        CLIENTS * PER_CLIENT,
        "every commit advanced the head once"
    );
    fluree.disconnect().await;
}
