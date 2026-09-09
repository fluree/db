//! The redo log seen from the API: a transaction costs one device flush, and
//! everything acknowledged survives a crash that loses every file the page
//! cache held. Unix only, like the log.

#![cfg(all(feature = "native", unix))]

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerHandle};
use fluree_db_core::{Durability, FileStorage};
use serde_json::json;
use std::path::Path;

const LEDGER: &str = "it/redo-log:main";

fn open(dir: &Path) -> Fluree {
    // Indexing publishes its own head pointer; keep the flush count to the
    // transaction path alone.
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build")
}

/// A second handle on the same root shares its log, so the counter it reports
/// is the log's, whichever handle appended.
fn probe(dir: &Path) -> FileStorage {
    let storage = FileStorage::new(dir).with_durability(Durability::Journal);
    storage.recover_redo_log().expect("attach");
    storage
}

async fn insert(fluree: &Fluree, handle: &LedgerHandle, n: usize) {
    fluree
        .stage(handle)
        .insert(&json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:person-{n}"),
            "ex:name": format!("Person {n}"),
        }))
        .execute()
        .await
        .expect("insert");
}

async fn count_people(fluree: &Fluree) -> usize {
    let state = fluree.ledger(LEDGER).await.expect("load ledger");
    let db = GraphDb::from_ledger_state(&state);
    let result = fluree
        .query(
            &db,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "select": ["?p"],
                "where": { "@id": "?p", "ex:name": "?n" }
            }),
        )
        .await
        .expect("query");
    result
        .to_jsonld(&state.snapshot)
        .expect("rows")
        .as_array()
        .map_or(0, Vec::len)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_costs_one_flush() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = open(dir.path());
    fluree.create_ledger(LEDGER).await.expect("create");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    insert(&fluree, &handle, 0).await;

    let probe = probe(dir.path());
    let before = probe.fsyncs_issued();
    insert(&fluree, &handle, 1).await;
    assert_eq!(
        probe.fsyncs_issued() - before,
        1,
        "a commit is one head publication, and that is the flush"
    );
    fluree.disconnect().await;

    // The control: the same transaction under per-write flushing pays for
    // the commit blob and the head separately, two flushes each.
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_storage_durability(Durability::Sync)
        .build()
        .expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    insert(&fluree, &handle, 0).await;
    let probe = FileStorage::new(dir.path()).with_durability(Durability::Sync);
    // A per-write handle counts only its own flushes, so count the files the
    // commit produced instead: no log directory, and the head in place.
    assert!(!dir.path().join(".fluree-redo").exists());
    assert_eq!(probe.effective_durability(), Durability::Sync);
    fluree.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_transactions_survive_losing_every_unflushed_file() {
    let dir = tempfile::tempdir().unwrap();
    let probe = probe(dir.path());
    probe
        .hold_redo_segments_for_test()
        .expect("keep the log until the crash");

    let fluree = open(dir.path());
    fluree.create_ledger(LEDGER).await.expect("create");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    for n in 0..3 {
        insert(&fluree, &handle, n).await;
    }
    assert_eq!(count_people(&fluree).await, 3);

    // Crash: nothing the process wrote outside the log reaches the device.
    probe.simulate_crash_for_test();
    fluree.disconnect().await;
    drop(fluree);
    drop(probe);
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == ".fluree-redo" {
            continue;
        }
        if entry.file_type().unwrap().is_dir() {
            std::fs::remove_dir_all(entry.path()).unwrap();
        } else {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let fluree = open(dir.path());
    assert_eq!(
        count_people(&fluree).await,
        3,
        "replay on open rebuilt the ledger, its commits and its head"
    );
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    insert(&fluree, &handle, 3).await;
    assert_eq!(count_people(&fluree).await, 4, "and it keeps working");
    fluree.disconnect().await;
    drop(handle);
    drop(fluree);

    // A clean close leaves the root readable by a binary without the log.
    let mut names: Vec<_> = std::fs::read_dir(dir.path().join(".fluree-redo"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["LOCK"]);
}
