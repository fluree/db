//! The WAL seen from the API: a transaction costs one device flush, and
//! everything acknowledged survives a crash that loses every file the page
//! cache held. Unix only, like the log.

#![cfg(all(feature = "native", unix))]

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerHandle};
use fluree_db_core::{Durability, FileStorage};
use serde_json::json;
use std::path::Path;

const LEDGER: &str = "it/wal:main";

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
    let storage = FileStorage::new(dir).with_durability(Durability::Wal);
    storage.recover_wal().expect("attach");
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
    assert!(!dir.path().join(".fluree-wal").exists());
    assert_eq!(probe.effective_durability(), Durability::Sync);
    fluree.disconnect().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_transactions_survive_losing_every_unflushed_file() {
    let dir = tempfile::tempdir().unwrap();
    let probe = probe(dir.path());
    probe
        .hold_wal_segments_for_test()
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
        if entry.file_name() == ".fluree-wal" {
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
    let mut names: Vec<_> = std::fs::read_dir(dir.path().join(".fluree-wal"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["LOCK"]);
}

/// Not a check: a measurement. Prints per-commit wall time for the durable
/// modes so the log's saving can be read off the machine it runs on.
///
/// `cargo test -p fluree-db-api --release --test it_wal -- --ignored --nocapture timing`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn timing_wal_vs_sync() {
    use fluree_db_api::CommitOpts;
    use std::time::Instant;

    const WARM: usize = 10;
    const SAMPLES: usize = 200;

    for (mode, durability) in [("wal", Durability::Wal), ("sync", Durability::Sync)] {
        for raw in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                .without_indexing()
                .with_storage_durability(durability)
                .build()
                .expect("build");
            fluree.create_ledger(LEDGER).await.expect("create");
            let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
            let store = fluree.content_store(handle.id());
            let mut samples = Vec::with_capacity(SAMPLES);
            for n in 0..WARM + SAMPLES {
                let body = json!({
                    "@context": { "ex": "http://example.org/" },
                    "@id": format!("ex:person-{n}"),
                    "ex:name": format!("Person {n}"),
                });
                let opts = if raw {
                    CommitOpts::default().with_raw_txn_spawned(store.clone(), body.clone())
                } else {
                    CommitOpts::default()
                };
                let started = Instant::now();
                fluree
                    .stage(&handle)
                    .insert(&body)
                    .commit_opts(opts)
                    .execute()
                    .await
                    .expect("insert");
                if n >= WARM {
                    samples.push(started.elapsed());
                }
            }
            samples.sort();
            let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
            let mean = samples.iter().sum::<std::time::Duration>() / samples.len() as u32;
            println!(
                "{mode:<8} raw={raw:<5} commits={SAMPLES} median={:?} p95={:?} mean={:?}",
                at(0.5),
                at(0.95),
                mean
            );
            fluree.disconnect().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn review_read_only_client_recovers_without_retaining_writer_lock() {
    use fluree_db_api::NameServiceMode;
    use fluree_db_core::{StorageRead, StorageWrite};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fs2::FileExt;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let previous = probe(dir.path());
    previous.hold_wal_segments_for_test().unwrap();
    previous
        .write_bytes("fluree:file://payload", b"recover me")
        .await
        .unwrap();
    previous.sync().await.unwrap();
    previous.simulate_crash_for_test();
    std::fs::remove_file(dir.path().join("payload")).unwrap();
    let reader = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build_client_with_nameservice(NameServiceMode::ReadOnly(
            Arc::new(MemoryNameService::new()),
        ))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("payload")).unwrap(),
        b"recover me"
    );
    // Use an independent OS lock, because in-process writers share a WAL
    // instance and would hide the reader's ownership bug.
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join(".fluree-wal/LOCK"))
        .unwrap();
    lock.try_lock_exclusive()
        .expect("reader must leave the WAL available to a writer process");
    drop(lock);
    let writer = probe(dir.path());
    writer
        .write_bytes("fluree:file://later", b"writer")
        .await
        .unwrap();
    assert_eq!(writer.effective_durability(), Durability::Wal);
    assert!(
        writer.fsyncs_issued() > 0,
        "writer actually opened a WAL segment"
    );
    assert_eq!(
        writer.read_bytes("fluree:file://later").await.unwrap(),
        b"writer"
    );
    drop(reader);
}
