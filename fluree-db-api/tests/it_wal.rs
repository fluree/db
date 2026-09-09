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

/// Commits on different ledgers that reach the log together share one
/// device flush. The flush is slowed so eight concurrent commits are sure to
/// pile up behind the first; they then cost fewer flushes than commits. A
/// lone commit still costs exactly one (`a_transaction_costs_one_flush`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ledgers_share_a_flush() {
    const LEDGERS: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let fluree = open(dir.path());
    let mut handles = Vec::with_capacity(LEDGERS);
    for n in 0..LEDGERS {
        let id = format!("it/wal-shared-{n}:main");
        fluree.create_ledger(&id).await.expect("create");
        let handle = fluree.ledger_cached(&id).await.expect("cache");
        insert(&fluree, &handle, n).await;
        handles.push(handle);
    }
    let probe = probe(dir.path());
    probe.hold_wal_segments_for_test().expect("hold");
    let delay = std::time::Duration::from_millis(60);
    probe.slow_wal_sync_for_test(delay).expect("slow");
    let before = probe.fsyncs_issued();
    let tasks: Vec<_> = handles
        .into_iter()
        .enumerate()
        .map(|(n, handle)| {
            let fluree = fluree.clone();
            tokio::spawn(async move { insert(&fluree, &handle, 100 + n).await })
        })
        .collect();
    for task in tasks {
        task.await.expect("commit task");
    }
    let flushes = probe.fsyncs_issued() - before;
    assert!(
        flushes < LEDGERS as u64,
        "{flushes} flushes for {LEDGERS} concurrent commits: none were shared"
    );
    probe
        .slow_wal_sync_for_test(std::time::Duration::ZERO)
        .expect("restore");
    fluree.disconnect().await;
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

/// Not a check: a measurement of throughput under concurrency. Spawns
/// `clients` writers that each loop small inserts, either all on one ledger
/// or one ledger each, and reports commits/s, latency, and how many device
/// flushes each commit cost.
///
/// `cargo test -p fluree-db-api --release --test it_wal -- --ignored --nocapture timing_concurrent`
///
/// `FLUREE_TIMING_CLIENTS` (default "1,4,16") and `FLUREE_TIMING_COMMITS`
/// (per client, default 100) shape the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn timing_concurrent_wal() {
    use std::time::{Duration, Instant};

    let clients: Vec<usize> = std::env::var("FLUREE_TIMING_CLIENTS")
        .ok()
        .map(|s| s.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 4, 16]);
    let per_client: usize = std::env::var("FLUREE_TIMING_COMMITS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    for durability in [Durability::Wal, Durability::Sync] {
        for &n in &clients {
            for shared_ledger in [true, false] {
                let dir = tempfile::tempdir().unwrap();
                let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                    .without_indexing()
                    .with_storage_durability(durability)
                    .build()
                    .expect("build");
                let mut handles = Vec::with_capacity(n);
                for c in 0..n {
                    let id = if shared_ledger {
                        LEDGER.to_string()
                    } else {
                        format!("it/wal-{c}:main")
                    };
                    if !shared_ledger || c == 0 {
                        fluree.create_ledger(&id).await.expect("create");
                    }
                    let handle = fluree.ledger_cached(&id).await.expect("cache");
                    // Warm: one insert so the ledger has a head.
                    if !shared_ledger || c == 0 {
                        insert(&fluree, &handle, 1_000_000 + c).await;
                    }
                    handles.push(handle);
                }
                let probe = probe(dir.path());
                let fsyncs_before = probe.fsyncs_issued();

                let started = Instant::now();
                let mut tasks = Vec::with_capacity(n);
                for (c, handle) in handles.into_iter().enumerate() {
                    let fluree = fluree.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut samples = Vec::with_capacity(per_client);
                        let mut failures = Vec::new();
                        for i in 0..per_client {
                            let t = Instant::now();
                            let result = fluree
                                .stage(&handle)
                                .insert(&json!({
                                    "@context": { "ex": "http://example.org/" },
                                    "@id": format!("ex:person-{}", c * 1_000 + i),
                                    "ex:name": format!("Person {}", c * 1_000 + i),
                                }))
                                .execute()
                                .await;
                            match result {
                                Ok(_) => samples.push(t.elapsed()),
                                Err(e) => failures.push(e.to_string()),
                            }
                        }
                        (samples, failures)
                    }));
                }
                let mut samples: Vec<Duration> = Vec::with_capacity(n * per_client);
                let mut failures: Vec<String> = Vec::new();
                for task in tasks {
                    let (s, f) = task.await.expect("client task");
                    samples.extend(s);
                    failures.extend(f);
                }
                if !failures.is_empty() {
                    println!(
                        "  FAILURES: {} of {} commits failed; first: {}",
                        failures.len(),
                        n * per_client,
                        failures[0]
                    );
                }
                let wall = started.elapsed();
                let fsyncs = probe.fsyncs_issued() - fsyncs_before;
                samples.sort();
                let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
                let commits = samples.len();
                println!(
                    "{:<5} clients={n:<3} {:<12} commits={commits:<5} wall={:?} rate={:.0}/s median={:?} p95={:?} fsyncs/commit={:.2}",
                    match durability {
                        Durability::Wal => "wal",
                        Durability::Sync => "sync",
                        Durability::PageCache => "pagecache",
                    },
                    if shared_ledger { "one-ledger" } else { "n-ledgers" },
                    wall,
                    commits as f64 / wall.as_secs_f64(),
                    at(0.5),
                    at(0.95),
                    fsyncs as f64 / commits as f64,
                );
                fluree.disconnect().await;
            }
        }
    }
}
