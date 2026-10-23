//! Commit latency around commit and transaction garbage collection.
//!
//! ```text
//! cargo run --release -p fluree-db-api --example garbage_perf -- <mode> [args]
//!
//! idle      [writers] [commits] [rounds]   pinned vs unpinned, interleaved
//! scheduled [writers] [commits] [every_ms] a pass every every_ms; commits a
//!                                          pass overlapped, apart
//! retries   [writers] [commits]            bodies first abandoned, then
//!                                          committed while the collector
//!                                          works through many candidates
//! large     [commits] [strays]             reconcile a ledger of `commits`
//!                                          commits while another commits
//! ```
//!
//! Writers commit to their own ledgers with raw bodies stored, and every
//! tenth transaction fails while staging, so garbage keeps arriving.
//! Durability is PageCache unless `GARBAGE_PERF_WAL` is set.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::Durability;
use fluree_db_transact::TxnOpts;
use parking_lot::Mutex;
use serde_json::{json, Value as JsonValue};

fn durability() -> Durability {
    if std::env::var_os("GARBAGE_PERF_WAL").is_some() {
        Durability::Wal
    } else {
        Durability::PageCache
    }
}

async fn instance(dir: &tempfile::TempDir, locked: bool) -> Arc<Fluree> {
    let mut builder = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_storage_durability(durability());
    if !locked {
        builder = builder.without_storage_lock();
    }
    Arc::new(builder.build_async().await.expect("build"))
}

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/ns/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

fn body(writer: usize, i: usize) -> JsonValue {
    if i % 10 == 9 {
        // Fails while staging, after its body is stored.
        json!({
            "@context": ctx(),
            "insert": {"@id": format!("ex:f{writer}-{i}"), "ex:age": {"@value": "x", "@type": "xsd:integer"}}
        })
    } else {
        json!({
            "@context": ctx(),
            "insert": {"@id": format!("ex:s{writer}-{i}"), "ex:name": format!("n{writer}-{i}"), "ex:n": i}
        })
    }
}

/// Transact `body` on `ledger_id`; the span it took when it committed.
async fn commit(fluree: &Fluree, ledger_id: &str, body: &JsonValue) -> Option<(Instant, Instant)> {
    let started = Instant::now();
    fluree
        .graph(ledger_id)
        .transact()
        .txn_opts(TxnOpts::default().store_raw_txn(true))
        .update(body)
        .commit()
        .await
        .ok()
        .map(|_| (started, Instant::now()))
}

fn report(label: &str, mut latencies: Vec<Duration>) {
    if latencies.is_empty() {
        println!("{label:<34} n=0");
        return;
    }
    latencies.sort();
    let at = |q: f64| latencies[((latencies.len() as f64 - 1.0) * q).round() as usize];
    let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    println!(
        "{label:<34} n={:<6} mean={:>8.3}ms p50={:>8.3}ms p95={:>8.3}ms p99={:>8.3}ms max={:>8.3}ms",
        latencies.len(),
        mean.as_secs_f64() * 1e3,
        at(0.50).as_secs_f64() * 1e3,
        at(0.95).as_secs_f64() * 1e3,
        at(0.99).as_secs_f64() * 1e3,
        latencies[latencies.len() - 1].as_secs_f64() * 1e3,
    );
}

/// `writers` ledgers, each written `commits` times concurrently; the spans
/// of the commits that succeeded after a warm-up of 20.
async fn write(fluree: &Arc<Fluree>, writers: usize, commits: usize) -> Vec<(Instant, Instant)> {
    for w in 0..writers {
        fluree
            .create_ledger(&format!("perf{w}"))
            .await
            .expect("create");
    }
    let mut tasks = Vec::new();
    for w in 0..writers {
        let fluree = Arc::clone(fluree);
        tasks.push(tokio::spawn(async move {
            let id = format!("perf{w}:main");
            let mut spans = Vec::new();
            for i in 0..commits {
                if let Some(span) = commit(&fluree, &id, &body(w, i)).await {
                    if i >= 20 {
                        spans.push(span);
                    }
                }
            }
            spans
        }));
    }
    let mut spans = Vec::new();
    for task in tasks {
        spans.extend(task.await.expect("writer"));
    }
    spans
}

/// One collection pass: collect, then reconcile each ledger in `names`.
async fn pass(fluree: &Fluree, names: &[String]) {
    fluree
        .collect_garbage(Duration::ZERO)
        .await
        .expect("collect");
    for name in names {
        fluree.reconcile_garbage(name).await.expect("reconcile");
    }
}

fn latencies(spans: &[(Instant, Instant)]) -> Vec<Duration> {
    spans.iter().map(|(a, b)| *b - *a).collect()
}

async fn idle(writers: usize, commits: usize, rounds: usize) {
    let lanes = [
        ("unpinned", false, false),
        ("pinned", true, false),
        ("pinned, collector looping", true, true),
    ];
    let mut results: Vec<Vec<Duration>> = vec![Vec::new(); lanes.len()];
    for _ in 0..rounds {
        for (i, (_, locked, looping)) in lanes.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let fluree = instance(&dir, *locked).await;
            let stop = Arc::new(AtomicBool::new(false));
            let names: Vec<String> = (0..writers).map(|w| format!("perf{w}")).collect();
            let collector = looping.then(|| {
                let (fluree, stop, names) = (Arc::clone(&fluree), Arc::clone(&stop), names.clone());
                tokio::spawn(async move {
                    // The ledgers are created by the writers.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    while !stop.load(Ordering::Relaxed) {
                        pass(&fluree, &names).await;
                    }
                })
            });
            results[i].extend(latencies(&write(&fluree, writers, commits).await));
            stop.store(true, Ordering::Relaxed);
            if let Some(collector) = collector {
                collector.await.unwrap();
            }
        }
    }
    for ((label, _, _), latencies) in lanes.iter().zip(results) {
        report(label, latencies);
    }
}

async fn scheduled(writers: usize, commits: usize, every: Duration) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    let names: Vec<String> = (0..writers).map(|w| format!("perf{w}")).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let passes: Arc<Mutex<Vec<(Instant, Instant)>>> = Arc::default();
    let collector = {
        let (fluree, stop, passes) = (Arc::clone(&fluree), Arc::clone(&stop), Arc::clone(&passes));
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(every).await;
                let started = Instant::now();
                pass(&fluree, &names).await;
                passes.lock().push((started, Instant::now()));
            }
        })
    };
    let spans = write(&fluree, writers, commits).await;
    stop.store(true, Ordering::Relaxed);
    collector.await.unwrap();
    let passes = passes.lock().clone();
    let (during, apart): (Vec<_>, Vec<_>) = spans
        .iter()
        .partition(|(a, b)| passes.iter().any(|(s, e)| a < e && s < b));
    report("commits a pass overlapped", latencies(&during));
    report("commits no pass overlapped", latencies(&apart));
    report("passes", latencies(&passes));
}

async fn retries(writers: usize, commits: usize) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    let names: Vec<String> = (0..writers).map(|w| format!("perf{w}")).collect();
    for name in &names {
        fluree.create_ledger(name).await.expect("create");
    }
    // Each `when` body changes nothing until its flag is set, so its first
    // submission is abandoned and recorded; the second, the same bytes,
    // commits once the flag is set.
    let when = |w: usize, i: usize| {
        json!({
            "@context": ctx(),
            "where": {"@id": format!("ex:flag{w}-{i}"), "ex:on": true},
            "insert": {"@id": format!("ex:r{w}-{i}"), "ex:name": format!("r{w}-{i}")}
        })
    };
    for (w, name) in names.iter().enumerate() {
        let id = format!("{name}:main");
        for i in 0..commits {
            commit(&fluree, &id, &when(w, i)).await;
        }
    }
    // Recorded, not yet due: the looping collector below takes them on
    // while the writers commit the same bodies.
    fluree
        .collect_garbage(Duration::from_secs(3600))
        .await
        .expect("record");
    let stop = Arc::new(AtomicBool::new(false));
    let collector = {
        let (fluree, stop, names) = (Arc::clone(&fluree), Arc::clone(&stop), names.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                pass(&fluree, &names).await;
            }
        })
    };
    let mut tasks = Vec::new();
    for w in 0..writers {
        let fluree = Arc::clone(&fluree);
        tasks.push(tokio::spawn(async move {
            let id = format!("perf{w}:main");
            let (mut retried, mut fresh) = (Vec::new(), Vec::new());
            for i in 0..commits {
                let flag = json!({"@context": ctx(), "insert": {"@id": format!("ex:flag{w}-{i}"), "ex:on": true}});
                if let Some((a, b)) = commit(&fluree, &id, &flag).await {
                    fresh.push(b - a);
                }
                if let Some((a, b)) = commit(&fluree, &id, &when(w, i)).await {
                    retried.push(b - a);
                }
            }
            (retried, fresh)
        }));
    }
    let (mut retried, mut fresh) = (Vec::new(), Vec::new());
    for task in tasks {
        let (r, f) = task.await.unwrap();
        retried.extend(r);
        fresh.extend(f);
    }
    stop.store(true, Ordering::Relaxed);
    collector.await.unwrap();
    report("body recorded as garbage, committed", retried);
    report("fresh body, committed", fresh);
}

async fn large(commits: usize, strays: usize) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    fluree.create_ledger("big").await.expect("create");
    fluree.create_ledger("small").await.expect("create");
    let built = Instant::now();
    for i in 0..commits {
        fluree
            .graph("big:main")
            .transact()
            .txn_opts(TxnOpts::default().store_raw_txn(true))
            .insert(&json!({"@context": ctx(), "@id": format!("ex:b{i}"), "ex:n": i}))
            .commit()
            .await
            .expect("commit");
    }
    let namespace = fluree.storage_namespace("big:main").await.unwrap();
    for n in 0..strays {
        fluree
            .content_store(&namespace)
            .put(
                fluree_db_core::ContentKind::Txn,
                format!("{{\"stray\":{n}}}").as_bytes(),
            )
            .await
            .unwrap();
    }
    println!(
        "built {commits} commits and {strays} strays in {:.1}s",
        built.elapsed().as_secs_f64()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (fluree, stop) = (Arc::clone(&fluree), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut spans = Vec::new();
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                if let Some(span) = commit(&fluree, "small:main", &body(0, i)).await {
                    spans.push(span);
                }
                i += 1;
            }
            spans
        })
    };
    tokio::time::sleep(Duration::from_secs(5)).await;
    let started = Instant::now();
    let report_r = fluree
        .reconcile_garbage("big")
        .await
        .unwrap()
        .expect("settled");
    let reconciled = (started, Instant::now());
    let collected_at = Instant::now();
    let collected = fluree.collect_garbage(Duration::ZERO).await.unwrap();
    let collection = (collected_at, Instant::now());
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    let spans = writer.await.unwrap();
    println!(
        "reconcile: {report_r:?} in {:.2}s; collect: {collected:?} in {:.2}s",
        (reconciled.1 - reconciled.0).as_secs_f64(),
        (collection.1 - collection.0).as_secs_f64()
    );
    let overlaps = |(a, b): &&(Instant, Instant), (s, e): (Instant, Instant)| *a < e && s < *b;
    let during_r: Vec<_> = spans
        .iter()
        .filter(|c| overlaps(c, reconciled))
        .copied()
        .collect();
    let during_c: Vec<_> = spans
        .iter()
        .filter(|c| overlaps(c, collection))
        .copied()
        .collect();
    let apart: Vec<_> = spans
        .iter()
        .filter(|c| !overlaps(c, reconciled) && !overlaps(c, collection))
        .copied()
        .collect();
    report("small: during reconciliation", latencies(&during_r));
    report("small: during collection", latencies(&during_c));
    report("small: neither", latencies(&apart));
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg =
        |i: usize, default: usize| args.get(i).and_then(|a| a.parse().ok()).unwrap_or(default);
    println!("durability {:?}", durability());
    match args.first().map(String::as_str) {
        Some("idle") => idle(arg(1, 8), arg(2, 200), arg(3, 3)).await,
        Some("scheduled") => {
            scheduled(
                arg(1, 8),
                arg(2, 1000),
                Duration::from_millis(arg(3, 2000) as u64),
            )
            .await;
        }
        Some("retries") => retries(arg(1, 8), arg(2, 200)).await,
        Some("large") => large(arg(1, 100_000), arg(2, 1000)).await,
        _ => eprintln!("usage: garbage_perf idle|scheduled|retries|large [args]"),
    }
}
