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
//! lane      <unpinned|pinned|looping> [writers] [commits] <file>
//!                                          one idle lane in its own process,
//!                                          latencies (µs) written to `file`
//! fixed     [writers] [commits] [at_s]     one pass at_s into the run; the
//!                                          commits within its window, or the
//!                                          window GARBAGE_PERF_WINDOW=a,b ms
//!                                          gives a run without it
//! cadence   [writers] [rate] [minutes] [interval_s]
//!                                          writers at rate commits/s each,
//!                                          housekeeping's pass every
//!                                          interval_s (default 300) with its
//!                                          grace and budget; per pass, the
//!                                          backlog it leaves
//! ```
//!
//! Writers commit to their own ledgers with raw bodies stored, and every
//! tenth transaction fails while staging, so garbage keeps arriving, unless
//! `GARBAGE_PERF_NO_FAILURES` is set. `GARBAGE_PERF_NO_PASSES` keeps
//! `scheduled`'s, `fixed`'s and `cadence`'s collector idle, for an A/B of
//! whole runs.
//! Durability is PageCache unless `GARBAGE_PERF_WAL` is set.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::{Fluree, FlureeBuilder, GarbageReport, HousekeepingOptions, ReconcilePass};
use fluree_db_core::{Durability, PinWaits};
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

fn fails(i: usize) -> bool {
    i % 10 == 9 && std::env::var_os("GARBAGE_PERF_NO_FAILURES").is_none()
}

fn body(writer: usize, i: usize) -> JsonValue {
    if fails(i) {
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

/// What a transaction must do.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    Commits,
    /// Succeeds without a commit: its stored body is abandoned.
    ChangesNothing,
    Fails,
}

fn expected(i: usize) -> Expect {
    if fails(i) {
        Expect::Fails
    } else {
        Expect::Commits
    }
}

/// Transact `body` on `ledger_id`, whose head was at `t`, and check that it
/// did as `expect`ed; the span it took when it committed.
async fn commit(
    fluree: &Fluree,
    ledger_id: &str,
    body: &JsonValue,
    expect: Expect,
    t: &mut i64,
) -> Option<(Instant, Instant)> {
    let started = Instant::now();
    let result = fluree
        .graph(ledger_id)
        .transact()
        .txn_opts(TxnOpts::default().store_raw_txn(true))
        .update(body)
        .commit()
        .await;
    let ended = Instant::now();
    let outcome = match &result {
        Ok(r) if r.receipt.t == *t + 1 => Expect::Commits,
        Ok(r) if r.receipt.t == *t => Expect::ChangesNothing,
        Ok(r) => panic!("{ledger_id}: committed t {} after {t}: {body}", r.receipt.t),
        Err(_) => Expect::Fails,
    };
    assert_eq!(outcome, expect, "{ledger_id}: {:?}: {body}", result.err());
    if outcome == Expect::Commits {
        *t += 1;
        return Some((started, ended));
    }
    None
}

/// Pins that waited for the collector, since `before`.
fn waits_since(fluree: &Fluree, before: PinWaits) -> String {
    let now = fluree.pin_waits().unwrap_or_default();
    let count = now.count - before.count;
    let total = now.total - before.total;
    let mean = if count == 0 {
        Duration::ZERO
    } else {
        total / count as u32
    };
    format!(
        "pins that waited for a claim: {count}, {:.3}ms in all, {:.3}ms mean",
        total.as_secs_f64() * 1e3,
        mean.as_secs_f64() * 1e3
    )
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
            let (mut spans, mut t) = (Vec::new(), 0);
            for i in 0..commits {
                if let Some(span) = commit(&fluree, &id, &body(w, i), expected(i), &mut t).await {
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

fn passes_off() -> bool {
    std::env::var_os("GARBAGE_PERF_NO_PASSES").is_some()
}

/// `writers` ledgers, each written `rate` times a second for `run`; the
/// spans of the commits that succeeded.
async fn write_paced(fluree: &Arc<Fluree>, writers: usize, rate: f64, run: Duration) -> Vec<Span> {
    for w in 0..writers {
        fluree
            .create_ledger(&format!("perf{w}"))
            .await
            .expect("create");
    }
    let end = Instant::now() + run;
    let mut tasks = Vec::new();
    for w in 0..writers {
        let fluree = Arc::clone(fluree);
        tasks.push(tokio::spawn(async move {
            let id = format!("perf{w}:main");
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let (mut spans, mut t, mut i) = (Vec::new(), 0, 0);
            while Instant::now() < end {
                tick.tick().await;
                if let Some(span) = commit(&fluree, &id, &body(w, i), expected(i), &mut t).await {
                    spans.push(span);
                }
                i += 1;
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

/// One collection pass, as housekeeping runs it: collect, then reconcile
/// every ledger, pausing after each step as long as it took.
async fn pass(fluree: &Fluree) -> GarbageReport {
    phases(fluree).await.0
}

type Span = (Instant, Instant);

/// [`pass`], with the span of each phase.
async fn phases(fluree: &Fluree) -> (GarbageReport, Span, ReconcilePass, Span) {
    let started = Instant::now();
    let collected = fluree
        .collect_garbage_within(Duration::ZERO, Some(Duration::from_secs(3600)))
        .await
        .expect("collect");
    let collection = (started, Instant::now());
    let reconciled = fluree
        .reconcile_due(Duration::ZERO, Duration::from_secs(3600))
        .await
        .expect("reconcile");
    (
        collected,
        collection,
        reconciled,
        (collection.1, Instant::now()),
    )
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
            let collector = looping.then(|| {
                let (fluree, stop) = (Arc::clone(&fluree), Arc::clone(&stop));
                tokio::spawn(async move {
                    // The ledgers are created by the writers.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    while !stop.load(Ordering::Relaxed) {
                        pass(&fluree).await;
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

async fn lane(name: &str, writers: usize, commits: usize, file: &str) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, name != "unpinned").await;
    let stop = Arc::new(AtomicBool::new(false));
    let collector = (name == "looping").then(|| {
        let (fluree, stop) = (Arc::clone(&fluree), Arc::clone(&stop));
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            while !stop.load(Ordering::Relaxed) {
                pass(&fluree).await;
            }
        })
    });
    let spans = write(&fluree, writers, commits).await;
    stop.store(true, Ordering::Relaxed);
    if let Some(collector) = collector {
        collector.await.unwrap();
    }
    let lines: Vec<String> = latencies(&spans)
        .iter()
        .map(|d| d.as_micros().to_string())
        .collect();
    std::fs::write(file, lines.join("\n")).unwrap();
}

async fn scheduled(writers: usize, commits: usize, every: Duration) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    let stop = Arc::new(AtomicBool::new(false));
    let passes: Arc<Mutex<Vec<(Span, Span)>>> = Arc::default();
    let collector = {
        let (fluree, stop, passes) = (Arc::clone(&fluree), Arc::clone(&stop), Arc::clone(&passes));
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(every).await;
                if std::env::var_os("GARBAGE_PERF_NO_PASSES").is_some() {
                    continue;
                }
                let (collected, collection, reconciled, reconciliation) = phases(&fluree).await;
                let files: usize = reconciled.ledgers.iter().map(|(_, r)| r.checked).sum();
                println!(
                    "pass: collected {} deleted {} in {:.2}s; reconciled {} files of {} ledgers in {:.2}s",
                    collected.recorded,
                    collected.deleted,
                    (collection.1 - collection.0).as_secs_f64(),
                    files,
                    reconciled.ledgers.len(),
                    (reconciliation.1 - reconciliation.0).as_secs_f64(),
                );
                println!("  collection {:?}", collected.timings);
                for (ledger, report) in &reconciled.ledgers {
                    println!(
                        "  {ledger}: {} files in {} steps, {} recorded, {:?}",
                        report.checked, report.steps, report.recorded, report.timings
                    );
                }
                passes.lock().push((collection, reconciliation));
            }
        })
    };
    let spans = write(&fluree, writers, commits).await;
    stop.store(true, Ordering::Relaxed);
    collector.await.unwrap();
    let passes = passes.lock().clone();
    let overlaps = |(a, b): &Span, (s, e): &Span| a < e && s < b;
    let mut during_c = Vec::new();
    let mut during_r = Vec::new();
    let mut apart = Vec::new();
    for span in &spans {
        if passes.iter().any(|(c, _)| overlaps(span, c)) {
            during_c.push(*span);
        } else if passes.iter().any(|(_, r)| overlaps(span, r)) {
            during_r.push(*span);
        } else {
            apart.push(*span);
        }
    }
    report("all commits", latencies(&spans));
    report("commits collection overlapped", latencies(&during_c));
    report("commits reconciliation overlapped", latencies(&during_r));
    report("commits no pass overlapped", latencies(&apart));
}

async fn fixed(writers: usize, commits: usize, at: Duration) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    let start = Instant::now();
    let collector = {
        let fluree = Arc::clone(&fluree);
        tokio::spawn(async move {
            tokio::time::sleep(at).await;
            if passes_off() {
                return None;
            }
            let (collected, collection, reconciled, reconciliation) = phases(&fluree).await;
            let files: usize = reconciled.ledgers.iter().map(|(_, r)| r.checked).sum();
            println!(
                "pass: collected {} deleted {} in {:.2}s; reconciled {} files in {:.2}s",
                collected.recorded,
                collected.deleted,
                (collection.1 - collection.0).as_secs_f64(),
                files,
                (reconciliation.1 - reconciliation.0).as_secs_f64(),
            );
            Some((collection.0, reconciliation.1))
        })
    };
    let spans = write(&fluree, writers, commits).await;
    let window = match std::env::var("GARBAGE_PERF_WINDOW") {
        Ok(window) => {
            let (a, b) = window.split_once(',').expect("a,b");
            let ms = |v: &str| start + Duration::from_millis(v.parse().expect("ms"));
            Some((ms(a), ms(b)))
        }
        Err(_) => collector.await.unwrap(),
    };
    let Some(window) = window else {
        report("all commits", latencies(&spans));
        return;
    };
    println!(
        "window {},{}",
        (window.0 - start).as_millis(),
        (window.1 - start).as_millis()
    );
    let within: Vec<Span> = spans
        .iter()
        .filter(|(a, b)| *a < window.1 && window.0 < *b)
        .copied()
        .collect();
    report("commits within the window", latencies(&within));
    report("all commits", latencies(&spans));
}

async fn cadence(writers: usize, rate: f64, minutes: u64, interval: Duration) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = instance(&dir, true).await;
    let options = HousekeepingOptions::default();
    let stop = Arc::new(AtomicBool::new(false));
    let passes: Arc<Mutex<Vec<Span>>> = Arc::default();
    let start = Instant::now();
    let collector = {
        let (fluree, stop, passes) = (Arc::clone(&fluree), Arc::clone(&stop), Arc::clone(&passes));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                if passes_off() {
                    continue;
                }
                let began = Instant::now();
                let collected = fluree
                    .collect_garbage_within(options.garbage_grace, Some(options.reconcile_budget))
                    .await
                    .expect("collect");
                let reconciled = fluree
                    .reconcile_due(options.reconcile_interval, options.reconcile_budget)
                    .await
                    .expect("reconcile");
                let ended = Instant::now();
                passes.lock().push((began, ended));
                let now = chrono::Utc::now().timestamp_millis();
                let files: usize = reconciled.ledgers.iter().map(|(_, r)| r.checked).sum();
                println!(
                    "+{:>4}s pass {:>5.2}s: abandoned {} deleted {} deferred {} left {} \
                     ledgers left {}; backlog {} (waiting {}), oldest {}s; reconciled {} \
                     files of {} ledgers, {} due",
                    (began - start).as_secs(),
                    (ended - began).as_secs_f64(),
                    collected.recorded,
                    collected.deleted,
                    collected.deferred,
                    collected.left,
                    collected.ledgers_left,
                    collected.waiting + collected.deferred + collected.left,
                    collected.waiting,
                    collected.oldest_ms.map_or(0, |ms| (now - ms) / 1000),
                    files,
                    reconciled.ledgers.len(),
                    reconciled.due,
                );
            }
        })
    };
    let spans = write_paced(&fluree, writers, rate, Duration::from_secs(60 * minutes)).await;
    stop.store(true, Ordering::Relaxed);
    collector.abort();
    let passes = passes.lock().clone();
    let (during, apart): (Vec<Span>, Vec<Span>) = spans
        .iter()
        .partition(|(a, b)| passes.iter().any(|(s, e)| a < e && s < b));
    report("all commits", latencies(&spans));
    report("commits a pass overlapped", latencies(&during));
    report("commits no pass overlapped", latencies(&apart));
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
    // A `fresh` body has the same shape, on its own flag, and is never
    // submitted before it commits.
    let when = |kind: &str, w: usize, i: usize| {
        json!({
            "@context": ctx(),
            "where": {"@id": format!("ex:{kind}flag{w}-{i}"), "ex:on": true},
            "insert": {"@id": format!("ex:{kind}{w}-{i}"), "ex:name": format!("{kind}{w}-{i}")}
        })
    };
    for (w, name) in names.iter().enumerate() {
        let id = format!("{name}:main");
        for i in 0..commits {
            commit(
                &fluree,
                &id,
                &when("r", w, i),
                Expect::ChangesNothing,
                &mut 0,
            )
            .await;
        }
    }
    // Recorded, not yet due: the looping collector below takes them on
    // while the writers commit the same bodies.
    fluree
        .collect_garbage(Duration::from_secs(3600))
        .await
        .expect("record");
    let before = fluree.pin_waits().unwrap_or_default();
    let stop = Arc::new(AtomicBool::new(false));
    let collector = {
        let (fluree, stop) = (Arc::clone(&fluree), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut deleted = 0;
            while !stop.load(Ordering::Relaxed) {
                deleted += pass(&fluree).await.deleted;
            }
            deleted
        })
    };
    let mut tasks = Vec::new();
    for w in 0..writers {
        let fluree = Arc::clone(&fluree);
        tasks.push(tokio::spawn(async move {
            let id = format!("perf{w}:main");
            let (mut retried, mut fresh, mut t) = (Vec::new(), Vec::new(), 0);
            for i in 0..commits {
                let flags = json!({"@context": ctx(), "insert": [
                    {"@id": format!("ex:rflag{w}-{i}"), "ex:on": true},
                    {"@id": format!("ex:fflag{w}-{i}"), "ex:on": true}
                ]});
                commit(&fluree, &id, &flags, Expect::Commits, &mut t).await;
                if let Some((a, b)) =
                    commit(&fluree, &id, &when("r", w, i), Expect::Commits, &mut t).await
                {
                    retried.push(b - a);
                }
                if let Some((a, b)) =
                    commit(&fluree, &id, &when("f", w, i), Expect::Commits, &mut t).await
                {
                    fresh.push(b - a);
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
    let deleted = collector.await.unwrap();
    report("body recorded as garbage, committed", retried);
    report("same shape, fresh body, committed", fresh);
    println!(
        "collector deleted {deleted} while the writers ran; {}",
        waits_since(&fluree, before)
    );
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
            let (mut spans, mut i, mut t) = (Vec::new(), 0, 0);
            while !stop.load(Ordering::Relaxed) {
                if let Some(span) =
                    commit(&fluree, "small:main", &body(0, i), expected(i), &mut t).await
                {
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
        Some("lane") => {
            let name = args.get(1).expect("lane name");
            let file = args.get(4).expect("output file");
            lane(name, arg(2, 8), arg(3, 300), file).await;
        }
        Some("fixed") => {
            fixed(
                arg(1, 8),
                arg(2, 100_000),
                Duration::from_secs(arg(3, 30) as u64),
            )
            .await;
        }
        Some("cadence") => {
            cadence(
                arg(1, 8),
                arg(2, 50) as f64,
                arg(3, 40) as u64,
                Duration::from_secs(arg(4, 300) as u64),
            )
            .await;
        }
        _ => eprintln!("usage: garbage_perf idle|scheduled|retries|large|fixed|cadence [args]"),
    }
}
