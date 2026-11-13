//! How long a purge pass takes over a ledger with a long indexed history, by
//! phase, and what it costs other ledgers' commits meanwhile.
//!
//! ```text
//! cargo run --release -p fluree-db-api --example purge_perf -- [commits] [subjects] [writers]
//! ```
//!
//! The holder gets `commits` commits of `subjects` subjects each, the first
//! also holding the value to erase. It is erased, indexed, proved with the
//! native test prover (proving's cost is PR 4's report, not this one's) and
//! published. Then `writers` ledgers commit continuously, and one purge pass
//! runs 5 s in. `PURGE_PERF_WINDOW=a,b` (ms since the writers started) skips
//! the purge and reports the commits within that window instead, for an A/B
//! in separate processes. Durability is PageCache unless `PURGE_PERF_WAL` is
//! set.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
use fluree_db_api::retention::FactSelector;
use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_core::Durability;
use fluree_db_erasure::testing::NativeProver;
use serde_json::{json, Value as JsonValue};

type Span = (Instant, Instant);

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/"})
}

fn report(label: &str, spans: &[Span]) {
    let mut latencies: Vec<Duration> = spans.iter().map(|(a, b)| *b - *a).collect();
    if latencies.is_empty() {
        println!("{label:<26} n=0");
        return;
    }
    latencies.sort();
    let at = |q: f64| latencies[((latencies.len() as f64 - 1.0) * q).round() as usize];
    let mean = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    println!(
        "{label:<26} n={:<7} mean={:>8.3}ms p50={:>8.3}ms p95={:>8.3}ms p99={:>8.3}ms max={:>8.3}ms",
        latencies.len(),
        mean.as_secs_f64() * 1e3,
        at(0.50).as_secs_f64() * 1e3,
        at(0.95).as_secs_f64() * 1e3,
        at(0.99).as_secs_f64() * 1e3,
        latencies[latencies.len() - 1].as_secs_f64() * 1e3,
    );
}

async fn build_holder(fluree: &Fluree, holder: &str, commits: usize, subjects: usize) {
    for c in 0..commits {
        let mut nodes: Vec<JsonValue> = (0..subjects)
            .map(|s| {
                let i = c * subjects + s;
                json!({
                    "@id": format!("ex:s{i}"),
                    "ex:name": format!("name {i}"),
                    "ex:n": i,
                    "ex:tag": format!("tag{}", i % 100)
                })
            })
            .collect();
        if c == 0 {
            nodes.push(json!({"@id": "ex:alice", "ex:email": "a@secret"}));
        }
        fluree
            .graph(holder)
            .transact()
            .insert(&json!({"@context": ctx(), "@graph": nodes}))
            .commit()
            .await
            .expect("holder commit");
    }
}

async fn write(fluree: Arc<Fluree>, ledger: String, stop: Arc<AtomicBool>) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut i = 0usize;
    while !stop.load(Ordering::Relaxed) {
        let body = json!({
            "@context": ctx(),
            "@id": format!("ex:w{i}"),
            "ex:name": format!("w{i}"),
            "ex:n": i
        });
        let started = Instant::now();
        fluree
            .graph(&ledger)
            .transact()
            .insert(&body)
            .commit()
            .await
            .expect("commit");
        spans.push((started, Instant::now()));
        i += 1;
    }
    spans
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let arg = |i: usize, default: usize| {
        std::env::args()
            .nth(i)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };
    let (commits, subjects, writers) = (arg(1, 10_000), arg(2, 10), arg(3, 4));
    let durability = if std::env::var_os("PURGE_PERF_WAL").is_some() {
        Durability::Wal
    } else {
        Durability::PageCache
    };
    let dir = tempfile::tempdir().unwrap();
    let fluree = Arc::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .with_storage_durability(durability)
            .with_erasure(ErasureConfig {
                prover: Arc::new(NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
            .build()
            .expect("build"),
    );
    println!("durability {durability:?}");

    let holder = "perf/holder:main";
    fluree.create_ledger("perf/holder").await.unwrap();
    let clock = Instant::now();
    build_holder(&fluree, holder, commits, subjects).await;
    println!(
        "holder: {commits} commits of {subjects} subjects in {:.1}s",
        clock.elapsed().as_secs_f64()
    );
    let clock = Instant::now();
    let request = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!("a@secret")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences(holder, request, None)
        .await
        .expect("erase");
    fluree
        .reindex(holder, ReindexOptions::default())
        .await
        .expect("reindex");
    let published = fluree.run_erasure_pass(holder).await.unwrap().published;
    assert_eq!(published, 1);
    println!(
        "erased, indexed and published in {:.1}s",
        clock.elapsed().as_secs_f64()
    );

    for w in 0..writers {
        fluree.create_ledger(&format!("perf/w{w}")).await.unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|w| {
            tokio::spawn(write(
                Arc::clone(&fluree),
                format!("perf/w{w}:main"),
                Arc::clone(&stop),
            ))
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let window = match std::env::var("PURGE_PERF_WINDOW") {
        Ok(window) => {
            let (a, b) = window.split_once(',').expect("a,b");
            let ms = |v: &str| start + Duration::from_millis(v.parse().expect("ms"));
            let window = (ms(a), ms(b));
            tokio::time::sleep_until(window.1.into()).await;
            window
        }
        Err(_) => {
            let a = Instant::now();
            let report = fluree
                .run_purge_pass(holder, Duration::ZERO)
                .await
                .expect("purge");
            let b = Instant::now();
            assert_eq!(report.purged, 1, "{report:?}");
            let t = report.timings;
            println!(
                "purge: {:.2}s, deleted {}, leafwise {}, rebuilt {}; plan {:.2}s index {:.2}s \
                 release {:.2}s scan {:.2}s delete {:.2}s",
                (b - a).as_secs_f64(),
                report.deleted,
                report.leafwise,
                report.rebuilt,
                t.plan.as_secs_f64(),
                t.index.as_secs_f64(),
                t.release.as_secs_f64(),
                t.scan.as_secs_f64(),
                t.delete.as_secs_f64(),
            );
            (a, b)
        }
    };
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    let mut spans = Vec::new();
    for task in tasks {
        spans.extend(task.await.unwrap());
    }
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
    report("commits within the window", &within);
    report("all commits", &spans);
}
