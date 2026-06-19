//! In-process concurrency-scaling micro-benchmark for the read/query path.
//!
//! Drives many concurrent simple SPARQL queries against a single cached ledger
//! and reports aggregate QPS at increasing concurrency. This isolates the
//! `LedgerHandle::snapshot()` read path (which clones `LedgerSnapshot` under an
//! exclusive Mutex per query) without HTTP/client noise, so it directly
//! measures whether concurrent reads scale across cores.
//!
//! Manual run (release-ish optimization matters for absolute numbers, but the
//! SCALING ratio is the signal):
//!   cargo test -p fluree-db-api --test it_scaling_bench --features native --release -- --ignored --nocapture
//!
//! Interpreting output: look at QPS(C)/QPS(1). Linear-ish scaling => reads run
//! concurrently. Flat after a few cores => a read-path serialization bottleneck.

#![cfg(feature = "native")]

mod support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::json;

const SEED_SUBJECTS: usize = 8000;
const RUN_SECS: u64 = 3;
const CONCURRENCIES: &[usize] = &[1, 2, 4, 8, 16];

// A trivial lookup-style query (BSBM-Explore-class): tiny work, so per-query
// fixed overhead (parse/plan/snapshot) dominates — exactly where serialization
// on the snapshot clone shows up.
const QUERY: &str = "PREFIX ex: <http://ex/> SELECT ?name WHERE { ex:s42 ex:name ?name }";

async fn setup(path: &str) -> (Arc<Fluree>, String) {
    let fluree = FlureeBuilder::file(path.to_string())
        .build()
        .expect("build");
    let ledger_id = "bench/scaling:main";

    let l0 = fluree.create_ledger(ledger_id).await.expect("create");
    // Many namespaces/predicates would enlarge the snapshot clone; one ns with
    // many subjects is enough to populate stats/schema and exercise the path.
    let g: Vec<_> = (0..SEED_SUBJECTS)
        .map(|i| json!({ "@id": format!("http://ex/s{i}"), "http://ex/name": format!("n{i}"), "http://ex/k": i }))
        .collect();
    let tx = json!({ "@graph": g });
    fluree.insert(l0, &tx).await.expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // Warm the cache so every bench query hits the cached snapshot() path.
    let _ = fluree.ledger_cached(ledger_id).await.expect("cache");
    eprintln!("caching_enabled = {}", fluree.is_caching_enabled());

    (Arc::new(fluree), ledger_id.to_string())
}

async fn run_concurrency(fluree: &Arc<Fluree>, ledger_id: &str, concurrency: usize) -> f64 {
    let counter = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let fluree = Arc::clone(fluree);
        let ledger_id = ledger_id.to_string();
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            let mut local = 0u64;
            while Instant::now() < deadline {
                let r = fluree
                    .graph(&ledger_id)
                    .query()
                    .sparql(QUERY)
                    .execute()
                    .await;
                if r.is_ok() {
                    local += 1;
                }
            }
            counter.fetch_add(local, Ordering::Relaxed);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    counter.load(Ordering::Relaxed) as f64 / elapsed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "manual scaling benchmark"]
async fn scaling_bench() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, ledger_id) = setup(&tmp.path().to_string_lossy()).await;

    // Sanity: query returns the expected single row.
    let r = fluree
        .graph(&ledger_id)
        .query()
        .sparql(QUERY)
        .execute()
        .await
        .expect("warm query");
    eprintln!("sanity rows = {}", r.row_count());

    eprintln!("\n=== concurrency scaling (in-process, {RUN_SECS}s per level) ===");
    eprintln!("{:>6}  {:>12}  {:>8}", "conc", "QPS", "speedup");
    let mut base = 0.0f64;
    for (i, &c) in CONCURRENCIES.iter().enumerate() {
        let qps = run_concurrency(&fluree, &ledger_id, c).await;
        if i == 0 {
            base = qps;
        }
        eprintln!("{c:>6}  {qps:>12.0}  {:>7.2}x", qps / base);
    }
    eprintln!("=== end ===\n");
}
