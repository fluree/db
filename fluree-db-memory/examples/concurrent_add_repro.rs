//! Regression/stress harness for concurrent fluree-memory MCP adds.
//!
//! Reproduces the production scenario: one or more MCP servers handling
//! overlapping `memory_add` requests. Each MCP `memory_add` does
//! `ensure_synced()`, `store.add(..)`, `recall_fulltext(..)`, and then loads all
//! current memories for related-memory ranking. We seed a realistic store, then
//! fire N of those add+recall+load sequences concurrently on a multi-thread
//! runtime.
//!
//! This is intentionally an example, not a test: normal `cargo test` does not
//! run it. Use it manually when changing memory file-sync, MCP add handling, or
//! ledger-cache rebuild behavior.
//!
//! Run:
//!   cargo run --example concurrent_add_repro -p fluree-db-memory
//!   gtimeout 90 ./target/debug/examples/concurrent_add_repro 4 30 3
//!
//! Optional torture run:
//!   gtimeout 300 ./target/debug/examples/concurrent_add_repro 8 60 20
//!
//! Env knobs:
//!   REPRO_MODE=recall    run the older read-only recall stress path
//!   REPRO_INDEX=1        enable frequent background indexing
//!   REPRO_STORES=2       simulate multiple MCP servers sharing one storage dir

use fluree_db_api::FlureeBuilder;
use fluree_db_memory::{MemoryFilter, MemoryInput, MemoryKind, MemoryStore, Scope};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

fn mk(kind: MemoryKind, content: &str, tags: &[&str]) -> MemoryInput {
    MemoryInput {
        kind,
        content: content.to_string(),
        tags: tags.iter().map(std::string::ToString::to_string).collect(),
        scope: Scope::Repo,
        severity: None,
        artifact_refs: Vec::new(),
        branch: None,
        rationale: None,
        alternatives: None,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let concurrency: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let seed: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let rounds: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(3);

    // Match production (fluree-db-cli context::build_fluree): file-backed,
    // no ledger caching, no background indexer, novelty thresholds set.
    let tmp = std::env::temp_dir().join(format!("mem_repro_{}", std::process::id()));
    let storage = tmp.join("storage");
    let mem_dir = tmp.join(".fluree-memory");
    std::fs::create_dir_all(&storage).unwrap();
    std::fs::create_dir_all(&mem_dir).unwrap();

    let index = std::env::var("REPRO_INDEX").as_deref() == Ok("1");
    let mode = std::env::var("REPRO_MODE").unwrap_or_else(|_| "add".to_string());
    let store_count: usize = std::env::var("REPRO_STORES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1);

    eprintln!(
        "config: mode={mode} concurrency={concurrency} seed={seed} rounds={rounds} \
         stores={store_count} indexing={index} root={}",
        tmp.display()
    );

    let mut stores = Vec::with_capacity(store_count);
    for _ in 0..store_count {
        stores.push(Arc::new(build_store(&storage, &mem_dir, index)));
    }

    eprintln!("seeding {seed} memories...");
    for i in 0..seed {
        let t0 = Instant::now();
        stores[0]
            .add(mk(
                MemoryKind::Fact,
                &format!(
                    "Seed memory {i} about binary index leaflet cache overlay cursor \
                     novelty merge psot post opst query execution fast path number {i}"
                ),
                &["binary-index", "cache", "query", "overlay"],
            ))
            .await
            .expect("seed add");
        let dt = t0.elapsed();
        if i % 10 == 0 || dt.as_millis() > 200 {
            eprintln!("  seeded {i} (last add {dt:?})");
        }
    }
    eprintln!("seeded.");

    if mode == "recall" {
        run_recall_stress(stores[0].clone(), concurrency).await;
    } else {
        run_add_stress(stores, concurrency, rounds).await;
    }
}

fn build_store(storage: &Path, mem_dir: &Path, index: bool) -> MemoryStore {
    let mut b = FlureeBuilder::file(storage.to_string_lossy().to_string()).without_ledger_caching();
    b = if index {
        b.with_indexing_thresholds(8 * 1024, 256 * 1024)
    } else {
        b.without_indexing()
            .with_novelty_thresholds(50 * 1024 * 1024, 100 * 1024 * 1024)
    };
    let fluree = b.build().expect("build native fluree");
    // memory_dir = Some triggers the production file-sync read-modify-write path.
    MemoryStore::new(fluree, Some(mem_dir.to_path_buf()))
}

async fn run_recall_stress(store: Arc<MemoryStore>, concurrency: usize) {
    eprintln!("firing {concurrency} concurrent read-only recalls...");
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..concurrency {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let q = format!(
                "overlay cursor count merge psot post leaflet novelty worker {} fast path",
                t % 7
            );
            let mut total = 0usize;
            for _ in 0..5 {
                let hits = store.recall_fulltext(&q, 4).await.expect("recall_fulltext");
                total += hits.len();
            }
            total
        }));
    }

    for h in handles {
        let n = h.await.expect("task join");
        eprintln!("  recall task done ({n} hits)");
    }
    eprintln!("ALL DONE in {:?} - no spin reproduced", start.elapsed());
}

async fn run_add_stress(stores: Vec<Arc<MemoryStore>>, concurrency: usize, rounds: usize) {
    eprintln!("firing {concurrency} concurrent MCP-like add tasks ({rounds} rounds each)...");

    let started = Arc::new(AtomicUsize::new(0));
    let synced = Arc::new(AtomicUsize::new(0));
    let added = Arc::new(AtomicUsize::new(0));
    let recalled = Arc::new(AtomicUsize::new(0));
    let loaded = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    let watchdog = {
        let started = Arc::clone(&started);
        let synced = Arc::clone(&synced);
        let added = Arc::clone(&added);
        let recalled = Arc::clone(&recalled);
        let loaded = Arc::clone(&loaded);
        let completed = Arc::clone(&completed);
        let errors = Arc::clone(&errors);
        let finished = Arc::clone(&finished);
        tokio::spawn(async move {
            while !finished.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                eprintln!(
                    "  watchdog: elapsed={:?} started={} synced={} added={} recalled={} \
                     loaded={} completed={} errors={}",
                    start.elapsed(),
                    started.load(Ordering::Relaxed),
                    synced.load(Ordering::Relaxed),
                    added.load(Ordering::Relaxed),
                    recalled.load(Ordering::Relaxed),
                    loaded.load(Ordering::Relaxed),
                    completed.load(Ordering::Relaxed),
                    errors.load(Ordering::Relaxed)
                );
            }
        })
    };

    let mut handles = Vec::new();
    for task_id in 0..concurrency {
        let store = stores[task_id % stores.len()].clone();
        let started = Arc::clone(&started);
        let synced = Arc::clone(&synced);
        let added = Arc::clone(&added);
        let recalled = Arc::clone(&recalled);
        let loaded = Arc::clone(&loaded);
        let completed = Arc::clone(&completed);
        let errors = Arc::clone(&errors);
        handles.push(tokio::spawn(async move {
            for round in 0..rounds {
                started.fetch_add(1, Ordering::Relaxed);
                let content = format!(
                    "Concurrent MCP add task {task_id} round {round} about binary index \
                     leaflet cache overlay cursor novelty merge psot post opst query \
                     execution fast path contention"
                );
                let op_start = Instant::now();

                let result: Result<(String, usize, usize), String> = async {
                    // MCP calls ensure_initialized(), which includes ensure_synced(),
                    // before store.add(). Calling ensure_synced() here recreates the
                    // file-lock and rebuild checks on every operation.
                    store
                        .ensure_synced()
                        .await
                        .map_err(|e| format!("ensure_synced: {e}"))?;
                    synced.fetch_add(1, Ordering::Relaxed);
                    let id = store
                        .add(mk(
                            MemoryKind::Fact,
                            &content,
                            &["binary-index", "cache", "query", "overlay", "mcp"],
                        ))
                        .await
                        .map_err(|e| format!("add: {e}"))?;
                    added.fetch_add(1, Ordering::Relaxed);
                    let hits = store
                        .recall_fulltext(&content, 4)
                        .await
                        .map_err(|e| format!("recall_fulltext: {e}"))?;
                    recalled.fetch_add(1, Ordering::Relaxed);
                    let all = store
                        .current_memories(&MemoryFilter::default())
                        .await
                        .map_err(|e| format!("current_memories: {e}"))?;
                    loaded.fetch_add(1, Ordering::Relaxed);
                    Ok((id, hits.len(), all.len()))
                }
                .await;

                match result {
                    Ok((id, hits, total)) => {
                        let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                        let dt = op_start.elapsed();
                        if done.is_multiple_of(10) || dt > Duration::from_secs(1) {
                            eprintln!(
                                "  add done #{done}: task={task_id} round={round} id={id} \
                                 hits={hits} total={total} elapsed={dt:?}"
                            );
                        }
                    }
                    Err(e) => {
                        let n = errors.fetch_add(1, Ordering::Relaxed) + 1;
                        if n <= 20 || n.is_multiple_of(25) {
                            eprintln!("  add error #{n}: task={task_id} round={round}: {e}");
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("task join");
    }
    finished.store(true, Ordering::Relaxed);
    watchdog.abort();

    eprintln!(
        "ALL DONE in {:?}: started={} synced={} added={} recalled={} loaded={} \
         completed={} errors={}",
        start.elapsed(),
        started.load(Ordering::Relaxed),
        synced.load(Ordering::Relaxed),
        added.load(Ordering::Relaxed),
        recalled.load(Ordering::Relaxed),
        loaded.load(Ordering::Relaxed),
        completed.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed)
    );
}
