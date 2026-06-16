//! Background-indexing throughput harness — the realistic write path.
//!
//! Unlike `transact_growth_profile` (which disables indexing and forces a
//! synchronous `reindex()`), this drives the **real background indexer**: a
//! file-backed `Fluree` with `with_indexing_thresholds(min, max)` spawns a
//! `BackgroundIndexerWorker` that indexes concurrently with commits.
//!
//! - `reindex_min_bytes` (MIN): novelty size at which a background index STARTS.
//! - `reindex_max_bytes` (MAX): novelty ceiling that BLOCKS commits
//!   (`TransactError::NoveltyAtMax`) until the index drains novelty below it.
//!
//! Two canonical configs:
//!   - **keep-up** (MIN tiny, MAX comfortable): index after ~every commit unless
//!     busy; measures whether the indexer keeps pace and how many index cycles
//!     run (more cycles = fresher index = more reads hit index, not novelty).
//!   - **wall** (MIN ≈ MAX): accumulate to the ceiling, block until the index
//!     drains, continue — effectively synchronous via backpressure.
//!
//! Runs on a MULTI-THREAD runtime so the indexer truly overlaps commits.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `IDX_COMMITS` | `4000` | commits to drive |
//! | `IDX_NODES_PER_COMMIT` | `10` | nodes/commit (~8 flakes each) |
//! | `IDX_MIN_BYTES` | `100` | reindex_min_bytes (start trigger) |
//! | `IDX_MAX_BYTES` | `4000000` | reindex_max_bytes (backpressure wall) |
//! | `IDX_DB_DIR` | `/dev/shm/idx-throughput` | file storage dir |
//! | `IDX_CSV` | `target/index-throughput.csv` | per-commit CSV |
//! | `IDX_DRAIN_WAIT_S` | `120` | max wait for the final index to drain after commits |
//!
//! ## Run
//! ```bash
//! IDX_MIN_BYTES=100 IDX_MAX_BYTES=4000000 \
//!   cargo run --release --example index_throughput -p fluree-db-api --features native
//! ```

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fluree_db_api::FlureeBuilder;
use serde_json::Value as JsonValue;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_jsonld};

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---- tracing capture of index START / FINISH (the orchestrator logs these) ----

#[derive(Default)]
struct MsgVisit {
    msg: String,
    index_t: i64,
}
impl Visit for MsgVisit {
    fn record_i64(&mut self, f: &Field, v: i64) {
        if f.name() == "index_t" {
            self.index_t = v;
        }
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        if f.name() == "index_t" {
            self.index_t = v as i64;
        }
    }
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        if f.name() == "message" {
            self.msg = format!("{v:?}");
        }
    }
}

#[derive(Clone, Copy)]
enum IdxEvent {
    Start(u128),
    Finish(u128, i64),
}

struct IdxLayer {
    start: Instant,
    ev: Arc<Mutex<Vec<IdxEvent>>>,
}
impl<S: Subscriber> Layer<S> for IdxLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut v = MsgVisit::default();
        event.record(&mut v);
        if v.msg.is_empty() {
            return;
        }
        let at = self.start.elapsed().as_millis();
        if v.msg.contains("Starting queued indexing work") {
            self.ev.lock().unwrap().push(IdxEvent::Start(at));
        } else if v.msg.contains("Successfully indexed ledger") {
            self.ev
                .lock()
                .unwrap()
                .push(IdxEvent::Finish(at, v.index_t));
        }
    }
}

struct Row {
    idx: usize,
    t: i64,
    index_t: i64,
    novelty_bytes: usize,
    commit_us: u128,
    wall_ms: u128,
    stalled_ms: u128,
}

fn is_at_max(e: &impl std::fmt::Debug) -> bool {
    format!("{e:?}").contains("NoveltyAtMax")
}

async fn run(ev: Arc<Mutex<Vec<IdxEvent>>>, t0: Instant) {
    let commits = env_usize("IDX_COMMITS", 4000);
    let npc = env_usize("IDX_NODES_PER_COMMIT", 10);
    let min_bytes = env_usize("IDX_MIN_BYTES", 100);
    let max_bytes = env_usize("IDX_MAX_BYTES", 4_000_000);
    let dir = env_str("IDX_DB_DIR", "/dev/shm/idx-throughput");
    let csv = env_str("IDX_CSV", "target/index-throughput.csv");
    let drain_wait_s = env_usize("IDX_DRAIN_WAIT_S", 120);
    let ledger_id = env_str("IDX_LEDGER", "idx/bench:main");

    let _ = std::fs::remove_dir_all(&dir);
    let fluree = FlureeBuilder::file(dir)
        .with_indexing_thresholds(min_bytes, max_bytes)
        .build()
        .expect("build (file + indexing)");

    eprintln!(
        "index_throughput: {commits} commits x {npc} nodes | MIN={min_bytes}B MAX={max_bytes}B"
    );

    let mut ledger = fluree
        .create_ledger(&ledger_id)
        .await
        .expect("create_ledger");
    let mut rows: Vec<Row> = Vec::with_capacity(commits);
    let mut last_index_t = ledger.index_t();
    let mut index_advances = 0usize; // index_t transitions seen from the commit loop
    let mut stalled_commits = 0usize;
    let mut total_stall = Duration::ZERO;
    let mut max_stall = Duration::ZERO;
    let mut commit_only_us: Vec<u128> = Vec::with_capacity(commits);
    let report_every = (commits / 20).max(1);

    let loop_start = Instant::now();
    for i in 0..commits {
        let json: JsonValue = txn_data_to_jsonld(&generate_txn_data(i, npc));

        // Each commit builds on the CURRENT published state (reload), so the
        // committer reflects background-index publications: novelty clears,
        // index_t advances, and backpressure (NoveltyAtMax) is evaluated against
        // the real shared novelty — modelling a server handling successive txns.
        let mut stall = Duration::ZERO;
        let next = loop {
            let base = fluree.ledger(&ledger_id).await.expect("reload base ledger");
            let c0 = Instant::now();
            match fluree.insert(base, &json).await {
                Ok(res) => {
                    commit_only_us.push(c0.elapsed().as_micros());
                    break res.ledger;
                }
                Err(e) if is_at_max(&e) => {
                    let s = Duration::from_micros(500);
                    tokio::time::sleep(s).await;
                    stall += s;
                    assert!(
                        stall <= Duration::from_secs(drain_wait_s as u64),
                        "commit {i} stalled >{drain_wait_s}s at MAX novelty — indexer not draining"
                    );
                }
                Err(e) => panic!("insert commit {i} failed: {e:?}"),
            }
        };
        ledger = next;
        if !stall.is_zero() {
            stalled_commits += 1;
            total_stall += stall;
            max_stall = max_stall.max(stall);
        }

        let it = ledger.index_t();
        if it > last_index_t {
            index_advances += 1;
            last_index_t = it;
        }
        rows.push(Row {
            idx: i,
            t: ledger.t(),
            index_t: it,
            novelty_bytes: ledger.novelty_size(),
            commit_us: commit_only_us.last().copied().unwrap_or(0),
            wall_ms: t0.elapsed().as_millis(),
            stalled_ms: stall.as_millis(),
        });
        if i % report_every == 0 {
            eprintln!(
                "[idx] commit {i}/{commits}  t={} index_t={} novelty={:.2} MiB  index_cycles={}  stalled={}",
                ledger.t(),
                it,
                ledger.novelty_size() as f64 / (1024.0 * 1024.0),
                index_advances,
                stalled_commits,
            );
        }
    }
    let commit_wall = loop_start.elapsed();

    // Wait for the final index to catch up (index_t -> t), polling a fresh view.
    let drain0 = Instant::now();
    let final_t = ledger.t();
    let mut final_index_t = ledger.index_t();
    while final_index_t < final_t && drain0.elapsed() < Duration::from_secs(drain_wait_s as u64) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let l = fluree.ledger(&ledger_id).await.expect("reload");
        final_index_t = l.index_t();
    }
    let drain_wall = drain0.elapsed();

    write_csv(&csv, &rows);
    summarize(
        commits,
        commit_wall,
        drain_wall,
        &commit_only_us,
        stalled_commits,
        total_stall,
        max_stall,
        index_advances,
        final_t,
        final_index_t,
        &rows,
        &ev.lock().unwrap(),
    );
}

#[allow(clippy::too_many_arguments)]
fn summarize(
    commits: usize,
    commit_wall: Duration,
    drain_wall: Duration,
    commit_only_us: &[u128],
    stalled_commits: usize,
    total_stall: Duration,
    max_stall: Duration,
    index_advances: usize,
    final_t: i64,
    final_index_t: i64,
    rows: &[Row],
    events: &[IdxEvent],
) {
    let mut sorted: Vec<u128> = commit_only_us.to_vec();
    sorted.sort_unstable();
    let pct = |p: f64| -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
    };
    let mean_commit = if sorted.is_empty() {
        0
    } else {
        sorted.iter().sum::<u128>() / sorted.len() as u128
    };

    // pair Start->Finish for per-index durations (serial per ledger)
    let mut durs: Vec<u128> = Vec::new();
    let mut pending_start: Option<u128> = None;
    let mut last_finish_ms = 0u128;
    let mut last_indexed_t = -1i64;
    for e in events {
        match *e {
            IdxEvent::Start(at) => pending_start = Some(at),
            IdxEvent::Finish(at, idx_t) => {
                if let Some(s) = pending_start.take() {
                    durs.push(at.saturating_sub(s));
                }
                last_finish_ms = at;
                last_indexed_t = last_indexed_t.max(idx_t);
            }
        }
    }
    let n_idx = durs.len();
    let total_idx_ms: u128 = durs.iter().sum();
    let mean_idx = if n_idx > 0 {
        total_idx_ms / n_idx as u128
    } else {
        0
    };
    let max_idx = durs.iter().copied().max().unwrap_or(0);
    let min_idx = durs.iter().copied().min().unwrap_or(0);

    let nov: Vec<usize> = rows.iter().map(|r| r.novelty_bytes).collect();
    let nov_max = nov.iter().copied().max().unwrap_or(0);
    let nov_mean = if nov.is_empty() {
        0
    } else {
        nov.iter().sum::<usize>() / nov.len()
    };
    let final_nov = rows.last().map(|r| r.novelty_bytes).unwrap_or(0);

    let total_wall = commit_wall + drain_wall;
    let cps_commit = commits as f64 / commit_wall.as_secs_f64();
    let cps_total = commits as f64 / total_wall.as_secs_f64();
    let mib = |b: usize| b as f64 / (1024.0 * 1024.0);

    println!("\n================ index-throughput summary ================");
    println!("commits                : {commits}");
    println!(
        "commit-loop wall       : {:.2} s",
        commit_wall.as_secs_f64()
    );
    println!(
        "final-drain wall       : {:.2} s (wait index_t -> t after commits)",
        drain_wall.as_secs_f64()
    );
    println!("THROUGHPUT (commit loop): {cps_commit:.0} commits/s");
    println!("THROUGHPUT (incl drain) : {cps_total:.0} commits/s");
    println!(
        "commit latency us      : mean {mean_commit}  p50 {}  p95 {}  p99 {}",
        pct(0.50),
        pct(0.95),
        pct(0.99)
    );
    println!("--- backpressure (MAX wall) ---");
    println!(
        "stalled commits        : {stalled_commits}  ({:.1}%)",
        100.0 * stalled_commits as f64 / commits as f64
    );
    println!(
        "total stall            : {:.2} s   max single stall {:.0} ms",
        total_stall.as_secs_f64(),
        max_stall.as_millis()
    );
    println!("--- background indexing ---");
    println!(
        "index cycles (logs)    : {n_idx}    (index_t advances seen by commits: {index_advances})"
    );
    println!("per-index ms           : mean {mean_idx}  min {min_idx}  max {max_idx}");
    println!(
        "total index time       : {:.2} s   ({:.0}% of commit-loop wall)",
        total_idx_ms as f64 / 1000.0,
        100.0 * total_idx_ms as f64 / commit_wall.as_millis() as f64
    );
    println!(
        "last index finished at : {:.2} s into run  (reached index_t={last_indexed_t})",
        last_finish_ms as f64 / 1000.0
    );
    println!("--- novelty (clearing / lag) ---");
    println!(
        "novelty MiB            : mean {:.2}  max {:.2}  final {:.2}",
        mib(nov_mean),
        mib(nov_max),
        mib(final_nov)
    );
    println!(
        "final t / index_t      : {final_t} / {final_index_t}   (lag {} commits)",
        final_t - final_index_t
    );
    let keeping_up = stalled_commits == 0;
    println!(
        "verdict                : indexer {} (more cycles + low novelty = fresher index for reads)",
        if keeping_up {
            "KEPT UP (no backpressure)"
        } else {
            "FELL BEHIND (hit MAX wall)"
        }
    );
    println!("=========================================================");
    println!("(slope of per-commit cost is flat by design; this measures NET throughput with real background indexing)");
}

fn write_csv(path: &str, rows: &[Row]) {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(rows.len() * 56 + 64);
    out.push_str("commit_idx,t,index_t,novelty_bytes,commit_us,wall_ms,stalled_ms\n");
    for r in rows {
        let _ = writeln!(
            out,
            "{},{},{},{},{},{},{}",
            r.idx, r.t, r.index_t, r.novelty_bytes, r.commit_us, r.wall_ms, r.stalled_ms
        );
    }
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(path, out) {
        eprintln!("[csv] write {path} failed: {e}");
    } else {
        eprintln!("[csv] wrote {} rows -> {path}", rows.len());
    }
}

fn main() {
    let t0 = Instant::now();
    let ev = Arc::new(Mutex::new(Vec::<IdxEvent>::new()));
    tracing_subscriber::registry()
        .with(IdxLayer {
            start: t0,
            ev: ev.clone(),
        })
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("multi-thread runtime");
    rt.block_on(run(ev, t0));
}
