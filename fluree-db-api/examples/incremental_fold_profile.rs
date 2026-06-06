//! Isolated incremental index-fold profiler & harness.
//!
//! Reproduces the "backlog fold" scenario we want to speed up: an existing
//! base index with ~N commits of novelty sitting above it, all folded into the
//! index by ONE incremental indexing run. It deliberately separates the
//! expensive *populate* step from the measured *fold* so a flamegraph of
//! `FOLD_MODE=fold` captures essentially only the incremental fold — not the
//! 1000-commit populate that precedes it.
//!
//! The fold is driven through [`fluree_db_indexer::build_index_for_ledger`]
//! with `IndexerConfig::default()`, which is exactly what `fluree index` (the
//! CLI) does. That config sets `incremental_max_commits = 10_000` and leaves
//! `incremental_max_commit_bytes = None` (NO byte budget), so the fold stays on
//! the incremental path for any chain up to 10_000 commits — it never silently
//! falls back to a full rebuild the way the orchestrator's `trigger_index` can
//! when the commit-chain bytes exceed `reindex_max_bytes`. It also does NOT
//! publish the resulting index, so `fold` is **repeatable** against the same
//! prepared directory (ideal for `cargo flamegraph` and repeated timing).
//!
//! ## Modes — `FOLD_MODE` (default `all`)
//! - `prepare` — build a persistent ledger at `$FOLD_DB_DIR`: base index plus
//!   `FOLD_DELTA_COMMITS` commits of novelty above it. Exits. Run once.
//! - `fold` — open `$FOLD_DB_DIR` and run ONE incremental fold, timed. Does not
//!   publish, so re-running folds the same backlog again.
//! - `all` — prepare into a tempdir, then fold once. Quick local sanity check.
//!
//! ## Config (env vars)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `FOLD_MODE` | `all` | `prepare` \| `fold` \| `all` |
//! | `FOLD_DB_DIR` | (required for prepare/fold) | persistent storage dir |
//! | `FOLD_LEDGER` | `fold-bench` | ledger alias (normalized to `…:main`) |
//! | `FOLD_BASE_NODES` | `50000` | base population size (one commit) |
//! | `FOLD_DELTA_COMMITS` | `1000` | commits of novelty above the index |
//! | `FOLD_NODES_PER_COMMIT` | `10` | nodes per delta commit |
//! | `FOLD_REPEAT` | `1` | fold timings to take (warm after the 1st) |
//!
//! Set `FLUREE_BENCH_TRACING=1 RUST_LOG=fluree_db_indexer=info` to see the
//! `attempting incremental index` / `starting full rebuild path` log lines and
//! confirm the run stayed on the incremental path.
//!
//! ## Run
//! ```bash
//! # quick local sanity check (tempdir, base=50k, delta=1000)
//! cargo run --release --example incremental_fold_profile -p fluree-db-api
//!
//! # isolated flamegraph: prepare once to disk, then flamegraph only the fold
//! FOLD_DB_DIR=/mnt/ebs/folddb FOLD_MODE=prepare \
//!   cargo run --release --example incremental_fold_profile -p fluree-db-api
//! FOLD_DB_DIR=/mnt/ebs/folddb FOLD_MODE=fold \
//!   cargo flamegraph --example incremental_fold_profile -p fluree-db-api
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_bench_support::bench_runtime;
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_turtle};
use fluree_bench_support::init_tracing_for_bench;
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, LedgerState, TxnOpts};
use fluree_db_core::ledger_id::normalize_ledger_id;
use fluree_db_core::ContentStore;
use fluree_db_indexer::{build_index_for_ledger, IndexerConfig};

/// Disable auto/background indexing while populating so every delta commit
/// piles up as novelty above the baseline index (the backlog we then fold).
const DISABLE_AUTOINDEX_BYTES: usize = 50_000_000_000;

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn insert_commit(fluree: &Fluree, ledger: LedgerState, turtle: &str) -> LedgerState {
    let index_config = IndexConfig {
        reindex_min_bytes: DISABLE_AUTOINDEX_BYTES,
        reindex_max_bytes: DISABLE_AUTOINDEX_BYTES,
    };
    fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("insert commit")
        .ledger
}

async fn prepare(fluree: &Fluree, ledger_id: &str, base_nodes: usize, delta: usize, npc: usize) {
    if fluree.ledger_exists(ledger_id).await.unwrap_or(false) {
        eprintln!(
            "[prepare] ledger '{ledger_id}' already exists here. Use a clean FOLD_DB_DIR \
             (or delete the existing one) so the backlog is built from scratch."
        );
        std::process::exit(1);
    }

    let mut ledger = fluree.create_ledger(ledger_id).await.expect("create_ledger");

    eprintln!("[prepare] base population: {base_nodes} nodes in one commit ...");
    let base = txn_data_to_turtle(&generate_txn_data(0, base_nodes));
    ledger = insert_commit(fluree, ledger, &base).await;

    eprintln!("[prepare] baseline full reindex (establishes the index we fold onto) ...");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("baseline reindex");
    drop(ledger);

    let mut ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload after baseline reindex");
    let base_index_t = ledger.index_t();

    eprintln!("[prepare] appending {delta} delta commits ({npc} nodes each) above the index ...");
    for i in 0..delta {
        // Offset by 1M so delta IDs never collide with the base population.
        let turtle = txn_data_to_turtle(&generate_txn_data(1_000_000 + i, npc));
        ledger = insert_commit(fluree, ledger, &turtle).await;
    }

    let commit_t = ledger.t();
    let index_t = ledger.index_t();
    eprintln!(
        "[prepare] DONE: index_t={index_t} commit_t={commit_t} commit_gap={} \
         (baseline_index_t={base_index_t}). Backlog of {} commits is ready to fold.",
        commit_t - index_t,
        commit_t - index_t,
    );
}

async fn fold_once(fluree: &Fluree, ledger_id: &str) -> (Duration, i64) {
    let cs: Arc<dyn ContentStore> = fluree
        .branched_content_store(ledger_id)
        .await
        .expect("branched content store");
    let mut config = IndexerConfig::default();
    // FOLD_FETCH_CONCURRENCY overrides the bounded commit-body fetch width on the
    // commit-index fast path (default 4). Higher k surfaces the parallel-fetch win
    // on concurrency-scalable backends (S3, gp3); it can't help an IOPS-capped HDD.
    if let Some(k) = std::env::var("FOLD_FETCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        config.incremental_max_concurrency = k;
    }
    // FOLD_FORCE_SERIAL_WALK=1 measures the BASELINE: skip the commit-CID index
    // fast path and discover the chain via the serial DAG walk on the same
    // prepared backlog. Leave unset (or =0) for the FIX (index + parallel fetch).
    config.force_serial_commit_walk = std::env::var("FOLD_FORCE_SERIAL_WALK")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let t0 = Instant::now();
    let result = build_index_for_ledger(cs, fluree.nameservice(), ledger_id, config)
        .await
        .expect("incremental fold");
    (t0.elapsed(), result.index_t)
}

async fn report_backlog(fluree: &Fluree, ledger_id: &str) {
    if let Ok(Some(record)) = fluree.nameservice().lookup(ledger_id).await {
        let gap = record.commit_t - record.index_t;
        eprintln!(
            "[fold] backlog: index_t={} commit_t={} commit_gap={gap} \
             (incremental while gap <= 10000)",
            record.index_t, record.commit_t,
        );
        if record.index_head_id.is_none() || record.index_t == 0 {
            eprintln!(
                "[fold] WARNING: no baseline index present — this will be a full rebuild, \
                 not an incremental fold. Run FOLD_MODE=prepare first."
            );
        }
    }
}

fn summarize(durations: &[Duration]) {
    if durations.is_empty() {
        return;
    }
    let mut sorted: Vec<u128> = durations.iter().map(Duration::as_micros).collect();
    sorted.sort_unstable();
    let n = sorted.len();
    let median = sorted[n / 2];
    let min = sorted[0];
    let max = sorted[n - 1];
    let mean: u128 = sorted.iter().sum::<u128>() / n as u128;
    eprintln!(
        "[fold] {n} run(s) — min={:.1}ms median={:.1}ms mean={:.1}ms max={:.1}ms \
         (run #1 is cold; later runs warm caches)",
        min as f64 / 1000.0,
        median as f64 / 1000.0,
        mean as f64 / 1000.0,
        max as f64 / 1000.0,
    );
}

/// Build the Fluree backend the fold runs against.
///
/// Default (no `FOLD_BACKEND`, or `=file`): local FileStorage at `dir`.
/// `FOLD_BACKEND=s3dynamo` (requires the `aws` feature): S3 object storage +
/// DynamoDB nameservice, configured from `FOLD_S3_BUCKET` / `FOLD_S3_PREFIX` /
/// `FOLD_S3_ENDPOINT` / `FOLD_DYNAMO_TABLE` / `AWS_REGION`. The DynamoDB table
/// is created if absent. `dir` is ignored on the S3 path.
async fn build_backend(dir: &str) -> Fluree {
    let backend = env_str("FOLD_BACKEND", "file");
    match backend.as_str() {
        "file" => FlureeBuilder::file(dir).build().expect("build file Fluree"),
        "s3dynamo" => build_s3dynamo().await,
        other => {
            eprintln!("[error] unknown FOLD_BACKEND='{other}' (want file|s3dynamo)");
            std::process::exit(2);
        }
    }
}

#[cfg(feature = "aws")]
async fn build_s3dynamo() -> Fluree {
    let bucket = env_str("FOLD_S3_BUCKET", "");
    if bucket.is_empty() {
        eprintln!("[error] FOLD_BACKEND=s3dynamo requires FOLD_S3_BUCKET");
        std::process::exit(2);
    }
    let table = env_str("FOLD_DYNAMO_TABLE", "");
    if table.is_empty() {
        eprintln!("[error] FOLD_BACKEND=s3dynamo requires FOLD_DYNAMO_TABLE");
        std::process::exit(2);
    }
    let region = env_str("AWS_REGION", "us-east-1");
    let endpoint = std::env::var("FOLD_S3_ENDPOINT").ok();
    let mut builder = FlureeBuilder::s3(bucket, endpoint.clone().unwrap_or_default());
    if let Ok(prefix) = std::env::var("FOLD_S3_PREFIX") {
        if !prefix.is_empty() {
            builder = builder.s3_prefix(prefix);
        }
    }
    eprintln!("[config] backend=s3dynamo table={table} region={region}");
    builder
        .build_s3_dynamo(table, Some(region), endpoint)
        .await
        .expect("build S3 + DynamoDB Fluree")
}

#[cfg(not(feature = "aws"))]
async fn build_s3dynamo() -> Fluree {
    eprintln!("[error] FOLD_BACKEND=s3dynamo requires building with --features aws");
    std::process::exit(2);
}

fn main() {
    init_tracing_for_bench();
    let rt = bench_runtime();

    let mode = env_str("FOLD_MODE", "all");
    let alias = env_str("FOLD_LEDGER", "fold-bench");
    let ledger_id = normalize_ledger_id(&alias).unwrap_or(alias);
    let base_nodes = env_usize("FOLD_BASE_NODES", 50_000);
    let delta = env_usize("FOLD_DELTA_COMMITS", 1_000);
    let npc = env_usize("FOLD_NODES_PER_COMMIT", 10);
    let repeat = env_usize("FOLD_REPEAT", 1).max(1);

    eprintln!(
        "[config] mode={mode} ledger={ledger_id} base_nodes={base_nodes} \
         delta_commits={delta} nodes_per_commit={npc} repeat={repeat}"
    );

    // FileStorage needs FOLD_DB_DIR for prepare/fold; the s3dynamo backend
    // ignores it (state lives in S3 + DynamoDB).
    let is_file_backend = env_str("FOLD_BACKEND", "file") == "file";

    rt.block_on(async {
        match mode.as_str() {
            "prepare" => {
                let dir = env_str("FOLD_DB_DIR", "");
                if is_file_backend && dir.is_empty() {
                    eprintln!("[prepare] FOLD_DB_DIR is required for prepare mode (file backend).");
                    std::process::exit(2);
                }
                let fluree = build_backend(&dir).await;
                prepare(&fluree, &ledger_id, base_nodes, delta, npc).await;
            }
            "fold" => {
                let dir = env_str("FOLD_DB_DIR", "");
                if is_file_backend && dir.is_empty() {
                    eprintln!("[fold] FOLD_DB_DIR is required for fold mode (file backend).");
                    std::process::exit(2);
                }
                let fluree = build_backend(&dir).await;
                report_backlog(&fluree, &ledger_id).await;
                let mut durations = Vec::with_capacity(repeat);
                for i in 0..repeat {
                    let (elapsed, index_t) = fold_once(&fluree, &ledger_id).await;
                    eprintln!(
                        "[fold] run {}/{repeat}: {:.1}ms -> index_t={index_t}",
                        i + 1,
                        elapsed.as_micros() as f64 / 1000.0,
                    );
                    durations.push(elapsed);
                }
                summarize(&durations);
            }
            "all" => {
                let tmp = tempfile::tempdir().expect("tempdir");
                let fluree = build_backend(&tmp.path().to_string_lossy()).await;
                prepare(&fluree, &ledger_id, base_nodes, delta, npc).await;
                report_backlog(&fluree, &ledger_id).await;
                let mut durations = Vec::with_capacity(repeat);
                for i in 0..repeat {
                    let (elapsed, index_t) = fold_once(&fluree, &ledger_id).await;
                    eprintln!(
                        "[fold] run {}/{repeat}: {:.1}ms -> index_t={index_t}",
                        i + 1,
                        elapsed.as_micros() as f64 / 1000.0,
                    );
                    durations.push(elapsed);
                }
                summarize(&durations);
            }
            other => {
                eprintln!("[error] unknown FOLD_MODE='{other}' (want prepare|fold|all)");
                std::process::exit(2);
            }
        }
    });
}
