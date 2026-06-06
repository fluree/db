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
//! - `import` — bulk-import a real RDF file (`$FOLD_IMPORT_PATH`) into
//!   `$FOLD_LEDGER`, building the base index. Run once before `scatter-prepare`.
//!   Targets the s3dynamo backend (DBLP many-leaves test) but also works on file.
//! - `scatter-prepare` — on an ALREADY-IMPORTED ledger, sample existing subject
//!   IRIs spread across the subject-id keyspace, then apply `$FOLD_DELTA_COMMITS`
//!   update commits that each stamp a marker triple on a partition of those
//!   subjects. The scattered novelty forces MANY SPOT leaves to be rewritten on
//!   the subsequent `FOLD_MODE=fold`. Auto-indexing stays disabled so the
//!   backlog piles above the base index.
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
//! ### `import` mode
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `FOLD_IMPORT_PATH` | (required) | `.nt.gz` / `.ttl` / … file to import |
//! | `FOLD_IMPORT_PARALLELISM` | (builder default) | parse threads |
//! | `FOLD_IMPORT_MEM_MB` | (builder default) | memory budget MB |
//! | `FOLD_IMPORT_CHUNK_MB` | (builder default) | chunk size MB |
//! | `FOLD_IMPORT_LEAFLET_ROWS` | (builder default) | rows per leaflet |
//!
//! ### `scatter-prepare` mode
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `FOLD_SAMPLE_LIMIT` | `5000000` | LIMIT on the subject-scan query |
//! | `FOLD_SCATTER_TOTAL` | `20000` | target distinct subjects to touch |
//! | `FOLD_DELTA_COMMITS` | `1000` | update commits to spread them across |
//! | `FOLD_SCATTER_PER_COMMIT` | total/commits | subjects per commit |
//! | `FOLD_MARKER_PRED` | `http://example.org/fold#touched` | marker predicate IRI |
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
//!
//! # DBLP many-leaves test on S3 + DynamoDB (built with --features aws):
//! #   1) import the base dataset (builds the base index in S3)
//! FOLD_BACKEND=s3dynamo FOLD_LEDGER=dblp \
//!   FOLD_S3_BUCKET=my-bucket FOLD_S3_PREFIX=fold FOLD_DYNAMO_TABLE=fold-ns AWS_REGION=us-east-1 \
//!   FOLD_MODE=import FOLD_IMPORT_PATH=/data/dblp.nt.gz FOLD_IMPORT_PARALLELISM=9 \
//!   cargo run --release --example incremental_fold_profile -p fluree-db-api --features aws
//! #   2) scatter 1000 update commits across existing subjects (the fold backlog)
//! FOLD_BACKEND=s3dynamo FOLD_LEDGER=dblp \
//!   FOLD_S3_BUCKET=my-bucket FOLD_S3_PREFIX=fold FOLD_DYNAMO_TABLE=fold-ns AWS_REGION=us-east-1 \
//!   FOLD_MODE=scatter-prepare FOLD_SCATTER_TOTAL=20000 FOLD_DELTA_COMMITS=1000 \
//!   cargo run --release --example incremental_fold_profile -p fluree-db-api --features aws
//! #   3) fold the scattered backlog (times the many-leaf rewrite)
//! FOLD_BACKEND=s3dynamo FOLD_LEDGER=dblp \
//!   FOLD_S3_BUCKET=my-bucket FOLD_S3_PREFIX=fold FOLD_DYNAMO_TABLE=fold-ns AWS_REGION=us-east-1 \
//!   FOLD_MODE=fold FOLD_FETCH_CONCURRENCY=16 \
//!   cargo run --release --example incremental_fold_profile -p fluree-db-api --features aws
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

/// Parse an optional usize env var. `None` when unset/empty so the caller can
/// leave a builder default in place rather than overriding it.
fn env_opt_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse().ok())
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

    let mut ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create_ledger");

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

/// `FOLD_MODE=import`: bulk-import a real RDF file, building the base index.
///
/// Mirrors the CLI's `run_bulk_import` (`fluree-db-cli/src/commands/create.rs`):
/// `fluree.create(ledger).import(path)`, optional tuning setters, terminal
/// `.execute()`. Tuning is applied only when its env var is set.
async fn run_import(fluree: &Fluree, ledger_id: &str) {
    if fluree.ledger_exists(ledger_id).await.unwrap_or(false) {
        eprintln!(
            "[import] ledger '{ledger_id}' already exists. Use a fresh FOLD_LEDGER / prefix \
             (or drop the existing ledger) so the base index is built from scratch."
        );
        std::process::exit(1);
    }

    let path = env_str("FOLD_IMPORT_PATH", "");
    if path.is_empty() {
        eprintln!("[import] FOLD_IMPORT_PATH is required (the .nt.gz / .ttl file to import).");
        std::process::exit(2);
    }

    let mut builder = fluree.create(ledger_id).import(&path);
    if let Some(p) = env_opt_usize("FOLD_IMPORT_PARALLELISM") {
        builder = builder.parallelism(p);
    }
    if let Some(m) = env_opt_usize("FOLD_IMPORT_MEM_MB") {
        builder = builder.memory_budget_mb(m);
    }
    if let Some(c) = env_opt_usize("FOLD_IMPORT_CHUNK_MB") {
        builder = builder.chunk_size_mb(c);
    }
    if let Some(r) = env_opt_usize("FOLD_IMPORT_LEAFLET_ROWS") {
        builder = builder.leaflet_rows(r);
    }

    let settings = builder.effective_import_settings();
    eprintln!(
        "[import] importing {path} -> ledger '{ledger_id}' \
         (memory={} MB, parallelism={}, chunk={} MB)",
        settings.memory_budget_mb, settings.parallelism, settings.chunk_size_mb,
    );

    let t0 = Instant::now();
    let result = builder.execute().await.expect("bulk import");
    let secs = t0.elapsed().as_secs_f64();
    eprintln!(
        "[import] DONE in {secs:.1}s: {:.1}M flakes, commit_t={} index_t={} \
         (base index built; ledger ready for scatter-prepare).",
        result.flake_count as f64 / 1_000_000.0,
        result.t,
        result.index_t,
    );
}

/// `FOLD_MODE=scatter-prepare`: apply scattered update commits to EXISTING
/// subjects spread across the subject-id keyspace.
///
/// Step 1 samples subject IRIs in subject-id order (an unordered triple scan)
/// and strides them for spread; step 2 partitions them across
/// `FOLD_DELTA_COMMITS` update commits that each stamp a marker triple, with
/// auto-indexing disabled so the novelty piles above the base index.
async fn scatter_prepare(fluree: &Fluree, ledger_id: &str, delta_commits: usize) {
    if !fluree.ledger_exists(ledger_id).await.unwrap_or(false) {
        eprintln!(
            "[scatter] ledger '{ledger_id}' does not exist. Run FOLD_MODE=import first to \
             build the base index, then scatter-prepare."
        );
        std::process::exit(1);
    }
    if delta_commits == 0 {
        eprintln!("[scatter] FOLD_DELTA_COMMITS must be > 0");
        std::process::exit(2);
    }

    let sample_limit = env_usize("FOLD_SAMPLE_LIMIT", 5_000_000);
    let scatter_total = env_usize("FOLD_SCATTER_TOTAL", 20_000);
    let per_commit = env_opt_usize("FOLD_SCATTER_PER_COMMIT")
        .unwrap_or_else(|| scatter_total.div_ceil(delta_commits))
        .max(1);
    let marker_pred = env_str("FOLD_MARKER_PRED", "http://example.org/fold#touched");

    let subjects = sample_subjects(fluree, ledger_id, sample_limit, scatter_total).await;
    if subjects.is_empty() {
        eprintln!("[scatter] sampled 0 subjects — is the ledger populated?");
        std::process::exit(1);
    }
    eprintln!(
        "[scatter] sampled {} distinct subjects (limit={sample_limit}, target≈{scatter_total}); \
         min={} max={}",
        subjects.len(),
        subjects.first().map(String::as_str).unwrap_or(""),
        subjects.last().map(String::as_str).unwrap_or(""),
    );

    let mut ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
    let mut touched = 0usize;
    let mut commits = 0usize;
    for chunk in subjects.chunks(per_commit) {
        if commits >= delta_commits {
            break;
        }
        let mut turtle = String::with_capacity(chunk.len() * 96);
        for s in chunk {
            // Full <iri> form is parser-safe for an existing subject; the marker
            // object carries the running sequence so re-runs assert fresh values.
            turtle.push('<');
            turtle.push_str(s);
            turtle.push_str("> <");
            turtle.push_str(&marker_pred);
            turtle.push_str("> \"");
            turtle.push_str(&touched.to_string());
            turtle.push_str("\" .\n");
            touched += 1;
        }
        ledger = insert_commit(fluree, ledger, &turtle).await;
        commits += 1;
    }

    let commit_t = ledger.t();
    let index_t = ledger.index_t();
    eprintln!(
        "[scatter] DONE: {commits} update commits touched {touched} subjects; \
         index_t={index_t} commit_t={commit_t} commit_gap={}. Backlog ready to fold.",
        commit_t - index_t,
    );
}

/// Scan subjects in subject-id order via an unordered triple pattern, LIMIT
/// `sample_limit`, then stride the result down to ~`target` distinct subjects
/// spread across the scanned range.
///
/// Uses the SPARQL JSON path (`execute_formatted`): default format for a SPARQL
/// query is W3C SPARQL JSON, so subject IRIs arrive as full `uri` values under
/// `results.bindings[].s.value`.
async fn sample_subjects(
    fluree: &Fluree,
    ledger_id: &str,
    sample_limit: usize,
    target: usize,
) -> Vec<String> {
    let query = format!("SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {sample_limit}");
    let formatted = fluree
        .graph(ledger_id)
        .query()
        .sparql(&query)
        .execute_formatted()
        .await
        .expect("subject-scan query");

    let bindings = formatted
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .cloned()
        .unwrap_or_default();

    // Stride so we keep ~`target` distinct subjects spread across the scan.
    let stride = (bindings.len() / target.max(1)).max(1);
    let mut out = Vec::with_capacity(target);
    let mut last: Option<String> = None;
    for (i, row) in bindings.iter().enumerate() {
        if !i.is_multiple_of(stride) {
            continue;
        }
        if let Some(iri) = row
            .get("s")
            .and_then(|s| s.get("value"))
            .and_then(|v| v.as_str())
        {
            // Adjacent same-subject rows are common in a triple scan; skip dups.
            if last.as_deref() != Some(iri) {
                out.push(iri.to_string());
                last = Some(iri.to_string());
            }
        }
    }
    out
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
    // FOLD_LEAF_UPLOAD_CONCURRENCY overrides the global Phase 2 leaf/sidecar
    // upload budget (default 16) shared across all order-tasks. Higher k
    // surfaces the parallel-upload win when leaves skew into one order-task.
    if let Some(k) = std::env::var("FOLD_LEAF_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        config.incremental_leaf_upload_concurrency = k;
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
            "import" => {
                let dir = env_str("FOLD_DB_DIR", "");
                if is_file_backend && dir.is_empty() {
                    eprintln!("[import] FOLD_DB_DIR is required for import mode (file backend).");
                    std::process::exit(2);
                }
                let fluree = build_backend(&dir).await;
                run_import(&fluree, &ledger_id).await;
            }
            "scatter-prepare" => {
                let dir = env_str("FOLD_DB_DIR", "");
                if is_file_backend && dir.is_empty() {
                    eprintln!(
                        "[scatter] FOLD_DB_DIR is required for scatter-prepare mode (file backend)."
                    );
                    std::process::exit(2);
                }
                let fluree = build_backend(&dir).await;
                scatter_prepare(&fluree, &ledger_id, delta).await;
            }
            other => {
                eprintln!(
                    "[error] unknown FOLD_MODE='{other}' \
                     (want prepare|fold|all|import|scatter-prepare)"
                );
                std::process::exit(2);
            }
        }
    });
}
