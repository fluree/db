//! Per-query execution: open a target, run a query with priming + measured
//! reps, capture pathway spans, and fold the result into a [`RunRecord`].
//!
//! ## Runtime & the non-Send R2RML path
//!
//! The R2RML/Iceberg query future is **not** `Send` (it holds Parquet reader
//! state across awaits), so it cannot be `tokio::spawn`ed. We run it with
//! `Runtime::block_on` on the calling thread instead. The runtime is
//! **multi-thread** on purpose: the Iceberg reader fans out per-file Parquet
//! decode with its own `tokio::spawn`, which needs worker threads to run in
//! parallel — a current-thread runtime would serialize decode and distort the
//! measurement.
//!
//! ## Deadlines
//!
//! Each execution gets a fresh [`QueryCancellation`]. A watchdog task
//! cooperatively cancels it at the deadline so the scan *stops* (the R2RML
//! operators poll the handle). A `tokio::time::timeout` at `deadline + grace` is
//! the hard backstop; if it fires, the future is dropped mid-scan — see the
//! caveat in the crate README. Either way the outcome is [`Status::Dnf`] with the
//! wall recorded as the deadline cap.
//!
//! ## Span capture
//!
//! The `BenchSpanCapture` layer is installed **once** at [`Engine::new`] (global
//! subscriber). Reps run strictly sequentially and `take()` drains the sink after
//! each, so each rep's counters are isolated even though the sink is process-global.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::Value;

use fluree_bench_support::tracing::{span_name_filter, BenchSpanCapture};
use fluree_db_api::{
    Fluree, FlureeBuilder, FormatterConfig, LedgerManagerConfig, QueryExecutionOptions,
};
use fluree_db_core::{QueryCancellation, QueryCancellationReason};

use crate::corpus::ExpectedOutcome;
use crate::schema::{Counters, RunRecord, Status};
use crate::targets::Target;
use crate::{canon, spans};

/// Seconds to sleep between executions against a virtual (live-Snowflake) target.
const PACE_SECS: u64 = 2;
/// Grace added to the cooperative deadline before the hard `timeout` backstop.
const GRACE_SECS: u64 = 5;
/// A first measured rep slower than this collapses the run to a single rep.
const ADAPTIVE_STOP: Duration = Duration::from_secs(60);
/// Heads retained under `--keep-heads`.
const HEADS: usize = 20;

/// Reps + deadline for one query on one target.
pub struct RunParams {
    pub timeout: Duration,
    pub reps: usize,
    pub keep_heads: bool,
}

/// The execution engine: one tokio runtime + one global span-capture sink for
/// the whole process.
pub struct Engine {
    rt: tokio::runtime::Runtime,
    capture: BenchSpanCapture,
    worker_threads: usize,
}

/// One execution's raw outcome (pre-record).
struct Outcome {
    wall: Duration,
    status: Status,
    rows: usize,
    hash: String,
    counters: Counters,
    heads: Option<Vec<String>>,
    error: Option<String>,
}

impl Engine {
    /// Build the runtime and install the span-capture subscriber (once).
    pub fn new() -> Result<Self> {
        let capture = install_span_capture();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("building multi-thread tokio runtime")?;
        let worker_threads = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Ok(Self {
            rt,
            capture,
            worker_threads,
        })
    }

    /// Human-readable runtime shape for the run header.
    pub fn runtime_shape(&self) -> String {
        format!("tokio-multi-thread(worker_threads={})", self.worker_threads)
    }

    /// Open a target's on-disk store. Reuse the returned handle across all
    /// queries for that target so the ledger/leaflet caches stay warm.
    ///
    /// `cache_dir`, when set, pins the binary-index / Iceberg on-disk artifact
    /// cache to a known directory (default is `$TMPDIR/fluree_binary_cache`). The
    /// cold protocol sets it so the parent can clear it between subprocess reps.
    pub fn open(&self, target: &Target, cache_dir: Option<&Path>) -> Result<Fluree> {
        let storage = target.storage_dir();
        if !storage.exists() {
            anyhow::bail!(
                "storage dir {} does not exist for target '{}'",
                storage.display(),
                target.id
            );
        }
        // Enter the runtime so builder-side spawns (if any) have a reactor.
        let _guard = self.rt.enter();
        let mut builder = FlureeBuilder::file(storage.to_string_lossy().to_string())
            // The store is already indexed; vbench only reads. No background
            // indexer, but keep the default ledger cache so hot reps are hot.
            .without_indexing();
        if let Some(dir) = cache_dir {
            builder = builder.with_ledger_cache_config(LedgerManagerConfig {
                cache_dir: dir.to_path_buf(),
                ..Default::default()
            });
        }
        builder.build().map_err(|e| {
            anyhow::anyhow!(
                "opening target '{}' at {}: {e}",
                target.id,
                storage.display()
            )
        })
    }

    /// Run one query on one target: priming rep (discarded) + measured reps, then
    /// the median-wall rep's record.
    pub fn run_query(
        &self,
        fluree: &Fluree,
        target: &Target,
        query_id: &str,
        sparql: &str,
        params: &RunParams,
        expected: ExpectedOutcome,
    ) -> RunRecord {
        let is_virtual = target.is_virtual();

        // Priming rep (discarded): warms ledger/leaflet caches (native) and the
        // cross-query catalog cache (virtual).
        if is_virtual {
            self.pace();
        }
        let _ = self.exec_once(fluree, target, sparql, params.timeout, false);

        let mut outcomes: Vec<Outcome> = Vec::with_capacity(params.reps.max(1));
        for i in 0..params.reps.max(1) {
            if is_virtual {
                self.pace();
            }
            let outcome = self.exec_once(fluree, target, sparql, params.timeout, params.keep_heads);
            let first_over_budget = i == 0 && outcome.wall > ADAPTIVE_STOP;
            let dnf = outcome.status == Status::Dnf;
            outcomes.push(outcome);
            // Adaptive: a first measured rep over the budget (or any dnf) means
            // one rep is enough — don't burn time (or Snowflake quota) repeating.
            if first_over_budget || dnf {
                break;
            }
        }

        self.build_record(query_id, target, is_virtual, "hot", outcomes, expected)
    }

    /// Single execution (no priming) — the unit behind `exec-one` and the cold
    /// protocol. `cold` only labels the record's `cache_state`; the on-disk cache
    /// clearing happens in the caller (`cmd_exec_one`) *before* `open`, since the
    /// cache is read at open time. Pacing is the parent's job (2 s between cold
    /// children), so a single exec-one does not sleep.
    pub fn exec_one(
        &self,
        fluree: &Fluree,
        target: &Target,
        query_id: &str,
        sparql: &str,
        timeout: Duration,
        keep_heads: bool,
        expected: ExpectedOutcome,
        cold: bool,
    ) -> RunRecord {
        let outcome = self.exec_once(fluree, target, sparql, timeout, keep_heads);
        let cache_state = if cold { "cold" } else { "warm" };
        self.build_record(
            query_id,
            target,
            target.is_virtual(),
            cache_state,
            vec![outcome],
            expected,
        )
    }

    /// A trivial probe execution for `setup --verify`. Returns wall time and the
    /// formatted SPARQL-JSON document, or a hard error.
    pub fn probe(
        &self,
        fluree: &Fluree,
        target: &Target,
        sparql: &str,
        timeout: Duration,
    ) -> Result<(Duration, Value)> {
        let is_virtual = target.is_virtual();
        let alias = target.alias.clone();
        let start = Instant::now();
        let out = self.rt.block_on(async move {
            tokio::time::timeout(timeout, async move {
                // Bind the Graph to a local so the builder can borrow it across
                // the conditional `with_r2rml()` split without the temporary
                // being dropped mid-chain.
                let graph = fluree.graph(&alias);
                let builder = graph
                    .query()
                    .sparql(sparql)
                    .format(FormatterConfig::sparql_json());
                let builder = if is_virtual {
                    builder.with_r2rml()
                } else {
                    builder
                };
                builder.execute_formatted().await
            })
            .await
        });
        let wall = start.elapsed();
        match out {
            Ok(Ok(doc)) => Ok((wall, doc)),
            Ok(Err(e)) => Err(anyhow::anyhow!("query error: {e}")),
            Err(_) => Err(anyhow::anyhow!(
                "probe timed out after {}s",
                timeout.as_secs()
            )),
        }
    }

    fn pace(&self) {
        std::thread::sleep(Duration::from_secs(PACE_SECS));
    }

    fn exec_once(
        &self,
        fluree: &Fluree,
        target: &Target,
        sparql: &str,
        timeout: Duration,
        keep_heads: bool,
    ) -> Outcome {
        // Isolate this rep's capture from any stray spans.
        let _ = self.capture.take();

        let cancel = QueryCancellation::new();
        let exec_opts = QueryExecutionOptions::new().with_cancellation(cancel.clone());
        let grace = Duration::from_secs(GRACE_SECS);

        // Watchdog: cooperatively cancel at the deadline so the scan stops.
        let watchdog = {
            let cancel = cancel.clone();
            self.rt.spawn(async move {
                tokio::time::sleep(timeout).await;
                cancel.cancel_with(QueryCancellationReason::Timeout);
            })
        };

        let is_virtual = target.is_virtual();
        let alias = target.alias.clone();
        // A CONSTRUCT/DESCRIBE result is an RDF graph, not a solution table:
        // SPARQL-JSON renders it as empty bindings, so format such queries as
        // JSON-LD (`{"@graph":[...]}`) and let `canon` count/hash the nodes.
        let graph_out = is_graph_query(sparql);
        let fmt = if graph_out {
            FormatterConfig::jsonld()
        } else {
            FormatterConfig::sparql_json()
        };

        let start = Instant::now();
        let result = self.rt.block_on(async move {
            let fut = async move {
                // Bind the Graph to a local (see `probe` for the borrow rationale).
                let graph = fluree.graph(&alias);
                let builder = graph
                    .query()
                    .sparql(sparql)
                    .format(fmt)
                    .execution_options(exec_opts);
                let builder = if is_virtual {
                    builder.with_r2rml()
                } else {
                    builder
                };
                builder.execute_formatted().await
            };
            tokio::time::timeout(timeout + grace, fut).await
        });
        let elapsed = start.elapsed();
        watchdog.abort();

        let captured = self.capture.take();
        let counters = spans::aggregate(&captured);
        let timed_out = cancel.reason() == Some(QueryCancellationReason::Timeout);

        match result {
            Ok(Ok(doc)) => match canon::canonicalize(&doc) {
                Ok(canonical) => {
                    let rows = canonical.rows;
                    let heads = keep_heads.then(|| canonical.heads(HEADS));
                    let hash = canonical.hash;
                    Outcome {
                        wall: elapsed,
                        status: Status::Ok,
                        rows,
                        hash,
                        heads,
                        counters,
                        error: None,
                    }
                }
                // A result document the canonicalizer doesn't recognize is a
                // hard error, not an empty success — otherwise a formatter
                // shape change could bless 0-row oracles.
                Err(e) => Outcome {
                    wall: elapsed,
                    status: Status::Error,
                    rows: 0,
                    hash: String::new(),
                    heads: None,
                    counters,
                    error: Some(format!("canonicalization failed: {e}")),
                },
            },
            Ok(Err(_e)) if timed_out => Outcome {
                // Cooperative cancel returned an engine error; record the cap.
                wall: timeout,
                status: Status::Dnf,
                rows: 0,
                hash: String::new(),
                heads: None,
                counters,
                error: Some(format!("deadline exceeded ({}s)", timeout.as_secs())),
            },
            Ok(Err(e)) => Outcome {
                wall: elapsed,
                status: Status::Error,
                rows: 0,
                hash: String::new(),
                heads: None,
                counters,
                error: Some(e.to_string()),
            },
            Err(_elapsed) => Outcome {
                // Hard backstop fired — future dropped mid-scan.
                wall: timeout,
                status: Status::Dnf,
                rows: 0,
                hash: String::new(),
                heads: None,
                counters,
                error: Some(format!(
                    "hard timeout backstop ({}s); scan may still be draining",
                    (timeout + grace).as_secs()
                )),
            },
        }
    }

    fn build_record(
        &self,
        query_id: &str,
        target: &Target,
        is_virtual: bool,
        cache_state: &str,
        outcomes: Vec<Outcome>,
        expected: ExpectedOutcome,
    ) -> RunRecord {
        debug_assert!(!outcomes.is_empty(), "at least one measured rep");
        let all_walls_ms: Vec<u64> = outcomes.iter().map(|o| ms(o.wall)).collect();
        let mut order: Vec<usize> = (0..outcomes.len()).collect();
        order.sort_by_key(|&i| outcomes[i].wall);
        let median_pos = order[(outcomes.len() - 1) / 2];
        let chosen = &outcomes[median_pos];
        let spans_missing = spans::spans_missing(&chosen.counters, is_virtual);
        // A hard engine error on a query the manifest declares is *expected* to
        // error on this target kind is a gating pass, recorded as `ExpectedError`
        // (not `Error`). A Dnf/Ok is left untouched — an "expected error" that
        // instead timed out or returned rows is a real signal, not a pass.
        let status = if chosen.status == Status::Error && expected == ExpectedOutcome::Error {
            Status::ExpectedError
        } else {
            chosen.status
        };
        RunRecord {
            query_id: query_id.to_string(),
            target: target.id.clone(),
            cache_state: cache_state.to_string(),
            rep: median_pos,
            reps: outcomes.len(),
            wall_ms: ms(chosen.wall),
            all_walls_ms,
            status,
            rows: chosen.rows,
            result_hash: chosen.hash.clone(),
            counters: chosen.counters.clone(),
            spans_missing,
            error: chosen.error.clone(),
            heads: chosen.heads.clone(),
        }
    }
}

/// Milliseconds of a `Duration`, saturating.
fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether a query's result is an RDF graph (CONSTRUCT/DESCRIBE) rather than a
/// solution table. Such a query must be formatted as JSON-LD — SPARQL-JSON
/// renders a graph as empty bindings. Scans past leading comments and
/// `PREFIX`/`BASE` declarations to the first query keyword.
fn is_graph_query(sparql: &str) -> bool {
    for raw in sparql.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let upper = line.to_ascii_uppercase();
        if upper.starts_with("PREFIX") || upper.starts_with("BASE") {
            continue;
        }
        return upper.starts_with("CONSTRUCT") || upper.starts_with("DESCRIBE");
    }
    false
}

/// Extract the single scalar COUNT value from a `SELECT (COUNT(*) AS ?n)` result.
pub fn scalar_count(doc: &Value) -> Option<u64> {
    let bindings = doc.get("results")?.get("bindings")?.as_array()?;
    let first = bindings.first()?.as_object()?;
    let cell = first.values().next()?;
    cell.get("value")?.as_str()?.trim().parse::<u64>().ok()
}

// ---------------------------------------------------------------------------
// Cold protocol: subprocess-of-self + disk-cache clear
// ---------------------------------------------------------------------------

/// The home-scoped Iceberg / binary-index artifact cache dir vbench pins for the
/// cold protocol.
///
/// The engine's default cache is machine-global (`$TMPDIR/fluree_binary_cache`),
/// which is neither home-scoped nor safe to clear blindly. So vbench pins a cache
/// dir *inside the target home* (a sibling of `storage/`, never `storage/`
/// itself) that a cold `exec-one` owns and can safely empty.
pub fn target_cache_dir(target: &Target) -> PathBuf {
    target.fluree_home.join(".vbench-iceberg-cache")
}

/// Remove the cold cache dir before a cold execution. Refuses any path whose
/// final component doesn't look like a vbench cache — a guard against ever
/// deleting `storage/` or ledger data.
pub fn clear_cold_cache(cache_dir: &Path) -> Result<()> {
    let name = cache_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    if !(name.contains("vbench") && name.contains("cache")) {
        anyhow::bail!(
            "refusing to clear '{}': not a vbench cache dir (name must contain 'vbench' and 'cache')",
            cache_dir.display()
        );
    }
    if cache_dir.exists() {
        std::fs::remove_dir_all(cache_dir)
            .with_context(|| format!("clearing cold cache {}", cache_dir.display()))?;
    }
    Ok(())
}

/// Flags for one cold `exec-one` child, mirroring the parent `run`'s overrides
/// so a cold run honors the same `--cache-dir` / `--timeout-s` a hot run would.
pub struct ColdRunOpts<'a> {
    pub keep_heads: bool,
    /// The parent's global `--cache-dir` override, forwarded to the child
    /// (which clears it before opening — the guard in [`clear_cold_cache`]
    /// still applies, so it must look like a vbench cache dir).
    pub cache_dir: Option<&'a Path>,
    /// The parent's `--timeout-s` per-query deadline override.
    pub timeout_s: Option<u64>,
}

/// The child argv for one cold `exec-one` (factored out so the flag forwarding
/// is unit-testable without spawning).
fn cold_exec_args(
    corpus_dir: &Path,
    targets_dir: &Path,
    target_id: &str,
    query_id: &str,
    opts: &ColdRunOpts<'_>,
) -> Vec<std::ffi::OsString> {
    let mut args: Vec<std::ffi::OsString> = vec![
        "--corpus-dir".into(),
        corpus_dir.into(),
        "--targets-dir".into(),
        targets_dir.into(),
    ];
    if let Some(dir) = opts.cache_dir {
        args.push("--cache-dir".into());
        args.push(dir.into());
    }
    args.extend::<[std::ffi::OsString; 6]>([
        "exec-one".into(),
        "--query".into(),
        query_id.into(),
        "--target".into(),
        target_id.into(),
        "--cold".into(),
    ]);
    if let Some(secs) = opts.timeout_s {
        args.push("--timeout-s".into());
        args.push(secs.to_string().into());
    }
    if opts.keep_heads {
        args.push("--keep-heads".into());
    }
    args
}

/// Cold-run one query by spawning `vbench exec-one --cold` in a fresh subprocess.
///
/// The **child** clears the home-scoped disk cache before executing (see
/// `cmd_exec_one`), and a fresh process empties the in-process caches (catalog
/// TTL, OAuth token, Parquet footer LRU, leaflet), so the child pays the full
/// cold cost. The child inherits this process's environment, so `VBENCH_PAT`
/// flows through to virtual targets, and the parent's `--cache-dir` /
/// `--timeout-s` overrides are forwarded as flags. The parent parses the
/// child's stdout `RunRecord` (already `cache_state = "cold"`). Pacing between
/// children is the caller's responsibility.
pub fn cold_run_query(
    exe: &Path,
    corpus_dir: &Path,
    targets_dir: &Path,
    target: &Target,
    query_id: &str,
    opts: &ColdRunOpts<'_>,
) -> Result<RunRecord> {
    let mut cmd = std::process::Command::new(exe);
    cmd.args(cold_exec_args(
        corpus_dir,
        targets_dir,
        &target.id,
        query_id,
        opts,
    ));
    let output = cmd
        .output()
        .with_context(|| format!("spawning cold exec-one for {query_id}/{}", target.id))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("cold exec-one {query_id}/{} failed: {stderr}", target.id);
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("parsing cold exec-one output for {query_id}/{}", target.id))
}

/// Install the pathway span-capture subscriber (once per process).
fn install_span_capture() -> BenchSpanCapture {
    use tracing_subscriber::prelude::*;
    let capture = BenchSpanCapture::new();
    let layer = capture.layer(Some(spans::SPAN_ALLOWLIST));
    let allowlist: Vec<String> = spans::SPAN_ALLOWLIST
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let _ = tracing_subscriber::registry()
        .with(layer.with_filter(span_name_filter(Some(allowlist))))
        .try_init();
    capture
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cold child must receive the parent's `--cache-dir` and
    /// `--timeout-s` overrides — they were silently dropped once.
    #[test]
    fn cold_exec_args_forward_parent_overrides() {
        let opts = ColdRunOpts {
            keep_heads: true,
            cache_dir: Some(Path::new("/homes/t/.vbench-iceberg-cache")),
            timeout_s: Some(35),
        };
        let args = cold_exec_args(
            Path::new("/c/corpus"),
            Path::new("/c/targets"),
            "virtual-sf01",
            "q011",
            &opts,
        );
        let args: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let has_pair = |k: &str, v: &str| args.windows(2).any(|w| w[0] == k && w[1] == v);
        assert!(has_pair("--cache-dir", "/homes/t/.vbench-iceberg-cache"));
        assert!(has_pair("--timeout-s", "35"));
        assert!(has_pair("--query", "q011"));
        assert!(has_pair("--target", "virtual-sf01"));
        assert!(args.contains(&"--cold".to_string()));
        assert!(args.contains(&"--keep-heads".to_string()));
        // Global flags must precede the subcommand for clap to accept them.
        let sub = args.iter().position(|a| a == "exec-one").unwrap();
        let cache = args.iter().position(|a| a == "--cache-dir").unwrap();
        assert!(cache < sub, "--cache-dir is a global flag");
        // No override, no flag.
        let bare = cold_exec_args(
            Path::new("/c/corpus"),
            Path::new("/c/targets"),
            "virtual-sf01",
            "q011",
            &ColdRunOpts {
                keep_heads: false,
                cache_dir: None,
                timeout_s: None,
            },
        );
        let bare: Vec<String> = bare
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(!bare.contains(&"--cache-dir".to_string()));
        assert!(!bare.contains(&"--timeout-s".to_string()));
        assert!(!bare.contains(&"--keep-heads".to_string()));
    }

    #[test]
    fn clear_cold_cache_refuses_non_cache_paths() {
        // The guard must refuse anything that isn't clearly a vbench cache dir —
        // this is what stops the cold protocol from ever deleting storage/ or
        // ledger data.
        assert!(clear_cold_cache(Path::new("/Users/x/vbench/.fluree/storage")).is_err());
        assert!(clear_cold_cache(Path::new("/var/data/ledger")).is_err());
        assert!(clear_cold_cache(Path::new("/tmp/enterprise-sf01")).is_err());
        // A properly-named, nonexistent cache dir is a no-op success.
        let ok = std::env::temp_dir().join("vbench-nonexistent-iceberg-cache");
        assert!(clear_cold_cache(&ok).is_ok());
    }
}
