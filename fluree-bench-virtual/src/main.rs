//! vbench — corpus benchmark runner for native-vs-virtual SPARQL execution.
//!
//! Subcommands:
//! - `setup --verify` — open each target and run a trivial probe (and, for a
//!   native target with a known triple count, assert schema stability).
//! - `run` — for each query × target, run a priming rep + measured reps and
//!   stream [`schema::RunRecord`]s to a `run.jsonl`.
//! - `exec-one` — a single execution to stdout (the cold-mode hook).
//! - `report` — render a `run.jsonl` as a native-vs-virtual comparison table.
//!
//! See `README.md` for the run protocol, corpus conventions, and caveats.

mod baseline;
mod budgets;
mod canon;
mod corpus;
mod dashboard;
mod exec;
mod report;
mod schema;
mod spans;
mod targets;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::corpus::Corpus;
use crate::exec::{Engine, RunParams};
use crate::schema::{Line, RunMeta, RunRecord, Status, TargetFingerprint, SCHEMA_VERSION};
use crate::targets::{Target, TargetKind};

const DEFAULT_NATIVE_REPS: usize = 5;
const DEFAULT_VIRTUAL_REPS: usize = 3;

/// Probe: count a small dimension class (works on native and virtual alike).
const PROBE_CLASS: &str =
    "PREFIX edw: <http://ns.fluree.dev/edw#> SELECT (COUNT(*) AS ?n) WHERE { ?s a edw:Store }";
/// Probe: total triple count, for the native schema-stability assertion.
const PROBE_TOTAL: &str = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";

#[derive(Parser)]
#[command(
    name = "vbench",
    about = "Native-vs-virtual SPARQL corpus benchmark runner"
)]
struct Cli {
    /// Corpus directory (default: <crate>/corpus).
    #[arg(long, global = true)]
    corpus_dir: Option<PathBuf>,
    /// Targets directory (default: <crate>/targets).
    #[arg(long, global = true)]
    targets_dir: Option<PathBuf>,
    /// Pin the binary-index / Iceberg on-disk artifact cache to this directory
    /// (default `$TMPDIR/fluree_binary_cache`). The cold protocol clears it
    /// between subprocess reps.
    #[arg(long, global = true)]
    cache_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Open each target and run a probe; assert triple count for native targets.
    Setup {
        /// Run the verification probe (the only mode today; kept explicit).
        #[arg(long)]
        verify: bool,
        /// Targets to verify (comma-separated). Default: native-sf01.
        #[arg(long, value_delimiter = ',', default_value = "native-sf01")]
        targets: Vec<String>,
    },
    /// Run the corpus against one or more targets, writing a run.jsonl.
    Run {
        /// Targets (comma-separated), e.g. native-sf01,virtual-sf20.
        #[arg(long, value_delimiter = ',', required = true)]
        targets: Vec<String>,
        /// Restrict to a subset (e.g. smoke).
        #[arg(long)]
        subset: Option<String>,
        /// Run only these query ids (comma-separated), in corpus order — used to
        /// resume a partial run in-process (hot-comparable). Overrides --subset.
        #[arg(long, value_delimiter = ',')]
        queries: Vec<String>,
        /// Output run.jsonl path (default: <crate>/results/runs/run-<ts>.jsonl).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Retain the first 20 canonical rows per record.
        #[arg(long)]
        keep_heads: bool,
        /// Measured reps for native targets.
        #[arg(long, default_value_t = DEFAULT_NATIVE_REPS)]
        native_reps: usize,
        /// Measured reps for virtual targets.
        #[arg(long, default_value_t = DEFAULT_VIRTUAL_REPS)]
        virtual_reps: usize,
        /// Override the per-query deadline (seconds) for every query, instead of
        /// the manifest's `timeout_s`. Used by the SF20 survey (slower scale).
        #[arg(long)]
        timeout_s: Option<u64>,
        /// Cache regime. `hot` (default) primes once and reuses one handle;
        /// `cold` spawns a fresh `exec-one --cold` subprocess per query that
        /// clears the home-scoped disk artifact cache first.
        #[arg(long, value_enum, default_value_t = CacheState::Hot)]
        cache_state: CacheState,
        /// Mark this run informational (never a gate): `baseline` refuses it and
        /// `compare` skips it. Use for the live SF20 stress survey.
        #[arg(long)]
        survey: bool,
        /// Cap the number of queries run (per the selected order) — bounds live
        /// Snowflake cost on a survey.
        #[arg(long)]
        max_queries: Option<usize>,
        /// Stop starting new queries for a target once its cumulative measured
        /// wall exceeds this many seconds (a soft per-target budget).
        #[arg(long)]
        max_wall_budget_s: Option<u64>,
    },
    /// Run a single query against a single target; print one RunRecord as JSON.
    ExecOne {
        #[arg(long)]
        query: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        keep_heads: bool,
        /// Clear the target's home-scoped disk artifact cache before executing
        /// (the cold protocol's per-query unit). Records `cache_state = cold`.
        #[arg(long)]
        cold: bool,
        /// Override the query's manifest `timeout_s` deadline (seconds).
        /// A cold `run --timeout-s` forwards its override through this flag.
        #[arg(long)]
        timeout_s: Option<u64>,
    },
    /// Render a run.jsonl as a comparison table (or --json).
    Report {
        #[arg(long)]
        run: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Bless baselines: `--expected` writes per-query native correctness oracles
    /// (baselines/expected/<qid>.json); `--perf` writes per-target perf
    /// references (baselines/perf/<target>.json). At least one is required.
    Baseline {
        /// Write the native correctness oracles.
        #[arg(long)]
        expected: bool,
        /// Write the per-target perf references.
        #[arg(long)]
        perf: bool,
        /// Bless from this run.jsonl. If omitted with `--expected`, a native run
        /// is executed fresh from `--targets`.
        #[arg(long)]
        run: Option<PathBuf>,
        /// Targets to run when no `--run` is given (default native-sf01).
        #[arg(long, value_delimiter = ',', default_value = "native-sf01")]
        targets: Vec<String>,
        /// Baselines directory (default: <crate>/baselines).
        #[arg(long)]
        baseline_dir: Option<PathBuf>,
        /// Allow `--expected` to overwrite a blessed oracle whose hash/rows
        /// differ from this run. Without it a differing oracle refuses (and
        /// prints the delta) so a native regression can't be blessed as truth.
        #[arg(long)]
        force: bool,
    },
    /// Compare a run against blessed baselines: expected-hash check + perf ratio
    /// vs budget. `--gate` exits nonzero on any violation (auto-reruns a perf
    /// violation once before declaring it red).
    Compare {
        #[arg(long)]
        run: PathBuf,
        /// Baselines directory (default: <crate>/baselines).
        #[arg(long)]
        baseline_dir: Option<PathBuf>,
        /// Fail (nonzero exit) on any violation.
        #[arg(long)]
        gate: bool,
    },
    /// Render one or more run.jsonl files as a self-contained HTML dashboard.
    Dashboard {
        /// Run files to merge (repeatable), e.g. --run native.jsonl --run virtual.jsonl.
        #[arg(long = "run", required = true)]
        runs: Vec<PathBuf>,
        /// Output HTML path (default: <crate>/results/dashboard.html).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Dashboard subtitle.
        #[arg(long, default_value = "virtual-dataset performance corpus")]
        title: String,
    },
}

/// Cache regime for `run`.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum CacheState {
    Hot,
    Cold,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let corpus_dir = cli
        .corpus_dir
        .clone()
        .unwrap_or_else(|| crate_dir().join("corpus"));
    let targets_dir = cli
        .targets_dir
        .clone()
        .unwrap_or_else(|| crate_dir().join("targets"));
    let cache_dir = cli.cache_dir.clone();

    match cli.command {
        Command::Setup { verify: _, targets } => {
            cmd_setup(&targets_dir, &targets, cache_dir.as_deref())
        }
        Command::Run {
            targets,
            subset,
            queries,
            out,
            keep_heads,
            native_reps,
            virtual_reps,
            timeout_s,
            cache_state,
            survey,
            max_queries,
            max_wall_budget_s,
        } => cmd_run(RunArgs {
            corpus_dir: &corpus_dir,
            targets_dir: &targets_dir,
            cache_dir: cache_dir.as_deref(),
            target_ids: &targets,
            subset: subset.as_deref(),
            queries: &queries,
            out,
            keep_heads,
            native_reps,
            virtual_reps,
            timeout_s,
            cold: cache_state == CacheState::Cold,
            survey,
            max_queries,
            max_wall_budget_s,
        }),
        Command::ExecOne {
            query,
            target,
            keep_heads,
            cold,
            timeout_s,
        } => cmd_exec_one(
            &corpus_dir,
            &targets_dir,
            cache_dir.as_deref(),
            &query,
            &target,
            keep_heads,
            cold,
            timeout_s,
        ),
        Command::Report { run, json } => cmd_report(&run, json),
        Command::Baseline {
            expected,
            perf,
            run,
            targets,
            baseline_dir,
            force,
        } => cmd_baseline(BaselineArgs {
            corpus_dir: &corpus_dir,
            targets_dir: &targets_dir,
            cache_dir: cache_dir.as_deref(),
            expected,
            perf,
            run,
            target_ids: &targets,
            baseline_dir,
            force,
        }),
        Command::Compare {
            run,
            baseline_dir,
            gate,
        } => cmd_compare(
            &corpus_dir,
            &targets_dir,
            cache_dir.as_deref(),
            &run,
            baseline_dir,
            gate,
        ),
        Command::Dashboard { runs, out, title } => cmd_dashboard(&corpus_dir, &runs, out, &title),
    }
}

/// The crate root (used for default corpus/targets/results locations).
fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn cmd_setup(targets_dir: &Path, target_ids: &[String], cache_dir: Option<&Path>) -> Result<()> {
    let engine = Engine::new()?;
    let mut all_ok = true;
    for id in target_ids {
        let target = Target::load(targets_dir, id)?;
        print!("target {id} ({}) ... ", target.kind_str());
        std::io::stdout().flush().ok();
        if let Err(e) = target.ensure_runnable() {
            println!("SKIP: {e}");
            continue;
        }
        let fluree = match engine.open(&target, cache_dir) {
            Ok(f) => f,
            Err(e) => {
                println!("FAIL (open): {e}");
                all_ok = false;
                continue;
            }
        };
        match engine.probe(&fluree, &target, PROBE_CLASS, Duration::from_secs(120)) {
            Ok((wall, doc)) => {
                let n = exec::scalar_count(&doc).unwrap_or(0);
                println!("ok (Store count = {n}, {} ms)", wall.as_millis());
            }
            Err(e) => {
                println!("FAIL (probe): {e}");
                all_ok = false;
                continue;
            }
        }

        if let Some(expected) = target.expected_total_triples {
            print!("  total-triples assertion ... ");
            std::io::stdout().flush().ok();
            match engine.probe(&fluree, &target, PROBE_TOTAL, Duration::from_secs(600)) {
                Ok((wall, doc)) => {
                    let actual = exec::scalar_count(&doc).unwrap_or(0);
                    if actual == expected {
                        println!("ok ({actual} triples, {} ms)", wall.as_millis());
                    } else {
                        println!("MISMATCH: expected {expected}, got {actual}");
                        all_ok = false;
                    }
                }
                Err(e) => {
                    println!("FAIL: {e}");
                    all_ok = false;
                }
            }
        }
    }
    if all_ok {
        Ok(())
    } else {
        anyhow::bail!("one or more targets failed verification")
    }
}

/// Arguments for a corpus run (grouped to keep the signature manageable).
struct RunArgs<'a> {
    corpus_dir: &'a Path,
    targets_dir: &'a Path,
    cache_dir: Option<&'a Path>,
    target_ids: &'a [String],
    subset: Option<&'a str>,
    queries: &'a [String],
    out: Option<PathBuf>,
    keep_heads: bool,
    native_reps: usize,
    virtual_reps: usize,
    timeout_s: Option<u64>,
    cold: bool,
    survey: bool,
    max_queries: Option<usize>,
    max_wall_budget_s: Option<u64>,
}

fn cmd_run(args: RunArgs) -> Result<()> {
    let corpus = Corpus::load(args.corpus_dir)?;
    // Explicit --queries (a resume) overrides --subset.
    let mut queries = if args.queries.is_empty() {
        corpus.select(args.subset)
    } else {
        corpus.select_by_ids(args.queries)?
    };
    if queries.is_empty() {
        anyhow::bail!("no queries match subset {:?}", args.subset);
    }
    if let Some(cap) = args.max_queries {
        queries.truncate(cap);
    }

    // Resolve + validate every target up front (fail fast on a pending target).
    let mut resolved = Vec::new();
    for id in args.target_ids {
        let target = Target::load(args.targets_dir, id)?;
        target.ensure_runnable()?;
        resolved.push(target);
    }

    let engine = Engine::new()?;
    let prov = provenance();
    let meta = RunMeta {
        schema_version: SCHEMA_VERSION,
        run_id: prov.run_id.clone(),
        timestamp: prov.timestamp.clone(),
        git_commit: prov.git_commit.clone(),
        git_dirty: prov.git_dirty,
        build_profile: prov.build_profile.clone(),
        host: prov.host.clone(),
        runtime: engine.runtime_shape(),
        subset: args.subset.map(str::to_string),
        corpus_version: corpus.corpus_version,
        survey: args.survey,
        targets: resolved.iter().map(target_fingerprint).collect(),
    };

    let out_path = args.out.unwrap_or_else(|| {
        crate_dir()
            .join("results")
            .join("runs")
            .join(format!("run-{}-{}.jsonl", prov.file_stamp, prov.run_id))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating results dir {}", parent.display()))?;
    }
    let mut writer = RunWriter::create(&out_path)?;
    writer.write(&Line::Meta(meta))?;

    let mode = if args.cold { "cold" } else { "warm" };
    eprintln!(
        "vbench run {} [{mode}{}] -> {}  ({} queries x {} targets)",
        prov.run_id,
        if args.survey { ", survey" } else { "" },
        out_path.display(),
        queries.len(),
        resolved.len()
    );

    let exe =
        std::env::current_exe().context("resolving current executable for cold subprocess")?;

    for target in &resolved {
        eprintln!("== target {} ({}) ==", target.id, target.kind_str());
        let reps = match target.kind {
            TargetKind::Native => args.native_reps,
            TargetKind::Virtual => args.virtual_reps,
        };
        // Warm mode reuses one open handle; cold mode spawns a fresh subprocess
        // per query (the child clears its own home-scoped disk cache).
        let fluree = if args.cold {
            None
        } else {
            Some(engine.open(target, args.cache_dir)?)
        };

        let mut spent = Duration::ZERO;
        let mut first = true;
        for q in &queries {
            if let Some(budget) = args.max_wall_budget_s {
                if spent.as_secs() >= budget {
                    eprintln!(
                        "  budget {budget}s exhausted for {} — skipping rest",
                        target.id
                    );
                    break;
                }
            }
            let sparql = corpus.read_query(q)?;
            let expected = q.expected_status.for_target(target.is_virtual());
            let record = if args.cold {
                // 2 s pacing between cold children (live-Snowflake courtesy).
                if !first {
                    std::thread::sleep(Duration::from_secs(2));
                }
                exec::cold_run_query(
                    &exe,
                    args.corpus_dir,
                    args.targets_dir,
                    target,
                    &q.id,
                    &exec::ColdRunOpts {
                        keep_heads: args.keep_heads,
                        cache_dir: args.cache_dir,
                        timeout_s: args.timeout_s,
                    },
                )?
            } else {
                let params = RunParams {
                    timeout: Duration::from_secs(args.timeout_s.unwrap_or(q.timeout_s)),
                    reps,
                    keep_heads: args.keep_heads,
                };
                engine.run_query(
                    fluree.as_ref().unwrap(),
                    target,
                    &q.id,
                    &sparql,
                    &params,
                    expected,
                )
            };
            first = false;
            spent += Duration::from_millis(record.all_walls_ms.iter().sum());
            log_progress(q, &record);
            check_expectations(q, &record);
            writer.write(&Line::Record(record))?;
        }
    }

    eprintln!("done -> {}", out_path.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)] // thin CLI adapter over the Engine call
fn cmd_exec_one(
    corpus_dir: &Path,
    targets_dir: &Path,
    cache_dir: Option<&Path>,
    query_id: &str,
    target_id: &str,
    keep_heads: bool,
    cold: bool,
    timeout_s: Option<u64>,
) -> Result<()> {
    let corpus = Corpus::load(corpus_dir)?;
    let q = corpus
        .get(query_id)
        .with_context(|| format!("no query '{query_id}' in corpus"))?;
    let target = Target::load(targets_dir, target_id)?;
    target.ensure_runnable()?;

    // Cold: pin the cache to a home-scoped dir and clear it *before* opening
    // (the cache is read at open time), so this exec pays the full cold cost.
    let cache_override;
    let open_cache = if cold {
        let dir = cache_dir
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| exec::target_cache_dir(&target));
        exec::clear_cold_cache(&dir)?;
        cache_override = dir;
        Some(cache_override.as_path())
    } else {
        cache_dir
    };

    let engine = Engine::new()?;
    let fluree = engine.open(&target, open_cache)?;
    let sparql = corpus.read_query(q)?;
    let expected = q.expected_status.for_target(target.is_virtual());
    let record = engine.exec_one(
        &fluree,
        &target,
        &q.id,
        &sparql,
        Duration::from_secs(timeout_s.unwrap_or(q.timeout_s)),
        keep_heads,
        expected,
        cold,
    );
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

fn cmd_report(run: &Path, json: bool) -> Result<()> {
    let (meta, records) = report::read_run(run)?;
    if json {
        report::print_json(&meta, &records)
    } else {
        report::print_table(&meta, &records);
        Ok(())
    }
}

/// Arguments for `vbench baseline`.
struct BaselineArgs<'a> {
    corpus_dir: &'a Path,
    targets_dir: &'a Path,
    cache_dir: Option<&'a Path>,
    expected: bool,
    perf: bool,
    run: Option<PathBuf>,
    target_ids: &'a [String],
    baseline_dir: Option<PathBuf>,
    force: bool,
}

fn cmd_baseline(args: BaselineArgs) -> Result<()> {
    if !args.expected && !args.perf {
        anyhow::bail!("specify --expected and/or --perf");
    }
    let baselines = args
        .baseline_dir
        .unwrap_or_else(|| crate_dir().join("baselines"));
    let corpus = Corpus::load(args.corpus_dir)?;

    // Source records: an existing run, or a fresh (warm, heads-on) native run.
    let (meta, records) = match &args.run {
        Some(path) => report::read_run(path)?,
        None => {
            eprintln!(
                "baseline: no --run given; executing a fresh run of {:?}",
                args.target_ids
            );
            run_corpus(
                &corpus,
                args.targets_dir,
                args.cache_dir,
                args.target_ids,
                None,
                true,
            )?
        }
    };
    if meta.survey {
        anyhow::bail!("refusing to bless a survey run (informational / never a gate)");
    }

    if args.expected {
        let s = baseline::write_expected(&meta, &records, &corpus, &baselines, args.force)?;
        eprintln!(
            "baseline --expected: wrote {} oracle(s) under {}/expected \
             ({} force-overwritten, {} unchanged); skipped {} ({:?})",
            s.written.len(),
            baselines.display(),
            s.overwritten.len(),
            s.unchanged.len(),
            s.skipped.len(),
            s.skipped
        );
    }
    if args.perf {
        let written = baseline::write_perf(&meta, &records, &baselines)?;
        for p in &written {
            eprintln!("baseline --perf: wrote {p}");
        }
    }
    Ok(())
}

/// Execute the whole corpus against the given targets in-process (warm), used by
/// `baseline` when no `--run` is supplied. Returns the run meta + records.
fn run_corpus(
    corpus: &Corpus,
    targets_dir: &Path,
    cache_dir: Option<&Path>,
    target_ids: &[String],
    subset: Option<&str>,
    keep_heads: bool,
) -> Result<(RunMeta, Vec<RunRecord>)> {
    let queries = corpus.select(subset);
    let mut resolved = Vec::new();
    for id in target_ids {
        let target = Target::load(targets_dir, id)?;
        target.ensure_runnable()?;
        resolved.push(target);
    }
    let engine = Engine::new()?;
    let prov = provenance();
    let meta = RunMeta {
        schema_version: SCHEMA_VERSION,
        run_id: prov.run_id.clone(),
        timestamp: prov.timestamp.clone(),
        git_commit: prov.git_commit.clone(),
        git_dirty: prov.git_dirty,
        build_profile: prov.build_profile.clone(),
        host: prov.host.clone(),
        runtime: engine.runtime_shape(),
        subset: subset.map(str::to_string),
        corpus_version: corpus.corpus_version,
        survey: false,
        targets: resolved.iter().map(target_fingerprint).collect(),
    };
    let mut records = Vec::new();
    for target in &resolved {
        let fluree = engine.open(target, cache_dir)?;
        let reps = match target.kind {
            TargetKind::Native => DEFAULT_NATIVE_REPS,
            TargetKind::Virtual => DEFAULT_VIRTUAL_REPS,
        };
        for q in &queries {
            let sparql = corpus.read_query(q)?;
            let expected = q.expected_status.for_target(target.is_virtual());
            let params = RunParams {
                timeout: Duration::from_secs(q.timeout_s),
                reps,
                keep_heads,
            };
            let record = engine.run_query(&fluree, target, &q.id, &sparql, &params, expected);
            log_progress(q, &record);
            records.push(record);
        }
    }
    Ok((meta, records))
}

fn cmd_compare(
    corpus_dir: &Path,
    targets_dir: &Path,
    cache_dir: Option<&Path>,
    run: &Path,
    baseline_dir: Option<PathBuf>,
    gate: bool,
) -> Result<()> {
    let (meta, records) = report::read_run(run)?;
    if meta.survey {
        eprintln!("compare: '{}' is a survey run — not gated", meta.run_id);
        return Ok(());
    }
    let baselines = baseline_dir.unwrap_or_else(|| crate_dir().join("baselines"));
    let budgets = budgets::Budgets::load(&crate_dir().join("budgets.json"))?;
    let corpus = Corpus::load(corpus_dir)?;

    use std::collections::HashMap;
    let is_virtual: HashMap<&str, bool> = meta
        .targets
        .iter()
        .map(|t| (t.id.as_str(), t.kind == "virtual"))
        .collect();
    let mut perf_cache: HashMap<String, Option<baseline::PerfBaseline>> = HashMap::new();

    // Auto-rerun needs to re-execute a violating query in-process.
    let engine = Engine::new()?;
    let mut opened: HashMap<String, fluree_db_api::Fluree> = HashMap::new();

    let mut fails = 0usize;
    let mut violations = 0usize;
    for r in &records {
        let expected = baseline::load_expected(&baselines, &r.query_id)?;
        let pb = perf_cache
            .entry(r.target.clone())
            .or_insert_with(|| baseline::load_perf(&baselines, &r.target).ok().flatten());
        let perf_entry = pb
            .as_ref()
            .and_then(|b| b.entries.get(&r.query_id))
            .cloned();
        let virt = is_virtual.get(r.target.as_str()).copied().unwrap_or(false);
        let cold = r.cache_state == "cold";
        let budget = budgets.budget(&r.query_id, virt, cold);
        let hash_gate = corpus
            .get(&r.query_id)
            .map(|q| q.hash_gate)
            .unwrap_or_default();
        let mut outcome =
            baseline::compare_one(r, expected.as_ref(), perf_entry.as_ref(), budget, hash_gate);

        // Live-noise discipline: auto-rerun a perf violation once before red.
        if let (Some(p), Some(b)) = (&outcome.perf, budget) {
            if p.violated {
                let baseline_ms = p.baseline_ms;
                match rerun_query(&engine, &mut opened, targets_dir, cache_dir, &corpus, r) {
                    Ok(rerun_ms) => {
                        outcome.reran = true;
                        let still = baseline::over_budget(baseline_ms, rerun_ms, b);
                        if let Some(pc) = outcome.perf.as_mut() {
                            pc.observed_ms = rerun_ms;
                            pc.ratio = if baseline_ms == 0 {
                                1.0
                            } else {
                                rerun_ms as f64 / baseline_ms as f64
                            };
                            pc.violated = still;
                        }
                    }
                    Err(e) => eprintln!("  (auto-rerun of {} failed: {e})", r.query_id),
                }
            }
        }

        if outcome.hash.is_fail() {
            fails += 1;
            let detail = match &outcome.hash {
                baseline::HashCheck::RowsMismatch { expected, observed } => {
                    format!("rows-only gate: {expected} → {observed}")
                }
                _ => "hash changed".to_string(),
            };
            println!("FAIL-PARITY {:<6} {:<16} {detail}", r.query_id, r.target);
        }
        if let Some(p) = &outcome.perf {
            if p.violated {
                violations += 1;
                let b = p.budget.map(|b| b.pct).unwrap_or(0.0);
                println!(
                    "SLOW       {:<6} {:<16} blessed {}ms  observed {}ms  ratio {:.2}x  budget +{:.0}%{}",
                    r.query_id,
                    r.target,
                    p.baseline_ms,
                    p.observed_ms,
                    p.ratio,
                    b,
                    if outcome.reran { "  (confirmed on rerun)" } else { "" }
                );
            }
        }
    }
    eprintln!(
        "compare: {} record(s), {fails} hash mismatch(es), {violations} perf violation(s)",
        records.len()
    );
    if gate && (fails > 0 || violations > 0) {
        anyhow::bail!("gate failed: {fails} hash mismatch(es), {violations} perf violation(s)");
    }
    Ok(())
}

/// Re-execute one query in-process (warm, single rep) for the compare auto-rerun.
/// Opens (and caches) the target handle; returns the fresh median wall (ms).
fn rerun_query(
    engine: &Engine,
    opened: &mut std::collections::HashMap<String, fluree_db_api::Fluree>,
    targets_dir: &Path,
    cache_dir: Option<&Path>,
    corpus: &Corpus,
    record: &RunRecord,
) -> Result<u64> {
    let target = Target::load(targets_dir, &record.target)?;
    let q = corpus
        .get(&record.query_id)
        .with_context(|| format!("no query '{}' in corpus for rerun", record.query_id))?;
    let sparql = corpus.read_query(q)?;
    if !opened.contains_key(&record.target) {
        let fluree = engine.open(&target, cache_dir)?;
        opened.insert(record.target.clone(), fluree);
    }
    let fluree = opened.get(&record.target).unwrap();
    let expected = q.expected_status.for_target(target.is_virtual());
    let params = RunParams {
        timeout: Duration::from_secs(q.timeout_s),
        reps: 1,
        keep_heads: false,
    };
    let rec = engine.run_query(fluree, &target, &q.id, &sparql, &params, expected);
    Ok(rec.wall_ms)
}

fn cmd_dashboard(
    corpus_dir: &Path,
    runs: &[PathBuf],
    out: Option<PathBuf>,
    title: &str,
) -> Result<()> {
    let corpus = Corpus::load(corpus_dir)?;
    let mut data = Vec::new();
    for path in runs {
        let (meta, records) = report::read_run(path)?;
        data.push(dashboard::RunData { meta, records });
    }
    let html = dashboard::render(&data, &corpus, title);
    let out_path = out.unwrap_or_else(|| crate_dir().join("results").join("dashboard.html"));
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dashboard dir {}", parent.display()))?;
    }
    std::fs::write(&out_path, html)
        .with_context(|| format!("writing dashboard {}", out_path.display()))?;
    eprintln!("dashboard -> {}", out_path.display());
    Ok(())
}

/// One-line stderr progress for a completed record.
fn log_progress(q: &corpus::QueryDef, record: &RunRecord) {
    let status = match record.status {
        Status::Ok => "ok",
        Status::Dnf => "DNF",
        Status::Error => "ERR",
        Status::ExpectedError => "xERR",
    };
    let missing = if record.spans_missing.is_empty() {
        String::new()
    } else {
        format!("  spans_missing={:?}", record.spans_missing)
    };
    eprintln!(
        "  {:<6} {:<5} {:>7} ms  rows={:<5} reps={}{}",
        q.id, status, record.wall_ms, record.rows, record.reps, missing
    );
}

/// Warn (do not fail the run) when a record's row count violates its expected
/// bound — the run keeps going so one bad query doesn't abort a long sweep.
fn check_expectations(q: &corpus::QueryDef, record: &RunRecord) {
    if record.status != Status::Ok {
        return;
    }
    if !q.expected_rows.contains(record.rows) {
        eprintln!(
            "    !! {} rows={} outside expected {} (target {})",
            q.id, record.rows, q.expected_rows, record.target
        );
    }
}

fn target_fingerprint(t: &Target) -> TargetFingerprint {
    TargetFingerprint {
        id: t.id.clone(),
        kind: t.kind_str().to_string(),
        alias: t.alias.clone(),
        fluree_home: t.fluree_home.display().to_string(),
    }
}

/// Run provenance gathered at start.
struct Provenance {
    run_id: String,
    timestamp: String,
    file_stamp: String,
    git_commit: String,
    git_dirty: bool,
    build_profile: String,
    host: String,
}

fn provenance() -> Provenance {
    let now = chrono::Utc::now();
    Provenance {
        run_id: ulid::Ulid::new().to_string(),
        timestamp: now.to_rfc3339(),
        file_stamp: now.format("%Y%m%dT%H%M%SZ").to_string(),
        git_commit: git_short_commit(),
        git_dirty: git_dirty(),
        build_profile: if cfg!(debug_assertions) {
            "debug".to_string()
        } else {
            "release".to_string()
        },
        host: hostname(),
    }
}

fn git_short_commit() -> String {
    run_capture(
        "git",
        &[
            "-C",
            env!("CARGO_MANIFEST_DIR"),
            "rev-parse",
            "--short",
            "HEAD",
        ],
    )
    .unwrap_or_else(|| "unknown".to_string())
}

fn git_dirty() -> bool {
    run_capture(
        "git",
        &["-C", env!("CARGO_MANIFEST_DIR"), "status", "--porcelain"],
    )
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

fn hostname() -> String {
    run_capture("hostname", &[]).unwrap_or_else(|| "unknown".to_string())
}

/// Run a command and capture trimmed stdout, or `None` on any failure.
fn run_capture(cmd: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// A streaming JSONL writer that flushes after every line so a crash leaves a
/// readable partial run.
struct RunWriter {
    file: std::fs::File,
}

impl RunWriter {
    fn create(path: &Path) -> Result<Self> {
        let file = std::fs::File::create(path)
            .with_context(|| format!("creating run file {}", path.display()))?;
        Ok(Self { file })
    }

    fn write(&mut self, line: &Line) -> Result<()> {
        let json = serde_json::to_string(line)?;
        writeln!(self.file, "{json}")?;
        self.file.flush()?;
        Ok(())
    }
}
