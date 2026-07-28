//! `bench-baseline` — capture and compare performance baselines.
//!
//! Backs the per-phase performance gate described in BENCHMARKING.md
//! ("Baselines: capture & compare") and the Phase 0.0 protocol in
//! `docs/audit/2026-06-architecture-audit.md`.
//!
//! ```bash
//! # 1. Run the benches you care about:
//! cargo bench -p fluree-db-api --bench query_overlay_matrix
//!
//! # 2. Capture a labeled, git-stamped baseline:
//! cargo run -p fluree-bench-support --bin bench-baseline -- \
//!     capture --label phase-1-pre --out bench-baselines/phase-1-pre.json
//!
//! # 2b. ...or accumulate repeat runs into one noise-aware baseline:
//! for i in 1 2 3 4 5; do
//!   cargo bench -p fluree-db-api --bench query_overlay_matrix
//!   cargo run -p fluree-bench-support --bin bench-baseline -- \
//!       capture --label phase-1-pre --out bench-baselines/phase-1-pre.json --accumulate
//! done
//!
//! # 3. ...make changes, rerun the benches...
//!
//! # 4. Compare (exit 1 on a budget breach, 2 on a refusal to compare):
//! cargo run -p fluree-bench-support --bin bench-baseline -- \
//!     compare --baseline bench-baselines/phase-1-pre.json
//!
//! # PR-scoped subset (only scenarios whose ID contains the filter):
//! cargo run -p fluree-bench-support --bin bench-baseline -- \
//!     compare --baseline bench-baselines/phase-1-pre.json --only query_overlay_matrix
//! ```
//!
//! Argument parsing is hand-rolled (std only) to keep clap out of every
//! bench's dev-dependency tree.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use fluree_bench_support::baseline::{
    accumulate, capture, compare, default_criterion_dir, describe_corpus, host_class,
    load_baseline, save_baseline, CompareOptions, CompareStatus, Comparison, HostMismatchPolicy,
    Metric,
};
use fluree_bench_support::budget;
use fluree_bench_support::mem;
use fluree_bench_support::meta;
use fluree_bench_support::meta::DEFAULT_SHARE_DRIFT_PP;

/// Budget breach on an enforced metric.
const EXIT_BREACH: u8 = 1;
/// Refusal to compare (host class, corpus identity, schema version). Distinct
/// from a breach because the two demand different responses: a breach means
/// "your change is slower", a refusal means "this comparison is meaningless".
const EXIT_REFUSED: u8 = 2;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("capture") => run_capture(&args[1..]),
        Some("compare") => run_compare(&args[1..]),
        Some("help" | "--help" | "-h") | None => {
            print_usage();
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("unknown subcommand: {other}\n");
            print_usage();
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    eprintln!(
        "bench-baseline — capture and compare performance baselines\n\n\
         USAGE:\n  \
         bench-baseline capture --label <name> --out <file> [--criterion-dir <dir>] [--mem-dir <dir>]\n                          \
         [--meta-dir <dir>] [--host-class <name>] [--accumulate] [--clean-sidecars]\n  \
         bench-baseline compare --baseline <file> [--criterion-dir <dir>] [--mem-dir <dir>]\n                          \
         [--meta-dir <dir>] [--budget <file>] [--only <substr>] [--host-class <name>]\n                          \
         [--share-drift-pp <pp>] [--allow-host-mismatch] [--advisory]\n\n\
         GATE\n  \
         time and peak_mem enforce only when the baseline's host_class matches\n  \
         this host's (env FLUREE_BENCH_HOST_CLASS, else FLUREE_BENCH_RUNNER_CLASS,\n  \
         else {{os}}-{{arch}}). A mismatch is a REFUSAL (exit {EXIT_REFUSED}) unless\n  \
         --allow-host-mismatch downgrades those metrics to advisory.\n  \
         phase_share drift enforces regardless of host class — shares are ratios\n  \
         within one run, so the machine cancels.\n  \
         A corpus mismatch (sha256 / syntax / thread count) is always a refusal:\n  \
         there is no posture in which comparing different inputs is meaningful.\n  \
         --advisory forces every absolute metric advisory.\n\n\
         EXIT CODES\n  \
         0 within budget   {EXIT_BREACH} enforced budget breach   {EXIT_REFUSED} refused to compare\n\n\
         Run the benches first (`cargo bench ...`); this tool reads criterion's\n\
         output from target/criterion, memory sidecars from target/fluree-bench-mem,\n\
         and corpus/phase sidecars from target/fluree-bench-meta.\n\
         See BENCHMARKING.md (\"Baselines\")."
    );
}

/// Minimal `--flag value` / `--flag` parser. Unknown flags are an error so
/// typos can't silently change what a gate run measures.
fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    bool_flags: &[&str],
) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        if bool_flags.contains(&flag) {
            out.insert(flag.to_string(), "true".to_string());
            i += 1;
        } else if value_flags.contains(&flag) {
            let value = args
                .get(i + 1)
                .ok_or_else(|| format!("{flag} requires a value"))?;
            out.insert(flag.to_string(), value.clone());
            i += 2;
        } else {
            return Err(format!("unknown flag: {flag}"));
        }
    }
    Ok(out)
}

fn criterion_dir(flags: &BTreeMap<String, String>) -> PathBuf {
    flags
        .get("--criterion-dir")
        .map(PathBuf::from)
        .unwrap_or_else(default_criterion_dir)
}

fn mem_dir(flags: &BTreeMap<String, String>) -> PathBuf {
    flags
        .get("--mem-dir")
        .map(PathBuf::from)
        .unwrap_or_else(mem::sidecar_dir)
}

fn meta_dir(flags: &BTreeMap<String, String>) -> PathBuf {
    flags
        .get("--meta-dir")
        .map(PathBuf::from)
        .unwrap_or_else(meta::sidecar_dir)
}

/// `--host-class` is applied by setting the env var the library reads, so there
/// is exactly one resolution path for the class and the flag can't disagree
/// with `HostBlock::detect()`.
fn apply_host_class_override(flags: &BTreeMap<String, String>) {
    if let Some(class) = flags.get("--host-class") {
        std::env::set_var("FLUREE_BENCH_HOST_CLASS", class);
    }
}

fn git_short_sha() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let sha = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!sha.is_empty()).then_some(sha)
}

fn run_capture(args: &[String]) -> ExitCode {
    let flags = match parse_flags(
        args,
        &[
            "--label",
            "--out",
            "--criterion-dir",
            "--mem-dir",
            "--meta-dir",
            "--host-class",
        ],
        &["--clean-mem", "--clean-sidecars", "--accumulate"],
    ) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}\n");
            print_usage();
            return ExitCode::FAILURE;
        }
    };
    let Some(label) = flags.get("--label") else {
        eprintln!("capture requires --label <name>");
        return ExitCode::FAILURE;
    };
    let Some(out_path) = flags.get("--out").map(PathBuf::from) else {
        eprintln!("capture requires --out <file>");
        return ExitCode::FAILURE;
    };
    apply_host_class_override(&flags);

    let crit = criterion_dir(&flags);
    let mem_d = mem_dir(&flags);
    let meta_d = meta_dir(&flags);
    if !crit.is_dir() {
        eprintln!(
            "criterion dir {} not found — run `cargo bench ...` first",
            crit.display()
        );
        return ExitCode::FAILURE;
    }

    let mut file = capture(
        label,
        &crit,
        &mem_d,
        &meta_d,
        git_short_sha(),
        chrono::Utc::now().to_rfc3339(),
    );
    if file.entries.is_empty() {
        eprintln!("no scenarios found under {}", crit.display());
        return ExitCode::FAILURE;
    }

    if flags.contains_key("--accumulate") {
        match load_baseline(&out_path) {
            Ok(prev) => {
                if prev.host.class != file.host.class {
                    eprintln!(
                        "refusing to accumulate: existing baseline host_class '{}' != this run's '{}'",
                        prev.host.class, file.host.class
                    );
                    return ExitCode::from(EXIT_REFUSED);
                }
                file = accumulate(&prev, &file);
            }
            // First pass of an accumulation loop: nothing to merge yet.
            Err(_) if !out_path.exists() => {}
            Err(e) => {
                eprintln!("--accumulate could not read {}: {e}", out_path.display());
                return ExitCode::FAILURE;
            }
        }
    }

    if let Err(e) = save_baseline(&file, &out_path) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    let with_mem = file.entries.values().filter(|e| e.mem.is_some()).count();
    let with_corpus = file.entries.values().filter(|e| e.corpus.is_some()).count();
    let with_phases = file
        .entries
        .values()
        .filter(|e| !e.phases.is_empty())
        .count();
    let sampled = file
        .entries
        .values()
        .filter_map(|e| e.noise.as_ref())
        .map(|n| n.n)
        .min();
    println!(
        "captured {} scenarios ({with_mem} with memory, {with_corpus} with corpus, {with_phases} with phases) → {} [label={} sha={} profile={} scale={}]",
        file.entries.len(),
        out_path.display(),
        file.label,
        file.git_sha.as_deref().unwrap_or("?"),
        file.profile,
        file.scale,
    );
    println!("host: {}", file.host.describe());
    if let Some(n) = sampled {
        println!("noise samples: {n} (min across entries)");
    }
    // `--clean-mem` is the pre-meta spelling; both clear every sidecar dir so a
    // stale corpus block can't outlive the run that wrote it.
    if flags.contains_key("--clean-sidecars") || flags.contains_key("--clean-mem") {
        mem::clear_sidecars(&mem_d);
        meta::clear_sidecars(&meta_d);
    }
    ExitCode::SUCCESS
}

fn run_compare(args: &[String]) -> ExitCode {
    let flags = match parse_flags(
        args,
        &[
            "--baseline",
            "--criterion-dir",
            "--mem-dir",
            "--meta-dir",
            "--budget",
            "--only",
            "--host-class",
            "--share-drift-pp",
        ],
        &["--advisory", "--allow-host-mismatch"],
    ) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}\n");
            print_usage();
            return ExitCode::FAILURE;
        }
    };
    let Some(baseline_path) = flags.get("--baseline").map(PathBuf::from) else {
        eprintln!("compare requires --baseline <file>");
        return ExitCode::FAILURE;
    };
    apply_host_class_override(&flags);

    let share_drift_pp = match flags.get("--share-drift-pp").map(|s| s.parse::<f64>()) {
        Some(Ok(pp)) if pp >= 0.0 => pp,
        Some(_) => {
            eprintln!("--share-drift-pp requires a non-negative number");
            return ExitCode::FAILURE;
        }
        None => DEFAULT_SHARE_DRIFT_PP,
    };

    let baseline = match load_baseline(&baseline_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let budgets = match flags.get("--budget") {
        Some(p) => budget::load_from(PathBuf::from(p).as_path()),
        None => budget::load(),
    };
    let budgets = match budgets {
        Ok(b) => b,
        Err(e) => {
            eprintln!("budget load failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Collect current measurements the same way capture does.
    let crit = criterion_dir(&flags);
    let mem_d = mem_dir(&flags);
    let meta_d = meta_dir(&flags);
    let current = capture("current", &crit, &mem_d, &meta_d, None, String::new());

    let opts = CompareOptions {
        only: flags.get("--only").cloned(),
        host_mismatch: if flags.contains_key("--allow-host-mismatch") {
            HostMismatchPolicy::Advise
        } else {
            HostMismatchPolicy::Fatal
        },
        share_drift_pp,
        force_advisory: flags.contains_key("--advisory"),
    };
    let report = compare(&baseline, &current, &budgets, &opts);

    println!(
        "comparing against '{}' (sha={} captured={} profile={} scale={})",
        baseline.label,
        baseline.git_sha.as_deref().unwrap_or("?"),
        baseline.captured_at,
        baseline.profile,
        baseline.scale,
    );
    println!("  baseline host: {}", baseline.host.describe());
    println!("  current  host: {}", current.host.describe());
    if let Some(corpus) = &baseline.corpus {
        println!("  baseline corpus: {}", describe_corpus(corpus));
    }

    if report.is_fatal() {
        eprintln!("\nREFUSED to compare:");
        for f in &report.fatal {
            match &f.id {
                Some(id) => eprintln!("  [{}] {id}: {}", f.kind.as_str(), f.detail),
                None => eprintln!("  [{}] {}", f.kind.as_str(), f.detail),
            }
            eprintln!("      → {}", f.remedy);
        }
        eprintln!(
            "\n::error::bench-baseline refused {} comparison(s); current host_class={}",
            report.fatal.len(),
            host_class(),
        );
        return ExitCode::from(EXIT_REFUSED);
    }

    match &report.advisory_reason {
        Some(reason) => println!("\ntime + peak_mem: ADVISORY ({reason}); phase_share: ENFORCED"),
        None => println!("\ntime + peak_mem + phase_share: ENFORCED"),
    }
    println!();
    println!(
        "{:<62} {:>9} {:>14} {:>14} {:>9} {:>8}",
        "scenario [metric]", "status", "baseline", "observed", "Δ", "budget"
    );
    for c in &report.comparisons {
        let status = match c.status {
            CompareStatus::Improved => "improved",
            CompareStatus::Within => "ok",
            CompareStatus::Exceeded if c.enforced => "EXCEEDED",
            CompareStatus::Exceeded => "advisory",
        };
        let (baseline_s, observed_s, delta_s, budget_s) = render(c);
        println!(
            "{:<62} {:>9} {:>14} {:>14} {:>9} {:>8}",
            c.label(),
            status,
            baseline_s,
            observed_s,
            delta_s,
            budget_s,
        );
    }
    if !report.missing.is_empty() {
        println!(
            "\nnot rerun (in baseline, no current measurement): {}",
            report.missing.join(", ")
        );
    }
    if !report.new_scenarios.is_empty() {
        println!(
            "new scenarios (no baseline entry): {}",
            report.new_scenarios.join(", ")
        );
    }

    let advisories: Vec<_> = report.advisories().collect();
    if !advisories.is_empty() {
        let reason = report
            .advisory_reason
            .as_deref()
            .unwrap_or("advisory metrics");
        println!(
            "\n::warning::advisory regressions (not gating — {reason}): {}",
            advisories
                .iter()
                .map(|c| format!("{} {}", c.label(), render(c).2))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let improved = report
        .comparisons
        .iter()
        .filter(|c| c.status == CompareStatus::Improved)
        .count();
    let breaches: Vec<_> = report.breaches().collect();
    println!(
        "\n{} compared, {} improved, {} enforced-breach, {} advisory",
        report.comparisons.len(),
        improved,
        breaches.len(),
        advisories.len(),
    );
    if breaches.is_empty() {
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "FAIL: enforced budget exceeded for {} scenario metric(s)",
            breaches.len()
        );
        ExitCode::from(EXIT_BREACH)
    }
}

/// `(baseline, observed, delta, budget)` rendered in the metric's own units.
/// Share rows are percentages moving in percentage points; everything else is a
/// magnitude moving in percent.
fn render(c: &Comparison) -> (String, String, String, String) {
    match c.metric {
        Metric::PeakMem => (
            format_bytes(c.baseline),
            format_bytes(c.observed),
            format!("{:+.2}%", c.change_pct),
            format!("{:.1}%", c.budget_pct),
        ),
        Metric::PhaseShare => (
            format!("{:.1}%", c.baseline),
            format!("{:.1}%", c.observed),
            format!("{:+.1}pp", c.change_pct),
            format!("{:.1}pp", c.budget_pct),
        ),
        Metric::Time | Metric::PhaseTime => (
            format_ns(c.baseline),
            format_ns(c.observed),
            format!("{:+.2}%", c.change_pct),
            format!("{:.1}%", c.budget_pct),
        ),
    }
}

fn format_ns(ns: f64) -> String {
    if ns >= 1e9 {
        format!("{:.3} s", ns / 1e9)
    } else if ns >= 1e6 {
        format!("{:.3} ms", ns / 1e6)
    } else if ns >= 1e3 {
        format!("{:.3} µs", ns / 1e3)
    } else {
        format!("{ns:.0} ns")
    }
}

fn format_bytes(b: f64) -> String {
    if b >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GiB", b / (1024.0 * 1024.0 * 1024.0))
    } else if b >= 1024.0 * 1024.0 {
        format!("{:.2} MiB", b / (1024.0 * 1024.0))
    } else if b >= 1024.0 {
        format!("{:.2} KiB", b / 1024.0)
    } else {
        format!("{b:.0} B")
    }
}
