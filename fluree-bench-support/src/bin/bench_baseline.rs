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
//! # 3. ...make changes, rerun the benches...
//!
//! # 4. Compare (exit 1 on any budget breach):
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
    capture, compare, default_criterion_dir, load_baseline, save_baseline, BaselineEntry,
    CompareStatus,
};
use fluree_bench_support::budget;
use fluree_bench_support::mem;

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
         bench-baseline capture --label <name> --out <file> [--criterion-dir <dir>] [--mem-dir <dir>] [--clean-mem]\n  \
         bench-baseline compare --baseline <file> [--criterion-dir <dir>] [--mem-dir <dir>] [--budget <file>] [--only <substr>]\n\n\
         Run the benches first (`cargo bench ...`); this tool reads criterion's\n\
         output from target/criterion and memory sidecars from\n\
         target/fluree-bench-mem. See BENCHMARKING.md (\"Baselines\")."
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
        &["--label", "--out", "--criterion-dir", "--mem-dir"],
        &["--clean-mem"],
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

    let crit = criterion_dir(&flags);
    let mem_d = mem_dir(&flags);
    if !crit.is_dir() {
        eprintln!(
            "criterion dir {} not found — run `cargo bench ...` first",
            crit.display()
        );
        return ExitCode::FAILURE;
    }

    let file = capture(
        label,
        &crit,
        &mem_d,
        git_short_sha(),
        chrono::Utc::now().to_rfc3339(),
    );
    if file.entries.is_empty() {
        eprintln!("no scenarios found under {}", crit.display());
        return ExitCode::FAILURE;
    }
    if let Err(e) = save_baseline(&file, &out_path) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    let with_mem = file.entries.values().filter(|e| e.mem.is_some()).count();
    println!(
        "captured {} scenarios ({} with memory metrics) → {} [label={} sha={} profile={} scale={}]",
        file.entries.len(),
        with_mem,
        out_path.display(),
        file.label,
        file.git_sha.as_deref().unwrap_or("?"),
        file.profile,
        file.scale,
    );
    if flags.contains_key("--clean-mem") {
        mem::clear_sidecars(&mem_d);
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
            "--budget",
            "--only",
        ],
        &[],
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
    let current_file = capture("current", &crit, &mem_d, None, String::new());
    let current: BTreeMap<String, BaselineEntry> = current_file.entries;

    let report = compare(
        &baseline,
        &current,
        &budgets,
        flags.get("--only").map(String::as_str),
    );

    println!(
        "comparing against '{}' (sha={} captured={} profile={} scale={})",
        baseline.label,
        baseline.git_sha.as_deref().unwrap_or("?"),
        baseline.captured_at,
        baseline.profile,
        baseline.scale,
    );
    println!();
    println!(
        "{:<55} {:>9} {:>14} {:>14} {:>8} {:>8}",
        "scenario [metric]", "status", "baseline", "observed", "Δ%", "budget%"
    );
    for c in &report.comparisons {
        let status = match c.status {
            CompareStatus::Improved => "improved",
            CompareStatus::Within => "ok",
            CompareStatus::Exceeded => "EXCEEDED",
        };
        let (baseline_s, observed_s) = if c.metric == "time" {
            (format_ns(c.baseline), format_ns(c.observed))
        } else {
            (format_bytes(c.baseline), format_bytes(c.observed))
        };
        println!(
            "{:<55} {:>9} {:>14} {:>14} {:>+8.2} {:>8.1}",
            format!("{} [{}]", c.id, c.metric),
            status,
            baseline_s,
            observed_s,
            c.change_pct,
            c.budget_pct,
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

    let improved = report
        .comparisons
        .iter()
        .filter(|c| c.status == CompareStatus::Improved)
        .count();
    let breaches: Vec<_> = report.breaches().collect();
    println!(
        "\n{} compared, {} improved, {} breached",
        report.comparisons.len(),
        improved,
        breaches.len()
    );
    if breaches.is_empty() {
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "FAIL: budget exceeded for {} scenario metric(s)",
            breaches.len()
        );
        ExitCode::FAILURE
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
