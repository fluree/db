//! Baseline capture and comparison for the per-phase performance gate.
//!
//! `BENCHMARKING.md` ("Baselines: capture & compare") documents the
//! workflow; `docs/audit/2026-06-architecture-audit.md` Phase 0.0 documents
//! the protocol this implements. In short:
//!
//! 1. Run benches (`cargo bench ...`). Criterion writes per-scenario
//!    estimates under `target/criterion/<group>/<fn>/<param>/new/estimates.json`;
//!    memory-aware benches additionally write sidecars under
//!    `target/fluree-bench-mem/` (see [`crate::mem`]).
//! 2. `bench-baseline capture --label phase-1-pre --out bench-baselines/phase-1-pre.json`
//!    walks both outputs into one git-stamped [`BaselineFile`].
//! 3. After a change, rerun the benches and
//!    `bench-baseline compare --baseline bench-baselines/phase-1-pre.json`.
//!    Each scenario present in both runs is compared against
//!    `baseline × (1 + budget_pct/100)` using `regression-budget.json`
//!    budgets (same machinery as [`crate::budget::check`]); the command
//!    prints a table of regressions *and* improvements and exits nonzero
//!    on any budget breach.
//!
//! Scenario IDs are criterion's directory layout joined with `/` —
//! `"<group>/<function>/<parameter>"` — and benches put the scale in the
//! parameter slot (`BenchScale::as_str()`), so budget lookup can recover
//! `(bench, scale)` from the ID's first and last segments.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::budget::RegressionBudget;
use crate::mem::MemMetrics;
use crate::runtime::BenchScale;

pub const BASELINE_SCHEMA_VERSION: u32 = 1;

/// One scenario's captured measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineEntry {
    /// Criterion `mean.point_estimate`, nanoseconds.
    pub mean_ns: f64,
    /// Criterion `median.point_estimate`, nanoseconds.
    pub median_ns: f64,
    /// Allocation metrics, when the bench records them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem: Option<MemMetrics>,
}

/// A captured baseline: scenario ID → measurements, plus provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineFile {
    pub schema_version: u32,
    /// Human label, e.g. `phase-1-pre`.
    pub label: String,
    /// `git rev-parse --short HEAD` at capture time, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    /// RFC 3339 capture timestamp.
    pub captured_at: String,
    /// `FLUREE_BENCH_PROFILE` / `FLUREE_BENCH_SCALE` at capture time —
    /// informational; compare against a baseline captured under the same
    /// knobs.
    pub profile: String,
    pub scale: String,
    pub entries: BTreeMap<String, BaselineEntry>,
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

/// Walk a criterion output directory and collect
/// `scenario_id → (mean_ns, median_ns)` from every `*/new/estimates.json`.
///
/// Criterion's own `report/` directories contain no `new/estimates.json`,
/// so they fall out naturally.
pub fn collect_criterion_entries(criterion_dir: &Path) -> BTreeMap<String, BaselineEntry> {
    let mut out = BTreeMap::new();
    walk(criterion_dir, criterion_dir, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, BaselineEntry>) {
    let estimates = dir.join("new").join("estimates.json");
    if estimates.is_file() {
        if let Some(entry) = parse_estimates(&estimates) {
            let id = dir
                .strip_prefix(root)
                .unwrap_or(dir)
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if !id.is_empty() {
                out.insert(id, entry);
            }
        }
        // A scenario dir can't also contain nested scenario dirs.
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let path = e.path();
        if path.is_dir() {
            walk(root, &path, out);
        }
    }
}

fn parse_estimates(path: &Path) -> Option<BaselineEntry> {
    let raw = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let point = |stat: &str| -> Option<f64> { v.get(stat)?.get("point_estimate")?.as_f64() };
    Some(BaselineEntry {
        mean_ns: point("mean")?,
        median_ns: point("median").or_else(|| point("mean"))?,
        mem: None,
    })
}

/// Capture a baseline from criterion output plus memory sidecars.
pub fn capture(
    label: &str,
    criterion_dir: &Path,
    mem_dir: &Path,
    git_sha: Option<String>,
    captured_at: String,
) -> BaselineFile {
    let mut entries = collect_criterion_entries(criterion_dir);
    for (id, metrics) in crate::mem::read_all_sidecars(mem_dir) {
        entries
            .entry(id)
            .and_modify(|e| e.mem = Some(metrics))
            .or_insert(BaselineEntry {
                mean_ns: 0.0,
                median_ns: 0.0,
                mem: Some(metrics),
            });
    }
    BaselineFile {
        schema_version: BASELINE_SCHEMA_VERSION,
        label: label.to_string(),
        git_sha,
        captured_at,
        profile: std::env::var("FLUREE_BENCH_PROFILE").unwrap_or_else(|_| "quick".into()),
        scale: std::env::var("FLUREE_BENCH_SCALE").unwrap_or_else(|_| "small".into()),
        entries,
    }
}

pub fn load_baseline(path: &Path) -> Result<BaselineFile, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", path.display()))
}

pub fn save_baseline(file: &BaselineFile, path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(file).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write {}: {e}", path.display()))
}

// ---------------------------------------------------------------------------
// Compare
// ---------------------------------------------------------------------------

/// Outcome of one scenario × one metric comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareStatus {
    /// Faster / smaller than baseline by more than 1%.
    Improved,
    /// Within the budget envelope.
    Within,
    /// Over `baseline × (1 + budget_pct/100)` — fails the gate.
    Exceeded,
}

#[derive(Debug, Clone)]
pub struct Comparison {
    pub id: String,
    /// `"time"`, `"scenario_mem"` (preferred), or `"peak_mem"` (legacy
    /// absolute fallback).
    pub metric: &'static str,
    pub baseline: f64,
    pub observed: f64,
    pub change_pct: f64,
    pub budget_pct: f64,
    pub status: CompareStatus,
}

#[derive(Debug, Default)]
pub struct CompareReport {
    pub comparisons: Vec<Comparison>,
    /// Scenario IDs in the baseline with no current measurement (bench not
    /// rerun — informational, not a failure: PR-scoped runs compare subsets).
    pub missing: Vec<String>,
    /// Scenario IDs measured now but absent from the baseline.
    pub new_scenarios: Vec<String>,
}

impl CompareReport {
    pub fn breaches(&self) -> impl Iterator<Item = &Comparison> {
        self.comparisons
            .iter()
            .filter(|c| c.status == CompareStatus::Exceeded)
    }
    pub fn passed(&self) -> bool {
        self.breaches().next().is_none()
    }
}

/// Recover `(bench, scale)` from a scenario ID and find its budget.
///
/// The bench name is the criterion group (first segment); the scale is the
/// last segment when it parses as a [`BenchScale`]. The owning crate is
/// found by scanning the budget's crate map for the bench name — bench
/// names are unique across the workspace (the `workspace_reconcile` test
/// keys budgets by them).
fn budget_for_id(budget: &RegressionBudget, id: &str) -> f64 {
    let mut parts = id.split('/');
    let bench = parts.next().unwrap_or(id);
    let scale = id
        .rsplit('/')
        .next()
        .and_then(parse_scale)
        .unwrap_or(BenchScale::Small);
    for (crate_name, benches) in &budget.crates {
        if benches.contains_key(bench) {
            return budget.budget_pct(crate_name, bench, scale);
        }
    }
    budget.default_budget_pct
}

fn parse_scale(s: &str) -> Option<BenchScale> {
    match s {
        "tiny" => Some(BenchScale::Tiny),
        "small" => Some(BenchScale::Small),
        "medium" => Some(BenchScale::Medium),
        "large" => Some(BenchScale::Large),
        _ => None,
    }
}

fn classify(baseline: f64, observed: f64, budget_pct: f64) -> (f64, CompareStatus) {
    if baseline <= 0.0 {
        return (0.0, CompareStatus::Within);
    }
    let change_pct = (observed - baseline) / baseline * 100.0;
    let status = if observed > baseline * (1.0 + budget_pct / 100.0) {
        CompareStatus::Exceeded
    } else if change_pct < -1.0 {
        CompareStatus::Improved
    } else {
        CompareStatus::Within
    };
    (change_pct, status)
}

/// Compare current entries against a baseline under the given budgets.
///
/// `only` restricts the comparison to scenario IDs containing the given
/// substring (PR-scoped subset runs).
pub fn compare(
    baseline: &BaselineFile,
    current: &BTreeMap<String, BaselineEntry>,
    budget: &RegressionBudget,
    only: Option<&str>,
) -> CompareReport {
    let selected = |id: &str| only.is_none_or(|f| id.contains(f));
    let mut report = CompareReport::default();

    for (id, base) in &baseline.entries {
        if !selected(id) {
            continue;
        }
        let Some(cur) = current.get(id) else {
            report.missing.push(id.clone());
            continue;
        };
        let budget_pct = budget_for_id(budget, id);

        if base.mean_ns > 0.0 && cur.mean_ns > 0.0 {
            let (change_pct, status) = classify(base.mean_ns, cur.mean_ns, budget_pct);
            report.comparisons.push(Comparison {
                id: id.clone(),
                metric: "time",
                baseline: base.mean_ns,
                observed: cur.mean_ns,
                change_pct,
                budget_pct,
                status,
            });
        }
        if let (Some(bm), Some(cm)) = (base.mem, cur.mem) {
            // Prefer the scenario-attributable peak (robust against ambient
            // process-heap shifts between binaries); absolute peak is the
            // legacy fallback for baselines captured before the field
            // existed (serde default 0).
            let (metric, b_val, c_val) = if bm.scenario_peak_bytes > 0 && cm.scenario_peak_bytes > 0
            {
                (
                    "scenario_mem",
                    bm.scenario_peak_bytes as f64,
                    cm.scenario_peak_bytes as f64,
                )
            } else {
                ("peak_mem", bm.peak_bytes as f64, cm.peak_bytes as f64)
            };
            let (change_pct, status) = classify(b_val, c_val, budget_pct);
            report.comparisons.push(Comparison {
                id: id.clone(),
                metric,
                baseline: b_val,
                observed: c_val,
                change_pct,
                budget_pct,
                status,
            });
        }
    }

    for id in current.keys() {
        if selected(id) && !baseline.entries.contains_key(id) {
            report.new_scenarios.push(id.clone());
        }
    }
    report
}

/// Default locations, resolved from the workspace root.
pub fn default_criterion_dir() -> PathBuf {
    crate::budget::workspace_root()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("target")
        .join("criterion")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(mean: f64) -> BaselineEntry {
        BaselineEntry {
            mean_ns: mean,
            median_ns: mean,
            mem: None,
        }
    }

    fn baseline_with(entries: Vec<(&str, BaselineEntry)>) -> BaselineFile {
        BaselineFile {
            schema_version: BASELINE_SCHEMA_VERSION,
            label: "test".into(),
            git_sha: None,
            captured_at: "2026-01-01T00:00:00Z".into(),
            profile: "quick".into(),
            scale: "small".into(),
            entries: entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    #[test]
    fn collect_walks_criterion_layout() {
        let dir = std::env::temp_dir().join(format!(
            "fluree-baseline-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let scenario = dir.join("my_group").join("q1").join("small").join("new");
        std::fs::create_dir_all(&scenario).unwrap();
        std::fs::write(
            scenario.join("estimates.json"),
            r#"{"mean":{"point_estimate":1500.0},"median":{"point_estimate":1400.0}}"#,
        )
        .unwrap();
        // report dirs must be ignored (no new/estimates.json inside).
        std::fs::create_dir_all(dir.join("my_group").join("report")).unwrap();

        let entries = collect_criterion_entries(&dir);
        assert_eq!(entries.len(), 1);
        let e = &entries["my_group/q1/small"];
        assert_eq!(e.mean_ns, 1500.0);
        assert_eq!(e.median_ns, 1400.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compare_classifies_improvement_within_and_breach() {
        let base = baseline_with(vec![
            ("g/q1/small", entry(1000.0)),
            ("g/q2/small", entry(1000.0)),
            ("g/q3/small", entry(1000.0)),
        ]);
        let mut current = BTreeMap::new();
        current.insert("g/q1/small".to_string(), entry(900.0)); // -10%
        current.insert("g/q2/small".to_string(), entry(1030.0)); // +3%
        current.insert("g/q3/small".to_string(), entry(1100.0)); // +10%
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, None);
        assert!(!report.passed());
        let by_id = |id: &str| {
            report
                .comparisons
                .iter()
                .find(|c| c.id == id)
                .unwrap()
                .status
        };
        assert_eq!(by_id("g/q1/small"), CompareStatus::Improved);
        assert_eq!(by_id("g/q2/small"), CompareStatus::Within);
        assert_eq!(by_id("g/q3/small"), CompareStatus::Exceeded);
    }

    #[test]
    fn compare_subset_filter_and_missing() {
        let base = baseline_with(vec![
            ("g/q1/small", entry(1000.0)),
            ("h/q1/small", entry(1000.0)),
        ]);
        let mut current = BTreeMap::new();
        current.insert("g/q1/small".to_string(), entry(1000.0));
        let budget = RegressionBudget::empty(5.0);

        // Unfiltered: h/q1 is missing (informational, not a failure).
        let report = compare(&base, &current, &budget, None);
        assert!(report.passed());
        assert_eq!(report.missing, vec!["h/q1/small".to_string()]);

        // Filtered to g/: nothing missing.
        let report = compare(&base, &current, &budget, Some("g/"));
        assert!(report.missing.is_empty());
        assert_eq!(report.comparisons.len(), 1);
    }

    #[test]
    fn compare_includes_memory_metric() {
        let mem = |peak: u64| MemMetrics {
            peak_bytes: peak,
            scenario_peak_bytes: peak / 2,
            total_allocated_bytes: peak * 2,
        };
        let mut base_entry = entry(1000.0);
        base_entry.mem = Some(mem(1_000_000));
        let base = baseline_with(vec![("g/q1/small", base_entry)]);

        let mut cur_entry = entry(1000.0);
        cur_entry.mem = Some(mem(2_000_000)); // +100% peak
        let mut current = BTreeMap::new();
        current.insert("g/q1/small".to_string(), cur_entry);

        let budget = RegressionBudget::empty(5.0);
        let report = compare(&base, &current, &budget, None);
        assert!(!report.passed());
        let breach = report.breaches().next().unwrap();
        assert_eq!(breach.metric, "scenario_mem");
    }

    #[test]
    fn budget_lookup_recovers_bench_and_scale() {
        let mut budget = RegressionBudget::empty(5.0);
        budget
            .crates
            .entry("fluree-db-api".into())
            .or_default()
            .entry("query_overlay_matrix".into())
            .or_default()
            .insert("tiny".into(), 12.0);
        assert_eq!(
            budget_for_id(&budget, "query_overlay_matrix/count_base/tiny"),
            12.0
        );
        // Unknown bench falls back to default.
        assert_eq!(budget_for_id(&budget, "nope/x/small"), 5.0);
    }

    #[test]
    fn baseline_file_round_trips() {
        let base = baseline_with(vec![("g/q1/small", entry(42.0))]);
        let json = serde_json::to_string(&base).unwrap();
        let read: BaselineFile = serde_json::from_str(&json).unwrap();
        assert_eq!(read.entries["g/q1/small"].mean_ns, 42.0);
    }
}
