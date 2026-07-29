//! Baseline capture and comparison for the per-phase performance gate.
//!
//! `BENCHMARKING.md` ("Baselines: capture & compare") documents the
//! workflow; `docs/audit/2026-06-architecture-audit.md` Phase 0.0 documents
//! the protocol this implements. In short:
//!
//! 1. Run benches (`cargo bench ...`). Criterion writes per-scenario
//!    estimates under `target/criterion/<group>/<fn>/<param>/new/estimates.json`;
//!    memory-aware benches additionally write sidecars under
//!    `target/fluree-bench-mem/` (see [`crate::mem`]), and pipeline benches
//!    write corpus identity + phase timings under `target/fluree-bench-meta/`
//!    (see [`crate::meta`]).
//! 2. `bench-baseline capture --label phase-1-pre --out bench-baselines/phase-1-pre.json`
//!    walks all three outputs into one git-stamped [`BaselineFile`].
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
//!
//! ## What the gate refuses to do
//!
//! Two comparisons produce a number that *looks* like a result but means
//! nothing, so they are hard errors rather than budget breaches — [`compare`]
//! reports them in [`CompareReport::fatal`] and the bin exits `2`:
//!
//! - **Different host class.** Absolute nanoseconds and a tracking allocator's
//!   peak are both properties of the machine. `--allow-host-mismatch` downgrades
//!   this to the advisory posture (absolute metrics annotate, share drift still
//!   gates), which is what the per-PR CI job runs today.
//! - **Different corpus.** Comparing a run over one input against a baseline
//!   captured over another is the most expensive silent failure available here,
//!   because the output is indistinguishable from a real regression. There is no
//!   override.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::budget::RegressionBudget;
use crate::mem::MemMetrics;
use crate::meta::{short_hash, CorpusInfo, PhaseTiming, ScenarioMeta, DEFAULT_SHARE_DRIFT_PP};
use crate::runtime::BenchScale;

/// Bumped from 1 when the host block, corpus identity, phase shares, and noise
/// stats landed. Version 1 files still read (see [`RawBaselineFile`]); a file
/// claiming a *newer* version than this is a fatal compare error, because
/// silently ignoring fields we don't understand is how a gate stops gating.
pub const BASELINE_SCHEMA_VERSION: u32 = 2;

/// Samples required before [`NoiseStats`] widens a budget. Below this the
/// median and MAD are too unstable to be a noise estimate, so the entry is
/// gated on its declared budget alone.
pub const MIN_NOISE_SAMPLES: u32 = 5;

/// Host class recorded by schema-1 baselines that predate the host block and
/// carried no `runner_class` either.
const LEGACY_DEFAULT_HOST_CLASS: &str = "local";

/// How far a scenario's phase shares may sum from 100% before `compare` says so.
/// Generous, because this catches hand-assembled data, not rounding.
const SHARE_SUM_TOLERANCE_PP: f64 = 1.0;

// ---------------------------------------------------------------------------
// Host
// ---------------------------------------------------------------------------

/// The machine a baseline was captured on.
///
/// `class` is the load-bearing field: it names a *comparability set*, not a
/// machine. Two hosts share a class when their absolute numbers are meant to be
/// compared against each other — which is a claim only a human can make. The
/// derived default (`{os}-{arch}`) is deliberately coarse and deliberately not
/// a promise: an M1 and an M4 both derive `macos-aarch64` and are not
/// interchangeable. Any host whose numbers are meant to *gate* should set
/// `FLUREE_BENCH_HOST_CLASS` explicitly (`ci-ubuntu-latest`, `bench-m8gd`, …).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostBlock {
    /// Comparability set. Required — a baseline without one is unusable, so
    /// legacy files inherit `runner_class` or `"local"`.
    pub class: String,
    /// Host name — informational provenance.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// `std::env::consts::OS` at capture time.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub os: String,
    /// `std::env::consts::ARCH` at capture time.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub arch: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cpu_model: String,
    /// Physical cores, when the platform probe succeeds. Distinct from
    /// `logical_cores` because thread-scaling curves are read against physical
    /// cores and SMT siblings flatten them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_cores: Option<u32>,
}

impl HostBlock {
    /// Probe the current machine.
    pub fn detect() -> Self {
        Self {
            class: host_class(),
            name: host_name(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_model: cpu_model(),
            physical_cores: physical_cores(),
            logical_cores: std::thread::available_parallelism()
                .ok()
                .map(|n| n.get() as u32),
        }
    }

    /// A class-only block, for tests and for legacy files with no host data.
    pub fn of_class(class: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            name: String::new(),
            os: String::new(),
            arch: String::new(),
            cpu_model: String::new(),
            physical_cores: None,
            logical_cores: None,
        }
    }

    /// One-line description for report headers.
    pub fn describe(&self) -> String {
        let mut parts = vec![self.class.clone()];
        if !self.name.is_empty() {
            parts.push(self.name.clone());
        }
        let platform = match (self.os.as_str(), self.arch.as_str()) {
            ("", "") => String::new(),
            (os, "") => os.to_string(),
            ("", arch) => arch.to_string(),
            (os, arch) => format!("{os}/{arch}"),
        };
        if !platform.is_empty() {
            parts.push(platform);
        }
        if !self.cpu_model.is_empty() {
            parts.push(self.cpu_model.clone());
        }
        if let Some(cores) = self.physical_cores {
            parts.push(format!("{cores}c"));
        }
        parts.join(", ")
    }
}

/// The host class of the current environment.
///
/// `FLUREE_BENCH_HOST_CLASS` wins; `FLUREE_BENCH_RUNNER_CLASS` is accepted as
/// the pre-rename spelling so existing workflow files keep working; otherwise
/// it derives `{os}-{arch}`.
pub fn host_class() -> String {
    std::env::var("FLUREE_BENCH_HOST_CLASS")
        .or_else(|_| std::env::var("FLUREE_BENCH_RUNNER_CLASS"))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(derived_host_class)
}

/// `{os}-{arch}`, e.g. `macos-aarch64`, `linux-x86_64`.
pub fn derived_host_class() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn host_name() -> String {
    std::env::var("FLUREE_BENCH_HOST")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Best-effort CPU model string. Empty when the platform has no cheap probe —
/// this is provenance, never a gate input, so a miss costs nothing.
fn cpu_model() -> String {
    #[cfg(target_os = "macos")]
    {
        if let Some(s) = sysctl("machdep.cpu.brand_string") {
            return s;
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(raw) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in raw.lines() {
                if let Some((key, value)) = line.split_once(':') {
                    let key = key.trim();
                    if key == "model name" || key == "Model" {
                        return value.trim().to_string();
                    }
                }
            }
        }
    }
    String::new()
}

/// Physical (not logical) core count.
///
/// `available_parallelism()` reports logical CPUs and is also cgroup-clamped on
/// CI, so it is the wrong number for a thread-scaling curve. Probe the platform
/// first and fall back to it only so the field is rarely empty.
fn physical_cores() -> Option<u32> {
    #[cfg(target_os = "macos")]
    {
        if let Some(n) = sysctl("hw.physicalcpu").and_then(|s| s.parse().ok()) {
            return Some(n);
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(raw) = std::fs::read_to_string("/proc/cpuinfo") {
            // (physical id, core id) pairs — the standard way to collapse SMT
            // siblings. Single-socket VMs often omit both, hence the fallback.
            let mut seen = std::collections::BTreeSet::new();
            let mut physical_id = None;
            for line in raw.lines() {
                if let Some((key, value)) = line.split_once(':') {
                    match key.trim() {
                        "physical id" => physical_id = Some(value.trim().to_string()),
                        "core id" => {
                            seen.insert((physical_id.clone(), value.trim().to_string()));
                        }
                        _ => {}
                    }
                }
            }
            if !seen.is_empty() {
                return Some(seen.len() as u32);
            }
        }
    }
    std::thread::available_parallelism()
        .ok()
        .map(|n| n.get() as u32)
}

#[cfg(target_os = "macos")]
fn sysctl(key: &str) -> Option<String> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", key])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!s.is_empty()).then_some(s)
}

// ---------------------------------------------------------------------------
// Entries
// ---------------------------------------------------------------------------

/// Repeat-run statistics for one scenario's wall-clock.
///
/// Criterion's own confidence interval describes iterations *within* one run;
/// it says nothing about the run-to-run spread that a shared runner, a thermal
/// event, or a noisy neighbour produces. This is that spread, accumulated by
/// `bench-baseline capture --accumulate` across ≥ [`MIN_NOISE_SAMPLES`] separate
/// bench invocations, and it is what stops a gate from flagging noise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoiseStats {
    pub n: u32,
    pub median_ns: f64,
    /// Median absolute deviation — the robust spread estimator. Chosen over
    /// stddev because a single outlier run (a CI hiccup) moves stddev a lot and
    /// MAD barely at all, and outlier runs are the common case here.
    pub mad_ns: f64,
    /// Every accumulated sample, so `--accumulate` can recompute from scratch
    /// and a human can audit what the floor was derived from.
    #[serde(default)]
    pub samples_ns: Vec<f64>,
}

impl NoiseStats {
    /// Build from raw per-run means. `samples` need not be sorted.
    pub fn from_samples(mut samples: Vec<f64>) -> Self {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_ns = median_of_sorted(&samples);
        let mut deviations: Vec<f64> = samples.iter().map(|s| (s - median_ns).abs()).collect();
        deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Self {
            n: samples.len() as u32,
            median_ns,
            mad_ns: median_of_sorted(&deviations),
            samples_ns: samples,
        }
    }

    fn is_usable(&self) -> bool {
        self.n >= MIN_NOISE_SAMPLES && self.median_ns > 0.0
    }
}

fn median_of_sorted(sorted: &[f64]) -> f64 {
    match sorted.len() {
        0 => 0.0,
        n if n % 2 == 1 => sorted[n / 2],
        n => (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0,
    }
}

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
    /// Repeat-run spread, when the baseline was accumulated over several runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noise: Option<NoiseStats>,
    /// What this scenario read. Falls back to [`BaselineFile::corpus`] when
    /// absent, so a single-corpus run declares it once at file level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corpus: Option<CorpusInfo>,
    /// Per-phase timings and shares, when the bench records them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub phases: BTreeMap<String, PhaseTiming>,
}

impl BaselineEntry {
    /// A bare time measurement — the shape criterion alone produces.
    pub fn from_ns(mean_ns: f64, median_ns: f64) -> Self {
        Self {
            mean_ns,
            median_ns,
            mem: None,
            noise: None,
            corpus: None,
            phases: BTreeMap::new(),
        }
    }

    /// The wall-clock figure the gate compares: the repeat-run median when
    /// enough samples exist, criterion's single-run mean otherwise.
    pub fn effective_ns(&self) -> f64 {
        match &self.noise {
            Some(n) if n.is_usable() => n.median_ns,
            _ => self.mean_ns,
        }
    }

    /// The noise floor this entry contributes, as a percentage: `3 × MAD /
    /// median`. Zero when there aren't enough samples to estimate one.
    pub fn noise_floor_pct(&self) -> f64 {
        match &self.noise {
            Some(n) if n.is_usable() => 3.0 * n.mad_ns / n.median_ns * 100.0,
            _ => 0.0,
        }
    }
}

// ---------------------------------------------------------------------------
// File
// ---------------------------------------------------------------------------

/// A captured baseline: scenario ID → measurements, plus provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "RawBaselineFile")]
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
    /// The machine, and above all its comparability class. A class mismatch is
    /// a hard compare error unless the caller opts into the advisory posture.
    pub host: HostBlock,
    /// File-level corpus, inherited by every entry that declares none. Lets a
    /// single-corpus capture state its input once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corpus: Option<CorpusInfo>,
    pub entries: BTreeMap<String, BaselineEntry>,
}

impl BaselineFile {
    /// The corpus that governs `id`: the entry's own, else the file's.
    pub fn corpus_for(&self, id: &str) -> Option<&CorpusInfo> {
        self.entries
            .get(id)
            .and_then(|e| e.corpus.as_ref())
            .or(self.corpus.as_ref())
    }
}

/// Deserialization mirror, so schema-1 files keep loading.
///
/// Two shapes have to survive: `runner_class: "local"` + `host: "some-hostname"`
/// (schema 1), and the `host: { class, … }` block (schema 2). Serde field
/// attributes can't reconcile the two because the legacy class lives in a
/// *sibling* field, so the fixup happens in `From`.
#[derive(Deserialize)]
struct RawBaselineFile {
    #[serde(default = "one")]
    schema_version: u32,
    #[serde(default)]
    label: String,
    #[serde(default)]
    git_sha: Option<String>,
    #[serde(default)]
    captured_at: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    scale: String,
    /// Schema-1 spelling of `host.class`.
    #[serde(default)]
    runner_class: Option<String>,
    #[serde(default)]
    host: Option<HostField>,
    #[serde(default)]
    corpus: Option<CorpusInfo>,
    #[serde(default)]
    entries: BTreeMap<String, BaselineEntry>,
}

fn one() -> u32 {
    1
}

#[derive(Deserialize)]
#[serde(untagged)]
enum HostField {
    /// Schema 1: a bare host name.
    Name(String),
    Block(HostBlock),
}

impl From<RawBaselineFile> for BaselineFile {
    fn from(raw: RawBaselineFile) -> Self {
        let legacy_class = || {
            raw.runner_class
                .clone()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| LEGACY_DEFAULT_HOST_CLASS.to_string())
        };
        let host = match raw.host {
            Some(HostField::Block(mut block)) => {
                if block.class.trim().is_empty() {
                    block.class = legacy_class();
                }
                block
            }
            Some(HostField::Name(name)) => HostBlock {
                name,
                ..HostBlock::of_class(legacy_class())
            },
            None => HostBlock::of_class(legacy_class()),
        };
        BaselineFile {
            schema_version: raw.schema_version,
            label: raw.label,
            git_sha: raw.git_sha,
            captured_at: raw.captured_at,
            profile: raw.profile,
            scale: raw.scale,
            host,
            corpus: raw.corpus,
            entries: raw.entries,
        }
    }
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
    let mean = point("mean")?;
    Some(BaselineEntry::from_ns(
        mean,
        point("median").unwrap_or(mean),
    ))
}

/// Capture a baseline from criterion output plus memory and meta sidecars.
pub fn capture(
    label: &str,
    criterion_dir: &Path,
    mem_dir: &Path,
    meta_dir: &Path,
    git_sha: Option<String>,
    captured_at: String,
) -> BaselineFile {
    let mut entries = collect_criterion_entries(criterion_dir);
    for (id, metrics) in crate::mem::read_all_sidecars(mem_dir) {
        entries
            .entry(id)
            .or_insert_with(|| BaselineEntry::from_ns(0.0, 0.0))
            .mem = Some(metrics);
    }
    for (id, meta) in crate::meta::read_all_sidecars(meta_dir) {
        let ScenarioMeta { corpus, phases } = meta;
        let entry = entries
            .entry(id)
            .or_insert_with(|| BaselineEntry::from_ns(0.0, 0.0));
        entry.corpus = corpus;
        entry.phases = phases;
    }
    BaselineFile {
        schema_version: BASELINE_SCHEMA_VERSION,
        label: label.to_string(),
        git_sha,
        captured_at,
        profile: std::env::var("FLUREE_BENCH_PROFILE").unwrap_or_else(|_| "quick".into()),
        scale: std::env::var("FLUREE_BENCH_SCALE").unwrap_or_else(|_| "small".into()),
        host: HostBlock::detect(),
        corpus: None,
        entries,
    }
}

/// Fold a fresh capture into an existing baseline as one more sample.
///
/// This is how a baseline reaches [`MIN_NOISE_SAMPLES`]: run the benches, run
/// `capture --accumulate`, repeat. Each pass appends the fresh criterion mean to
/// the entry's sample vector and recomputes median + MAD, so the recorded floor
/// always matches the samples stored beside it.
///
/// Scenarios absent from `fresh` keep their prior stats untouched — a subset
/// rerun tops up only what it measured. Scenarios new in `fresh` start their own
/// sample series.
///
/// # Refusals
///
/// Accumulation is the one operation that *destroys* the evidence of a mismatch
/// rather than reporting it: fold a run from another machine, or over another
/// corpus, into the sample vector and the resulting median and MAD are a blend
/// of two populations that no later `compare` can unpick. The provenance it
/// would have been caught by is overwritten in the same breath, because the
/// accumulated file takes `fresh`'s label, sha, and host.
///
/// So both guards live here, in the library, rather than in the CLI that
/// happens to call it — a bench harness or script calling `accumulate` directly
/// gets the same protection:
///
/// - **host class** must match between `prev` and `fresh`.
/// - **corpus identity** must match for every scenario present in both,
///   resolved entry-then-file exactly as [`compare`] resolves it.
pub fn accumulate(prev: &BaselineFile, fresh: &BaselineFile) -> Result<BaselineFile, String> {
    if prev.host.class != fresh.host.class {
        return Err(format!(
            "refusing to accumulate across host classes: existing baseline is '{}' ({}), this run \
             is '{}' ({}). Samples from different machines cannot be pooled into one median/MAD.",
            prev.host.class,
            prev.host.describe(),
            fresh.host.class,
            fresh.host.describe(),
        ));
    }
    for id in prev.entries.keys() {
        if !fresh.entries.contains_key(id) {
            continue;
        }
        if let (Some(prev_corpus), Some(fresh_corpus)) = (prev.corpus_for(id), fresh.corpus_for(id))
        {
            if let Some(detail) = prev_corpus.identity_mismatch(fresh_corpus) {
                return Err(format!(
                    "refusing to accumulate across corpora for {id}: {detail}. Samples over \
                     different inputs cannot be pooled into one median/MAD."
                ));
            }
        }
    }

    let mut out = fresh.clone();
    for (id, prev_entry) in &prev.entries {
        let prior: Vec<f64> = prev_entry
            .noise
            .as_ref()
            .map(|n| n.samples_ns.clone())
            .unwrap_or_else(|| {
                // A pre-accumulation baseline holds one measurement and no
                // sample vector; seed the series with it so the first
                // --accumulate pass yields n=2 rather than discarding history.
                if prev_entry.mean_ns > 0.0 {
                    vec![prev_entry.mean_ns]
                } else {
                    Vec::new()
                }
            });
        match out.entries.get_mut(id) {
            Some(entry) if entry.mean_ns > 0.0 => {
                let mut samples = prior;
                samples.push(entry.mean_ns);
                entry.noise = Some(NoiseStats::from_samples(samples));
            }
            // Not rerun this pass: carry the previous entry forward whole.
            _ => {
                out.entries.insert(id.clone(), prev_entry.clone());
            }
        }
    }
    Ok(out)
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
    /// Faster / smaller than baseline by more than 1% (for share metrics: by
    /// more than the drift threshold).
    Improved,
    /// Within the budget envelope.
    Within,
    /// Over `baseline × (1 + budget_pct/100)` — fails the gate.
    Exceeded,
}

/// What a comparison measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Wall-clock, whole scenario.
    Time,
    /// Tracking-allocator peak, whole scenario.
    PeakMem,
    /// Wall-clock of one pipeline phase, absolute nanoseconds.
    PhaseTime,
    /// One phase's share of the scenario's measured phases, in percentage
    /// points. Dimensionless, so it survives a host change.
    PhaseShare,
}

impl Metric {
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Time => "time",
            Metric::PeakMem => "peak_mem",
            Metric::PhaseTime => "phase_time",
            Metric::PhaseShare => "phase_share",
        }
    }

    /// Whether this metric is comparable across host classes. Only shares are:
    /// they are ratios within a single run, so the machine cancels.
    pub fn is_host_portable(self) -> bool {
        matches!(self, Metric::PhaseShare)
    }
}

/// Whether a phase exists on both sides of a comparison, or only one.
///
/// A phase appearing or disappearing is a change in the *shape* of the
/// pipeline, and it is invisible to a same-name comparison. Tracking it is what
/// stops work relocating into an unmeasured phase from reading as an
/// improvement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PhasePresence {
    #[default]
    Both,
    /// New in this run — no baseline counterpart.
    AddedInCurrent,
    /// In the baseline but gone from this run.
    MissingFromCurrent,
}

#[derive(Debug, Clone)]
pub struct Comparison {
    pub id: String,
    pub metric: Metric,
    /// Phase name for the per-phase metrics.
    pub phase: Option<String>,
    /// Whether this phase existed on both sides. Always [`PhasePresence::Both`]
    /// for whole-scenario metrics.
    pub presence: PhasePresence,
    pub baseline: f64,
    pub observed: f64,
    /// Percent change for time/memory metrics; **percentage points** of
    /// movement for [`Metric::PhaseShare`].
    pub change_pct: f64,
    /// The threshold applied: percent for time/memory (budget, widened by any
    /// noise floor), percentage points for share drift.
    pub budget_pct: f64,
    pub status: CompareStatus,
    /// Whether an `Exceeded` here fails the gate. Absolute metrics enforce only
    /// when the host classes match; share drift always enforces.
    pub enforced: bool,
}

impl Comparison {
    /// `"<id> [<metric>]"`, with the phase folded in for per-phase rows and
    /// one-sided phases marked, since a bare name would hide the shape change.
    pub fn label(&self) -> String {
        let Some(phase) = &self.phase else {
            return format!("{} [{}]", self.id, self.metric.as_str());
        };
        let mark = match self.presence {
            PhasePresence::Both => "",
            PhasePresence::AddedInCurrent => " (NEW)",
            PhasePresence::MissingFromCurrent => " (VANISHED)",
        };
        format!("{}::{}{} [{}]", self.id, phase, mark, self.metric.as_str())
    }
}

/// A comparison the gate refuses to make.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchKind {
    /// Baseline and current ran on different comparability classes.
    HostClass,
    /// Baseline and current measured different inputs.
    Corpus,
    /// The baseline file is newer than this binary understands.
    SchemaVersion,
}

impl MismatchKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MismatchKind::HostClass => "host-class",
            MismatchKind::Corpus => "corpus",
            MismatchKind::SchemaVersion => "schema-version",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FatalMismatch {
    pub kind: MismatchKind,
    /// Scenario the mismatch applies to; `None` for file-level mismatches.
    pub id: Option<String>,
    pub detail: String,
    /// What the operator can do about it.
    pub remedy: String,
}

/// What [`compare`] does when the host classes differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostMismatchPolicy {
    /// Refuse to compare (default): absolute numbers from different machines
    /// are not evidence.
    #[default]
    Fatal,
    /// Compare anyway, with every absolute metric advisory. Share drift still
    /// gates, which is the whole reason this posture is useful.
    Advise,
}

#[derive(Debug, Clone)]
pub struct CompareOptions {
    /// Restrict to scenario IDs containing this substring (PR-scoped subsets).
    pub only: Option<String>,
    pub host_mismatch: HostMismatchPolicy,
    /// Percentage points of share movement that count as drift.
    pub share_drift_pp: f64,
    /// Force every absolute metric advisory regardless of host class.
    pub force_advisory: bool,
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self {
            only: None,
            host_mismatch: HostMismatchPolicy::default(),
            share_drift_pp: DEFAULT_SHARE_DRIFT_PP,
            force_advisory: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct CompareReport {
    pub comparisons: Vec<Comparison>,
    /// Scenario IDs in the baseline with no current measurement (bench not
    /// rerun — informational, not a failure: PR-scoped runs compare subsets).
    pub missing: Vec<String>,
    /// Scenario IDs measured now but absent from the baseline.
    pub new_scenarios: Vec<String>,
    /// Comparisons the gate refused to make. Any entry here fails the gate,
    /// distinctly from a budget breach.
    pub fatal: Vec<FatalMismatch>,
    /// Data that looks wrong but does not invalidate the comparison — reported,
    /// never gating. Today: phase shares that don't sum to 100%.
    pub anomalies: Vec<String>,
    /// Why absolute metrics are advisory, when they are.
    pub advisory_reason: Option<String>,
}

impl CompareReport {
    /// Enforced breaches — an `Exceeded` on an enforced metric. These fail the
    /// gate.
    pub fn breaches(&self) -> impl Iterator<Item = &Comparison> {
        self.comparisons
            .iter()
            .filter(|c| c.status == CompareStatus::Exceeded && c.enforced)
    }
    /// Advisory breaches — an `Exceeded` measured against a baseline from a
    /// different host class. Reported as annotations; they do NOT fail the
    /// gate.
    pub fn advisories(&self) -> impl Iterator<Item = &Comparison> {
        self.comparisons
            .iter()
            .filter(|c| c.status == CompareStatus::Exceeded && !c.enforced)
    }
    /// True when the gate refused to produce a verdict at all.
    pub fn is_fatal(&self) -> bool {
        !self.fatal.is_empty()
    }
    pub fn passed(&self) -> bool {
        !self.is_fatal() && self.breaches().next().is_none()
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

/// Share drift is measured in percentage points, and only *growth* gates.
///
/// Shares sum to 100, so every phase that grows forces others to shrink. Gating
/// both directions would fail a clean improvement twice — once for the phase
/// that got better and once for its complement. Growth is the regression;
/// shrink is reported as an improvement so the report still shows the whole
/// picture.
fn classify_share(baseline_pct: f64, observed_pct: f64, drift_pp: f64) -> (f64, CompareStatus) {
    let delta_pp = observed_pct - baseline_pct;
    let status = if delta_pp > drift_pp {
        CompareStatus::Exceeded
    } else if delta_pp < -drift_pp {
        CompareStatus::Improved
    } else {
        CompareStatus::Within
    };
    (delta_pp, status)
}

/// Emit the per-phase rows for one scenario, over the UNION of the two sides'
/// phase names.
///
/// Walking only the baseline's phases is how a regression hides: move work into
/// a phase the baseline never had and the old phase's share *falls*, which
/// reads as an improvement, while the new phase — carrying the cost — produces
/// no row at all. So a one-sided phase is gated on its share alone:
///
/// - **new in current**: breach when the phase claims more than the drift
///   threshold of the run. It has no baseline, so its whole share is drift.
/// - **gone from current**: breach when it *used* to claim more than the
///   threshold. Work that large did not evaporate; it moved somewhere, and the
///   somewhere is not being measured.
///
/// Neither one-sided case emits a `phase_time` row: with nothing on one side
/// there is no ratio to report, and a row reading "ok" against a zero baseline
/// would be worse than no row.
fn compare_phases(
    report: &mut CompareReport,
    id: &str,
    base: &BaselineEntry,
    cur: &BaselineEntry,
    time_budget_pct: f64,
    share_drift_pp: f64,
    advisory: bool,
) {
    let names: BTreeSet<&String> = base.phases.keys().chain(cur.phases.keys()).collect();
    for phase in names {
        let share_row =
            |presence, baseline: f64, observed: f64, delta_pp: f64, status| Comparison {
                id: id.to_string(),
                metric: Metric::PhaseShare,
                phase: Some(phase.clone()),
                presence,
                baseline,
                observed,
                change_pct: delta_pp,
                budget_pct: share_drift_pp,
                status,
                // Shares are ratios within one run, so the machine cancels. This is
                // the one signal a cross-host comparison keeps, and making it
                // advisory alongside the absolute metrics would throw away the
                // entire reason to compare across hosts at all.
                enforced: true,
            };

        match (base.phases.get(phase), cur.phases.get(phase)) {
            (Some(base_phase), Some(cur_phase)) => {
                let (change_pct, status) = classify(base_phase.ns, cur_phase.ns, time_budget_pct);
                report.comparisons.push(Comparison {
                    id: id.to_string(),
                    metric: Metric::PhaseTime,
                    phase: Some(phase.clone()),
                    presence: PhasePresence::Both,
                    baseline: base_phase.ns,
                    observed: cur_phase.ns,
                    change_pct,
                    budget_pct: time_budget_pct,
                    status,
                    enforced: !advisory,
                });
                let (delta_pp, status) =
                    classify_share(base_phase.share_pct, cur_phase.share_pct, share_drift_pp);
                report.comparisons.push(share_row(
                    PhasePresence::Both,
                    base_phase.share_pct,
                    cur_phase.share_pct,
                    delta_pp,
                    status,
                ));
            }
            (None, Some(cur_phase)) => {
                let status = if cur_phase.share_pct > share_drift_pp {
                    CompareStatus::Exceeded
                } else {
                    CompareStatus::Within
                };
                report.comparisons.push(share_row(
                    PhasePresence::AddedInCurrent,
                    0.0,
                    cur_phase.share_pct,
                    cur_phase.share_pct,
                    status,
                ));
            }
            (Some(base_phase), None) => {
                let status = if base_phase.share_pct > share_drift_pp {
                    CompareStatus::Exceeded
                } else {
                    CompareStatus::Within
                };
                report.comparisons.push(share_row(
                    PhasePresence::MissingFromCurrent,
                    base_phase.share_pct,
                    0.0,
                    -base_phase.share_pct,
                    status,
                ));
            }
            (None, None) => unreachable!("phase name came from one of the two maps"),
        }
    }
}

/// Shares are constructed to sum to 100 across a scenario's measured phases,
/// but that is a property of [`crate::meta::ScenarioMeta::with_phase_ns`], not
/// an invariant — the fields are public and a baseline file is untrusted input.
/// A sum far from 100 means the shares were hand-written or assembled by
/// something other than the constructor, which makes every drift number on that
/// scenario suspect. Report it; do not gate on it, since the drift comparison
/// is still internally consistent and failing here would block on a cosmetic
/// defect.
fn note_share_sum_anomaly(
    report: &mut CompareReport,
    id: &str,
    side: &str,
    phases: &BTreeMap<String, PhaseTiming>,
) {
    if phases.is_empty() {
        return;
    }
    let sum: f64 = phases.values().map(|p| p.share_pct).sum();
    if (sum - 100.0).abs() > SHARE_SUM_TOLERANCE_PP {
        report.anomalies.push(format!(
            "{id}: {side} phase shares sum to {sum:.1}%, not 100% — drift numbers for this \
             scenario are only as trustworthy as the shares they came from"
        ));
    }
}

/// Compare a current capture against a baseline under the given budgets.
///
/// Returns a report rather than a `Result`: a refusal to compare
/// ([`CompareReport::fatal`]) still carries the provenance a reader needs to fix
/// it, and the caller decides the exit code.
pub fn compare(
    baseline: &BaselineFile,
    current: &BaselineFile,
    budget: &RegressionBudget,
    opts: &CompareOptions,
) -> CompareReport {
    let mut report = CompareReport::default();

    if baseline.schema_version > BASELINE_SCHEMA_VERSION {
        report.fatal.push(FatalMismatch {
            kind: MismatchKind::SchemaVersion,
            id: None,
            detail: format!(
                "baseline schema_version {} is newer than this binary understands ({BASELINE_SCHEMA_VERSION})",
                baseline.schema_version
            ),
            remedy: "rebuild bench-baseline from the commit that wrote the baseline, or recapture"
                .to_string(),
        });
        return report;
    }

    let host_differs = baseline.host.class != current.host.class;
    if host_differs && opts.host_mismatch == HostMismatchPolicy::Fatal {
        report.fatal.push(FatalMismatch {
            kind: MismatchKind::HostClass,
            id: None,
            detail: format!(
                "baseline host_class '{}' ({}) != current '{}' ({})",
                baseline.host.class,
                baseline.host.describe(),
                current.host.class,
                current.host.describe(),
            ),
            // Deliberately does NOT suggest relabelling this host to the
            // baseline's class. Renaming a machine does not make its numbers
            // comparable — it converts this refusal into a silent, confident
            // ENFORCEMENT against a baseline from different hardware, which is
            // the exact failure the refusal exists to prevent.
            remedy: "pass --allow-host-mismatch to compare share drift only (absolute time and \
                     memory become advisory), or recapture the baseline on this host class"
                .to_string(),
        });
        return report;
    }

    let advisory = opts.force_advisory || host_differs;
    report.advisory_reason = if opts.force_advisory {
        Some("--advisory was passed".to_string())
    } else if host_differs {
        Some(format!(
            "baseline host_class '{}' != current '{}'",
            baseline.host.class, current.host.class
        ))
    } else {
        None
    };

    let selected = |id: &str| opts.only.as_deref().is_none_or(|f| id.contains(f));

    for (id, base) in &baseline.entries {
        if !selected(id) {
            continue;
        }
        let Some(cur) = current.entries.get(id) else {
            report.missing.push(id.clone());
            continue;
        };
        // A sidecar-only current entry (no fresh criterion estimate this run,
        // mean_ns == 0) means the bench was NOT rerun this session — its mem
        // sidecar is stale from a previous run (rust-cache persists target/).
        // Treat it as not-rerun rather than silently comparing stale memory
        // (review item 10). The CI compare step also clears the sidecar dir
        // before the run as belt-and-suspenders.
        if cur.mean_ns <= 0.0 {
            report.missing.push(id.clone());
            continue;
        }

        // Corpus identity gates everything else about this scenario: if the two
        // runs read different inputs, none of their numbers are comparable and
        // emitting rows for them would be worse than emitting nothing.
        if let (Some(base_corpus), Some(cur_corpus)) =
            (baseline.corpus_for(id), current.corpus_for(id))
        {
            if let Some(detail) = base_corpus.identity_mismatch(cur_corpus) {
                report.fatal.push(FatalMismatch {
                    kind: MismatchKind::Corpus,
                    id: Some(id.clone()),
                    detail,
                    remedy: "rerun against the corpus the baseline recorded, or recapture the \
                             baseline against the corpus you intend to gate on"
                        .to_string(),
                });
                continue;
            }
        }

        let declared_budget = budget_for_id(budget, id);
        // The noise floor is a WALL-CLOCK statement, and it comes from the
        // BASELINE only. Two halves, both load-bearing:
        //
        // - Wall-clock only. A tracking allocator's peak is near-deterministic
        //   for a given workload; widening the memory budget by a *timing* MAD
        //   would let a real allocation regression through on the strength of a
        //   noisy clock. `time_budget_pct` therefore never reaches peak_mem.
        // - Baseline only. The floor says "this is how much this scenario
        //   moves on a quiet machine". Taking the current run's floor too lets
        //   an erratic run widen its own budget — the worse the run behaves,
        //   the more it is forgiven, which inverts the gate.
        //
        // Within those bounds the floor only ever *widens*: a budget tighter
        // than the measured noise can only produce false alarms.
        let time_budget_pct = declared_budget.max(base.noise_floor_pct());

        if base.effective_ns() > 0.0 {
            let (change_pct, status) =
                classify(base.effective_ns(), cur.effective_ns(), time_budget_pct);
            report.comparisons.push(Comparison {
                id: id.clone(),
                metric: Metric::Time,
                phase: None,
                presence: PhasePresence::Both,
                baseline: base.effective_ns(),
                observed: cur.effective_ns(),
                change_pct,
                budget_pct: time_budget_pct,
                status,
                enforced: !advisory,
            });
        }
        if let (Some(bm), Some(cm)) = (base.mem, cur.mem) {
            let (change_pct, status) =
                classify(bm.peak_bytes as f64, cm.peak_bytes as f64, declared_budget);
            report.comparisons.push(Comparison {
                id: id.clone(),
                metric: Metric::PeakMem,
                phase: None,
                presence: PhasePresence::Both,
                baseline: bm.peak_bytes as f64,
                observed: cm.peak_bytes as f64,
                change_pct,
                // Declared budget, NOT the timing-derived floor.
                budget_pct: declared_budget,
                status,
                // Same host rule as time: a tracking allocator's peak is no more
                // portable across machine classes than wall clock is.
                enforced: !advisory,
            });
        }

        compare_phases(
            &mut report,
            id,
            base,
            cur,
            time_budget_pct,
            opts.share_drift_pp,
            advisory,
        );
        note_share_sum_anomaly(&mut report, id, "baseline", &base.phases);
        note_share_sum_anomaly(&mut report, id, "current", &cur.phases);
    }

    for id in current.entries.keys() {
        if selected(id) && !baseline.entries.contains_key(id) {
            report.new_scenarios.push(id.clone());
        }
    }
    report
}

/// One-line summary of a corpus, for report headers.
pub fn describe_corpus(corpus: &CorpusInfo) -> String {
    let mut s = format!(
        "{} sha256:{} {} bytes",
        corpus.name,
        short_hash(&corpus.sha256),
        corpus.byte_len
    );
    if corpus.element_count > 0 {
        s.push_str(&format!(", {} elements", corpus.element_count));
    }
    s.push_str(&format!(
        ", {}→{}, {} thread(s)",
        corpus.input_syntax, corpus.output_syntax, corpus.thread_count
    ));
    s
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
        BaselineEntry::from_ns(mean, mean)
    }

    fn mem_entry(mean: f64, peak: u64) -> BaselineEntry {
        BaselineEntry {
            mem: Some(MemMetrics {
                peak_bytes: peak,
                total_allocated_bytes: peak * 2,
            }),
            ..entry(mean)
        }
    }

    fn file_of(class: &str, entries: Vec<(&str, BaselineEntry)>) -> BaselineFile {
        BaselineFile {
            schema_version: BASELINE_SCHEMA_VERSION,
            label: "test".into(),
            git_sha: None,
            captured_at: "2026-01-01T00:00:00Z".into(),
            profile: "quick".into(),
            scale: "small".into(),
            host: HostBlock::of_class(class),
            corpus: None,
            entries: entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    fn baseline_with(entries: Vec<(&str, BaselineEntry)>) -> BaselineFile {
        file_of("local", entries)
    }

    /// Same host class as `baseline_with`, so absolute metrics enforce.
    fn current_with(entries: Vec<(&str, BaselineEntry)>) -> BaselineFile {
        file_of("local", entries)
    }

    fn corpus(bytes: &[u8]) -> CorpusInfo {
        CorpusInfo::from_bytes("dblp.ttl", bytes, "turtle", "nquads", 8)
    }

    fn opts() -> CompareOptions {
        CompareOptions::default()
    }

    fn allow_mismatch() -> CompareOptions {
        CompareOptions {
            host_mismatch: HostMismatchPolicy::Advise,
            ..CompareOptions::default()
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
        let current = current_with(vec![
            ("g/q1/small", entry(900.0)),  // -10%
            ("g/q2/small", entry(1030.0)), // +3%
            ("g/q3/small", entry(1100.0)), // +10%
        ]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
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
        let current = current_with(vec![("g/q1/small", entry(1000.0))]);
        let budget = RegressionBudget::empty(5.0);

        // Unfiltered: h/q1 is missing (informational, not a failure).
        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed());
        assert_eq!(report.missing, vec!["h/q1/small".to_string()]);

        // Filtered to g/: nothing missing.
        let filtered = CompareOptions {
            only: Some("g/".into()),
            ..opts()
        };
        let report = compare(&base, &current, &budget, &filtered);
        assert!(report.missing.is_empty());
        assert_eq!(report.comparisons.len(), 1);
    }

    #[test]
    fn compare_includes_memory_metric() {
        let base = baseline_with(vec![("g/q1/small", mem_entry(1000.0, 1_000_000))]);
        let current = current_with(vec![("g/q1/small", mem_entry(1000.0, 2_000_000))]);
        let budget = RegressionBudget::empty(5.0);
        let report = compare(&base, &current, &budget, &opts());
        assert!(!report.passed());
        let breach = report.breaches().next().unwrap();
        assert_eq!(breach.metric, Metric::PeakMem);
    }

    // -----------------------------------------------------------------------
    // Host class
    // -----------------------------------------------------------------------

    #[test]
    fn host_class_mismatch_is_fatal_by_default() {
        let base = file_of("bench-m8gd", vec![("g/q1/small", entry(1000.0))]);
        let current = file_of("macos-aarch64", vec![("g/q1/small", entry(1000.0))]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.is_fatal());
        assert!(!report.passed());
        assert_eq!(report.fatal[0].kind, MismatchKind::HostClass);
        // No verdict at all — not even a clean one.
        assert!(report.comparisons.is_empty());
        assert!(
            report.fatal[0].detail.contains("bench-m8gd")
                && report.fatal[0].detail.contains("macos-aarch64"),
            "message must name both classes: {}",
            report.fatal[0].detail
        );
        assert!(report.fatal[0].remedy.contains("--allow-host-mismatch"));
    }

    #[test]
    fn allow_host_mismatch_makes_absolute_metrics_advisory() {
        let base = file_of(
            "bench-m8gd",
            vec![("g/q1/small", mem_entry(1000.0, 1_000_000))],
        );
        let current = file_of(
            "macos-aarch64",
            vec![("g/q1/small", mem_entry(5000.0, 9_000_000))],
        );
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &allow_mismatch());
        assert!(!report.is_fatal());
        assert!(
            report.passed(),
            "cross-host absolute breaches must not gate"
        );
        assert_eq!(report.advisories().count(), 2);
        assert!(report.advisory_reason.unwrap().contains("bench-m8gd"));
    }

    #[test]
    fn matching_host_class_enforces_absolute_metrics() {
        let base = file_of("ci-ubuntu-latest", vec![("g/q1/small", entry(1000.0))]);
        let current = file_of("ci-ubuntu-latest", vec![("g/q1/small", entry(2000.0))]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(!report.passed());
        assert_eq!(report.breaches().count(), 1);
        assert!(report.advisory_reason.is_none());
    }

    #[test]
    fn force_advisory_downgrades_even_on_matching_hosts() {
        let base = baseline_with(vec![("g/q1/small", entry(1000.0))]);
        let current = current_with(vec![("g/q1/small", entry(2000.0))]);
        let budget = RegressionBudget::empty(5.0);
        let forced = CompareOptions {
            force_advisory: true,
            ..opts()
        };

        let report = compare(&base, &current, &budget, &forced);
        assert!(report.passed());
        assert_eq!(report.advisories().count(), 1);
    }

    // -----------------------------------------------------------------------
    // Corpus identity
    // -----------------------------------------------------------------------

    #[test]
    fn corpus_hash_mismatch_is_fatal_and_drops_the_scenario() {
        let mut base_entry = entry(1000.0);
        base_entry.corpus = Some(corpus(b"one"));
        let mut cur_entry = entry(1000.0);
        cur_entry.corpus = Some(corpus(b"two"));

        let base = baseline_with(vec![("g/q1/small", base_entry)]);
        let current = current_with(vec![("g/q1/small", cur_entry)]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.is_fatal());
        assert!(!report.passed());
        assert_eq!(report.fatal[0].kind, MismatchKind::Corpus);
        assert_eq!(report.fatal[0].id.as_deref(), Some("g/q1/small"));
        assert!(report.fatal[0].detail.starts_with("sha256:"));
        assert!(
            report.comparisons.is_empty(),
            "a scenario with a corpus mismatch must produce no rows"
        );
    }

    #[test]
    fn corpus_mismatch_has_no_override() {
        // Unlike a host mismatch, --allow-host-mismatch does not rescue this:
        // there is no posture in which comparing different inputs is meaningful.
        let mut base_entry = entry(1000.0);
        base_entry.corpus = Some(corpus(b"one"));
        let mut cur_entry = entry(1000.0);
        cur_entry.corpus = Some(corpus(b"two"));
        let base = baseline_with(vec![("g/q1/small", base_entry)]);
        let current = current_with(vec![("g/q1/small", cur_entry)]);
        let budget = RegressionBudget::empty(5.0);

        let forced = CompareOptions {
            force_advisory: true,
            ..allow_mismatch()
        };
        assert!(compare(&base, &current, &budget, &forced).is_fatal());
    }

    #[test]
    fn matching_corpus_compares_normally() {
        let mut base_entry = entry(1000.0);
        base_entry.corpus = Some(corpus(b"same"));
        let mut cur_entry = entry(1030.0);
        cur_entry.corpus = Some(corpus(b"same"));

        let base = baseline_with(vec![("g/q1/small", base_entry)]);
        let current = current_with(vec![("g/q1/small", cur_entry)]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed());
        assert_eq!(report.comparisons.len(), 1);
    }

    #[test]
    fn file_level_corpus_is_inherited_by_entries() {
        let mut base = baseline_with(vec![("g/q1/small", entry(1000.0))]);
        base.corpus = Some(corpus(b"one"));
        let mut current = current_with(vec![("g/q1/small", entry(1000.0))]);
        current.corpus = Some(corpus(b"two"));
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.is_fatal(), "file-level corpus must gate too");
        assert_eq!(report.fatal[0].kind, MismatchKind::Corpus);
    }

    #[test]
    fn entry_corpus_overrides_file_corpus() {
        let mut base = baseline_with(vec![("g/q1/small", {
            let mut e = entry(1000.0);
            e.corpus = Some(corpus(b"same"));
            e
        })]);
        base.corpus = Some(corpus(b"file-level-differs"));
        let mut current = current_with(vec![("g/q1/small", {
            let mut e = entry(1000.0);
            e.corpus = Some(corpus(b"same"));
            e
        })]);
        current.corpus = Some(corpus(b"and-differs-here-too"));
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(
            report.passed(),
            "entry-level corpus agreement must win over file-level disagreement"
        );
    }

    #[test]
    fn one_sided_corpus_declaration_does_not_gate() {
        // An old baseline with no corpus block compared against a run that has
        // one: nothing to check, so compare normally rather than inventing a
        // mismatch.
        let base = baseline_with(vec![("g/q1/small", entry(1000.0))]);
        let current = current_with(vec![("g/q1/small", {
            let mut e = entry(1000.0);
            e.corpus = Some(corpus(b"one"));
            e
        })]);
        let budget = RegressionBudget::empty(5.0);
        assert!(compare(&base, &current, &budget, &opts()).passed());
    }

    // -----------------------------------------------------------------------
    // Phases and share drift
    // -----------------------------------------------------------------------

    fn phased(mean: f64, phases: &[(&str, f64)]) -> BaselineEntry {
        BaselineEntry {
            phases: ScenarioMeta::new()
                .with_phase_ns(phases.iter().map(|(n, ns)| (n.to_string(), *ns)))
                .phases,
            ..entry(mean)
        }
    }

    #[test]
    fn share_drift_gates_even_across_host_classes() {
        // Whole-scenario time is identical; the work moved from write to parse.
        let base = file_of(
            "bench-m8gd",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "macos-aarch64",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 80.0), ("write", 20.0)]),
            )],
        );
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &allow_mismatch());
        let share_breaches: Vec<_> = report
            .breaches()
            .filter(|c| c.metric == Metric::PhaseShare)
            .collect();
        assert_eq!(share_breaches.len(), 1, "only the grown phase gates");
        assert_eq!(share_breaches[0].phase.as_deref(), Some("parse"));
        assert!((share_breaches[0].change_pct - 20.0).abs() < 1e-9, "20pp");
        assert!(!report.passed(), "share drift must fail the gate");
    }

    #[test]
    fn share_shrink_is_reported_as_improvement_not_a_breach() {
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 80.0), ("write", 20.0)]),
            )],
        );
        let budget = RegressionBudget::empty(5.0);
        let report = compare(&base, &current, &budget, &opts());

        let write = report
            .comparisons
            .iter()
            .find(|c| c.metric == Metric::PhaseShare && c.phase.as_deref() == Some("write"))
            .unwrap();
        assert_eq!(write.status, CompareStatus::Improved);
    }

    #[test]
    fn share_within_threshold_does_not_gate() {
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 62.0), ("write", 38.0)]),
            )],
        );
        let budget = RegressionBudget::empty(5.0);
        assert!(compare(&base, &current, &budget, &opts()).passed());
    }

    #[test]
    fn share_drift_threshold_is_configurable() {
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 63.0), ("write", 37.0)]),
            )],
        );
        let budget = RegressionBudget::empty(500.0);
        let tight = CompareOptions {
            share_drift_pp: 1.0,
            ..opts()
        };
        assert!(!compare(&base, &current, &budget, &tight).passed());
        assert!(compare(&base, &current, &budget, &opts()).passed());
    }

    #[test]
    fn absolute_phase_time_is_advisory_across_host_classes() {
        // Same shares, 10x the absolute phase cost: a machine change, not a
        // regression. The share rows stay clean and the phase_time rows only
        // annotate.
        let base = file_of(
            "bench-m8gd",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "macos-aarch64",
            vec![(
                "g/q1/small",
                phased(10000.0, &[("parse", 600.0), ("write", 400.0)]),
            )],
        );
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &allow_mismatch());
        assert!(report.passed());
        assert!(report
            .advisories()
            .any(|c| c.metric == Metric::PhaseTime && c.phase.as_deref() == Some("parse")));
        assert!(report
            .comparisons
            .iter()
            .filter(|c| c.metric == Metric::PhaseShare)
            .all(|c| c.status == CompareStatus::Within));
    }

    /// reproA, from review-prbench. The defect this pins: walking only the
    /// baseline's phases made a doubling regression report as an IMPROVEMENT.
    ///
    /// Baseline is one 100%-share `parse` phase. The current run is 2x slower
    /// overall and has moved 75% of its work into a brand-new `validate` phase.
    /// Before the fix, `parse` fell 100% → 25% and printed "improved", while
    /// `validate` — carrying the regression — produced no row at all.
    #[test]
    fn work_moved_into_a_new_phase_is_a_breach_not_an_improvement() {
        let base = file_of(
            "local",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(2000.0, &[("parse", 50.0), ("validate", 150.0)]),
            )],
        );
        // Wide budget so only the phase rules can fail this.
        let budget = RegressionBudget::empty(500.0);
        let report = compare(&base, &current, &budget, &opts());

        let validate = report
            .comparisons
            .iter()
            .find(|c| c.phase.as_deref() == Some("validate"))
            .expect("a new phase must produce a row");
        assert_eq!(validate.metric, Metric::PhaseShare);
        assert_eq!(validate.presence, PhasePresence::AddedInCurrent);
        assert_eq!(validate.status, CompareStatus::Exceeded);
        assert!(validate.enforced);
        assert_eq!(validate.baseline, 0.0);
        assert_eq!(validate.observed, 75.0);
        assert!(validate.label().contains("(NEW)"), "{}", validate.label());
        assert!(!report.passed(), "the gate must fail on the new phase");
    }

    #[test]
    fn a_small_new_phase_within_the_drift_threshold_does_not_gate() {
        // Splitting 2% of the work into its own phase is bookkeeping, not a
        // regression — the threshold is what separates the two.
        let base = file_of(
            "local",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 98.0), ("flush", 2.0)]),
            )],
        );
        let budget = RegressionBudget::empty(500.0);
        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed());
        let flush = report
            .comparisons
            .iter()
            .find(|c| c.phase.as_deref() == Some("flush"))
            .unwrap();
        assert_eq!(flush.presence, PhasePresence::AddedInCurrent);
        assert_eq!(flush.status, CompareStatus::Within);
    }

    #[test]
    fn a_phase_vanishing_from_the_current_run_is_a_breach() {
        // The mirror hazard: a phase carrying 40% of the baseline is simply
        // absent now. Under growth-only share gating its -40pp would read as an
        // improvement, but work that large moved somewhere unmeasured.
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 60.0), ("write", 40.0)]),
            )],
        );
        let current = file_of(
            "local",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let budget = RegressionBudget::empty(500.0);
        let report = compare(&base, &current, &budget, &opts());

        let write = report
            .comparisons
            .iter()
            .find(|c| c.phase.as_deref() == Some("write"))
            .expect("a vanished phase must produce a row");
        assert_eq!(write.presence, PhasePresence::MissingFromCurrent);
        assert_eq!(write.status, CompareStatus::Exceeded);
        assert_eq!(write.observed, 0.0);
        assert!((write.change_pct + 40.0).abs() < 1e-9, "-40pp");
        assert!(write.label().contains("(VANISHED)"));
        assert!(!report.passed());
    }

    #[test]
    fn a_tiny_vanished_phase_does_not_gate() {
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 98.0), ("flush", 2.0)]),
            )],
        );
        let current = file_of(
            "local",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let budget = RegressionBudget::empty(500.0);
        assert!(compare(&base, &current, &budget, &opts()).passed());
    }

    #[test]
    fn one_sided_phases_emit_no_phase_time_row() {
        // There is no ratio to report against a missing side, and a row reading
        // "ok" versus a zero baseline would be worse than no row.
        let base = file_of(
            "local",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let current = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 50.0), ("lex", 50.0)]),
            )],
        );
        let budget = RegressionBudget::empty(500.0);
        let report = compare(&base, &current, &budget, &opts());
        assert!(!report
            .comparisons
            .iter()
            .any(|c| c.phase.as_deref() == Some("lex") && c.metric == Metric::PhaseTime));
    }

    #[test]
    fn new_phase_gating_survives_a_cross_host_comparison() {
        // The whole point of the share metric: absolute numbers are advisory
        // across hosts, but a pipeline reshaping still has to fail.
        let base = file_of(
            "bench-m8gd",
            vec![("g/q1/small", phased(1000.0, &[("parse", 100.0)]))],
        );
        let current = file_of(
            "macos-aarch64",
            vec![(
                "g/q1/small",
                phased(9000.0, &[("parse", 50.0), ("validate", 150.0)]),
            )],
        );
        let budget = RegressionBudget::empty(500.0);
        let report = compare(&base, &current, &budget, &allow_mismatch());
        assert!(!report.passed());
        assert!(report
            .breaches()
            .any(|c| c.phase.as_deref() == Some("validate")));
    }

    // -----------------------------------------------------------------------
    // Noise floor
    // -----------------------------------------------------------------------

    fn noisy(samples: &[f64]) -> BaselineEntry {
        let stats = NoiseStats::from_samples(samples.to_vec());
        BaselineEntry {
            noise: Some(stats.clone()),
            ..entry(stats.median_ns)
        }
    }

    #[test]
    fn mad_and_median_are_computed_from_samples() {
        let stats = NoiseStats::from_samples(vec![100.0, 102.0, 98.0, 101.0, 99.0]);
        assert_eq!(stats.n, 5);
        assert_eq!(stats.median_ns, 100.0);
        // deviations 0,1,1,2,2 -> median 1
        assert_eq!(stats.mad_ns, 1.0);
        assert_eq!(stats.samples_ns, vec![98.0, 99.0, 100.0, 101.0, 102.0]);
    }

    #[test]
    fn even_sample_count_medians_the_middle_pair() {
        let stats = NoiseStats::from_samples(vec![10.0, 20.0, 30.0, 40.0]);
        assert_eq!(stats.median_ns, 25.0);
    }

    /// Deviations from the median of 1000 are 20/10/0/10/20, so MAD = 10 and
    /// the floor is `3 × 10 / 1000 = 3%`.
    const MAD_10_SAMPLES: [f64; 5] = [980.0, 990.0, 1000.0, 1010.0, 1020.0];

    #[test]
    fn noise_floor_widens_a_tight_budget() {
        let base = file_of("local", vec![("g/q1/small", noisy(&MAD_10_SAMPLES))]);
        // +2.5%: inside the 3% noise floor, outside the declared 1% budget.
        let current = current_with(vec![("g/q1/small", entry(1025.0))]);
        let budget = RegressionBudget::empty(1.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed(), "2.5% move must not flag under a 3% floor");
        let c = &report.comparisons[0];
        assert_eq!(c.budget_pct, 3.0, "budget widened to the floor: {c:?}");
    }

    #[test]
    fn noise_floor_does_not_hide_a_move_beyond_it() {
        let base = file_of("local", vec![("g/q1/small", noisy(&MAD_10_SAMPLES))]);
        let current = current_with(vec![("g/q1/small", entry(1200.0))]); // +20%
        let budget = RegressionBudget::empty(1.0);
        assert!(!compare(&base, &current, &budget, &opts()).passed());
    }

    /// Review decision: the floor is BASELINE-ONLY.
    ///
    /// Taking the wider of the two floors — which this originally did — lets an
    /// erratic run widen its own budget, so the worse a run behaves the more it
    /// is forgiven. That inverts the gate. The baseline is the controlled
    /// measurement; it alone gets to say how much this scenario moves.
    #[test]
    fn a_noisy_current_run_does_not_widen_the_budget() {
        let base = file_of("local", vec![("g/q1/small", entry(1000.0))]);
        let current = current_with(vec![("g/q1/small", noisy(&MAD_10_SAMPLES))]);
        let budget = RegressionBudget::empty(1.0);
        assert_eq!(
            compare(&base, &current, &budget, &opts()).comparisons[0].budget_pct,
            1.0,
            "current-side noise must not widen the declared budget"
        );
    }

    /// reproC, from review-prbench. A wall-clock MAD was widening the MEMORY
    /// budget: baseline MAD 100 on a median of 1000 gave a 30% floor, and a
    /// +25% peak_bytes regression sailed through a 5% memory budget.
    ///
    /// A tracking allocator's peak is near-deterministic for a given workload,
    /// so a noisy clock is no reason at all to forgive it.
    #[test]
    fn timing_noise_never_widens_the_memory_budget() {
        let mut base_entry = mem_entry(1000.0, 1_000_000);
        base_entry.noise = Some(NoiseStats::from_samples(vec![
            800.0, 900.0, 1000.0, 1100.0, 1200.0,
        ]));
        assert_eq!(base_entry.noise_floor_pct(), 30.0, "MAD 100 / median 1000");

        let base = file_of("local", vec![("g/q1/small", base_entry)]);
        // Same wall clock, +25% peak memory.
        let current = current_with(vec![("g/q1/small", mem_entry(1000.0, 1_250_000))]);
        let budget = RegressionBudget::empty(5.0);
        let report = compare(&base, &current, &budget, &opts());

        let mem = report
            .comparisons
            .iter()
            .find(|c| c.metric == Metric::PeakMem)
            .unwrap();
        assert_eq!(mem.budget_pct, 5.0, "declared budget, not the timing floor");
        assert_eq!(mem.status, CompareStatus::Exceeded);
        assert!(!report.passed());

        // The time row still gets the widened envelope — the split is the point.
        let time = report
            .comparisons
            .iter()
            .find(|c| c.metric == Metric::Time)
            .unwrap();
        assert_eq!(time.budget_pct, 30.0);
    }

    #[test]
    fn timing_noise_widens_phase_time_but_not_share() {
        let mut base_entry = phased(1000.0, &[("parse", 600.0), ("write", 400.0)]);
        base_entry.noise = Some(NoiseStats::from_samples(vec![
            800.0, 900.0, 1000.0, 1100.0, 1200.0,
        ]));
        let base = file_of("local", vec![("g/q1/small", base_entry)]);
        let current = current_with(vec![(
            "g/q1/small",
            phased(1000.0, &[("parse", 600.0), ("write", 400.0)]),
        )]);
        let budget = RegressionBudget::empty(5.0);
        let report = compare(&base, &current, &budget, &opts());

        let phase_time = report
            .comparisons
            .iter()
            .find(|c| c.metric == Metric::PhaseTime)
            .unwrap();
        assert_eq!(phase_time.budget_pct, 30.0, "phase ns is wall clock");
        let share = report
            .comparisons
            .iter()
            .find(|c| c.metric == Metric::PhaseShare)
            .unwrap();
        assert_eq!(
            share.budget_pct, DEFAULT_SHARE_DRIFT_PP,
            "share drift has its own threshold in pp"
        );
    }

    #[test]
    fn noise_floor_never_tightens_a_looser_budget() {
        // A quiet scenario (MAD 0) must not drag a 10% budget down to 0%.
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                noisy(&[1000.0, 1000.0, 1000.0, 1000.0, 1000.0]),
            )],
        );
        let current = current_with(vec![("g/q1/small", entry(1080.0))]); // +8%
        let budget = RegressionBudget::empty(10.0);
        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed());
        assert_eq!(report.comparisons[0].budget_pct, 10.0);
    }

    #[test]
    fn too_few_samples_do_not_widen_anything() {
        let base = file_of(
            "local",
            vec![("g/q1/small", noisy(&[900.0, 1000.0, 1100.0]))],
        );
        let current = current_with(vec![("g/q1/small", entry(1100.0))]);
        let budget = RegressionBudget::empty(1.0);
        let report = compare(&base, &current, &budget, &opts());
        assert_eq!(
            report.comparisons[0].budget_pct, 1.0,
            "n=3 < MIN_NOISE_SAMPLES"
        );
        assert!(!report.passed());
    }

    #[test]
    fn noise_median_replaces_the_single_run_mean() {
        // The entry's mean_ns is a wild outlier; the accumulated median is what
        // the gate must compare against.
        let mut base_entry = noisy(&[1000.0, 1000.0, 1000.0, 1000.0, 1000.0]);
        base_entry.mean_ns = 50_000.0;
        let base = file_of("local", vec![("g/q1/small", base_entry)]);
        let current = current_with(vec![("g/q1/small", entry(1000.0))]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert_eq!(report.comparisons[0].baseline, 1000.0);
        assert!(report.passed());
    }

    #[test]
    fn accumulate_grows_the_sample_series() {
        let mut acc = file_of("local", vec![("g/q1/small", entry(1000.0))]);
        for mean in [1010.0, 990.0, 1005.0, 995.0] {
            let fresh = file_of("local", vec![("g/q1/small", entry(mean))]);
            acc = accumulate(&acc, &fresh).unwrap();
        }
        let noise = acc.entries["g/q1/small"].noise.as_ref().unwrap();
        assert_eq!(noise.n, 5, "seed run plus four accumulations");
        assert_eq!(noise.median_ns, 1000.0);
        assert!(noise.samples_ns.contains(&1010.0));
    }

    #[test]
    fn accumulate_carries_forward_scenarios_not_rerun() {
        let acc = file_of(
            "local",
            vec![("g/q1/small", entry(1000.0)), ("g/q2/small", entry(2000.0))],
        );
        let fresh = file_of("local", vec![("g/q1/small", entry(1010.0))]);
        let merged = accumulate(&acc, &fresh).unwrap();
        assert_eq!(merged.entries.len(), 2);
        assert_eq!(merged.entries["g/q2/small"].mean_ns, 2000.0);
        assert!(merged.entries["g/q2/small"].noise.is_none());
    }

    /// reproB, from review-prbench. Both accumulate guards live in the library,
    /// not in the CLI that happens to call it — accumulation is the one
    /// operation that DESTROYS the evidence of a mismatch (two populations
    /// blended into one median/MAD, with the provenance overwritten in the same
    /// breath), so a direct caller must not be able to skip them.
    #[test]
    fn accumulate_refuses_across_host_classes() {
        let prev = file_of("bench-m8gd", vec![("g/q1/small", entry(1000.0))]);
        let fresh = file_of("macos-aarch64", vec![("g/q1/small", entry(1010.0))]);
        let err = accumulate(&prev, &fresh).unwrap_err();
        assert!(
            err.contains("bench-m8gd") && err.contains("macos-aarch64"),
            "{err}"
        );
        assert!(err.contains("host class"), "{err}");
    }

    #[test]
    fn accumulate_refuses_across_corpora() {
        let mut prev_entry = entry(1000.0);
        prev_entry.corpus = Some(corpus(b"one"));
        let mut fresh_entry = entry(1010.0);
        fresh_entry.corpus = Some(corpus(b"two"));
        let prev = file_of("local", vec![("g/q1/small", prev_entry)]);
        let fresh = file_of("local", vec![("g/q1/small", fresh_entry)]);

        let err = accumulate(&prev, &fresh).unwrap_err();
        assert!(err.contains("g/q1/small"), "{err}");
        assert!(err.contains("sha256"), "{err}");
    }

    #[test]
    fn accumulate_refuses_across_file_level_corpora() {
        let mut prev = file_of("local", vec![("g/q1/small", entry(1000.0))]);
        prev.corpus = Some(corpus(b"one"));
        let mut fresh = file_of("local", vec![("g/q1/small", entry(1010.0))]);
        fresh.corpus = Some(corpus(b"two"));
        assert!(accumulate(&prev, &fresh).is_err());
    }

    #[test]
    fn accumulate_allows_a_matching_corpus() {
        let mut prev_entry = entry(1000.0);
        prev_entry.corpus = Some(corpus(b"same"));
        let mut fresh_entry = entry(1010.0);
        fresh_entry.corpus = Some(corpus(b"same"));
        let prev = file_of("local", vec![("g/q1/small", prev_entry)]);
        let fresh = file_of("local", vec![("g/q1/small", fresh_entry)]);
        assert!(accumulate(&prev, &fresh).is_ok());
    }

    #[test]
    fn accumulate_ignores_corpora_for_scenarios_not_in_both() {
        // A subset rerun tops up only what it measured; a scenario absent from
        // the fresh run has no corpus to disagree about.
        let mut prev_a = entry(1000.0);
        prev_a.corpus = Some(corpus(b"one"));
        let mut prev_b = entry(2000.0);
        prev_b.corpus = Some(corpus(b"other"));
        let prev = file_of(
            "local",
            vec![("g/q1/small", prev_a), ("g/q2/small", prev_b)],
        );

        let mut fresh_a = entry(1010.0);
        fresh_a.corpus = Some(corpus(b"one"));
        let fresh = file_of("local", vec![("g/q1/small", fresh_a)]);
        assert!(accumulate(&prev, &fresh).is_ok());
    }

    /// The remedy must not suggest relabelling this host to the baseline's
    /// class: that converts a refusal into a confident ENFORCEMENT against
    /// hardware the baseline never ran on — the exact failure the refusal
    /// exists to prevent.
    #[test]
    fn host_mismatch_remedy_does_not_suggest_relabelling_the_host() {
        let base = file_of("bench-m8gd", vec![("g/q1/small", entry(1000.0))]);
        let current = file_of("macos-aarch64", vec![("g/q1/small", entry(1000.0))]);
        let budget = RegressionBudget::empty(5.0);
        let remedy = &compare(&base, &current, &budget, &opts()).fatal[0].remedy;

        assert!(remedy.contains("--allow-host-mismatch"), "{remedy}");
        assert!(
            !remedy.contains("--host-class") && !remedy.contains("FLUREE_BENCH_HOST_CLASS"),
            "remedy must not offer relabelling as a fix: {remedy}"
        );
    }

    #[test]
    fn shares_that_do_not_sum_to_100_are_reported_but_do_not_gate() {
        // Hand-assembled or hand-edited shares: the drift comparison is still
        // internally consistent, so report rather than fail.
        let mut skewed = entry(1000.0);
        skewed.phases = [
            (
                "parse".to_string(),
                PhaseTiming {
                    ns: 10.0,
                    share_pct: 30.0,
                },
            ),
            (
                "write".to_string(),
                PhaseTiming {
                    ns: 10.0,
                    share_pct: 30.0,
                },
            ),
        ]
        .into_iter()
        .collect();
        let base = file_of("local", vec![("g/q1/small", skewed.clone())]);
        let current = file_of("local", vec![("g/q1/small", skewed)]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.passed(), "a cosmetic share defect must not gate");
        assert!(
            report.anomalies.iter().any(|a| a.contains("60.0%")),
            "{:?}",
            report.anomalies
        );
    }

    #[test]
    fn well_formed_shares_produce_no_anomaly() {
        let base = file_of(
            "local",
            vec![(
                "g/q1/small",
                phased(1000.0, &[("parse", 600.0), ("write", 400.0)]),
            )],
        );
        let current = base.clone();
        let budget = RegressionBudget::empty(5.0);
        assert!(compare(&base, &current, &budget, &opts())
            .anomalies
            .is_empty());
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    #[test]
    fn schema_1_baseline_loads_with_host_class_from_runner_class() {
        let json = r#"{"schema_version":1,"label":"old","captured_at":"2026-01-01T00:00:00Z",
            "profile":"quick","scale":"small","runner_class":"ci-ubuntu-latest",
            "host":"runner-7","entries":{"g/q1/small":{"mean_ns":10.0,"median_ns":10.0}}}"#;
        let read: BaselineFile = serde_json::from_str(json).unwrap();
        assert_eq!(read.host.class, "ci-ubuntu-latest");
        assert_eq!(read.host.name, "runner-7");
        assert_eq!(read.host.os, "");
        assert_eq!(read.entries["g/q1/small"].mean_ns, 10.0);
        assert!(read.entries["g/q1/small"].phases.is_empty());
        assert!(read.entries["g/q1/small"].corpus.is_none());
    }

    #[test]
    fn schema_1_baseline_without_runner_class_defaults_to_local() {
        let json = r#"{"schema_version":1,"label":"old","captured_at":"2026-01-01T00:00:00Z","profile":"quick","scale":"small","entries":{}}"#;
        let read: BaselineFile = serde_json::from_str(json).unwrap();
        assert_eq!(read.host.class, LEGACY_DEFAULT_HOST_CLASS);
        assert_eq!(read.host.name, "");
    }

    #[test]
    fn committed_schema_1_baseline_shape_still_loads() {
        // The exact shape of bench-baselines/guardrails-pre.json, which stays on
        // disk unchanged: the legacy reader is what keeps it comparable.
        let json = r#"{"schema_version":1,"label":"guardrails-pre","git_sha":"2c5162014",
            "captured_at":"2026-07-11T23:50:43.824907+00:00","profile":"quick","scale":"small",
            "runner_class":"local","host":"MacBook-Pro-8-applesilicon",
            "entries":{"query_overlay_matrix/count_base/small":{"mean_ns":15669.9,"median_ns":15670.4,
            "mem":{"peak_bytes":4820445,"total_allocated_bytes":33717125336}}}}"#;
        let read: BaselineFile = serde_json::from_str(json).unwrap();
        assert_eq!(read.host.class, "local");
        assert_eq!(read.host.name, "MacBook-Pro-8-applesilicon");
        assert_eq!(
            read.entries["query_overlay_matrix/count_base/small"]
                .mem
                .unwrap()
                .peak_bytes,
            4_820_445
        );
    }

    #[test]
    fn newer_schema_version_is_fatal() {
        let mut base = baseline_with(vec![("g/q1/small", entry(1000.0))]);
        base.schema_version = BASELINE_SCHEMA_VERSION + 1;
        let current = current_with(vec![("g/q1/small", entry(1000.0))]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(report.is_fatal());
        assert_eq!(report.fatal[0].kind, MismatchKind::SchemaVersion);
    }

    #[test]
    fn baseline_file_round_trips_through_schema_2() {
        let mut base = baseline_with(vec![("g/q1/small", {
            let mut e = phased(42.0, &[("parse", 10.0), ("write", 30.0)]);
            e.corpus = Some(corpus(b"bytes"));
            e.noise = Some(NoiseStats::from_samples(vec![40.0, 42.0, 44.0, 41.0, 43.0]));
            e
        })]);
        base.host = HostBlock {
            class: "bench-m8gd".into(),
            name: "ip-10-0-0-1".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            cpu_model: "AMD EPYC 9R14".into(),
            physical_cores: Some(16),
            logical_cores: Some(32),
        };
        base.corpus = Some(corpus(b"file-level"));

        let json = serde_json::to_string(&base).unwrap();
        let read: BaselineFile = serde_json::from_str(&json).unwrap();
        assert_eq!(read.host, base.host);
        assert_eq!(read.corpus, base.corpus);
        let e = &read.entries["g/q1/small"];
        assert_eq!(e.mean_ns, 42.0);
        assert_eq!(e.phases["parse"].share_pct, 25.0);
        assert_eq!(e.noise.as_ref().unwrap().n, 5);
        assert_eq!(e.corpus.as_ref().unwrap().sha256, corpus(b"bytes").sha256);
    }

    #[test]
    fn derived_host_class_is_os_dash_arch() {
        assert_eq!(
            derived_host_class(),
            format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
        );
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
    fn stale_sidecar_only_entry_is_not_compared() {
        // Current has ONLY a mem sidecar (mean_ns == 0): the bench was not rerun
        // this session, so its mem is stale and must not be compared (item 10);
        // it counts as missing.
        let base = baseline_with(vec![("g/q1/small", mem_entry(1000.0, 1_000_000))]);
        let stale = BaselineEntry {
            mem: Some(MemMetrics {
                peak_bytes: 9_000_000,
                total_allocated_bytes: 0,
            }),
            ..entry(0.0)
        };
        let current = current_with(vec![("g/q1/small", stale)]);
        let budget = RegressionBudget::empty(5.0);

        let report = compare(&base, &current, &budget, &opts());
        assert!(
            report.comparisons.is_empty(),
            "stale sidecar must not be compared"
        );
        assert_eq!(report.missing, vec!["g/q1/small".to_string()]);
    }

    /// The whole gate rule, as a truth table over
    /// (host class matched?) × (which metric breached).
    ///
    /// Matched → both metrics enforce. Mismatched *under
    /// `--allow-host-mismatch`* → both are advisory, so the gate passes while
    /// still annotating. Without the flag a mismatch never reaches this table at
    /// all — it is fatal.
    #[test]
    fn gate_truth_table_over_host_class_match() {
        let budget = RegressionBudget::empty(5.0);
        // (label, current entry, mismatched?, expect_pass, breaches, advisories)
        let cases = [
            (
                "matched + clean",
                mem_entry(1000.0, 1_000_000),
                false,
                true,
                0,
                0,
            ),
            (
                "matched + time breach",
                mem_entry(2000.0, 1_000_000),
                false,
                false,
                1,
                0,
            ),
            (
                "matched + mem breach",
                mem_entry(1000.0, 2_000_000),
                false,
                false,
                1,
                0,
            ),
            (
                "matched + both breach",
                mem_entry(2000.0, 2_000_000),
                false,
                false,
                2,
                0,
            ),
            (
                "mismatched + clean",
                mem_entry(1000.0, 1_000_000),
                true,
                true,
                0,
                0,
            ),
            (
                "mismatched + time breach",
                mem_entry(2000.0, 1_000_000),
                true,
                true,
                0,
                1,
            ),
            (
                "mismatched + mem breach",
                mem_entry(1000.0, 2_000_000),
                true,
                true,
                0,
                1,
            ),
            (
                "mismatched + both breach",
                mem_entry(2000.0, 2_000_000),
                true,
                true,
                0,
                2,
            ),
        ];
        for (label, cur, mismatched, expect_pass, breaches, advisories) in cases {
            let base = file_of(
                "class-a",
                vec![("g/q1/small", mem_entry(1000.0, 1_000_000))],
            );
            let current = file_of(
                if mismatched { "class-b" } else { "class-a" },
                vec![("g/q1/small", cur)],
            );
            let report = compare(&base, &current, &budget, &allow_mismatch());
            assert_eq!(report.passed(), expect_pass, "{label}: gate outcome");
            assert_eq!(report.breaches().count(), breaches, "{label}: breach count");
            assert_eq!(
                report.advisories().count(),
                advisories,
                "{label}: advisory count"
            );
        }
    }
}
