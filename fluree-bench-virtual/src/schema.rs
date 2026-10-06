//! Serde types for the run record stream (`run.jsonl`).
//!
//! Stability over cleverness: this schema is written to disk on every run and
//! read back by `vbench report` (and, later, dashboards and regression gates)
//! across months of perf work. Bump [`SCHEMA_VERSION`] on any breaking change to
//! the field set so old runs remain interpretable.
//!
//! A run file is newline-delimited JSON where **every** line is a [`Line`]: the
//! first line is a [`Line::Meta`] (one [`RunMeta`]) and each subsequent line is a
//! [`Line::Record`] (one [`RunRecord`]). Records are appended and flushed as they
//! complete, so a crash mid-run still leaves a readable partial file.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Bump on any breaking change to [`RunMeta`] / [`RunRecord`] field sets.
pub const SCHEMA_VERSION: u32 = 1;

/// One line of a `run.jsonl` file. Internally tagged by `kind` so a reader can
/// stream lines without knowing their position.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Line {
    /// The run header — always the first line.
    Meta(RunMeta),
    /// One measured (query, target) outcome.
    Record(RunRecord),
}

/// Run-level provenance: everything needed to reproduce and to reject a
/// stale/incomparable run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    pub schema_version: u32,
    pub run_id: String,
    /// RFC-3339 UTC timestamp of run start.
    pub timestamp: String,
    /// Short git commit of the `fluree-bench-virtual` worktree, or `"unknown"`.
    pub git_commit: String,
    /// Whether the worktree had uncommitted changes at run start.
    pub git_dirty: bool,
    /// `"debug"` or `"release"` (from `cfg!(debug_assertions)`).
    pub build_profile: String,
    /// Host identifier (`hostname`), or `"unknown"`.
    pub host: String,
    /// Tokio runtime shape, e.g. `"tokio-multi-thread(worker_threads=10)"`.
    pub runtime: String,
    /// The subset filter applied, if any (e.g. `"smoke"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subset: Option<String>,
    /// The corpus manifest's `corpus_version` this run executed against, so a run
    /// made before/after a mid-flight corpus amendment stays distinguishable.
    /// Defaults to `0` for older runs missing the field.
    #[serde(default)]
    pub corpus_version: u32,
    /// A survey run is informational — capped and rate-limited (typically the
    /// live SF20 stress subset) and is **never a gate**: `bless` refuses it and
    /// `check` skips it. Defaults to `false` for older runs missing the field.
    #[serde(default)]
    pub survey: bool,
    /// Fingerprint of every target this run touched.
    pub targets: Vec<TargetFingerprint>,
}

/// Enough of a target's identity to detect an incomparable comparison later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetFingerprint {
    pub id: String,
    /// `"native"` or `"virtual"`.
    pub kind: String,
    pub alias: String,
    pub fluree_home: String,
}

/// Terminal state of a single execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Completed and produced a result.
    Ok,
    /// Failed with an engine/builder error (not a timeout).
    Error,
    /// Did-not-finish: hit the per-query deadline.
    Dnf,
    /// Failed with an engine error, but the manifest declared this query is
    /// *expected* to error on this target kind (e.g. the R2RML router rejects a
    /// lang-tagged / custom-datatype bound object with a whole-GRAPH-scope error).
    /// Counts as a pass for gating. See `corpus::ExpectedStatus`.
    #[serde(rename = "expected_error")]
    ExpectedError,
}

/// Aggregated timing for one span name across a single execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanAgg {
    /// Number of spans of this name observed.
    pub n: u64,
    /// Sum of span lifetimes (microseconds).
    pub total_us: u64,
    /// Longest single span lifetime (microseconds).
    pub max_us: u64,
}

/// Pathway counters for one execution: per-span timing plus the summed numeric
/// fields the Iceberg planner/reader record on their spans.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    /// Per-span-name aggregates, keyed by span name (see `spans::SPAN_ALLOWLIST`).
    pub spans: BTreeMap<String, SpanAgg>,
    /// Sum of `iceberg.scan_plan.files_selected`.
    pub files_selected: u64,
    /// Sum of `iceberg.scan_plan.files_pruned`.
    pub files_pruned: u64,
    /// Sum of `iceberg.scan_plan.estimated_row_count`.
    pub estimated_row_count: u64,
    /// Sum of `iceberg.parquet_read.file_size` (bytes).
    pub file_size: u64,
}

/// One (query, target) outcome. Timing/counters/hash all come from the same
/// (median-wall) measured rep so they are internally consistent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub query_id: String,
    /// Target id (join to [`RunMeta::targets`] for its kind).
    pub target: String,
    /// Cache state of the reported rep. Always `"hot"` today — every measured rep
    /// follows a discarded priming rep. The field exists so a future cold-mode
    /// protocol can record `"cold"` without a schema bump.
    pub cache_state: String,
    /// Index (0-based, in execution order) of the reported/median rep.
    pub rep: usize,
    /// Number of measured reps (excludes the priming rep).
    pub reps: usize,
    /// Median measured wall time (milliseconds).
    pub wall_ms: u64,
    /// Every measured rep's wall (milliseconds), in execution order.
    pub all_walls_ms: Vec<u64>,
    pub status: Status,
    /// Row count of the reported rep (0 on error/dnf).
    pub rows: usize,
    /// SHA-256 (hex) of the canonicalized result multiset (empty on error/dnf).
    pub result_hash: String,
    pub counters: Counters,
    /// Expected-for-virtual spans that did not fire (empty for native targets).
    pub spans_missing: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// First 20 canonical rows, when `--keep-heads` was passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heads: Option<Vec<String>>,
}
