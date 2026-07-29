//! `vbench report`: read a `run.jsonl` and print a per-query native-vs-virtual
//! comparison table (or `--json`).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::schema::{Line, RunMeta, RunRecord, Status};
use crate::spans;

/// Parse a run file into its header and records.
pub fn read_run(path: &Path) -> Result<(RunMeta, Vec<RunRecord>)> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading run file {}", path.display()))?;
    let mut meta: Option<RunMeta> = None;
    let mut records = Vec::new();
    for (i, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: Line = serde_json::from_str(line)
            .with_context(|| format!("parsing line {} of {}", i + 1, path.display()))?;
        match parsed {
            Line::Meta(m) => meta = Some(m),
            Line::Record(r) => records.push(r),
        }
    }
    let meta = meta.context("run file has no meta (first) line")?;
    Ok((meta, records))
}

/// One native-vs-virtual comparison row for `--json`.
#[derive(Debug, Serialize)]
pub struct Comparison {
    pub query_id: String,
    pub native_target: Option<String>,
    pub virtual_target: String,
    pub native_ms: Option<u64>,
    pub virtual_ms: u64,
    pub ratio: Option<f64>,
    pub virtual_status: Status,
    pub scans: u64,
    pub load_table: u64,
    /// Total microseconds in `r2rml.load_table` spans (cold REST/OAuth catalog cost).
    pub load_table_us: u64,
    pub files_pruned: u64,
    pub files_selected: u64,
    pub hash_match: Option<bool>,
    pub spans_missing: Vec<String>,
}

/// Build the comparison rows: for each query, pair each virtual target's record
/// with the query's native record (if present).
pub fn comparisons(meta: &RunMeta, records: &[RunRecord]) -> Vec<Comparison> {
    let kinds: BTreeMap<&str, &str> = meta
        .targets
        .iter()
        .map(|t| (t.id.as_str(), t.kind.as_str()))
        .collect();
    let is_virtual = |target: &str| kinds.get(target).copied() == Some("virtual");
    let is_native = |target: &str| kinds.get(target).copied() == Some("native");

    // Group records by query id, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut by_query: BTreeMap<String, Vec<&RunRecord>> = BTreeMap::new();
    for r in records {
        if !by_query.contains_key(&r.query_id) {
            order.push(r.query_id.clone());
        }
        by_query.entry(r.query_id.clone()).or_default().push(r);
    }

    let mut out = Vec::new();
    for qid in &order {
        let group = &by_query[qid];
        let native = group.iter().copied().find(|r| is_native(&r.target));
        for vrec in group.iter().copied().filter(|r| is_virtual(&r.target)) {
            let native_ms = native.map(|n| n.wall_ms);
            let ratio = native_ms.and_then(|nms| {
                if nms == 0 {
                    None
                } else {
                    Some(vrec.wall_ms as f64 / nms as f64)
                }
            });
            let hash_match = native.and_then(|n| {
                if n.status == Status::Ok && vrec.status == Status::Ok {
                    Some(n.result_hash == vrec.result_hash)
                } else {
                    None
                }
            });
            out.push(Comparison {
                query_id: qid.clone(),
                native_target: native.map(|n| n.target.clone()),
                virtual_target: vrec.target.clone(),
                native_ms,
                virtual_ms: vrec.wall_ms,
                ratio,
                virtual_status: vrec.status,
                scans: spans::span_count(&vrec.counters, "r2rml.scan_table"),
                load_table: spans::span_count(&vrec.counters, "r2rml.load_table"),
                load_table_us: spans::span_total_us(&vrec.counters, "r2rml.load_table"),
                files_pruned: vrec.counters.files_pruned,
                files_selected: vrec.counters.files_selected,
                hash_match,
                spans_missing: vrec.spans_missing.clone(),
            });
        }
    }
    out
}

/// Print the human-readable comparison table to stdout.
pub fn print_table(meta: &RunMeta, records: &[RunRecord]) {
    let rows = comparisons(meta, records);

    println!(
        "run {}  ({}, git {}{}, {})",
        meta.run_id,
        meta.timestamp,
        meta.git_commit,
        if meta.git_dirty { "-dirty" } else { "" },
        meta.build_profile,
    );

    if rows.is_empty() {
        // No virtual records: fall back to a native-only timing list.
        print_native_only(meta, records);
        return;
    }

    let header = format!(
        "{:<8} {:>10} {:>10} {:>7} {:>6} {:>6} {:>14} {:>6}",
        "query", "native ms", "virt ms", "ratio", "scans", "load", "pruned/select", "hash"
    );
    println!("{header}");
    println!("{}", "-".repeat(header.len()));
    for r in &rows {
        let native_ms = r
            .native_ms
            .map_or_else(|| "-".to_string(), |m| m.to_string());
        let ratio = r
            .ratio
            .map_or_else(|| "-".to_string(), |x| format!("{x:.2}x"));
        let virt_ms = match r.virtual_status {
            Status::Ok => r.virtual_ms.to_string(),
            Status::Dnf => format!("{} DNF", r.virtual_ms),
            Status::Error => "ERR".to_string(),
            Status::ExpectedError => "xERR".to_string(),
        };
        let hash = match r.hash_match {
            Some(true) => "ok".to_string(),
            Some(false) => "MISMATCH".to_string(),
            None => "-".to_string(),
        };
        let pruned_select = format!("{}/{}", r.files_pruned, r.files_selected);
        println!(
            "{:<8} {:>10} {:>10} {:>7} {:>6} {:>6} {:>14} {:>6}",
            r.query_id, native_ms, virt_ms, ratio, r.scans, r.load_table, pruned_select, hash
        );
        if !r.spans_missing.is_empty() {
            println!(
                "         !! expected virtual spans missing: {:?}",
                r.spans_missing
            );
        }
    }
}

/// Fallback table when a run has no virtual records (e.g. native-only smoke).
fn print_native_only(_meta: &RunMeta, records: &[RunRecord]) {
    println!("(no virtual records — native timings only)");
    let header = format!(
        "{:<8} {:<16} {:>10} {:>8} {:>6}",
        "query", "target", "wall ms", "status", "rows"
    );
    println!("{header}");
    println!("{}", "-".repeat(header.len()));
    for r in records {
        let status = match r.status {
            Status::Ok => "ok",
            Status::Dnf => "dnf",
            Status::Error => "error",
            Status::ExpectedError => "xerror",
        };
        println!(
            "{:<8} {:<16} {:>10} {:>8} {:>6}",
            r.query_id, r.target, r.wall_ms, status, r.rows
        );
    }
}

/// Emit the comparison rows as JSON.
pub fn print_json(meta: &RunMeta, records: &[RunRecord]) -> Result<()> {
    let rows = comparisons(meta, records);
    let doc: Value = serde_json::json!({
        "meta": meta,
        "comparisons": rows,
    });
    println!("{}", serde_json::to_string_pretty(&doc)?);
    Ok(())
}
