//! Per-scenario memory metric recording.
//!
//! Criterion measures wall-clock only. Benches that also want allocation
//! metrics install the tracking allocator from `fluree-bench-alloc` in
//! their own binary, then record a [`MemMetrics`] per scenario via
//! [`record_scenario`]. Metrics land as JSON sidecar files under
//! `target/fluree-bench-mem/<bench>.json`, keyed by the same
//! `<group>/<function>/<scale>` ID criterion uses, so the
//! `bench-baseline` tool can merge them into the captured baseline and
//! compare them with the same budget machinery as time.
//!
//! This module is pure-safe-Rust (file IO + serde); the one `unsafe impl`
//! the allocator needs lives in `fluree-bench-alloc`, keeping this crate's
//! `#![forbid(unsafe_code)]` intact.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Memory metrics for one bench scenario.
///
/// `peak_bytes` is the live-bytes high-water mark across the scenario's
/// measured iterations; `total_allocated_bytes` is cumulative allocation
/// (churn). Both come from `fluree_bench_alloc::snapshot()` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemMetrics {
    pub peak_bytes: u64,
    pub total_allocated_bytes: u64,
}

/// Sidecar directory under the cargo target dir.
///
/// Resolved relative to the workspace root (same discovery as
/// `budget::workspace_root()`), or `target/` under the current dir as a
/// fallback so the helper still works outside the workspace.
pub fn sidecar_dir() -> PathBuf {
    let base = crate::budget::workspace_root()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("target");
    base.join("fluree-bench-mem")
}

/// Record metrics for one scenario of `bench`, merging into the bench's
/// sidecar file. `scenario_id` must match the criterion ID path for the
/// scenario — `"<group>/<function>/<scale>"` — so baseline capture can
/// join time and memory rows.
///
/// Best-effort by design: bench output must not fail because a sidecar
/// write did. IO errors are reported to stderr and swallowed.
pub fn record_scenario(bench: &str, scenario_id: &str, metrics: MemMetrics) {
    let dir = sidecar_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("  [bench-mem] create {}: {e}", dir.display());
        return;
    }
    let path = dir.join(format!("{bench}.json"));
    let mut entries = read_sidecar(&path).unwrap_or_default();
    entries.insert(scenario_id.to_string(), metrics);
    match serde_json::to_string_pretty(&entries) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                eprintln!("  [bench-mem] write {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("  [bench-mem] serialize {}: {e}", path.display()),
    }
}

/// Read one sidecar file. Returns `None` when missing or unparsable.
pub fn read_sidecar(path: &Path) -> Option<BTreeMap<String, MemMetrics>> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Read every sidecar in [`sidecar_dir`] (or the given dir), merged into a
/// single scenario-ID → metrics map. Used by `bench-baseline capture`.
pub fn read_all_sidecars(dir: &Path) -> BTreeMap<String, MemMetrics> {
    let mut merged = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return merged;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            if let Some(map) = read_sidecar(&path) {
                merged.extend(map);
            }
        }
    }
    merged
}

/// Remove all sidecar files so a fresh bench run starts clean. Called by
/// `bench-baseline capture --clean-mem` and useful at the top of scripted
/// baseline runs; stale sidecars from an earlier run would otherwise be
/// merged into the new baseline.
pub fn clear_sidecars(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "fluree-bench-mem-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("some_bench.json");

        let mut entries: BTreeMap<String, MemMetrics> = BTreeMap::new();
        entries.insert(
            "some_bench/q1/small".into(),
            MemMetrics {
                peak_bytes: 1024,
                total_allocated_bytes: 4096,
            },
        );
        std::fs::write(&path, serde_json::to_string(&entries).unwrap()).unwrap();

        let read = read_sidecar(&path).unwrap();
        assert_eq!(read["some_bench/q1/small"].peak_bytes, 1024);

        let all = read_all_sidecars(&dir);
        assert_eq!(all.len(), 1);

        clear_sidecars(&dir);
        assert!(read_all_sidecars(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_sidecar_is_none() {
        assert!(read_sidecar(Path::new("/nonexistent/x.json")).is_none());
    }
}
