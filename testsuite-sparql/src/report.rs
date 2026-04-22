//! Machine-readable JSON report output for W3C SPARQL test results.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A single test entry in the JSON report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEntry {
    pub test_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Whether this test failed due to a timeout (subprocess killed).
    #[serde(default, skip_serializing_if = "is_false")]
    pub timeout: bool,
}

fn is_false(v: &bool) -> bool {
    !v
}

/// Aggregate summary of a test run.
#[derive(Debug, Serialize)]
struct Report<'a> {
    suite: &'a str,
    total: usize,
    passed: usize,
    ignored: usize,
    failed: usize,
    pass_rate: String,
    tests: &'a [TestEntry],
}

/// Write a JSON report file for a single test suite run.
pub fn write_json_report(
    path: &str,
    suite: &str,
    entries: &[TestEntry],
    total: usize,
    passed: usize,
    ignored: usize,
    failed: usize,
) -> Result<()> {
    let pass_rate = if total > 0 {
        format!("{:.1}%", (passed as f64 / total as f64) * 100.0)
    } else {
        "N/A".to_string()
    };

    let report = Report {
        suite,
        total,
        passed,
        ignored,
        failed,
        pass_rate,
        tests: entries,
    };

    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(path, &json)?;
    eprintln!("JSON report written to {path}");
    Ok(())
}

/// Append entries to a combined JSON report that aggregates multiple suites.
///
/// If the file exists, it reads, merges, and rewrites. Otherwise creates new.
pub fn append_to_combined_report(path: &str, new_entries: &[TestEntry]) -> Result<()> {
    let mut all_entries: Vec<TestEntry> = if std::path::Path::new(path).exists() {
        let existing = std::fs::read_to_string(path)?;
        serde_json::from_str(&existing).unwrap_or_default()
    } else {
        Vec::new()
    };

    all_entries.extend_from_slice(new_entries);

    let json = serde_json::to_string_pretty(&all_entries)?;
    std::fs::write(path, &json)?;
    Ok(())
}
