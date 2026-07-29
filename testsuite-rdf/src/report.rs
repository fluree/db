//! Machine-readable conformance reporting.
//!
//! [`write_json_report`] keeps the shape of `testsuite-sparql`'s report (same
//! field names, same `TestEntry`) so existing tooling reads both.
//! [`Conformance`] is the aggregate `conformance.json` the benchmark lane
//! requires: `riot-analog-bench-strategy.md` §6b makes a Tier-2 matrix
//! refuse to publish unless a conformance run at the *same* `git_sha` and
//! `rdf_tests_submodule_sha` exists, so every speed number carries a pass
//! rate measured on the same code and the same suite revision.
//!
//! # Denominator policy
//!
//! `total = passed + failed + ignored`. **Registered (ignored) tests stay in
//! the denominator and out of the numerator** — a known failure must depress
//! the pass rate, or registering a test would be a way to improve the number.
//! `pass_rate = passed / total`.
//!
//! Only RDF 1.1 suites in [`ParseMode::Conformant`](crate::ParseMode::Conformant)
//! gate. RDF 1.2 (the asserting subset) and the informational
//! ingest-default runs are recorded with `gating: false` and must never be
//! averaged into a headline number.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A single test entry — field-compatible with testsuite-sparql's reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEntry {
    pub test_id: String,
    /// `pass` | `fail` | `ignored` | `unexpected-pass`
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregate summary of one suite run, in one parse mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteReport {
    /// Short format label: `turtle`, `ntriples`, …
    pub format: String,
    /// RDF version the suite belongs to: `rdf11` | `rdf12`.
    pub spec: String,
    /// `conformant` | `ingest-default`.
    pub mode: String,
    /// Whether this run gates CI (RDF 1.1 + conformant only).
    pub gating: bool,
    pub manifest: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
    pub pass_rate: String,
}

impl SuiteReport {
    pub fn pass_rate_pct(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.passed as f64 / self.total as f64) * 100.0
        }
    }
}

/// The `conformance.json` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conformance {
    /// Commit of THIS repo the suites ran against.
    pub git_sha: String,
    /// RFC 3339 capture time.
    pub captured_at: String,
    /// Suite family this document covers.
    pub suite: String,
    /// Commit of the shared w3c/rdf-tests checkout.
    pub rdf_tests_submodule_sha: Option<String>,
    /// One entry per (format, spec, mode).
    pub per_format: Vec<SuiteReport>,
    /// Human-readable statement of the denominator rule, carried in the
    /// artifact so a reader of the JSON alone cannot misread the number.
    pub denominator_policy: String,
}

pub const DENOMINATOR_POLICY: &str = "total = passed + failed + ignored; registered \
    (ignored) tests count in the denominator and NOT the numerator, so a known \
    failure depresses the pass rate. Only entries with gating=true (RDF 1.1, \
    conformant mode) gate CI; rdf12 and ingest-default runs are informational.";

/// Write a per-suite JSON report (testsuite-sparql's shape).
pub fn write_json_report(path: &str, report: &SuiteReport, entries: &[TestEntry]) -> Result<()> {
    #[derive(Serialize)]
    struct Doc<'a> {
        #[serde(flatten)]
        summary: &'a SuiteReport,
        tests: &'a [TestEntry],
    }

    let json = serde_json::to_string_pretty(&Doc {
        summary: report,
        tests: entries,
    })?;
    std::fs::write(path, &json)?;
    eprintln!("JSON report written to {path}");
    Ok(())
}

/// Compose a [`SuiteReport`] from raw counts.
#[allow(clippy::too_many_arguments)]
pub fn suite_report(
    format: &str,
    spec: &str,
    mode: &str,
    gating: bool,
    manifest: &str,
    total: usize,
    passed: usize,
    failed: usize,
    ignored: usize,
) -> SuiteReport {
    let pass_rate = if total > 0 {
        format!("{:.1}%", (passed as f64 / total as f64) * 100.0)
    } else {
        "N/A".to_string()
    };
    SuiteReport {
        format: format.to_string(),
        spec: spec.to_string(),
        mode: mode.to_string(),
        gating,
        manifest: manifest.to_string(),
        total,
        passed,
        failed,
        ignored,
        pass_rate,
    }
}

/// Current repo commit, for the conformance artifact.
pub fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// RFC 3339 timestamp without pulling in a date crate.
///
/// `date -u` is present on both CI (ubuntu) and dev (darwin); if it is not,
/// the field records that rather than a fabricated time.
pub fn captured_at() -> String {
    std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_tests_stay_in_the_denominator() {
        let r = suite_report("turtle", "rdf11", "conformant", true, "m", 100, 90, 0, 10);
        assert_eq!(r.pass_rate, "90.0%");
        assert_eq!(r.total, r.passed + r.failed + r.ignored);
    }

    #[test]
    fn an_empty_suite_reports_na_not_a_perfect_score() {
        let r = suite_report("turtle", "rdf11", "conformant", true, "m", 0, 0, 0, 0);
        assert_eq!(r.pass_rate, "N/A");
        assert_eq!(r.pass_rate_pct(), 0.0);
    }
}
