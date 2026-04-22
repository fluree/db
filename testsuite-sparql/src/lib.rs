pub mod evaluator;
pub mod files;
pub mod manifest;
pub mod query_handler;
pub mod rdfxml;
pub mod report;
pub mod result_comparison;
pub mod result_format;
pub mod sparql_handlers;
pub mod subprocess;
pub mod vocab;

use anyhow::{bail, Result};

use evaluator::TestEvaluator;
use manifest::TestManifest;
use report::TestEntry;
use sparql_handlers::register_sparql_tests;

/// Run all tests from the given manifest URL(s).
///
/// Tests listed in `ignored_tests` are expected to fail and won't cause
/// the overall suite to fail. Every other test must pass.
///
/// If the `W3C_REPORT_JSON` environment variable is set, a machine-readable
/// JSON report is written to that path. Use `--test-threads=1` when generating
/// reports to avoid concurrent writes to the same file.
pub fn check_testsuite(manifest_url: &str, ignored_tests: &[&str]) -> Result<()> {
    let mut evaluator = TestEvaluator::default();
    register_sparql_tests(&mut evaluator);

    let manifest = TestManifest::new([manifest_url]);
    let results = evaluator.evaluate(manifest)?;

    let mut failures = Vec::new();
    let mut pass_count = 0;
    let mut ignore_count = 0;
    let mut total = 0;
    let mut report_entries = Vec::new();

    for result in &results {
        total += 1;
        let status;
        match &result.outcome {
            Ok(()) => {
                pass_count += 1;
                status = "pass";
            }
            Err(error) => {
                if ignored_tests.contains(&result.test.as_str()) {
                    ignore_count += 1;
                    status = "ignored";
                } else {
                    failures.push(format!("{}: {error:#}", result.test));
                    status = "fail";
                }
            }
        }
        let is_timeout = result
            .outcome
            .as_ref()
            .err()
            .map(|e| {
                let msg = format!("{e:#}");
                msg.contains("timed out") || msg.contains("timeout")
            })
            .unwrap_or(false);

        report_entries.push(TestEntry {
            test_id: result.test.clone(),
            status: status.to_string(),
            error: result.outcome.as_ref().err().map(|e| format!("{e:#}")),
            timeout: is_timeout,
        });
    }

    eprintln!(
        "\n=== Test Summary ===\n\
         Total:   {total}\n\
         Passed:  {pass_count}\n\
         Ignored: {ignore_count}\n\
         Failed:  {}\n",
        failures.len()
    );

    // Write JSON report if requested via env var.
    // NOTE: Use --test-threads=1 when generating reports to avoid
    // concurrent writes from parallel test functions.
    if let Ok(report_path) = std::env::var("W3C_REPORT_JSON") {
        report::write_json_report(
            &report_path,
            manifest_url,
            &report_entries,
            total,
            pass_count,
            ignore_count,
            failures.len(),
        )?;
    }

    if !failures.is_empty() {
        bail!(
            "{} failing test(s):\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }

    Ok(())
}
