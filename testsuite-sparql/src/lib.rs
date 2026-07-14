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
/// The skip register is policed in both directions: a test in
/// `ignored_tests` that *passes* fails the suite as a stale skip entry.
/// This keeps the register an accurate ledger of remaining gaps — entries
/// must be removed in the same change that fixes the underlying feature.
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
    let mut seen_tests = std::collections::HashSet::new();

    for result in &results {
        total += 1;
        seen_tests.insert(result.test.as_str());
        let status;
        match &result.outcome {
            Ok(()) => {
                if ignored_tests.contains(&result.test.as_str()) {
                    failures.push(format!(
                        "{}: unexpectedly PASSED but is in the skip register — \
                         remove its entry (stale skip entries hide regressions)",
                        result.test
                    ));
                    status = "unexpected-pass";
                } else {
                    pass_count += 1;
                    status = "pass";
                }
            }
            Err(error) => {
                let msg = format!("{error:#}");
                if ignored_tests.contains(&result.test.as_str()) {
                    // The register excuses a KNOWN WRONG ANSWER, not an infra
                    // death: a registered test that starts timing out or
                    // crashing is a new regression (hang, panic) hiding
                    // behind an old entry — fail it.
                    if is_infra_death(&msg) {
                        failures.push(format!(
                            "{}: registered test died by timeout/crash instead of \
                             its registered failure mode — investigate as a \
                             regression: {msg}",
                            result.test
                        ));
                        status = "fail";
                    } else {
                        ignore_count += 1;
                        status = "ignored";
                    }
                } else {
                    failures.push(format!("{}: {msg}", result.test));
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

    // A manifest that resolves but yields zero tests means coverage silently
    // vanished (submodule restructure, partial checkout, manifest-parser
    // regression) — that must never report green.
    if total == 0 {
        bail!(
            "Manifest yielded ZERO tests — coverage silently lost \
             (submodule drift or manifest-parse regression?): {manifest_url}"
        );
    }

    // Register entries must correspond to tests that actually ran. An entry
    // matching no discovered test (typo, upstream rename, dawgt:Rejected
    // drop) would otherwise live forever, overstating the register.
    for entry in ignored_tests {
        if !seen_tests.contains(entry) {
            failures.push(format!(
                "register entry matches no test discovered in this suite — \
                 remove or correct it: {entry}"
            ));
        }
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

/// Whether a test error is an infrastructure death (subprocess timeout,
/// crash, or unparseable output) rather than a produced wrong answer.
fn is_infra_death(msg: &str) -> bool {
    msg.contains("timed out")
        || msg.contains("Subprocess error:")
        || msg.contains("Subprocess produced no parseable output")
}
