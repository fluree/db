//! W3C SHACL core test-suite runner.
//!
//! Env vars:
//! - `SHACL_CATEGORY=node`  — run one category only
//! - `SHACL_TEST=minLength-001` — run tests whose name contains the string
//! - `W3C_REPORT_JSON=report.json` — write a machine-readable report
//! - `SHACL_STRICT=1` — fail the test on any non-pass (default: report only)

use std::collections::BTreeMap;
use std::path::Path;

use testsuite_shacl::{collect_tests, run_case, Outcome};

#[test]
fn shacl_core_w3c_testsuite() {
    let manifest = Path::new("data-shapes/data-shapes-test-suite/tests/core/manifest.ttl");
    assert!(
        manifest.exists(),
        "data-shapes submodule missing — run: git submodule update --init testsuite-shacl/data-shapes"
    );

    let category_filter = std::env::var("SHACL_CATEGORY").ok();
    let test_filter = std::env::var("SHACL_TEST").ok();

    let cases = collect_tests(manifest).expect("manifest walk");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut per_category: BTreeMap<String, (usize, usize)> = BTreeMap::new(); // (passed, total)
    let mut failures: Vec<(String, String)> = Vec::new();
    let mut json_tests = Vec::new();
    let mut skipped = 0usize;

    for case in &cases {
        if let Some(cat) = &category_filter {
            if case.category != *cat {
                skipped += 1;
                continue;
            }
        }
        if let Some(pat) = &test_filter {
            if !case.name.contains(pat.as_str()) {
                skipped += 1;
                continue;
            }
        }

        let outcome = runtime.block_on(run_case(case));
        let entry = per_category.entry(case.category.clone()).or_default();
        entry.1 += 1;

        let (status, detail) = match &outcome {
            Outcome::Pass => {
                entry.0 += 1;
                ("PASS", String::new())
            }
            Outcome::Fail(reason) => ("FAIL", reason.clone()),
            Outcome::Error(reason) => ("ERROR", reason.clone()),
        };
        println!("{status:5} {}", case.name);
        if !outcome.passed() {
            println!("      {detail}");
            failures.push((case.name.clone(), detail.clone()));
        }
        json_tests.push(serde_json::json!({
            "name": case.name,
            "category": case.category,
            "approved": case.approved,
            "status": status,
            "detail": detail,
        }));
    }

    let total: usize = per_category.values().map(|(_, t)| t).sum();
    let passed: usize = per_category.values().map(|(p, _)| p).sum();

    println!();
    println!("=== W3C SHACL Core — Per-Category Breakdown ===");
    for (cat, (p, t)) in &per_category {
        println!("  {cat:20} {p:3}/{t:<3}");
    }
    println!();
    println!(
        "=== Test Summary === Total: {total}  Passed: {passed}  Failed: {}  Skipped: {skipped}  Rate: {:.1}%",
        total - passed,
        if total > 0 {
            passed as f64 * 100.0 / total as f64
        } else {
            0.0
        }
    );

    if let Ok(path) = std::env::var("W3C_REPORT_JSON") {
        let report = serde_json::json!({
            "total": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": format!("{:.1}%", if total > 0 { passed as f64 * 100.0 / total as f64 } else { 0.0 }),
            "tests": json_tests,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap())
            .expect("write report");
        println!("JSON report written to {path}");
    }

    if std::env::var("SHACL_STRICT").is_ok() && !failures.is_empty() {
        panic!("{} W3C SHACL test(s) failed", failures.len());
    }
}
