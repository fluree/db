//! Emit `conformance.json` — the artifact the benchmark lane gates on.
//!
//! `riot-analog-bench-strategy.md` §6b: a Tier-2 speed matrix refuses to
//! publish unless a conformance run exists at the same `git_sha` and the same
//! `rdf_tests_submodule_sha`. This binary produces that record.
//!
//! Usage: `cargo run --bin rdf-conformance [-- <output-path>]`
//! (default output: `conformance.json` in the current directory).
//!
//! Unlike `cargo test`, this runs with an EMPTY register: the artifact records
//! what the parser actually does, not what CI tolerates. A gated CI run and a
//! conformance artifact from the same commit will therefore report the same
//! `passed`, and differ only in how the remainder is labelled.

use anyhow::Result;
use testsuite_rdf::{
    files::rdf_tests_submodule_sha,
    report::{captured_at, git_sha, Conformance, DENOMINATOR_POLICY},
    run_suite, ParseMode, RDF11_NTRIPLES, RDF11_TURTLE, RDF12_NTRIPLES, RDF12_TURTLE,
};

fn main() -> Result<()> {
    let out = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "conformance.json".to_string());

    let suites = [
        (RDF11_TURTLE, ParseMode::Conformant),
        (RDF11_NTRIPLES, ParseMode::Conformant),
        (RDF11_TURTLE, ParseMode::IngestDefault),
        (RDF11_NTRIPLES, ParseMode::IngestDefault),
        (RDF12_TURTLE, ParseMode::Conformant),
        (RDF12_NTRIPLES, ParseMode::Conformant),
    ];

    let mut per_format = Vec::new();
    for (suite, mode) in suites {
        let run = run_suite(&suite, mode, &[])?;
        println!(
            "{:<8} {:<6} {:<15} total={:<4} passed={:<4} failed={:<4} rate={}{}",
            suite.spec,
            suite.format,
            mode.label(),
            run.summary.total,
            run.summary.passed,
            run.summary.failed,
            run.summary.pass_rate,
            if run.summary.gating {
                "  [GATING]"
            } else {
                "  [informational]"
            }
        );
        per_format.push(run.summary);
    }

    let doc = Conformance {
        git_sha: git_sha(),
        captured_at: captured_at(),
        suite: "w3c-rdf-syntax".to_string(),
        rdf_tests_submodule_sha: rdf_tests_submodule_sha(),
        per_format,
        denominator_policy: DENOMINATOR_POLICY.to_string(),
    };

    std::fs::write(&out, serde_json::to_string_pretty(&doc)?)?;
    println!("\nconformance.json written to {out}");
    Ok(())
}
