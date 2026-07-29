//! W3C RDF **syntax** conformance harness (`rdf-tests`, `rdf/` tree).
//!
//! Sibling of `testsuite-sparql`, which gates the query engine; this one gates
//! the readers — the Turtle parser and the graph IR it emits into. It depends
//! on `fluree-graph-turtle` and `fluree-graph-ir` only: no database, no
//! runtime, no `fluree-db-api`.
//!
//! # What runs, and what gates
//!
//! Every registered suite runs on every `cargo test`. A suite is green when
//! each of its tests either passes or appears in that suite's register
//! (`tests/registers/mod.rs`), and [`check_testsuite`] polices the register in
//! **both** directions — an unregistered failure and a registered pass both
//! fail the suite. That is the same contract testsuite-sparql operates under,
//! and it is what makes the register a shrinking burn-down list rather than a
//! place failures go to be forgotten.
//!
//! Gating is narrower than running:
//!
//! - **RDF 1.1 Turtle and N-Triples, [`ParseMode::Conformant`]** — gates.
//! - **RDF 1.2** — reported, never gates. The parser implements the
//!   *asserting* subset of RDF-star deliberately; non-asserting forms are a
//!   documented non-goal, so an RDF 1.2 pass rate is information about
//!   distance travelled, not a bar to clear.
//! - **[`ParseMode::IngestDefault`]** — reported, never gates. The ingest
//!   shape is deliberately lossy as RDF (indexed list items instead of an
//!   `rdf:first`/`rdf:rest` spine, canonicalized numeric literals), so its
//!   failures are the *design*, and quantifying them is the point: the
//!   delta between the two modes is exactly what `ParserOptions` bought.
//!
//! # N-Triples has no dedicated parser (harness limitation, not a lie)
//!
//! N-Triples is a syntactic subset of Turtle, so the N-Triples suites run
//! through the Turtle parser. Positive-syntax tests are therefore meaningful.
//! Negative-syntax tests are only meaningful where the construct is invalid in
//! *Turtle too*: a document using `@prefix`, a prefixed name, a bare integer
//! or a relative IRI is invalid N-Triples but perfectly valid Turtle, and the
//! Turtle parser accepts it, so those tests fail and are registered under that
//! cause. Closing them needs a strict N-Triples mode, not a parser bug fix.
//! Until then the N-Triples pass rate must be read with that caveat attached —
//! it is stated in every report the harness writes.

pub mod files;
pub mod isomorphism;
pub mod manifest;
pub mod report;
pub mod runner;
pub mod vocab;

use anyhow::{bail, Result};
use std::collections::HashSet;

use manifest::TestManifest;
use report::{suite_report, SuiteReport, TestEntry};
use runner::run_test;
pub use runner::ParseMode;

/// A suite run: the aggregate plus the per-test detail.
#[derive(Debug)]
pub struct SuiteRun {
    pub summary: SuiteReport,
    pub entries: Vec<TestEntry>,
    /// Human-readable failure descriptions, in discovery order.
    pub failures: Vec<String>,
    /// Problems with the register itself (entries naming no discovered test).
    /// Kept out of `failures` so the report's count identity holds — see the
    /// note in `run_suite`.
    pub register_errors: Vec<String>,
}

/// Descriptor for one gated or informational suite.
#[derive(Clone, Copy, Debug)]
pub struct Suite {
    /// Short format label used in reports and bench matrices.
    pub format: &'static str,
    /// `rdf11` | `rdf12`.
    pub spec: &'static str,
    /// Manifest roots to run, in order. Usually one; the RDF 1.2 suites take
    /// several because their published root manifest is a COMPOSITE — it
    /// `mf:include`s `../../rdf11/...`, so running it would fold the RDF 1.1
    /// results into the RDF 1.2 number and report a figure that is neither.
    /// Naming the RDF 1.2-only sub-manifests keeps each number about one
    /// spec.
    pub manifests: &'static [&'static str],
    /// Exactly how many tests this suite must discover.
    ///
    /// A pass RATE cannot notice a shrinking denominator: drop half the
    /// manifest and the survivors still score 100%. The zero-test guard only
    /// catches total loss, and the realistic failure is PARTIAL — a spine
    /// walk that stops early, a manifest restructure, a half-initialized
    /// submodule. So the count is pinned exactly.
    ///
    /// It doubles as the submodule-drift tripwire. When rdf-tests legitimately
    /// updates, this assert is what fails, and its message says to re-count
    /// and update the number in the same commit that moves the submodule.
    pub expected_total: usize,
}

/// RDF 1.1 Turtle.
pub const RDF11_TURTLE: Suite = Suite {
    format: "turtle",
    spec: "rdf11",
    manifests: &["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl"],
    expected_total: 313,
};

/// RDF 1.1 N-Triples.
pub const RDF11_NTRIPLES: Suite = Suite {
    format: "ntriples",
    spec: "rdf11",
    manifests: &["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl"],
    expected_total: 70,
};

/// RDF 1.1 TriG.
///
/// Syntax tests gate; the 147 eval tests are registered pending an N-Quads
/// reader, since they compare against `.nq` gold files.
pub const RDF11_TRIG: Suite = Suite {
    format: "trig",
    spec: "rdf11",
    manifests: &["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl"],
    expected_total: 356,
};

/// RDF 1.2 Turtle — informational (asserting subset only), RDF 1.2 tests only.
pub const RDF12_TURTLE: Suite = Suite {
    format: "turtle",
    spec: "rdf12",
    manifests: &[
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/eval/manifest.ttl",
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax/manifest.ttl",
    ],
    expected_total: 96,
};

/// RDF 1.2 N-Triples — informational (asserting subset only), RDF 1.2 tests
/// only.
///
/// Includes the `c14n` sub-manifest, whose tests all fail on "no canonical
/// N-Triples writer exists" — that is the honest state, and it is why this
/// number is the lowest the harness prints.
pub const RDF12_NTRIPLES: Suite = Suite {
    format: "ntriples",
    spec: "rdf12",
    manifests: &[
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/c14n/manifest.ttl",
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax/manifest.ttl",
    ],
    expected_total: 70,
};

/// Whether a (suite, mode) pair gates CI. See the module docs.
pub fn is_gating(suite: &Suite, mode: ParseMode) -> bool {
    suite.spec == "rdf11" && mode == ParseMode::Conformant
}

/// Run one suite, classifying every test against `register`.
///
/// This never fails on test outcomes — it *reports*. [`check_testsuite`] is
/// the gating wrapper.
pub fn run_suite(suite: &Suite, mode: ParseMode, register: &[&str]) -> Result<SuiteRun> {
    files::ensure_rdf_tests_checkout()?;

    let mut entries = Vec::new();
    let mut failures = Vec::new();
    // Register-integrity problems are NOT test outcomes: nothing ran, so
    // folding them into `failed` would break the report's
    // total = passed + failed + ignored identity precisely when a run is
    // already failing. They are tracked apart and surfaced apart.
    let mut register_errors: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let (mut passed, mut ignored, mut total) = (0usize, 0usize, 0usize);

    for test in TestManifest::new(suite.manifests.iter().copied()) {
        let test = test?;
        total += 1;
        seen.insert(test.id.clone());

        let outcome = run_test(&test, mode);
        let registered = register.contains(&test.id.as_str());

        let (status, error) = match outcome {
            Ok(()) if registered => {
                failures.push(format!(
                    "{}: unexpectedly PASSED but is in the register — remove its \
                     entry (stale entries hide regressions)",
                    test.id
                ));
                ("unexpected-pass", None)
            }
            Ok(()) => {
                passed += 1;
                ("pass", None)
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if registered {
                    ignored += 1;
                    ("ignored", Some(msg))
                } else {
                    failures.push(format!("{}: {msg}", test.id));
                    ("fail", Some(msg))
                }
            }
        };

        entries.push(TestEntry {
            test_id: test.id,
            status: status.to_string(),
            error,
        });
    }

    // A manifest that resolves but yields nothing means coverage vanished
    // (submodule restructure, partial checkout, manifest-parser regression) —
    // that must never report green.
    if total == 0 {
        bail!(
            "Manifest yielded ZERO tests — coverage silently lost \
             (submodule drift or manifest-parse regression?): {}",
            suite.manifests.join(", ")
        );
    }

    // The partial-loss guard. See `Suite::expected_total`.
    if total != suite.expected_total {
        bail!(
            "{} {} discovered {total} tests, expected exactly {}.\n\
             If the rdf-tests submodule was updated on purpose, re-count the \
             suite and update `expected_total` for {} {} in the SAME commit \
             that moves the submodule. Otherwise coverage was silently lost \
             (partial manifest walk, half-initialized submodule): {}",
            suite.spec,
            suite.format,
            suite.expected_total,
            suite.spec,
            suite.format,
            suite.manifests.join(", ")
        );
    }

    // A register entry matching no discovered test (typo, upstream rename,
    // withdrawn test) would otherwise live forever, overstating the register.
    for entry in register {
        if !seen.contains(*entry) {
            register_errors.push(format!(
                "register entry matches no test discovered in this suite — \
                 remove or correct it: {entry}"
            ));
        }
    }

    let failed = failures.len();
    let summary = suite_report(
        suite.format,
        suite.spec,
        mode.label(),
        is_gating(suite, mode),
        &suite.manifests.join(", "),
        total,
        passed,
        failed,
        ignored,
    );

    Ok(SuiteRun {
        summary,
        entries,
        failures,
        register_errors,
    })
}

/// Run a suite and fail the test on any unexpected failure or stale register
/// entry.
///
/// Set `W3C_RDF_REPORT_JSON` to also write the per-test JSON report. Use
/// `--test-threads=1` when doing so, since every suite writes to that one
/// path.
pub fn check_testsuite(suite: &Suite, mode: ParseMode, register: &[&str]) -> Result<()> {
    let run = run_suite(suite, mode, register)?;
    let s = &run.summary;

    eprintln!(
        "\n=== {} {} [{}] ===\n\
         Total:   {}\n\
         Passed:  {}\n\
         Ignored: {}\n\
         Failed:  {}\n\
         Rate:    {} (registered failures count against it)\n",
        s.spec, s.format, s.mode, s.total, s.passed, s.ignored, s.failed, s.pass_rate
    );

    debug_assert_eq!(
        s.total,
        s.passed + s.failed + s.ignored,
        "report counts must satisfy total = passed + failed + ignored"
    );

    if let Ok(path) = std::env::var("W3C_RDF_REPORT_JSON") {
        report::write_json_report(&path, s, &run.entries)?;
    }

    if !run.failures.is_empty() || !run.register_errors.is_empty() {
        let mut sections = Vec::new();
        if !run.failures.is_empty() {
            sections.push(format!(
                "{} failing test(s):\n\n{}",
                run.failures.len(),
                run.failures.join("\n\n")
            ));
        }
        if !run.register_errors.is_empty() {
            sections.push(format!(
                "{} register problem(s) (not test failures — nothing ran):\n\n{}",
                run.register_errors.len(),
                run.register_errors.join("\n\n")
            ));
        }
        bail!(
            "{} {} [{}]:\n\n{}",
            s.spec,
            s.format,
            s.mode,
            sections.join("\n\n")
        );
    }

    Ok(())
}

/// Run a suite for information only: print the numbers, never fail.
///
/// Used for RDF 1.2 and for the ingest-default mode, whose failures are known
/// design consequences rather than defects.
pub fn report_testsuite(suite: &Suite, mode: ParseMode, note: &str) -> Result<SuiteReport> {
    let run = run_suite(suite, mode, &[])?;
    let s = &run.summary;

    eprintln!(
        "\n=== [INFORMATIONAL, NOT GATED] {} {} [{}] ===\n\
         {note}\n\
         Total: {}  Passed: {}  Failed: {}  Rate: {}\n",
        s.spec, s.format, s.mode, s.total, s.passed, s.failed, s.pass_rate
    );

    Ok(run.summary.clone())
}

/// Tests of the harness itself.
///
/// The register mechanism is the reason a green suite means anything, so it
/// gets tested rather than trusted: if it silently stopped enforcing, every
/// suite would keep reporting green while the parser rotted.
#[cfg(test)]
mod tests {
    use super::*;

    /// A test in the register that PASSES must fail the suite — otherwise the
    /// register accumulates stale entries that mask later regressions.
    #[test]
    fn a_registered_test_that_passes_fails_the_suite() {
        // #IRI_subject passes today (the simplest eval test in the suite).
        let stale =
            ["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#IRI_subject"];
        let err = check_testsuite(&RDF11_TURTLE, ParseMode::Conformant, &stale)
            .expect_err("a stale register entry must fail the suite");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unexpectedly PASSED"),
            "expected a stale-entry diagnosis, got: {msg}"
        );
    }

    /// A failing test NOT in the register must fail the suite.
    #[test]
    fn an_unregistered_failure_fails_the_suite() {
        let err = check_testsuite(&RDF11_TURTLE, ParseMode::Conformant, &[])
            .expect_err("the known failures must fail an empty register");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failing test(s)"),
            "expected a failure list, got: {msg}"
        );
    }

    /// A register entry naming no discovered test must be reported, not
    /// silently carried (upstream rename, typo, withdrawn test).
    #[test]
    fn a_register_entry_matching_nothing_is_reported() {
        let bogus =
            ["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#no_such_test"];
        let err = check_testsuite(&RDF11_TURTLE, ParseMode::Conformant, &bogus)
            .expect_err("a register entry matching no test must fail the suite");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("matches no test discovered"),
            "expected a dangling-entry diagnosis, got: {msg}"
        );
    }

    /// A manifest that yields nothing must be an error, never a vacuous pass.
    #[test]
    fn an_empty_suite_is_an_error_not_a_green_run() {
        let empty = Suite {
            format: "turtle",
            spec: "rdf11",
            manifests: &["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/IRI_subject.ttl"],
            expected_total: 313,
        };
        let err = run_suite(&empty, ParseMode::Conformant, &[])
            .expect_err("a manifest with no entries must not report green");
        assert!(format!("{err:#}").contains("ZERO tests"));
    }

    /// A suite that discovers the wrong NUMBER of tests must fail, even when
    /// every test it did discover passed. A pass rate cannot see its own
    /// denominator shrink.
    #[test]
    fn a_partial_manifest_walk_fails_even_when_everything_passes() {
        let short = Suite {
            expected_total: RDF11_TURTLE.expected_total + 1,
            ..RDF11_TURTLE
        };
        let err = run_suite(&short, ParseMode::Conformant, &[])
            .expect_err("a wrong discovery count must fail the suite");
        let msg = format!("{err:#}");
        assert!(msg.contains("discovered 313 tests, expected exactly 314"));
        // The message has to say what to do when the submodule moved legitimately.
        assert!(msg.contains("SAME commit"));
    }

    /// A dangling register entry is a register problem, not a test failure:
    /// it must be reported, but it must not be counted in `failed`, or the
    /// report's total = passed + failed + ignored identity breaks exactly
    /// when a run is already failing.
    #[test]
    fn a_dangling_register_entry_does_not_inflate_the_failure_count() {
        let bogus =
            ["https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#no_such_test"];
        let run = run_suite(&RDF11_TURTLE, ParseMode::Conformant, &bogus).unwrap();

        assert_eq!(run.register_errors.len(), 1);
        assert!(run.register_errors[0].contains("matches no test discovered"));
        assert!(
            !run.failures.iter().any(|f| f.contains("no_such_test")),
            "register problems must stay out of the failure list"
        );

        let s = &run.summary;
        assert_eq!(
            s.total,
            s.passed + s.failed + s.ignored,
            "count identity must hold even with a dangling register entry"
        );
    }

    /// Only RDF 1.1 in conformant mode may be quoted as conformance.
    #[test]
    fn gating_is_rdf11_conformant_only() {
        assert!(is_gating(&RDF11_TURTLE, ParseMode::Conformant));
        assert!(is_gating(&RDF11_NTRIPLES, ParseMode::Conformant));
        assert!(!is_gating(&RDF11_TURTLE, ParseMode::IngestDefault));
        assert!(!is_gating(&RDF12_TURTLE, ParseMode::Conformant));
        assert!(!is_gating(&RDF12_NTRIPLES, ParseMode::Conformant));
    }
}
