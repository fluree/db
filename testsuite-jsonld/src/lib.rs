//! W3C JSON-LD 1.1 API test suite (expand, toRdf) against
//! `fluree-graph-json-ld`.
//!
//! [`check_suite`] runs one manifest and enforces its skip register both
//! ways, as `testsuite-sparql` does: a test that fails and is not registered
//! fails the suite, and so does a registered test that passes (remove the
//! entry in the change that fixes it) or a registered id the manifest no
//! longer has.
//!
//! What a pass means here:
//!
//! - Tests for JSON-LD 1.0 processors only (`specVersion: json-ld-1.0`) do
//!   not run: Fluree is a 1.1 processor. They are counted, not registered.
//! - A negative test passes when the input is refused. Fluree's errors carry
//!   no JSON-LD error codes, so which error is not compared.
//! - There is no document loader: a context named by IRI is not fetched.
//!
//! `JSONLD_TEST=<substring>` runs only the tests whose IRI contains it, and
//! prints each one's outcome in full.

pub mod expand;
pub mod iso;
pub mod manifest;
pub mod to_rdf;

use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;

use manifest::TestCase;

pub(crate) fn read_json(path: &Path) -> Result<Value> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

enum Outcome {
    Pass,
    Fail(String),
    Panic(String),
}

fn outcome(run: fn(&TestCase) -> Result<()>, test: &TestCase) -> Outcome {
    match catch_unwind(AssertUnwindSafe(|| run(test))) {
        Ok(Ok(())) => Outcome::Pass,
        Ok(Err(error)) => Outcome::Fail(format!("{error:#}")),
        Err(panic) => Outcome::Panic(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "panicked".to_string()),
        ),
    }
}

/// Run `tests/<manifest_file>` with `run`, against `register`.
pub fn check_suite(
    manifest_file: &str,
    run: fn(&TestCase) -> Result<()>,
    register: &[&str],
) -> Result<()> {
    let manifest = manifest::load(manifest_file)?;
    let only = std::env::var("JSONLD_TEST").ok();
    let registered: HashSet<&str> = register.iter().copied().collect();
    if registered.len() != register.len() {
        bail!("{manifest_file}: the register lists a test twice");
    }

    let (mut passed, mut excused, mut not_applicable) = (0, 0, 0);
    let mut problems = Vec::new();
    let mut seen = HashSet::new();

    // A panic is reported as the test's failure; keep the default hook from
    // printing each one as well.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for test in &manifest.tests {
        seen.insert(test.id.as_str());
        if only.as_deref().is_some_and(|o| !test.id.contains(o)) {
            continue;
        }
        if test.is_json_ld_10_only() {
            not_applicable += 1;
            continue;
        }
        let is_registered = registered.contains(test.id.as_str());
        let result = outcome(run, test);
        if only.is_some() {
            let detail = match &result {
                Outcome::Pass => "PASS".to_string(),
                Outcome::Fail(e) => format!("FAIL {e}"),
                Outcome::Panic(e) => format!("PANIC {e}"),
            };
            eprintln!("{} ({}): {detail}\n", test.id, test.name);
        }
        match (result, is_registered) {
            (Outcome::Pass, false) => passed += 1,
            (Outcome::Pass, true) => problems.push(format!(
                "{}: passes but is in the skip register; remove its entry",
                test.id
            )),
            (Outcome::Fail(_), true) => excused += 1,
            (Outcome::Fail(e), false) => problems.push(format!("{}: {e}", test.id)),
            // The register excuses a known wrong answer, not a crash.
            (Outcome::Panic(e), _) => problems.push(format!("{}: panicked: {e}", test.id)),
        }
    }
    std::panic::set_hook(hook);

    if only.is_none() {
        for id in register {
            if !seen.contains(id) {
                problems.push(format!("{id}: registered, but not in {manifest_file}"));
            }
        }
    }

    eprintln!(
        "{}: {passed} passed, {excused} registered failures, {not_applicable} for \
         JSON-LD 1.0 processors only (not run), {} problems",
        manifest.name,
        problems.len()
    );
    if problems.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{}", problems.join("\n\n")))
    }
}
