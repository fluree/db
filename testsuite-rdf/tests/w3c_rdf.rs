//! W3C RDF syntax suite registration.
//!
//! Every suite below runs unconditionally on `cargo test` — no `#[ignore]`.
//! What differs is whether the result GATES:
//!
//! - `*_conformant` tests gate. A suite is green when each test either passes
//!   or is registered in `tests/registers/mod.rs`, policed in both directions.
//! - `*_informational` tests never fail on a test outcome. They quantify
//!   deliberate distance: the ingest parser shape, and the RDF 1.2 suites the
//!   parser implements only the asserting subset of.
//!
//! See `README.md` for the denominator policy and the submodule coupling.

use anyhow::Result;
use testsuite_rdf::{
    check_testsuite, report_testsuite, ParseMode, RDF11_NQUADS, RDF11_NTRIPLES, RDF11_TRIG,
    RDF11_TURTLE, RDF12_NTRIPLES, RDF12_TURTLE,
};

mod registers;
use registers as reg;

// =============================================================================
// GATED: RDF 1.1, conformant parser mode
// =============================================================================

/// RDF 1.1 Turtle: positive/negative syntax, eval, negative eval.
#[test]
fn rdf11_turtle_conformant() -> Result<()> {
    check_testsuite(&RDF11_TURTLE, ParseMode::Conformant, reg::RDF11_TURTLE)
}

/// RDF 1.1 N-Triples, through the strict line reader (NOT the Turtle parser).
#[test]
fn rdf11_ntriples_conformant() -> Result<()> {
    check_testsuite(&RDF11_NTRIPLES, ParseMode::Conformant, reg::RDF11_NTRIPLES)
}

/// RDF 1.1 N-Quads, through the strict line reader.
#[test]
fn rdf11_nquads_conformant() -> Result<()> {
    check_testsuite(&RDF11_NQUADS, ParseMode::Conformant, reg::RDF11_NQUADS)
}

/// RDF 1.1 TriG: syntax and eval, the latter compared against N-Quads gold
/// files through the strict reader.
#[test]
fn rdf11_trig_conformant() -> Result<()> {
    check_testsuite(&RDF11_TRIG, ParseMode::Conformant, reg::RDF11_TRIG)
}

// =============================================================================
// INFORMATIONAL: the ingest parser shape
// =============================================================================

/// What the ingest defaults cost in RDF fidelity, in tests.
///
/// `ParserOptions::default` is deliberately lossy as RDF — object-position
/// collections become indexed list items instead of an `rdf:first`/`rdf:rest`
/// spine, `()` emits nothing, and numeric literals lose their lexical form.
/// This test does not judge that; it measures it, so the value of the
/// conformant preset is a number rather than an assertion.
///
/// It DOES gate one thing: the conformant mode must never score below the
/// ingest default. That direction is our own invariant, not W3C's — if it
/// inverted, `ParserOptions::conformant` would have stopped doing its job.
#[test]
fn rdf11_turtle_ingest_default_is_informational() -> Result<()> {
    eprintln!("\nCAVEAT: {}\n", reg::NEGATIVE_SYNTAX_BLIND_SPOT);

    let conformant = report_testsuite(
        &RDF11_TURTLE,
        ParseMode::Conformant,
        "conformant mode, unregistered — the raw parser result the gated run scores",
    )?;
    let ingest = report_testsuite(
        &RDF11_TURTLE,
        ParseMode::IngestDefault,
        "ingest defaults: indexed list items + canonicalized numerics. Failures here \
         are the DESIGN, not defects.",
    )?;

    eprintln!(
        "ParserOptions::conformant is worth {} Turtle test(s) ({:.1}% -> {:.1}%)\n",
        conformant.passed as i64 - ingest.passed as i64,
        ingest.pass_rate_pct(),
        conformant.pass_rate_pct(),
    );

    assert!(
        conformant.passed >= ingest.passed,
        "conformant mode ({} passed) scored BELOW the ingest default ({} passed) — \
         ParserOptions::conformant has regressed",
        conformant.passed,
        ingest.passed
    );

    Ok(())
}

// =============================================================================
// INFORMATIONAL: RDF 1.2
// =============================================================================

/// RDF 1.2 Turtle — asserting subset only, never gated.
///
/// The parser implements RDF-star's *asserting* forms by decision; the
/// non-asserting forms (`<<( … )>>` triple terms that state nothing) are a
/// documented non-goal. A pass rate here measures distance travelled toward
/// RDF 1.2, and must not be quoted as a conformance figure.
#[test]
fn rdf12_turtle_informational() -> Result<()> {
    report_testsuite(
        &RDF12_TURTLE,
        ParseMode::Conformant,
        "RDF 1.2 asserting subset only — non-asserting triple terms are a documented \
         non-goal. NOT a conformance claim.",
    )?;
    Ok(())
}

/// RDF 1.2 N-Triples — asserting subset only, never gated.
///
/// Carries the N-Triples caveat too (no strict N-Triples parser), so this is
/// the least load-bearing number the harness prints.
#[test]
fn rdf12_ntriples_informational() -> Result<()> {
    report_testsuite(
        &RDF12_NTRIPLES,
        ParseMode::Conformant,
        "RDF 1.2 asserting subset, read by the Turtle parser — two caveats deep. \
         NOT a conformance claim.",
    )?;
    Ok(())
}
