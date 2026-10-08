//! W3C RDF test suite registration.
//!
//! Runs the vendored `rdf-tests` RDF 1.1 and RDF 1.2 Turtle manifests
//! through the Turtle parser on the `GraphCollectorSink` path — the parser
//! behind `parse_to_json`, i.e. the conversion every JSON-LD write path
//! (upsert, graph sync, memory import) uses for Turtle input — and the RDF
//! 1.2 N-Triples, N-Quads and TriG manifests (each including its RDF 1.1
//! suite) through the parsers their ingest paths use. Same contract
//! as `w3c_sparql.rs`: a suite is green when every test passes or appears in
//! its register, and `check_testsuite` polices the register both ways.
//!
//! The `*_reader` suites run the same manifests through the standalone
//! readers: the strict N-Triples / N-Quads reader, and the Turtle parser's
//! conformant Turtle and TriG.

use anyhow::Result;
use testsuite_sparql::{check_rdf_reader_suite, check_testsuite};

mod registers;
use registers as reg;

/// RDF 1.1 Turtle: syntax + evaluation (the base every 1.2 suite includes).
#[test]
fn rdf11_turtle() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl",
        reg::RDF11_TURTLE,
    )
}

/// RDF 1.2 Turtle syntax: reified triples, annotations, `VERSION`,
/// base-direction language tags.
#[test]
fn rdf12_turtle_syntax() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax/manifest.ttl",
        reg::RDF12_TURTLE_SYNTAX,
    )
}

/// RDF 1.2 Turtle evaluation: star documents against N-Triples 1.2 graphs.
#[test]
fn rdf12_turtle_eval() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/eval/manifest.ttl",
        reg::RDF12_TURTLE_EVAL,
    )
}

/// RDF 1.2 N-Triples (with RDF 1.1): syntax and canonical form.
#[test]
fn rdf12_ntriples() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/manifest.ttl",
        reg::RDF12_NTRIPLES,
    )
}

/// RDF 1.2 N-Quads (with RDF 1.1): syntax and canonical form.
#[test]
fn rdf12_nquads() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-quads/manifest.ttl",
        reg::RDF12_NQUADS,
    )
}

/// RDF 1.2 TriG (with RDF 1.1): syntax and evaluation.
#[test]
fn rdf12_trig() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/manifest.ttl",
        reg::RDF12_TRIG,
    )
}

/// The standalone readers: conformant Turtle.
#[test]
fn rdf11_turtle_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl",
        reg::RDF11_TURTLE_READER,
    )
}

#[test]
fn rdf12_turtle_syntax_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax/manifest.ttl",
        reg::RDF12_TURTLE_SYNTAX_READER,
    )
}

#[test]
fn rdf12_turtle_eval_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/eval/manifest.ttl",
        reg::RDF12_TURTLE_EVAL_READER,
    )
}

/// The standalone readers: the strict line reader for N-Triples.
#[test]
fn rdf12_ntriples_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/manifest.ttl",
        reg::RDF12_NTRIPLES_READER,
    )
}

/// The standalone readers: the strict line reader for N-Quads.
#[test]
fn rdf12_nquads_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-quads/manifest.ttl",
        reg::RDF12_NQUADS_READER,
    )
}

/// The standalone readers: conformant TriG.
#[test]
fn rdf12_trig_reader() -> Result<()> {
    check_rdf_reader_suite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/manifest.ttl",
        reg::RDF12_TRIG_READER,
    )
}
