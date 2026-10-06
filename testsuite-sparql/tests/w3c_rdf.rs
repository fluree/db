//! W3C RDF (Turtle) test suite registration.
//!
//! Runs the vendored `rdf-tests` RDF 1.1 and RDF 1.2 Turtle manifests
//! through the Turtle parser on the `GraphCollectorSink` path — the parser
//! behind `parse_to_json`, i.e. the conversion every JSON-LD write path
//! (upsert, graph sync, memory import) uses for Turtle input. Same contract
//! as `w3c_sparql.rs`: a suite is green when every test passes or appears in
//! its register, and `check_testsuite` polices the register both ways.

use anyhow::Result;
use testsuite_sparql::check_testsuite;

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
