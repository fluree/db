//! The W3C JSON-LD 1.1 API expand and toRdf suites. A suite is green when
//! each test passes or appears in its register; see the crate docs for what
//! a pass means and `registers/mod.rs` for the entries.

use anyhow::Result;
use testsuite_jsonld::{check_suite, expand, to_rdf};

mod registers;

#[test]
fn jsonld_expand() -> Result<()> {
    check_suite("expand-manifest.jsonld", expand::run, registers::EXPAND)
}

#[test]
fn jsonld_to_rdf() -> Result<()> {
    check_suite("toRdf-manifest.jsonld", to_rdf::run, registers::TO_RDF)
}
