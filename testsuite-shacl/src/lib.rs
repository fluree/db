//! W3C SHACL test-suite harness.
//!
//! Walks the official `data-shapes` test manifests (vendored as a git
//! submodule under `data-shapes/`), runs each `sht:Validate` case through
//! the `fluree_db_api::validate` core (the same code path behind
//! `fluree validate`), and compares the produced report against the
//! expected `sh:ValidationReport` embedded in the manifest.
//!
//! ## Comparison semantics
//!
//! The W3C suite compares validation reports as RDF graphs. This harness
//! compares the *result multiset* on the fields implementations are judged
//! on — `sh:focusNode`, `sh:resultPath`, `sh:resultSeverity`,
//! `sh:sourceConstraintComponent`, `sh:value` — with deliberate leniency
//! where exact comparison needs machinery we don't have yet:
//!
//! - **Blank nodes** (focus nodes, values, complex `sh:resultPath`
//!   structures, `sh:sourceShape`): matched as wildcards rather than by
//!   graph isomorphism. `sh:sourceShape` is not compared at all.
//! - **Missing expected fields**: not compared (an expected result without
//!   `sh:value` accepts any actual value).
//!
//! `sh:resultMessage` is never compared (per the suite's own rules).

pub mod compare;
pub mod manifest;
pub mod runner;

pub use compare::{compare_report, Mismatch};
pub use manifest::{collect_tests, Expectation, ExpectedResult, TermPat, TestCase};
pub use runner::{run_case, Outcome};

/// Namespaces used by the manifests.
pub mod ns {
    pub const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    pub const SHT: &str = "http://www.w3.org/ns/shacl-test#";
    pub const SH: &str = "http://www.w3.org/ns/shacl#";
    pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
}

/// Turn a filesystem path into the `file://` IRI used as the Turtle
/// `@base`, so relative IRIs (including `<>` self-references) resolve
/// identically in the manifest graph, the loaded data, and the report.
pub fn file_iri(path: &std::path::Path) -> String {
    let abs = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    format!("file://{}", abs.display())
}
