//! The W3C RDF Dataset Canonicalization (RDFC-1.0) test suite, vendored in
//! `tests/rdf-canon` (see its README for the source commit).

use fluree_graph_format::canon::{
    canonicalize, CanonicalizeError, CanonicalizeOptions, HashAlgorithm,
};
use fluree_graph_ir::{Dataset, GraphCollectorSink};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The manifest's entry count at the vendored commit. A run that found
/// fewer has skipped tests, not passed them.
const TESTS: usize = 86;

fn suite() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/rdf-canon")
}

fn read(file: &str) -> String {
    let path = suite().join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn dataset(file: &str) -> Dataset {
    let mut sink = GraphCollectorSink::with_named_graphs();
    fluree_graph_turtle::parse_nquads(&read(file), &mut sink)
        .unwrap_or_else(|e| panic!("{file}: {e}"));
    sink.into_dataset()
}

/// What went wrong with one manifest entry, or `None`.
fn run(entry: &Value) -> Option<String> {
    let field = |name: &str| entry[name].as_str();
    let options = CanonicalizeOptions {
        hash: match field("hashAlgorithm") {
            None => HashAlgorithm::Sha256,
            Some("SHA384") => HashAlgorithm::Sha384,
            Some(other) => return Some(format!("unknown hashAlgorithm {other}")),
        },
        ..CanonicalizeOptions::default()
    };
    let outcome = canonicalize(&dataset(field("action")?), &options);
    match (field("type")?, outcome) {
        ("rdfc:RDFC10EvalTest", Ok(canonical)) => {
            let expected = read(field("result")?);
            (canonical.nquads != expected)
                .then(|| format!("expected:\n{expected}\ngot:\n{}", canonical.nquads))
        }
        ("rdfc:RDFC10MapTest", Ok(canonical)) => {
            let expected: BTreeMap<String, String> =
                serde_json::from_str(&read(field("result")?)).expect("issued map");
            let issued: BTreeMap<String, String> = canonical.issued.into_iter().collect();
            (issued != expected).then(|| format!("expected {expected:?}\ngot {issued:?}"))
        }
        ("rdfc:RDFC10NegativeEvalTest", Err(CanonicalizeError::TooComplex(_))) => None,
        ("rdfc:RDFC10NegativeEvalTest", Ok(_)) => Some("canonicalized; must be refused".into()),
        (kind, Err(error)) => Some(format!("{kind}: {error}")),
        (kind, Ok(_)) => Some(format!("unknown test type {kind}")),
    }
}

#[test]
fn w3c_rdfc10_suite_passes() {
    let manifest: Value = serde_json::from_str(&read("manifest.jsonld")).expect("manifest");
    let entries = manifest["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), TESTS, "the manifest's test count changed");

    let failures: Vec<String> = entries
        .iter()
        .filter_map(|entry| {
            let failure = run(entry).or_else(|| {
                (entry["action"].is_null() || entry["type"].is_null())
                    .then(|| "entry has no action or type".to_string())
            })?;
            Some(format!("{} ({}): {failure}", entry["id"], entry["name"]))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {TESTS} failed:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
