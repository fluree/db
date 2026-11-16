//! The json-ld-api test manifests (`tests/*-manifest.jsonld`).
//!
//! A manifest is a JSON-LD document; its entries are read as plain JSON, as
//! every json-ld-api runner does, since the manifest's own context is fixed.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::{Map, Value};

/// Where the vendored suite's `tests/` directory is.
pub fn tests_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("json-ld-api/tests")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The output must match `expect`.
    Positive,
    /// Processing must fail.
    Negative,
    /// Processing must succeed; the output is not compared.
    PositiveSyntax,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    /// The test's full IRI, the key in the skip register.
    pub id: String,
    pub name: String,
    pub kind: Kind,
    /// The input document's IRI, its base unless `option.base` says otherwise.
    pub input_iri: String,
    pub input: PathBuf,
    pub expect: Option<PathBuf>,
    pub expect_error_code: Option<String>,
    pub option: Map<String, Value>,
}

impl TestCase {
    fn option_str(&self, key: &str) -> Option<&str> {
        self.option.get(key).and_then(Value::as_str)
    }

    /// The document base: `option.base`, else the input's own IRI.
    pub fn base(&self) -> &str {
        self.option_str("base").unwrap_or(&self.input_iri)
    }

    /// `option.expandContext`, a path relative to the manifest.
    pub fn expand_context(&self) -> Option<PathBuf> {
        self.option_str("expandContext")
            .map(|p| tests_dir().join(p))
    }

    /// Whether the test is written for a JSON-LD 1.0 processor only.
    /// Fluree is a 1.1 processor, so these do not apply to it.
    pub fn is_json_ld_10_only(&self) -> bool {
        self.option_str("specVersion") == Some("json-ld-1.0")
    }
}

pub struct Manifest {
    pub name: String,
    pub tests: Vec<TestCase>,
}

/// Read `tests/<file>`, e.g. `expand-manifest.jsonld`.
pub fn load(file: &str) -> Result<Manifest> {
    let path = tests_dir().join(file);
    let text = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "reading {} (is the json-ld-api submodule checked out? \
             run `git submodule update --init testsuite-jsonld/json-ld-api`)",
            path.display()
        )
    })?;
    let doc: Value = serde_json::from_str(&text)?;
    let base_iri = doc["baseIri"]
        .as_str()
        .context("manifest has no baseIri")?
        .to_string();
    let manifest_iri = format!("{base_iri}{}", file.trim_end_matches(".jsonld"));
    let name = doc["name"].as_str().unwrap_or(file).to_string();

    let tests = doc["sequence"]
        .as_array()
        .context("manifest has no sequence")?
        .iter()
        .map(|entry| test_case(entry, &base_iri, &manifest_iri))
        .collect::<Result<_>>()?;
    Ok(Manifest { name, tests })
}

fn test_case(entry: &Value, base_iri: &str, manifest_iri: &str) -> Result<TestCase> {
    let str_of = |key: &str| entry.get(key).and_then(Value::as_str);
    let fragment = str_of("@id").context("test has no @id")?;
    let types: Vec<&str> = match &entry["@type"] {
        Value::Array(types) => types.iter().filter_map(Value::as_str).collect(),
        Value::String(t) => vec![t.as_str()],
        _ => vec![],
    };
    let kind = if types.contains(&"jld:PositiveEvaluationTest") {
        Kind::Positive
    } else if types.contains(&"jld:NegativeEvaluationTest") {
        Kind::Negative
    } else if types.contains(&"jld:PositiveSyntaxTest") {
        Kind::PositiveSyntax
    } else {
        bail!("{fragment}: unknown test type {types:?}");
    };
    let input = str_of("input").context("test has no input")?;
    Ok(TestCase {
        id: format!("{manifest_iri}{fragment}"),
        name: str_of("name").unwrap_or_default().to_string(),
        kind,
        input_iri: format!("{base_iri}{input}"),
        input: tests_dir().join(input),
        expect: str_of("expect").map(|p| tests_dir().join(p)),
        expect_error_code: str_of("expectErrorCode").map(str::to_string),
        option: entry
            .get("option")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
    })
}
