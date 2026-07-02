//! Manifest walking: turn the W3C `data-shapes` manifest tree into test cases.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use fluree_graph_ir::{Graph, GraphCollectorSink, Term};
use fluree_graph_turtle::parse;
use serde_json::{json, Value as JsonValue};

use crate::{file_iri, ns};

/// One `sht:Validate` test case.
#[derive(Debug)]
pub struct TestCase {
    /// Entry IRI (resolved against the manifest file).
    pub id: String,
    /// Short display name, e.g. `node/minLength-001`.
    pub name: String,
    /// Category directory, e.g. `node`.
    pub category: String,
    /// `rdfs:label` when present.
    pub label: Option<String>,
    /// `mf:status` is `sht:approved`.
    pub approved: bool,
    /// Data graph file.
    pub data_path: PathBuf,
    /// Shapes graph file.
    pub shapes_path: PathBuf,
    /// Expected outcome.
    pub expect: Expectation,
}

/// Expected outcome of a test case.
#[derive(Debug)]
pub enum Expectation {
    /// A validation report with the given conformance and results.
    Report {
        conforms: bool,
        results: Vec<ExpectedResult>,
    },
    /// Validation itself is expected to fail (`mf:result sht:Failure`).
    Failure,
}

/// One expected `sh:ValidationResult`, reduced to comparable patterns.
#[derive(Debug)]
pub struct ExpectedResult {
    pub focus: TermPat,
    pub path: TermPat,
    pub severity: Option<String>,
    pub component: Option<String>,
    pub value: TermPat,
}

/// A comparable pattern for one expected term.
#[derive(Debug, Clone, PartialEq)]
pub enum TermPat {
    /// The field is absent in the expected result — accept anything.
    Absent,
    /// A blank node (or other structure we match leniently) — accept anything.
    Any,
    /// Concrete term in the same JSON form the report core emits.
    Json(JsonValue),
}

/// Recursively collect all `sht:Validate` cases reachable from a manifest.
pub fn collect_tests(manifest_path: &Path) -> Result<Vec<TestCase>> {
    let mut out = Vec::new();
    walk(manifest_path, &mut out)?;
    Ok(out)
}

fn walk(manifest_path: &Path, out: &mut Vec<TestCase>) -> Result<()> {
    let base = file_iri(manifest_path);
    let raw = std::fs::read_to_string(manifest_path)
        .with_context(|| format!("reading manifest {}", manifest_path.display()))?;
    let content = format!("@base <{base}> .\n{raw}");

    let mut sink = GraphCollectorSink::new();
    parse(&content, &mut sink)
        .with_context(|| format!("parsing manifest {}", manifest_path.display()))?;
    let graph = sink.finish();

    let manifest_subject = find_manifest_subject(&graph, &base)
        .ok_or_else(|| anyhow!("no mf:Manifest subject in {}", manifest_path.display()))?;

    // Sub-manifests
    let mf_include = format!("{}include", ns::MF);
    for include in list_items(&graph, &manifest_subject, &mf_include) {
        let path = iri_to_path(&include)
            .ok_or_else(|| anyhow!("mf:include is not a file IRI: {include}"))?;
        walk(&path, out)?;
    }

    // Entries in this manifest
    let mf_entries = format!("{}entries", ns::MF);
    for entry in list_items(&graph, &manifest_subject, &mf_entries) {
        if let Some(case) = parse_entry(&graph, &entry, manifest_path)
            .with_context(|| format!("entry {entry} in {}", manifest_path.display()))?
        {
            out.push(case);
        }
    }
    Ok(())
}

fn parse_entry(graph: &Graph, entry_iri: &str, manifest_path: &Path) -> Result<Option<TestCase>> {
    let entry = Term::iri(entry_iri);

    // Only sht:Validate entries are runnable validation tests.
    let sht_validate = format!("{}Validate", ns::SHT);
    let is_validate = objects_for(graph, &entry, ns::RDF_TYPE)
        .iter()
        .any(|t| t.as_iri() == Some(sht_validate.as_str()));
    if !is_validate {
        return Ok(None);
    }

    let label = object_for(graph, &entry, "http://www.w3.org/2000/01/rdf-schema#label")
        .and_then(|t| t.as_literal().map(|(v, _, _)| v.lexical()));

    let approved = object_for(graph, &entry, &format!("{}status", ns::MF))
        .and_then(|t| t.as_iri().map(String::from))
        .is_some_and(|iri| iri == format!("{}approved", ns::SHT));

    // mf:action → sht:dataGraph / sht:shapesGraph
    let action = object_for(graph, &entry, &format!("{}action", ns::MF))
        .ok_or_else(|| anyhow!("entry has no mf:action"))?
        .clone();
    let data_iri = object_for(graph, &action, &format!("{}dataGraph", ns::SHT))
        .and_then(|t| t.as_iri().map(String::from))
        .ok_or_else(|| anyhow!("mf:action has no sht:dataGraph"))?;
    let shapes_iri = object_for(graph, &action, &format!("{}shapesGraph", ns::SHT))
        .and_then(|t| t.as_iri().map(String::from))
        .ok_or_else(|| anyhow!("mf:action has no sht:shapesGraph"))?;
    let data_path = iri_to_path(&data_iri).ok_or_else(|| anyhow!("dataGraph is not a file IRI"))?;
    let shapes_path =
        iri_to_path(&shapes_iri).ok_or_else(|| anyhow!("shapesGraph is not a file IRI"))?;

    // mf:result → sht:Failure or an inline sh:ValidationReport
    let result = object_for(graph, &entry, &format!("{}result", ns::MF))
        .ok_or_else(|| anyhow!("entry has no mf:result"))?
        .clone();
    let expect = if result.as_iri() == Some(format!("{}Failure", ns::SHT).as_str()) {
        Expectation::Failure
    } else {
        let conforms = object_for(graph, &result, &format!("{}conforms", ns::SH))
            .and_then(|t| t.as_literal().map(|(v, _, _)| v.lexical() == "true"))
            .ok_or_else(|| anyhow!("expected report has no sh:conforms"))?;
        let results = objects_for(graph, &result, &format!("{}result", ns::SH))
            .into_iter()
            .map(|r| parse_expected_result(graph, r))
            .collect();
        Expectation::Report { conforms, results }
    };

    // node/minLength-001.ttl → category "node", name "node/minLength-001"
    let category = manifest_path
        .parent()
        .and_then(|p| p.file_name())
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let stem = manifest_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    Ok(Some(TestCase {
        id: entry_iri.to_string(),
        name: format!("{category}/{stem}"),
        category,
        label,
        approved,
        data_path,
        shapes_path,
        expect,
    }))
}

fn parse_expected_result(graph: &Graph, result: &Term) -> ExpectedResult {
    let get = |local: &str| object_for(graph, result, &format!("{}{local}", ns::SH));
    ExpectedResult {
        focus: term_pat(get("focusNode")),
        path: term_pat(get("resultPath")),
        severity: get("resultSeverity").and_then(|t| t.as_iri().map(String::from)),
        component: get("sourceConstraintComponent").and_then(|t| t.as_iri().map(String::from)),
        value: term_pat(get("value")),
    }
}

/// Reduce an expected term to a comparable pattern in the same JSON form
/// the report core emits for `sh:value` / focus nodes.
fn term_pat(term: Option<&Term>) -> TermPat {
    match term {
        None => TermPat::Absent,
        Some(Term::BlankNode(_)) => TermPat::Any,
        Some(Term::Iri(iri)) => TermPat::Json(json!({"@id": iri.to_string()})),
        Some(Term::Literal {
            value,
            datatype,
            language,
        }) => TermPat::Json(literal_json(value, datatype.as_iri(), language.as_deref())),
    }
}

/// Mirror of the report core's `value_json` emission rules: JSON-native XSD
/// types render as bare scalars, language-tagged literals as
/// `{"@value", "@language"}`, everything else as `{"@value", "@type"}`.
fn literal_json(
    value: &fluree_graph_ir::LiteralValue,
    dt_iri: &str,
    lang: Option<&str>,
) -> JsonValue {
    use fluree_graph_ir::LiteralValue as LV;
    if let Some(lang) = lang {
        return json!({"@value": value.lexical(), "@language": lang});
    }
    match (value, dt_iri.strip_prefix(ns::XSD)) {
        (LV::String(s), Some("string")) => json!(s.to_string()),
        (LV::Boolean(b), Some("boolean")) => json!(b),
        (LV::Integer(i), Some("integer")) => json!(i),
        (LV::Double(d), Some("double")) => json!(d),
        _ => json!({"@value": value.lexical(), "@type": dt_iri}),
    }
}

// ---------------------------------------------------------------------------
// Graph helpers
// ---------------------------------------------------------------------------

fn find_manifest_subject(graph: &Graph, base: &str) -> Option<Term> {
    let mf_manifest = format!("{}Manifest", ns::MF);
    for t in graph.iter() {
        if t.p.as_iri() == Some(ns::RDF_TYPE) && t.o.as_iri() == Some(mf_manifest.as_str()) {
            return Some(t.s.clone());
        }
    }
    let mf_entries = format!("{}entries", ns::MF);
    for t in graph.iter() {
        if t.p.as_iri() == Some(mf_entries.as_str()) {
            return Some(t.s.clone());
        }
    }
    Some(Term::iri(base))
}

fn object_for<'g>(graph: &'g Graph, subject: &Term, predicate: &str) -> Option<&'g Term> {
    graph
        .iter()
        .find(|t| t.s == *subject && t.p.as_iri() == Some(predicate))
        .map(|t| &t.o)
}

fn objects_for<'g>(graph: &'g Graph, subject: &Term, predicate: &str) -> Vec<&'g Term> {
    graph
        .iter()
        .filter(|t| t.s == *subject && t.p.as_iri() == Some(predicate))
        .map(|t| &t.o)
        .collect()
}

/// Ordered items of an object-position collection. Fluree's Turtle parser
/// emits list members as triples carrying `list_index` (not rdf:first/rest).
fn list_items(graph: &Graph, subject: &Term, predicate: &str) -> Vec<String> {
    let mut items: Vec<(i32, String)> = graph
        .iter()
        .filter(|t| t.s == *subject && t.p.as_iri() == Some(predicate))
        .filter_map(|t| {
            let iri = t.o.as_iri()?;
            Some((t.list_index.unwrap_or(0), iri.to_string()))
        })
        .collect();
    items.sort_by_key(|(i, _)| *i);
    items.into_iter().map(|(_, v)| v).collect()
}

fn iri_to_path(iri: &str) -> Option<PathBuf> {
    iri.strip_prefix("file://").map(PathBuf::from)
}
