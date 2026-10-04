//! Engine values to and from plain Python data.
//!
//! SPARQL terms cross the boundary as small tuples that `fluree/_terms.py`
//! turns into Python values: `("iri", iri)`, `("bnode", label)`, and
//! `("literal", lexical, datatype_iri, language)`. Unbound cells are `None`.

use crate::error::{fluree_error, invalid_request};
use fluree_db_api::{CommitRef, ResolvedFlake, ResolvedValue, TimeSpec};
use fluree_db_core::{CommitSummary, ContentId};
use fluree_db_sparql::ast::{QueryBody, SelectVariables};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use serde_json::Value as JsonValue;

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

pub(crate) fn to_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    Ok(pythonize::depythonize(obj)?)
}

pub(crate) fn from_json<'py>(py: Python<'py>, value: &JsonValue) -> PyResult<Bound<'py, PyAny>> {
    Ok(pythonize::pythonize(py, value)?)
}

/// `("t", 5)`, `("time", "2026-01-01T00:00:00Z")`, `("commit", "bafy...")`,
/// or `None` for the latest state.
pub(crate) fn time_spec(at: Option<&Bound<'_, PyTuple>>) -> PyResult<TimeSpec> {
    let Some(at) = at else {
        return Ok(TimeSpec::Latest);
    };
    let (kind, value): (String, Bound<'_, PyAny>) = at.extract()?;
    Ok(match kind.as_str() {
        "t" => TimeSpec::AtT(value.extract()?),
        "time" => TimeSpec::AtTime(value.extract()?),
        "commit" => TimeSpec::AtCommit(value.extract()?),
        other => return Err(invalid_request(format!("unknown time kind {other:?}"))),
    })
}

/// A commit named by its `t` (an int), its full id, or a prefix of its hex
/// digest. Only a canonical CID counts as an id, so a digest is never misread
/// as one.
pub(crate) fn commit_ref(commit: &Bound<'_, PyAny>) -> PyResult<CommitRef> {
    if let Ok(t) = commit.extract::<i64>() {
        return Ok(CommitRef::T(t));
    }
    let text: String = commit.extract()?;
    Ok(match ContentId::parse_canonical(&text) {
        Some(cid) => CommitRef::Exact(cid),
        None => CommitRef::Prefix(text),
    })
}

/// A SPARQL result: the engine's SPARQL results JSON (JSON-LD for CONSTRUCT
/// and DESCRIBE), plus what the query text says about its shape.
pub(crate) struct SparqlResult {
    json: JsonValue,
    construct: bool,
    /// Column names in projection order; `None` for `SELECT *`.
    columns: Option<Vec<String>>,
}

impl SparqlResult {
    /// SPARQL results JSON orders its head by name, but rows unpack
    /// positionally, so they take the order the query text projects. The
    /// engine has already accepted `sparql` when this runs.
    pub(crate) fn new(sparql: &str, json: JsonValue) -> Self {
        let body = fluree_db_sparql::parse_sparql(sparql)
            .ast
            .map(|ast| ast.body);
        let construct = matches!(body, Some(QueryBody::Construct(_) | QueryBody::Describe(_)));
        Self {
            json,
            construct,
            columns: body.as_ref().and_then(select_columns),
        }
    }
}

/// The SELECT columns a SPARQL query projects, in order; `None` for `SELECT *`
/// and for other query forms.
pub(crate) fn sparql_columns(sparql: &str) -> Option<Vec<String>> {
    fluree_db_sparql::parse_sparql(sparql)
        .ast
        .and_then(|ast| select_columns(&ast.body))
}

fn select_columns(body: &QueryBody) -> Option<Vec<String>> {
    match body {
        QueryBody::Select(select) => match &select.select.variables {
            SelectVariables::Explicit(vars) => {
                Some(vars.iter().map(|v| v.var().name.to_string()).collect())
            }
            SelectVariables::Star => None,
        },
        _ => None,
    }
}

/// The columns a JSON-LD query's `select` projects (`"?x"` or `["?x", ...]`).
pub(crate) fn jsonld_columns(query: &JsonValue) -> Option<Vec<String>> {
    let var = |v: &JsonValue| v.as_str()?.strip_prefix('?').map(str::to_string);
    match query.get("select")? {
        JsonValue::Array(vars) => vars.iter().map(var).collect(),
        single => var(single).map(|v| vec![v]),
    }
}

/// `("select", columns, rows)`, `("ask", bool)`, or `("graph", jsonld)`.
pub(crate) fn sparql_to_py<'py>(
    py: Python<'py>,
    result: &SparqlResult,
) -> PyResult<Bound<'py, PyAny>> {
    let json = &result.json;
    if result.construct {
        return ("graph", from_json(py, json)?)
            .into_pyobject(py)
            .map(Bound::into_any);
    }
    if let Some(boolean) = json.get("boolean").and_then(JsonValue::as_bool) {
        return ("ask", boolean).into_pyobject(py).map(Bound::into_any);
    }

    let columns = match &result.columns {
        Some(columns) => columns.clone(),
        None => json["head"]["vars"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
    };
    let rows = PyList::empty(py);
    for binding in json["results"]["bindings"].as_array().into_iter().flatten() {
        let cells = columns
            .iter()
            .map(|column| binding.get(column).map(|t| term(py, t)).transpose())
            .collect::<PyResult<Vec<_>>>()?;
        rows.append(PyTuple::new(py, cells)?)?;
    }
    ("select", columns, rows)
        .into_pyobject(py)
        .map(Bound::into_any)
}

pub(crate) fn term<'py>(py: Python<'py>, term: &JsonValue) -> PyResult<Bound<'py, PyTuple>> {
    let value = term["value"].as_str().unwrap_or_default();
    match term["type"].as_str() {
        Some("uri") => ("iri", value).into_pyobject(py),
        Some("bnode") => ("bnode", value).into_pyobject(py),
        Some("literal") => {
            let language = term.get("xml:lang").and_then(JsonValue::as_str);
            let datatype =
                term.get("datatype")
                    .and_then(JsonValue::as_str)
                    .unwrap_or(if language.is_some() {
                        RDF_LANG_STRING
                    } else {
                        XSD_STRING
                    });
            ("literal", value, datatype, language).into_pyobject(py)
        }
        _ => Err(fluree_error(format!(
            "unsupported SPARQL result term: {term}"
        ))),
    }
}

/// A commit summary as the dict `fluree.Commit` is built from.
pub(crate) fn commit_summary<'py>(
    py: Python<'py>,
    summary: &CommitSummary,
) -> PyResult<Bound<'py, PyDict>> {
    let commit = PyDict::new(py);
    commit.set_item("t", summary.t)?;
    commit.set_item("id", summary.commit_id.to_string())?;
    commit.set_item("digest", summary.commit_id.digest_hex())?;
    commit.set_item("asserts", summary.asserts)?;
    commit.set_item("retracts", summary.retracts)?;
    commit.set_item("time", summary.time.as_deref())?;
    commit.set_item("message", summary.message.as_deref())?;
    Ok(commit)
}

/// `(subject, predicate, object, assert, graph)`, the object a term tuple.
/// The flake's IRIs must be whole, not compacted.
pub(crate) fn flake<'py>(py: Python<'py>, flake: &ResolvedFlake) -> PyResult<Bound<'py, PyTuple>> {
    let object = if flake.dt == "@id" {
        ("iri", lexical(&flake.o)).into_pyobject(py)?
    } else {
        (
            "literal",
            lexical(&flake.o),
            &flake.dt,
            flake.lang.as_deref(),
        )
            .into_pyobject(py)?
    };
    (&flake.s, &flake.p, object, flake.op, flake.graph.as_deref()).into_pyobject(py)
}

fn lexical(value: &ResolvedValue) -> String {
    match value {
        ResolvedValue::String(s) | ResolvedValue::Lexical(s) => s.clone(),
        ResolvedValue::Boolean(b) => b.to_string(),
        ResolvedValue::Long(n) => n.to_string(),
        ResolvedValue::Double(d) => d.to_string(),
    }
}
