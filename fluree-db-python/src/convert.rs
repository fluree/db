//! Engine values to and from plain Python data.
//!
//! SPARQL terms cross the boundary as small tuples that `fluree/_terms.py`
//! turns into Python values: `("iri", iri)`, `("bnode", label)`, and
//! `("literal", lexical, datatype_iri, language)`. Unbound cells are `None`.

use crate::error::{fluree_error, invalid_request};
use fluree_db_api::{CommitRef, ResolvedFlake, ResolvedValue, TimeSpec};
use fluree_db_core::{CommitSummary, ContentId};
use fluree_db_sparql::ast::{QueryBody, SelectVariables};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};
use serde_json::Value as JsonValue;

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_DATETIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";
const XSD_DATE: &str = "http://www.w3.org/2001/XMLSchema#date";
const XSD_TIME: &str = "http://www.w3.org/2001/XMLSchema#time";

/// How [`convert`] reads the values it meets.
#[derive(Clone, Copy, PartialEq)]
enum Values {
    /// Plain data: a configuration, a context, a policy.
    Plain,
    /// RDF values in a JSON-LD document or query.
    Rdf,
}

/// A Python value as plain JSON data. A `fluree.Vector`, or a
/// one-dimensional numpy array, becomes an `f:embeddingVector` value object.
pub(crate) fn to_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    convert(obj, Values::Plain)
}

/// A JSON-LD document or query. Where a property's value goes, an RDF term —
/// `IRI`, `BlankNode`, `LangString`, `Literal`, `Vector`, a Cypher `Node` —
/// or a Python value with an XSD datatype — `Decimal`, `datetime`, `date`,
/// `time`, an int past 64 bits, a non-finite float — becomes the JSON-LD
/// value object that stores it as that term, so it reads back as it went in.
/// Keyword entries (`@id`, `@type`, `@context`, ...) are plain data.
pub(crate) fn to_jsonld(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    convert(obj, Values::Rdf)
}

/// The keywords whose entries hold values or nodes rather than plain data:
/// JSON-LD's containers, and Fluree's edge annotations (`@annotation`, its
/// alias `@edge`) and the edge an annotation `@reifies`.
const VALUE_KEYWORDS: [&str; 9] = [
    "@list",
    "@set",
    "@graph",
    "@included",
    "@reverse",
    "@nest",
    "@annotation",
    "@edge",
    "@reifies",
];

fn convert(obj: &Bound<'_, PyAny>, values: Values) -> PyResult<JsonValue> {
    let py = obj.py();
    let rdf = values == Values::Rdf;
    if obj.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(JsonValue::Bool(b.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(i.into());
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(u.into());
        }
        let lexical = obj.str()?.to_str()?.to_owned();
        return match values {
            Values::Rdf => Ok(typed(lexical, XSD_INTEGER)),
            Values::Plain => Err(invalid_request(format!(
                "{lexical} does not fit in a 64-bit JSON number"
            ))),
        };
    }
    if obj.is_instance_of::<PyFloat>() {
        let value: f64 = obj.extract()?;
        if let Some(n) = serde_json::Number::from_f64(value) {
            return Ok(JsonValue::Number(n));
        }
        let lexical = if value.is_nan() {
            "NaN"
        } else if value > 0.0 {
            "INF"
        } else {
            "-INF"
        };
        return match values {
            Values::Rdf => Ok(typed(lexical.to_owned(), XSD_DOUBLE)),
            Values::Plain => Err(invalid_request(format!("{value} is not a JSON number"))),
        };
    }
    if let Ok(s) = obj.cast::<PyString>() {
        let text = s.to_str()?.to_owned();
        if rdf {
            if obj.is_instance(term_class(py, &IRI_CLASS, "IRI")?)? {
                return Ok(serde_json::json!({ "@id": text }));
            }
            if obj.is_instance(term_class(py, &BLANK_NODE_CLASS, "BlankNode")?)? {
                return Ok(serde_json::json!({ "@id": format!("_:{text}") }));
            }
            if obj.is_instance(term_class(py, &LANG_STRING_CLASS, "LangString")?)? {
                let language: String = obj.getattr("language")?.extract()?;
                return Ok(serde_json::json!({ "@value": text, "@language": language }));
            }
        }
        return Ok(JsonValue::String(text));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key = key
                .cast::<PyString>()
                .map_err(|_| invalid_request(format!("dict keys must be strings, not {key}")))?;
            let key = key.to_str()?;
            let entry = if key.starts_with('@') && !VALUE_KEYWORDS.contains(&key) {
                Values::Plain
            } else {
                values
            };
            map.insert(key.to_owned(), convert(&value, entry)?);
        }
        return Ok(JsonValue::Object(map));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return list.iter().map(|item| convert(&item, values)).collect();
    }
    if obj.is_instance(term_class(py, &VECTOR_CLASS, "Vector")?)? || is_ndarray(obj)? {
        return vector_json(obj);
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        return tuple.iter().map(|item| convert(&item, values)).collect();
    }
    if let Some(term) = rdf_value(obj)? {
        return match values {
            Values::Rdf => Ok(term),
            Values::Plain => Err(PyTypeError::new_err(format!(
                "a {} is an RDF value, which has no place here",
                obj.get_type().name()?
            ))),
        };
    }
    pythonize::depythonize(obj).map_err(|_| {
        let kind = obj
            .get_type()
            .name()
            .map_or_else(|_| "value".into(), |n| n.to_string());
        match values {
            Values::Rdf => PyTypeError::new_err(format!("a {kind} is not an RDF term")),
            Values::Plain => PyTypeError::new_err(format!("cannot convert a {kind} to JSON")),
        }
    })
}

/// The JSON-LD form of a `Literal`, `Node`, `Decimal`, `datetime`, `date`,
/// or `time`; `None` for anything else.
fn rdf_value(obj: &Bound<'_, PyAny>) -> PyResult<Option<JsonValue>> {
    let py = obj.py();
    if obj.is_instance(term_class(py, &LITERAL_CLASS, "Literal")?)? {
        let value: String = obj.getattr("value")?.extract()?;
        let datatype: String = obj.getattr("datatype")?.extract()?;
        return Ok(Some(typed(value, &datatype)));
    }
    if obj.is_instance(NODE_CLASS.import(py, "fluree._graph", "Node")?)? {
        return convert(&obj.getattr("element_id")?, Values::Rdf).map(Some);
    }
    // A datetime is also a date, so it is tested first.
    for (class, cell, datatype) in [
        ("datetime", &DATETIME_CLASS, XSD_DATETIME),
        ("date", &DATE_CLASS, XSD_DATE),
        ("time", &TIME_CLASS, XSD_TIME),
    ] {
        if obj.is_instance(cell.import(py, "datetime", class)?)? {
            let lexical: String = obj.call_method0("isoformat")?.extract()?;
            return Ok(Some(typed(lexical, datatype)));
        }
    }
    if obj.is_instance(DECIMAL_CLASS.import(py, "decimal", "Decimal")?)? {
        if !obj.call_method0("is_finite")?.is_truthy()? {
            return Err(invalid_request(format!("{obj} is not an xsd:decimal")));
        }
        let format = py.import("builtins")?.getattr("format")?;
        let lexical: String = format.call1((obj, "f"))?.extract()?;
        return Ok(Some(typed(lexical, XSD_DECIMAL)));
    }
    Ok(None)
}

fn typed(lexical: String, datatype: &str) -> JsonValue {
    serde_json::json!({ "@value": lexical, "@type": datatype })
}

static IRI_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static BLANK_NODE_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static LANG_STRING_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static LITERAL_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static VECTOR_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static NODE_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static DATETIME_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static DATE_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static TIME_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static DECIMAL_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// `iri` as a `fluree.IRI`.
pub(crate) fn iri<'py>(py: Python<'py>, iri: &str) -> PyResult<Bound<'py, PyAny>> {
    term_class(py, &IRI_CLASS, "IRI")?.call1((iri,))
}

fn term_class<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyType>>,
    name: &str,
) -> PyResult<&'py Bound<'py, PyType>> {
    cell.import(py, "fluree._terms", name)
}

fn is_ndarray(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let class = obj.get_type();
    Ok(class.name()? == "ndarray" && class.module()?.to_str()? == "numpy")
}

fn vector_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if is_ndarray(obj)? {
        let ndim: usize = obj.getattr("ndim")?.extract()?;
        if ndim != 1 {
            return Err(invalid_request(format!(
                "a vector is one-dimensional; this numpy array has {ndim} dimensions"
            )));
        }
    }
    let values = obj
        .try_iter()?
        .map(|item| {
            let value: f64 = item?.extract()?;
            serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    invalid_request(format!("a vector holds finite numbers, not {value}"))
                })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(serde_json::json!({
        "@value": values,
        "@type": fluree_vocab::fluree::EMBEDDING_VECTOR,
    }))
}

/// SPARQL parameters: variable name to value, each converted as a property
/// value is by [`to_jsonld`]; `None` when empty.
pub(crate) fn sparql_params(
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<fluree_db_api::SparqlParamMap>> {
    let Some(params) = params else {
        return Ok(None);
    };
    let params = params
        .cast::<PyDict>()
        .map_err(|_| invalid_request("SPARQL parameters must be a dict"))?;
    if params.is_empty() {
        return Ok(None);
    }
    let mut map = fluree_db_api::SparqlParamMap::new();
    for (name, value) in params.iter() {
        let name: String = name.extract()?;
        let mut term = to_jsonld(&value).map_err(|e| {
            invalid_request(format!("parameter {name:?}: {}", e.value(params.py())))
        })?;
        // A parameter's `@value` is a scalar, so a vector goes as its JSON text.
        if let Some(object) = term.as_object_mut() {
            if let Some(vector) = object.get_mut("@value").filter(|v| v.is_array()) {
                *vector = JsonValue::String(vector.to_string());
            }
        }
        map.insert(name, term);
    }
    Ok(Some(map))
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

/// The form of a SPARQL request: `"select"`, `"ask"`, `"construct"`,
/// `"describe"` or `"update"`; `None` when it does not parse, for running it
/// to report why.
#[pyfunction]
pub(crate) fn sparql_form(sparql: &str) -> Option<&'static str> {
    let body = fluree_db_sparql::parse_sparql(sparql).ast?.body;
    Some(match body {
        QueryBody::Select(_) => "select",
        QueryBody::Ask(_) => "ask",
        QueryBody::Construct(_) => "construct",
        QueryBody::Describe(_) => "describe",
        QueryBody::Update(_) => "update",
    })
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
