//! Cypher: reads through the query path (writes stage in the engine
//! `Transaction`), results as typed cells — nodes, relationships, and paths
//! hydrated.
//!
//! Cells cross to Python as tagged tuples that `fluree/_cypher.py` decodes:
//! `("value", json)`, `("decimal", text)`, `("bigint", text)`,
//! `("temporal", kind, iso)`, `("list", [cell])`, `("map", [(key, cell)])`,
//! `("node", iri, labels, [(key, cell)])`,
//! `("rel", reifier, type, start, end, [(key, cell)])`, and
//! `("path", [node], [rel], indices)`.

use crate::convert::from_json;
use crate::error::{api_error, invalid_request, raise_status};
use crate::query::Controls;
use crate::runtime::block_on_cancellable;
use fluree_db_api::cypher_import::split_statements;
use fluree_db_api::cypher_write::cypher_statement_is_write;
use fluree_db_api::format::cypher_typed::{
    CypherCell, CypherNode, CypherRelationship, CypherTemporal,
};
use fluree_db_api::{ApiError, CypherParamMap, Fluree, GraphDb, QueryExecutionOptions, Tracker};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

type Table = (Vec<String>, Vec<Vec<CypherCell>>);

/// `$param` values, from a Python dict.
pub(crate) fn params(params: Option<&Bound<'_, PyAny>>) -> PyResult<Option<CypherParamMap>> {
    let Some(params) = params else {
        return Ok(None);
    };
    match crate::convert::to_json(params)? {
        serde_json::Value::Object(map) if map.is_empty() => Ok(None),
        serde_json::Value::Object(map) => Ok(Some(map)),
        _ => Err(invalid_request("Cypher parameters must be a dict")),
    }
}

/// Whether `cypher` (one statement or a `;`-separated script) writes.
pub(crate) fn is_write(cypher: &str) -> PyResult<bool> {
    for statement in split_statements(cypher) {
        if cypher_statement_is_write(&statement).map_err(api_error)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Refuse a write where only reads run.
pub(crate) fn require_read(cypher: &str) -> PyResult<()> {
    if is_write(cypher)? {
        return Err(invalid_request(
            "this Cypher statement writes; run it with update()",
        ));
    }
    Ok(())
}

/// Run a read against `db`, cancelled at the controls' timeout, through
/// their canceller, or on Ctrl-C, and stopped at their fuel limit. Returns
/// the table, or `(table, stats)` when the controls ask for stats.
pub(crate) fn read<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    db: &GraphDb,
    cypher: &str,
    params: Option<&CypherParamMap>,
    controls: Controls,
) -> PyResult<Bound<'py, PyAny>> {
    let timeout = controls.checked_timeout()?;
    let cancellation = controls.cancellation();
    let options = QueryExecutionOptions::new().with_cancellation(cancellation.clone());
    let tracker = controls
        .tracking()
        .map_or_else(Tracker::disabled, Tracker::new);
    let table = block_on_cancellable(py, &cancellation, timeout, async {
        let result = fluree
            .query_cypher_with_tracker(db, cypher, params, &options, &tracker)
            .await?;
        result
            .to_cypher_typed_table(db)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))
    })?
    .map_err(|e| {
        if crate::ops::fuel_exhausted(&e) {
            raise_status("ResourceLimitError", e.to_string(), e.status_code())
        } else {
            api_error(e)
        }
    })?;
    let table = table_to_py(py, &table)?.into_any();
    if !controls.wants_stats() {
        return Ok(table);
    }
    let tally = tracker.tally();
    let stats = PyDict::new(py);
    stats.set_item("fuel", tally.as_ref().and_then(|t| t.fuel))?;
    stats.set_item("time", tally.and_then(|t| t.time))?;
    (table, stats).into_pyobject(py).map(Bound::into_any)
}

pub(crate) async fn read_table(
    fluree: &Fluree,
    db: &GraphDb,
    cypher: &str,
    params: Option<&CypherParamMap>,
    options: &QueryExecutionOptions,
) -> fluree_db_api::Result<Table> {
    let result = fluree
        .query_cypher_with_options(db, cypher, params, options)
        .await?;
    result
        .to_cypher_typed_table(db)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))
}

pub(crate) fn table_to_py<'py>(
    py: Python<'py>,
    (columns, rows): &Table,
) -> PyResult<Bound<'py, PyTuple>> {
    let out = PyList::empty(py);
    for row in rows {
        let cells = row
            .iter()
            .map(|c| cell(py, c))
            .collect::<PyResult<Vec<_>>>()?;
        out.append(PyTuple::new(py, cells)?)?;
    }
    (columns, out).into_pyobject(py)
}

fn cell<'py>(py: Python<'py>, cell: &CypherCell) -> PyResult<Bound<'py, PyAny>> {
    let tuple = match cell {
        CypherCell::Value(json) => ("value", from_json(py, json)?).into_pyobject(py)?,
        CypherCell::Decimal(text) => ("decimal", text).into_pyobject(py)?,
        CypherCell::BigInt(text) => ("bigint", text).into_pyobject(py)?,
        CypherCell::Temporal(t) => {
            let (kind, iso) = match t {
                CypherTemporal::Date { iso, .. } => ("date", iso),
                CypherTemporal::DateTime { iso, .. } => ("datetime", iso),
                CypherTemporal::Time { iso, .. } => ("time", iso),
            };
            ("temporal", kind, iso).into_pyobject(py)?
        }
        CypherCell::List(items) => {
            let items = items
                .iter()
                .map(|c| self::cell(py, c))
                .collect::<PyResult<Vec<_>>>()?;
            ("list", items).into_pyobject(py)?
        }
        CypherCell::Map(entries) => ("map", properties(py, entries)?).into_pyobject(py)?,
        CypherCell::Node(node) => return self::node(py, node),
        CypherCell::Relationship(rel) => return relationship(py, rel),
        CypherCell::Path(path) => {
            let nodes = path
                .nodes
                .iter()
                .map(|n| node(py, n))
                .collect::<PyResult<Vec<_>>>()?;
            let rels = path
                .rels
                .iter()
                .map(|r| relationship(py, r))
                .collect::<PyResult<Vec<_>>>()?;
            ("path", nodes, rels, &path.indices).into_pyobject(py)?
        }
    };
    Ok(tuple.into_any())
}

fn properties<'py, K: AsRef<str>>(
    py: Python<'py>,
    entries: &[(K, CypherCell)],
) -> PyResult<Vec<(String, Bound<'py, PyAny>)>> {
    entries
        .iter()
        .map(|(k, v)| Ok((k.as_ref().to_string(), cell(py, v)?)))
        .collect()
}

fn node<'py>(py: Python<'py>, node: &CypherNode) -> PyResult<Bound<'py, PyAny>> {
    let labels: Vec<&str> = node.labels.iter().map(AsRef::as_ref).collect();
    Ok((
        "node",
        node.iri.as_ref(),
        labels,
        properties(py, &node.properties)?,
    )
        .into_pyobject(py)?
        .into_any())
}

fn relationship<'py>(py: Python<'py>, rel: &CypherRelationship) -> PyResult<Bound<'py, PyAny>> {
    Ok((
        "rel",
        rel.reifier_iri.as_deref(),
        rel.type_name.as_ref(),
        rel.start_iri.as_ref(),
        rel.end_iri.as_ref(),
        properties(py, &rel.properties)?,
    )
        .into_pyobject(py)?
        .into_any())
}
