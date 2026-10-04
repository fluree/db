//! Cypher: reads through the query path, writes through the engine's
//! interactive Cypher transaction (the one Bolt explicit transactions use),
//! results as typed cells — nodes, relationships, and paths hydrated.
//!
//! Cells cross to Python as tagged tuples that `fluree/_cypher.py` decodes:
//! `("value", json)`, `("decimal", text)`, `("bigint", text)`,
//! `("temporal", kind, iso)`, `("list", [cell])`, `("map", [(key, cell)])`,
//! `("node", iri, labels, [(key, cell)])`,
//! `("rel", reifier, type, start, end, [(key, cell)])`, and
//! `("path", [node], [rel], indices)`.

use crate::connection::receipt_to_py;
use crate::convert::from_json;
use crate::error::{api_error, invalid_request};
use crate::runtime::{block_on, block_on_cancellable, InRuntime};
use fluree_db_api::cypher_import::split_statements;
use fluree_db_api::cypher_txn::CypherTransaction as EngineTxn;
use fluree_db_api::cypher_write::cypher_statement_is_write;
use fluree_db_api::format::cypher_typed::{
    CypherCell, CypherNode, CypherRelationship, CypherTemporal,
};
use fluree_db_api::{
    ApiError, CypherParamMap, Fluree, GovernanceOptions, GraphDb, QueryCancellation,
    QueryExecutionOptions, TrackingOptions, TransactError,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::sync::Mutex;
use std::time::Duration;

type Table = (Vec<String>, Vec<Vec<CypherCell>>);

/// How many times an autocommit write is retried when another commit lands
/// between its start and its commit.
const MAX_WRITE_ATTEMPTS: usize = 16;

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

/// Run a read against `db`, cancelled at `timeout` or on Ctrl-C.
pub(crate) fn read<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    db: &GraphDb,
    cypher: &str,
    params: Option<&CypherParamMap>,
    timeout: Option<f64>,
) -> PyResult<Bound<'py, PyTuple>> {
    let cancellation = QueryCancellation::new();
    let options = QueryExecutionOptions::new().with_cancellation(cancellation.clone());
    let table = block_on_cancellable(
        py,
        &cancellation,
        timeout.map(Duration::from_secs_f64),
        read_table(fluree, db, cypher, params, &options),
    )?
    .map_err(api_error)?;
    table_to_py(py, &table)
}

async fn read_table(
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

/// Run the statements of `cypher` in `txn`: writes stage into it, reads see
/// what it has staged. Returns the last statement's table.
async fn run_in(
    fluree: &Fluree,
    txn: &mut EngineTxn,
    cypher: &str,
    params: Option<&CypherParamMap>,
) -> fluree_db_api::Result<Option<Table>> {
    let mut last = None;
    for statement in split_statements(cypher) {
        last = if cypher_statement_is_write(&statement)? {
            fluree
                .cypher_transaction_write(txn, &statement, params)
                .await?
                .return_table
        } else {
            let view = fluree.cypher_transaction_view(txn).await?;
            Some(
                read_table(
                    fluree,
                    &view,
                    &statement,
                    params,
                    &QueryExecutionOptions::default(),
                )
                .await?,
            )
        };
    }
    Ok(last)
}

fn is_commit_conflict(e: &ApiError) -> bool {
    matches!(
        e,
        ApiError::Transact(
            TransactError::CommitConflict { .. } | TransactError::PublishLostRace { .. }
        )
    )
}

/// Run a write (one statement or a script) and commit it, all or nothing.
/// Returns `(commit, table)`.
pub(crate) fn write<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    cypher: &str,
    params: Option<&CypherParamMap>,
    governance: GovernanceOptions,
) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyTuple>)> {
    let (receipt, table) = block_on(py, async {
        let mut attempt = 1;
        loop {
            let outcome = async {
                let mut txn = fluree
                    .begin_cypher_transaction(
                        ledger,
                        governance.clone(),
                        TrackingOptions::default(),
                    )
                    .await?;
                let table = run_in(fluree, &mut txn, cypher, params).await?;
                let committed = fluree.commit_cypher_transaction(txn).await?;
                Ok::<_, ApiError>((committed.receipt, table))
            }
            .await;
            match outcome {
                Err(e) if attempt < MAX_WRITE_ATTEMPTS && is_commit_conflict(&e) => attempt += 1,
                other => break other,
            }
        }
    })?
    .map_err(api_error)?;
    let table = table.unwrap_or_default();
    Ok((receipt_to_py(py, &receipt)?, table_to_py(py, &table)?))
}

fn table_to_py<'py>(py: Python<'py>, (columns, rows): &Table) -> PyResult<Bound<'py, PyTuple>> {
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

/// An explicit Cypher transaction: statements run against its private state
/// and publish together on `commit`.
#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct CypherTransaction {
    fluree: InRuntime<Fluree>,
    state: Mutex<State>,
}

/// Checked out for each engine call, so the lock is never held while the GIL
/// is released.
enum State {
    Open(Box<InRuntime<EngineTxn>>),
    Busy,
    Closed,
}

impl CypherTransaction {
    pub(crate) fn begin(
        py: Python<'_>,
        fluree: &Fluree,
        ledger: &str,
        governance: GovernanceOptions,
    ) -> PyResult<Self> {
        let txn = block_on(
            py,
            fluree.begin_cypher_transaction(ledger, governance, TrackingOptions::default()),
        )?
        .map_err(api_error)?;
        Ok(Self {
            fluree: InRuntime::new(fluree.clone()),
            state: Mutex::new(State::Open(Box::new(InRuntime::new(txn)))),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn check_out(&self) -> PyResult<Box<InRuntime<EngineTxn>>> {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, State::Busy) {
            State::Open(txn) => Ok(txn),
            State::Busy => Err(invalid_request(
                "the transaction is in use by another thread",
            )),
            State::Closed => {
                *state = State::Closed;
                Err(invalid_request(
                    "the transaction has already been committed or rolled back",
                ))
            }
        }
    }

    fn set(&self, state: State) {
        *self.lock() = state;
    }
}

#[pymethods]
impl CypherTransaction {
    #[getter]
    fn is_open(&self) -> bool {
        !matches!(*self.lock(), State::Closed)
    }

    /// Run a statement or script in the transaction; returns its table.
    #[pyo3(signature = (cypher, params = None))]
    fn run<'py>(
        &self,
        py: Python<'py>,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let params = self::params(params)?;
        let mut txn = self.check_out()?;
        let fluree = &*self.fluree;
        let ran = block_on(py, run_in(fluree, txn.get_mut(), cypher, params.as_ref()));
        self.set(State::Open(txn));
        let table = ran?.map_err(api_error)?.unwrap_or_default();
        table_to_py(py, &table)
    }

    /// Publish every statement's changes; returns the `Commit` dict. The
    /// transaction is closed afterwards, whether or not it committed.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let txn = self.check_out()?;
        let fluree = &*self.fluree;
        let committed = block_on(py, async move {
            fluree
                .commit_cypher_transaction(InRuntime::into_inner(*txn))
                .await
        });
        self.set(State::Closed);
        let committed = committed?.map_err(api_error)?;
        receipt_to_py(py, &committed.receipt)
    }

    fn rollback(&self) -> PyResult<()> {
        drop(self.check_out()?);
        self.set(State::Closed);
        Ok(())
    }
}
