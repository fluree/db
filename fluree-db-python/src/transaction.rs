//! The native transaction behind `fluree.Transaction`: writes staged one at a
//! time, readable before they commit, committed as one commit.

use crate::connection::{commit_opts, operation, receipt_to_py, Database, Snapshot};
use crate::cypher;
use crate::error::{api_error, invalid_request};
use crate::runtime::{block_on, InRuntime};
use fluree_db_api::cypher_import::split_statements;
use fluree_db_api::cypher_write::cypher_statement_is_write;
use fluree_db_api::QueryExecutionOptions;
use fluree_db_api::{Fluree, GovernanceOptions, GraphDb, TransactionOptions};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use std::sync::Mutex;

#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct Transaction {
    fluree: Database,
    ledger: String,
    policy: Option<GovernanceOptions>,
    state: Mutex<State>,
}

/// The engine transaction is checked out for each engine call, so the lock
/// is never held while the GIL is released.
enum State {
    Open(Box<EngineTxn>),
    /// Checked out by a call in progress on another thread.
    Busy,
    Closed,
}

type EngineTxn = InRuntime<fluree_db_api::Transaction>;

impl Transaction {
    pub(crate) fn begin(
        py: Python<'_>,
        database: &Database,
        ledger: &str,
        policy: Option<GovernanceOptions>,
    ) -> PyResult<Self> {
        let fluree = database.get()?;
        let options = TransactionOptions {
            governance: policy.clone().unwrap_or_default(),
            ..TransactionOptions::default()
        };
        let txn = block_on(py, fluree.begin_transaction(ledger, options))?.map_err(api_error)?;
        Ok(Self {
            fluree: database.clone(),
            ledger: ledger.to_string(),
            policy,
            state: Mutex::new(State::Open(Box::new(InRuntime::new(txn)))),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn check_out(&self) -> PyResult<Box<EngineTxn>> {
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

    /// Run `f` on the checked-out transaction and check it back in.
    fn with_txn<T>(&self, f: impl FnOnce(&mut EngineTxn) -> PyResult<T>) -> PyResult<T> {
        let mut txn = self.check_out()?;
        let result = f(&mut txn);
        *self.lock() = State::Open(txn);
        result
    }
}

impl Transaction {
    /// The staged state, governed and carrying the default context as
    /// `Connection.snapshot` views are. Reading it marks the transaction read.
    async fn view(
        &self,
        fluree: &Fluree,
        txn: &fluree_db_api::Transaction,
    ) -> fluree_db_api::Result<GraphDb> {
        let view = txn.db().await?;
        let view = match &self.policy {
            Some(policy) => fluree.wrap_policy(view, policy).await?,
            None => fluree.wrap_policy_defaults(view).await?,
        };
        let context = fluree.get_default_context(&self.ledger).await?;
        Ok(view.with_default_context(context))
    }
}

#[pymethods]
impl Transaction {
    #[getter]
    fn ledger(&self) -> &str {
        &self.ledger
    }

    #[getter]
    fn is_open(&self) -> bool {
        !matches!(*self.lock(), State::Closed)
    }

    /// Stage one write over those before it; see `Connection.transact`.
    #[pyo3(signature = (op, kind, payload, params = None))]
    fn stage(
        &self,
        py: Python<'_>,
        op: &str,
        kind: &str,
        payload: &Bound<'_, PyAny>,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let operation = operation(op, kind, payload, params)?;
        self.fluree.get()?;
        self.with_txn(|txn| block_on(py, txn.get_mut()?.stage(operation))?.map_err(api_error))
    }

    /// Stage a Cypher write — one statement or a `;` script, whose
    /// statements stage in turn, all or nothing; a read statement in it
    /// reads the staged state. Returns the last statement's table, or `None`.
    #[pyo3(signature = (query, params = None))]
    fn stage_cypher<'py>(
        &self,
        py: Python<'py>,
        query: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        if !cypher::is_write(query)? {
            return Err(crate::error::invalid_request(
                "this Cypher statement only reads; run it with query()",
            ));
        }
        let params = cypher::params(params)?;
        let fluree = self.fluree.get()?;
        let rows = self.with_txn(|txn| {
            let txn = txn.get_mut()?;
            block_on(py, async {
                let savepoint = txn.savepoint();
                let staged = async {
                    let mut last = None;
                    for statement in split_statements(query) {
                        last = if cypher_statement_is_write(&statement)? {
                            txn.stage_cypher(&statement, params.clone()).await?
                        } else {
                            let view = self.view(fluree, txn).await?;
                            Some(
                                cypher::read_table(
                                    fluree,
                                    &view,
                                    &statement,
                                    params.as_ref(),
                                    &QueryExecutionOptions::default(),
                                )
                                .await?,
                            )
                        };
                    }
                    Ok::<_, fluree_db_api::ApiError>(last)
                }
                .await;
                if staged.is_err() {
                    txn.rollback_to(savepoint).await?;
                }
                staged
            })?
            .map_err(api_error)
        })?;
        rows.map(|table| cypher::table_to_py(py, &table))
            .transpose()
    }

    /// The ledger as the staged writes leave it, governed and carrying the
    /// default context as `Connection.snapshot` views are.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Snapshot> {
        let fluree = self.fluree.get()?;
        let db =
            self.with_txn(|txn| block_on(py, self.view(fluree, txn.get()?))?.map_err(api_error))?;
        Ok(Snapshot::new(&self.fluree, db))
    }

    /// Commit the staged writes as one commit; returns the `Commit` dict.
    /// The transaction is closed afterwards, whether or not it committed.
    #[pyo3(signature = (message = None))]
    fn commit<'py>(
        &self,
        py: Python<'py>,
        message: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.fluree.get()?;
        let txn = self.check_out()?;
        let committed = InRuntime::into_inner(*txn)
            .and_then(|txn| block_on(py, async move { txn.commit(commit_opts(message)).await }));
        *self.lock() = State::Closed;
        let receipt = committed?.map_err(api_error)?.receipt;
        receipt_to_py(py, &receipt)
    }

    /// Discard the staged writes.
    fn rollback(&self) -> PyResult<()> {
        drop(self.check_out()?);
        *self.lock() = State::Closed;
        Ok(())
    }
}
