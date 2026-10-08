//! The native connection and snapshot handles behind `fluree.Connection`.
//!
//! These take ledger ids and plain data and return plain data; the Python
//! layer owns argument handling, defaults, and result objects.

use crate::branch;
use crate::convert::{
    commit_ref, commit_summary, flake, from_json, jsonld_columns, sparql_columns, sparql_params,
    time_spec, to_json, to_jsonld,
};
use crate::cypher;
use crate::error::{api_error, fluree_error, invalid_request, not_found};
use crate::graph_source;
use crate::ops;
use crate::query::{execute, Controls};
use crate::runtime::{block_on, enter, runtime, InRuntime};
use crate::stream::{RowStream, CHANNEL_DEPTH};
use crate::transaction::Transaction;
use fluree_db_api::{
    build_transact_policy_context, export::ExportFormat, ApiError, CommitDetail, CommitReceipt,
    CommitRef, DataSetDb, DropMode, Fluree, FlureeBuilder, FormatterConfig, GovernanceOptions,
    GraphDb, GraphSnapshotQueryBuilder, OwnedStreamQuery, ParsedContext, PolicyContext,
    QueryExecutionOptions, SparqlParamMap, TimeSpec, Tracker, TxnOperation,
};
use fluree_db_api::{CommitOpts, GraphPayload, GraphSel, SyncGraphOpts, TxnOpts};
use fluree_db_core::commit::{TxnMetaEntry, TxnMetaValue};
use fluree_db_core::ledger_id::normalize_ledger_id;
use fluree_db_core::ContentId;

use fluree_vocab::namespaces::FLUREE_DB;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct Connection {
    fluree: Database,
}

/// The engine a connection opened, shared by the snapshots, transactions and
/// streams opened through it, so closing the connection closes them too.
#[derive(Clone)]
pub(crate) struct Database(Arc<DatabaseState>);

struct DatabaseState {
    fluree: InRuntime<Fluree>,
    closed: AtomicBool,
}

impl Database {
    fn new(fluree: Fluree) -> Self {
        Self(Arc::new(DatabaseState {
            fluree: InRuntime::new(fluree),
            closed: AtomicBool::new(false),
        }))
    }

    pub(crate) fn get(&self) -> PyResult<&Fluree> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(invalid_request("the connection is closed"));
        }
        self.0.fluree.get()
    }

    /// Close the connection, once; closing it again does nothing.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let fluree = self.0.fluree.get()?;
        if self.0.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        block_on(py, fluree.disconnect())
    }
}

/// A sync payload: JSON-LD, or Turtle / N-Triples / TriG text.
enum Payload {
    Json(JsonValue),
    Text(String),
}

/// A write from its operation (`insert`, `upsert`, `update`) and payload
/// format: `"jsonld"` (a JSON-able object), `"turtle"` (insert and upsert),
/// or `"sparql"` (update, which alone takes `params`).
pub(crate) fn operation(
    op: &str,
    kind: &str,
    payload: &Bound<'_, PyAny>,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<TxnOperation> {
    let params = sparql_params(params)?;
    if params.is_some() && (op, kind) != ("update", "sparql") {
        return Err(invalid_request(
            "parameters apply to SPARQL and Cypher updates",
        ));
    }
    let json = || -> PyResult<JsonValue> {
        let json = to_jsonld(payload)?;
        jsonld_policy_free(&json)?;
        Ok(json)
    };
    Ok(match (op, kind) {
        ("insert", "jsonld") => TxnOperation::Insert(json()?),
        ("upsert", "jsonld") => TxnOperation::Upsert(json()?),
        ("update", "jsonld") => TxnOperation::Update(json()?),
        ("insert", "turtle") => TxnOperation::InsertTurtle(payload.extract()?),
        ("upsert", "turtle") => TxnOperation::UpsertTurtle(payload.extract()?),
        ("update", "sparql") => {
            let sparql: String = payload.extract()?;
            sparql_policy_free(&sparql)?;
            TxnOperation::SparqlUpdate(sparql, params)
        }
        ("insert" | "upsert" | "update", other) => {
            return Err(invalid_request(format!(
                "{other:?} is not a valid format for this operation"
            )))
        }
        (other, _) => return Err(invalid_request(format!("unknown operation {other:?}"))),
    })
}

/// Commit options carrying `message` as the commit's `f:message`.
pub(crate) fn commit_opts(message: Option<String>) -> CommitOpts {
    let opts = CommitOpts::default();
    match message {
        Some(message) => opts.with_txn_meta(vec![TxnMetaEntry::new(
            FLUREE_DB,
            "message",
            TxnMetaValue::string(message),
        )]),
        None => opts,
    }
}

/// The policy context a governed write is checked against.
pub(crate) async fn write_policy(
    fluree: &Fluree,
    ledger: &str,
    policy: Option<&GovernanceOptions>,
) -> fluree_db_api::Result<Option<PolicyContext>> {
    let Some(opts) = policy else {
        return Ok(None);
    };
    let state = fluree.ledger(ledger).await?;
    build_transact_policy_context(
        fluree,
        &state.snapshot,
        state.novelty.as_ref(),
        Some(state.novelty.as_ref()),
        state.t(),
        opts,
    )
    .await
}

/// The receipt of a write, as the dict `fluree.Commit` is built from.
pub(crate) fn receipt_to_py<'py>(
    py: Python<'py>,
    receipt: &CommitReceipt,
) -> PyResult<Bound<'py, PyDict>> {
    let commit = PyDict::new(py);
    commit.set_item("t", receipt.t)?;
    // A transaction that changes nothing writes no commit.
    let written = receipt.flake_count > 0;
    commit.set_item("id", written.then(|| receipt.commit_id.to_string()))?;
    commit.set_item("digest", written.then(|| receipt.commit_id.digest_hex()))?;
    commit.set_item("asserts", receipt.assert_count)?;
    commit.set_item("retracts", receipt.retract_count)?;
    Ok(commit)
}

fn canonical(ledger: &str) -> PyResult<String> {
    normalize_ledger_id(ledger).map_err(|e| invalid_request(e.to_string()))
}

/// Policy options from the JSON-LD `opts` keys (`identity`, `policyClass`,
/// `policy`, `policyValues`, `defaultAllow`), parsed as the server parses them.
/// `server_identity` stays unset: there is no auth layer here to verify one,
/// so an `f:IdentityRestricted` override control denies, as it does for the CLI.
fn governance(policy: Option<&Bound<'_, PyAny>>) -> PyResult<Option<GovernanceOptions>> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    let opts = serde_json::json!({ "opts": to_json(policy)? });
    GovernanceOptions::from_json(&opts)
        .map(Some)
        .map_err(|e| invalid_request(e.to_string()))
}

/// Policy belongs to the handle (`Ledger.with_policy`). A query or write
/// that selects its own — JSON-LD `opts`, SPARQL `# PRAGMA` — is refused:
/// on a view it would be ignored, and it must never widen a governed handle.
fn refuse_inline_policy(inline: GovernanceOptions) -> PyResult<()> {
    if inline.has_any_policy_inputs() {
        return Err(invalid_request(
            "policy is chosen with Ledger.with_policy(), not inside a query or transaction",
        ));
    }
    Ok(())
}

fn jsonld_policy_free(json: &JsonValue) -> PyResult<()> {
    refuse_inline_policy(
        GovernanceOptions::from_json(json).map_err(|e| invalid_request(e.to_string()))?,
    )
}

fn sparql_policy_free(sparql: &str) -> PyResult<()> {
    refuse_inline_policy(GovernanceOptions::from_sparql(sparql))
}

/// A JSON-LD query on a ledger reads that ledger, and the engine ignores a
/// dataset the query names there, so one that names anything else is refused
/// rather than answered from the wrong graph.
fn jsonld_reads_ledger(json: &JsonValue, ledger: &str) -> fluree_db_api::Result<()> {
    let names_ledger = |value: &JsonValue| {
        value
            .as_str()
            .is_some_and(|name| normalize_ledger_id(name).is_ok_and(|id| id == ledger))
    };
    for scope in [Some(json), json.get("opts")].into_iter().flatten() {
        for key in ["from", "ledger", "fromNamed", "from-named"] {
            let Some(value) = scope.get(key) else {
                continue;
            };
            let this_ledger = matches!(key, "from" | "ledger")
                && match value {
                    JsonValue::Array(names) => !names.is_empty() && names.iter().all(names_ledger),
                    name => names_ledger(name),
                };
            if !this_ledger {
                return Err(ApiError::invalid_query(format!(
                    "a query on ledger {ledger} reads that ledger, so its `{key}` cannot name \
                     another dataset; query other ledgers with Connection.query(), and a named \
                     graph with [\"graph\", iri, pattern]"
                )));
            }
        }
    }
    Ok(())
}

/// A view of `ledger` at `spec`, governed by the ledger's policy defaults or
/// by `policy`, carrying the ledger's default context so a query without
/// `PREFIX` / `@context` resolves its prefixes, as the CLI and server do. A
/// past view takes the current default context, as the CLI's does.
async fn load(
    fluree: &Fluree,
    ledger: &str,
    spec: TimeSpec,
    policy: Option<&GovernanceOptions>,
) -> fluree_db_api::Result<GraphDb> {
    let view = match policy {
        // `load` applies the ledger's configured policy defaults.
        None => fluree.graph_at(ledger, spec).load().await?.into_db(),
        // `wrap_policy` merges those defaults under the caller's options.
        Some(policy) => {
            let view = fluree.db_at(ledger, spec).await?;
            fluree.wrap_policy(view, policy).await?
        }
    };
    let context = fluree.get_default_context(ledger).await?;
    Ok(view.with_default_context(context))
}

#[pymethods]
impl Connection {
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let _runtime = enter()?;
        Ok(Self {
            fluree: Database::new(FlureeBuilder::memory().build_memory()),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (path, *, indexing = true))]
    fn file(path: PathBuf, indexing: bool) -> PyResult<Self> {
        let _runtime = enter()?;
        let mut builder = FlureeBuilder::file(path.to_string_lossy().into_owned());
        if !indexing {
            builder = builder.without_indexing();
        }
        let fluree = builder.build().map_err(api_error)?;
        Ok(Self {
            fluree: Database::new(fluree),
        })
    }

    /// A connection built from a JSON-LD connection config: S3 or tiered
    /// storage, a DynamoDB nameservice, encryption keys, `envVar` values.
    #[staticmethod]
    fn from_config(py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<Self> {
        let config = to_json(config)?;
        let builder = FlureeBuilder::from_json_ld(&config).map_err(api_error)?;
        let fluree = block_on(py, builder.build_client())?.map_err(api_error)?;
        Ok(Self {
            fluree: Database::new(fluree),
        })
    }

    #[pyo3(signature = (ledger, source = None))]
    fn create(&self, py: Python<'_>, ledger: &str, source: Option<PathBuf>) -> PyResult<String> {
        let id = canonical(ledger)?;
        let fluree = self.fluree.get()?;
        match source {
            None => block_on(py, fluree.create_ledger(&id))?
                .map(drop)
                .map_err(api_error)?,
            Some(source) => block_on(py, async {
                fluree
                    .create(&id)
                    .import(&source)
                    .execute()
                    .await
                    .map(drop)
                    .map_err(|e| e.to_string())
            })?
            .map_err(fluree_error)?,
        }
        Ok(id)
    }

    fn exists(&self, py: Python<'_>, ledger: &str) -> PyResult<bool> {
        let id = canonical(ledger)?;
        block_on(py, self.fluree.get()?.ledger_exists(&id))?.map_err(api_error)
    }

    /// The canonical id of an existing ledger.
    fn ledger(&self, py: Python<'_>, ledger: &str) -> PyResult<String> {
        let id = canonical(ledger)?;
        if block_on(py, self.fluree.get()?.ledger_exists(&id))?.map_err(api_error)? {
            Ok(id)
        } else {
            Err(not_found(format!("ledger {ledger:?} does not exist")))
        }
    }

    fn ledgers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let records = block_on(py, self.fluree.get()?.nameservice().all_records())?
            .map_err(|e| api_error(e.into()))?;
        let mut ids: Vec<String> = records
            .into_iter()
            .filter(|r| !r.retracted)
            .map(|r| r.ledger_id.to_string())
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Drop a whole ledger, every branch. Takes the bare name: the engine
    /// rejects a `name:branch` id here rather than guess at the intent.
    fn drop(&self, py: Python<'_>, ledger: &str) -> PyResult<()> {
        block_on(py, self.fluree.get()?.drop_ledger(ledger, DropMode::Hard))?
            .map(drop)
            .map_err(api_error)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.fluree.close(py)
    }

    /// Every branch of `ledger`'s ledger, by name.
    fn branches<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Vec<Bound<'py, PyDict>>> {
        branch::list(py, self.fluree.get()?, &canonical(ledger)?)
    }

    /// Branch `name` off `ledger` at its head or at `at`; returns its id.
    #[pyo3(signature = (ledger, name, at = None))]
    fn create_branch(
        &self,
        py: Python<'_>,
        ledger: &str,
        name: &str,
        at: Option<&Bound<'_, PyTuple>>,
    ) -> PyResult<String> {
        branch::create(
            py,
            self.fluree.get()?,
            &canonical(ledger)?,
            name,
            time_spec(at)?,
        )
    }

    fn drop_branch(&self, py: Python<'_>, ledger: &str) -> PyResult<()> {
        branch::drop(py, self.fluree.get()?, &canonical(ledger)?)
    }

    fn merge<'py>(
        &self,
        py: Python<'py>,
        target: &str,
        source: &str,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::merge(
            py,
            self.fluree.get()?,
            &canonical(target)?,
            source,
            strategy,
        )
    }

    fn rebase<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::rebase(py, self.fluree.get()?, &canonical(ledger)?, strategy)
    }

    fn revert<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        commits: &Bound<'py, PyList>,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::revert(
            py,
            self.fluree.get()?,
            &canonical(ledger)?,
            commits,
            strategy,
        )
    }

    fn merge_preview<'py>(
        &self,
        py: Python<'py>,
        target: &str,
        source: &str,
        options: branch::MergePreviewArgs,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::merge_preview(py, self.fluree.get()?, &canonical(target)?, source, options)
    }

    fn revert_preview<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        commits: &Bound<'py, PyList>,
        options: branch::RevertPreviewArgs,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::revert_preview(
            py,
            self.fluree.get()?,
            &canonical(ledger)?,
            commits,
            options,
        )
    }

    /// Commit one write; see [`operation`] for `op`, `kind` and `payload`.
    #[pyo3(signature = (ledger, op, kind, payload, policy = None, message = None, params = None))]
    #[allow(clippy::too_many_arguments)]
    fn transact<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        op: &str,
        kind: &str,
        payload: &Bound<'py, PyAny>,
        policy: Option<&Bound<'py, PyAny>>,
        message: Option<String>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let id = canonical(ledger)?;
        let policy = governance(policy)?;
        let operation = operation(op, kind, payload, params)?;
        let fluree = self.fluree.get()?;
        let receipt = block_on(py, async {
            let policy = write_policy(fluree, &id, policy.as_ref()).await?;
            let graph = fluree.graph(&id);
            let tx = graph.transact();
            let tx = match &operation {
                TxnOperation::Insert(v) => tx.insert(v),
                TxnOperation::InsertTurtle(s) => tx.insert_turtle(s),
                TxnOperation::Upsert(v) => tx.upsert(v),
                TxnOperation::UpsertTurtle(s) => tx.upsert_turtle(s),
                TxnOperation::Update(v) => tx.update(v),
                TxnOperation::SparqlUpdate(s, None) => tx.sparql_update(s),
                TxnOperation::SparqlUpdate(s, Some(params)) => {
                    tx.sparql_update_with_params(s, params)
                }
            };
            let tx = tx.commit_opts(commit_opts(message));
            let tx = match policy {
                Some(ctx) => tx.policy(ctx),
                None => tx,
            };
            tx.commit().await.map(|out| out.receipt)
        })?
        .map_err(api_error)?;
        receipt_to_py(py, &receipt)
    }

    fn validate<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        options: ops::ValidateArgs,
    ) -> PyResult<Bound<'py, PyAny>> {
        ops::validate(py, self.fluree.get()?, &canonical(ledger)?, options)
    }

    fn index_status<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Bound<'py, PyDict>> {
        ops::index_status(py, self.fluree.get()?, &canonical(ledger)?)
    }

    #[pyo3(signature = (ledger, timeout = None))]
    fn index(&self, py: Python<'_>, ledger: &str, timeout: Option<f64>) -> PyResult<i64> {
        ops::index(py, self.fluree.get()?, &canonical(ledger)?, timeout)
    }

    fn reindex(&self, py: Python<'_>, ledger: &str) -> PyResult<i64> {
        ops::reindex(py, self.fluree.get()?, &canonical(ledger)?)
    }

    #[pyo3(signature = (ledger, max_commits = None))]
    fn verify<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        max_commits: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        ops::verify(py, self.fluree.get()?, &canonical(ledger)?, max_commits)
    }

    fn sweep<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        ops::sweep(py, self.fluree.get()?, &canonical(ledger)?, dry_run)
    }

    /// Make `graph` (the default graph when `None`) hold exactly `payload`,
    /// committing only the difference. `kind` is `"jsonld"` or `"turtle"`
    /// (Turtle, N-Triples or TriG text). A dry run commits nothing and
    /// reports what the commit would hold.
    #[pyo3(signature = (ledger, kind, payload, graph = None, allow_empty = false, dry_run = false, policy = None, message = None))]
    #[allow(clippy::too_many_arguments)]
    fn sync<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        kind: &str,
        payload: &Bound<'py, PyAny>,
        graph: Option<String>,
        allow_empty: bool,
        dry_run: bool,
        policy: Option<&Bound<'py, PyAny>>,
        message: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let id = canonical(ledger)?;
        let policy = governance(policy)?;
        let payload = match kind {
            "jsonld" => Payload::Json(to_jsonld(payload)?),
            "turtle" => Payload::Text(payload.extract()?),
            other => return Err(invalid_request(format!("cannot sync {other:?} data"))),
        };
        let graph = match graph {
            Some(iri) => GraphSel::Graph(iri),
            None => GraphSel::Default,
        };
        let opts = SyncGraphOpts {
            dry_run,
            allow_empty,
            message,
        };
        let fluree = self.fluree.get()?;
        let report = block_on(py, async {
            let policy = write_policy(fluree, &id, policy.as_ref()).await?;
            let payload = match &payload {
                Payload::Json(json) => GraphPayload::JsonLd(json),
                Payload::Text(text) => GraphPayload::Rdf(text),
            };
            fluree
                .sync_graph_with(&id, &graph, payload, opts, TxnOpts::default(), policy)
                .await
        })?
        .map_err(api_error)?;
        let commit = PyDict::new(py);
        commit.set_item("t", report.t)?;
        commit.set_item("id", report.commit_id.as_ref().map(ToString::to_string))?;
        commit.set_item(
            "digest",
            report.commit_id.as_ref().map(ContentId::digest_hex),
        )?;
        commit.set_item("asserts", report.asserted)?;
        commit.set_item("retracts", report.retracted)?;
        Ok(commit)
    }

    /// A Cypher read of `ledger` at `at`; returns `(columns, rows)`.
    #[pyo3(signature = (ledger, cypher, params = None, at = None, policy = None, controls = None))]
    #[allow(clippy::too_many_arguments)]
    fn cypher_query<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        cypher::require_read(cypher)?;
        let id = canonical(ledger)?;
        let params = cypher::params(params)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let fluree = self.fluree.get()?;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        let controls = controls.unwrap_or_default();
        cypher::read(py, fluree, &db, cypher, params.as_ref(), controls)
    }

    /// The plan a Cypher read would run with.
    #[pyo3(signature = (ledger, cypher, params = None, at = None, policy = None))]
    fn explain_cypher<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let params = cypher::params(params)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let fluree = self.fluree.get()?;
        let plan = block_on(py, async {
            let db = load(fluree, &id, spec, policy.as_ref()).await?;
            fluree.explain_cypher(&db, cypher, params.as_ref()).await
        })?
        .map_err(api_error)?;
        from_json(py, &plan)
    }

    /// Open a transaction on `ledger`; its writes are checked against
    /// `policy` and its reads filtered by it.
    #[pyo3(signature = (ledger, policy = None))]
    fn begin(
        &self,
        py: Python<'_>,
        ledger: &str,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Transaction> {
        Transaction::begin(py, &self.fluree, &canonical(ledger)?, governance(policy)?)
    }

    #[pyo3(signature = (ledger, sparql, at = None, policy = None, controls = None, params = None))]
    #[allow(clippy::too_many_arguments)]
    fn query_sparql<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        sparql: &str,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let params = sparql_params(params)?;
        sparql_policy_free(sparql)?;
        let fluree = self.fluree.get()?;
        let controls = controls.unwrap_or_default();
        let (id, policy) = (&id, policy.as_ref());
        let answer = controls.run(py, |cancel, controls| async move {
            let db = load(fluree, id, spec, policy).await?;
            let builder = GraphSnapshotQueryBuilder::new_from_parts(fluree, &db)
                .sparql(sparql)
                .format(FormatterConfig::sparql_json());
            let builder = match params {
                Some(params) => builder.params(params),
                None => builder,
            };
            execute!(controls, cancel, builder)
        })?;
        answer.into_py(py, Some(sparql))
    }

    #[pyo3(signature = (ledger, query, at = None, policy = None, controls = None))]
    fn query_jsonld<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        query: &Bound<'py, PyAny>,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let query = to_jsonld(query)?;
        jsonld_policy_free(&query)?;
        jsonld_reads_ledger(&query, &id).map_err(api_error)?;
        let fluree = self.fluree.get()?;
        let controls = controls.unwrap_or_default();
        let (id, policy, query) = (&id, policy.as_ref(), &query);
        let answer = controls.run(py, |cancel, controls| async move {
            let db = load(fluree, id, spec, policy).await?;
            execute!(
                controls,
                cancel,
                GraphSnapshotQueryBuilder::new_from_parts(fluree, &db).jsonld(query)
            )
        })?;
        answer.into_py(py, None)
    }

    /// A connection-level SPARQL query: its `FROM` / `FROM NAMED` / `TO`
    /// clauses pick the ledgers and times, so it can span ledgers or read a
    /// ledger's history.
    #[pyo3(signature = (sparql, policy = None, controls = None, params = None))]
    fn query_sparql_from<'py>(
        &self,
        py: Python<'py>,
        sparql: &str,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let policy = governance(policy)?;
        let params = sparql_params(params)?;
        let fluree = self.fluree.get()?;
        let controls = controls.unwrap_or_default();
        let answer = controls.run(py, |cancel, controls| async move {
            let builder = fluree
                .query_from()
                .sparql(sparql)
                .format(FormatterConfig::sparql_json());
            let builder = match policy {
                Some(opts) => builder.connection_opts(opts),
                None => builder,
            };
            let builder = match params {
                Some(params) => builder.params(params),
                None => builder,
            };
            execute!(controls, cancel, builder)
        })?;
        answer.into_py(py, Some(sparql))
    }

    /// A connection-level JSON-LD query, its ledgers named by `from`; policy
    /// rides in the query's `opts`.
    #[pyo3(signature = (query, controls = None))]
    fn query_jsonld_from<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'py, PyAny>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = to_jsonld(query)?;
        let fluree = self.fluree.get()?;
        let controls = controls.unwrap_or_default();
        let query = &query;
        let answer = controls.run(py, |cancel, controls| async move {
            execute!(controls, cancel, fluree.query_from().jsonld(query))
        })?;
        answer.into_py(py, None)
    }

    /// Start a streaming SELECT; rows are read from the returned stream.
    #[pyo3(signature = (ledger, query, at = None, policy = None, controls = None, params = None))]
    #[allow(clippy::too_many_arguments)]
    fn stream(
        &self,
        py: Python<'_>,
        ledger: &str,
        query: &Bound<'_, PyAny>,
        at: Option<&Bound<'_, PyTuple>>,
        policy: Option<&Bound<'_, PyAny>>,
        controls: Option<Controls>,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RowStream> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let query = QueryText::from_py(query, params)?;
        let fluree = self.fluree.get()?;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        start_stream(py, &self.fluree, db, query, controls.unwrap_or_default())
    }

    /// The query plan for a SPARQL (text) or JSON-LD (object) query.
    #[pyo3(signature = (ledger, query, at = None, policy = None, params = None))]
    fn explain<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        query: &Bound<'py, PyAny>,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let query = QueryText::from_py(query, params)?;
        let fluree = self.fluree.get()?;
        let plan = block_on(py, async {
            let db = load(fluree, &id, spec, policy.as_ref()).await?;
            query.explain(fluree, &db).await
        })?
        .map_err(api_error)?;
        from_json(py, &plan)
    }

    #[pyo3(signature = (ledger, at = None, policy = None))]
    fn snapshot(
        &self,
        py: Python<'_>,
        ledger: &str,
        at: Option<&Bound<'_, PyTuple>>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Snapshot> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let fluree = self.fluree.get()?;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        Ok(Snapshot::new(&self.fluree, db))
    }

    /// Commit summaries, newest first, and the total number of commits.
    #[pyo3(signature = (ledger, limit = None))]
    fn log<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        limit: Option<usize>,
    ) -> PyResult<(Vec<Bound<'py, PyDict>>, usize)> {
        let id = canonical(ledger)?;
        let (summaries, total) =
            block_on(py, self.fluree.get()?.commit_log(&id, limit))?.map_err(api_error)?;
        let commits = summaries
            .iter()
            .map(|s| commit_summary(py, s))
            .collect::<PyResult<_>>()?;
        Ok((commits, total))
    }

    /// One commit and its changes. `commit` is a `t` (int), a full commit id,
    /// or a hex-digest prefix.
    #[pyo3(signature = (ledger, commit, policy = None))]
    fn commit_detail<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        commit: &Bound<'py, PyAny>,
        policy: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let id = canonical(ledger)?;
        let policy = governance(policy)?;
        let reference = commit_ref(commit)?;
        let fluree = self.fluree.get()?;
        let detail = block_on(py, async {
            let graph = fluree.graph(&id);
            let builder = match &reference {
                CommitRef::T(t) => graph.commit_t(*t),
                CommitRef::Exact(cid) => graph.commit(cid),
                CommitRef::Prefix(prefix) => graph.commit_prefix(prefix),
            };
            // An empty context leaves IRIs whole rather than compacted to
            // prefixes derived from the namespace table.
            let builder = builder.context(ParsedContext::default());
            let builder = match &policy {
                Some(opts) => builder.governance(opts),
                None => builder,
            };
            builder.execute().await
        })?
        .map_err(api_error)?;
        commit_detail_to_py(py, &detail)
    }

    /// Write the ledger as RDF to `path`, or return it as text when `path`
    /// is `None`. Returns `(text, triples_written)`.
    #[pyo3(signature = (ledger, format, path = None, graph = None, all_graphs = false, context = None, at = None))]
    #[allow(clippy::too_many_arguments)]
    fn export(
        &self,
        py: Python<'_>,
        ledger: &str,
        format: &str,
        path: Option<PathBuf>,
        graph: Option<&str>,
        all_graphs: bool,
        context: Option<&Bound<'_, PyAny>>,
        at: Option<&Bound<'_, PyTuple>>,
    ) -> PyResult<(Option<String>, u64)> {
        let id = canonical(ledger)?;
        let format = match format {
            "turtle" => ExportFormat::Turtle,
            "trig" => ExportFormat::TriG,
            "ntriples" => ExportFormat::NTriples,
            "nquads" => ExportFormat::NQuads,
            "jsonld" => ExportFormat::JsonLd,
            other => return Err(invalid_request(format!("unknown export format {other:?}"))),
        };
        let context = context.map(to_json).transpose()?;
        let spec = time_spec(at)?;
        let fluree = self.fluree.get()?;
        block_on(py, async {
            let mut builder = fluree.export(&id).format(format).as_of(spec);
            if all_graphs {
                builder = builder.all_graphs();
            }
            if let Some(graph) = graph {
                builder = builder.graph(graph);
            }
            if let Some(context) = &context {
                builder = builder.context(context);
            }
            match &path {
                Some(path) => {
                    let file = std::fs::File::create(path).map_err(|e| {
                        ApiError::internal(format!("cannot write {}: {e}", path.display()))
                    })?;
                    let mut out = std::io::BufWriter::new(file);
                    let stats = builder.write_to(&mut out).await?;
                    std::io::Write::flush(&mut out).map_err(|e| {
                        ApiError::internal(format!("cannot write {}: {e}", path.display()))
                    })?;
                    Ok((None, stats.triples_written))
                }
                None => {
                    let mut out = Vec::new();
                    let stats = builder.write_to(&mut out).await?;
                    let text = String::from_utf8(out)
                        .map_err(|e| ApiError::internal(format!("export is not UTF-8: {e}")))?;
                    Ok((Some(text), stats.triples_written))
                }
            }
        })?
        .map_err(api_error)
    }

    /// Write a self-contained ledger archive (`.flpack`) to `path`.
    #[pyo3(signature = (ledger, path, include_indexes = true))]
    fn archive(
        &self,
        py: Python<'_>,
        ledger: &str,
        path: PathBuf,
        include_indexes: bool,
    ) -> PyResult<()> {
        let id = canonical(ledger)?;
        let fluree = self.fluree.get()?;
        block_on(py, async {
            let io = |e: std::io::Error| ApiError::internal(format!("{}: {e}", path.display()));
            let mut file = tokio::fs::File::create(&path).await.map_err(io)?;
            fluree
                .archive_ledger(&id, include_indexes, &mut file)
                .await?;
            tokio::io::AsyncWriteExt::flush(&mut file).await.map_err(io)
        })?
        .map_err(api_error)
    }

    /// Create `ledger` from a `.flpack` archive. Returns its canonical id.
    fn restore(&self, py: Python<'_>, path: PathBuf, ledger: &str) -> PyResult<String> {
        let id = canonical(ledger)?;
        let fluree = self.fluree.get()?;
        block_on(py, async {
            let mut file = tokio::fs::File::open(&path)
                .await
                .map_err(|e| ApiError::internal(format!("{}: {e}", path.display())))?;
            fluree.restore_ledger(&id, &mut file).await
        })?
        .map(|restored| restored.ledger_id)
        .map_err(api_error)
    }

    /// The ledger's default JSON-LD context, or `None` if it has none.
    fn context<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
        let id = canonical(ledger)?;
        block_on(py, self.fluree.get()?.get_default_context(&id))?
            .map_err(api_error)?
            .map(|context| from_json(py, &context))
            .transpose()
    }

    fn set_context(
        &self,
        py: Python<'_>,
        ledger: &str,
        context: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let id = canonical(ledger)?;
        let context = to_json(context)?;
        block_on(py, self.fluree.get()?.set_default_context(&id, &context))?
            .map(drop)
            .map_err(api_error)
    }

    /// Register an Iceberg table as a graph source.
    fn map_iceberg<'py>(
        &self,
        py: Python<'py>,
        common: graph_source::Common,
        spec: graph_source::IcebergSpec,
    ) -> PyResult<Bound<'py, PyDict>> {
        graph_source::map_iceberg(py, self.fluree.get()?, common, spec)
    }

    /// Register Delta tables as a graph source.
    fn map_delta<'py>(
        &self,
        py: Python<'py>,
        common: graph_source::Common,
        spec: graph_source::DeltaSpec,
    ) -> PyResult<Bound<'py, PyDict>> {
        graph_source::map_delta(py, self.fluree.get()?, common, spec)
    }

    /// Register tables behind a Trino-protocol SQL endpoint as a graph source.
    fn map_sql<'py>(
        &self,
        py: Python<'py>,
        common: graph_source::Common,
        spec: graph_source::SqlSpec,
    ) -> PyResult<Bound<'py, PyDict>> {
        graph_source::map_sql(py, self.fluree.get()?, common, spec)
    }

    fn graph_sources<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        graph_source::list(py, self.fluree.get()?)
    }

    fn drop_graph_source(&self, py: Python<'_>, name: &str, branch: &str) -> PyResult<()> {
        graph_source::drop(py, self.fluree.get()?, name, branch)
    }

    #[pyo3(signature = (source, into, full = false))]
    fn materialize<'py>(
        &self,
        py: Python<'py>,
        source: &str,
        into: &str,
        full: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        graph_source::materialize(py, self.fluree.get()?, source, &canonical(into)?, full)
    }

    /// A SPARQL (text) or JSON-LD (object) query of graph `graph` — a ledger,
    /// or a graph source, which only this path resolves.
    #[pyo3(signature = (graph, query, controls = None, params = None))]
    fn query_graph<'py>(
        &self,
        py: Python<'py>,
        graph: &str,
        query: &Bound<'py, PyAny>,
        controls: Option<Controls>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = QueryText::from_py(query, params)?;
        let fluree = self.fluree.get()?;
        let controls = controls.unwrap_or_default();
        let (graph, query_ref) = (graph, &query);
        let answer = controls.run(py, |cancel, controls| async move {
            let handle = fluree.graph(graph);
            let builder = handle.query().with_r2rml();
            match query_ref {
                QueryText::Sparql(sparql, params) => {
                    let builder = builder
                        .sparql(sparql)
                        .format(FormatterConfig::sparql_json());
                    let builder = match params {
                        Some(params) => builder.params(params.clone()),
                        None => builder,
                    };
                    execute!(controls, cancel, builder)
                }
                QueryText::JsonLd(json) => execute!(controls, cancel, builder.jsonld(json)),
            }
        })?;
        match &query {
            QueryText::Sparql(sparql, _) => answer.into_py(py, Some(sparql)),
            QueryText::JsonLd(_) => answer.into_py(py, None),
        }
    }

    /// Retract every fact in named graph `graph` in one commit; the dict
    /// `fluree.Commit` is built from (`id` `None` when it held nothing).
    fn drop_graph<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        graph: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let id = canonical(ledger)?;
        let report =
            block_on(py, self.fluree.get()?.drop_named_graph(&id, graph))?.map_err(api_error)?;
        let commit = PyDict::new(py);
        commit.set_item("t", report.t)?;
        commit.set_item("id", report.commit_id.as_ref().map(ToString::to_string))?;
        commit.set_item(
            "digest",
            report.commit_id.as_ref().map(ContentId::digest_hex),
        )?;
        commit.set_item("asserts", 0)?;
        commit.set_item("retracts", report.retracted)?;
        Ok(commit)
    }

    fn info<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let info =
            block_on(py, self.fluree.get()?.ledger_info(&id).execute())?.map_err(api_error)?;
        from_json(py, &info)
    }
}

fn commit_detail_to_py<'py>(
    py: Python<'py>,
    detail: &CommitDetail,
) -> PyResult<Bound<'py, PyDict>> {
    let commit = PyDict::new(py);
    commit.set_item("t", detail.t)?;
    commit.set_item("id", &detail.id)?;
    let digest = detail
        .id
        .parse::<ContentId>()
        .map(|cid| cid.digest_hex())
        .map_err(|e| fluree_error(e.to_string()))?;
    commit.set_item("digest", digest)?;
    commit.set_item("asserts", detail.asserts)?;
    commit.set_item("retracts", detail.retracts)?;
    commit.set_item("time", detail.time.as_deref())?;
    commit.set_item("parents", &detail.parents)?;
    let flakes = detail
        .flakes
        .iter()
        .map(|f| flake(py, f))
        .collect::<PyResult<Vec<_>>>()?;
    commit.set_item("flakes", flakes)?;
    Ok(commit)
}

/// A ledger view frozen at one `t`: every query sees the same state.
#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct Snapshot {
    fluree: Database,
    db: InRuntime<GraphDb>,
}

impl Snapshot {
    pub(crate) fn new(fluree: &Database, db: GraphDb) -> Self {
        Self {
            fluree: fluree.clone(),
            db: InRuntime::new(db),
        }
    }
}

#[pymethods]
impl Snapshot {
    #[getter]
    fn ledger(&self) -> PyResult<&str> {
        Ok(&self.db.get()?.ledger_id)
    }

    #[getter]
    fn t(&self) -> PyResult<i64> {
        Ok(self.db.get()?.t)
    }

    /// A Cypher read of this snapshot.
    #[pyo3(signature = (cypher, params = None, controls = None))]
    fn cypher_query<'py>(
        &self,
        py: Python<'py>,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        cypher::require_read(cypher)?;
        let params = cypher::params(params)?;
        let controls = controls.unwrap_or_default();
        cypher::read(
            py,
            self.fluree.get()?,
            self.db.get()?,
            cypher,
            params.as_ref(),
            controls,
        )
    }

    #[pyo3(signature = (cypher, params = None))]
    fn explain_cypher<'py>(
        &self,
        py: Python<'py>,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = cypher::params(params)?;
        let plan = block_on(
            py,
            self.fluree
                .get()?
                .explain_cypher(self.db.get()?, cypher, params.as_ref()),
        )?
        .map_err(api_error)?;
        from_json(py, &plan)
    }

    #[pyo3(signature = (sparql, controls = None, params = None))]
    fn query_sparql<'py>(
        &self,
        py: Python<'py>,
        sparql: &str,
        controls: Option<Controls>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = sparql_params(params)?;
        sparql_policy_free(sparql)?;
        let controls = controls.unwrap_or_default();
        let (fluree, db) = (self.fluree.get()?, self.db.get()?);
        let answer = controls.run(py, |cancel, controls| async move {
            let builder = GraphSnapshotQueryBuilder::new_from_parts(fluree, db)
                .sparql(sparql)
                .format(FormatterConfig::sparql_json());
            let builder = match params {
                Some(params) => builder.params(params),
                None => builder,
            };
            execute!(controls, cancel, builder)
        })?;
        answer.into_py(py, Some(sparql))
    }

    #[pyo3(signature = (query, controls = None))]
    fn query_jsonld<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'py, PyAny>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = to_jsonld(query)?;
        jsonld_policy_free(&query)?;
        let controls = controls.unwrap_or_default();
        let (fluree, db, query) = (self.fluree.get()?, self.db.get()?, &query);
        jsonld_reads_ledger(query, &db.ledger_id).map_err(api_error)?;
        let answer = controls.run(py, |cancel, controls| async move {
            execute!(
                controls,
                cancel,
                GraphSnapshotQueryBuilder::new_from_parts(fluree, db).jsonld(query)
            )
        })?;
        answer.into_py(py, None)
    }

    #[pyo3(signature = (query, controls = None, params = None))]
    fn stream(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyAny>,
        controls: Option<Controls>,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RowStream> {
        let query = QueryText::from_py(query, params)?;
        start_stream(
            py,
            &self.fluree,
            self.db.get()?.clone(),
            query,
            controls.unwrap_or_default(),
        )
    }

    #[pyo3(signature = (query, params = None))]
    fn explain<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'py, PyAny>,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = QueryText::from_py(query, params)?;
        let plan =
            block_on(py, query.explain(self.fluree.get()?, self.db.get()?))?.map_err(api_error)?;
        from_json(py, &plan)
    }
}

/// Plan a streaming SELECT against `db` and start its producer.
fn start_stream(
    py: Python<'_>,
    database: &Database,
    db: GraphDb,
    query: QueryText,
    controls: Controls,
) -> PyResult<RowStream> {
    controls.checked_timeout()?;
    let fluree = database.get()?;
    // The producer reads a dataset, which keeps the view's policy with it: the
    // graphs a SPARQL `FROM` names in this ledger, as `query()` reads them, or
    // else the view alone.
    let (input, columns, params, dataset) = match query {
        QueryText::Sparql(sparql, params) => {
            let columns = sparql_columns(&sparql);
            let dataset = fluree
                .sparql_dataset_within_ledger(&db, &sparql)
                .map_err(api_error)?;
            let dataset = dataset.unwrap_or_else(|| DataSetDb::single(db));
            (OwnedStreamQuery::Sparql(sparql), columns, params, dataset)
        }
        QueryText::JsonLd(json) => {
            jsonld_reads_ledger(&json, &db.ledger_id).map_err(api_error)?;
            let columns = jsonld_columns(&json);
            let dataset = DataSetDb::single(db);
            (OwnedStreamQuery::JsonLd(json), columns, None, dataset)
        }
    };
    let cancellation = controls.cancellation();
    let options = QueryExecutionOptions::new().with_cancellation(cancellation.clone());
    let options = match params {
        Some(params) => options.with_params(params),
        None => options,
    };
    let plan = block_on(
        py,
        fluree.plan_stream_query_dataset_with_options(&dataset, &input, &options),
    )?
    .map_err(api_error)?;
    let tracker = controls
        .tracking()
        .map_or_else(Tracker::disabled, Tracker::new);
    let (records, received) = tokio::sync::mpsc::channel(CHANNEL_DEPTH);
    let producer = fluree.clone();
    runtime()?.spawn(async move {
        producer
            .run_stream_query_dataset(dataset, plan, tracker, options, records)
            .await;
    });
    Ok(RowStream::new(
        database.clone(),
        received,
        cancellation,
        columns,
        controls.timeout(),
    ))
}

/// A query to explain or stream: SPARQL text with its parameters, or a
/// JSON-LD object.
enum QueryText {
    Sparql(String, Option<SparqlParamMap>),
    JsonLd(JsonValue),
}

impl QueryText {
    fn from_py(query: &Bound<'_, PyAny>, params: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let params = sparql_params(params)?;
        match query.extract::<String>() {
            Ok(sparql) => {
                sparql_policy_free(&sparql)?;
                Ok(Self::Sparql(sparql, params))
            }
            Err(_) if params.is_some() => Err(invalid_request(
                "parameters apply to SPARQL and Cypher queries; a JSON-LD query takes its values in the query",
            )),
            Err(_) => {
                let json = to_jsonld(query)?;
                jsonld_policy_free(&json)?;
                Ok(Self::JsonLd(json))
            }
        }
    }

    async fn explain(&self, fluree: &Fluree, db: &GraphDb) -> fluree_db_api::Result<JsonValue> {
        match self {
            Self::Sparql(sparql, params) => {
                fluree
                    .explain_sparql_with_params(db, sparql, params.as_ref())
                    .await
            }
            Self::JsonLd(query) => {
                jsonld_reads_ledger(query, &db.ledger_id)?;
                fluree.explain(db, query).await
            }
        }
    }
}
