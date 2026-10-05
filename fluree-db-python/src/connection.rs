//! The native connection and snapshot handles behind `fluree.Connection`.
//!
//! These take ledger ids and plain data and return plain data; the Python
//! layer owns argument handling, defaults, and result objects.

use crate::branch;
use crate::convert::{
    commit_ref, commit_summary, flake, from_json, jsonld_columns, sparql_columns, time_spec,
    to_json,
};
use crate::cypher;
use crate::error::{api_error, fluree_error, invalid_request, not_found};
use crate::ops;
use crate::query::{execute, Controls};
use crate::runtime::{block_on, enter, runtime, InRuntime};
use crate::stream::{RowStream, CHANNEL_DEPTH};
use crate::transaction::Transaction;
use fluree_db_api::{
    build_transact_policy_context, export::ExportFormat, ApiError, CommitDetail, CommitReceipt,
    CommitRef, DataSetDb, DropMode, Fluree, FlureeBuilder, FormatterConfig, GovernanceOptions,
    GraphDb, GraphSnapshotQueryBuilder, OwnedStreamQuery, ParsedContext, PolicyContext,
    QueryExecutionOptions, TimeSpec, Tracker, TxnOperation,
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

#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct Connection {
    fluree: InRuntime<Fluree>,
}

/// A write from its operation (`insert`, `upsert`, `update`) and payload
/// format: `"jsonld"` (a JSON-able object), `"turtle"` (insert and upsert),
/// or `"sparql"` (update).
/// A sync payload: JSON-LD, or Turtle / N-Triples / TriG text.
enum Payload {
    Json(JsonValue),
    Text(String),
}

pub(crate) fn operation(
    op: &str,
    kind: &str,
    payload: &Bound<'_, PyAny>,
) -> PyResult<TxnOperation> {
    Ok(match (op, kind) {
        ("insert", "jsonld") => TxnOperation::Insert(to_json(payload)?),
        ("upsert", "jsonld") => TxnOperation::Upsert(to_json(payload)?),
        ("update", "jsonld") => TxnOperation::Update(to_json(payload)?),
        ("insert", "turtle") => TxnOperation::InsertTurtle(payload.extract()?),
        ("upsert", "turtle") => TxnOperation::UpsertTurtle(payload.extract()?),
        ("update", "sparql") => TxnOperation::SparqlUpdate(payload.extract()?),
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
            fluree: InRuntime::new(FlureeBuilder::memory().build_memory()),
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
            fluree: InRuntime::new(fluree),
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
            fluree: InRuntime::new(fluree),
        })
    }

    #[pyo3(signature = (ledger, source = None))]
    fn create(&self, py: Python<'_>, ledger: &str, source: Option<PathBuf>) -> PyResult<String> {
        let id = canonical(ledger)?;
        let fluree = &*self.fluree;
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
        block_on(py, self.fluree.ledger_exists(&id))?.map_err(api_error)
    }

    /// The canonical id of an existing ledger.
    fn ledger(&self, py: Python<'_>, ledger: &str) -> PyResult<String> {
        let id = canonical(ledger)?;
        if block_on(py, self.fluree.ledger_exists(&id))?.map_err(api_error)? {
            Ok(id)
        } else {
            Err(not_found(format!("ledger {ledger:?} does not exist")))
        }
    }

    fn ledgers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let records = block_on(py, self.fluree.nameservice().all_records())?
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
        block_on(py, self.fluree.drop_ledger(ledger, DropMode::Hard))?
            .map(drop)
            .map_err(api_error)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, self.fluree.disconnect())
    }

    /// Every branch of `ledger`'s ledger, by name.
    fn branches<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Vec<Bound<'py, PyDict>>> {
        branch::list(py, &self.fluree, &canonical(ledger)?)
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
        branch::create(py, &self.fluree, &canonical(ledger)?, name, time_spec(at)?)
    }

    fn drop_branch(&self, py: Python<'_>, ledger: &str) -> PyResult<()> {
        branch::drop(py, &self.fluree, &canonical(ledger)?)
    }

    fn merge<'py>(
        &self,
        py: Python<'py>,
        target: &str,
        source: &str,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::merge(py, &self.fluree, &canonical(target)?, source, strategy)
    }

    fn rebase<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::rebase(py, &self.fluree, &canonical(ledger)?, strategy)
    }

    fn revert<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        commits: &Bound<'py, PyList>,
        strategy: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::revert(py, &self.fluree, &canonical(ledger)?, commits, strategy)
    }

    fn merge_preview<'py>(
        &self,
        py: Python<'py>,
        target: &str,
        source: &str,
        options: branch::MergePreviewArgs,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::merge_preview(py, &self.fluree, &canonical(target)?, source, options)
    }

    fn revert_preview<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        commits: &Bound<'py, PyList>,
        options: branch::RevertPreviewArgs,
    ) -> PyResult<Bound<'py, PyDict>> {
        branch::revert_preview(py, &self.fluree, &canonical(ledger)?, commits, options)
    }

    /// Commit one write; see [`operation`] for `op`, `kind` and `payload`.
    #[pyo3(signature = (ledger, op, kind, payload, policy = None, message = None))]
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
    ) -> PyResult<Bound<'py, PyDict>> {
        let id = canonical(ledger)?;
        let policy = governance(policy)?;
        let operation = operation(op, kind, payload)?;
        let fluree = &*self.fluree;
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
                TxnOperation::SparqlUpdate(s) => tx.sparql_update(s),
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
        ops::validate(py, &self.fluree, &canonical(ledger)?, options)
    }

    fn index_status<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Bound<'py, PyDict>> {
        ops::index_status(py, &self.fluree, &canonical(ledger)?)
    }

    #[pyo3(signature = (ledger, timeout = None))]
    fn index(&self, py: Python<'_>, ledger: &str, timeout: Option<f64>) -> PyResult<i64> {
        ops::index(py, &self.fluree, &canonical(ledger)?, timeout)
    }

    fn reindex(&self, py: Python<'_>, ledger: &str) -> PyResult<i64> {
        ops::reindex(py, &self.fluree, &canonical(ledger)?)
    }

    #[pyo3(signature = (ledger, max_commits = None))]
    fn verify<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        max_commits: Option<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        ops::verify(py, &self.fluree, &canonical(ledger)?, max_commits)
    }

    fn sweep<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        ops::sweep(py, &self.fluree, &canonical(ledger)?, dry_run)
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
            "jsonld" => Payload::Json(to_json(payload)?),
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
        let fluree = &*self.fluree;
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
    ) -> PyResult<Bound<'py, PyTuple>> {
        cypher::require_read(cypher)?;
        let id = canonical(ledger)?;
        let params = cypher::params(params)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let fluree = &*self.fluree;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        let controls = controls.unwrap_or_default();
        cypher::read(py, fluree, &db, cypher, params.as_ref(), controls)
    }

    /// A Cypher write — one statement or a `;` script — committed all or
    /// nothing. Returns `(commit, (columns, rows))`, the rows a `RETURN`
    /// produced.
    #[pyo3(signature = (ledger, cypher, params = None, policy = None))]
    fn cypher_update<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
        policy: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyTuple>)> {
        if !cypher::is_write(cypher)? {
            return Err(invalid_request(
                "this Cypher statement only reads; run it with query()",
            ));
        }
        let id = canonical(ledger)?;
        let params = cypher::params(params)?;
        let governance = governance(policy)?.unwrap_or_default();
        cypher::write(py, &self.fluree, &id, cypher, params.as_ref(), governance)
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
        let fluree = &*self.fluree;
        let plan = block_on(py, async {
            let db = load(fluree, &id, spec, policy.as_ref()).await?;
            fluree.explain_cypher(&db, cypher, params.as_ref()).await
        })?
        .map_err(api_error)?;
        from_json(py, &plan)
    }

    /// Open an explicit Cypher transaction on `ledger`.
    #[pyo3(signature = (ledger, policy = None))]
    fn begin_cypher(
        &self,
        py: Python<'_>,
        ledger: &str,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<cypher::CypherTransaction> {
        let governance = governance(policy)?.unwrap_or_default();
        cypher::CypherTransaction::begin(py, &self.fluree, &canonical(ledger)?, governance)
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

    #[pyo3(signature = (ledger, sparql, at = None, policy = None, controls = None))]
    fn query_sparql<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        sparql: &str,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let fluree = &*self.fluree;
        let controls = controls.unwrap_or_default();
        let (id, policy) = (&id, policy.as_ref());
        let answer = controls.run(py, |cancel, controls| async move {
            let db = load(fluree, id, spec, policy).await?;
            execute!(
                controls,
                cancel,
                GraphSnapshotQueryBuilder::new_from_parts(fluree, &db)
                    .sparql(sparql)
                    .format(FormatterConfig::sparql_json())
            )
        })?;
        answer.into_py(py, Some(sparql))
    }

    /// A JSON-LD query through the ledger's query builder, which honors the
    /// query's own `opts` (identity, policy) as well as configured defaults;
    /// the Python layer folds a governed ledger's policy into those `opts`.
    #[pyo3(signature = (ledger, query, at = None, controls = None))]
    fn query_jsonld<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        query: &Bound<'py, PyAny>,
        at: Option<&Bound<'py, PyTuple>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let mut query = to_json(query)?;
        let fluree = &*self.fluree;
        let controls = controls.unwrap_or_default();
        let id = &id;
        let answer = controls.run(py, |cancel, controls| async move {
            // This path takes no view, so the default context goes on the query.
            if let Some(obj) = query
                .as_object_mut()
                .filter(|o| !o.contains_key("@context"))
            {
                if let Some(context) = fluree.get_default_context(id).await? {
                    obj.insert("@context".to_string(), context);
                }
            }
            let graph = fluree.graph_at(id, spec);
            execute!(controls, cancel, graph.query().jsonld(&query))
        })?;
        answer.into_py(py, None)
    }

    /// A connection-level SPARQL query: its `FROM` / `FROM NAMED` / `TO`
    /// clauses pick the ledgers and times, so it can span ledgers or read a
    /// ledger's history.
    #[pyo3(signature = (sparql, policy = None, controls = None))]
    fn query_sparql_from<'py>(
        &self,
        py: Python<'py>,
        sparql: &str,
        policy: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let policy = governance(policy)?;
        let fluree = &*self.fluree;
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
        let query = to_json(query)?;
        let fluree = &*self.fluree;
        let controls = controls.unwrap_or_default();
        let query = &query;
        let answer = controls.run(py, |cancel, controls| async move {
            execute!(controls, cancel, fluree.query_from().jsonld(query))
        })?;
        answer.into_py(py, None)
    }

    /// Start a streaming SELECT; rows are read from the returned stream.
    #[pyo3(signature = (ledger, query, at = None, policy = None, controls = None))]
    fn stream(
        &self,
        py: Python<'_>,
        ledger: &str,
        query: &Bound<'_, PyAny>,
        at: Option<&Bound<'_, PyTuple>>,
        policy: Option<&Bound<'_, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<RowStream> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let query = QueryText::from_py(query)?;
        let fluree = &*self.fluree;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        start_stream(py, fluree, db, query, controls.unwrap_or_default())
    }

    /// The query plan for a SPARQL (text) or JSON-LD (object) query.
    #[pyo3(signature = (ledger, query, at = None, policy = None))]
    fn explain<'py>(
        &self,
        py: Python<'py>,
        ledger: &str,
        query: &Bound<'py, PyAny>,
        at: Option<&Bound<'py, PyTuple>>,
        policy: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let spec = time_spec(at)?;
        let policy = governance(policy)?;
        let query = QueryText::from_py(query)?;
        let fluree = &*self.fluree;
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
        let fluree = &*self.fluree;
        let db = block_on(py, load(fluree, &id, spec, policy.as_ref()))?.map_err(api_error)?;
        Ok(Snapshot {
            fluree: InRuntime::new(fluree.clone()),
            db: InRuntime::new(db),
        })
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
            block_on(py, self.fluree.commit_log(&id, limit))?.map_err(api_error)?;
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
        let fluree = &*self.fluree;
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
        let fluree = &*self.fluree;
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
        let fluree = &*self.fluree;
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
        let fluree = &*self.fluree;
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
        block_on(py, self.fluree.get_default_context(&id))?
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
        block_on(py, self.fluree.set_default_context(&id, &context))?
            .map(drop)
            .map_err(api_error)
    }

    fn info<'py>(&self, py: Python<'py>, ledger: &str) -> PyResult<Bound<'py, PyAny>> {
        let id = canonical(ledger)?;
        let info = block_on(py, self.fluree.ledger_info(&id).execute())?.map_err(api_error)?;
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
    fluree: InRuntime<Fluree>,
    db: InRuntime<GraphDb>,
}

impl Snapshot {
    pub(crate) fn new(fluree: &Fluree, db: GraphDb) -> Self {
        Self {
            fluree: InRuntime::new(fluree.clone()),
            db: InRuntime::new(db),
        }
    }
}

#[pymethods]
impl Snapshot {
    #[getter]
    fn ledger(&self) -> &str {
        &self.db.ledger_id
    }

    #[getter]
    fn t(&self) -> i64 {
        self.db.t
    }

    /// A Cypher read of this snapshot.
    #[pyo3(signature = (cypher, params = None, controls = None))]
    fn cypher_query<'py>(
        &self,
        py: Python<'py>,
        cypher: &str,
        params: Option<&Bound<'py, PyAny>>,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        cypher::require_read(cypher)?;
        let params = cypher::params(params)?;
        let controls = controls.unwrap_or_default();
        cypher::read(
            py,
            &self.fluree,
            &self.db,
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
                .explain_cypher(&self.db, cypher, params.as_ref()),
        )?
        .map_err(api_error)?;
        from_json(py, &plan)
    }

    #[pyo3(signature = (sparql, controls = None))]
    fn query_sparql<'py>(
        &self,
        py: Python<'py>,
        sparql: &str,
        controls: Option<Controls>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let controls = controls.unwrap_or_default();
        let (fluree, db) = (&*self.fluree, &*self.db);
        let answer = controls.run(py, |cancel, controls| async move {
            execute!(
                controls,
                cancel,
                GraphSnapshotQueryBuilder::new_from_parts(fluree, db)
                    .sparql(sparql)
                    .format(FormatterConfig::sparql_json())
            )
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
        let query = to_json(query)?;
        let controls = controls.unwrap_or_default();
        let (fluree, db, query) = (&*self.fluree, &*self.db, &query);
        let answer = controls.run(py, |cancel, controls| async move {
            execute!(
                controls,
                cancel,
                GraphSnapshotQueryBuilder::new_from_parts(fluree, db).jsonld(query)
            )
        })?;
        answer.into_py(py, None)
    }

    #[pyo3(signature = (query, controls = None))]
    fn stream(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyAny>,
        controls: Option<Controls>,
    ) -> PyResult<RowStream> {
        let query = QueryText::from_py(query)?;
        start_stream(
            py,
            &self.fluree,
            (*self.db).clone(),
            query,
            controls.unwrap_or_default(),
        )
    }

    fn explain<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = QueryText::from_py(query)?;
        let plan = block_on(py, query.explain(&self.fluree, &self.db))?.map_err(api_error)?;
        from_json(py, &plan)
    }
}

/// Plan a streaming SELECT against `db` and start its producer.
fn start_stream(
    py: Python<'_>,
    fluree: &Fluree,
    db: GraphDb,
    query: QueryText,
    controls: Controls,
) -> PyResult<RowStream> {
    let (input, columns) = match query {
        QueryText::Sparql(sparql) => {
            let columns = sparql_columns(&sparql);
            (OwnedStreamQuery::Sparql(sparql), columns)
        }
        QueryText::JsonLd(json) => {
            let columns = jsonld_columns(&json);
            (OwnedStreamQuery::JsonLd(json), columns)
        }
    };
    let cancellation = controls.cancellation();
    let options = QueryExecutionOptions::new().with_cancellation(cancellation.clone());
    // A single-ledger dataset keeps the view's policy with the producer.
    let dataset = DataSetDb::single(db);
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
        received,
        cancellation,
        columns,
        controls.timeout(),
    ))
}

/// A query to explain or stream: SPARQL text or a JSON-LD object.
enum QueryText {
    Sparql(String),
    JsonLd(JsonValue),
}

impl QueryText {
    fn from_py(query: &Bound<'_, PyAny>) -> PyResult<Self> {
        match query.extract::<String>() {
            Ok(sparql) => Ok(Self::Sparql(sparql)),
            Err(_) => Ok(Self::JsonLd(to_json(query)?)),
        }
    }

    async fn explain(&self, fluree: &Fluree, db: &GraphDb) -> fluree_db_api::Result<JsonValue> {
        match self {
            Self::Sparql(sparql) => fluree.explain_sparql(db, sparql).await,
            Self::JsonLd(query) => fluree.explain(db, query).await,
        }
    }
}
