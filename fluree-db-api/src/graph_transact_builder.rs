//! Transaction builder for the [`Graph`] API.
//!
//! - [`GraphTransactBuilder`] — lazy transaction from a [`Graph`] handle
//! - [`StagedGraph`] — staged (uncommitted) transaction queryable via [`GraphSnapshotQueryBuilder`]

use serde_json::Value as JsonValue;

use crate::error::BuilderErrors;
use crate::graph::Graph;
use crate::graph_query_builder::GraphSnapshotQueryBuilder;
use crate::tx_builder::{commit_with_handle, Staged, TransactCore, TransactOperation};
use crate::view::GraphDb;
use crate::{
    ApiError, Fluree, PolicyContext, Result, TrackedErrorResponse, TrackedTransactionInput,
    Tracker, TrackingOptions, TransactResultRef,
};
use fluree_db_ledger::IndexConfig;
use fluree_db_transact::{CommitOpts, TxnOpts};

// ============================================================================
// GraphTransactBuilder
// ============================================================================

/// Transaction builder from a lazy [`Graph`] handle.
///
/// No I/O occurs until a terminal method (`.commit()`, `.stage()`) is called.
///
/// # Examples
///
/// ```ignore
/// // Commit directly
/// let out = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .commit()
///     .await?;
///
/// // Stage without committing
/// let staged = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .stage()
///     .await?;
/// ```
pub struct GraphTransactBuilder<'a, 'g> {
    graph: &'g Graph<'a>,
    core: TransactCore<'g>,
}

impl<'a, 'g> GraphTransactBuilder<'a, 'g> {
    /// Create a new builder (called by `Graph::transact()`).
    pub(crate) fn new(graph: &'g Graph<'a>) -> Self {
        Self {
            graph,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'g str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'g str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
        self
    }

    // -- Option setters --

    /// Set transaction options (author, context, etc.).
    pub fn txn_opts(mut self, opts: TxnOpts) -> Self {
        self.core.txn_opts = opts;
        self
    }

    /// Set commit options (message, author, etc.).
    pub fn commit_opts(mut self, opts: CommitOpts) -> Self {
        self.core.commit_opts = opts;
        self
    }

    /// Override the index configuration.
    pub fn index_config(mut self, config: IndexConfig) -> Self {
        self.core.index_config = Some(config);
        self
    }

    /// Enable tracking with custom options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.tracking = Some(opts);
        self
    }

    /// Set policy enforcement for the transaction.
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.core.policy = Some(ctx);
        self
    }

    // -- Terminal operations --

    /// Validate the builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        self.core.validate()
    }

    /// Stage + commit the transaction against the latest ledger head.
    ///
    /// Loads the cached ledger handle, acquires a write lock, stages,
    /// commits (with head-check), and updates the cache.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let out = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .commit()
    ///     .await?;
    /// ```
    pub async fn commit(self) -> Result<TransactResultRef> {
        let handle = self
            .graph
            .fluree
            .ledger_cached(&self.graph.ledger_id)
            .await?;
        commit_with_handle(self.graph.fluree, &handle, self.core).await
    }

    /// Stage the transaction without committing.
    ///
    /// Returns a [`StagedGraph`] that can be queried to preview changes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let staged = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .stage()
    ///     .await?;
    ///
    /// let preview = staged.query().jsonld(&q).execute().await?;
    /// ```
    pub async fn stage(self) -> Result<StagedGraph<'a>> {
        self.core.validate().map_err(ApiError::Builder)?;

        let op = self.core.operation.unwrap();
        let txn_type = op.txn_type();
        // Parse transaction, extracting TriG metadata for Turtle inputs
        let parsed = op.to_json_with_trig_meta()?;
        let txn_json = parsed.json;
        let trig_meta = parsed.trig_meta;
        let index_config = self.core.index_config.unwrap_or_default();

        // Load the current ledger state
        let ledger_state = self.graph.fluree.ledger(&self.graph.ledger_id).await?;

        // Stage
        // TODO: Add trig_meta support to tracked+policy path
        let stage_result = if let Some(policy) = &self.core.policy {
            let tracker = Tracker::new(self.core.tracking.unwrap_or(TrackingOptions {
                track_time: true,
                track_fuel: true,
                track_policy: true,
                max_fuel: None,
            }));
            let input =
                TrackedTransactionInput::new(txn_type, &txn_json, self.core.txn_opts, policy);
            self.graph
                .fluree
                .stage_transaction_tracked_with_policy(
                    ledger_state,
                    input,
                    Some(&index_config),
                    &tracker,
                )
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?
        } else {
            self.graph
                .fluree
                .stage_transaction_with_trig_meta(
                    ledger_state,
                    txn_type,
                    &txn_json,
                    self.core.txn_opts,
                    Some(&index_config),
                    trig_meta.as_ref(),
                )
                .await?
        };

        // Pre-build the GraphDb from staged so query() can borrow it
        let staged = Staged {
            view: stage_result.view,
            ns_registry: stage_result.ns_registry,
            graph_delta: stage_result.graph_delta,
        };
        let staged_view = GraphDb::from_staged(&staged)?;

        Ok(StagedGraph {
            fluree: self.graph.fluree,
            staged,
            staged_view,
        })
    }
}

// ============================================================================
// StagedGraph
// ============================================================================

/// A staged (uncommitted) transaction bound to an executor.
///
/// Queries against this type see the staged changes.
/// Stage-on-stage and commit-from-staged are TBD.
///
/// # Example
///
/// ```ignore
/// let staged = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .stage()
///     .await?;
///
/// let preview = staged.query().jsonld(&q).execute().await?;
/// ```
pub struct StagedGraph<'a> {
    fluree: &'a Fluree,
    staged: Staged,
    staged_view: GraphDb,
}

impl<'a> StagedGraph<'a> {
    /// Create a query builder that sees the staged changes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let preview = staged.query().jsonld(&q).execute().await?;
    /// ```
    pub fn query(&self) -> GraphSnapshotQueryBuilder<'a, '_> {
        GraphSnapshotQueryBuilder::new_from_parts(self.fluree, &self.staged_view)
    }

    /// Access the underlying [`Staged`] transaction.
    pub fn staged(&self) -> &Staged {
        &self.staged
    }

    /// Check if the transaction produced any staged changes.
    pub fn has_staged(&self) -> bool {
        self.staged.view.has_staged()
    }
}
