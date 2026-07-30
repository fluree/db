//! Transaction builder for the [`Graph`] API.
//!
//! - [`GraphTransactBuilder`] — lazy transaction from a [`Graph`] handle
//! - [`StagedGraph`] — staged (uncommitted) transaction queryable via [`GraphSnapshotQueryBuilder`]

use serde_json::Value as JsonValue;

use crate::error::BuilderErrors;
use crate::graph::Graph;
use crate::graph_query_builder::GraphSnapshotQueryBuilder;
use crate::tx_builder::{parse_and_lower_sparql_update, Staged, TransactCore, TransactOperation};
use crate::view::GraphDb;
use crate::{
    ApiError, Fluree, PolicyContext, Result, TrackedErrorResponse, TrackedTransactionInput,
    Tracker, TrackingOptions, TransactResultRef,
};
use fluree_db_ledger::IndexConfig;
use fluree_db_transact::{CommitOpts, Txn, TxnOpts};

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

    /// Set the operation to a SPARQL UPDATE.
    ///
    /// The query is parsed and lowered during `commit()` under the ledger
    /// write lock — so its namespace allocation and staging share one
    /// registry. This avoids the namespace-conflict retry that pre-lowering
    /// against an unlocked snapshot would require.
    pub fn sparql_update(mut self, sparql: &'g str) -> Self {
        self.core.set_sparql_update(sparql);
        self
    }

    // -- Graph-management operations (non-SPARQL surface for the SPARQL 1.1
    //    Update graph-management verbs; share the `Txn` IR and staging path) --

    /// Retract every flake in the named graph `iri` — the transact-builder
    /// analog of `CLEAR GRAPH <iri>`. Indistinguishable from [`Self::drop_graph`]
    /// in Fluree's model (roadmap D-6).
    pub fn clear_graph(mut self, iri: impl Into<String>) -> Self {
        self.core.set_pre_built_txn(Txn::clear_graph(iri));
        self
    }

    /// Drop the named graph `iri` (retract all of its flakes). `DROP ≡ CLEAR`
    /// in Fluree's additive-only registry model (roadmap D-6).
    pub fn drop_graph(mut self, iri: impl Into<String>) -> Self {
        self.core.set_pre_built_txn(Txn::drop_graph(iri));
        self
    }

    /// Copy all flakes from named graph `from` into named graph `to`, replacing
    /// `to`'s prior contents — the transact-builder analog of `COPY <from> TO
    /// <to>`. A `from == to` copy is a no-op.
    pub fn copy_graph(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.core.set_pre_built_txn(Txn::copy_graph(from, to));
        self
    }

    /// Move all flakes from named graph `from` into named graph `to` (copy +
    /// retract the source) — the transact-builder analog of `MOVE <from> TO
    /// <to>`. A `from == to` move is a no-op.
    pub fn move_graph(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.core.set_pre_built_txn(Txn::move_graph(from, to));
        self
    }

    /// Merge all flakes from named graph `from` into named graph `to`, keeping
    /// `to`'s prior contents — the transact-builder analog of `ADD <from> TO
    /// <to>`. A `from == to` add is a no-op.
    pub fn add_graph(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.core.set_pre_built_txn(Txn::add_graph(from, to));
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
        self.graph
            .fluree
            .commit_with_handle(&handle, self.core)
            .await
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

        let index_config = self
            .core
            .index_config
            .clone()
            .unwrap_or_else(crate::server_defaults::default_index_config);

        // Load the current ledger state
        let ledger_state = self.graph.fluree.ledger(&self.graph.ledger_id).await?;

        // Stage
        // TODO: Add trig_meta support to tracked+policy path
        let stage_result = if let Some(sparql) = self.core.pending_sparql {
            let txns =
                parse_and_lower_sparql_update(sparql, &ledger_state.snapshot, self.core.txn_opts)?;
            let tracker = self
                .core
                .tracking
                .map(Tracker::new)
                .unwrap_or_else(Tracker::disabled);
            self.graph
                .fluree
                .stage_transaction_from_txns(
                    ledger_state,
                    txns,
                    Some(&index_config),
                    self.core.policy.as_ref(),
                    Some(&tracker),
                )
                .await?
        } else {
            let op = self.core.operation.unwrap();
            let txn_type = op.txn_type();
            // Parse transaction, extracting TriG metadata for Turtle inputs
            let parsed = op.to_json_with_trig_meta()?;
            let txn_json = parsed.json;
            let trig_meta = parsed.trig_meta;

            if let Some(policy) = &self.core.policy {
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
            }
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
