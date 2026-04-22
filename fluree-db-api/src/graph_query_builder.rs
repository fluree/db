//! Query builders for the [`Graph`] and [`Db`] API.
//!
//! - [`GraphQueryBuilder`] — lazy query from a [`Graph`] handle (defers view load)
//! - [`GraphSnapshotQueryBuilder`] — query from a materialized snapshot or [`StagedGraph`]

use serde_json::Value as JsonValue;

use crate::error::BuilderErrors;
use crate::format::FormatterConfig;
use crate::graph::Graph;
use crate::query::builder::QueryCore;
use crate::view::GraphDb;
use crate::{
    ApiError, Fluree, QueryResult, Result, TrackedErrorResponse, TrackedQueryResponse,
    TrackingOptions,
};

#[cfg(feature = "iceberg")]
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
#[cfg(feature = "iceberg")]
use std::sync::Arc;

// ============================================================================
// GraphQueryBuilder (lazy — defers view load to terminal)
// ============================================================================

/// Query builder from a lazy [`Graph`] handle.
///
/// No I/O occurs until a terminal method (`.execute()`, `.execute_formatted()`,
/// `.execute_tracked()`) is called.
///
/// # Example
///
/// ```ignore
/// let result = fluree
///     .graph("mydb:main")
///     .query()
///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
///     .execute()
///     .await?;
/// ```
pub struct GraphQueryBuilder<'a, 'g> {
    graph: &'g Graph<'a>,
    core: QueryCore<'g>,
    #[cfg(feature = "iceberg")]
    /// When true, use graph source fallback for resolution (set by `with_r2rml()`).
    graph_source_fallback: bool,
}

impl<'a, 'g> GraphQueryBuilder<'a, 'g> {
    /// Create a new builder (called by `Graph::query()`).
    pub(crate) fn new(graph: &'g Graph<'a>) -> Self {
        Self {
            graph,
            core: QueryCore::new(),
            #[cfg(feature = "iceberg")]
            graph_source_fallback: false,
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD.
    pub fn jsonld(mut self, json: &'g JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'g str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics (fuel, time, policy).
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration (used by `.execute_formatted()`).
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers for graph source queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    ///
    /// Attaches actual R2RML provider objects so that GRAPH patterns
    /// targeting graph sources resolve via the R2RML/Iceberg engine.
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self {
        let shared = Arc::new(crate::graph_source::FlureeR2rmlProvider::new(
            self.graph.fluree,
        ));
        let provider: Arc<dyn R2rmlProvider + 'g> = shared.clone();
        let table_provider: Arc<dyn R2rmlTableProvider + 'g> = shared;
        self.core.r2rml = Some((provider, table_provider));
        self.core.set_r2rml();
        self.graph_source_fallback = true;
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.core.validate();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Load the view, using graph source fallback when enabled.
    async fn load_view(&self) -> Result<crate::view::GraphDb> {
        let result = self
            .graph
            .fluree
            .load_graph_db_at(&self.graph.ledger_id, self.graph.time_spec.clone())
            .await;

        // If graph source fallback is enabled and the ledger wasn't found,
        // try resolving as a graph source with a genesis snapshot.
        // The R2RML provider (stored in core.r2rml by with_r2rml()) checks
        // has_r2rml_mapping to detect graph sources without requiring
        // additional trait bounds beyond NameService.
        #[cfg(feature = "iceberg")]
        if self.graph_source_fallback
            && result
                .as_ref()
                .is_err_and(super::error::ApiError::is_not_found)
        {
            let ledger_id = &self.graph.ledger_id;
            let gs_id = fluree_db_core::normalize_ledger_id(ledger_id)
                .unwrap_or_else(|_| ledger_id.to_string());

            if let Some((r2rml, _)) = &self.core.r2rml {
                if r2rml.has_r2rml_mapping(&gs_id).await {
                    let snapshot = fluree_db_core::LedgerSnapshot::genesis(&gs_id);
                    let state = fluree_db_ledger::LedgerState::new(
                        snapshot,
                        fluree_db_novelty::Novelty::new(0),
                    );
                    let mut db = crate::view::GraphDb::from_ledger_state(&state);
                    db.graph_source_id = Some(gs_id.into());
                    return Ok(db);
                }
            }
        }

        result
    }

    /// Execute the query and return raw [`QueryResult`].
    ///
    /// Loads the graph snapshot internally, then runs the query.
    /// When R2RML is enabled (via `.with_r2rml()`), falls back to graph source
    /// resolution if the ledger is not found, and routes through the R2RML-aware
    /// execution path so GRAPH patterns targeting graph sources resolve.
    pub async fn execute(mut self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let view = self.load_view().await?;
        let r2rml = self.core.r2rml.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_view_with_r2rml(&view, input, provider.as_ref(), table_provider.as_ref())
                    .await
            }
            None => self.graph.fluree.query(&view, input).await,
        }
    }

    /// Execute and return formatted JSON output.
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let view = self.load_view().await?;
        let r2rml = self.core.r2rml.take();
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.take().unwrap();
        let result = match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_view_with_r2rml(&view, input, provider.as_ref(), table_provider.as_ref())
                    .await?
            }
            None => self.graph.fluree.query(&view, input).await?,
        };
        match view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(view.as_graph_db_ref(), &format_config, policy)
                .await?),
            None => Ok(result
                .format_async(view.as_graph_db_ref(), &format_config)
                .await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        mut self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let db = self
            .load_view()
            .await
            .map_err(|e| TrackedErrorResponse::new(404, e.to_string(), None))?;
        let r2rml = self.core.r2rml.take();
        let format_config = self.core.format.take();
        let tracking = self.core.tracking.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.graph
                    .fluree
                    .query_tracked_with_r2rml(
                        &db,
                        input,
                        format_config,
                        tracking,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => {
                self.graph
                    .fluree
                    .query_tracked(&db, input, format_config, tracking)
                    .await
            }
        }
    }
}

// ============================================================================
// GraphSnapshotQueryBuilder (materialized — view already loaded)
// ============================================================================

/// Query builder from a materialized snapshot (used by both [`GraphSnapshot`] and [`StagedGraph`]).
///
/// The view is already loaded, so no additional I/O occurs for the load step.
///
/// # Example
///
/// ```ignore
/// let snapshot = fluree.graph("mydb:main").load().await?;
/// let result = snapshot.query().jsonld(&q).execute().await?;
/// ```
pub struct GraphSnapshotQueryBuilder<'a, 'v> {
    fluree: &'a Fluree,
    view: &'v GraphDb,
    core: QueryCore<'v>,
}

impl<'a: 'v, 'v> GraphSnapshotQueryBuilder<'a, 'v> {
    /// Create a new builder from a fluree reference and a view.
    pub fn new_from_parts(fluree: &'a Fluree, view: &'v GraphDb) -> Self {
        Self {
            fluree,
            view,
            core: QueryCore::new(),
        }
    }

    // --- Shared setters ---

    /// Set the query input as JSON-LD.
    pub fn jsonld(mut self, json: &'v JsonValue) -> Self {
        self.core.set_jsonld(json);
        self
    }

    /// Set the query input as SPARQL.
    pub fn sparql(mut self, sparql: &'v str) -> Self {
        self.core.set_sparql(sparql);
        self
    }

    /// Enable tracking of all metrics (fuel, time, policy).
    pub fn track_all(mut self) -> Self {
        self.core.set_track_all();
        self
    }

    /// Set custom tracking options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.set_tracking(opts);
        self
    }

    /// Set format configuration (used by `.execute_formatted()`).
    pub fn format(mut self, config: FormatterConfig) -> Self {
        self.core.set_format(config);
        self
    }

    /// Enable BM25/Vector index providers for graph source queries.
    pub fn with_index_providers(mut self) -> Self {
        self.core.set_index_providers();
        self
    }

    /// Enable R2RML/Iceberg support (feature-gated).
    #[cfg(feature = "iceberg")]
    pub fn with_r2rml(mut self) -> Self {
        let shared = Arc::new(crate::graph_source::FlureeR2rmlProvider::new(self.fluree));
        let provider: Arc<dyn R2rmlProvider + 'v> = shared.clone();
        let table_provider: Arc<dyn R2rmlTableProvider + 'v> = shared;
        self.core.r2rml = Some((provider, table_provider));
        self.core.set_r2rml();
        self
    }

    // --- Terminal operations ---

    /// Validate builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let errs = self.core.validate();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errs))
        }
    }

    /// Execute the query and return raw [`QueryResult`].
    pub async fn execute(mut self) -> Result<QueryResult> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let r2rml = self.core.r2rml.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_view_with_r2rml(
                        self.view,
                        input,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => self.fluree.query(self.view, input).await,
        }
    }

    /// Execute and return formatted JSON output.
    pub async fn execute_formatted(mut self) -> Result<JsonValue> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            return Err(ApiError::Builder(BuilderErrors(errs)));
        }

        let r2rml = self.core.r2rml.take();
        let format_config = self
            .core
            .format
            .take()
            .unwrap_or_else(|| self.core.default_format());
        let input = self.core.input.take().unwrap();
        let result = match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_view_with_r2rml(
                        self.view,
                        input,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await?
            }
            None => self.fluree.query(self.view, input).await?,
        };
        match self.view.policy() {
            Some(policy) => Ok(result
                .format_async_with_policy(self.view.as_graph_db_ref(), &format_config, policy)
                .await?),
            None => Ok(result
                .format_async(self.view.as_graph_db_ref(), &format_config)
                .await?),
        }
    }

    /// Execute with tracking (fuel, time, policy stats).
    pub async fn execute_tracked(
        mut self,
    ) -> std::result::Result<TrackedQueryResponse, TrackedErrorResponse> {
        let errs = self.core.validate();
        if !errs.is_empty() {
            let msg = BuilderErrors(errs).to_string();
            return Err(TrackedErrorResponse::new(400, msg, None));
        }

        let r2rml = self.core.r2rml.take();
        let format_config = self.core.format.take();
        let tracking = self.core.tracking.take();
        let input = self.core.input.take().unwrap();
        match r2rml.as_ref() {
            Some((provider, table_provider)) => {
                self.fluree
                    .query_tracked_with_r2rml(
                        self.view,
                        input,
                        format_config,
                        tracking,
                        provider.as_ref(),
                        table_provider.as_ref(),
                    )
                    .await
            }
            None => {
                self.fluree
                    .query_tracked(self.view, input, format_config, tracking)
                    .await
            }
        }
    }
}
