//! Query execution against DataSetDb
//!
//! Provides `query_dataset` for multi-ledger queries.

use crate::query::helpers::{
    build_query_result, parse_and_validate_sparql, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits,
    tracker_for_tracked_endpoint,
};
use crate::view::{DataSetDb, QueryInput};
use crate::{ApiError, ExecutableQuery, Fluree, QueryResult, Result, Tracker, TrackingOptions};
use fluree_db_query::execute::{
    execute_prepared, prepare_execution_with_binary_store, ContextConfig,
};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};

// ============================================================================
// Dataset Query Execution
// ============================================================================

impl Fluree {
    /// Execute a query against a dataset view (multi-ledger).
    ///
    /// For single-ledger datasets, this delegates to `query`.
    /// For multi-ledger datasets, this executes against the merged default graphs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view1 = fluree.db("ledger1:main").await?;
    /// let view2 = fluree.db("ledger2:main").await?;
    ///
    /// let dataset = DataSetDb::new()
    ///     .with_default(view1)
    ///     .with_default(view2);
    ///
    /// let result = fluree.query_dataset(&dataset, &query).await?;
    /// ```
    pub async fn query_dataset(
        &self,
        dataset: &DataSetDb,
        q: impl Into<QueryInput<'_>>,
    ) -> Result<QueryResult> {
        let input = q.into();

        // Single-ledger fast path (only safe for JSON-LD or SPARQL without dataset clauses).
        if dataset.is_single_ledger() {
            if let Some(view) = dataset.primary() {
                match &input {
                    QueryInput::JsonLd(_) => return self.query(view, input).await,
                    QueryInput::Sparql(sparql) => {
                        let ast = parse_and_validate_sparql(sparql)?;
                        let has_dataset = match &ast.body {
                            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Update(_) => false,
                        };
                        if !has_dataset {
                            return self.query(view, input).await;
                        }
                    }
                }
            }
        }

        // Require at least one default graph.
        //
        // IMPORTANT (multi-ledger semantics):
        // - We intentionally treat the *first* default graph as the "primary" view.
        // - The primary db is used for:
        //   - parsing / namespace resolution
        //   - reasoning defaults
        //   - query planning / optimization stats (HLL/NDV)
        //
        // Execution still scans *all* default graphs in the dataset (union semantics),
        // but optimization is driven by the primary graph under the assumption that
        // default graphs in a dataset represent similarly-shaped data.
        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no graphs for query execution"))?;

        // 1. Parse to common IR (using primary db for namespace resolution).
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(
                json,
                &primary.snapshot,
                primary.default_context.as_ref(),
                None,
            )?,
            QueryInput::Sparql(sparql) => {
                // For dataset view, SPARQL FROM/FROM NAMED are allowed
                // (they were validated when building the dataset)
                parse_sparql_to_ir(sparql, &primary.snapshot, primary.default_context.as_ref())?
            }
        };

        // 1b. Auto-wrap for graph source context
        super::query::maybe_wrap_for_graph_source(primary, &mut parsed);

        // 2. Build executable with optional reasoning override from primary view
        let executable = self.build_executable_for_dataset(dataset, &parsed).await?;

        // 3. Get tracker for fuel limits
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute against merged dataset
        let batches = self
            .execute_dataset_internal(dataset, &vars, &executable, &tracker)
            .await?;

        // 5. Build result with max_t across all views
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            dataset.result_t(),
            dataset.composite_overlay(),
            primary.binary_graph(),
        ))
    }

    pub(crate) async fn query_dataset_with_r2rml(
        &self,
        dataset: &DataSetDb,
        q: impl Into<QueryInput<'_>>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> Result<QueryResult> {
        let input = q.into();

        // Single-ledger fast path (only safe for JSON-LD or SPARQL without dataset clauses).
        if dataset.is_single_ledger() {
            if let Some(view) = dataset.primary() {
                match &input {
                    QueryInput::JsonLd(_) => {
                        return self
                            .query_view_with_r2rml(
                                view,
                                input,
                                r2rml_provider,
                                r2rml_table_provider,
                            )
                            .await;
                    }
                    QueryInput::Sparql(sparql) => {
                        let ast = parse_and_validate_sparql(sparql)?;
                        let has_dataset = match &ast.body {
                            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.is_some(),
                            fluree_db_sparql::ast::QueryBody::Update(_) => false,
                        };
                        if !has_dataset {
                            return self
                                .query_view_with_r2rml(
                                    view,
                                    input,
                                    r2rml_provider,
                                    r2rml_table_provider,
                                )
                                .await;
                        }
                    }
                }
            }
        }

        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no graphs for query execution"))?;

        // 1. Parse to common IR (using primary db for namespace resolution).
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(
                json,
                &primary.snapshot,
                primary.default_context.as_ref(),
                None,
            )?,
            QueryInput::Sparql(sparql) => {
                parse_sparql_to_ir(sparql, &primary.snapshot, primary.default_context.as_ref())?
            }
        };

        // 1b. Auto-wrap for graph source context
        super::query::maybe_wrap_for_graph_source(primary, &mut parsed);

        // 2. Build executable with optional reasoning override from primary view
        let executable = self.build_executable_for_dataset(dataset, &parsed).await?;

        // 3. Get tracker for fuel limits
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute against merged dataset
        let batches = self
            .execute_dataset_internal_with_r2rml(
                dataset,
                &vars,
                &executable,
                &tracker,
                r2rml_provider,
                r2rml_table_provider,
            )
            .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            dataset.result_t(),
            dataset.composite_overlay(),
            primary.binary_graph(),
        ))
    }

    /// Execute a dataset query with tracking.
    ///
    /// When `format_config` is `None`, defaults to JSON-LD for FlureeQL
    /// queries and SPARQL JSON for SPARQL queries.
    pub(crate) async fn query_dataset_tracked(
        &self,
        dataset: &DataSetDb,
        q: impl Into<QueryInput<'_>>,
        format_config: Option<crate::format::FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        // Get tracker: use caller-provided options if given, otherwise fall back
        // to defaults (all-enabled for SPARQL, opts-derived for JSON-LD).
        let tracker = if let Some(opts) = tracking_override {
            Tracker::new(opts)
        } else {
            match &input {
                QueryInput::JsonLd(json) => tracker_for_tracked_endpoint(json),
                QueryInput::Sparql(_) => Tracker::new(TrackingOptions::all_enabled()),
            }
        };

        // Determine output format: caller override > input-type default
        let default_format = match &input {
            QueryInput::Sparql(_) => crate::format::FormatterConfig::sparql_json(),
            _ => crate::format::FormatterConfig::jsonld(),
        };
        let mut format_config = format_config.unwrap_or(default_format);

        // Require primary
        let primary = dataset.primary().ok_or_else(|| {
            crate::query::TrackedErrorResponse::new(400, "Dataset has no graphs", tracker.tally())
        })?;

        // Parse
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(
                json,
                &primary.snapshot,
                primary.default_context.as_ref(),
                None,
            )
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?,
            QueryInput::Sparql(sparql) => {
                parse_sparql_to_ir(sparql, &primary.snapshot, primary.default_context.as_ref())
                    .map_err(|e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    })?
            }
        };

        // Auto-wrap for graph source context
        if let Some(primary) = dataset.primary() {
            super::query::maybe_wrap_for_graph_source(primary, &mut parsed);
        }

        // Build executable
        let executable = self
            .build_executable_for_dataset(dataset, &parsed)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?;

        // Execute with tracking
        let batches = self
            .execute_dataset_tracked(dataset, &vars, &executable, &tracker)
            .await
            .map_err(|e| {
                let status = status_for_query_error(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        // Build result
        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            dataset.result_t(),
            None,
            primary.binary_graph(),
        );

        // CONSTRUCT/DESCRIBE graph results must be formatted as JSON-LD.
        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        // Format with tracking
        let result_json = match primary.policy() {
            Some(policy) => query_result
                .format_async_with_policy_tracked(
                    primary.as_graph_db_ref(),
                    &format_config,
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .format_async_tracked(primary.as_graph_db_ref(), &format_config, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
        };

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    pub(crate) async fn query_dataset_tracked_with_r2rml(
        &self,
        dataset: &DataSetDb,
        q: impl Into<QueryInput<'_>>,
        format_config: Option<crate::format::FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let input = q.into();

        let tracker = if let Some(opts) = tracking_override {
            Tracker::new(opts)
        } else {
            match &input {
                QueryInput::JsonLd(json) => tracker_for_tracked_endpoint(json),
                QueryInput::Sparql(_) => Tracker::new(TrackingOptions::all_enabled()),
            }
        };

        let default_format = match &input {
            QueryInput::Sparql(_) => crate::format::FormatterConfig::sparql_json(),
            _ => crate::format::FormatterConfig::jsonld(),
        };
        let mut format_config = format_config.unwrap_or(default_format);

        let primary = dataset.primary().ok_or_else(|| {
            crate::query::TrackedErrorResponse::new(400, "Dataset has no graphs", tracker.tally())
        })?;

        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => parse_jsonld_query(
                json,
                &primary.snapshot,
                primary.default_context.as_ref(),
                None,
            )
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?,
            QueryInput::Sparql(sparql) => {
                parse_sparql_to_ir(sparql, &primary.snapshot, primary.default_context.as_ref())
                    .map_err(|e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    })?
            }
        };

        // Auto-wrap for graph source context
        if let Some(primary) = dataset.primary() {
            super::query::maybe_wrap_for_graph_source(primary, &mut parsed);
        }

        let executable = self
            .build_executable_for_dataset(dataset, &parsed)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?;

        let batches = self
            .execute_dataset_tracked_with_r2rml(
                dataset,
                &vars,
                &executable,
                &tracker,
                r2rml_provider,
                r2rml_table_provider,
            )
            .await
            .map_err(|e| {
                let status = status_for_query_error(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            dataset.result_t(),
            None,
            primary.binary_graph(),
        );

        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        let result_json = match primary.policy() {
            Some(policy) => query_result
                .format_async_with_policy_tracked(
                    primary.as_graph_db_ref(),
                    &format_config,
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .format_async_tracked(primary.as_graph_db_ref(), &format_config, &tracker)
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
        };

        Ok(crate::query::TrackedQueryResponse::success(
            result_json,
            tracker.tally(),
        ))
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Build an ExecutableQuery for dataset queries.
    ///
    /// Applies reasoning from the primary view if set. When reasoning config
    /// on the primary view declares `f:schemaSource`, resolves the schema
    /// bundle closure and attaches it to `executable.options.schema_bundle`.
    async fn build_executable_for_dataset(
        &self,
        dataset: &DataSetDb,
        parsed: &fluree_db_query::parse::ParsedQuery,
    ) -> Result<ExecutableQuery> {
        let mut executable = prepare_for_execution(parsed);

        // Apply reasoning from primary view if set
        if let Some(primary) = dataset.primary() {
            if primary.reasoning().is_some() {
                let query_has_reasoning = executable.options.reasoning.has_any_enabled();
                let query_disabled = executable.options.reasoning.is_disabled();

                if let Some(effective) =
                    primary.effective_reasoning(query_has_reasoning, query_disabled)
                {
                    executable.options.reasoning = effective.clone();
                }
            }

            // Resolve schema bundle against the primary view's ledger
            // (same-ledger only). Mirrors the single-view path in
            // `view/query.rs::attach_schema_bundle`; see that method for the
            // reasoning-disabled short-circuit rationale.
            Self::attach_dataset_schema_bundle(primary, &mut executable).await?;
        }

        Ok(executable)
    }

    async fn attach_dataset_schema_bundle(
        primary: &crate::view::GraphDb,
        executable: &mut ExecutableQuery,
    ) -> Result<()> {
        if executable.options.reasoning.is_disabled() {
            return Ok(());
        }
        let Some(resolved) = primary.resolved_config() else {
            return Ok(());
        };
        let Some(reasoning) = resolved.reasoning.as_ref() else {
            return Ok(());
        };
        if reasoning.schema_source.is_none() {
            return Ok(());
        }
        let db_ref = primary.as_graph_db_ref();
        let Some(bundle) = crate::ontology_imports::resolve_schema_bundle(
            db_ref.snapshot,
            db_ref.overlay,
            db_ref.t,
            reasoning,
        )
        .await?
        else {
            return Ok(());
        };
        let flakes = crate::ontology_imports::get_or_build_schema_bundle_flakes(
            db_ref.snapshot,
            db_ref.overlay,
            &bundle,
        )
        .await?;
        executable.options.schema_bundle = Some(flakes);
        Ok(())
    }

    /// Execute against dataset (multi-ledger).
    ///
    /// Calls `prepare_execution` + `execute_prepared` directly so that
    /// `binary_store` from the primary view is threaded into the
    /// `ExecutionContext` for `BinaryScanOperator`.
    async fn execute_dataset_internal(
        &self,
        dataset: &DataSetDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_dataset_internal_with_r2rml(dataset, vars, executable, tracker, &noop, &noop)
            .await
    }

    /// Execute against dataset with explicit R2RML provider.
    ///
    /// Used by callers that need R2RML/Iceberg graph source support
    /// (e.g., server query handlers with iceberg support).
    pub(crate) async fn execute_dataset_internal_with_r2rml<'b>(
        &self,
        dataset: &DataSetDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml_provider: &'b dyn fluree_db_query::r2rml::R2rmlProvider,
        r2rml_table_provider: &'b dyn fluree_db_query::r2rml::R2rmlTableProvider,
    ) -> Result<Vec<crate::Batch>> {
        let primary = dataset
            .primary()
            .ok_or_else(|| ApiError::query("Dataset has no default graphs"))?;

        let runtime_dataset = dataset.as_runtime_dataset();

        let db = primary.as_graph_db_ref();

        let prepared =
            prepare_execution_with_binary_store(db, executable, primary.binary_store.as_ref())
                .await
                .map_err(query_error_to_api_error)?;

        let (from_t, to_t, history_mode) = match dataset.history_time_range() {
            Some((hist_from, hist_to)) => (Some(hist_from), hist_to, true),
            None => (None, primary.t, false),
        };

        // Binary scans rely on a ledger-specific binary index store. For datasets that span
        // multiple ledgers, using only the primary view's store will silently drop results.
        //
        // In multi-ledger mode we disable binary scans (and associated provider maps) so
        // execution falls back to per-snapshot range scans which are correctly ledger-scoped.
        let primary_ledger_id: &str = primary.ledger_id.as_ref();
        let is_single_ledger_dataset = dataset
            .default
            .iter()
            .all(|v| v.ledger_id.as_ref() == primary_ledger_id)
            && dataset
                .named
                .values()
                .all(|v| v.ledger_id.as_ref() == primary_ledger_id);

        // Perf guardrail: skip fulltext arena map + `"en"` lang_id resolution
        // for queries that don't actually call `fulltext(...)`. Spatial
        // providers keep their current eager-build semantics.
        let uses_fulltext = executable.uses_fulltext();
        let (binary_store, dict_novelty, spatial_map, fulltext_map, english_lang_id) =
            if is_single_ledger_dataset {
                let spatial_map = primary
                    .binary_store
                    .as_ref()
                    .map(|s| s.spatial_provider_map());
                let fulltext_map = if uses_fulltext {
                    primary
                        .binary_store
                        .as_ref()
                        .map(|s| s.fulltext_provider_map())
                } else {
                    None
                };
                let english_lang_id = if uses_fulltext {
                    primary
                        .binary_store
                        .as_ref()
                        .and_then(|s| s.resolve_lang_id("en"))
                } else {
                    None
                };
                (
                    primary.binary_store.clone(),
                    primary.dict_novelty.clone(),
                    spatial_map,
                    fulltext_map,
                    english_lang_id,
                )
            } else {
                (None, None, None, None, None)
            };

        let config = ContextConfig {
            tracker: if tracker.is_enabled() {
                Some(tracker)
            } else {
                None
            },
            dataset: Some(&runtime_dataset),
            policy_enforcer: primary.policy_enforcer().cloned(),
            r2rml: Some((r2rml_provider, r2rml_table_provider)),
            binary_g_id: primary.graph_id,
            binary_store,
            dict_novelty,
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            english_lang_id,
            remote_service: self.remote_service_executor(),
            history_mode,
            from_t,
            strict_bind_errors: true,
            ..Default::default()
        };

        let exec_db = db.with_t(to_t);
        execute_prepared(exec_db, vars, prepared, config)
            .await
            .map_err(query_error_to_api_error)
    }

    /// Execute against dataset with tracking.
    ///
    /// Threads `binary_store` from the primary view into the execution context.
    async fn execute_dataset_tracked(
        &self,
        dataset: &DataSetDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_dataset_tracked_with_r2rml(dataset, vars, executable, tracker, &noop, &noop)
            .await
    }

    async fn execute_dataset_tracked_with_r2rml(
        &self,
        dataset: &DataSetDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let primary = dataset.primary().ok_or_else(|| {
            fluree_db_query::QueryError::InvalidQuery("Dataset has no default graphs".into())
        })?;

        let runtime_dataset = dataset.as_runtime_dataset();

        let db = primary.as_graph_db_ref();

        let prepared =
            prepare_execution_with_binary_store(db, executable, primary.binary_store.as_ref())
                .await?;

        let (from_t, to_t, history_mode) = match dataset.history_time_range() {
            Some((hist_from, hist_to)) => (Some(hist_from), hist_to, true),
            None => (None, primary.t, false),
        };

        let primary_ledger_id: &str = primary.ledger_id.as_ref();
        let is_single_ledger_dataset = dataset
            .default
            .iter()
            .all(|v| v.ledger_id.as_ref() == primary_ledger_id)
            && dataset
                .named
                .values()
                .all(|v| v.ledger_id.as_ref() == primary_ledger_id);

        // Perf guardrail: skip fulltext arena map + `"en"` lang_id resolution
        // for queries that don't actually call `fulltext(...)`. Spatial
        // providers keep their current eager-build semantics.
        let uses_fulltext = executable.uses_fulltext();
        let (binary_store, dict_novelty, spatial_map, fulltext_map, english_lang_id) =
            if is_single_ledger_dataset {
                let spatial_map = primary
                    .binary_store
                    .as_ref()
                    .map(|s| s.spatial_provider_map());
                let fulltext_map = if uses_fulltext {
                    primary
                        .binary_store
                        .as_ref()
                        .map(|s| s.fulltext_provider_map())
                } else {
                    None
                };
                let english_lang_id = if uses_fulltext {
                    primary
                        .binary_store
                        .as_ref()
                        .and_then(|s| s.resolve_lang_id("en"))
                } else {
                    None
                };
                (
                    primary.binary_store.clone(),
                    primary.dict_novelty.clone(),
                    spatial_map,
                    fulltext_map,
                    english_lang_id,
                )
            } else {
                (None, None, None, None, None)
            };

        let config = ContextConfig {
            tracker: Some(tracker),
            dataset: Some(&runtime_dataset),
            policy_enforcer: primary.policy_enforcer().cloned(),
            r2rml: Some((r2rml_provider, r2rml_table_provider)),
            binary_g_id: primary.graph_id,
            binary_store,
            dict_novelty,
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            english_lang_id,
            remote_service: self.remote_service_executor(),
            history_mode,
            from_t,
            strict_bind_errors: true,
            ..Default::default()
        };

        let exec_db = db.with_t(to_t);
        execute_prepared(exec_db, vars, prepared, config).await
    }
}

fn query_error_to_api_error(err: fluree_db_query::QueryError) -> ApiError {
    ApiError::query(err.to_string())
}

#[cfg(test)]
mod tests {

    use crate::view::DataSetDb;
    use crate::FlureeBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_query_dataset_single_ledger() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query via dataset view (single ledger)
        let view = fluree.db("testdb:main").await.unwrap();
        let dataset = DataSetDb::single(view);

        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = fluree.query_dataset(&dataset, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_dataset_formatted() {
        let fluree = FlureeBuilder::memory().build_memory();

        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/bob",
                "http://example.org/name": "Bob"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let view = fluree.db("testdb:main").await.unwrap();
        let dataset = DataSetDb::single(view);

        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/bob", "http://example.org/name": "?name"}
        });

        let result = dataset
            .query(&fluree)
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();
        assert!(result.is_array() || result.is_object());
    }

    #[tokio::test]
    async fn test_query_dataset_multi_ledger_union() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Two independent ledgers with distinct subjects
        let ledger1 = fluree.create_ledger("db1").await.unwrap();
        let ledger2 = fluree.create_ledger("db2").await.unwrap();

        let txn1 = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger1 = fluree.update(ledger1, &txn1).await.unwrap().ledger;

        let txn2 = json!({
            "insert": [{
                "@id": "http://example.org/bob",
                "http://example.org/name": "Bob"
            }]
        });
        let _ledger2 = fluree.update(ledger2, &txn2).await.unwrap().ledger;

        let view1 = fluree.db("db1:main").await.unwrap();
        let view2 = fluree.db("db2:main").await.unwrap();

        let dataset = DataSetDb::new().with_default(view1).with_default(view2);

        let query = json!({
            "select": ["?s", "?name"],
            "where": {
                "@id": "?s",
                "http://example.org/name": "?name"
            }
        });

        let result = fluree.query_dataset(&dataset, &query).await.unwrap();
        let total_solutions: usize = result.batches.iter().map(fluree_db_query::Batch::len).sum();
        assert_eq!(total_solutions, 2);
    }

    #[tokio::test]
    async fn test_query_empty_dataset_error() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let dataset: DataSetDb = DataSetDb::new();
        let query = json!({ "select": ["?s"], "where": {"@id": "?s"} });

        let result = fluree.query_dataset(&dataset, &query).await;
        assert!(result.is_err());
    }
}
