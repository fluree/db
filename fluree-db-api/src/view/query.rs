//! Query execution against GraphDb
//!
//! Provides `query` and related methods that execute queries against
//! a GraphDb, respecting policy and reasoning wrappers.

use crate::query::helpers::{
    build_query_result, parse_and_validate_sparql, parse_jsonld_query, parse_sparql_to_ir,
    prepare_for_execution, status_for_query_error, tracker_for_limits,
    tracker_for_tracked_endpoint,
};
use crate::view::{GraphDb, QueryInput};
use crate::{ApiError, ExecutableQuery, Fluree, QueryResult, Result, Tracker, TrackingOptions};
use fluree_db_query::execute::{
    execute_prepared, prepare_execution_with_config, ContextConfig, PrepareConfig,
};
use fluree_db_query::ir::{GraphName, Pattern};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
use serde_json::Value as JsonValue;

/// If the view was created from a graph source, wrap all top-level patterns
/// in `GRAPH <gs_id> { ... }` so the R2RML provider handles them.
///
/// Skips wrapping if the query already contains a top-level GRAPH pattern
/// (the user explicitly scoped it).
pub(crate) fn maybe_wrap_for_graph_source(db: &GraphDb, parsed: &mut fluree_db_query::ir::Query) {
    if let Some(ref gs_id) = db.graph_source_id {
        let has_graph_pattern = parsed
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Graph { .. }));
        if !has_graph_pattern {
            let inner = std::mem::take(&mut parsed.patterns);
            parsed.patterns = vec![Pattern::Graph {
                name: GraphName::Iri(gs_id.to_string().into()),
                patterns: inner,
            }];
        }
    }
}

// ============================================================================
// Query Execution
// ============================================================================

impl Fluree {
    /// Execute a query against a GraphDb.
    ///
    /// Accepts JSON-LD or SPARQL via `QueryInput`. Wrapper settings
    /// (policy, reasoning) are applied automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use serde_json::json;
    ///
    /// let db = fluree.db("mydb:main").await?
    ///     .with_reasoning(ReasoningModes::owl2ql());
    ///
    /// // JSON-LD query
    /// let query = json!({"select": ["?s"], "where": [["?s", "?p", "?o"]]});
    /// let result = fluree.query(&db, &query).await?;
    ///
    /// // SPARQL query
    /// let result = fluree.query(&db, "SELECT * WHERE { ?s ?p ?o }").await?;
    /// ```
    ///
    /// # SPARQL Dataset Clause Restriction
    ///
    /// A `GraphDb` represents a single ledger. SPARQL queries with
    /// `FROM` or `FROM NAMED` clauses will be rejected. Use
    /// `query_connection_sparql` for multi-ledger queries.
    pub async fn query(&self, db: &GraphDb, q: impl Into<QueryInput<'_>>) -> Result<QueryResult> {
        let input = q.into();

        // 1. Parse to common IR
        let parse_start = std::time::Instant::now();
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None)?
            }
            QueryInput::Sparql(sparql) => {
                // Validate no dataset clauses
                self.validate_sparql_for_view(sparql)?;
                parse_sparql_to_ir(sparql, &db.snapshot, db.default_context.as_ref())?
            }
        };
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        // 1b. Auto-wrap for graph source context
        maybe_wrap_for_graph_source(db, &mut parsed);

        // 2. Build executable with optional reasoning override
        let plan_start = std::time::Instant::now();
        let executable = self.build_executable_for_view(db, &parsed).await?;
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        // 3. Get tracker for fuel limits only (no tracking overhead for non-tracked calls)
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute
        let exec_start = std::time::Instant::now();
        let batches = self
            .execute_view_internal(db, &vars, &executable, &tracker)
            .await?;
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        tracing::info!(
            parse_ms = format!("{:.2}", parse_ms),
            plan_ms = format!("{:.2}", plan_ms),
            exec_ms = format!("{:.2}", exec_ms),
            "query phases"
        );

        // 5. Build result
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        ))
    }

    /// Execute a query against a GraphDb with explicit R2RML providers.
    ///
    /// This is used by connection query paths (and builders) that need to resolve
    /// graph sources via R2RML/Iceberg while still running against a ledger-backed
    /// planning database.
    pub(crate) async fn query_view_with_r2rml(
        &self,
        db: &GraphDb,
        q: impl Into<QueryInput<'_>>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> Result<QueryResult> {
        let input = q.into();

        // 1. Parse to common IR
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None)?
            }
            QueryInput::Sparql(sparql) => {
                // Validate no dataset clauses
                self.validate_sparql_for_view(sparql)?;
                parse_sparql_to_ir(sparql, &db.snapshot, db.default_context.as_ref())?
            }
        };

        // 1b. Auto-wrap for graph source context
        maybe_wrap_for_graph_source(db, &mut parsed);

        // 2. Build executable with optional reasoning override
        let executable = self.build_executable_for_view(db, &parsed).await?;

        // 3. Tracker (fuel limits only)
        let tracker = match &input {
            QueryInput::JsonLd(json) => tracker_for_limits(json),
            QueryInput::Sparql(_) => Tracker::disabled(),
        };

        // 4. Execute
        let batches = self
            .execute_view_internal_with_r2rml(
                db,
                &vars,
                &executable,
                &tracker,
                r2rml_provider,
                r2rml_table_provider,
            )
            .await?;

        // 5. Build result
        Ok(build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        ))
    }

    /// Explain a JSON-LD query plan against a GraphDb.
    ///
    /// This uses the same default-context behavior as query execution.
    pub async fn explain(&self, db: &GraphDb, query_json: &JsonValue) -> Result<JsonValue> {
        crate::explain::explain_jsonld_with_default_context(
            &db.snapshot,
            query_json,
            db.default_context.as_ref(),
        )
        .await
    }

    /// Explain a SPARQL query plan against a GraphDb.
    pub async fn explain_sparql(&self, db: &GraphDb, sparql: &str) -> Result<JsonValue> {
        crate::explain::explain_sparql_with_default_context(
            &db.snapshot,
            sparql,
            db.default_context.as_ref(),
        )
        .await
    }

    /// Execute a query with tracking.
    ///
    /// Returns a tracked response with fuel, time, and policy statistics.
    /// When `format_config` is `None`, defaults to JSON-LD for FlureeQL
    /// queries and SPARQL JSON for SPARQL queries.
    pub(crate) async fn query_tracked(
        &self,
        db: &GraphDb,
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

        // Parse
        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
            QueryInput::Sparql(sparql) => {
                self.validate_sparql_for_view(sparql).map_err(|e| {
                    crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                })?;
                parse_sparql_to_ir(sparql, &db.snapshot, db.default_context.as_ref()).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
        };

        // Auto-wrap for graph source context
        maybe_wrap_for_graph_source(db, &mut parsed);

        // Build executable with reasoning
        let executable = self
            .build_executable_for_view(db, &parsed)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?;

        // Execute with tracking
        let batches = self
            .execute_view_tracked(db, &vars, &executable, &tracker)
            .await
            .map_err(|e| {
                let status = query_error_to_status(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        // Build result
        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        );

        // CONSTRUCT/DESCRIBE graph results must be formatted as JSON-LD.
        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        // Format with tracking
        let result_json = match db.policy() {
            Some(policy) => query_result
                .format_async_with_policy_tracked(
                    db.as_graph_db_ref(),
                    &format_config,
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .format_async_tracked(db.as_graph_db_ref(), &format_config, &tracker)
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

    pub(crate) async fn query_tracked_with_r2rml(
        &self,
        db: &GraphDb,
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

        let (vars, mut parsed) = match &input {
            QueryInput::JsonLd(json) => {
                parse_jsonld_query(json, &db.snapshot, db.default_context.as_ref(), None).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
            QueryInput::Sparql(sparql) => {
                self.validate_sparql_for_view(sparql).map_err(|e| {
                    crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                })?;
                parse_sparql_to_ir(sparql, &db.snapshot, db.default_context.as_ref()).map_err(
                    |e| {
                        crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
                    },
                )?
            }
        };

        // Auto-wrap for graph source context
        maybe_wrap_for_graph_source(db, &mut parsed);

        let executable = self
            .build_executable_for_view(db, &parsed)
            .await
            .map_err(|e| {
                crate::query::TrackedErrorResponse::new(400, e.to_string(), tracker.tally())
            })?;

        let batches = self
            .execute_view_tracked_with_r2rml(
                db,
                &vars,
                &executable,
                &tracker,
                r2rml_provider,
                r2rml_table_provider,
            )
            .await
            .map_err(|e| {
                let status = query_error_to_status(&e);
                crate::query::TrackedErrorResponse::new(status, e.to_string(), tracker.tally())
            })?;

        let query_result = build_query_result(
            vars,
            parsed,
            batches,
            Some(db.t),
            Some(db.overlay.clone()),
            db.binary_graph(),
        );

        if query_result.output.construct_template().is_some()
            && format_config.format != crate::format::OutputFormat::JsonLd
        {
            format_config = crate::format::FormatterConfig::jsonld();
        }

        let result_json = match db.policy() {
            Some(policy) => query_result
                .format_async_with_policy_tracked(
                    db.as_graph_db_ref(),
                    &format_config,
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e| {
                    crate::query::TrackedErrorResponse::new(500, e.to_string(), tracker.tally())
                })?,
            None => query_result
                .format_async_tracked(db.as_graph_db_ref(), &format_config, &tracker)
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

    /// Validate that SPARQL doesn't have dataset clauses (FROM/FROM NAMED).
    ///
    /// A GraphDb is single-ledger; dataset clauses would conflict with
    /// the db's ledger alias.
    fn validate_sparql_for_view(&self, sparql: &str) -> Result<()> {
        let ast = parse_and_validate_sparql(sparql)?;

        // Check for dataset clauses
        let has_dataset = match &ast.body {
            fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.is_some(),
            fluree_db_sparql::ast::QueryBody::Update(_) => false,
        };

        if has_dataset {
            return Err(ApiError::query(
                "SPARQL FROM/FROM NAMED clauses are not supported on a single-ledger GraphDb. \
                 Use query_connection_sparql for multi-ledger queries.",
            ));
        }

        Ok(())
    }

    /// Build an ExecutableQuery with optional reasoning override.
    ///
    /// Also enforces config-graph datalog restrictions: if config disables
    /// datalog and the query can't override, the datalog flag and/or
    /// query-time rules are stripped. When reasoning config declares an
    /// `f:schemaSource` (with optional `owl:imports` closure), the resolved
    /// schema bundle is attached to `options.schema_bundle` so the runner
    /// can layer it as a `SchemaBundleOverlay` at prep time.
    async fn build_executable_for_view(
        &self,
        db: &GraphDb,
        parsed: &fluree_db_query::ir::Query,
    ) -> Result<ExecutableQuery> {
        // Start with the standard executable
        let mut executable = prepare_for_execution(parsed);

        // Apply wrapper reasoning if applicable
        if db.reasoning().is_some() {
            // Check query's reasoning state
            let query_has_reasoning = executable.reasoning.modes.has_any_enabled();
            let query_disabled = executable.reasoning.modes.is_disabled();

            // Apply precedence rules
            if let Some(effective) = db.effective_reasoning(query_has_reasoning, query_disabled) {
                executable.reasoning.modes = effective.clone();
            }
        }

        // Enforce config-graph datalog restrictions
        if !db.datalog_override_allowed() {
            // Config override denied — force config settings
            if !db.datalog_enabled() {
                executable.reasoning.modes.datalog = false;
            }
            if !db.query_time_rules_allowed() {
                executable.reasoning.modes.rules.clear();
            }
        }

        // Carry the pre-resolved `f:rulesSource` graph id (if any)
        // into the executable so `compute_derived_facts` extracts
        // datalog rules from the configured graph instead of the
        // query graph.
        executable.reasoning.rules_source_g_id = db.rules_source_g_id();

        // Build a single per-request `ResolveCtx` so every
        // cross-ledger artifact (rules, schema, …) captured by this
        // query observes a coherent head-t per model ledger. Two
        // separate contexts would each lazy-capture a head-t and
        // could disagree if M advances between awaits — that breaks
        // the resolver's per-request consistency contract.
        //
        // Seeded from `db.cross_ledger_resolved_ts` so a preceding
        // `wrap_policy` call's captures carry forward: policy and
        // reasoning/rules on the same M must agree on which
        // version of M they're enforcing, even though they enter
        // through separate Rust API calls.
        let mut ctx = crate::cross_ledger::ResolveCtx::with_resolved_ts(
            db.as_graph_db_ref().snapshot.ledger_id.as_str(),
            self,
            (**db.cross_ledger_resolved_ts()).clone(),
        );

        // Cross-ledger `f:rulesSource`: when M is referenced via
        // `f:ledger`, resolve M's rules graph through the
        // cross-ledger resolver and merge the JSON rule bodies into
        // `executable.reasoning.rules` so they pass through the
        // existing query-time rule code path. Same-ledger references
        // are handled above via `rules_source_g_id`.
        self.attach_cross_ledger_rules(db, &mut executable, &mut ctx)
            .await?;

        // Resolve `f:schemaSource` + `owl:imports` closure, if configured.
        self.attach_schema_bundle(db, &mut executable, &mut ctx)
            .await?;

        Ok(executable)
    }

    /// If the resolved datalog config carries a cross-ledger
    /// `f:rulesSource`, dispatch through the cross-ledger resolver
    /// and append the parsed JSON rules to
    /// `executable.reasoning.rules`. Short-circuits when:
    /// - the view has no resolved config,
    /// - no `f:rulesSource` is configured,
    /// - `f:rulesSource` is purely local (`f:ledger` unset — handled
    ///   by the `rules_source_g_id` pre-resolution path),
    /// - datalog reasoning is not enabled on the executable (no point
    ///   pulling rules we won't run).
    ///
    /// Errors propagate as `ApiError::CrossLedger`; the server maps
    /// those to HTTP 502.
    async fn attach_cross_ledger_rules(
        &self,
        db: &GraphDb,
        executable: &mut ExecutableQuery,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<()> {
        if !executable.reasoning.modes.datalog {
            return Ok(());
        }
        let Some(resolved) = db.resolved_config() else {
            return Ok(());
        };
        let Some(datalog) = resolved.datalog.as_ref() else {
            return Ok(());
        };
        let Some(source) = datalog.rules_source.as_ref() else {
            return Ok(());
        };
        if source.ledger.is_none() {
            return Ok(());
        }

        let resolved = crate::cross_ledger::resolve_graph_ref(
            source,
            crate::cross_ledger::ArtifactKind::Rules,
            ctx,
        )
        .await?;
        let crate::cross_ledger::GovernanceArtifact::Rules(wire) = &resolved.artifact else {
            return Err(crate::error::ApiError::CrossLedger(
                crate::cross_ledger::CrossLedgerError::TranslationFailed {
                    ledger_id: resolved.model_ledger_id.clone(),
                    graph_iri: resolved.graph_iri.clone(),
                    detail: "resolver returned a non-Rules artifact for a Rules request; \
                             resolver dispatch bug"
                        .into(),
                },
            ));
        };
        executable
            .reasoning
            .modes
            .rules
            .extend(wire.parsed_rules()?);
        Ok(())
    }

    /// Resolve the schema bundle from the ledger's reasoning config and attach
    /// the projected schema flakes to `executable.reasoning.schema_bundle`.
    ///
    /// Short-circuits in three cases (no bundle is built, no error is
    /// raised):
    /// - The view has no resolved config.
    /// - Reasoning defaults have no `f:schemaSource`.
    /// - The effective query reasoning is **explicitly disabled**
    ///   (`"reasoning": "none"`). Users who opt out of reasoning must not
    ///   be exposed to errors from an otherwise-unrelated broken ontology
    ///   import; the bundle is a reasoning-only concern.
    ///
    /// Errors with [`ApiError::OntologyImport`] only when reasoning is
    /// actually engaged and an import can't be resolved locally.
    async fn attach_schema_bundle(
        &self,
        db: &GraphDb,
        executable: &mut ExecutableQuery,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<()> {
        if executable.reasoning.modes.is_disabled() {
            return Ok(());
        }

        let db_ref = db.as_graph_db_ref();

        // 1. Resolve the configured `f:schemaSource` (if any) into a
        //    bundle. Either branch — cross-ledger or local — may yield
        //    None when the field isn't configured.
        let configured_bundle = self
            .resolve_configured_schema_bundle(db, &db_ref, ctx)
            .await?;

        // 2. Parse inline `opts.ontology` axioms (if any) into a
        //    bundle. Layered on top of `configured_bundle` so a
        //    query can extend the ledger's reasoning with per-request
        //    axioms without persisting them.
        let inline_bundle = match executable.reasoning.modes.ontology.as_ref() {
            Some(json) => {
                crate::inline_ontology::parse_inline_ontology_to_bundle(json, db_ref.snapshot)?
            }
            None => None,
        };

        // 3. Merge.
        executable.reasoning.schema_bundle = match (configured_bundle, inline_bundle) {
            (None, None) => None,
            (Some(b), None) | (None, Some(b)) => Some(b),
            (Some(a), Some(b)) => Some(crate::inline_ontology::merge_bundles(a, b)?),
        };
        Ok(())
    }

    /// Resolve the configured `f:schemaSource` (same- or cross-ledger)
    /// into a [`SchemaBundleFlakes`]. Returns `Ok(None)` when no
    /// `f:schemaSource` is configured. Extracted so
    /// [`attach_schema_bundle`] can layer the inline ontology on top
    /// regardless of which configured branch ran (or whether either
    /// ran).
    async fn resolve_configured_schema_bundle(
        &self,
        db: &GraphDb,
        db_ref: &fluree_db_core::GraphDbRef<'_>,
        ctx: &mut crate::cross_ledger::ResolveCtx<'_>,
    ) -> Result<Option<std::sync::Arc<fluree_db_query::schema_bundle::SchemaBundleFlakes>>> {
        let Some(resolved) = db.resolved_config() else {
            return Ok(None);
        };
        let Some(reasoning) = resolved.reasoning.as_ref() else {
            return Ok(None);
        };
        let Some(schema_source) = reasoning.schema_source.as_ref() else {
            return Ok(None);
        };

        // Cross-ledger detection: if the source carries `f:ledger`,
        // dispatch through the cross-ledger resolver and translate
        // the resulting `SchemaArtifactWire` into a SchemaBundleFlakes
        // against D's snapshot.
        if schema_source.ledger.is_some() {
            let resolved = crate::cross_ledger::resolve_graph_ref(
                schema_source,
                crate::cross_ledger::ArtifactKind::SchemaClosure,
                ctx,
            )
            .await?;
            let crate::cross_ledger::GovernanceArtifact::SchemaClosure(wire) = &resolved.artifact
            else {
                return Err(crate::error::ApiError::CrossLedger(
                    crate::cross_ledger::CrossLedgerError::TranslationFailed {
                        ledger_id: resolved.model_ledger_id.clone(),
                        graph_iri: resolved.graph_iri.clone(),
                        detail: "resolver returned a non-SchemaClosure artifact for a \
                                SchemaClosure request; resolver dispatch bug"
                            .into(),
                    },
                ));
            };
            return Ok(Some(
                wire.translate_to_schema_bundle_flakes(db_ref.snapshot)?,
            ));
        }

        let Some(bundle) = crate::ontology_imports::resolve_schema_bundle(
            db_ref.snapshot,
            db_ref.overlay,
            db_ref.t,
            reasoning,
        )
        .await?
        else {
            return Ok(None);
        };

        let flakes = crate::ontology_imports::get_or_build_schema_bundle_flakes(
            db_ref.snapshot,
            db_ref.overlay,
            &bundle,
        )
        .await?;
        Ok(Some(flakes))
    }

    /// Execute against a GraphDb with policy awareness.
    ///
    /// Single internal path that handles both policy and non-policy execution.
    /// Threads `binary_store` from the db into `ContextConfig` so that
    /// `BinaryScanOperator` can use the binary cursor path when available.
    pub(crate) async fn execute_view_internal(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> Result<Vec<crate::Batch>> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_view_internal_with_r2rml(db, vars, executable, tracker, &noop, &noop)
            .await
    }

    /// Execute against a GraphDb with explicit R2RML provider.
    ///
    /// Used by callers that need R2RML/Iceberg graph source support
    /// (e.g., server query handlers with iceberg support).
    pub(crate) async fn execute_view_internal_with_r2rml<'b>(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml_provider: &'b dyn fluree_db_query::r2rml::R2rmlProvider,
        r2rml_table_provider: &'b dyn fluree_db_query::r2rml::R2rmlTableProvider,
    ) -> Result<Vec<crate::Batch>> {
        let db_ref = db.as_graph_db_ref();
        // Single-graph view: no dataset-level history detection — current state.
        let prepare_config = PrepareConfig::current(db.binary_store.as_ref());
        let prepared = prepare_execution_with_config(db_ref, executable, &prepare_config)
            .await
            .map_err(query_error_to_api_error)?;

        let spatial_map = db.binary_store.as_ref().map(|s| s.spatial_provider_map());
        // Perf guardrail: skip fulltext arena map + `"en"` lang_id resolution
        // for queries that don't actually call `fulltext(...)`. The setup
        // cost (HashMap clone over every (graph, predicate, language) arena
        // plus one lang dict probe) is real on wide ledgers — an unrelated
        // query shouldn't pay it.
        let uses_fulltext = executable.uses_fulltext();
        let fulltext_map = if uses_fulltext {
            db.binary_store.as_ref().map(|s| s.fulltext_provider_map())
        } else {
            None
        };
        let english_lang_id = if uses_fulltext {
            db.binary_store
                .as_ref()
                .and_then(|s| s.resolve_lang_id("en"))
        } else {
            None
        };

        let config = ContextConfig {
            tracker: Some(tracker),
            policy_enforcer: db.policy_enforcer().cloned(),
            r2rml: Some((r2rml_provider, r2rml_table_provider)),
            binary_store: db.binary_store.clone(),
            binary_g_id: db.graph_id,
            dict_novelty: db.dict_novelty.clone(),
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            english_lang_id,
            remote_service: self.remote_service_executor(),
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(db_ref, vars, prepared, config)
            .await
            .map_err(query_error_to_api_error)
    }

    /// Execute against a GraphDb with policy awareness (tracked variant).
    ///
    /// Uses tracked execution functions to properly record fuel/time/policy stats.
    pub(crate) async fn execute_view_tracked(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let noop = crate::NoOpR2rmlProvider::new();
        self.execute_view_tracked_with_r2rml(db, vars, executable, tracker, &noop, &noop)
            .await
    }

    pub(crate) async fn execute_view_tracked_with_r2rml(
        &self,
        db: &GraphDb,
        vars: &crate::VarRegistry,
        executable: &ExecutableQuery,
        tracker: &Tracker,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<Vec<crate::Batch>, fluree_db_query::QueryError> {
        let db_ref = db.as_graph_db_ref();
        // Single-graph view: no dataset-level history detection — current state.
        let prepare_config = PrepareConfig::current(db.binary_store.as_ref());
        let prepared = prepare_execution_with_config(db_ref, executable, &prepare_config).await?;

        let spatial_map = db.binary_store.as_ref().map(|s| s.spatial_provider_map());
        // Perf guardrail: skip fulltext arena map + `"en"` lang_id resolution
        // for queries that don't actually call `fulltext(...)`. The setup
        // cost (HashMap clone over every (graph, predicate, language) arena
        // plus one lang dict probe) is real on wide ledgers — an unrelated
        // query shouldn't pay it.
        let uses_fulltext = executable.uses_fulltext();
        let fulltext_map = if uses_fulltext {
            db.binary_store.as_ref().map(|s| s.fulltext_provider_map())
        } else {
            None
        };
        let english_lang_id = if uses_fulltext {
            db.binary_store
                .as_ref()
                .and_then(|s| s.resolve_lang_id("en"))
        } else {
            None
        };

        let config = ContextConfig {
            tracker: Some(tracker),
            policy_enforcer: db.policy_enforcer().cloned(),
            r2rml: Some((r2rml_provider, r2rml_table_provider)),
            binary_store: db.binary_store.clone(),
            binary_g_id: db.graph_id,
            dict_novelty: db.dict_novelty.clone(),
            spatial_providers: spatial_map.as_ref(),
            fulltext_providers: fulltext_map.as_ref(),
            english_lang_id,
            remote_service: self.remote_service_executor(),
            strict_bind_errors: true,
            ..Default::default()
        };

        execute_prepared(db_ref, vars, prepared, config).await
    }
}

// ============================================================================
// Error Conversion Helpers
// ============================================================================

fn query_error_to_api_error(err: fluree_db_query::QueryError) -> ApiError {
    ApiError::query(err.to_string())
}

/// Map QueryError to HTTP-ish status code.
fn query_error_to_status(err: &fluree_db_query::QueryError) -> u16 {
    status_for_query_error(err)
}

#[cfg(test)]
mod tests {

    use crate::FlureeBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_query_jsonld() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data (using full IRIs)
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        let db = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = fluree.query(&db, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_sparql() {
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

        let db = fluree.db("testdb:main").await.unwrap();
        let result = fluree
            .query(
                &db,
                "SELECT ?name WHERE { <http://example.org/alice> <http://example.org/name> ?name }",
            )
            .await
            .unwrap();

        assert!(!result.batches.is_empty());
    }

    #[tokio::test]
    async fn test_query_sparql_with_dataset_clause_rejected() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let db = fluree.db("testdb:main").await.unwrap();

        // SPARQL with FROM clause should be rejected
        let result = fluree
            .query(
                &db,
                "SELECT * FROM <http://other.org/ledger> WHERE { ?s ?p ?o }",
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("FROM"));
    }

    #[tokio::test]
    async fn test_query_jsonld_format() {
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

        let db = fluree.db("testdb:main").await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });

        let result = db
            .query(&fluree)
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();

        // Should be JSON-LD formatted
        assert!(result.is_array() || result.is_object());
    }

    #[tokio::test]
    async fn test_query_with_time_travel() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create ledger with data at t=1
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({
            "insert": [{
                "@id": "http://example.org/alice",
                "http://example.org/name": "Alice"
            }]
        });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Query at t=0 (before insert)
        let db = fluree.db_at_t("testdb:main", 0).await.unwrap();
        let query = json!({
            "select": ["?name"],
            "where": {"@id": "http://example.org/alice", "http://example.org/name": "?name"}
        });
        let result = fluree.query(&db, &query).await.unwrap();
        assert!(result.batches.is_empty() || result.batches[0].is_empty());

        // Query at t=1 (after insert)
        let db = fluree.db_at_t("testdb:main", 1).await.unwrap();
        let result = fluree.query(&db, &query).await.unwrap();
        assert!(!result.batches.is_empty());
    }
}
