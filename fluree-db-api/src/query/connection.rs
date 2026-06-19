use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::query::helpers::{
    charge_query_floor, extract_sparql_dataset_spec, parse_and_validate_sparql, parse_dataset_spec,
    tracked_query_tracker,
};
use crate::view::{DataSetDb, GraphDb, QueryInput};
use crate::{
    ApiError, DatasetSpec, Fluree, FormatterConfig, GovernanceOptions, PolicyContext,
    QueryExecutionOptions, QueryResult, Result,
};
use fluree_db_core::TrackingOptions;
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};

type TrackedResult<T> = std::result::Result<T, crate::query::TrackedErrorResponse>;

impl Fluree {
    async fn prepare_single_view_for_connection(
        &self,
        spec: &DatasetSpec,
        qc_opts: &GovernanceOptions,
    ) -> Result<Option<GraphDb>> {
        let Some(view) = self.try_single_view_from_spec(spec).await? else {
            return Ok(None);
        };

        let source = &spec.default_graphs[0];
        let view = self
            .apply_source_or_global_policy(view, source, qc_opts)
            .await?;
        let view = self.apply_config_defaults(view, None);
        Ok(Some(view))
    }

    async fn build_dataset_for_connection(
        &self,
        spec: &DatasetSpec,
        qc_opts: &GovernanceOptions,
    ) -> Result<DataSetDb> {
        if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(spec, qc_opts).await
        } else {
            self.build_dataset_view(spec).await
        }
    }

    async fn prepare_single_view_for_connection_tracked(
        &self,
        spec: &DatasetSpec,
        qc_opts: &GovernanceOptions,
    ) -> TrackedResult<Option<GraphDb>> {
        let view = self
            .try_single_view_from_spec(spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        let Some(view) = view else {
            return Ok(None);
        };

        let source = &spec.default_graphs[0];
        let view = self
            .apply_source_or_global_policy(view, source, qc_opts)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let view = self.apply_config_defaults(view, None);
        Ok(Some(view))
    }

    async fn build_dataset_for_connection_tracked(
        &self,
        spec: &DatasetSpec,
        qc_opts: &GovernanceOptions,
    ) -> TrackedResult<DataSetDb> {
        let dataset = if qc_opts.has_any_policy_inputs() {
            self.build_dataset_view_with_policy(spec, qc_opts).await
        } else {
            self.build_dataset_view(spec).await
        };
        dataset.map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))
    }

    async fn prepare_single_view_for_connection_with_policy(
        &self,
        spec: &DatasetSpec,
        policy: &PolicyContext,
    ) -> Result<Option<GraphDb>> {
        let Some(view) = self.try_single_view_from_spec(spec).await? else {
            return Ok(None);
        };
        let view = view.with_policy(Arc::new(policy.clone()));
        let view = self.apply_config_defaults(view, None);
        Ok(Some(view))
    }

    /// Execute a JSON-LD query via connection.
    ///
    /// This is the unified entry point for connection queries. It:
    /// 1. Parses dataset spec and options from query JSON
    /// 2. For single-ledger: builds a `GraphDb` and uses view API
    /// 3. For multi-ledger: builds a `DataSetDb` for proper merge
    /// 4. Applies policy wrappers if policy options are present
    pub async fn query_connection(&self, query_json: &JsonValue) -> Result<QueryResult> {
        self.query_connection_with_options(query_json, QueryExecutionOptions::default())
            .await
    }

    pub async fn query_connection_with_options(
        &self,
        query_json: &JsonValue,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection(&spec, &qc_opts)
            .await?
        {
            return self.query_with_options(&view, query_json, options).await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = self.build_dataset_for_connection(&spec, &qc_opts).await?;

        self.query_dataset_with_options(&dataset, query_json, options)
            .await
    }

    /// Execute a JSON-LD connection query and, for the multi-ledger case,
    /// return the `DataSetDb` alongside the result so the caller can format
    /// hydration output against each ledger's own view (issue #1259).
    ///
    /// Mirrors the policy/r2rml combinations of [`Self::query_connection`],
    /// [`Self::query_connection_with_policy`],
    /// [`Self::query_connection_jsonld_with_r2rml`], and
    /// [`Self::query_connection_with_policy_and_r2rml`] in one place. Returns
    /// `None` for the single-ledger fast path (formatting is correct against
    /// the sole view) and `Some(dataset)` for genuine multi-ledger queries.
    pub(crate) async fn query_connection_jsonld_returning_dataset_with_options(
        &self,
        query_json: &JsonValue,
        policy: Option<&PolicyContext>,
        r2rml: Option<(&dyn R2rmlProvider, &dyn R2rmlTableProvider)>,
        options: QueryExecutionOptions,
    ) -> Result<(QueryResult, Option<DataSetDb>)> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        // Single-ledger fast path — no dataset needed for formatting.
        let single = match policy {
            Some(p) => {
                self.prepare_single_view_for_connection_with_policy(&spec, p)
                    .await?
            }
            None => {
                self.prepare_single_view_for_connection(&spec, &qc_opts)
                    .await?
            }
        };
        if let Some(view) = single {
            let result = match r2rml {
                Some((rp, rtp)) => {
                    self.query_view_with_r2rml_options(&view, query_json, rp, rtp, options)
                        .await?
                }
                None => self.query_with_options(&view, query_json, options).await?,
            };
            return Ok((result, None));
        }

        // Multi-ledger: build the DataSetDb (with per-view policy) and keep it
        // alive so the formatter can route hydration per ledger.
        let dataset = match policy {
            Some(p) => apply_policy_to_dataset(self.build_dataset_view(&spec).await?, p),
            None => self.build_dataset_for_connection(&spec, &qc_opts).await?,
        };
        let result = match r2rml {
            Some((rp, rtp)) => {
                self.query_dataset_with_r2rml_options(&dataset, query_json, rp, rtp, options)
                    .await?
            }
            None => {
                self.query_dataset_with_options(&dataset, query_json, options)
                    .await?
            }
        };
        Ok((result, Some(dataset)))
    }

    /// Execute a JSON-LD connection query with explicit R2RML providers.
    ///
    /// Uses graph source fallback for alias resolution: if a source in the
    /// dataset spec is not found as a ledger, it checks graph sources and
    /// creates a minimal genesis context tagged with the graph source ID.
    pub(crate) async fn query_connection_jsonld_with_r2rml_options(
        &self,
        query_json: &JsonValue,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection(&spec, &qc_opts)
            .await?
        {
            return self
                .query_view_with_r2rml_options(
                    &view,
                    query_json,
                    r2rml_provider,
                    r2rml_table_provider,
                    options,
                )
                .await;
        }

        // Multi-ledger dataset — use standard builder (graph source fallback
        // in dataset is deferred to the scan backend level)
        let dataset = self.build_dataset_for_connection(&spec, &qc_opts).await?;

        self.query_dataset_with_r2rml_options(
            &dataset,
            query_json,
            r2rml_provider,
            r2rml_table_provider,
            options,
        )
        .await
    }

    pub(crate) async fn query_connection_sparql_with_r2rml_options(
        &self,
        sparql: &str,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        self.query_dataset_with_r2rml_options(
            &dataset,
            sparql,
            r2rml_provider,
            r2rml_table_provider,
            options,
        )
        .await
    }

    /// Execute a connection query and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked_with_options(
        &self,
        query_json: &JsonValue,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // Enforce the query floor up front: a sub-floor `max-fuel` fails here
        // (before parsing the dataset spec), matching the per-view tracked
        // path. On success `floor` is a throwaway used only to tally the floor
        // for connection-level parse/spec errors; the per-view delegate charges
        // its own floor downstream, so the reported fuel is never double-counted.
        let input = QueryInput::JsonLd(query_json);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let (spec, qc_opts) = parse_dataset_spec(query_json).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                floor.tally(),
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_tracked(&spec, &qc_opts)
            .await?
        {
            return self
                .query_tracked_with_options(
                    &view,
                    query_json,
                    format_config,
                    tracking_override,
                    options,
                )
                .await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = self
            .build_dataset_for_connection_tracked(&spec, &qc_opts)
            .await?;

        self.query_dataset_tracked_with_options(
            &dataset,
            query_json,
            format_config,
            tracking_override,
            options,
        )
        .await
    }

    /// Compatibility alias: tracked connection query entrypoint.
    pub async fn query_connection_tracked(
        &self,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_connection_jsonld_tracked_with_options(
            query_json,
            None,
            None,
            QueryExecutionOptions::default(),
        )
        .await
    }

    /// Execute a JSON-LD query via connection with explicit policy context.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_with_policy_options(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_with_policy(&spec, policy)
            .await?
        {
            return self.query_with_options(&view, query_json, options).await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_with_options(&dataset, query_json, options)
            .await
    }

    pub(crate) async fn query_connection_with_policy_and_r2rml_options(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection query",
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_with_policy(&spec, policy)
            .await?
        {
            return self
                .query_view_with_r2rml_options(
                    &view,
                    query_json,
                    r2rml_provider,
                    r2rml_table_provider,
                    options,
                )
                .await;
        }

        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_with_r2rml_options(
            &dataset,
            query_json,
            r2rml_provider,
            r2rml_table_provider,
            options,
        )
        .await
    }

    pub(crate) async fn query_connection_jsonld_tracked_with_r2rml_options(
        &self,
        query_json: &JsonValue,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::JsonLd(query_json);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let (spec, qc_opts) = parse_dataset_spec(query_json).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                floor.tally(),
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_tracked(&spec, &qc_opts)
            .await?
        {
            return self
                .query_tracked_with_r2rml_options(
                    &view,
                    query_json,
                    format_config,
                    tracking_override,
                    crate::R2rmlProviders {
                        provider: r2rml_provider,
                        table_provider: r2rml_table_provider,
                    },
                    options,
                )
                .await;
        }

        let dataset = self
            .build_dataset_for_connection_tracked(&spec, &qc_opts)
            .await?;

        self.query_dataset_tracked_with_r2rml_options(
            &dataset,
            query_json,
            format_config,
            tracking_override,
            crate::R2rmlProviders {
                provider: r2rml_provider,
                table_provider: r2rml_table_provider,
            },
            options,
        )
        .await
    }

    pub(crate) async fn query_connection_jsonld_tracked_with_policy_and_r2rml_options(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml: crate::R2rmlProviders<'_>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::JsonLd(query_json);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let (spec, _qc_opts) = parse_dataset_spec(query_json).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                floor.tally(),
            ));
        }

        let single_view = self
            .try_single_view_from_spec(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        if let Some(view) = single_view {
            let view = view.with_policy(Arc::new(policy.clone()));
            let view = self.apply_config_defaults(view, None);
            return self
                .query_tracked_with_r2rml_options(
                    &view,
                    query_json,
                    format_config,
                    tracking_override,
                    r2rml,
                    options,
                )
                .await;
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_tracked_with_r2rml_options(
            &dataset,
            query_json,
            format_config,
            tracking_override,
            r2rml,
            options,
        )
        .await
    }

    /// Execute a connection query with explicit policy context and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked_with_policy_options(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::JsonLd(query_json);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let (spec, _qc_opts) = parse_dataset_spec(query_json).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                floor.tally(),
            ));
        }

        // Try single-ledger path (including with time spec)
        let single_view = self
            .try_single_view_from_spec(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        if let Some(view) = single_view {
            let view = view.with_policy(Arc::new(policy.clone()));
            let view = self.apply_config_defaults(view, None);
            return self
                .query_tracked_with_options(
                    &view,
                    query_json,
                    format_config,
                    tracking_override,
                    options,
                )
                .await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_tracked_with_options(
            &dataset,
            query_json,
            format_config,
            tracking_override,
            options,
        )
        .await
    }

    /// Explain a JSON-LD query via connection.
    ///
    /// Mirrors [`query_connection`] but emits the query plan rather than
    /// executing it. The dataset spec (including any time-travel `from`
    /// suffix like `mydb:main@t:5`) drives snapshot selection, so an
    /// `--at` explain returns the plan at that historical `t` rather than
    /// at HEAD.
    ///
    /// Multi-ledger dataset specs are rejected — explain is single-ledger
    /// (consistent with [`Fluree::explain`] taking a `GraphDb`).
    pub async fn explain_connection(&self, query_json: &JsonValue) -> Result<JsonValue> {
        let (spec, qc_opts) = parse_dataset_spec(query_json)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing ledger specification in connection explain",
            ));
        }

        let Some(view) = self
            .prepare_single_view_for_connection(&spec, &qc_opts)
            .await?
        else {
            return Err(ApiError::query(
                "Multi-ledger datasets are not supported for explain; \
                 specify a single `from` ledger (with optional time-travel suffix).",
            ));
        };

        self.explain(&view, query_json).await
    }

    /// Explain a SPARQL query via connection. SPARQL counterpart to
    /// [`explain_connection`] — requires exactly one `FROM` (with optional
    /// time-travel suffix). Rejects `FROM NAMED` and multi-`FROM` queries,
    /// since the planner is single-ledger.
    pub async fn explain_connection_sparql(&self, sparql: &str) -> Result<JsonValue> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL explain (no FROM)",
            ));
        }

        let Some(view) = self
            .prepare_single_view_for_connection(&spec, &crate::GovernanceOptions::default())
            .await?
        else {
            return Err(ApiError::query(
                "Multi-ledger / FROM NAMED datasets are not supported for SPARQL explain; \
                 use a single `FROM <ledger:branch>` (with optional time-travel suffix).",
            ));
        };

        self.explain_sparql(&view, sparql).await
    }

    /// Execute a SPARQL query via connection (dataset specified via SPARQL `FROM` / `FROM NAMED`).
    ///
    /// Note: SPARQL connection queries allow dataset clauses because the dataset
    /// is being specified at the connection level.
    pub async fn query_connection_sparql(&self, sparql: &str) -> Result<QueryResult> {
        self.query_connection_sparql_with_options(sparql, QueryExecutionOptions::default())
            .await
    }

    pub async fn query_connection_sparql_with_options(
        &self,
        sparql: &str,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        self.query_dataset_with_options(&dataset, sparql, options)
            .await
    }

    pub(crate) async fn query_connection_sparql_with_policy_options(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_with_options(&dataset, sparql, options)
            .await
    }

    /// Execute a SPARQL connection query applying policy derived from
    /// [`GovernanceOptions`] (identity / policy-class / inline policy).
    ///
    /// SPARQL bodies carry no `opts` block, so the multi-query dispatcher
    /// passes the merged envelope/sub opts here explicitly. This mirrors
    /// `query_connection`'s opts→policy behaviour for JSON-LD: when the opts
    /// carry any policy input the dataset is built with policy
    /// (`build_dataset_view_with_policy`), otherwise it is the plain view.
    pub(crate) async fn query_connection_sparql_with_opts_options(
        &self,
        sparql: &str,
        qc_opts: &GovernanceOptions,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_for_connection(&spec, qc_opts).await?;
        self.query_dataset_with_options(&dataset, sparql, options)
            .await
    }

    pub(crate) async fn query_connection_sparql_with_policy_and_r2rml_options(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_with_r2rml_options(
            &dataset,
            sparql,
            r2rml_provider,
            r2rml_table_provider,
            options,
        )
        .await
    }

    /// Execute a SPARQL query via connection with tracking (dataset specified via SPARQL `FROM` / `FROM NAMED`).
    ///
    /// Note: Unlike JSON-LD connection queries, SPARQL always uses the dataset path because
    /// SPARQL FROM clauses specify the dataset and are incompatible with the single-view path
    /// (which validates against FROM clauses).
    pub async fn query_connection_sparql_tracked(
        &self,
        sparql: &str,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_connection_sparql_tracked_with_options(
            sparql,
            format_config,
            tracking_override,
            QueryExecutionOptions::default(),
        )
        .await
    }

    pub async fn query_connection_sparql_tracked_with_options(
        &self,
        sparql: &str,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::Sparql(sparql);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let ast = parse_and_validate_sparql(sparql).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;
        let spec = extract_sparql_dataset_spec(&ast).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                floor.tally(),
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        self.query_dataset_tracked_with_options(
            &dataset,
            sparql,
            format_config,
            tracking_override,
            options,
        )
        .await
    }

    pub async fn query_connection_sparql_tracked_with_r2rml(
        &self,
        sparql: &str,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_connection_sparql_tracked_with_r2rml_options(
            sparql,
            format_config,
            tracking_override,
            r2rml_provider,
            r2rml_table_provider,
            QueryExecutionOptions::default(),
        )
        .await
    }

    pub async fn query_connection_sparql_tracked_with_r2rml_options(
        &self,
        sparql: &str,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::Sparql(sparql);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let ast = parse_and_validate_sparql(sparql).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;
        let spec = extract_sparql_dataset_spec(&ast).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                floor.tally(),
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        self.query_dataset_tracked_with_r2rml_options(
            &dataset,
            sparql,
            format_config,
            tracking_override,
            crate::R2rmlProviders {
                provider: r2rml_provider,
                table_provider: r2rml_table_provider,
            },
            options,
        )
        .await
    }

    pub(crate) async fn query_connection_sparql_tracked_with_policy_options(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::Sparql(sparql);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let ast = parse_and_validate_sparql(sparql).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;
        let spec = extract_sparql_dataset_spec(&ast).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                floor.tally(),
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);

        self.query_dataset_tracked_with_options(
            &dataset,
            sparql,
            format_config,
            tracking_override,
            options,
        )
        .await
    }

    /// Tracked SPARQL connection query applying policy derived from
    /// [`GovernanceOptions`]. Opts→policy twin of
    /// [`Self::query_connection_sparql_tracked`], used by the multi-query
    /// dispatcher for policy-enforced SPARQL aliases under tracking.
    pub(crate) async fn query_connection_sparql_tracked_with_opts_options(
        &self,
        sparql: &str,
        qc_opts: &GovernanceOptions,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::Sparql(sparql);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let ast = parse_and_validate_sparql(sparql).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;
        let spec = extract_sparql_dataset_spec(&ast).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                floor.tally(),
            ));
        }

        let dataset = self
            .build_dataset_for_connection_tracked(&spec, qc_opts)
            .await?;

        self.query_dataset_tracked_with_options(
            &dataset,
            sparql,
            format_config,
            tracking_override,
            options,
        )
        .await
    }

    pub(crate) async fn query_connection_sparql_tracked_with_policy_and_r2rml_options(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        tracking_override: Option<TrackingOptions>,
        r2rml: crate::R2rmlProviders<'_>,
        options: QueryExecutionOptions,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        // See `query_connection_jsonld_tracked` for the up-front floor enforcement.
        let input = QueryInput::Sparql(sparql);
        let floor = tracked_query_tracker(&input, &tracking_override);
        charge_query_floor(&floor)
            .map_err(|e| crate::query::TrackedErrorResponse::fuel_exceeded(&e, floor.tally()))?;
        let ast = parse_and_validate_sparql(sparql).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;
        let spec = extract_sparql_dataset_spec(&ast).map_err(|e| {
            crate::query::TrackedErrorResponse::new(400, e.to_string(), floor.tally())
        })?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                floor.tally(),
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);

        self.query_dataset_tracked_with_r2rml_options(
            &dataset,
            sparql,
            format_config,
            tracking_override,
            r2rml,
            options,
        )
        .await
    }

    /// Apply per-source or global policy to a view.
    ///
    /// Per-source policy takes precedence if present, otherwise global policy is used.
    /// If neither has policy, returns the view unchanged.
    async fn apply_source_or_global_policy(
        &self,
        view: crate::view::GraphDb,
        source: &crate::dataset::GraphSource,
        global_opts: &crate::GovernanceOptions,
    ) -> Result<crate::view::GraphDb> {
        // Per-source policy takes precedence
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts, None).await;
            }
        }
        // Fall back to global policy if present
        if global_opts.has_any_policy_inputs() {
            self.wrap_policy(view, global_opts, None).await
        } else {
            Ok(view)
        }
    }
}

fn apply_policy_to_dataset(mut dataset: DataSetDb, policy: &PolicyContext) -> DataSetDb {
    let policy = Arc::new(policy.clone());

    dataset.default = dataset
        .default
        .into_iter()
        .map(|v| v.with_policy(Arc::clone(&policy)))
        .collect();

    dataset.named = dataset
        .named
        .into_iter()
        .map(|(k, v)| (k, v.with_policy(Arc::clone(&policy))))
        .collect();

    dataset
}
