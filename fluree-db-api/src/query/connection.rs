use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::query::helpers::{
    extract_sparql_dataset_spec, parse_and_validate_sparql, parse_dataset_spec,
};
use crate::view::{DataSetDb, GraphDb};
use crate::{
    ApiError, DatasetSpec, Fluree, FormatterConfig, PolicyContext, QueryConnectionOptions,
    QueryResult, Result,
};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};

type TrackedResult<T> = std::result::Result<T, crate::query::TrackedErrorResponse>;

impl Fluree {
    async fn prepare_single_view_for_connection(
        &self,
        spec: &DatasetSpec,
        qc_opts: &QueryConnectionOptions,
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
        qc_opts: &QueryConnectionOptions,
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
        qc_opts: &QueryConnectionOptions,
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
        qc_opts: &QueryConnectionOptions,
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
            return self.query(&view, query_json).await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = self.build_dataset_for_connection(&spec, &qc_opts).await?;

        self.query_dataset(&dataset, query_json).await
    }

    /// Execute a JSON-LD connection query with explicit R2RML providers.
    ///
    /// Uses graph source fallback for alias resolution: if a source in the
    /// dataset spec is not found as a ledger, it checks graph sources and
    /// creates a minimal genesis context tagged with the graph source ID.
    pub(crate) async fn query_connection_jsonld_with_r2rml(
        &self,
        query_json: &JsonValue,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
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
                .query_view_with_r2rml(&view, query_json, r2rml_provider, r2rml_table_provider)
                .await;
        }

        // Multi-ledger dataset — use standard builder (graph source fallback
        // in dataset is deferred to the scan backend level)
        let dataset = self.build_dataset_for_connection(&spec, &qc_opts).await?;

        self.query_dataset_with_r2rml(&dataset, query_json, r2rml_provider, r2rml_table_provider)
            .await
    }

    /// Execute a SPARQL connection query with explicit R2RML providers.
    pub(crate) async fn query_connection_sparql_with_r2rml(
        &self,
        sparql: &str,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        self.query_dataset_with_r2rml(&dataset, sparql, r2rml_provider, r2rml_table_provider)
            .await
    }

    /// Execute a connection query and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked(
        &self,
        query_json: &JsonValue,
        format_config: Option<FormatterConfig>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_tracked(&spec, &qc_opts)
            .await?
        {
            return self
                .query_tracked(&view, query_json, format_config, None)
                .await;
        }

        // Multi-ledger: use DataSetDb
        let dataset = self
            .build_dataset_for_connection_tracked(&spec, &qc_opts)
            .await?;

        self.query_dataset_tracked(&dataset, query_json, format_config, None)
            .await
    }

    /// Compatibility alias: tracked connection query entrypoint.
    pub async fn query_connection_tracked(
        &self,
        query_json: &JsonValue,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        self.query_connection_jsonld_tracked(query_json, None).await
    }

    /// Execute a JSON-LD query via connection with explicit policy context.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_with_policy(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
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
            return self.query(&view, query_json).await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset(&dataset, query_json).await
    }

    pub(crate) async fn query_connection_with_policy_and_r2rml(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
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
                .query_view_with_r2rml(&view, query_json, r2rml_provider, r2rml_table_provider)
                .await;
        }

        let dataset = self.build_dataset_view(&spec).await?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_with_r2rml(&dataset, query_json, r2rml_provider, r2rml_table_provider)
            .await
    }

    pub(crate) async fn query_connection_jsonld_tracked_with_r2rml(
        &self,
        query_json: &JsonValue,
        format_config: Option<FormatterConfig>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
            ));
        }

        if let Some(view) = self
            .prepare_single_view_for_connection_tracked(&spec, &qc_opts)
            .await?
        {
            return self
                .query_tracked_with_r2rml(
                    &view,
                    query_json,
                    format_config,
                    None,
                    r2rml_provider,
                    r2rml_table_provider,
                )
                .await;
        }

        let dataset = self
            .build_dataset_for_connection_tracked(&spec, &qc_opts)
            .await?;

        self.query_dataset_tracked_with_r2rml(
            &dataset,
            query_json,
            format_config,
            None,
            r2rml_provider,
            r2rml_table_provider,
        )
        .await
    }

    pub(crate) async fn query_connection_jsonld_tracked_with_policy_and_r2rml(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
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
                .query_tracked_with_r2rml(
                    &view,
                    query_json,
                    format_config,
                    None,
                    r2rml_provider,
                    r2rml_table_provider,
                )
                .await;
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_tracked_with_r2rml(
            &dataset,
            query_json,
            format_config,
            None,
            r2rml_provider,
            r2rml_table_provider,
        )
        .await
    }

    /// Execute a connection query with explicit policy context and return a tracked JSON-LD response.
    ///
    /// Uses GraphDb API for single-ledger, DataSetDb for multi-ledger.
    pub(crate) async fn query_connection_jsonld_tracked_with_policy(
        &self,
        query_json: &JsonValue,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let (spec, _qc_opts) = parse_dataset_spec(query_json)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing ledger specification in connection query",
                None,
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
                .query_tracked(&view, query_json, format_config, None)
                .await;
        }

        // Multi-ledger: use DataSetDb and apply explicit policy to each view
        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);
        self.query_dataset_tracked(&dataset, query_json, format_config, None)
            .await
    }

    /// Execute a SPARQL query via connection (dataset specified via SPARQL `FROM` / `FROM NAMED`).
    ///
    /// Note: SPARQL connection queries allow dataset clauses because the dataset
    /// is being specified at the connection level.
    pub async fn query_connection_sparql(&self, sparql: &str) -> Result<QueryResult> {
        let ast = parse_and_validate_sparql(sparql)?;
        let spec = extract_sparql_dataset_spec(&ast)?;

        if spec.is_empty() {
            return Err(ApiError::query(
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
            ));
        }

        let dataset = self.build_dataset_view(&spec).await?;
        self.query_dataset(&dataset, sparql).await
    }

    /// Execute a SPARQL query via connection with explicit policy context.
    pub(crate) async fn query_connection_sparql_with_policy(
        &self,
        sparql: &str,
        policy: &PolicyContext,
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
        self.query_dataset(&dataset, sparql).await
    }

    pub(crate) async fn query_connection_sparql_with_policy_and_r2rml(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
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
        self.query_dataset_with_r2rml(&dataset, sparql, r2rml_provider, r2rml_table_provider)
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
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        self.query_dataset_tracked(&dataset, sparql, format_config, None)
            .await
    }

    pub async fn query_connection_sparql_tracked_with_r2rml(
        &self,
        sparql: &str,
        format_config: Option<FormatterConfig>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;

        self.query_dataset_tracked_with_r2rml(
            &dataset,
            sparql,
            format_config,
            None,
            r2rml_provider,
            r2rml_table_provider,
        )
        .await
    }

    /// Execute a SPARQL query via connection with explicit policy context and tracking.
    ///
    /// Note: Unlike JSON-LD connection queries, SPARQL always uses the dataset path because
    /// SPARQL FROM clauses specify the dataset and are incompatible with the single-view path
    /// (which validates against FROM clauses).
    pub(crate) async fn query_connection_sparql_tracked_with_policy(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);

        self.query_dataset_tracked(&dataset, sparql, format_config, None)
            .await
    }

    pub(crate) async fn query_connection_sparql_tracked_with_policy_and_r2rml(
        &self,
        sparql: &str,
        policy: &PolicyContext,
        format_config: Option<FormatterConfig>,
        r2rml_provider: &dyn R2rmlProvider,
        r2rml_table_provider: &dyn R2rmlTableProvider,
    ) -> std::result::Result<crate::query::TrackedQueryResponse, crate::query::TrackedErrorResponse>
    {
        let ast = parse_and_validate_sparql(sparql)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;
        let spec = extract_sparql_dataset_spec(&ast)
            .map_err(|e| crate::query::TrackedErrorResponse::new(400, e.to_string(), None))?;

        if spec.is_empty() {
            return Err(crate::query::TrackedErrorResponse::new(
                400,
                "Missing dataset specification in SPARQL connection query (no FROM / FROM NAMED)",
                None,
            ));
        }

        let dataset = self
            .build_dataset_view(&spec)
            .await
            .map_err(|e| crate::query::TrackedErrorResponse::new(500, e.to_string(), None))?;
        let dataset = apply_policy_to_dataset(dataset, policy);

        self.query_dataset_tracked_with_r2rml(
            &dataset,
            sparql,
            format_config,
            None,
            r2rml_provider,
            r2rml_table_provider,
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
        global_opts: &crate::QueryConnectionOptions,
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
