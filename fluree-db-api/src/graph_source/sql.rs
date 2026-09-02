//! SQL graph sources: an R2RML mapping over tables reached through a
//! Trino-protocol HTTP endpoint.
//!
//! Registration mirrors the Iceberg/R2RML path (mapping compiled and stored in
//! CAS, record published under `f:SqlMapping`), and scans are served through
//! the same [`super::FlureeR2rmlProvider`], which dispatches here when the
//! record's type is `Sql`. A SQL source has no snapshot to pin, so its build
//! watermark records the endpoint, table and first-touch time.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::plan::RelSource;
use fluree_db_query::r2rml::{
    ColumnBatchStream, PushdownCapabilities, RelPlan, ScanFilter, ScanValue, TableWatermark,
};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use fluree_db_sql::{
    AuthConfig, CmpOp, Literal, LogicalSource, MappingSource, Predicate, ScanRequest, SqlDialect,
    SqlError, SqlGsConfig, TrinoClient, WireProtocol,
};
use fluree_db_tabular::BatchSchema;
use futures::StreamExt;
use tracing::{debug, info, warn};

use super::config::R2rmlMappingInput;
use crate::graph_source::catalog_session::IcebergCatalogSession;

/// Everything needed to register a SQL graph source.
#[derive(Debug, Clone)]
pub struct SqlCreateConfig {
    /// Graph source name (e.g. `"warehouse-sql"`).
    pub name: String,
    /// Branch (defaults to `"main"`).
    pub branch: Option<String>,
    /// Statement endpoint base URL.
    pub endpoint: String,
    pub dialect: SqlDialect,
    pub protocol: WireProtocol,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    /// `X-Trino-User`; defaults to `fluree`.
    pub user: Option<String>,
    pub auth: AuthConfig,
    pub session: BTreeMap<String, String>,
    /// The R2RML mapping — inline content or a pre-existing address.
    pub mapping: R2rmlMappingInput,
    pub mapping_media_type: Option<String>,
    /// Accept subject keys the registration probe finds non-unique (the
    /// pushdown lane otherwise refuses statements over those tables).
    pub allow_duplicate_subjects: bool,
    /// Optional model ledger (`name:branch`) whose default graph supplies the
    /// source's view policies and class/property hierarchy.
    pub model: Option<String>,
    /// Optional `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
}

impl SqlCreateConfig {
    pub fn new(
        name: impl Into<String>,
        endpoint: impl Into<String>,
        mapping_content: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            endpoint: endpoint.into(),
            dialect: SqlDialect::default(),
            protocol: WireProtocol::default(),
            catalog: None,
            schema: None,
            user: None,
            auth: AuthConfig::default(),
            session: BTreeMap::new(),
            mapping: R2rmlMappingInput::Content(mapping_content.into()),
            mapping_media_type: None,
            allow_duplicate_subjects: false,
            model: None,
            default_allow: None,
        }
    }

    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or("main")
    }

    pub fn graph_source_id(&self) -> String {
        format!("{}:{}", self.name, self.effective_branch())
    }

    /// The persisted config, with the mapping's CAS address filled in.
    pub fn to_gs_config(&self, mapping_address: &str) -> SqlGsConfig {
        let mut cfg = SqlGsConfig::new(self.endpoint.clone());
        cfg.dialect = self.dialect;
        cfg.protocol = self.protocol;
        cfg.catalog = self.catalog.clone();
        cfg.schema = self.schema.clone();
        if let Some(u) = &self.user {
            cfg.user = u.clone();
        }
        cfg.auth = self.auth.clone();
        cfg.session = self.session.clone();
        let media_type = self.mapping_media_type.clone().unwrap_or_else(|| {
            fluree_db_r2rml::loader::MappingFormat::resolve(None, mapping_address)
                .media_type()
                .to_string()
        });
        cfg.mapping = Some(MappingSource {
            source: mapping_address.to_string(),
            media_type: Some(media_type),
        });
        cfg.allow_duplicate_subjects = self.allow_duplicate_subjects;
        cfg.model = self.model.clone();
        cfg.default_allow = self.default_allow;
        cfg
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::Config(
                "graph source name must not be empty".to_string(),
            ));
        }
        if self.name.contains(':') {
            return Err(crate::ApiError::Config(format!(
                "graph source name '{}' may not contain ':'",
                self.name
            )));
        }
        self.to_gs_config("")
            .validate()
            .map_err(|e| crate::ApiError::Config(e.to_string()))
    }
}

/// What `create_sql_graph_source` reports back.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SqlCreateResult {
    pub graph_source_id: String,
    pub endpoint: String,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_count: usize,
    pub table_names: Vec<String>,
    /// Whether `SELECT 1` succeeded against the endpoint. A failure is logged,
    /// not fatal: the record is still created (credentials may arrive later).
    pub connection_tested: bool,
    pub mapping_validated: bool,
    /// Warnings about the `model` reference (policies a virtual source cannot
    /// evaluate). Empty when no model is set or nothing is amiss.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_warnings: Vec<String>,
    /// Findings of the registration probe: tables whose subject keys repeat,
    /// or a probe that could not run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mapping_warnings: Vec<String>,
}

/// Result of `fluree sql check`: the re-run uniqueness probe.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SqlCheckResult {
    pub graph_source_id: String,
    pub duplicate_subject_tables: Vec<String>,
    pub allow_duplicate_subjects: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mapping_warnings: Vec<String>,
}

/// The key columns per table whose uniqueness the mapping assumes: every
/// subject template column, plus the parent columns of every foreign key
/// pointing at the map (a repeated parent key multiplies a join).
fn duplicate_probe_keys(mapping: &CompiledR2rmlMapping) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut maps: Vec<&fluree_db_r2rml::mapping::TriplesMap> =
        mapping.triples_maps.values().collect();
    maps.sort_by(|a, b| a.iri.cmp(&b.iri));
    for tm in &maps {
        let Some(table) = tm.table_name() else {
            continue;
        };
        let mut keys: Vec<String> = tm.subject_columns().into_iter().map(String::from).collect();
        for child in mapping.find_maps_referencing(&tm.iri) {
            for pom in &child.predicate_object_maps {
                if let fluree_db_r2rml::mapping::ObjectMap::RefObjectMap(rom) = &pom.object_map {
                    if rom.parent_triples_map == tm.iri {
                        for c in rom.parent_columns() {
                            if !keys.iter().any(|k| k == c) {
                                keys.push(c.to_string());
                            }
                        }
                    }
                }
            }
        }
        if keys.is_empty() {
            continue;
        }
        match out.iter_mut().find(|(t, _)| t == table) {
            Some((_, existing)) => {
                for k in keys {
                    if !existing.contains(&k) {
                        existing.push(k);
                    }
                }
            }
            None => out.push((table.to_string(), keys)),
        }
    }
    out
}

/// Probe every table's subject keys for duplicates. Returns the flagged
/// tables and human-readable warnings (a duplicate, or a probe that could not
/// run — the latter never blocks registration).
async fn probe_duplicate_subjects(
    client: &TrinoClient,
    mapping: &CompiledR2rmlMapping,
) -> (Vec<String>, Vec<String>) {
    let mut flagged = Vec::new();
    let mut warnings = Vec::new();
    for (table, keys) in duplicate_probe_keys(mapping) {
        let source = match mapping.sql_query_for_table(&table) {
            Some(sql) => LogicalSource::Query(sql.to_string()),
            None => LogicalSource::Table(table.clone()),
        };
        let sql = fluree_db_sql::render_duplicate_probe(&source, &keys, client.dialect());
        match client.execute_collect(&sql).await {
            Ok((_, batches)) => {
                if batches.iter().any(|b| b.num_rows > 0) {
                    warnings.push(format!(
                        "table '{table}': subject key ({}) is not unique; a star over a repeated                          subject returns duplicate rows and the pushdown lane refuses statements                          over it (register with allow_duplicate_subjects to proceed anyway)",
                        keys.join(", ")
                    ));
                    flagged.push(table);
                }
            }
            Err(e) => warnings.push(format!(
                "table '{table}': could not probe subject key uniqueness: {e}"
            )),
        }
    }
    (flagged, warnings)
}

impl crate::Fluree {
    /// Register a SQL graph source. Compiles the mapping, stores it in CAS,
    /// probes the endpoint, and publishes the record.
    pub async fn create_sql_graph_source(
        &self,
        config: SqlCreateConfig,
    ) -> crate::Result<SqlCreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(graph_source_id = %graph_source_id, "Creating SQL graph source");
        config.validate()?;
        let model_warnings = self.validate_source_model(config.model.as_deref()).await?;

        let mut compiled_for_probe: Option<CompiledR2rmlMapping> = None;
        let (mapping_address, triples_map_count, table_names, mapping_validated) = match &config
            .mapping
        {
            R2rmlMappingInput::Content(content) => {
                let compiled =
                    Self::compile_r2rml_content(content, config.mapping_media_type.as_deref(), "")?;
                let count = compiled.len();
                let tables = Self::sorted_table_names(&compiled);
                compiled_for_probe = Some(compiled);
                let cid = self
                    .content_store(&graph_source_id)
                    .put(
                        fluree_db_core::ContentKind::GraphSourceMapping,
                        content.as_bytes(),
                    )
                    .await
                    .map_err(|e| {
                        crate::ApiError::Config(format!("Failed to store R2RML mapping: {e}"))
                    })?;
                (cid.to_string(), count, tables, true)
            }
            R2rmlMappingInput::Address(address) => {
                let storage = self.admin_storage().ok_or_else(|| {
                    crate::ApiError::Config(
                        "address-based mappings are not supported on this backend".to_string(),
                    )
                })?;
                let (count, tables, validated) = match storage.read_bytes(address).await {
                    Ok(bytes) => match String::from_utf8(bytes)
                        .map_err(|e| crate::ApiError::Config(e.to_string()))
                        .and_then(|content| {
                            Self::compile_r2rml_content(
                                &content,
                                config.mapping_media_type.as_deref(),
                                address,
                            )
                        }) {
                        Ok(compiled) => {
                            let summary =
                                (compiled.len(), Self::sorted_table_names(&compiled), true);
                            compiled_for_probe = Some(compiled);
                            summary
                        }
                        Err(e) => {
                            warn!(graph_source_id = %graph_source_id, error = %e, "Could not validate R2RML mapping from address");
                            (0, Vec::new(), false)
                        }
                    },
                    Err(e) => {
                        warn!(graph_source_id = %graph_source_id, error = %e, "Could not read R2RML mapping from address");
                        (0, Vec::new(), false)
                    }
                };
                (address.clone(), count, tables, validated)
            }
        };

        let mut gs_config = config.to_gs_config(&mapping_address);
        let mut mapping_warnings = Vec::new();
        let connection_tested = match build_sql_client(&gs_config, self.secret_resolver()).await {
            Ok(client) => match client.execute_collect("SELECT 1").await {
                Ok(_) => {
                    if let Some(mapping) = &compiled_for_probe {
                        let (flagged, warnings) = probe_duplicate_subjects(&client, mapping).await;
                        gs_config.duplicate_subject_tables = flagged;
                        mapping_warnings = warnings;
                    }
                    true
                }
                Err(e) => {
                    warn!(graph_source_id = %graph_source_id, error = %e, "SQL endpoint connection test failed; registering anyway");
                    false
                }
            },
            Err(e) => {
                warn!(graph_source_id = %graph_source_id, error = %e, "SQL endpoint connection test failed; registering anyway");
                false
            }
        };
        if !connection_tested {
            mapping_warnings.push(
                "subject key uniqueness was not probed (endpoint unreachable); run `fluree sql check`                  once it is"
                    .to_string(),
            );
        }

        let config_json = gs_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {e}")))?;
        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Sql,
                &config_json,
                &[],
            )
            .await?;

        info!(graph_source_id = %graph_source_id, mapping_address = %mapping_address, "Created SQL graph source");
        Ok(SqlCreateResult {
            model_warnings,
            mapping_warnings,
            graph_source_id,
            endpoint: gs_config.endpoint,
            mapping_source: mapping_address,
            triples_map_count,
            table_count: table_names.len(),
            table_names,
            connection_tested,
            mapping_validated,
        })
    }

    /// Re-run the subject key uniqueness probe for a registered SQL source
    /// (tables are live) and store the result on its record.
    pub async fn check_sql_graph_source(
        &self,
        graph_source_id: &str,
    ) -> crate::Result<SqlCheckResult> {
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| crate::ApiError::Config(format!("nameservice: {e}")))?
            .ok_or_else(|| {
                crate::ApiError::Config(format!("graph source '{graph_source_id}' not found"))
            })?;
        if record.source_type != GraphSourceType::Sql {
            return Err(crate::ApiError::Config(format!(
                "graph source '{graph_source_id}' is not a SQL source"
            )));
        }
        let mut config = SqlGsConfig::from_json(&record.config)
            .map_err(|e| crate::ApiError::Config(e.to_string()))?;
        let mapping_ref = config.mapping.clone().ok_or_else(|| {
            crate::ApiError::Config(format!("graph source '{graph_source_id}' has no mapping"))
        })?;
        let bytes = if let Ok(cid) = mapping_ref.source.parse::<fluree_db_core::ContentId>() {
            self.content_store(graph_source_id)
                .get(&cid)
                .await
                .map_err(|e| crate::ApiError::Config(format!("read mapping: {e}")))?
        } else {
            let storage = self.admin_storage().ok_or_else(|| {
                crate::ApiError::Config(
                    "address-based mappings are not supported on this backend".to_string(),
                )
            })?;
            storage
                .read_bytes(&mapping_ref.source)
                .await
                .map_err(|e| crate::ApiError::Config(format!("read mapping: {e}")))?
        };
        let content =
            String::from_utf8(bytes).map_err(|e| crate::ApiError::Config(e.to_string()))?;
        let compiled = Self::compile_r2rml_content(
            &content,
            mapping_ref.media_type.as_deref(),
            &mapping_ref.source,
        )?;
        let client = build_sql_client(&config, self.secret_resolver())
            .await
            .map_err(|e| crate::ApiError::Config(e.to_string()))?;
        let (flagged, mapping_warnings) = probe_duplicate_subjects(&client, &compiled).await;
        config.duplicate_subject_tables = flagged.clone();
        let (name, branch) = graph_source_id
            .rsplit_once(':')
            .unwrap_or((graph_source_id, "main"));
        self.publisher()?
            .publish_graph_source(
                name,
                branch,
                GraphSourceType::Sql,
                &config
                    .to_json()
                    .map_err(|e| crate::ApiError::Config(e.to_string()))?,
                &[],
            )
            .await?;
        Ok(SqlCheckResult {
            graph_source_id: graph_source_id.to_string(),
            duplicate_subject_tables: flagged,
            allow_duplicate_subjects: config.allow_duplicate_subjects,
            mapping_warnings,
        })
    }

    /// `SELECT 1` against the endpoint with the configured credentials.
    pub async fn test_sql_connection(&self, config: &SqlGsConfig) -> crate::Result<()> {
        let client = build_sql_client(config, self.secret_resolver())
            .await
            .map_err(|e| crate::ApiError::Config(e.to_string()))?;
        client
            .execute_collect("SELECT 1")
            .await
            .map(|_| ())
            .map_err(|e| {
                crate::ApiError::Config(format!("SQL endpoint connection test failed: {e}"))
            })
    }
}

/// Hydrate secrets, build the auth provider, and construct the client.
async fn build_sql_client(
    config: &SqlGsConfig,
    resolver: Option<&Arc<dyn fluree_db_sql::SecretResolver>>,
) -> Result<TrinoClient, SqlError> {
    let hydrated = config.hydrate(resolver).await?;
    let auth = hydrated.auth.create_provider_arc()?;
    TrinoClient::new(&hydrated, auth)
}

/// One SQL source resolved from its nameservice record.
pub(crate) struct SqlSource {
    pub(crate) graph_source_id: String,
    pub(crate) config: SqlGsConfig,
    pub(crate) client: Arc<TrinoClient>,
}

impl SqlSource {
    /// Resolve the record's config and the (process-cached) client. The cache
    /// key is a fingerprint of the RAW config so a secret rotation behind an
    /// env var / secret ref does not rebuild the client every query.
    pub(crate) async fn open(
        fluree: &crate::Fluree,
        record: &GraphSourceRecord,
    ) -> QueryResult<Self> {
        let config = SqlGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse SQL graph source config for '{}': {e}",
                record.graph_source_id
            ))
        })?;
        let cache = fluree.r2rml_cache();
        let key = super::r2rml::rest_client_cache_key(&record.graph_source_id, &record.config);
        let client = match cache.sql_client(&key) {
            Some(c) => c,
            None => {
                let c = Arc::new(
                    build_sql_client(&config, fluree.secret_resolver())
                        .await
                        .map_err(|e| {
                            QueryError::Internal(format!(
                                "SQL graph source '{}': {e}",
                                record.graph_source_id
                            ))
                        })?,
                );
                cache.put_sql_client(key, Arc::clone(&c));
                c
            }
        };
        Ok(Self {
            graph_source_id: record.graph_source_id.clone(),
            config,
            client,
        })
    }

    /// A table name, or the `rr:sqlQuery` text behind a query alias.
    fn source(&self, mapping: &CompiledR2rmlMapping, table_name: &str) -> LogicalSource {
        match mapping.sql_query_for_table(table_name) {
            Some(sql) => LogicalSource::Query(sql.to_string()),
            None => LogicalSource::Table(table_name.to_string()),
        }
    }

    /// Stamp this table into the build watermark on first touch.
    fn record_watermark(&self, session: &IcebergCatalogSession, table_name: &str) {
        session.mark_sql_source(&self.graph_source_id);
        session.record_snapshot(
            IcebergCatalogSession::snapshot_key(&self.graph_source_id, table_name),
            TableWatermark {
                metadata_location: format!(
                    "sql://{}/{}@{}",
                    self.config
                        .endpoint_base()
                        .trim_start_matches("https://")
                        .trim_start_matches("http://"),
                    table_name,
                    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                ),
                snapshot_id: None,
                sequence_number: None,
            },
        );
    }

    pub(crate) async fn scan(
        &self,
        session: &IcebergCatalogSession,
        mapping: &CompiledR2rmlMapping,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
    ) -> QueryResult<ColumnBatchStream> {
        let source = self.source(mapping, table_name);
        let schema = self
            .client
            .schema(&source)
            .await
            .map_err(|e| sql_query_error(&self.graph_source_id, table_name, e))?;
        self.record_watermark(session, table_name);

        let request = ScanRequest {
            source,
            projection: projection.to_vec(),
            predicates: filters.iter().map(to_predicate).collect(),
        };
        let rendered =
            fluree_db_sql::dialect::render_scan(&request, &schema, self.client.dialect())
                .map_err(|e| sql_query_error(&self.graph_source_id, table_name, e))?;
        if !rendered.declined_predicates.is_empty() {
            debug!(
                graph_source_id = %self.graph_source_id,
                table_name,
                declined = ?rendered.declined_predicates,
                "SQL pushdown declined some predicates (in-engine FILTER enforces them)"
            );
        }
        info!(
            graph_source_id = %self.graph_source_id,
            table_name,
            sql = %rendered.sql,
            "SQL table scan"
        );

        let gs = self.graph_source_id.clone();
        let table = table_name.to_string();
        let stream = self
            .client
            .execute(rendered.sql)
            .map(move |item| item.map_err(|e| sql_query_error(&gs, &table, e)));
        Ok(Box::pin(stream))
    }

    /// What the pushdown lane may send this source.
    pub(crate) fn pushdown_capabilities(&self) -> PushdownCapabilities {
        fluree_db_sql::pushdown_capabilities(self.client.dialect())
    }

    /// One statement for a whole block. Every table access is probed for its
    /// schema (cached per client) so literals render typed; a statement over
    /// the size budget is refused before it is sent.
    pub(crate) async fn execute_plan(
        &self,
        session: &IcebergCatalogSession,
        mapping: &CompiledR2rmlMapping,
        plan: &RelPlan,
    ) -> QueryResult<(String, ColumnBatchStream)> {
        if !self.config.allow_duplicate_subjects {
            let touched: Vec<&str> = plan
                .root
                .accesses()
                .iter()
                .filter_map(|(_, src)| match src {
                    RelSource::Table(t) => self
                        .config
                        .duplicate_subject_tables
                        .iter()
                        .any(|d| d == t)
                        .then_some(t.as_str()),
                    RelSource::Query(_) => None,
                })
                .collect();
            if !touched.is_empty() {
                return Err(QueryError::InvalidQuery(format!(
                    "SQL graph source '{}': table(s) {} have non-unique subject keys, so a                      statement over them would return wrong row multiplicities; fix the mapping                      (map from a view with distinct keys), or re-register with                      allow_duplicate_subjects to accept duplicate rows",
                    self.graph_source_id,
                    touched.join(", ")
                )));
            }
        }
        let mut schemas: HashMap<String, Arc<BatchSchema>> = HashMap::new();
        for (alias, source) in plan.root.accesses() {
            let (logical, table_name) = match source {
                RelSource::Table(t) => (LogicalSource::Table(t.clone()), t.clone()),
                RelSource::Query(q) => {
                    let alias_name = mapping
                        .triples_maps
                        .values()
                        .find(|tm| tm.sql_query() == Some(q.as_str()))
                        .and_then(|tm| tm.table_name())
                        .unwrap_or("rr:sqlQuery")
                        .to_string();
                    (LogicalSource::Query(q.clone()), alias_name)
                }
            };
            let schema = self
                .client
                .schema(&logical)
                .await
                .map_err(|e| sql_query_error(&self.graph_source_id, &table_name, e))?;
            self.record_watermark(session, &table_name);
            schemas.insert(alias.to_string(), schema);
        }
        let caps = self.pushdown_capabilities();
        let sql = fluree_db_sql::render_plan(plan, &schemas, self.client.dialect())
            .map_err(|e| sql_query_error(&self.graph_source_id, "<plan>", e))?;
        if sql.len() > caps.statement_max_bytes {
            return Err(QueryError::Internal(format!(
                "SQL graph source '{}': pushed-down statement is {} bytes, over the {} byte budget",
                self.graph_source_id,
                sql.len(),
                caps.statement_max_bytes
            )));
        }
        info!(
            graph_source_id = %self.graph_source_id,
            sql = %sql,
            "SQL block pushdown"
        );
        let gs = self.graph_source_id.clone();
        let stream = self
            .client
            .execute(sql.clone())
            .map(move |item| item.map_err(|e| sql_query_error(&gs, "<plan>", e)));
        Ok((sql, Box::pin(stream)))
    }

    pub(crate) async fn row_count(
        &self,
        session: &IcebergCatalogSession,
        mapping: &CompiledR2rmlMapping,
        table_name: &str,
        non_null_cols: &[String],
    ) -> QueryResult<Option<u64>> {
        let source = self.source(mapping, table_name);
        self.record_watermark(session, table_name);
        let n = self
            .client
            .count(&source, non_null_cols)
            .await
            .map_err(|e| sql_query_error(&self.graph_source_id, table_name, e))?;
        Ok(Some(n))
    }
}

fn sql_query_error(graph_source_id: &str, table_name: &str, e: SqlError) -> QueryError {
    let msg = format!("SQL graph source '{graph_source_id}', table '{table_name}': {e}");
    match e {
        SqlError::Config(_) | SqlError::Unsupported(_) => QueryError::InvalidQuery(msg),
        _ => QueryError::Internal(msg),
    }
}

fn to_predicate(f: &ScanFilter) -> Predicate {
    use fluree_db_query::r2rml::ScanCmpOp;
    Predicate {
        column: f.column.clone(),
        op: match f.op {
            ScanCmpOp::Eq => CmpOp::Eq,
            ScanCmpOp::NotEq => CmpOp::NotEq,
            ScanCmpOp::Lt => CmpOp::Lt,
            ScanCmpOp::LtEq => CmpOp::LtEq,
            ScanCmpOp::Gt => CmpOp::Gt,
            ScanCmpOp::GtEq => CmpOp::GtEq,
            ScanCmpOp::In => CmpOp::In,
        },
        value: to_literal(&f.value),
    }
}

fn to_literal(v: &ScanValue) -> Literal {
    match v {
        ScanValue::Bool(b) => Literal::Bool(*b),
        ScanValue::Int(i) => Literal::Int(*i),
        ScanValue::Date(d) => Literal::Date(*d),
        ScanValue::Str(s) => Literal::Str(s.clone()),
        ScanValue::Double(d) => Literal::Double(*d),
        ScanValue::Decimal {
            unscaled, scale, ..
        } => Literal::Decimal {
            unscaled: *unscaled,
            scale: *scale,
        },
        ScanValue::TemplateKey(k) => Literal::TemplateKey(k.clone()),
        ScanValue::Set(members) => Literal::Set(members.iter().map(to_literal).collect()),
        ScanValue::Timestamp { micros, tz } => Literal::Timestamp {
            micros: *micros,
            tz: *tz,
        },
    }
}

/// The mapping reference of a SQL record, if it has one.
pub(crate) fn mapping_source(record: &GraphSourceRecord) -> Option<MappingSource> {
    SqlGsConfig::from_json(&record.config)
        .ok()
        .and_then(|c| c.mapping)
}

pub(crate) fn policy_config(record: &GraphSourceRecord) -> (Option<String>, Option<bool>) {
    SqlGsConfig::from_json(&record.config)
        .ok()
        .map_or((None, None), |c| (c.model, c.default_allow))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_config_round_trips_into_gs_config() {
        let mut c = SqlCreateConfig::new("wh", "http://localhost:8080/", "@prefix rr: <x> .");
        c.catalog = Some("pg".into());
        c.user = Some("svc".into());
        assert_eq!(c.graph_source_id(), "wh:main");
        let gs = c.to_gs_config("bafy123");
        assert_eq!(gs.endpoint_base(), "http://localhost:8080");
        assert_eq!(gs.catalog.as_deref(), Some("pg"));
        assert_eq!(gs.user, "svc");
        let m = gs.mapping.unwrap();
        assert_eq!(m.source, "bafy123");
        assert_eq!(m.media_type.as_deref(), Some("text/turtle"));
        c.validate().unwrap();
        c.name = "a:b".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn scan_filters_convert() {
        let f = ScanFilter {
            column: "id".into(),
            op: fluree_db_query::r2rml::ScanCmpOp::In,
            value: ScanValue::Set(vec![ScanValue::Int(1), ScanValue::TemplateKey("2".into())]),
        };
        let p = to_predicate(&f);
        assert_eq!(p.op, CmpOp::In);
        assert_eq!(
            p.value,
            Literal::Set(vec![Literal::Int(1), Literal::TemplateKey("2".into())])
        );
    }
}
