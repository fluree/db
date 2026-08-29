//! SQL graph sources: an R2RML mapping over tables reached through a
//! Trino-protocol HTTP endpoint.
//!
//! Registration mirrors the Iceberg/R2RML path (mapping compiled and stored in
//! CAS, record published under `f:SqlMapping`), and scans are served through
//! the same [`super::FlureeR2rmlProvider`], which dispatches here when the
//! record's type is `Sql`. A SQL source has no snapshot to pin, so its build
//! watermark records the endpoint, table and first-touch time.

use std::collections::BTreeMap;
use std::sync::Arc;

use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{ColumnBatchStream, ScanFilter, ScanValue, TableWatermark};
use fluree_db_sql::{
    AuthConfig, CmpOp, Literal, LogicalSource, MappingSource, Predicate, ScanRequest, SqlDialect,
    SqlError, SqlGsConfig, TrinoClient, WireProtocol,
};
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

        let (mapping_address, triples_map_count, table_names, mapping_validated) = match &config
            .mapping
        {
            R2rmlMappingInput::Content(content) => {
                let compiled =
                    Self::compile_r2rml_content(content, config.mapping_media_type.as_deref(), "")?;
                let count = compiled.len();
                let tables = Self::sorted_table_names(&compiled);
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
                        Ok(compiled) => (compiled.len(), Self::sorted_table_names(&compiled), true),
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

        let gs_config = config.to_gs_config(&mapping_address);
        let connection_tested = match self.test_sql_connection(&gs_config).await {
            Ok(()) => true,
            Err(e) => {
                warn!(graph_source_id = %graph_source_id, error = %e, "SQL endpoint connection test failed; registering anyway");
                false
            }
        };

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

    fn source(&self, table_name: &str) -> LogicalSource {
        LogicalSource::Table(table_name.to_string())
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
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
    ) -> QueryResult<ColumnBatchStream> {
        let source = self.source(table_name);
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

    pub(crate) async fn row_count(
        &self,
        session: &IcebergCatalogSession,
        table_name: &str,
        non_null_cols: &[String],
    ) -> QueryResult<Option<u64>> {
        let source = self.source(table_name);
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
