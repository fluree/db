//! Delta Lake graph sources: R2RML mappings over Delta tables.
//!
//! Registration stores the mapping and publishes a `GraphSourceType::Delta`
//! record; at query time [`DeltaSource`] serves the R2RML provider's scans. A
//! table is resolved to one version the first time a query touches it and
//! every later scan or count in that query reads the same version.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use fluree_db_delta::{
    ColumnFilter, DeltaError, DeltaGsConfig, DeltaIoConfig, DeltaSnapshot, DeltaTable, FilterOp,
    FilterValue, Placement, UnityConfig,
};
use fluree_db_iceberg::config::MappingSource;
use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{
    ColumnBatchStream, ScanCmpOp, ScanFilter, ScanValue, SourceTime, TableWatermark,
};
use futures::StreamExt;
use tracing::{info, warn};

use super::catalog_session::IcebergCatalogSession;
use super::config::R2rmlMappingInput;

/// Registration request for a Delta graph source.
#[derive(Debug, Clone)]
pub struct DeltaCreateConfig {
    pub name: String,
    pub branch: Option<String>,
    /// Directory the mapping's `rr:tableName`s resolve beneath.
    pub root: Option<String>,
    /// Explicit table name → table location.
    pub tables: BTreeMap<String, String>,
    /// Unity Catalog, through which every table without a `tables` entry is
    /// found and read. Excludes `root`.
    pub unity: Option<UnityConfig>,
    pub io: DeltaIoConfig,
    pub mapping: R2rmlMappingInput,
    pub mapping_media_type: Option<String>,
    /// Model ledger supplying policy and schema.
    pub model: Option<String>,
    pub default_allow: Option<bool>,
}

/// The flat Azure service-principal fields the CLI and HTTP surfaces accept.
/// `client_secret_env` names an environment variable read where the tables are
/// read, so the secret itself is never stored; `client_secret` is a literal.
#[derive(Debug, Clone, Default)]
pub struct DeltaAzureFields {
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub client_secret_env: Option<String>,
}

impl DeltaAzureFields {
    /// `None` when no field is set (ambient credentials). A partial set is an
    /// error: silently falling back to ambient credentials would read with an
    /// identity the caller did not name.
    pub fn into_auth(self) -> crate::Result<Option<fluree_db_delta::AzureAuth>> {
        use fluree_db_iceberg::ConfigValue;
        let Self {
            tenant_id,
            client_id,
            client_secret,
            client_secret_env,
        } = self;
        if tenant_id.is_none()
            && client_id.is_none()
            && client_secret.is_none()
            && client_secret_env.is_none()
        {
            return Ok(None);
        }
        let client_secret = match (client_secret, client_secret_env) {
            (Some(literal), None) => ConfigValue::literal(literal),
            (None, Some(var)) => ConfigValue::from_env(var),
            _ => {
                return Err(crate::ApiError::Config(
                    "Azure service principal: give exactly one of the client secret and the \
                     environment variable holding it"
                        .to_string(),
                ))
            }
        };
        match (tenant_id, client_id) {
            (Some(tenant_id), Some(client_id)) => {
                Ok(Some(fluree_db_delta::AzureAuth::ClientSecret {
                    tenant_id,
                    client_id,
                    client_secret,
                }))
            }
            _ => Err(crate::ApiError::Config(
                "Azure service principal: tenant id, client id and client secret are all required"
                    .to_string(),
            )),
        }
    }
}

impl DeltaCreateConfig {
    /// A source whose tables live under `root`.
    pub fn new(
        name: impl Into<String>,
        root: impl Into<String>,
        mapping_content: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            root: Some(root.into()),
            tables: BTreeMap::new(),
            unity: None,
            io: DeltaIoConfig::default(),
            mapping: R2rmlMappingInput::Content(mapping_content.into()),
            mapping_media_type: None,
            model: None,
            default_allow: None,
        }
    }

    /// A source whose tables are named in Unity Catalog.
    pub fn in_unity(
        name: impl Into<String>,
        unity: UnityConfig,
        mapping_content: impl Into<String>,
    ) -> Self {
        let mut config = Self::new(name, "", mapping_content);
        config.root = None;
        config.unity = Some(unity);
        config
    }

    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or("main")
    }

    pub fn graph_source_id(&self) -> String {
        format!("{}:{}", self.name, self.effective_branch())
    }

    /// The persisted config, with the mapping's stored address filled in.
    pub fn to_gs_config(&self, mapping_address: &str) -> DeltaGsConfig {
        let media_type = self.mapping_media_type.clone().unwrap_or_else(|| {
            fluree_db_r2rml::loader::MappingFormat::resolve(None, mapping_address)
                .media_type()
                .to_string()
        });
        DeltaGsConfig {
            root: self.root.clone(),
            tables: self.tables.clone(),
            unity: self.unity.clone(),
            io: self.io.clone(),
            mapping: Some(MappingSource {
                source: mapping_address.to_string(),
                media_type: Some(media_type),
            }),
            model: self.model.clone(),
            default_allow: self.default_allow,
        }
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

/// What `create_delta_graph_source` reports back.
#[derive(Debug, Clone)]
pub struct DeltaCreateResult {
    pub graph_source_id: String,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_names: Vec<String>,
    pub mapping_validated: bool,
    /// Mapped tables that opened at registration, with their current version.
    pub table_versions: BTreeMap<String, u64>,
    /// Mapped tables that could not be read at registration, and why. The
    /// source is registered regardless: a table may not exist yet, or the
    /// registering process may lack the credentials the query process has.
    pub table_warnings: Vec<String>,
    pub model_warnings: Vec<String>,
}

impl crate::Fluree {
    /// Register a Delta graph source: compile and store the mapping, probe each
    /// mapped table, and publish the record.
    pub async fn create_delta_graph_source(
        &self,
        config: DeltaCreateConfig,
    ) -> crate::Result<DeltaCreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(graph_source_id = %graph_source_id, "Creating Delta graph source");
        config.validate()?;
        let model_warnings = self.validate_source_model(config.model.as_deref()).await?;

        let registered = self
            .register_r2rml_mapping(
                &graph_source_id,
                &config.mapping,
                config.mapping_media_type.as_deref(),
                false,
            )
            .await?;
        let (triples_map_count, table_names, mapping_validated) = registered.summary();
        let gs_config = config.to_gs_config(&registered.address);

        // An unresolvable secret is a registration error, not a table warning.
        let io = gs_config
            .io
            .hydrate(self.secret_resolver())
            .await
            .map_err(|e| crate::ApiError::Config(e.to_string()))?;
        let unity = match &gs_config.unity {
            Some(unity) => Some(
                unity
                    .hydrate(self.secret_resolver())
                    .await
                    .map_err(|e| crate::ApiError::Config(e.to_string()))?,
            ),
            None => None,
        };
        let mut table_versions = BTreeMap::new();
        let mut table_warnings = Vec::new();
        for table_name in &table_names {
            // Exactly the columns a scan of this table will project.
            let mut columns: Vec<String> = registered
                .compiled
                .iter()
                .flat_map(|m| m.find_maps_for_table(table_name))
                .flat_map(|tm| tm.referenced_columns())
                .map(str::to_string)
                .collect();
            columns.sort();
            columns.dedup();
            let probed = async {
                let table = match (gs_config.placement(table_name)?, &unity) {
                    (Placement::Unity(full_name), Some(unity)) => {
                        DeltaTable::open_in_unity(table_name, unity, &full_name, &io).await?
                    }
                    (Placement::Path(location), _) => DeltaTable::open(table_name, &location, &io)?,
                    (Placement::Unity(full_name), None) => {
                        return Err(DeltaError::Config(format!(
                            "table '{full_name}' is placed in a catalog the source does not have"
                        )))
                    }
                };
                let snapshot = table
                    .snapshot(fluree_db_delta::VersionSelector::Latest)
                    .await?;
                // Planning surfaces a mapped column the table lacks, or one of
                // a type the batch model cannot carry, now rather than at the
                // first query.
                snapshot.batch_schema(&columns)?;
                Ok::<_, DeltaError>(snapshot.version())
            }
            .await;
            match probed {
                Ok(version) => {
                    table_versions.insert(table_name.clone(), version);
                }
                // A name the config cannot place is a registration error, not a
                // table that might appear later.
                Err(DeltaError::Config(e)) => return Err(crate::ApiError::Config(e)),
                Err(e) => {
                    warn!(graph_source_id = %graph_source_id, table = %table_name, error = %e, "Delta table probe failed; registering anyway");
                    table_warnings.push(format!("table '{table_name}': {e}"));
                }
            }
        }

        let config_json = gs_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(e.to_string()))?;
        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Delta,
                &config_json,
                &[],
            )
            .await?;

        info!(graph_source_id = %graph_source_id, mapping_address = %registered.address, "Created Delta graph source");
        Ok(DeltaCreateResult {
            graph_source_id,
            mapping_source: registered.address,
            triples_map_count,
            table_names,
            mapping_validated,
            table_versions,
            table_warnings,
            model_warnings,
        })
    }
}

/// One Delta source for the life of one query.
pub(crate) struct DeltaSource {
    graph_source_id: String,
    config: DeltaGsConfig,
    /// Table name → the version this query reads. First resolution wins, so a
    /// commit landing mid-query cannot split a query across two versions. One
    /// cell per table: tables resolve concurrently, and concurrent readers of
    /// one table share a single log replay.
    snapshots: std::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<DeltaSnapshot>>>>,
}

impl DeltaSource {
    pub(crate) fn open(record: &GraphSourceRecord) -> QueryResult<Self> {
        let config = DeltaGsConfig::from_json(&record.config)
            .and_then(|c| c.validate().map(|()| c))
            .map_err(|e| {
                QueryError::Internal(format!(
                    "Delta graph source '{}': {e}",
                    record.graph_source_id
                ))
            })?;
        Ok(Self {
            graph_source_id: record.graph_source_id.clone(),
            config,
            snapshots: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// The version of `table_name` this query reads: `pin` resolved against the
    /// table's log, else its latest version.
    async fn snapshot(
        &self,
        fluree: &crate::Fluree,
        session: &IcebergCatalogSession,
        table_name: &str,
        pin: Option<SourceTime>,
    ) -> QueryResult<DeltaSnapshot> {
        if fluree_db_r2rml::mapping::LogicalTable::is_sql_query_alias(table_name) {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}': rr:sqlQuery logical tables are only supported by SQL \
                 graph sources",
                self.graph_source_id
            )));
        }
        let cell = Arc::clone(
            self.snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(table_name.to_string())
                .or_default(),
        );
        cell.get_or_try_init(|| self.resolve(fluree, session, table_name, pin))
            .await
            .cloned()
    }

    /// Warm the versions of `table_names` concurrently, so a multi-table query
    /// does not replay its logs one after another. Failures are left for the
    /// scan that needs the table to report.
    pub(crate) async fn prefetch(
        &self,
        fluree: &crate::Fluree,
        session: &IcebergCatalogSession,
        table_names: &[String],
        pin: Option<SourceTime>,
    ) {
        futures::future::join_all(
            table_names
                .iter()
                .map(|table| self.snapshot(fluree, session, table, pin)),
        )
        .await;
    }

    async fn resolve(
        &self,
        fluree: &crate::Fluree,
        session: &IcebergCatalogSession,
        table_name: &str,
        pin: Option<SourceTime>,
    ) -> QueryResult<DeltaSnapshot> {
        let placement = self
            .config
            .placement(table_name)
            .map_err(|e| self.query_error(table_name, e))?;
        let table = fluree
            .r2rml_cache()
            .delta_table(
                table_name,
                &placement,
                &self.config,
                fluree.secret_resolver(),
            )
            .await
            .map_err(|e| self.query_error(table_name, e))?;
        let snapshot = table
            .snapshot(version_selector(table_name, pin)?)
            .await
            .map_err(|e| self.query_error(table_name, e))?;
        let located = match &placement {
            Placement::Path(location) => location.as_str(),
            Placement::Unity(_) => table.location(),
        };

        session.record_snapshot(
            IcebergCatalogSession::snapshot_key(&self.graph_source_id, table_name),
            TableWatermark {
                metadata_location: format!("{}@v{}", located, snapshot.version()),
                snapshot_id: i64::try_from(snapshot.version()).ok(),
                sequence_number: None,
            },
        );
        Ok(snapshot)
    }

    /// The pinned version's row count from the log, when it provably equals a
    /// scan's; see [`DeltaSnapshot::exact_row_count`].
    pub(crate) async fn row_count(
        &self,
        fluree: &crate::Fluree,
        session: &IcebergCatalogSession,
        table_name: &str,
        non_null_cols: &[String],
        pin: Option<SourceTime>,
    ) -> QueryResult<Option<u64>> {
        let snapshot = self.snapshot(fluree, session, table_name, pin).await?;
        snapshot
            .exact_row_count(non_null_cols)
            .await
            .map_err(|e| self.query_error(table_name, e))
    }

    pub(crate) async fn scan(
        &self,
        fluree: &crate::Fluree,
        session: &IcebergCatalogSession,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        pin: Option<SourceTime>,
    ) -> QueryResult<ColumnBatchStream> {
        let snapshot = self.snapshot(fluree, session, table_name, pin).await?;
        let filters: Vec<ColumnFilter> = filters.iter().filter_map(column_filter).collect();
        info!(
            graph_source_id = %self.graph_source_id,
            table_name = %table_name,
            version = snapshot.version(),
            projection = ?projection,
            filters = filters.len(),
            "Starting Delta table scan"
        );
        let stream = snapshot
            .scan(projection, &filters)
            .map_err(|e| self.query_error(table_name, e))?;
        let graph_source_id = self.graph_source_id.clone();
        let table = table_name.to_string();
        let version = snapshot.version();
        Ok(Box::pin(stream.map(move |batch| {
            batch.map_err(|e| {
                // The log replayed, so the version exists; its data does not.
                if e.is_missing_file() {
                    return QueryError::InvalidQuery(format!(
                        "Delta graph source '{graph_source_id}': version {version} of table \
                         '{table}' can no longer be read — a data file it references has been \
                         removed from storage (VACUUM or data retention)"
                    ));
                }
                delta_query_error(&graph_source_id, &table, e)
            })
        })))
    }

    fn query_error(&self, table_name: &str, error: DeltaError) -> QueryError {
        delta_query_error(&self.graph_source_id, table_name, error)
    }
}

/// A pushed filter in the reader's terms, or `None` for a value it has no
/// exact form for (decimals). The reader drops any it cannot state against the
/// column's physical type.
fn column_filter(filter: &ScanFilter) -> Option<ColumnFilter> {
    fn value(v: &ScanValue) -> Option<FilterValue> {
        Some(match v {
            ScanValue::Bool(b) => FilterValue::Bool(*b),
            ScanValue::Int(n) => FilterValue::Int(*n),
            ScanValue::Date(days) => FilterValue::Date(*days),
            ScanValue::Str(s) => FilterValue::Str(s.clone()),
            ScanValue::Double(d) => FilterValue::Double(*d),
            ScanValue::TemplateKey(raw) => FilterValue::Raw(raw.clone()),
            ScanValue::Timestamp { micros, tz } => FilterValue::Timestamp {
                micros: *micros,
                tz: *tz,
            },
            ScanValue::Set(members) => {
                FilterValue::Set(members.iter().map(value).collect::<Option<_>>()?)
            }
            ScanValue::Decimal { .. } => return None,
        })
    }
    Some(ColumnFilter {
        column: filter.column.clone(),
        op: match filter.op {
            ScanCmpOp::Eq => FilterOp::Eq,
            ScanCmpOp::NotEq => FilterOp::NotEq,
            ScanCmpOp::Lt => FilterOp::Lt,
            ScanCmpOp::LtEq => FilterOp::LtEq,
            ScanCmpOp::Gt => FilterOp::Gt,
            ScanCmpOp::GtEq => FilterOp::GtEq,
            ScanCmpOp::In => FilterOp::In,
        },
        value: value(&filter.value)?,
    })
}

/// A Delta version is the format's own state identifier, so `@snapshot:<n>`
/// names it directly.
fn version_selector(
    table_name: &str,
    pin: Option<SourceTime>,
) -> QueryResult<fluree_db_delta::VersionSelector> {
    use fluree_db_delta::VersionSelector;
    Ok(match pin {
        None => VersionSelector::Latest,
        Some(SourceTime::AsOfTimestampMs(ms)) => VersionSelector::AsOfTimestampMs(ms),
        Some(SourceTime::SnapshotId(id)) => {
            VersionSelector::Version(u64::try_from(id).map_err(|_| {
                QueryError::SnapshotNotFound {
                    table: table_name.to_string(),
                    snapshot_id: id,
                }
            })?)
        }
    })
}

fn delta_query_error(graph_source_id: &str, table_name: &str, error: DeltaError) -> QueryError {
    match error {
        DeltaError::VersionNotFound { version, .. } => QueryError::SnapshotNotFound {
            table: table_name.to_string(),
            snapshot_id: i64::try_from(version).unwrap_or(i64::MAX),
        },
        DeltaError::NoVersionAtTime {
            requested_ms,
            oldest_ms,
            ..
        } => QueryError::NoSnapshotAtTime {
            table: table_name.to_string(),
            requested: crate::time_resolve::epoch_ms_to_iso(requested_ms),
            oldest: oldest_ms.map(crate::time_resolve::epoch_ms_to_iso),
        },
        DeltaError::ColumnNotFound { .. } => {
            QueryError::InvalidQuery(format!("Delta graph source '{graph_source_id}': {error}"))
        }
        other => QueryError::Internal(format!(
            "Delta graph source '{graph_source_id}', table '{table_name}': {other}"
        )),
    }
}

/// The mapping reference of a Delta record, if it has one.
pub(crate) fn mapping_source(record: &GraphSourceRecord) -> Option<MappingSource> {
    DeltaGsConfig::from_json(&record.config)
        .ok()
        .and_then(|c| c.mapping)
}

pub(crate) fn policy_config(record: &GraphSourceRecord) -> (Option<String>, Option<bool>) {
    DeltaGsConfig::from_json(&record.config)
        .ok()
        .map_or((None, None), |c| (c.model, c.default_allow))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_delta::AzureAuth;

    fn fields(
        tenant: Option<&str>,
        client: Option<&str>,
        secret: Option<&str>,
        secret_env: Option<&str>,
    ) -> DeltaAzureFields {
        DeltaAzureFields {
            tenant_id: tenant.map(str::to_string),
            client_id: client.map(str::to_string),
            client_secret: secret.map(str::to_string),
            client_secret_env: secret_env.map(str::to_string),
        }
    }

    #[test]
    fn a_unity_source_stores_its_catalog_and_no_root() {
        let unity = UnityConfig {
            uri: "https://workspace.example.com".to_string(),
            auth: fluree_db_iceberg::auth::AuthConfig::Bearer {
                token: fluree_db_iceberg::ConfigValue::from_env("DATABRICKS_TOKEN"),
            },
            catalog: Some("main".to_string()),
            schema: None,
        };
        let mut config = DeltaCreateConfig::in_unity("sales", unity, "");
        config.validate().unwrap();

        let stored = config.to_gs_config("mapping.ttl");
        assert!(stored.root.is_none());
        assert_eq!(
            stored.placement("sales.orders").unwrap(),
            Placement::Unity("main.sales.orders".to_string())
        );
        let json = stored.to_json().unwrap();
        assert!(json.contains("\"env_var\":\"DATABRICKS_TOKEN\""), "{json}");

        config.root = Some("s3://lake/Tables".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn no_azure_fields_means_ambient_credentials() {
        assert!(fields(None, None, None, None)
            .into_auth()
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_partial_service_principal_is_an_error_not_ambient_credentials() {
        for partial in [
            fields(Some("t"), None, Some("s"), None),
            fields(None, Some("c"), Some("s"), None),
            fields(Some("t"), Some("c"), None, None),
            fields(Some("t"), Some("c"), Some("s"), Some("VAR")),
            fields(Some("t"), None, None, None),
        ] {
            assert!(partial.clone().into_auth().is_err(), "{partial:?}");
        }
    }

    #[test]
    fn a_secret_named_by_environment_variable_is_not_stored() {
        let auth = fields(Some("t"), Some("c"), None, Some("DELTA_TEST_AZURE_SECRET"))
            .into_auth()
            .unwrap()
            .expect("service principal");
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains("DELTA_TEST_AZURE_SECRET"), "{json}");
        let AzureAuth::ClientSecret { client_secret, .. } = auth;
        assert!(!client_secret.is_literal());
    }
}
