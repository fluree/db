//! Delta Lake graph sources: R2RML mappings over Delta tables.
//!
//! Registration stores the mapping and publishes a `GraphSourceType::Delta`
//! record; at query time [`DeltaSource`] serves the R2RML provider's scans. A
//! table is resolved to one version the first time a query touches it and
//! every later scan or count in that query reads the same version. A
//! materialize read resolves its own version on every call.

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
use super::r2rml::MaterializeScan;

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
    /// Materialization: rows a column marks as deleted; see
    /// [`DeltaGsConfig::delete`].
    pub delete: Option<fluree_db_iceberg::DeleteConvention>,
    /// Materialization: the column that orders a key's rows; see
    /// [`DeltaGsConfig::order_by`].
    pub order_by: Option<String>,
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
    /// identity the caller did not name. The one rule every surface uses
    /// ([`super::surface::azure_auth`]).
    pub fn into_auth(self) -> crate::Result<Option<fluree_db_delta::AzureAuth>> {
        let client_secret = super::surface::secret_field(
            "azure_client_secret",
            self.client_secret,
            "azure_client_secret_env",
            self.client_secret_env,
        )?;
        Ok(super::surface::azure_auth(
            self.tenant_id,
            self.client_id,
            client_secret,
        )?)
    }
}

/// The flat Unity Catalog fields the CLI and HTTP surfaces accept. A secret
/// arrives as the surface resolved it: a literal, or the name of an
/// environment variable read where the tables are read.
#[derive(Debug, Clone, Default)]
pub struct DeltaUnityFields {
    /// The Databricks workspace URL.
    pub uri: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub bearer: Option<fluree_db_iceberg::ConfigValue>,
    pub oauth2_client_id: Option<String>,
    pub oauth2_client_secret: Option<fluree_db_iceberg::ConfigValue>,
    /// Defaults to the workspace's own token endpoint.
    pub oauth2_token_url: Option<String>,
    /// Defaults to `all-apis`.
    pub oauth2_scope: Option<String>,
}

impl DeltaUnityFields {
    /// `None` when no field is set. Catalog fields without a workspace, or a
    /// workspace without exactly one way to authenticate, are errors: the
    /// catalog-auth rule every surface uses
    /// ([`super::surface::CatalogAuthFields`]).
    pub fn into_config(self) -> crate::Result<Option<UnityConfig>> {
        let auth = super::surface::CatalogAuthFields {
            bearer: self.bearer.map(|token| ("auth_bearer", token)),
            oauth2_token_url: self.oauth2_token_url,
            oauth2_client_id: self.oauth2_client_id,
            oauth2_client_secret: self
                .oauth2_client_secret
                .map(|secret| ("oauth2_client_secret", secret)),
            oauth2_scope: self.oauth2_scope,
            ..Default::default()
        };
        Ok(super::surface::unity_config(
            self.uri,
            self.catalog,
            self.schema,
            auth,
        )?)
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
            delete: None,
            order_by: None,
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

    /// Mark deleted rows by a column's values when materializing.
    pub fn with_delete_convention(
        mut self,
        convention: fluree_db_iceberg::DeleteConvention,
    ) -> Self {
        self.delete = Some(convention);
        self
    }

    /// Order one key's rows by `column` when materializing.
    pub fn with_order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by = Some(column.into());
        self
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
            delete: self.delete.clone(),
            order_by: self.order_by.clone(),
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
                let table = open_placed(
                    &gs_config,
                    unity.as_ref(),
                    &io,
                    table_name,
                    self.identity_context(),
                )
                .await?;
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

/// Open `table_name` where `config` places it. `unity` and `io` are the
/// config's own, hydrated; Unity's auth is built under `identity`.
pub(crate) async fn open_placed(
    config: &DeltaGsConfig,
    unity: Option<&UnityConfig>,
    io: &DeltaIoConfig,
    table_name: &str,
    identity: &fluree_db_iceberg::IdentityContext,
) -> Result<DeltaTable, DeltaError> {
    match (config.placement(table_name)?, unity) {
        (Placement::Unity(full_name), Some(unity)) => {
            DeltaTable::open_in_unity(table_name, unity, &full_name, io, identity).await
        }
        (Placement::Path(location), _) => DeltaTable::open(table_name, &location, io),
        (Placement::Unity(full_name), None) => Err(DeltaError::Config(format!(
            "table '{full_name}' is placed in a catalog the source does not have"
        ))),
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
            graph_source_id: record.graph_source_id.to_string(),
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
        let (placement, table, snapshot) = self.replay(fluree, table_name, pin).await?;
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

    /// `table_name` replayed to `pin`, outside the per-query memo that
    /// [`Self::snapshot`] keeps.
    async fn replay(
        &self,
        fluree: &crate::Fluree,
        table_name: &str,
        pin: Option<SourceTime>,
    ) -> QueryResult<(Placement, DeltaTable, DeltaSnapshot)> {
        if fluree_db_r2rml::mapping::LogicalTable::is_sql_query_alias(table_name) {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}': rr:sqlQuery logical tables are only supported by SQL \
                 graph sources",
                self.graph_source_id
            )));
        }
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
                fluree.identity_context(),
            )
            .await
            .map_err(|e| self.query_error(table_name, e))?;
        let snapshot = table
            .snapshot(version_selector(table_name, pin)?)
            .await
            .map_err(|e| self.query_error(table_name, e))?;
        Ok((placement, table, snapshot))
    }

    /// The latest version of `table_name`, as a materialize consumer's pin.
    pub(crate) async fn latest_version(
        &self,
        fluree: &crate::Fluree,
        table_name: &str,
    ) -> QueryResult<i64> {
        let (_, _, snapshot) = self.replay(fluree, table_name, None).await?;
        version_id(table_name, snapshot.version())
    }

    /// One materialize window of `table_name`, ending at version `to` (`None` =
    /// latest): the changes after `from` from the table's change data feed, or
    /// the `to` version whole when there is no `from` or the feed cannot serve
    /// the window. `row_budget` (`0` = none) cuts a change window at a commit
    /// boundary; see [`MaterializeScan::stopped_at`].
    ///
    /// Not through [`Self::snapshot`]: a materialize consumer reads one table at
    /// several versions through one provider, and that memo would answer every
    /// later pin with the first version resolved.
    pub(crate) async fn materialize_scan(
        &self,
        fluree: &crate::Fluree,
        table_name: &str,
        projection: &[String],
        from: Option<i64>,
        to: Option<i64>,
        row_budget: i64,
    ) -> QueryResult<MaterializeScan> {
        let pin = to.map(SourceTime::SnapshotId);
        let (_, _, snapshot) = self.replay(fluree, table_name, pin).await?;
        let head = snapshot.version();
        let to_snapshot_id = Some(version_id(table_name, head)?);
        let whole = |reason: Option<&str>| -> QueryResult<MaterializeScan> {
            if let Some(reason) = reason {
                warn!(
                    graph_source_id = %self.graph_source_id,
                    table = %table_name,
                    from_version = ?from,
                    to_version = head,
                    reason,
                    "materialize: reading the Delta table version whole. Deletes made since \
                     the watermark do not reach the target on a whole read; enable the \
                     table's change data feed (delta.enableChangeDataFeed) for them to"
                );
            }
            Ok(MaterializeScan {
                to_snapshot_id,
                incremental: false,
                window_age_ms: None,
                to_sequence: None,
                change_feed: false,
                stopped_at: None,
                stream: self.stream(&snapshot, table_name, projection, &[])?,
            })
        };
        let Some(from) = from else {
            return whole(None);
        };
        let Ok(after) = u64::try_from(from) else {
            return whole(Some("the watermark is not a Delta version"));
        };
        match after.cmp(&head) {
            std::cmp::Ordering::Equal => {
                return Ok(MaterializeScan {
                    to_snapshot_id,
                    incremental: true,
                    window_age_ms: Some(0),
                    to_sequence: None,
                    change_feed: true,
                    stopped_at: None,
                    stream: Box::pin(futures::stream::empty()),
                });
            }
            std::cmp::Ordering::Greater => {
                return whole(Some(
                    "the watermark is past the table's version: the table was replaced or \
                     restored",
                ));
            }
            std::cmp::Ordering::Less => {}
        }
        let budget = u64::try_from(row_budget).ok().filter(|rows| *rows > 0);
        match snapshot.changes_since(after, projection, budget).await {
            Ok(changes) => {
                info!(
                    graph_source_id = %self.graph_source_id,
                    table = %table_name,
                    from_version = after,
                    to_version = head,
                    row_budget = ?budget,
                    "materialize: reading the Delta change data feed"
                );
                Ok(MaterializeScan {
                    to_snapshot_id,
                    incremental: true,
                    // Every version the feed moves past is persisted, so the
                    // watermark never trails the log far enough to be cleaned.
                    window_age_ms: None,
                    to_sequence: None,
                    change_feed: true,
                    stopped_at: Some(changes.stopped_at),
                    stream: self.query_stream(changes.stream, table_name, head),
                })
            }
            Err(DeltaError::ChangeFeedUnavailable { reason, .. }) => whole(Some(&reason)),
            Err(e) => Err(self.query_error(table_name, e)),
        }
    }

    /// The rows `table_name` holds at `version`, narrowed by `filters` where
    /// the reader can state them exactly.
    pub(crate) async fn rows_at(
        &self,
        fluree: &crate::Fluree,
        table_name: &str,
        version: i64,
        filters: &[ScanFilter],
    ) -> QueryResult<ColumnBatchStream> {
        let pin = Some(SourceTime::SnapshotId(version));
        let (_, _, snapshot) = self.replay(fluree, table_name, pin).await?;
        self.stream(&snapshot, table_name, &[], filters)
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
        self.stream(&snapshot, table_name, projection, filters)
    }

    fn stream(
        &self,
        snapshot: &DeltaSnapshot,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
    ) -> QueryResult<ColumnBatchStream> {
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
        Ok(self.query_stream(stream, table_name, snapshot.version()))
    }

    /// A reader stream in query errors, naming `version` when a file it needs
    /// has been removed.
    fn query_stream(
        &self,
        stream: fluree_db_delta::DeltaBatchStream,
        table_name: &str,
        version: u64,
    ) -> ColumnBatchStream {
        let graph_source_id = self.graph_source_id.clone();
        let table = table_name.to_string();
        Box::pin(stream.map(move |batch| {
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
        }))
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

/// A Delta version as the snapshot id the materialize API carries.
fn version_id(table_name: &str, version: u64) -> QueryResult<i64> {
    i64::try_from(version).map_err(|_| {
        QueryError::Internal(format!(
            "Delta table '{table_name}': version {version} does not fit a snapshot id"
        ))
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
        DeltaError::Catalog {
            table,
            message,
            denied: true,
        } => QueryError::CatalogAccessDenied {
            table,
            // As for Iceberg: no AWS identity in a query client's error.
            message: crate::graph_source::iceberg_catalog::query_safe_denial(
                "Delta catalog denied access",
                message,
            ),
        },
        // The catalog identity's own refusals: the codes an Iceberg source
        // reports for them.
        DeltaError::Identity(refusal) => {
            let fallback =
                format!("Delta graph source '{graph_source_id}', table '{table_name}': {refusal}");
            crate::graph_source::iceberg_catalog::identity_query_error(refusal)
                .unwrap_or(QueryError::Internal(fallback))
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

    fn google_unity() -> UnityConfig {
        UnityConfig {
            uri: "https://workspace.invalid".to_string(),
            auth: fluree_db_iceberg::auth::AuthConfig::GoogleMetadata {
                scopes: None,
                metadata_url: None,
            },
            catalog: None,
            schema: None,
        }
    }

    fn denying() -> crate::Fluree {
        crate::FlureeBuilder::memory()
            .build_memory()
            .with_host_identity_policy(fluree_db_iceberg::HostIdentityPolicy::deny_all())
    }

    /// The catalog identity's refusal is typed on the query path (opening a
    /// Unity-placed table, the step a query's first touch takes) and through
    /// the onboarding mapping, with the codes an Iceberg source reports.
    #[tokio::test]
    async fn a_refused_unity_identity_is_typed_on_query_and_onboarding_paths() {
        let fluree = denying();
        let config = DeltaGsConfig {
            unity: Some(google_unity()),
            ..Default::default()
        };
        let opened = fluree
            .r2rml_cache()
            .delta_table(
                "orders",
                &Placement::Unity("main.sales.orders".to_string()),
                &config,
                fluree.identity_context(),
            )
            .await;
        let error = match opened {
            Ok(_) => panic!("the policy refuses the metadata server"),
            Err(error) => error,
        };
        assert!(matches!(error, DeltaError::Identity(_)), "{error:?}");
        let query = delta_query_error("gs:main", "orders", error);
        assert!(
            matches!(&query, QueryError::HostIdentityRefused { kind, .. } if kind == "google-metadata"),
            "{query:?}"
        );
        let api = crate::ApiError::Query(query);
        assert_eq!(api.status_code(), 403);
        assert_eq!(
            api.error_type(),
            fluree_vocab::errors::HOST_IDENTITY_REFUSED
        );

        // Onboarding validates a Unity config before building its auth (and
        // accepts only bearer / OAuth2 there today), so the mapping is checked
        // directly: a refusal reaching it keeps its type and code.
        let onboarding = super::super::delta_catalog::config_error(DeltaError::Identity(
            fluree_db_iceberg::IcebergError::HostIdentityRefused {
                kind: fluree_db_iceberg::HostIdentityKind::GoogleMetadata,
            },
        ));
        assert_eq!(onboarding.status_code(), 403, "{onboarding:?}");
        assert_eq!(
            onboarding.error_type(),
            fluree_vocab::errors::HOST_IDENTITY_REFUSED
        );
    }

    #[test]
    fn a_google_exchange_refusal_from_unity_keeps_its_code() {
        let query = delta_query_error(
            "gs:main",
            "orders",
            DeltaError::Identity(fluree_db_iceberg::IcebergError::GoogleTokenExchange {
                stage: fluree_db_iceberg::GoogleTokenStage::Jwt,
                status: Some(400),
                reason: Some("invalid_grant".to_string()),
            }),
        );
        let api = crate::ApiError::Query(query);
        assert_eq!(api.status_code(), 401, "{api:?}");
        assert_eq!(
            api.error_type(),
            fluree_vocab::errors::GOOGLE_TOKEN_EXCHANGE
        );
    }

    /// A catalog's 401/403 reaches the client as a typed 403, not as an
    /// invalid query; any other catalog failure keeps its old mapping.
    #[test]
    fn a_catalog_refusal_is_a_typed_403() {
        let refusal = |denied| DeltaError::Catalog {
            table: "main.sales.orders".to_string(),
            message: "User does not have SELECT on Table 'main.sales.orders'. (403 Forbidden)"
                .to_string(),
            denied,
        };
        let denied = delta_query_error("orders:main", "orders", refusal(true));
        assert!(
            matches!(&denied, QueryError::CatalogAccessDenied { table, .. } if table == "main.sales.orders"),
            "{denied:?}"
        );
        assert_eq!(crate::ApiError::Query(denied).status_code(), 403);

        let outage = delta_query_error("orders:main", "orders", refusal(false));
        assert!(matches!(outage, QueryError::Internal(_)), "{outage:?}");
    }

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

    fn secret(value: &str) -> Option<fluree_db_iceberg::ConfigValue> {
        Some(fluree_db_iceberg::ConfigValue::literal(value))
    }

    #[test]
    fn a_service_principal_needs_only_its_workspace_id_and_secret() {
        let unity = DeltaUnityFields {
            uri: Some("https://workspace.example.com/".to_string()),
            catalog: Some("main".to_string()),
            oauth2_client_id: Some("app".to_string()),
            oauth2_client_secret: Some(fluree_db_iceberg::ConfigValue::from_env("SP_SECRET")),
            ..Default::default()
        }
        .into_config()
        .unwrap()
        .unwrap();
        assert_eq!(unity.catalog.as_deref(), Some("main"));
        let auth = serde_json::to_value(&unity.auth).unwrap();
        assert_eq!(auth["type"], "oauth2_client_credentials");
        assert_eq!(
            auth["token_url"],
            "https://workspace.example.com/oidc/v1/token"
        );
        assert_eq!(auth["scope"], "all-apis");
        assert_eq!(auth["client_secret"]["env_var"], "SP_SECRET");

        let overridden = DeltaUnityFields {
            uri: Some("https://workspace.example.com".to_string()),
            oauth2_client_id: Some("app".to_string()),
            oauth2_client_secret: secret("s"),
            oauth2_token_url: Some("https://login.example.com/token".to_string()),
            oauth2_scope: Some("sql".to_string()),
            ..Default::default()
        }
        .into_config()
        .unwrap()
        .unwrap();
        let auth = serde_json::to_value(&overridden.auth).unwrap();
        assert_eq!(auth["token_url"], "https://login.example.com/token");
        assert_eq!(auth["scope"], "sql");
    }

    #[test]
    fn unity_fields_are_all_or_nothing_with_one_way_to_authenticate() {
        assert!(DeltaUnityFields::default().into_config().unwrap().is_none());
        let uri = || Some("https://workspace.example.com".to_string());

        let bearer = DeltaUnityFields {
            uri: uri(),
            bearer: secret("t"),
            ..Default::default()
        };
        assert!(bearer.clone().into_config().unwrap().is_some());

        for broken in [
            DeltaUnityFields {
                uri: uri(),
                ..Default::default()
            },
            DeltaUnityFields {
                catalog: Some("main".to_string()),
                ..Default::default()
            },
            DeltaUnityFields {
                bearer: secret("t"),
                ..Default::default()
            },
            DeltaUnityFields {
                oauth2_client_id: Some("app".to_string()),
                ..bearer.clone()
            },
            DeltaUnityFields {
                uri: uri(),
                oauth2_client_id: Some("app".to_string()),
                ..Default::default()
            },
            DeltaUnityFields {
                uri: uri(),
                oauth2_client_secret: secret("s"),
                ..Default::default()
            },
        ] {
            assert!(broken.clone().into_config().is_err(), "{broken:?}");
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
