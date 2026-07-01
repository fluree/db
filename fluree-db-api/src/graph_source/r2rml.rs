//! R2RML graph source operations and provider.
//!
//! This module provides APIs for creating R2RML graph sources and implements
//! the R2RML provider traits for query execution against Iceberg tables.
//!
//! This module is only available with the `iceberg` feature.

use crate::graph_source::cache::{CachedScanFiles, R2rmlCache};
use crate::graph_source::config::{CatalogMode, IcebergCreateConfig, R2rmlCreateConfig};
use crate::graph_source::result::{IcebergCreateResult, R2rmlCreateResult};
use crate::Result;
use async_trait::async_trait;
use fluree_db_core::ContentStore;
use fluree_db_iceberg::{
    catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient},
    io::{ColumnBatch, S3IcebergStorage, SendIcebergStorage, SendParquetReader},
    metadata::TableMetadata,
    scan::{ComparisonOp, Expression, FileScanTask, LiteralValue, ScanConfig, SendScanPlanner},
    IcebergGsConfig,
};
use fluree_db_nameservice::GraphSourceType;
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{
    ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanCmpOp, ScanFilter, ScanValue,
};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use futures::StreamExt;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// How many data files to read concurrently. Defaults to
/// `min(available_parallelism, files, 8)`; override with
/// `FLUREE_ICEBERG_SCAN_CONCURRENCY` (a positive integer; not capped, so callers
/// can raise it for high-latency remote object stores). Bounded to keep memory
/// and S3 request fan-out in check.
fn iceberg_scan_concurrency(num_files: usize) -> usize {
    if let Some(n) = std::env::var("FLUREE_ICEBERG_SCAN_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        return n.min(num_files.max(1));
    }
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    cpus.min(num_files.max(1)).clamp(1, 8)
}

/// Stable hash of a graph source's raw config JSON. Keys the process-wide REST
/// catalog client cache. A config *edit* (including a secret written inline)
/// yields a new fingerprint and a freshly built client. Note this hashes the raw
/// JSON only: a secret referenced by env var / secret store is stored as that
/// reference, so rotating the underlying secret leaves the fingerprint unchanged
/// — the client cache's TTL (see `cache::DEFAULT_REST_CLIENT_TTL_SECS`), not this
/// fingerprint, is what bounds staleness in that case.
fn config_fingerprint(config: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    config.hash(&mut h);
    h.finish()
}

/// Translate resolved scan filters into an Iceberg pushdown `Expression` for
/// file pruning. Filters on unknown columns are skipped; an empty result is
/// `None`. Conservative — pruning never drops matching rows because the
/// in-engine FILTER still runs.
fn build_iceberg_filter(
    filters: &[ScanFilter],
    schema: &fluree_db_iceberg::metadata::Schema,
) -> Option<Expression> {
    let mut comparisons = Vec::new();
    for f in filters {
        let Some(field) = schema.field_by_name(&f.column) else {
            continue;
        };
        let op = match f.op {
            ScanCmpOp::Eq => ComparisonOp::Eq,
            ScanCmpOp::NotEq => ComparisonOp::NotEq,
            ScanCmpOp::Lt => ComparisonOp::Lt,
            ScanCmpOp::LtEq => ComparisonOp::LtEq,
            ScanCmpOp::Gt => ComparisonOp::Gt,
            ScanCmpOp::GtEq => ComparisonOp::GtEq,
        };
        let value = match &f.value {
            ScanValue::Bool(b) => LiteralValue::Boolean(*b),
            ScanValue::Date(d) => LiteralValue::Date(*d),
            // Iceberg `int` is 32-bit, `long` 64-bit. For an `int` column a
            // literal outside i32 range must NOT be truncated with `as` (it would
            // wrap and could prune files the residual filter keeps); skip the
            // pushdown for that predicate instead.
            ScanValue::Int(n) => match field.type_string() {
                Some("int") => match i32::try_from(*n) {
                    Ok(v) => LiteralValue::Int32(v),
                    Err(_) => continue,
                },
                _ => LiteralValue::Int64(*n),
            },
        };
        comparisons.push(Expression::Comparison {
            field_id: field.id,
            column: f.column.clone(),
            op,
            value,
        });
    }
    match comparisons.len() {
        0 => None,
        1 => comparisons.into_iter().next(),
        _ => Some(Expression::And(comparisons)),
    }
}

// =============================================================================
// Iceberg/R2RML Graph Source Creation
// =============================================================================

impl crate::Fluree {
    /// Create an Iceberg graph source.
    ///
    /// This operation:
    /// 1. Validates the configuration
    /// 2. Optionally tests the catalog connection
    /// 3. Publishes the graph source record to the nameservice
    pub async fn create_iceberg_graph_source(
        &self,
        config: IcebergCreateConfig,
    ) -> Result<IcebergCreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(
            graph_source_id = %graph_source_id,
            catalog = %config.catalog_uri_or_location(),
            table = %config.table_identifier_display(),
            "Creating Iceberg graph source"
        );

        // 1. Validate configuration
        config.validate()?;

        // 2. Test catalog connection (REST mode only — Direct mode verified at query time)
        let connection_tested = if config.is_rest() {
            let ok = self.test_iceberg_connection(&config).await.is_ok();
            if !ok {
                warn!(
                    graph_source_id = %graph_source_id,
                    "Could not verify catalog connection - graph source will be created but may fail at query time"
                );
            }
            ok
        } else {
            false
        };

        // 3. Convert config to storage format
        let iceberg_config = config.to_iceberg_gs_config();
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {e}")))?;

        // 4. Publish graph source record to nameservice
        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[], // No ledger dependencies for Iceberg graph sources
            )
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            connection_tested = connection_tested,
            "Created Iceberg graph source"
        );

        Ok(IcebergCreateResult {
            graph_source_id,
            table_identifier: config.table_identifier_display(),
            catalog_uri: config.catalog_uri_or_location().to_string(),
            connection_tested,
        })
    }

    /// Create an R2RML graph source (Iceberg table with R2RML mapping).
    ///
    /// For `R2rmlMappingInput::Content`, validates the mapping content and
    /// stores it to CAS. For `R2rmlMappingInput::Address`, validates from
    /// the pre-existing storage address.
    pub async fn create_r2rml_graph_source(
        &self,
        config: R2rmlCreateConfig,
    ) -> Result<R2rmlCreateResult> {
        use crate::graph_source::config::R2rmlMappingInput;

        let graph_source_id = config.graph_source_id();
        info!(graph_source_id = %graph_source_id, "Creating R2RML graph source");

        config.validate()?;

        // Resolve mapping: validate and store to CAS if inline content
        let (mapping_address, triples_map_count, table_names, mapping_validated) = match &config
            .mapping
        {
            R2rmlMappingInput::Content(content) => {
                // Inline content has no filename to sniff; the shared resolver
                // defaults a missing media type to Turtle (matching the eventual
                // CID address, which is also extensionless).
                let compiled =
                    Self::compile_r2rml_content(content, config.mapping_media_type.as_deref(), "")?;
                let count = compiled.len();
                let tables = Self::sorted_table_names(&compiled);
                let gs_id = config.graph_source_id();
                let cs = self.content_store(&gs_id);
                let cid = cs
                    .put(
                        fluree_db_core::ContentKind::GraphSourceMapping,
                        content.as_bytes(),
                    )
                    .await
                    .map_err(|e| {
                        crate::ApiError::Config(format!("Failed to store R2RML mapping: {e}"))
                    })?;
                let addr = cid.to_string();
                info!(graph_source_id = %graph_source_id, mapping_cid = %addr, "R2RML mapping stored to CAS");
                (addr, count, tables, true)
            }
            R2rmlMappingInput::Address(address) => {
                let (count, tables, validated) = self
                        .validate_r2rml_mapping_from_address(address, &config)
                        .await
                        .map(|(c, t)| (c, t, true))
                        .unwrap_or_else(|e| {
                            warn!(graph_source_id = %graph_source_id, error = %e, "Could not validate R2RML mapping from address");
                            (0, Vec::new(), false)
                        });
                (address.clone(), count, tables, validated)
            }
        };
        let table_count = table_names.len();

        // Test catalog connection (REST mode only)
        let connection_tested = if config.iceberg.is_rest() {
            self.test_iceberg_connection(&config.iceberg).await.is_ok()
        } else {
            false
        };

        // Store config with CAS mapping address
        let iceberg_config = config.to_iceberg_gs_config(&mapping_address);
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {e}")))?;

        self.publisher()?
            .publish_graph_source(
                &config.iceberg.name,
                config.iceberg.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[],
            )
            .await?;

        info!(graph_source_id = %graph_source_id, mapping_address = %mapping_address, "Created R2RML graph source");

        Ok(R2rmlCreateResult {
            graph_source_id,
            table_identifier: config.iceberg.table_identifier_display(),
            catalog_uri: config.iceberg.catalog_uri_or_location().to_string(),
            mapping_source: mapping_address,
            triples_map_count,
            table_count,
            table_names,
            connection_tested,
            mapping_validated,
        })
    }

    /// Test connection to an Iceberg REST catalog.
    ///
    /// Only applicable to REST mode. Direct mode has no catalog to test.
    async fn test_iceberg_connection(&self, config: &IcebergCreateConfig) -> Result<()> {
        use fluree_db_iceberg::catalog::parse_table_identifier;

        let rest = match &config.catalog_mode {
            CatalogMode::Rest(rest) => rest,
            CatalogMode::Direct { .. } => {
                return Err(crate::ApiError::Config(
                    "Connection test is not supported for Direct catalog mode".to_string(),
                ));
            }
        };

        // Create auth provider
        let auth = rest
            .auth
            .create_provider_arc()
            .map_err(|e| crate::ApiError::Config(format!("Failed to create auth provider: {e}")))?;

        // Create catalog client
        let catalog_config = RestCatalogConfig {
            uri: rest.catalog_uri.clone(),
            warehouse: rest.warehouse.clone(),
            ..Default::default()
        };

        let catalog = RestCatalogClient::new(catalog_config, auth).map_err(|e| {
            crate::ApiError::Config(format!("Failed to create catalog client: {e}"))
        })?;

        // Parse table identifier
        let table_id = parse_table_identifier(&rest.table_identifier)
            .map_err(|e| crate::ApiError::Config(format!("Invalid table identifier: {e}")))?;

        // Attempt to load table metadata (this tests the connection)
        catalog
            .load_table(&table_id, rest.vended_credentials)
            .await
            .map_err(|e| {
                crate::ApiError::Config(format!("Failed to load table from catalog: {e}"))
            })?;

        Ok(())
    }

    /// Compile R2RML content and return the compiled mapping.
    ///
    /// `source` is the mapping's filename, storage address, or content-addressed
    /// CID; it is only consulted to infer the format when no explicit
    /// `media_type` is given. Format selection goes through the shared
    /// [`fluree_db_r2rml::loader::MappingFormat`] resolver (default Turtle) so
    /// registration and query time can never disagree (issue #1397).
    fn compile_r2rml_content(
        content: &str,
        media_type: Option<&str>,
        source: &str,
    ) -> Result<fluree_db_r2rml::mapping::CompiledR2rmlMapping> {
        use fluree_db_r2rml::loader::MappingFormat;
        match MappingFormat::resolve(media_type, source) {
            MappingFormat::Turtle => fluree_db_r2rml::loader::R2rmlLoader::from_turtle(content)
                .map_err(|e| crate::ApiError::Config(format!("Failed to parse R2RML Turtle: {e}")))?
                .compile()
                .map_err(|e| {
                    crate::ApiError::Config(format!("Failed to compile R2RML mapping: {e}"))
                }),
            MappingFormat::JsonLd => Err(crate::ApiError::Config(
                "R2RML mapping must be in Turtle format. JSON-LD is not yet supported.".into(),
            )),
        }
    }

    /// Validate an R2RML mapping from a pre-existing storage address.
    ///
    /// Returns the number of TriplesMap definitions and the sorted list of
    /// distinct logical table names referenced by the mapping.
    async fn validate_r2rml_mapping_from_address(
        &self,
        address: &str,
        config: &R2rmlCreateConfig,
    ) -> Result<(usize, Vec<String>)> {
        let storage = self.admin_storage().ok_or_else(|| {
            crate::ApiError::Config(format!(
                "Cannot load R2RML mapping from address '{address}': address-based reads are not supported on this backend"
            ))
        })?;
        let bytes = storage.read_bytes(address).await.map_err(|e| {
            crate::ApiError::Config(format!(
                "Failed to load R2RML mapping from '{address}': {e}"
            ))
        })?;
        let content = String::from_utf8(bytes).map_err(|e| {
            crate::ApiError::Config(format!("R2RML mapping is not valid UTF-8: {e}"))
        })?;
        // `address` may carry an extension (e.g. `.ttl`/`.jsonld`); pass it so the
        // resolver can infer the format when no explicit media type is set.
        let compiled =
            Self::compile_r2rml_content(&content, config.mapping_media_type.as_deref(), address)?;
        Ok((compiled.len(), Self::sorted_table_names(&compiled)))
    }

    /// Collect the distinct logical table names referenced by a compiled
    /// mapping, sorted for deterministic reporting.
    fn sorted_table_names(compiled: &CompiledR2rmlMapping) -> Vec<String> {
        let mut names: Vec<String> = compiled
            .table_names()
            .into_iter()
            .map(str::to_string)
            .collect();
        names.sort();
        names
    }
}

// =============================================================================
// R2RML Provider Implementation
// =============================================================================

/// Provider for R2RML graph source query integration.
///
/// This provider implements the `R2rmlProvider` and `R2rmlTableProvider` traits
/// required by the query engine to execute R2RML-backed queries against
/// Iceberg tables.
///
/// # Usage
///
/// ```ignore
/// use fluree_db_api::FlureeR2rmlProvider;
///
/// let provider = FlureeR2rmlProvider::new(&fluree);
/// let ctx = ExecutionContext::new(&db, &vars)
///     .with_r2rml_providers(&provider, &provider);
/// ```
pub struct FlureeR2rmlProvider<'a> {
    fluree: &'a crate::Fluree,
    /// Query-scoped catalog state. The provider is constructed once per query, so
    /// this caches the REST client (OAuth token) and `loadTable` responses for
    /// the lifetime of one query — collapsing the per-scan REST round-trip storm
    /// and pinning a single Iceberg snapshot across the query.
    session: std::sync::Arc<super::catalog_session::IcebergCatalogSession>,
}

impl<'a> FlureeR2rmlProvider<'a> {
    /// Create a new R2RML provider wrapping a Fluree instance.
    pub fn new(fluree: &'a crate::Fluree) -> Self {
        Self {
            fluree,
            session: std::sync::Arc::new(super::catalog_session::IcebergCatalogSession::default()),
        }
    }
}

impl std::fmt::Debug for FlureeR2rmlProvider<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlureeR2rmlProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl R2rmlProvider for FlureeR2rmlProvider<'_> {
    /// Check if a graph source has an R2RML mapping.
    async fn has_r2rml_mapping(&self, graph_source_id: &str) -> bool {
        match self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
        {
            Ok(Some(record)) => {
                // First check if this is an R2RML or Iceberg graph source type
                if !matches!(
                    record.source_type,
                    GraphSourceType::R2rml | GraphSourceType::Iceberg
                ) {
                    return false;
                }

                // Parse into typed config to stay aligned with real config schema
                match IcebergGsConfig::from_json(&record.config) {
                    Ok(config) => config.mapping.is_some(),
                    Err(_) => false,
                }
            }
            Ok(None) => false,
            Err(_) => false,
        }
    }

    /// Get the compiled R2RML mapping for a graph source.
    ///
    /// This method uses the R2RML cache to avoid repeated parsing and compilation.
    async fn compiled_mapping(
        &self,
        graph_source_id: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        // Look up the graph source record
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;

        // Verify it's an R2RML or Iceberg graph source
        if !matches!(
            record.source_type,
            GraphSourceType::R2rml | GraphSourceType::Iceberg
        ) {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}' is not an R2RML graph source (type: {:?})",
                graph_source_id, record.source_type
            )));
        }

        // Parse into typed config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        let mapping_config = iceberg_config.mapping.as_ref().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "Graph source '{graph_source_id}' is missing 'mapping' in config"
            ))
        })?;

        let mapping_source = &mapping_config.source;
        let media_type = mapping_config.media_type.as_deref();

        // Check cache first
        let cache = self.fluree.r2rml_cache();
        let cache_key = R2rmlCache::mapping_cache_key(graph_source_id, mapping_source, media_type);

        if let Some(cached) = cache.get_mapping(&cache_key).await {
            debug!(
                graph_source_id = %graph_source_id,
                cache_key = %cache_key,
                "R2RML mapping cache hit"
            );
            return Ok(cached);
        }

        debug!(
            graph_source_id = %graph_source_id,
            cache_key = %cache_key,
            "R2RML mapping cache miss - loading from storage"
        );

        // Cache miss - load the mapping content.
        // Try CID-based content store first (CAS-stored mappings),
        // fall back to raw storage read (legacy address-based mappings).
        let mapping_bytes = if let Ok(cid) = mapping_source.parse::<fluree_db_core::ContentId>() {
            let cs = self.fluree.content_store(graph_source_id);
            cs.get(&cid).await.map_err(|e| {
                QueryError::InvalidQuery(format!(
                    "Failed to load R2RML mapping (CID {mapping_source}): {e}"
                ))
            })?
        } else {
            let storage = self.fluree.admin_storage().ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "Cannot load R2RML mapping from address '{mapping_source}': address-based reads are not supported on this backend",
                ))
            })?;
            storage.read_bytes(mapping_source).await.map_err(|e| {
                QueryError::InvalidQuery(format!(
                    "Failed to load R2RML mapping from '{mapping_source}': {e}"
                ))
            })?
        };

        let mapping_content = String::from_utf8(mapping_bytes).map_err(|e| {
            QueryError::InvalidQuery(format!(
                "R2RML mapping at '{mapping_source}' is not valid UTF-8: {e}"
            ))
        })?;

        // Parse and compile the mapping. Format selection goes through the same
        // shared resolver the registration path uses, so a mapping stored
        // without an explicit media type (e.g. a CAS CID) defaults to Turtle
        // here too instead of erroring as JSON-LD (issue #1397).
        use fluree_db_r2rml::loader::MappingFormat;
        let compiled = match MappingFormat::resolve(media_type, mapping_source) {
            MappingFormat::Turtle => {
                fluree_db_r2rml::loader::R2rmlLoader::from_turtle(&mapping_content)
                    .map_err(|e| {
                        QueryError::InvalidQuery(format!(
                            "Failed to parse R2RML Turtle from '{mapping_source}': {e}"
                        ))
                    })?
                    .compile()
                    .map_err(|e| {
                        QueryError::InvalidQuery(format!(
                            "Failed to compile R2RML mapping from '{mapping_source}': {e}"
                        ))
                    })?
            }
            MappingFormat::JsonLd => {
                return Err(QueryError::InvalidQuery(format!(
                    "R2RML mapping for '{graph_source_id}' uses JSON-LD format, which is not yet supported. \
                     Please use Turtle format (.ttl)."
                )));
            }
        };

        let compiled = Arc::new(compiled);

        // Cache the compiled mapping
        cache
            .put_mapping(cache_key.clone(), Arc::clone(&compiled))
            .await;

        info!(
            graph_source_id = %graph_source_id,
            cache_key = %cache_key,
            triples_maps = compiled.triples_maps.len(),
            "Loaded, compiled, and cached R2RML mapping"
        );

        Ok(compiled)
    }
}

#[async_trait]
impl R2rmlTableProvider for FlureeR2rmlProvider<'_> {
    /// Scan an Iceberg table, streaming column batches as data files are read.
    ///
    /// This method connects to the Iceberg catalog, plans the scan with the
    /// specified projection/filters, and returns a [`ColumnBatchStream`] that
    /// yields one file's batches at a time (bounded-parallel reads) so a
    /// streaming consumer never holds the whole table in memory.
    async fn scan_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        // Look up the graph source record to get Iceberg connection info
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {e}")))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!("Graph source '{graph_source_id}' not found"))
            })?;

        // Parse the Iceberg graph source config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        // Validate the config
        iceberg_config.validate().map_err(|e| {
            QueryError::InvalidQuery(format!(
                "Invalid Iceberg graph source config for '{graph_source_id}': {e}"
            ))
        })?;

        info!(
            graph_source_id = %graph_source_id,
            table_name = %table_name,
            projection = ?projection,
            "Starting Iceberg table scan"
        );

        // Branch on catalog mode: REST vs Direct
        use fluree_db_iceberg::config::CatalogConfig;
        use fluree_db_iceberg::SendDirectCatalogClient;

        // Parse the table identifier
        use fluree_db_iceberg::catalog::parse_table_identifier;
        let table_id = if !table_name.is_empty() {
            parse_table_identifier(table_name).map_err(|e| {
                QueryError::Internal(format!(
                    "Failed to parse table identifier '{table_name}': {e}"
                ))
            })?
        } else {
            iceberg_config.table_identifier().map_err(|e| {
                QueryError::Internal(format!("Failed to parse table identifier: {e}"))
            })?
        };

        // Resolve metadata location and create storage based on catalog mode
        let (load_response, storage) = match &iceberg_config.catalog {
            CatalogConfig::Rest {
                uri,
                warehouse,
                auth,
                ..
            } => {
                let cache = self.fluree.r2rml_cache();

                // Process-wide REST client keyed by the source config fingerprint:
                // its OAuth `CachedToken` and HTTPS connection pool are reused
                // across queries, so a warm server does one token exchange per
                // ~hour instead of one per query. The fingerprint hashes the full
                // source config, so a rotated PAT (or any config change) builds a
                // fresh client.
                let client_fp = format!(
                    "{graph_source_id}\u{1f}{:016x}",
                    config_fingerprint(&record.config)
                );
                let catalog = match cache.rest_client(&client_fp) {
                    Some(c) => c,
                    None => {
                        let auth_provider = auth.create_provider_arc().map_err(|e| {
                            QueryError::Internal(format!("Failed to create auth provider: {e}"))
                        })?;
                        let catalog_config = RestCatalogConfig {
                            uri: uri.clone(),
                            warehouse: warehouse.clone(),
                            ..Default::default()
                        };
                        let client = Arc::new(
                            RestCatalogClient::new(catalog_config, auth_provider).map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to create catalog client: {e}"
                                ))
                            })?,
                        );
                        cache.put_rest_client(client_fp, Arc::clone(&client));
                        client
                    }
                };

                let lt_key = super::catalog_session::IcebergCatalogSession::load_table_key(
                    graph_source_id,
                    &table_id.namespace,
                    &table_id.table,
                );

                // Resolve `loadTable`, cheapest first: (1) the per-query pin (one
                // snapshot for the whole query); (2) the cross-query cache (skips
                // the ~1.3–3s catalog GET, TTL + creds gated); (3) a real REST
                // load, which populates both caches.
                let load_response = if let Some(cached) = self.session.cached_load_table(&lt_key) {
                    debug!(namespace = %table_id.namespace, table = %table_id.table,
                        "loadTable pin hit (query-scoped)");
                    cached
                } else {
                    let pinned = self.session.pinned_metadata_location(&lt_key);
                    // A cross-query hit applies only on the FIRST resolution of
                    // this table in the query. Once pinned, a reload is a creds
                    // refresh that must keep the pinned snapshot.
                    let cross_query = if pinned.is_none() {
                        cache.get_rest_load_table(&lt_key)
                    } else {
                        None
                    };
                    let mut resp = if let Some(cq) = cross_query {
                        debug!(namespace = %table_id.namespace, table = %table_id.table,
                            "loadTable cache hit (cross-query)");
                        cq.to_response()
                    } else {
                        info!(catalog_uri = %uri, namespace = %table_id.namespace,
                            table = %table_id.table, "Loading table from REST catalog");
                        let actual = catalog
                            .load_table(&table_id, iceberg_config.io.vended_credentials)
                            .await
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to load table from catalog: {e}"
                                ))
                            })?;
                        // The cross-query cache reflects the CURRENT catalog state
                        // (never this query's pin), so other queries see the newest
                        // snapshot within the TTL.
                        cache.put_rest_load_table(
                            lt_key.clone(),
                            Arc::new(super::catalog_session::CachedLoadTable::from_response(
                                &actual,
                            )),
                        );
                        // This query keeps its pinned snapshot across a creds
                        // refresh: vended creds are bucket/prefix-scoped, so the
                        // fresh creds still read the pinned snapshot's immutable
                        // data files.
                        let mut r = actual;
                        if let Some(ref pinned_loc) = pinned {
                            if *pinned_loc != r.metadata_location {
                                debug!(pinned = %pinned_loc, reloaded = %r.metadata_location,
                                    "Refreshed vended credentials; keeping the query's pinned snapshot");
                                r.metadata_location = pinned_loc.clone();
                            }
                        }
                        info!(metadata_location = %r.metadata_location,
                            has_credentials = r.credentials.is_some(), "Loaded table metadata location");
                        r
                    };
                    self.session.store_load_table(lt_key.clone(), &resp);
                    // Converge on the pinned snapshot. `store_load_table` keeps the
                    // first writer's `metadata_location`, so if a concurrent first
                    // load of this table pinned a different location between our
                    // pin check above and this store, adopt the winning pin rather
                    // than scan our own freshly loaded location — otherwise two
                    // scans in one query could read different snapshots
                    // (fluree/db#1406 review). Sequential execution makes this a
                    // no-op; it holds the invariant unconditionally.
                    if let Some(pinned_loc) = self.session.pinned_metadata_location(&lt_key) {
                        resp.metadata_location = pinned_loc;
                    }
                    resp
                };

                let storage = if let Some(ref credentials) = load_response.credentials {
                    info!(
                        region = ?iceberg_config.io.s3_region,
                        endpoint = ?iceberg_config.io.s3_endpoint,
                        "Using vended credentials from catalog"
                    );
                    // Thread the io overrides so a catalog that omits the region (or where
                    // we want an operator-configured endpoint/path-style) still resolves
                    // correctly. Precedence inside the call: vended > these overrides > SDK.
                    S3IcebergStorage::from_vended_credentials(
                        credentials,
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create S3 storage: {e}"))
                    })?
                } else {
                    info!(
                        region = ?iceberg_config.io.s3_region,
                        endpoint = ?iceberg_config.io.s3_endpoint,
                        "Using ambient AWS credentials"
                    );
                    S3IcebergStorage::from_default_chain(
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create S3 storage: {e}"))
                    })?
                };

                (load_response, Arc::new(storage))
            }
            CatalogConfig::Direct { table_location } => {
                info!(
                    table_location = %table_location,
                    "Loading table via direct S3 access"
                );

                // Direct mode: create storage once, share via Arc
                let storage = Arc::new(
                    S3IcebergStorage::from_default_chain(
                        iceberg_config.io.s3_region.as_deref(),
                        iceberg_config.io.s3_endpoint.as_deref(),
                        iceberg_config.io.s3_path_style,
                    )
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!("Failed to create S3 storage: {e}"))
                    })?,
                );

                let cache = self.fluree.r2rml_cache();
                let load_response = if let Some(metadata_location) =
                    cache.get_direct_metadata_location(table_location).await
                {
                    debug!(
                        table_location = %table_location,
                        metadata_location = %metadata_location,
                        "Direct metadata-location cache hit"
                    );
                    fluree_db_iceberg::catalog::LoadTableResponse {
                        metadata_location,
                        config: std::collections::HashMap::default(),
                        credentials: None,
                    }
                } else {
                    debug!(table_location = %table_location, "Direct metadata-location cache miss");

                    let direct_catalog =
                        SendDirectCatalogClient::new(table_location.clone(), Arc::clone(&storage));

                    let load_response =
                        direct_catalog
                            .load_table(&table_id, false)
                            .await
                            .map_err(|e| {
                                QueryError::Internal(format!(
                                    "Failed to resolve table metadata from {table_location}: {e}"
                                ))
                            })?;
                    cache
                        .put_direct_metadata_location(
                            table_location.clone(),
                            load_response.metadata_location.clone(),
                        )
                        .await;
                    load_response
                };

                info!(
                    metadata_location = %load_response.metadata_location,
                    "Resolved table metadata via version-hint.text"
                );

                (load_response, storage)
            }
        };

        // Shared on-disk cache for data files (one global byte budget, deduped
        // per directory). Threaded into the Parquet readers, which apply a
        // whole-file-vs-range policy per file based on how much each query reads.
        let cache_dir = self.fluree.binary_store_cache_dir();
        let disk_cache = fluree_db_iceberg::DiskArtifactCache::for_dir(&cache_dir);

        // Check cache for table metadata
        let cache = self.fluree.r2rml_cache();
        let metadata_location = &load_response.metadata_location;

        let metadata = if let Some(cached) = cache.get_metadata(metadata_location).await {
            debug!(metadata_location = %metadata_location, "Table metadata cache hit");
            cached
        } else {
            debug!(metadata_location = %metadata_location, "Table metadata cache miss");

            let metadata_bytes = storage
                .as_ref()
                .read(metadata_location)
                .await
                .map_err(|e| QueryError::Internal(format!("Failed to read table metadata: {e}")))?;

            let parsed = TableMetadata::from_json(&metadata_bytes).map_err(|e| {
                QueryError::Internal(format!("Failed to parse table metadata: {e}"))
            })?;

            let metadata = Arc::new(parsed);
            cache
                .put_metadata(metadata_location.clone(), Arc::clone(&metadata))
                .await;

            info!(
                metadata_location = %metadata_location,
                format_version = metadata.format_version,
                "Loaded and cached table metadata"
            );

            metadata
        };

        let schema = metadata
            .current_schema()
            .ok_or_else(|| QueryError::Internal("Table has no current schema".to_string()))?;

        info!(
            format_version = metadata.format_version,
            schema_id = schema.schema_id,
            field_count = schema.fields.len(),
            "Parsed table metadata"
        );

        // Resolve column names to field IDs for projection
        let projected_field_ids: Vec<i32> = if projection.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !f.is_nested())
                .map(|f| f.id)
                .collect()
        } else {
            projection
                .iter()
                .filter_map(|col_name| schema.field_by_name(col_name).map(|f| f.id))
                .collect()
        };

        if projected_field_ids.is_empty() && !projection.is_empty() {
            return Err(QueryError::InvalidQuery(format!(
                "None of the projected columns {:?} exist in table schema. Available: {:?}",
                projection,
                schema.field_names()
            )));
        }

        let schema_arc = Arc::new(schema.clone());

        // Build an Iceberg pushdown predicate for file pruning. Filters resolve
        // to fields by name; unknown fields are skipped (conservative).
        let filter_expr = build_iceberg_filter(filters, schema);

        // Reuse manifest-derived file selections across repeated scans of the
        // same snapshot. Projection still varies per scan, so we rebuild tasks.
        // The scan-files cache is keyed only by metadata location, so it is
        // bypassed when a pushdown filter is present (different filter → a
        // different pruned file set).
        let (tasks, files_selected, files_pruned, estimated_row_count) =
            if let Some(filter) = &filter_expr {
                let scan_config = ScanConfig::new()
                    .with_projection(projected_field_ids.clone())
                    .with_filter(filter.clone());
                let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);
                let plan = planner
                    .plan_scan()
                    .await
                    .map_err(|e| QueryError::Internal(format!("Failed to plan scan: {e}")))?;
                (
                    plan.tasks,
                    plan.files_selected,
                    plan.files_pruned,
                    plan.estimated_row_count,
                )
            } else if let Some(cached) = cache.get_scan_files(metadata_location).await {
                debug!(
                    metadata_location = %metadata_location,
                    cached_files = cached.data_files.len(),
                    "Iceberg scan-files cache hit"
                );

                let tasks = cached
                    .data_files
                    .iter()
                    .cloned()
                    .map(|data_file| {
                        FileScanTask::for_whole_file_with_schema(
                            data_file,
                            projected_field_ids.clone(),
                            None,
                            Arc::clone(&schema_arc),
                        )
                    })
                    .collect::<Vec<_>>();

                (
                    tasks,
                    cached.files_selected,
                    cached.files_pruned,
                    cached.estimated_row_count,
                )
            } else {
                debug!(metadata_location = %metadata_location, "Iceberg scan-files cache miss");

                // Create scan configuration with projection for the first plan.
                let scan_config = ScanConfig::new().with_projection(projected_field_ids.clone());
                let planner = SendScanPlanner::new(storage.as_ref(), &metadata, scan_config);
                let plan = planner
                    .plan_scan()
                    .await
                    .map_err(|e| QueryError::Internal(format!("Failed to plan scan: {e}")))?;

                let cached = Arc::new(CachedScanFiles {
                    data_files: Arc::new(
                        plan.tasks
                            .iter()
                            .map(|task| task.data_file.clone())
                            .collect(),
                    ),
                    estimated_row_count: plan.estimated_row_count,
                    files_selected: plan.files_selected,
                    files_pruned: plan.files_pruned,
                });
                cache
                    .put_scan_files(metadata_location.clone(), Arc::clone(&cached))
                    .await;

                (
                    plan.tasks,
                    cached.files_selected,
                    cached.files_pruned,
                    cached.estimated_row_count,
                )
            };

        info!(
            files_selected,
            files_pruned,
            estimated_rows = estimated_row_count,
            "Scan plan created"
        );

        if tasks.is_empty() {
            info!("Scan plan has no files - returning empty result");
            return Ok(empty_batch_stream());
        }

        // Read data files with bounded parallelism, streaming each file's batches
        // to the consumer as the worker completes. Concurrency is capped (see
        // `iceberg_scan_concurrency`) so only O(concurrency) file decodes are
        // resident — the consumer (R2rmlScanOperator) materializes and aggregates
        // incrementally instead of the whole table being collected here.
        let footers = cache.parquet_footers();
        let concurrency = iceberg_scan_concurrency(tasks.len());
        debug!(
            files = tasks.len(),
            concurrency, "streaming Parquet files (bounded parallel)"
        );

        let stream = futures::stream::iter(tasks)
            .map(move |task| {
                let storage = Arc::clone(&storage);
                let footers = Arc::clone(&footers);
                let disk_cache = Arc::clone(&disk_cache);
                let cache_dir = cache_dir.clone();
                async move {
                    tokio::spawn(async move {
                        let reader = SendParquetReader::with_caches(
                            storage.as_ref(),
                            footers.as_ref(),
                            &disk_cache,
                            &cache_dir,
                        );
                        reader.read_task(&task).await.map_err(|e| {
                            QueryError::Internal(format!(
                                "Failed to read Parquet file '{}': {e}",
                                task.data_file.file_path
                            ))
                        })
                    })
                    .await
                    .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
                }
            })
            .buffer_unordered(concurrency)
            // Flatten each file's `Result<Vec<ColumnBatch>>` into individual
            // `Result<ColumnBatch>` items; a read error becomes one error item.
            .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                Ok(batches) => {
                    futures::stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>())
                }
                Err(e) => futures::stream::iter(vec![Err(e)]),
            });

        Ok(Box::pin(stream))
    }
}

/// An empty [`ColumnBatchStream`], used when a scan plan selects no files.
fn empty_batch_stream() -> ColumnBatchStream {
    Box::pin(futures::stream::empty())
}
