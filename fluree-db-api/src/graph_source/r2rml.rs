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
    scan::{
        topk::{batch_sort_values, plan_topk_read, TopKBound},
        ComparisonOp, Expression, FileScanTask, LiteralValue, ScanConfig, SendScanPlanner,
    },
    stats::{aggregate_column_stats, send_read_snapshot_data_files},
    IcebergGsConfig,
};
use fluree_db_nameservice::GraphSourceType;
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{
    ColumnBatchStream, R2rmlProvider, R2rmlTableProvider, ScanCmpOp, ScanFilter, ScanTopK,
    ScanValue,
};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use futures::StreamExt;
use std::sync::Arc;
use tracing::{debug, info, warn, Instrument};

/// Max files a scan-side top-k (PR-5) reads SEQUENTIALLY (bound-ordered, with
/// early-stop) before conceding the prune is ineffective and handing the rest to
/// the normal bounded-parallel reader. Caps the worst case (adversarial layout /
/// all files tie at the bound / a heap that never fills) so the topk path can
/// never be slower than the parallel path it replaces. The win case (q046) reads
/// ~10-15 files and stops well under this.
const TOPK_SEQUENTIAL_CAP: usize = 128;

/// How many data files to read concurrently. Defaults to
/// `min(available_parallelism, files, 32)`; override with
/// `FLUREE_ICEBERG_SCAN_CONCURRENCY` (a positive integer; not capped, so callers
/// can raise it further for high-latency remote object stores).
///
/// PR-2 Lever B raised the ceiling from 8 to 32. The per-file decode cost is
/// fixed S3 round-trip latency, not CPU (see
/// `docs/audit/2026-07-virtual-dataset-perf/06-per-file-cost.md`), so more
/// in-flight reads is close to pure win on the thousands-of-tiny-files fact-table
/// shape; the sweep showed wall still improving to c=32 with only mild per-file
/// contention creep past ~c=16, hence 32 as the ceiling. Raising the ceiling
/// never lowers the previous default on any core count (`clamp(1, 32) >=
/// clamp(1, 8)` pointwise; a 2-core host still runs 2), but the memory trade is
/// real: in-flight buffer bytes are `O(concurrency)` file decodes (each <=32MB
/// whole-file / <=64MB sparse-buffer), and the default now scales with cores up
/// to 32 where it was previously capped at 8 regardless of cores — up to 4x the
/// prior in-flight bytes on a >=32-core host. The sweep data and the
/// tiny-file fact-table shape justify the trade; the env override is the
/// pressure valve in both directions — raise it to reach the ceiling on a
/// low-core host, lower it on a memory-tight one.
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
    cpus.min(num_files.max(1)).clamp(1, 32)
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

/// Build the process-wide REST-client cache key for a graph source: its id plus a
/// fingerprint of the raw config JSON. Shared by the query scan path and the
/// `/info` row-count fetch so both reuse the SAME cached client (one OAuth token
/// and one HTTPS connection pool), warmed by whichever path runs first. Keeping
/// this in one place guarantees the two keys never drift.
pub(crate) fn rest_client_cache_key(graph_source_id: &str, config: &str) -> String {
    format!("{graph_source_id}\u{1f}{:016x}", config_fingerprint(config))
}

/// Whether numeric (double / decimal) FILTER pushdown — including the integer →
/// scale-0-decimal coercion against a decimal column — is enabled (PR-7). Mirrors
/// the query-crate `FLUREE_ICEBERG_NUMERIC_STATS` switch (the two crates can't
/// share the `pub(crate)` symbol); read once, cached for the process. Off restores
/// the pre-PR-7 behavior: an integer literal against a decimal column pushes as
/// `Int64`, which the decimal bound compare declines → no prune (full revert).
pub(crate) fn iceberg_numeric_stats_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_ICEBERG_NUMERIC_STATS") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// The Iceberg pushdown literal for an integer scan value against a column of
/// `type_str`. Against a `decimal(p,s)` column with `numeric_stats` on, the
/// integer is pushed as an EXACT scale-0 decimal (comparable to the column's
/// decimal bounds — `decimal_cmp` normalizes the scale gap); with it off it stays
/// `Int64` (which the decimal bound compare declines → no prune), preserving the
/// switch's revert guarantee. This is what lets an integer FILTER (`?deb >
/// 1000000`) prune an `xsd:decimal` column (q019 / H4). An `int`-typed column
/// narrows to `Int32`, skipping (`None`) an out-of-range literal rather than
/// wrapping. `None` = skip the push (the operator still enforces).
fn int_pushdown_literal(
    n: i64,
    type_str: Option<&str>,
    numeric_stats: bool,
) -> Option<LiteralValue> {
    match type_str {
        Some("int") => i32::try_from(n).ok().map(LiteralValue::Int32),
        Some(t) if t.starts_with("decimal") && numeric_stats => Some(LiteralValue::Decimal {
            unscaled: i128::from(n),
            // precision is cosmetic for pruning (`decimal_cmp` ignores it); an i64
            // is ≤19 digits, so the decimal128 max always covers it.
            precision: 38,
            scale: 0,
        }),
        _ => Some(LiteralValue::Int64(n)),
    }
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
            // Push a Date literal only against a physically-`date` column. The
            // Arrow reader applies it as an exact row filter (casting the column
            // to text), but the operator enforces with a lenient `Date::parse`
            // that also accepts `"2024-01-15Z"` / offset forms. On a physically
            // string column the operator would keep such a row while the row
            // filter drops it — so gate the pushdown to keep it a strict subset.
            ScanValue::Date(d) => match field.type_string() {
                Some("date") => LiteralValue::Date(*d),
                _ => continue,
            },
            // Iceberg `int` is 32-bit, `long` 64-bit; against a `decimal` column an
            // integer pushes as an EXACT scale-0 decimal when numeric pushdown is
            // on (else stays `Int64` → no prune). An out-of-i32-range literal on an
            // `int` column is skipped rather than wrapped. See `int_pushdown_literal`.
            ScanValue::Int(n) => {
                match int_pushdown_literal(*n, field.type_string(), iceberg_numeric_stats_enabled())
                {
                    Some(v) => v,
                    None => continue,
                }
            }
            ScanValue::Str(s) => LiteralValue::String(s.clone()),
            // xsd:double / xsd:float FILTER value. Push only against a physically
            // `double` column (exact f64 bounds); a `float` column would need an
            // f64→f32 narrowing that can round the literal and over-prune a range,
            // so skip it — the in-engine FILTER still applies.
            ScanValue::Double(d) => match field.type_string() {
                Some("double") => LiteralValue::Float64(*d),
                // A binary float → decimal coercion is not exact in general, so a
                // double literal is NOT pushed against a decimal column (keep is
                // correct; the in-engine FILTER enforces). Breadcrumb per the
                // decline-observably ruling.
                Some(t) if t.starts_with("decimal") => {
                    debug!(
                        column = %f.column,
                        "double literal vs decimal column: pushdown declined (inexact float→decimal); in-engine FILTER enforces"
                    );
                    continue;
                }
                _ => continue,
            },
            // xsd:decimal FILTER value. Push only against a `decimal(...)` column;
            // the literal keeps its own scale and the bound compare normalizes it
            // against the column's scale. Row-group stats prune only when the
            // column is FLBA-encoded (see `prunable_stats`); file-level manifest
            // bounds prune regardless.
            ScanValue::Decimal {
                unscaled,
                precision,
                scale,
            } => match field.type_string() {
                Some(t) if t.starts_with("decimal") => LiteralValue::Decimal {
                    unscaled: *unscaled,
                    precision: *precision,
                    scale: *scale,
                },
                // A decimal literal against an integer column has no exact
                // cross-type bound compare, so it is NOT pushed (keep is correct).
                // Breadcrumb per the decline-observably ruling.
                Some("int" | "long") => {
                    debug!(
                        column = %f.column,
                        "decimal literal vs integer column: pushdown declined (no exact cross-type bound compare); in-engine FILTER enforces"
                    );
                    continue;
                }
                _ => continue,
            },
            // A reversed subject-template key: coerce the raw string to the
            // column's physical type. A key that parses as an integer pushes as an
            // integer literal against an `int`/`long`/`decimal` column — including
            // a `decimal` of any scale (the Arrow reader casts the integer to the
            // column's decimal type; row-group stats conservatively skip
            // decimals). A `string` column pushes the raw string. A key that is
            // not integer-valued, or any other physical type
            // (float/date/timestamp/boolean), skips the pushdown — the operator
            // still enforces the subject equality either way.
            ScanValue::TemplateKey(s) => match field.type_string() {
                Some("int") => match s.parse::<i32>() {
                    Ok(v) => LiteralValue::Int32(v),
                    Err(_) => continue,
                },
                Some(t) if t == "long" || t.starts_with("decimal") => match s.parse::<i64>() {
                    Ok(v) => LiteralValue::Int64(v),
                    Err(_) => continue,
                },
                Some("string") => LiteralValue::String(s.clone()),
                _ => continue,
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

        let rest = match &config.connection.catalog_mode {
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
        let table_id = parse_table_identifier(&config.table_identifier)
            .map_err(|e| crate::ApiError::Config(format!("Invalid table identifier: {e}")))?;

        // Attempt to load table metadata (this tests the connection)
        catalog
            .load_table(&table_id, config.connection.io.vended_credentials)
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

/// Bounded concurrency for warming per-table catalog contexts in
/// [`FlureeR2rmlProvider::prefetch_tables`] (PR-8 slice 1). Matches the
/// generate-path preview fan-out (`0ade90c59`); the catalog-request semaphore
/// (PR-8 slice 3) is the global Horizon-QPS bound, this is the per-query width.
const CATALOG_PREFETCH_CONCURRENCY: usize = 8;

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
        topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        // Time the whole scan SETUP (loadTable + planning) as one span; it closes
        // when the stream is constructed. Per-file Parquet decode happens later,
        // while the returned stream is consumed, and is timed separately by the
        // `iceberg.parquet_read` spans, so a bare wrapper here would not (and must
        // not) cover decode.
        let span = tracing::debug_span!(
            "r2rml.scan_table",
            graph_source_id,
            table_name,
            projection_len = projection.len()
        );
        self.scan_table_inner(
            graph_source_id,
            table_name,
            projection,
            filters,
            topk,
            _as_of_t,
        )
        .instrument(span)
        .await
    }

    /// The table's exact live row count from the pinned Iceberg manifest — **only
    /// when it provably equals a full-scan `COUNT(*)`** (see the trait contract).
    ///
    /// Resolves the SAME per-query pinned table context the scan uses (via
    /// [`Self::load_table_context`], sharing `self.session`), so a `COUNT` and a
    /// scan in one query read one Iceberg snapshot. It then reads that snapshot's
    /// manifest-list + manifest Avro (never a Parquet/data file), and returns the
    /// `record_count` sum only if [`sound_manifest_row_count`] proves it equals a
    /// full scan: no delete manifests, and every `non_null_col` provably zero-null.
    /// Otherwise `Ok(None)` and the caller falls back to the scan.
    async fn table_row_count(
        &self,
        graph_source_id: &str,
        table_name: &str,
        non_null_cols: &[String],
        _as_of_t: Option<i64>,
    ) -> QueryResult<Option<u64>> {
        // Time the manifest-only read as one span (the same `.instrument` split as
        // `scan_table` / `scan_table_inner`). `fired` records answered (true) vs
        // declined-to-scan (false). The name is allowlisted in
        // `fluree-bench-virtual::spans`, so the vbench pathway counters show the
        // shortcut directly instead of inferring it from `files_selected=0` plus
        // scan-span absence.
        let span = tracing::debug_span!(
            "r2rml.count_manifest",
            graph_source_id,
            table_name,
            fired = tracing::field::Empty
        );
        self.table_row_count_inner(graph_source_id, table_name, non_null_cols, _as_of_t)
            .instrument(span)
            .await
    }

    /// Warm the per-query catalog session pin + cross-query caches for a set of
    /// tables concurrently (PR-8 slice 1). Best-effort and side-effect-only: each
    /// `load_table_context` populates `self.session` + the moka caches, so the
    /// query's following *serial* scans resolve from the pin and skip the
    /// `loadTable` GET. Resolution errors are intentionally swallowed — the real
    /// scan re-resolves and surfaces them — so a warm failure degrades to today's
    /// serial GET, never a changed result.
    async fn prefetch_tables(&self, graph_source_id: &str, table_names: &[String]) {
        // Dedup, preserving first-seen order, AND skip tables already resolved
        // (with unexpired creds) in this query's session pin — re-warming a
        // pinned table would issue a wasted `loadTable` GET. Collect OWNED names:
        // a `Vec<&str>` here makes the `buffered` fan-out closure take a borrowed
        // argument, which trips rustc's "FnOnce is not general enough" HRTB check.
        let mut seen = std::collections::HashSet::new();
        let mut to_warm: Vec<String> = Vec::new();
        for t in table_names {
            if seen.insert(t.as_str()) && !self.is_table_pinned(graph_source_id, t) {
                to_warm.push(t.clone());
            }
        }

        // Engagement + measurement span (allowlisted as `r2rml.prefetch`): its
        // presence proves the prefetch path ran, and `warmed`/`requested` show the
        // fan-out width vs how many were skipped as already-pinned. Emitted even
        // for a no-op fan-out so "ran but skipped" is distinguishable from
        // "never ran".
        let span = tracing::debug_span!(
            "r2rml.prefetch",
            requested = table_names.len(),
            warmed = to_warm.len(),
        );
        if to_warm.len() < 2 {
            // Nothing to overlap. Enter/drop the span (no `.await` under it) so a
            // no-op prefetch is still visible in the counters.
            let _entered = span.entered();
            return;
        }
        // `buffered` polls these futures COOPERATIVELY on one task (no spawn), and
        // the REST-client build inside `load_table_context` is synchronous, so the
        // first future polled builds + caches the process-wide client before any
        // other future resumes past its (async) nameservice lookup — every later
        // table then reuses that one client and its cached OAuth token. Verified
        // live: a cold 3-table fan-out does exactly ONE `iceberg.oauth_token`
        // exchange, not one per table. (If the client build ever becomes async,
        // this dedup breaks and a serial first-table warm would be needed.)
        //
        // The `buffered` width here is the per-query fan-out ceiling; the true
        // bound on concurrent catalog QPS is the process-wide catalog-request
        // semaphore (PR-8 slice 3, `rest.rs`), which every `loadTable` GET this
        // fan-out issues must acquire — so the prefetch cannot defeat the 429
        // protection it runs ahead of, and a lower `FLUREE_ICEBERG_CATALOG_CONCURRENCY`
        // transparently throttles it.
        futures::stream::iter(to_warm)
            .map(|table| async move {
                let _ = self.load_table_context(graph_source_id, &table).await;
            })
            .buffered(CATALOG_PREFETCH_CONCURRENCY)
            .for_each(|()| async {})
            .instrument(span)
            .await;
    }
}

impl FlureeR2rmlProvider<'_> {
    /// Body of [`R2rmlTableProvider::table_row_count`], split out so the trait
    /// method can wrap it in the `r2rml.count_manifest` timing span via
    /// `.instrument()` (the same pattern as [`Self::scan_table_inner`]).
    async fn table_row_count_inner(
        &self,
        graph_source_id: &str,
        table_name: &str,
        non_null_cols: &[String],
        _as_of_t: Option<i64>,
    ) -> QueryResult<Option<u64>> {
        // Same pinned context as the scan: one Iceberg snapshot per query (the
        // shared `self.session` pin), so a count and a scan cannot disagree.
        // GREP: r2rml-as-of-t — `as_of_t` is ignored here exactly as the scan path
        // ignores it (matching breadcrumb in `scan_table_inner`); if time-travel
        // semantics ever land on the scan, this method MUST follow, or a COUNT and
        // a scan in one query could answer from different snapshots.
        let (storage, metadata, metadata_location) =
            self.load_table_context(graph_source_id, table_name).await?;

        // The count must equal a full scan of THIS snapshot — the one the scan
        // planner reads from the same pinned metadata. No current snapshot (an
        // empty table) or no current schema: decline and let the scan handle it (an
        // empty scan folds to 0; a missing schema surfaces the scan's own error).
        let (Some(snapshot), Some(schema)) =
            (metadata.current_snapshot(), metadata.current_schema())
        else {
            return Ok(None);
        };

        // Manifest-only read (never a Parquet/data file): the live data files, and
        // whether the snapshot carries merge-on-read delete manifests.
        //
        // PR-8 slice 2: this manifest read (the COUNT(*) path's, ~450ms cold) is
        // keyed by the content-addressed `metadata_location`, so persist it to the
        // disk catalog cache and serve it from there on a warm-catalog cold
        // process (no S3 read, no `r2rml.count_manifest_read` span).
        //
        // Measurement sub-span (PR-8 cold decomposition): the COUNT(*) path's
        // manifest-list + manifest read (the scan path's equivalent is
        // `iceberg.scan_plan`). For a bare `COUNT(*)` (q036) this plus
        // `r2rml.load_table` + `r2rml.read_metadata` accounts for the entire cold
        // wall — no data file is read. Allowlisted in `fluree-bench-virtual::spans`.
        let catalog_cache = self.catalog_disk_cache();
        let (data_files, has_delete_manifests) = if let Some(hit) =
            catalog_cache.get_count_stats(&metadata_location)
        {
            debug!(table_name = %table_name, "COUNT(*) manifest stats disk-cache hit");
            hit
        } else {
            let (data_files, _manifests_read, has_delete_manifests) =
                send_read_snapshot_data_files(storage.as_ref(), snapshot)
                    .instrument(tracing::debug_span!(
                        "r2rml.count_manifest_read",
                        table_name
                    ))
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!(
                            "Failed to read manifests for row count of '{table_name}': {e}"
                        ))
                    })?;
            catalog_cache.put_count_stats(&metadata_location, &data_files, has_delete_manifests);
            (data_files, has_delete_manifests)
        };

        let count =
            sound_manifest_row_count(schema, &data_files, has_delete_manifests, non_null_cols);
        // Recorded on the `r2rml.count_manifest` span wrapping this body.
        tracing::Span::current().record("fired", count.is_some());
        match count {
            Some(n) => debug!(
                table_name = %table_name,
                count = n,
                non_null_cols = non_null_cols.len(),
                "COUNT(*) manifest shortcut: answered from manifest record_count sum"
            ),
            None => debug!(
                table_name = %table_name,
                has_delete_manifests,
                "COUNT(*) manifest shortcut declined; falling back to scan"
            ),
        }
        Ok(count)
    }

    /// Whether `table_name` is already resolved (with unexpired credentials) in
    /// this query's session pin, so [`R2rmlTableProvider::prefetch_tables`] can
    /// skip re-warming it. A name that fails to parse is reported as NOT pinned so
    /// prefetch still attempts it and the real scan surfaces any error.
    fn is_table_pinned(&self, graph_source_id: &str, table_name: &str) -> bool {
        use fluree_db_iceberg::catalog::parse_table_identifier;
        let Ok(id) = parse_table_identifier(table_name) else {
            return false;
        };
        let key = super::catalog_session::IcebergCatalogSession::load_table_key(
            graph_source_id,
            &id.namespace,
            &id.table,
        );
        self.session.is_pinned(&key)
    }

    /// The persistent on-disk catalog cache (PR-8 slice 2), rooted in a dedicated
    /// dir sibling to this instance's Parquet/binary artifact cache so the cold
    /// benchmark protocol can clear data while keeping catalog persistence. Cheap
    /// to build per call (a `create_dir_all` that no-ops once the dir exists).
    fn catalog_disk_cache(&self) -> super::disk_catalog_cache::DiskCatalogCache {
        let artifact_dir = self.fluree.binary_store_cache_dir();
        super::disk_catalog_cache::DiskCatalogCache::for_dir(
            &super::disk_catalog_cache::catalog_cache_dir(&artifact_dir),
        )
    }

    /// Resolve a graph source down to its pinned Iceberg table context: the S3
    /// storage, the (metadata-location-pinned) [`TableMetadata`], and that metadata
    /// location. Shared by [`Self::scan_table_inner`] and
    /// [`R2rmlTableProvider::table_row_count`] so a `COUNT` and a scan in the same
    /// query read ONE Iceberg snapshot — the whole `loadTable` resolution (the
    /// per-query snapshot pin in [`super::catalog_session::IcebergCatalogSession`]
    /// plus the cross-query / metadata caches) runs here, through the shared
    /// `self.session`, exactly as the scan did before this was extracted. It
    /// excludes the scan-only concerns — the "Starting Iceberg table scan" log and
    /// the Parquet disk cache — which stay in `scan_table_inner`.
    async fn load_table_context(
        &self,
        graph_source_id: &str,
        table_name: &str,
    ) -> QueryResult<(Arc<S3IcebergStorage>, Arc<TableMetadata>, String)> {
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
                let client_fp = rest_client_cache_key(graph_source_id, &record.config);
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
                        // The cold REST/OAuth catalog round-trip (~1-3s) — the
                        // highest-value span for attributing a slow virtual-dataset
                        // query to cold-remote-retrieval vs. caching vs. decode.
                        let actual = catalog
                            .load_table(&table_id, iceberg_config.io.vended_credentials)
                            .instrument(tracing::debug_span!(
                                "r2rml.load_table",
                                namespace = %table_id.namespace,
                                table = %table_id.table,
                            ))
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

                // GCS-backed tables (S3-interop endpoint) are read through this
                // same S3 SDK path; the SDK client is pinned to HTTP/1.1 so the
                // GCS HTTP/2 range-read bug cannot occur.
                //
                // Reuse the query session's cached S3 client for this table when
                // one is present: constructing it (`aws_config` load + S3 client +
                // HTTP client) is not free, and a correlated join — or the slice-1
                // prefetch→scan — resolves the same table repeatedly. Any fresh
                // loadTable above dropped the entry via `store_load_table`, so a hit
                // here always corresponds to the current pinned credentials.
                let storage = if let Some(cached) = self.session.cached_storage(&lt_key) {
                    debug!(namespace = %table_id.namespace, table = %table_id.table,
                        "S3 storage client reused (query-scoped)");
                    cached
                } else {
                    let built = if let Some(ref credentials) = load_response.credentials {
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
                    let built = Arc::new(built);
                    self.session
                        .store_storage(lt_key.clone(), Arc::clone(&built));
                    built
                };

                (load_response, storage)
            }
            CatalogConfig::Direct { table_location } => {
                info!(
                    table_location = %table_location,
                    "Loading table via direct S3 access"
                );

                // Direct mode: create storage once, share via Arc. gs://-backed
                // tables (GCS S3-interop endpoint) are read through the same S3
                // SDK path; the client is pinned to HTTP/1.1 to avoid the AWS-SDK
                // HTTP/2 range-read bug against that endpoint.
                let storage: Arc<S3IcebergStorage> = Arc::new(
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
                        metadata: None,
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

        // Check cache for table metadata
        let cache = self.fluree.r2rml_cache();
        let metadata_location = &load_response.metadata_location;

        let metadata = if let Some(cached) = cache.get_metadata(metadata_location).await {
            debug!(metadata_location = %metadata_location, "Table metadata cache hit");
            cached
        } else {
            debug!(metadata_location = %metadata_location, "Table metadata cache miss");

            // PR-8 slice 2: on the in-memory miss, try the persistent disk catalog
            // cache before hitting S3. `metadata_location` is content-addressed, so
            // a hit is always current for that snapshot. A cold process with a warm
            // catalog dir serves the parsed metadata from local disk (no S3 GET,
            // no `r2rml.read_metadata` span).
            let catalog_cache = self.catalog_disk_cache();
            let metadata = if let Some(disk) = catalog_cache.get_metadata(metadata_location) {
                debug!(metadata_location = %metadata_location, "Table metadata disk-cache hit");
                disk
            } else {
                // Measurement sub-span (PR-8 cold decomposition): isolate the
                // metadata-JSON S3 GET + parse — the `load_table_context` component
                // between the loadTable REST GET (`r2rml.load_table`) and the
                // manifest read (`iceberg.scan_plan` / `r2rml.count_manifest_read`).
                // Allowlisted in `fluree-bench-virtual::spans`.
                let metadata = async {
                    let metadata_bytes =
                        storage
                            .as_ref()
                            .read(metadata_location)
                            .await
                            .map_err(|e| {
                                QueryError::Internal(format!("Failed to read table metadata: {e}"))
                            })?;
                    let parsed = TableMetadata::from_json(&metadata_bytes).map_err(|e| {
                        QueryError::Internal(format!("Failed to parse table metadata: {e}"))
                    })?;
                    Ok::<_, QueryError>(Arc::new(parsed))
                }
                .instrument(tracing::debug_span!(
                    "r2rml.read_metadata",
                    metadata_location = %metadata_location,
                ))
                .await?;
                catalog_cache.put_metadata(metadata_location, metadata.as_ref());
                metadata
            };
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
        Ok((storage, metadata, load_response.metadata_location.clone()))
    }

    /// Inner implementation of [`R2rmlTableProvider::scan_table`], split out so the
    /// trait method can wrap the setup in an `r2rml.scan_table` timing span via
    /// `.instrument()` (the codebase's established pattern for timing an async body
    /// without holding a span guard across an `.await`). The shared `loadTable`
    /// resolution (and its per-query snapshot pin) lives in
    /// [`Self::load_table_context`]; this adds the scan-only concerns — the
    /// scan-start log and the Parquet disk cache — and the streaming scan plan.
    async fn scan_table_inner(
        &self,
        graph_source_id: &str,
        table_name: &str,
        projection: &[String],
        filters: &[ScanFilter],
        topk: Option<&ScanTopK>,
        _as_of_t: Option<i64>,
    ) -> QueryResult<ColumnBatchStream> {
        // GREP: r2rml-as-of-t — time-travel is not implemented for Iceberg scans;
        // `_as_of_t` is deliberately ignored. If as-of semantics ever land here,
        // `table_row_count_inner` MUST honor them identically (matching breadcrumb
        // there): a COUNT and a scan in one query must read the same snapshot.
        info!(
            graph_source_id = %graph_source_id,
            table_name = %table_name,
            projection = ?projection,
            "Starting Iceberg table scan"
        );

        // Resolve the pinned table context (S3 storage + the snapshot-pinned
        // metadata) shared with the COUNT(*) manifest shortcut, so a count and a
        // scan in one query read the same pinned Iceberg snapshot.
        let (storage, metadata, metadata_location) =
            self.load_table_context(graph_source_id, table_name).await?;

        // Shared on-disk cache for data files (one global byte budget, deduped per
        // directory). Threaded into the Parquet readers, which apply a
        // whole-file-vs-range policy per file based on how much each query reads.
        // Scan-only: the COUNT shortcut reads no data files, so it never builds it.
        let cache_dir = self.fluree.binary_store_cache_dir();
        let disk_cache = fluree_db_iceberg::DiskArtifactCache::for_dir(&cache_dir);

        let cache = self.fluree.r2rml_cache();

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
        let (tasks, files_selected, files_pruned, estimated_row_count) = if let Some(filter) =
            &filter_expr
        {
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
        } else if let Some(cached) = cache.get_scan_files(&metadata_location).await {
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
        } else if let Some(disk) = self.catalog_disk_cache().get_scan_files(&metadata_location) {
            // PR-8 slice 2: in-memory miss, but the persistent disk catalog
            // cache has this snapshot's (unfiltered) file list — a warm-catalog
            // cold process skips the manifest read (`iceberg.scan_plan`). Rebuild
            // tasks from the file list exactly as the in-memory-hit arm does, and
            // populate the in-memory cache for the rest of the process.
            debug!(
                metadata_location = %metadata_location,
                cached_files = disk.data_files.len(),
                "Iceberg scan-files disk-cache hit"
            );
            cache
                .put_scan_files(metadata_location.clone(), Arc::clone(&disk))
                .await;
            let tasks = disk
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
                disk.files_selected,
                disk.files_pruned,
                disk.estimated_row_count,
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
            // Persist to the disk catalog cache (content-addressed, immutable).
            self.catalog_disk_cache()
                .put_scan_files(&metadata_location, &cached);

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

        // PR-5 scan-side top-k. When a resolvable single-column DESC directive is
        // present, read files in `upper_bound(sort_col)`-DESC order with a running
        // k-th bound and stop once no unread file can beat it — streaming a strict
        // SUPERSET of the top-k (the `SortOperator` above is authoritative). The
        // pruned subset MUST bypass the operator's scan cache (handled by its
        // `cacheable` guard gaining `&& topk.is_none()`); the disk *artifact* cache
        // is keyed by file path+size with whole-file entries, so a pruned subset
        // never poisons it. Falls through to the parallel path if the sort column
        // is unresolvable. Sequential reads are bounded by `TOPK_SEQUENTIAL_CAP`:
        // if the prune is ineffective (adversarial layout / all files tie at the
        // bound / a heap that can't fill), the remaining files are handed to the
        // normal parallel reader so the topk path can never be slower than it.
        if let Some(tk) = topk {
            if let Some(field) = schema.field_by_name(&tk.sort_column) {
                let sort_field_id = field.id;
                let sort_type = field.type_string().map(str::to_string);
                let order = plan_topk_read(
                    tasks.iter().map(|t| &t.data_file),
                    sort_field_id,
                    sort_type.as_deref(),
                );

                let mut bound = TopKBound::new(tk.k);
                let mut collected: Vec<ColumnBatch> = Vec::new();
                let mut tail: Vec<FileScanTask> = Vec::new();
                let mut reads = 0usize;
                for pos in 0..order.len() {
                    if pos >= TOPK_SEQUENTIAL_CAP {
                        // Prune ineffective after the cap — finish in parallel so
                        // the topk path is never slower than the full parallel scan.
                        tail = order[pos..]
                            .iter()
                            .map(|(orig, _)| tasks[*orig].clone())
                            .collect();
                        break;
                    }
                    let (orig, _) = order[pos];
                    let read_span = tracing::debug_span!(
                        "iceberg.parquet_read",
                        path = %tasks[orig].data_file.file_path,
                        file_size = tasks[orig].data_file.file_size_in_bytes,
                    );
                    let batches = SendParquetReader::with_caches(
                        storage.as_ref(),
                        footers.as_ref(),
                        &disk_cache,
                        &cache_dir,
                    )
                    .read_task(&tasks[orig])
                    .instrument(read_span)
                    .await
                    .map_err(|e| {
                        QueryError::Internal(format!(
                            "Failed to read Parquet file '{}': {e}",
                            tasks[orig].data_file.file_path
                        ))
                    })?;
                    // SOUNDNESS INVARIANT: the heap is fed the sort values of the
                    // rows this scan EMITS — which are the QUALIFYING result rows
                    // (post any pushed row filter). The directive is declined
                    // upstream (`resolve_topk_directive`) whenever a RESIDUAL filter
                    // the operator enforces after this scan is present, because
                    // feeding pre-filter values would ride the k-th bound too high
                    // and prune files whose qualifying rows belong in the true
                    // top-k. Never feed a superset of the qualifying rows here.
                    for b in &batches {
                        bound.observe_all(batch_sort_values(b, sort_field_id));
                    }
                    collected.extend(batches);
                    reads += 1;
                    // Stop iff the heap is full and the NEXT (highest-remaining)
                    // file's upper_bound is strictly below the k-th (over-keep on
                    // ties; a no-bound next → never stops). See `TopKBound::can_stop`.
                    if let Some((_, next_upper)) = order.get(pos + 1) {
                        if bound.can_stop(next_upper.as_ref()) {
                            break;
                        }
                    }
                }

                // Report the topk file selection through the SAME span the bench
                // harness sums (`iceberg.scan_plan`) — the planner's span does not
                // fire on this path, so without this the `files_selected` /
                // `files_pruned` counters would read 0. `files_selected` is the
                // files actually read (the sequential prefix plus any parallel
                // tail); the rest were provably unable to beat the k-th bound.
                let files_selected = reads + tail.len();
                let files_pruned = order.len().saturating_sub(files_selected);
                tracing::debug_span!(
                    "iceberg.scan_plan",
                    files_selected = files_selected as u64,
                    files_pruned = files_pruned as u64,
                )
                .in_scope(|| {});
                debug!(
                    files_selected,
                    files_pruned,
                    total_files = order.len(),
                    k = tk.k,
                    "scan-side top-k prune"
                );
                let prefix = futures::stream::iter(collected.into_iter().map(Ok));
                if tail.is_empty() {
                    return Ok(Box::pin(prefix));
                }
                // Parallel fallback tail (same bounded-parallel read as the normal
                // path). The bound still holds; we just stop paying sequentiality.
                let concurrency = iceberg_scan_concurrency(tail.len());
                let tail_stream =
                    futures::stream::iter(tail)
                        .map(move |task| {
                            let storage = Arc::clone(&storage);
                            let footers = Arc::clone(&footers);
                            let disk_cache = Arc::clone(&disk_cache);
                            let cache_dir = cache_dir.clone();
                            let read_span = tracing::debug_span!(
                                "iceberg.parquet_read",
                                path = %task.data_file.file_path,
                                file_size = task.data_file.file_size_in_bytes,
                            );
                            async move {
                                tokio::spawn(async move {
                                    let reader = SendParquetReader::with_caches(
                                        storage.as_ref(),
                                        footers.as_ref(),
                                        &disk_cache,
                                        &cache_dir,
                                    );
                                    reader.read_task(&task).instrument(read_span).await.map_err(
                                        |e| {
                                            QueryError::Internal(format!(
                                                "Failed to read Parquet file '{}': {e}",
                                                task.data_file.file_path
                                            ))
                                        },
                                    )
                                })
                                .await
                                .map_err(|e| {
                                    QueryError::Internal(format!("Parquet read worker failed: {e}"))
                                })?
                            }
                        })
                        .buffer_unordered(concurrency)
                        .flat_map(|res: QueryResult<Vec<ColumnBatch>>| match res {
                            Ok(batches) => futures::stream::iter(
                                batches.into_iter().map(Ok).collect::<Vec<_>>(),
                            ),
                            Err(e) => futures::stream::iter(vec![Err(e)]),
                        });
                return Ok(Box::pin(prefix.chain(tail_stream)));
            }
        }

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
                // Create the per-file span HERE, before `tokio::spawn`, so it is
                // parented under the consumer's current span: `tokio::spawn` does
                // NOT propagate the current span into the spawned task, but a span
                // records its parent at creation time. Instrumenting the read
                // future inside the task then times the actual read+decode while
                // keeping the correct parent (and gives each concurrent read a
                // distinct span, respecting the `buffer_unordered` fan-out).
                let read_span = tracing::debug_span!(
                    "iceberg.parquet_read",
                    path = %task.data_file.file_path,
                    file_size = task.data_file.file_size_in_bytes,
                );
                async move {
                    tokio::spawn(async move {
                        let reader = SendParquetReader::with_caches(
                            storage.as_ref(),
                            footers.as_ref(),
                            &disk_cache,
                            &cache_dir,
                        );
                        reader
                            .read_task(&task)
                            .instrument(read_span)
                            .await
                            .map_err(|e| {
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

/// Decide whether a pinned snapshot's manifest `record_count` sum is a sound
/// answer to a bare `COUNT(*)`, and if so return it. Pure over the manifest read
/// result (no I/O), so the soundness gates are unit-tested directly against
/// hand-built [`fluree_db_iceberg::DataFile`]s.
///
/// Returns `Some(n)` only when both hold:
/// 1. the snapshot has **no delete manifests** — a merge-on-read position/equality
///    delete would make the `record_count` sum an over-count; and
/// 2. **every** `non_null_col` is provably zero-null from the manifest stats.
///    `aggregate_column_stats`' coverage gate makes `null_count` `Some(0)` only
///    when EVERY data file reported a null count for the column and they sum to
///    zero; an absent or partially-covered stat is `None` (unknown) and a positive
///    count is `Some(n>0)` — both decline. An unknown null count is **never**
///    treated as zero. A column absent from the schema is likewise unproven.
/// 3. the per-file `record_count`s are well-formed — a negative per-file count,
///    or a sum that would overflow `u64` (both only possible in a corrupt
///    manifest), declines rather than serving a wrapped/bogus "exact" count.
///
/// An empty `non_null_cols` is a constant-subject mapping (a row is produced for
/// every table row), so the count is sound with no null proof required.
fn sound_manifest_row_count(
    schema: &fluree_db_iceberg::metadata::Schema,
    data_files: &[fluree_db_iceberg::DataFile],
    has_delete_manifests: bool,
    non_null_cols: &[String],
) -> Option<u64> {
    if has_delete_manifests {
        return None;
    }
    let agg = aggregate_column_stats(data_files, schema);
    for col in non_null_cols {
        let field = schema.field_by_name(col)?;
        match agg.columns.get(&field.id).and_then(|c| c.null_count) {
            Some(0) => {}
            _ => return None,
        }
    }
    // `record_count` is non-negative in real Iceberg metadata; a corrupt manifest
    // must decline rather than feed a bogus "exact" count. Re-summed here with
    // per-file checked u64 arithmetic instead of trusting `agg.row_count`, whose
    // plain i64 sum saturates on corrupt input and cannot distinguish a per-file
    // negative from a smaller valid total: a negative per-file count, or a sum
    // that would overflow `u64`, declines.
    let mut total: u64 = 0;
    for df in data_files {
        total = total.checked_add(u64::try_from(df.record_count).ok()?)?;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_iceberg::metadata::{Schema, SchemaField};
    use fluree_db_query::r2rml::{ScanCmpOp, ScanFilter, ScanValue};
    use serde_json::json;

    fn field(id: i32, name: &str, ty: serde_json::Value) -> SchemaField {
        SchemaField {
            id,
            name: name.to_string(),
            required: false,
            field_type: ty,
            doc: None,
        }
    }

    fn key_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: vec![
                field(1, "int_key", json!("int")),
                field(2, "long_key", json!("long")),
                field(3, "dec_key", json!("decimal(38,0)")),
                field(4, "str_key", json!("string")),
                field(5, "date_key", json!("date")),
                field(6, "double_key", json!("double")),
                field(7, "float_key", json!("float")),
            ],
        }
    }

    fn key_filter(col: &str, raw: &str) -> ScanFilter {
        ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Eq,
            value: ScanValue::TemplateKey(raw.to_string()),
        }
    }

    fn only_literal(filters: &[ScanFilter], schema: &Schema) -> Option<LiteralValue> {
        match build_iceberg_filter(filters, schema)? {
            Expression::Comparison { value, .. } => Some(value),
            other => panic!("expected a single comparison, got {other:?}"),
        }
    }

    #[test]
    fn template_key_coerces_by_physical_type() {
        let s = key_schema();
        assert!(matches!(
            only_literal(&[key_filter("int_key", "5")], &s),
            Some(LiteralValue::Int32(5))
        ));
        assert!(matches!(
            only_literal(&[key_filter("long_key", "5")], &s),
            Some(LiteralValue::Int64(5))
        ));
        // Integer key on a Decimal column pushes as Int64 — the Arrow reader casts
        // it to the Decimal column (the validated integer-vs-decimal path).
        assert!(matches!(
            only_literal(&[key_filter("dec_key", "5")], &s),
            Some(LiteralValue::Int64(5))
        ));
        // The raw string is already percent-decoded upstream.
        assert!(matches!(
            only_literal(&[key_filter("str_key", "west/5")], &s),
            Some(LiteralValue::String(v)) if v == "west/5"
        ));
    }

    #[test]
    fn date_scalar_pushed_only_against_date_column() {
        let s = key_schema();
        let date_filter = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Eq,
            value: ScanValue::Date(19_737), // 2024-01-15, days since epoch
        };
        // Physically-`date` column: the scan filter compares like the operator.
        assert!(matches!(
            only_literal(&[date_filter("date_key")], &s),
            Some(LiteralValue::Date(19_737))
        ));
        // Physically-`string` column: skip. The operator's lenient `Date::parse`
        // keeps `"2024-01-15Z"`/offset forms that the exact row filter (Date32 →
        // Utf8 `"2024-01-15"`) would drop — pushing here would remove an
        // operator-kept row.
        assert!(build_iceberg_filter(&[date_filter("str_key")], &s).is_none());
    }

    #[test]
    fn double_pushed_only_against_double_column() {
        let s = key_schema();
        let dbl = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Lt,
            value: ScanValue::Double(9.99),
        };
        // Physically-`double`: pushes as an exact f64 bound.
        assert!(matches!(
            only_literal(&[dbl("double_key")], &s),
            Some(LiteralValue::Float64(v)) if v == 9.99
        ));
        // Physically-`float`: skipped (an f64→f32 narrowing could round the
        // literal and over-prune a range).
        assert!(build_iceberg_filter(&[dbl("float_key")], &s).is_none());
        // Non-numeric column: skipped.
        assert!(build_iceberg_filter(&[dbl("str_key")], &s).is_none());
    }

    #[test]
    fn int_literal_coerces_to_scale0_decimal_only_when_numeric_stats_on() {
        // On: an integer against a decimal column → EXACT scale-0 decimal (prunable).
        assert!(matches!(
            int_pushdown_literal(1_000_000, Some("decimal(38,2)"), true),
            Some(LiteralValue::Decimal {
                unscaled: 1_000_000,
                scale: 0,
                ..
            })
        ));
        // Off (revert guarantee): stays Int64 → the decimal bound compare declines
        // → no prune, exactly the pre-PR-7 behavior.
        assert!(matches!(
            int_pushdown_literal(1_000_000, Some("decimal(38,2)"), false),
            Some(LiteralValue::Int64(1_000_000))
        ));
        // An `int` column narrows to Int32; an out-of-range literal skips (no wrap).
        assert!(matches!(
            int_pushdown_literal(5, Some("int"), true),
            Some(LiteralValue::Int32(5))
        ));
        assert!(int_pushdown_literal(i64::from(i32::MAX) + 1, Some("int"), true).is_none());
        // `long` / other columns: Int64 unchanged, on or off.
        assert!(matches!(
            int_pushdown_literal(5, Some("long"), true),
            Some(LiteralValue::Int64(5))
        ));
        assert!(matches!(
            int_pushdown_literal(5, Some("long"), false),
            Some(LiteralValue::Int64(5))
        ));
    }

    #[test]
    fn int_scalar_against_decimal_column_pushes_scale0_decimal() {
        // End-to-end through build_iceberg_filter with the default (on) switch: an
        // integer FILTER literal on a decimal column becomes a scale-0 decimal.
        let s = key_schema();
        let f = ScanFilter {
            column: "dec_key".to_string(),
            op: ScanCmpOp::Gt,
            value: ScanValue::Int(1_000_000),
        };
        assert!(matches!(
            only_literal(&[f], &s),
            Some(LiteralValue::Decimal {
                unscaled: 1_000_000,
                scale: 0,
                ..
            })
        ));
    }

    #[test]
    fn decimal_pushed_only_against_decimal_column_preserving_literal_scale() {
        let s = key_schema();
        // The `ScanValue::Decimal` carries the LITERAL's scale (9.99 → scale 2);
        // the column is decimal(38,0). The bridge preserves the literal scale —
        // the bound compare normalizes across the column/literal scale gap.
        let dec = |col: &str| ScanFilter {
            column: col.to_string(),
            op: ScanCmpOp::Lt,
            value: ScanValue::Decimal {
                unscaled: 999,
                precision: 3,
                scale: 2,
            },
        };
        assert!(matches!(
            only_literal(&[dec("dec_key")], &s),
            Some(LiteralValue::Decimal {
                unscaled: 999,
                scale: 2,
                ..
            })
        ));
        // Non-decimal columns: skipped (no cross-type bound compare exists).
        assert!(build_iceberg_filter(&[dec("long_key")], &s).is_none());
        assert!(build_iceberg_filter(&[dec("str_key")], &s).is_none());
    }

    #[test]
    fn template_key_skips_unsupported_or_unparseable() {
        let s = key_schema();
        // Date physical type is not pushed yet (needs a live decimal/date check).
        assert!(build_iceberg_filter(&[key_filter("date_key", "2024-01-15")], &s).is_none());
        // Non-integer value against an integer column → skip (operator enforces).
        assert!(build_iceberg_filter(&[key_filter("int_key", "abc")], &s).is_none());
        assert!(build_iceberg_filter(&[key_filter("dec_key", "5.5")], &s).is_none());
        // Unknown column → skip.
        assert!(build_iceberg_filter(&[key_filter("nope", "5")], &s).is_none());
    }

    // ------------------------------------------------------------------
    // COUNT(*) manifest shortcut soundness (`sound_manifest_row_count`).
    // The decision core is pure over the manifest read result, so the gates
    // are exercised directly against hand-built DataFiles (the same fixture
    // style as `fluree_db_iceberg::stats` tests).
    // ------------------------------------------------------------------

    use fluree_db_iceberg::manifest::{DataFile, FileFormat, PartitionData};
    use std::collections::HashMap;

    fn count_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![1],
            fields: vec![
                field(1, "SALE_KEY", json!("long")),
                field(2, "AMOUNT", json!("decimal(18,2)")),
            ],
        }
    }

    /// A data file with `record_count` rows. `null_value_counts` = `Some(pairs)`
    /// makes the file report those per-field null counts; `None` makes it report
    /// no null counts at all (to simulate absent/partial coverage).
    fn count_data_file(record_count: i64, null_value_counts: Option<&[(i32, i64)]>) -> DataFile {
        DataFile {
            file_path: "s3://b/t/data/f.parquet".to_string(),
            file_format: FileFormat::Parquet,
            record_count,
            file_size_in_bytes: 1000,
            partition: PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: null_value_counts
                .map(|pairs| pairs.iter().copied().collect::<HashMap<i32, i64>>()),
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    #[test]
    fn count_shortcut_clean_table_returns_exact_count() {
        let schema = count_schema();
        // Two files; every required column reports zero nulls in EVERY file (full
        // coverage), so the record_count sum equals a full-scan COUNT.
        let files = vec![
            count_data_file(100, Some(&[(1, 0), (2, 0)])),
            count_data_file(200, Some(&[(1, 0), (2, 0)])),
        ];
        let cols = vec!["SALE_KEY".to_string(), "AMOUNT".to_string()];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &cols),
            Some(300)
        );
    }

    #[test]
    fn count_shortcut_declines_with_delete_manifests() {
        let schema = count_schema();
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 0)]))];
        // A merge-on-read delete manifest makes record_count an over-count.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, true, &["SALE_KEY".to_string()]),
            None
        );
    }

    #[test]
    fn count_shortcut_declines_nullable_column() {
        let schema = count_schema();
        // AMOUNT carries 5 nulls; a COUNT requiring AMOUNT non-null must not adopt
        // the manifest total (which counts those rows).
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 5)]))];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["AMOUNT".to_string()]),
            None
        );
        // Same table, but only the provably zero-null key is required → sound.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["SALE_KEY".to_string()]),
            Some(300)
        );
    }

    #[test]
    fn count_shortcut_declines_when_null_stats_absent() {
        let schema = count_schema();
        // Two files, but only one reports a null count for the key: partial
        // coverage → aggregate_column_stats yields null_count None (unknown),
        // which must NOT be read as zero.
        let files = vec![
            count_data_file(100, Some(&[(1, 0)])),
            count_data_file(200, None),
        ];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["SALE_KEY".to_string()]),
            None
        );
    }

    #[test]
    fn count_shortcut_constant_subject_needs_no_null_proof() {
        // Empty non_null_cols = constant-subject mapping: a row exists for every
        // table row, so the count is sound with no per-column null proof — even
        // when NO file reports any null stats.
        let schema = count_schema();
        let files = vec![count_data_file(100, None), count_data_file(200, None)];
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &[]),
            Some(300)
        );
        // A delete manifest still declines, even for a constant subject.
        assert_eq!(sound_manifest_row_count(&schema, &files, true, &[]), None);
    }

    #[test]
    fn count_shortcut_declines_corrupt_record_counts() {
        let schema = count_schema();
        // A negative per-file record_count (corrupt manifest) declines — even
        // though the SUM (10 - 5 = 5) is positive and a sign check on the
        // aggregate alone would have served it as an "exact" count.
        let files = vec![count_data_file(10, None), count_data_file(-5, None)];
        assert_eq!(sound_manifest_row_count(&schema, &files, false, &[]), None);

        // Per-file counts whose total overflows u64 decline (three i64::MAX
        // files: the wrapped i64 sum would land positive at ~2^63-3, so a plain
        // sign check on the aggregate would happily pass it).
        let files = vec![
            count_data_file(i64::MAX, None),
            count_data_file(i64::MAX, None),
            count_data_file(i64::MAX, None),
        ];
        assert_eq!(sound_manifest_row_count(&schema, &files, false, &[]), None);
    }

    #[test]
    fn count_shortcut_declines_unknown_column() {
        let schema = count_schema();
        let files = vec![count_data_file(300, Some(&[(1, 0), (2, 0)]))];
        // A required column absent from the schema cannot be proven non-null.
        assert_eq!(
            sound_manifest_row_count(&schema, &files, false, &["NOPE".to_string()]),
            None
        );
    }
}
