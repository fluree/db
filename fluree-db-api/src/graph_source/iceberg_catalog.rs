//! Read-only Iceberg catalog browse + metadata preview API (metadata-only).
//!
//! This module exposes the catalog-browse and metadata-preview surface that
//! feeds the deterministic R2RML generator (PR-1 item (c), a separate lane) and
//! the solo onboarding flow (PR-2). Everything here is **metadata-only**: browse
//! lists namespaces/tables via the REST catalog, Tier-A preview reads the inline
//! `metadata` object the REST `loadTable` response already carries (no S3), and
//! Tier-B preview aggregates per-column statistics from the snapshot's
//! manifest-list + manifest Avro files (never a Parquet/data file).
//!
//! All entry points accept an inline [`IcebergConnectionConfig`] so onboarding
//! can browse/preview **before** a graph source is saved.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::graph_source::config::{CatalogMode, IcebergConnectionConfig};
use crate::Result;

use fluree_db_iceberg::catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient};
use fluree_db_iceberg::io::batch::IcebergFieldTypeExt;
use fluree_db_iceberg::io::S3IcebergStorage;
use fluree_db_iceberg::metadata::{
    PartitionField, Schema, SchemaField, Snapshot, SortField, TableMetadata,
};
use fluree_db_iceberg::stats::{
    aggregate_column_stats, send_read_snapshot_data_files, AggregatedColumnStats,
};
use fluree_db_iceberg::FieldType;
// The emitter owns the canonical `FieldType → xsd:` map; the preview lane reuses
// it (single source of truth) rather than duplicating it here.
use fluree_db_r2rml::emit::naming::xsd_datatype;

// =============================================================================
// Shared identifiers
// =============================================================================

/// A table reference: catalog namespace + table name (byte-for-byte catalog
/// casing). Pinned shape `{ namespace, name }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// Catalog namespace (e.g. `"DW"`).
    pub namespace: String,
    /// Table name (e.g. `"DIM_STORE"`).
    pub name: String,
}

impl TableIdentifier {
    /// Construct a table identifier.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    /// The canonical `"NAMESPACE.NAME"` string (byte-for-byte catalog casing).
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// Convert to the iceberg-crate catalog identifier (`{ namespace, table }`).
    pub(crate) fn to_catalog(&self) -> fluree_db_iceberg::catalog::TableIdentifier {
        fluree_db_iceberg::catalog::TableIdentifier {
            namespace: self.namespace.clone(),
            table: self.name.clone(),
        }
    }
}

/// Alias for [`TableIdentifier`] — browse returns these under `tables`; the
/// shape is identical (`{ namespace, name }`).
pub type TableRef = TableIdentifier;

// =============================================================================
// (a) Browse
// =============================================================================

/// How deep a catalog browse should reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrowseDepth {
    /// List namespaces only.
    Namespaces,
    /// List namespaces and, for each, its tables.
    Tables,
}

/// The result of browsing a catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogBrowse {
    /// The catalog URI that was browsed.
    pub catalog_uri: String,
    /// The warehouse (if any) the browse was scoped to.
    pub warehouse: Option<String>,
    /// The namespaces discovered in the catalog.
    pub namespaces: Vec<String>,
    /// The tables discovered (empty when `depth = Namespaces`).
    pub tables: Vec<TableRef>,
}

/// Build a REST catalog client from a connection, or a clear typed error for
/// Direct mode (which has no catalog to browse/list).
fn rest_catalog_client(
    conn: &IcebergConnectionConfig,
    op: &str,
) -> Result<(RestCatalogClient, String, Option<String>)> {
    let rest = match &conn.catalog_mode {
        CatalogMode::Rest(rest) => rest,
        CatalogMode::Direct { .. } => {
            return Err(crate::ApiError::config(format!(
                "Direct catalog mode cannot be used for {op}: there is no REST catalog to query. \
                 Provide a REST connection (catalog_uri + auth)."
            )));
        }
    };

    let auth = rest
        .auth
        .create_provider_arc()
        .map_err(|e| crate::ApiError::config(format!("Failed to create auth provider: {e}")))?;

    let catalog_config = RestCatalogConfig {
        uri: rest.catalog_uri.clone(),
        warehouse: rest.warehouse.clone(),
        ..Default::default()
    };

    let catalog = RestCatalogClient::new(catalog_config, auth)
        .map_err(|e| crate::ApiError::config(format!("Failed to create catalog client: {e}")))?;

    Ok((catalog, rest.catalog_uri.clone(), rest.warehouse.clone()))
}

/// Browse an Iceberg REST catalog: list namespaces and, at `depth = Tables`,
/// the tables in each namespace.
///
/// **Metadata-only** and stateless — it needs no `Fluree` instance and touches
/// no S3. Direct catalog mode returns a clear [`crate::ApiError::Config`] (there
/// is nothing to browse).
pub async fn browse_iceberg_catalog(
    conn: IcebergConnectionConfig,
    depth: BrowseDepth,
) -> Result<CatalogBrowse> {
    let (catalog, catalog_uri, warehouse) = rest_catalog_client(&conn, "catalog browse")?;

    let namespaces = SendCatalogClient::list_namespaces(&catalog)
        .await
        .map_err(|e| crate::ApiError::config(format!("Failed to list namespaces: {e}")))?;

    let mut tables = Vec::new();
    if depth == BrowseDepth::Tables {
        for ns in &namespaces {
            let ns_tables = SendCatalogClient::list_tables(&catalog, ns)
                .await
                .map_err(|e| {
                    crate::ApiError::config(format!("Failed to list tables in namespace {ns}: {e}"))
                })?;
            for qualified in ns_tables {
                tables.push(split_qualified_table(ns, &qualified));
            }
        }
    }

    Ok(CatalogBrowse {
        catalog_uri,
        warehouse,
        namespaces,
        tables,
    })
}

/// Recover a `{ namespace, name }` ref from a queried namespace plus the
/// `"ns.table"`-style identifier the catalog returns. The queried namespace is
/// authoritative (namespaces can contain dots), so we strip its prefix; if the
/// entry does not carry the prefix we fall back to a last-segment split.
fn split_qualified_table(queried_ns: &str, qualified: &str) -> TableRef {
    if let Some(name) = qualified.strip_prefix(&format!("{queried_ns}.")) {
        return TableRef::new(queried_ns.to_string(), name.to_string());
    }
    match qualified.rsplit_once('.') {
        Some((ns, name)) => TableRef::new(ns.to_string(), name.to_string()),
        None => TableRef::new(queried_ns.to_string(), qualified.to_string()),
    }
}

impl crate::Fluree {
    /// Browse an Iceberg REST catalog (namespaces, and tables at
    /// `depth = Tables`). Convenience wrapper over the stateless
    /// [`browse_iceberg_catalog`] free function — browse needs no engine state.
    pub async fn browse_iceberg_catalog(
        &self,
        conn: IcebergConnectionConfig,
        depth: BrowseDepth,
    ) -> Result<CatalogBrowse> {
        browse_iceberg_catalog(conn, depth).await
    }
}

// =============================================================================
// (b) Metadata preview — Tier-A schema (this section) + Tier-B stats (aggregated
//     from manifests; see the ColumnStats wiring below).
// =============================================================================

/// Human-readable note attached to every Tier-B preview: NDV / distinct counts
/// are not derivable from Iceberg metadata alone (Puffin/theta-sketch reading is
/// deferred to PR-5), so `distinct_count` is always `None`.
pub(crate) const DISTINCT_COUNT_WARNING: &str =
    "distinct_count (NDV) is unavailable from metadata alone; it requires column \
     profiling and is deferred to PR-5.";

/// Which statistics tier a preview should compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StatsTier {
    /// Tier-A: schema only (columns/types/partition/sort/snapshot), from the
    /// inline REST `loadTable` metadata. No S3 reads.
    Schema,
    /// Tier-A + Tier-B: additionally aggregate per-column statistics from the
    /// snapshot's manifest-list + manifest Avro files (never a data file).
    Stats,
}

/// A reference to a table snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    /// Snapshot ID.
    pub id: i64,
    /// Snapshot creation timestamp (epoch millis).
    pub timestamp_ms: i64,
    /// The schema ID that was current at snapshot time.
    pub schema_id: Option<i32>,
}

/// A partition field, resolved to readable names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFieldInfo {
    /// Partition field name.
    pub name: String,
    /// The source column the partition transform is applied to.
    pub source_field: String,
    /// The transform (`identity`, `bucket[N]`, `day`, `month`, …).
    pub transform: String,
}

/// A sort field, resolved to a readable column name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortFieldInfo {
    /// The sorted column name.
    pub field: String,
    /// Sort direction (`asc` / `desc`).
    pub direction: String,
    /// Null ordering (`nulls-first` / `nulls-last`).
    pub null_order: String,
}

/// Per-column statistics aggregated from manifests (Tier-B). Every field is
/// best-effort — a stat is `None` when the manifests do not carry it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Number of null values across the snapshot's data files.
    pub null_count: Option<i64>,
    /// Number of values (including nulls) across the snapshot's data files.
    pub value_count: Option<i64>,
    /// `null_count / value_count`, when both are known and `value_count > 0`.
    pub null_fraction: Option<f64>,
    /// Number of NaN values (float/double columns only).
    pub nan_count: Option<i64>,
    /// Column-wide minimum (value_codec-decoded lower bound, JSON-rendered).
    pub min: Option<serde_json::Value>,
    /// Column-wide maximum (value_codec-decoded upper bound, JSON-rendered).
    pub max: Option<serde_json::Value>,
    /// On-disk size in bytes for this column across the snapshot's data files.
    pub on_disk_bytes: Option<i64>,
    /// Distinct value count — ALWAYS `None` in Phase-1 (NDV deferred to PR-5).
    pub distinct_count: Option<i64>,
}

/// A single column of a table, with type mapping and optional statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Iceberg field ID (canonical).
    pub field_id: i32,
    /// Byte-for-byte Iceberg field name.
    pub name: String,
    /// Iceberg type string (`"long"`, `"decimal(18, 2)"`, `"struct"`, …).
    pub iceberg_type: String,
    /// Parsed [`FieldType`] (via `IcebergFieldTypeExt`); `None` for nested types.
    #[serde(with = "field_type_serde", default)]
    pub field_type: Option<FieldType>,
    /// The emitter's chosen `xsd:` datatype CURIE; `None` for string/nested.
    ///
    /// Pinned at `xsd_long_as_integer = true` (the reference convention); a
    /// generate call overriding that to `false` makes this hint differ from the
    /// emitted datatype for `Int32`/`Int64` columns.
    pub xsd_type: Option<String>,
    /// Whether the column is required (non-nullable) per the schema.
    pub required: bool,
    /// Whether the column is a nested type (struct/list/map).
    pub nested: bool,
    /// Column documentation, if any.
    pub doc: Option<String>,
    /// Per-column statistics; present only for `tier = Stats`.
    pub stats: Option<ColumnStats>,
}

/// The schema (and table-level metadata) of a table, Tier-A.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// `"NAMESPACE.NAME"` (byte-for-byte catalog casing).
    pub table: String,
    /// Table UUID, if the metadata carries one.
    pub table_uuid: Option<String>,
    /// Iceberg format version (1 or 2).
    pub format_version: i32,
    /// The current schema ID.
    pub current_schema_id: i32,
    /// The current snapshot.
    pub snapshot: SnapshotRef,
    /// Authoritative row count from the snapshot summary.
    pub row_count: Option<i64>,
    /// Authoritative data-file count from the snapshot summary.
    pub data_file_count: Option<i64>,
    /// Authoritative on-disk byte count from the snapshot summary.
    pub total_bytes: Option<i64>,
    /// Iceberg row-identity hint (equality-delete identity) — the primary PK signal.
    pub identifier_field_ids: Vec<i32>,
    /// The default partition spec, resolved to readable names.
    pub partition_spec: Vec<PartitionFieldInfo>,
    /// The default sort order, resolved to readable names.
    pub sort_order: Vec<SortFieldInfo>,
    /// Table properties.
    pub properties: HashMap<String, String>,
    /// Columns in schema order.
    pub columns: Vec<ColumnInfo>,
    /// Snapshot history (additive beyond the pinned `snapshot`; scope item 3
    /// asks for snapshot history alongside the current snapshot).
    pub snapshot_log: Vec<SnapshotRef>,
}

/// How complete the statistics in a preview are.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsCompleteness {
    /// The tier that was computed (`"schema"` or `"stats"`).
    pub tier: String,
    /// Number of manifest files read (0 for Tier-A / Schema).
    pub manifests_read: usize,
    /// Whether any column carried lower/upper bounds in the manifests read.
    pub had_column_bounds: bool,
}

/// The full preview of a table: schema, statistics completeness, warnings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePreview {
    /// The table schema (+ per-column stats when `tier = Stats`).
    pub schema: TableSchema,
    /// How complete the statistics are.
    pub stats_completeness: StatsCompleteness,
    /// Non-fatal warnings (e.g. `distinct_count` unavailable).
    pub warnings: Vec<String>,
}

/// Canonical Iceberg type string for a [`FieldType`], round-trippable through
/// `FieldType::from_iceberg_type` (used for wire serialization of `field_type`).
fn field_type_to_iceberg_string(field_type: FieldType) -> String {
    match field_type {
        FieldType::Boolean => "boolean".to_string(),
        FieldType::Int32 => "int".to_string(),
        FieldType::Int64 => "long".to_string(),
        FieldType::Float32 => "float".to_string(),
        FieldType::Float64 => "double".to_string(),
        FieldType::String => "string".to_string(),
        FieldType::Bytes => "binary".to_string(),
        FieldType::Date => "date".to_string(),
        FieldType::Timestamp => "timestamp".to_string(),
        FieldType::TimestampTz => "timestamptz".to_string(),
        FieldType::Decimal { precision, scale } => format!("decimal({precision}, {scale})"),
    }
}

/// Serde adapter so `Option<FieldType>` (which is not itself `Serialize`) rides
/// the wire as its Iceberg type string (`"long"`, `"decimal(18, 2)"`, …).
mod field_type_serde {
    use super::{field_type_to_iceberg_string, FieldType, IcebergFieldTypeExt};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        field_type: &Option<FieldType>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match field_type {
            Some(ft) => serializer.serialize_some(&field_type_to_iceberg_string(*ft)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<FieldType>, D::Error> {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        Ok(opt.and_then(|s| FieldType::from_iceberg_type(&s)))
    }
}

/// The Iceberg type string for a field: the primitive type for scalars, or the
/// nested `type` tag (`struct` / `list` / `map`) for nested columns.
fn iceberg_type_string(field: &SchemaField) -> String {
    match field.type_string() {
        Some(s) => s.to_string(),
        None => field
            .field_type
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("nested")
            .to_string(),
    }
}

/// Build the Tier-A [`ColumnInfo`] for a schema field (no statistics).
fn column_info_tier_a(field: &SchemaField) -> ColumnInfo {
    let nested = field.is_nested();
    let field_type = if nested {
        None
    } else {
        field.type_string().and_then(FieldType::from_iceberg_type)
    };
    // Canonical emitter map, pinned at `xsd_long_as_integer = true` (see the
    // `ColumnInfo::xsd_type` doc for the generate-override caveat).
    let xsd_type = field_type
        .and_then(|ft| xsd_datatype(ft, true))
        .map(str::to_string);
    ColumnInfo {
        field_id: field.id,
        name: field.name.clone(),
        iceberg_type: iceberg_type_string(field),
        field_type,
        xsd_type,
        required: field.required,
        nested,
        doc: field.doc.clone(),
        stats: None,
    }
}

fn snapshot_ref(snapshot: &Snapshot) -> SnapshotRef {
    SnapshotRef {
        id: snapshot.snapshot_id,
        timestamp_ms: snapshot.timestamp_ms,
        schema_id: snapshot.schema_id,
    }
}

fn partition_field_info(pf: &PartitionField, schema: &Schema) -> PartitionFieldInfo {
    PartitionFieldInfo {
        name: pf.name.clone(),
        source_field: schema
            .field(pf.source_id)
            .map_or_else(|| pf.source_id.to_string(), |f| f.name.clone()),
        transform: pf.transform.clone(),
    }
}

fn sort_field_info(sf: &SortField, schema: &Schema) -> SortFieldInfo {
    SortFieldInfo {
        field: schema
            .field(sf.source_id)
            .map_or_else(|| sf.source_id.to_string(), |f| f.name.clone()),
        direction: sf.direction.clone(),
        null_order: sf.null_order.clone(),
    }
}

/// Build the Tier-A [`TableSchema`] from retained inline table metadata. Pure —
/// no I/O — so it is exercised offline over a metadata fixture.
pub(crate) fn table_schema_from_metadata(
    table: &TableIdentifier,
    metadata: &TableMetadata,
) -> Result<TableSchema> {
    let schema = metadata
        .current_schema()
        .ok_or_else(|| crate::ApiError::config("Table metadata has no current schema"))?;

    let current_snapshot = metadata.current_snapshot();
    let snapshot = match current_snapshot {
        Some(s) => snapshot_ref(s),
        None => SnapshotRef {
            id: metadata.current_snapshot_id.unwrap_or_default(),
            timestamp_ms: metadata.last_updated_ms,
            schema_id: Some(metadata.current_schema_id),
        },
    };

    let (row_count, data_file_count, total_bytes) = current_snapshot.map_or((None, None, None), |s| {
        (s.total_records(), s.total_data_files(), s.total_files_size())
    });

    let partition_spec = metadata.default_partition_spec().map_or_else(Vec::new, |spec| {
        spec.fields
            .iter()
            .map(|pf| partition_field_info(pf, schema))
            .collect()
    });

    let sort_order = metadata
        .sort_orders
        .iter()
        .find(|so| so.order_id == metadata.default_sort_order_id)
        .map_or_else(Vec::new, |so| {
            so.fields
                .iter()
                .map(|sf| sort_field_info(sf, schema))
                .collect()
        });

    Ok(TableSchema {
        table: table.qualified(),
        table_uuid: metadata.table_uuid.clone(),
        format_version: metadata.format_version,
        current_schema_id: metadata.current_schema_id,
        snapshot,
        row_count,
        data_file_count,
        total_bytes,
        identifier_field_ids: schema.identifier_field_ids.clone(),
        partition_spec,
        sort_order,
        properties: metadata.properties.clone(),
        columns: schema.fields.iter().map(column_info_tier_a).collect(),
        snapshot_log: metadata.snapshots.iter().map(snapshot_ref).collect(),
    })
}

/// Preview an Iceberg table's schema (Tier-A) and, at `tier = Stats`, its
/// per-column statistics (Tier-B).
///
/// **Metadata-only**: Tier-A reads the inline REST `loadTable` metadata (no S3);
/// Tier-B additionally reads the snapshot's manifest-list + manifest Avro files
/// (never a Parquet/data file). Direct catalog mode and a catalog that omits the
/// inline metadata both return a clear typed error.
pub async fn preview_iceberg_table(
    conn: IcebergConnectionConfig,
    table: TableIdentifier,
    tier: StatsTier,
) -> Result<TablePreview> {
    let (catalog, _uri, _wh) = rest_catalog_client(&conn, "table preview")?;
    let table_id = table.to_catalog();

    let load = SendCatalogClient::load_table(&catalog, &table_id, conn.io.vended_credentials)
        .await
        .map_err(|e| {
            crate::ApiError::config(format!("Failed to load table {}: {e}", table.qualified()))
        })?;

    let metadata = load.metadata.as_ref().ok_or_else(|| {
        crate::ApiError::config(format!(
            "Catalog did not return inline table metadata for {} — metadata preview requires a \
             REST catalog whose loadTable response includes the `metadata` object.",
            table.qualified()
        ))
    })?;

    let mut schema = table_schema_from_metadata(&table, metadata)?;

    match tier {
        StatsTier::Schema => Ok(TablePreview {
            schema,
            stats_completeness: StatsCompleteness {
                tier: "schema".to_string(),
                manifests_read: 0,
                had_column_bounds: false,
            },
            warnings: Vec::new(),
        }),
        StatsTier::Stats => {
            let mut warnings = vec![DISTINCT_COUNT_WARNING.to_string()];

            let (manifests_read, had_column_bounds) = match metadata.current_snapshot() {
                Some(snapshot) => {
                    let iceberg_schema = metadata.current_schema().ok_or_else(|| {
                        crate::ApiError::config("Table metadata has no current schema")
                    })?;

                    // Build S3 storage from vended credentials (if the catalog
                    // delegated them) or the ambient AWS chain — same policy as
                    // the scan path.
                    let storage = build_preview_storage(&conn, load.credentials.as_ref()).await?;

                    // Metadata-only: reads the manifest-list + manifests, never a
                    // Parquet/data file (see fluree_db_iceberg::stats).
                    let (data_files, manifests_read) =
                        send_read_snapshot_data_files(&storage, snapshot)
                            .await
                            .map_err(|e| {
                                crate::ApiError::config(format!(
                                    "Failed to read manifests for {}: {e}",
                                    table.qualified()
                                ))
                            })?;

                    let agg = aggregate_column_stats(&data_files, iceberg_schema);

                    for col in &mut schema.columns {
                        if col.nested {
                            continue;
                        }
                        if let Some(a) = agg.columns.get(&col.field_id) {
                            col.stats = Some(to_api_column_stats(a));
                        }
                    }

                    // Fill authoritative counts from the aggregation if the
                    // snapshot summary omitted them.
                    schema.row_count = schema.row_count.or(Some(agg.row_count));
                    schema.data_file_count = schema.data_file_count.or(Some(agg.data_file_count));
                    schema.total_bytes = schema.total_bytes.or(Some(agg.total_bytes));

                    (manifests_read, agg.had_column_bounds)
                }
                None => {
                    warnings.push(
                        "Table has no current snapshot; no column statistics available.".to_string(),
                    );
                    (0, false)
                }
            };

            Ok(TablePreview {
                schema,
                stats_completeness: StatsCompleteness {
                    tier: "stats".to_string(),
                    manifests_read,
                    had_column_bounds,
                },
                warnings,
            })
        }
    }
}

/// Build S3 storage for reading manifests during a Tier-B preview, mirroring the
/// scan path's policy: vended credentials when the catalog delegated them,
/// otherwise the ambient AWS credential chain.
async fn build_preview_storage(
    conn: &IcebergConnectionConfig,
    credentials: Option<&fluree_db_iceberg::credential::VendedCredentials>,
) -> Result<S3IcebergStorage> {
    let io = &conn.io;
    let storage = if let Some(creds) = credentials {
        S3IcebergStorage::from_vended_credentials(
            creds,
            io.s3_region.as_deref(),
            io.s3_endpoint.as_deref(),
            io.s3_path_style,
        )
        .await
    } else {
        S3IcebergStorage::from_default_chain(
            io.s3_region.as_deref(),
            io.s3_endpoint.as_deref(),
            io.s3_path_style,
        )
        .await
    };
    storage.map_err(|e| crate::ApiError::config(format!("Failed to create S3 storage: {e}")))
}

/// Map an iceberg-crate [`AggregatedColumnStats`] onto the API [`ColumnStats`].
fn to_api_column_stats(a: &AggregatedColumnStats) -> ColumnStats {
    ColumnStats {
        null_count: a.null_count,
        value_count: a.value_count,
        null_fraction: a.null_fraction,
        nan_count: a.nan_count,
        min: a.min.clone(),
        max: a.max.clone(),
        on_disk_bytes: a.on_disk_bytes,
        distinct_count: a.distinct_count,
    }
}

impl crate::Fluree {
    /// Preview an Iceberg table's schema (Tier-A) and optionally its per-column
    /// statistics (Tier-B). Convenience wrapper over the stateless
    /// [`preview_iceberg_table`] free function.
    pub async fn preview_iceberg_table(
        &self,
        conn: IcebergConnectionConfig,
        table: TableIdentifier,
        tier: StatsTier,
    ) -> Result<TablePreview> {
        preview_iceberg_table(conn, table, tier).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn browse_direct_mode_errors() {
        // Direct mode has no catalog to browse — must return a clear typed error
        // without any network access.
        let conn = IcebergConnectionConfig::direct("s3://bucket/warehouse/ns/table");
        let err = browse_iceberg_catalog(conn, BrowseDepth::Tables)
            .await
            .expect_err("Direct mode must not be browsable");
        let msg = err.to_string();
        assert!(
            msg.contains("Direct catalog mode"),
            "error should explain Direct mode is not browsable, got: {msg}"
        );
    }

    #[test]
    fn split_qualified_table_recovers_name() {
        assert_eq!(
            split_qualified_table("DW", "DW.DIM_STORE"),
            TableRef::new("DW", "DIM_STORE")
        );
        // Multi-level namespace with dots is handled via the queried prefix.
        assert_eq!(
            split_qualified_table("db.schema", "db.schema.events"),
            TableRef::new("db.schema", "events")
        );
        // Missing prefix falls back to a last-segment split.
        assert_eq!(
            split_qualified_table("DW", "OTHER.TABLE"),
            TableRef::new("OTHER", "TABLE")
        );
    }

    #[test]
    fn table_identifier_qualified() {
        let t = TableIdentifier::new("DW", "DIM_STORE");
        assert_eq!(t.qualified(), "DW.DIM_STORE");
    }

    /// A representative table metadata fixture exercising every Tier-A facet:
    /// identifier_field_ids, a partition spec, a sort order, a nested column,
    /// scalars of several types, snapshot summary counts, and properties.
    const SAMPLE_METADATA: &str = r#"{
        "format-version": 2,
        "table-uuid": "abc-123",
        "location": "s3://bucket/dw/sales",
        "last-updated-ms": 1700000000000,
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "identifier-field-ids": [1],
            "fields": [
                {"id": 1, "name": "SALE_KEY", "required": true, "type": "long"},
                {"id": 2, "name": "NAME", "required": false, "type": "string"},
                {"id": 3, "name": "AMOUNT", "required": false, "type": "decimal(18, 2)"},
                {"id": 4, "name": "CREATED", "required": false, "type": "timestamp"},
                {"id": 5, "name": "META", "required": false, "type": {
                    "type": "struct",
                    "fields": [{"id": 6, "name": "K", "required": true, "type": "int"}]
                }},
                {"id": 7, "name": "IS_OPEN", "required": false, "type": "boolean", "doc": "open flag"}
            ]
        }],
        "current-snapshot-id": 55,
        "snapshots": [
            {"snapshot-id": 40, "timestamp-ms": 1699000000000, "schema-id": 0, "summary": {}},
            {"snapshot-id": 55, "timestamp-ms": 1700000000000, "schema-id": 0, "summary": {
                "total-records": "1000", "total-data-files": "4", "total-files-size": "204800"
            }}
        ],
        "default-spec-id": 0,
        "partition-specs": [{
            "spec-id": 0,
            "fields": [{"source-id": 4, "field-id": 1000, "name": "created_day", "transform": "day"}]
        }],
        "default-sort-order-id": 1,
        "sort-orders": [{
            "order-id": 1,
            "fields": [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]
        }],
        "properties": {"owner": "analytics"}
    }"#;

    fn column<'a>(schema: &'a TableSchema, name: &str) -> &'a ColumnInfo {
        schema
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} not found"))
    }

    #[test]
    fn tier_a_schema_from_inline_metadata() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        let table = TableIdentifier::new("DW", "SALES");
        let schema = table_schema_from_metadata(&table, &metadata).unwrap();

        assert_eq!(schema.table, "DW.SALES");
        assert_eq!(schema.table_uuid.as_deref(), Some("abc-123"));
        assert_eq!(schema.format_version, 2);
        assert_eq!(schema.current_schema_id, 0);
        assert_eq!(schema.columns.len(), 6);

        // identifier_field_ids (the PK hint) survives.
        assert_eq!(schema.identifier_field_ids, vec![1]);

        // Snapshot + authoritative counts from the current snapshot summary.
        assert_eq!(schema.snapshot.id, 55);
        assert_eq!(schema.snapshot.timestamp_ms, 1_700_000_000_000);
        assert_eq!(schema.snapshot.schema_id, Some(0));
        assert_eq!(schema.row_count, Some(1000));
        assert_eq!(schema.data_file_count, Some(4));
        assert_eq!(schema.total_bytes, Some(204_800));
        // Snapshot history (both snapshots).
        assert_eq!(schema.snapshot_log.len(), 2);

        // Partition + sort resolved to source column names.
        assert_eq!(schema.partition_spec.len(), 1);
        assert_eq!(schema.partition_spec[0].name, "created_day");
        assert_eq!(schema.partition_spec[0].source_field, "CREATED");
        assert_eq!(schema.partition_spec[0].transform, "day");
        assert_eq!(schema.sort_order.len(), 1);
        assert_eq!(schema.sort_order[0].field, "SALE_KEY");
        assert_eq!(schema.sort_order[0].direction, "asc");
        assert_eq!(schema.sort_order[0].null_order, "nulls-first");

        assert_eq!(schema.properties.get("owner").map(String::as_str), Some("analytics"));

        // Column type mapping: field_type + xsd_type per FieldType.
        let key = column(&schema, "SALE_KEY");
        assert_eq!(key.field_id, 1);
        assert_eq!(key.iceberg_type, "long");
        assert_eq!(key.field_type, Some(FieldType::Int64));
        assert_eq!(key.xsd_type.as_deref(), Some("xsd:integer"));
        assert!(key.required);
        assert!(!key.nested);
        assert!(key.stats.is_none());

        let name = column(&schema, "NAME");
        assert_eq!(name.field_type, Some(FieldType::String));
        assert_eq!(name.xsd_type, None, "strings are left untyped");

        let amount = column(&schema, "AMOUNT");
        assert_eq!(amount.iceberg_type, "decimal(18, 2)");
        assert_eq!(
            amount.field_type,
            Some(FieldType::Decimal { precision: 18, scale: 2 })
        );
        assert_eq!(amount.xsd_type.as_deref(), Some("xsd:decimal"));

        let created = column(&schema, "CREATED");
        assert_eq!(created.field_type, Some(FieldType::Timestamp));
        assert_eq!(created.xsd_type.as_deref(), Some("xsd:dateTime"));

        let is_open = column(&schema, "IS_OPEN");
        assert_eq!(is_open.field_type, Some(FieldType::Boolean));
        assert_eq!(is_open.xsd_type.as_deref(), Some("xsd:boolean"));
        assert_eq!(is_open.doc.as_deref(), Some("open flag"));

        // Nested column: no field_type, no xsd_type, nested flag set.
        let meta = column(&schema, "META");
        assert!(meta.nested);
        assert_eq!(meta.field_type, None);
        assert_eq!(meta.xsd_type, None);
        assert_eq!(meta.iceberg_type, "struct");
    }

    #[test]
    fn column_info_serde_roundtrips_field_type() {
        let metadata = TableMetadata::from_json_str(SAMPLE_METADATA).unwrap();
        let table = TableIdentifier::new("DW", "SALES");
        let schema = table_schema_from_metadata(&table, &metadata).unwrap();

        // field_type rides the wire as its iceberg type string and round-trips.
        let json = serde_json::to_value(column(&schema, "SALE_KEY")).unwrap();
        assert_eq!(json["field_type"], "long");
        assert_eq!(json["xsd_type"], "xsd:integer");

        let json = serde_json::to_value(column(&schema, "META")).unwrap();
        assert!(json["field_type"].is_null());

        let back: ColumnInfo =
            serde_json::from_value(serde_json::to_value(column(&schema, "AMOUNT")).unwrap())
                .unwrap();
        assert_eq!(back.field_type, Some(FieldType::Decimal { precision: 18, scale: 2 }));
    }

    #[tokio::test]
    async fn preview_direct_mode_errors() {
        let conn = IcebergConnectionConfig::direct("s3://bucket/warehouse/ns/table");
        let err = preview_iceberg_table(conn, TableIdentifier::new("ns", "table"), StatsTier::Schema)
            .await
            .expect_err("Direct mode must not be previewable");
        assert!(err.to_string().contains("Direct catalog mode"));
    }

    /// Preview surfaces the emitter's canonical `xsd_datatype` map (pinned at
    /// `xsd_long_as_integer = true`, the reference convention) as its `xsd_type`
    /// hint. This pins the load-bearing cases the preview lane relies on; the map
    /// is now the emitter's single source of truth (no api-side duplicate).
    #[test]
    fn xsd_map_matches_emitter() {
        assert_eq!(xsd_datatype(FieldType::Bytes, true), Some("xsd:hexBinary"));
        assert_eq!(xsd_datatype(FieldType::Timestamp, true), Some("xsd:dateTime"));
        assert_eq!(
            xsd_datatype(FieldType::TimestampTz, true),
            Some("xsd:dateTime")
        );
        assert_eq!(xsd_datatype(FieldType::String, true), None);
        assert_eq!(xsd_datatype(FieldType::Boolean, true), Some("xsd:boolean"));
        assert_eq!(xsd_datatype(FieldType::Float64, true), Some("xsd:double"));
        assert_eq!(xsd_datatype(FieldType::Float32, true), Some("xsd:float"));
        assert_eq!(xsd_datatype(FieldType::Int64, true), Some("xsd:integer"));
        assert_eq!(xsd_datatype(FieldType::Int32, true), Some("xsd:integer"));
        assert_eq!(xsd_datatype(FieldType::Date, true), Some("xsd:date"));
        assert_eq!(
            xsd_datatype(FieldType::Decimal { precision: 18, scale: 2 }, true),
            Some("xsd:decimal")
        );
    }
}
