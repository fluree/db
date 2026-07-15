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
use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};
use fluree_db_iceberg::manifest::DataFile;
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
// SSRF boundary guard
// =============================================================================

/// Up-front SSRF reject for the request-supplied outbound URLs on an Iceberg
/// connection, for use at the (unauthenticated-by-default) route boundary.
///
/// This is a cheap literal-IP + scheme check; the AUTHORITATIVE enforcement is
/// at the client layer, where the catalog / OAuth2 clients are built with a
/// redirect-refusing, IP-denylisting resolver (see [`fluree_db_iceberg::net`]) —
/// which is what closes the redirect / DNS-rebinding bypasses a boundary string
/// check alone cannot. `catalog_uri` / `oauth2_token_url` get the full internal
/// denylist; `s3_endpoint` gets the narrower metadata-only block (MinIO /
/// LocalStack legitimately use loopback/private hosts).
pub fn guard_iceberg_connection_urls(
    catalog_uri: Option<&str>,
    oauth2_token_url: Option<&str>,
    s3_endpoint: Option<&str>,
) -> Result<()> {
    let to_err = |e: fluree_db_iceberg::IcebergError| crate::ApiError::config(e.to_string());
    if let Some(u) = catalog_uri {
        fluree_db_iceberg::net::validate_public_url(u).map_err(to_err)?;
    }
    if let Some(u) = oauth2_token_url {
        fluree_db_iceberg::net::validate_public_url(u).map_err(to_err)?;
    }
    if let Some(u) = s3_endpoint {
        fluree_db_iceberg::net::validate_s3_endpoint(u).map_err(to_err)?;
    }
    Ok(())
}

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
pub(crate) fn rest_catalog_client(
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
    /// Resolve any `ConfigValue::SecretRef` auth references in a REST connection
    /// via this instance's injected secret resolver, returning a connection whose
    /// auth is resolver-free (literal / env-var / none) and therefore safe to
    /// hand to the SYNCHRONOUS [`rest_catalog_client`].
    ///
    /// Pass-through for Direct mode (no catalog auth) and for connections with no
    /// secret reference. Fails closed with an actionable error when a `SecretRef`
    /// is present but no resolver was injected (the OSS/CLI path). This is the
    /// single hydration hop the impl-`Fluree` catalog wrappers call at their top,
    /// keeping `rest_catalog_client` and the free functions unchanged for
    /// external literal/env-var users.
    pub(crate) async fn hydrate_conn(
        &self,
        mut conn: IcebergConnectionConfig,
    ) -> Result<IcebergConnectionConfig> {
        if let CatalogMode::Rest(ref mut rest) = conn.catalog_mode {
            let hydrated = rest
                .auth
                .hydrate(self.secret_resolver())
                .await
                .map_err(|e| {
                    crate::ApiError::config(format!("Failed to resolve catalog auth secret: {e}"))
                })?;
            rest.auth = hydrated;
        }
        Ok(conn)
    }

    /// Browse an Iceberg REST catalog (namespaces, and tables at
    /// `depth = Tables`). Convenience wrapper over the stateless
    /// [`browse_iceberg_catalog`] free function — browse needs no engine state.
    pub async fn browse_iceberg_catalog(
        &self,
        conn: IcebergConnectionConfig,
        depth: BrowseDepth,
    ) -> Result<CatalogBrowse> {
        let conn = self.hydrate_conn(conn).await?;
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
    /// Whether `min`/`max` are **truncated prefixes** rather than exact observed
    /// values. Iceberg truncates variable-length (string/binary) bounds, so the
    /// decoded value is a valid bound but not necessarily present in the column.
    #[serde(default)]
    pub bounds_truncated: bool,
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
    /// Whether the snapshot carries **merge-on-read delete files**. When `true`,
    /// the aggregated `row_count` / value / null counts are **upper bounds**:
    /// they sum live data-file records without subtracting position/equality
    /// deletes, so they are not exact.
    #[serde(default)]
    pub has_delete_files: bool,
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
    let schema = metadata.current_schema().ok_or_else(|| {
        crate::ApiError::config(format!(
            "Table {} metadata has no current schema",
            table.qualified()
        ))
    })?;

    let current_snapshot = metadata.current_snapshot();
    let snapshot = match current_snapshot {
        Some(s) => snapshot_ref(s),
        None => SnapshotRef {
            id: metadata.current_snapshot_id.unwrap_or_default(),
            timestamp_ms: metadata.last_updated_ms,
            schema_id: Some(metadata.current_schema_id),
        },
    };

    let (row_count, data_file_count, total_bytes) =
        current_snapshot.map_or((None, None, None), |s| {
            (
                s.total_records(),
                s.total_data_files(),
                s.total_files_size(),
            )
        });

    let partition_spec = metadata
        .default_partition_spec()
        .map_or_else(Vec::new, |spec| {
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
                has_delete_files: false,
            },
            warnings: Vec::new(),
        }),
        StatsTier::Stats => {
            let mut warnings = vec![DISTINCT_COUNT_WARNING.to_string()];

            let (manifests_read, had_column_bounds, has_delete_files) = match metadata
                .current_snapshot()
            {
                Some(snapshot) => {
                    let iceberg_schema = metadata.current_schema().ok_or_else(|| {
                        crate::ApiError::config(format!(
                            "Table {} metadata has no current schema",
                            table.qualified()
                        ))
                    })?;

                    // Build S3 storage from vended credentials (if the catalog
                    // delegated them) or the ambient AWS chain — same policy as
                    // the scan path.
                    let storage = build_preview_storage(&conn, load.credentials.as_ref()).await?;

                    // Metadata-only: reads the manifest-list + manifests, never
                    // a Parquet/data file (see fluree_db_iceberg::stats).
                    let (data_files, manifests_read, has_delete_files) =
                        send_read_snapshot_data_files(&storage, snapshot)
                            .await
                            .map_err(|e| {
                                storage_api_error(
                                    &format!("Failed to read manifests for {}", table.qualified()),
                                    e,
                                )
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
                    // snapshot summary omitted them. When the snapshot carries
                    // merge-on-read deletes, the aggregated counts are upper
                    // bounds (deletes are not subtracted), so warn.
                    schema.row_count = schema.row_count.or(Some(agg.row_count));
                    schema.data_file_count = schema.data_file_count.or(Some(agg.data_file_count));
                    schema.total_bytes = schema.total_bytes.or(Some(agg.total_bytes));

                    if has_delete_files {
                        warnings.push(
                            "Table has merge-on-read delete files; aggregated row/value/null \
                                 counts are upper bounds (position/equality deletes are not \
                                 subtracted)."
                                .to_string(),
                        );
                    }

                    (manifests_read, agg.had_column_bounds, has_delete_files)
                }
                None => {
                    warnings.push(
                        "Table has no current snapshot; no column statistics available."
                            .to_string(),
                    );
                    (0, false, false)
                }
            };

            Ok(TablePreview {
                schema,
                stats_completeness: StatsCompleteness {
                    tier: "stats".to_string(),
                    manifests_read,
                    had_column_bounds,
                    has_delete_files,
                },
                warnings,
            })
        }
    }
}

/// Lift an [`IcebergError`](fluree_db_iceberg::IcebergError) raised at a
/// **storage-read** site into a [`QueryError`](fluree_db_query::QueryError),
/// preserving the typed access-denied case.
///
/// [`StorageAccessDenied`](fluree_db_iceberg::IcebergError::StorageAccessDenied)
/// becomes `QueryError::StorageAccessDenied` (→ HTTP 403); every other variant
/// becomes `QueryError::Internal("{context}: {err}")`, byte-for-byte the
/// pre-existing wrapping. Use ONLY at storage-read sites (metadata / manifest /
/// Parquet / resolve-from-table-location) — client-construction failures stay
/// `Internal`.
pub(crate) fn storage_query_error(
    context: &str,
    err: fluree_db_iceberg::IcebergError,
) -> fluree_db_query::QueryError {
    match err {
        fluree_db_iceberg::IcebergError::StorageAccessDenied {
            bucket,
            key,
            region,
            message,
        } => fluree_db_query::QueryError::StorageAccessDenied {
            bucket,
            key,
            region,
            message,
        },
        other => fluree_db_query::QueryError::Internal(format!("{context}: {other}")),
    }
}

/// Preview/browse-path analogue of [`storage_query_error`]: lift a
/// storage-read [`IcebergError`](fluree_db_iceberg::IcebergError) into an
/// [`ApiError`](crate::ApiError). The access-denied case becomes
/// `ApiError::StorageAccessDenied` (→ HTTP 403); everything else becomes
/// `ApiError::config("{context}: {err}")`, matching the pre-existing preview
/// wrapping.
pub(crate) fn storage_api_error(
    context: &str,
    err: fluree_db_iceberg::IcebergError,
) -> crate::ApiError {
    match err {
        fluree_db_iceberg::IcebergError::StorageAccessDenied {
            bucket,
            key,
            region,
            message,
        } => crate::ApiError::StorageAccessDenied {
            bucket,
            key,
            region,
            message,
        },
        other => crate::ApiError::config(format!("{context}: {other}")),
    }
}

/// Which credential source a scan/preview should use, given the source's
/// `vended_credentials` flag, whether the catalog actually vended credentials,
/// and whether this is a REST catalog. See [`decide_credential_source`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialSource {
    /// Use the catalog-vended credentials (they are present).
    Vended,
    /// Use the ambient AWS credential chain (explicit `vended_credentials =
    /// false` opt-in, or Direct mode which never vends).
    Ambient,
    /// Refuse: the REST source requires vending but the catalog vended none.
    FailClosed,
}

/// §2 credential-source decision — the single source of truth for the scan and
/// preview paths, factored out so the full matrix is unit-testable.
///
/// | `vended_credentials` | vended creds present | REST | decision |
/// |----------------------|----------------------|------|----------|
/// | any                  | yes                  | any  | `Vended` (response-driven) |
/// | true                 | no                   | yes  | `FailClosed` |
/// | true                 | no                   | no (Direct) | `Ambient` (Direct never vends) |
/// | false                | no                   | any  | `Ambient` (explicit opt-in) |
///
/// The `(false, present)` case stays `Vended` to preserve today's
/// response-driven behavior: `vended_credentials = false` suppresses the
/// delegation request, so credentials should not arrive — but if a catalog vends
/// them anyway, using them (rather than ignoring them) matches prior behavior.
pub(crate) fn decide_credential_source(
    vended_credentials_cfg: bool,
    has_vended_creds: bool,
    is_rest_catalog: bool,
) -> CredentialSource {
    if has_vended_creds {
        CredentialSource::Vended
    } else if vended_credentials_cfg && is_rest_catalog {
        CredentialSource::FailClosed
    } else {
        CredentialSource::Ambient
    }
}

/// Build S3 storage for reading manifests during a Tier-B preview, mirroring the
/// scan path's policy via [`decide_credential_source`]: vended credentials when
/// the catalog delegated them, otherwise — for `vended_credentials = false` or
/// Direct mode — the ambient AWS credential chain.
///
/// §2 fail-closed: a REST source configured to require vended credentials
/// (`io.vended_credentials = true`, the default) must NOT silently downgrade to
/// ambient credentials when the catalog vended none — that would read from
/// whatever ambient identity the process happens to have. Direct mode never
/// vends (it sends no delegation request), so it is exempt.
pub(crate) async fn build_preview_storage(
    conn: &IcebergConnectionConfig,
    credentials: Option<&fluree_db_iceberg::credential::VendedCredentials>,
) -> Result<S3IcebergStorage> {
    let io = &conn.io;
    let is_rest = matches!(conn.catalog_mode, CatalogMode::Rest(_));
    let storage =
        match decide_credential_source(io.vended_credentials, credentials.is_some(), is_rest) {
            CredentialSource::Vended => {
                let creds = credentials.expect("Vended decision implies credentials are present");
                S3IcebergStorage::from_vended_credentials(
                    creds,
                    io.s3_region.as_deref(),
                    io.s3_endpoint.as_deref(),
                    io.s3_path_style,
                )
                .await
            }
            CredentialSource::Ambient => {
                S3IcebergStorage::from_default_chain(
                    io.s3_region.as_deref(),
                    io.s3_endpoint.as_deref(),
                    io.s3_path_style,
                )
                .await
            }
            CredentialSource::FailClosed => {
                // FailClosed only arises for a REST catalog (see the decision table),
                // so this destructure always matches.
                let catalog_uri = match &conn.catalog_mode {
                    CatalogMode::Rest(rest) => rest.catalog_uri.clone(),
                    CatalogMode::Direct { .. } => {
                        unreachable!("FailClosed is only produced for REST catalogs")
                    }
                };
                return Err(crate::ApiError::CatalogCredentialsNotVended { catalog_uri });
            }
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
        bounds_truncated: a.bounds_truncated,
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
        let conn = self.hydrate_conn(conn).await?;
        preview_iceberg_table(conn, table, tier).await
    }
}

// =============================================================================
// (c) Storage-access verification (§6)
//
// A config-time probe that goes through the ENGINE'S OWN credential decision
// ([`decide_credential_source`], §2 fail-closed included) and storage
// construction, then proves BOTH S3 prefixes a query needs — the `metadata/`
// prefix (manifest-list + manifests) and the `data/` prefix (a `HeadObject` on
// the first data file). It never reads a Parquet/data file's bytes. A green
// report proves a query will not fail on storage permissions, so solo's
// onboarding "Test" button validates on the same path queries use.
// =============================================================================

/// The result of a storage-access verification probe.
///
/// This shape goes over HTTP verbatim (solo's onboarding wizard renders it), so
/// the field names are pinned.
#[derive(Debug, Serialize)]
pub struct StorageAccessReport {
    /// Which credential source the probe used: `"vended"` (catalog-delegated) or
    /// `"ambient"` (the process AWS credential chain). This is the SAME decision
    /// the scan/preview paths make (see [`decide_credential_source`]), so a green
    /// probe proves a query authenticates to storage the same way.
    pub credential_source: &'static str,
    /// The table's current metadata-JSON location
    /// (`…/metadata/*.metadata.json`), from the catalog `loadTable` response.
    pub metadata_location: String,
    /// Number of data files listed in the current snapshot's manifests. Listing
    /// them proves the `metadata/` prefix (manifest-list + manifests) is readable.
    pub data_files_listed: usize,
    /// The single data file the probe stat-checked (`HeadObject`) to prove the
    /// `data/` prefix is readable; `None` when the data probe was skipped.
    pub probed_data_file: Option<String>,
    /// The stat-checked data file's size in bytes; `None` when skipped.
    pub probed_data_file_bytes: Option<u64>,
    /// `true` when the `data/` prefix probe was skipped because the table has no
    /// data file to stat (an empty table, or a snapshotless table). The
    /// `metadata/` prefix was still proven for an empty table; for a snapshotless
    /// table there were no manifests to read either — see `skip_reason`.
    pub data_probe_skipped: bool,
    /// Human-readable reason the data probe was skipped; `None` when it ran.
    pub skip_reason: Option<String>,
}

/// The `data/`-prefix half of the probe, factored out so it is unit-testable over
/// a stub storage (no Avro manifest fixtures needed): given the already-listed
/// data files, `HeadObject` the first one to prove the `data/` prefix is
/// readable, or record a skip when the table lists no data files.
#[derive(Debug)]
struct DataFileProbe {
    probed_data_file: Option<String>,
    probed_data_file_bytes: Option<u64>,
    data_probe_skipped: bool,
    skip_reason: Option<String>,
}

async fn probe_data_files<S: SendIcebergStorage + ?Sized>(
    storage: &S,
    data_files: &[DataFile],
    table_qualified: &str,
) -> Result<DataFileProbe> {
    match data_files.first() {
        Some(first) => {
            // `HeadObject` (not a byte read): proves the `data/` prefix is
            // readable under the same credentials without downloading the file.
            let bytes = storage.file_size(&first.file_path).await.map_err(|e| {
                storage_api_error(
                    &format!(
                        "Failed to stat data file {} for {table_qualified}",
                        first.file_path
                    ),
                    e,
                )
            })?;
            Ok(DataFileProbe {
                probed_data_file: Some(first.file_path.clone()),
                probed_data_file_bytes: Some(bytes),
                data_probe_skipped: false,
                skip_reason: None,
            })
        }
        None => Ok(DataFileProbe {
            probed_data_file: None,
            probed_data_file_bytes: None,
            data_probe_skipped: true,
            skip_reason: Some(
                "table snapshot lists no data files (empty table); the data/ prefix \
                 was not probed"
                    .to_string(),
            ),
        }),
    }
}

/// Read the snapshot's manifests (proving the `metadata/` prefix) and stat the
/// first data file (proving the `data/` prefix), assembling a
/// [`StorageAccessReport`]. Factored to be generic over the storage trait so the
/// metadata- and data-prefix denial paths are unit-testable without S3 (mirrors
/// §1's `resolve_table_metadata`).
async fn probe_storage_access<S: SendIcebergStorage + ?Sized>(
    storage: &S,
    snapshot: &Snapshot,
    credential_source: &'static str,
    metadata_location: String,
    table_qualified: &str,
) -> Result<StorageAccessReport> {
    // `metadata/` prefix proof: read the manifest-list + manifests (never a data
    // file). Reuses the same reader the Tier-B preview / scan planner use.
    let (data_files, _manifests_read, _has_deletes) =
        send_read_snapshot_data_files(storage, snapshot)
            .await
            .map_err(|e| {
                storage_api_error(
                    &format!("Failed to read manifests for {table_qualified}"),
                    e,
                )
            })?;

    let data_files_listed = data_files.len();
    let probe = probe_data_files(storage, &data_files, table_qualified).await?;

    Ok(StorageAccessReport {
        credential_source,
        metadata_location,
        data_files_listed,
        probed_data_file: probe.probed_data_file,
        probed_data_file_bytes: probe.probed_data_file_bytes,
        data_probe_skipped: probe.data_probe_skipped,
        skip_reason: probe.skip_reason,
    })
}

/// Split a `"NAMESPACE.NAME"` table string into a catalog [`TableIdentifier`].
/// Namespaces may themselves contain dots, so the LAST dot separates namespace
/// from name (mirrors [`split_qualified_table`] / the validate path). A name with
/// no dot yields an empty-namespace identifier, which the catalog rejects as
/// not-found.
fn parse_qualified_table(table: &str) -> TableIdentifier {
    match table.rsplit_once('.') {
        Some((namespace, name)) => TableIdentifier::new(namespace, name),
        None => TableIdentifier::new("", table),
    }
}

/// Verify that this connection's resolved credentials can actually READ a table's
/// storage — the config-time probe behind solo's onboarding "Test" button.
///
/// It goes through the ENGINE'S OWN credential decision
/// ([`decide_credential_source`], including §2 fail-closed) and storage
/// construction, then proves both S3 prefixes a query needs: the `metadata/`
/// prefix (manifest-list + manifests) and the `data/` prefix (a `HeadObject` on
/// the first data file — never a byte read). A green report therefore proves a
/// query will not fail on storage permissions, on the same credential path
/// queries use.
///
/// REST catalogs only: Direct mode returns the same clear typed error as
/// [`preview_iceberg_table`] (there is no catalog to authorize the read).
pub async fn verify_storage_access(
    conn: IcebergConnectionConfig,
    table: &str,
) -> Result<StorageAccessReport> {
    let (catalog, _uri, _wh) = rest_catalog_client(&conn, "storage access verification")?;
    let table_id = parse_qualified_table(table);
    let catalog_table = table_id.to_catalog();

    let load = SendCatalogClient::load_table(&catalog, &catalog_table, conn.io.vended_credentials)
        .await
        .map_err(|e| {
            crate::ApiError::config(format!(
                "Failed to load table {}: {e}",
                table_id.qualified()
            ))
        })?;

    // Build storage through the SAME credential decision the scan path uses. §2
    // fail-closed fires HERE: a REST source that requires vended credentials but
    // whose catalog vended none is refused (ApiError::CatalogCredentialsNotVended)
    // rather than silently probing with ambient credentials — which is exactly the
    // "validated on a different credential path than queries use" bug this guards.
    let storage = build_preview_storage(&conn, load.credentials.as_ref()).await?;
    let credential_source = if load.credentials.is_some() {
        "vended"
    } else {
        "ambient"
    };

    let metadata = load.metadata.as_ref().ok_or_else(|| {
        crate::ApiError::config(format!(
            "Catalog did not return inline table metadata for {} — storage verification \
             resolves the current snapshot from the loadTable `metadata` object.",
            table_id.qualified()
        ))
    })?;

    // Mirror the Tier-B preview: resolve the CURRENT snapshot from the inline
    // metadata. A snapshotless table has no manifests or data files to read, so
    // there is nothing to probe — report a skip rather than erroring (the catalog
    // authorization and the credential decision were still exercised above).
    match metadata.current_snapshot() {
        Some(snapshot) => {
            probe_storage_access(
                &storage,
                snapshot,
                credential_source,
                load.metadata_location,
                &table_id.qualified(),
            )
            .await
        }
        None => Ok(StorageAccessReport {
            credential_source,
            metadata_location: load.metadata_location,
            data_files_listed: 0,
            probed_data_file: None,
            probed_data_file_bytes: None,
            data_probe_skipped: true,
            skip_reason: Some(
                "table has no current snapshot; there are no manifests or data files to \
                 probe (catalog authorization and credential resolution still succeeded)"
                    .to_string(),
            ),
        }),
    }
}

impl crate::Fluree {
    /// Verify that a connection's resolved credentials can read an Iceberg table's
    /// storage (the onboarding "Test" probe). Resolver-aware wrapper over
    /// [`verify_storage_access`]: hydrates any `SecretRef` catalog auth via this
    /// instance's secret resolver FIRST (mirroring the browse/preview wrappers),
    /// then runs the probe through the engine's own credential + storage path.
    pub async fn verify_iceberg_storage_access(
        &self,
        conn: IcebergConnectionConfig,
        table: &str,
    ) -> Result<StorageAccessReport> {
        let conn = self.hydrate_conn(conn).await?;
        verify_storage_access(conn, table).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── §2 credential-source decision (pure; the full matrix) ──

    #[test]
    fn creds_present_always_vended() {
        // Response-driven: if the catalog vended credentials, use them —
        // regardless of the flag or catalog kind.
        for &cfg in &[true, false] {
            for &rest in &[true, false] {
                assert_eq!(
                    decide_credential_source(cfg, true, rest),
                    CredentialSource::Vended,
                    "cfg={cfg} rest={rest}"
                );
            }
        }
    }

    #[test]
    fn rest_requires_vending_fails_closed_when_none() {
        // vended_credentials = true (default) + REST + no creds → refuse.
        assert_eq!(
            decide_credential_source(true, false, true),
            CredentialSource::FailClosed
        );
    }

    #[test]
    fn direct_mode_never_fails_closed() {
        // Direct never vends; vended_credentials = true must NOT start failing —
        // it uses the ambient chain (item 8).
        assert_eq!(
            decide_credential_source(true, false, false),
            CredentialSource::Ambient
        );
    }

    #[test]
    fn explicit_opt_in_uses_ambient() {
        // vended_credentials = false + no creds → ambient (both catalog kinds).
        assert_eq!(
            decide_credential_source(false, false, true),
            CredentialSource::Ambient
        );
        assert_eq!(
            decide_credential_source(false, false, false),
            CredentialSource::Ambient
        );
    }

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

        assert_eq!(
            schema.properties.get("owner").map(String::as_str),
            Some("analytics")
        );

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
            Some(FieldType::Decimal {
                precision: 18,
                scale: 2
            })
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
        assert_eq!(
            back.field_type,
            Some(FieldType::Decimal {
                precision: 18,
                scale: 2
            })
        );
    }

    #[tokio::test]
    async fn preview_direct_mode_errors() {
        let conn = IcebergConnectionConfig::direct("s3://bucket/warehouse/ns/table");
        let err =
            preview_iceberg_table(conn, TableIdentifier::new("ns", "table"), StatsTier::Schema)
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
        assert_eq!(
            xsd_datatype(FieldType::Timestamp, true),
            Some("xsd:dateTime")
        );
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
            xsd_datatype(
                FieldType::Decimal {
                    precision: 18,
                    scale: 2
                },
                true
            ),
            Some("xsd:decimal")
        );
    }

    // ── secret-resolver injection + fail-closed hydration gate ──

    #[derive(Debug)]
    struct StubResolver;

    #[async_trait::async_trait]
    impl fluree_db_iceberg::SecretResolver for StubResolver {
        async fn resolve_secret(
            &self,
            secret_ref: &str,
        ) -> std::result::Result<String, fluree_db_iceberg::SecretResolveError> {
            Ok(format!("resolved:{secret_ref}"))
        }
    }

    #[tokio::test]
    async fn with_secret_resolver_clones_and_leaves_original_untouched() {
        let fluree = crate::FlureeBuilder::memory().build_memory();
        assert!(fluree.secret_resolver().is_none());

        let resolver: std::sync::Arc<dyn fluree_db_iceberg::SecretResolver> =
            std::sync::Arc::new(StubResolver);
        let derived = fluree.with_secret_resolver(resolver);

        // The derived clone carries the resolver; the original is untouched.
        assert!(derived.secret_resolver().is_some());
        assert!(fluree.secret_resolver().is_none());
    }

    #[tokio::test]
    async fn hydrate_conn_fails_closed_before_network_when_ref_has_no_resolver() {
        // No resolver injected + SecretRef auth ⇒ hydrate_conn errors with an
        // actionable message BEFORE any catalog client is built or network I/O
        // happens (the same gate the connection test relies on).
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let conn = IcebergConnectionConfig::rest("https://unreachable.invalid")
            .with_auth_bearer_token_ref("vault://team/bearer");
        let err = fluree.hydrate_conn(conn).await.unwrap_err().to_string();
        assert!(
            err.contains("secret resolver"),
            "actionable message expected: {err}"
        );
    }

    #[tokio::test]
    async fn hydrate_conn_passes_through_literal_auth() {
        // A literal-auth connection with no resolver hydrates to itself (no error).
        let fluree = crate::FlureeBuilder::memory().build_memory();
        let conn = IcebergConnectionConfig::rest("https://c.example.com")
            .with_auth_bearer("literal-token");
        assert!(fluree.hydrate_conn(conn).await.is_ok());
    }

    // ── §6 storage-access verification ──

    use fluree_db_iceberg::error::Result as IcebergResult;
    use fluree_db_iceberg::manifest::{FileFormat, PartitionData};

    /// A structured 403 as the S3 storage layer produces it, so the probe's
    /// error-lift is exercised end to end (StorageAccessDenied → 403).
    fn denied(path: &str) -> fluree_db_iceberg::IcebergError {
        fluree_db_iceberg::IcebergError::StorageAccessDenied {
            bucket: "bucket".to_string(),
            key: path.to_string(),
            region: Some("us-east-1".to_string()),
            message: "service error: AccessDenied".to_string(),
        }
    }

    /// Storage stub whose every access denies with a structured 403.
    #[derive(Debug)]
    struct DeniedStorage;

    #[async_trait::async_trait]
    impl SendIcebergStorage for DeniedStorage {
        async fn read(&self, path: &str) -> IcebergResult<bytes::Bytes> {
            Err(denied(path))
        }
        async fn read_range(
            &self,
            path: &str,
            _range: std::ops::Range<u64>,
        ) -> IcebergResult<bytes::Bytes> {
            Err(denied(path))
        }
        async fn file_size(&self, path: &str) -> IcebergResult<u64> {
            Err(denied(path))
        }
    }

    /// Storage stub that panics on ANY access: the empty-table data probe must not
    /// touch storage, so a panic here fails the test loudly.
    #[derive(Debug)]
    struct PanicStorage;

    #[async_trait::async_trait]
    impl SendIcebergStorage for PanicStorage {
        async fn read(&self, path: &str) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read must not be called on the empty-table probe (path={path})");
        }
        async fn read_range(
            &self,
            _path: &str,
            _range: std::ops::Range<u64>,
        ) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read_range must not be called on the empty-table probe");
        }
        async fn file_size(&self, path: &str) -> IcebergResult<u64> {
            panic!("storage.file_size must not be called on the empty-table probe (path={path})");
        }
    }

    /// Storage stub that answers `file_size` (`HeadObject`) with a fixed size and
    /// panics on any byte read: the data-prefix probe must stat, never download.
    #[derive(Debug)]
    struct HeadOnlyStorage {
        size: u64,
    }

    #[async_trait::async_trait]
    impl SendIcebergStorage for HeadOnlyStorage {
        async fn read(&self, path: &str) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read must not be called on the data-prefix probe (path={path})");
        }
        async fn read_range(
            &self,
            _path: &str,
            _range: std::ops::Range<u64>,
        ) -> IcebergResult<bytes::Bytes> {
            panic!("storage.read_range must not be called on the data-prefix probe");
        }
        async fn file_size(&self, _path: &str) -> IcebergResult<u64> {
            Ok(self.size)
        }
    }

    fn sample_data_file(path: &str) -> DataFile {
        DataFile {
            file_path: path.to_string(),
            file_format: FileFormat::Parquet,
            record_count: 10,
            file_size_in_bytes: 100,
            partition: PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    fn snapshot_with_manifest_list(path: &str) -> Snapshot {
        Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 0,
            timestamp_ms: 1_700_000_000_000,
            manifest_list: Some(path.to_string()),
            manifests: None,
            summary: HashMap::new(),
            schema_id: Some(0),
        }
    }

    #[test]
    fn storage_access_report_serde_shape() {
        // The solo wizard reads these exact keys — pin the field set + names.
        let report = StorageAccessReport {
            credential_source: "vended",
            metadata_location: "s3://bucket/warehouse/t/metadata/v3.metadata.json".to_string(),
            data_files_listed: 4,
            probed_data_file: Some("s3://bucket/warehouse/t/data/part-0.parquet".to_string()),
            probed_data_file_bytes: Some(2048),
            data_probe_skipped: false,
            skip_reason: None,
        };
        let v = serde_json::to_value(&report).unwrap();
        assert_eq!(v["credential_source"], "vended");
        assert_eq!(
            v["metadata_location"],
            "s3://bucket/warehouse/t/metadata/v3.metadata.json"
        );
        assert_eq!(v["data_files_listed"], 4);
        assert_eq!(
            v["probed_data_file"],
            "s3://bucket/warehouse/t/data/part-0.parquet"
        );
        assert_eq!(v["probed_data_file_bytes"], 2048);
        assert_eq!(v["data_probe_skipped"], false);
        assert!(v["skip_reason"].is_null());
        assert_eq!(
            v.as_object().unwrap().len(),
            7,
            "report field set is pinned: {v}"
        );
    }

    #[tokio::test]
    async fn probe_skips_data_prefix_for_empty_table() {
        // No data files ⇒ the data/ prefix probe is skipped WITHOUT any storage
        // read (PanicStorage would panic on a touch).
        let probe = probe_data_files(&PanicStorage, &[], "DW.EMPTY")
            .await
            .expect("empty-table probe must succeed without any storage read");
        assert!(probe.data_probe_skipped);
        assert!(probe.probed_data_file.is_none());
        assert!(probe.probed_data_file_bytes.is_none());
        assert!(probe.skip_reason.is_some());
    }

    #[tokio::test]
    async fn probe_stats_first_data_file_when_present() {
        // A non-empty table stats (HeadObject) the first data file — never reads it.
        let df = sample_data_file("s3://bucket/warehouse/t/data/part-0.parquet");
        let probe = probe_data_files(
            &HeadOnlyStorage { size: 99 },
            std::slice::from_ref(&df),
            "DW.T",
        )
        .await
        .expect("a readable data file resolves via HeadObject");
        assert!(!probe.data_probe_skipped);
        assert_eq!(
            probe.probed_data_file.as_deref(),
            Some("s3://bucket/warehouse/t/data/part-0.parquet")
        );
        assert_eq!(probe.probed_data_file_bytes, Some(99));
        assert!(probe.skip_reason.is_none());
    }

    #[tokio::test]
    async fn probe_data_prefix_denied_surfaces_storage_access_denied() {
        // A denied HeadObject on the data/ prefix must surface as the typed
        // access-denied variant (→ HTTP 403), not a generic config error.
        let df = sample_data_file("s3://bucket/warehouse/t/data/part-0.parquet");
        let err = probe_data_files(&DeniedStorage, std::slice::from_ref(&df), "DW.T")
            .await
            .expect_err("a denied HeadObject must propagate");
        assert!(
            matches!(err, crate::ApiError::StorageAccessDenied { .. }),
            "expected StorageAccessDenied, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn probe_metadata_prefix_denied_surfaces_storage_access_denied() {
        // A denied read of the manifest list (metadata/ prefix) must likewise
        // surface as the typed access-denied variant.
        let snapshot = snapshot_with_manifest_list(
            "s3://bucket/warehouse/t/metadata/snap-1-manifest-list.avro",
        );
        let err = probe_storage_access(
            &DeniedStorage,
            &snapshot,
            "vended",
            "s3://bucket/warehouse/t/metadata/v3.metadata.json".to_string(),
            "DW.T",
        )
        .await
        .expect_err("a denied manifest-list read must propagate");
        assert!(
            matches!(err, crate::ApiError::StorageAccessDenied { .. }),
            "expected StorageAccessDenied, got: {err:?}"
        );
    }

    #[test]
    fn parse_qualified_table_splits_on_last_dot() {
        assert_eq!(
            parse_qualified_table("DW.DIM_STORE"),
            TableIdentifier::new("DW", "DIM_STORE")
        );
        // Dotted namespace: the LAST dot separates namespace from name.
        assert_eq!(
            parse_qualified_table("db.schema.events"),
            TableIdentifier::new("db.schema", "events")
        );
        // No dot ⇒ empty namespace (catalog rejects as not-found).
        assert_eq!(
            parse_qualified_table("bare"),
            TableIdentifier::new("", "bare")
        );
    }

    #[tokio::test]
    async fn verify_direct_mode_errors() {
        // Direct mode has no catalog to authorize the read — mirror preview and
        // reject with a clear typed error, without any network access.
        let conn = IcebergConnectionConfig::direct("s3://bucket/warehouse/ns/table");
        let err = verify_storage_access(conn, "ns.table")
            .await
            .expect_err("Direct mode must not be verifiable");
        assert!(
            err.to_string().contains("Direct catalog mode"),
            "error should explain Direct mode is unsupported, got: {err}"
        );
    }
}
