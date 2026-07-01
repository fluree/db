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

use serde::{Deserialize, Serialize};

use crate::graph_source::config::{CatalogMode, IcebergConnectionConfig};
use crate::Result;

use fluree_db_iceberg::catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient};

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
}
