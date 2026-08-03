//! Direct catalog client — bypasses REST catalog, reads metadata from S3 directly.
//!
//! This client resolves the current Iceberg table version via `version-hint.text`
//! in the table's metadata directory, then reads the corresponding
//! `vN.metadata.json` file. This is the same pattern used by Iceberg's
//! Hadoop file-based catalog.
//!
//! # Usage
//!
//! ```ignore
//! use fluree_db_iceberg::catalog::direct::DirectCatalogClient;
//! use fluree_db_iceberg::io::S3IcebergStorage;
//!
//! let storage = S3IcebergStorage::from_default_chain(Some("us-east-1"), None, false).await?;
//! let client = DirectCatalogClient::new(
//!     "s3://bucket/warehouse/ns/table".to_string(),
//!     Arc::new(storage),
//! );
//!
//! let response = client.load_table(&table_id, false).await?;
//! // response.metadata_location → "s3://bucket/warehouse/ns/table/metadata/00042-abc.metadata.json"
//! ```

use crate::catalog::{CatalogClient, LoadTableResponse, TableIdentifier};
use crate::error::{IcebergError, Result};
use crate::io::IcebergStorage;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Catalog client that reads Iceberg metadata directly from a known S3 table location.
///
/// Instead of querying a REST catalog API, this client:
/// 1. Reads `{table_location}/metadata/version-hint.text` (one small S3 GET)
/// 2. Uses the hint content as the metadata filename (or full path)
/// 3. Resolves to `{table_location}/metadata/{hint}` (or the absolute path if provided)
///
/// This is ideal for use cases where the writer (e.g., `iceberg-rust`) already
/// knows the table location and a REST catalog adds unnecessary overhead.
pub struct DirectCatalogClient<S: IcebergStorage> {
    table_location: String,
    storage: Arc<S>,
}

impl<S: IcebergStorage> DirectCatalogClient<S> {
    /// Create a new direct catalog client.
    ///
    /// `table_location` should be the S3 prefix for the table root directory
    /// (e.g., `s3://bucket/warehouse/ns/table`). It must contain a `metadata/`
    /// subdirectory with Iceberg metadata files.
    pub fn new(table_location: String, storage: Arc<S>) -> Self {
        Self {
            table_location,
            storage,
        }
    }

    /// Resolve the current metadata location via `version-hint.text`.
    ///
    /// Returns the full S3 path to the current metadata JSON file.
    /// The hint file should contain the metadata filename
    /// (e.g., `00001-abc-def.metadata.json`) or a full path.
    async fn resolve_metadata_location(&self) -> Result<String> {
        let hint_path = format!("{}/metadata/version-hint.text", self.table_location);
        let hint_bytes = self.storage.read(&hint_path).await.map_err(|e| {
            IcebergError::Metadata(format!(
                "Failed to read version-hint.text at {hint_path}: {e}"
            ))
        })?;

        let hint = std::str::from_utf8(&hint_bytes)
            .map_err(|e| IcebergError::Metadata(format!("Invalid version-hint.text: {e}")))?
            .trim();

        if hint.is_empty() {
            return Err(IcebergError::Metadata(
                "version-hint.text is empty".to_string(),
            ));
        }

        Ok(resolve_hint_to_metadata_path(hint, &self.table_location))
    }
}

impl<S: IcebergStorage> std::fmt::Debug for DirectCatalogClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectCatalogClient")
            .field("table_location", &self.table_location)
            .finish()
    }
}

#[async_trait(?Send)]
impl<S: IcebergStorage> CatalogClient for DirectCatalogClient<S> {
    /// Not supported for direct catalogs — returns an error.
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support namespace listing".to_string(),
        ))
    }

    /// Not supported for direct catalogs — returns an error.
    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support table listing".to_string(),
        ))
    }

    /// Load table metadata by resolving `version-hint.text` from the table location.
    ///
    /// The `request_credentials` parameter is ignored — direct mode uses the
    /// storage client's own credentials (IAM role, env vars, etc.).
    async fn load_table(
        &self,
        _table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let metadata_location = self.resolve_metadata_location().await?;

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None, // Direct mode uses ambient credentials
            metadata: None,    // Direct mode has no inline metadata (resolved via version-hint)
        })
    }
}

// ---------------------------------------------------------------------------
// Send-safe variant for server-side usage
// ---------------------------------------------------------------------------

#[cfg(feature = "aws")]
use crate::catalog::SendCatalogClient;
#[cfg(feature = "aws")]
use crate::io::SendIcebergStorage;

/// Send-safe direct catalog client for server-side usage with `tokio::spawn`.
#[cfg(feature = "aws")]
pub struct SendDirectCatalogClient<S: SendIcebergStorage> {
    table_location: String,
    storage: Arc<S>,
}

#[cfg(feature = "aws")]
impl<S: SendIcebergStorage> SendDirectCatalogClient<S> {
    /// Create a new send-safe direct catalog client.
    pub fn new(table_location: String, storage: Arc<S>) -> Self {
        Self {
            table_location,
            storage,
        }
    }

    /// Resolve the current metadata location via `version-hint.text`.
    ///
    /// See [`DirectCatalogClient`] for format details.
    async fn resolve_metadata_location(&self) -> Result<String> {
        let hint_path = format!("{}/metadata/version-hint.text", self.table_location);
        let hint_bytes = self.storage.read(&hint_path).await.map_err(|e| {
            IcebergError::Metadata(format!(
                "Failed to read version-hint.text at {hint_path}: {e}"
            ))
        })?;

        let hint = std::str::from_utf8(&hint_bytes)
            .map_err(|e| IcebergError::Metadata(format!("Invalid version-hint.text: {e}")))?
            .trim();

        if hint.is_empty() {
            return Err(IcebergError::Metadata(
                "version-hint.text is empty".to_string(),
            ));
        }

        Ok(resolve_hint_to_metadata_path(hint, &self.table_location))
    }
}

#[cfg(feature = "aws")]
impl<S: SendIcebergStorage> std::fmt::Debug for SendDirectCatalogClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendDirectCatalogClient")
            .field("table_location", &self.table_location)
            .finish()
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl<S: SendIcebergStorage + 'static> SendCatalogClient for SendDirectCatalogClient<S> {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support namespace listing".to_string(),
        ))
    }

    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support table listing".to_string(),
        ))
    }

    async fn load_table(
        &self,
        _table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let metadata_location = self.resolve_metadata_location().await?;

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None,
            metadata: None,
        })
    }
}

/// Resolve a version-hint.text value to a full metadata path.
///
/// Accepts three formats:
/// - **Full path** (e.g., `"s3://bucket/.../00001-abc.metadata.json"`) →
///   returned as-is.
/// - **Filename** (e.g., `"00001-abc-def.metadata.json"`) →
///   `{table_location}/metadata/00001-abc-def.metadata.json`
///   (Spark / iceberg-rust / AWS Glue style).
/// - **Bare integer version** (e.g., `"1782390319"`) →
///   `{table_location}/metadata/v1782390319.metadata.json`. This is the Iceberg
///   Hadoop file-based catalog convention — `version-hint.text` holds the
///   version number `N` and the metadata file is `vN.metadata.json`.
fn resolve_hint_to_metadata_path(hint: &str, table_location: &str) -> String {
    if hint.contains("://") {
        hint.to_string()
    } else if hint.ends_with(".metadata.json") {
        format!("{table_location}/metadata/{hint}")
    } else if !hint.is_empty() && hint.bytes().all(|b| b.is_ascii_digit()) {
        format!("{table_location}/metadata/v{hint}.metadata.json")
    } else {
        format!("{table_location}/metadata/{hint}")
    }
}

// ---------------------------------------------------------------------------
// Warehouse-root resolution (catalog-less multi-table Direct mode)
// ---------------------------------------------------------------------------

/// The table-name part of a warehouse child directory: the segment before the
/// first `.` (a Snowflake-style random suffix, e.g. `fact_order.UIHGsQex`), with
/// any trailing `/` trimmed. A bare `fact_order/` yields `fact_order`.
pub fn warehouse_dir_name(dir: &str) -> &str {
    dir.trim_end_matches('/').split('.').next().unwrap_or("")
}

/// Resolve a table name to its directory under a warehouse root, given the root's
/// immediate child directory names. A catalog-less warehouse (e.g. a bucket copy
/// of a Snowflake-managed Iceberg database) stores each table in
/// `<name>.<random-suffix>/` or a bare `<name>/`. Matches the requested table
/// (namespace already stripped) to exactly one such directory, case-INSENSITIVE
/// on the name part. Ambiguity (two dirs matching one name) or a miss is a
/// fail-loud [`IcebergError::Catalog`] naming what WAS found.
pub fn match_warehouse_table_dir(table_name: &str, dir_names: &[String]) -> Result<String> {
    let want = table_name.trim();
    let matches: Vec<&String> = dir_names
        .iter()
        .filter(|d| warehouse_dir_name(d).eq_ignore_ascii_case(want))
        .collect();
    match matches.as_slice() {
        [one] => Ok((*one).trim_end_matches('/').to_string()),
        [] => Err(IcebergError::Catalog(format!(
            "no directory under the warehouse root matches table '{table_name}' \
             (matched on the name before '.', case-insensitive). Found {} directories: [{}]",
            dir_names.len(),
            dir_names.join(", ")
        ))),
        many => Err(IcebergError::Catalog(format!(
            "table '{table_name}' is AMBIGUOUS under the warehouse root — {} directories match: [{}]",
            many.len(),
            many.iter().map(|d| d.as_str()).collect::<Vec<_>>().join(", ")
        ))),
    }
}

#[cfg(test)]
mod warehouse_tests {
    use super::*;

    fn dirs(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn matches_snowflake_suffixed_dir_case_insensitively() {
        let d = dirs(&[
            "fact_order.UIHGsQex/",
            "dim_customer.AbCdEf/",
            "dim_date.ZzZz/",
        ]);
        // rr:tableName `DW.FACT_ORDER` arrives here namespace-stripped + upper.
        assert_eq!(
            match_warehouse_table_dir("FACT_ORDER", &d).unwrap(),
            "fact_order.UIHGsQex"
        );
        assert_eq!(
            match_warehouse_table_dir("dim_customer", &d).unwrap(),
            "dim_customer.AbCdEf"
        );
    }

    #[test]
    fn matches_bare_dir_without_suffix() {
        let d = dirs(&["fact_order/", "dim_customer/"]);
        assert_eq!(
            match_warehouse_table_dir("Fact_Order", &d).unwrap(),
            "fact_order"
        );
    }

    #[test]
    fn miss_lists_what_was_found() {
        let d = dirs(&["fact_order.X/", "dim_customer.Y/"]);
        let err = match_warehouse_table_dir("dim_geography", &d)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("dim_geography"),
            "names the missing table: {err}"
        );
        assert!(
            err.contains("fact_order.X") && err.contains("dim_customer.Y"),
            "lists candidates: {err}"
        );
    }

    #[test]
    fn ambiguity_is_a_loud_error_naming_candidates() {
        // Two dirs whose name part collides (a bare + a suffixed copy) — refuse.
        let d = dirs(&["fact_order/", "fact_order.NEWSUFFIX/"]);
        let err = match_warehouse_table_dir("fact_order", &d)
            .unwrap_err()
            .to_string();
        assert!(err.contains("AMBIGUOUS"), "flags ambiguity: {err}");
        assert!(
            err.contains("fact_order.NEWSUFFIX"),
            "lists both candidates: {err}"
        );
    }

    #[test]
    fn warehouse_dir_name_strips_suffix_and_slash() {
        assert_eq!(warehouse_dir_name("fact_order.UIHGsQex/"), "fact_order");
        assert_eq!(warehouse_dir_name("dim_customer/"), "dim_customer");
        assert_eq!(warehouse_dir_name("plain"), "plain");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryStorage;

    #[tokio::test]
    async fn test_direct_catalog_resolves_version_hint() {
        let mut storage = MemoryStorage::new();
        storage.add_file(
            "s3://bucket/table/metadata/version-hint.text",
            "00005-abcd-1234.metadata.json",
        );
        // We don't need the actual metadata file for load_table — just the location
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/00005-abcd-1234.metadata.json"
        );
        assert!(response.credentials.is_none());
    }

    #[tokio::test]
    async fn test_direct_catalog_missing_version_hint() {
        let storage = MemoryStorage::new();
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let result = client.load_table(&table_id, false).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("version-hint.text"), "Error: {err_msg}");
    }

    #[tokio::test]
    async fn test_direct_catalog_uuid_metadata_filename_hint() {
        // UUID-based naming: version-hint.text contains the full filename
        // (standard format used by Spark, iceberg-rust, AWS Glue, etc.)
        let mut storage = MemoryStorage::new();
        storage.add_file(
            "s3://bucket/table/metadata/version-hint.text",
            "00001-abcd-1234.metadata.json",
        );

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/00001-abcd-1234.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_direct_catalog_full_path_hint() {
        // Full absolute path in version-hint.text
        let mut storage = MemoryStorage::new();
        storage.add_file(
            "s3://bucket/table/metadata/version-hint.text",
            "s3://bucket/table/metadata/00002-efgh-5678.metadata.json",
        );

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/00002-efgh-5678.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_direct_catalog_version_hint_with_whitespace() {
        let mut storage = MemoryStorage::new();
        storage.add_file(
            "s3://bucket/table/metadata/version-hint.text",
            "00042-efgh-5678.metadata.json\n",
        );

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/00042-efgh-5678.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_direct_catalog_hadoop_integer_version_hint() {
        // Iceberg Hadoop file-based catalog convention: version-hint.text holds
        // a bare integer N; the metadata file is vN.metadata.json.
        let mut storage = MemoryStorage::new();
        storage.add_file("s3://bucket/table/metadata/version-hint.text", "1782390319");

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/v1782390319.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_direct_catalog_list_namespaces_unsupported() {
        let storage = MemoryStorage::new();
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let result = client.list_namespaces().await;
        assert!(result.is_err());
    }
}
