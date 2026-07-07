//! AWS-SDK-backed Iceberg catalog clients: AWS Glue Data Catalog + S3 Tables.
//!
//! Unlike the REST catalog client (`rest.rs`), these reach the catalog through
//! the native AWS SDK (`aws-sdk-glue` / `aws-sdk-s3tables`): the SDK performs
//! SigV4 signing, credential resolution, refresh, and retries — no request
//! signing, REST prefixing, or credential vending lives here. Each client only
//! resolves a table's current metadata-JSON *location*; the metadata, manifests
//! and data files are then read from S3 by the shared scan path using the ambient
//! AWS credential chain (`S3IcebergStorage::from_default_chain`) — the exact same
//! reader the Direct catalog uses.
//!
//! This is the empirically-chosen path for AWS catalogs: for a normally
//! S3-authorized principal, neither Glue Data Catalog (IAM mode) nor S3 Tables
//! vends credentials, so ambient reads suffice. Lake-Formation-governed catalogs
//! that require *vended* credentials remain the REST client's domain.
//!
//! Gated behind the `aws` feature.

use crate::catalog::{LoadTableResponse, SendCatalogClient, TableIdentifier};
use crate::error::{IcebergError, Result};
use async_trait::async_trait;
use std::collections::HashMap;

/// Build an `SdkConfig` from the ambient credential chain, optionally pinning a region.
async fn sdk_config(region: Option<&str>) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(r) = region {
        loader = loader.region(aws_config::Region::new(r.to_string()));
    }
    loader.load().await
}

// ---------------------------------------------------------------------------
// AWS Glue Data Catalog
// ---------------------------------------------------------------------------

/// Catalog client backed by the AWS Glue Data Catalog via `aws-sdk-glue`.
///
/// `load_table` resolves the Iceberg `metadata_location` from Glue `GetTable`
/// (stored in the table's `Parameters`). The namespace/table come from the
/// graph source's `TableConfig`; the Glue *database* is the namespace.
pub struct GlueSdkCatalogClient {
    client: aws_sdk_glue::Client,
    /// Glue catalog id for cross-account access (`None` = the caller's account).
    catalog_id: Option<String>,
}

impl std::fmt::Debug for GlueSdkCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueSdkCatalogClient")
            .field("catalog_id", &self.catalog_id)
            .finish()
    }
}

impl GlueSdkCatalogClient {
    /// Build a Glue catalog client from the ambient AWS credential chain.
    pub async fn new(region: Option<&str>, catalog_id: Option<String>) -> Result<Self> {
        let cfg = sdk_config(region).await;
        Ok(Self {
            client: aws_sdk_glue::Client::new(&cfg),
            catalog_id,
        })
    }
}

#[async_trait]
impl SendCatalogClient for GlueSdkCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let out = self
            .client
            .get_databases()
            .set_catalog_id(self.catalog_id.clone())
            .send()
            .await
            .map_err(|e| IcebergError::Catalog(format!("Glue GetDatabases failed: {e}")))?;
        Ok(out
            .database_list()
            .iter()
            .map(|d| d.name().to_string())
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let out = self
            .client
            .get_tables()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(namespace)
            .send()
            .await
            .map_err(|e| IcebergError::Catalog(format!("Glue GetTables failed: {e}")))?;
        Ok(out
            .table_list()
            .iter()
            .map(|t| t.name().to_string())
            .collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let out = self
            .client
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(&table_id.namespace)
            .name(&table_id.table)
            .send()
            .await
            .map_err(|e| {
                IcebergError::Catalog(format!(
                    "Glue GetTable {}.{} failed: {e}",
                    table_id.namespace, table_id.table
                ))
            })?;

        let table = out.table().ok_or_else(|| {
            IcebergError::TableNotFound(format!(
                "Glue table {}.{} not found",
                table_id.namespace, table_id.table
            ))
        })?;

        // Iceberg tables registered in Glue carry `metadata_location` in Parameters.
        let metadata_location = table
            .parameters()
            .and_then(|p| p.get("metadata_location"))
            .cloned()
            .ok_or_else(|| {
                IcebergError::Metadata(format!(
                    "Glue table {}.{} has no `metadata_location` parameter (not an Iceberg table?)",
                    table_id.namespace, table_id.table
                ))
            })?;

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None, // ambient AWS credential chain reads S3
            metadata: None,
        })
    }
}

// ---------------------------------------------------------------------------
// AWS S3 Tables
// ---------------------------------------------------------------------------

/// Catalog client backed by AWS S3 Tables via `aws-sdk-s3tables`.
///
/// `load_table` resolves the Iceberg metadata location from
/// `GetTableMetadataLocation`. Data files live in the S3-Tables-managed bucket
/// and are read with the ambient credential chain.
pub struct S3TablesSdkCatalogClient {
    client: aws_sdk_s3tables::Client,
    table_bucket_arn: String,
}

impl std::fmt::Debug for S3TablesSdkCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3TablesSdkCatalogClient")
            .field("table_bucket_arn", &self.table_bucket_arn)
            .finish()
    }
}

impl S3TablesSdkCatalogClient {
    /// Build an S3 Tables catalog client from the ambient AWS credential chain.
    pub async fn new(region: Option<&str>, table_bucket_arn: String) -> Result<Self> {
        let cfg = sdk_config(region).await;
        Ok(Self {
            client: aws_sdk_s3tables::Client::new(&cfg),
            table_bucket_arn,
        })
    }
}

#[async_trait]
impl SendCatalogClient for S3TablesSdkCatalogClient {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let out = self
            .client
            .list_namespaces()
            .table_bucket_arn(&self.table_bucket_arn)
            .send()
            .await
            .map_err(|e| IcebergError::Catalog(format!("S3Tables ListNamespaces failed: {e}")))?;
        Ok(out
            .namespaces()
            .iter()
            .filter_map(|n| n.namespace().first().cloned())
            .collect())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let out = self
            .client
            .list_tables()
            .table_bucket_arn(&self.table_bucket_arn)
            .namespace(namespace)
            .send()
            .await
            .map_err(|e| IcebergError::Catalog(format!("S3Tables ListTables failed: {e}")))?;
        Ok(out.tables().iter().map(|t| t.name().to_string()).collect())
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let out = self
            .client
            .get_table_metadata_location()
            .table_bucket_arn(&self.table_bucket_arn)
            .namespace(&table_id.namespace)
            .name(&table_id.table)
            .send()
            .await
            .map_err(|e| {
                IcebergError::Catalog(format!(
                    "S3Tables GetTableMetadataLocation {}.{} failed: {e}",
                    table_id.namespace, table_id.table
                ))
            })?;

        let metadata_location = out
            .metadata_location()
            .ok_or_else(|| {
                IcebergError::Metadata(format!(
                    "S3Tables table {}.{} has no metadata location",
                    table_id.namespace, table_id.table
                ))
            })?
            .to_string();

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None,
            metadata: None,
        })
    }
}
