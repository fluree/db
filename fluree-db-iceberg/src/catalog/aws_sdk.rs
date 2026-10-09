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
//! vends credentials, so ambient reads suffice. Glue tables readable only with
//! Lake-Formation-vended credentials are not supported yet (fluree/db#1456).
//!
//! The SDK config comes from `aws_config::defaults`, so the standard AWS
//! endpoint overrides apply (`AWS_ENDPOINT_URL_GLUE`, `AWS_ENDPOINT_URL_S3TABLES`,
//! `AWS_ENDPOINT_URL`, or `endpoint_url` in the shared config file) — that is how
//! a private endpoint or a local mock (moto) is reached.
//!
//! Gated behind the `aws` feature.

use crate::catalog::{LoadTableResponse, SendCatalogClient, TableIdentifier};
use crate::error::{IcebergError, Result};
use crate::io::storage::error_chain;
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

/// How a failed catalog call should surface. Split out from the SDK call sites
/// (like `is_s3_access_denied` in `io/storage.rs`) so the decision is
/// unit-testable without constructing `SdkError` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SdkFailure {
    /// The database / table / namespace does not exist.
    NotFound,
    /// The ambient identity may not call this catalog operation (a missing IAM
    /// or Lake Formation grant).
    AccessDenied,
    /// Anything else (throttling, timeouts, malformed input, ...).
    Other,
}

/// Classify a catalog SDK error by its modeled error code, falling back to the
/// raw HTTP status. Glue reports a missing database or table as
/// `EntityNotFoundException`; S3 Tables as `NotFoundException`. Glue's denial is
/// unmodeled (`AccessDeniedException`, HTTP 400), as are its credential failures
/// (`ExpiredTokenException`, `UnrecognizedClientException`,
/// `InvalidSignatureException`); S3 Tables' are `ForbiddenException` /
/// `AccessDeniedException` (HTTP 403).
fn classify_sdk_failure(code: Option<&str>, http_status: Option<u16>) -> SdkFailure {
    match code {
        Some("EntityNotFoundException" | "NotFoundException") => SdkFailure::NotFound,
        // A refusal, or credentials the service will not accept: Glue reports an
        // expired session or an unknown key as HTTP 400 codes, so match the code,
        // and an expired session reads as a denial on both catalogs.
        Some(
            "AccessDeniedException"
            | "ForbiddenException"
            | "ExpiredTokenException"
            | "UnrecognizedClientException"
            | "InvalidSignatureException",
        ) => SdkFailure::AccessDenied,
        _ => match http_status {
            Some(404) => SdkFailure::NotFound,
            Some(403) => SdkFailure::AccessDenied,
            _ => SdkFailure::Other,
        },
    }
}

/// Build the [`IcebergError`] for a failed catalog call. `what` names the
/// object (`"Glue table demo.orders"`); `detail` is the error with its full
/// source chain — an `SdkError` alone renders as the bare `"service error"`.
fn sdk_catalog_error(op: &str, what: &str, failure: SdkFailure, detail: String) -> IcebergError {
    match failure {
        SdkFailure::NotFound => IcebergError::TableNotFound(format!("{what} not found: {detail}")),
        // `load_table` renames `table` to the table identifier (`name_denied_table`).
        SdkFailure::AccessDenied => IcebergError::CatalogAccessDenied {
            table: what.to_string(),
            message: format!("{op} on {what}: {detail}"),
        },
        SdkFailure::Other => IcebergError::Catalog(format!("{op} on {what} failed: {detail}")),
    }
}

/// Map an `SdkError` from a catalog call to an [`IcebergError`]. Glue and S3
/// Tables share one smithy runtime, so Glue's `SdkError` alias names both.
fn map_sdk_error<E>(op: &str, what: &str, err: &aws_sdk_glue::error::SdkError<E>) -> IcebergError
where
    E: aws_sdk_glue::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    use aws_sdk_glue::error::ProvideErrorMetadata;
    let status = err.raw_response().map(|r| r.status().as_u16());
    sdk_catalog_error(
        op,
        what,
        classify_sdk_failure(err.code(), status),
        error_chain(err),
    )
}

/// Extract the Iceberg `metadata_location` from a Glue table's `Parameters`.
///
/// Iceberg writers that register with Glue (Spark, Trino, pyiceberg, Athena)
/// record the current metadata file there; the catalog pointer is authoritative,
/// never a listing of `metadata/`. A Glue table without it is not an Iceberg
/// table (e.g. a Hive/Parquet table) and is refused with a clear error rather
/// than read as empty.
fn glue_metadata_location(
    parameters: Option<&HashMap<String, String>>,
    namespace: &str,
    table: &str,
) -> Result<String> {
    if let Some(location) = parameters.and_then(|p| p.get("metadata_location")) {
        return Ok(location.clone());
    }
    let table_type = parameters
        .and_then(|p| p.get("table_type"))
        .map_or_else(String::new, |t| format!(" (table_type={t})"));
    Err(IcebergError::Metadata(format!(
        "Glue table {namespace}.{table} is not an Iceberg table{table_type}: it has no \
         `metadata_location` parameter"
    )))
}

/// Whether a Glue table (as listed by `GetTables`) is an Iceberg table — the
/// same test [`glue_metadata_location`] applies on load, so browse never offers
/// a table that a load would refuse.
fn glue_is_iceberg(parameters: Option<&HashMap<String, String>>) -> bool {
    parameters.is_some_and(|p| p.contains_key("metadata_location"))
}

// ---------------------------------------------------------------------------
// AWS Glue Data Catalog
// ---------------------------------------------------------------------------

/// Catalog client backed by the AWS Glue Data Catalog via `aws-sdk-glue`.
///
/// `load_table` resolves the Iceberg `metadata_location` from Glue `GetTable`
/// (stored in the table's `Parameters`). The Glue *database* is the namespace.
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
    ///
    /// `region` is the catalog's region (see
    /// [`CatalogConfig::aws_catalog_region`](crate::config::CatalogConfig::aws_catalog_region));
    /// `None` defers to the SDK's region chain.
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
        // GetDatabases pages (100 per page by default); read every page.
        let mut pages = self
            .client
            .get_databases()
            .set_catalog_id(self.catalog_id.clone())
            .into_paginator()
            .send();
        let mut names = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| map_sdk_error("Glue GetDatabases", "Glue catalog", &e))?;
            names.extend(page.database_list().iter().map(|d| d.name().to_string()));
        }
        Ok(names)
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        // GetTables pages too, and lists every table in the database: keep only
        // the Iceberg ones (a Hive table would only fail on load).
        let what = format!("Glue database {namespace}");
        let mut pages = self
            .client
            .get_tables()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(namespace)
            .into_paginator()
            .send();
        let mut names = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| map_sdk_error("Glue GetTables", &what, &e))?;
            names.extend(
                page.table_list()
                    .iter()
                    .filter(|t| glue_is_iceberg(t.parameters()))
                    .map(|t| t.name().to_string()),
            );
        }
        Ok(names)
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let what = format!("Glue table {}.{}", table_id.namespace, table_id.table);
        let out = self
            .client
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(&table_id.namespace)
            .name(&table_id.table)
            .send()
            .await
            .map_err(|e| {
                super::name_denied_table(map_sdk_error("Glue GetTable", &what, &e), table_id)
            })?;

        let table = out
            .table()
            .ok_or_else(|| IcebergError::TableNotFound(format!("{what} not found")))?;

        let metadata_location =
            glue_metadata_location(table.parameters(), &table_id.namespace, &table_id.table)?;

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
        let what = format!("S3 table bucket {}", self.table_bucket_arn);
        let mut pages = self
            .client
            .list_namespaces()
            .table_bucket_arn(&self.table_bucket_arn)
            .into_paginator()
            .send();
        let mut names = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| map_sdk_error("S3Tables ListNamespaces", &what, &e))?;
            // S3 Tables namespaces are single-level.
            names.extend(
                page.namespaces()
                    .iter()
                    .filter_map(|n| n.namespace().first().cloned()),
            );
        }
        Ok(names)
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let what = format!("S3 Tables namespace {namespace}");
        let mut pages = self
            .client
            .list_tables()
            .table_bucket_arn(&self.table_bucket_arn)
            .namespace(namespace)
            .into_paginator()
            .send();
        let mut names = Vec::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| map_sdk_error("S3Tables ListTables", &what, &e))?;
            names.extend(page.tables().iter().map(|t| t.name().to_string()));
        }
        Ok(names)
    }

    async fn load_table(
        &self,
        table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let what = format!("S3 Tables table {}.{}", table_id.namespace, table_id.table);
        let out = self
            .client
            .get_table_metadata_location()
            .table_bucket_arn(&self.table_bucket_arn)
            .namespace(&table_id.namespace)
            .name(&table_id.table)
            .send()
            .await
            .map_err(|e| {
                super::name_denied_table(
                    map_sdk_error("S3Tables GetTableMetadataLocation", &what, &e),
                    table_id,
                )
            })?;

        let metadata_location = out
            .metadata_location()
            .ok_or_else(|| {
                IcebergError::Metadata(format!(
                    "{what} has no metadata location (a table with no committed snapshot?)"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glue_metadata_location_extracted_from_parameters() {
        let mut params = HashMap::new();
        params.insert("table_type".to_string(), "ICEBERG".to_string());
        params.insert(
            "metadata_location".to_string(),
            "s3://bucket/sales/orders/metadata/00001-abc.metadata.json".to_string(),
        );
        assert_eq!(
            glue_metadata_location(Some(&params), "sales", "orders").unwrap(),
            "s3://bucket/sales/orders/metadata/00001-abc.metadata.json"
        );
        assert!(glue_is_iceberg(Some(&params)));
    }

    #[test]
    fn glue_non_iceberg_table_is_a_clear_error() {
        // A Hive/Parquet table registered in Glue has no `metadata_location`: it
        // must be a clear error naming the table, never a panic or an empty read.
        let params = HashMap::from([
            ("classification".to_string(), "parquet".to_string()),
            ("table_type".to_string(), "EXTERNAL_TABLE".to_string()),
        ]);
        let err = glue_metadata_location(Some(&params), "db", "t")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not an Iceberg table"),
            "unhelpful error: {err}"
        );
        assert!(err.contains("metadata_location"), "unhelpful error: {err}");
        assert!(err.contains("table_type=EXTERNAL_TABLE"), "{err}");
        assert!(err.contains("db.t"), "error should name the table: {err}");
        assert!(glue_metadata_location(None, "db", "t").is_err());
        assert!(!glue_is_iceberg(Some(&params)));
        assert!(!glue_is_iceberg(None));
    }

    #[test]
    fn sdk_failures_classify_by_code_then_status() {
        use SdkFailure::*;
        assert_eq!(
            classify_sdk_failure(Some("EntityNotFoundException"), Some(400)),
            NotFound
        );
        assert_eq!(
            classify_sdk_failure(Some("NotFoundException"), Some(404)),
            NotFound
        );
        assert_eq!(
            classify_sdk_failure(Some("AccessDeniedException"), Some(400)),
            AccessDenied
        );
        assert_eq!(
            classify_sdk_failure(Some("ForbiddenException"), Some(403)),
            AccessDenied
        );
        assert_eq!(classify_sdk_failure(None, Some(404)), NotFound);
        assert_eq!(classify_sdk_failure(None, Some(403)), AccessDenied);
        // Glue's credential failures arrive as HTTP 400 codes, not a 403.
        for code in [
            "ExpiredTokenException",
            "UnrecognizedClientException",
            "InvalidSignatureException",
        ] {
            assert_eq!(
                classify_sdk_failure(Some(code), Some(400)),
                AccessDenied,
                "{code}"
            );
        }
        assert_eq!(
            classify_sdk_failure(Some("ThrottlingException"), Some(400)),
            Other
        );
        assert_eq!(classify_sdk_failure(None, None), Other);
    }

    #[test]
    fn a_denied_load_names_its_table() {
        let id = TableIdentifier {
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        };
        let denied = sdk_catalog_error(
            "Glue GetTable",
            "Glue table sales.orders",
            SdkFailure::AccessDenied,
            "AccessDeniedException".into(),
        );
        match super::super::name_denied_table(denied, &id) {
            IcebergError::CatalogAccessDenied { table, message } => {
                assert_eq!(table, "sales.orders");
                assert!(
                    message.starts_with("Glue GetTable on Glue table sales.orders"),
                    "{message}"
                );
            }
            other => panic!("expected CatalogAccessDenied, got {other:?}"),
        }
        // Any other error passes through untouched.
        let other = IcebergError::Catalog("x".into());
        assert!(matches!(
            super::super::name_denied_table(other, &id),
            IcebergError::Catalog(_)
        ));
    }

    #[test]
    fn sdk_catalog_errors_name_the_object_and_keep_the_detail() {
        let not_found = sdk_catalog_error(
            "Glue GetTable",
            "Glue table demo.nope",
            SdkFailure::NotFound,
            "service error: EntityNotFoundException: Table nope not found".to_string(),
        );
        assert!(matches!(not_found, IcebergError::TableNotFound(_)));
        let msg = not_found.to_string();
        assert!(msg.contains("Glue table demo.nope not found"), "{msg}");
        assert!(msg.contains("EntityNotFoundException"), "{msg}");

        let denied = sdk_catalog_error(
            "Glue GetTable",
            "Glue table a.b",
            SdkFailure::AccessDenied,
            "AccessDeniedException: not authorized to perform glue:GetTable".into(),
        );
        match &denied {
            IcebergError::CatalogAccessDenied { table, message } => {
                assert_eq!(table, "Glue table a.b");
                assert!(
                    message.contains("Glue GetTable on Glue table a.b"),
                    "{message}"
                );
                assert!(message.contains("glue:GetTable"), "{message}");
            }
            other => panic!("expected CatalogAccessDenied, got {other:?}"),
        }
        let other = sdk_catalog_error(
            "Glue GetTable",
            "Glue table a.b",
            SdkFailure::Other,
            "x".into(),
        );
        assert!(matches!(other, IcebergError::Catalog(_)), "{other:?}");
    }
}
