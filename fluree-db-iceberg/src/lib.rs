//! Iceberg REST catalog support for Fluree DB graph sources.
//!
//! This crate provides Iceberg table access via REST catalogs (primarily Polaris),
//! including authentication, vended credentials, metadata loading, and data scanning.
//!
//! # Features
//!
//! - `aws` - Enables AWS SDK integration (S3 storage + credential provider)
//! - `arrow` - Enables Arrow RecordBatch output (OFF by default for Lambda size)
//!
//! # Architecture
//!
//! The crate is organized into several modules:
//!
//! - [`auth`] - Authentication providers (bearer tokens, OAuth2 client credentials)
//! - [`catalog`] - REST catalog client for table discovery and metadata location
//! - [`credential`] - Vended credentials handling and caching
//! - [`metadata`] - Iceberg table metadata parsing and snapshot selection
//! - [`config`] - Graph source configuration schemas for nameservice integration
//! - [`manifest`] - Manifest list and manifest file parsing (Avro format)
//! - [`scan`] - Scan planning with partition/file pruning
//! - [`io`] - Storage abstraction and Parquet reading
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_iceberg::{
//!     auth::{AuthConfig, BearerTokenAuth},
//!     catalog::{RestCatalogClient, RestCatalogConfig, CatalogClient},
//!     config::IcebergGsConfig,
//!     scan::{ScanConfig, ScanPlanner},
//! };
//!
//! // Create a catalog client with bearer token auth
//! let config = RestCatalogConfig {
//!     uri: "https://polaris.example.com".to_string(),
//!     ..Default::default()
//! };
//! let auth = BearerTokenAuth::new("my-token".to_string());
//! let client = RestCatalogClient::new(config, Arc::new(auth))?;
//!
//! // Load table metadata
//! let table_id = parse_table_identifier("openflights.airlines")?;
//! let response = client.load_table(&table_id, true).await?;
//!
//! // Plan a scan with projection and filter
//! let scan_config = ScanConfig::new()
//!     .with_projection(vec![1, 2])  // field IDs
//!     .with_filter(Expression::gt(1, "id", LiteralValue::Int64(100)));
//! let planner = ScanPlanner::new(&storage, &metadata, scan_config);
//! let plan = planner.plan_scan().await?;
//! ```

pub mod auth;
pub mod catalog;
pub mod config;
pub mod config_value;
pub mod credential;
pub mod error;
pub mod io;
pub mod manifest;
pub mod metadata;
pub mod scan;

// Re-export commonly used types
pub use config::{CatalogConfig, IcebergGsConfig, IoConfig, MappingSource, TableConfig};
pub use config_value::ConfigValue;
pub use error::{IcebergError, Result};

// Re-export Phase 2 types for convenience
pub use io::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType, IcebergStorage};
pub use manifest::{DataFile, ManifestContent, ManifestEntry, ManifestListEntry, TypedValue};
pub use scan::{
    ComparisonOp, Expression, FileScanTask, LiteralValue, ScanConfig, ScanPlan, ScanPlanner,
};

// AWS/Send-safe types
#[cfg(feature = "aws")]
pub use catalog::SendDirectCatalogClient;
#[cfg(feature = "aws")]
pub use io::{S3IcebergStorage, SendIcebergStorage, SendParquetReader};
#[cfg(feature = "aws")]
pub use scan::SendScanPlanner;
