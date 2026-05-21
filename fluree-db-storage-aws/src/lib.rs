//! AWS storage backends for Fluree DB
//!
//! This crate provides AWS-specific storage implementations:
//!
//! - **S3 Storage** (`s3` feature): Read/write data to Amazon S3, with native support for
//!   S3 Express One Zone directory buckets (handled automatically by the SDK)
//! - **DynamoDB Nameservice** (`dynamodb` feature): Store ledger metadata in DynamoDB
//!   with conditional updates for monotonic publishing
//!
//! ## Features
//!
//! - `s3`: Enable S3 storage backend (includes S3 Express One Zone support)
//! - `dynamodb`: Enable DynamoDB nameservice backend
//!
//! ## Usage
//!
//! ```ignore
//! use fluree_db_storage_aws::s3::{S3Storage, S3Config};
//! use fluree_db_storage_aws::dynamodb::{DynamoDbNameService, DynamoDbConfig};
//!
//! // Load AWS SDK config
//! let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//!
//! // Create S3 storage (works with both standard and Express buckets)
//! let s3_config = S3Config {
//!     bucket: "my-bucket".to_string(),
//!     prefix: Some("ledgers".to_string()),
//!     timeout_ms: Some(30000),
//!     ..Default::default()
//! };
//! let storage = S3Storage::new(&sdk_config, s3_config).await?;
//!
//! // Create DynamoDB nameservice
//! let dynamo_config = DynamoDbConfig {
//!     table_name: "fluree-nameservice".to_string(),
//!     ..Default::default()
//! };
//! let nameservice = DynamoDbNameService::new(&sdk_config, dynamo_config).await?;
//! ```

pub mod error;

#[cfg(feature = "s3")]
pub mod s3;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

// Re-export main types
pub use error::{AwsStorageError, Result};

#[cfg(feature = "s3")]
pub use s3::{ListResult, S3Config, S3Storage};

#[cfg(feature = "dynamodb")]
pub use dynamodb::{DynamoDbConfig, DynamoDbNameService};

// Re-export core traits for convenience
pub use fluree_db_core::{
    Storage, StorageCas, StorageDelete, StorageExtError, StorageExtResult, StorageList,
    StorageWrite,
};
pub use fluree_db_nameservice::{NameService, NsRecord, Publisher};
