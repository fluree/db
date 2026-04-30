//! AWS connection handle for Lambda and cloud deployments
//!
//! Provides `AwsConnectionHandle` which owns AWS-specific storage components
//! (S3 for index/commit storage, DynamoDB or S3 for nameservice) and provides
//! methods for loading databases.
//!
//! ## Nameservice Options
//!
//! - **DynamoDB** (recommended for production): Fast, scalable, conditional updates
//! - **S3 Storage-backed**: Uses same S3 bucket as data, no separate DynamoDB table needed
//!
//! ## S3 Express One Zone Support
//!
//! S3 Express One Zone (directory buckets) is expected to be handled natively by
//! the AWS SDK - use an Express bucket name and the SDK should handle session
//! authentication automatically. **Validate with real AWS integration tests
//! before production use.**
//!
//! This module is used by the JSON-LD connection path (`connect_async`) when a
//! `ConnectionConfig` contains S3 storage. It intentionally does not expose a
//! separate AWS-specific configuration surface.

use crate::config::ConnectionConfig;
use crate::error::{ConnectionError, Result};
use fluree_db_core::LedgerSnapshot;
use fluree_db_nameservice::NameServicePublisher;
use fluree_db_storage_aws::S3Storage;
use once_cell::sync::OnceCell;
use std::sync::Arc;

/// Global AWS SDK config cache
///
/// Caches the SDK config to avoid repeated credential resolution
/// in Lambda environments where cold start latency matters.
static SDK_CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::new();

/// Full nameservice capability for AWS backends.
///
/// Extends [`NameServicePublisher`] with [`ReadWriteNameService`] to enable
/// trait object coercion for the background indexer. Both `DynamoDbNameService`
/// and `StorageNameService<S3Storage>` satisfy this automatically.
///
/// Replaces the former `AwsNameService` dispatch enum.
pub trait AwsNameServiceDyn:
    NameServicePublisher + fluree_db_nameservice::ReadWriteNameService
{
}

impl<T: NameServicePublisher> AwsNameServiceDyn for T {}

/// AWS-specific connection handle for Lambda deployments
///
/// S3Storage and nameservice are Clone (via `Arc`), so this handle can be
/// cheaply cloned for concurrent operations.
#[derive(Clone)]
pub struct AwsConnectionHandle {
    /// Connection configuration
    config: ConnectionConfig,
    /// S3 storage for indexes
    index_storage: S3Storage,
    /// Optional separate S3 storage for commits
    commit_storage: Option<S3Storage>,
    /// Nameservice (DynamoDB or S3 storage-backed), type-erased
    nameservice: Arc<dyn AwsNameServiceDyn>,
}

impl std::fmt::Debug for AwsConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsConnectionHandle")
            .field("index_storage", &self.index_storage)
            .field(
                "has_separate_commit_storage",
                &self.commit_storage.is_some(),
            )
            .field("nameservice", &self.nameservice)
            .finish()
    }
}

impl AwsConnectionHandle {
    /// Create a new AWS connection handle
    ///
    /// This is used internally by `create_aws_connection` for JSON-LD configs
    /// where storage sharing via registry is desired.
    pub fn new(
        config: ConnectionConfig,
        index_storage: S3Storage,
        commit_storage: Option<S3Storage>,
        nameservice: Arc<dyn AwsNameServiceDyn>,
    ) -> Self {
        Self {
            config,
            index_storage,
            commit_storage,
            nameservice,
        }
    }

    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Get a reference to the index storage
    pub fn index_storage(&self) -> &S3Storage {
        &self.index_storage
    }

    /// Get a reference to the commit storage
    ///
    /// Returns the index storage if no separate commit storage was configured.
    pub fn commit_storage(&self) -> &S3Storage {
        self.commit_storage.as_ref().unwrap_or(&self.index_storage)
    }

    /// Get the nameservice as a type-erased trait object.
    ///
    /// This `Arc` can be cloned and wrapped in `NameServiceMode::ReadWrite()`
    /// by higher-level layers.
    pub fn nameservice_arc(&self) -> &Arc<dyn AwsNameServiceDyn> {
        &self.nameservice
    }

    /// Load a database by ledger ID
    ///
    /// Uses the nameservice to look up the ledger's index root address,
    /// then loads the database from that address.
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - Ledger ID (e.g., "mydb" or "mydb:main")
    ///
    /// # Returns
    ///
    /// A `LedgerSnapshot` instance backed by S3 storage, or an error if the ledger
    /// is not found or has no index.
    pub async fn load_ledger_snapshot(&self, ledger_id: &str) -> Result<LedgerSnapshot> {
        let record = self
            .nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {e}")))?
            .ok_or_else(|| ConnectionError::not_found(format!("Ledger not found: {ledger_id}")))?;

        if record.retracted {
            return Err(ConnectionError::not_found(format!(
                "Ledger has been retracted: {ledger_id}"
            )));
        }

        let index_id = record.index_head_id.ok_or_else(|| {
            ConnectionError::not_found(format!("Ledger has no index yet: {ledger_id}"))
        })?;

        let storage = self.index_storage.clone();
        Ok(fluree_db_core::load_ledger_snapshot(&storage, &index_id, ledger_id).await?)
    }

    /// Look up a ledger record by ledger ID
    ///
    /// Returns the full `NsRecord` including commit and index IDs.
    pub async fn lookup(&self, ledger_id: &str) -> Result<Option<fluree_db_nameservice::NsRecord>> {
        self.nameservice
            .lookup(ledger_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Nameservice lookup failed: {e}")))
    }

    /// Publish a new index to the nameservice
    ///
    /// This is typically called by the indexer after successfully writing new index roots.
    pub async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &fluree_db_core::ContentId,
    ) -> Result<()> {
        self.nameservice
            .publish_index(ledger_id, index_t, index_id)
            .await
            .map_err(|e| ConnectionError::storage(format!("Publish index failed: {e}")))
    }
}

/// Get or initialize the AWS SDK config
///
/// Uses `OnceCell` to cache the config and avoid repeated credential
/// resolution in Lambda environments.
///
/// This is public for use by `create_aws_connection` which builds connections
/// from JSON-LD configs using the storage registry.
pub async fn get_or_init_sdk_config() -> Result<&'static aws_config::SdkConfig> {
    // Try to get existing config
    if let Some(config) = SDK_CONFIG.get() {
        return Ok(config);
    }

    // Load new config
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Try to set it (another thread may have beaten us)
    let _ = SDK_CONFIG.set(config);

    // Return the config (either ours or the one another thread set)
    SDK_CONFIG
        .get()
        .ok_or_else(|| ConnectionError::storage("Failed to initialize AWS SDK config"))
}

/// Load a freshly-resolved AWS SDK config WITHOUT touching the global cache.
///
/// Each call invokes `aws_config::load_defaults`, which builds a new
/// `SdkConfig` containing a new `HttpClient` (and therefore a new HTTP
/// connector pool). The returned `SdkConfig` is owned by the caller; when
/// it (and every AWS client built from it) is dropped, the underlying
/// connector pool is torn down — every TCP/TLS connection it held is
/// closed.
///
/// Use this in environments where AWS connection state must NOT survive
/// across logical "runs" — most notably AWS Lambda freeze/thaw, where a
/// long-lived `SdkConfig` cached in `SDK_CONFIG` would carry stale
/// connections (especially S3 Express session-bound TLS) across
/// invocations and can wedge subsequent requests.
///
/// Pair with [`crate::connect_from_config_with_sdk_config`] (and
/// `FlureeBuilder::with_aws_sdk_config` in the api layer) to ensure every
/// AWS client built by Fluree honours the fresh config rather than
/// silently falling back to [`get_or_init_sdk_config`].
pub async fn load_fresh_sdk_config() -> aws_config::SdkConfig {
    aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
}
