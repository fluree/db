//! # Fluree DB Connection
//!
//! Configuration parsing and connection initialization for Fluree databases.
//!
//! This crate provides:
//! - Configuration types for storage, cache, and connections
//! - [`ConnectionHandle`] for runtime-dispatched storage backends
//! - JSON-LD configuration parsing compatible with legacy configs
//!
//! ## Usage
//!
//! Most users should use `FlureeBuilder` in `fluree-db-api` rather than
//! constructing connections directly. This crate is primarily used internally
//! by the builder and for AWS connection initialization.

pub mod cache;
pub mod config;
pub mod error;
pub mod graph;
pub mod registry;
pub mod storage;
pub mod vocab;

#[cfg(feature = "aws")]
pub mod aws;

// Re-export main types
pub use cache::default_cache_max_mb;
pub use config::{CacheConfig, ConnectionConfig, StorageConfig, StorageType};
pub use error::{ConnectionError, Result};
pub use graph::ConfigGraph;

// AWS re-exports
#[cfg(feature = "aws")]
pub use aws::AwsConnectionHandle;

// Re-export core types commonly used with connections
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use fluree_db_core::FileStorage;
pub use fluree_db_core::{LedgerSnapshot, MemoryStorage};

/// Connection that can be file, memory, or AWS backed
///
/// This enum is returned by `connect()` and `connect_async()` when parsing
/// JSON-LD configs, where the storage type is determined at runtime from the config.
#[derive(Debug)]
pub enum ConnectionHandle {
    /// File-backed connection
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    File {
        config: ConnectionConfig,
        storage: FileStorage,
    },
    /// Memory-backed connection
    Memory {
        config: ConnectionConfig,
        storage: MemoryStorage,
    },
    /// AWS-backed connection (S3 storage, DynamoDB nameservice)
    #[cfg(feature = "aws")]
    Aws(AwsConnectionHandle),
}

impl ConnectionHandle {
    /// Get the connection configuration
    pub fn config(&self) -> &ConnectionConfig {
        match self {
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            ConnectionHandle::File { config, .. } => config,
            ConnectionHandle::Memory { config, .. } => config,
            #[cfg(feature = "aws")]
            ConnectionHandle::Aws(c) => c.config(),
        }
    }

    /// Load a database by root content ID and ledger ID.
    ///
    /// The storage address is derived from the `ContentId` and `ledger_id`
    /// using the storage backend's method identifier.
    pub async fn load_ledger_snapshot(
        &self,
        root_id: &fluree_db_core::ContentId,
        ledger_id: &str,
    ) -> Result<LedgerSnapshot> {
        match self {
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            ConnectionHandle::File { storage, .. } => {
                Ok(fluree_db_core::load_ledger_snapshot(storage, root_id, ledger_id).await?)
            }
            ConnectionHandle::Memory { storage, .. } => {
                Ok(fluree_db_core::load_ledger_snapshot(storage, root_id, ledger_id).await?)
            }
            #[cfg(feature = "aws")]
            ConnectionHandle::Aws(c) => {
                let storage = c.index_storage().clone();
                Ok(fluree_db_core::load_ledger_snapshot(&storage, root_id, ledger_id).await?)
            }
        }
    }
}

/// Create connection from JSON config (auto-detects format)
///
/// Accepts either:
/// - JSON-LD with @graph and @context
/// - Simple flat JSON format
///
/// # Example
///
/// ```ignore
/// use fluree_db_connection::connect;
/// use serde_json::json;
///
/// // JSON-LD format
/// let config = json!({
///     "@context": {
///         "@base": "https://ns.flur.ee/config/connection/",
///         "@vocab": "https://ns.flur.ee/system#"
///     },
///     "@graph": [
///         {"@id": "fileStorage", "@type": "Storage", "filePath": "/data"},
///         {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "fileStorage"}}
///     ]
/// });
/// let conn = connect(&config)?;
/// ```
pub fn connect(config_json: &serde_json::Value) -> Result<ConnectionHandle> {
    // Check if this is JSON-LD format
    if fluree_graph_json_ld::is_json_ld(config_json) {
        let config = ConnectionConfig::from_json_ld(config_json)?;
        create_sync_connection(config)
    } else {
        // Fall back to simple flat format
        let config = ConnectionConfig::from_json(config_json)?;
        create_sync_connection(config)
    }
}

/// Create connection from JSON config asynchronously
///
/// This is the async version of `connect()` that supports AWS backends (S3, DynamoDB).
/// Use this when your config includes S3 storage or DynamoDB nameservice.
///
/// # Example
///
/// ```ignore
/// use fluree_db_connection::connect_async;
/// use serde_json::json;
///
/// let config = json!({
///     "@context": {
///         "@base": "https://ns.flur.ee/config/connection/",
///         "@vocab": "https://ns.flur.ee/system#"
///     },
///     "@graph": [
///         {"@id": "s3Storage", "@type": "Storage", "s3Bucket": "my-bucket"},
///         {"@id": "conn", "@type": "Connection",
///          "indexStorage": {"@id": "s3Storage"},
///          "primaryPublisher": {"@type": "Publisher", "dynamodbTable": "fluree-ns"}}
///     ]
/// });
/// let conn = connect_async(&config).await?;
/// let db = conn.load_ledger_snapshot(&root_id, "mydb:main").await?;
/// ```
pub async fn connect_async(config_json: &serde_json::Value) -> Result<ConnectionHandle> {
    // Check if this is JSON-LD format
    let config = if fluree_graph_json_ld::is_json_ld(config_json) {
        ConnectionConfig::from_json_ld(config_json)?
    } else {
        ConnectionConfig::from_json(config_json)?
    };

    create_async_connection(config).await
}

/// Create an async connection from a pre-parsed `ConnectionConfig`.
///
/// This avoids the JSON-LD parse round-trip when the config has already been
/// parsed (e.g., by `FlureeBuilder::from_json_ld`).
pub async fn connect_from_config(config: ConnectionConfig) -> Result<ConnectionHandle> {
    create_async_connection(config).await
}

/// Sync connection creation for local backends (file, memory)
fn create_sync_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::File => {
            #[cfg(not(all(feature = "native", not(target_arch = "wasm32"))))]
            {
                return Err(ConnectionError::unsupported_component(
                    "https://ns.flur.ee/system#filePath (native feature disabled)",
                ));
            }
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            {
                let path =
                    config.index_storage.path.as_ref().ok_or_else(|| {
                        ConnectionError::invalid_config("File storage requires path")
                    })?;
                let storage = FileStorage::new(path.as_ref());
                Ok(ConnectionHandle::File { config, storage })
            }
        }
        StorageType::Memory => {
            let storage = MemoryStorage::new();
            Ok(ConnectionHandle::Memory { config, storage })
        }
        StorageType::S3(_) => Err(ConnectionError::invalid_config(
            "S3 storage requires async initialization. Use connect_async() instead of connect().",
        )),
        StorageType::Unsupported { type_iri, .. } => {
            Err(ConnectionError::unsupported_component(type_iri))
        }
    }
}

/// Async connection creation that supports all backends
#[cfg(feature = "aws")]
async fn create_async_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::S3(s3_config) => create_aws_connection(config.clone(), s3_config).await,
        StorageType::File => {
            #[cfg(not(all(feature = "native", not(target_arch = "wasm32"))))]
            {
                return Err(ConnectionError::unsupported_component(
                    "https://ns.flur.ee/system#filePath (native feature disabled)",
                ));
            }
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            {
                let path =
                    config.index_storage.path.as_ref().ok_or_else(|| {
                        ConnectionError::invalid_config("File storage requires path")
                    })?;
                let storage = FileStorage::new(path.as_ref());
                Ok(ConnectionHandle::File { config, storage })
            }
        }
        StorageType::Memory => {
            let storage = MemoryStorage::new();
            Ok(ConnectionHandle::Memory { config, storage })
        }
        StorageType::Unsupported { type_iri, .. } => {
            Err(ConnectionError::unsupported_component(type_iri))
        }
    }
}

/// Create async connection without AWS feature - fallback to sync
#[cfg(not(feature = "aws"))]
async fn create_async_connection(config: ConnectionConfig) -> Result<ConnectionHandle> {
    match &config.index_storage.storage_type {
        StorageType::S3(_) => Err(ConnectionError::unsupported_component(
            "S3 storage requires the 'aws' feature to be enabled",
        )),
        _ => create_sync_connection(config),
    }
}

/// Create AWS connection from parsed JSON-LD config
///
/// Uses StorageRegistry for storage sharing when the same @id is referenced
/// multiple times (e.g., index and commit storage pointing to same storage node).
#[cfg(feature = "aws")]
async fn create_aws_connection(
    config: ConnectionConfig,
    _s3_config: &config::S3StorageConfig,
) -> Result<ConnectionHandle> {
    use crate::config::PublisherType;
    use crate::registry::StorageRegistry;
    use fluree_db_nameservice::StorageNameService;
    use fluree_db_storage_aws::{DynamoDbConfig as RawDynamoDbConfig, DynamoDbNameService};
    use std::sync::Arc;

    // Get or initialize SDK config
    let sdk_config = aws::get_or_init_sdk_config().await?;

    // Create storage registry for @id-based sharing
    let mut registry = StorageRegistry::new();

    // Create index storage via registry (enables sharing)
    let index_storage = registry
        .get_or_create_s3(sdk_config, &config.index_storage)
        .await?;

    // Create commit storage if configured and different from index
    // Registry handles sharing if same @id
    let commit_storage = if let Some(commit_config) = &config.commit_storage {
        match &commit_config.storage_type {
            StorageType::S3(_) => {
                let storage = registry.get_or_create_s3(sdk_config, commit_config).await?;
                // Only keep separate if actually different instance
                if Arc::ptr_eq(&storage, &index_storage) {
                    None
                } else {
                    Some(storage)
                }
            }
            _ => None, // Non-S3 commit storage not supported with S3 index
        }
    } else {
        None
    };

    // Create nameservice based on publisher config
    let nameservice = match &config.primary_publisher {
        Some(pub_config) => match &pub_config.publisher_type {
            PublisherType::DynamoDb {
                table,
                region,
                endpoint,
                timeout_ms,
                ..
            } => {
                let dynamo_config = RawDynamoDbConfig {
                    table_name: table.to_string(),
                    region: region.as_ref().map(std::string::ToString::to_string),
                    endpoint: endpoint.as_ref().map(std::string::ToString::to_string),
                    timeout_ms: *timeout_ms,
                };
                let ns = DynamoDbNameService::new(sdk_config, dynamo_config)
                    .await
                    .map_err(|e| {
                        ConnectionError::storage(format!(
                            "Failed to create DynamoDB nameservice: {e}"
                        ))
                    })?;
                Arc::new(ns) as Arc<dyn aws::AwsNameServiceDyn>
            }
            PublisherType::Storage { storage } => {
                // Storage-backed nameservice - use registry for storage sharing
                let ns_storage = match &storage.storage_type {
                    StorageType::S3(_) => registry.get_or_create_s3(sdk_config, storage).await?,
                    _ => {
                        return Err(ConnectionError::invalid_config(
                            "Storage-backed publisher requires S3 storage",
                        ))
                    }
                };
                // StorageNameService prefix is empty - S3Storage has the bucket prefix
                let ns = StorageNameService::new((*ns_storage).clone(), "");
                Arc::new(ns) as Arc<dyn aws::AwsNameServiceDyn>
            }
            PublisherType::Unsupported { type_iri, .. } => {
                return Err(ConnectionError::unsupported_component(type_iri));
            }
        },
        None => {
            // Default DynamoDB nameservice
            let dynamo_config = RawDynamoDbConfig {
                table_name: "fluree-nameservice".to_string(),
                region: None,
                endpoint: None,
                timeout_ms: None,
            };
            let ns = DynamoDbNameService::new(sdk_config, dynamo_config)
                .await
                .map_err(|e| {
                    ConnectionError::storage(format!("Failed to create DynamoDB nameservice: {e}"))
                })?;
            Arc::new(ns) as Arc<dyn aws::AwsNameServiceDyn>
        }
    };

    // Build connection handle directly (bypasses connect_aws to use registry-shared storage)
    let handle = aws::AwsConnectionHandle::new(
        config,
        (*index_storage).clone(),
        commit_storage.map(|s| (*s).clone()),
        nameservice,
    );

    Ok(ConnectionHandle::Aws(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_handle_memory() {
        let handle = ConnectionHandle::Memory {
            config: ConnectionConfig::memory(),
            storage: MemoryStorage::new(),
        };
        assert!(format!("{handle:?}").contains("MemoryStorage"));
        assert_eq!(
            handle.config().parallelism,
            ConnectionConfig::default().parallelism
        );
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_connection_handle_file() {
        let handle = ConnectionHandle::File {
            config: ConnectionConfig::file("/tmp/test"),
            storage: FileStorage::new("/tmp/test"),
        };
        assert!(format!("{handle:?}").contains("FileStorage"));
    }
}
