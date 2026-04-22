//! Storage registry for sharing storage instances across connection components
//!
//! The `StorageRegistry` provides instance sharing when the same storage is referenced
//! multiple times in a JSON-LD config (via @id). This matches the legacy storage catalog
//! pattern where storage backends are created once and shared across commit storage,
//! index storage, and nameservice.
//!
//! # Example
//!
//! In JSON-LD config, when the same storage is used for both index and commit:
//!
//! ```json
//! {
//!   "@graph": [
//!     {"@id": "s3Storage", "@type": "Storage", "s3Bucket": "my-bucket"},
//!     {"@id": "conn", "@type": "Connection",
//!      "indexStorage": {"@id": "s3Storage"},
//!      "commitStorage": {"@id": "s3Storage"}}
//!   ]
//! }
//! ```
//!
//! The registry ensures only one `S3Storage` instance is created and shared.

#[cfg(feature = "aws")]
use crate::config::{StorageConfig, StorageType};
#[cfg(feature = "aws")]
use crate::error::{ConnectionError, Result};
#[cfg(feature = "aws")]
use std::collections::HashMap;
#[cfg(feature = "aws")]
use std::sync::Arc;

#[cfg(feature = "aws")]
use fluree_db_storage_aws::S3Storage;

/// A registry that caches storage instances by their @id
///
/// When building connections from JSON-LD configs, the same storage @id may be
/// referenced multiple times (index_storage, commit_storage, publisher storage).
/// The registry ensures we create each storage instance only once.
#[derive(Default)]
pub struct StorageRegistry {
    /// Cached S3 storage instances by @id
    #[cfg(feature = "aws")]
    s3_storages: HashMap<String, Arc<S3Storage>>,
}

impl StorageRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create an S3 storage instance
    ///
    /// If a storage with the same @id was already created, returns the cached instance.
    /// Otherwise creates a new instance and caches it.
    #[cfg(feature = "aws")]
    pub async fn get_or_create_s3(
        &mut self,
        sdk_config: &aws_config::SdkConfig,
        config: &StorageConfig,
    ) -> Result<Arc<S3Storage>> {
        use fluree_db_storage_aws::S3Config as RawS3Config;

        // Extract S3 config
        let s3_config = match &config.storage_type {
            StorageType::S3(s3) => s3,
            _ => {
                return Err(ConnectionError::invalid_config(
                    "Expected S3 storage config",
                ))
            }
        };

        // Check cache by @id if present
        if let Some(id) = &config.id {
            if let Some(cached) = self.s3_storages.get(id.as_ref()) {
                return Ok(cached.clone());
            }
        }

        // Create new storage instance
        let timeout_ms = s3_config
            .read_timeout_ms
            .into_iter()
            .chain(s3_config.write_timeout_ms)
            .chain(s3_config.list_timeout_ms)
            .max();

        let raw_config = RawS3Config {
            bucket: s3_config.bucket.to_string(),
            prefix: s3_config
                .prefix
                .as_ref()
                .map(std::string::ToString::to_string),
            endpoint: s3_config
                .endpoint
                .as_ref()
                .map(std::string::ToString::to_string),
            timeout_ms,
            max_retries: s3_config.max_retries.map(|n| n as u32),
            retry_base_delay_ms: s3_config.retry_base_delay_ms,
            retry_max_delay_ms: s3_config.retry_max_delay_ms,
        };

        let storage = S3Storage::new(sdk_config, raw_config)
            .await
            .map_err(|e| ConnectionError::storage(format!("Failed to create S3 storage: {e}")))?;

        let arc_storage = Arc::new(storage);

        // Cache if @id is present
        if let Some(id) = &config.id {
            self.s3_storages.insert(id.to_string(), arc_storage.clone());
        }

        Ok(arc_storage)
    }

    /// Check if a storage with the given @id is already cached
    #[cfg(feature = "aws")]
    pub fn has_s3(&self, id: &str) -> bool {
        self.s3_storages.contains_key(id)
    }

    /// Get a cached S3 storage by @id
    #[cfg(feature = "aws")]
    pub fn get_s3(&self, id: &str) -> Option<Arc<S3Storage>> {
        self.s3_storages.get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_new() {
        let registry = StorageRegistry::new();
        #[cfg(feature = "aws")]
        assert!(!registry.has_s3("test"));
        #[cfg(not(feature = "aws"))]
        let _ = registry;
    }
}
