//! Storage and cache factory functions

use crate::config::{StorageConfig, StorageType};
use crate::error::{ConnectionError, Result};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
use fluree_db_core::FileStorage;
use fluree_db_core::MemoryStorage;

/// Create a memory storage instance
pub fn create_memory_storage() -> MemoryStorage {
    MemoryStorage::new()
}

/// Create a file storage instance
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub fn create_file_storage(base_path: &str) -> FileStorage {
    FileStorage::new(base_path)
}

/// Validate storage config and return the path for file storage
pub fn validate_storage_config(config: &StorageConfig) -> Result<Option<&str>> {
    match &config.storage_type {
        StorageType::Memory => Ok(None),
        StorageType::File => {
            #[cfg(not(all(feature = "native", not(target_arch = "wasm32"))))]
            {
                return Err(ConnectionError::unsupported_component(
                    "https://ns.flur.ee/system#filePath (native feature disabled)",
                ));
            }
            let path = config.path.as_ref().ok_or_else(|| {
                ConnectionError::invalid_config("File storage requires 'path' to be specified")
            })?;
            Ok(Some(path.as_ref()))
        }
        StorageType::S3(_) => Err(ConnectionError::unsupported_component(
            "https://ns.flur.ee/system#s3Bucket",
        )),
        StorageType::Unsupported { type_iri, .. } => {
            Err(ConnectionError::unsupported_component(type_iri))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_create_memory_storage() {
        let storage = create_memory_storage();
        assert!(format!("{storage:?}").contains("MemoryStorage"));
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_create_file_storage() {
        let storage = create_file_storage("/tmp/test");
        assert!(format!("{storage:?}").contains("FileStorage"));
    }

    #[test]
    fn test_validate_memory_config() {
        let config = StorageConfig::default();
        let result = validate_storage_config(&config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_validate_file_config() {
        let config = StorageConfig {
            id: None,
            storage_type: StorageType::File,
            path: Some(Arc::from("/tmp/test")),
            aes256_key: None,
            address_identifier: None,
        };
        let result = validate_storage_config(&config).unwrap();
        assert_eq!(result, Some("/tmp/test"));
    }

    #[test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    fn test_file_storage_requires_path() {
        let config = StorageConfig {
            id: None,
            storage_type: StorageType::File,
            path: None,
            aes256_key: None,
            address_identifier: None,
        };
        let result = validate_storage_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires 'path'"));
    }

    #[test]
    fn test_unsupported_storage_errors() {
        let config = StorageConfig {
            id: None,
            storage_type: StorageType::Unsupported {
                type_iri: "S3Storage".to_string(),
                raw: serde_json::json!({}),
            },
            path: None,
            aes256_key: None,
            address_identifier: None,
        };
        let result = validate_storage_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("S3Storage"));
    }
}
