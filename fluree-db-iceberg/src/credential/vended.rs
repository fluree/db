//! Vended credential structures and caching.

use crate::error::Result;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Temporary storage credentials vended by a REST catalog.
#[derive(Debug, Clone)]
pub struct VendedCredentials {
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Session token (required for temporary credentials)
    pub session_token: Option<String>,
    /// Credential expiration time (wall-clock)
    pub expires_at: Option<DateTime<Utc>>,
    /// S3 endpoint override (for MinIO, etc.)
    pub endpoint: Option<String>,
    /// AWS region
    pub region: Option<String>,
    /// Use path-style S3 access
    pub path_style: bool,
}

impl VendedCredentials {
    /// Parse credentials from REST catalog response config map.
    ///
    /// Expected keys (from reference / Polaris):
    /// - `s3.access-key-id`
    /// - `s3.secret-access-key`
    /// - `s3.session-token`
    /// - `s3.endpoint`
    /// - `s3.region`
    /// - `s3.path-style-access`
    /// - `expiration-time` or `s3.session-token-expires-at-ms`
    pub fn from_config_map(config: &HashMap<String, serde_json::Value>) -> Result<Option<Self>> {
        let access_key = config
            .get("s3.access-key-id")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let secret_key = config
            .get("s3.secret-access-key")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        // Both required for valid credentials
        let (access_key_id, secret_access_key) = match (access_key, secret_key) {
            (Some(ak), Some(sk)) => (ak, sk),
            _ => return Ok(None), // No vended credentials in response
        };

        let session_token = config
            .get("s3.session-token")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let endpoint = config
            .get("s3.endpoint")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let region = config
            .get("s3.region")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let path_style = config
            .get("s3.path-style-access")
            .and_then(|v| v.as_str())
            .map(|s| s == "true")
            .unwrap_or(false);

        // Parse expiration - try both formats (ms timestamp)
        let expires_at = config
            .get("expiration-time")
            .or_else(|| config.get("s3.session-token-expires-at-ms"))
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    s.parse::<i64>().ok()
                } else {
                    v.as_i64()
                }
            })
            .and_then(DateTime::from_timestamp_millis);

        Ok(Some(Self {
            access_key_id,
            secret_access_key,
            session_token,
            expires_at,
            endpoint,
            region,
            path_style,
        }))
    }

    /// Check if credentials are expired or will expire within buffer.
    ///
    /// Uses a 30-second buffer to ensure we refresh before actual expiration.
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            Utc::now() + Duration::seconds(30) >= exp
        } else {
            false // No expiration means never expires (for testing)
        }
    }

    /// Get seconds until expiration, or None if no expiration set.
    pub fn seconds_until_expiry(&self) -> Option<i64> {
        self.expires_at.map(|exp| (exp - Utc::now()).num_seconds())
    }
}

/// Operation scope for credential caching.
///
/// Different operations may require different credentials or permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OperationScope {
    /// Read operations (default)
    #[default]
    Read,
    /// Write operations
    Write,
}

/// Key for the credential cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialCacheKey {
    /// Catalog URI
    pub catalog_uri: String,
    /// Table identifier (canonical form)
    pub table_identifier: String,
    /// Operation scope
    pub operation_scope: OperationScope,
}

impl CredentialCacheKey {
    /// Create a new cache key for read operations.
    pub fn for_read(catalog_uri: impl Into<String>, table_identifier: impl Into<String>) -> Self {
        Self {
            catalog_uri: catalog_uri.into(),
            table_identifier: table_identifier.into(),
            operation_scope: OperationScope::Read,
        }
    }

    /// Create a new cache key for write operations.
    pub fn for_write(catalog_uri: impl Into<String>, table_identifier: impl Into<String>) -> Self {
        Self {
            catalog_uri: catalog_uri.into(),
            table_identifier: table_identifier.into(),
            operation_scope: OperationScope::Write,
        }
    }
}

/// Thread-safe cache for vended credentials.
///
/// Credentials are keyed by `(catalog, table, scope)` and automatically
/// invalidated based on their wall-clock expiration time.
pub struct VendedCredentialCache {
    cache: Arc<RwLock<HashMap<CredentialCacheKey, VendedCredentials>>>,
}

impl std::fmt::Debug for VendedCredentialCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendedCredentialCache").finish()
    }
}

impl VendedCredentialCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get credentials if cached and not expired.
    pub async fn get(&self, key: &CredentialCacheKey) -> Option<VendedCredentials> {
        let cache = self.cache.read().await;
        cache.get(key).filter(|c| !c.is_expired()).cloned()
    }

    /// Store credentials in cache.
    pub async fn put(&self, key: CredentialCacheKey, creds: VendedCredentials) {
        let mut cache = self.cache.write().await;
        cache.insert(key, creds);
    }

    /// Remove credentials from cache.
    pub async fn invalidate(&self, key: &CredentialCacheKey) {
        let mut cache = self.cache.write().await;
        cache.remove(key);
    }

    /// Remove all expired credentials from cache.
    pub async fn evict_expired(&self) {
        let mut cache = self.cache.write().await;
        cache.retain(|_, creds| !creds.is_expired());
    }

    /// Get the number of cached credentials.
    pub async fn len(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Check if cache is empty.
    pub async fn is_empty(&self) -> bool {
        let cache = self.cache.read().await;
        cache.is_empty()
    }
}

impl Default for VendedCredentialCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for VendedCredentialCache {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credentials_from_config() {
        let mut config = HashMap::new();
        config.insert(
            "s3.access-key-id".to_string(),
            serde_json::json!("AKIATEST"),
        );
        config.insert(
            "s3.secret-access-key".to_string(),
            serde_json::json!("secret123"),
        );
        config.insert(
            "s3.session-token".to_string(),
            serde_json::json!("session456"),
        );
        config.insert("s3.region".to_string(), serde_json::json!("us-east-1"));
        config.insert(
            "s3.endpoint".to_string(),
            serde_json::json!("http://minio:9000"),
        );
        config.insert(
            "s3.path-style-access".to_string(),
            serde_json::json!("true"),
        );

        let creds = VendedCredentials::from_config_map(&config)
            .unwrap()
            .unwrap();
        assert_eq!(creds.access_key_id, "AKIATEST");
        assert_eq!(creds.secret_access_key, "secret123");
        assert_eq!(creds.session_token, Some("session456".to_string()));
        assert_eq!(creds.region, Some("us-east-1".to_string()));
        assert_eq!(creds.endpoint, Some("http://minio:9000".to_string()));
        assert!(creds.path_style);
    }

    #[test]
    fn test_parse_credentials_missing() {
        let config = HashMap::new();
        let creds = VendedCredentials::from_config_map(&config).unwrap();
        assert!(creds.is_none());
    }

    #[test]
    fn test_parse_credentials_partial() {
        let mut config = HashMap::new();
        // Only access key, no secret
        config.insert(
            "s3.access-key-id".to_string(),
            serde_json::json!("AKIATEST"),
        );
        let creds = VendedCredentials::from_config_map(&config).unwrap();
        assert!(creds.is_none());
    }

    #[test]
    fn test_expiration_check() {
        // Not expired (1 hour in future)
        let future_creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        };
        assert!(!future_creds.is_expired());

        // Expired (within 30s buffer)
        let soon_creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::seconds(10)),
            endpoint: None,
            region: None,
            path_style: false,
        };
        assert!(soon_creds.is_expired());

        // Already expired
        let past_creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() - Duration::minutes(5)),
            endpoint: None,
            region: None,
            path_style: false,
        };
        assert!(past_creds.is_expired());
    }

    #[test]
    fn test_no_expiration_never_expires() {
        let creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: None,
            endpoint: None,
            region: None,
            path_style: false,
        };
        assert!(!creds.is_expired());
    }

    #[tokio::test]
    async fn test_cache_put_get() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        };

        cache.put(key.clone(), creds.clone()).await;

        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().access_key_id, "test");
    }

    #[tokio::test]
    async fn test_cache_expired_not_returned() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() - Duration::minutes(5)), // Already expired
            endpoint: None,
            region: None,
            path_style: false,
        };

        cache.put(key.clone(), creds).await;

        // Should not return expired credentials
        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        };

        cache.put(key.clone(), creds).await;
        assert!(cache.get(&key).await.is_some());

        cache.invalidate(&key).await;
        assert!(cache.get(&key).await.is_none());
    }
}
