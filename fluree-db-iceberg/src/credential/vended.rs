//! Vended credential structures and caching.

use crate::error::{IcebergError, Result};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Temporary storage credentials vended by a REST catalog for one table: S3
/// keys, or a Google Cloud Storage OAuth token (BigLake's runtime catalogs).
#[derive(Clone, Debug)]
pub enum VendedCredentials {
    /// AWS (or S3-compatible) keys.
    S3(S3Credentials),
    /// A GCS bearer token.
    Gcs(GcsCredentials),
}

/// Vended S3 keys and the connection settings the catalog sent beside them.
#[derive(Clone)]
pub struct S3Credentials {
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

/// A vended GCS OAuth token. Always carries its expiry: a token without one is
/// refused at parse time, since nothing could tell when to renew it.
#[derive(Clone)]
pub struct GcsCredentials {
    /// The bearer token (`gcs.oauth2.token`).
    pub token: String,
    /// When it expires (`gcs.oauth2.token-expires-at`, epoch milliseconds).
    pub expires_at: DateTime<Utc>,
    /// The project requests bill to (`gcs.user-project`, requester pays).
    pub user_project: Option<String>,
}

/// Redacting `Debug`: never leak the secret access key or session token via a
/// `{:?}` in a log or error. The access key ID is an identifier (not usable
/// without the secret) and is shown to aid debugging.
impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"***")
            .field("session_token", &self.session_token.as_ref().map(|_| "***"))
            .field("expires_at", &self.expires_at)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("path_style", &self.path_style)
            .finish()
    }
}

/// Redacting `Debug`: the token is a live bearer credential.
impl std::fmt::Debug for GcsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsCredentials")
            .field("token", &"***")
            .field("expires_at", &self.expires_at)
            .field("user_project", &self.user_project)
            .finish()
    }
}

/// The `loadTable` config keys that ARE the vended credential (as opposed to how
/// to reach storage). A matched `storage-credentials` entry is their only source;
/// the table's top-level `config` supplies the rest.
const CREDENTIAL_KEYS: [&str; 8] = [
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
    "s3.session-token-expires-at-ms",
    "expiration-time",
    GCS_TOKEN,
    GCS_TOKEN_EXPIRES_AT,
    GCS_TOKEN_EXPIRES_AT_UNDERSCORE,
];

/// The vended GCS token.
const GCS_TOKEN: &str = "gcs.oauth2.token";
/// Its expiry, as Iceberg Java spells it.
const GCS_TOKEN_EXPIRES_AT: &str = "gcs.oauth2.token-expires-at";
/// Its expiry, as Google's BigLake documentation spells it.
const GCS_TOKEN_EXPIRES_AT_UNDERSCORE: &str = "gcs.oauth2.token_expires_at";
/// The requester-pays project for GCS reads.
const GCS_USER_PROJECT: &str = "gcs.user-project";

impl VendedCredentials {
    /// Parse credentials from REST catalog response config map.
    ///
    /// S3 keys (from reference / Polaris):
    /// - `s3.access-key-id`
    /// - `s3.secret-access-key`
    /// - `s3.session-token`
    /// - `s3.endpoint`
    /// - `client.region` (Iceberg-REST spec key, e.g. sent by Snowflake Horizon),
    ///   with `s3.region` accepted as a defensive fallback
    /// - `s3.path-style-access`
    /// - `expiration-time` or `s3.session-token-expires-at-ms`
    ///
    /// A GCS token (BigLake runtime catalogs):
    /// - `gcs.oauth2.token`
    /// - `gcs.oauth2.token-expires-at` or `gcs.oauth2.token_expires_at` (epoch
    ///   milliseconds; required: a token without an expiry is refused)
    /// - `gcs.user-project`
    ///
    /// When a map carries both, the S3 keys win; [`Self::from_load_table_response`]
    /// picks by the table's location instead.
    pub fn from_config_map(config: &HashMap<String, serde_json::Value>) -> Result<Option<Self>> {
        Self::parse(config, None)
    }

    /// [`Self::from_config_map`], choosing between S3 keys and a GCS token by
    /// the scheme of `location` when the map carries both.
    fn parse(
        config: &HashMap<String, serde_json::Value>,
        location: Option<&str>,
    ) -> Result<Option<Self>> {
        let s3 = Self::s3_from(config);
        let wants_gcs = location.is_some_and(|l| {
            crate::io::location::LocationScheme::of(l) == crate::io::location::LocationScheme::Gs
        });
        if config.contains_key(GCS_TOKEN) && (wants_gcs || s3.is_none()) {
            return Self::gcs_from(config).map(|gcs| Some(VendedCredentials::Gcs(gcs)));
        }
        Ok(s3.map(VendedCredentials::S3))
    }

    fn s3_from(config: &HashMap<String, serde_json::Value>) -> Option<S3Credentials> {
        let access_key_id = config
            .get("s3.access-key-id")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)?;

        // Both required for valid credentials
        let secret_access_key = config
            .get("s3.secret-access-key")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)?;

        let session_token = config
            .get("s3.session-token")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let endpoint = config
            .get("s3.endpoint")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        // The Iceberg-REST spec carries the region as `client.region` (this is what
        // Snowflake Horizon / Polaris send). Prefer it, falling back to `s3.region`.
        let region = config
            .get("client.region")
            .or_else(|| config.get("s3.region"))
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
            .and_then(epoch_ms)
            .and_then(DateTime::from_timestamp_millis);

        Some(S3Credentials {
            access_key_id,
            secret_access_key,
            session_token,
            expires_at,
            endpoint,
            region,
            path_style,
        })
    }

    fn gcs_from(config: &HashMap<String, serde_json::Value>) -> Result<GcsCredentials> {
        let token = config
            .get(GCS_TOKEN)
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                IcebergError::Credential(format!(
                    "the catalog vended an empty or non-string {GCS_TOKEN}"
                ))
            })?
            .to_string();
        let expires_at = config
            .get(GCS_TOKEN_EXPIRES_AT)
            .or_else(|| config.get(GCS_TOKEN_EXPIRES_AT_UNDERSCORE))
            .and_then(epoch_ms)
            .and_then(DateTime::from_timestamp_millis)
            .ok_or_else(|| {
                IcebergError::Credential(format!(
                    "the catalog vended a GCS token ({GCS_TOKEN}) without a usable expiry \
                     ({GCS_TOKEN_EXPIRES_AT}, epoch milliseconds); a token that cannot be \
                     renewed before it lapses is refused"
                ))
            })?;
        let user_project = config
            .get(GCS_USER_PROJECT)
            .and_then(|v| v.as_str())
            .filter(|p| !p.is_empty())
            .map(str::to_string);
        if let Some(project) = &user_project {
            if !crate::auth::google::is_project_id(project) {
                return Err(IcebergError::Credential(format!(
                    "the catalog vended a {GCS_USER_PROJECT} that is not a Google Cloud project id"
                )));
            }
        }
        Ok(GcsCredentials {
            token,
            expires_at,
            user_project,
        })
    }

    /// Parse vended credentials from a full REST `loadTable` response.
    ///
    /// Honors the standardized top-level `storage-credentials` array
    /// (apache/iceberg #10722): per spec a client MUST check `storage-credentials`
    /// **before** the legacy top-level `config` map. Among the entries whose
    /// `prefix` matches `metadata_location` (an empty prefix matches everything),
    /// the **longest** prefix is the catalog's decision for this table, and it is
    /// final:
    ///
    /// - its static `s3.*` keys (or its `gcs.oauth2.*` token) are the
    ///   credentials, read with the table's top-level connection settings
    ///   (region, endpoint, path-style, user project) beneath the entry's own;
    /// - if it has none — e.g. a remote-signing entry, which this reader does not
    ///   implement — the result is `None`, and the source's `vended_credentials`
    ///   policy refuses the read (§2 fail-closed) rather than falling back to a
    ///   shorter entry or the legacy `config`, credentials the catalog did not
    ///   scope to this table.
    ///
    /// Only when no entry matches do the credentials come from the flat top-level
    /// `config` map (the shape AWS Lake Formation and older catalogs still emit). Ties between equal
    /// prefixes go to the later entry. Entries without a `config` object are
    /// malformed and ignored.
    ///
    /// The prefix is matched against `metadata_location` because one credential
    /// set serves the whole table; a table whose data files sit outside its
    /// metadata prefix is matched on the metadata. The same response shape is
    /// what the per-table credentials endpoint (`…/tables/{t}/credentials`)
    /// returns, without a top-level `config`.
    pub fn from_load_table_response(
        response: &serde_json::Value,
        metadata_location: &str,
    ) -> Result<Option<Self>> {
        let best_entry = response
            .get("storage-credentials")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let prefix = entry.get("prefix").and_then(|p| p.as_str()).unwrap_or("");
                let config = entry.get("config").and_then(|c| c.as_object())?;
                (prefix.is_empty() || metadata_location.starts_with(prefix))
                    .then_some((prefix.len(), config))
            })
            .max_by_key(|(prefix_len, _)| *prefix_len);

        let top_level = response.get("config").and_then(|v| v.as_object());
        let config: HashMap<String, serde_json::Value> = match best_entry {
            // The table's own settings (region, endpoint, path-style) with the
            // matched entry laid over them, as Java's S3FileIO layers a storage
            // credential's properties over the table's. A catalog may put only the
            // credential keys in the entry (Polaris / Snowflake Open Catalog do) and
            // the connection settings only at the top level. The credentials
            // themselves come ONLY from the entry: a top-level key never stands in
            // for one the entry lacks, so an entry with no static keys (e.g. remote
            // signing) still yields no credentials (see above).
            Some((_, entry_config)) => top_level
                .into_iter()
                .flatten()
                .filter(|(key, _)| !CREDENTIAL_KEYS.contains(&key.as_str()))
                .chain(entry_config)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            None => match top_level {
                Some(legacy) => legacy.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                None => return Ok(None),
            },
        };
        Self::parse(&config, Some(metadata_location))
    }

    /// When the credentials expire, if the catalog said.
    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        match self {
            VendedCredentials::S3(s3) => s3.expires_at,
            VendedCredentials::Gcs(gcs) => Some(gcs.expires_at),
        }
    }

    /// Check if credentials are expired or will expire within buffer.
    ///
    /// Uses a 30-second buffer to ensure we refresh before actual expiration.
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at() {
            Utc::now() + Duration::seconds(30) >= exp
        } else {
            false // No expiration means never expires (for testing)
        }
    }

    /// Get seconds until expiration, or None if no expiration set.
    pub fn seconds_until_expiry(&self) -> Option<i64> {
        self.expires_at()
            .map(|exp| (exp - Utc::now()).num_seconds())
    }

    /// The kind of credential, for messages: `S3 keys` or `a GCS token`.
    pub fn kind(&self) -> &'static str {
        match self {
            VendedCredentials::S3(_) => "S3 keys",
            VendedCredentials::Gcs(_) => "a GCS token",
        }
    }
}

/// An epoch-milliseconds value, as a JSON number or a numeric string.
fn epoch_ms(value: &serde_json::Value) -> Option<i64> {
    match value.as_str() {
        Some(s) => s.trim().parse::<i64>().ok(),
        None => value.as_i64(),
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

    /// The S3 keys `creds` must be.
    fn s3(creds: VendedCredentials) -> S3Credentials {
        match creds {
            VendedCredentials::S3(s3) => s3,
            other => panic!("expected S3 keys, got {other:?}"),
        }
    }

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

        let creds = s3(VendedCredentials::from_config_map(&config)
            .unwrap()
            .unwrap());
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
    fn test_parse_client_region_preferred() {
        // The Iceberg-REST spec key `client.region` (what Snowflake Horizon sends)
        // must be honored even when no `s3.region` is present.
        let mut config = HashMap::new();
        config.insert(
            "s3.access-key-id".to_string(),
            serde_json::json!("AKIATEST"),
        );
        config.insert(
            "s3.secret-access-key".to_string(),
            serde_json::json!("secret123"),
        );
        config.insert("client.region".to_string(), serde_json::json!("us-east-2"));

        let creds = s3(VendedCredentials::from_config_map(&config)
            .unwrap()
            .unwrap());
        assert_eq!(creds.region, Some("us-east-2".to_string()));
    }

    #[test]
    fn test_parse_client_region_beats_s3_region() {
        // When both are present, `client.region` wins.
        let mut config = HashMap::new();
        config.insert(
            "s3.access-key-id".to_string(),
            serde_json::json!("AKIATEST"),
        );
        config.insert(
            "s3.secret-access-key".to_string(),
            serde_json::json!("secret123"),
        );
        config.insert("client.region".to_string(), serde_json::json!("us-east-2"));
        config.insert("s3.region".to_string(), serde_json::json!("us-east-1"));

        let creds = s3(VendedCredentials::from_config_map(&config)
            .unwrap()
            .unwrap());
        assert_eq!(creds.region, Some("us-east-2".to_string()));
    }

    #[test]
    fn test_parse_s3_region_fallback() {
        // With only the legacy `s3.region` key present, it is still parsed.
        let mut config = HashMap::new();
        config.insert(
            "s3.access-key-id".to_string(),
            serde_json::json!("AKIATEST"),
        );
        config.insert(
            "s3.secret-access-key".to_string(),
            serde_json::json!("secret123"),
        );
        config.insert("s3.region".to_string(), serde_json::json!("us-east-1"));

        let creds = s3(VendedCredentials::from_config_map(&config)
            .unwrap()
            .unwrap());
        assert_eq!(creds.region, Some("us-east-1".to_string()));
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
        let future_creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        });
        assert!(!future_creds.is_expired());

        // Expired (within 30s buffer)
        let soon_creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::seconds(10)),
            endpoint: None,
            region: None,
            path_style: false,
        });
        assert!(soon_creds.is_expired());

        // Already expired
        let past_creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() - Duration::minutes(5)),
            endpoint: None,
            region: None,
            path_style: false,
        });
        assert!(past_creds.is_expired());
    }

    #[test]
    fn test_no_expiration_never_expires() {
        let creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
            expires_at: None,
            endpoint: None,
            region: None,
            path_style: false,
        });
        assert!(!creds.is_expired());
    }

    #[tokio::test]
    async fn test_cache_put_get() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        });

        cache.put(key.clone(), creds.clone()).await;

        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());
        assert_eq!(s3(retrieved.unwrap()).access_key_id, "test");
    }

    #[tokio::test]
    async fn test_cache_expired_not_returned() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() - Duration::minutes(5)), // Already expired
            endpoint: None,
            region: None,
            path_style: false,
        });

        cache.put(key.clone(), creds).await;

        // Should not return expired credentials
        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let cache = VendedCredentialCache::new();
        let key = CredentialCacheKey::for_read("https://polaris.example.com", "ns.table");
        let creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "test".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(Utc::now() + Duration::hours(1)),
            endpoint: None,
            region: None,
            path_style: false,
        });

        cache.put(key.clone(), creds).await;
        assert!(cache.get(&key).await.is_some());

        cache.invalidate(&key).await;
        assert!(cache.get(&key).await.is_none());
    }

    #[test]
    fn debug_redacts_secret_and_session_token() {
        let creds = VendedCredentials::S3(S3Credentials {
            access_key_id: "AKIAEXAMPLE".to_string(),
            secret_access_key: "super-secret-key".to_string(),
            session_token: Some("super-secret-session".to_string()),
            expires_at: None,
            endpoint: None,
            region: Some("us-east-1".to_string()),
            path_style: false,
        });
        let dbg = format!("{creds:?}");
        assert!(!dbg.contains("super-secret-key"), "secret leaked: {dbg}");
        assert!(
            !dbg.contains("super-secret-session"),
            "session token leaked: {dbg}"
        );
        // The access key ID (an identifier, unusable without the secret) is shown.
        assert!(dbg.contains("AKIAEXAMPLE"));
    }

    // ── from_load_table_response: standardized `storage-credentials` array
    //    (apache/iceberg #10722), precedence + longest-usable-prefix + fallback ──

    const ML: &str = "s3://bucket/db/tbl/metadata/v1.json";

    fn creds_config(ak: &str) -> serde_json::Value {
        serde_json::json!({
            "s3.access-key-id": ak,
            "s3.secret-access-key": "secret",
            "s3.session-token": "token",
            "client.region": "us-east-1",
        })
    }

    fn load_creds(resp: &serde_json::Value) -> Option<S3Credentials> {
        VendedCredentials::from_load_table_response(resp, ML)
            .unwrap()
            .map(s3)
    }

    #[test]
    fn storage_credentials_array_is_parsed() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "storage-credentials": [
                { "prefix": "s3://bucket/db/tbl", "config": creds_config("AKIA_SC") }
            ],
        });
        let c = load_creds(&resp).expect("creds");
        assert_eq!(c.access_key_id, "AKIA_SC");
        assert_eq!(c.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn storage_credentials_take_precedence_over_config() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "config": creds_config("AKIA_CONFIG"),
            "storage-credentials": [
                { "prefix": "s3://bucket/db/tbl", "config": creds_config("AKIA_SC") }
            ],
        });
        assert_eq!(load_creds(&resp).unwrap().access_key_id, "AKIA_SC");
    }

    #[test]
    fn longest_matching_prefix_wins() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "storage-credentials": [
                { "prefix": "s3://bucket", "config": creds_config("AKIA_SHORT") },
                { "prefix": "s3://bucket/db/tbl", "config": creds_config("AKIA_LONG") }
            ],
        });
        assert_eq!(load_creds(&resp).unwrap().access_key_id, "AKIA_LONG");
    }

    #[test]
    fn longest_prefix_without_static_keys_yields_no_credentials() {
        // The longest matching prefix is remote-signing-only (no static s3.* keys).
        // It decides this table: no credentials, NOT the shorter entry's keys.
        let resp = serde_json::json!({
            "metadata-location": ML,
            "storage-credentials": [
                { "prefix": "s3://bucket/db", "config": creds_config("AKIA_SHORTER") },
                { "prefix": "s3://bucket/db/tbl", "config": { "s3.remote-signing-enabled": "true" } }
            ],
        });
        assert!(load_creds(&resp).is_none());
    }

    #[test]
    fn matching_remote_signing_entry_does_not_fall_back_to_legacy_config() {
        // fluree/db#1451 review: a prefix-matching remote-signing entry plus static
        // keys in the legacy `config` must not read with the legacy keys — the
        // catalog asked for remote signing on this prefix.
        let resp = serde_json::json!({
            "metadata-location": ML,
            "config": creds_config("AKIA_LEGACY"),
            "storage-credentials": [
                { "prefix": "s3://bucket/db/tbl", "config": { "s3.remote-signing-enabled": "true" } }
            ],
        });
        assert!(load_creds(&resp).is_none());
    }

    #[test]
    fn a_matched_entry_keeps_the_tables_top_level_connection_settings() {
        // fluree/db#1451 review: Polaris / Snowflake Open Catalog put only the
        // credential keys in the entry; region, endpoint and path-style live only
        // in the top-level `config`. They must survive, or reads of a MinIO / other
        // S3-compatible store go to AWS S3, and an out-of-region bucket fails.
        let resp = serde_json::json!({
            "metadata-location": ML,
            "config": {
                "client.region": "eu-west-1",
                "s3.endpoint": "http://minio:9000",
                "s3.path-style-access": "true",
                "s3.access-key-id": "AKIA_TOP_LEVEL",
                "s3.secret-access-key": "top-secret",
            },
            "storage-credentials": [
                { "prefix": "s3://bucket/db/tbl", "config": {
                    "s3.access-key-id": "AKIA_ENTRY",
                    "s3.secret-access-key": "entry-secret",
                    "s3.session-token": "entry-token",
                } }
            ],
        });
        let c = load_creds(&resp).expect("creds");
        assert_eq!(
            c.access_key_id, "AKIA_ENTRY",
            "credentials come from the entry"
        );
        assert_eq!(c.session_token.as_deref(), Some("entry-token"));
        assert_eq!(c.region.as_deref(), Some("eu-west-1"));
        assert_eq!(c.endpoint.as_deref(), Some("http://minio:9000"));
        assert!(c.path_style);

        // The entry's own settings win over the top level's.
        let mut overridden = resp.clone();
        overridden["storage-credentials"][0]["config"]["client.region"] = "us-west-2".into();
        assert_eq!(
            load_creds(&overridden).unwrap().region.as_deref(),
            Some("us-west-2")
        );

        // And the top level never supplies a credential the entry lacks: an entry
        // without static keys still yields none, even with top-level credentials.
        let unsigned = serde_json::json!({
            "metadata-location": ML,
            "config": { "client.region": "eu-west-1", "s3.access-key-id": "AKIA_TOP_LEVEL",
                        "s3.secret-access-key": "top-secret" },
            "storage-credentials": [
                { "prefix": "s3://bucket/db/tbl", "config": { "s3.remote-signing-enabled": "true" } }
            ],
        });
        assert!(load_creds(&unsigned).is_none());
    }

    #[test]
    fn non_matching_prefix_falls_back_to_config() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "storage-credentials": [
                { "prefix": "s3://other-bucket/x", "config": creds_config("AKIA_OTHER") }
            ],
            "config": creds_config("AKIA_CONFIG"),
        });
        assert_eq!(load_creds(&resp).unwrap().access_key_id, "AKIA_CONFIG");
    }

    #[test]
    fn falls_back_to_legacy_config_map() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "config": creds_config("AKIA_CONFIG"),
        });
        assert_eq!(load_creds(&resp).unwrap().access_key_id, "AKIA_CONFIG");
    }

    #[test]
    fn malformed_entries_are_skipped_gracefully() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "storage-credentials": [
                "not-an-object",
                { "prefix": "s3://bucket/db/tbl" },            // no config
                { "config": creds_config("AKIA_NOPREFIX") }    // no prefix -> applies to all
            ],
        });
        assert_eq!(load_creds(&resp).unwrap().access_key_id, "AKIA_NOPREFIX");
    }

    #[test]
    fn no_credentials_yields_none() {
        let resp = serde_json::json!({
            "metadata-location": ML,
            "config": { "table_type": "ICEBERG" },
        });
        assert!(load_creds(&resp).is_none());
    }

    // ── GCS tokens (BigLake runtime catalogs) ──

    const GS_ML: &str = "gs://lake/db/tbl/metadata/v1.json";

    fn gcs(creds: VendedCredentials) -> GcsCredentials {
        match creds {
            VendedCredentials::Gcs(gcs) => gcs,
            other => panic!("expected a GCS token, got {other:?}"),
        }
    }

    #[test]
    fn a_gcs_token_parses_with_either_expiry_spelling() {
        for (key, value) in [
            (
                "gcs.oauth2.token-expires-at",
                serde_json::json!("1893456000000"),
            ),
            (
                "gcs.oauth2.token_expires_at",
                serde_json::json!(1_893_456_000_000_i64),
            ),
        ] {
            let mut config = HashMap::new();
            config.insert(
                "gcs.oauth2.token".to_string(),
                serde_json::json!("ya29.tok"),
            );
            config.insert(key.to_string(), value);
            config.insert(
                "gcs.user-project".to_string(),
                serde_json::json!("lake-billing"),
            );
            let creds = gcs(VendedCredentials::from_config_map(&config)
                .unwrap()
                .unwrap());
            assert_eq!(creds.token, "ya29.tok", "{key}");
            assert_eq!(
                creds.expires_at.timestamp_millis(),
                1_893_456_000_000,
                "{key}"
            );
            assert_eq!(creds.user_project.as_deref(), Some("lake-billing"));
        }
    }

    #[test]
    fn a_gcs_token_without_a_usable_expiry_is_refused() {
        for expiry in [None, Some(serde_json::json!("next tuesday"))] {
            let mut config = HashMap::new();
            config.insert(
                "gcs.oauth2.token".to_string(),
                serde_json::json!("ya29.tok"),
            );
            if let Some(expiry) = expiry {
                config.insert("gcs.oauth2.token-expires-at".to_string(), expiry);
            }
            let err = VendedCredentials::from_config_map(&config).unwrap_err();
            assert!(matches!(err, IcebergError::Credential(_)), "{err:?}");
            let message = err.to_string();
            assert!(message.contains("gcs.oauth2.token-expires-at"), "{message}");
            assert!(!message.contains("ya29.tok"), "the token leaked: {message}");
        }
    }

    #[test]
    fn a_vended_user_project_must_be_a_project_id() {
        let mut config = HashMap::new();
        config.insert(
            "gcs.oauth2.token".to_string(),
            serde_json::json!("ya29.tok"),
        );
        config.insert(
            "gcs.oauth2.token-expires-at".to_string(),
            serde_json::json!(1_893_456_000_000_i64),
        );
        config.insert(
            "gcs.user-project".to_string(),
            serde_json::json!("not a project\r\n"),
        );
        assert!(VendedCredentials::from_config_map(&config).is_err());
    }

    #[test]
    fn a_gcs_token_is_matched_like_s3_keys_and_never_borrowed_from_the_top_level() {
        let entry = serde_json::json!({
            "gcs.oauth2.token": "ya29.entry",
            "gcs.oauth2.token-expires-at": "1893456000000",
        });
        let resp = serde_json::json!({
            "metadata-location": GS_ML,
            "config": { "gcs.user-project": "lake-billing", "gcs.oauth2.token": "ya29.top" },
            "storage-credentials": [ { "prefix": "gs://lake/db/tbl", "config": entry } ],
        });
        let creds = gcs(VendedCredentials::from_load_table_response(&resp, GS_ML)
            .unwrap()
            .unwrap());
        assert_eq!(creds.token, "ya29.entry");
        // The top level's connection settings still apply beneath the entry.
        assert_eq!(creds.user_project.as_deref(), Some("lake-billing"));

        // An entry without a token yields nothing, even with one at the top level.
        let unsigned = serde_json::json!({
            "metadata-location": GS_ML,
            "config": { "gcs.oauth2.token": "ya29.top", "gcs.oauth2.token-expires-at": "1893456000000" },
            "storage-credentials": [ { "prefix": "gs://lake/db/tbl", "config": {} } ],
        });
        assert!(
            VendedCredentials::from_load_table_response(&unsigned, GS_ML)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_map_carrying_both_kinds_is_read_by_the_tables_scheme() {
        let both = serde_json::json!({
            "metadata-location": GS_ML,
            "config": {
                "s3.access-key-id": "AKIA", "s3.secret-access-key": "secret",
                "gcs.oauth2.token": "ya29.tok", "gcs.oauth2.token-expires-at": "1893456000000",
            },
        });
        assert!(matches!(
            VendedCredentials::from_load_table_response(&both, GS_ML).unwrap(),
            Some(VendedCredentials::Gcs(_))
        ));
        assert!(matches!(
            VendedCredentials::from_load_table_response(&both, ML).unwrap(),
            Some(VendedCredentials::S3(_))
        ));
    }

    #[test]
    fn gcs_debug_redacts_the_token() {
        let creds = VendedCredentials::Gcs(GcsCredentials {
            token: "ya29.super-secret".to_string(),
            expires_at: Utc::now(),
            user_project: None,
        });
        let dbg = format!("{creds:?}");
        assert!(!dbg.contains("super-secret"), "token leaked: {dbg}");
    }
}
