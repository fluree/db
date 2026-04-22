//! JWKS (JSON Web Key Set) fetcher and cache.
//!
//! Fetches JWKS from configured issuer URLs, caches keys by `kid`,
//! and refreshes on kid-miss or TTL expiry.
//!
//! # Trust Model
//!
//! Configuration is the source of trust: only issuers explicitly configured
//! via `--jwks-issuer` are accepted. The cache is purely a performance layer.
//!
//! # Cache Behavior
//!
//! - Keys are fetched at startup via `warm()` (warn-only if unreachable).
//! - On `kid` cache miss: refresh JWKS from the issuer's endpoint.
//! - On TTL expiry: refresh on next request.
//! - Rate-limited: minimum 10 seconds between refresh attempts per issuer.
//!   The rate limit applies even when fetches fail, preventing stampedes
//!   against unreachable JWKS endpoints.

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::DecodingKey;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Default TTL for cached JWKS entries (5 minutes).
const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(300);

/// Minimum time between refresh attempts for the same issuer (prevents stampede).
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Timeout for JWKS HTTP fetches (connect + response).
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for a JWKS issuer.
#[derive(Debug, Clone)]
pub struct JwksIssuerConfig {
    /// Issuer identifier (must match `iss` claim in tokens).
    pub issuer: String,
    /// JWKS endpoint URL.
    pub jwks_url: String,
}

/// Cached JWKS entry for a single issuer.
struct CachedJwks {
    /// Pre-computed DecodingKeys indexed by `kid`.
    keys_by_kid: HashMap<String, DecodingKey>,
    /// When this entry was last successfully fetched.
    fetched_at: Instant,
    /// When the last refresh attempt was started (even if it failed).
    /// Updated BEFORE the fetch to prevent concurrent stampedes.
    last_refresh_attempt: Instant,
}

/// Thread-safe JWKS cache shared via `AppState`.
///
/// Constructed synchronously (no fetching). Call `warm()` from an async
/// context before the server starts listening.
pub struct JwksCache {
    /// HTTP client for fetching JWKS (with timeout).
    client: reqwest::Client,
    /// Configured issuers (issuer URL → config).
    issuers: HashMap<String, JwksIssuerConfig>,
    /// Cached JWKS entries (issuer URL → cached keys).
    cache: RwLock<HashMap<String, CachedJwks>>,
    /// Cache TTL.
    ttl: Duration,
}

impl JwksCache {
    /// Create a new JWKS cache from issuer configurations.
    ///
    /// Does NOT fetch JWKS — call `warm()` from an async context.
    pub fn new(configs: Vec<JwksIssuerConfig>, ttl: Option<Duration>) -> Self {
        let mut issuers = HashMap::new();
        for config in configs {
            issuers.insert(config.issuer.clone(), config);
        }
        let client = reqwest::Client::builder()
            .timeout(JWKS_FETCH_TIMEOUT)
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build JWKS HTTP client");
        Self {
            client,
            issuers,
            cache: RwLock::new(HashMap::new()),
            ttl: ttl.unwrap_or(DEFAULT_JWKS_TTL),
        }
    }

    /// Check if a given issuer is configured (sync, for early rejection).
    pub fn is_configured_issuer(&self, issuer: &str) -> bool {
        self.issuers.contains_key(issuer)
    }

    /// Number of configured issuers (for startup logging).
    pub fn configured_issuer_count(&self) -> usize {
        self.issuers.len()
    }

    /// Warm the cache by fetching JWKS for all configured issuers.
    ///
    /// Logs warnings for unreachable endpoints but does NOT fail.
    /// If `data_auth_mode=required` and all JWKS endpoints are unreachable,
    /// the caller should log a loud warning.
    ///
    /// Returns the number of issuers successfully warmed.
    pub async fn warm(&self) -> usize {
        let mut success_count = 0;
        for (issuer, config) in &self.issuers {
            match self.fetch_jwks(&config.jwks_url).await {
                Ok(jwk_set) => {
                    let entry = Self::build_cache_entry(&jwk_set);
                    let kid_count = entry.keys_by_kid.len();
                    self.cache.write().insert(issuer.clone(), entry);
                    tracing::info!(
                        issuer = issuer.as_str(),
                        jwks_url = config.jwks_url.as_str(),
                        kid_count = kid_count,
                        "JWKS cached"
                    );
                    success_count += 1;
                }
                Err(e) => {
                    // Insert a placeholder so failed issuers are rate-limited
                    // on subsequent requests (prevents stampede).
                    self.cache.write().insert(
                        issuer.clone(),
                        CachedJwks {
                            keys_by_kid: HashMap::new(),
                            fetched_at: Instant::now() - self.ttl, // immediately stale
                            last_refresh_attempt: Instant::now(),
                        },
                    );
                    tracing::warn!(
                        issuer = issuer.as_str(),
                        jwks_url = config.jwks_url.as_str(),
                        error = %e,
                        "Failed to fetch JWKS at startup (will retry on first token)"
                    );
                }
            }
        }
        success_count
    }

    /// Look up a `DecodingKey` for the given issuer and `kid`.
    ///
    /// Returns the key, or refreshes the JWKS if:
    /// 1. No cached entry for this issuer
    /// 2. Cached entry is stale (TTL expired)
    /// 3. `kid` not found in cached entry (key rotation)
    pub async fn get_key(&self, issuer: &str, kid: &str) -> Result<DecodingKey, JwksCacheError> {
        let config = self
            .issuers
            .get(issuer)
            .ok_or_else(|| JwksCacheError::UnknownIssuer(issuer.to_string()))?;

        // Try cache first (read lock)
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(issuer) {
                if entry.fetched_at.elapsed() < self.ttl {
                    if let Some(key) = entry.keys_by_kid.get(kid) {
                        return Ok(key.clone());
                    }
                    // kid miss — fall through to refresh
                }
                // TTL expired — fall through to refresh
            }
        }

        // Refresh needed
        self.refresh_and_lookup(issuer, kid, config).await
    }

    /// Refresh JWKS for an issuer and look up the `kid`.
    ///
    /// Rate-limits refresh attempts: stamps `last_refresh_attempt` BEFORE the
    /// fetch (under write lock) so concurrent callers see the stamp and bail
    /// rather than all triggering parallel fetches.
    async fn refresh_and_lookup(
        &self,
        issuer: &str,
        kid: &str,
        config: &JwksIssuerConfig,
    ) -> Result<DecodingKey, JwksCacheError> {
        // Rate limit check + stamp "attempt in progress" under write lock.
        {
            let mut cache = self.cache.write();
            if let Some(entry) = cache.get_mut(issuer) {
                if entry.last_refresh_attempt.elapsed() < MIN_REFRESH_INTERVAL {
                    // Too soon to retry — return kid-not-found
                    return Err(JwksCacheError::KeyNotFound {
                        kid: kid.to_string(),
                        issuer: issuer.to_string(),
                    });
                }
                // Stamp the attempt time BEFORE fetching to prevent stampede
                entry.last_refresh_attempt = Instant::now();
            } else {
                // No entry at all (cold cache) — insert placeholder to rate-limit
                cache.insert(
                    issuer.to_string(),
                    CachedJwks {
                        keys_by_kid: HashMap::new(),
                        fetched_at: Instant::now() - self.ttl, // immediately stale
                        last_refresh_attempt: Instant::now(),
                    },
                );
            }
        }

        // Fetch fresh JWKS (outside the lock)
        let jwk_set = match self.fetch_jwks(&config.jwks_url).await {
            Ok(set) => set,
            Err(e) => {
                // Fetch failed — placeholder with stamped last_refresh_attempt
                // already in cache from above, so subsequent calls are rate-limited.
                return Err(e);
            }
        };

        let entry = Self::build_cache_entry(&jwk_set);

        // Look up kid in fresh set
        let result = entry.keys_by_kid.get(kid).cloned();

        // Update cache with successful fetch (write lock)
        self.cache.write().insert(issuer.to_string(), entry);

        result.ok_or_else(|| JwksCacheError::KeyNotFound {
            kid: kid.to_string(),
            issuer: issuer.to_string(),
        })
    }

    /// Fetch JWKS from a URL.
    async fn fetch_jwks(&self, url: &str) -> Result<JwkSet, JwksCacheError> {
        let response =
            self.client
                .get(url)
                .send()
                .await
                .map_err(|e| JwksCacheError::FetchFailed {
                    url: url.to_string(),
                    error: e.to_string(),
                })?;

        if !response.status().is_success() {
            return Err(JwksCacheError::FetchFailed {
                url: url.to_string(),
                error: format!("HTTP {}", response.status()),
            });
        }

        let jwk_set: JwkSet = response
            .json()
            .await
            .map_err(|e| JwksCacheError::ParseFailed {
                url: url.to_string(),
                error: e.to_string(),
            })?;

        Ok(jwk_set)
    }

    /// Build a cache entry from a `JwkSet`.
    ///
    /// Extracts RSA keys (kty="RSA" with n/e). Keys without a `kid` are skipped
    /// since we need `kid` for lookup.
    fn build_cache_entry(jwk_set: &JwkSet) -> CachedJwks {
        let mut keys_by_kid = HashMap::new();
        for jwk in &jwk_set.keys {
            if let Some(kid) = &jwk.common.key_id {
                match DecodingKey::from_jwk(jwk) {
                    Ok(key) => {
                        keys_by_kid.insert(kid.clone(), key);
                    }
                    Err(e) => {
                        tracing::debug!(
                            kid = kid.as_str(),
                            error = %e,
                            "Skipping JWK (cannot create DecodingKey)"
                        );
                    }
                }
            }
        }

        CachedJwks {
            keys_by_kid,
            fetched_at: Instant::now(),
            last_refresh_attempt: Instant::now(),
        }
    }
}

impl std::fmt::Debug for JwksCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.cache.read();
        let total_keys: usize = cache.values().map(|e| e.keys_by_kid.len()).sum();
        f.debug_struct("JwksCache")
            .field("issuers", &self.issuers.len())
            .field("cached_keys", &total_keys)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// JWKS cache errors.
#[derive(Debug, thiserror::Error)]
pub enum JwksCacheError {
    /// Issuer not in configured JWKS issuers.
    #[error("OIDC issuer not configured: {0}")]
    UnknownIssuer(String),

    /// Key ID not found in the issuer's JWKS.
    #[error("Key not found in JWKS: kid={kid} issuer={issuer}")]
    KeyNotFound { kid: String, issuer: String },

    /// Failed to fetch JWKS from the endpoint.
    #[error("Failed to fetch JWKS from {url}: {error}")]
    FetchFailed { url: String, error: String },

    /// Failed to parse JWKS response.
    #[error("Failed to parse JWKS from {url}: {error}")]
    ParseFailed { url: String, error: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_jwks_json() -> &'static str {
        r#"{
            "keys": [
                {
                    "kty": "RSA",
                    "kid": "test-kid-1",
                    "use": "sig",
                    "alg": "RS256",
                    "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                    "e": "AQAB"
                },
                {
                    "kty": "RSA",
                    "kid": "test-kid-2",
                    "use": "sig",
                    "alg": "RS256",
                    "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                    "e": "AQAB"
                }
            ]
        }"#
    }

    #[test]
    fn test_build_cache_entry_from_jwks() {
        let jwk_set: JwkSet = serde_json::from_str(sample_jwks_json()).unwrap();
        let entry = JwksCache::build_cache_entry(&jwk_set);

        assert_eq!(entry.keys_by_kid.len(), 2);
        assert!(entry.keys_by_kid.contains_key("test-kid-1"));
        assert!(entry.keys_by_kid.contains_key("test-kid-2"));
    }

    #[test]
    fn test_build_cache_entry_skips_keys_without_kid() {
        let jwks_json = r#"{
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                    "e": "AQAB"
                }
            ]
        }"#;
        let jwk_set: JwkSet = serde_json::from_str(jwks_json).unwrap();
        let entry = JwksCache::build_cache_entry(&jwk_set);

        assert_eq!(
            entry.keys_by_kid.len(),
            0,
            "keys without kid should be skipped"
        );
    }

    #[test]
    fn test_is_configured_issuer() {
        let cache = JwksCache::new(
            vec![JwksIssuerConfig {
                issuer: "https://solo.example.com".to_string(),
                jwks_url: "https://solo.example.com/.well-known/jwks.json".to_string(),
            }],
            None,
        );

        assert!(cache.is_configured_issuer("https://solo.example.com"));
        assert!(!cache.is_configured_issuer("https://evil.example.com"));
    }

    #[test]
    fn test_new_creates_empty_cache() {
        let cache = JwksCache::new(
            vec![JwksIssuerConfig {
                issuer: "https://test.example.com".to_string(),
                jwks_url: "https://test.example.com/.well-known/jwks.json".to_string(),
            }],
            Some(Duration::from_secs(60)),
        );

        assert_eq!(cache.issuers.len(), 1);
        assert_eq!(
            cache.cache.read().len(),
            0,
            "cache should be empty before warm()"
        );
        assert_eq!(cache.ttl, Duration::from_secs(60));
    }
}
