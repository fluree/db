//! Storage proxy Bearer token extraction
//!
//! Provides an Axum extractor specifically for `/fluree/storage/*` endpoints.
//! Unlike `MaybeBearer` (for events), this extractor:
//! - Always parses Bearer tokens (regardless of `events_auth_mode`)
//! - Validates using `StorageProxyConfig` (not `EventsAuthConfig`)
//! - Requires `fluree.storage.*` claims (not `fluree.events.*`)
//!
//! When the `oidc` feature is enabled, tokens are dispatched through
//! [`verify_bearer_token`](crate::token_verify::verify_bearer_token) which
//! supports both embedded-JWK (Ed25519) and OIDC/JWKS (RS256) paths.

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::collections::HashSet;
use std::sync::Arc;

use super::bearer::extract_bearer_token;
use crate::config::{EventsAuthConfig, StorageProxyConfig};
use crate::error::ServerError;
use crate::state::AppState;
#[cfg(not(feature = "oidc"))]
use fluree_db_credential::{verify_jws, EventsTokenPayload};

/// Verified principal from storage proxy Bearer token
#[derive(Debug, Clone)]
pub struct StorageProxyPrincipal {
    /// Issuer (did:key for embedded JWK, URL for OIDC)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
    /// fluree.storage.all claim
    pub storage_all: bool,
    /// fluree.storage.ledgers claim (HashSet for O(1) lookup)
    pub storage_ledgers: HashSet<String>,
}

impl StorageProxyPrincipal {
    /// Check if principal has any storage proxy permissions
    pub fn has_storage_permissions(&self) -> bool {
        self.storage_all || !self.storage_ledgers.is_empty()
    }

    /// Check if principal is authorized for a specific ledger alias
    pub fn is_authorized_for_ledger(&self, alias: &str) -> bool {
        self.storage_all || self.storage_ledgers.contains(alias)
    }

    /// Convert to a transport-agnostic [`BlockAccessScope`] for use with
    /// the `block_fetch` API.
    pub fn to_block_access_scope(&self) -> fluree_db_api::BlockAccessScope {
        fluree_db_api::BlockAccessScope {
            all_ledgers: self.storage_all,
            authorized_ledgers: self.storage_ledgers.clone(),
        }
    }
}

/// Storage proxy Bearer token extractor.
///
/// Unlike `MaybeBearer`, this extractor:
/// - Always attempts to parse Bearer tokens (ignores `events_auth_mode`)
/// - Validates against `StorageProxyConfig` (with fallback to events trusted issuers)
/// - Requires `fluree.storage.*` claims
/// - Returns 404 if storage proxy is disabled (no existence leak)
/// - Returns 401 if token is missing/invalid
#[derive(Debug)]
pub struct StorageProxyBearer(pub StorageProxyPrincipal);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for StorageProxyBearer {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let storage_config = state.config.storage_proxy();
        let events_auth = state.config.events_auth();

        // Check if storage proxy is enabled
        if !storage_config.enabled {
            return Err(ServerError::not_found("Storage proxy not enabled"));
        }

        // Extract Authorization header (case-insensitive, trim whitespace)
        let token = extract_bearer_token(&parts.headers)
            .ok_or_else(|| ServerError::unauthorized("Bearer token required"))?;

        // Verify token using cfg-gated dispatch
        #[cfg(feature = "oidc")]
        {
            let jwks_cache = state.jwks_cache.as_deref();
            verify_token(&token, &storage_config, &events_auth, jwks_cache).await
        }
        #[cfg(not(feature = "oidc"))]
        {
            verify_token(&token, &storage_config, &events_auth)
        }
    }
}

/// Build a `StorageProxyPrincipal` from verified claims.
fn build_principal(
    payload: &fluree_db_credential::jwt_claims::EventsTokenPayload,
    issuer: String,
) -> StorageProxyPrincipal {
    StorageProxyPrincipal {
        issuer,
        subject: payload.sub.clone(),
        identity: payload.resolve_identity(),
        storage_all: payload.storage_all.unwrap_or(false),
        storage_ledgers: payload
            .storage_ledgers
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}

/// Verify token and build principal (embedded JWK only — non-oidc builds)
#[cfg(not(feature = "oidc"))]
fn verify_token(
    token: &str,
    storage_config: &StorageProxyConfig,
    events_auth: &EventsAuthConfig,
) -> Result<StorageProxyBearer, ServerError> {
    // 1. Verify JWS (embedded JWK mode)
    let verified =
        verify_jws(token).map_err(|e| ServerError::unauthorized(format!("Invalid token: {e}")))?;

    // 2. Parse combined payload
    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {e}")))?;

    // 3. Validate standard claims (don't require identity - that's optional for storage proxy)
    payload
        .validate(
            None, // No audience requirement for storage proxy
            &verified.did,
            false, // Identity not required
        )
        .map_err(|e| ServerError::unauthorized(e.to_string()))?;

    // 4. Check issuer trust (use verified.did — validate() confirmed it matches iss)
    if !storage_config.is_issuer_trusted(&verified.did, events_auth) {
        return Err(ServerError::unauthorized("Untrusted issuer"));
    }

    // 5. Check token grants STORAGE permissions
    if !payload.has_storage_permissions() {
        return Err(ServerError::unauthorized(
            "Token lacks storage proxy permissions",
        ));
    }

    // 6. Build principal
    let principal = build_principal(&payload, verified.did);
    Ok(StorageProxyBearer(principal))
}

/// Verify token with dual-path dispatch (oidc builds)
///
/// Supports both embedded-JWK (Ed25519) and OIDC/JWKS (RS256) tokens,
/// mirroring the same dual-path dispatch used by data, admin, and events endpoints.
#[cfg(feature = "oidc")]
async fn verify_token(
    token: &str,
    storage_config: &StorageProxyConfig,
    events_auth: &EventsAuthConfig,
    jwks_cache: Option<&crate::jwks::JwksCache>,
) -> Result<StorageProxyBearer, ServerError> {
    // 1. Dual-path dispatch (reuse shared verify_bearer_token)
    let verified = crate::token_verify::verify_bearer_token(token, jwks_cache).await?;

    // 2. Path-specific claims validation
    if verified.is_oidc {
        // OIDC: validate iss == expected_issuer, exp/nbf
        verified
            .payload
            .validate_oidc(
                None, // No audience requirement for storage proxy
                &verified.issuer,
                false, // Identity not required
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;
        // OIDC trust already verified by JWKS path (only configured issuers' keys work)
    } else {
        // Embedded JWK: validate iss == did:key, exp/nbf
        verified
            .payload
            .validate(
                None, // No audience requirement for storage proxy
                &verified.issuer,
                false, // Identity not required
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;
        // Check did:key trust — use verified.issuer for consistency
        if !storage_config.is_issuer_trusted(&verified.issuer, events_auth) {
            return Err(ServerError::unauthorized("Untrusted issuer"));
        }
    }

    // 3. Check token grants STORAGE permissions
    if !verified.payload.has_storage_permissions() {
        return Err(ServerError::unauthorized(
            "Token lacks storage proxy permissions",
        ));
    }

    // 4. Build principal — use verified.issuer as the authoritative identity
    let principal = build_principal(&verified.payload, verified.issuer);
    Ok(StorageProxyBearer(principal))
}

// ============================================================================
// Tests
// ============================================================================

/// Principal-level tests (always compiled, no token verification involved)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_proxy_principal_fields() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: Some("ex:PeerServiceAccount".to_string()),
            storage_all: false,
            storage_ledgers: vec!["books:main".to_string(), "users:main".to_string()]
                .into_iter()
                .collect(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_authorized_for_ledger("books:main"));
        assert!(principal.is_authorized_for_ledger("users:main"));
        assert!(!principal.is_authorized_for_ledger("other:main"));
    }

    #[test]
    fn test_storage_all_authorization() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: None,
            storage_all: true,
            storage_ledgers: HashSet::new(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_authorized_for_ledger("any:ledger"));
        assert!(principal.is_authorized_for_ledger("books:main"));
    }

    #[test]
    fn test_no_storage_permissions() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: None,
            identity: None,
            storage_all: false,
            storage_ledgers: HashSet::new(),
        };

        assert!(!principal.has_storage_permissions());
        assert!(!principal.is_authorized_for_ledger("any:ledger"));
    }
}

/// Shared test helpers for storage proxy auth tests (both oidc and non-oidc paths)
#[cfg(test)]
mod test_helpers {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::Signer;
    use serde_json::json;

    use crate::config::{EventsAuthConfig, EventsAuthMode, StorageProxyConfig};

    /// Generate a signing key and did:key for testing
    pub fn test_key() -> (ed25519_dalek::SigningKey, String) {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let did = fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes());
        (signing_key, did)
    }

    /// Create a JWS token (embedded JWK) with custom claims
    pub fn create_jws(claims: &serde_json::Value, key: &ed25519_dalek::SigningKey) -> String {
        let pubkey = key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        let header = json!({
            "alg": "EdDSA",
            "jwk": {
                "kty": "OKP",
                "crv": "Ed25519",
                "x": pubkey_b64
            }
        });

        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{header_b64}.{payload_b64}.{sig_b64}")
    }

    pub fn now_secs() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// StorageProxyConfig that trusts a specific issuer
    pub fn config_trusted(did: &str) -> StorageProxyConfig {
        StorageProxyConfig {
            enabled: true,
            trusted_issuers: Some(vec![did.to_string()]),
            has_jwks_issuers: false,
            ..Default::default()
        }
    }

    /// EventsAuthConfig with no issuers (storage proxy uses its own list)
    pub fn empty_events_auth() -> EventsAuthConfig {
        EventsAuthConfig {
            mode: EventsAuthMode::None,
            ..Default::default()
        }
    }
}

/// Token verification tests (non-oidc builds)
#[cfg(test)]
#[cfg(not(feature = "oidc"))]
mod tests_verify {
    use super::test_helpers::*;
    use super::*;
    use serde_json::json;

    #[test]
    fn test_verify_storage_all() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.identity": "ex:PeerServiceAccount",
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth);
        assert!(result.is_ok(), "should accept valid storage.all token");
        let StorageProxyBearer(principal) = result.unwrap();
        assert!(principal.storage_all);
        assert_eq!(principal.identity.as_deref(), Some("ex:PeerServiceAccount"));
    }

    #[test]
    fn test_verify_storage_ledgers() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.ledgers": ["books:main", "users:main"]
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth);
        assert!(result.is_ok());
        let StorageProxyBearer(principal) = result.unwrap();
        assert!(!principal.storage_all);
        assert!(principal.is_authorized_for_ledger("books:main"));
        assert!(principal.is_authorized_for_ledger("users:main"));
        assert!(!principal.is_authorized_for_ledger("other:main"));
    }

    #[test]
    fn test_verify_no_storage_permissions_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.events.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth);
        assert!(
            result.is_err(),
            "token without storage permissions should be rejected"
        );
    }

    #[test]
    fn test_verify_untrusted_issuer_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        // Config trusts a different DID
        let config = config_trusted("did:key:z6Mkother");
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth);
        assert!(result.is_err(), "untrusted issuer should be rejected");
    }

    #[test]
    fn test_verify_expired_token_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() - 100, // expired
            "iat": now_secs() - 3700,
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth);
        assert!(result.is_err(), "expired token should be rejected");
    }
}

/// Token verification tests (oidc builds — tests embedded JWK path through async fn)
#[cfg(test)]
#[cfg(feature = "oidc")]
mod tests_verify_oidc {
    use super::test_helpers::*;
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_verify_storage_all() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.identity": "ex:PeerServiceAccount",
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(result.is_ok(), "should accept valid storage.all token");
        let StorageProxyBearer(principal) = result.unwrap();
        assert!(principal.storage_all);
        assert_eq!(principal.identity.as_deref(), Some("ex:PeerServiceAccount"));
    }

    #[tokio::test]
    async fn test_verify_storage_ledgers() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.ledgers": ["books:main", "users:main"]
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(result.is_ok());
        let StorageProxyBearer(principal) = result.unwrap();
        assert!(!principal.storage_all);
        assert!(principal.is_authorized_for_ledger("books:main"));
        assert!(principal.is_authorized_for_ledger("users:main"));
        assert!(!principal.is_authorized_for_ledger("other:main"));
    }

    #[tokio::test]
    async fn test_verify_no_storage_permissions_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.events.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(
            result.is_err(),
            "token without storage permissions should be rejected"
        );
    }

    #[tokio::test]
    async fn test_verify_untrusted_issuer_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted("did:key:z6Mkother");
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(result.is_err(), "untrusted issuer should be rejected");
    }

    #[tokio::test]
    async fn test_verify_expired_token_rejected() {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "peer@example.com",
            "exp": now_secs() - 100,
            "iat": now_secs() - 3700,
            "fluree.storage.all": true
        });
        let token = create_jws(&claims, &key);
        let config = config_trusted(&did);
        let events_auth = empty_events_auth();

        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(result.is_err(), "expired token should be rejected");
    }

    #[tokio::test]
    async fn test_verify_oidc_no_jwks_cache_rejected() {
        // An RS256 token with kid but no JWKS cache should fail
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        let rsa_private =
            include_str!("../../../fluree-db-credential/tests/fixtures/test_rsa_private.pem");
        let encoding_key = EncodingKey::from_rsa_pem(rsa_private.as_bytes()).unwrap();

        let claims = json!({
            "iss": "https://solo.example.com",
            "sub": "peer@example.com",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.all": true
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-kid".to_string());
        let token = encode(&header, &claims, &encoding_key).unwrap();

        let config = StorageProxyConfig {
            enabled: true,
            has_jwks_issuers: true,
            ..Default::default()
        };
        let events_auth = empty_events_auth();

        // No JWKS cache → should reject
        let result = verify_token(&token, &config, &events_auth, None).await;
        assert!(
            result.is_err(),
            "RS256 token without JWKS cache should be rejected"
        );
    }
}
