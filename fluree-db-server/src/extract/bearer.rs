//! Bearer token extraction for SSE events authentication
//!
//! Provides an Axum extractor that verifies JWT/JWS Bearer tokens
//! for the `/fluree/events` SSE endpoint.
//!
//! # Modes
//! - `None`: Token ignored entirely (no parsing, no logging)
//! - `Optional`: Accept tokens but don't require (invalid token = 401)
//! - `Required`: Must have valid token (missing token = 401)
//!
//! When the `oidc` feature is enabled, tokens are dispatched through
//! [`verify_bearer_token`](crate::token_verify::verify_bearer_token) which
//! supports both embedded-JWK (Ed25519) and OIDC/JWKS (RS256) paths.

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::header::{HeaderMap, AUTHORIZATION};
use axum::http::request::Parts;
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::{EventsAuthConfig, EventsAuthMode};
use crate::error::ServerError;
use crate::state::AppState;
#[cfg(not(feature = "oidc"))]
use fluree_db_credential::{verify_jws, EventsTokenPayload};

/// Verified principal from Bearer token
#[derive(Debug, Clone)]
pub struct EventsPrincipal {
    /// Issuer did:key (from iss claim, verified against signing key)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,

    // Events permissions
    /// fluree.events.all claim
    pub allowed_all: bool,
    /// fluree.events.ledgers claim (HashSet for O(1) lookup)
    pub allowed_ledgers: HashSet<String>,
    /// fluree.events.graph_sources claim (HashSet for O(1) lookup)
    pub allowed_graph_sources: HashSet<String>,

    // Storage proxy permissions
    /// fluree.storage.all claim
    pub storage_all: bool,
    /// fluree.storage.ledgers claim (HashSet for O(1) lookup)
    pub storage_ledgers: HashSet<String>,
}

impl EventsPrincipal {
    /// Check if principal has any storage proxy permissions
    pub fn has_storage_permissions(&self) -> bool {
        self.storage_all || !self.storage_ledgers.is_empty()
    }

    /// Check if principal is authorized for a specific ledger alias (storage proxy)
    pub fn is_storage_authorized_for_ledger(&self, alias: &str) -> bool {
        self.storage_all || self.storage_ledgers.contains(alias)
    }
}

/// Optional Bearer token extractor for events endpoint.
///
/// # Behavior by Auth Mode
/// - `None`: Always returns `MaybeBearer(None)`, token ignored
/// - `Optional`: Returns `MaybeBearer(Some(principal))` if token valid,
///   `MaybeBearer(None)` if no token, 401 if invalid token
/// - `Required`: Returns `MaybeBearer(Some(principal))` if token valid,
///   401 if no token or invalid token
#[derive(Debug)]
pub struct MaybeBearer(pub Option<EventsPrincipal>);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for MaybeBearer {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let config = state.config.events_auth();

        // In None mode, ignore token entirely (no parsing, no logging)
        if config.mode == EventsAuthMode::None {
            return Ok(MaybeBearer(None));
        }

        // Extract Authorization header (case-insensitive, trim whitespace)
        let token = match extract_bearer_token(&parts.headers) {
            Some(t) => t,
            None => {
                return match config.mode {
                    EventsAuthMode::Required => {
                        Err(ServerError::unauthorized("Bearer token required"))
                    }
                    _ => Ok(MaybeBearer(None)),
                };
            }
        };

        // From here on, we have a token and must validate it
        // (even in Optional mode, invalid token = 401)
        #[cfg(feature = "oidc")]
        {
            let jwks_cache = state.jwks_cache.as_deref();
            verify_token(&token, &config, jwks_cache).await
        }
        #[cfg(not(feature = "oidc"))]
        {
            verify_token(&token, &config)
        }
    }
}

/// Extract bearer token with HTTP-standard tolerance:
/// - Case-insensitive scheme ("Bearer", "bearer", "BEARER")
/// - Trim leading/trailing whitespace from header and token
///
/// Shared between `MaybeBearer` (events) and `StorageProxyBearer` (storage proxy).
pub(crate) fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get(AUTHORIZATION)?.to_str().ok()?.trim();
    let auth_lower = auth.to_ascii_lowercase();
    if auth_lower.starts_with("bearer ") {
        Some(auth[7..].trim().to_string())
    } else {
        None
    }
}

/// Verify token and build principal (embedded JWK only — non-oidc builds)
#[cfg(not(feature = "oidc"))]
fn verify_token(token: &str, config: &EventsAuthConfig) -> Result<MaybeBearer, ServerError> {
    // 1. Verify JWS (embedded JWK mode)
    let verified = verify_jws(token)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token: {}", e)))?;

    // 2. Parse combined payload (single parse)
    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {}", e)))?;

    // 3. Validate standard claims (identity required in Required mode)
    payload
        .validate(
            config.audience.as_deref(),
            &verified.did,
            config.requires_identity(),
        )
        .map_err(|e| ServerError::unauthorized(e.to_string()))?;

    // 4. Check issuer trust (always required when token presented)
    // Use verified.did (not payload.iss) — validate() confirmed they match
    if !config.is_issuer_trusted(&verified.did) {
        return Err(ServerError::unauthorized("Untrusted issuer"));
    }

    // 5. Check token grants some permissions
    if !payload.has_permissions() {
        return Err(ServerError::unauthorized("token authorizes no resources"));
    }

    // 6. Build principal with HashSet for efficient filtering
    let principal = build_principal(&payload, verified.did);

    Ok(MaybeBearer(Some(principal)))
}

/// Verify token with dual-path dispatch (oidc builds)
///
/// Supports both embedded-JWK (Ed25519) and OIDC/JWKS (RS256) tokens,
/// mirroring the same dual-path dispatch used by data and admin endpoints.
#[cfg(feature = "oidc")]
async fn verify_token(
    token: &str,
    config: &EventsAuthConfig,
    jwks_cache: Option<&crate::jwks::JwksCache>,
) -> Result<MaybeBearer, ServerError> {
    // 1. Dual-path dispatch (reuse shared verify_bearer_token)
    let verified = crate::token_verify::verify_bearer_token(token, jwks_cache).await?;

    // 2. Path-specific claims validation
    if verified.is_oidc {
        // OIDC: validate iss == expected_issuer, exp/nbf
        verified
            .payload
            .validate_oidc(
                config.audience.as_deref(),
                &verified.issuer,
                config.requires_identity(),
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;
        // OIDC trust already verified by JWKS path (only configured issuers' keys work)
    } else {
        // Embedded JWK: validate iss == did:key, exp/nbf
        verified
            .payload
            .validate(
                config.audience.as_deref(),
                &verified.issuer,
                config.requires_identity(),
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;
        // Check did:key trust — use verified.issuer for consistency
        if !config.is_issuer_trusted(&verified.issuer) {
            return Err(ServerError::unauthorized("Untrusted issuer"));
        }
    }

    // 3. Check token grants some permissions
    if !verified.payload.has_permissions() {
        return Err(ServerError::unauthorized("token authorizes no resources"));
    }

    // 4. Build principal — use verified.issuer as the authoritative identity
    let principal = build_principal(&verified.payload, verified.issuer);

    Ok(MaybeBearer(Some(principal)))
}

/// Build an `EventsPrincipal` from verified claims.
fn build_principal(
    payload: &fluree_db_credential::jwt_claims::EventsTokenPayload,
    issuer: String,
) -> EventsPrincipal {
    EventsPrincipal {
        issuer,
        subject: payload.sub.clone(),
        identity: payload.resolve_identity(),
        // Events permissions
        allowed_all: payload.events_all.unwrap_or(false),
        allowed_ledgers: payload
            .events_ledgers
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        allowed_graph_sources: payload
            .events_graph_sources
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        // Storage proxy permissions
        storage_all: payload.storage_all.unwrap_or(false),
        storage_ledgers: payload
            .storage_ledgers
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}

/// Shared test helpers for events auth tests (both oidc and non-oidc paths)
#[cfg(test)]
mod test_helpers {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::{Signer, SigningKey};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    pub fn create_test_token(claims: &serde_json::Value, signing_key: &SigningKey) -> String {
        let pubkey = signing_key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        let header = serde_json::json!({
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
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{header_b64}.{payload_b64}.{sig_b64}")
    }
}

// Tests that don't depend on verify_token — always compiled
#[cfg(test)]
mod tests_common {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_extract_bearer_token_standard() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer eyJhbGciOiJFZERTQSJ9.payload.sig"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("eyJhbGciOiJFZERTQSJ9.payload.sig".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_lowercase() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("bearer eyJhbGciOiJFZERTQSJ9.payload.sig"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("eyJhbGciOiJFZERTQSJ9.payload.sig".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_uppercase() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("BEARER eyJhbGciOiJFZERTQSJ9.payload.sig"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("eyJhbGciOiJFZERTQSJ9.payload.sig".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_with_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer   eyJhbGciOiJFZERTQSJ9.payload.sig   "),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("eyJhbGciOiJFZERTQSJ9.payload.sig".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_leading_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("  Bearer eyJhbGciOiJFZERTQSJ9.payload.sig"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("eyJhbGciOiJFZERTQSJ9.payload.sig".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn test_extract_bearer_token_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn test_events_principal_fields() {
        let principal = EventsPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("user@example.com".to_string()),
            identity: Some("user@example.com".to_string()),
            allowed_all: false,
            allowed_ledgers: vec!["books:main".to_string()].into_iter().collect(),
            allowed_graph_sources: HashSet::new(),
            storage_all: false,
            storage_ledgers: HashSet::new(),
        };

        assert!(!principal.allowed_all);
        assert!(principal.allowed_ledgers.contains("books:main"));
        assert!(!principal.allowed_graph_sources.contains("search:main"));
    }

    #[test]
    fn test_storage_principal_fields() {
        let principal = EventsPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: Some("ex:PeerServiceAccount".to_string()),
            allowed_all: false,
            allowed_ledgers: HashSet::new(),
            allowed_graph_sources: HashSet::new(),
            storage_all: false,
            storage_ledgers: vec!["books:main".to_string(), "users:main".to_string()]
                .into_iter()
                .collect(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_storage_authorized_for_ledger("books:main"));
        assert!(principal.is_storage_authorized_for_ledger("users:main"));
        assert!(!principal.is_storage_authorized_for_ledger("other:main"));
    }

    #[test]
    fn test_storage_all_authorization() {
        let principal = EventsPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: None,
            allowed_all: false,
            allowed_ledgers: HashSet::new(),
            allowed_graph_sources: HashSet::new(),
            storage_all: true,
            storage_ledgers: HashSet::new(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_storage_authorized_for_ledger("any:ledger"));
        assert!(principal.is_storage_authorized_for_ledger("books:main"));
    }
}

/// Tests for non-oidc build path (sync verify_token with 2 params)
#[cfg(test)]
#[cfg(not(feature = "oidc"))]
mod tests {
    use super::test_helpers::*;
    use super::*;
    use ed25519_dalek::SigningKey;
    use fluree_db_credential::did_from_pubkey;

    #[test]
    fn test_verify_token_valid_with_trusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config).unwrap();
        let principal = result.0.unwrap();

        assert_eq!(principal.issuer, did);
        assert_eq!(principal.subject, Some("user@example.com".to_string()));
        assert!(principal.allowed_all);
    }

    #[test]
    fn test_verify_token_untrusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        // Config with different trusted issuer
        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec!["did:key:z6MkOTHER".to_string()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Untrusted issuer"));
    }

    #[test]
    fn test_verify_token_insecure_accept_any_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        // No trusted issuers, but insecure flag set
        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: true,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_token_no_permissions() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600
            // No fluree.events.* claims
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("token authorizes no resources"));
    }

    #[test]
    fn test_verify_token_expired() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() - 120,  // Expired 2 minutes ago (beyond skew)
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expired"));
    }

    #[test]
    fn test_verify_token_with_ledgers() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.ledgers": ["books:main", "users:prod"]
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config).unwrap();
        let principal = result.0.unwrap();

        assert!(!principal.allowed_all);
        assert!(principal.allowed_ledgers.contains("books:main"));
        assert!(principal.allowed_ledgers.contains("users:prod"));
        assert_eq!(principal.allowed_ledgers.len(), 2);
    }

    #[test]
    fn test_verify_token_identity_resolution() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        // fluree.identity takes precedence over sub
        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.identity": "custom-identity",
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config).unwrap();
        let principal = result.0.unwrap();

        assert_eq!(principal.subject, Some("user@example.com".to_string()));
        assert_eq!(principal.identity, Some("custom-identity".to_string()));
    }

    #[test]
    fn test_verify_token_invalid_jws() {
        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: true,
            has_jwks_issuers: false,
        };

        let result = verify_token("not.a.valid.jws", &config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid token"));
    }
}

/// Tests for oidc build path (async verify_token with 3 params)
///
/// Exercises both the embedded-JWK path (jwks_cache=None) through the async
/// code path, and OIDC-specific error handling.
#[cfg(test)]
#[cfg(feature = "oidc")]
mod tests_oidc {
    use super::test_helpers::*;
    use super::*;
    use ed25519_dalek::SigningKey;
    use fluree_db_credential::did_from_pubkey;

    #[tokio::test]
    async fn test_verify_token_valid_with_trusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await.unwrap();
        let principal = result.0.unwrap();

        assert_eq!(principal.issuer, did);
        assert_eq!(principal.subject, Some("user@example.com".to_string()));
        assert!(principal.allowed_all);
    }

    #[tokio::test]
    async fn test_verify_token_untrusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec!["did:key:z6MkOTHER".to_string()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Untrusted issuer"));
    }

    #[tokio::test]
    async fn test_verify_token_insecure_accept_any_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: true,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_verify_token_no_permissions() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600
            // No fluree.events.* claims
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("token authorizes no resources"));
    }

    #[tokio::test]
    async fn test_verify_token_expired() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() - 120,  // Expired 2 minutes ago (beyond skew)
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expired"));
    }

    #[tokio::test]
    async fn test_verify_token_with_ledgers() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.events.ledgers": ["books:main", "users:prod"]
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await.unwrap();
        let principal = result.0.unwrap();

        assert!(!principal.allowed_all);
        assert!(principal.allowed_ledgers.contains("books:main"));
        assert!(principal.allowed_ledgers.contains("users:prod"));
        assert_eq!(principal.allowed_ledgers.len(), 2);
    }

    #[tokio::test]
    async fn test_verify_token_identity_resolution() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        // fluree.identity takes precedence over sub
        let claims = serde_json::json!({
            "iss": did,
            "sub": "user@example.com",
            "exp": now_secs() + 3600,
            "fluree.identity": "custom-identity",
            "fluree.events.all": true
        });

        let token = create_test_token(&claims, &signing_key);

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await.unwrap();
        let principal = result.0.unwrap();

        assert_eq!(principal.subject, Some("user@example.com".to_string()));
        assert_eq!(principal.identity, Some("custom-identity".to_string()));
    }

    #[tokio::test]
    async fn test_verify_token_invalid_jws() {
        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: true,
            has_jwks_issuers: false,
        };

        let result = verify_token("not.a.valid.jws", &config, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid token"));
    }

    /// Test that a token with a kid header (OIDC-style) but no JWKS cache
    /// returns a clear error about OIDC not being configured.
    #[tokio::test]
    async fn test_verify_token_oidc_no_jwks_cache() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

        // Create a token with kid header (OIDC-style) instead of embedded JWK
        let header = serde_json::json!({
            "alg": "RS256",
            "kid": "test-key-1",
            "typ": "JWT"
        });
        let claims = serde_json::json!({
            "iss": "https://auth.example.com",
            "sub": "user@example.com",
            "exp": now_secs() + 3600
        });

        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
        // Fake signature — won't get to verification since no JWKS cache
        let sig_b64 = URL_SAFE_NO_PAD.encode(b"fake-signature");
        let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

        let config = EventsAuthConfig {
            mode: EventsAuthMode::Required,
            audience: None,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: false,
            has_jwks_issuers: false,
        };

        let result = verify_token(&token, &config, None).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("OIDC issuer not configured"),
            "Expected OIDC-specific error, got: {err_msg}"
        );
    }
}
