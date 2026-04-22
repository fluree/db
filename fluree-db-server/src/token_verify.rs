//! Unified token verification: dual-path dispatch.
//!
//! Decides whether to use the embedded-JWK path (existing Ed25519) or
//! the JWKS path (new, for OIDC tokens) based on the JWT header.
//!
//! - **Embedded JWK**: Token header contains `jwk` field → existing `verify_jws()` path
//! - **JWKS (kid)**: Token header contains `kid` field → OIDC `verify_jwt()` path

use crate::error::ServerError;
use crate::jwks::JwksCache;
use fluree_db_credential::jwt_claims::EventsTokenPayload;
use fluree_db_credential::{verify_jws, JwsVerified};
use jsonwebtoken::Algorithm;

/// Result of unified token verification.
pub struct VerifiedToken {
    /// Parsed claims payload.
    pub payload: EventsTokenPayload,
    /// Issuer (did:key for embedded JWK, URL for OIDC).
    pub issuer: String,
    /// Whether this was verified via the OIDC/JWKS path (vs embedded JWK).
    pub is_oidc: bool,
}

/// Verify a bearer token using dual-path dispatch.
///
/// 1. Peek at the JWT header.
/// 2. If header has an embedded JWK (no kid), use existing `verify_jws()` path.
/// 3. If header has a kid (no embedded JWK), use OIDC `verify_jwt()` path.
///
/// # Arguments
/// * `token` - The raw bearer token string
/// * `jwks_cache` - JWKS cache for OIDC verification (None if OIDC not configured)
pub async fn verify_bearer_token(
    token: &str,
    jwks_cache: Option<&JwksCache>,
) -> Result<VerifiedToken, ServerError> {
    let (kid, _alg, has_embedded_jwk) = fluree_db_credential::peek_jwt_header(token)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token header: {e}")))?;

    if has_embedded_jwk {
        // Existing path: embedded JWK (Ed25519)
        return verify_embedded_jwk(token);
    }

    // OIDC path: need kid
    let kid = kid.ok_or_else(|| {
        ServerError::unauthorized(
            "Token has neither embedded JWK nor kid header — \
             cannot determine verification method"
                .to_string(),
        )
    })?;

    let jwks_cache = jwks_cache.ok_or_else(|| {
        ServerError::unauthorized(
            "OIDC issuer not configured: server has no JWKS issuers".to_string(),
        )
    })?;

    verify_via_jwks(token, &kid, jwks_cache).await
}

/// Verify using the existing embedded-JWK path (Ed25519).
///
/// The issuer is set to `verified.did` (the did:key derived from the embedded
/// signing key), NOT `payload.iss`. This ensures the downstream `validate()`
/// call confirms that the `iss` claim matches the actual signer — preventing
/// a token from claiming an arbitrary issuer while being signed by a different key.
fn verify_embedded_jwk(token: &str) -> Result<VerifiedToken, ServerError> {
    let verified: JwsVerified =
        verify_jws(token).map_err(|e| ServerError::unauthorized(format!("Invalid token: {e}")))?;

    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token claims: {e}")))?;

    Ok(VerifiedToken {
        issuer: verified.did,
        payload,
        is_oidc: false,
    })
}

/// Verify using the OIDC/JWKS path.
async fn verify_via_jwks(
    token: &str,
    kid: &str,
    jwks_cache: &JwksCache,
) -> Result<VerifiedToken, ServerError> {
    // Decode the unverified payload to extract `iss` for key lookup.
    // This is safe: we reject immediately if `iss` is not a configured issuer,
    // and we verify the signature before trusting any claims.
    let issuer = fluree_db_credential::decode_unverified_issuer(token)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token format: {e}")))?
        .ok_or_else(|| ServerError::unauthorized("Token missing iss claim".to_string()))?;

    // Immediately reject if issuer is not configured
    if !jwks_cache.is_configured_issuer(&issuer) {
        return Err(ServerError::unauthorized(format!(
            "OIDC issuer not configured: {issuer}"
        )));
    }

    // Look up the key from the JWKS cache
    let key = jwks_cache
        .get_key(&issuer, kid)
        .await
        .map_err(|e| ServerError::unauthorized(format!("JWKS key lookup failed: {e}")))?;

    // Verify the token with the looked-up key (RS256 constrained)
    let jwt_verified = fluree_db_credential::verify_jwt(token, &key, &[Algorithm::RS256], &issuer)
        .map_err(|e| ServerError::unauthorized(format!("Token verification failed: {e}")))?;

    // Parse claims into EventsTokenPayload
    let payload: EventsTokenPayload = serde_json::from_str(&jwt_verified.payload_json)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token claims: {e}")))?;

    Ok(VerifiedToken {
        issuer: jwt_verified.issuer,
        payload,
        is_oidc: true,
    })
}
