//! Bearer token extraction for data API authentication (query/update/info/exists).
//!
//! This extractor verifies JWT/JWS Bearer tokens and yields a `DataPrincipal`
//! containing ledger read/write scopes and policy identity.
//!
//! When the `oidc` feature is enabled, tokens are dispatched through
//! [`verify_bearer_token`](crate::token_verify::verify_bearer_token) which
//! supports both embedded-JWK (Ed25519) and OIDC/JWKS (RS256) paths.
//!
//! Signed requests (JWS/VC in request body) are handled separately by
//! [`MaybeCredential`](crate::extract::MaybeCredential). Data endpoints can accept
//! either mechanism depending on `data_auth.mode`.

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::DataAuthMode;
use crate::error::ServerError;
use crate::state::AppState;
use fluree_db_credential::jwt_claims::EventsTokenPayload;

/// Verified principal from a data API Bearer token
#[derive(Debug, Clone)]
pub struct DataPrincipal {
    /// Issuer (did:key for embedded JWK, URL for OIDC)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
    /// Read access to all ledgers
    pub read_all: bool,
    /// Read access to specific ledgers (HashSet for O(1) lookup)
    pub read_ledgers: HashSet<String>,
    /// Write access to all ledgers
    pub write_all: bool,
    /// Write access to specific ledgers (HashSet for O(1) lookup)
    pub write_ledgers: HashSet<String>,
}

impl DataPrincipal {
    pub fn can_read(&self, ledger_id: &str) -> bool {
        self.read_all || self.read_ledgers.contains(ledger_id)
    }

    pub fn can_write(&self, ledger_id: &str) -> bool {
        self.write_all || self.write_ledgers.contains(ledger_id)
    }
}

/// Optional/required data API Bearer token extractor.
#[derive(Debug)]
pub struct MaybeDataBearer(pub Option<DataPrincipal>);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for MaybeDataBearer {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let config = state.config.data_auth();

        // In None mode, ignore token entirely
        if config.mode == DataAuthMode::None {
            return Ok(MaybeDataBearer(None));
        }

        // Extract Authorization header (case-insensitive, trim whitespace)
        let token = match super::extract_bearer_token(&parts.headers) {
            Some(t) => t,
            None => {
                return match config.mode {
                    DataAuthMode::Required => {
                        Err(ServerError::unauthorized("Bearer token required"))
                    }
                    _ => Ok(MaybeDataBearer(None)),
                };
            }
        };

        verify_data_token(&token, state).await
    }
}

/// Verify token and build `DataPrincipal`.
///
/// When `oidc` feature is enabled, uses dual-path dispatch (embedded JWK or JWKS).
/// When `oidc` feature is disabled, only the embedded JWK path is available.
async fn verify_data_token(token: &str, state: &AppState) -> Result<MaybeDataBearer, ServerError> {
    let config = state.config.data_auth();

    // Verify the token and extract claims
    #[cfg(feature = "oidc")]
    let (payload, issuer, is_oidc) = {
        let jwks_cache = state.jwks_cache.as_deref();
        let verified = crate::token_verify::verify_bearer_token(token, jwks_cache).await?;
        (verified.payload, verified.issuer, verified.is_oidc)
    };

    #[cfg(not(feature = "oidc"))]
    let (payload, issuer, is_oidc) = {
        let verified = fluree_db_credential::verify_jws(token)
            .map_err(|e| ServerError::unauthorized(format!("Invalid token: {}", e)))?;
        let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
            .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {}", e)))?;
        // Use verified.did (did:key derived from the embedded signing key), NOT
        // payload.iss, so that validate() confirms iss matches the actual signer.
        (payload, verified.did, false)
    };

    // Validate claims (path-specific)
    if is_oidc {
        // OIDC: validate iss == expected_issuer, exp/nbf/aud
        payload
            .validate_oidc(
                config.audience.as_deref(),
                &issuer,
                false, // identity not strictly required
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;
        // For OIDC tokens, issuer trust is already verified by the JWKS path:
        // only configured issuers' keys can verify the signature.
    } else {
        // Embedded JWK: validate iss == did:key, exp/nbf/aud
        payload
            .validate(
                config.audience.as_deref(),
                &issuer, // did:key derived from signing key
                false,
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;

        // Check issuer trust for did:key tokens.
        // At this point validate() confirmed payload.iss == issuer (verified.did),
        // so either can be used for the trust check.
        if !config.is_issuer_trusted(&issuer) {
            return Err(ServerError::unauthorized("Untrusted issuer"));
        }
    }

    // Require some data permissions
    if !payload.has_ledger_read_permissions() && !payload.has_ledger_write_permissions() {
        return Err(ServerError::unauthorized("token authorizes no resources"));
    }

    let principal = build_principal(&payload);
    Ok(MaybeDataBearer(Some(principal)))
}

/// Build a `DataPrincipal` from verified claims.
fn build_principal(payload: &EventsTokenPayload) -> DataPrincipal {
    DataPrincipal {
        issuer: payload.iss.clone(),
        subject: payload.sub.clone(),
        identity: payload.resolve_identity(),
        // Read: use explicit ledger.read.* if present, else fall back to storage.*
        read_all: payload.ledger_read_all.unwrap_or(false) || payload.storage_all.unwrap_or(false),
        read_ledgers: payload
            .ledger_read_ledgers
            .clone()
            .or_else(|| payload.storage_ledgers.clone())
            .unwrap_or_default()
            .into_iter()
            .collect(),
        write_all: payload.ledger_write_all.unwrap_or(false),
        write_ledgers: payload
            .ledger_write_ledgers
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}
