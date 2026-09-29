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

use super::CredentialPolicy;
use crate::config::DataAuthMode;
use crate::error::ServerError;
use crate::state::AppState;
use fluree_db_api::LedgerId;
use fluree_db_credential::jwt_claims::EventsTokenPayload;
use fluree_db_credential::jwt_claims::PolicyClaim;

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
    /// Read access to specific ledgers. A bare `mydb` scope means `mydb:main`;
    /// there is no whole-ledger scope.
    pub read_ledgers: HashSet<LedgerId>,
    /// Write access to all ledgers
    pub write_all: bool,
    /// Write access to specific ledgers, parsed like `read_ledgers`.
    pub write_ledgers: HashSet<LedgerId>,
    /// Token expiry (Unix seconds). HTTP re-verifies per request so this is
    /// redundant there; long-lived transports (Bolt sessions) re-check it
    /// before each statement.
    pub expires_unix: u64,
    /// Policy selection constructed from verified claims and server configuration.
    pub policy_authorization: CredentialPolicy,
}

impl DataPrincipal {
    pub fn can_read(&self, ledger_id: &LedgerId) -> bool {
        let allowed = self.read_all || self.read_ledgers.contains(ledger_id);
        self.audit_scope(ledger_id, "read", allowed);
        allowed
    }

    pub fn can_write(&self, ledger_id: &LedgerId) -> bool {
        let allowed = self.write_all || self.write_ledgers.contains(ledger_id);
        self.audit_scope(ledger_id, "write", allowed);
        allowed
    }

    /// Request/statement-level evidence of the authority used for a scope
    /// check, not a claim that subsequent per-fact policy enforcement allowed
    /// the operation. Disabled unless this tracing target is enabled at DEBUG.
    fn audit_scope(&self, ledger_id: &LedgerId, action: &str, allowed: bool) {
        let effective_identity = self
            .policy_authorization
            .fixed_options()
            .and_then(|o| o.identity.as_deref());
        let mode = self.policy_authorization.mode();
        tracing::debug!(
            target: "fluree_db_server::authorization",
            issuer = %self.issuer,
            effective_identity = ?effective_identity,
            authorization_mode = mode,
            ledger = %ledger_id,
            action,
            scope_allowed = allowed,
            "data authorization scope check"
        );
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

        verify_data_principal(&token, state)
            .await
            .map(|p| MaybeDataBearer(Some(p)))
    }
}

/// Verify a data-plane bearer token and build the `DataPrincipal`.
///
/// The complete, transport-agnostic identity pipeline: signature
/// verification (dual-path dispatch when `oidc` is enabled), claim
/// validation, issuer trust for did:key tokens, and the permission-scope
/// check. Every transport that accepts data-plane tokens (HTTP extractor,
/// Bolt LOGON) must resolve identity through this one function.
pub(crate) async fn verify_data_principal(
    token: &str,
    state: &AppState,
) -> Result<DataPrincipal, ServerError> {
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
            .map_err(|e| ServerError::unauthorized(format!("Invalid token: {e}")))?;
        let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
            .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {e}")))?;
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

    if payload.fluree_policy.is_some()
        && (!config.policy_authorities.contains(&issuer)
            || config.audience.as_deref().is_none_or(str::is_empty))
    {
        return Err(ServerError::unauthorized(
            "Issuer is not a configured policy authority",
        ));
    }
    let authorization = if matches!(payload.fluree_policy, Some(PolicyClaim::Request(_))) {
        CredentialPolicy::Request
    } else if let Some(PolicyClaim::Fixed(policy)) = &payload.fluree_policy {
        CredentialPolicy::Fixed(fluree_db_api::PolicyAuthorization::from_trusted_options(
            fluree_db_api::GovernanceOptions {
                identity: payload.resolve_identity(),
                policy_class: policy.policy_class.clone(),
                policy: policy.policy.clone(),
                policy_values: policy.policy_values.clone(),
                default_allow: policy.default_allow,
                ..Default::default()
            },
        ))
    } else if payload.resolve_identity().is_none() && config.default_policy_class.is_none() {
        CredentialPolicy::ScopeOnly
    } else {
        CredentialPolicy::Fixed(fluree_db_api::PolicyAuthorization::from_trusted_options(
            fluree_db_api::GovernanceOptions {
                identity: payload.resolve_identity(),
                policy_class: config.default_policy_class.map(|c| vec![c]),
                ..Default::default()
            },
        ))
    };
    Ok(build_principal(&payload, authorization))
}

/// Parse token ledger scopes once, at verification. An entry that is not a
/// ledger id grants nothing (fail closed) rather than matching itself.
pub(crate) fn parse_scopes(scopes: Option<&Vec<String>>) -> HashSet<LedgerId> {
    scopes
        .into_iter()
        .flatten()
        .filter_map(|s| match LedgerId::parse(s) {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!(scope = %s, error = %e, "ignoring token ledger scope that is not a ledger id");
                None
            }
        })
        .collect()
}

/// Build a `DataPrincipal` from verified claims.
fn build_principal(
    payload: &EventsTokenPayload,
    policy_authorization: CredentialPolicy,
) -> DataPrincipal {
    DataPrincipal {
        policy_authorization,
        issuer: payload.iss.clone(),
        subject: payload.sub.clone(),
        identity: payload.resolve_identity(),
        // Read: use explicit ledger.read.* if present, else fall back to storage.*
        read_all: payload.ledger_read_all.unwrap_or(false) || payload.storage_all.unwrap_or(false),
        read_ledgers: parse_scopes(
            payload
                .ledger_read_ledgers
                .as_ref()
                .or(payload.storage_ledgers.as_ref()),
        ),
        write_all: payload.ledger_write_all.unwrap_or(false),
        write_ledgers: parse_scopes(payload.ledger_write_ledgers.as_ref()),
        expires_unix: payload.exp,
    }
}
