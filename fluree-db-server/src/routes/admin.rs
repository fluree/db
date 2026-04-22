//! Admin endpoints: /health, /v1/fluree/stats, /v1/fluree/whoami, /.well-known/fluree.json, /swagger.json

use crate::config::{AdminAuthMode, DataAuthMode, EventsAuthMode};
use crate::error::Result;
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;
use tracing::Instrument;

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

/// Health check endpoint
///
/// GET /health
///
/// Returns a simple health check response to verify the server is running.
pub async fn health() -> Json<HealthResponse> {
    // Simple health check - no complex span needed
    tracing::debug!("health check requested");
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Server statistics response
#[derive(Serialize)]
pub struct StatsResponse {
    /// Server uptime in seconds
    pub uptime_secs: u64,
    /// Storage type (memory or file)
    pub storage_type: &'static str,
    /// Whether indexing is enabled
    pub indexing_enabled: bool,
    /// Number of cached ledgers
    pub cached_ledgers: usize,
    /// Server version
    pub version: &'static str,
}

/// Server statistics endpoint
///
/// GET /v1/fluree/stats
///
/// Returns server statistics including uptime, storage type, and cache info.
pub async fn stats(
    State(state): State<Arc<AppState>>,
    _headers: FlureeHeaders,
) -> Json<StatsResponse> {
    let span = tracing::debug_span!("stats");
    async move {
        tracing::info!("server stats requested");

        Json(StatsResponse {
            uptime_secs: state.uptime_secs(),
            storage_type: state.config.storage_type_str(),
            indexing_enabled: state.config.indexing_enabled,
            cached_ledgers: match state.fluree.ledger_manager() {
                Some(mgr) => mgr.cached_count().await,
                None => 0,
            },
            version: env!("CARGO_PKG_VERSION"),
        })
    }
    .instrument(span)
    .await
}

/// Who-am-I diagnostic endpoint
///
/// GET /v1/fluree/whoami
///
/// Verifies the Bearer token (if present) using the same cryptographic
/// verification paths as data endpoints, then returns a summary of the
/// verified principal. If verification fails, includes the error and
/// unverified decoded claims (marked as such) for debugging.
pub async fn whoami(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Json<WhoAmIResponse> {
    let token = crate::extract::extract_bearer_token(&headers);
    let Some(token) = token else {
        return Json(WhoAmIResponse {
            token_present: false,
            ..Default::default()
        });
    };

    // Attempt cryptographic verification through the same path data endpoints use
    #[cfg(feature = "oidc")]
    let verify_result = {
        let jwks_cache = state.jwks_cache.as_deref();
        crate::token_verify::verify_bearer_token(&token, jwks_cache).await
    };

    #[cfg(not(feature = "oidc"))]
    let verify_result = {
        let _ = &state; // suppress unused warning
        verify_embedded_jwk_for_whoami(&token)
    };

    match verify_result {
        Ok(verified) => {
            let payload = &verified.payload;
            Json(WhoAmIResponse {
                token_present: true,
                verified: true,
                auth_method: Some(if verified.is_oidc {
                    "oidc".to_string()
                } else {
                    "embedded_jwk".to_string()
                }),
                issuer: Some(verified.issuer),
                subject: payload.sub.clone(),
                identity: payload.resolve_identity(),
                expires_at: Some(payload.exp),
                scopes: Some(scopes_from_payload(payload)),
                ..Default::default()
            })
        }
        Err(e) => {
            // Verification failed — include unverified decoded claims for debugging
            let decoded = decode_unverified_claims(&token);
            Json(WhoAmIResponse {
                token_present: true,
                verified: false,
                error: Some(e.to_string()),
                issuer: decoded.as_ref().and_then(|d| d.issuer.clone()),
                subject: decoded.as_ref().and_then(|d| d.subject.clone()),
                identity: decoded.as_ref().and_then(|d| d.identity.clone()),
                expires_at: decoded.as_ref().and_then(|d| d.expires_at),
                ..Default::default()
            })
        }
    }
}

/// Verify via embedded JWK for the non-oidc build. Returns the same shape
/// as `token_verify::VerifiedToken` but without the OIDC module dependency.
#[cfg(not(feature = "oidc"))]
fn verify_embedded_jwk_for_whoami(
    token: &str,
) -> std::result::Result<WhoAmIVerified, crate::error::ServerError> {
    use fluree_db_credential::jwt_claims::EventsTokenPayload;

    let jws_verified = fluree_db_credential::verify_jws(token)
        .map_err(|e| crate::error::ServerError::unauthorized(format!("Invalid token: {e}")))?;

    let payload: EventsTokenPayload = serde_json::from_str(&jws_verified.payload).map_err(|e| {
        crate::error::ServerError::unauthorized(format!("Invalid token claims: {e}"))
    })?;

    Ok(WhoAmIVerified {
        payload,
        issuer: jws_verified.did,
        is_oidc: false,
    })
}

/// Minimal verified token for non-oidc builds (mirrors `token_verify::VerifiedToken`).
#[cfg(not(feature = "oidc"))]
struct WhoAmIVerified {
    payload: fluree_db_credential::jwt_claims::EventsTokenPayload,
    issuer: String,
    is_oidc: bool,
}

/// Whoami response shape.
#[derive(Serialize, Default)]
pub struct WhoAmIResponse {
    /// Whether a Bearer token was present in the request.
    pub token_present: bool,
    /// Whether the token's cryptographic signature was successfully verified.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub verified: bool,
    /// Verification method used: `"embedded_jwk"` (Ed25519) or `"oidc"` (JWKS/RS256).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<WhoAmIScopes>,
    /// Verification error message (only present when `verified` is false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Scope summary for the whoami response.
#[derive(Serialize, Default)]
pub struct WhoAmIScopes {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_read_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_write_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_all: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_read_ledgers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_write_ledgers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_ledgers: Option<Vec<String>>,
}

/// Extract scope fields from a verified payload into the response shape.
fn scopes_from_payload(p: &fluree_db_credential::jwt_claims::EventsTokenPayload) -> WhoAmIScopes {
    WhoAmIScopes {
        ledger_read_all: p.ledger_read_all,
        ledger_write_all: p.ledger_write_all,
        storage_all: p.storage_all,
        ledger_read_ledgers: p.ledger_read_ledgers.clone(),
        ledger_write_ledgers: p.ledger_write_ledgers.clone(),
        storage_ledgers: p.storage_ledgers.clone(),
    }
}

/// Unverified claims for the error-path debugging output.
struct DecodedClaims {
    issuer: Option<String>,
    subject: Option<String>,
    identity: Option<String>,
    expires_at: Option<u64>,
}

/// Decode JWT claims without verification (for error-path debugging only).
fn decode_unverified_claims(token: &str) -> Option<DecodedClaims> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;

    Some(DecodedClaims {
        issuer: claims.get("iss").and_then(|v| v.as_str()).map(String::from),
        subject: claims.get("sub").and_then(|v| v.as_str()).map(String::from),
        identity: claims
            .get("fluree.identity")
            .and_then(|v| v.as_str())
            .map(String::from),
        expires_at: claims.get("exp").and_then(serde_json::Value::as_u64),
    })
}

/// Auth discovery endpoint
///
/// GET /.well-known/fluree.json
///
/// Returns a discovery document that tells the CLI how to authenticate.
/// For standalone `fluree-server` this advertises `"type": "token"` when any
/// auth mode accepts tokens (Optional or Required), signalling that the CLI
/// should prompt for a manual Bearer token. When all auth modes are `None`,
/// the `auth` block is omitted (no token needed).
///
/// Solo (or other OIDC-capable products) can override this endpoint to return
/// full `oidc_device` configuration.
///
/// See docs/design/auth-contract.md for the full contract.
pub async fn discovery(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let config = &state.config;

    // Any auth mode other than None means the server accepts (or requires) tokens.
    let any_auth_enabled = config.events_auth().mode != EventsAuthMode::None
        || config.data_auth().mode != DataAuthMode::None
        || config.admin_auth().mode != AdminAuthMode::None;

    let mut doc = serde_json::json!({
        "version": 1,
        // Versioned API base path (resolved against discovery origin by the CLI).
        "api_base_url": "/v1/fluree",
    });

    if any_auth_enabled {
        doc["auth"] = serde_json::json!({
            "type": "token",
        });
    }

    Json(doc)
}

/// OpenAPI specification endpoint
///
/// GET /swagger.json
///
/// Returns the OpenAPI specification for the Fluree server API.
/// TODO: Generate from utoipa annotations
pub async fn openapi_spec() -> Result<Json<serde_json::Value>> {
    // Minimal OpenAPI spec - will be expanded with utoipa
    let spec = serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Fluree DB Server",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "HTTP REST API for Fluree DB"
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {
                        "200": {
                            "description": "Server is healthy"
                        }
                    }
                }
            },
            "/v1/fluree/create": {
                "post": {
                    "summary": "Create a new ledger",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "ledger": {
                                            "type": "string",
                                            "description": "Ledger alias"
                                        }
                                    },
                                    "required": ["ledger"]
                                }
                            }
                        }
                    }
                }
            },
            "/v1/fluree/query": {
                "post": {
                    "summary": "Execute a query",
                    "description": "Execute JSON-LD or SPARQL queries"
                }
            },
            "/v1/fluree/update": {
                "post": {
                    "summary": "Execute an update transaction"
                }
            }
        }
    });

    Ok(Json(spec))
}
