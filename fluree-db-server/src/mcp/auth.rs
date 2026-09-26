//! Bearer token authentication for MCP endpoint
//!
//! Provides middleware to verify JWT/JWS Bearer tokens for MCP requests.
//! Reuses the same token format and verification as the events endpoint.

use crate::config::McpAuthConfig;
use crate::error::ServerError;
use crate::extract::extract_bearer_token;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use fluree_db_api::LedgerId;
use fluree_db_credential::{verify_jws, EventsTokenPayload};
use std::collections::HashSet;
use std::sync::Arc;

/// Verified principal from MCP Bearer token
#[derive(Debug, Clone)]
pub struct McpPrincipal {
    /// Issuer did:key (from iss claim, verified against signing key); `None`
    /// on a server that runs MCP without tokens.
    pub issuer: Option<String>,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
    /// Read access to all ledgers (`fluree.ledger.read.all`).
    pub read_all: bool,
    /// Ledgers this token may read (`fluree.ledger.read.ledgers`), parsed like
    /// data API scopes: `mydb` means `mydb:main`.
    pub read_ledgers: HashSet<LedgerId>,
}

impl McpPrincipal {
    /// The principal of a tokenless request: every ledger, no identity, so
    /// ledger policy defaults apply exactly as on the open data API.
    fn open() -> Self {
        Self {
            issuer: None,
            subject: None,
            identity: None,
            read_all: true,
            read_ledgers: HashSet::new(),
        }
    }

    /// Whether this token may read `ledger_id`. Issuer trust admits a token;
    /// its ledger claims decide what it reaches.
    pub fn can_read(&self, ledger_id: &LedgerId) -> bool {
        self.read_all || self.read_ledgers.contains(ledger_id)
    }
}

/// Middleware to validate MCP Bearer tokens.
///
/// When tokens are required (see [`McpAuthConfig::token_required`]), every
/// request to /mcp must carry a valid Bearer token from a trusted issuer.
/// Otherwise any token is ignored, as the data API does in `none` mode.
pub async fn validate_mcp_token(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let mcp_auth = state.config.mcp_auth();
    let events_auth = state.config.events_auth();

    if !mcp_auth.token_required(&events_auth) {
        request.extensions_mut().insert(McpPrincipal::open());
        return next.run(request).await;
    }

    // Extract Bearer token from Authorization header
    let token = match extract_bearer_token(request.headers()) {
        Some(t) => t,
        None => {
            tracing::debug!("MCP request missing Bearer token");
            return (
                StatusCode::UNAUTHORIZED,
                "Bearer token required for MCP endpoint",
            )
                .into_response();
        }
    };

    // Verify token and build principal
    let principal = match verify_mcp_token(&token, &mcp_auth, &events_auth) {
        Ok(p) => p,
        Err(e) => {
            // Log detailed error but return generic message to client
            tracing::warn!(error = %e, "MCP token verification failed");
            return (StatusCode::UNAUTHORIZED, "Invalid or unauthorized token").into_response();
        }
    };

    tracing::debug!(
        issuer = ?principal.issuer,
        identity = ?principal.identity,
        "MCP token verified"
    );

    // Store principal in request extensions for tools to access
    request.extensions_mut().insert(principal);

    // Continue to the MCP service
    next.run(request).await
}

/// Verify MCP token and build principal
fn verify_mcp_token(
    token: &str,
    mcp_auth: &McpAuthConfig,
    events_auth: &crate::config::EventsAuthConfig,
) -> Result<McpPrincipal, ServerError> {
    // 1. Verify JWS (embedded JWK mode)
    let verified =
        verify_jws(token).map_err(|e| ServerError::unauthorized(format!("Invalid token: {e}")))?;

    // 2. Parse payload (reuse EventsTokenPayload for standard claims)
    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {e}")))?;

    if payload.fluree_policy.is_some() {
        return Err(ServerError::unauthorized(
            "Policy delegation and controller credentials are not supported by MCP",
        ));
    }

    // 3. Validate standard claims (exp, iss matches signing key)
    // We don't require specific audience for MCP
    payload
        .validate(None, &verified.did, false)
        .map_err(|e| ServerError::unauthorized(e.to_string()))?;

    // 4. Check issuer trust
    if !mcp_auth.is_issuer_trusted(&payload.iss, events_auth) {
        return Err(ServerError::unauthorized("Untrusted issuer"));
    }

    // 5. Build principal
    let identity = payload.resolve_identity();
    let (read_all, read_ledgers) = crate::extract::read_scopes(&payload);
    Ok(McpPrincipal {
        issuer: Some(payload.iss),
        subject: payload.sub,
        identity,
        read_all,
        read_ledgers,
    })
}
