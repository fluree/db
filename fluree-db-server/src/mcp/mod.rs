//! MCP (Model Context Protocol) service integration
//!
//! Provides MCP tools for Fluree DB access:
//! - `sparql_query`: Execute SPARQL queries
//! - `get_data_model`: Get ledger schema as markdown
//!
//! Authentication is via Bearer token (same format as events endpoint).

pub mod auth;
pub mod format;
pub mod tools;

use crate::state::AppState;
use axum::middleware;
use axum::Router;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use std::sync::Arc;
use tools::FlureeToolService;

/// Build the MCP router with authentication middleware.
///
/// Returns a Router that can be nested at `/mcp` in the main application.
/// All requests require a valid Bearer token from a trusted issuer.
/// The returned router has state `Arc<AppState>` and will receive state from the parent router.
pub fn build_mcp_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    // Create factory that produces FlureeToolService instances for each session
    let state_for_factory = state.clone();
    let factory = move || Ok(FlureeToolService::new(state_for_factory.clone()));

    // Create the MCP HTTP service with local session management
    let mcp_service = StreamableHttpService::new(
        factory,
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );

    // Build router with authentication middleware
    // The MCP service handles all HTTP methods for the MCP protocol
    // State is provided by the parent router via .with_state()
    Router::new()
        .nest_service("/", mcp_service)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::validate_mcp_token,
        ))
}
