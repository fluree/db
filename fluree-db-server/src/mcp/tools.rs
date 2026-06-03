//! MCP tool service implementation
//!
//! Provides the `FlureeToolService` struct that implements the rmcp `ServerHandler`
//! trait with tools for SPARQL query execution and data model retrieval.

use crate::mcp::auth::McpPrincipal;
use crate::state::AppState;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::format::format_data_model_markdown;

/// Extract McpPrincipal from the MCP request context.
///
/// The auth middleware stores the principal in HTTP request extensions,
/// which are forwarded to the MCP context via `http::request::Parts`.
fn extract_principal(context: &rmcp::service::RequestContext<RoleServer>) -> Option<McpPrincipal> {
    context
        .extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<McpPrincipal>())
        .cloned()
}

/// Request parameters for SPARQL query tool
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SparqlQueryRequest {
    /// The ledger alias to query (e.g., "mydb" or "mydb:main")
    #[schemars(description = "The ledger alias to query (e.g., 'mydb' or 'mydb:main')")]
    pub ledger: String,

    /// The SPARQL query to execute
    #[schemars(description = "The SPARQL query to execute. Must be a valid SPARQL SELECT query.")]
    pub query: String,

    /// Optional transaction time (`t`) to pin the query to a specific snapshot.
    #[serde(default)]
    #[schemars(
        description = "Optional transaction time `t` to pin the query to a specific historical \
        snapshot. Pass the `t` value returned in a previous result's envelope to paginate \
        deterministically across calls (same `t` plus an increased SPARQL OFFSET/LIMIT). Omit to \
        query the latest snapshot."
    )]
    pub t: Option<i64>,
}

/// Request parameters for get_data_model tool
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetDataModelRequest {
    /// The ledger alias to get the data model for
    #[schemars(
        description = "The ledger alias to get the data model for (e.g., 'mydb' or 'mydb:main')"
    )]
    pub ledger: String,
}

/// MCP tool service for Fluree DB
///
/// Provides tools for:
/// - `sparql_query`: Execute SPARQL queries against a ledger
/// - `get_data_model`: Get the data model (classes, properties, statistics) as markdown
#[derive(Clone)]
pub struct FlureeToolService {
    state: Arc<AppState>,
    tool_router: ToolRouter<FlureeToolService>,
}

#[tool_router]
impl FlureeToolService {
    /// Create a new FlureeToolService with access to server state
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }

    /// Execute a SPARQL query against a Fluree ledger
    #[tool(
        description = "Execute a SPARQL SELECT query against a Fluree ledger. Returns a compact \
        Agent JSON envelope: `schema` (per-variable datatypes, declared once), `rows` (objects \
        with native JSON values), `rowCount`, `t` (the snapshot's transaction time), `iso` (the \
        snapshot timestamp), and `hasMore`. When `hasMore` is true the result was truncated to a \
        byte budget — fetch the next page by re-running this tool with the SAME `t` value (for a \
        consistent snapshot) plus an increased SPARQL OFFSET/LIMIT. Use get_data_model first to \
        understand the schema before querying."
    )]
    async fn sparql_query(
        &self,
        Parameters(req): Parameters<SparqlQueryRequest>,
        context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let start = std::time::Instant::now();

        // Extract identity from MCP principal for policy enforcement
        let principal = extract_principal(&context);
        let identity = principal.as_ref().and_then(|p| p.identity.as_deref());

        tracing::info!(
            ledger = %req.ledger,
            query_len = req.query.len(),
            identity = ?identity,
            t = ?req.t,
            "MCP sparql_query tool invoked"
        );

        let max_bytes = self.state.config.mcp_agent_json_max_bytes;
        let result = self
            .execute_sparql_agent_json(&req.ledger, &req.query, identity, req.t, max_bytes)
            .await;

        match result {
            Ok(envelope) => {
                let elapsed = start.elapsed();
                tracing::info!(
                    ledger = %req.ledger,
                    elapsed_ms = elapsed.as_millis(),
                    "MCP sparql_query succeeded"
                );

                // Compact serialization — the formatter already applied the byte budget,
                // so pretty-printing here would only inflate the token cost.
                let text =
                    serde_json::to_string(&envelope).unwrap_or_else(|_| envelope.to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                let elapsed = start.elapsed();
                tracing::warn!(
                    ledger = %req.ledger,
                    elapsed_ms = elapsed.as_millis(),
                    error = %e,
                    "MCP sparql_query failed"
                );

                // Return error as tool error content (learnable by LLM)
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "SPARQL query error: {e}"
                ))]))
            }
        }
    }

    /// Execute a SPARQL query and format the result as an Agent JSON envelope.
    ///
    /// Extracted from [`sparql_query`](Self::sparql_query) so it can be exercised in tests
    /// without an rmcp `RequestContext`.
    ///
    /// - `identity` — when `Some`, the query runs under that identity's policy.
    /// - `t` — when `Some`, the query runs against that historical snapshot, which is how
    ///   callers paginate deterministically (re-run with the previous result's `t`).
    /// - `max_bytes` — byte budget; rows beyond it are dropped and the envelope reports
    ///   `hasMore: true`.
    pub async fn execute_sparql_agent_json(
        &self,
        ledger: &str,
        query: &str,
        identity: Option<&str>,
        t: Option<i64>,
        max_bytes: usize,
    ) -> Result<serde_json::Value, fluree_db_api::ApiError> {
        let agent_ctx = fluree_db_api::AgentJsonContext {
            // Leave `sparql_text` unset: the formatter's resume query targets the
            // connection-scoped `FROM <ledger@t:N>` path, which does not round-trip through
            // this ledger-scoped tool. MCP pagination uses the `t` request field instead.
            sparql_text: None,
            from_count: fluree_db_api::sparql_from_count(query),
            iso_timestamp: Some(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            ),
            ..Default::default()
        };
        let config = fluree_db_api::FormatterConfig::agent_json()
            .with_max_bytes(max_bytes)
            .with_agent_json_context(agent_ctx);

        match identity {
            Some(id) => {
                let opts = fluree_db_api::QueryConnectionOptions {
                    identity: Some(id.to_string()),
                    ..Default::default()
                };
                let view = match t {
                    Some(t) => {
                        self.state
                            .fluree
                            .db_at_t_with_policy(ledger, t, &opts)
                            .await?
                    }
                    None => self.state.fluree.db_with_policy(ledger, &opts).await?,
                };
                view.query(self.state.fluree.as_ref())
                    .sparql(query)
                    .format(config)
                    .execute_formatted()
                    .await
            }
            None => {
                let graph = match t {
                    Some(t) => self
                        .state
                        .fluree
                        .graph_at(ledger, fluree_db_api::TimeSpec::AtT(t)),
                    None => self.state.fluree.graph(ledger),
                };
                graph
                    .query()
                    .sparql(query)
                    .format(config)
                    .execute_formatted()
                    .await
            }
        }
    }

    /// Get the data model (schema) for a Fluree ledger
    #[tool(
        description = "Get the data model (schema) of a Fluree ledger as markdown. Returns classes, properties, instance counts, and statistics. CRITICAL: Use this before sparql_query to understand what data exists."
    )]
    async fn get_data_model(
        &self,
        Parameters(req): Parameters<GetDataModelRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let start = std::time::Instant::now();

        tracing::info!(
            ledger = %req.ledger,
            "MCP get_data_model tool invoked"
        );

        // Build comprehensive ledger info (works with both file and proxy storage)
        let info = self
            .state
            .fluree
            .ledger_info(&req.ledger)
            .execute()
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "Failed to build ledger info");
                rmcp::ErrorData::internal_error(format!("Failed to load ledger: {e}"), None)
            })?;

        // Format as markdown for LLM consumption
        let markdown = format_data_model_markdown(&req.ledger, &info);

        let elapsed = start.elapsed();
        tracing::info!(
            ledger = %req.ledger,
            elapsed_ms = elapsed.as_millis(),
            markdown_len = markdown.len(),
            "MCP get_data_model succeeded"
        );

        Ok(CallToolResult::success(vec![Content::text(markdown)]))
    }
}

#[tool_handler]
impl ServerHandler for FlureeToolService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "fluree-db".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("Fluree DB".to_string()),
                icons: None,
                website_url: Some("https://flur.ee".to_string()),
            },
            instructions: Some(
                "Fluree DB MCP server. Available tools:\n\
                 - get_data_model: Get the schema (classes, properties, counts) for a ledger. \
                   Use this FIRST to understand what data exists.\n\
                 - sparql_query: Execute a SPARQL SELECT query against a ledger. Returns a \
                   compact Agent JSON envelope (schema, rows, rowCount, t, hasMore). When hasMore \
                   is true, paginate by re-running with the same `t` and an increased OFFSET/LIMIT."
                    .to_string(),
            ),
        }
    }
}
