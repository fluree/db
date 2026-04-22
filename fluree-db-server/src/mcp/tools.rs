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
        description = "Execute a SPARQL query against a Fluree ledger. Returns JSON results as tuples matching SELECT variable order. Use get_data_model first to understand the schema before querying."
    )]
    async fn sparql_query(
        &self,
        Parameters(req): Parameters<SparqlQueryRequest>,
        context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Result truncation limits to avoid overloading LLM context
        const MAX_ROWS: usize = 100;
        const MAX_CHARS: usize = 50000;

        let start = std::time::Instant::now();

        // Extract identity from MCP principal for policy enforcement
        let principal = extract_principal(&context);
        let identity = principal.as_ref().and_then(|p| p.identity.as_deref());

        tracing::info!(
            ledger = %req.ledger,
            query_len = req.query.len(),
            identity = ?identity,
            "MCP sparql_query tool invoked"
        );

        // Execute SPARQL query with identity-based policy (if identity is present)
        let to_err =
            |e: fluree_db_api::ApiError| rmcp::ErrorData::internal_error(e.to_string(), None);
        let result = match identity {
            Some(id) => {
                let opts = fluree_db_api::QueryConnectionOptions {
                    identity: Some(id.to_string()),
                    ..Default::default()
                };
                let view = self
                    .state
                    .fluree
                    .db_with_policy(&req.ledger, &opts)
                    .await
                    .map_err(to_err)?;
                view.query(self.state.fluree.as_ref())
                    .sparql(&req.query)
                    .execute_formatted()
                    .await
            }
            None => {
                self.state
                    .fluree
                    .graph(&req.ledger)
                    .query()
                    .sparql(&req.query)
                    .execute_formatted()
                    .await
            }
        };

        match result {
            Ok(json_result) => {
                let elapsed = start.elapsed();
                tracing::info!(
                    ledger = %req.ledger,
                    elapsed_ms = elapsed.as_millis(),
                    "MCP sparql_query succeeded"
                );

                // Apply truncation if result is an array
                let (output, truncated, total_rows, returned_rows) =
                    if let Some(arr) = json_result.as_array() {
                        let total = arr.len();
                        let row_truncated = total > MAX_ROWS;
                        let limited: Vec<_> = arr.iter().take(MAX_ROWS).collect();
                        let limited_len = limited.len();

                        // Check character limit and reduce further if needed
                        let mut result_json =
                            serde_json::to_string_pretty(&limited).unwrap_or_default();
                        let mut final_slice = limited;

                        while result_json.len() > MAX_CHARS && final_slice.len() > 1 {
                            let new_len = (final_slice.len() as f64 * 0.8) as usize;
                            final_slice = final_slice.into_iter().take(new_len).collect();
                            result_json =
                                serde_json::to_string_pretty(&final_slice).unwrap_or_default();
                        }

                        let char_truncated = final_slice.len() < limited_len;

                        (
                            result_json,
                            row_truncated || char_truncated,
                            total,
                            final_slice.len(),
                        )
                    } else {
                        (
                            serde_json::to_string_pretty(&json_result)
                                .unwrap_or_else(|_| json_result.to_string()),
                            false,
                            0,
                            0,
                        )
                    };

                // Add truncation notice if results were limited
                let text = if truncated {
                    format!(
                        "{output}\n\n---\nNote: Results truncated. Showing {returned_rows} of {total_rows} total rows. \
                         Use SPARQL LIMIT and OFFSET clauses to paginate through results."
                    )
                } else {
                    output
                };

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
                 - sparql_query: Execute a SPARQL SELECT query against a ledger. \
                   Returns JSON-LD results."
                    .to_string(),
            ),
        }
    }
}
