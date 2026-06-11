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
        deterministically across calls: re-run with the same `t`, an ORDER BY clause, and OFFSET \
        advanced by the previous result's `rowCount`. Omit to query the latest snapshot."
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
        description = "Execute a SPARQL SELECT query against a Fluree ledger (the ledger is set by \
        the `ledger` parameter, not a FROM clause; only SELECT is supported). Returns a compact \
        Agent JSON envelope: `schema` (per-variable datatypes inferred from the rows on this page), \
        `rows` (objects with native JSON values), `rowCount`, `t` (the snapshot's transaction \
        time), and `hasMore`. When `hasMore` is true the result was truncated to a byte budget — \
        fetch the next page by re-running this tool with the SAME `t` (to stay on one snapshot), \
        keeping your ORDER BY (for stable page boundaries), and advancing OFFSET by the returned \
        `rowCount` (the byte budget can return fewer rows than `LIMIT`). Use get_data_model first \
        to understand the schema before querying."
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
        // The Agent JSON envelope (and its byte budget) assume a SELECT solution table. ASK
        // returns a bare boolean, and CONSTRUCT/DESCRIBE return a JSON-LD graph that bypasses
        // `max_bytes` entirely — both contradict the advertised contract, so reject non-SELECT up
        // front. A genuine parse error (`ast` is None) falls through to execution, which surfaces
        // it as a normal query error.
        if let Some(ast) = fluree_db_sparql::parse_sparql(query).ast {
            if !matches!(ast.body, fluree_db_sparql::ast::QueryBody::Select(_)) {
                return Err(fluree_db_api::ApiError::http(
                    400,
                    "sparql_query supports SELECT queries only; ASK, CONSTRUCT, DESCRIBE, and \
                     UPDATE are not supported by this tool",
                ));
            }
        }

        // No AgentJsonContext: the ledger (and optional `t`) fully scope a single-ledger query,
        // so `t` is always included and the formatter's FROM-based resume / multi-ledger advice
        // — which target the connection-scoped path and don't round-trip through this
        // ledger-scoped tool — never apply. Pagination guidance is added by `annotate_pagination`.
        let config = fluree_db_api::FormatterConfig::agent_json().with_max_bytes(max_bytes);
        let state = self.state.clone();
        let ledger = ledger.to_string();
        let query = query.to_string();
        let identity = identity.map(str::to_string);
        let timeout_ms = state.config.mcp_query_timeout_ms;

        let mut envelope = crate::query_control::run_query_task(timeout_ms, move || async move {
            let envelope = match identity.as_deref() {
                Some(id) => {
                    let opts = fluree_db_api::QueryConnectionOptions {
                        identity: Some(id.to_string()),
                        ..Default::default()
                    };
                    let view = match t {
                        Some(t) => state.fluree.db_at_t_with_policy(&ledger, t, &opts).await?,
                        None => state.fluree.db_with_policy(&ledger, &opts).await?,
                    };
                    view.query(state.fluree.as_ref())
                        .sparql(&query)
                        .format(config)
                        .execution_options(crate::query_control::current_query_execution_options(
                            timeout_ms,
                        ))
                        .execute_formatted()
                        .await?
                }
                None => {
                    let graph = match t {
                        Some(t) => state
                            .fluree
                            .graph_at(&ledger, fluree_db_api::TimeSpec::AtT(t)),
                        None => state.fluree.graph(&ledger),
                    };
                    graph
                        .query()
                        .sparql(&query)
                        .format(config)
                        .execution_options(crate::query_control::current_query_execution_options(
                            timeout_ms,
                        ))
                        .execute_formatted()
                        .await?
                }
            };
            Ok(envelope)
        })
        .await
        .map_err(|e| match e {
            crate::error::ServerError::Api(api) => api,
            other => fluree_db_api::ApiError::Internal(other.to_string()),
        })?;

        Self::annotate_pagination(&mut envelope);
        Ok(envelope)
    }

    /// When the envelope was truncated, append this tool's pagination protocol to its `message`.
    ///
    /// The shared formatter only emits a generic "truncated to N bytes" notice; the
    /// `t` + `ORDER BY` + `OFFSET` protocol is specific to this ledger-scoped tool, so without
    /// this it would live only in the tool description and not in the result itself — exactly
    /// where an agent looks when it sees `hasMore`.
    fn annotate_pagination(envelope: &mut serde_json::Value) {
        if envelope.get("hasMore").and_then(serde_json::Value::as_bool) != Some(true) {
            return;
        }
        // Read `t` / `rowCount` before taking the mutable borrow below.
        let pin = match envelope.get("t").and_then(serde_json::Value::as_i64) {
            Some(t) => format!("t={t}"),
            None => "the same `t`".to_string(),
        };
        // Advance by the returned rowCount, not by LIMIT: the byte budget can trip before the
        // query's LIMIT, leaving rowCount < LIMIT, so advancing by LIMIT would skip dropped rows.
        let advance = match envelope.get("rowCount").and_then(serde_json::Value::as_u64) {
            Some(n) => format!("the {n} rows returned here"),
            None => "the rows returned here".to_string(),
        };
        let guidance = format!(
            " To page through the remaining rows deterministically, re-run sparql_query with \
             {pin} and the same ORDER BY (for stable page boundaries), then add {advance} to your \
             current OFFSET (advance OFFSET by rowCount, not by LIMIT, since the byte budget can \
             return fewer rows than requested)."
        );
        let Some(obj) = envelope.as_object_mut() else {
            return;
        };
        let message = match obj.get("message").and_then(serde_json::Value::as_str) {
            Some(existing) => format!("{existing}{guidance}"),
            None => guidance.trim_start().to_string(),
        };
        obj.insert("message".to_string(), serde_json::Value::String(message));
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
                   is true, paginate by re-running with the same `t`, an ORDER BY, and OFFSET \
                   advanced by the returned rowCount."
                    .to_string(),
            ),
        }
    }
}
