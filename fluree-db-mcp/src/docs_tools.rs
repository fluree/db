//! The `docs` toolset: `docs_search` / `docs_get` / `docs_examples` / `docs_tree`
//! over the embedded, version-pinned corpus in `fluree-db-docs`.
//!
//! Read-only over static content (safe to auto-allowlist). The tool bodies just
//! delegate to [`fluree_db_docs::index`]; this module is the rmcp surface.

use crate::service::{json_result, FlureeMcpService};
use fluree_db_docs::index;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_router, RoleServer};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Request parameters for the `docs_search` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DocsSearchRequest {
    #[schemars(
        description = "Topic keywords, e.g. 'property paths', 'policy enforcement', 'time travel'"
    )]
    pub query: String,
    #[schemars(description = "Max hits (default 10)")]
    pub limit: Option<usize>,
}

/// Request parameters for the `docs_get` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DocsGetRequest {
    #[schemars(
        description = "Book-relative page path, e.g. 'query/sparql.md' (from a docs_search hit)"
    )]
    pub path: String,
    #[schemars(
        description = "Optional heading anchor to return just that section, e.g. 'property-paths'"
    )]
    pub anchor: Option<String>,
}

/// Request parameters for the `docs_examples` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DocsExamplesRequest {
    #[schemars(
        description = "Topic keywords to pull code examples for, e.g. 'insert transaction'"
    )]
    pub query: String,
    #[schemars(description = "Optional language filter, e.g. 'json', 'sparql', 'rust'")]
    pub lang: Option<String>,
    #[schemars(description = "Max examples (default 10)")]
    pub limit: Option<usize>,
}

/// Empty request parameters for `docs_tree` (no inputs needed). Present so rmcp
/// emits a valid `{"type": "object"}` input schema — some MCP clients reject a
/// bare `{}`.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DocsTreeRequest {}

#[tool_router(router = docs_router, vis = "pub(crate)")]
impl FlureeMcpService {
    /// Ranked, section-level documentation search.
    #[tool(
        description = "Search the embedded, version-pinned Fluree documentation. Returns ranked section-level hits (path, heading anchor, title, snippet). Results are pinned to this binary's version, so they match the Fluree you're building against. Use specific topic words ('property paths', 'policy', 'vector index'). Follow up with docs_get for the full text."
    )]
    async fn docs_search(
        &self,
        Parameters(req): Parameters<DocsSearchRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let hits = index().search(&req.query, req.limit.unwrap_or(10));
        json_result(&hits)
    }

    /// Fetch a documentation page, or a single heading-scoped slice of one.
    #[tool(
        description = "Fetch a documentation page as markdown by its path (e.g. 'query/sparql.md'), or just one section by passing its heading anchor (e.g. 'property-paths'). Get the path/anchor from docs_search."
    )]
    async fn docs_get(
        &self,
        Parameters(req): Parameters<DocsGetRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        match index().get(&req.path, req.anchor.as_deref()) {
            Some(page) => json_result(&page),
            None => Ok(CallToolResult::error(vec![Content::text(format!(
                "No docs page found for '{}'. Use docs_search to find valid paths.",
                req.path
            ))])),
        }
    }

    /// Extract code examples for a feature.
    #[tool(
        description = "Extract code examples (fenced code blocks) from the docs sections most relevant to a query. Optionally filter by language. Often all you need to get the syntax right in one shot."
    )]
    async fn docs_examples(
        &self,
        Parameters(req): Parameters<DocsExamplesRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let examples = index().examples(&req.query, req.lang.as_deref(), req.limit.unwrap_or(10));
        json_result(&examples)
    }

    /// Browse the documentation table of contents.
    #[tool(
        description = "Return the documentation table of contents (the curated SUMMARY tree of titles and page paths). Use for orientation — to see what topics exist and grab a page path to feed docs_get — instead of inferring structure from search results."
    )]
    async fn docs_tree(
        &self,
        Parameters(_req): Parameters<DocsTreeRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        json_result(&index().tree())
    }
}

/// Per-toolset guidance fragment, merged into the unified server's `get_info`
/// instructions when the `docs` toolset is enabled.
pub(crate) const DOCS_INSTRUCTIONS: &str = "DOCS (version-pinned, read-only):\n\
     - Before writing or debugging any Fluree query, transaction, policy, or config, call \
       docs_search with specific topic words (e.g. 'property paths', 'optional patterns', \
       'time travel', 'policy enforcement').\n\
     - docs_get reads a full page or a single section once docs_search points you at it.\n\
     - docs_examples pulls ready-to-adapt code blocks.\n\
     These results are pinned to this binary's version — trust them over training-data recall.";
