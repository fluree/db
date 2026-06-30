//! The unified `FlureeMcpService` — one rmcp service exposing a runtime-selected
//! subset of toolsets.
//!
//! Each toolset contributes its tools through a named router on this same type
//! (`memory_router` in [`crate::memory_tools`], `docs_router` in
//! [`crate::docs_tools`]). `new` composes only the enabled toolsets' routers
//! into `combined_router`, which `#[tool_handler]` dispatches against — so
//! `tools/list` and `tools/call` reflect exactly the selected set.

use crate::docs_tools::DOCS_INSTRUCTIONS;
use crate::memory_tools::MEMORY_INSTRUCTIONS;
use crate::toolset::Toolset;
use fluree_db_memory::MemoryStore;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::*;
use rmcp::{tool_handler, ServerHandler, ServiceExt};
use serde::Serialize;
use std::sync::Arc;

/// MCP service for Fluree, exposing a selected set of [`Toolset`]s over one
/// server. Construct with [`FlureeMcpService::new`].
#[derive(Clone)]
pub struct FlureeMcpService {
    /// Present only when the `memory` toolset is enabled. The memory tools guard
    /// on this; the docs toolset needs no state.
    memory: Option<Arc<MemoryStore>>,
    /// The enabled toolsets, in canonical order — drives `get_info` instructions.
    toolsets: Vec<Toolset>,
    /// Tools from only the enabled toolsets; what `#[tool_handler]` serves.
    combined_router: ToolRouter<FlureeMcpService>,
}

impl FlureeMcpService {
    /// Build a service exposing `toolsets`. `memory` must be `Some` whenever
    /// `toolsets` contains [`Toolset::Memory`]; pass `None` otherwise (the docs
    /// toolset is stateless).
    pub fn new(toolsets: &[Toolset], memory: Option<MemoryStore>) -> Self {
        let mut combined_router = ToolRouter::new();
        for ts in toolsets {
            match ts {
                Toolset::Memory => combined_router += Self::memory_router(),
                Toolset::Docs => combined_router += Self::docs_router(),
            }
        }
        Self {
            memory: memory.map(Arc::new),
            toolsets: toolsets.to_vec(),
            combined_router,
        }
    }

    /// The memory store, if the `memory` toolset is enabled.
    pub(crate) fn memory(&self) -> Option<&MemoryStore> {
        self.memory.as_deref()
    }
}

/// Serialize a payload to pretty JSON as a successful tool result. Shared by the
/// docs tools.
pub(crate) fn json_result<T: Serialize>(value: &T) -> Result<CallToolResult, rmcp::ErrorData> {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| "[]".to_string());
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[tool_handler(router = self.combined_router)]
impl ServerHandler for FlureeMcpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "fluree".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("Fluree".to_string()),
                icons: None,
                website_url: Some("https://flur.ee".to_string()),
            },
            instructions: Some(self.instructions()),
        }
    }
}

impl FlureeMcpService {
    /// Compose the server instructions from the enabled toolsets' fragments.
    fn instructions(&self) -> String {
        let mut out = String::from(
            "Fluree MCP — one server, selectable toolsets. The tools available to you depend on \
             which toolsets this server was started with (below).\n\n",
        );
        for ts in &self.toolsets {
            match ts {
                Toolset::Memory => out.push_str(MEMORY_INSTRUCTIONS),
                Toolset::Docs => out.push_str(DOCS_INSTRUCTIONS),
            }
            out.push_str("\n\n");
        }
        out.truncate(out.trim_end().len());
        out
    }
}

/// Serve `service` over stdio (stdin/stdout JSON-RPC) until the client
/// disconnects. The caller must not have written to stdout/stderr.
pub async fn serve_stdio(service: FlureeMcpService) -> Result<(), String> {
    let server = service
        .serve(rmcp::transport::io::stdio())
        .await
        .map_err(|e| format!("failed to start MCP server: {e}"))?;
    server
        .waiting()
        .await
        .map_err(|e| format!("MCP server error: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_names(toolsets: &[Toolset]) -> Vec<String> {
        // Memory tools guard on the store, so for list-shape tests we can pass
        // None even when memory is enabled — list_all only reflects routing.
        let svc = FlureeMcpService::new(toolsets, None);
        let mut names: Vec<String> = svc
            .combined_router
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn docs_only_exposes_only_docs_tools() {
        let names = tool_names(&[Toolset::Docs]);
        assert_eq!(
            names,
            vec!["docs_examples", "docs_get", "docs_search", "docs_tree"]
        );
    }

    #[test]
    fn memory_only_exposes_only_memory_tools() {
        let names = tool_names(&[Toolset::Memory]);
        assert_eq!(
            names,
            vec![
                "kg_query",
                "memory_add",
                "memory_forget",
                "memory_recall",
                "memory_status",
                "memory_update",
            ]
        );
    }

    #[test]
    fn all_exposes_both_toolsets() {
        let names = tool_names(Toolset::ALL);
        assert_eq!(names.len(), 10);
        assert!(names.contains(&"docs_search".to_string()));
        assert!(names.contains(&"memory_add".to_string()));
    }

    #[test]
    fn instructions_mention_only_enabled_toolsets() {
        let docs = FlureeMcpService::new(&[Toolset::Docs], None).instructions();
        assert!(docs.contains("DOCS"));
        assert!(!docs.contains("MEMORY ("));

        let mem = FlureeMcpService::new(&[Toolset::Memory], None).instructions();
        assert!(mem.contains("MEMORY ("));
        assert!(!mem.contains("DOCS"));
    }
}
