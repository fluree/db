//! MCP tool service for the developer memory layer.
//!
//! Provides `MemoryToolService` with tools for storing, recalling, updating,
//! and forgetting memories. Designed for IDE agent integration via stdio transport.

use crate::format::{
    format_context_paged, format_json, format_related_memories, format_status_text,
};
use crate::recall::RecallEngine;
use crate::secrets::SecretDetector;
use crate::store::MemoryStore;
use crate::types::{MemoryFilter, MemoryInput, MemoryKind, MemoryUpdate, Scope, Severity};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

/// Request parameters for the `memory_add` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryAddRequest {
    /// fact | decision | constraint
    #[schemars(description = "fact (true thing), decision (choice + why), or constraint (rule)")]
    pub kind: String,

    /// Content text (≤750 chars)
    #[schemars(description = "1-3 sentences capturing one insight; max 750 chars")]
    pub content: String,

    /// Tags (required; at least one)
    #[schemars(
        description = "REQUIRED. Lowercase topic tags, e.g. ['indexer', 'performance']. At least one tag is required — tags are the primary recall signal."
    )]
    pub tags: Vec<String>,

    /// File paths
    #[schemars(description = "File paths this memory relates to")]
    #[serde(default)]
    pub refs: Vec<String>,

    /// repo | user (default: repo)
    #[schemars(description = "repo (shared, default) or user (personal, gitignored)")]
    #[serde(default)]
    pub scope: Option<String>,

    /// Constraint severity
    #[schemars(description = "For kind=constraint: must, should, or prefer")]
    pub severity: Option<String>,

    /// Why this exists
    #[schemars(description = "Why this exists — works on any kind")]
    pub rationale: Option<String>,

    /// Alternatives considered
    #[schemars(description = "Alternatives considered (comma-separated)")]
    pub alternatives: Option<String>,
}

/// Request parameters for the `memory_recall` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryRecallRequest {
    /// Search query
    #[schemars(description = "Specific topic words from your task; not generic")]
    pub query: String,

    /// Result limit (default: 3)
    #[schemars(description = "Max results (default: 3)")]
    pub limit: Option<usize>,

    /// Pagination offset
    #[schemars(description = "Skip first N results for pagination")]
    #[serde(default)]
    pub offset: Option<usize>,

    /// Kind filter
    #[schemars(description = "Filter: fact, decision, or constraint")]
    pub kind: Option<String>,

    /// Tag filter
    #[schemars(description = "Only memories with these tags")]
    #[serde(default)]
    pub tags: Vec<String>,

    /// Scope filter
    #[schemars(description = "Filter: repo or user")]
    pub scope: Option<String>,
}

/// Request parameters for the `memory_update` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryUpdateRequest {
    /// Memory ID
    #[schemars(description = "ID of the memory to patch (e.g. 'mem:fact-01JDXYZ...')")]
    pub id: String,

    /// New content (omit to keep existing)
    #[schemars(description = "New content; omit to keep existing")]
    pub content: Option<String>,

    /// New tags (omit to keep, [] to clear)
    #[schemars(description = "Replace tags; omit to keep, [] to clear")]
    pub tags: Option<Vec<String>>,

    /// New refs (omit to keep, [] to clear)
    #[schemars(description = "Replace refs; omit to keep, [] to clear")]
    pub refs: Option<Vec<String>>,

    /// New rationale
    #[schemars(description = "Replace rationale; omit to keep existing")]
    pub rationale: Option<String>,

    /// New alternatives
    #[schemars(description = "Replace alternatives; omit to keep existing")]
    pub alternatives: Option<String>,
}

/// Request parameters for the `memory_forget` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryForgetRequest {
    /// Memory ID
    #[schemars(description = "ID of the memory to delete (e.g. 'mem:fact-01JDXYZ...')")]
    pub id: String,
}

/// Empty request parameters for `memory_status` (no inputs needed).
///
/// Exists to ensure rmcp generates a valid `{"type": "object"}` JSON Schema
/// for the tool's `inputSchema`. An empty schema `{}` causes some MCP clients
/// (including Claude Code) to fail tool registration.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryStatusRequest {}

/// Request parameters for the `kg_query` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct KgQueryRequest {
    /// SPARQL query
    #[schemars(
        description = "SPARQL SELECT against the memory graph; prefix mem: → https://ns.flur.ee/memory#"
    )]
    pub query: String,
}

/// MCP tool service for Fluree developer memory.
///
/// Provides tools for:
/// - `memory_add`: Store a new memory (fact, decision, constraint)
/// - `memory_recall`: Search and retrieve relevant memories
/// - `memory_update`: Patch an existing memory in place
/// - `memory_forget`: Delete a memory
/// - `memory_status`: Show memory store status
/// - `kg_query`: Execute raw SPARQL queries against the memory graph
#[derive(Clone)]
pub struct MemoryToolService {
    store: std::sync::Arc<MemoryStore>,
    tool_router: ToolRouter<MemoryToolService>,
}

#[tool_router]
impl MemoryToolService {
    /// Create a new MemoryToolService wrapping a MemoryStore.
    pub fn new(store: MemoryStore) -> Self {
        Self {
            store: std::sync::Arc::new(store),
            tool_router: Self::tool_router(),
        }
    }

    /// Store a new memory (fact, decision, or constraint).
    ///
    /// Memories persist across sessions and are used to maintain project context.
    /// Secrets (API keys, passwords, tokens) are automatically detected and redacted.
    #[tool(
        description = "Store ONE insight that persists across sessions. REQUIRED fields: kind, content, tags (at least one). Content is capped at 750 chars — aim for 1-3 sentences. One insight per memory; use multiple memory_add calls for multiple insights. Put file paths in `refs`, not embedded in content. Use `rationale` to explain why. DO store: invariants, gotchas, design decisions, rules. DO NOT store: implementation walkthroughs, architecture summaries, session progress, plans, or anything grep/git-log could answer. Secrets are auto-redacted."
    )]
    async fn memory_add(
        &self,
        Parameters(req): Parameters<MemoryAddRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Auto-initialize if needed
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        let kind = MemoryKind::parse(&req.kind).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!(
                    "Invalid memory kind '{}'. Valid: fact, decision, constraint",
                    req.kind
                ),
                None,
            )
        })?;

        // Tags are required — they are the primary recall signal.
        if req.tags.is_empty() {
            return Ok(CallToolResult::error(vec![Content::text(
                "At least one tag is required. Tags are the primary recall signal \
                 — memories without tags are much harder to surface later. \
                 Add descriptive lowercase topic tags like 'indexer', 'query', \
                 'error-handling', or module/feature names."
                    .to_string(),
            )]));
        }

        let severity = req
            .severity
            .as_deref()
            .map(|s| {
                Severity::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid severity '{s}'. Valid: must, should, prefer"),
                        None,
                    )
                })
            })
            .transpose()?;

        // Check for and redact secrets
        let (content, redacted) = if SecretDetector::has_secrets(&req.content) {
            (SecretDetector::redact(&req.content), true)
        } else {
            (req.content, false)
        };

        // Enforce content length limit
        if content.len() > crate::types::MAX_CONTENT_LENGTH {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Memory content is {} characters (max {}). Shorten to a single insight per memory \
                 and add multiple memories for multiple insights. Lead with the key point, use \
                 rationale/alternatives fields for supporting detail, and use refs for file paths \
                 instead of embedding them in content.",
                content.len(),
                crate::types::MAX_CONTENT_LENGTH,
            ))]));
        }

        let scope = req
            .scope
            .as_deref()
            .map(|s| {
                Scope::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid scope '{s}'. Valid: repo, user"),
                        None,
                    )
                })
            })
            .transpose()?
            .unwrap_or_default();

        let branch = crate::detect_git_branch_from(self.store.memory_dir());

        // Capture preview and recall query before content is moved into MemoryInput
        let preview: String = content
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(40)
            .collect();
        let ellipsis = if content.len() > preview.len() {
            "..."
        } else {
            ""
        };
        let preview_line = format!("{preview}{ellipsis}");
        let recall_query = content.clone();

        let input = MemoryInput {
            kind,
            content,
            tags: req.tags,
            scope,
            severity,
            artifact_refs: req.refs,
            branch,
            rationale: req.rationale,
            alternatives: req.alternatives,
        };

        match self.store.add(input).await {
            Ok(id) => {
                info!(id = %id, kind = %req.kind, "Memory added");

                let mut text = format!("Stored memory: {} | {}: {}", id, req.kind, preview_line);

                if redacted {
                    text.push_str("\n\nWarning: Secrets were detected and automatically redacted.");
                }

                // Surface related memories for housekeeping
                if let Some(related_text) = self.find_related_memories(&id, &recall_query).await {
                    text.push_str(&related_text);
                }

                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to store memory");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to store memory: {e}"
                ))]))
            }
        }
    }

    /// Search and retrieve relevant memories for a query.
    ///
    /// Returns memories ranked by relevance, formatted as XML context blocks
    /// suitable for LLM consumption.
    #[tool(
        description = "BM25 keyword search over stored memories. Call at the start of non-trivial tasks. Use specific topic words from the task ('error handling', 'index pipeline', 'sparql federation') — generic queries return nothing. If unsure what's stored, call memory_status first."
    )]
    async fn memory_recall(
        &self,
        Parameters(req): Parameters<MemoryRecallRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        let kind_filter = req
            .kind
            .as_deref()
            .map(|s| {
                MemoryKind::parse(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(format!("Invalid memory kind '{s}'"), None)
                })
            })
            .transpose()?;

        let scope_filter = req
            .scope
            .as_deref()
            .map(|s| {
                Scope::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid scope '{s}'. Valid: repo, user"),
                        None,
                    )
                })
            })
            .transpose()?;

        let filter = MemoryFilter {
            kind: kind_filter,
            tags: req.tags,
            branch: None,
            scope: scope_filter,
        };

        let limit = req.limit.unwrap_or(3);
        let offset = req.offset.unwrap_or(0);
        // Only apply smart score-based filtering when the caller did not explicitly
        // request a specific limit or a non-zero offset.  Explicit pagination means
        // the caller already knows what it wants.
        let use_smart_filter = req.limit.is_none() && offset == 0;
        // Fetch one extra beyond what we need so we can always report the score of
        // the first result we did *not* return.
        let fetch_n = offset + limit + 1;

        debug!(query = %req.query, limit = limit, offset = offset, use_smart_filter, "Memory recall request");

        // BM25 fulltext search for content relevance
        let bm25_hits = match self.store.recall_fulltext(&req.query, fetch_n).await {
            Ok(hits) => {
                debug!(hits = hits.len(), "BM25 search complete");
                hits
            }
            Err(e) => {
                error!(error = %e, query = %req.query, "BM25 search failed");
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to search memories: {e}"
                ))]));
            }
        };

        // Load full memory objects for metadata re-ranking
        match self.store.current_memories(&filter).await {
            Ok(all) => {
                debug!(
                    total_current = all.len(),
                    "Loaded current memories for re-ranking"
                );
                let branch = crate::detect_git_branch_from(self.store.memory_dir());
                let scored = if bm25_hits.is_empty() {
                    RecallEngine::recall_metadata_only(
                        &req.query,
                        &all,
                        branch.as_deref(),
                        Some(fetch_n),
                    )
                } else {
                    RecallEngine::rerank(&req.query, &bm25_hits, &all, branch.as_deref())
                };

                // Apply offset then take up to limit+1 (the extra is the peek-ahead).
                let mut after_offset: Vec<_> = scored.into_iter().skip(offset).collect();

                if after_offset.is_empty() {
                    let msg = if offset > 0 {
                        format!("No more memories at offset={offset}.")
                    } else {
                        "No relevant memories found. Tip: use specific topic keywords, not generic terms.".to_string()
                    };
                    debug!(query = %req.query, offset = offset, "No relevant memories found");
                    return Ok(CallToolResult::success(vec![Content::text(msg)]));
                }

                // The peek item lives at index `limit` (if it exists).
                let peek_score = after_offset.get(limit).map(|s| s.score);

                // Truncate to at most `limit` results.
                after_offset.truncate(limit);
                let mut page = after_offset;

                // Smart score-based trimming: when the caller did not request an
                // explicit limit, drop results whose score is less than 50% of the
                // top score.  This keeps only clearly relevant hits and avoids
                // feeding the LLM noisy low-confidence memories.
                let next_score = if use_smart_filter && !page.is_empty() {
                    let top = page[0].score;
                    if top > 0.0 {
                        let threshold = top * 0.5;
                        let keep = page
                            .iter()
                            .take_while(|s| s.score >= threshold)
                            .count()
                            .max(1); // always return at least 1 result
                        if keep < page.len() {
                            let trimmed_next = page[keep].score;
                            page.truncate(keep);
                            // Report the tighter of score-trim boundary vs peek-ahead.
                            Some(peek_score.map_or(trimmed_next, |p| p.min(trimmed_next)))
                        } else {
                            peek_score
                        }
                    } else {
                        peek_score
                    }
                } else {
                    peek_score
                };

                let has_more = next_score.is_some();
                info!(query = %req.query, shown = page.len(), offset = offset, has_more = has_more, "Memory recall complete");
                let context =
                    format_context_paged(&page, offset, req.limit, all.len(), has_more, next_score);
                Ok(CallToolResult::success(vec![Content::text(context)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to load current memories");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to recall memories: {e}"
                ))]))
            }
        }
    }

    /// Update an existing memory in place.
    ///
    /// Modifies the memory with the given ID, changing only the fields you provide.
    /// The ID stays the same. History is tracked via git.
    #[tool(
        description = "Patch an existing memory in place. Pass only the fields you want to change (content, tags, refs, rationale, alternatives) — omitted fields stay as-is. The memory keeps its ID. Git tracks history."
    )]
    async fn memory_update(
        &self,
        Parameters(req): Parameters<MemoryUpdateRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        // Check for secrets in new content
        let content = req.content.map(|c| {
            if SecretDetector::has_secrets(&c) {
                SecretDetector::redact(&c)
            } else {
                c
            }
        });

        let update = MemoryUpdate {
            content,
            tags: req.tags,
            severity: None,
            artifact_refs: req.refs,
            rationale: req.rationale,
            alternatives: req.alternatives,
        };

        match self.store.update(&req.id, update).await {
            Ok(id) => {
                let mut text = format!("Updated: {id}");
                if let Ok(Some(mem)) = self.store.get(&id).await {
                    text = serde_json::to_string_pretty(&format_json(&mem)).unwrap_or(text);
                }
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to update memory: {e}"
            ))])),
        }
    }

    /// Delete a memory permanently.
    #[tool(
        description = "Permanently delete a memory by ID. Use for incorrect or obsolete entries. Prefer memory_update for evolving information."
    )]
    async fn memory_forget(
        &self,
        Parameters(req): Parameters<MemoryForgetRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        match self.store.forget(&req.id).await {
            Ok(()) => Ok(CallToolResult::success(vec![Content::text(format!(
                "Forgotten: {}",
                req.id
            ))])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to forget memory: {e}"
            ))])),
        }
    }

    /// Show the memory store status summary.
    #[tool(
        description = "Total counts and previews of the most recent memories. Call before memory_recall if you don't yet know what topics/keywords the store contains."
    )]
    async fn memory_status(
        &self,
        Parameters(_req): Parameters<MemoryStatusRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        match self.store.status().await {
            Ok(status) => {
                debug!(total = status.total_memories, "Memory status requested");
                let text = format_status_text(&status);
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to get memory status");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get memory status: {e}"
                ))]))
            }
        }
    }

    /// Execute a raw SPARQL query against the memory knowledge graph.
    #[tool(
        description = "Escape hatch: raw SPARQL against the memory graph (prefix `mem:` → https://ns.flur.ee/memory#). Prefer memory_recall for normal use. Classes: mem:Fact, mem:Decision, mem:Constraint."
    )]
    async fn kg_query(
        &self,
        Parameters(req): Parameters<KgQueryRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {e}"
            ))]));
        }

        debug!(query = %req.query, "SPARQL query requested");
        match self.store.query_sparql(&req.query).await {
            Ok(result) => {
                let text =
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, query = %req.query, "SPARQL query failed");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "SPARQL query error: {e}"
                ))]))
            }
        }
    }
}

impl MemoryToolService {
    /// Find existing memories related to a just-stored memory.
    ///
    /// Returns `None` if no related memories score above threshold.
    async fn find_related_memories(&self, new_id: &str, content: &str) -> Option<String> {
        // fetch_n = 2 (LIMIT) + 2: one extra for the self-match that gets
        // filtered out, one extra for the score cliff peek-ahead.
        let bm25_hits = self.store.recall_fulltext(content, 4).await.ok()?;
        let filter = MemoryFilter::default();
        let all = self.store.current_memories(&filter).await.ok()?;
        let branch = crate::detect_git_branch_from(self.store.memory_dir());

        let candidates =
            RecallEngine::find_related(new_id, content, &bm25_hits, &all, branch.as_deref());

        if candidates.is_empty() {
            return None;
        }

        debug!(new_id = %new_id, related = candidates.len(), "Found related memories after add");
        Some(format_related_memories(&candidates))
    }

    /// Auto-initialize the memory store if not already initialized.
    async fn ensure_initialized(&self) -> std::result::Result<(), String> {
        if !self.store.is_initialized().await.unwrap_or(false) {
            info!("Memory store not initialized, initializing");
            self.store.initialize().await.map_err(|e| {
                error!(error = %e, "Memory initialization failed");
                format!("initialization failed: {e}")
            })?;
            info!("Memory store initialized");
        }
        // Rebuild ledger from .ttl files if they've changed (e.g. git pull)
        self.store.ensure_synced().await.map_err(|e| {
            error!(error = %e, "File sync failed");
            format!("file sync failed: {e}")
        })?;
        Ok(())
    }
}

#[tool_handler]
impl ServerHandler for MemoryToolService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "fluree-memory".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("Fluree Developer Memory".to_string()),
                icons: None,
                website_url: Some("https://flur.ee".to_string()),
            },
            instructions: Some(
                "Fluree Developer Memory — persistent project knowledge across sessions.\n\n\
                 WHEN TO USE:\n\
                 - Start of a non-trivial task: call memory_recall with specific keywords \
                   from the task (e.g. 'indexer leaflet cache', 'sparql federation'). \
                   Use memory_status first if you don't know what topics are stored.\n\
                 - End of a task, when you learned something non-obvious: call memory_add. \
                   See that tool's description for what qualifies.\n\n\
                 RECALL QUERIES must use specific topic words. Generic queries \
                 ('all', 'everything', 'memory') return nothing — BM25 needs content terms."
                    .to_string(),
            ),
        }
    }
}
