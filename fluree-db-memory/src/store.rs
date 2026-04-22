use crate::error::{MemoryError, Result};
use crate::schema::{memory_schema_jsonld, memory_to_jsonld};
use crate::types::{
    Memory, MemoryFilter, MemoryInput, MemoryKind, MemoryPreview, MemoryStatus, MemoryUpdate, Scope,
};
use chrono::Utc;
use fluree_db_api::Fluree;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

const MEM_PREFIX: &str = "mem:";
const MEM_NAMESPACE: &str = "https://ns.flur.ee/memory#";

/// Expand compact `mem:` prefix IDs to full IRIs for SPARQL queries.
/// Passes through already-expanded IRIs unchanged.
fn expand_id(id: &str) -> String {
    if id.starts_with(MEM_NAMESPACE) {
        id.to_string()
    } else if let Some(local) = id.strip_prefix(MEM_PREFIX) {
        format!("{MEM_NAMESPACE}{local}")
    } else {
        id.to_string()
    }
}

/// Compact a full IRI back to `mem:` prefix form (canonical for Memory.id).
fn compact_id(id: &str) -> String {
    if let Some(local) = id.strip_prefix(MEM_NAMESPACE) {
        format!("{MEM_PREFIX}{local}")
    } else {
        id.to_string()
    }
}

fn escape_sparql_string(s: &str) -> String {
    let normalized = crate::turtle_io::normalize_unicode_quotes(s);
    let mut out = String::with_capacity(normalized.len());
    for c in normalized.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

fn preview_utf8(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }

    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    if end == 0 {
        return "...".to_string();
    }
    format!("{}...", &s[..end])
}

fn optional_memory_clauses() -> String {
    crate::vocab::OPTIONAL_PROPS
        .iter()
        .map(|(iri, var)| format!("OPTIONAL {{ ?id <{iri}> ?{var} }}"))
        .collect::<Vec<_>>()
        .join("\n  ")
}

fn optional_memory_clauses_for_subject(subject_iri: &str) -> String {
    crate::vocab::OPTIONAL_PROPS
        .iter()
        .map(|(iri, var)| format!("OPTIONAL {{ <{subject_iri}> <{iri}> ?{var} }}"))
        .collect::<Vec<_>>()
        .join("\n  ")
}

/// Name of the internal memory ledger.
pub const MEMORY_LEDGER: &str = "__memory";

/// Normalized ledger ID (with branch suffix).
const MEMORY_LEDGER_ID: &str = "__memory:main";

/// The memory store: CRUD operations backed by a Fluree ledger.
///
/// When `memory_dir` is set, mutations are also written to `.ttl` files
/// for git-based sharing. The ledger serves as a derived query cache.
pub struct MemoryStore {
    fluree: Fluree,
    memory_dir: Option<PathBuf>,
}

impl MemoryStore {
    /// Create a new memory store wrapping a Fluree instance.
    ///
    /// Pass `memory_dir` to enable file-based sync (e.g., `.fluree-memory/`).
    /// Pass `None` for legacy behavior (ledger-only, no file sharing).
    pub fn new(fluree: Fluree, memory_dir: Option<PathBuf>) -> Self {
        Self { fluree, memory_dir }
    }

    /// The memory directory path, if file-based sync is enabled.
    pub fn memory_dir(&self) -> Option<&Path> {
        self.memory_dir.as_deref()
    }

    /// Check if the memory ledger has been initialized.
    pub async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .fluree
            .ledger_exists(MEMORY_LEDGER_ID)
            .await
            .unwrap_or(false))
    }

    /// Initialize the memory ledger and file structure.
    ///
    /// Idempotent — safe to call on every operation. Creates the ledger,
    /// transacts the schema, and (when `memory_dir` is set) creates the
    /// directory structure, `.gitignore`, and empty `.ttl` files.
    pub async fn initialize(&self) -> Result<()> {
        if self.is_initialized().await? {
            // Ledger exists — but ensure file structure exists too
            self.ensure_file_structure()?;
            return Ok(());
        }

        debug!("Creating memory ledger");
        self.fluree.create_ledger(MEMORY_LEDGER).await?;

        debug!("Transacting memory schema");
        let schema = memory_schema_jsonld();
        match self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&schema)
            .commit()
            .await
        {
            Ok(_) => {}
            Err(fluree_db_api::ApiError::Transact(
                fluree_db_api::TransactError::CommitConflict { .. },
            )) => {
                // A concurrent process (e.g. an old MCP server) may have
                // modified the ledger between create_ledger and the schema
                // transact. This commonly happens after `fluree init` when
                // an old process is still running. Fall back to a full
                // drop-and-reinit to recover cleanly.
                warn!("Commit conflict during init — falling back to drop_and_reinit");
                self.drop_and_reinit().await?;
                self.ensure_file_structure()?;
                debug!("Memory ledger initialized (via drop_and_reinit fallback)");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        self.ensure_file_structure()?;

        debug!("Memory ledger initialized");
        Ok(())
    }

    /// Create the file-based memory directory structure if `memory_dir` is set.
    ///
    /// Idempotent — skips anything that already exists.
    fn ensure_file_structure(&self) -> Result<()> {
        let Some(dir) = &self.memory_dir else {
            return Ok(());
        };

        // Create .local/ subdirectory
        let local_dir = dir.join(".local");
        std::fs::create_dir_all(&local_dir)?;

        // .gitignore for .local/
        let gitignore_path = dir.join(".gitignore");
        if !gitignore_path.exists() {
            std::fs::write(&gitignore_path, ".local/\n")?;
        }

        // Empty .ttl files with prefix headers
        let repo_ttl = crate::turtle_io::repo_ttl_path(dir);
        if !repo_ttl.exists() {
            crate::turtle_io::create_empty_memory_file(&repo_ttl, crate::turtle_io::REPO_HEADER)?;
        }

        let user_ttl = crate::turtle_io::user_ttl_path(dir);
        if !user_ttl.exists() {
            crate::turtle_io::create_empty_memory_file(&user_ttl, crate::turtle_io::USER_HEADER)?;
        }

        Ok(())
    }

    /// Drop and reinitialize the `__memory` ledger.
    ///
    /// Used by the rebuild pipeline to recreate the ledger from `.ttl` files.
    pub async fn drop_and_reinit(&self) -> Result<()> {
        // Delete the ledger if it exists
        if self.is_initialized().await? {
            debug!("Dropping __memory ledger for rebuild");
            self.fluree
                .drop_ledger(MEMORY_LEDGER_ID, fluree_db_api::DropMode::Hard)
                .await?;
        }

        // Recreate
        debug!("Recreating __memory ledger");
        self.fluree.create_ledger(MEMORY_LEDGER).await?;

        let schema = memory_schema_jsonld();
        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&schema)
            .commit()
            .await?;

        debug!("__memory ledger reinitialized");
        Ok(())
    }

    /// Ensure the ledger is in sync with `.ttl` files.
    ///
    /// No-op if `memory_dir` is `None`.
    pub async fn ensure_synced(&self) -> Result<()> {
        if let Some(dir) = &self.memory_dir {
            crate::file_sync::ensure_synced(self, dir).await?;
        }
        Ok(())
    }

    /// Insert a JSON-LD document into the memory ledger (used by rebuild).
    pub async fn transact_insert(&self, doc: &Value) -> Result<()> {
        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(doc)
            .commit()
            .await?;
        Ok(())
    }

    /// Add a new memory to the store.
    ///
    /// Returns the generated memory ID.
    ///
    /// In file-based mode, the `.ttl` file is written first (authoritative),
    /// then the ledger cache is updated. The build hash is only updated after
    /// the ledger cache commit succeeds so that any cache failure leaves a hash
    /// mismatch and triggers a rebuild on the next `ensure_synced()`.
    pub async fn add(&self, input: MemoryInput) -> Result<String> {
        self.initialize().await?;

        let id = crate::id::generate_memory_id(input.kind);
        let created_at = Utc::now().to_rfc3339();

        let mem = Memory {
            id: id.clone(),
            kind: input.kind,
            content: input.content,
            tags: input.tags,
            scope: input.scope,
            severity: input.severity,
            artifact_refs: input.artifact_refs,
            branch: input.branch,
            created_at,
            rationale: input.rationale,
            alternatives: input.alternatives,
        };

        // File is truth — sorted rewrite so memories from different branches
        // land in different regions of the file, reducing merge conflicts.
        if let Some(dir) = &self.memory_dir {
            let (path, header, scope_filter) = match mem.scope {
                Scope::Repo => (
                    crate::turtle_io::repo_ttl_path(dir),
                    crate::turtle_io::REPO_HEADER,
                    Some(Scope::Repo),
                ),
                Scope::User => (
                    crate::turtle_io::user_ttl_path(dir),
                    crate::turtle_io::USER_HEADER,
                    Some(Scope::User),
                ),
            };
            let mut all = self.all_memories_for_scope(scope_filter.as_ref()).await?;
            all.push(mem.clone());
            crate::turtle_io::write_memory_file(&path, &all, header)?;
        }

        // Then update the ledger cache
        let doc = memory_to_jsonld(&mem);
        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&doc)
            .commit()
            .await?;

        // Update the file watermark only after cache commit succeeds.
        if let Some(dir) = &self.memory_dir {
            crate::file_sync::update_hash(dir)?;
        }

        debug!(id = %id, kind = %mem.kind, "Memory added");
        Ok(id)
    }

    /// Get a single memory by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Memory>> {
        self.initialize().await?;

        let expanded = expand_id(id);
        // Compact form is canonical for Memory.id
        let compact = compact_id(&expanded);
        let id = &expanded;
        let optional = optional_memory_clauses_for_subject(id);
        let sparql = format!(
            "SELECT ?type ?content ?scope ?severity ?tag ?artifactRef ?branch ?createdAt ?rationale ?alternatives\n\
WHERE {{\n\
  <{id}> a ?type .\n\
  <{id}> <https://ns.flur.ee/memory#content> ?content .\n\
  <{id}> <https://ns.flur.ee/memory#createdAt> ?createdAt .\n\
  {optional}\n\
}}"
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        parse_memory_from_sparql_results(&compact, &result)
    }

    /// Update a memory in place.
    ///
    /// Merges the provided fields over the existing memory, keeping the same ID.
    /// Returns the memory's (unchanged) ID.
    ///
    /// In file-based mode, the `.ttl` file is rewritten first (authoritative),
    /// then the ledger is updated via retract-all + re-insert.
    pub async fn update(&self, id: &str, update: MemoryUpdate) -> Result<String> {
        self.initialize().await?;

        let expanded = expand_id(id);
        let compact = compact_id(&expanded);

        // Load the existing memory
        let existing = self
            .get(&expanded)
            .await?
            .ok_or_else(|| MemoryError::NotFound(id.to_string()))?;

        // Merge updates over existing values, keeping the same ID
        let merged = Memory {
            id: compact.clone(),
            kind: existing.kind,
            content: update.content.unwrap_or(existing.content),
            tags: update.tags.unwrap_or(existing.tags),
            scope: existing.scope,
            severity: update.severity.or(existing.severity),
            artifact_refs: update.artifact_refs.unwrap_or(existing.artifact_refs),
            branch: existing.branch,
            created_at: existing.created_at,
            rationale: update.rationale.or(existing.rationale),
            alternatives: update.alternatives.or(existing.alternatives),
        };

        // File is truth — rewrite with the modified memory in place
        if let Some(dir) = &self.memory_dir {
            let (path, header, scope_filter) = match merged.scope {
                Scope::Repo => (
                    crate::turtle_io::repo_ttl_path(dir),
                    crate::turtle_io::REPO_HEADER,
                    Some(Scope::Repo),
                ),
                Scope::User => (
                    crate::turtle_io::user_ttl_path(dir),
                    crate::turtle_io::USER_HEADER,
                    Some(Scope::User),
                ),
            };
            let updated: Vec<Memory> = self
                .all_memories_for_scope(scope_filter.as_ref())
                .await?
                .into_iter()
                .map(|m| {
                    if m.id == compact || m.id == expanded {
                        merged.clone()
                    } else {
                        m
                    }
                })
                .collect();
            crate::turtle_io::write_memory_file(&path, &updated, header)?;
        }

        // Ledger: retract all old triples and insert updated ones in a single transaction
        let mut insert_body = memory_to_jsonld(&merged);
        insert_body.as_object_mut().unwrap().remove("@context");
        let update_doc = json!({
            "@context": { "mem": "https://ns.flur.ee/memory#" },
            "where": { "@id": &compact, "?p": "?o" },
            "delete": { "@id": &compact, "?p": "?o" },
            "insert": insert_body
        });
        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .update(&update_doc)
            .commit()
            .await?;

        // Update the file watermark only after cache commits succeed.
        if let Some(dir) = &self.memory_dir {
            crate::file_sync::update_hash(dir)?;
        }

        debug!(id = %compact, "Memory updated in place");
        Ok(compact)
    }

    /// Delete a memory by retracting all its triples.
    ///
    /// In file-based mode, the `.ttl` file is rewritten first (authoritative),
    /// then the ledger cache is updated. This is the only non-append file mutation.
    pub async fn forget(&self, id: &str) -> Result<()> {
        self.initialize().await?;

        let expanded = expand_id(id);
        let compact = compact_id(&expanded);

        // Load the memory to know its scope (for file routing)
        let mem = self
            .get(&expanded)
            .await?
            .ok_or_else(|| MemoryError::NotFound(id.to_string()))?;

        // File is truth — rewrite excluding the forgotten memory first
        if let Some(dir) = &self.memory_dir {
            let (path, header, scope_filter) = match mem.scope {
                Scope::Repo => (
                    crate::turtle_io::repo_ttl_path(dir),
                    crate::turtle_io::REPO_HEADER,
                    Some(Scope::Repo),
                ),
                Scope::User => (
                    crate::turtle_io::user_ttl_path(dir),
                    crate::turtle_io::USER_HEADER,
                    Some(Scope::User),
                ),
            };
            // Get all memories for this scope, then exclude the one being forgotten.
            // Compare both compact and expanded forms since Memory.id may use either.
            let remaining: Vec<Memory> = self
                .all_memories_for_scope(scope_filter.as_ref())
                .await?
                .into_iter()
                .filter(|m| m.id != compact && m.id != expanded)
                .collect();
            crate::turtle_io::write_memory_file(&path, &remaining, header)?;
        }

        // Then update the ledger cache (JSON-LD @context expands mem: prefix)
        let delete_doc = json!({
            "@context": {
                "mem": "https://ns.flur.ee/memory#"
            },
            "where": { "@id": &compact, "?p": "?o" },
            "delete": { "@id": &compact, "?p": "?o" }
        });

        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .update(&delete_doc)
            .commit()
            .await?;

        // Update the file watermark only after cache commit succeeds.
        if let Some(dir) = &self.memory_dir {
            crate::file_sync::update_hash(dir)?;
        }

        debug!(id = %id, "Memory forgotten");
        Ok(())
    }

    /// Get ALL memories for a scope.
    ///
    /// Used by `forget()` and `update()` to rewrite the Turtle file.
    async fn all_memories_for_scope(&self, scope: Option<&Scope>) -> Result<Vec<Memory>> {
        self.initialize().await?;

        let mut where_clauses = vec![
            "?id a ?type".to_string(),
            "?id <https://ns.flur.ee/memory#content> ?content".to_string(),
            "?id <https://ns.flur.ee/memory#createdAt> ?createdAt".to_string(),
        ];

        if let Some(scope) = scope {
            where_clauses.push(format!(
                "?id <https://ns.flur.ee/memory#scope> <{}>",
                scope.iri()
            ));
        }

        let sparql = format!(
            "SELECT ?id ?type ?content ?scope ?severity ?tag ?artifactRef ?branch ?createdAt ?rationale ?alternatives\nWHERE {{\n  {}\n  {}\n}}\nORDER BY ASC(?id)",
            where_clauses.join(" .\n  "),
            optional_memory_clauses(),
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        parse_memories_from_sparql_results(&result)
    }

    /// Get all memories matching the filter.
    pub async fn current_memories(&self, filter: &MemoryFilter) -> Result<Vec<Memory>> {
        self.initialize().await?;

        // Build SPARQL query with filters
        let mut where_clauses = vec![
            "?id a ?type".to_string(),
            "?id <https://ns.flur.ee/memory#content> ?content".to_string(),
            "?id <https://ns.flur.ee/memory#createdAt> ?createdAt".to_string(),
        ];

        // Apply kind filter
        if let Some(kind) = &filter.kind {
            where_clauses.push(format!(
                "?id a <{}>",
                kind.class_iri()
                    .replace("mem:", "https://ns.flur.ee/memory#")
            ));
        }

        // Apply tag filter
        for tag in &filter.tags {
            let tag = escape_sparql_string(tag);
            where_clauses.push(format!("?id <https://ns.flur.ee/memory#tag> \"{tag}\""));
        }

        // Apply branch filter
        if let Some(branch) = &filter.branch {
            let branch = escape_sparql_string(branch);
            where_clauses.push(format!(
                "?id <https://ns.flur.ee/memory#branch> \"{branch}\""
            ));
        }

        // Apply scope filter (IRI-based)
        if let Some(scope) = &filter.scope {
            where_clauses.push(format!(
                "?id <https://ns.flur.ee/memory#scope> <{}>",
                scope.iri()
            ));
        }

        let sparql = format!(
            "SELECT ?id ?type ?content ?scope ?severity ?tag ?artifactRef ?branch ?createdAt ?rationale ?alternatives\nWHERE {{\n  {}\n  {}\n}}\nORDER BY DESC(?createdAt)",
            where_clauses.join(" .\n  "),
            optional_memory_clauses(),
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        parse_memories_from_sparql_results(&result)
    }

    /// Get memory store status summary, including previews of recent memories.
    pub async fn status(&self) -> Result<MemoryStatus> {
        let memory_dir = self.memory_dir.as_ref().map(|p| p.display().to_string());

        if !self.is_initialized().await? {
            return Ok(MemoryStatus {
                initialized: false,
                total_memories: 0,
                by_kind: Vec::new(),
                total_tags: 0,
                recent: Vec::new(),
                memory_dir,
            });
        }

        let all = self.current_memories(&MemoryFilter::default()).await?;

        let mut by_kind: std::collections::HashMap<MemoryKind, usize> =
            std::collections::HashMap::new();
        let mut total_tags = 0;

        for m in &all {
            *by_kind.entry(m.kind).or_default() += 1;
            total_tags += m.tags.len();
        }

        let by_kind: Vec<(MemoryKind, usize)> = by_kind.into_iter().collect();

        // Build previews of the most recent memories (up to 10)
        let recent: Vec<MemoryPreview> = all
            .iter()
            .take(10)
            .map(|m| {
                let summary = preview_utf8(&m.content, 100);
                MemoryPreview {
                    id: m.id.clone(),
                    kind: m.kind,
                    summary,
                    tags: m.tags.clone(),
                }
            })
            .collect();

        Ok(MemoryStatus {
            initialized: true,
            total_memories: all.len(),
            by_kind,
            total_tags,
            recent,
            memory_dir,
        })
    }

    /// Export all memories as JSON.
    pub async fn export(&self) -> Result<Value> {
        self.initialize().await?;
        let all = self.current_memories(&MemoryFilter::default()).await?;
        Ok(serde_json::to_value(&all)?)
    }

    /// Full-text recall: BM25-ranked search over memory content.
    ///
    /// Uses the native `@fulltext` datatype and `fulltext()` scoring function
    /// to rank memories by relevance. Returns `(memory_id, bm25_score)` pairs
    /// ordered by descending score, limited to non-zero matches.
    pub async fn recall_fulltext(
        &self,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<(String, f64)>> {
        self.initialize().await?;

        let bind_expr = format!(
            "(fulltext ?content \"{}\")",
            query_text.replace('"', "\\\"")
        );

        let query = json!({
            "@context": {
                "mem": "https://ns.flur.ee/memory#"
            },
            "select": ["?id", "?score"],
            "where": [
                {
                    "@id": "?id",
                    "mem:content": "?content"
                },
                ["bind", "?score", bind_expr],
                ["filter", "(> ?score 0)"]
            ],
            "orderBy": [["desc", "?score"]],
            "limit": limit
        });

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .jsonld(&query)
            .execute_formatted()
            .await?;

        // Parse the flat-array result: [[id, score], ...]
        let mut pairs = Vec::new();
        if let Some(rows) = result.as_array() {
            for row in rows {
                if let Some(arr) = row.as_array() {
                    if let (Some(id), Some(score)) = (
                        arr.first().and_then(|v| v.as_str()),
                        arr.get(1).and_then(serde_json::Value::as_f64),
                    ) {
                        pairs.push((id.to_string(), score));
                    }
                }
            }
        }

        Ok(pairs)
    }

    /// Execute a raw SPARQL query against the memory ledger.
    ///
    /// Returns the raw JSON result from Fluree.
    pub async fn query_sparql(&self, sparql: &str) -> Result<Value> {
        self.initialize().await?;

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(sparql)
            .execute_formatted()
            .await?;

        Ok(result)
    }

    /// Import memories from a JSON array.
    ///
    /// Returns the number of memories imported.
    pub async fn import(&self, data: Value) -> Result<usize> {
        self.initialize().await?;

        let memories: Vec<Memory> = serde_json::from_value(data)?;
        let count = memories.len();

        for mut mem in memories {
            if crate::secrets::SecretDetector::has_secrets(&mem.content) {
                mem.content = crate::secrets::SecretDetector::redact(&mem.content);
            }
            if let Some(rationale) = mem.rationale.as_deref() {
                if crate::secrets::SecretDetector::has_secrets(rationale) {
                    mem.rationale = Some(crate::secrets::SecretDetector::redact(rationale));
                }
            }
            if let Some(alternatives) = mem.alternatives.as_deref() {
                if crate::secrets::SecretDetector::has_secrets(alternatives) {
                    mem.alternatives = Some(crate::secrets::SecretDetector::redact(alternatives));
                }
            }

            let doc = memory_to_jsonld(&mem);

            self.fluree
                .graph(MEMORY_LEDGER_ID)
                .transact()
                .insert(&doc)
                .commit()
                .await?;
        }

        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// SPARQL result parsing helpers
// ---------------------------------------------------------------------------

/// Parse SPARQL JSON results format into a list of Memory structs.
///
/// The SPARQL results format is:
/// ```json
/// { "results": { "bindings": [ { "var": { "value": "..." }, ... }, ... ] } }
/// ```
///
/// Or the Fluree flat table format:
/// ```json
/// [ [ val1, val2, ... ], ... ]
/// ```
fn parse_memories_from_sparql_results(result: &Value) -> Result<Vec<Memory>> {
    // Try SPARQL JSON Results format first
    if let Some(bindings) = result
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
    {
        return parse_from_sparql_bindings(bindings);
    }

    // Try Fluree flat array format: [[val, val, ...], ...]
    if let Some(rows) = result.as_array() {
        return parse_from_flat_rows(rows);
    }

    Ok(Vec::new())
}

/// Parse a single memory from SPARQL results where the ID is known (not a binding variable).
///
/// The `get()` query uses `<{id}>` as a constant, so `?id` is NOT in the result bindings.
/// We inject the known ID into each binding so `parse_from_sparql_bindings` can process them.
fn parse_memory_from_sparql_results(id: &str, result: &Value) -> Result<Option<Memory>> {
    // Inject the known ID into bindings since the get() query doesn't select ?id
    if let Some(bindings) = result
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
    {
        if bindings.is_empty() {
            return Ok(None);
        }
        let patched: Vec<Value> = bindings
            .iter()
            .map(|b| {
                let mut b = b.clone();
                if let Some(obj) = b.as_object_mut() {
                    obj.insert(
                        "id".to_string(),
                        serde_json::json!({"type": "uri", "value": id}),
                    );
                }
                b
            })
            .collect();
        let memories = parse_from_sparql_bindings(&patched)?;
        if memories.is_empty() {
            return Ok(None);
        }
        return Ok(merge_memory_rows(id, &memories));
    }

    // Fallback: try generic parse
    let memories = parse_memories_from_sparql_results(result)?;
    if memories.is_empty() {
        return Ok(None);
    }
    Ok(merge_memory_rows(id, &memories))
}

/// Parse memories from SPARQL bindings format.
fn parse_from_sparql_bindings(bindings: &[Value]) -> Result<Vec<Memory>> {
    use std::collections::HashMap;

    // Group bindings by subject ID — compact to `mem:` prefix form (canonical)
    let mut grouped: HashMap<String, Vec<&Value>> = HashMap::new();
    for binding in bindings {
        if let Some(id) = extract_binding_value(binding, "id") {
            grouped.entry(compact_id(&id)).or_default().push(binding);
        }
    }

    let mut memories = Vec::new();
    for (id, rows) in grouped {
        if let Some(mem) = merge_bindings_to_memory(&id, &rows) {
            memories.push(mem);
        }
    }

    Ok(memories)
}

/// Parse memories from Fluree flat row format.
///
/// Expected column order matches the SELECT clause:
/// `?id ?type ?content ?scope ?severity ?tag ?artifactRef ?branch ?createdAt ?rationale ?alternatives`
fn parse_from_flat_rows(rows: &[Value]) -> Result<Vec<Memory>> {
    use std::collections::HashMap;

    // Group rows by subject ID — compact to `mem:` prefix form (canonical)
    let mut grouped: HashMap<String, Vec<&Value>> = HashMap::new();
    for row in rows {
        if let Some(arr) = row.as_array() {
            if let Some(id) = arr.first().and_then(|v| v.as_str()) {
                grouped.entry(compact_id(id)).or_default().push(row);
            }
        }
    }

    let mut memories = Vec::new();
    for (id, rows) in grouped {
        if let Some(mem) = merge_flat_rows_to_memory(&id, &rows) {
            memories.push(mem);
        }
    }

    Ok(memories)
}

fn extract_binding_value(binding: &Value, var: &str) -> Option<String> {
    binding
        .get(var)
        .and_then(|v| v.get("value").or(Some(v)))
        .and_then(|v| v.as_str())
        .map(std::string::ToString::to_string)
}

fn merge_bindings_to_memory(id: &str, bindings: &[&Value]) -> Option<Memory> {
    let first = bindings.first()?;

    let type_str = extract_binding_value(first, "type")?;
    let content = extract_binding_value(first, "content")?;
    let created_at = extract_binding_value(first, "createdAt")?;

    let kind = iri_to_kind(&type_str)?;

    // Collect multi-value fields across rows
    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for b in bindings {
        if let Some(tag) = extract_binding_value(b, "tag") {
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }
        if let Some(aref) = extract_binding_value(b, "artifactRef") {
            if !artifact_refs.contains(&aref) {
                artifact_refs.push(aref);
            }
        }
    }

    let scope = extract_binding_value(first, "scope")
        .and_then(|s| crate::types::Scope::parse_str(&s))
        .unwrap_or_default();

    let severity = extract_binding_value(first, "severity")
        .and_then(|s| crate::types::Severity::parse_str(&s));

    Some(Memory {
        id: id.to_string(),
        kind,
        content,
        tags,
        scope,
        severity,
        artifact_refs,
        branch: extract_binding_value(first, "branch"),
        created_at,
        rationale: extract_binding_value(first, "rationale"),
        alternatives: extract_binding_value(first, "alternatives"),
    })
}

fn merge_flat_rows_to_memory(id: &str, rows: &[&Value]) -> Option<Memory> {
    let first = rows.first()?.as_array()?;
    if first.len() < 9 {
        return None;
    }

    // Column indices match SELECT order:
    // 0=id, 1=type, 2=content, 3=scope, 4=severity,
    // 5=tag, 6=artifactRef, 7=branch, 8=createdAt,
    // 9=rationale, 10=alternatives
    let type_str = first.get(1)?.as_str()?;
    let content = first.get(2)?.as_str()?.to_string();
    let created_at = first.get(8)?.as_str()?.to_string();

    let kind = iri_to_kind(type_str)?;

    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for row in rows {
        if let Some(arr) = row.as_array() {
            if let Some(tag) = arr.get(5).and_then(|v| v.as_str()) {
                if !tags.contains(&tag.to_string()) {
                    tags.push(tag.to_string());
                }
            }
            if let Some(aref) = arr.get(6).and_then(|v| v.as_str()) {
                if !artifact_refs.contains(&aref.to_string()) {
                    artifact_refs.push(aref.to_string());
                }
            }
        }
    }

    let scope = first
        .get(3)
        .and_then(|v| v.as_str())
        .and_then(crate::types::Scope::parse_str)
        .unwrap_or_default();

    let severity = first
        .get(4)
        .and_then(|v| v.as_str())
        .and_then(crate::types::Severity::parse_str);

    Some(Memory {
        id: id.to_string(),
        kind,
        content,
        tags,
        scope,
        severity,
        artifact_refs,
        branch: first.get(7).and_then(|v| v.as_str()).map(String::from),
        created_at,
        rationale: first.get(9).and_then(|v| v.as_str()).map(String::from),
        alternatives: first.get(10).and_then(|v| v.as_str()).map(String::from),
    })
}

/// Merge multiple result rows for the same ID into one Memory.
fn merge_memory_rows(id: &str, memories: &[Memory]) -> Option<Memory> {
    let first = memories.first()?;
    if memories.len() == 1 {
        return Some(first.clone());
    }

    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for m in memories {
        for tag in &m.tags {
            if !tags.contains(tag) {
                tags.push(tag.clone());
            }
        }
        for aref in &m.artifact_refs {
            if !artifact_refs.contains(aref) {
                artifact_refs.push(aref.clone());
            }
        }
    }

    Some(Memory {
        id: id.to_string(),
        kind: first.kind,
        content: first.content.clone(),
        tags,
        scope: first.scope.clone(),
        severity: first.severity.clone(),
        artifact_refs,
        branch: first.branch.clone(),
        created_at: first.created_at.clone(),
        rationale: first.rationale.clone(),
        alternatives: first.alternatives.clone(),
    })
}

/// Convert a full IRI or prefixed name to a MemoryKind.
fn iri_to_kind(iri: &str) -> Option<MemoryKind> {
    let local = if let Some(stripped) = iri.strip_prefix("https://ns.flur.ee/memory#") {
        stripped
    } else if let Some(stripped) = iri.strip_prefix("mem:") {
        stripped
    } else {
        iri
    };

    match local {
        "Fact" => Some(MemoryKind::Fact),
        "Decision" => Some(MemoryKind::Decision),
        "Constraint" => Some(MemoryKind::Constraint),
        // Backwards compat: map removed kinds to Fact
        "Preference" | "Artifact" => Some(MemoryKind::Fact),
        _ => None,
    }
}
