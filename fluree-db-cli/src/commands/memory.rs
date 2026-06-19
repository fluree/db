use crate::cli::MemoryAction;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{
    format_context_paged, MemoryFilter, MemoryInput, MemoryKind, MemoryStore, MemoryUpdate,
    RecallEngine, RecallResult, Scope, SecretDetector,
};

mod ide;

pub async fn run(action: MemoryAction, dirs: &FlureeDir) -> CliResult<()> {
    match action {
        MemoryAction::Init { yes, no_mcp } => run_init(dirs, yes, no_mcp).await,
        MemoryAction::Add {
            kind,
            text,
            tags,
            refs,
            severity,
            scope,
            rationale,
            alternatives,
            format,
        } => {
            run_add(
                kind,
                text,
                tags,
                refs,
                severity,
                scope,
                rationale,
                alternatives,
                &format,
                dirs,
            )
            .await
        }
        MemoryAction::Recall {
            query,
            limit,
            offset,
            kind,
            tags,
            scope,
            format,
        } => run_recall(&query, limit, offset, kind, tags, scope, &format, dirs).await,
        MemoryAction::Update {
            id,
            text,
            tags,
            refs,
            format,
        } => run_update(&id, text, tags, refs, &format, dirs).await,
        MemoryAction::Forget { id } => run_forget(&id, dirs).await,
        MemoryAction::Status => run_status(dirs).await,
        MemoryAction::Export => run_export(dirs).await,
        MemoryAction::Import { file } => run_import(&file, dirs).await,
        MemoryAction::McpInstall { ide: ide_arg } => ide::run_mcp_install(ide_arg.as_deref()),
    }
}

fn build_store(dirs: &FlureeDir) -> CliResult<MemoryStore> {
    // Short-lived CLI commands keep a persistent (file-backed) ledger so that
    // `import` and the `init` legacy-ledger migration work and repeated
    // invocations don't rebuild from scratch. The long-lived `mcp serve` path
    // uses an ephemeral in-memory ledger instead (see `mcp_serve`), which is
    // what makes many concurrent MCP processes safe.
    let fluree = context::build_fluree(dirs)?;

    // Determine memory_dir: use .fluree-memory/ at the project root.
    // In unified (local) mode, data_dir is .fluree/ so its parent is the project root.
    // Always enable in unified mode — MemoryStore creates the directory structure on init.
    let memory_dir = if dirs.is_unified() {
        let project_root = dirs.data_dir().parent().unwrap_or(dirs.data_dir());
        Some(project_root.join(".fluree-memory"))
    } else {
        None // Global mode — no file sharing
    };

    Ok(MemoryStore::new(fluree, memory_dir))
}

/// Build the memory store and bring its in-memory ledger up to date with
/// any `.ttl` files on disk. Use from every memory subcommand except
/// `init`, which intentionally constructs an empty store before sync.
async fn build_synced_store(dirs: &FlureeDir) -> CliResult<MemoryStore> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    Ok(store)
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

async fn run_init(dirs: &FlureeDir, yes: bool, no_mcp: bool) -> CliResult<()> {
    // === Phase 1: Initialize memory store (existing behavior) ===
    let store = build_store(dirs)?;
    store.initialize().await.map_err(memory_err)?;

    // Migration: export existing ledger memories to .ttl files
    if let Some(memory_dir) = store.memory_dir() {
        let memory_dir = memory_dir.to_path_buf();
        let repo_ttl = fluree_db_memory::turtle_io::repo_ttl_path(&memory_dir);
        let user_ttl = fluree_db_memory::turtle_io::user_ttl_path(&memory_dir);

        let existing = store
            .current_memories(&MemoryFilter::default())
            .await
            .map_err(memory_err)?;
        if !existing.is_empty() {
            let repo_mems: Vec<_> = existing
                .iter()
                .filter(|m| m.scope == fluree_db_memory::Scope::Repo)
                .cloned()
                .collect();
            let user_mems: Vec<_> = existing
                .iter()
                .filter(|m| m.scope == fluree_db_memory::Scope::User)
                .cloned()
                .collect();

            if !repo_mems.is_empty() {
                fluree_db_memory::turtle_io::write_memory_file(
                    &repo_ttl,
                    &repo_mems,
                    fluree_db_memory::turtle_io::REPO_HEADER,
                )
                .map_err(memory_err)?;
            }
            if !user_mems.is_empty() {
                fluree_db_memory::turtle_io::write_memory_file(
                    &user_ttl,
                    &user_mems,
                    fluree_db_memory::turtle_io::USER_HEADER,
                )
                .map_err(memory_err)?;
            }

            fluree_db_memory::file_sync::update_hash(&memory_dir).map_err(memory_err)?;

            println!(
                "Migrated {} existing memories to .ttl files.",
                existing.len()
            );
        }

        println!("Memory store initialized at {}", memory_dir.display());
        println!();
        println!("Repo memories are stored in .fluree-memory/repo.ttl (git-tracked).");
        println!("Commit this directory to share project knowledge with your team.");
    } else {
        println!("Memory store initialized.");
    }

    // === Phase 2: Detect and configure AI tools ===
    if no_mcp {
        return Ok(());
    }
    ide::run_mcp_phase(yes)
}

// ---------------------------------------------------------------------------
// Remaining subcommands (unchanged)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_add(
    kind_str: String,
    text: Option<String>,
    tags: Vec<String>,
    refs: Vec<String>,
    severity: Option<String>,
    scope: Option<String>,
    rationale: Option<String>,
    alternatives: Option<String>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let kind = MemoryKind::parse(&kind_str).ok_or_else(|| {
        CliError::Usage(format!(
            "invalid memory kind '{kind_str}'; valid: fact, decision, constraint"
        ))
    })?;

    let content = match text {
        Some(t) => t,
        None => {
            // Read from stdin
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read stdin: {e}")))?;
            buf.trim().to_string()
        }
    };

    if content.is_empty() {
        return Err(CliError::Usage(
            "no content provided; use --text or pipe via stdin".to_string(),
        ));
    }

    if tags.is_empty() {
        return Err(CliError::Usage(
            "at least one tag is required (use --tags t1,t2,...); \
             tags are the primary recall signal"
                .to_string(),
        ));
    }

    // Check for secrets
    let content = if SecretDetector::has_secrets(&content) {
        eprintln!(
            "  warning: secrets detected in content — storing redacted version.\n  \
             Original content contained sensitive data that was replaced with [REDACTED]."
        );
        SecretDetector::redact(&content)
    } else {
        content
    };

    // Enforce content length limit
    if content.len() > fluree_db_memory::MAX_CONTENT_LENGTH {
        return Err(CliError::Usage(format!(
            "memory content is {} characters (max {}). \
             A good memory is 1-3 sentences capturing a single insight.",
            content.len(),
            fluree_db_memory::MAX_CONTENT_LENGTH,
        )));
    }

    let severity = severity
        .map(|s| {
            fluree_db_memory::Severity::parse_str(&s).ok_or_else(|| {
                CliError::Usage(format!(
                    "invalid severity '{s}'; valid: must, should, prefer"
                ))
            })
        })
        .transpose()?;

    let scope = scope
        .map(|s| {
            Scope::parse_str(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid scope '{s}'; valid: repo, user")))
        })
        .transpose()?
        .unwrap_or_default();

    let branch = fluree_db_memory::detect_git_branch();

    let recall_query = content.clone();

    let input = MemoryInput {
        kind,
        content,
        tags,
        scope,
        severity,
        artifact_refs: refs,
        branch,
        rationale,
        alternatives,
    };

    let store = build_synced_store(dirs).await?;
    let id = store.add(input).await.map_err(memory_err)?;

    match format {
        "json" => {
            if let Some(mem) = store.get(&id).await.map_err(memory_err)? {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&fluree_db_memory::format_json(&mem))
                        .unwrap_or_default()
                );
            }
        }
        _ => {
            println!("Stored memory: {id}");
        }
    }

    // Surface related memories for housekeeping
    if let Some(related) = find_related_memories_cli(&store, &id, &recall_query).await {
        print!("{related}");
    }

    Ok(())
}

/// Find existing memories related to a just-stored memory.
async fn find_related_memories_cli(
    store: &MemoryStore,
    new_id: &str,
    content: &str,
) -> Option<String> {
    let bm25_hits = store.recall_fulltext(content, 5).await.ok()?;
    let filter = MemoryFilter::default();
    let all = store.current_memories(&filter).await.ok()?;
    let branch = fluree_db_memory::detect_git_branch();

    let candidates =
        RecallEngine::find_related(new_id, content, &bm25_hits, &all, branch.as_deref());

    if candidates.is_empty() {
        return None;
    }

    Some(fluree_db_memory::format_related_memories(&candidates))
}

#[allow(clippy::too_many_arguments)]
async fn run_recall(
    query: &str,
    limit: usize,
    offset: usize,
    kind: Option<String>,
    tags: Vec<String>,
    scope: Option<String>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let kind_filter = kind
        .map(|s| {
            MemoryKind::parse(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid memory kind '{s}'")))
        })
        .transpose()?;

    let scope_filter = scope
        .map(|s| {
            Scope::parse_str(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid scope '{s}'; valid: repo, user")))
        })
        .transpose()?;

    let filter = MemoryFilter {
        kind: kind_filter,
        tags,
        branch: None,
        scope: scope_filter,
    };

    let store = build_synced_store(dirs).await?;

    let fetch_n = offset + limit;

    // BM25 fulltext search for content relevance
    let bm25_hits = store
        .recall_fulltext(query, fetch_n)
        .await
        .map_err(memory_err)?;

    // Load full memory objects for metadata re-ranking
    let all = store.current_memories(&filter).await.map_err(memory_err)?;
    let total_store = all.len();

    let branch = fluree_db_memory::detect_git_branch();
    let scored = if bm25_hits.is_empty() {
        // Fallback to metadata-only scoring when BM25 returns nothing
        RecallEngine::recall_metadata_only(query, &all, branch.as_deref(), Some(fetch_n))
    } else {
        RecallEngine::rerank(query, &bm25_hits, &all, branch.as_deref())
    };

    // Apply offset + limit slicing
    let paged: Vec<_> = scored.into_iter().skip(offset).take(limit).collect();
    let has_more = paged.len() == limit;

    let result = RecallResult {
        query: query.to_string(),
        memories: paged.clone(),
        total_count: total_store,
    };

    match format {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&fluree_db_memory::format_recall_json(&result))
                    .unwrap_or_default()
            );
        }
        "context" => {
            print!(
                "{}",
                format_context_paged(&paged, offset, Some(limit), total_store, has_more, None)
            );
        }
        _ => {
            print!("{}", fluree_db_memory::format_recall_text(&result));
            if has_more {
                println!(
                    "  (showing results {}–{}; use --offset {} for more)",
                    offset + 1,
                    offset + paged.len(),
                    offset + paged.len()
                );
            }
        }
    }

    Ok(())
}

async fn run_update(
    id: &str,
    text: Option<String>,
    tags: Option<Vec<String>>,
    refs: Option<Vec<String>>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for secrets in new content
    let text = text.map(|t| {
        if SecretDetector::has_secrets(&t) {
            eprintln!("  warning: secrets detected — storing redacted version.");
            SecretDetector::redact(&t)
        } else {
            t
        }
    });

    let update = MemoryUpdate {
        content: text,
        tags,
        severity: None,
        artifact_refs: refs,
        rationale: None,
        alternatives: None,
    };

    let store = build_synced_store(dirs).await?;
    let updated_id = store.update(id, update).await.map_err(memory_err)?;

    match format {
        "json" => {
            if let Some(mem) = store.get(&updated_id).await.map_err(memory_err)? {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&fluree_db_memory::format_json(&mem))
                        .unwrap_or_default()
                );
            }
        }
        _ => {
            println!("Updated: {updated_id}");
        }
    }

    Ok(())
}

async fn run_forget(id: &str, dirs: &FlureeDir) -> CliResult<()> {
    let store = build_synced_store(dirs).await?;
    store.forget(id).await.map_err(memory_err)?;
    println!("Forgotten: {id}");
    Ok(())
}

async fn run_status(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_synced_store(dirs).await?;
    let status = store.status().await.map_err(memory_err)?;
    print!("{}", fluree_db_memory::format_status_text(&status));
    Ok(())
}

async fn run_export(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_synced_store(dirs).await?;
    let data = store.export().await.map_err(memory_err)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&data).unwrap_or_default()
    );
    Ok(())
}

async fn run_import(file: &std::path::Path, dirs: &FlureeDir) -> CliResult<()> {
    let content = std::fs::read_to_string(file)
        .map_err(|e| CliError::Input(format!("failed to read {}: {e}", file.display())))?;
    let data: serde_json::Value = serde_json::from_str(&content)?;

    let store = build_synced_store(dirs).await?;
    let count = store.import(data).await.map_err(memory_err)?;
    println!("Imported {count} memories.");
    Ok(())
}

/// Convert MemoryError to CliError.
fn memory_err(e: fluree_db_memory::MemoryError) -> CliError {
    match e {
        fluree_db_memory::MemoryError::NotFound(id) => {
            CliError::NotFound(format!("memory '{id}' not found"))
        }
        fluree_db_memory::MemoryError::Api(api_err) => CliError::Api(api_err),
        _ => CliError::Config(e.to_string()),
    }
}
