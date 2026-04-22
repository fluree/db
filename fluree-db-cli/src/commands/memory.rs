use crate::cli::MemoryAction;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{
    format_context_paged, MemoryFilter, MemoryInput, MemoryKind, MemoryStore, MemoryUpdate,
    RecallEngine, RecallResult, Scope, SecretDetector,
};
use std::path::{Path, PathBuf};

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
        MemoryAction::McpInstall { ide } => run_mcp_install(ide.as_deref()),
    }
}

fn build_store(dirs: &FlureeDir) -> CliResult<MemoryStore> {
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

// ---------------------------------------------------------------------------
// AI tool detection types
// ---------------------------------------------------------------------------

/// AI coding tools that support MCP server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AiTool {
    /// Claude Code CLI + VS Code extension (local scope via `claude mcp add`)
    ClaudeCode,
    /// Cursor IDE
    Cursor,
    /// VS Code with GitHub Copilot (or other VS Code-native MCP consumers)
    VsCode,
    /// Windsurf (Codeium) — global config only
    Windsurf,
    /// Zed editor
    Zed,
}

impl AiTool {
    fn display_name(self) -> &'static str {
        match self {
            AiTool::ClaudeCode => "Claude Code",
            AiTool::Cursor => "Cursor",
            AiTool::VsCode => "VS Code (Copilot)",
            AiTool::Windsurf => "Windsurf",
            AiTool::Zed => "Zed",
        }
    }

    fn ide_id(self) -> &'static str {
        match self {
            AiTool::ClaudeCode => "claude-code",
            AiTool::Cursor => "cursor",
            AiTool::VsCode => "vscode",
            AiTool::Windsurf => "windsurf",
            AiTool::Zed => "zed",
        }
    }
}

struct DetectedTool {
    tool: AiTool,
    already_configured: bool,
}

// ---------------------------------------------------------------------------
// Filesystem-only detection
// ---------------------------------------------------------------------------

fn home_dir() -> Option<PathBuf> {
    dirs::home_dir()
}

/// Best-effort project root for writing IDE config files.
///
/// IDE config locations like `.cursor/mcp.json` are interpreted relative to the
/// workspace root, not the caller's current subdirectory. When `fluree memory init`
/// is run from a nested path, we still want to install at the repository root.
///
/// Heuristic: walk up from cwd looking for a `.git` entry. If none is found,
/// fall back to cwd.
fn project_root_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut current = cwd.clone();
    loop {
        if current.join(".git").exists() {
            return current;
        }
        if !current.pop() {
            return cwd;
        }
    }
}

/// Check `/Applications/{name}` on macOS. Returns false on other platforms.
fn is_app_installed(app_name: &str) -> bool {
    #[cfg(target_os = "macos")]
    {
        Path::new("/Applications").join(app_name).exists()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = app_name;
        false
    }
}

/// Read a JSON file and check if a nested key path exists.
fn json_has_key(path: &Path, keys: &[&str]) -> bool {
    let Ok(content) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };
    let mut current = &json;
    for key in keys {
        match current.get(key) {
            Some(v) => current = v,
            None => return false,
        }
    }
    true
}

/// Check if Claude Code already has fluree-memory configured (local scope).
/// Local-scope MCP servers are stored in ~/.claude.json at:
///   projects["/path/to/project"].mcpServers["fluree-memory"]
fn claude_code_already_configured(home: &Option<PathBuf>) -> bool {
    let Some(home) = home else { return false };
    let claude_json = home.join(".claude.json");
    let Ok(content) = std::fs::read_to_string(&claude_json) else {
        return false;
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };
    // Get the canonical working directory to match Claude's stored project paths
    // (canonicalize resolves symlinks, matching how Claude stores project keys)
    let Ok(cwd) = std::env::current_dir().and_then(std::fs::canonicalize) else {
        return false;
    };
    let cwd_str = cwd.display().to_string();
    json.get("projects")
        .and_then(|p| p.get(&cwd_str))
        .and_then(|proj| proj.get("mcpServers"))
        .and_then(|servers| servers.get("fluree-memory"))
        .is_some()
}

fn detect_ai_tools() -> Vec<DetectedTool> {
    let home = home_dir();
    let project_root = project_root_dir();
    let mut detected = Vec::new();

    // Claude Code — detected via ~/.claude/ directory
    if home.as_ref().is_some_and(|h| h.join(".claude").is_dir()) {
        let already = claude_code_already_configured(&home)
            || json_has_key(
                &project_root.join(".mcp.json"),
                &["mcpServers", "fluree-memory"],
            );
        detected.push(DetectedTool {
            tool: AiTool::ClaudeCode,
            already_configured: already,
        });
    }

    // Cursor — /Applications/Cursor.app or ~/.cursor/
    if is_app_installed("Cursor.app") || home.as_ref().is_some_and(|h| h.join(".cursor").is_dir()) {
        detected.push(DetectedTool {
            tool: AiTool::Cursor,
            already_configured: json_has_key(
                &project_root.join(".cursor/mcp.json"),
                &["mcpServers", "fluree-memory"],
            ),
        });
    }

    // VS Code — /Applications/Visual Studio Code.app or ~/.vscode/
    if is_app_installed("Visual Studio Code.app")
        || home.as_ref().is_some_and(|h| h.join(".vscode").is_dir())
    {
        detected.push(DetectedTool {
            tool: AiTool::VsCode,
            already_configured: json_has_key(
                &project_root.join(".vscode/mcp.json"),
                &["servers", "fluree-memory"],
            ),
        });
    }

    // Windsurf — /Applications/Windsurf.app or ~/.codeium/windsurf/
    if is_app_installed("Windsurf.app")
        || home
            .as_ref()
            .is_some_and(|h| h.join(".codeium").join("windsurf").is_dir())
    {
        let already = home.as_ref().is_some_and(|h| {
            json_has_key(
                &h.join(".codeium/windsurf/mcp_config.json"),
                &["mcpServers", "fluree-memory"],
            )
        });
        detected.push(DetectedTool {
            tool: AiTool::Windsurf,
            already_configured: already,
        });
    }

    // Zed — /Applications/Zed.app or ~/.zed/ or ~/.config/zed/
    if is_app_installed("Zed.app")
        || home.as_ref().is_some_and(|h| h.join(".zed").is_dir())
        || home
            .as_ref()
            .is_some_and(|h| h.join(".config").join("zed").is_dir())
    {
        detected.push(DetectedTool {
            tool: AiTool::Zed,
            already_configured: json_has_key(
                &project_root.join(".zed/settings.json"),
                &["context_servers", "fluree-memory"],
            ),
        });
    }

    detected
}

// ---------------------------------------------------------------------------
// Interactive prompting
// ---------------------------------------------------------------------------

/// Returns true if stdin is a terminal (not piped).
fn stdin_is_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}

/// Prompt for Y/n confirmation on stderr. Returns true for Y (default).
fn prompt_yn(question: &str) -> bool {
    use std::io::Write;
    eprint!("{question} [Y/n] ");
    let _ = std::io::stderr().flush();

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return true;
    }
    let trimmed = input.trim().to_lowercase();
    trimmed.is_empty() || trimmed == "y" || trimmed == "yes"
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

    let detected = detect_ai_tools();
    if detected.is_empty() {
        println!();
        println!("No AI coding tools detected.");
        println!("Run 'fluree memory mcp-install --ide <tool>' to configure manually.");
        println!("Supported: claude-code, cursor, vscode, windsurf, zed");
        return Ok(());
    }

    // Show detection summary
    println!();
    println!("Detected AI coding tools:");
    for dt in &detected {
        if dt.already_configured {
            println!("  - {} (already configured)", dt.tool.display_name());
        } else {
            println!("  - {}", dt.tool.display_name());
        }
    }

    let to_install: Vec<&DetectedTool> = detected
        .iter()
        .filter(|dt| !dt.already_configured)
        .collect();

    if to_install.is_empty() {
        println!();
        println!("All detected tools are already configured.");
        return Ok(());
    }

    // Non-interactive: --yes auto-confirms; no TTY without --yes skips entirely.
    let interactive = stdin_is_tty();
    if !yes && !interactive {
        println!();
        println!("Non-interactive shell detected. Use --yes to auto-install MCP configs,");
        println!("or run 'fluree memory mcp-install --ide <tool>' interactively.");
        return Ok(());
    }

    let fluree_bin = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "fluree".to_string());

    println!();
    let mut installed_count = 0usize;
    for dt in &to_install {
        let confirmed = yes
            || prompt_yn(&format!(
                "Install MCP config for {}?",
                dt.tool.display_name()
            ));

        if confirmed {
            match install_tool(dt.tool, &fluree_bin) {
                Ok(()) => {
                    installed_count += 1;
                }
                Err(e) => {
                    eprintln!(
                        "  warning: failed to configure {}: {}",
                        dt.tool.display_name(),
                        e
                    );
                }
            }
        } else {
            println!("  Skipped.");
        }
    }

    if installed_count > 0 {
        println!();
        println!(
            "Configured {} tool{}.",
            installed_count,
            if installed_count == 1 { "" } else { "s" }
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-tool installation
// ---------------------------------------------------------------------------

fn install_tool(tool: AiTool, fluree_bin: &str) -> CliResult<()> {
    match tool {
        AiTool::ClaudeCode => install_claude_code(fluree_bin),
        AiTool::Cursor => install_cursor(fluree_bin),
        AiTool::VsCode => install_vscode(fluree_bin),
        AiTool::Windsurf => install_windsurf(fluree_bin),
        AiTool::Zed => install_zed(fluree_bin),
    }
}

/// Server entry JSON used by tools with `mcpServers` key (Claude Code, Cursor, Windsurf).
fn server_entry_json(fluree_bin: &str) -> serde_json::Value {
    serde_json::json!({
        "command": fluree_bin,
        "args": ["mcp", "serve", "--transport", "stdio"]
    })
}

/// Cursor expects a `type: "stdio"` field for local command servers and supports
/// config interpolation like `${workspaceFolder}`.
///
/// Use `FLUREE_HOME=${workspaceFolder}/.fluree` so the MCP server consistently
/// uses the workspace's `.fluree/` directory even if Cursor spawns it with a
/// different working directory.
///
/// See: https://cursor.com/docs/context/mcp
fn cursor_server_entry_json(fluree_bin: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "stdio",
        "command": fluree_bin,
        "args": ["mcp", "serve", "--transport", "stdio"],
        "env": {
            "FLUREE_HOME": "${workspaceFolder}/.fluree"
        }
    })
}

/// Read a JSON file, or return a default object on missing/corrupt files.
fn read_or_default(path: &Path, default: serde_json::Value) -> serde_json::Value {
    match std::fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or(default),
        Err(_) => default,
    }
}

/// Merge our server entry into a JSON object under `top_key`.
fn merge_server_entry(config: &mut serde_json::Value, top_key: &str, fluree_bin: &str) {
    let entry = server_entry_json(fluree_bin);
    if let Some(servers) = config.get_mut(top_key).and_then(|v| v.as_object_mut()) {
        servers.insert("fluree-memory".to_string(), entry);
    } else if let Some(obj) = config.as_object_mut() {
        obj.insert(
            top_key.to_string(),
            serde_json::json!({ "fluree-memory": entry }),
        );
    }
}

/// Write JSON config to a file, creating parent directories if needed.
fn write_config(path: &Path, config: &serde_json::Value) -> CliResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| CliError::Config(format!("failed to create {}: {e}", parent.display())))?;
    }
    std::fs::write(
        path,
        serde_json::to_string_pretty(config).unwrap_or_default(),
    )
    .map_err(|e| CliError::Config(format!("failed to write {}: {e}", path.display())))?;
    Ok(())
}

fn install_claude_code(fluree_bin: &str) -> CliResult<()> {
    // Register via `claude mcp add` (local scope → ~/.claude.json).
    // This works for both the CLI and the VS Code extension.
    // Users who want project-level .mcp.json can add it themselves.
    // Syntax: claude mcp add --transport stdio <name> -- <command> [args...]
    let result = std::process::Command::new("claude")
        .args([
            "mcp",
            "add",
            "--transport",
            "stdio",
            "fluree-memory",
            "--",
            fluree_bin,
            "mcp",
            "serve",
            "--transport",
            "stdio",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match result {
        Ok(status) if status.success() => {
            println!("  Registered via `claude mcp add` (local scope → ~/.claude.json)");
        }
        Ok(_) => {
            eprintln!("  Warning: `claude mcp add` failed. Is Claude Code installed?");
            eprintln!("  You can manually add fluree-memory to .mcp.json or run:");
            eprintln!("    claude mcp add -t stdio fluree-memory -- {fluree_bin} mcp serve --transport stdio");
        }
        Err(_) => {
            eprintln!("  Warning: `claude` not found on PATH.");
            eprintln!("  Install Claude Code, then run:");
            eprintln!("    claude mcp add -t stdio fluree-memory -- {fluree_bin} mcp serve --transport stdio");
        }
    }

    // Append memory instructions to CLAUDE.md if it doesn't already mention us
    let claude_md = Path::new("CLAUDE.md");
    if claude_md.exists() {
        let content = std::fs::read_to_string(claude_md)
            .map_err(|e| CliError::Input(format!("failed to read CLAUDE.md: {e}")))?;
        if !content.contains("fluree memory") && !content.contains("memory_recall") {
            let snippet = "\n\n## Developer Memory\n\n\
                Use the `memory_recall` MCP tool at the start of tasks to retrieve project context.\n\
                Use `memory_add` to store important facts, decisions, and constraints.\n\
                See `fluree memory --help` for CLI usage.\n";
            std::fs::write(claude_md, format!("{content}{snippet}"))
                .map_err(|e| CliError::Config(format!("failed to update CLAUDE.md: {e}")))?;
            println!("  Appended memory instructions to CLAUDE.md");
        }
    }

    Ok(())
}

fn install_cursor(fluree_bin: &str) -> CliResult<()> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".cursor/mcp.json");
    let mut config = read_or_default(&config_path, serde_json::json!({ "mcpServers": {} }));
    // Cursor requires `type: "stdio"` and benefits from workspace-scoped FLUREE_HOME.
    // We install a Cursor-specific server entry rather than the generic `mcpServers` entry.
    let entry = cursor_server_entry_json(fluree_bin);
    if let Some(servers) = config.get_mut("mcpServers").and_then(|v| v.as_object_mut()) {
        servers.insert("fluree-memory".to_string(), entry);
    } else if let Some(obj) = config.as_object_mut() {
        obj.insert(
            "mcpServers".to_string(),
            serde_json::json!({ "fluree-memory": entry }),
        );
    }
    write_config(&config_path, &config)?;
    println!("  Installed: .cursor/mcp.json");

    // Rules file
    let rules_dir = project_root.join(".cursor/rules");
    std::fs::create_dir_all(&rules_dir)
        .map_err(|e| CliError::Config(format!("failed to create .cursor/rules/: {e}")))?;
    let rules_src = include_str!("../../../fluree-db-memory/rules/fluree_rules.md");
    std::fs::write(rules_dir.join("fluree_rules.md"), rules_src)
        .map_err(|e| CliError::Config(format!("failed to write rules: {e}")))?;
    println!("  Installed: .cursor/rules/fluree_rules.md");

    Ok(())
}

fn install_vscode(fluree_bin: &str) -> CliResult<()> {
    // VS Code native MCP uses "servers" key (not "mcpServers")
    // and requires a "type" field on each server entry.
    let project_root = project_root_dir();
    let config_path = project_root.join(".vscode/mcp.json");
    let mut config = read_or_default(&config_path, serde_json::json!({ "servers": {} }));
    let entry = serde_json::json!({
        "type": "stdio",
        "command": fluree_bin,
        "args": ["mcp", "serve", "--transport", "stdio"]
    });
    if let Some(servers) = config.get_mut("servers").and_then(|v| v.as_object_mut()) {
        servers.insert("fluree-memory".to_string(), entry);
    } else if let Some(obj) = config.as_object_mut() {
        obj.insert(
            "servers".to_string(),
            serde_json::json!({ "fluree-memory": entry }),
        );
    }
    write_config(&config_path, &config)?;
    println!("  Installed: .vscode/mcp.json");

    // Rules file
    let vscode_dir = project_root.join(".vscode");
    let rules_src = include_str!("../../../fluree-db-memory/rules/fluree_rules.md");
    std::fs::write(vscode_dir.join("fluree_rules.md"), rules_src)
        .map_err(|e| CliError::Config(format!("failed to write rules: {e}")))?;
    println!("  Installed: .vscode/fluree_rules.md");

    Ok(())
}

fn install_windsurf(fluree_bin: &str) -> CliResult<()> {
    let home = home_dir()
        .ok_or_else(|| CliError::Config("cannot determine home directory".to_string()))?;

    let config_path = home.join(".codeium/windsurf/mcp_config.json");
    let mut config = read_or_default(&config_path, serde_json::json!({ "mcpServers": {} }));
    merge_server_entry(&mut config, "mcpServers", fluree_bin);
    write_config(&config_path, &config)?;
    println!("  Installed: {}", config_path.display());

    Ok(())
}

fn install_zed(fluree_bin: &str) -> CliResult<()> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".zed/settings.json");

    // Zed's settings.json may contain JSONC (comments). If parsing fails,
    // skip with a message rather than clobbering the file.
    let mut config = if config_path.exists() {
        let content = std::fs::read_to_string(&config_path)
            .map_err(|e| CliError::Input(format!("failed to read .zed/settings.json: {e}")))?;
        match serde_json::from_str::<serde_json::Value>(&content) {
            Ok(val) => val,
            Err(_) => {
                eprintln!("  .zed/settings.json contains JSONC (comments) — cannot safely merge.");
                eprintln!(
                    "  Add manually: \"context_servers\": {{ \"fluree-memory\": {{ \"command\": \"{fluree_bin}\", \"args\": [\"mcp\", \"serve\", \"--transport\", \"stdio\"] }} }}"
                );
                return Ok(());
            }
        }
    } else {
        serde_json::json!({})
    };

    merge_server_entry(&mut config, "context_servers", fluree_bin);
    write_config(&config_path, &config)?;
    println!("  Installed: .zed/settings.json");

    Ok(())
}

// ---------------------------------------------------------------------------
// mcp-install (non-interactive escape hatch)
// ---------------------------------------------------------------------------

fn run_mcp_install(ide: Option<&str>) -> CliResult<()> {
    let fluree_bin = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "fluree".to_string());

    let ide = ide.map(String::from).unwrap_or_else(|| {
        detect_ai_tools()
            .into_iter()
            .find(|dt| !dt.already_configured)
            .map(|dt| dt.tool.ide_id().to_string())
            .unwrap_or_else(|| "claude-code".to_string())
    });

    match ide.as_str() {
        "claude-code" => install_claude_code(&fluree_bin),
        // Accept old name for backward compatibility
        "claude-vscode" | "vscode" | "github-copilot" => install_vscode(&fluree_bin),
        "cursor" => install_cursor(&fluree_bin),
        "windsurf" => install_windsurf(&fluree_bin),
        "zed" => install_zed(&fluree_bin),
        other => Err(CliError::Usage(format!(
            "unknown IDE '{other}'; valid: claude-code, vscode, cursor, windsurf, zed"
        ))),
    }
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

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
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

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;

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

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
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
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    store.forget(id).await.map_err(memory_err)?;
    println!("Forgotten: {id}");
    Ok(())
}

async fn run_status(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let status = store.status().await.map_err(memory_err)?;
    print!("{}", fluree_db_memory::format_status_text(&status));
    Ok(())
}

async fn run_export(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
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

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
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
