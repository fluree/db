use crate::cli::MemoryAction;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{
    format_context_paged, MemoryFilter, MemoryInput, MemoryKind, MemoryStore, MemoryUpdate,
    RecallEngine, RecallResult, Scope, SecretDetector,
};
use std::ffi::OsString;
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
// Detection environment — abstracted so tests can drive detection without
// touching the real filesystem, PATH, or home directory.
// ---------------------------------------------------------------------------

/// Filesystem + environment probes used by detection.
///
/// All filesystem access in `detect_ai_tools` goes through this trait so the
/// detection logic can be unit-tested with a `FakeEnv` (see `mod tests`). The
/// production implementation is `RealEnv`, which delegates to `std::env`,
/// `dirs`, and `std::fs`.
trait DetectionEnv {
    /// User home dir (`~`). `None` if unavailable.
    fn home(&self) -> Option<PathBuf>;

    /// Platform user-config dir: `~/.config` on Linux,
    /// `~/Library/Application Support` on macOS, `%APPDATA%` on Windows.
    fn config_dir(&self) -> Option<PathBuf>;

    /// macOS system Applications folder, or `None` off macOS.
    fn system_applications_dir(&self) -> Option<PathBuf>;

    /// Project root: walk up from `current_dir` looking for `.git`.
    /// Falls back to `current_dir` if none is found.
    fn project_root(&self) -> PathBuf;

    /// Current working directory.
    fn current_dir(&self) -> PathBuf;

    /// `current_dir` with symlinks resolved (matches Claude's stored project key).
    fn canonical_current_dir(&self) -> Option<PathBuf>;

    /// Resolve a path with symlinks. `None` if the path can't be canonicalized
    /// (does not exist, permission error, etc.).
    fn canonicalize(&self, path: &Path) -> Option<PathBuf>;

    /// True if `path` exists as a directory.
    fn dir_exists(&self, path: &Path) -> bool;

    /// True if `path` exists (file or dir).
    fn path_exists(&self, path: &Path) -> bool;

    /// Read a file's contents as UTF-8.
    fn read_to_string(&self, path: &Path) -> Option<String>;

    /// True if `name` resolves to an executable on `PATH`. On Windows, honors
    /// `PATHEXT`; otherwise tries the bare name.
    fn executable_on_path(&self, name: &str) -> bool;
}

struct RealEnv;

impl DetectionEnv for RealEnv {
    fn home(&self) -> Option<PathBuf> {
        dirs::home_dir()
    }

    fn config_dir(&self) -> Option<PathBuf> {
        dirs::config_dir()
    }

    fn system_applications_dir(&self) -> Option<PathBuf> {
        if cfg!(target_os = "macos") {
            Some(PathBuf::from("/Applications"))
        } else {
            None
        }
    }

    fn project_root(&self) -> PathBuf {
        walk_to_project_root(&self.current_dir(), |p| self.path_exists(&p.join(".git")))
    }

    fn current_dir(&self) -> PathBuf {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    }

    fn canonical_current_dir(&self) -> Option<PathBuf> {
        std::env::current_dir().and_then(std::fs::canonicalize).ok()
    }

    fn canonicalize(&self, path: &Path) -> Option<PathBuf> {
        std::fs::canonicalize(path).ok()
    }

    fn dir_exists(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn read_to_string(&self, path: &Path) -> Option<String> {
        std::fs::read_to_string(path).ok()
    }

    fn executable_on_path(&self, name: &str) -> bool {
        let Some(path_var) = std::env::var_os("PATH") else {
            return false;
        };
        let exts = path_extensions();
        for dir in std::env::split_paths(&path_var) {
            for ext in &exts {
                let candidate = if ext.is_empty() {
                    dir.join(name)
                } else {
                    let mut name_ext = OsString::from(name);
                    name_ext.push(ext);
                    dir.join(name_ext)
                };
                if candidate.is_file() {
                    return true;
                }
            }
        }
        false
    }
}

/// Extensions to try when probing `PATH` for an executable.
///
/// On Unix, only the bare name. On Windows, the user's `PATHEXT` if set
/// (split on `;` per Windows convention), with a sensible default fallback,
/// plus the bare name (so something like `claude` without an extension still
/// matches a literal file of that name).
fn path_extensions() -> Vec<OsString> {
    if cfg!(target_os = "windows") {
        let mut exts: Vec<OsString> = std::env::var_os("PATHEXT")
            .map(|s| {
                // PATHEXT on Windows is `;`-separated, not the platform path separator,
                // but std::env::split_paths uses `;` on Windows so this is correct.
                std::env::split_paths(&s)
                    .map(OsString::from)
                    .filter(|e| !e.as_os_str().is_empty())
                    .collect()
            })
            .unwrap_or_default();
        if exts.is_empty() {
            exts = [".COM", ".EXE", ".BAT", ".CMD"]
                .into_iter()
                .map(OsString::from)
                .collect();
        }
        // Also try the bare name (matches an extensionless file).
        exts.push(OsString::new());
        exts
    } else {
        vec![OsString::new()]
    }
}

/// Walk up from `start` looking for a directory containing a marker.
/// Used by both `RealEnv::project_root` and the fake env in tests.
fn walk_to_project_root<F: Fn(&Path) -> bool>(start: &Path, marker_at: F) -> PathBuf {
    let mut current = start.to_path_buf();
    loop {
        if marker_at(&current) {
            return current;
        }
        if !current.pop() {
            return start.to_path_buf();
        }
    }
}

// ---------------------------------------------------------------------------
// Per-IDE detection
// ---------------------------------------------------------------------------

/// Probes that say "this IDE is present on this machine, even if never launched."
fn is_claude_code_present(env: &dyn DetectionEnv) -> bool {
    if env.executable_on_path("claude") {
        return true;
    }
    let Some(home) = env.home() else { return false };
    env.dir_exists(&home.join(".claude")) || env.path_exists(&home.join(".claude.json"))
}

fn is_cursor_present(env: &dyn DetectionEnv) -> bool {
    if env.executable_on_path("cursor") {
        return true;
    }
    if macos_app_installed(env, "Cursor.app") {
        return true;
    }
    let home = env.home();
    let config = env.config_dir();
    home.as_ref()
        .is_some_and(|h| env.dir_exists(&h.join(".cursor")))
        || config
            .as_ref()
            .is_some_and(|c| env.dir_exists(&c.join("Cursor")))
}

fn is_vscode_present(env: &dyn DetectionEnv) -> bool {
    if env.executable_on_path("code") {
        return true;
    }
    if macos_app_installed(env, "Visual Studio Code.app") {
        return true;
    }
    let home = env.home();
    let config = env.config_dir();
    home.as_ref()
        .is_some_and(|h| env.dir_exists(&h.join(".vscode")))
        || config
            .as_ref()
            .is_some_and(|c| env.dir_exists(&c.join("Code")))
}

fn is_windsurf_present(env: &dyn DetectionEnv) -> bool {
    if env.executable_on_path("windsurf") {
        return true;
    }
    if macos_app_installed(env, "Windsurf.app") {
        return true;
    }
    let home = env.home();
    let config = env.config_dir();
    home.as_ref()
        .is_some_and(|h| env.dir_exists(&h.join(".codeium").join("windsurf")))
        || config
            .as_ref()
            .is_some_and(|c| env.dir_exists(&c.join("Windsurf")))
}

fn is_zed_present(env: &dyn DetectionEnv) -> bool {
    if env.executable_on_path("zed") {
        return true;
    }
    if macos_app_installed(env, "Zed.app") {
        return true;
    }
    let home = env.home();
    let config = env.config_dir();
    if home
        .as_ref()
        .is_some_and(|h| env.dir_exists(&h.join(".zed")))
    {
        return true;
    }
    // ~/.config/zed (Linux) and ~/Library/Application Support/Zed (macOS via config_dir).
    if config
        .as_ref()
        .is_some_and(|c| env.dir_exists(&c.join("Zed")))
        || config
            .as_ref()
            .is_some_and(|c| env.dir_exists(&c.join("zed")))
    {
        return true;
    }
    home.as_ref()
        .is_some_and(|h| env.dir_exists(&h.join(".config").join("zed")))
}

/// macOS app-bundle probe: checks both `/Applications` and `~/Applications`.
/// No-op off macOS (gated on `system_applications_dir.is_some()`).
fn macos_app_installed(env: &dyn DetectionEnv, app_name: &str) -> bool {
    let Some(system) = env.system_applications_dir() else {
        return false;
    };
    if env.path_exists(&system.join(app_name)) {
        return true;
    }
    if let Some(home) = env.home() {
        if env.path_exists(&home.join("Applications").join(app_name)) {
            return true;
        }
    }
    false
}

/// Read a JSON file via `env` and check if a nested key path exists.
fn json_has_key(env: &dyn DetectionEnv, path: &Path, keys: &[&str]) -> bool {
    let Some(content) = env.read_to_string(path) else {
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

/// Check whether Claude Code's local-scope `~/.claude.json` already lists
/// `fluree-memory` under this project. Claude keys its projects map by the
/// canonical path of the project root, which we must match — not by `cwd`
/// (which may be a subdirectory).
fn claude_code_already_configured(env: &dyn DetectionEnv) -> bool {
    let Some(home) = env.home() else { return false };
    let Some(content) = env.read_to_string(&home.join(".claude.json")) else {
        return false;
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };
    // Try canonical project_root first, then the non-canonical form, then the
    // canonical cwd as a final fallback (covers older Claude Code state where
    // registration happened from a subdir).
    let project_root = env.project_root();
    let canonical_root = env.canonicalize(&project_root);
    let cwd_canonical = env.canonical_current_dir();
    let candidates = [canonical_root, Some(project_root), cwd_canonical];

    let projects = json.get("projects");
    for candidate in candidates.iter().flatten() {
        let key = candidate.display().to_string();
        if projects
            .and_then(|p| p.get(&key))
            .and_then(|proj| proj.get("mcpServers"))
            .and_then(|servers| servers.get("fluree-memory"))
            .is_some()
        {
            return true;
        }
    }
    false
}

fn detect_ai_tools_with(env: &dyn DetectionEnv) -> Vec<DetectedTool> {
    let project_root = env.project_root();
    let mut detected = Vec::new();

    if is_claude_code_present(env) {
        let already = claude_code_already_configured(env)
            || json_has_key(
                env,
                &project_root.join(".mcp.json"),
                &["mcpServers", "fluree-memory"],
            );
        detected.push(DetectedTool {
            tool: AiTool::ClaudeCode,
            already_configured: already,
        });
    }

    if is_cursor_present(env) {
        detected.push(DetectedTool {
            tool: AiTool::Cursor,
            already_configured: json_has_key(
                env,
                &project_root.join(".cursor/mcp.json"),
                &["mcpServers", "fluree-memory"],
            ),
        });
    }

    if is_vscode_present(env) {
        detected.push(DetectedTool {
            tool: AiTool::VsCode,
            already_configured: json_has_key(
                env,
                &project_root.join(".vscode/mcp.json"),
                &["servers", "fluree-memory"],
            ),
        });
    }

    if is_windsurf_present(env) {
        let already = env.home().is_some_and(|h| {
            json_has_key(
                env,
                &h.join(".codeium/windsurf/mcp_config.json"),
                &["mcpServers", "fluree-memory"],
            )
        });
        detected.push(DetectedTool {
            tool: AiTool::Windsurf,
            already_configured: already,
        });
    }

    if is_zed_present(env) {
        detected.push(DetectedTool {
            tool: AiTool::Zed,
            already_configured: json_has_key(
                env,
                &project_root.join(".zed/settings.json"),
                &["context_servers", "fluree-memory"],
            ),
        });
    }

    detected
}

fn detect_ai_tools() -> Vec<DetectedTool> {
    detect_ai_tools_with(&RealEnv)
}

fn project_root_dir() -> PathBuf {
    RealEnv.project_root()
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
    let mut skipped_count = 0usize;
    for dt in &to_install {
        let confirmed = yes
            || prompt_yn(&format!(
                "Install MCP config for {}?",
                dt.tool.display_name()
            ));

        if confirmed {
            match install_tool(dt.tool, &fluree_bin) {
                Ok(InstallOutcome::Installed) => {
                    installed_count += 1;
                }
                Ok(InstallOutcome::Skipped) => {
                    skipped_count += 1;
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
    if skipped_count > 0 {
        println!(
            "Skipped {} tool{} (see manual snippet above).",
            skipped_count,
            if skipped_count == 1 { "" } else { "s" }
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-tool installation
// ---------------------------------------------------------------------------

/// Outcome of an attempted MCP install. The "installed" tally in run_init
/// only increments for `Installed`; `Skipped` covers the case where we
/// refused to overwrite a JSONC/corrupt config and printed a manual snippet.
#[derive(Debug, PartialEq, Eq)]
enum InstallOutcome {
    Installed,
    Skipped,
}

fn install_tool(tool: AiTool, fluree_bin: &str) -> CliResult<InstallOutcome> {
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

/// Outcome of loading an MCP config file for merge.
enum LoadedConfig {
    /// File was missing or empty — start from `default`.
    Default(serde_json::Value),
    /// File parsed cleanly — merge into the existing value.
    Parsed(serde_json::Value),
    /// File exists but is unsafe to overwrite — either it contains
    /// non-JSON content (JSONC with comments, hand-edited corruption,
    /// invalid UTF-8) or we couldn't read it for some non-missing reason
    /// (permission denied, I/O error). The caller must NOT overwrite —
    /// emit a manual-install snippet instead. `reason` is shown to the user.
    Unsafe { reason: String },
}

/// Load an MCP config file. Refuses to silently default a corrupt/JSONC file
/// or a file we can't read cleanly, so we never clobber a user's existing
/// IDE config.
fn load_config(path: &Path, default: serde_json::Value) -> LoadedConfig {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return LoadedConfig::Default(default);
        }
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            // File exists but is not valid UTF-8. Don't risk overwriting it.
            return LoadedConfig::Unsafe {
                reason: "file is not valid UTF-8".to_string(),
            };
        }
        Err(e) => {
            return LoadedConfig::Unsafe {
                reason: format!("cannot read file: {e}"),
            };
        }
    };
    if content.trim().is_empty() {
        return LoadedConfig::Default(default);
    }
    match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(v) => LoadedConfig::Parsed(v),
        Err(_) => LoadedConfig::Unsafe {
            reason: "file is not valid JSON (comments or hand-edits?)".to_string(),
        },
    }
}

/// Emit a "we won't touch your config" message with a manual snippet.
fn warn_unsafe_config(path: &Path, reason: &str, snippet: &str) {
    eprintln!("  {} {} — refusing to overwrite.", path.display(), reason);
    eprintln!("  Add this entry manually:");
    for line in snippet.lines() {
        eprintln!("    {line}");
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

fn install_claude_code(fluree_bin: &str) -> CliResult<InstallOutcome> {
    // Register via `claude mcp add` (local scope → ~/.claude.json).
    // This works for both the CLI and the VS Code extension.
    // Users who want project-level .mcp.json can add it themselves.
    // Syntax: claude mcp add --transport stdio <name> -- <command> [args...]
    //
    // Spawn `claude` from project_root so it keys its local-scope `projects`
    // map by the repo root, not the (possibly nested) cwd we were invoked in.
    let project_root = project_root_dir();
    let result = std::process::Command::new("claude")
        .current_dir(&project_root)
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

    let registered = match result {
        Ok(status) if status.success() => {
            println!("  Registered via `claude mcp add` (local scope → ~/.claude.json)");
            true
        }
        Ok(_) => {
            eprintln!("  Warning: `claude mcp add` failed. Is Claude Code installed?");
            eprintln!("  You can manually add fluree-memory to .mcp.json or run:");
            eprintln!("    claude mcp add -t stdio fluree-memory -- {fluree_bin} mcp serve --transport stdio");
            false
        }
        Err(_) => {
            eprintln!("  Warning: `claude` not found on PATH.");
            eprintln!("  Install Claude Code, then run:");
            eprintln!("    claude mcp add -t stdio fluree-memory -- {fluree_bin} mcp serve --transport stdio");
            false
        }
    };

    // Append memory instructions to CLAUDE.md at the project root if present
    // and not already configured. Using project_root (not cwd) keeps behavior
    // consistent with the other IDE installers.
    let claude_md = project_root.join("CLAUDE.md");
    if claude_md.exists() {
        let content = std::fs::read_to_string(&claude_md)
            .map_err(|e| CliError::Input(format!("failed to read CLAUDE.md: {e}")))?;
        if !content.contains("fluree memory") && !content.contains("memory_recall") {
            let snippet = "\n\n## Developer Memory\n\n\
                Use the `memory_recall` MCP tool at the start of tasks to retrieve project context.\n\
                Use `memory_add` to store important facts, decisions, and constraints.\n\
                See `fluree memory --help` for CLI usage.\n";
            std::fs::write(&claude_md, format!("{content}{snippet}"))
                .map_err(|e| CliError::Config(format!("failed to update CLAUDE.md: {e}")))?;
            println!("  Appended memory instructions to CLAUDE.md");
        }
    }

    Ok(if registered {
        InstallOutcome::Installed
    } else {
        InstallOutcome::Skipped
    })
}

fn install_cursor(fluree_bin: &str) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".cursor/mcp.json");
    // Cursor requires `type: "stdio"` and benefits from workspace-scoped FLUREE_HOME.
    // We install a Cursor-specific server entry rather than the generic `mcpServers` entry.
    let entry = cursor_server_entry_json(fluree_bin);
    let mut config = match load_config(&config_path, serde_json::json!({ "mcpServers": {} })) {
        LoadedConfig::Default(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            let snippet = format!(
                "\"mcpServers\": {{\n  \"fluree-memory\": {}\n}}",
                serde_json::to_string_pretty(&entry).unwrap_or_default()
            );
            warn_unsafe_config(&config_path, &reason, &snippet);
            return Ok(InstallOutcome::Skipped);
        }
    };
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

    Ok(InstallOutcome::Installed)
}

fn install_vscode(fluree_bin: &str) -> CliResult<InstallOutcome> {
    // VS Code native MCP uses "servers" key (not "mcpServers")
    // and requires a "type" field on each server entry.
    let project_root = project_root_dir();
    let config_path = project_root.join(".vscode/mcp.json");
    let entry = serde_json::json!({
        "type": "stdio",
        "command": fluree_bin,
        "args": ["mcp", "serve", "--transport", "stdio"]
    });
    let mut config = match load_config(&config_path, serde_json::json!({ "servers": {} })) {
        LoadedConfig::Default(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            let snippet = format!(
                "\"servers\": {{\n  \"fluree-memory\": {}\n}}",
                serde_json::to_string_pretty(&entry).unwrap_or_default()
            );
            warn_unsafe_config(&config_path, &reason, &snippet);
            return Ok(InstallOutcome::Skipped);
        }
    };
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

    Ok(InstallOutcome::Installed)
}

fn install_windsurf(fluree_bin: &str) -> CliResult<InstallOutcome> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::Config("cannot determine home directory".to_string()))?;

    let config_path = home.join(".codeium/windsurf/mcp_config.json");
    let mut config = match load_config(&config_path, serde_json::json!({ "mcpServers": {} })) {
        LoadedConfig::Default(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            let entry = server_entry_json(fluree_bin);
            let snippet = format!(
                "\"mcpServers\": {{\n  \"fluree-memory\": {}\n}}",
                serde_json::to_string_pretty(&entry).unwrap_or_default()
            );
            warn_unsafe_config(&config_path, &reason, &snippet);
            return Ok(InstallOutcome::Skipped);
        }
    };
    merge_server_entry(&mut config, "mcpServers", fluree_bin);
    write_config(&config_path, &config)?;
    println!("  Installed: {}", config_path.display());

    Ok(InstallOutcome::Installed)
}

fn install_zed(fluree_bin: &str) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".zed/settings.json");

    // Zed's settings.json is commonly JSONC (comments). load_config refuses
    // to overwrite unparseable JSON so we don't clobber it.
    let mut config = match load_config(&config_path, serde_json::json!({})) {
        LoadedConfig::Default(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            let entry = server_entry_json(fluree_bin);
            let snippet = format!(
                "\"context_servers\": {{\n  \"fluree-memory\": {}\n}}",
                serde_json::to_string_pretty(&entry).unwrap_or_default()
            );
            warn_unsafe_config(&config_path, &reason, &snippet);
            return Ok(InstallOutcome::Skipped);
        }
    };

    merge_server_entry(&mut config, "context_servers", fluree_bin);
    write_config(&config_path, &config)?;
    println!("  Installed: .zed/settings.json");

    Ok(InstallOutcome::Installed)
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
        "claude-code" => install_claude_code(&fluree_bin).map(|_| ()),
        // Accept old name for backward compatibility
        "claude-vscode" | "vscode" | "github-copilot" => install_vscode(&fluree_bin).map(|_| ()),
        "cursor" => install_cursor(&fluree_bin).map(|_| ()),
        "windsurf" => install_windsurf(&fluree_bin).map(|_| ()),
        "zed" => install_zed(&fluree_bin).map(|_| ()),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// In-memory `DetectionEnv` used to drive detection logic without touching
    /// the real filesystem, PATH, or home directory.
    #[derive(Default)]
    struct FakeEnv {
        home: Option<PathBuf>,
        config_dir: Option<PathBuf>,
        system_apps: Option<PathBuf>,
        current_dir: PathBuf,
        canonical_current_dir: Option<PathBuf>,
        dirs: HashSet<PathBuf>,
        files: HashMap<PathBuf, String>,
        path_executables: HashSet<String>,
    }

    impl FakeEnv {
        fn add_dir(&mut self, p: impl Into<PathBuf>) -> &mut Self {
            let p = p.into();
            // Add all ancestors so `dir_exists` walks behave naturally.
            let mut cur = Some(p.as_path());
            while let Some(d) = cur {
                self.dirs.insert(d.to_path_buf());
                cur = d.parent();
            }
            self
        }
        fn add_file(&mut self, p: impl Into<PathBuf>, content: impl Into<String>) -> &mut Self {
            let p = p.into();
            if let Some(parent) = p.parent() {
                self.add_dir(parent.to_path_buf());
            }
            self.files.insert(p, content.into());
            self
        }
        fn add_executable(&mut self, name: impl Into<String>) -> &mut Self {
            self.path_executables.insert(name.into());
            self
        }
    }

    impl DetectionEnv for FakeEnv {
        fn home(&self) -> Option<PathBuf> {
            self.home.clone()
        }
        fn config_dir(&self) -> Option<PathBuf> {
            self.config_dir.clone()
        }
        fn system_applications_dir(&self) -> Option<PathBuf> {
            self.system_apps.clone()
        }
        fn project_root(&self) -> PathBuf {
            walk_to_project_root(&self.current_dir, |p| self.path_exists(&p.join(".git")))
        }
        fn current_dir(&self) -> PathBuf {
            self.current_dir.clone()
        }
        fn canonical_current_dir(&self) -> Option<PathBuf> {
            self.canonical_current_dir.clone()
        }
        fn canonicalize(&self, path: &Path) -> Option<PathBuf> {
            // Identity in tests — exercise the candidate fallback explicitly
            // by populating canonical_current_dir or the JSON keys.
            if self.path_exists(path) {
                Some(path.to_path_buf())
            } else {
                None
            }
        }
        fn dir_exists(&self, path: &Path) -> bool {
            self.dirs.contains(path)
        }
        fn path_exists(&self, path: &Path) -> bool {
            self.dirs.contains(path) || self.files.contains_key(path)
        }
        fn read_to_string(&self, path: &Path) -> Option<String> {
            self.files.get(path).cloned()
        }
        fn executable_on_path(&self, name: &str) -> bool {
            self.path_executables.contains(name)
        }
    }

    fn empty_env_with_repo(root: &Path) -> FakeEnv {
        let mut env = FakeEnv {
            current_dir: root.to_path_buf(),
            ..Default::default()
        };
        env.add_dir(root.join(".git"));
        env
    }

    // -- project_root walk-up -----------------------------------------------

    #[test]
    fn project_root_walks_up_to_git_marker() {
        let repo = PathBuf::from("/home/u/proj");
        let nested = repo.join("crates/fluree-db-memory/src");
        let mut env = empty_env_with_repo(&repo);
        env.current_dir = nested.clone();
        env.add_dir(&nested);

        assert_eq!(env.project_root(), repo);
    }

    #[test]
    fn project_root_falls_back_to_cwd_when_no_marker() {
        let cwd = PathBuf::from("/tmp/loose");
        let mut env = FakeEnv {
            current_dir: cwd.clone(),
            ..Default::default()
        };
        env.add_dir(&cwd);
        assert_eq!(env.project_root(), cwd);
    }

    // -- per-IDE presence probes --------------------------------------------

    #[test]
    fn claude_present_via_path() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.add_executable("claude");
        assert!(is_claude_code_present(&env));
    }

    #[test]
    fn claude_present_via_home_marker() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.add_dir("/h/.claude");
        assert!(is_claude_code_present(&env));
    }

    #[test]
    fn claude_absent_when_no_signal() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.add_dir("/h");
        assert!(!is_claude_code_present(&env));
    }

    #[test]
    fn vscode_present_via_config_dir() {
        // Linux launched-once marker is ~/.config/Code, not ~/.vscode.
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Code");
        assert!(is_vscode_present(&env));
    }

    #[test]
    fn cursor_present_via_config_dir() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Cursor");
        assert!(is_cursor_present(&env));
    }

    #[test]
    fn windsurf_present_via_config_dir() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Windsurf");
        assert!(is_windsurf_present(&env));
    }

    #[test]
    fn macos_app_probe_is_noop_when_system_dir_unset() {
        // system_applications_dir is None on non-macOS; even if a path like
        // /Users/u/Applications/Foo.app happens to exist, we don't probe it.
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/h"));
        env.system_apps = None; // simulating non-macOS
        env.add_dir("/h/Applications/Cursor.app");
        assert!(!macos_app_installed(&env, "Cursor.app"));
    }

    #[test]
    fn path_extensions_unix_returns_bare_name_only() {
        // The function is platform-conditional via cfg!, so we can only assert
        // the host's behavior. The Unix invariant: single empty extension.
        if !cfg!(target_os = "windows") {
            let exts = path_extensions();
            assert_eq!(exts.len(), 1);
            assert!(exts[0].is_empty());
        }
    }

    #[test]
    fn macos_app_probe_checks_both_system_and_user_applications() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.system_apps = Some(PathBuf::from("/Applications"));
        env.home = Some(PathBuf::from("/Users/u"));

        // System install
        env.add_dir("/Applications/Cursor.app");
        assert!(macos_app_installed(&env, "Cursor.app"));

        // User-local install — wipe the system install and try ~/Applications.
        env.dirs.clear();
        env.add_dir("/Users/u/Applications/Cursor.app");
        assert!(macos_app_installed(&env, "Cursor.app"));
    }

    // -- claude_code_already_configured -------------------------------------

    #[test]
    fn claude_already_configured_matches_project_root_from_nested_cwd() {
        let repo = PathBuf::from("/home/u/proj");
        let nested = repo.join("crates/x");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/home/u"));
        env.current_dir = nested.clone();
        env.add_dir(&nested);

        // Claude stores config keyed by the repo root, not the nested cwd.
        let claude_json = serde_json::json!({
            "projects": {
                repo.display().to_string(): {
                    "mcpServers": { "fluree-memory": { "command": "fluree" } }
                }
            }
        });
        env.add_file("/home/u/.claude.json", claude_json.to_string());

        assert!(claude_code_already_configured(&env));
    }

    #[test]
    fn claude_already_configured_returns_false_when_other_project() {
        let repo = PathBuf::from("/home/u/proj");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/home/u"));

        let claude_json = serde_json::json!({
            "projects": {
                "/home/u/other": {
                    "mcpServers": { "fluree-memory": { "command": "fluree" } }
                }
            }
        });
        env.add_file("/home/u/.claude.json", claude_json.to_string());

        assert!(!claude_code_already_configured(&env));
    }

    // -- detect_ai_tools end-to-end -----------------------------------------

    #[test]
    fn detect_returns_empty_on_empty_env() {
        let repo = PathBuf::from("/r");
        let env = empty_env_with_repo(&repo);
        assert!(detect_ai_tools_with(&env).is_empty());
    }

    #[test]
    fn detect_finds_claude_via_path_only() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.add_executable("claude");
        let detected = detect_ai_tools_with(&env);
        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].tool, AiTool::ClaudeCode);
        assert!(!detected[0].already_configured);
    }

    #[test]
    fn detect_flags_already_configured_via_project_mcp_json() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.add_executable("claude");
        env.add_file(
            repo.join(".mcp.json"),
            r#"{"mcpServers":{"fluree-memory":{"command":"fluree"}}}"#,
        );
        let detected = detect_ai_tools_with(&env);
        assert_eq!(detected.len(), 1);
        assert!(detected[0].already_configured);
    }

    // -- load_config / parse-failure handling -------------------------------

    #[test]
    fn load_config_returns_default_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.json");
        let result = load_config(&path, serde_json::json!({"k": 1}));
        assert!(matches!(result, LoadedConfig::Default(v) if v == serde_json::json!({"k": 1})));
    }

    #[test]
    fn load_config_returns_default_when_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::write(&path, "   \n").unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(matches!(result, LoadedConfig::Default(_)));
    }

    #[test]
    fn load_config_returns_unsafe_for_jsonc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        // JSONC with comments — common in Zed, VS Code user settings, etc.
        std::fs::write(&path, "// hi\n{\"a\": 1}\n").unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(matches!(result, LoadedConfig::Unsafe { .. }));
    }

    #[test]
    fn load_config_returns_unsafe_for_invalid_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-utf8.json");
        // Lone 0xFF byte — invalid UTF-8 sequence.
        std::fs::write(&path, [0xFFu8, 0xFE, 0x00, 0x01]).unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(matches!(result, LoadedConfig::Unsafe { .. }));
    }

    #[test]
    fn load_config_returns_parsed_for_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("valid.json");
        std::fs::write(&path, r#"{"servers":{}}"#).unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(
            matches!(result, LoadedConfig::Parsed(v) if v == serde_json::json!({"servers":{}}))
        );
    }
}
