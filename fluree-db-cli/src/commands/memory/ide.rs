//! IDE detection and MCP configuration for `fluree memory init` /
//! `fluree memory mcp-install`.
//!
//! All IDE-specific logic — filesystem/env probing, JSON merging, per-IDE
//! install flow — lives here so `memory.rs` can stay focused on dispatch
//! and memory CRUD.
//!
//! The entry points used by `memory.rs` are [`run_mcp_phase`] (called from
//! `memory init` after the store is created) and [`run_mcp_install`]
//! (called from `memory mcp-install`).

use crate::error::{CliError, CliResult};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// AI tool model
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

    fn detection_spec(self) -> &'static IdeDetectionSpec {
        match self {
            AiTool::ClaudeCode => &CLAUDE_CODE,
            AiTool::Cursor => &CURSOR,
            AiTool::VsCode => &VSCODE,
            AiTool::Windsurf => &WINDSURF,
            AiTool::Zed => &ZED,
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

/// Filesystem + environment probes used by detection. All filesystem access
/// in [`detect_ai_tools_with`] goes through this trait so the detection
/// logic can be unit-tested with a `FakeEnv` (see the `tests` module).
/// The production implementation is [`RealEnv`].
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

    /// Resolve a path with symlinks. `None` if the path can't be canonicalized.
    fn canonicalize(&self, path: &Path) -> Option<PathBuf>;

    /// True if `path` exists (file or dir).
    fn path_exists(&self, path: &Path) -> bool;

    /// Read a file's contents as UTF-8.
    fn read_to_string(&self, path: &Path) -> Option<String>;

    /// True if `name` resolves to an executable on `PATH`. On Windows,
    /// honors `PATHEXT`; otherwise tries the bare name.
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
/// (with a sensible default fallback) plus the bare name (so something
/// like `claude` without an extension still matches a literal file).
fn path_extensions() -> Vec<OsString> {
    if cfg!(target_os = "windows") {
        let mut exts: Vec<OsString> = std::env::var_os("PATHEXT")
            .map(|s| {
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
        exts.push(OsString::new());
        exts
    } else {
        vec![OsString::new()]
    }
}

/// Walk up from `start` looking for a directory containing a marker.
/// Used by `RealEnv::project_root` (and the fake env in tests).
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

/// Resolve the project root via the production environment. Available to
/// `memory.rs` callers that need it without constructing a `RealEnv` directly.
pub(super) fn project_root_dir() -> PathBuf {
    RealEnv.project_root()
}

// ---------------------------------------------------------------------------
// Per-IDE detection specs (data-driven probes)
// ---------------------------------------------------------------------------

/// What signals to check for a given IDE. `path_exists` matches both files
/// and directories, so `home_markers` / `config_markers` may name either
/// (e.g. Claude Code's `.claude.json` file vs. its `.claude/` dir).
struct IdeDetectionSpec {
    /// Binary name to probe on `PATH`.
    binary: &'static str,
    /// macOS `.app` bundle name (probed in both `/Applications` and
    /// `~/Applications`). `None` for CLI-only tools like Claude Code.
    macos_app: Option<&'static str>,
    /// Paths relative to `home()` to probe.
    home_markers: &'static [&'static str],
    /// Paths relative to `config_dir()` to probe.
    config_markers: &'static [&'static str],
}

const CLAUDE_CODE: IdeDetectionSpec = IdeDetectionSpec {
    binary: "claude",
    macos_app: None,
    home_markers: &[".claude", ".claude.json"],
    config_markers: &[],
};

const CURSOR: IdeDetectionSpec = IdeDetectionSpec {
    binary: "cursor",
    macos_app: Some("Cursor.app"),
    home_markers: &[".cursor"],
    config_markers: &["Cursor"],
};

const VSCODE: IdeDetectionSpec = IdeDetectionSpec {
    binary: "code",
    macos_app: Some("Visual Studio Code.app"),
    home_markers: &[".vscode"],
    config_markers: &["Code"],
};

const WINDSURF: IdeDetectionSpec = IdeDetectionSpec {
    binary: "windsurf",
    macos_app: Some("Windsurf.app"),
    home_markers: &[".codeium/windsurf"],
    config_markers: &["Windsurf"],
};

const ZED: IdeDetectionSpec = IdeDetectionSpec {
    binary: "zed",
    macos_app: Some("Zed.app"),
    // `.config/zed` is the Linux convention even though config_dir() on
    // Linux is `~/.config` (so it'd also match via config_markers).
    // Listing it here makes the spec work on a non-default config_dir.
    home_markers: &[".zed", ".config/zed"],
    // Linux uses `zed` (lowercase), macOS Application Support uses `Zed`.
    config_markers: &["Zed", "zed"],
};

/// True if any of the spec's signals fire.
fn is_ide_present(env: &dyn DetectionEnv, spec: &IdeDetectionSpec) -> bool {
    if env.executable_on_path(spec.binary) {
        return true;
    }
    if let Some(app) = spec.macos_app {
        if macos_app_installed(env, app) {
            return true;
        }
    }
    if let Some(home) = env.home() {
        for m in spec.home_markers {
            if env.path_exists(&home.join(m)) {
                return true;
            }
        }
    }
    if let Some(config) = env.config_dir() {
        for m in spec.config_markers {
            if env.path_exists(&config.join(m)) {
                return true;
            }
        }
    }
    false
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
/// canonical path of the project root, so we must match the root — not the
/// cwd (which may be a subdirectory).
fn claude_code_already_configured(env: &dyn DetectionEnv) -> bool {
    let Some(home) = env.home() else { return false };
    let Some(content) = env.read_to_string(&home.join(".claude.json")) else {
        return false;
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) else {
        return false;
    };
    // Try canonical project_root, then the non-canonical form, then the
    // canonical cwd as a final fallback (covers older Claude Code state
    // where registration happened from a subdir).
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

/// Where the per-IDE "already configured" check should look. Returned as a
/// list of (path, JSON key path) pairs; any match counts.
fn already_configured_probes(
    tool: AiTool,
    env: &dyn DetectionEnv,
) -> Vec<(PathBuf, &'static [&'static str])> {
    let project_root = env.project_root();
    match tool {
        AiTool::ClaudeCode => {
            // .mcp.json (project-scope) is one signal; the local-scope
            // ~/.claude.json check is handled separately because it needs
            // the project_root as a *value* (not a static key path).
            vec![(
                project_root.join(".mcp.json"),
                &["mcpServers", "fluree-memory"],
            )]
        }
        AiTool::Cursor => vec![(
            project_root.join(".cursor/mcp.json"),
            &["mcpServers", "fluree-memory"],
        )],
        AiTool::VsCode => vec![(
            project_root.join(".vscode/mcp.json"),
            &["servers", "fluree-memory"],
        )],
        AiTool::Windsurf => match env.home() {
            Some(h) => vec![(
                h.join(".codeium/windsurf/mcp_config.json"),
                &["mcpServers", "fluree-memory"],
            )],
            None => vec![],
        },
        AiTool::Zed => vec![(
            project_root.join(".zed/settings.json"),
            &["context_servers", "fluree-memory"],
        )],
    }
}

fn is_already_configured(tool: AiTool, env: &dyn DetectionEnv) -> bool {
    if tool == AiTool::ClaudeCode && claude_code_already_configured(env) {
        return true;
    }
    for (path, keys) in already_configured_probes(tool, env) {
        if json_has_key(env, &path, keys) {
            return true;
        }
    }
    false
}

const ALL_TOOLS: &[AiTool] = &[
    AiTool::ClaudeCode,
    AiTool::Cursor,
    AiTool::VsCode,
    AiTool::Windsurf,
    AiTool::Zed,
];

fn detect_ai_tools_with(env: &dyn DetectionEnv) -> Vec<DetectedTool> {
    ALL_TOOLS
        .iter()
        .copied()
        .filter(|t| is_ide_present(env, t.detection_spec()))
        .map(|tool| DetectedTool {
            tool,
            already_configured: is_already_configured(tool, env),
        })
        .collect()
}

fn detect_ai_tools() -> Vec<DetectedTool> {
    detect_ai_tools_with(&RealEnv)
}

// ---------------------------------------------------------------------------
// Interactive prompting
// ---------------------------------------------------------------------------

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
// Install flow
// ---------------------------------------------------------------------------

/// Outcome of an attempted MCP install. The "installed" tally in
/// [`run_mcp_phase`] only increments for [`InstallOutcome::Installed`];
/// [`InstallOutcome::ManualRequired`] covers the cases where we refused
/// to clobber a JSONC/corrupt config and printed a manual snippet, or
/// where `claude mcp add` was not available.
#[derive(Debug, PartialEq, Eq)]
enum InstallOutcome {
    Installed,
    ManualRequired,
}

fn install_tool(tool: AiTool, fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    match tool {
        AiTool::ClaudeCode => install_claude_code(fluree_bin, sel),
        AiTool::Cursor => install_cursor(fluree_bin, sel),
        AiTool::VsCode => install_vscode(fluree_bin, sel),
        AiTool::Windsurf => install_windsurf(fluree_bin, sel),
        AiTool::Zed => install_zed(fluree_bin, sel),
    }
}

/// CLI args that launch each MCP server over stdio. `fluree-memory` serves the
/// developer memory store; `fluree-docs` serves the embedded documentation.
const MEMORY_ARGS: &[&str] = &["mcp", "serve", "--transport", "stdio"];
const DOCS_ARGS: &[&str] = &["docs", "serve", "--transport", "stdio"];

/// Which Fluree MCP server(s) to register.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ServerSel {
    Memory,
    Docs,
    All,
}

impl ServerSel {
    /// Parse the `--server` value (`memory` / `docs` / `all`).
    fn parse(s: &str) -> Option<Self> {
        match s {
            "memory" => Some(Self::Memory),
            "docs" => Some(Self::Docs),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn wants_memory(self) -> bool {
        matches!(self, Self::Memory | Self::All)
    }

    fn wants_docs(self) -> bool {
        matches!(self, Self::Docs | Self::All)
    }
}

/// Build one stdio MCP server entry. `typed` adds the `"type": "stdio"` field
/// that Cursor / VS Code expect; `env` attaches an `env` object (used only to
/// pin the memory store's `FLUREE_HOME` to the workspace).
fn stdio_entry(
    fluree_bin: &str,
    args: &[&str],
    typed: bool,
    env: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if typed {
        obj.insert("type".to_string(), serde_json::json!("stdio"));
    }
    obj.insert("command".to_string(), serde_json::json!(fluree_bin));
    obj.insert("args".to_string(), serde_json::json!(args));
    if let Some(env) = env {
        obj.insert("env".to_string(), env);
    }
    serde_json::Value::Object(obj)
}

/// The `(server-name, entry)` pairs to register for a given IDE. Both Fluree
/// MCP servers are registered together so a single install wires up memory +
/// docs. `typed` adds `"type": "stdio"` (Cursor/VS Code). `memory_env` pins the
/// memory store's `FLUREE_HOME` to the workspace (Cursor only); the docs server
/// is stateless (embedded corpus) so it never needs an env.
fn fluree_server_entries(
    fluree_bin: &str,
    typed: bool,
    memory_env: bool,
    sel: ServerSel,
) -> Vec<(&'static str, serde_json::Value)> {
    let mut entries = Vec::new();
    if sel.wants_memory() {
        let env = memory_env
            .then(|| serde_json::json!({ "FLUREE_HOME": "${workspaceFolder}/.fluree" }));
        entries.push((
            "fluree-memory",
            stdio_entry(fluree_bin, MEMORY_ARGS, typed, env),
        ));
    }
    if sel.wants_docs() {
        entries.push(("fluree-docs", stdio_entry(fluree_bin, DOCS_ARGS, typed, None)));
    }
    entries
}

/// Outcome of loading an MCP config file for merge.
enum LoadedConfig {
    /// File was missing or empty — start from `default`.
    MissingOrEmpty(serde_json::Value),
    /// File parsed cleanly — merge into the existing value.
    Parsed(serde_json::Value),
    /// File exists but is unsafe to overwrite — either it contains
    /// non-JSON content (JSONC with comments, hand-edited corruption,
    /// invalid UTF-8) or we couldn't read it cleanly (permission denied,
    /// I/O error). Caller must NOT overwrite — emit a manual-install
    /// snippet instead. `reason` is shown to the user.
    Unsafe { reason: String },
}

/// Load an MCP config file. Refuses to silently default a corrupt/JSONC
/// file or one we can't read cleanly, so we never clobber a user's
/// existing IDE config.
fn load_config(path: &Path, default: serde_json::Value) -> LoadedConfig {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return LoadedConfig::MissingOrEmpty(default);
        }
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
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
        return LoadedConfig::MissingOrEmpty(default);
    }
    match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(v) if v.is_object() => LoadedConfig::Parsed(v),
        // Valid JSON, but not a top-level object: we don't know how to
        // merge `"fluree-memory": {...}` into an array, scalar, or null,
        // and overwriting would clobber whatever the user put there.
        Ok(_) => LoadedConfig::Unsafe {
            reason: "file is JSON but not an object at the root".to_string(),
        },
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

/// Merge our server entries into a JSON object under `top_key`, creating the
/// `top_key` object if absent. Existing entries (other servers, or a prior
/// install) are preserved; our named entries are inserted or overwritten.
fn merge_mcp_entries(
    config: &mut serde_json::Value,
    top_key: &str,
    entries: &[(&str, serde_json::Value)],
) {
    if !config.get(top_key).is_some_and(serde_json::Value::is_object) {
        if let Some(obj) = config.as_object_mut() {
            obj.insert(top_key.to_string(), serde_json::json!({}));
        }
    }
    if let Some(servers) = config.get_mut(top_key).and_then(|v| v.as_object_mut()) {
        for (name, entry) in entries {
            servers.insert((*name).to_string(), entry.clone());
        }
    }
}

/// Write a JSON config file, creating parent directories if needed.
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

/// Parameters for [`install_json_mcp_config`], the shared install flow
/// used by Cursor / VS Code / Windsurf / Zed.
struct JsonMcpInstall<'a> {
    config_path: &'a Path,
    /// Top-level key under which the server entries are merged
    /// (`mcpServers` for Cursor/Windsurf, `servers` for VS Code,
    /// `context_servers` for Zed).
    top_key: &'a str,
    /// Default JSON value to use when the file is missing or empty.
    default: serde_json::Value,
    /// The `(name, entry)` pairs to insert under `top_key` (e.g. `fluree-memory`
    /// and `fluree-docs`).
    entries: Vec<(&'static str, serde_json::Value)>,
}

/// Common install shape: load, merge, refuse-on-unsafe, write. Used by
/// every JSON-config-based IDE. The caller is responsible for any
/// follow-up side effects (e.g. rules-file install) and should gate
/// them on `Ok(InstallOutcome::Installed)`.
fn install_json_mcp_config(opts: JsonMcpInstall<'_>) -> CliResult<InstallOutcome> {
    let manual_snippet = || {
        let inner = opts
            .entries
            .iter()
            .map(|(name, entry)| {
                format!(
                    "  \"{name}\": {}",
                    serde_json::to_string_pretty(entry).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");
        format!("\"{}\": {{\n{inner}\n}}", opts.top_key)
    };

    let mut config = match load_config(opts.config_path, opts.default) {
        LoadedConfig::MissingOrEmpty(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            warn_unsafe_config(opts.config_path, &reason, &manual_snippet());
            return Ok(InstallOutcome::ManualRequired);
        }
    };

    // The root is guaranteed to be an object by load_config, but the
    // existing value under `top_key` (if any) might not be. Refuse rather
    // than clobber — e.g. don't silently turn `{"mcpServers": []}` into
    // `{"mcpServers": {"fluree-memory": ...}}`.
    if let Some(existing) = config.get(opts.top_key) {
        if !existing.is_object() {
            let reason = format!("\"{}\" exists but is not a JSON object", opts.top_key);
            warn_unsafe_config(opts.config_path, &reason, &manual_snippet());
            return Ok(InstallOutcome::ManualRequired);
        }
    }

    merge_mcp_entries(&mut config, opts.top_key, &opts.entries);
    write_config(opts.config_path, &config)?;
    let names = opts
        .entries
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    println!("  Installed: {} ({names})", opts.config_path.display());
    Ok(InstallOutcome::Installed)
}

/// Write the bundled `fluree_rules.md` into `dir`. Used by IDEs whose
/// agent can be told to read a per-workspace rules file (Cursor, VS Code).
///
/// Best-effort: if creating the directory or writing the file fails, this
/// prints a warning and returns. The rules file is a hint for the agent,
/// not a requirement for MCP to work; we don't want a rules-file failure
/// to mask a successful MCP config install in the per-tool summary.
fn install_rules_file(dir: &Path) {
    if let Err(e) = std::fs::create_dir_all(dir) {
        eprintln!(
            "  warning: rules file not installed — failed to create {}: {e}",
            dir.display()
        );
        return;
    }
    let rules_src = include_str!("../../../../fluree-db-memory/rules/fluree_rules.md");
    let target = dir.join("fluree_rules.md");
    if let Err(e) = std::fs::write(&target, rules_src) {
        eprintln!(
            "  warning: rules file not installed — failed to write {}: {e}",
            target.display()
        );
        return;
    }
    println!("  Installed: {}", target.display());
}

// ---------------------------------------------------------------------------
// Per-IDE install
// ---------------------------------------------------------------------------

fn install_claude_code(fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    // Register via `claude mcp add` (local scope → ~/.claude.json). Spawn
    // `claude` from project_root so it keys its `projects` map by the
    // repo root, not the (possibly nested) cwd we were invoked from.
    let project_root = project_root_dir();
    let mut ok = true;
    if sel.wants_memory() {
        ok &= claude_mcp_add(&project_root, "fluree-memory", fluree_bin, MEMORY_ARGS);
    }
    if sel.wants_docs() {
        ok &= claude_mcp_add(&project_root, "fluree-docs", fluree_bin, DOCS_ARGS);
    }

    append_claude_md_instructions(&project_root, sel)?;

    // Installed if every requested server registered; otherwise the user needs
    // to finish setup manually (see the per-server warnings above).
    Ok(if ok {
        InstallOutcome::Installed
    } else {
        InstallOutcome::ManualRequired
    })
}

/// Append Fluree usage instructions to `<project_root>/CLAUDE.md` if it exists
/// and doesn't already mention the tools. The note covers only the servers in
/// `sel`. Mirrors the per-repo rules-file behavior of the other installers.
fn append_claude_md_instructions(project_root: &Path, sel: ServerSel) -> CliResult<()> {
    let claude_md = project_root.join("CLAUDE.md");
    if !claude_md.exists() {
        return Ok(());
    }
    let content = std::fs::read_to_string(&claude_md)
        .map_err(|e| CliError::Input(format!("failed to read CLAUDE.md: {e}")))?;
    if content.contains("memory_recall") || content.contains("docs_search") {
        return Ok(());
    }

    let mut body = String::new();
    if sel.wants_memory() {
        body.push_str(
            "Use the `memory_recall` MCP tool at the start of tasks to retrieve project context, \
             and `memory_add` to store important facts, decisions, and constraints.\n",
        );
    }
    if sel.wants_docs() {
        body.push_str(
            "Use `docs_search` / `docs_get` / `docs_examples` to look up version-pinned Fluree \
             syntax (queries, transactions, policy, config) instead of guessing.\n",
        );
    }
    std::fs::write(&claude_md, format!("{content}\n\n## Fluree\n\n{body}"))
        .map_err(|e| CliError::Config(format!("failed to update CLAUDE.md: {e}")))?;
    println!("  Appended Fluree instructions to CLAUDE.md");
    Ok(())
}

/// Register one MCP server with Claude Code via `claude mcp add` (local scope).
/// Returns whether registration succeeded; prints a manual-fallback hint if not.
fn claude_mcp_add(
    project_root: &Path,
    name: &str,
    fluree_bin: &str,
    server_args: &[&str],
) -> bool {
    let mut args = vec!["mcp", "add", "--transport", "stdio", name, "--", fluree_bin];
    args.extend_from_slice(server_args);
    let result = std::process::Command::new("claude")
        .current_dir(project_root)
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    let manual = server_args.join(" ");
    match result {
        Ok(status) if status.success() => {
            println!("  Registered {name} via `claude mcp add` (local scope → ~/.claude.json)");
            true
        }
        Ok(_) => {
            eprintln!("  Warning: `claude mcp add {name}` failed. Is Claude Code installed?");
            eprintln!("  Run: claude mcp add -t stdio {name} -- {fluree_bin} {manual}");
            false
        }
        Err(_) => {
            eprintln!("  Warning: `claude` not found on PATH.");
            eprintln!("  Run: claude mcp add -t stdio {name} -- {fluree_bin} {manual}");
            false
        }
    }
}

fn install_cursor(fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".cursor/mcp.json");
    let outcome = install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "mcpServers",
        default: serde_json::json!({ "mcpServers": {} }),
        entries: fluree_server_entries(fluree_bin, true, true, sel),
    })?;
    if outcome == InstallOutcome::Installed && sel.wants_memory() {
        install_rules_file(&project_root.join(".cursor/rules"));
    }
    Ok(outcome)
}

fn install_vscode(fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".vscode/mcp.json");
    let outcome = install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "servers",
        default: serde_json::json!({ "servers": {} }),
        entries: fluree_server_entries(fluree_bin, true, false, sel),
    })?;
    if outcome == InstallOutcome::Installed && sel.wants_memory() {
        install_rules_file(&project_root.join(".vscode"));
    }
    Ok(outcome)
}

fn install_windsurf(fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::Config("cannot determine home directory".to_string()))?;
    let config_path = home.join(".codeium/windsurf/mcp_config.json");
    install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "mcpServers",
        default: serde_json::json!({ "mcpServers": {} }),
        entries: fluree_server_entries(fluree_bin, false, false, sel),
    })
}

fn install_zed(fluree_bin: &str, sel: ServerSel) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".zed/settings.json");
    install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "context_servers",
        default: serde_json::json!({}),
        entries: fluree_server_entries(fluree_bin, false, false, sel),
    })
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Phase 2 of `fluree memory init`: detect installed IDEs, ask whether to
/// configure each, and install the MCP config. Called from `memory::run_init`
/// after the memory store is set up.
pub(super) fn run_mcp_phase(yes: bool) -> CliResult<()> {
    let detected = detect_ai_tools();
    if detected.is_empty() {
        println!();
        println!("No AI coding tools detected.");
        println!("Run 'fluree memory mcp-install --ide <tool>' to configure manually.");
        println!("Supported: claude-code, cursor, vscode, windsurf, zed");
        return Ok(());
    }

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

    // Non-interactive: --yes auto-confirms; no TTY without --yes skips.
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
    let mut manual_count = 0usize;
    for dt in &to_install {
        let confirmed = yes
            || prompt_yn(&format!(
                "Install MCP config for {}?",
                dt.tool.display_name()
            ));

        if confirmed {
            // `memory init` wires up both servers.
            match install_tool(dt.tool, &fluree_bin, ServerSel::All) {
                Ok(InstallOutcome::Installed) => {
                    installed_count += 1;
                }
                Ok(InstallOutcome::ManualRequired) => {
                    manual_count += 1;
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
    if manual_count > 0 {
        println!(
            "{} tool{} need manual setup (see snippet above).",
            manual_count,
            if manual_count == 1 { "" } else { "s" }
        );
    }

    Ok(())
}

/// `fluree mcp install [--ide <ide>] [--server <sel>]` — non-interactive
/// installer. `server` selects which MCP server(s) to register
/// (`memory` / `docs` / `all`); needs no `.fluree/` directory.
pub(super) fn run_mcp_install(ide: Option<&str>, server: &str) -> CliResult<()> {
    let sel = ServerSel::parse(server).ok_or_else(|| {
        CliError::Usage(format!("unknown --server '{server}'; valid: memory, docs, all"))
    })?;

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
        "claude-code" => install_claude_code(&fluree_bin, sel).map(|_| ()),
        // Accept old names for backward compatibility.
        "claude-vscode" | "vscode" | "github-copilot" => {
            install_vscode(&fluree_bin, sel).map(|_| ())
        }
        "cursor" => install_cursor(&fluree_bin, sel).map(|_| ()),
        "windsurf" => install_windsurf(&fluree_bin, sel).map(|_| ()),
        "zed" => install_zed(&fluree_bin, sel).map(|_| ()),
        other => Err(CliError::Usage(format!(
            "unknown IDE '{other}'; valid: claude-code, vscode, cursor, windsurf, zed"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// In-memory `DetectionEnv` for driving detection logic without
    /// touching the real filesystem, PATH, or home directory.
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
            if self.path_exists(path) {
                Some(path.to_path_buf())
            } else {
                None
            }
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
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.add_executable("claude");
        assert!(is_ide_present(&env, &CLAUDE_CODE));
    }

    #[test]
    fn claude_present_via_home_dir_marker() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.add_dir("/h/.claude");
        assert!(is_ide_present(&env, &CLAUDE_CODE));
    }

    #[test]
    fn claude_present_via_home_file_marker() {
        // .claude.json is a file, not a dir — path_exists must match both.
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.add_file("/h/.claude.json", "{}");
        assert!(is_ide_present(&env, &CLAUDE_CODE));
    }

    #[test]
    fn claude_absent_when_no_signal() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.add_dir("/h");
        assert!(!is_ide_present(&env, &CLAUDE_CODE));
    }

    #[test]
    fn vscode_present_via_config_dir() {
        // Linux launched-once marker is ~/.config/Code, not ~/.vscode.
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Code");
        assert!(is_ide_present(&env, &VSCODE));
    }

    #[test]
    fn cursor_present_via_config_dir() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Cursor");
        assert!(is_ide_present(&env, &CURSOR));
    }

    #[test]
    fn windsurf_present_via_config_dir() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Windsurf");
        assert!(is_ide_present(&env, &WINDSURF));
    }

    #[test]
    fn zed_present_via_lowercase_config_dir() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/zed");
        assert!(is_ide_present(&env, &ZED));
    }

    #[test]
    fn macos_app_probe_checks_both_system_and_user_applications() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.system_apps = Some(PathBuf::from("/Applications"));
        env.home = Some(PathBuf::from("/Users/u"));

        env.add_dir("/Applications/Cursor.app");
        assert!(macos_app_installed(&env, "Cursor.app"));

        env.dirs.clear();
        env.add_dir("/Users/u/Applications/Cursor.app");
        assert!(macos_app_installed(&env, "Cursor.app"));
    }

    #[test]
    fn macos_app_probe_is_noop_when_system_dir_unset() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.system_apps = None;
        env.add_dir("/h/Applications/Cursor.app");
        assert!(!macos_app_installed(&env, "Cursor.app"));
    }

    #[test]
    fn path_extensions_unix_returns_bare_name_only() {
        if !cfg!(target_os = "windows") {
            let exts = path_extensions();
            assert_eq!(exts.len(), 1);
            assert!(exts[0].is_empty());
        }
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
        let env = empty_env_with_repo(Path::new("/r"));
        assert!(detect_ai_tools_with(&env).is_empty());
    }

    #[test]
    fn detect_finds_claude_via_path_only() {
        let mut env = empty_env_with_repo(Path::new("/r"));
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

    // -- load_config matrix -------------------------------------------------

    #[test]
    fn load_config_returns_default_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.json");
        let result = load_config(&path, serde_json::json!({"k": 1}));
        assert!(
            matches!(result, LoadedConfig::MissingOrEmpty(v) if v == serde_json::json!({"k": 1}))
        );
    }

    #[test]
    fn load_config_returns_default_when_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::write(&path, "   \n").unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(matches!(result, LoadedConfig::MissingOrEmpty(_)));
    }

    #[test]
    fn load_config_returns_unsafe_for_jsonc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, "// hi\n{\"a\": 1}\n").unwrap();
        let result = load_config(&path, serde_json::json!({}));
        assert!(matches!(result, LoadedConfig::Unsafe { .. }));
    }

    #[test]
    fn load_config_returns_unsafe_for_invalid_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-utf8.json");
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

    // -- install_json_mcp_config end-to-end ---------------------------------

    #[test]
    fn install_json_mcp_config_writes_default_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        let entry = serde_json::json!({ "command": "fluree" });
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: vec![("fluree-memory", entry.clone())],
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::Installed);
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(written["mcpServers"]["fluree-memory"], entry);
    }

    #[test]
    fn install_json_mcp_config_returns_manual_for_unsafe_file() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        std::fs::write(&config_path, "// jsonc\n{}").unwrap();
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: vec![("fluree-memory", serde_json::json!({ "command": "fluree" }))],
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::ManualRequired);
        // File must be left as-is.
        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            "// jsonc\n{}"
        );
    }

    #[test]
    fn load_config_returns_unsafe_for_non_object_root() {
        // Valid JSON, but a top-level array/scalar/null isn't an MCP config
        // shape we can merge into. We must refuse rather than silently
        // overwriting with our default.
        for body in ["[]", "[1, 2, 3]", "\"hello\"", "42", "null", "true"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c.json");
            std::fs::write(&path, body).unwrap();
            let result = load_config(&path, serde_json::json!({}));
            assert!(
                matches!(result, LoadedConfig::Unsafe { .. }),
                "expected Unsafe for non-object root {body:?}"
            );
        }
    }

    #[test]
    fn install_json_mcp_config_refuses_non_object_root() {
        // The full install flow must not report Installed when the file
        // is valid JSON but a non-object — that would print "Installed" to
        // the user while merge_mcp_entries silently did nothing.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        std::fs::write(&config_path, "[]").unwrap();
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: vec![("fluree-memory", serde_json::json!({ "command": "fluree" }))],
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::ManualRequired);
        // File untouched.
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), "[]");
    }

    #[test]
    fn install_json_mcp_config_refuses_non_object_top_key() {
        // Root is an object (so load_config returns Parsed), but the
        // top_key holds a non-object value. We must NOT silently replace
        // the array with our merged object — that would clobber whatever
        // the user had configured under that key.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        let original = r#"{"mcpServers":[]}"#;
        std::fs::write(&config_path, original).unwrap();
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: vec![("fluree-memory", serde_json::json!({ "command": "fluree" }))],
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::ManualRequired);
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
    }

    #[test]
    fn install_json_mcp_config_merges_into_existing_object_top_key() {
        // Existing entries under top_key are preserved; we only insert
        // (or overwrite) `fluree-memory`.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        std::fs::write(&config_path, r#"{"mcpServers":{"other":{"command":"x"}}}"#).unwrap();
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: vec![("fluree-memory", serde_json::json!({ "command": "fluree" }))],
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::Installed);
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(written["mcpServers"]["other"]["command"], "x");
        assert_eq!(written["mcpServers"]["fluree-memory"]["command"], "fluree");
    }

    #[test]
    fn install_registers_both_memory_and_docs_servers() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("mcp.json");
        let outcome = install_json_mcp_config(JsonMcpInstall {
            config_path: &config_path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entries: fluree_server_entries("fluree", false, false, ServerSel::All),
        })
        .unwrap();
        assert_eq!(outcome, InstallOutcome::Installed);

        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(
            written["mcpServers"]["fluree-memory"]["args"],
            serde_json::json!(["mcp", "serve", "--transport", "stdio"])
        );
        assert_eq!(
            written["mcpServers"]["fluree-docs"]["args"],
            serde_json::json!(["docs", "serve", "--transport", "stdio"])
        );
    }

    #[test]
    fn server_selection_filters_entries() {
        let names = |sel| {
            fluree_server_entries("fluree", false, false, sel)
                .into_iter()
                .map(|(n, _)| n)
                .collect::<Vec<_>>()
        };
        assert_eq!(names(ServerSel::All), vec!["fluree-memory", "fluree-docs"]);
        assert_eq!(names(ServerSel::Memory), vec!["fluree-memory"]);
        assert_eq!(names(ServerSel::Docs), vec!["fluree-docs"]);
        assert!(ServerSel::parse("bogus").is_none());
    }

    #[test]
    fn cursor_entries_pin_memory_env_but_not_docs() {
        // Cursor (typed + memory_env): memory gets type+FLUREE_HOME, docs gets
        // type but no env (the docs corpus is embedded, stateless).
        let entries = fluree_server_entries("fluree", true, true, ServerSel::All);
        let mem = &entries.iter().find(|(n, _)| *n == "fluree-memory").unwrap().1;
        let docs = &entries.iter().find(|(n, _)| *n == "fluree-docs").unwrap().1;
        assert_eq!(mem["type"], "stdio");
        assert!(mem["env"]["FLUREE_HOME"].is_string());
        assert_eq!(docs["type"], "stdio");
        assert!(docs.get("env").is_none());
    }

    #[test]
    fn install_rules_file_does_not_panic_when_target_dir_collides_with_file() {
        // If a *file* exists at the path we want to use as the rules
        // directory, create_dir_all will fail. Best-effort install should
        // emit a warning and return without propagating an error.
        let dir = tempfile::tempdir().unwrap();
        let bogus_dir = dir.path().join("rules");
        std::fs::write(&bogus_dir, "i am a file").unwrap();
        install_rules_file(&bogus_dir); // must not panic
                                        // The pre-existing file is untouched (we didn't overwrite it).
        assert_eq!(std::fs::read_to_string(&bogus_dir).unwrap(), "i am a file");
    }
}
