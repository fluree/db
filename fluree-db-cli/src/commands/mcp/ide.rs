//! IDE detection and MCP configuration for `fluree mcp init` / `fluree mcp
//! status` (and the deprecated `fluree memory mcp-install` alias).
//!
//! All IDE-specific logic — filesystem/env probing, JSON merging, per-IDE
//! install flow — lives here so the `mcp` dispatch module can stay focused.
//!
//! The unified Fluree MCP server is registered under a single entry named
//! `fluree`, whose `--toolsets` arg selects which toolsets it exposes. Older
//! installs registered separate `fluree-memory` / `fluree-docs` servers; the
//! installer migrates those away (see [`LEGACY_SERVER_NAMES`]).

use crate::error::{CliError, CliResult};
use fluree_db_mcp::Toolset;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The single MCP server entry name the unified installer writes.
const SERVER_NAME: &str = "fluree";

/// Server-entry names written by older versions (one server per feature). The
/// installer removes these whenever it writes the unified `fluree` entry so a
/// user isn't left with duplicate/stale servers.
const LEGACY_SERVER_NAMES: &[&str] = &["fluree-memory", "fluree-docs"];

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

/// Filesystem + environment probes used by detection. All filesystem access in
/// [`detect_ai_tools_with`] goes through this trait so the detection logic can
/// be unit-tested with a `FakeEnv` (see the `tests` module). The production
/// implementation is [`RealEnv`].
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
/// On Unix, only the bare name. On Windows, the user's `PATHEXT` if set (with a
/// sensible default fallback) plus the bare name (so something like `claude`
/// without an extension still matches a literal file).
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

/// Walk up from `start` looking for a directory containing a marker. Used by
/// `RealEnv::project_root` (and the fake env in tests).
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

/// Resolve the project root via the production environment.
pub(crate) fn project_root_dir() -> PathBuf {
    RealEnv.project_root()
}

// ---------------------------------------------------------------------------
// Per-IDE detection specs (data-driven probes)
// ---------------------------------------------------------------------------

/// What signals to check for a given IDE. `path_exists` matches both files and
/// directories, so `home_markers` / `config_markers` may name either (e.g.
/// Claude Code's `.claude.json` file vs. its `.claude/` dir).
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
    // `.config/zed` is the Linux convention even though config_dir() on Linux is
    // `~/.config` (so it'd also match via config_markers). Listing it here makes
    // the spec work on a non-default config_dir.
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

/// The MCP config file and the JSON key for its server map, per IDE. Returns
/// `(path, top_key)`. `None` for Claude Code (handled separately — its
/// local-scope config keys by project root, not a static path).
fn server_map_location(tool: AiTool, env: &dyn DetectionEnv) -> Option<(PathBuf, &'static str)> {
    let project_root = env.project_root();
    match tool {
        AiTool::ClaudeCode => None,
        AiTool::Cursor => Some((project_root.join(".cursor/mcp.json"), "mcpServers")),
        AiTool::VsCode => Some((project_root.join(".vscode/mcp.json"), "servers")),
        AiTool::Windsurf => env
            .home()
            .map(|h| (h.join(".codeium/windsurf/mcp_config.json"), "mcpServers")),
        AiTool::Zed => Some((project_root.join(".zed/settings.json"), "context_servers")),
    }
}

/// The toolsets the `fluree` server is configured with for `tool`, by reading
/// the IDE's MCP config. `None` if no `fluree` entry is present; an empty vec if
/// the entry has no parseable `--toolsets` arg.
fn installed_toolsets(tool: AiTool, env: &dyn DetectionEnv) -> Option<Vec<Toolset>> {
    let servers = match tool {
        AiTool::ClaudeCode => claude_code_server_map(env)?,
        _ => {
            let (path, top_key) = server_map_location(tool, env)?;
            let content = env.read_to_string(&path)?;
            let json: serde_json::Value = serde_json::from_str(&content).ok()?;
            json.get(top_key)?.clone()
        }
    };
    let entry = servers.get(SERVER_NAME)?;
    Some(toolsets_from_args(entry))
}

/// Read Claude Code's local-scope `mcpServers` object for the current project
/// (keyed by canonical project root / cwd), if present.
fn claude_code_server_map(env: &dyn DetectionEnv) -> Option<serde_json::Value> {
    let home = env.home()?;
    let content = env.read_to_string(&home.join(".claude.json"))?;
    let json: serde_json::Value = serde_json::from_str(&content).ok()?;
    let projects = json.get("projects")?;

    let project_root = env.project_root();
    let canonical_root = env.canonicalize(&project_root);
    let cwd_canonical = env.canonical_current_dir();
    let candidates = [canonical_root, Some(project_root), cwd_canonical];
    for candidate in candidates.iter().flatten() {
        let key = candidate.display().to_string();
        if let Some(servers) = projects.get(&key).and_then(|p| p.get("mcpServers")) {
            return Some(servers.clone());
        }
    }
    None
}

/// Parse the `--toolsets <csv>` value out of a server entry's `args` array.
fn toolsets_from_args(entry: &serde_json::Value) -> Vec<Toolset> {
    let Some(args) = entry.get("args").and_then(|a| a.as_array()) else {
        return Vec::new();
    };
    let mut iter = args.iter().filter_map(|v| v.as_str());
    while let Some(a) = iter.next() {
        if a == "--toolsets" {
            if let Some(csv) = iter.next() {
                return Toolset::parse_selection(csv).unwrap_or_default();
            }
        }
    }
    Vec::new()
}

/// Whether the `fluree` MCP server is already registered for `tool`.
fn is_already_configured(tool: AiTool, env: &dyn DetectionEnv) -> bool {
    installed_toolsets(tool, env).is_some()
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
// Server entry construction
// ---------------------------------------------------------------------------

/// CLI args that launch the unified MCP server over stdio with `toolsets`.
fn serve_args(toolsets: &[Toolset]) -> Vec<String> {
    vec![
        "mcp".to_string(),
        "serve".to_string(),
        "--transport".to_string(),
        "stdio".to_string(),
        "--toolsets".to_string(),
        Toolset::join(toolsets),
    ]
}

/// Build the single `fluree` MCP server entry. `typed` adds the `"type":
/// "stdio"` field Cursor / VS Code expect; `memory_env` pins the memory store's
/// `FLUREE_HOME` to the workspace (Cursor only) — attached only when the memory
/// toolset is enabled, since the docs toolset is stateless.
fn fluree_entry(
    fluree_bin: &str,
    typed: bool,
    memory_env: bool,
    toolsets: &[Toolset],
) -> (&'static str, serde_json::Value) {
    let env = (memory_env && toolsets.contains(&Toolset::Memory))
        .then(|| serde_json::json!({ "FLUREE_HOME": "${workspaceFolder}/.fluree" }));
    let mut obj = serde_json::Map::new();
    if typed {
        obj.insert("type".to_string(), serde_json::json!("stdio"));
    }
    obj.insert("command".to_string(), serde_json::json!(fluree_bin));
    obj.insert("args".to_string(), serde_json::json!(serve_args(toolsets)));
    if let Some(env) = env {
        obj.insert("env".to_string(), env);
    }
    (SERVER_NAME, serde_json::Value::Object(obj))
}

// ---------------------------------------------------------------------------
// Install flow
// ---------------------------------------------------------------------------

/// Outcome of an attempted MCP install.
#[derive(Debug, PartialEq, Eq)]
enum InstallOutcome {
    Installed,
    ManualRequired,
}

/// Outcome of loading an MCP config file for merge.
enum LoadedConfig {
    /// File was missing or empty — start from `default`.
    MissingOrEmpty(serde_json::Value),
    /// File parsed cleanly — merge into the existing value.
    Parsed(serde_json::Value),
    /// File exists but is unsafe to overwrite (JSONC/comments, corruption,
    /// invalid UTF-8, unreadable). Caller must NOT overwrite — emit a manual
    /// snippet instead. `reason` is shown to the user.
    Unsafe { reason: String },
}

/// Load an MCP config file. Refuses to silently default a corrupt/JSONC file or
/// one we can't read cleanly, so we never clobber a user's existing IDE config.
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

/// Insert/overwrite the `fluree` entry under `top_key` (creating `top_key` if
/// absent) and remove any legacy per-feature server entries.
fn merge_fluree_entry(
    config: &mut serde_json::Value,
    top_key: &str,
    entry: &(&str, serde_json::Value),
) {
    if !config
        .get(top_key)
        .is_some_and(serde_json::Value::is_object)
    {
        if let Some(obj) = config.as_object_mut() {
            obj.insert(top_key.to_string(), serde_json::json!({}));
        }
    }
    if let Some(servers) = config.get_mut(top_key).and_then(|v| v.as_object_mut()) {
        servers.insert(entry.0.to_string(), entry.1.clone());
        for legacy in LEGACY_SERVER_NAMES {
            servers.remove(*legacy);
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

/// Parameters for [`install_json_mcp_config`], the shared install flow used by
/// Cursor / VS Code / Windsurf / Zed.
struct JsonMcpInstall<'a> {
    config_path: &'a Path,
    /// Top-level key under which the server entry is merged (`mcpServers` for
    /// Cursor/Windsurf, `servers` for VS Code, `context_servers` for Zed).
    top_key: &'a str,
    /// Default JSON value to use when the file is missing or empty.
    default: serde_json::Value,
    /// The `(name, entry)` pair to insert under `top_key` (the `fluree` server).
    entry: (&'static str, serde_json::Value),
}

/// Common install shape: load, merge (+ legacy cleanup), refuse-on-unsafe,
/// write. Used by every JSON-config-based IDE.
fn install_json_mcp_config(opts: JsonMcpInstall<'_>) -> CliResult<InstallOutcome> {
    let manual_snippet = || {
        format!(
            "\"{}\": {{\n  \"{}\": {}\n}}",
            opts.top_key,
            opts.entry.0,
            serde_json::to_string_pretty(&opts.entry.1).unwrap_or_default()
        )
    };

    let mut config = match load_config(opts.config_path, opts.default) {
        LoadedConfig::MissingOrEmpty(v) | LoadedConfig::Parsed(v) => v,
        LoadedConfig::Unsafe { reason } => {
            warn_unsafe_config(opts.config_path, &reason, &manual_snippet());
            return Ok(InstallOutcome::ManualRequired);
        }
    };

    // The root is guaranteed to be an object by load_config, but the existing
    // value under `top_key` (if any) might not be. Refuse rather than clobber.
    if let Some(existing) = config.get(opts.top_key) {
        if !existing.is_object() {
            let reason = format!("\"{}\" exists but is not a JSON object", opts.top_key);
            warn_unsafe_config(opts.config_path, &reason, &manual_snippet());
            return Ok(InstallOutcome::ManualRequired);
        }
    }

    merge_fluree_entry(&mut config, opts.top_key, &opts.entry);
    write_config(opts.config_path, &config)?;
    println!(
        "  Installed: {} ({})",
        opts.config_path.display(),
        opts.entry.0
    );
    Ok(InstallOutcome::Installed)
}

/// Write the bundled `fluree_rules.md` into `dir`. Used by IDEs whose agent can
/// be told to read a per-workspace rules file (Cursor, VS Code). Best-effort.
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

fn install_claude_code(fluree_bin: &str, toolsets: &[Toolset]) -> CliResult<InstallOutcome> {
    // Register via `claude mcp add` (local scope → ~/.claude.json). Spawn
    // `claude` from project_root so it keys its `projects` map by the repo root,
    // not the (possibly nested) cwd we were invoked from.
    let project_root = project_root_dir();

    // Migrate: remove any legacy per-feature servers first (best-effort).
    for legacy in LEGACY_SERVER_NAMES {
        claude_mcp_remove(&project_root, legacy);
    }

    let ok = claude_mcp_add(
        &project_root,
        SERVER_NAME,
        fluree_bin,
        &serve_args(toolsets),
    );

    append_claude_md_instructions(&project_root, toolsets)?;

    Ok(if ok {
        InstallOutcome::Installed
    } else {
        InstallOutcome::ManualRequired
    })
}

/// Append Fluree usage instructions to `<project_root>/CLAUDE.md` if it exists
/// and doesn't already mention the tools. The note covers only the enabled
/// toolsets. Mirrors the per-repo rules-file behavior of the other installers.
fn append_claude_md_instructions(project_root: &Path, toolsets: &[Toolset]) -> CliResult<()> {
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
    if toolsets.contains(&Toolset::Memory) {
        body.push_str(
            "Use the `memory_recall` MCP tool at the start of tasks to retrieve project context, \
             and `memory_add` to store important facts, decisions, and constraints.\n",
        );
    }
    if toolsets.contains(&Toolset::Docs) {
        body.push_str(
            "Use `docs_search` / `docs_get` / `docs_examples` to look up version-pinned Fluree \
             syntax (queries, transactions, policy, config) instead of guessing.\n",
        );
    }
    if body.is_empty() {
        return Ok(());
    }
    std::fs::write(&claude_md, format!("{content}\n\n## Fluree\n\n{body}"))
        .map_err(|e| CliError::Config(format!("failed to update CLAUDE.md: {e}")))?;
    println!("  Appended Fluree instructions to CLAUDE.md");
    Ok(())
}

/// Register the MCP server with Claude Code via `claude mcp add` (local scope).
/// Returns whether registration succeeded; prints a manual-fallback hint if not.
fn claude_mcp_add(
    project_root: &Path,
    name: &str,
    fluree_bin: &str,
    server_args: &[String],
) -> bool {
    let mut args: Vec<&str> = vec!["mcp", "add", "--transport", "stdio", name, "--", fluree_bin];
    args.extend(server_args.iter().map(String::as_str));
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

/// Best-effort removal of a (legacy) MCP server from Claude Code's local scope.
fn claude_mcp_remove(project_root: &Path, name: &str) {
    let _ = std::process::Command::new("claude")
        .current_dir(project_root)
        .args(["mcp", "remove", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn install_cursor(fluree_bin: &str, toolsets: &[Toolset]) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".cursor/mcp.json");
    let outcome = install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "mcpServers",
        default: serde_json::json!({ "mcpServers": {} }),
        entry: fluree_entry(fluree_bin, true, true, toolsets),
    })?;
    if outcome == InstallOutcome::Installed && toolsets.contains(&Toolset::Memory) {
        install_rules_file(&project_root.join(".cursor/rules"));
    }
    Ok(outcome)
}

fn install_vscode(fluree_bin: &str, toolsets: &[Toolset]) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".vscode/mcp.json");
    let outcome = install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "servers",
        default: serde_json::json!({ "servers": {} }),
        entry: fluree_entry(fluree_bin, true, false, toolsets),
    })?;
    if outcome == InstallOutcome::Installed && toolsets.contains(&Toolset::Memory) {
        install_rules_file(&project_root.join(".vscode"));
    }
    Ok(outcome)
}

fn install_windsurf(fluree_bin: &str, toolsets: &[Toolset]) -> CliResult<InstallOutcome> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::Config("cannot determine home directory".to_string()))?;
    let config_path = home.join(".codeium/windsurf/mcp_config.json");
    install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "mcpServers",
        default: serde_json::json!({ "mcpServers": {} }),
        entry: fluree_entry(fluree_bin, false, false, toolsets),
    })
}

fn install_zed(fluree_bin: &str, toolsets: &[Toolset]) -> CliResult<InstallOutcome> {
    let project_root = project_root_dir();
    let config_path = project_root.join(".zed/settings.json");
    install_json_mcp_config(JsonMcpInstall {
        config_path: &config_path,
        top_key: "context_servers",
        default: serde_json::json!({}),
        entry: fluree_entry(fluree_bin, false, false, toolsets),
    })
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Resolve the path to the running `fluree` binary, for installed config args.
fn fluree_bin() -> String {
    std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "fluree".to_string())
}

/// `fluree mcp init [--ide <ide>] --toolsets <sel>` — register the unified
/// `fluree` MCP server (with `toolsets`) in an IDE's config. Needs no
/// `.fluree/` directory.
pub(super) fn run_install(ide: Option<&str>, toolsets: &[Toolset]) -> CliResult<()> {
    let fluree_bin = fluree_bin();

    let ide = ide.map(String::from).unwrap_or_else(|| {
        detect_ai_tools()
            .into_iter()
            .find(|dt| !dt.already_configured)
            .map(|dt| dt.tool.ide_id().to_string())
            .unwrap_or_else(|| "claude-code".to_string())
    });

    println!(
        "Registering Fluree MCP server (toolsets: {}) for {ide}…",
        Toolset::join(toolsets)
    );
    match ide.as_str() {
        "claude-code" => install_claude_code(&fluree_bin, toolsets).map(|_| ()),
        // Accept old names for backward compatibility.
        "claude-vscode" | "vscode" | "github-copilot" => {
            install_vscode(&fluree_bin, toolsets).map(|_| ())
        }
        "cursor" => install_cursor(&fluree_bin, toolsets).map(|_| ()),
        "windsurf" => install_windsurf(&fluree_bin, toolsets).map(|_| ()),
        "zed" => install_zed(&fluree_bin, toolsets).map(|_| ()),
        other => Err(CliError::Usage(format!(
            "unknown IDE '{other}'; valid: claude-code, vscode, cursor, windsurf, zed"
        ))),
    }?;
    println!("Done. Reload your editor to activate the Fluree MCP tools.");
    Ok(())
}

/// `fluree mcp status` — show, per detected IDE, whether the `fluree` server is
/// installed and which toolsets it exposes.
pub(super) fn run_status() -> CliResult<()> {
    let detected = detect_ai_tools();
    if detected.is_empty() {
        println!("No AI coding tools detected.");
        println!("Run `fluree mcp init` to set one up.");
        return Ok(());
    }

    println!("Fluree MCP status (per detected IDE):");
    for dt in &detected {
        match installed_toolsets(dt.tool, &RealEnv) {
            Some(toolsets) if !toolsets.is_empty() => {
                println!(
                    "  - {}: installed (toolsets: {})",
                    dt.tool.display_name(),
                    Toolset::join(&toolsets)
                );
            }
            Some(_) => {
                println!(
                    "  - {}: installed (toolsets: unknown — re-run `fluree mcp init`)",
                    dt.tool.display_name()
                );
            }
            None => {
                println!("  - {}: not installed", dt.tool.display_name());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// In-memory `DetectionEnv` for driving detection logic without touching the
    /// real filesystem, PATH, or home directory.
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
    fn claude_present_via_home_file_marker() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.add_file("/h/.claude.json", "{}");
        assert!(is_ide_present(&env, &CLAUDE_CODE));
    }

    #[test]
    fn vscode_present_via_config_dir() {
        let mut env = empty_env_with_repo(Path::new("/r"));
        env.home = Some(PathBuf::from("/h"));
        env.config_dir = Some(PathBuf::from("/h/.config"));
        env.add_dir("/h/.config/Code");
        assert!(is_ide_present(&env, &VSCODE));
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

    // -- toolset detection from installed config ----------------------------

    #[test]
    fn installed_toolsets_parses_cursor_entry() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.add_file(
            repo.join(".cursor/mcp.json"),
            r#"{"mcpServers":{"fluree":{"command":"fluree","args":["mcp","serve","--transport","stdio","--toolsets","memory,docs"]}}}"#,
        );
        assert_eq!(
            installed_toolsets(AiTool::Cursor, &env),
            Some(vec![Toolset::Memory, Toolset::Docs])
        );
    }

    #[test]
    fn installed_toolsets_none_when_absent() {
        let repo = PathBuf::from("/r");
        let env = empty_env_with_repo(&repo);
        assert_eq!(installed_toolsets(AiTool::Cursor, &env), None);
    }

    #[test]
    fn installed_toolsets_reads_claude_code_local_scope() {
        let repo = PathBuf::from("/home/u/proj");
        let mut env = empty_env_with_repo(&repo);
        env.home = Some(PathBuf::from("/home/u"));
        let claude_json = serde_json::json!({
            "projects": {
                repo.display().to_string(): {
                    "mcpServers": { "fluree": { "command": "fluree",
                        "args": ["mcp","serve","--transport","stdio","--toolsets","docs"] } }
                }
            }
        });
        env.add_file("/home/u/.claude.json", claude_json.to_string());
        assert_eq!(
            installed_toolsets(AiTool::ClaudeCode, &env),
            Some(vec![Toolset::Docs])
        );
    }

    #[test]
    fn detect_flags_already_configured_via_fluree_entry() {
        let repo = PathBuf::from("/r");
        let mut env = empty_env_with_repo(&repo);
        env.add_executable("cursor");
        env.add_file(
            repo.join(".cursor/mcp.json"),
            r#"{"mcpServers":{"fluree":{"command":"fluree","args":["mcp","serve","--toolsets","all"]}}}"#,
        );
        let detected = detect_ai_tools_with(&env);
        let cursor = detected.iter().find(|d| d.tool == AiTool::Cursor).unwrap();
        assert!(cursor.already_configured);
    }

    // -- serve args / entry construction ------------------------------------

    #[test]
    fn serve_args_carry_canonical_toolsets() {
        assert_eq!(
            serve_args(Toolset::ALL),
            vec![
                "mcp",
                "serve",
                "--transport",
                "stdio",
                "--toolsets",
                "memory,docs"
            ]
        );
        assert_eq!(
            serve_args(&[Toolset::Docs]),
            vec!["mcp", "serve", "--transport", "stdio", "--toolsets", "docs"]
        );
    }

    #[test]
    fn cursor_entry_pins_memory_env_only_with_memory_toolset() {
        let (_, with_mem) = fluree_entry("fluree", true, true, Toolset::ALL);
        assert_eq!(with_mem["type"], "stdio");
        assert!(with_mem["env"]["FLUREE_HOME"].is_string());
        // docs-only -> no env even though memory_env=true (stateless)
        let (_, docs_only) = fluree_entry("fluree", true, true, &[Toolset::Docs]);
        assert!(docs_only.get("env").is_none());
    }

    // -- load_config matrix -------------------------------------------------

    #[test]
    fn load_config_returns_default_when_missing_or_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.json");
        assert!(matches!(
            load_config(&missing, serde_json::json!({"k": 1})),
            LoadedConfig::MissingOrEmpty(v) if v == serde_json::json!({"k": 1})
        ));
        let empty = dir.path().join("empty.json");
        std::fs::write(&empty, "   \n").unwrap();
        assert!(matches!(
            load_config(&empty, serde_json::json!({})),
            LoadedConfig::MissingOrEmpty(_)
        ));
    }

    #[test]
    fn load_config_returns_unsafe_for_jsonc_and_non_object() {
        let dir = tempfile::tempdir().unwrap();
        let jsonc = dir.path().join("settings.json");
        std::fs::write(&jsonc, "// hi\n{\"a\": 1}\n").unwrap();
        assert!(matches!(
            load_config(&jsonc, serde_json::json!({})),
            LoadedConfig::Unsafe { .. }
        ));
        for body in ["[]", "\"hello\"", "42", "null"] {
            let path = dir.path().join("c.json");
            std::fs::write(&path, body).unwrap();
            assert!(
                matches!(
                    load_config(&path, serde_json::json!({})),
                    LoadedConfig::Unsafe { .. }
                ),
                "expected Unsafe for {body:?}"
            );
        }
    }

    // -- install_json_mcp_config end-to-end ---------------------------------

    fn install_to(path: &Path, toolsets: &[Toolset]) -> InstallOutcome {
        install_json_mcp_config(JsonMcpInstall {
            config_path: path,
            top_key: "mcpServers",
            default: serde_json::json!({ "mcpServers": {} }),
            entry: fluree_entry("fluree", false, false, toolsets),
        })
        .unwrap()
    }

    #[test]
    fn install_writes_single_fluree_entry_with_toolset_args() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("mcp.json");
        assert_eq!(
            install_to(&config_path, Toolset::ALL),
            InstallOutcome::Installed
        );
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        let servers = written["mcpServers"].as_object().unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(
            written["mcpServers"]["fluree"]["args"],
            serde_json::json!([
                "mcp",
                "serve",
                "--transport",
                "stdio",
                "--toolsets",
                "memory,docs"
            ])
        );
    }

    #[test]
    fn install_migrates_away_legacy_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("mcp.json");
        std::fs::write(
            &config_path,
            r#"{"mcpServers":{"fluree-memory":{"command":"x"},"fluree-docs":{"command":"y"},"other":{"command":"z"}}}"#,
        )
        .unwrap();
        assert_eq!(
            install_to(&config_path, &[Toolset::Memory]),
            InstallOutcome::Installed
        );
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        let servers = written["mcpServers"].as_object().unwrap();
        assert!(!servers.contains_key("fluree-memory"));
        assert!(!servers.contains_key("fluree-docs"));
        assert_eq!(servers["other"]["command"], "z");
        assert!(servers.contains_key("fluree"));
    }

    #[test]
    fn install_refuses_unsafe_file_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        std::fs::write(&config_path, "// jsonc\n{}").unwrap();
        assert_eq!(
            install_to(&config_path, Toolset::ALL),
            InstallOutcome::ManualRequired
        );
        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            "// jsonc\n{}"
        );
    }

    #[test]
    fn install_refuses_non_object_top_key_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        let original = r#"{"mcpServers":[]}"#;
        std::fs::write(&config_path, original).unwrap();
        assert_eq!(
            install_to(&config_path, Toolset::ALL),
            InstallOutcome::ManualRequired
        );
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
    }

    #[test]
    fn install_preserves_existing_other_servers() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("a.json");
        std::fs::write(&config_path, r#"{"mcpServers":{"other":{"command":"x"}}}"#).unwrap();
        assert_eq!(
            install_to(&config_path, &[Toolset::Docs]),
            InstallOutcome::Installed
        );
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(written["mcpServers"]["other"]["command"], "x");
        assert_eq!(written["mcpServers"]["fluree"]["command"], "fluree");
    }

    #[test]
    fn install_rules_file_does_not_panic_when_target_dir_collides_with_file() {
        let dir = tempfile::tempdir().unwrap();
        let bogus_dir = dir.path().join("rules");
        std::fs::write(&bogus_dir, "i am a file").unwrap();
        install_rules_file(&bogus_dir); // must not panic
        assert_eq!(std::fs::read_to_string(&bogus_dir).unwrap(), "i am a file");
    }
}
