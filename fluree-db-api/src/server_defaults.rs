//! Default values for Fluree server configuration.
//!
//! These constants are the single source of truth shared between
//! `fluree-db-server` (clap `default_value` attributes) and
//! `fluree-db-cli` (the `fluree init` config template).

use serde_json::json;
use std::path::{Path, PathBuf};

// ── Top-level server settings ───────────────────────────────────────

pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8090";
pub const DEFAULT_STORAGE_PATH: &str = ".fluree/storage";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_CORS_ENABLED: bool = true;
pub const DEFAULT_BODY_LIMIT: usize = 52_428_800; // 50 MB

// ── Indexing ────────────────────────────────────────────────────────

pub const DEFAULT_INDEXING_ENABLED: bool = true;
pub const DEFAULT_REINDEX_MIN_BYTES: usize = 100_000;

/// Fallback hard-threshold when RAM detection is unavailable (WASM, sandbox).
///
/// Production defaults use [`default_reindex_max_bytes`], which returns 20%
/// of detected system RAM. This constant is only used when that detection
/// fails or on platforms without `sysinfo`.
pub const DEFAULT_REINDEX_MAX_BYTES_FALLBACK: usize = 256 * 1024 * 1024; // 256 MB

/// Default hard-threshold for novelty backpressure (bytes).
///
/// Returns 20% of system RAM on native platforms, with a 256 MB fallback
/// when detection is unavailable. Novelty is held in memory between index
/// builds; once it exceeds this threshold, commits block until indexing
/// catches up. 20% of RAM leaves plenty of headroom for the query cache,
/// incoming requests, and the indexer itself.
#[cfg(feature = "native")]
pub fn default_reindex_max_bytes() -> usize {
    use sysinfo::{MemoryRefreshKind, System};

    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());

    let total_memory_bytes = sys.total_memory() as usize;
    if total_memory_bytes == 0 {
        return DEFAULT_REINDEX_MAX_BYTES_FALLBACK;
    }

    // 20% of RAM, floored at 64 MB so very small hosts still have a
    // workable buffer between soft and hard thresholds.
    (total_memory_bytes / 5).max(64 * 1024 * 1024)
}

/// Default hard-threshold (WASM/non-native fallback).
#[cfg(not(feature = "native"))]
pub fn default_reindex_max_bytes() -> usize {
    DEFAULT_REINDEX_MAX_BYTES_FALLBACK
}

/// Canonical default `IndexConfig` for API-layer callers.
///
/// Combines [`DEFAULT_REINDEX_MIN_BYTES`] with [`default_reindex_max_bytes`]
/// so the server, CLI, programmatic `FlureeBuilder`, and any transient
/// internal callers all resolve the same value.
pub fn default_index_config() -> fluree_db_ledger::IndexConfig {
    fluree_db_ledger::IndexConfig {
        reindex_min_bytes: DEFAULT_REINDEX_MIN_BYTES,
        reindex_max_bytes: default_reindex_max_bytes(),
    }
}

// ── Auth ────────────────────────────────────────────────────────────

pub const DEFAULT_AUTH_MODE: &str = "none";
pub const DEFAULT_JWKS_CACHE_TTL: u64 = 300;

// ── MCP ─────────────────────────────────────────────────────────────

pub const DEFAULT_MCP_ENABLED: bool = false;

// ── Peer ────────────────────────────────────────────────────────────

pub const DEFAULT_PEER_ROLE: &str = "transaction";
pub const DEFAULT_PEER_RECONNECT_INITIAL_MS: u64 = 1000;
pub const DEFAULT_PEER_RECONNECT_MAX_MS: u64 = 30000;
pub const DEFAULT_PEER_RECONNECT_MULTIPLIER: f64 = 2.0;

// ── Storage proxy ───────────────────────────────────────────────────

pub const DEFAULT_STORAGE_PROXY_ENABLED: bool = false;

// ── Global directory resolution ──────────────────────────────────────

/// Resolved Fluree directory paths, separating config from data.
///
/// In local mode (`.fluree/` in a project), both config and data point
/// to the same directory. In global mode (`fluree init --global`), config
/// may differ from data per XDG conventions on Linux.
///
/// **Config dir** holds: `config.toml` / `config.jsonld` (including
/// remotes, upstreams, tracked_ledgers sections).
///
/// **Data dir** holds: `storage/`, `active`, `prefixes.json`.
#[derive(Debug, Clone)]
pub struct FlureeDir {
    config_dir: PathBuf,
    data_dir: PathBuf,
}

impl FlureeDir {
    /// Create a unified `FlureeDir` where config and data share a path.
    ///
    /// Used for local mode (`.fluree/` in a project) and when `$FLUREE_HOME`
    /// provides a single override directory.
    pub fn unified(path: PathBuf) -> Self {
        Self {
            config_dir: path.clone(),
            data_dir: path,
        }
    }

    /// Create a split `FlureeDir` with separate config and data paths.
    ///
    /// Used for global mode when `$FLUREE_HOME` is not set, so platform
    /// directories determine the locations (e.g. `~/.config/fluree` for
    /// config and `~/.local/share/fluree` for data on Linux).
    pub fn split(config_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            config_dir,
            data_dir,
        }
    }

    /// Directory for configuration files (`config.toml`, `config.jsonld`,
    /// remotes, upstreams, tracked_ledgers).
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Directory for data and state files (`storage/`, `active`,
    /// `prefixes.json`).
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Whether config and data dirs are the same path (unified mode).
    pub fn is_unified(&self) -> bool {
        self.config_dir == self.data_dir
    }

    /// Resolve global Fluree directories.
    ///
    /// When `$FLUREE_HOME` is set, both config and data share that single
    /// path (unified mode). Otherwise, config goes to
    /// `dirs::config_local_dir()/fluree` and data goes to
    /// `dirs::data_local_dir()/fluree` (XDG-split on Linux; unified on
    /// macOS and Windows where both resolve to the same directory).
    pub fn global() -> Option<Self> {
        if let Ok(p) = std::env::var("FLUREE_HOME") {
            return Some(Self::unified(PathBuf::from(p)));
        }
        let config = dirs::config_local_dir().map(|d| d.join("fluree"))?;
        let data = dirs::data_local_dir().map(|d| d.join("fluree"))?;
        Some(Self::split(config, data))
    }
}

// ── Config file discovery ────────────────────────────────────────────

/// The `.fluree` directory name.
pub const FLUREE_DIR: &str = ".fluree";

/// TOML config file name within `.fluree/`.
pub const CONFIG_FILE_TOML: &str = "config.toml";

/// JSON-LD config file name within `.fluree/`.
pub const CONFIG_FILE_JSONLD: &str = "config.jsonld";

/// The JSON-LD vocabulary IRI for Fluree config properties.
pub const CONFIG_VOCAB: &str = "https://ns.flur.ee/config#";

/// Config file format. Used for init template generation, config file
/// detection, and format-dispatched read/write in both CLI and server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    Toml,
    JsonLd,
}

impl ConfigFormat {
    /// File name for this format.
    pub fn filename(&self) -> &'static str {
        match self {
            Self::Toml => CONFIG_FILE_TOML,
            Self::JsonLd => CONFIG_FILE_JSONLD,
        }
    }
}

/// Detect which config file exists in a directory.
///
/// Checks for `config.toml` first, then `config.jsonld`. If both exist,
/// TOML takes precedence and `both_exist` is set to `true` so callers
/// can emit a warning in their preferred manner (e.g. `eprintln!` for
/// CLI, `tracing::warn` for server).
///
/// Returns `None` if neither file exists.
pub fn detect_config_in_dir(dir: &Path) -> Option<ConfigDetection> {
    let toml_path = dir.join(CONFIG_FILE_TOML);
    let jsonld_path = dir.join(CONFIG_FILE_JSONLD);

    let toml_exists = toml_path.is_file();
    let jsonld_exists = jsonld_path.is_file();

    match (toml_exists, jsonld_exists) {
        (true, true) => Some(ConfigDetection {
            path: toml_path,
            format: ConfigFormat::Toml,
            both_exist: true,
        }),
        (true, false) => Some(ConfigDetection {
            path: toml_path,
            format: ConfigFormat::Toml,
            both_exist: false,
        }),
        (false, true) => Some(ConfigDetection {
            path: jsonld_path,
            format: ConfigFormat::JsonLd,
            both_exist: false,
        }),
        (false, false) => None,
    }
}

/// Result of config file detection in a directory.
#[derive(Debug)]
pub struct ConfigDetection {
    /// Path to the detected config file.
    pub path: PathBuf,
    /// Format of the detected config file.
    pub format: ConfigFormat,
    /// True if both `config.toml` and `config.jsonld` exist (TOML wins).
    pub both_exist: bool,
}

// ── Secret resolution ───────────────────────────────────────────────

/// Resolve a config value that may be an `@filepath` reference.
///
/// If the value starts with `@`, the remainder is treated as a file path;
/// the file is read and its contents returned (trimmed of leading/trailing
/// whitespace). Otherwise the value is returned as-is.
///
/// This is the same convention used by CLI flags like `--peer-events-token`.
pub fn resolve_at_filepath(value: &str) -> Result<String, std::io::Error> {
    if let Some(path) = value.strip_prefix('@') {
        let content = std::fs::read_to_string(path)?;
        Ok(content.trim().to_string())
    } else {
        Ok(value.to_string())
    }
}

/// Returns `true` if `value` looks like a plaintext secret (not an
/// `@filepath` reference and not empty).
pub fn is_plaintext_secret(value: &str) -> bool {
    !value.is_empty() && !value.starts_with('@')
}

// ── Template generation ─────────────────────────────────────────────

/// Generate a commented-out TOML config template using the canonical
/// default values above.  Written by `fluree init`.
///
/// `storage_path_override` — when `Some`, the template uses that path
/// instead of the default relative `".fluree/storage"`.  Pass the
/// absolute global data dir + "/storage" for `fluree init --global`.
pub fn generate_config_template(storage_path_override: Option<&str>) -> String {
    let storage_path = storage_path_override.unwrap_or(DEFAULT_STORAGE_PATH);
    let storage_comment = if storage_path_override.is_some() {
        "# absolute path to global data directory"
    } else {
        "# relative to working directory"
    };
    format!(
        r#"# Fluree Configuration
#
# This file is shared by the Fluree CLI and Fluree Server.
# CLI-managed sections (remotes, upstreams) are managed by `fluree` commands.
# Server settings live under [server] and can be customized below.
#
# Precedence (highest to lowest):
#   1. CLI arguments
#   2. Environment variables (FLUREE_*)
#   3. Profile overrides ([profiles.<name>.server])
#   4. This file ([server])
#   5. Built-in defaults

# ──────────────────────────────────────────────────────────────────────
# Server Configuration
# ──────────────────────────────────────────────────────────────────────
# Uncomment and modify values as needed.

# [server]
# listen_addr = "{listen_addr}"
# storage_path = "{storage_path}"   {storage_comment}
# log_level = "{log_level}"                 # trace, debug, info, warn, error
# cors_enabled = {cors_enabled}
# body_limit = {body_limit}              # 50 MB
# cache_max_mb = 4096                    # global cache budget (MB); default: tiered fraction of RAM (30% <4GB, 40% 4-8GB, 50% ≥8GB)

# [server.indexing]
# enabled = {indexing_enabled}                    # disable only when a separate peer/indexer owns indexing for this storage
# reindex_min_bytes = {reindex_min_bytes}         # {reindex_min_kb} KB — triggers background reindexing
# reindex_max_bytes = {reindex_max_bytes}      # {reindex_max_mb} MB (default: 20% of system RAM) — blocks commits until reindexed

# [server.auth.events]
# mode = "{auth_mode}"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.data]
# mode = "{auth_mode}"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]
# # default_policy_class = "ex:DefaultPolicy"

# [server.auth.admin]
# mode = "{auth_mode}"                      # none, required
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.jwks]
# # issuers = ["https://auth.example.com=https://auth.example.com/.well-known/jwks.json"]
# # cache_ttl = {jwks_cache_ttl}                  # seconds

# [server.mcp]
# enabled = {mcp_enabled}
# # auth_trusted_issuers = ["did:key:z6Mk..."]

# [server.peer]
# role = "{peer_role}"               # transaction, peer
# # tx_server_url = "http://tx.internal:8090"
# # events_token = "@/etc/fluree/peer-token.jwt"  # use @filepath for secrets
# # subscribe_all = false
# # ledgers = ["books:main"]
# # graph_sources = []

# [server.peer.reconnect]
# initial_ms = {reconnect_initial_ms}
# max_ms = {reconnect_max_ms}
# multiplier = {reconnect_multiplier}

# [server.storage_proxy]
# enabled = {storage_proxy_enabled}
# # trusted_issuers = ["did:key:z6Mk..."]
# # default_identity = "ex:ServiceAccount"
# # default_policy_class = "ex:ProxyPolicy"
# # debug_headers = false

# ──────────────────────────────────────────────────────────────────────
# Profiles
# ──────────────────────────────────────────────────────────────────────
# Activate with: fluree-server --profile <name>
# Profile values are deep-merged onto [server].

# [profiles.dev.server]
# log_level = "debug"

# [profiles.prod.server]
# log_level = "warn"
# [profiles.prod.server.auth.data]
# mode = "required"

# Example: a transaction-only peer that delegates index maintenance to a
# separate indexer process. Only disable indexing when something else
# is producing index roots for this storage.
# [profiles.peer.server.indexing]
# enabled = false
# [profiles.peer.server.peer]
# role = "transaction"
# tx_server_url = "http://indexer.internal:8090"
"#,
        listen_addr = DEFAULT_LISTEN_ADDR,
        storage_comment = storage_comment,
        log_level = DEFAULT_LOG_LEVEL,
        cors_enabled = DEFAULT_CORS_ENABLED,
        body_limit = DEFAULT_BODY_LIMIT,
        indexing_enabled = DEFAULT_INDEXING_ENABLED,
        reindex_min_bytes = DEFAULT_REINDEX_MIN_BYTES,
        reindex_min_kb = DEFAULT_REINDEX_MIN_BYTES / 1000,
        reindex_max_bytes = default_reindex_max_bytes(),
        reindex_max_mb = default_reindex_max_bytes() / (1024 * 1024),
        auth_mode = DEFAULT_AUTH_MODE,
        jwks_cache_ttl = DEFAULT_JWKS_CACHE_TTL,
        mcp_enabled = DEFAULT_MCP_ENABLED,
        peer_role = DEFAULT_PEER_ROLE,
        reconnect_initial_ms = DEFAULT_PEER_RECONNECT_INITIAL_MS,
        reconnect_max_ms = DEFAULT_PEER_RECONNECT_MAX_MS,
        reconnect_multiplier = DEFAULT_PEER_RECONNECT_MULTIPLIER,
        storage_proxy_enabled = DEFAULT_STORAGE_PROXY_ENABLED,
    )
}

/// Generate a JSON-LD config template with `@context` and all default values.
///
/// The `@context` maps config keys to the Fluree config vocabulary, making
/// the file valid JSON-LD that can be processed by standard JSON-LD tooling.
/// Serde ignores `@context` during deserialization, so the same serde types
/// work for both JSON and JSON-LD config files.
///
/// See [`generate_config_template`] for `storage_path_override` semantics.
pub fn generate_jsonld_config_template(storage_path_override: Option<&str>) -> String {
    let storage_path = storage_path_override.unwrap_or(DEFAULT_STORAGE_PATH);
    let template = json!({
        "@context": {
            "@vocab": CONFIG_VOCAB
        },
        "_comment": "Fluree Configuration — JSON-LD format. Precedence: CLI > env > profile > file > defaults. Remove keys to use built-in defaults.",
        "server": {
            "listen_addr": DEFAULT_LISTEN_ADDR,
            "storage_path": storage_path,
            "log_level": DEFAULT_LOG_LEVEL,
            "cors_enabled": DEFAULT_CORS_ENABLED,
            "body_limit": DEFAULT_BODY_LIMIT,
            "indexing": {
                "enabled": DEFAULT_INDEXING_ENABLED,
                "reindex_min_bytes": DEFAULT_REINDEX_MIN_BYTES,
                "reindex_max_bytes": default_reindex_max_bytes()
            },
            "auth": {
                "events": { "mode": DEFAULT_AUTH_MODE },
                "data": { "mode": DEFAULT_AUTH_MODE },
                "admin": { "mode": DEFAULT_AUTH_MODE },
                "jwks": { "cache_ttl": DEFAULT_JWKS_CACHE_TTL }
            },
            "mcp": { "enabled": DEFAULT_MCP_ENABLED },
            "peer": {
                "role": DEFAULT_PEER_ROLE,
                "reconnect": {
                    "initial_ms": DEFAULT_PEER_RECONNECT_INITIAL_MS,
                    "max_ms": DEFAULT_PEER_RECONNECT_MAX_MS,
                    "multiplier": DEFAULT_PEER_RECONNECT_MULTIPLIER
                }
            },
            "storage_proxy": { "enabled": DEFAULT_STORAGE_PROXY_ENABLED }
        }
    });
    serde_json::to_string_pretty(&template).expect("template serialization cannot fail")
}

/// Generate a config template in the given format.
///
/// See [`generate_config_template`] for `storage_path_override` semantics.
pub fn generate_config_template_for(
    format: ConfigFormat,
    storage_path_override: Option<&str>,
) -> String {
    match format {
        ConfigFormat::Toml => generate_config_template(storage_path_override),
        ConfigFormat::JsonLd => generate_jsonld_config_template(storage_path_override),
    }
}

/// Validate the `@context` in a JSON-LD config file.
///
/// Uses `fluree_graph_json_ld::parse_context()` to confirm the context is
/// well-formed. Logs a warning if `@vocab` is not the standard Fluree config
/// vocabulary. Returns `Ok(())` on success or if no `@context` is present.
pub fn validate_jsonld_context(json: &serde_json::Value) -> Result<(), String> {
    let Some(ctx_value) = json.get("@context") else {
        return Ok(());
    };

    let parsed = fluree_graph_json_ld::parse_context(ctx_value)
        .map_err(|e| format!("invalid @context in config: {e}"))?;

    if parsed.vocab.as_deref() != Some(CONFIG_VOCAB) {
        tracing::warn!(
            vocab = ?parsed.vocab,
            expected = CONFIG_VOCAB,
            "JSON-LD config @vocab does not match the standard Fluree config vocabulary"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_contains_default_values() {
        let t = generate_config_template(None);
        // Spot-check a few interpolated values
        assert!(t.contains(&format!("# listen_addr = \"{DEFAULT_LISTEN_ADDR}\"")));
        assert!(t.contains(&format!(
            "# reindex_min_bytes = {DEFAULT_REINDEX_MIN_BYTES}"
        )));
        assert!(t.contains(&format!("# log_level = \"{DEFAULT_LOG_LEVEL}\"")));
        assert!(t.contains(&format!("# enabled = {DEFAULT_INDEXING_ENABLED}")));
        assert!(t.contains("# cache_max_mb = 4096"));
    }

    #[test]
    fn jsonld_template_is_valid_json_with_context() {
        let t = generate_jsonld_config_template(None);
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();

        // Has @context with correct @vocab
        let vocab = v["@context"]["@vocab"].as_str().unwrap();
        assert_eq!(vocab, CONFIG_VOCAB);

        // Has server settings with correct defaults
        assert_eq!(
            v["server"]["listen_addr"].as_str().unwrap(),
            DEFAULT_LISTEN_ADDR
        );
        assert_eq!(
            v["server"]["indexing"]["reindex_min_bytes"]
                .as_u64()
                .unwrap() as usize,
            DEFAULT_REINDEX_MIN_BYTES
        );
    }

    #[test]
    fn validate_context_accepts_valid_vocab() {
        let doc = json!({
            "@context": { "@vocab": CONFIG_VOCAB },
            "server": {}
        });
        assert!(validate_jsonld_context(&doc).is_ok());
    }

    #[test]
    fn validate_context_accepts_missing_context() {
        let doc = json!({ "server": {} });
        assert!(validate_jsonld_context(&doc).is_ok());
    }

    #[test]
    fn validate_context_rejects_malformed() {
        // An integer is not a valid @context
        let doc = json!({ "@context": 42 });
        assert!(validate_jsonld_context(&doc).is_err());
    }

    #[test]
    fn config_format_filenames() {
        assert_eq!(ConfigFormat::Toml.filename(), "config.toml");
        assert_eq!(ConfigFormat::JsonLd.filename(), "config.jsonld");
    }

    #[test]
    fn generate_template_for_dispatches() {
        let toml = generate_config_template_for(ConfigFormat::Toml, None);
        assert!(toml.starts_with("# Fluree Configuration"));

        let jsonld = generate_config_template_for(ConfigFormat::JsonLd, None);
        let v: serde_json::Value = serde_json::from_str(&jsonld).unwrap();
        assert!(v.get("@context").is_some());
    }

    #[test]
    fn resolve_at_filepath_passthrough() {
        assert_eq!(resolve_at_filepath("plain-value").unwrap(), "plain-value");
        assert_eq!(resolve_at_filepath("").unwrap(), "");
    }

    #[test]
    fn resolve_at_filepath_reads_file() {
        let dir = std::env::temp_dir().join("fluree-test-at-filepath");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("secret.txt");
        std::fs::write(&path, "  my-secret-token\n").unwrap();

        let value = format!("@{}", path.display());
        assert_eq!(resolve_at_filepath(&value).unwrap(), "my-secret-token");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_at_filepath_missing_file() {
        assert!(resolve_at_filepath("@/nonexistent/path/token.txt").is_err());
    }

    #[test]
    fn is_plaintext_secret_detection() {
        assert!(is_plaintext_secret("eyJhbGciOiJFZDI1NTE5..."));
        assert!(!is_plaintext_secret("@/etc/fluree/token.jwt"));
        assert!(!is_plaintext_secret(""));
    }

    // ========================================================================
    // detect_config_in_dir tests (commit 98f3899)
    // ========================================================================

    #[test]
    fn detect_config_toml_only() {
        let dir = std::env::temp_dir().join("fluree-test-detect-toml-only");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CONFIG_FILE_TOML), "# toml").unwrap();
        // Ensure jsonld does NOT exist
        let _ = std::fs::remove_file(dir.join(CONFIG_FILE_JSONLD));

        let result = detect_config_in_dir(&dir).unwrap();
        assert_eq!(result.format, ConfigFormat::Toml);
        assert!(!result.both_exist);
        assert_eq!(result.path, dir.join(CONFIG_FILE_TOML));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn detect_config_jsonld_only() {
        let dir = std::env::temp_dir().join("fluree-test-detect-jsonld-only");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CONFIG_FILE_JSONLD), "{}").unwrap();
        // Ensure toml does NOT exist
        let _ = std::fs::remove_file(dir.join(CONFIG_FILE_TOML));

        let result = detect_config_in_dir(&dir).unwrap();
        assert_eq!(result.format, ConfigFormat::JsonLd);
        assert!(!result.both_exist);
        assert_eq!(result.path, dir.join(CONFIG_FILE_JSONLD));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn detect_config_both_exist_prefers_toml() {
        let dir = std::env::temp_dir().join("fluree-test-detect-both");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CONFIG_FILE_TOML), "# toml").unwrap();
        std::fs::write(dir.join(CONFIG_FILE_JSONLD), "{}").unwrap();

        let result = detect_config_in_dir(&dir).unwrap();
        assert_eq!(result.format, ConfigFormat::Toml);
        assert!(result.both_exist);
        assert_eq!(result.path, dir.join(CONFIG_FILE_TOML));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn detect_config_neither_exists() {
        let dir = std::env::temp_dir().join("fluree-test-detect-none");
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join(CONFIG_FILE_TOML));
        let _ = std::fs::remove_file(dir.join(CONFIG_FILE_JSONLD));

        assert!(detect_config_in_dir(&dir).is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    // ========================================================================
    // FlureeDir tests
    // ========================================================================

    #[test]
    fn fluree_dir_unified_returns_same_paths() {
        let dir = FlureeDir::unified(PathBuf::from("/tmp/fluree"));
        assert_eq!(dir.config_dir(), Path::new("/tmp/fluree"));
        assert_eq!(dir.data_dir(), Path::new("/tmp/fluree"));
        assert!(dir.is_unified());
    }

    #[test]
    fn fluree_dir_split_returns_different_paths() {
        let dir = FlureeDir::split(
            PathBuf::from("/home/user/.config/fluree"),
            PathBuf::from("/home/user/.local/share/fluree"),
        );
        assert_eq!(dir.config_dir(), Path::new("/home/user/.config/fluree"));
        assert_eq!(dir.data_dir(), Path::new("/home/user/.local/share/fluree"));
        assert!(!dir.is_unified());
    }

    #[test]
    fn fluree_dir_split_same_path_is_unified() {
        let dir = FlureeDir::split(PathBuf::from("/same/path"), PathBuf::from("/same/path"));
        assert!(dir.is_unified());
    }

    #[test]
    fn toml_template_uses_storage_path_override() {
        let t = generate_config_template(Some("/global/data/storage"));
        assert!(
            t.contains(r#"# storage_path = "/global/data/storage""#),
            "template should contain the override path"
        );
        assert!(
            !t.contains(DEFAULT_STORAGE_PATH),
            "template should NOT contain the default storage path"
        );
        assert!(
            t.contains("# absolute path to global data directory"),
            "template should use the absolute-path comment"
        );
        assert!(
            !t.contains("# relative to working directory"),
            "template should NOT contain the relative-path comment"
        );
    }

    #[test]
    fn toml_template_default_uses_relative_comment() {
        let t = generate_config_template(None);
        assert!(t.contains(DEFAULT_STORAGE_PATH));
        assert!(t.contains("# relative to working directory"));
        assert!(!t.contains("# absolute path to global data directory"));
    }

    #[test]
    fn jsonld_template_uses_storage_path_override() {
        let t = generate_jsonld_config_template(Some("/global/data/storage"));
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
        assert_eq!(
            v["server"]["storage_path"].as_str().unwrap(),
            "/global/data/storage"
        );
    }
}
