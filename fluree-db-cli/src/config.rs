use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::{self, ConfigFormat, FlureeDir, CONFIG_FILE_TOML, FLUREE_DIR};
use std::fs;
use std::path::{Path, PathBuf};

const ACTIVE_FILE: &str = "active";
const STORAGE_DIR: &str = "storage";
const PREFIXES_FILE: &str = "prefixes.json";

/// Re-export ConfigFormat as ConfigFileFormat for backward compatibility
/// within the CLI codebase.
pub type ConfigFileFormat = ConfigFormat;

/// Detect which config file exists in a `.fluree/` directory.
///
/// Checks for `config.toml` first, then `config.jsonld`. If both exist,
/// TOML wins and a warning is printed to stderr.
///
/// Returns `None` if neither file exists.
pub fn detect_config_file(fluree_dir: &Path) -> Option<(PathBuf, ConfigFileFormat)> {
    let detection = server_defaults::detect_config_in_dir(fluree_dir)?;
    if detection.both_exist {
        eprintln!(
            "warning: both config.toml and config.jsonld exist in {}; using config.toml",
            fluree_dir.display(),
        );
    }
    Some((detection.path, detection.format))
}

/// Walk up from `start` looking for a `.fluree/` directory.
fn find_fluree_dir_from(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let candidate = current.join(FLUREE_DIR);
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

/// Find `.fluree/` by walking up from cwd. Returns a unified `FlureeDir`
/// (local project mode) or `None` if not found.
pub fn find_fluree_dir() -> Option<FlureeDir> {
    let cwd = std::env::current_dir().ok()?;
    find_fluree_dir_from(&cwd).map(FlureeDir::unified)
}

/// Find `.fluree/` by walking up from cwd, falling back to global directories.
pub fn find_or_global_fluree_dir() -> Option<FlureeDir> {
    if let Some(d) = find_fluree_dir() {
        return Some(d);
    }
    let global = FlureeDir::global()?;
    // Accept if either config or data dir already exists
    if global.config_dir().is_dir() || global.data_dir().is_dir() {
        Some(global)
    } else {
        None
    }
}

/// Resolve a `--config` override to a `.fluree/` directory path.
///
/// Accepts either:
/// - A file path (e.g., `--config /path/to/.fluree/config.toml`) → uses parent dir
/// - A directory path (e.g., `--config /path/to/.fluree/`) → uses it directly
///
/// Validates the resolved directory exists and contains expected structure.
fn resolve_config_override(p: &Path) -> CliResult<PathBuf> {
    // Canonicalize to handle relative paths like `--config config.toml`
    let resolved = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    };

    if resolved.is_file() {
        // It's a file; use its parent directory as the .fluree/ dir
        let dir = resolved
            .parent()
            .ok_or_else(|| {
                CliError::Config(format!(
                    "cannot determine parent of: {}",
                    resolved.display()
                ))
            })?
            .to_path_buf();
        if dir.is_dir() {
            return Ok(dir);
        }
        return Err(CliError::Config(format!(
            "parent directory does not exist: {}",
            dir.display()
        )));
    }

    if resolved.is_dir() {
        return Ok(resolved);
    }

    Err(CliError::Config(format!(
        "config path does not exist: {}",
        p.display()
    )))
}

/// Require a local `.fluree/` directory (for mutating commands).
pub fn require_fluree_dir(config_override: Option<&Path>) -> CliResult<FlureeDir> {
    if let Some(p) = config_override {
        // An explicit --config path means the user chose a single directory;
        // config and data are co-located there, so we use unified mode.
        return resolve_config_override(p).map(FlureeDir::unified);
    }
    find_fluree_dir().ok_or(CliError::NoFlureeDir)
}

/// Require a `.fluree/` directory, allowing global fallback (for read-only commands).
pub fn require_fluree_dir_or_global(config_override: Option<&Path>) -> CliResult<FlureeDir> {
    if let Some(p) = config_override {
        // An explicit --config path means the user chose a single directory;
        // config and data are co-located there, so we use unified mode.
        return resolve_config_override(p).map(FlureeDir::unified);
    }
    find_or_global_fluree_dir().ok_or(CliError::NoFlureeDir)
}

/// Resolve the directories for `fluree init` without creating anything.
///
/// In local mode (`global = false`), returns a unified `.fluree/` under cwd.
/// In global mode (`global = true`), delegates to `FlureeDir::global()` which
/// may split config and data across platform directories.
///
/// Returns the `FlureeDir` that `init_fluree_dir` will use, so callers can
/// inspect it (e.g. to customise the config template) before writing.
pub fn resolve_init_dirs(global: bool) -> CliResult<FlureeDir> {
    if global {
        FlureeDir::global()
            .ok_or_else(|| CliError::Config("cannot determine global directories".into()))
    } else {
        Ok(FlureeDir::unified(
            std::env::current_dir()?.join(FLUREE_DIR),
        ))
    }
}

/// Create Fluree directories with config template and storage subdirectory.
///
/// Creates `storage/` in `dirs.data_dir()` and writes `config_template`
/// to `dirs.config_dir()` under `config_filename` (e.g. `"config.toml"`)
/// if no config file already exists. In global mode, config and data may
/// reside in different directories.
pub fn init_fluree_dir(
    dirs: &FlureeDir,
    config_template: &str,
    config_filename: &str,
) -> CliResult<()> {
    // Create storage directory in data_dir
    let storage_dir = dirs.data_dir().join(STORAGE_DIR);
    fs::create_dir_all(&storage_dir).map_err(|e| {
        CliError::Config(format!("failed to create {}: {e}", storage_dir.display()))
    })?;

    // Create config directory (may differ from data_dir in global mode)
    fs::create_dir_all(dirs.config_dir()).map_err(|e| {
        CliError::Config(format!(
            "failed to create {}: {e}",
            dirs.config_dir().display()
        ))
    })?;

    // Write config template if config file doesn't already exist
    let config_path = dirs.config_dir().join(config_filename);
    if !config_path.exists() {
        fs::write(&config_path, config_template).map_err(|e| {
            CliError::Config(format!("failed to create {}: {e}", config_path.display()))
        })?;
    }

    Ok(())
}

/// Read the currently active ledger name from `.fluree/active`.
pub fn read_active_ledger(fluree_dir: &Path) -> Option<String> {
    let path = fluree_dir.join(ACTIVE_FILE);
    fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Write the active ledger name to `.fluree/active`.
pub fn write_active_ledger(fluree_dir: &Path, alias: &str) -> CliResult<()> {
    let path = fluree_dir.join(ACTIVE_FILE);
    fs::write(&path, alias).map_err(|e| {
        CliError::Config(format!(
            "failed to write active ledger to {}: {e}",
            path.display()
        ))
    })
}

/// Clear the active ledger (remove `.fluree/active`).
pub fn clear_active_ledger(fluree_dir: &Path) -> CliResult<()> {
    let path = fluree_dir.join(ACTIVE_FILE);
    if path.exists() {
        fs::remove_file(&path)
            .map_err(|e| CliError::Config(format!("failed to clear active ledger: {e}")))?;
    }
    Ok(())
}

/// Resolve the storage path for the Fluree instance.
///
/// Checks the config file (`config.toml` / `config.jsonld`) in
/// `dirs.config_dir()` for an explicit `[server].storage_path` value.
/// If found, that path is used (absolute as-is, relative resolved from
/// cwd). Otherwise falls back to `dirs.data_dir()/storage`.
pub fn resolve_storage_path(dirs: &FlureeDir) -> PathBuf {
    if let Some(configured) = read_configured_storage_path(dirs.config_dir()) {
        PathBuf::from(configured)
    } else {
        dirs.data_dir().join(STORAGE_DIR)
    }
}

/// Read `[server].storage_path` from the config file, if present.
///
/// Uses a minimal serde struct so the CLI doesn't depend on the server's
/// full config types. Returns `None` if no config file exists or the
/// field is absent/commented-out.
fn read_configured_storage_path(config_dir: &Path) -> Option<String> {
    let (path, format) = detect_config_file(config_dir)?;
    let content = fs::read_to_string(&path).ok()?;
    match format {
        ConfigFileFormat::Toml => {
            let doc: toml::Value = toml::from_str(&content).ok()?;
            doc.get("server")?
                .get("storage_path")?
                .as_str()
                .map(String::from)
        }
        ConfigFileFormat::JsonLd => {
            let doc: serde_json::Value = serde_json::from_str(&content).ok()?;
            doc.get("server")?
                .get("storage_path")?
                .as_str()
                .map(String::from)
        }
    }
}

/// Indexing thresholds read from the config file.
pub struct IndexingThresholds {
    pub reindex_min_bytes: Option<usize>,
    pub reindex_max_bytes: Option<usize>,
}

/// Read `[server.indexing]` thresholds from the config file.
///
/// Returns `None` values for fields that are absent or commented-out;
/// callers fall back to compiled defaults.
pub fn read_indexing_thresholds(config_dir: &Path) -> IndexingThresholds {
    let empty = IndexingThresholds {
        reindex_min_bytes: None,
        reindex_max_bytes: None,
    };

    let Some((path, format)) = detect_config_file(config_dir) else {
        return empty;
    };
    let Ok(content) = fs::read_to_string(&path) else {
        return empty;
    };

    match format {
        ConfigFileFormat::Toml => {
            let doc: toml::Value = match toml::from_str(&content) {
                Ok(d) => d,
                Err(_) => return empty,
            };
            let indexing = doc.get("server").and_then(|s| s.get("indexing"));
            IndexingThresholds {
                reindex_min_bytes: indexing
                    .and_then(|i| i.get("reindex_min_bytes"))
                    .and_then(toml::Value::as_integer)
                    .map(|v| v as usize),
                reindex_max_bytes: indexing
                    .and_then(|i| i.get("reindex_max_bytes"))
                    .and_then(toml::Value::as_integer)
                    .map(|v| v as usize),
            }
        }
        ConfigFileFormat::JsonLd => {
            let doc: serde_json::Value = match serde_json::from_str(&content) {
                Ok(d) => d,
                Err(_) => return empty,
            };
            let indexing = doc.get("server").and_then(|s| s.get("indexing"));
            IndexingThresholds {
                reindex_min_bytes: indexing
                    .and_then(|i| i.get("reindex_min_bytes"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|v| v as usize),
                reindex_max_bytes: indexing
                    .and_then(|i| i.get("reindex_max_bytes"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|v| v as usize),
            }
        }
    }
}

/// Prefix map type: prefix -> IRI namespace
pub type PrefixMap = std::collections::HashMap<String, String>;

/// Read stored prefixes from `.fluree/prefixes.json`.
pub fn read_prefixes(fluree_dir: &Path) -> PrefixMap {
    let path = fluree_dir.join(PREFIXES_FILE);
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// Write prefixes to `.fluree/prefixes.json`.
pub fn write_prefixes(fluree_dir: &Path, prefixes: &PrefixMap) -> CliResult<()> {
    let path = fluree_dir.join(PREFIXES_FILE);
    let json = serde_json::to_string_pretty(prefixes)
        .map_err(|e| CliError::Config(format!("failed to serialize prefixes: {e}")))?;
    fs::write(&path, json).map_err(|e| CliError::Config(format!("failed to write prefixes: {e}")))
}

/// Add a prefix mapping.
pub fn add_prefix(fluree_dir: &Path, prefix: &str, iri: &str) -> CliResult<()> {
    let mut prefixes = read_prefixes(fluree_dir);
    prefixes.insert(prefix.to_string(), iri.to_string());
    write_prefixes(fluree_dir, &prefixes)
}

/// Remove a prefix mapping.
pub fn remove_prefix(fluree_dir: &Path, prefix: &str) -> CliResult<bool> {
    let mut prefixes = read_prefixes(fluree_dir);
    let existed = prefixes.remove(prefix).is_some();
    if existed {
        write_prefixes(fluree_dir, &prefixes)?;
    }
    Ok(existed)
}

/// Expand a compact IRI (e.g., "ex:alice") using stored prefixes.
/// Returns the original string if no prefix matches.
pub fn expand_iri(fluree_dir: &Path, compact: &str) -> String {
    if let Some((prefix, local)) = compact.split_once(':') {
        // Don't expand if it looks like an absolute IRI
        if local.starts_with("//") {
            return compact.to_string();
        }
        let prefixes = read_prefixes(fluree_dir);
        if let Some(namespace) = prefixes.get(prefix) {
            return format!("{namespace}{local}");
        }
    }
    compact.to_string()
}

/// Build a JSON-LD @context object from stored prefixes.
pub fn prefixes_to_context(fluree_dir: &Path) -> serde_json::Value {
    let prefixes = read_prefixes(fluree_dir);
    serde_json::Value::Object(
        prefixes
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect(),
    )
}

// --- Sync Configuration (remotes and upstreams) ---

use async_trait::async_trait;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteConfig, RemoteEndpoint, SyncConfigStore, UpstreamConfig,
};
use serde::{Deserialize, Serialize};

/// Configuration for a tracked (remote-only) ledger.
///
/// Tracked ledgers have no local data — all operations are proxied to the
/// remote server via HTTP. This is distinct from upstreams, which track
/// a local ledger's relationship to a remote for ref-level sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedLedgerConfig {
    pub local_alias: String,
    pub remote: String,
    pub remote_alias: String,
}

/// TOML structure for sync configuration in config.toml
#[derive(Debug, Default, Serialize, Deserialize)]
struct SyncToml {
    #[serde(default)]
    remotes: Vec<RemoteConfigToml>,
    #[serde(default)]
    upstreams: Vec<UpstreamConfig>,
    #[serde(default)]
    tracked_ledgers: Vec<TrackedLedgerConfig>,
}

/// TOML-friendly remote config (converts RemoteName to String)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteConfigToml {
    name: String,
    #[serde(flatten)]
    endpoint: RemoteEndpoint,
    #[serde(default)]
    auth: RemoteAuth,
    fetch_interval_secs: Option<u64>,
}

impl From<RemoteConfig> for RemoteConfigToml {
    fn from(c: RemoteConfig) -> Self {
        Self {
            name: c.name.as_str().to_string(),
            endpoint: c.endpoint,
            auth: c.auth,
            fetch_interval_secs: c.fetch_interval_secs,
        }
    }
}

impl From<RemoteConfigToml> for RemoteConfig {
    fn from(c: RemoteConfigToml) -> Self {
        Self {
            name: RemoteName::new(&c.name),
            endpoint: c.endpoint,
            auth: c.auth,
            fetch_interval_secs: c.fetch_interval_secs,
        }
    }
}

/// File-backed sync config store using `.fluree/config.toml` or `.fluree/config.jsonld`.
///
/// Detects which config file exists and adapts read/write accordingly.
/// For TOML, uses `toml_edit` to preserve comments and non-sync sections.
/// For JSON-LD, uses `serde_json::Value` to preserve `@context` and non-sync sections.
#[derive(Debug)]
pub struct TomlSyncConfigStore {
    fluree_dir: PathBuf,
}

impl TomlSyncConfigStore {
    pub fn new(fluree_dir: PathBuf) -> Self {
        Self { fluree_dir }
    }

    /// Returns `(path, format)` for the active config file.
    /// Defaults to TOML path if neither file exists.
    fn config_file(&self) -> (PathBuf, ConfigFileFormat) {
        detect_config_file(&self.fluree_dir).unwrap_or_else(|| {
            (
                self.fluree_dir.join(CONFIG_FILE_TOML),
                ConfigFileFormat::Toml,
            )
        })
    }

    fn read_sync_config(&self) -> SyncToml {
        let (path, format) = self.config_file();
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return SyncToml::default(),
        };
        match format {
            ConfigFileFormat::Toml => toml::from_str(&content).unwrap_or_default(),
            ConfigFileFormat::JsonLd => serde_json::from_str(&content).unwrap_or_default(),
        }
    }

    /// Write sync config, dispatching to format-specific writer.
    fn write_sync_config(&self, config: &SyncToml) -> CliResult<()> {
        let (path, format) = self.config_file();
        match format {
            ConfigFileFormat::Toml => self.write_sync_config_toml(&path, config),
            ConfigFileFormat::JsonLd => self.write_sync_config_jsonld(&path, config),
        }
    }

    /// Write sync config to a JSON-LD file, preserving `@context`, `server`, etc.
    fn write_sync_config_jsonld(&self, path: &Path, config: &SyncToml) -> CliResult<()> {
        // Read existing file as generic JSON Value to preserve non-sync keys
        let mut doc: serde_json::Value = fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| serde_json::json!({}));

        let obj = doc
            .as_object_mut()
            .ok_or_else(|| CliError::Config("config.jsonld root is not a JSON object".into()))?;

        // Update only sync sections; serde_json handles internally-tagged enums correctly
        if config.remotes.is_empty() {
            obj.remove("remotes");
        } else {
            obj.insert(
                "remotes".into(),
                serde_json::to_value(&config.remotes)
                    .map_err(|e| CliError::Config(e.to_string()))?,
            );
        }

        if config.upstreams.is_empty() {
            obj.remove("upstreams");
        } else {
            obj.insert(
                "upstreams".into(),
                serde_json::to_value(&config.upstreams)
                    .map_err(|e| CliError::Config(e.to_string()))?,
            );
        }

        if config.tracked_ledgers.is_empty() {
            obj.remove("tracked_ledgers");
        } else {
            obj.insert(
                "tracked_ledgers".into(),
                serde_json::to_value(&config.tracked_ledgers)
                    .map_err(|e| CliError::Config(e.to_string()))?,
            );
        }

        let pretty =
            serde_json::to_string_pretty(&doc).map_err(|e| CliError::Config(e.to_string()))?;
        fs::write(path, pretty)
            .map_err(|e| CliError::Config(format!("failed to write config: {e}")))
    }

    /// Write sync config to a TOML file using toml_edit to preserve other keys.
    fn write_sync_config_toml(&self, path: &Path, config: &SyncToml) -> CliResult<()> {
        use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, Value};

        // Parse existing file or start fresh
        let mut doc: DocumentMut = fs::read_to_string(path)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();

        // Build remotes array of tables ([[remotes]])
        let mut remotes_aot = ArrayOfTables::new();
        for remote in &config.remotes {
            let mut table = Table::new();
            table.insert("name", Value::from(remote.name.as_str()).into());

            // RemoteEndpoint uses #[serde(tag = "type")] so we need to write the tag
            match &remote.endpoint {
                RemoteEndpoint::Http { base_url } => {
                    table.insert("type", Value::from("Http").into());
                    table.insert("base_url", Value::from(base_url.as_str()).into());
                }
                RemoteEndpoint::Sse { events_url } => {
                    table.insert("type", Value::from("Sse").into());
                    table.insert("events_url", Value::from(events_url.as_str()).into());
                }
                RemoteEndpoint::Storage { prefix } => {
                    table.insert("type", Value::from("Storage").into());
                    table.insert("prefix", Value::from(prefix.as_str()).into());
                }
            }

            // Build [remotes.auth] sub-table with all populated fields
            {
                use fluree_db_nameservice_sync::RemoteAuthType;

                let auth = &remote.auth;
                let has_any = auth.auth_type.is_some()
                    || auth.token.is_some()
                    || auth.issuer.is_some()
                    || auth.client_id.is_some()
                    || auth.exchange_url.is_some()
                    || auth.refresh_token.is_some()
                    || auth.scopes.is_some()
                    || auth.redirect_port.is_some();

                if has_any {
                    let mut auth_table = Table::new();
                    if let Some(ref at) = auth.auth_type {
                        let type_str = match at {
                            RemoteAuthType::Token => "token",
                            RemoteAuthType::OidcDevice => "oidc_device",
                        };
                        auth_table.insert("type", Value::from(type_str).into());
                    }
                    if let Some(ref v) = auth.token {
                        auth_table.insert("token", Value::from(v.as_str()).into());
                    }
                    if let Some(ref v) = auth.issuer {
                        auth_table.insert("issuer", Value::from(v.as_str()).into());
                    }
                    if let Some(ref v) = auth.client_id {
                        auth_table.insert("client_id", Value::from(v.as_str()).into());
                    }
                    if let Some(ref v) = auth.exchange_url {
                        auth_table.insert("exchange_url", Value::from(v.as_str()).into());
                    }
                    if let Some(ref v) = auth.refresh_token {
                        auth_table.insert("refresh_token", Value::from(v.as_str()).into());
                    }
                    if let Some(ref scopes) = auth.scopes {
                        let arr: toml_edit::Array =
                            scopes.iter().map(std::string::String::as_str).collect();
                        auth_table.insert("scopes", Value::from(arr).into());
                    }
                    if let Some(port) = auth.redirect_port {
                        auth_table.insert("redirect_port", Value::from(i64::from(port)).into());
                    }
                    table.insert("auth", Item::Table(auth_table));
                }
            }

            if let Some(interval) = remote.fetch_interval_secs {
                table.insert("fetch_interval_secs", Value::from(interval as i64).into());
            }

            remotes_aot.push(table);
        }

        // Build upstreams array of tables ([[upstreams]])
        let mut upstreams_aot = ArrayOfTables::new();
        for upstream in &config.upstreams {
            let mut table = Table::new();
            table.insert(
                "local_alias",
                Value::from(upstream.local_alias.as_str()).into(),
            );
            table.insert("remote", Value::from(upstream.remote.as_str()).into());
            table.insert(
                "remote_alias",
                Value::from(upstream.remote_alias.as_str()).into(),
            );
            table.insert("auto_pull", Value::from(upstream.auto_pull).into());
            upstreams_aot.push(table);
        }

        // Build tracked_ledgers array of tables ([[tracked_ledgers]])
        let mut tracked_aot = ArrayOfTables::new();
        for tracked in &config.tracked_ledgers {
            let mut table = Table::new();
            table.insert(
                "local_alias",
                Value::from(tracked.local_alias.as_str()).into(),
            );
            table.insert("remote", Value::from(tracked.remote.as_str()).into());
            table.insert(
                "remote_alias",
                Value::from(tracked.remote_alias.as_str()).into(),
            );
            tracked_aot.push(table);
        }

        // Update only sync-related keys, preserving everything else
        if config.remotes.is_empty() {
            doc.remove("remotes");
        } else {
            doc["remotes"] = Item::ArrayOfTables(remotes_aot);
        }

        if config.upstreams.is_empty() {
            doc.remove("upstreams");
        } else {
            doc["upstreams"] = Item::ArrayOfTables(upstreams_aot);
        }

        if config.tracked_ledgers.is_empty() {
            doc.remove("tracked_ledgers");
        } else {
            doc["tracked_ledgers"] = Item::ArrayOfTables(tracked_aot);
        }

        fs::write(path, doc.to_string())
            .map_err(|e| CliError::Config(format!("failed to write config: {e}")))
    }
}

#[async_trait]
impl SyncConfigStore for TomlSyncConfigStore {
    async fn get_remote(
        &self,
        name: &RemoteName,
    ) -> fluree_db_nameservice_sync::Result<Option<RemoteConfig>> {
        let config = self.read_sync_config();
        Ok(config
            .remotes
            .into_iter()
            .find(|r| r.name == name.as_str())
            .map(RemoteConfig::from))
    }

    async fn set_remote(&self, remote: &RemoteConfig) -> fluree_db_nameservice_sync::Result<()> {
        let mut config = self.read_sync_config();
        let toml_config = RemoteConfigToml::from(remote.clone());

        // Replace existing or add new
        if let Some(pos) = config
            .remotes
            .iter()
            .position(|r| r.name == remote.name.as_str())
        {
            config.remotes[pos] = toml_config;
        } else {
            config.remotes.push(toml_config);
        }

        self.write_sync_config(&config)
            .map_err(|e| fluree_db_nameservice_sync::SyncError::Config(e.to_string()))
    }

    async fn remove_remote(&self, name: &RemoteName) -> fluree_db_nameservice_sync::Result<()> {
        let mut config = self.read_sync_config();
        config.remotes.retain(|r| r.name != name.as_str());
        self.write_sync_config(&config)
            .map_err(|e| fluree_db_nameservice_sync::SyncError::Config(e.to_string()))
    }

    async fn list_remotes(&self) -> fluree_db_nameservice_sync::Result<Vec<RemoteConfig>> {
        let config = self.read_sync_config();
        Ok(config.remotes.into_iter().map(RemoteConfig::from).collect())
    }

    async fn get_upstream(
        &self,
        local_alias: &str,
    ) -> fluree_db_nameservice_sync::Result<Option<UpstreamConfig>> {
        let config = self.read_sync_config();
        Ok(config
            .upstreams
            .into_iter()
            .find(|u| u.local_alias == local_alias))
    }

    async fn set_upstream(
        &self,
        upstream: &UpstreamConfig,
    ) -> fluree_db_nameservice_sync::Result<()> {
        let mut config = self.read_sync_config();

        // Replace existing or add new
        if let Some(pos) = config
            .upstreams
            .iter()
            .position(|u| u.local_alias == upstream.local_alias)
        {
            config.upstreams[pos] = upstream.clone();
        } else {
            config.upstreams.push(upstream.clone());
        }

        self.write_sync_config(&config)
            .map_err(|e| fluree_db_nameservice_sync::SyncError::Config(e.to_string()))
    }

    async fn remove_upstream(&self, local_alias: &str) -> fluree_db_nameservice_sync::Result<()> {
        let mut config = self.read_sync_config();
        config.upstreams.retain(|u| u.local_alias != local_alias);
        self.write_sync_config(&config)
            .map_err(|e| fluree_db_nameservice_sync::SyncError::Config(e.to_string()))
    }

    async fn list_upstreams(&self) -> fluree_db_nameservice_sync::Result<Vec<UpstreamConfig>> {
        let config = self.read_sync_config();
        Ok(config.upstreams)
    }
}

// --- Tracked Ledger Operations (CLI-only, not part of SyncConfigStore trait) ---

impl TomlSyncConfigStore {
    /// List all tracked ledgers.
    pub fn tracked_ledgers(&self) -> Vec<TrackedLedgerConfig> {
        self.read_sync_config().tracked_ledgers
    }

    /// Get a tracked ledger by local name.
    pub fn get_tracked(&self, local_alias: &str) -> Option<TrackedLedgerConfig> {
        self.read_sync_config()
            .tracked_ledgers
            .into_iter()
            .find(|t| t.local_alias == local_alias)
    }

    /// Add a tracked ledger. Replaces if the name already exists.
    pub fn add_tracked(&self, tracked: TrackedLedgerConfig) -> CliResult<()> {
        let mut config = self.read_sync_config();

        if let Some(pos) = config
            .tracked_ledgers
            .iter()
            .position(|t| t.local_alias == tracked.local_alias)
        {
            config.tracked_ledgers[pos] = tracked;
        } else {
            config.tracked_ledgers.push(tracked);
        }

        self.write_sync_config(&config)
    }

    /// Remove a tracked ledger by local name. Returns true if it existed.
    pub fn remove_tracked(&self, local_alias: &str) -> CliResult<bool> {
        let mut config = self.read_sync_config();
        let before = config.tracked_ledgers.len();
        config
            .tracked_ledgers
            .retain(|t| t.local_alias != local_alias);
        let removed = config.tracked_ledgers.len() < before;
        if removed {
            self.write_sync_config(&config)?;
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env-var tests must run serially because they mutate process-wide state.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Safety: these tests hold ENV_LOCK so no concurrent env mutation.
    /// set_var/remove_var are safe in edition 2021 but wrap in unsafe for
    /// forward-compatibility with edition 2024.
    unsafe fn set_env(key: &str, val: &std::ffi::OsStr) {
        std::env::set_var(key, val);
    }
    unsafe fn unset_env(key: &str) {
        std::env::remove_var(key);
    }

    #[test]
    fn resolve_init_dirs_local_returns_unified() {
        let dirs = resolve_init_dirs(false).unwrap();
        assert!(dirs.is_unified(), "local mode should be unified");
        assert!(
            dirs.data_dir().ends_with(FLUREE_DIR),
            "data dir should end with .fluree"
        );
    }

    #[test]
    fn resolve_init_dirs_global_returns_some() {
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: holding ENV_LOCK, no concurrent env mutation.
        unsafe { set_env("FLUREE_HOME", tmp.path().as_os_str()) };
        let result = resolve_init_dirs(true);
        unsafe { unset_env("FLUREE_HOME") };
        let dirs = result.unwrap();
        assert!(
            dirs.is_unified(),
            "FLUREE_HOME should produce a unified dir"
        );
        assert_eq!(dirs.data_dir(), tmp.path());
    }

    #[test]
    fn fluree_dir_global_respects_fluree_home() {
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: holding ENV_LOCK.
        unsafe { set_env("FLUREE_HOME", tmp.path().as_os_str()) };
        let dirs = FlureeDir::global().unwrap();
        unsafe { unset_env("FLUREE_HOME") };
        assert!(dirs.is_unified());
        assert_eq!(dirs.config_dir(), tmp.path());
        assert_eq!(dirs.data_dir(), tmp.path());
    }

    #[test]
    fn fluree_dir_global_without_fluree_home() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: holding ENV_LOCK.
        unsafe { unset_env("FLUREE_HOME") };
        // Without FLUREE_HOME, global() should still return Some on all
        // platforms (dirs::config_local_dir() is always available).
        let dirs = FlureeDir::global();
        assert!(dirs.is_some(), "global() should resolve on this platform");
    }

    #[test]
    fn find_or_global_accepts_existing_config_dir_only() {
        let _guard = ENV_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let config_dir = tmp.path().join("config");
        let data_dir = tmp.path().join("data");
        // Create only config dir
        std::fs::create_dir_all(&config_dir).unwrap();
        // SAFETY: holding ENV_LOCK.
        unsafe { set_env("FLUREE_HOME", config_dir.as_os_str()) };
        let result = find_or_global_fluree_dir();
        unsafe { unset_env("FLUREE_HOME") };
        assert!(result.is_some(), "should find existing config dir");
        // data_dir should not exist but that's fine — we only require one
        assert!(!data_dir.exists());
    }

    #[test]
    fn resolve_storage_path_uses_config_when_set() {
        let tmp = tempfile::tempdir().unwrap();
        let fluree_dir = tmp.path().join(".fluree");
        std::fs::create_dir_all(fluree_dir.join("storage")).unwrap();
        std::fs::write(
            fluree_dir.join("config.toml"),
            "[server]\nstorage_path = \"/custom/storage\"\n",
        )
        .unwrap();

        let dirs = FlureeDir::unified(fluree_dir);
        let result = resolve_storage_path(&dirs);
        assert_eq!(result, PathBuf::from("/custom/storage"));
    }

    #[test]
    fn resolve_storage_path_falls_back_to_default() {
        let tmp = tempfile::tempdir().unwrap();
        let fluree_dir = tmp.path().join(".fluree");
        std::fs::create_dir_all(fluree_dir.join("storage")).unwrap();
        // Config exists but storage_path is commented out
        std::fs::write(
            fluree_dir.join("config.toml"),
            "# [server]\n# storage_path = \".fluree/storage\"\n",
        )
        .unwrap();

        let dirs = FlureeDir::unified(fluree_dir.clone());
        let result = resolve_storage_path(&dirs);
        assert_eq!(result, fluree_dir.join("storage"));
    }

    #[test]
    fn resolve_storage_path_no_config_file() {
        let tmp = tempfile::tempdir().unwrap();
        let fluree_dir = tmp.path().join(".fluree");
        std::fs::create_dir_all(&fluree_dir).unwrap();
        // No config file at all

        let dirs = FlureeDir::unified(fluree_dir.clone());
        let result = resolve_storage_path(&dirs);
        assert_eq!(result, fluree_dir.join("storage"));
    }
}
