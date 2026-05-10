use crate::config::{self, TomlSyncConfigStore, TrackedLedgerConfig};
use crate::error::{CliError, CliResult};
use crate::remote_client::{RefreshConfig, RemoteLedgerClient};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};
use serde::Deserialize;
use std::fs;
use std::sync::OnceLock;
use std::time::Duration;

/// Global HTTP timeout for remote operations, set once from CLI args.
static REMOTE_TIMEOUT: OnceLock<Duration> = OnceLock::new();

/// Set the remote HTTP timeout (called once at startup from CLI args).
pub fn set_remote_timeout(timeout: Duration) {
    let _ = REMOTE_TIMEOUT.set(timeout);
}

/// Get the configured remote HTTP timeout.
fn remote_timeout() -> Duration {
    REMOTE_TIMEOUT
        .get()
        .copied()
        .unwrap_or(RemoteLedgerClient::DEFAULT_TIMEOUT)
}

/// Resolved ledger mode: either local or tracked (remote-only).
pub enum LedgerMode {
    /// Local ledger via Fluree API (traditional path).
    Local { fluree: Box<Fluree>, alias: String },
    /// Remote-only tracked ledger via HTTP.
    Tracked {
        client: Box<RemoteLedgerClient>,
        /// The alias on the remote server.
        remote_alias: String,
        /// The local alias the user used.
        local_alias: String,
        /// The remote config name (for persisting refreshed tokens).
        remote_name: String,
    },
}

/// Resolve which ledger to operate on and how (local vs tracked).
///
/// Resolution precedence:
/// 1. `--remote <name>` flag → temporary RemoteLedgerClient (caller provides this)
/// 2. Compound `remote/ledger` syntax (e.g., "origin/mydb") → remote query
/// 3. Local ledger with this alias exists → LedgerMode::Local
/// 4. Tracked config for this alias exists → LedgerMode::Tracked
/// 5. Error
pub async fn resolve_ledger_mode(
    explicit: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<LedgerMode> {
    let alias = resolve_ledger(explicit, dirs)?;

    // Strip `#fragment` (e.g., `#txn-meta`) for ledger resolution.
    // The fragment selects a named graph and is handled later by
    // `fluree.view()` / `parse_graph_ref()`. Existence checks and
    // tracked-config lookups must use just the ledger portion.
    let (ledger_part, _graph_fragment) = match alias.split_once('#') {
        Some((base, _frag)) => (base, Some(_frag)),
        None => (alias.as_str(), None),
    };

    // Try compound remote/ledger syntax (e.g., "origin/mydb")
    if let Some(mode) = try_compound_remote_syntax(ledger_part, dirs).await? {
        // For remote mode, pass through the full alias (with fragment) so the
        // server can resolve the graph. Currently remote doesn't support this,
        // but it avoids silently dropping the fragment.
        return Ok(mode);
    }

    let fluree = build_fluree(dirs)?;

    // Check if local ledger exists (local wins)
    let ledger_id = to_ledger_id(ledger_part);
    if fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Ok(LedgerMode::Local {
            fluree: Box::new(fluree),
            alias,
        });
    }

    // Check tracked config
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if let Some(tracked) = store.get_tracked(ledger_part) {
        return build_tracked_mode(&store, &tracked, ledger_part).await;
    }

    // Also try the normalized ledger_id (user might have typed "mydb" but tracked as "mydb:main")
    if ledger_part != ledger_id {
        if let Some(tracked) = store.get_tracked(&ledger_id) {
            return build_tracked_mode(&store, &tracked, &ledger_id).await;
        }
    }

    // Also try the base name without branch suffix (user typed "mydb:main" but tracked as "mydb").
    // This handles configs created before track-time normalization was added.
    if let Some(base) = ledger_part.split(':').next() {
        if base != ledger_part && base != ledger_id {
            if let Some(tracked) = store.get_tracked(base) {
                return build_tracked_mode(&store, &tracked, ledger_part).await;
            }
        }
    }

    // Not found locally or tracked
    let display = ledger_part;
    Err(CliError::NotFound(format!(
        "ledger '{display}' not found locally or in tracked config.\n  \
         Use `fluree create {display}` to create locally, `fluree track add {display}` to track a remote,\n  \
         or use remote/ledger syntax (e.g., origin/{display})."
    )))
}

/// Try to parse `alias` as `remote_name/ledger_alias` compound syntax.
///
/// If the alias contains `/` and the part before the first `/` matches
/// a configured remote name, returns `Some(LedgerMode::Tracked)`.
/// Otherwise returns `None` to let the caller fall through to other resolution.
///
/// Remote names are validated to not contain `/` on `fluree remote add`,
/// so the first `/` is always an unambiguous delimiter.
async fn try_compound_remote_syntax(
    alias: &str,
    dirs: &FlureeDir,
) -> CliResult<Option<LedgerMode>> {
    let slash_pos = match alias.find('/') {
        Some(pos) if pos > 0 => pos,
        _ => return Ok(None),
    };

    let remote_name = &alias[..slash_pos];
    let ledger_alias = &alias[slash_pos + 1..];

    if ledger_alias.is_empty() {
        return Ok(None);
    }

    // Check if a remote with this name exists
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_key = RemoteName::new(remote_name);
    let remote = match store.get_remote(&remote_key).await {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(None), // Not a known remote — fall through
        Err(e) => return Err(CliError::Config(e.to_string())),
    };

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name}' is not an HTTP remote"
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    Ok(Some(LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias: ledger_alias.to_string(),
        local_alias: alias.to_string(),
        remote_name: remote_name.to_string(),
    }))
}

/// Build a `LedgerMode::Tracked` from a tracked config entry.
async fn build_tracked_mode(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<LedgerMode> {
    let remote_name = RemoteName::new(&tracked.remote);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| {
            CliError::Config(format!(
                "remote '{}' referenced by tracked ledger '{}' not found in config",
                tracked.remote, local_alias
            ))
        })?;

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote; tracking requires HTTP",
                tracked.remote
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    Ok(LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias: tracked.remote_alias.clone(),
        local_alias: local_alias.to_string(),
        remote_name: tracked.remote.clone(),
    })
}

/// Build a `LedgerMode::Tracked` for a one-shot --remote flag.
pub async fn build_remote_mode(
    remote_name_str: &str,
    ledger_alias: &str,
    dirs: &FlureeDir,
) -> CliResult<LedgerMode> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_name = RemoteName::new(remote_name_str);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote_name_str}' not found")))?;

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name_str}' is not an HTTP remote"
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    // Canonicalize the remote alias so the URL path carries the full
    // `name:branch` form. The server's `can_read` check is a literal string
    // match against the path, so a token scoped to `mydb:main` would 404 if
    // we sent `mydb` here.
    let remote_alias = to_ledger_id(ledger_alias);
    Ok(LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias,
        local_alias: ledger_alias.to_string(),
        remote_name: remote_name_str.to_string(),
    })
}

/// Build a `RemoteLedgerClient` for a named remote (no ledger alias needed).
///
/// Returns `(client, remote_name)` for use by commands that operate on the
/// remote itself rather than a specific ledger (e.g., `list --remote`).
pub async fn build_remote_client(
    remote_name_str: &str,
    dirs: &FlureeDir,
) -> CliResult<RemoteLedgerClient> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_name = RemoteName::new(remote_name_str);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote_name_str}' not found")))?;

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name_str}' is not an HTTP remote"
            )));
        }
    };

    Ok(build_client_from_auth(&base_url, &remote.auth))
}

/// Build a `RemoteLedgerClient` from auth config, wiring up refresh if available.
pub fn build_client_from_auth(base_url: &str, auth: &RemoteAuth) -> RemoteLedgerClient {
    let client = RemoteLedgerClient::with_timeout(base_url, auth.token.clone(), remote_timeout());

    // Attach refresh config for OIDC remotes that have a refresh_token + exchange_url
    if auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice) {
        if let (Some(exchange_url), Some(refresh_token)) = (&auth.exchange_url, &auth.refresh_token)
        {
            return client.with_refresh(RefreshConfig {
                exchange_url: exchange_url.clone(),
                refresh_token: refresh_token.clone(),
            });
        }
    }

    client
}

/// Resolve which ledger to operate on.
///
/// Priority: explicit argument > active ledger > error.
pub fn resolve_ledger(explicit: Option<&str>, dirs: &FlureeDir) -> CliResult<String> {
    if let Some(alias) = explicit {
        return Ok(alias.to_string());
    }
    config::read_active_ledger(dirs.data_dir()).ok_or(CliError::NoActiveLedger)
}

/// Build a Fluree instance using the resolved storage path.
///
/// Honors `[server].storage_path` and `[server.indexing]` thresholds
/// from the config file if set, otherwise falls back to defaults.
pub fn build_fluree(dirs: &FlureeDir) -> CliResult<Fluree> {
    let storage = config::resolve_storage_path(dirs);
    let storage_str = storage.to_string_lossy().to_string();
    let mut builder = FlureeBuilder::file(storage_str).without_ledger_caching();

    // Apply novelty backpressure thresholds from config file so that
    // limits set via `fluree config set` are respected when executing
    // transactions directly (without a running server).
    // Uses with_novelty_thresholds (not with_indexing_thresholds) because
    // the CLI is a short-lived process — a background indexer would be
    // killed before it could finish.
    let thresholds = config::read_indexing_thresholds(dirs.config_dir());
    let min_bytes = thresholds
        .reindex_min_bytes
        .unwrap_or(fluree_db_api::server_defaults::DEFAULT_REINDEX_MIN_BYTES);
    let max_bytes = thresholds
        .reindex_max_bytes
        .unwrap_or_else(fluree_db_api::server_defaults::default_reindex_max_bytes);
    builder = builder
        .without_indexing()
        .with_novelty_thresholds(min_bytes, max_bytes);

    builder
        .build()
        .map_err(|e| CliError::Config(format!("failed to initialize Fluree: {e}")))
}

/// Normalize a ledger identifier to include a branch suffix if missing.
///
/// The nameservice uses canonical ledger IDs like `mydb:main`.
/// When users provide just `mydb`, we append `:main`.
pub fn to_ledger_id(ledger_id: &str) -> String {
    fluree_db_core::normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string())
}

/// Persist any refreshed tokens back to config.toml after a remote operation.
///
/// If the client performed a silent token refresh during a 401 retry, this
/// writes the new access_token (and optionally rotated refresh_token) back
/// to the remote's auth section in config.toml so subsequent commands use
/// the refreshed credentials.
pub async fn persist_refreshed_tokens(
    client: &RemoteLedgerClient,
    remote_name: &str,
    dirs: &FlureeDir,
) {
    let refreshed = match client.take_refreshed_tokens() {
        Some(t) => t,
        None => return,
    };

    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let name = RemoteName::new(remote_name);

    let remote = match store.get_remote(&name).await {
        Ok(Some(r)) => r,
        _ => return, // Can't persist if remote config disappeared
    };

    let mut updated_auth = remote.auth.clone();
    updated_auth.token = Some(refreshed.access_token);
    if let Some(new_rt) = refreshed.refresh_token {
        updated_auth.refresh_token = Some(new_rt);
    }

    let updated = RemoteConfig {
        auth: updated_auth,
        ..remote
    };

    if store.set_remote(&updated).await.is_err() {
        eprintln!("  warning: failed to persist refreshed token to config");
    }
}

// ---------------------------------------------------------------------------
// Local server auto-routing
// ---------------------------------------------------------------------------

/// Sentinel remote name for local server auto-routing.
/// Token persistence is skipped for this value.
pub const LOCAL_SERVER_REMOTE: &str = "";

/// Minimal mirror of `ServerMeta` from `commands/server.rs`.
/// Only the fields needed for auto-routing are included.
#[derive(Deserialize)]
struct ServerMeta {
    pid: u32,
    listen_addr: String,
}

/// Attempt to route a `LedgerMode::Local` through a locally-running server.
///
/// If `server.meta.json` exists, the PID is alive, and the server process
/// looks like a Fluree server, this returns `LedgerMode::Tracked` pointing
/// to `http://{listen_addr}/fluree`. Otherwise returns the original mode.
///
/// A hint is printed to stderr so the user knows the request was routed.
pub fn try_server_route(mode: LedgerMode, dirs: &FlureeDir) -> LedgerMode {
    let alias = match &mode {
        LedgerMode::Local { alias, .. } => alias.clone(),
        // Already tracked/remote — nothing to do
        LedgerMode::Tracked { .. } => return mode,
    };

    let meta_path = dirs.data_dir().join("server.meta.json");
    let meta: ServerMeta = match fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(m) => m,
        None => return mode, // No server.meta.json or can't parse it
    };

    // Verify the server process is alive
    if !is_process_alive(meta.pid) {
        // Server crashed or was killed — warn and fall back to direct
        eprintln!(
            "  {} local server (pid {}) is no longer running; executing directly",
            "notice:".yellow().bold(),
            meta.pid
        );
        return mode;
    }

    if !is_fluree_process(meta.pid) {
        // PID is alive but not a Fluree server — stale meta file
        eprintln!(
            "  {} pid {} is not a Fluree server; executing directly",
            "notice:".yellow().bold(),
            meta.pid
        );
        return mode;
    }

    // Build HTTP client pointing to the local server.
    // The server mounts its API at /v1/fluree (matches the discovery endpoint's
    // api_base_url). This is the same path that `fluree remote add` resolves via
    // /.well-known/fluree.json.
    let base_url = format!("http://{}/v1/fluree", meta.listen_addr);
    let client = RemoteLedgerClient::with_timeout(&base_url, None, remote_timeout());

    eprintln!(
        "  {} routing through local server at {} (use {} to bypass)",
        "server:".cyan().bold(),
        meta.listen_addr,
        "--direct".bold()
    );

    LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias: alias.clone(),
        local_alias: alias,
        remote_name: LOCAL_SERVER_REMOTE.to_string(),
    }
}

/// Check if a local server is running and return a client for it.
///
/// Used by commands like `list` that don't operate on a specific ledger
/// but can still benefit from server routing.
pub fn try_server_route_client(dirs: &FlureeDir) -> Option<RemoteLedgerClient> {
    let meta_path = dirs.data_dir().join("server.meta.json");
    let meta: ServerMeta = fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())?;

    if !is_process_alive(meta.pid) || !is_fluree_process(meta.pid) {
        return None;
    }

    let base_url = format!("http://{}/v1/fluree", meta.listen_addr);

    eprintln!(
        "  {} routing through local server at {} (use {} to bypass)",
        "server:".cyan().bold(),
        meta.listen_addr,
        "--direct".bold()
    );

    Some(RemoteLedgerClient::with_timeout(
        &base_url,
        None,
        remote_timeout(),
    ))
}

// ---------------------------------------------------------------------------
// Process liveness helpers (mirrored from commands/server.rs)
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    true
}

#[cfg(unix)]
fn is_fluree_process(pid: u32) -> bool {
    if let Ok(raw) = fs::read(format!("/proc/{pid}/cmdline")) {
        let cmdline = String::from_utf8_lossy(&raw);
        return cmdline.contains("fluree") && cmdline.contains("server");
    }
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
    {
        if output.status.success() {
            let cmd = String::from_utf8_lossy(&output.stdout);
            return cmd.contains("fluree") && cmd.contains("server");
        }
    }
    true
}

#[cfg(not(unix))]
fn is_fluree_process(_pid: u32) -> bool {
    true
}
