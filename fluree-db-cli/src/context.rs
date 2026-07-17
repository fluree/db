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

/// A resolved `fluree query` target.
///
/// Extends [`LedgerMode`] with a graph-source arm: a name that isn't a native
/// ledger (or tracked config) but *is* a locally-registered Iceberg/R2RML graph
/// source resolves here instead of failing with `NotFound`. The query command
/// runs it through the R2RML-aware single-target builder (`fluree.graph().query()`),
/// which the standalone `db()` + `query()` snapshot path can't reach.
pub enum QueryTarget {
    /// A native ledger (local or tracked) — the traditional query path.
    Ledger(LedgerMode),
    /// A locally-registered graph source (Iceberg/R2RML). `alias` is normalized
    /// to `<name>:main` for routing.
    GraphSource { fluree: Box<Fluree>, alias: String },
    /// A peer-tracked ledger: queries execute locally against index blocks
    /// fetched on demand from the remote's raw storage tier (CID-verified,
    /// cached on disk per remote). Only queries take this arm —
    /// [`resolve_ledger_mode`] downgrades it to [`LedgerMode::Tracked`] so
    /// every other command keeps forwarding over HTTP.
    Peer {
        /// Fluree wired to the remote via `ProxyStorage` (raw mode) +
        /// `ProxyNameService`; queries run through the normal local path
        /// against `remote_alias`.
        fluree: Box<Fluree>,
        /// HTTP client for the downgrade-to-Tracked path.
        client: Box<RemoteLedgerClient>,
        /// The alias on the remote server (the peer Fluree resolves this
        /// directly — no local prefix).
        remote_alias: String,
        /// The local alias the user used.
        local_alias: String,
        /// The remote config name.
        remote_name: String,
    },
}

/// Resolve a `fluree query` target, distinguishing native ledgers from
/// registered graph sources.
///
/// Resolution precedence (first match wins):
/// 1. Compound `remote/ledger` syntax (e.g., "origin/mydb") → tracked remote.
/// 2. Local native ledger with this alias → [`LedgerMode::Local`].
/// 3. Local graph source (Iceberg/R2RML) with this name → [`QueryTarget::GraphSource`].
///    Graph sources live under a separate nameservice key than native ledgers,
///    so the `ledger_exists` check (a `lookup`) never sees them; this probe is
///    what lets `fluree query <graph-source>` resolve instead of returning
///    NotFound.
/// 4. Tracked config for this alias → [`LedgerMode::Tracked`].
/// 5. Error (NotFound).
///
/// A locally-registered graph source wins over a tracked remote of the same
/// name, mirroring the "local wins" rule already applied to native ledgers.
pub async fn resolve_query_target(
    explicit: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<QueryTarget> {
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
        return Ok(QueryTarget::Ledger(mode));
    }

    let fluree = build_fluree(dirs)?;

    // Check if local ledger exists (local wins)
    let ledger_id = to_ledger_id(ledger_part);
    if fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Ok(QueryTarget::Ledger(LedgerMode::Local {
            fluree: Box::new(fluree),
            alias,
        }));
    }

    // Check if a graph source (Iceberg/R2RML) is registered under this name.
    // Graph sources are keyed separately from native ledgers in the
    // nameservice, so the `ledger_exists` check above never sees them — this is
    // the fix for #1398 (`fluree query <graph-source>` → "ledger not found").
    // A non-retracted local graph source wins over a tracked remote of the same
    // name, consistent with the "local wins" rule for native ledgers.
    //
    // This probe now runs for every command that resolves a ledger (via the
    // `resolve_ledger_mode` wrapper), so a graph-source-store *read error* must
    // not break ledger resolution: treat any `Err` as "not a graph source" and
    // fall through to the tracked-config / NotFound path exactly as before.
    match fluree.nameservice().lookup_graph_source(&ledger_id).await {
        Ok(Some(record)) if !record.retracted => {
            return Ok(QueryTarget::GraphSource {
                fluree: Box::new(fluree),
                alias: ledger_id,
            });
        }
        // Absent or retracted — not a queryable graph source; fall through.
        Ok(_) => {}
        Err(e) => {
            tracing::debug!(
                graph_source = %ledger_id,
                error = ?e,
                "graph-source probe failed; treating as not a graph source"
            );
        }
    }

    // Check tracked config
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if let Some(tracked) = store.get_tracked(ledger_part) {
        return build_tracked_target(&store, &tracked, ledger_part).await;
    }

    // Also try the normalized ledger_id (user might have typed "mydb" but tracked as "mydb:main")
    if ledger_part != ledger_id {
        if let Some(tracked) = store.get_tracked(&ledger_id) {
            return build_tracked_target(&store, &tracked, &ledger_id).await;
        }
    }

    // Also try the base name without branch suffix (user typed "mydb:main" but tracked as "mydb").
    // This handles configs created before track-time normalization was added.
    if let Some(base) = ledger_part.split(':').next() {
        if base != ledger_part && base != ledger_id {
            if let Some(tracked) = store.get_tracked(base) {
                return build_tracked_target(&store, &tracked, ledger_part).await;
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

/// Resolve which ledger to operate on and how (local vs tracked).
///
/// This is the ledger-only view of [`resolve_query_target`]: every command
/// except `query` operates on a native ledger, so a name that resolves to a
/// graph source is reported here as a (clear) error pointing at `fluree query`.
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
    match resolve_query_target(explicit, dirs).await? {
        QueryTarget::Ledger(mode) => Ok(mode),
        // Peer mode only changes where QUERIES execute; every other command
        // (writes, admin, history) forwards over HTTP exactly like a
        // proxy-tracked ledger.
        QueryTarget::Peer {
            client,
            remote_alias,
            local_alias,
            remote_name,
            ..
        } => Ok(LedgerMode::Tracked {
            client,
            remote_alias,
            local_alias,
            remote_name,
        }),
        QueryTarget::GraphSource { alias, .. } => Err(CliError::NotFound(format!(
            "'{alias}' is a registered graph source, not a ledger.\n  \
             Query it with `fluree query {alias}`, or inspect it with `fluree iceberg info {alias}`."
        ))),
    }
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

/// Build a query target from a tracked config entry, honoring its
/// [`TrackMode`](crate::config::TrackMode) (proxy → HTTP query-shipping,
/// peer → local execution over remotely-fetched blocks).
async fn build_tracked_target(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<QueryTarget> {
    if tracked.mode == crate::config::TrackMode::Peer {
        return build_peer_target(store, tracked, local_alias).await;
    }
    Ok(QueryTarget::Ledger(
        build_tracked_mode(store, tracked, local_alias).await?,
    ))
}

/// Resolve a tracked entry's remote to its HTTP endpoint + auth.
async fn tracked_remote_http(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<(String, fluree_db_nameservice_sync::RemoteAuth)> {
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

    match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => Ok((base_url.clone(), remote.auth)),
        _ => Err(CliError::Config(format!(
            "remote '{}' is not an HTTP remote; tracking requires HTTP",
            tracked.remote
        ))),
    }
}

/// Build a `LedgerMode::Tracked` from a tracked config entry.
async fn build_tracked_mode(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<LedgerMode> {
    let (base_url, auth) = tracked_remote_http(store, tracked, local_alias).await?;
    let client = build_client_from_auth(&base_url, &auth);
    Ok(LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias: tracked.remote_alias.clone(),
        local_alias: local_alias.to_string(),
        remote_name: tracked.remote.clone(),
    })
}

/// Build a [`QueryTarget::Peer`]: an embedded Fluree whose reads go to the
/// remote's raw storage tier with a persistent per-remote disk cache for
/// index artifacts.
///
/// Storage negotiation: when the remote vends S3 credentials for the ledger
/// (`GET /storage/credentials`), reads go directly to S3 with auto-refreshed
/// scoped credentials; otherwise blocks proxy through the remote's HTTP
/// endpoints (`ProxyStorage` in Raw mode, CID-verified).
async fn build_peer_target(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<QueryTarget> {
    use fluree_db_api::{Fluree, LedgerManagerConfig, NameServiceMode};
    use fluree_db_nameservice_sync::{ProxyNameService, ProxyReadMode, ProxyStorage};

    let (base_url, auth) = tracked_remote_http(store, tracked, local_alias).await?;
    let token = auth.token.clone().ok_or_else(|| {
        CliError::Config(format!(
            "tracked ledger '{local_alias}' uses peer mode, which requires a bearer token \
             with storage scope for remote '{}'.\n  \
             Log in with `fluree auth login {}` (or `--token`).",
            tracked.remote, tracked.remote
        ))
    })?;
    let client = build_client_from_auth(&base_url, &auth);

    // Persistent, per-remote artifact cache. Everything cached is
    // content-addressed and immutable, so entries never invalidate. The
    // ledger head is the only per-query remote state: each CLI invocation
    // builds a fresh in-process Fluree, so the first load always resolves
    // the head through the proxy nameservice.
    let cache_config = LedgerManagerConfig {
        cache_dir: peer_cache_dir(&tracked.remote),
        ..Default::default()
    };

    fn assemble(
        cache_config: LedgerManagerConfig,
        storage: impl fluree_db_api::Storage + 'static,
        ns: ProxyNameService,
    ) -> Fluree {
        FlureeBuilder::memory()
            .with_ledger_cache_config(cache_config)
            .build_with(storage, NameServiceMode::ReadOnly(std::sync::Arc::new(ns)))
    }

    // Remote base URLs are API bases (ending in `/fluree`), so use the
    // api-base constructors rather than the server-root ones.
    let ns = ProxyNameService::from_api_base(base_url.clone(), token.clone());

    #[cfg(feature = "aws")]
    let vended = match fluree_db_nameservice_sync::vended_s3::build_vended_s3_storage(
        base_url.clone(),
        token.clone(),
        tracked.remote_alias.clone(),
    )
    .await
    {
        Ok(storage) => storage,
        Err(e) => {
            tracing::debug!(error = %e, "vended credential probe failed; using proxied reads");
            None
        }
    };
    #[cfg(not(feature = "aws"))]
    let vended: Option<std::convert::Infallible> = None;

    let fluree = match vended {
        #[cfg(feature = "aws")]
        Some(s3) => {
            eprintln!(
                "  {} reading '{}' directly from S3 (vended credentials)",
                "notice:".dimmed(),
                tracked.remote_alias
            );
            assemble(cache_config, s3, ns)
        }
        #[cfg(not(feature = "aws"))]
        Some(_) => unreachable!(),
        None => {
            let storage = ProxyStorage::from_api_base(base_url, token, ProxyReadMode::Raw);
            assemble(cache_config, storage, ns)
        }
    };

    Ok(QueryTarget::Peer {
        fluree: Box::new(fluree),
        client: Box::new(client),
        remote_alias: tracked.remote_alias.clone(),
        local_alias: local_alias.to_string(),
        remote_name: tracked.remote.clone(),
    })
}

/// Disk cache root for peer-mode index artifacts, per remote.
///
/// Content is CID-addressed and immutable, so the cache is shared across
/// projects and safe to delete at any time (`fluree cache clear`).
pub fn peer_cache_dir(remote_name: &str) -> std::path::PathBuf {
    peer_cache_root().join(remote_name)
}

/// Root directory for all peer-mode caches.
pub fn peer_cache_root() -> std::path::PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("fluree")
        .join("peer-cache")
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
    } else if auth.refresh_token.is_some() {
        // Auto-refresh only engages for `oidc_device`; a refresh_token under any
        // other auth type is dead config that will never be used. Warn rather
        // than silently ignore it.
        eprintln!(
            "warning: 'refresh_token' is set but auth type is not 'oidc_device'; it will be ignored"
        );
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

/// Build the Fluree instance that backs the developer-memory store
/// (`mcp serve`, `fluree memory`).
///
/// Unlike [`build_fluree`], this uses a **process-private, in-memory** ledger
/// rather than the shared on-disk `.fluree/storage`. The git-tracked
/// `.fluree-memory/*.ttl` files remain the durable source of truth; the ledger
/// is a disposable query cache rebuilt from them on startup. Keeping it private
/// is what makes concurrent access safe: several memory helpers can run at once
/// (one per IDE/agent session, plus overlapping CLI invocations) without one
/// process's cache rebuild deleting commits another process is reading. It also
/// right-sizes memory — an in-memory ledger never holds the server's RAM-tiered
/// leaflet cache — and the cap below keeps that explicit.
pub fn build_memory_fluree() -> Fluree {
    FlureeBuilder::memory()
        .cache_max_mb(fluree_db_api::server_defaults::DEFAULT_MEMORY_HELPER_CACHE_MAX_MB)
        .build_memory()
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

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_nameservice::GraphSourceType;

    /// A fresh, isolated `FlureeDir` backed by a temp directory. `build_fluree`
    /// falls back to `<dir>/storage`, so no `fluree init` is needed.
    fn temp_dirs() -> (tempfile::TempDir, FlureeDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dirs = FlureeDir::unified(tmp.path().to_path_buf());
        (tmp, dirs)
    }

    /// Register a graph source directly through the publisher. This bypasses
    /// `fluree iceberg map` (which needs a live catalog) so the resolution
    /// branch can be tested in isolation.
    async fn register_graph_source(dirs: &FlureeDir, name: &str) {
        let fluree = build_fluree(dirs).unwrap();
        fluree
            .publisher()
            .unwrap()
            .publish_graph_source(name, "main", GraphSourceType::R2rml, "{}", &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn resolves_registered_graph_source_to_graph_source_target() {
        let (_tmp, dirs) = temp_dirs();
        register_graph_source(&dirs, "warehouse-orders").await;

        // The query path resolves it as a graph source (normalized to :main),
        // not as a missing ledger.
        let target = resolve_query_target(Some("warehouse-orders"), &dirs)
            .await
            .unwrap();
        match target {
            QueryTarget::GraphSource { alias, .. } => {
                assert_eq!(alias, "warehouse-orders:main");
            }
            QueryTarget::Ledger(_) | QueryTarget::Peer { .. } => {
                panic!("expected a GraphSource target, got a ledger")
            }
        }
    }

    #[tokio::test]
    async fn ledger_only_resolution_reports_graph_source_as_clear_error() {
        let (_tmp, dirs) = temp_dirs();
        register_graph_source(&dirs, "warehouse-orders").await;

        // Non-query commands resolve through `resolve_ledger_mode`, which must
        // not treat the graph source as a ledger — it errors with a pointer to
        // `fluree query` rather than the generic "not found" message.
        // (`LedgerMode` isn't `Debug`, so match rather than `unwrap_err`.)
        match resolve_ledger_mode(Some("warehouse-orders"), &dirs).await {
            Err(CliError::NotFound(msg)) => {
                assert!(msg.contains("graph source"), "unexpected message: {msg}");
                assert!(msg.contains("fluree query"), "unexpected message: {msg}");
            }
            Ok(_) => panic!("expected an error, resolved as a ledger"),
            Err(other) => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_target_still_reports_not_found() {
        let (_tmp, dirs) = temp_dirs();

        // A genuinely-missing name keeps the original, ledger-oriented message
        // on both resolution paths (regression guard).
        // (`QueryTarget` isn't `Debug`, so match rather than `unwrap_err`.)
        match resolve_query_target(Some("nope"), &dirs).await {
            Err(CliError::NotFound(msg)) => {
                assert!(
                    msg.contains("not found locally or in tracked config"),
                    "unexpected message: {msg}"
                );
            }
            Ok(_) => panic!("expected NotFound for a missing target"),
            Err(other) => panic!("expected NotFound, got {other:?}"),
        }
    }

    /// Seed a remote + tracked entry with the given mode and token.
    async fn seed_tracked(dirs: &FlureeDir, mode: crate::config::TrackMode, token: Option<&str>) {
        use fluree_db_nameservice_sync::{RemoteAuth, RemoteConfig, RemoteEndpoint};
        let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
        store
            .set_remote(&RemoteConfig {
                name: RemoteName::new("origin"),
                endpoint: RemoteEndpoint::Http {
                    base_url: "http://127.0.0.1:1/v1/fluree".to_string(),
                },
                auth: RemoteAuth {
                    token: token.map(str::to_string),
                    ..Default::default()
                },
                fetch_interval_secs: None,
            })
            .await
            .unwrap();
        store
            .add_tracked(TrackedLedgerConfig {
                local_alias: "inv:main".to_string(),
                remote: "origin".to_string(),
                remote_alias: "inventory:main".to_string(),
                mode,
            })
            .unwrap();
    }

    /// A peer-tracked ledger resolves to `QueryTarget::Peer` for queries
    /// (local execution) while `resolve_ledger_mode` downgrades it to
    /// `Tracked` so writes keep forwarding over HTTP. No network is
    /// touched — building the peer target is offline.
    #[tokio::test]
    async fn peer_tracked_ledger_resolves_to_peer_target() {
        let (_tmp, dirs) = temp_dirs();
        seed_tracked(&dirs, crate::config::TrackMode::Peer, Some("tok")).await;

        match resolve_query_target(Some("inv:main"), &dirs).await {
            Ok(QueryTarget::Peer {
                remote_alias,
                local_alias,
                remote_name,
                ..
            }) => {
                assert_eq!(remote_alias, "inventory:main");
                assert_eq!(local_alias, "inv:main");
                assert_eq!(remote_name, "origin");
            }
            Ok(_) => panic!("expected a Peer target"),
            Err(e) => panic!("resolution failed: {e:?}"),
        }

        match resolve_ledger_mode(Some("inv:main"), &dirs).await {
            Ok(LedgerMode::Tracked { remote_alias, .. }) => {
                assert_eq!(remote_alias, "inventory:main");
            }
            Ok(LedgerMode::Local { .. }) => {
                panic!("peer must downgrade to Tracked for non-query commands")
            }
            Err(e) => panic!("downgrade resolution failed: {e:?}"),
        }
    }

    /// Peer mode without a configured token fails with a pointer to
    /// `fluree auth login`, not an opaque network error.
    #[tokio::test]
    async fn peer_tracked_ledger_without_token_errors_clearly() {
        let (_tmp, dirs) = temp_dirs();
        seed_tracked(&dirs, crate::config::TrackMode::Peer, None).await;

        match resolve_query_target(Some("inv:main"), &dirs).await {
            Err(CliError::Config(msg)) => {
                assert!(msg.contains("peer mode"), "unexpected message: {msg}");
                assert!(
                    msg.contains("fluree auth login"),
                    "unexpected message: {msg}"
                );
            }
            Ok(_) => panic!("expected a config error"),
            Err(other) => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Default (proxy) tracked entries keep resolving to `Tracked`.
    #[tokio::test]
    async fn proxy_tracked_ledger_resolves_to_tracked() {
        let (_tmp, dirs) = temp_dirs();
        seed_tracked(&dirs, crate::config::TrackMode::Proxy, Some("tok")).await;

        match resolve_query_target(Some("inv:main"), &dirs).await {
            Ok(QueryTarget::Ledger(LedgerMode::Tracked { remote_alias, .. })) => {
                assert_eq!(remote_alias, "inventory:main");
            }
            Ok(_) => panic!("expected a Tracked target"),
            Err(e) => panic!("resolution failed: {e:?}"),
        }
    }
}
