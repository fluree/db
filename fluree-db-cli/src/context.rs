use crate::config::{self, TomlSyncConfigStore, TrackedLedgerConfig};
use crate::error::{CliError, CliResult};
use crate::remote_client::{RefreshConfig, RemoteLedgerClient};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerId};
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Global HTTP timeout for remote operations, set once from CLI args.
static REMOTE_TIMEOUT: OnceLock<Duration> = OnceLock::new();

/// Set the remote HTTP timeout (called once at startup from CLI args).
pub fn set_remote_timeout(timeout: Duration) {
    let _ = REMOTE_TIMEOUT.set(timeout);
}

/// Bypass a running local server: `--direct`, set once at startup.
static DIRECT: AtomicBool = AtomicBool::new(false);

pub fn set_direct(direct: bool) {
    DIRECT.store(direct, Ordering::Relaxed);
}

pub(crate) fn direct() -> bool {
    DIRECT.load(Ordering::Relaxed)
}

/// Get the configured remote HTTP timeout.
pub(crate) fn remote_timeout() -> Duration {
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

    // Strip `#fragment` (e.g., `#txn-meta`) for ledger resolution, where the
    // address grammar splits it ([`AddressText`](fluree_db_core::AddressText)).
    // The fragment selects a named graph and is read later, when the view
    // parses the alias as a typed address. Existence checks and
    // tracked-config lookups must use just the ledger portion.
    let ledger_part = fluree_db_core::AddressText::split(&alias).before_graph();

    // Try compound remote/ledger syntax (e.g., "origin/mydb")
    if let Some(mode) = try_compound_remote_syntax(ledger_part, dirs).await? {
        // For remote mode, pass through the full alias (with fragment) so the
        // server can resolve the graph. Currently remote doesn't support this,
        // but it avoids silently dropping the fragment.
        return Ok(QueryTarget::Ledger(mode));
    }

    // A server running on the project's storage answers for it, so the
    // storage is not opened beside it.
    if let Some(server) = crate::local_server::route(dirs) {
        return local_server_target(&server, alias.clone(), ledger_part, dirs).await;
    }

    let fluree = build_fluree(dirs).await?;

    // Check if local ledger exists (local wins)
    let ledger_id = to_ledger_id(ledger_part)?;
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
                alias: ledger_id.to_string(),
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

    // Check tracked config. Entries are canonical ids (configs written
    // before track-time normalization read back as `name:main`), so one
    // lookup covers every spelling.
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if let Some(tracked) = store.get_tracked(&ledger_id) {
        return build_tracked_target(&store, &tracked, ledger_part).await;
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
        remote_alias: tracked.remote_alias.to_string(),
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
        remote_alias: tracked.remote_alias.to_string(),
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
    // Send the canonical `name:branch` form so the URL names the same ledger
    // however the user spelled it.
    let remote_alias = to_ledger_id(ledger_alias)?.to_string();
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
pub async fn build_fluree(dirs: &FlureeDir) -> CliResult<Fluree> {
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

    open_waiting(builder, dirs, storage_lock_wait()).await
}

/// How long a command waits for storage another process holds; `0` refuses
/// at once.
pub const STORAGE_LOCK_WAIT_ENV_VAR: &str = "FLUREE_STORAGE_LOCK_WAIT_SECS";
const DEFAULT_STORAGE_LOCK_WAIT: Duration = Duration::from_secs(30);

fn storage_lock_wait() -> Duration {
    std::env::var(STORAGE_LOCK_WAIT_ENV_VAR)
        .ok()
        .and_then(|secs| secs.trim().parse().ok())
        .map_or(DEFAULT_STORAGE_LOCK_WAIT, Duration::from_secs)
}

/// Build `builder`'s file-backed instance, waiting while another process
/// holds its storage.
///
/// Commands often run side by side (scripts, agents), each holding the
/// storage only for its own short run, so one that finds it held waits up to
/// `wait` for it. A server holds its storage for as long as it runs, so a
/// command that meets one is refused at once and pointed at the server.
async fn open_waiting(
    builder: FlureeBuilder,
    dirs: &FlureeDir,
    wait: Duration,
) -> CliResult<Fluree> {
    let deadline = std::time::Instant::now() + wait;
    let mut waiting = false;
    loop {
        let error = match builder.clone().build_async().await {
            Ok(fluree) => return Ok(fluree),
            Err(error) => error,
        };
        let fatal = |error| CliError::Config(format!("failed to initialize Fluree: {error}"));
        let fluree_db_api::ApiError::StorageInUse { root, holder } = &error else {
            return Err(fatal(error));
        };
        let server = crate::local_server::LocalServer::find(dirs)
            .filter(|server| server.holds_storage || *holder == Some(server.pid))
            .map(|server| server.pid);
        if let Some(server) = server {
            // A command that can go through the server only gets here
            // under `--direct`.
            let hint = if direct() {
                "drop --direct to send the command through it".to_string()
            } else {
                "this command does not go through a server yet, so stop the server \
                 first (`fluree server stop`)"
                    .to_string()
            };
            return Err(CliError::Config(format!(
                "a Fluree server (pid {server}) is running on {root}; {hint}"
            )));
        }
        if *holder == Some(std::process::id()) || std::time::Instant::now() >= deadline {
            return Err(fatal(error));
        }
        if !waiting {
            let by = holder
                .map(|pid| format!(" (pid {pid})"))
                .unwrap_or_default();
            eprintln!(
                "  {} {root} is in use by another Fluree process{by}; waiting for it",
                "storage:".yellow().bold()
            );
            waiting = true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
/// When users provide just `mydb`, we append `:main`. An argument that is
/// not a ledger id is an error here rather than a lookup that silently misses.
pub fn to_ledger_id(ledger_id: &str) -> CliResult<LedgerId> {
    LedgerId::parse(ledger_id).map_err(|e| CliError::Input(e.to_string()))
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

/// The target a server on the project's storage gives `alias`, in the order
/// local resolution uses: a ledger or graph source it holds ("local wins"),
/// else a tracked remote of that name, else the server, which reports a name
/// it lacks.
async fn local_server_target(
    server: &crate::local_server::LocalServer,
    alias: String,
    ledger_part: &str,
    dirs: &FlureeDir,
) -> CliResult<QueryTarget> {
    let ledger_id = to_ledger_id(ledger_part)?;
    let client = server.client();
    let held =
        client.ledger_exists(&ledger_id).await? || serves_graph_source(&client, &ledger_id).await?;
    if !held {
        let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
        if let Some(tracked) = store.get_tracked(&ledger_id) {
            return build_tracked_target(&store, &tracked, ledger_part).await;
        }
    }
    server.announce();
    Ok(QueryTarget::Ledger(LedgerMode::Tracked {
        client: Box::new(client),
        remote_alias: alias.clone(),
        local_alias: alias,
        remote_name: LOCAL_SERVER_REMOTE.to_string(),
    }))
}

/// Whether the server lists a graph source under `ledger_id`.
async fn serves_graph_source(client: &RemoteLedgerClient, ledger_id: &LedgerId) -> CliResult<bool> {
    let listed = client.list_ledgers().await?;
    let wanted = ledger_id.to_string();
    Ok(listed.as_array().is_some_and(|entries| {
        entries.iter().any(|entry| {
            let field = |key| entry.get(key).and_then(serde_json::Value::as_str);
            field("type").is_some_and(|kind| kind != "Ledger")
                && matches!((field("name"), field("branch")), (Some(name), Some(branch))
                    if format!("{name}:{branch}") == wanted)
        })
    }))
}

/// A client for the local server, for a command that is not about one
/// ledger; `None` when there is none or `--direct` bypasses it.
pub fn local_server_client(dirs: &FlureeDir) -> Option<RemoteLedgerClient> {
    let server = crate::local_server::route(dirs)?;
    server.announce();
    Some(server.client())
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
        let fluree = build_fluree(dirs).await.unwrap();
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
                local_alias: fluree_db_api::LedgerId::parse("inv:main").unwrap(),
                remote: "origin".to_string(),
                remote_alias: fluree_db_api::LedgerId::parse("inventory:main").unwrap(),
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

    fn storage_builder(dirs: &FlureeDir) -> FlureeBuilder {
        FlureeBuilder::file(
            config::resolve_storage_path(dirs)
                .to_string_lossy()
                .to_string(),
        )
        .without_ledger_caching()
        .without_indexing()
    }

    fn hold_storage(dirs: &FlureeDir) -> fluree_db_core::RootLock {
        fluree_db_core::RootLock::acquire(&config::resolve_storage_path(dirs)).unwrap()
    }

    const HOLD_ROOT_ENV: &str = "FLUREE_CLI_TEST_HOLD_ROOT";

    /// Not a test of its own: the other process
    /// `a_command_waits_for_storage_another_process_lets_go_of` starts, which
    /// holds a storage root for a moment.
    #[test]
    fn hold_a_root_for_a_moment() {
        let Ok(root) = std::env::var(HOLD_ROOT_ENV) else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let _held = fluree_db_core::RootLock::acquire(&root).unwrap();
        std::fs::write(root.join("held"), "").unwrap();
        std::thread::sleep(Duration::from_millis(1500));
    }

    #[tokio::test]
    async fn a_command_waits_for_storage_another_process_lets_go_of() {
        let (_tmp, dirs) = temp_dirs();
        let root = config::resolve_storage_path(&dirs);
        let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "context::tests::hold_a_root_for_a_moment"])
            .env(HOLD_ROOT_ENV, &root)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let started = std::time::Instant::now();
        while !root.join("held").exists() {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "holder never started"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        open_waiting(storage_builder(&dirs), &dirs, Duration::from_secs(60))
            .await
            .expect("opened once the holder let go");
        assert!(holder.wait().unwrap().success());
    }

    #[tokio::test]
    async fn without_a_wait_a_held_storage_is_refused_at_once() {
        let (_tmp, dirs) = temp_dirs();
        let _held = hold_storage(&dirs);

        let refused = open_waiting(storage_builder(&dirs), &dirs, Duration::ZERO).await;

        assert!(refused.is_err());
    }
}
