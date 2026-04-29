//! `fluree server` subcommand — manage the Fluree HTTP server.
//!
//! Supports foreground (`run`), background (`start`/`stop`/`restart`),
//! status, and log tailing. The server reuses the same `.fluree/` context
//! (config file, storage path) as the CLI.

use crate::cli::ServerAction;
use crate::config;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_server::config_file::load_and_merge_config;
use fluree_db_server::{
    init_logging, shutdown_tracer, FlureeServer, ServerConfig, TelemetryConfig,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read as _, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Metadata persisted alongside the PID file for `restart` and `status`.
#[derive(Debug, Serialize, Deserialize)]
struct ServerMeta {
    pid: u32,
    listen_addr: String,
    storage_path: String,
    #[serde(default)]
    connection_config: Option<String>,
    config_path: Option<String>,
    started_at: String,
    /// The raw args passed to the `_child` process (for `restart`).
    args: Vec<String>,
}

// ---------------------------------------------------------------------------
// File paths within data_dir
// ---------------------------------------------------------------------------

fn pid_path(data_dir: &Path) -> PathBuf {
    data_dir.join("server.pid")
}

fn log_path(data_dir: &Path) -> PathBuf {
    data_dir.join("server.log")
}

fn meta_path(data_dir: &Path) -> PathBuf {
    data_dir.join("server.meta.json")
}

// ---------------------------------------------------------------------------
// Public dispatch
// ---------------------------------------------------------------------------

pub async fn run(action: ServerAction, config_override: Option<&Path>) -> CliResult<()> {
    match action {
        ServerAction::Run {
            listen_addr,
            storage_path,
            connection_config,
            log_level,
            profile,
            extra_args,
        } => {
            run_foreground(
                config_override,
                listen_addr,
                storage_path,
                connection_config,
                log_level,
                profile,
                &extra_args,
            )
            .await
        }

        ServerAction::Start {
            listen_addr,
            storage_path,
            connection_config,
            log_level,
            profile,
            dry_run,
            extra_args,
        } => {
            run_start(
                config_override,
                listen_addr,
                storage_path,
                connection_config,
                log_level,
                profile,
                dry_run,
                &extra_args,
            )
            .await
        }

        ServerAction::Stop { force } => run_stop(config_override, force).await,
        ServerAction::Status => run_status(config_override).await,

        ServerAction::Restart {
            listen_addr,
            storage_path,
            connection_config,
            log_level,
            profile,
            extra_args,
        } => {
            run_restart(
                config_override,
                listen_addr,
                storage_path,
                connection_config,
                log_level,
                profile,
                &extra_args,
            )
            .await
        }

        ServerAction::Logs { follow, lines } => run_logs(config_override, follow, lines).await,

        ServerAction::Child { args } => run_child(&args).await,
    }
}

// ---------------------------------------------------------------------------
// `fluree server run` — foreground
// ---------------------------------------------------------------------------

async fn run_foreground(
    config_override: Option<&Path>,
    listen_addr: Option<SocketAddr>,
    storage_path: Option<PathBuf>,
    connection_config: Option<PathBuf>,
    log_level: Option<String>,
    profile: Option<String>,
    extra_args: &[String],
) -> CliResult<()> {
    let dirs = config::require_fluree_dir(config_override)?;
    let data_dir = dirs.data_dir();

    let server_config = build_server_config(
        config_override,
        listen_addr,
        storage_path.clone(),
        connection_config,
        log_level,
        profile,
        extra_args,
    )?;

    let telemetry_config = TelemetryConfig::with_server_config(&server_config);
    init_logging(&telemetry_config);

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        addr = %server_config.listen_addr,
        storage = server_config.storage_type_str(),
        "Starting Fluree server (foreground)"
    );

    // Write server.meta.json so CLI auto-routing works for foreground servers too.
    let meta_file = meta_path(data_dir);
    let meta = ServerMeta {
        pid: std::process::id(),
        listen_addr: server_config.listen_addr.to_string(),
        storage_path: server_config
            .storage_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(memory)".into()),
        connection_config: server_config
            .connection_config
            .as_ref()
            .map(|p| p.display().to_string()),
        config_path: config_override.map(|p| p.display().to_string()),
        started_at: now_iso8601(),
        args: Vec::new(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&meta) {
        let _ = fs::write(&meta_file, json);
    }

    let server = FlureeServer::new(server_config).await.map_err(|e| {
        if let Err(rm_err) = fs::remove_file(&meta_file) {
            tracing::warn!(path = %meta_file.display(), error = %rm_err, "failed to remove meta file after server init failure");
        }
        CliError::Server(format!("failed to initialize server: {e}"))
    })?;
    let result = server.run().await;

    // Clean up meta file on exit (normal shutdown or error).
    if let Err(rm_err) = fs::remove_file(&meta_file) {
        tracing::warn!(path = %meta_file.display(), error = %rm_err, "failed to remove meta file on shutdown");
    }

    result.map_err(|e| CliError::Server(format!("server error: {e}")))?;

    shutdown_tracer().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// `fluree server start` — background daemon
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_start(
    config_override: Option<&Path>,
    listen_addr: Option<SocketAddr>,
    storage_path: Option<PathBuf>,
    connection_config: Option<PathBuf>,
    log_level: Option<String>,
    profile: Option<String>,
    dry_run: bool,
    extra_args: &[String],
) -> CliResult<()> {
    let dirs = config::require_fluree_dir(config_override)?;
    let server_config = build_server_config(
        config_override,
        listen_addr,
        storage_path.clone(),
        connection_config,
        log_level.clone(),
        profile.clone(),
        extra_args,
    )?;

    if dry_run {
        print_resolved_config(&server_config, &dirs);
        return Ok(());
    }

    // Check for existing server
    let data_dir = dirs.data_dir();
    let pid_file = pid_path(data_dir);
    if pid_file.exists() {
        if let Some(pid) = read_pid(&pid_file) {
            if is_process_alive(pid) {
                return Err(CliError::Server(format!(
                    "server already running (pid {pid}). Use 'fluree server stop' first."
                )));
            }
            // Stale PID file — clean up
            let _ = fs::remove_file(&pid_file);
            let _ = fs::remove_file(meta_path(data_dir));
        }
    }

    // Build child args: resolved server flags for the _child subcommand
    let child_args = build_child_args(
        &server_config,
        config_override,
        log_level.as_deref(),
        profile.as_deref(),
        extra_args,
    );

    // Open log file (append mode)
    let log_file_path = log_path(data_dir);
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .map_err(|e| CliError::Server(format!("failed to open log file: {e}")))?;

    let log_stderr = log_file
        .try_clone()
        .map_err(|e| CliError::Server(format!("failed to clone log file handle: {e}")))?;

    // Spawn child process
    let exe = std::env::current_exe()
        .map_err(|e| CliError::Server(format!("failed to resolve current executable: {e}")))?;

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("server")
        .arg("child")
        .arg("--")
        .args(&child_args)
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_stderr))
        .stdin(std::process::Stdio::null());

    // Detach child into its own session so it survives terminal close.
    #[cfg(unix)]
    pre_exec_detach(&mut cmd);

    let child = cmd
        .spawn()
        .map_err(|e| CliError::Server(format!("failed to spawn server process: {e}")))?;

    let pid = child.id();

    // Write PID file
    fs::write(&pid_file, pid.to_string())
        .map_err(|e| CliError::Server(format!("failed to write PID file: {e}")))?;

    // Write metadata
    let meta = ServerMeta {
        pid,
        listen_addr: server_config.listen_addr.to_string(),
        storage_path: server_config
            .storage_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(memory)".into()),
        connection_config: server_config
            .connection_config
            .as_ref()
            .map(|p| p.display().to_string()),
        config_path: config_override.map(|p| p.display().to_string()),
        started_at: now_iso8601(),
        args: child_args,
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| CliError::Server(format!("failed to serialize metadata: {e}")))?;
    fs::write(meta_path(data_dir), meta_json)
        .map_err(|e| CliError::Server(format!("failed to write metadata: {e}")))?;

    // Brief wait then health-check
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    if is_process_alive(pid) {
        eprintln!(
            "{} Server started (pid {}) on http://{}",
            "ok:".green().bold(),
            pid,
            server_config.listen_addr,
        );
        eprintln!("  log: {}", log_file_path.display());
    } else {
        // Process exited immediately — likely a config error
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(meta_path(data_dir));
        return Err(CliError::Server(
            "server process exited immediately. Check logs with 'fluree server logs'.".into(),
        ));
    }

    Ok(())
}

/// Start the server using pre-built child args (used by `restart` to replay
/// the original arguments from `server.meta.json`).
async fn run_start_with_child_args(
    config_override: Option<&Path>,
    child_args: &[String],
    dirs: &FlureeDir,
) -> CliResult<()> {
    let data_dir = dirs.data_dir();
    let pid_file = pid_path(data_dir);

    // Check for existing server
    if pid_file.exists() {
        if let Some(pid) = read_pid(&pid_file) {
            if is_process_alive(pid) {
                return Err(CliError::Server(format!(
                    "server already running (pid {pid}). Use 'fluree server stop' first."
                )));
            }
            let _ = fs::remove_file(&pid_file);
            let _ = fs::remove_file(meta_path(data_dir));
        }
    }

    let log_file_path = log_path(data_dir);
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .map_err(|e| CliError::Server(format!("failed to open log file: {e}")))?;

    let log_stderr = log_file
        .try_clone()
        .map_err(|e| CliError::Server(format!("failed to clone log file handle: {e}")))?;

    let exe = std::env::current_exe()
        .map_err(|e| CliError::Server(format!("failed to resolve current executable: {e}")))?;

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("server")
        .arg("child")
        .arg("--")
        .args(child_args)
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_stderr))
        .stdin(std::process::Stdio::null());

    #[cfg(unix)]
    pre_exec_detach(&mut cmd);

    let child = cmd
        .spawn()
        .map_err(|e| CliError::Server(format!("failed to spawn server process: {e}")))?;

    let pid = child.id();

    fs::write(&pid_file, pid.to_string())
        .map_err(|e| CliError::Server(format!("failed to write PID file: {e}")))?;

    // Derive listen_addr from args for display
    let listen_addr = child_args
        .windows(2)
        .find(|w| w[0] == "--listen-addr")
        .map(|w| w[1].clone())
        .unwrap_or_else(|| "unknown".into());

    let meta = ServerMeta {
        pid,
        listen_addr: listen_addr.clone(),
        storage_path: child_args
            .windows(2)
            .find(|w| w[0] == "--storage-path")
            .map(|w| w[1].clone())
            .unwrap_or_else(|| "(memory)".into()),
        connection_config: child_args
            .windows(2)
            .find(|w| w[0] == "--connection-config")
            .map(|w| w[1].clone()),
        config_path: config_override.map(|p| p.display().to_string()),
        started_at: now_iso8601(),
        args: child_args.to_vec(),
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| CliError::Server(format!("failed to serialize metadata: {e}")))?;
    fs::write(meta_path(data_dir), meta_json)
        .map_err(|e| CliError::Server(format!("failed to write metadata: {e}")))?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    if is_process_alive(pid) {
        eprintln!(
            "{} Server started (pid {}) on http://{}",
            "ok:".green().bold(),
            pid,
            listen_addr,
        );
        eprintln!("  log: {}", log_file_path.display());
    } else {
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(meta_path(data_dir));
        return Err(CliError::Server(
            "server process exited immediately. Check logs with 'fluree server logs'.".into(),
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// `fluree server stop`
// ---------------------------------------------------------------------------

async fn run_stop(config_override: Option<&Path>, force: bool) -> CliResult<()> {
    let dirs = config::require_fluree_dir(config_override)?;
    let data_dir = dirs.data_dir();
    let pid_file = pid_path(data_dir);

    let pid = read_pid(&pid_file)
        .ok_or_else(|| CliError::Server("no server.pid found — server is not running.".into()))?;

    if !is_process_alive(pid) {
        // Stale PID file
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(meta_path(data_dir));
        eprintln!("Server is not running (stale PID file cleaned up).");
        return Ok(());
    }

    // Validate PID belongs to a Fluree process before signaling
    if !is_fluree_process(pid) {
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(meta_path(data_dir));
        return Err(CliError::Server(format!(
            "pid {pid} is alive but does not appear to be a Fluree server \
             (possible PID reuse). PID file cleaned up."
        )));
    }

    // Send SIGTERM
    send_signal(pid, Signal::Term);
    eprintln!("Sent SIGTERM to pid {pid}, waiting for shutdown...");

    // Poll for exit with timeout
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if !is_process_alive(pid) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            if force {
                eprintln!("Timeout reached — sending SIGKILL.");
                send_signal(pid, Signal::Kill);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            } else {
                eprintln!(
                    "{} server did not stop within 10s. Use '--force' to SIGKILL.",
                    "warning:".yellow().bold()
                );
                return Ok(());
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Clean up
    let _ = fs::remove_file(&pid_file);
    let _ = fs::remove_file(meta_path(data_dir));
    eprintln!("{} Server stopped.", "ok:".green().bold());
    Ok(())
}

// ---------------------------------------------------------------------------
// `fluree server status`
// ---------------------------------------------------------------------------

async fn run_status(config_override: Option<&Path>) -> CliResult<()> {
    let dirs = config::require_fluree_dir_or_global(config_override)?;
    let data_dir = dirs.data_dir();
    let pid_file = pid_path(data_dir);

    let pid = match read_pid(&pid_file) {
        Some(p) => p,
        None => {
            eprintln!("Server is not running (no PID file).");
            return Ok(());
        }
    };

    if !is_process_alive(pid) {
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(meta_path(data_dir));
        eprintln!("Server is not running (stale PID file cleaned up).");
        return Ok(());
    }

    // Read metadata
    let meta: Option<ServerMeta> = fs::read_to_string(meta_path(data_dir))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    eprintln!("{} Server is running", "ok:".green().bold());
    eprintln!("  pid:          {pid}");

    if let Some(ref m) = meta {
        eprintln!("  listen_addr:  {}", m.listen_addr);
        if let Some(ref cc) = m.connection_config {
            eprintln!("  connection:   {cc}");
        } else {
            eprintln!("  storage_path: {}", m.storage_path);
        }
        eprintln!("  started_at:   {}", m.started_at);
        if let Some(uptime) = format_uptime(&m.started_at) {
            eprintln!("  uptime:       {uptime}");
        }
        if let Some(ref cp) = m.config_path {
            eprintln!("  config:       {cp}");
        }

        // Try HTTP health check
        let health_url = format!("http://{}/health", m.listen_addr);
        match reqwest::Client::new()
            .get(&health_url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("  health:       {}", "ok".green());
            }
            Ok(resp) => {
                eprintln!(
                    "  health:       {} (HTTP {})",
                    "degraded".yellow(),
                    resp.status()
                );
            }
            Err(_) => {
                eprintln!(
                    "  health:       {} (not responding on {})",
                    "unreachable".red(),
                    m.listen_addr
                );
            }
        }
    }

    eprintln!("  log:          {}", log_path(data_dir).display());
    Ok(())
}

// ---------------------------------------------------------------------------
// `fluree server restart`
// ---------------------------------------------------------------------------

async fn run_restart(
    config_override: Option<&Path>,
    listen_addr: Option<SocketAddr>,
    storage_path: Option<PathBuf>,
    connection_config: Option<PathBuf>,
    log_level: Option<String>,
    profile: Option<String>,
    extra_args: &[String],
) -> CliResult<()> {
    let dirs = config::require_fluree_dir(config_override)?;
    let data_dir = dirs.data_dir();

    // Read original args from metadata before stopping (so we can recover them).
    let original_meta: Option<ServerMeta> = fs::read_to_string(meta_path(data_dir))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    // Stop first (ignore "not running" errors)
    let _ = run_stop(config_override, false).await;

    // Brief pause for port release
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Merge strategy: start from old child args, patch in any new overrides.
    // This preserves passthrough args from the original `start` while allowing
    // `restart --log-level debug` to change just that flag.
    if let Some(meta) = original_meta {
        let merged = merge_restart_args(
            &meta.args,
            listen_addr.as_ref(),
            storage_path.as_deref(),
            connection_config.as_deref(),
            log_level.as_deref(),
            profile.as_deref(),
            extra_args,
        );
        return run_start_with_child_args(config_override, &merged, &dirs).await;
    }

    // No previous metadata — start fresh with whatever flags were given.
    run_start(
        config_override,
        listen_addr,
        storage_path,
        connection_config,
        log_level,
        profile,
        false,
        extra_args,
    )
    .await
}

// ---------------------------------------------------------------------------
// `fluree server logs`
// ---------------------------------------------------------------------------

async fn run_logs(config_override: Option<&Path>, follow: bool, lines: usize) -> CliResult<()> {
    let dirs = config::require_fluree_dir_or_global(config_override)?;
    let log_file_path = log_path(dirs.data_dir());

    if !log_file_path.exists() {
        return Err(CliError::Server(format!(
            "no log file found at {}",
            log_file_path.display()
        )));
    }

    // Print last N lines
    print_last_n_lines(&log_file_path, lines)?;

    if follow {
        // Tail the file: seek to end and poll for new data
        let mut file = fs::File::open(&log_file_path)?;
        file.seek(SeekFrom::End(0))?;

        let mut buf = String::new();
        loop {
            buf.clear();
            let n = file.read_to_string(&mut buf)?;
            if n > 0 {
                print!("{buf}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// `fluree server child` — internal entry point for backgrounded server
// ---------------------------------------------------------------------------

async fn run_child(args: &[String]) -> CliResult<()> {
    // Parse the forwarded args as a ServerConfig.
    // The args are constructed by build_child_args() and include all resolved flags.
    use clap::{CommandFactory, FromArgMatches};

    let cmd = ServerConfig::command();
    let matches = cmd
        .try_get_matches_from(
            std::iter::once("fluree-server".to_string()).chain(args.iter().cloned()),
        )
        .map_err(|e| CliError::Server(format!("failed to parse server args: {e}")))?;

    let mut server_config = ServerConfig::from_arg_matches(&matches)
        .map_err(|e| CliError::Server(format!("failed to build server config: {e}")))?;

    // Merge config file values (the child inherits --config-file if present)
    if let Err(e) = load_and_merge_config(&mut server_config, &matches) {
        eprintln!("Warning: config file error: {e}");
    }

    let telemetry_config = TelemetryConfig::with_server_config(&server_config);
    init_logging(&telemetry_config);

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        addr = %server_config.listen_addr,
        storage = server_config.storage_type_str(),
        "Starting Fluree server (background child)"
    );

    let server = FlureeServer::new(server_config)
        .await
        .map_err(|e| CliError::Server(format!("failed to initialize server: {e}")))?;

    server
        .run()
        .await
        .map_err(|e| CliError::Server(format!("server error: {e}")))?;

    shutdown_tracer().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Config resolution helpers
// ---------------------------------------------------------------------------

/// Build a `ServerConfig` by merging the config file with CLI flag overrides.
fn build_server_config(
    config_override: Option<&Path>,
    listen_addr: Option<SocketAddr>,
    storage_path: Option<PathBuf>,
    connection_config: Option<PathBuf>,
    log_level: Option<String>,
    profile: Option<String>,
    extra_args: &[String],
) -> CliResult<ServerConfig> {
    use clap::{CommandFactory, FromArgMatches};

    // Build a synthetic arg list for clap to parse.
    // Start with defaults, then layer on overrides.
    let mut args: Vec<String> = vec!["fluree-server".into()];

    if let Some(addr) = listen_addr {
        args.push("--listen-addr".into());
        args.push(addr.to_string());
    }

    if let Some(ref path) = connection_config {
        // Connection config takes precedence — don't set a default storage path
        args.push("--connection-config".into());
        args.push(path.display().to_string());
    } else if let Some(ref path) = storage_path {
        args.push("--storage-path".into());
        args.push(path.display().to_string());
    } else {
        // Default: use the CLI's resolved storage path
        let dirs = config::require_fluree_dir(config_override)?;
        let resolved = config::resolve_storage_path(&dirs);
        args.push("--storage-path".into());
        args.push(resolved.display().to_string());
    }

    if let Some(ref level) = log_level {
        args.push("--log-level".into());
        args.push(level.clone());
    }

    if let Some(ref p) = profile {
        args.push("--profile".into());
        args.push(p.clone());
    }

    // The CLI's --config flag resolves to a .fluree/ directory, but the server's
    // --config-file expects a file path. Resolve to the actual config file.
    if let Some(cp) = config_override {
        if let Some((file_path, _format)) = config::detect_config_file(cp) {
            args.push("--config-file".into());
            args.push(file_path.display().to_string());
        } else if cp.is_file() {
            // User passed a file directly via --config
            args.push("--config-file".into());
            args.push(cp.display().to_string());
        }
        // If directory has no config file, skip --config-file and let
        // the server discover config via its normal walk-up-from-cwd logic.
    }

    // Append extra passthrough args
    args.extend(extra_args.iter().cloned());

    let cmd = ServerConfig::command();
    let matches = cmd
        .try_get_matches_from(&args)
        .map_err(|e| CliError::Server(format!("invalid server flags: {e}")))?;

    let mut server_config = ServerConfig::from_arg_matches(&matches)
        .map_err(|e| CliError::Server(format!("failed to build server config: {e}")))?;

    // Merge config file values for fields not explicitly set via CLI
    if let Err(e) = load_and_merge_config(&mut server_config, &matches) {
        // Non-fatal for auto-discovered configs
        if server_config.config_file.is_some() || server_config.profile.is_some() {
            return Err(CliError::Server(format!("config file error: {e}")));
        }
        eprintln!("{} config file: {e}", "warning:".yellow().bold());
    }

    Ok(server_config)
}

/// Build the arg list to pass to the `_child` subprocess.
///
/// Strategy: pass the config file + the explicit CLI overrides (listen_addr,
/// storage_path, log_level, profile) and let the child's `load_and_merge_config`
/// handle merging file defaults. This avoids having to exhaustively serialize
/// all ~40 `ServerConfig` fields. The `extra_args` (`--` passthrough) are
/// forwarded verbatim so no user-specified flags are lost.
fn build_child_args(
    config: &ServerConfig,
    config_override: Option<&Path>,
    log_level: Option<&str>,
    profile: Option<&str>,
    extra_args: &[String],
) -> Vec<String> {
    let mut args = Vec::new();

    // Always pass resolved listen_addr and storage_path so the child
    // doesn't need to re-resolve the .fluree/ directory.
    args.push("--listen-addr".into());
    args.push(config.listen_addr.to_string());

    if let Some(ref path) = config.storage_path {
        args.push("--storage-path".into());
        args.push(path.display().to_string());
    }

    if let Some(ref path) = config.connection_config {
        args.push("--connection-config".into());
        args.push(path.display().to_string());
    }

    // Pass through explicit overrides
    if log_level.is_some() {
        args.push("--log-level".into());
        args.push(config.log_level.clone());
    }

    if let Some(p) = profile {
        args.push("--profile".into());
        args.push(p.to_string());
    }

    // Resolve CLI --config (directory) to actual config file path for --config-file.
    if let Some(cp) = config_override {
        if let Some((file_path, _format)) = config::detect_config_file(cp) {
            args.push("--config-file".into());
            args.push(file_path.display().to_string());
        } else if cp.is_file() {
            args.push("--config-file".into());
            args.push(cp.display().to_string());
        }
    }

    // Forward extra passthrough args verbatim
    args.extend(extra_args.iter().cloned());

    args
}

/// Merge restart overrides into the original child args from `server.meta.json`.
///
/// For flags like `--listen-addr`, `--storage-path`, `--log-level`, `--profile`:
/// if the user provided a new value on `restart`, replace the old one; otherwise
/// keep the original. Extra passthrough args (`--` ...) are replaced wholesale
/// if new ones are provided, otherwise the originals are kept.
#[allow(clippy::too_many_arguments)]
fn merge_restart_args(
    old_args: &[String],
    new_listen_addr: Option<&SocketAddr>,
    new_storage_path: Option<&Path>,
    new_connection_config: Option<&Path>,
    new_log_level: Option<&str>,
    new_profile: Option<&str>,
    new_extra_args: &[String],
) -> Vec<String> {
    let mut result = Vec::new();
    let mut i = 0;

    // --storage-path and --connection-config are mutually exclusive modes.
    // Switching from one to the other on restart must drop the old flag.
    let switching_to_storage = new_storage_path.is_some();
    let switching_to_connection = new_connection_config.is_some();

    // Walk old args, replacing values for known flags when overrides are present.
    while i < old_args.len() {
        let flag = &old_args[i];
        match flag.as_str() {
            "--listen-addr" if new_listen_addr.is_some() => {
                result.push("--listen-addr".into());
                result.push(new_listen_addr.unwrap().to_string());
                i += 2; // skip old flag + value
            }
            "--storage-path" if new_storage_path.is_some() => {
                result.push("--storage-path".into());
                result.push(new_storage_path.unwrap().display().to_string());
                i += 2;
            }
            // Drop old --storage-path when switching to --connection-config
            "--storage-path" if switching_to_connection => {
                i += 2;
            }
            "--connection-config" if new_connection_config.is_some() => {
                result.push("--connection-config".into());
                result.push(new_connection_config.unwrap().display().to_string());
                i += 2;
            }
            // Drop old --connection-config when switching to --storage-path
            "--connection-config" if switching_to_storage => {
                i += 2;
            }
            "--log-level" if new_log_level.is_some() => {
                result.push("--log-level".into());
                result.push(new_log_level.unwrap().to_string());
                i += 2;
            }
            "--profile" if new_profile.is_some() => {
                result.push("--profile".into());
                result.push(new_profile.unwrap().to_string());
                i += 2;
            }
            // Known flag with a value argument — copy both
            "--listen-addr"
            | "--storage-path"
            | "--connection-config"
            | "--log-level"
            | "--profile"
            | "--config-file" => {
                result.push(old_args[i].clone());
                if i + 1 < old_args.len() {
                    result.push(old_args[i + 1].clone());
                }
                i += 2;
            }
            // Everything else (including passthrough args from --) — copy as-is
            _ => {
                result.push(old_args[i].clone());
                i += 1;
            }
        }
    }

    // If new overrides were given for flags not already in old_args, append them.
    let has = |flag: &str| old_args.iter().any(|a| a == flag);
    if let Some(addr) = new_listen_addr {
        if !has("--listen-addr") {
            result.push("--listen-addr".into());
            result.push(addr.to_string());
        }
    }
    if let Some(path) = new_storage_path {
        if !has("--storage-path") {
            result.push("--storage-path".into());
            result.push(path.display().to_string());
        }
    }
    if let Some(path) = new_connection_config {
        if !has("--connection-config") {
            result.push("--connection-config".into());
            result.push(path.display().to_string());
        }
    }
    if let Some(level) = new_log_level {
        if !has("--log-level") {
            result.push("--log-level".into());
            result.push(level.to_string());
        }
    }
    if let Some(prof) = new_profile {
        if !has("--profile") {
            result.push("--profile".into());
            result.push(prof.to_string());
        }
    }

    // Replace extra passthrough args wholesale if new ones were provided.
    if !new_extra_args.is_empty() {
        // Remove any trailing args that came after known flags in old_args
        // (these would have been the old passthrough args). Since build_child_args
        // appends extra_args at the end, we can't perfectly distinguish them
        // from unknown flags. Best-effort: just append the new ones.
        result.extend(new_extra_args.iter().cloned());
    }

    result
}

/// Print resolved config for `--dry-run`.
fn print_resolved_config(config: &ServerConfig, dirs: &FlureeDir) {
    eprintln!("{}", "Resolved server configuration:".bold());
    eprintln!("  listen_addr:  {}", config.listen_addr);
    if let Some(ref cc) = config.connection_config {
        eprintln!("  connection_config: {}", cc.display());
    } else {
        eprintln!(
            "  storage_path: {}",
            config
                .storage_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(memory)".into())
        );
    }
    eprintln!("  log_level:    {}", config.log_level);
    eprintln!("  cors_enabled: {}", config.cors_enabled);
    eprintln!("  indexing:     {}", config.indexing_enabled);
    if config.indexing_enabled {
        eprintln!("    min_bytes:  {}", config.reindex_min_bytes);
        eprintln!(
            "    max_bytes:  {}",
            config
                .reindex_max_bytes
                .map(|v| v.to_string())
                .unwrap_or_else(|| "(default: 20% of RAM)".into())
        );
    }
    eprintln!(
        "  cache_max_mb: {}",
        config
            .cache_max_mb
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(default: 30/40/50% of RAM, tiered)".into())
    );
    eprintln!("  server_role:  {:?}", config.server_role);
    eprintln!("  pid_file:     {}", pid_path(dirs.data_dir()).display());
    eprintln!("  log_file:     {}", log_path(dirs.data_dir()).display());
}

// ---------------------------------------------------------------------------
// Process management helpers (Unix)
// ---------------------------------------------------------------------------

enum Signal {
    Term,
    Kill,
}

#[cfg(unix)]
fn send_signal(pid: u32, sig: Signal) {
    let signal = match sig {
        Signal::Term => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    };
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}

#[cfg(not(unix))]
fn send_signal(_pid: u32, _sig: Signal) {
    eprintln!("Signal sending is only supported on Unix. Use task manager to stop the server.");
}

#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    // Conservative: assume alive if we can't check
    true
}

/// Validate that a PID belongs to a Fluree server process (not a recycled PID).
///
/// On Linux, reads `/proc/<pid>/cmdline`. On macOS (and Linux fallback), uses
/// `ps -o command=` to get the full argv and checks for "fluree" + "server child".
/// Returns true if we can confirm it's ours, or if we can't check (conservative).
#[cfg(unix)]
fn is_fluree_process(pid: u32) -> bool {
    // Try /proc/<pid>/cmdline first (Linux). Fields are NUL-separated.
    if let Ok(raw) = fs::read(format!("/proc/{pid}/cmdline")) {
        let cmdline = String::from_utf8_lossy(&raw);
        // Look for "server" + "child" in the command line (our _child pattern)
        return cmdline.contains("fluree") && cmdline.contains("server");
    }
    // Fallback: use `ps -o command=` for the full argv (works on macOS and Linux).
    // `command=` gives the full command line, not just the executable name.
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
    {
        if output.status.success() {
            let cmd = String::from_utf8_lossy(&output.stdout);
            return cmd.contains("fluree") && cmd.contains("server");
        }
    }
    // Can't verify — assume it's ours (conservative)
    true
}

#[cfg(not(unix))]
fn is_fluree_process(_pid: u32) -> bool {
    true
}

fn read_pid(path: &Path) -> Option<u32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Detach a child command into its own session (Unix only).
/// This ensures the server survives terminal close.
#[cfg(unix)]
fn pre_exec_detach(cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    // SAFETY: setsid() is async-signal-safe and is the standard way to detach
    // a child process from the parent's session/terminal.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
}

// ---------------------------------------------------------------------------
// Log helpers
// ---------------------------------------------------------------------------

/// Print the last `n` lines of a file using a backwards scan.
/// Reads from the end in chunks to avoid loading the entire file into memory.
fn print_last_n_lines(path: &Path, n: usize) -> CliResult<()> {
    let mut file = fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok(());
    }

    // Read backwards in 8KB chunks to find the last N newlines.
    const CHUNK_SIZE: u64 = 8192;
    let mut offset = file_len;
    let mut buf = Vec::new();

    loop {
        let read_start = offset.saturating_sub(CHUNK_SIZE);
        let read_len = (offset - read_start) as usize;
        if read_len == 0 {
            break;
        }

        file.seek(SeekFrom::Start(read_start))?;
        let mut chunk = vec![0u8; read_len];
        std::io::Read::read_exact(&mut file, &mut chunk)?;

        // Prepend chunk to our buffer
        chunk.append(&mut buf);
        buf = chunk;

        // Count newlines; we need n+1 (or start of file) to get n lines
        let newline_count = buf.iter().filter(|&&b| b == b'\n').count();
        if newline_count > n || read_start == 0 {
            break;
        }
        offset = read_start;
    }

    // Extract the last n lines from the buffer
    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    for line in &lines[start..] {
        println!("{line}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

/// Format current time as ISO-8601 UTC string (e.g., "2026-02-16T10:30:00Z").
/// Uses a simple manual format to avoid adding a chrono dependency.
fn now_iso8601() -> String {
    let now = std::time::SystemTime::now();
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format_unix_as_iso8601(secs)
}

/// Convert Unix timestamp (seconds) to ISO-8601 UTC string.
fn format_unix_as_iso8601(secs: u64) -> String {
    // Simple conversion — no leap seconds, good enough for display purposes.
    const SECS_PER_DAY: u64 = 86400;
    const SECS_PER_HOUR: u64 = 3600;
    const SECS_PER_MIN: u64 = 60;

    let days = secs / SECS_PER_DAY;
    let time_of_day = secs % SECS_PER_DAY;
    let hour = time_of_day / SECS_PER_HOUR;
    let min = (time_of_day % SECS_PER_HOUR) / SECS_PER_MIN;
    let sec = time_of_day % SECS_PER_MIN;

    // Convert days since epoch to y/m/d using the civil-from-days algorithm.
    let (year, month, day) = civil_from_days(days as i64);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Civil date from days since Unix epoch (1970-01-01).
/// Howard Hinnant's algorithm, adapted from chrono/date.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Format a duration as a human-readable uptime string (e.g., "2h 15m 30s").
fn format_uptime(started_at: &str) -> Option<String> {
    // Try parsing ISO-8601 timestamp back to seconds, or raw seconds for old metadata
    let start_secs =
        parse_iso8601_to_unix(started_at).or_else(|| started_at.parse::<u64>().ok())?;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let elapsed = now_secs.saturating_sub(start_secs);

    let hours = elapsed / 3600;
    let mins = (elapsed % 3600) / 60;
    let secs = elapsed % 60;

    if hours > 0 {
        Some(format!("{hours}h {mins}m {secs}s"))
    } else if mins > 0 {
        Some(format!("{mins}m {secs}s"))
    } else {
        Some(format!("{secs}s"))
    }
}

/// Parse an ISO-8601 UTC timestamp (e.g., "2026-02-16T10:30:00Z") to Unix seconds.
fn parse_iso8601_to_unix(s: &str) -> Option<u64> {
    // Minimal parser for our own output format: YYYY-MM-DDThh:mm:ssZ
    let s = s.trim_end_matches('Z');
    let (date, time) = s.split_once('T')?;
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;

    let time_parts: Vec<&str> = time.split(':').collect();
    if time_parts.len() != 3 {
        return None;
    }
    let hour: u64 = time_parts[0].parse().ok()?;
    let min: u64 = time_parts[1].parse().ok()?;
    let sec: u64 = time_parts[2].parse().ok()?;

    let days = days_from_civil(y, m, d);
    Some(days as u64 * 86400 + hour * 3600 + min * 60 + sec)
}

/// Inverse of `civil_from_days` — days since Unix epoch from civil date.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y as i64 - 1 } else { y as i64 };
    let m = m as i64;
    let d = d as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    era * 146_097 + doe as i64 - 719_468
}
