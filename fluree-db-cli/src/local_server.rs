//! The Fluree server running on this project's storage, if any.
//!
//! `fluree server run` and `fluree server start` record themselves in
//! `<data dir>/server.meta.json`. A command that can send its work to a
//! server sends it to that one, rather than opening the storage beside it,
//! unless `--direct` says otherwise.

use crate::config;
use crate::context;
use crate::remote_client::RemoteLedgerClient;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// What a running server records about itself, for routing, `status` and
/// `restart`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ServerMeta {
    pub pid: u32,
    pub listen_addr: String,
    #[serde(default)]
    pub storage_path: String,
    #[serde(default)]
    pub connection_config: Option<String>,
    #[serde(default)]
    pub bolt_listen_addr: Option<String>,
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub started_at: String,
    /// The raw args passed to the `_child` process (for `restart`).
    #[serde(default)]
    pub args: Vec<String>,
    /// The storage root the server locks, if it locks one.
    #[serde(default)]
    pub locked_root: Option<PathBuf>,
}

pub(crate) fn meta_path(data_dir: &Path) -> PathBuf {
    data_dir.join("server.meta.json")
}

pub(crate) fn read_meta(data_dir: &Path) -> Option<ServerMeta> {
    fs::read_to_string(meta_path(data_dir))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
}

/// A live Fluree server serving this project's storage.
pub(crate) struct LocalServer {
    pub pid: u32,
    listen_addr: String,
}

impl LocalServer {
    /// The server `server.meta.json` names, when it is alive, is a Fluree
    /// server, and serves the storage this CLI would open.
    pub fn find(dirs: &FlureeDir) -> Option<Self> {
        let meta = read_meta(dirs.data_dir())?;
        if !is_process_alive(meta.pid) {
            notice(format!(
                "local server (pid {}) is no longer running; executing directly",
                meta.pid
            ));
            return None;
        }
        if !is_fluree_process(meta.pid) {
            notice(format!(
                "pid {} is not a Fluree server; executing directly",
                meta.pid
            ));
            return None;
        }
        let ours = config::resolve_storage_path(dirs);
        if meta.connection_config.is_none()
            && !meta.storage_path.is_empty()
            && !same_path(Path::new(&meta.storage_path), &ours)
        {
            notice(format!(
                "local server (pid {}) serves {}, not this project's {}; executing directly",
                meta.pid,
                meta.storage_path,
                ours.display()
            ));
            return None;
        }
        Some(Self {
            pid: meta.pid,
            listen_addr: meta.listen_addr,
        })
    }

    /// Tell the user the command goes through the server.
    pub fn announce(&self) {
        eprintln!(
            "  {} routing through local server at {} (use {} to bypass)",
            "server:".cyan().bold(),
            self.listen_addr,
            "--direct".bold()
        );
    }

    pub fn client(&self) -> RemoteLedgerClient {
        // The server mounts its API at /v1/fluree, the path `fluree remote add`
        // resolves through /.well-known/fluree.json.
        let base_url = format!("http://{}/v1/fluree", self.listen_addr);
        RemoteLedgerClient::with_timeout(&base_url, None, context::remote_timeout())
    }
}

/// The local server a command should send its work to: none under
/// `--direct`.
pub(crate) fn route(dirs: &FlureeDir) -> Option<LocalServer> {
    if context::direct() {
        return None;
    }
    LocalServer::find(dirs)
}

fn same_path(a: &Path, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// A stale or foreign `server.meta.json` is reported once per command, not
/// at every lookup.
fn notice(message: String) {
    static SHOWN: AtomicBool = AtomicBool::new(false);
    if !SHOWN.swap(true, Ordering::Relaxed) {
        eprintln!("  {} {message}", "notice:".yellow().bold());
    }
}

#[cfg(unix)]
pub(crate) fn is_process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
pub(crate) fn is_process_alive(_pid: u32) -> bool {
    // Conservative: assume alive if we can't check
    true
}

/// Whether a pid belongs to a Fluree server rather than a recycled pid.
///
/// Reads `/proc/<pid>/cmdline` on Linux and `ps -o command=` elsewhere, and
/// looks for "fluree" and "server". True when it cannot tell (conservative).
#[cfg(unix)]
pub(crate) fn is_fluree_process(pid: u32) -> bool {
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
pub(crate) fn is_fluree_process(_pid: u32) -> bool {
    true
}
