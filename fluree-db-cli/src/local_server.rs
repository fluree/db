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
    /// It holds the storage's lock, so this CLI cannot open the storage.
    pub holds_storage: bool,
}

impl LocalServer {
    /// The server `server.meta.json` names, when it serves the storage this
    /// CLI would open and is running.
    ///
    /// A server that locks its storage is running exactly while it holds the
    /// lock, which a crash cannot leave behind; a recorded pid can be reused.
    /// Any other server (a connection config, a peer, one `restart` launched)
    /// is running while its pid is alive and a Fluree server's.
    pub fn find(dirs: &FlureeDir) -> Option<Self> {
        let meta = read_meta(dirs.data_dir())?;
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
        let gone = || {
            notice(format!(
                "local server (pid {}) is no longer running; executing directly",
                meta.pid
            ));
            None
        };
        let holds_storage = match meta.locked_root.as_ref().map(|_| probe(&ours)) {
            Some(Probe::Free) => return gone(),
            Some(Probe::Held(holder)) => {
                // The holder is unknown where the lock file cannot be read.
                if holder.map_or(!is_process_alive(meta.pid), |pid| pid != meta.pid) {
                    return gone();
                }
                true
            }
            Some(Probe::Unknown) | None => {
                if !is_process_alive(meta.pid) {
                    return gone();
                }
                if !is_fluree_process(meta.pid) {
                    notice(format!(
                        "pid {} is not a Fluree server; executing directly",
                        meta.pid
                    ));
                    return None;
                }
                false
            }
        };
        Some(Self {
            pid: meta.pid,
            listen_addr: meta.listen_addr,
            holds_storage,
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

/// What a storage root's lock says about who holds it.
enum Probe {
    Free,
    Held(Option<u32>),
    /// The lock cannot tell: the filesystem cannot lock, or taking it failed.
    Unknown,
}

fn probe(root: &Path) -> Probe {
    // Taking the lock would create a root that does not exist.
    if !root.is_dir() {
        return Probe::Free;
    }
    match fluree_db_core::RootLock::acquire(root) {
        Ok(lock) if lock.is_locked() => Probe::Free,
        Ok(_) => Probe::Unknown,
        Err(fluree_db_core::RootLockError::Held { holder }) => Probe::Held(holder),
        Err(_) => Probe::Unknown,
    }
}

pub(crate) fn same_path(a: &Path, b: &Path) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Project {
        _dir: tempfile::TempDir,
        dirs: FlureeDir,
        storage: PathBuf,
    }

    fn project() -> Project {
        let dir = tempfile::tempdir().unwrap();
        let dirs = FlureeDir::unified(dir.path().join(".fluree"));
        let storage = config::resolve_storage_path(&dirs);
        fs::create_dir_all(&storage).unwrap();
        Project {
            _dir: dir,
            dirs,
            storage,
        }
    }

    /// Record a server on the project's storage, as `server run` does.
    fn record(project: &Project, pid: u32, locks_storage: bool, connection_config: Option<&str>) {
        let meta = ServerMeta {
            pid,
            listen_addr: "127.0.0.1:8090".into(),
            storage_path: project.storage.display().to_string(),
            connection_config: connection_config.map(Into::into),
            bolt_listen_addr: None,
            config_path: None,
            started_at: String::new(),
            args: Vec::new(),
            locked_root: locks_storage.then(|| project.storage.clone()),
        };
        fs::write(
            meta_path(project.dirs.data_dir()),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
    }

    /// A live process whose command line reads as a Fluree server's, as a
    /// pid reused after a crash can.
    #[cfg(unix)]
    struct LookAlike(std::process::Child);

    #[cfg(unix)]
    impl LookAlike {
        fn spawn() -> Self {
            // `sh -c` names its script after the next argument; `; :` keeps
            // it from exec'ing into `sleep`, which would drop that name.
            let child = std::process::Command::new("sh")
                .args(["-c", "sleep 60; :", "fluree-server"])
                .spawn()
                .unwrap();
            let pid = child.id();
            let look_alike = Self(child);
            // Right after `spawn` it may not read as one yet (seen on Linux CI).
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !(is_process_alive(pid) && is_fluree_process(pid)) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "pid {pid} never read as a Fluree server"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            look_alike
        }

        fn pid(&self) -> u32 {
            self.0.id()
        }
    }

    #[cfg(unix)]
    impl Drop for LookAlike {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn a_server_is_found_while_it_holds_the_storage() {
        let project = project();
        let _server = fluree_db_core::RootLock::acquire(&project.storage).unwrap();
        record(&project, std::process::id(), true, None);

        let found = LocalServer::find(&project.dirs).expect("the server holds the storage");
        assert!(found.holds_storage);
    }

    #[cfg(unix)]
    #[test]
    fn a_server_whose_storage_is_free_is_gone_though_its_pid_lives() {
        let project = project();
        let reused = LookAlike::spawn();
        record(&project, reused.pid(), true, None);

        assert!(LocalServer::find(&project.dirs).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn a_server_whose_storage_another_process_holds_is_gone() {
        let project = project();
        let reused = LookAlike::spawn();
        let _other = fluree_db_core::RootLock::acquire(&project.storage).unwrap();
        record(&project, reused.pid(), true, None);

        assert!(LocalServer::find(&project.dirs).is_none());
    }

    /// A connection-config server never locks the project's storage.
    #[cfg(unix)]
    #[test]
    fn a_server_that_does_not_lock_the_storage_is_found_by_its_pid() {
        let project = project();
        let server = LookAlike::spawn();
        record(&project, server.pid(), false, Some("connection.jsonld"));

        let found = LocalServer::find(&project.dirs).expect("found by its pid");
        assert!(!found.holds_storage);
    }

    #[test]
    fn finding_a_server_does_not_create_the_storage() {
        let project = project();
        fs::remove_dir(&project.storage).unwrap();
        record(&project, std::process::id(), true, None);

        assert!(LocalServer::find(&project.dirs).is_none());
        assert!(!project.storage.exists());
    }
}
