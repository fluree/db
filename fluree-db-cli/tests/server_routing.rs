//! Commands sent through a `fluree server run` on the project's storage,
//! rather than opening that storage beside it.
#![cfg(all(feature = "server", unix))]

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use std::fs::File;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// A `fluree server run` child in a project directory.
struct ProjectServer {
    child: Child,
    base: String,
    log: std::path::PathBuf,
}

impl ProjectServer {
    /// Start in `project` with `extra` server flags, and wait for `/health`.
    async fn start(project: &Path, extra: &[&str]) -> Self {
        let addr = {
            let probe = TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap()
        };
        let log = project.join(format!("server-{}.log", addr.port()));
        let log_file = File::create(&log).unwrap();
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_fluree"));
        cmd.args(["server", "run", "--listen-addr"])
            .arg(addr.to_string())
            .args(extra)
            .current_dir(project)
            .env("HOME", project)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file));
        clear_fluree_env(&mut cmd);
        let mut server = Self {
            child: cmd.spawn().unwrap(),
            base: format!("http://{addr}"),
            log,
        };

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!("server exited early ({status}):\n{}", server.output());
            }
            let health = reqwest::get(format!("{}/health", server.base)).await;
            let meta = project.join(".fluree").join("server.meta.json");
            if health.is_ok_and(|r| r.status().is_success()) && meta.exists() {
                return server;
            }
            assert!(
                Instant::now() < deadline,
                "server never became healthy:\n{}",
                server.output()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn has_ledger(&self, ledger: &str) -> bool {
        reqwest::get(format!("{}/v1/fluree/exists/{ledger}", self.base))
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .contains("true")
    }
}

impl Drop for ProjectServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn clear_fluree_env(cmd: &mut std::process::Command) {
    for (var, _) in std::env::vars_os() {
        let name = var.to_string_lossy();
        if name.starts_with("FLUREE_") || name.starts_with("XDG_") {
            cmd.env_remove(&var);
        }
    }
}

fn fluree(project: &Path) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(project)
        .env("HOME", project)
        .env("NO_COLOR", "1");
    for (var, _) in std::env::vars_os() {
        let name = var.to_string_lossy();
        if name.starts_with("FLUREE_") || name.starts_with("XDG_") {
            cmd.env_remove(&var);
        }
    }
    cmd
}

fn project() -> TempDir {
    let dir = TempDir::new().unwrap();
    fluree(dir.path()).arg("init").assert().success();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_and_use_go_through_the_server() {
    let dir = project();
    let server = ProjectServer::start(dir.path(), &[]).await;

    fluree(dir.path())
        .args(["create", "people"])
        .assert()
        .success()
        .stderr(predicate::str::contains("routing through local server"));
    assert!(
        server.has_ledger("people:main").await,
        "{}",
        server.output()
    );

    fluree(dir.path())
        .args(["use", "people"])
        .assert()
        .success()
        .stderr(predicate::str::contains("routing through local server"));
    fluree(dir.path())
        .args([
            "insert",
            r#"{"@id": "http://example.org/a", "http://example.org/n": 1}"#,
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("routing through local server"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_on_other_storage_is_not_routed_to() {
    let dir = project();
    let elsewhere = TempDir::new().unwrap();
    let elsewhere_path = elsewhere.path().to_string_lossy().to_string();
    let _server = ProjectServer::start(dir.path(), &["--storage-path", &elsewhere_path]).await;

    fluree(dir.path())
        .arg("list")
        .assert()
        .success()
        .stderr(predicate::str::contains("not this project's"))
        .stderr(predicate::str::contains("routing through local server").not());
}
