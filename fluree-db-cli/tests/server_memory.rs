//! `fluree server run --memory`: a throwaway server that needs no project
//! directory, writes nothing where it runs, and forgets everything on exit.
#![cfg(all(feature = "server", unix))]

use serde_json::{json, Value};
use std::fs::File;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const LEDGER: &str = "ci:main";

/// A `fluree server run --memory` child process.
struct MemoryServer {
    child: Child,
    base: String,
    log: std::path::PathBuf,
}

impl MemoryServer {
    /// Start in `cwd`, with HOME, TMPDIR and the log under `outside`, and wait
    /// for `/health`.
    async fn start(cwd: &Path, outside: &Path) -> Self {
        let addr = {
            let probe = TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap()
        };
        let log = outside.join(format!("server-{}.log", addr.port()));
        let log_file = File::create(&log).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_fluree"));
        cmd.args(["server", "run", "--memory", "--listen-addr"])
            .arg(addr.to_string())
            .current_dir(cwd)
            .env("HOME", outside)
            .env("TMPDIR", outside)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file));
        for (var, _) in std::env::vars_os() {
            let name = var.to_string_lossy();
            if name.starts_with("FLUREE_") || name.starts_with("XDG_") {
                cmd.env_remove(&var);
            }
        }
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
            if let Ok(resp) = reqwest::get(format!("{}/health", server.base)).await {
                if resp.status().is_success() {
                    return server;
                }
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

    fn url(&self, path: &str) -> String {
        format!("{}/v1/fluree{path}", self.base)
    }

    /// SIGTERM, then wait for the process to exit.
    async fn stop(mut self) -> ExitStatus {
        // SAFETY: signalling a child process this test spawned and still owns.
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "server ignored SIGTERM");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for MemoryServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn sparql_names(client: &reqwest::Client, server: &MemoryServer) -> Value {
    let resp = client
        .post(server.url(&format!("/query/{LEDGER}")))
        .header("content-type", "application/sparql-query")
        .body("SELECT ?name WHERE { ?s <http://example.org/name> ?name }")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

fn assert_empty(dir: &Path) {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert!(entries.is_empty(), "memory server wrote {entries:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_server_serves_without_a_project_and_leaves_nothing() {
    let cwd = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let client = reqwest::Client::new();

    let server = MemoryServer::start(cwd.path(), outside.path()).await;

    let resp = client
        .post(server.url("/create"))
        .json(&json!({ "ledger": LEDGER }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .post(server.url("/insert"))
        .header("fluree-ledger", LEDGER)
        .json(&json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice",
            "ex:name": "Alice"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let names = sparql_names(&client, &server).await;
    assert!(names.to_string().contains("Alice"), "{names}");

    // An index build writes to and reads back from memory storage too.
    let resp = client
        .post(server.url("/reindex"))
        .json(&json!({ "ledger": LEDGER }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    let names = sparql_names(&client, &server).await;
    assert!(names.to_string().contains("Alice"), "{names}");

    let stats: Value = client
        .get(server.url("/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["storage_type"], "memory");

    assert_empty(cwd.path());
    assert!(server.stop().await.success());
    assert_empty(cwd.path());

    // The data went with the process.
    let server = MemoryServer::start(cwd.path(), outside.path()).await;
    let resp = client
        .get(server.url(&format!("/info/{LEDGER}")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    assert!(server.stop().await.success());
    assert_empty(cwd.path());
}

#[test]
fn memory_conflicts_with_other_storage_flags() {
    let dir = TempDir::new().unwrap();
    for other in ["--storage-path", "--connection-config"] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_fluree"))
            .args(["server", "run", "--memory", other, "/elsewhere"])
            .args(["--listen-addr", "127.0.0.1:0"])
            .current_dir(dir.path())
            .env("HOME", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // Accepting the combination would start a server; don't wait on it.
        let deadline = Instant::now() + Duration::from_secs(20);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("`--memory {other}` was accepted");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let out = child.wait_with_output().unwrap();
        assert!(!out.status.success());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("cannot be used with"), "{stderr}");
    }
}

/// Memory storage is foreground-only, however it is asked for.
#[test]
fn start_and_restart_refuse_memory() {
    let dir = TempDir::new().unwrap();
    let attempts: [(&[&str], Option<&str>); 4] = [
        (&["server", "start", "--memory"], None),
        (&["server", "start", "--", "--memory"], None),
        (&["server", "start"], Some("true")),
        (&["server", "restart", "--memory"], None),
    ];
    for (args, env) in attempts {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_fluree"));
        cmd.args(args)
            .current_dir(dir.path())
            .env("HOME", dir.path());
        match env {
            Some(v) => cmd.env("FLUREE_MEMORY_STORAGE", v),
            None => cmd.env_remove("FLUREE_MEMORY_STORAGE"),
        };
        let out = cmd.output().unwrap();
        assert!(!out.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("fluree server run --memory"),
            "{args:?}: {stderr}"
        );
    }
    assert_empty(dir.path());
}

/// `restart` refuses memory mode, from `-- --memory` or the environment,
/// before it stops anything: a running daemon is still serving afterwards.
#[tokio::test]
async fn restart_refuses_memory_without_stopping_the_daemon() {
    let dir = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let fluree = |args: &[&str], memory_env: bool| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_fluree"));
        cmd.args(args)
            .current_dir(dir.path())
            .env("HOME", outside.path())
            .env("TMPDIR", outside.path())
            .stdin(Stdio::null());
        for (var, _) in std::env::vars_os() {
            let name = var.to_string_lossy();
            if name.starts_with("FLUREE_") || name.starts_with("XDG_") {
                cmd.env_remove(&var);
            }
        }
        if memory_env {
            cmd.env("FLUREE_MEMORY_STORAGE", "true");
        }
        cmd
    };
    /// Stops the daemon however the test ends.
    struct Daemon<F: Fn(&[&str], bool) -> Command>(F);
    impl<F: Fn(&[&str], bool) -> Command> Drop for Daemon<F> {
        fn drop(&mut self) {
            let _ = (self.0)(&["server", "stop"], false).output();
        }
    }

    assert!(fluree(&["init"], false).status().unwrap().success());
    let addr = {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap()
    };
    let started = fluree(
        &["server", "start", "--listen-addr", &addr.to_string()],
        false,
    )
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .status()
    .unwrap();
    assert!(started.success());
    let _daemon = Daemon(fluree);
    let health = format!("http://{addr}/health");
    let healthy = || async {
        reqwest::get(&health)
            .await
            .is_ok_and(|r| r.status().is_success())
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !healthy().await {
        assert!(Instant::now() < deadline, "daemon never became healthy");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (args, memory_env) in [
        (&["server", "restart", "--", "--memory"][..], false),
        (&["server", "restart"][..], true),
    ] {
        let out = (_daemon.0)(args, memory_env).output().unwrap();
        assert!(!out.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("fluree server run --memory"),
            "{args:?}: {stderr}"
        );
        assert!(healthy().await, "{args:?} stopped the daemon");
    }
}

/// Run the `fluree` CLI in `dir` with `home` as HOME.
fn cli(dir: &Path, home: &Path, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fluree"));
    cmd.args(args)
        .current_dir(dir)
        .env("HOME", home)
        .env("NO_COLOR", "1")
        .stdin(Stdio::null());
    for (var, _) in std::env::vars_os() {
        let name = var.to_string_lossy();
        if name.starts_with("FLUREE_") || name.starts_with("XDG_") {
            cmd.env_remove(&var);
        }
    }
    cmd.output().unwrap()
}

/// `fluree query --remote … --at` reads the ledger at that time through the
/// server's path pin and sends the query as written. A `GRAPH` with no dataset
/// clause reads the ledger's named graph (an injected FROM used to hide it), a
/// JSON-LD `from` naming a graph keeps it (it used to be overwritten with the
/// ledger's address), and a SPARQL query may carry its own FROM (it used to be
/// refused).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_at_pins_the_path_and_keeps_the_querys_dataset() {
    let outside = TempDir::new().unwrap();
    let serve_dir = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let server = MemoryServer::start(serve_dir.path(), outside.path()).await;
    let client = reqwest::Client::new();

    let post = |path: &str, content_type: &str, body: String| {
        client
            .post(server.url(path))
            .header("content-type", content_type)
            .body(body)
            .send()
    };
    let resp = post(
        "/create",
        "application/json",
        json!({"ledger": "pin:main"}).to_string(),
    )
    .await
    .unwrap();
    assert!(resp.status().is_success(), "create: {}", resp.status());
    for (default, graph) in [("A", "GA"), ("B", "GB")] {
        let trig = format!(
            "<http://ex.org/d{default}> <http://ex.org/name> \"{default}\" .\n\
             GRAPH <http://ex.org/g> {{ <http://ex.org/g{graph}> <http://ex.org/name> \"{graph}\" . }}\n"
        );
        let resp = post("/upsert/pin:main", "application/trig", trig)
            .await
            .unwrap();
        assert!(resp.status().is_success(), "upsert: {}", resp.status());
    }

    let home = outside.path();
    assert!(cli(project.path(), home, &["init"]).status.success());
    assert!(cli(
        project.path(),
        home,
        &["remote", "add", "origin", &server.base]
    )
    .status
    .success());
    let query = |args: &[&str]| {
        let mut all = vec![
            "query", "--remote", "origin", "-l", "pin:main", "--at", "1", "--format", "json",
        ];
        all.extend_from_slice(args);
        let out = cli(project.path(), home, &all);
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).to_string(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    };

    // No dataset: the default graph at t=1.
    let (ok, stdout, stderr) = query(&[
        "--sparql",
        "-e",
        "SELECT ?n WHERE { ?s <http://ex.org/name> ?n }",
    ]);
    assert!(ok, "{stderr}");
    assert!(
        stdout.contains("\"A\"") && !stdout.contains("\"B\""),
        "{stdout}"
    );

    // No dataset and a GRAPH: the ledger's named graph at t=1 (the injected
    // FROM of the older rewrite left GRAPH nothing to match).
    let (ok, stdout, stderr) = query(&[
        "--sparql",
        "-e",
        "SELECT ?n WHERE { GRAPH <http://ex.org/g> { ?s <http://ex.org/name> ?n } }",
    ]);
    assert!(ok, "{stderr}");
    assert!(
        stdout.contains("\"GA\"") && !stdout.contains("\"GB\""),
        "{stdout}"
    );

    // A JSON-LD `from` naming a graph keeps it, at t=1.
    let (ok, stdout, stderr) = query(&[
        "-e",
        r#"{"from": {"@id": "pin:main", "graph": "http://ex.org/g"}, "select": "?n",
            "where": {"@id": "?s", "http://ex.org/name": "?n"}}"#,
    ]);
    assert!(ok, "{stderr}");
    assert!(
        stdout.contains("\"GA\"") && !stdout.contains("\"GB\"") && !stdout.contains("\"A\""),
        "{stdout}"
    );

    // A SPARQL query with its own FROM NAMED, at t=1.
    let (ok, stdout, stderr) = query(&[
        "--sparql",
        "-e",
        "SELECT ?n FROM NAMED <http://ex.org/g> \
         WHERE { GRAPH <http://ex.org/g> { ?s <http://ex.org/name> ?n } }",
    ]);
    assert!(ok, "{stderr}");
    assert!(
        stdout.contains("\"GA\"") && !stdout.contains("\"GB\""),
        "{stdout}"
    );

    server.stop().await;
}

/// A server that predates path pins (before v4.2.2) parses the pinned path as a
/// ledger id and answers a 500 naming it. Against such a server `--remote --at`
/// falls back to the older rewrite: the same query with the time in an
/// injected FROM, on the unpinned path. The stand-in replies as v4.1.6 and
/// v4.2.1 do, verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_at_falls_back_on_a_server_without_path_pins() {
    use axum::http::{StatusCode, Uri};
    use axum::response::IntoResponse;
    use std::sync::{Arc, Mutex};

    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let recorder = Arc::clone(&seen);
    let app = axum::Router::new().fallback(move |uri: Uri, body: String| {
        let recorder = Arc::clone(&recorder);
        async move {
            let path = uri.path().replace("%40", "@").replace("%3A", ":");
            recorder.lock().unwrap().push((path.clone(), body));
            match path.strip_prefix("/v1/fluree/query/") {
                Some(ledger) if ledger.contains('@') => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({
                        "error": format!(
                            "Ledger error: Nameservice error: Invalid ID format: Invalid ledger \
                             ID format '{ledger}': expected 'name' or 'name:branch'"
                        ),
                        "status": 500,
                        "@type": "err:system/InternalError"
                    })),
                )
                    .into_response(),
                Some(_) => axum::Json(json!({
                    "head": {"vars": ["n"]},
                    "results": {"bindings": [{"n": {"type": "literal", "value": "A"}}]}
                }))
                .into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}/v1/fluree", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let outside = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let home = outside.path();
    assert!(cli(project.path(), home, &["init"]).status.success());
    assert!(cli(
        project.path(),
        home,
        &["remote", "add", "origin", &api_base]
    )
    .status
    .success());
    let out = cli(
        project.path(),
        home,
        &[
            "query",
            "--remote",
            "origin",
            "-l",
            "pin:main",
            "--at",
            "1",
            "--format",
            "json",
            "--sparql",
            "-e",
            "SELECT ?n WHERE { ?s <http://ex.org/name> ?n }",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("\"A\""), "{stdout}");

    let queries: Vec<(String, String)> = seen
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.starts_with("/v1/fluree/query/"))
        .cloned()
        .collect();
    assert_eq!(queries.len(), 2, "{queries:?}");
    assert_eq!(queries[0].0, "/v1/fluree/query/pin:main@t:1");
    assert_eq!(queries[1].0, "/v1/fluree/query/pin:main");
    assert!(queries[1].1.contains("FROM <pin:main@t:1>"), "{queries:?}");
}
