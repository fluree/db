//! `fluree publish` and `fluree push` of a history containing a merge, run as
//! the real binary against a real server over HTTP.
//!
//! The pieces are tested on their own elsewhere: the transfer planner, the
//! remote client's endpoint choice, the server routes, and the receiver. This
//! file checks that they meet. A merge must reach the server with the commits
//! it brought in, and the server must end up with the merged data and a
//! complete commit history.
//!
//! Like `track_headers_seam.rs`, this compiles only with the `server`
//! feature, which is a default feature of this crate.

#![cfg(feature = "server")]

use assert_cmd::cargo_bin_cmd;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

struct Server {
    _storage: TempDir,
    state: Arc<AppState>,
    url: String,
}

async fn start_server() -> Server {
    let storage = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(storage.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = build_router(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        _storage: storage,
        state,
        url,
    }
}

/// Run the CLI in `home`, off the async runtime so the server keeps
/// serving, and assert that it succeeds.
async fn fluree(home: &TempDir, args: &[&str]) -> String {
    let dir = home.path().to_path_buf();
    let args: Vec<String> = args.iter().map(ToString::to_string).collect();
    tokio::task::spawn_blocking(move || {
        let output = cargo_bin_cmd!("fluree")
            .current_dir(&dir)
            .env("HOME", &dir)
            .env("NO_COLOR", "1")
            .args(&args)
            .output()
            .expect("run fluree");
        assert!(
            output.status.success(),
            "fluree {args:?} failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    })
    .await
    .expect("join")
}

async fn insert(home: &TempDir, ledger: &str, subject: &str, name: &str) {
    let data = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:{subject}"),
        "ex:name": name,
    })
    .to_string();
    fluree(home, &["insert", ledger, &data]).await;
}

/// Commit `name` on a new branch and a different name on main, then merge
/// the branch into main. Main cannot fast-forward.
async fn merge_a_branch(home: &TempDir, branch: &str, (on_branch, on_main): (&str, &str)) {
    fluree(home, &["branch", "create", branch, "--ledger", "mydb"]).await;
    insert(home, &format!("mydb:{branch}"), on_branch, on_branch).await;
    insert(home, "mydb", on_main, on_main).await;
    fluree(home, &["branch", "merge", branch, "--ledger", "mydb"]).await;
}

/// The `ex:name` values on the server's `mydb:main`, sorted.
async fn remote_names(server: &Server) -> Vec<String> {
    let response = reqwest::Client::new()
        .post(format!("{}/v1/fluree/query", server.url))
        .json(&json!({
            "@context": { "ex": "http://example.org/" },
            "from": "mydb:main",
            "select": ["?name"],
            "where": { "@id": "?s", "ex:name": "?name" }
        }))
        .send()
        .await
        .expect("query");
    assert!(response.status().is_success(), "query failed");
    let rows: Vec<serde_json::Value> = response.json().await.expect("json");
    let mut names: Vec<String> = rows
        .iter()
        .map(|row| {
            let value = row.as_array().map_or(row, |cells| &cells[0]);
            value.as_str().expect("name").to_string()
        })
        .collect();
    names.sort();
    names
}

/// How many commits a walk of every parent from the server's head reaches.
async fn remote_commit_count(server: &Server) -> usize {
    let fluree = &server.state.fluree;
    let store = fluree.branched_content_store("mydb:main").await.unwrap();
    let head = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();
    fluree_db_core::collect_dag_cids(store.as_ref(), &head, 0)
        .await
        .expect("every parent the history names is stored")
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_and_push_carry_merges() {
    let server = start_server().await;
    let home = TempDir::new().unwrap();

    fluree(&home, &["init"]).await;
    fluree(&home, &["create", "mydb"]).await;
    insert(&home, "mydb", "alice", "alice").await;
    merge_a_branch(&home, "dev", ("bob", "carol")).await;

    // Publish sends the whole history, merge included.
    fluree(&home, &["remote", "add", "origin", &server.url]).await;
    fluree(&home, &["publish", "origin", "mydb"]).await;
    assert_eq!(remote_names(&server).await, ["alice", "bob", "carol"]);
    // alice, carol, and the merge on the line, plus dev's bob.
    assert_eq!(remote_commit_count(&server).await, 4);

    // Push sends what the remote lacks, merge included.
    merge_a_branch(&home, "feature", ("dave", "erin")).await;
    fluree(&home, &["push", "mydb"]).await;
    assert_eq!(
        remote_names(&server).await,
        ["alice", "bob", "carol", "dave", "erin"]
    );
    // Adds erin and the second merge on the line, plus feature's dave.
    assert_eq!(remote_commit_count(&server).await, 7);
}
