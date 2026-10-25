//! `fluree publish` and `fluree push` of a history containing a merge, run as
//! the real binary against a real server over HTTP.
//!
//! The pieces are tested on their own elsewhere: the transfer planner, the
//! remote client's endpoint choice, the server routes, and the receiver. This
//! file checks that they meet. A merge must reach the server with the commits
//! it brought in, and the server must end up with the merged data and a
//! complete commit history.
//!
//! The same harness checks that `fluree info` of a tracked ledger shows the
//! config diagnostics the server reports, and that `fluree pull` takes what
//! a remote's journal gained without a commit, and `fluree push` sends it.
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
    serve(false).await
}

/// A server whose storage a token with `fluree.storage.all` replicates, as
/// clone and pull do, and such a token, which may also read the raw
/// commits of a ledger that suppresses history, and push.
async fn start_replicating_server() -> (Server, String) {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use fluree_db_credential::{did_from_pubkey, sign_ed25519, SigningKey};

    let key = SigningKey::from_bytes(&[7u8; 32]);
    let pubkey = key.verifying_key().to_bytes();
    let header = json!({
        "alg": "EdDSA",
        "jwk": { "kty": "OKP", "crv": "Ed25519", "x": URL_SAFE_NO_PAD.encode(pubkey) }
    });
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let payload = json!({
        "iss": did_from_pubkey(&pubkey),
        "sub": "replica@example.com",
        "exp": now + 3600,
        "iat": now,
        "fluree.storage.all": true,
        "fluree.ledger.audit.all": true,
        "fluree.ledger.write.all": true,
        "fluree.identity": "ex:Replica"
    });
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(payload.to_string())
    );
    let signature = URL_SAFE_NO_PAD.encode(sign_ed25519(&key, signing_input.as_bytes()));
    (serve(true).await, format!("{signing_input}.{signature}"))
}

async fn serve(replicating: bool) -> Server {
    let storage = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(storage.path().to_path_buf()),
        storage_proxy_enabled: replicating,
        storage_proxy_insecure_accept_any_issuer: replicating,
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

/// Run the CLI in `home` and assert that it fails. Returns its stderr.
async fn fluree_fails(home: &TempDir, args: &[&str]) -> String {
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
            !output.status.success(),
            "fluree {args:?} succeeded\nstdout: {}",
            String::from_utf8_lossy(&output.stdout),
        );
        String::from_utf8_lossy(&output.stderr).into_owned()
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

/// `fluree info` of a tracked ledger prints the config diagnostics in the
/// server's ledger info, as it does for a local ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn info_of_a_tracked_ledger_prints_config_diagnostics() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/fluree/create", server.url))
        .json(&json!({ "ledger": "mydb" }))
        .send()
        .await
        .expect("create");
    assert!(response.status().is_success(), "create failed");

    let home = TempDir::new().unwrap();
    fluree(&home, &["init"]).await;
    fluree(&home, &["remote", "add", "origin", &server.url]).await;
    fluree(&home, &["track", "add", "mydb", "--remote", "origin"]).await;
    let info = fluree(&home, &["info", "mydb"]).await;
    assert!(!info.contains("Config diagnostics"), "{info}");

    // A shape, and no config that enables SHACL.
    let response = client
        .post(format!("{}/v1/fluree/insert/mydb:main", server.url))
        .json(&json!({
            "@context": {
                "ex": "http://example.org/",
                "sh": "http://www.w3.org/ns/shacl#"
            },
            "@id": "ex:PersonShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": { "@id": "ex:Person" }
        }))
        .send()
        .await
        .expect("insert");
    assert!(response.status().is_success(), "insert failed");

    let info = fluree(&home, &["info", "mydb"]).await;
    assert!(info.contains("Config diagnostics:"), "{info}");
    assert!(info.contains("shapes present"), "{info}");
}

/// A pull from a remote whose commits the clone already has still takes
/// what the remote's journal gained since, which no commit records: history
/// the remote removed is refused here too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_with_no_new_commits_takes_the_journal() {
    let (server, token) = start_replicating_server().await;
    let client = reqwest::Client::new();
    let ctx = json!({ "ex": "http://example.org/" });
    for (path, body) in [
        ("create", json!({ "ledger": "mydb" })),
        (
            "insert/mydb:main",
            json!({ "@context": ctx, "@id": "ex:alice", "ex:email": "a@old" }),
        ),
        (
            "upsert/mydb:main",
            json!({ "@context": ctx, "@id": "ex:alice", "ex:email": "a@new" }),
        ),
    ] {
        let response = client
            .post(format!("{}/v1/fluree/{path}", server.url))
            .json(&body)
            .send()
            .await
            .expect(path);
        assert!(response.status().is_success(), "{path} failed");
    }
    let remote = &server.state.fluree;
    // A remote's ledger info reports its index's `t`.
    remote
        .reindex("mydb:main", fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();

    let home = TempDir::new().unwrap();
    fluree(&home, &["init"]).await;
    fluree(
        &home,
        &["remote", "add", "origin", &server.url, "--token", &token],
    )
    .await;
    fluree(&home, &["clone", "origin", "mydb"]).await;
    let past = "SELECT ?o WHERE { <http://example.org/alice> <http://example.org/email> ?o }";
    let served = fluree(&home, &["query", "mydb", past, "--at", "1"]).await;
    assert!(served.contains("a@old"), "{served}");

    let removal = fluree_db_api::retention::RemovalRequest {
        matching: vec![fluree_db_api::retention::FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!("a@old")),
        }],
        ..Default::default()
    };
    let before = remote.ledger("mydb:main").await.unwrap().t();
    let receipt = remote
        .remove_occurrences("mydb:main", removal)
        .await
        .unwrap();
    assert_eq!(receipt.retracted_t, None, "no longer current: no commit");
    assert_eq!(remote.ledger("mydb:main").await.unwrap().t(), before);

    let pulled = fluree(&home, &["pull", "mydb"]).await;
    assert!(pulled.contains("already up to date"), "{pulled}");
    let refused = fluree_fails(&home, &["query", "mydb", past, "--at", "1"]).await;
    assert!(refused.contains("history"), "{refused}");
}

/// A push to a remote whose head is this copy's still sends what the copy's
/// journal gained since, which no commit records: history removed here is
/// refused there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_with_no_new_commits_sends_the_journal() {
    let (server, token) = start_replicating_server().await;
    let client = reqwest::Client::new();
    let ctx = json!({ "ex": "http://example.org/" });
    for (path, body) in [
        ("create", json!({ "ledger": "mydb" })),
        (
            "insert/mydb:main",
            json!({ "@context": ctx, "@id": "ex:alice", "ex:email": "a@old" }),
        ),
        (
            "upsert/mydb:main",
            json!({ "@context": ctx, "@id": "ex:alice", "ex:email": "a@new" }),
        ),
    ] {
        let response = client
            .post(format!("{}/v1/fluree/{path}", server.url))
            .json(&body)
            .send()
            .await
            .expect(path);
        assert!(response.status().is_success(), "{path} failed");
    }
    let remote = &server.state.fluree;
    // A remote's ledger info reports its index's `t`.
    remote
        .reindex("mydb:main", fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();

    let home = TempDir::new().unwrap();
    fluree(&home, &["init"]).await;
    fluree(
        &home,
        &["remote", "add", "origin", &server.url, "--token", &token],
    )
    .await;
    fluree(&home, &["clone", "origin", "mydb"]).await;

    let storage = home.path().join(".fluree").join("storage");
    assert!(storage.is_dir(), "{}", storage.display());
    {
        let local = fluree_db_api::FlureeBuilder::file(storage.to_string_lossy().to_string())
            .build()
            .unwrap();
        let removal = fluree_db_api::retention::RemovalRequest {
            matching: vec![fluree_db_api::retention::FactSelector {
                subject: "http://example.org/alice".into(),
                predicate: "http://example.org/email".into(),
                graph: None,
                object: Some(json!("a@old")),
            }],
            ..Default::default()
        };
        let receipt = local
            .remove_occurrences("mydb:main", removal)
            .await
            .unwrap();
        assert_eq!(receipt.retracted_t, None, "no longer current: no commit");
    }

    let pushed = fluree(&home, &["push", "mydb"]).await;
    assert!(pushed.contains("already up to date"), "{pushed}");
    assert!(pushed.contains("removal journal"), "{pushed}");
    let past = remote
        .query_connection(&json!({
            "@context": ctx,
            "from": "mydb:main@t:1",
            "select": "?o",
            "where": { "@id": "ex:alice", "ex:email": "?o" }
        }))
        .await;
    assert!(
        matches!(past, Err(fluree_db_api::ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
}
