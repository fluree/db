//! `fluree iceberg map --stored-catalog / --stored-mapping`, run as the real
//! binary over HTTP.
//!
//! A remote that stores catalog connections and mappings takes a map request
//! naming them by id. This file checks what reaches such a remote (only the
//! references and the source's options, never a connection), that a remote's
//! refusal comes back with a hint, that a db server, which stores neither,
//! refuses the request rather than registering anything, and that a local map
//! refuses references outright.
//!
//! Compiles only with the `server` and `iceberg` features, both default
//! features of this crate.

#![cfg(all(feature = "server", feature = "iceberg"))]

use assert_cmd::cargo_bin_cmd;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// What the CLI printed and whether it succeeded.
struct Run {
    ok: bool,
    stdout: String,
    stderr: String,
}

/// Run the CLI in `home`, off the async runtime so the server keeps serving.
async fn fluree(home: &TempDir, args: &[&str]) -> Run {
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
        Run {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    })
    .await
    .expect("join")
}

/// A project directory whose remote `origin` is the server at `api_base`.
async fn project_with_remote(api_base: &str) -> TempDir {
    let home = tempfile::tempdir().expect("tempdir");
    for args in [vec!["init"], vec!["remote", "add", "origin", api_base]] {
        let run = fluree(&home, &args).await;
        assert!(run.ok, "fluree {args:?}: {}", run.stderr);
    }
    home
}

/// Every map request body a mock remote has received.
type Bodies = Arc<Mutex<Vec<Value>>>;

/// A remote that stores connections and mappings, reduced to its map
/// endpoint: it records every body and answers with `reply`.
struct MockRemote {
    api_base: String,
    bodies: Bodies,
}

async fn mock_remote(reply: (StatusCode, Value)) -> MockRemote {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let state = (bodies.clone(), reply);
    let app = Router::new()
        .route(
            "/v1/fluree/iceberg/map",
            post(
                |State((bodies, reply)): State<(Bodies, (StatusCode, Value))>,
                 Json(body): Json<Value>| async move {
                    bodies.lock().unwrap().push(body);
                    (reply.0, Json(reply.1))
                },
            ),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let api_base = format!("http://{}/v1/fluree", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    MockRemote { api_base, bodies }
}

fn mapped() -> (StatusCode, Value) {
    (
        StatusCode::CREATED,
        json!({
            "graph_source_id": "orders:main",
            "table_identifier": "sales.orders",
            "catalog_uri": "https://catalog.example.com",
            "connection_tested": true,
            "mapping_source": "stored",
            "triples_map_count": 2,
            "table_count": 1,
            "table_names": ["sales.orders"],
            "mapping_validated": true
        }),
    )
}

#[tokio::test]
async fn a_stored_remote_receives_references_and_no_connection() {
    let remote = mock_remote(mapped()).await;
    let home = project_with_remote(&remote.api_base).await;
    let mapping = home.path().join("orders.ttl");
    std::fs::write(&mapping, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();

    let stored_mapping = fluree(
        &home,
        &[
            "iceberg",
            "map",
            "orders",
            "--remote",
            "origin",
            "--stored-mapping",
            "map-1",
            "--branch",
            "main",
        ],
    )
    .await;
    assert!(stored_mapping.ok, "{}", stored_mapping.stderr);
    assert!(
        stored_mapping
            .stdout
            .contains("Mapped Iceberg table as R2RML graph source 'orders:main'"),
        "{}",
        stored_mapping.stdout
    );

    let stored_catalog = fluree(
        &home,
        &[
            "iceberg",
            "map",
            "orders",
            "--remote",
            "origin",
            "--stored-catalog",
            "cat-1",
            "--r2rml",
            mapping.to_str().unwrap(),
            "--table",
            "sales.orders",
        ],
    )
    .await;
    assert!(stored_catalog.ok, "{}", stored_catalog.stderr);

    let bodies = remote.bodies.lock().unwrap().clone();
    assert_eq!(
        bodies,
        vec![
            json!({"name": "orders", "stored_mapping_id": "map-1", "branch": "main"}),
            json!({
                "name": "orders",
                "stored_catalog_id": "cat-1",
                "table": "sales.orders",
                "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
                "r2rml_type": "text/turtle"
            }),
        ]
    );
}

#[tokio::test]
async fn a_remote_refusal_of_a_reference_carries_the_hint() {
    let remote = mock_remote((
        StatusCode::BAD_REQUEST,
        json!({"error": "unknown stored mapping"}),
    ))
    .await;
    let home = project_with_remote(&remote.api_base).await;

    let run = fluree(
        &home,
        &[
            "iceberg",
            "map",
            "orders",
            "--remote",
            "origin",
            "--stored-mapping",
            "map-9",
        ],
    )
    .await;
    assert!(!run.ok);
    assert!(
        run.stderr.contains("unknown stored mapping"),
        "{}",
        run.stderr
    );
    assert!(
        run.stderr
            .contains("this remote may not support stored catalog connections or mappings"),
        "{}",
        run.stderr
    );
}

#[tokio::test]
async fn a_db_server_refuses_a_reference_and_registers_nothing() {
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
    let home = project_with_remote(&url).await;
    let mapping = home.path().join("orders.ttl");
    std::fs::write(&mapping, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();
    let mapping = mapping.to_str().unwrap();

    // The server's own reason: a body without `r2rml` lacks the mapping it
    // requires; one with it carries a reference it does not know.
    for (refs, reason) in [
        (
            vec!["--stored-mapping", "map-1"],
            "an R2RML mapping is required",
        ),
        (
            vec!["--stored-catalog", "cat-1", "--stored-mapping", "map-1"],
            "an R2RML mapping is required",
        ),
        (
            vec![
                "--stored-catalog",
                "cat-1",
                "--r2rml",
                mapping,
                "--table",
                "sales.orders",
            ],
            "is not a field of this request",
        ),
    ] {
        let mut args = vec!["iceberg", "map", "orders", "--remote", "origin"];
        args.extend_from_slice(&refs);
        let run = fluree(&home, &args).await;
        assert!(!run.ok, "{refs:?} was accepted: {}", run.stdout);
        assert!(run.stderr.contains(reason), "{refs:?}: {}", run.stderr);
        assert!(
            run.stderr
                .contains("may not support stored catalog connections"),
            "{refs:?}: {}",
            run.stderr
        );
        // The command's context and the server's refusal make one message.
        assert_eq!(
            run.stderr.matches("error:").count(),
            1,
            "{refs:?}: {}",
            run.stderr
        );
        assert!(
            run.stderr
                .contains("failed to map Iceberg graph source on 'origin': bad request:"),
            "{refs:?}: {}",
            run.stderr
        );
    }
    assert!(state
        .fluree
        .nameservice()
        .lookup_graph_source("orders:main")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_local_map_refuses_a_reference() {
    let home = tempfile::tempdir().expect("tempdir");
    assert!(fluree(&home, &["init"]).await.ok);
    let run = fluree(
        &home,
        &["iceberg", "map", "orders", "--stored-mapping", "map-1"],
    )
    .await;
    assert!(!run.ok);
    assert!(
        run.stderr.contains("a local map has no such store"),
        "{}",
        run.stderr
    );
}
