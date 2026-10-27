//! The graph-source flag groups (catalog auth, Google identity, storage
//! identity) through the binary, and the same flags sent to a server.
//!
//! Locally, `fluree iceberg map` registers what its flags say and refuses
//! what they cannot mean in the flags' own spelling. Remotely, the CLI sends
//! the structured connection its local map would store; the seam test drives
//! the real server router in-process with that exact body and checks the
//! server stores the same record a local map does.
//!
//! No network: the catalog host is `catalog.invalid` (never resolves) and the
//! Google identity is a service-account key held in a variable that is never
//! set, so the registration's connection test fails before anything connects.

#![cfg(feature = "iceberg")]

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

const CATALOG: &str = "https://catalog.invalid/iceberg/v1/restcatalog";
const FEDERATION: &str = "bq://projects/lake/locations/us/catalogs/sales";
/// Named by the flags, never set: the key is not read before registration.
const SA_KEY_VAR: &str = "FLUREE_TEST_GRAPH_SOURCE_SA_KEY_NEVER_SET";
/// The mapping every `map` is registered with, naming the one table.
const MAPPING: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
<http://example.org/mapping#Orders>
    a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "sales.orders" ] ;
    rr:subjectMap [ rr:template "http://example.org/orders/{id}" ] ;
    rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "total" ] ] .
"#;

fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd.env_remove(SA_KEY_VAR);
    cmd
}

/// Write [`MAPPING`] into `dir` and return its path, for `--r2rml`.
fn mapping_file(dir: &std::path::Path) -> String {
    let path = dir.join("mapping.ttl");
    std::fs::write(&path, MAPPING).unwrap();
    path.to_str().unwrap().to_string()
}

fn google_federation_flags() -> Vec<&'static str> {
    vec![
        "--catalog-uri",
        CATALOG,
        "--table",
        "sales.orders",
        "--warehouse",
        FEDERATION,
        "--auth-google-service-account-env",
        SA_KEY_VAR,
        "--auth-google-quota-project",
        "lake",
    ]
}

#[test]
fn a_local_map_stores_the_google_identity_its_flags_name() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    let mapping = mapping_file(tmp.path());
    fluree_cmd(&tmp)
        .args(["iceberg", "map", "orders", "--r2rml", &mapping])
        .args(google_federation_flags())
        .assert()
        .success();

    // `info` shows the stored record, its key named by variable only.
    fluree_cmd(&tmp)
        .args(["iceberg", "info", "orders"])
        .assert()
        .success()
        .stdout(predicate::str::contains("service_account"))
        .stdout(predicate::str::contains(SA_KEY_VAR))
        .stdout(predicate::str::contains("\"quota_project\": \"lake\""))
        // The bq:// default: the catalog's Google identity reads the files.
        .stdout(predicate::str::contains("\"type\": \"catalog\""));
}

#[test]
fn a_local_map_refuses_flags_that_cannot_mean_anything_in_their_spelling() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    let mapping = mapping_file(tmp.path());
    for (flags, refusal) in [
        (
            vec![
                "--catalog-uri",
                CATALOG,
                "--table",
                "ns.t",
                "--oauth2-token-url",
                "https://t.example.com",
            ],
            "--oauth2-token-url needs --oauth2-client-secret",
        ),
        (
            vec![
                "--catalog-uri",
                CATALOG,
                "--table",
                "ns.t",
                "--auth-bearer",
                "t",
                "--auth-google-adc",
            ],
            "--auth-google-adc and --auth-bearer are two ways to authenticate",
        ),
        (
            vec![
                "--mode",
                "glue",
                "--table",
                "ns.t",
                "--auth-google-metadata",
            ],
            "--auth-google-metadata applies to rest mode only",
        ),
        (
            vec![
                "--catalog-uri",
                CATALOG,
                "--table",
                "ns.t",
                "--warehouse",
                FEDERATION,
                "--auth-bearer",
                "t",
            ],
            "never vends",
        ),
    ] {
        fluree_cmd(&tmp)
            .args(["iceberg", "map", "gs", "--r2rml", &mapping])
            .args(&flags)
            .assert()
            .failure()
            .stderr(predicate::str::contains(refusal));
    }
}

#[cfg(feature = "server")]
mod seam {
    use super::*;
    use axum::body::Body;
    use clap::Parser;
    use fluree_db_server::routes::build_router;
    use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn map_args(flags: &[&str]) -> fluree_db_cli::cli::IcebergMapArgs {
        let mut argv = vec!["fluree", "iceberg", "map", "orders"];
        argv.extend_from_slice(flags);
        match fluree_db_cli::cli::Cli::try_parse_from(argv)
            .unwrap()
            .command
        {
            fluree_db_cli::cli::Commands::Iceberg {
                action: fluree_db_cli::cli::IcebergAction::Map(args),
            } => *args,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn a_remote_map_stores_what_a_local_map_would() {
        // The server may name the variable; this binary sets nothing else.
        std::env::set_var(fluree_db_api::SECRET_ENV_VARS_ENV, SA_KEY_VAR);
        std::env::remove_var(SA_KEY_VAR);

        let dir = tempfile::tempdir().unwrap();
        let mapping = mapping_file(dir.path());
        let mut flags = vec!["--r2rml", mapping.as_str()];
        flags.extend(google_federation_flags());
        let args = map_args(&flags);
        let body = fluree_db_cli::commands::iceberg::args_to_json(&args).unwrap();
        assert!(
            body.get("catalog").is_some() && body.get("auth_google_service_account_env").is_none(),
            "the CLI sends the structured connection: {body}"
        );

        let tmp = tempfile::tempdir().unwrap();
        let cfg = ServerConfig {
            cors_enabled: false,
            indexing_enabled: false,
            storage_path: Some(tmp.path().to_path_buf()),
            ..Default::default()
        };
        let telemetry = TelemetryConfig::with_server_config(&cfg);
        let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
        let resp = build_router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/fluree/iceberg/map")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&bytes)
        );

        let record = state
            .fluree
            .nameservice()
            .lookup_graph_source("orders:main")
            .await
            .unwrap()
            .expect("registered");
        let mut remote: serde_json::Value = serde_json::from_str(&record.config).unwrap();
        // The stored mapping is registration's: its content address. The
        // connection and settings are the flags'.
        let stored_mapping = remote.as_object_mut().unwrap().remove("mapping");
        assert!(
            stored_mapping.is_some_and(|m| m["source"].is_string()),
            "the mapping is stored: {remote}"
        );
        let mut local = serde_json::to_value(
            fluree_db_cli::commands::iceberg::build_iceberg_config(&args)
                .unwrap()
                .to_iceberg_gs_config(),
        )
        .unwrap();
        local.as_object_mut().unwrap().remove("mapping");
        assert_eq!(remote, local);
        assert_eq!(
            remote["catalog"]["auth"]["credentials"]["key"]["env_var"],
            SA_KEY_VAR
        );
        assert_eq!(remote["io"]["storage_auth"]["type"], "catalog");
    }
}
