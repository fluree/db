//! Every Iceberg route reads its connection through the one codec, so the
//! auth a request gives reaches the catalog's auth provider on the read-only
//! routes exactly as it does on `iceberg/map`. Google metadata auth used to be
//! dropped silently by browse, preview, verify, generate and validate: their
//! request type had no such field, and serde ignored it.
//!
//! No network: the server's host-identity policy refuses every host identity,
//! so a Google metadata identity that reaches the provider is answered 403
//! `HostIdentityRefused` before anything connects. A dropped one would build no
//! provider at all and try `catalog.invalid` instead, which never resolves.
#![cfg(feature = "iceberg")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const CATALOG: &str = "https://catalog.invalid/iceberg/v1/restcatalog";

async fn server() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let mut state = AppState::new(cfg, telemetry).await.expect("AppState::new");
    state.fluree = Arc::new(
        state
            .fluree
            .with_host_identity_policy(fluree_db_api::HostIdentityPolicy::deny_all()),
    );
    (tmp, Arc::new(state))
}

async fn post(state: &Arc<AppState>, route: &str, body: &Value) -> (StatusCode, Value) {
    let resp = build_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/iceberg/{route}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// `connection` plus each route's own fields.
fn bodies(connection: &Value) -> Vec<(&'static str, Value)> {
    let with = |extra: Value| {
        let mut body = connection.clone();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        body
    };
    vec![
        ("catalog/browse", with(json!({}))),
        (
            "catalog/preview",
            with(json!({"namespace": "ns", "name": "t"})),
        ),
        ("catalog/verify", with(json!({"table": "ns.t"}))),
        (
            "r2rml/generate",
            with(json!({"tables": [{"namespace": "ns", "name": "t"}],
                "base_namespace": "http://example.org/"})),
        ),
    ]
}

#[tokio::test]
async fn google_auth_reaches_the_provider_on_every_read_only_route() {
    let (_tmp, state) = server().await;
    let flat = json!({"mode": "rest", "catalog_uri": CATALOG, "auth_google_metadata": true});
    let structured = json!({"catalog": {"type": "rest", "uri": CATALOG,
        "auth": {"type": "google", "credentials": {"source": "metadata"},
            "quota_project": "lake"}}});
    for connection in [flat, structured] {
        for (route, body) in bodies(&connection) {
            let (status, response) = post(&state, route, &body).await;
            assert_eq!(
                (status, response["@type"].as_str()),
                (
                    StatusCode::FORBIDDEN,
                    Some(fluree_vocab::errors::HOST_IDENTITY_REFUSED)
                ),
                "{route} {connection}: {response}"
            );
        }
        // `validate` reports a table it could not preview as a diagnostic,
        // so the refusal arrives in a 200 naming the policy's variable.
        let mut validate = connection.clone();
        validate["r2rml"] = MAPPING.into();
        let (status, response) = post(&state, "r2rml/validate", &validate).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert!(
            response
                .to_string()
                .contains(fluree_db_api::HOST_IDENTITIES_ENV),
            "validate {connection}: {response}"
        );
    }
}

const MAPPING: &str = r#"@prefix rr: <http://www.w3.org/ns/r2rml#> .
<http://example.org/mapping#T> a rr:TriplesMap ; rr:logicalTable [ rr:tableName "ns.t" ] ;
  rr:subjectMap [ rr:template "http://example.org/{id}" ] ."#;

#[tokio::test]
async fn a_connection_the_codec_refuses_is_a_typed_400_naming_the_field() {
    let (_tmp, state) = server().await;
    for (connection, field) in [
        (
            json!({"mode": "rest", "catalog_uri": CATALOG, "auth_google_metadta": true}),
            "auth_google_metadta",
        ),
        (
            json!({"catalog": {"type": "rest", "uri": CATALOG}, "auth_bearer": "t"}),
            "auth_bearer",
        ),
        (
            json!({"mode": "rest", "catalog_uri": CATALOG, "auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}),
            "auth_bearer_env",
        ),
        (
            json!({"catalog": {"type": "rest", "uri": CATALOG, "auth": {"type": "google",
                "credentials": {"source": "service_account", "key": "{}"}}}}),
            "catalog.auth.credentials.key",
        ),
    ] {
        for (route, body) in bodies(&connection) {
            let (status, response) = post(&state, route, &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route}: {response}");
            assert_eq!(
                response["@type"],
                fluree_vocab::errors::INVALID_GRAPH_SOURCE_CONFIG,
                "{route}: {response}"
            );
            assert!(
                response["error"].as_str().unwrap_or("").contains(field),
                "{route}: {response}"
            );
        }
    }
}
