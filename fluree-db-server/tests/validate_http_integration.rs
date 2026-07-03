//! HTTP-layer coverage for `GET|POST /v1/fluree/validate/*ledger` — the
//! SHACL validation-report endpoint. Exercises the shapes-source selection
//! (attached / inline Turtle / inline JSON-LD / replace-vs-union), Accept
//! negotiation (JSON envelope, JSON-LD report, Turtle report), and error
//! paths (conflicting sources, unknown graph, unknown ledger).

#![cfg(feature = "shacl")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn server_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState"));
    (tmp, state)
}

async fn post_json(state: &Arc<AppState>, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

/// Seed a ledger whose committed state violates its attached shape: the
/// data (a User without schema:name) commits before the shape exists, so
/// staging-time enforcement never fires.
async fn seed_nonconforming(state: &Arc<AppState>, ledger: &str) {
    let (status, _) = post_json(state, "/v1/fluree/create", json!({ "ledger": ledger })).await;
    assert_eq!(status, StatusCode::CREATED, "create {ledger}");

    let (status, _) = post_json(
        state,
        &format!("/v1/fluree/insert/{ledger}"),
        json!({
            "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
            "@id": "ex:bob",
            "@type": "ex:User",
            "schema:email": "bob@example.org"
        }),
    )
    .await;
    assert!(status.is_success(), "insert data into {ledger}: {status}");

    let (status, _) = post_json(
        state,
        &format!("/v1/fluree/insert/{ledger}"),
        json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "sh": "http://www.w3.org/ns/shacl#",
                "schema": "http://schema.org/"
            },
            "@id": "ex:UserShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:User"},
            "sh:property": [{
                "@id": "ex:name-ps",
                "sh:path": {"@id": "schema:name"},
                "sh:minCount": 1
            }]
        }),
    )
    .await;
    assert!(status.is_success(), "insert shape into {ledger}: {status}");
}

const EMAIL_SHAPES_TTL: &str = r"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
ex:EmailShape a sh:NodeShape ;
    sh:targetClass ex:User ;
    sh:property [ sh:path schema:email ; sh:minCount 1 ] .
";

#[tokio::test]
async fn validate_attached_shapes_json_envelope() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-attached").await;

    let (status, body) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-attached",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["conforms"], json!(false));
    assert_eq!(body["violations"], json!(1));
    assert_eq!(body["shapesChecked"], json!(1));
    let result = &body["results"][0];
    assert_eq!(result["focus_node"], json!("http://example.org/ns/bob"));
    assert_eq!(
        result["constraint_component"],
        json!("http://www.w3.org/ns/shacl#MinCountConstraintComponent")
    );
}

#[tokio::test]
async fn validate_get_without_body() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-get").await;

    let resp = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/validate/it/validate-get")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: JsonValue = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["conforms"], json!(false));
}

#[tokio::test]
async fn validate_inline_turtle_replaces_attached() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-inline").await;

    // Replace (default): bob conforms to the email-only inline shapes.
    let (status, body) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-inline",
        json!({ "shapes": EMAIL_SHAPES_TTL }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["conforms"], json!(true), "{body}");

    // Union: the attached name shape fires again.
    let (status, body) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-inline",
        json!({ "shapes": EMAIL_SHAPES_TTL, "includeAttached": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["conforms"], json!(false), "{body}");
    assert_eq!(body["violations"], json!(1));
}

#[tokio::test]
async fn validate_inline_jsonld_shapes() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-jsonld-shapes").await;

    let shapes = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "sh": "http://www.w3.org/ns/shacl#",
            "schema": "http://schema.org/"
        },
        "@id": "ex:PhoneShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:User"},
        "sh:property": [{
            "@id": "ex:phone-ps",
            "sh:path": {"@id": "schema:telephone"},
            "sh:minCount": 1
        }]
    });
    let (status, body) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-jsonld-shapes",
        json!({ "shapes": shapes }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["conforms"], json!(false));
    assert_eq!(
        body["results"][0]["result_path"],
        json!("http://schema.org/telephone")
    );
}

#[tokio::test]
async fn validate_accept_negotiation() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-accept").await;

    for (accept, content_type, needle) in [
        (
            "text/turtle",
            "text/turtle",
            "a sh:ValidationReport".to_string(),
        ),
        (
            "application/ld+json",
            "application/ld+json",
            "\"sh:conforms\":false".to_string(),
        ),
    ] {
        let resp = build_router(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/fluree/validate/it/validate-accept")
                    .header("content-type", "application/json")
                    .header("accept", accept)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let got_ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(
            got_ct.starts_with(content_type),
            "Accept {accept}: content-type {got_ct}"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&bytes).replace(char::is_whitespace, "");
        let needle = needle.replace(char::is_whitespace, "");
        assert!(
            text.contains(&needle),
            "Accept {accept}: body missing {needle}: {text}"
        );
    }
}

#[tokio::test]
async fn validate_conflicting_shape_sources_is_bad_request() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-conflict").await;

    let (status, _) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-conflict",
        json!({
            "shapes": EMAIL_SHAPES_TTL,
            "shapesGraph": "http://example.org/graphs/shapes"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn validate_unknown_graph_is_not_found() {
    let (_tmp, state) = server_state().await;
    seed_nonconforming(&state, "it/validate-nograph").await;

    let (status, _) = post_json(
        &state,
        "/v1/fluree/validate/it/validate-nograph",
        json!({ "graph": "http://example.org/graphs/missing" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn validate_unknown_ledger_is_not_found() {
    let (_tmp, state) = server_state().await;
    let (status, _) = post_json(&state, "/v1/fluree/validate/it/no-such-ledger", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
