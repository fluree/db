//! HTTP-layer integration test for cross-ledger failure propagation.
//!
//! Verifies that `ApiError::CrossLedger` actually surfaces as HTTP 502
//! with a structured body. The api-crate already unit-tests the
//! `status_code() == 502` mapping; this test closes the loop through
//! the server's `ServerError` translation and Axum response chain.
//! A regression where the server's status-code mapping omits the
//! `CrossLedger` arm (and silently falls back to 500) would fail
//! here even with all api-level tests passing.

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue =
        serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

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

/// A query against a data ledger whose `#config` declares
/// `f:policySource` with a cross-ledger `f:ledger` pointing at a
/// model ledger that does not exist on this instance must return
/// HTTP 502, NOT 500. The body must carry the structured
/// `err:system/CrossLedgerError` error_type so clients can branch.
#[tokio::test]
async fn query_under_cross_ledger_config_to_missing_model_returns_502() {
    let (_tmp, state) = server_state().await;

    let data_id = "test/xledger-http/data:main";
    let model_id = "test/xledger-http/never-created:main";
    let policy_graph_iri = "http://example.org/missing-policies";

    // Create D through the HTTP layer. The model ledger is
    // intentionally never created — the missing dependency is
    // exactly what we're proving the server reports.
    let create_resp = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(json!({"ledger": data_id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED, "create D");

    // Seed D's data + cross-ledger config in one TriG transaction so
    // the config that points at the missing model lands atomically.
    let config_iri = format!("urn:fluree:{data_id}#config");
    let trig = format!(
        r#"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/ns/> .

        ex:alice rdf:type ex:User ; ex:name "Alice" .

        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
            <urn:cfg:policy> f:defaultAllow true .
            <urn:cfg:policy> f:policyClass f:AccessPolicy .
            <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
            <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                 f:graphSource <urn:cfg:policy-src> .
            <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                 f:graphSelector <{policy_graph_iri}> .
        }}
    "#
    );
    let upsert_resp = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/upsert/{data_id}"))
                .header("content-type", "application/trig")
                .body(Body::from(trig))
                .unwrap(),
        )
        .await
        .unwrap();
    let upsert_status = upsert_resp.status();
    let (_, upsert_body) = json_body(upsert_resp).await;
    assert!(
        upsert_status.is_success(),
        "upsert TriG (data + config) should succeed; got {upsert_status}, body: {upsert_body}"
    );

    // --- query D. The cross-ledger config routes through
    // resolve_graph_ref, which fails with ModelLedgerMissing since
    // the model ledger was never created. That lifts through
    // ApiError::CrossLedger and (this is what we're testing) the
    // server should translate it to HTTP 502.
    //
    // The JSON-LD query route only engages policy enforcement when
    // the request carries `opts.identity`, `opts.policy-class`, or
    // `opts.policy` (see `has_policy_opts` in routes/query.rs). The
    // server injects `fluree-policy-class` headers into
    // `opts.policy-class`, so sending that header is the minimal
    // way to engage the policy path without a credential. Setting
    // it to `f:AccessPolicy` also matches the default class the
    // cross-ledger filter applies, so the materialized restrictions
    // would actually be enforced if the model ledger existed —
    // here the missing-model surface is the failure mode we want
    // to observe.
    //
    // Whether config-only policy should auto-engage without an
    // explicit policy header is a separate concern from this slice;
    // this test pins the 502 mapping, not that auto-engagement.
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": "?u",
        "where": {"@id": "?u", "@type": "ex:User"}
    });
    let resp = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/query/{data_id}"))
                .header("content-type", "application/json")
                .header("fluree-policy-class", "https://ns.flur.ee/db#AccessPolicy")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = json_body(resp).await;

    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "cross-ledger model missing must surface as 502 Bad Gateway, \
         not 500 Internal Server Error. Body: {body}"
    );

    // Structured body shape: status echoes 502, @type names the
    // cross-ledger error category, and the error message mentions
    // the missing model ledger by id so operators can locate it
    // without spelunking logs.
    assert_eq!(body["status"], 502);
    assert_eq!(body["@type"], "err:system/CrossLedgerError");
    let msg = body["error"].as_str().unwrap_or("");
    assert!(
        msg.contains(model_id),
        "error message must name the missing model ledger '{model_id}', got: {msg}"
    );
}
