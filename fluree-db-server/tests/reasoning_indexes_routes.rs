//! The reasoning-index admin routes, driven through the real router: register a
//! program, refresh it, list the branch's indexes, and retire one.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

async fn post(app: &axum::Router, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

#[tokio::test]
async fn register_refresh_list_and_retire_an_index() {
    let (_tmp, state) = test_state().await;
    let app = build_router(Arc::clone(&state));
    let ledger = "routes/indexes:main";
    let (status, _) = post(&app, "/v1/fluree/create", json!({"ledger": ledger})).await;
    assert!(status.is_success(), "{status}");
    let (status, _) = post(
        &app,
        "/v1/fluree/insert",
        json!({
            "ledger": ledger,
            "@context": {
                "ex": "http://example.org/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
            },
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    assert!(status.is_success(), "{status}");

    let (status, registered) = post(
        &app,
        "/v1/fluree/reasoning-indexes/register",
        json!({"ledger": ledger, "program": {"reasoning": "owl2rl"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let index_id = registered["indexId"].as_str().expect("indexId").to_string();

    let (status, refreshed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/refresh",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{refreshed}");
    assert_eq!(refreshed["results"][0]["indexId"], json!(index_id));
    assert_eq!(refreshed["results"][0]["outcome"], json!("published"));
    assert_eq!(refreshed["results"][0]["generation"], json!(1));

    let (status, listed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/list",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert_eq!(listed["indexes"][&index_id]["head"]["generation"], json!(1));

    let (status, _) = post(
        &app,
        "/v1/fluree/reasoning-indexes/retire",
        json!({"ledger": ledger, "indexId": index_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, listed) = post(
        &app,
        "/v1/fluree/reasoning-indexes/list",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(
        listed["indexes"][&index_id]["registration"]["state"]["state"],
        json!("retired")
    );

    let (status, _) = post(
        &app,
        "/v1/fluree/reasoning-indexes/retire",
        json!({"ledger": ledger, "indexId": "not-a-ulid"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A policy-restricted query is answered from an index over HTTP and judged as
/// it would be at query time: a deny on a class a rule derives hides that
/// class's member. The rule derives two facts, so the one-fact budget with
/// `exact` freshness would refuse a query-time answer; the reasoning header
/// names the index.
#[tokio::test]
async fn a_restricted_query_is_served_from_an_index() {
    let (_tmp, state) = test_state().await;
    let app = build_router(Arc::clone(&state));
    let ledger = "routes/restricted:main";
    let ctx = json!({"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"});
    let (status, _) = post(&app, "/v1/fluree/create", json!({"ledger": ledger})).await;
    assert!(status.is_success(), "{status}");
    let rule = json!({
        "@context": {"ex": "http://example.org/"},
        "where": [{"@id": "?p", "ex:salary": "?s"}, ["filter", "(> ?s 200000)"]],
        "insert": {"@id": "?p", "@type": "ex:HighEarner"}
    });
    let (status, body) = post(
        &app,
        "/v1/fluree/insert",
        json!({
            "ledger": ledger,
            "@context": ctx,
            "@graph": [
                {"@id": "ex:rule", "f:rule": {"@type": "@json", "@value": rule}},
                {"@id": "ex:alice", "ex:name": "Alice", "ex:salary": 250_000},
                {"@id": "ex:bob", "ex:name": "Bob", "ex:salary": 90000},
                {"@id": "ex:carol", "ex:salary": 300_000}
            ]
        }),
    )
    .await;
    assert!(status.is_success(), "{status} {body}");
    let (status, body) = post(
        &app,
        "/v1/fluree/reasoning-indexes/register",
        json!({"ledger": ledger, "program": {"reasoning": "datalog"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = post(
        &app,
        "/v1/fluree/reasoning-indexes/refresh",
        json!({"ledger": ledger}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let query = json!({
        "@context": ctx,
        "from": ledger,
        "select": "?n",
        "where": {"@id": "?s", "ex:name": "?n"},
        "reasoning": "datalog",
        "reasoningBudget": {"maxFacts": 1},
        "reasoningFreshness": "exact",
        "opts": {
            "meta": true,
            "default-allow": true,
            "policy": [{
                "@id": "ex:denyHighEarner",
                "f:required": true,
                "f:onClass": [{"@id": "http://example.org/HighEarner"}],
                "f:action": "f:view",
                "f:allow": false
            }]
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .expect("request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let reasoning = resp
        .headers()
        .get("x-fdb-reasoning")
        .map(|v| v.to_str().expect("header").to_string());
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    let body: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.get("result").unwrap_or(&body);
    assert_eq!(rows, &json!(["Bob"]), "{body}");
    let reasoning = reasoning.expect("x-fdb-reasoning");
    assert!(reasoning.contains("indexId"), "{reasoning}");
}
