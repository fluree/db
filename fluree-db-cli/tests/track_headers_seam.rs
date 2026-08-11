//! CLI → server round trip for the `fluree-track-*` request headers.
//!
//! The CLI builds these headers for `--track-policy` / `--track-fuel` /
//! `--track-time`; the server parses them in `FlureeHeaders`. Nothing tested
//! the two halves together, and they drifted: `fluree-track-policy` was emitted
//! by the CLI, documented as supported, and never parsed — so the flag worked
//! embedded and silently produced nothing over HTTP.
//!
//! These tests drive the real server router in-process with the exact headers
//! `TrackingFlags::as_request_headers` produces, so a rename or a dropped
//! branch on either side fails here.
//!
//! **When this file runs.** `server` is a default feature of this crate, so a
//! plain `cargo test -p fluree-db-cli` compiles and runs it, as does CI's
//! `--all-features`. It vanishes — as a silently empty test binary, since a
//! crate-level `cfg` leaves nothing to report — only under
//! `--no-default-features` or a feature set that drops `server`. The whole
//! point of the file is to catch drift between two crates, and a green run
//! that never compiled it is not evidence the contract holds: if you are
//! testing with a narrowed feature set, check that these test names appear in
//! the output before concluding the seam is intact.

#![cfg(feature = "server")]

use axum::body::Body;
use axum::Router;
use fluree_db_cli::commands::query::TrackingFlags;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "cli/track:main";

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

async fn seeded_server() -> (TempDir, Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    let app = build_router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": LEDGER }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    // A policy plus an identity the policy applies to, so a tracked query has
    // a policy to execute and report.
    let seed = serde_json::json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "insert": [
            {"@id": "ex:doc1", "@type": "ex:Document", "ex:classification": "public"},
            {"@id": "ex:doc2", "@type": "ex:Document", "ex:classification": "secret"},
            {
                "@id": "ex:public-policy",
                "@type": ["f:AccessPolicy", "ex:PublicClass"],
                "f:action": [{"@id": "f:view"}],
                "f:query": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [{"@id": "?$this", "ex:classification": "public"}]
                    }
                }
            },
            {
                "@id": "http://example.org/public-user",
                "f:policyClass": [{"@id": "ex:PublicClass"}]
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{LEDGER}"))
                .header("content-type", "application/json")
                .body(Body::from(seed.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "seed ledger");

    (tmp, app)
}

/// Issue the query the CLI would issue, carrying the headers the CLI builds
/// for `flags`, and return the server's response.
async fn query_with_cli_headers(
    app: &Router,
    flags: TrackingFlags,
    identity: Option<&str>,
) -> (StatusCode, JsonValue) {
    let body = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?doc", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{LEDGER}"))
        .header("content-type", "application/json");

    for (name, value) in flags.as_request_headers() {
        req = req.header(name, value);
    }
    if let Some(id) = identity {
        req = req.header("fluree-identity", id);
    }

    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    json_body(resp).await
}

/// `--track-policy` alone: the server must treat the request as tracked and
/// return the policy tally, not a bare result array.
#[tokio::test]
async fn cli_track_policy_header_returns_policy_tally() {
    let (_tmp, app) = seeded_server().await;

    let flags = TrackingFlags {
        track_policy: true,
        ..Default::default()
    };
    // Sanity-check the producer half in the same test, so a rename on the CLI
    // side is attributable rather than showing up as a mystery server failure.
    assert!(
        flags
            .as_request_headers()
            .iter()
            .any(|(n, v)| *n == "fluree-track-policy" && v == "true"),
        "CLI must emit fluree-track-policy for --track-policy"
    );

    let (status, json) =
        query_with_cli_headers(&app, flags, Some("http://example.org/public-user")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        json.get("policy").is_some(),
        "tracked response must carry the policy tally, got: {json}"
    );
    assert_eq!(
        json.pointer("/policy_enforcement/enforced"),
        Some(&JsonValue::Bool(true)),
        "an identity-carrying request runs enforced, got: {json}"
    );
    assert!(
        json.get("fuel").is_none() && json.get("time").is_none(),
        "--track-policy must not turn on fuel or time, got: {json}"
    );
}

/// `--track` collapses to the omnibus header, which must still carry policy.
#[tokio::test]
async fn cli_track_header_returns_policy_tally() {
    let (_tmp, app) = seeded_server().await;

    let flags = TrackingFlags {
        track: true,
        ..Default::default()
    };
    let (status, json) =
        query_with_cli_headers(&app, flags, Some("http://example.org/public-user")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        json.get("policy").is_some() && json.get("fuel").is_some() && json.get("time").is_some(),
        "--track asks for every metric, got: {json}"
    );
}

/// No tracking flags: no tracking headers, and the response stays the plain
/// result array it has always been.
#[tokio::test]
async fn no_tracking_flags_leave_the_response_untracked() {
    let (_tmp, app) = seeded_server().await;

    let flags = TrackingFlags::default();
    assert!(flags.as_request_headers().is_empty());

    let (status, json) =
        query_with_cli_headers(&app, flags, Some("http://example.org/public-user")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        json.is_array(),
        "untracked query returns bare results, got: {json}"
    );
}
