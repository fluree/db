//! Audit-log access on a ledger whose retention suppresses history: once an
//! occurrence is removed, raw commit content needs the
//! `fluree.ledger.audit.*` permission, and a historical read that could see
//! the occurrence is refused with 410.

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use tower::ServiceExt;

use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};

fn jws(claims: &JsonValue, key: &SigningKey) -> String {
    let header = json!({
        "alg": "EdDSA",
        "jwk": {"kty": "OKP", "crv": "Ed25519",
                "x": URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes())}
    });
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let sig = key.sign(input.as_bytes());
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig.to_bytes()))
}

fn token(key: &SigningKey, ledger: &str, audit: bool) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&key.verifying_key().to_bytes()),
        "exp": now + 3600,
        "iat": now,
        "fluree.ledger.read.ledgers": [ledger],
        "fluree.ledger.write.ledgers": [ledger]
    });
    if audit {
        claims["fluree.ledger.audit.ledgers"] = json!([ledger]);
    }
    jws(&claims, key)
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<JsonValue>,
) -> (StatusCode, JsonValue) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(t) = bearer {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(
            req.body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null),
    )
}

#[tokio::test]
async fn removal_makes_raw_commits_audit_data() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: fluree_db_server::config::DataAuthMode::Required,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = std::sync::Arc::new(AppState::new(cfg, telemetry).await.unwrap());
    let app = build_router(state);
    let ledger = "audit:main";
    let key = SigningKey::from_bytes(&[11u8; 32]);
    let reader = token(&key, ledger, false);
    let auditor = token(&key, ledger, true);

    let (status, _) = call(
        &app,
        "POST",
        "/v1/fluree/create",
        None,
        Some(json!({"ledger": ledger})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        &app,
        "POST",
        &format!("/v1/fluree/insert/{ledger}"),
        Some(&reader),
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "insert": {"@id": "ex:alice", "ex:email": "a@x", "ex:name": "Alice"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let show = format!("/v1/fluree/show/{ledger}?commit=t:1");
    let (status, _) = call(&app, "GET", &show, Some(&reader), None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a ledger without suppression is unaffected"
    );

    let (status, receipt) = call(
        &app,
        "POST",
        &format!("/v1/fluree/remove/{ledger}"),
        None,
        Some(json!({"matching": [{"subject": "http://example.org/alice",
                                  "predicate": "http://example.org/email"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert!(receipt["retractedT"].is_number(), "{receipt}");

    let (status, body) = call(&app, "GET", &show, Some(&reader), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, _) = call(&app, "GET", &show, Some(&auditor), None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the audit permission reads the original commit"
    );

    let (status, body) = call(
        &app,
        "POST",
        "/v1/fluree/query",
        Some(&reader),
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "from": format!("{ledger}@t:1"),
                    "select": "?e",
                    "where": {"@id": "ex:alice", "ex:email": "?e"}})),
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    let (status, body) = call(
        &app,
        "POST",
        "/v1/fluree/query",
        Some(&reader),
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "from": ledger,
                    "select": "?n",
                    "where": {"@id": "ex:alice", "ex:name": "?n"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
