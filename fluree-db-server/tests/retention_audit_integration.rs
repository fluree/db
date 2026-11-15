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

    // The removal journal is audit data too: a reader sees how many.
    let retention = format!("/v1/fluree/retention/{ledger}");
    let (status, audited) = call(&app, "GET", &retention, Some(&auditor), None).await;
    assert_eq!(status, StatusCode::OK, "{audited}");
    let removals = audited["registration"]["journal"]["removals"]
        .as_array()
        .map_or(0, Vec::len);
    assert!(removals > 0, "{audited}");
    let (status, body) = call(&app, "GET", &retention, Some(&reader), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["removals"], removals, "{body}");
    assert!(
        body["registration"]["journal"]["removals"].is_null(),
        "{body}"
    );
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

/// A server that asks no token of its readers still keeps audit data for an
/// auditor, and reads the audit grant of a token it would otherwise ignore:
/// the status route as the raw commit routes do.
#[tokio::test]
async fn a_server_without_data_auth_reads_an_audit_token() {
    let tmp = tempfile::tempdir().unwrap();
    let key = SigningKey::from_bytes(&[12u8; 32]);
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_trusted_issuers: vec![fluree_db_credential::did_from_pubkey(
            &key.verifying_key().to_bytes(),
        )],
        ..Default::default()
    };
    assert_eq!(
        cfg.data_auth_mode,
        fluree_db_server::config::DataAuthMode::None
    );
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = std::sync::Arc::new(AppState::new(cfg, telemetry).await.unwrap());
    let app = build_router(state);
    let ledger = "open:main";
    let reader = token(&key, ledger, false);
    let auditor = token(&key, ledger, true);
    let stranger = token(&SigningKey::from_bytes(&[13u8; 32]), ledger, true);

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
        None,
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "insert": {"@id": "ex:alice", "ex:email": "a@x"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
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

    let retention = format!("/v1/fluree/retention/{ledger}");
    let journal = |body: &JsonValue| body["registration"]["journal"]["removals"].clone();
    let (status, audited) = call(&app, "GET", &retention, Some(&auditor), None).await;
    assert_eq!(status, StatusCode::OK, "{audited}");
    assert!(
        journal(&audited).as_array().is_some_and(|r| !r.is_empty()),
        "{audited}"
    );
    for (who, bearer) in [
        ("no token", None),
        ("a reader", Some(&reader)),
        ("an untrusted issuer", Some(&stranger)),
    ] {
        let (status, body) = call(&app, "GET", &retention, bearer.map(String::as_str), None).await;
        assert_eq!(status, StatusCode::OK, "{who}: {body}");
        assert!(journal(&body).is_null(), "{who}: {body}");
        assert_eq!(body["removals"], audited["removals"], "{who}: {body}");
    }

    let show = format!("/v1/fluree/show/{ledger}?commit=t:1");
    let (status, body) = call(&app, "GET", &show, None, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = call(&app, "GET", &show, Some(&auditor), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn erasure_needs_a_prover_and_file_storage() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = std::sync::Arc::new(AppState::new(cfg, telemetry).await.unwrap());
    let app = build_router(state);
    let ledger = "erase:main";
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
        None,
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "insert": {"@id": "ex:alice", "ex:email": "a@x"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // No prover configured: refused, and nothing removed.
    let (status, body) = call(
        &app,
        "POST",
        &format!("/v1/fluree/erase/{ledger}"),
        None,
        Some(json!({"matching": [{"subject": "http://example.org/alice",
                                  "predicate": "http://example.org/email"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    let (status, results) = call(
        &app,
        "POST",
        &format!("/v1/fluree/query/{ledger}"),
        None,
        Some(
            json!({"@context": {"ex": "http://example.org/"}, "select": "?o",
                    "where": {"@id": "ex:alice", "ex:email": "?o"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(results, json!(["a@x"]));

    // A prover is configured only over a single node's own file storage.
    let prover = Some(std::path::PathBuf::from("/usr/local/bin/fluree-prover"));
    let memory = ServerConfig {
        memory: true,
        storage_path: None,
        erasure_prover: prover.clone(),
        ..Default::default()
    };
    assert!(memory.validate().is_err());
    let file = ServerConfig {
        storage_path: Some(tmp.path().to_path_buf()),
        erasure_prover: prover,
        ..Default::default()
    };
    assert!(file.validate().is_ok(), "{:?}", file.validate());
}

/// A cluster replica that has not installed the erasures it applied hands a
/// ledger's raw history to no one, an auditor included.
#[tokio::test]
async fn a_replica_short_of_its_erasures_serves_no_raw_history() {
    struct NotInstalled;
    #[async_trait::async_trait]
    impl fluree_db_api::retention::replicas::ReplicaSync for NotInstalled {
        async fn sync_reads(&self) -> Result<(), String> {
            Ok(())
        }
        async fn await_replicas(&self, _: std::time::Duration) -> Vec<String> {
            Vec::new()
        }
        async fn erasures_installed(&self, ledger_id: &str) -> Result<(), String> {
            Err(format!(
                "this node has not yet installed an erasure activated on {ledger_id}"
            ))
        }
    }
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
    let app = build_router(state.clone());
    let ledger = "replica:main";
    let key = SigningKey::from_bytes(&[12u8; 32]);
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
        Some(&auditor),
        Some(json!({"@context": {"ex": "http://example.org/"},
                    "insert": {"@id": "ex:alice", "ex:name": "Alice"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let show = format!("/v1/fluree/show/{ledger}?commit=t:1");
    let (status, body) = call(&app, "GET", &show, Some(&auditor), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    state
        .fluree
        .set_retention_replicas(std::sync::Arc::new(NotInstalled));
    let (status, body) = call(&app, "GET", &show, Some(&auditor), None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}
