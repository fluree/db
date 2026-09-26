//! Ledger scopes name branches, and every spelling of an id authorizes alike.
//!
//! A token scope `mydb` means `mydb:main` (there is no whole-ledger scope),
//! routes authorize the branch they actually read, and a scope check covers
//! every ledger the request will touch — not a stand-in like the header.

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn data_auth_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: fluree_db_server::config::DataAuthMode::Required,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &JsonValue, signing_key: &SigningKey) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);
    let header = json!({
        "alg": "EdDSA",
        "jwk": { "kty": "OKP", "crv": "Ed25519", "x": pubkey_b64 }
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

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

async fn create_ledger(app: &axum::Router, ledger: &str) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"ledger":"{ledger}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create {ledger}");
}

async fn insert_one(app: &axum::Router, ledger: &str, id: &str, name: &str, token: &str) {
    let body = json!({
        "@context": { "ex": "http://example.org/" },
        "insert":   { "@id": id, "ex:name": name }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

fn write_scoped_token(ledgers: &[&str], secret_seed: u8) -> String {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers":  ledgers,
        "fluree.ledger.write.ledgers": ledgers
    });
    create_jws(&claims, &signing_key)
}

fn read_scoped_token(ledgers: &[&str], secret_seed: u8) -> String {
    let signing_key = SigningKey::from_bytes(&[secret_seed; 32]);
    let claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers": ledgers
    });
    create_jws(&claims, &signing_key)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: &str,
    headers: &[(&str, &str)],
    content_type: &str,
    body: String,
) -> (StatusCode, JsonValue) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", content_type);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    json_body(resp).await
}

async fn sparql_query(app: &axum::Router, ledger: &str, token: &str) -> StatusCode {
    send(
        app,
        "POST",
        &format!("/v1/fluree/query/{ledger}"),
        token,
        &[],
        "application/sparql-query",
        "SELECT ?s WHERE { ?s ?p ?o }".to_string(),
    )
    .await
    .0
}

/// `fluree-ledger: allowed:main@t:1` routes explain through the dataset path,
/// which explains against FROM. Authorizing only the header let a token scoped
/// to `allowed` explain `FROM <secret:main>`.
#[tokio::test]
async fn explain_authorizes_every_from_ledger_not_just_the_header() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "allowed").await;
    create_ledger(&app, "secret").await;
    let writer = write_scoped_token(&["allowed", "secret"], 51);
    insert_one(&app, "allowed:main", "ex:a", "A", &writer).await;
    insert_one(&app, "secret:main", "ex:s", "S", &writer).await;
    let reader = read_scoped_token(&["allowed"], 52);

    let explain = |from: &'static str| {
        let app = app.clone();
        let reader = reader.clone();
        async move {
            send(
                &app,
                "POST",
                "/v1/fluree/explain",
                &reader,
                &[("fluree-ledger", "allowed:main@t:1")],
                "application/sparql-query",
                format!("SELECT ?s FROM <{from}> WHERE {{ ?s ?p ?o }}"),
            )
            .await
            .0
        }
    };
    assert_eq!(explain("allowed:main@t:1").await, StatusCode::OK, "control");
    assert_eq!(explain("secret:main@t:1").await, StatusCode::NOT_FOUND);
}

/// A token scoped to `mydb:main` sees only that branch in the listing and
/// cannot preview another branch through the bare-name path.
#[tokio::test]
async fn branch_routes_authorize_the_branch_they_read() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "mydb").await;
    let writer = write_scoped_token(&["mydb"], 61);
    insert_one(&app, "mydb", "ex:a", "A", &writer).await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/branch")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ledger":"mydb","branch":"secret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create branch: {}",
        resp.status()
    );
    let main_only = read_scoped_token(&["mydb"], 62);

    let (status, listed) = send(
        &app,
        "GET",
        "/v1/fluree/branch/mydb",
        &main_only,
        &[],
        "application/json",
        String::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let branches: Vec<&str> = listed
        .as_array()
        .expect("array")
        .iter()
        .map(|b| b["branch"].as_str().unwrap())
        .collect();
    assert_eq!(branches, ["main"], "listing must hide unreadable branches");

    let get = |uri: &'static str| {
        let app = app.clone();
        let token = main_only.clone();
        async move {
            send(
                &app,
                "GET",
                uri,
                &token,
                &[],
                "application/json",
                String::new(),
            )
            .await
            .0
        }
    };
    assert_eq!(
        get("/v1/fluree/merge-preview/mydb?source=secret&target=main").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get("/v1/fluree/revert-preview/mydb?branch=secret").await,
        StatusCode::NOT_FOUND
    );
    // Control: the readable branch passes authorization and fails only on the
    // missing commit selector.
    assert_eq!(
        get("/v1/fluree/revert-preview/mydb?branch=main").await,
        StatusCode::BAD_REQUEST
    );
}

/// Every spelling of one ledger authorizes the same way, in the token and in
/// the request; a scope for another branch authorizes neither.
#[tokio::test]
async fn scope_checks_agree_across_spellings() {
    let (_tmp, state) = data_auth_state().await;
    let app = build_router(state);
    create_ledger(&app, "spell").await;
    let writer = write_scoped_token(&["spell:main"], 71);
    insert_one(&app, "spell", "ex:a", "A", &writer).await;

    for (scope, seed) in [("spell", 72u8), ("spell:main", 73)] {
        let token = read_scoped_token(&[scope], seed);
        for path in ["spell", "spell:main"] {
            assert_eq!(
                sparql_query(&app, path, &token).await,
                StatusCode::OK,
                "scope {scope} must authorize /query/{path}"
            );
        }
    }
    let other_branch = read_scoped_token(&["spell:dev"], 74);
    assert_eq!(
        sparql_query(&app, "spell", &other_branch).await,
        StatusCode::NOT_FOUND
    );
}
