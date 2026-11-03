//! `fluree login` / `logout` / `auth status` against an in-process account
//! service that speaks the contract in `docs/design/auth-contract.md`:
//! discovery with an `account` object (and no Fluree data API), the device
//! authorization grant, and the token exchange whose refresh token keeps the
//! sign-in alive.

use assert_cmd::cargo_bin_cmd;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use base64::Engine;
use predicates::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Seen {
    device_name: Option<String>,
    polls: u32,
    exchanges: u32,
    refreshes: u32,
    disconnects: Vec<String>,
    /// Signed out server-side already (e.g. from the dashboard).
    revoked: bool,
    /// `Authorization` of each call to `/v1/whoami`.
    whoami: Vec<String>,
    /// How many `/v1/whoami` calls answer 401 `config_stale` first.
    stale: u32,
}

#[derive(Clone)]
struct Fake {
    base: String,
    seen: Arc<Mutex<Seen>>,
}

/// An unsigned JWT with `exp` = now + `secs` (the CLI only reads claims).
fn jwt(secs: i64, tag: &str) -> String {
    let b64 = |v: Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string());
    let exp = chrono::Utc::now().timestamp() + secs;
    format!(
        "{}.{}.sig",
        b64(json!({ "alg": "RS256" })),
        b64(json!({ "sub": tag, "exp": exp }))
    )
}

fn oauth_error(code: &str) -> (axum::http::StatusCode, Json<Value>) {
    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(json!({ "error": code, "error_description": code })),
    )
}

/// Fronts like fluree.ai's WAF refuse requests without a User-Agent; insist
/// on the CLI's own.
async fn discovery(
    State(f): State<Fake>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, axum::http::StatusCode> {
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !ua.starts_with("fluree/") {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    Ok(Json(json!({
        "version": 1,
        "service": "fluree.ai",
        "account": {
            "api_base_url": format!("{}/v1", f.base),
            "dashboard_url": f.base,
        },
        "auth": {
            "type": "oidc_device",
            "issuer": f.base,
            "client_id": "fluree-cli",
            "exchange_url": format!("{}/v1/oauth/exchange", f.base),
            "scopes": ["openid"],
        },
    })))
}

async fn openid(State(f): State<Fake>) -> Json<Value> {
    Json(json!({
        "issuer": f.base,
        "device_authorization_endpoint": format!("{}/v1/oauth/device", f.base),
        "token_endpoint": format!("{}/v1/oauth/token", f.base),
    }))
}

async fn device(State(f): State<Fake>, Form(form): Form<HashMap<String, String>>) -> Json<Value> {
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("fluree-cli")
    );
    f.seen.lock().unwrap().device_name = form.get("device_name").cloned();
    Json(json!({
        "device_code": "dc-1",
        "user_code": "ACDE-2467",
        "verification_uri": format!("{}/activate", f.base),
        "verification_uri_complete": format!("{}/activate?user_code=ACDE-2467", f.base),
        "expires_in": 600,
        "interval": 1,
    }))
}

async fn token(
    State(f): State<Fake>,
    Form(form): Form<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    assert_eq!(form.get("device_code").map(String::as_str), Some("dc-1"));
    let mut seen = f.seen.lock().unwrap();
    seen.polls += 1;
    if seen.polls == 1 {
        return oauth_error("authorization_pending").into_response();
    }
    Json(json!({ "access_token": "grant-1", "token_type": "Bearer", "expires_in": 300 }))
        .into_response()
}

async fn exchange(State(f): State<Fake>, Json(body): Json<Value>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let account = json!({ "account_id": "acct-1", "org_name": "Acme", "email": "owner@acme.test", "tier": "free" });
    let mut seen = f.seen.lock().unwrap();
    match body["grant_type"].as_str() {
        Some("urn:ietf:params:oauth:grant-type:token-exchange") => {
            assert_eq!(body["subject_token"], "grant-1");
            seen.exchanges += 1;
            Json(json!({
                "access_token": jwt(3600, "first"),
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": format!("flk_key{}_secret", seen.exchanges),
                "deployment_id": format!("dep-{}", seen.exchanges),
                "account": account,
            }))
            .into_response()
        }
        Some("refresh_token") => {
            assert!(body["refresh_token"].as_str().unwrap().starts_with("flk_"));
            seen.refreshes += 1;
            Json(json!({ "access_token": jwt(3600, "refreshed"), "token_type": "Bearer", "expires_in": 3600, "account": account }))
                .into_response()
        }
        _ => oauth_error("unsupported_grant_type").into_response(),
    }
}

async fn disconnect(
    State(f): State<Fake>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let mut seen = f.seen.lock().unwrap();
    if seen.revoked {
        // fluree.ai's answer for a deployment that is already disconnected.
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": { "message": "Deployment is already disconnected", "status": 400, "error": "invalid_state" } })),
        )
            .into_response();
    }
    seen.disconnects.push(auth);
    Json(json!({ "status": "disconnected" })).into_response()
}

/// A call made with the account's token: 401 `config_stale` while `stale`
/// lasts, as fluree.ai answers after the account's configuration changed.
async fn whoami(State(f): State<Fake>, headers: axum::http::HeaderMap) -> axum::response::Response {
    use axum::response::IntoResponse;
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let mut seen = f.seen.lock().unwrap();
    seen.whoami.push(auth);
    if seen.stale > 0 {
        seen.stale -= 1;
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({ "error": { "message": "Configuration updated, please refresh token", "status": 401, "error": "config_stale" } })),
        )
            .into_response();
    }
    Json(json!({ "ok": true })).into_response()
}

async fn start_fake() -> Fake {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let fake = Fake {
        base,
        seen: Arc::default(),
    };
    let app = Router::new()
        .route("/.well-known/fluree.json", get(discovery))
        .route("/.well-known/openid-configuration", get(openid))
        .route("/v1/oauth/device", post(device))
        .route("/v1/oauth/token", post(token))
        .route("/v1/oauth/exchange", post(exchange))
        .route("/v1/auth/disconnect", post(disconnect))
        .route("/v1/whoami", get(whoami))
        .with_state(fake.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    fake
}

/// Run `fluree <args>` with `home` as FLUREE_HOME and cwd (no project).
async fn fluree(home: &std::path::Path, args: &[&str]) -> assert_cmd::assert::Assert {
    let home = home.to_path_buf();
    let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    tokio::task::spawn_blocking(move || {
        cargo_bin_cmd!("fluree")
            .current_dir(&home)
            .env("FLUREE_HOME", &home)
            .env("NO_COLOR", "1")
            .args(&args)
            .assert()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn login_status_logout_without_a_project() {
    let fake = start_fake().await;
    let home = tempfile::tempdir().unwrap();

    fluree(home.path(), &["login", "--url", &fake.base, "--no-browser"])
        .await
        .success()
        .stdout(predicate::str::contains(
            "Signed in to fluree.ai as owner@acme.test (Acme)",
        ))
        .stderr(predicate::str::contains("ACDE-2467"));

    {
        let seen = fake.seen.lock().unwrap();
        assert_eq!(seen.polls, 2, "one pending answer, then the grant");
        assert_eq!(seen.exchanges, 1);
        assert!(seen.device_name.as_deref().is_some_and(|n| !n.is_empty()));
    }

    let text = std::fs::read_to_string(home.path().join("account.toml")).unwrap();
    assert!(text.contains("refresh_token = \"flk_key1_secret\""));
    assert!(text.contains(&format!("api_base_url = \"{}/v1\"", fake.base)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(home.path().join("account.toml"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    // Nothing lands in a project: there is none, and none was created.
    assert!(!home.path().join(".fluree").exists());

    fluree(home.path(), &["auth", "status"])
        .await
        .success()
        .stdout(predicate::str::contains(
            "Signed in to fluree.ai as owner@acme.test (Acme)",
        ))
        .stdout(predicate::str::contains("Tier: free"))
        .stdout(predicate::str::contains("Access token: valid until"));

    // Signing in again replaces the sign-in and signs the old one out:
    // the old one, by its own token, not the new one.
    let first = fluree_db_cli::account::load_from(&home.path().join("account.toml"))
        .unwrap()
        .unwrap();
    fluree(home.path(), &["login", "--url", &fake.base, "--no-browser"])
        .await
        .success();
    let second = fluree_db_cli::account::load_from(&home.path().join("account.toml"))
        .unwrap()
        .unwrap();
    {
        let seen = fake.seen.lock().unwrap();
        assert_eq!(seen.exchanges, 2);
        assert_eq!(
            seen.disconnects,
            vec![format!("Bearer {}", first.access_token)]
        );
        assert_ne!(first.access_token, second.access_token);
    }
    assert!(std::fs::read_to_string(home.path().join("account.toml"))
        .unwrap()
        .contains("flk_key2_secret"));

    fluree(home.path(), &["logout"])
        .await
        .success()
        .stdout(predicate::str::contains("Signed out of fluree.ai"));
    assert_eq!(fake.seen.lock().unwrap().disconnects.len(), 2);
    assert!(!home.path().join("account.toml").exists());

    fluree(home.path(), &["auth", "status"])
        .await
        .success()
        .stdout(predicate::str::contains("Not signed in"));
    fluree(home.path(), &["logout"])
        .await
        .success()
        .stdout(predicate::str::contains("Not signed in"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_account_service_is_not_a_remote() {
    let fake = start_fake().await;
    let home = tempfile::tempdir().unwrap();
    fluree(home.path(), &["init"]).await.success();

    fluree(home.path(), &["remote", "add", "acct", &fake.base])
        .await
        .failure()
        .stderr(predicate::str::contains("is an account service"))
        .stderr(predicate::str::contains(format!(
            "fluree login --url {}",
            fake.base
        )));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_renews_an_expired_access_token() {
    let fake = start_fake().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.toml");
    let mut stored = fluree_db_cli::account::StoredAccount {
        url: fake.base.clone(),
        service: Some("fluree.ai".into()),
        api_base_url: format!("{}/v1", fake.base),
        dashboard_url: None,
        exchange_url: format!("{}/v1/oauth/exchange", fake.base),
        access_token: jwt(-10, "stale"),
        refresh_token: "flk_key1_secret".into(),
        deployment_id: None,
        account: None,
    };
    fluree_db_cli::account::save_to(&path, &stored).unwrap();

    let http = reqwest::Client::new();
    fluree_db_cli::account::refresh(&http, &mut stored)
        .await
        .unwrap();
    assert_eq!(fake.seen.lock().unwrap().refreshes, 1);
    assert!(fluree_db_cli::account::token_seconds_left(&stored.access_token).unwrap() > 3000);
    // The refresh token is kept when the server doesn't rotate it.
    assert_eq!(stored.refresh_token, "flk_key1_secret");
    assert_eq!(
        stored.account.as_ref().and_then(|a| a.email.as_deref()),
        Some("owner@acme.test")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn logout_after_the_server_already_signed_the_machine_out() {
    let fake = start_fake().await;
    let home = tempfile::tempdir().unwrap();
    fluree(home.path(), &["login", "--url", &fake.base, "--no-browser"])
        .await
        .success();

    // Disconnected from the dashboard in the meantime.
    fake.seen.lock().unwrap().revoked = true;

    fluree(home.path(), &["logout"])
        .await
        .success()
        .stdout(predicate::str::contains(
            "the server had already signed this machine out",
        ))
        .stderr(predicate::str::contains("did not confirm").not());
    assert!(!home.path().join("account.toml").exists());
}

/// A stored sign-in at `path` whose access token has `secs` left.
fn stored_at(fake: &Fake, path: &std::path::Path, secs: i64) {
    let stored = fluree_db_cli::account::StoredAccount {
        url: fake.base.clone(),
        service: Some("fluree.ai".into()),
        api_base_url: format!("{}/v1", fake.base),
        dashboard_url: None,
        exchange_url: format!("{}/v1/oauth/exchange", fake.base),
        access_token: jwt(secs, "stored"),
        refresh_token: "flk_key1_secret".into(),
        deployment_id: None,
        account: None,
    };
    fluree_db_cli::account::save_to(path, &stored).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_sign_in_can_still_be_removed() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("account.toml"), "not = [toml").unwrap();

    // Status reports it instead of failing.
    fluree(home.path(), &["auth", "status"])
        .await
        .success()
        .stdout(predicate::str::contains("is unreadable"))
        .stdout(predicate::str::contains("fluree login"));
    // Logout removes it, and says the server wasn't told.
    fluree(home.path(), &["logout"])
        .await
        .success()
        .stdout(predicate::str::contains("Removed"))
        .stderr(predicate::str::contains(
            "disconnect it from your account's dashboard",
        ));
    assert!(!home.path().join("account.toml").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_token_account_prints_the_sign_ins_token_and_refresh_forces_one() {
    let fake = start_fake().await;
    let home = tempfile::tempdir().unwrap();
    stored_at(&fake, &home.path().join("account.toml"), 3600);

    let out = fluree(home.path(), &["auth", "token", "--account"])
        .await
        .success();
    let printed = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert_eq!(printed.trim().split('.').count(), 3, "{printed}");
    assert_eq!(
        fake.seen.lock().unwrap().refreshes,
        0,
        "time left: no refresh"
    );

    let out = fluree(home.path(), &["auth", "token", "--account", "--refresh"])
        .await
        .success();
    let refreshed = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert_eq!(fake.seen.lock().unwrap().refreshes, 1);
    assert_ne!(printed, refreshed);
    assert!(std::fs::read_to_string(home.path().join("account.toml"))
        .unwrap()
        .contains(refreshed.trim()));

    fluree(home.path(), &["auth", "token", "--refresh"])
        .await
        .failure()
        .stderr(predicate::str::contains("--account"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_config_stale_answer_refreshes_and_sends_once_more() {
    let fake = start_fake().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.toml");
    stored_at(&fake, &path, 3600);
    fake.seen.lock().unwrap().stale = 1;

    let http = reqwest::Client::new();
    let url = format!("{}/v1/whoami", fake.base);
    let resp = fluree_db_cli::account::send_at(&http, &path, |s| {
        http.get(&url).bearer_auth(&s.access_token)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(resp.status().is_success());
    {
        let seen = fake.seen.lock().unwrap();
        assert_eq!(seen.refreshes, 1);
        assert_eq!(seen.whoami.len(), 2);
        assert_ne!(
            seen.whoami[0], seen.whoami[1],
            "sent again with the new token"
        );
    }

    // Stale again after the refresh: not retried a second time.
    fake.seen.lock().unwrap().stale = 2;
    let resp = fluree_db_cli::account::send_at(&http, &path, |s| {
        http.get(&url).bearer_auth(&s.access_token)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(fake.seen.lock().unwrap().whoami.len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_refreshes_at_once_spend_the_refresh_token_once() {
    let fake = start_fake().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.toml");
    stored_at(&fake, &path, -10);

    let http = reqwest::Client::new();
    let (a, b) = tokio::join!(
        fluree_db_cli::account::session_at(&http, &path, false),
        fluree_db_cli::account::session_at(&http, &path, false),
    );
    let (a, b) = (a.unwrap().unwrap(), b.unwrap().unwrap());
    assert_eq!(fake.seen.lock().unwrap().refreshes, 1);
    assert_eq!(a.access_token, b.access_token);
}

#[tokio::test(flavor = "multi_thread")]
async fn login_needs_https_except_to_this_machine() {
    let home = tempfile::tempdir().unwrap();
    fluree(
        home.path(),
        &["login", "--url", "http://fluree.example", "--no-browser"],
    )
    .await
    .failure()
    .stderr(predicate::str::contains("needs https"));
}
