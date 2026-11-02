//! Google identity tests, every exchange against a wiremock Google.
//!
//! Mock servers are pooled and reuse their URLs, so each `GoogleEndpoints::at`
//! gets a token-cache partition of its own: tests never see each other's
//! cached tokens, and two identities share one only when built from the same
//! endpoints value.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::auth::AuthConfig;
use crate::config_value::ConfigValue;
use crate::host_identity::{HostIdentityPolicy, HOST_IDENTITIES_ENV};

/// A BigLake catalog request: where a host identity's token may go.
const BIGLAKE: &str = "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config";

fn ctx() -> IdentityContext {
    IdentityContext::default()
}

fn token_body(token: &str, expires_in: i64) -> serde_json::Value {
    serde_json::json!({"access_token": token, "expires_in": expires_in, "token_type": "Bearer"})
}

/// An `application/x-www-form-urlencoded` body, decoded.
fn form(body: &[u8]) -> HashMap<String, String> {
    let query = std::str::from_utf8(body).unwrap();
    reqwest::Url::parse(&format!("http://form.invalid/?{query}"))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

async fn requests_to(server: &MockServer, route: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == route)
        .collect()
}

/// A metadata identity whose server is `server`, in a token-cache partition
/// of its own.
fn metadata_identity(server: &MockServer) -> GoogleIdentity {
    metadata_identity_with(&GoogleEndpoints::at(&server.uri()))
}

fn metadata_identity_with(endpoints: &GoogleEndpoints) -> GoogleIdentity {
    GoogleIdentity::with_endpoints(&GoogleCredentials::Metadata, None, &ctx(), endpoints).unwrap()
}

const METADATA_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";

// ── metadata ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn metadata_sends_the_flavor_header_and_scopes_and_caches() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .and(header("metadata-flavor", "Google"))
        .and(query_param("scopes", CLOUD_PLATFORM_SCOPE))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.md", 3599)))
        .mount(&server)
        .await;

    let endpoints = GoogleEndpoints::at(&server.uri());
    let auth = GoogleCatalogAuth::new(
        Arc::new(metadata_identity_with(&endpoints)),
        vec![CLOUD_PLATFORM_SCOPE.to_string()],
    );
    let headers = SendCatalogAuth::request_headers(&auth, BIGLAKE)
        .await
        .unwrap();
    assert_eq!(headers[AUTHORIZATION], "Bearer ya29.md");
    assert!(headers[AUTHORIZATION].is_sensitive());
    assert!(
        headers.get(QUOTA_PROJECT_HEADER).is_none(),
        "no quota project, no header"
    );

    // A second call, and a second identity with the same fingerprint, reuse
    // the cached token.
    SendCatalogAuth::request_headers(&auth, BIGLAKE)
        .await
        .unwrap();
    metadata_identity_with(&endpoints)
        .access_token(&[CLOUD_PLATFORM_SCOPE])
        .await
        .unwrap();
    assert_eq!(requests_to(&server, METADATA_PATH).await.len(), 1);
}

#[tokio::test]
async fn metadata_errors_carry_the_status_and_never_the_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_string(r#"{"error":"forbidden","detail":"SECRET-BODY-TEXT"}"#),
        )
        .mount(&server)
        .await;
    let err = metadata_identity(&server)
        .access_token(&[])
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            IcebergError::GoogleTokenExchange {
                stage: crate::error::GoogleTokenStage::Metadata,
                status: Some(403),
                reason: None,
            }
        ),
        "{err:?}"
    );
    let message = err.to_string();
    assert!(message.contains("403"), "{message}");
    assert!(!message.contains("SECRET-BODY-TEXT"), "{message}");
}

#[test]
fn a_stored_metadata_url_other_than_the_pinned_one_is_refused() {
    if std::env::var_os(METADATA_URL_ENV).is_some() {
        return; // the runner set the operator override; nothing to assert
    }
    let auth = AuthConfig::GoogleMetadata {
        scopes: None,
        metadata_url: Some("http://169.254.169.254/evil-route".to_string()),
    };
    let err = auth.create_provider_arc(&ctx()).unwrap_err();
    assert!(
        matches!(err, IcebergError::GoogleCredentialsInvalid(_)),
        "{err:?}"
    );
    let message = err.to_string();
    assert!(message.contains(METADATA_URL_ENV), "{message}");
    assert!(!message.contains("evil-route"), "{message}");

    // The pinned URL itself, written out, is accepted.
    let pinned = AuthConfig::GoogleMetadata {
        scopes: None,
        metadata_url: Some(METADATA_TOKEN_URL.to_string()),
    };
    pinned.create_provider_arc(&ctx()).unwrap();
}

#[tokio::test]
async fn a_stored_metadata_url_equal_to_the_operators_is_used() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.op", 3599)))
        .mount(&server)
        .await;
    let endpoints = GoogleEndpoints::at(&server.uri());
    let auth = AuthConfig::GoogleMetadata {
        scopes: None,
        metadata_url: Some(endpoints.metadata_token.clone()),
    }
    .create_provider_arc_with(&ctx(), &endpoints)
    .unwrap();
    let headers = auth.request_headers(BIGLAKE).await.unwrap();
    assert_eq!(headers[AUTHORIZATION], "Bearer ya29.op");
}

// ── single flight and refresh ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_share_one_token_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(200))
                .set_body_json(token_body("ya29.once", 3599)),
        )
        .mount(&server)
        .await;
    let identity = Arc::new(metadata_identity(&server));
    let calls = (0..16).map(|_| {
        let identity = Arc::clone(&identity);
        tokio::spawn(async move { identity.access_token(&[CLOUD_PLATFORM_SCOPE]).await })
    });
    for joined in futures::future::join_all(calls).await {
        assert_eq!(joined.unwrap().unwrap().token, "ya29.once");
    }
    assert_eq!(
        requests_to(&server, METADATA_PATH).await.len(),
        1,
        "sixteen concurrent callers, one exchange"
    );
}

/// Answers each request with the next body in `bodies` (the last repeats).
struct Sequence {
    calls: AtomicUsize,
    responses: Vec<ResponseTemplate>,
}

impl Respond for Sequence {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        self.responses[n.min(self.responses.len() - 1)].clone()
    }
}

#[tokio::test]
async fn a_failed_refresh_reads_on_with_a_token_that_is_still_valid() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(Sequence {
            calls: AtomicUsize::new(0),
            responses: vec![
                // Inside the refresh margin, but not expired.
                ResponseTemplate::new(200).set_body_json(token_body("ya29.held", 20)),
                ResponseTemplate::new(500),
            ],
        })
        .mount(&server)
        .await;
    let identity = metadata_identity(&server);
    assert_eq!(identity.access_token(&[]).await.unwrap().token, "ya29.held");
    // Due for renewal; the renewal fails; the held token still works.
    assert_eq!(identity.access_token(&[]).await.unwrap().token, "ya29.held");
    assert_eq!(requests_to(&server, METADATA_PATH).await.len(), 2);
}

#[tokio::test]
async fn a_failed_refresh_with_an_expired_token_fails() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(Sequence {
            calls: AtomicUsize::new(0),
            responses: vec![
                ResponseTemplate::new(200).set_body_json(token_body("ya29.gone", 0)),
                ResponseTemplate::new(500),
            ],
        })
        .mount(&server)
        .await;
    let identity = metadata_identity(&server);
    identity.access_token(&[]).await.unwrap();
    let err = identity.access_token(&[]).await.unwrap_err();
    assert!(
        matches!(
            err,
            IcebergError::GoogleTokenExchange {
                status: Some(500),
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_refusal_re_mints_a_shared_token_at_most_once_per_window() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(Sequence {
            calls: AtomicUsize::new(0),
            responses: vec![
                ResponseTemplate::new(200).set_body_json(token_body("ya29.first", 3599)),
                ResponseTemplate::new(200).set_body_json(token_body("ya29.second", 3599)),
            ],
        })
        .mount(&server)
        .await;
    let auth = GoogleCatalogAuth::new(Arc::new(metadata_identity(&server)), Vec::new());
    assert_eq!(
        SendCatalogAuth::request_headers(&auth, BIGLAKE)
            .await
            .unwrap()[AUTHORIZATION],
        "Bearer ya29.first"
    );
    // The first refusal re-mints; the rest within 30 s keep that token,
    // however many sources sharing it see a refusal, so a refusal storm cannot
    // drive the shared host token's exchanges. The window passing is pinned in
    // `cache::tests`.
    for _ in 0..5 {
        SendCatalogAuth::refresh(&auth).await.unwrap();
    }
    assert_eq!(
        SendCatalogAuth::request_headers(&auth, BIGLAKE)
            .await
            .unwrap()[AUTHORIZATION],
        "Bearer ya29.second"
    );
    assert_eq!(requests_to(&server, METADATA_PATH).await.len(), 2);
}

#[tokio::test]
async fn a_token_lifetime_past_the_ceiling_is_clamped_not_a_panic() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.long", i64::MAX)))
        .mount(&server)
        .await;
    let token = metadata_identity(&server).access_token(&[]).await.unwrap();
    let ceiling = Utc::now()
        + chrono::Duration::seconds(crate::auth::token::MAX_TOKEN_LIFETIME_SECS)
        + chrono::Duration::seconds(5);
    assert!(token.expires_at <= ceiling, "{:?}", token.expires_at);
}

// ── where a host identity's token may go ───────────────────────────────────

/// A host identity's token goes only to Google's APIs over https; anything
/// else is refused before a token is even minted, naming the host and never
/// the token.
#[tokio::test]
async fn a_host_identitys_token_goes_only_to_google_apis() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.HOST", 3599)))
        .mount(&server)
        .await;
    let auth = GoogleCatalogAuth::new(Arc::new(metadata_identity(&server)), Vec::new());

    for (url, host) in [
        ("https://attacker.example/v1/config", "attacker.example"),
        (
            "http://biglake.googleapis.com/v1/config",
            "biglake.googleapis.com",
        ),
        (
            "https://googleapis.com.attacker.example/",
            "googleapis.com.attacker.example",
        ),
        ("https://attackergoogleapis.com/", "attackergoogleapis.com"),
        (
            "https://biglake.googleapis.com@attacker.example/",
            "attacker.example",
        ),
        ("https://169.254.169.254/", "169.254.169.254"),
        ("not a url", "(not a URL)"),
    ] {
        let err = SendCatalogAuth::request_headers(&auth, url)
            .await
            .unwrap_err();
        match &err {
            IcebergError::GoogleTokenHostRefused {
                kind,
                host: refused,
            } => {
                assert_eq!(*kind, HostIdentityKind::GoogleMetadata, "{url}");
                assert_eq!(refused, host, "{url}");
            }
            other => panic!("{url}: expected a refusal, got {other:?}"),
        }
        let message = err.to_string();
        assert!(message.contains(host), "{message}");
        assert!(message.contains(TOKEN_HOSTS_ENV), "{message}");
        assert!(!message.contains("ya29"), "{message}");
    }
    assert!(
        requests_to(&server, METADATA_PATH).await.is_empty(),
        "a refused destination mints nothing"
    );

    for url in [
        "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config",
        "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries",
        "https://biglake-endpoint.p.googleapis.com/iceberg/v1/restcatalog",
        "https://googleapis.com/x",
        "https://BIGLAKE.GOOGLEAPIS.COM./v1/config",
    ] {
        let headers = SendCatalogAuth::request_headers(&auth, url).await.unwrap();
        assert_eq!(headers[AUTHORIZATION], "Bearer ya29.HOST", "{url}");
    }
}

/// Every host kind is held to it; a source's own identity is not.
#[cfg(feature = "aws")]
#[test]
fn only_host_identities_are_held_to_google_apis() {
    let endpoints = GoogleEndpoints::at("http://127.0.0.1:9");
    let tenant_host = "https://catalog.tenant.example/v1/config";

    let metadata = metadata_identity_with(&endpoints);
    assert_eq!(metadata.host_kind(), Some(HostIdentityKind::GoogleMetadata));
    assert!(metadata.ensure_token_destination(tenant_host).is_err());

    let host_signed =
        GoogleIdentity::with_endpoints(&wif_credentials(), None, &ctx(), &endpoints).unwrap();
    assert_eq!(
        host_signed.host_kind(),
        Some(HostIdentityKind::GoogleHostSignedWif)
    );
    assert!(matches!(
        host_signed.ensure_token_destination(tenant_host),
        Err(IcebergError::GoogleTokenHostRefused {
            kind: HostIdentityKind::GoogleHostSignedWif,
            ..
        })
    ));

    // The tenant's own: a service-account key, WIF signed with an embedder's
    // per-tenant subject. They go wherever the source points.
    let service_account = GoogleIdentity::with_endpoints(
        &sa_credentials("tenant@p.iam.gserviceaccount.com", TEST_PRIVATE_KEY, None),
        None,
        &ctx(),
        &endpoints,
    )
    .unwrap();
    assert_eq!(service_account.host_kind(), None);
    service_account
        .ensure_token_destination(tenant_host)
        .unwrap();
    let embedder_signed = GoogleIdentity::with_endpoints(
        &wif_credentials(),
        None,
        &wif::subject("tenant-a"),
        &endpoints,
    )
    .unwrap();
    assert_eq!(embedder_signed.host_kind(), None);
    embedder_signed
        .ensure_token_destination(tenant_host)
        .unwrap();
}

#[test]
fn the_operator_can_name_more_token_hosts() {
    assert_eq!(
        parse_token_hosts(Some(" 127.0.0.1, *.Corp.Example ,, private.example. ")),
        vec!["127.0.0.1", "*.corp.example", "private.example"]
    );
    assert!(parse_token_hosts(None).is_empty());
    let mut endpoints = GoogleEndpoints::at("http://127.0.0.1:9");
    endpoints.token_hosts = parse_token_hosts(Some("127.0.0.1,*.corp.example"));
    let identity = metadata_identity_with(&endpoints);
    for allowed in [
        "http://127.0.0.1:8080/v1/config",
        "https://catalog.corp.example/v1/config",
        "https://biglake.googleapis.com/v1/config",
    ] {
        identity.ensure_token_destination(allowed).unwrap();
    }
    for refused in [
        "https://corp.example/v1/config",
        "https://evilcorp.example/",
        "http://127.0.0.2/",
    ] {
        assert!(
            identity.ensure_token_destination(refused).is_err(),
            "{refused}"
        );
    }
}

/// End to end through the REST catalog: a host identity pointed at a catalog
/// that is not Google's sends nothing at all; the operator naming the host
/// lets the same catalog through.
#[tokio::test]
async fn a_catalog_off_google_never_receives_a_host_token() {
    use crate::catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(METADATA_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.HOST", 3599)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"namespaces": []})),
        )
        .mount(&server)
        .await;
    let client = |endpoints: &GoogleEndpoints| {
        let auth = AuthConfig::GoogleMetadata {
            scopes: None,
            metadata_url: None,
        }
        .create_provider_arc_with(&ctx(), endpoints)
        .unwrap();
        RestCatalogClient::new(
            RestCatalogConfig {
                uri: server.uri(),
                // A warehouse no other test uses (pooled servers reuse URLs).
                warehouse: Some("host-token-destination".to_string()),
                ..Default::default()
            },
            auth,
        )
        .unwrap()
    };

    let endpoints = GoogleEndpoints::at(&server.uri());
    let err = SendCatalogClient::list_namespaces(&client(&endpoints))
        .await
        .unwrap_err();
    assert!(
        matches!(err, IcebergError::GoogleTokenHostRefused { .. }),
        "{err:?}"
    );
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "nothing reaches a catalog off Google, not even a token request"
    );

    let mut operator = GoogleEndpoints::at(&server.uri());
    operator.token_hosts = parse_token_hosts(Some("127.0.0.1"));
    SendCatalogClient::list_namespaces(&client(&operator))
        .await
        .unwrap();
    let listed = requests_to(&server, "/v1/namespaces").await;
    assert_eq!(listed[0].headers["authorization"], "Bearer ya29.HOST");
}

// ── policy ────────────────────────────────────────────────────────────────

fn refused_kind(err: IcebergError, kind: HostIdentityKind) {
    match &err {
        IcebergError::HostIdentityRefused { kind: refused } => assert_eq!(*refused, kind),
        other => panic!("expected {kind} to be refused, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains(HOST_IDENTITIES_ENV), "{message}");
    assert!(message.contains(kind.as_str()), "{message}");
}

#[test]
fn each_google_host_identity_is_refused_when_the_policy_disallows_it() {
    let closed = IdentityContext {
        policy: HostIdentityPolicy::deny_all(),
        ..IdentityContext::default()
    };
    let endpoints = GoogleEndpoints::at("http://127.0.0.1:9");

    refused_kind(
        GoogleIdentity::with_endpoints(&GoogleCredentials::Metadata, None, &closed, &endpoints)
            .unwrap_err(),
        HostIdentityKind::GoogleMetadata,
    );
    // The legacy record is the same identity, refused the same way.
    refused_kind(
        AuthConfig::GoogleMetadata {
            scopes: None,
            metadata_url: None,
        }
        .create_provider_arc(&closed)
        .unwrap_err(),
        HostIdentityKind::GoogleMetadata,
    );
    // Refused before the ADC file is even looked for.
    refused_kind(
        GoogleIdentity::with_endpoints(
            &GoogleCredentials::ApplicationDefault,
            None,
            &closed,
            &endpoints,
        )
        .unwrap_err(),
        HostIdentityKind::GoogleApplicationDefault,
    );
    #[cfg(feature = "aws")]
    refused_kind(
        GoogleIdentity::with_endpoints(&wif_credentials(), None, &closed, &endpoints).unwrap_err(),
        HostIdentityKind::GoogleHostSignedWif,
    );

    // Allowing one kind allows exactly it.
    let metadata_only = IdentityContext {
        policy: HostIdentityPolicy::parse(Some("google-metadata"), None).unwrap(),
        ..IdentityContext::default()
    };
    GoogleIdentity::with_endpoints(
        &GoogleCredentials::Metadata,
        None,
        &metadata_only,
        &endpoints,
    )
    .unwrap();
    refused_kind(
        GoogleIdentity::with_endpoints(
            &GoogleCredentials::ApplicationDefault,
            None,
            &metadata_only,
            &endpoints,
        )
        .unwrap_err(),
        HostIdentityKind::GoogleApplicationDefault,
    );
}

// ── quota project, scopes, redaction ──────────────────────────────────────

#[test]
fn an_explicit_quota_project_must_be_a_project_id() {
    let err = GoogleIdentity::with_endpoints(
        &GoogleCredentials::Metadata,
        Some("evil\r\nx-injected: 1"),
        &ctx(),
        &GoogleEndpoints::at("http://127.0.0.1:9"),
    )
    .unwrap_err();
    assert!(matches!(err, IcebergError::GoogleCredentialsInvalid(_)));
    assert!(!err.to_string().contains("injected"), "{err}");
}

#[test]
fn scopes_parse_sorted_with_cloud_platform_by_default() {
    assert_eq!(parse_scopes(None).unwrap(), vec![CLOUD_PLATFORM_SCOPE]);
    assert_eq!(
        parse_scopes(Some(" b , a,,a ")).unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(parse_scopes(Some(" , ")).is_err());
    assert!(parse_scopes(Some("a b")).is_err());
}

#[test]
fn access_tokens_and_identities_never_print_a_secret() {
    let token = AccessToken {
        token: "ya29.SECRET".to_string(),
        expires_at: Utc::now(),
    };
    assert!(!format!("{token:?}").contains("SECRET"));

    let identity = metadata_identity_at("http://127.0.0.1:9");
    let auth = GoogleCatalogAuth::new(Arc::new(identity), Vec::new());
    let dbg = format!("{auth:?}");
    assert!(dbg.contains("metadata"), "{dbg}");
}

fn metadata_identity_at(base: &str) -> GoogleIdentity {
    GoogleIdentity::with_endpoints(
        &GoogleCredentials::Metadata,
        None,
        &ctx(),
        &GoogleEndpoints::at(base),
    )
    .unwrap()
}

// ── application default credentials ───────────────────────────────────────

const DEVSTORAGE_READ_ONLY: &str = "https://www.googleapis.com/auth/devstorage.read_only";

/// Google's token endpoint for a user's refresh grant, as observed live: it
/// grants a scope of the user's consent (gcloud's login consents to
/// cloud-platform) and refuses any other, `devstorage.read_only` included,
/// with `400 invalid_scope`.
struct ConsentedScopes;

impl Respond for ConsentedScopes {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        match form(&request.body).get("scope").map(String::as_str) {
            Some(CLOUD_PLATFORM_SCOPE) => {
                ResponseTemplate::new(200).set_body_json(token_body("ya29.user", 3599))
            }
            _ => ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_scope",
                "error_description": "Bad Request"
            })),
        }
    }
}

/// The one test that sets `GOOGLE_APPLICATION_CREDENTIALS` (nothing else in
/// this binary reads it).
#[tokio::test]
async fn application_default_user_credentials_refresh_and_carry_their_quota_project() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ConsentedScopes)
        .mount(&server)
        .await;
    let file = std::env::temp_dir().join(format!("fluree-adc-test-{}.json", std::process::id()));
    let write_with = |client_id: &str, kind: &str, refresh_token: &str| {
        let adc = serde_json::json!({
            "type": kind,
            "client_id": client_id,
            "client_secret": "CLIENT-SECRET",
            "refresh_token": refresh_token,
            "quota_project_id": "user-project",
        });
        std::fs::write(&file, adc.to_string()).unwrap();
    };
    let write = |client_id: &str, kind: &str| write_with(client_id, kind, "REFRESH-SECRET");
    write(
        "gcloud-client.apps.googleusercontent.com",
        "authorized_user",
    );
    std::env::set_var(adc::APPLICATION_CREDENTIALS_ENV, &file);

    let endpoints = GoogleEndpoints::at(&server.uri());
    let identity = GoogleIdentity::with_endpoints(
        &GoogleCredentials::ApplicationDefault,
        None,
        &ctx(),
        &endpoints,
    )
    .unwrap();
    assert_eq!(
        identity.principal(),
        "adc:gcloud-client.apps.googleusercontent.com"
    );
    assert_eq!(identity.source(), GoogleSource::ApplicationDefault);
    // The host's user credentials go to Google's APIs only.
    assert!(matches!(
        identity.ensure_token_destination("https://attacker.example/v1/config"),
        Err(IcebergError::GoogleTokenHostRefused {
            kind: HostIdentityKind::GoogleApplicationDefault,
            ..
        })
    ));
    assert_eq!(identity.quota_project(), Some("user-project"));
    let dbg = format!("{identity:?}");
    assert!(!dbg.contains("SECRET"), "{dbg}");

    let identity = Arc::new(identity);
    let auth = GoogleCatalogAuth::new(Arc::clone(&identity), Vec::new());
    let headers = SendCatalogAuth::request_headers(&auth, BIGLAKE)
        .await
        .unwrap();
    assert_eq!(headers[AUTHORIZATION], "Bearer ya29.user");
    assert_eq!(headers[QUOTA_PROJECT_HEADER], "user-project");
    let sent = form(&requests_to(&server, "/token").await[0].body);
    assert_eq!(sent["grant_type"], "refresh_token");
    assert_eq!(sent["refresh_token"], "REFRESH-SECRET");
    assert_eq!(sent["scope"], CLOUD_PLATFORM_SCOPE);

    // Same identity, cached: no second exchange.
    SendCatalogAuth::request_headers(&auth, BIGLAKE)
        .await
        .unwrap();
    assert_eq!(requests_to(&server, "/token").await.len(), 1);

    // Storage asks for read-only storage, which a user's refresh grant
    // cannot narrow to: the user's cloud-platform token serves it, the one
    // already cached, and a renewal asks for cloud-platform again.
    let storage = identity
        .access_token(&[DEVSTORAGE_READ_ONLY])
        .await
        .unwrap();
    assert_eq!(storage.token, "ya29.user");
    assert_eq!(requests_to(&server, "/token").await.len(), 1);
    identity.refresh(&[DEVSTORAGE_READ_ONLY]).await.unwrap();
    let exchanges = requests_to(&server, "/token").await;
    assert_eq!(exchanges.len(), 2, "the renewal is one exchange");
    assert_eq!(form(&exchanges[1].body)["scope"], CLOUD_PLATFORM_SCOPE);

    // A new login for the same identity (a new refresh token) never serves
    // the token the old one minted: the file's secrets key the cache.
    write_with(
        "gcloud-client.apps.googleusercontent.com",
        "authorized_user",
        "REFRESH-SECRET-ROTATED",
    );
    SendCatalogAuth::request_headers(&auth, BIGLAKE)
        .await
        .unwrap();
    let exchanges = requests_to(&server, "/token").await;
    assert_eq!(exchanges.len(), 3, "the rotated refresh token is exchanged");
    assert_eq!(
        form(&exchanges[2].body)["refresh_token"],
        "REFRESH-SECRET-ROTATED"
    );

    // The file now holds someone else: refused, not followed.
    write(
        "another-client.apps.googleusercontent.com",
        "authorized_user",
    );
    let err = SendCatalogAuth::refresh(&auth).await.unwrap_err();
    assert!(
        matches!(err, IcebergError::GoogleCredentialsInvalid(_)),
        "{err:?}"
    );

    // A type outside the subset is refused when the identity is built.
    write("x", "external_account");
    let err = GoogleIdentity::with_endpoints(
        &GoogleCredentials::ApplicationDefault,
        None,
        &ctx(),
        &endpoints,
    )
    .unwrap_err();
    assert!(err.to_string().contains("not supported"), "{err}");

    std::env::remove_var(adc::APPLICATION_CREDENTIALS_ENV);
    let _ = std::fs::remove_file(&file);
}

#[test]
fn an_adc_file_naming_its_account_uses_it_as_the_principal() {
    let file = |account: serde_json::Value| {
        adc::parse(
            &serde_json::json!({
                "type": "authorized_user",
                "client_id": "gcloud-client.apps.googleusercontent.com",
                "client_secret": "s",
                "refresh_token": "r",
                "account": account,
            })
            .to_string(),
        )
        .unwrap()
    };
    assert_eq!(
        file("analyst@lake.example".into()).principal(),
        "adc:analyst@lake.example"
    );
    // What current gcloud writes (an empty account), or anything that is not
    // an email, falls back to gcloud's client id.
    for unusable in [
        serde_json::json!(""),
        serde_json::json!("not an email"),
        serde_json::json!(5),
    ] {
        assert_eq!(
            file(unusable).principal(),
            "adc:gcloud-client.apps.googleusercontent.com"
        );
    }
}

// ── service accounts ──────────────────────────────────────────────────────

const TEST_PRIVATE_KEY: &str = include_str!("testdata/test_rsa_private.pem");
#[cfg(feature = "aws")]
const TEST_PUBLIC_KEY: &str = include_str!("testdata/test_rsa_public.pem");
#[cfg(feature = "aws")]
const OTHER_PRIVATE_KEY: &str = include_str!("testdata/other_rsa_private.pem");

/// A service-account key file for `email`, built at runtime.
fn sa_key(email: &str, private_key: &str, quota_project: Option<&str>) -> String {
    let mut key = serde_json::json!({
        "type": "service_account",
        "project_id": "lake-project",
        "private_key_id": "kid-1",
        "private_key": private_key,
        "client_email": email,
        "client_id": "1234",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "universe_domain": "googleapis.com",
    });
    if let Some(project) = quota_project {
        key["quota_project_id"] = project.into();
    }
    key.to_string()
}

fn sa_credentials(
    email: &str,
    private_key: &str,
    quota_project: Option<&str>,
) -> GoogleCredentials {
    GoogleCredentials::ServiceAccount {
        key: ConfigValue::literal(sa_key(email, private_key, quota_project)),
    }
}

#[test]
fn service_account_keys_are_validated_field_by_field() {
    let refused = |field: &str, edit: &dyn Fn(&mut serde_json::Value)| {
        let mut key: serde_json::Value = serde_json::from_str(&sa_key(
            "sa@p.iam.gserviceaccount.com",
            TEST_PRIVATE_KEY,
            None,
        ))
        .unwrap();
        edit(&mut key);
        let err =
            service_account::ServiceAccountKey::parse(&key.to_string(), "service_account.key")
                .unwrap_err();
        assert!(
            matches!(err, IcebergError::GoogleCredentialsInvalid(_)),
            "{field}: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains(field), "{field}: {message}");
        assert!(
            !message.contains("evil"),
            "{field} echoed its value: {message}"
        );
        assert!(!message.contains("PRIVATE KEY"), "{field}: {message}");
    };
    refused("type", &|k| k["type"] = "evil".into());
    refused("token_uri", &|k| {
        k["token_uri"] = "https://evil.example/token".into();
    });
    refused("universe_domain", &|k| {
        k["universe_domain"] = "evil.example".into();
    });
    refused("client_email", &|k| {
        k["client_email"] = "evil/x@y.com".into();
    });
    refused("client_email", &|k| {
        k.as_object_mut().unwrap().remove("client_email");
    });
    refused("private_key_id", &|k| k["private_key_id"] = 5.into());
    refused("quota_project_id", &|k| {
        k["quota_project_id"] = "evil project".into();
    });
    #[cfg(feature = "aws")]
    refused("private_key", &|k| {
        k["private_key"] = "-----BEGIN PRIVATE KEY-----\nevil\n-----END PRIVATE KEY-----".into();
    });

    let err =
        service_account::ServiceAccountKey::parse("{\"private_key\": \"x", "service_account.key")
            .unwrap_err();
    assert!(err.to_string().contains("not JSON"), "{err}");
}

#[cfg(not(feature = "aws"))]
#[test]
fn without_signing_a_service_account_is_refused_by_name() {
    let err = GoogleIdentity::with_endpoints(
        &sa_credentials("sa@p.iam.gserviceaccount.com", TEST_PRIVATE_KEY, None),
        None,
        &ctx(),
        &GoogleEndpoints::at("http://127.0.0.1:9"),
    )
    .unwrap_err();
    // The typed credential refusal (err:auth/GoogleCredentialsInvalid), naming
    // why: not a generic error, never a panic.
    assert!(
        matches!(err, IcebergError::GoogleCredentialsInvalid(_)),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("not available in this build"),
        "{err}"
    );
    assert!(err.to_string().contains("`aws` feature"), "{err}");
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn the_service_account_assertion_has_the_right_claims_and_verifies() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.sa", 3600)))
        .mount(&server)
        .await;
    let email = "reader@lake-project.iam.gserviceaccount.com";
    let identity = GoogleIdentity::with_endpoints(
        &sa_credentials(email, TEST_PRIVATE_KEY, None),
        None,
        &ctx(),
        &GoogleEndpoints::at(&server.uri()),
    )
    .unwrap();
    assert_eq!(identity.principal(), email);
    assert_eq!(identity.source(), GoogleSource::ServiceAccount);
    let dbg = format!("{identity:?}");
    assert!(!dbg.contains("PRIVATE KEY"), "{dbg}");

    let before = Utc::now().timestamp();
    let token = identity
        .access_token(&[
            "https://www.googleapis.com/auth/bigquery",
            CLOUD_PLATFORM_SCOPE,
        ])
        .await
        .unwrap();
    assert_eq!(token.token, "ya29.sa");

    let sent = form(&requests_to(&server, "/token").await[0].body);
    assert_eq!(
        sent["grant_type"],
        "urn:ietf:params:oauth:grant-type:jwt-bearer"
    );
    let assertion = &sent["assertion"];
    let header = jsonwebtoken::decode_header(assertion).unwrap();
    assert_eq!(header.alg, jsonwebtoken::Algorithm::RS256);
    assert_eq!(header.kid.as_deref(), Some("kid-1"));

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.set_audience(&[OAUTH2_TOKEN_URL]);
    validation.set_issuer(&[email]);
    let claims = jsonwebtoken::decode::<serde_json::Value>(
        assertion,
        &jsonwebtoken::DecodingKey::from_rsa_pem(TEST_PUBLIC_KEY.as_bytes()).unwrap(),
        &validation,
    )
    .expect("the assertion verifies against the key's public half")
    .claims;
    assert_eq!(claims["sub"], email);
    assert_eq!(
        claims["scope"],
        "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform"
    );
    let iat = claims["iat"].as_i64().unwrap();
    assert!(
        (before - 61..=before - 59).contains(&iat),
        "iat backdated 60 s: {iat} vs {before}"
    );
    assert_eq!(claims["exp"].as_i64().unwrap(), iat + 3600);
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn a_token_exchange_refusal_keeps_googles_code_and_drops_its_description() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid_grant",
            "error_description": "SECRET-DESCRIPTION echoing the assertion",
        })))
        .mount(&server)
        .await;
    let identity = GoogleIdentity::with_endpoints(
        &sa_credentials("denied@p.iam.gserviceaccount.com", TEST_PRIVATE_KEY, None),
        None,
        &ctx(),
        &GoogleEndpoints::at(&server.uri()),
    )
    .unwrap();
    let err = identity.access_token(&[]).await.unwrap_err();
    match &err {
        IcebergError::GoogleTokenExchange {
            stage,
            status,
            reason,
        } => {
            assert_eq!(*stage, crate::error::GoogleTokenStage::Jwt);
            assert_eq!(*status, Some(400));
            assert_eq!(reason.as_deref(), Some("invalid_grant"));
        }
        other => panic!("expected a token-exchange refusal, got {other:?}"),
    }
    assert!(!err.to_string().contains("SECRET"), "{err}");
}

/// Two keys claiming one service account's email and key id are different
/// credentials: the second must not be handed the first's cached token.
#[cfg(feature = "aws")]
#[tokio::test]
async fn a_cached_service_account_token_is_bound_to_the_key_that_minted_it() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(Sequence {
            calls: AtomicUsize::new(0),
            responses: vec![
                ResponseTemplate::new(200).set_body_json(token_body("ya29.owner", 3600)),
                ResponseTemplate::new(200).set_body_json(token_body("ya29.other", 3600)),
            ],
        })
        .mount(&server)
        .await;
    let email = "victim@lake-project.iam.gserviceaccount.com";
    let endpoints = GoogleEndpoints::at(&server.uri());
    let owner = GoogleIdentity::with_endpoints(
        &sa_credentials(email, TEST_PRIVATE_KEY, None),
        None,
        &ctx(),
        &endpoints,
    )
    .unwrap();
    assert_eq!(owner.access_token(&[]).await.unwrap().token, "ya29.owner");

    let impostor = GoogleIdentity::with_endpoints(
        &sa_credentials(email, OTHER_PRIVATE_KEY, None),
        None,
        &ctx(),
        &endpoints,
    )
    .unwrap();
    assert_eq!(impostor.principal(), owner.principal());
    assert_ne!(
        impostor.access_token(&[]).await.unwrap().token,
        "ya29.owner",
        "a key that only names the service account must not get its token"
    );
    assert_eq!(requests_to(&server, "/token").await.len(), 2);
}

/// End to end: a REST catalog sees the minted bearer and the key file's
/// quota project; an explicit quota project wins over it.
#[cfg(feature = "aws")]
#[tokio::test]
async fn the_key_files_quota_project_reaches_the_catalog_request() {
    use crate::catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.minted", 3600)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"namespaces": [["db"]]})),
        )
        .mount(&server)
        .await;
    let endpoints = GoogleEndpoints::at(&server.uri());

    for (quota_project, expected) in [
        (None, "key-project"),
        (Some("explicit-project"), "explicit-project"),
    ] {
        let auth = AuthConfig::Google {
            credentials: sa_credentials(
                "catalog@lake-project.iam.gserviceaccount.com",
                TEST_PRIVATE_KEY,
                Some("key-project"),
            ),
            scopes: None,
            quota_project: quota_project.map(str::to_string),
        }
        .create_provider_arc_with(&ctx(), &endpoints)
        .unwrap();
        let client = RestCatalogClient::new(
            RestCatalogConfig {
                uri: server.uri(),
                // A warehouse no other test uses: pooled mock servers reuse
                // URLs, and the route-prefix cache is keyed by URL + warehouse.
                warehouse: Some("google-quota-project".to_string()),
                ..Default::default()
            },
            auth,
        )
        .unwrap();
        let namespaces = SendCatalogClient::list_namespaces(&client).await.unwrap();
        assert_eq!(namespaces, vec!["db".to_string()]);

        let listed = requests_to(&server, "/v1/namespaces").await;
        let last = listed.last().unwrap();
        assert_eq!(last.headers["authorization"], "Bearer ya29.minted");
        assert_eq!(last.headers["x-goog-user-project"], expected);
    }
}

// ── workload identity federation ──────────────────────────────────────────

#[cfg(feature = "aws")]
mod wif {
    use super::*;
    use crate::auth::google::external_account;
    use aws_credential_types::Credentials;
    use sha2::{Digest, Sha256};
    use wiremock::matchers::path_regex;

    #[derive(Debug)]
    pub(super) struct FakeSubject {
        pub(super) key: &'static str,
        pub(super) credentials: Credentials,
    }

    #[async_trait]
    impl AwsSubjectCredentials for FakeSubject {
        fn principal_key(&self) -> String {
            self.key.to_string()
        }

        async fn credentials(&self) -> Result<Credentials> {
            Ok(self.credentials.clone())
        }

        fn region(&self) -> Option<String> {
            Some("us-east-1".to_string())
        }
    }

    pub(super) fn subject(key: &'static str) -> IdentityContext {
        IdentityContext {
            aws_subject: Some(Arc::new(FakeSubject {
                key,
                credentials: Credentials::new(
                    "AKIDEXAMPLE",
                    "AWS-SECRET-KEY",
                    Some("SESSION-TOKEN".to_string()),
                    None,
                    "test",
                ),
            })),
            ..IdentityContext::default()
        }
    }

    const AUDIENCE: &str = "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider";

    async fn sts(server: &MockServer, token: &str) {
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": token,
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "token_type": "Bearer",
                "expires_in": 3599,
            })))
            .mount(server)
            .await;
    }

    fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
        let mut block = [0u8; 64];
        block[..key.len()].copy_from_slice(key);
        let pad = |byte: u8| block.map(|b| b ^ byte);
        let inner = Sha256::new()
            .chain_update(pad(0x36))
            .chain_update(message)
            .finalize();
        Sha256::new()
            .chain_update(pad(0x5c))
            .chain_update(inner)
            .finalize()
            .into()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The STS request carries a subject token that is AWS's SigV4 signature
    /// of GetCallerIdentity, with the audience header inside the signature —
    /// verified independently of the signer, from the SigV4 spec.
    #[tokio::test]
    async fn the_subject_token_is_a_signed_get_caller_identity() {
        let server = MockServer::start().await;
        sts(&server, "ya29.federated").await;
        let mut config = external_account::tests::gcloud_aws_config();
        config.remove("service_account_impersonation_url");
        let identity = GoogleIdentity::with_endpoints(
            &GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(config),
            },
            None,
            &subject("tenant-a"),
            &GoogleEndpoints::at(&server.uri()),
        )
        .unwrap();
        assert_eq!(identity.principal(), format!("wif:{AUDIENCE}"));
        assert_eq!(
            identity.source(),
            GoogleSource::ExternalAccount { host_signed: false }
        );
        assert_eq!(
            identity.access_token(&[]).await.unwrap().token,
            "ya29.federated"
        );

        let sent = form(&requests_to(&server, "/v1/token").await[0].body);
        assert_eq!(
            sent["grant_type"],
            "urn:ietf:params:oauth:grant-type:token-exchange"
        );
        assert_eq!(sent["audience"], AUDIENCE);
        assert_eq!(sent["scope"], CLOUD_PLATFORM_SCOPE);
        assert_eq!(
            sent["requested_token_type"],
            "urn:ietf:params:oauth:token-type:access_token"
        );
        assert_eq!(
            sent["subject_token_type"],
            "urn:ietf:params:aws:token-type:aws4_request"
        );

        let decoded = urlencoding::decode(&sent["subject_token"]).unwrap();
        let token: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(
            token["url"],
            "https://sts.us-east-1.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
        );
        assert_eq!(token["method"], "POST");
        let headers: HashMap<String, String> = token["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| {
                (
                    h["key"].as_str().unwrap().to_string(),
                    h["value"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(headers["x-goog-cloud-target-resource"], AUDIENCE);
        assert_eq!(headers["host"], "sts.us-east-1.amazonaws.com");
        assert_eq!(headers["x-amz-security-token"], "SESSION-TOKEN");
        let date = &headers["x-amz-date"];
        assert_eq!(date.len(), 16, "{date}");
        let day = &date[..8];

        let authorization = &headers["Authorization"];
        assert!(!authorization.contains("AWS-SECRET-KEY"), "{authorization}");
        let signed_headers = "host;x-amz-date;x-amz-security-token;x-goog-cloud-target-resource";
        let scope = format!("{day}/us-east-1/sts/aws4_request");
        let prefix = format!(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/{scope}, SignedHeaders={signed_headers}, Signature="
        );
        let signature = authorization
            .strip_prefix(&prefix)
            .unwrap_or_else(|| panic!("unexpected Authorization: {authorization}"));

        let canonical_request = format!(
            "POST\n/\nAction=GetCallerIdentity&Version=2011-06-15\n\
             host:sts.us-east-1.amazonaws.com\nx-amz-date:{date}\n\
             x-amz-security-token:SESSION-TOKEN\nx-goog-cloud-target-resource:{AUDIENCE}\n\n\
             {signed_headers}\n{}",
            hex(&Sha256::digest(b""))
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            hex(&Sha256::digest(canonical_request.as_bytes()))
        );
        let k_date = hmac(b"AWS4AWS-SECRET-KEY", day.as_bytes());
        let k_region = hmac(&k_date, b"us-east-1");
        let k_service = hmac(&k_region, b"sts");
        let k_signing = hmac(&k_service, b"aws4_request");
        assert_eq!(
            signature,
            hex(&hmac(&k_signing, string_to_sign.as_bytes())),
            "the signature covers the audience header"
        );
    }

    #[tokio::test]
    async fn impersonation_exchanges_the_federated_token_and_reads_expire_time() {
        let server = MockServer::start().await;
        sts(&server, "ya29.federated").await;
        // Within the 24 h ceiling, so read as Google states it.
        let expire_time = (Utc::now() + chrono::Duration::hours(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Mock::given(method("POST"))
            .and(path_regex(
                r"^/v1/projects/-/serviceAccounts/reader@lake-project\.iam\.gserviceaccount\.com:generateAccessToken$",
            ))
            .and(header("authorization", "Bearer ya29.federated"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accessToken": "ya29.impersonated",
                "expireTime": expire_time,
            })))
            .mount(&server)
            .await;
        let identity = GoogleIdentity::with_endpoints(
            &GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(external_account::tests::gcloud_aws_config()),
            },
            None,
            &subject("tenant-a"),
            &GoogleEndpoints::at(&server.uri()),
        )
        .unwrap();
        assert_eq!(
            identity.principal(),
            format!("wif:{AUDIENCE}→reader@lake-project.iam.gserviceaccount.com")
        );
        let token = identity
            .access_token(&["https://www.googleapis.com/auth/bigquery"])
            .await
            .unwrap();
        assert_eq!(token.token, "ya29.impersonated");
        assert_eq!(
            token.expires_at,
            DateTime::parse_from_rfc3339(&expire_time).unwrap()
        );

        // STS mints a cloud-platform federated token; the requested scope goes
        // to the impersonated one, with Google's default lifetime.
        let sent = form(&requests_to(&server, "/v1/token").await[0].body);
        assert_eq!(sent["scope"], CLOUD_PLATFORM_SCOPE);
        let impersonation = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.url.path().ends_with(":generateAccessToken"))
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&impersonation.body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"scope": ["https://www.googleapis.com/auth/bigquery"], "lifetime": "3600s"})
        );
    }

    /// A far-future `expireTime` is believed only up to the 24 h ceiling.
    #[tokio::test]
    async fn a_far_future_expire_time_is_clamped() {
        let server = MockServer::start().await;
        sts(&server, "ya29.federated").await;
        Mock::given(method("POST"))
            .and(path_regex(r":generateAccessToken$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accessToken": "ya29.impersonated",
                "expireTime": "9999-12-31T23:59:59Z",
            })))
            .mount(&server)
            .await;
        let token = GoogleIdentity::with_endpoints(
            &GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(external_account::tests::gcloud_aws_config()),
            },
            None,
            &subject("tenant-a"),
            &GoogleEndpoints::at(&server.uri()),
        )
        .unwrap()
        .access_token(&[])
        .await
        .unwrap();
        let ceiling = Utc::now()
            + chrono::Duration::seconds(crate::auth::token::MAX_TOKEN_LIFETIME_SECS)
            + chrono::Duration::seconds(5);
        assert!(token.expires_at <= ceiling, "{:?}", token.expires_at);
    }

    #[tokio::test]
    async fn an_impersonation_refusal_is_typed_with_googles_status() {
        let server = MockServer::start().await;
        sts(&server, "ya29.federated").await;
        Mock::given(method("POST"))
            .and(path_regex(r":generateAccessToken$"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {"code": 403, "message": "SECRET principal://x lacks", "status": "PERMISSION_DENIED"}
            })))
            .mount(&server)
            .await;
        let identity = GoogleIdentity::with_endpoints(
            &GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(external_account::tests::gcloud_aws_config()),
            },
            None,
            &subject("tenant-a"),
            &GoogleEndpoints::at(&server.uri()),
        )
        .unwrap();
        let err = identity.access_token(&[]).await.unwrap_err();
        assert!(
            matches!(
                &err,
                IcebergError::GoogleTokenExchange {
                    stage: crate::error::GoogleTokenStage::Impersonation,
                    status: Some(403),
                    reason: Some(reason),
                } if reason == "PERMISSION_DENIED"
            ),
            "{err:?}"
        );
        assert!(!err.to_string().contains("SECRET"), "{err}");
    }

    /// One pasted WIF config, two AWS subjects: two STS exchanges, never a
    /// shared token. The same subject again is a cache hit.
    #[tokio::test]
    async fn two_aws_subjects_never_share_a_token() {
        let server = MockServer::start().await;
        sts(&server, "ya29.federated").await;
        let mut config = external_account::tests::gcloud_aws_config();
        config.remove("service_account_impersonation_url");
        let credentials = GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Inline(config),
        };
        let endpoints = GoogleEndpoints::at(&server.uri());
        for tenant in ["tenant-a", "tenant-b", "tenant-a"] {
            GoogleIdentity::with_endpoints(&credentials, None, &subject(tenant), &endpoints)
                .unwrap()
                .access_token(&[])
                .await
                .unwrap();
        }
        let exchanges: Vec<String> = requests_to(&server, "/v1/token")
            .await
            .iter()
            .map(|r| form(&r.body)["subject_token"].clone())
            .collect();
        assert_eq!(
            exchanges.len(),
            2,
            "tenant-a, tenant-b; then tenant-a cached"
        );
    }

    #[test]
    fn an_embedder_subject_is_not_a_host_identity() {
        let closed = IdentityContext {
            policy: HostIdentityPolicy::deny_all(),
            ..subject("tenant-a")
        };
        let identity = GoogleIdentity::with_endpoints(
            &wif_credentials(),
            None,
            &closed,
            &GoogleEndpoints::at("http://127.0.0.1:9"),
        )
        .unwrap();
        assert_eq!(
            identity.source(),
            GoogleSource::ExternalAccount { host_signed: false }
        );
    }

    #[test]
    fn the_host_chain_signs_when_no_subject_is_supplied() {
        let identity = GoogleIdentity::with_endpoints(
            &wif_credentials(),
            None,
            &ctx(),
            &GoogleEndpoints::at("http://127.0.0.1:9"),
        )
        .unwrap();
        assert_eq!(
            identity.source(),
            GoogleSource::ExternalAccount { host_signed: true }
        );
        // An embedder whose subject calls itself `host` keys apart from the
        // host chain, so it can never be handed the host's token.
        let impostor = GoogleIdentity::with_endpoints(
            &wif_credentials(),
            None,
            &subject("host"),
            &GoogleEndpoints::at("http://127.0.0.1:9"),
        )
        .unwrap();
        assert_ne!(identity.subject, impostor.subject);
    }

    #[test]
    fn a_wif_config_held_in_a_value_is_parsed_and_validated() {
        let text =
            serde_json::Value::Object(external_account::tests::gcloud_aws_config()).to_string();
        let held = GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Ref(ConfigValue::literal(text)),
        };
        GoogleIdentity::with_endpoints(
            &held,
            None,
            &subject("t"),
            &GoogleEndpoints::at("http://127.0.0.1:9"),
        )
        .unwrap();
        let mut bad = external_account::tests::gcloud_aws_config();
        bad.insert("token_url".into(), "https://evil.example/token".into());
        let err = GoogleIdentity::with_endpoints(
            &GoogleCredentials::ExternalAccount {
                config: ExternalAccountConfig::Inline(bad),
            },
            None,
            &subject("t"),
            &GoogleEndpoints::at("http://127.0.0.1:9"),
        )
        .unwrap_err();
        assert!(matches!(err, IcebergError::GoogleCredentialsInvalid(_)));
    }
}

#[cfg(feature = "aws")]
fn wif_credentials() -> GoogleCredentials {
    GoogleCredentials::ExternalAccount {
        config: ExternalAccountConfig::Inline(external_account::tests::gcloud_aws_config()),
    }
}
