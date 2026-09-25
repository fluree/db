//! W3C SPARQL 1.1 Graph Store HTTP Protocol over `/v1/fluree/data/<ledger>`:
//! `GET`/`HEAD` read a graph, `PUT` replaces it, `POST` adds to it, `DELETE`
//! removes it, with the protocol's status codes.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "gsp:main";
const TOOLS: &str = "urn:example:tools";
const OTHER: &str = "urn:example:other";

const SEED_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
ex:seed ex:name "SeedDefault" .
<urn:example:other> { ex:zed ex:name "Zed" . }
"#;

async fn seeded_app() -> (TempDir, axum::Router) {
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

    let create = serde_json::json!({ "ledger": LEDGER }).to_string();
    let (status, body) = send(
        &app,
        "POST",
        "/v1/fluree/create",
        Some("application/json"),
        &create,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create ledger: {body}");
    let (status, body) = send(
        &app,
        "POST",
        &format!("/v1/fluree/upsert/{LEDGER}"),
        Some("application/trig"),
        SEED_TRIG,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seed: {body}");
    (tmp, app)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: &str,
    accept: Option<&str>,
) -> (StatusCode, String) {
    let (status, _, body) = send_full(app, method, uri, content_type, body, accept).await;
    (status, body)
}

async fn send_full(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: &str,
    accept: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    if let Some(accept) = accept {
        req = req.header("accept", accept);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, ct, String::from_utf8_lossy(&bytes).into_owned())
}

fn named(graph: &str) -> String {
    format!(
        "/v1/fluree/data/{LEDGER}?graph={}",
        urlencoding::encode(graph)
    )
}

fn default_graph() -> String {
    format!("/v1/fluree/data/{LEDGER}?default")
}

const TTL: Option<&str> = Some("text/turtle");
const JSON_LD: Option<&str> = Some("application/ld+json");

#[tokio::test]
async fn named_graph_lifecycle() {
    let (_tmp, app) = seeded_app().await;
    let tools = named(TOOLS);

    let (status, body) = send(&app, "GET", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "absent graph: {body}");

    let spec = "@prefix ex: <http://example.org/> .\n\
                ex:search ex:name \"search\" ; ex:param [ ex:name \"q\" ] .\n";
    let (status, body) = send(&app, "PUT", &tools, TTL, spec, None).await;
    assert_eq!(status, StatusCode::CREATED, "PUT creates: {body}");
    let t1 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"].as_i64();

    // An identical PUT replaces nothing and commits nothing.
    let (status, body) = send(&app, "PUT", &tools, TTL, spec, None).await;
    assert_eq!(status, StatusCode::OK, "PUT replaces: {body}");
    let t2 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"].as_i64();
    assert_eq!(t1, t2, "an identical PUT must not commit");

    let (status, ct, body) = send_full(&app, "GET", &tools, None, "", JSON_LD).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ct.as_deref(), Some("application/ld+json; charset=utf-8"));
    assert!(body.contains("search") && body.contains("\"q\""), "{body}");
    assert!(
        !body.contains("Zed") && !body.contains("SeedDefault"),
        "only this graph: {body}"
    );

    let (status, ct, body) = send_full(
        &app,
        "GET",
        &tools,
        None,
        "",
        Some("text/turtle;q=0.5, application/rdf+xml"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ct.as_deref(), Some("application/rdf+xml; charset=utf-8"));
    assert!(body.contains("search"), "{body}");

    let (status, ct, nt) =
        send_full(&app, "GET", &tools, None, "", Some("application/n-triples")).await;
    assert_eq!(status, StatusCode::OK, "{nt}");
    assert_eq!(ct.as_deref(), Some("application/n-triples; charset=utf-8"));
    assert_eq!(nt.lines().count(), 3, "{nt}");

    // Turtle out, Turtle back in: the graph (its blank node included, by its
    // stored label) comes back unchanged, so the PUT commits nothing.
    let (status, ct, ttl) = send_full(&app, "GET", &tools, None, "", TTL).await;
    assert_eq!(status, StatusCode::OK, "{ttl}");
    assert_eq!(ct.as_deref(), Some("text/turtle; charset=utf-8"));
    assert!(ttl.contains("\"search\""), "{ttl}");
    let (status, body) = send(&app, "PUT", &tools, TTL, &ttl, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let t3 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"].as_i64();
    assert_eq!(t1, t3, "a GET's Turtle PUT back must not commit:\n{ttl}");

    let (status, body) = send(&app, "GET", &tools, None, "", Some("text/csv")).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{body}");

    let (status, body) = send(&app, "HEAD", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty(), "HEAD has no body: {body}");

    // POST adds; nothing already there is retracted.
    let more = "@prefix ex: <http://example.org/> .\nex:fetch ex:name \"fetch\" .\n";
    let (status, body) = send(&app, "POST", &tools, TTL, more, None).await;
    assert_eq!(status, StatusCode::OK, "POST adds: {body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && body.contains("search"), "{body}");

    // PUT with fewer triples retracts the rest.
    let smaller = "@prefix ex: <http://example.org/> .\nex:fetch ex:name \"fetch\" .\n";
    let (status, body) = send(&app, "PUT", &tools, TTL, smaller, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && !body.contains("search"), "{body}");

    let (status, body) = send(&app, "DELETE", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "DELETE: {body}");
    let (status, _) = send(&app, "GET", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "gone after DELETE");
    let (status, _) = send(&app, "DELETE", &tools, None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "nothing left to DELETE");

    // Other graphs were never touched.
    let (status, body) = send(&app, "GET", &named(OTHER), None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Zed"), "{body}");
}

#[tokio::test]
async fn empty_put_clears_the_graph() {
    let (_tmp, app) = seeded_app().await;
    let other = named(OTHER);

    // The protocol's PUT replaces the graph with the body, an empty one
    // included, with no allowEmpty opt-in.
    let (status, body) = send(&app, "PUT", &other, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = send(&app, "GET", &other, None, "", None).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an emptied named graph no longer exists"
    );
}

#[tokio::test]
async fn default_graph_lifecycle() {
    let (_tmp, app) = seeded_app().await;
    let default = default_graph();

    let (status, body) = send(&app, "GET", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("SeedDefault") && !body.contains("Zed"),
        "default graph only: {body}"
    );

    let replacement = "@prefix ex: <http://example.org/> .\nex:new ex:name \"NewDefault\" .\n";
    let (status, body) = send(&app, "PUT", &default, TTL, replacement, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the default graph always exists: {body}"
    );
    let (_, body) = send(&app, "GET", &default, None, "", None).await;
    assert!(
        body.contains("NewDefault") && !body.contains("SeedDefault"),
        "{body}"
    );

    let (status, body) = send(&app, "DELETE", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(&app, "GET", &default, None, "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("NewDefault"), "cleared: {body}");

    let (_, body) = send(&app, "GET", &named(OTHER), None, "", None).await;
    assert!(body.contains("Zed"), "named graphs untouched: {body}");
}

#[tokio::test]
async fn json_ld_bodies() {
    let (_tmp, app) = seeded_app().await;
    let tools = named(TOOLS);
    let doc = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{ "@id": "ex:search", "ex:name": "search" }]
    })
    .to_string();
    let (status, body) = send(&app, "PUT", &tools, JSON_LD, &doc, None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let add = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:fetch", "ex:name": "fetch"
    })
    .to_string();
    let (status, body) = send(&app, "POST", &tools, Some("application/json"), &add, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(&app, "GET", &tools, None, "", None).await;
    assert!(body.contains("fetch") && body.contains("search"), "{body}");
}

#[tokio::test]
async fn request_errors() {
    let (_tmp, app) = seeded_app().await;
    let base = format!("/v1/fluree/data/{LEDGER}");
    let spec = "@prefix ex: <http://example.org/> .\nex:a ex:p \"x\" .\n";

    for uri in [
        base.clone(),
        format!("{base}?graph={TOOLS}&default"),
        format!("{base}?graph=relative"),
        format!(
            "{base}?graph={}",
            urlencoding::encode("urn:fluree:gsp:main#txn-meta")
        ),
    ] {
        let (status, body) = send(&app, "GET", &uri, None, "", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        let (status, body) = send(&app, "PUT", &uri, TTL, spec, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
    }

    let (status, body) = send(
        &app,
        "PUT",
        &named(TOOLS),
        Some("application/xml"),
        "<x/>",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");

    let other_block =
        format!("@prefix ex: <http://example.org/> .\nGRAPH <{OTHER}> {{ ex:a ex:p \"x\" . }}\n");
    let (status, body) = send(
        &app,
        "POST",
        &named(TOOLS),
        Some("application/trig"),
        &other_block,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = send(&app, "POST", &named(TOOLS), TTL, "", None).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an empty POST has nothing to add: {body}"
    );

    let (status, body) = send(
        &app,
        "GET",
        "/v1/fluree/data/nosuch:main?default",
        None,
        "",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// An Ed25519-signed bearer token carrying `claims`.
fn bearer(claims: serde_json::Value) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::{Signer, SigningKey};

    let key = SigningKey::from_bytes(&[7u8; 32]);
    let mut claims = claims;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    claims["iss"] = fluree_db_credential::did_from_pubkey(&key.verifying_key().to_bytes()).into();
    claims["iat"] = now.into();
    claims["exp"] = (now + 3600).into();
    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {"kty": "OKP", "crv": "Ed25519",
                "x": URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes())}
    });
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let signature = URL_SAFE_NO_PAD.encode(key.sign(signing_input.as_bytes()).to_bytes());
    format!("Bearer {signing_input}.{signature}")
}

async fn send_as(
    app: &axum::Router,
    method: &str,
    uri: &str,
    auth: Option<&str>,
    content_type: Option<&str>,
    body: &str,
    accept: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        req = req.header("authorization", auth);
    }
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    if let Some(accept) = accept {
        req = req.header("accept", accept);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, ct, String::from_utf8_lossy(&bytes).into_owned())
}

/// With data auth required: every write verb needs a credential that may
/// write the ledger, a read-only one gets the write routes' existence-hiding
/// 404, and reads authenticate before saying whether a graph exists.
#[tokio::test]
async fn graph_store_requires_authorization() {
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
    let app = build_router(state);
    let create = serde_json::json!({ "ledger": LEDGER }).to_string();
    let (status, body) = send(&app, "POST", "/v1/fluree/create", JSON_LD, &create, None).await;
    assert_eq!(status, StatusCode::CREATED, "create ledger: {body}");

    let writer = bearer(serde_json::json!({
        "fluree.ledger.read.ledgers": [LEDGER],
        "fluree.ledger.write.ledgers": [LEDGER]
    }));
    let reader = bearer(serde_json::json!({ "fluree.ledger.read.ledgers": [LEDGER] }));
    // An identity routes the read through the identity-scoped query path; a
    // view policy (inserted below) lets it see the graph.
    let identified = bearer(serde_json::json!({
        "fluree.ledger.read.ledgers": [LEDGER],
        "fluree.identity": "http://example.org/reader"
    }));
    let tools = named(TOOLS);
    let absent = named(OTHER);
    let ttl = r#"<urn:x> <http://example.org/name> "tool" ."#;

    for (method, auth, expected) in [
        ("PUT", None, StatusCode::UNAUTHORIZED),
        ("POST", None, StatusCode::UNAUTHORIZED),
        ("DELETE", None, StatusCode::UNAUTHORIZED),
        ("PUT", Some(reader.as_str()), StatusCode::NOT_FOUND),
        ("POST", Some(reader.as_str()), StatusCode::NOT_FOUND),
    ] {
        let (status, _, body) = send_as(&app, method, &tools, auth, TTL, ttl, None).await;
        assert_eq!(status, expected, "{method} as {auth:?}: {body}");
    }
    let (status, _, body) = send_as(&app, "PUT", &tools, Some(&writer), TTL, ttl, None).await;
    assert_eq!(status, StatusCode::CREATED, "authorized PUT: {body}");

    // Against a graph that exists, so the refusal cannot be the absent-graph 404.
    let (status, _, body) = send_as(&app, "DELETE", &tools, Some(&reader), None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "DELETE as reader: {body}");
    let (status, _, body) = send_as(&app, "HEAD", &tools, Some(&reader), None, "", None).await;
    assert_eq!(status, StatusCode::OK, "the graph survives: {body}");

    // Unauthenticated, a present and an absent graph answer alike.
    for uri in [&tools, &absent] {
        for method in ["GET", "HEAD"] {
            let (status, _, body) = send_as(&app, method, uri, None, None, "", None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}: {body}");
        }
    }

    let (status, _, body) = send_as(&app, "HEAD", &tools, Some(&reader), None, "", None).await;
    assert_eq!(status, StatusCode::OK, "HEAD: {body}");
    let (status, _, body) = send_as(&app, "HEAD", &absent, Some(&reader), None, "", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "HEAD absent: {body}");

    let policy = serde_json::json!({
        "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/"},
        "@graph": [
            {
                "@id": "ex:reader-policy",
                "@type": ["f:AccessPolicy", "ex:ReaderClass"],
                "f:action": [{"@id": "f:view"}],
                "f:allow": true
            },
            {"@id": "http://example.org/reader", "f:policyClass": [{"@id": "ex:ReaderClass"}]}
        ]
    });
    let (status, _, body) = send_as(
        &app,
        "POST",
        &format!("/v1/fluree/insert/{LEDGER}"),
        Some(&writer),
        JSON_LD,
        &policy.to_string(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "insert policy: {body}");

    let (status, ct, body) = send_as(
        &app,
        "GET",
        &tools,
        Some(&identified),
        None,
        "",
        Some("application/rdf+xml"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "identity-scoped RDF/XML GET: {body}"
    );
    assert!(
        ct.as_deref()
            .is_some_and(|ct| ct.starts_with("application/rdf+xml")),
        "{ct:?}"
    );
    assert!(body.contains("tool"), "{body}");
}
