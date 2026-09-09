//! `f:IdentityRestricted` override control over the real HTTP query and
//! transact routes.
//!
//! The identity the allow-list is checked against is the auth-layer-verified
//! one: the bearer token's `fluree.identity` (or `sub`), or a signed
//! credential's DID. A value the caller writes into `opts.identity` or the
//! `fluree-identity` header is policy context, not authorization, and must
//! never satisfy the allow-list.
//!
//! Config says `f:reasoningModes f:rdfs`; every query here says
//! `"reasoning": "none"` (or the SPARQL pragma). An entailed row means the
//! override was denied and config won; no row means it was permitted.
//! `default-allow: true` rides along so the policy wrap a bearer identity
//! triggers cannot hide the row and masquerade as a permitted override.

use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::config::DataAuthMode;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const LEDGER: &str = "overridectl:main";
const ADMIN: &str = "did:key:admin";
const OTHER: &str = "did:key:other";

/// `ex:childName rdfs:subPropertyOf ex:name`, so a query for `ex:name` finds
/// "Alice" only when RDFS reasoning is engaged.
const SEED_TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:childName rdfs:subPropertyOf ex:name .
ex:alice ex:childName "Alice" .
"#;

/// RDFS by default; only `did:key:admin` may override.
const CONFIG_TRIG: &str = r"
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:overridectl:main#config> {
    <urn:overridectl:config> rdf:type f:LedgerConfig .
    <urn:overridectl:config> f:reasoningDefaults <urn:overridectl:reasoning> .
    <urn:overridectl:reasoning> f:reasoningModes f:rdfs .
    <urn:overridectl:reasoning> f:overrideControl <urn:overridectl:oc> .
    <urn:overridectl:oc> f:controlMode f:IdentityRestricted .
    <urn:overridectl:oc> f:allowedIdentities <did:key:admin> .
}
";

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &JsonValue, signing_key: &SigningKey) -> String {
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes());
    let header = json!({
        "alg": "EdDSA",
        "jwk": {"kty": "OKP", "crv": "Ed25519", "x": pubkey_b64}
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    format!(
        "{header_b64}.{payload_b64}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    )
}

/// A bearer token scoped to the test ledger. `identity` becomes the token's
/// `fluree.identity`, which the server treats as the verified identity.
fn bearer(identity: Option<&str>, write: bool) -> String {
    let signing_key = SigningKey::from_bytes(&[9u8; 32]);
    let mut claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.ledger.read.ledgers": [LEDGER],
    });
    if write {
        claims["fluree.ledger.write.ledgers"] = json!([LEDGER]);
    }
    if let Some(identity) = identity {
        claims["fluree.identity"] = json!(identity);
    }
    create_jws(&claims, &signing_key)
}

/// Fail-closed policy by default; only `did:key:admin` may override it.
const POLICY_CONFIG_TRIG: &str = r"
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

GRAPH <urn:fluree:overridectl:main#config> {
    <urn:overridectl:config> rdf:type f:LedgerConfig .
    <urn:overridectl:config> f:policyDefaults <urn:overridectl:policy> .
    <urn:overridectl:policy> f:defaultAllow false .
    <urn:overridectl:policy> f:overrideControl <urn:overridectl:oc> .
    <urn:overridectl:oc> f:controlMode f:IdentityRestricted .
    <urn:overridectl:oc> f:allowedIdentities <did:key:admin> .
}
";

async fn seeded_app(mode: DataAuthMode) -> (TempDir, axum::Router) {
    seeded_app_with(mode, CONFIG_TRIG).await
}

async fn seeded_app_with(mode: DataAuthMode, config_trig: &str) -> (TempDir, axum::Router) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: mode,
        data_auth_insecure_accept_any_issuer: true,
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
                .body(Body::from(json!({ "ledger": LEDGER }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    // Seed with a write-scoped token that carries no identity, so no policy
    // context is built for the writes (they would otherwise be fail-closed).
    let seed_token = bearer(None, true);
    for trig in [SEED_TRIG, config_trig] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/upsert/{LEDGER}"))
                    .header("content-type", "application/trig")
                    .header("authorization", format!("Bearer {seed_token}"))
                    .body(Body::from(trig.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "trig upsert");
    }
    (tmp, app)
}

async fn query(
    app: &axum::Router,
    content_type: &str,
    body: String,
    headers: &[(&str, String)],
) -> (StatusCode, JsonValue) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{LEDGER}"))
        .header("content-type", content_type);
    for (k, v) in headers {
        req = req.header(*k, v.as_str());
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

/// The entailment query with the override the config restricts.
fn reasoning_none_body(opts: JsonValue) -> String {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"},
        "reasoning": "none",
        "opts": opts
    })
    .to_string()
}

const SPARQL_REASONING_NONE: &str = "# PRAGMA reasoning: none
PREFIX ex: <http://example.org/>
SELECT ?v WHERE { ex:alice ex:name ?v }";

fn bindings(json: &JsonValue) -> usize {
    json.get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(JsonValue::as_array)
        .map(Vec::len)
        .unwrap_or_else(|| panic!("expected SPARQL-results bindings, got {json}"))
}

/// A bearer whose verified identity is on the allow-list may override; one
/// whose verified identity is not gets the configured defaults.
#[tokio::test]
async fn allow_listed_bearer_may_override_reasoning_defaults() {
    let (_tmp, app) = seeded_app(DataAuthMode::Required).await;
    let body = reasoning_none_body(json!({"default-allow": true}));

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(ADMIN), false)),
    );
    let (status, json) = query(&app, "application/json", body.clone(), &[auth]).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json,
        json!([]),
        "the allow-listed verified identity may turn reasoning off"
    );

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(OTHER), false)),
    );
    let (status, json) = query(&app, "application/json", body, &[auth]).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json,
        json!(["Alice"]),
        "a verified identity outside the allow-list is denied; config's rdfs wins"
    );
}

/// SPARQL twin over the same route: the pragma is the query-time override.
#[tokio::test]
async fn allow_listed_bearer_may_override_reasoning_defaults_sparql() {
    let (_tmp, app) = seeded_app(DataAuthMode::Required).await;
    let default_allow = ("fluree-default-allow", "true".to_string());

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(ADMIN), false)),
    );
    let (status, json) = query(
        &app,
        "application/sparql-query",
        SPARQL_REASONING_NONE.to_string(),
        &[auth, default_allow.clone()],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        bindings(&json),
        0,
        "the allow-listed verified identity may turn reasoning off (SPARQL): {json}"
    );

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(OTHER), false)),
    );
    let (status, json) = query(
        &app,
        "application/sparql-query",
        SPARQL_REASONING_NONE.to_string(),
        &[auth, default_allow],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        bindings(&json),
        1,
        "a verified identity outside the allow-list is denied (SPARQL): {json}"
    );
}

/// With no auth layer in play, neither `opts.identity` nor the
/// `fluree-identity` header naming the allow-listed DID is a verified
/// identity: the request is anonymous for override purposes and config wins.
#[tokio::test]
async fn unverified_identity_in_body_or_header_does_not_authorize_override() {
    let (_tmp, app) = seeded_app(DataAuthMode::None).await;

    let body = reasoning_none_body(json!({"identity": ADMIN, "default-allow": true}));
    let (status, json) = query(&app, "application/json", body, &[]).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json,
        json!(["Alice"]),
        "opts.identity naming the allow-listed DID must not authorize the override"
    );

    let body = reasoning_none_body(json!({"default-allow": true}));
    let header = ("fluree-identity", ADMIN.to_string());
    let (status, json) = query(&app, "application/json", body, &[header]).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json,
        json!(["Alice"]),
        "a fluree-identity header naming the allow-listed DID must not authorize the override"
    );

    let headers = [
        ("fluree-identity", ADMIN.to_string()),
        ("fluree-default-allow", "true".to_string()),
    ];
    let (status, json) = query(
        &app,
        "application/sparql-query",
        SPARQL_REASONING_NONE.to_string(),
        &headers,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        bindings(&json),
        1,
        "a fluree-identity header must not authorize the override (SPARQL): {json}"
    );
}

// =============================================================================
// Transact-time policy override control
// =============================================================================

async fn insert(
    app: &axum::Router,
    body: String,
    headers: &[(&str, String)],
) -> (StatusCode, JsonValue) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/insert/{LEDGER}"))
        .header("content-type", "application/json");
    for (k, v) in headers {
        req = req.header(*k, v.as_str());
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

/// A write that asks to open policy (`default-allow: true`) against a ledger
/// whose config closes it. Config wins unless the override is permitted, and
/// a closed policy with no grants rejects the write.
fn open_policy_insert(subject: &str, opts: JsonValue) -> String {
    json!({
        "@context": {"ex": "http://example.org/"},
        "insert": {"@id": subject, "ex:name": "Bob"},
        "opts": opts
    })
    .to_string()
}

/// `merge_policy_opts` on the transact path reads the verified identity from
/// the governance the route built, so the allow-listed bearer may open policy
/// for its write and a non-listed one may not.
#[tokio::test]
async fn allow_listed_bearer_may_override_transact_policy_defaults() {
    let (_tmp, app) = seeded_app_with(DataAuthMode::Required, POLICY_CONFIG_TRIG).await;
    let opts = json!({"default-allow": true});

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(ADMIN), true)),
    );
    let (status, json) = insert(&app, open_policy_insert("ex:bob", opts.clone()), &[auth]).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the allow-listed verified identity may open policy for its write: {json}"
    );

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(OTHER), true)),
    );
    let (status, json) = insert(&app, open_policy_insert("ex:carol", opts), &[auth]).await;
    assert!(
        status.is_client_error(),
        "a verified identity outside the allow-list is denied; config's closed policy wins: \
         {status} {json}"
    );
}

/// With no auth layer, the allow-listed DID in `opts.identity` is not a
/// verified identity: the override is denied and the closed policy rejects
/// the write.
#[tokio::test]
async fn unverified_identity_does_not_authorize_transact_override() {
    let (_tmp, app) = seeded_app_with(DataAuthMode::None, POLICY_CONFIG_TRIG).await;

    let opts = json!({"identity": ADMIN, "default-allow": true});
    let (status, json) = insert(&app, open_policy_insert("ex:bob", opts), &[]).await;
    assert!(
        status.is_client_error(),
        "opts.identity naming the allow-listed DID must not authorize the override: \
         {status} {json}"
    );
}

// =============================================================================
// Transact-time SHACL override control (`opts.validationMode`)
// =============================================================================

/// A shape every `ex:Person` violates without an `ex:name`, plus a SHACL
/// config that rejects by default and lets only `did:key:admin` soften it.
const SHACL_CONFIG_TRIG: &str = r#"
@prefix f: <https://ns.flur.ee/db#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:PersonShape rdf:type sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .

# The allow-listed DID exists as a subject, as a policy identity would. The
# old gate decoded the policy context's identity and so only ever matched
# an IRI the ledger knew; without this the unverified-identity regression
# test below could not tell the old gate from the new one.
<did:key:admin> ex:name "Admin" .

GRAPH <urn:fluree:overridectl:main#config> {
    <urn:overridectl:config> rdf:type f:LedgerConfig .
    <urn:overridectl:config> f:shaclDefaults <urn:overridectl:shacl> .
    <urn:overridectl:shacl> f:shaclEnabled true .
    <urn:overridectl:shacl> f:overrideControl <urn:overridectl:oc> .
    <urn:overridectl:oc> f:controlMode f:IdentityRestricted .
    <urn:overridectl:oc> f:allowedIdentities <did:key:admin> .
}
"#;

/// A violating write that asks to soften the posture to warn-and-commit.
/// `default-allow: true` keeps the policy wrap a bearer identity triggers
/// from being the reason the write fails.
fn warn_mode_violating_insert(subject: &str, extra_opts: JsonValue) -> String {
    let mut opts = json!({"validationMode": "warn", "default-allow": true});
    if let (Some(base), Some(extra)) = (opts.as_object_mut(), extra_opts.as_object()) {
        base.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    json!({
        "@context": {"ex": "http://example.org/"},
        "insert": {"@id": subject, "@type": "ex:Person"},
        "opts": opts
    })
    .to_string()
}

fn assert_shacl_rejected(status: StatusCode, json: &JsonValue, what: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {json}");
    assert!(
        json.to_string().contains("SHACL validation failed"),
        "{what}: expected a SHACL rejection, got {json}"
    );
}

/// The SHACL gate reads the verified identity the route recorded, so the
/// allow-listed bearer may soften the posture for its own write and a
/// non-listed bearer may not.
#[tokio::test]
async fn allow_listed_bearer_may_soften_shacl_posture() {
    let (_tmp, app) = seeded_app_with(DataAuthMode::Required, SHACL_CONFIG_TRIG).await;

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(ADMIN), true)),
    );
    let (status, json) = insert(
        &app,
        warn_mode_violating_insert("ex:nameless1", json!({})),
        &[auth],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the allow-listed verified identity may soften the posture: {json}"
    );

    let auth = (
        "authorization",
        format!("Bearer {}", bearer(Some(OTHER), true)),
    );
    let (status, json) = insert(
        &app,
        warn_mode_violating_insert("ex:nameless2", json!({})),
        &[auth],
    )
    .await;
    assert_shacl_rejected(
        status,
        &json,
        "a verified identity outside the allow-list is denied",
    );
}

/// With no auth layer, the allow-listed DID in `opts.identity` or the
/// `fluree-identity` header becomes the policy identity, but it is not a
/// verified one: the softening request is denied and the write is rejected.
/// This is the regression test for the gate reading the policy context.
#[tokio::test]
async fn unverified_identity_does_not_soften_shacl_posture() {
    let (_tmp, app) = seeded_app_with(DataAuthMode::None, SHACL_CONFIG_TRIG).await;

    let (status, json) = insert(
        &app,
        warn_mode_violating_insert("ex:nameless1", json!({"identity": ADMIN})),
        &[],
    )
    .await;
    assert_shacl_rejected(
        status,
        &json,
        "opts.identity naming the allow-listed DID must not soften the posture",
    );

    let header = ("fluree-identity", ADMIN.to_string());
    let (status, json) = insert(
        &app,
        warn_mode_violating_insert("ex:nameless2", json!({})),
        &[header],
    )
    .await;
    assert_shacl_rejected(
        status,
        &json,
        "a fluree-identity header naming the allow-listed DID must not soften the posture",
    );
}
