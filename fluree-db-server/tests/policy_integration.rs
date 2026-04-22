use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_credential::did_from_pubkey;
use fluree_db_server::config::DataAuthMode;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

// ── helpers ──────────────────────────────────────────────────────────────────

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

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn identity_token(signing_key: &SigningKey, identity: &str, ledger: &str) -> String {
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.identity": identity,
        "fluree.ledger.read.ledgers": [ledger],
    });
    create_jws(&claims, signing_key)
}

async fn policy_test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: DataAuthMode::Optional,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

/// Creates the ledger, inserts 3 documents, then inserts policies + identity nodes.
///
/// Documents:
///   ex:doc1 — "Public Post"        classification=public
///   ex:doc2 — "Internal Memo"      classification=internal
///   ex:doc3 — "Executive Salaries" classification=confidential
///
/// Policy classes:
///   ex:PublicClass   → only public
///   ex:EmployeeClass → public OR internal
///   ex:ManagerClass  → f:allow true (all)
///
/// Identity nodes (in-ledger):
///   ex:public-user   → ex:PublicClass
///   ex:employee-user → ex:EmployeeClass
///   ex:manager-user  → ex:ManagerClass
async fn setup_policy_ledger(app: axum::Router, ledger: &str) -> axum::Router {
    // 1. Create ledger
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": ledger }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    // 2. Insert sample documents
    let docs_tx = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "insert": [
            {
                "@id": "ex:doc1",
                "@type": "ex:Document",
                "schema:name": "Public Post",
                "ex:classification": "public",
                "ex:content": "visible to all"
            },
            {
                "@id": "ex:doc2",
                "@type": "ex:Document",
                "schema:name": "Internal Memo",
                "ex:classification": "internal",
                "ex:content": "visible to employees"
            },
            {
                "@id": "ex:doc3",
                "@type": "ex:Document",
                "schema:name": "Executive Salaries",
                "ex:classification": "confidential",
                "ex:content": "visible to managers"
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(docs_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert documents");

    // 3. Insert policies + identity nodes
    let policy_tx = serde_json::json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        },
        "insert": [
            // Public policy: only ex:classification = "public"
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
            // Employee policy: public OR internal
            {
                "@id": "ex:employee-policy",
                "@type": ["f:AccessPolicy", "ex:EmployeeClass"],
                "f:action": [{"@id": "f:view"}],
                "f:query": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            ["union",
                                {"@id": "?$this", "ex:classification": "public"},
                                {"@id": "?$this", "ex:classification": "internal"}
                            ]
                        ]
                    }
                }
            },
            // Manager policy: f:allow true — bypass filter entirely
            {
                "@id": "ex:manager-policy",
                "@type": ["f:AccessPolicy", "ex:ManagerClass"],
                "f:action": [{"@id": "f:view"}],
                "f:allow": true
            },
            // Identity nodes using full IRIs (not compact CURIEs).
            // JSON-LD would expand "ex:public-user" → "http://example.org/public-user"
            // during ingestion, so the token's fluree.identity must also be the
            // full IRI for resolve_identity_iri_to_sid to find the subject node.
            {
                "@id": "http://example.org/public-user",
                "f:policyClass": [{"@id": "ex:PublicClass"}]
            },
            {
                "@id": "http://example.org/employee-user",
                "f:policyClass": [{"@id": "ex:EmployeeClass"}]
            },
            {
                "@id": "http://example.org/manager-user",
                "f:policyClass": [{"@id": "ex:ManagerClass"}]
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(policy_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert policies");

    app
}

/// Runs the standard document query and returns the result rows.
///
/// The query selects `?name` and `?class` for all `ex:Document` subjects.
/// `default-allow` is controlled by the caller. No `opts.identity` — the
/// server injects it from the Bearer token via `force_query_auth_opts`.
async fn query_docs(
    app: axum::Router,
    ledger: &str,
    token: Option<&str>,
    default_allow: bool,
) -> (StatusCode, JsonValue) {
    let body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": default_allow },
        "select": ["?name", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{ledger}"))
        .header("content-type", "application/json");

    if let Some(tok) = token {
        req = req.header("authorization", format!("Bearer {tok}"));
    }

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();

    json_body(resp).await
}

fn names_from_results(results: &JsonValue) -> Vec<&str> {
    results
        .as_array()
        .expect("results should be an array")
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_str()))
        .collect()
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Public identity can only see documents classified as "public".
#[tokio::test]
async fn public_identity_sees_only_public_docs() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy1:main").await;

    let signing_key = SigningKey::from_bytes(&[1u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/public-user",
        "policy1:main",
    );

    let (status, json) = query_docs(app, "policy1:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        1,
        "public user should see exactly 1 document; got: {names:?}"
    );
    assert!(
        names.contains(&"Public Post"),
        "expected 'Public Post'; got: {names:?}"
    );
}

/// Employee identity sees public + internal documents, but not confidential.
///
/// This is the primary regression test for issue #106: `execute_query` was
/// building a plain `GraphDb` and discarding `opts.identity`, so no policy
/// filtering was applied and all 3 documents were returned.
#[tokio::test]
async fn employee_identity_sees_public_and_internal() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy2:main").await;

    let signing_key = SigningKey::from_bytes(&[2u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "policy2:main",
    );

    let (status, json) = query_docs(app, "policy2:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        2,
        "employee should see exactly 2 documents; got: {names:?}"
    );
    assert!(
        names.contains(&"Public Post"),
        "expected 'Public Post'; got: {names:?}"
    );
    assert!(
        names.contains(&"Internal Memo"),
        "expected 'Internal Memo'; got: {names:?}"
    );
    assert!(
        !names.contains(&"Executive Salaries"),
        "employee must NOT see 'Executive Salaries'; got: {names:?}"
    );
}

/// Manager identity has `f:allow: true` and should see all documents.
#[tokio::test]
async fn manager_identity_sees_all_documents() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy3:main").await;

    let signing_key = SigningKey::from_bytes(&[3u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/manager-user",
        "policy3:main",
    );

    let (status, json) = query_docs(app, "policy3:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        3,
        "manager should see all 3 documents; got: {names:?}"
    );
}

/// An identity with no policyClass node in the ledger and `default-allow: false`
/// should see nothing — fail-closed.
///
/// Regression test for issue #106 Bug 2: the server was returning a 500 when
/// looking up an identity IRI that had no subject node in the ledger.
#[tokio::test]
async fn identity_without_policy_class_default_allow_false_denies_all() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy4:main").await;

    let signing_key = SigningKey::from_bytes(&[4u8; 32]);
    // This identity IRI has no node in the ledger — no f:policyClass binding
    let token = identity_token(
        &signing_key,
        "http://example.org/unknown-user",
        "policy4:main",
    );

    let (status, json) = query_docs(app, "policy4:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert!(
        names.is_empty(),
        "unknown identity + default-allow:false must see nothing; got: {names:?}"
    );
}

/// An identity with no subject node in the ledger must be allowed when
/// `default-allow: true` is set.
///
/// Rationale: `default-allow: true` is an explicit admin opt-in that applies to
/// every requester not specifically restricted by a policy — including unknown
/// identities. A common pattern is an application layer in front of the DB that
/// handles authorization itself and uses credential-signed writes only so that
/// Fluree records *who* transacted for provenance. In that setup a first-time DID
/// must be able to transact without being pre-provisioned, and reads must follow
/// the same opt-in. Callers who want fail-closed behavior set `default-allow: false`.
#[tokio::test]
async fn unknown_identity_allowed_with_default_allow_true() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy5:main").await;

    let signing_key = SigningKey::from_bytes(&[5u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/unknown-user",
        "policy5:main",
    );

    let (status, json) = query_docs(app, "policy5:main", Some(&token), true).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        3,
        "unknown identity + default-allow:true must see all documents; got: {names:?}"
    );
}

/// A property-level `f:allow: false` on `ex:content` should strip that field
/// from results for the employee identity. Documents that pass the row-level
/// filter should still appear, but without the `ex:content` binding.
#[tokio::test]
async fn property_level_deny_hides_ex_content_field() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy6:main").await;

    // Add a property-level deny policy for ex:content on the employee class
    let deny_tx = serde_json::json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        },
        "insert": [
            {
                "@id": "ex:employee-deny-content",
                "@type": ["f:AccessPolicy", "ex:EmployeeClass"],
                "f:action": [{"@id": "f:view"}],
                "f:onProperty": [{"@id": "ex:content"}],
                "f:allow": false
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/policy6:main")
                .header("content-type", "application/json")
                .body(Body::from(deny_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert content deny policy");

    let signing_key = SigningKey::from_bytes(&[6u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "policy6:main",
    );

    // Query 1: without ex:content in WHERE — verifies row-level policy still works.
    // Employee should see public + internal docs (2), not the confidential one.
    let row_level_body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": false },
        "select": ["?name", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/policy6:main")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(row_level_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json.as_array().expect("results should be array");
    let names: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_str()))
        .collect();
    assert!(
        !names.contains(&"Executive Salaries"),
        "employee must NOT see confidential doc; got: {names:?}"
    );
    assert_eq!(
        rows.len(),
        2,
        "employee should see 2 documents; got: {names:?}"
    );

    // Query 2: with ex:content as a required triple pattern — verifies property-level deny.
    // Because ex:content is property-denied for EmployeeClass, the triple pattern
    // `?doc ex:content ?content` never matches, so the entire query returns 0 rows.
    // This is the expected fail-closed behavior: denied properties are invisible,
    // so required patterns on them produce no results.
    let property_deny_body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": false },
        "select": ["?name", "?class", "?content"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"},
            {"@id": "?doc", "ex:content": "?content"}
        ]
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/policy6:main")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(property_deny_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    let rows_with_content = json.as_array().expect("results should be array");
    assert_eq!(
        rows_with_content.len(),
        0,
        "property deny on ex:content must cause required triple pattern to match nothing; got: {rows_with_content:?}"
    );
}

/// A **known** identity subject (exists in the ledger) with no `f:policyClass` and
/// `default-allow: true` should see all documents.
///
/// This validates the `FoundNoPolicies` path: the identity subject node is present in the
/// ledger (so it is not `NotFound`), but it carries no policy class binding. With no
/// restrictions and `default_allow = true`, access is granted to everything.
///
/// Contrast with `unknown_identity_allowed_with_default_allow_true` which uses an
/// identity that has NO subject node at all (`NotFound`) — that path also now honors
/// `default_allow`, so both tests succeed for the same reason: empty restrictions +
/// permissive default.
#[tokio::test]
async fn known_identity_no_policy_class_default_allow_true_allows_all() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "policy7:main").await;

    // Insert a registered identity node that EXISTS in the ledger but has no f:policyClass.
    let register_tx = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "insert": { "@id": "http://example.org/registered-user", "ex:role": "guest" }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/policy7:main")
                .header("content-type", "application/json")
                .body(Body::from(register_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert registered-user node");

    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/registered-user",
        "policy7:main",
    );

    let (status, json) = query_docs(app, "policy7:main", Some(&token), true).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        3,
        "known identity with no policyClass + default-allow:true must see all 3 docs; got: {names:?}"
    );
}

// ── Root-bearer impersonation tests ──────────────────────────────────────────
//
// These exercise the "service-account impersonation" pattern: a bearer
// identity that has no `f:policyClass` on the target ledger may delegate to a
// body- or header-supplied target identity for policy testing. The check is
// performed via `fluree_db_api::identity_has_no_policies` — only the
// `FoundNoPolicies` outcome enables impersonation.

/// Insert a registered identity subject with no `f:policyClass` so it qualifies
/// as a root-equivalent (FoundNoPolicies) identity for impersonation.
async fn register_root_identity(app: &axum::Router, ledger: &str, identity_iri: &str) {
    let tx = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "insert": { "@id": identity_iri, "ex:role": "service-account" }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "insert root identity {identity_iri}"
    );
}

/// Query as `bearer_identity`, requesting impersonation as `target_identity`
/// via body `opts.identity`. Server should honor when bearer is root.
async fn query_docs_as(
    app: axum::Router,
    ledger: &str,
    bearer_token: &str,
    target_identity: &str,
) -> (StatusCode, JsonValue) {
    let body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "identity": target_identity },
        "select": ["?name", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{ledger}"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {bearer_token}"));

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();

    json_body(resp).await
}

/// A root bearer (no f:policyClass) can impersonate the employee identity and
/// receives the employee's filtered view (public + internal docs only).
#[tokio::test]
async fn root_bearer_can_impersonate_employee_via_body_opts() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "imp1:main").await;
    register_root_identity(&app, "imp1:main", "http://example.org/svc-bearer").await;

    let signing_key = SigningKey::from_bytes(&[10u8; 32]);
    let token = identity_token(&signing_key, "http://example.org/svc-bearer", "imp1:main");

    let (status, json) =
        query_docs_as(app, "imp1:main", &token, "http://example.org/employee-user").await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        2,
        "impersonating employee should see exactly 2 docs; got: {names:?}"
    );
    assert!(names.contains(&"Public Post"));
    assert!(names.contains(&"Internal Memo"));
    assert!(!names.contains(&"Executive Salaries"));
}

/// A restricted bearer (employee) attempting impersonation has the
/// `opts.identity` force-overridden by the server back to its own bearer
/// identity — it sees its own filtered view, NOT the target's.
#[tokio::test]
async fn restricted_bearer_cannot_impersonate_manager() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "imp2:main").await;

    let signing_key = SigningKey::from_bytes(&[11u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "imp2:main",
    );

    // Employee tries to impersonate manager — should be force-overridden.
    let (status, json) =
        query_docs_as(app, "imp2:main", &token, "http://example.org/manager-user").await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        2,
        "restricted bearer should see employee view (2 docs), not manager view (3); got: {names:?}"
    );
    assert!(!names.contains(&"Executive Salaries"));
}

/// Root bearer impersonates via the `fluree-identity` HTTP header on a SPARQL
/// query; result set matches the impersonated identity's policy.
#[tokio::test]
async fn root_bearer_can_impersonate_via_sparql_header() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "imp3:main").await;
    register_root_identity(&app, "imp3:main", "http://example.org/svc-bearer-sparql").await;

    let signing_key = SigningKey::from_bytes(&[12u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/svc-bearer-sparql",
        "imp3:main",
    );

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { \
                    ?doc a ex:Document ; schema:name ?name . \
                  }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/imp3:main")
        .header("content-type", "application/sparql-query")
        .header("authorization", format!("Bearer {token}"))
        .header("fluree-identity", "http://example.org/public-user");

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // SPARQL results bindings format: {"results": {"bindings": [{"name": {"value": "..."}}]}}
    let bindings = json
        .pointer("/results/bindings")
        .and_then(|v| v.as_array())
        .expect("expected SPARQL results.bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .filter_map(|b| b.pointer("/name/value").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        names.len(),
        1,
        "impersonated public-user should see 1 doc via SPARQL; got: {names:?}"
    );
    assert_eq!(names[0], "Public Post");
}

/// Restricted bearer attempting SPARQL impersonation via header is force-overridden.
#[tokio::test]
async fn restricted_bearer_cannot_impersonate_via_sparql_header() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "imp4:main").await;

    let signing_key = SigningKey::from_bytes(&[13u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "imp4:main",
    );

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { \
                    ?doc a ex:Document ; schema:name ?name . \
                  }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/imp4:main")
        .header("content-type", "application/sparql-query")
        .header("authorization", format!("Bearer {token}"))
        .header("fluree-identity", "http://example.org/manager-user");

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let bindings = json
        .pointer("/results/bindings")
        .and_then(|v| v.as_array())
        .expect("expected SPARQL results.bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .filter_map(|b| b.pointer("/name/value").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        names.len(),
        2,
        "restricted SPARQL bearer should see employee view (2), not manager (3); got: {names:?}"
    );
    assert!(!names.contains(&"Executive Salaries"));
}

// ── Inline policy and policy-values tests ────────────────────────────────────
//
// These exercise the ad-hoc policy CLI flags (`--policy` / `--policy-file` and
// `--policy-values` / `--policy-values-file`) via their on-the-wire forms:
// body `opts.policy` / `opts.policy-values` for JSON-LD, and
// `fluree-policy` / `fluree-policy-values` headers for SPARQL. These are the
// "test policies before persisting them" path.

/// Helper: extract names from SPARQL results bindings.
fn sparql_names(json: &JsonValue) -> Vec<&str> {
    json.pointer("/results/bindings")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|b| b.pointer("/name/value").and_then(|v| v.as_str()))
                .collect()
        })
        .unwrap_or_default()
}

/// Helper: extract names from a JSON-LD select result. Single-column selects
/// return a flat string array; multi-column selects return arrays of arrays.
/// This handles both shapes.
fn jsonld_select_names(json: &JsonValue) -> Vec<&str> {
    let arr = json.as_array().expect("results should be an array");
    arr.iter()
        .filter_map(|row| match row {
            JsonValue::String(s) => Some(s.as_str()),
            JsonValue::Array(cols) => cols.first().and_then(|v| v.as_str()),
            _ => None,
        })
        .collect()
}

/// Helper: build a root bearer token (identity has no f:policyClass on the
/// ledger, so it qualifies for the impersonation gate when needed).
async fn root_bearer(app: &axum::Router, ledger: &str, key_byte: u8, identity: &str) -> String {
    register_root_identity(app, ledger, identity).await;
    let signing_key = SigningKey::from_bytes(&[key_byte; 32]);
    identity_token(&signing_key, identity, ledger)
}

/// Inline JSON-LD policy supplied via `opts.policy` filters results to public
/// documents only — verifies the `--policy` flag's body-opts transport.
#[tokio::test]
async fn inline_policy_via_body_opts_filters_to_public() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "inline1:main").await;
    let token = root_bearer(&app, "inline1:main", 20, "http://example.org/inline-svc").await;

    // Inline policy: only documents with classification = "public" are visible.
    let inline_policy = serde_json::json!([
        {
            "@id": "ex:adhocPublicOnly",
            "@type": "f:AccessPolicy",
            "f:action": [{"@id": "f:view"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/"},
                    "where": [{"@id": "?$this", "ex:classification": "public"}]
                }
            }
        }
    ]);

    let body = serde_json::json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "opts": { "policy": inline_policy, "default-allow": false },
        "select": ["?name"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"}
        ]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/inline1:main")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = jsonld_select_names(&json);
    assert_eq!(
        names.len(),
        1,
        "inline policy should restrict to 1 public doc; got: {names:?}"
    );
    assert_eq!(names[0], "Public Post");
}

/// Inline policy supplied via the `fluree-policy` header on a SPARQL query —
/// verifies the `--policy` flag's header transport for SPARQL.
///
/// Unauthenticated request: the policy_builder uses identity > policy_class >
/// policy as a priority chain (mutually exclusive), so to exercise the inline
/// policy path the test omits the bearer token. `DataAuthMode::Optional` in
/// the test config allows this.
#[tokio::test]
async fn inline_policy_via_header_filters_sparql() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "inline2:main").await;

    let inline_policy = serde_json::json!([
        {
            "@id": "ex:adhocInternalOnly",
            "@type": "f:AccessPolicy",
            "f:action": [{"@id": "f:view"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/"},
                    "where": [{"@id": "?$this", "ex:classification": "internal"}]
                }
            }
        }
    ]);

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?doc a ex:Document ; schema:name ?name . }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/inline2:main")
        .header("content-type", "application/sparql-query")
        .header("fluree-policy", inline_policy.to_string())
        .header("fluree-default-allow", "false");

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = sparql_names(&json);
    assert_eq!(
        names.len(),
        1,
        "header-supplied inline policy should filter SPARQL to internal-only; got: {names:?}"
    );
    assert_eq!(names[0], "Internal Memo");
}

/// Insert `ex:assignedTo` relationships from each document to one of the
/// identity nodes — used by the policy-values tests below to demonstrate
/// `?$identity` binding.
async fn assign_docs_to_identities(app: &axum::Router, ledger: &str) {
    let tx = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "insert": [
            { "@id": "ex:doc1", "ex:assignedTo": { "@id": "http://example.org/public-user" } },
            { "@id": "ex:doc2", "ex:assignedTo": { "@id": "http://example.org/employee-user" } },
            { "@id": "ex:doc3", "ex:assignedTo": { "@id": "http://example.org/manager-user" } }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/upsert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "assign docs to identities");
}

/// `opts.policy-values` binds `?$identity` for an inline policy that filters
/// by document assignment. Verifies the JSON-LD body-opts transport for
/// `--policy-values`.
#[tokio::test]
async fn policy_values_substitute_into_inline_policy() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "pv1:main").await;
    assign_docs_to_identities(&app, "pv1:main").await;

    // Parameterized policy: a document is viewable if `?$this`'s
    // `ex:assignedTo` equals the bound `?$identity`.
    let inline_policy = serde_json::json!([
        {
            "@id": "ex:adhocByAssignment",
            "@type": "f:AccessPolicy",
            "f:action": [{"@id": "f:view"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/"},
                    "where": [{"@id": "?$this", "ex:assignedTo": "?$identity"}]
                }
            }
        }
    ]);

    let body = serde_json::json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "opts": {
            "policy": inline_policy,
            "policy-values": { "?$identity": { "@id": "http://example.org/employee-user" } },
            "default-allow": false
        },
        "select": ["?name"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"}
        ]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/pv1:main")
        .header("content-type", "application/json");

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = jsonld_select_names(&json);
    assert_eq!(
        names.len(),
        1,
        "?$identity=employee-user should yield only doc2; got: {names:?}"
    );
    assert_eq!(names[0], "Internal Memo");
}

/// Same as above but transported via `fluree-policy` + `fluree-policy-values`
/// headers on a SPARQL query.
#[tokio::test]
async fn policy_values_via_header_for_sparql() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "pv2:main").await;
    assign_docs_to_identities(&app, "pv2:main").await;

    let inline_policy = serde_json::json!([
        {
            "@id": "ex:adhocByAssignment",
            "@type": "f:AccessPolicy",
            "f:action": [{"@id": "f:view"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/"},
                    "where": [{"@id": "?$this", "ex:assignedTo": "?$identity"}]
                }
            }
        }
    ]);
    let policy_values = serde_json::json!({
        "?$identity": { "@id": "http://example.org/manager-user" }
    });

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?doc a ex:Document ; schema:name ?name . }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/pv2:main")
        .header("content-type", "application/sparql-query")
        .header("fluree-policy", inline_policy.to_string())
        .header("fluree-policy-values", policy_values.to_string())
        .header("fluree-default-allow", "false");

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = sparql_names(&json);
    assert_eq!(
        names.len(),
        1,
        "?$identity=manager-user binding via headers should yield 1 doc; got: {names:?}"
    );
    assert_eq!(names[0], "Executive Salaries");
}

/// Repeated `fluree-policy-class` headers accumulate into a multi-class set —
/// verifies the multi-value transport that `--policy-class` (repeatable) needs
/// for SPARQL parity with JSON-LD body opts.
///
/// Sends both `ex:PublicClass` and `ex:EmployeeClass`. The persisted policies
/// for those classes are `{public}` and `{public, internal}` respectively;
/// applying them as a class-set yields `{public, internal}` (the union of
/// what either class permits).
/// As above. The policy_builder uses identity > policy_class > policy as a
/// priority chain, so to exercise the multi-class header path the request
/// omits the bearer token.
#[tokio::test]
async fn multi_value_policy_class_via_repeated_sparql_headers() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "pc:main").await;

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?doc a ex:Document ; schema:name ?name . }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/pc:main")
        .header("content-type", "application/sparql-query")
        .header("fluree-policy-class", "http://example.org/PublicClass")
        .header("fluree-policy-class", "http://example.org/EmployeeClass");

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = sparql_names(&json);
    assert!(
        names.contains(&"Public Post") && names.contains(&"Internal Memo"),
        "two policy-class headers should union into public+internal access; got: {names:?}"
    );
    assert!(
        !names.contains(&"Executive Salaries"),
        "confidential should remain hidden; got: {names:?}"
    );
}

/// Comma-separated `fluree-policy-class` value parses into multiple classes
/// (alternative wire form to repeated headers).
#[tokio::test]
async fn comma_separated_policy_class_header() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "pc2:main").await;

    let sparql = "PREFIX ex: <http://example.org/> \
                  PREFIX schema: <http://schema.org/> \
                  SELECT ?name WHERE { ?doc a ex:Document ; schema:name ?name . }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/query/pc2:main")
        .header("content-type", "application/sparql-query")
        .header(
            "fluree-policy-class",
            "http://example.org/PublicClass, http://example.org/EmployeeClass",
        );

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    let names = sparql_names(&json);
    assert!(names.contains(&"Public Post") && names.contains(&"Internal Memo"));
    assert!(!names.contains(&"Executive Salaries"));
}

// ── Write-policy enforcement tests ───────────────────────────────────────────
//
// These validate the end-to-end policy path for transactions:
//   bearer identity → `apply_auth_identity_to_opts`
//   → `fluree_db_api::build_policy_context`
//   → `TxBuilder.policy(ctx)` → `Fluree::transact_tracked_with_policy`
//
// Covers JSON-LD `/v1/fluree/update`, SPARQL UPDATE, and the impersonation
// gate's behavior on writes. The setup installs a required `f:modify` gate on
// the employee class (denies modifying `ex:content` unless the target doc's
// classification is "internal") and a blanket `f:allow: true` modify policy on
// the manager class, so both the denial and the allow paths exercise real
// policy evaluation rather than a no-policy fallthrough.

/// Bearer token with both read and write scopes for `ledger`.
fn identity_token_rw(signing_key: &SigningKey, identity: &str, ledger: &str) -> String {
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.identity": identity,
        "fluree.ledger.read.ledgers": [ledger],
        "fluree.ledger.write.ledgers": [ledger],
    });
    create_jws(&claims, signing_key)
}

/// Insert modify policies on top of the view-only setup:
/// - `ex:employee-modify-deny`: employees may not modify `ex:content` on any
///   document. Carries a custom `f:exMessage` surfaced on denial.
/// - `ex:manager-modify-allow`: blanket `f:allow: true` modify for managers.
async fn add_modify_policies(app: &axum::Router, ledger: &str) {
    let policy_tx = serde_json::json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        },
        "insert": [
            {
                "@id": "ex:employee-modify-deny",
                "@type": ["f:AccessPolicy", "ex:EmployeeClass"],
                "f:onProperty": [{"@id": "ex:content"}],
                "f:action": [{"@id": "f:modify"}],
                "f:exMessage": "Employees may not modify document content.",
                "f:allow": false
            },
            {
                "@id": "ex:manager-modify-allow",
                "@type": ["f:AccessPolicy", "ex:ManagerClass"],
                "f:action": [{"@id": "f:modify"}],
                "f:allow": true
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/insert/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(policy_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert modify policies");
}

/// WHERE/DELETE/INSERT body that rewrites `ex:doc1`'s `ex:content`. The
/// employee deny policy blocks all `ex:content` modifications for that class;
/// managers bypass via their blanket modify-allow policy.
fn modify_public_doc_content_body() -> JsonValue {
    serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "ex:doc1", "ex:content": "?c"},
        "delete": {"@id": "ex:doc1", "ex:content": "?c"},
        "insert": {"@id": "ex:doc1", "ex:content": "rewritten"}
    })
}

/// An employee bearer attempting to rewrite a public document's `ex:content`
/// is denied by the required modify gate. The response is HTTP 400 and the
/// `error` field carries the custom `f:exMessage` verbatim.
#[tokio::test]
async fn employee_bearer_update_denied_with_ex_message() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "wpol1:main").await;
    add_modify_policies(&app, "wpol1:main").await;

    let signing_key = SigningKey::from_bytes(&[30u8; 32]);
    let token = identity_token_rw(
        &signing_key,
        "http://example.org/employee-user",
        "wpol1:main",
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/update/wpol1:main")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));

    let resp = app
        .oneshot(
            req.body(Body::from(modify_public_doc_content_body().to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "employee write must be rejected; got body: {json}"
    );
    let err_msg = json
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        err_msg.contains("Employees may not modify document content."),
        "expected custom f:exMessage in error; got: {err_msg}"
    );
}

/// Control: a manager bearer can rewrite the same document — the manager
/// class has a blanket `f:allow: true` modify policy, so the gate's required
/// constraint (which only applies to the employee class) never fires.
/// Demonstrates the setup isn't trivially denying all writes.
#[tokio::test]
async fn manager_bearer_update_allowed() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "wpol2:main").await;
    add_modify_policies(&app, "wpol2:main").await;

    let signing_key = SigningKey::from_bytes(&[31u8; 32]);
    let token = identity_token_rw(
        &signing_key,
        "http://example.org/manager-user",
        "wpol2:main",
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/update/wpol2:main")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));

    let resp = app
        .oneshot(
            req.body(Body::from(modify_public_doc_content_body().to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "manager write must succeed; got body: {json}"
    );
}

/// A root bearer (no `f:policyClass`) impersonating the employee identity via
/// `opts.identity` is rejected. Policy enforcement follows the impersonated
/// identity, not the bearer's own unrestricted service-account identity —
/// impersonation doesn't bypass policy, it tests under the target's view.
#[tokio::test]
async fn root_bearer_impersonating_employee_update_denied() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "wpol3:main").await;
    add_modify_policies(&app, "wpol3:main").await;
    register_root_identity(&app, "wpol3:main", "http://example.org/svc-writer").await;

    let signing_key = SigningKey::from_bytes(&[32u8; 32]);
    let token = identity_token_rw(&signing_key, "http://example.org/svc-writer", "wpol3:main");

    let mut body = modify_public_doc_content_body();
    body.as_object_mut().unwrap().insert(
        "opts".to_string(),
        serde_json::json!({"identity": "http://example.org/employee-user"}),
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/update/wpol3:main")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "impersonated employee write must be rejected; got body: {json}"
    );
    let err_msg = json
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        err_msg.contains("Employees may not modify document content."),
        "expected exMessage in error; got: {err_msg}"
    );
}

/// SPARQL UPDATE under an employee bearer is subject to the same modify gate
/// as the JSON-LD path. Confirms end-to-end enforcement on the
/// `application/sparql-update` transport: the server builds a PolicyContext
/// from the bearer-derived identity and routes through the policy-enforcing
/// transact path, not a root bypass.
#[tokio::test]
async fn sparql_update_under_employee_bearer_denied() {
    let (_tmp, state) = policy_test_state().await;
    let app = setup_policy_ledger(build_router(state), "wpol4:main").await;
    add_modify_policies(&app, "wpol4:main").await;

    let signing_key = SigningKey::from_bytes(&[33u8; 32]);
    let token = identity_token_rw(
        &signing_key,
        "http://example.org/employee-user",
        "wpol4:main",
    );

    let sparql = "PREFIX ex: <http://example.org/> \
                  DELETE { ex:doc1 ex:content ?c } \
                  INSERT { ex:doc1 ex:content \"rewritten\" } \
                  WHERE  { ex:doc1 ex:content ?c }";

    let req = Request::builder()
        .method("POST")
        .uri("/v1/fluree/update/wpol4:main")
        .header("content-type", "application/sparql-update")
        .header("authorization", format!("Bearer {token}"));

    let resp = app
        .oneshot(req.body(Body::from(sparql)).unwrap())
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "employee SPARQL UPDATE must be rejected; got body: {json}"
    );
    let err_msg = json
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        err_msg.contains("Employees may not modify document content."),
        "expected exMessage in error; got: {err_msg}"
    );
}
