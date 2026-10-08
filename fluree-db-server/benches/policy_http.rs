//! Warm, in-process HTTP requests, including credential verification, policy
//! binding, config resolution, execution and response serialization. Socket/TLS
//! costs are excluded. All modes read the same one-row result from novelty.
//! Run with `cargo bench -p fluree-db-server --bench policy_http`.

use axum::{body::Body, Router};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use criterion::{criterion_group, criterion_main, Criterion};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_server::{
    config::DataAuthMode, routes::build_router, AppState, ServerConfig, TelemetryConfig,
};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

const LEDGER: &str = "bench-policy:main";
const CLASS: &str = "http://example.org/Reader";

fn token(key: &SigningKey, policy: Option<Value>) -> String {
    let public = key.verifying_key().to_bytes();
    let mut claims = json!({
        "iss": fluree_db_credential::did_from_pubkey(&public),
        "aud": "policy-bench", "sub": "http://example.org/user",
        "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        "fluree.ledger.read.ledgers": [LEDGER]
    });
    if let Some(policy) = policy {
        claims["fluree.policy"] = policy;
    }
    let header = json!({"alg": "EdDSA", "jwk": {
        "kty": "OKP", "crv": "Ed25519", "x": URL_SAFE_NO_PAD.encode(public)
    }});
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let signature = key.sign(input.as_bytes());
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

async fn app(mode: DataAuthMode, key: &SigningKey) -> (tempfile::TempDir, Router) {
    let directory = tempfile::tempdir().unwrap();
    let config = ServerConfig {
        storage_path: Some(directory.path().to_owned()),
        indexing_enabled: false,
        cors_enabled: false,
        data_auth_mode: mode,
        data_auth_audience: Some("policy-bench".into()),
        data_auth_policy_authorities: vec![fluree_db_credential::did_from_pubkey(
            &key.verifying_key().to_bytes(),
        )],
        data_auth_default_policy_class: Some(CLASS.into()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&config);
    let state = Arc::new(AppState::new(config, telemetry).await.unwrap());
    let ledger = state.fluree.create_ledger(LEDGER).await.unwrap();
    let mut data: Vec<Value> = (0..256)
        .map(|i| {
            json!({
                "@id": format!("http://example.org/person/{i}"),
                "http://example.org/name": format!("Person {i}")
            })
        })
        .collect();
    data.push(json!({
        "@id": "http://example.org/read", "@type": CLASS,
        "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
        "https://ns.flur.ee/db#allow": true
    }));
    state
        .fluree
        .insert(ledger, &json!({"@graph": data}))
        .await
        .unwrap();
    (directory, build_router(state))
}

async fn request(app: &Router, token: Option<&str>, body: &str) -> Vec<u8> {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{LEDGER}"))
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

fn bench_http(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let key = SigningKey::from_bytes(&[201; 32]);
    let (_plain_dir, plain) = runtime.block_on(app(DataAuthMode::None, &key));
    let (_auth_dir, authenticated) = runtime.block_on(app(DataAuthMode::Required, &key));
    let query = json!({
        "select": ["?name"],
        "where": {"@id": "http://example.org/person/0", "http://example.org/name": "?name"}
    });
    let mut selected = query.clone();
    selected["opts"] = json!({"policy-class": [CLASS]});
    let inline: Vec<_> = (0..32)
        .map(|i| {
            json!({
                "@id": format!("http://example.org/rule/{i}"),
                "f:action": "f:view", "f:allow": true
            })
        })
        .collect();
    let cases = [
        ("no_auth", &plain, None, query.to_string()),
        (
            "ordinary",
            &authenticated,
            Some(token(&key, None)),
            query.to_string(),
        ),
        (
            "fixed",
            &authenticated,
            Some(token(&key, Some(json!({"policy-class": [CLASS]})))),
            query.to_string(),
        ),
        (
            "request",
            &authenticated,
            Some(token(&key, Some(json!("request")))),
            selected.to_string(),
        ),
        (
            "fixed_inline_32",
            &authenticated,
            Some(token(
                &key,
                Some(json!({"policy-class": [CLASS], "policy": inline})),
            )),
            query.to_string(),
        ),
    ];
    let mut group = c.benchmark_group("policy_http");
    for (name, app, token, body) in &cases {
        // Warm caches and require identical effective access before timing.
        let bytes = runtime.block_on(request(app, token.as_deref(), body));
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!([["Person 0"]]),
            "{name}"
        );
        group.bench_function(*name, |b| {
            b.to_async(&runtime)
                .iter(|| request(app, token.as_deref(), body));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_http);
criterion_main!(benches);
