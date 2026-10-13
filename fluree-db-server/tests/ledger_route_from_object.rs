//! On the ledger-scoped JSON-LD routes, a single `from` object names one
//! source, by `@id` or else `id`. Every spelling of the route's ledger is
//! accepted, no spelling of another ledger is, and an object that names no
//! source is refused in every execution lane.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const OWN: &str = "own:main";
const OTHER: &str = "other:main";

async fn post(
    app: &axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
    body: String,
) -> (StatusCode, String) {
    let mut req = Request::builder().method("POST").uri(uri);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Two ledgers with one named subject each: `Own` in the route's ledger,
/// `Other` in the other one.
async fn fixture() -> (TempDir, axum::Router) {
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
    for (ledger, id, name) in [(OWN, "ex:o", "Own"), (OTHER, "ex:x", "Other")] {
        let (status, body) = post(
            &app,
            "/v1/fluree/create",
            &[("content-type", "application/json")],
            json!({ "ledger": ledger }).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let insert = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": id,
            "ex:name": name
        });
        let (status, body) = post(
            &app,
            "/v1/fluree/insert",
            &[
                ("content-type", "application/json"),
                ("fluree-ledger", ledger),
            ],
            insert.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    (tmp, app)
}

/// The query, with `dataset` supplying its dataset keys. `opts.default-allow`
/// sends it down the policy-aware dataset lane rather than the plain one.
fn query(dataset: &JsonValue, dataset_lane: bool) -> JsonValue {
    let mut q = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let obj = q.as_object_mut().unwrap();
    for (k, v) in dataset.as_object().unwrap() {
        obj.insert(k.clone(), v.clone());
    }
    if dataset_lane {
        obj.insert("opts".into(), json!({ "default-allow": true }));
    }
    q
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Expect {
    /// 200 with only the route ledger's row.
    Own,
    /// 200 with no rows.
    NoRows,
    /// 400 "Ledger mismatch".
    Mismatch,
    /// 400: the object names no source.
    NoSource,
}

fn observed(status: StatusCode, body: &str) -> Expect {
    match status {
        StatusCode::OK => {
            assert!(
                !body.contains("\"Other\""),
                "another ledger's row came back: {body}"
            );
            if body.contains("\"Own\"") {
                Expect::Own
            } else {
                Expect::NoRows
            }
        }
        StatusCode::BAD_REQUEST if body.contains("Ledger mismatch") => Expect::Mismatch,
        StatusCode::BAD_REQUEST => Expect::NoSource,
        other => panic!("unexpected {other}: {body}"),
    }
}

fn cases() -> Vec<(JsonValue, Expect)> {
    use Expect::*;
    vec![
        // Controls.
        (json!({}), Own),
        (json!({ "from": "own:main" }), Own),
        (json!({ "from": "other:main" }), Mismatch),
        // `@id`: every spelling of the route's ledger reads it.
        (json!({ "from": { "@id": "own:main" } }), Own),
        (json!({ "from": { "@id": "own" } }), Own),
        (json!({ "from": { "@id": "urn:fluree:own:main" } }), Own),
        (json!({ "from": { "@id": "own:main", "t": 1 } }), Own),
        (
            json!({ "from": { "@id": "own:main", "graph": "txn-meta" } }),
            NoRows,
        ),
        (json!({ "from": { "@id": "own:main", "unknown": 1 } }), Own),
        (json!({ "from": { "@id": "other:main" } }), Mismatch),
        (
            json!({ "from": { "@id": "urn:fluree:other:main" } }),
            Mismatch,
        ),
        // `id`, when there is no `@id`.
        (json!({ "from": { "id": "own:main" } }), Own),
        (json!({ "from": { "id": "own" } }), Own),
        (json!({ "from": { "id": "own:main", "t": 1 } }), Own),
        (json!({ "from": { "id": "other:main" } }), Mismatch),
        (json!({ "from": { "id": "other:main", "t": 1 } }), Mismatch),
        (
            json!({ "from": { "id": "urn:fluree:other:main" } }),
            Mismatch,
        ),
        (
            json!({ "from": { "id": "other:main", "unknown": 1 } }),
            Mismatch,
        ),
        // Both keys: `@id` names the source.
        (
            json!({ "from": { "@id": "own:main", "id": "other:main" } }),
            Own,
        ),
        (
            json!({ "from": { "id": "own:main", "@id": "other:main" } }),
            Mismatch,
        ),
        // An object that names no source.
        (json!({ "from": {} }), NoSource),
        (json!({ "from": { "graph": "txn-meta" } }), NoSource),
        (json!({ "from": { "unknown": "other:main" } }), NoSource),
        (json!({ "from": { "@id": 5 } }), NoSource),
    ]
}

#[tokio::test]
async fn ledger_route_from_object_is_read_as_the_parser_reads_it() {
    let (_tmp, app) = fixture().await;
    let uri = format!("/v1/fluree/query/{OWN}");
    for (dataset, expect) in cases() {
        for dataset_lane in [false, true] {
            let (status, body) = post(
                &app,
                &uri,
                &[("content-type", "application/json")],
                query(&dataset, dataset_lane).to_string(),
            )
            .await;
            assert_eq!(
                observed(status, &body),
                expect,
                "{dataset} (dataset lane: {dataset_lane}): {status} {body}"
            );
        }
    }
}

/// The stream and explain routes read a `from` object the same way.
#[tokio::test]
async fn ledger_route_from_object_check_covers_stream_and_explain() {
    let (_tmp, app) = fixture().await;
    for route in ["stream/query", "explain"] {
        let uri = format!("/v1/fluree/{route}/{OWN}");
        let (status, body) = post(
            &app,
            &uri,
            &[("content-type", "application/json")],
            query(&json!({ "from": { "id": OTHER } }), false).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{route}: {body}");
        assert!(body.contains("Ledger mismatch"), "{route}: {body}");

        let (status, body) = post(
            &app,
            &uri,
            &[("content-type", "application/json")],
            query(&json!({ "from": { "id": OWN } }), false).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        assert!(!body.contains("\"Other\""), "{route}: {body}");
    }
}
