//! Integration tests for the `/v1/fluree/multi-query` envelope endpoint.
//!
//! Each test stands up a fresh in-process server, creates one or two
//! ledgers, inserts a small fixture, and exercises the envelope through a
//! single `oneshot` HTTP request. The fixture stays tiny — these tests
//! cover the envelope contract (shape, status, snapshot echo, error
//! mapping), not query execution; per-feature query coverage lives in
//! `it_query.rs` and `it_query_sparql.rs` in the api crate.

use axum::body::Body;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

async fn test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    (tmp, state)
}

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, json)
}

async fn create_ledger(app: &axum::Router, ledger: &str) {
    let body = json!({ "ledger": ledger });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create {ledger} should return 201"
    );
}

async fn insert_one(app: &axum::Router, ledger: &str, id: &str, name: &str) {
    let body = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": id,
        "ex:name": name
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", ledger)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "insert into {ledger} should return 200"
    );
}

async fn post_envelope(app: &axum::Router, envelope: &JsonValue) -> (StatusCode, JsonValue) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/multi-query")
                .header("content-type", "application/json")
                .body(Body::from(envelope.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

// =============================================================================
// Happy path
// =============================================================================

#[tokio::test]
async fn multi_query_all_jsonld_succeeds() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:a").await;
    insert_one(&app, "mq:a", "ex:alice", "Alice").await;

    let envelope = json!({
        "queries": {
            "alice": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:a",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            },
            "alice2": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:a",
                    "selectOne": "?id",
                    "where": { "@id": "?id", "ex:name": "Alice" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
    assert!(body["results"]["alice"].is_array());
    assert!(body["results"]["alice2"].is_string() || body["results"]["alice2"].is_array());
    assert!(body["errors"].is_null() || body["errors"].as_object().unwrap().is_empty());
}

#[tokio::test]
async fn multi_query_mixed_language_succeeds() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:mixed").await;
    insert_one(&app, "mq:mixed", "ex:bob", "Bob").await;

    let envelope = json!({
        "queries": {
            "jsonld_q": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:mixed",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            },
            "sparql_q": {
                "language": "sparql",
                "query": "PREFIX ex: <http://example.org/> SELECT ?name FROM <mq:mixed> WHERE { ?s ex:name ?name }"
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
    // Both aliases must have results; envelope guarantees they ran against
    // the same snapshot.
    assert!(body["results"]["jsonld_q"].is_array());
    assert!(body["results"]["sparql_q"].is_object() || body["results"]["sparql_q"].is_array());
}

#[tokio::test]
async fn multi_query_shared_context_lifts_to_jsonld_subqueries() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:ctx").await;
    insert_one(&app, "mq:ctx", "ex:carol", "Carol").await;

    // Envelope-level @context — sub-queries don't repeat it.
    let envelope = json!({
        "@context": { "ex": "http://example.org/" },
        "queries": {
            "find": {
                "language": "jsonld",
                "query": {
                    "from": "mq:ctx",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
    let s = serde_json::to_string(&body["results"]["find"]).unwrap();
    assert!(s.contains("Carol"), "expected Carol in: {s}");
}

#[tokio::test]
async fn multi_query_shared_context_injects_sparql_prefix() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:sparqlctx").await;
    insert_one(&app, "mq:sparqlctx", "ex:dan", "Dan").await;

    // SPARQL query without its own PREFIX line — should pick up `ex:`
    // from envelope @context.
    let envelope = json!({
        "@context": { "ex": "http://example.org/" },
        "queries": {
            "find": {
                "language": "sparql",
                "query": "SELECT ?name FROM <mq:sparqlctx> WHERE { ?s ex:name ?name }"
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
    let s = serde_json::to_string(&body["results"]["find"]).unwrap();
    assert!(s.contains("Dan"), "expected Dan in: {s}");
}

#[tokio::test]
async fn multi_query_response_echoes_snapshot_ledgers() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:snap").await;
    insert_one(&app, "mq:snap", "ex:e", "E").await;

    let envelope = json!({
        "queries": {
            "x": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:snap",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    let snapshot = &body["snapshot"];
    assert!(snapshot["asOf"].is_string(), "asOf should echo server-now");
    let t = snapshot["ledgers"]["mq:snap"].as_i64();
    assert!(t.is_some() && t.unwrap() >= 1, "snapshot.ledgers should pin mq:snap to t >= 1, got: {snapshot}");
}

// =============================================================================
// Multi-ledger atomicity
// =============================================================================

#[tokio::test]
async fn multi_query_two_ledgers_share_snapshot() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:l1").await;
    insert_one(&app, "mq:l1", "ex:foo", "Foo").await;
    create_ledger(&app, "mq:l2").await;
    insert_one(&app, "mq:l2", "ex:bar", "Bar").await;

    let envelope = json!({
        "queries": {
            "from_l1": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:l1",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            },
            "from_l2": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:l2",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert_eq!(body["status"], "ok");
    // Both ledgers' resolved t values must appear in the snapshot echo.
    assert!(body["snapshot"]["ledgers"]["mq:l1"].is_i64());
    assert!(body["snapshot"]["ledgers"]["mq:l2"].is_i64());
}

// =============================================================================
// Partial failure
// =============================================================================

#[tokio::test]
async fn multi_query_partial_failure_reports_per_alias_error() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:partial").await;
    insert_one(&app, "mq:partial", "ex:p", "P").await;

    // 'bad' uses a SPARQL string that the envelope validator can't tell
    // apart from a valid query (parse failure defers to the downstream
    // parser) — so envelope-level checks pass, snapshot resolution
    // succeeds against mq:partial, and dispatch reports the per-alias
    // SPARQL parse error.
    let envelope = json!({
        "queries": {
            "good": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:partial",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            },
            "bad": {
                "language": "sparql",
                "query": "SELECT ?x FROM <mq:partial> WHERE { this is not valid SPARQL }"
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    // One worked, one errored — partial.
    assert_eq!(body["status"], "partial");
    assert!(body["results"]["good"].is_array());
    assert!(
        body["errors"]["bad"].is_object(),
        "bad alias should land in errors, got: {body}"
    );
}

// =============================================================================
// Validation errors — 4xx
// =============================================================================

#[tokio::test]
async fn multi_query_empty_envelope_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    let envelope = json!({ "queries": {} });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multi_query_subquery_missing_from_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    let envelope = json!({
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "select": ["?s"], "where": { "@id": "?s" } }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multi_query_asof_collision_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:as").await;
    insert_one(&app, "mq:as", "ex:y", "Y").await;

    // Envelope sets asOf, sub-query also carries a @t: pin — collision.
    let envelope = json!({
        "asOf": "2024-01-01T00:00:00Z",
        "queries": {
            "a": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:as@t:1",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multi_query_envelope_max_fuel_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:fuel").await;
    insert_one(&app, "mq:fuel", "ex:f", "F").await;

    // Envelope-level fuel budget isn't supported in v1.
    let envelope = json!({
        "opts": { "max-fuel": 1000 },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:fuel",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multi_query_history_subquery_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:hist").await;
    insert_one(&app, "mq:hist", "ex:h", "H").await;

    // History range queries aren't supported inside envelopes in v1.
    let envelope = json!({
        "queries": {
            "h": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:hist@t:1",
                    "to":   "mq:hist@t:latest",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multi_query_max_concurrency_zero_returns_400() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:zc").await;
    insert_one(&app, "mq:zc", "ex:z", "Z").await;

    let envelope = json!({
        "opts": { "maxConcurrency": 0 },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:zc",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, _body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// =============================================================================
// Meta
// =============================================================================

#[tokio::test]
async fn multi_query_meta_block_included_when_opts_meta_true() {
    let (_tmp, state) = test_state().await;
    let app = build_router(state);
    create_ledger(&app, "mq:meta").await;
    insert_one(&app, "mq:meta", "ex:m", "M").await;

    let envelope = json!({
        "opts": { "meta": true },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": {
                    "@context": { "ex": "http://example.org/" },
                    "from": "mq:meta",
                    "select": ["?name"],
                    "where": { "@id": "?s", "ex:name": "?name" }
                }
            }
        }
    });
    let (status, body) = post_envelope(&app, &envelope).await;
    assert_eq!(status, StatusCode::OK, "got body: {body}");
    assert!(
        body["meta"]["elapsed_ms"].is_u64() || body["meta"]["elapsed_ms"].is_i64(),
        "meta.elapsed_ms should be present: {body}"
    );
}
