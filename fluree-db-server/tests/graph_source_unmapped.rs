//! A graph source with no R2RML mapping, over HTTP.
//!
//!   - `POST /iceberg/map` without `r2rml` is a 400 and stores nothing: no
//!     surface can register a source its queries could not be answered from.
//!   - A record stored before registration required a mapping answers every
//!     query route with `409 err:db/GraphSourceUnmapped`, naming the cause,
//!     rather than a ledger 404 (the target routes) or an empty result (the
//!     dataset routes).
//!
//! The stored record points at a catalog that does not exist: the refusal comes
//! before any catalog call, so no live catalog is needed.
#![cfg(feature = "iceberg")]

use axum::body::Body;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

const GS: &str = "unmapped:main";

async fn state() -> (TempDir, Arc<AppState>) {
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

/// Store the record the way registration once could: an Iceberg source with
/// no `mapping`.
async fn state_with_unmapped_source() -> (TempDir, Arc<AppState>) {
    let (tmp, state) = state().await;
    let config =
        fluree_db_api::IcebergCreateConfig::new("unmapped", "https://example.invalid", "ns.t")
            .to_iceberg_gs_config()
            .to_json()
            .expect("serialize config");
    state
        .fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_graph_source(
            "unmapped",
            "main",
            fluree_db_nameservice::GraphSourceType::Iceberg,
            &config,
            &[],
        )
        .await
        .expect("publish unmapped record");
    (tmp, state)
}

async fn send(
    state: &Arc<AppState>,
    uri: &str,
    content_type: &str,
    body: String,
) -> (StatusCode, String) {
    let resp = build_router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", content_type)
                .header("accept", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn assert_unmapped((status, text): (StatusCode, String), route: &str) {
    assert_eq!(status, StatusCode::CONFLICT, "{route}: {text}");
    let body: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{route}: {e}: {text}"));
    assert_eq!(
        body["@type"], "err:db/GraphSourceUnmapped",
        "{route}: {text}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|m| m.contains(&format!("graph source '{GS}' has no R2RML mapping"))),
        "{route}: {text}"
    );
}

/// The target routes (`/query/<source>`) once answered 404 "Ledger not found".
#[tokio::test]
async fn an_unmapped_source_is_a_409_on_its_own_query_route() {
    let (_tmp, state) = state_with_unmapped_source().await;
    let uri = format!("/v1/fluree/query/{GS}");

    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    assert_unmapped(
        send(&state, &uri, "application/json", jsonld.to_string()).await,
        "JSON-LD /query/<source>",
    );
    assert_unmapped(
        send(
            &state,
            &uri,
            "application/sparql-query",
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }".into(),
        )
        .await,
        "SPARQL /query/<source>",
    );
    assert_unmapped(
        send(
            &state,
            &uri,
            "application/sparql-query",
            format!("SELECT ?name FROM <{GS}> WHERE {{ ?s <http://example.org/name> ?name }}"),
        )
        .await,
        "SPARQL /query/<source> with FROM",
    );
}

/// The connection route (`/query` with a dataset naming the source) once
/// answered 200 with no rows.
#[tokio::test]
async fn an_unmapped_source_is_a_409_on_the_connection_query_route() {
    let (_tmp, state) = state_with_unmapped_source().await;

    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "from": GS,
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"},
    });
    assert_unmapped(
        send(
            &state,
            "/v1/fluree/query",
            "application/json",
            jsonld.to_string(),
        )
        .await,
        "JSON-LD /query from",
    );
    assert_unmapped(
        send(
            &state,
            "/v1/fluree/query",
            "application/sparql-query",
            format!("SELECT ?name FROM <{GS}> WHERE {{ ?s <http://example.org/name> ?name }}"),
        )
        .await,
        "SPARQL /query FROM",
    );
}

/// `POST /iceberg/map` needs a mapping: without one it is a 400 naming the
/// field, and no source is stored.
#[tokio::test]
async fn iceberg_map_without_a_mapping_is_a_400_and_stores_nothing() {
    let (_tmp, state) = state().await;
    let body = json!({
        "name": "nomap",
        "mode": "direct",
        "table_location": "s3://bucket/warehouse/ns/t",
    });
    let (status, text) = send(
        &state,
        "/v1/fluree/iceberg/map",
        "application/json",
        body.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("missing field `r2rml`"), "{text}");
    assert!(
        state
            .fluree
            .nameservice()
            .lookup_graph_source("nomap:main")
            .await
            .expect("lookup")
            .is_none(),
        "a refused registration stores no record"
    );
}
