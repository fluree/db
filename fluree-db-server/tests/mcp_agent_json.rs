//! Integration tests for the MCP `sparql_query` tool's Agent JSON output.
//!
//! These exercise `FlureeToolService::execute_sparql_agent_json` directly — the testable
//! core of the `sparql_query` tool, which avoids needing an rmcp `RequestContext` — while
//! seeding data through the regular HTTP routes.

use axum::body::Body;
use fluree_db_server::mcp::tools::FlureeToolService;
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use serde_json::Value as JsonValue;
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

async fn create_ledger(state: &Arc<AppState>, ledger: &str) {
    let app = build_router(state.clone());
    let body = serde_json::json!({ "ledger": ledger });
    let resp = app
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
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger {ledger}");
}

async fn insert(state: &Arc<AppState>, ledger: &str, body: JsonValue) {
    let app = build_router(state.clone());
    let resp = app
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
    assert_eq!(resp.status(), StatusCode::OK, "insert into {ledger}");
}

/// `ex:item{i} ex:name "name-{i}"` for `i in start..start+n`.
fn rows_graph_range(start: usize, n: usize) -> JsonValue {
    let graph: Vec<JsonValue> = (start..start + n)
        .map(
            |i| serde_json::json!({ "@id": format!("ex:item{i}"), "ex:name": format!("name-{i}") }),
        )
        .collect();
    serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": graph,
    })
}

fn rows_graph(n: usize) -> JsonValue {
    rows_graph_range(0, n)
}

const QUERY: &str = r"PREFIX ex: <http://example.org/>
SELECT ?s ?name WHERE { ?s ex:name ?name }";

#[tokio::test]
async fn agent_json_envelope_shape() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:shape").await;
    insert(&state, "test:shape", rows_graph(3)).await;

    let svc = FlureeToolService::new(state.clone());
    let env = svc
        .execute_sparql_agent_json("test:shape", QUERY, None, None, 32_768)
        .await
        .expect("query ok");

    assert!(
        env.get("schema").map(JsonValue::is_object).unwrap_or(false),
        "schema should be an object: {env}"
    );
    let rows = env
        .get("rows")
        .and_then(JsonValue::as_array)
        .expect("rows array");
    let row_count = env
        .get("rowCount")
        .and_then(JsonValue::as_u64)
        .expect("rowCount");
    assert_eq!(
        row_count as usize,
        rows.len(),
        "rowCount matches rows length"
    );
    assert_eq!(row_count, 3, "all three rows fit under the budget");
    assert_eq!(env.get("hasMore"), Some(&JsonValue::Bool(false)));
    assert!(
        env.get("t").and_then(JsonValue::as_i64).is_some(),
        "t (snapshot marker) present"
    );
    // The ledger-scoped MCP path never emits the FROM-rewritten resume query.
    assert!(env.get("resume").is_none(), "no resume key for MCP");
    // `iso` is intentionally dropped: it would be wall-clock query time, not the snapshot's
    // timestamp, and pagination keys on `t` — so a misleading field is simply omitted.
    assert!(env.get("iso").is_none(), "no iso key for MCP");
}

#[tokio::test]
async fn agent_json_byte_budget_truncates() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:trunc").await;
    insert(&state, "test:trunc", rows_graph(20)).await;

    let svc = FlureeToolService::new(state.clone());
    // Tiny budget forces truncation; the formatter always keeps at least one row.
    let env = svc
        .execute_sparql_agent_json("test:trunc", QUERY, None, None, 64)
        .await
        .expect("query ok");

    assert_eq!(
        env.get("hasMore"),
        Some(&JsonValue::Bool(true)),
        "byte budget should truncate: {env}"
    );
    let row_count = env
        .get("rowCount")
        .and_then(JsonValue::as_u64)
        .expect("rowCount") as usize;
    assert!(
        (1..20).contains(&row_count),
        "expected a partial page of rows, got {row_count}"
    );
}

#[tokio::test]
async fn agent_json_t_pinning_is_deterministic() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:pin").await;
    insert(&state, "test:pin", rows_graph_range(0, 3)).await;

    let svc = FlureeToolService::new(state.clone());

    // First call at latest: capture the snapshot `t` and the baseline row count.
    let env1 = svc
        .execute_sparql_agent_json("test:pin", QUERY, None, None, 32_768)
        .await
        .expect("latest query ok");
    let t1 = env1
        .get("t")
        .and_then(JsonValue::as_i64)
        .expect("t present");
    assert_eq!(env1.get("rowCount").and_then(JsonValue::as_u64), Some(3));

    // Advance the ledger with three more distinct rows.
    insert(&state, "test:pin", rows_graph_range(3, 3)).await;

    // Pinned to t1: still the original snapshot (3 rows), and `t` is echoed.
    let env_pinned = svc
        .execute_sparql_agent_json("test:pin", QUERY, None, Some(t1), 32_768)
        .await
        .expect("pinned query ok");
    assert_eq!(
        env_pinned.get("rowCount").and_then(JsonValue::as_u64),
        Some(3),
        "pinned snapshot is unchanged by later writes"
    );
    assert_eq!(
        env_pinned.get("t").and_then(JsonValue::as_i64),
        Some(t1),
        "pinned result echoes the requested t"
    );

    // Latest now sees all six rows.
    let env_latest = svc
        .execute_sparql_agent_json("test:pin", QUERY, None, None, 32_768)
        .await
        .expect("latest query ok");
    assert_eq!(
        env_latest.get("rowCount").and_then(JsonValue::as_u64),
        Some(6),
        "latest snapshot reflects the new rows"
    );
}

#[tokio::test]
async fn agent_json_identity_branch_shape() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:ident").await;
    insert(&state, "test:ident", rows_graph(2)).await;

    let svc = FlureeToolService::new(state.clone());
    // Exercise the identity/policy branch. The ledger defines no policy, so the exact row
    // count depends on default policy semantics — assert only that the envelope shape holds.
    let env = svc
        .execute_sparql_agent_json(
            "test:ident",
            QUERY,
            Some("did:key:z6MkExample"),
            None,
            32_768,
        )
        .await
        .expect("policy query ok");

    assert!(env.get("schema").map(JsonValue::is_object).unwrap_or(false));
    assert!(env.get("rows").map(JsonValue::is_array).unwrap_or(false));
    assert!(env.get("rowCount").and_then(JsonValue::as_u64).is_some());
    assert!(env
        .get("hasMore")
        .map(JsonValue::is_boolean)
        .unwrap_or(false));
}

#[tokio::test]
async fn agent_json_truncation_message_explains_pagination() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:msg").await;
    insert(&state, "test:msg", rows_graph(20)).await;

    let svc = FlureeToolService::new(state.clone());
    // Force truncation so the pagination guidance is appended to `message`.
    let env = svc
        .execute_sparql_agent_json("test:msg", QUERY, None, None, 64)
        .await
        .expect("query ok");

    assert_eq!(env.get("hasMore"), Some(&JsonValue::Bool(true)));
    let t = env.get("t").and_then(JsonValue::as_i64).expect("t present");
    let msg = env
        .get("message")
        .and_then(JsonValue::as_str)
        .expect("message present");
    assert!(
        msg.contains(&format!("t={t}")),
        "message should reference the snapshot t: {msg}"
    );
    assert!(
        msg.contains("ORDER BY"),
        "message should recommend ORDER BY: {msg}"
    );
    assert!(
        msg.contains("OFFSET"),
        "message should mention OFFSET: {msg}"
    );
}

/// `SELECT ?s ?name … ORDER BY ?s` with optional `LIMIT n OFFSET m`.
fn paging_query(limit_offset: Option<(usize, usize)>) -> String {
    let base = "PREFIX ex: <http://example.org/>\n\
                SELECT ?s ?name WHERE { ?s ex:name ?name } ORDER BY ?s";
    match limit_offset {
        Some((limit, offset)) => format!("{base} LIMIT {limit} OFFSET {offset}"),
        None => base.to_string(),
    }
}

fn s_values(env: &JsonValue) -> Vec<String> {
    env.get("rows")
        .and_then(JsonValue::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.get("?s").and_then(JsonValue::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn agent_json_pinned_paging_is_disjoint_and_complete() {
    let (_tmp, state) = test_state().await;
    create_ledger(&state, "test:page").await;
    insert(&state, "test:page", rows_graph(4)).await;

    let svc = FlureeToolService::new(state.clone());

    // Pin the snapshot, then page it with ORDER BY so the contract — disjoint, complete pages —
    // is exercised end to end (snapshot fixity alone doesn't prove stable scan order).
    let base = svc
        .execute_sparql_agent_json("test:page", &paging_query(None), None, None, 32_768)
        .await
        .expect("latest ok");
    let t = base
        .get("t")
        .and_then(JsonValue::as_i64)
        .expect("t present");
    assert_eq!(base.get("rowCount").and_then(JsonValue::as_u64), Some(4));

    let page1 = svc
        .execute_sparql_agent_json(
            "test:page",
            &paging_query(Some((2, 0))),
            None,
            Some(t),
            32_768,
        )
        .await
        .expect("page1 ok");
    let page2 = svc
        .execute_sparql_agent_json(
            "test:page",
            &paging_query(Some((2, 2))),
            None,
            Some(t),
            32_768,
        )
        .await
        .expect("page2 ok");

    let s1 = s_values(&page1);
    let s2 = s_values(&page2);
    assert_eq!(s1.len(), 2, "page 1 returns 2 rows: {page1}");
    assert_eq!(s2.len(), 2, "page 2 returns 2 rows: {page2}");

    let mut union: Vec<String> = s1.iter().chain(s2.iter()).cloned().collect();
    union.sort();
    union.dedup();
    assert_eq!(
        union.len(),
        4,
        "pages are disjoint and together cover all 4 rows: {s1:?} {s2:?}"
    );
}
