//! The protocol over a real SQLite file, in-process.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn app(token: Option<&str>, page_rows: usize) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    sqlx::query(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score REAL, born DATE, ok BOOLEAN, raw BLOB)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for i in 1..=12 {
        sqlx::query(
            "INSERT INTO people (id, name, score, born, ok, raw) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(i)
        .bind(if i == 3 { None } else { Some(format!("p{i}")) })
        .bind(f64::from(i) * 1.5)
        .bind("2024-01-02")
        .bind(i % 2 == 0)
        .bind(vec![1u8, 2, 3])
        .execute(&pool)
        .await
        .unwrap();
    }
    drop(pool);
    let backend = fluree_sql_bridge::connect_backend(&url, 2, 6)
        .await
        .unwrap();
    let app = fluree_sql_bridge::App::new(
        backend,
        token.map(String::from),
        page_rows,
        Duration::from_secs(60),
    );
    (Arc::clone(&app).router(), dir)
}

async fn call(router: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

fn post(sql: &str, token: Option<&str>) -> Request<Body> {
    let mut b = Request::post("/v1/statement")
        .header("host", "bridge:8080")
        .header("X-Trino-User", "fluree");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::from(sql.to_string())).unwrap()
}

fn get(uri: &str, token: Option<&str>) -> Request<Body> {
    let path = uri.trim_start_matches("http://bridge:8080");
    let mut b = Request::get(path).header("host", "bridge:8080");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn pages_stream_in_order_with_trino_typed_columns() {
    let (router, _dir) = app(None, 5).await;
    let (status, first) = call(
        &router,
        post(
            "SELECT id, name, score, born, ok, raw FROM people ORDER BY id",
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let types: Vec<&str> = first["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        [
            "bigint",
            "varchar",
            "double",
            "date",
            "boolean",
            "varbinary"
        ]
    );
    assert!(first["data"].is_null());

    let mut next = first["nextUri"].as_str().unwrap().to_string();
    let mut rows: Vec<Value> = Vec::new();
    let mut pages = 0;
    loop {
        let (status, page) = call(&router, get(&next, None)).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert!(page["error"].is_null(), "{page}");
        pages += 1;
        rows.extend(page["data"].as_array().cloned().unwrap_or_default());
        match page["nextUri"].as_str() {
            Some(n) => next = n.to_string(),
            None => break,
        }
    }
    assert_eq!(rows.len(), 12);
    assert!(pages >= 3, "5 rows per page: {pages}");
    assert_eq!(rows[0], json!([1, "p1", 1.5, "2024-01-02", false, "AQID"]));
    assert_eq!(rows[2][1], Value::Null, "NULL name");

    // A finished statement is gone.
    let (status, _) = call(&router, get(&next, None)).await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test]
async fn probe_with_no_rows_still_reports_columns_and_count_is_a_scalar() {
    let (router, _dir) = app(None, 100).await;
    let (_, first) = call(&router, post("SELECT * FROM people LIMIT 0", None)).await;
    assert_eq!(first["columns"].as_array().unwrap().len(), 6);
    let (_, page) = call(&router, get(first["nextUri"].as_str().unwrap(), None)).await;
    assert_eq!(page["data"], json!([]));
    assert!(page["nextUri"].is_null());

    let (_, first) = call(
        &router,
        post("SELECT COUNT(*) FROM people WHERE name IS NOT NULL", None),
    )
    .await;
    let (_, page) = call(&router, get(first["nextUri"].as_str().unwrap(), None)).await;
    assert_eq!(page["data"], json!([[11]]));
}

#[tokio::test]
async fn sql_errors_come_back_as_protocol_errors() {
    let (router, _dir) = app(None, 100).await;
    let (status, resp) = call(&router, post("SELECT nope FROM missing", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing"),
        "{resp}"
    );
    assert_eq!(resp["stats"]["state"], "FAILED");
}

#[tokio::test]
async fn bearer_token_is_enforced_and_cancel_drops_the_statement() {
    let (router, _dir) = app(Some("s3cret"), 2).await;
    let (status, _) = call(&router, post("SELECT 1", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, first) = call(&router, post("SELECT id FROM people", Some("s3cret"))).await;
    assert_eq!(status, StatusCode::OK);
    let next = first["nextUri"].as_str().unwrap().to_string();
    let path = next.trim_start_matches("http://bridge:8080").to_string();
    let del = Request::delete(&path)
        .header("authorization", "Bearer s3cret")
        .body(Body::empty())
        .unwrap();
    let (status, _) = call(&router, del).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&router, get(&next, Some("s3cret"))).await;
    assert_eq!(status, StatusCode::GONE);
}
