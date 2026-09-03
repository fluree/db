//! `fluree-sql-bridge` — a Trino-protocol HTTP front for one Postgres, MySQL
//! or SQLite database.
//!
//! Fluree's SQL graph sources talk to "anything that speaks the Trino client
//! protocol". Trino itself is the general answer; this sidecar is the small
//! one for a single database when running a JVM is not wanted. It holds the
//! connection pool; the Fluree process holds nothing.
//!
//! Protocol subset served:
//!
//! - `POST /v1/statement` (body = SQL) → `{id, columns, nextUri}`
//! - `GET  /v1/statement/{id}/{page}` → `{id, columns, data, nextUri?}`
//! - `DELETE /v1/statement/{id}/{page}` → cancel
//! - `GET  /v1/info` → health
//!
//! Rows are streamed from the driver through a bounded channel and served a
//! page at a time, so a large result never sits in memory.

pub mod backend;
pub mod mysql;
pub mod postgres;
pub mod render;
pub mod sqlite;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use backend::{Backend, ColumnMeta, RowChunk, Session};

/// Open the backend named by a database URL.
pub async fn connect_backend(
    url: &str,
    max_connections: u32,
    decimal_scale: i64,
) -> Result<Box<dyn Backend>, String> {
    Ok(match url.split(':').next().unwrap_or("") {
        "postgres" | "postgresql" => {
            Box::new(postgres::Postgres::connect(url, max_connections, decimal_scale).await?)
        }
        "mysql" | "mariadb" => {
            Box::new(mysql::MySql::connect(url, max_connections, decimal_scale).await?)
        }
        "sqlite" => Box::new(sqlite::Sqlite::connect(url, max_connections).await?),
        other => {
            return Err(format!(
                "unsupported database URL scheme '{other}' (postgres://, mysql://, sqlite://)"
            ))
        }
    })
}

pub struct Statement {
    columns: Vec<ColumnMeta>,
    rx: mpsc::Receiver<RowChunk>,
    /// Rows already pulled from the channel but not yet served.
    pending: Vec<Vec<Value>>,
    next_page: u64,
    last_touch: Instant,
}

pub struct App {
    backend: Box<dyn Backend>,
    statements: Mutex<HashMap<String, Statement>>,
    token: Option<String>,
    page_rows: usize,
    idle: Duration,
}

impl App {
    pub fn new(
        backend: Box<dyn Backend>,
        token: Option<String>,
        page_rows: usize,
        idle: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            statements: Mutex::new(HashMap::new()),
            token,
            page_rows: page_rows.max(1),
            idle: idle.max(Duration::from_secs(1)),
        })
    }

    /// Drop statements nobody has fetched from within the idle window.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let app = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(app.idle / 2).await;
                let mut st = app.statements.lock().await;
                let before = st.len();
                st.retain(|_, s| s.last_touch.elapsed() < app.idle);
                if st.len() != before {
                    info!(dropped = before - st.len(), "dropped idle statements");
                }
            }
        });
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/v1/statement", post(post_statement))
            .route("/v1/statement/:id/:page", get(get_page).delete(cancel))
            .route("/v1/info", get(info_route))
            .with_state(self)
    }
}

fn authorized(app: &App, headers: &HeaderMap) -> bool {
    match &app.token {
        None => true,
        Some(t) => headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|got| got == t),
    }
}

fn header(headers: &HeaderMap, suffix: &str) -> Option<String> {
    for family in ["X-Trino-", "X-Presto-"] {
        if let Some(v) = headers.get(format!("{family}{suffix}")) {
            if let Ok(s) = v.to_str() {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn column_json(columns: &[ColumnMeta]) -> Value {
    Value::Array(
        columns
            .iter()
            .map(|c| {
                let raw = c
                    .trino_type
                    .split('(')
                    .next()
                    .unwrap_or(&c.trino_type)
                    .trim();
                json!({
                    "name": c.name,
                    "type": c.trino_type,
                    "typeSignature": { "rawType": raw, "arguments": [] }
                })
            })
            .collect(),
    )
}

fn page_uri(headers: &HeaderMap, id: &str, page: u64) -> String {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    format!("{scheme}://{host}/v1/statement/{id}/{page}")
}

fn error_response(id: &str, message: String, name: &str) -> Value {
    json!({
        "id": id,
        "error": { "message": message, "errorName": name, "errorCode": 65536 },
        "stats": { "state": "FAILED" }
    })
}

async fn info_route(State(app): State<Arc<App>>) -> Json<Value> {
    Json(json!({ "starting": false, "dialect": app.backend.dialect(), "coordinator": true }))
}

async fn post_statement(State(app): State<Arc<App>>, headers: HeaderMap, body: String) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let sql = body.trim().trim_end_matches(';').to_string();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty statement").into_response();
    }
    let id = format!(
        "{}_{}",
        chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        uuid::Uuid::new_v4().simple()
    );
    let session = Session {
        schema: header(&headers, "Schema"),
    };
    info!(id, sql = %sql, "statement");

    let (tx, rx) = mpsc::channel(8);
    let columns = match app.backend.start(sql, session, tx).await {
        Ok(c) => c,
        Err(e) => {
            warn!(id, error = %e, "statement failed to start");
            return Json(error_response(&id, e, "SYNTAX_ERROR")).into_response();
        }
    };
    let cols = column_json(&columns);
    app.statements.lock().await.insert(
        id.clone(),
        Statement {
            columns,
            rx,
            pending: Vec::new(),
            next_page: 1,
            last_touch: Instant::now(),
        },
    );
    Json(json!({
        "id": id,
        "infoUri": page_uri(&headers, &id, 0),
        "nextUri": page_uri(&headers, &id, 1),
        "columns": cols,
        "stats": { "state": "RUNNING" }
    }))
    .into_response()
}

async fn get_page(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((id, page)): Path<(String, u64)>,
) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(mut st) = app.statements.lock().await.remove(&id) else {
        return (StatusCode::GONE, "unknown or finished statement").into_response();
    };
    if page != st.next_page {
        let expected = st.next_page;
        app.statements.lock().await.insert(id.clone(), st);
        return (
            StatusCode::GONE,
            format!(
                "page {page} is not the next page ({expected}); pages are served once, in order"
            ),
        )
            .into_response();
    }

    let mut rows = std::mem::take(&mut st.pending);
    let mut finished = false;
    let mut error: Option<String> = None;
    while rows.len() < app.page_rows {
        let chunk = if rows.is_empty() {
            st.rx.recv().await
        } else {
            match st.rx.try_recv() {
                Ok(c) => Some(c),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        };
        match chunk {
            Some(Ok(mut c)) => rows.append(&mut c),
            Some(Err(e)) => {
                error = Some(e);
                finished = true;
                break;
            }
            None => {
                finished = true;
                break;
            }
        }
    }
    if rows.len() > app.page_rows {
        st.pending = rows.split_off(app.page_rows);
    }

    if let Some(e) = error {
        warn!(id, error = %e, "statement failed");
        return Json(error_response(&id, e, "GENERIC_INTERNAL_ERROR")).into_response();
    }

    let cols = column_json(&st.columns);
    let mut body = json!({
        "id": id,
        "columns": cols,
        "data": rows,
        "stats": { "state": if finished { "FINISHED" } else { "RUNNING" } }
    });
    if !finished {
        st.next_page += 1;
        st.last_touch = Instant::now();
        body["nextUri"] = json!(page_uri(&headers, &id, st.next_page));
        app.statements.lock().await.insert(id, st);
    }
    Json(body).into_response()
}

async fn cancel(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((id, _page)): Path<(String, u64)>,
) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // Dropping the statement drops its receiver; the driver task's next send
    // fails and it stops reading.
    let removed = app.statements.lock().await.remove(&id).is_some();
    info!(id, removed, "statement cancelled");
    StatusCode::NO_CONTENT.into_response()
}
