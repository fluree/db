//! Protocol tests that need a real MySQL or Postgres server.
//!
//! SQLite covers the protocol shape (`sqlite_protocol.rs`), but it is the one
//! backend whose string literals cannot misbehave, so the escaping rule these
//! tests pin is invisible there.
//!
//! Gated on `FLUREE_BRIDGE_MYSQL_URL` / `FLUREE_BRIDGE_POSTGRES_URL`. CI's
//! `sql-bridge` job supplies both from service containers, and
//! `server_backends_are_configured_in_ci` fails if it ever stops — a skipped
//! test must not read as a passing one.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use sqlx::Executor;
use tower::ServiceExt;

/// The value a hostile query author supplies. Under MySQL's default `sql_mode`
/// the rendered form `'a\'' UNION …' ` closes its literal after `a'`, leaving
/// `UNION SELECT token FROM bridge_secrets --` to parse as SQL.
const PAYLOAD: &str = r"a\' UNION SELECT token FROM bridge_secrets -- ";

/// A Windows-ish path: the minimal trailing-backslash case, and ordinary data.
const TRAILING_BACKSLASH: &str = r"c:\";

/// What `bridge_secrets` holds — a mapping-scoped query must never see it.
const SECRET: &str = "topsecret";

/// What a `LIMIT 0` probe over `bridge_types` reports, on either backend.
///
/// Both produce the same vector: MySQL stores `BOOLEAN` as `TINYINT(1)`, but
/// sqlx reports the declared type, so it maps to Trino `boolean` like
/// Postgres's. That equivalence is what lets a mapping move between the two
/// backends unchanged, so it is shared here rather than written out twice.
const PROBE_TYPES: [&str; 8] = [
    "integer",
    "bigint",
    "double",
    "varchar",
    "decimal(38,6)",
    "date",
    "timestamp(6)",
    "boolean",
];

fn backend_url(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: {var} is unset");
            None
        }
    }
}

/// Render a string the way the engine's `sql_string` does: wrap in `'…'` and
/// double any embedded `'`. Nothing else. Reproduced here rather than imported
/// because the point is to pin the *bridge's* behaviour against exactly this
/// rendering, independent of what the engine chooses to push.
fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

async fn router_for(url: &str) -> axum::Router {
    let backend = fluree_sql_bridge::connect_backend(url, 2, 6)
        .await
        .unwrap_or_else(|e| panic!("connect {url}: {e}"));
    let app = fluree_sql_bridge::App::new(backend, None, 100, Duration::from_secs(60));
    Arc::clone(&app).router()
}

async fn call(router: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Run one statement through the protocol and drain every page.
///
/// Returns `Err` with the protocol error message when the statement fails, so a
/// test can distinguish "the server rejected it" from "the server ran it".
async fn rows(router: &axum::Router, sql: &str) -> Result<Vec<Value>, String> {
    let req = Request::post("/v1/statement")
        .header("host", "bridge:8080")
        .header("X-Trino-User", "fluree")
        .body(Body::from(sql.to_string()))
        .unwrap();
    let (status, first) = call(router, req).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    if let Some(msg) = first["error"]["message"].as_str() {
        return Err(msg.to_string());
    }

    let mut out = Vec::new();
    let mut next = first["nextUri"].as_str().unwrap().to_string();
    loop {
        let path = next.trim_start_matches("http://bridge:8080").to_string();
        let req = Request::get(&path)
            .header("host", "bridge:8080")
            .body(Body::empty())
            .unwrap();
        let (status, page) = call(router, req).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        if let Some(msg) = page["error"]["message"].as_str() {
            return Err(msg.to_string());
        }
        out.extend(page["data"].as_array().cloned().unwrap_or_default());
        match page["nextUri"].as_str() {
            Some(n) => next = n.to_string(),
            None => break,
        }
    }
    Ok(out)
}

/// Assert that a literal reaches the database as data, not as code.
///
/// `quote` renders an identifier for the dialect under test. Seeding goes
/// through bound parameters, so the row really does hold `PAYLOAD` byte for
/// byte and a correct server returns exactly it.
async fn assert_literals_are_data(router: &axum::Router, quote: fn(&str) -> String) {
    let name = quote("name");
    let people = quote("bridge_people");

    let found = rows(
        router,
        &format!(
            "SELECT {name} FROM {people} WHERE {name} = {}",
            sql_string(PAYLOAD)
        ),
    )
    .await
    .expect("the payload is a well-formed literal and the statement must run");

    let values: Vec<&str> = found.iter().filter_map(|r| r[0].as_str()).collect();
    assert!(
        !values.contains(&SECRET),
        "injected UNION reached bridge_secrets: {values:?}"
    );
    assert_eq!(
        values,
        [PAYLOAD],
        "the payload must match itself and nothing else"
    );

    let found = rows(
        router,
        &format!(
            "SELECT {name} FROM {people} WHERE {name} = {}",
            sql_string(TRAILING_BACKSLASH)
        ),
    )
    .await
    .expect("a trailing backslash is a well-formed literal");
    let values: Vec<&str> = found.iter().filter_map(|r| r[0].as_str()).collect();
    assert_eq!(values, [TRAILING_BACKSLASH]);

    // Quote doubling itself still works, and is not disturbed by the mode change.
    let found = rows(
        router,
        &format!(
            "SELECT {name} FROM {people} WHERE {name} = {}",
            sql_string("O'Brien")
        ),
    )
    .await
    .unwrap();
    let values: Vec<&str> = found.iter().filter_map(|r| r[0].as_str()).collect();
    assert_eq!(values, ["O'Brien"]);
}

fn mysql_quote(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn pg_quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[tokio::test]
async fn mysql_string_literals_are_data_not_code() {
    let Some(url) = backend_url("FLUREE_BRIDGE_MYSQL_URL") else {
        return;
    };
    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS bridge_people",
        "DROP TABLE IF EXISTS bridge_secrets",
        "CREATE TABLE bridge_people (id INT PRIMARY KEY, name VARCHAR(255))",
        "CREATE TABLE bridge_secrets (token VARCHAR(255))",
    ] {
        pool.execute(stmt).await.unwrap();
    }
    for (id, name) in [(1, PAYLOAD), (2, TRAILING_BACKSLASH), (3, "O'Brien")] {
        sqlx::query("INSERT INTO bridge_people (id, name) VALUES (?, ?)")
            .bind(id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO bridge_secrets (token) VALUES (?)")
        .bind(SECRET)
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    assert_literals_are_data(&router_for(&url).await, mysql_quote).await;
}

#[tokio::test]
async fn postgres_string_literals_are_data_not_code() {
    let Some(url) = backend_url("FLUREE_BRIDGE_POSTGRES_URL") else {
        return;
    };
    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS bridge_people",
        "DROP TABLE IF EXISTS bridge_secrets",
        "CREATE TABLE bridge_people (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE bridge_secrets (token TEXT)",
    ] {
        pool.execute(stmt).await.unwrap();
    }
    for (id, name) in [(1, PAYLOAD), (2, TRAILING_BACKSLASH), (3, "O'Brien")] {
        sqlx::query("INSERT INTO bridge_people (id, name) VALUES ($1, $2)")
            .bind(id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO bridge_secrets (token) VALUES ($1)")
        .bind(SECRET)
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    assert_literals_are_data(&router_for(&url).await, pg_quote).await;
}

/// The probe the engine's schema cache depends on: `SELECT * … LIMIT 0` must
/// name every column with a Trino type, and values must round-trip.
///
/// See [`PROBE_TYPES`] for why both backends report the same vector.
#[tokio::test]
async fn mysql_probe_names_trino_types_and_values_round_trip() {
    let Some(url) = backend_url("FLUREE_BRIDGE_MYSQL_URL") else {
        return;
    };
    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS bridge_types",
        "CREATE TABLE bridge_types (i INT, b BIGINT, d DOUBLE, s VARCHAR(8), \
         n DECIMAL(10,2), dt DATE, ts DATETIME, ok BOOLEAN)",
        "INSERT INTO bridge_types VALUES (1, 2, 1.5, 'x', 3.25, '2024-01-02', \
         '2024-01-02 03:04:05', 1)",
    ] {
        pool.execute(stmt).await.unwrap();
    }
    drop(pool);
    let router = router_for(&url).await;

    assert_eq!(probe_types(&router).await, PROBE_TYPES);

    let found = rows(&router, "SELECT i, b, d, s, dt, ok FROM bridge_types")
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0],
        serde_json::json!([1, 2, 1.5, "x", "2024-01-02", true])
    );
}

#[tokio::test]
async fn postgres_probe_names_trino_types_and_values_round_trip() {
    let Some(url) = backend_url("FLUREE_BRIDGE_POSTGRES_URL") else {
        return;
    };
    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS bridge_types",
        "CREATE TABLE bridge_types (i INT, b BIGINT, d DOUBLE PRECISION, s VARCHAR(8), \
         n DECIMAL(10,2), dt DATE, ts TIMESTAMP, ok BOOLEAN)",
        "INSERT INTO bridge_types VALUES (1, 2, 1.5, 'x', 3.25, '2024-01-02', \
         '2024-01-02 03:04:05', true)",
    ] {
        pool.execute(stmt).await.unwrap();
    }
    drop(pool);
    let router = router_for(&url).await;

    assert_eq!(probe_types(&router).await, PROBE_TYPES);

    let found = rows(&router, "SELECT i, b, d, s, dt, ok FROM bridge_types")
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0],
        serde_json::json!([1, 2, 1.5, "x", "2024-01-02", true])
    );
}

/// The column types a `LIMIT 0` probe reports.
async fn probe_types(router: &axum::Router) -> Vec<String> {
    let req = Request::post("/v1/statement")
        .header("host", "bridge:8080")
        .header("X-Trino-User", "fluree")
        .body(Body::from("SELECT * FROM bridge_types LIMIT 0"))
        .unwrap();
    let (status, first) = call(router, req).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    first["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns: {first}"))
        .iter()
        .map(|c| c["type"].as_str().unwrap().to_string())
        .collect()
}

/// The session settings the escaping rule depends on, asserted directly.
///
/// For MySQL this is the whole fix: a default server does *not* set
/// `NO_BACKSLASH_ESCAPES`, so dropping the pool's `after_connect` makes this
/// fail. For Postgres it is weaker — `standard_conforming_strings` is already
/// `on` by default, so this passes with or without the session `SET`, and only
/// a server configured with it `off` would tell the two apart. It is asserted
/// anyway so the setting cannot be dropped without a visible reason.
#[tokio::test]
async fn sessions_use_standard_sql_string_literals() {
    if let Some(url) = backend_url("FLUREE_BRIDGE_MYSQL_URL") {
        let found = rows(&router_for(&url).await, "SELECT @@sql_mode")
            .await
            .unwrap();
        let mode = found[0][0].as_str().unwrap();
        assert!(
            mode.contains("NO_BACKSLASH_ESCAPES"),
            "MySQL sessions must disable backslash escapes, got: {mode}"
        );
    }
    if let Some(url) = backend_url("FLUREE_BRIDGE_POSTGRES_URL") {
        let found = rows(
            &router_for(&url).await,
            "SELECT current_setting('standard_conforming_strings')",
        )
        .await
        .unwrap();
        assert_eq!(found[0][0].as_str(), Some("on"));
    }
}

/// A MySQL `TIMESTAMP` comes back as wall-clock time in the session's zone
/// and the driver labels it UTC, so a session left in the server's zone
/// would shift every zoned value by the server's offset. The driver pins
/// `time_zone = '+00:00'` on connect by default (`MySqlConnectOptions::
/// timezone`); this pins that default, since dropping it would go unnoticed
/// on a UTC server. CI runs the server five hours behind UTC (`TZ` on the
/// service) so the round trip below is non-vacuous there.
#[tokio::test]
async fn mysql_sessions_read_zoned_timestamps_as_utc() {
    let Some(url) = backend_url("FLUREE_BRIDGE_MYSQL_URL") else {
        return;
    };
    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS bridge_zoned",
        "CREATE TABLE bridge_zoned (at TIMESTAMP NULL)",
        // An offset literal names the instant regardless of the session zone.
        "INSERT INTO bridge_zoned VALUES ('2024-01-10 03:00:00+00:00')",
    ] {
        pool.execute(stmt).await.unwrap();
    }
    drop(pool);
    let router = router_for(&url).await;

    let zone = rows(&router, "SELECT @@session.time_zone").await.unwrap();
    assert_eq!(zone[0][0].as_str(), Some("+00:00"));

    let found = rows(&router, "SELECT at FROM bridge_zoned").await.unwrap();
    assert_eq!(
        found[0][0].as_str(),
        Some("2024-01-10 03:00:00.000000 UTC"),
        "a zoned value must read back as the instant stored"
    );
    // And a zoned literal compares with it as an instant: the row sits three
    // hours after the boundary, inside the server's offset.
    let after = rows(
        &router,
        "SELECT COUNT(*) FROM bridge_zoned WHERE at > TIMESTAMP '2024-01-10 00:00:00.000000+00:00'",
    )
    .await
    .unwrap();
    assert_eq!(after[0][0].as_i64(), Some(1));
}

/// A skipped test is not a passing one. CI must supply both backends.
#[test]
fn server_backends_are_configured_in_ci() {
    if std::env::var("CI").is_err() {
        eprintln!("SKIPPED: not CI");
        return;
    }
    for var in ["FLUREE_BRIDGE_MYSQL_URL", "FLUREE_BRIDGE_POSTGRES_URL"] {
        assert!(
            std::env::var(var).is_ok_and(|v| !v.is_empty()),
            "{var} must be set in CI, or the server-backed tests above pass by \
             doing nothing"
        );
    }
}
