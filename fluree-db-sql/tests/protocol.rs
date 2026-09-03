//! The statement/page protocol against a fake endpoint.

use std::sync::Arc;

use fluree_db_iceberg::auth::NoAuth;
use fluree_db_sql::{LogicalSource, SqlGsConfig, TrinoClient};
use futures::StreamExt;
use serde_json::json;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn client(server: &MockServer) -> TrinoClient {
    let mut cfg = SqlGsConfig::new(server.uri());
    cfg.catalog = Some("hive".into());
    cfg.schema = Some("sales".into());
    cfg.session.insert("query_max_run_time".into(), "5m".into());
    TrinoClient::new(&cfg, Arc::new(NoAuth)).unwrap()
}

fn page(
    id: &str,
    next: Option<String>,
    columns: bool,
    data: serde_json::Value,
) -> serde_json::Value {
    let mut p = json!({ "id": id, "stats": { "state": "RUNNING" } });
    if let Some(n) = next {
        p["nextUri"] = json!(n);
    }
    if columns {
        p["columns"] = json!([
            { "name": "id", "type": "bigint" },
            { "name": "name", "type": "varchar" }
        ]);
    }
    p["data"] = data;
    p
}

#[tokio::test]
async fn statement_pages_stream_as_batches_with_protocol_headers() {
    let server = MockServer::start().await;
    let base = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("X-Trino-User", "fluree"))
        .and(header("X-Trino-Catalog", "hive"))
        .and(header("X-Trino-Schema", "sales"))
        .and(header("X-Trino-Session", "query_max_run_time=5m"))
        .and(header("X-Trino-Time-Zone", "UTC"))
        .and(body_string("SELECT 1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q1",
            Some(format!("{base}/v1/statement/q1/1")),
            false,
            json!(null),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q1",
            Some(format!("{base}/v1/statement/q1/2")),
            true,
            json!([[1, "a"], [2, null]]),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q1",
            Some(format!("{base}/v1/statement/q1/3")),
            false,
            json!([[3, "c"]]),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q1/3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q1",
            None,
            false,
            json!(null),
        )))
        .mount(&server)
        .await;

    let c = client(&server);
    let batches: Vec<_> = c.execute("SELECT 1".into()).collect().await;
    let batches: Vec<_> = batches.into_iter().map(|b| b.unwrap()).collect();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows, 2);
    assert_eq!(batches[1].num_rows, 1);
    assert_eq!(batches[0].column_by_name("id").unwrap().get_i64(1), Some(2));
    assert_eq!(
        batches[0].column_by_name("name").unwrap().get_string(1),
        None
    );
    assert_eq!(
        batches[1].column_by_name("name").unwrap().get_string(0),
        Some("c")
    );

    let (schema, all) = c.execute_collect("SELECT 1").await.unwrap();
    assert_eq!(schema.unwrap().num_fields(), 2);
    assert_eq!(all.iter().map(|b| b.num_rows).sum::<usize>(), 3);
}

#[tokio::test]
async fn a_503_is_retried_and_an_error_page_fails_the_statement() {
    let server = MockServer::start().await;
    let base = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q2",
            Some(format!("{base}/v1/statement/q2/1")),
            true,
            json!([[1, "a"]]),
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/statement/q2/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q2",
            "error": { "message": "line 1:8: Table 'x' does not exist", "errorName": "TABLE_NOT_FOUND", "errorCode": 43 },
            "stats": { "state": "FAILED" }
        })))
        .mount(&server)
        .await;

    let c = client(&server);
    let items: Vec<_> = c.execute("SELECT * FROM x".into()).collect().await;
    assert_eq!(items.len(), 2, "one batch then the error");
    assert!(items[0].is_ok());
    let err = items[1].as_ref().unwrap_err().to_string();
    assert!(
        err.contains("does not exist") && err.contains("TABLE_NOT_FOUND 43"),
        "{err}"
    );
}

#[tokio::test]
async fn unauthorized_and_bad_json_surface_as_typed_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let err = client(&server)
        .execute_collect("SELECT 1")
        .await
        .unwrap_err();
    assert!(matches!(err, fluree_db_sql::SqlError::Auth(_)), "{err}");

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>not trino</html>"))
        .mount(&server)
        .await;
    let err = client(&server)
        .execute_collect("SELECT 1")
        .await
        .unwrap_err();
    assert!(matches!(err, fluree_db_sql::SqlError::Decode(_)), "{err}");
}

#[tokio::test]
async fn schema_probe_is_cached_and_count_reads_the_scalar() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string(r#"SELECT * FROM "sales"."orders" LIMIT 0"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "p", "columns": [{"name": "id", "type": "bigint"}, {"name": "total", "type": "decimal(10,2)"}],
            "data": [], "stats": {"state": "FINISHED"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string(r#"SELECT COUNT(*) FROM "sales"."orders" WHERE "id" IS NOT NULL"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c", "columns": [{"name": "_col0", "type": "bigint"}], "data": [[42]], "stats": {"state": "FINISHED"}
        })))
        .mount(&server)
        .await;

    let c = client(&server);
    let src = LogicalSource::Table("sales.orders".into());
    let s1 = c.schema(&src).await.unwrap();
    let s2 = c.schema(&src).await.unwrap();
    assert!(Arc::ptr_eq(&s1, &s2));
    assert_eq!(
        s1.field_by_name("total").unwrap().field_type,
        fluree_db_tabular::FieldType::Decimal {
            precision: 10,
            scale: 2
        }
    );
    assert_eq!(c.count(&src, &["id".into()]).await.unwrap(), 42);
}

#[tokio::test]
async fn dropping_the_stream_cancels_the_statement() {
    let server = MockServer::start().await;
    let base = server.uri();
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q3",
            Some(format!("{base}/v1/statement/q3/1")),
            true,
            json!([[1, "a"]]),
        )))
        .mount(&server)
        .await;
    // Every GET answers another page forever, so only a cancel ends this.
    Mock::given(method("GET"))
        .and(path("/v1/statement/q3/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "q3",
            Some(format!("{base}/v1/statement/q3/1")),
            false,
            json!([[2, "b"]]),
        )))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/statement/q3/1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let c = client(&server);
    let mut stream = c.execute("SELECT 1".into());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows, 1);
    drop(stream);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let cancelled = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .any(|r: &Request| r.method == "DELETE");
        if cancelled {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no DELETE observed after drop"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn presto_header_family_is_selectable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("X-Presto-User", "svc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "p", "columns": [{"name": "x", "type": "integer"}], "data": [[1]], "stats": {"state": "FINISHED"}
        })))
        .mount(&server)
        .await;
    let mut cfg = SqlGsConfig::new(server.uri());
    cfg.protocol = fluree_db_sql::WireProtocol::Presto;
    cfg.user = "svc".into();
    let c = TrinoClient::new(&cfg, Arc::new(NoAuth)).unwrap();
    let (_, b) = c.execute_collect("SELECT 1").await.unwrap();
    assert_eq!(b[0].column(0).unwrap().get_i32(0), Some(1));
}
