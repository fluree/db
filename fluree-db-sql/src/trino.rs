//! The Trino client protocol: `POST /v1/statement`, then `GET nextUri` until
//! it disappears. Stateless from our side — every page is one plain HTTP
//! request carrying its own auth — which is what makes this usable from a
//! Lambda, and what lets a small sidecar in front of Postgres/MySQL/SQLite
//! speak the same protocol and need no driver code in this binary.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use fluree_db_iceberg::auth::SendCatalogAuth;
use fluree_db_tabular::{BatchSchema, ColumnBatch};
use futures::Stream;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::SqlGsConfig;
use crate::dialect::{render_count, render_probe, LogicalSource, SqlDialect};
use crate::error::{Result, SqlError};
use crate::types::{decode_rows, schema_from_columns};

const SCHEMA_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_503_RETRIES: u32 = 6;
const STREAM_CHANNEL_DEPTH: usize = 4;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatementResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    next_uri: Option<String>,
    #[serde(default)]
    columns: Option<Vec<TrinoColumn>>,
    #[serde(default)]
    data: Option<Vec<Vec<Value>>>,
    #[serde(default)]
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
struct TrinoColumn {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoError {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error_name: Option<String>,
    #[serde(default)]
    error_code: Option<i64>,
}

impl TrinoError {
    fn render(&self) -> String {
        let mut s = self
            .message
            .clone()
            .unwrap_or_else(|| "unknown error".to_string());
        if let Some(name) = &self.error_name {
            s.push_str(&format!(" [{name}"));
            if let Some(code) = self.error_code {
                s.push_str(&format!(" {code}"));
            }
            s.push(']');
        }
        s
    }
}

/// A stream of batches fed by a background driver task. `Sync` because it
/// holds only the channel receiver — the request futures live in the task.
pub struct BatchStream {
    rx: mpsc::Receiver<Result<ColumnBatch>>,
}

impl Stream for BatchStream {
    type Item = Result<ColumnBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

pub type SqlBatchStream = Pin<Box<dyn Stream<Item = Result<ColumnBatch>> + Send + Sync>>;

/// A client bound to one endpoint + credential.
#[derive(Clone)]
pub struct TrinoClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    statement_url: String,
    base_headers: HeaderMap,
    auth: Arc<dyn SendCatalogAuth>,
    dialect: SqlDialect,
    schema_cache: Mutex<HashMap<String, (Instant, Arc<BatchSchema>)>>,
}

impl std::fmt::Debug for TrinoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoClient")
            .field("statement_url", &self.inner.statement_url)
            .field("dialect", &self.inner.dialect)
            .finish_non_exhaustive()
    }
}

impl TrinoClient {
    /// `config` must already be hydrated (no `secret_ref` left in `auth`).
    pub fn new(config: &SqlGsConfig, auth: Arc<dyn SendCatalogAuth>) -> Result<Self> {
        config.validate()?;
        let http = crate::net::build_client(Duration::from_secs(config.request_timeout_secs))?;

        let mut base_headers = HeaderMap::new();
        let h = |suffix: &str| HeaderName::from_bytes(config.protocol.header(suffix).as_bytes());
        let put = |headers: &mut HeaderMap, name: HeaderName, value: &str| -> Result<()> {
            let v = HeaderValue::from_str(value)
                .map_err(|_| SqlError::Config(format!("header {name} has a non-ASCII value")))?;
            headers.insert(name, v);
            Ok(())
        };
        put(&mut base_headers, h("User").unwrap(), &config.user)?;
        put(&mut base_headers, h("Source").unwrap(), "fluree")?;
        put(&mut base_headers, h("Time-Zone").unwrap(), "UTC")?;
        if let Some(c) = &config.catalog {
            put(&mut base_headers, h("Catalog").unwrap(), c)?;
        }
        if let Some(s) = &config.schema {
            put(&mut base_headers, h("Schema").unwrap(), s)?;
        }
        if !config.session.is_empty() {
            let joined = config
                .session
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",");
            put(&mut base_headers, h("Session").unwrap(), &joined)?;
        }

        Ok(Self {
            inner: Arc::new(Inner {
                http,
                statement_url: format!("{}/v1/statement", config.endpoint_base()),
                base_headers,
                auth,
                dialect: config.dialect,
                schema_cache: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn dialect(&self) -> SqlDialect {
        self.inner.dialect
    }

    /// Run `sql`, streaming each protocol page as one batch. Dropping the
    /// stream cancels the statement on the server.
    pub fn execute(&self, sql: String) -> SqlBatchStream {
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_DEPTH);
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut cancel_uri: Option<String> = None;
            let outcome = inner
                .drive(&sql, &mut cancel_uri, |batch| {
                    let tx = tx.clone();
                    async move { tx.send(Ok(batch)).await.is_ok() }
                })
                .await;
            match outcome {
                Ok(_) => {}
                Err(SqlError::Http(m)) if m == CONSUMER_GONE => {
                    if let Some(uri) = cancel_uri {
                        inner.cancel(&uri).await;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });
        Box::pin(BatchStream { rx })
    }

    /// Run `sql` to completion. Returns the schema (present even for zero rows
    /// once the statement planned) and every batch.
    pub async fn execute_collect(
        &self,
        sql: &str,
    ) -> Result<(Option<Arc<BatchSchema>>, Vec<ColumnBatch>)> {
        let batches = Mutex::new(Vec::new());
        let mut cancel_uri = None;
        let schema = self
            .inner
            .drive(sql, &mut cancel_uri, |batch| {
                batches.lock().unwrap().push(batch);
                async { true }
            })
            .await?;
        Ok((schema, batches.into_inner().unwrap()))
    }

    /// The source's column schema from a cached `LIMIT 0` probe.
    pub async fn schema(&self, source: &LogicalSource) -> Result<Arc<BatchSchema>> {
        let key = source.render(self.inner.dialect);
        if let Some((at, schema)) = self.inner.schema_cache.lock().unwrap().get(&key) {
            if at.elapsed() < SCHEMA_CACHE_TTL {
                return Ok(Arc::clone(schema));
            }
        }
        let sql = render_probe(source, self.inner.dialect);
        let (schema, _) = self.execute_collect(&sql).await?;
        let schema = schema.ok_or_else(|| {
            SqlError::Query(format!("schema probe returned no column metadata: {sql}"))
        })?;
        self.inner
            .schema_cache
            .lock()
            .unwrap()
            .insert(key, (Instant::now(), Arc::clone(&schema)));
        Ok(schema)
    }

    /// Exact `COUNT(*)` with the given columns required non-null.
    pub async fn count(&self, source: &LogicalSource, non_null_cols: &[String]) -> Result<u64> {
        let sql = render_count(source, non_null_cols, self.inner.dialect);
        let (_, batches) = self.execute_collect(&sql).await?;
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows > 0)
            .ok_or_else(|| SqlError::Query(format!("COUNT(*) returned no rows: {sql}")))?;
        let col = batch
            .column(0)
            .ok_or_else(|| SqlError::Decode("COUNT(*) returned no column".to_string()))?;
        let n = col
            .get_i64(0)
            .or_else(|| col.get_i32(0).map(i64::from))
            .or_else(|| col.get_f64(0).map(|f| f as i64))
            .ok_or_else(|| {
                SqlError::Decode(format!("COUNT(*) value is not an integer: {col:?}"))
            })?;
        u64::try_from(n).map_err(|_| SqlError::Decode(format!("negative COUNT(*): {n}")))
    }
}

const CONSUMER_GONE: &str = "consumer dropped the result stream";

impl Inner {
    /// Post the statement and walk every page, handing each decoded batch to
    /// `sink`; a `false` from the sink means the consumer went away.
    async fn drive<F, Fut>(
        &self,
        sql: &str,
        cancel_uri: &mut Option<String>,
        mut sink: F,
    ) -> Result<Option<Arc<BatchSchema>>>
    where
        F: FnMut(ColumnBatch) -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        debug!(sql = %sql, "SQL statement");
        let mut resp = self.post_statement(sql).await?;
        let mut schema: Option<Arc<BatchSchema>> = None;
        loop {
            if let Some(err) = &resp.error {
                return Err(SqlError::Query(err.render()));
            }
            if schema.is_none() {
                if let Some(cols) = &resp.columns {
                    let pairs: Vec<(String, String)> = cols
                        .iter()
                        .map(|c| (c.name.clone(), c.type_name.clone()))
                        .collect();
                    schema = Some(schema_from_columns(&pairs));
                }
            }
            if let Some(rows) = resp.data.take() {
                if !rows.is_empty() {
                    let s = schema.as_ref().ok_or_else(|| {
                        SqlError::Decode("page carried data before any column metadata".to_string())
                    })?;
                    let batch = decode_rows(s, rows)?;
                    if !sink(batch).await {
                        return Err(SqlError::Http(CONSUMER_GONE.to_string()));
                    }
                }
            }
            match resp.next_uri.take() {
                Some(uri) => {
                    *cancel_uri = Some(uri.clone());
                    resp = self.get_page(&uri).await?;
                }
                None => {
                    *cancel_uri = None;
                    return Ok(schema);
                }
            }
        }
    }

    async fn auth_headers(&self) -> Result<HeaderMap> {
        let mut headers = self.base_headers.clone();
        if let Some(value) = self
            .auth
            .authorization_header()
            .await
            .map_err(|e| SqlError::Auth(e.to_string()))?
        {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&value)
                    .map_err(|_| SqlError::Auth("invalid authorization header".into()))?,
            );
        }
        Ok(headers)
    }

    async fn post_statement(&self, sql: &str) -> Result<StatementResponse> {
        let mut attempt = 0;
        loop {
            let headers = self.auth_headers().await?;
            let resp = self
                .http
                .post(&self.statement_url)
                .headers(headers)
                .header(reqwest::header::CONTENT_TYPE, "text/plain")
                .body(sql.to_string())
                .send()
                .await
                .map_err(|e| SqlError::Http(format!("POST {}: {e}", self.statement_url)))?;
            match self.classify(resp, attempt).await? {
                Some(parsed) => return Ok(parsed),
                None => attempt += 1,
            }
        }
    }

    async fn get_page(&self, uri: &str) -> Result<StatementResponse> {
        let mut attempt = 0;
        loop {
            let headers = self.auth_headers().await?;
            let resp = self
                .http
                .get(uri)
                .headers(headers)
                .send()
                .await
                .map_err(|e| SqlError::Http(format!("GET {uri}: {e}")))?;
            match self.classify(resp, attempt).await? {
                Some(parsed) => return Ok(parsed),
                None => attempt += 1,
            }
        }
    }

    /// `Ok(Some)` = a page; `Ok(None)` = retry (503 with budget left).
    async fn classify(
        &self,
        resp: reqwest::Response,
        attempt: u32,
    ) -> Result<Option<StatementResponse>> {
        let status = resp.status();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            if attempt >= MAX_503_RETRIES {
                return Err(SqlError::Http(format!(
                    "endpoint kept answering 503 after {MAX_503_RETRIES} retries"
                )));
            }
            let backoff = Duration::from_millis(100 * (1u64 << attempt.min(5)));
            tokio::time::sleep(backoff).await;
            return Ok(None);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(SqlError::Auth(format!("endpoint returned {status}")));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(500).collect::<String>();
            return Err(SqlError::Http(format!(
                "endpoint returned {status}: {body}"
            )));
        }
        let parsed: StatementResponse = resp
            .json()
            .await
            .map_err(|e| SqlError::Decode(format!("statement response is not valid JSON: {e}")))?;
        if let Some(id) = &parsed.id {
            debug!(query_id = %id, has_next = parsed.next_uri.is_some(), "SQL page");
        }
        Ok(Some(parsed))
    }

    async fn cancel(&self, uri: &str) {
        match self.auth_headers().await {
            Ok(headers) => {
                if let Err(e) = self.http.delete(uri).headers(headers).send().await {
                    warn!(error = %e, "failed to cancel abandoned SQL statement");
                }
            }
            Err(e) => warn!(error = %e, "failed to cancel abandoned SQL statement"),
        }
    }
}
