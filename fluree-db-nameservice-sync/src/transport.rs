//! HTTP transport seam under the proxy clients.
//!
//! [`ProxyStorage`](crate::proxy_storage::ProxyStorage) and
//! [`ProxyNameService`](crate::proxy_nameservice::ProxyNameService) perform
//! all network I/O through the [`HttpTransport`] trait, keeping the wire
//! logic — URL construction, address↔CID parsing, status mapping, integrity
//! verification — transport-agnostic. Native builds use [`ReqwestTransport`];
//! a browser (wasm32) build supplies its own implementation over `fetch`.
//!
//! ## Send bounds
//!
//! `execute` returns a `Send` future on every target, deliberately. The
//! engine's storage traits (`StorageRead` etc. in fluree-db-core) box `Send`
//! futures unconditionally, so a `?Send` transport could never carry a
//! `ProxyStorage` impl anyway — relaxing the bound here would buy nothing.
//! A wasm implementation whose underlying fetch future holds JS values (and
//! is therefore `!Send`) satisfies this trait by bridging over channels: the
//! transport holds only a `Send + Sync` job sender, a `spawn_local` driver
//! task owns the JS handles, and `execute` awaits a oneshot reply.
//!
//! ## Zero-copy
//!
//! Request and response bodies are [`Bytes`]: the reqwest implementation
//! hands the response buffer through without re-copying, and callers slice
//! or convert at their own boundary. Every CAS block a peer reads flows
//! through this type, so implementations should avoid intermediate copies.

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use std::fmt::Debug;
use std::time::Duration;

/// HTTP method for a [`TransportRequest`]. Only the verbs the proxy wire
/// protocol uses are modeled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMethod {
    Get,
    Post,
}

/// A fully-formed HTTP request: the URL carries any query string, headers
/// are plain name/value pairs (names are compile-time constants at every
/// call site), and the body — when present — is ready-to-send bytes.
#[derive(Clone)]
pub struct TransportRequest {
    pub method: TransportMethod,
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Option<Bytes>,
}

/// Manual `Debug`: the `authorization` header carries a bearer token, and a
/// transport impl logging `?req` must never leak it.
impl Debug for TransportRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, value)| {
                if name.eq_ignore_ascii_case("authorization") {
                    (*name, "[redacted]")
                } else {
                    (*name, value.as_str())
                }
            })
            .collect();
        f.debug_struct("TransportRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("headers", &headers)
            .field("body_len", &self.body.as_ref().map(Bytes::len))
            .finish()
    }
}

impl TransportRequest {
    /// Start a GET request for `url`.
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: TransportMethod::Get,
            url: url.into(),
            headers: Vec::new(),
            body: None,
        }
    }

    /// Start a POST request for `url`.
    pub fn post(url: impl Into<String>) -> Self {
        Self {
            method: TransportMethod::Post,
            url: url.into(),
            headers: Vec::new(),
            body: None,
        }
    }

    /// Append a header.
    #[must_use]
    pub fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// Attach a request body.
    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }
}

/// A complete HTTP response. The transport reads the full body before
/// returning; the proxy protocol has no streaming reads (SSE subscriptions
/// live elsewhere and do not go through this trait).
#[derive(Debug)]
pub struct TransportResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// Transport-level failure, split along the lines the proxy clients report
/// distinctly to callers. Each variant carries the underlying error's
/// display text so caller-facing messages are preserved verbatim.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The request timed out.
    #[error("{0}")]
    Timeout(String),
    /// Establishing the connection failed.
    #[error("{0}")]
    Connect(String),
    /// The request failed for any other reason before a response arrived.
    #[error("{0}")]
    Request(String),
    /// A response arrived but reading its body failed.
    #[error("{0}")]
    Body(String),
}

/// Pluggable HTTP execution for the proxy clients. See the module docs for
/// the Send-bound and zero-copy contracts.
#[async_trait]
pub trait HttpTransport: Debug + Send + Sync {
    /// Execute the request and return the complete response.
    ///
    /// Implementations return `Ok` for **any** HTTP status — status handling
    /// is wire-protocol logic and belongs to the caller. Errors are reserved
    /// for requests that produced no readable response.
    async fn execute(&self, req: TransportRequest) -> Result<TransportResponse, TransportError>;
}

#[async_trait]
impl HttpTransport for std::sync::Arc<dyn HttpTransport> {
    async fn execute(&self, req: TransportRequest) -> Result<TransportResponse, TransportError> {
        self.as_ref().execute(req).await
    }
}

/// Native transport over a pooled [`reqwest::Client`].
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl Debug for ReqwestTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestTransport").finish_non_exhaustive()
    }
}

impl ReqwestTransport {
    /// Build a transport whose requests time out after `timeout`.
    pub fn with_timeout(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to create HTTP transport client");
        Self { client }
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn execute(&self, req: TransportRequest) -> Result<TransportResponse, TransportError> {
        let mut builder = match req.method {
            TransportMethod::Get => self.client.get(&req.url),
            TransportMethod::Post => self.client.post(&req.url),
        };
        for (name, value) in req.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = req.body {
            builder = builder.body(body);
        }

        let response = builder.send().await.map_err(|e| {
            if e.is_timeout() {
                TransportError::Timeout(e.to_string())
            } else if e.is_connect() {
                TransportError::Connect(e.to_string())
            } else {
                TransportError::Request(e.to_string())
            }
        })?;

        let status = response.status();
        let mut response = response;
        let headers = std::mem::take(response.headers_mut());
        let body = response
            .bytes()
            .await
            .map_err(|e| TransportError::Body(e.to_string()))?;

        Ok(TransportResponse {
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_builders_compose() {
        let req = TransportRequest::post("http://example.com/x")
            .header("accept", "application/json")
            .body(&b"{}"[..]);
        assert_eq!(req.method, TransportMethod::Post);
        assert_eq!(
            req.headers,
            vec![("accept", "application/json".to_string())]
        );
        assert_eq!(req.body.as_deref(), Some(&b"{}"[..]));
    }

    #[test]
    fn transport_request_debug_redacts_the_bearer() {
        let req = TransportRequest::get("http://origin.example/x")
            .header("authorization", "Bearer secret-token")
            .header("accept", "application/json");
        let debug = format!("{req:?}");
        assert!(!debug.contains("secret-token"), "got: {debug}");
        assert!(debug.contains("[redacted]"));
        assert!(debug.contains("application/json"), "other headers stay visible");
    }

    #[test]
    fn reqwest_transport_debug_holds_no_secrets() {
        let t = ReqwestTransport::with_timeout(Duration::from_secs(1));
        assert!(format!("{t:?}").contains("ReqwestTransport"));
    }
}
