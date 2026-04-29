//! HTTP client for remote ledger query/update operations
//!
//! Used by the CLI's "track" mode to forward data operations to a remote
//! Fluree server instead of executing them locally. This is distinct from
//! `fluree-db-nameservice-sync`'s `HttpRemoteClient`, which handles only
//! nameservice ref-level operations (lookup, push, snapshot).
//!
//! When a `RefreshConfig` is provided, the client automatically attempts
//! token refresh on 401 responses and retries the request once. Callers
//! should check `take_refreshed_tokens()` after operations to persist any
//! updated tokens.

use parking_lot::Mutex;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

// RFC 3986 §3.3 — a path segment may contain `pchar = unreserved / pct-encoded
// / sub-delims / ":" / "@"`. We encode only the characters that must not appear
// literally in a URL path: the generic-delims that would otherwise reframe the
// URL (`?`, `#`), whitespace, literal `%`, and a handful of hostile-looking
// ASCII. Crucially, `:` is left untouched so ledger identifiers like
// `ledger:branch` round-trip correctly through the server's path router.
// `/` is preserved so ledger names with path separators still land in the
// server's wildcard (`/*ledger`) capture.
const LEDGER_PATH: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'%');

// Single-segment variant for routes that use a `:alias` matcher instead of a
// wildcard; `/` must be encoded or it would split the URL path.
const LEDGER_SEGMENT: &AsciiSet = &LEDGER_PATH.add(b'/');

fn encode_ledger_path(s: &str) -> Cow<'_, str> {
    utf8_percent_encode(s, LEDGER_PATH).into()
}

fn encode_ledger_segment(s: &str) -> Cow<'_, str> {
    utf8_percent_encode(s, LEDGER_SEGMENT).into()
}

use crate::cli::PolicyArgs;
use fluree_db_api::{ExportCommitsResponse, PushCommitsResponse};
use fluree_db_core::pack::PackRequest;
use fluree_db_nameservice::NsRecord;

/// Build the set of HTTP headers that carry policy enforcement options to a
/// remote Fluree server.
///
/// Returns an empty vec when `policy` is unset. The server accepts:
///
/// - `fluree-identity` — the identity IRI to execute as
/// - `fluree-policy-class` — repeated for each policy class IRI (the server
///   accumulates all instances; comma-separated values within a single header
///   are also accepted)
/// - `fluree-default-allow` — `"true"` to allow access absent matching rules
///
/// JSON-LD requests additionally carry the same fields via body `opts` so
/// future opts-only fields ride through; see [`inject_policy_into_json_opts`].
pub(crate) fn policy_headers(policy: &PolicyArgs) -> Vec<(&'static str, String)> {
    let mut headers = Vec::new();
    if let Some(id) = &policy.identity {
        headers.push(("fluree-identity", id.clone()));
    }
    for pc in &policy.policy_class {
        headers.push(("fluree-policy-class", pc.clone()));
    }
    // `--policy` and `--policy-values` are JSON-encoded values transported in
    // headers as their compact JSON representation. Failures to read/parse the
    // source flag/file are surfaced earlier (when the CLI builds opts), so a
    // resolution error here is treated as a no-op.
    if let Ok(Some(p)) = policy.resolve_policy() {
        if let Ok(s) = serde_json::to_string(&p) {
            headers.push(("fluree-policy", s));
        }
    }
    if let Ok(Some(values)) = policy.resolve_policy_values() {
        let obj: serde_json::Map<String, serde_json::Value> = values.into_iter().collect();
        if let Ok(s) = serde_json::to_string(&serde_json::Value::Object(obj)) {
            headers.push(("fluree-policy-values", s));
        }
    }
    if policy.default_allow {
        headers.push(("fluree-default-allow", "true".to_string()));
    }
    headers
}

/// Inject policy opts into a JSON-LD query/transaction body.
///
/// Does nothing when `policy` is unset or `body` is not a JSON object. Uses
/// the standard `opts.identity` / `opts.policy-class` / `opts.default-allow`
/// shape the server parses via `QueryConnectionOptions::from_json`.
pub(crate) fn inject_policy_into_json_opts(body: &mut serde_json::Value, policy: &PolicyArgs) {
    if !policy.is_set() {
        return;
    }
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let opts = obj
        .entry("opts")
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    let Some(opts_obj) = opts.as_object_mut() else {
        return;
    };
    if let Some(id) = &policy.identity {
        opts_obj.insert(
            "identity".to_string(),
            serde_json::Value::String(id.clone()),
        );
    }
    if !policy.policy_class.is_empty() {
        let arr = policy
            .policy_class
            .iter()
            .cloned()
            .map(serde_json::Value::String)
            .collect();
        opts_obj.insert("policy-class".to_string(), serde_json::Value::Array(arr));
    }
    if let Ok(Some(p)) = policy.resolve_policy() {
        opts_obj.insert("policy".to_string(), p);
    }
    if let Ok(Some(values)) = policy.resolve_policy_values() {
        let obj: serde_json::Map<String, serde_json::Value> = values.into_iter().collect();
        opts_obj.insert("policy-values".to_string(), serde_json::Value::Object(obj));
    }
    if policy.default_allow {
        opts_obj.insert("default-allow".to_string(), serde_json::Value::Bool(true));
    }
}

/// Configuration for automatic token refresh on 401.
#[derive(Clone, Debug)]
pub struct RefreshConfig {
    /// Exchange endpoint URL for token refresh.
    pub exchange_url: String,
    /// Refresh token for silent renewal.
    pub refresh_token: String,
}

/// New token values after a successful refresh. Callers should persist these.
#[derive(Clone, Debug)]
pub struct RefreshedTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// HTTP client for ledger data operations against a remote Fluree server.
///
/// Supports query (JSON-LD/SPARQL), insert, upsert, transact, ledger-info, and
/// existence checks via the server's REST API. Optionally performs automatic
/// token refresh on 401 when a `RefreshConfig` is provided.
#[derive(Clone)]
pub struct RemoteLedgerClient {
    client: Client,
    base_url: String,
    token: Arc<Mutex<Option<String>>>,
    refresh_config: Option<Arc<Mutex<RefreshConfig>>>,
    refreshed: Arc<Mutex<Option<RefreshedTokens>>>,
    /// Optional policy flags that are automatically injected as HTTP headers
    /// on every request and (when the body is JSON-LD) as body-level `opts`
    /// fields. Set via [`RemoteLedgerClient::with_policy`].
    policy: PolicyArgs,
}

impl fmt::Debug for RemoteLedgerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteLedgerClient")
            .field("base_url", &self.base_url)
            .field("has_token", &self.token.lock().is_some())
            .field("has_refresh", &self.refresh_config.is_some())
            .finish()
    }
}

/// Error type for remote ledger operations.
#[derive(Debug)]
pub enum RemoteLedgerError {
    /// Network or connection error
    Network(String),
    /// 401 Unauthorized
    Unauthorized,
    /// 403 Forbidden
    Forbidden,
    /// 404 Not Found (includes server message if any)
    NotFound(String),
    /// 400 Bad Request (includes server error message)
    BadRequest(String),
    /// 409 Conflict (includes server error message)
    Conflict(String),
    /// 422 Unprocessable Entity / validation error
    ValidationError(String),
    /// 5xx Server Error (includes server error message)
    ServerError(String),
    /// Request could not be serialized (client-side bug)
    InvalidRequest(String),
    /// Response could not be parsed as expected
    InvalidResponse(String),
}

impl fmt::Display for RemoteLedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteLedgerError::Network(msg) => write!(f, "network error: {msg}"),
            RemoteLedgerError::Unauthorized => write!(
                f,
                "authentication failed (401). Token may be expired or revoked.\n  \
                 Run `fluree auth login` to store a new token, or \
                 `fluree auth status` to check expiry."
            ),
            RemoteLedgerError::Forbidden => write!(f, "access denied (403)"),
            RemoteLedgerError::NotFound(msg) => write!(f, "not found: {msg}"),
            RemoteLedgerError::BadRequest(msg) => write!(f, "bad request: {msg}"),
            RemoteLedgerError::Conflict(msg) => write!(f, "conflict (409): {msg}"),
            RemoteLedgerError::ValidationError(msg) => write!(f, "validation error (422): {msg}"),
            RemoteLedgerError::ServerError(msg) => write!(f, "server error: {msg}"),
            RemoteLedgerError::InvalidRequest(msg) => write!(f, "invalid request: {msg}"),
            RemoteLedgerError::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
        }
    }
}

impl RemoteLedgerClient {
    /// Default HTTP request timeout (5 minutes).
    ///
    /// Long-running queries and transactions are expected; the server should
    /// be the authority on when to time out. This client-side value is a
    /// safety net, not a policy knob.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

    /// Per-call timeout override for `POST /reindex`.
    ///
    /// A full commit-history rebuild on a large ledger can legitimately run
    /// longer than the default 5-minute request timeout; if the client
    /// abandons the connection, the server keeps rebuilding but the user
    /// loses the result. 1 hour is a pragmatic ceiling — the server still
    /// owns hard cutoffs.
    pub const REINDEX_TIMEOUT: Duration = Duration::from_secs(60 * 60);

    /// Create a new remote ledger client with the default 5-minute timeout.
    ///
    /// `base_url` is the Fluree API base (e.g., `http://localhost:8090/fluree`
    /// or `https://example.com/v1/fluree`). Trailing slashes are stripped.
    pub fn new(base_url: &str, auth_token: Option<String>) -> Self {
        Self::with_timeout(base_url, auth_token, Self::DEFAULT_TIMEOUT)
    }

    /// Create a new remote ledger client with a custom timeout.
    pub fn with_timeout(base_url: &str, auth_token: Option<String>, timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: Arc::new(Mutex::new(auth_token)),
            refresh_config: None,
            refreshed: Arc::new(Mutex::new(None)),
            policy: PolicyArgs::default(),
        }
    }

    /// Attach refresh configuration for automatic 401 retry.
    pub fn with_refresh(mut self, config: RefreshConfig) -> Self {
        self.refresh_config = Some(Arc::new(Mutex::new(config)));
        self
    }

    /// Attach policy flags that are automatically applied to every request.
    ///
    /// Policy is transported as HTTP headers (`fluree-identity`,
    /// `fluree-policy-class`, `fluree-default-allow`) on all requests, and
    /// additionally injected into the body-level `opts` object for JSON-LD
    /// query/transaction requests (enabling multi-value `policy-class` and
    /// future opts-only fields). No-op when `policy` is empty.
    pub fn with_policy(mut self, policy: PolicyArgs) -> Self {
        self.policy = policy;
        self
    }

    /// Take any refreshed tokens (consuming them). Callers should persist
    /// these back to config.toml after the operation completes.
    pub fn take_refreshed_tokens(&self) -> Option<RefreshedTokens> {
        self.refreshed.lock().take()
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let token = self.token.lock();
        if let Some(ref t) = *token {
            req.bearer_auth(t)
        } else {
            req
        }
    }

    /// Map a non-2xx response to a `RemoteLedgerError`.
    async fn map_error(resp: reqwest::Response) -> RemoteLedgerError {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let message = extract_error_message(&body);

        match status {
            StatusCode::UNAUTHORIZED => RemoteLedgerError::Unauthorized,
            StatusCode::FORBIDDEN => RemoteLedgerError::Forbidden,
            StatusCode::NOT_FOUND => RemoteLedgerError::NotFound(if message.is_empty() {
                "resource not found".to_string()
            } else {
                message
            }),
            StatusCode::BAD_REQUEST => RemoteLedgerError::BadRequest(if message.is_empty() {
                "bad request".to_string()
            } else {
                message
            }),
            StatusCode::CONFLICT => RemoteLedgerError::Conflict(if message.is_empty() {
                "conflict".to_string()
            } else {
                message
            }),
            StatusCode::UNPROCESSABLE_ENTITY => {
                RemoteLedgerError::ValidationError(if message.is_empty() {
                    "validation error".to_string()
                } else {
                    message
                })
            }
            s if s.is_server_error() => RemoteLedgerError::ServerError(if message.is_empty() {
                format!("status {s}")
            } else {
                message
            }),
            _ => RemoteLedgerError::ServerError(if message.is_empty() {
                format!("unexpected status {status}")
            } else {
                format!("unexpected status {status}: {message}")
            }),
        }
    }

    /// Map a reqwest error (network/timeout) to a `RemoteLedgerError`.
    fn map_network_error(e: reqwest::Error) -> RemoteLedgerError {
        if e.is_timeout() {
            RemoteLedgerError::Network(format!("request timed out: {e}"))
        } else if e.is_connect() {
            RemoteLedgerError::Network(format!("connection failed: {e}"))
        } else {
            RemoteLedgerError::Network(e.to_string())
        }
    }

    /// Attempt to refresh the access token using the stored refresh_token.
    /// Returns true if refresh succeeded and the token was updated.
    async fn try_refresh(&self) -> bool {
        let refresh_cfg = match &self.refresh_config {
            Some(cfg) => cfg.clone(),
            None => return false,
        };

        let (exchange_url, refresh_token) = {
            let cfg = refresh_cfg.lock();
            (cfg.exchange_url.clone(), cfg.refresh_token.clone())
        };

        if !exchange_url.starts_with("https://") {
            tracing::warn!(
                url = %exchange_url,
                "token refresh exchange URL is not HTTPS — credentials may be sent in cleartext"
            );
        }

        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        });

        let resp = match self.client.post(&exchange_url).json(&body).send().await {
            Ok(r) => r,
            Err(_) => return false,
        };

        if !resp.status().is_success() {
            return false;
        }

        let resp_body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(_) => return false,
        };

        let new_access = match resp_body.get("access_token").and_then(|v| v.as_str()) {
            Some(t) => t.to_string(),
            None => return false,
        };

        let new_refresh = resp_body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Update the token
        *self.token.lock() = Some(new_access.clone());

        // Update refresh_token if a new one was provided
        if let Some(ref new_rt) = new_refresh {
            refresh_cfg.lock().refresh_token = new_rt.clone();
        }

        // Store refreshed tokens for caller to persist
        *self.refreshed.lock() = Some(RefreshedTokens {
            access_token: new_access,
            refresh_token: new_refresh,
        });

        eprintln!("  (token refreshed automatically)");
        true
    }

    // =========================================================================
    // Generic request execution with 401 retry
    // =========================================================================

    /// Execute a request. On 401, attempt token refresh and retry once.
    async fn send_json(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        body: Option<RequestBody<'_>>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        // First attempt
        let resp = self
            .build_request(method.clone(), url, content_type, &body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            return resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
        }

        if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            // Retry with refreshed token
            let resp2 = self
                .build_request(method, url, content_type, &body)
                .send()
                .await
                .map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                return resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
            }
            return Err(Self::map_error(resp2).await);
        }

        Err(Self::map_error(resp).await)
    }

    /// Execute a JSON request with a per-call timeout override.
    ///
    /// Used for operations (e.g. `/reindex`) whose legitimate duration can
    /// exceed `DEFAULT_TIMEOUT`. On 401, attempts token refresh and retries once.
    async fn send_json_with_timeout(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        body: Option<RequestBody<'_>>,
        timeout: Duration,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let resp = self
            .build_request(method.clone(), url, content_type, &body)
            .timeout(timeout)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            return resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
        }

        if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            let resp2 = self
                .build_request(method, url, content_type, &body)
                .timeout(timeout)
                .send()
                .await
                .map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                return resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
            }
            return Err(Self::map_error(resp2).await);
        }

        Err(Self::map_error(resp).await)
    }

    /// Execute a request with additional headers. On 401, attempt token refresh and retry once.
    async fn send_json_with_headers(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        extra_headers: &[(&'static str, String)],
        body: Option<RequestBody<'_>>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        // First attempt
        let mut req = self.build_request(method.clone(), url, content_type, &body);
        for (k, v) in extra_headers {
            req = req.header(*k, v);
        }
        let resp = req.send().await.map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            return resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
        }

        if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            // Retry with refreshed token
            let mut req2 = self.build_request(method, url, content_type, &body);
            for (k, v) in extra_headers {
                req2 = req2.header(*k, v);
            }
            let resp2 = req2.send().await.map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                return resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
            }
            return Err(Self::map_error(resp2).await);
        }

        Err(Self::map_error(resp).await)
    }

    /// Execute a request, returning the raw response. On 401, attempt
    /// token refresh and retry once. Returns the response for caller to
    /// interpret status codes (except 401, which is handled here).
    async fn send_raw(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        accept: Option<&str>,
        body: Option<RequestBody<'_>>,
    ) -> Result<reqwest::Response, RemoteLedgerError> {
        let mut req = self.build_request(method.clone(), url, content_type, &body);
        if let Some(a) = accept {
            req = req.header("Accept", a);
        }
        let resp = req.send().await.map_err(Self::map_network_error)?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            if self.try_refresh().await {
                let mut req2 = self.build_request(method, url, content_type, &body);
                if let Some(a) = accept {
                    req2 = req2.header("Accept", a);
                }
                let resp2 = req2.send().await.map_err(Self::map_network_error)?;
                if resp2.status() == StatusCode::UNAUTHORIZED {
                    return Err(RemoteLedgerError::Unauthorized);
                }
                return Ok(resp2);
            }
            return Err(RemoteLedgerError::Unauthorized);
        }

        Ok(resp)
    }

    fn build_request(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        body: &Option<RequestBody<'_>>,
    ) -> reqwest::RequestBuilder {
        let mut req = self.add_auth(self.client.request(method, url));
        req = req.header("Content-Type", content_type);
        // Policy flags transport as HTTP headers on every request; servers read
        // them for SPARQL (which has no body opts) and merge them into body opts
        // for JSON-LD (with body values taking precedence).
        for (k, v) in policy_headers(&self.policy) {
            req = req.header(k, v);
        }
        match body {
            Some(RequestBody::Json(v)) => {
                // For JSON-LD bodies, also inject opts so multi-value
                // policy-class and any future opts-only fields ride through.
                if self.policy.is_set() {
                    let mut cloned = (*v).clone();
                    inject_policy_into_json_opts(&mut cloned, &self.policy);
                    req.json(&cloned)
                } else {
                    req.json(*v)
                }
            }
            Some(RequestBody::Text(s)) => req.body(s.to_string()),
            None => req,
        }
    }

    fn ledger_tail(ledger: &str) -> &str {
        ledger.trim_start_matches('/')
    }

    fn op_url(&self, op: &str, ledger: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_url,
            op,
            encode_ledger_path(Self::ledger_tail(ledger)),
        )
    }

    fn op_url_root(&self, op: &str) -> String {
        format!("{}/{}", self.base_url, op)
    }

    fn with_default_context_param(mut url: String) -> String {
        url.push_str("?default-context=true");
        url
    }

    // =========================================================================
    // Query
    // =========================================================================

    /// Execute a JSON-LD query against a ledger.
    pub async fn query_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = Self::with_default_context_param(self.op_url("query", ledger));
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Execute a SPARQL query against a ledger.
    pub async fn query_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = Self::with_default_context_param(self.op_url("query", ledger));
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-query",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    /// Execute a SPARQL query and return the raw response bytes with a custom Accept header.
    ///
    /// Used by the CLI to request delimited formats (TSV/CSV) directly from the server,
    /// bypassing JSON serialization on both sides.
    pub async fn query_sparql_accept_bytes(
        &self,
        ledger: &str,
        sparql: &str,
        accept: &str,
    ) -> Result<bytes::Bytes, RemoteLedgerError> {
        let url = Self::with_default_context_param(self.op_url("query", ledger));
        let resp = self
            .send_raw(
                reqwest::Method::POST,
                &url,
                "application/sparql-query",
                Some(accept),
                Some(RequestBody::Text(sparql)),
            )
            .await?;

        if resp.status().is_success() {
            resp.bytes()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Execute a connection-scoped SPARQL query and return raw response bytes with a custom Accept header.
    pub async fn query_connection_sparql_accept_bytes(
        &self,
        sparql: &str,
        accept: &str,
    ) -> Result<bytes::Bytes, RemoteLedgerError> {
        let url = self.op_url_root("query");
        let resp = self
            .send_raw(
                reqwest::Method::POST,
                &url,
                "application/sparql-query",
                Some(accept),
                Some(RequestBody::Text(sparql)),
            )
            .await?;

        if resp.status().is_success() {
            resp.bytes()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Execute a JSON-LD connection query (ledger specified via `from` in body).
    pub async fn query_connection_jsonld(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = Self::with_default_context_param(self.op_url_root("query"));
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Execute a SPARQL connection query (ledger specified via `FROM` clause).
    pub async fn query_connection_sparql(
        &self,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("query");
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-query",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    // =========================================================================
    // Explain
    // =========================================================================

    /// Explain a JSON-LD query plan against a ledger.
    pub async fn explain_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("explain", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Explain a SPARQL query plan against a ledger.
    pub async fn explain_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("explain", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-query",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    /// Explain a JSON-LD connection query plan (ledger specified via `from` in body).
    pub async fn explain_connection_jsonld(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("explain");
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Explain a SPARQL connection query plan (ledger specified via `FROM` clause).
    pub async fn explain_connection_sparql(
        &self,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("explain");
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-query",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    // =========================================================================
    // Insert
    // =========================================================================

    /// Insert JSON-LD data into a ledger.
    pub async fn insert_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("insert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Insert Turtle data into a ledger.
    pub async fn insert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("insert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "text/turtle",
            Some(RequestBody::Text(turtle)),
        )
        .await
    }

    // =========================================================================
    // Upsert
    // =========================================================================

    /// Upsert JSON-LD data into a ledger.
    pub async fn upsert_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("upsert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Upsert Turtle data into a ledger.
    pub async fn upsert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("upsert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "text/turtle",
            Some(RequestBody::Text(turtle)),
        )
        .await
    }

    // =========================================================================
    // Update (WHERE/DELETE/INSERT)
    // =========================================================================

    /// Execute a JSON-LD update (WHERE/DELETE/INSERT) via the update endpoint.
    pub async fn update_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("update", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Execute a SPARQL UPDATE via the update endpoint.
    pub async fn update_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("update", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-update",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    // =========================================================================
    // Ledger Info / Exists
    // =========================================================================

    /// Get ledger info from the remote server.
    ///
    /// When `graph` is `Some`, scopes the `stats` block to that named graph
    /// (well-known name or IRI).
    pub async fn ledger_info(
        &self,
        ledger: &str,
        graph: Option<&str>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let mut url = self.op_url("info", ledger);
        if let Some(g) = graph {
            url.push_str("?graph=");
            url.push_str(&urlencoding::encode(g));
        }
        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    /// Get a decoded commit from the remote server.
    ///
    /// Calls `GET {base_url}/show/{ledger}?commit={commit_ref}`.
    pub async fn commit_show(
        &self,
        ledger: &str,
        commit_ref: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!(
            "{}/show/{}?commit={}",
            self.base_url,
            encode_ledger_path(Self::ledger_tail(ledger)),
            urlencoding::encode(commit_ref),
        );
        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    /// Check if a ledger exists on the remote server.
    pub async fn ledger_exists(&self, ledger: &str) -> Result<bool, RemoteLedgerError> {
        let url = self.op_url("exists", ledger);

        let resp = self
            .build_request(reqwest::Method::GET, &url, "application/json", &None)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))?;
            Ok(body
                .get("exists")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false))
        } else if resp.status() == StatusCode::NOT_FOUND {
            Ok(false)
        } else if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            // Retry after refresh
            let resp2 = self
                .build_request(reqwest::Method::GET, &url, "application/json", &None)
                .send()
                .await
                .map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                let body: serde_json::Value = resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))?;
                Ok(body
                    .get("exists")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false))
            } else if resp2.status() == StatusCode::NOT_FOUND {
                Ok(false)
            } else {
                Err(Self::map_error(resp2).await)
            }
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    // =========================================================================
    // Create ledger
    // =========================================================================

    /// Create a new empty ledger on the remote server.
    ///
    /// Calls `POST {base_url}/create` with `{"ledger": "<alias>"}`.
    /// Returns 201 on success, 409 if the ledger already exists.
    pub async fn create_ledger(
        &self,
        ledger: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("create");
        let body = serde_json::json!({ "ledger": ledger });
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    /// Drop a ledger or graph source on the remote server.
    ///
    /// Calls `POST {base_url}/drop` with `{"ledger": "<name>", "hard": true|false}`.
    /// The server resolves the name as a ledger first, then as a graph source.
    pub async fn drop_resource(
        &self,
        name: &str,
        hard: bool,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("drop");
        let body = serde_json::json!({
            "ledger": name,
            "hard": hard,
        });
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    // =========================================================================
    // Reindex
    // =========================================================================

    /// Trigger a full reindex on the remote server.
    ///
    /// Calls `POST {base_url}/reindex` with `{"ledger": "<alias>"}`. The server
    /// rebuilds the ledger's index from commit history using whatever indexer
    /// settings it is configured with. Uses `REINDEX_TIMEOUT` (1 hour) because
    /// full rebuilds can legitimately exceed the default client timeout on
    /// large ledgers.
    ///
    /// An `opts` field is reserved in the request contract for future
    /// per-request overrides but is currently ignored by the server.
    pub async fn reindex(
        &self,
        ledger: &str,
    ) -> Result<fluree_db_api::wire::ReindexResponse, RemoteLedgerError> {
        let url = self.op_url_root("reindex");
        let body = serde_json::json!({ "ledger": ledger });
        let raw = self
            .send_json_with_timeout(
                reqwest::Method::POST,
                &url,
                "application/json",
                Some(RequestBody::Json(&body)),
                Self::REINDEX_TIMEOUT,
            )
            .await?;
        serde_json::from_value(raw)
            .map_err(|e| RemoteLedgerError::InvalidResponse(format!("reindex response: {e}")))
    }

    // =========================================================================
    // List ledgers
    // =========================================================================

    /// List all ledgers on the remote server.
    ///
    /// Calls `GET {base_url}/ledgers`. The response is expected to be a JSON
    /// array of objects with at minimum a `name` field.
    pub async fn list_ledgers(&self) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/ledgers", self.base_url);
        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    // =========================================================================
    // Branch management
    // =========================================================================

    /// Create a new branch on the remote server.
    ///
    /// Calls `POST {base_url}/branch` with a JSON body. `at` optionally
    /// specifies a historical commit to branch from (as accepted by
    /// `CommitRef::parse`, e.g. `"t:5"` or a hex digest / full CID).
    pub async fn create_branch(
        &self,
        ledger: &str,
        branch: &str,
        source: Option<&str>,
        at: Option<&str>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("branch");
        let mut body = serde_json::json!({
            "ledger": ledger,
            "branch": branch,
        });
        if let Some(s) = source {
            body["source"] = serde_json::Value::String(s.to_string());
        }
        if let Some(a) = at {
            body["at"] = serde_json::Value::String(a.to_string());
        }
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    /// Drop a branch on the remote server.
    ///
    /// Calls `POST {base_url}/drop-branch` with a JSON body.
    pub async fn drop_branch(
        &self,
        ledger: &str,
        branch: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("drop-branch");
        let body = serde_json::json!({
            "ledger": ledger,
            "branch": branch,
        });
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    /// Rebase a branch on the remote server.
    ///
    /// Calls `POST {base_url}/rebase` with a JSON body.
    pub async fn rebase_branch(
        &self,
        ledger: &str,
        branch: &str,
        strategy: Option<&str>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("rebase");
        let mut body = serde_json::json!({
            "ledger": ledger,
            "branch": branch,
        });
        if let Some(s) = strategy {
            body["strategy"] = serde_json::Value::String(s.to_string());
        }
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    /// Merge a branch into its target on the remote server.
    ///
    /// Calls `POST {base_url}/merge`.
    pub async fn merge_branch(
        &self,
        ledger: &str,
        source: &str,
        target: Option<&str>,
        strategy: Option<&str>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("merge");
        let mut body = serde_json::json!({
            "ledger": ledger,
            "source": source,
        });
        if let Some(t) = target {
            body["target"] = serde_json::Value::String(t.to_string());
        }
        if let Some(s) = strategy {
            body["strategy"] = serde_json::Value::String(s.to_string());
        }
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(&body)),
        )
        .await
    }

    /// List all branches for a ledger on the remote server.
    ///
    /// Calls `GET {base_url}/branch/{ledger}`.
    pub async fn list_branches(
        &self,
        ledger: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/branch/{}", self.base_url, ledger);
        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    /// Read-only merge preview between two branches on the remote server.
    ///
    /// Calls `GET {base_url}/merge-preview/{ledger}?source=&target=&max_commits=&max_conflict_keys=&include_conflicts=&include_conflict_details=&strategy=`.
    /// The ledger path segment is URL-encoded (via [`op_url`](Self::op_url))
    /// so names containing spaces, `?`, `#`, `%`, etc. produce well-formed URLs.
    #[allow(clippy::too_many_arguments)]
    pub async fn merge_preview(
        &self,
        ledger: &str,
        source: &str,
        target: Option<&str>,
        max_commits: Option<usize>,
        max_conflict_keys: Option<usize>,
        include_conflicts: Option<bool>,
        include_conflict_details: Option<bool>,
        strategy: Option<&str>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let mut url = self.op_url("merge-preview", ledger);
        let mut sep = '?';
        let push = |url: &mut String, sep: &mut char, key: &str, val: String| {
            url.push(*sep);
            url.push_str(key);
            url.push('=');
            url.push_str(&val);
            *sep = '&';
        };
        push(
            &mut url,
            &mut sep,
            "source",
            urlencoding::encode(source).into_owned(),
        );
        if let Some(t) = target {
            push(
                &mut url,
                &mut sep,
                "target",
                urlencoding::encode(t).into_owned(),
            );
        }
        if let Some(n) = max_commits {
            push(&mut url, &mut sep, "max_commits", n.to_string());
        }
        if let Some(n) = max_conflict_keys {
            push(&mut url, &mut sep, "max_conflict_keys", n.to_string());
        }
        if let Some(b) = include_conflicts {
            push(&mut url, &mut sep, "include_conflicts", b.to_string());
        }
        if let Some(b) = include_conflict_details {
            push(
                &mut url,
                &mut sep,
                "include_conflict_details",
                b.to_string(),
            );
        }
        if let Some(s) = strategy {
            push(
                &mut url,
                &mut sep,
                "strategy",
                urlencoding::encode(s).into_owned(),
            );
        }

        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    // =========================================================================
    // Push commits
    // =========================================================================

    /// Push precomputed commit blobs to the remote server.
    pub async fn push_commits(
        &self,
        ledger: &str,
        request: &fluree_db_api::PushCommitsRequest,
    ) -> Result<PushCommitsResponse, RemoteLedgerError> {
        let url = self.op_url("push", ledger);
        let body = serde_json::to_value(request)
            .map_err(|e| RemoteLedgerError::InvalidRequest(e.to_string()))?;

        // Deterministic across retries: allows servers to implement idempotent push replay.
        let idempotency_key = push_idempotency_key(ledger, request);
        let resp = self
            .send_json_with_headers(
                reqwest::Method::POST,
                &url,
                "application/json",
                &[("Idempotency-Key", idempotency_key)],
                Some(RequestBody::Json(&body)),
            )
            .await?;

        serde_json::from_value(resp).map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
    }

    // =========================================================================
    // Pack / Sync
    // =========================================================================

    /// Attempt to fetch a pack stream from the remote.
    ///
    /// Returns `Ok(Some(response))` on 200 (caller feeds to `ingest_pack_stream`),
    /// `Ok(None)` on 404/405/406 (pack not supported by server).
    pub async fn fetch_pack_response(
        &self,
        ledger: &str,
        request: &PackRequest,
    ) -> Result<Option<reqwest::Response>, RemoteLedgerError> {
        let url = self.op_url("pack", ledger);
        let body = serde_json::to_value(request)
            .map_err(|e| RemoteLedgerError::InvalidRequest(e.to_string()))?;

        let resp = self
            .send_raw(
                reqwest::Method::POST,
                &url,
                "application/json",
                Some("application/x-fluree-pack"),
                Some(RequestBody::Json(&body)),
            )
            .await?;

        let status = resp.status();
        if status.is_success() {
            Ok(Some(resp))
        } else if status == StatusCode::NOT_FOUND
            || status == StatusCode::METHOD_NOT_ALLOWED
            || status == StatusCode::NOT_ACCEPTABLE
            || status == StatusCode::NOT_IMPLEMENTED
        {
            Ok(None)
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Fetch the NsRecord via the storage proxy.
    ///
    /// Returns `Ok(Some(record))` on 200, `Ok(None)` on 404.
    pub async fn fetch_ns_record(
        &self,
        ledger: &str,
    ) -> Result<Option<NsRecord>, RemoteLedgerError> {
        let url = format!(
            "{}/storage/ns/{}",
            self.base_url,
            encode_ledger_segment(ledger)
        );

        let resp = self
            .send_raw(reqwest::Method::GET, &url, "application/json", None, None)
            .await?;

        let status = resp.status();
        if status.is_success() {
            let record: NsRecord = resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))?;
            Ok(Some(record))
        } else if status == StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    // =========================================================================
    // Fetch commits (export)
    // =========================================================================

    /// Fetch paginated commits from the remote server.
    ///
    /// Uses address-cursor pagination. Pass `cursor: None` for the first page
    /// (starts from head). Each response includes `next_cursor` for the next page,
    /// or `None` when genesis has been reached.
    pub async fn fetch_commits(
        &self,
        ledger: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ExportCommitsResponse, RemoteLedgerError> {
        let mut url = self.op_url("commits", ledger);
        url.push_str(&format!("?limit={limit}"));
        if let Some(c) = cursor {
            url.push_str(&format!("&cursor={}", urlencoding::encode(c)));
        }

        let resp = self
            .send_json(reqwest::Method::GET, &url, "application/json", None)
            .await?;

        serde_json::from_value(resp).map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
    }
}

/// Request body variants for the generic send method.
enum RequestBody<'a> {
    Json(&'a serde_json::Value),
    Text(&'a str),
}

fn extract_error_message(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Prefer the structured server error envelope when present:
    // {"error":"...","status":409,"@type":"err:...","cause":{...}}
    if trimmed.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(msg) = v.get("message").and_then(|m| m.as_str()) {
                return msg.to_string();
            }
            if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
                return err.to_string();
            }
        }
    }

    trimmed.to_string()
}

impl RemoteLedgerClient {
    // =========================================================================
    // Iceberg graph source operations
    // =========================================================================

    /// Map an Iceberg table as a graph source on the remote server.
    ///
    /// Calls `POST {base_url}/iceberg/map`.
    pub async fn iceberg_map(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url_root("iceberg/map");
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }
}

fn push_idempotency_key(ledger: &str, request: &fluree_db_api::PushCommitsRequest) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"fluree-push-v1\0");
    hasher.update(ledger.as_bytes());
    hasher.update([0u8]);

    for commit in &request.commits {
        hasher.update((commit.0.len() as u64).to_be_bytes());
        hasher.update(&commit.0);
    }

    let mut blobs: Vec<(&String, &fluree_db_api::Base64Bytes)> = request.blobs.iter().collect();
    blobs.sort_by_key(|(a, _)| *a);
    for (k, v) in blobs {
        hasher.update(k.as_bytes());
        hasher.update([0u8]);
        hasher.update((v.0.len() as u64).to_be_bytes());
        hasher.update(&v.0);
    }

    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_debug_hides_token() {
        let client = RemoteLedgerClient::new("http://localhost:8090", Some("secret".to_string()));
        let debug = format!("{client:?}");
        assert!(debug.contains("RemoteLedgerClient"));
        assert!(debug.contains("localhost:8090"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_client_strips_trailing_slash() {
        let client = RemoteLedgerClient::new("http://localhost:8090/fluree/", None);
        assert_eq!(client.base_url, "http://localhost:8090/fluree");
    }

    #[test]
    fn test_error_display() {
        let err = RemoteLedgerError::Unauthorized;
        let msg = format!("{err}");
        assert!(msg.contains("authentication failed"));
        assert!(msg.contains("fluree auth login"));

        let err = RemoteLedgerError::BadRequest("invalid query syntax".to_string());
        assert_eq!(format!("{err}"), "bad request: invalid query syntax");
    }

    #[test]
    fn test_extract_error_message_json_envelope() {
        let body = r#"{"error":"conflict","status":409,"@type":"err:test"}"#;
        assert_eq!(extract_error_message(body), "conflict");
    }

    #[test]
    fn test_extract_error_message_plain_text() {
        assert_eq!(extract_error_message("  nope  "), "nope");
    }

    #[test]
    fn test_encode_ledger_path_preserves_colon_and_slash() {
        // `:` is a valid pchar and MUST pass through so `ledger:branch`
        // round-trips correctly through the server's path router.
        assert_eq!(
            encode_ledger_path("trigger-test:testing"),
            "trigger-test:testing"
        );
        // `/` is preserved so nested ledger names land in the wildcard capture.
        assert_eq!(encode_ledger_path("org/name:branch"), "org/name:branch");
        // Truly unsafe chars still get encoded.
        assert_eq!(encode_ledger_path("a b"), "a%20b");
        assert_eq!(encode_ledger_path("a?b"), "a%3Fb");
        assert_eq!(encode_ledger_path("a#b"), "a%23b");
    }

    #[test]
    fn test_encode_ledger_segment_encodes_slash_preserves_colon() {
        assert_eq!(encode_ledger_segment("ledger:branch"), "ledger:branch");
        assert_eq!(encode_ledger_segment("a/b"), "a%2Fb");
    }

    #[test]
    fn test_op_url_branched_ledger() {
        let client = RemoteLedgerClient::new("http://localhost:8090/fluree", None);
        assert_eq!(
            client.op_url("query", "trigger-test:testing"),
            "http://localhost:8090/fluree/query/trigger-test:testing"
        );
    }

    #[test]
    fn test_op_url_merge_preview_encodes_unsafe_chars() {
        // Regression: merge-preview previously interpolated the ledger raw,
        // breaking on names with spaces/?/#/% etc. The implementation now
        // routes through `op_url` so these get URL-encoded the same as
        // every other ledger-tailed endpoint.
        let client = RemoteLedgerClient::new("http://localhost:8090/fluree", None);
        assert_eq!(
            client.op_url("merge-preview", "weird name?:branch#x"),
            "http://localhost:8090/fluree/merge-preview/weird%20name%3F:branch%23x"
        );
    }

    #[test]
    fn test_with_refresh_config() {
        let client = RemoteLedgerClient::new("http://localhost:8090", Some("token".to_string()))
            .with_refresh(RefreshConfig {
                exchange_url: "http://localhost:8090/auth/exchange".to_string(),
                refresh_token: "rt_123".to_string(),
            });
        let debug = format!("{client:?}");
        assert!(debug.contains("has_refresh: true"));
    }
}
