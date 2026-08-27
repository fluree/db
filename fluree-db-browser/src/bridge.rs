//! The channel bridge: `Send + Sync` handles on the engine side, JS-owning
//! driver on the other.
//!
//! The engine's storage and nameservice traits box `Send` futures on every
//! target, so nothing that awaits a JS promise directly can implement them.
//! [`WasmFetchTransport`] satisfies `HttpTransport` by holding only an
//! [`IoHandle`] (a channel sender) and awaiting a oneshot reply; the driver
//! task that actually calls `fetch` lives behind the channel.

use crate::protocol::IoJob;
use async_trait::async_trait;
use fluree_db_nameservice_sync::{
    HttpTransport, TransportError, TransportRequest, TransportResponse,
};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Shared, hot-swappable bearer token.
///
/// Long-lived subscribed sessions outlive their tokens: the shell's auth
/// flow refreshes the bearer mid-session, and every I/O surface holding a
/// clone of this cell — the fetch transports, the SSE source — picks the
/// new value up on its next request or connect, with no teardown and no
/// loss of warm per-store state. [`crate::BrowserPeer::set_token`] is the
/// public entry.
///
/// Per-request cost is one read-lock acquisition plus the header-string
/// allocation the request needed anyway. `Debug` redacts the token.
#[derive(Clone)]
pub struct TokenCell {
    inner: Arc<RwLock<String>>,
}

impl TokenCell {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(token.into())),
        }
    }

    /// Replace the token. Requests already in flight keep the header they
    /// were stamped with; everything issued afterwards carries the new one.
    pub fn set(&self, token: impl Into<String>) {
        *self.inner.write().unwrap_or_else(PoisonError::into_inner) = token.into();
    }

    /// The current `authorization` header value (`Bearer {token}`).
    pub fn bearer_header(&self) -> String {
        format!(
            "Bearer {}",
            self.inner.read().unwrap_or_else(PoisonError::into_inner)
        )
    }
}

impl std::fmt::Debug for TokenCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenCell(<redacted>)")
    }
}

/// Receiving end of the job channel, consumed by a driver.
pub type IoReceiver = mpsc::UnboundedReceiver<IoJob>;

/// The driver is gone: its receiver was dropped or it shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("browser I/O driver is not running")]
pub struct IoClosed;

/// Cheap, cloneable, `Send + Sync` handle to the driver's job queue.
#[derive(Debug, Clone)]
pub struct IoHandle {
    tx: mpsc::UnboundedSender<IoJob>,
}

impl IoHandle {
    /// Create a job channel. The receiver is handed to a driver (the real
    /// wasm driver, or a mock consumer in tests).
    pub fn channel() -> (IoHandle, IoReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (IoHandle { tx }, rx)
    }

    /// Enqueue a job. Fails only when the driver is gone.
    pub fn send(&self, job: IoJob) -> Result<(), IoClosed> {
        self.tx.send(job).map_err(|_| IoClosed)
    }

    /// Whether the driver has gone away.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Ask the driver to stop. Idempotent; a missing driver is not an error.
    pub fn shutdown(&self) {
        let _ = self.tx.send(IoJob::Shutdown);
    }
}

/// `HttpTransport` over the channel bridge.
///
/// Meets every constraint of the transport contract by construction: the
/// future returned by `execute` awaits only channel operations (so it is
/// `Send`), status handling stays with the caller (the driver replies `Ok`
/// for any HTTP status), bodies are fully buffered, and the timeout is
/// carried per request so one driver can serve transports with different
/// deadlines (block fetches vs nameservice lookups).
#[derive(Debug, Clone)]
pub struct WasmFetchTransport {
    io: IoHandle,
    timeout: Duration,
    token: Option<TokenCell>,
}

impl WasmFetchTransport {
    /// A transport whose requests are executed by the driver behind `io`
    /// and abandoned (aborted) after `timeout`.
    pub fn new(io: IoHandle, timeout: Duration) -> Self {
        Self {
            io,
            timeout,
            token: None,
        }
    }

    /// Source the `authorization` header from `cell` on every request,
    /// REPLACING any authorization header the caller baked into the
    /// request. This is the hot-refresh seam: the proxy clients construct
    /// requests with the token they were built with, and a transport
    /// carrying a cell overrides it with the current one, so
    /// [`TokenCell::set`] takes effect mid-session without rebuilding the
    /// peer.
    pub fn with_token(mut self, cell: TokenCell) -> Self {
        self.token = Some(cell);
        self
    }

    /// The per-request deadline this transport stamps on its jobs.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[async_trait]
impl HttpTransport for WasmFetchTransport {
    async fn execute(&self, req: TransportRequest) -> Result<TransportResponse, TransportError> {
        let mut req = req;
        if let Some(cell) = &self.token {
            req.headers
                .retain(|(name, _)| !name.eq_ignore_ascii_case("authorization"));
            req.headers.push(("authorization", cell.bearer_header()));
        }
        let (reply, rx) = oneshot::channel();
        self.io
            .send(IoJob::Fetch {
                req,
                timeout: self.timeout,
                reply,
            })
            .map_err(|e| TransportError::Request(e.to_string()))?;
        rx.await.map_err(|_| {
            TransportError::Request("browser I/O driver dropped the request".to_string())
        })?
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, StatusCode};

    #[tokio::test]
    async fn execute_round_trips_through_a_mock_driver() {
        let (io, mut rx) = IoHandle::channel();
        let transport = WasmFetchTransport::new(io, Duration::from_secs(7));

        let driver = tokio::spawn(async move {
            let job = rx.recv().await.expect("one job");
            match job {
                IoJob::Fetch {
                    req,
                    timeout,
                    reply,
                } => {
                    assert_eq!(req.url, "http://origin.example/x");
                    assert_eq!(timeout, Duration::from_secs(7));
                    let mut headers = HeaderMap::new();
                    headers.insert("etag", "\"abc\"".parse().unwrap());
                    let _ = reply.send(Ok(TransportResponse {
                        status: StatusCode::NOT_FOUND,
                        headers,
                        body: Bytes::from_static(b"nope"),
                    }));
                }
                other => panic!("unexpected job {other:?}"),
            }
        });

        let resp = transport
            .execute(TransportRequest::get("http://origin.example/x"))
            .await
            .expect("driver replied");
        // Any status is Ok — status semantics belong to the caller.
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
        assert_eq!(resp.headers.get("etag").unwrap(), "\"abc\"");
        assert_eq!(&resp.body[..], b"nope");
        driver.await.unwrap();
    }

    /// A transport carrying a token cell REPLACES any baked authorization
    /// header with the cell's current value — the hot-refresh contract —
    /// and never duplicates it. `Debug` on the cell redacts the token.
    #[tokio::test]
    async fn token_cell_stamps_and_replaces_the_authorization_header() {
        let (io, mut rx) = IoHandle::channel();
        let cell = TokenCell::new("first");
        let transport =
            WasmFetchTransport::new(io, Duration::from_secs(1)).with_token(cell.clone());

        // Echo every authorization header value back as the body.
        let driver = tokio::spawn(async move {
            for _ in 0..2 {
                match rx.recv().await.expect("job") {
                    IoJob::Fetch { req, reply, .. } => {
                        let auth: Vec<String> = req
                            .headers
                            .iter()
                            .filter(|(name, _)| *name == "authorization")
                            .map(|(_, value)| value.clone())
                            .collect();
                        let _ = reply.send(Ok(TransportResponse {
                            status: StatusCode::OK,
                            headers: HeaderMap::new(),
                            body: Bytes::from(auth.join("|")),
                        }));
                    }
                    other => panic!("unexpected job {other:?}"),
                }
            }
        });

        // The proxy clients bake the token they were built with; the
        // transport overrides it with the cell's current value.
        let stale = TransportRequest::get("http://origin.example/x")
            .header("authorization", "Bearer stale".to_string());
        let resp = transport.execute(stale).await.expect("reply");
        assert_eq!(&resp.body[..], b"Bearer first");

        cell.set("second");
        let resp = transport
            .execute(TransportRequest::get("http://origin.example/x"))
            .await
            .expect("reply");
        assert_eq!(&resp.body[..], b"Bearer second");
        driver.await.unwrap();

        let debugged = format!("{cell:?} / {transport:?}");
        assert!(
            !debugged.contains("second") && !debugged.contains("first"),
            "Debug must redact the token: {debugged}"
        );
    }

    #[tokio::test]
    async fn execute_fails_cleanly_when_the_driver_is_gone() {
        let (io, rx) = IoHandle::channel();
        drop(rx);
        assert!(io.is_closed());
        let transport = WasmFetchTransport::new(io, Duration::from_secs(1));
        let err = transport
            .execute(TransportRequest::get("http://origin.example/x"))
            .await
            .expect_err("no driver");
        assert!(matches!(err, TransportError::Request(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn dropped_reply_surfaces_as_request_error() {
        let (io, mut rx) = IoHandle::channel();
        let transport = WasmFetchTransport::new(io, Duration::from_secs(1));
        let driver = tokio::spawn(async move {
            // Take the job and drop the reply sender without answering.
            let _ = rx.recv().await;
        });
        let err = transport
            .execute(TransportRequest::get("http://origin.example/x"))
            .await
            .expect_err("dropped reply");
        assert!(matches!(err, TransportError::Request(_)), "got {err:?}");
        driver.await.unwrap();
    }
}
