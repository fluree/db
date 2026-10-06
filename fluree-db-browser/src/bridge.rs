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
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

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
}

impl WasmFetchTransport {
    /// A transport whose requests are executed by the driver behind `io`
    /// and abandoned (aborted) after `timeout`.
    pub fn new(io: IoHandle, timeout: Duration) -> Self {
        Self { io, timeout }
    }

    /// The per-request deadline this transport stamps on its jobs.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[async_trait]
impl HttpTransport for WasmFetchTransport {
    async fn execute(&self, req: TransportRequest) -> Result<TransportResponse, TransportError> {
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
