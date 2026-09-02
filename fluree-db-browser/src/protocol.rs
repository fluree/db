//! The job protocol between the engine side and the browser I/O driver.
//!
//! Everything that must touch JavaScript — `fetch`, `AbortController`,
//! IndexedDB — runs inside a single driver task that owns those handles.
//! The engine side never holds a JS value: it enqueues an [`IoJob`] over an
//! unbounded channel and awaits a oneshot reply. Both halves are plain Rust
//! data, so the protocol is exercised natively by a mock consumer in tests.

use crate::gauge::WriteBehindPermit;
use bytes::Bytes;
use fluree_db_core::ContentId;
use fluree_db_nameservice_sync::{
    SseConnectError, TransportError, TransportRequest, TransportResponse,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// SSE request headers, carrying a bearer `authorization` value.
///
/// A newtype purely so `Debug` redacts the token: `IoJob` derives `Debug`,
/// and one `tracing::debug!(?job, …)` or `panic!("unexpected job {other:?}")`
/// in the driver would otherwise write the user's bearer token to the browser
/// console, where any extension or error-reporting SDK on the page can read
/// it. The sibling `TransportRequest` (fluree-db-nameservice-sync) redacts for
/// the same reason; this keeps `IoJob`'s derived `Debug` correct without a
/// hand-written impl per variant.
pub struct SseHeaders(pub Vec<(&'static str, String)>);

impl std::fmt::Debug for SseHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, value)| {
                if name.eq_ignore_ascii_case("authorization") {
                    (*name, "[redacted]")
                } else {
                    (*name, value.as_str())
                }
            }))
            .finish()
    }
}

/// One unit of work for the driver.
#[derive(Debug)]
pub enum IoJob {
    /// Execute an HTTP request. The driver replies with the complete
    /// response for **any** status, or a transport error when no readable
    /// response was produced (timeout, network failure, body read failure).
    Fetch {
        req: TransportRequest,
        /// Deadline for the whole request including body read.
        timeout: Duration,
        reply: oneshot::Sender<Result<TransportResponse, TransportError>>,
    },
    /// Look a block up in the persistent cache. `None` means miss (or no
    /// cache configured / cache unavailable).
    CacheGet {
        key: ContentId,
        reply: oneshot::Sender<Option<Bytes>>,
    },
    /// Persist a block. Write-behind: the sender has already served the
    /// bytes and never waits for the write. The `Arc` is the residency
    /// tier's own allocation, so enqueueing costs a refcount, not a copy.
    /// The permit sizes the block against the write-behind gauge and
    /// releases when the driver finishes (or drops) the write — the driver
    /// just moves it into the write task.
    CachePut {
        key: ContentId,
        bytes: Arc<[u8]>,
        permit: Option<WriteBehindPermit>,
    },
    /// Open a fetch-streamed SSE connection. Streaming deliberately does
    /// NOT go through the `HttpTransport` seam (whose contract is
    /// full-body buffering): the JS `ReadableStream` stays inside the
    /// driver, which forwards raw body chunks over `chunks`. `ready`
    /// resolves once response headers arrive (`Err` classifies the
    /// failure); the chunk sender dropping means clean stream end, a
    /// `chunks` `Err` item means a mid-stream failure, and the receiver
    /// dropping tells the driver to cancel the stream and abort the fetch.
    SseOpen {
        url: String,
        headers: SseHeaders,
        ready: oneshot::Sender<Result<(), SseConnectError>>,
        chunks: mpsc::UnboundedSender<Result<Bytes, String>>,
    },
    /// Reply after `duration`. The engine side has no timer of its own —
    /// JS owns the clock — so bounded waits (deferred-insert deadlines)
    /// borrow the driver's.
    Sleep {
        duration: Duration,
        reply: oneshot::Sender<()>,
    },
    /// Stop the driver after draining what it already spawned.
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bearer token must never reach a debug string. `IoJob` derives
    /// `Debug`, so this is enforced by `SseHeaders`' own impl; one stray
    /// `debug!(?job)`/`panic!("{job:?}")` in the driver would otherwise leak
    /// the token to the browser console.
    #[test]
    fn sse_headers_debug_redacts_the_bearer_token() {
        let headers = SseHeaders(vec![
            ("accept", "text/event-stream".to_string()),
            ("authorization", "Bearer super-secret-token".to_string()),
        ]);
        let rendered = format!("{headers:?}");
        assert!(
            !rendered.contains("super-secret-token"),
            "token leaked: {rendered}"
        );
        assert!(rendered.contains("[redacted]"), "not redacted: {rendered}");
        // Non-sensitive headers stay visible for debugging.
        assert!(rendered.contains("text/event-stream"), "{rendered}");
    }
}
