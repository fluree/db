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
use fluree_db_nameservice_sync::{TransportError, TransportRequest, TransportResponse};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

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
