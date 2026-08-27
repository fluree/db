//! The browser I/O driver: the one task that owns JavaScript handles.
//!
//! Started with [`start_driver`], it drains [`IoJob`]s from the channel and
//! dispatches each to its own `spawn_local` task so requests overlap.
//! `fetch` (with `AbortController` timeouts) lives in [`fetch`]; the
//! IndexedDB block cache lives in [`idb`]. If the cache cannot be opened
//! (private mode, quota, missing API) the driver keeps serving fetches and
//! answers every cache lookup as a miss.

pub mod fetch;
pub mod idb;
pub mod sse;

use crate::bridge::{IoHandle, IoReceiver};
use crate::config::BrowserIoConfig;
use crate::protocol::IoJob;
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen_futures::spawn_local;

pub use idb::IdbCache;

/// Start the driver on the current JavaScript event loop (window or
/// worker) and return the handle the engine side enqueues jobs on.
pub fn start_driver(config: BrowserIoConfig) -> IoHandle {
    let (handle, rx) = IoHandle::channel();
    spawn_local(run(rx, config));
    handle
}

async fn run(mut rx: IoReceiver, config: BrowserIoConfig) {
    // The cache is an optimization and MUST NOT gate job servicing.
    //
    // This await used to sit in front of the loop below, which made every
    // fetch in the process depend on IndexedDB opening. An open can hang
    // indefinitely rather than fail — a version upgrade held up by another
    // connection fires `blocked`, which is neither `success` nor `error` —
    // and a hang there deadlocked the whole peer: no job ever dispatched,
    // so no request was issued, no per-request timeout could fire (those
    // live inside `fetch::execute`), and nothing surfaced an error. The
    // symptom is a peer that connects, reports healthy, and then answers
    // nothing forever, with zero HTTP requests to show for it.
    //
    // Opening concurrently restores the degraded mode this module already
    // documents: until the cache lands, lookups miss and writes are
    // dropped, which costs a cold-start window of persistence and nothing
    // else — CAS entries are immutable and always re-fetchable.
    let cache: Rc<RefCell<Option<Rc<IdbCache>>>> = Rc::new(RefCell::new(None));
    if config.cache.enabled {
        let slot = Rc::clone(&cache);
        let cache_config = config.cache.clone();
        spawn_local(async move {
            match IdbCache::open(&cache_config).await {
                Ok(opened) => *slot.borrow_mut() = Some(opened),
                Err(e) => {
                    tracing::warn!(error = %e, "IndexedDB cache unavailable; running without persistence");
                }
            }
        });
    }
    // Snapshot the slot per job; `borrow()` never spans an await.
    let current = |slot: &Rc<RefCell<Option<Rc<IdbCache>>>>| slot.borrow().clone();

    while let Some(job) = rx.recv().await {
        match job {
            IoJob::Fetch {
                req,
                timeout,
                reply,
            } => {
                spawn_local(async move {
                    let _ = reply.send(fetch::execute(req, timeout).await);
                });
            }
            IoJob::CacheGet { key, reply } => match current(&cache) {
                Some(cache) => spawn_local(async move {
                    let _ = reply.send(cache.get(&key).await);
                }),
                None => {
                    let _ = reply.send(None);
                }
            },
            IoJob::CachePut { key, bytes, permit } => {
                if let Some(cache) = current(&cache) {
                    spawn_local(async move {
                        cache.put(key, bytes).await;
                        // Credit the write-behind gauge only once the write
                        // finished (or failed) — this is the backpressure.
                        drop(permit);
                    });
                }
            }
            IoJob::SseOpen {
                url,
                headers,
                ready,
                chunks,
            } => {
                spawn_local(sse::run(url, headers, ready, chunks));
            }
            IoJob::Sleep { duration, reply } => {
                let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
                spawn_local(async move {
                    gloo_timers::future::TimeoutFuture::new(millis).await;
                    let _ = reply.send(());
                });
            }
            IoJob::Shutdown => break,
        }
    }

    let opened = current(&cache);
    if let Some(cache) = opened {
        cache.flush_access_times().await;
        cache.close();
    }
}
