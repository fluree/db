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
use tokio::sync::watch;
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
    // indefinitely rather than fail — a version change held up by another
    // connection fires `blocked`, which is neither `success` nor `error` —
    // and a hang there deadlocked the whole peer: no job ever dispatched,
    // so no request was issued, no per-request timeout could fire (those
    // live inside `fetch::execute`), and nothing surfaced an error. The
    // symptom is a peer that connects, reports healthy, and then answers
    // nothing forever, with zero HTTP requests to show for it.
    //
    // So the open runs concurrently and the loop starts immediately. Cache
    // jobs that arrive before it resolves WAIT for it rather than being
    // dropped: a dropped `CachePut` silently loses persistence for exactly
    // the blocks a cold start fetches first, and would also strand its
    // write-behind permit. Fetches never wait on any of this.
    let cache: Rc<RefCell<Option<Rc<IdbCache>>>> = Rc::new(RefCell::new(None));
    // `false` until the open has succeeded OR failed; cache jobs park on it,
    // and it is always eventually set so nothing parks forever.
    let (resolved_tx, resolved_rx) = watch::channel(false);
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
            let _ = resolved_tx.send(true);
        });
    } else {
        let _ = resolved_tx.send(true);
    }

    /// Resolve the cache for one job, waiting for the open if it is still in
    /// flight. Returns `None` once the open has resolved unsuccessfully.
    async fn cache_for(
        slot: &Rc<RefCell<Option<Rc<IdbCache>>>>,
        resolved: &watch::Receiver<bool>,
    ) -> Option<Rc<IdbCache>> {
        // Check-then-drop: a `RefCell` borrow must never span an await.
        let ready = *resolved.borrow();
        if !ready {
            let mut rx = resolved.clone();
            let _ = rx.wait_for(|r| *r).await;
        }
        let hit = slot.borrow().clone();
        hit
    }

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
            IoJob::CacheGet { key, reply } => {
                let slot = Rc::clone(&cache);
                let resolved = resolved_rx.clone();
                spawn_local(async move {
                    let hit = match cache_for(&slot, &resolved).await {
                        Some(cache) => cache.get(&key).await,
                        None => None,
                    };
                    let _ = reply.send(hit);
                });
            }
            IoJob::CachePut { key, bytes, permit } => {
                let slot = Rc::clone(&cache);
                let resolved = resolved_rx.clone();
                spawn_local(async move {
                    if let Some(cache) = cache_for(&slot, &resolved).await {
                        cache.put(key, bytes).await;
                    }
                    // Credit the write-behind gauge once the write finished,
                    // failed, or was abandoned for want of a cache — this is
                    // the backpressure, and it must never be stranded.
                    drop(permit);
                });
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

    let opened = cache.borrow().clone();
    if let Some(cache) = opened {
        cache.flush_access_times().await;
        cache.close();
    }
}
