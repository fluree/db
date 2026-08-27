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
    // fetch in the process depend on IndexedDB opening. That open can hang
    // with NO event at all — not `success`, not `error`, not even `blocked`
    // — when the database has been wedged (observed in the wild: a
    // `deleteDatabase` that can never complete leaves later opens of that
    // name queued forever, while other names open instantly). A hang there
    // deadlocked the whole peer: no job dispatched, so no request issued, no
    // per-request timeout able to fire (those live inside `fetch::execute`),
    // and no error anywhere. A peer that connects, reports healthy, and
    // answers nothing, with zero HTTP requests to show for it.
    //
    // Three rules keep that impossible, and `driver_serves_fetches_when_the_
    // cache_never_opens` pins all three:
    //
    //  1. The loop starts immediately; the open runs beside it.
    //  2. The open is BOUNDED. One that has not landed by now never will,
    //     and resolving it as unavailable is what releases everything
    //     parked behind it.
    //  3. Reads never wait on it. `CacheGet` answers a miss while the open
    //     is in flight, because a cache read sits on the query's critical
    //     path (`BrowserCasStorage::fetch_into_residency` awaits it before
    //     fetching) — waiting there would put a wedged cache right back in
    //     front of every query. A miss is always safe: it costs a refetch.
    //
    // Writes DO wait for (2), because dropping a `CachePut` silently loses
    // persistence for exactly the blocks a cold start fetches first. They
    // are off the critical path, and their write-behind permit is released
    // on every path, so a wedged cache cannot starve fetch admission either.
    let cache: Rc<RefCell<Option<Rc<IdbCache>>>> = Rc::new(RefCell::new(None));
    // `false` until the open has succeeded, failed, or timed out. Always
    // eventually `true`, so nothing parks on it forever.
    let (resolved_tx, resolved_rx) = watch::channel(false);
    if config.cache.enabled {
        let slot = Rc::clone(&cache);
        let cache_config = config.cache.clone();
        let open_timeout = config.cache_open_timeout;
        spawn_local(async move {
            let millis = u32::try_from(open_timeout.as_millis()).unwrap_or(u32::MAX);
            let opening = IdbCache::open(&cache_config);
            futures::pin_mut!(opening);
            match futures::future::select(opening, gloo_timers::future::TimeoutFuture::new(millis))
                .await
            {
                futures::future::Either::Left((Ok(opened), _)) => {
                    *slot.borrow_mut() = Some(opened);
                }
                futures::future::Either::Left((Err(e), _)) => {
                    tracing::warn!(error = %e, "IndexedDB cache unavailable; running without persistence");
                }
                futures::future::Either::Right(((), _)) => {
                    tracing::warn!(
                        timeout = ?open_timeout,
                        "IndexedDB open did not complete (the database may be wedged);                          running without persistence"
                    );
                }
            }
            let _ = resolved_tx.send(true);
        });
    } else {
        let _ = resolved_tx.send(true);
    }

    /// The cache if the open has already resolved successfully; `None` while
    /// it is still in flight or if it failed. NEVER waits — see rule 3.
    fn cache_now(slot: &Rc<RefCell<Option<Rc<IdbCache>>>>) -> Option<Rc<IdbCache>> {
        slot.borrow().clone()
    }

    /// The cache once the open has resolved, waiting if it is still in
    /// flight. Bounded by rule 2, so this always returns.
    async fn cache_settled(
        slot: &Rc<RefCell<Option<Rc<IdbCache>>>>,
        resolved: &watch::Receiver<bool>,
    ) -> Option<Rc<IdbCache>> {
        // Check-then-drop: a `RefCell` borrow must never span an await.
        let ready = *resolved.borrow();
        if !ready {
            let mut rx = resolved.clone();
            let _ = rx.wait_for(|r| *r).await;
        }
        let settled = slot.borrow().clone();
        settled
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
            // Rule 3: never wait. A miss while the open is in flight costs
            // one refetch; waiting would hand a wedged cache the power to
            // hang every query.
            IoJob::CacheGet { key, reply } => match cache_now(&cache) {
                Some(cache) => {
                    spawn_local(async move {
                        let _ = reply.send(cache.get(&key).await);
                    });
                }
                None => {
                    let _ = reply.send(None);
                }
            },
            IoJob::CachePut { key, bytes, permit } => {
                let slot = Rc::clone(&cache);
                let resolved = resolved_rx.clone();
                spawn_local(async move {
                    if let Some(cache) = cache_settled(&slot, &resolved).await {
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
