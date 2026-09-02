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
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use tokio::sync::{oneshot, watch};
use wasm_bindgen_futures::spawn_local;

pub use idb::IdbCache;

/// The share of the write-behind budget that may sit PARKED waiting for
/// the cache open to resolve. Keeping the parked set strictly smaller than
/// the budget is what leaves fetch admission headroom while the open is
/// still in flight — see rule 4 in [`run`].
const PARKED_PUT_BUDGET_DIVISOR: u64 = 4;

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
    // Four rules keep that impossible:
    //
    //  1. The loop starts immediately; the open runs beside it.
    //     (`driver_serves_fetches_when_the_cache_never_opens`)
    //  2. The open is BOUNDED. One that has not landed by now never will,
    //     and resolving it as unavailable is what releases everything
    //     parked behind it.
    //     (`a_wedged_cache_open_times_out_and_frees_the_write_behind_it`)
    //  3. Reads never wait on it. `CacheGet` answers a miss while the open
    //     is in flight, because a cache read sits on the query's critical
    //     path (`BrowserCasStorage::fetch_into_residency` awaits it before
    //     fetching) — waiting there would put a wedged cache right back in
    //     front of every query. A miss is always safe: it costs a refetch.
    //     (`driver_serves_fetches_when_the_cache_never_opens`)
    //  4. Writes DO wait for (2) — dropping a `CachePut` silently loses
    //     persistence for exactly the blocks a cold start fetches first —
    //     but only up to a BOUNDED number of bytes. Each parked put still
    //     holds its write-behind permit, and `WriteBehindGauge::acquire`
    //     is awaited inside the fetch-slot scope (`cas.rs`), so parked
    //     puts consume fetch admission: let them fill the whole budget and
    //     every fetch slot blocks until the open resolves. Past
    //     `parked_cap` a put therefore drops its bytes AND credits its
    //     permit at once, trading persistence for those blocks (the same
    //     trade an unavailable cache already makes) for admission.
    //     (`parked_writes_past_the_bound_credit_their_permits_at_once`)
    //
    // Residual bound, stated honestly: while the open is in flight, at
    // most `parked_cap` = a quarter of the write-behind budget can be held
    // by parked writes, so a fetch of up to the remaining three quarters
    // is always admissible and the pipeline cannot stall — and the window
    // itself is at most `cache_open_timeout`. It is a bound, not "cannot
    // happen": a single block larger than three quarters of the budget
    // fetched while the open is still in flight would still wait for the
    // open to resolve (default budget 64 MiB, so ~48 MiB — orders of
    // magnitude above any index block).
    let cache: Rc<RefCell<Option<Rc<IdbCache>>>> = Rc::new(RefCell::new(None));
    // `false` until the open has succeeded, failed, or timed out. Always
    // eventually `true`, so nothing parks on it forever.
    let (resolved_tx, resolved_rx) = watch::channel(false);
    if config.cache.enabled {
        let slot = Rc::clone(&cache);
        let cache_config = config.cache.clone();
        let open_timeout = config.cache_open_timeout;
        spawn_local(async move {
            let millis = crate::config::timer_millis(open_timeout);
            // The open runs in a task of its OWN and reports through a
            // oneshot; the timeout races the RECEIVER, never the open.
            // Dropping an in-flight `IdbCache::open` would drop the
            // wasm-bindgen closures that are still registered on a live
            // `IDBOpenDBRequest` (`request()` clears its handlers only
            // after the await returns), and a late `success` / `error` /
            // `upgradeneeded` event would then invoke a dropped closure —
            // wasm-bindgen's "closure invoked recursively or after being
            // dropped", uncaught, which in the shell reaches
            // `worker.onerror` and recycles the whole engine. That is
            // reachable exactly where this bound is aimed: a genuinely
            // blocked open resolves once the other connection closes,
            // which can easily be past the timeout. Running the open to
            // completion in its own task means it always clears its own
            // handlers, whether or not anyone is still waiting.
            let (open_tx, open_rx) = oneshot::channel();
            spawn_local(async move {
                let _ = open_tx.send(IdbCache::open(&cache_config).await);
            });
            match futures::future::select(open_rx, gloo_timers::future::TimeoutFuture::new(millis))
                .await
            {
                futures::future::Either::Left((Ok(Ok(opened)), _)) => {
                    *slot.borrow_mut() = Some(opened);
                }
                futures::future::Either::Left((Ok(Err(e)), _)) => {
                    tracing::warn!(error = %e, "IndexedDB cache unavailable; running without persistence");
                }
                futures::future::Either::Left((Err(_), _)) => {
                    tracing::warn!(
                        "IndexedDB open task went away without answering; running without persistence"
                    );
                }
                futures::future::Either::Right(((), open_rx)) => {
                    tracing::warn!(
                        timeout = ?open_timeout,
                        "IndexedDB open did not complete (the database may be wedged); running without persistence"
                    );
                    // This session runs without persistence, but a late
                    // arrival must not be left holding a connection that
                    // would block another tab's version change.
                    spawn_local(async move {
                        if let Ok(Ok(late)) = open_rx.await {
                            late.close();
                        }
                    });
                }
            }
            let _ = resolved_tx.send(true);
        });
    } else {
        let _ = resolved_tx.send(true);
    }
    // Bytes of write-behind currently parked on the open (rule 4).
    let parked_bytes: Rc<Cell<u64>> = Rc::new(Cell::new(0));
    let parked_cap = config.write_behind_budget_bytes / PARKED_PUT_BUDGET_DIVISOR;
    let parked_warned: Rc<Cell<bool>> = Rc::new(Cell::new(false));

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
                    let _ = reply
                        .send(fetch::execute(req, timeout, config.residency_budget_bytes as u64).await);
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
                // Rule 4: a put that has to park on the open counts against
                // `parked_cap`, because it parks holding fetch admission.
                let parking = !*resolved_rx.borrow();
                let size = if parking { bytes.len() as u64 } else { 0 };
                if parking && parked_bytes.get().saturating_add(size) > parked_cap {
                    if !parked_warned.replace(true) {
                        tracing::warn!(
                            parked_cap,
                            "IndexedDB open is still outstanding and the parked write-behind is at \
                             its bound; dropping blocks (this session may start cold again) so \
                             fetch admission keeps its headroom"
                        );
                    }
                    // Bytes AND permit go now: the gauge must not stay
                    // charged for something nobody is going to write.
                    drop(bytes);
                    drop(permit);
                    continue;
                }
                parked_bytes.set(parked_bytes.get() + size);
                let slot = Rc::clone(&cache);
                let resolved = resolved_rx.clone();
                let parked_bytes = Rc::clone(&parked_bytes);
                spawn_local(async move {
                    if let Some(cache) = cache_settled(&slot, &resolved).await {
                        cache.put(key, bytes).await;
                    }
                    parked_bytes.set(parked_bytes.get().saturating_sub(size));
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
                spawn_local(sse::run(
                    url,
                    headers.0,
                    config.nameservice_timeout,
                    ready,
                    chunks,
                ));
            }
            IoJob::Sleep { duration, reply } => {
                let millis = crate::config::timer_millis(duration);
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
