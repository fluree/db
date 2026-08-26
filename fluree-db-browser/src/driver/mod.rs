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

use crate::bridge::{IoHandle, IoReceiver};
use crate::config::BrowserIoConfig;
use crate::protocol::IoJob;
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
    let cache: Option<Rc<IdbCache>> = if config.cache.enabled {
        match IdbCache::open(&config.cache).await {
            Ok(cache) => Some(cache),
            Err(e) => {
                tracing::warn!(error = %e, "IndexedDB cache unavailable; running without persistence");
                None
            }
        }
    } else {
        None
    };

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
            IoJob::CacheGet { key, reply } => match cache.clone() {
                Some(cache) => spawn_local(async move {
                    let _ = reply.send(cache.get(&key).await);
                }),
                None => {
                    let _ = reply.send(None);
                }
            },
            IoJob::CachePut { key, bytes, permit } => {
                if let Some(cache) = cache.clone() {
                    spawn_local(async move {
                        cache.put(key, bytes).await;
                        // Credit the write-behind gauge only once the write
                        // finished (or failed) — this is the backpressure.
                        drop(permit);
                    });
                }
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

    if let Some(cache) = cache {
        cache.flush_access_times().await;
        cache.close();
    }
}
