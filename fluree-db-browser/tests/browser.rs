//! Browser-only tests for the driver's fetch and IndexedDB paths.
//!
//! Run with `wasm-pack test --headless --chrome fluree-db-browser`. Native
//! logic (protocol, coalescing, residency, eviction planning, peer
//! assembly) is covered by the crate's ordinary unit tests; these exercise
//! only what needs a real browser.

#![cfg(target_arch = "wasm32")]

use fluree_db_browser::driver::IdbCache;
use fluree_db_browser::{
    start_driver, BrowserIoConfig, CacheConfig, ChannelSseSource, DriverSleeper, HeadSink,
    HttpTransport, IoJob, RemoteEvent, TransportError, TransportRequest, WasmFetchTransport,
    WriteBehindGauge,
};
use fluree_db_core::{ContentId, ContentKind};
use gloo_timers::future::TimeoutFuture;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn no_cache_config() -> BrowserIoConfig {
    BrowserIoConfig {
        cache: CacheConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cache_config(db_name: &str, budget: u64) -> CacheConfig {
    CacheConfig {
        enabled: true,
        db_name: db_name.to_string(),
        budget_bytes: budget,
        low_water_ratio: 0.5,
        access_flush_interval: Duration::from_secs(3600),
    }
}

fn cid_of(bytes: &[u8]) -> ContentId {
    ContentId::new(ContentKind::IndexLeaf, bytes)
}

/// Wedge an IndexedDB name deterministically: hold a live connection, then
/// queue a `deleteDatabase` behind it. The delete cannot run while the
/// connection is open, and every later open of that name queues behind the
/// delete — silently, with no `success`, no `error`, not even `blocked`.
/// Closing the returned holder releases the queue.
///
/// An ABSENT database opens fine, which is why no fresh-profile test can
/// produce this shape.
async fn wedge(name: &'static str) -> std::rc::Rc<IdbCache> {
    let holder = IdbCache::open(&cache_config(name, 1024))
        .await
        .expect("holder opens");
    wasm_bindgen_futures::spawn_local(async move {
        let _ = IdbCache::delete_database(name).await;
    });
    TimeoutFuture::new(100).await;
    holder
}

/// Poll `probe` until it holds, up to `budget_ms`. Returns whether it did —
/// callers assert on that, so a timeout is a failure with a message, never
/// a silent pass.
async fn settles_within<F: FnMut() -> bool>(budget_ms: u32, mut probe: F) -> bool {
    let step = 25;
    let mut waited = 0;
    while waited < budget_ms {
        if probe() {
            return true;
        }
        TimeoutFuture::new(step).await;
        waited += step;
    }
    probe()
}

#[wasm_bindgen_test]
async fn fetch_data_url_round_trips_status_headers_and_body() {
    let io = start_driver(no_cache_config());
    let transport = WasmFetchTransport::new(io.clone(), Duration::from_secs(5));
    let resp = transport
        .execute(TransportRequest::get(
            "data:application/octet-stream;base64,aGVsbG8gd2FzbQ==",
        ))
        .await
        .expect("data URL fetch succeeds");
    assert_eq!(resp.status.as_u16(), 200);
    assert_eq!(&resp.body[..], b"hello wasm");
    let content_type = resp
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.contains("application/octet-stream"),
        "content-type header must be collected, got {content_type:?}"
    );
    io.shutdown();
}

#[wasm_bindgen_test]
async fn unreachable_host_is_a_connect_error() {
    let io = start_driver(no_cache_config());
    let transport = WasmFetchTransport::new(io.clone(), Duration::from_secs(5));
    let err = transport
        .execute(TransportRequest::get("http://127.0.0.1:9/nothing"))
        .await
        .expect_err("connection refused");
    assert!(matches!(err, TransportError::Connect(_)), "got {err:?}");
    io.shutdown();
}

#[wasm_bindgen_test]
async fn idb_cache_puts_gets_evicts_and_survives_reopen() {
    let name = "fluree-cas-test-evict";
    let _ = IdbCache::delete_database(name).await;
    let config = cache_config(name, 25);

    let a = vec![1u8; 10];
    let b = vec![2u8; 10];
    let c = vec![3u8; 10];
    let (ka, kb, kc) = (cid_of(&a), cid_of(&b), cid_of(&c));

    {
        let cache = IdbCache::open(&config).await.expect("open");
        assert!(cache.is_empty());
        cache.put(ka.clone(), Arc::from(a.clone())).await;
        cache.put(kb.clone(), Arc::from(b.clone())).await;
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_bytes(), 20);
        let hit = cache.get(&ka).await.expect("a persisted");
        assert_eq!(&hit[..], &a[..]);
        // 20 + 10 > 25: evict to the low-water mark (12) minus incoming
        // (10) → both older entries go.
        cache.put(kc.clone(), Arc::from(c.clone())).await;
        assert!(cache.get(&kb).await.is_none(), "LRU victim evicted");
        assert!(cache.get(&ka).await.is_none());
        assert_eq!(&cache.get(&kc).await.expect("c persisted")[..], &c[..]);
        assert_eq!(cache.len(), 1);
        cache.flush_access_times().await;
        cache.close();
    }

    // Reopen: the index is rebuilt from `meta`, payloads are still there.
    let cache = IdbCache::open(&config).await.expect("reopen");
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.total_bytes(), 10);
    assert_eq!(&cache.get(&kc).await.expect("c after reopen")[..], &c[..]);
    cache.close();
    IdbCache::delete_database(name).await.expect("cleanup");
}

#[wasm_bindgen_test]
async fn driver_serves_cache_jobs_end_to_end() {
    let name = "fluree-cas-test-driver";
    let _ = IdbCache::delete_database(name).await;
    let config = BrowserIoConfig {
        cache: cache_config(name, 1024),
        ..Default::default()
    };
    let io = start_driver(config);

    let payload = b"driver cache payload".to_vec();
    let key = cid_of(&payload);

    // Miss before any put.
    let (tx, rx) = oneshot::channel();
    io.send(IoJob::CacheGet {
        key: key.clone(),
        reply: tx,
    })
    .unwrap();
    assert!(rx.await.unwrap().is_none());

    io.send(IoJob::CachePut {
        key: key.clone(),
        bytes: Arc::from(payload.clone()),
        permit: None,
    })
    .unwrap();
    // Write-behind is async: poll until the put lands instead of trusting
    // a fixed sleep (CI boxes are slow).
    let mut hit = None;
    for _ in 0..40 {
        TimeoutFuture::new(50).await;
        let (tx, rx) = oneshot::channel();
        io.send(IoJob::CacheGet {
            key: key.clone(),
            reply: tx,
        })
        .unwrap();
        if let Some(bytes) = rx.await.unwrap() {
            hit = Some(bytes);
            break;
        }
    }
    let hit = hit.expect("persisted through the driver within 2s");
    assert_eq!(&hit[..], &payload[..]);

    io.shutdown();
    TimeoutFuture::new(100).await;
    IdbCache::delete_database(name).await.expect("cleanup");
}

/// The driver's SSE job streams a real fetch body through the shared pump:
/// a `data:text/event-stream` URL delivers its frames via the response's
/// `ReadableStream`, the pump parses and dispatches them, and the sink's
/// stop signal ends the pump after the first ledger event (the data URL
/// would otherwise reconnect forever).
#[wasm_bindgen_test]
async fn sse_driver_streams_frames_from_a_data_url() {
    use async_trait::async_trait;
    use fluree_db_nameservice_sync::{run_head_stream, HeadStreamConfig};
    use std::sync::Mutex;
    use tokio::sync::watch;

    struct StopOnLedgerSink {
        events: Mutex<Vec<String>>,
        stop: watch::Sender<bool>,
    }

    #[async_trait]
    impl HeadSink for StopOnLedgerSink {
        async fn on_event(&self, event: RemoteEvent) {
            let tag = match &event {
                RemoteEvent::Connected => "connected".to_string(),
                RemoteEvent::LedgerUpdated(r) => format!("ledger:{}@{}", r.ledger_id, r.commit_t),
                other => format!("{other:?}"),
            };
            self.events.lock().unwrap().push(tag);
            if matches!(event, RemoteEvent::LedgerUpdated(_)) {
                let _ = self.stop.send(true);
            }
        }
    }

    let io = start_driver(no_cache_config());
    let frame = "event: ns-record\ndata: {\"action\":\"ns-record\",\"kind\":\"ledger\",\"resource_id\":\"books:main\",\"record\":{\"ledger_id\":\"books:main\",\"branch\":\"main\",\"commit_head_id\":null,\"commit_t\":9,\"index_head_id\":null,\"index_t\":3,\"retracted\":false},\"emitted_at\":\"now\"}\n\n";
    let url = format!("data:text/event-stream,{}", urlencoding::encode(frame));
    let source = ChannelSseSource::new(io.clone(), url, None);
    let (stop_tx, stop_rx) = watch::channel(false);
    let sink = StopOnLedgerSink {
        events: Mutex::new(Vec::new()),
        stop: stop_tx,
    };
    let sleeper = DriverSleeper::new(io.clone());
    let config = HeadStreamConfig {
        reconnect_initial: Duration::from_millis(5),
        reconnect_max: Duration::from_millis(20),
        reconnect_multiplier: 2.0,
    };

    run_head_stream(&source, &sink, &sleeper, config, stop_rx).await;

    let events = sink.events.lock().unwrap().clone();
    assert!(
        events.contains(&"connected".to_string()),
        "must connect: {events:?}"
    );
    assert!(
        events.contains(&"ledger:books:main@9".to_string()),
        "ledger event must flow through the real stream: {events:?}"
    );
    io.shutdown();
}

/// A job the driver never dispatches must fail TYPED, not hang forever.
///
/// This is the regression guard for the worst failure class this crate has:
/// the per-request timeout lives inside the driver (`fetch::execute`'s
/// `AbortController`), so it only ever applies to a request that actually
/// started. Anything that stops the driver servicing jobs used to leave the
/// caller awaiting a reply that would never come — with the job holding the
/// reply sender, so even the "driver dropped the request" arm stayed silent.
/// A peer in that state connects, reports healthy, issues no HTTP request at
/// all, and answers nothing, forever.
///
/// Here the receiver is held but never drained, which is exactly a driver
/// that is alive and not dispatching. The deadline must fire, and it must
/// say what timed out.
#[wasm_bindgen_test]
async fn undispatched_job_times_out_instead_of_hanging() {
    // Held, never polled: the sender stays open, so nothing closes the
    // channel — the job is simply never picked up.
    let (io, _never_drained) = fluree_db_browser::IoHandle::channel();
    let transport = WasmFetchTransport::new(io, Duration::from_millis(10));

    let err = transport
        .execute(TransportRequest::get("http://example.invalid/blocked"))
        .await
        .expect_err("an undispatched job must not resolve");

    match err {
        TransportError::Timeout(message) => {
            assert!(
                message.contains("http://example.invalid/blocked"),
                "the error must name what timed out: {message}"
            );
            assert!(
                message.contains("did not dispatch"),
                "the error must distinguish 'never started' from a request timeout: {message}"
            );
        }
        other => panic!("expected a typed Timeout, got {other:?}"),
    }
}

/// The deadline must NOT pre-empt a driver that is merely slow: a reply that
/// arrives within it still wins, so this guard cannot mask real behavior.
#[wasm_bindgen_test]
async fn a_slow_but_live_driver_still_wins_the_race() {
    let (io, mut rx) = fluree_db_browser::IoHandle::channel();
    let transport = WasmFetchTransport::new(io, Duration::from_secs(30));

    wasm_bindgen_futures::spawn_local(async move {
        if let Some(IoJob::Fetch { reply, .. }) = rx.recv().await {
            TimeoutFuture::new(50).await;
            let _ = reply.send(Err(TransportError::Request("driver answered".into())));
        }
    });

    let err = transport
        .execute(TransportRequest::get("http://example.invalid/slow"))
        .await
        .expect_err("the driver's own error");
    assert!(
        matches!(err, TransportError::Request(ref m) if m == "driver answered"),
        "the driver's reply must win, not the deadline: {err:?}"
    );
}

/// The invariant: **the driver serves fetches even when the cache never
/// opens.** This is the shape no cold-profile test can produce, and the one
/// that shipped broken behind a doc comment that already claimed it.
///
/// An ABSENT database opens fine — which is why every fresh-profile test
/// passes and why this bug was invisible for so long. A WEDGED one never
/// returns *any* event: not `success`, not `error`, not even `blocked`.
/// Holding a connection open and then starting a `deleteDatabase` that can
/// never complete wedges the name deterministically, exactly reproducing the
/// profile this was first observed in.
#[wasm_bindgen_test]
async fn driver_serves_fetches_when_the_cache_never_opens() {
    // No leading delete: this name is wedged on purpose below, and a delete
    // of a wedged name would itself hang. The test runner starts a fresh
    // browser, so the database does not exist yet.
    let name = "fluree-cas-test-wedged";

    // Hold a live connection, then queue a delete behind it. The delete can
    // never run while this connection is open, and every later open of this
    // name queues behind the delete — silently, forever.
    let holder = IdbCache::open(&cache_config(name, 1024))
        .await
        .expect("holder opens");
    wasm_bindgen_futures::spawn_local(async move {
        let _ = IdbCache::delete_database(name).await;
    });
    TimeoutFuture::new(100).await;

    let config = BrowserIoConfig {
        cache: cache_config(name, 1024),
        // Long enough that the test proves fetches are served WHILE the open
        // is still outstanding, not merely after it gave up.
        cache_open_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let io = start_driver(config);

    // 1. A fetch is served, with the cache open still hanging.
    let transport = WasmFetchTransport::new(io.clone(), Duration::from_secs(5));
    let resp = transport
        .execute(TransportRequest::get(
            "data:application/octet-stream;base64,aGVsbG8gd2FzbQ==",
        ))
        .await
        .expect("a wedged cache must not stop the driver serving fetches");
    assert_eq!(&resp.body[..], b"hello wasm");

    // 2. A cache READ answers a miss promptly instead of hanging. This is
    //    the one that matters most: `fetch_into_residency` awaits this
    //    before every network fetch, so a read that waits would put the
    //    wedged cache straight back on the query's critical path.
    let (tx, rx) = oneshot::channel();
    io.send(IoJob::CacheGet {
        key: cid_of(b"anything"),
        reply: tx,
    })
    .unwrap();
    let miss = rx.await.expect("the cache read must answer, not hang");
    assert!(miss.is_none(), "an unopened cache reads as a miss");

    // Teardown must not await anything queued behind the wedge — that is
    // what the wedge means. Closing the holder releases the queued delete,
    // which then completes on its own and takes the database with it.
    io.shutdown();
    holder.close();
    TimeoutFuture::new(200).await;
}

/// **Rule 2 of the driver's contract: the cache open is BOUNDED, and
/// resolving it is what releases the writes parked behind it.**
///
/// `driver_serves_fetches_when_the_cache_never_opens` sets a 30 s
/// `cache_open_timeout` precisely so the bound never fires — it pins rules
/// 1 and 3 and cannot pin this one. Nothing else exercised the timeout, or
/// a `CachePut`'s write-behind permit being credited when the cache never
/// opens, which is the coupling that makes a wedged cache visible to fetch
/// admission at all.
///
/// Both halves are asserted, because either alone is passable by a broken
/// driver: the put is still parked (its permit still charged) while the
/// open is outstanding — a driver that silently dropped every put would
/// fail here — and its permit is credited once the bound fires.
#[wasm_bindgen_test]
async fn a_wedged_cache_open_times_out_and_frees_the_write_behind_it() {
    let name = "fluree-cas-test-open-timeout";
    let holder = wedge(name).await;

    let io = start_driver(BrowserIoConfig {
        cache: cache_config(name, 1024),
        cache_open_timeout: Duration::from_millis(750),
        ..Default::default()
    });

    let gauge = WriteBehindGauge::new(4096);
    let payload = vec![7u8; 64];
    let permit = gauge.acquire(payload.len() as u64).await;
    assert_eq!(gauge.outstanding(), 64, "the permit is charged up front");
    io.send(IoJob::CachePut {
        key: cid_of(&payload),
        bytes: Arc::from(payload.clone()),
        permit: Some(permit),
    })
    .unwrap();

    // Inside the bound: the write really does WAIT for the open. (Writes
    // wait where reads do not — dropping a put loses persistence for
    // exactly the blocks a cold start fetches first.)
    TimeoutFuture::new(200).await;
    assert_eq!(
        gauge.outstanding(),
        64,
        "a queued put must wait for the open, not be dropped on the floor"
    );

    // Past the bound: the open resolves as unavailable, and that is what
    // releases the parked write — bytes abandoned, permit credited.
    let freed = settles_within(3_000, || gauge.outstanding() == 0).await;
    assert!(
        freed,
        "the bounded open must release the parked write's permit; still {} outstanding",
        gauge.outstanding()
    );

    io.shutdown();
    holder.close();
    TimeoutFuture::new(200).await;
}

/// **Rule 4: parked writes are bounded, so a wedged cache cannot starve
/// fetch admission.**
///
/// A parked `CachePut` holds its write-behind permit, and
/// `WriteBehindGauge::acquire` is awaited INSIDE the fetch-slot scope — so
/// parked puts spend fetch admission. Let them fill the budget and every
/// fetch slot blocks until the open resolves. Past the parked bound (a
/// quarter of the write-behind budget) a put must therefore credit its
/// permit immediately instead of waiting.
///
/// The discriminator is exact rather than directional: with the bound at
/// 100 bytes, the first 64-byte put parks and the second does not, so the
/// gauge must read exactly one permit's worth while the open is still
/// outstanding.
#[wasm_bindgen_test]
async fn parked_writes_past_the_bound_credit_their_permits_at_once() {
    let name = "fluree-cas-test-parked-bound";
    let holder = wedge(name).await;

    let io = start_driver(BrowserIoConfig {
        cache: cache_config(name, 1024),
        cache_open_timeout: Duration::from_millis(1_500),
        // A quarter of this is the parked bound: 100 bytes.
        write_behind_budget_bytes: 400,
        ..Default::default()
    });

    let gauge = WriteBehindGauge::new(4096);
    let first = vec![1u8; 64];
    let second = vec![2u8; 64];
    let p1 = gauge.acquire(64).await;
    let p2 = gauge.acquire(64).await;
    assert_eq!(gauge.outstanding(), 128, "both permits charged");

    for (bytes, permit) in [(first, p1), (second, p2)] {
        io.send(IoJob::CachePut {
            key: cid_of(&bytes),
            bytes: Arc::from(bytes),
            permit: Some(permit),
        })
        .unwrap();
    }

    // Well inside `cache_open_timeout`, so nothing here is the bound in
    // rule 2 firing: the second put is past the PARKED bound (64 + 64 >
    // 100) and gives its permit back at once, while the first is still
    // parked on the open.
    let bounded = settles_within(500, || gauge.outstanding() == 64).await;
    assert!(
        bounded,
        "the put past the parked bound must credit its permit immediately, \
         leaving exactly one parked; outstanding = {}",
        gauge.outstanding()
    );

    // And the survivor is released when the open finally resolves.
    let freed = settles_within(3_000, || gauge.outstanding() == 0).await;
    assert!(
        freed,
        "the parked write is released when the open resolves; still {} outstanding",
        gauge.outstanding()
    );

    io.shutdown();
    holder.close();
    TimeoutFuture::new(200).await;
}

/// **A timed-out open must not be DROPPED.**
///
/// `request()` in `driver/idb.rs` clears its `onsuccess`/`onerror` only
/// after its await returns, and `open_db` clears `onupgradeneeded` /
/// `onblocked` only on the normal path. Dropping the open future therefore
/// drops those `Closure`s while a live `IDBOpenDBRequest` still references
/// them, and a LATE event then invokes a dropped closure — wasm-bindgen
/// throws "closure invoked recursively or after being dropped", uncaught,
/// which in the shell reaches `worker.onerror` and recycles the engine
/// (losing residency and every subscription) over an IndexedDB that was
/// merely slow.
///
/// That late event is reachable exactly where the bound is aimed: a
/// genuinely blocked open resolves once the other connection closes, which
/// can easily be past the timeout. Here the wedge is released AFTER the
/// bound fires, so the driver's open completes with nobody waiting.
///
/// Ran-marker: IndexedDB serializes requests per database name, so this
/// test's own open — queued last — can only succeed once the driver's open
/// has run. Without it the "no uncaught errors" assertion would be vacuous.
#[wasm_bindgen_test]
async fn an_open_that_lands_after_the_timeout_does_not_fire_a_dropped_closure() {
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::{JsCast, JsValue};

    let errors = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let listener = {
        let errors = Arc::clone(&errors);
        Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
            let message = js_sys::Reflect::get(&event, &JsValue::from_str("message"))
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "<no message>".to_string());
            errors.lock().unwrap().push(message);
        })
    };
    let global: web_sys::EventTarget = js_sys::global().unchecked_into();
    global
        .add_event_listener_with_callback("error", listener.as_ref().unchecked_ref())
        .expect("listen for uncaught errors");

    let name = "fluree-cas-test-late-open";
    let holder = wedge(name).await;

    let io = start_driver(BrowserIoConfig {
        cache: cache_config(name, 1024),
        cache_open_timeout: Duration::from_millis(300),
        ..Default::default()
    });
    // Past the bound: the driver has given up on this open.
    TimeoutFuture::new(600).await;

    // Release the wedge. The queued delete completes, and the driver's open
    // — which nobody is waiting for any more — then runs to completion,
    // firing `upgradeneeded` and `success` on a request whose handlers must
    // still be alive.
    holder.close();
    // Bounded, and in its OWN task for the same reason the driver's open is:
    // a dropped open would leave dangling handlers of the test's own making.
    let (probe_tx, probe_rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        // Closing matters: an open connection left behind would block the
        // next `deleteDatabase` on this name forever.
        let ok = match IdbCache::open(&cache_config(name, 1024)).await {
            Ok(cache) => {
                cache.close();
                true
            }
            Err(_) => false,
        };
        let _ = probe_tx.send(ok);
    });
    let drained = matches!(
        futures::future::select(probe_rx, TimeoutFuture::new(5_000)).await,
        futures::future::Either::Left((Ok(true), _))
    );
    assert!(
        drained,
        "ran-marker: the wedge must clear, which means the driver's open (queued first) ran"
    );
    // Give any late event a turn to dispatch before reading the log.
    TimeoutFuture::new(200).await;

    let seen = errors.lock().unwrap().clone();
    assert!(
        seen.is_empty(),
        "a late open event must not invoke a dropped closure: {seen:?}"
    );

    global
        .remove_event_listener_with_callback("error", listener.as_ref().unchecked_ref())
        .expect("stop listening");
    io.shutdown();
    TimeoutFuture::new(100).await;
}
