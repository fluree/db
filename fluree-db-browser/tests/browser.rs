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
