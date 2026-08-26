//! Browser-only tests for the driver's fetch and IndexedDB paths.
//!
//! Run with `wasm-pack test --headless --chrome fluree-db-browser`. Native
//! logic (protocol, coalescing, residency, eviction planning, peer
//! assembly) is covered by the crate's ordinary unit tests; these exercise
//! only what needs a real browser.

#![cfg(target_arch = "wasm32")]

use fluree_db_browser::driver::IdbCache;
use fluree_db_browser::{
    start_driver, BrowserIoConfig, CacheConfig, HttpTransport, IoJob, TransportError,
    TransportRequest, WasmFetchTransport,
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
    let mut config = BrowserIoConfig::default();
    config.cache = cache_config(name, 1024);
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
    })
    .unwrap();
    // Write-behind: give the put task a moment.
    TimeoutFuture::new(200).await;

    let (tx, rx) = oneshot::channel();
    io.send(IoJob::CacheGet {
        key: key.clone(),
        reply: tx,
    })
    .unwrap();
    let hit = rx.await.unwrap().expect("persisted through the driver");
    assert_eq!(&hit[..], &payload[..]);

    io.shutdown();
    TimeoutFuture::new(100).await;
    IdbCache::delete_database(name).await.expect("cleanup");
}
