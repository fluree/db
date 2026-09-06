//! Peer assembly: the browser storage and nameservice wired into a `Fluree`.
//!
//! The shape is the native peer's exactly —
//! `FlureeBuilder::memory().build_with(storage, NameServiceMode::ReadOnly(ns))`
//! — with the browser transport injected into the shared proxy clients.
//! Nothing here is browser-specific except [`connect`], which starts the
//! real driver; [`build_peer`] takes any [`IoHandle`] and is what native
//! tests use.

use crate::bridge::{IoHandle, WasmFetchTransport};
use crate::cas::BrowserCasStorage;
use crate::config::BrowserIoConfig;
use crate::heads::{
    events_url, ChannelSseSource, DriverSleeper, HeadChange, HeadRegistry, HeadTracker,
    PeerHeadSink,
};
use fluree_db_api::{Fluree, FlureeBuilder, NameServiceMode};
use fluree_db_nameservice_sync::{
    run_head_stream, HeadStreamConfig, ProxyNameService, ProxyReadMode, ProxyStorage,
};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::watch;

/// A connected browser peer: the engine plus handles to its I/O layer.
pub struct BrowserPeer {
    fluree: Fluree,
    cas: BrowserCasStorage,
    io: IoHandle,
    api_base: String,
    token: String,
    config: BrowserIoConfig,
    heads: Arc<HeadRegistry>,
}

impl std::fmt::Debug for BrowserPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserPeer")
            .field("cas", &self.cas)
            .finish_non_exhaustive()
    }
}

impl BrowserPeer {
    /// The engine. Queries go through the usual `Fluree` API; ledger heads
    /// resolve through the remote nameservice and CAS bytes through
    /// [`cas`](Self::cas).
    pub fn fluree(&self) -> &Fluree {
        &self.fluree
    }

    /// The storage layer, for prefetching, pinning, and stats.
    pub fn cas(&self) -> &BrowserCasStorage {
        &self.cas
    }

    /// The driver handle.
    pub fn io(&self) -> &IoHandle {
        &self.io
    }

    /// Stop the driver. In-flight jobs already spawned still complete;
    /// new I/O fails with a transport error.
    pub fn shutdown(&self) {
        self.io.shutdown();
    }

    /// Take the engine, dropping the peer handles (the driver keeps running
    /// as long as the storage inside the engine holds its handle).
    pub fn into_fluree(self) -> Fluree {
        self.fluree
    }

    /// Register a callback for ledger head changes seen by head tracking.
    ///
    /// Callbacks run on the pump's task: keep them cheap and non-blocking
    /// (a JS-facing shell bridges to its own dispatch rather than calling
    /// into JS from here — the callback must be `Send + Sync`).
    pub fn on_head_change(&self, callback: impl Fn(&HeadChange) + Send + Sync + 'static) {
        self.heads.add(Box::new(callback));
    }

    /// Build the head-tracking pump for `ledgers` (empty = everything the
    /// token may see). Returns the tracker and the pump future — the
    /// caller spawns it ([`start_head_tracking`](Self::start_head_tracking)
    /// does so on the browser event loop; native tests use `tokio::spawn`).
    ///
    /// Head changes refresh the cached ledger between queries (the same
    /// `LedgerManager::notify` path the native peer uses — an in-flight
    /// query keeps its frozen view; the next `db()` sees the new head) and
    /// fan out to [`on_head_change`](Self::on_head_change) callbacks.
    pub fn head_stream(&self, ledgers: &[String]) -> (HeadTracker, BoxFuture<'static, ()>) {
        let source = ChannelSseSource::new(
            self.io.clone(),
            events_url(&self.api_base, ledgers),
            Some(self.token.clone()),
        );
        let sink = PeerHeadSink {
            fluree: self.fluree.clone(),
            registry: Arc::clone(&self.heads),
        };
        let sleeper = DriverSleeper::new(self.io.clone());
        let stream_config = HeadStreamConfig {
            reconnect_initial: self.config.reconnect_initial,
            reconnect_max: self.config.reconnect_max,
            reconnect_multiplier: 2.0,
        };
        let (stop_tx, stop_rx) = watch::channel(false);
        let future = Box::pin(async move {
            run_head_stream(&source, &sink, &sleeper, stream_config, stop_rx).await;
        });
        (HeadTracker::new(stop_tx), future)
    }

    /// Start head tracking on the browser event loop.
    #[cfg(target_arch = "wasm32")]
    pub fn start_head_tracking(&self, ledgers: &[String]) -> HeadTracker {
        let (tracker, future) = self.head_stream(ledgers);
        wasm_bindgen_futures::spawn_local(future);
        tracker
    }
}

/// Assemble a peer over an existing driver handle.
///
/// `api_base` is the remote's versioned API base (for example
/// `https://data.example.com/v1/fluree`); `token` is a bearer token with
/// `fluree.storage.*` scope for the ledgers to be read (a full-read grant —
/// see the storage proxy's authorization model).
pub fn build_peer(
    io: IoHandle,
    api_base: impl Into<String>,
    token: impl Into<String>,
    config: &BrowserIoConfig,
) -> BrowserPeer {
    let api_base = api_base.into();
    let token = token.into();

    let block_transport = Arc::new(WasmFetchTransport::new(io.clone(), config.fetch_timeout));
    let ns_transport = Arc::new(WasmFetchTransport::new(
        io.clone(),
        config.nameservice_timeout,
    ));

    let proxy = ProxyStorage::from_api_base_with_transport(
        api_base.clone(),
        token.clone(),
        ProxyReadMode::Raw,
        block_transport,
    );
    let nameservice = ProxyNameService::from_api_base_with_transport(
        api_base.clone(),
        token.clone(),
        ns_transport,
    );
    let cas = BrowserCasStorage::new(proxy, io.clone(), config);

    let fluree = FlureeBuilder::memory().build_with(
        cas.clone(),
        NameServiceMode::ReadOnly(Arc::new(nameservice)),
    );

    BrowserPeer {
        fluree,
        cas,
        io,
        api_base: api_base.trim_end_matches('/').to_string(),
        token,
        config: config.clone(),
        heads: Arc::new(HeadRegistry::default()),
    }
}

/// Start the browser driver and assemble a peer over it.
#[cfg(target_arch = "wasm32")]
pub fn connect(
    api_base: impl Into<String>,
    token: impl Into<String>,
    config: BrowserIoConfig,
) -> BrowserPeer {
    let io = crate::driver::start_driver(config.clone());
    build_peer(io, api_base, token, &config)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::cas::tests::{spawn_mock_driver, MockState, API_BASE};
    use std::sync::Mutex;
    use std::time::Duration;

    /// The nameservice and storage both ride the injected transport: a
    /// ledger open resolves its head through `GET /storage/ns/{alias}` with
    /// the bearer header, and an unknown ledger surfaces as an open error
    /// rather than a transport failure.
    #[tokio::test]
    async fn peer_resolves_heads_through_the_proxy_nameservice() {
        let state = Arc::new(Mutex::new(MockState::default()));
        let (io, rx) = IoHandle::channel();
        let driver = spawn_mock_driver(rx, Arc::clone(&state));
        let config = BrowserIoConfig {
            nameservice_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let peer = build_peer(io, API_BASE, "tok", &config);

        let err = peer
            .fluree()
            .db("mydb:main")
            .await
            .expect_err("unknown ledger cannot open");
        let text = err.to_string();
        assert!(
            !text.contains("driver"),
            "must not be a transport failure: {text}"
        );

        {
            let s = state.lock().unwrap();
            let ns_calls: Vec<_> = s
                .url_log
                .iter()
                .filter(|(url, _)| url.contains("/storage/ns/"))
                .collect();
            assert!(
                !ns_calls.is_empty(),
                "head resolution must hit the nameservice"
            );
            let (url, headers) = ns_calls[0];
            assert_eq!(url, &format!("{API_BASE}/storage/ns/mydb%3Amain"));
            assert_eq!(headers, &vec![("authorization", "Bearer tok".to_string())]);
        }

        peer.shutdown();
        driver.await.unwrap();
    }

    /// End-to-end head tracking against the mock driver: the pump opens the
    /// events URL with the right subscription and headers, a ledger event
    /// reaches the registered callback (and the notify path — the ledger is
    /// not cached, so notify is a NotLoaded no-op), and the tracker stops
    /// the pump.
    #[tokio::test]
    async fn head_tracking_dispatches_callbacks_and_stops() {
        let state = Arc::new(Mutex::new(MockState::default()));
        let frame = "event: ns-record\ndata: {\"action\":\"ns-record\",\"kind\":\"ledger\",\"resource_id\":\"books:main\",\"record\":{\"ledger_id\":\"books:main\",\"branch\":\"main\",\"commit_head_id\":null,\"commit_t\":5,\"index_head_id\":null,\"index_t\":2,\"retracted\":false},\"emitted_at\":\"now\"}\n\n";
        state
            .lock()
            .unwrap()
            .sse_script
            .push_back(vec![frame.as_bytes().to_vec()]);
        let (io, rx) = IoHandle::channel();
        let driver = spawn_mock_driver(rx, Arc::clone(&state));
        let config = BrowserIoConfig {
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(20),
            ..Default::default()
        };
        let peer = build_peer(io, API_BASE, "tok", &config);

        let received = Arc::new(Mutex::new(Vec::new()));
        let arrived = Arc::new(tokio::sync::Notify::new());
        {
            let received = Arc::clone(&received);
            let arrived = Arc::clone(&arrived);
            peer.on_head_change(move |change| {
                received.lock().unwrap().push(change.clone());
                arrived.notify_one();
            });
        }

        let (tracker, future) = peer.head_stream(&["books:main".to_string()]);
        let pump = tokio::spawn(future);

        tokio::time::timeout(Duration::from_secs(2), arrived.notified())
            .await
            .expect("head change must arrive");
        {
            let received = received.lock().unwrap();
            assert_eq!(
                received[0],
                crate::heads::HeadChange {
                    ledger_id: "books:main".to_string(),
                    commit_t: 5,
                    index_t: 2,
                }
            );
        }
        {
            let s = state.lock().unwrap();
            let (url, headers) = &s.sse_log[0];
            assert_eq!(url, &format!("{API_BASE}/events?ledger=books%3Amain"));
            assert!(headers.contains(&("accept", "text/event-stream".to_string())));
            assert!(headers.contains(&("authorization", "Bearer tok".to_string())));
        }

        tracker.stop();
        tokio::time::timeout(Duration::from_secs(2), pump)
            .await
            .expect("pump must stop")
            .unwrap();
        peer.shutdown();
        driver.await.unwrap();
    }
}
