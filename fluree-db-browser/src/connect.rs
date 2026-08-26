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
use fluree_db_api::{Fluree, FlureeBuilder, NameServiceMode};
use fluree_db_nameservice_sync::{ProxyNameService, ProxyReadMode, ProxyStorage};
use std::sync::Arc;

/// A connected browser peer: the engine plus handles to its I/O layer.
pub struct BrowserPeer {
    fluree: Fluree,
    cas: BrowserCasStorage,
    io: IoHandle,
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
    let nameservice = ProxyNameService::from_api_base_with_transport(api_base, token, ns_transport);
    let cas = BrowserCasStorage::new(proxy, io.clone(), config);

    let fluree = FlureeBuilder::memory().build_with(
        cas.clone(),
        NameServiceMode::ReadOnly(Arc::new(nameservice)),
    );

    BrowserPeer { fluree, cas, io }
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
}
