//! Browser (wasm32) I/O layer for a Fluree peer.
//!
//! This crate is the client half of the server's peer proxy mode, hosted in
//! a browser: it turns the shared proxy clients from
//! `fluree-db-nameservice-sync` into a working `Fluree` instance whose
//! bytes arrive over `fetch`, are cached forever by CID in IndexedDB, and
//! are served to the engine's synchronous read path from an in-memory
//! residency tier.
//!
//! # Shape
//!
//! ```text
//!  engine (Send futures)            │ channel bridge │   driver task (owns JS)
//!  ─────────────────────────────────┼────────────────┼──────────────────────────
//!  Fluree ─ StorageContentStore ─┐  │                │
//!    resolve_cached_bytes (sync) ─┼─▶ BrowserCasStorage ── ResidencyTier (Arc<[u8]>)
//!    get / read_bytes (async)    ─┘  │   ├─ coalesce  │
//!                                    │   ├─ CacheGet/CachePut ──▶ IndexedDB
//!                                    │   └─ ProxyStorage ── WasmFetchTransport ──▶ fetch()
//!  ProxyNameService ──────────────── WasmFetchTransport ──▶ fetch()
//! ```
//!
//! Everything left of the bridge is plain Rust and `Send + Sync`; it is
//! unit-tested natively against a mock driver (see `cas::tests`). The
//! driver (`driver` module, wasm32 only) is the only code that touches
//! JavaScript.
//!
//! # For the JS shell
//!
//! The shell calls [`connect`] (wasm32) with the remote's API base, a
//! bearer token, and a [`BrowserIoConfig`], and gets a [`BrowserPeer`]:
//! [`BrowserPeer::fluree`] is the engine, [`BrowserPeer::cas`] exposes
//! [`BrowserCasStorage::prefetch`] / [`BrowserCasStorage::ensure_resident`]
//! (make bytes resident without copying them out again),
//! [`BrowserCasStorage::pin_set`] (query-duration pins for a
//! fetch-and-re-run loop), and [`BrowserCasStorage::stats`].
//! [`BrowserPeer::shutdown`] stops the driver. On any target,
//! [`build_peer`] assembles the same peer over a caller-supplied
//! [`IoHandle`] whose receiver is driven by something else — the native
//! test mock, or a future non-browser host.
//!
//! # Copies on the block path
//!
//! One copy out of JavaScript memory into the transport's `Bytes`, one
//! from `Bytes` into the `Arc<[u8]>` the residency hook requires, and
//! nothing further for synchronous hits. See `cas` for the reasoning and
//! the residency-first entry points that avoid a third copy.

#![forbid(unsafe_code)]

pub mod bridge;
pub mod budget;
pub mod cas;
pub mod coalesce;
pub mod config;
pub mod connect;
#[cfg(target_arch = "wasm32")]
pub mod driver;
pub mod protocol;
pub mod residency;

pub use bridge::{IoClosed, IoHandle, IoReceiver, WasmFetchTransport};
pub use cas::{BrowserCasStorage, CasStats};
pub use config::{BrowserIoConfig, CacheConfig};
#[cfg(target_arch = "wasm32")]
pub use connect::connect;
pub use connect::{build_peer, BrowserPeer};
#[cfg(target_arch = "wasm32")]
pub use driver::start_driver;
pub use protocol::IoJob;
pub use residency::{PinSet, ResidencyError, ResidencyStats, ResidencyTier};

// The transport contract this crate implements, re-exported so the shell
// does not need a direct dependency on the sync crate.
pub use fluree_db_nameservice_sync::{
    HttpTransport, TransportError, TransportMethod, TransportRequest, TransportResponse,
};
