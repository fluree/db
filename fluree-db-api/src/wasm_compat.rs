//! Target-compat shims for the wasm32 build.
//!
//! On native this module is the canonical import path for the re-exported
//! indexer types — not a spike artifact; removing it breaks native imports.
//!
//! On native these re-export the real indexer types; on wasm32 they provide
//! compile-compatible stand-ins. wasm builds run with
//! [`crate::tx::IndexingMode::Disabled`] — the existing "Lambda / external
//! indexer" mode — so no indexer machinery ever executes there.

#[cfg(not(target_arch = "wasm32"))]
pub use fluree_db_indexer::{
    ConfiguredFulltextProperty, ConfiguredFulltextScope, IndexerConfig, IndexerError,
    IndexerHandle, DEFAULT_CATCHUP_INTERVAL_SECS, DEFAULT_MAX_OLD_INDEXES,
};

/// Spawn a fire-and-forget background task.
///
/// Native: `tokio::spawn` on the ambient runtime, exactly as call sites did
/// before this seam existed (the discarded `JoinHandle` included).
#[cfg(not(target_arch = "wasm32"))]
#[inline]
pub fn spawn_detached<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(fut);
}

/// wasm32: there is no tokio runtime to attach to (`tokio::spawn` panics
/// "must be called from within a runtime"); the browser event loop is the
/// executor, reached through `wasm_bindgen_futures::spawn_local`.
#[cfg(target_arch = "wasm32")]
#[inline]
pub fn spawn_detached<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(fut);
}

#[cfg(target_arch = "wasm32")]
mod stubs {
    /// Mirrors `fluree_db_indexer::gc::DEFAULT_MAX_OLD_INDEXES`.
    pub const DEFAULT_MAX_OLD_INDEXES: u32 = 5;

    /// Mirrors `fluree_db_indexer::DEFAULT_CATCHUP_INTERVAL_SECS`.
    ///
    /// Inert here — the catch-up sweeps belong to `BackgroundIndexerWorker`,
    /// which wasm never runs. It exists because `server_defaults` is compiled
    /// for wasm and sources this constant for the server's clap default.
    pub const DEFAULT_CATCHUP_INTERVAL_SECS: u64 = 300;

    /// Config carrier only on wasm: fields the api layer reads/writes.
    ///
    /// The catch-up fields are carried but never consulted — nothing on wasm
    /// runs a worker to read them. They exist so `FlureeBuilder`'s catch-up
    /// setters compile for both targets without a `cfg` at each call site.
    #[derive(Debug, Clone, Default)]
    pub struct IndexerConfig {
        pub gc_max_old_indexes: u32,
        pub gc_min_time_mins: u32,
        pub catchup_interval: std::time::Duration,
        pub catchup_sweeps_enabled: bool,
    }

    impl IndexerConfig {
        pub fn with_catchup_interval(mut self, interval: std::time::Duration) -> Self {
            self.catchup_interval = interval;
            self
        }

        pub fn with_catchup_sweeps(mut self, enabled: bool) -> Self {
            self.catchup_sweeps_enabled = enabled;
            self
        }
    }

    /// Never constructed on wasm (`IndexingMode::Background` is unreachable),
    /// so these methods exist only to satisfy call sites behind that variant.
    #[derive(Debug, Clone)]
    pub struct IndexerHandle;

    impl IndexerHandle {
        pub async fn trigger(&self, _ledger_id: impl Into<String>, _min_t: i64) {
            unreachable!("IndexingMode::Background is never constructed on wasm32")
        }
        pub async fn trigger_if_idle(&self, _ledger_id: &str, _min_t: i64) -> bool {
            unreachable!("IndexingMode::Background is never constructed on wasm32")
        }
        pub async fn cancel_all(&self) {
            unreachable!("IndexingMode::Background is never constructed on wasm32")
        }
        pub async fn wait_all_idle(&self) {
            unreachable!("IndexingMode::Background is never constructed on wasm32")
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum IndexerError {
        #[error("ledger not found: {0}")]
        LedgerNotFound(String),
        #[error("no commits")]
        NoCommits,
        #[error("{0}")]
        Other(String),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ConfiguredFulltextScope {
        AnyGraph,
        DefaultGraph,
        TxnMetaGraph,
        NamedGraph(String),
    }

    #[derive(Debug, Clone)]
    pub struct ConfiguredFulltextProperty {
        pub scope: ConfiguredFulltextScope,
        pub property_iri: String,
    }
}

#[cfg(target_arch = "wasm32")]
pub use stubs::*;

/// The wasm32 stubs restate constants the indexer owns, because
/// `fluree-db-indexer` is a `cfg(not(target_arch = "wasm32"))` dependency and
/// cannot be named from a wasm build. Nothing cross-compiles the two, so
/// nothing catches them drifting apart.
///
/// These assertions run on native and pin the real constants to the literals
/// the stubs carry. Changing a default in the indexer fails here, which is the
/// only place that will tell you the wasm stub also needs updating.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod stub_drift {
    #[test]
    fn wasm_stub_constants_match_the_indexer() {
        assert_eq!(
            super::DEFAULT_CATCHUP_INTERVAL_SECS,
            300,
            "stubs::DEFAULT_CATCHUP_INTERVAL_SECS must be updated to match"
        );
        assert_eq!(
            super::DEFAULT_MAX_OLD_INDEXES,
            5,
            "stubs::DEFAULT_MAX_OLD_INDEXES must be updated to match"
        );
    }
}
