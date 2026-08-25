//! Target-compat shims for the wasm32 build (spike).
//!
//! On native these re-export the real indexer types; on wasm32 they provide
//! compile-compatible stand-ins. wasm builds run with
//! [`crate::tx::IndexingMode::Disabled`] — the existing "Lambda / external
//! indexer" mode — so no indexer machinery ever executes there.

#[cfg(not(target_arch = "wasm32"))]
pub use fluree_db_indexer::{
    ConfiguredFulltextProperty, ConfiguredFulltextScope, IndexerConfig, IndexerError,
    IndexerHandle, DEFAULT_MAX_OLD_INDEXES,
};

#[cfg(target_arch = "wasm32")]
mod stubs {
    /// Mirrors `fluree_db_indexer::gc::DEFAULT_MAX_OLD_INDEXES`.
    pub const DEFAULT_MAX_OLD_INDEXES: u32 = 5;

    /// Config carrier only on wasm: fields the api layer reads/writes.
    #[derive(Debug, Clone, Default)]
    pub struct IndexerConfig {
        pub gc_max_old_indexes: u32,
        pub gc_min_time_mins: u32,
    }

    /// Never constructed on wasm (`IndexingMode::Background` is unreachable),
    /// so these methods exist only to satisfy call sites behind that variant.
    #[derive(Debug, Clone)]
    pub struct IndexerHandle;

    impl IndexerHandle {
        pub async fn trigger(&self, _ledger_id: impl Into<String>, _min_t: i64) {
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
