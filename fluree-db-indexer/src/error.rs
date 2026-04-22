//! Error types for the indexer

use thiserror::Error;

/// Indexer errors
#[derive(Error, Debug)]
pub enum IndexerError {
    /// Error from fluree-db-core
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Error from fluree-db-novelty
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Nameservice error
    #[error("Nameservice error: {0}")]
    NameService(String),

    /// Ledger not found in nameservice
    #[error("Ledger not found: {0}")]
    LedgerNotFound(String),

    /// No commits to index
    #[error("No commits found - cannot build index")]
    NoCommits,

    /// No existing index found (for refresh-only operations)
    #[error("No existing index found - use build_index_for_ledger for full rebuild")]
    NoIndex,

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Storage write error
    #[error("Storage write error: {0}")]
    StorageWrite(String),

    /// Storage read error
    #[error("Storage read error: {0}")]
    StorageRead(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Background indexer has shut down
    #[error("Background indexer has shut down")]
    IndexerShutdown,

    /// Error applying index to ledger state
    #[error("Ledger apply error: {0}")]
    LedgerApply(String),

    /// Incremental indexing aborted; caller should fall back to full rebuild.
    #[error("Incremental index aborted: {0}")]
    IncrementalAbort(String),

    /// General-purpose error for spatial index building and other auxiliary pipelines.
    #[error("{0}")]
    Other(String),
}

impl From<serde_json::Error> for IndexerError {
    fn from(e: serde_json::Error) -> Self {
        IndexerError::Serialization(e.to_string())
    }
}

/// Result type for indexer operations
pub type Result<T> = std::result::Result<T, IndexerError>;
