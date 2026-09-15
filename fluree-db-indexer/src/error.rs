//! Error types for the indexer

use fluree_db_core::task::TaskFailure;
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

    /// A build task was cancelled: the runtime is shutting down underneath
    /// the indexer. Not a failed build — nothing for this process to retry.
    #[error("{0} was cancelled (runtime shutdown or abort)")]
    Cancelled(String),

    /// Error applying index to ledger state
    #[error("Ledger apply error: {0}")]
    LedgerApply(String),

    /// Incremental indexing aborted; caller should fall back to full rebuild.
    #[error("Incremental index aborted: {0}")]
    IncrementalAbort(String),

    /// General-purpose error for spatial index building and other auxiliary pipelines.
    #[error("{0}")]
    Other(String),

    /// Fuel limit exceeded during an indexer CAS write. Indexing trackers are
    /// expected to be no-limit (measurement only), so in normal use this is
    /// unreachable — it exists for defensive type safety if a caller supplies
    /// a limited tracker.
    #[error("Indexer fuel limit exceeded: {0}")]
    FuelExceeded(#[from] fluree_db_core::tracking::FuelExceededError),
}

impl IndexerError {
    /// Classify a joined build task's `JoinError`. Cancellation gets its own
    /// variant so the orchestrator can decline to retry; a panic keeps its
    /// payload text and stays a `StorageWrite` failure as before.
    pub fn from_join(task: &str, e: tokio::task::JoinError) -> Self {
        let failure = TaskFailure::from(e);
        if failure.is_cancelled() {
            Self::Cancelled(task.to_string())
        } else {
            Self::StorageWrite(failure.describe(task))
        }
    }
}

impl From<serde_json::Error> for IndexerError {
    fn from(e: serde_json::Error) -> Self {
        IndexerError::Serialization(e.to_string())
    }
}

/// Result type for indexer operations
pub type Result<T> = std::result::Result<T, IndexerError>;
