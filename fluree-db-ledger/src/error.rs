//! Error types for the ledger crate

use thiserror::Error;

/// Result type for ledger operations
pub type Result<T> = std::result::Result<T, LedgerError>;

/// Errors that can occur in ledger operations
#[derive(Error, Debug)]
pub enum LedgerError {
    /// Ledger not found in nameservice
    #[error("Ledger not found: {0}")]
    NotFound(String),

    /// Core error wrapper
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Novelty error wrapper
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Nameservice error wrapper
    #[error("Nameservice error: {0}")]
    Nameservice(#[from] fluree_db_nameservice::NameServiceError),

    /// Backpressure - novelty at max capacity
    #[error("Novelty at max capacity, waiting for indexer")]
    MaxNovelty,

    /// Ledger ID mismatch when applying index
    #[error("Index ledger_id '{new}' does not match expected '{expected}'")]
    LedgerIdMismatch { new: String, expected: String },

    /// Stale index (older than current)
    #[error("Index at t={index_t} is older than current index at t={current_t}")]
    StaleIndex { index_t: i64, current_t: i64 },

    /// Missing index CID in nameservice record
    #[error("Nameservice has index_t={index_t} for '{ledger_id}' but no index_head_id")]
    MissingIndexId { ledger_id: String, index_t: i64 },

    /// No index exists at or before the requested time
    #[error("No index available at or before t={target_t} for '{ledger_id}' (earliest index at t={earliest_t})")]
    NoIndexAtTime {
        ledger_id: String,
        target_t: i64,
        earliest_t: i64,
    },

    /// Invalid data encountered during incremental update
    #[error("{0}")]
    InvalidData(String),

    /// Target time is in the future (beyond current head)
    #[error("Target t={target_t} is beyond current head t={head_t} for '{ledger_id}'")]
    FutureTime {
        ledger_id: String,
        target_t: i64,
        head_t: i64,
    },
}

impl LedgerError {
    /// Create a not found error
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    /// Create a ledger ID mismatch error
    pub fn ledger_id_mismatch(new: impl Into<String>, expected: impl Into<String>) -> Self {
        Self::LedgerIdMismatch {
            new: new.into(),
            expected: expected.into(),
        }
    }

    /// Create a stale index error
    pub fn stale_index(index_t: i64, current_t: i64) -> Self {
        Self::StaleIndex { index_t, current_t }
    }

    /// Create a missing index CID error
    pub fn missing_index_id(ledger_id: impl Into<String>, index_t: i64) -> Self {
        Self::MissingIndexId {
            ledger_id: ledger_id.into(),
            index_t,
        }
    }

    /// Create a no index at time error
    pub fn no_index_at_time(ledger_id: impl Into<String>, target_t: i64, earliest_t: i64) -> Self {
        Self::NoIndexAtTime {
            ledger_id: ledger_id.into(),
            target_t,
            earliest_t,
        }
    }

    /// Create a future time error
    pub fn future_time(ledger_id: impl Into<String>, target_t: i64, head_t: i64) -> Self {
        Self::FutureTime {
            ledger_id: ledger_id.into(),
            target_t,
            head_t,
        }
    }
}
