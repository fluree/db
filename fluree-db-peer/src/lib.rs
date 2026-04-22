//! Fluree DB Query Peer
//!
//! A query peer that connects to a Fluree transaction server's `/fluree/events`
//! SSE endpoint and maintains an in-memory view of ledger and graph source metadata.
//!
//! # Overview
//!
//! The peer:
//! - Connects to the transaction server's SSE endpoint with Bearer token authentication
//! - Receives an initial snapshot of all subscribed ledgers and graph sources
//! - Receives live updates as ledgers and graph sources change
//! - Maintains state for query serving (future PR3)
//! - Automatically reconnects with exponential backoff on disconnection
//!
//! # Example
//!
//! ```no_run
//! use fluree_db_peer::{PeerConfig, PeerRuntime, LoggingCallbacks};
//! use clap::Parser;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = PeerConfig::parse();
//!     let runtime = PeerRuntime::new(config, LoggingCallbacks);
//!     runtime.run().await?;
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod runtime;
pub mod sse;
pub mod state;

// Re-export main types
pub use config::{PeerConfig, ReadMode};
pub use error::{PeerError, SseError};
pub use runtime::{LoggingCallbacks, PeerCallbacks, PeerRuntime};
pub use sse::{GraphSourceRecord, LedgerRecord, SseClient, SseClientEvent};
pub use state::{GraphSourceState, LedgerState, PeerState};
