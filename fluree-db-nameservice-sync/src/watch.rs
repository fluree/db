//! Remote watch trait and event types
//!
//! A `RemoteWatch` provides a stream of changes from a remote, either via
//! SSE (real-time) or polling (fallback).

use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
use futures::Stream;
use std::fmt::Debug;
use std::pin::Pin;

/// An event from a remote watch
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RemoteEvent {
    /// A ledger record was created or updated on the remote
    LedgerUpdated(NsRecord),
    /// A ledger was retracted on the remote
    LedgerRetracted { ledger_id: String },
    /// A graph source record was created or updated on the remote
    GraphSourceUpdated(GraphSourceRecord),
    /// A graph source was retracted on the remote
    GraphSourceRetracted { graph_source_id: String },
    /// Connected to the remote
    Connected,
    /// Disconnected from the remote (will attempt reconnect)
    Disconnected { reason: String },
    /// Fatal error — stop watching, do not retry (e.g., 401/403)
    Fatal { reason: String },
}

/// A watch over a remote nameservice that yields events as they occur
pub trait RemoteWatch: Debug + Send + Sync {
    /// Start watching and return a stream of events.
    ///
    /// The stream should:
    /// - Emit `Connected` on successful connection
    /// - Emit record events as they arrive
    /// - Emit `Disconnected` on connection loss
    /// - Automatically reconnect with backoff
    /// - Continue yielding events after reconnect
    fn watch(&self) -> Pin<Box<dyn Stream<Item = RemoteEvent> + Send>>;
}
