//! SSE client and event types
//!
//! This module provides the SSE client for connecting to the `/fluree/events`
//! endpoint and receiving ledger/graph source update events.
//!
//! The low-level SSE parser (`SseParser`, `SseEvent`) lives in the `fluree-sse` crate.

pub mod client;
pub mod events;

pub use client::SseClient;
pub use events::{GraphSourceRecord, LedgerRecord, SseClientEvent};
