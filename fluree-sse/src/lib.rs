//! Lightweight SSE (Server-Sent Events) parser
//!
//! Provides a streaming parser for the SSE protocol format. The parser
//! accumulates bytes and yields complete [`SseEvent`]s.
//!
//! This crate has minimal dependencies (only `tracing` for warnings on
//! invalid UTF-8) and is designed to be shared across Fluree crates that
//! need SSE parsing without pulling in heavy dependencies.

mod parser;

pub use parser::{SseEvent, SseParser};

/// SSE event kind for ledger events (published, retracted, updated).
pub const SSE_KIND_LEDGER: &str = "ledger";

/// SSE event kind for graph source events (published, retracted, updated).
pub const SSE_KIND_GRAPH_SOURCE: &str = "graph-source";
