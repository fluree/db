//! Server-side implementation of the Bolt protocol (Neo4j's versioned
//! binary protocol): PackStream serialization, chunked message framing,
//! version negotiation, and the session message state machine.
//!
//! This crate is **pure**: no IO, no async, no Fluree dependencies. Bytes go
//! in, bytes/actions come out, so everything unit-tests against captured
//! byte fixtures. The server crate owns the TCP listener, feeds inbound
//! bytes to [`chunk::ChunkAssembler`] + [`message::Request::decode`], drives
//! [`session::Session`], and executes the [`session::Turn::Run`] actions it
//! emits against the Fluree query/transact API.
//!
//! Scope (v1, see `docs/api/bolt.md`): Bolt 4.4 and 5.x,
//! autocommit only. `BEGIN` answers a clear FAILURE; explicit transactions
//! are deliberately unsupported.

pub mod chunk;
pub mod handshake;
pub mod message;
pub mod packstream;
pub mod session;
pub mod value;

pub use handshake::BoltVersion;
pub use value::Value;

/// Errors from decoding inbound bytes (malformed PackStream, oversized
/// values, unknown message tags). These are protocol violations: the
/// connection should fail the request and usually close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bolt decode error: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

impl DecodeError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}
