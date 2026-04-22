//! Commit format v2: binary commit writer.
//!
//! The codec (format, varint, string_dict, op_codec), reader, and writer live
//! in `fluree_db_core::commit::codec`. This module re-exports the writer and
//! provides the streaming writer used by the commit pipeline.

mod streaming;
mod writer;

// Re-export core types from novelty for convenience
pub use fluree_db_core::commit::codec::CodecEnvelope;
pub use fluree_db_core::commit::codec::CommitCodecError;
pub use fluree_db_core::commit::codec::MAGIC;
pub use streaming::StreamingCommitWriter;
pub use writer::{write_commit, CommitWriteResult};
