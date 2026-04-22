//! Binary commit codec.
//!
//! Encodes and decodes [`Commit`](super::Commit) values to/from a compact
//! binary format.
//!
//! # Versions
//!
//! - **V4** (current write format) — see [`reader`], [`raw_reader`], [`writer`]
//! - **V3** (legacy read-only) — see [`legacy_v3`]
//!
//! V3 blobs carry a trailing 32-byte SHA-256 hash over the commit body and
//! derive their CID from that embedded hash. V4 removes the embedded hash and
//! computes the CID as SHA-256 over the full blob via the content store.
//! The two formats are otherwise identical: same header, envelope, ops,
//! dictionaries, and footer layout.
//!
//! # Dispatch
//!
//! The public `read_commit`, `read_commit_envelope`, `load_commit_ops`, and
//! `verify_commit_blob` functions peek the version byte in the commit
//! header and route to the appropriate reader. Callers should never call the
//! version-specific functions directly.
//!
//! # V4 layout
//!
//! ```text
//! [Header 32B][Envelope (binary)][Ops section][Dictionaries][Footer 64B][optional signature block]
//! ```
//!
//! # V3 layout
//!
//! ```text
//! [Header 32B][Envelope (binary)][Ops section][Dictionaries][Footer 64B][Hash 32B][optional signature block]
//! ```
//!
//! See [`format`] for constants and layout details.

pub mod envelope;
mod error;
pub mod format;
pub mod legacy_v3;
pub mod op_codec;
pub mod raw_reader;
mod reader;
pub mod string_dict;
pub mod varint;
#[cfg(feature = "credential")]
mod writer;

pub use envelope::CodecEnvelope;
pub use error::CommitCodecError;
pub use format::{CommitSignature, ALGO_ED25519, MAGIC, VERSION, VERSION_V3};
pub use raw_reader::{CommitOps, RawObject, RawOp};
#[cfg(feature = "credential")]
pub use writer::{write_commit, CommitWriteResult};

use crate::{Commit, CommitEnvelope, ContentId};
use format::{HEADER_LEN, MAGIC as HEADER_MAGIC};

// ============================================================================
// Version dispatch
// ============================================================================

/// Detected commit blob version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitBlobVersion {
    V3,
    V4,
}

/// Inspect the blob header and determine which format version it is.
///
/// Validates minimum length and magic bytes before returning the version.
fn detect_version(bytes: &[u8]) -> Result<CommitBlobVersion, CommitCodecError> {
    if bytes.len() < HEADER_LEN {
        return Err(CommitCodecError::TooSmall {
            got: bytes.len(),
            min: HEADER_LEN,
        });
    }
    if bytes[0..4] != HEADER_MAGIC {
        return Err(CommitCodecError::InvalidMagic);
    }
    match bytes[4] {
        VERSION_V3 => Ok(CommitBlobVersion::V3),
        VERSION => Ok(CommitBlobVersion::V4),
        v => Err(CommitCodecError::UnsupportedVersion(v)),
    }
}

/// Read a commit blob and return a full `Commit` (with flakes).
///
/// Dispatches to the v3 or v4 reader based on the blob's header version
/// byte. V3 blobs are decoded with context-free datatype canonicalization
/// (see [`legacy_v3`] for the scope of fixups applied).
pub fn read_commit(bytes: &[u8]) -> Result<Commit, CommitCodecError> {
    match detect_version(bytes)? {
        CommitBlobVersion::V3 => legacy_v3::read_commit_v3(bytes),
        CommitBlobVersion::V4 => reader::read_commit_v4(bytes),
    }
}

/// Read only the envelope from a commit blob (no flakes, no hash check).
///
/// Dispatches to the v3 or v4 envelope reader based on the blob's header
/// version byte.
pub fn read_commit_envelope(bytes: &[u8]) -> Result<CommitEnvelope, CommitCodecError> {
    match detect_version(bytes)? {
        CommitBlobVersion::V3 => legacy_v3::read_commit_envelope_v3(bytes),
        CommitBlobVersion::V4 => reader::read_commit_envelope_v4(bytes),
    }
}

/// Load commit ops for raw replay.
///
/// Dispatches to the v3 or v4 raw reader based on the blob's header version
/// byte. V3 raw ops are pass-through; the indexer's resolver applies
/// chain-aware datatype canonicalization during replay.
pub fn load_commit_ops(bytes: &[u8]) -> Result<CommitOps, CommitCodecError> {
    match detect_version(bytes)? {
        CommitBlobVersion::V3 => legacy_v3::load_commit_ops_v3(bytes),
        CommitBlobVersion::V4 => raw_reader::load_commit_ops_v4(bytes),
    }
}

/// Verify a commit blob and return its `ContentId`.
///
/// Dispatches to the v3 or v4 verifier based on the blob's header version
/// byte. V3 verifies the embedded trailing hash; V4 derives the CID from
/// `SHA-256(full blob)` and relies on the content store for integrity.
pub fn verify_commit_blob(bytes: &[u8]) -> Result<ContentId, CommitCodecError> {
    match detect_version(bytes)? {
        CommitBlobVersion::V3 => legacy_v3::verify_commit_v3(bytes),
        CommitBlobVersion::V4 => reader::verify_commit_blob_v4(bytes),
    }
}
