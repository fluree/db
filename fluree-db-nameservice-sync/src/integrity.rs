//! CID integrity verification for fetched CAS objects.
//!
//! Shared by every consumer that pulls canonical bytes over the wire — the
//! proxy storage client, the origin fetchers, and pack ingestion — so the
//! "verify against the CID before trusting" rule has exactly one
//! implementation. Runtime-agnostic (builds for `wasm32`).

use fluree_db_core::{ContentId, CODEC_FLUREE_COMMIT};

/// Verify fetched object bytes against a CID, with format-sniffing for commits.
///
/// - Commit blobs (`FCV2` magic): the commit format's own hashing rules, via
///   `commit_blob_matches`
/// - All other kinds (txn, config, dict, index, etc.): the full bytes, under
///   the CID's own hash algorithm
///
/// **Forward-compat note:** If a future commit format uses `CODEC_FLUREE_COMMIT`
/// but has different hashing rules, add its magic-byte check here — the
/// `id.verify(bytes)` fallback hashes the full bytes.
pub fn verify_object_integrity(id: &ContentId, bytes: &[u8]) -> bool {
    const COMMIT_V2_MAGIC: &[u8] = b"FCV2";

    if id.codec() == CODEC_FLUREE_COMMIT && bytes.starts_with(COMMIT_V2_MAGIC) {
        fluree_db_core::commit::codec::commit_blob_matches(id, bytes)
    } else {
        id.verify(bytes)
    }
}
