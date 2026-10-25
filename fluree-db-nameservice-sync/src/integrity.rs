//! CID integrity verification for fetched CAS objects.
//!
//! Shared by every consumer that pulls canonical bytes over the wire — the
//! proxy storage client, the origin fetchers, and pack ingestion — so the
//! "verify against the CID before trusting" rule has exactly one
//! implementation. Runtime-agnostic (builds for `wasm32`).

use fluree_db_core::pack::{decode_frame, ErasureArtifactKind, PackFrame, DEFAULT_MAX_PAYLOAD};
use fluree_db_core::{ContentId, ContentKind, CODEC_FLUREE_COMMIT};

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

/// The replacement an object request's answer for `cid`, an erased commit,
/// carries (`X-Fluree-Content-Kind: erasure`: pack frames holding its chain
/// and latest replacement). Checked to be that commit's: the chain names it,
/// the replacement is the one its latest step names, and the replacement
/// stands for it. The proofs are the server's to check, as are the
/// policy-filtered leaves a proxy reads.
pub fn erased_replacement(cid: &ContentId, frames: &[u8]) -> Result<Vec<u8>, String> {
    let commit = cid.to_string();
    let (mut named, mut replacements) = (None, Vec::new());
    let mut pos = 0;
    while pos < frames.len() {
        let (frame, used) =
            decode_frame(&frames[pos..], DEFAULT_MAX_PAYLOAD).map_err(|e| e.to_string())?;
        pos += used;
        match frame {
            PackFrame::Erasure(chain) if chain["commit"] == commit.as_str() => {
                named = chain["steps"]
                    .as_array()
                    .and_then(|steps| steps.last())
                    .and_then(|step| step["replacement"].as_str())
                    .map(str::to_string);
            }
            PackFrame::ErasureArtifact {
                kind: ErasureArtifactKind::Replacement,
                payload,
            } => replacements.push(payload),
            _ => {}
        }
    }
    let named =
        named.ok_or_else(|| format!("the erasure answer for {cid} names no chain of it"))?;
    let replacement = replacements
        .into_iter()
        .find(|r| ContentId::new(ContentKind::Commit, r).to_string() == named)
        .ok_or_else(|| format!("the erasure answer for {cid} lacks its replacement"))?;
    match fluree_db_core::commit::codec::replacement::original_of(&replacement) {
        Ok(original) if original == *cid => Ok(replacement),
        _ => Err(format!(
            "the replacement answered for {cid} stands for another commit"
        )),
    }
}
