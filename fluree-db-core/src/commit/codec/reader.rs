//! Commit v4 reader: binary blob -> Commit / CommitEnvelope.
//!
//! No NamespaceRegistry needed — Sids are reconstructed directly from
//! (namespace_code, name) pairs stored in the binary format.
//!
//! The reader produces CID-based types (`ContentId`, `CommitRef`).
//!
//! # Dispatch
//!
//! These functions are `pub(crate)` and are only called from the dispatch
//! layer in [`super`]. External callers go through the public `read_commit`,
//! `read_commit_envelope`, and `verify_commit_blob` functions, which route
//! to either this module (v4) or [`super::legacy_v3`] (v3) based on the
//! commit blob's version byte.

use super::error::CommitCodecError;
use super::format::{
    decode_sig_block, CommitFooter, CommitHeader, FLAG_HAS_COMMIT_SIG, FLAG_ZSTD, FOOTER_LEN,
    HEADER_LEN, MIN_COMMIT_LEN,
};
use super::op_codec::{decode_op, ReadDicts};
use super::string_dict::StringDict;
use crate::{Commit, CommitEnvelope, ContentId, ContentKind};

/// Verify a v4 commit blob and return its `ContentId`.
///
/// The CID is `SHA-256(full blob)`, computed via
/// `ContentId::new(ContentKind::Commit, bytes)`. Integrity is guaranteed by
/// the content-addressed store.
pub(crate) fn verify_commit_blob_v4(bytes: &[u8]) -> Result<ContentId, CommitCodecError> {
    let blob_len = bytes.len();

    if blob_len < MIN_COMMIT_LEN {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN,
        });
    }

    // Validate header (checks magic + version).
    CommitHeader::read_from(bytes)?;

    Ok(ContentId::new(ContentKind::Commit, bytes))
}

/// Read a v4 commit blob and return a full `Commit` (with flakes).
///
/// CID = `SHA-256(full blob)` via `ContentId::new(ContentKind::Commit, bytes)`.
pub(crate) fn read_commit_v4(bytes: &[u8]) -> Result<Commit, CommitCodecError> {
    let blob_len = bytes.len();

    // 1. Validate minimum size
    if blob_len < MIN_COMMIT_LEN {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN,
        });
    }

    // 2. Parse header
    let header = CommitHeader::read_from(bytes)?;

    // 3. Determine body_end and commit_id
    let sig_block_len = header.sig_block_len as usize;
    let has_sig_block = header.flags & FLAG_HAS_COMMIT_SIG != 0 && sig_block_len > 0;

    // V4: no embedded hash. Body ends before signature block (if any).
    let body_end = if has_sig_block {
        if blob_len < sig_block_len {
            return Err(CommitCodecError::TooSmall {
                got: blob_len,
                min: sig_block_len,
            });
        }
        blob_len - sig_block_len
    } else {
        blob_len
    };

    let commit_id = ContentId::new(ContentKind::Commit, bytes);

    // Parse signature block (if present) — located directly after footer
    let commit_signatures = if has_sig_block {
        let sig_block_data = &bytes[body_end..body_end + sig_block_len];
        decode_sig_block(sig_block_data)?
    } else {
        Vec::new()
    };

    // 4. Decode binary envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > blob_len {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: envelope_end,
        });
    }
    let envelope = super::envelope::decode_envelope(&bytes[envelope_start..envelope_end])?;

    // 5. Parse footer
    let footer_start = body_end - FOOTER_LEN;
    let footer = CommitFooter::read_from(&bytes[footer_start..body_end])?;

    // 6. Validate ops section bounds
    let ops_start = envelope_end;
    let ops_end = ops_start + footer.ops_section_len as usize;
    if ops_end > footer_start {
        return Err(CommitCodecError::InvalidOp(
            "ops section extends into footer".into(),
        ));
    }

    // 7. Load dictionaries
    let dicts = load_dicts(bytes, &footer, ops_end, footer_start)?;
    let ops_bytes = &bytes[ops_start..ops_end];
    let ops_decompressed;
    let ops_data = if header.flags & FLAG_ZSTD != 0 {
        let _span = tracing::debug_span!("v2_read_decompress", compressed_bytes = ops_bytes.len())
            .entered();
        ops_decompressed =
            zstd::decode_all(ops_bytes).map_err(CommitCodecError::DecompressionFailed)?;
        tracing::debug!(
            compressed = ops_bytes.len(),
            decompressed = ops_decompressed.len(),
            "ops decompressed"
        );
        &ops_decompressed[..]
    } else {
        ops_bytes
    };

    // 8. Decode ops into flakes
    let flakes = {
        let _span = tracing::debug_span!(
            "v2_decode_ops",
            op_count = header.op_count,
            ops_bytes = ops_data.len()
        )
        .entered();
        let mut flakes = Vec::with_capacity(header.op_count as usize);
        let mut pos = 0;
        for _ in 0..header.op_count {
            let flake = decode_op(ops_data, &mut pos, &dicts, header.t)?;
            flakes.push(flake);
        }
        flakes
    };

    tracing::debug!(
        blob_len,
        op_count = header.op_count,
        t = header.t,
        compressed = (header.flags & FLAG_ZSTD != 0),
        "v2 commit read"
    );

    // 9. Assemble Commit with CID-based types
    Ok(Commit {
        id: Some(commit_id),
        t: header.t,
        time: envelope.time,
        flakes,
        previous_refs: envelope.previous_refs,
        txn: envelope.txn,
        namespace_delta: envelope.namespace_delta,
        txn_signature: envelope.txn_signature,
        commit_signatures,
        txn_meta: envelope.txn_meta,
        graph_delta: envelope.graph_delta,
        ns_split_mode: envelope.ns_split_mode,
    })
}

/// Read only the envelope from a v4 commit blob (no flakes, no hash check).
///
/// This is fast because it only reads the header + binary envelope section,
/// skipping the ops, dictionaries, and footer entirely.
pub(crate) fn read_commit_envelope_v4(bytes: &[u8]) -> Result<CommitEnvelope, CommitCodecError> {
    // 1. Validate minimum size for header
    if bytes.len() < HEADER_LEN {
        return Err(CommitCodecError::TooSmall {
            got: bytes.len(),
            min: HEADER_LEN,
        });
    }

    // 2. Parse header
    let header = CommitHeader::read_from(bytes)?;

    // 3. Decode binary envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > bytes.len() {
        return Err(CommitCodecError::TooSmall {
            got: bytes.len(),
            min: envelope_end,
        });
    }
    let env = super::envelope::decode_envelope(&bytes[envelope_start..envelope_end])?;

    Ok(CommitEnvelope {
        t: header.t,
        previous_refs: env.previous_refs,
        txn: env.txn,
        namespace_delta: env.namespace_delta,
        txn_meta: env.txn_meta,
        ns_split_mode: env.ns_split_mode,
    })
}

/// Load and deserialize the 5 string dictionaries from the blob.
///
/// Validates that each dictionary is within `[valid_start..valid_end)` (the region
/// between the ops section and footer) and that dictionaries don't overlap.
pub(crate) fn load_dicts(
    bytes: &[u8],
    footer: &CommitFooter,
    valid_start: usize,
    valid_end: usize,
) -> Result<ReadDicts, CommitCodecError> {
    let dict_names = ["graph", "subject", "predicate", "datatype", "object_ref"];
    let mut prev_end = valid_start;

    let load_one = |loc: &super::format::DictLocation,
                    name: &str,
                    prev_end: &mut usize|
     -> Result<StringDict, CommitCodecError> {
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if start < *prev_end {
            return Err(CommitCodecError::InvalidDictionary(format!(
                "{} dict at offset {} overlaps previous region ending at {}",
                name, start, *prev_end
            )));
        }
        if end > valid_end {
            return Err(CommitCodecError::InvalidDictionary(format!(
                "{} dict at offset {} len {} extends past dict region end {}",
                name, start, loc.len, valid_end
            )));
        }
        *prev_end = end;
        StringDict::deserialize(&bytes[start..end])
    };

    Ok(ReadDicts {
        graph: load_one(&footer.dicts[0], dict_names[0], &mut prev_end)?,
        subject: load_one(&footer.dicts[1], dict_names[1], &mut prev_end)?,
        predicate: load_one(&footer.dicts[2], dict_names[2], &mut prev_end)?,
        datatype: load_one(&footer.dicts[3], dict_names[3], &mut prev_end)?,
        object_ref: load_one(&footer.dicts[4], dict_names[4], &mut prev_end)?,
    })
}
