//! Commit writer: Commit -> binary blob.
//!
//! Encodes a [`Commit`] into the binary format using Sid-direct encoding
//! (namespace_code + name dict entries). No NamespaceRegistry is needed —
//! Sid fields are read directly from flakes.

use super::envelope;
use super::format::{
    encode_sig_block, sig_block_size, CommitFooter, CommitHeader, CommitSignature, DictLocation,
    FLAG_HAS_COMMIT_SIG, FLAG_ZSTD, FOOTER_LEN, HEADER_LEN, VERSION,
};
use super::op_codec::{encode_op, CommitDicts};
use super::CommitCodecError;
use crate::Commit;
use fluree_db_credential::{did_from_pubkey, sign_commit_digest, SigningKey};
use sha2::{Digest, Sha256};

/// Result of writing a v4 commit blob.
///
/// In v4 there is no embedded hash. The CID is derived from the full blob
/// bytes via `ContentId::new(ContentKind::Commit, &result.bytes)`.
pub struct CommitWriteResult {
    /// The complete binary blob (body + optional signature block).
    pub bytes: Vec<u8>,
}

/// Write a commit as a v4 binary blob.
///
/// V4 layout: `[header][envelope][ops][dicts][footer][optional signature block]`
/// No embedded hash — the CID is derived from the full blob by the caller
/// via `ContentId::new(ContentKind::Commit, &result.bytes)`.
///
/// Encodes flakes using Sid-direct encoding (namespace_code + name dict entries).
/// No NamespaceRegistry is needed — Sid fields are read directly from flakes.
///
/// When `signing` is `Some((key, ledger_id))`, the body hash
/// (`SHA-256([header..footer])`) is computed for signing only (not embedded).
/// The signature block is appended directly after the footer.
pub fn write_commit(
    commit: &Commit,
    compress: bool,
    signing: Option<(&SigningKey, &str)>,
) -> Result<CommitWriteResult, CommitCodecError> {
    // Validate t fits in u32 for v3 wire format
    if commit.t < 0 || commit.t > u32::MAX as i64 {
        return Err(CommitCodecError::TOutOfRange(commit.t));
    }

    let op_count = commit.flakes.len();

    // 1. Pre-compute signing metadata (needed for header before hash)
    let (signer_did, pre_sig_block_len) = if let Some((key, _)) = &signing {
        let did = did_from_pubkey(&key.verifying_key().to_bytes());
        // Build a temporary CommitSignature to compute encoded size
        let tmp_sig = CommitSignature {
            signer: did.clone(),
            algo: super::format::ALGO_ED25519,
            signature: [0u8; 64],
            metadata: None,
        };
        let len = sig_block_size(&[tmp_sig]);
        (Some(did), len as u16)
    } else {
        (None, 0u16)
    };

    // 2. Serialize envelope (binary)
    let envelope_bytes = {
        let _span = tracing::debug_span!("v2_write_envelope").entered();
        let mut buf = Vec::new();
        envelope::encode_envelope(commit, &mut buf)?;
        buf
    };

    // 3. Encode all ops into a buffer, populating dictionaries
    let (ops_raw, dicts) = {
        let _span = tracing::debug_span!("v2_encode_ops", op_count).entered();
        let mut dicts = CommitDicts::new();
        let mut ops_raw = Vec::new();
        for flake in &commit.flakes {
            encode_op(flake, &mut dicts, &mut ops_raw)?;
        }
        (ops_raw, dicts)
    };

    // 4. Optionally compress ops
    let (ops_section, is_compressed) = if compress && !ops_raw.is_empty() {
        let _span = tracing::debug_span!("v2_compress_ops", raw_bytes = ops_raw.len()).entered();
        let compressed =
            zstd::encode_all(ops_raw.as_slice(), 3).map_err(CommitCodecError::CompressionFailed)?;
        // Only use compressed if it's actually smaller
        if compressed.len() < ops_raw.len() {
            tracing::debug!(
                raw = ops_raw.len(),
                compressed = compressed.len(),
                ratio = format_args!("{:.1}x", ops_raw.len() as f64 / compressed.len() as f64),
                "ops compressed"
            );
            (compressed, true)
        } else {
            (ops_raw, false)
        }
    } else {
        (ops_raw, false)
    };

    // 5. Serialize dictionaries
    let dict_bytes: Vec<Vec<u8>> = [
        &dicts.graph,
        &dicts.subject,
        &dicts.predicate,
        &dicts.datatype,
        &dicts.object_ref,
    ]
    .iter()
    .map(|d| d.serialize())
    .collect();

    // 6. Calculate total size and allocate output (v4: no embedded hash)
    let total_size = HEADER_LEN
        + envelope_bytes.len()
        + ops_section.len()
        + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
        + FOOTER_LEN
        + pre_sig_block_len as usize;
    let mut output = Vec::with_capacity(total_size);

    // 7. Write header (sig_block_len and FLAG_HAS_COMMIT_SIG set BEFORE hash)
    let mut flags = 0u8;
    if is_compressed {
        flags |= FLAG_ZSTD;
    }
    if signing.is_some() {
        flags |= FLAG_HAS_COMMIT_SIG;
    }
    let header = CommitHeader {
        version: VERSION,
        flags,
        t: commit.t,
        op_count: commit.flakes.len() as u32,
        envelope_len: envelope_bytes.len() as u32,
        sig_block_len: pre_sig_block_len,
    };
    let mut header_buf = [0u8; HEADER_LEN];
    header.write_to(&mut header_buf);
    output.extend_from_slice(&header_buf);

    // 8. Write envelope
    output.extend_from_slice(&envelope_bytes);

    // 9. Write ops section
    output.extend_from_slice(&ops_section);

    // 10. Write dictionaries, recording locations
    let mut dict_locations = [DictLocation::default(); 5];
    for (i, bytes) in dict_bytes.iter().enumerate() {
        dict_locations[i] = DictLocation {
            offset: output.len() as u64,
            len: bytes.len() as u32,
        };
        output.extend_from_slice(bytes);
    }

    // 11. Write footer
    let footer = CommitFooter {
        dicts: dict_locations,
        ops_section_len: ops_section.len() as u32,
    };
    let mut footer_buf = [0u8; FOOTER_LEN];
    footer.write_to(&mut footer_buf);
    output.extend_from_slice(&footer_buf);

    // 12. If signing, compute body hash for signature (NOT embedded), append sig block
    if let Some((signing_key, ledger_id)) = signing {
        let _span = tracing::debug_span!("v4_write_sign").entered();
        let body_hash: [u8; 32] = Sha256::digest(&output).into();
        let signature = sign_commit_digest(signing_key, &body_hash, ledger_id);
        let commit_sig = CommitSignature {
            signer: signer_did.expect("signer_did set when signing is Some"),
            algo: super::format::ALGO_ED25519,
            signature,
            metadata: None,
        };
        encode_sig_block(&[commit_sig], &mut output);
    }

    debug_assert_eq!(output.len(), total_size);

    tracing::debug!(
        blob_bytes = output.len(),
        op_count,
        envelope_bytes = envelope_bytes.len(),
        ops_bytes = ops_section.len(),
        compressed = is_compressed,
        signed = signing.is_some(),
        "commit written"
    );

    Ok(CommitWriteResult { bytes: output })
}
