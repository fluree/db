//! Legacy v3 commit blob reader.
//!
//! V3 is a read-only format. All commit writes now produce v4 blobs. This
//! module exists so that databases written by older Fluree versions continue
//! to load and reindex without data rewriting.
//!
//! # Differences from v4
//!
//! V3 and v4 share the same header, envelope, footer, op stream, string
//! dictionaries, and op codec. They differ only in two places:
//!
//! 1. **Trailing 32-byte hash**. V3 blobs carry a SHA-256 hash over the body
//!    (everything from the start of the blob up to but excluding the hash
//!    itself). The blob's `ContentId` is derived from this embedded hash via
//!    `ContentId::from_sha256_digest(CODEC_FLUREE_COMMIT, &hash)`. V4 removed
//!    the embedded hash — the CID is `SHA-256(full blob)` computed by the
//!    content store.
//!
//! 2. **Signature block layout**. Each v3 signature includes an 8-byte
//!    `timestamp: i64` field between the signature bytes and the metadata
//!    length. V4 dropped this field. The v3 sig-block decoder here reads and
//!    discards the timestamp so that downstream code can use the shared
//!    `CommitSignature` struct.
//!
//! # Datatype canonicalization
//!
//! Some v3-era commits were written by a transaction parser that stored
//! JSON-LD CURIE datatypes verbatim (e.g. `"@type": "xsd:string"`) without
//! expanding them to canonical IRI form. This produces `Sid` values like
//! `Sid { namespace_code: EMPTY, name: "xsd:string" }` or, in the
//! "dynamic-prefix" shape, a fresh namespace code whose prefix string is the
//! compact CURIE (`"xsd:"`) rather than the canonical IRI.
//!
//! This module canonicalizes the **context-free subset** at decode time:
//! cases where the fix can be determined from the `Sid` alone without
//! consulting the chain-accumulated namespace map. The rules are deliberately
//! conservative:
//!
//! 1. **Only attempt repair on clearly broken shapes.** `namespace_code` must
//!    be `EMPTY` (and the name must split into a known `xsd:` / `rdf:` /
//!    `rdfs:` prefix + known local), OR `namespace_code` must be `JSON_LD`
//!    with an exact-match legacy alias like `"json"`.
//! 2. **Parse with `split_once(':')`**, never `strip_prefix` — a name like
//!    `"xsd:string:extra"` must not be rewritten to `(XSD, "string:extra")`.
//! 3. **Canonicalize from an allowlist**, not any local name. Only
//!    well-known XSD / RDF / RDFS locals are accepted. Unknown locals pass
//!    through unchanged (they may be legitimate custom datatypes).
//! 4. **Exact-match JSON-LD shorthands only**: `@json`, `@vector`,
//!    `@fulltext`.
//!
//! The dynamic-prefix case — where `namespace_code` is a valid but mis-mapped
//! code — is **not** handled here. That case requires the chain's namespace
//! delta history and is canonicalized by the resolver's
//! `canonicalize_datatype_curie` during raw-op replay.
//!
//! # Public API
//!
//! Mirrors the v4 reader's surface:
//!
//! - [`read_commit_v3`] — full commit decode
//! - [`read_commit_envelope_v3`] — envelope only
//! - [`load_commit_ops_v3`] — raw op reader for replay (pass-through; no
//!   canonicalization — resolver handles it during replay)
//! - [`verify_commit_v3`] — verify trailing hash and return CID
//!
//! None of these functions should be called directly by consumer code;
//! dispatch from the top-level [`super::read_commit`], etc., is the only
//! supported entry point.

use super::envelope::decode_envelope;
use super::error::CommitCodecError;
use super::format::{
    CommitFooter, CommitHeader, CommitSignature, ALGO_ED25519, FLAG_HAS_COMMIT_SIG, FLAG_ZSTD,
    FOOTER_LEN, HASH_LEN_V3, HEADER_LEN, MIN_COMMIT_LEN,
};
use super::op_codec::decode_op;
use super::raw_reader::CommitOps;
use super::reader::load_dicts;
use super::varint::{read_exact, read_u8};
use crate::sid::Sid;
use crate::{Commit, CommitEnvelope, ContentId, TxnMetaEntry, TxnMetaValue, CODEC_FLUREE_COMMIT};
use fluree_vocab::namespaces;
use sha2::{Digest, Sha256};
use std::sync::Once;

// ============================================================================
// One-time logging
// ============================================================================

static V3_SESSION_NOTICE: Once = Once::new();

fn log_v3_first_seen() {
    V3_SESSION_NOTICE.call_once(|| {
        tracing::info!(
            "reading legacy v3 commit format; on-the-fly canonicalization applied \
             for context-free cases (dynamic-prefix cases handled by the resolver)"
        );
    });
}

// ============================================================================
// Public entry points
// ============================================================================

/// Verify a v3 commit blob and return its `ContentId`.
///
/// V3 CID = `SHA-256(body)` where body excludes the trailing 32-byte embedded
/// hash and any optional signature block. Returns `HashMismatch` if the
/// embedded hash does not match the computed one.
pub fn verify_commit_v3(bytes: &[u8]) -> Result<ContentId, CommitCodecError> {
    log_v3_first_seen();

    let blob_len = bytes.len();
    if blob_len < MIN_COMMIT_LEN + HASH_LEN_V3 {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN + HASH_LEN_V3,
        });
    }

    let header = CommitHeader::read_from(bytes)?;
    let hash_offset = v3_hash_offset(blob_len, &header)?;

    let expected_hash: [u8; 32] = bytes[hash_offset..hash_offset + HASH_LEN_V3]
        .try_into()
        .unwrap();
    let actual_hash: [u8; 32] = Sha256::digest(&bytes[..hash_offset]).into();
    if expected_hash != actual_hash {
        return Err(CommitCodecError::HashMismatch {
            expected: expected_hash,
            actual: actual_hash,
        });
    }

    Ok(ContentId::from_sha256_digest(
        CODEC_FLUREE_COMMIT,
        &actual_hash,
    ))
}

/// Read a v3 commit blob and return a full `Commit` with flakes.
///
/// Verifies the embedded trailing hash, decodes envelope + ops, and applies
/// context-free datatype canonicalization to `commit.flakes[*].dt` and to
/// typed-literal entries in `commit.txn_meta`.
pub fn read_commit_v3(bytes: &[u8]) -> Result<Commit, CommitCodecError> {
    log_v3_first_seen();

    let blob_len = bytes.len();
    if blob_len < MIN_COMMIT_LEN + HASH_LEN_V3 {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN + HASH_LEN_V3,
        });
    }

    // 1. Header
    let header = CommitHeader::read_from(bytes)?;

    // 2. Determine hash offset and verify the embedded trailing hash.
    let hash_offset = v3_hash_offset(blob_len, &header)?;
    let (commit_id, commit_signatures) = {
        let _span = tracing::debug_span!("v3_read_verify_hash", blob_len).entered();
        let expected_hash: [u8; 32] = bytes[hash_offset..hash_offset + HASH_LEN_V3]
            .try_into()
            .unwrap();
        let actual_hash: [u8; 32] = Sha256::digest(&bytes[..hash_offset]).into();
        if expected_hash != actual_hash {
            return Err(CommitCodecError::HashMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }

        let cid = ContentId::from_sha256_digest(CODEC_FLUREE_COMMIT, &actual_hash);

        // V3 signature block lives *after* the trailing hash.
        let has_sig_block = header.flags & FLAG_HAS_COMMIT_SIG != 0 && header.sig_block_len > 0;
        let sigs = if has_sig_block {
            let sig_start = hash_offset + HASH_LEN_V3;
            let sig_end = sig_start + header.sig_block_len as usize;
            decode_sig_block_v3(&bytes[sig_start..sig_end])?
        } else {
            Vec::new()
        };

        (cid, sigs)
    };

    // 3. Decode binary envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > blob_len {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: envelope_end,
        });
    }
    let envelope = decode_envelope(&bytes[envelope_start..envelope_end])?;

    // 4. Parse footer (immediately before the trailing hash)
    let footer_start = hash_offset - FOOTER_LEN;
    let footer = CommitFooter::read_from(&bytes[footer_start..hash_offset])?;

    // 5. Ops section bounds
    let ops_start = envelope_end;
    let ops_end = ops_start + footer.ops_section_len as usize;
    if ops_end > footer_start {
        return Err(CommitCodecError::InvalidOp(
            "ops section extends into footer".into(),
        ));
    }

    // 6. Load dictionaries
    let dicts = load_dicts(bytes, &footer, ops_end, footer_start)?;
    let ops_bytes = &bytes[ops_start..ops_end];
    let ops_decompressed;
    let ops_data = if header.flags & FLAG_ZSTD != 0 {
        ops_decompressed =
            zstd::decode_all(ops_bytes).map_err(CommitCodecError::DecompressionFailed)?;
        &ops_decompressed[..]
    } else {
        ops_bytes
    };

    // 7. Decode ops into flakes (reuses shared op codec)
    let mut flakes = Vec::with_capacity(header.op_count as usize);
    let mut pos = 0;
    for _ in 0..header.op_count {
        let flake = decode_op(ops_data, &mut pos, &dicts, header.t)?;
        flakes.push(flake);
    }

    // 8. Apply context-free datatype canonicalization
    let flake_fixups = canonicalize_flake_datatypes(&mut flakes);
    let mut txn_meta = envelope.txn_meta;
    let txn_meta_fixups = canonicalize_txn_meta_datatypes(&mut txn_meta);

    tracing::debug!(
        blob_len,
        op_count = header.op_count,
        t = header.t,
        flake_dt_fixups = flake_fixups,
        txn_meta_dt_fixups = txn_meta_fixups,
        "v3 commit decoded"
    );

    Ok(Commit {
        id: Some(commit_id),
        t: header.t,
        time: envelope.time,
        flakes,
        parents: envelope.parents,
        txn: envelope.txn,
        namespace_delta: envelope.namespace_delta,
        txn_signature: envelope.txn_signature,
        commit_signatures,
        txn_meta,
        graph_delta: envelope.graph_delta,
        ns_split_mode: envelope.ns_split_mode,
    })
}

/// Read only the envelope from a v3 commit blob.
///
/// Applies context-free datatype canonicalization to typed-literal entries
/// in the envelope's `txn_meta`. No flake decode, no hash verification.
pub fn read_commit_envelope_v3(bytes: &[u8]) -> Result<CommitEnvelope, CommitCodecError> {
    log_v3_first_seen();

    if bytes.len() < HEADER_LEN {
        return Err(CommitCodecError::TooSmall {
            got: bytes.len(),
            min: HEADER_LEN,
        });
    }

    let header = CommitHeader::read_from(bytes)?;
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > bytes.len() {
        return Err(CommitCodecError::TooSmall {
            got: bytes.len(),
            min: envelope_end,
        });
    }
    let env = decode_envelope(&bytes[envelope_start..envelope_end])?;

    let mut txn_meta = env.txn_meta;
    let txn_meta_fixups = canonicalize_txn_meta_datatypes(&mut txn_meta);

    tracing::debug!(
        blob_len = bytes.len(),
        t = header.t,
        txn_meta_dt_fixups = txn_meta_fixups,
        "v3 commit envelope decoded"
    );

    Ok(CommitEnvelope {
        t: header.t,
        parents: env.parents,
        txn: env.txn,
        namespace_delta: env.namespace_delta,
        txn_meta,
        ns_split_mode: env.ns_split_mode,
    })
}

/// Load v3 commit ops for raw replay, with legacy-v3 datatype
/// canonicalization applied on each yielded [`crate::commit::codec::RawOp`].
///
/// The returned [`CommitOps`] has its internal
/// `legacy_v3_canonicalize` flag set, so
/// [`CommitOps::for_each_op`](CommitOps::for_each_op) rewrites any
/// context-free-corrupt `(dt_ns_code, dt_name)` pairs to their canonical
/// form before invoking the caller's closure. Consumers (the indexer's
/// resolver in particular) see canonical datatypes regardless of which
/// on-disk shape the v3 blob used.
///
/// V3 and v4 share the same envelope + ops layout; the only framing
/// difference this path cares about is the trailing-hash offset when
/// locating the footer.
pub fn load_commit_ops_v3(bytes: &[u8]) -> Result<CommitOps, CommitCodecError> {
    log_v3_first_seen();

    let blob_len = bytes.len();
    if blob_len < MIN_COMMIT_LEN + HASH_LEN_V3 {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: MIN_COMMIT_LEN + HASH_LEN_V3,
        });
    }

    let header = CommitHeader::read_from(bytes)?;
    let hash_offset = v3_hash_offset(blob_len, &header)?;

    // Envelope
    let envelope_start = HEADER_LEN;
    let envelope_end = envelope_start + header.envelope_len as usize;
    if envelope_end > blob_len {
        return Err(CommitCodecError::TooSmall {
            got: blob_len,
            min: envelope_end,
        });
    }
    let envelope = decode_envelope(&bytes[envelope_start..envelope_end])?;

    // Footer sits immediately before the trailing hash in v3.
    let footer_start = hash_offset - FOOTER_LEN;
    let footer = CommitFooter::read_from(&bytes[footer_start..hash_offset])?;

    // Ops section bounds
    let ops_start = envelope_end;
    let ops_end = ops_start + footer.ops_section_len as usize;
    if ops_end > footer_start {
        return Err(CommitCodecError::InvalidOp(
            "ops section extends into footer".into(),
        ));
    }

    // Dictionaries
    let dicts = load_dicts(bytes, &footer, ops_end, footer_start)?;

    // Decompress ops
    let ops_bytes = &bytes[ops_start..ops_end];
    let ops_data = if header.flags & FLAG_ZSTD != 0 {
        zstd::decode_all(ops_bytes).map_err(CommitCodecError::DecompressionFailed)?
    } else {
        ops_bytes.to_vec()
    };

    // Populate envelope.t from header (decode_envelope leaves t=0)
    let mut envelope = envelope;
    envelope.t = header.t;

    tracing::debug!(
        blob_len,
        op_count = header.op_count,
        t = header.t,
        "v3 commit ops loaded (canonicalization applied per-op at iteration)"
    );

    let ops = CommitOps::new(envelope, header.t, header.op_count, dicts, ops_data);
    Ok(ops.with_legacy_v3_canonicalization())
}

// ============================================================================
// V3 framing helpers
// ============================================================================

/// Compute the v3 trailing-hash offset.
///
/// V3 layout with optional signature:
/// `[header][envelope][ops][dicts][footer][hash: 32B][optional sig block]`
///
/// Returns the byte offset at which the 32-byte hash begins. The footer
/// ends at this offset; the body covered by the hash is `bytes[..hash_offset]`.
fn v3_hash_offset(blob_len: usize, header: &CommitHeader) -> Result<usize, CommitCodecError> {
    let sig_block_len = header.sig_block_len as usize;
    let has_sig_block = header.flags & FLAG_HAS_COMMIT_SIG != 0 && sig_block_len > 0;

    let hash_offset = if has_sig_block {
        if blob_len < HEADER_LEN + HASH_LEN_V3 + sig_block_len {
            return Err(CommitCodecError::TooSmall {
                got: blob_len,
                min: HEADER_LEN + HASH_LEN_V3 + sig_block_len,
            });
        }
        blob_len - sig_block_len - HASH_LEN_V3
    } else {
        blob_len - HASH_LEN_V3
    };

    Ok(hash_offset)
}

/// Decode a v3 signature block.
///
/// V3 per-signature layout:
/// `signer_len: u16 | signer: bytes | algo: u8 | signature: [u8; 64] |
///  timestamp: i64 (discarded) | meta_len: u16 | metadata: bytes (if meta_len > 0)`
///
/// The v4 format dropped the 8-byte timestamp. We read and discard it here
/// so that v3 signature blocks decode into the shared `CommitSignature`
/// struct.
fn decode_sig_block_v3(data: &[u8]) -> Result<Vec<CommitSignature>, CommitCodecError> {
    const MAX_SIG_COUNT_LOCAL: u16 = 16;
    const MAX_SIGNER_LEN_LOCAL: usize = 512;
    const MAX_METADATA_LEN_LOCAL: usize = 4096;

    let mut pos = 0;
    let sig_count = u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap());
    if sig_count > MAX_SIG_COUNT_LOCAL {
        return Err(CommitCodecError::EnvelopeDecode(format!(
            "signature count {sig_count} exceeds maximum {MAX_SIG_COUNT_LOCAL}"
        )));
    }

    let mut sigs = Vec::with_capacity(sig_count as usize);
    for _ in 0..sig_count {
        let signer_len =
            u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap()) as usize;
        if signer_len > MAX_SIGNER_LEN_LOCAL {
            return Err(CommitCodecError::EnvelopeDecode(format!(
                "signer length {signer_len} exceeds maximum {MAX_SIGNER_LEN_LOCAL}"
            )));
        }
        let signer_bytes = read_exact(data, &mut pos, signer_len)?;
        let signer = std::str::from_utf8(signer_bytes)
            .map_err(|e| CommitCodecError::EnvelopeDecode(format!("invalid signer UTF-8: {e}")))?;

        let algo = read_u8(data, &mut pos)?;
        if algo != ALGO_ED25519 {
            return Err(CommitCodecError::EnvelopeDecode(format!(
                "unknown signature algorithm: 0x{algo:02x}"
            )));
        }

        let sig_slice = read_exact(data, &mut pos, 64)?;
        let mut signature = [0u8; 64];
        signature.copy_from_slice(sig_slice);

        // V3 timestamp field (8 bytes, i64 LE) — read and discard.
        let _timestamp_bytes = read_exact(data, &mut pos, 8)?;

        let meta_len =
            u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap()) as usize;
        let metadata = if meta_len > 0 {
            if meta_len > MAX_METADATA_LEN_LOCAL {
                return Err(CommitCodecError::EnvelopeDecode(format!(
                    "metadata length {meta_len} exceeds maximum {MAX_METADATA_LEN_LOCAL}"
                )));
            }
            Some(read_exact(data, &mut pos, meta_len)?.to_vec())
        } else {
            None
        };

        sigs.push(CommitSignature {
            signer: signer.to_string(),
            algo,
            signature,
            metadata,
        });
    }

    if pos != data.len() {
        return Err(CommitCodecError::EnvelopeDecode(format!(
            "v3 signature block: consumed {} of {} bytes",
            pos,
            data.len()
        )));
    }

    Ok(sigs)
}

// ============================================================================
// Context-free datatype canonicalization
// ============================================================================

/// Repair a single CURIE-shaped `name` stored under the empty namespace.
///
/// Returns the canonical `(namespace_code, local_name)` pair if and only if
/// the `name` parses as a known CURIE — specifically:
/// - `xsd:<known-xsd-local>`
/// - `rdf:<known-rdf-local>`
/// - `rdfs:<known-rdfs-local>`
///
/// Uses `split_once(':')` so a name like `"xsd:string:extra"` does not match
/// (its prefix is `"xsd"`, its local is `"string:extra"`, and `"string:extra"`
/// is not in the XSD allowlist).
///
/// Allowlists are intentionally narrow: only well-known vocabulary locals are
/// accepted. Unknown locals pass through (they may be legitimate custom
/// datatypes or unrelated strings).
fn canonicalize_empty_curie_name(name: &str) -> Option<(u16, &'static str)> {
    let (prefix, local) = name.split_once(':')?;
    match prefix {
        "xsd" => known_xsd_local(local).map(|l| (namespaces::XSD, l)),
        "rdf" => known_rdf_local(local).map(|l| (namespaces::RDF, l)),
        "rdfs" => known_rdfs_local(local).map(|l| (namespaces::RDFS, l)),
        _ => None,
    }
}

/// Return `Some(&'static str)` for a known XSD built-in datatype local name.
/// Delegates to `fluree_vocab::datatype::KnownDatatype` so the allowlist is
/// shared with every other recognizer in the codebase.
fn known_xsd_local(local: &str) -> Option<&'static str> {
    fluree_vocab::datatype::KnownDatatype::from_xsd_local(local).map(|dt| dt.local_name())
}

/// Return `Some(&'static str)` for a known RDF built-in datatype local name.
fn known_rdf_local(local: &str) -> Option<&'static str> {
    fluree_vocab::datatype::KnownDatatype::from_rdf_local(local).map(|dt| dt.local_name())
}

/// Return `Some(&'static str)` for a known RDFS built-in datatype local name.
fn known_rdfs_local(local: &str) -> Option<&'static str> {
    // RDFS has no reserved datatype locals in current Fluree use, but we
    // accept a small allowlist of common terms defensively. Expand only if
    // a real corruption is encountered.
    match local {
        "Resource" => Some("Resource"),
        _ => None,
    }
}

/// Canonicalize an exact-match JSON-LD shorthand keyword (no other forms).
fn canonicalize_jsonld_shorthand(name: &str) -> Option<(u16, &'static str)> {
    match name {
        "@json" => Some((namespaces::RDF, "JSON")),
        "@vector" => Some((namespaces::FLUREE_DB, "embeddingVector")),
        "@fulltext" => Some((namespaces::FLUREE_DB, "fullText")),
        _ => None,
    }
}

/// Canonicalize a datatype `Sid` to its canonical form.
///
/// Handles only the **context-free subset** — cases where the fix is
/// determined by the `Sid` alone with no chain namespace lookup:
///
/// - `namespace_code: EMPTY, name: "xsd:string"` (and other allowlisted XSD
///   locals) → `(XSD, "string")`
/// - `namespace_code: EMPTY, name: "rdf:JSON" | "rdf:langString"` → `(RDF, <local>)`
/// - `namespace_code: EMPTY, name: "@json" | "@vector" | "@fulltext"` → canonical
/// - `namespace_code: JSON_LD, name: "json"` (exact match only) → `(RDF, "JSON")`
///
/// Returns `None` for:
/// - Already-canonical Sids
/// - Dynamic-prefix cases (non-EMPTY, non-JSON_LD namespace codes) — handled
///   by the resolver during raw-op replay
/// - CURIE names whose local is not in the XSD/RDF/RDFS allowlist
/// - Any other shape
fn canonicalize_dt_sid(dt: &Sid) -> Option<Sid> {
    match dt.namespace_code {
        namespaces::EMPTY => {
            let name = dt.name.as_ref();
            if let Some((ns, local)) = canonicalize_empty_curie_name(name) {
                Some(Sid::new(ns, local))
            } else if let Some((ns, local)) = canonicalize_jsonld_shorthand(name) {
                Some(Sid::new(ns, local))
            } else {
                None
            }
        }
        namespaces::JSON_LD => {
            // Earlier aliasing: @json was sometimes encoded as JSON_LD + "json"
            // instead of RDF + "JSON". Exact match only — no prefix / split logic.
            if dt.name.as_ref() == "json" {
                Some(Sid::new(namespaces::RDF, "JSON"))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Canonicalize a `(dt_ns, dt_name)` pair using only the static allowlist.
/// Returns a `&'static str` for the canonical local name so callers that
/// borrow into op-stream data can substitute without allocation or lifetime
/// gymnastics.
///
/// Exposed to the rest of the `codec` module so that
/// [`CommitOps::for_each_op`](super::raw_reader::CommitOps::for_each_op)
/// can inline this rewrite when iterating v3 ops.
pub(in crate::commit::codec) fn canonicalize_dt_parts_static(
    dt_ns: u16,
    dt_name: &str,
) -> Option<(u16, &'static str)> {
    match dt_ns {
        namespaces::EMPTY => canonicalize_empty_curie_name(dt_name)
            .or_else(|| canonicalize_jsonld_shorthand(dt_name)),
        namespaces::JSON_LD => {
            // Earlier aliasing: @json was sometimes encoded as
            // `(JSON_LD, "json")` instead of `(RDF, "JSON")`.
            if dt_name == "json" {
                Some((namespaces::RDF, "JSON"))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Canonicalize a `(dt_ns, dt_name)` pair in txn-meta parts representation.
///
/// Thin wrapper around [`canonicalize_dt_parts_static`] that allocates an
/// owned `String` for callers that need to rewrite
/// `TxnMetaValue::TypedLiteral` fields in place.
fn canonicalize_dt_parts(dt_ns: u16, dt_name: &str) -> Option<(u16, String)> {
    canonicalize_dt_parts_static(dt_ns, dt_name).map(|(ns, name)| (ns, name.to_string()))
}

/// Walk flakes and canonicalize their `dt` fields in place. Returns the
/// number of fixups applied.
fn canonicalize_flake_datatypes(flakes: &mut [crate::Flake]) -> usize {
    let mut count = 0;
    for flake in flakes {
        if let Some(canonical) = canonicalize_dt_sid(&flake.dt) {
            flake.dt = canonical;
            count += 1;
        }
    }
    count
}

/// Walk txn-meta entries and canonicalize typed-literal datatypes in place.
/// Returns the number of fixups applied.
fn canonicalize_txn_meta_datatypes(entries: &mut [TxnMetaEntry]) -> usize {
    let mut count = 0;
    for entry in entries {
        if let TxnMetaValue::TypedLiteral { dt_ns, dt_name, .. } = &mut entry.value {
            if let Some((new_ns, new_name)) = canonicalize_dt_parts(*dt_ns, dt_name) {
                *dt_ns = new_ns;
                *dt_name = new_name;
                count += 1;
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_sid_empty_xsd_curie() {
        let s = Sid::new(namespaces::EMPTY, "xsd:string");
        let c = canonicalize_dt_sid(&s).unwrap();
        assert_eq!(c.namespace_code, namespaces::XSD);
        assert_eq!(c.name.as_ref(), "string");
    }

    #[test]
    fn canonicalize_sid_empty_rdf_curie() {
        let s = Sid::new(namespaces::EMPTY, "rdf:JSON");
        let c = canonicalize_dt_sid(&s).unwrap();
        assert_eq!(c.namespace_code, namespaces::RDF);
        assert_eq!(c.name.as_ref(), "JSON");
    }

    #[test]
    fn canonicalize_sid_empty_shorthand() {
        let s = Sid::new(namespaces::EMPTY, "@json");
        let c = canonicalize_dt_sid(&s).unwrap();
        assert_eq!(c.namespace_code, namespaces::RDF);
        assert_eq!(c.name.as_ref(), "JSON");

        let s = Sid::new(namespaces::EMPTY, "@vector");
        let c = canonicalize_dt_sid(&s).unwrap();
        assert_eq!(c.namespace_code, namespaces::FLUREE_DB);
        assert_eq!(c.name.as_ref(), "embeddingVector");
    }

    #[test]
    fn canonicalize_sid_jsonld_json_alias() {
        let s = Sid::new(namespaces::JSON_LD, "json");
        let c = canonicalize_dt_sid(&s).unwrap();
        assert_eq!(c.namespace_code, namespaces::RDF);
        assert_eq!(c.name.as_ref(), "JSON");
    }

    #[test]
    fn canonicalize_sid_already_canonical_is_none() {
        let s = Sid::new(namespaces::XSD, "string");
        assert!(canonicalize_dt_sid(&s).is_none());
    }

    #[test]
    fn canonicalize_sid_dynamic_prefix_is_none() {
        // Dynamic-prefix corruption (ns_code=14 etc.) is NOT handled here —
        // that's the resolver's job during replay.
        let s = Sid::new(14, "string");
        assert!(canonicalize_dt_sid(&s).is_none());
    }

    #[test]
    fn canonicalize_sid_rejects_extra_colon_segments() {
        // "xsd:string:extra" must NOT be rewritten to (XSD, "string:extra") —
        // split_once gives us ("xsd", "string:extra"), and "string:extra" is
        // not in the XSD allowlist.
        let s = Sid::new(namespaces::EMPTY, "xsd:string:extra");
        assert!(canonicalize_dt_sid(&s).is_none());
    }

    #[test]
    fn canonicalize_sid_rejects_unknown_xsd_local() {
        // A CURIE with an xsd: prefix but an unknown local name passes
        // through unchanged — it may be a legitimate custom datatype.
        let s = Sid::new(namespaces::EMPTY, "xsd:notARealType");
        assert!(canonicalize_dt_sid(&s).is_none());
    }

    #[test]
    fn canonicalize_sid_rejects_unknown_rdf_local() {
        let s = Sid::new(namespaces::EMPTY, "rdf:notAThing");
        assert!(canonicalize_dt_sid(&s).is_none());
    }

    #[test]
    fn canonicalize_sid_jsonld_only_exact_match() {
        // Only exact-match JSON-LD keywords are rewritten; a variant like
        // "@jsonld" or "@json-something" must not match.
        assert!(canonicalize_dt_sid(&Sid::new(namespaces::EMPTY, "@jsonld")).is_none());
        assert!(canonicalize_dt_sid(&Sid::new(namespaces::EMPTY, "@json-extra")).is_none());
        // And JSON_LD + anything other than exact "json" is untouched.
        assert!(canonicalize_dt_sid(&Sid::new(namespaces::JSON_LD, "JSON")).is_none());
        assert!(canonicalize_dt_sid(&Sid::new(namespaces::JSON_LD, "json-suffix")).is_none());
    }

    #[test]
    fn canonicalize_sid_accepts_full_xsd_allowlist() {
        // Spot-check a representative sample across numeric, temporal, and
        // string subtype locals to confirm the allowlist is populated.
        for (local, expected_ns) in [
            ("string", namespaces::XSD),
            ("integer", namespaces::XSD),
            ("int", namespaces::XSD),
            ("long", namespaces::XSD),
            ("boolean", namespaces::XSD),
            ("double", namespaces::XSD),
            ("dateTime", namespaces::XSD),
            ("gYearMonth", namespaces::XSD),
            ("anyURI", namespaces::XSD),
            ("normalizedString", namespaces::XSD),
        ] {
            let curie = format!("xsd:{local}");
            let s = Sid::new(namespaces::EMPTY, &curie);
            let c = canonicalize_dt_sid(&s)
                .unwrap_or_else(|| panic!("should canonicalize xsd:{local}"));
            assert_eq!(c.namespace_code, expected_ns);
            assert_eq!(c.name.as_ref(), local);
        }
    }

    #[test]
    fn canonicalize_parts_empty_xsd_curie() {
        let (ns, name) = canonicalize_dt_parts(namespaces::EMPTY, "xsd:integer").unwrap();
        assert_eq!(ns, namespaces::XSD);
        assert_eq!(name, "integer");
    }

    #[test]
    fn canonicalize_parts_already_canonical_is_none() {
        assert!(canonicalize_dt_parts(namespaces::XSD, "string").is_none());
    }

    #[test]
    fn canonicalize_txn_meta_typed_literal() {
        let mut entries = vec![TxnMetaEntry {
            predicate_ns: 100,
            predicate_name: "custom".to_string(),
            value: TxnMetaValue::TypedLiteral {
                value: "42".to_string(),
                dt_ns: namespaces::EMPTY,
                dt_name: "xsd:integer".to_string(),
            },
        }];
        let count = canonicalize_txn_meta_datatypes(&mut entries);
        assert_eq!(count, 1);
        match &entries[0].value {
            TxnMetaValue::TypedLiteral { dt_ns, dt_name, .. } => {
                assert_eq!(*dt_ns, namespaces::XSD);
                assert_eq!(dt_name, "integer");
            }
            _ => panic!("expected TypedLiteral"),
        }
    }

    // ========================================================================
    // Round-trip decode tests: build a v3 blob by hand and verify the decoder
    // recovers the expected `Commit` / `CommitEnvelope` / `CommitOps`.
    // ========================================================================

    use crate::commit::codec::envelope::{encode_envelope_fields, CodecEnvelope};
    use crate::commit::codec::format::{
        self, CommitFooter, CommitHeader, DictLocation, FOOTER_LEN, HEADER_LEN,
    };
    use crate::commit::codec::op_codec::{encode_op, CommitDicts};
    use crate::{Flake, FlakeValue};
    use std::collections::HashMap;

    /// Build a minimal v3 commit blob from flakes.
    ///
    /// Layout: `[header][envelope][ops][dicts][footer][hash: 32B]`
    /// The header is written with `version: VERSION_V3`, and the trailing
    /// hash is computed as `SHA-256(blob[..hash_offset])`.
    fn build_v3_test_blob(flakes: &[Flake], t: i64) -> Vec<u8> {
        // Encode ops into a buffer and populate dicts
        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        // Envelope
        let envelope = CodecEnvelope {
            t,
            parents: Vec::new(),
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        // Serialize dicts
        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(),
            dicts.subject.serialize(),
            dicts.predicate.serialize(),
            dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        // Layout offsets
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = DictLocation {
                offset,
                len: d.len() as u32,
            };
            offset += d.len() as u64;
        }

        let footer = CommitFooter {
            dicts: dict_locations,
            ops_section_len: ops_buf.len() as u32,
        };
        let header = CommitHeader {
            version: format::VERSION_V3,
            flags: 0,
            t,
            op_count: flakes.len() as u32,
            envelope_len: envelope_bytes.len() as u32,
            sig_block_len: 0,
        };

        // Size includes trailing 32-byte hash
        let body_len = HEADER_LEN
            + envelope_bytes.len()
            + ops_buf.len()
            + dict_bytes.iter().map(std::vec::Vec::len).sum::<usize>()
            + FOOTER_LEN;
        let total_len = body_len + HASH_LEN_V3;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);
        // pos is now at body_len (start of trailing hash)
        assert_eq!(pos + FOOTER_LEN, body_len);

        // Compute + embed the trailing SHA-256 hash of the body
        let body_hash: [u8; 32] = Sha256::digest(&blob[..body_len]).into();
        blob[body_len..body_len + HASH_LEN_V3].copy_from_slice(&body_hash);

        blob
    }

    /// Build a single flake with a given datatype Sid, for corruption tests.
    fn test_flake(dt: Sid, t: i64) -> Flake {
        Flake::new(
            Sid::new(100, "s1"), // subject
            Sid::new(100, "p1"), // predicate
            FlakeValue::String("hello".to_string()),
            dt,
            t,
            true, // assert
            None, // no metadata
        )
    }

    #[test]
    fn v3_read_commit_round_trip_with_curie_canonicalization() {
        // Craft a flake whose datatype is the corrupted form
        // `Sid { namespace_code: EMPTY, name: "xsd:string" }` — the
        // context-free subset that decode canonicalizes.
        let corrupted_dt = Sid::new(namespaces::EMPTY, "xsd:string");
        let flake = test_flake(corrupted_dt, 1);
        let blob = build_v3_test_blob(&[flake], 1);

        // Dispatch through the public read_commit — should route to v3.
        let commit = crate::commit::codec::read_commit(&blob).unwrap();
        assert_eq!(commit.t, 1);
        assert_eq!(commit.flakes.len(), 1);

        // The flake datatype must be canonicalized to (XSD, "string").
        assert_eq!(commit.flakes[0].dt.namespace_code, namespaces::XSD);
        assert_eq!(commit.flakes[0].dt.name.as_ref(), "string");
    }

    #[test]
    fn v3_read_commit_dynamic_prefix_passes_through() {
        // Dynamic-prefix corruption: ns_code=14 pointing at some dynamically
        // allocated prefix. The decoder MUST NOT rewrite this — it's the
        // resolver's job at replay time with chain context.
        let corrupted_dt = Sid::new(14, "string");
        let flake = test_flake(corrupted_dt.clone(), 1);
        let blob = build_v3_test_blob(&[flake], 1);

        let commit = crate::commit::codec::read_commit(&blob).unwrap();
        // Decoder preserves the on-disk shape.
        assert_eq!(commit.flakes[0].dt.namespace_code, 14);
        assert_eq!(commit.flakes[0].dt.name.as_ref(), "string");
    }

    #[test]
    fn v3_verify_commit_blob_returns_embedded_hash_cid() {
        let flake = test_flake(Sid::new(namespaces::XSD, "string"), 1);
        let blob = build_v3_test_blob(&[flake], 1);

        let cid = crate::commit::codec::verify_commit_blob(&blob).unwrap();
        // CID should be derived from the embedded SHA-256 body hash — i.e.,
        // it should match `ContentId::from_sha256_digest` over the body.
        let body_end = blob.len() - HASH_LEN_V3;
        let body_hash: [u8; 32] = Sha256::digest(&blob[..body_end]).into();
        let expected = ContentId::from_sha256_digest(CODEC_FLUREE_COMMIT, &body_hash);
        assert_eq!(cid, expected);
    }

    #[test]
    fn v3_read_commit_envelope_round_trip() {
        let flake = test_flake(Sid::new(namespaces::XSD, "string"), 42);
        let blob = build_v3_test_blob(&[flake], 42);
        let envelope = crate::commit::codec::read_commit_envelope(&blob).unwrap();
        assert_eq!(envelope.t, 42);
        assert!(envelope.parents.is_empty());
        assert!(envelope.txn_meta.is_empty());
    }

    #[test]
    fn v3_load_commit_ops_round_trip_passes_through_dynamic_prefix() {
        // Raw-op path is pass-through — dynamic-prefix datatypes survive
        // unchanged so the resolver can canonicalize during replay.
        let corrupted_dt = Sid::new(14, "string");
        let flake = test_flake(corrupted_dt, 7);
        let blob = build_v3_test_blob(&[flake], 7);

        let ops = crate::commit::codec::load_commit_ops(&blob).unwrap();
        assert_eq!(ops.t, 7);
        assert_eq!(ops.op_count, 1);

        let mut saw_op = false;
        ops.for_each_op(|raw_op| {
            saw_op = true;
            // Pass-through: on-disk ns_code preserved.
            assert_eq!(raw_op.dt_ns_code, 14);
            assert_eq!(raw_op.dt_name, "string");
            Ok(())
        })
        .unwrap();
        assert!(saw_op);
    }

    #[test]
    fn v3_hash_mismatch_is_detected() {
        let flake = test_flake(Sid::new(namespaces::XSD, "string"), 1);
        let mut blob = build_v3_test_blob(&[flake], 1);
        // Corrupt the last byte of the embedded hash.
        let last = blob.len() - 1;
        blob[last] ^= 0xff;

        let err = crate::commit::codec::verify_commit_blob(&blob).unwrap_err();
        assert!(matches!(
            err,
            crate::commit::codec::CommitCodecError::HashMismatch { .. }
        ));
    }

    #[test]
    fn v3_and_v4_dispatch_via_version_byte() {
        // Build a v3 blob, confirm `read_commit` routes to the v3 path.
        let flake = test_flake(Sid::new(namespaces::XSD, "string"), 1);
        let v3_blob = build_v3_test_blob(std::slice::from_ref(&flake), 1);
        assert_eq!(v3_blob[4], format::VERSION_V3);
        let v3_commit = crate::commit::codec::read_commit(&v3_blob).unwrap();
        assert_eq!(v3_commit.t, 1);

        // And confirm unknown versions are rejected cleanly.
        let mut bad_blob = v3_blob.clone();
        bad_blob[4] = 99;
        let err = crate::commit::codec::read_commit(&bad_blob).unwrap_err();
        assert!(matches!(
            err,
            crate::commit::codec::CommitCodecError::UnsupportedVersion(99)
        ));
    }
}
