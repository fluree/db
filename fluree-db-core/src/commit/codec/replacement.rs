//! Redacted replacements of v4 commits (`FCR1`).
//!
//! Permanent erasure stores, in place of an original commit, a replacement
//! that the redaction transform (`fluree-db-redaction`) produced and a proof
//! verified: the original's header and envelope byte for byte, its surviving
//! operations under their original slot numbers, and its dictionaries less
//! the entries only removed operations used. Read here, a replacement is the
//! original commit minus its redacted operations, with the original's CID as
//! its identity. It carries no signatures (none sign these bytes); it names
//! the original signers the proof verified.
//!
//! Layout, integers little-endian:
//!
//! ```text
//! "FCR1" | version u32 | original CID (u16 len, bytes) | original header (32 bytes)
//! | envelope (u32 len, bytes) | op_count u32 | per slot: 0, varint len, op bytes | 1 (redacted)
//! | 5 x (entry count varint | per entry: 0, varint len, bytes | 1 (removed))
//! | signer count u16 | per signer: u16 len, DID bytes
//! ```

use super::envelope::decode_envelope;
use super::error::CommitCodecError;
use super::format::{CommitHeader, HEADER_LEN, VERSION};
use super::op_codec::{decode_op, ReadDicts};
use super::raw_reader::CommitOps;
use super::string_dict::StringDict;
use super::varint::{decode_varint, read_exact, read_u8};
use crate::commit::{Commit, CommitEnvelope, Redaction};
use crate::ContentId;

/// Leading bytes of a replacement blob.
pub const REPLACEMENT_MAGIC: [u8; 4] = *b"FCR1";

/// Replacement encoding (and transform) version this reader decodes.
pub const REPLACEMENT_VERSION: u32 = 1;

struct Replacement<'a> {
    original: ContentId,
    header: CommitHeader,
    envelope: &'a [u8],
    /// Surviving operations: original slot and encoded bytes.
    survivors: Vec<(u32, &'a [u8])>,
    dicts: ReadDicts,
    signers: Vec<String>,
}

fn malformed(what: &str) -> CommitCodecError {
    CommitCodecError::InvalidOp(format!("replacement: {what}"))
}

fn u16_at(data: &[u8], pos: &mut usize) -> Result<u16, CommitCodecError> {
    Ok(u16::from_le_bytes(
        read_exact(data, pos, 2)?.try_into().unwrap(),
    ))
}

fn u32_at(data: &[u8], pos: &mut usize) -> Result<u32, CommitCodecError> {
    Ok(u32::from_le_bytes(
        read_exact(data, pos, 4)?.try_into().unwrap(),
    ))
}

fn len_prefixed<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8], CommitCodecError> {
    let len = decode_varint(data, pos)? as usize;
    read_exact(data, pos, len)
}

/// Whether `bytes` are a replacement rather than a commit.
pub fn is_replacement(bytes: &[u8]) -> bool {
    bytes.get(..4) == Some(&REPLACEMENT_MAGIC[..])
}

fn parse(bytes: &[u8]) -> Result<Replacement<'_>, CommitCodecError> {
    let mut pos = 0;
    if read_exact(bytes, &mut pos, 4)? != REPLACEMENT_MAGIC {
        return Err(CommitCodecError::InvalidMagic);
    }
    let version = u32_at(bytes, &mut pos)?;
    if version != REPLACEMENT_VERSION {
        return Err(malformed(&format!("version {version}")));
    }
    let cid_len = u16_at(bytes, &mut pos)? as usize;
    let original = ContentId::from_bytes(read_exact(bytes, &mut pos, cid_len)?)
        .map_err(|e| malformed(&format!("original CID: {e}")))?;
    let header = CommitHeader::read_from(read_exact(bytes, &mut pos, HEADER_LEN)?)?;
    if header.version != VERSION {
        return Err(malformed("not a v4 original"));
    }
    let envelope_len = u32_at(bytes, &mut pos)? as usize;
    let envelope = read_exact(bytes, &mut pos, envelope_len)?;
    let op_count = u32_at(bytes, &mut pos)?;
    if op_count != header.op_count {
        return Err(malformed("slot count is not the original's"));
    }
    let mut survivors = Vec::new();
    for slot in 0..op_count {
        match read_u8(bytes, &mut pos)? {
            0 => survivors.push((slot, len_prefixed(bytes, &mut pos)?)),
            1 => {}
            _ => return Err(malformed("slot tag")),
        }
    }
    let dict = |pos: &mut usize| -> Result<StringDict, CommitCodecError> {
        let count = decode_varint(bytes, pos)? as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            // A removed entry keeps its position; no survivor refers to it.
            entries.push(match read_u8(bytes, pos)? {
                0 => std::str::from_utf8(len_prefixed(bytes, pos)?)
                    .map_err(|_| malformed("dictionary entry is not UTF-8"))?
                    .to_string(),
                1 => String::new(),
                _ => return Err(malformed("dictionary entry tag")),
            });
        }
        Ok(StringDict::from_entries(entries))
    };
    let dicts = ReadDicts {
        graph: dict(&mut pos)?,
        subject: dict(&mut pos)?,
        predicate: dict(&mut pos)?,
        datatype: dict(&mut pos)?,
        object_ref: dict(&mut pos)?,
    };
    let signer_count = u16_at(bytes, &mut pos)?;
    let mut signers = Vec::with_capacity(signer_count as usize);
    for _ in 0..signer_count {
        let len = u16_at(bytes, &mut pos)? as usize;
        signers.push(
            std::str::from_utf8(read_exact(bytes, &mut pos, len)?)
                .map_err(|_| malformed("signer is not UTF-8"))?
                .to_string(),
        );
    }
    if pos != bytes.len() {
        return Err(malformed("trailing bytes"));
    }
    Ok(Replacement {
        original,
        header,
        envelope,
        survivors,
        dicts,
        signers,
    })
}

/// The original commit a replacement stands for: its CID as identity, its
/// surviving flakes, and [`Commit::redaction`] naming their slots.
pub(crate) fn read_commit_replacement(bytes: &[u8]) -> Result<Commit, CommitCodecError> {
    let r = parse(bytes)?;
    let envelope = decode_envelope(r.envelope)?;
    let mut flakes = Vec::with_capacity(r.survivors.len());
    for (_, op) in &r.survivors {
        let mut pos = 0;
        flakes.push(decode_op(op, &mut pos, &r.dicts, r.header.t)?);
        if pos != op.len() {
            return Err(malformed("operation trailing bytes"));
        }
    }
    Ok(Commit {
        id: Some(r.original),
        t: r.header.t,
        time: envelope.time,
        flakes,
        parents: envelope.parents,
        txn: envelope.txn,
        namespace_delta: envelope.namespace_delta,
        txn_signature: envelope.txn_signature,
        commit_signatures: Vec::new(),
        txn_meta: envelope.txn_meta,
        graph_delta: envelope.graph_delta,
        ns_split_mode: envelope.ns_split_mode,
        redaction: Some(Redaction {
            slots: r.survivors.iter().map(|(slot, _)| *slot).collect(),
            op_count: r.header.op_count,
            signers: r.signers,
        }),
    })
}

/// A replacement's envelope: the original's.
pub(crate) fn read_envelope_replacement(bytes: &[u8]) -> Result<CommitEnvelope, CommitCodecError> {
    let r = parse(bytes)?;
    let env = decode_envelope(r.envelope)?;
    Ok(CommitEnvelope {
        t: r.header.t,
        parents: env.parents,
        txn: env.txn,
        namespace_delta: env.namespace_delta,
        txn_meta: env.txn_meta,
        graph_delta: env.graph_delta,
        ns_split_mode: env.ns_split_mode,
    })
}

/// A replacement's surviving operations, for raw replay.
pub(crate) fn load_commit_ops_replacement(bytes: &[u8]) -> Result<CommitOps, CommitCodecError> {
    let r = parse(bytes)?;
    let mut envelope = decode_envelope(r.envelope)?;
    envelope.t = r.header.t;
    let ops: Vec<u8> = r
        .survivors
        .iter()
        .flat_map(|(_, op)| *op)
        .copied()
        .collect();
    Ok(CommitOps::new(
        envelope,
        r.header.t,
        r.survivors.len() as u32,
        r.dicts,
        ops,
    ))
}
