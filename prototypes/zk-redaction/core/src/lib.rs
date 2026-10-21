//! The redaction transform for Fluree V4 commit blobs.
//!
//! Removes selected operation slots from a commit. Every other operation
//! keeps its bytes, order and slot number; the envelope (parents, `t`, txn,
//! metadata) is copied byte for byte; a dictionary string is removed only
//! when no surviving operation refers to it. The result is a deterministic
//! replacement blob (`FCR1`), and a [`Statement`] binding the input and
//! output CIDs, the slots removed, the transform version and the original
//! signers whose signatures over the original body were checked.
//!
//! The same code runs in the zkVM guest, which proves it, and on the host,
//! which predicts it. A replacement can be redacted again: the statement
//! then relates the current replacement to the next, carrying the original
//! CID and slot numbering.

use serde::{Deserialize, Serialize};

/// Version of the transform and of the `FCR1` encoding.
pub const TRANSFORM_VERSION: u32 = 1;

const V4_MAGIC: &[u8; 4] = b"FCV2";
const V4_VERSION: u8 = 4;
const REPLACEMENT_MAGIC: &[u8; 4] = b"FCR1";
const HEADER_LEN: usize = 32;
const FOOTER_LEN: usize = 64;
const FLAG_ZSTD: u8 = 0x01;
const FLAG_HAS_COMMIT_SIG: u8 = 0x02;
const OP_FLAG_HAS_LANG: u8 = 0x02;
const OP_FLAG_HAS_I: u8 = 0x04;
const ALGO_ED25519: u8 = 0x01;
const CODEC_FLUREE_COMMIT: u64 = 0x30_0001;
const SHA2_256: u8 = 0x12;
const COMMIT_DOMAIN: &[u8] = b"fluree/commit/v1";
/// The five commit dictionaries: graph, subject, predicate, datatype,
/// object reference.
const DICTS: usize = 5;
const GRAPH: usize = 0;
const SUBJECT: usize = 1;
const PREDICATE: usize = 2;
const DATATYPE: usize = 3;
const OBJECT_REF: usize = 4;

/// SHA-256, supplied by the environment: the zkVM's accelerated circuit in
/// the guest, the `sha2` crate on the host.
pub trait Sha256 {
    fn sha256(data: &[u8]) -> [u8; 32];
}

/// SHA-256 from the `sha2` crate.
#[cfg(feature = "host-sha")]
pub struct HostSha;

#[cfg(feature = "host-sha")]
impl Sha256 for HostSha {
    fn sha256(data: &[u8]) -> [u8; 32] {
        use sha2::Digest;
        sha2::Sha256::digest(data).into()
    }
}

/// Records phase boundaries, for cost breakdowns. `()` records nothing.
pub trait Probe {
    fn mark(&mut self, phase: &'static str);
}

impl Probe for () {
    fn mark(&mut self, _: &'static str) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The input is not a well-formed V4 commit or `FCR1` replacement.
    Malformed(&'static str),
    /// The slots requested are empty, unsorted, repeated, out of range or
    /// already removed.
    Slots(&'static str),
    /// An original signature does not verify.
    Signature(&'static str),
    /// A replacement of another transform version.
    Version(u32),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::Malformed(m) => write!(f, "malformed input: {m}"),
            Error::Slots(m) => write!(f, "invalid slots: {m}"),
            Error::Signature(m) => write!(f, "signature: {m}"),
            Error::Version(v) => write!(f, "replacement of transform version {v}"),
        }
    }
}

pub type Result<T> = core::result::Result<T, Error>;

/// What a proof of one redaction step makes public.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    pub transform_version: u32,
    /// CID (binary) of the step's input: the original commit, or the
    /// replacement a previous step produced.
    pub input_cid: Vec<u8>,
    /// CID (binary) of the replacement this step produced.
    pub output_cid: Vec<u8>,
    /// CID of the original commit the chain started from.
    pub original_cid: Vec<u8>,
    /// Slots this step removed, ascending.
    pub removed_slots: Vec<u32>,
    /// Every slot the output no longer holds, ascending.
    pub redacted_slots: Vec<u32>,
    /// Operations in the original commit.
    pub op_count: u32,
    pub t: u32,
    /// Signers whose signature over the original body this step verified;
    /// empty for a second step, which does not see the original.
    pub signers: Vec<String>,
    /// Ledger the signatures were made for.
    pub ledger_id: String,
    /// Dictionary strings this step removed (no survivor used them).
    pub removed_terms: u32,
}

/// The guest's input.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProveInput {
    pub input: Vec<u8>,
    pub removed_slots: Vec<u32>,
    pub ledger_id: String,
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *data.get(*pos).ok_or(Error::Malformed("varint past end"))?;
        *pos += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Malformed("varint too long"))
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn take<'a>(data: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(n)
        .ok_or(Error::Malformed("length overflow"))?;
    let slice = data
        .get(*pos..end)
        .ok_or(Error::Malformed("read past end"))?;
    *pos = end;
    Ok(slice)
}

fn u8_at(data: &[u8], pos: &mut usize) -> Result<u8> {
    Ok(take(data, pos, 1)?[0])
}

fn u16_le(data: &[u8], pos: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(take(data, pos, 2)?.try_into().unwrap()))
}

fn u32_le(data: &[u8], pos: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take(data, pos, 4)?.try_into().unwrap()))
}

fn u64_le(data: &[u8], pos: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take(data, pos, 8)?.try_into().unwrap()))
}

/// The binary CIDv1 of a commit blob with SHA-256 `digest`.
pub fn commit_cid(digest: &[u8; 32]) -> Vec<u8> {
    let mut cid = vec![0x01];
    write_varint(CODEC_FLUREE_COMMIT, &mut cid);
    cid.push(SHA2_256);
    cid.push(32);
    cid.extend_from_slice(digest);
    cid
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// The dictionary ids one operation refers to, by dictionary.
type Refs = [Vec<u32>; DICTS];

fn skip_inline_str(data: &[u8], pos: &mut usize) -> Result<()> {
    let len = read_varint(data, pos)? as usize;
    take(data, pos, len)?;
    Ok(())
}

fn name_ref(data: &[u8], pos: &mut usize, dict: usize, refs: &mut Refs) -> Result<()> {
    read_varint(data, pos)?; // namespace code
    let id = read_varint(data, pos)? as u32;
    refs[dict].push(id);
    Ok(())
}

fn skip_object(tag: u8, data: &[u8], pos: &mut usize, refs: &mut Refs) -> Result<()> {
    match tag {
        0 => name_ref(data, pos, OBJECT_REF, refs)?,
        1 => {
            read_varint(data, pos)?;
        }
        2 => {
            take(data, pos, 8)?;
        }
        3 | 5..=10 | 12..=19 => skip_inline_str(data, pos)?,
        4 => {
            take(data, pos, 1)?;
        }
        11 => {}
        20 => {
            take(data, pos, 16)?;
        }
        21 => {
            let len = read_varint(data, pos)? as usize;
            take(
                data,
                pos,
                len.checked_mul(8)
                    .ok_or(Error::Malformed("vector length"))?,
            )?;
        }
        22 => {
            name_ref(data, pos, SUBJECT, refs)?;
            name_ref(data, pos, PREDICATE, refs)?;
            name_ref(data, pos, DATATYPE, refs)?;
            if u8_at(data, pos)? != 0 {
                skip_inline_str(data, pos)?;
            }
            let nested = u8_at(data, pos)?;
            skip_object(nested, data, pos, refs)?;
        }
        _ => return Err(Error::Malformed("unknown object tag")),
    }
    Ok(())
}

/// Parse one operation at `*pos`, returning the dictionary ids it uses.
fn parse_op(data: &[u8], pos: &mut usize) -> Result<Refs> {
    let mut refs: Refs = Default::default();
    let g_ns = read_varint(data, pos)?;
    let g_id = read_varint(data, pos)? as u32;
    if g_ns != 0 || g_id != 0 {
        if g_id == 0 {
            return Err(Error::Malformed("graph name id 0"));
        }
        refs[GRAPH].push(g_id);
    }
    name_ref(data, pos, SUBJECT, &mut refs)?;
    name_ref(data, pos, PREDICATE, &mut refs)?;
    name_ref(data, pos, DATATYPE, &mut refs)?;
    let tag = u8_at(data, pos)?;
    skip_object(tag, data, pos, &mut refs)?;
    let flags = u8_at(data, pos)?;
    if flags & OP_FLAG_HAS_LANG != 0 {
        skip_inline_str(data, pos)?;
    }
    if flags & OP_FLAG_HAS_I != 0 {
        read_varint(data, pos)?;
    }
    Ok(refs)
}

fn parse_dict(data: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
    let mut pos = 0;
    let count = read_varint(data, &mut pos)? as usize;
    let mut entries = Vec::with_capacity(count.min(data.len()));
    for _ in 0..count {
        let len = read_varint(data, &mut pos)? as usize;
        entries.push(Some(take(data, &mut pos, len)?.to_vec()));
    }
    if pos != data.len() {
        return Err(Error::Malformed("dictionary trailing bytes"));
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// The commit as the transform sees it
// ---------------------------------------------------------------------------

/// A commit, original or replacement, in slot form.
struct Slotted {
    original_cid: Vec<u8>,
    /// The original 32-byte header, kept verbatim.
    header: [u8; HEADER_LEN],
    envelope: Vec<u8>,
    /// Each slot's encoded operation; `None` once redacted.
    slots: Vec<Option<Vec<u8>>>,
    /// Each dictionary's entries (1-based ids); `None` once removed.
    dicts: [Vec<Option<Vec<u8>>>; DICTS],
    /// Original signers, verified when the original was read.
    signers: Vec<String>,
}

impl Slotted {
    fn t(&self) -> u32 {
        u32::from_le_bytes(self.header[6..10].try_into().unwrap())
    }
}

/// Verify every signature in a V4 signature block over `body`.
fn verify_signatures<H: Sha256>(
    sig_block: &[u8],
    body: &[u8],
    ledger_id: &str,
) -> Result<Vec<String>> {
    let body_hash = H::sha256(body);
    let mut to_sign = Vec::with_capacity(COMMIT_DOMAIN.len() + 10 + ledger_id.len() + 32);
    to_sign.extend_from_slice(COMMIT_DOMAIN);
    write_varint(ledger_id.len() as u64, &mut to_sign);
    to_sign.extend_from_slice(ledger_id.as_bytes());
    to_sign.extend_from_slice(&body_hash);
    let digest = H::sha256(&to_sign);

    let mut pos = 0;
    let count = u16_le(sig_block, &mut pos)?;
    let mut signers = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let signer_len = u16_le(sig_block, &mut pos)? as usize;
        let signer = core::str::from_utf8(take(sig_block, &mut pos, signer_len)?)
            .map_err(|_| Error::Malformed("signer is not UTF-8"))?
            .to_string();
        if u8_at(sig_block, &mut pos)? != ALGO_ED25519 {
            return Err(Error::Signature("unknown algorithm"));
        }
        let signature: [u8; 64] = take(sig_block, &mut pos, 64)?.try_into().unwrap();
        let meta_len = u16_le(sig_block, &mut pos)? as usize;
        take(sig_block, &mut pos, meta_len)?;
        let key = ed25519_key_of_did(&signer)?;
        key.verify_strict(&digest, &ed25519_dalek::Signature::from_bytes(&signature))
            .map_err(|_| Error::Signature("does not verify"))?;
        signers.push(signer);
    }
    if pos != sig_block.len() {
        return Err(Error::Malformed("signature block trailing bytes"));
    }
    Ok(signers)
}

fn ed25519_key_of_did(did: &str) -> Result<ed25519_dalek::VerifyingKey> {
    let encoded = did
        .strip_prefix("did:key:z")
        .ok_or(Error::Signature("signer is not a did:key"))?;
    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|_| Error::Signature("signer key is not base58"))?;
    let key: [u8; 32] = bytes
        .strip_prefix(&[0xed, 0x01])
        .and_then(|k| k.try_into().ok())
        .ok_or(Error::Signature("signer is not an Ed25519 key"))?;
    ed25519_dalek::VerifyingKey::from_bytes(&key).map_err(|_| Error::Signature("invalid key"))
}

/// Read an original V4 commit, verifying its signatures.
fn read_original<H: Sha256, P: Probe>(
    blob: &[u8],
    original_cid: Vec<u8>,
    ledger_id: &str,
    probe: &mut P,
) -> Result<Slotted> {
    if blob.len() < HEADER_LEN + FOOTER_LEN || &blob[0..4] != V4_MAGIC {
        return Err(Error::Malformed("not a commit"));
    }
    if blob[4] != V4_VERSION {
        return Err(Error::Malformed("not a v4 commit"));
    }
    let header: [u8; HEADER_LEN] = blob[..HEADER_LEN].try_into().unwrap();
    let flags = blob[5];
    let op_count = u32::from_le_bytes(blob[10..14].try_into().unwrap());
    let envelope_len = u32::from_le_bytes(blob[14..18].try_into().unwrap()) as usize;
    let sig_block_len = u16::from_le_bytes(blob[18..20].try_into().unwrap()) as usize;
    let signed = flags & FLAG_HAS_COMMIT_SIG != 0 && sig_block_len > 0;
    let body_end = if signed {
        blob.len()
            .checked_sub(sig_block_len)
            .ok_or(Error::Malformed("signature block length"))?
    } else {
        blob.len()
    };
    let body = &blob[..body_end];

    let signers = if signed {
        verify_signatures::<H>(&blob[body_end..], body, ledger_id)?
    } else {
        Vec::new()
    };
    probe.mark("signatures");

    let footer_start = body_end
        .checked_sub(FOOTER_LEN)
        .ok_or(Error::Malformed("footer"))?;
    let mut fpos = footer_start;
    let mut locations = [(0usize, 0usize); DICTS];
    for loc in &mut locations {
        let offset = u64_le(blob, &mut fpos)? as usize;
        let len = u32_le(blob, &mut fpos)? as usize;
        *loc = (offset, len);
    }
    let ops_len = u32_le(blob, &mut fpos)? as usize;
    let mut pos = HEADER_LEN;
    let envelope = take(body, &mut pos, envelope_len)?.to_vec();
    let ops_section = take(body, &mut pos, ops_len)?;
    if pos > footer_start {
        return Err(Error::Malformed("ops overlap the footer"));
    }

    let decompressed;
    let ops: &[u8] = if flags & FLAG_ZSTD != 0 {
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(ops_section)
            .map_err(|_| Error::Malformed("ops section is not zstd"))?;
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut out)
            .map_err(|_| Error::Malformed("ops section does not decompress"))?;
        decompressed = out;
        &decompressed
    } else {
        ops_section
    };
    probe.mark("decompress");

    let mut slots = Vec::with_capacity(op_count as usize);
    let mut opos = 0;
    for _ in 0..op_count {
        let start = opos;
        parse_op(ops, &mut opos)?;
        slots.push(Some(ops[start..opos].to_vec()));
    }
    if opos != ops.len() {
        return Err(Error::Malformed("ops trailing bytes"));
    }
    let mut dicts: [Vec<Option<Vec<u8>>>; DICTS] = Default::default();
    let mut prev_end = pos;
    for (dict, (offset, len)) in dicts.iter_mut().zip(locations) {
        let end = offset
            .checked_add(len)
            .ok_or(Error::Malformed("dictionary bounds"))?;
        if offset < prev_end || end > footer_start {
            return Err(Error::Malformed("dictionary bounds"));
        }
        prev_end = end;
        *dict = parse_dict(&blob[offset..end])?;
    }
    probe.mark("parse");

    Ok(Slotted {
        original_cid,
        header,
        envelope,
        slots,
        dicts,
        signers,
    })
}

/// Encode a replacement. Layout (`FCR1`), all integers little-endian:
///
/// ```text
/// "FCR1" | transform_version u32 | original_cid (u16 len, bytes)
/// | original header (32 bytes) | envelope (u32 len, bytes)
/// | op_count u32 | per slot: 0, varint len, op bytes | 1 (redacted)
/// | 5 x (entry count varint | per entry: 0, varint len, bytes | 1 (removed))
/// | signer count u16 | per signer: u16 len, DID bytes
/// ```
fn encode_replacement(c: &Slotted) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(REPLACEMENT_MAGIC);
    out.extend_from_slice(&TRANSFORM_VERSION.to_le_bytes());
    out.extend_from_slice(&(c.original_cid.len() as u16).to_le_bytes());
    out.extend_from_slice(&c.original_cid);
    out.extend_from_slice(&c.header);
    out.extend_from_slice(&(c.envelope.len() as u32).to_le_bytes());
    out.extend_from_slice(&c.envelope);
    out.extend_from_slice(&(c.slots.len() as u32).to_le_bytes());
    for slot in &c.slots {
        match slot {
            Some(op) => {
                out.push(0);
                write_varint(op.len() as u64, &mut out);
                out.extend_from_slice(op);
            }
            None => out.push(1),
        }
    }
    for dict in &c.dicts {
        write_varint(dict.len() as u64, &mut out);
        for entry in dict {
            match entry {
                Some(s) => {
                    out.push(0);
                    write_varint(s.len() as u64, &mut out);
                    out.extend_from_slice(s);
                }
                None => out.push(1),
            }
        }
    }
    out.extend_from_slice(&(c.signers.len() as u16).to_le_bytes());
    for signer in &c.signers {
        out.extend_from_slice(&(signer.len() as u16).to_le_bytes());
        out.extend_from_slice(signer.as_bytes());
    }
    out
}

fn read_replacement(blob: &[u8]) -> Result<Slotted> {
    let mut pos = 0;
    if take(blob, &mut pos, 4)? != REPLACEMENT_MAGIC {
        return Err(Error::Malformed("not a replacement"));
    }
    let version = u32_le(blob, &mut pos)?;
    if version != TRANSFORM_VERSION {
        return Err(Error::Version(version));
    }
    let cid_len = u16_le(blob, &mut pos)? as usize;
    let original_cid = take(blob, &mut pos, cid_len)?.to_vec();
    let header: [u8; HEADER_LEN] = take(blob, &mut pos, HEADER_LEN)?.try_into().unwrap();
    let envelope_len = u32_le(blob, &mut pos)? as usize;
    let envelope = take(blob, &mut pos, envelope_len)?.to_vec();
    let op_count = u32_le(blob, &mut pos)?;
    let mut slots = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        slots.push(match u8_at(blob, &mut pos)? {
            0 => {
                let len = read_varint(blob, &mut pos)? as usize;
                Some(take(blob, &mut pos, len)?.to_vec())
            }
            1 => None,
            _ => return Err(Error::Malformed("slot tag")),
        });
    }
    let mut dicts: [Vec<Option<Vec<u8>>>; DICTS] = Default::default();
    for dict in &mut dicts {
        let count = read_varint(blob, &mut pos)? as usize;
        for _ in 0..count {
            dict.push(match u8_at(blob, &mut pos)? {
                0 => {
                    let len = read_varint(blob, &mut pos)? as usize;
                    Some(take(blob, &mut pos, len)?.to_vec())
                }
                1 => None,
                _ => return Err(Error::Malformed("dictionary entry tag")),
            });
        }
    }
    let signer_count = u16_le(blob, &mut pos)?;
    let mut signers = Vec::with_capacity(signer_count as usize);
    for _ in 0..signer_count {
        let len = u16_le(blob, &mut pos)? as usize;
        signers.push(
            core::str::from_utf8(take(blob, &mut pos, len)?)
                .map_err(|_| Error::Malformed("signer is not UTF-8"))?
                .to_string(),
        );
    }
    if pos != blob.len() {
        return Err(Error::Malformed("replacement trailing bytes"));
    }
    Ok(Slotted {
        original_cid,
        header,
        envelope,
        slots,
        dicts,
        signers,
    })
}

/// Remove `removed` (ascending, distinct, present) from `c`, and every
/// dictionary string only they used. Returns the strings removed.
fn remove_slots(c: &mut Slotted, removed: &[u32]) -> Result<u32> {
    if removed.is_empty() {
        return Err(Error::Slots("none requested"));
    }
    if removed.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::Slots("not ascending and distinct"));
    }
    let mut is_removed = vec![false; c.slots.len()];
    for &slot in removed {
        let entry = c
            .slots
            .get(slot as usize)
            .ok_or(Error::Slots("out of range"))?;
        if entry.is_none() {
            return Err(Error::Slots("already removed"));
        }
        is_removed[slot as usize] = true;
    }
    let mut kept: [Vec<bool>; DICTS] = Default::default();
    let mut dropped: [Vec<bool>; DICTS] = Default::default();
    for d in 0..DICTS {
        kept[d] = vec![false; c.dicts[d].len()];
        dropped[d] = vec![false; c.dicts[d].len()];
    }
    for (slot, op) in c.slots.iter().enumerate() {
        let Some(op) = op else { continue };
        let mut pos = 0;
        let refs = parse_op(op, &mut pos)?;
        if pos != op.len() {
            return Err(Error::Malformed("operation trailing bytes"));
        }
        let marks = if is_removed[slot] {
            &mut dropped
        } else {
            &mut kept
        };
        for d in 0..DICTS {
            for &id in &refs[d] {
                let index = (id as usize)
                    .checked_sub(1)
                    .filter(|i| *i < marks[d].len())
                    .ok_or(Error::Malformed("dictionary id out of range"))?;
                marks[d][index] = true;
            }
        }
    }
    let mut removed_terms = 0;
    for d in 0..DICTS {
        for (i, entry) in c.dicts[d].iter_mut().enumerate() {
            if entry.is_some() && dropped[d][i] && !kept[d][i] {
                *entry = None;
                removed_terms += 1;
            }
        }
    }
    for &slot in removed {
        c.slots[slot as usize] = None;
    }
    Ok(removed_terms)
}

/// Redact `input` (an original V4 commit or an `FCR1` replacement), removing
/// `removed` slots. Returns the replacement and its statement.
pub fn redact<H: Sha256, P: Probe>(
    input: &[u8],
    removed: &[u32],
    ledger_id: &str,
    probe: &mut P,
) -> Result<(Vec<u8>, Statement)> {
    let input_cid = commit_cid(&H::sha256(input));
    probe.mark("hash_input");
    let original = input.starts_with(V4_MAGIC);
    let mut c = if original {
        read_original::<H, P>(input, input_cid.clone(), ledger_id, probe)?
    } else {
        let c = read_replacement(input)?;
        probe.mark("parse");
        c
    };
    let removed_terms = remove_slots(&mut c, removed)?;
    let replacement = encode_replacement(&c);
    probe.mark("redact");
    let output_cid = commit_cid(&H::sha256(&replacement));
    probe.mark("hash_output");
    let statement = Statement {
        transform_version: TRANSFORM_VERSION,
        input_cid,
        output_cid,
        original_cid: c.original_cid.clone(),
        removed_slots: removed.to_vec(),
        redacted_slots: (0..c.slots.len() as u32)
            .filter(|&i| c.slots[i as usize].is_none())
            .collect(),
        op_count: c.slots.len() as u32,
        t: c.t(),
        signers: if original {
            c.signers.clone()
        } else {
            Vec::new()
        },
        ledger_id: ledger_id.to_string(),
        removed_terms,
    };
    Ok((replacement, statement))
}

/// A replacement's surviving operations by slot, and its dictionaries, for
/// inspection.
pub struct ReplacementView {
    pub original_cid: Vec<u8>,
    pub envelope: Vec<u8>,
    pub slots: Vec<Option<Vec<u8>>>,
    pub dicts: Vec<Vec<Option<Vec<u8>>>>,
    pub signers: Vec<String>,
}

pub fn view_replacement(blob: &[u8]) -> Result<ReplacementView> {
    let c = read_replacement(blob)?;
    Ok(ReplacementView {
        original_cid: c.original_cid,
        envelope: c.envelope,
        slots: c.slots,
        dicts: c.dicts.into(),
        signers: c.signers,
    })
}
