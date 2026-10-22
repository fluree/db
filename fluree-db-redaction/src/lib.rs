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
    /// empty for a second step, which does not see the original, and when
    /// the signatures are exposed instead.
    pub signers: Vec<String>,
    /// The original's signatures, exposed for the verifier to check outside
    /// the proof ([`Signatures::Expose`]), with the body hash they sign.
    pub signatures: Vec<SignatureEvidence>,
    pub body_hash: Vec<u8>,
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

fn skip_inline_str(data: &[u8], pos: &mut usize) -> Result<()> {
    let len = read_varint(data, pos)? as usize;
    take(data, pos, len)?;
    Ok(())
}

fn name_ref<F>(data: &[u8], pos: &mut usize, dict: usize, visit: &mut F) -> Result<()>
where
    F: FnMut(usize, u32) -> Result<()>,
{
    read_varint(data, pos)?; // namespace code
    let id = read_varint(data, pos)? as u32;
    visit(dict, id)
}

fn skip_object<F>(tag: u8, data: &[u8], pos: &mut usize, visit: &mut F) -> Result<()>
where
    F: FnMut(usize, u32) -> Result<()>,
{
    match tag {
        0 => name_ref(data, pos, OBJECT_REF, visit)?,
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
            name_ref(data, pos, SUBJECT, visit)?;
            name_ref(data, pos, PREDICATE, visit)?;
            name_ref(data, pos, DATATYPE, visit)?;
            if u8_at(data, pos)? != 0 {
                skip_inline_str(data, pos)?;
            }
            let nested = u8_at(data, pos)?;
            skip_object(nested, data, pos, visit)?;
        }
        _ => return Err(Error::Malformed("unknown object tag")),
    }
    Ok(())
}

/// Parse one operation at `*pos`, visiting each dictionary id it uses.
fn parse_op<F>(data: &[u8], pos: &mut usize, visit: &mut F) -> Result<()>
where
    F: FnMut(usize, u32) -> Result<()>,
{
    let g_ns = read_varint(data, pos)?;
    let g_id = read_varint(data, pos)? as u32;
    if g_ns != 0 || g_id != 0 {
        if g_id == 0 {
            return Err(Error::Malformed("graph name id 0"));
        }
        visit(GRAPH, g_id)?;
    }
    name_ref(data, pos, SUBJECT, visit)?;
    name_ref(data, pos, PREDICATE, visit)?;
    name_ref(data, pos, DATATYPE, visit)?;
    let tag = u8_at(data, pos)?;
    skip_object(tag, data, pos, visit)?;
    let flags = u8_at(data, pos)?;
    if flags & OP_FLAG_HAS_LANG != 0 {
        skip_inline_str(data, pos)?;
    }
    if flags & OP_FLAG_HAS_I != 0 {
        read_varint(data, pos)?;
    }
    Ok(())
}

fn parse_dict(data: &[u8]) -> Result<Vec<Option<&[u8]>>> {
    let mut pos = 0;
    let count = read_varint(data, &mut pos)? as usize;
    let mut entries = Vec::with_capacity(count.min(data.len()));
    for _ in 0..count {
        let len = read_varint(data, &mut pos)? as usize;
        entries.push(Some(take(data, &mut pos, len)?));
    }
    if pos != data.len() {
        return Err(Error::Malformed("dictionary trailing bytes"));
    }
    Ok(entries)
}

/// Which dictionary entries surviving and removed operations use.
struct Usage {
    kept: [Vec<bool>; DICTS],
    dropped: [Vec<bool>; DICTS],
}

impl Usage {
    fn new(dicts: &[Vec<Option<&[u8]>>; DICTS]) -> Self {
        Self {
            kept: core::array::from_fn(|d| vec![false; dicts[d].len()]),
            dropped: core::array::from_fn(|d| vec![false; dicts[d].len()]),
        }
    }

    fn mark(&mut self, removed: bool, dict: usize, id: u32) -> Result<()> {
        let marks = if removed {
            &mut self.dropped[dict]
        } else {
            &mut self.kept[dict]
        };
        let index = (id as usize)
            .checked_sub(1)
            .filter(|i| *i < marks.len())
            .ok_or(Error::Malformed("dictionary id out of range"))?;
        marks[index] = true;
        Ok(())
    }
}

/// `removed` as a bitmap over `count` slots, of which `present` are not
/// yet redacted.
fn removal_map(
    removed: &[u32],
    count: usize,
    present: impl Fn(usize) -> bool,
) -> Result<Vec<bool>> {
    if removed.is_empty() {
        return Err(Error::Slots("none requested"));
    }
    if removed.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::Slots("not ascending and distinct"));
    }
    let mut map = vec![false; count];
    for &slot in removed {
        let slot = slot as usize;
        if slot >= count {
            return Err(Error::Slots("out of range"));
        }
        if !present(slot) {
            return Err(Error::Slots("already removed"));
        }
        map[slot] = true;
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// The commit as the transform sees it
// ---------------------------------------------------------------------------

/// A commit, original or replacement, in slot form, borrowing its input.
struct Slotted<'a> {
    original_cid: Vec<u8>,
    /// The original 32-byte header, kept verbatim.
    header: [u8; HEADER_LEN],
    envelope: &'a [u8],
    /// Each slot's encoded operation; `None` once redacted.
    slots: Vec<Option<&'a [u8]>>,
    /// Each dictionary's entries (1-based ids); `None` once removed.
    dicts: [Vec<Option<&'a [u8]>>; DICTS],
    /// Original signers, verified when the original was read.
    signers: Vec<String>,
}

impl Slotted<'_> {
    fn t(&self) -> u32 {
        u32::from_le_bytes(self.header[6..10].try_into().unwrap())
    }
}

/// One signature over an original commit body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureEvidence {
    /// The signer's `did:key`.
    pub signer: String,
    /// The Ed25519 signature (64 bytes).
    pub signature: Vec<u8>,
}

/// Whether the guest verifies the original's signatures, or exposes them
/// for the verifier to check outside the proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signatures {
    Verify,
    /// Cheaper, but a signed commit's CID covers signatures a verifier does
    /// not otherwise hold: with them and the body hash, anyone able to
    /// rebuild the rest of the body can test guesses at what was removed.
    Expose,
}

/// The digest a commit signature signs: the body hash, bound to the ledger.
fn signed_digest<H: Sha256>(ledger_id: &str, body_hash: &[u8]) -> [u8; 32] {
    let mut to_sign = Vec::with_capacity(COMMIT_DOMAIN.len() + 10 + ledger_id.len() + 32);
    to_sign.extend_from_slice(COMMIT_DOMAIN);
    write_varint(ledger_id.len() as u64, &mut to_sign);
    to_sign.extend_from_slice(ledger_id.as_bytes());
    to_sign.extend_from_slice(body_hash);
    H::sha256(&to_sign)
}

/// The signatures in a V4 signature block.
fn read_signatures(sig_block: &[u8]) -> Result<Vec<SignatureEvidence>> {
    let mut pos = 0;
    let count = u16_le(sig_block, &mut pos)?;
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let signer_len = u16_le(sig_block, &mut pos)? as usize;
        let signer = core::str::from_utf8(take(sig_block, &mut pos, signer_len)?)
            .map_err(|_| Error::Malformed("signer is not UTF-8"))?
            .to_string();
        if u8_at(sig_block, &mut pos)? != ALGO_ED25519 {
            return Err(Error::Signature("unknown algorithm"));
        }
        let signature = take(sig_block, &mut pos, 64)?.to_vec();
        let meta_len = u16_le(sig_block, &mut pos)? as usize;
        take(sig_block, &mut pos, meta_len)?;
        out.push(SignatureEvidence { signer, signature });
    }
    if pos != sig_block.len() {
        return Err(Error::Malformed("signature block trailing bytes"));
    }
    Ok(out)
}

/// Verify each signature over `digest`; their signers.
fn check_signatures(evidence: &[SignatureEvidence], digest: &[u8; 32]) -> Result<Vec<String>> {
    evidence
        .iter()
        .map(|e| {
            let signature: [u8; 64] = e
                .signature
                .as_slice()
                .try_into()
                .map_err(|_| Error::Signature("not 64 bytes"))?;
            ed25519_key_of_did(&e.signer)?
                .verify_strict(digest, &ed25519_dalek::Signature::from_bytes(&signature))
                .map_err(|_| Error::Signature("does not verify"))?;
            Ok(e.signer.clone())
        })
        .collect()
}

/// The signers of a statement that exposes its original's signatures
/// ([`Signatures::Expose`]), each verified here, outside the proof: the
/// proof binds the body hash and signatures to the original CID.
pub fn verify_exposed_signatures<H: Sha256>(statement: &Statement) -> Result<Vec<String>> {
    check_exposed::<H>(
        &statement.ledger_id,
        &statement.body_hash,
        &statement.signatures,
    )
}

/// Verify signatures exposed with the body hash they sign; their signers.
pub fn check_exposed<H: Sha256>(
    ledger_id: &str,
    body_hash: &[u8],
    signatures: &[SignatureEvidence],
) -> Result<Vec<String>> {
    if signatures.is_empty() {
        return Ok(Vec::new());
    }
    check_signatures(signatures, &signed_digest::<H>(ledger_id, body_hash))
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

/// The parts of an original V4 commit, located but not yet parsed.
struct Original<'a> {
    header: [u8; HEADER_LEN],
    compressed: bool,
    op_count: u32,
    body: &'a [u8],
    envelope: &'a [u8],
    ops_section: &'a [u8],
    /// Where the ops section ends, and the dictionaries begin.
    ops_end: usize,
    footer_start: usize,
    dicts: [(usize, usize); DICTS],
    sig_block: Option<&'a [u8]>,
}

fn locate_original(blob: &[u8]) -> Result<Original<'_>> {
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
    let footer_start = body_end
        .checked_sub(FOOTER_LEN)
        .ok_or(Error::Malformed("footer"))?;
    let mut fpos = footer_start;
    let mut dicts = [(0usize, 0usize); DICTS];
    for loc in &mut dicts {
        let offset = u64_le(blob, &mut fpos)? as usize;
        let len = u32_le(blob, &mut fpos)? as usize;
        *loc = (offset, len);
    }
    let ops_len = u32_le(blob, &mut fpos)? as usize;
    let mut pos = HEADER_LEN;
    let envelope = take(body, &mut pos, envelope_len)?;
    let ops_section = take(body, &mut pos, ops_len)?;
    if pos > footer_start {
        return Err(Error::Malformed("ops overlap the footer"));
    }
    Ok(Original {
        header,
        compressed: flags & FLAG_ZSTD != 0,
        op_count,
        body,
        envelope,
        ops_section,
        ops_end: pos,
        footer_start,
        dicts,
        sig_block: signed.then(|| &blob[body_end..]),
    })
}

/// The original's signers: verified here, or exposed with the body hash
/// they sign for a verifier to check.
fn original_signers<H: Sha256>(
    o: &Original,
    ledger_id: &str,
    mode: Signatures,
    exposed: &mut Option<(Vec<SignatureEvidence>, Vec<u8>)>,
) -> Result<Vec<String>> {
    let Some(sig_block) = o.sig_block else {
        return Ok(Vec::new());
    };
    let evidence = read_signatures(sig_block)?;
    let body_hash = H::sha256(o.body);
    match mode {
        Signatures::Verify => {
            check_signatures(&evidence, &signed_digest::<H>(ledger_id, &body_hash))
        }
        Signatures::Expose => {
            let signers = evidence.iter().map(|e| e.signer.clone()).collect();
            *exposed = Some((evidence, body_hash.to_vec()));
            Ok(signers)
        }
    }
}

#[cfg(not(any(feature = "c-zstd", feature = "rust-zstd")))]
fn decompress(_: &[u8]) -> Result<Vec<u8>> {
    Err(Error::Malformed("this build decompresses no commits"))
}

#[cfg(all(feature = "rust-zstd", not(feature = "c-zstd")))]
fn decompress(section: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ruzstd::decoding::StreamingDecoder::new(section)
        .map_err(|_| Error::Malformed("ops section is not zstd"))?;
    let mut out = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut out)
        .map_err(|_| Error::Malformed("ops section does not decompress"))?;
    Ok(out)
}

#[cfg(feature = "c-zstd")]
fn decompress(section: &[u8]) -> Result<Vec<u8>> {
    let bad = || Error::Malformed("ops section does not decompress");
    let mut dctx = zstd_safe::DCtx::create();
    let mut out: Vec<u8> = Vec::with_capacity(section.len().saturating_mul(4).max(1024));
    let mut input = zstd_safe::InBuffer::around(section);
    loop {
        if out.len() == out.capacity() {
            out.reserve(out.capacity());
        }
        let pos = out.len();
        let remaining = {
            let mut output = zstd_safe::OutBuffer::around_pos(&mut out, pos);
            dctx.decompress_stream(&mut output, &mut input)
                .map_err(|_| bad())?
        };
        if remaining == 0 && input.pos() == section.len() {
            return Ok(out);
        }
        // Input left, or output full: go on. Neither: the frame is cut short.
        if input.pos() == section.len() && out.len() < out.capacity() {
            return Err(bad());
        }
    }
}

/// Slot an original's operations (`ops`, decompressed) and dictionaries,
/// marking which dictionary entries removed and surviving operations use in
/// the one pass that finds the operations.
fn slot_original<'a>(
    blob: &'a [u8],
    o: &Original<'a>,
    ops: &'a [u8],
    original_cid: Vec<u8>,
    signers: Vec<String>,
    removed: &[u32],
) -> Result<(Slotted<'a>, Usage)> {
    let mut dicts: [Vec<Option<&[u8]>>; DICTS] = Default::default();
    let mut prev_end = o.ops_end;
    for (dict, (offset, len)) in dicts.iter_mut().zip(o.dicts) {
        let end = offset
            .checked_add(len)
            .ok_or(Error::Malformed("dictionary bounds"))?;
        if offset < prev_end || end > o.footer_start {
            return Err(Error::Malformed("dictionary bounds"));
        }
        prev_end = end;
        *dict = parse_dict(&blob[offset..end])?;
    }
    let count = o.op_count as usize;
    // Nothing removed when only slotting (authentication).
    let is_removed = if removed.is_empty() {
        vec![false; count]
    } else {
        removal_map(removed, count, |_| true)?
    };
    let mut usage = Usage::new(&dicts);
    let mut slots = Vec::with_capacity(count);
    let mut pos = 0;
    for gone in is_removed {
        let start = pos;
        parse_op(ops, &mut pos, &mut |dict, id| usage.mark(gone, dict, id))?;
        slots.push(Some(&ops[start..pos]));
    }
    if pos != ops.len() {
        return Err(Error::Malformed("ops trailing bytes"));
    }
    Ok((
        Slotted {
            original_cid,
            header: o.header,
            envelope: o.envelope,
            slots,
            dicts,
            signers,
        },
        usage,
    ))
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
    out.extend_from_slice(c.envelope);
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

fn read_replacement(blob: &[u8]) -> Result<Slotted<'_>> {
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
    let envelope = take(blob, &mut pos, envelope_len)?;
    let op_count = u32_le(blob, &mut pos)?;
    let mut slots = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        slots.push(match u8_at(blob, &mut pos)? {
            0 => {
                let len = read_varint(blob, &mut pos)? as usize;
                Some(take(blob, &mut pos, len)?)
            }
            1 => None,
            _ => return Err(Error::Malformed("slot tag")),
        });
    }
    let mut dicts: [Vec<Option<&[u8]>>; DICTS] = Default::default();
    for dict in &mut dicts {
        let count = read_varint(blob, &mut pos)? as usize;
        for _ in 0..count {
            dict.push(match u8_at(blob, &mut pos)? {
                0 => {
                    let len = read_varint(blob, &mut pos)? as usize;
                    Some(take(blob, &mut pos, len)?)
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

/// Mark which dictionary entries a replacement's removed and surviving
/// operations use: one parse of each operation still held.
fn replacement_usage(c: &Slotted, removed: &[u32]) -> Result<Usage> {
    let is_removed = removal_map(removed, c.slots.len(), |slot| c.slots[slot].is_some())?;
    let mut usage = Usage::new(&c.dicts);
    for (op, gone) in c.slots.iter().zip(is_removed) {
        let Some(op) = op else { continue };
        let mut pos = 0;
        parse_op(op, &mut pos, &mut |dict, id| usage.mark(gone, dict, id))?;
        if pos != op.len() {
            return Err(Error::Malformed("operation trailing bytes"));
        }
    }
    Ok(usage)
}

/// Remove `removed` (validated) from `c`, and every dictionary string only
/// they used. Returns the strings removed.
fn remove_slots(c: &mut Slotted, usage: &Usage, removed: &[u32]) -> u32 {
    let mut removed_terms = 0;
    for d in 0..DICTS {
        for (i, entry) in c.dicts[d].iter_mut().enumerate() {
            if entry.is_some() && usage.dropped[d][i] && !usage.kept[d][i] {
                *entry = None;
                removed_terms += 1;
            }
        }
    }
    for &slot in removed {
        c.slots[slot as usize] = None;
    }
    removed_terms
}

/// Redact `input` (an original V4 commit or an `FCR1` replacement), removing
/// `removed` slots, verifying the original's signatures. Returns the
/// replacement and its statement.
pub fn redact<H: Sha256, P: Probe>(
    input: &[u8],
    removed: &[u32],
    ledger_id: &str,
    probe: &mut P,
) -> Result<(Vec<u8>, Statement)> {
    redact_with::<H, P>(input, removed, ledger_id, Signatures::Verify, probe)
}

/// [`redact`], with the original's signatures verified or exposed as
/// `mode` says.
pub fn redact_with<H: Sha256, P: Probe>(
    input: &[u8],
    removed: &[u32],
    ledger_id: &str,
    mode: Signatures,
    probe: &mut P,
) -> Result<(Vec<u8>, Statement)> {
    if removed.is_empty() {
        return Err(Error::Slots("none requested"));
    }
    let input_cid = commit_cid(&H::sha256(input));
    probe.mark("hash_input");
    let original = input.starts_with(V4_MAGIC);
    let mut exposed = None;
    let decompressed: Vec<u8>;
    let (mut c, usage) = if original {
        let o = locate_original(input)?;
        let signers = original_signers::<H>(&o, ledger_id, mode, &mut exposed)?;
        probe.mark("signatures");
        let ops: &[u8] = if o.compressed {
            decompressed = decompress(o.ops_section)?;
            &decompressed
        } else {
            o.ops_section
        };
        probe.mark("decompress");
        let slotted = slot_original(input, &o, ops, input_cid.clone(), signers, removed)?;
        probe.mark("parse");
        slotted
    } else {
        let c = read_replacement(input)?;
        let usage = replacement_usage(&c, removed)?;
        probe.mark("parse");
        (c, usage)
    };
    let removed_terms = remove_slots(&mut c, &usage, removed);
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
        signers: if original && exposed.is_none() {
            c.signers.clone()
        } else {
            Vec::new()
        },
        signatures: exposed
            .as_ref()
            .map(|(evidence, _)| evidence.clone())
            .unwrap_or_default(),
        body_hash: exposed.map(|(_, hash)| hash).unwrap_or_default(),
        ledger_id: ledger_id.to_string(),
        removed_terms,
    };
    Ok((replacement, statement))
}

// ---------------------------------------------------------------------------
// Commit authentication and selective disclosure
// ---------------------------------------------------------------------------

/// Version of [`CommitClaim`] and of its leaf encoding.
pub const CLAIM_VERSION: u32 = 2;

/// Bytes of a leaf's salt.
pub const SALT_LEN: usize = 32;

const SALT_DOMAIN: &[u8] = b"fluree/leaf-salt";

/// What a commit authentication proof makes public: a commit, by the CID
/// the ledger's history anchors, its place in that history, and a Merkle
/// root over its operations' salted leaves, so any one operation can be
/// disclosed later with its salt and a path of hashes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitClaim {
    pub claim_version: u32,
    pub cid: Vec<u8>,
    pub t: u32,
    /// Parent CIDs (binary), from the envelope.
    pub parents: Vec<Vec<u8>>,
    pub op_count: u32,
    /// RFC 9162 Merkle tree hash over the operations' salted leaves, in
    /// slot order (see [`leaves`] and [`leaf_hash`]).
    pub ops_root: Vec<u8>,
    /// Verified signers, or the signatures exposed for the verifier to check
    /// (see [`Signatures`]).
    pub signers: Vec<String>,
    pub signatures: Vec<SignatureEvidence>,
    pub body_hash: Vec<u8>,
    pub ledger_id: String,
}

/// The parent CIDs an envelope names.
fn envelope_parents(env: &[u8]) -> Result<Vec<Vec<u8>>> {
    let bad = |m| Error::Malformed(m);
    let mut pos = 0;
    let zz = read_varint(env, &mut pos)?;
    let version = ((zz >> 1) as i64) ^ (-((zz & 1) as i64));
    if version != 2 && version != 3 {
        return Err(bad("envelope version"));
    }
    let flags = u8_at(env, &mut pos)?;
    if flags & !0x9f != 0 {
        return Err(bad("envelope flags"));
    }
    let len_bytes = |pos: &mut usize| -> Result<Vec<u8>> {
        let len = read_varint(env, pos)? as usize;
        Ok(take(env, pos, len)?.to_vec())
    };
    if flags & 0x01 != 0 {
        for _ in 0..read_varint(env, &mut pos)? {
            read_varint(env, &mut pos)?; // predicate namespace
            skip_inline_str(env, &mut pos)?; // predicate name
            match u8_at(env, &mut pos)? {
                0 => skip_inline_str(env, &mut pos)?,
                1 => {
                    skip_inline_str(env, &mut pos)?;
                    read_varint(env, &mut pos)?;
                    skip_inline_str(env, &mut pos)?;
                }
                2 => {
                    skip_inline_str(env, &mut pos)?;
                    skip_inline_str(env, &mut pos)?;
                }
                3 => {
                    read_varint(env, &mut pos)?;
                    skip_inline_str(env, &mut pos)?;
                }
                4 => {
                    read_varint(env, &mut pos)?;
                }
                5 => {
                    take(env, &mut pos, 8)?;
                }
                6 => {
                    take(env, &mut pos, 1)?;
                }
                _ => return Err(bad("txn_meta tag")),
            }
        }
    }
    let mut parents = Vec::new();
    if flags & 0x02 != 0 {
        let count = if version == 3 {
            read_varint(env, &mut pos)?
        } else {
            1
        };
        for _ in 0..count {
            parents.push(len_bytes(&mut pos)?);
        }
    }
    Ok(parents)
}

/// Append operation `slot`'s leaf to `out`: its slot, its encoded bytes,
/// and every dictionary entry it refers to, in the order it refers to them,
/// so a disclosed leaf reads on its own.
fn write_leaf(
    out: &mut Vec<u8>,
    slot: u32,
    op: &[u8],
    dicts: &[Vec<Option<&[u8]>>; DICTS],
) -> Result<()> {
    out.extend_from_slice(&slot.to_le_bytes());
    write_varint(op.len() as u64, out);
    out.extend_from_slice(op);
    let mut pos = 0;
    parse_op(op, &mut pos, &mut |d, id| {
        let entry = (id as usize)
            .checked_sub(1)
            .and_then(|i| dicts[d].get(i).copied().flatten())
            .ok_or(Error::Malformed("dictionary id out of range"))?;
        out.push(d as u8);
        write_varint(entry.len() as u64, out);
        out.extend_from_slice(entry);
        Ok(())
    })
}

fn leaf(slot: u32, op: &[u8], dicts: &[Vec<Option<&[u8]>>; DICTS]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(op.len() + 64);
    write_leaf(&mut out, slot, op, dicts)?;
    Ok(out)
}

/// The leaves of an original commit's operations, in slot order.
pub fn leaves(blob: &[u8]) -> Result<Vec<Vec<u8>>> {
    let o = locate_original(blob)?;
    let decompressed;
    let ops: &[u8] = if o.compressed {
        decompressed = decompress(o.ops_section)?;
        &decompressed
    } else {
        o.ops_section
    };
    let (c, _) = slot_original(blob, &o, ops, Vec::new(), Vec::new(), &[])?;
    c.slots
        .iter()
        .enumerate()
        .map(|(slot, op)| leaf(slot as u32, op.unwrap_or_default(), &c.dicts))
        .collect()
}

/// Leaf `slot`'s salt, from the commit holder's secret `seed`. Only a
/// disclosure of that leaf reveals it, so the hashes on a path cannot be
/// tested against guesses at the leaves they cover.
pub fn leaf_salt<H: Sha256>(seed: &[u8; 32], slot: u32) -> [u8; SALT_LEN] {
    let mut data = [0u8; SALT_DOMAIN.len() + 32 + 4];
    data[..SALT_DOMAIN.len()].copy_from_slice(SALT_DOMAIN);
    data[SALT_DOMAIN.len()..SALT_DOMAIN.len() + 32].copy_from_slice(seed);
    data[SALT_DOMAIN.len() + 32..].copy_from_slice(&slot.to_le_bytes());
    H::sha256(&data)
}

/// The salts of `n` leaves, concatenated: what [`authenticate`] reads.
pub fn leaf_salts<H: Sha256>(seed: &[u8; 32], n: usize) -> Vec<u8> {
    (0..n as u32)
        .flat_map(|i| leaf_salt::<H>(seed, i))
        .collect()
}

/// The RFC 9162 leaf hash of a salted leaf: `SHA256(0x00 || salt || leaf)`.
pub fn leaf_hash<H: Sha256>(salt: &[u8; SALT_LEN], leaf: &[u8]) -> [u8; 32] {
    let mut data = Vec::with_capacity(1 + SALT_LEN + leaf.len());
    data.push(0);
    data.extend_from_slice(salt);
    data.extend_from_slice(leaf);
    H::sha256(&data)
}

fn node_hash<H: Sha256>(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut data = [0u8; 65];
    data[0] = 1;
    data[1..33].copy_from_slice(left);
    data[33..].copy_from_slice(right);
    H::sha256(&data)
}

/// The largest power of two smaller than `n` (n > 1).
fn split(n: usize) -> usize {
    1 << (usize::BITS - 1 - (n - 1).leading_zeros())
}

/// RFC 9162 Merkle tree hash over leaf hashes.
pub fn merkle_root<H: Sha256>(hashes: &[[u8; 32]]) -> [u8; 32] {
    match hashes.len() {
        0 => H::sha256(&[]),
        1 => hashes[0],
        n => {
            let k = split(n);
            node_hash::<H>(
                &merkle_root::<H>(&hashes[..k]),
                &merkle_root::<H>(&hashes[k..]),
            )
        }
    }
}

/// RFC 9162 audit path for leaf `m`.
pub fn audit_path<H: Sha256>(m: usize, hashes: &[[u8; 32]]) -> Vec<[u8; 32]> {
    let n = hashes.len();
    if n <= 1 {
        return Vec::new();
    }
    let k = split(n);
    let (mut path, sibling) = if m < k {
        (
            audit_path::<H>(m, &hashes[..k]),
            merkle_root::<H>(&hashes[k..]),
        )
    } else {
        (
            audit_path::<H>(m - k, &hashes[k..]),
            merkle_root::<H>(&hashes[..k]),
        )
    };
    path.push(sibling);
    path
}

/// The root an audit path leads to from leaf `m` of `n`; `None` when the
/// path is not one for that position.
pub fn root_from_path<H: Sha256>(
    m: usize,
    n: usize,
    leaf_hash: [u8; 32],
    path: &[[u8; 32]],
) -> Option<[u8; 32]> {
    if m >= n {
        return None;
    }
    if n == 1 {
        return path.is_empty().then_some(leaf_hash);
    }
    let k = split(n);
    let (sibling, rest) = path.split_last()?;
    Some(if m < k {
        node_hash::<H>(&root_from_path::<H>(m, k, leaf_hash, rest)?, sibling)
    } else {
        node_hash::<H>(
            sibling,
            &root_from_path::<H>(m - k, n - k, leaf_hash, rest)?,
        )
    })
}

/// Authenticate an original V4 commit: its CID, parents, signers and the
/// Merkle root over its operations' leaves, each salted with its
/// `SALT_LEN` bytes of `salts` (see [`leaf_salts`]).
pub fn authenticate<H: Sha256, P: Probe>(
    input: &[u8],
    ledger_id: &str,
    mode: Signatures,
    salts: &[u8],
    probe: &mut P,
) -> Result<CommitClaim> {
    let cid = commit_cid(&H::sha256(input));
    probe.mark("hash_input");
    let o = locate_original(input)?;
    let mut exposed = None;
    let signers = original_signers::<H>(&o, ledger_id, mode, &mut exposed)?;
    probe.mark("signatures");
    let parents = envelope_parents(o.envelope)?;
    let decompressed;
    let ops: &[u8] = if o.compressed {
        decompressed = decompress(o.ops_section)?;
        &decompressed
    } else {
        o.ops_section
    };
    probe.mark("decompress");
    let (c, _) = slot_original(input, &o, ops, Vec::new(), Vec::new(), &[])?;
    probe.mark("parse");
    if salts.len() != SALT_LEN * c.slots.len() {
        return Err(Error::Malformed("not one salt per operation"));
    }
    // Each leaf hashed as [`leaf_hash`] does, in one buffer.
    let mut buf = Vec::with_capacity(256);
    let mut hashes = Vec::with_capacity(c.slots.len());
    for ((slot, op), salt) in c.slots.iter().enumerate().zip(salts.chunks_exact(SALT_LEN)) {
        buf.clear();
        buf.push(0);
        buf.extend_from_slice(salt);
        write_leaf(&mut buf, slot as u32, op.unwrap_or_default(), &c.dicts)?;
        hashes.push(H::sha256(&buf));
    }
    probe.mark("leaves");
    let ops_root = merkle_root::<H>(&hashes);
    probe.mark("tree");
    Ok(CommitClaim {
        claim_version: CLAIM_VERSION,
        cid,
        t: c.t(),
        parents,
        op_count: c.slots.len() as u32,
        ops_root: ops_root.to_vec(),
        signers: if exposed.is_none() {
            signers
        } else {
            Vec::new()
        },
        signatures: exposed.as_ref().map(|(e, _)| e.clone()).unwrap_or_default(),
        body_hash: exposed.map(|(_, h)| h).unwrap_or_default(),
        ledger_id: ledger_id.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Ancestry
// ---------------------------------------------------------------------------

/// Version of [`ChainClaim`].
pub const CHAIN_VERSION: u32 = 1;

/// What a chain proof makes public: commit `head_cid` descends, along first
/// parents, from `base_cid`, `length` commits in all. Each link proves one
/// commit and verifies the proof of the link before it, which must be this
/// same program (`image_id`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainClaim {
    pub claim_version: u32,
    pub image_id: [u32; 8],
    pub base_cid: Vec<u8>,
    pub head_cid: Vec<u8>,
    pub head_t: u32,
    pub length: u32,
}

/// Extend `prev` (or start a chain) with `commit`, whose first parent must
/// be `prev`'s head.
pub fn chain_link<H: Sha256>(
    commit: &[u8],
    prev: Option<&ChainClaim>,
    image_id: [u32; 8],
) -> Result<ChainClaim> {
    let cid = commit_cid(&H::sha256(commit));
    let o = locate_original(commit)?;
    let t = u32::from_le_bytes(o.header[6..10].try_into().unwrap());
    let (base_cid, length) = match prev {
        None => (cid.clone(), 1),
        Some(prev) => {
            if prev.image_id != image_id {
                return Err(Error::Malformed("the previous link is another program's"));
            }
            let parents = envelope_parents(o.envelope)?;
            if parents.first() != Some(&prev.head_cid) {
                return Err(Error::Malformed(
                    "the commit does not follow the previous link",
                ));
            }
            (prev.base_cid.clone(), prev.length + 1)
        }
    };
    Ok(ChainClaim {
        claim_version: CHAIN_VERSION,
        image_id,
        base_cid,
        head_cid: cid,
        head_t: t,
        length,
    })
}
