//! Binary layout constants and header/footer I/O for commit format v4.
//!
//! All fixed-width numeric fields are little-endian.
//!
//! V4 layout:
//! ```text
//! [Header 32B][Envelope (binary)][Ops section][Dictionaries][Footer 64B][optional signature block]
//! ```

use super::error::CommitCodecError;
use super::varint::{read_exact, read_u8};

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes identifying a v2 commit blob.
pub const MAGIC: [u8; 4] = *b"FCV2";

/// Current (write) format version.
/// Version 4: no embedded trailing hash. CID is computed as SHA-256(full blob).
pub const VERSION: u8 = 4;

/// Legacy format version (read-only support via `legacy_v3` module).
/// Version 3: carries a trailing 32-byte embedded hash; CID is derived from
/// SHA-256(body) where body excludes the trailing hash and any signature block.
pub const VERSION_V3: u8 = 3;

/// Header size in bytes (fixed, shared by v3 and v4).
pub const HEADER_LEN: usize = 32;

/// Footer size in bytes (fixed, excludes trailing hash).
/// 5 dictionaries x (offset: u64 + len: u32) = 5 x 12 = 60, plus ops_section_len: u32 = 4.
pub const FOOTER_LEN: usize = 64;

/// Trailing SHA-256 hash size (v3 only).
pub const HASH_LEN_V3: usize = 32;

/// Minimum valid commit blob size (v4: no embedded hash).
/// V3 blobs are always at least `HEADER_LEN + FOOTER_LEN + HASH_LEN_V3 = 128`,
/// which exceeds this minimum, so this bound works for both versions.
pub const MIN_COMMIT_LEN: usize = HEADER_LEN + FOOTER_LEN; // 96

// --- Commit-level flags (header) ---

/// Bit 0: ops section is zstd-compressed.
pub const FLAG_ZSTD: u8 = 0x01;

/// Bit 1: commit has a signature block after the hash.
pub const FLAG_HAS_COMMIT_SIG: u8 = 0x02;

// --- Per-op flags ---

/// Bit 0: 1 = assert, 0 = retract.
pub const OP_FLAG_ASSERT: u8 = 0x01;
/// Bit 1: a language tag follows.
pub const OP_FLAG_HAS_LANG: u8 = 0x02;
/// Bit 2: a list index follows.
pub const OP_FLAG_HAS_I: u8 = 0x04;

// --- Object tag values ---

/// FlakeValue type discriminant in the op stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OTag {
    Ref = 0,
    Long = 1,
    Double = 2,
    String = 3,
    Boolean = 4,
    DateTime = 5,
    Date = 6,
    Time = 7,
    BigInt = 8,
    Decimal = 9,
    Json = 10,
    Null = 11,
    GYear = 12,
    GYearMonth = 13,
    GMonth = 14,
    GDay = 15,
    GMonthDay = 16,
    YearMonthDuration = 17,
    DayTimeDuration = 18,
    Duration = 19,
    GeoPoint = 20,
    Vector = 21,
}

impl OTag {
    pub fn from_u8(b: u8) -> Result<Self, CommitCodecError> {
        match b {
            0 => Ok(OTag::Ref),
            1 => Ok(OTag::Long),
            2 => Ok(OTag::Double),
            3 => Ok(OTag::String),
            4 => Ok(OTag::Boolean),
            5 => Ok(OTag::DateTime),
            6 => Ok(OTag::Date),
            7 => Ok(OTag::Time),
            8 => Ok(OTag::BigInt),
            9 => Ok(OTag::Decimal),
            10 => Ok(OTag::Json),
            11 => Ok(OTag::Null),
            12 => Ok(OTag::GYear),
            13 => Ok(OTag::GYearMonth),
            14 => Ok(OTag::GMonth),
            15 => Ok(OTag::GDay),
            16 => Ok(OTag::GMonthDay),
            17 => Ok(OTag::YearMonthDuration),
            18 => Ok(OTag::DayTimeDuration),
            19 => Ok(OTag::Duration),
            20 => Ok(OTag::GeoPoint),
            21 => Ok(OTag::Vector),
            _ => Err(CommitCodecError::InvalidOpTag(b)),
        }
    }
}

// =============================================================================
// Header
// =============================================================================

/// Signing algorithm identifier stored in commit signatures.
///
/// Unknown values must be rejected, not silently skipped.
pub const ALGO_ED25519: u8 = 0x01;

/// A single commit signature (proof of which node wrote the commit).
///
/// Independently verifiable using the domain-separated commit digest:
/// `to_sign = SHA-256("fluree/commit/v1" || varint(alias.len()) || alias || commit_hash)`
#[derive(Clone, Debug)]
pub struct CommitSignature {
    /// Signer identity (did:key:z6Mk...)
    pub signer: String,
    /// Signing algorithm (0x01 = Ed25519)
    pub algo: u8,
    /// Signature bytes (64 bytes for Ed25519) over domain-separated commit digest
    pub signature: [u8; 64],
    /// Optional metadata (node_id, region, role, etc. for consensus)
    pub metadata: Option<Vec<u8>>,
}

/// Maximum number of signatures in a signature block (decode cap).
const MAX_SIG_COUNT: u16 = 64;

/// Maximum signer DID string length (decode cap).
const MAX_SIGNER_LEN: usize = 256;

/// Maximum metadata length (decode cap).
const MAX_METADATA_LEN: usize = 4096;

/// Encode a signature block into a buffer.
///
/// Format: `sig_count: u16` (LE) + for each signature:
/// `signer_len: u16` (LE) + `signer` + `algo: u8` + `signature: [u8; 64]`
/// + `meta_len: u16` (LE) + `metadata` (if meta_len > 0)
pub fn encode_sig_block(sigs: &[CommitSignature], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(sigs.len() as u16).to_le_bytes());
    for sig in sigs {
        let signer_bytes = sig.signer.as_bytes();
        buf.extend_from_slice(&(signer_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(signer_bytes);
        buf.push(sig.algo);
        buf.extend_from_slice(&sig.signature);
        // metadata (optional, length-prefixed)
        if let Some(meta) = &sig.metadata {
            buf.extend_from_slice(&(meta.len() as u16).to_le_bytes());
            buf.extend_from_slice(meta);
        } else {
            buf.extend_from_slice(&0u16.to_le_bytes());
        }
    }
}

/// Decode a signature block from a byte slice.
///
/// Returns the parsed signatures and verifies the entire block is consumed.
pub fn decode_sig_block(data: &[u8]) -> Result<Vec<CommitSignature>, CommitCodecError> {
    let mut pos = 0;
    let sig_count = u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap());
    if sig_count > MAX_SIG_COUNT {
        return Err(CommitCodecError::EnvelopeDecode(format!(
            "signature count {sig_count} exceeds maximum {MAX_SIG_COUNT}"
        )));
    }

    let mut sigs = Vec::with_capacity(sig_count as usize);
    for _ in 0..sig_count {
        // signer_len
        let signer_len =
            u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap()) as usize;
        if signer_len > MAX_SIGNER_LEN {
            return Err(CommitCodecError::EnvelopeDecode(format!(
                "signer length {signer_len} exceeds maximum {MAX_SIGNER_LEN}"
            )));
        }

        // signer
        let signer_bytes = read_exact(data, &mut pos, signer_len)?;
        let signer = std::str::from_utf8(signer_bytes)
            .map_err(|e| CommitCodecError::EnvelopeDecode(format!("invalid signer UTF-8: {e}")))?;

        // algo (u8)
        let algo = read_u8(data, &mut pos)?;
        if algo != ALGO_ED25519 {
            return Err(CommitCodecError::EnvelopeDecode(format!(
                "unknown signature algorithm: 0x{algo:02x}"
            )));
        }

        // signature (64 bytes)
        let sig_slice = read_exact(data, &mut pos, 64)?;
        let mut signature = [0u8; 64];
        signature.copy_from_slice(sig_slice);

        // metadata (optional, length-prefixed)
        let meta_len =
            u16::from_le_bytes(read_exact(data, &mut pos, 2)?.try_into().unwrap()) as usize;
        let metadata = if meta_len > 0 {
            if meta_len > MAX_METADATA_LEN {
                return Err(CommitCodecError::EnvelopeDecode(format!(
                    "metadata length {meta_len} exceeds maximum {MAX_METADATA_LEN}"
                )));
            }
            let meta = read_exact(data, &mut pos, meta_len)?.to_vec();
            Some(meta)
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
            "signature block: consumed {} of {} bytes",
            pos,
            data.len()
        )));
    }

    Ok(sigs)
}

/// Compute the encoded size of a signature block.
pub fn sig_block_size(sigs: &[CommitSignature]) -> usize {
    let mut size = 2; // sig_count: u16
    for sig in sigs {
        size += 2; // signer_len: u16
        size += sig.signer.len();
        size += 1; // algo: u8
        size += 64; // signature
        size += 2; // meta_len: u16
        if let Some(meta) = &sig.metadata {
            size += meta.len();
        }
    }
    size
}

/// 32-byte fixed header.
#[derive(Debug, Clone)]
pub struct CommitHeader {
    pub version: u8,
    pub flags: u8,
    pub t: i64,
    pub op_count: u32,
    pub envelope_len: u32,
    /// Length of the signature block appended after the hash (0 if unsigned).
    pub sig_block_len: u16,
}

impl CommitHeader {
    /// Write the header into the first 32 bytes of `buf`.
    ///
    /// Wire layout (v3):
    /// `[0..4] magic, [4] version, [5] flags, [6..10] t: u32 LE,
    ///  [10..14] op_count: u32, [14..18] envelope_len: u32,
    ///  [18..20] sig_block_len: u16, [20..32] reserved (12 bytes)`
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= HEADER_LEN);
        debug_assert!(
            self.t >= 0 && self.t <= u32::MAX as i64,
            "t value {} out of u32 range for wire format",
            self.t
        );
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..10].copy_from_slice(&(self.t as u32).to_le_bytes());
        buf[10..14].copy_from_slice(&self.op_count.to_le_bytes());
        buf[14..18].copy_from_slice(&self.envelope_len.to_le_bytes());
        buf[18..20].copy_from_slice(&self.sig_block_len.to_le_bytes());
        // reserved bytes 20..32
        buf[20..32].fill(0);
    }

    /// Read the header from the first 32 bytes of `buf`.
    ///
    /// Wire layout (v3 and v4 share the same header):
    /// `[0..4] magic, [4] version, [5] flags, [6..10] t: u32 LE,
    ///  [10..14] op_count: u32, [14..18] envelope_len: u32,
    ///  [18..20] sig_block_len: u16, [20..32] reserved (12 bytes)`
    ///
    /// Accepts both v3 and v4 version bytes. Dispatch to the correct body
    /// reader based on `header.version` (see top-level `codec` module).
    /// Unknown versions return `UnsupportedVersion`.
    pub fn read_from(buf: &[u8]) -> Result<Self, CommitCodecError> {
        if buf.len() < HEADER_LEN {
            return Err(CommitCodecError::TooSmall {
                got: buf.len(),
                min: HEADER_LEN,
            });
        }
        if buf[0..4] != MAGIC {
            return Err(CommitCodecError::InvalidMagic);
        }
        let version = buf[4];
        if version != VERSION && version != VERSION_V3 {
            return Err(CommitCodecError::UnsupportedVersion(version));
        }
        let flags = buf[5];
        let raw_t = u32::from_le_bytes(buf[6..10].try_into().unwrap());
        let t = raw_t as i64;
        let op_count = u32::from_le_bytes(buf[10..14].try_into().unwrap());
        let envelope_len = u32::from_le_bytes(buf[14..18].try_into().unwrap());
        let sig_block_len = u16::from_le_bytes(buf[18..20].try_into().unwrap());

        Ok(Self {
            version,
            flags,
            t,
            op_count,
            envelope_len,
            sig_block_len,
        })
    }
}

// =============================================================================
// Footer
// =============================================================================

/// Dictionary location in the blob.
#[derive(Debug, Clone, Copy, Default)]
pub struct DictLocation {
    pub offset: u64,
    pub len: u32,
}

/// 64-byte fixed footer (does NOT include the trailing 32-byte hash).
#[derive(Debug, Clone)]
pub struct CommitFooter {
    /// Dictionary locations in order: graph, subject, predicate, datatype, object_ref.
    pub dicts: [DictLocation; 5],
    /// Length of the (possibly compressed) ops section in bytes.
    pub ops_section_len: u32,
}

impl CommitFooter {
    /// Write the footer into `buf` (must be >= FOOTER_LEN bytes).
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= FOOTER_LEN);
        let mut pos = 0;
        for d in &self.dicts {
            buf[pos..pos + 8].copy_from_slice(&d.offset.to_le_bytes());
            pos += 8;
            buf[pos..pos + 4].copy_from_slice(&d.len.to_le_bytes());
            pos += 4;
        }
        buf[pos..pos + 4].copy_from_slice(&self.ops_section_len.to_le_bytes());
    }

    /// Read the footer from `buf` (must be >= FOOTER_LEN bytes).
    pub fn read_from(buf: &[u8]) -> Result<Self, CommitCodecError> {
        if buf.len() < FOOTER_LEN {
            return Err(CommitCodecError::TooSmall {
                got: buf.len(),
                min: FOOTER_LEN,
            });
        }
        let mut pos = 0;
        let mut dicts = [DictLocation::default(); 5];
        for d in &mut dicts {
            d.offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
            pos += 8;
            d.len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
            pos += 4;
        }
        let ops_section_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());

        Ok(Self {
            dicts,
            ops_section_len,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let header = CommitHeader {
            version: VERSION,
            flags: FLAG_ZSTD,
            t: 42,
            op_count: 1000,
            envelope_len: 256,
            sig_block_len: 0,
        };
        let mut buf = [0u8; HEADER_LEN];
        header.write_to(&mut buf);

        let parsed = CommitHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.version, VERSION);
        assert_eq!(parsed.flags, FLAG_ZSTD);
        assert_eq!(parsed.t, 42);
        assert_eq!(parsed.op_count, 1000);
        assert_eq!(parsed.envelope_len, 256);
        assert_eq!(parsed.sig_block_len, 0);
    }

    #[test]
    fn test_header_with_sig_block_len() {
        let header = CommitHeader {
            version: VERSION,
            flags: FLAG_ZSTD | FLAG_HAS_COMMIT_SIG,
            t: 10,
            op_count: 50,
            envelope_len: 128,
            sig_block_len: 200,
        };
        let mut buf = [0u8; HEADER_LEN];
        header.write_to(&mut buf);

        let parsed = CommitHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.flags, FLAG_ZSTD | FLAG_HAS_COMMIT_SIG);
        assert_eq!(parsed.sig_block_len, 200);
    }

    #[test]
    fn test_header_bad_magic() {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(b"NOPE");
        assert!(matches!(
            CommitHeader::read_from(&buf),
            Err(CommitCodecError::InvalidMagic)
        ));
    }

    #[test]
    fn test_header_bad_version() {
        // Reject old version 2
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = 2;
        assert!(matches!(
            CommitHeader::read_from(&buf),
            Err(CommitCodecError::UnsupportedVersion(2))
        ));

        // Reject future version 99
        buf[4] = 99;
        assert!(matches!(
            CommitHeader::read_from(&buf),
            Err(CommitCodecError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn test_header_accepts_v3_and_v4() {
        // The header reader is version-agnostic across supported versions;
        // dispatch to the correct body reader is done by the codec module.
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&MAGIC);

        buf[4] = VERSION_V3;
        let parsed = CommitHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.version, VERSION_V3);

        buf[4] = VERSION;
        let parsed = CommitHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.version, VERSION);
    }

    #[test]
    fn test_footer_round_trip() {
        let footer = CommitFooter {
            dicts: [
                DictLocation {
                    offset: 100,
                    len: 50,
                },
                DictLocation {
                    offset: 150,
                    len: 200,
                },
                DictLocation {
                    offset: 350,
                    len: 100,
                },
                DictLocation {
                    offset: 450,
                    len: 80,
                },
                DictLocation {
                    offset: 530,
                    len: 120,
                },
            ],
            ops_section_len: 9999,
        };
        let mut buf = [0u8; FOOTER_LEN];
        footer.write_to(&mut buf);

        let parsed = CommitFooter::read_from(&buf).unwrap();
        assert_eq!(parsed.ops_section_len, 9999);
        for i in 0..5 {
            assert_eq!(parsed.dicts[i].offset, footer.dicts[i].offset);
            assert_eq!(parsed.dicts[i].len, footer.dicts[i].len);
        }
    }

    #[test]
    fn test_otag_round_trip() {
        for tag_byte in 0..=21u8 {
            let tag = OTag::from_u8(tag_byte).unwrap();
            assert_eq!(tag as u8, tag_byte);
        }
        assert!(OTag::from_u8(22).is_err());
        assert!(OTag::from_u8(255).is_err());
    }
}
