//! Segmented envelope (version 2): an object encrypted as fixed-size,
//! independently authenticated segments, so a reader can decrypt the part of
//! an object it needs without decrypting the rest.
//!
//! ## Layout
//!
//! ```text
//! offset  len  field
//!      0    4  magic "FLU\0"
//!      4    1  version = 0x02
//!      5    1  algorithm = 0x02 (AES-256-GCM, HKDF-SHA256 object key)
//!      6    4  key id, u32 LE (same offset as version 1)
//!     10    1  log2 of the plaintext segment size, 12..=24
//!     11    1  reserved, 0
//!     12    8  plaintext length, u64 LE
//!     20   32  salt, random per encryption
//!     52    7  nonce prefix, random per encryption
//!     59    5  reserved, 0
//!     64       segment 0 ‖ segment 1 ‖ … ‖ segment n-1
//! ```
//!
//! With `S` the segment size and `L` the plaintext length, there are
//! `n = max(1, ⌈L / S⌉)` segments. Segment `i` holds plaintext bytes
//! `[i·S, min((i+1)·S, L))` as their AES-GCM ciphertext followed by the
//! 16-byte tag, and starts at `64 + i·(S + 16)`. An empty object is one
//! segment of zero plaintext bytes: a tag alone. The envelope is exactly
//! `64 + 16·n + L` bytes; anything shorter is truncated and anything longer
//! is malformed.
//!
//! ## Keys and nonces
//!
//! Each envelope has its own AES-256 key:
//!
//! ```text
//! object_key = HKDF-SHA256(salt = salt, ikm = master key,
//!                          info = "fluree-db/envelope-v2" ‖ header[0..64], L = 32)
//! nonce_i    = nonce_prefix (7) ‖ i as u32 BE (4) ‖ last (1)
//! ```
//!
//! `last` is 1 for segment `n-1` and 0 otherwise. Segments carry no
//! associated data: the header is bound through the key, so changing any
//! header byte (length, segment size, key id, nonce prefix) changes the key
//! and fails every segment, and the nonce binds each segment to its position
//! and to whether it ends the object, so segments cannot be reordered,
//! dropped from the end or spliced between objects.
//!
//! This follows the construction of Tink's AES-GCM-HKDF streaming AEAD
//! (per-object key from a random salt, nonce = prefix ‖ index ‖ last flag),
//! with three differences: the header carries the plaintext length, so a
//! reader can map a range to segments without the ciphertext length; every
//! segment has the same size (Tink shortens the first by the header); and
//! the HKDF info is the header rather than caller associated data.
//!
//! As in version 1, the envelope is not bound to its address or content id:
//! copies between branches, backups and restores stay readable. Swapping one
//! whole valid envelope for another under the same key is not detected here;
//! content identity above this layer is what answers that, for full reads.
//!
//! ## Reads
//!
//! A full read verifies every segment and the exact length. A range read
//! verifies the segments it returns bytes from; it cannot vouch for the rest
//! of the object. A range that starts at or past the end still verifies the
//! final segment, so an end-of-object answer never rests on an
//! unauthenticated length.

use crate::envelope::{KEY_LEN, MAGIC, TAG_LEN};
use crate::error::{EncryptionError, Result};

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use std::ops::Range;

/// Envelope format version of a segmented envelope.
pub const VERSION: u8 = 0x02;

/// Algorithm id: AES-256-GCM segments under an HKDF-SHA256 object key.
pub const ALG_AES256_GCM_HKDF_SHA256: u8 = 0x02;

/// Header length of a segmented envelope.
pub const HEADER_LEN: usize = 64;

/// Smallest and largest supported segment sizes (log2): 4 KiB to 16 MiB.
pub const MIN_SEGMENT_LOG2: u8 = 12;
pub const MAX_SEGMENT_LOG2: u8 = 24;

/// Segment size new envelopes are written with.
pub const DEFAULT_SEGMENT_LOG2: u8 = 16;

const SALT_LEN: usize = 32;
const NONCE_PREFIX_LEN: usize = 7;
const INFO_PREFIX: &[u8] = b"fluree-db/envelope-v2";

const VERSION_OFFSET: usize = 4;
const ALG_OFFSET: usize = 5;
const KEY_ID_OFFSET: usize = 6;
const SEGMENT_LOG2_OFFSET: usize = 10;
const RESERVED_A_OFFSET: usize = 11;
const PLAINTEXT_LEN_OFFSET: usize = 12;
const SALT_OFFSET: usize = 20;
const NONCE_PREFIX_OFFSET: usize = SALT_OFFSET + SALT_LEN; // 52
const RESERVED_B_OFFSET: usize = NONCE_PREFIX_OFFSET + NONCE_PREFIX_LEN; // 59

/// The parsed header of a segmented envelope.
#[derive(Clone, PartialEq, Eq)]
pub struct Header {
    pub key_id: u32,
    segment_log2: u8,
    plaintext_len: u64,
    salt: [u8; SALT_LEN],
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
}

impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Header")
            .field("key_id", &self.key_id)
            .field("segment_size", &self.segment_size())
            .field("plaintext_len", &self.plaintext_len)
            .finish_non_exhaustive()
    }
}

impl Header {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[..4].copy_from_slice(MAGIC);
        out[VERSION_OFFSET] = VERSION;
        out[ALG_OFFSET] = ALG_AES256_GCM_HKDF_SHA256;
        out[KEY_ID_OFFSET..KEY_ID_OFFSET + 4].copy_from_slice(&self.key_id.to_le_bytes());
        out[SEGMENT_LOG2_OFFSET] = self.segment_log2;
        out[PLAINTEXT_LEN_OFFSET..PLAINTEXT_LEN_OFFSET + 8]
            .copy_from_slice(&self.plaintext_len.to_le_bytes());
        out[SALT_OFFSET..SALT_OFFSET + SALT_LEN].copy_from_slice(&self.salt);
        out[NONCE_PREFIX_OFFSET..NONCE_PREFIX_OFFSET + NONCE_PREFIX_LEN]
            .copy_from_slice(&self.nonce_prefix);
        out
    }

    /// Parse a segmented header from the first [`HEADER_LEN`] bytes of an
    /// envelope. Nothing here is authenticated yet: the fields are checked
    /// for shape, and the first segment decrypted under the derived key
    /// authenticates them.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(EncryptionError::truncated("segmented envelope header"));
        }
        if &bytes[..4] != MAGIC {
            return Err(EncryptionError::invalid_format(
                "not a Fluree encrypted object (magic mismatch)",
            ));
        }
        if bytes[VERSION_OFFSET] != VERSION {
            return Err(EncryptionError::invalid_format("not a segmented envelope"));
        }
        if bytes[ALG_OFFSET] != ALG_AES256_GCM_HKDF_SHA256 {
            return Err(EncryptionError::invalid_format(
                "unsupported encryption algorithm",
            ));
        }
        let segment_log2 = bytes[SEGMENT_LOG2_OFFSET];
        if !(MIN_SEGMENT_LOG2..=MAX_SEGMENT_LOG2).contains(&segment_log2) {
            return Err(EncryptionError::invalid_format("unsupported segment size"));
        }
        if bytes[RESERVED_A_OFFSET] != 0 || bytes[RESERVED_B_OFFSET..HEADER_LEN] != [0u8; 5] {
            return Err(EncryptionError::invalid_format(
                "reserved header bytes are not zero",
            ));
        }
        let header = Self {
            key_id: u32::from_le_bytes(read_array(bytes, KEY_ID_OFFSET)),
            segment_log2,
            plaintext_len: u64::from_le_bytes(read_array(bytes, PLAINTEXT_LEN_OFFSET)),
            salt: read_array(bytes, SALT_OFFSET),
            nonce_prefix: read_array(bytes, NONCE_PREFIX_OFFSET),
        };
        // Rejects lengths whose segment count or envelope size overflows.
        header.envelope_len()?;
        Ok(header)
    }

    /// Plaintext bytes per segment.
    pub fn segment_size(&self) -> u64 {
        1u64 << self.segment_log2
    }

    /// The object's plaintext length.
    pub fn plaintext_len(&self) -> u64 {
        self.plaintext_len
    }

    /// Number of segments: at least one, so an empty object has a tag.
    pub fn segment_count(&self) -> u64 {
        self.plaintext_len.div_ceil(self.segment_size()).max(1)
    }

    /// The exact envelope length this header declares.
    pub fn envelope_len(&self) -> Result<u64> {
        let n = self.segment_count();
        if n > u64::from(u32::MAX) + 1 {
            return Err(EncryptionError::invalid_format(
                "segment count exceeds the nonce's index space",
            ));
        }
        n.checked_mul(TAG_LEN as u64)
            .and_then(|tags| tags.checked_add(self.plaintext_len))
            .and_then(|body| body.checked_add(HEADER_LEN as u64))
            .ok_or_else(|| EncryptionError::invalid_format("envelope length overflows"))
    }

    fn segment_plain_len(&self, index: u64) -> u64 {
        let start = index * self.segment_size();
        self.segment_size().min(self.plaintext_len - start)
    }

    fn segment_offset(&self, index: u64) -> u64 {
        HEADER_LEN as u64 + index * (self.segment_size() + TAG_LEN as u64)
    }

    /// The ciphertext bytes holding segments `first..=last`.
    fn ciphertext_span(&self, first: u64, last: u64) -> Range<u64> {
        let end = self.segment_offset(last) + self.segment_plain_len(last) + TAG_LEN as u64;
        self.segment_offset(first)..end
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("caller checked the length")
}

/// A segmented envelope's object key, derived from its header and the
/// master key it names.
pub struct ObjectKey {
    cipher: Aes256Gcm,
    header: Header,
}

impl std::fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectKey")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

impl ObjectKey {
    /// Derive the object key for `header` from `master`.
    pub fn derive(master: &[u8; KEY_LEN], header: Header) -> Self {
        let encoded = header.encode();
        let mut info = Vec::with_capacity(INFO_PREFIX.len() + HEADER_LEN);
        info.extend_from_slice(INFO_PREFIX);
        info.extend_from_slice(&encoded);
        let mut okm = zeroize::Zeroizing::new([0u8; KEY_LEN]);
        Hkdf::<Sha256>::new(Some(&header.salt), master)
            .expand(&info, okm.as_mut())
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        let cipher = Aes256Gcm::new_from_slice(okm.as_ref()).expect("key is 32 bytes");
        Self { cipher, header }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    fn nonce(&self, index: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..NONCE_PREFIX_LEN].copy_from_slice(&self.header.nonce_prefix);
        let index32 = u32::try_from(index).expect("segment count checked at parse");
        nonce[NONCE_PREFIX_LEN..11].copy_from_slice(&index32.to_be_bytes());
        nonce[11] = u8::from(index + 1 == self.header.segment_count());
        nonce
    }

    /// Decrypt the segments starting at `first` whose ciphertext is exactly
    /// `ciphertext`, returning their plaintext. Nothing is returned for a
    /// segment that fails to authenticate.
    fn decrypt_segments(&self, first: u64, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(ciphertext.len());
        let mut pos = 0usize;
        let mut index = first;
        while pos < ciphertext.len() {
            if index >= self.header.segment_count() {
                return Err(EncryptionError::invalid_format(
                    "trailing bytes after the final segment",
                ));
            }
            let plain_len = self.header.segment_plain_len(index) as usize;
            let seg_len = plain_len + TAG_LEN;
            if ciphertext.len() - pos < seg_len {
                return Err(EncryptionError::truncated("segment"));
            }
            let start = out.len();
            out.extend_from_slice(&ciphertext[pos..pos + plain_len]);
            let tag = Tag::from_slice(&ciphertext[pos + plain_len..pos + seg_len]);
            if self
                .cipher
                .decrypt_in_place_detached(
                    Nonce::from_slice(&self.nonce(index)),
                    b"",
                    &mut out[start..],
                    tag,
                )
                .is_err()
            {
                // The buffer now holds unauthenticated plaintext; never
                // hand it out.
                out.fill(0);
                return Err(EncryptionError::decrypt_failed(
                    "segment failed to authenticate: wrong key or data corrupted/tampered",
                ));
            }
            crate::stats::segment_decrypted(plain_len);
            pos += seg_len;
            index += 1;
        }
        Ok(out)
    }

    /// The plaintext of `range` from `envelope`, the whole envelope's bytes.
    /// Clamped to the object; an empty range reads nothing, and a range
    /// starting at or past the end verifies the final segment and returns
    /// nothing.
    pub fn read_range(&self, envelope: &[u8], range: Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let len = self.header.plaintext_len;
        let (start, end) = (range.start.min(len), range.end.min(len));
        let (first, last) = if start >= end {
            let final_segment = self.header.segment_count() - 1;
            (final_segment, final_segment)
        } else {
            let size = self.header.segment_size();
            (start / size, (end - 1) / size)
        };
        let span = self.header.ciphertext_span(first, last);
        let ciphertext = envelope
            .get(span.start as usize..span.end as usize)
            .ok_or_else(|| EncryptionError::truncated("segmented envelope"))?;
        let mut plain = self.decrypt_segments(first, ciphertext)?;
        if start >= end {
            return Ok(Vec::new());
        }
        let skip = (start - first * self.header.segment_size()) as usize;
        plain.drain(..skip);
        plain.truncate((end - start) as usize);
        Ok(plain)
    }

    /// The whole plaintext of `envelope`, verifying every segment and the
    /// exact envelope length.
    pub fn decrypt(&self, envelope: &[u8]) -> Result<Vec<u8>> {
        check_envelope_len(&self.header, envelope.len() as u64)?;
        crate::stats::whole_object_decrypted(self.header.plaintext_len as usize);
        self.decrypt_segments(0, &envelope[HEADER_LEN..])
    }
}

/// Check an envelope of `actual` bytes against the length its header
/// declares, telling truncation apart from trailing bytes.
pub fn check_envelope_len(header: &Header, actual: u64) -> Result<()> {
    let expected = header.envelope_len()?;
    match actual.cmp(&expected) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Less => Err(EncryptionError::truncated("segmented envelope")),
        std::cmp::Ordering::Greater => Err(EncryptionError::invalid_format(
            "trailing bytes after the final segment",
        )),
    }
}

/// Encrypt `plaintext` as a segmented envelope under `master`, with fresh
/// salt and nonce prefix.
pub fn encrypt(
    master: &[u8; KEY_LEN],
    key_id: u32,
    segment_log2: u8,
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    use rand_core::{OsRng, RngCore};
    let mut salt = [0u8; SALT_LEN];
    let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce_prefix);
    encrypt_with_material(master, key_id, segment_log2, salt, nonce_prefix, plaintext)
}

fn encrypt_with_material(
    master: &[u8; KEY_LEN],
    key_id: u32,
    segment_log2: u8,
    salt: [u8; SALT_LEN],
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    if !(MIN_SEGMENT_LOG2..=MAX_SEGMENT_LOG2).contains(&segment_log2) {
        return Err(EncryptionError::encrypt_failed("unsupported segment size"));
    }
    let header = Header {
        key_id,
        segment_log2,
        plaintext_len: plaintext.len() as u64,
        salt,
        nonce_prefix,
    };
    let envelope_len = header
        .envelope_len()
        .map_err(|_| EncryptionError::encrypt_failed("object too large to encrypt"))?;
    let key = ObjectKey::derive(master, header);
    let mut out = Vec::with_capacity(envelope_len as usize);
    out.extend_from_slice(&key.header.encode());
    let size = key.header.segment_size() as usize;
    for index in 0..key.header.segment_count() {
        let start = index as usize * size;
        let chunk = &plaintext[start..(start + size).min(plaintext.len())];
        let seg_start = out.len();
        out.extend_from_slice(chunk);
        let tag = key
            .cipher
            .encrypt_in_place_detached(
                Nonce::from_slice(&key.nonce(index)),
                b"",
                &mut out[seg_start..],
            )
            .map_err(|_| EncryptionError::encrypt_failed("AES-GCM encryption failed"))?;
        out.extend_from_slice(&tag);
    }
    debug_assert_eq!(out.len() as u64, envelope_len);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::Aead;

    const MASTER: [u8; KEY_LEN] = [0x42; KEY_LEN];
    const SALT: [u8; SALT_LEN] = [0x5a; SALT_LEN];
    const PREFIX: [u8; NONCE_PREFIX_LEN] = [1, 2, 3, 4, 5, 6, 7];

    fn plaintext(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    fn sealed(len: usize) -> (Vec<u8>, Vec<u8>) {
        let plain = plaintext(len);
        let envelope =
            encrypt_with_material(&MASTER, 7, MIN_SEGMENT_LOG2, SALT, PREFIX, &plain).unwrap();
        (plain, envelope)
    }

    fn key_for(envelope: &[u8]) -> ObjectKey {
        ObjectKey::derive(&MASTER, Header::parse(envelope).unwrap())
    }

    /// The construction spelled out with the high-level AEAD API, one
    /// segment at a time, so the layout above is checked against the
    /// description rather than against itself.
    fn reference_envelope(plain: &[u8], segment_log2: u8) -> Vec<u8> {
        let size = 1usize << segment_log2;
        let n = plain.len().div_ceil(size).max(1);
        let mut header = [0u8; HEADER_LEN];
        header[..4].copy_from_slice(b"FLU\0");
        header[4] = 2;
        header[5] = 2;
        header[6..10].copy_from_slice(&7u32.to_le_bytes());
        header[10] = segment_log2;
        header[12..20].copy_from_slice(&(plain.len() as u64).to_le_bytes());
        header[20..52].copy_from_slice(&SALT);
        header[52..59].copy_from_slice(&PREFIX);
        let mut info = b"fluree-db/envelope-v2".to_vec();
        info.extend_from_slice(&header);
        let mut okm = [0u8; 32];
        Hkdf::<Sha256>::new(Some(&SALT), &MASTER)
            .expand(&info, &mut okm)
            .unwrap();
        let cipher = Aes256Gcm::new_from_slice(&okm).unwrap();
        let mut out = header.to_vec();
        for i in 0..n {
            let mut nonce = [0u8; 12];
            nonce[..7].copy_from_slice(&PREFIX);
            nonce[7..11].copy_from_slice(&(i as u32).to_be_bytes());
            nonce[11] = u8::from(i + 1 == n);
            let chunk = &plain[i * size..((i + 1) * size).min(plain.len())];
            out.extend(cipher.encrypt(Nonce::from_slice(&nonce), chunk).unwrap());
        }
        out
    }

    #[test]
    fn envelope_matches_the_reference_construction() {
        for len in [0, 1, 4095, 4096, 4097, 3 * 4096 + 17] {
            let (plain, envelope) = sealed(len);
            assert_eq!(
                envelope,
                reference_envelope(&plain, MIN_SEGMENT_LOG2),
                "len {len}"
            );
            let n = len.div_ceil(4096).max(1);
            assert_eq!(envelope.len(), HEADER_LEN + n * TAG_LEN + len);
        }
    }

    /// Fixed vectors, computed by an independent implementation of the
    /// layout above (Python `cryptography`: HKDF-SHA256 and AES-GCM). A
    /// change to the format, the key derivation or the nonce layout shows up
    /// here, not as unreadable data in the field.
    #[test]
    fn fixed_vectors() {
        let (_, empty) = sealed(0);
        assert_eq!(
            hex::encode(&empty),
            "464c55000202070000000c0000000000000000005a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a\
             5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a01020304050607000000000086\
             6fd4e33f7e16ce1d61552148a00922"
        );
        let (_, three) = sealed(2 * 4096 + 5);
        assert_eq!(
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&three)),
            "60dfd052d67887803eda38662010d801f4abdb022c578a33fd6c0ff6caab071c"
        );
    }

    #[test]
    fn full_and_ranged_reads_agree() {
        let (plain, envelope) = sealed(3 * 4096 + 17);
        let key = key_for(&envelope);
        assert_eq!(key.decrypt(&envelope).unwrap(), plain);
        let len = plain.len() as u64;
        for range in [
            0..1,
            0..4096,
            4095..4097,
            4096..8192,
            100..len,
            len - 1..len,
            5000..u64::MAX,
            0..len + 10,
        ] {
            let end = range.end.min(len) as usize;
            assert_eq!(
                key.read_range(&envelope, range.clone()).unwrap(),
                plain[range.start as usize..end],
                "{range:?}"
            );
        }
        assert!(key.read_range(&envelope, 10..10).unwrap().is_empty());
        #[allow(clippy::reversed_empty_ranges)]
        let reversed = 20..10;
        assert!(key.read_range(&envelope, reversed).unwrap().is_empty());
        assert!(key.read_range(&envelope, len..len + 5).unwrap().is_empty());
    }

    #[test]
    fn a_range_reads_only_the_segments_it_touches() {
        let (plain, mut envelope) = sealed(3 * 4096);
        // Corrupt segment 2; segment 0 still reads.
        let seg2 = HEADER_LEN + 2 * (4096 + TAG_LEN) + 3;
        envelope[seg2] ^= 1;
        let key = key_for(&envelope);
        assert_eq!(
            key.read_range(&envelope, 10..20).unwrap(),
            plain[10..20],
            "an untouched segment's corruption does not fail the read"
        );
        assert!(key.read_range(&envelope, 8190..8200).is_err());
        assert!(key.decrypt(&envelope).is_err(), "a full read sees it");
    }

    #[test]
    fn past_the_end_authenticates_the_final_segment() {
        let (_, mut envelope) = sealed(4096 + 10);
        let last = envelope.len() - 1;
        envelope[last] ^= 1;
        let key = key_for(&envelope);
        assert!(key.read_range(&envelope, 0..10).is_ok());
        assert!(
            key.read_range(&envelope, 5000..6000).is_err(),
            "an end-of-object answer verifies the final segment"
        );
    }

    #[test]
    fn tampering_is_detected() {
        let (_, envelope) = sealed(2 * 4096 + 5);
        let check = |envelope: &[u8], what: &str| {
            let failed = match Header::parse(envelope) {
                Err(_) => true,
                Ok(header) => ObjectKey::derive(&MASTER, header)
                    .decrypt(envelope)
                    .is_err(),
            };
            assert!(failed, "{what} went undetected");
        };
        // Every header byte is bound: through parsing or through the key.
        for i in 0..HEADER_LEN {
            let mut bad = envelope.clone();
            bad[i] ^= 0x01;
            check(&bad, &format!("header byte {i}"));
        }
        let seg = 4096 + TAG_LEN;
        let mut swapped = envelope.clone();
        let (a, b) = (HEADER_LEN, HEADER_LEN + seg);
        let first: Vec<u8> = envelope[a..a + seg].to_vec();
        swapped.copy_within(b..b + seg, a);
        swapped[b..b + seg].copy_from_slice(&first);
        check(&swapped, "reordered segments");

        let mut dup = envelope.clone();
        dup.copy_within(a..a + seg, b);
        check(&dup, "duplicated segment");

        check(&envelope[..envelope.len() - 1], "truncation");
        let mut trailing = envelope.clone();
        trailing.push(0);
        check(&trailing, "trailing data");

        // Dropping the final segment and relabelling the length fails: the
        // new last segment was sealed as not-last, and the length is bound.
        let mut short = envelope[..HEADER_LEN + 2 * seg].to_vec();
        short[PLAINTEXT_LEN_OFFSET..PLAINTEXT_LEN_OFFSET + 8]
            .copy_from_slice(&(2 * 4096u64).to_le_bytes());
        check(&short, "a dropped final segment");

        // A segment from another envelope of the same key does not splice.
        let other = encrypt_with_material(
            &MASTER,
            7,
            MIN_SEGMENT_LOG2,
            [9; SALT_LEN],
            PREFIX,
            &plaintext(2 * 4096 + 5),
        )
        .unwrap();
        let mut spliced = envelope.clone();
        spliced[a..a + seg].copy_from_slice(&other[a..a + seg]);
        check(&spliced, "a segment spliced from another object");
    }

    #[test]
    fn wrong_key_fails_and_returns_nothing() {
        let (_, envelope) = sealed(100);
        let key = ObjectKey::derive(&[0x43; KEY_LEN], Header::parse(&envelope).unwrap());
        assert!(matches!(
            key.decrypt(&envelope),
            Err(EncryptionError::DecryptFailed { .. })
        ));
        assert!(key.read_range(&envelope, 0..10).is_err());
    }

    #[test]
    fn truncation_and_trailing_bytes_are_told_apart() {
        let (_, envelope) = sealed(5000);
        let header = Header::parse(&envelope).unwrap();
        let len = envelope.len() as u64;
        assert!(check_envelope_len(&header, len).is_ok());
        assert!(matches!(
            check_envelope_len(&header, len - 1),
            Err(EncryptionError::Truncated { .. })
        ));
        assert!(matches!(
            check_envelope_len(&header, len + 1),
            Err(EncryptionError::InvalidFormat { .. })
        ));
        let key = key_for(&envelope);
        assert!(matches!(
            key.read_range(&envelope[..envelope.len() - 3], 4096..4100),
            Err(EncryptionError::Truncated { .. })
        ));
    }

    #[test]
    fn hostile_headers_are_rejected_before_any_allocation() {
        let (_, envelope) = sealed(10);
        let mut huge = envelope.clone();
        huge[PLAINTEXT_LEN_OFFSET..PLAINTEXT_LEN_OFFSET + 8]
            .copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(Header::parse(&huge).is_err(), "an overflowing length");

        let mut past_index_space = envelope.clone();
        past_index_space[PLAINTEXT_LEN_OFFSET..PLAINTEXT_LEN_OFFSET + 8]
            .copy_from_slice(&(1u64 << 50).to_le_bytes());
        assert!(
            Header::parse(&past_index_space).is_err(),
            "more segments than the nonce can number"
        );

        let mut long = envelope.clone();
        long[PLAINTEXT_LEN_OFFSET..PLAINTEXT_LEN_OFFSET + 8]
            .copy_from_slice(&(1u64 << 40).to_le_bytes());
        let header = Header::parse(&long).unwrap();
        assert!(
            check_envelope_len(&header, long.len() as u64).is_err(),
            "a length the bytes do not back"
        );
        let key = ObjectKey::derive(&MASTER, header);
        assert!(key.read_range(&long, 0..u64::MAX).is_err());

        for log2 in [0, MIN_SEGMENT_LOG2 - 1, MAX_SEGMENT_LOG2 + 1, 255] {
            let mut bad = envelope.clone();
            bad[SEGMENT_LOG2_OFFSET] = log2;
            assert!(Header::parse(&bad).is_err(), "segment log2 {log2}");
        }
        assert!(Header::parse(&envelope[..HEADER_LEN - 1]).is_err());
    }

    #[test]
    fn fresh_material_per_encryption() {
        let plain = plaintext(100);
        let a = encrypt(&MASTER, 1, DEFAULT_SEGMENT_LOG2, &plain).unwrap();
        let b = encrypt(&MASTER, 1, DEFAULT_SEGMENT_LOG2, &plain).unwrap();
        assert_ne!(a[SALT_OFFSET..HEADER_LEN], b[SALT_OFFSET..HEADER_LEN]);
        assert_ne!(a, b);
        assert_eq!(key_for(&a).decrypt(&a).unwrap(), plain);
        assert_eq!(key_for(&b).decrypt(&b).unwrap(), plain);
    }
}
