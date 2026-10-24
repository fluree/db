//! FIR6 root framing: the 24-byte header stays readable, the body after it is
//! one zstd frame. Per-graph stats make up most of a root with many graphs and
//! compress well, so this is what a remote load fetches.
//!
//! Both root parsers (`IndexRoot::decode` and the metadata decode in
//! [`crate::db`]) inflate first and then read the uncompressed layout, which is
//! also what roots written before compression hold.

use std::borrow::Cow;
use std::io;

/// Fixed header: magic, version, flags, extended flags, pad, `index_t`, `base_t`.
pub const HEADER_LEN: usize = 24;

/// Byte offset of the extended-flags byte.
const FLAGS_EXT_AT: usize = 6;

/// Extended flag: the body after the header is one zstd frame.
pub const FLAG_EXT_COMPRESSED: u8 = 1 << 5;

const LEVEL: i32 = 3;

/// Compress an encoded root's body and mark it compressed.
pub fn deflate(raw: &[u8]) -> io::Result<Vec<u8>> {
    if raw.len() < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FIR6: too short to frame",
        ));
    }
    let body = zstd::bulk::compress(&raw[HEADER_LEN..], LEVEL)?;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&raw[..HEADER_LEN]);
    out[FLAGS_EXT_AT] |= FLAG_EXT_COMPRESSED;
    out.extend_from_slice(&body);
    Ok(out)
}

/// The uncompressed encoding of a root: a compressed root inflated (its flag
/// cleared), an uncompressed one as given.
pub fn inflate(bytes: &[u8]) -> io::Result<Cow<'_, [u8]>> {
    if bytes.len() < HEADER_LEN || bytes[FLAGS_EXT_AT] & FLAG_EXT_COMPRESSED == 0 {
        return Ok(Cow::Borrowed(bytes));
    }
    let body = zstd::stream::decode_all(&bytes[HEADER_LEN..])?;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&bytes[..HEADER_LEN]);
    out[FLAGS_EXT_AT] &= !FLAG_EXT_COMPRESSED;
    out.extend_from_slice(&body);
    Ok(Cow::Owned(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_root() -> Vec<u8> {
        let mut raw = b"FIR6".to_vec();
        raw.extend_from_slice(&[3, 0, 0b10, 0]);
        raw.extend_from_slice(&7i64.to_le_bytes());
        raw.extend_from_slice(&1i64.to_le_bytes());
        raw.extend(std::iter::repeat_n(b"graph-stats".as_slice(), 200).flatten());
        raw
    }

    #[test]
    fn deflate_keeps_the_header_and_inflate_restores_the_root() {
        let raw = raw_root();
        let framed = deflate(&raw).unwrap();
        assert!(framed.len() < raw.len() / 10);
        assert_eq!(framed[..6], raw[..6]);
        assert_eq!(framed[FLAGS_EXT_AT], 0b10 | FLAG_EXT_COMPRESSED);
        assert_eq!(framed[8..HEADER_LEN], raw[8..HEADER_LEN]);
        assert_eq!(&*inflate(&framed).unwrap(), raw.as_slice());
    }

    #[test]
    fn an_uncompressed_root_inflates_to_itself() {
        let raw = raw_root();
        assert!(matches!(inflate(&raw).unwrap(), Cow::Borrowed(b) if b == raw.as_slice()));
    }
}
