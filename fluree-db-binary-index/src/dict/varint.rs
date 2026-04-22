//! LEB128 variable-length integer encoding for dict tree leaf formats.

use std::io;

/// Encode an unsigned 64-bit integer as LEB128 into `buf`.
#[inline]
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a LEB128 unsigned 64-bit integer from `buf` starting at `*pos`.
/// Advances `*pos` past the consumed bytes.
#[inline]
pub fn decode_varint(buf: &[u8], pos: &mut usize) -> io::Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "varint: unexpected end of buffer",
            ));
        }
        let byte = buf[*pos];
        *pos += 1;

        let payload = (byte & 0x7F) as u64;
        if shift >= 63 && payload > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
        result |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(val: u64) {
        let mut buf = Vec::new();
        encode_varint(val, &mut buf);
        let mut pos = 0;
        let decoded = decode_varint(&buf, &mut pos).unwrap();
        assert_eq!(decoded, val);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_round_trip_various() {
        for val in [
            0,
            1,
            127,
            128,
            255,
            256,
            65535,
            65536,
            u32::MAX as u64,
            u64::MAX,
        ] {
            round_trip(val);
        }
    }

    #[test]
    fn test_encoding_sizes() {
        let mut buf = Vec::new();
        encode_varint(127, &mut buf);
        assert_eq!(buf.len(), 1);

        buf.clear();
        encode_varint(128, &mut buf);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_eof_error() {
        let mut pos = 0;
        assert!(decode_varint(&[], &mut pos).is_err());
    }
}
