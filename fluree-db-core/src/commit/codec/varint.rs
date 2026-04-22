//! Binary read/write primitives: varints, zigzag encoding, and fixed-width reads.

use super::error::CommitCodecError;

/// Read exactly `n` bytes from `data` at `*pos`, advancing `*pos`.
///
/// Returns a slice into `data`. Returns `UnexpectedEof` if insufficient bytes.
#[inline]
pub fn read_exact<'a>(
    data: &'a [u8],
    pos: &mut usize,
    n: usize,
) -> Result<&'a [u8], CommitCodecError> {
    let end = pos.checked_add(n).ok_or(CommitCodecError::UnexpectedEof)?;
    if end > data.len() {
        return Err(CommitCodecError::UnexpectedEof);
    }
    let slice = &data[*pos..end];
    *pos = end;
    Ok(slice)
}

/// Read a single byte from `data` at `*pos`, advancing `*pos`.
#[inline]
pub fn read_u8(data: &[u8], pos: &mut usize) -> Result<u8, CommitCodecError> {
    if *pos >= data.len() {
        return Err(CommitCodecError::UnexpectedEof);
    }
    let byte = data[*pos];
    *pos += 1;
    Ok(byte)
}

/// Encode an unsigned 64-bit integer as LEB128 into `buf`.
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
pub fn decode_varint(buf: &[u8], pos: &mut usize) -> Result<u64, CommitCodecError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= buf.len() {
            return Err(CommitCodecError::UnexpectedEof);
        }
        let byte = buf[*pos];
        *pos += 1;

        let payload = (byte & 0x7F) as u64;
        // Prevent overflow: shift must be < 64, and the value must fit
        if shift >= 63 && payload > 1 {
            return Err(CommitCodecError::InvalidOp("varint overflow".into()));
        }
        result |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

/// Zigzag-encode a signed i64 into an unsigned u64.
/// Maps: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, ...
#[inline]
pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Zigzag-decode an unsigned u64 back to a signed i64.
#[inline]
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_varint(val: u64) {
        let mut buf = Vec::new();
        encode_varint(val, &mut buf);
        let mut pos = 0;
        let decoded = decode_varint(&buf, &mut pos).unwrap();
        assert_eq!(decoded, val);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_varint_zero() {
        round_trip_varint(0);
    }

    #[test]
    fn test_varint_one() {
        round_trip_varint(1);
    }

    #[test]
    fn test_varint_127() {
        let mut buf = Vec::new();
        encode_varint(127, &mut buf);
        assert_eq!(buf.len(), 1); // fits in single byte
        round_trip_varint(127);
    }

    #[test]
    fn test_varint_128() {
        let mut buf = Vec::new();
        encode_varint(128, &mut buf);
        assert_eq!(buf.len(), 2); // needs two bytes
        round_trip_varint(128);
    }

    #[test]
    fn test_varint_u32_max() {
        round_trip_varint(u32::MAX as u64);
    }

    #[test]
    fn test_varint_u64_max() {
        round_trip_varint(u64::MAX);
    }

    #[test]
    fn test_varint_various() {
        for val in [255, 256, 1000, 65535, 65536, 1_000_000, u64::MAX / 2] {
            round_trip_varint(val);
        }
    }

    #[test]
    fn test_varint_multiple_in_buffer() {
        let mut buf = Vec::new();
        encode_varint(100, &mut buf);
        encode_varint(200, &mut buf);
        encode_varint(300, &mut buf);

        let mut pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos).unwrap(), 100);
        assert_eq!(decode_varint(&buf, &mut pos).unwrap(), 200);
        assert_eq!(decode_varint(&buf, &mut pos).unwrap(), 300);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_varint_unexpected_eof() {
        let buf = [];
        let mut pos = 0;
        assert!(decode_varint(&buf, &mut pos).is_err());
    }

    fn round_trip_zigzag(val: i64) {
        let encoded = zigzag_encode(val);
        let decoded = zigzag_decode(encoded);
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_zigzag_zero() {
        assert_eq!(zigzag_encode(0), 0);
        round_trip_zigzag(0);
    }

    #[test]
    fn test_zigzag_positive() {
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(2), 4);
        round_trip_zigzag(1);
        round_trip_zigzag(2);
        round_trip_zigzag(100);
    }

    #[test]
    fn test_zigzag_negative() {
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(-2), 3);
        round_trip_zigzag(-1);
        round_trip_zigzag(-2);
        round_trip_zigzag(-100);
    }

    #[test]
    fn test_zigzag_extremes() {
        round_trip_zigzag(i64::MIN);
        round_trip_zigzag(i64::MAX);
    }
}
