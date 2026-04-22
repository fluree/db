//! Column block encoder/decoder for the V3 (FLI3) leaflet format.
//!
//! Each column in a leaflet is an independently compressed block. The block
//! metadata (`ColumnBlockRef`) lives in the leaf-level leaflet directory —
//! there is no per-leaflet local directory.

use std::io;

/// Column identifier. Determines the semantic meaning and element type
/// of a column block within a leaflet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ColumnId {
    SId = 0,
    PId = 1,
    OType = 2,
    OKey = 3,
    OI = 4,
    T = 5,
}

impl ColumnId {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::SId),
            1 => Some(Self::PId),
            2 => Some(Self::OType),
            3 => Some(Self::OKey),
            4 => Some(Self::OI),
            5 => Some(Self::T),
            _ => None,
        }
    }

    pub fn to_u16(self) -> u16 {
        self as u16
    }
}

/// Compression codec identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    /// Zstandard compression.
    Zstd = 1,
}

/// Per-column block reference stored in the leaf-level leaflet directory.
///
/// Contains everything needed to locate and decompress a single column
/// block without reading any leaflet-local headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnBlockRef {
    /// Which column this block contains.
    pub col_id: u16,
    /// Compression codec (currently always Zstd).
    pub codec: u8,
    /// Element width in bytes (1, 2, 4, or 8).
    pub elem_width: u8,
    /// Byte offset of the compressed block relative to the leaflet payload start.
    pub offset: u32,
    /// Compressed size in bytes.
    pub compressed_len: u32,
    /// Uncompressed size in bytes.
    pub uncompressed_len: u32,
}

/// Wire size of a serialized `ColumnBlockRef`.
pub const COLUMN_BLOCK_REF_SIZE: usize = 16;

impl ColumnBlockRef {
    /// Serialize to wire format (16 bytes, little-endian).
    ///
    /// ```text
    /// col_id:           u16  [0..2]
    /// codec:            u8   [2]
    /// elem_width:       u8   [3]
    /// offset:           u32  [4..8]
    /// compressed_len:   u32  [8..12]
    /// uncompressed_len: u32  [12..16]
    /// ```
    pub fn write_le(&self, buf: &mut [u8; COLUMN_BLOCK_REF_SIZE]) {
        buf[0..2].copy_from_slice(&self.col_id.to_le_bytes());
        buf[2] = self.codec;
        buf[3] = self.elem_width;
        buf[4..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
    }

    /// Deserialize from wire format (16 bytes, little-endian).
    ///
    /// All `try_into().unwrap()` calls are safe: `buf` is exactly 16 bytes,
    /// so each fixed-width sub-slice is guaranteed to be the correct length.
    pub fn read_le(buf: &[u8; COLUMN_BLOCK_REF_SIZE]) -> Self {
        Self {
            col_id: u16::from_le_bytes(buf[0..2].try_into().unwrap()),
            codec: buf[2],
            elem_width: buf[3],
            offset: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        }
    }

    /// Number of elements in this column block.
    #[inline]
    pub fn element_count(&self) -> usize {
        if self.elem_width == 0 {
            0
        } else {
            self.uncompressed_len as usize / self.elem_width as usize
        }
    }
}

// ============================================================================
// Encoding
// ============================================================================

/// Encode a column of `u64` values into a compressed block.
///
/// Returns `(compressed_bytes, ref_metadata)`. The caller sets `ref.offset`
/// after positioning the block in the leaflet payload.
pub fn encode_column_u64(
    col_id: ColumnId,
    values: &[u64],
    zstd_level: i32,
) -> std::io::Result<(Vec<u8>, ColumnBlockRef)> {
    let raw = u64_slice_to_le_bytes(values);
    let compressed = zstd::bulk::compress(&raw, zstd_level)
        .map_err(|e| std::io::Error::other(format!("zstd compress u64 column: {e}")))?;
    let r = ColumnBlockRef {
        col_id: col_id.to_u16(),
        codec: Codec::Zstd as u8,
        elem_width: 8,
        offset: 0, // set by caller
        compressed_len: compressed.len() as u32,
        uncompressed_len: raw.len() as u32,
    };
    Ok((compressed, r))
}

/// Encode a column of `u32` values into a compressed block.
pub fn encode_column_u32(
    col_id: ColumnId,
    values: &[u32],
    zstd_level: i32,
) -> std::io::Result<(Vec<u8>, ColumnBlockRef)> {
    let raw = u32_slice_to_le_bytes(values);
    let compressed = zstd::bulk::compress(&raw, zstd_level)
        .map_err(|e| std::io::Error::other(format!("zstd compress u32 column: {e}")))?;
    let r = ColumnBlockRef {
        col_id: col_id.to_u16(),
        codec: Codec::Zstd as u8,
        elem_width: 4,
        offset: 0,
        compressed_len: compressed.len() as u32,
        uncompressed_len: raw.len() as u32,
    };
    Ok((compressed, r))
}

/// Encode a column of `u16` values into a compressed block.
pub fn encode_column_u16(
    col_id: ColumnId,
    values: &[u16],
    zstd_level: i32,
) -> std::io::Result<(Vec<u8>, ColumnBlockRef)> {
    let raw = u16_slice_to_le_bytes(values);
    let compressed = zstd::bulk::compress(&raw, zstd_level)
        .map_err(|e| std::io::Error::other(format!("zstd compress u16 column: {e}")))?;
    let r = ColumnBlockRef {
        col_id: col_id.to_u16(),
        codec: Codec::Zstd as u8,
        elem_width: 2,
        offset: 0,
        compressed_len: compressed.len() as u32,
        uncompressed_len: raw.len() as u32,
    };
    Ok((compressed, r))
}

// ============================================================================
// Decoding (minimal, for test validation)
// ============================================================================

/// Decode a compressed column block of `u64` values.
pub fn decode_column_u64(data: &[u8], block_ref: &ColumnBlockRef) -> io::Result<Vec<u64>> {
    let start = block_ref.offset as usize;
    let end = start + block_ref.compressed_len as usize;
    if end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "column block extends beyond data",
        ));
    }
    let decompressed =
        zstd::bulk::decompress(&data[start..end], block_ref.uncompressed_len as usize)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(le_bytes_to_u64_vec(&decompressed))
}

/// Decode a compressed column block of `u32` values.
pub fn decode_column_u32(data: &[u8], block_ref: &ColumnBlockRef) -> io::Result<Vec<u32>> {
    let start = block_ref.offset as usize;
    let end = start + block_ref.compressed_len as usize;
    if end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "column block extends beyond data",
        ));
    }
    let decompressed =
        zstd::bulk::decompress(&data[start..end], block_ref.uncompressed_len as usize)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(le_bytes_to_u32_vec(&decompressed))
}

/// Decode a compressed column block of `u16` values.
pub fn decode_column_u16(data: &[u8], block_ref: &ColumnBlockRef) -> io::Result<Vec<u16>> {
    let start = block_ref.offset as usize;
    let end = start + block_ref.compressed_len as usize;
    if end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "column block extends beyond data",
        ));
    }
    let decompressed =
        zstd::bulk::decompress(&data[start..end], block_ref.uncompressed_len as usize)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(le_bytes_to_u16_vec(&decompressed))
}

// ============================================================================
// Byte conversion helpers (little-endian, zero-copy where possible)
// ============================================================================

#[inline]
fn u64_slice_to_le_bytes(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    for &v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

#[inline]
fn u32_slice_to_le_bytes(values: &[u32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 4);
    for &v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

#[inline]
fn u16_slice_to_le_bytes(values: &[u16]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 2);
    for &v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

#[inline]
fn le_bytes_to_u64_vec(data: &[u8]) -> Vec<u64> {
    data.chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

#[inline]
fn le_bytes_to_u32_vec(data: &[u8]) -> Vec<u32> {
    data.chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

#[inline]
fn le_bytes_to_u16_vec(data: &[u8]) -> Vec<u16> {
    data.chunks_exact(2)
        .map(|c| u16::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_block_ref_roundtrip() {
        let r = ColumnBlockRef {
            col_id: ColumnId::OKey.to_u16(),
            codec: Codec::Zstd as u8,
            elem_width: 8,
            offset: 1024,
            compressed_len: 500,
            uncompressed_len: 2000,
        };
        let mut buf = [0u8; COLUMN_BLOCK_REF_SIZE];
        r.write_le(&mut buf);
        let r2 = ColumnBlockRef::read_le(&buf);
        assert_eq!(r, r2);
    }

    #[test]
    fn encode_decode_u64() {
        let values: Vec<u64> = (0..100).collect();
        let (compressed, mut block_ref) = encode_column_u64(ColumnId::SId, &values, 1).unwrap();
        block_ref.offset = 0;
        let decoded = decode_column_u64(&compressed, &block_ref).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn encode_decode_u32() {
        let values: Vec<u32> = (0..100).collect();
        let (compressed, mut block_ref) = encode_column_u32(ColumnId::PId, &values, 1).unwrap();
        block_ref.offset = 0;
        let decoded = decode_column_u32(&compressed, &block_ref).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn encode_decode_u16() {
        let values: Vec<u16> = (0..100).collect();
        let (compressed, mut block_ref) = encode_column_u16(ColumnId::OType, &values, 1).unwrap();
        block_ref.offset = 0;
        let decoded = decode_column_u16(&compressed, &block_ref).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn element_count() {
        let values: Vec<u64> = vec![1, 2, 3, 4, 5];
        let (_, block_ref) = encode_column_u64(ColumnId::OKey, &values, 1).unwrap();
        assert_eq!(block_ref.element_count(), 5);
    }

    #[test]
    fn encode_decode_with_offset() {
        // Simulate block at a non-zero offset within a larger buffer.
        let values: Vec<u32> = vec![10, 20, 30];
        let (compressed, mut block_ref) = encode_column_u32(ColumnId::T, &values, 1).unwrap();

        let prefix = vec![0u8; 64]; // some preceding data
        let mut full_buf = prefix;
        block_ref.offset = full_buf.len() as u32;
        full_buf.extend_from_slice(&compressed);

        let decoded = decode_column_u32(&full_buf, &block_ref).unwrap();
        assert_eq!(decoded, values);
    }
}
