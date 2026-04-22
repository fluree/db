//! V2 streaming run reader — buffered record reading for k-way merge.
//!
//! Reads V2 run files (`FRN2`) and implements the `MergeSource` trait
//! for use with the generic merge engine.

use super::run_file::{RunFileHeader, RUN_V2_HEADER_LEN, RUN_V2_VERSION_WITH_OP};
use fluree_db_binary_index::format::run_record_v2::{
    RunRecordV2, RECORD_V2_WIRE_SIZE, RECORD_V2_WITH_OP_WIRE_SIZE,
};
use std::io::{self, BufReader, Read};
use std::path::Path;

/// Records per buffer fill (~960 KB at 30 bytes/record).
const BUFFER_SIZE: usize = 32_768;

/// File read buffer size.
const FILE_BUF_BYTES: usize = 1024 * 1024;

/// Flag for zstd-compressed blocks.
const RUN_FLAG_ZSTD_BLOCKS: u8 = 1 << 0;

// ============================================================================
// MergeSource trait
// ============================================================================

/// Trait for merge sources producing `RunRecordV2` values.
///
/// Parallel to `MergeSource` (V1) but typed for `RunRecordV2`.
pub trait MergeSource {
    fn peek(&self) -> Option<&RunRecordV2>;
    fn advance(&mut self) -> io::Result<()>;
    fn is_exhausted(&self) -> bool;

    /// Return the op byte for the current record.
    ///
    /// Default is `1` (assert) — correct for import-path readers that carry
    /// no op information. The `StreamingRunReaderWithOp` override returns
    /// the actual op byte from the wire format.
    fn peek_op(&self) -> u8 {
        1
    }
}

// ============================================================================
// StreamingRunReader
// ============================================================================

/// Buffered reader for V2 run files.
///
/// Auto-detects FRN2 file version: version 1 (no-op, 30-byte records) or
/// version 2 (with-op, 31-byte records). When ops are present, they're
/// returned via `peek_op()` in the `MergeSource` impl.
pub struct StreamingRunReader {
    file: BufReader<std::fs::File>,
    /// Kept for the sort_order accessor.
    pub header: RunFileHeader,
    buffer: Vec<RunRecordV2>,
    /// Parallel op buffer (only populated when `has_op` is true).
    op_buffer: Vec<u8>,
    buf_pos: usize,
    remaining: u64,
    // For compressed blocks:
    block_raw: Vec<u8>,
    is_compressed: bool,
    /// True when the run file is version 2 (31-byte records with op sideband).
    has_op: bool,
}

impl StreamingRunReader {
    /// Open a V2 run file for streaming.
    pub fn open(path: &Path) -> io::Result<Self> {
        let raw = std::fs::File::open(path)?;
        let mut file = BufReader::with_capacity(FILE_BUF_BYTES, raw);

        // Read header.
        let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = RunFileHeader::read_from(&header_buf)?;

        let is_compressed = (header.flags & RUN_FLAG_ZSTD_BLOCKS) != 0;
        let has_op = header.version == RUN_V2_VERSION_WITH_OP;
        let record_count = header.record_count;

        let mut reader = Self {
            file,
            remaining: record_count,
            header,
            buffer: Vec::with_capacity(BUFFER_SIZE.min(record_count as usize)),
            op_buffer: Vec::with_capacity(if has_op {
                BUFFER_SIZE.min(record_count as usize)
            } else {
                0
            }),
            buf_pos: 0,
            block_raw: Vec::new(),
            is_compressed,
            has_op,
        };

        if reader.remaining > 0 {
            reader.fill_buffer()?;
        }

        Ok(reader)
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        self.buffer.clear();
        self.op_buffer.clear();
        self.buf_pos = 0;

        if self.remaining == 0 {
            return Ok(());
        }

        let record_size = if self.has_op {
            RECORD_V2_WITH_OP_WIRE_SIZE
        } else {
            RECORD_V2_WIRE_SIZE
        };

        if !self.is_compressed {
            let to_read = (self.remaining as usize).min(BUFFER_SIZE);
            let byte_count = to_read * record_size;
            let mut raw = vec![0u8; byte_count];
            self.file.read_exact(&mut raw)?;

            self.buffer.reserve(to_read);
            if self.has_op {
                self.op_buffer.reserve(to_read);
            }
            for i in 0..to_read {
                let off = i * record_size;
                if self.has_op {
                    let buf: &[u8; RECORD_V2_WITH_OP_WIRE_SIZE] = raw
                        [off..off + RECORD_V2_WITH_OP_WIRE_SIZE]
                        .try_into()
                        .unwrap();
                    let (rec, op) = RunRecordV2::read_run_le_with_op(buf);
                    self.buffer.push(rec);
                    self.op_buffer.push(op);
                } else {
                    let buf: &[u8; RECORD_V2_WIRE_SIZE] =
                        raw[off..off + RECORD_V2_WIRE_SIZE].try_into().unwrap();
                    self.buffer.push(RunRecordV2::read_run_le(buf));
                }
            }
            self.remaining -= to_read as u64;
        } else {
            // Read one compressed block.
            let mut block_header = [0u8; 12];
            self.file.read_exact(&mut block_header)?;
            let n_records = u32::from_le_bytes(block_header[0..4].try_into().unwrap()) as usize;
            let raw_len = u32::from_le_bytes(block_header[4..8].try_into().unwrap()) as usize;
            let z_len = u32::from_le_bytes(block_header[8..12].try_into().unwrap()) as usize;

            let mut compressed = vec![0u8; z_len];
            self.file.read_exact(&mut compressed)?;

            self.block_raw = zstd::bulk::decompress(&compressed, raw_len)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            self.buffer.reserve(n_records);
            if self.has_op {
                self.op_buffer.reserve(n_records);
            }
            for i in 0..n_records {
                let off = i * record_size;
                if self.has_op {
                    let buf: &[u8; RECORD_V2_WITH_OP_WIRE_SIZE] = self.block_raw
                        [off..off + RECORD_V2_WITH_OP_WIRE_SIZE]
                        .try_into()
                        .unwrap();
                    let (rec, op) = RunRecordV2::read_run_le_with_op(buf);
                    self.buffer.push(rec);
                    self.op_buffer.push(op);
                } else {
                    let buf: &[u8; RECORD_V2_WIRE_SIZE] = self.block_raw
                        [off..off + RECORD_V2_WIRE_SIZE]
                        .try_into()
                        .unwrap();
                    self.buffer.push(RunRecordV2::read_run_le(buf));
                }
            }
            self.remaining -= n_records as u64;
        }

        Ok(())
    }
}

impl MergeSource for StreamingRunReader {
    #[inline]
    fn peek(&self) -> Option<&RunRecordV2> {
        self.buffer.get(self.buf_pos)
    }

    fn advance(&mut self) -> io::Result<()> {
        self.buf_pos += 1;
        if self.buf_pos >= self.buffer.len() && self.remaining > 0 {
            self.fill_buffer()?;
        }
        Ok(())
    }

    #[inline]
    fn is_exhausted(&self) -> bool {
        self.buf_pos >= self.buffer.len() && self.remaining == 0
    }

    #[inline]
    fn peek_op(&self) -> u8 {
        if self.has_op {
            self.op_buffer.get(self.buf_pos).copied().unwrap_or(1)
        } else {
            1 // Default: assert (import path)
        }
    }
}

// ============================================================================
// StreamingRunReaderWithOp
// ============================================================================

/// Buffered reader for V2 run files that carry an op byte per record.
///
/// Reads 31-byte records (version 2 FRN2 files) and maintains parallel
/// `buffer` and `op_buffer` vectors.
// Kept for: rebuild path merge input (reads version-2 FRN2 files with op sideband).
// Use when: incremental rebuild pipeline is wired in.
// Note: #[expect] will warn when this code is actually used.
#[expect(dead_code)]
pub(crate) struct StreamingRunReaderWithOp {
    file: BufReader<std::fs::File>,
    /// Kept for the sort_order accessor.
    pub header: RunFileHeader,
    buffer: Vec<RunRecordV2>,
    op_buffer: Vec<u8>,
    buf_pos: usize,
    remaining: u64,
    block_raw: Vec<u8>,
    is_compressed: bool,
}

impl StreamingRunReaderWithOp {
    /// Open a V2 run file with op sideband for streaming.
    ///
    /// The file must be version 2 (with-op). Returns an error if the
    /// file is version 1 (no-op).
    #[allow(dead_code)] // Used when rebuild pipeline is wired in; #[expect] triggers unfulfilled in test targets.
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let raw = std::fs::File::open(path)?;
        let mut file = BufReader::with_capacity(FILE_BUF_BYTES, raw);

        let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = RunFileHeader::read_from(&header_buf)?;

        if header.version != RUN_V2_VERSION_WITH_OP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "StreamingRunReaderWithOp requires version {}, got {}",
                    RUN_V2_VERSION_WITH_OP, header.version
                ),
            ));
        }

        let is_compressed = (header.flags & RUN_FLAG_ZSTD_BLOCKS) != 0;
        let record_count = header.record_count;

        let mut reader = Self {
            file,
            remaining: record_count,
            header,
            buffer: Vec::with_capacity(BUFFER_SIZE.min(record_count as usize)),
            op_buffer: Vec::with_capacity(BUFFER_SIZE.min(record_count as usize)),
            buf_pos: 0,
            block_raw: Vec::new(),
            is_compressed,
        };

        if reader.remaining > 0 {
            reader.fill_buffer()?;
        }

        Ok(reader)
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        self.buffer.clear();
        self.op_buffer.clear();
        self.buf_pos = 0;

        if self.remaining == 0 {
            return Ok(());
        }

        if !self.is_compressed {
            let to_read = (self.remaining as usize).min(BUFFER_SIZE);
            let byte_count = to_read * RECORD_V2_WITH_OP_WIRE_SIZE;
            let mut raw = vec![0u8; byte_count];
            self.file.read_exact(&mut raw)?;

            self.buffer.reserve(to_read);
            self.op_buffer.reserve(to_read);
            for i in 0..to_read {
                let off = i * RECORD_V2_WITH_OP_WIRE_SIZE;
                let buf: &[u8; RECORD_V2_WITH_OP_WIRE_SIZE] = raw
                    [off..off + RECORD_V2_WITH_OP_WIRE_SIZE]
                    .try_into()
                    .unwrap();
                let (rec, op) = RunRecordV2::read_run_le_with_op(buf);
                self.buffer.push(rec);
                self.op_buffer.push(op);
            }
            self.remaining -= to_read as u64;
        } else {
            // Read one compressed block.
            let mut block_header = [0u8; 12];
            self.file.read_exact(&mut block_header)?;
            let n_records = u32::from_le_bytes(block_header[0..4].try_into().unwrap()) as usize;
            let raw_len = u32::from_le_bytes(block_header[4..8].try_into().unwrap()) as usize;
            let z_len = u32::from_le_bytes(block_header[8..12].try_into().unwrap()) as usize;

            let mut compressed = vec![0u8; z_len];
            self.file.read_exact(&mut compressed)?;

            self.block_raw = zstd::bulk::decompress(&compressed, raw_len)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            self.buffer.reserve(n_records);
            self.op_buffer.reserve(n_records);
            for i in 0..n_records {
                let off = i * RECORD_V2_WITH_OP_WIRE_SIZE;
                let buf: &[u8; RECORD_V2_WITH_OP_WIRE_SIZE] = self.block_raw
                    [off..off + RECORD_V2_WITH_OP_WIRE_SIZE]
                    .try_into()
                    .unwrap();
                let (rec, op) = RunRecordV2::read_run_le_with_op(buf);
                self.buffer.push(rec);
                self.op_buffer.push(op);
            }
            self.remaining -= n_records as u64;
        }

        Ok(())
    }
}

impl MergeSource for StreamingRunReaderWithOp {
    #[inline]
    fn peek(&self) -> Option<&RunRecordV2> {
        self.buffer.get(self.buf_pos)
    }

    fn advance(&mut self) -> io::Result<()> {
        self.buf_pos += 1;
        if self.buf_pos >= self.buffer.len() && self.remaining > 0 {
            self.fill_buffer()?;
        }
        Ok(())
    }

    #[inline]
    fn is_exhausted(&self) -> bool {
        self.buf_pos >= self.buffer.len() && self.remaining == 0
    }

    #[inline]
    fn peek_op(&self) -> u8 {
        self.op_buffer.get(self.buf_pos).copied().unwrap_or(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::runs::run_file::{write_run_file, write_run_file_with_op};
    use fluree_db_binary_index::format::run_record::RunSortOrder;
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_binary_index::format::run_record_v2::cmp_v2_spot;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id: 0,
        }
    }

    #[test]
    fn streaming_read_roundtrip() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_v2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let mut records = vec![
            make_rec(3, 1, OType::XSD_INTEGER.as_u16(), 30, 3),
            make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1),
            make_rec(2, 1, OType::XSD_STRING.as_u16(), 20, 2),
        ];
        records.sort_by(cmp_v2_spot);

        write_run_file(&path, &records, RunSortOrder::Spot, 1, 3).unwrap();

        // Stream read.
        let mut reader = StreamingRunReader::open(&path).unwrap();
        let mut read_back = Vec::new();
        while let Some(rec) = reader.peek() {
            read_back.push(*rec);
            reader.advance().unwrap();
        }
        assert!(reader.is_exhausted());
        assert_eq!(read_back.len(), 3);

        // Verify sort order preserved.
        assert_eq!(read_back[0].s_id.as_u64(), 1);
        assert_eq!(read_back[1].s_id.as_u64(), 2);
        assert_eq!(read_back[2].s_id.as_u64(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn streaming_read_with_op_roundtrip() {
        let dir = std::env::temp_dir().join("fluree_test_streaming_v2_with_op");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_op.frn");

        let mut records = vec![
            make_rec(3, 1, OType::XSD_INTEGER.as_u16(), 30, 3),
            make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1),
            make_rec(2, 1, OType::XSD_STRING.as_u16(), 20, 2),
        ];
        let mut ops = vec![1u8, 0u8, 1u8];

        // Sort records and ops together by SPOT order.
        let mut indices: Vec<usize> = (0..records.len()).collect();
        indices.sort_unstable_by(|&a, &b| cmp_v2_spot(&records[a], &records[b]));
        let sorted_recs: Vec<_> = indices.iter().map(|&i| records[i]).collect();
        let sorted_ops: Vec<_> = indices.iter().map(|&i| ops[i]).collect();
        records = sorted_recs;
        ops = sorted_ops;

        write_run_file_with_op(&path, &records, &ops, RunSortOrder::Spot, 1, 3).unwrap();

        // Stream read with op.
        let mut reader = StreamingRunReaderWithOp::open(&path).unwrap();
        let mut read_back = Vec::new();
        let mut read_ops = Vec::new();
        while let Some(rec) = reader.peek() {
            read_back.push(*rec);
            read_ops.push(reader.peek_op());
            reader.advance().unwrap();
        }
        assert!(reader.is_exhausted());
        assert_eq!(read_back.len(), 3);

        // Verify sort order preserved.
        assert_eq!(read_back[0].s_id.as_u64(), 1);
        assert_eq!(read_back[1].s_id.as_u64(), 2);
        assert_eq!(read_back[2].s_id.as_u64(), 3);

        // Verify ops survived the roundtrip.
        // After sorting: s=1 had op=0, s=2 had op=1, s=3 had op=1
        assert_eq!(read_ops[0], 0); // s=1 was retract
        assert_eq!(read_ops[1], 1); // s=2 was assert
        assert_eq!(read_ops[2], 1); // s=3 was assert

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_peek_op_is_one() {
        // StreamingRunReader (no-op) should return peek_op() == 1 by default.
        let dir = std::env::temp_dir().join("fluree_test_peek_op_default");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let records = vec![make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1)];
        write_run_file(&path, &records, RunSortOrder::Spot, 1, 1).unwrap();

        let reader = StreamingRunReader::open(&path).unwrap();
        assert_eq!(reader.peek_op(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
