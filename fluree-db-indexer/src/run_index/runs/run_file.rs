//! V2 run file binary format: header + records (no language dictionary).
//!
//! Language identity is captured in `o_type`, so no separate language
//! dictionary is needed in V2 run files.
//!
//! ```text
//! [Header: 64 bytes]
//!   magic: "FRN2" (4B), version: u8 (=1), sort_order: u8, flags: u8, _pad: u8
//!   record_count: u64
//!   records_offset: u64  (= 64, immediately after header)
//!   min_t: u32, max_t: u32
//!   _reserved: [u8; 32]
//! [Records: record_count × RECORD_V2_WIRE_SIZE bytes]
//!   (or zstd-compressed blocks if FLAG_ZSTD is set)
//! ```

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    RunRecordV2, RECORD_V2_WIRE_SIZE, RECORD_V2_WITH_OP_WIRE_SIZE,
};
use std::io::{self, Write};
use std::path::Path;

/// Magic bytes for a V2 run file.
pub const RUN_V2_MAGIC: [u8; 4] = *b"FRN2";

/// V2 run file version (30-byte records, no op).
pub const RUN_V2_VERSION: u8 = 1;

/// V2 run file version with op sideband (31-byte records: 30 record + 1 op).
pub const RUN_V2_VERSION_WITH_OP: u8 = 2;

/// Run file flags.
const RUN_FLAG_ZSTD_BLOCKS: u8 = 1 << 0;

/// Header size in bytes.
pub const RUN_V2_HEADER_LEN: usize = 64;

/// V2 run file header.
#[derive(Debug, Clone)]
pub struct RunFileHeader {
    pub version: u8,
    pub sort_order: RunSortOrder,
    pub flags: u8,
    pub record_count: u64,
    pub records_offset: u64,
    pub min_t: u32,
    pub max_t: u32,
}

impl RunFileHeader {
    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RUN_V2_HEADER_LEN);
        buf[0..4].copy_from_slice(&RUN_V2_MAGIC);
        buf[4] = self.version;
        buf[5] = self.sort_order.to_wire_id();
        buf[6] = self.flags;
        buf[7] = 0;
        buf[8..16].copy_from_slice(&self.record_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.records_offset.to_le_bytes());
        buf[24..28].copy_from_slice(&self.min_t.to_le_bytes());
        buf[28..32].copy_from_slice(&self.max_t.to_le_bytes());
        buf[32..64].fill(0); // reserved
    }

    /// Returns true if this file uses the with-op wire format (version 2).
    pub fn has_op(&self) -> bool {
        self.version == RUN_V2_VERSION_WITH_OP
    }

    pub fn read_from(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < RUN_V2_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 run file header too small",
            ));
        }
        if buf[0..4] != RUN_V2_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("V2 run file: expected magic FRN2, got {:?}", &buf[0..4]),
            ));
        }
        let version = buf[4];
        if version != RUN_V2_VERSION && version != RUN_V2_VERSION_WITH_OP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("V2 run file: unsupported version {version}"),
            ));
        }
        let sort_order = RunSortOrder::from_u8(buf[5]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("V2 run file: unknown sort order {}", buf[5]),
            )
        })?;
        Ok(Self {
            version,
            sort_order,
            flags: buf[6],
            record_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            records_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            min_t: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            max_t: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
        })
    }
}

// ============================================================================
// Write a V2 run file
// ============================================================================

/// Write a sorted V2 run file to disk.
pub fn write_run_file(
    path: &Path,
    records: &[RunRecordV2],
    sort_order: RunSortOrder,
    min_t: u32,
    max_t: u32,
) -> io::Result<RunFileInfo> {
    let raw = std::fs::File::create(path)?;
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let _ = libc::fcntl(raw.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }
    let mut file = io::BufWriter::new(raw);

    let compress = std::env::var("FLUREE_RUN_ZSTD")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true);
    let zstd_level = std::env::var("FLUREE_RUN_ZSTD_LEVEL")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    let records_offset = RUN_V2_HEADER_LEN as u64;

    let header = RunFileHeader {
        version: RUN_V2_VERSION,
        sort_order,
        flags: if compress { RUN_FLAG_ZSTD_BLOCKS } else { 0 },
        record_count: records.len() as u64,
        records_offset,
        min_t,
        max_t,
    };
    let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
    header.write_to(&mut header_buf);
    file.write_all(&header_buf)?;

    if !compress {
        let mut rec_buf = [0u8; RECORD_V2_WIRE_SIZE];
        for rec in records {
            rec.write_run_le(&mut rec_buf);
            file.write_all(&rec_buf)?;
        }
    } else {
        const BLOCK_RECORDS: usize = 8192;
        let mut raw_buf: Vec<u8> = Vec::with_capacity(BLOCK_RECORDS * RECORD_V2_WIRE_SIZE);
        let mut rec_buf = [0u8; RECORD_V2_WIRE_SIZE];

        for chunk in records.chunks(BLOCK_RECORDS) {
            raw_buf.clear();
            raw_buf.resize(chunk.len() * RECORD_V2_WIRE_SIZE, 0u8);
            for (i, rec) in chunk.iter().enumerate() {
                rec.write_run_le(&mut rec_buf);
                let off = i * RECORD_V2_WIRE_SIZE;
                raw_buf[off..off + RECORD_V2_WIRE_SIZE].copy_from_slice(&rec_buf);
            }

            let compressed = zstd::bulk::compress(&raw_buf, zstd_level)?;
            file.write_all(&(chunk.len() as u32).to_le_bytes())?;
            file.write_all(&(raw_buf.len() as u32).to_le_bytes())?;
            file.write_all(&(compressed.len() as u32).to_le_bytes())?;
            file.write_all(&compressed)?;
        }
    }

    file.flush()?;

    Ok(RunFileInfo {
        path: path.to_path_buf(),
        record_count: records.len() as u64,
        sort_order,
        min_t,
        max_t,
    })
}

/// Write a sorted V2 run file with op sideband to disk.
///
/// `records` and `ops` are parallel arrays of equal length.
/// Each record is written as 31 bytes (30 record + 1 op byte).
/// The header version is set to `2` so readers can distinguish from
/// version-1 (no-op) files.
// Kept for: rebuild path where assert/retract ops must survive into the merge.
// Use when: incremental rebuild pipeline is wired in.
// Called by: RunWriterWithOp::flush_buffer and tests.
// Note: Using #[allow] instead of #[expect] because when the sole caller
// (RunWriterWithOp) is also dead, #[expect(dead_code)] becomes unfulfilled
// and triggers a separate warning. This is a known Rust lint transitivity issue.
#[allow(dead_code)]
pub(crate) fn write_run_file_with_op(
    path: &Path,
    records: &[RunRecordV2],
    ops: &[u8],
    sort_order: RunSortOrder,
    min_t: u32,
    max_t: u32,
) -> io::Result<RunFileInfo> {
    debug_assert_eq!(records.len(), ops.len());

    let raw = std::fs::File::create(path)?;
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let _ = libc::fcntl(raw.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }
    let mut file = io::BufWriter::new(raw);

    let compress = std::env::var("FLUREE_RUN_ZSTD")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true);
    let zstd_level = std::env::var("FLUREE_RUN_ZSTD_LEVEL")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    let records_offset = RUN_V2_HEADER_LEN as u64;

    let header = RunFileHeader {
        version: RUN_V2_VERSION_WITH_OP,
        sort_order,
        flags: if compress { RUN_FLAG_ZSTD_BLOCKS } else { 0 },
        record_count: records.len() as u64,
        records_offset,
        min_t,
        max_t,
    };
    let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
    header.write_to(&mut header_buf);
    file.write_all(&header_buf)?;

    if !compress {
        let mut rec_buf = [0u8; RECORD_V2_WITH_OP_WIRE_SIZE];
        for (rec, &op) in records.iter().zip(ops.iter()) {
            rec.write_run_le_with_op(op, &mut rec_buf);
            file.write_all(&rec_buf)?;
        }
    } else {
        const BLOCK_RECORDS: usize = 8192;
        let mut raw_buf: Vec<u8> = Vec::with_capacity(BLOCK_RECORDS * RECORD_V2_WITH_OP_WIRE_SIZE);
        let mut rec_buf = [0u8; RECORD_V2_WITH_OP_WIRE_SIZE];

        let record_ops: Vec<(&RunRecordV2, &u8)> = records.iter().zip(ops.iter()).collect();

        for chunk in record_ops.chunks(BLOCK_RECORDS) {
            raw_buf.clear();
            raw_buf.resize(chunk.len() * RECORD_V2_WITH_OP_WIRE_SIZE, 0u8);
            for (i, &(rec, &op)) in chunk.iter().enumerate() {
                rec.write_run_le_with_op(op, &mut rec_buf);
                let off = i * RECORD_V2_WITH_OP_WIRE_SIZE;
                raw_buf[off..off + RECORD_V2_WITH_OP_WIRE_SIZE].copy_from_slice(&rec_buf);
            }

            let compressed = zstd::bulk::compress(&raw_buf, zstd_level)?;
            file.write_all(&(chunk.len() as u32).to_le_bytes())?;
            file.write_all(&(raw_buf.len() as u32).to_le_bytes())?;
            file.write_all(&(compressed.len() as u32).to_le_bytes())?;
            file.write_all(&compressed)?;
        }
    }

    file.flush()?;

    Ok(RunFileInfo {
        path: path.to_path_buf(),
        record_count: records.len() as u64,
        sort_order,
        min_t,
        max_t,
    })
}

/// Metadata about a written V2 run file.
#[derive(Debug, Clone)]
pub struct RunFileInfo {
    pub path: std::path::PathBuf,
    pub record_count: u64,
    pub sort_order: RunSortOrder,
    pub min_t: u32,
    pub max_t: u32,
}

/// Records per streamed block (matches `write_run_file`'s block size, so
/// streamed output is byte-compatible with the slice-based writers).
const STREAM_BLOCK_RECORDS: usize = 8192;

/// Streaming V2 run-file writer: writes a placeholder header, streams records
/// in [`STREAM_BLOCK_RECORDS`]-sized blocks (same wire format as
/// [`write_run_file`] / [`write_run_file_with_op`]), then seeks back and
/// patches `record_count`/`min_t`/`max_t` on [`Self::finish`].
///
/// Exists for cascaded merges, whose output is produced record-at-a-time from
/// a k-way merge and must not be buffered wholesale in memory. Version is 2
/// (with-op, 31-byte records) when `with_op` is set, else 1 —
/// `StreamingRunReader` auto-detects both, so cascade output is transparently
/// readable wherever original run files are.
pub(crate) struct StreamingRunFileWriter {
    file: io::BufWriter<std::fs::File>,
    sort_order: RunSortOrder,
    with_op: bool,
    compress: bool,
    zstd_level: i32,
    block_records: Vec<RunRecordV2>,
    /// Parallel to `block_records`; only populated when `with_op`.
    block_ops: Vec<u8>,
    record_count: u64,
    min_t: u32,
    max_t: u32,
    path: std::path::PathBuf,
}

impl StreamingRunFileWriter {
    /// Create with compression settings from the environment — the same
    /// `FLUREE_RUN_ZSTD` / `FLUREE_RUN_ZSTD_LEVEL` contract as
    /// [`write_run_file`].
    pub(crate) fn create(path: &Path, sort_order: RunSortOrder, with_op: bool) -> io::Result<Self> {
        let compress = std::env::var("FLUREE_RUN_ZSTD")
            .ok()
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        let zstd_level = std::env::var("FLUREE_RUN_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1);
        Self::create_with_options(path, sort_order, with_op, compress, zstd_level)
    }

    /// Create with explicit compression settings (tests exercise both paths
    /// without mutating process-global environment variables).
    pub(crate) fn create_with_options(
        path: &Path,
        sort_order: RunSortOrder,
        with_op: bool,
        compress: bool,
        zstd_level: i32,
    ) -> io::Result<Self> {
        let raw = std::fs::File::create(path)?;
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                let _ = libc::fcntl(raw.as_raw_fd(), libc::F_NOCACHE, 1);
            }
        }
        let mut file = io::BufWriter::new(raw);

        // Placeholder header; patched in `finish`.
        let header = Self::header(with_op, sort_order, compress, 0, 0, 0);
        let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
        header.write_to(&mut header_buf);
        file.write_all(&header_buf)?;

        Ok(Self {
            file,
            sort_order,
            with_op,
            compress,
            zstd_level,
            block_records: Vec::with_capacity(STREAM_BLOCK_RECORDS),
            block_ops: if with_op {
                Vec::with_capacity(STREAM_BLOCK_RECORDS)
            } else {
                Vec::new()
            },
            record_count: 0,
            min_t: u32::MAX,
            max_t: 0,
            path: path.to_path_buf(),
        })
    }

    fn header(
        with_op: bool,
        sort_order: RunSortOrder,
        compress: bool,
        record_count: u64,
        min_t: u32,
        max_t: u32,
    ) -> RunFileHeader {
        RunFileHeader {
            version: if with_op {
                RUN_V2_VERSION_WITH_OP
            } else {
                RUN_V2_VERSION
            },
            sort_order,
            flags: if compress { RUN_FLAG_ZSTD_BLOCKS } else { 0 },
            record_count,
            records_offset: RUN_V2_HEADER_LEN as u64,
            min_t,
            max_t,
        }
    }

    /// Append one record (with its op byte; ignored unless `with_op`).
    pub(crate) fn push(&mut self, record: RunRecordV2, op: u8) -> io::Result<()> {
        self.min_t = self.min_t.min(record.t);
        self.max_t = self.max_t.max(record.t);
        self.record_count += 1;
        self.block_records.push(record);
        if self.with_op {
            self.block_ops.push(op);
        }
        if self.block_records.len() >= STREAM_BLOCK_RECORDS {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.block_records.is_empty() {
            return Ok(());
        }
        let record_size = if self.with_op {
            RECORD_V2_WITH_OP_WIRE_SIZE
        } else {
            RECORD_V2_WIRE_SIZE
        };
        let mut raw_buf = vec![0u8; self.block_records.len() * record_size];
        if self.with_op {
            let mut rec_buf = [0u8; RECORD_V2_WITH_OP_WIRE_SIZE];
            for (i, (rec, &op)) in self
                .block_records
                .iter()
                .zip(self.block_ops.iter())
                .enumerate()
            {
                rec.write_run_le_with_op(op, &mut rec_buf);
                let off = i * record_size;
                raw_buf[off..off + record_size].copy_from_slice(&rec_buf);
            }
        } else {
            let mut rec_buf = [0u8; RECORD_V2_WIRE_SIZE];
            for (i, rec) in self.block_records.iter().enumerate() {
                rec.write_run_le(&mut rec_buf);
                let off = i * record_size;
                raw_buf[off..off + record_size].copy_from_slice(&rec_buf);
            }
        }
        if self.compress {
            let compressed = zstd::bulk::compress(&raw_buf, self.zstd_level)?;
            self.file
                .write_all(&(self.block_records.len() as u32).to_le_bytes())?;
            self.file.write_all(&(raw_buf.len() as u32).to_le_bytes())?;
            self.file
                .write_all(&(compressed.len() as u32).to_le_bytes())?;
            self.file.write_all(&compressed)?;
        } else {
            self.file.write_all(&raw_buf)?;
        }
        self.block_records.clear();
        self.block_ops.clear();
        Ok(())
    }

    /// Flush the trailing block, patch the header, and sync buffered bytes.
    pub(crate) fn finish(mut self) -> io::Result<RunFileInfo> {
        use std::io::Seek;

        self.flush_block()?;
        let min_t = if self.record_count == 0 { 0 } else { self.min_t };
        let header = Self::header(
            self.with_op,
            self.sort_order,
            self.compress,
            self.record_count,
            min_t,
            self.max_t,
        );
        let mut header_buf = [0u8; RUN_V2_HEADER_LEN];
        header.write_to(&mut header_buf);
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.write_all(&header_buf)?;
        self.file.flush()?;
        Ok(RunFileInfo {
            path: self.path,
            record_count: self.record_count,
            sort_order: self.sort_order,
            min_t,
            max_t: self.max_t,
        })
    }
}

// ── Language dictionary serialization (format-independent) ─────────────────

use crate::run_index::resolve::global_dict::LanguageTagDict;

/// Serialize a `LanguageTagDict` to bytes.
///
/// Format: `count: u16` then `[len: u8, utf8_bytes]` for each entry.
pub fn serialize_lang_dict(dict: &LanguageTagDict) -> Vec<u8> {
    let count = dict.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&count.to_le_bytes());
    for (_id, tag) in dict.iter() {
        let tag_bytes = tag.as_bytes();
        debug_assert!(tag_bytes.len() <= 255, "language tag too long");
        buf.push(tag_bytes.len() as u8);
        buf.extend_from_slice(tag_bytes);
    }
    buf
}

/// Deserialize a `LanguageTagDict` from bytes.
pub fn deserialize_lang_dict(data: &[u8]) -> io::Result<LanguageTagDict> {
    if data.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run file: lang dict too small",
        ));
    }
    let count = u16::from_le_bytes(data[0..2].try_into().unwrap());
    let mut dict = LanguageTagDict::new();
    let mut pos = 2;
    for _ in 0..count {
        if pos >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: lang dict truncated",
            ));
        }
        let len = data[pos] as usize;
        pos += 1;
        if pos + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run file: lang dict entry truncated",
            ));
        }
        let tag = std::str::from_utf8(&data[pos..pos + len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("run file: invalid UTF-8 in lang dict: {e}"),
            )
        })?;
        dict.get_or_insert(Some(tag));
        pos += len;
    }
    Ok(dict)
}

/// Alias for backwards compatibility with code referencing the V1 header length constant.
pub const RUN_HEADER_LEN: usize = RUN_V2_HEADER_LEN;

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
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
    fn header_round_trip() {
        let header = RunFileHeader {
            version: RUN_V2_VERSION,
            sort_order: RunSortOrder::Post,
            flags: 0,
            record_count: 500,
            records_offset: 64,
            min_t: 1,
            max_t: 42,
        };
        let mut buf = [0u8; RUN_V2_HEADER_LEN];
        header.write_to(&mut buf);
        let parsed = RunFileHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.sort_order, RunSortOrder::Post);
        assert_eq!(parsed.record_count, 500);
        assert_eq!(parsed.min_t, 1);
        assert_eq!(parsed.max_t, 42);
    }

    #[test]
    fn header_version_with_op() {
        let header = RunFileHeader {
            version: RUN_V2_VERSION_WITH_OP,
            sort_order: RunSortOrder::Spot,
            flags: 0,
            record_count: 10,
            records_offset: 64,
            min_t: 1,
            max_t: 10,
        };
        assert!(header.has_op());

        let mut buf = [0u8; RUN_V2_HEADER_LEN];
        header.write_to(&mut buf);
        let parsed = RunFileHeader::read_from(&buf).unwrap();
        assert!(parsed.has_op());
        assert_eq!(parsed.version, RUN_V2_VERSION_WITH_OP);
    }

    #[test]
    fn write_and_read_v2_with_op_run_file() {
        let dir = std::env::temp_dir().join("fluree_test_run_v2_with_op");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_op.frn");

        let records = vec![
            make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1),
            make_rec(2, 1, OType::XSD_STRING.as_u16(), 20, 2),
            make_rec(3, 2, OType::IRI_REF.as_u16(), 30, 3),
        ];
        let ops = vec![1u8, 0u8, 1u8]; // assert, retract, assert

        let info = write_run_file_with_op(&path, &records, &ops, RunSortOrder::Spot, 1, 3).unwrap();
        assert_eq!(info.record_count, 3);

        // Read back header and verify version.
        let data = std::fs::read(&path).unwrap();
        let header = RunFileHeader::read_from(&data).unwrap();
        assert_eq!(header.record_count, 3);
        assert!(header.has_op());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The streaming writer must be byte-identical to the slice-based
    /// writers (same header, same 8192-record zstd blocks) — cascade
    /// intermediates are read by the same `StreamingRunReader` as originals.
    #[test]
    fn streaming_writer_matches_slice_writers() {
        let dir = tempfile::tempdir().unwrap();

        // Multi-block volume (> 8192 records) with wrapping t values.
        let records: Vec<RunRecordV2> = (0..20_000u64)
            .map(|i| {
                make_rec(
                    i,
                    1,
                    OType::XSD_INTEGER.as_u16(),
                    i * 3,
                    (i % 7) as u32 + 1,
                )
            })
            .collect();
        let ops: Vec<u8> = (0..20_000u64).map(|i| (i % 2) as u8).collect();
        let min_t = 1;
        let max_t = 7;

        // No-op variant (version 1), compressed level 1 — the slice writer's
        // defaults with no FLUREE_RUN_ZSTD* env set.
        let slice_path = dir.path().join("slice.frn");
        write_run_file(&slice_path, &records, RunSortOrder::Spot, min_t, max_t).unwrap();
        let stream_path = dir.path().join("stream.frn");
        let mut writer =
            StreamingRunFileWriter::create_with_options(&stream_path, RunSortOrder::Spot, false, true, 1)
                .unwrap();
        for rec in &records {
            writer.push(*rec, 1).unwrap();
        }
        let info = writer.finish().unwrap();
        assert_eq!(info.record_count, records.len() as u64);
        assert_eq!(info.min_t, min_t);
        assert_eq!(info.max_t, max_t);
        assert_eq!(
            std::fs::read(&slice_path).unwrap(),
            std::fs::read(&stream_path).unwrap()
        );

        // With-op variant (version 2).
        let slice_op_path = dir.path().join("slice_op.frn");
        write_run_file_with_op(&slice_op_path, &records, &ops, RunSortOrder::Spot, min_t, max_t)
            .unwrap();
        let stream_op_path = dir.path().join("stream_op.frn");
        let mut writer =
            StreamingRunFileWriter::create_with_options(&stream_op_path, RunSortOrder::Spot, true, true, 1)
                .unwrap();
        for (rec, &op) in records.iter().zip(ops.iter()) {
            writer.push(*rec, op).unwrap();
        }
        writer.finish().unwrap();
        assert_eq!(
            std::fs::read(&slice_op_path).unwrap(),
            std::fs::read(&stream_op_path).unwrap()
        );
    }

    /// Uncompressed streaming output round-trips through the auto-detecting
    /// reader (the compressed path is covered byte-for-byte above).
    #[test]
    fn streaming_writer_uncompressed_roundtrip() {
        use crate::run_index::runs::streaming_reader::{MergeSource, StreamingRunReader};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw.frn");
        let records: Vec<RunRecordV2> = (0..100u64)
            .map(|i| make_rec(i, 1, OType::XSD_INTEGER.as_u16(), i, i as u32 + 1))
            .collect();

        let mut writer =
            StreamingRunFileWriter::create_with_options(&path, RunSortOrder::Spot, true, false, 1)
                .unwrap();
        for (i, rec) in records.iter().enumerate() {
            writer.push(*rec, (i % 2) as u8).unwrap();
        }
        writer.finish().unwrap();

        let mut reader = StreamingRunReader::open(&path).unwrap();
        let mut n = 0usize;
        while let Some(rec) = reader.peek() {
            assert_eq!(rec.s_id.as_u64(), n as u64);
            assert_eq!(reader.peek_op(), (n % 2) as u8);
            n += 1;
            reader.advance().unwrap();
        }
        assert_eq!(n, records.len());

        // Empty output: header must read back with zero records.
        let empty_path = dir.path().join("empty.frn");
        let writer =
            StreamingRunFileWriter::create_with_options(&empty_path, RunSortOrder::Post, false, true, 1)
                .unwrap();
        let info = writer.finish().unwrap();
        assert_eq!(info.record_count, 0);
        let mut reader = StreamingRunReader::open(&empty_path).unwrap();
        assert!(reader.peek().is_none());
        assert!(MergeSource::is_exhausted(&reader));
        let _ = reader.advance();
    }

    #[test]
    fn write_and_read_v2_run_file() {
        let dir = std::env::temp_dir().join("fluree_test_run_v2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.frn");

        let records = vec![
            make_rec(1, 1, OType::XSD_INTEGER.as_u16(), 10, 1),
            make_rec(2, 1, OType::XSD_STRING.as_u16(), 20, 2),
            make_rec(3, 2, OType::IRI_REF.as_u16(), 30, 3),
        ];

        let info = write_run_file(&path, &records, RunSortOrder::Spot, 1, 3).unwrap();
        assert_eq!(info.record_count, 3);

        // Read back via header + raw bytes (streaming reader will be separate)
        let data = std::fs::read(&path).unwrap();
        let header = RunFileHeader::read_from(&data).unwrap();
        assert_eq!(header.record_count, 3);
        assert_eq!(header.sort_order, RunSortOrder::Spot);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
