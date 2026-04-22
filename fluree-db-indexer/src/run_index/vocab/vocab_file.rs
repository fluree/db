//! Sorted vocabulary file I/O for the external-sort merge pipeline.
//!
//! Each parse chunk sorts its local dictionary entries and writes them to a
//! `.voc` file on disk. These sorted files are later consumed by a k-way merge
//! ([`super::vocab_merge`]) to produce global dictionaries and remap tables.
//!
//! ## Subject vocab format (`chunk_NNNNN.subjects.voc`)
//!
//! ```text
//! Header (20 bytes):
//!   magic:        "SV01" [u8; 4]
//!   entry_count:  u64    (number of entries)
//!   max_local_id: u64    (must equal entry_count - 1 for non-empty files)
//!
//! Entries (sorted by ns_code ASC, then suffix_bytes lexicographic ASC):
//!   ns_code:      u16
//!   local_id:     u64    (chunk-local ID, for remap table indexing)
//!   suffix_len:   u32
//!   suffix_bytes: [u8; suffix_len]
//! ```
//!
//! ## String vocab format (`chunk_NNNNN.strings.voc`)
//!
//! ```text
//! Header (20 bytes):
//!   magic:        "ST01" [u8; 4]
//!   entry_count:  u64    (number of entries)
//!   max_local_id: u32    (must equal entry_count - 1 for non-empty files)
//!   _pad:         u32    (reserved, zero)
//!
//! Entries (sorted by string_bytes lexicographic ASC):
//!   local_id:     u32    (chunk-local ID)
//!   string_len:   u32
//!   string_bytes: [u8; string_len]
//! ```
//!
//! The `max_local_id` field is a safety check: the merge phase pre-allocates
//! remap tables sized `(max_local_id + 1) × entry_width`. If local IDs aren't
//! dense in `[0..entry_count)`, the file is rejected at open time rather than
//! silently producing undersized remap mmaps.

use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

// ============================================================================
// Constants
// ============================================================================

const SUBJECT_MAGIC: [u8; 4] = *b"SV01";
const STRING_MAGIC: [u8; 4] = *b"ST01";
const HEADER_LEN: usize = 20;

// ============================================================================
// Subject vocab writer
// ============================================================================

/// Writes sorted subject vocab entries to a `.voc` file.
///
/// Usage:
/// 1. Create with [`SubjectVocabWriter::new`]
/// 2. Call [`write_entry`](SubjectVocabWriter::write_entry) for each entry
///    (must be in sorted order: ns_code ASC, suffix lexicographic ASC)
/// 3. Call [`finish`](SubjectVocabWriter::finish) to write the header
pub struct SubjectVocabWriter {
    writer: BufWriter<std::fs::File>,
    entry_count: u64,
    max_local_id: u64,
}

impl SubjectVocabWriter {
    /// Create a new writer. Writes a placeholder header that gets filled in by
    /// [`finish`](SubjectVocabWriter::finish).
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);
        // Write placeholder header (zeros).
        writer.write_all(&[0u8; HEADER_LEN])?;
        Ok(Self {
            writer,
            entry_count: 0,
            max_local_id: 0,
        })
    }

    /// Write a single entry. Entries must be written in sorted order.
    #[inline]
    pub fn write_entry(&mut self, ns_code: u16, local_id: u64, suffix: &[u8]) -> io::Result<()> {
        self.writer.write_all(&ns_code.to_le_bytes())?;
        self.writer.write_all(&local_id.to_le_bytes())?;
        let suffix_len = suffix.len() as u32;
        self.writer.write_all(&suffix_len.to_le_bytes())?;
        self.writer.write_all(suffix)?;

        if local_id > self.max_local_id || self.entry_count == 0 {
            self.max_local_id = local_id;
        }
        self.entry_count += 1;
        Ok(())
    }

    /// Finalize: seek back and write the real header. Validates that local IDs
    /// are dense in `[0..entry_count)`.
    pub fn finish(mut self) -> io::Result<u64> {
        self.writer.flush()?;

        // Validate contiguity: max_local_id + 1 == entry_count (when non-empty).
        if self.entry_count > 0 && self.max_local_id + 1 != self.entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject vocab: non-contiguous local IDs: max_local_id={}, entry_count={}",
                    self.max_local_id, self.entry_count
                ),
            ));
        }

        let mut file = self.writer.into_inner()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&SUBJECT_MAGIC)?;
        file.write_all(&self.entry_count.to_le_bytes())?;
        file.write_all(&self.max_local_id.to_le_bytes())?;
        file.flush()?;

        Ok(self.entry_count)
    }
}

// ============================================================================
// Subject vocab reader
// ============================================================================

/// A single entry read from a subject vocab file.
#[derive(Debug, Clone)]
pub struct SubjectVocabEntry {
    pub ns_code: u16,
    pub local_id: u64,
    pub suffix: Vec<u8>,
}

/// Header metadata from a subject vocab file.
#[derive(Debug, Clone, Copy)]
pub struct SubjectVocabHeader {
    pub entry_count: u64,
    pub max_local_id: u64,
}

/// Reads sorted subject vocab entries from a `.voc` file.
#[derive(Debug)]
pub struct SubjectVocabReader {
    reader: BufReader<std::fs::File>,
    remaining: u64,
    header: SubjectVocabHeader,
}

impl SubjectVocabReader {
    /// Open and validate a subject vocab file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut hdr = [0u8; HEADER_LEN];
        reader.read_exact(&mut hdr)?;

        if hdr[0..4] != SUBJECT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("subject vocab: bad magic {:?}", &hdr[0..4]),
            ));
        }

        let entry_count = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
        let max_local_id = u64::from_le_bytes(hdr[12..20].try_into().unwrap());

        if entry_count > 0 && max_local_id + 1 != entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "subject vocab: non-contiguous local IDs: max_local_id={max_local_id}, entry_count={entry_count}"
                ),
            ));
        }

        let header = SubjectVocabHeader {
            entry_count,
            max_local_id,
        };

        Ok(Self {
            reader,
            remaining: entry_count,
            header,
        })
    }

    /// The header metadata.
    pub fn header(&self) -> SubjectVocabHeader {
        self.header
    }

    /// Number of entries remaining to be read.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Read the next entry. Returns `None` when all entries have been consumed.
    pub fn next_entry(&mut self) -> io::Result<Option<SubjectVocabEntry>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        let mut buf = [0u8; 14]; // 2 (ns_code) + 8 (local_id) + 4 (suffix_len)
        self.reader.read_exact(&mut buf)?;

        let ns_code = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let local_id = u64::from_le_bytes(buf[2..10].try_into().unwrap());
        let suffix_len = u32::from_le_bytes(buf[10..14].try_into().unwrap()) as usize;

        let mut suffix = vec![0u8; suffix_len];
        if suffix_len > 0 {
            self.reader.read_exact(&mut suffix)?;
        }

        self.remaining -= 1;

        Ok(Some(SubjectVocabEntry {
            ns_code,
            local_id,
            suffix,
        }))
    }
}

// ============================================================================
// String vocab writer
// ============================================================================

/// Writes sorted string vocab entries to a `.voc` file.
///
/// Usage mirrors [`SubjectVocabWriter`] but for string dictionaries.
pub struct StringVocabWriter {
    writer: BufWriter<std::fs::File>,
    entry_count: u64,
    max_local_id: u32,
}

impl StringVocabWriter {
    /// Create a new writer with a placeholder header.
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&[0u8; HEADER_LEN])?;
        Ok(Self {
            writer,
            entry_count: 0,
            max_local_id: 0,
        })
    }

    /// Write a single entry. Entries must be written in sorted order.
    #[inline]
    pub fn write_entry(&mut self, local_id: u32, string_bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(&local_id.to_le_bytes())?;
        let string_len = string_bytes.len() as u32;
        self.writer.write_all(&string_len.to_le_bytes())?;
        self.writer.write_all(string_bytes)?;

        if local_id > self.max_local_id || self.entry_count == 0 {
            self.max_local_id = local_id;
        }
        self.entry_count += 1;
        Ok(())
    }

    /// Finalize: seek back and write the real header. Validates contiguity.
    pub fn finish(mut self) -> io::Result<u64> {
        self.writer.flush()?;

        if self.entry_count > 0 && (self.max_local_id as u64) + 1 != self.entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string vocab: non-contiguous local IDs: max_local_id={}, entry_count={}",
                    self.max_local_id, self.entry_count
                ),
            ));
        }

        let mut file = self.writer.into_inner()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&STRING_MAGIC)?;
        file.write_all(&self.entry_count.to_le_bytes())?;
        file.write_all(&self.max_local_id.to_le_bytes())?; // u32 (4 bytes)
        file.write_all(&0u32.to_le_bytes())?; // padding
        file.flush()?;

        Ok(self.entry_count)
    }
}

// ============================================================================
// String vocab reader
// ============================================================================

/// A single entry read from a string vocab file.
#[derive(Debug, Clone)]
pub struct StringVocabEntry {
    pub local_id: u32,
    pub string_bytes: Vec<u8>,
}

/// Header metadata from a string vocab file.
#[derive(Debug, Clone, Copy)]
pub struct StringVocabHeader {
    pub entry_count: u64,
    pub max_local_id: u32,
}

/// Reads sorted string vocab entries from a `.voc` file.
#[derive(Debug)]
pub struct StringVocabReader {
    reader: BufReader<std::fs::File>,
    remaining: u64,
    header: StringVocabHeader,
}

impl StringVocabReader {
    /// Open and validate a string vocab file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut hdr = [0u8; HEADER_LEN];
        reader.read_exact(&mut hdr)?;

        if hdr[0..4] != STRING_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("string vocab: bad magic {:?}", &hdr[0..4]),
            ));
        }

        let entry_count = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
        let max_local_id = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
        // hdr[16..20] is padding

        if entry_count > 0 && (max_local_id as u64) + 1 != entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "string vocab: non-contiguous local IDs: max_local_id={max_local_id}, entry_count={entry_count}"
                ),
            ));
        }

        let header = StringVocabHeader {
            entry_count,
            max_local_id,
        };

        Ok(Self {
            reader,
            remaining: entry_count,
            header,
        })
    }

    /// The header metadata.
    pub fn header(&self) -> StringVocabHeader {
        self.header
    }

    /// Number of entries remaining to be read.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Read the next entry. Returns `None` when all entries have been consumed.
    pub fn next_entry(&mut self) -> io::Result<Option<StringVocabEntry>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        let mut buf = [0u8; 8]; // 4 (local_id) + 4 (string_len)
        self.reader.read_exact(&mut buf)?;

        let local_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let string_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;

        let mut string_bytes = vec![0u8; string_len];
        if string_len > 0 {
            self.reader.read_exact(&mut string_bytes)?;
        }

        self.remaining -= 1;

        Ok(Some(StringVocabEntry {
            local_id,
            string_bytes,
        }))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("fluree_vocab_file_tests");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    // ---- Subject round-trip ----

    #[test]
    fn test_subject_vocab_round_trip() {
        let path = temp_path("subj_round_trip.voc");

        // Write entries in sorted order: (ns_code ASC, suffix ASC)
        {
            let mut w = SubjectVocabWriter::new(&path).unwrap();
            w.write_entry(5, 0, b"Alice").unwrap();
            w.write_entry(5, 1, b"Bob").unwrap();
            w.write_entry(10, 2, b"Carol").unwrap();
            let count = w.finish().unwrap();
            assert_eq!(count, 3);
        }

        // Read back
        {
            let mut r = SubjectVocabReader::open(&path).unwrap();
            assert_eq!(r.header().entry_count, 3);
            assert_eq!(r.header().max_local_id, 2);
            assert_eq!(r.remaining(), 3);

            let e0 = r.next_entry().unwrap().unwrap();
            assert_eq!(e0.ns_code, 5);
            assert_eq!(e0.local_id, 0);
            assert_eq!(e0.suffix, b"Alice");

            let e1 = r.next_entry().unwrap().unwrap();
            assert_eq!(e1.ns_code, 5);
            assert_eq!(e1.local_id, 1);
            assert_eq!(e1.suffix, b"Bob");

            let e2 = r.next_entry().unwrap().unwrap();
            assert_eq!(e2.ns_code, 10);
            assert_eq!(e2.local_id, 2);
            assert_eq!(e2.suffix, b"Carol");

            assert!(r.next_entry().unwrap().is_none());
            assert_eq!(r.remaining(), 0);
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_subject_vocab_empty() {
        let path = temp_path("subj_empty.voc");

        {
            let w = SubjectVocabWriter::new(&path).unwrap();
            let count = w.finish().unwrap();
            assert_eq!(count, 0);
        }

        {
            let mut r = SubjectVocabReader::open(&path).unwrap();
            assert_eq!(r.header().entry_count, 0);
            assert!(r.next_entry().unwrap().is_none());
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_subject_vocab_non_contiguous_rejects() {
        let path = temp_path("subj_non_contig.voc");

        let mut w = SubjectVocabWriter::new(&path).unwrap();
        // local_ids 0 and 5 — gap means max_local_id(5) + 1 != entry_count(2)
        w.write_entry(1, 0, b"a").unwrap();
        w.write_entry(1, 5, b"b").unwrap();
        let err = w.finish().unwrap_err();
        assert!(
            err.to_string().contains("non-contiguous"),
            "expected non-contiguous error, got: {err}"
        );

        std::fs::remove_file(&path).ok();
    }

    // ---- String round-trip ----

    #[test]
    fn test_string_vocab_round_trip() {
        let path = temp_path("str_round_trip.voc");

        {
            let mut w = StringVocabWriter::new(&path).unwrap();
            w.write_entry(0, b"alpha").unwrap();
            w.write_entry(1, b"beta").unwrap();
            w.write_entry(2, b"gamma").unwrap();
            let count = w.finish().unwrap();
            assert_eq!(count, 3);
        }

        {
            let mut r = StringVocabReader::open(&path).unwrap();
            assert_eq!(r.header().entry_count, 3);
            assert_eq!(r.header().max_local_id, 2);

            let e0 = r.next_entry().unwrap().unwrap();
            assert_eq!(e0.local_id, 0);
            assert_eq!(e0.string_bytes, b"alpha");

            let e1 = r.next_entry().unwrap().unwrap();
            assert_eq!(e1.local_id, 1);
            assert_eq!(e1.string_bytes, b"beta");

            let e2 = r.next_entry().unwrap().unwrap();
            assert_eq!(e2.local_id, 2);
            assert_eq!(e2.string_bytes, b"gamma");

            assert!(r.next_entry().unwrap().is_none());
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_vocab_empty() {
        let path = temp_path("str_empty.voc");

        {
            let w = StringVocabWriter::new(&path).unwrap();
            let count = w.finish().unwrap();
            assert_eq!(count, 0);
        }

        {
            let mut r = StringVocabReader::open(&path).unwrap();
            assert_eq!(r.header().entry_count, 0);
            assert!(r.next_entry().unwrap().is_none());
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_vocab_non_contiguous_rejects() {
        let path = temp_path("str_non_contig.voc");

        let mut w = StringVocabWriter::new(&path).unwrap();
        w.write_entry(0, b"x").unwrap();
        w.write_entry(3, b"y").unwrap();
        let err = w.finish().unwrap_err();
        assert!(
            err.to_string().contains("non-contiguous"),
            "expected non-contiguous error, got: {err}"
        );

        std::fs::remove_file(&path).ok();
    }

    // ---- Bad magic rejection ----

    #[test]
    fn test_subject_bad_magic_rejects() {
        let path = temp_path("subj_bad_magic.voc");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"XXXX").unwrap();
            f.write_all(&[0u8; 16]).unwrap();
        }
        let err = SubjectVocabReader::open(&path).unwrap_err();
        assert!(err.to_string().contains("bad magic"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_bad_magic_rejects() {
        let path = temp_path("str_bad_magic.voc");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"XXXX").unwrap();
            f.write_all(&[0u8; 16]).unwrap();
        }
        let err = StringVocabReader::open(&path).unwrap_err();
        assert!(err.to_string().contains("bad magic"));
        std::fs::remove_file(&path).ok();
    }

    // ---- Empty suffix / empty string ----

    #[test]
    fn test_subject_empty_suffix() {
        let path = temp_path("subj_empty_suffix.voc");

        {
            let mut w = SubjectVocabWriter::new(&path).unwrap();
            w.write_entry(1, 0, b"").unwrap();
            w.finish().unwrap();
        }

        {
            let mut r = SubjectVocabReader::open(&path).unwrap();
            let e = r.next_entry().unwrap().unwrap();
            assert_eq!(e.suffix, b"");
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_string_empty_value() {
        let path = temp_path("str_empty_val.voc");

        {
            let mut w = StringVocabWriter::new(&path).unwrap();
            w.write_entry(0, b"").unwrap();
            w.finish().unwrap();
        }

        {
            let mut r = StringVocabReader::open(&path).unwrap();
            let e = r.next_entry().unwrap().unwrap();
            assert_eq!(e.string_bytes, b"");
        }

        std::fs::remove_file(&path).ok();
    }
}
