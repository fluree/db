//! Dictionary I/O: binary serialization for predicate, string, and subject dictionaries.
//!
//! ## Formats
//!
//! **PredicateDict** (`*.dict`):
//! ```text
//! magic: "FRD1" (4B)
//! count: u32
//! For each entry: len: u32, utf8_bytes: [u8; len]
//! ```
//!
//! **Forward index** (`*.idx`):
//! ```text
//! magic: "FSI1" (4B)
//! count: u32
//! offsets: [u64] × count
//! lens: [u32] × count
//! ```
//!
//! **Forward file** (`*.fwd`): concatenated raw bytes, no length prefixes.
//! Requires a corresponding index file for access.

use crate::run_index::resolve::global_dict::{LanguageTagDict, PredicateDict};
use std::io::{self, Write};
use std::path::Path;

/// Magic bytes for a predicate/graph dictionary file.
const PRED_MAGIC: [u8; 4] = *b"FRD1";

/// Magic bytes for a forward-index file (subject or string).
const INDEX_MAGIC: [u8; 4] = *b"FSI1";

// ============================================================================
// PredicateDict (and GraphDict — same format)
// ============================================================================

/// Write a `PredicateDict` (or graph dict) to a binary file.
pub fn write_predicate_dict(path: &Path, dict: &PredicateDict) -> io::Result<()> {
    let mut file = io::BufWriter::new(std::fs::File::create(path)?);
    file.write_all(&PRED_MAGIC)?;
    let count = dict.len();
    file.write_all(&count.to_le_bytes())?;
    for i in 0..count {
        let s = dict.resolve(i).unwrap();
        let bytes = s.as_bytes();
        file.write_all(&(bytes.len() as u32).to_le_bytes())?;
        file.write_all(bytes)?;
    }
    file.flush()?;
    Ok(())
}

/// Parse a `PredicateDict` (or graph dict) from a byte buffer.
pub fn read_predicate_dict_from_bytes(data: &[u8]) -> io::Result<PredicateDict> {
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "predicate dict too small",
        ));
    }
    if data[0..4] != PRED_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "predicate dict: invalid magic",
        ));
    }
    let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let mut dict = PredicateDict::new();
    let mut pos = 8;
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "predicate dict truncated",
            ));
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "predicate dict entry truncated",
            ));
        }
        let s = std::str::from_utf8(&data[pos..pos + len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("predicate dict: invalid UTF-8: {e}"),
            )
        })?;
        dict.get_or_insert(s);
        pos += len;
    }
    Ok(dict)
}

/// Read a `PredicateDict` (or graph dict) from a binary file.
pub fn read_predicate_dict(path: &Path) -> io::Result<PredicateDict> {
    read_predicate_dict_from_bytes(&std::fs::read(path)?)
}

// ============================================================================
// StringValueDict (forward file + index)
// ============================================================================

/// Write a `StringValueDict` (aliased as `PredicateDict`) as a forward file
/// (concatenated raw bytes) + index file (offsets + lengths).
pub fn write_string_dict(fwd_path: &Path, idx_path: &Path, dict: &PredicateDict) -> io::Result<()> {
    let mut fwd_file = io::BufWriter::new(std::fs::File::create(fwd_path)?);
    let count = dict.len();
    let mut offsets = Vec::with_capacity(count as usize);
    let mut lens = Vec::with_capacity(count as usize);
    let mut offset: u64 = 0;

    for i in 0..count {
        let s = dict.resolve(i).unwrap();
        let bytes = s.as_bytes();
        offsets.push(offset);
        lens.push(bytes.len() as u32);
        fwd_file.write_all(bytes)?;
        offset += bytes.len() as u64;
    }
    fwd_file.flush()?;

    write_forward_index(idx_path, &offsets, &lens)?;
    Ok(())
}

// ============================================================================
// Subject index (offset/len tables for subjects.fwd)
// ============================================================================

/// Write a subject index file (offsets + lengths for the opaque `subjects.fwd`).
///
/// Uses the same forward-index format as string dict.
pub fn write_subject_index(path: &Path, offsets: &[u64], lens: &[u32]) -> io::Result<()> {
    write_forward_index(path, offsets, lens)
}

// ============================================================================
// Subject sid64 mapping
// ============================================================================

/// Magic bytes for a subject sid mapping file.
const SID_MAP_MAGIC: [u8; 4] = *b"SSM1";

/// Write a subject sid64 mapping file: sequential insertion index → sid64.
///
/// Format: `SSM1` magic (4B) + count (u64) + `[sid64: u64] × count`.
///
/// At query time, this mapping is loaded to build a reverse lookup
/// (sid64 → sequential index) for forward file resolution.
pub fn write_subject_sid_map(path: &Path, sids: &[u64]) -> io::Result<()> {
    let mut file = io::BufWriter::new(std::fs::File::create(path)?);
    file.write_all(&SID_MAP_MAGIC)?;
    let count = sids.len() as u64;
    file.write_all(&count.to_le_bytes())?;
    for &sid in sids {
        file.write_all(&sid.to_le_bytes())?;
    }
    file.flush()?;
    Ok(())
}

/// Read a subject sid64 mapping from a byte buffer.
pub fn read_subject_sid_map_from_bytes(data: &[u8]) -> io::Result<Vec<u64>> {
    if data.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "subject sid map too small",
        ));
    }
    if data[0..4] != SID_MAP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "subject sid map: invalid magic",
        ));
    }
    let count = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
    let expected_len = 12 + count * 8;
    if data.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "subject sid map truncated: {} < {} (count={})",
                data.len(),
                expected_len,
                count
            ),
        ));
    }

    let mut sids = Vec::with_capacity(count);
    let mut pos = 12;
    for _ in 0..count {
        sids.push(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
        pos += 8;
    }
    Ok(sids)
}

/// Read a subject sid64 mapping from a file.
pub fn read_subject_sid_map(path: &Path) -> io::Result<Vec<u64>> {
    read_subject_sid_map_from_bytes(&std::fs::read(path)?)
}

// ============================================================================
// Forward index (shared format for subjects.idx and strings.idx)
// ============================================================================

/// Write a forward-index file: magic + count + offsets + lens.
fn write_forward_index(path: &Path, offsets: &[u64], lens: &[u32]) -> io::Result<()> {
    debug_assert_eq!(offsets.len(), lens.len());
    let mut file = io::BufWriter::new(std::fs::File::create(path)?);
    file.write_all(&INDEX_MAGIC)?;
    let count = offsets.len() as u32;
    file.write_all(&count.to_le_bytes())?;
    for &off in offsets {
        file.write_all(&off.to_le_bytes())?;
    }
    for &len in lens {
        file.write_all(&len.to_le_bytes())?;
    }
    file.flush()?;
    Ok(())
}

/// Parse a forward-index from a byte buffer. Returns `(offsets, lens)`.
pub fn read_forward_index_from_bytes(data: &[u8]) -> io::Result<(Vec<u64>, Vec<u32>)> {
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "forward index too small",
        ));
    }
    if data[0..4] != INDEX_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "forward index: invalid magic",
        ));
    }
    let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let expected_len = 8 + count * 8 + count * 4;
    if data.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "forward index truncated: {} < {} (count={})",
                data.len(),
                expected_len,
                count
            ),
        ));
    }

    let mut offsets = Vec::with_capacity(count);
    let mut pos = 8;
    for _ in 0..count {
        offsets.push(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
        pos += 8;
    }

    let mut lens = Vec::with_capacity(count);
    for _ in 0..count {
        lens.push(u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
        pos += 4;
    }

    Ok((offsets, lens))
}

/// Read a forward-index file. Returns `(offsets, lens)`.
pub fn read_forward_index(path: &Path) -> io::Result<(Vec<u64>, Vec<u32>)> {
    read_forward_index_from_bytes(&std::fs::read(path)?)
}

// ============================================================================
// LanguageTagDict
// ============================================================================

/// Write a `LanguageTagDict` to a binary file using the FRD1 format.
///
/// Language tag IDs are 1-based: entry at position 0 in the file = lang_id 1.
/// The sentinel (lang_id 0 = "no language tag") is not stored in the file.
///
/// Query-time decoding: `lang_id - 1` = index into the loaded array.
pub fn write_language_dict(path: &Path, dict: &LanguageTagDict) -> io::Result<()> {
    let mut file = io::BufWriter::new(std::fs::File::create(path)?);
    file.write_all(&PRED_MAGIC)?; // same FRD1 format
    let count = dict.len() as u32;
    file.write_all(&count.to_le_bytes())?;
    for (_, tag) in dict.iter() {
        let bytes = tag.as_bytes();
        file.write_all(&(bytes.len() as u32).to_le_bytes())?;
        file.write_all(bytes)?;
    }
    file.flush()?;
    Ok(())
}

/// Parse a `LanguageTagDict` from a byte buffer (FRD1 format).
///
/// Returns a LanguageTagDict where entry at position 0 = lang_id 1.
pub fn read_language_dict_from_bytes(data: &[u8]) -> io::Result<LanguageTagDict> {
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "language dict too small",
        ));
    }
    if data[0..4] != PRED_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "language dict: invalid magic",
        ));
    }
    let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let mut dict = LanguageTagDict::new();
    let mut pos = 8;
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "language dict truncated",
            ));
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "language dict entry truncated",
            ));
        }
        let s = std::str::from_utf8(&data[pos..pos + len]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("language dict: invalid UTF-8: {e}"),
            )
        })?;
        dict.get_or_insert(Some(s));
        pos += len;
    }
    Ok(dict)
}

/// Read a `LanguageTagDict` from a binary file (FRD1 format).
///
/// Returns a LanguageTagDict where entry at position 0 = lang_id 1.
pub fn read_language_dict(path: &Path) -> io::Result<LanguageTagDict> {
    read_language_dict_from_bytes(&std::fs::read(path)?)
}

// ============================================================================
// Forward entry read (mmap-based)
// ============================================================================

/// Read a single string from a memory-mapped forward file by offset and length.
///
/// Returns `Err` if the range is out of bounds. Uses `debug_assert` for
/// UTF-8 validation (we wrote the data — corruption is a debug concern).
pub fn read_forward_entry(forward_mmap: &memmap2::Mmap, offset: u64, len: u32) -> io::Result<&str> {
    let start = offset as usize;
    let end = start + len as usize;
    if end > forward_mmap.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "forward entry out of bounds: {}..{} (file len {})",
                start,
                end,
                forward_mmap.len()
            ),
        ));
    }
    let bytes = &forward_mmap[start..end];
    debug_assert!(
        std::str::from_utf8(bytes).is_ok(),
        "forward entry is not valid UTF-8 at offset {offset}",
    );
    // SAFETY: We wrote this data as valid UTF-8 strings. debug_assert catches
    // corruption during development without paying for validation in release.
    Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::rdf;

    #[test]
    fn test_predicate_dict_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_dict_io_pred");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graphs.dict");

        let mut dict = PredicateDict::new();
        dict.get_or_insert(rdf::TYPE);
        dict.get_or_insert("http://purl.org/dc/terms/title");
        dict.get_or_insert("http://xmlns.com/foaf/0.1/name");

        write_predicate_dict(&path, &dict).unwrap();
        let restored = read_predicate_dict(&path).unwrap();

        assert_eq!(restored.len(), 3);
        assert_eq!(restored.resolve(0), Some(rdf::TYPE));
        assert_eq!(restored.resolve(1), Some("http://purl.org/dc/terms/title"));
        assert_eq!(restored.resolve(2), Some("http://xmlns.com/foaf/0.1/name"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_predicate_dict_empty() {
        let dir = std::env::temp_dir().join("fluree_test_dict_io_pred_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graphs.dict");

        let dict = PredicateDict::new();
        write_predicate_dict(&path, &dict).unwrap();
        let restored = read_predicate_dict(&path).unwrap();
        assert_eq!(restored.len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_string_dict_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_dict_io_strings");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let fwd_path = dir.join("strings.fwd");
        let idx_path = dir.join("strings.idx");

        let mut dict = PredicateDict::new();
        dict.get_or_insert("Alice");
        dict.get_or_insert("Bob");
        dict.get_or_insert("Charlie");
        dict.get_or_insert("A longer string with spaces and Unicode: Ü ö ä");

        write_string_dict(&fwd_path, &idx_path, &dict).unwrap();

        // Read back the index
        let (offsets, lens) = read_forward_index(&idx_path).unwrap();
        assert_eq!(offsets.len(), 4);
        assert_eq!(lens.len(), 4);

        // Read via mmap
        let file = std::fs::File::open(&fwd_path).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };

        for i in 0..4u32 {
            let entry = read_forward_entry(&mmap, offsets[i as usize], lens[i as usize]).unwrap();
            assert_eq!(entry, dict.resolve(i).unwrap());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_subject_index_round_trip() {
        let dir = std::env::temp_dir().join("fluree_test_dict_io_subj_idx");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subjects.idx");

        let offsets = vec![0u64, 30, 55, 80];
        let lens = vec![30u32, 25, 25, 40];

        write_subject_index(&path, &offsets, &lens).unwrap();
        let (restored_offsets, restored_lens) = read_forward_index(&path).unwrap();

        assert_eq!(restored_offsets, offsets);
        assert_eq!(restored_lens, lens);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_forward_entry_out_of_bounds() {
        let dir = std::env::temp_dir().join("fluree_test_dict_io_oob");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("small.fwd");

        std::fs::write(&path, b"hello").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };

        // Valid read
        let s = read_forward_entry(&mmap, 0, 5).unwrap();
        assert_eq!(s, "hello");

        // Out of bounds
        assert!(read_forward_entry(&mmap, 0, 6).is_err());
        assert!(read_forward_entry(&mmap, 3, 5).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
