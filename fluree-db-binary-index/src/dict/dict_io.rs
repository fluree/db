//! Dictionary I/O: binary deserialization for predicate, string, and subject dictionaries.
//!
//! Read-side functions for loading dictionaries from binary files. Write-side
//! functions remain in `fluree-db-indexer`.
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
//! offsets: [u64] x count
//! lens: [u32] x count
//! ```
//!
//! **Forward file** (`*.fwd`): concatenated raw bytes, no length prefixes.
//! Requires a corresponding index file for access.

use super::global_dict::{LanguageTagDict, PredicateDict};
use std::io;
use std::path::Path;

/// Magic bytes for a predicate/graph dictionary file.
const PRED_MAGIC: [u8; 4] = *b"FRD1";

/// Magic bytes for a forward-index file (subject or string).
const INDEX_MAGIC: [u8; 4] = *b"FSI1";

/// Magic bytes for a subject sid mapping file.
const SID_MAP_MAGIC: [u8; 4] = *b"SSM1";

// ============================================================================
// PredicateDict (and GraphDict -- same format)
// ============================================================================

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
// Subject sid64 mapping
// ============================================================================

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
/// UTF-8 validation (we wrote the data -- corruption is a debug concern).
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
