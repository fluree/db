//! Shared wire-format helpers and CID reference types used by the index root codec.
//!
//! These are format-agnostic utilities for reading/writing binary fields
//! (integers, CIDs, strings, dictionary pack refs, tree refs) and shared
//! structural types that appear in index roots.

use fluree_db_core::ContentId;
use fluree_db_core::GraphId;
use serde::{Deserialize, Serialize};
use std::io;

// ============================================================================
// CID reference types
// ============================================================================

/// CID references for a dictionary CoW tree (branch + leaves).
///
/// Mirrors `GraphOrderRefs` — a branch manifest that references
/// a set of leaf blobs. The branch holds the key-range index; leaves
/// hold the actual dictionary entries.
#[derive(Debug, Clone, PartialEq)]
pub struct DictTreeRefs {
    /// CID of the branch manifest (DTB1).
    pub branch: ContentId,
    /// CIDs of leaf blobs, ordered by leaf index.
    pub leaves: Vec<ContentId>,
}

/// A single entry in the pack branch routing table.
///
/// Maps an ID range to a pack CID. Used inline in the index root
/// to route forward dictionary lookups without an extra fetch.
#[derive(Debug, Clone, PartialEq)]
pub struct PackBranchEntry {
    pub first_id: u64,
    pub last_id: u64,
    pub pack_cid: ContentId,
}

/// Forward dictionary pack references (replaces tree-based forward dicts).
///
/// String forward packs are a flat list (global contiguous IDs).
/// Subject forward packs are grouped by namespace code (contiguous local IDs within each ns).
#[derive(Debug, Clone, PartialEq)]
pub struct DictPackRefs {
    /// String forward packs, sorted by first_id.
    pub string_fwd_packs: Vec<PackBranchEntry>,
    /// Subject forward packs, grouped by ns_code. Sorted by ns_code, then by first_id within.
    pub subject_fwd_ns_packs: Vec<(u16, Vec<PackBranchEntry>)>,
}

/// Largest routing-table count the root's `u16` wire fields can carry.
pub const PACK_COUNT_WIRE_MAX: usize = u16::MAX as usize;

impl DictPackRefs {
    /// The first count that would not fit its `u16` wire field, as
    /// `(what, len)`, or `None` when the table encodes cleanly.
    ///
    /// Callers that can still choose a different route — incremental indexing,
    /// which can defer to a full rebuild — check this BEFORE encoding, so an
    /// oversized table becomes a recoverable abort rather than the panic in
    /// [`write_dict_pack_refs`], which is the last-ditch invariant for paths
    /// with nowhere left to go.
    pub fn wire_count_overflow(&self) -> Option<(&'static str, usize)> {
        if self.string_fwd_packs.len() > PACK_COUNT_WIRE_MAX {
            return Some(("string", self.string_fwd_packs.len()));
        }
        if self.subject_fwd_ns_packs.len() > PACK_COUNT_WIRE_MAX {
            return Some(("subject namespace", self.subject_fwd_ns_packs.len()));
        }
        self.subject_fwd_ns_packs
            .iter()
            .find(|(_, packs)| packs.len() > PACK_COUNT_WIRE_MAX)
            .map(|(_, packs)| ("subject", packs.len()))
    }
}

// ============================================================================
// GC chain types (prev_index / garbage)
// ============================================================================

/// Reference to the previous index root in the GC chain.
///
/// The garbage collector walks this chain backwards to determine which roots
/// (and their associated CAS artifacts) are eligible for deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryPrevIndexRef {
    /// `index_t` of the previous root.
    pub t: i64,
    /// CID of the previous root blob.
    pub id: ContentId,
}

/// Reference to this root's garbage manifest.
///
/// The garbage manifest lists CIDs that were replaced when building
/// this root from the previous one. The GC collector reads this to know which
/// objects to delete when this root ages out of the retention window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryGarbageRef {
    /// CID of the garbage record JSON blob.
    pub id: ContentId,
}

// ============================================================================
// Dictionary and arena reference types
// ============================================================================

/// Dictionary refs (graph-independent dict artifacts only).
///
/// Forward dictionaries use packed FPK1 format (DictPackRefs).
/// Reverse dictionaries use CoW tree format (DictTreeRefs).
/// Per-graph specialty arenas (numbig, vectors, spatial) live in
/// `GraphArenaRefs` on the root, not here.
#[derive(Debug, Clone)]
pub struct DictRefs {
    /// Forward dictionary packs (string + subject, FPK1 format).
    pub forward_packs: DictPackRefs,
    /// Subject reverse tree: [ns_code BE][suffix] → sid64 (ns-compressed).
    pub subject_reverse: DictTreeRefs,
    /// String reverse tree: value → string_id.
    pub string_reverse: DictTreeRefs,
}

/// Per-graph specialty arena refs (numbig, vectors, spatial).
///
/// One entry per graph that has any specialty arenas.
#[derive(Debug, Clone)]
pub struct GraphArenaRefs {
    pub g_id: GraphId,
    /// Per-predicate numbig arenas, sorted by p_id.
    pub numbig: Vec<(u32, ContentId)>,
    /// Per-predicate vector arenas, sorted by p_id.
    pub vectors: Vec<VectorDictRef>,
    /// Per-predicate spatial index refs, sorted by p_id.
    pub spatial: Vec<SpatialArenaRef>,
    /// Per-predicate fulltext BoW arena refs, sorted by p_id.
    pub fulltext: Vec<FulltextArenaRef>,
}

/// Vector arena ref with u32 p_id key.
#[derive(Debug, Clone)]
pub struct VectorDictRef {
    pub p_id: u32,
    pub manifest: ContentId,
    pub shards: Vec<ContentId>,
}

/// Spatial index ref for one (graph, predicate) pair.
#[derive(Debug, Clone)]
pub struct SpatialArenaRef {
    pub p_id: u32,
    /// CID of the serialized `SpatialIndexRoot` JSON blob (contains SpatialConfig, hashes, etc.).
    pub root_cid: ContentId,
    /// CID of the cell index manifest.
    pub manifest: ContentId,
    /// CID of the geometry arena.
    pub arena: ContentId,
    /// CIDs of all leaflet chunks (for GC).
    pub leaflets: Vec<ContentId>,
}

/// Fulltext arena ref for one (graph, predicate, language) triple.
///
/// `lang_id` is the ordinary dict-assigned ID for the bucket's BCP-47 tag.
/// `@fulltext`-datatype arenas and config-driven English content both resolve
/// to the lang_id assigned to `"en"` and therefore share a single arena.
/// `lang_id` is stable per ledger because the language dict is append-only.
#[derive(Debug, Clone)]
pub struct FulltextArenaRef {
    pub p_id: u32,
    pub lang_id: u16,
    /// CID of the FTA1 blob.
    pub arena_cid: ContentId,
}

// ============================================================================
// Wire format helpers
// ============================================================================

pub(crate) fn io_err(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

pub(crate) fn ensure_bytes(data: &[u8], pos: usize, need: usize, ctx: &str) -> io::Result<()> {
    if pos + need > data.len() {
        Err(io_err(&format!(
            "root: truncated at {ctx} (need {need} at offset {pos}, have {})",
            data.len()
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn read_u8_at(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    ensure_bytes(data, *pos, 1, "u8")?;
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

pub(crate) fn read_u16_at(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    ensure_bytes(data, *pos, 2, "u16")?;
    let v = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

pub(crate) fn read_u32_at(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    ensure_bytes(data, *pos, 4, "u32")?;
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

pub(crate) fn read_u64_at(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    ensure_bytes(data, *pos, 8, "u64")?;
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

pub(crate) fn read_i64_at(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    ensure_bytes(data, *pos, 8, "i64")?;
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

/// Write a CID as `cid_len:u16(LE) + cid_bytes`.
pub(crate) fn write_cid(buf: &mut Vec<u8>, cid: &ContentId) {
    let cid_bytes = cid.to_bytes();
    buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&cid_bytes);
}

/// Read a CID from `cid_len:u16(LE) + cid_bytes`.
pub(crate) fn read_cid(data: &[u8], pos: &mut usize) -> io::Result<ContentId> {
    let cid_len = read_u16_at(data, pos)? as usize;
    ensure_bytes(data, *pos, cid_len, "cid bytes")?;
    let cid = ContentId::from_bytes(&data[*pos..*pos + cid_len])
        .map_err(|e| io_err(&format!("invalid CID: {e}")))?;
    *pos += cid_len;
    Ok(cid)
}

/// Write a length-prefixed UTF-8 string as `len:u16(LE) + bytes`.
///
/// # Panics
/// Debug-asserts that the string fits in a u16 length prefix (64 KiB).
pub(crate) fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    debug_assert!(
        bytes.len() <= u16::MAX as usize,
        "write_str: string length {} exceeds u16::MAX",
        bytes.len()
    );
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Read a length-prefixed UTF-8 string.
pub(crate) fn read_string(data: &[u8], pos: &mut usize) -> io::Result<String> {
    let len = read_u16_at(data, pos)? as usize;
    ensure_bytes(data, *pos, len, "string bytes")?;
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| io_err(&format!("invalid UTF-8: {e}")))?
        .to_string();
    *pos += len;
    Ok(s)
}

/// Write a string array as `count:u16(LE) + [len:u16 + bytes]...`.
///
/// # Panics
/// Debug-asserts that the array length fits in a u16 count.
pub(crate) fn write_string_array(buf: &mut Vec<u8>, strings: &[String]) {
    debug_assert!(
        strings.len() <= u16::MAX as usize,
        "write_string_array: count {} exceeds u16::MAX",
        strings.len()
    );
    buf.extend_from_slice(&(strings.len() as u16).to_le_bytes());
    for s in strings {
        write_str(buf, s);
    }
}

/// Read a string array.
pub(crate) fn read_string_array(data: &[u8], pos: &mut usize) -> io::Result<Vec<String>> {
    let count = read_u16_at(data, pos)? as usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(read_string(data, pos)?);
    }
    Ok(result)
}

/// Write forward dictionary pack refs (FPK1 packs).
///
/// Wire format:
/// ```text
/// [string_fwd_pack_count: u16 LE]
///   For each: [first_id: u64] [last_id: u64] [pack_cid: len_prefixed]
/// [subject_fwd_ns_count: u16 LE]
///   For each ns: [ns_code: u16] [pack_count: u16]
///     For each: [first_id: u64] [last_id: u64] [pack_cid: len_prefixed]
/// ```
pub(crate) fn write_dict_pack_refs(buf: &mut Vec<u8>, packs: &DictPackRefs) {
    // String forward packs
    buf.extend_from_slice(&pack_count_u16(packs.string_fwd_packs.len(), "string").to_le_bytes());
    for entry in &packs.string_fwd_packs {
        buf.extend_from_slice(&entry.first_id.to_le_bytes());
        buf.extend_from_slice(&entry.last_id.to_le_bytes());
        write_cid(buf, &entry.pack_cid);
    }

    // Subject forward packs (per namespace)
    let mut sorted_ns = packs.subject_fwd_ns_packs.clone();
    sorted_ns.sort_by_key(|(ns_code, _)| *ns_code);
    buf.extend_from_slice(&pack_count_u16(sorted_ns.len(), "subject namespace").to_le_bytes());
    for (ns_code, ns_packs) in &sorted_ns {
        buf.extend_from_slice(&ns_code.to_le_bytes());
        buf.extend_from_slice(&pack_count_u16(ns_packs.len(), "subject").to_le_bytes());
        for entry in ns_packs {
            buf.extend_from_slice(&entry.first_id.to_le_bytes());
            buf.extend_from_slice(&entry.last_id.to_le_bytes());
            write_cid(buf, &entry.pack_cid);
        }
    }
}

/// Pack counts are `u16` on the wire while the routing table itself is
/// unbounded, so an unchecked cast would wrap and silently drop routing entries
/// — a root that reads back cleanly having lost ID ranges.
///
/// Panicking aborts the in-progress build instead. Roots are immutable and
/// published last, so the previously published index stays intact and readable;
/// a failed build is recoverable, a truncated routing table is not.
fn pack_count_u16(len: usize, what: &str) -> u16 {
    u16::try_from(len).unwrap_or_else(|_| {
        panic!(
            "index root: {what} forward pack count {len} exceeds the u16 wire limit of {}; \
             tail compaction should keep this far below the cap",
            u16::MAX
        )
    })
}

/// Read forward dictionary pack refs.
pub(crate) fn read_dict_pack_refs(data: &[u8], pos: &mut usize) -> io::Result<DictPackRefs> {
    // String forward packs
    let str_count = read_u16_at(data, pos)? as usize;
    let mut string_fwd_packs = Vec::with_capacity(str_count);
    for _ in 0..str_count {
        let first_id = read_u64_at(data, pos)?;
        let last_id = read_u64_at(data, pos)?;
        let pack_cid = read_cid(data, pos)?;
        string_fwd_packs.push(PackBranchEntry {
            first_id,
            last_id,
            pack_cid,
        });
    }

    // Subject forward packs (per namespace)
    let ns_count = read_u16_at(data, pos)? as usize;
    let mut subject_fwd_ns_packs = Vec::with_capacity(ns_count);
    for _ in 0..ns_count {
        let ns_code = read_u16_at(data, pos)?;
        let pack_count = read_u16_at(data, pos)? as usize;
        let mut ns_packs = Vec::with_capacity(pack_count);
        for _ in 0..pack_count {
            let first_id = read_u64_at(data, pos)?;
            let last_id = read_u64_at(data, pos)?;
            let pack_cid = read_cid(data, pos)?;
            ns_packs.push(PackBranchEntry {
                first_id,
                last_id,
                pack_cid,
            });
        }
        subject_fwd_ns_packs.push((ns_code, ns_packs));
    }

    Ok(DictPackRefs {
        string_fwd_packs,
        subject_fwd_ns_packs,
    })
}

/// Write dict tree refs: branch CID + leaf_count:u32 + leaf CIDs.
pub(crate) fn write_dict_tree_refs(buf: &mut Vec<u8>, tree: &DictTreeRefs) {
    write_cid(buf, &tree.branch);
    buf.extend_from_slice(&(tree.leaves.len() as u32).to_le_bytes());
    for leaf_cid in &tree.leaves {
        write_cid(buf, leaf_cid);
    }
}

/// Read dict tree refs: branch CID + leaf_count:u32 + leaf CIDs.
pub(crate) fn read_dict_tree_refs(data: &[u8], pos: &mut usize) -> io::Result<DictTreeRefs> {
    let branch = read_cid(data, pos)?;
    let leaf_count = read_u32_at(data, pos)? as usize;
    let mut leaves = Vec::with_capacity(leaf_count);
    for _ in 0..leaf_count {
        leaves.push(read_cid(data, pos)?);
    }
    Ok(DictTreeRefs { branch, leaves })
}

#[cfg(test)]
mod wire_count_tests {
    use super::*;

    fn entries(n: usize) -> Vec<PackBranchEntry> {
        let cid = ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_DICT_BLOB,
            &fluree_db_core::sha256_hex(b"pack"),
        )
        .unwrap();
        (0..n)
            .map(|i| PackBranchEntry {
                first_id: i as u64,
                last_id: i as u64,
                pack_cid: cid.clone(),
            })
            .collect()
    }

    #[test]
    fn a_table_at_the_wire_limit_encodes_and_one_past_it_does_not() {
        // The boundary is what matters: `pack_count_u16` accepts exactly
        // u16::MAX, so the pre-encode check must not abort a build that would
        // have encoded fine.
        let at_limit = DictPackRefs {
            string_fwd_packs: entries(PACK_COUNT_WIRE_MAX),
            subject_fwd_ns_packs: Vec::new(),
        };
        assert_eq!(at_limit.wire_count_overflow(), None, "u16::MAX still fits");

        let over = DictPackRefs {
            string_fwd_packs: entries(PACK_COUNT_WIRE_MAX + 1),
            subject_fwd_ns_packs: Vec::new(),
        };
        assert_eq!(
            over.wire_count_overflow(),
            Some(("string", PACK_COUNT_WIRE_MAX + 1))
        );
    }

    #[test]
    fn every_u16_count_in_the_table_is_checked() {
        // Three separate u16 fields are written: the string count, the
        // namespace count, and each namespace's own pack count. A check that
        // covered only the first would let the other two reach the panic.
        let ns_overflow = DictPackRefs {
            string_fwd_packs: Vec::new(),
            subject_fwd_ns_packs: (0..=PACK_COUNT_WIRE_MAX)
                .map(|i| (i as u16, Vec::new()))
                .collect(),
        };
        assert_eq!(
            ns_overflow.wire_count_overflow(),
            Some(("subject namespace", PACK_COUNT_WIRE_MAX + 1))
        );

        let per_ns_overflow = DictPackRefs {
            string_fwd_packs: Vec::new(),
            subject_fwd_ns_packs: vec![(0, entries(2)), (1, entries(PACK_COUNT_WIRE_MAX + 1))],
        };
        assert_eq!(
            per_ns_overflow.wire_count_overflow(),
            Some(("subject", PACK_COUNT_WIRE_MAX + 1)),
            "an oversized namespace is found even when earlier ones are fine"
        );
    }

    #[test]
    fn an_ordinary_table_reports_no_overflow() {
        let ordinary = DictPackRefs {
            string_fwd_packs: entries(1200),
            subject_fwd_ns_packs: vec![(0, entries(64)), (7, entries(19_629))],
        };
        assert_eq!(ordinary.wire_count_overflow(), None);
    }
}
