//! Reverse leaf format: variable-length key → numeric ID.
//!
//! Used by both subject reverse (`(ns_code, suffix)` → sid64) and string
//! reverse (`value` → string_id) trees. Entries are sorted by key for
//! binary search.
//!
//! ## Binary layout (`DLR1`)
//!
//! ```text
//! [magic: 4B "DLR1"]
//! [entry_count: u32]
//! [offset_table: u32 × entry_count]  // byte offset of each entry in data section
//! [data section: entries...]
//!   entry := [key_len: u32] [key_bytes: u8 × key_len] [id: u64]
//! ```
//!
//! ## Front-coded layout (`DLR2`)
//!
//! ```text
//! [magic: 4B "DLR2"]
//! [entry_count: u32]
//! [restart_count: u32]
//! [flags: u8]                          // bit 0: ids stored namespace-relative
//! [restart_table: u32 × restart_count] // byte offset of each restart entry in data section
//! [data section: entries...]
//!   entry := [shared: varint] [unshared_len: varint] [unshared: u8 × unshared_len] [id: varint]
//! ```
//!
//! Each key is stored as the bytes it does not share with the key before it.
//! Every [`FRONT_CODED_RESTART_INTERVAL`]th entry is a restart: `shared` is 0,
//! so its key is whole and the restart table can binary-search it. A lookup
//! scans at most one restart interval. With namespace-relative ids, an id is
//! stored XORed with the key's namespace code in its top 16 bits, which for a
//! subject leaves just its local id.
//!
//! Readers accept either layout, so a tree may mix them.
//!
//! ## Key Format
//!
//! For **string reverse** trees, the key is the raw UTF-8 value bytes.
//! Sorted in UTF-8 byte order.
//!
//! For **subject reverse** trees, the key is `[ns_code: u16 BE] [suffix: UTF-8]`.
//! The big-endian ns_code prefix ensures lexicographic byte comparison
//! matches the logical `(ns_code, suffix)` ordering.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::io;

use fluree_db_core::DictKind;

use super::key_scheme::KeyScheme;
use super::varint::{decode_varint, encode_varint};

/// Magic bytes for a reverse leaf file.
pub const REVERSE_LEAF_MAGIC: [u8; 4] = *b"DLR1";

/// Magic bytes for a front-coded reverse leaf.
pub const FRONT_CODED_LEAF_MAGIC: [u8; 4] = *b"DLR2";

/// Entries from one front-coded restart to the next: the most a lookup scans.
pub const FRONT_CODED_RESTART_INTERVAL: usize = 16;

/// Header size: magic (4) + entry_count (4).
const HEADER_SIZE: usize = 8;

/// Front-coded header: magic (4) + entry_count (4) + restart_count (4) + flags (1).
const FC_HEADER_SIZE: usize = 13;

const FLAG_NS_RELATIVE_IDS: u8 = 1;

/// Fixed overhead per entry: key_len (4) + id (8).
const ENTRY_OVERHEAD: usize = 12;

/// How a reverse tree's leaves are written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeafFormat {
    /// `DLR1`: every key whole, one offset per entry.
    Plain,
    /// `DLR2`: keys front-coded, ids as varints.
    FrontCoded { ns_relative_ids: bool },
}

impl LeafFormat {
    /// The format a reverse tree of `dict` keyed by `scheme` is written in.
    /// Subject keys are a namespace code and a local name, which neighbours
    /// share most of. Raw string keys share too little for front coding to
    /// pay for the scan it adds to a lookup, but hashed string keys are
    /// short, and their ids gain from varints.
    pub fn for_tree(dict: DictKind, scheme: KeyScheme) -> Self {
        match (dict, scheme) {
            (DictKind::SubjectReverse, _) => Self::FrontCoded {
                ns_relative_ids: true,
            },
            (DictKind::StringReverse, KeyScheme::PrefixHash { .. }) => Self::FrontCoded {
                ns_relative_ids: false,
            },
            _ => Self::Plain,
        }
    }

    /// Encode sorted `entries` as one leaf.
    pub fn encode(self, entries: &[ReverseEntry]) -> Vec<u8> {
        match self {
            Self::Plain => encode_reverse_leaf(entries),
            Self::FrontCoded { ns_relative_ids } => {
                let mut writer = FrontCodedLeafWriter::new(ns_relative_ids);
                for e in entries {
                    writer.push(&e.key, e.id);
                }
                writer.finish()
            }
        }
    }

    /// A writer that streams sorted entries into leaves of this format.
    pub fn writer(self) -> LeafWriter {
        LeafWriter(match self {
            Self::Plain => LeafWriterInner::Plain(Vec::new()),
            Self::FrontCoded { ns_relative_ids } => {
                LeafWriterInner::FrontCoded(FrontCodedLeafWriter::new(ns_relative_ids))
            }
        })
    }
}

/// Streams sorted entries into leaves of one [`LeafFormat`], one leaf per
/// [`Self::finish`].
pub struct LeafWriter(LeafWriterInner);

enum LeafWriterInner {
    Plain(Vec<ReverseEntry>),
    FrontCoded(FrontCodedLeafWriter),
}

impl LeafWriter {
    pub fn entry_count(&self) -> u32 {
        match &self.0 {
            LeafWriterInner::Plain(entries) => entries.len() as u32,
            LeafWriterInner::FrontCoded(writer) => writer.entry_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    /// Append an entry. Keys must arrive strictly ascending.
    pub fn push(&mut self, key: &[u8], id: u64) {
        match &mut self.0 {
            LeafWriterInner::Plain(entries) => entries.push(ReverseEntry {
                key: key.to_vec(),
                id,
            }),
            LeafWriterInner::FrontCoded(writer) => writer.push(key, id),
        }
    }

    /// The leaf's bytes. The writer is left empty, ready for the next leaf.
    pub fn finish(&mut self) -> Vec<u8> {
        match &mut self.0 {
            LeafWriterInner::Plain(entries) => encode_reverse_leaf(&std::mem::take(entries)),
            LeafWriterInner::FrontCoded(writer) => writer.finish(),
        }
    }
}

/// The key's namespace code in an id's top 16 bits; 0 for a key too short
/// to have one. Applied by XOR, so any id round-trips.
#[inline]
fn ns_bits(key: &[u8]) -> u64 {
    match key {
        [a, b, ..] => u64::from(u16::from_be_bytes([*a, *b])) << 48,
        _ => 0,
    }
}

#[inline]
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Streams sorted entries into a front-coded (`DLR2`) leaf.
pub struct FrontCodedLeafWriter {
    ns_relative_ids: bool,
    entry_count: u32,
    restarts: Vec<u32>,
    data: Vec<u8>,
    prev: Vec<u8>,
}

impl FrontCodedLeafWriter {
    pub fn new(ns_relative_ids: bool) -> Self {
        Self {
            ns_relative_ids,
            entry_count: 0,
            restarts: Vec::new(),
            data: Vec::new(),
            prev: Vec::new(),
        }
    }

    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Append an entry. Keys must arrive strictly ascending.
    pub fn push(&mut self, key: &[u8], id: u64) {
        debug_assert!(
            self.entry_count == 0 || self.prev.as_slice() < key,
            "front-coded leaf keys must be strictly ascending"
        );
        let shared = if (self.entry_count as usize).is_multiple_of(FRONT_CODED_RESTART_INTERVAL) {
            self.restarts.push(self.data.len() as u32);
            0
        } else {
            common_prefix_len(&self.prev, key)
        };
        encode_varint(shared as u64, &mut self.data);
        encode_varint((key.len() - shared) as u64, &mut self.data);
        self.data.extend_from_slice(&key[shared..]);
        let stored = if self.ns_relative_ids {
            id ^ ns_bits(key)
        } else {
            id
        };
        encode_varint(stored, &mut self.data);
        self.prev.truncate(shared);
        self.prev.extend_from_slice(&key[shared..]);
        self.entry_count += 1;
    }

    /// The leaf's bytes. The writer is left empty, ready for the next leaf.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(FC_HEADER_SIZE + self.restarts.len() * 4 + self.data.len());
        buf.extend_from_slice(&FRONT_CODED_LEAF_MAGIC);
        buf.extend_from_slice(&self.entry_count.to_le_bytes());
        buf.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        buf.push(if self.ns_relative_ids {
            FLAG_NS_RELATIVE_IDS
        } else {
            0
        });
        for offset in &self.restarts {
            buf.extend_from_slice(&offset.to_le_bytes());
        }
        buf.extend_from_slice(&self.data);
        self.entry_count = 0;
        self.restarts.clear();
        self.data.clear();
        self.prev.clear();
        buf
    }
}

/// A single reverse leaf entry (key → id).
#[derive(Debug, Clone)]
pub struct ReverseEntry {
    /// For string reverse: raw UTF-8 value.
    /// For subject reverse: `[ns_code: 2B BE] [suffix bytes]`.
    pub key: Vec<u8>,
    pub id: u64,
}

/// A sparse index of a leaf for lookups that read it by range: for each
/// `spacing`-byte stretch of the leaf in which an entry starts, the first
/// entry starting there. Keys are packed end to end.
pub struct Fences {
    keys: Box<[u8]>,
    key_ends: Box<[u32]>,
    /// Per fence: its entry's index and byte offset in the leaf.
    entries: Box<[(u32, u32)]>,
}

/// The entries a key can be among: from a fence's entry up to the next
/// fence's, or to the end of the leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fenced {
    pub index: u32,
    pub offset: u64,
    pub end_index: u32,
    pub end_offset: u64,
}

impl Fences {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn key(&self, fence: usize) -> &[u8] {
        let start = fence
            .checked_sub(1)
            .map_or(0, |i| self.key_ends[i] as usize);
        &self.keys[start..self.key_ends[fence] as usize]
    }

    pub fn index(&self, fence: usize) -> u32 {
        self.entries[fence].0
    }

    pub fn offset(&self, fence: usize) -> u32 {
        self.entries[fence].1
    }

    /// Heap bytes held.
    pub fn heap_bytes(&self) -> usize {
        self.keys.len() + self.key_ends.len() * 4 + self.entries.len() * 8
    }

    /// The fenced entries `key` is among, in a leaf of `entry_count`
    /// entries and `len` bytes; `None` when it sorts before the first.
    pub fn locate(&self, key: &[u8], entry_count: u32, len: u64) -> Option<Fenced> {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let fence = lo.checked_sub(1)?;
        let (end_index, end_offset) = self
            .entries
            .get(lo)
            .map_or((entry_count, len), |&(i, o)| (i, u64::from(o)));
        Some(Fenced {
            index: self.index(fence),
            offset: u64::from(self.offset(fence)),
            end_index,
            end_offset,
        })
    }
}

/// Build a subject reverse key from (ns_code, suffix).
///
/// Encodes ns_code as big-endian u16 prefix so lexicographic byte
/// comparison matches `(ns_code, suffix)` ordering.
pub fn subject_reverse_key(ns_code: u16, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + suffix.len());
    key.extend_from_slice(&ns_code.to_be_bytes());
    key.extend_from_slice(suffix);
    key
}

/// Extract (ns_code, suffix) from a subject reverse key.
pub fn split_subject_reverse_key(key: &[u8]) -> (u16, &[u8]) {
    debug_assert!(key.len() >= 2);
    let ns_code = u16::from_be_bytes(key[0..2].try_into().unwrap());
    (ns_code, &key[2..])
}

/// Encode a sorted slice of reverse entries into a leaf blob.
///
/// Entries **must** be sorted by `key` in ascending byte order.
pub fn encode_reverse_leaf(entries: &[ReverseEntry]) -> Vec<u8> {
    let entry_count = entries.len() as u32;

    let data_size: usize = entries.iter().map(|e| ENTRY_OVERHEAD + e.key.len()).sum();
    let offset_table_size = entries.len() * 4;
    let total = HEADER_SIZE + offset_table_size + data_size;

    let mut buf = Vec::with_capacity(total);

    // Header
    buf.extend_from_slice(&REVERSE_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());

    // Offset table
    let mut offset: u32 = 0;
    for e in entries {
        buf.extend_from_slice(&offset.to_le_bytes());
        offset += (ENTRY_OVERHEAD + e.key.len()) as u32;
    }

    // Data section
    for e in entries {
        buf.extend_from_slice(&(e.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.key);
        buf.extend_from_slice(&e.id.to_le_bytes());
    }

    debug_assert_eq!(buf.len(), total);
    buf
}

/// A front-coded leaf's shape, from its header.
#[derive(Clone, Copy)]
struct FrontCoded {
    restart_count: usize,
    ns_relative_ids: bool,
}

/// One front-coded entry as stored.
struct FcEntry<'a> {
    shared: usize,
    unshared: &'a [u8],
    stored: u64,
}

/// Walks a front-coded leaf's entries in order, rebuilding each key.
struct FcCursor {
    pos: usize,
    key: Vec<u8>,
}

impl FcCursor {
    fn at(pos: usize) -> Self {
        Self {
            pos,
            key: Vec::new(),
        }
    }

    /// Step to the next entry, leaving its key in `self.key`; its id.
    fn advance(&mut self, leaf: &ReverseLeaf<'_>, fc: FrontCoded) -> u64 {
        let e = leaf.fc_entry(&mut self.pos);
        debug_assert!(e.shared <= self.key.len());
        self.key.truncate(e.shared);
        self.key.extend_from_slice(e.unshared);
        fc.id(&self.key, e.stored)
    }
}

impl FrontCoded {
    #[inline]
    fn id(self, key: &[u8], stored: u64) -> u64 {
        if self.ns_relative_ids {
            stored ^ ns_bits(key)
        } else {
            stored
        }
    }
}

fn invalid_leaf(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("reverse leaf: {what}"))
}

/// Decoded reverse leaf providing O(log n) lookup by key.
pub struct ReverseLeaf<'a> {
    data: &'a [u8],
    entry_count: u32,
    /// `DLR1`: the per-entry offset table. `DLR2`: the restart table.
    offset_table_start: usize,
    data_section_start: usize,
    front_coded: Option<FrontCoded>,
}

impl<'a> ReverseLeaf<'a> {
    /// Parse a reverse leaf from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(invalid_leaf("too small for header"));
        }
        let entry_count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let (front_coded, offset_table_start, table_len) = if data[0..4] == REVERSE_LEAF_MAGIC {
            (None, HEADER_SIZE, entry_count as usize)
        } else if data[0..4] == FRONT_CODED_LEAF_MAGIC {
            if data.len() < FC_HEADER_SIZE {
                return Err(invalid_leaf("too small for header"));
            }
            let restart_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
            if (restart_count == 0) != (entry_count == 0) {
                return Err(invalid_leaf("restart table does not match entry count"));
            }
            let front_coded = FrontCoded {
                restart_count,
                ns_relative_ids: data[12] & FLAG_NS_RELATIVE_IDS != 0,
            };
            (Some(front_coded), FC_HEADER_SIZE, restart_count)
        } else {
            return Err(invalid_leaf("invalid magic"));
        };
        let data_section_start = offset_table_start + table_len * 4;

        if data.len() < data_section_start {
            return Err(invalid_leaf("truncated offset table"));
        }

        Ok(Self {
            data,
            entry_count,
            offset_table_start,
            data_section_start,
            front_coded,
        })
    }

    /// Number of entries in this leaf.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Whether this leaf is front-coded (`DLR2`).
    pub fn is_front_coded(&self) -> bool {
        self.front_coded.is_some()
    }

    /// The front-coded entry at `*pos`, advancing past it. A truncated entry
    /// panics, as an out-of-range `DLR1` offset does.
    #[inline]
    fn fc_entry(&self, pos: &mut usize) -> FcEntry<'a> {
        self.try_fc_entry(pos)
            .expect("reverse leaf: truncated front-coded entry")
    }

    fn try_fc_entry(&self, pos: &mut usize) -> io::Result<FcEntry<'a>> {
        let shared = decode_varint(self.data, pos)? as usize;
        let len = decode_varint(self.data, pos)? as usize;
        let unshared = self
            .data
            .get(*pos..)
            .and_then(|rest| rest.get(..len))
            .ok_or_else(|| invalid_leaf("truncated front-coded entry"))?;
        *pos += len;
        let stored = decode_varint(self.data, pos)?;
        Ok(FcEntry {
            shared,
            unshared,
            stored,
        })
    }

    /// Restart `r`'s key, stored whole.
    fn restart_key(&self, r: usize) -> &'a [u8] {
        let mut pos = self.entry_offset(r);
        let entry = self.fc_entry(&mut pos);
        debug_assert_eq!(entry.shared, 0, "a restart entry shares nothing");
        entry.unshared
    }

    /// Where restart `r`'s entries end.
    fn block_end(&self, fc: FrontCoded, r: usize) -> usize {
        if r + 1 < fc.restart_count {
            self.entry_offset(r + 1)
        } else {
            self.data.len()
        }
    }

    /// The restart block `key` sorts into: the last whose first key is at or
    /// below it. `None` when it sorts before the leaf's first key.
    fn block_for(&self, fc: FrontCoded, key: &[u8]) -> Option<usize> {
        let (mut lo, mut hi) = (0, fc.restart_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.restart_key(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.checked_sub(1)
    }

    /// Exact lookup within the key's restart block, without rebuilding keys:
    /// `matched` tracks how much of `target` the previous key shares.
    fn fc_lookup(&self, fc: FrontCoded, target: &[u8]) -> Option<u64> {
        let block = self.block_for(fc, target)?;
        let mut pos = self.entry_offset(block);
        let end = self.block_end(fc, block);
        let mut matched = 0usize;
        while pos < end {
            let entry = self.fc_entry(&mut pos);
            match entry.shared.cmp(&matched) {
                // Diverges from `target` where the previous key did, so it
                // still sorts below it.
                Ordering::Greater => {}
                // Leaves the previous key upward inside the part that
                // matched `target`: past it.
                Ordering::Less => return None,
                Ordering::Equal => {
                    let rest = &target[matched..];
                    let common = common_prefix_len(entry.unshared, rest);
                    matched += common;
                    match (entry.unshared.get(common), rest.get(common)) {
                        (None, None) => return Some(fc.id(target, entry.stored)),
                        (None, Some(_)) => {}
                        (Some(_), None) => return None,
                        (Some(k), Some(t)) if k > t => return None,
                        (Some(_), Some(_)) => {}
                    }
                }
            }
        }
        None
    }

    /// Byte offset of entry `index` within `self.data` — of restart `index`
    /// in a front-coded leaf.
    #[inline]
    fn entry_offset(&self, index: usize) -> usize {
        let table_pos = self.offset_table_start + index * 4;
        let relative =
            u32::from_le_bytes(self.data[table_pos..table_pos + 4].try_into().unwrap()) as usize;
        self.data_section_start + relative
    }

    /// Read the key bytes at the given index.
    #[inline]
    fn key_at(&self, index: usize) -> &'a [u8] {
        let offset = self.entry_offset(index);
        let key_len =
            u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        &self.data[offset + 4..offset + 4 + key_len]
    }

    /// Read the ID at the given index.
    #[inline]
    fn id_at(&self, index: usize) -> u64 {
        let offset = self.entry_offset(index);
        let key_len =
            u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        let id_offset = offset + 4 + key_len;
        u64::from_le_bytes(self.data[id_offset..id_offset + 8].try_into().unwrap())
    }

    /// Look up an ID by exact key match using binary search.
    pub fn lookup(&self, target_key: &[u8]) -> Option<u64> {
        if self.entry_count == 0 {
            return None;
        }
        if let Some(fc) = self.front_coded {
            return self.fc_lookup(fc, target_key);
        }

        let mut lo = 0usize;
        let mut hi = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key_at(mid);
            match mid_key.cmp(target_key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(self.id_at(mid)),
            }
        }

        None
    }

    /// Whether every entry starts where the one before it ends, with keys
    /// strictly ascending: the layout a fenced scan reads. Never for a
    /// front-coded leaf, which is read whole.
    pub fn is_sequential(&self) -> bool {
        if self.front_coded.is_some() {
            return false;
        }
        let mut expected = self.data_section_start;
        let mut prev: Option<&[u8]> = None;
        for index in 0..self.entry_count as usize {
            let offset = self.entry_offset(index);
            if offset != expected || offset + 4 > self.data.len() {
                return false;
            }
            let key_len =
                u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
            let end = offset + ENTRY_OVERHEAD + key_len;
            if end > self.data.len() {
                return false;
            }
            let key = &self.data[offset + 4..offset + 4 + key_len];
            if prev.is_some_and(|p| p >= key) {
                return false;
            }
            prev = Some(key);
            expected = end;
        }
        expected == self.data.len()
    }

    /// The leaf's [`Fences`] at `spacing` bytes, or `None` when its layout
    /// is not one a fenced scan reads ([`Self::is_sequential`]), as no
    /// front-coded leaf's is.
    pub fn fences(&self, spacing: usize) -> Option<Fences> {
        if !self.is_sequential() {
            return None;
        }
        let (mut keys, mut key_ends, mut entries) = (Vec::new(), Vec::new(), Vec::new());
        let mut last: Option<usize> = None;
        for index in 0..self.entry_count as usize {
            let offset = self.entry_offset(index);
            if last.is_none_or(|l| l / spacing != offset / spacing) {
                keys.extend_from_slice(self.key_at(index));
                key_ends.push(keys.len() as u32);
                entries.push((index as u32, offset as u32));
                last = Some(offset);
            }
        }
        Some(Fences {
            keys: keys.into(),
            key_ends: key_ends.into(),
            entries: entries.into(),
        })
    }

    /// [`Self::lookup`] narrowed by `fences` to the entries from the last
    /// fence at or before the key to the next fence, read forward from that
    /// fence's entry through the data section alone: the reference for a
    /// fenced leaf's scan. Reports each byte range it reads.
    #[cfg(test)]
    fn lookup_scanning(
        &self,
        fences: &Fences,
        target_key: &[u8],
        read: &mut impl FnMut(std::ops::Range<usize>),
    ) -> Option<u64> {
        let fenced = fences.locate(target_key, self.entry_count, self.data.len() as u64)?;
        let mut offset = fenced.offset as usize;
        for _ in fenced.index..fenced.end_index {
            let key_len =
                u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
            read(offset..offset + 4 + key_len);
            let key = &self.data[offset + 4..offset + 4 + key_len];
            match key.cmp(target_key) {
                std::cmp::Ordering::Less => offset += ENTRY_OVERHEAD + key_len,
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Equal => {
                    read(offset + 4 + key_len..offset + ENTRY_OVERHEAD + key_len);
                    return Some(u64::from_le_bytes(
                        self.data[offset + 4 + key_len..offset + ENTRY_OVERHEAD + key_len]
                            .try_into()
                            .unwrap(),
                    ));
                }
            }
        }
        None
    }

    /// Return the first key in this leaf (for branch boundary keys).
    pub fn first_key(&self) -> Option<Cow<'a, [u8]>> {
        if self.entry_count == 0 {
            return None;
        }
        Some(Cow::Borrowed(match self.front_coded {
            Some(_) => self.restart_key(0),
            None => self.key_at(0),
        }))
    }

    /// Return the last key in this leaf.
    pub fn last_key(&self) -> Option<Cow<'a, [u8]>> {
        if self.entry_count == 0 {
            return None;
        }
        let Some(fc) = self.front_coded else {
            return Some(Cow::Borrowed(self.key_at(self.entry_count as usize - 1)));
        };
        let mut cursor = FcCursor::at(self.entry_offset(fc.restart_count - 1));
        while cursor.pos < self.data.len() {
            cursor.advance(self, fc);
        }
        Some(Cow::Owned(cursor.key))
    }

    /// Iterate all entries in order. A front-coded leaf's keys are rebuilt,
    /// so they come owned.
    pub fn iter(&'a self) -> ReverseLeafIter<'a> {
        ReverseLeafIter {
            leaf: self,
            index: 0,
            cursor: FcCursor::at(self.data_section_start),
        }
    }

    /// Entries with `start_key <= key < end_key`, in key order. A
    /// front-coded leaf's keys come back owned.
    pub fn scan_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<(Cow<'a, [u8]>, u64)> {
        let mut results = Vec::new();
        self.walk_range(start_key, end_key, |key, id| {
            results.push((
                match key {
                    RangeKey::Leaf(key) => Cow::Borrowed(key),
                    RangeKey::Built(key) => Cow::Owned(key.to_vec()),
                },
                id,
            ));
        });
        results
    }

    /// Visit the IDs with `start_key <= key < end_key`, in key order,
    /// without copying the keys rebuilt by a front-coded scan.
    pub fn visit_range_ids(&self, start_key: &[u8], end_key: &[u8], mut visit: impl FnMut(u64)) {
        self.walk_range(start_key, end_key, |_, id| visit(id));
    }

    /// How many entries have `start_key <= key < end_key`, copying no key.
    pub fn count_range(&self, start_key: &[u8], end_key: &[u8]) -> usize {
        let mut count = 0;
        self.walk_range(start_key, end_key, |_, _| count += 1);
        count
    }

    /// Visit the entries with `start_key <= key < end_key`, in key order.
    fn walk_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        mut visit: impl FnMut(RangeKey<'a, '_>, u64),
    ) {
        if self.entry_count == 0 {
            return;
        }

        if let Some(fc) = self.front_coded {
            let block = self.block_for(fc, start_key).unwrap_or(0);
            let mut cursor = FcCursor::at(self.entry_offset(block));
            while cursor.pos < self.data.len() {
                let id = cursor.advance(self, fc);
                if cursor.key.as_slice() < start_key {
                    continue;
                }
                if cursor.key.as_slice() >= end_key {
                    break;
                }
                visit(RangeKey::Built(&cursor.key), id);
            }
            return;
        }

        // Binary search for the first entry >= start_key.
        let n = self.entry_count as usize;
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key_at(mid) < start_key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        for i in lo..n {
            let key = self.key_at(i);
            if key >= end_key {
                break;
            }
            visit(RangeKey::Leaf(key), self.id_at(i));
        }
    }
}

/// A key a range walk visits: in the leaf's bytes, or rebuilt by a
/// front-coded walk.
enum RangeKey<'a, 'k> {
    Leaf(&'a [u8]),
    Built(&'k [u8]),
}

/// Iterator over reverse leaf entries.
pub struct ReverseLeafIter<'a> {
    leaf: &'a ReverseLeaf<'a>,
    index: usize,
    cursor: FcCursor,
}

impl<'a> Iterator for ReverseLeafIter<'a> {
    /// (key, id)
    type Item = (Cow<'a, [u8]>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.leaf.entry_count as usize {
            return None;
        }
        let item = match self.leaf.front_coded {
            Some(fc) => {
                let id = self.cursor.advance(self.leaf, fc);
                (Cow::Owned(self.cursor.key.clone()), id)
            }
            None => (
                Cow::Borrowed(self.leaf.key_at(self.index)),
                self.leaf.id_at(self.index),
            ),
        };
        self.index += 1;
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fenced lookups find exactly what a full lookup finds — present and
    /// absent keys, keys before, between and after the entries, long keys
    /// and keys sharing long prefixes — for fences at several unit sizes.
    #[test]
    fn fenced_lookups_agree_with_the_full_lookup() {
        let prefix = "p".repeat(300);
        let mut entries: Vec<ReverseEntry> = (0..3_000u64)
            .map(|i| ReverseEntry {
                key: match i % 3 {
                    0 => format!("{prefix}{:06}", i * 2),
                    1 => format!("k{:06}", i * 2),
                    _ => format!("long-{:06}-{}", i * 2, "z".repeat((i % 700) as usize)),
                }
                .into_bytes(),
                id: i,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
        let mut probes: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();
        for e in &entries {
            let mut absent = e.key.clone();
            absent.push(b'!');
            probes.push(absent);
            probes.push(e.key[..e.key.len() - 1].to_vec());
        }
        probes.extend([
            b"".to_vec(),
            b"a".to_vec(),
            b"zzzz".to_vec(),
            prefix.clone().into_bytes(),
        ]);
        let keys: Vec<&[u8]> = entries.iter().map(|e| e.key.as_slice()).collect();
        let entry_end = |i: usize| leaf.entry_offset(i) + ENTRY_OVERHEAD + keys[i].len();
        for unit in [512usize, 4096, 65536] {
            let fences = leaf.fences(unit).unwrap();
            assert!((1..fences.len()).all(|i| fences.key(i - 1) < fences.key(i)));
            assert!(
                (0..fences.len()).all(|i| fences.key(i) == keys[fences.index(i) as usize]
                    && leaf.entry_offset(fences.index(i) as usize) == fences.offset(i) as usize)
            );
            assert!(
                fences.len() as u64 >= blob.len().div_ceil(unit) as u64 / 2,
                "{unit}"
            );
            for key in &probes {
                let want = leaf.lookup(key);
                let mut scanned = Vec::new();
                let scan = leaf.lookup_scanning(&fences, key, &mut |r| scanned.push(r));
                assert_eq!(scan, want, "{unit}");
                // Reads stay in the key's fenced stretch: from its fence to
                // the first entry at or past the key.
                let Some(at) = (0..fences.len()).rposition(|i| fences.key(i) <= key.as_slice())
                else {
                    assert!(scanned.is_empty());
                    continue;
                };
                let end = if at + 1 < fences.len() {
                    fences.index(at + 1) as usize
                } else {
                    keys.len()
                };
                let first = fences.index(at) as usize;
                let stop = keys.partition_point(|k| *k < key.as_slice()).min(end - 1);
                let (lo, hi) = (leaf.entry_offset(first), entry_end(stop));
                assert!(
                    scanned.iter().all(|r| r.start >= lo && r.end <= hi),
                    "scan {unit}"
                );
            }
        }
    }

    #[test]
    fn test_string_reverse_round_trip() {
        let mut entries: Vec<ReverseEntry> = vec![
            ReverseEntry {
                key: b"alpha".to_vec(),
                id: 100,
            },
            ReverseEntry {
                key: b"beta".to_vec(),
                id: 200,
            },
            ReverseEntry {
                key: b"gamma".to_vec(),
                id: 300,
            },
        ];
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.entry_count(), 3);
        assert_eq!(leaf.lookup(b"alpha"), Some(100));
        assert_eq!(leaf.lookup(b"beta"), Some(200));
        assert_eq!(leaf.lookup(b"gamma"), Some(300));
        assert_eq!(leaf.lookup(b"delta"), None);
    }

    #[test]
    fn test_subject_reverse_key_ordering() {
        // ns_code 2 < ns_code 3, and within same ns_code, suffix sorts
        let k1 = subject_reverse_key(2, b"aaa");
        let k2 = subject_reverse_key(2, b"bbb");
        let k3 = subject_reverse_key(3, b"aaa");

        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_subject_reverse_round_trip() {
        let mut entries = vec![
            ReverseEntry {
                key: subject_reverse_key(2, b"Alice"),
                id: 2u64 << 48,
            },
            ReverseEntry {
                key: subject_reverse_key(2, b"Bob"),
                id: (2u64 << 48) | 1,
            },
            ReverseEntry {
                key: subject_reverse_key(3, b"Carol"),
                id: 3u64 << 48,
            },
        ];
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let key = subject_reverse_key(2, b"Bob");
        assert_eq!(leaf.lookup(&key), Some((2u64 << 48) | 1));

        let miss = subject_reverse_key(2, b"Dave");
        assert_eq!(leaf.lookup(&miss), None);
    }

    #[test]
    fn test_split_subject_key() {
        let key = subject_reverse_key(42, b"hello");
        let (ns, suffix) = split_subject_reverse_key(&key);
        assert_eq!(ns, 42);
        assert_eq!(suffix, b"hello");
    }

    #[test]
    fn test_empty_leaf() {
        let blob = encode_reverse_leaf(&[]);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
        assert_eq!(leaf.entry_count(), 0);
        assert!(leaf.first_key().is_none());
        assert!(leaf.lookup(b"anything").is_none());
    }

    #[test]
    fn test_first_last_key() {
        let entries = vec![
            ReverseEntry {
                key: b"aaa".to_vec(),
                id: 1,
            },
            ReverseEntry {
                key: b"zzz".to_vec(),
                id: 2,
            },
        ];
        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.first_key().as_deref(), Some(b"aaa".as_slice()));
        assert_eq!(leaf.last_key().as_deref(), Some(b"zzz".as_slice()));
    }

    #[test]
    fn test_iterator() {
        let entries = vec![
            ReverseEntry {
                key: b"a".to_vec(),
                id: 10,
            },
            ReverseEntry {
                key: b"b".to_vec(),
                id: 20,
            },
            ReverseEntry {
                key: b"c".to_vec(),
                id: 30,
            },
        ];
        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let collected: Vec<_> = leaf.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (Cow::Borrowed(b"a".as_slice()), 10));
        assert_eq!(collected[1], (Cow::Borrowed(b"b".as_slice()), 20));
        assert_eq!(collected[2], (Cow::Borrowed(b"c".as_slice()), 30));
    }

    /// Subject-shaped entries: several namespaces, numbered and named local
    /// names, an empty local name, keys that are prefixes of others, long
    /// overflow-style IRIs, and ids whose namespace bits do not match their
    /// key (which namespace-relative storage must still round-trip).
    fn subject_like_entries(n: u64) -> Vec<ReverseEntry> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut entries: Vec<ReverseEntry> = (0..n)
            .map(|i| {
                let ns = [3u16, 17, 0x00FF, 0xFFFE][(next() % 4) as usize];
                let suffix = match i % 7 {
                    0 => format!("Offer{}", next() % 100_000),
                    1 => format!("Offer{}/", next() % 100_000),
                    2 => format!("Review{}", next() % 1_000_000),
                    3 => format!(
                        "http://www.example.org/vendor{}/Product{}",
                        i % 40,
                        next() % 9_000
                    ),
                    4 => "Offer".to_string(),
                    5 => String::new(),
                    _ => format!("é-{}", next() % 500),
                };
                let id = if i % 11 == 0 {
                    next()
                } else {
                    fluree_db_core::subject_id::SubjectId::new(ns, next() % (1 << 40)).as_u64()
                };
                ReverseEntry {
                    key: subject_reverse_key(ns, suffix.as_bytes()),
                    id,
                }
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries.dedup_by(|a, b| a.key == b.key);
        entries
    }

    /// A front-coded leaf answers every lookup, iteration and range scan the
    /// way the plain leaf of the same entries does — present keys, near
    /// misses on both sides, keys before and after the leaf — for both id
    /// modes and for entry counts on and off a restart boundary.
    #[test]
    fn front_coded_leaves_agree_with_plain_leaves() {
        for n in [0u64, 1, 2, 15, 16, 17, 33, 1_000, 5_000] {
            let entries = subject_like_entries(n);
            let plain_blob = encode_reverse_leaf(&entries);
            let plain = ReverseLeaf::from_bytes(&plain_blob).unwrap();
            for ns_relative_ids in [true, false] {
                let blob = LeafFormat::FrontCoded { ns_relative_ids }.encode(&entries);
                let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
                assert!(leaf.is_front_coded());
                assert!(!leaf.is_sequential());
                assert!(leaf.fences(4096).is_none(), "not fenced");
                assert_eq!(leaf.entry_count(), plain.entry_count());
                assert_eq!(leaf.first_key(), plain.first_key(), "{n}");
                assert_eq!(leaf.last_key(), plain.last_key(), "{n}");
                assert!(leaf.iter().eq(plain.iter()), "iter {n}");

                let mut probes: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0xFF; 3]];
                for e in &entries {
                    probes.push(e.key.clone());
                    let mut longer = e.key.clone();
                    longer.push(0);
                    probes.push(longer);
                    probes.push(e.key[..e.key.len() - 1].to_vec());
                    let mut bumped = e.key.clone();
                    if let Some(last) = bumped.last_mut() {
                        *last = last.wrapping_add(1);
                    }
                    probes.push(bumped);
                }
                for probe in &probes {
                    assert_eq!(leaf.lookup(probe), plain.lookup(probe), "{n} {probe:?}");
                }
                for w in probes.windows(2).step_by(7) {
                    let (lo, hi) = if w[0] <= w[1] {
                        (&w[0], &w[1])
                    } else {
                        (&w[1], &w[0])
                    };
                    assert_eq!(leaf.scan_range(lo, hi), plain.scan_range(lo, hi), "{n}");
                    let mut ids = Vec::new();
                    leaf.visit_range_ids(lo, hi, |id| ids.push(id));
                    let expected: Vec<_> = entries
                        .iter()
                        .filter(|e| e.key >= *lo && e.key < *hi)
                        .map(|e| e.id)
                        .collect();
                    assert_eq!(ids, expected, "IDs, n={n}, ns_relative={ns_relative_ids}");
                }
                assert_eq!(
                    leaf.scan_range(&[], &[0xFF; 3]),
                    plain.scan_range(&[], &[0xFF; 3])
                );
            }
        }
    }

    /// One writer cuts consecutive leaves of its format: `finish` leaves
    /// nothing of the previous leaf behind, so each leaf is the one
    /// [`LeafFormat::encode`] writes for its entries alone (a front-coded
    /// leaf's first key stored whole).
    #[test]
    fn a_leaf_writer_starts_each_leaf_fresh() {
        let entries = subject_like_entries(100);
        let (a, b) = entries.split_at(37);
        for format in [
            LeafFormat::Plain,
            LeafFormat::FrontCoded {
                ns_relative_ids: true,
            },
            LeafFormat::FrontCoded {
                ns_relative_ids: false,
            },
        ] {
            let mut writer = format.writer();
            for e in a {
                writer.push(&e.key, e.id);
            }
            assert_eq!(writer.entry_count(), a.len() as u32);
            let first = writer.finish();
            assert!(writer.is_empty());
            for e in b {
                writer.push(&e.key, e.id);
            }
            let second = writer.finish();
            for (blob, part) in [(first, a), (second, b)] {
                assert_eq!(blob, format.encode(part), "{format:?}");
                let leaf = ReverseLeaf::from_bytes(&blob).unwrap();
                let got: Vec<_> = leaf.iter().map(|(k, id)| (k.into_owned(), id)).collect();
                let want: Vec<_> = part.iter().map(|e| (e.key.clone(), e.id)).collect();
                assert_eq!(got, want, "{format:?}");
            }
        }
    }

    /// Subject keys under one namespace share most of their bytes and their
    /// ids are small local numbers, which is what front coding exploits.
    #[test]
    fn front_coded_subject_leaves_are_smaller() {
        let entries: Vec<ReverseEntry> = (0..10_000u64)
            .map(|i| ReverseEntry {
                key: subject_reverse_key(21, format!("Offer{i:07}").as_bytes()),
                id: fluree_db_core::subject_id::SubjectId::new(21, i).as_u64(),
            })
            .collect();
        let plain = encode_reverse_leaf(&entries).len();
        let front_coded = LeafFormat::for_tree(DictKind::SubjectReverse, KeyScheme::Raw)
            .encode(&entries)
            .len();
        assert!(front_coded * 3 < plain, "{front_coded} vs {plain}");
    }

    #[test]
    fn subject_and_hashed_string_trees_are_front_coded() {
        let hashed = KeyScheme::PrefixHash {
            prefix_len: 48,
            cap: 32,
        };
        assert_eq!(
            LeafFormat::for_tree(DictKind::SubjectReverse, KeyScheme::Raw),
            LeafFormat::FrontCoded {
                ns_relative_ids: true
            }
        );
        assert_eq!(
            LeafFormat::for_tree(DictKind::StringReverse, hashed),
            LeafFormat::FrontCoded {
                ns_relative_ids: false
            }
        );
        assert_eq!(
            LeafFormat::for_tree(DictKind::StringReverse, KeyScheme::Raw),
            LeafFormat::Plain
        );
        assert_eq!(
            LeafFormat::for_tree(DictKind::TermReverse, KeyScheme::Raw),
            LeafFormat::Plain
        );
    }

    #[test]
    fn a_truncated_front_coded_header_is_refused() {
        let blob = LeafFormat::FrontCoded {
            ns_relative_ids: true,
        }
        .encode(&subject_like_entries(40));
        assert!(ReverseLeaf::from_bytes(&blob[..10]).is_err());
        assert!(ReverseLeaf::from_bytes(&blob[..FC_HEADER_SIZE + 2]).is_err());
    }

    #[test]
    fn test_large_reverse_leaf() {
        let mut entries: Vec<ReverseEntry> = (0..5000)
            .map(|i| ReverseEntry {
                key: format!("http://example.org/entity/{i:06}").into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let blob = encode_reverse_leaf(&entries);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.entry_count(), 5000);

        // Spot-check
        let key = format!("http://example.org/entity/{:06}", 2500).into_bytes();
        assert_eq!(leaf.lookup(&key), Some(2500));

        let miss = b"http://example.org/nonexistent".to_vec();
        assert_eq!(leaf.lookup(&miss), None);
    }

    /// Regression test: reverse tree entries for subjects must store the full
    /// SubjectId (ns_code + local_id encoded via `SubjectId::new().as_u64()`),
    /// not just the raw local_id. Using raw local_id loses the namespace and
    /// breaks multi-pattern query JOINs that translate Sids back to s_ids via
    /// the reverse tree.
    #[test]
    fn test_subject_reverse_stores_full_subject_id() {
        use fluree_db_core::subject_id::SubjectId;

        let ns_code: u16 = 12;
        let local_id: u64 = 2;
        let suffix = b"person2";

        // Build entry the CORRECT way — full SubjectId encoding
        let sid = SubjectId::new(ns_code, local_id);
        let entry = ReverseEntry {
            key: subject_reverse_key(ns_code, suffix),
            id: sid.as_u64(),
        };

        let blob = encode_reverse_leaf(&[entry]);
        let leaf = ReverseLeaf::from_bytes(&blob).unwrap();

        let lookup_key = subject_reverse_key(ns_code, suffix);
        let found_id = leaf.lookup(&lookup_key).expect("entry should be found");

        // The stored ID must decode back to the original (ns_code, local_id)
        let decoded = SubjectId::from_u64(found_id);
        assert_eq!(decoded.ns_code(), ns_code);
        assert_eq!(decoded.local_id(), local_id);

        // Critically: the raw local_id alone is NOT the same as the full sid64
        assert_ne!(
            found_id, local_id,
            "must store full sid64, not raw local_id"
        );
        assert_eq!(found_id, sid.as_u64());
    }
}
