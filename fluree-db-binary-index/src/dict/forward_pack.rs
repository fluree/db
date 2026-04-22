//! Forward dictionary pack format: large immutable blobs containing many pages.
//!
//! Replaces the per-leaf CAS object model (DLF1) with packed pages inside
//! large (~256 MiB) immutable pack files. IDs within a page are **contiguous**,
//! enabling O(1) value lookup via an offsets-of-next layout.
//!
//! ## Pack wire format (`FPK1`)
//!
//! ```text
//! Header (40 bytes):
//!   [magic: 4B "FPK1"] [version: u8=1] [kind: u8] [ns_code: u16 LE]
//!   [first_id: u64 LE] [last_id: u64 LE]
//!   [page_count: u32 LE] [page_dir_offset: u64 LE] [reserved: u32=0]
//!
//! Pages (concatenated):
//!   page_0 ... page_N
//!
//! Page Directory (at page_dir_offset, 20 bytes per entry):
//!   [page_first_id: u64 LE] [entry_count: u32 LE]
//!   [page_offset: u32 LE] [page_len: u32 LE]
//! ```
//!
//! ## Page wire format (offsets-of-next)
//!
//! ```text
//! [entry_count: u32 LE]
//! [offsets: u32 LE × (entry_count + 1)]
//! [data: concatenated value bytes]
//! ```
//!
//! Lookup: `local = id - page_first_id; value = data[offsets[local]..offsets[local+1]]`

use std::io;

// ============================================================================
// Constants
// ============================================================================

/// Magic bytes identifying a forward pack file.
pub const PACK_MAGIC: [u8; 4] = *b"FPK1";

/// Current wire format version.
pub const PACK_VERSION: u8 = 1;

/// Fixed header size in bytes.
pub const PACK_HEADER_SIZE: usize = 40;

/// Size of a single page directory entry in bytes.
pub const PAGE_DIR_ENTRY_SIZE: usize = 20;

/// Pack kind: string forward dictionary.
pub const KIND_STRING_FWD: u8 = 0;

/// Pack kind: subject forward dictionary (one pack per namespace).
pub const KIND_SUBJECT_FWD: u8 = 1;

// ============================================================================
// Pack header
// ============================================================================

/// Decoded pack header.
#[derive(Debug, Clone, Copy)]
pub struct PackHeader {
    pub version: u8,
    pub kind: u8,
    pub ns_code: u16,
    pub first_id: u64,
    pub last_id: u64,
    pub page_count: u32,
    pub page_dir_offset: u64,
}

impl PackHeader {
    fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < PACK_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack header: need {} bytes, got {}",
                    PACK_HEADER_SIZE,
                    data.len()
                ),
            ));
        }
        if data[0..4] != PACK_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("pack header: expected magic FPK1, got {:?}", &data[0..4]),
            ));
        }
        let version = data[4];
        if version != PACK_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("pack header: unsupported version {version}"),
            ));
        }
        Ok(Self {
            version,
            kind: data[5],
            ns_code: u16::from_le_bytes([data[6], data[7]]),
            first_id: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            last_id: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            page_count: u32::from_le_bytes(data[24..28].try_into().unwrap()),
            page_dir_offset: u64::from_le_bytes(data[28..36].try_into().unwrap()),
        })
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&PACK_MAGIC);
        buf.push(self.version);
        buf.push(self.kind);
        buf.extend_from_slice(&self.ns_code.to_le_bytes());
        buf.extend_from_slice(&self.first_id.to_le_bytes());
        buf.extend_from_slice(&self.last_id.to_le_bytes());
        buf.extend_from_slice(&self.page_count.to_le_bytes());
        buf.extend_from_slice(&self.page_dir_offset.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
    }
}

// ============================================================================
// Page directory entry
// ============================================================================

/// Decoded page directory entry (20 bytes on disk).
#[derive(Debug, Clone, Copy)]
pub struct PageDirEntry {
    pub page_first_id: u64,
    pub entry_count: u32,
    pub page_offset: u32,
    pub page_len: u32,
}

impl PageDirEntry {
    fn decode(data: &[u8]) -> Self {
        Self {
            page_first_id: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            entry_count: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            page_offset: u32::from_le_bytes(data[12..16].try_into().unwrap()),
            page_len: u32::from_le_bytes(data[16..20].try_into().unwrap()),
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.page_first_id.to_le_bytes());
        buf.extend_from_slice(&self.entry_count.to_le_bytes());
        buf.extend_from_slice(&self.page_offset.to_le_bytes());
        buf.extend_from_slice(&self.page_len.to_le_bytes());
    }
}

// ============================================================================
// Page reader (zero-copy over borrowed bytes)
// ============================================================================

/// A single page within a pack. Provides O(1) value lookup by ID.
///
/// Page layout: `[entry_count: u32][offsets: u32×(N+1)][data]`
#[derive(Debug)]
pub struct ForwardPage<'a> {
    data: &'a [u8],
    entry_count: u32,
    /// Byte offset within `data` where the offsets array starts (immediately after entry_count).
    offsets_start: usize,
    /// Byte offset within `data` where the concatenated values start.
    data_start: usize,
    /// The first ID covered by this page (from the directory entry).
    first_id: u64,
}

impl<'a> ForwardPage<'a> {
    /// Parse a page from raw bytes. `first_id` comes from the page directory.
    pub fn from_bytes(data: &'a [u8], first_id: u64) -> io::Result<Self> {
        if data.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page: too small for entry_count",
            ));
        }
        let entry_count = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let offsets_start = 4;
        let offsets_bytes = (entry_count as usize + 1) * 4;
        let data_start = offsets_start + offsets_bytes;

        if data.len() < data_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "page: need {} bytes for offsets, got {}",
                    data_start,
                    data.len()
                ),
            ));
        }

        // Validate: offsets must be monotonically non-decreasing and in bounds.
        let data_section_len = data.len() - data_start;
        let mut prev_off = 0u32;
        for i in 0..=entry_count as usize {
            let pos = offsets_start + i * 4;
            let off = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            if off < prev_off {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "page: offset[{}]={} < offset[{}]={}",
                        i,
                        off,
                        i - 1,
                        prev_off
                    ),
                ));
            }
            prev_off = off;
        }
        // Final offset must match data section length.
        let final_offset = u32::from_le_bytes(
            data[offsets_start + entry_count as usize * 4
                ..offsets_start + entry_count as usize * 4 + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if final_offset != data_section_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "page: final offset {final_offset} != data section length {data_section_len}"
                ),
            ));
        }

        Ok(Self {
            data,
            entry_count,
            offsets_start,
            data_start,
            first_id,
        })
    }

    /// Number of entries in this page.
    #[inline]
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Last ID covered by this page (inclusive).
    #[inline]
    pub fn last_id(&self) -> u64 {
        if self.entry_count == 0 {
            self.first_id
        } else {
            self.first_id + self.entry_count as u64 - 1
        }
    }

    /// Read the offset at position `i` in the offsets array.
    #[inline]
    fn offset_at(&self, i: usize) -> u32 {
        let pos = self.offsets_start + i * 4;
        u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap())
    }

    /// Look up a value by ID. Returns the raw value bytes, or `None` if out of range.
    #[inline]
    pub fn lookup(&self, id: u64) -> Option<&'a [u8]> {
        if id < self.first_id {
            return None;
        }
        let local = (id - self.first_id) as usize;
        if local >= self.entry_count as usize {
            return None;
        }
        let start = self.data_start + self.offset_at(local) as usize;
        let end = self.data_start + self.offset_at(local + 1) as usize;
        Some(&self.data[start..end])
    }

    /// Append value bytes to `out`. Returns `true` if found.
    #[inline]
    pub fn lookup_into(&self, id: u64, out: &mut Vec<u8>) -> bool {
        match self.lookup(id) {
            Some(bytes) => {
                out.extend_from_slice(bytes);
                true
            }
            None => false,
        }
    }
}

// ============================================================================
// Parsed pack metadata (shared between ForwardPack and pack_reader)
// ============================================================================

/// Pre-parsed pack metadata: header fields + decoded page directory.
///
/// Constructed once at load time and reused for every lookup, avoiding
/// per-lookup `Vec<PageDirEntry>` allocations.
#[derive(Debug)]
pub(crate) struct ParsedPackMeta {
    pub first_id: u64,
    pub last_id: u64,
    pub kind: u8,
    pub ns_code: u16,
    pub directory: Vec<PageDirEntry>,
}

/// Parse and validate a pack's header + page directory from raw bytes.
///
/// Rejects:
/// - Bad magic / unsupported version
/// - Directory out of bounds
/// - Pages out of bounds
/// - `entry_count == 0` pages (prevents underflow in ID range math)
/// - Pages too small for their declared entry count
/// - Non-ascending / overlapping page ID ranges
pub(crate) fn parse_pack_meta(data: &[u8]) -> io::Result<ParsedPackMeta> {
    let header = PackHeader::decode(data)?;

    let dir_offset = header.page_dir_offset as usize;
    let dir_size = header.page_count as usize * PAGE_DIR_ENTRY_SIZE;
    let dir_end = dir_offset
        .checked_add(dir_size)
        .ok_or_else(|| io::Error::other("pack: directory offset overflow"))?;

    if dir_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack: page directory at offset {} (size {}) exceeds pack length {}",
                dir_offset,
                dir_size,
                data.len()
            ),
        ));
    }

    let mut directory = Vec::with_capacity(header.page_count as usize);
    for i in 0..header.page_count as usize {
        let entry_offset = dir_offset + i * PAGE_DIR_ENTRY_SIZE;
        let entry = PageDirEntry::decode(&data[entry_offset..entry_offset + PAGE_DIR_ENTRY_SIZE]);

        // Reject empty pages — entry_count==0 causes underflow in ID range math.
        if entry.entry_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("pack: page {i} has entry_count 0"),
            ));
        }

        // Structural minimum: entry_count(4) + offsets((N+1)*4).
        let min_page_len = 4 + (entry.entry_count as usize + 1) * 4;
        if (entry.page_len as usize) < min_page_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack: page {} len {} < structural minimum {}",
                    i, entry.page_len, min_page_len
                ),
            ));
        }

        // Validate page bounds.
        let page_end = entry.page_offset as usize + entry.page_len as usize;
        if page_end > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack: page {} at offset {} (len {}) exceeds pack length {}",
                    i,
                    entry.page_offset,
                    entry.page_len,
                    data.len()
                ),
            ));
        }

        // Validate directory ordering: non-overlapping, ascending ID ranges.
        if i > 0 {
            let prev: &PageDirEntry = &directory[i - 1];
            let prev_last = prev.page_first_id + prev.entry_count as u64 - 1;
            if entry.page_first_id <= prev_last {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "pack: page {} first_id {} overlaps with page {} range [{}..{}]",
                        i,
                        entry.page_first_id,
                        i - 1,
                        prev.page_first_id,
                        prev_last
                    ),
                ));
            }
        }

        directory.push(entry);
    }

    Ok(ParsedPackMeta {
        first_id: header.first_id,
        last_id: header.last_id,
        kind: header.kind,
        ns_code: header.ns_code,
        directory,
    })
}

/// Zero-alloc lookup using pre-parsed metadata. Returns raw value bytes.
///
/// Binary-searches the cached directory, then does O(1) offset indexing
/// within the matching page.
pub(crate) fn lookup_in_pack<'a>(
    data: &'a [u8],
    meta: &ParsedPackMeta,
    id: u64,
) -> Option<&'a [u8]> {
    if id < meta.first_id || id > meta.last_id {
        return None;
    }
    let page_idx = find_page_in_dir(&meta.directory, id)?;
    let dir = &meta.directory[page_idx];
    let page_data =
        &data[dir.page_offset as usize..dir.page_offset as usize + dir.page_len as usize];
    let page = ForwardPage::from_bytes_unchecked(page_data, dir.page_first_id);
    page.lookup(id)
}

/// Binary search a page directory for the page containing `id`.
fn find_page_in_dir(directory: &[PageDirEntry], id: u64) -> Option<usize> {
    let idx = directory.partition_point(|d| d.page_first_id <= id);
    if idx == 0 {
        return None;
    }
    let candidate = idx - 1;
    let dir = &directory[candidate];
    let page_last_id = dir.page_first_id + dir.entry_count as u64 - 1;
    if id <= page_last_id {
        Some(candidate)
    } else {
        None
    }
}

// ============================================================================
// Pack reader (zero-copy over borrowed bytes)
// ============================================================================

/// A forward dictionary pack. Borrows the underlying bytes (works with mmap or `&[u8]`).
#[derive(Debug)]
pub struct ForwardPack<'a> {
    data: &'a [u8],
    header: PackHeader,
    /// Cached decoded directory entries.
    directory: Vec<PageDirEntry>,
}

impl<'a> ForwardPack<'a> {
    /// Parse and validate a pack from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> io::Result<Self> {
        let header = PackHeader::decode(data)?;
        let meta = parse_pack_meta(data)?;
        Ok(Self {
            data,
            header,
            directory: meta.directory,
        })
    }

    /// The decoded pack header.
    pub fn header(&self) -> &PackHeader {
        &self.header
    }

    /// Number of pages in this pack.
    pub fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Total entries across all pages.
    pub fn total_entries(&self) -> u64 {
        self.directory.iter().map(|d| d.entry_count as u64).sum()
    }

    /// Look up a value by ID. Returns raw value bytes, or `None` if not found.
    pub fn lookup(&self, id: u64) -> Option<&'a [u8]> {
        if id < self.header.first_id || id > self.header.last_id {
            return None;
        }
        let page_idx = find_page_in_dir(&self.directory, id)?;
        let dir = &self.directory[page_idx];
        let page_data =
            &self.data[dir.page_offset as usize..dir.page_offset as usize + dir.page_len as usize];
        let page = ForwardPage::from_bytes_unchecked(page_data, dir.page_first_id);
        page.lookup(id)
    }

    /// Append value bytes to `out`. Returns `true` if found.
    pub fn lookup_into(&self, id: u64, out: &mut Vec<u8>) -> bool {
        match self.lookup(id) {
            Some(bytes) => {
                out.extend_from_slice(bytes);
                true
            }
            None => false,
        }
    }

    /// Look up and return as a UTF-8 string.
    pub fn lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        match self.lookup(id) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(s.to_string()))
            }
            None => Ok(None),
        }
    }
}

impl<'a> ForwardPage<'a> {
    /// Lightweight parse that skips validation (used internally by `ForwardPack`
    /// and `lookup_in_pack` which already validated directory bounds).
    fn from_bytes_unchecked(data: &'a [u8], first_id: u64) -> Self {
        let entry_count = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let offsets_start = 4;
        let data_start = offsets_start + (entry_count as usize + 1) * 4;
        Self {
            data,
            entry_count,
            offsets_start,
            data_start,
            first_id,
        }
    }
}

// ============================================================================
// Encoder
// ============================================================================

/// Encode a forward pack from sorted, contiguous entries.
///
/// `entries` must be sorted by ID in ascending order and IDs must be contiguous
/// (no gaps). Returns the complete pack bytes.
///
/// Uses a two-pass approach: first determines page boundaries, then encodes
/// directly into the output buffer with no intermediate per-page allocations.
pub fn encode_forward_pack(
    entries: &[(u64, &[u8])],
    kind: u8,
    ns_code: u16,
    target_page_bytes: usize,
) -> io::Result<Vec<u8>> {
    if entries.is_empty() {
        // Empty pack: header + empty directory.
        let mut buf = Vec::with_capacity(PACK_HEADER_SIZE);
        let header = PackHeader {
            version: PACK_VERSION,
            kind,
            ns_code,
            first_id: 0,
            last_id: 0,
            page_count: 0,
            page_dir_offset: PACK_HEADER_SIZE as u64,
        };
        header.encode(&mut buf);
        return Ok(buf);
    }

    // Validate contiguity.
    let first_id = entries[0].0;
    for (i, &(id, _)) in entries.iter().enumerate() {
        let expected = first_id + i as u64;
        if id != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "pack encode: entry {i} has id {id} but expected {expected} (contiguity violation)"
                ),
            ));
        }
    }
    let last_id = entries.last().unwrap().0;

    // Pass 1: determine page boundaries (no data allocations).
    let page_plan = plan_pages(entries, target_page_bytes);

    // Pre-calculate total size with checked arithmetic.
    let mut total_page_bytes: usize = 0;
    for &(start, end) in &page_plan {
        let count = end - start;
        let data_size: usize = entries[start..end].iter().map(|(_, v)| v.len()).sum();
        let page_size = 4 + (count + 1) * 4 + data_size;
        total_page_bytes = total_page_bytes
            .checked_add(page_size)
            .ok_or_else(|| io::Error::other("pack encode: total page size overflow"))?;
    }
    let dir_size = page_plan
        .len()
        .checked_mul(PAGE_DIR_ENTRY_SIZE)
        .ok_or_else(|| io::Error::other("pack encode: directory size overflow"))?;
    let total = PACK_HEADER_SIZE
        .checked_add(total_page_bytes)
        .and_then(|s| s.checked_add(dir_size))
        .ok_or_else(|| io::Error::other("pack encode: total size overflow"))?;

    let mut buf = Vec::with_capacity(total);

    // Write header.
    let page_dir_offset = (PACK_HEADER_SIZE + total_page_bytes) as u64;
    let header = PackHeader {
        version: PACK_VERSION,
        kind,
        ns_code,
        first_id,
        last_id,
        page_count: page_plan.len() as u32,
        page_dir_offset,
    };
    header.encode(&mut buf);

    // Pass 2: encode pages directly into buf, recording directory entries.
    let mut dir_entries: Vec<PageDirEntry> = Vec::with_capacity(page_plan.len());
    for &(start, end) in &page_plan {
        let page_offset = u32::try_from(buf.len())
            .map_err(|_| io::Error::other("pack encode: page offset exceeds u32"))?;
        let page_first_id = entries[start].0;
        let entry_count = (end - start) as u32;

        encode_page_into(&entries[start..end], &mut buf)?;

        let page_len = u32::try_from(buf.len() - page_offset as usize)
            .map_err(|_| io::Error::other("pack encode: page length exceeds u32"))?;
        dir_entries.push(PageDirEntry {
            page_first_id,
            entry_count,
            page_offset,
            page_len,
        });
    }

    // Write directory.
    for entry in &dir_entries {
        entry.encode(&mut buf);
    }

    debug_assert_eq!(buf.len(), total);
    Ok(buf)
}

/// Determine page boundaries without allocating page data.
///
/// Returns `Vec<(start_idx, end_idx_exclusive)>` into the entries slice.
fn plan_pages(entries: &[(u64, &[u8])], target_page_bytes: usize) -> Vec<(usize, usize)> {
    let mut pages = Vec::new();
    let mut page_start = 0usize;
    let mut page_data_size = 0usize;

    for (i, &(_, value)) in entries.iter().enumerate() {
        page_data_size += value.len();
        let entries_so_far = i - page_start + 1;
        let est_page_size = 4 + (entries_so_far + 1) * 4 + page_data_size;

        let is_last = i == entries.len() - 1;
        if est_page_size >= target_page_bytes || is_last {
            pages.push((page_start, i + 1));
            page_start = i + 1;
            page_data_size = 0;
        }
    }

    pages
}

/// Encode a single page directly into `buf` (no intermediate allocation).
fn encode_page_into(entries: &[(u64, &[u8])], buf: &mut Vec<u8>) -> io::Result<()> {
    let entry_count = entries.len() as u32;

    // entry_count
    buf.extend_from_slice(&entry_count.to_le_bytes());

    // offsets array
    let mut offset = 0u32;
    for &(_, value) in entries {
        buf.extend_from_slice(&offset.to_le_bytes());
        offset = offset
            .checked_add(
                u32::try_from(value.len())
                    .map_err(|_| io::Error::other("pack encode: value length exceeds u32"))?,
            )
            .ok_or_else(|| io::Error::other("pack encode: page data offset overflow"))?;
    }
    // sentinel (end offset)
    buf.extend_from_slice(&offset.to_le_bytes());

    // data section
    for &(_, value) in entries {
        buf.extend_from_slice(value);
    }

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(first: u64, count: usize) -> Vec<(u64, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let id = first + i as u64;
                let value = format!("value_{id}").into_bytes();
                (id, value)
            })
            .collect()
    }

    fn as_refs(entries: &[(u64, Vec<u8>)]) -> Vec<(u64, &[u8])> {
        entries.iter().map(|(id, v)| (*id, v.as_slice())).collect()
    }

    #[test]
    fn test_single_page_round_trip() {
        let entries = make_entries(0, 10);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.header().first_id, 0);
        assert_eq!(pack.header().last_id, 9);
        assert_eq!(pack.page_count(), 1);
        assert_eq!(pack.total_entries(), 10);

        for &(id, ref value) in &entries {
            let found = pack.lookup(id).unwrap();
            assert_eq!(found, value.as_slice(), "mismatch at id={id}");
        }

        // Out of range
        assert!(pack.lookup(10).is_none());
        assert!(pack.lookup(100).is_none());
    }

    #[test]
    fn test_multi_page_round_trip() {
        let entries = make_entries(100, 500);
        let refs = as_refs(&entries);
        // Small target to force many pages.
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 512).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert!(
            pack.page_count() > 1,
            "expected multiple pages, got {}",
            pack.page_count()
        );
        assert_eq!(pack.total_entries(), 500);

        // Check every entry.
        for &(id, ref value) in &entries {
            let found = pack
                .lookup(id)
                .unwrap_or_else(|| panic!("should find id={id}"));
            assert_eq!(found, value.as_slice());
        }

        // Boundary checks.
        assert!(pack.lookup(99).is_none());
        assert!(pack.lookup(600).is_none());
    }

    #[test]
    fn test_lookup_into() {
        let entries = make_entries(0, 5);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        let mut out = Vec::new();
        assert!(pack.lookup_into(3, &mut out));
        assert_eq!(out, b"value_3");

        // Append to existing data.
        assert!(pack.lookup_into(0, &mut out));
        assert_eq!(out, b"value_3value_0");

        // Miss.
        let prev_len = out.len();
        assert!(!pack.lookup_into(999, &mut out));
        assert_eq!(out.len(), prev_len);
    }

    #[test]
    fn test_lookup_str() {
        let entries = make_entries(0, 3);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.lookup_str(1).unwrap(), Some("value_1".to_string()));
        assert_eq!(pack.lookup_str(999).unwrap(), None);
    }

    #[test]
    fn test_empty_pack() {
        let pack_bytes = encode_forward_pack(&[], KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.page_count(), 0);
        assert_eq!(pack.total_entries(), 0);
        assert!(pack.lookup(0).is_none());
    }

    #[test]
    fn test_large_pack() {
        let entries = make_entries(0, 100_000);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 2 * 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.total_entries(), 100_000);

        // Spot-check a few entries.
        for id in [0, 1, 999, 50_000, 99_999] {
            let val = pack
                .lookup(id)
                .unwrap_or_else(|| panic!("should find id={id}"));
            assert_eq!(val, format!("value_{id}").as_bytes());
        }
    }

    #[test]
    fn test_subject_forward_pack() {
        // Subject packs store local_id as the key, ns_code in header.
        let entries: Vec<(u64, Vec<u8>)> = (0..20)
            .map(|i| (i as u64, format!("suffix/{i}").into_bytes()))
            .collect();
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_SUBJECT_FWD, 7, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.header().kind, KIND_SUBJECT_FWD);
        assert_eq!(pack.header().ns_code, 7);

        assert_eq!(pack.lookup_str(15).unwrap(), Some("suffix/15".to_string()));
    }

    #[test]
    fn test_contiguity_violation() {
        let entries = vec![(0u64, b"a".as_slice()), (2, b"c".as_slice())]; // gap at id=1
        let result = encode_forward_pack(&entries, KIND_STRING_FWD, 0, 1024 * 1024);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("contiguity violation"));
    }

    #[test]
    fn test_validation_truncated() {
        let result = ForwardPack::from_bytes(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_bad_magic() {
        let mut data = vec![0u8; PACK_HEADER_SIZE];
        data[0..4].copy_from_slice(b"XXXX");
        let result = ForwardPack::from_bytes(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic"));
    }

    #[test]
    fn test_page_validation_non_monotone_offsets() {
        // Craft a page with non-monotone offsets.
        let mut page_data = Vec::new();
        page_data.extend_from_slice(&2u32.to_le_bytes()); // entry_count = 2
        page_data.extend_from_slice(&0u32.to_le_bytes()); // offsets[0] = 0
        page_data.extend_from_slice(&5u32.to_le_bytes()); // offsets[1] = 5
        page_data.extend_from_slice(&3u32.to_le_bytes()); // offsets[2] = 3 (BAD: < 5)
        page_data.extend_from_slice(&[0u8; 5]); // data

        let result = ForwardPage::from_bytes(&page_data, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("offset"));
    }

    #[test]
    fn test_non_zero_first_id() {
        let entries = make_entries(1000, 50);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.header().first_id, 1000);
        assert_eq!(pack.header().last_id, 1049);

        assert!(pack.lookup(999).is_none());
        assert_eq!(pack.lookup(1000).unwrap(), b"value_1000");
        assert_eq!(pack.lookup(1049).unwrap(), b"value_1049");
        assert!(pack.lookup(1050).is_none());
    }

    #[test]
    fn test_empty_values() {
        // Some values can be empty strings (valid).
        let entries: Vec<(u64, &[u8])> = vec![(0, b""), (1, b"hello"), (2, b""), (3, b"world")];
        let pack_bytes = encode_forward_pack(&entries, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();
        let pack = ForwardPack::from_bytes(&pack_bytes).unwrap();

        assert_eq!(pack.lookup(0).unwrap(), b"");
        assert_eq!(pack.lookup(1).unwrap(), b"hello");
        assert_eq!(pack.lookup(2).unwrap(), b"");
        assert_eq!(pack.lookup(3).unwrap(), b"world");
    }

    #[test]
    fn test_zero_entry_count_page_rejected() {
        // Craft a pack with a page directory entry that has entry_count=0.
        // This should be rejected by parse_pack_meta.
        let entries = make_entries(0, 10);
        let refs = as_refs(&entries);
        let mut pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 1024 * 1024).unwrap();

        // Find the directory entry and zero out entry_count.
        let header = PackHeader::decode(&pack_bytes).unwrap();
        let dir_offset = header.page_dir_offset as usize;
        // entry_count is at bytes [8..12] within each 20-byte directory entry.
        pack_bytes[dir_offset + 8..dir_offset + 12].copy_from_slice(&0u32.to_le_bytes());

        let result = parse_pack_meta(&pack_bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("entry_count 0"));
    }

    #[test]
    fn test_parse_pack_meta_round_trip() {
        let entries = make_entries(100, 500);
        let refs = as_refs(&entries);
        let pack_bytes = encode_forward_pack(&refs, KIND_STRING_FWD, 0, 512).unwrap();
        let meta = parse_pack_meta(&pack_bytes).unwrap();

        assert_eq!(meta.first_id, 100);
        assert_eq!(meta.last_id, 599);
        assert_eq!(meta.kind, KIND_STRING_FWD);
        assert_eq!(meta.ns_code, 0);
        assert!(!meta.directory.is_empty());

        // Verify lookup_in_pack works with pre-parsed meta.
        for &(id, ref value) in &entries {
            let found = lookup_in_pack(&pack_bytes, &meta, id).unwrap();
            assert_eq!(found, value.as_slice(), "mismatch at id={id}");
        }

        assert!(lookup_in_pack(&pack_bytes, &meta, 99).is_none());
        assert!(lookup_in_pack(&pack_bytes, &meta, 600).is_none());
    }
}
