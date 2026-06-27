//! Dictionary flat-file upload to CAS.
//!
//! `upload_dicts_from_disk` reads persisted flat dict files (subjects, strings,
//! numbig arenas, vector arenas) and uploads them to CAS as forward packs +
//! reverse trees.
//!
//! ## Memory model
//!
//! There are two code paths, selected by `trust_sorted_order_invariants`:
//!
//! * **Streaming (trust = true)** — used by bulk import, where vocab-merge has
//!   already emitted subjects/strings in sorted `(ns_code, suffix)` order.
//!   `subjects.{sids,idx}` and `strings.idx` are read with small sequential
//!   readers, so peak RAM no longer scales with distinct-term cardinality. The
//!   forward and reverse passes each open fresh readers (cheap re-reads of small
//!   files); the large `.fwd` files are mmap'd read-only in both passes.
//! * **Materialized (trust = false)** — reads the index files fully into Vecs
//!   so it can sort by `id_order` / `rev_order` when the on-disk order is not
//!   already correct. Output is byte-identical to the streaming path.

use std::io::{BufReader, Read, Seek, SeekFrom};

use fluree_db_binary_index::{
    DictPackRefs, DictRefs, DictTreeRefs, PackBranchEntry, VectorDictRef,
};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{ContentId, ContentStore};

use crate::error::{IndexerError, Result};
use crate::run_index;

use super::types::UploadedDicts;
use super::upload::upload_dict_file;

/// Magic for `subjects.sids` (`SSM1`); mirrors `dict_io::write_subject_sid_map`.
const SID_MAP_MAGIC: [u8; 4] = *b"SSM1";
/// Magic for forward-index files (`FSI1`); mirrors `dict_io::write_forward_index`.
const FORWARD_INDEX_MAGIC: [u8; 4] = *b"FSI1";

/// Sequential reader over `subjects.sids` (`SSM1` + count + `[u64 LE]`).
///
/// Yields one `sid64` per [`next`](Self::next) in file order, never holding more
/// than one entry resident. Equivalent to iterating the Vec returned by
/// `dict_io::read_subject_sid_map`, without materializing it.
struct SubjectSidReader {
    inner: BufReader<std::fs::File>,
    remaining: u64,
}

impl SubjectSidReader {
    fn open(path: &std::path::Path) -> Result<Self> {
        let file = std::fs::File::open(path)
            .map_err(|e| IndexerError::StorageRead(format!("open {}: {}", path.display(), e)))?;
        let mut inner = BufReader::new(file);
        let mut header = [0u8; 12];
        inner.read_exact(&mut header).map_err(|e| {
            IndexerError::StorageRead(format!("read {} header: {}", path.display(), e))
        })?;
        if header[0..4] != SID_MAP_MAGIC {
            return Err(IndexerError::StorageRead(format!(
                "{}: invalid sid map magic",
                path.display()
            )));
        }
        let remaining = u64::from_le_bytes(header[4..12].try_into().expect("12-byte header"));
        Ok(Self { inner, remaining })
    }

    fn next(&mut self) -> Result<Option<u64>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut buf = [0u8; 8];
        self.inner
            .read_exact(&mut buf)
            .map_err(|e| IndexerError::StorageRead(format!("read sid entry: {e}")))?;
        self.remaining -= 1;
        Ok(Some(u64::from_le_bytes(buf)))
    }
}

/// Sequential reader over a forward-index file (`FSI1` + count + offsets region
/// + lens region).
///
/// The offsets (`u64 LE`) and lens (`u32 LE`) live in two separate contiguous
/// regions, so this keeps two `BufReader`s over the same file seeked to each
/// region and advances them in lockstep.
///
/// Yields `(offset, len)` per [`next`](Self::next) in file order; equivalent to
/// zipping the two Vecs returned by `dict_io::read_forward_index`.
struct ForwardIndexReader {
    offsets: BufReader<std::fs::File>,
    lens: BufReader<std::fs::File>,
    remaining: u32,
}

impl ForwardIndexReader {
    fn open(path: &std::path::Path) -> Result<Self> {
        let open = || {
            std::fs::File::open(path)
                .map_err(|e| IndexerError::StorageRead(format!("open {}: {}", path.display(), e)))
        };
        let mut offsets = BufReader::new(open()?);
        let mut header = [0u8; 8];
        offsets.read_exact(&mut header).map_err(|e| {
            IndexerError::StorageRead(format!("read {} header: {}", path.display(), e))
        })?;
        if header[0..4] != FORWARD_INDEX_MAGIC {
            return Err(IndexerError::StorageRead(format!(
                "{}: invalid forward index magic",
                path.display()
            )));
        }
        let count = u32::from_le_bytes(header[4..8].try_into().expect("8-byte header"));
        // offsets region starts at byte 8 (offsets reader is already positioned);
        // lens region starts at 8 + 8 * count.
        let lens_start = 8u64 + 8 * count as u64;
        let mut lens = BufReader::new(open()?);
        lens.seek(SeekFrom::Start(lens_start)).map_err(|e| {
            IndexerError::StorageRead(format!("seek {} lens region: {}", path.display(), e))
        })?;
        Ok(Self {
            offsets,
            lens,
            remaining: count,
        })
    }

    fn next(&mut self) -> Result<Option<(u64, u32)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut off_buf = [0u8; 8];
        self.offsets
            .read_exact(&mut off_buf)
            .map_err(|e| IndexerError::StorageRead(format!("read fwd-index offset: {e}")))?;
        let mut len_buf = [0u8; 4];
        self.lens
            .read_exact(&mut len_buf)
            .map_err(|e| IndexerError::StorageRead(format!("read fwd-index len: {e}")))?;
        self.remaining -= 1;
        Ok(Some((
            u64::from_le_bytes(off_buf),
            u32::from_le_bytes(len_buf),
        )))
    }
}

/// One subject dict entry, scalars only — the suffix bytes are read from the
/// mmap'd `.fwd` file by the builder using `(suf_off, suf_len)`.
#[derive(Clone, Copy)]
struct SubjectEntry {
    sid: u64,
    ns_code: u16,
    suf_off: u64,
    suf_len: u32,
}

/// One string dict entry: the contiguous `id` plus the suffix range in `.fwd`.
#[derive(Clone, Copy)]
struct StringEntry {
    id: u64,
    off: u64,
    len: u32,
}

/// Running watermark accumulation over subject sids (see `UploadedDicts`).
#[derive(Default)]
struct SubjectWatermarks {
    needs_wide: bool,
    max_ns_code: u16,
    /// `watermark_map[ns_code]` = max local_id seen for that ns_code.
    map: std::collections::BTreeMap<u16, u64>,
}

impl SubjectWatermarks {
    /// Overflow namespace marker: always forces wide encoding, watermark 0.
    const OVERFLOW_NS: u16 = 0xFFFF;

    fn observe(&mut self, sid: u64) {
        let subject_id = SubjectId::from_u64(sid);
        let ns_code = subject_id.ns_code();
        let local_id = subject_id.local_id();
        if ns_code == Self::OVERFLOW_NS {
            self.needs_wide = true;
            return;
        }
        if local_id > u16::MAX as u64 {
            self.needs_wide = true;
        }
        if ns_code > self.max_ns_code {
            self.max_ns_code = ns_code;
        }
        let entry = self.map.entry(ns_code).or_insert(0);
        if local_id > *entry {
            *entry = local_id;
        }
    }

    fn finish(self) -> (fluree_db_core::SubjectIdEncoding, Vec<u64>) {
        use fluree_db_core::SubjectIdEncoding;
        let encoding = if self.needs_wide {
            SubjectIdEncoding::Wide
        } else {
            SubjectIdEncoding::Narrow
        };
        let len = if self.map.is_empty() {
            0
        } else {
            self.max_ns_code as usize + 1
        };
        let mut watermarks = vec![0u64; len];
        for (&ns_code, &max_local) in &self.map {
            watermarks[ns_code as usize] = max_local;
        }
        (encoding, watermarks)
    }
}

/// Derive `(ns_code, suffix_offset, suffix_len)` for a subject by stripping the
/// namespace prefix from its IRI, exactly as the forward/reverse trees key on.
fn subject_suffix_range(
    sid: u64,
    off: u64,
    len: u32,
    fwd: &[u8],
    namespace_codes: &std::collections::HashMap<u16, String>,
) -> SubjectEntry {
    let ns_code = SubjectId::from_u64(sid).ns_code();
    let iri = &fwd[off as usize..(off as usize + len as usize)];
    let prefix_bytes = namespace_codes
        .get(&ns_code)
        .map(std::string::String::as_bytes)
        .unwrap_or(b"");
    if !prefix_bytes.is_empty() && iri.starts_with(prefix_bytes) {
        SubjectEntry {
            sid,
            ns_code,
            suf_off: off + prefix_bytes.len() as u64,
            suf_len: len.saturating_sub(prefix_bytes.len() as u32),
        }
    } else {
        SubjectEntry {
            sid,
            ns_code,
            suf_off: off,
            suf_len: len,
        }
    }
}

/// Output of a flushed reverse-index leaf: `(leaf_bytes, first_key, last_key, entry_count)`.
type FlushedLeaf = (Vec<u8>, Vec<u8>, Vec<u8>, u32);

fn flush_reverse_leaf<F>(
    leaf_offsets: &mut Vec<u32>,
    leaf_data: &mut Vec<u8>,
    first_key: &mut Option<Vec<u8>>,
    chunk_bytes: &mut usize,
    mut last_key: F,
) -> Option<FlushedLeaf>
where
    F: FnMut() -> Vec<u8>,
{
    if leaf_offsets.is_empty() {
        return None;
    }
    let entry_count = leaf_offsets.len() as u32;
    let header_size = 8;
    let offset_table_size = leaf_offsets.len() * 4;
    let total = header_size + offset_table_size + leaf_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&fluree_db_binary_index::dict::reverse_leaf::REVERSE_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());
    for off in leaf_offsets.iter() {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(leaf_data);
    debug_assert_eq!(buf.len(), total);

    let fk = first_key.take().unwrap_or_default();
    let lk = last_key();

    leaf_offsets.clear();
    leaf_data.clear();
    *chunk_bytes = 0;

    Some((buf, fk, lk, entry_count))
}

fn build_forward_pack_artifact(
    entries: &[(u64, &[u8])],
    kind: u8,
    ns_code: u16,
    target_page_bytes: usize,
) -> std::io::Result<(Vec<u8>, u64, u64)> {
    if entries.is_empty() {
        return Err(std::io::Error::other("cannot build empty forward pack"));
    }
    let bytes = fluree_db_binary_index::dict::forward_pack::encode_forward_pack(
        entries,
        kind,
        ns_code,
        target_page_bytes,
    )?;
    Ok((bytes, entries[0].0, entries.last().expect("non-empty").0))
}

/// Build subject forward packs (one FPK1 stream per namespace) by draining
/// `next_entry`. `entries` must arrive grouped by `ns_code` (the on-disk and the
/// `id_order`-sorted orders both satisfy this). Suffix bytes come from `fwd`.
async fn build_subject_forward_packs<F>(
    content_store: &dyn ContentStore,
    fwd: &[u8],
    mut next_entry: F,
) -> Result<Vec<(u16, Vec<PackBranchEntry>)>>
where
    F: FnMut() -> Result<Option<SubjectEntry>>,
{
    use fluree_db_binary_index::dict::forward_pack::KIND_SUBJECT_FWD;
    use fluree_db_binary_index::dict::pack_builder::{
        DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES,
    };
    use fluree_db_core::{ContentKind, DictKind};

    let kind = ContentKind::DictBlob {
        dict: DictKind::SubjectForward,
    };

    let mut subject_fwd_ns_packs: Vec<(u16, Vec<PackBranchEntry>)> = Vec::new();
    let mut current_ns: Option<u16> = None;
    let mut current_pack_refs: Vec<PackBranchEntry> = Vec::new();
    let mut current_entries: Vec<(u64, &[u8])> = Vec::new();
    let mut current_pack_est = 0usize;

    while let Some(entry) = next_entry()? {
        let ns_code = entry.ns_code;
        let local_id = SubjectId::from_u64(entry.sid).local_id();
        let suffix =
            &fwd[entry.suf_off as usize..(entry.suf_off as usize + entry.suf_len as usize)];

        if current_ns != Some(ns_code) {
            if let Some(prev_ns) = current_ns {
                if !current_entries.is_empty() {
                    let (bytes, first_id, last_id) = build_forward_pack_artifact(
                        &current_entries,
                        KIND_SUBJECT_FWD,
                        prev_ns,
                        DEFAULT_TARGET_PAGE_BYTES,
                    )
                    .map_err(|e| IndexerError::StorageWrite(format!("subject pack build: {e}")))?;
                    let cas_result = content_store
                        .put(kind, &bytes)
                        .await
                        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                    current_pack_refs.push(PackBranchEntry {
                        first_id,
                        last_id,
                        pack_cid: cas_result,
                    });
                    current_entries.clear();
                    current_pack_est = 0;
                }
                subject_fwd_ns_packs.push((prev_ns, std::mem::take(&mut current_pack_refs)));
            }
            current_ns = Some(ns_code);
        }

        current_pack_est += suffix.len() + 4;
        current_entries.push((local_id, suffix));

        if current_pack_est >= DEFAULT_TARGET_PACK_BYTES {
            let (bytes, first_id, last_id) = build_forward_pack_artifact(
                &current_entries,
                KIND_SUBJECT_FWD,
                ns_code,
                DEFAULT_TARGET_PAGE_BYTES,
            )
            .map_err(|e| IndexerError::StorageWrite(format!("subject pack build: {e}")))?;
            let cas_result = content_store
                .put(kind, &bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            current_pack_refs.push(PackBranchEntry {
                first_id,
                last_id,
                pack_cid: cas_result,
            });
            current_entries.clear();
            current_pack_est = 0;
        }
    }

    if let Some(ns_code) = current_ns {
        if !current_entries.is_empty() {
            let (bytes, first_id, last_id) = build_forward_pack_artifact(
                &current_entries,
                KIND_SUBJECT_FWD,
                ns_code,
                DEFAULT_TARGET_PAGE_BYTES,
            )
            .map_err(|e| IndexerError::StorageWrite(format!("subject pack build: {e}")))?;
            let cas_result = content_store
                .put(kind, &bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            current_pack_refs.push(PackBranchEntry {
                first_id,
                last_id,
                pack_cid: cas_result,
            });
        }
        subject_fwd_ns_packs.push((ns_code, current_pack_refs));
    }

    Ok(subject_fwd_ns_packs)
}

/// Build the subject reverse tree by draining `next_entry`. Entries must arrive
/// in reverse-tree key order (`(ns_code, suffix)`). Key = `[ns_code BE][suffix]`.
async fn build_subject_reverse_tree<F>(
    content_store: &dyn ContentStore,
    fwd: &[u8],
    mut next_entry: F,
) -> Result<DictTreeRefs>
where
    F: FnMut() -> Result<Option<SubjectEntry>>,
{
    use fluree_db_binary_index::dict::branch::{BranchLeafEntry, DictBranch};
    use fluree_db_binary_index::dict::builder;
    use fluree_db_core::{ContentKind, DictKind};

    let kind = ContentKind::DictBlob {
        dict: DictKind::SubjectReverse,
    };
    let mut leaf_cids: Vec<ContentId> = Vec::new();
    let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

    let mut leaf_offsets: Vec<u32> = Vec::new();
    let mut leaf_data: Vec<u8> = Vec::new();
    let mut chunk_bytes: usize = 0;
    let mut first_key: Option<Vec<u8>> = None;
    // Track last key parts for branch boundary (avoid per-entry allocation).
    let mut last_ns: u16 = 0;
    let mut last_off: u64 = 0;
    let mut last_len: u32 = 0;

    while let Some(entry) = next_entry()? {
        let ns = entry.ns_code;
        let suffix =
            &fwd[entry.suf_off as usize..(entry.suf_off as usize + entry.suf_len as usize)];
        let sid = entry.sid;

        // Key = [ns_code BE][suffix]
        let key_len = 2 + suffix.len();
        let entry_size = 12 + key_len;
        leaf_offsets.push(chunk_bytes as u32);
        chunk_bytes += entry_size;

        leaf_data.extend_from_slice(&(key_len as u32).to_le_bytes());
        leaf_data.extend_from_slice(&ns.to_be_bytes());
        leaf_data.extend_from_slice(suffix);
        leaf_data.extend_from_slice(&sid.to_le_bytes());

        if first_key.is_none() {
            let mut k = Vec::with_capacity(key_len);
            k.extend_from_slice(&ns.to_be_bytes());
            k.extend_from_slice(suffix);
            first_key = Some(k);
        }
        last_ns = ns;
        last_off = entry.suf_off;
        last_len = entry.suf_len;

        if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
            if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                &mut leaf_offsets,
                &mut leaf_data,
                &mut first_key,
                &mut chunk_bytes,
                || {
                    let suffix = &fwd[last_off as usize..(last_off as usize + last_len as usize)];
                    let mut lk = Vec::with_capacity(2 + suffix.len());
                    lk.extend_from_slice(&last_ns.to_be_bytes());
                    lk.extend_from_slice(suffix);
                    lk
                },
            ) {
                let cas_result = content_store
                    .put(kind, &leaf_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                let address = cas_result.to_string();
                leaf_cids.push(cas_result);
                branch_entries.push(BranchLeafEntry {
                    first_key: fk,
                    last_key: lk,
                    entry_count,
                    address,
                });
            }
        }
    }

    if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
        &mut leaf_offsets,
        &mut leaf_data,
        &mut first_key,
        &mut chunk_bytes,
        || {
            let suffix = &fwd[last_off as usize..(last_off as usize + last_len as usize)];
            let mut lk = Vec::with_capacity(2 + suffix.len());
            lk.extend_from_slice(&last_ns.to_be_bytes());
            lk.extend_from_slice(suffix);
            lk
        },
    ) {
        let cas_result = content_store
            .put(kind, &leaf_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let address = cas_result.to_string();
        leaf_cids.push(cas_result);
        branch_entries.push(BranchLeafEntry {
            first_key: fk,
            last_key: lk,
            entry_count,
            address,
        });
    }

    let branch = DictBranch {
        leaves: branch_entries,
    };
    let branch_bytes = branch.encode();
    let branch_result = content_store
        .put(kind, &branch_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    Ok(DictTreeRefs {
        branch: branch_result,
        leaves: leaf_cids,
    })
}

/// Build string forward packs (FPK1 format, single `ns_code = 0` stream) by
/// draining `next_entry`. Suffix bytes come from `fwd`.
async fn build_string_forward_packs<F>(
    content_store: &dyn ContentStore,
    fwd: &[u8],
    mut next_entry: F,
) -> Result<Vec<PackBranchEntry>>
where
    F: FnMut() -> Result<Option<StringEntry>>,
{
    use fluree_db_binary_index::dict::forward_pack::KIND_STRING_FWD;
    use fluree_db_binary_index::dict::pack_builder::{
        DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES,
    };
    use fluree_db_core::{ContentKind, DictKind};

    let kind = ContentKind::DictBlob {
        dict: DictKind::StringForward,
    };
    let mut pack_refs = Vec::new();
    let mut entries: Vec<(u64, &[u8])> = Vec::new();
    let mut pack_est = 0usize;

    while let Some(entry) = next_entry()? {
        let bytes = &fwd[entry.off as usize..(entry.off as usize + entry.len as usize)];
        pack_est += bytes.len() + 4;
        entries.push((entry.id, bytes));

        if pack_est >= DEFAULT_TARGET_PACK_BYTES {
            let (bytes, first_id, last_id) = build_forward_pack_artifact(
                &entries,
                KIND_STRING_FWD,
                0,
                DEFAULT_TARGET_PAGE_BYTES,
            )
            .map_err(|e| IndexerError::StorageWrite(format!("string pack build: {e}")))?;
            let cas_result = content_store
                .put(kind, &bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            pack_refs.push(PackBranchEntry {
                first_id,
                last_id,
                pack_cid: cas_result,
            });
            entries.clear();
            pack_est = 0;
        }
    }

    if !entries.is_empty() {
        let (bytes, first_id, last_id) =
            build_forward_pack_artifact(&entries, KIND_STRING_FWD, 0, DEFAULT_TARGET_PAGE_BYTES)
                .map_err(|e| IndexerError::StorageWrite(format!("string pack build: {e}")))?;
        let cas_result = content_store
            .put(kind, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        pack_refs.push(PackBranchEntry {
            first_id,
            last_id,
            pack_cid: cas_result,
        });
    }

    Ok(pack_refs)
}

/// Build the string reverse tree by draining `next_entry`. Entries must arrive
/// in reverse-tree key order (lexicographic by string). Key = the string bytes.
async fn build_string_reverse_tree<F>(
    content_store: &dyn ContentStore,
    fwd: &[u8],
    mut next_entry: F,
) -> Result<DictTreeRefs>
where
    F: FnMut() -> Result<Option<StringEntry>>,
{
    use fluree_db_binary_index::dict::branch::{BranchLeafEntry, DictBranch};
    use fluree_db_binary_index::dict::builder;
    use fluree_db_core::{ContentKind, DictKind};

    let kind = ContentKind::DictBlob {
        dict: DictKind::StringReverse,
    };
    let mut leaf_cids: Vec<ContentId> = Vec::new();
    let mut branch_entries: Vec<BranchLeafEntry> = Vec::new();

    let mut leaf_offsets: Vec<u32> = Vec::new();
    let mut leaf_data: Vec<u8> = Vec::new();
    let mut chunk_bytes: usize = 0;
    let mut first_key: Option<Vec<u8>> = None;
    // Track last key slice for boundary without cloning per entry.
    let mut last_off: usize = 0;
    let mut last_len: usize = 0;

    while let Some(entry) = next_entry()? {
        let off = entry.off as usize;
        let len = entry.len as usize;
        let key = &fwd[off..off + len];
        let id = entry.id;

        let entry_size = 12 + key.len();
        leaf_offsets.push(chunk_bytes as u32);
        chunk_bytes += entry_size;

        leaf_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        leaf_data.extend_from_slice(key);
        leaf_data.extend_from_slice(&id.to_le_bytes());

        if first_key.is_none() {
            first_key = Some(key.to_vec());
        }
        last_off = off;
        last_len = len;

        if chunk_bytes >= builder::DEFAULT_TARGET_LEAF_BYTES {
            if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
                &mut leaf_offsets,
                &mut leaf_data,
                &mut first_key,
                &mut chunk_bytes,
                || fwd[last_off..(last_off + last_len)].to_vec(),
            ) {
                let cas_result = content_store
                    .put(kind, &leaf_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                let address = cas_result.to_string();
                leaf_cids.push(cas_result);
                branch_entries.push(BranchLeafEntry {
                    first_key: fk,
                    last_key: lk,
                    entry_count,
                    address,
                });
            }
        }
    }

    if let Some((leaf_bytes, fk, lk, entry_count)) = flush_reverse_leaf(
        &mut leaf_offsets,
        &mut leaf_data,
        &mut first_key,
        &mut chunk_bytes,
        || fwd[last_off..(last_off + last_len)].to_vec(),
    ) {
        let cas_result = content_store
            .put(kind, &leaf_bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        let address = cas_result.to_string();
        leaf_cids.push(cas_result);
        branch_entries.push(BranchLeafEntry {
            first_key: fk,
            last_key: lk,
            entry_count,
            address,
        });
    }

    let branch = DictBranch {
        leaves: branch_entries,
    };
    let branch_bytes = branch.encode();
    let branch_result = content_store
        .put(kind, &branch_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    Ok(DictTreeRefs {
        branch: branch_result,
        leaves: leaf_cids,
    })
}

///
/// Reads flat files written by `GlobalDicts::persist()` and builds CoW trees
/// for subject/string dicts. Does NOT require `GlobalDicts` in memory.
///
/// Required files in `run_dir`:
///   - `subjects.fwd`, `subjects.idx`, `subjects.sids`
///   - `strings.fwd`, `strings.idx`
///   - `graphs.dict`, `datatypes.dict`, `languages.dict`
///   - `numbig/p_*.nba` (zero or more)
///
/// Watermark derivation from subject sids:
///   - Decode each sid64 via `SubjectId::from_u64` → `(ns_code, local_id)`
///   - `subject_watermarks[ns_code]` = max local_id for that ns_code
///   - Overflow ns_code (0xFFFF): always wide, watermark = 0
///   - `needs_wide` = any local_id exceeds `u16::MAX`
///   - `string_watermark` = string entry count − 1 (IDs are 0..=N contiguous)
#[allow(clippy::type_complexity)]
pub async fn upload_dicts_from_disk(
    content_store: &dyn ContentStore,
    run_dir: &std::path::Path,
    namespace_codes: &std::collections::HashMap<u16, String>,
    trust_sorted_order_invariants: bool,
) -> Result<UploadedDicts> {
    use fluree_db_binary_index::dict::branch::DictBranch;
    use fluree_db_core::{ContentKind, DictKind};
    use std::collections::BTreeMap;

    // ---- 1. Read small dicts for v4 root inlining ----
    tracing::info!("reading small dictionaries for v4 root (graphs, datatypes, languages)");

    let graphs_path = run_dir.join("graphs.dict");
    let graphs_dict = run_index::dict_io::read_predicate_dict(&graphs_path)
        .map_err(|e| IndexerError::StorageRead(format!("read {}: {}", graphs_path.display(), e)))?;
    let graph_iris: Vec<String> = (0..graphs_dict.len())
        .filter_map(|id| {
            graphs_dict
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();

    let datatypes_path = run_dir.join("datatypes.dict");
    let datatypes_dict = run_index::dict_io::read_predicate_dict(&datatypes_path).map_err(|e| {
        IndexerError::StorageRead(format!("read {}: {}", datatypes_path.display(), e))
    })?;
    let datatype_iris: Vec<String> = (0..datatypes_dict.len())
        .filter_map(|id| {
            datatypes_dict
                .resolve(id)
                .map(std::string::ToString::to_string)
        })
        .collect();

    let languages_path = run_dir.join("languages.dict");
    let language_tags = if languages_path.exists() {
        let lang_dict = run_index::dict_io::read_language_dict(&languages_path).map_err(|e| {
            IndexerError::StorageRead(format!("read {}: {}", languages_path.display(), e))
        })?;
        let mut tags: Vec<(u16, String)> = lang_dict
            .iter()
            .map(|(id, tag)| (id, tag.to_string()))
            .collect();
        tags.sort_unstable_by_key(|(id, _)| *id);
        tags.into_iter().map(|(_, tag)| tag).collect()
    } else {
        Vec::new()
    };

    // ---- 2. Upload subject trees, string trees, numbig, vectors in parallel ----
    tracing::info!(
        streaming = trust_sorted_order_invariants,
        rss_mib = crate::mem::current_rss_mib(),
        "dict upload: start (subject/string trees, numbig, vectors)"
    );
    if trust_sorted_order_invariants {
        tracing::info!(
            "trusting bulk-import sorted-order invariants for subject/string dict reverse trees"
        );
    }

    let sids_path = run_dir.join("subjects.sids");
    let subj_idx_path = run_dir.join("subjects.idx");
    let subj_fwd_path = run_dir.join("subjects.fwd");

    let (subject_result, string_result, numbig, vectors) = tokio::try_join!(
        // Task A: Subject forward + reverse trees (+ streamed watermarks)
        async {
            let subj_fwd_file = std::fs::File::open(&subj_fwd_path).map_err(|e| {
                IndexerError::StorageRead(format!("open {}: {}", subj_fwd_path.display(), e))
            })?;
            // SAFETY: The file is opened read-only and is not concurrently modified.
            // The forward-dict file is an immutable index artifact written before this point.
            let subj_fwd_data = unsafe { memmap2::Mmap::map(&subj_fwd_file) }.map_err(|e| {
                IndexerError::StorageRead(format!("mmap {}: {}", subj_fwd_path.display(), e))
            })?;

            if trust_sorted_order_invariants {
                // --- Streaming path: sorted order trusted; never materialize. ---
                // Forward pass: stream sids+idx, accumulate watermarks inline.
                let mut watermarks = SubjectWatermarks::default();
                let fwd_pack_refs = {
                    let mut sid_reader = SubjectSidReader::open(&sids_path)?;
                    let mut idx_reader = ForwardIndexReader::open(&subj_idx_path)?;
                    let next = || -> Result<Option<SubjectEntry>> {
                        match (sid_reader.next()?, idx_reader.next()?) {
                            (Some(sid), Some((off, len))) => {
                                watermarks.observe(sid);
                                Ok(Some(subject_suffix_range(
                                    sid,
                                    off,
                                    len,
                                    &subj_fwd_data,
                                    namespace_codes,
                                )))
                            }
                            (None, None) => Ok(None),
                            _ => Err(IndexerError::StorageRead(
                                "subjects.sids and subjects.idx length mismatch".into(),
                            )),
                        }
                    };
                    build_subject_forward_packs(content_store, &subj_fwd_data, next).await?
                };

                // Reverse pass: fresh readers (re-read the small files).
                let reverse = {
                    let mut sid_reader = SubjectSidReader::open(&sids_path)?;
                    let mut idx_reader = ForwardIndexReader::open(&subj_idx_path)?;
                    let next = || -> Result<Option<SubjectEntry>> {
                        match (sid_reader.next()?, idx_reader.next()?) {
                            (Some(sid), Some((off, len))) => Ok(Some(subject_suffix_range(
                                sid,
                                off,
                                len,
                                &subj_fwd_data,
                                namespace_codes,
                            ))),
                            (None, None) => Ok(None),
                            _ => Err(IndexerError::StorageRead(
                                "subjects.sids and subjects.idx length mismatch".into(),
                            )),
                        }
                    };
                    build_subject_reverse_tree(content_store, &subj_fwd_data, next).await?
                };

                let (encoding, watermark_vec) = watermarks.finish();
                Ok::<_, IndexerError>((fwd_pack_refs, reverse, encoding, watermark_vec))
            } else {
                // --- Materialized path: read fully, sort indices as needed. ---
                let sids: Vec<u64> =
                    run_index::dict_io::read_subject_sid_map(&sids_path).map_err(|e| {
                        IndexerError::StorageRead(format!("read {}: {}", sids_path.display(), e))
                    })?;
                let (subj_offsets, subj_lens) =
                    run_index::dict_io::read_forward_index(&subj_idx_path).map_err(|e| {
                        IndexerError::StorageRead(format!(
                            "read {}: {}",
                            subj_idx_path.display(),
                            e
                        ))
                    })?;
                if subj_offsets.len() != sids.len() {
                    return Err(IndexerError::StorageRead(
                        "subjects.sids and subjects.idx length mismatch".into(),
                    ));
                }
                tracing::info!(
                    subjects = sids.len(),
                    fwd_bytes = subj_fwd_data.len(),
                    "subject dict files loaded"
                );

                // Precompute suffix ranges (random access needed for index sorts).
                let entries: Vec<SubjectEntry> = sids
                    .iter()
                    .zip(subj_offsets.iter().zip(subj_lens.iter()))
                    .map(|(&sid, (&off, &len))| {
                        subject_suffix_range(sid, off, len, &subj_fwd_data, namespace_codes)
                    })
                    .collect();

                // Forward order: by sid (== by (ns_code, local_id)).
                let sids_sorted = sids.windows(2).all(|w| w[0] <= w[1]);
                let id_order: Option<Vec<usize>> = if sids_sorted {
                    None
                } else {
                    let mut v: Vec<usize> = (0..sids.len()).collect();
                    v.sort_unstable_by_key(|&i| sids[i]);
                    Some(v)
                };
                let fwd_pack_refs = {
                    let mut pos = 0usize;
                    let next = || -> Result<Option<SubjectEntry>> {
                        let e = match &id_order {
                            None => entries.get(pos).copied(),
                            Some(order) => order.get(pos).map(|&i| entries[i]),
                        };
                        if e.is_some() {
                            pos += 1;
                        }
                        Ok(e)
                    };
                    build_subject_forward_packs(content_store, &subj_fwd_data, next).await?
                };

                // Reverse order: by (ns_code, suffix).
                let keys_sorted = {
                    let mut ok = true;
                    for i in 1..entries.len() {
                        let prev = &entries[i - 1];
                        let curr = &entries[i];
                        if prev.ns_code < curr.ns_code {
                            continue;
                        }
                        if prev.ns_code > curr.ns_code {
                            ok = false;
                            break;
                        }
                        let a = &subj_fwd_data[prev.suf_off as usize
                            ..(prev.suf_off as usize + prev.suf_len as usize)];
                        let b = &subj_fwd_data[curr.suf_off as usize
                            ..(curr.suf_off as usize + curr.suf_len as usize)];
                        if a > b {
                            ok = false;
                            break;
                        }
                    }
                    ok
                };
                let rev_order: Option<Vec<usize>> = if keys_sorted {
                    None
                } else {
                    tracing::info!("building subject reverse tree (fallback index-sort)");
                    let mut v: Vec<usize> = (0..entries.len()).collect();
                    v.sort_unstable_by(|&a, &b| {
                        let ea = &entries[a];
                        let eb = &entries[b];
                        match ea.ns_code.cmp(&eb.ns_code) {
                            std::cmp::Ordering::Equal => {
                                let sa = &subj_fwd_data[ea.suf_off as usize
                                    ..(ea.suf_off as usize + ea.suf_len as usize)];
                                let sb = &subj_fwd_data[eb.suf_off as usize
                                    ..(eb.suf_off as usize + eb.suf_len as usize)];
                                sa.cmp(sb)
                            }
                            other => other,
                        }
                    });
                    Some(v)
                };
                let reverse = {
                    let mut pos = 0usize;
                    let next = || -> Result<Option<SubjectEntry>> {
                        let e = match &rev_order {
                            None => entries.get(pos).copied(),
                            Some(order) => order.get(pos).map(|&i| entries[i]),
                        };
                        if e.is_some() {
                            pos += 1;
                        }
                        Ok(e)
                    };
                    build_subject_reverse_tree(content_store, &subj_fwd_data, next).await?
                };

                // Watermarks from the materialized sids (unchanged behavior).
                let mut watermarks = SubjectWatermarks::default();
                for &sid in &sids {
                    watermarks.observe(sid);
                }
                let (encoding, watermark_vec) = watermarks.finish();

                tracing::info!("subject dict artifacts uploaded");
                Ok::<_, IndexerError>((fwd_pack_refs, reverse, encoding, watermark_vec))
            }
        },
        // Task B: String forward + reverse trees
        async {
            let str_idx_path = run_dir.join("strings.idx");
            let str_fwd_path = run_dir.join("strings.fwd");
            if str_idx_path.exists() && str_fwd_path.exists() {
                let str_fwd_file = std::fs::File::open(&str_fwd_path).map_err(|e| {
                    IndexerError::StorageRead(format!("open {}: {}", str_fwd_path.display(), e))
                })?;
                // SAFETY: The file is opened read-only and is not concurrently modified.
                // The forward-dict file is an immutable index artifact written before this point.
                let str_fwd_data = unsafe { memmap2::Mmap::map(&str_fwd_file) }.map_err(|e| {
                    IndexerError::StorageRead(format!("mmap {}: {}", str_fwd_path.display(), e))
                })?;

                if trust_sorted_order_invariants {
                    // --- Streaming path: IDs/keys already in lexicographic order. ---
                    // Count comes from the FSI1 header without materializing the Vecs.
                    let count = {
                        let mut reader = ForwardIndexReader::open(&str_idx_path)?;
                        let mut n = 0u64;
                        while reader.next()?.is_some() {
                            n += 1;
                        }
                        n as usize
                    };
                    tracing::info!(
                        strings = count,
                        fwd_bytes = str_fwd_data.len(),
                        "string dict files loaded"
                    );

                    let fwd_packs = {
                        let mut idx_reader = ForwardIndexReader::open(&str_idx_path)?;
                        let mut id = 0u64;
                        let next = || -> Result<Option<StringEntry>> {
                            match idx_reader.next()? {
                                Some((off, len)) => {
                                    let entry = StringEntry { id, off, len };
                                    id += 1;
                                    Ok(Some(entry))
                                }
                                None => Ok(None),
                            }
                        };
                        build_string_forward_packs(content_store, &str_fwd_data, next).await?
                    };

                    let reverse = {
                        let mut idx_reader = ForwardIndexReader::open(&str_idx_path)?;
                        let mut id = 0u64;
                        let next = || -> Result<Option<StringEntry>> {
                            match idx_reader.next()? {
                                Some((off, len)) => {
                                    let entry = StringEntry { id, off, len };
                                    id += 1;
                                    Ok(Some(entry))
                                }
                                None => Ok(None),
                            }
                        };
                        build_string_reverse_tree(content_store, &str_fwd_data, next).await?
                    };

                    tracing::info!("string dict artifacts uploaded");
                    Ok::<_, IndexerError>((count, fwd_packs, reverse))
                } else {
                    // --- Materialized path: read fully, sort indices as needed. ---
                    let (str_offsets, str_lens) =
                        run_index::dict_io::read_forward_index(&str_idx_path).map_err(|e| {
                            IndexerError::StorageRead(format!(
                                "read {}: {}",
                                str_idx_path.display(),
                                e
                            ))
                        })?;
                    let count = str_offsets.len();
                    tracing::info!(
                        strings = count,
                        fwd_bytes = str_fwd_data.len(),
                        "string dict files loaded"
                    );

                    // Forward order: IDs are 0..count contiguous, file order.
                    let fwd_packs = {
                        let mut pos = 0usize;
                        let next = || -> Result<Option<StringEntry>> {
                            if pos >= count {
                                return Ok(None);
                            }
                            let entry = StringEntry {
                                id: pos as u64,
                                off: str_offsets[pos],
                                len: str_lens[pos],
                            };
                            pos += 1;
                            Ok(Some(entry))
                        };
                        build_string_forward_packs(content_store, &str_fwd_data, next).await?
                    };

                    // Reverse order: by string bytes.
                    let strings_sorted = {
                        let mut ok = true;
                        for i in 1..count {
                            let oa = str_offsets[i - 1] as usize;
                            let la = str_lens[i - 1] as usize;
                            let ob = str_offsets[i] as usize;
                            let lb = str_lens[i] as usize;
                            if str_fwd_data[oa..oa + la] > str_fwd_data[ob..ob + lb] {
                                ok = false;
                                break;
                            }
                        }
                        ok
                    };
                    let rev_order: Option<Vec<usize>> = if strings_sorted {
                        None
                    } else {
                        tracing::info!("building string reverse tree (fallback index-sort)");
                        let mut v: Vec<usize> = (0..count).collect();
                        v.sort_unstable_by(|&a, &b| {
                            let oa = str_offsets[a] as usize;
                            let la = str_lens[a] as usize;
                            let ob = str_offsets[b] as usize;
                            let lb = str_lens[b] as usize;
                            str_fwd_data[oa..oa + la].cmp(&str_fwd_data[ob..ob + lb])
                        });
                        Some(v)
                    };
                    let reverse = {
                        let mut pos = 0usize;
                        let next = || -> Result<Option<StringEntry>> {
                            let i = match &rev_order {
                                None => {
                                    if pos >= count {
                                        None
                                    } else {
                                        Some(pos)
                                    }
                                }
                                Some(order) => order.get(pos).copied(),
                            };
                            match i {
                                Some(i) => {
                                    pos += 1;
                                    Ok(Some(StringEntry {
                                        id: i as u64,
                                        off: str_offsets[i],
                                        len: str_lens[i],
                                    }))
                                }
                                None => Ok(None),
                            }
                        };
                        build_string_reverse_tree(content_store, &str_fwd_data, next).await?
                    };

                    tracing::info!("string dict artifacts uploaded");
                    Ok::<_, IndexerError>((count, fwd_packs, reverse))
                }
            } else {
                // No strings persisted — empty packs + empty reverse tree
                let kind_rev = ContentKind::DictBlob {
                    dict: DictKind::StringReverse,
                };
                let empty_branch = DictBranch { leaves: vec![] };
                let empty_bytes = empty_branch.encode();
                let wr_rev = content_store
                    .put(kind_rev, &empty_bytes)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
                Ok((
                    0,
                    vec![], // no forward packs
                    DictTreeRefs {
                        branch: wr_rev,
                        leaves: vec![],
                    },
                ))
            }
        },
        // Task C: Numbig arenas (per-graph subdirectories)
        async {
            let mut numbig: BTreeMap<String, BTreeMap<String, ContentId>> = BTreeMap::new();
            // Scan for g_{id}/numbig/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {e}")))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {e}")))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let nb_dir = dir_entry.path().join("numbig");
                    if nb_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&nb_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read numbig dir: {e}"))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read numbig entry: {e}"))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".nba") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let cid = upload_dict_file(
                                            content_store,
                                            &entry.path(),
                                            DictKind::NumBig { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;
                                        per_pred.insert(p_id.to_string(), cid);
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            numbig.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(numbig)
        },
        // Task D: Vector arenas (per-graph subdirectories)
        async {
            let mut vectors: BTreeMap<String, BTreeMap<String, VectorDictRef>> = BTreeMap::new();
            // Scan for g_{id}/vectors/ subdirectories
            for dir_entry in std::fs::read_dir(run_dir)
                .map_err(|e| IndexerError::StorageRead(format!("read run_dir: {e}")))?
            {
                let dir_entry = dir_entry
                    .map_err(|e| IndexerError::StorageRead(format!("read run_dir entry: {e}")))?;
                let dir_name = dir_entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if let Some(g_id_str) = dir_name_str.strip_prefix("g_") {
                    let vec_dir = dir_entry.path().join("vectors");
                    if vec_dir.exists() {
                        let mut per_pred = BTreeMap::new();
                        for entry in std::fs::read_dir(&vec_dir).map_err(|e| {
                            IndexerError::StorageRead(format!("read vectors dir: {e}"))
                        })? {
                            let entry = entry.map_err(|e| {
                                IndexerError::StorageRead(format!("read vectors entry: {e}"))
                            })?;
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if let Some(rest) = name_str.strip_prefix("p_") {
                                if let Some(id_str) = rest.strip_suffix(".vam") {
                                    if let Ok(p_id) = id_str.parse::<u32>() {
                                        let manifest_bytes =
                                            tokio::fs::read(entry.path()).await.map_err(|e| {
                                                IndexerError::StorageRead(format!(
                                                    "read vector manifest: {e}"
                                                ))
                                            })?;
                                        let manifest =
                                            fluree_db_binary_index::arena::vector::read_vector_manifest(
                                                &manifest_bytes,
                                            )
                                            .map_err(
                                                |e| {
                                                    IndexerError::StorageRead(format!(
                                                        "parse vector manifest: {e}"
                                                    ))
                                                },
                                            )?;

                                        let mut shard_cids =
                                            Vec::with_capacity(manifest.shards.len());
                                        let mut shard_infos =
                                            Vec::with_capacity(manifest.shards.len());
                                        for (shard_idx, shard_info) in
                                            manifest.shards.iter().enumerate()
                                        {
                                            let shard_path =
                                                vec_dir.join(format!("p_{p_id}_s_{shard_idx}.vas"));
                                            let shard_cid = upload_dict_file(
                                                content_store,
                                                &shard_path,
                                                DictKind::VectorShard { p_id },
                                                "dict artifact uploaded to CAS (from disk)",
                                            )
                                            .await?;
                                            shard_infos.push(
                                                fluree_db_binary_index::arena::vector::ShardInfo {
                                                    cas: shard_cid.to_string(),
                                                    count: shard_info.count,
                                                },
                                            );
                                            shard_cids.push(shard_cid);
                                        }

                                        let final_manifest =
                                            fluree_db_binary_index::arena::vector::VectorManifest {
                                                shards: shard_infos,
                                                ..manifest
                                            };
                                        let manifest_json = serde_json::to_vec_pretty(
                                            &final_manifest,
                                        )
                                        .map_err(|e| {
                                            IndexerError::StorageWrite(format!(
                                                "serialize vector manifest: {e}"
                                            ))
                                        })?;
                                        let final_manifest_path =
                                            vec_dir.join(format!("p_{p_id}_final.vam"));
                                        std::fs::write(&final_manifest_path, &manifest_json)
                                            .map_err(|e| {
                                                IndexerError::StorageWrite(format!(
                                                    "write final vector manifest: {e}"
                                                ))
                                            })?;
                                        let manifest_cid = upload_dict_file(
                                            content_store,
                                            &final_manifest_path,
                                            DictKind::VectorManifest { p_id },
                                            "dict artifact uploaded to CAS (from disk)",
                                        )
                                        .await?;

                                        per_pred.insert(
                                            p_id.to_string(),
                                            VectorDictRef {
                                                p_id,
                                                manifest: manifest_cid,
                                                shards: shard_cids,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        if !per_pred.is_empty() {
                            vectors.insert(g_id_str.to_string(), per_pred);
                        }
                    }
                }
            }
            Ok::<_, IndexerError>(vectors)
        },
    )?;

    let (subject_fwd_ns_packs, subject_reverse, subject_id_encoding, subject_watermarks) =
        subject_result;
    let (string_count, string_fwd_packs, string_reverse) = string_result;

    let string_watermark = if string_count > 0 {
        (string_count - 1) as u32
    } else {
        0
    };

    tracing::info!(
        subjects = subject_watermarks.len(),
        strings = string_count,
        numbig_graphs = numbig.len(),
        vector_graphs = vectors.len(),
        ?subject_id_encoding,
        watermarks = subject_watermarks.len(),
        string_watermark,
        rss_mib = crate::mem::current_rss_mib(),
        "dict upload: done (dictionary trees built and uploaded to CAS)"
    );
    Ok(UploadedDicts {
        dict_refs: DictRefs {
            forward_packs: DictPackRefs {
                string_fwd_packs,
                subject_fwd_ns_packs,
            },
            subject_reverse,
            string_reverse,
        },
        subject_id_encoding,
        subject_watermarks,
        string_watermark,
        graph_iris,
        datatype_iris,
        language_tags,
        numbig,
        vectors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::resolve::chunk_dict::{ChunkStringDict, ChunkSubjectDict};
    use fluree_db_core::storage::MemoryContentStore;
    use std::collections::HashMap;
    use std::path::Path;

    fn write_subject_chunk(
        dir: &Path,
        chunk_id: usize,
        entries: &[(u16, &[u8])],
    ) -> std::path::PathBuf {
        let mut dict = ChunkSubjectDict::new();
        for &(ns, name) in entries {
            dict.get_or_insert(ns, name);
        }
        let path = dir.join(format!("chunk_{chunk_id:05}.subjects.voc"));
        dict.sort_and_write_sorted_vocab(&path).unwrap();
        path
    }

    fn write_string_chunk(dir: &Path, chunk_id: usize, entries: &[&[u8]]) -> std::path::PathBuf {
        let mut dict = ChunkStringDict::new();
        for &s in entries {
            dict.get_or_insert(s);
        }
        let path = dir.join(format!("chunk_{chunk_id:05}.strings.voc"));
        dict.sort_and_write_sorted_vocab(&path).unwrap();
        path
    }

    /// Build a realistic sorted fixture (subjects + strings) in `run_dir` via the
    /// production vocab-merge writers, plus the small graph/datatype/language dicts.
    fn build_fixture(run_dir: &Path, namespace_codes: &HashMap<u16, String>, empty_strings: bool) {
        use crate::run_index::resolve::global_dict::PredicateDict;
        use crate::run_index::vocab::vocab_merge;

        let remap_dir = run_dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        // Multiple chunks, deliberately unsorted insertion within each chunk,
        // cross-chunk duplication, multiple namespaces.
        let s0 = write_subject_chunk(
            run_dir,
            0,
            &[(10, b"zebra"), (5, b"Bob"), (5, b"Alice"), (10, b"apple")],
        );
        let s1 = write_subject_chunk(
            run_dir,
            1,
            &[(5, b"Alice"), (10, b"mango"), (5, b"Carol"), (10, b"apple")],
        );
        let s2 = write_subject_chunk(run_dir, 2, &[(20, b"x"), (5, b"Dave"), (20, b"a")]);
        vocab_merge::merge_subject_vocabs(
            &[s0, s1, s2],
            &[0, 1, 2],
            &remap_dir,
            run_dir,
            namespace_codes,
        )
        .unwrap();

        if empty_strings {
            // No strings.{fwd,idx} written → exercises the empty-strings branch.
        } else {
            let t0 = write_string_chunk(run_dir, 0, &[b"pear", b"apple", b"cherry"]);
            let t1 = write_string_chunk(run_dir, 1, &[b"apple", b"banana", b"date"]);
            let t2 = write_string_chunk(run_dir, 2, &[b"fig", b"banana"]);
            vocab_merge::merge_string_vocabs(&[t0, t1, t2], &[0, 1, 2], &remap_dir, run_dir)
                .unwrap();
        }

        // Small dicts the upload function reads.
        let mut graphs = PredicateDict::new();
        graphs.get_or_insert("https://ns.flur.ee/ledger#default");
        run_index::dict_io::write_predicate_dict(&run_dir.join("graphs.dict"), &graphs).unwrap();

        let mut datatypes = PredicateDict::new();
        datatypes.get_or_insert("http://www.w3.org/2001/XMLSchema#string");
        run_index::dict_io::write_predicate_dict(&run_dir.join("datatypes.dict"), &datatypes)
            .unwrap();
    }

    async fn run_upload(
        run_dir: &Path,
        namespace_codes: &HashMap<u16, String>,
        trust: bool,
    ) -> (MemoryContentStore, UploadedDicts) {
        let store = MemoryContentStore::new();
        let out = upload_dicts_from_disk(&store, run_dir, namespace_codes, trust)
            .await
            .unwrap();
        (store, out)
    }

    fn assert_dicts_equal(a: &UploadedDicts, b: &UploadedDicts) {
        assert_eq!(
            a.dict_refs.forward_packs.subject_fwd_ns_packs,
            b.dict_refs.forward_packs.subject_fwd_ns_packs,
            "subject forward packs differ"
        );
        assert_eq!(
            a.dict_refs.forward_packs.string_fwd_packs, b.dict_refs.forward_packs.string_fwd_packs,
            "string forward packs differ"
        );
        assert_eq!(
            a.dict_refs.subject_reverse.branch, b.dict_refs.subject_reverse.branch,
            "subject reverse branch differs"
        );
        assert_eq!(
            a.dict_refs.subject_reverse.leaves, b.dict_refs.subject_reverse.leaves,
            "subject reverse leaves differ"
        );
        assert_eq!(
            a.dict_refs.string_reverse.branch, b.dict_refs.string_reverse.branch,
            "string reverse branch differs"
        );
        assert_eq!(
            a.dict_refs.string_reverse.leaves, b.dict_refs.string_reverse.leaves,
            "string reverse leaves differ"
        );
        assert_eq!(
            a.subject_id_encoding, b.subject_id_encoding,
            "subject_id_encoding differs"
        );
        assert_eq!(
            a.subject_watermarks, b.subject_watermarks,
            "subject_watermarks differ"
        );
        assert_eq!(
            a.string_watermark, b.string_watermark,
            "string_watermark differs"
        );
    }

    /// Byte-compat oracle: the streaming (trust=true) and materialized
    /// (trust=false) paths must produce identical CAS artifacts and metadata.
    /// MemoryContentStore is content-addressed, so equal refs ⇒ equal bytes.
    #[tokio::test]
    async fn streaming_matches_materialized_byte_for_byte() {
        let ns = HashMap::from([
            (5u16, "http://ex.org/".to_string()),
            (10, "http://foo/".to_string()),
            (20, "http://bar/".to_string()),
        ]);

        let dir = std::env::temp_dir().join("fluree_upload_dicts_oracle_full");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        build_fixture(&dir, &ns, false);

        let (_s_mat, mat) = run_upload(&dir, &ns, false).await;
        let (_s_str, streamed) = run_upload(&dir, &ns, true).await;
        assert_dicts_equal(&mat, &streamed);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same oracle with no strings persisted (empty-strings branch).
    #[tokio::test]
    async fn streaming_matches_materialized_empty_strings() {
        let ns = HashMap::from([
            (5u16, "http://ex.org/".to_string()),
            (10, "http://foo/".to_string()),
            (20, "http://bar/".to_string()),
        ]);

        let dir = std::env::temp_dir().join("fluree_upload_dicts_oracle_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        build_fixture(&dir, &ns, true);

        let (_s_mat, mat) = run_upload(&dir, &ns, false).await;
        let (_s_str, streamed) = run_upload(&dir, &ns, true).await;
        assert_dicts_equal(&mat, &streamed);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sequential `SubjectSidReader` yields exactly the same sequence as
    /// `dict_io::read_subject_sid_map`.
    #[test]
    fn subject_sid_reader_matches_materialized() {
        let dir = std::env::temp_dir().join("fluree_upload_dicts_sid_reader");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subjects.sids");
        let sids: Vec<u64> = vec![0, 1, (5u64 << 48), (5u64 << 48) | 1, (10u64 << 48) | 42];
        run_index::dict_io::write_subject_sid_map(&path, &sids).unwrap();

        let expected = run_index::dict_io::read_subject_sid_map(&path).unwrap();
        let mut reader = SubjectSidReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(v) = reader.next().unwrap() {
            got.push(v);
        }
        assert_eq!(got, expected);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sequential `ForwardIndexReader` yields exactly the same `(off, len)`
    /// sequence as `dict_io::read_forward_index`.
    #[test]
    fn forward_index_reader_matches_materialized() {
        let dir = std::env::temp_dir().join("fluree_upload_dicts_fwd_reader");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subjects.idx");
        let offsets = vec![0u64, 30, 55, 80, 140];
        let lens = vec![30u32, 25, 25, 60, 5];
        run_index::dict_io::write_subject_index(&path, &offsets, &lens).unwrap();

        let (exp_offsets, exp_lens) = run_index::dict_io::read_forward_index(&path).unwrap();
        let expected: Vec<(u64, u32)> = exp_offsets.into_iter().zip(exp_lens).collect();

        let mut reader = ForwardIndexReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(v) = reader.next().unwrap() {
            got.push(v);
        }
        assert_eq!(got, expected);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Memory stress (cgroup hard-cap repro vehicle) ----

    /// Process peak resident set in bytes (high-water mark).
    fn peak_rss_bytes() -> u64 {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return 0;
        }
        let maxrss = usage.ru_maxrss.max(0) as u64;
        // Linux reports kibibytes; macOS reports bytes.
        if cfg!(target_os = "macos") {
            maxrss
        } else {
            maxrss * 1024
        }
    }

    /// A content store that computes the (content-addressed) CID and discards
    /// the bytes, so the stress measurement reflects the upload's own working
    /// set rather than retained CAS artifacts.
    #[derive(Debug, Default)]
    struct DiscardingContentStore {
        puts: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl ContentStore for DiscardingContentStore {
        async fn has(&self, _id: &ContentId) -> fluree_db_core::error::Result<bool> {
            Ok(false)
        }
        async fn get(&self, id: &ContentId) -> fluree_db_core::error::Result<Vec<u8>> {
            Err(fluree_db_core::error::Error::not_found(id.to_string()))
        }
        async fn put(
            &self,
            kind: fluree_db_core::ContentKind,
            bytes: &[u8],
        ) -> fluree_db_core::error::Result<ContentId> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(ContentId::new(kind, bytes))
        }
        async fn put_with_id(
            &self,
            _id: &ContentId,
            _bytes: &[u8],
        ) -> fluree_db_core::error::Result<()> {
            Ok(())
        }
        async fn release(&self, _id: &ContentId) -> fluree_db_core::error::Result<()> {
            Ok(())
        }
    }

    /// Build a large sorted fixture with `chunks * local` DISTINCT subjects and
    /// strings (unique terms per chunk → no cross-chunk dedup → maximal distinct
    /// cardinality, which is what the materialized upload path allocates for).
    fn build_large_fixture(
        run_dir: &Path,
        namespace_codes: &HashMap<u16, String>,
        chunks: usize,
        local: usize,
    ) {
        use crate::run_index::resolve::global_dict::PredicateDict;
        use crate::run_index::vocab::vocab_merge;

        let remap_dir = run_dir.join("remap");
        std::fs::create_dir_all(&remap_dir).unwrap();

        let mut subj_vocs = Vec::with_capacity(chunks);
        let mut str_vocs = Vec::with_capacity(chunks);
        for c in 0..chunks {
            let mut sd = ChunkSubjectDict::new();
            let mut td = ChunkStringDict::new();
            for j in 0..local {
                let g = c * local + j;
                sd.get_or_insert(5, format!("s{g:010}").as_bytes());
                td.get_or_insert(format!("v{g:010}").as_bytes());
            }
            let sp = run_dir.join(format!("chunk_{c:05}.subjects.voc"));
            let tp = run_dir.join(format!("chunk_{c:05}.strings.voc"));
            sd.sort_and_write_sorted_vocab(&sp).unwrap();
            td.sort_and_write_sorted_vocab(&tp).unwrap();
            subj_vocs.push(sp);
            str_vocs.push(tp);
        }
        let ids: Vec<usize> = (0..chunks).collect();
        vocab_merge::merge_subject_vocabs(&subj_vocs, &ids, &remap_dir, run_dir, namespace_codes)
            .unwrap();
        vocab_merge::merge_string_vocabs(&str_vocs, &ids, &remap_dir, run_dir).unwrap();

        let mut graphs = PredicateDict::new();
        graphs.get_or_insert("https://ns.flur.ee/ledger#default");
        run_index::dict_io::write_predicate_dict(&run_dir.join("graphs.dict"), &graphs).unwrap();
        let mut datatypes = PredicateDict::new();
        datatypes.get_or_insert("http://www.w3.org/2001/XMLSchema#string");
        run_index::dict_io::write_predicate_dict(&run_dir.join("datatypes.dict"), &datatypes)
            .unwrap();
    }

    /// Drives the cgroup hard-cap repro (`scripts/import-memory-repro.sh`).
    ///
    /// `FLUREE_UPLOAD_TRUST=0` exercises the materialized path (full term-sized
    /// Vecs — the pre-fix behavior); `=1` exercises the streaming path. Under a
    /// container `--memory` cap below the materialized footprint, `0` OOM-kills
    /// while `1` survives. Scale via `FLUREE_UPLOAD_CHUNKS` / `FLUREE_UPLOAD_LOCAL`.
    #[tokio::test]
    #[ignore = "stress/repro; run explicitly, ideally under a container memory cap"]
    async fn stress_upload_memory() {
        let env = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let chunks = env("FLUREE_UPLOAD_CHUNKS", 200);
        let local = env("FLUREE_UPLOAD_LOCAL", 50_000);
        let trust = env("FLUREE_UPLOAD_TRUST", 1) != 0;
        let n = chunks * local;

        let ns = HashMap::from([(5u16, "http://ex.org/".to_string())]);
        let dir = std::env::temp_dir().join("fluree_upload_stress");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        build_large_fixture(&dir, &ns, chunks, local);
        let before = peak_rss_bytes();

        let store = DiscardingContentStore::default();
        let out = upload_dicts_from_disk(&store, &dir, &ns, trust)
            .await
            .unwrap();

        let after = peak_rss_bytes();
        // Rough materialized footprint: subjects ~44 B/term + strings ~36 B/term,
        // both tasks live concurrently.
        let materialized_bytes = (n as u64) * 80;
        eprintln!(
            "upload-stress: trust={trust} chunks={chunks} local={local} N={n} \
             subjects={} strings={} peak_rss_growth={}MiB \
             (materialized footprint ~{}MiB)",
            out.subject_watermarks.len(),
            out.string_watermark + 1,
            after.saturating_sub(before) / (1024 * 1024),
            materialized_bytes / (1024 * 1024),
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
