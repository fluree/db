//! BM25 Index Serialization
//!
//! Provides snapshot serialization and deserialization for the BM25 index
//! using the postcard binary format. This enables persistence of the index
//! for graph source storage.
//!
//! Supports two versions:
//! - Version 3: Inverted index with delta-encoded doc_ids for compact serialization
//! - Version 4: Chunked format — root blob with metadata + separate posting leaflet blobs
//!
//! Single-blob writes (v3) via `serialize()`. Chunked writes (v4) via
//! `prepare_chunked()` + `finalize_chunked_root()`.
//! The caller (API layer) decides which format to write based on storage backend.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::index::{
    Bm25Config, Bm25Index, Bm25Stats, DocMeta, GraphSourceWatermark, Posting, PostingList,
    PropertyDeps, TermEntry,
};

/// Error type for serialization operations.
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("Postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid snapshot format: {0}")]
    InvalidFormat(String),
}

pub type Result<T> = std::result::Result<T, SerializeError>;

/// Magic bytes for BM25 snapshot files
const SNAPSHOT_MAGIC: &[u8; 4] = b"BM25";

/// V3 snapshot format version (inverted index, delta-encoded doc_ids)
const SNAPSHOT_VERSION_V3: u8 = 3;

/// Version written by serialize()
const SNAPSHOT_VERSION: u8 = SNAPSHOT_VERSION_V3;

// ============================================================================
// V3 delta-encoded types
// ============================================================================

/// V3 snapshot: delta-encoded posting lists for compact serialization.
///
/// Postcard already uses varint encoding internally (u32 < 128 → 1 byte).
/// Delta-encoding doc_ids converts large absolute values into small deltas,
/// which postcard's varint then compresses efficiently.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaBm25Snapshot {
    terms: BTreeMap<Arc<str>, TermEntry>,
    posting_lists: Vec<DeltaPostingList>,
    doc_meta: Vec<Option<DocMeta>>,
    stats: Bm25Stats,
    config: Bm25Config,
    watermark: GraphSourceWatermark,
    property_deps: PropertyDeps,
    next_term_idx: u32,
    next_doc_id: u32,
}

/// A posting list with delta-encoded doc_ids.
///
/// `doc_id_deltas[i]` = postings[i].doc_id - postings[i-1].doc_id (first is absolute).
/// `term_freqs[i]` = postings[i].term_freq (unchanged, already small).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaPostingList {
    doc_id_deltas: Vec<u32>,
    term_freqs: Vec<u32>,
}

/// Convert a posting list to delta-encoded form for v3 serialization.
///
/// Precondition: postings must be sorted by doc_id (guaranteed by compact()).
/// Returns an error if doc_ids are not sorted, since unsorted input would
/// produce a corrupt delta stream that silently yields wrong results on decode.
fn posting_list_to_delta(pl: &PostingList) -> Result<DeltaPostingList> {
    let mut deltas = Vec::with_capacity(pl.postings.len());
    let mut freqs = Vec::with_capacity(pl.postings.len());
    let mut prev = 0u32;
    for p in &pl.postings {
        let delta = p.doc_id.checked_sub(prev).ok_or_else(|| {
            SerializeError::InvalidFormat(format!(
                "Posting list not sorted: doc_id {} < prev {}",
                p.doc_id, prev
            ))
        })?;
        deltas.push(delta);
        prev = p.doc_id;
        freqs.push(p.term_freq);
    }
    Ok(DeltaPostingList {
        doc_id_deltas: deltas,
        term_freqs: freqs,
    })
}

/// Expand delta-encoded posting list back to absolute doc_ids.
///
/// Returns Err on length mismatch or doc_id overflow (corrupted snapshot).
fn delta_to_posting_list(dpl: DeltaPostingList) -> Result<PostingList> {
    if dpl.doc_id_deltas.len() != dpl.term_freqs.len() {
        return Err(SerializeError::InvalidFormat(format!(
            "DeltaPostingList length mismatch: {} deltas vs {} freqs",
            dpl.doc_id_deltas.len(),
            dpl.term_freqs.len()
        )));
    }
    let mut postings = Vec::with_capacity(dpl.doc_id_deltas.len());
    let mut doc_id = 0u32;
    for (delta, tf) in dpl.doc_id_deltas.into_iter().zip(dpl.term_freqs) {
        doc_id = doc_id.checked_add(delta).ok_or_else(|| {
            SerializeError::InvalidFormat("doc_id overflow in delta decoding".to_string())
        })?;
        postings.push(Posting {
            doc_id,
            term_freq: tf,
        });
    }
    Ok(PostingList {
        postings,
        block_meta: Vec::new(),
    })
}

// ============================================================================
// V4 chunked types
// ============================================================================

/// V4 snapshot format version (chunked: root blob + posting leaflets)
const SNAPSHOT_VERSION_V4: u8 = 4;

/// Target uncompressed byte size per posting leaflet.
/// With ~3:1 zstd+delta compression, this targets ~2MB compressed leaflets.
const LEAFLET_TARGET_UNCOMPRESSED: usize = 6 * 1024 * 1024;

/// Maximum allowed decompressed size for a v4 root blob (256 MB).
/// Protects against zip-bomb attacks where a small compressed payload
/// claims a huge uncompressed_len.
const MAX_ROOT_DECOMPRESSED: usize = 256 * 1024 * 1024;

/// Maximum allowed decompressed size for a posting leaflet blob (64 MB).
/// Leaflets target ~6MB uncompressed; 64MB provides generous headroom
/// while still bounding memory allocation from untrusted data.
const MAX_LEAFLET_DECOMPRESSED: usize = 64 * 1024 * 1024;

/// V4 root: contains everything except posting list data.
/// Posting lists are stored in separate CAS blobs referenced by `posting_leaflets`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkedBm25Root {
    terms: BTreeMap<Arc<str>, TermEntry>,
    doc_meta: Vec<Option<DocMeta>>,
    stats: Bm25Stats,
    config: Bm25Config,
    watermark: GraphSourceWatermark,
    property_deps: PropertyDeps,
    next_term_idx: u32,
    next_doc_id: u32,
    posting_leaflets: Vec<LeafletEntry>,
}

/// Leaflet routing entry stored in the root. CID stored as raw bytes to avoid
/// coupling the query crate's public API to the ContentId storage type.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeafletEntry {
    cid_bytes: Vec<u8>,
    first_term_idx: u32,
    last_term_idx: u32,
    list_count: u32,
    posting_count: u32,
}

/// Posting leaflet payload. Serialized (postcard) then compressed (zstd)
/// to form each leaflet blob. Includes `first_term_idx` and `list_count`
/// for validation on deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostingLeafletPayload {
    first_term_idx: u32,
    list_count: u32,
    posting_lists: Vec<DeltaPostingList>,
}

// -- Public v4 types --

/// Lightweight per-leaflet metadata (no CID). Returned by `prepare_chunked()`;
/// the caller fills in CID bytes after writing the leaflet blob to CAS.
#[derive(Debug, Clone)]
pub struct LeafletInfo {
    pub first_term_idx: u32,
    pub last_term_idx: u32,
    pub list_count: u32,
    pub posting_count: u32,
}

/// Opaque result of the first phase of chunked serialization.
/// Contains everything needed to build the final v4 root once leaflet CIDs are known.
pub struct ChunkedPrepResult {
    /// Private root data (terms, doc_meta, stats, config, etc.)
    root: ChunkedBm25RootData,
    /// Compressed leaflet blobs, ready for CAS write. One per leaflet group.
    pub leaflet_blobs: Vec<Vec<u8>>,
    /// Per-leaflet metadata (same order as `leaflet_blobs`).
    pub leaflet_infos: Vec<LeafletInfo>,
}

/// Private inner struct holding root data before CIDs are known.
struct ChunkedBm25RootData {
    terms: BTreeMap<Arc<str>, TermEntry>,
    doc_meta: Vec<Option<DocMeta>>,
    stats: Bm25Stats,
    config: Bm25Config,
    watermark: GraphSourceWatermark,
    property_deps: PropertyDeps,
    next_term_idx: u32,
    next_doc_id: u32,
}

/// Leaflet reference extracted from a deserialized v4 root.
/// CID is raw bytes — the caller converts to `ContentId` at the storage boundary.
#[derive(Debug, Clone)]
pub struct LeafletRef {
    pub cid_bytes: Vec<u8>,
    pub first_term_idx: u32,
    pub last_term_idx: u32,
    pub list_count: u32,
    pub posting_count: u32,
}

/// Deserialized v4 chunked root. Internal data is opaque; access is via methods.
pub struct ChunkedRoot {
    root: ChunkedBm25Root,
}

impl ChunkedRoot {
    /// Leaflet routing references (for fetching from CAS).
    pub fn leaflet_refs(&self) -> Vec<LeafletRef> {
        self.root
            .posting_leaflets
            .iter()
            .map(|e| LeafletRef {
                cid_bytes: e.cid_bytes.clone(),
                first_term_idx: e.first_term_idx,
                last_term_idx: e.last_term_idx,
                list_count: e.list_count,
                posting_count: e.posting_count,
            })
            .collect()
    }

    pub fn next_term_idx(&self) -> u32 {
        self.root.next_term_idx
    }

    pub fn next_doc_id(&self) -> u32 {
        self.root.next_doc_id
    }

    pub fn num_docs(&self) -> u64 {
        self.root.stats.num_docs
    }

    pub fn num_terms(&self) -> usize {
        self.root.terms.len()
    }

    /// Look up a term's entry (for selective loading: resolve query terms to term_idx).
    pub fn get_term(&self, term: &str) -> Option<&TermEntry> {
        self.root.terms.get(term)
    }

    /// Find leaflet refs that contain any of the given term_idx values.
    pub fn leaflet_refs_for_terms(&self, term_idxs: &[u32]) -> Vec<LeafletRef> {
        self.root
            .posting_leaflets
            .iter()
            .filter(|e| {
                term_idxs
                    .iter()
                    .any(|&idx| idx >= e.first_term_idx && idx <= e.last_term_idx)
            })
            .map(|e| LeafletRef {
                cid_bytes: e.cid_bytes.clone(),
                first_term_idx: e.first_term_idx,
                last_term_idx: e.last_term_idx,
                list_count: e.list_count,
                posting_count: e.posting_count,
            })
            .collect()
    }
}

// -- V4 internal helpers --

/// Group posting lists into leaflets by consecutive term_idx ranges.
/// Returns `(first_term_idx, posting_list_slice)` for each group.
fn group_posting_lists(
    posting_lists: &[PostingList],
    target_uncompressed_bytes: usize,
) -> Vec<(u32, &[PostingList])> {
    if posting_lists.is_empty() {
        return Vec::new();
    }

    let mut groups = Vec::new();
    let mut group_start = 0usize;
    let mut group_bytes = 0usize;

    for (i, pl) in posting_lists.iter().enumerate() {
        let pl_bytes = pl.postings.len() * 8; // 4 bytes doc_id + 4 bytes term_freq

        // If adding this posting list would exceed the target and we have at least
        // one list in the current group, emit the group first.
        if group_bytes + pl_bytes > target_uncompressed_bytes && group_start < i {
            groups.push((group_start as u32, &posting_lists[group_start..i]));
            group_start = i;
            group_bytes = 0;
        }

        group_bytes += pl_bytes;
    }

    // Emit final group
    if group_start < posting_lists.len() {
        groups.push((
            group_start as u32,
            &posting_lists[group_start..posting_lists.len()],
        ));
    }

    groups
}

/// Serialize a group of posting lists into a compressed leaflet blob.
/// Applies delta encoding (reusing v3 DeltaPostingList) then postcard + zstd.
fn serialize_leaflet_blob(posting_lists: &[PostingList], first_term_idx: u32) -> Result<Vec<u8>> {
    let payload = PostingLeafletPayload {
        first_term_idx,
        list_count: posting_lists.len() as u32,
        posting_lists: posting_lists
            .iter()
            .map(posting_list_to_delta)
            .collect::<Result<Vec<_>>>()?,
    };

    let postcard_bytes = postcard::to_allocvec(&payload)?;
    let compressed = zstd::encode_all(postcard_bytes.as_slice(), 3)
        .map_err(|e| SerializeError::Io(std::io::Error::other(e)))?;
    Ok(compressed)
}

// ============================================================================
// Public API
// ============================================================================

/// Serialize a BM25 index to bytes.
///
/// Calls `compact()` first to ensure deterministic CAS output.
/// Writes v3 format with delta-encoded posting list doc_ids.
pub fn serialize(index: &Bm25Index) -> Result<Vec<u8>> {
    // Compact for deterministic serialization
    let mut compacted = index.clone();
    compacted.compact();

    // Capture derived values before moving fields into the snapshot
    let next_term_idx = compacted.terms.len() as u32;
    let next_doc_id = compacted.next_doc_id();

    // Convert to delta-encoded snapshot
    let snapshot = DeltaBm25Snapshot {
        terms: compacted.terms,
        posting_lists: compacted
            .posting_lists
            .iter()
            .map(posting_list_to_delta)
            .collect::<Result<Vec<_>>>()?,
        doc_meta: compacted.doc_meta,
        stats: compacted.stats,
        config: compacted.config,
        watermark: compacted.watermark,
        property_deps: compacted.property_deps,
        next_term_idx,
        next_doc_id,
    };

    let mut data = Vec::new();

    // Write header
    data.extend_from_slice(SNAPSHOT_MAGIC);
    data.push(SNAPSHOT_VERSION);

    // Serialize the snapshot with postcard
    let index_bytes = postcard::to_allocvec(&snapshot)?;

    // Write length prefix (4 bytes, big-endian)
    let len = index_bytes.len() as u32;
    data.extend_from_slice(&len.to_be_bytes());

    // Write index data
    data.extend_from_slice(&index_bytes);

    Ok(data)
}

/// Deserialize a BM25 index from bytes.
///
/// Only supports v3 (delta-encoded doc_ids). V4 chunked format requires
/// the two-phase `deserialize_chunked_root()` + `deserialize_posting_leaflet()` API.
pub fn deserialize(data: &[u8]) -> Result<Bm25Index> {
    if data.len() < 9 {
        return Err(SerializeError::InvalidFormat(
            "Data too short for header".to_string(),
        ));
    }

    // Check magic bytes
    if &data[0..4] != SNAPSHOT_MAGIC {
        return Err(SerializeError::InvalidFormat(
            "Invalid magic bytes".to_string(),
        ));
    }

    let version = data[4];

    // V4 chunked format has a different header layout (13 bytes) and requires
    // two-phase loading — redirect callers to the chunked API.
    if version == SNAPSHOT_VERSION_V4 {
        return Err(SerializeError::InvalidFormat(
            "V4 chunked format cannot be loaded via deserialize(); \
             use deserialize_chunked_root() + deserialize_posting_leaflet() instead"
                .to_string(),
        ));
    }

    if version != SNAPSHOT_VERSION_V3 {
        return Err(SerializeError::InvalidFormat(format!(
            "Unsupported version: {version} (only v3 and v4 supported)",
        )));
    }

    // Read length prefix
    let len_bytes: [u8; 4] = data[5..9].try_into().unwrap();
    let len = u32::from_be_bytes(len_bytes) as usize;

    if data.len() < 9 + len {
        return Err(SerializeError::InvalidFormat("Data truncated".to_string()));
    }

    let payload = &data[9..9 + len];

    let snapshot: DeltaBm25Snapshot = postcard::from_bytes(payload)?;
    let posting_lists: Vec<PostingList> = snapshot
        .posting_lists
        .into_iter()
        .map(delta_to_posting_list)
        .collect::<Result<Vec<_>>>()?;
    let mut index = Bm25Index::from_parts(
        snapshot.terms,
        posting_lists,
        snapshot.doc_meta,
        snapshot.stats,
        snapshot.config,
        snapshot.watermark,
        snapshot.property_deps,
        snapshot.next_term_idx,
        snapshot.next_doc_id,
    );
    index.rebuild_lookups();
    Ok(index)
}

/// Write a BM25 index snapshot to a writer.
pub fn write_snapshot<W: Write>(index: &Bm25Index, mut writer: W) -> Result<()> {
    let data = serialize(index)?;
    writer.write_all(&data)?;
    Ok(())
}

/// Read a BM25 index snapshot from a reader.
pub fn read_snapshot<R: Read>(mut reader: R) -> Result<Bm25Index> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    deserialize(&data)
}

/// Compute a checksum of the index for verification.
///
/// Uses key index properties to generate a deterministic hash.
pub fn compute_checksum(index: &Bm25Index) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash key statistics
    index.stats.num_docs.hash(&mut hasher);
    index.stats.total_terms.hash(&mut hasher);
    index.config.k1.to_bits().hash(&mut hasher);
    index.config.b.to_bits().hash(&mut hasher);

    // Hash structural counts
    index.terms.len().hash(&mut hasher);

    // Live doc count
    let live_docs = index.doc_meta.iter().filter(|opt| opt.is_some()).count();
    live_docs.hash(&mut hasher);

    // Total live postings count
    let total_postings: usize = index
        .posting_lists
        .iter()
        .map(|pl| {
            pl.postings
                .iter()
                .filter(|p| {
                    index
                        .doc_meta
                        .get(p.doc_id as usize)
                        .and_then(|opt| opt.as_ref())
                        .is_some()
                })
                .count()
        })
        .sum();
    total_postings.hash(&mut hasher);

    hasher.finish()
}

// ============================================================================
// V4 Chunked Public API
// ============================================================================

/// Check if a snapshot blob uses the v4 chunked format.
pub fn is_chunked_format(data: &[u8]) -> bool {
    data.len() >= 5 && &data[0..4] == SNAPSHOT_MAGIC && data[4] == SNAPSHOT_VERSION_V4
}

/// Phase 1 of chunked serialization: compact the index, group posting lists
/// into leaflets, and serialize each leaflet blob.
///
/// Returns a `ChunkedPrepResult` containing compressed leaflet blobs (ready for
/// CAS write) and metadata. After writing leaflets to CAS, pass the result and
/// the CID bytes to `finalize_chunked_root()`.
pub fn prepare_chunked(index: &Bm25Index) -> Result<ChunkedPrepResult> {
    let mut compacted = index.clone();
    compacted.compact();

    let next_term_idx = compacted.terms.len() as u32;
    let next_doc_id = compacted.next_doc_id();

    let groups = group_posting_lists(&compacted.posting_lists, LEAFLET_TARGET_UNCOMPRESSED);

    let mut leaflet_blobs = Vec::with_capacity(groups.len());
    let mut leaflet_infos = Vec::with_capacity(groups.len());

    for (first_term_idx, lists) in &groups {
        let blob = serialize_leaflet_blob(lists, *first_term_idx)?;
        let posting_count: u32 = lists.iter().map(|pl| pl.postings.len() as u32).sum();
        let last_term_idx = if lists.is_empty() {
            *first_term_idx
        } else {
            first_term_idx + lists.len() as u32 - 1
        };

        leaflet_blobs.push(blob);
        leaflet_infos.push(LeafletInfo {
            first_term_idx: *first_term_idx,
            last_term_idx,
            list_count: lists.len() as u32,
            posting_count,
        });
    }

    Ok(ChunkedPrepResult {
        root: ChunkedBm25RootData {
            terms: compacted.terms,
            doc_meta: compacted.doc_meta,
            stats: compacted.stats,
            config: compacted.config,
            watermark: compacted.watermark,
            property_deps: compacted.property_deps,
            next_term_idx,
            next_doc_id,
        },
        leaflet_blobs,
        leaflet_infos,
    })
}

/// Phase 2 of chunked serialization: finalize the v4 root blob with CID bytes
/// obtained from writing leaflets to CAS.
///
/// `leaflet_cid_bytes` must have the same length as `prep.leaflet_blobs`, with
/// each entry being the raw CID bytes (`ContentId::to_bytes()`) for the
/// corresponding leaflet blob.
///
/// Returns the complete v4 root blob (13-byte header + zstd-compressed payload).
pub fn finalize_chunked_root(
    prep: ChunkedPrepResult,
    leaflet_cid_bytes: Vec<Vec<u8>>,
) -> Result<Vec<u8>> {
    if leaflet_cid_bytes.len() != prep.leaflet_infos.len() {
        return Err(SerializeError::InvalidFormat(format!(
            "CID count ({}) does not match leaflet count ({})",
            leaflet_cid_bytes.len(),
            prep.leaflet_infos.len()
        )));
    }

    let posting_leaflets: Vec<LeafletEntry> = prep
        .leaflet_infos
        .into_iter()
        .zip(leaflet_cid_bytes)
        .map(|(info, cid_bytes)| LeafletEntry {
            cid_bytes,
            first_term_idx: info.first_term_idx,
            last_term_idx: info.last_term_idx,
            list_count: info.list_count,
            posting_count: info.posting_count,
        })
        .collect();

    let root = ChunkedBm25Root {
        terms: prep.root.terms,
        doc_meta: prep.root.doc_meta,
        stats: prep.root.stats,
        config: prep.root.config,
        watermark: prep.root.watermark,
        property_deps: prep.root.property_deps,
        next_term_idx: prep.root.next_term_idx,
        next_doc_id: prep.root.next_doc_id,
        posting_leaflets,
    };

    let postcard_bytes = postcard::to_allocvec(&root)?;
    let uncompressed_len = postcard_bytes.len() as u32;
    let compressed = zstd::encode_all(postcard_bytes.as_slice(), 3)
        .map_err(|e| SerializeError::Io(std::io::Error::other(e)))?;
    let compressed_len = compressed.len() as u32;

    let mut data = Vec::with_capacity(13 + compressed.len());
    data.extend_from_slice(SNAPSHOT_MAGIC);
    data.push(SNAPSHOT_VERSION_V4);
    data.extend_from_slice(&uncompressed_len.to_be_bytes());
    data.extend_from_slice(&compressed_len.to_be_bytes());
    data.extend_from_slice(&compressed);

    Ok(data)
}

/// Deserialize a v4 chunked root blob.
///
/// Returns a `ChunkedRoot` with accessors for leaflet references and term
/// lookups. The caller fetches leaflet blobs from CAS, deserializes them
/// with `deserialize_posting_leaflet()`, and assembles the full index with
/// `assemble_from_chunked_root()`.
pub fn deserialize_chunked_root(data: &[u8]) -> Result<ChunkedRoot> {
    if data.len() < 13 {
        return Err(SerializeError::InvalidFormat(
            "Data too short for v4 header".to_string(),
        ));
    }

    if &data[0..4] != SNAPSHOT_MAGIC {
        return Err(SerializeError::InvalidFormat(
            "Invalid magic bytes".to_string(),
        ));
    }

    if data[4] != SNAPSHOT_VERSION_V4 {
        return Err(SerializeError::InvalidFormat(format!(
            "Expected version {}, got {}",
            SNAPSHOT_VERSION_V4, data[4]
        )));
    }

    let uncompressed_len = u32::from_be_bytes(data[5..9].try_into().unwrap()) as usize;
    let compressed_len = u32::from_be_bytes(data[9..13].try_into().unwrap()) as usize;

    if data.len() < 13 + compressed_len {
        return Err(SerializeError::InvalidFormat(
            "Data truncated (compressed payload shorter than header declares)".to_string(),
        ));
    }

    if uncompressed_len > MAX_ROOT_DECOMPRESSED {
        return Err(SerializeError::InvalidFormat(format!(
            "Declared uncompressed size {uncompressed_len} exceeds maximum allowed {MAX_ROOT_DECOMPRESSED} for root blob"
        )));
    }

    let compressed = &data[13..13 + compressed_len];
    let decompressed = zstd::bulk::decompress(compressed, uncompressed_len)
        .map_err(|e| SerializeError::Io(std::io::Error::other(e)))?;

    if decompressed.len() != uncompressed_len {
        return Err(SerializeError::InvalidFormat(format!(
            "Decompressed size {} does not match header ({})",
            decompressed.len(),
            uncompressed_len
        )));
    }

    let root: ChunkedBm25Root = postcard::from_bytes(&decompressed)?;
    Ok(ChunkedRoot { root })
}

/// Deserialize a posting leaflet blob.
///
/// Returns `(first_term_idx, posting_lists)` where posting lists have been
/// expanded from delta encoding back to absolute doc_ids. Validates that
/// the embedded `list_count` matches the actual number of posting lists.
pub fn deserialize_posting_leaflet(data: &[u8]) -> Result<(u32, Vec<PostingList>)> {
    let decompressed = zstd::bulk::decompress(data, MAX_LEAFLET_DECOMPRESSED).map_err(|e| {
        SerializeError::Io(std::io::Error::other(format!(
            "Leaflet decompression failed (max {MAX_LEAFLET_DECOMPRESSED} bytes): {e}"
        )))
    })?;

    let payload: PostingLeafletPayload = postcard::from_bytes(&decompressed)?;

    if payload.list_count as usize != payload.posting_lists.len() {
        return Err(SerializeError::InvalidFormat(format!(
            "Leaflet list_count ({}) does not match posting_lists.len() ({})",
            payload.list_count,
            payload.posting_lists.len()
        )));
    }

    let posting_lists: Vec<PostingList> = payload
        .posting_lists
        .into_iter()
        .map(delta_to_posting_list)
        .collect::<Result<Vec<_>>>()?;

    Ok((payload.first_term_idx, posting_lists))
}

/// Assemble a `Bm25Index` from a deserialized chunked root and posting lists.
///
/// `posting_lists` must be pre-sized to `root.next_term_idx()` entries. For
/// selective loading, unloaded term indices should contain empty `PostingList`
/// defaults — the scorer only accesses posting lists for query terms.
pub fn assemble_from_chunked_root(root: ChunkedRoot, posting_lists: Vec<PostingList>) -> Bm25Index {
    let r = root.root;
    let mut index = Bm25Index::from_parts(
        r.terms,
        posting_lists,
        r.doc_meta,
        r.stats,
        r.config,
        r.watermark,
        r.property_deps,
        r.next_term_idx,
        r.next_doc_id,
    );
    index.rebuild_lookups();
    index
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bm25::index::{Bm25Config, DocKey};
    use std::collections::HashMap;

    fn build_test_index() -> Bm25Index {
        let mut index = Bm25Index::with_config(Bm25Config::new(1.5, 0.8));

        // Add some documents
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 2);
        tf1.insert("world", 1);
        index.add_document(doc1, tf1);

        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 1);
        tf2.insert("rust", 3);
        index.add_document(doc2, tf2);

        // Set watermarks
        index.watermark.update("test:main", 42);

        index
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = build_test_index();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        // Verify basic properties
        assert_eq!(restored.num_docs(), original.num_docs());
        assert_eq!(restored.num_terms(), original.num_terms());
        assert_eq!(restored.stats.total_terms, original.stats.total_terms);
        assert_eq!(restored.config.k1, original.config.k1);
        assert_eq!(restored.config.b, original.config.b);

        // Verify watermarks
        assert_eq!(
            restored.watermark.get("test:main"),
            original.watermark.get("test:main")
        );

        // Verify documents exist
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        assert!(restored.contains_doc(&doc1));
        assert!(restored.contains_doc(&doc2));

        // Verify document metadata
        let meta1 = restored.get_doc_meta(&doc1).unwrap();
        assert_eq!(meta1.doc_len, 3); // "hello" x2 + "world" x1
    }

    #[test]
    fn test_serialize_empty_index() {
        let original = Bm25Index::new();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        assert_eq!(restored.num_docs(), 0);
        assert_eq!(restored.num_terms(), 0);
    }

    #[test]
    fn test_invalid_magic_bytes() {
        let data = b"XXXXsome data here";
        let result = deserialize(data);
        assert!(matches!(result, Err(SerializeError::InvalidFormat(_))));
    }

    #[test]
    fn test_invalid_version() {
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(99); // Invalid version
        data.extend_from_slice(&[0, 0, 0, 0]); // Zero length

        let result = deserialize(&data);
        match &result {
            Err(SerializeError::InvalidFormat(msg)) => {
                assert!(
                    msg.contains("v3") && msg.contains("v4"),
                    "Error should list supported versions: {msg}"
                );
            }
            other => panic!("Expected InvalidFormat, got: {other:?}"),
        }
    }

    #[test]
    fn test_truncated_data() {
        let original = build_test_index();
        let data = serialize(&original).expect("serialize failed");

        // Truncate the data
        let truncated = &data[0..data.len() / 2];
        let result = deserialize(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_read_snapshot() {
        let original = build_test_index();

        let mut buffer = Vec::new();
        write_snapshot(&original, &mut buffer).expect("write failed");

        let cursor = std::io::Cursor::new(buffer);
        let restored = read_snapshot(cursor).expect("read failed");

        assert_eq!(restored.num_docs(), original.num_docs());
    }

    #[test]
    fn test_compute_checksum() {
        let index1 = build_test_index();
        let index2 = build_test_index();

        // Same index should have same checksum
        let checksum1 = compute_checksum(&index1);
        let checksum2 = compute_checksum(&index2);
        assert_eq!(checksum1, checksum2);

        // Different index should have different checksum
        let mut index3 = build_test_index();
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let mut tf3 = HashMap::new();
        tf3.insert("different", 1);
        index3.add_document(doc3, tf3);

        let checksum3 = compute_checksum(&index3);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_serialize_produces_deterministic_output() {
        // Two indexes built in different order should serialize identically
        let mut index_a = Bm25Index::new();
        let mut index_b = Bm25Index::new();

        let doc1 = DocKey::new("test:main", "http://example.org/aaa");
        let doc2 = DocKey::new("test:main", "http://example.org/bbb");

        let mut tf1 = HashMap::new();
        tf1.insert("alpha", 1);
        tf1.insert("beta", 2);

        let mut tf2 = HashMap::new();
        tf2.insert("beta", 1);
        tf2.insert("gamma", 3);

        // Build in different order
        index_a.add_document(doc1.clone(), tf1.clone());
        index_a.add_document(doc2.clone(), tf2.clone());

        index_b.add_document(doc2, tf2);
        index_b.add_document(doc1, tf1);

        let bytes_a = serialize(&index_a).expect("serialize a failed");
        let bytes_b = serialize(&index_b).expect("serialize b failed");

        assert_eq!(
            bytes_a, bytes_b,
            "Serialized bytes should be identical regardless of insertion order"
        );
    }

    #[test]
    fn test_v2_rejected() {
        // Old v2 format should be rejected
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(2); // v2
        data.extend_from_slice(&[0, 0, 0, 0]); // dummy length
        let result = deserialize(&data);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Unsupported version"), "got: {msg}");
    }

    #[test]
    fn test_v3_roundtrip_scoring_equivalence() {
        use crate::bm25::scoring::Bm25Scorer;

        let original = build_test_index();

        let data = serialize(&original).expect("serialize failed");
        // Verify it's v3
        assert_eq!(data[4], SNAPSHOT_VERSION_V3);

        let restored = deserialize(&data).expect("deserialize failed");

        // Score a query on both and compare
        let query_terms = ["hello", "world"];
        let scorer_orig = Bm25Scorer::new(&original, &query_terms.map(|s| s));
        let scorer_rest = Bm25Scorer::new(&restored, &query_terms.map(|s| s));

        let results_orig = scorer_orig.score_all();
        let results_rest = scorer_rest.score_all();

        assert_eq!(
            results_orig.len(),
            results_rest.len(),
            "Different number of scored docs"
        );
        for ((dk_o, s_o), (dk_r, s_r)) in results_orig.iter().zip(results_rest.iter()) {
            assert_eq!(dk_o, dk_r, "DocKey mismatch");
            assert!(
                (s_o - s_r).abs() < 1e-10,
                "Score mismatch for {dk_o:?}: {s_o} vs {s_r}"
            );
        }

        // Checksums should match
        assert_eq!(compute_checksum(&original), compute_checksum(&restored));
    }

    #[test]
    fn test_delta_encoding_roundtrip_large() {
        // Build a large-ish index where one term appears in most docs
        // so that after compact(), doc_ids are 0..N with deltas of ~1
        let mut index = Bm25Index::new();

        for i in 0..200 {
            let iri = format!("http://example.org/doc{i}");
            let doc = DocKey::new("test:main", iri.as_str());
            let mut tf = HashMap::new();
            tf.insert("common", 1); // appears in all 200 docs
            if i % 10 == 0 {
                tf.insert("rare", 1); // appears in 20 docs
            }
            index.add_document(doc, tf);
        }

        // Serialize as v3 (the default) and verify roundtrip
        let v3_bytes = serialize(&index).expect("v3 serialize failed");
        let restored = deserialize(&v3_bytes).expect("v3 deserialize failed");
        assert_eq!(restored.num_docs(), 200);
        assert_eq!(restored.num_terms(), 2);
    }

    #[test]
    fn test_delta_length_mismatch_rejected() {
        // Manually construct a v3 blob with mismatched delta/freq lengths
        let bad_snapshot = DeltaBm25Snapshot {
            terms: BTreeMap::new(),
            posting_lists: vec![DeltaPostingList {
                doc_id_deltas: vec![0, 1, 2],
                term_freqs: vec![1, 2], // one fewer than deltas
            }],
            doc_meta: vec![],
            stats: Bm25Stats {
                num_docs: 0,
                total_terms: 0,
            },
            config: Bm25Config::default(),
            watermark: GraphSourceWatermark::new(),
            property_deps: PropertyDeps::new(),
            next_term_idx: 0,
            next_doc_id: 0,
        };

        let payload = postcard::to_allocvec(&bad_snapshot).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(SNAPSHOT_VERSION_V3);
        let len = payload.len() as u32;
        data.extend_from_slice(&len.to_be_bytes());
        data.extend_from_slice(&payload);

        let result = deserialize(&data);
        match &result {
            Err(SerializeError::InvalidFormat(msg)) => {
                assert!(
                    msg.contains("length mismatch"),
                    "Expected length mismatch error, got: {msg}"
                );
            }
            other => panic!("Expected InvalidFormat, got: {other:?}"),
        }
    }

    #[test]
    fn test_v1_rejected() {
        // Old v1 format should be rejected
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(1); // v1
        data.extend_from_slice(&[0, 0, 0, 0]); // dummy length
        let result = deserialize(&data);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Unsupported version"), "got: {msg}");
    }

    // ====================================================================
    // V4 chunked format tests
    // ====================================================================

    #[test]
    fn test_is_chunked_format() {
        // v3 blob → false
        let index = build_test_index();
        let v3_bytes = serialize(&index).expect("serialize");
        assert!(!is_chunked_format(&v3_bytes));

        // v4 blob → true
        let prep = prepare_chunked(&index).expect("prepare_chunked");
        let fake_cids: Vec<Vec<u8>> = prep
            .leaflet_infos
            .iter()
            .enumerate()
            .map(|(i, _)| vec![0xCA, 0xFE, i as u8])
            .collect();
        let root_bytes = finalize_chunked_root(prep, fake_cids).expect("finalize");
        assert!(is_chunked_format(&root_bytes));

        // Garbage → false
        assert!(!is_chunked_format(b"XXXX"));
        assert!(!is_chunked_format(b""));
        assert!(!is_chunked_format(b"BM25"));
    }

    #[test]
    fn test_v4_chunked_roundtrip_scoring_equivalence() {
        use crate::bm25::scoring::Bm25Scorer;

        let original = build_test_index();

        // Phase 1: prepare chunked
        let prep = prepare_chunked(&original).expect("prepare_chunked");
        assert!(
            !prep.leaflet_blobs.is_empty(),
            "Should have at least one leaflet"
        );
        assert_eq!(prep.leaflet_blobs.len(), prep.leaflet_infos.len());

        // Simulate CAS writes: use leaflet index as fake CID bytes
        let fake_cids: Vec<Vec<u8>> = (0..prep.leaflet_blobs.len())
            .map(|i| vec![0xDE, 0xAD, i as u8])
            .collect();

        // Stash leaflet blobs before consuming prep
        let leaflet_blobs: Vec<Vec<u8>> = prep.leaflet_blobs.clone();
        let leaflet_infos: Vec<LeafletInfo> = prep.leaflet_infos.clone();

        // Phase 2: finalize root
        let root_bytes = finalize_chunked_root(prep, fake_cids).expect("finalize");
        assert!(is_chunked_format(&root_bytes));

        // Deserialize root
        let chunked_root = deserialize_chunked_root(&root_bytes).expect("deserialize_root");
        assert_eq!(chunked_root.num_docs(), original.num_docs());
        assert_eq!(chunked_root.num_terms(), original.num_terms());

        let leaflet_refs = chunked_root.leaflet_refs();
        assert_eq!(leaflet_refs.len(), leaflet_blobs.len());

        // Deserialize all leaflets and assemble
        let mut posting_lists = vec![PostingList::default(); chunked_root.next_term_idx() as usize];
        for (blob, info) in leaflet_blobs.iter().zip(leaflet_infos.iter()) {
            let (first_idx, lists) =
                deserialize_posting_leaflet(blob).expect("deserialize_leaflet");
            assert_eq!(first_idx, info.first_term_idx);
            assert_eq!(lists.len(), info.list_count as usize);
            for (j, pl) in lists.into_iter().enumerate() {
                posting_lists[first_idx as usize + j] = pl;
            }
        }

        let restored = assemble_from_chunked_root(chunked_root, posting_lists);

        // Verify scoring equivalence
        let query_terms = ["hello", "world"];
        let scorer_orig = Bm25Scorer::new(&original, &query_terms.map(|s| s));
        let scorer_rest = Bm25Scorer::new(&restored, &query_terms.map(|s| s));

        let results_orig = scorer_orig.score_all();
        let results_rest = scorer_rest.score_all();

        assert_eq!(
            results_orig.len(),
            results_rest.len(),
            "Different number of scored docs"
        );
        for ((dk_o, s_o), (dk_r, s_r)) in results_orig.iter().zip(results_rest.iter()) {
            assert_eq!(dk_o, dk_r, "DocKey mismatch");
            assert!(
                (s_o - s_r).abs() < 1e-10,
                "Score mismatch for {dk_o:?}: {s_o} vs {s_r}"
            );
        }

        // Checksums should match
        assert_eq!(compute_checksum(&original), compute_checksum(&restored));
    }

    #[test]
    fn test_v4_empty_index_roundtrip() {
        let empty = Bm25Index::new();
        let prep = prepare_chunked(&empty).expect("prepare_chunked empty");

        // Empty index produces no leaflets
        assert!(prep.leaflet_blobs.is_empty());
        assert!(prep.leaflet_infos.is_empty());

        let root_bytes = finalize_chunked_root(prep, vec![]).expect("finalize");
        assert!(is_chunked_format(&root_bytes));

        let chunked_root = deserialize_chunked_root(&root_bytes).expect("deserialize_root");
        assert_eq!(chunked_root.num_docs(), 0);
        assert_eq!(chunked_root.num_terms(), 0);
        assert!(chunked_root.leaflet_refs().is_empty());

        let restored = assemble_from_chunked_root(chunked_root, vec![]);
        assert_eq!(restored.num_docs(), 0);
        assert_eq!(restored.num_terms(), 0);
    }

    #[test]
    fn test_v4_deserialize_rejects_chunked() {
        let index = build_test_index();
        let prep = prepare_chunked(&index).expect("prepare_chunked");
        let fake_cids: Vec<Vec<u8>> = (0..prep.leaflet_blobs.len())
            .map(|i| vec![i as u8])
            .collect();
        let root_bytes = finalize_chunked_root(prep, fake_cids).expect("finalize");

        // deserialize() should reject v4
        let result = deserialize(&root_bytes);
        match &result {
            Err(SerializeError::InvalidFormat(msg)) => {
                assert!(
                    msg.contains("V4") && msg.contains("deserialize_chunked_root"),
                    "Should direct caller to chunked API: {msg}"
                );
            }
            other => panic!("Expected InvalidFormat for v4, got: {other:?}"),
        }
    }

    #[test]
    fn test_v4_leaflet_list_count_validation() {
        // Build a valid leaflet blob, then tamper with list_count
        let payload = PostingLeafletPayload {
            first_term_idx: 0,
            list_count: 99, // Wrong — actual list has 1 entry
            posting_lists: vec![DeltaPostingList {
                doc_id_deltas: vec![0],
                term_freqs: vec![1],
            }],
        };

        let postcard_bytes = postcard::to_allocvec(&payload).unwrap();
        let compressed = zstd::encode_all(postcard_bytes.as_slice(), 3).unwrap();

        let result = deserialize_posting_leaflet(&compressed);
        match &result {
            Err(SerializeError::InvalidFormat(msg)) => {
                assert!(
                    msg.contains("list_count"),
                    "Should report list_count mismatch: {msg}"
                );
            }
            other => panic!("Expected InvalidFormat for bad list_count, got: {other:?}"),
        }
    }

    #[test]
    fn test_v4_cid_count_mismatch_rejected() {
        let index = build_test_index();
        let prep = prepare_chunked(&index).expect("prepare_chunked");

        // Pass wrong number of CIDs
        let result = finalize_chunked_root(prep, vec![]);
        assert!(matches!(result, Err(SerializeError::InvalidFormat(_))));
    }

    #[test]
    fn test_v4_grouping_basic() {
        // Create posting lists with known sizes
        let lists: Vec<PostingList> = (0..10)
            .map(|i| PostingList {
                postings: (0..100)
                    .map(|d| Posting {
                        doc_id: d,
                        term_freq: i + 1,
                    })
                    .collect(),
                block_meta: Vec::new(),
            })
            .collect();

        // With a small target, should create multiple groups
        // Each list is 100 postings * 8 bytes = 800 bytes
        // Target of 2000 bytes → ~2 lists per group
        let groups = group_posting_lists(&lists, 2000);
        assert!(groups.len() > 1, "Should create multiple groups");

        // Verify all lists are covered
        let total_lists: usize = groups.iter().map(|(_, slice)| slice.len()).sum();
        assert_eq!(total_lists, 10);

        // Verify consecutive term_idx ranges
        let mut expected_start = 0u32;
        for (first_idx, slice) in &groups {
            assert_eq!(*first_idx, expected_start);
            expected_start += slice.len() as u32;
        }
    }

    #[test]
    fn test_v4_grouping_empty() {
        let groups = group_posting_lists(&[], 1000);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_v4_grouping_single_large_list() {
        // One posting list that exceeds the target — gets its own group
        let lists = vec![PostingList {
            postings: (0..10000)
                .map(|d| Posting {
                    doc_id: d,
                    term_freq: 1,
                })
                .collect(),
            block_meta: Vec::new(),
        }];

        let groups = group_posting_lists(&lists, 1000);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, 0);
        assert_eq!(groups[0].1.len(), 1);
    }

    #[test]
    fn test_v4_selective_leaflet_refs() {
        // Build an index with multiple terms, use small target to force multiple leaflets
        let mut index = Bm25Index::new();
        for i in 0..50 {
            let iri = format!("http://example.org/doc{i}");
            let doc = DocKey::new("test:main", iri.as_str());
            let mut tf = HashMap::new();
            let term = format!("term{}", i % 10);
            tf.insert(term.as_str(), 1);
            tf.insert("common", 1);
            index.add_document(doc, tf);
        }

        let prep = prepare_chunked(&index).expect("prepare_chunked");
        let fake_cids: Vec<Vec<u8>> = (0..prep.leaflet_blobs.len())
            .map(|i| vec![i as u8])
            .collect();
        let root_bytes = finalize_chunked_root(prep, fake_cids).expect("finalize");
        let root = deserialize_chunked_root(&root_bytes).expect("deserialize_root");

        // Look up a term and find which leaflet(s) contain it
        if let Some(entry) = root.get_term("common") {
            let refs = root.leaflet_refs_for_terms(&[entry.idx]);
            assert!(
                !refs.is_empty(),
                "Should find at least one leaflet for 'common'"
            );
            // The term_idx should be within the leaflet's range
            for r in &refs {
                assert!(entry.idx >= r.first_term_idx && entry.idx <= r.last_term_idx);
            }
        }

        // Non-existent term → no leaflets
        let refs = root.leaflet_refs_for_terms(&[9999]);
        assert!(refs.is_empty());
    }
}
