//! Per-predicate vector arena storing packed f32 arrays.
//!
//! ## Precision contract
//!
//! The `f:vector` / `@vector` datatype is defined as **f32 storage**.
//! Values are quantized to IEEE-754 binary32 at ingest (in the coercion
//! layer) and stored in contiguous packed f32 arrays here. Non-finite
//! values (NaN, ±Inf) and values outside f32 range are rejected at ingest.
//!
//! Users requiring higher-precision or non-float vectors (e.g. f64, integer,
//! sparse) should use a custom RDF datatype, which Fluree stores as a
//! string literal.
//!
//! Each vector gets a sequential `u32` handle stored in `ObjKey` with
//! `ObjKind::VECTOR_ID`.
//!
//! ## On-disk formats
//!
//! **VAS1** (Vector Arena Shard): binary, CAS-addressed, 16-byte-aligned header.
//! ```text
//! Magic: "VAS1"       (4 bytes)
//! version: u8          (= 1)
//! dims: u16 LE
//! count: u32 LE
//! _pad: [u8; 5]        (zero, aligns header to 16 bytes)
//! data: [f32 LE; count * dims]
//! ```
//!
//! **VAM1** (Vector Arena Manifest): JSON, CAS-addressed.
//! ```json
//! {
//!     "version": 1,
//!     "dims": 768,
//!     "dtype": "f32",
//!     "normalized": true,
//!     "shard_capacity": 3072,
//!     "total_count": 50000,
//!     "shards": [
//!         { "cas": "fluree:file://...", "count": 3072 },
//!         { "cas": "fluree:file://...", "count": 784 }
//!     ]
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Maximum vectors per shard. At 768-dim f32 each shard ≈ 9 MB.
pub const SHARD_CAPACITY: u32 = 3072;

/// Epsilon for unit-norm check: |1.0 - ‖v‖²| < ε.
const UNIT_NORM_EPSILON: f32 = 1e-5;

/// VAS1 header magic bytes.
const VAS_MAGIC: [u8; 4] = *b"VAS1";

/// VAS1 header size (16-byte aligned).
const VAS_HEADER_SIZE: usize = 16;

// ============================================================================
// In-memory VectorArena
// ============================================================================

/// Per-predicate vector arena storing packed f32 arrays.
/// Vectors are appended sequentially; each gets a u32 handle.
#[derive(Debug)]
pub struct VectorArena {
    /// Fixed dimensionality per property; 0 until first insert.
    dims: u16,
    /// Packed f32 values: values[handle * dims .. (handle+1) * dims].
    values: Vec<f32>,
    /// Number of vectors stored.
    count: u32,
    /// Whether all inserted vectors have unit norm (±epsilon).
    all_unit_norm: bool,
}

impl VectorArena {
    pub fn new() -> Self {
        Self {
            dims: 0,
            values: Vec::new(),
            count: 0,
            all_unit_norm: true,
        }
    }

    /// Insert an f32 vector, returning its handle.
    ///
    /// Non-finite values (NaN, ±Inf) are rejected. The coercion layer
    /// should have already validated this, but we check here as a safety net.
    pub fn insert_f32(&mut self, vec: &[f32]) -> Result<u32, String> {
        if vec.is_empty() {
            return Err("vector must not be empty".into());
        }
        // Reject non-finite elements
        if let Some(pos) = vec.iter().position(|v| !v.is_finite()) {
            return Err(format!(
                "vector element [{}] is not finite: {}",
                pos, vec[pos]
            ));
        }
        if self.dims == 0 {
            if vec.len() > u16::MAX as usize {
                return Err(format!("vector dims {} exceeds u16::MAX", vec.len()));
            }
            self.dims = vec.len() as u16;
        } else if vec.len() != self.dims as usize {
            return Err(format!(
                "vector dims mismatch: expected {}, got {}",
                self.dims,
                vec.len()
            ));
        }

        // Check unit norm
        if self.all_unit_norm {
            let mag2: f32 = vec.iter().map(|&x| x * x).sum();
            if (1.0 - mag2).abs() > UNIT_NORM_EPSILON {
                self.all_unit_norm = false;
            }
        }

        let handle = self.count;
        self.values.extend_from_slice(vec);
        self.count += 1;
        Ok(handle)
    }

    /// Insert an f64 vector by downcasting to f32, returning its handle.
    pub fn insert_f64(&mut self, vec: &[f64]) -> Result<u32, String> {
        let f32_vec: Vec<f32> = vec.iter().map(|&x| x as f32).collect();
        self.insert_f32(&f32_vec)
    }

    /// Borrow an f32 slice for a given handle (zero-copy hot path).
    pub fn get_f32(&self, handle: u32) -> Option<&[f32]> {
        if handle >= self.count || self.dims == 0 {
            return None;
        }
        let d = self.dims as usize;
        let start = handle as usize * d;
        let end = start + d;
        Some(&self.values[start..end])
    }

    /// Get a vector as f64 (upcast from f32). Used for FlakeValue::Vector.
    pub fn get_f64(&self, handle: u32) -> Option<Vec<f64>> {
        self.get_f32(handle)
            .map(|s| s.iter().map(|&x| x as f64).collect())
    }

    /// Dimensionality (0 if no vectors inserted yet).
    pub fn dims(&self) -> u16 {
        self.dims
    }

    /// Number of vectors stored.
    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether all stored vectors have unit norm.
    pub fn is_normalized(&self) -> bool {
        self.all_unit_norm && self.count > 0
    }

    /// Raw packed values (for persistence).
    pub fn raw_values(&self) -> &[f32] {
        &self.values
    }
}

impl Default for VectorArena {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VectorShard — parsed VAS1 data (one shard's worth of vectors)
// ============================================================================

/// Parsed VAS1 shard data.
pub struct VectorShard {
    pub dims: u16,
    pub count: u32,
    pub values: Vec<f32>,
}

impl VectorShard {
    /// Get an f32 slice for an offset within this shard.
    pub fn get_f32(&self, offset: u32) -> Option<&[f32]> {
        if offset >= self.count || self.dims == 0 {
            return None;
        }
        let d = self.dims as usize;
        let start = offset as usize * d;
        let end = start + d;
        Some(&self.values[start..end])
    }
}

// ============================================================================
// VectorManifest (VAM1) — JSON metadata
// ============================================================================

/// JSON manifest for a per-predicate vector arena.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorManifest {
    pub version: u32,
    pub dims: u16,
    pub dtype: String,
    pub normalized: bool,
    pub shard_capacity: u32,
    pub total_count: u32,
    pub shards: Vec<ShardInfo>,
}

/// One shard entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub cas: String,
    pub count: u32,
}

// ============================================================================
// VAS1 binary persistence
// ============================================================================

/// Write a single VAS1 shard from a slice of the arena.
fn write_vas1_shard(writer: &mut impl io::Write, dims: u16, data: &[f32]) -> io::Result<()> {
    let count = data.len() / dims as usize;

    // 16-byte header
    writer.write_all(&VAS_MAGIC)?;
    writer.write_all(&[1u8])?; // version
    writer.write_all(&dims.to_le_bytes())?;
    writer.write_all(&(count as u32).to_le_bytes())?;
    writer.write_all(&[0u8; 5])?; // padding to 16 bytes

    // Data: packed f32 LE
    // f32 is already LE on little-endian targets; for portability, write each value.
    for &val in data {
        writer.write_all(&val.to_le_bytes())?;
    }

    Ok(())
}

/// Serialize a single VAS1 shard to a byte buffer.
///
/// `data` is a flat slice of `count * dims` f32 values.
pub fn write_vector_shard_to_bytes(dims: u16, data: &[f32]) -> io::Result<Vec<u8>> {
    if dims == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "vector shard dims must be > 0",
        ));
    }
    if !data.len().is_multiple_of(dims as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "vector shard data length {} not divisible by dims {}",
                data.len(),
                dims
            ),
        ));
    }
    let mut buf = Vec::new();
    write_vas1_shard(&mut buf, dims, data)?;
    Ok(buf)
}

/// Serialize raw packed vectors into one or more VAS1 shards.
///
/// `raw_values` is a flat slice of `total_count * dims` f32 values.
/// Returns `(shard_bytes, shard_info)` pairs in shard order.
pub fn write_vector_shards_from_raw(
    dims: u16,
    raw_values: &[f32],
) -> io::Result<Vec<(Vec<u8>, ShardInfo)>> {
    if dims == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "vector dims must be > 0",
        ));
    }
    if raw_values.is_empty() {
        return Ok(Vec::new());
    }
    if !raw_values.len().is_multiple_of(dims as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "raw vector data length {} not divisible by dims {}",
                raw_values.len(),
                dims
            ),
        ));
    }

    let dims_usize = dims as usize;
    let cap = SHARD_CAPACITY as usize;
    let total = raw_values.len() / dims_usize;
    let num_shards = total.div_ceil(cap);
    let mut result = Vec::with_capacity(num_shards);

    for shard_idx in 0..num_shards {
        let start_vec = shard_idx * cap;
        let end_vec = (start_vec + cap).min(total);
        let count = (end_vec - start_vec) as u32;
        let start_f32 = start_vec * dims_usize;
        let end_f32 = end_vec * dims_usize;
        let shard_data = &raw_values[start_f32..end_f32];
        let buf = write_vector_shard_to_bytes(dims, shard_data)?;
        result.push((
            buf,
            ShardInfo {
                cas: String::new(),
                count,
            },
        ));
    }

    Ok(result)
}

/// Serialize a VectorArena to one or more VAS1 shard byte buffers.
///
/// Returns `(shard_bytes_vec, shard_infos)` where each element corresponds
/// to one shard. The `ShardInfo.cas` field is left empty (caller fills it
/// after CAS upload).
pub fn write_vector_shards_to_bytes(arena: &VectorArena) -> io::Result<Vec<(Vec<u8>, ShardInfo)>> {
    if arena.is_empty() {
        return Ok(Vec::new());
    }
    write_vector_shards_from_raw(arena.dims(), arena.raw_values())
}

/// Write vector arena shards to disk. Returns paths of created shard files.
pub fn write_vector_shards(
    dir: &Path,
    p_id: u32,
    arena: &VectorArena,
) -> io::Result<Vec<std::path::PathBuf>> {
    if arena.is_empty() {
        return Ok(Vec::new());
    }

    let dims = arena.dims() as usize;
    let cap = SHARD_CAPACITY as usize;
    let total = arena.len() as usize;
    let num_shards = total.div_ceil(cap);
    let mut paths = Vec::with_capacity(num_shards);

    for shard_idx in 0..num_shards {
        let start_vec = shard_idx * cap;
        let end_vec = (start_vec + cap).min(total);
        let start_f32 = start_vec * dims;
        let end_f32 = end_vec * dims;
        let shard_data = &arena.raw_values()[start_f32..end_f32];

        let path = dir.join(format!("p_{p_id}_s_{shard_idx}.vas"));
        let file = std::fs::File::create(&path)?;
        let mut writer = io::BufWriter::new(file);
        write_vas1_shard(&mut writer, arena.dims(), shard_data)?;
        use io::Write;
        writer.flush()?;
        paths.push(path);
    }

    Ok(paths)
}

/// Write a vector arena manifest (VAM1 JSON).
pub fn write_vector_manifest(
    path: &Path,
    arena: &VectorArena,
    shard_addrs: &[ShardInfo],
) -> io::Result<()> {
    let manifest = VectorManifest {
        version: 1,
        dims: arena.dims(),
        dtype: "f32".to_string(),
        normalized: arena.is_normalized(),
        shard_capacity: SHARD_CAPACITY,
        total_count: arena.len(),
        shards: shard_addrs.to_vec(),
    };
    let json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Parse a VAS1 shard from bytes.
pub fn read_vector_shard_from_bytes(data: &[u8]) -> io::Result<VectorShard> {
    if data.len() < VAS_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "vector shard too small for header",
        ));
    }
    if data[0..4] != VAS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vector shard: invalid magic {:?}", &data[0..4]),
        ));
    }
    let version = data[4];
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vector shard: unsupported version {version}"),
        ));
    }
    let dims = u16::from_le_bytes(data[5..7].try_into().unwrap());
    let count = u32::from_le_bytes(data[7..11].try_into().unwrap());

    let expected_data_size = count as usize * dims as usize * 4;
    let actual_data_size = data.len() - VAS_HEADER_SIZE;
    if actual_data_size < expected_data_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "vector shard: data truncated (expected {expected_data_size} bytes, got {actual_data_size})"
            ),
        ));
    }

    let mut values = Vec::with_capacity(count as usize * dims as usize);
    let data_start = VAS_HEADER_SIZE;
    for i in 0..(count as usize * dims as usize) {
        let offset = data_start + i * 4;
        let val = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        values.push(val);
    }

    Ok(VectorShard {
        dims,
        count,
        values,
    })
}

/// Parse a VAS1 shard from a file.
pub fn read_vector_shard(path: &Path) -> io::Result<VectorShard> {
    read_vector_shard_from_bytes(&std::fs::read(path)?)
}

/// Parse a VAM1 manifest from bytes.
pub fn read_vector_manifest(data: &[u8]) -> io::Result<VectorManifest> {
    serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Reassemble a VectorArena from a manifest and shard data.
pub fn load_arena_from_shards(
    manifest: &VectorManifest,
    shard_data: Vec<VectorShard>,
) -> io::Result<VectorArena> {
    let mut arena = VectorArena::new();
    if manifest.total_count == 0 {
        return Ok(arena);
    }

    // Pre-allocate
    let total_floats = manifest.total_count as usize * manifest.dims as usize;
    arena.dims = manifest.dims;
    arena.values.reserve(total_floats);
    arena.all_unit_norm = manifest.normalized;

    for shard in &shard_data {
        if shard.dims != manifest.dims {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shard dims {} != manifest dims {}",
                    shard.dims, manifest.dims
                ),
            ));
        }
        arena.values.extend_from_slice(&shard.values);
        arena.count += shard.count;
    }

    if arena.count != manifest.total_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "total count mismatch: shards have {}, manifest says {}",
                arena.count, manifest.total_count
            ),
        ));
    }

    Ok(arena)
}

// ============================================================================
// LazyVectorArena — on-demand shard loading backed by LeafletCache
// ============================================================================

/// Per-shard resolution metadata (addressing only, no data loaded).
pub struct ShardSource {
    /// xxh3_128 of shard CID bytes — LeafletCache key.
    pub(crate) cid_hash: u128,
    /// Content ID for remote fetching. `None` for FileStorage (local path suffices).
    pub(crate) cid: Option<fluree_db_core::ContentId>,
    /// Local file path to shard data.
    /// FileStorage: direct CAS path. Remote: disk-cache path.
    pub(crate) path: PathBuf,
    /// Whether the shard file is known to exist on disk.
    /// AtomicBool for safe mutation through `Arc<BinaryIndexStore>`.
    pub(crate) on_disk: AtomicBool,
}

/// Per-predicate lazy vector shard reader (read-only, sync access).
///
/// Shards are loaded on demand through the shared [`crate::read::leaflet_cache::LeafletCache`],
/// competing for the same TinyLFU memory pool as all other cached artifacts.
/// Two access modes are provided:
/// - **Cached** (point lookups): shards enter the shared cache.
/// - **Transient** (streaming scans): shards bypass the cache to avoid evicting
///   R1/dict/BM25 entries.
///
/// For remote backends (S3), shards that are not yet on local disk are fetched
/// on demand using the same sync→async bridge as index leaflets and dict leaves
/// (thread + `tokio::Handle::block_on`). This makes vector loading truly lazy:
/// only the shards actually decoded by a query are ever downloaded.
pub struct LazyVectorArena {
    manifest: VectorManifest,
    shard_sources: Vec<ShardSource>,
    cache: Arc<crate::read::leaflet_cache::LeafletCache>,
    /// Content store for on-demand remote shard fetching.
    /// `None` for FileStorage (all shards are always on disk).
    cas: Option<Arc<dyn fluree_db_core::ContentStore>>,
}

/// A vector slice backed by a cache-managed shard.
///
/// Holds an `Arc<VectorShard>` to keep the shard alive while the caller
/// reads the slice. Dropping the `VectorSlice` releases the Arc reference
/// (the shard stays in cache if still referenced there).
pub struct VectorSlice {
    shard: Arc<VectorShard>,
    offset: u32,
}

impl std::fmt::Debug for VectorSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSlice")
            .field("offset", &self.offset)
            .field("dims", &self.shard.dims)
            .finish()
    }
}

impl VectorSlice {
    /// Borrow the f32 slice for this vector.
    #[inline]
    pub fn as_f32(&self) -> &[f32] {
        // Safety: offset was validated at construction time in lookup_vector().
        self.shard
            .get_f32(self.offset)
            .expect("offset validated at construction in lookup_vector()")
    }
}

impl LazyVectorArena {
    /// Create a new lazy arena from a parsed manifest, shard source metadata,
    /// a shared cache handle, and an optional content store for remote fetching.
    pub fn new(
        manifest: VectorManifest,
        shard_sources: Vec<ShardSource>,
        cache: Arc<crate::read::leaflet_cache::LeafletCache>,
        cas: Option<Arc<dyn fluree_db_core::ContentStore>>,
    ) -> Self {
        Self {
            manifest,
            shard_sources,
            cache,
            cas,
        }
    }

    // ========================================================================
    // Manifest-only accessors (no shard loading)
    // ========================================================================

    /// Vector dimensionality.
    pub fn dims(&self) -> u16 {
        self.manifest.dims
    }

    /// Total number of vectors across all shards.
    pub fn len(&self) -> u32 {
        self.manifest.total_count
    }

    /// Whether the arena has no vectors.
    pub fn is_empty(&self) -> bool {
        self.manifest.total_count == 0
    }

    /// Whether all stored vectors have unit norm.
    pub fn is_normalized(&self) -> bool {
        self.manifest.normalized
    }

    /// Number of shards backing this arena.
    pub fn shard_count(&self) -> usize {
        self.shard_sources.len()
    }

    // ========================================================================
    // Internal shard loading — two flavors
    // ========================================================================

    /// Load shard through the global cache (point lookups, small batches).
    fn load_shard_cached(&self, shard_idx: usize) -> io::Result<Arc<VectorShard>> {
        let source = self.get_source(shard_idx)?;
        self.ensure_on_disk(source, shard_idx)?;
        let path = source.path.clone();
        let shard = self
            .cache
            .try_get_or_load_vector_shard(source.cid_hash, || {
                let bytes = std::fs::read(&path)?;
                let parsed = read_vector_shard_from_bytes(&bytes)?;
                Ok(Arc::new(parsed))
            })?;
        self.validate_shard_dims(&shard, shard_idx)?;
        Ok(shard)
    }

    /// Load shard WITHOUT inserting into the global cache.
    /// For streaming scans — avoids evicting BM25/dict/R1/R2 entries.
    fn load_shard_transient(&self, shard_idx: usize) -> io::Result<Arc<VectorShard>> {
        let source = self.get_source(shard_idx)?;
        self.ensure_on_disk(source, shard_idx)?;
        let bytes = std::fs::read(&source.path)?;
        let shard = Arc::new(read_vector_shard_from_bytes(&bytes)?);
        self.validate_shard_dims(&shard, shard_idx)?;
        Ok(shard)
    }

    fn get_source(&self, shard_idx: usize) -> io::Result<&ShardSource> {
        self.shard_sources.get(shard_idx).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "shard index {} out of range (have {} shards)",
                    shard_idx,
                    self.shard_sources.len()
                ),
            )
        })
    }

    /// Ensure a shard file exists on local disk, fetching from remote if needed.
    ///
    /// For FileStorage (all shards `on_disk: true` at construction), this is
    /// a fast Acquire-load no-op. For remote backends, uses the same sync→async
    /// bridge as `ensure_index_leaf_cached`: spawns an OS thread that calls
    /// `tokio::Handle::block_on(cs.get(&cid))`, writes the result to disk,
    /// then flips `on_disk`.
    fn ensure_on_disk(&self, source: &ShardSource, idx: usize) -> io::Result<()> {
        if source.on_disk.load(Ordering::Acquire) {
            return Ok(());
        }
        // Shard not on disk — try lazy fetch from remote CAS.
        let cas = self.cas.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("vector shard {idx} not on disk and no CAS configured"),
            )
        })?;
        let cid = source.cid.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("vector shard {idx} not on disk and no CID for remote fetch"),
            )
        })?;
        // Sync→async bridge: spawn an OS thread to block_on the async fetch.
        // Same pattern as ensure_index_leaf_cached() for index leaflets.
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("vector shard download requires a Tokio runtime"))?;
        let cs = Arc::clone(cas);
        let cid = cid.clone();
        let path = source.path.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>, String>>(1);
        std::thread::spawn(move || {
            let res = handle
                .block_on(async { cs.get(&cid).await })
                .map_err(|e| e.to_string());
            let _ = tx.send(res);
        });
        let bytes = rx
            .recv()
            .map_err(|_| io::Error::other("vector shard fetch thread died"))?
            .map_err(io::Error::other)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &bytes)?;
        source.on_disk.store(true, Ordering::Release);
        Ok(())
    }

    fn validate_shard_dims(&self, shard: &VectorShard, idx: usize) -> io::Result<()> {
        if shard.dims != self.manifest.dims {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "vector shard {} dims {} != manifest dims {}",
                    idx, shard.dims, self.manifest.dims
                ),
            ))
        } else {
            Ok(())
        }
    }

    // ========================================================================
    // Access mode 1: Filtered point-lookup (cached)
    // ========================================================================

    /// Look up a single vector by its global handle.
    ///
    /// Loads the containing shard through the cache on first access.
    /// Returns `None` if the handle is out of range.
    pub fn lookup_vector(&self, handle: u32) -> io::Result<Option<VectorSlice>> {
        if handle >= self.manifest.total_count {
            return Ok(None);
        }
        let shard_cap = self.manifest.shard_capacity;
        let shard_idx = (handle / shard_cap) as usize;
        let offset = handle % shard_cap;
        let shard = self.load_shard_cached(shard_idx)?;
        // Validate offset within partially-filled last shard
        if offset >= shard.count {
            return Ok(None);
        }
        Ok(Some(VectorSlice { shard, offset }))
    }

    /// Batch point-lookup. Sorts handles for sequential shard access,
    /// loading each shard at most once through the cache.
    pub fn lookup_many<F>(&self, handles: &mut [u32], mut f: F) -> io::Result<()>
    where
        F: FnMut(u32, &[f32]),
    {
        if handles.is_empty() {
            return Ok(());
        }
        handles.sort_unstable();

        let shard_cap = self.manifest.shard_capacity;
        let mut current_shard_idx = u32::MAX;
        let mut current_shard: Option<Arc<VectorShard>> = None;

        for &handle in handles.iter() {
            if handle >= self.manifest.total_count {
                continue;
            }
            let shard_idx = handle / shard_cap;
            let offset = handle % shard_cap;

            if shard_idx != current_shard_idx {
                current_shard = Some(self.load_shard_cached(shard_idx as usize)?);
                current_shard_idx = shard_idx;
            }

            if let Some(ref shard) = current_shard {
                if let Some(slice) = shard.get_f32(offset) {
                    f(handle, slice);
                }
            }
        }
        Ok(())
    }

    // ========================================================================
    // Access mode 2: Streaming scan (transient — no cache pollution)
    // ========================================================================

    /// Scan all vectors sequentially. Each shard is loaded transiently
    /// (not inserted into the cache) to avoid evicting other entries.
    ///
    /// The callback receives `(handle, &[f32])` and can return
    /// `ControlFlow::Break(())` to stop early.
    pub fn scan_all<F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut(u32, &[f32]) -> ControlFlow<()>,
    {
        let shard_cap = self.manifest.shard_capacity;
        for shard_idx in 0..self.shard_sources.len() {
            let shard = self.load_shard_transient(shard_idx)?;
            for offset in 0..shard.count {
                let handle = shard_idx as u32 * shard_cap + offset;
                if let Some(slice) = shard.get_f32(offset) {
                    if let ControlFlow::Break(()) = f(handle, slice) {
                        return Ok(());
                    }
                }
            }
            // shard Arc dropped here unless caller captured a reference
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arena() {
        let arena = VectorArena::new();
        assert_eq!(arena.len(), 0);
        assert!(arena.is_empty());
        assert_eq!(arena.dims(), 0);
        assert!(!arena.is_normalized()); // empty is not "normalized"
        assert!(arena.get_f32(0).is_none());
    }

    #[test]
    fn test_insert_and_retrieve_f32() {
        let mut arena = VectorArena::new();
        let v = vec![1.0f32, 2.0, 3.0];
        let h = arena.insert_f32(&v).unwrap();
        assert_eq!(h, 0);
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.dims(), 3);
        assert_eq!(arena.get_f32(0).unwrap(), &[1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_insert_f64_downcasts() {
        let mut arena = VectorArena::new();
        let v = vec![1.0f64, 2.0, 3.0];
        let h = arena.insert_f64(&v).unwrap();
        assert_eq!(h, 0);
        let slice = arena.get_f32(0).unwrap();
        assert_eq!(slice, &[1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_dims_mismatch_rejected() {
        let mut arena = VectorArena::new();
        arena.insert_f32(&[1.0, 2.0, 3.0]).unwrap();
        let err = arena.insert_f32(&[1.0, 2.0]).unwrap_err();
        assert!(err.contains("dims mismatch"));
    }

    #[test]
    fn test_normalized_tracking() {
        let mut arena = VectorArena::new();
        // Unit vector in 3D
        let inv_sqrt3 = 1.0f32 / 3.0f32.sqrt();
        arena
            .insert_f32(&[inv_sqrt3, inv_sqrt3, inv_sqrt3])
            .unwrap();
        assert!(arena.is_normalized());

        // Non-unit vector
        arena.insert_f32(&[2.0, 0.0, 0.0]).unwrap();
        assert!(!arena.is_normalized());
    }

    #[test]
    fn test_sequential_handles() {
        let mut arena = VectorArena::new();
        assert_eq!(arena.insert_f32(&[1.0, 0.0]).unwrap(), 0);
        assert_eq!(arena.insert_f32(&[0.0, 1.0]).unwrap(), 1);
        assert_eq!(arena.insert_f32(&[1.0, 1.0]).unwrap(), 2);
        assert_eq!(arena.len(), 3);
    }

    #[test]
    fn test_get_f64_upcasts() {
        let mut arena = VectorArena::new();
        arena.insert_f32(&[1.5, 2.5]).unwrap();
        let v = arena.get_f64(0).unwrap();
        assert!((v[0] - 1.5f64).abs() < 1e-6);
        assert!((v[1] - 2.5f64).abs() < 1e-6);
    }

    #[test]
    fn test_vas1_round_trip() {
        let mut arena = VectorArena::new();
        for i in 0..5 {
            arena
                .insert_f32(&[i as f32, (i * 2) as f32, (i * 3) as f32])
                .unwrap();
        }

        let dir = std::env::temp_dir().join("fluree_vas1_test");
        let _ = std::fs::create_dir_all(&dir);

        let paths = write_vector_shards(&dir, 42, &arena).unwrap();
        assert_eq!(paths.len(), 1); // 5 vectors < SHARD_CAPACITY

        let shard = read_vector_shard(&paths[0]).unwrap();
        assert_eq!(shard.dims, 3);
        assert_eq!(shard.count, 5);
        assert_eq!(shard.get_f32(0).unwrap(), &[0.0f32, 0.0, 0.0]);
        assert_eq!(shard.get_f32(4).unwrap(), &[4.0f32, 8.0, 12.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_manifest_round_trip() {
        let mut arena = VectorArena::new();
        let inv_sqrt2 = 1.0f32 / 2.0f32.sqrt();
        arena.insert_f32(&[inv_sqrt2, inv_sqrt2]).unwrap();
        arena.insert_f32(&[inv_sqrt2, -inv_sqrt2]).unwrap();

        let shard_infos = vec![ShardInfo {
            cas: "fluree:file://test".to_string(),
            count: 2,
        }];

        let dir = std::env::temp_dir().join("fluree_vam1_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.vam");

        write_vector_manifest(&path, &arena, &shard_infos).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let manifest = read_vector_manifest(&bytes).unwrap();

        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.dims, 2);
        assert_eq!(manifest.dtype, "f32");
        assert!(manifest.normalized);
        assert_eq!(manifest.total_count, 2);
        assert_eq!(manifest.shards.len(), 1);
        assert_eq!(manifest.shards[0].count, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_arena_from_shards() {
        let mut arena = VectorArena::new();
        for i in 0..5u32 {
            arena.insert_f32(&[i as f32, (i + 1) as f32]).unwrap();
        }

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 5,
            shards: vec![ShardInfo {
                cas: "test".to_string(),
                count: 5,
            }],
        };

        let shard = VectorShard {
            dims: 2,
            count: 5,
            values: arena.raw_values().to_vec(),
        };

        let loaded = load_arena_from_shards(&manifest, vec![shard]).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded.dims(), 2);
        assert_eq!(loaded.get_f32(0).unwrap(), &[0.0f32, 1.0]);
        assert_eq!(loaded.get_f32(4).unwrap(), &[4.0f32, 5.0]);
    }

    #[test]
    fn test_nan_rejected() {
        let mut arena = VectorArena::new();
        let err = arena.insert_f32(&[1.0, f32::NAN, 3.0]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");
    }

    #[test]
    fn test_infinity_rejected() {
        let mut arena = VectorArena::new();
        let err = arena.insert_f32(&[f32::INFINITY, 0.0]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");

        let mut arena2 = VectorArena::new();
        let err2 = arena2.insert_f32(&[0.0, f32::NEG_INFINITY]).unwrap_err();
        assert!(err2.contains("not finite"), "got: {err2}");
    }

    #[test]
    fn test_f64_overflow_rejected_via_insert_f64() {
        let mut arena = VectorArena::new();
        // f64::MAX overflows to f32::INFINITY
        let err = arena.insert_f64(&[1.0, f64::MAX]).unwrap_err();
        assert!(err.contains("not finite"), "got: {err}");
    }

    // ========================================================================
    // LazyVectorArena tests
    // ========================================================================

    /// Helper: write a VAS1 shard file and return a ShardSource for it.
    fn write_test_shard(
        dir: &Path,
        name: &str,
        dims: u16,
        vectors: &[&[f32]],
    ) -> (std::path::PathBuf, ShardSource) {
        let path = dir.join(name);
        let mut flat: Vec<f32> = Vec::new();
        for v in vectors {
            flat.extend_from_slice(v);
        }
        let mut buf = Vec::new();
        write_vas1_shard(&mut buf, dims, &flat).unwrap();
        std::fs::write(&path, &buf).unwrap();

        let cid_hash = crate::read::leaflet_cache::LeafletCache::cid_cache_key(name.as_bytes());
        let source = ShardSource {
            cid_hash,
            cid: None,
            path: path.clone(),
            on_disk: AtomicBool::new(true),
        };
        (path, source)
    }

    /// Helper: create a LazyVectorArena from test shards.
    fn make_lazy_arena(
        manifest: VectorManifest,
        shard_sources: Vec<ShardSource>,
    ) -> LazyVectorArena {
        let cache = Arc::new(crate::read::leaflet_cache::LeafletCache::with_max_bytes(
            10 * 1024 * 1024,
        ));
        LazyVectorArena::new(manifest, shard_sources, cache, None)
    }

    #[test]
    fn test_lazy_arena_single_shard_lookup() {
        let dir = std::env::temp_dir().join("fluree_lazy_single");
        let _ = std::fs::create_dir_all(&dir);

        let vecs: Vec<&[f32]> = vec![&[1.0, 2.0], &[3.0, 4.0], &[5.0, 6.0]];
        let (_, source) = write_test_shard(&dir, "s0.vas", 2, &vecs);

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 3,
            shards: vec![ShardInfo {
                cas: "test".to_string(),
                count: 3,
            }],
        };
        let arena = make_lazy_arena(manifest, vec![source]);

        // Manifest accessors
        assert_eq!(arena.dims(), 2);
        assert_eq!(arena.len(), 3);
        assert!(!arena.is_empty());
        assert_eq!(arena.shard_count(), 1);

        // Point lookup
        let vs0 = arena.lookup_vector(0).unwrap().unwrap();
        assert_eq!(vs0.as_f32(), &[1.0f32, 2.0]);
        let vs2 = arena.lookup_vector(2).unwrap().unwrap();
        assert_eq!(vs2.as_f32(), &[5.0f32, 6.0]);

        // Out of range
        assert!(arena.lookup_vector(3).unwrap().is_none());
        assert!(arena.lookup_vector(u32::MAX).unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_multi_shard_lookup() {
        let dir = std::env::temp_dir().join("fluree_lazy_multi");
        let _ = std::fs::create_dir_all(&dir);

        // Use shard_capacity=2 for easy boundary testing
        let vecs0: Vec<&[f32]> = vec![&[1.0, 0.0], &[0.0, 1.0]];
        let vecs1: Vec<&[f32]> = vec![&[2.0, 0.0]];
        let (_, s0) = write_test_shard(&dir, "s0.vas", 2, &vecs0);
        let (_, s1) = write_test_shard(&dir, "s1.vas", 2, &vecs1);

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: 2,
            total_count: 3,
            shards: vec![
                ShardInfo {
                    cas: "s0".to_string(),
                    count: 2,
                },
                ShardInfo {
                    cas: "s1".to_string(),
                    count: 1,
                },
            ],
        };
        let arena = make_lazy_arena(manifest, vec![s0, s1]);

        // Handle 0 → shard 0, offset 0
        assert_eq!(
            arena.lookup_vector(0).unwrap().unwrap().as_f32(),
            &[1.0f32, 0.0]
        );
        // Handle 1 → shard 0, offset 1
        assert_eq!(
            arena.lookup_vector(1).unwrap().unwrap().as_f32(),
            &[0.0f32, 1.0]
        );
        // Handle 2 → shard 1, offset 0
        assert_eq!(
            arena.lookup_vector(2).unwrap().unwrap().as_f32(),
            &[2.0f32, 0.0]
        );
        // Handle 3 → out of range
        assert!(arena.lookup_vector(3).unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_partial_shard_in_middle_breaks_handle_math() {
        let dir = std::env::temp_dir().join("fluree_lazy_partial_middle");
        let _ = std::fs::create_dir_all(&dir);

        // shard_capacity=2; create a "partial middle shard" layout:
        // - shard0: 2 vectors (full)
        // - shard1: 1 vector (partial, but NOT last)
        // - shard2: 1 vector (appended)
        //
        // With the current handle math (handle / cap), handle 3 maps to shard1 offset 1,
        // which is out of range (shard1.count=1). Even though shard2 contains the data,
        // lookup cannot reach it. This is why incremental appends must fill/replace the
        // last partial shard before appending new shards.
        let s0_vecs: Vec<&[f32]> = vec![&[1.0, 0.0], &[0.0, 1.0]];
        let s1_vecs: Vec<&[f32]> = vec![&[2.0, 0.0]];
        let s2_vecs: Vec<&[f32]> = vec![&[3.0, 0.0]];
        let (_, s0) = write_test_shard(&dir, "s0.vas", 2, &s0_vecs);
        let (_, s1) = write_test_shard(&dir, "s1.vas", 2, &s1_vecs);
        let (_, s2) = write_test_shard(&dir, "s2.vas", 2, &s2_vecs);

        let bad_manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: 2,
            total_count: 4,
            shards: vec![
                ShardInfo {
                    cas: "s0".to_string(),
                    count: 2,
                },
                ShardInfo {
                    cas: "s1".to_string(),
                    count: 1, // partial, but not last
                },
                ShardInfo {
                    cas: "s2".to_string(),
                    count: 1,
                },
            ],
        };
        let bad_arena = make_lazy_arena(bad_manifest, vec![s0, s1, s2]);

        assert!(bad_arena.lookup_vector(0).unwrap().is_some());
        assert!(bad_arena.lookup_vector(1).unwrap().is_some());
        assert!(bad_arena.lookup_vector(2).unwrap().is_some());
        assert!(
            bad_arena.lookup_vector(3).unwrap().is_none(),
            "handle 3 should be unreachable with a partial middle shard"
        );

        // Correct layout: fill/replace the partial last shard instead of appending after it.
        let s0_vecs2: Vec<&[f32]> = vec![&[1.0, 0.0], &[0.0, 1.0]];
        let s1_fixed_vecs: Vec<&[f32]> = vec![&[2.0, 0.0], &[3.0, 0.0]];
        let (_, s0b) = write_test_shard(&dir, "s0b.vas", 2, &s0_vecs2);
        let (_, s1b) = write_test_shard(&dir, "s1b.vas", 2, &s1_fixed_vecs);

        let good_manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: 2,
            total_count: 4,
            shards: vec![
                ShardInfo {
                    cas: "s0b".to_string(),
                    count: 2,
                },
                ShardInfo {
                    cas: "s1b".to_string(),
                    count: 2,
                },
            ],
        };
        let good_arena = make_lazy_arena(good_manifest, vec![s0b, s1b]);
        assert_eq!(
            good_arena.lookup_vector(3).unwrap().unwrap().as_f32(),
            &[3.0f32, 0.0]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_scan_all() {
        let dir = std::env::temp_dir().join("fluree_lazy_scan");
        let _ = std::fs::create_dir_all(&dir);

        let vecs0: Vec<&[f32]> = vec![&[1.0, 2.0], &[3.0, 4.0]];
        let vecs1: Vec<&[f32]> = vec![&[5.0, 6.0]];
        let (_, s0) = write_test_shard(&dir, "s0.vas", 2, &vecs0);
        let (_, s1) = write_test_shard(&dir, "s1.vas", 2, &vecs1);

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: 2,
            total_count: 3,
            shards: vec![
                ShardInfo {
                    cas: "s0".to_string(),
                    count: 2,
                },
                ShardInfo {
                    cas: "s1".to_string(),
                    count: 1,
                },
            ],
        };
        let arena = make_lazy_arena(manifest, vec![s0, s1]);

        let mut collected: Vec<(u32, Vec<f32>)> = Vec::new();
        arena
            .scan_all(|handle, slice| {
                collected.push((handle, slice.to_vec()));
                ControlFlow::Continue(())
            })
            .unwrap();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (0, vec![1.0f32, 2.0]));
        assert_eq!(collected[1], (1, vec![3.0f32, 4.0]));
        assert_eq!(collected[2], (2, vec![5.0f32, 6.0]));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_scan_transient_no_cache() {
        let dir = std::env::temp_dir().join("fluree_lazy_scan_nc");
        let _ = std::fs::create_dir_all(&dir);

        let vecs: Vec<&[f32]> = vec![&[1.0, 2.0]];
        let (_, source) = write_test_shard(&dir, "s0.vas", 2, &vecs);
        let cid_hash = source.cid_hash;

        let cache = Arc::new(crate::read::leaflet_cache::LeafletCache::with_max_bytes(
            10 * 1024 * 1024,
        ));
        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 1,
            shards: vec![ShardInfo {
                cas: "s0".to_string(),
                count: 1,
            }],
        };
        let arena = LazyVectorArena::new(manifest, vec![source], cache.clone(), None);

        // Scan should not populate cache
        arena.scan_all(|_, _| ControlFlow::Continue(())).unwrap();
        assert!(cache.get_vector_shard(cid_hash).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_lookup_many() {
        let dir = std::env::temp_dir().join("fluree_lazy_many");
        let _ = std::fs::create_dir_all(&dir);

        let vecs0: Vec<&[f32]> = vec![&[10.0, 20.0], &[30.0, 40.0]];
        let vecs1: Vec<&[f32]> = vec![&[50.0, 60.0]];
        let (_, s0) = write_test_shard(&dir, "s0.vas", 2, &vecs0);
        let (_, s1) = write_test_shard(&dir, "s1.vas", 2, &vecs1);

        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: 2,
            total_count: 3,
            shards: vec![
                ShardInfo {
                    cas: "s0".to_string(),
                    count: 2,
                },
                ShardInfo {
                    cas: "s1".to_string(),
                    count: 1,
                },
            ],
        };
        let arena = make_lazy_arena(manifest, vec![s0, s1]);

        let mut handles = vec![2u32, 0, 1];
        let mut results: Vec<(u32, Vec<f32>)> = Vec::new();
        arena
            .lookup_many(&mut handles, |h, slice| {
                results.push((h, slice.to_vec()));
            })
            .unwrap();

        // Results should be in sorted handle order (lookup_many sorts)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[1].0, 1);
        assert_eq!(results[2].0, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_dims_mismatch() {
        let dir = std::env::temp_dir().join("fluree_lazy_dims");
        let _ = std::fs::create_dir_all(&dir);

        // Shard has dims=3 but manifest says dims=2
        let vecs: Vec<&[f32]> = vec![&[1.0, 2.0, 3.0]];
        let (_, source) = write_test_shard(&dir, "s0.vas", 3, &vecs);

        let manifest = VectorManifest {
            version: 1,
            dims: 2, // mismatch!
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 1,
            shards: vec![ShardInfo {
                cas: "s0".to_string(),
                count: 1,
            }],
        };
        let arena = make_lazy_arena(manifest, vec![source]);

        let err = arena.lookup_vector(0).unwrap_err();
        assert!(err.to_string().contains("dims"), "got: {err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_lazy_arena_shard_not_on_disk() {
        let manifest = VectorManifest {
            version: 1,
            dims: 2,
            dtype: "f32".to_string(),
            normalized: false,
            shard_capacity: SHARD_CAPACITY,
            total_count: 1,
            shards: vec![ShardInfo {
                cas: "missing".to_string(),
                count: 1,
            }],
        };
        let source = ShardSource {
            cid_hash: 42,
            cid: None,
            path: PathBuf::from("/nonexistent/shard.vas"),
            on_disk: AtomicBool::new(false),
        };
        let arena = make_lazy_arena(manifest, vec![source]);

        let err = arena.lookup_vector(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains("not on disk"), "got: {err}");
    }
}
