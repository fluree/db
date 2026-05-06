//! Bulk import pipeline: TTL → commits → spool → merge → remap → runs → indexes → CAS → publish.
//!
//! Provides `.create("mydb").import("/path/to/chunks/").execute().await` API
//! on [`Fluree`] for high-throughput bulk import of Turtle data.
//!
//! ## Pipeline overview (Tier 2: parallel local IDs + remap)
//!
//! 1. **Create ledger** — `nameservice.publish_ledger_init(ledger_id)`
//! 2. **Parse + commit** — parallel chunk parsing with chunk-local IDs written
//!    to spool files, serial commit finalization
//! 3. **Dict merge** — merge chunk-local subject/string dicts into global dicts,
//!    produce per-chunk remap tables
//! 4. **Parallel remap** — N threads read spool files, remap IDs to global,
//!    write sorted run files
//! 5. **Build indexes** — `build_all_indexes()` from completed run files
//! 6. **CAS upload** — dicts + indexes uploaded to content-addressed storage
//! 7. **FIR6 root** — `IndexRoot` encoded and written to CAS
//! 8. **Publish** — `nameservice.publish_index_allow_equal()`
//! 9. **Cleanup** — remove tmp session directory (only on full success)
//!
//! ## Architecture
//!
//! Parse workers resolve subjects/strings to **chunk-local IDs** via per-chunk
//! dictionaries (`ChunkSubjectDict`, `ChunkStringDict`), while predicates,
//! datatypes, and graphs use globally-assigned IDs via `SharedDictAllocator`.
//! After all chunks are parsed, a merge pass deduplicates across chunks and
//! builds remap tables. Parallel remap threads then convert chunk-local IDs to
//! global IDs and produce sorted run files for the index builder.
//!
//! Commits are finalized in strict serial order (`t` increments by 1 per chunk)
//! even though chunk parsing is parallel.

use crate::error::ApiError;
use fluree_db_core::{ContentId, ContentKind, ContentStore, RemoteObject, Storage, StorageRead};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

// ============================================================================
// Configuration
// ============================================================================

/// Progress event emitted at key points during the import pipeline.
#[derive(Debug, Clone)]
pub enum ImportPhase {
    /// Reader thread is scanning through the file (emitted periodically).
    Scanning {
        /// Bytes of data read so far (excludes prefix block header).
        bytes_read: u64,
        /// Total data bytes in the file.
        total_bytes: u64,
    },
    /// Chunk parsing started (emitted before chunk 0 serial parse).
    Parsing {
        chunk: usize,
        total: usize,
        chunk_bytes: u64,
    },
    /// Chunk committed during phase 2.
    Committing {
        chunk: usize,
        total: usize,
        cumulative_flakes: u64,
        elapsed_secs: f64,
    },
    /// Index preparation stage (Tier 2): merge/persist/remap/link runs.
    ///
    /// Emitted before `Indexing` begins so the CLI doesn't appear to "hang" at 0%.
    PreparingIndex {
        /// Human-readable stage label (static string for cheap cloning).
        stage: &'static str,
    },
    /// Index build in progress for a specific subphase.
    Indexing {
        /// Human-readable stage label.
        stage: &'static str,
        /// Records/rows processed so far in this stage.
        processed_flakes: u64,
        /// Total records/rows expected in this stage.
        total_flakes: u64,
        /// Seconds elapsed since this stage started.
        stage_elapsed_secs: f64,
    },
    /// Pipeline complete.
    Done,
}

/// Callback type for import progress events.
pub type ProgressFn = Arc<dyn Fn(ImportPhase) + Send + Sync>;

/// Configuration for the bulk import pipeline.
#[derive(Clone)]
pub struct ImportConfig {
    /// Number of parallel TTL parse threads. Default: available parallelism (capped at 6).
    pub parse_threads: usize,
    /// Whether to build multi-order indexes after runs. Default: true.
    pub build_index: bool,
    /// Whether to publish to nameservice after index build. Default: true.
    pub publish: bool,
    /// Whether to delete session tmp dir on success. Default: true.
    pub cleanup_local_files: bool,
    /// Whether to zstd-compress commit blobs. Default: true.
    pub compress_commits: bool,
    /// Whether to collect ID-based stats during commit resolution. Default: true.
    ///
    /// When enabled, the import resolver performs per-op stats collection (HLL NDV,
    /// datatype counts, and optional class/property attribution) while resolving commit
    /// blobs to run records. This can be CPU-intensive and may reduce peak import
    /// throughput, but produces richer `stats.json` for the query planner.
    ///
    /// When disabled, `stats.json` falls back to cheaper summaries derived from the
    /// SPOT index build results (flake counts only).
    pub collect_id_stats: bool,
    /// Publish nameservice head every N chunks during import. Default: 50.
    /// 0 disables periodic checkpoints.
    pub publish_every: usize,
    /// Overall memory budget in MB for the import pipeline. 0 = auto-detect (60% of RAM).
    ///
    /// Used to derive `chunk_size_mb` and `max_inflight_chunks` when those fields
    /// are left at 0.
    pub memory_budget_mb: usize,
    /// Chunk size in MB for splitting a single large Turtle file. 0 = derive from budget.
    pub chunk_size_mb: usize,
    /// Maximum flakes per chunk. When importing a single large file, the chunk is
    /// split at `chunk_size_mb` OR `chunk_max_flakes`, whichever triggers first.
    /// 0 = no flake-count limit (use byte size only). Default: 20_000_000.
    ///
    /// This bounds per-commit buffer memory: 20M flakes × 40 bytes ≈ 800 MB.
    /// When importing from a directory (each file = one commit), this limit is
    /// not applied — files are never split.
    pub chunk_max_flakes: usize,
    /// Maximum number of chunk texts materialized in memory simultaneously.
    /// 0 = derive from budget.
    pub max_inflight_chunks: usize,
    /// Number of records per leaflet in the index. Default: 25_000.
    /// Larger values produce fewer, bigger leaflets (less I/O, more memory per read).
    pub leaflet_rows: usize,
    /// Number of leaflets per leaf file. Default: 10.
    /// Larger values produce fewer, bigger leaf files (less tree depth, bigger reads).
    pub leaflets_per_leaf: usize,
    /// Target rows per leaf. Default: 250_000.
    pub leaf_target_rows: usize,
    /// Optional progress callback invoked at key pipeline milestones.
    pub progress: Option<ProgressFn>,
}

impl std::fmt::Debug for ImportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportConfig")
            .field("parse_threads", &self.parse_threads)
            .field("memory_budget_mb", &self.memory_budget_mb)
            .field("chunk_size_mb", &self.chunk_size_mb)
            .field("chunk_max_flakes", &self.chunk_max_flakes)
            .field("progress", &self.progress.as_ref().map(|_| "..."))
            .finish_non_exhaustive()
    }
}

impl Default for ImportConfig {
    fn default() -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get().min(6))
            .unwrap_or(4);
        Self {
            parse_threads: threads,
            build_index: true,
            publish: true,
            cleanup_local_files: true,
            compress_commits: true,
            collect_id_stats: true,
            publish_every: 50,
            memory_budget_mb: 0,
            chunk_size_mb: 0,
            chunk_max_flakes: 20_000_000,
            max_inflight_chunks: 0,
            leaflet_rows: 25_000,
            leaflets_per_leaf: 10,
            leaf_target_rows: 250_000,
            progress: None,
        }
    }
}

// ============================================================================
// Memory budget derivation
// ============================================================================

/// Detect total system memory in MB. Falls back to 16 GB if detection fails.
#[cfg(feature = "native")]
pub fn detect_system_memory_mb() -> usize {
    use sysinfo::{MemoryRefreshKind, System};

    let mut sys = System::new();
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());
    let total_bytes = sys.total_memory();

    if total_bytes == 0 {
        tracing::warn!("could not detect system memory, falling back to 16 GB");
        16 * 1024
    } else {
        (total_bytes / (1024 * 1024)) as usize
    }
}

/// Fallback: assume 16 GB when native feature is off.
#[cfg(not(feature = "native"))]
pub fn detect_system_memory_mb() -> usize {
    16 * 1024
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

impl ImportConfig {
    /// Effective memory budget in MB (auto-detected if 0).
    pub fn effective_memory_budget_mb(&self) -> usize {
        if self.memory_budget_mb > 0 {
            self.memory_budget_mb
        } else {
            let ram = detect_system_memory_mb();
            // 60% of system RAM
            (ram as f64 * 0.60) as usize
        }
    }

    /// Effective max inflight chunks (derived from budget if 0).
    pub fn effective_max_inflight(&self) -> usize {
        if self.max_inflight_chunks > 0 {
            return self.max_inflight_chunks;
        }
        let budget = self.effective_memory_budget_mb();
        if budget >= 20 * 1024 {
            3
        } else {
            2
        }
    }

    /// Effective chunk size in MB (derived from budget if 0).
    pub fn effective_chunk_size_mb(&self) -> usize {
        if self.chunk_size_mb > 0 {
            return self.chunk_size_mb;
        }
        let budget_mb = self.effective_memory_budget_mb();
        let max_inflight = self.effective_max_inflight();
        // Budget ≈ max_inflight * chunk_size * 2.5 + run_budget + 2GB (fixed overhead)
        // Solve for chunk_size: (budget - 2048) / (max_inflight * 2.5 + 1)
        let numerator = budget_mb.saturating_sub(2048) as f64;
        let denominator = max_inflight as f64 * 2.5 + 1.0;
        let raw = (numerator / denominator).floor() as usize;
        raw.clamp(128, 768)
    }

    /// Effective run budget in MB (always auto-derived from budget and parallelism).
    pub fn effective_run_budget_mb(&self) -> usize {
        let budget_mb = self.effective_memory_budget_mb();
        let chunk_size = self.effective_chunk_size_mb();
        let threads = self.parse_threads.max(1);
        // IMPORTANT: In Tier 2, we have N independent run writers (one per remap worker),
        // so the *total* run budget must scale with parallelism. Otherwise each writer
        // gets a tiny slice and flushes many small run files, exploding disk I/O.
        //
        // Heuristic:
        // - target total run budget ≈ chunk_size × threads (so each worker can buffer ~1 chunk)
        // - cap at ~50% of the overall memory budget (leave room for dicts, parsing, etc.)
        let desired_total = chunk_size.saturating_mul(threads);
        let cap = (budget_mb / 2).max(256);
        desired_total.min(cap).max(256)
    }

    /// Cap the number of concurrent "heavy" indexing workers.
    ///
    /// These workers remap sorted-commit artifacts to per-order run files and
    /// then merge those runs into final index artifacts.
    /// Memory per worker is bounded by `per_thread_budget_bytes` (derived from
    /// `effective_run_budget_mb() / worker_count`), so we can safely run as many workers as
    /// we have parse threads (which is already capped at CPU count, max 6).
    ///
    /// Override with `FLUREE_IMPORT_HEAVY_WORKERS=<n>`.
    pub fn effective_heavy_workers(&self) -> usize {
        if let Ok(v) = std::env::var("FLUREE_IMPORT_HEAVY_WORKERS") {
            if let Ok(n) = v.parse::<usize>() {
                return n.max(1);
            }
        }
        self.parse_threads.max(1)
    }

    /// Log all computed import settings.
    pub fn log_effective_settings(&self) {
        let budget = self.effective_memory_budget_mb();
        let chunk_size = self.effective_chunk_size_mb();
        let max_inflight = self.effective_max_inflight();
        let run_budget = self.effective_run_budget_mb();
        let parallelism = self.parse_threads;

        tracing::info!(
            memory_budget_mb = budget,
            chunk_size_mb = chunk_size,
            max_inflight = max_inflight,
            run_budget_mb = run_budget,
            parallelism = parallelism,
            "import pipeline computed settings"
        );
    }

    /// Effective settings that will be used for the import (auto-derived when not set).
    /// Callers can use this to report to the user what resources the import will use.
    pub fn effective_import_settings(&self) -> EffectiveImportSettings {
        EffectiveImportSettings {
            memory_budget_mb: self.effective_memory_budget_mb(),
            parallelism: self.parse_threads,
            chunk_size_mb: self.effective_chunk_size_mb(),
            max_inflight_chunks: self.effective_max_inflight(),
        }
    }

    /// Emit a progress event (no-op when no callback is set).
    fn emit_progress(&self, phase: ImportPhase) {
        if let Some(ref cb) = self.progress {
            cb(phase);
        }
    }
}

/// Effective import resource settings (memory budget, parallelism, chunk size, etc.).
/// Used to report to the user what the import pipeline will use when values are auto-detected.
#[derive(Debug, Clone)]
pub struct EffectiveImportSettings {
    /// Memory budget in MB (60% of system RAM when not set).
    pub memory_budget_mb: usize,
    /// Number of parallel parse threads (system cores capped at 6 when not set).
    pub parallelism: usize,
    /// Chunk size in MB for large-file splitting (derived from budget when not set).
    pub chunk_size_mb: usize,
    /// Max inflight chunks (derived from budget when not set).
    pub max_inflight_chunks: usize,
}

// ============================================================================
// Result
// ============================================================================

/// Result of a successful bulk import.
#[derive(Debug)]
pub struct ImportResult {
    /// Ledger ID.
    pub ledger_id: String,
    /// Final commit t (= number of imported chunks).
    pub t: i64,
    /// Total flake count across all commits.
    pub flake_count: u64,
    /// Content identifier of the head commit.
    pub commit_head_id: fluree_db_core::ContentId,
    /// Content identifier of the index root. `None` if `build_index == false`.
    pub root_id: Option<fluree_db_core::ContentId>,
    /// Index t (same as `t` for fresh import). 0 if `build_index == false`.
    pub index_t: i64,
    /// Optional summary of top classes, properties, and connections.
    pub summary: Option<ImportSummary>,
}

/// Lightweight summary of the imported dataset for CLI display.
#[derive(Debug)]
pub struct ImportSummary {
    /// Top classes by instance count: `(class_iri, count)`.
    pub top_classes: Vec<(String, u64)>,
    /// Top properties by flake count: `(property_iri, count)`.
    pub top_properties: Vec<(String, u64)>,
    /// Top connections by count: `(source_class, property, target_class, count)`.
    pub top_connections: Vec<(String, String, String, u64)>,
}

// ============================================================================
// Error
// ============================================================================

/// Errors from the bulk import pipeline.
#[derive(Debug)]
pub enum ImportError {
    /// Ledger creation / nameservice error.
    Api(ApiError),
    /// Storage I/O error.
    Storage(String),
    /// TTL parse / commit error.
    Transact(String),
    /// Run generation / resolver error.
    RunGeneration(String),
    /// Index build error.
    IndexBuild(String),
    /// CAS upload error.
    Upload(String),
    /// Filesystem I/O error.
    Io(std::io::Error),
    /// Chunk discovery error.
    NoChunks(String),
    /// Directory contains both Turtle and JSON-LD files.
    MixedFormats(String),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "api: {e}"),
            Self::Storage(msg) => write!(f, "storage: {msg}"),
            Self::Transact(msg) => write!(f, "transact: {msg}"),
            Self::RunGeneration(msg) => write!(f, "run generation: {msg}"),
            Self::IndexBuild(msg) => write!(f, "index build: {msg}"),
            Self::Upload(msg) => write!(f, "upload: {msg}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::NoChunks(msg) => write!(f, "no chunks: {msg}"),
            Self::MixedFormats(msg) => write!(f, "mixed formats: {msg}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<ApiError> for ImportError {
    fn from(e: ApiError) -> Self {
        Self::Api(e)
    }
}

impl From<std::io::Error> for ImportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<fluree_db_core::Error> for ImportError {
    fn from(e: fluree_db_core::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

// ============================================================================
// Remote source types
// ============================================================================

/// How to enumerate objects in a remote `StorageRead` source.
///
/// `OrderedObjects` is recommended for production because the caller controls
/// commit ordering exactly. `Prefix` lists addresses lexicographically and
/// is convenient for ad-hoc imports.
#[derive(Debug, Clone)]
pub enum RemoteSource {
    /// Caller-supplied ordered list of objects to import. Production-recommended.
    OrderedObjects(Vec<RemoteObject>),
    /// Lex-sorted prefix listing via `StorageRead::list_prefix_with_metadata`.
    Prefix { prefix: String },
}

/// Where the import driver pulls bytes from.
pub(crate) enum ImportSource {
    Local(PathBuf),
    Remote {
        storage: Arc<dyn StorageRead>,
        source: RemoteSource,
    },
}

/// Format of an individual remote object, derived from its extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteFormat {
    Ttl,
    Trig,
    JsonLd,
}

/// `(chunk_index, raw_bytes)` payload sent from the remote producer to parser workers.
type RemoteChunk = (usize, Vec<u8>);

type RemoteChunkRx = Arc<std::sync::Mutex<std::sync::mpsc::Receiver<RemoteChunk>>>;

/// Bridge handle for the remote-fetch pipeline: workers receive whole-object
/// payloads from `rx`, the producer task is owned by `_producer_task`, and
/// final completion (or producer error) is signaled via `error_rx`.
///
/// **EOF semantics:** `rx` closing alone does NOT mean "import finished
/// successfully" — the import driver must await `error_rx` after parsers
/// exit to distinguish clean completion from producer failure.
pub struct RemoteChunkProducer {
    pub(crate) rx: RemoteChunkRx,
    /// Take-once handles, accessible through `&self` via Mutex<Option<_>>.
    /// `error_rx` carries `Some(err)` on producer failure, `None` on success.
    pub(crate) error_rx:
        std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<Option<ImportError>>>>,
    pub(crate) bridge_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(crate) estimated_count: usize,
    /// Per-object format, indexed by chunk_idx. Chunks arrive in the producer's
    /// input order, so chunk_idx == position in this vec.
    pub(crate) per_chunk_format: Vec<RemoteFormat>,
}

impl RemoteChunkProducer {
    fn format_at(&self, idx: usize) -> Option<RemoteFormat> {
        self.per_chunk_format.get(idx).copied()
    }

    /// True iff every chunk in this producer is `.ttl` (the parallel hot path).
    fn all_ttl(&self) -> bool {
        self.per_chunk_format
            .iter()
            .all(|f| matches!(f, RemoteFormat::Ttl))
    }

    fn has_jsonld(&self) -> bool {
        self.per_chunk_format
            .iter()
            .any(|f| matches!(f, RemoteFormat::JsonLd))
    }
}

// ============================================================================
// ChunkSource
// ============================================================================

/// Abstraction over the source of import chunks.
///
/// Three shapes:
/// - `Files`: pre-split local files, index-based access.
/// - `Streaming`: a single large local Turtle file, channel-fed by a background reader.
/// - `Remote`: a directory of remote objects, channel-fed by an async producer task.
pub enum ChunkSource {
    /// Pre-split Turtle/TriG/JSON-LD files from a directory (sorted lexicographically).
    Files(Vec<PathBuf>),
    /// Streaming reader for a single large Turtle file. Chunks are emitted
    /// through a channel as the file is read — no full pre-scan needed.
    Streaming(fluree_graph_turtle::splitter::StreamingTurtleReader),
    /// Remote-fetched whole objects (one chunk per object). Each object's
    /// prelude is auto-extracted at parse time, mirroring the local `Files` path.
    Remote(RemoteChunkProducer),
}

impl ChunkSource {
    /// Estimated number of chunks.
    ///
    /// Exact for `Files` and `Remote`, estimated for `Streaming` (file_size / chunk_size).
    pub fn estimated_len(&self) -> usize {
        match self {
            Self::Files(files) => files.len(),
            Self::Streaming(reader) => reader.estimated_chunk_count(),
            Self::Remote(producer) => producer.estimated_count,
        }
    }

    /// Whether this is a streaming source (no index-based access).
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::Streaming(_))
    }

    /// Whether this is a remote channel-fed source.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    /// Read chunk at `index` as a String (only for `Files` variant).
    ///
    /// Panics if called on `Streaming`/`Remote` — use `recv_next` instead.
    pub fn read_chunk(&self, index: usize) -> std::io::Result<String> {
        match self {
            Self::Files(files) => std::fs::read_to_string(&files[index]),
            Self::Streaming(_) | Self::Remote(_) => {
                panic!("read_chunk not supported for channel-fed source; use recv_next")
            }
        }
    }

    /// Receive the next chunk from a streaming source as ready-to-parse TTL text.
    ///
    /// Returns `Ok(Some((index, text)))` for each chunk, `Ok(None)` when done.
    /// The text includes the prefix block prepended to the raw bytes.
    /// Only valid for `Streaming` variant.
    pub fn recv_next(&self) -> std::result::Result<Option<(usize, String)>, ImportError> {
        match self {
            Self::Streaming(reader) => {
                let payload = reader
                    .recv_chunk()
                    .map_err(|e| ImportError::NoChunks(format!("streaming read failed: {e}")))?;
                match payload {
                    Some((idx, raw)) => {
                        // Note: we return only the raw TTL data for this chunk
                        // (no prefix block prepended). The streaming import
                        // path parses with a pre-extracted header prelude to
                        // avoid an extra full-chunk string copy.
                        let data = String::from_utf8(raw).map_err(|e| {
                            ImportError::Transact(format!("chunk {idx} invalid UTF-8: {e}"))
                        })?;
                        Ok(Some((idx, data)))
                    }
                    None => Ok(None),
                }
            }
            Self::Files(_) | Self::Remote(_) => {
                panic!("recv_next not supported for this source variant")
            }
        }
    }

    /// Whether chunk at `index` is a TriG file (case-insensitive).
    pub fn is_trig(&self, index: usize) -> bool {
        match self {
            Self::Files(files) => files
                .get(index)
                .and_then(|p| p.extension())
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("trig")),
            Self::Streaming(_) => false, // Streaming is Turtle only.
            Self::Remote(producer) => matches!(producer.format_at(index), Some(RemoteFormat::Trig)),
        }
    }

    /// Whether chunk at `index` is a JSON-LD file (case-insensitive `.jsonld`).
    pub fn is_jsonld(&self, index: usize) -> bool {
        match self {
            Self::Files(files) => files
                .get(index)
                .and_then(|p| p.extension())
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonld")),
            Self::Streaming(_) => false,
            Self::Remote(producer) => {
                matches!(producer.format_at(index), Some(RemoteFormat::JsonLd))
            }
        }
    }

    /// Whether any file in this source is JSON-LD.
    ///
    /// Used to force serial import — the parallel pipeline only handles Turtle.
    pub fn has_jsonld(&self) -> bool {
        match self {
            Self::Files(files) => files.iter().any(|p| {
                p.extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonld"))
            }),
            Self::Streaming(_) => false,
            Self::Remote(producer) => producer.has_jsonld(),
        }
    }
}

/// Resolve the import path into a `ChunkSource`.
///
/// - If `path` is a directory: discover `.ttl`/`.trig`/`.jsonld` files (sorted lexicographically).
/// - If `path` is a single large `.ttl` file: auto-split using `TurtleChunkReader`.
/// - If `path` is a single small `.ttl`/`.trig`/`.jsonld` file: treat as a single-element `Files` source.
fn resolve_chunk_source(
    path: &Path,
    config: &ImportConfig,
) -> std::result::Result<ChunkSource, ImportError> {
    if path.is_dir() {
        let files = discover_chunks(path)?;
        return Ok(ChunkSource::Files(files));
    }

    if !path.exists() {
        return Err(ImportError::NoChunks(format!(
            "path does not exist: {}",
            path.display()
        )));
    }

    // Single file — decide whether to auto-split based on size.
    let file_size = std::fs::metadata(path)?.len();
    let chunk_size_bytes = config.effective_chunk_size_mb() as u64 * 1024 * 1024;

    let is_ttl = path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("ttl"));

    if is_ttl && file_size > chunk_size_bytes {
        // Large file: stream chunks via background reader thread.
        //
        // Reader channel capacity controls how many raw chunks (~chunk_size_mb
        // each) the reader thread can buffer ahead of parsing. This is the
        // primary memory/throughput knob for streaming imports, and is exposed
        // via max_inflight_chunks / --max-inflight so operators can tune it.
        let reader_channel_capacity = config.effective_max_inflight();

        // Build progress callback that forwards to the import progress handler.
        let scan_progress: Option<fluree_graph_turtle::splitter::ScanProgressFn> =
            config.progress.as_ref().map(|cb| {
                let cb = Arc::clone(cb);
                let f: fluree_graph_turtle::splitter::ScanProgressFn =
                    Arc::new(move |bytes_read, total_bytes| {
                        cb(ImportPhase::Scanning {
                            bytes_read,
                            total_bytes,
                        });
                    });
                f
            });

        let reader = fluree_graph_turtle::splitter::StreamingTurtleReader::new(
            path,
            chunk_size_bytes,
            reader_channel_capacity,
            scan_progress,
        )
        .map_err(|e| ImportError::NoChunks(format!("turtle file split failed: {e}")))?;
        tracing::info!(
            estimated_chunks = reader.estimated_chunk_count(),
            chunk_size_mb = config.effective_chunk_size_mb(),
            file_size_mb = file_size / (1024 * 1024),
            "streaming large Turtle file (no pre-scan)"
        );
        Ok(ChunkSource::Streaming(reader))
    } else {
        // Small file or non-TTL: treat as a single-element source.
        Ok(ChunkSource::Files(vec![path.to_path_buf()]))
    }
}

/// Resolve a remote source (`OrderedObjects` or `Prefix`) into a list of
/// `RemoteObject`s, sorted lex by address for `Prefix` mode.
///
/// Returns the accepted objects and their per-chunk formats (parallel to
/// the objects vec). Rejects mixing Turtle (`.ttl`/`.trig`) with JSON-LD
/// (`.jsonld`), mirroring the local `scan_directory_format` rule.
async fn resolve_remote_objects(
    storage: &Arc<dyn StorageRead>,
    source: &RemoteSource,
) -> std::result::Result<(Vec<RemoteObject>, Vec<RemoteFormat>), ImportError> {
    let all_objects = match source {
        RemoteSource::OrderedObjects(objs) => objs.clone(),
        RemoteSource::Prefix { prefix } => {
            let mut listed = storage
                .list_prefix_with_metadata(prefix)
                .await
                .map_err(|e| {
                    ImportError::Storage(format!(
                        "list_prefix_with_metadata({prefix:?}) failed: {e}; \
                     backend may not support metadata listing — \
                     use RemoteSource::OrderedObjects to supply addresses+sizes directly"
                    ))
                })?;
            listed.sort_by(|a, b| a.address.cmp(&b.address));
            listed
        }
    };

    if all_objects.is_empty() {
        return Err(ImportError::NoChunks(
            "remote source contains no objects".to_string(),
        ));
    }

    // Detect format by extension (mirrors local discover_chunks rules).
    // Turtle-family (.ttl/.trig) and JSON-LD must not be mixed in a single import.
    let mut has_ttl = false;
    let mut has_trig = false;
    let mut has_jsonld = false;
    let mut accepted: Vec<RemoteObject> = Vec::with_capacity(all_objects.len());
    let mut extensions: Vec<RemoteFormat> = Vec::with_capacity(all_objects.len());
    for obj in all_objects {
        let ext = std::path::Path::new(&obj.address)
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);
        match ext.as_deref() {
            Some("ttl") => {
                has_ttl = true;
                accepted.push(obj);
                extensions.push(RemoteFormat::Ttl);
            }
            Some("trig") => {
                has_trig = true;
                accepted.push(obj);
                extensions.push(RemoteFormat::Trig);
            }
            Some("jsonld") => {
                has_jsonld = true;
                accepted.push(obj);
                extensions.push(RemoteFormat::JsonLd);
            }
            _ => {
                // Skip non-data files silently (mirrors local behavior).
            }
        }
    }

    let has_turtle_family = has_ttl || has_trig;
    if has_turtle_family && has_jsonld {
        return Err(ImportError::MixedFormats(
            "remote source contains both Turtle (.ttl/.trig) and JSON-LD (.jsonld) objects; \
             use a single format family per import"
                .into(),
        ));
    }

    if accepted.is_empty() {
        return Err(ImportError::NoChunks(
            "remote source contains no .ttl/.trig/.jsonld objects".into(),
        ));
    }

    // `.trig` import is wired through the same serial path the local
    // `.import(dir)` uses, but that path has a documented upstream limitation
    // in `import_trig_commit` (fluree-db-transact/src/import.rs): the Tier 2
    // spool/index pipeline does not fully capture TriG content. Imported TriG
    // data may not become queryable. Fail loud rather than silently producing
    // a half-imported ledger.
    if has_trig {
        return Err(ImportError::NoChunks(
            "remote .trig import is not currently supported: the Tier 2 import \
             pipeline does not fully capture named-graph or default-graph TriG \
             content, so imported data may not become queryable. Convert TriG \
             to .ttl or .jsonld before import. See `import_trig_commit` in \
             fluree-db-transact/src/import.rs for context."
                .into(),
        ));
    }

    Ok((accepted, extensions))
}

/// Spawn the async producer task + bridge thread for a remote source.
///
/// Producer task: runs on the current tokio runtime, fetches each object
/// via `StorageRead::read_bytes` in order, sends `(idx, bytes)` into a
/// bounded tokio channel. On error, sends the error to `error_tx` and
/// drops the channel.
///
/// Bridge thread: blocking_recv from tokio channel, forwards to a
/// `std::sync::mpsc::SyncSender` that parser workers drain. This keeps
/// parser workers entirely off the tokio runtime (no `block_on`).
fn spawn_remote_producer(
    storage: Arc<dyn StorageRead>,
    objects: Vec<RemoteObject>,
    per_chunk_format: Vec<RemoteFormat>,
    in_flight: usize,
) -> RemoteChunkProducer {
    let estimated_count = objects.len();
    debug_assert_eq!(estimated_count, per_chunk_format.len());
    let in_flight = in_flight.max(1);

    // tokio mpsc — async producer side.
    let (tokio_tx, mut tokio_rx) = tokio::sync::mpsc::channel::<(usize, Vec<u8>)>(in_flight);
    // std mpsc — sync worker side. Capacity 2 (small handoff buffer; tokio
    // channel is the real backpressure knob).
    let (std_tx, std_rx) = std::sync::mpsc::sync_channel::<(usize, Vec<u8>)>(2);
    let (error_tx, error_rx) = tokio::sync::oneshot::channel::<Option<ImportError>>();

    // Producer task — async on tokio.
    tokio::spawn(async move {
        for (idx, obj) in objects.into_iter().enumerate() {
            let bytes = match storage.read_bytes(&obj.address).await {
                Ok(b) => b,
                Err(e) => {
                    let _ = error_tx.send(Some(ImportError::Storage(format!(
                        "remote read failed for {} ({} bytes expected): {e}",
                        obj.address, obj.size_bytes
                    ))));
                    return;
                }
            };
            // Sanity check vs reported size — log on mismatch but continue.
            if obj.size_bytes != 0 && bytes.len() as u64 != obj.size_bytes {
                tracing::warn!(
                    address = %obj.address,
                    expected = obj.size_bytes,
                    actual = bytes.len(),
                    "remote object size differs from listing metadata"
                );
            }
            if tokio_tx.send((idx, bytes)).await.is_err() {
                // Bridge dropped — pipeline aborted upstream. Exit cleanly.
                let _ = error_tx.send(None);
                return;
            }
        }
        // Normal EOF: signal success.
        let _ = error_tx.send(None);
    });

    // Bridge thread — blocking_recv → sync channel.
    let bridge_handle = std::thread::Builder::new()
        .name("ttl-remote-bridge".into())
        .spawn(move || {
            while let Some(payload) = tokio_rx.blocking_recv() {
                if std_tx.send(payload).is_err() {
                    break;
                }
            }
        })
        .expect("spawn remote bridge thread");

    RemoteChunkProducer {
        rx: Arc::new(std::sync::Mutex::new(std_rx)),
        error_rx: std::sync::Mutex::new(Some(error_rx)),
        bridge_handle: std::sync::Mutex::new(Some(bridge_handle)),
        estimated_count,
        per_chunk_format,
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for a bulk import operation.
///
/// Created via `fluree.create("mydb").import("/path/to/chunks")`.
///
/// # Example
///
/// ```ignore
/// let result = fluree.create("mydb")
///     .import("/data/chunks/")
///     .memory_budget_mb(24000)
///     .execute()
///     .await?;
/// ```
pub struct ImportBuilder<'a> {
    fluree: &'a super::Fluree,
    ledger_id: String,
    source: ImportSource,
    config: ImportConfig,
}

impl<'a> ImportBuilder<'a> {
    pub(crate) fn new(fluree: &'a super::Fluree, ledger_id: String, import_path: PathBuf) -> Self {
        Self {
            fluree,
            ledger_id,
            source: ImportSource::Local(import_path),
            config: ImportConfig::default(),
        }
    }

    pub(crate) fn new_remote(
        fluree: &'a super::Fluree,
        ledger_id: String,
        storage: Arc<dyn StorageRead>,
        source: RemoteSource,
    ) -> Self {
        Self {
            fluree,
            ledger_id,
            source: ImportSource::Remote { storage, source },
            config: ImportConfig::default(),
        }
    }

    /// Set the number of parallel TTL parse threads.
    pub fn threads(mut self, n: usize) -> Self {
        self.config.parse_threads = n;
        self
    }

    /// Set the overall memory budget in MB. 0 = auto-detect (60% of RAM).
    pub fn memory_budget_mb(mut self, mb: usize) -> Self {
        self.config.memory_budget_mb = mb;
        self
    }

    /// Set the chunk size in MB for large-file splitting. 0 = derive from budget.
    pub fn chunk_size_mb(mut self, mb: usize) -> Self {
        self.config.chunk_size_mb = mb;
        self
    }

    /// Set the maximum flakes per chunk. 0 = no limit. Default: 20_000_000.
    pub fn chunk_max_flakes(mut self, n: usize) -> Self {
        self.config.chunk_max_flakes = n;
        self
    }

    /// Set the parallelism (alias for `.threads()`).
    pub fn parallelism(mut self, n: usize) -> Self {
        self.config.parse_threads = n;
        self
    }

    /// Whether to build indexes after import. Default: true.
    pub fn build_index(mut self, v: bool) -> Self {
        self.config.build_index = v;
        self
    }

    /// Whether to publish to nameservice. Default: true.
    pub fn publish(mut self, v: bool) -> Self {
        self.config.publish = v;
        self
    }

    /// Whether to clean up tmp files on success. Default: true.
    pub fn cleanup(mut self, v: bool) -> Self {
        self.config.cleanup_local_files = v;
        self
    }

    /// Whether to zstd-compress commit blobs. Default: true.
    pub fn compress(mut self, v: bool) -> Self {
        self.config.compress_commits = v;
        self
    }

    /// Whether to collect ID-based stats during commit resolution. Default: true.
    pub fn collect_id_stats(mut self, v: bool) -> Self {
        self.config.collect_id_stats = v;
        self
    }

    /// Publish nameservice checkpoint every N chunks. Default: 50. 0 disables.
    pub fn publish_every(mut self, n: usize) -> Self {
        self.config.publish_every = n;
        self
    }

    /// Set the number of records per leaflet. Default: 25_000.
    /// Larger values produce fewer, bigger leaflets (less I/O overhead).
    pub fn leaflet_rows(mut self, n: usize) -> Self {
        self.config.leaflet_rows = n;
        self
    }

    /// Set the number of leaflets per leaf file. Default: 10.
    /// Larger values produce fewer leaf files (shallower tree).
    pub fn leaflets_per_leaf(mut self, n: usize) -> Self {
        self.config.leaflets_per_leaf = n;
        self
    }

    /// Set the target rows per leaf. Default: 250_000.
    pub fn leaf_target_rows(mut self, n: usize) -> Self {
        self.config.leaf_target_rows = n;
        self
    }

    /// Set a progress callback invoked at key pipeline milestones.
    pub fn on_progress(mut self, f: impl Fn(ImportPhase) + Send + Sync + 'static) -> Self {
        self.config.progress = Some(Arc::new(f));
        self
    }

    /// Effective resource settings that will be used for this import (auto-derived when not set).
    /// Use this to report to the user what memory budget and parallelism the import will use.
    pub fn effective_import_settings(&self) -> EffectiveImportSettings {
        self.config.effective_import_settings()
    }

    /// Execute the bulk import pipeline.
    pub async fn execute(self) -> std::result::Result<ImportResult, ImportError> {
        let storage = self
            .fluree
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| {
                ImportError::Storage("bulk import requires a managed storage backend".into())
            })?;
        run_import_pipeline(
            &storage,
            self.fluree.publisher()?,
            &self.ledger_id,
            self.source,
            &self.config,
        )
        .await
    }
}

// ============================================================================
// Create builder (intermediate)
// ============================================================================

/// Intermediate builder returned by `fluree.create("mydb")`.
///
/// Supports `.import(path)` for bulk import, or `.execute()` for empty ledger creation.
pub struct CreateBuilder<'a> {
    fluree: &'a super::Fluree,
    ledger_id: String,
}

impl<'a> CreateBuilder<'a> {
    pub(crate) fn new(fluree: &'a super::Fluree, ledger_id: String) -> Self {
        Self { fluree, ledger_id }
    }

    /// Attach a bulk import to this create operation.
    ///
    /// `path` can be a directory containing `.ttl`/`.trig`/`.jsonld` files
    /// (sorted lexicographically), or a single `.ttl`/`.jsonld` file.
    pub fn import(self, path: impl AsRef<Path>) -> ImportBuilder<'a> {
        ImportBuilder::new(self.fluree, self.ledger_id, path.as_ref().to_path_buf())
    }

    /// Attach a bulk import that streams source bytes from a remote
    /// `StorageRead` backend (e.g. S3) instead of local disk.
    ///
    /// Each remote object is fetched whole into memory by an async producer
    /// task and parsed by the existing pipeline — no input is staged to
    /// local disk. **Scratch (spool/runs/index) still uses local disk** under
    /// `FLUREE_IMPORT_DIR`; size your runtime accordingly.
    ///
    /// Supports either all-`.ttl` or all-`.jsonld` per import. Pure `.ttl`
    /// imports take a parallel parser pool; `.jsonld` takes a serial path —
    /// same as the local fallback — because the JSON-LD parser does not
    /// parallelize across chunks. Mixing `.ttl` with `.jsonld` in a single
    /// import is rejected, matching the local `.import` rule.
    ///
    /// `.trig` is currently rejected with an explicit error: the underlying
    /// TriG-via-import path has a known upstream limitation (named-graph
    /// flakes are not captured by the Tier 2 spool pipeline, and even
    /// default-graph TriG content does not become queryable). When that
    /// upstream issue is resolved, `.trig` will be enabled here without
    /// further API changes — the serial remote arm already dispatches it
    /// correctly.
    pub fn import_from_storage(
        self,
        storage: Arc<dyn StorageRead>,
        source: RemoteSource,
    ) -> ImportBuilder<'a> {
        ImportBuilder::new_remote(self.fluree, self.ledger_id, storage, source)
    }
}

// ============================================================================
// Directory format detection
// ============================================================================

/// What kind of data files a directory contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryFormat {
    /// Only `.ttl` / `.trig` files found.
    Turtle,
    /// Only `.jsonld` files found.
    JsonLd,
}

/// Scan a directory and determine its data format.
///
/// Returns [`DirectoryFormat::Turtle`] if all supported files are `.ttl`/`.trig`,
/// [`DirectoryFormat::JsonLd`] if all are `.jsonld`.
/// Returns [`ImportError::MixedFormats`] on mixed formats,
/// [`ImportError::NoChunks`] on empty directories or directories with no supported files.
pub fn scan_directory_format(dir: &Path) -> std::result::Result<DirectoryFormat, ImportError> {
    let mut has_turtle = false;
    let mut has_jsonld = false;

    for entry in std::fs::read_dir(dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().is_ok_and(|ft| ft.is_file()) {
            continue;
        }
        if let Some(ext) = entry.path().extension().and_then(|e| e.to_str()) {
            match ext.to_ascii_lowercase().as_str() {
                "ttl" | "trig" => has_turtle = true,
                "jsonld" => has_jsonld = true,
                _ => {}
            }
        }
    }

    match (has_turtle, has_jsonld) {
        (true, true) => Err(ImportError::MixedFormats(format!(
            "directory {} contains both Turtle (.ttl/.trig) and JSON-LD (.jsonld) files; \
             use a single format per directory",
            dir.display(),
        ))),
        (true, false) => Ok(DirectoryFormat::Turtle),
        (false, true) => Ok(DirectoryFormat::JsonLd),
        (false, false) => Err(ImportError::NoChunks(format!(
            "no supported data files (.ttl, .trig, .jsonld) found in {}",
            dir.display()
        ))),
    }
}

// ============================================================================
// Chunk discovery
// ============================================================================

/// Discover and sort `.ttl`, `.trig`, or `.jsonld` files from a directory (case-insensitive).
///
/// Returns an error if the directory contains a mix of Turtle (`.ttl`/`.trig`) and
/// JSON-LD (`.jsonld`) files — all files must be the same format family.
fn discover_chunks(dir: &Path) -> std::result::Result<Vec<PathBuf>, ImportError> {
    if !dir.is_dir() {
        // Single file import
        if dir.exists() {
            return Ok(vec![dir.to_path_buf()]);
        }
        return Err(ImportError::NoChunks(format!(
            "path does not exist: {}",
            dir.display()
        )));
    }

    // Validate format consistency (also catches empty directories).
    scan_directory_format(dir)?;

    let mut chunks: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_ok_and(|ft| ft.is_file()))
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| {
                    ext.eq_ignore_ascii_case("ttl")
                        || ext.eq_ignore_ascii_case("trig")
                        || ext.eq_ignore_ascii_case("jsonld")
                })
        })
        .collect();

    chunks.sort();
    Ok(chunks)
}

// ============================================================================
// Import pipeline
// ============================================================================

/// Core import pipeline. Orchestrates all phases.
async fn run_import_pipeline<S>(
    storage: &S,
    nameservice: &dyn crate::NameServicePublisher,
    alias: &str,
    import_source: ImportSource,
    config: &ImportConfig,
) -> std::result::Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let pipeline_start = Instant::now();
    let span = tracing::debug_span!("bulk_import", alias = %alias);

    async {
        // ---- Log effective settings and resolve chunk source ----
        config.log_effective_settings();
        let chunk_source = match &import_source {
            ImportSource::Local(path) => {
                let cs = resolve_chunk_source(path, config)?;
                tracing::info!(
                    estimated_chunks = cs.estimated_len(),
                    streaming = cs.is_streaming(),
                    path = %path.display(),
                    "resolved import chunks"
                );
                cs
            }
            ImportSource::Remote { storage, source } => {
                let (objects, per_chunk_format) = resolve_remote_objects(storage, source).await?;
                let count = objects.len();
                let total_bytes: u64 = objects.iter().map(|o| o.size_bytes).sum();
                let in_flight = config.effective_max_inflight();

                // Each remote object is fetched whole into memory by the
                // producer (MVP does not split single objects via byte-range).
                // Warn if any object exceeds the configured chunk size — peak
                // memory will be `largest_object × in_flight`.
                let chunk_size_bytes = config.effective_chunk_size_mb() as u64 * 1024 * 1024;
                if let Some(largest) = objects.iter().max_by_key(|o| o.size_bytes) {
                    if largest.size_bytes > chunk_size_bytes {
                        tracing::warn!(
                            address = %largest.address,
                            size_mb = largest.size_bytes / (1024 * 1024),
                            chunk_size_mb = config.effective_chunk_size_mb(),
                            in_flight,
                            "remote object exceeds configured chunk size; \
                             single-object byte-range splitting is not yet \
                             supported — object will be materialized whole. \
                             Pre-split large objects or raise chunk_size_mb \
                             to silence this warning."
                        );
                    }
                }

                let producer = spawn_remote_producer(
                    Arc::clone(storage),
                    objects,
                    per_chunk_format,
                    in_flight,
                );
                tracing::info!(
                    estimated_chunks = count,
                    total_bytes,
                    in_flight,
                    "resolved remote import chunks"
                );
                ChunkSource::Remote(producer)
            }
        };

        // ---- Phase 1: Create ledger (init nameservice) ----
        let normalized_alias = fluree_db_core::ledger_id::normalize_ledger_id(alias)
            .unwrap_or_else(|_| alias.to_string());

        // Check if ledger already exists
        let ns_record = nameservice
            .lookup(&normalized_alias)
            .await
            .map_err(|e| ImportError::Storage(e.to_string()))?;

        let needs_init = match &ns_record {
            None => true,
            Some(record) if record.retracted => {
                // Ledger was dropped — safe to re-create.
                tracing::info!(alias = %normalized_alias, "re-initializing retracted ledger");
                true
            }
            Some(record) if record.commit_t > 0 || record.commit_head_id.is_some() => {
                return Err(ImportError::Transact(format!(
                    "import requires a fresh ledger, but '{}' already has commits (t={})",
                    normalized_alias, record.commit_t
                )));
            }
            Some(_) => false,
        };

        if needs_init {
            nameservice
                .publish_ledger_init(&normalized_alias)
                .await
                .map_err(|e| ImportError::Storage(e.to_string()))?;
            tracing::info!(alias = %normalized_alias, "initialized new ledger in nameservice");
        }

        // ---- Set up session directory for runs/indexes ----
        let alias_prefix =
            fluree_db_core::address_path::ledger_id_to_path_prefix(&normalized_alias)
                .unwrap_or_else(|_| normalized_alias.replace(':', "/"));

        // Derive session dir from storage's data directory.
        // For file storage: {data_dir}/{alias_path}/tmp_import/{session_id}/
        let sid = session_id();
        let session_dir = derive_session_dir(storage, &alias_prefix, &sid);
        let run_dir = session_dir.join("runs");
        let index_dir = session_dir.join("index");
        std::fs::create_dir_all(&run_dir)?;

        tracing::info!(
            session_dir = %session_dir.display(),
            run_dir = %run_dir.display(),
            "import session directory created"
        );

        // ---- Phases 2-6: Import, build, upload, publish ----
        // Wrapped in a helper to ensure cleanup semantics:
        // - On success or failure + cleanup_local_files=true → delete session dir
        // - If cleanup itself fails → log warning, do not fail import
        let paths = PipelinePaths {
            run_dir: &run_dir,
            index_dir: &index_dir,
        };
        let chunk_source = std::sync::Arc::new(chunk_source);
        let pipeline_result = run_pipeline_phases(
            storage,
            nameservice,
            &normalized_alias,
            &chunk_source,
            paths,
            config,
            pipeline_start,
        )
        .await;

        // Cleanup session dir on both success and failure to avoid accumulating
        // hundreds of GB of orphaned temp files from failed imports.
        if config.cleanup_local_files {
            if let Err(e) = std::fs::remove_dir_all(&session_dir) {
                tracing::warn!(
                    session_dir = %session_dir.display(),
                    error = %e,
                    "failed to clean up import session directory"
                );
            } else {
                tracing::info!(
                    session_dir = %session_dir.display(),
                    "import session directory cleaned up"
                );
            }
        } else {
            tracing::info!(
                session_dir = %session_dir.display(),
                "cleanup disabled; import artifacts retained"
            );
        }

        match pipeline_result {
            Ok(result) => {
                let total_elapsed = pipeline_start.elapsed();
                tracing::info!(
                    alias = %normalized_alias,
                    t = result.t,
                    flakes = result.flake_count,
                    root_id = ?result.root_id,
                    elapsed = ?total_elapsed,
                    "bulk import pipeline complete"
                );

                Ok(result)
            }
            Err(e) => Err(e),
        }
    }
    .instrument(span)
    .await
}

// ============================================================================
// Pipeline phases 2-6
// ============================================================================

/// Paths used by the import pipeline.
struct PipelinePaths<'a> {
    /// Directory for run files.
    run_dir: &'a Path,
    /// Directory for index files.
    index_dir: &'a Path,
}

/// Input parameters for index building and uploading.
struct IndexBuildInput<'a> {
    /// Directory containing run files.
    run_dir: &'a Path,
    /// Directory for index output.
    index_dir: &'a Path,
    /// Final transaction t value.
    final_t: i64,
    /// Namespace code to prefix mappings.
    namespace_codes: &'a HashMap<u16, String>,
    /// Total flakes from commit resolution (used for indexing progress).
    cumulative_flakes: u64,
    /// V2-native sorted commit artifacts for the index build.
    sorted_commit_infos: Vec<fluree_db_indexer::run_index::SortedCommitInfo>,
    /// Unified language dict (global lang_id → tag string), built from per-chunk
    /// lang vocab files. All indexes use this mapping.
    unified_lang_dict: fluree_db_indexer::run_index::LanguageTagDict,
    /// Per-chunk language remap tables (chunk-local lang_id → global lang_id).
    /// Built from per-chunk lang vocab files and reused during the run-generation stage.
    lang_remaps: Vec<Vec<u16>>,
    // Kept for: future import stats/index metadata expansion.
    // Use when: import pipeline is extended with per-property stats + class tracking.
    /// Predicate field width in bytes (1, 2, or 4). Pre-computed from predicate
    /// dict size to avoid re-reading predicates.json in build_and_upload.
    #[expect(dead_code)]
    p_width: u8,
    /// Datatype field width in bytes (1 or 2). Pre-computed from datatype dict
    /// size to avoid re-reading datatypes.dict in build_and_upload.
    #[expect(dead_code)]
    dt_width: u8,
    /// Predicate ID for rdf:type (for inline class stats during index build).
    rdf_type_p_id: u32,
    /// Whether to collect ID-based stats during import index build.
    collect_id_stats: bool,
    /// Turtle @prefix IRI → short prefix name, for IRI compaction in display.
    prefix_map: &'a HashMap<String, String>,
}

/// Run phases 2-6: import chunks, build indexes, upload to CAS, write V4 root, publish.
///
/// Separated from `run_import_pipeline` to enable clean error-path handling:
/// on failure, the caller keeps the session dir for debugging.
async fn run_pipeline_phases<S>(
    storage: &S,
    nameservice: &dyn crate::NameServicePublisher,
    alias: &str,
    chunk_source: &std::sync::Arc<ChunkSource>,
    paths: PipelinePaths<'_>,
    config: &ImportConfig,
    pipeline_start: Instant,
) -> std::result::Result<ImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // ---- Phase 2: Import TTL → commits + streaming runs ----
    let import_result = run_import_chunks(
        storage,
        nameservice,
        alias,
        chunk_source,
        paths.run_dir,
        config,
    )
    .await?;

    tracing::info!(
        t = import_result.final_t,
        flakes = import_result.cumulative_flakes,
        commit_head = %import_result.commit_head_id,
        elapsed = ?pipeline_start.elapsed(),
        "import + run generation complete"
    );

    // ---- Phases 3-6: Build index, upload, root, publish ----
    let root_id;
    let index_t;
    let summary;

    if config.build_index {
        let build_input = IndexBuildInput {
            run_dir: paths.run_dir,
            index_dir: paths.index_dir,
            final_t: import_result.final_t,
            namespace_codes: &import_result.namespace_codes,
            cumulative_flakes: import_result.cumulative_flakes,
            sorted_commit_infos: import_result.sorted_commit_infos,
            unified_lang_dict: import_result.unified_lang_dict,
            lang_remaps: import_result.lang_remaps,
            p_width: import_result.p_width,
            dt_width: import_result.dt_width,
            rdf_type_p_id: import_result.rdf_type_p_id,
            collect_id_stats: config.collect_id_stats,
            prefix_map: &import_result.prefix_map,
        };
        let index_result = build_and_upload(
            storage,
            nameservice,
            alias,
            build_input,
            config,
            import_result.total_commit_size,
            import_result.total_asserts,
            import_result.total_retracts,
        )
        .await?;

        // Publish index CID to nameservice so the server can find the root.
        if config.publish {
            nameservice
                .publish_index(alias, index_result.index_t, &index_result.root_id)
                .await
                .map_err(|e| ImportError::Storage(format!("publish index: {e}")))?;
            tracing::info!(
                index_t = index_result.index_t,
                root_id = %index_result.root_id,
                "published index root to nameservice"
            );
        }

        root_id = Some(index_result.root_id);
        index_t = index_result.index_t;
        summary = index_result.summary;
    } else {
        root_id = None;
        index_t = 0;
        summary = None;
    }

    // ---- Phase 7: Persist default context from turtle prefixes ----
    if let Err(e) =
        store_default_context(storage, nameservice, alias, &import_result.prefix_map).await
    {
        tracing::warn!(%e, "failed to persist default context (non-fatal)");
    }

    config.emit_progress(ImportPhase::Done);

    Ok(ImportResult {
        ledger_id: alias.to_string(),
        t: import_result.final_t,
        flake_count: import_result.cumulative_flakes,
        commit_head_id: import_result.commit_head_id,
        root_id,
        index_t,
        summary,
    })
}

// ============================================================================
// Phase 2: Import chunks
// ============================================================================

/// Lightweight per-commit metadata collected during the serial commit loop.
/// Used to generate the txn-meta "meta chunk" without re-reading commit blobs.
struct CommitMeta {
    /// ContentId hex digest (64-char SHA-256 hex).
    commit_hash_hex: String,
    /// Transaction number.
    t: i64,
    /// Commit blob size in bytes.
    blob_bytes: usize,
    /// Number of flakes (= asserts for fresh import).
    flake_count: u32,
    /// Epoch milliseconds (parsed once at collection time). `None` if no timestamp.
    time_epoch_ms: Option<i64>,
    /// Previous commit's hex digest (for db:previous), `None` for first commit.
    previous_commit_hex: Option<String>,
}

/// Internal result from the import phase (before index build).
struct ChunkImportResult {
    final_t: i64,
    cumulative_flakes: u64,
    commit_head_id: fluree_db_core::ContentId,
    namespace_codes: HashMap<u16, String>,
    /// Total size of all commit blobs in bytes.
    total_commit_size: u64,
    /// Total number of assertions across all commits.
    total_asserts: u64,
    /// Total number of retractions across all commits.
    total_retracts: u64,
    /// Turtle @prefix short names accumulated across all chunks: IRI → short prefix.
    prefix_map: HashMap<String, String>,
    /// V2-native sorted commit artifacts for the index build.
    sorted_commit_infos: Vec<fluree_db_indexer::run_index::SortedCommitInfo>,
    /// Unified language dict (global lang_id → tag string), built from per-chunk
    /// lang vocab files. All indexes use this mapping.
    unified_lang_dict: fluree_db_indexer::run_index::LanguageTagDict,
    /// Per-chunk language remap tables (chunk-local lang_id → global lang_id).
    lang_remaps: Vec<Vec<u16>>,
    /// Predicate field width in bytes (1, 2, or 4).
    p_width: u8,
    /// Datatype field width in bytes (1 or 2).
    dt_width: u8,
    /// Predicate ID for rdf:type (for inline class stats during SPOT merge).
    rdf_type_p_id: u32,
}

/// Import all TTL chunks: parallel parse + serial commit + streaming runs.
async fn run_import_chunks<S>(
    storage: &S,
    nameservice: &dyn crate::NameServicePublisher,
    alias: &str,
    chunk_source: &std::sync::Arc<ChunkSource>,
    run_dir: &Path,
    config: &ImportConfig,
) -> std::result::Result<ChunkImportResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_indexer::run_index::{persist_namespaces, SortedCommitInfo};
    use fluree_db_transact::import::{
        finalize_parsed_chunk, import_trig_commit, parse_chunk, parse_chunk_with_prelude,
        parse_jsonld_chunk, ImportState, ParsedChunk,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn current_custom_datatype_iris(
        datatype_alloc: &fluree_db_indexer::run_index::global_dict::SharedDictAllocator,
    ) -> Vec<String> {
        let dict = datatype_alloc.to_predicate_dict();
        let reserved = fluree_db_core::DatatypeDictId::RESERVED_COUNT as u32;
        (reserved..dict.len())
            .filter_map(|id| dict.resolve(id).map(String::from))
            .collect()
    }

    async fn spawn_sorted_commit_write(
        sort_write_handles: &mut Vec<
            tokio::task::JoinHandle<
                std::io::Result<fluree_db_indexer::run_index::SortedCommitInfo>,
            >,
        >,
        sort_write_semaphore: &Arc<tokio::sync::Semaphore>,
        vocab_dir: &Path,
        spool_dir: &Path,
        rdf_type_p_id: u32,
        datatype_alloc: &Arc<fluree_db_indexer::run_index::global_dict::SharedDictAllocator>,
        sr: fluree_db_transact::import_sink::BufferedSpoolResult,
    ) {
        let ci = sr.chunk_idx;
        let record_count = sr.records.len();
        let vd = vocab_dir.to_path_buf();
        let sd = spool_dir.to_path_buf();
        let datatype_alloc = Arc::clone(datatype_alloc);
        let permit_wait_start = std::time::Instant::now();
        let permit = sort_write_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        tracing::info!(
            chunk = ci,
            record_count,
            wait_elapsed_ms = permit_wait_start.elapsed().as_millis(),
            "acquired sort/write permit"
        );
        let parent_span = tracing::Span::current();
        sort_write_handles.push(tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            let task_start = std::time::Instant::now();
            tracing::info!(chunk = ci, record_count, "starting sorted-commit write");
            let custom_datatype_iris = current_custom_datatype_iris(&datatype_alloc);
            let otype_registry = fluree_db_core::OTypeRegistry::new(&custom_datatype_iris);
            let r = fluree_db_indexer::run_index::spool::sort_remap_and_write_sorted_commit(
                sr.records,
                sr.subjects,
                sr.strings,
                &vd.join(format!("chunk_{ci:05}.subjects.voc")),
                &vd.join(format!("chunk_{ci:05}.strings.voc")),
                &sd.join(format!("commit_{ci:05}.fsv2")),
                ci,
                Some((
                    &sr.languages,
                    &vd.join(format!("chunk_{ci:05}.languages.voc")),
                )),
                Some(fluree_db_indexer::run_index::TypesMapConfig {
                    rdf_type_p_id,
                    output_dir: &sd,
                }),
                &otype_registry,
            );
            tracing::info!(
                chunk = ci,
                record_count,
                elapsed_ms = task_start.elapsed().as_millis(),
                ok = r.is_ok(),
                "finished sorted-commit write"
            );
            drop(permit);
            r
        }));
    }

    /// Shared immutable environment for the serial commit pipeline.
    struct CommitPipelineEnv<'a, S> {
        estimated_total: usize,
        run_start: Instant,
        storage: &'a S,
        nameservice: &'a dyn crate::NameServicePublisher,
        alias: &'a str,
        config: &'a ImportConfig,
        sort_write_semaphore: &'a Arc<tokio::sync::Semaphore>,
        vocab_dir: &'a Path,
        spool_dir: &'a Path,
        datatype_alloc: &'a Arc<fluree_db_indexer::run_index::global_dict::SharedDictAllocator>,
        rdf_type_p_id: u32,
        import_time_epoch_ms: Option<i64>,
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_parsed_chunks_in_order<S>(
        result_rx: std::sync::mpsc::Receiver<std::result::Result<(usize, ParsedChunk), String>>,
        env: &CommitPipelineEnv<'_, S>,
        state: &mut ImportState,
        published_codes: &mut rustc_hash::FxHashSet<u16>,
        compute_ns_delta: impl Fn(
            &rustc_hash::FxHashSet<u16>,
            &mut rustc_hash::FxHashSet<u16>,
        ) -> std::collections::HashMap<u16, String>,
        sort_write_handles: &mut Vec<
            tokio::task::JoinHandle<
                std::io::Result<fluree_db_indexer::run_index::SortedCommitInfo>,
            >,
        >,
        total_commit_size: &mut u64,
        commit_metas: &mut Vec<CommitMeta>,
    ) -> std::result::Result<usize, ImportError>
    where
        S: Storage,
    {
        // Serial commit loop: receive parsed chunks, reorder, finalize in order.
        // Parsed chunks arrive out of order from parallel workers.
        let mut next_expected: usize = 0;
        let mut pending: std::collections::BTreeMap<usize, ParsedChunk> =
            std::collections::BTreeMap::new();

        for recv_result in &result_rx {
            let (idx, parsed) = recv_result.map_err(ImportError::Transact)?;
            pending.insert(idx, parsed);

            while let Some(parsed) = pending.remove(&next_expected) {
                let ns_delta = compute_ns_delta(&parsed.new_codes, published_codes);

                // Capture previous commit hex BEFORE finalize advances state.
                let previous_commit_hex = commit_metas.last().map(|m| m.commit_hash_hex.clone());

                let result = finalize_parsed_chunk(state, parsed, ns_delta, env.storage, env.alias)
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?;

                // Collect txn-meta for this commit (no I/O, just captures data already in scope).
                commit_metas.push(CommitMeta {
                    commit_hash_hex: result.commit_id.digest_hex(),
                    t: result.t,
                    blob_bytes: result.blob_bytes,
                    flake_count: result.flake_count,
                    time_epoch_ms: env.import_time_epoch_ms,
                    previous_commit_hex,
                });

                if let Some(sr) = result.spool_result {
                    spawn_sorted_commit_write(
                        sort_write_handles,
                        env.sort_write_semaphore,
                        env.vocab_dir,
                        env.spool_dir,
                        env.rdf_type_p_id,
                        env.datatype_alloc,
                        sr,
                    )
                    .await;
                }

                *total_commit_size += result.blob_bytes as u64;

                let total_elapsed = env.run_start.elapsed().as_secs_f64();
                tracing::info!(
                    chunk = next_expected + 1,
                    total = env.estimated_total,
                    t = result.t,
                    flakes = result.flake_count,
                    cumulative_flakes = state.cumulative_flakes,
                    flakes_per_sec = format!(
                        "{:.2}M",
                        state.cumulative_flakes as f64 / total_elapsed / 1_000_000.0
                    ),
                    "chunk committed"
                );
                env.config.emit_progress(ImportPhase::Committing {
                    chunk: next_expected + 1,
                    total: env.estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: total_elapsed,
                });

                // Periodic nameservice checkpoint
                if env.config.publish_every > 0
                    && (next_expected + 1).is_multiple_of(env.config.publish_every)
                {
                    env.nameservice
                        .publish_commit(env.alias, result.t, &result.commit_id)
                        .await
                        .map_err(|e| ImportError::Storage(e.to_string()))?;
                    tracing::info!(
                        t = result.t,
                        chunk = next_expected + 1,
                        "published nameservice checkpoint"
                    );
                }

                next_expected += 1;
            }
        }

        Ok(next_expected)
    }

    let is_streaming = chunk_source.is_streaming();
    let is_remote = chunk_source.is_remote();
    // Remote takes the parallel TTL hot path only when every object is `.ttl`.
    // Otherwise it falls into a serial remote arm that handles `.trig` /
    // `.jsonld` per-chunk, mirroring the local serial fallback.
    let remote_all_ttl = match &**chunk_source {
        ChunkSource::Remote(producer) => producer.all_ttl(),
        _ => false,
    };
    let is_remote_parallel = is_remote && remote_all_ttl;
    let is_remote_serial = is_remote && !remote_all_ttl;
    let is_channel_fed = is_streaming || is_remote_parallel;
    let estimated_total = chunk_source.estimated_len();
    let compress = config.compress_commits;
    let num_threads = config.parse_threads;
    let mut state = ImportState::new();
    let run_start = Instant::now();

    // ---- Inflight permit channel (memory budget enforcement) ----
    // For Files mode: limits the number of chunk texts materialized in memory.
    // For Streaming/Remote modes: backpressure is handled by the bounded channel
    // in the producer (StreamingTurtleReader / remote bridge), so permits are not needed.
    let (permit_tx, permit_rx) = if !is_channel_fed {
        let max_inflight = config.effective_max_inflight();
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(max_inflight);
        for _ in 0..max_inflight {
            tx.send(()).unwrap();
        }
        (
            Some(tx),
            Some(std::sync::Arc::new(std::sync::Mutex::new(rx))),
        )
    } else {
        (None, None)
    };

    // ---- Pipeline infrastructure ----
    std::fs::create_dir_all(run_dir)?;

    // Spool directory for chunk-local spool files (Tier 2 pipeline).
    let spool_dir = run_dir.join("spool");
    std::fs::create_dir_all(&spool_dir)?;

    // Background sort/write pipeline: V2 sorted-commit files (.fsv2) + vocab files (.voc)
    // are produced asynchronously after commit-v2 finalization. The serial commit loop
    // hands off records+dicts to spawn_blocking tasks, then continues immediately.
    // Semaphore bounds memory: each inflight job holds a Vec<RunRecord>.
    // Scale permits inversely with chunk size — smaller chunks need less memory per job,
    // so we can safely run more concurrently without RSS explosion.
    // Target: ~3GB total inflight sort/write memory.
    let chunk_mb = config.effective_chunk_size_mb();
    let sort_write_permits = match chunk_mb {
        0..=256 => 8,
        257..=512 => 6,
        513..=768 => 4,
        _ => 3,
    };
    tracing::info!(
        sort_write_permits,
        chunk_mb,
        "background sort/write semaphore"
    );
    let sort_write_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(sort_write_permits));
    let mut sort_write_handles: Vec<tokio::task::JoinHandle<std::io::Result<SortedCommitInfo>>> =
        Vec::new();
    let vocab_dir = run_dir.join("vocab");
    std::fs::create_dir_all(&vocab_dir)?;

    // Track commit metadata across all chunks (previously tracked by resolver).
    let mut total_commit_size: u64 = 0;
    let mut commit_metas: Vec<CommitMeta> = Vec::new();
    // Parse import timestamp once (it's constant for the whole import).
    let import_time_epoch_ms: Option<i64> =
        chrono::DateTime::parse_from_rfc3339(&state.import_time)
            .ok()
            .map(|dt| dt.timestamp_millis());
    // In fresh import, all ops are assertions (no retractions).
    // total_asserts = state.cumulative_flakes at the end.

    // ---- Phase 2a: Create shared allocators, then parse all chunks in parallel ----
    //
    // Create shared allocators before any parsing so all chunks can produce
    // spool output concurrently. For streaming, register prelude prefixes first
    // so the shared allocator knows about the data's namespace IRIs.
    use fluree_db_transact::SharedNamespaceAllocator;
    use fluree_vocab::namespaces::OVERFLOW;
    use rustc_hash::FxHashSet;

    let streaming_prelude = if is_streaming {
        match &**chunk_source {
            ChunkSource::Streaming(reader) => Some(reader.prelude().clone()),
            _ => None,
        }
    } else {
        None
    };

    // For streaming: pre-register prelude prefixes so the shared allocator
    // includes them (they won't appear as @prefix directives in chunk data).
    if let Some(ref prelude) = streaming_prelude {
        for (short, ns_iri) in &prelude.prefixes {
            state.ns_registry.get_or_allocate(ns_iri);
            if !short.is_empty() {
                state.prefix_map.insert(ns_iri.clone(), short.clone());
            }
        }
    }

    let shared_alloc = Arc::new(SharedNamespaceAllocator::from_registry(&state.ns_registry));
    let mut published_codes: FxHashSet<u16> = state.ns_registry.all_codes();

    // Create shared allocators for the spool pipeline (Tier 2).
    // These are the same seed values as GlobalDicts::new(), but wrapped in
    // thread-safe SharedDictAllocator so parse workers can allocate IDs
    // concurrently without the resolver bottleneck.
    use fluree_db_indexer::run_index::global_dict::SharedDictAllocator;
    use fluree_db_indexer::run_index::shared_pool::{SharedNumBigPool, SharedVectorArenaPool};
    use fluree_db_transact::import_sink::SpoolConfig;

    let spool_config = Arc::new(SpoolConfig {
        predicate_alloc: Arc::new(SharedDictAllocator::new_predicate()),
        datatype_alloc: Arc::new(SharedDictAllocator::new_datatype()),
        graph_alloc: Arc::new(SharedDictAllocator::new_graph(alias)),
        numbig_pool: Arc::new(SharedNumBigPool::new()),
        vector_pool: Arc::new(SharedVectorArenaPool::new()),
        ns_alloc: Arc::clone(&shared_alloc),
    });

    // Pre-insert rdf:type so we know the predicate ID before Phase A begins.
    // This allows sort_remap_and_write_sorted_commit to extract rdf:type edges
    // into a types-map sidecar for building the subject→class bitset table.
    let rdf_type_p_id = spool_config
        .predicate_alloc
        .get_or_insert(fluree_vocab::rdf::TYPE);

    // Pre-insert txn-meta predicates so they get stable IDs in predicates.json
    // and are included in p_width calculation. These match the predicates used
    // by `CommitResolver::emit_txn_meta` in the non-import indexing path.
    {
        use fluree_vocab::{db, fluree};
        for &(prefix, name) in &[
            (fluree::DB, db::ADDRESS),
            (fluree::DB, db::TIME),
            (fluree::DB, db::T),
            (fluree::DB, db::SIZE),
            (fluree::DB, db::ASSERTS),
            (fluree::DB, db::RETRACTS),
            (fluree::DB, db::PREVIOUS),
        ] {
            spool_config
                .predicate_alloc
                .get_or_insert_parts(prefix, name);
        }
    }

    // Helper: compute ns_delta for a parsed chunk and advance published_codes.
    let compute_ns_delta = |new_codes: &FxHashSet<u16>,
                            published: &mut FxHashSet<u16>|
     -> std::collections::HashMap<u16, String> {
        let unpublished: FxHashSet<u16> = new_codes
            .iter()
            .copied()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();
        let delta = if unpublished.is_empty() {
            std::collections::HashMap::new()
        } else {
            shared_alloc.lookup_codes(&unpublished)
        };
        published.extend(&unpublished);
        delta
    };

    /// Shared context for parsing TTL chunks.
    struct ParseChunkContext<'a> {
        shared_alloc: &'a Arc<SharedNamespaceAllocator>,
        prelude: Option<&'a fluree_graph_turtle::splitter::TurtlePrelude>,
        ledger: &'a str,
        compress: bool,
        spool_dir: &'a Path,
        spool_config: &'a Arc<SpoolConfig>,
    }

    fn parse_ttl_chunk(
        ttl: &str,
        ctx: &ParseChunkContext<'_>,
        t: i64,
        idx: usize,
    ) -> std::result::Result<ParsedChunk, String> {
        let parsed = if let Some(prelude) = ctx.prelude {
            parse_chunk_with_prelude(
                ttl,
                ctx.shared_alloc,
                prelude,
                t,
                ctx.ledger,
                ctx.compress,
                Some(ctx.spool_dir),
                Some(ctx.spool_config),
                idx,
            )
        } else {
            parse_chunk(
                ttl,
                ctx.shared_alloc,
                t,
                ctx.ledger,
                ctx.compress,
                Some(ctx.spool_dir),
                Some(ctx.spool_config),
                idx,
            )
        };
        parsed.map_err(|e| e.to_string())
    }

    // ---- Parse all chunks in parallel, commit serially in order ----
    //
    // All chunks (including chunk 0) go through the parallel pipeline.
    // SharedNamespaceAllocator + SharedDictAllocator are created above,
    // so no chunk needs to "establish namespaces" before others can start.

    let commit_env = CommitPipelineEnv {
        estimated_total,
        run_start,
        storage,
        nameservice,
        alias,
        config,
        sort_write_semaphore: &sort_write_semaphore,
        vocab_dir: &vocab_dir,
        spool_dir: &spool_dir,
        datatype_alloc: &spool_config.datatype_alloc,
        rdf_type_p_id,
        import_time_epoch_ms,
    };

    if is_streaming {
        // Streaming path: workers receive chunk data from the reader thread's
        // channel. No worker I/O — the reader is the only entity reading from disk.
        // This avoids double I/O that would kill throughput on external drives.
        let ledger = alias.to_string();

        let (reader_rx, prelude, ns_preflight_cell) = match &**chunk_source {
            ChunkSource::Streaming(reader) => (
                reader.shared_receiver(),
                reader.prelude().clone(),
                reader.namespace_preflight_cell(),
            ),
            _ => unreachable!(),
        };

        // Forward raw chunk payloads from the reader thread to parse workers.
        // This lets the main thread apply one-time policy decisions (e.g. namespace fallback)
        // before any chunk is parsed, without adding an extra I/O pass.
        //
        // This is a handoff buffer, not a read-ahead buffer — the reader
        // channel (ch1) is the memory knob. Capacity 2 here is enough to
        // keep workers fed at startup (they drain instantly) and in
        // steady-state (reader is always faster than parsing). Keeping this
        // tight avoids duplicating the raw-chunk buffering already in ch1.
        let (work_tx, work_rx) =
            std::sync::mpsc::sync_channel::<fluree_graph_turtle::splitter::ChunkPayload>(2);
        let work_rx = Arc::new(std::sync::Mutex::new(work_rx));

        // One slot per worker is sufficient: the serial commit loop drains
        // in-order via a BTreeMap reorder buffer, so out-of-order arrivals
        // don't need extra channel depth — they just wait in the map.
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
            std::result::Result<(usize, ParsedChunk), String>,
        >(num_threads);

        let mut parse_handles = Vec::with_capacity(num_threads);
        for thread_idx in 0..num_threads {
            let work_rx = Arc::clone(&work_rx);
            let result_tx = result_tx.clone();
            let shared_alloc = Arc::clone(&shared_alloc);
            let ledger = ledger.clone();
            let prelude = prelude.clone();
            let spool_dir = spool_dir.clone();
            let spool_config = Arc::clone(&spool_config);

            let handle = std::thread::Builder::new()
                .name(format!("ttl-parser-{thread_idx}"))
                .spawn(move || {
                    let ctx = ParseChunkContext {
                        shared_alloc: &shared_alloc,
                        prelude: Some(&prelude),
                        ledger: &ledger,
                        compress,
                        spool_dir: &spool_dir,
                        spool_config: &spool_config,
                    };
                    loop {
                        // Pull next chunk data from the main-thread forwarder (no I/O here).
                        let (idx, raw_bytes) = match work_rx.lock().unwrap().recv() {
                            Ok(payload) => payload,
                            Err(_) => break, // Reader thread finished.
                        };

                        // Convert raw bytes to String (CPU-only; no copy on success).
                        let ttl = match String::from_utf8(raw_bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                let _ =
                                    result_tx.send(Err(format!("chunk {idx} invalid UTF-8: {e}")));
                                break;
                            }
                        };

                        let t = (idx + 1) as i64;
                        tracing::debug!(
                            chunk_idx = idx,
                            chunk_text_len = ttl.len(),
                            starts_with = &ttl[..ttl.len().min(200)],
                            "about to parse chunk"
                        );
                        match parse_ttl_chunk(&ttl, &ctx, t, idx) {
                            Ok(parsed) => {
                                if result_tx.send(Ok((idx, parsed))).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ =
                                    result_tx.send(Err(format!("parse chunk {idx} failed: {e}")));
                                break;
                            }
                        }
                    }
                })
                .map_err(|e| ImportError::Transact(format!("spawn parser: {e}")))?;

            parse_handles.push(handle);
        }
        drop(result_tx); // main thread's copy

        // Forwarder thread: receive from reader thread, apply any one-time namespace
        // fallback mode before chunk 0 is parsed, then dispatch to workers.
        let forward_shared_alloc = Arc::clone(&shared_alloc);
        let forward_handle = std::thread::Builder::new()
            .name("ttl-forwarder".into())
            .spawn(move || {
                use fluree_db_core::NsSplitMode;
                let mut ns_mode_set = false;
                loop {
                    let payload = match reader_rx.lock().unwrap().recv() {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    if !ns_mode_set && payload.0 == 0 {
                        if let Some(pre) = ns_preflight_cell.get() {
                            if pre.exceeded_budget {
                                forward_shared_alloc.set_split_mode(NsSplitMode::HostPlusN(1));
                                ns_mode_set = true;
                                tracing::info!(
                                    distinct_prefixes = pre.distinct_prefixes,
                                    http_host_prefixes = pre.http_host_prefixes,
                                    http_host_seg1_prefixes = pre.http_host_seg1_prefixes,
                                    "namespace preflight exceeded budget; enabling HostPlusN(1) split mode"
                                );
                            }
                        } else {
                            tracing::warn!(
                                "chunk 0 received but namespace preflight not available; using default namespace fallback"
                            );
                        }
                    }
                    if work_tx.send(payload).is_err() {
                        break;
                    }
                }
                // Drop sender so workers exit when queue drained.
                drop(work_tx);
            })
            .map_err(|e| ImportError::Transact(format!("spawn forwarder: {e}")))?;

        let next_expected = commit_parsed_chunks_in_order(
            result_rx,
            &commit_env,
            &mut state,
            &mut published_codes,
            compute_ns_delta,
            &mut sort_write_handles,
            &mut total_commit_size,
            &mut commit_metas,
        )
        .await?;

        // Wait for parse threads.
        for handle in parse_handles {
            handle.join().expect("parse thread panicked");
        }
        forward_handle.join().expect("forwarder thread panicked");

        // Note: The reader thread finishes when all chunks are consumed (channel
        // drained). Any reader errors would have manifested as channel closure,
        // which the parse workers handle by breaking their loop.
        tracing::info!(
            committed_chunks = next_expected,
            "streaming import phase complete"
        );
    } else if is_remote_parallel {
        // Remote parallel path (all-`.ttl` remote): workers receive whole-object
        // payloads from a producer task (async tokio fetch) bridged into a sync
        // channel by a small forwarder thread. Workers parse with auto-extracted
        // per-chunk prelude (each remote object is a self-contained file with
        // its own header) — same parser semantics as the local `Files` path,
        // but bytes arrive via channel instead of disk read.
        //
        // The remote arm always uses at least one parser worker — zero workers
        // would mean the bridge thread blocks forever sending into an unread
        // channel.
        let num_threads = num_threads.max(1);
        let ledger = alias.to_string();

        let (remote_rx, error_rx, bridge_handle) = match &**chunk_source {
            ChunkSource::Remote(producer) => {
                let error_rx = producer.error_rx.lock().unwrap().take().ok_or_else(|| {
                    ImportError::Transact(
                        "remote producer error_rx already taken (import re-entered?)".into(),
                    )
                })?;
                let bridge = producer.bridge_handle.lock().unwrap().take();
                (Arc::clone(&producer.rx), error_rx, bridge)
            }
            _ => unreachable!("is_remote guard"),
        };

        // One slot per worker is sufficient (same logic as streaming arm).
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
            std::result::Result<(usize, ParsedChunk), String>,
        >(num_threads);

        let mut parse_handles = Vec::with_capacity(num_threads);
        for thread_idx in 0..num_threads {
            let work_rx = Arc::clone(&remote_rx);
            let result_tx = result_tx.clone();
            let shared_alloc = Arc::clone(&shared_alloc);
            let ledger = ledger.clone();
            let spool_dir = spool_dir.clone();
            let spool_config = Arc::clone(&spool_config);

            let handle = std::thread::Builder::new()
                .name(format!("ttl-remote-parser-{thread_idx}"))
                .spawn(move || {
                    let ctx = ParseChunkContext {
                        shared_alloc: &shared_alloc,
                        // Remote objects are whole files with embedded preludes —
                        // parse_chunk extracts prelude per-chunk.
                        prelude: None,
                        ledger: &ledger,
                        compress,
                        spool_dir: &spool_dir,
                        spool_config: &spool_config,
                    };
                    loop {
                        let (idx, raw_bytes) = match work_rx.lock().unwrap().recv() {
                            Ok(payload) => payload,
                            Err(_) => break, // Bridge dropped — channel closed.
                        };

                        let ttl = match String::from_utf8(raw_bytes) {
                            Ok(s) => s,
                            Err(e) => {
                                let _ =
                                    result_tx.send(Err(format!("chunk {idx} invalid UTF-8: {e}")));
                                break;
                            }
                        };

                        let t = (idx + 1) as i64;
                        match parse_ttl_chunk(&ttl, &ctx, t, idx) {
                            Ok(parsed) => {
                                if result_tx.send(Ok((idx, parsed))).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ =
                                    result_tx.send(Err(format!("parse chunk {idx} failed: {e}")));
                                break;
                            }
                        }
                    }
                })
                .map_err(|e| ImportError::Transact(format!("spawn parser: {e}")))?;

            parse_handles.push(handle);
        }
        drop(result_tx); // main thread's copy

        let next_expected = commit_parsed_chunks_in_order(
            result_rx,
            &commit_env,
            &mut state,
            &mut published_codes,
            compute_ns_delta,
            &mut sort_write_handles,
            &mut total_commit_size,
            &mut commit_metas,
        )
        .await?;

        for handle in parse_handles {
            handle.join().expect("parse thread panicked");
        }
        if let Some(bridge) = bridge_handle {
            bridge.join().expect("remote bridge thread panicked");
        }

        // CRITICAL: distinguish clean EOF from producer failure.
        // Channel closing alone is not sufficient — a producer crash also
        // closes the channel, but means partial import.
        match error_rx.await {
            Ok(None) => {
                // Producer reported success.
            }
            Ok(Some(err)) => return Err(err),
            Err(_) => {
                return Err(ImportError::Storage(
                    "remote producer task dropped without signaling completion".into(),
                ));
            }
        }

        tracing::info!(
            committed_chunks = next_expected,
            "remote import phase complete"
        );
    } else if is_remote_serial {
        // Remote serial path: producer fetches each object whole; we drain
        // the channel one chunk at a time and dispatch to the format-specific
        // parser (`import_trig_commit` for `.trig`, `parse_jsonld_chunk` for
        // `.jsonld`, `parse_chunk` otherwise). Mirrors the local serial
        // fallback below — the only difference is `recv()` vs `read_chunk(i)`.
        //
        // This path is used whenever the remote source contains any `.trig`
        // or `.jsonld` objects. Performance is single-threaded by design
        // (TriG/JSON-LD parsers do not parallelize across chunks today).
        let (remote_rx, error_rx, bridge_handle) = match &**chunk_source {
            ChunkSource::Remote(producer) => {
                let error_rx = producer.error_rx.lock().unwrap().take().ok_or_else(|| {
                    ImportError::Transact(
                        "remote producer error_rx already taken (import re-entered?)".into(),
                    )
                })?;
                let bridge = producer.bridge_handle.lock().unwrap().take();
                (Arc::clone(&producer.rx), error_rx, bridge)
            }
            _ => unreachable!("is_remote_serial guard"),
        };

        let mut next_expected: i64 = 0;
        loop {
            let (idx, raw_bytes) = match remote_rx.lock().unwrap().recv() {
                Ok(payload) => payload,
                Err(_) => break, // Channel closed — bridge thread exited.
            };

            let content = String::from_utf8(raw_bytes)
                .map_err(|e| ImportError::Transact(format!("chunk {idx} invalid UTF-8: {e}")))?;
            let t = (idx + 1) as i64;

            let result = if chunk_source.is_trig(idx) {
                let r = import_trig_commit(
                    &mut state,
                    &content,
                    storage,
                    alias,
                    compress,
                    Some(&spool_dir),
                    Some(&spool_config),
                    idx,
                )
                .await
                .map_err(|e| ImportError::Transact(e.to_string()))?;
                shared_alloc
                    .sync_from_registry(&state.ns_registry)
                    .map_err(|e| ImportError::Transact(format!("namespace sync conflict: {e}")))?;
                published_codes.extend(state.ns_registry.all_codes());
                r
            } else {
                let parsed = if chunk_source.is_jsonld(idx) {
                    parse_jsonld_chunk(
                        &content,
                        &shared_alloc,
                        t,
                        alias,
                        compress,
                        Some(&spool_dir),
                        Some(&spool_config),
                        idx,
                    )
                } else {
                    parse_chunk(
                        &content,
                        &shared_alloc,
                        t,
                        alias,
                        compress,
                        Some(&spool_dir),
                        Some(&spool_config),
                        idx,
                    )
                }
                .map_err(|e| ImportError::Transact(e.to_string()))?;

                let ns_delta = compute_ns_delta(&parsed.new_codes, &mut published_codes);
                finalize_parsed_chunk(&mut state, parsed, ns_delta, storage, alias)
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?
            };

            // Collect txn-meta for this commit.
            {
                let previous_commit_hex = commit_metas.last().map(|m| m.commit_hash_hex.clone());
                commit_metas.push(CommitMeta {
                    commit_hash_hex: result.commit_id.digest_hex(),
                    t: result.t,
                    blob_bytes: result.blob_bytes,
                    flake_count: result.flake_count,
                    time_epoch_ms: import_time_epoch_ms,
                    previous_commit_hex,
                });
            }

            if let Some(sr) = result.spool_result {
                spawn_sorted_commit_write(
                    &mut sort_write_handles,
                    &sort_write_semaphore,
                    &vocab_dir,
                    &spool_dir,
                    rdf_type_p_id,
                    &spool_config.datatype_alloc,
                    sr,
                )
                .await;
            }
            total_commit_size += result.blob_bytes as u64;
            next_expected = result.t;

            config.emit_progress(ImportPhase::Committing {
                chunk: idx + 1,
                total: estimated_total,
                cumulative_flakes: state.cumulative_flakes,
                elapsed_secs: run_start.elapsed().as_secs_f64(),
            });
            if config.publish_every > 0 && (idx + 1).is_multiple_of(config.publish_every) {
                nameservice
                    .publish_commit(alias, result.t, &result.commit_id)
                    .await
                    .map_err(|e| ImportError::Storage(e.to_string()))?;
            }
        }

        if let Some(bridge) = bridge_handle {
            bridge.join().expect("remote bridge thread panicked");
        }

        // Producer error vs clean EOF — same protocol as the parallel arm.
        match error_rx.await {
            Ok(None) => {}
            Ok(Some(err)) => return Err(err),
            Err(_) => {
                return Err(ImportError::Storage(
                    "remote producer task dropped without signaling completion".into(),
                ));
            }
        }

        tracing::info!(
            committed_chunks = next_expected,
            "remote serial import phase complete"
        );
    } else {
        // File-based path: index-based access to chunk files.
        let has_trig = (0..estimated_total).any(|i| chunk_source.is_trig(i));
        let has_jsonld = chunk_source.has_jsonld();
        if estimated_total > 0 && num_threads > 0 && !has_trig && !has_jsonld {
            let ledger = alias.to_string();

            let next_chunk = Arc::new(AtomicUsize::new(0));
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<
                std::result::Result<(usize, ParsedChunk), String>,
            >(num_threads);

            let permit_rx = permit_rx.expect("permit_rx must exist for file-based path");
            let permit_tx = permit_tx.expect("permit_tx must exist for file-based path");

            // Spawn parse worker threads
            let mut parse_handles = Vec::with_capacity(num_threads);
            for thread_idx in 0..num_threads {
                let next_chunk = Arc::clone(&next_chunk);
                let result_tx = result_tx.clone();
                let shared_alloc = Arc::clone(&shared_alloc);
                let ledger = ledger.clone();
                let chunk_source = Arc::clone(chunk_source);
                let permit_rx_ref = Arc::clone(&permit_rx);
                let permit_tx_ref = permit_tx.clone();
                let total = estimated_total;
                let spool_dir = spool_dir.clone();
                let spool_config = Arc::clone(&spool_config);

                let handle = std::thread::Builder::new()
                    .name(format!("ttl-parser-{thread_idx}"))
                    .spawn(move || {
                        let ctx = ParseChunkContext {
                            shared_alloc: &shared_alloc,
                            prelude: None,
                            ledger: &ledger,
                            compress,
                            spool_dir: &spool_dir,
                            spool_config: &spool_config,
                        };
                        loop {
                            let idx = next_chunk.fetch_add(1, Ordering::Relaxed);
                            if idx >= total {
                                break;
                            }

                            // Acquire inflight permit (blocks if at max_inflight).
                            let permit_result = permit_rx_ref.lock().unwrap().recv();
                            if permit_result.is_err() {
                                break;
                            }

                            let ttl = match chunk_source.read_chunk(idx) {
                                Ok(s) => s,
                                Err(e) => {
                                    let _ = permit_tx_ref.send(()); // release permit
                                    let _ = result_tx
                                        .send(Err(format!("failed to read chunk {idx}: {e}")));
                                    break;
                                }
                            };

                            let t = (idx + 1) as i64;
                            match parse_ttl_chunk(&ttl, &ctx, t, idx) {
                                Ok(parsed) => {
                                    let _ = permit_tx_ref.send(());
                                    if result_tx.send(Ok((idx, parsed))).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = permit_tx_ref.send(());
                                    let _ = result_tx
                                        .send(Err(format!("parse chunk {idx} failed: {e}")));
                                    break;
                                }
                            }
                        }
                    })
                    .map_err(|e| ImportError::Transact(format!("spawn parser: {e}")))?;

                parse_handles.push(handle);
            }
            drop(result_tx); // main thread's copy

            let _committed_chunks = commit_parsed_chunks_in_order(
                result_rx,
                &commit_env,
                &mut state,
                &mut published_codes,
                compute_ns_delta,
                &mut sort_write_handles,
                &mut total_commit_size,
                &mut commit_metas,
            )
            .await?;

            // Wait for parse threads
            for handle in parse_handles {
                handle.join().expect("parse thread panicked");
            }
        } else if estimated_total > 0 {
            // Serial fallback (0 threads, TriG, or JSON-LD files present).
            //
            // Uses parse_chunk / parse_jsonld_chunk + finalize_parsed_chunk
            // (same as the parallel path) so that namespace codes are allocated
            // in shared_alloc — the spool pipeline reads prefixes from
            // shared_alloc, so they must be in sync during the parse pass.
            for i in 0..estimated_total {
                let content = chunk_source.read_chunk(i)?;
                let t = (i + 1) as i64;

                let result = if chunk_source.is_trig(i) {
                    // TriG uses its own commit function (named graph handling).
                    // It allocates codes in state.ns_registry; sync them to
                    // shared_alloc afterward for subsequent chunks' spool writes.
                    let r = import_trig_commit(
                        &mut state,
                        &content,
                        storage,
                        alias,
                        compress,
                        Some(&spool_dir),
                        Some(&spool_config),
                        i,
                    )
                    .await
                    .map_err(|e| ImportError::Transact(e.to_string()))?;
                    shared_alloc
                        .sync_from_registry(&state.ns_registry)
                        .map_err(|e| {
                            ImportError::Transact(format!("namespace sync conflict: {e}"))
                        })?;
                    published_codes.extend(state.ns_registry.all_codes());
                    r
                } else {
                    // TTL and JSON-LD: parse via shared allocator, then finalize.
                    let parsed = if chunk_source.is_jsonld(i) {
                        parse_jsonld_chunk(
                            &content,
                            &shared_alloc,
                            t,
                            alias,
                            compress,
                            Some(&spool_dir),
                            Some(&spool_config),
                            i,
                        )
                    } else {
                        parse_chunk(
                            &content,
                            &shared_alloc,
                            t,
                            alias,
                            compress,
                            Some(&spool_dir),
                            Some(&spool_config),
                            i,
                        )
                    }
                    .map_err(|e| ImportError::Transact(e.to_string()))?;

                    let ns_delta = compute_ns_delta(&parsed.new_codes, &mut published_codes);

                    finalize_parsed_chunk(&mut state, parsed, ns_delta, storage, alias)
                        .await
                        .map_err(|e| ImportError::Transact(e.to_string()))?
                };

                // Collect txn-meta for this commit.
                {
                    let previous_commit_hex =
                        commit_metas.last().map(|m| m.commit_hash_hex.clone());
                    commit_metas.push(CommitMeta {
                        commit_hash_hex: result.commit_id.digest_hex(),
                        t: result.t,
                        blob_bytes: result.blob_bytes,
                        flake_count: result.flake_count,
                        time_epoch_ms: import_time_epoch_ms,
                        previous_commit_hex,
                    });
                }

                // Hand off sort + remap + write to a background task.
                if let Some(sr) = result.spool_result {
                    spawn_sorted_commit_write(
                        &mut sort_write_handles,
                        &sort_write_semaphore,
                        &vocab_dir,
                        &spool_dir,
                        rdf_type_p_id,
                        &spool_config.datatype_alloc,
                        sr,
                    )
                    .await;
                }
                total_commit_size += result.blob_bytes as u64;

                config.emit_progress(ImportPhase::Committing {
                    chunk: i + 1,
                    total: estimated_total,
                    cumulative_flakes: state.cumulative_flakes,
                    elapsed_secs: run_start.elapsed().as_secs_f64(),
                });
                if config.publish_every > 0 && (i + 1).is_multiple_of(config.publish_every) {
                    nameservice
                        .publish_commit(alias, result.t, &result.commit_id)
                        .await
                        .map_err(|e| ImportError::Storage(e.to_string()))?;
                }
            }
        }
    }

    // Final commit head publish
    let commit_head_id = state
        .parent
        .clone()
        .ok_or_else(|| ImportError::Storage("no commit head after import".to_string()))?;

    nameservice
        .publish_commit(alias, state.t, &commit_head_id)
        .await
        .map_err(|e| ImportError::Storage(e.to_string()))?;
    tracing::info!(t = state.t, "published final commit head");

    // ---- Spawn txn-meta "meta chunk" build in background ----
    // Build a tiny extra chunk containing commit metadata records (g_id=1).
    // Runs in spawn_blocking concurrently with the sort_write_handles await below,
    // so it adds zero wall-clock time. The meta chunk participates in dict merge
    // and the import index build so `ledger#txn-meta` queries work after import.
    let meta_chunk_handle = if !commit_metas.is_empty() {
        use fluree_vocab::{db, fluree};

        // Resolve predicate/graph IDs while spool_config is still accessible.
        // These were pre-inserted in Phase 2a, so these are pure lookups.
        let p_address = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::ADDRESS);
        let p_time = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::TIME);
        let p_t = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::T);
        let p_size = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::SIZE);
        let p_asserts = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::ASSERTS);
        let p_retracts = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::RETRACTS);
        let p_previous = spool_config
            .predicate_alloc
            .get_or_insert_parts(fluree::DB, db::PREVIOUS);

        // txn-meta is always pre-seeded as dict_id=0 in the graph allocator
        // (via SharedDictAllocator::new_graph), so g_id = dict_id + 1 = 1.
        let g_id: u16 = 1;

        // meta_chunk_idx = number of data chunks (next sequential index).
        let meta_chunk_idx = sort_write_handles.len();
        let vocab_dir = vocab_dir.clone();
        let spool_dir = spool_dir.clone();
        let meta_spool_config = Arc::clone(&spool_config);

        let parent_span = tracing::Span::current();
        Some(tokio::task::spawn_blocking(move || {
            let _guard = parent_span.enter();
            use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
            use fluree_db_binary_index::RunRecord;
            use fluree_db_core::value_id::{ObjKey, ObjKind};
            use fluree_db_core::{DatatypeDictId, SubjectId};
            use fluree_db_indexer::run_index::{
                sort_remap_and_write_sorted_commit, ChunkStringDict, ChunkSubjectDict,
            };
            use fluree_vocab::namespaces;

            let mut meta_subjects = ChunkSubjectDict::new();
            let mut meta_strings = ChunkStringDict::new();
            let mut records: Vec<RunRecord> = Vec::with_capacity(commit_metas.len() * 8);

            for cm in &commit_metas {
                let commit_s = meta_subjects
                    .get_or_insert(namespaces::FLUREE_COMMIT, cm.commit_hash_hex.as_bytes());
                let t = cm.t as u32;

                let mut push =
                    |s_id: u64, p_id: u32, o_kind: ObjKind, o_key: ObjKey, dt: DatatypeDictId| {
                        records.push(RunRecord {
                            g_id,
                            s_id: SubjectId::from_u64(s_id),
                            p_id,
                            dt: dt.as_u16(),
                            o_kind: o_kind.as_u8(),
                            op: 1, // assert
                            o_key: o_key.as_u64(),
                            t,
                            lang_id: 0,
                            i: LIST_INDEX_NONE,
                        });
                    };

                // db:address — commit hash hex as LEX_ID string
                let addr_str_id = meta_strings.get_or_insert(cm.commit_hash_hex.as_bytes());
                push(
                    commit_s,
                    p_address,
                    ObjKind::LEX_ID,
                    ObjKey::encode_u32_id(addr_str_id),
                    DatatypeDictId::STRING,
                );

                // db:time — epoch_ms as LONG (pre-parsed at collection time)
                if let Some(epoch_ms) = cm.time_epoch_ms {
                    push(
                        commit_s,
                        p_time,
                        ObjKind::NUM_INT,
                        ObjKey::encode_i64(epoch_ms),
                        DatatypeDictId::LONG,
                    );
                }

                // db:t — INTEGER
                push(
                    commit_s,
                    p_t,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.t),
                    DatatypeDictId::INTEGER,
                );

                // db:size — LONG (blob bytes)
                push(
                    commit_s,
                    p_size,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.blob_bytes as i64),
                    DatatypeDictId::LONG,
                );

                // db:asserts — INTEGER
                push(
                    commit_s,
                    p_asserts,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(cm.flake_count as i64),
                    DatatypeDictId::INTEGER,
                );

                // db:retracts — INTEGER (always 0 for fresh import)
                push(
                    commit_s,
                    p_retracts,
                    ObjKind::NUM_INT,
                    ObjKey::encode_i64(0),
                    DatatypeDictId::INTEGER,
                );

                // db:previous — REF_ID (only if this commit has a predecessor)
                if let Some(ref prev_hex) = cm.previous_commit_hex {
                    let prev_s =
                        meta_subjects.get_or_insert(namespaces::FLUREE_COMMIT, prev_hex.as_bytes());
                    push(
                        commit_s,
                        p_previous,
                        ObjKind::REF_ID,
                        ObjKey::encode_sid64(prev_s),
                        DatatypeDictId::ID,
                    );
                }
            }

            let meta_records_count = records.len();
            let commit_count = commit_metas.len();

            let subj_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.subjects.voc"));
            let str_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.strings.voc"));
            let commit_path = spool_dir.join(format!("commit_{meta_chunk_idx:05}.fsv2"));

            // Write empty language vocab for uniformity.
            let lang_voc_path = vocab_dir.join(format!("chunk_{meta_chunk_idx:05}.languages.voc"));
            let empty_lang = fluree_db_indexer::run_index::LanguageTagDict::new();
            let lang_bytes =
                fluree_db_indexer::run_index::run_file::serialize_lang_dict(&empty_lang);
            std::fs::write(&lang_voc_path, &lang_bytes)?;

            let meta_custom_datatype_iris =
                current_custom_datatype_iris(&meta_spool_config.datatype_alloc);
            let meta_otype_registry =
                fluree_db_core::OTypeRegistry::new(&meta_custom_datatype_iris);
            let meta_sorted_info = sort_remap_and_write_sorted_commit(
                records,
                meta_subjects,
                meta_strings,
                &subj_voc_path,
                &str_voc_path,
                &commit_path,
                meta_chunk_idx,
                None, // no language tags
                None, // no types-map sidecar
                &meta_otype_registry,
            )?;

            tracing::info!(
                meta_chunk_idx,
                commit_count,
                meta_records_count,
                "txn-meta meta chunk built"
            );

            Ok::<_, std::io::Error>(meta_sorted_info)
        }))
    } else {
        None
    };

    // ---- Collect background sort/write results ----
    // Wait for all background sort_remap_and_write_sorted_commit tasks to complete.
    // Their .fsv2 and .voc files must exist before dictionary merge can begin.
    // The meta chunk build (above) runs concurrently with this await loop.
    let mut sorted_commit_infos: Vec<SortedCommitInfo> =
        Vec::with_capacity(sort_write_handles.len() + 1);
    let collect_sorted_start = Instant::now();
    for handle in sort_write_handles {
        let await_start = Instant::now();
        let info = handle
            .await
            .map_err(|e| ImportError::RunGeneration(format!("sort/write task panicked: {e}")))?
            .map_err(ImportError::Io)?;
        tracing::info!(
            chunk = info.chunk_idx,
            record_count = info.record_count,
            await_elapsed_ms = await_start.elapsed().as_millis(),
            "collected sorted-commit write result"
        );
        sorted_commit_infos.push(info);
    }
    tracing::info!(
        chunks = sorted_commit_infos.len(),
        elapsed_ms = collect_sorted_start.elapsed().as_millis(),
        "all data-chunk sorted-commit writes collected"
    );

    // Await meta chunk (already running in background, likely finished by now).
    if let Some(handle) = meta_chunk_handle {
        let meta_sorted_info = handle
            .await
            .map_err(|e| ImportError::RunGeneration(format!("meta chunk task panicked: {e}")))?
            .map_err(ImportError::Io)?;
        sorted_commit_infos.push(meta_sorted_info);
    }

    // ---- Phase 3: Merge chunk dictionaries via k-way sorted merge ----
    //
    // Sorted .voc files were written alongside each commit (one per chunk).
    // Now k-way merge them to produce global forward dicts + mmap'd remap tables.
    // Memory: O(K) where K = number of chunks (no hash maps).
    config.emit_progress(ImportPhase::PreparingIndex {
        stage: "Merging dictionaries",
    });

    // Sort sorted_commit_infos by chunk_idx so downstream remap phase sees them in order.
    sorted_commit_infos.sort_by_key(|si| si.chunk_idx);

    let total_sorted_commit_records: u64 = sorted_commit_infos.iter().map(|s| s.record_count).sum();
    tracing::info!(
        chunks = sorted_commit_infos.len(),
        total_records = total_sorted_commit_records,
        "sorted commit files written"
    );

    tracing::info!(
        chunks = sorted_commit_infos.len(),
        "starting dictionary merge"
    );
    let merge_start = Instant::now();

    // Get namespace codes for IRI reconstruction (subjects.fwd needs full IRIs).
    //
    // Use Arc so Phase B parallel tasks can borrow without cloning the whole map.
    let namespace_codes: Arc<HashMap<u16, String>> =
        Arc::new(shared_alloc.lookup_codes(&published_codes));

    // Diagnostics: namespace explosion investigation.
    //
    // This is intentionally cheap (single pass over the ns_code→prefix map) and
    // only visible when logs are enabled (CLI --verbose).
    {
        let ns_total = namespace_codes.len();
        let mut dblp_pid_deep: usize = 0;
        let mut dblp_pid_shallow: usize = 0;
        let mut doi_deep: usize = 0;
        let mut samples: Vec<(u16, String)> = Vec::new();

        for (&code, prefix) in namespace_codes.iter() {
            if let Some(rest) = prefix.strip_prefix("https://dblp.org/pid/") {
                // Expecting either ".../pid/" (shallow) in coarse mode,
                // or ".../pid/<bucket>/" (deep) in legacy mode.
                if rest.is_empty() {
                    dblp_pid_shallow += 1;
                } else if rest.as_bytes().iter().filter(|&&b| b == b'/').count() >= 1 {
                    dblp_pid_deep += 1;
                    if samples.len() < 8 {
                        samples.push((code, prefix.clone()));
                    }
                } else {
                    dblp_pid_shallow += 1;
                }
            } else if let Some(rest) = prefix.strip_prefix("https://doi.org/") {
                // Deep DOI prefixes like https://doi.org/10.1007/... indicate over-splitting.
                if rest.as_bytes().iter().filter(|&&b| b == b'/').count() >= 2 {
                    doi_deep += 1;
                    if samples.len() < 8 {
                        samples.push((code, prefix.clone()));
                    }
                }
            }
        }

        tracing::info!(
            namespaces_total = ns_total,
            dblp_pid_shallow,
            dblp_pid_deep,
            doi_deep,
            sample_prefixes = ?samples,
            "namespace code table summary"
        );
    }

    let remap_dir = run_dir.join("remap");
    std::fs::create_dir_all(&remap_dir)?;

    // Build vocab file paths + chunk_ids from sorted_commit_infos (deterministic naming).
    let chunk_ids: Vec<usize> = sorted_commit_infos.iter().map(|si| si.chunk_idx).collect();
    let subject_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.subjects.voc", si.chunk_idx)))
        .collect();
    let string_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.strings.voc", si.chunk_idx)))
        .collect();
    let lang_vocab_paths: Vec<std::path::PathBuf> = sorted_commit_infos
        .iter()
        .map(|si| vocab_dir.join(format!("chunk_{:05}.languages.voc", si.chunk_idx)))
        .collect();

    use fluree_db_indexer::run_index::vocab_merge;

    // Phase B can use more CPU: subject, string, and language merges are independent.
    // Run them concurrently to better utilize cores while this phase is otherwise I/O-bound.
    let run_dir_path = run_dir.to_path_buf();
    let remap_dir_path = remap_dir.to_path_buf();

    let subj_vocab_paths_for_task = subject_vocab_paths.clone();
    let chunk_ids_for_subj = chunk_ids.clone();
    let namespace_codes_for_subj = Arc::clone(&namespace_codes);
    let run_dir_for_subj = run_dir_path.clone();
    let remap_dir_for_subj = remap_dir_path.clone();
    let merge_parent_span = tracing::Span::current();
    let subj_span = merge_parent_span.clone();
    let subj_handle = tokio::task::spawn_blocking(move || {
        let _guard = subj_span.enter();
        vocab_merge::merge_subject_vocabs(
            &subj_vocab_paths_for_task,
            &chunk_ids_for_subj,
            &remap_dir_for_subj,
            &run_dir_for_subj,
            namespace_codes_for_subj.as_ref(),
        )
    });

    let str_vocab_paths_for_task = string_vocab_paths.clone();
    let chunk_ids_for_str = chunk_ids.clone();
    let run_dir_for_str = run_dir_path.clone();
    let remap_dir_for_str = remap_dir_path.clone();
    let str_span = merge_parent_span.clone();
    let str_handle = tokio::task::spawn_blocking(move || {
        let _guard = str_span.enter();
        vocab_merge::merge_string_vocabs(
            &str_vocab_paths_for_task,
            &chunk_ids_for_str,
            &remap_dir_for_str,
            &run_dir_for_str,
        )
    });

    let lang_vocab_paths_for_task = lang_vocab_paths.clone();
    let lang_span = merge_parent_span;
    let lang_handle = tokio::task::spawn_blocking(move || {
        let _guard = lang_span.enter();
        fluree_db_indexer::run_index::build_lang_remap_from_vocabs(&lang_vocab_paths_for_task)
    });

    let subj_stats = subj_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("subject vocab merge panicked: {e}")))?
        .map_err(ImportError::Io)?;
    let str_stats = str_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("string vocab merge panicked: {e}")))?
        .map_err(ImportError::Io)?;
    let (unified_lang_dict, lang_remaps) = lang_handle
        .await
        .map_err(|e| ImportError::RunGeneration(format!("language vocab merge panicked: {e}")))?
        .map_err(|e| ImportError::RunGeneration(format!("lang remap: {e}")))?;

    let total_unique_subjects = subj_stats.total_unique;
    let needs_wide = subj_stats.needs_wide;
    let next_string_id = str_stats.total_unique;

    tracing::info!(
        tags = unified_lang_dict.len(),
        chunks = lang_remaps.len(),
        "built unified language dict"
    );

    // Delete .voc files now that all merges (subject, string, language) are complete.
    let _ = std::fs::remove_dir_all(&vocab_dir);

    tracing::info!(
        unique_subjects = total_unique_subjects,
        unique_strings = next_string_id,
        needs_wide,
        elapsed_ms = merge_start.elapsed().as_millis(),
        "dictionary merge + persistence complete"
    );

    // Unwrap the spool_config Arc (parse workers are done, only this thread holds it).
    let spool_config = Arc::try_unwrap(spool_config).unwrap_or_else(|_| {
        panic!("spool_config Arc still shared after all parse workers completed")
    });

    // Write predicates.json (JSON array of IRI strings, indexed by predicate ID).
    {
        let pred_alloc = &spool_config.predicate_alloc;
        let pred_count = pred_alloc.len();
        let preds: Vec<String> = (0..pred_count)
            .map(|id| pred_alloc.resolve(id).unwrap_or_default())
            .collect();
        std::fs::write(
            run_dir.join("predicates.json"),
            serde_json::to_vec(&preds).map_err(|e| {
                ImportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?,
        )?;
    }

    // Write graphs.dict and datatypes.dict (FRD1 binary format).
    {
        use fluree_db_indexer::run_index::dict_io::write_predicate_dict;
        let graphs_dict = spool_config.graph_alloc.to_predicate_dict();
        write_predicate_dict(&run_dir.join("graphs.dict"), &graphs_dict)?;
        let datatypes_dict = spool_config.datatype_alloc.to_predicate_dict();
        write_predicate_dict(&run_dir.join("datatypes.dict"), &datatypes_dict)?;
    }

    // Persist namespaces.json from shared allocator.
    persist_namespaces(namespace_codes.as_ref(), run_dir)?;

    // Persist numbig arenas from shared pool (per-graph subdirectories).
    {
        let numbig_arenas = Arc::try_unwrap(spool_config.numbig_pool)
            .unwrap_or_else(|_| panic!("numbig_pool still shared after import"))
            .into_arenas();
        if !numbig_arenas.is_empty() {
            let mut total_predicates = 0usize;
            let mut total_entries = 0usize;
            for (&g_id, per_pred) in &numbig_arenas {
                if per_pred.is_empty() {
                    continue;
                }
                let nb_dir = run_dir.join(format!("g_{g_id}")).join("numbig");
                std::fs::create_dir_all(&nb_dir)?;
                for (&p_id, arena) in per_pred {
                    fluree_db_binary_index::arena::numbig::write_numbig_arena(
                        &nb_dir.join(format!("p_{p_id}.nba")),
                        arena,
                    )?;
                    total_predicates += 1;
                    total_entries += arena.len();
                }
            }
            if total_predicates > 0 {
                tracing::info!(
                    graphs = numbig_arenas.len(),
                    predicates = total_predicates,
                    total_entries,
                    "numbig arenas persisted"
                );
            }
        }
    }

    // Persist vector arenas from shared pool (per-graph subdirectories).
    {
        let vector_arenas = Arc::try_unwrap(spool_config.vector_pool)
            .unwrap_or_else(|_| panic!("vector_pool still shared after import"))
            .into_arenas();
        if !vector_arenas.is_empty() {
            let mut total_predicates = 0usize;
            let mut total_vectors = 0usize;
            for (&g_id, per_pred) in &vector_arenas {
                if per_pred.is_empty() {
                    continue;
                }
                let vec_dir = run_dir.join(format!("g_{g_id}")).join("vectors");
                std::fs::create_dir_all(&vec_dir)?;
                for (&p_id, arena) in per_pred {
                    if arena.is_empty() {
                        continue;
                    }
                    let shard_paths = fluree_db_binary_index::arena::vector::write_vector_shards(
                        &vec_dir, p_id, arena,
                    )?;
                    let shard_infos: Vec<fluree_db_binary_index::arena::vector::ShardInfo> =
                        shard_paths
                            .iter()
                            .enumerate()
                            .map(|(i, path)| {
                                let cap = fluree_db_binary_index::arena::vector::SHARD_CAPACITY;
                                let start = i as u32 * cap;
                                let count = (arena.len() - start).min(cap);
                                fluree_db_binary_index::arena::vector::ShardInfo {
                                    cas: path.display().to_string(),
                                    count,
                                }
                            })
                            .collect();
                    fluree_db_binary_index::arena::vector::write_vector_manifest(
                        &vec_dir.join(format!("p_{p_id}.vam")),
                        arena,
                        &shard_infos,
                    )?;
                    total_predicates += 1;
                    total_vectors += arena.len() as usize;
                }
            }
            if total_predicates > 0 {
                tracing::info!(
                    graphs = vector_arenas.len(),
                    predicates = total_predicates,
                    total_vectors,
                    "vector arenas persisted"
                );
            }
        }
    }

    tracing::info!(
        subjects = total_unique_subjects,
        predicates = spool_config.predicate_alloc.len(),
        strings = next_string_id,
        "all dictionaries persisted"
    );

    // rdf_type_p_id was pre-inserted before Phase A (used for types-map sidecar +
    // SPOT merge class stats tracking).

    // In a fresh import, all ops are assertions (no retractions).
    let total_asserts = state.cumulative_flakes;
    let total_retracts = 0u64;

    // p_width/dt_width are fixed in the current format.
    // Keep the values as zero sentinels for the ChunkImportResult struct.
    let p_width: u8 = 0;
    let dt_width: u8 = 0;

    Ok(ChunkImportResult {
        final_t: state.t,
        cumulative_flakes: state.cumulative_flakes,
        commit_head_id,
        // Phase B parallel tasks borrow `namespace_codes` via Arc. By this point
        // they are complete, so we should be able to unwrap without cloning.
        namespace_codes: Arc::try_unwrap(namespace_codes).unwrap_or_else(|arc| (*arc).clone()),
        total_commit_size,
        total_asserts,
        total_retracts,
        prefix_map: state.prefix_map,
        sorted_commit_infos,
        unified_lang_dict,
        lang_remaps,
        p_width,
        dt_width,
        rdf_type_p_id,
    })
}

// ============================================================================
// Phase 3-6: Build indexes, upload to CAS, write V4 root, publish
// ============================================================================

struct IndexUploadResult {
    root_id: fluree_db_core::ContentId,
    index_t: i64,
    summary: Option<ImportSummary>,
}

#[allow(clippy::too_many_arguments)]
async fn build_and_upload<S>(
    storage: &S,
    _nameservice: &dyn crate::NameServicePublisher,
    alias: &str,
    input: IndexBuildInput<'_>,
    config: &ImportConfig,
    total_commit_size: u64,
    total_asserts: u64,
    total_retracts: u64,
) -> std::result::Result<IndexUploadResult, ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_binary_index::RunSortOrder;
    use fluree_db_indexer::upload_dicts_from_disk;

    // ---- Import index build + dictionary upload ----
    //
    // Pipeline overlap:
    //   - Start dictionary upload (CoW tree building + CAS writes)
    //   - Remap sorted-commit artifacts to per-order runs
    //   - Merge those runs into FLI3/FBR3 artifacts
    //   - Upload index segments to CAS
    //   - Wait for dictionary upload to finish (may already be done)
    let secondary_orders = RunSortOrder::secondary_orders();

    tracing::info!(
        secondary_orders = ?secondary_orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
        sorted_commits = input.sorted_commit_infos.len(),
        run_dir = %input.run_dir.display(),
        index_dir = %input.index_dir.display(),
        "starting import index build + dictionary upload"
    );
    config.emit_progress(ImportPhase::PreparingIndex {
        stage: "Generating per-order runs",
    });

    // Write the authoritative unified language dict to languages.dict so
    // upload_dicts_from_disk can include it. This dict was built from per-chunk
    // lang vocab files — all indexes use these global lang_ids.
    {
        let lang_dict_path = input.run_dir.join("languages.dict");
        fluree_db_indexer::run_index::dict_io::write_language_dict(
            &lang_dict_path,
            &input.unified_lang_dict,
        )?;
        tracing::info!(
            tags = input.unified_lang_dict.len(),
            path = %lang_dict_path.display(),
            "wrote authoritative unified language dict"
        );
    }

    // Shared content store for dict upload, index upload, and other CAS operations.
    let content_store: std::sync::Arc<dyn fluree_db_core::ContentStore> = std::sync::Arc::new(
        fluree_db_core::storage::content_store_for(storage.clone(), alias),
    );

    // Start dict upload (reads flat files from run_dir, builds CoW trees, uploads to CAS).
    // This runs concurrently with the index builds below.
    let dict_upload_handle = {
        let content_store = content_store.clone();
        let run_dir = input.run_dir.to_path_buf();
        let namespace_codes = input.namespace_codes.clone();
        tokio::spawn(async move {
            upload_dicts_from_disk(content_store.as_ref(), &run_dir, &namespace_codes, true).await
        })
    };

    let remap_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let build_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stage_marker = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(
        fluree_db_indexer::BUILD_STAGE_REMAP,
    ));

    // ---- V3 (FLI3/FIR6) index build ----
    //
    // Only V3 format is supported. Builds FLI3 columnar leaves, FBR3 branches,
    // assembles an FIR6 root, and returns early.
    {
        let lang_remaps = input.lang_remaps;
        // V3 path: remap → V2 run files → k-way merge → FLI3/FBR3 for all 4 orders.
        let v3_index_dir = input.index_dir.to_path_buf();
        let v3_run_dir = input.run_dir.to_path_buf();
        let v3_leaflet_target_rows = config.leaflet_rows;
        let v3_leaf_target_rows = config.leaf_target_rows;
        let v3_run_budget = config.effective_run_budget_mb() * 1024 * 1024;
        let v3_worker_count = config.effective_heavy_workers();
        let v3_sorted_commit_infos = input.sorted_commit_infos;
        let v3_lang_remaps: Vec<Vec<u16>> = lang_remaps.clone();
        let v3_remap_counter = remap_counter.clone();
        let v3_build_counter = build_counter.clone();
        let v3_stage_marker = stage_marker.clone();

        config.emit_progress(ImportPhase::PreparingIndex {
            stage: "Generating per-order runs",
        });

        // Stats collection (optional): collect ID-based property stats while generating runs.
        let disable_import_id_stats = env_flag("FLUREE_IMPORT_DISABLE_ID_STATS");
        let disable_import_ref_target_stats = env_flag("FLUREE_IMPORT_DISABLE_REF_TARGET_STATS");
        let v3_collect_id_stats = input.collect_id_stats && !disable_import_id_stats;
        let v3_rdf_type_p_id = input.rdf_type_p_id;

        if input.collect_id_stats && disable_import_id_stats {
            tracing::warn!("ID stats disabled for import via FLUREE_IMPORT_DISABLE_ID_STATS");
        }
        if input.collect_id_stats && disable_import_ref_target_stats {
            tracing::warn!(
            "ref-target class stats disabled for import via FLUREE_IMPORT_DISABLE_REF_TARGET_STATS"
        );
        }

        // Type alias for the aggregate stats output from finalize_with_aggregate_properties.
        // (IdStatsResult, agg_props, class_counts, class_properties, class_ref_targets)
        type StatsOutput = (
            fluree_db_indexer::stats::IdStatsResult,
            Vec<fluree_db_core::GraphPropertyStatEntry>,
            Vec<(fluree_db_core::GraphId, u64, u64)>,
            std::collections::HashMap<
                (fluree_db_core::GraphId, u64),
                std::collections::HashSet<u32>,
            >,
            std::collections::HashMap<
                (fluree_db_core::GraphId, u64),
                std::collections::HashMap<u32, std::collections::HashMap<u64, i64>>,
            >,
        );
        type BuildStatsOutput = (
            StatsOutput,
            Option<fluree_db_indexer::stats::SpotClassStats>,
        );

        let mut v3_handle = tokio::task::spawn_blocking(
            move || -> std::result::Result<(_, Option<BuildStatsOutput>), ImportError> {
                let commits: Vec<fluree_db_indexer::CommitInput> = v3_sorted_commit_infos
                    .iter()
                    .enumerate()
                    .map(|(i, info)| {
                        let remap_dir = v3_run_dir.join("remap");
                        fluree_db_indexer::CommitInput {
                            commit_path: info.path.clone(),
                            record_count: info.record_count,
                            subject_remap_path: remap_dir.join(format!("subjects_{i:05}.rmp")),
                            string_remap_path: remap_dir.join(format!("strings_{i:05}.rmp")),
                            lang_remap: v3_lang_remaps.get(i).cloned().unwrap_or_default(),
                            types_map_path: info.types_map_path.clone(),
                        }
                    })
                    .collect();

                let mut stats_hook = v3_collect_id_stats.then(|| {
                    let mut hook = fluree_db_indexer::stats::IdStatsHook::new();
                    hook.set_rdf_type_p_id(v3_rdf_type_p_id);
                    if disable_import_ref_target_stats {
                        hook.set_track_ref_targets(false);
                    }
                    hook
                });

                // Build V3 indexes for:
                // - g_id=0 (default graph) across all chunks
                // - g_id=1 (txn-meta) from the dedicated meta chunk
                //
                // The meta chunk is appended as the final "chunk" during import so that
                // `ledger#txn-meta` queries work immediately after import without re-reading
                // commit blobs. We build g_id=1 separately to avoid a full second pass.
                let v3_runs_g0 = v3_run_dir.join("v3_runs_g0");
                let v3_runs_g1 = v3_run_dir.join("v3_runs_g1");

                let cfg_g0 = fluree_db_indexer::BuildConfig {
                    run_dir: v3_runs_g0,
                    index_dir: v3_index_dir.clone(),
                    g_id: 0,
                    leaflet_target_rows: v3_leaflet_target_rows,
                    leaf_target_rows: v3_leaf_target_rows,
                    zstd_level: 1,
                    run_budget_bytes: v3_run_budget,
                    worker_count: v3_worker_count,
                    remap_progress: Some(v3_remap_counter),
                    build_progress: Some(v3_build_counter),
                    stage_marker: Some(v3_stage_marker),
                };
                std::fs::create_dir_all(&cfg_g0.run_dir)
                    .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

                let (g0_result, spot_class_stats) = fluree_db_indexer::build_indexes_from_commits(
                    &commits,
                    &cfg_g0,
                    stats_hook.as_mut(),
                )
                .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

                let stats_output = stats_hook.map(|h| {
                    let stats_finalize_start = Instant::now();
                    tracing::info!("finalizing import id stats");
                    let output = h.finalize_with_aggregate_properties();
                    tracing::info!(
                        elapsed_ms = stats_finalize_start.elapsed().as_millis(),
                        "finalized import id stats"
                    );
                    (output, spot_class_stats)
                });

                // Meta chunk is always the last chunk when present.
                let g1_result = if let Some(meta_commit) = commits.last() {
                    let cfg_g1 = fluree_db_indexer::BuildConfig {
                        run_dir: v3_runs_g1,
                        index_dir: v3_index_dir,
                        g_id: 1,
                        leaflet_target_rows: v3_leaflet_target_rows,
                        leaf_target_rows: v3_leaf_target_rows,
                        zstd_level: 1,
                        run_budget_bytes: v3_run_budget,
                        worker_count: 1,
                        remap_progress: None,
                        build_progress: None,
                        stage_marker: None,
                    };
                    std::fs::create_dir_all(&cfg_g1.run_dir)
                        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;

                    Some(
                        fluree_db_indexer::build_indexes_from_commits(
                            std::slice::from_ref(meta_commit),
                            &cfg_g1,
                            None, // no stats in txn-meta build
                        )
                        .map_err(|e| ImportError::IndexBuild(e.to_string()))?,
                    )
                } else {
                    None
                };

                // Merge g_id=0 and g_id=1 results for upload/root assembly.
                let mut order_results = g0_result.order_results;
                let mut total_rows = g0_result.total_rows;
                let mut total_remapped = g0_result.total_remapped;
                let mut remap_elapsed = g0_result.remap_elapsed;
                let mut build_elapsed = g0_result.build_elapsed;

                if let Some((g1, _)) = g1_result {
                    total_rows += g1.total_rows;
                    total_remapped += g1.total_remapped;
                    remap_elapsed += g1.remap_elapsed;
                    build_elapsed += g1.build_elapsed;

                    for (order, g1_order) in g1.order_results {
                        if let Some((_, existing)) =
                            order_results.iter_mut().find(|(o, _)| *o == order)
                        {
                            existing.graphs.extend(g1_order.graphs);
                            existing.total_rows += g1_order.total_rows;
                        } else {
                            order_results.push((order, g1_order));
                        }
                    }
                }

                let result = fluree_db_indexer::BuildResult {
                    order_results,
                    total_rows,
                    total_remapped,
                    remap_elapsed,
                    build_elapsed,
                };

                tracing::info!(
                    total_rows = result.total_rows,
                    total_remapped = result.total_remapped,
                    remap_elapsed = ?result.remap_elapsed,
                    build_elapsed = ?result.build_elapsed,
                    orders = result.order_results.len(),
                    "V3 index build complete"
                );

                Ok((result, stats_output))
            },
        );

        let remap_total_flakes = input.cumulative_flakes;
        let build_total_flakes = input.cumulative_flakes * 4;

        let emit_index_progress =
            |stage: u8, current_stage: &mut u8, stage_start: &mut std::time::Instant| {
                if stage != *current_stage {
                    *current_stage = stage;
                    *stage_start = std::time::Instant::now();
                }

                if stage == fluree_db_indexer::BUILD_STAGE_LINK_RUNS {
                    config.emit_progress(ImportPhase::PreparingIndex {
                        stage: "Linking secondary runs",
                    });
                    return;
                }

                if stage == fluree_db_indexer::BUILD_STAGE_MERGE {
                    config.emit_progress(ImportPhase::Indexing {
                        stage: "Building indexes",
                        processed_flakes: build_counter.load(std::sync::atomic::Ordering::Relaxed),
                        total_flakes: build_total_flakes,
                        stage_elapsed_secs: stage_start.elapsed().as_secs_f64(),
                    });
                    return;
                }

                let remapped = remap_counter.load(std::sync::atomic::Ordering::Relaxed);
                if remapped >= remap_total_flakes {
                    config.emit_progress(ImportPhase::PreparingIndex {
                        stage: "Finalizing run files",
                    });
                } else {
                    config.emit_progress(ImportPhase::Indexing {
                        stage: "Generating per-order runs",
                        processed_flakes: remapped,
                        total_flakes: remap_total_flakes,
                        stage_elapsed_secs: stage_start.elapsed().as_secs_f64(),
                    });
                }
            };

        // Poll the build counters periodically so the CLI progress bar tracks the
        // actual import subphase while the work runs on a blocking thread.
        let index_start = std::time::Instant::now();
        let mut current_stage = fluree_db_indexer::BUILD_STAGE_REMAP;
        let mut stage_start = index_start;
        let (v3_result, stats_output) = loop {
            tokio::select! {
                result = &mut v3_handle => {
                    let stage = stage_marker.load(std::sync::atomic::Ordering::Relaxed);
                    emit_index_progress(stage, &mut current_stage, &mut stage_start);
                    break result
                        .map_err(|e| ImportError::IndexBuild(format!("build task panicked: {e}")))?
                        .map_err(|e| ImportError::IndexBuild(e.to_string()))?;
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {
                    let stage = stage_marker.load(std::sync::atomic::Ordering::Relaxed);
                    emit_index_progress(stage, &mut current_stage, &mut stage_start);
                }
            }
        };

        // Upload V3 artifacts to CAS.
        config.emit_progress(ImportPhase::PreparingIndex {
            stage: "Uploading index artifacts",
        });
        let upload_indexes_start = Instant::now();
        let v3_uploaded =
            fluree_db_indexer::upload_indexes_to_cas(content_store.as_ref(), &v3_result)
                .await
                .map_err(|e| ImportError::Upload(e.to_string()))?;
        tracing::info!(
            elapsed_ms = upload_indexes_start.elapsed().as_millis(),
            default_orders = v3_uploaded.default_graph_orders.len(),
            named_graphs = v3_uploaded.named_graphs.len(),
            "index artifact upload complete"
        );

        // Wait for dict upload to complete (shared with V2 path).
        if !dict_upload_handle.is_finished() {
            config.emit_progress(ImportPhase::PreparingIndex {
                stage: "Waiting for dictionary upload",
            });
        }
        let dict_wait_start = Instant::now();
        let uploaded_dicts = dict_upload_handle
            .await
            .map_err(|e| ImportError::Upload(format!("dict upload join: {e}")))?
            .map_err(|e| ImportError::Upload(e.to_string()))?;
        tracing::info!(
            elapsed_ms = dict_wait_start.elapsed().as_millis(),
            graph_iris = uploaded_dicts.graph_iris.len(),
            datatype_iris = uploaded_dicts.datatype_iris.len(),
            language_tags = uploaded_dicts.language_tags.len(),
            "dictionary upload complete"
        );

        // ── V3 FIR6 root assembly ──────────────────────────────────
        use fluree_db_binary_index::format::index_root::DefaultGraphOrder;
        use fluree_db_binary_index::{GraphArenaRefs, IndexRoot, VectorDictRef};

        config.emit_progress(ImportPhase::PreparingIndex {
            stage: "Assembling index root",
        });
        tracing::info!("V3 path: assembling FIR6 root");
        let root_assembly_start = Instant::now();

        // Extract DictRefs from uploaded dicts (arena refs are separate fields).
        let dict_refs_v6 = uploaded_dicts.dict_refs;

        let mut graph_ids = std::collections::BTreeSet::new();
        for g_id_str in uploaded_dicts.numbig.keys() {
            if let Ok(g_id) = g_id_str.parse::<u16>() {
                graph_ids.insert(g_id);
            }
        }
        for g_id_str in uploaded_dicts.vectors.keys() {
            if let Ok(g_id) = g_id_str.parse::<u16>() {
                graph_ids.insert(g_id);
            }
        }
        let graph_arenas_v6: Vec<GraphArenaRefs> = graph_ids
            .into_iter()
            .map(|g_id| {
                let g_id_str = g_id.to_string();
                let numbig: Vec<(u32, ContentId)> = uploaded_dicts
                    .numbig
                    .get(&g_id_str)
                    .map(|m| {
                        m.iter()
                            .map(|(k, v): (&String, _)| (k.parse::<u32>().unwrap_or(0), v.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                let vectors: Vec<VectorDictRef> = uploaded_dicts
                    .vectors
                    .get(&g_id_str)
                    .map(|m| {
                        m.iter()
                            .map(|(k, v): (&String, _)| VectorDictRef {
                                p_id: k.parse::<u32>().unwrap_or(0),
                                manifest: v.manifest.clone(),
                                shards: v.shards.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                GraphArenaRefs {
                    g_id,
                    numbig,
                    vectors,
                    spatial: Vec::new(),
                    fulltext: vec![],
                }
            })
            .collect();

        let ns_codes_v6: std::collections::BTreeMap<u16, String> = input
            .namespace_codes
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();

        // Build predicate_sids (done inline here since
        // the shared code runs after V3 would have already returned).
        let ns_reverse_v6: std::collections::HashMap<String, u16> = input
            .namespace_codes
            .iter()
            .map(|(&code, prefix)| (prefix.clone(), code))
            .collect();
        let pred_path = input.run_dir.join("predicates.json");
        let predicate_sids_v6: Vec<(u16, String)> = if pred_path.exists() {
            let bytes = std::fs::read(&pred_path)?;
            let by_id: Vec<String> = serde_json::from_slice(&bytes).map_err(|e| {
                ImportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            by_id
                .iter()
                .map(|iri| {
                    let (prefix, suffix) = fluree_db_core::canonical_split(
                        iri,
                        fluree_db_core::NsSplitMode::default(),
                    );
                    match ns_reverse_v6.get(prefix) {
                        Some(&code) => (code, suffix.to_string()),
                        None => (0u16, iri.clone()),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Destructure the aggregate stats output for both IndexStats (root) and CLI summary.
        let (
            id_stats_result,
            summary_agg_props,
            id_hook_class_counts,
            id_hook_class_ref_targets,
            spot_class_stats,
        ) = match stats_output {
            Some(((ids, agg_props, class_counts, _class_props, class_ref_targets), spot_stats)) => {
                (
                    Some(ids),
                    agg_props,
                    class_counts,
                    class_ref_targets,
                    spot_stats,
                )
            }
            None => (None, Vec::new(), Vec::new(), HashMap::new(), None),
        };

        #[allow(clippy::type_complexity)]
        let (summary_class_counts, summary_class_ref_targets): (
            Vec<(fluree_db_core::GraphId, u64, u64)>,
            HashMap<(fluree_db_core::GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
        ) = if let Some(ref cs) = spot_class_stats {
            let mut class_counts: Vec<(fluree_db_core::GraphId, u64, u64)> = cs
                .class_counts
                .iter()
                .map(|(&(g_id, class_sid64), &count)| (g_id, class_sid64, count))
                .collect();
            class_counts.sort_by_key(|&(g_id, class_sid64, _)| (g_id, class_sid64));

            let mut class_ref_targets: HashMap<
                (fluree_db_core::GraphId, u64),
                HashMap<u32, HashMap<u64, i64>>,
            > = HashMap::new();
            for (&(g_id, class_sid64), prop_map) in &cs.class_prop_refs {
                let mut converted_props: HashMap<u32, HashMap<u64, i64>> = HashMap::new();
                for (&p_id, target_map) in prop_map {
                    let mut converted_targets: HashMap<u64, i64> = HashMap::new();
                    for (&target_sid, &count) in target_map {
                        converted_targets.insert(target_sid, count as i64);
                    }
                    converted_props.insert(p_id, converted_targets);
                }
                class_ref_targets.insert((g_id, class_sid64), converted_props);
            }

            (class_counts, class_ref_targets)
        } else {
            (id_hook_class_counts, id_hook_class_ref_targets)
        };

        let stats_v6: Option<fluree_db_core::IndexStats> = id_stats_result.map(|id_stats| {
            use fluree_db_core::index_stats as is;

            // Aggregate across graphs by p_id (deprecated SID-keyed view).
            struct PropAgg {
                count: u64,
                ndv_values: u64,
                ndv_subjects: u64,
                last_modified_t: i64,
                datatypes: Vec<(u8, u64)>,
            }
            let mut agg: std::collections::HashMap<u32, PropAgg> = std::collections::HashMap::new();
            for g in &id_stats.graphs {
                for p in &g.properties {
                    let e = agg.entry(p.p_id).or_insert(PropAgg {
                        count: 0,
                        ndv_values: 0,
                        ndv_subjects: 0,
                        last_modified_t: 0,
                        datatypes: Vec::new(),
                    });
                    e.count += p.count;
                    e.ndv_values = e.ndv_values.max(p.ndv_values);
                    e.ndv_subjects = e.ndv_subjects.max(p.ndv_subjects);
                    e.last_modified_t = e.last_modified_t.max(p.last_modified_t);
                    for &(dt, cnt) in &p.datatypes {
                        if let Some(existing) = e.datatypes.iter_mut().find(|(d, _)| *d == dt) {
                            existing.1 += cnt;
                        } else {
                            e.datatypes.push((dt, cnt));
                        }
                    }
                }
            }

            let properties: Vec<is::PropertyStatEntry> = agg
                .into_iter()
                .map(|(p_id, pa)| {
                    let (ns, name) = predicate_sids_v6
                        .get(p_id as usize)
                        .cloned()
                        .unwrap_or((0u16, String::new()));
                    is::PropertyStatEntry {
                        sid: (ns, name),
                        count: pa.count,
                        ndv_values: pa.ndv_values,
                        ndv_subjects: pa.ndv_subjects,
                        last_modified_t: pa.last_modified_t,
                        datatypes: pa.datatypes,
                    }
                })
                .collect();

            let mut graphs = id_stats.graphs;
            if let Some(ref cs) = spot_class_stats {
                let mut per_graph_classes = fluree_db_indexer::stats::build_class_stat_entries(
                    cs,
                    &predicate_sids_v6,
                    &[],
                    &uploaded_dicts.language_tags,
                    input.run_dir,
                    input.namespace_codes,
                )
                .map_err(ImportError::Io)
                .ok()
                .unwrap_or_default();
                for g in &mut graphs {
                    g.classes = per_graph_classes.remove(&g.g_id);
                }
            }

            is::IndexStats {
                flakes: id_stats.total_flakes,
                size: 0,
                properties: Some(properties),
                classes: None,
                graphs: Some(graphs),
            }
        });

        // Build CLI import summary before IndexRoot consumes predicate_sids_v6.
        let summary = if summary_agg_props.is_empty() && summary_class_counts.is_empty() {
            None
        } else {
            Some(build_import_summary(
                &summary_agg_props,
                &summary_class_counts,
                &summary_class_ref_targets,
                &predicate_sids_v6,
                input.namespace_codes,
                input.run_dir,
                input.prefix_map,
            ))
        };

        // Build default_graph_orders from V3 upload result.
        let default_graph_orders: Vec<DefaultGraphOrder> = v3_uploaded
            .default_graph_orders
            .into_iter()
            .map(|(order, leaves)| DefaultGraphOrder { order, leaves })
            .collect();

        // Custom datatype IRIs (non-reserved only, for o_type table).
        let custom_dt_iris: Vec<String> = uploaded_dicts
            .datatype_iris
            .iter()
            .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
            .cloned()
            .collect();

        let root_v6 = IndexRoot {
            ledger_id: alias.to_string(),
            index_t: input.final_t,
            base_t: 0,
            subject_id_encoding: uploaded_dicts.subject_id_encoding,
            namespace_codes: ns_codes_v6,
            predicate_sids: predicate_sids_v6,
            graph_iris: uploaded_dicts.graph_iris,
            datatype_iris: uploaded_dicts.datatype_iris,
            language_tags: uploaded_dicts.language_tags.clone(),
            dict_refs: dict_refs_v6,
            subject_watermarks: uploaded_dicts.subject_watermarks,
            string_watermark: uploaded_dicts.string_watermark,
            // Bulk import assigns global string IDs via k-way merge over per-chunk
            // sorted vocab files, so StringId order is lexicographic by UTF-8 bytes.
            lex_sorted_string_ids: true,
            total_commit_size,
            total_asserts,
            total_retracts,
            graph_arenas: graph_arenas_v6,
            o_type_table: IndexRoot::build_o_type_table(
                &custom_dt_iris,
                &uploaded_dicts.language_tags,
            ),
            default_graph_orders,
            named_graphs: v3_uploaded.named_graphs,
            stats: stats_v6,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
        };

        // Encode and upload FIR6 root.
        let root_bytes = root_v6.encode();
        let root_digest = fluree_db_core::sha256_hex(&root_bytes);
        let root_cid = fluree_db_core::ContentId::from_hex_digest(
            fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT,
            &root_digest,
        )
        .expect("valid SHA-256 hex digest");

        storage
            .content_write_bytes_with_hash(
                fluree_db_core::ContentKind::IndexRoot,
                alias,
                &root_digest,
                &root_bytes,
            )
            .await
            .map_err(|e| ImportError::Upload(format!("FIR6 root upload: {e}")))?;

        tracing::info!(
            root_cid = %root_cid,
            root_bytes = root_bytes.len(),
            o_type_entries = root_v6.o_type_table.len(),
            default_orders = root_v6.default_graph_orders.len(),
            named_graphs = root_v6.named_graphs.len(),
            elapsed_ms = root_assembly_start.elapsed().as_millis(),
            "FIR6 root assembled and uploaded"
        );

        Ok(IndexUploadResult {
            root_id: root_cid,
            index_t: input.final_t,
            summary,
        })
    }
}

/// Build a lightweight import summary for CLI display.
///
/// Extracts top-5 classes, properties, and connections from the aggregate
/// stats produced by `IdStatsHook::finalize_with_aggregate_properties()`.
/// Uses the subject dict files on disk for class IRI resolution.
#[allow(clippy::type_complexity)]
fn build_import_summary(
    agg_props: &[fluree_db_core::GraphPropertyStatEntry],
    class_counts: &[(fluree_db_core::GraphId, u64, u64)],
    class_ref_targets: &HashMap<(fluree_db_core::GraphId, u64), HashMap<u32, HashMap<u64, i64>>>,
    predicate_sids: &[(u16, String)],
    namespace_codes: &HashMap<u16, String>,
    run_dir: &Path,
    prefix_map: &HashMap<String, String>,
) -> ImportSummary {
    // Build IRI → compact form using turtle @prefix declarations + well-known builtins.
    let builtin_prefixes: &[(&str, &str)] = &[
        (fluree_vocab::rdf::NS, "rdf"),
        (fluree_vocab::rdfs::NS, "rdfs"),
        (fluree_vocab::xsd::NS, "xsd"),
        (fluree_vocab::owl::NS, "owl"),
        (fluree_vocab::shacl::NS, "sh"),
    ];
    let mut iri_to_short: Vec<(&str, &str)> = prefix_map
        .iter()
        .map(|(iri, short)| (iri.as_str(), short.as_str()))
        .collect();
    for &(iri, short) in builtin_prefixes {
        if !prefix_map.contains_key(iri) {
            iri_to_short.push((iri, short));
        }
    }
    // Sort longest-first so longest match wins.
    iri_to_short.sort_by_key(|b| std::cmp::Reverse(b.0.len()));

    let compact = |full_iri: &str| -> String {
        for &(ns_iri, short) in &iri_to_short {
            if let Some(suffix) = full_iri.strip_prefix(ns_iri) {
                return format!("{short}:{suffix}");
            }
        }
        full_iri.to_string()
    };

    // ---- Top 5 properties by count ----
    let mut props_sorted: Vec<_> = agg_props.iter().collect();
    props_sorted.sort_by_key(|b| std::cmp::Reverse(b.count));
    let top_properties: Vec<(String, u64)> = props_sorted
        .iter()
        .take(5)
        .filter_map(|p| {
            let (ns_code, suffix) = predicate_sids.get(p.p_id as usize)?;
            let ns_iri = namespace_codes
                .get(ns_code)
                .map(std::string::String::as_str)
                .unwrap_or("");
            Some((compact(&format!("{ns_iri}{suffix}")), p.count))
        })
        .collect();

    if class_counts.is_empty() {
        return ImportSummary {
            top_classes: Vec::new(),
            top_properties,
            top_connections: Vec::new(),
        };
    }

    // Build SID resolver from dict files on disk.
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_indexer::run_index::dict_io;

    let resolve_sid_to_iri = |sid64: u64| -> Option<String> {
        let subj = SubjectId::from_u64(sid64);
        let ns_code = subj.ns_code();
        let prefix = namespace_codes
            .get(&ns_code)
            .map(std::string::String::as_str)
            .unwrap_or("");
        let sids_path = run_dir.join("subjects.sids");
        let idx_path = run_dir.join("subjects.idx");
        let fwd_path = run_dir.join("subjects.fwd");

        let sids_vec = dict_io::read_subject_sid_map(&sids_path).ok()?;
        let (fwd_offsets, fwd_lens) = dict_io::read_forward_index(&idx_path).ok()?;
        let pos = sids_vec.binary_search(&sid64).ok()?;
        let off = fwd_offsets[pos];
        let len = fwd_lens[pos] as usize;
        let mut iri_buf = vec![0u8; len];
        let mut file = std::fs::File::open(&fwd_path).ok()?;
        use std::io::{Read as _, Seek as _, SeekFrom};
        file.seek(SeekFrom::Start(off)).ok()?;
        file.read_exact(&mut iri_buf).ok()?;
        let iri = std::str::from_utf8(&iri_buf).ok()?;
        let suffix = if !prefix.is_empty() && iri.starts_with(prefix) {
            &iri[prefix.len()..]
        } else {
            iri
        };
        Some(format!("{prefix}{suffix}"))
    };

    // Cache resolved IRIs to avoid repeated file I/O.
    let mut iri_cache: HashMap<u64, Option<String>> = HashMap::new();
    let mut resolve_cached = |sid64: u64| -> Option<String> {
        iri_cache
            .entry(sid64)
            .or_insert_with(|| resolve_sid_to_iri(sid64))
            .clone()
    };

    // ---- Top 5 classes by count (union across graphs) ----
    let mut class_totals: HashMap<u64, u64> = HashMap::new();
    for &(_g_id, class_sid64, count) in class_counts {
        *class_totals.entry(class_sid64).or_insert(0) += count;
    }
    let mut classes_sorted: Vec<_> = class_totals.iter().collect();
    classes_sorted.sort_by(|a, b| b.1.cmp(a.1));
    let top_classes: Vec<(String, u64)> = classes_sorted
        .iter()
        .take(5)
        .filter_map(|(&sid, &count)| Some((compact(&resolve_cached(sid)?), count)))
        .collect();

    // ---- Top 5 connections: Class → property → Class (union across graphs) ----
    let mut connections: Vec<(u64, u32, u64, u64)> = Vec::new();
    for (&(_g_id, src_class), prop_map) in class_ref_targets {
        for (&p_id, target_map) in prop_map {
            for (&target_class, &delta) in target_map {
                if delta > 0 {
                    connections.push((src_class, p_id, target_class, delta as u64));
                }
            }
        }
    }
    connections.sort_by_key(|b| std::cmp::Reverse(b.3));
    let top_connections: Vec<(String, String, String, u64)> = connections
        .iter()
        .take(5)
        .filter_map(|&(src, p_id, tgt, count)| {
            let src_iri = compact(&resolve_cached(src)?);
            let tgt_iri = compact(&resolve_cached(tgt)?);
            let (ns_code, suffix) = predicate_sids.get(p_id as usize)?;
            let ns_iri = namespace_codes
                .get(ns_code)
                .map(std::string::String::as_str)
                .unwrap_or("");
            Some((
                src_iri,
                compact(&format!("{ns_iri}{suffix}")),
                tgt_iri,
                count,
            ))
        })
        .collect();

    ImportSummary {
        top_classes,
        top_properties,
        top_connections,
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Generate a unique session identifier for directory naming.
///
/// Uses nanosecond timestamp XOR'd for uniqueness. Not cryptographic,
/// just unique enough for concurrent session directories.
fn session_id() -> String {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", seed ^ (seed >> 64))
}

/// Derive the session directory path.
///
/// Uses `{temp_dir}/fluree-import/{alias_prefix}/tmp_import/{session_id}/`.
/// The cleanup phase removes this directory on success; on failure it is
/// kept for debugging (logged with full path).
fn derive_session_dir<S: Storage>(_storage: &S, alias_prefix: &str, sid: &str) -> PathBuf {
    // Allow overriding import scratch space for large imports.
    //
    // This is critical on macOS where `std::env::temp_dir()` often points to a
    // small system volume. For multi-GB TTL imports, run files + spool files
    // can exceed hundreds of GB temporarily.
    //
    // Set `FLUREE_IMPORT_DIR=/path/with/space` to force the base directory.
    let base = std::env::var_os("FLUREE_IMPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("fluree-import"));
    base.join(alias_prefix).join("tmp_import").join(sid)
}

/// Build a JSON-LD @context from turtle prefix declarations + built-in namespaces,
/// write it to CAS, and push it as the ledger's default context via nameservice config.
async fn store_default_context<S>(
    storage: &S,
    nameservice: &dyn crate::NameServicePublisher,
    alias: &str,
    turtle_prefix_map: &HashMap<String, String>,
) -> std::result::Result<(), ImportError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use fluree_db_nameservice::{ConfigPayload, ConfigValue};

    // Build IRI → short prefix map, starting with well-known built-in prefixes.
    // Turtle-declared prefixes override built-ins if they map the same IRI.
    let builtin_prefixes: &[(&str, &str)] = &[
        (fluree_vocab::rdf::NS, "rdf"),
        (fluree_vocab::rdfs::NS, "rdfs"),
        (fluree_vocab::xsd::NS, "xsd"),
        (fluree_vocab::owl::NS, "owl"),
        (fluree_vocab::shacl::NS, "sh"),
        (fluree_vocab::geo::NS, "geo"),
    ];

    let mut context_map = serde_json::Map::new();

    // Add built-ins first
    for &(iri, short) in builtin_prefixes {
        context_map.insert(
            short.to_string(),
            serde_json::Value::String(iri.to_string()),
        );
    }

    // Overlay turtle-declared prefixes (IRI → short name)
    for (iri, short) in turtle_prefix_map {
        context_map.insert(short.clone(), serde_json::Value::String(iri.clone()));
    }

    if context_map.is_empty() {
        return Ok(());
    }

    let context_json = serde_json::Value::Object(context_map);
    let context_bytes = serde_json::to_vec(&context_json)
        .map_err(|e| ImportError::Storage(format!("serialize default context: {e}")))?;

    // Write to CAS via ContentStore (returns CID)
    let cs = fluree_db_core::content_store_for(storage.clone(), alias);
    let cid = cs
        .put(ContentKind::LedgerConfig, &context_bytes)
        .await
        .map_err(|e| ImportError::Storage(format!("write default context to CAS: {e}")))?;

    tracing::info!(
        cid = %cid,
        prefixes = context_json.as_object().map(serde_json::Map::len).unwrap_or(0),
        "default context written to CAS"
    );

    // Read current config before push (needed for GC of old blob)
    let current_config = nameservice
        .get_config(alias)
        .await
        .map_err(|e| ImportError::Storage(format!("get config: {e}")))?;

    let old_default_context = current_config
        .as_ref()
        .and_then(|c| c.payload.as_ref())
        .and_then(|p| p.default_context.clone());

    // Push new CID to nameservice config
    let new_config = ConfigValue::new(
        current_config.as_ref().map_or(1, |c| c.v + 1),
        Some(ConfigPayload::with_default_context(cid.clone())),
    );

    nameservice
        .push_config(alias, current_config.as_ref(), &new_config)
        .await
        .map_err(|e| ImportError::Storage(format!("push default context config: {e}")))?;

    tracing::info!("default context published to nameservice config");

    // GC: best-effort delete of the old context blob if CID changed
    if let Some(old_cid) = old_default_context {
        if old_cid != cid {
            let kind = old_cid.content_kind().unwrap_or(ContentKind::LedgerConfig);
            let addr = fluree_db_core::content_address(
                storage.storage_method(),
                kind,
                alias,
                &old_cid.digest_hex(),
            );
            if let Err(e) = storage.delete(&addr).await {
                tracing::debug!(%e, old_addr = %addr, "could not GC old default context blob");
            }
        }
    }

    Ok(())
}
