//! Incremental commit resolution for the V6 (FIR6) index root.
//!
//! V6-native version of `incremental_resolve.rs`. Loads an `IndexRoot`
//! instead of `IndexRoot`, builds an `OTypeRegistry`, and produces
//! `RunRecordV2` output with per-record op bytes.
//!
//! The commit chain walking, reconciliation, and remap logic is reused from
//! the V5 module — those are format-agnostic. The differences are:
//!
//! 1. Root type: `IndexRoot` (shared `DictRefs` for dict trees)
//! 2. Output records: `Vec<RunRecordV2>` + `Vec<u8>` (parallel ops)
//! 3. `OTypeRegistry` built from root's `datatype_iris` + `language_tags`

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Instant;

use fluree_db_binary_index::dict::DictTreeReader;
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_binary_index::format::run_record::{cmp_for_order, RunRecord, RunSortOrder};
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_core::content_id::ContentId;
use fluree_db_core::o_type_registry::OTypeRegistry;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::DatatypeDictId;

use crate::run_index::resolve::resolver::{RebuildChunk, ResolverError, SharedResolverState};

// ============================================================================
// Error type
// ============================================================================

/// Errors specific to incremental resolution.
#[derive(Debug)]
pub enum IncrementalResolveError {
    /// Root loading or decoding failed.
    RootLoad(String),
    /// Dict tree loading failed.
    DictTreeLoad(String),
    /// Commit chain walking failed.
    CommitChain(String),
    /// Resolution failed.
    Resolve(ResolverError),
    /// I/O error.
    Io(io::Error),
}

impl std::fmt::Display for IncrementalResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RootLoad(msg) => write!(f, "root load: {msg}"),
            Self::DictTreeLoad(msg) => write!(f, "dict tree load: {msg}"),
            Self::CommitChain(msg) => write!(f, "commit chain: {msg}"),
            Self::Resolve(e) => write!(f, "resolve: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
        }
    }
}

impl std::error::Error for IncrementalResolveError {}

impl From<io::Error> for IncrementalResolveError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ResolverError> for IncrementalResolveError {
    fn from(e: ResolverError) -> Self {
        Self::Resolve(e)
    }
}

// ============================================================================
// Types
// ============================================================================

/// Configuration for V6 incremental commit resolution.
pub struct IncrementalResolveConfig {
    /// CID of the base FIR6 index root.
    pub base_root_id: ContentId,
    /// CID of the head commit (latest commit to include).
    pub head_commit_id: ContentId,
    /// Only include commits with `t > from_t` (typically `root.index_t`).
    pub from_t: i64,
    /// Optional disk-backed artifact cache directory for remote dict leaves.
    pub artifact_cache_dir: Option<std::path::PathBuf>,
    /// Maximum cumulative commit bytes to load during the commit-chain walk.
    /// If exceeded, incremental resolution aborts so the caller can fall back
    /// to a full rebuild. `None` means unlimited.
    pub max_commit_bytes: Option<usize>,
    /// Configured full-text properties for this incremental run. Seeded into
    /// `SharedResolverState.fulltext_hook_config` before resolution so the
    /// hook routes configured plain-string values into BM25 arena building.
    /// Empty = only the `@fulltext` datatype path collects entries.
    pub fulltext_configured_properties: Vec<crate::config::ConfiguredFulltextProperty>,
}

/// Result of V6 incremental commit resolution.
///
/// Contains globally-addressed `RunRecordV2` records sorted by graph+SPOT,
/// with parallel `ops` array, plus metadata for downstream phases.
pub struct IncrementalNovelty {
    /// Globally-addressed RunRecordV2 records, sorted by (g_id, SPOT).
    pub records: Vec<RunRecordV2>,
    /// Parallel ops array: 1=assert, 0=retract. Same length as `records`.
    pub ops: Vec<u8>,
    /// The decoded base root (needed by downstream phases).
    pub base_root: IndexRoot,
    /// Updated resolver state.
    pub shared: SharedResolverState,
    /// New subject entries not found in reverse tree.
    ///
    /// **Invariant**: sorted by `(ns_code, local_id)` ascending. Within each
    /// namespace, local_ids are contiguous starting at `watermark + 1`.
    /// Consumed directly by forward pack builders (no re-sorting needed).
    pub new_subjects: Vec<(u16, u64, Vec<u8>)>,
    /// New string entries not found in reverse tree.
    ///
    /// **Invariant**: sorted by `string_id` ascending. IDs are contiguous
    /// starting at `string_watermark + 1`. Consumed directly by forward
    /// pack builders (no re-sorting needed).
    pub new_strings: Vec<(u32, Vec<u8>)>,
    /// Updated subject watermarks.
    pub updated_watermarks: Vec<u64>,
    /// Updated string watermark.
    pub updated_string_watermark: u32,
    /// Maximum t value across all resolved commits.
    pub max_t: i64,
    /// Cumulative commit blob size.
    pub delta_commit_size: u64,
    /// Total assertions.
    pub delta_asserts: u64,
    /// Total retractions.
    pub delta_retracts: u64,
    /// Base vector arena counts per (g_id, p_id) for handle offsetting.
    pub base_vector_counts: HashMap<(u16, u32), u32>,
    /// Base numbig arena counts per (g_id, p_id).
    pub base_numbig_counts: HashMap<(u16, u32), usize>,
    /// String text bytes for fulltext assertion entries.
    pub fulltext_string_bytes: HashMap<u32, Vec<u8>>,
}

// ============================================================================
// Public API
// ============================================================================

/// Resolve incremental commits against a V6 (FIR6) index root.
///
/// This is the V6-native Phase 1 for the incremental indexing pipeline.
pub async fn resolve_incremental_commits_v6(
    cs: Arc<dyn ContentStore>,
    config: IncrementalResolveConfig,
) -> Result<IncrementalNovelty, IncrementalResolveError> {
    let t_start = Instant::now();
    tracing::debug!(
        base_root = %config.base_root_id,
        head = %config.head_commit_id,
        from_t = config.from_t,
        "V6 incremental resolve: starting"
    );

    // 1. Load and decode IndexRoot.
    let (root_bytes, t_root_load_ms) = {
        let t0 = Instant::now();
        let bytes = cs.get(&config.base_root_id).await.map_err(|e| {
            IncrementalResolveError::RootLoad(format!(
                "failed to load FIR6 root {}: {}",
                config.base_root_id, e
            ))
        })?;
        (bytes, t0.elapsed().as_millis() as u64)
    };

    let (root, t_root_decode_ms) = {
        let t0 = Instant::now();
        let root = IndexRoot::decode(&root_bytes).map_err(|e| {
            IncrementalResolveError::RootLoad(format!("failed to decode FIR6: {e}"))
        })?;
        (root, t0.elapsed().as_millis() as u64)
    };

    tracing::debug!(
        index_t = root.index_t,
        from_t = config.from_t,
        head = %config.head_commit_id,
        "V6 incremental resolve: loaded base root"
    );

    // 2. Build OTypeRegistry from root's datatype and language metadata.
    let custom_dt_iris: Vec<String> = root
        .datatype_iris
        .iter()
        .skip(DatatypeDictId::RESERVED_COUNT as usize)
        .cloned()
        .collect();
    let o_type_registry = OTypeRegistry::new(&custom_dt_iris);

    // 3. Load subject + string reverse dict trees (same DictRefs as V5).
    let (subject_tree, string_tree, t_dict_load_ms) = {
        let t0 = Instant::now();
        let subject_tree = DictTreeReader::from_refs(
            &cs,
            &root.dict_refs.subject_reverse,
            None,
            config.artifact_cache_dir.as_deref(),
        )
        .await
        .map_err(|e| IncrementalResolveError::DictTreeLoad(format!("subject reverse: {e}")))?;
        let string_tree = DictTreeReader::from_refs(
            &cs,
            &root.dict_refs.string_reverse,
            None,
            config.artifact_cache_dir.as_deref(),
        )
        .await
        .map_err(|e| IncrementalResolveError::DictTreeLoad(format!("string reverse: {e}")))?;
        (subject_tree, string_tree, t0.elapsed().as_millis() as u64)
    };

    // 4. Seed SharedResolverState from V6 root.
    let mut shared = SharedResolverState::from_index_root(&root)?;

    // Enable spatial hook for non-POINT geometry detection.
    shared.spatial_hook = Some(crate::spatial_hook::SpatialHook::new());
    // Enable fulltext hook.
    shared.fulltext_hook = Some(crate::fulltext_hook::FulltextHook::new());

    // Seed the configured full-text property set so the hook routes
    // plain-string values on configured properties into BM25 arena building.
    if !config.fulltext_configured_properties.is_empty() {
        shared.configure_fulltext_properties(&config.fulltext_configured_properties);
        tracing::debug!(
            count = config.fulltext_configured_properties.len(),
            "fulltext: seeded configured property set for incremental run"
        );
    }

    // 4a. Pre-seed numbig arenas.
    let (base_numbig_counts, base_vector_counts, t_seed_arenas_ms) = {
        let t0 = Instant::now();

        let mut base_numbig_counts: HashMap<(u16, u32), usize> = HashMap::new();
        for ga in &root.graph_arenas {
            if ga.numbig.is_empty() {
                continue;
            }
            let nb_map = shared.numbigs.entry(ga.g_id).or_default();
            for (p_id, cid) in &ga.numbig {
                let bytes = cs.get(cid).await.map_err(|e| {
                    IncrementalResolveError::RootLoad(format!(
                        "numbig arena load for g_id={}, p_id={}: {}",
                        ga.g_id, p_id, e
                    ))
                })?;
                let arena =
                    fluree_db_binary_index::arena::numbig::read_numbig_arena_from_bytes(&bytes)
                        .map_err(|e| {
                            IncrementalResolveError::RootLoad(format!("numbig arena decode: {e}"))
                        })?;
                base_numbig_counts.insert((ga.g_id, *p_id), arena.len());
                nb_map.insert(*p_id, arena);
            }
        }

        // 4b. Pre-load base vector arenas into `shared.vectors`.
        //
        // Loading the actual shard bytes (rather than just the count) makes
        // chunk inserts append to the unified base+chunk arena, so handles
        // returned by `arena.insert_f64` are already global. This in turn
        // makes the offset pass a no-op for VECTOR_ID and lets re-asserted
        // logical vector facts (same `(s, p, value)` across commits) dedup
        // against the base entry instead of producing duplicate rows.
        let mut base_vector_counts: HashMap<(u16, u32), u32> = HashMap::new();
        for ga in &root.graph_arenas {
            for vref in &ga.vectors {
                let manifest_bytes = cs.get(&vref.manifest).await.map_err(|e| {
                    IncrementalResolveError::RootLoad(format!(
                        "vector manifest load for g_id={}, p_id={}: {}",
                        ga.g_id, vref.p_id, e
                    ))
                })?;
                let manifest =
                    fluree_db_binary_index::arena::vector::read_vector_manifest(&manifest_bytes)
                        .map_err(|e| {
                            IncrementalResolveError::RootLoad(format!(
                                "vector manifest decode: {e}"
                            ))
                        })?;
                base_vector_counts.insert((ga.g_id, vref.p_id), manifest.total_count);

                // Fetch each shard and reassemble a fully-populated
                // VectorArena. Inserts during chunk processing will append
                // to this — handles already global.
                let mut shards = Vec::with_capacity(manifest.shards.len());
                for shard_info in &manifest.shards {
                    let cid: ContentId = shard_info.cas.parse().map_err(|e| {
                        IncrementalResolveError::RootLoad(format!(
                            "vector shard CID parse for g_id={}, p_id={}: {e}",
                            ga.g_id, vref.p_id
                        ))
                    })?;
                    let bytes = cs.get(&cid).await.map_err(|e| {
                        IncrementalResolveError::RootLoad(format!(
                            "vector shard load for g_id={}, p_id={}, cid={}: {e}",
                            ga.g_id, vref.p_id, shard_info.cas
                        ))
                    })?;
                    let shard =
                        fluree_db_binary_index::arena::vector::read_vector_shard_from_bytes(&bytes)
                            .map_err(|e| {
                                IncrementalResolveError::RootLoad(format!(
                                    "vector shard decode for g_id={}, p_id={}: {e}",
                                    ga.g_id, vref.p_id
                                ))
                            })?;
                    shards.push(shard);
                }
                let arena = fluree_db_binary_index::arena::vector::load_arena_from_shards(
                    &manifest, shards,
                )
                .map_err(|e| {
                    IncrementalResolveError::RootLoad(format!(
                        "vector arena reassembly for g_id={}, p_id={}: {e}",
                        ga.g_id, vref.p_id
                    ))
                })?;
                shared
                    .vectors
                    .entry(ga.g_id)
                    .or_default()
                    .insert(vref.p_id, arena);
            }
        }

        (
            base_numbig_counts,
            base_vector_counts,
            t0.elapsed().as_millis() as u64,
        )
    };

    // 4c. Pre-populate `vector_fact_handles` from base SPOT.
    //
    // For every base VECTOR_ID row, decode `(s_ns_code, s_name, p_id, o_i, value, handle)`
    // and insert into the shared fact map. This makes:
    //  - Vector RETRACTIONS in this chunk find the assertion's existing handle
    //    (correctness — without this, the chunk would emit Ok(None) and the
    //    user's DELETE silently no-ops at incremental publish).
    //  - Vector RE-ASSERTIONS dedup against the base entry (same handle reused),
    //    so we don't end up with two encoded rows for the same logical fact.
    if !shared.vectors.is_empty() {
        seed_vector_fact_handles(
            Arc::clone(&cs),
            &root,
            &mut shared,
            config.artifact_cache_dir.as_deref(),
        )
        .await
        .map_err(|e| IncrementalResolveError::RootLoad(format!("vector fact-handle seed: {e}")))?;
    }

    // 5. Walk commit chain (commit format is version-independent).
    let (walked_commits, t_walk_chain_ms) = {
        let t0 = Instant::now();
        let commits = walk_commit_chain_since(
            cs.as_ref(),
            &config.head_commit_id,
            config.from_t,
            config.max_commit_bytes,
        )
        .await?;
        (commits, t0.elapsed().as_millis() as u64)
    };

    tracing::debug!(
        commit_count = walked_commits.len(),
        root_load_ms = t_root_load_ms,
        root_decode_ms = t_root_decode_ms,
        dict_load_ms = t_dict_load_ms,
        seed_arenas_ms = t_seed_arenas_ms,
        walk_chain_ms = t_walk_chain_ms,
        avg_walk_ms_per_commit = if walked_commits.is_empty() {
            0.0
        } else {
            t_walk_chain_ms as f64 / walked_commits.len() as f64
        },
        "V6 incremental resolve: setup and commit-chain walk finished"
    );

    // 6. Resolve commits into chunk.
    let (
        chunk,
        max_t,
        delta_commit_size,
        delta_asserts,
        delta_retracts,
        commit_count,
        t_commit_resolve_ms,
    ) = {
        let t0 = Instant::now();
        let mut chunk = RebuildChunk::new();
        let mut max_t: i64 = root.index_t;
        let mut delta_commit_size = 0u64;
        let mut delta_asserts = 0u64;
        let mut delta_retracts = 0u64;
        let mut commit_count = 0usize;

        for walked in &walked_commits {
            let resolved = shared
                .resolve_commit_into_chunk(&walked.bytes, &walked.cid.digest_hex(), &mut chunk)
                .map_err(IncrementalResolveError::Resolve)?;

            max_t = max_t.max(walked.t);
            delta_commit_size += resolved.size;
            delta_asserts += resolved.asserts as u64;
            delta_retracts += resolved.retracts as u64;
            commit_count += 1;
        }

        (
            chunk,
            max_t,
            delta_commit_size,
            delta_asserts,
            delta_retracts,
            commit_count,
            t0.elapsed().as_millis() as u64,
        )
    };

    tracing::debug!(
        commit_count,
        records = chunk.records.len(),
        max_t,
        "V6 incremental resolve: commits resolved into chunk"
    );
    tracing::debug!(
        commit_count,
        chunk_records = chunk.records.len(),
        max_t,
        delta_commit_size,
        delta_asserts,
        delta_retracts,
        commit_resolve_ms = t_commit_resolve_ms,
        avg_resolve_ms_per_commit = if commit_count == 0 {
            0.0
        } else {
            t_commit_resolve_ms as f64 / commit_count as f64
        },
        elapsed_since_start_ms = t_start.elapsed().as_millis() as u64,
        "V6 incremental resolve: commit resolution finished"
    );

    // Cache watermarks before potential root move.
    let base_subject_watermarks = root.subject_watermarks.clone();
    let base_string_watermark = root.string_watermark;

    if chunk.records.is_empty() {
        tracing::debug!(
            root_load_ms = t_root_load_ms,
            root_decode_ms = t_root_decode_ms,
            dict_load_ms = t_dict_load_ms,
            seed_arenas_ms = t_seed_arenas_ms,
            walk_chain_ms = t_walk_chain_ms,
            commit_resolve_ms = t_commit_resolve_ms,
            total_ms = t_start.elapsed().as_millis() as u64,
            commit_count,
            "V6 incremental resolve: timings (no records)"
        );
        return Ok(IncrementalNovelty {
            records: Vec::new(),
            ops: Vec::new(),
            base_root: root,
            shared,
            updated_watermarks: base_subject_watermarks,
            updated_string_watermark: base_string_watermark,
            new_subjects: Vec::new(),
            new_strings: Vec::new(),
            max_t,
            delta_commit_size,
            delta_asserts,
            delta_retracts,
            base_vector_counts,
            base_numbig_counts,
            fulltext_string_bytes: HashMap::new(),
        });
    }

    // 7. Reconcile chunk-local IDs to global IDs (same algorithm as V5).
    let reconcile = reconcile_chunk_to_global(
        &chunk,
        &subject_tree,
        &string_tree,
        &base_subject_watermarks,
        base_string_watermark,
    )?;
    let t_reconcile_ms = t_start.elapsed().as_millis().saturating_sub(
        (t_root_load_ms
            + t_root_decode_ms
            + t_dict_load_ms
            + t_seed_arenas_ms
            + t_walk_chain_ms
            + t_commit_resolve_ms) as u128,
    ) as u64;
    // Note: the above gives a conservative aggregate since start; we also measure precise
    // step timings below where possible.

    // 8. Remap fulltext hook entries.
    let t0 = Instant::now();
    let fulltext_string_bytes: HashMap<u32, Vec<u8>> = {
        let chunk_forward = chunk.strings.forward_entries();
        if let Some(ref mut ft) = shared.fulltext_hook {
            let mut map = HashMap::new();
            for entry in ft.entries_mut() {
                let local_id = entry.string_id as usize;
                let global_id = match reconcile.string_remap.get(local_id) {
                    Some(&id) => id,
                    None => {
                        tracing::warn!(local_id, "fulltext entry string_id remap miss; skipping");
                        entry.is_assert = false;
                        entry.string_id = u32::MAX;
                        continue;
                    }
                };
                if entry.is_assert {
                    if let Some(bytes) = chunk_forward.get(local_id) {
                        map.entry(global_id).or_insert_with(|| bytes.clone());
                    }
                }
                entry.string_id = global_id;
            }
            map
        } else {
            HashMap::new()
        }
    };
    let t_remap_fulltext_ms = t0.elapsed().as_millis() as u64;

    // 9. Remap all V1 records in-place, then convert to V2.
    let t0 = Instant::now();
    let mut v1_records = chunk.records;
    for record in &mut v1_records {
        remap_record(record, &reconcile.subject_remap, &reconcile.string_remap)?;
    }
    let t_remap_records_ms = t0.elapsed().as_millis() as u64;

    // VECTOR_ID handles are already globally-correct: chunk inserts
    // appended to the pre-loaded base arena (step 4b) so they return
    // `base_count..base_count+chunk_count` directly, and retractions /
    // re-assertions resolve to base handles via the pre-populated
    // `vector_fact_handles` (step 4c). No offset pass needed.
    //
    // Sanity guard: every VECTOR_ID record's o_key must be < total arena
    // count (base + chunk). A regression here would mean a chunk emitted
    // a handle that doesn't exist in the unified arena.
    debug_assert!(
        v1_records.iter().all(|record| {
            if ObjKind::from_u8(record.o_kind) != ObjKind::VECTOR_ID {
                return true;
            }
            let base_count = base_vector_counts
                .get(&(record.g_id, record.p_id))
                .copied()
                .unwrap_or(0) as u64;
            let chunk_count = shared
                .vectors
                .get(&record.g_id)
                .and_then(|m| m.get(&record.p_id))
                .map(|a| a.len() as u64)
                .unwrap_or(0);
            // `chunk_count` is base + new (since base was pre-loaded).
            record.o_key < chunk_count.max(base_count)
        }),
        "VECTOR_ID record has out-of-range o_key after pre-loaded incremental resolve"
    );

    // 10. Sort V1 records by (g_id, SPOT) — needed for the conversion step
    //     which preserves sort order.
    let t0 = Instant::now();
    let spot_cmp = cmp_for_order(RunSortOrder::Spot);
    v1_records.sort_unstable_by(|a, b| a.g_id.cmp(&b.g_id).then_with(|| spot_cmp(a, b)));
    let t_sort_ms = t0.elapsed().as_millis() as u64;

    // 11. Convert V1 → V2 + extract ops.
    let t0 = Instant::now();
    let mut records = Vec::with_capacity(v1_records.len());
    let mut ops = Vec::with_capacity(v1_records.len());
    for v1 in &v1_records {
        records.push(RunRecordV2::from_v1(v1, &o_type_registry));
        ops.push(v1.op);
    }
    let t_convert_ms = t0.elapsed().as_millis() as u64;

    tracing::debug!(
        root_load_ms = t_root_load_ms,
        root_decode_ms = t_root_decode_ms,
        dict_load_ms = t_dict_load_ms,
        seed_arenas_ms = t_seed_arenas_ms,
        walk_chain_ms = t_walk_chain_ms,
        commit_resolve_ms = t_commit_resolve_ms,
        reconcile_ms = t_reconcile_ms,
        remap_fulltext_ms = t_remap_fulltext_ms,
        remap_records_ms = t_remap_records_ms,
        sort_ms = t_sort_ms,
        convert_ms = t_convert_ms,
        total_ms = t_start.elapsed().as_millis() as u64,
        commit_count,
        record_count = records.len(),
        new_subjects = reconcile.new_subjects.len(),
        new_strings = reconcile.new_strings.len(),
        "V6 incremental resolve: timings"
    );

    Ok(IncrementalNovelty {
        records,
        ops,
        base_root: root,
        shared,
        new_subjects: reconcile.new_subjects,
        new_strings: reconcile.new_strings,
        updated_watermarks: reconcile.updated_watermarks,
        updated_string_watermark: reconcile.updated_string_watermark,
        max_t,
        delta_commit_size,
        delta_asserts,
        delta_retracts,
        base_vector_counts,
        base_numbig_counts,
        fulltext_string_bytes,
    })
}

// ============================================================================
// Internal: Commit Chain Walking
// ============================================================================

struct WalkedCommit {
    cid: ContentId,
    t: i64,
    bytes: Vec<u8>,
}

/// SPOT-scan the base index for every `o_kind = VECTOR_ID` row and
/// pre-populate `shared.vector_fact_handles` with
/// `(g_id, s_ns_code, s_name, p_id, o_i, f32_bits) → handle` so chunk
/// retractions of base-asserted vectors find their handle (and chunk
/// re-assertions dedup against the base entry).
///
/// Reuses the public `BinaryIndexStore::load_from_root_v6` for the SPOT
/// cursor + dict decoding. The duplicate arena loading is bounded; if it
/// becomes a hot path we can add a slimmer "scan-only" loader.
async fn seed_vector_fact_handles(
    cs: Arc<dyn ContentStore>,
    root: &fluree_db_binary_index::format::index_root::IndexRoot,
    shared: &mut SharedResolverState,
    cache_dir: Option<&std::path::Path>,
) -> io::Result<()> {
    use fluree_db_binary_index::format::run_record::RunSortOrder;
    use fluree_db_binary_index::read::binary_cursor::BinaryCursor;
    use fluree_db_binary_index::read::binary_index_store::BinaryIndexStore;
    use fluree_db_binary_index::read::column_types::{BinaryFilter, ColumnProjection, ColumnSet};
    use fluree_db_core::o_type::OType;

    let cache_dir = cache_dir
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);

    let store = Arc::new(
        BinaryIndexStore::load_from_root_v6(Arc::clone(&cs), root, &cache_dir, None).await?,
    );

    // Walk every graph that has vector arenas; for each, scan SPOT for
    // VECTOR_ID rows and decode (s_ns_code, s_name, p_id, o_i, value) →
    // handle into the shared fact map.
    let graphs_with_vectors: Vec<u16> = shared.vectors.keys().copied().collect();
    for g_id in graphs_with_vectors {
        let branch = match store.branch_for_order(g_id, RunSortOrder::Spot) {
            Some(b) => Arc::clone(b),
            None => continue,
        };
        let arena_map = match shared.vectors.get(&g_id) {
            Some(m) => m,
            None => continue,
        };
        let filter = BinaryFilter {
            o_type: Some(OType::VECTOR.as_u16()),
            ..Default::default()
        };
        let projection = ColumnProjection::for_scan(ColumnSet::EMPTY, false, RunSortOrder::Spot);
        let mut cursor = BinaryCursor::scan_all(
            Arc::clone(&store),
            RunSortOrder::Spot,
            branch,
            filter,
            projection,
        );
        let fact_map = shared.vector_fact_handles.entry(g_id).or_default();
        while let Some(batch) = cursor.next_batch()? {
            for i in 0..batch.row_count {
                if batch.o_type.get_or(i, 0) != OType::VECTOR.as_u16() {
                    continue;
                }
                let s_id = batch.s_id.get(i);
                let p_id = batch.p_id.get(i);
                let handle = batch.o_key.get(i) as u32;
                let o_i = batch.o_i.get_or(i, u32::MAX);
                let (ns_code, name) = match store.resolve_subject_parts(s_id) {
                    Ok(parts) => parts,
                    Err(e) => {
                        tracing::warn!(
                            g_id,
                            s_id,
                            error = %e,
                            "vector fact-handle seed: subject decode failed; skipping row"
                        );
                        continue;
                    }
                };
                let arena = match arena_map.get(&p_id) {
                    Some(a) => a,
                    None => continue,
                };
                let f32_bits: Vec<u32> = match arena.get_f32(handle) {
                    Some(slice) => slice.iter().map(|&x| x.to_bits()).collect(),
                    None => {
                        tracing::warn!(
                            g_id,
                            p_id,
                            handle,
                            "vector fact-handle seed: arena handle out of range; skipping"
                        );
                        continue;
                    }
                };
                fact_map.insert(
                    (
                        ns_code,
                        std::sync::Arc::<str>::from(name.as_str()),
                        p_id,
                        o_i,
                        f32_bits,
                    ),
                    handle,
                );
            }
        }
    }
    Ok(())
}

async fn walk_commit_chain_since(
    cs: &dyn ContentStore,
    head_id: &ContentId,
    from_t: i64,
    max_commit_bytes: Option<usize>,
) -> Result<Vec<WalkedCommit>, IncrementalResolveError> {
    let walk_started = Instant::now();

    // Use DAG-aware traversal to handle merge commits with multiple parents.
    let dag = fluree_db_core::collect_dag_cids(cs, head_id, from_t)
        .await
        .map_err(|e| IncrementalResolveError::CommitChain(e.to_string()))?;

    // collect_dag_cids returns (t, cid) sorted by t descending; reverse for chronological order.
    let mut commits = Vec::with_capacity(dag.len());
    let mut cumulative_bytes: usize = 0;

    for (t, cid) in dag.into_iter().rev() {
        // Check byte budget before loading the next commit.
        if let Some(budget) = max_commit_bytes {
            if cumulative_bytes >= budget {
                tracing::info!(
                    cumulative_bytes,
                    budget,
                    commits_so_far = commits.len(),
                    "V6 incremental resolve: commit-chain walk exceeded byte budget, aborting"
                );
                return Err(IncrementalResolveError::CommitChain(format!(
                    "commit chain bytes ({cumulative_bytes}) exceeded budget ({budget}); \
                     falling back to full rebuild"
                )));
            }
        }

        let bytes = cs.get(&cid).await.map_err(|e| {
            IncrementalResolveError::CommitChain(format!("failed to load commit {cid}: {e}"))
        })?;
        cumulative_bytes += bytes.len();
        commits.push(WalkedCommit { cid, t, bytes });
    }

    tracing::debug!(
        commits = commits.len(),
        cumulative_bytes,
        from_t,
        head = %head_id,
        elapsed_ms = walk_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: commit-chain walk complete"
    );
    Ok(commits)
}

// ============================================================================
// Internal: Reconciliation (reused from V5 — same algorithm)
// ============================================================================

use fluree_db_binary_index::dict::reverse_leaf::subject_reverse_key;

struct ReconcileResult {
    subject_remap: Vec<u64>,
    string_remap: Vec<u32>,
    new_subjects: Vec<(u16, u64, Vec<u8>)>,
    new_strings: Vec<(u32, Vec<u8>)>,
    updated_watermarks: Vec<u64>,
    updated_string_watermark: u32,
}

fn reconcile_chunk_to_global(
    chunk: &RebuildChunk,
    subject_tree: &DictTreeReader,
    string_tree: &DictTreeReader,
    subject_watermarks: &[u64],
    string_watermark: u32,
) -> Result<ReconcileResult, IncrementalResolveError> {
    // Subject reconciliation.
    let subject_entries = chunk.subjects.forward_entries();
    let subject_started = Instant::now();
    let subject_reads_before = subject_tree.disk_reads();
    let subject_local_file_reads_before = subject_tree.local_file_reads();
    let subject_remote_fetches_before = subject_tree.remote_fetches();
    let subject_hits_before = subject_tree.cache_hits();
    let subject_cache_misses_before = subject_tree.cache_misses();
    let mut subject_remap = vec![0u64; subject_entries.len()];
    let mut new_subjects = Vec::new();
    let mut ns_next_local: HashMap<u16, u64> = HashMap::new();
    let mut updated_watermarks = subject_watermarks.to_vec();
    let mut subject_existing = 0usize;
    let mut subject_new = 0usize;
    tracing::debug!(
        subject_entries = subject_entries.len(),
        subject_tree_entries = subject_tree.total_entries(),
        subject_tree_source = subject_tree.source_kind(),
        subject_tree_leaf_count = subject_tree.leaf_count(),
        subject_tree_local_file_count = subject_tree.local_file_count(),
        subject_tree_remote_cid_count = subject_tree.remote_cid_count(),
        subject_tree_has_global_cache = subject_tree.has_global_cache(),
        subject_tree_disk_reads = subject_reads_before,
        subject_tree_local_file_reads = subject_tree.local_file_reads(),
        subject_tree_remote_fetches = subject_tree.remote_fetches(),
        subject_tree_cache_hits = subject_hits_before,
        subject_tree_cache_misses = subject_tree.cache_misses(),
        "V6 incremental resolve: subject reconciliation starting"
    );

    let subject_reverse_keys: Vec<Vec<u8>> = subject_entries
        .iter()
        .map(|(ns_code, name_bytes)| subject_reverse_key(*ns_code, name_bytes))
        .collect();
    let subject_existing_ids = subject_tree
        .reverse_lookup_many(subject_reverse_keys.iter().map(Vec::as_slice))
        .map_err(IncrementalResolveError::Io)?;

    for (chunk_local_id, ((ns_code, name_bytes), existing_id)) in
        subject_entries.iter().zip(subject_existing_ids).enumerate()
    {
        let global_sid64 = match existing_id {
            Some(sid64) => {
                subject_existing += 1;
                sid64
            }
            None => {
                subject_new += 1;
                let wm = if (*ns_code as usize) < updated_watermarks.len() {
                    updated_watermarks[*ns_code as usize]
                } else {
                    updated_watermarks.resize(*ns_code as usize + 1, 0);
                    0
                };
                let next = ns_next_local.entry(*ns_code).or_insert(wm + 1);
                let local_id = *next;
                *next += 1;
                updated_watermarks[*ns_code as usize] = local_id;
                let sid64 = SubjectId::new(*ns_code, local_id).as_u64();
                new_subjects.push((*ns_code, local_id, name_bytes.clone()));
                sid64
            }
        };
        subject_remap[chunk_local_id] = global_sid64;
    }

    tracing::debug!(
        subject_entries = subject_entries.len(),
        existing = subject_existing,
        new = subject_new,
        disk_reads = subject_tree
            .disk_reads()
            .saturating_sub(subject_reads_before),
        local_file_reads = subject_tree
            .local_file_reads()
            .saturating_sub(subject_local_file_reads_before),
        remote_fetches = subject_tree
            .remote_fetches()
            .saturating_sub(subject_remote_fetches_before),
        cache_hits = subject_tree
            .cache_hits()
            .saturating_sub(subject_hits_before),
        cache_misses = subject_tree
            .cache_misses()
            .saturating_sub(subject_cache_misses_before),
        elapsed_ms = subject_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: subject reconciliation complete"
    );

    // String reconciliation.
    let string_entries = chunk.strings.forward_entries();
    let string_started = Instant::now();
    let string_reads_before = string_tree.disk_reads();
    let string_local_file_reads_before = string_tree.local_file_reads();
    let string_remote_fetches_before = string_tree.remote_fetches();
    let string_hits_before = string_tree.cache_hits();
    let string_cache_misses_before = string_tree.cache_misses();
    let mut string_remap = vec![0u32; string_entries.len()];
    let mut new_strings = Vec::new();
    let mut next_string_id = string_watermark + 1;
    let mut string_existing = 0usize;
    let mut string_new = 0usize;
    tracing::debug!(
        string_entries = string_entries.len(),
        string_tree_entries = string_tree.total_entries(),
        string_tree_source = string_tree.source_kind(),
        string_tree_leaf_count = string_tree.leaf_count(),
        string_tree_local_file_count = string_tree.local_file_count(),
        string_tree_remote_cid_count = string_tree.remote_cid_count(),
        string_tree_has_global_cache = string_tree.has_global_cache(),
        string_tree_disk_reads = string_reads_before,
        string_tree_local_file_reads = string_tree.local_file_reads(),
        string_tree_remote_fetches = string_tree.remote_fetches(),
        string_tree_cache_hits = string_hits_before,
        string_tree_cache_misses = string_tree.cache_misses(),
        "V6 incremental resolve: string reconciliation starting"
    );

    let string_existing_ids = string_tree
        .reverse_lookup_many(string_entries.iter().map(Vec::as_slice))
        .map_err(IncrementalResolveError::Io)?;

    for (chunk_local_id, (value_bytes, existing_id)) in
        string_entries.iter().zip(string_existing_ids).enumerate()
    {
        let global_str_id = match existing_id {
            Some(id) => {
                string_existing += 1;
                id as u32
            }
            None => {
                string_new += 1;
                let id = next_string_id;
                next_string_id += 1;
                new_strings.push((id, value_bytes.clone()));
                id
            }
        };
        string_remap[chunk_local_id] = global_str_id;
    }

    tracing::debug!(
        string_entries = string_entries.len(),
        existing = string_existing,
        new = string_new,
        disk_reads = string_tree.disk_reads().saturating_sub(string_reads_before),
        local_file_reads = string_tree
            .local_file_reads()
            .saturating_sub(string_local_file_reads_before),
        remote_fetches = string_tree
            .remote_fetches()
            .saturating_sub(string_remote_fetches_before),
        cache_hits = string_tree.cache_hits().saturating_sub(string_hits_before),
        cache_misses = string_tree
            .cache_misses()
            .saturating_sub(string_cache_misses_before),
        elapsed_ms = string_started.elapsed().as_millis() as u64,
        "V6 incremental resolve: string reconciliation complete"
    );

    let updated_string_watermark = if next_string_id > string_watermark + 1 {
        next_string_id - 1
    } else {
        string_watermark
    };

    // Enforce sort invariants for downstream consumers (forward pack builders).
    // Strings are already sorted (sequential IDs from watermark+1).
    debug_assert!(
        new_strings.windows(2).all(|w| w[0].0 < w[1].0),
        "new_strings must be sorted by string_id ascending"
    );
    // Subjects may be interleaved across namespaces; sort by (ns_code, local_id).
    new_subjects.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    Ok(ReconcileResult {
        subject_remap,
        string_remap,
        new_subjects,
        new_strings,
        updated_watermarks,
        updated_string_watermark,
    })
}

// ============================================================================
// Internal: Record remap
// ============================================================================

fn remap_record(
    record: &mut RunRecord,
    subject_remap: &[u64],
    string_remap: &[u32],
) -> Result<(), IncrementalResolveError> {
    let local_s = record.s_id.as_u64() as usize;
    let global_s = *subject_remap.get(local_s).ok_or_else(|| {
        IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
            "subject remap out of range: local_id={}, remap_len={}",
            local_s,
            subject_remap.len()
        )))
    })?;
    record.s_id = SubjectId::from_u64(global_s);

    let o_kind = ObjKind::from_u8(record.o_kind);
    if o_kind == ObjKind::REF_ID {
        let local_o = record.o_key as usize;
        let global_o = *subject_remap.get(local_o).ok_or_else(|| {
            IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
                "ref object remap out of range: local_id={}, remap_len={}",
                local_o,
                subject_remap.len()
            )))
        })?;
        record.o_key = global_o;
    } else if o_kind == ObjKind::LEX_ID || o_kind == ObjKind::JSON_ID {
        let local_str = ObjKey::from_u64(record.o_key).decode_u32_id() as usize;
        let global_str = *string_remap.get(local_str).ok_or_else(|| {
            IncrementalResolveError::Resolve(ResolverError::Resolve(format!(
                "string remap out of range: local_id={}, remap_len={}",
                local_str,
                string_remap.len()
            )))
        })?;
        record.o_key = ObjKey::encode_u32_id(global_str).as_u64();
    }

    Ok(())
}
