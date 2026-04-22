//! Run-based index generation pipeline.
//!
//! This module implements Phase B: walking the binary commit chain,
//! resolving commit-local IDs to global numeric IDs, and producing
//! sorted run files for external-merge index building.
//!
//! ## Sub-modules
//!
//! - [`resolve`] — CommitResolver, global dictionaries, chunk dicts, language remapping
//! - [`runs`] — Run file I/O, spool, run writer, streaming readers
//! - [`build`] — Index building, merge, incremental updates
//! - [`vocab`] — Dictionary I/O, vocab files, vocab merge

// ── Sub-modules ──────────────────────────────────────────────────────────────
pub mod build;
pub mod numfloat_dict;
pub mod resolve;
pub mod runs;
pub mod vocab;

// ── Submodule re-exports (preserve `run_index::module::*` paths) ─────────────
pub use resolve::chunk_dict;
pub use resolve::global_dict;
pub use resolve::lang_remap;
pub use resolve::resolver;

pub use runs::run_file;
pub use runs::run_writer;
pub use runs::spool;
pub use runs::streaming_reader;

pub use build::incremental_branch;
pub use build::incremental_leaf;
pub use build::incremental_resolve;
pub use build::incremental_root;
pub use build::index_build;
pub use build::merge;
pub use build::novelty_merge;
pub use build::shared_pool;

pub use vocab::dict_io;
pub use vocab::dict_merge;
pub use vocab::vocab_file;
pub use vocab::vocab_merge;

// ── Flat re-exports ──────────────────────────────────────────────────────────

// From resolve/
pub use resolve::chunk_dict::{hash_subject, ChunkStringDict, ChunkSubjectDict};
pub use resolve::global_dict::{
    DictAllocator, DictWorkerCache, GlobalDicts, LanguageTagDict, PredicateDict,
    SharedDictAllocator, StringValueDict, SubjectDict,
};
pub use resolve::lang_remap::build_lang_remap_from_vocabs;
pub use resolve::resolver::{CommitResolver, ResolverError};

// From runs/
pub use runs::run_file::{write_run_file, RunFileInfo};
pub use runs::run_writer::{MultiOrderConfig, MultiOrderRunWriter, RunWriter, RunWriterResult};
pub use runs::spool::{
    collect_chunk_run_files, remap_commit_to_runs, remap_spool_to_runs, remap_v1_to_v2,
    sort_remap_and_write_sorted_commit, spool_to_runs, SortedCommitInfo, SpoolFileInfo,
    SpoolReader, SpoolWriter, TypesMapConfig,
};
pub use runs::streaming_reader::StreamingRunReader;

// From build/
pub use build::incremental_branch::{update_branch, BranchUpdateConfig, BranchUpdateResult};
pub use build::incremental_leaf::{update_leaf, LeafUpdateInput, LeafUpdateOutput, NewLeafBlob};
pub use build::incremental_resolve::{IncrementalNovelty, IncrementalResolveConfig};
pub use build::incremental_root::IncrementalRootBuilder;
pub use build::index_build::{
    build_all_indexes, build_index, BuildAllConfig, GraphIndexResult, IndexBuildConfig,
    IndexBuildError, IndexBuildResult,
};
pub use build::merge::KWayMerge;
pub use build::novelty_merge::{merge_novelty, MergeInput, MergeOutput};
pub use build::shared_pool::{SharedNumBigPool, SharedVectorArenaPool};

use std::collections::HashMap;
use std::io;
use std::path::Path;

/// Result of run generation.
#[derive(Debug)]
pub struct RunGenerationResult {
    /// The run files produced (sorted SPOT runs).
    pub run_files: Vec<RunFileInfo>,
    /// Number of distinct subjects in the global dictionary.
    pub subject_count: u64,
    /// Number of distinct predicates in the global dictionary.
    pub predicate_count: u32,
    /// Number of distinct string values in the global dictionary.
    pub string_count: u32,
    /// Whether any namespace's local subject ID exceeded u16::MAX,
    /// requiring wide (u64) subject ID encoding in leaflets.
    pub needs_wide: bool,
    /// Total RunRecords emitted across all run files.
    pub total_records: u64,
    /// Number of commits processed.
    pub commit_count: usize,
    /// ID-based stats hook accumulated across all resolved commits.
    /// Contains per-(graph, property) HLL sketches and datatype usage.
    /// `None` if stats collection was not enabled.
    pub stats_hook: Option<crate::stats::IdStatsHook>,
    /// Total size of all commit blobs in bytes.
    pub total_commit_size: u64,
    /// Total number of assertions across all commits.
    pub total_asserts: u64,
    /// Total number of retractions across all commits.
    pub total_retracts: u64,
}

/// Persist namespace map to `{run_dir}/namespaces.json`.
pub fn persist_namespaces(ns_prefixes: &HashMap<u16, String>, run_dir: &Path) -> io::Result<()> {
    let mut entries: Vec<_> = ns_prefixes.iter().collect();
    entries.sort_by_key(|(&code, _)| code);

    let json_array: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|(&code, prefix)| serde_json::json!({ "code": code, "prefix": prefix }))
        .collect();

    let json_str = serde_json::to_string_pretty(&json_array).map_err(io::Error::other)?;

    let path = run_dir.join("namespaces.json");
    std::fs::write(&path, json_str)?;
    tracing::info!(
        ?path,
        entries = ns_prefixes.len(),
        "namespace map persisted"
    );
    Ok(())
}
