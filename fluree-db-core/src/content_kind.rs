//! Content kind and dictionary kind types with multicodec mapping.
//!
//! This module consolidates the `ContentKind` enum, `DictKind` enum, and
//! their associated multicodec constants. These types are used throughout
//! the workspace for content-addressed storage routing, CID construction,
//! and filesystem layout.

// ============================================================================
// Fluree multicodec constants (private-use range)
// ============================================================================

/// Private-use multicodec base offset for Fluree.
const FLUREE_CODEC_BASE: u64 = 0x0030_0000;

/// Multicodec for Fluree commit blobs.
pub const CODEC_FLUREE_COMMIT: u64 = FLUREE_CODEC_BASE + 1;

/// Multicodec for Fluree transaction (txn) blobs.
pub const CODEC_FLUREE_TXN: u64 = FLUREE_CODEC_BASE + 2;

/// Multicodec for Fluree dictionary blobs (all sub-kinds).
///
/// `DictKind` is parameterized (`NumBig { p_id }`, `VectorShard { p_id }`, etc.)
/// and cannot map 1:1 to a codec value. The dict sub-kind is part of the
/// stored bytes, not the CID.
pub const CODEC_FLUREE_DICT_BLOB: u64 = FLUREE_CODEC_BASE + 6;

/// Multicodec for Fluree garbage collection records.
pub const CODEC_FLUREE_GARBAGE: u64 = FLUREE_CODEC_BASE + 7;

/// Multicodec for Fluree ledger configuration objects (origin discovery).
pub const CODEC_FLUREE_LEDGER_CONFIG: u64 = FLUREE_CODEC_BASE + 8;

/// Multicodec for Fluree HLL stats sketch blobs.
pub const CODEC_FLUREE_STATS_SKETCH: u64 = FLUREE_CODEC_BASE + 9;

/// Multicodec for Fluree graph source snapshot blobs (BM25, vector, etc.).
pub const CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT: u64 = FLUREE_CODEC_BASE + 10;

/// Multicodec for Fluree spatial index artifacts (S2 cell index, geometry arena, root manifest).
pub const CODEC_FLUREE_SPATIAL_INDEX: u64 = FLUREE_CODEC_BASE + 11;

/// Multicodec for Fluree history sidecar blobs (FHS1, per-leaf time-travel data).
pub const CODEC_FLUREE_HISTORY_SIDECAR: u64 = FLUREE_CODEC_BASE + 12;

/// Multicodec for Fluree index branch manifests (FBR3).
pub const CODEC_FLUREE_INDEX_BRANCH: u64 = FLUREE_CODEC_BASE + 13;

/// Multicodec for Fluree index leaf files (FLI3).
pub const CODEC_FLUREE_INDEX_LEAF: u64 = FLUREE_CODEC_BASE + 14;

/// Multicodec for Fluree index root descriptors (FIR6).
pub const CODEC_FLUREE_INDEX_ROOT: u64 = FLUREE_CODEC_BASE + 15;

/// Multicodec for graph source mapping blobs (R2RML Turtle).
pub const CODEC_FLUREE_GRAPH_SOURCE_MAPPING: u64 = FLUREE_CODEC_BASE + 16;

// Legacy codec constants (pre-V3 format). Kept for backward-compatible CID
// resolution — existing ledgers may have index artifacts stored under these
// codecs in the nameservice.
const CODEC_LEGACY_INDEX_ROOT: u64 = FLUREE_CODEC_BASE + 3;
const CODEC_LEGACY_INDEX_BRANCH: u64 = FLUREE_CODEC_BASE + 4;
const CODEC_LEGACY_INDEX_LEAF: u64 = FLUREE_CODEC_BASE + 5;

// ============================================================================
// DictKind
// ============================================================================

/// What kind of dictionary blob is being stored.
///
/// Used by [`ContentKind::DictBlob`] to route dictionary artifacts to
/// typed CAS paths (e.g. `objects/dicts/{hash}.dict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DictKind {
    /// Named-graph IRI dictionary (FRD1 format).
    Graphs,
    /// Datatype IRI dictionary (FRD1 format).
    Datatypes,
    /// Language tag dictionary (FRD1 format).
    Languages,
    /// Subject forward file — raw concatenated UTF-8 IRIs (mmap'd).
    SubjectForward,
    /// Subject index — offsets/lengths into subject forward file (FSI1 format).
    SubjectIndex,
    /// Subject reverse hash index — hash→s_id binary search table (SRV1 format, mmap'd).
    SubjectReverse,
    /// String value forward file — raw concatenated UTF-8 (mmap'd).
    StringForward,
    /// String value index — offsets/lengths into string forward file (FSI1 format).
    StringIndex,
    /// String value reverse hash index (SRV1 format, mmap'd).
    StringReverse,
    /// Per-predicate overflow BigInt/BigDecimal arena (NBA1 format).
    NumBig { p_id: u32 },
    /// Per-predicate vector arena shard (VAS1 format).
    VectorShard { p_id: u32 },
    /// Per-predicate vector arena manifest (VAM1 JSON format).
    VectorManifest { p_id: u32 },
}

// ============================================================================
// ContentKind
// ============================================================================

/// What a blob "is", so storage can choose its layout.
///
/// Filesystem-like storages typically map this to directory prefixes such as
/// `index/spot/` vs `commit/`. Some storages may ignore it (e.g. IPFS-like
/// content stores).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentKind {
    /// Commit blob (binary commit format)
    Commit,
    /// Transaction blob (binary encoded flakes)
    Txn,
    /// DB root index node (FIR6 format)
    IndexRoot,
    /// Garbage record (GC metadata)
    GarbageRecord,
    /// Dictionary artifact (predicates, subjects, strings, etc.)
    DictBlob { dict: DictKind },
    /// Index branch manifest (FBR3 format)
    IndexBranch,
    /// Index leaf file (FLI3 format)
    IndexLeaf,
    /// Ledger configuration object (origin discovery, replication defaults)
    LedgerConfig,
    /// HLL stats sketch blob (per-property HyperLogLog registers for NDV estimation)
    StatsSketch,
    /// Graph source snapshot blob (serialized BM25/vector index)
    GraphSourceSnapshot,
    /// Spatial index artifact (S2 cell index, geometry arena, root manifest)
    SpatialIndex,
    /// History sidecar blob (FHS1, per-leaf time-travel data)
    HistorySidecar,
    /// Graph source mapping blob (R2RML Turtle content, stored via CAS)
    GraphSourceMapping,
}

// ============================================================================
// ContentKind <-> codec mapping
// ============================================================================

impl ContentKind {
    /// Map this content kind to its Fluree multicodec value.
    ///
    /// All `DictBlob { .. }` sub-kinds map to the single `CODEC_FLUREE_DICT_BLOB`.
    pub fn to_codec(&self) -> u64 {
        match self {
            ContentKind::Commit => CODEC_FLUREE_COMMIT,
            ContentKind::Txn => CODEC_FLUREE_TXN,
            ContentKind::IndexRoot => CODEC_FLUREE_INDEX_ROOT,
            ContentKind::IndexBranch => CODEC_FLUREE_INDEX_BRANCH,
            ContentKind::IndexLeaf => CODEC_FLUREE_INDEX_LEAF,
            ContentKind::DictBlob { .. } => CODEC_FLUREE_DICT_BLOB,
            ContentKind::GarbageRecord => CODEC_FLUREE_GARBAGE,
            ContentKind::LedgerConfig => CODEC_FLUREE_LEDGER_CONFIG,
            ContentKind::StatsSketch => CODEC_FLUREE_STATS_SKETCH,
            ContentKind::GraphSourceSnapshot => CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT,
            ContentKind::SpatialIndex => CODEC_FLUREE_SPATIAL_INDEX,
            ContentKind::HistorySidecar => CODEC_FLUREE_HISTORY_SIDECAR,
            ContentKind::GraphSourceMapping => CODEC_FLUREE_GRAPH_SOURCE_MAPPING,
        }
    }

    /// Attempt to reverse-map a multicodec value to a `ContentKind`.
    ///
    /// Returns `None` for unknown codecs. For `CODEC_FLUREE_DICT_BLOB`,
    /// returns a default `DictBlob` variant — callers that need the exact
    /// `DictKind` sub-variant must inspect the stored bytes.
    pub fn from_codec(codec: u64) -> Option<Self> {
        match codec {
            CODEC_FLUREE_COMMIT => Some(ContentKind::Commit),
            CODEC_FLUREE_TXN => Some(ContentKind::Txn),
            CODEC_FLUREE_INDEX_ROOT => Some(ContentKind::IndexRoot),
            CODEC_FLUREE_INDEX_BRANCH => Some(ContentKind::IndexBranch),
            CODEC_FLUREE_INDEX_LEAF => Some(ContentKind::IndexLeaf),
            CODEC_FLUREE_DICT_BLOB => Some(ContentKind::DictBlob {
                dict: DictKind::Graphs,
            }),
            CODEC_FLUREE_GARBAGE => Some(ContentKind::GarbageRecord),
            CODEC_FLUREE_LEDGER_CONFIG => Some(ContentKind::LedgerConfig),
            CODEC_FLUREE_STATS_SKETCH => Some(ContentKind::StatsSketch),
            CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT => Some(ContentKind::GraphSourceSnapshot),
            CODEC_FLUREE_SPATIAL_INDEX => Some(ContentKind::SpatialIndex),
            CODEC_FLUREE_HISTORY_SIDECAR => Some(ContentKind::HistorySidecar),
            CODEC_FLUREE_GRAPH_SOURCE_MAPPING => Some(ContentKind::GraphSourceMapping),
            // Legacy codecs (pre-V3 format) — map to current content kinds so
            // CIDs stored by older builds can still be resolved.
            CODEC_LEGACY_INDEX_ROOT => Some(ContentKind::IndexRoot),
            CODEC_LEGACY_INDEX_BRANCH => Some(ContentKind::IndexBranch),
            CODEC_LEGACY_INDEX_LEAF => Some(ContentKind::IndexLeaf),
            _ => None,
        }
    }

    /// Short name for this content kind, used in filesystem layout.
    pub fn codec_dir_name(&self) -> &'static str {
        match self {
            ContentKind::Commit => "commit",
            ContentKind::Txn => "txn",
            ContentKind::IndexRoot => "index-root",
            ContentKind::IndexBranch => "index-branch",
            ContentKind::IndexLeaf => "index-leaf",
            ContentKind::DictBlob { .. } => "dict",
            ContentKind::GarbageRecord => "garbage",
            ContentKind::LedgerConfig => "config",
            ContentKind::StatsSketch => "stats-sketch",
            ContentKind::GraphSourceSnapshot => "graph-source-snapshot",
            ContentKind::SpatialIndex => "spatial-index",
            ContentKind::HistorySidecar => "history-sidecar",
            ContentKind::GraphSourceMapping => "graph-source-mapping",
        }
    }
}

/// File extension for a given [`DictKind`] (used in CAS paths).
///
/// All dict blob sub-kinds use a single extension because the CID codec
/// (`CODEC_FLUREE_DICT_BLOB`) cannot distinguish sub-kinds. Using a uniform
/// extension ensures that `cid_to_address()` resolves to the same path
/// regardless of whether the caller knows the exact `DictKind`.
pub(crate) fn dict_kind_extension(_dict: DictKind) -> &'static str {
    "dict"
}
