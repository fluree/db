//! Result types for graph source operations.
//!
//! These structs represent the outcome of creating, syncing, and managing graph sources.

use fluree_db_core::ContentId;

// =============================================================================
// BM25 Results
// =============================================================================

/// Result of creating a BM25 full-text index.
#[derive(Debug, Clone)]
pub struct Bm25CreateResult {
    /// Graph source ID (name:branch)
    pub graph_source_id: String,

    /// Number of documents indexed
    pub doc_count: usize,

    /// Number of unique terms in the index
    pub term_count: usize,

    /// The commit t of the source ledger at index creation time
    pub index_t: i64,

    /// Content identifier of the persisted index root
    pub index_id: Option<ContentId>,
}

/// Result of a sync operation.
#[derive(Debug, Clone)]
pub struct Bm25SyncResult {
    /// Graph source ID
    pub graph_source_id: String,

    /// Number of documents upserted
    pub upserted: usize,

    /// Number of documents removed
    pub removed: usize,

    /// Number of subjects affected
    pub affected_subjects: usize,

    /// Old watermark before sync
    pub old_watermark: i64,

    /// New watermark after sync
    pub new_watermark: i64,

    /// Whether a full resync was performed
    pub was_full_resync: bool,
}

/// Staleness check result for a BM25 index.
#[derive(Debug, Clone)]
pub struct Bm25StalenessCheck {
    /// Graph source ID
    pub graph_source_id: String,

    /// Source ledger ID
    pub source_ledger: String,

    /// Current index watermark (what the index has been synced to)
    pub index_t: i64,

    /// Current ledger head (latest committed t)
    pub ledger_t: i64,

    /// Whether the index is stale (index_t < ledger_t)
    pub is_stale: bool,

    /// How far behind the index is (ledger_t - index_t)
    pub lag: i64,
}

/// Result of dropping a BM25 full-text index.
#[derive(Debug, Clone)]
pub struct Bm25DropResult {
    /// Graph source ID that was dropped
    pub graph_source_id: String,

    /// Number of snapshot files deleted from storage
    pub deleted_snapshots: usize,

    /// Whether the graph source was already retracted (no-op drop)
    pub was_already_retracted: bool,
}

/// Result of selecting a snapshot for time-travel.
#[derive(Debug, Clone)]
pub struct SnapshotSelection {
    /// Graph source ID
    pub graph_source_id: String,

    /// The snapshot's index time (watermark)
    pub snapshot_t: i64,

    /// Content identifier of the snapshot
    pub snapshot_id: ContentId,
}

// =============================================================================
// Vector Search Results
// =============================================================================

/// Result of creating a vector similarity search index.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorCreateResult {
    /// Graph source ID (name:branch)
    pub graph_source_id: String,

    /// Number of vectors indexed
    pub vector_count: usize,

    /// Number of documents skipped (extraction errors)
    pub skipped_count: usize,

    /// Vector dimensions
    pub dimensions: usize,

    /// The commit t of the source ledger at index creation time
    pub index_t: i64,

    /// Content identifier of the persisted index root
    pub index_id: Option<ContentId>,
}

/// Result of a vector index sync operation.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorSyncResult {
    /// Graph source ID
    pub graph_source_id: String,

    /// Number of vectors upserted
    pub upserted: usize,

    /// Number of vectors removed
    pub removed: usize,

    /// Number of vectors skipped (extraction errors)
    pub skipped: usize,

    /// Old watermark before sync
    pub old_watermark: i64,

    /// New watermark after sync
    pub new_watermark: i64,

    /// Whether a full resync was performed
    pub was_full_resync: bool,
}

/// Staleness check result for a vector index.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorStalenessCheck {
    /// Graph source ID
    pub graph_source_id: String,

    /// Source ledger ID
    pub source_ledger: String,

    /// Current index watermark
    pub index_t: i64,

    /// Current ledger head
    pub ledger_t: i64,

    /// Whether the index is stale
    pub is_stale: bool,

    /// How far behind the index is
    pub lag: i64,
}

/// Result of dropping a vector index.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorDropResult {
    /// Graph source ID that was dropped
    pub graph_source_id: String,

    /// Number of snapshot files deleted from storage
    pub deleted_snapshots: usize,

    /// Whether the graph source was already retracted (no-op drop)
    pub was_already_retracted: bool,
}

// =============================================================================
// Iceberg/R2RML Results
// =============================================================================

/// Result of creating an Iceberg graph source.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct IcebergCreateResult {
    /// Graph source ID (name:branch)
    pub graph_source_id: String,

    /// Table identifier that was registered
    pub table_identifier: String,

    /// Catalog URI
    pub catalog_uri: String,

    /// Whether the catalog connection was tested successfully
    pub connection_tested: bool,
}

/// Result of creating an R2RML graph source.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct R2rmlCreateResult {
    /// Graph source ID (name:branch)
    pub graph_source_id: String,

    /// Table identifier that was registered
    pub table_identifier: String,

    /// Catalog URI
    pub catalog_uri: String,

    /// R2RML mapping source
    pub mapping_source: String,

    /// Number of TriplesMap definitions in the mapping
    pub triples_map_count: usize,

    /// Whether the catalog connection was tested successfully
    pub connection_tested: bool,

    /// Whether the mapping was validated successfully
    pub mapping_validated: bool,
}
