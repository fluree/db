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

    /// Number of distinct logical tables referenced by the mapping
    pub table_count: usize,

    /// Names of the distinct logical tables referenced by the mapping (sorted)
    pub table_names: Vec<String>,

    /// Whether every table the mapping reads loaded at registration, the way
    /// a query loads it: through the catalog for REST, AWS Glue and S3 Tables,
    /// and by resolving its current metadata for Direct. A `false` never fails
    /// registration. `true` exactly when `connection_check` is
    /// [`ConnectionCheck::Verified`].
    pub connection_tested: bool,

    /// What that check found, kept apart so a caller can tell a slow check
    /// from a table that did not load.
    pub connection_check: ConnectionCheck,

    /// Whether the mapping was validated successfully
    pub mapping_validated: bool,

    /// Warnings about the `model` reference (policies a virtual source cannot
    /// evaluate). Empty when no model is set or nothing is amiss.
    pub model_warnings: Vec<String>,
}

/// What loading a new Iceberg source's tables at registration found. Only
/// [`Self::Verified`] means every table loaded; no outcome fails registration.
///
/// Serialized with an `outcome` tag (`"verified"`, `"no_tables"`,
/// `"client_failed"`, `"table_failed"`, `"timed_out"`) and the fields of each.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ConnectionCheck {
    /// Every table the mapping names loaded.
    Verified,
    /// The mapping could not be read, so it named no table to load.
    NoTables,
    /// The client every load goes through could not be built (for example, a
    /// catalog credential that does not resolve), so no table was loaded.
    ClientFailed {
        /// Why the client could not be built.
        error: String,
    },
    /// `table` did not load. `error` is the catalog's or the storage's
    /// reason: a missing table and an unreachable catalog are told apart by
    /// it, not by the outcome.
    TableFailed {
        /// The table as the mapping names it.
        table: String,
        /// Why it did not load.
        error: String,
    },
    /// The check ran past its budget with `loaded` of `total` tables loaded.
    /// The tables may only be slow.
    TimedOut {
        /// Tables loaded before the budget ran out.
        loaded: usize,
        /// Tables the mapping names.
        total: usize,
        /// The check's budget, in seconds.
        budget_secs: u64,
    },
}

#[cfg(feature = "iceberg")]
impl ConnectionCheck {
    /// Whether every mapped table loaded.
    pub fn is_verified(&self) -> bool {
        matches!(self, Self::Verified)
    }

    /// One line saying what the check found, for a person reading a
    /// registration report. `direct` is a source read at its table location
    /// rather than through a catalog. `"verified"`, or text starting
    /// `"not verified: "` that says why.
    pub fn summary(&self, direct: bool) -> String {
        let failed = if direct {
            "its metadata could not be read at its location"
        } else {
            "it did not load through the catalog"
        };
        match self {
            Self::Verified => "verified".to_string(),
            Self::NoTables => {
                "not verified: the mapping could not be read, so no table was loaded".to_string()
            }
            Self::ClientFailed { error } => {
                let client = if direct { "storage" } else { "catalog" };
                format!(
                    "not verified: the {client} client could not be built, so no table was \
                     loaded ({error})"
                )
            }
            Self::TableFailed { table, error } => format!(
                "not verified: table '{table}': {failed}; queries that read it fail until it \
                 loads ({error})"
            ),
            Self::TimedOut {
                loaded,
                total,
                budget_secs,
            } => format!(
                "not verified: the check ran past {budget_secs} s ({loaded} of {total} tables \
                 loaded); the tables may only be slow"
            ),
        }
    }
}

#[cfg(all(test, feature = "iceberg"))]
mod tests {
    use super::ConnectionCheck;

    /// The server sends the outcome as JSON and the CLI reads it back: the
    /// tag and field names are the wire contract.
    #[test]
    fn connection_check_wire_shape() {
        let cases = [
            (
                ConnectionCheck::Verified,
                serde_json::json!({"outcome": "verified"}),
            ),
            (
                ConnectionCheck::NoTables,
                serde_json::json!({"outcome": "no_tables"}),
            ),
            (
                ConnectionCheck::ClientFailed { error: "e".into() },
                serde_json::json!({"outcome": "client_failed", "error": "e"}),
            ),
            (
                ConnectionCheck::TableFailed {
                    table: "ns.t".into(),
                    error: "e".into(),
                },
                serde_json::json!({"outcome": "table_failed", "table": "ns.t", "error": "e"}),
            ),
            (
                ConnectionCheck::TimedOut {
                    loaded: 1,
                    total: 2,
                    budget_secs: 10,
                },
                serde_json::json!({"outcome": "timed_out", "loaded": 1, "total": 2, "budget_secs": 10}),
            ),
        ];
        for (check, json) in cases {
            assert_eq!(serde_json::to_value(&check).unwrap(), json);
            assert_eq!(
                serde_json::from_value::<ConnectionCheck>(json).unwrap(),
                check
            );
        }
    }
}
