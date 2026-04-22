//! Nameservice traits and implementations for Fluree DB
//!
//! This crate provides the core abstractions for ledger discovery and publishing.
//! Main traits:
//!
//! - [`NameService`]: Read-only lookup of ledger metadata
//! - [`Publisher`]: Publishing commit and index updates
//! - [`RefPublisher`]: Explicit compare-and-set ref operations for sync
//!
//! Event notification is provided by [`LedgerEventBus`], a standalone
//! broadcast channel. Wrap any nameservice in [`NotifyingNameService`] to
//! get automatic event emission after successful writes.
//!
//! # Implementations
//!
//! - [`MemoryNameService`]: In-memory implementation for testing
//! - [`FileNameService`]: File-based implementation using ns@v2 format
//! - [`StorageNameService`]: Storage-backed implementation using CAS operations

mod error;
mod event_bus;
#[cfg(feature = "native")]
pub mod file;
pub mod ledger_config;
pub mod memory;
mod notifying;
pub(crate) mod ns_format;
pub mod storage_ns;
pub mod tracking;
#[cfg(feature = "native")]
pub mod tracking_file;

pub use error::{NameServiceError, Result};
pub use event_bus::LedgerEventBus;
pub use ledger_config::{AuthRequirement, LedgerConfig, Origin, ReplicationDefaults};
pub use notifying::NotifyingNameService;

use fluree_db_core::StorageExtError;

/// Convert a serde_json error to a StorageExtError for use inside CAS closures.
fn json_ext_err(e: serde_json::Error) -> StorageExtError {
    StorageExtError::other(e.to_string())
}

/// Deserialize JSON bytes inside a CAS closure.
pub(crate) fn deserialize_json<T: for<'de> Deserialize<'de>>(
    data: &[u8],
) -> std::result::Result<T, StorageExtError> {
    serde_json::from_slice(data).map_err(json_ext_err)
}

/// Serialize a value to pretty-printed JSON bytes inside a CAS closure.
pub(crate) fn serialize_json<T: Serialize>(
    value: &T,
) -> std::result::Result<Vec<u8>, StorageExtError> {
    serde_json::to_vec_pretty(value).map_err(json_ext_err)
}

/// Check CAS expectation against the current value.
///
/// Returns `Some(conflict_result)` if there is a mismatch, `None` if the
/// expectation is satisfied and the caller should proceed with the write.
///
/// `allow_create` controls the `(None, None)` case: if `true`, creating a
/// new record when none exists is allowed; if `false`, it's a conflict.
pub(crate) fn check_cas_expectation<T: Clone, R>(
    expected: &Option<T>,
    current: &Option<T>,
    allow_create: bool,
    eq: impl Fn(&T, &T) -> bool,
    conflict: impl Fn(Option<T>) -> R,
) -> Option<R> {
    match (expected, current) {
        (None, None) => {
            if allow_create {
                None
            } else {
                Some(conflict(None))
            }
        }
        (None, Some(actual)) => Some(conflict(Some(actual.clone()))),
        (Some(_), None) => Some(conflict(None)),
        (Some(exp), Some(actual)) => {
            if eq(exp, actual) {
                None
            } else {
                Some(conflict(Some(actual.clone())))
            }
        }
    }
}

/// Compare two `RefValue`s by ContentId identity (ignoring `t`).
pub(crate) fn ref_values_match(a: &RefValue, b: &RefValue) -> bool {
    match (&a.id, &b.id) {
        (Some(x), Some(y)) => x == y,
        (None, None) => true,
        _ => false,
    }
}

/// Storage path segment for graph source artifacts.
///
/// Used when constructing storage addresses for BM25, vector, and other graph
/// source index artifacts, e.g. `fluree:file://graph-sources/{name}/{branch}/bm25/...`.
pub const STORAGE_SEGMENT_GRAPH_SOURCES: &str = "graph-sources";
pub use storage_ns::StorageNameService;
pub use tracking::{MemoryTrackingStore, RemoteName, RemoteTrackingStore, TrackingRecord};
#[cfg(feature = "native")]
pub use tracking_file::FileTrackingStore;

use async_trait::async_trait;
use fluree_db_core::{format_ledger_id, ContentId};
use fluree_vocab::ns_types;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::broadcast;

/// Parse a default_context value.
///
/// **CID-only**: default context identities are always CID multibase strings
/// (e.g., `"bafy..."`). Legacy storage address strings are not supported.
pub fn parse_default_context_value(s: &str) -> Option<ContentId> {
    s.parse::<ContentId>().ok()
}

/// Nameservice record containing ledger metadata
///
/// This struct preserves the distinction between the ledger_id (canonical ledger:branch)
/// and the ledger name (without branch suffix).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NsRecord {
    /// Canonical ledger ID with branch (e.g., "mydb:main")
    ///
    /// This is the primary cache key and the fully-qualified identifier.
    /// Use this for cache lookups and as the canonical form.
    pub ledger_id: String,

    /// Ledger name without branch suffix (e.g., "mydb")
    pub name: String,

    /// Branch name (e.g., "main")
    pub branch: String,

    /// Content identifier for the head commit.
    /// This is the authoritative identity for CAS comparisons
    /// and commit-chain integrity checks.
    #[serde(default)]
    pub commit_head_id: Option<ContentId>,

    /// Transaction time of latest commit
    pub commit_t: i64,

    /// Content identifier for the head index root.
    /// This is the authoritative identity for index lookups.
    #[serde(default)]
    pub index_head_id: Option<ContentId>,

    /// Transaction time of latest index
    pub index_t: i64,

    /// Content identifier for the default JSON-LD context blob.
    #[serde(default)]
    pub default_context: Option<ContentId>,

    /// Whether this ledger has been retracted
    pub retracted: bool,

    /// Content identifier for the ledger configuration object (origin discovery).
    /// Points to a content-addressed `LedgerConfig` blob that describes
    /// origins, auth requirements, and replication defaults.
    #[serde(default)]
    pub config_id: Option<ContentId>,

    /// The branch this was created from (e.g., "main"). `None` for the
    /// initial branch. The divergence point is computed on demand by walking
    /// the commit chains rather than being stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_branch: Option<String>,

    /// Number of child branches that were created from this branch.
    /// Used for safe deletion: a branch with children cannot be fully purged
    /// until all children are dropped.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub branches: u32,
}

pub(crate) fn is_zero(v: &u32) -> bool {
    *v == 0
}

impl NsRecord {
    /// Create a new NsRecord with minimal required fields
    pub fn new(name: impl Into<String>, branch: impl Into<String>) -> Self {
        let name = name.into();
        let branch = branch.into();
        let ledger_id = format_ledger_id(&name, &branch);

        Self {
            ledger_id,
            name,
            branch,
            commit_head_id: None,
            commit_t: 0,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: false,
            config_id: None,
            source_branch: None,
            branches: 0,
        }
    }

    /// Check if this record has an index
    pub fn has_index(&self) -> bool {
        self.index_head_id.is_some()
    }

    /// Check if there are commits newer than the index
    pub fn has_novelty(&self) -> bool {
        self.commit_t > self.index_t
    }
}

// ============================================================================
// Graph Source types
// ============================================================================

/// Broad capability category for a graph source.
///
/// This provides a first-class way to distinguish *what kind of source*
/// a graph source is, without matching on every specific backend type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GraphSourceKind {
    /// Default/named graphs stored in a ledger (RDF triple store)
    Ledger,
    /// Persisted indexes queried through graph-integrated patterns (BM25, Vector/HNSW, Geo)
    Index,
    /// Non-ledger data mapped into an RDF-shaped graph (Iceberg, R2RML/JDBC)
    Mapped,
}

/// Specific backend type for a graph source.
///
/// Each variant maps to a concrete implementation that knows how to build,
/// query, and sync a particular kind of graph source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphSourceType {
    /// BM25 full-text search index
    Bm25,
    /// Vector similarity search index (HNSW)
    Vector,
    /// S2 geospatial index
    Geo,
    /// R2RML relational mapping
    R2rml,
    /// Apache Iceberg table
    Iceberg,
    /// Unknown/custom graph source type
    Unknown(String),
}

impl GraphSourceType {
    /// Get the broad capability category for this source type.
    pub fn kind(&self) -> GraphSourceKind {
        match self {
            GraphSourceType::Bm25 | GraphSourceType::Vector | GraphSourceType::Geo => {
                GraphSourceKind::Index
            }
            GraphSourceType::R2rml | GraphSourceType::Iceberg => GraphSourceKind::Mapped,
            GraphSourceType::Unknown(_) => GraphSourceKind::Index, // default assumption
        }
    }

    /// Convert to the compact JSON-LD @type string using `f:` prefix.
    ///
    /// Returns the compact form (e.g., `"f:Bm25Index"`) suitable for use in
    /// JSON files where the `@context` provides `{"f": "https://ns.flur.ee/db#"}`.
    pub fn to_type_string(&self) -> String {
        match self {
            GraphSourceType::Bm25 => "f:Bm25Index".to_string(),
            GraphSourceType::Vector => "f:HnswIndex".to_string(),
            GraphSourceType::Geo => "f:GeoIndex".to_string(),
            GraphSourceType::R2rml => "f:R2rmlMapping".to_string(),
            GraphSourceType::Iceberg => "f:IcebergMapping".to_string(),
            GraphSourceType::Unknown(s) => s.clone(),
        }
    }

    /// Parse from a JSON-LD @type string.
    ///
    /// Accepts compact (`f:Bm25Index`) and full IRI
    /// (`https://ns.flur.ee/db#Bm25Index`) forms, plus fuzzy matching as fallback.
    pub fn from_type_string(s: &str) -> Self {
        match s {
            // Compact forms (primary, used in ns@v2 files)
            "f:Bm25Index" => GraphSourceType::Bm25,
            "f:HnswIndex" => GraphSourceType::Vector,
            "f:GeoIndex" => GraphSourceType::Geo,
            "f:R2rmlMapping" => GraphSourceType::R2rml,
            "f:IcebergMapping" => GraphSourceType::Iceberg,
            // Full IRI forms
            ns_types::BM25_INDEX => GraphSourceType::Bm25,
            ns_types::HNSW_INDEX => GraphSourceType::Vector,
            ns_types::GEO_INDEX => GraphSourceType::Geo,
            ns_types::R2RML_MAPPING => GraphSourceType::R2rml,
            ns_types::ICEBERG_MAPPING => GraphSourceType::Iceberg,
            _ => GraphSourceType::Unknown(s.to_string()),
        }
    }
}

/// Graph source nameservice record
///
/// Holds metadata for non-ledger graph sources (BM25, Vector, Geo, R2RML, Iceberg, etc.)
/// stored in the nameservice. Graph source records are separate from ledger records but
/// follow a similar ns@v2 storage pattern.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSourceRecord {
    /// Canonical identifier for this graph source (e.g., "my-search:main")
    pub graph_source_id: String,

    /// Base name of the graph source (e.g., "my-search")
    pub name: String,

    /// Branch name (e.g., "main")
    pub branch: String,

    /// Graph source type (BM25, Vector, Geo, R2RML, Iceberg, etc.)
    pub source_type: GraphSourceType,

    /// Configuration as JSON string (parsed by graph source implementation)
    pub config: String,

    /// Dependent ledger IDs (e.g., ["source-ledger:main"])
    pub dependencies: Vec<String>,

    /// Content identifier for the index snapshot (if any)
    pub index_id: Option<ContentId>,

    /// Index watermark (transaction time of indexed data)
    pub index_t: i64,

    /// Whether this graph source has been retracted
    pub retracted: bool,
}

impl GraphSourceRecord {
    /// Create a new GraphSourceRecord with required fields
    pub fn new(
        name: impl Into<String>,
        branch: impl Into<String>,
        source_type: GraphSourceType,
        config: impl Into<String>,
        dependencies: Vec<String>,
    ) -> Self {
        let name = name.into();
        let branch = branch.into();
        let graph_source_id = format_ledger_id(&name, &branch);

        Self {
            graph_source_id,
            name,
            branch,
            source_type,
            config: config.into(),
            dependencies,
            index_id: None,
            index_t: 0,
            retracted: false,
        }
    }

    /// Check if this is a BM25 graph source
    pub fn is_bm25(&self) -> bool {
        matches!(self.source_type, GraphSourceType::Bm25)
    }

    /// Check if this is a Vector graph source
    pub fn is_vector(&self) -> bool {
        matches!(self.source_type, GraphSourceType::Vector)
    }

    /// Check if this graph source has an index
    pub fn has_index(&self) -> bool {
        self.index_id.is_some()
    }
}

/// Result of looking up a nameservice record
///
/// Can be either a ledger record or a graph source record.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NsLookupResult {
    /// A ledger record
    Ledger(NsRecord),
    /// A graph source record (non-ledger)
    GraphSource(GraphSourceRecord),
    /// Record not found
    NotFound,
}

/// Read-only nameservice lookup trait
///
/// Implementations provide ledger discovery by ledger ID.
#[async_trait]
pub trait NameService:
    GraphSourceLookup + RefLookup + StatusLookup + ConfigLookup + Debug + Send + Sync
{
    /// Look up a ledger by its ledger ID (e.g. "mydb:main")
    ///
    /// Returns `None` if the ledger is not found.
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>>;

    /// Get all known ledger records
    ///
    /// Used for building in-memory query indexes over the nameservice.
    async fn all_records(&self) -> Result<Vec<NsRecord>>;

    /// List all branches for a given ledger name.
    ///
    /// Returns the [`NsRecord`] for every non-retracted branch that shares the
    /// given base name (e.g., passing `"mydb"` returns records for `"mydb:main"`,
    /// `"mydb:feature-x"`, etc.).
    ///
    /// The default implementation filters [`all_records`](Self::all_records);
    /// backends may override for efficiency.
    async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        let all = self.all_records().await?;
        Ok(all
            .into_iter()
            .filter(|r| r.name == ledger_name && !r.retracted)
            .collect())
    }

    /// Create a new branch for a ledger.
    ///
    /// Creates a new [`NsRecord`] for `ledger_name:new_branch` with its
    /// [`source_branch`](NsRecord::source_branch) set to record the parent.
    /// The new branch starts at the same commit head as the source, so
    /// subsequent transactions on either branch will diverge independently.
    ///
    /// Also increments the source branch's `branches` count to track
    /// the child reference for safe deletion.
    ///
    /// # Errors
    /// Returns [`LedgerAlreadyExists`](NameServiceError::LedgerAlreadyExists)
    /// if the branch already exists.
    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
    ) -> Result<()>;

    /// Drop a branch, purging its nameservice record and decrementing
    /// the parent branch's child count.
    ///
    /// Returns `Some(new_count)` with the parent's updated `branches` count
    /// if the dropped branch had a parent, or `None` if it had no parent
    /// (i.e., was the root branch).
    ///
    /// # Errors
    /// Returns [`NotFound`](NameServiceError::NotFound) if the branch
    /// record does not exist.
    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>>;

    /// Force-reset a branch's commit head and index head to a previously
    /// captured snapshot.
    ///
    /// Unlike [`Publisher::publish_commit`] and [`Publisher::publish_index`],
    /// this bypasses monotonic guards — the new `t` values may be lower than
    /// the current ones. Used to roll back a branch after a failed rebase.
    ///
    /// # Errors
    /// Returns [`NotFound`](NameServiceError::NotFound) if the branch does not exist.
    async fn reset_head(&self, ledger_id: &str, snapshot: NsRecordSnapshot) -> Result<()>;
}

/// Captured state of an `NsRecord` for rollback purposes.
///
/// Contains only the fields that `reset_head` restores — commit head,
/// index head.
#[derive(Clone, Debug)]
pub struct NsRecordSnapshot {
    pub commit_head_id: Option<ContentId>,
    pub commit_t: i64,
    pub index_head_id: Option<ContentId>,
    pub index_t: i64,
}

impl NsRecordSnapshot {
    /// Capture the restorable fields from an `NsRecord`.
    pub fn from_record(record: &NsRecord) -> Self {
        Self {
            commit_head_id: record.commit_head_id.clone(),
            commit_t: record.commit_t,
            index_head_id: record.index_head_id.clone(),
            index_t: record.index_t,
        }
    }
}

/// Publisher trait for writing nameservice records
///
/// Implementations handle publishing commit and index updates with
/// monotonic guarantees.
#[async_trait]
pub trait Publisher: Debug + Send + Sync {
    /// Initialize a new ledger in the nameservice
    ///
    /// Creates a minimal NsRecord for a new ledger with no commits yet.
    /// Only succeeds if no record exists for this ledger ID.
    ///
    /// # Arguments
    /// * `ledger_id` - The normalized ledger ID (e.g., "mydb:main")
    ///
    /// # Errors
    /// Returns an error if a record already exists (including retracted records).
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()>;

    /// Publish a new commit
    ///
    /// Only updates if: `(not exists) OR (new_t > existing_t)`
    ///
    /// This is called by the transactor after each successful commit.
    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()>;

    /// Publish a new index
    ///
    /// Only updates if: `(not exists) OR (new_t > existing_t)` - STRICTLY monotonic.
    ///
    /// This is called by the indexer after successfully writing new index roots.
    /// The index is published to a separate file/attribute to avoid contention
    /// with commit publishing.
    ///
    /// Note: "equal t prefers index file" is a READ-TIME merge rule, not a write rule.
    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()>;

    /// Retract a ledger (soft drop)
    ///
    /// Marks the ledger as retracted. Future lookups will return the record
    /// with `retracted: true`. The record is preserved so the alias cannot
    /// be reused until `purge` is called.
    async fn retract(&self, ledger_id: &str) -> Result<()>;

    /// Purge a ledger record entirely (hard drop)
    ///
    /// Removes the nameservice record so the alias can be reused.
    /// Default implementation falls back to `retract` for backends
    /// that don't support full removal.
    async fn purge(&self, ledger_id: &str) -> Result<()> {
        self.retract(ledger_id).await
    }

    /// Get the publishing ledger ID for a ledger.
    ///
    /// Returns `None` for "private" publishing (don't write ns field to commit).
    /// Returns `Some(ledger_id)` for the value to write into commit's ns field.
    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String>;
}

/// Combined read-write nameservice trait.
///
/// A convenience super-trait for components that need both `NameService`
/// (lookup) and `Publisher` (write) access via a single `dyn` reference.
/// All types that implement both `NameService` and `Publisher` automatically
/// implement this trait via the blanket impl.
pub trait ReadWriteNameService: NameService + Publisher {}

impl<T> ReadWriteNameService for T where T: NameService + Publisher {}

/// Admin-level publisher operations
///
/// Unlike `Publisher`, these methods allow non-monotonic updates
/// for admin operations like reindexing.
#[async_trait]
pub trait AdminPublisher: Publisher {
    /// Publish index, allowing overwrite when t == existing_t
    ///
    /// Unlike `publish_index()` which enforces strict monotonicity (new_t > existing_t),
    /// this method allows overwriting when t == existing_t. This is needed for admin
    /// operations like `reindex()` where we rebuild to the same t with a new root.
    ///
    /// Note: This does NOT allow t < existing_t to preserve invariants for time-travel
    /// and snapshot history.
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()>;
}

/// Read-only graph source lookup trait.
///
/// Provides discovery of graph source records without write capability.
/// This is a supertrait of `NameService` so that query execution can
/// detect and resolve graph sources transparently.
#[async_trait]
pub trait GraphSourceLookup: Debug + Send + Sync {
    /// Look up a graph source by its graph_source_id (e.g. "my-search:main").
    ///
    /// Returns `None` if not found or if the record is a ledger (not a graph source).
    async fn lookup_graph_source(&self, graph_source_id: &str)
        -> Result<Option<GraphSourceRecord>>;

    /// Look up any record (ledger or graph source) and return unified result.
    ///
    /// `resource_id` can be either a ledger_id or graph_source_id.
    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult>;

    /// Get all known graph source records
    ///
    /// Used for building in-memory query indexes over the nameservice.
    /// Returns all graph source records including retracted ones (callers can filter by status).
    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>>;
}

/// Graph source publisher trait (read + write).
///
/// Extends `GraphSourceLookup` with publish/retract operations.
/// Required by APIs that create, update, or drop graph sources.
#[async_trait]
pub trait GraphSourcePublisher: GraphSourceLookup {
    /// Publish a graph source configuration record
    ///
    /// Creates or updates the graph source config in nameservice. This stores the
    /// definition (type, config, dependencies) but NOT the index state.
    ///
    /// The config record is stored at `ns@v2/{name}/{branch}.json`.
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()>;

    /// Update graph source index head pointer
    ///
    /// Only updates if: `new_index_t > existing_index_t` (strictly monotonic).
    ///
    /// The index record is stored at `ns@v2/{name}/{branch}.index.json`,
    /// separate from the config record to avoid contention.
    ///
    /// Config updates must NOT reset index watermark.
    /// Index updates must NOT rewrite config.
    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()>;

    /// Retract a graph source
    ///
    /// Marks the graph source as retracted. Future lookups will return the record
    /// with `retracted: true`.
    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()>;
}

/// Subscription scope for filtering nameservice events.
///
/// Determines which events a subscriber will receive:
/// - `ResourceId(String)` - Only events matching this specific ledger_id or graph_source_id
/// - `All` - All events from any ledger or graph source
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionScope {
    /// Subscribe to events for a specific resource (ledger_id or graph_source_id)
    ResourceId(String),
    /// Subscribe to all events (all ledgers and graph sources)
    All,
}

impl SubscriptionScope {
    /// Create a scope for a specific resource ID (ledger_id or graph_source_id)
    pub fn resource_id(id: impl Into<String>) -> Self {
        Self::ResourceId(id.into())
    }

    /// Create a scope for all events
    pub fn all() -> Self {
        Self::All
    }

    /// Check if this scope matches a given event's resource_id
    pub fn matches(&self, event_resource_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::ResourceId(id) => id == event_resource_id,
        }
    }
}

/// Nameservice event emitted when records change.
///
/// These events are **in-process only** (they are not persisted, and they do not
/// automatically propagate across multiple processes/machines even if the
/// nameservice backend is file/storage based).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NameServiceEvent {
    /// A ledger commit head was advanced.
    LedgerCommitPublished {
        ledger_id: String,
        commit_id: ContentId,
        commit_t: i64,
    },
    /// A ledger index head was advanced.
    LedgerIndexPublished {
        ledger_id: String,
        index_id: ContentId,
        index_t: i64,
    },
    /// A ledger was retracted.
    LedgerRetracted { ledger_id: String },
    /// A graph source config was published/updated.
    GraphSourceConfigPublished {
        graph_source_id: String,
        source_type: GraphSourceType,
        dependencies: Vec<String>,
    },
    /// A graph source index head pointer was advanced.
    GraphSourceIndexPublished {
        graph_source_id: String,
        index_id: ContentId,
        index_t: i64,
    },
    /// A graph source was retracted.
    GraphSourceRetracted { graph_source_id: String },
}

/// Subscription handle for receiving ledger updates
#[derive(Debug)]
pub struct Subscription {
    /// The subscription scope (resource_id or all)
    pub scope: SubscriptionScope,
    /// Receiver for nameservice events (in-process).
    pub receiver: broadcast::Receiver<NameServiceEvent>,
}

// ---------------------------------------------------------------------------
// Ref-level CAS (compare-and-set) types and trait
// ---------------------------------------------------------------------------

/// Which ref is being read or updated.
///
/// `Copy` — small enum, pass by value at call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RefKind {
    /// The commit head pointer (`f:commit` + `f:t` in ns@v2).
    CommitHead,
    /// The index head pointer (`f:index` + `f:indexT` in ns@v2).
    IndexHead,
}

/// A ref value: identity + transaction-time watermark.
///
/// Semantics when returned from [`RefPublisher::get_ref`]:
/// - `Some(RefValue { id: None, t: 0 })` — ref exists but is "unborn"
///   (ledger initialised, no commit yet — analogous to git's unborn HEAD).
/// - `Some(RefValue { id: Some(..), .. })` — ref exists with a CID identity.
/// - `None` (at the `Option` level) — ledger ID/ref is completely unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValue {
    /// Content identifier — the **identity** of the referenced object.
    pub id: Option<ContentId>,
    /// Monotonic watermark (transaction time).
    pub t: i64,
}

/// Outcome of a compare-and-set operation.
///
/// Conflicts are **not errors** — they are expected outcomes of concurrent
/// writes and must be handled by the caller (retry, report, etc.).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult {
    /// CAS succeeded — the ref was updated to the new value.
    Updated,
    /// CAS failed — `expected` did not match the current value.
    /// `actual` carries the current ref (if any) so the caller can decide
    /// what to do next (retry, diverge, etc.).
    Conflict { actual: Option<RefValue> },
}

/// Read-only ref access for ledger head pointers.
///
/// Provides `get_ref` for reading commit/index head refs.
/// This is the read-only counterpart to [`RefPublisher`].
#[async_trait]
pub trait RefLookup: Debug + Send + Sync {
    /// Read the current ref value for a ledger ID + kind.
    ///
    /// Returns:
    /// - `Some(RefValue { id: None, t: 0 })` — ref exists, unborn
    /// - `Some(RefValue { id: Some(..), .. })` — ref exists with CID identity
    /// - `None` — ledger ID/ref completely unknown
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>>;
}

/// Explicit ref-level CAS operations for sync.
///
/// CAS compares on **identity** via `id` (ContentId). `t` serves as a
/// **kind-dependent monotonic guard**:
///
/// | Kind         | Guard             | Rationale                          |
/// |--------------|-------------------|------------------------------------|
/// | `CommitHead` | `new.t > cur.t`   | No two commits share a `t`.        |
/// | `IndexHead`  | `new.t >= cur.t`  | Re-index at same `t` is allowed.   |
///
/// "Fast-forward" in Fluree is defined by `t`-ordering, **not** commit
/// ancestry.  If ancestry-based FF is ever needed, commit parent links and a
/// graph walk would be required — that is out of scope here.
#[async_trait]
pub trait RefPublisher: RefLookup {
    /// Atomic compare-and-set.
    ///
    /// Updates the ref **only if** the current identity matches `expected`.
    /// Pass `expected = None` for initial creation (ref must not exist).
    ///
    /// The kind-dependent monotonic guard is also checked:
    /// - `CommitHead`: `new.t > current.t`
    /// - `IndexHead`: `new.t >= current.t`
    ///
    /// Returns [`CasResult::Conflict`] (with the actual value) on mismatch.
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult>;

    /// Fast-forward the commit head with a retry loop.
    ///
    /// Succeeds only when `new.t > current.t` (strict monotonicity).
    /// On CAS conflict from a concurrent writer the method re-reads the
    /// current ref and retries if the update is still a fast-forward.
    /// Returns [`CasResult::Conflict`] once it determines the ref has
    /// diverged (`current.t >= new.t` after re-read).
    async fn fast_forward_commit(
        &self,
        ledger_id: &str,
        new: &RefValue,
        max_retries: usize,
    ) -> Result<CasResult> {
        for _ in 0..max_retries {
            let current = self.get_ref(ledger_id, RefKind::CommitHead).await?;

            // Check whether fast-forward is still possible.
            if let Some(ref cur) = current {
                if new.t <= cur.t {
                    return Ok(CasResult::Conflict { actual: current });
                }
            }

            match self
                .compare_and_set_ref(ledger_id, RefKind::CommitHead, current.as_ref(), new)
                .await?
            {
                CasResult::Updated => return Ok(CasResult::Updated),
                CasResult::Conflict { actual } => {
                    // Another writer advanced the ref — still FF-able?
                    if let Some(ref a) = actual {
                        if new.t <= a.t {
                            return Ok(CasResult::Conflict { actual });
                        }
                    }
                    // Retry — next iteration re-reads current.
                    continue;
                }
            }
        }
        // Exhausted retries — return latest known state.
        let current = self.get_ref(ledger_id, RefKind::CommitHead).await?;
        Ok(CasResult::Conflict { actual: current })
    }
}

// ---------------------------------------------------------------------------
// V2 Concern Types (Status and Config extensions)
// ---------------------------------------------------------------------------

/// Which concern is being read or updated (v2 extension).
///
/// Extends the concept of `RefKind` to include Status and Config concerns.
/// Head and Index concerns map directly to `RefKind::CommitHead` and
/// `RefKind::IndexHead` respectively.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConcernKind {
    /// Commit head pointer - equivalent to RefKind::CommitHead
    Head,
    /// Index state - equivalent to RefKind::IndexHead
    Index,
    /// Status state (queue depth, locks, progress, etc.)
    Status,
    /// Config state (default context, settings)
    Config,
}

impl ConcernKind {
    /// Convert to RefKind if applicable (Head/Index only).
    ///
    /// Returns `None` for Status and Config since they don't map to RefKind.
    pub fn as_ref_kind(&self) -> Option<RefKind> {
        match self {
            ConcernKind::Head => Some(RefKind::CommitHead),
            ConcernKind::Index => Some(RefKind::IndexHead),
            ConcernKind::Status | ConcernKind::Config => None,
        }
    }
}

/// Status payload with extensible metadata.
///
/// The `state` field contains the primary status (e.g., "ready", "indexing", "error").
/// Additional metadata can be stored in `extra` using `#[serde(flatten)]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusPayload {
    /// Primary state value (e.g., "ready", "init", "indexing", "error", "retracted")
    pub state: String,

    /// Extensible metadata (queue_depth, locks, progress, error messages, etc.)
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl StatusPayload {
    /// Create a new status payload with just a state
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
            extra: std::collections::HashMap::new(),
        }
    }

    /// Create a status payload with state and extra metadata
    pub fn with_extra(
        state: impl Into<String>,
        extra: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            state: state.into(),
            extra,
        }
    }

    /// Check if the status indicates "ready" state
    pub fn is_ready(&self) -> bool {
        self.state == "ready"
    }

    /// Check if the status indicates "retracted" state
    pub fn is_retracted(&self) -> bool {
        self.state == "retracted"
    }
}

impl Default for StatusPayload {
    fn default() -> Self {
        Self::new("ready")
    }
}

/// Config payload with known fields + extensibility.
///
/// Contains common config fields like `default_context`, with additional
/// settings stored in `extra`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConfigPayload {
    /// Content identifier for the default JSON-LD context blob.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_context: Option<ContentId>,

    /// Content ID of the LedgerConfig blob (origin discovery).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_id: Option<ContentId>,

    /// Additional config (index_threshold, replication settings, etc.)
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl ConfigPayload {
    /// Create a new empty config payload
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config payload with a default context CID
    pub fn with_default_context(cid: ContentId) -> Self {
        Self {
            default_context: Some(cid),
            config_id: None,
            extra: std::collections::HashMap::new(),
        }
    }
}

/// Status concern value (watermark + payload).
///
/// The watermark `v` is a monotonically increasing counter that changes
/// on every status update. Status always has a payload (never unborn).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusValue {
    /// Watermark (monotonically increasing version counter)
    pub v: i64,
    /// Status payload (always present)
    pub payload: StatusPayload,
}

impl StatusValue {
    /// Create a new status value
    pub fn new(v: i64, payload: StatusPayload) -> Self {
        Self { v, payload }
    }

    /// Create initial status value (v=1, state="ready")
    pub fn initial() -> Self {
        Self {
            v: 1,
            payload: StatusPayload::default(),
        }
    }
}

/// Config concern value (watermark + optional payload).
///
/// The watermark `v` is a monotonically increasing counter. Config can be
/// "unborn" (v=0, payload=None) if no config has been set yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValue {
    /// Watermark (monotonically increasing version counter)
    pub v: i64,
    /// Config payload (None if unborn)
    pub payload: Option<ConfigPayload>,
}

impl ConfigValue {
    /// Create a new config value
    pub fn new(v: i64, payload: Option<ConfigPayload>) -> Self {
        Self { v, payload }
    }

    /// Create an unborn config value (v=0, no payload)
    pub fn unborn() -> Self {
        Self {
            v: 0,
            payload: None,
        }
    }

    /// Check if this config is unborn (no config set yet)
    pub fn is_unborn(&self) -> bool {
        self.v == 0 && self.payload.is_none()
    }
}

/// Result of a compare-and-set operation for status.
///
/// Conflicts are NOT errors — they indicate the expected value didn't match
/// the current value. The caller should handle conflicts by examining `actual`
/// and deciding whether to retry or report divergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatusCasResult {
    /// CAS succeeded — status was updated to the new value
    Updated,
    /// CAS failed — expected didn't match current.
    /// `actual` contains the current status value (if record exists).
    Conflict { actual: Option<StatusValue> },
}

/// Result of a compare-and-set operation for config.
///
/// Conflicts are NOT errors — they indicate the expected value didn't match
/// the current value. The caller should handle conflicts by examining `actual`
/// and deciding whether to retry or report divergence.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ConfigCasResult {
    /// CAS succeeded — config was updated to the new value
    Updated,
    /// CAS failed — expected didn't match current.
    /// `actual` contains the current config value (if record exists).
    Conflict { actual: Option<ConfigValue> },
}

// ---------------------------------------------------------------------------
// V2 Publisher Traits (Status and Config)
// ---------------------------------------------------------------------------

/// Read-only status access.
///
/// Provides `get_status` for reading ledger operational status.
/// This is the read-only counterpart to [`StatusPublisher`].
#[async_trait]
pub trait StatusLookup: Debug + Send + Sync {
    /// Get current status for a ledger ID.
    ///
    /// Returns:
    /// - `Some(StatusValue)` — record exists with status
    /// - `None` — record doesn't exist at all
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>>;
}

/// Publisher for status concern (v2 extension).
///
/// Status tracks operational metadata like queue depth, locks, progress,
/// and error states. It uses a monotonically increasing watermark and
/// CAS semantics for coordination.
///
/// Status always exists once a record is created (initial state is "ready" with v=1).
#[async_trait]
pub trait StatusPublisher: StatusLookup {
    /// Push status with CAS semantics.
    ///
    /// Updates only if current matches expected. Returns conflict with actual on mismatch.
    ///
    /// # Arguments
    /// * `ledger_id` - The ledger ID
    /// * `expected` - The expected current status (`None` for initial creation)
    /// * `new` - The new status to set (must have `new.v > expected.v`)
    ///
    /// # Returns
    /// - `Updated` — successfully updated
    /// - `Conflict { actual }` — current didn't match expected
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult>;
}

/// Read-only config access.
///
/// Provides `get_config` for reading ledger configuration.
/// This is the read-only counterpart to [`ConfigPublisher`].
#[async_trait]
pub trait ConfigLookup: Debug + Send + Sync {
    /// Get current config for a ledger ID.
    ///
    /// Returns:
    /// - `Some(ConfigValue)` — record exists (may be unborn with v=0)
    /// - `None` — record doesn't exist at all
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>>;
}

/// Publisher for config concern (v2 extension).
///
/// Config tracks settings like default context, index thresholds, and other
/// configuration options. It uses a monotonically increasing watermark and
/// CAS semantics.
///
/// Config can be "unborn" (v=0, payload=None) if no config has been set yet.
#[async_trait]
pub trait ConfigPublisher: ConfigLookup {
    /// Push config with CAS semantics.
    ///
    /// Updates only if current matches expected. Returns conflict with actual on mismatch.
    ///
    /// # Arguments
    /// * `ledger_id` - The ledger ID
    /// * `expected` - The expected current config (`None` for initial creation)
    /// * `new` - The new config to set (must have `new.v > expected.v`)
    ///
    /// # Returns
    /// - `Updated` — successfully updated
    /// - `Conflict { actual }` — current didn't match expected
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult>;
}

/// A dynamically-dispatched nameservice with full read-write capability.
///
/// This is the complete set of nameservice traits needed for full read-write
/// access to a nameservice. All concrete nameservice backends (File, Memory,
/// DynamoDB, S3 storage-backed) implement this automatically via the blanket
/// impl.
///
/// Use `Arc<dyn NameServicePublisher>` when a component needs ownership of a
/// nameservice that supports all operations.
pub trait NameServicePublisher:
    NameService
    + Publisher
    + AdminPublisher
    + RefPublisher
    + GraphSourcePublisher
    + StatusPublisher
    + ConfigPublisher
{
}

impl<T> NameServicePublisher for T where
    T: NameService
        + Publisher
        + AdminPublisher
        + RefPublisher
        + GraphSourcePublisher
        + StatusPublisher
        + ConfigPublisher
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    #[test]
    fn test_ns_record_new() {
        let record = NsRecord::new("mydb", "main");
        assert_eq!(record.name, "mydb");
        assert_eq!(record.branch, "main");
        assert_eq!(record.ledger_id, "mydb:main");
        assert_eq!(record.commit_t, 0);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[test]
    fn test_ns_record_has_novelty() {
        let mut record = NsRecord::new("mydb", "main");
        assert!(!record.has_novelty());

        record.commit_t = 10;
        record.index_t = 5;
        assert!(record.has_novelty());

        record.index_t = 10;
        assert!(!record.has_novelty());
    }

    #[test]
    fn test_graph_source_type_to_string() {
        // to_type_string returns compact "f:" prefixed forms
        assert_eq!(GraphSourceType::Bm25.to_type_string(), "f:Bm25Index");
        assert_eq!(GraphSourceType::Vector.to_type_string(), "f:HnswIndex");
        assert_eq!(GraphSourceType::Geo.to_type_string(), "f:GeoIndex");
        assert_eq!(GraphSourceType::R2rml.to_type_string(), "f:R2rmlMapping");
        assert_eq!(
            GraphSourceType::Iceberg.to_type_string(),
            "f:IcebergMapping"
        );
        assert_eq!(
            GraphSourceType::Unknown("https://example.com/Custom".to_string()).to_type_string(),
            "https://example.com/Custom"
        );
    }

    #[test]
    fn test_graph_source_type_from_string() {
        // Full IRI forms
        assert_eq!(
            GraphSourceType::from_type_string(ns_types::BM25_INDEX),
            GraphSourceType::Bm25
        );
        assert_eq!(
            GraphSourceType::from_type_string(ns_types::HNSW_INDEX),
            GraphSourceType::Vector
        );
        assert_eq!(
            GraphSourceType::from_type_string(ns_types::GEO_INDEX),
            GraphSourceType::Geo
        );
        assert_eq!(
            GraphSourceType::from_type_string(ns_types::R2RML_MAPPING),
            GraphSourceType::R2rml
        );
        assert_eq!(
            GraphSourceType::from_type_string(ns_types::ICEBERG_MAPPING),
            GraphSourceType::Iceberg
        );
        // Unknown types
        assert_eq!(
            GraphSourceType::from_type_string("https://example.com/Custom"),
            GraphSourceType::Unknown("https://example.com/Custom".to_string())
        );
    }

    #[test]
    fn test_graph_source_type_kind() {
        assert_eq!(GraphSourceType::Bm25.kind(), GraphSourceKind::Index);
        assert_eq!(GraphSourceType::Vector.kind(), GraphSourceKind::Index);
        assert_eq!(GraphSourceType::Geo.kind(), GraphSourceKind::Index);
        assert_eq!(GraphSourceType::R2rml.kind(), GraphSourceKind::Mapped);
        assert_eq!(GraphSourceType::Iceberg.kind(), GraphSourceKind::Mapped);
    }

    #[test]
    fn test_graph_source_record_new() {
        let record = GraphSourceRecord::new(
            "my-search",
            "main",
            GraphSourceType::Bm25,
            r#"{"k1": 1.2, "b": 0.75}"#,
            vec!["source-ledger:main".to_string()],
        );

        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.graph_source_id, "my-search:main");
        assert_eq!(record.source_type, GraphSourceType::Bm25);
        assert_eq!(record.config, r#"{"k1": 1.2, "b": 0.75}"#);
        assert_eq!(record.dependencies, vec!["source-ledger:main".to_string()]);
        assert_eq!(record.index_id, None);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[test]
    fn test_graph_source_record_is_bm25() {
        let bm25 = GraphSourceRecord::new("search", "main", GraphSourceType::Bm25, "{}", vec![]);
        let r2rml = GraphSourceRecord::new("mapping", "main", GraphSourceType::R2rml, "{}", vec![]);

        assert!(bm25.is_bm25());
        assert!(!r2rml.is_bm25());
    }

    #[test]
    fn test_graph_source_record_has_index() {
        let mut record =
            GraphSourceRecord::new("search", "main", GraphSourceType::Bm25, "{}", vec![]);
        assert!(!record.has_index());

        record.index_id = Some(ContentId::new(
            ContentKind::IndexRoot,
            b"test-graph-source-index",
        ));
        record.index_t = 42;
        assert!(record.has_index());
    }

    // ========== V2 Concern Type Tests ==========

    #[test]
    fn test_concern_kind_as_ref_kind() {
        assert_eq!(ConcernKind::Head.as_ref_kind(), Some(RefKind::CommitHead));
        assert_eq!(ConcernKind::Index.as_ref_kind(), Some(RefKind::IndexHead));
        assert_eq!(ConcernKind::Status.as_ref_kind(), None);
        assert_eq!(ConcernKind::Config.as_ref_kind(), None);
    }

    #[test]
    fn test_status_payload_new() {
        let status = StatusPayload::new("ready");
        assert_eq!(status.state, "ready");
        assert!(status.extra.is_empty());
        assert!(status.is_ready());
        assert!(!status.is_retracted());
    }

    #[test]
    fn test_status_payload_with_extra() {
        let mut extra = std::collections::HashMap::new();
        extra.insert("queue_depth".to_string(), serde_json::json!(5));
        extra.insert("last_commit_ms".to_string(), serde_json::json!(42));

        let status = StatusPayload::with_extra("indexing", extra);
        assert_eq!(status.state, "indexing");
        assert_eq!(status.extra.get("queue_depth"), Some(&serde_json::json!(5)));
        assert!(!status.is_ready());
    }

    #[test]
    fn test_status_payload_default() {
        let status = StatusPayload::default();
        assert_eq!(status.state, "ready");
        assert!(status.extra.is_empty());
    }

    #[test]
    fn test_config_payload_new() {
        let config = ConfigPayload::new();
        assert_eq!(config.default_context, None);
        assert!(config.extra.is_empty());
    }

    #[test]
    fn test_config_payload_with_default_context() {
        let cid = ContentId::new(ContentKind::LedgerConfig, b"test-context-data");
        let config = ConfigPayload::with_default_context(cid.clone());
        assert_eq!(config.default_context, Some(cid));
    }

    #[test]
    fn test_status_value_new() {
        let status = StatusValue::new(42, StatusPayload::new("ready"));
        assert_eq!(status.v, 42);
        assert_eq!(status.payload.state, "ready");
    }

    #[test]
    fn test_status_value_initial() {
        let status = StatusValue::initial();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[test]
    fn test_config_value_new() {
        let ctx_cid = ContentId::new(ContentKind::LedgerConfig, b"ctx");
        let config = ConfigValue::new(5, Some(ConfigPayload::with_default_context(ctx_cid)));
        assert_eq!(config.v, 5);
        assert!(config.payload.is_some());
        assert!(!config.is_unborn());
    }

    #[test]
    fn test_config_value_unborn() {
        let config = ConfigValue::unborn();
        assert_eq!(config.v, 0);
        assert!(config.payload.is_none());
        assert!(config.is_unborn());
    }

    #[test]
    fn test_status_cas_result() {
        let updated = StatusCasResult::Updated;
        let conflict = StatusCasResult::Conflict {
            actual: Some(StatusValue::initial()),
        };

        assert!(matches!(updated, StatusCasResult::Updated));
        assert!(matches!(conflict, StatusCasResult::Conflict { .. }));
    }

    #[test]
    fn test_config_cas_result() {
        let updated = ConfigCasResult::Updated;
        let conflict = ConfigCasResult::Conflict {
            actual: Some(ConfigValue::unborn()),
        };

        assert!(matches!(updated, ConfigCasResult::Updated));
        assert!(matches!(conflict, ConfigCasResult::Conflict { .. }));
    }
}
