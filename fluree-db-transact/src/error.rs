//! Transaction error types

use thiserror::Error;

/// Transaction errors
#[derive(Error, Debug)]
pub enum TransactError {
    /// Core database error
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Query error
    #[error("Query error: {0}")]
    Query(#[from] fluree_db_query::QueryError),

    /// Novelty error
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Ledger error
    #[error("Ledger error: {0}")]
    Ledger(#[from] fluree_db_ledger::LedgerError),

    /// Nameservice error
    #[error("Nameservice error: {0}")]
    Nameservice(#[from] fluree_db_nameservice::NameServiceError),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// JSON-LD processing error
    #[error("JSON-LD error: {0}")]
    JsonLd(#[from] fluree_graph_json_ld::JsonLdError),

    /// Transaction parsing error
    #[error("Parse error: {0}")]
    Parse(String),

    /// A syntactically valid transaction feature is not yet implemented.
    ///
    /// Distinct from [`Self::Parse`] (which is user-error) and
    /// [`fluree_db_query::QueryError::UnsupportedFeature`] (the read-side
    /// counterpart). M0 surface for edge annotations on inserts/updates.
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    /// SPARQL lowering error (structured, includes source spans)
    #[error("SPARQL lowering error: {0}")]
    SparqlLower(#[from] fluree_db_sparql::LowerError),

    /// Flake generation error
    #[error("Flake generation error: {0}")]
    FlakeGeneration(String),

    /// Commit conflict (concurrent modification)
    #[error("Commit conflict: expected t={expected_t}, head_t={head_t}")]
    CommitConflict { expected_t: i64, head_t: i64 },

    /// Commit ID mismatch during commit (head CID does not match expected)
    #[error("Commit ID mismatch: expected {expected}, found {found}")]
    CommitIdMismatch { expected: String, found: String },

    /// Commit lost the race while publishing the new head.
    #[error(
        "Commit publish race lost for ledger {ledger_id}: attempted t={attempted_t} commit={attempted_commit_id}, current head is t={published_t} commit={published_commit_id}"
    )]
    PublishLostRace {
        ledger_id: String,
        attempted_t: i64,
        attempted_commit_id: String,
        published_t: i64,
        published_commit_id: String,
    },

    /// Namespace allocations from a pre-built `Txn` (e.g. SPARQL UPDATE
    /// lowered against a stale snapshot) conflict with the staging
    /// registry. Retry-safe: re-lower against the latest snapshot.
    ///
    /// Triggered when two concurrent SPARQL UPDATEs lower against the same
    /// pre-commit snapshot, both pick the same first-time namespace code
    /// for *different* prefixes, and the second writer reaches staging
    /// after the first has committed.
    #[error(
        "Namespace allocation from pre-built Txn conflicts with the staging \
         registry (likely stale lowering against an out-of-date snapshot): {0}"
    )]
    NamespaceConflict(String),

    /// Ledger or branch has been retracted (soft-deleted)
    #[error("Ledger has been retracted: {0}")]
    Retracted(String),

    /// Empty transaction (no flakes to commit)
    #[error("Empty transaction: no flakes to commit")]
    EmptyTransaction,

    /// Invalid commit event time (unparseable, future-dated, or earlier
    /// than the head commit's event time)
    #[error("Invalid event time: {0}")]
    InvalidEventTime(String),

    /// Novelty at maximum size (backpressure)
    #[error("Novelty at maximum size, reindexing required")]
    NoveltyAtMax,

    /// Transaction would exceed maximum novelty size
    #[error("Transaction would exceed novelty limit: current={current_bytes}, delta={delta_bytes}, max={max_bytes}")]
    NoveltyWouldExceed {
        current_bytes: usize,
        delta_bytes: usize,
        max_bytes: usize,
    },

    /// Invalid template term
    #[error("Invalid template term: {0}")]
    InvalidTerm(String),

    /// Unbound variable in template
    #[error("Unbound variable in template: {0}")]
    UnboundVariable(String),

    /// Policy violation
    #[error("{0}")]
    PolicyViolation(#[from] fluree_db_policy::PolicyError),

    /// Commit codec error
    #[error("Commit codec error: {0}")]
    CommitCodec(#[from] crate::commit_v2::CommitCodecError),

    /// SHACL validation error (only available with `shacl` feature)
    #[cfg(feature = "shacl")]
    #[error("SHACL error: {0}")]
    Shacl(#[from] fluree_db_shacl::ShaclError),

    /// SHACL validation violation (only available with `shacl` feature)
    #[cfg(feature = "shacl")]
    #[error("{0}")]
    ShaclViolation(String),

    /// Transaction exceeded the configured max-fuel limit
    #[error("{0}")]
    FuelExceeded(#[from] fluree_db_core::FuelExceededError),

    /// Raw transaction upload failed (parallel `store_raw_txn` path).
    #[error("Raw transaction upload failed: {0}")]
    RawTxnUpload(String),

    /// Transaction violates a structural invariant of the staged
    /// flake set — e.g. an annotation SID would be attached to more
    /// than one edge in a single transaction without retracting the
    /// prior attachment. Distinct from [`Self::Parse`] (input syntax)
    /// and [`Self::UniqueConstraintViolation`] (schema-level
    /// `f:enforceUnique`).
    #[error("Transaction invariant violation: {0}")]
    InvariantViolation(String),

    /// A SPARQL/builder graph-management operation (CLEAR/DROP/COPY/MOVE/ADD)
    /// resolved a target, source, or destination to a reserved system graph:
    /// `urn:fluree:{ledger}#config` (g_id 2) or `#txn-meta` (g_id 1). These
    /// graphs are Fluree-internal — `#config` seeds SHACL/uniqueness governance
    /// and cross-ledger rules, `#txn-meta` holds commit metadata — and are never
    /// part of the W3C dataset, so they must never be a graph-management target.
    /// Mirrors the cross-ledger resolver's reserved-graph guard.
    #[error("graph-management operation targets reserved system graph <{graph_iri}>; refusing")]
    ReservedGraphTarget {
        /// The reserved graph IRI the operation attempted to target.
        graph_iri: String,
    },

    /// A SPARQL/builder graph-management transfer (ADD/COPY/MOVE) named a
    /// source graph that has never been registered — a typo'd or never-written
    /// IRI. Per SPARQL 1.1 Update §3.2, ADD/COPY/MOVE from a nonexistent source
    /// raise an error unless `SILENT`; without this guard COPY/MOVE clear the
    /// destination and copy nothing back in, silently emptying it (roadmap O3).
    /// An emptied-but-once-registered graph resolves to a real g_id (the
    /// registry is additive-only, D-6) and is a legitimate empty source — it
    /// proceeds, and does not raise this error.
    #[error("source graph <{graph_iri}> does not exist for ADD/COPY/MOVE; use SILENT to ignore")]
    SourceGraphNotFound {
        /// The missing source graph IRI the transfer attempted to read.
        graph_iri: String,
    },

    /// Unique constraint violation (`f:enforceUnique`).
    ///
    /// A property annotated with `f:enforceUnique true` has duplicate values
    /// within a single named graph.
    #[error(
        "Unique constraint violation: property <{property}> value \"{value}\" \
             already exists for subject <{existing_subject}> in graph {graph} \
             (conflicting subject: <{new_subject}>)"
    )]
    UniqueConstraintViolation {
        /// The property IRI that requires uniqueness.
        property: String,
        /// The duplicate value (display representation).
        value: String,
        /// The graph where the violation occurred (IRI or "default").
        graph: String,
        /// The subject that already holds this value.
        existing_subject: String,
        /// The new subject trying to assert this value.
        new_subject: String,
    },
}

/// Result type for transaction operations
pub type Result<T> = std::result::Result<T, TransactError>;
