//! Error types for the Fluree DB API

use crate::format::FormatError;
use thiserror::Error;

// ============================================================================
// Fan-out outcome tally
// ============================================================================

/// Per-target outcome counts for one fan-out materialize window.
///
/// The unit of materialize work is the TARGET, not the poll. A single job resolves to
/// N target ledgers and each can independently commit, defer on novelty backpressure,
/// or fail — so any counter measured in polls is measuring the wrong thing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TargetTally {
    /// Targets that committed (or had nothing to do and are up to date).
    pub ok: usize,
    /// Targets deferred by novelty backpressure. Self-heals on the next poll.
    pub deferred: usize,
    /// Targets that errored.
    pub failed: usize,
}

impl TargetTally {
    /// Total targets attempted in the window.
    pub fn total(&self) -> usize {
        self.ok + self.deferred + self.failed
    }

    /// True when every target reached the same successful outcome — the only case in
    /// which the shared watermark may advance.
    pub fn is_complete(&self) -> bool {
        self.deferred == 0 && self.failed == 0
    }
}

// ============================================================================
// Builder errors
// ============================================================================

/// Single builder validation error.
///
/// Builders accumulate these during setter calls and report them all at once
/// from `.validate()` or `.execute()`.
#[derive(Debug, Clone)]
pub enum BuilderError {
    /// A required field was not set.
    Missing {
        /// Field name (e.g., "input")
        field: &'static str,
        /// Human-readable hint (e.g., "call .jsonld() or .sparql()")
        hint: &'static str,
    },
    /// A mutually exclusive field was set more than once.
    Conflict {
        /// Field name (e.g., "input")
        field: &'static str,
        /// Description of the conflict
        message: String,
    },
    /// A field value is invalid for this builder context.
    Invalid {
        /// Field name
        field: &'static str,
        /// Description of the problem
        message: String,
    },
}

impl BuilderError {
    /// Stable error code string for API responses.
    ///
    /// Aligned with `fluree-vocab` error code conventions (`err:api/*`).
    pub fn error_code(&self) -> &'static str {
        match self {
            BuilderError::Missing { .. } => "err:api/MissingParameter",
            BuilderError::Conflict { .. } => "err:api/BadRequest",
            BuilderError::Invalid { .. } => "err:api/BadRequest",
        }
    }
}

impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuilderError::Missing { field, hint } => {
                write!(f, "missing required field '{field}': {hint}")
            }
            BuilderError::Conflict { field, message } => {
                write!(f, "conflict on field '{field}': {message}")
            }
            BuilderError::Invalid { field, message } => {
                write!(f, "invalid field '{field}': {message}")
            }
        }
    }
}

/// Aggregated builder validation errors.
///
/// Wraps all errors found during validation so that users see every problem
/// at once rather than fixing them one at a time.
#[derive(Debug, Clone)]
pub struct BuilderErrors(pub Vec<BuilderError>);

impl std::fmt::Display for BuilderErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.0.len();
        write!(
            f,
            "{} builder error{}: ",
            count,
            if count == 1 { "" } else { "s" }
        )?;
        for (i, err) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{err}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BuilderErrors {}

impl BuilderErrors {
    /// Check if there are any errors.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of errors.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// API error type that wraps errors from child crates
#[derive(Error, Debug)]
pub enum ApiError {
    /// Connection errors
    #[error("Connection error: {0}")]
    Connection(#[from] fluree_db_connection::ConnectionError),

    /// Query parsing errors
    #[error("Query parse error: {0}")]
    Parse(#[from] fluree_db_query::parse::ParseError),

    /// Query execution errors
    #[error("Query error: {0}")]
    Query(#[from] fluree_db_query::QueryError),

    /// Batch construction errors
    #[error("Batch error: {0}")]
    Batch(#[from] fluree_db_query::BatchError),

    /// Ledger errors
    #[error("Ledger error: {0}")]
    Ledger(#[from] fluree_db_ledger::LedgerError),

    /// Nameservice errors
    #[error("Nameservice error: {0}")]
    NameService(#[from] fluree_db_nameservice::NameServiceError),

    /// Transaction errors
    #[error("Transaction error: {0}")]
    Transact(#[from] fluree_db_transact::TransactError),

    /// SPARQL parse/validate errors (with structured diagnostics)
    #[error("SPARQL error: {message}")]
    Sparql {
        /// Human-readable error message
        message: String,
        /// Structured diagnostics with source spans
        diagnostics: Vec<fluree_db_sparql::Diagnostic>,
    },

    /// SPARQL lowering errors
    #[error("SPARQL lowering error: {0}")]
    SparqlLower(#[from] fluree_db_sparql::LowerError),

    /// Cypher parse errors with structured diagnostics.
    #[error("Cypher parse error: {message}")]
    Cypher {
        message: String,
        diagnostics: Vec<fluree_db_cypher::Diagnostic>,
    },

    /// Cypher lowering errors (read path).
    #[error("Cypher lowering error: {0}")]
    CypherLower(#[from] fluree_db_cypher::LowerError),

    /// Cypher write-path lowering errors.
    #[error("Cypher update lowering error: {0}")]
    CypherUpdateLower(#[from] fluree_db_transact::lower_cypher_update::LowerCypherError),

    /// Turtle parse errors
    #[error("Turtle parse error: {0}")]
    Turtle(#[from] fluree_graph_turtle::TurtleError),

    /// BM25 index builder errors
    #[error("BM25 builder error: {0}")]
    Bm25Builder(#[from] fluree_db_query::bm25::BuilderError),

    /// BM25 serialization errors
    #[error("BM25 serialization error: {0}")]
    Bm25Serialize(#[from] fluree_db_query::bm25::SerializeError),

    /// Vector index errors (requires `vector` feature)
    #[cfg(feature = "vector")]
    #[error("Vector index error: {0}")]
    Vector(#[from] fluree_db_query::vector::usearch::VectorError),

    /// Novelty/commit tracing errors
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Credential verification errors (requires `credential` feature)
    #[cfg(feature = "credential")]
    #[error("Credential error: {0}")]
    Credential(#[from] fluree_db_credential::CredentialError),

    /// Iceberg graph-source errors (requires `iceberg` feature).
    ///
    /// Preserves the typed discriminant from `fluree_db_iceberg` — notably
    /// [`fluree_db_iceberg::IcebergError::MergeOnReadDeletes`], which the
    /// fail-closed MoR guard raises for a correctly-configured table that merely
    /// carries delete files (a 409 Conflict, not a 400 config error). Display is
    /// the inner error verbatim (no prefix) so the guard's actionable message —
    /// including the `merge-on-read` substring the CLI/solo classifiers match on
    /// and the `FLUREE_ICEBERG_ALLOW_MOR_DELETES` override name — reaches the
    /// caller unchanged.
    #[cfg(feature = "iceberg")]
    #[error("{0}")]
    Iceberg(#[from] fluree_db_iceberg::IcebergError),

    /// Core/Storage errors
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// JSON serialization errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration errors
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// A materialize pass's subject accumulator outgrew its memory budget.
    ///
    /// A pre-OOM circuit breaker, not an allocator meter: the bytes are
    /// ESTIMATED (string lengths + JSON value sizes + flat per-entry overhead)
    /// as rows accumulate, and the pass aborts BEFORE any commit — no retract
    /// has run, no target ledger is touched, the watermark is un-advanced, so
    /// the failure leaves everything exactly as the last successful poll did.
    /// The previous behavior was the kernel OOM-killing the whole server with
    /// no log line (measured: 21.4 GiB resident on a 735k-row full re-read,
    /// killed every 4-6 minutes, watermark never advancing).
    ///
    /// This failure is DETERMINISTIC: the same window fails identically on the
    /// next poll. Levers, in order: an incremental window this large usually
    /// means the poll interval is too long — shorten it so windows stay small;
    /// a FULL read this large has no window to shrink — raise
    /// `FLUREE_MATERIALIZE_MEMORY_BUDGET_MB` (default 1024; 0 disables the
    /// gate) for a scheduled off-peak sync until streaming finalization lands.
    #[error(
        "materialize window for table '{table}' needs ~{estimated_bytes} B of accumulator \
         memory ({distinct_subjects} distinct subjects) against a budget of {budget_bytes} B; \
         nothing was committed. Shorten the poll interval (smaller windows) or raise \
         FLUREE_MATERIALIZE_MEMORY_BUDGET_MB (0 disables)"
    )]
    MaterializeMemoryBudget {
        /// Source table whose window overflowed the accumulator.
        table: String,
        /// Estimated resident bytes of the accumulator at abort.
        estimated_bytes: usize,
        /// The configured budget in bytes.
        budget_bytes: usize,
        /// Distinct (target, graph, subject) keys accumulated at abort.
        distinct_subjects: usize,
    },

    /// Unresolved `owl:imports` in the reasoning schema closure.
    ///
    /// Produced when a graph reachable from `f:schemaSource` declares
    /// `owl:imports <iri>` that cannot be resolved — the IRI is neither a
    /// named graph in the current ledger nor listed in
    /// `f:ontologyImportMap`. Import chains are strict: unresolved imports
    /// fail the query rather than being silently ignored.
    #[error("Unresolved owl:imports: {0}")]
    OntologyImport(String),

    /// Result formatting errors
    #[error("Format error: {0}")]
    Format(#[from] FormatError),

    /// Drop operation errors
    #[error("Drop error: {0}")]
    Drop(String),

    /// Invalid branch operation (merge into self, missing branch point, etc.)
    #[error("Invalid branch operation: {0}")]
    InvalidBranch(String),

    /// Branch conflict (fast-forward not possible, rebase abort, etc.)
    #[error("Branch conflict: {0}")]
    BranchConflict(String),

    /// Not found errors
    #[error("Not found: {0}")]
    NotFound(String),

    /// Ledger already exists
    #[error("Ledger already exists: {0}")]
    LedgerExists(String),

    /// Materialization deferred by novelty backpressure — NOT a failure.
    ///
    /// The target ledger's novelty is at its ceiling, and only the indexer can drain
    /// it. Deliberately a distinct variant rather than an `Internal` string so callers
    /// can treat it as "retry next poll" instead of logging a fault: the materialize
    /// worker polls every 30-57 s, which is the correct backoff.
    ///
    /// Waiting in-process instead of deferring caused a production deadlock — the
    /// worker holds what the indexer needs to publish, so the wait guaranteed the
    /// condition could not clear. See `transact_chunks_with_backpressure`.
    // NOT "(will retry)". Whether anything retries depends on the CALLER: the
    // materialize worker re-polls every 30-57 s, but a one-shot HTTP
    // /iceberg/materialize does not — so promising a retry misinformed an operator who
    // invoked it by hand and reasonably read it as "in progress".
    #[error(
        "Materialization deferred: novelty at capacity, {remaining} items pending. Nothing was \
         applied for the deferred target. The tracking worker retries automatically; a one-shot \
         /iceberg/materialize call must be re-issued."
    )]
    NoveltyDeferred {
        /// Items not applied in this window; they are re-derived on the next poll.
        remaining: usize,
    },

    /// A fan-out window where the targets did not all reach the same outcome.
    ///
    /// One materialize job resolves to N target ledgers, each an independent commit
    /// domain. Partial application is therefore the NORMAL case, not an exception, and
    /// reporting it as a single scalar outcome loses the only number that matters:
    /// how many targets actually progressed.
    ///
    /// This is still an `Err` because the shared watermark is held back whenever any
    /// target is behind, so the window is NOT complete and a caller must re-poll. The
    /// tally rides along so the caller can account for the targets that did commit.
    ///
    /// Concretely: a production poll with 21 of 22 targets committing surfaced as one
    /// `NoveltyDeferred`, so the worker recorded zero commits and one deferral. Read off
    /// the stats, a healthy window was indistinguishable from a total stall — and it was
    /// diagnosed as one.
    #[error(
        "Materialization applied {} of {} targets ({} deferred, {} failed); the watermark is held \
         back so the window is retried. Most serious outcome: {detail}",
        tally.ok, tally.total(), tally.deferred, tally.failed
    )]
    MaterializePartial {
        /// Per-target outcome counts for this window.
        tally: TargetTally,
        /// The most serious single outcome — a failure if any target failed, else a
        /// deferral. Failure outranks deferral because a deferral self-heals on the
        /// next poll and a failure usually needs attention.
        detail: String,
    },

    /// Internal errors (ledger_info, etc.)
    #[error("Internal error: {0}")]
    Internal(String),

    /// Object storage denied a read of an external table's data (S3 403 /
    /// `AccessDenied`), on the preview/browse path.
    ///
    /// Surfaced as HTTP 403 (not the generic 400/500) so the caller can tell a
    /// permission problem from a bad query. Because S3 also returns
    /// `AccessDenied` for a missing object without `s3:ListBucket`, this means
    /// the credentials lack access **or** the object was moved/removed. The scan
    /// path produces the equivalent [`fluree_db_query::QueryError::StorageAccessDenied`]
    /// (wrapped here via [`ApiError::Query`]); both map to the same server code.
    #[error(
        "Storage access denied for s3://{bucket}/{key}{region_suffix}: {message}",
        region_suffix = .region.as_deref().map(|r| format!(" (region {r})")).unwrap_or_default()
    )]
    StorageAccessDenied {
        /// Bucket parsed from the object path.
        bucket: String,
        /// Object key parsed from the object path.
        key: String,
        /// Configured/resolved region, if known.
        region: Option<String>,
        /// The underlying storage error detail.
        message: String,
    },

    /// The catalog authorized the table but vended no storage credentials while
    /// the source requires them (`vended_credentials = true`).
    ///
    /// Fail-closed on the preview/browse path: refused rather than silently
    /// downgrading to ambient (process-default) AWS credentials.
    #[error(
        "Catalog {catalog_uri} authorized the table but vended no storage credentials; \
         either fix the catalog's credential vending or set vended_credentials=false on \
         the source to explicitly use ambient AWS credentials"
    )]
    CatalogCredentialsNotVended {
        /// The REST catalog URI that authorized the table.
        catalog_uri: String,
    },

    /// HTTP error with explicit status code
    ///
    /// Used when the error source already has a known HTTP status (e.g., TrackedErrorResponse
    /// from credentialed transactions). This preserves the original status for the server layer.
    #[error("{message}")]
    Http {
        /// HTTP status code
        status: u16,
        /// Error message
        message: String,
    },

    /// Timeout waiting for indexing to complete
    #[error("Index operation timed out after {0}ms")]
    IndexTimeout(u64),

    /// Indexing not available (disabled mode)
    #[error("Indexing is disabled - no background indexer configured")]
    IndexingDisabled,

    /// Refresh did not reach the requested minimum `t` value.
    ///
    /// The nameservice was polled and any available commits were applied,
    /// but the ledger's `t` is still below the caller's `min_t` threshold.
    /// The caller should decide whether to retry (with backoff) or give up.
    #[error("Ledger has not reached t={requested}, current t={current}")]
    AwaitTNotReached {
        /// The `t` value the caller asked for.
        requested: i64,
        /// The ledger's `t` after the refresh attempt.
        current: i64,
    },

    /// Ledger advanced during reindex (conflict)
    #[error("Ledger advanced during reindex: expected t={expected}, found t={found}")]
    ReindexConflict {
        /// Expected commit_t at start of reindex
        expected: i64,
        /// Actual commit_t found after reindex
        found: i64,
    },

    /// Policy errors
    #[error("Policy error: {0}")]
    Policy(#[from] fluree_db_policy::PolicyError),

    /// Indexer crate errors
    #[error("Indexer error: {0}")]
    Indexer(#[from] fluree_db_indexer::IndexerError),

    /// Builder validation errors (one or more problems with builder configuration)
    #[error("{0}")]
    Builder(BuilderErrors),

    /// Cross-ledger governance resolution failed.
    ///
    /// Wrapped variant carries the specific failure (missing ledger,
    /// graph missing at t, retention pruned, reserved graph,
    /// translation failure, trust failure, cross-instance, cycle).
    /// HTTP layer maps this to 502; the variant is preserved in the
    /// response body so callers can branch on it. Every variant is
    /// fail-closed — there is no silent fallback to "no policy" or
    /// "no shapes" when a cross-ledger dependency cannot be served.
    #[error("Cross-ledger error: {0}")]
    CrossLedger(#[from] crate::cross_ledger::CrossLedgerError),
}

impl ApiError {
    /// Per-target tally when this error came from a fan-out materialize window.
    ///
    /// Exists so a caller can credit the targets that DID commit without matching on the
    /// variant. The absence of any such accessor is what made the previous behaviour
    /// invisible: a partial window arrived as `NoveltyDeferred { remaining }`, which has
    /// nowhere to put "21 targets succeeded", so the information was not so much lost as
    /// unrepresentable.
    pub fn target_tally(&self) -> Option<TargetTally> {
        match self {
            ApiError::MaterializePartial { tally, .. } => Some(*tally),
            _ => None,
        }
    }

    /// Check if this error represents a "not found" condition.
    ///
    /// Matches both `ApiError::NotFound` and `ApiError::Ledger(LedgerError::NotFound)`.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            ApiError::NotFound(_) | ApiError::Ledger(fluree_db_ledger::LedgerError::NotFound(_))
        )
    }

    /// Create a configuration error
    pub fn config(msg: impl Into<String>) -> Self {
        ApiError::Config(msg.into())
    }

    /// Create a SPARQL error with diagnostics
    pub fn sparql(
        message: impl Into<String>,
        diagnostics: Vec<fluree_db_sparql::Diagnostic>,
    ) -> Self {
        ApiError::Sparql {
            message: message.into(),
            diagnostics,
        }
    }

    /// Create a Cypher error with diagnostics
    pub fn cypher(
        message: impl Into<String>,
        diagnostics: Vec<fluree_db_cypher::Diagnostic>,
    ) -> Self {
        ApiError::Cypher {
            message: message.into(),
            diagnostics,
        }
    }

    /// Create a drop operation error
    pub fn drop_error(msg: impl Into<String>) -> Self {
        ApiError::Drop(msg.into())
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        ApiError::Internal(msg.into())
    }

    /// Create a query error
    pub fn query(msg: impl Into<String>) -> Self {
        ApiError::Internal(format!("Query error: {}", msg.into()))
    }

    /// Create a not-implemented error
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        ApiError::Internal(format!("Not implemented: {}", feature.into()))
    }

    /// Create a graph source not found error
    pub fn graph_source_not_found(alias: impl Into<String>) -> Self {
        ApiError::NotFound(format!("Graph source not found: {}", alias.into()))
    }

    /// Create a ledger already exists error
    pub fn ledger_exists(alias: impl Into<String>) -> Self {
        ApiError::LedgerExists(alias.into())
    }

    /// Create an index not found error for a graph source
    pub fn graph_source_index_not_found(alias: impl Into<String>) -> Self {
        ApiError::NotFound(format!("No index for graph source: {}", alias.into()))
    }

    /// Create a stale index error
    pub fn graph_source_stale(alias: impl Into<String>, index_t: i64, target_t: i64) -> Self {
        ApiError::Config(format!(
            "Graph source '{}' index (t={}) is behind target (t={}). Use sync=true to catch up.",
            alias.into(),
            index_t,
            target_t
        ))
    }

    /// HTTP status code for error (useful for HTTP server layer)
    ///
    /// NOTE: fluree-db-api has no server layer; this is for consumers
    /// like fluree-db-server or external HTTP wrappers.
    pub fn status_code(&self) -> u16 {
        match self {
            ApiError::Http { status, .. } => *status,
            #[cfg(feature = "credential")]
            ApiError::Credential(e) => e.status_code(),
            // A correctly-configured Iceberg table that merely carries
            // merge-on-read delete files is a conflict (unsupported state), not
            // bad input — 409. Other Iceberg errors preserve the pre-typed-variant
            // 400 (they previously flowed through `ApiError::config`).
            #[cfg(feature = "iceberg")]
            ApiError::Iceberg(e) => match e {
                fluree_db_iceberg::IcebergError::MergeOnReadDeletes(_) => 409,
                _ => 400,
            },
            ApiError::InvalidBranch(_) => 400,
            ApiError::BranchConflict(_) => 409,
            ApiError::NotFound(_) => 404,
            ApiError::Ledger(fluree_db_ledger::LedgerError::NotFound(_)) => 404,
            ApiError::LedgerExists(_) => 409,
            ApiError::ReindexConflict { .. } => 409,
            ApiError::IndexTimeout(_) => 504, // Gateway Timeout
            // 503 + retryable: novelty is at capacity and only the indexer can clear
            // it. Not the caller's fault (no 4xx) and not a fault at all (no 500) —
            // the correct client behaviour is to try again shortly.
            ApiError::NoveltyDeferred { .. } => 503,
            ApiError::IndexingDisabled => 400, // Bad Request
            ApiError::Indexer(e) => {
                use fluree_db_indexer::IndexerError;
                match e {
                    IndexerError::LedgerNotFound(_) => 404,
                    IndexerError::NoCommits => 400,
                    _ => 500,
                }
            }
            // Builder validation errors
            ApiError::Builder(_) => 400,
            ApiError::Query(fluree_db_query::QueryError::Cancelled { .. }) => 408,
            // R3-B: memory-budget abort → 507 (Insufficient Storage), distinct from
            // the 408 timeout so the caller can degrade on it specifically.
            ApiError::Query(fluree_db_query::QueryError::MemoryBudgetExceeded { .. }) => 507,
            // Storage-permission / fail-closed errors are 403 (Forbidden),
            // whether raised directly (preview path) or wrapped from the query
            // engine (scan path). These arms MUST precede the generic
            // `ApiError::Query(_) => 400` below.
            ApiError::StorageAccessDenied { .. }
            | ApiError::CatalogCredentialsNotVended { .. }
            | ApiError::Query(
                fluree_db_query::QueryError::StorageAccessDenied { .. }
                | fluree_db_query::QueryError::CatalogCredentialsNotVended { .. },
            ) => 403,
            // Most errors are client errors (bad input)
            ApiError::Parse(_)
            | ApiError::Query(_)
            | ApiError::Config(_)
            | ApiError::Sparql { .. }
            | ApiError::SparqlLower(_)
            | ApiError::Cypher { .. }
            | ApiError::CypherLower(_)
            | ApiError::CypherUpdateLower(_)
            | ApiError::Turtle(_)
            | ApiError::Json(_)
            | ApiError::Batch(_)
            | ApiError::Format(_) => 400,
            ApiError::Transact(
                fluree_db_transact::TransactError::CommitConflict { .. }
                | fluree_db_transact::TransactError::CommitIdMismatch { .. }
                | fluree_db_transact::TransactError::PublishLostRace { .. }
                | fluree_db_transact::TransactError::NamespaceConflict(_),
            ) => 409,
            // 413: the transaction's own delta meets or exceeds
            // `reindex_max_bytes` (the commit check is `current + delta >=
            // max`, so with drained novelty this still fails) — no amount of
            // indexer draining can ever admit it. A 503 here would tell the
            // client to retry a request that can never work; 413 says the
            // payload itself is the problem. MUST precede the drainable
            // novelty arm below.
            ApiError::Transact(fluree_db_transact::TransactError::NoveltyWouldExceed {
                delta_bytes,
                max_bytes,
                ..
            }) if delta_bytes >= max_bytes => 413,
            // 503 + retryable: novelty backpressure, the same class as
            // `NoveltyDeferred` above. `NoveltyAtMax` (novelty already at
            // `reindex_max_bytes`) and drainable `NoveltyWouldExceed` (this
            // delta would cross it, but fits once novelty drains) are cleared
            // by the indexer, not by changing the request — a 400 tells
            // retrying clients the write is permanently invalid and to drop
            // it.
            ApiError::Transact(
                fluree_db_transact::TransactError::NoveltyAtMax
                | fluree_db_transact::TransactError::NoveltyWouldExceed { .. },
            ) => 503,
            // Other transaction errors are usually validation failures
            ApiError::Transact(_) => 400,
            // Cross-ledger model dependency could not be resolved /
            // used. Conceptually an upstream-dependency failure, not
            // an internal panic. 502 Bad Gateway is the pragmatic
            // choice; 424 Failed Dependency is closer semantically
            // but less commonly handled by client tooling. The
            // wrapped variant is preserved for callers that branch
            // on the specific failure.
            ApiError::CrossLedger(_) => 502,
            // Internal/infrastructure errors
            _ => 500,
        }
    }

    /// Create an HTTP error with explicit status code
    pub fn http(status: u16, message: impl Into<String>) -> Self {
        ApiError::Http {
            status,
            message: message.into(),
        }
    }
}

/// Result type alias for API operations
pub type Result<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_permission_errors_are_403() {
        // Direct (preview path) and query-wrapped (scan path) both → 403, and
        // the query-wrapped ones must NOT fall through to the generic
        // `ApiError::Query(_) => 400`.
        assert_eq!(
            ApiError::StorageAccessDenied {
                bucket: "b".into(),
                key: "k".into(),
                region: None,
                message: "m".into(),
            }
            .status_code(),
            403
        );
        assert_eq!(
            ApiError::CatalogCredentialsNotVended {
                catalog_uri: "https://c/v1".into(),
            }
            .status_code(),
            403
        );
        assert_eq!(
            ApiError::Query(fluree_db_query::QueryError::StorageAccessDenied {
                bucket: "b".into(),
                key: "k".into(),
                region: Some("us-east-2".into()),
                message: "m".into(),
            })
            .status_code(),
            403
        );
        assert_eq!(
            ApiError::Query(fluree_db_query::QueryError::CatalogCredentialsNotVended {
                catalog_uri: "https://c/v1".into(),
            })
            .status_code(),
            403
        );
    }

    #[test]
    fn generic_query_error_still_400() {
        // Guard against the new 403 arms accidentally swallowing other query
        // errors.
        assert_eq!(
            ApiError::Query(fluree_db_query::QueryError::InvalidQuery("bad".into())).status_code(),
            400
        );
    }

    #[test]
    fn tally_totals_and_completeness() {
        let all_good = TargetTally {
            ok: 22,
            deferred: 0,
            failed: 0,
        };
        assert_eq!(all_good.total(), 22);
        assert!(all_good.is_complete());

        // One target behind is enough to hold the shared watermark back.
        for behind in [
            TargetTally {
                ok: 21,
                deferred: 1,
                failed: 0,
            },
            TargetTally {
                ok: 21,
                deferred: 0,
                failed: 1,
            },
        ] {
            assert_eq!(behind.total(), 22);
            assert!(
                !behind.is_complete(),
                "a target that did not commit must block watermark advance: {behind:?}"
            );
        }
    }

    /// The regression this variant exists for. Reproduces the exact production window:
    /// 21 of 22 targets committed, one did not.
    ///
    /// This test CANNOT be written against the previous behaviour — that path returned
    /// `NoveltyDeferred { remaining }` / `Internal(String)`, neither of which has a field
    /// capable of holding "21 targets succeeded". The count was unrepresentable, so the
    /// worker scored the window as zero commits and the deployment was diagnosed as
    /// stalled while 21 ledgers were in fact being written every poll.
    #[test]
    fn a_partial_window_reports_the_targets_that_succeeded() {
        let tally = TargetTally {
            ok: 21,
            deferred: 1,
            failed: 0,
        };
        let e = ApiError::MaterializePartial {
            tally,
            detail: "novelty at capacity, 3088 items pending".into(),
        };

        assert_eq!(
            e.target_tally().map(|t| t.ok),
            Some(21),
            "the 21 committed targets must be recoverable from the error itself"
        );

        // The operator-facing message must lead with the ratio, because "deferred" alone
        // is what read as a total stall.
        let msg = e.to_string();
        assert!(
            msg.contains("21 of 22"),
            "message must state the ratio, got: {msg}"
        );
        assert!(msg.contains("1 deferred"), "got: {msg}");
        assert!(
            msg.contains("watermark is held back"),
            "message must say the window will be retried, got: {msg}"
        );
    }

    #[test]
    fn target_tally_is_none_for_unrelated_errors() {
        assert!(ApiError::NoveltyDeferred { remaining: 5 }
            .target_tally()
            .is_none());
        assert!(ApiError::Internal("boom".into()).target_tally().is_none());
    }
}
