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

    /// Faults in a ledger's stored config graph.
    ///
    /// Distinct from [`Config`](Self::Config), which covers configuration the
    /// caller supplies and is therefore a 400. The config graph is operator
    /// data: no request can cause a fault in it and no change to a request can
    /// clear one, so telling the caller their request was bad sends them
    /// looking in the wrong place.
    #[error("Ledger configuration error: {0}")]
    LedgerConfig(String),

    /// The ledger config could not be read (a storage or read failure).
    /// Nothing in the request is wrong, and a retry can succeed. The same
    /// condition on a transaction's staging lanes is
    /// [`TransactError::ConfigUnavailable`](fluree_db_transact::TransactError::ConfigUnavailable).
    #[error("Ledger config unavailable: {0}")]
    ConfigUnavailable(String),

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

    /// An operation on a graph source that has been dropped (409).
    #[error("{0}")]
    GraphSourceRetracted(String),

    /// Invalid branch operation (merge into self, missing branch point, etc.)
    #[error("Invalid branch operation: {0}")]
    InvalidBranch(String),

    /// A ledger id that does not parse (the message names the rule broken).
    #[error("{0}")]
    InvalidLedgerId(#[from] fluree_db_core::LedgerIdParseError),

    /// Branch conflict (fast-forward not possible, rebase abort, etc.)
    #[error("Branch conflict: {0}")]
    BranchConflict(String),

    /// Not found errors
    #[error("Not found: {0}")]
    NotFound(String),

    /// A graph the addressed ledger does not have (404).
    ///
    /// Deliberately not [`ApiError::is_not_found`]: that condition means "no
    /// such ledger" and sends a lookup on to graph sources, while here the
    /// ledger exists and only the graph is missing.
    #[error("Graph not found: {0}")]
    GraphNotFound(String),

    /// A commit reference (a CID, a hex prefix, a `t`) that names no commit of
    /// the ledger it is resolved in (404).
    ///
    /// Deliberately not [`ApiError::is_not_found`], for the reason
    /// [`ApiError::GraphNotFound`] is not: the ledger exists, and a time pin
    /// that resolves to no commit must not read as a missing ledger.
    #[error("{0}")]
    CommitNotFound(String),

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

    /// Another open instance holds the file storage root: one in another
    /// process, or a second one built in this process. `holder` is its
    /// process id when known.
    #[error("{}", storage_in_use_message(root, *holder))]
    StorageInUse { root: String, holder: Option<u32> },

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

    /// Every commit attempt lost its race with other writers to the ledger.
    ///
    /// Retryable (503): nothing was committed, and the same request can
    /// succeed once the other writers ease off.
    #[error(
        "Commit contention: {attempts} commit attempts lost to concurrent writers; nothing was \
         committed, retry the transaction. Last conflict: {last}"
    )]
    CommitContention {
        attempts: usize,
        /// The conflict the final attempt hit.
        #[source]
        last: Box<ApiError>,
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
    Indexer(#[from] crate::wasm_compat::IndexerError),

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

    /// Create an error describing a fault in the ledger's stored config graph.
    pub fn ledger_config(msg: impl Into<String>) -> Self {
        ApiError::LedgerConfig(msg.into())
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

    /// The error a commit retry loop returns once its last attempt also
    /// lost a race. An inner loop's contention is unwrapped, so `last` is
    /// always the conflict itself.
    pub fn commit_contention(attempts: usize, last: ApiError) -> Self {
        let last = match last {
            ApiError::CommitContention { last, .. } => *last,
            last => last,
        };
        ApiError::CommitContention {
            attempts,
            last: Box::new(last),
        }
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        ApiError::Internal(msg.into())
    }

    /// Create a query error
    pub fn query(msg: impl Into<String>) -> Self {
        ApiError::Internal(format!("Query error: {}", msg.into()))
    }

    /// Create an error for a query the caller got wrong: a 400, where
    /// [`Self::query`] is a 500.
    pub fn invalid_query(msg: impl Into<String>) -> Self {
        ApiError::Query(fluree_db_query::QueryError::InvalidQuery(msg.into()))
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

    /// The HTTP status for this error.
    ///
    /// The one status table: fluree-db-server answers with it, and every path
    /// that flattens an error to [`ApiError::Http`] (the consensus committer,
    /// shared ledger loads, tracked queries) carries it, so an error answers
    /// the same whichever route it leaves by. Its `@type` is
    /// [`Self::error_type`].
    pub fn status_code(&self) -> u16 {
        use fluree_db_transact::TransactError as TE;
        match self {
            ApiError::Http { status, .. } => *status,
            // Signed-request verification failures are documented as 401,
            // malformed or not.
            #[cfg(feature = "credential")]
            ApiError::Credential(_) => 401,
            ApiError::Query(e) => query_error_status(e),

            ApiError::NotFound(_)
            | ApiError::GraphNotFound(_)
            | ApiError::CommitNotFound(_)
            | ApiError::Ledger(fluree_db_ledger::LedgerError::NotFound(_)) => 404,

            ApiError::Parse(_)
            | ApiError::Batch(_)
            | ApiError::Config(_)
            | ApiError::Sparql { .. }
            | ApiError::SparqlLower(_)
            | ApiError::Cypher { .. }
            | ApiError::CypherLower(_)
            | ApiError::CypherUpdateLower(_)
            | ApiError::Turtle(_)
            | ApiError::Format(_)
            | ApiError::Builder(_)
            | ApiError::InvalidBranch(_)
            | ApiError::InvalidLedgerId(_)
            | ApiError::NameService(fluree_db_nameservice::NameServiceError::InvalidId(_))
            | ApiError::IndexingDisabled
            | ApiError::Ledger(fluree_db_ledger::LedgerError::FutureTime { .. }) => 400,

            ApiError::LedgerExists(_)
            | ApiError::BranchConflict(_)
            | ApiError::ReindexConflict { .. }
            | ApiError::GraphSourceRetracted(_) => 409,

            ApiError::StorageInUse { .. } => 423,

            // Read-after-write: the ledger has not caught up to the `t` the
            // caller asked for.
            ApiError::AwaitTNotReached { .. } => 408,
            ApiError::IndexTimeout(_) => 504,

            // 503 + retryable: capacity that clears on its own (the indexer
            // draining, other writers easing off), so neither the caller's
            // fault (4xx) nor a fault at all (500).
            ApiError::NoveltyDeferred { .. } | ApiError::CommitContention { .. } => 503,
            // A failed target is a fault and must not be dressed up as
            // backpressure, or a permanently broken target would 503 forever.
            ApiError::MaterializePartial { tally, .. } => {
                if tally.failed > 0 {
                    500
                } else {
                    503
                }
            }

            ApiError::StorageAccessDenied { .. } | ApiError::CatalogCredentialsNotVended { .. } => {
                403
            }
            // The write was understood but refused by its policy. Other
            // policy errors (invalid definitions, evaluation failures) are
            // not authorization denials.
            ApiError::Policy(fluree_db_policy::PolicyError::ModifyDenied { .. })
            | ApiError::Transact(TE::PolicyViolation(
                fluree_db_policy::PolicyError::ModifyDenied { .. },
            )) => 403,
            ApiError::Policy(_) => 500,

            ApiError::Transact(
                TE::CommitConflict { .. }
                | TE::CommitIdMismatch { .. }
                | TE::PublishLostRace { .. }
                | TE::NamespaceConflict(_),
            ) => 409,
            // The transaction's own delta meets or exceeds
            // `reindex_max_bytes` (the commit check is `current + delta >=
            // max`, so with drained novelty this still fails): no amount of
            // draining can ever admit it, so 413, not the retryable 503 below.
            ApiError::Transact(TE::NoveltyWouldExceed {
                delta_bytes,
                max_bytes,
                ..
            }) if delta_bytes >= max_bytes => 413,
            ApiError::Transact(TE::NoveltyAtMax | TE::NoveltyWouldExceed { .. }) => 503,
            // Permanent: datatype IDs are never released.
            ApiError::Transact(TE::DatatypeLimitExceeded { .. }) => 422,
            // The ledger config could not be read: a storage failure, whichever
            // lane read it. Nothing in the request is wrong, and it is not
            // capacity pressure, so not a 503.
            ApiError::ConfigUnavailable(_) | ApiError::Transact(TE::ConfigUnavailable(_)) => 500,
            ApiError::Transact(_) => 400,

            ApiError::Indexer(e) => {
                use crate::wasm_compat::IndexerError;
                match e {
                    IndexerError::LedgerNotFound(_) => 404,
                    IndexerError::NoCommits => 400,
                    _ => 500,
                }
            }

            // A table that merely carries merge-on-read delete files is a
            // conflict (unsupported state), not bad input. Other Iceberg
            // errors are configuration the caller supplied.
            #[cfg(feature = "iceberg")]
            ApiError::Iceberg(e) => match e {
                fluree_db_iceberg::IcebergError::MergeOnReadDeletes(_) => 409,
                _ => 400,
            },

            // A cross-ledger model dependency could not be resolved or used:
            // an upstream-dependency failure, not an internal one.
            ApiError::CrossLedger(_) => 502,

            // Stored data and infrastructure. `Json` is here because every
            // `serde_json` error that reaches it parses stored records
            // (graph-source configs, index manifests), and `LedgerConfig`
            // because no change to the request can clear a fault in the
            // ledger's config graph.
            ApiError::Connection(_)
            | ApiError::NameService(_)
            | ApiError::Ledger(_)
            | ApiError::Bm25Builder(_)
            | ApiError::Bm25Serialize(_)
            | ApiError::Novelty(_)
            | ApiError::Core(_)
            | ApiError::Json(_)
            | ApiError::LedgerConfig(_)
            | ApiError::MaterializeMemoryBudget { .. }
            | ApiError::OntologyImport(_)
            | ApiError::Drop(_)
            | ApiError::Internal(_) => 500,
            #[cfg(feature = "vector")]
            ApiError::Vector(_) => 500,
        }
    }

    /// The `@type` code ([`fluree_vocab::errors`]) for this error, the
    /// companion to [`Self::status_code`].
    pub fn error_type(&self) -> &'static str {
        use fluree_db_query::QueryError as QE;
        use fluree_db_transact::TransactError as TE;
        use fluree_vocab::errors;
        match self {
            // Flattened: only the status survives, so name the status.
            ApiError::Http { status, .. } => match status {
                400 => errors::BAD_REQUEST,
                401 => errors::UNAUTHORIZED,
                403 => errors::ACCESS_DENIED,
                404 => errors::NOT_FOUND,
                // Only a tracked query flattens a 408.
                408 => errors::QUERY_CANCELLED,
                409 => errors::COMMIT_CONFLICT,
                // The HTTP body-size cap. An oversized novelty delta is the
                // other 413, and stays typed end to end so the two carry
                // distinct codes.
                413 => errors::PAYLOAD_TOO_LARGE,
                422 => errors::INVALID_TRANSACTION,
                423 => errors::STORAGE_IN_USE,
                _ => errors::INTERNAL,
            },

            ApiError::GraphNotFound(_) => errors::GRAPH_NOT_FOUND,
            ApiError::CommitNotFound(_) => errors::COMMIT_NOT_FOUND,
            ApiError::NotFound(msg) => {
                if msg.contains("Graph source") || msg.contains("graph source") {
                    errors::GRAPH_SOURCE_NOT_FOUND
                } else {
                    errors::LEDGER_NOT_FOUND
                }
            }
            ApiError::Ledger(fluree_db_ledger::LedgerError::NotFound(_)) => {
                errors::LEDGER_NOT_FOUND
            }
            ApiError::GraphSourceRetracted(_) => errors::GRAPH_SOURCE_RETRACTED,

            ApiError::NoveltyDeferred { .. } => errors::NOVELTY_DEFERRED,
            ApiError::MaterializePartial { tally, .. } => {
                if tally.failed > 0 {
                    errors::MATERIALIZE_PARTIAL
                } else {
                    errors::NOVELTY_DEFERRED
                }
            }
            ApiError::LedgerExists(_) => errors::LEDGER_EXISTS,
            ApiError::BranchConflict(_) => errors::COMMIT_CONFLICT,
            ApiError::CommitContention { .. } => errors::COMMIT_CONTENTION,

            ApiError::IndexTimeout(_) => errors::INDEX_TIMEOUT,
            ApiError::IndexingDisabled => errors::INDEXING_DISABLED,
            ApiError::StorageInUse { .. } => errors::STORAGE_IN_USE,
            ApiError::ReindexConflict { .. } => errors::REINDEX_CONFLICT,
            ApiError::Indexer(e) => {
                use crate::wasm_compat::IndexerError;
                match e {
                    IndexerError::LedgerNotFound(_) => errors::LEDGER_NOT_FOUND,
                    IndexerError::NoCommits => errors::BAD_REQUEST,
                    _ => errors::INDEXING,
                }
            }

            ApiError::Parse(fluree_db_query::parse::ParseError::TypeCoercion(_)) => {
                errors::TYPE_COERCION
            }
            ApiError::Parse(_) => errors::JSONLD_PARSE,
            ApiError::Turtle(_) => errors::TURTLE_PARSE,
            ApiError::Sparql { .. } => errors::SPARQL_PARSE,
            ApiError::SparqlLower(_) => errors::SPARQL_LOWER,
            ApiError::Cypher { .. } => errors::CYPHER_PARSE,
            ApiError::CypherLower(_) | ApiError::CypherUpdateLower(_) => errors::CYPHER_LOWER,

            ApiError::AwaitTNotReached { .. } => errors::READ_AFTER_WRITE_TIMEOUT,
            ApiError::StorageAccessDenied { .. } => errors::STORAGE_ACCESS_DENIED,
            ApiError::CatalogCredentialsNotVended { .. } => errors::CATALOG_CREDENTIALS_NOT_VENDED,
            ApiError::Query(e) => match e {
                QE::Cancelled { .. } => errors::QUERY_CANCELLED,
                QE::StorageAccessDenied { .. } => errors::STORAGE_ACCESS_DENIED,
                QE::CatalogCredentialsNotVended { .. } => errors::CATALOG_CREDENTIALS_NOT_VENDED,
                QE::CatalogAccessDenied { .. } => errors::CATALOG_ACCESS_DENIED,
                // Well-formed but unsupported on this source: still a 400,
                // with its own code so clients can branch on it.
                QE::R2rmlUnsupportedPattern { .. } => errors::R2RML_UNSUPPORTED_PATTERN,
                // Typed as `ApiError::NameService` is: the backend's fault.
                QE::Nameservice(_) => errors::NAMESERVICE,
                e if query_error_status(e) == 500 => errors::INTERNAL,
                _ => errors::INVALID_QUERY,
            },
            ApiError::Batch(_) => errors::INVALID_QUERY,

            ApiError::Policy(fluree_db_policy::PolicyError::ModifyDenied { .. })
            | ApiError::Transact(TE::PolicyViolation(
                fluree_db_policy::PolicyError::ModifyDenied { .. },
            )) => errors::ACCESS_DENIED,
            ApiError::Transact(
                TE::CommitConflict { .. }
                | TE::CommitIdMismatch { .. }
                | TE::PublishLostRace { .. }
                | TE::NamespaceConflict(_),
            ) => errors::COMMIT_CONFLICT,
            ApiError::Transact(TE::NoveltyWouldExceed {
                delta_bytes,
                max_bytes,
                ..
            }) if delta_bytes >= max_bytes => errors::NOVELTY_DELTA_TOO_LARGE,
            // One code for both at-max and would-exceed: clients branch on
            // "the indexer needs to drain".
            ApiError::Transact(TE::NoveltyAtMax | TE::NoveltyWouldExceed { .. }) => {
                errors::NOVELTY_AT_MAX
            }
            ApiError::Transact(TE::DatatypeLimitExceeded { .. }) => errors::DATATYPE_LIMIT_EXCEEDED,
            ApiError::ConfigUnavailable(_) | ApiError::Transact(TE::ConfigUnavailable(_)) => {
                errors::STORAGE_READ
            }
            ApiError::Transact(_) => errors::INVALID_TRANSACTION,

            ApiError::InvalidLedgerId(_)
            | ApiError::InvalidBranch(_)
            | ApiError::NameService(fluree_db_nameservice::NameServiceError::InvalidId(_))
            | ApiError::Ledger(fluree_db_ledger::LedgerError::FutureTime { .. }) => {
                errors::BAD_REQUEST
            }
            ApiError::Builder(errs) => errs
                .0
                .first()
                .map_or(errors::BAD_REQUEST, BuilderError::error_code),
            #[cfg(feature = "credential")]
            ApiError::Credential(_) => errors::INVALID_CREDENTIAL,
            #[cfg(feature = "iceberg")]
            ApiError::Iceberg(e) => match e {
                fluree_db_iceberg::IcebergError::MergeOnReadDeletes(_) => {
                    errors::MERGE_ON_READ_DELETES
                }
                _ => errors::CONFIG,
            },

            ApiError::Connection(_) => errors::CONNECTION,
            ApiError::NameService(_) => errors::NAMESERVICE,
            ApiError::Bm25Builder(_) | ApiError::Bm25Serialize(_) => errors::BM25,
            ApiError::Config(_) | ApiError::LedgerConfig(_) => errors::CONFIG,
            ApiError::Format(_) => errors::FORMAT,
            // The specific failure stays in the message; this is the umbrella.
            ApiError::CrossLedger(_) => errors::CROSS_LEDGER,

            ApiError::Ledger(_)
            | ApiError::Novelty(_)
            | ApiError::Core(_)
            | ApiError::Json(_)
            | ApiError::MaterializeMemoryBudget { .. }
            | ApiError::OntologyImport(_)
            | ApiError::Drop(_)
            | ApiError::Policy(_)
            | ApiError::Internal(_) => errors::INTERNAL,
            #[cfg(feature = "vector")]
            ApiError::Vector(_) => errors::INTERNAL,
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

fn storage_in_use_message(root: &str, holder: Option<u32>) -> String {
    if holder == Some(std::process::id()) {
        return format!(
            "storage '{root}' is already open in this process; \
             use that instance instead of opening the storage again"
        );
    }
    let by = holder
        .map(|pid| format!(" (pid {pid})"))
        .unwrap_or_default();
    format!(
        "storage '{root}' is in use by another Fluree process{by}. Only one process \
         can open a storage directory; to share it, run `fluree server` on it and \
         send requests to the server"
    )
}

/// The HTTP status for a query engine error, wherever it surfaces: wrapped in
/// [`ApiError::Query`], or reported directly by a tracked query.
///
/// Exhaustive so a new variant has to be classified.
pub(crate) fn query_error_status(err: &fluree_db_query::QueryError) -> u16 {
    use fluree_db_query::QueryError as QE;
    match err {
        QE::Cancelled { .. } => 408,
        // The in-memory join/aggregate budget: distinct from the 408 timeout
        // so the caller degrades on it specifically (too memory-heavy, not
        // too slow).
        QE::MemoryBudgetExceeded { .. } => 507,
        QE::StorageAccessDenied { .. }
        | QE::CatalogCredentialsNotVended { .. }
        | QE::CatalogAccessDenied { .. } => 403,
        // Engine and storage faults no change to the query can clear.
        QE::Core(_)
        | QE::OperatorNotOpened
        | QE::OperatorAlreadyOpened
        | QE::OperatorClosed
        | QE::NoSuitableIndex
        | QE::DictionaryLookup(_)
        | QE::NeedFetch(_)
        | QE::Policy(_)
        | QE::Internal(_) => 500,
        // A nameservice lookup the query needed failed (a graph-source probe
        // or lookup): a backend fault, as `ApiError::NameService` is.
        QE::Nameservice(_) => 500,
        QE::Batch(_)
        | QE::R2rml(_)
        | QE::VariableNotFound(_)
        | QE::InvalidQuery(_)
        | QE::UngroupedRead(_)
        | QE::InvalidFilter(_)
        | QE::InvalidExpression(_)
        | QE::SnapshotNotFound { .. }
        | QE::NoSnapshotAtTime { .. }
        | QE::ResourceLimit(_)
        | QE::FuelLimitExceeded(_)
        | QE::R2rmlUnsupportedPattern { .. }
        | QE::UnsupportedMode(_)
        | QE::UnsupportedFeature(_)
        | QE::TimeRangeNotCovered { .. }
        | QE::Arithmetic(_)
        | QE::Comparison(_) => 400,
    }
}

/// A reference that does not name a graph of the ledger a surface reads: a
/// caller mistake, never an internal error. Another ledger's address is a 400
/// that names the fix; a graph the ledger does not have is a 404.
impl From<fluree_db_core::TargetError> for ApiError {
    fn from(e: fluree_db_core::TargetError) -> Self {
        match e {
            fluree_db_core::TargetError::GraphNotFound(iri) => ApiError::GraphNotFound(iri),
            cross @ fluree_db_core::TargetError::CrossLedger { .. } => {
                ApiError::invalid_query(cross.to_string())
            }
        }
    }
}

/// Result type alias for API operations
pub type Result<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_ledger::CrossLedgerError;
    use crate::wasm_compat::IndexerError as IE;
    use fluree_db_connection::ConnectionError;
    use fluree_db_core::LedgerIdParseError;
    use fluree_db_core::QueryCancellationReason::Timeout;
    #[cfg(feature = "credential")]
    use fluree_db_credential::CredentialError as CE;
    use fluree_db_cypher::LowerError as CypherLowerError;
    #[cfg(feature = "iceberg")]
    use fluree_db_iceberg::IcebergError as IbE;
    use fluree_db_ledger::LedgerError as LE;
    use fluree_db_nameservice::NameServiceError as NSE;
    use fluree_db_novelty::NoveltyError;
    use fluree_db_policy::PolicyError as PE;
    use fluree_db_query::bm25;
    use fluree_db_query::parse::ParseError;
    use fluree_db_query::QueryError as QE;
    use fluree_db_sparql::{LowerError as SLE, SourceSpan};
    use fluree_db_transact::lower_cypher_update::LowerCypherError;
    use fluree_db_transact::TransactError as TE;
    use fluree_graph_turtle::TurtleError;
    use fluree_vocab::errors::*;

    /// Distinct `ApiError` variants `every_variant` must cover. Adding a
    /// variant breaks the match in `every_variant`; give it a row there and
    /// count it here.
    const VARIANTS: usize = 49
        + cfg!(feature = "vector") as usize
        + cfg!(feature = "credential") as usize
        + cfg!(feature = "iceberg") as usize;

    fn conflict() -> ApiError {
        ApiError::Transact(TE::CommitConflict {
            expected_t: 1,
            head_t: 2,
        })
    }

    /// A fan-out window with one target deferred and `failed` failed.
    fn partial(failed: usize) -> ApiError {
        ApiError::MaterializePartial {
            tally: TargetTally {
                ok: 1,
                deferred: 1,
                failed,
            },
            detail: "x".into(),
        }
    }

    /// Every `ApiError` variant (several forms of the ones that split), with
    /// the status and `@type` it must answer with, in status order.
    fn every_variant() -> Vec<(ApiError, u16, &'static str)> {
        // Every `serde_json` error that reaches `ApiError::Json` parses stored
        // records, not the request.
        let json = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let span = SourceSpan::from(0..1);
        let rows = vec![
            (
                ApiError::Parse(ParseError::MissingField("select")),
                400,
                JSONLD_PARSE,
            ),
            (
                ApiError::Parse(ParseError::TypeCoercion("x".into())),
                400,
                TYPE_COERCION,
            ),
            (ApiError::invalid_query("x"), 400, INVALID_QUERY),
            (
                ApiError::Query(QE::r2rml_unsupported_pattern("x")),
                400,
                R2RML_UNSUPPORTED_PATTERN,
            ),
            (
                ApiError::Batch(fluree_db_query::BatchError::ColumnLengthMismatch {
                    expected: 1,
                    got: 2,
                    column: 0,
                }),
                400,
                INVALID_QUERY,
            ),
            (
                ApiError::Ledger(LE::future_time("db", 9, 1)),
                400,
                BAD_REQUEST,
            ),
            (
                ApiError::NameService(NSE::InvalidId("a@b".into())),
                400,
                BAD_REQUEST,
            ),
            (
                ApiError::Transact(TE::InvalidTerm("x".into())),
                400,
                INVALID_TRANSACTION,
            ),
            (ApiError::sparql("x", vec![]), 400, SPARQL_PARSE),
            (
                ApiError::SparqlLower(SLE::AggregateWithoutAlias { span }),
                400,
                SPARQL_LOWER,
            ),
            (ApiError::cypher("x", vec![]), 400, CYPHER_PARSE),
            (
                ApiError::CypherLower(CypherLowerError::WriteOnQueryPath),
                400,
                CYPHER_LOWER,
            ),
            (
                ApiError::CypherUpdateLower(LowerCypherError::NotAnUpdate),
                400,
                CYPHER_LOWER,
            ),
            (
                ApiError::Turtle(TurtleError::IriResolution("x".into())),
                400,
                TURTLE_PARSE,
            ),
            #[cfg(feature = "iceberg")]
            (ApiError::Iceberg(IbE::Config("x".into())), 400, CONFIG),
            (ApiError::config("x"), 400, CONFIG),
            (
                ApiError::Format(FormatError::InvalidBinding("x".into())),
                400,
                FORMAT,
            ),
            (ApiError::InvalidBranch("x".into()), 400, BAD_REQUEST),
            (
                ApiError::InvalidLedgerId(LedgerIdParseError::new("x")),
                400,
                BAD_REQUEST,
            ),
            (ApiError::IndexingDisabled, 400, INDEXING_DISABLED),
            (ApiError::Indexer(IE::NoCommits), 400, BAD_REQUEST),
            (
                ApiError::Builder(BuilderErrors(vec![BuilderError::Missing {
                    field: "input",
                    hint: "h",
                }])),
                400,
                MISSING_PARAMETER,
            ),
            // Signed requests: every verification failure is documented as 401.
            #[cfg(feature = "credential")]
            (
                ApiError::Credential(CE::InvalidSignature("x".into())),
                401,
                INVALID_CREDENTIAL,
            ),
            (
                ApiError::Query(QE::CatalogAccessDenied {
                    table: "t".into(),
                    message: "m".into(),
                }),
                403,
                CATALOG_ACCESS_DENIED,
            ),
            (
                ApiError::Transact(TE::PolicyViolation(PE::modify_denied("no"))),
                403,
                ACCESS_DENIED,
            ),
            (
                ApiError::StorageAccessDenied {
                    bucket: "b".into(),
                    key: "k".into(),
                    region: None,
                    message: "m".into(),
                },
                403,
                STORAGE_ACCESS_DENIED,
            ),
            (
                ApiError::CatalogCredentialsNotVended {
                    catalog_uri: "c".into(),
                },
                403,
                CATALOG_CREDENTIALS_NOT_VENDED,
            ),
            (
                ApiError::Policy(PE::modify_denied("no")),
                403,
                ACCESS_DENIED,
            ),
            (ApiError::Ledger(LE::not_found("db")), 404, LEDGER_NOT_FOUND),
            (ApiError::NotFound("db".into()), 404, LEDGER_NOT_FOUND),
            (
                ApiError::graph_source_not_found("gs"),
                404,
                GRAPH_SOURCE_NOT_FOUND,
            ),
            (ApiError::GraphNotFound("g".into()), 404, GRAPH_NOT_FOUND),
            (ApiError::CommitNotFound("c".into()), 404, COMMIT_NOT_FOUND),
            (ApiError::http(404, "x"), 404, NOT_FOUND),
            (
                ApiError::Indexer(IE::LedgerNotFound("db".into())),
                404,
                LEDGER_NOT_FOUND,
            ),
            (
                ApiError::Query(QE::Cancelled { reason: Timeout }),
                408,
                QUERY_CANCELLED,
            ),
            (
                ApiError::AwaitTNotReached {
                    requested: 2,
                    current: 1,
                },
                408,
                READ_AFTER_WRITE_TIMEOUT,
            ),
            (conflict(), 409, COMMIT_CONFLICT),
            (
                ApiError::Transact(TE::CommitIdMismatch {
                    expected: "a".into(),
                    found: "b".into(),
                }),
                409,
                COMMIT_CONFLICT,
            ),
            #[cfg(feature = "iceberg")]
            (
                ApiError::Iceberg(IbE::MergeOnReadDeletes("x".into())),
                409,
                MERGE_ON_READ_DELETES,
            ),
            (
                ApiError::GraphSourceRetracted("x".into()),
                409,
                GRAPH_SOURCE_RETRACTED,
            ),
            (ApiError::BranchConflict("x".into()), 409, COMMIT_CONFLICT),
            (ApiError::ledger_exists("db"), 409, LEDGER_EXISTS),
            (
                ApiError::StorageInUse {
                    root: "x".into(),
                    holder: None,
                },
                423,
                STORAGE_IN_USE,
            ),
            (
                ApiError::ReindexConflict {
                    expected: 1,
                    found: 2,
                },
                409,
                REINDEX_CONFLICT,
            ),
            (
                ApiError::Transact(TE::NoveltyWouldExceed {
                    current_bytes: 0,
                    delta_bytes: 100,
                    max_bytes: 100,
                }),
                413,
                NOVELTY_DELTA_TOO_LARGE,
            ),
            (
                ApiError::Transact(TE::DatatypeLimitExceeded {
                    used: 1,
                    adding: 1,
                    max: 1,
                }),
                422,
                DATATYPE_LIMIT_EXCEEDED,
            ),
            (
                ApiError::Connection(ConnectionError::InvalidConfig("x".into())),
                500,
                CONNECTION,
            ),
            (ApiError::Query(QE::Internal("x".into())), 500, INTERNAL),
            (
                ApiError::Query(QE::Nameservice("x".into())),
                500,
                NAMESERVICE,
            ),
            (ApiError::Ledger(LE::InvalidData("x".into())), 500, INTERNAL),
            (
                ApiError::NameService(NSE::Storage("x".into())),
                500,
                NAMESERVICE,
            ),
            (
                ApiError::Bm25Builder(bm25::BuilderError::MissingId("x".into())),
                500,
                BM25,
            ),
            (
                ApiError::Bm25Serialize(std::io::Error::other("x").into()),
                500,
                BM25,
            ),
            #[cfg(feature = "vector")]
            (
                ApiError::Vector(
                    fluree_db_query::vector::usearch::VectorError::DimensionMismatch {
                        expected: 1,
                        actual: 2,
                    },
                ),
                500,
                INTERNAL,
            ),
            (
                ApiError::Novelty(NoveltyError::Storage("x".into())),
                500,
                INTERNAL,
            ),
            (
                ApiError::Core(fluree_db_core::Error::Storage("x".into())),
                500,
                INTERNAL,
            ),
            (ApiError::Json(json), 500, INTERNAL),
            (ApiError::ledger_config("x"), 500, CONFIG),
            // A ledger config read failure, from the API's own reads (the
            // policy context, a query's config) and from a staging lane.
            (ApiError::ConfigUnavailable("x".into()), 500, STORAGE_READ),
            (
                ApiError::Transact(TE::ConfigUnavailable("x".into())),
                500,
                STORAGE_READ,
            ),
            (
                ApiError::Transact(TE::RequestOverrideRefused {
                    setting: "opts.shapes".into(),
                    graph: "default".into(),
                    control: "f:OverrideNone".into(),
                }),
                400,
                INVALID_TRANSACTION,
            ),
            (
                ApiError::MaterializeMemoryBudget {
                    table: "t".into(),
                    estimated_bytes: 2,
                    budget_bytes: 1,
                    distinct_subjects: 1,
                },
                500,
                INTERNAL,
            ),
            (ApiError::OntologyImport("x".into()), 500, INTERNAL),
            (ApiError::drop_error("x"), 500, INTERNAL),
            (partial(1), 500, MATERIALIZE_PARTIAL),
            (ApiError::internal("x"), 500, INTERNAL),
            (ApiError::Policy(PE::invalid_query("x")), 500, INTERNAL),
            (ApiError::Indexer(IE::Other("x".into())), 500, INDEXING),
            (
                ApiError::CrossLedger(CrossLedgerError::ModelLedgerMissing {
                    ledger_id: "m".into(),
                }),
                502,
                CROSS_LEDGER,
            ),
            (ApiError::Transact(TE::NoveltyAtMax), 503, NOVELTY_AT_MAX),
            (
                ApiError::NoveltyDeferred { remaining: 1 },
                503,
                NOVELTY_DEFERRED,
            ),
            (partial(0), 503, NOVELTY_DEFERRED),
            (
                ApiError::commit_contention(16, conflict()),
                503,
                COMMIT_CONTENTION,
            ),
            (ApiError::IndexTimeout(1), 504, INDEX_TIMEOUT),
            (
                ApiError::Query(QE::MemoryBudgetExceeded {
                    used_bytes: 2,
                    budget_bytes: 1,
                }),
                507,
                INVALID_QUERY,
            ),
        ];
        for (e, ..) in &rows {
            match e {
                ApiError::Connection(_)
                | ApiError::Parse(_)
                | ApiError::Query(_)
                | ApiError::Batch(_)
                | ApiError::Ledger(_)
                | ApiError::NameService(_)
                | ApiError::Transact(_)
                | ApiError::Sparql { .. }
                | ApiError::SparqlLower(_)
                | ApiError::Cypher { .. }
                | ApiError::CypherLower(_)
                | ApiError::CypherUpdateLower(_)
                | ApiError::Turtle(_)
                | ApiError::Bm25Builder(_)
                | ApiError::Bm25Serialize(_)
                | ApiError::Novelty(_)
                | ApiError::Core(_)
                | ApiError::Json(_)
                | ApiError::Config(_)
                | ApiError::LedgerConfig(_)
                | ApiError::ConfigUnavailable(_)
                | ApiError::MaterializeMemoryBudget { .. }
                | ApiError::OntologyImport(_)
                | ApiError::Format(_)
                | ApiError::Drop(_)
                | ApiError::GraphSourceRetracted(_)
                | ApiError::InvalidBranch(_)
                | ApiError::InvalidLedgerId(_)
                | ApiError::BranchConflict(_)
                | ApiError::NotFound(_)
                | ApiError::GraphNotFound(_)
                | ApiError::CommitNotFound(_)
                | ApiError::LedgerExists(_)
                | ApiError::NoveltyDeferred { .. }
                | ApiError::MaterializePartial { .. }
                | ApiError::Internal(_)
                | ApiError::StorageAccessDenied { .. }
                | ApiError::CatalogCredentialsNotVended { .. }
                | ApiError::Http { .. }
                | ApiError::IndexTimeout(_)
                | ApiError::IndexingDisabled
                | ApiError::StorageInUse { .. }
                | ApiError::AwaitTNotReached { .. }
                | ApiError::CommitContention { .. }
                | ApiError::ReindexConflict { .. }
                | ApiError::Policy(_)
                | ApiError::Indexer(_)
                | ApiError::Builder(_)
                | ApiError::CrossLedger(_) => {}
                #[cfg(feature = "vector")]
                ApiError::Vector(_) => {}
                #[cfg(feature = "credential")]
                ApiError::Credential(_) => {}
                #[cfg(feature = "iceberg")]
                ApiError::Iceberg(_) => {}
            }
        }
        rows
    }

    /// The one table, row by row, and the same answer whether an error
    /// reaches the response typed or flattened to `ApiError::Http` (the
    /// consensus committer, shared ledger loads, tracked queries).
    #[test]
    fn every_variant_answers_the_same_typed_or_flattened() {
        let rows = every_variant();
        let variants: std::collections::HashSet<_> = rows
            .iter()
            .map(|(e, ..)| std::mem::discriminant(e))
            .collect();
        assert_eq!(variants.len(), VARIANTS, "every_variant misses a variant");

        for (typed, status, code) in rows {
            assert_eq!(typed.status_code(), status, "{typed:?}");
            assert_eq!(typed.error_type(), code, "{typed:?}");
            let flattened = ApiError::http(status, typed.to_string());
            assert_eq!(flattened.status_code(), status, "{flattened:?}");
            // A client error must not read as a fault, in either form.
            for form in [&typed, &flattened] {
                assert!(
                    form.error_type() != INTERNAL || status >= 500,
                    "{status} answers {INTERNAL}: {form:?}"
                );
            }
        }
    }

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

    /// A nameservice lookup that fails while a query runs is the backend's
    /// fault: a 500 typed like `ApiError::NameService`, never the generic 400.
    #[test]
    fn a_failed_nameservice_lookup_in_a_query_is_500() {
        let e = ApiError::Query(fluree_db_query::QueryError::Nameservice("down".into()));
        assert_eq!(e.status_code(), 500);
        assert_eq!(e.error_type(), fluree_vocab::errors::NAMESERVICE);
    }

    #[test]
    fn only_actual_policy_denials_are_forbidden() {
        use fluree_db_policy::PolicyError;
        use fluree_db_transact::TransactError;

        let denied = || PolicyError::modify_denied("write refused");
        assert_eq!(ApiError::Policy(denied()).status_code(), 403);
        assert_eq!(
            ApiError::Transact(TransactError::PolicyViolation(denied())).status_code(),
            403
        );
        assert_eq!(
            ApiError::Transact(TransactError::InvalidTerm("bad term".into())).status_code(),
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
