//! Error types for the Fluree DB API

use crate::format::FormatError;
use thiserror::Error;

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

    /// Core/Storage errors
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// JSON serialization errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration errors
    #[error("Invalid configuration: {0}")]
    Config(String),

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

    /// Internal errors (ledger_info, etc.)
    #[error("Internal error: {0}")]
    Internal(String),

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
}

impl ApiError {
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
            ApiError::InvalidBranch(_) => 400,
            ApiError::BranchConflict(_) => 409,
            ApiError::NotFound(_) => 404,
            ApiError::LedgerExists(_) => 409,
            ApiError::ReindexConflict { .. } => 409,
            ApiError::IndexTimeout(_) => 504,  // Gateway Timeout
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
            // Most errors are client errors (bad input)
            ApiError::Parse(_)
            | ApiError::Query(_)
            | ApiError::Config(_)
            | ApiError::Sparql { .. }
            | ApiError::SparqlLower(_)
            | ApiError::Turtle(_)
            | ApiError::Json(_)
            | ApiError::Batch(_)
            | ApiError::Format(_) => 400,
            ApiError::Transact(
                fluree_db_transact::TransactError::CommitConflict { .. }
                | fluree_db_transact::TransactError::CommitIdMismatch { .. }
                | fluree_db_transact::TransactError::PublishLostRace { .. },
            ) => 409,
            // Other transaction errors are usually validation failures
            ApiError::Transact(_) => 400,
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
