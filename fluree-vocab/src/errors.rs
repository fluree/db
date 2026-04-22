//! Error type compact IRIs
//!
//! These compact IRI strings identify error types in API responses.
//! They follow the pattern: `err:category/ErrorName`
//!
//! The `err:` prefix maps to `https://ns.flur.ee/errors#` in the full JSON-LD context.
//!
//! # Example Usage
//!
//! ```json
//! {
//!   "error": "Invalid query: missing WHERE clause",
//!   "status": 400,
//!   "@type": "err:db/InvalidQuery"
//! }
//! ```
//!
//! Full expansion (for documentation):
//! ```json
//! {
//!   "@context": {
//!     "err": "https://ns.flur.ee/errors#"
//!   },
//!   "error": "Invalid query: missing WHERE clause",
//!   "status": 400,
//!   "@type": "err:db/InvalidQuery"
//! }
//! ```

/// Error namespace prefix
pub const ERR_PREFIX: &str = "err:";

// =============================================================================
// Database Errors (db)
// =============================================================================

/// Invalid query error (malformed syntax, missing clauses, etc.)
pub const INVALID_QUERY: &str = "err:db/InvalidQuery";

/// Invalid transaction error
pub const INVALID_TRANSACTION: &str = "err:db/InvalidTransaction";

/// Query execution error
pub const QUERY_EXECUTION: &str = "err:db/QueryExecution";

/// Transaction execution error
pub const TRANSACTION_EXECUTION: &str = "err:db/TransactionExecution";

/// Ledger not found
pub const LEDGER_NOT_FOUND: &str = "err:db/LedgerNotFound";

/// Ledger already exists
pub const LEDGER_EXISTS: &str = "err:db/LedgerExists";

/// Novelty at maximum size (backpressure)
pub const NOVELTY_AT_MAX: &str = "err:db/NoveltyAtMax";

/// Commit conflict (concurrent modification)
pub const COMMIT_CONFLICT: &str = "err:db/CommitConflict";

/// Empty transaction (no flakes)
pub const EMPTY_TRANSACTION: &str = "err:db/EmptyTransaction";

/// Graph source not found
pub const GRAPH_SOURCE_NOT_FOUND: &str = "err:db/GraphSourceNotFound";

/// Graph source index stale
pub const GRAPH_SOURCE_STALE: &str = "err:db/GraphSourceStale";

// =============================================================================
// API Errors (api)
// =============================================================================

/// Missing required parameter
pub const MISSING_PARAMETER: &str = "err:api/MissingParameter";

/// Invalid header value
pub const INVALID_HEADER: &str = "err:api/InvalidHeader";

/// Missing ledger alias
pub const MISSING_LEDGER: &str = "err:api/MissingLedger";

/// Bad request (generic)
pub const BAD_REQUEST: &str = "err:api/BadRequest";

/// Not implemented
pub const NOT_IMPLEMENTED: &str = "err:api/NotImplemented";

/// Not found (generic resource not found)
pub const NOT_FOUND: &str = "err:api/NotFound";

/// Not acceptable (content negotiation failure)
pub const NOT_ACCEPTABLE: &str = "err:api/NotAcceptable";

// =============================================================================
// Parsing Errors (parse)
// =============================================================================

/// JSON parsing error
pub const JSON_PARSE: &str = "err:parse/JsonParse";

/// JSON-LD parsing error
pub const JSONLD_PARSE: &str = "err:parse/JsonLdParse";

/// SPARQL parsing error
pub const SPARQL_PARSE: &str = "err:parse/SparqlParse";

/// SPARQL lowering error
pub const SPARQL_LOWER: &str = "err:parse/SparqlLower";

/// Turtle parsing error
pub const TURTLE_PARSE: &str = "err:parse/TurtleParse";

/// Type coercion error (incompatible @value/@type combination)
pub const TYPE_COERCION: &str = "err:parse/TypeCoercion";

// =============================================================================
// Storage Errors (storage)
// =============================================================================

/// Storage read failure
pub const STORAGE_READ: &str = "err:storage/ReadFailure";

/// Storage write failure
pub const STORAGE_WRITE: &str = "err:storage/WriteFailure";

/// Connection error
pub const CONNECTION: &str = "err:storage/ConnectionError";

// =============================================================================
// Policy/Auth Errors (policy)
// =============================================================================

/// Access denied by policy
pub const ACCESS_DENIED: &str = "err:policy/AccessDenied";

/// Unauthorized (missing/invalid credentials)
pub const UNAUTHORIZED: &str = "err:policy/Unauthorized";

/// Invalid credential
pub const INVALID_CREDENTIAL: &str = "err:policy/InvalidCredential";

/// Policy violation
pub const POLICY_VIOLATION: &str = "err:policy/PolicyViolation";

// =============================================================================
// System Errors (system)
// =============================================================================

/// Internal server error (catch-all)
pub const INTERNAL: &str = "err:system/InternalError";

/// Nameservice error
pub const NAMESERVICE: &str = "err:system/NameServiceError";

/// Configuration error
pub const CONFIG: &str = "err:system/ConfigError";

/// Format error
pub const FORMAT: &str = "err:system/FormatError";

/// Indexing error
pub const INDEXING: &str = "err:system/IndexingError";

/// BM25 index error
pub const BM25: &str = "err:system/Bm25Error";

/// Index timeout
pub const INDEX_TIMEOUT: &str = "err:system/IndexTimeout";

/// Indexing disabled
pub const INDEXING_DISABLED: &str = "err:system/IndexingDisabled";

/// Reindex conflict
pub const REINDEX_CONFLICT: &str = "err:system/ReindexConflict";

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if a string is an error type IRI
pub fn is_error_type(s: &str) -> bool {
    s.starts_with(ERR_PREFIX)
}

/// Get the category from an error type (e.g., "db" from "err:db/InvalidQuery")
pub fn error_category(error_type: &str) -> Option<&str> {
    error_type.strip_prefix(ERR_PREFIX)?.split('/').next()
}

/// Get the error name from an error type (e.g., "InvalidQuery" from "err:db/InvalidQuery")
pub fn error_name(error_type: &str) -> Option<&str> {
    error_type.strip_prefix(ERR_PREFIX)?.split('/').nth(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_error_type() {
        assert!(is_error_type(INVALID_QUERY));
        assert!(is_error_type("err:custom/MyError"));
        assert!(!is_error_type("not:an/error"));
    }

    #[test]
    fn test_error_category() {
        assert_eq!(error_category(INVALID_QUERY), Some("db"));
        assert_eq!(error_category(JSON_PARSE), Some("parse"));
        assert_eq!(error_category(INTERNAL), Some("system"));
    }

    #[test]
    fn test_error_name() {
        assert_eq!(error_name(INVALID_QUERY), Some("InvalidQuery"));
        assert_eq!(error_name(JSON_PARSE), Some("JsonParse"));
        assert_eq!(error_name(INTERNAL), Some("InternalError"));
    }
}
