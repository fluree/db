//! Error types for query execution

use crate::binding::BatchError;
use crate::eval::{ArithmeticError, ComparisonError};
use thiserror::Error;

/// Query execution errors
#[derive(Error, Debug)]
pub enum QueryError {
    /// Error from fluree-db-core
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Batch construction error
    #[error("Batch error: {0}")]
    Batch(#[from] BatchError),

    /// R2RML materialization error
    #[error("R2RML error: {0}")]
    R2rml(#[from] fluree_db_r2rml::R2rmlError),

    /// Operator not opened
    #[error("Operator not opened - call open() before next_batch()")]
    OperatorNotOpened,

    /// Operator already opened
    #[error("Operator already opened")]
    OperatorAlreadyOpened,

    /// Operator is closed
    #[error("Operator is closed")]
    OperatorClosed,

    /// Variable not found
    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    /// Index selection failed
    #[error("No suitable index for query pattern")]
    NoSuitableIndex,

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Invalid filter expression
    #[error("Invalid filter: {0}")]
    InvalidFilter(String),

    /// Invalid expression (function/BIND evaluation error)
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    /// Dictionary lookup or encoded-value decode failed during query execution
    #[error("Dictionary lookup failed: {0}")]
    DictionaryLookup(String),

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Fuel limit exceeded
    #[error(transparent)]
    FuelLimitExceeded(#[from] fluree_db_core::FuelExceededError),

    /// Query execution was cancelled cooperatively.
    #[error("Query cancelled: {reason}")]
    Cancelled {
        reason: fluree_db_core::QueryCancellationReason,
    },

    /// Object storage denied a read of an external table's data (S3 403 /
    /// `AccessDenied`).
    ///
    /// Kept iceberg-agnostic (plain fields, no crate dependency): the API layer
    /// lifts `IcebergError::StorageAccessDenied` into this so the server can
    /// surface HTTP 403 instead of a generic 400/500. Because S3 also returns
    /// `AccessDenied` for a missing object without `s3:ListBucket`, this means
    /// the credentials lack access **or** the object was moved/removed.
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
    /// Fail-closed: the scan is refused rather than silently downgrading to
    /// ambient (process-default) AWS credentials.
    #[error(
        "Catalog {catalog_uri} authorized the table but vended no storage credentials; \
         either fix the catalog's credential vending or set vended_credentials=false on \
         the source to explicitly use ambient AWS credentials"
    )]
    CatalogCredentialsNotVended {
        /// The REST catalog URI that authorized the table.
        catalog_uri: String,
    },

    /// Internal error (should not happen in normal operation)
    #[error("Internal error: {0}")]
    Internal(String),

    /// Policy evaluation error
    #[error("Policy error: {0}")]
    Policy(String),

    /// Query mode not yet supported with binary indexes
    #[error("Unsupported mode: {0}")]
    UnsupportedMode(String),

    /// A syntactically valid query feature is not yet implemented.
    ///
    /// Distinguished from [`Self::UnsupportedMode`] (which is mode-bound) and
    /// [`Self::InvalidQuery`] (which is user-error). Examples: edge
    /// annotations parsed but no executor wired, deferred property-path
    /// shapes, etc.
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    /// Requested time range not covered by binary index
    #[error("Time range not covered: requested t={requested_t} but base_t={base_t}")]
    TimeRangeNotCovered { requested_t: i64, base_t: i64 },

    /// Arithmetic error during expression evaluation
    #[error("Arithmetic error: {0}")]
    Arithmetic(#[from] ArithmeticError),

    /// Comparison error during expression evaluation
    #[error("Comparison error: {0}")]
    Comparison(#[from] ComparisonError),
}

impl QueryError {
    /// Create a dictionary lookup failure with debug context.
    pub fn dictionary_lookup(msg: impl Into<String>) -> Self {
        Self::DictionaryLookup(msg.into())
    }

    /// Returns true when an expression error should degrade to false/unbound
    /// under normal SPARQL evaluation instead of aborting the query.
    pub fn can_demote_in_expression(&self) -> bool {
        matches!(
            self,
            Self::InvalidFilter(_)
                | Self::InvalidExpression(_)
                | Self::Arithmetic(_)
                | Self::Comparison(_)
        )
    }

    /// Returns true when an expression error should leave the variable UNBOUND
    /// for a SELECT/BIND/ORDER-BY solution (SPARQL 1.1 §18.5 `Extend`) instead
    /// of aborting the query.
    ///
    /// Narrower than [`Self::can_demote_in_expression`]: only *dynamic value*
    /// errors (arithmetic on incompatible operands, comparison errors) demote.
    /// *Structural* errors — a built-in called with the wrong arity, an unknown
    /// datatype IRI ([`Self::InvalidExpression`]), or a malformed filter
    /// ([`Self::InvalidFilter`]) — describe a malformed query, not dirty data, so
    /// they still surface as a query error. (Dynamic type mismatches already
    /// evaluate to `Ok(None)` → unbound without raising an error at all.)
    pub fn demotes_to_unbound_in_extend(&self) -> bool {
        matches!(self, Self::Arithmetic(_) | Self::Comparison(_))
    }

    /// Create an execution error (runtime configuration/environment issue).
    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Convert an `io::Error` to a `QueryError`, preserving fuel-exhaustion
    /// errors (which `BinaryGraphView`/`BinaryCursor` smuggle through as
    /// `io::Error::other(FuelExceededError)`).
    pub fn from_io(context: &str, err: std::io::Error) -> Self {
        if let Some(fe) = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<fluree_db_core::FuelExceededError>())
        {
            return Self::FuelLimitExceeded(fe.clone());
        }
        Self::Internal(format!("{context}: {err}"))
    }
}

/// Result type for query operations
pub type Result<T> = std::result::Result<T, QueryError>;

#[cfg(test)]
mod tests {
    use super::QueryError;

    #[test]
    fn can_demote_expression_errors_only() {
        assert!(QueryError::InvalidFilter("bad regex".into()).can_demote_in_expression());
        assert!(QueryError::InvalidExpression("bad bind".into()).can_demote_in_expression());
        assert!(
            !QueryError::dictionary_lookup("missing string id".to_string())
                .can_demote_in_expression()
        );
        assert!(!QueryError::Internal("runtime failure".into()).can_demote_in_expression());
    }

    #[test]
    fn extend_demotes_only_dynamic_value_errors() {
        // §18.5 Extend: dynamic value errors leave the variable unbound.
        assert!(
            QueryError::Arithmetic(crate::eval::ArithmeticError::TypeMismatch)
                .demotes_to_unbound_in_extend()
        );
        // Structural errors (arity, unknown datatype IRI) stay query errors.
        assert!(
            !QueryError::InvalidExpression("IRI requires exactly 1 argument".into())
                .demotes_to_unbound_in_extend()
        );
        assert!(!QueryError::InvalidFilter("bad regex".into()).demotes_to_unbound_in_extend());
        // Fatal infrastructure errors always propagate.
        assert!(
            !QueryError::dictionary_lookup("missing string id".to_string())
                .demotes_to_unbound_in_extend()
        );
    }

    #[test]
    fn storage_access_denied_display_names_object_and_region() {
        let e = QueryError::StorageAccessDenied {
            bucket: "b".to_string(),
            key: "warehouse/t/data/f.parquet".to_string(),
            region: Some("us-east-2".to_string()),
            message: "service error: AccessDenied".to_string(),
        };
        let shown = e.to_string();
        assert!(
            shown.contains("s3://b/warehouse/t/data/f.parquet"),
            "{shown}"
        );
        assert!(shown.contains("region us-east-2"), "{shown}");

        // Region is omitted cleanly when unknown.
        let no_region = QueryError::StorageAccessDenied {
            bucket: "b".to_string(),
            key: "k".to_string(),
            region: None,
            message: "m".to_string(),
        };
        assert_eq!(
            no_region.to_string(),
            "Storage access denied for s3://b/k: m"
        );
    }

    #[test]
    fn catalog_credentials_not_vended_display_is_actionable() {
        let e = QueryError::CatalogCredentialsNotVended {
            catalog_uri: "https://catalog.example/v1".to_string(),
        };
        let shown = e.to_string();
        assert!(shown.contains("https://catalog.example/v1"), "{shown}");
        assert!(shown.contains("vended_credentials=false"), "{shown}");
    }
}
