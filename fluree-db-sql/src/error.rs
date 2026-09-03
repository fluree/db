//! Error type for the SQL graph source.

/// Errors from configuring, rendering, or executing a SQL graph-source scan.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// The graph-source config is malformed or internally inconsistent.
    #[error("SQL graph source configuration error: {0}")]
    Config(String),

    /// Credential material could not be resolved or the endpoint refused it.
    #[error("SQL graph source authentication error: {0}")]
    Auth(String),

    /// Transport-level failure talking to the SQL endpoint.
    #[error("SQL endpoint HTTP error: {0}")]
    Http(String),

    /// The endpoint accepted the statement and then reported a failure.
    #[error("SQL statement failed: {0}")]
    Query(String),

    /// A value or type on the wire could not be turned into a column.
    #[error("SQL result decode error: {0}")]
    Decode(String),

    /// Something the SQL graph source deliberately does not do.
    #[error("unsupported by SQL graph sources: {0}")]
    Unsupported(String),
}

impl From<fluree_db_iceberg::IcebergError> for SqlError {
    fn from(e: fluree_db_iceberg::IcebergError) -> Self {
        // Only the shared config/auth machinery is reachable through this
        // conversion; anything else from that crate would be a wiring mistake.
        match e {
            fluree_db_iceberg::IcebergError::Config(m) => SqlError::Config(m),
            other => SqlError::Auth(other.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqlError>;
