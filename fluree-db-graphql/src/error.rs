//! Errors surfaced through the GraphQL `errors` envelope.

/// A GraphQL-facing error. `code` becomes `extensions.code`, which is what
/// Apollo-style clients branch on.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The document could not be parsed.
    #[error("{0}")]
    Parse(String),
    /// The schema model could not be built or registered.
    #[error("{0}")]
    Schema(String),
    /// The document is valid GraphQL but cannot be expressed as a Fluree query.
    #[error("{0}")]
    Lower(String),
    /// The underlying query or transaction failed.
    #[error("{0}")]
    Execution(String),
}

impl Error {
    /// The `extensions.code` value for this error.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Parse(_) => "GRAPHQL_PARSE_FAILED",
            Error::Schema(_) => "SCHEMA_ERROR",
            Error::Lower(_) => "UNSUPPORTED_QUERY",
            Error::Execution(_) => "EXECUTION_ERROR",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
