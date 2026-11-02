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

    /// A graph-source identity refusal from the shared auth machinery (a
    /// Google token exchange refused, invalid Google credentials, a host
    /// identity the policy refuses), kept typed so the source reports it with
    /// the same wire code and status as an Iceberg source would.
    #[error("{0}")]
    Identity(fluree_db_iceberg::IcebergError),
}

impl SqlError {
    /// An error from the endpoint's auth provider: an identity refusal stays
    /// typed, anything else is an authentication failure.
    pub fn from_auth(e: fluree_db_iceberg::IcebergError) -> Self {
        if e.is_identity_refusal() {
            SqlError::Identity(e)
        } else {
            SqlError::Auth(e.to_string())
        }
    }
}

impl From<fluree_db_iceberg::IcebergError> for SqlError {
    fn from(e: fluree_db_iceberg::IcebergError) -> Self {
        // Only the shared config/auth machinery is reachable through this
        // conversion; anything else from that crate would be a wiring mistake.
        match e {
            fluree_db_iceberg::IcebergError::Config(m) => SqlError::Config(m),
            other => SqlError::from_auth(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqlError>;

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_iceberg::IcebergError;

    #[test]
    fn identity_refusals_stay_typed_and_the_rest_keep_their_mapping() {
        let refusals = [
            IcebergError::HostIdentityRefused {
                kind: fluree_db_iceberg::HostIdentityKind::GoogleMetadata,
            },
            IcebergError::GoogleCredentialsInvalid("field".to_string()),
            IcebergError::GoogleTokenHostRefused {
                kind: fluree_db_iceberg::HostIdentityKind::GoogleMetadata,
                host: "attacker.example".to_string(),
            },
        ];
        for refusal in refusals {
            assert!(matches!(SqlError::from(refusal), SqlError::Identity(_)));
        }
        assert!(matches!(
            SqlError::from(IcebergError::Config("c".to_string())),
            SqlError::Config(_)
        ));
        assert!(matches!(
            SqlError::from(IcebergError::Auth("a".to_string())),
            SqlError::Auth(_)
        ));
    }
}
