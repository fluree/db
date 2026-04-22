//! Remote SERVICE execution trait.
//!
//! Defines the `RemoteServiceExecutor` trait used by `ServiceOperator` to
//! execute SPARQL queries against remote Fluree instances. The trait lives
//! in `fluree-db-query` (Layer 3) to keep it HTTP-agnostic; concrete
//! implementations using `reqwest` live in `fluree-db-api` (Layer 4).
//!
//! # Endpoint scheme
//!
//! Remote Fluree endpoints use the `fluree:remote:<connection>/<ledger>` scheme:
//! - `connection` is a registered alias (configured at connection build time)
//! - `ledger` is the ledger ID on the remote server (e.g., `mydb:main`)
//!
//! The connection alias maps to a `(base_url, bearer_token)` pair.

use crate::binding::Binding;
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Prefix for remote Fluree SERVICE endpoints.
pub const FLUREE_REMOTE_PREFIX: &str = "fluree:remote:";

/// A single row of bindings returned from a remote SPARQL query.
///
/// Keys are variable names (without `?` prefix), values are `Binding` instances.
/// URIs are represented as `Binding::Iri`, literals as `Binding::Lit`.
pub type RemoteBindingRow = HashMap<Arc<str>, Binding>;

/// Result of a remote SPARQL query execution.
#[derive(Debug)]
pub struct RemoteQueryResult {
    /// Variable names from the result head (without `?` prefix).
    pub vars: Vec<Arc<str>>,
    /// Result rows, each mapping variable names to bindings.
    pub rows: Vec<RemoteBindingRow>,
}

/// Trait for executing SPARQL queries against remote Fluree endpoints.
///
/// Implementations handle HTTP transport, authentication, and response parsing.
/// The `ServiceOperator` calls through this trait when it encounters a
/// `fluree:remote:` endpoint, keeping `fluree-db-query` HTTP-free.
///
/// # Connection resolution
///
/// The `connection_name` parameter is the alias registered at build time
/// (e.g., `"acme"` from `fluree:remote:acme/mydb:main`). The implementation
/// maps this to a concrete `(base_url, bearer_token)` pair.
#[async_trait]
pub trait RemoteServiceExecutor: Debug + Send + Sync {
    /// Execute a SPARQL query against a remote Fluree ledger.
    ///
    /// # Arguments
    ///
    /// * `connection_name` — registered alias for the remote server
    /// * `ledger` — ledger ID on the remote server (e.g., `"mydb:main"`)
    /// * `sparql` — complete SPARQL query text to send
    ///
    /// # Returns
    ///
    /// Parsed result bindings, or a `QueryError` on failure.
    async fn execute_remote_sparql(
        &self,
        connection_name: &str,
        ledger: &str,
        sparql: &str,
    ) -> Result<RemoteQueryResult>;
}

/// Parse a `fluree:remote:` endpoint IRI into `(connection_name, ledger)`.
///
/// Returns `None` if the IRI doesn't match the expected format.
///
/// # Examples
///
/// ```
/// use fluree_db_query::remote_service::parse_fluree_remote_ref;
///
/// let (conn, ledger) = parse_fluree_remote_ref("fluree:remote:acme/customers:main").unwrap();
/// assert_eq!(conn, "acme");
/// assert_eq!(ledger, "customers:main");
///
/// // No ledger path
/// assert!(parse_fluree_remote_ref("fluree:remote:acme").is_none());
/// ```
pub fn parse_fluree_remote_ref(iri: &str) -> Option<(&str, &str)> {
    let rest = iri.strip_prefix(FLUREE_REMOTE_PREFIX)?;
    let slash_pos = rest.find('/')?;
    let connection = &rest[..slash_pos];
    let ledger = &rest[slash_pos + 1..];
    if connection.is_empty() || ledger.is_empty() {
        return None;
    }
    Some((connection, ledger))
}

/// Check whether an IRI uses the `fluree:remote:` scheme.
pub fn is_fluree_remote_endpoint(iri: &str) -> bool {
    iri.starts_with(FLUREE_REMOTE_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_ref_basic() {
        let (conn, ledger) = parse_fluree_remote_ref("fluree:remote:acme/customers:main").unwrap();
        assert_eq!(conn, "acme");
        assert_eq!(ledger, "customers:main");
    }

    #[test]
    fn parse_remote_ref_nested_ledger() {
        let (conn, ledger) = parse_fluree_remote_ref("fluree:remote:prod/org/mydb:main").unwrap();
        assert_eq!(conn, "prod");
        assert_eq!(ledger, "org/mydb:main");
    }

    #[test]
    fn parse_remote_ref_missing_ledger() {
        assert!(parse_fluree_remote_ref("fluree:remote:acme").is_none());
    }

    #[test]
    fn parse_remote_ref_empty_connection() {
        assert!(parse_fluree_remote_ref("fluree:remote:/ledger:main").is_none());
    }

    #[test]
    fn parse_remote_ref_empty_ledger() {
        assert!(parse_fluree_remote_ref("fluree:remote:acme/").is_none());
    }

    #[test]
    fn parse_remote_ref_wrong_prefix() {
        assert!(parse_fluree_remote_ref("fluree:ledger:mydb:main").is_none());
    }

    #[test]
    fn is_remote_checks_prefix() {
        assert!(is_fluree_remote_endpoint("fluree:remote:acme/db:main"));
        assert!(!is_fluree_remote_endpoint("fluree:ledger:db:main"));
        assert!(!is_fluree_remote_endpoint("http://example.org"));
    }
}
