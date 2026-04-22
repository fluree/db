//! Policy query evaluation trait (async)
//!
//! This module defines the async trait for evaluating policy queries (f:query).
//! The trait allows the policy crate to delegate query execution to the
//! query engine without creating a circular dependency.
//!
//! # Design
//!
//! - `PolicyQueryExecutor` trait is implemented by the caller (e.g., fluree-db-query)
//! - The implementation converts `PolicyQuery` to the query engine's IR
//! - Queries are executed with a "root" context (no policy filtering)
//! - Returns true if any results are found
//!
//! # Async Architecture
//!
//! The trait is async to avoid blocking inside async contexts (which causes deadlocks).
//! This enables "async all the way" policy enforcement in the query pipeline.
//!
//! The `PolicyQueryFut` type alias handles Send bounds differently for WASM vs native:
//! - Native: `Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>`
//! - WASM: `Pin<Box<dyn Future<Output = Result<bool>> + 'a>>` (no Send requirement)

use crate::types::PolicyQuery;
use crate::Result;
use core::future::Future;
use core::pin::Pin;
use fluree_db_core::Sid;
use std::collections::HashMap;

/// Future type for policy query evaluation.
///
/// Uses conditional compilation to handle Send bounds:
/// - Native targets require Send for multi-threaded runtimes
/// - WASM targets don't support Send (single-threaded)
#[cfg(not(target_arch = "wasm32"))]
pub type PolicyQueryFut<'a> = Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub type PolicyQueryFut<'a> = Pin<Box<dyn Future<Output = Result<bool>> + 'a>>;

/// Async trait for evaluating policy queries
///
/// Implementations must execute the policy query against the database
/// with the provided variable bindings and return whether any results
/// were found.
///
/// # Variable Bindings
///
/// The `bindings` parameter contains pre-bound special variables:
/// - `?$this` - The subject being checked
/// - `?$identity` - The requesting identity
/// - Additional user-provided policy values
///
/// # Execution Context
///
/// Policy queries must be executed with a "root" context:
/// - Same database state (same `t`, same overlays/staged view)
/// - NO policy filtering (to avoid recursion and ensure policy
///   evaluation can access all data needed for authorization)
///
/// # Implementation Notes
///
/// Implementations should convert the `PolicyQuery` JSON to the query engine's IR,
/// inject the bindings as a VALUES clause, execute with root context (no policy
/// filtering to avoid recursion), and return `true` if any results are found.
///
/// # Platform Support
///
/// On native targets, the trait requires `Send + Sync` for multi-threaded runtimes.
/// On WASM, these bounds are removed since WASM is single-threaded.
#[cfg(not(target_arch = "wasm32"))]
pub trait PolicyQueryExecutor: Send + Sync {
    /// Evaluate a policy query with the given variable bindings
    ///
    /// # Arguments
    ///
    /// * `query` - The policy query to evaluate
    /// * `bindings` - Pre-bound special variables (?$this, ?$identity, etc.)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Query returned at least one result (allow)
    /// * `Ok(false)` - Query returned no results (continue to next policy)
    /// * `Err(_)` - Query execution failed
    fn evaluate_policy_query<'a>(
        &'a self,
        query: &'a PolicyQuery,
        bindings: &'a HashMap<String, Sid>,
    ) -> PolicyQueryFut<'a>;
}

/// WASM version without Send + Sync bounds (single-threaded environment)
#[cfg(target_arch = "wasm32")]
pub trait PolicyQueryExecutor {
    fn evaluate_policy_query<'a>(
        &'a self,
        query: &'a PolicyQuery,
        bindings: &'a HashMap<String, Sid>,
    ) -> PolicyQueryFut<'a>;
}

/// A no-op executor that treats all query policies as deny
///
/// Used when no query executor is provided. This is the conservative
/// default - query policies won't allow access, but Allow/Deny policies
/// still work correctly.
pub struct NoOpQueryExecutor;

impl PolicyQueryExecutor for NoOpQueryExecutor {
    fn evaluate_policy_query<'a>(
        &'a self,
        _query: &'a PolicyQuery,
        _bindings: &'a HashMap<String, Sid>,
    ) -> PolicyQueryFut<'a> {
        Box::pin(async move {
            // Conservative default: query policies don't allow
            // This means Allow/Deny policies work, but Query policies
            // always continue to next policy (like deny, but not short-circuit)
            Ok(false)
        })
    }
}
