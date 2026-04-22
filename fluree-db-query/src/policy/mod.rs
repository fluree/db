//! Policy enforcement for query execution
//!
//! This module provides async policy enforcement for the query pipeline:
//!
//! - [`QueryPolicyEnforcer`]: Filters flakes/batches by policy with caching
//! - [`QueryPolicyExecutor`]: Implements `PolicyQueryExecutor` using the query engine
//!
//! # Architecture
//!
//! Policy enforcement is async all the way through, avoiding the deadlocks that
//! occur when using `block_on` inside async contexts. The design follows:
//!
//! 1. `PolicyContext` (fluree-db-policy) holds the policy data and provides
//!    `allow_view_flake_async` which takes a `PolicyQueryExecutor`.
//!
//! 2. `QueryPolicyExecutor` (this module) implements `PolicyQueryExecutor` by
//!    running the query engine asynchronously with a root context (no policy).
//!
//! 3. `QueryPolicyEnforcer` wraps it all together with caching and provides
//!    `filter_flakes` for batch filtering in scan operators.
//!
//! # Caching
//!
//! Policy query results are cached by `(restriction_id, subject, identity)` to
//! avoid re-executing the same query for every flake. This is critical for
//! performance when policies use f:query.

mod enforcer;
mod executor;

pub use enforcer::QueryPolicyEnforcer;
pub use executor::QueryPolicyExecutor;
