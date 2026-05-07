//! Composable view abstraction for ledger queries
//!
//! This module provides `GraphDb`, a first-class, composable view of a ledger
//! snapshot. It follows a "db is a value" pattern where you create a view
//! once and query it repeatedly.
//!
//! # Design Philosophy
//!
//! - **View is the stable value**: `GraphDb` is the immutable snapshot you pass to queries
//! - **Query syntax is polymorphic**: One entrypoint accepts JSON-LD or SPARQL via `QueryInput`
//! - **Wrappers apply uniformly**: Reasoning/policy apply regardless of query syntax
//! - **Single-ledger scope**: A `GraphDb` represents one ledger; use `query_connection` for multi-ledger
//!
//! # Composition
//!
//! Views support wrapper composition:
//!
//! ```ignore
//! use fluree_db_api::{Fluree, GraphDb};
//! use fluree_db_query::ir::ReasoningModes;
//!
//! // Load a base view
//! let view = fluree.db("mydb:main").await?;
//!
//! // Compose wrappers (order-independent)
//! let view = view
//!     .with_policy(policy)
//!     .with_reasoning(ReasoningModes::owl2ql());
//!
//! // Query the composed view (JSON-LD or SPARQL)
//! let result = fluree.query(&view, &query_json).await?;
//! let result = fluree.query(&view, "SELECT * WHERE { ?s ?p ?o }").await?;
//! ```
//!
//! # Wrapper Semantics
//!
//! ## Time Travel (`as_of`)
//!
//! Adjusts the view's time bound. For proper historical queries with index pruning,
//! construct from `HistoricalLedgerView` instead.
//!
//! ## Policy (`with_policy`)
//!
//! Attaches a `PolicyContext` and `QueryPolicyEnforcer` for access control.
//! Policy applies to both query execution and result formatting (expansion).
//!
//! ## Reasoning (`with_reasoning`)
//!
//! Applies default reasoning modes (RDFS, OWL2-QL, OWL2-RL, datalog) to queries.
//! Precedence controls whether query-specified reasoning overrides the wrapper:
//!
//! - `DefaultUnlessQueryOverrides`: Query reasoning wins if specified (default)
//! - `Force`: Wrapper reasoning always applies
//!
//! # Single-Ledger Constraint
//!
//! A `GraphDb` represents one ledger snapshot. SPARQL queries with `FROM`/`FROM NAMED`
//! dataset clauses are rejected; use `query_connection_sparql` for multi-ledger queries.

mod dataset;
mod dataset_builder;
mod dataset_query;
mod fluree_ext;
mod query;
mod query_builder;
mod query_input;
mod types;

pub use dataset::DataSetDb;
pub use query_input::QueryInput;
pub use types::{DerivedFactsHandle, GraphDb, ReasoningModePrecedence};
