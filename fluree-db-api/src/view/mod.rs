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

/// Bind the single-graph [`ContextConfig`](fluree_db_query::execute::ContextConfig)
/// for a `GraphDb` view.
///
/// The borrowed provider maps (`spatial`/`fulltext`) must outlive the config,
/// which holds references into them, so they are bound as locals in the
/// caller's scope; the macro then binds `$cfg` to the assembled config. Shared
/// by the buffered, tracked, and streaming single-ledger execution paths so
/// their context wiring stays identical (previously three hand-synced copies).
///
/// `$r2rml` is the value for the `r2rml` field: `Some((provider, table))` for
/// the R2RML-aware paths, `None` for the streaming path.
macro_rules! view_context_config {
    ($cfg:ident, $self:expr, $db:expr, $executable:expr, $tracker:expr, $options:expr, $r2rml:expr $(,)?) => {
        let __db = $db;
        let __spatial_map = __db.binary_store.as_ref().map(|s| s.spatial_provider_map());
        // Perf guardrail: skip fulltext arena map + `"en"` lang_id resolution
        // for queries that don't actually call `fulltext(...)`. The setup cost
        // (HashMap clone over every (graph, predicate, language) arena plus one
        // lang dict probe) is real on wide ledgers — an unrelated query
        // shouldn't pay it.
        let __uses_fulltext = $executable.uses_fulltext();
        let __fulltext_map = if __uses_fulltext {
            __db.binary_store
                .as_ref()
                .map(|s| s.fulltext_provider_map())
        } else {
            None
        };
        let __english_lang_id = if __uses_fulltext {
            __db.binary_store
                .as_ref()
                .and_then(|s| s.resolve_lang_id("en"))
        } else {
            None
        };
        let $cfg = ContextConfig {
            tracker: Some($tracker),
            cancellation: $options.cancellation.clone(),
            policy_enforcer: __db.policy_enforcer().cloned(),
            r2rml: $r2rml,
            binary_store: __db.binary_store.clone(),
            binary_g_id: __db.graph_id,
            dict_novelty: __db.dict_novelty.clone(),
            spatial_providers: __spatial_map.as_ref(),
            fulltext_providers: __fulltext_map.as_ref(),
            english_lang_id: __english_lang_id,
            remote_service: $self.remote_service_executor(),
            strict_bind_errors: true,
            ..Default::default()
        };
    };
}

mod dataset;
mod dataset_builder;
mod dataset_query;
mod fluree_ext;
pub(crate) mod query;
mod query_builder;
mod query_input;
mod stream_query;
mod types;

pub use dataset::DataSetDb;
pub use query_input::QueryInput;
pub use stream_query::{OwnedStreamQuery, StreamDatasetPlan, StreamQueryPlan};
pub use types::{ConfigReasoningBudget, DerivedFactsHandle, GraphDb, ReasoningModePrecedence};
