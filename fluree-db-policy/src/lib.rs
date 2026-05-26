//! Policy enforcement for Fluree DB
//!
//! This crate provides fine-grained access control policies for queries and transactions.
//!
//! - **Targeting**: `f:onProperty`, `f:onSubject`, `f:onClass`
//! - **Evaluation**: `f:allow` (boolean) and `f:query` (conditional)
//! - **View policies**: Query-time flake filtering
//! - **Modify policies**: Transaction-time flake validation
//! - **Special variables**: `?$identity`, `?$this`
//!
//! # Core Types
//!
//! - [`PolicyWrapper`]: Container for view and modify policy sets with cheap cloning (Arc-wrapped)
//! - [`PolicyContext`]: Runtime context holding wrapper, grounded identity, and class cache
//! - [`PolicyRestriction`]: An individual policy rule with targeting and value
//! - [`PolicySet`]: Indexed collection of restrictions for O(1) lookup
//!
//! # Evaluation Semantics
//!
//! Policy evaluation follows the expected semantics:
//!
//! 1. **Schema bypass**: Schema flakes (rdfs:subClassOf, etc.) always allowed
//! 2. **Candidate collection**: Policies gathered in order (property → subject → default)
//! 3. **Class filtering**: Class policies filtered by subject membership
//! 4. **Required subset**: If any `f:required` policy applies, only evaluate required policies
//! 5. **Ordered evaluation**: Return `true` on first `Allow` or successful `Query`
//! 6. **No short-circuit on Deny**: `Deny` continues to next policy, doesn't fail immediately
//! 7. **Default fallback**: If no policies match, use `default_allow` setting
//!
//! # Usage
//!
//! Create a [`PolicyWrapper`] containing view and modify [`PolicySet`]s, then wrap it
//! in a [`PolicyContext`] with an optional identity SID. Use `PolicyWrapper::root()` for
//! unrestricted access that bypasses all policy checks.
//!
//! To check view access, call `allow_view_flake()` with the subject, property, object,
//! and subject's class memberships. For policies using `f:query` (conditional access),
//! use the async `allow_view_flake_async()` variant with a [`PolicyQueryExecutor`].
//!
//! The policy module integrates with `fluree-db-query` via `ExecutionContext::with_policy()`,
//! enabling automatic policy filtering in query operators.

mod class_lookup;
mod error;
mod evaluate;
mod index;
mod query_eval;
mod schema;
mod types;
mod wire;

pub use class_lookup::{lookup_subject_classes, populate_class_cache};
pub use error::{PolicyError, Result};
pub use evaluate::{
    build_policy_values_clause, filter_by_required, PolicyContext, UNBOUND_IDENTITY_PREFIX,
};
pub use index::{build_policy_set, compute_class_check_needed, get_all_classes_for_property};
pub use query_eval::{NoOpQueryExecutor, PolicyQueryExecutor, PolicyQueryFut};
pub use schema::is_schema_flake;
pub use types::{
    FlakePolicyEntry, PolicyAction, PolicyDecision, PolicyQuery, PolicyRestriction, PolicySet,
    PolicyValue, PolicyWrapper, PropertyPolicyEntry, TargetMode,
};
pub use wire::{
    build_policy_set_from_wire, wire_to_restrictions, PolicyArtifactWire, WireOrigin,
    WirePolicyValue, WireRestriction,
};
