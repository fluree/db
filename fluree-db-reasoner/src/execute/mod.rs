//! Rule execution with predicate-indexed deltas
//!
//! This module provides the core rule execution infrastructure for OWL2-RL reasoning.
//! It uses predicate-indexed delta sets for efficient semi-naive evaluation.
//!
//! ## Module Organization
//!
//! - `delta` - DeltaSet for tracking new facts during iteration
//! - `derived` - DerivedSet for accumulated derived facts with deduplication
//! - `util` - Shared helper functions for rule implementations
//! - `property_rules` - Property-related OWL2-RL rules (prp-*)
//! - `class_rules` - Class hierarchy rules (cax-*)
//! - `restriction_rules` - OWL restriction rules (cls-*)
//! - `equality_rules` - Equality rules (eq-*)

mod class_rules;
mod delta;
mod derived;
mod equality_rules;
mod property_rules;
mod restriction_rules;
pub mod util;

// Re-export main types
pub use delta::DeltaSet;
pub use derived::DerivedSet;
pub use util::{IdentityRuleContext, RuleContext};

// Re-export property rules (prp-*)
pub use property_rules::{
    apply_domain_rule, apply_functional_property_rule, apply_has_key_rule,
    apply_inverse_functional_property_rule, apply_inverse_rule, apply_property_chain_rule,
    apply_range_rule, apply_sub_property_rule, apply_symmetric_rule, apply_transitive_rule,
};

// Re-export class rules (cax-*)
pub use class_rules::{apply_equivalent_class_rule, apply_subclass_rule};

// Re-export restriction rules (cls-*)
pub use restriction_rules::{
    apply_all_values_from_rule, apply_has_value_backward_rule, apply_has_value_forward_rule,
    apply_intersection_backward_rule, apply_intersection_forward_rule, apply_max_cardinality_rule,
    apply_max_qualified_cardinality_rule, apply_one_of_rule, apply_some_values_from_rule,
    apply_union_rule,
};

// Re-export equality rules (eq-*)
pub use equality_rules::apply_same_as_rule;

// Re-export types needed by tests and external consumers
pub use crate::ontology_rl::OntologyRL;
pub use crate::restrictions::{ClassRef, RestrictionIndex, RestrictionType, RestrictionValue};
pub use crate::same_as::SameAsTracker;
pub use crate::types::{ChainElement, PropertyChain, PropertyExpression};
pub use crate::ReasoningDiagnostics;
pub use fluree_db_core::flake::Flake;
pub use fluree_db_core::value::FlakeValue;
pub use fluree_db_core::Sid;
pub use fluree_vocab::namespaces::RDF;
pub use fluree_vocab::predicates::RDF_TYPE;
pub use hashbrown::{HashMap, HashSet};

#[cfg(test)]
mod tests;
