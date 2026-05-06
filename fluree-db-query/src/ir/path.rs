//! Property-path patterns: extending what users can write against the
//! standard graph (transitive predicate traversal), independent of where
//! the data lives.

use super::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_core::Sid;

/// Property path modifier (transitive operators)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathModifier {
    /// + : one or more (at least one hop)
    OneOrMore,
    /// * : zero or more (includes starting node)
    ZeroOrMore,
}

/// Resolved property path pattern for transitive traversal.
///
/// Produced by `@path` aliases with `+` or `*` modifiers, e.g.:
/// `{"@context": {"knowsPlus": {"@path": "ex:knows+"}}, "where": [{"@id": "ex:alice", "knowsPlus": "?who"}]}`
#[derive(Debug, Clone)]
pub struct PropertyPathPattern {
    /// Subject ref (Var or Sid — literals not allowed)
    pub subject: Ref,
    /// Predicate to traverse (always resolved to Sid)
    pub predicate: Sid,
    /// Path modifier (+ or *)
    pub modifier: PathModifier,
    /// Object ref (Var or Sid — literals not allowed)
    pub object: Ref,
}

impl PropertyPathPattern {
    /// Create a new property path pattern
    pub fn new(subject: Ref, predicate: Sid, modifier: PathModifier, object: Ref) -> Self {
        Self {
            subject,
            predicate,
            modifier,
            object,
        }
    }

    fn positional_vars(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(2);
        if let Ref::Var(v) = &self.subject {
            vars.push(*v);
        }
        if let Ref::Var(v) = &self.object {
            vars.push(*v);
        }
        vars
    }

    /// Variables mentioned in this pattern (subject and object slots).
    pub fn referenced_vars(&self) -> Vec<VarId> {
        self.positional_vars()
    }

    /// Variables this pattern adds to the binding set.
    pub fn produced_vars(&self) -> Vec<VarId> {
        self.positional_vars()
    }
}
