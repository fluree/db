//! The components of a triple term, as a relation the planner can join on.
//!
//! `TermComponents(?t, s, p, o)` holds when `?t` is the triple term
//! `<<( s p o )>>`. The reified-edge lowering emits it beside the link
//! `?r rdf:reifies ?t`, so a component variable shared with another pattern
//! is a join the planner sees, and a bound subject can drive the term
//! dictionary's subject-first reverse tree instead of reading every link.

use crate::var_registry::VarId;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};

/// One component position of a [`TermComponentsPattern`].
#[derive(Debug, Clone, PartialEq)]
pub enum Component {
    /// Unconstrained: nothing reads it.
    Any,
    /// Bound by the pattern, or joined on when an earlier pattern bound it.
    Var(VarId),
    /// A constant node: subject, predicate, or IRI object.
    Node(Sid),
    /// A constant literal object, with its datatype or language tag.
    Literal(FlakeValue, DatatypeConstraint),
}

impl Component {
    pub fn var(&self) -> Option<VarId> {
        match self {
            Component::Var(v) => Some(*v),
            _ => None,
        }
    }
}

/// `TermComponents(term, subject, predicate, object)`.
#[derive(Debug, Clone, PartialEq)]
pub struct TermComponentsPattern {
    pub term: VarId,
    pub subject: Component,
    pub predicate: Component,
    pub object: Component,
}

impl TermComponentsPattern {
    pub fn components(&self) -> [&Component; 3] {
        [&self.subject, &self.predicate, &self.object]
    }

    /// The term variable, then each component variable.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        std::iter::once(self.term)
            .chain(self.components().into_iter().filter_map(Component::var))
            .collect()
    }

    pub fn produced_vars(&self) -> Vec<VarId> {
        self.referenced_vars()
    }
}
