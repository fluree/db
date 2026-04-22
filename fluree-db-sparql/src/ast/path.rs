//! SPARQL Property Path types.
//!
//! This module defines the AST for SPARQL 1.1 property paths.
//! Property paths allow complex traversal patterns in graph queries.
//!
//! ## SPARQL 1.1 Property Path Operators
//!
//! | Syntax | Name | Description |
//! |--------|------|-------------|
//! | `iri` | Link | Direct predicate |
//! | `^iri` | Inverse | Reverse direction |
//! | `p/q` | Sequence | Path then path |
//! | `p\|q` | Alternative | Either path |
//! | `p*` | Zero or more | Transitive closure |
//! | `p+` | One or more | Positive transitive closure |
//! | `p?` | Zero or one | Optional step |
//! | `!iri` or `!(iri\|...)` | Negated property set | Any predicate except |
//!
//! ## Fluree Support
//!
//! Fluree supports: simple predicates, `+`, `*`, `?`, `/`, `|`, `^`.
//! Not supported: depth modifiers `{n,m}`, negated property sets.

use super::term::Iri;
use crate::span::SourceSpan;

/// A property path expression in SPARQL.
///
/// Property paths can appear in the predicate position of a triple pattern.
#[derive(Clone, Debug, PartialEq)]
pub enum PropertyPath {
    /// Simple predicate (an IRI)
    Iri(Iri),

    /// The `a` keyword (rdf:type shorthand)
    A { span: SourceSpan },

    /// Inverse path: `^path`
    Inverse {
        path: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// Sequence path: `path1/path2`
    Sequence {
        left: Box<PropertyPath>,
        right: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// Alternative path: `path1|path2`
    Alternative {
        left: Box<PropertyPath>,
        right: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// Zero or more: `path*`
    ZeroOrMore {
        path: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// One or more: `path+`
    OneOrMore {
        path: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// Zero or one: `path?`
    ZeroOrOne {
        path: Box<PropertyPath>,
        span: SourceSpan,
    },

    /// Negated property set: `!iri` or `!(iri1|iri2|...)`
    ///
    /// Matches any predicate NOT in the set.
    /// Note: Not supported by Fluree engine (validator will reject).
    NegatedSet {
        iris: Vec<NegatedPredicate>,
        span: SourceSpan,
    },

    /// Parenthesized path: `(path)`
    Group {
        path: Box<PropertyPath>,
        span: SourceSpan,
    },
}

impl PropertyPath {
    /// Get the source span of this path.
    pub fn span(&self) -> SourceSpan {
        match self {
            PropertyPath::Iri(iri) => iri.span,
            PropertyPath::A { span } => *span,
            PropertyPath::Inverse { span, .. } => *span,
            PropertyPath::Sequence { span, .. } => *span,
            PropertyPath::Alternative { span, .. } => *span,
            PropertyPath::ZeroOrMore { span, .. } => *span,
            PropertyPath::OneOrMore { span, .. } => *span,
            PropertyPath::ZeroOrOne { span, .. } => *span,
            PropertyPath::NegatedSet { span, .. } => *span,
            PropertyPath::Group { span, .. } => *span,
        }
    }

    /// Check if this is a simple path (just an IRI or `a`).
    pub fn is_simple(&self) -> bool {
        matches!(self, PropertyPath::Iri(_) | PropertyPath::A { .. })
    }

    /// Check if this path uses operators not supported by Fluree.
    pub fn uses_unsupported_features(&self) -> bool {
        match self {
            PropertyPath::NegatedSet { .. } => true,
            PropertyPath::Inverse { path, .. } => path.uses_unsupported_features(),
            PropertyPath::Sequence { left, right, .. } => {
                left.uses_unsupported_features() || right.uses_unsupported_features()
            }
            PropertyPath::Alternative { left, right, .. } => {
                left.uses_unsupported_features() || right.uses_unsupported_features()
            }
            PropertyPath::ZeroOrMore { path, .. } => path.uses_unsupported_features(),
            PropertyPath::OneOrMore { path, .. } => path.uses_unsupported_features(),
            PropertyPath::ZeroOrOne { path, .. } => path.uses_unsupported_features(),
            PropertyPath::Group { path, .. } => path.uses_unsupported_features(),
            PropertyPath::Iri(_) | PropertyPath::A { .. } => false,
        }
    }

    /// Create a simple IRI path.
    pub fn iri(iri: Iri) -> Self {
        PropertyPath::Iri(iri)
    }

    /// Create an inverse path.
    pub fn inverse(path: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::Inverse {
            path: Box::new(path),
            span,
        }
    }

    /// Create a sequence path.
    pub fn sequence(left: PropertyPath, right: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::Sequence {
            left: Box::new(left),
            right: Box::new(right),
            span,
        }
    }

    /// Create an alternative path.
    pub fn alternative(left: PropertyPath, right: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::Alternative {
            left: Box::new(left),
            right: Box::new(right),
            span,
        }
    }

    /// Create a zero-or-more path.
    pub fn zero_or_more(path: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::ZeroOrMore {
            path: Box::new(path),
            span,
        }
    }

    /// Create a one-or-more path.
    pub fn one_or_more(path: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::OneOrMore {
            path: Box::new(path),
            span,
        }
    }

    /// Create a zero-or-one path.
    pub fn zero_or_one(path: PropertyPath, span: SourceSpan) -> Self {
        PropertyPath::ZeroOrOne {
            path: Box::new(path),
            span,
        }
    }
}

/// A predicate in a negated property set.
///
/// Can be a forward or inverse IRI/`a`.
#[derive(Clone, Debug, PartialEq)]
pub enum NegatedPredicate {
    /// Forward predicate
    Forward(Iri),
    /// The `a` keyword (rdf:type)
    ForwardA { span: SourceSpan },
    /// Inverse predicate `^iri`
    Inverse(Iri),
    /// Inverse `a` keyword `^a`
    InverseA { span: SourceSpan },
}

impl NegatedPredicate {
    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            NegatedPredicate::Forward(iri) => iri.span,
            NegatedPredicate::ForwardA { span } => *span,
            NegatedPredicate::Inverse(iri) => iri.span,
            NegatedPredicate::InverseA { span } => *span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    fn test_iri() -> Iri {
        Iri::prefixed("ex", "name", test_span())
    }

    #[test]
    fn test_simple_path() {
        let path = PropertyPath::iri(test_iri());
        assert!(path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_inverse_path() {
        let inner = PropertyPath::iri(test_iri());
        let path = PropertyPath::inverse(inner, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_sequence_path() {
        let left = PropertyPath::iri(test_iri());
        let right = PropertyPath::iri(test_iri());
        let path = PropertyPath::sequence(left, right, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_alternative_path() {
        let left = PropertyPath::iri(test_iri());
        let right = PropertyPath::iri(test_iri());
        let path = PropertyPath::alternative(left, right, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_zero_or_more_path() {
        let inner = PropertyPath::iri(test_iri());
        let path = PropertyPath::zero_or_more(inner, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_one_or_more_path() {
        let inner = PropertyPath::iri(test_iri());
        let path = PropertyPath::one_or_more(inner, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_zero_or_one_path() {
        let inner = PropertyPath::iri(test_iri());
        let path = PropertyPath::zero_or_one(inner, test_span());

        assert!(!path.is_simple());
        assert!(!path.uses_unsupported_features());
    }

    #[test]
    fn test_negated_set_unsupported() {
        let path = PropertyPath::NegatedSet {
            iris: vec![NegatedPredicate::Forward(test_iri())],
            span: test_span(),
        };

        assert!(!path.is_simple());
        assert!(path.uses_unsupported_features());
    }

    #[test]
    fn test_nested_path_unsupported() {
        // ^(!ex:name) - inverse of negated set should be unsupported
        let negated = PropertyPath::NegatedSet {
            iris: vec![NegatedPredicate::Forward(test_iri())],
            span: test_span(),
        };
        let inverse = PropertyPath::inverse(negated, test_span());

        assert!(inverse.uses_unsupported_features());
    }

    #[test]
    fn test_complex_supported_path() {
        // ^ex:parent/ex:child* - inverse followed by transitive
        let inverse = PropertyPath::inverse(PropertyPath::iri(test_iri()), test_span());
        let transitive = PropertyPath::one_or_more(PropertyPath::iri(test_iri()), test_span());
        let path = PropertyPath::sequence(inverse, transitive, test_span());

        assert!(!path.uses_unsupported_features());
    }
}
