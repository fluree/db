//! SPARQL graph pattern types.
//!
//! This module defines the AST for SPARQL graph patterns before lowering
//! to the query algebra. All nodes carry source spans for diagnostics.

use super::expr::Expression;
use super::path::PropertyPath;
use super::query::{GroupByClause, SelectVariables};
use super::term::{Iri, ObjectTerm, PredicateTerm, SubjectTerm, Term, Var};
use crate::span::SourceSpan;

/// A triple pattern in SPARQL.
///
/// Represents `subject predicate object` in a WHERE clause.
#[derive(Clone, Debug, PartialEq)]
pub struct TriplePattern {
    /// The subject
    pub subject: SubjectTerm,
    /// The predicate
    pub predicate: PredicateTerm,
    /// The object
    pub object: ObjectTerm,
    /// Source span covering the entire pattern
    pub span: SourceSpan,
}

impl TriplePattern {
    /// Create a new triple pattern.
    pub fn new(
        subject: SubjectTerm,
        predicate: PredicateTerm,
        object: ObjectTerm,
        span: SourceSpan,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            span,
        }
    }

    /// Get all variables referenced in this pattern.
    pub fn variables(&self) -> Vec<&Var> {
        let mut vars = Vec::with_capacity(3);
        if let SubjectTerm::Var(v) = &self.subject {
            vars.push(v);
        }
        if let PredicateTerm::Var(v) = &self.predicate {
            vars.push(v);
        }
        if let Term::Var(v) = &self.object {
            vars.push(v);
        }
        vars
    }
}

/// A graph pattern in SPARQL.
///
/// This represents the various pattern types that can appear in WHERE clauses.
/// The structure follows the SPARQL algebra with AST-level representation.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphPattern {
    /// Basic Graph Pattern - a sequence of triple patterns
    Bgp {
        patterns: Vec<TriplePattern>,
        span: SourceSpan,
    },

    /// Group graph pattern - `{ ... }`
    Group {
        patterns: Vec<GraphPattern>,
        span: SourceSpan,
    },

    /// Optional pattern - `OPTIONAL { ... }`
    Optional {
        pattern: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Union of patterns - `{ ... } UNION { ... }`
    Union {
        left: Box<GraphPattern>,
        right: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Difference (anti-join) - `{ ... } MINUS { ... }`
    Minus {
        left: Box<GraphPattern>,
        right: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Filter constraint - `FILTER (...)`
    Filter {
        /// The filter expression
        expr: Expression,
        span: SourceSpan,
    },

    /// Bind expression - `BIND (expr AS ?var)`
    Bind {
        /// The expression to bind
        expr: Expression,
        /// The variable to bind to
        var: Var,
        span: SourceSpan,
    },

    /// Inline data - `VALUES (?x ?y) { ... }`
    Values {
        /// Variables in the VALUES clause
        vars: Vec<Var>,
        /// Data rows (each row has values matching vars)
        data: Vec<Vec<Option<Term>>>,
        span: SourceSpan,
    },

    /// Named graph pattern - `GRAPH <uri> { ... }` or `GRAPH ?var { ... }`
    Graph {
        /// The graph name (IRI or variable)
        name: GraphName,
        /// The pattern within the graph
        pattern: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Service pattern - `SERVICE <uri> { ... }`
    Service {
        /// Whether SILENT is specified
        silent: bool,
        /// The service endpoint
        endpoint: ServiceEndpoint,
        /// The pattern to execute at the service
        pattern: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Sub-select - `{ SELECT ... }`
    SubSelect {
        /// The nested select query (placeholder - will be SelectQuery in Phase 3)
        query: Box<SubSelect>,
        span: SourceSpan,
    },

    /// Property path pattern - `?s path ?o`
    ///
    /// Property paths extend basic triple patterns with path expressions
    /// like transitive closure (`+`, `*`), sequence (`/`), alternative (`|`),
    /// and inverse (`^`).
    Path {
        /// The subject of the path pattern
        subject: SubjectTerm,
        /// The property path expression
        path: PropertyPath,
        /// The object of the path pattern
        object: ObjectTerm,
        /// Source span
        span: SourceSpan,
    },
}

impl GraphPattern {
    /// Get the source span of this pattern.
    pub fn span(&self) -> SourceSpan {
        match self {
            GraphPattern::Bgp { span, .. } => *span,
            GraphPattern::Group { span, .. } => *span,
            GraphPattern::Optional { span, .. } => *span,
            GraphPattern::Union { span, .. } => *span,
            GraphPattern::Minus { span, .. } => *span,
            GraphPattern::Filter { span, .. } => *span,
            GraphPattern::Bind { span, .. } => *span,
            GraphPattern::Values { span, .. } => *span,
            GraphPattern::Graph { span, .. } => *span,
            GraphPattern::Service { span, .. } => *span,
            GraphPattern::SubSelect { span, .. } => *span,
            GraphPattern::Path { span, .. } => *span,
        }
    }

    /// Create an empty BGP.
    pub fn empty_bgp(span: SourceSpan) -> Self {
        GraphPattern::Bgp {
            patterns: Vec::new(),
            span,
        }
    }

    /// Create a BGP from triple patterns.
    pub fn bgp(patterns: Vec<TriplePattern>, span: SourceSpan) -> Self {
        GraphPattern::Bgp { patterns, span }
    }

    /// Create a group pattern.
    pub fn group(patterns: Vec<GraphPattern>, span: SourceSpan) -> Self {
        GraphPattern::Group { patterns, span }
    }
}

/// A graph name in a GRAPH pattern.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphName {
    /// Named graph by IRI
    Iri(Iri),
    /// Named graph by variable
    Var(Var),
}

impl GraphName {
    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            GraphName::Iri(i) => i.span,
            GraphName::Var(v) => v.span,
        }
    }
}

/// A SERVICE endpoint.
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceEndpoint {
    /// Endpoint by IRI
    Iri(Iri),
    /// Endpoint by variable
    Var(Var),
}

impl ServiceEndpoint {
    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            ServiceEndpoint::Iri(i) => i.span,
            ServiceEndpoint::Var(v) => v.span,
        }
    }
}

/// A sub-select query nested inside a graph pattern.
///
/// Subqueries have the form `{ SELECT ... WHERE { ... } }`.
/// Note: This is a self-contained representation to avoid circular
/// dependencies with the `query` module.
#[derive(Clone, Debug, PartialEq)]
pub struct SubSelect {
    /// Whether DISTINCT modifier is present
    pub distinct: bool,
    /// Whether REDUCED modifier is present
    pub reduced: bool,
    /// Variables to select (may include aggregate expressions like COUNT)
    pub variables: SelectVariables,
    /// The WHERE clause pattern
    pub pattern: Box<GraphPattern>,
    /// GROUP BY clause (for aggregation)
    pub group_by: Option<GroupByClause>,
    /// ORDER BY variables (simplified - just variable names for now)
    pub order_by: Vec<SubSelectOrderBy>,
    /// LIMIT value
    pub limit: Option<u64>,
    /// OFFSET value
    pub offset: Option<u64>,
    /// Source span
    pub span: SourceSpan,
}

/// Order by specification for subqueries.
#[derive(Clone, Debug, PartialEq)]
pub struct SubSelectOrderBy {
    /// The variable to order by
    pub var: Var,
    /// True for descending, false for ascending
    pub descending: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::term::{Iri, Literal};

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    #[test]
    fn test_triple_pattern_creation() {
        let s = SubjectTerm::Var(Var::new("s", SourceSpan::new(0, 2)));
        let p = PredicateTerm::Iri(Iri::prefixed("foaf", "name", SourceSpan::new(3, 12)));
        let o = Term::Var(Var::new("name", SourceSpan::new(13, 18)));

        let tp = TriplePattern::new(s, p, o, SourceSpan::new(0, 18));

        let vars = tp.variables();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].name.as_ref(), "s");
        assert_eq!(vars[1].name.as_ref(), "name");
    }

    #[test]
    fn test_triple_pattern_all_bound() {
        let s = SubjectTerm::Iri(Iri::prefixed("ex", "alice", test_span()));
        let p = PredicateTerm::Iri(Iri::prefixed("foaf", "name", test_span()));
        let o = Term::Literal(Literal::string("Alice", test_span()));

        let tp = TriplePattern::new(s, p, o, test_span());

        let vars = tp.variables();
        assert!(vars.is_empty());
    }

    #[test]
    fn test_bgp_pattern() {
        let tp = TriplePattern::new(
            SubjectTerm::Var(Var::new("s", test_span())),
            PredicateTerm::Iri(Iri::prefixed("rdf", "type", test_span())),
            Term::Iri(Iri::prefixed("foaf", "Person", test_span())),
            test_span(),
        );

        let bgp = GraphPattern::bgp(vec![tp], test_span());

        assert!(matches!(bgp, GraphPattern::Bgp { patterns, .. } if patterns.len() == 1));
    }

    #[test]
    fn test_optional_pattern() {
        let inner = GraphPattern::empty_bgp(test_span());
        let optional = GraphPattern::Optional {
            pattern: Box::new(inner),
            span: test_span(),
        };

        assert!(matches!(optional, GraphPattern::Optional { .. }));
    }

    #[test]
    fn test_union_pattern() {
        let left = GraphPattern::empty_bgp(test_span());
        let right = GraphPattern::empty_bgp(test_span());
        let union = GraphPattern::Union {
            left: Box::new(left),
            right: Box::new(right),
            span: test_span(),
        };

        assert!(matches!(union, GraphPattern::Union { .. }));
    }

    #[test]
    fn test_values_pattern() {
        let vars = vec![Var::new("x", test_span()), Var::new("y", test_span())];

        let row1 = vec![
            Some(Term::Literal(Literal::integer(1, test_span()))),
            Some(Term::Literal(Literal::integer(2, test_span()))),
        ];
        let row2 = vec![
            Some(Term::Literal(Literal::integer(3, test_span()))),
            None, // UNDEF
        ];

        let values = GraphPattern::Values {
            vars,
            data: vec![row1, row2],
            span: test_span(),
        };

        match values {
            GraphPattern::Values { vars, data, .. } => {
                assert_eq!(vars.len(), 2);
                assert_eq!(data.len(), 2);
                assert!(data[1][1].is_none()); // UNDEF
            }
            _ => panic!("Expected Values pattern"),
        }
    }

    #[test]
    fn test_graph_pattern_span() {
        let patterns = [
            GraphPattern::empty_bgp(SourceSpan::new(0, 10)),
            GraphPattern::Optional {
                pattern: Box::new(GraphPattern::empty_bgp(SourceSpan::new(20, 30))),
                span: SourceSpan::new(15, 35),
            },
        ];

        assert_eq!(patterns[0].span(), SourceSpan::new(0, 10));
        assert_eq!(patterns[1].span(), SourceSpan::new(15, 35));
    }
}
