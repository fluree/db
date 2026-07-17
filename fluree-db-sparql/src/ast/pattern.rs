//! SPARQL graph pattern types.
//!
//! This module defines the AST for SPARQL graph patterns before lowering
//! to the query algebra. All nodes carry source spans for diagnostics.

use super::annotation::Annotation;
use super::expr::Expression;
use super::path::PropertyPath;
use super::query::{SelectVariables, SolutionModifiers};
use super::term::{Iri, ObjectTerm, PredicateTerm, SubjectTerm, Term, Var};
use crate::span::SourceSpan;

/// A triple pattern in SPARQL.
///
/// Represents `subject predicate object` in a WHERE clause, optionally
/// followed by an RDF 1.2 annotation tail (`~ reifier? {| ... |}`).
#[derive(Clone, Debug, PartialEq)]
pub struct TriplePattern {
    /// The subject
    pub subject: SubjectTerm,
    /// The predicate
    pub predicate: PredicateTerm,
    /// The object
    pub object: ObjectTerm,
    /// Optional RDF 1.2 annotation tail. `None` for ordinary triples.
    pub annotation: Option<Annotation>,
    /// Source span covering the entire pattern (including annotation).
    pub span: SourceSpan,
}

impl TriplePattern {
    /// Create a new triple pattern with no annotation tail.
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
            annotation: None,
            span,
        }
    }

    /// Create a triple pattern carrying an annotation tail.
    pub fn with_annotation(
        subject: SubjectTerm,
        predicate: PredicateTerm,
        object: ObjectTerm,
        annotation: Annotation,
        span: SourceSpan,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            annotation: Some(annotation),
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

    /// RDF 1.2 reifier-rooted annotation pattern.
    ///
    /// Source form: `<reifier> rdf:reifies <<( s p o )>>`. The `reifier`
    /// is the annotation subject; the triple term is the reified base
    /// edge. Sibling triples about the reifier in the surrounding scope
    /// join through the executor on the reifier's variable — this AST
    /// node carries no body, by design (see
    /// `docs/concepts/edge-annotations.md` "SPARQL 1.2 / RDF 1.2 surface").
    AnnotationTarget {
        /// The annotation subject (the LHS of the `rdf:reifies` triple).
        reifier: SubjectTerm,
        /// The predicate term as written. The parser recognizes both
        /// the full IRI and the conventional `rdf:reifies` prefixed
        /// name lexically; the lower step uses the prologue to verify
        /// the prefix actually resolves to the standard rdf:reifies
        /// IRI, rejecting rebound-prefix false positives.
        predicate: super::term::PredicateTerm,
        /// The reified base edge (`<<( s p o )>>`).
        /// Boxed: `TripleTerm` embeds full subject/object terms (which
        /// can hold reified triples inline), dominating the enum size.
        triple_term: Box<super::annotation::TripleTerm>,
        /// Source span covering the entire pattern.
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
            GraphPattern::AnnotationTarget { span, .. } => *span,
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

    /// Whether this pattern must keep its enclosing `{ }` scope boundary when it
    /// is the *only* child of a group, rather than being unwrapped to bare form.
    ///
    /// A nested `{ }` block is a SPARQL §18.2.2 group graph pattern, evaluated in
    /// its own scope. `FILTER` and `BIND` read variables from that scope only, so
    /// unwrapping `{ FILTER(?v) }` / `{ BIND(expr AS ?v) }` out of its group would
    /// wrongly expose an enclosing-scope `?v` to them (W3C `filter-nested-2`,
    /// `dawg-optional-filter-005`). A nested `Group` child is itself a scope
    /// boundary and must not be flattened into its parent. Every other pattern
    /// (BGP, OPTIONAL, UNION, MINUS, GRAPH, SERVICE, sub-SELECT, path, VALUES,
    /// annotation) either introduces no enclosing-scope dependency or manages its
    /// own scope, so it unwraps safely — keeping common single-pattern groups cheap.
    ///
    /// Single source of truth for both the parser's single-child simplification
    /// (`parse_group_graph_pattern`) and the lowerer's group-scope invariant
    /// assertion (`lower_graph_pattern`).
    pub(crate) fn is_scope_sensitive(&self) -> bool {
        matches!(
            self,
            GraphPattern::Filter { .. } | GraphPattern::Bind { .. } | GraphPattern::Group { .. }
        )
    }

    /// Collect the in-scope variables of this pattern per SPARQL 1.1
    /// §18.2.1 ("Variable Scope"), appending references to `out`.
    /// Duplicates are preserved; callers needing a set should dedupe.
    ///
    /// Rules implemented:
    /// - BGP / property-path patterns: every variable in the pattern
    ///   (including annotation-tail variables).
    /// - `Group`: union of its children.
    /// - `OPTIONAL { P }`, `GRAPH g { P }`, `SERVICE e { P }`: in-scope
    ///   of `P` (plus the graph-name / endpoint variable, if any).
    /// - `{ A } UNION { B }`: union of both branches.
    /// - `A MINUS { B }`: in-scope of `A` only (the right side never
    ///   projects variables out).
    /// - `FILTER`: contributes nothing.
    /// - `BIND (expr AS ?v)`: contributes `?v`.
    /// - `VALUES`: contributes its variable list.
    /// - Sub-`SELECT`: its projection (`SELECT *` projects the in-scope
    ///   variables of its own pattern).
    pub fn add_in_scope_variables<'a>(&'a self, out: &mut Vec<&'a Var>) {
        match self {
            GraphPattern::Bgp { patterns, .. } => {
                for triple in patterns {
                    add_triple_pattern_variables(triple, out);
                }
            }
            GraphPattern::Group { patterns, .. } => {
                for p in patterns {
                    p.add_in_scope_variables(out);
                }
            }
            GraphPattern::Optional { pattern, .. } => pattern.add_in_scope_variables(out),
            GraphPattern::Union { left, right, .. } => {
                left.add_in_scope_variables(out);
                right.add_in_scope_variables(out);
            }
            GraphPattern::Minus { left, .. } => left.add_in_scope_variables(out),
            GraphPattern::Filter { .. } => {}
            GraphPattern::Bind { var, .. } => out.push(var),
            GraphPattern::Values { vars, .. } => out.extend(vars.iter()),
            GraphPattern::Graph { name, pattern, .. } => {
                if let GraphName::Var(v) = name {
                    out.push(v);
                }
                pattern.add_in_scope_variables(out);
            }
            GraphPattern::Service {
                endpoint, pattern, ..
            } => {
                if let ServiceEndpoint::Var(v) = endpoint {
                    out.push(v);
                }
                pattern.add_in_scope_variables(out);
            }
            GraphPattern::SubSelect { query, .. } => match &query.variables {
                SelectVariables::Star => query.pattern.add_in_scope_variables(out),
                SelectVariables::Explicit(items) => {
                    for item in items {
                        out.push(item.var());
                    }
                }
            },
            GraphPattern::Path {
                subject, object, ..
            } => {
                if let SubjectTerm::Var(v) = subject {
                    out.push(v);
                }
                if let Term::Var(v) = object {
                    out.push(v);
                }
            }
            GraphPattern::AnnotationTarget {
                reifier,
                triple_term,
                ..
            } => {
                if let SubjectTerm::Var(v) = reifier {
                    out.push(v);
                }
                if let SubjectTerm::Var(v) = &triple_term.subject {
                    out.push(v);
                }
                if let PredicateTerm::Var(v) = &triple_term.predicate {
                    out.push(v);
                }
                if let Term::Var(v) = &triple_term.object {
                    out.push(v);
                }
            }
        }
    }
}

/// Collect every variable of a triple pattern including its RDF 1.2
/// annotation tail (reifier variable and annotation-block entries).
///
/// `TriplePattern::variables` intentionally covers only subject /
/// predicate / object (pre-annotation callers depend on that); scope
/// computation must also see annotation-bound variables.
fn add_triple_pattern_variables<'a>(triple: &'a TriplePattern, out: &mut Vec<&'a Var>) {
    if let SubjectTerm::Var(v) = &triple.subject {
        out.push(v);
    }
    if let PredicateTerm::Var(v) = &triple.predicate {
        out.push(v);
    }
    if let Term::Var(v) = &triple.object {
        out.push(v);
    }
    if let Some(annotation) = &triple.annotation {
        for unit in &annotation.units {
            if let Some(super::annotation::ReifierId::Var(v)) = &unit.reifier {
                out.push(v);
            }
            if let Some(block) = &unit.block {
                for entry in &block.entries {
                    if let super::annotation::AnnotationVerb::Simple(PredicateTerm::Var(v)) =
                        &entry.verb
                    {
                        out.push(v);
                    }
                    if let Term::Var(v) = &entry.object {
                        out.push(v);
                    }
                }
            }
        }
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
    /// Solution modifiers (GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET), parsed
    /// with the same machinery as a top-level SELECT (`parse_solution_modifiers`)
    /// and lowered through the same `lower_solution_modifiers` path, so a
    /// subquery inherits HAVING, post-aggregation SELECT binds, and
    /// expression/aggregate ORDER BY identically.
    pub modifiers: SolutionModifiers,
    /// Trailing VALUES clause (`SubSelect ::= SelectClause WhereClause
    /// SolutionModifier ValuesClause`). Always a `GraphPattern::Values`
    /// when present.
    pub values: Option<Box<GraphPattern>>,
    /// Source span
    pub span: SourceSpan,
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

    fn in_scope_names(pattern: &GraphPattern) -> Vec<String> {
        let mut vars = Vec::new();
        pattern.add_in_scope_variables(&mut vars);
        let mut names: Vec<String> = vars.iter().map(|v| v.name.to_string()).collect();
        names.sort();
        names.dedup();
        names
    }

    fn triple(s: &str, o: &str) -> TriplePattern {
        TriplePattern::new(
            SubjectTerm::Var(Var::new(s, test_span())),
            PredicateTerm::Iri(Iri::prefixed("ex", "p", test_span())),
            Term::Var(Var::new(o, test_span())),
            test_span(),
        )
    }

    #[test]
    fn test_in_scope_variables_bgp_bind_values() {
        let bgp = GraphPattern::bgp(vec![triple("s", "o")], test_span());
        assert_eq!(in_scope_names(&bgp), vec!["o", "s"]);

        let bind = GraphPattern::Bind {
            expr: crate::ast::expr::Expression::literal(Literal::integer(1, test_span())),
            var: Var::new("b", test_span()),
            span: test_span(),
        };
        assert_eq!(in_scope_names(&bind), vec!["b"]);

        let values = GraphPattern::Values {
            vars: vec![Var::new("x", test_span()), Var::new("y", test_span())],
            data: vec![],
            span: test_span(),
        };
        assert_eq!(in_scope_names(&values), vec!["x", "y"]);
    }

    #[test]
    fn test_in_scope_variables_filter_contributes_nothing() {
        let filter = GraphPattern::Filter {
            expr: crate::ast::expr::Expression::var(Var::new("f", test_span())),
            span: test_span(),
        };
        assert!(in_scope_names(&filter).is_empty());
    }

    #[test]
    fn test_in_scope_variables_minus_right_excluded() {
        let minus = GraphPattern::Minus {
            left: Box::new(GraphPattern::bgp(vec![triple("a", "b")], test_span())),
            right: Box::new(GraphPattern::bgp(vec![triple("c", "d")], test_span())),
            span: test_span(),
        };
        assert_eq!(in_scope_names(&minus), vec!["a", "b"]);
    }

    #[test]
    fn test_in_scope_variables_union_both_branches() {
        let union = GraphPattern::Union {
            left: Box::new(GraphPattern::bgp(vec![triple("a", "b")], test_span())),
            right: Box::new(GraphPattern::bgp(vec![triple("c", "d")], test_span())),
            span: test_span(),
        };
        assert_eq!(in_scope_names(&union), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_in_scope_variables_subselect_projection() {
        // Explicit projection: only the projected variable is in scope.
        let sub = GraphPattern::SubSelect {
            query: Box::new(SubSelect {
                distinct: false,
                reduced: false,
                variables: SelectVariables::Explicit(vec![crate::ast::query::SelectVariable::Var(
                    Var::new("x", test_span()),
                )]),
                pattern: Box::new(GraphPattern::bgp(vec![triple("x", "hidden")], test_span())),
                modifiers: SolutionModifiers::default(),
                values: None,
                span: test_span(),
            }),
            span: test_span(),
        };
        assert_eq!(in_scope_names(&sub), vec!["x"]);

        // SELECT * projects the pattern's own in-scope variables.
        let sub_star = GraphPattern::SubSelect {
            query: Box::new(SubSelect {
                distinct: false,
                reduced: false,
                variables: SelectVariables::Star,
                pattern: Box::new(GraphPattern::bgp(vec![triple("x", "y")], test_span())),
                modifiers: SolutionModifiers::default(),
                values: None,
                span: test_span(),
            }),
            span: test_span(),
        };
        assert_eq!(in_scope_names(&sub_star), vec!["x", "y"]);
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
