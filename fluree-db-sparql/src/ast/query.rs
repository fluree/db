//! SPARQL query types.
//!
//! This module defines the top-level query AST types including
//! the prologue (BASE/PREFIX), query forms (SELECT, CONSTRUCT, etc.),
//! solution modifiers (ORDER BY, LIMIT, OFFSET), and update operations.

use super::expr::Expression;
use super::pattern::{GraphPattern, TriplePattern};
use super::term::{Iri, Var};
use super::update::UpdateOperation;
use crate::span::SourceSpan;
use std::sync::Arc;

/// A complete SPARQL query or update.
#[derive(Clone, Debug, PartialEq)]
pub struct SparqlAst {
    /// The prologue (BASE and PREFIX declarations)
    pub prologue: Prologue,
    /// The query or update body
    pub body: QueryBody,
    /// Source span for the entire query
    pub span: SourceSpan,
}

impl SparqlAst {
    /// Create a new SPARQL AST.
    pub fn new(prologue: Prologue, body: QueryBody, span: SourceSpan) -> Self {
        Self {
            prologue,
            body,
            span,
        }
    }
}

/// The body of a SPARQL query (SELECT, CONSTRUCT, ASK, DESCRIBE, or UPDATE).
#[derive(Clone, Debug, PartialEq)]
pub enum QueryBody {
    /// SELECT query
    Select(SelectQuery),
    /// CONSTRUCT query
    Construct(ConstructQuery),
    /// ASK query
    Ask(AskQuery),
    /// DESCRIBE query
    Describe(DescribeQuery),
    /// SPARQL Update operation (INSERT DATA, DELETE DATA, DELETE WHERE, etc.)
    Update(UpdateOperation),
}

/// The query prologue containing BASE and PREFIX declarations.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Prologue {
    /// Base IRI declaration
    pub base: Option<BaseDecl>,
    /// Prefix declarations
    pub prefixes: Vec<PrefixDecl>,
}

impl Prologue {
    /// Create an empty prologue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a base declaration.
    pub fn with_base(mut self, base: BaseDecl) -> Self {
        self.base = Some(base);
        self
    }

    /// Add a prefix declaration.
    pub fn with_prefix(mut self, prefix: PrefixDecl) -> Self {
        self.prefixes.push(prefix);
        self
    }

    /// Look up a prefix namespace.
    pub fn get_prefix(&self, prefix: &str) -> Option<&Arc<str>> {
        self.prefixes
            .iter()
            .find(|p| p.prefix.as_ref() == prefix)
            .map(|p| &p.iri)
    }
}

/// A BASE declaration.
#[derive(Clone, Debug, PartialEq)]
pub struct BaseDecl {
    /// The base IRI
    pub iri: Arc<str>,
    /// Source span
    pub span: SourceSpan,
}

impl BaseDecl {
    /// Create a new BASE declaration.
    pub fn new(iri: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            iri: Arc::from(iri.as_ref()),
            span,
        }
    }
}

/// A PREFIX declaration.
#[derive(Clone, Debug, PartialEq)]
pub struct PrefixDecl {
    /// The prefix (empty string for default prefix)
    pub prefix: Arc<str>,
    /// The namespace IRI
    pub iri: Arc<str>,
    /// Source span
    pub span: SourceSpan,
}

impl PrefixDecl {
    /// Create a new PREFIX declaration.
    pub fn new(prefix: impl AsRef<str>, iri: impl AsRef<str>, span: SourceSpan) -> Self {
        Self {
            prefix: Arc::from(prefix.as_ref()),
            iri: Arc::from(iri.as_ref()),
            span,
        }
    }
}

/// A SELECT query.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectQuery {
    /// Select clause (variables or expressions)
    pub select: SelectClause,
    /// Dataset clause (FROM, FROM NAMED)
    pub dataset: Option<DatasetClause>,
    /// WHERE clause
    pub where_clause: WhereClause,
    /// Solution modifiers
    pub modifiers: SolutionModifiers,
    /// Post-query VALUES clause (ValuesClause in SPARQL grammar, after SolutionModifier).
    /// Boxed to avoid inflating the size of `QueryBody::Select`.
    pub values: Option<Box<GraphPattern>>,
    /// Source span
    pub span: SourceSpan,
}

impl SelectQuery {
    /// Create a new SELECT query.
    pub fn new(
        select: SelectClause,
        where_clause: WhereClause,
        modifiers: SolutionModifiers,
        span: SourceSpan,
    ) -> Self {
        Self {
            select,
            dataset: None,
            where_clause,
            modifiers,
            values: None,
            span,
        }
    }
}

/// The SELECT clause specifying what to return.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// Modifier (DISTINCT, REDUCED, or none)
    pub modifier: Option<SelectModifier>,
    /// Variables to select (* for all, or specific list)
    pub variables: SelectVariables,
    /// Source span
    pub span: SourceSpan,
}

impl SelectClause {
    /// Create a SELECT * clause.
    pub fn star(span: SourceSpan) -> Self {
        Self {
            modifier: None,
            variables: SelectVariables::Star,
            span,
        }
    }

    /// Create a SELECT with specific variables.
    pub fn variables(vars: Vec<SelectVariable>, span: SourceSpan) -> Self {
        Self {
            modifier: None,
            variables: SelectVariables::Explicit(vars),
            span,
        }
    }
}

/// SELECT modifier (DISTINCT or REDUCED).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectModifier {
    Distinct,
    Reduced,
}

/// Variables selected by a SELECT clause.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectVariables {
    /// SELECT * - all variables
    Star,
    /// Explicit list of variables/expressions
    Explicit(Vec<SelectVariable>),
}

/// A variable or expression in a SELECT clause.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectVariable {
    /// Simple variable
    Var(Var),
    /// Expression with alias: `(expr AS ?var)`
    Expr {
        /// The expression
        expr: Expression,
        /// The alias variable
        alias: Var,
        /// Full span including parens
        span: SourceSpan,
    },
}

impl SelectVariable {
    /// Get the variable being selected.
    pub fn var(&self) -> &Var {
        match self {
            SelectVariable::Var(v) => v,
            SelectVariable::Expr { alias, .. } => alias,
        }
    }

    /// Get the source span.
    pub fn span(&self) -> SourceSpan {
        match self {
            SelectVariable::Var(v) => v.span,
            SelectVariable::Expr { span, .. } => *span,
        }
    }
}

/// Dataset clause (FROM and FROM NAMED).
///
/// Supports Fluree extension for history queries: `FROM <iri> TO <iri>`.
/// When `to_graph` is Some, this indicates a history time range query.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DatasetClause {
    /// Default graphs (FROM <iri>)
    pub default_graphs: Vec<Iri>,
    /// Named graphs (FROM NAMED <iri>)
    pub named_graphs: Vec<Iri>,
    /// End of history time range (Fluree extension: FROM <from> TO <to>)
    /// When present, indicates a history range query from default_graphs[0] to this IRI
    pub to_graph: Option<Iri>,
    /// Source span
    pub span: SourceSpan,
}

/// The WHERE clause containing the graph pattern.
#[derive(Clone, Debug, PartialEq)]
pub struct WhereClause {
    /// Whether WHERE keyword was present (it's optional)
    pub has_where_keyword: bool,
    /// The graph pattern
    pub pattern: GraphPattern,
    /// Source span
    pub span: SourceSpan,
}

impl WhereClause {
    /// Create a WHERE clause.
    pub fn new(pattern: GraphPattern, has_where_keyword: bool, span: SourceSpan) -> Self {
        Self {
            has_where_keyword,
            pattern,
            span,
        }
    }
}

/// Solution modifiers (GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SolutionModifiers {
    /// GROUP BY clause
    pub group_by: Option<GroupByClause>,
    /// HAVING clause
    pub having: Option<HavingClause>,
    /// ORDER BY clause
    pub order_by: Option<OrderByClause>,
    /// LIMIT value
    pub limit: Option<LimitClause>,
    /// OFFSET value
    pub offset: Option<OffsetClause>,
}

impl SolutionModifiers {
    /// Create empty modifiers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the ORDER BY clause.
    pub fn with_order_by(mut self, order_by: OrderByClause) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Set the LIMIT.
    pub fn with_limit(mut self, limit: LimitClause) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the OFFSET.
    pub fn with_offset(mut self, offset: OffsetClause) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set the GROUP BY clause.
    pub fn with_group_by(mut self, group_by: GroupByClause) -> Self {
        self.group_by = Some(group_by);
        self
    }

    /// Set the HAVING clause.
    pub fn with_having(mut self, having: HavingClause) -> Self {
        self.having = Some(having);
        self
    }
}

/// GROUP BY clause.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupByClause {
    /// Group conditions (variables or expressions)
    pub conditions: Vec<GroupCondition>,
    /// Source span
    pub span: SourceSpan,
}

/// A condition in GROUP BY.
#[derive(Clone, Debug, PartialEq)]
pub enum GroupCondition {
    /// Variable
    Var(Var),
    /// Expression (with optional AS alias)
    Expr {
        expr: Expression,
        alias: Option<Var>,
        span: SourceSpan,
    },
}

/// HAVING clause.
#[derive(Clone, Debug, PartialEq)]
pub struct HavingClause {
    /// Having conditions (each is a constraint expression)
    pub conditions: Vec<Expression>,
    /// Source span
    pub span: SourceSpan,
}

/// ORDER BY clause.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByClause {
    /// Order conditions
    pub conditions: Vec<OrderCondition>,
    /// Source span
    pub span: SourceSpan,
}

/// A condition in ORDER BY.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderCondition {
    /// The ordering expression
    pub expr: OrderExpr,
    /// Sort direction
    pub direction: OrderDirection,
    /// Source span
    pub span: SourceSpan,
}

/// An expression in ORDER BY.
#[derive(Clone, Debug, PartialEq)]
pub enum OrderExpr {
    /// Simple variable
    Var(Var),
    /// Complex expression
    Expr(Expression),
}

/// Sort direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending (default)
    #[default]
    Asc,
    /// Descending
    Desc,
}

/// LIMIT clause.
#[derive(Clone, Debug, PartialEq)]
pub struct LimitClause {
    /// The limit value
    pub value: u64,
    /// Source span
    pub span: SourceSpan,
}

impl LimitClause {
    /// Create a LIMIT clause.
    pub fn new(value: u64, span: SourceSpan) -> Self {
        Self { value, span }
    }
}

/// OFFSET clause.
#[derive(Clone, Debug, PartialEq)]
pub struct OffsetClause {
    /// The offset value
    pub value: u64,
    /// Source span
    pub span: SourceSpan,
}

impl OffsetClause {
    /// Create an OFFSET clause.
    pub fn new(value: u64, span: SourceSpan) -> Self {
        Self { value, span }
    }
}

// ============================================================================
// Other query forms (Phase 6)
// ============================================================================

/// CONSTRUCT query.
///
/// Builds RDF triples from a template pattern.
///
/// ```sparql
/// CONSTRUCT { ?s ex:knows ?o }
/// WHERE { ?s ex:friend ?o }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ConstructQuery {
    /// The template triples to construct.
    /// None for "CONSTRUCT WHERE { ... }" shorthand form.
    pub template: Option<ConstructTemplate>,
    /// Dataset clause (FROM, FROM NAMED)
    pub dataset: Option<DatasetClause>,
    /// WHERE clause
    pub where_clause: WhereClause,
    /// Solution modifiers (ORDER BY, LIMIT, OFFSET - no GROUP BY/HAVING for CONSTRUCT)
    pub modifiers: SolutionModifiers,
    /// Source span
    pub span: SourceSpan,
}

impl ConstructQuery {
    /// Create a new CONSTRUCT query.
    pub fn new(
        template: Option<ConstructTemplate>,
        where_clause: WhereClause,
        modifiers: SolutionModifiers,
        span: SourceSpan,
    ) -> Self {
        Self {
            template,
            dataset: None,
            where_clause,
            modifiers,
            span,
        }
    }
}

/// A CONSTRUCT template - the triples to build.
#[derive(Clone, Debug, PartialEq)]
pub struct ConstructTemplate {
    /// Triple patterns in the template
    pub triples: Vec<TriplePattern>,
    /// Source span (including braces)
    pub span: SourceSpan,
}

impl ConstructTemplate {
    /// Create a new construct template.
    pub fn new(triples: Vec<TriplePattern>, span: SourceSpan) -> Self {
        Self { triples, span }
    }
}

/// ASK query.
///
/// Tests whether a pattern has any matches. Returns boolean.
///
/// ```sparql
/// ASK { ?s ex:name "Alice" }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct AskQuery {
    /// Dataset clause (FROM, FROM NAMED)
    pub dataset: Option<DatasetClause>,
    /// WHERE clause
    pub where_clause: WhereClause,
    /// Solution modifiers (limited - typically none for ASK)
    pub modifiers: SolutionModifiers,
    /// Source span
    pub span: SourceSpan,
}

impl AskQuery {
    /// Create a new ASK query.
    pub fn new(where_clause: WhereClause, span: SourceSpan) -> Self {
        Self {
            dataset: None,
            where_clause,
            modifiers: SolutionModifiers::new(),
            span,
        }
    }
}

/// DESCRIBE query.
///
/// Returns RDF data about resources.
///
/// ```sparql
/// DESCRIBE ?x WHERE { ?x ex:name "Alice" }
/// DESCRIBE <http://example.org/alice>
/// DESCRIBE *
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeQuery {
    /// What to describe: specific resources or * (all)
    pub target: DescribeTarget,
    /// Dataset clause (FROM, FROM NAMED)
    pub dataset: Option<DatasetClause>,
    /// WHERE clause (optional for DESCRIBE)
    pub where_clause: Option<WhereClause>,
    /// Solution modifiers
    pub modifiers: SolutionModifiers,
    /// Source span
    pub span: SourceSpan,
}

impl DescribeQuery {
    /// Create a new DESCRIBE query with specific targets.
    pub fn new(target: DescribeTarget, span: SourceSpan) -> Self {
        Self {
            target,
            dataset: None,
            where_clause: None,
            modifiers: SolutionModifiers::new(),
            span,
        }
    }

    /// Create a DESCRIBE * query.
    pub fn star(span: SourceSpan) -> Self {
        Self::new(DescribeTarget::Star, span)
    }
}

/// What to describe in a DESCRIBE query.
#[derive(Clone, Debug, PartialEq)]
pub enum DescribeTarget {
    /// DESCRIBE * - describe all bound variables
    Star,
    /// DESCRIBE ?var <iri> ... - specific resources
    Resources(Vec<VarOrIri>),
}

/// A variable or IRI reference.
#[derive(Clone, Debug, PartialEq)]
pub enum VarOrIri {
    /// A variable
    Var(Var),
    /// An IRI
    Iri(Iri),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pattern::GraphPattern;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    #[test]
    fn test_prologue_creation() {
        let prologue = Prologue::new()
            .with_base(BaseDecl::new("http://example.org/", test_span()))
            .with_prefix(PrefixDecl::new("ex", "http://example.org/", test_span()))
            .with_prefix(PrefixDecl::new(
                "foaf",
                "http://xmlns.com/foaf/0.1/",
                test_span(),
            ));

        assert!(prologue.base.is_some());
        assert_eq!(prologue.prefixes.len(), 2);
        assert!(prologue.get_prefix("ex").is_some());
        assert!(prologue.get_prefix("foaf").is_some());
        assert!(prologue.get_prefix("unknown").is_none());
    }

    #[test]
    fn test_select_clause_star() {
        let select = SelectClause::star(test_span());
        assert!(matches!(select.variables, SelectVariables::Star));
        assert!(select.modifier.is_none());
    }

    #[test]
    fn test_select_clause_variables() {
        let vars = vec![
            SelectVariable::Var(Var::new("name", test_span())),
            SelectVariable::Var(Var::new("age", test_span())),
        ];
        let select = SelectClause::variables(vars, test_span());

        match select.variables {
            SelectVariables::Explicit(vars) => {
                assert_eq!(vars.len(), 2);
                assert_eq!(vars[0].var().name.as_ref(), "name");
                assert_eq!(vars[1].var().name.as_ref(), "age");
            }
            _ => panic!("Expected explicit variables"),
        }
    }

    #[test]
    fn test_select_clause_with_modifier() {
        let mut select = SelectClause::star(test_span());
        select.modifier = Some(SelectModifier::Distinct);

        assert!(matches!(select.modifier, Some(SelectModifier::Distinct)));
    }

    #[test]
    fn test_solution_modifiers() {
        let modifiers = SolutionModifiers::new()
            .with_order_by(OrderByClause {
                conditions: vec![OrderCondition {
                    expr: OrderExpr::Var(Var::new("name", test_span())),
                    direction: OrderDirection::Asc,
                    span: test_span(),
                }],
                span: test_span(),
            })
            .with_limit(LimitClause::new(10, test_span()))
            .with_offset(OffsetClause::new(5, test_span()));

        assert!(modifiers.order_by.is_some());
        assert_eq!(modifiers.limit.as_ref().unwrap().value, 10);
        assert_eq!(modifiers.offset.as_ref().unwrap().value, 5);
    }

    #[test]
    fn test_select_query() {
        let select = SelectClause::star(test_span());
        let where_clause =
            WhereClause::new(GraphPattern::empty_bgp(test_span()), true, test_span());
        let modifiers = SolutionModifiers::new();

        let query = SelectQuery::new(select, where_clause, modifiers, test_span());

        assert!(matches!(query.select.variables, SelectVariables::Star));
        assert!(query.where_clause.has_where_keyword);
    }

    #[test]
    fn test_sparql_ast() {
        let prologue = Prologue::new();
        let select = SelectClause::star(test_span());
        let where_clause =
            WhereClause::new(GraphPattern::empty_bgp(test_span()), true, test_span());
        let modifiers = SolutionModifiers::new();
        let query = SelectQuery::new(select, where_clause, modifiers, test_span());

        let ast = SparqlAst::new(prologue, QueryBody::Select(query), test_span());

        assert!(matches!(ast.body, QueryBody::Select(_)));
    }
}
