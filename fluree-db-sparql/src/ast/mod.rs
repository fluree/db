//! SPARQL Abstract Syntax Tree types.
//!
//! This module contains the typed AST representation of SPARQL queries
//! and updates. All nodes carry source spans for precise diagnostics.
//!
//! ## Module Structure
//!
//! - `term`: Terms that can appear in patterns (variables, IRIs, literals, blank nodes)
//! - `pattern`: Graph patterns (BGP, OPTIONAL, UNION, FILTER, etc.)
//! - `path`: Property path expressions (transitive, inverse, sequence, etc.)
//! - `expr`: Expressions (arithmetic, comparison, boolean, function calls)
//! - `query`: Query forms (SELECT, CONSTRUCT, ASK, DESCRIBE) and solution modifiers
//! - `update`: Update operations (INSERT DATA, DELETE DATA, DELETE WHERE, etc.)
//!
//! ## Example
//!
//! ```
//! use fluree_db_sparql::parse_sparql;
//!
//! let output = parse_sparql("SELECT ?name WHERE { ?s <http://example.org/name> ?name }");
//! let ast = output.ast.unwrap();
//! // ast is a SparqlAst with source spans on every node
//! ```

pub mod expr;
pub mod path;
pub mod pattern;
pub mod query;
pub mod term;
pub mod update;

// Re-export commonly used types at the ast module level
pub use expr::{AggregateFunction, BinaryOp, Expression, FunctionName, UnaryOp};
pub use path::{NegatedPredicate, PropertyPath};
pub use pattern::{GraphName, GraphPattern, TriplePattern};
pub use query::{
    AskQuery, BaseDecl, ConstructQuery, ConstructTemplate, DatasetClause, DescribeQuery,
    DescribeTarget, GroupByClause, GroupCondition, HavingClause, LimitClause, OffsetClause,
    OrderByClause, OrderCondition, OrderDirection, OrderExpr, PrefixDecl, Prologue, QueryBody,
    SelectClause, SelectModifier, SelectQuery, SelectVariable, SelectVariables, SolutionModifiers,
    SparqlAst, VarOrIri, WhereClause,
};
pub use term::{
    BlankNode, BlankNodeValue, Iri, IriValue, Literal, LiteralValue, ObjectTerm, PredicateTerm,
    QuotedTriple, Spanned, SubjectTerm, Term, Var,
};
pub use update::{
    DeleteData, DeleteWhere, InsertData, Modify, QuadData, QuadPattern, QuadPatternElement,
    UpdateOperation, UsingClause,
};
