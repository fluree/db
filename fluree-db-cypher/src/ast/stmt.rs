//! Top-level statement AST.

use crate::span::SourceSpan;

use super::expr::{Expr, Variable};
use super::pattern::{MapLit, Pattern};

/// A Cypher statement is either a read query (terminating in RETURN)
/// or an update (terminating in CREATE/SET/REMOVE/DELETE/MERGE without
/// a final RETURN).
///
/// v1 supports exactly one statement per request body.
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Query(Query),
    Update(Update),
}

/// A read-shaped Cypher statement.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub clauses: Vec<ReadClause>,
    pub return_clause: ReturnClause,
    pub span: SourceSpan,
}

/// A write-shaped Cypher statement. May still have leading MATCH /
/// WHERE clauses that bind variables for the write template; those
/// live in `read_clauses`. May terminate in a final RETURN; if so,
/// that lives in `return_clause`.
#[derive(Clone, Debug, PartialEq)]
pub struct Update {
    pub read_clauses: Vec<ReadClause>,
    pub write_clauses: Vec<WriteClause>,
    pub return_clause: Option<ReturnClause>,
    pub span: SourceSpan,
}

/// Read-side clauses (anything that contributes to bindings but does
/// not write to the graph).
#[derive(Clone, Debug, PartialEq)]
pub enum ReadClause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    With(WithClause),
    Unwind(UnwindClause),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WithClause {
    pub items: Vec<ProjectionItem>,
    pub distinct: bool,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: Variable,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ProjectionItem>,
    pub distinct: bool,
    pub order_by: Vec<OrderItem>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionItem {
    pub expr: Expr,
    pub alias: Option<Variable>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub direction: OrderDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

/// Write clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteClause {
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateClause {
    pub pattern: Pattern,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SetItem {
    /// `n.prop = expr` — single property.
    Property {
        target: Variable,
        property: String,
        value: Expr,
    },
    /// `n += {p:v, q:w}` — merge map into existing properties.
    MapMerge { target: Variable, map: MapLit },
    /// `n = {p:v, q:w}` — replace all data properties with the map.
    MapReplace { target: Variable, map: MapLit },
    /// `n:Label[:Label2]` — add labels.
    Labels {
        target: Variable,
        labels: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RemoveItem {
    Property { target: Variable, property: String },
    Labels { target: Variable, labels: Vec<String> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteClause {
    pub detach: bool,
    pub targets: Vec<Variable>,
    pub span: SourceSpan,
}
