//! Top-level statement AST.

use crate::span::SourceSpan;

use super::expr::{Expr, Variable};
use super::pattern::{MapLit, Pattern};

/// A Cypher statement is either a read query (terminating in RETURN),
/// an update (terminating in CREATE/SET/REMOVE/DELETE/MERGE without
/// a final RETURN), a schema DDL command (accepted as a no-op), or a
/// standalone procedure call (`CALL db.labels() YIELD label …`).
///
/// v1 supports exactly one statement per request body.
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Query(Query),
    Update(Update),
    Schema(SchemaCommand),
    CallProcedure(ProcedureCall),
}

/// A standalone procedure call statement:
/// `CALL dotted.name[(args)] [YIELD col [AS alias], … [WHERE expr]]
/// [<read clauses…>] [RETURN …]`.
///
/// The parser accepts any dotted name; resolution against the supported
/// shim set (`db.labels`, `dbms.components`, …) happens at the API layer,
/// which has the ledger stats needed to answer them. Bare `CALL proc()`
/// implicitly yields all of the procedure's columns. After the YIELD the
/// statement continues like any read query (`WITH` / `UNWIND` / `MATCH` /
/// nested `CALL { … }`), the shape schema-introspection tooling emits
/// (e.g. `CALL apoc.meta.data() YIELD … UNWIND other AS o RETURN …`).
#[derive(Clone, Debug, PartialEq)]
pub struct ProcedureCall {
    /// Dotted procedure name as written (e.g. `db.labels`). Matched
    /// case-insensitively at resolution.
    pub name: String,
    pub args: Vec<Expr>,
    /// Explicit `YIELD` items. Empty for bare `CALL proc()` and for
    /// `YIELD *` — both expose every column.
    pub yields: Vec<YieldItem>,
    /// `YIELD … WHERE expr` filter (only valid after a YIELD).
    pub where_clause: Option<Expr>,
    /// Read clauses following the YIELD, before the RETURN. When non-empty,
    /// an explicit RETURN is required (the implicit all-columns return only
    /// applies to the bare call shape).
    pub rest: Vec<ReadClause>,
    pub return_clause: Option<ReturnClause>,
    pub span: SourceSpan,
}

/// One `YIELD` item: a procedure column, optionally rebound with `AS`.
#[derive(Clone, Debug, PartialEq)]
pub struct YieldItem {
    pub column: String,
    pub alias: Option<Variable>,
    pub span: SourceSpan,
}

/// A schema DDL command. Fluree indexes everything and has no user-managed
/// index/constraint catalog, so these are accepted for tooling compatibility
/// (framework migrations run them at startup): CREATE/DROP are no-op writes,
/// SHOW answers zero rows. The command body is consumed without detailed
/// parsing.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaCommand {
    pub kind: SchemaCommandKind,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaCommandKind {
    /// `CREATE [OR REPLACE] INDEX … / CONSTRAINT …` — no-op write.
    CreateSchema,
    /// `DROP INDEX … / CONSTRAINT …` — no-op write.
    DropSchema,
    /// `SHOW INDEXES / CONSTRAINTS …` — zero rows.
    ShowSchema,
}

/// A read-shaped Cypher statement.
///
/// `union_tail` is `Some` when this query is followed by `UNION` (or
/// `UNION ALL`) and another query. The structure is right-recursive
/// so a chain `Q1 UNION Q2 UNION Q3` is represented as
/// `Q1 { tail: Some(Union { right: Q2 { tail: Some(Union { right: Q3 })} }) }`.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub clauses: Vec<ReadClause>,
    pub return_clause: ReturnClause,
    pub union_tail: Option<Box<UnionTail>>,
    pub span: SourceSpan,
}

/// A `UNION` / `UNION ALL` continuation of a query.
#[derive(Clone, Debug, PartialEq)]
pub struct UnionTail {
    /// `true` for `UNION ALL`, `false` for plain `UNION`.
    pub all: bool,
    pub right: Query,
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
    /// `CALL (a, b) { <subquery> }` — a correlated subquery clause. The scope
    /// clause `(a, b)` lists the imported variables (empty for `CALL { … }`,
    /// which runs uncorrelated). The body is a read-only query terminating in
    /// RETURN. Lowers to a `Pattern::Subquery` appended to the pipeline.
    CallSubquery(CallSubqueryClause),
    /// Internal-only: a constant multi-column row set, never produced by the
    /// parser. The parameter desugaring rewrites `UNWIND $listOfMaps AS row`
    /// (when the body has a MATCH) into this — one column per `row.field`
    /// accessed — so it lowers to a `VALUES` join (batched edge insert). Each
    /// inner `Vec<Expr>` is one row of literals, aligned to `vars`.
    InlineRows {
        vars: Vec<Variable>,
        rows: Vec<Vec<Expr>>,
    },
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
pub struct CallSubqueryClause {
    /// Imported (correlated) variables from the explicit scope clause
    /// `CALL (a, b) { … }`. Empty for `CALL { … }` (uncorrelated) and for
    /// `CALL (*) { … }` (see `import_all`).
    pub imports: Vec<Variable>,
    /// `CALL (*) { … }` — import every variable visible in the outer scope.
    /// When set, `imports` is empty and the importer is resolved at lowering
    /// from the outer scope.
    pub import_all: bool,
    /// The read-only subquery body (ends in RETURN).
    pub query: Box<Query>,
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
    /// `FOREACH (x IN <list> | <write clauses>)` — unrolled at
    /// param-substitution time for constant lists (inline literals,
    /// `range()`, `$param` arrays); runtime lists are deferred.
    Foreach(ForeachClause),
}

/// `FOREACH (var IN list | body)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ForeachClause {
    pub var: Variable,
    pub list: Expr,
    pub body: Vec<WriteClause>,
    pub span: SourceSpan,
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
    Property {
        target: Variable,
        property: String,
    },
    Labels {
        target: Variable,
        labels: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteClause {
    pub detach: bool,
    pub targets: Vec<Variable>,
    pub span: SourceSpan,
}
