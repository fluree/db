//! Pattern AST — node patterns, relationship patterns, and chains.

use crate::span::SourceSpan;

use super::expr::{Expr, Variable};

/// A pattern is a comma-separated list of pattern parts. Each part
/// starts at a node and alternates node/relationship.
#[derive(Clone, Debug, PartialEq)]
pub struct Pattern {
    pub parts: Vec<PatternPart>,
    pub span: SourceSpan,
}

/// One linear pattern in the comma-separated list (e.g., `(a)-[]->(b)`).
#[derive(Clone, Debug, PartialEq)]
pub struct PatternPart {
    /// Path variable assignment (`p = ...`). Set only for a
    /// `shortestPath`/`allShortestPaths` part (see `path_search`); plain
    /// `p = (...)` path values are still deferred.
    pub path_var: Option<Variable>,
    /// `shortestPath(...)` / `allShortestPaths(...)` wrapper, if any. When
    /// set, `head`/`tail` are the inner pattern searched for a path.
    pub path_search: Option<PathSearch>,
    /// The first node and then alternating (rel, node) pairs.
    pub head: NodePattern,
    pub tail: Vec<(RelPattern, NodePattern)>,
    pub span: SourceSpan,
}

/// Which path-search wraps a `p = …(pattern)` part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathSearch {
    /// `shortestPath((a)-[:T*]->(b))` — one shortest path.
    Shortest,
    /// `allShortestPaths((a)-[:T*]->(b))` — all paths at the shortest length.
    AllShortest,
}

/// `(var:Label1:Label2 {prop:val})`.
#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub var: Option<Variable>,
    pub labels: Vec<Label>,
    pub props: Option<MapLit>,
    pub span: SourceSpan,
}

/// `:Label`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label {
    pub name: String,
    pub span: SourceSpan,
}

/// `-[var:T1|T2 {prop:val} *N..M]->` and variants.
#[derive(Clone, Debug, PartialEq)]
pub struct RelPattern {
    pub var: Option<Variable>,
    pub direction: Direction,
    /// Relationship type alternatives. Empty = untyped.
    pub types: Vec<RelType>,
    /// `*N..M`. v1 rejects this at lower time (variable-length paths
    /// deferred); the AST carries it so the parser can produce a
    /// precise error.
    pub length: Option<LengthRange>,
    pub props: Option<MapLit>,
    pub span: SourceSpan,
}

/// `:TYPE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelType {
    pub name: String,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// `-[]->`
    Outgoing,
    /// `<-[]-`
    Incoming,
    /// `-[]-` — rejected in v1 with a clear error.
    Either,
}

/// `*` / `*N` / `*..M` / `*N..M`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LengthRange {
    pub min: Option<u32>,
    pub max: Option<u32>,
    pub span: SourceSpan,
}

/// A map literal `{key: expr, ...}`. Keys are bare identifiers per
/// Cypher syntax.
#[derive(Clone, Debug, PartialEq)]
pub struct MapLit {
    pub entries: Vec<(String, Expr)>,
    pub span: SourceSpan,
}
