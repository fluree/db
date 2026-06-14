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
    /// Path variable assignment (`p = ...`). v1 rejects bound path
    /// variables since path values aren't a v1 surface; this stays
    /// `None`.
    pub path_var: Option<Variable>,
    /// The first node and then alternating (rel, node) pairs.
    pub head: NodePattern,
    pub tail: Vec<(RelPattern, NodePattern)>,
    pub span: SourceSpan,
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
