//! SPARQL Update AST types.
//!
//! This module defines AST types for SPARQL Update operations:
//! - INSERT DATA
//! - DELETE DATA
//! - DELETE WHERE
//! - INSERT/DELETE with WHERE (Modify operation)
//!
//! ## Fluree Restrictions
//!
//! Not all SPARQL Update features are supported by Fluree:
//! - GRAPH in INSERT/DELETE templates only supports IRI graph names (no variables)
//!
//! These restrictions are enforced at the validation layer, not during parsing.

use super::pattern::{GraphName, GraphPattern, TriplePattern};
use super::term::Iri;
use crate::span::SourceSpan;

/// A SPARQL Update operation.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateOperation {
    /// INSERT DATA { triples }
    InsertData(InsertData),
    /// DELETE DATA { triples }
    DeleteData(DeleteData),
    /// DELETE WHERE { pattern }
    DeleteWhere(DeleteWhere),
    /// INSERT/DELETE with WHERE clause (Modify operation)
    /// Boxed to reduce enum size (Modify is ~288 bytes vs ~56 for others)
    Modify(Box<Modify>),
}

/// INSERT DATA operation.
///
/// Inserts ground triples (no variables allowed).
///
/// ```sparql
/// INSERT DATA { <http://example.org/s> <http://example.org/p> "value" }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct InsertData {
    /// The triples to insert (must be ground - no variables)
    pub data: QuadData,
    /// Source span
    pub span: SourceSpan,
}

impl InsertData {
    /// Create a new INSERT DATA operation.
    pub fn new(data: QuadData, span: SourceSpan) -> Self {
        Self { data, span }
    }
}

/// DELETE DATA operation.
///
/// Deletes ground triples (no variables allowed).
///
/// ```sparql
/// DELETE DATA { <http://example.org/s> <http://example.org/p> "value" }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteData {
    /// The triples to delete (must be ground - no variables)
    pub data: QuadData,
    /// Source span
    pub span: SourceSpan,
}

impl DeleteData {
    /// Create a new DELETE DATA operation.
    pub fn new(data: QuadData, span: SourceSpan) -> Self {
        Self { data, span }
    }
}

/// DELETE WHERE operation.
///
/// Deletes triples matching the pattern (variables allowed).
///
/// ```sparql
/// DELETE WHERE { ?s ex:obsolete ?o }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteWhere {
    /// The pattern to match and delete
    pub pattern: QuadPattern,
    /// Source span
    pub span: SourceSpan,
}

impl DeleteWhere {
    /// Create a new DELETE WHERE operation.
    pub fn new(pattern: QuadPattern, span: SourceSpan) -> Self {
        Self { pattern, span }
    }
}

/// Modify operation (INSERT/DELETE with WHERE).
///
/// The most general update form with optional WITH, DELETE, INSERT, and WHERE clauses.
///
/// ```sparql
/// WITH <http://example.org/graph>
/// DELETE { ?s ex:old ?o }
/// INSERT { ?s ex:new ?o }
/// WHERE { ?s ex:old ?o }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Modify {
    /// WITH clause specifying the graph to modify
    pub with_iri: Option<Iri>,
    /// USING clause(s) for the WHERE pattern
    pub using: Option<UsingClause>,
    /// DELETE clause (optional)
    pub delete_clause: Option<QuadPattern>,
    /// INSERT clause (optional)
    pub insert_clause: Option<QuadPattern>,
    /// WHERE clause
    pub where_clause: GraphPattern,
    /// Source span
    pub span: SourceSpan,
}

impl Modify {
    /// Create a new Modify operation.
    pub fn new(
        delete_clause: Option<QuadPattern>,
        insert_clause: Option<QuadPattern>,
        where_clause: GraphPattern,
        span: SourceSpan,
    ) -> Self {
        Self {
            with_iri: None,
            using: None,
            delete_clause,
            insert_clause,
            where_clause,
            span,
        }
    }

    /// Set the WITH clause.
    pub fn with_graph(mut self, iri: Iri) -> Self {
        self.with_iri = Some(iri);
        self
    }

    /// Set the USING clause.
    pub fn with_using(mut self, using: UsingClause) -> Self {
        self.using = Some(using);
        self
    }
}

/// Ground quad data (for INSERT DATA / DELETE DATA).
///
/// Contains triples that must be ground (no variables).
/// In the current implementation, we reuse `TriplePattern` but
/// validation will ensure no variables are present.
#[derive(Clone, Debug, PartialEq)]
pub struct QuadData {
    /// The ground triples
    pub triples: Vec<TriplePattern>,
    /// Source span
    pub span: SourceSpan,
}

impl QuadData {
    /// Create new quad data.
    pub fn new(triples: Vec<TriplePattern>, span: SourceSpan) -> Self {
        Self { triples, span }
    }
}

/// Quad pattern (for DELETE/INSERT templates and DELETE WHERE).
///
/// Can contain variables that will be bound by the WHERE clause.
#[derive(Clone, Debug, PartialEq)]
pub struct QuadPattern {
    /// The quad pattern elements (triples and GRAPH blocks)
    pub patterns: Vec<QuadPatternElement>,
    /// Source span
    pub span: SourceSpan,
}

impl QuadPattern {
    /// Create a new quad pattern.
    pub fn new(patterns: Vec<QuadPatternElement>, span: SourceSpan) -> Self {
        Self { patterns, span }
    }
}

/// Element of a quad pattern: either a triple, or a GRAPH block containing triples.
#[derive(Clone, Debug, PartialEq)]
pub enum QuadPatternElement {
    /// A triple in the default graph.
    Triple(TriplePattern),
    /// A GRAPH block: `GRAPH <iri>|?g { ... }`
    ///
    /// Note: Fluree currently supports only IRI graph names in UPDATE templates.
    Graph {
        name: GraphName,
        triples: Vec<TriplePattern>,
        span: SourceSpan,
    },
}

/// USING clause for Modify operations.
///
/// Specifies the dataset for the WHERE pattern.
#[derive(Clone, Debug, PartialEq)]
pub struct UsingClause {
    /// Default graphs (USING <iri>)
    ///
    /// Multiple USING clauses are allowed. Semantics follow SPARQL dataset rules:
    /// the WHERE clause evaluates over the merged default graph of these entries.
    pub default_graphs: Vec<Iri>,
    /// Named graphs (USING NAMED <iri>) - not supported by Fluree
    pub named_graphs: Vec<Iri>,
    /// Source span
    pub span: SourceSpan,
}

impl UsingClause {
    /// Create a new USING clause with a single default graph.
    pub fn with_default_graph(iri: Iri, span: SourceSpan) -> Self {
        Self {
            default_graphs: vec![iri],
            named_graphs: Vec::new(),
            span,
        }
    }

    /// Create a new USING NAMED clause with a single named graph.
    pub fn with_named_graph(iri: Iri, span: SourceSpan) -> Self {
        Self {
            default_graphs: Vec::new(),
            named_graphs: vec![iri],
            span,
        }
    }

    #[deprecated(note = "Use UsingClause::with_default_graph")]
    pub fn default_graph(iri: Iri, span: SourceSpan) -> Self {
        Self::with_default_graph(iri, span)
    }

    #[deprecated(note = "Use UsingClause::with_named_graph")]
    pub fn named_graph(iri: Iri, span: SourceSpan) -> Self {
        Self::with_named_graph(iri, span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    #[test]
    fn test_insert_data_creation() {
        let data = QuadData::new(vec![], test_span());
        let insert = InsertData::new(data, test_span());
        assert!(insert.data.triples.is_empty());
    }

    #[test]
    fn test_delete_data_creation() {
        let data = QuadData::new(vec![], test_span());
        let delete = DeleteData::new(data, test_span());
        assert!(delete.data.triples.is_empty());
    }

    #[test]
    fn test_modify_builder() {
        use crate::ast::term::Iri;

        let where_pattern = GraphPattern::empty_bgp(test_span());
        let modify = Modify::new(None, None, where_pattern, test_span())
            .with_graph(Iri::full("http://example.org/graph", test_span()));

        assert!(modify.with_iri.is_some());
        assert!(modify.delete_clause.is_none());
        assert!(modify.insert_clause.is_none());
    }
}
