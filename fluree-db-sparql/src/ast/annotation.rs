//! RDF 1.2 annotation syntax AST nodes.
//!
//! Covers the three SPARQL 1.2 / RDF 1.2 surfaces that lower to Fluree's
//! edge-annotation primitive:
//!
//! - Anonymous annotation block: `s p o {| pred obj ; ... |}`
//! - Named annotation block:     `s p o ~ <reifier> {| pred obj ; ... |}`
//! - Bare reifier:               `s p o ~ <reifier>`
//! - Reifier via `rdf:reifies`:  `?ann rdf:reifies <<( s p o )>>`
//!
//! See `SPARQL_EDGE_ANNOTATIONS_IMPL_PLAN.md` for the surface contract,
//! including the per-context blank-node / variable rules.

use super::term::{BlankNode, Iri, ObjectTerm, PredicateTerm, SubjectTerm, Var};
use crate::span::SourceSpan;

/// Annotation tail attached to a triple per the RDF 1.2 grammar:
///
/// ```text
/// annotation ::= ( reifier | annotationBlock )*
/// ```
///
/// In v1 we accept at most one reifier and one block in any order. The
/// parser collapses repeated `~` / `{| ... |}` runs into a single
/// `Annotation` node and rejects multi-reifier shapes with a deferred-
/// feature error pointing at the JSON-LD multi-triple-reifier rejection.
#[derive(Clone, Debug, PartialEq)]
pub struct Annotation {
    /// Optional explicit reifier id. `None` means "mint fresh".
    /// A bare `~` (no following IRI/BlankNode) also lowers to `None`.
    pub reifier: Option<ReifierId>,
    /// Optional `{| ... |}` body. `None` means a bare `~` with no
    /// annotation block (still a valid RDF 1.2 production).
    pub block: Option<AnnotationBlock>,
    pub span: SourceSpan,
}

/// Identity of a reifier appearing after `~`.
///
/// Lowering rules (per the plan's "Blank node and variable semantics"
/// table) differ between query, INSERT DATA, DELETE DATA, and
/// WHERE-template paths.
#[derive(Clone, Debug, PartialEq)]
pub enum ReifierId {
    /// Explicit IRI reifier, e.g. `~ ex:employment-2024`.
    Iri(Iri),
    /// Blank-node reifier, e.g. `~ _:ann`. Semantics is context-
    /// dependent (see plan).
    BlankNode(BlankNode),
    /// Variable reifier, e.g. `~ ?ann`. Allowed in WHERE / WHERE-template
    /// contexts; rejected in `INSERT DATA` / `DELETE DATA` per
    /// SPARQL §3.1.1.
    Var(Var),
}

impl ReifierId {
    pub fn span(&self) -> SourceSpan {
        match self {
            ReifierId::Iri(i) => i.span,
            ReifierId::BlankNode(b) => b.span,
            ReifierId::Var(v) => v.span,
        }
    }
}

/// Body of a `{| ... |}` annotation block.
///
/// Each entry is a (predicate, object) pair applied to the reifier in
/// the enclosing `Annotation`. The body itself is a flat list — nested
/// annotation tails on body entries are deferred per the design doc and
/// rejected at parse time.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationBlock {
    pub entries: Vec<AnnotationEntry>,
    pub span: SourceSpan,
}

/// One predicate-object pair inside a `{| ... |}` block.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationEntry {
    pub predicate: PredicateTerm,
    pub object: ObjectTerm,
    pub span: SourceSpan,
}

/// RDF 1.2 triple term: `<<( subject predicate object )>>`.
///
/// In v1 a `TripleTerm` is **only** valid as the object of `rdf:reifies`.
/// Any other use is a parse-time deferred-feature error. We do NOT add
/// `TripleTerm` as a `Term` / `ObjectTerm` variant for that reason —
/// the parser surfaces it via `parse_reifies_object` and never lets it
/// flow through ordinary object-position handling.
#[derive(Clone, Debug, PartialEq)]
pub struct TripleTerm {
    pub subject: SubjectTerm,
    pub predicate: PredicateTerm,
    pub object: ObjectTerm,
    pub span: SourceSpan,
}
