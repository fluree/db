//! RDF 1.2 annotation syntax AST nodes.
//!
//! Covers the SPARQL 1.2 / RDF 1.2 surfaces that lower to Fluree's
//! edge-annotation primitive:
//!
//! - Anonymous annotation block: `s p o {| pred obj ; ... |}`
//! - Named annotation block:     `s p o ~ <reifier> {| pred obj ; ... |}`
//! - Bare reifier:               `s p o ~ <reifier>`
//! - Multiple reifications:      `s p o ~ :r1 {| ... |} ~ :r2 {| ... |}`
//! - Reifier via `rdf:reifies`:  `?ann rdf:reifies <<( s p o )>>`
//!
//! (Reified triples `<< s p o ~ r? >>` used *as terms* live on
//! `ast::term::QuotedTriple` — see its `reifier` field.)
//!
//! See `docs/concepts/edge-annotations.md` "SPARQL 1.2 / RDF 1.2
//! surface" for the surface contract, including the per-operation
//! blank-node / variable rules.

use super::path::PropertyPath;
use super::term::{BlankNode, Iri, ObjectTerm, PredicateTerm, SubjectTerm, Var};
use crate::span::SourceSpan;

/// Annotation tail attached to a triple per the RDF 1.2 grammar:
///
/// ```text
/// annotation ::= ( reifier | annotationBlock )*
/// ```
///
/// The parser groups the element sequence into [`AnnotationUnit`]s —
/// one reification of the base triple per unit — following the RDF 1.2
/// attachment rule: an annotation block combines with an immediately
/// preceding `~ reifier` element; a block with no immediately preceding
/// reifier mints a fresh one. So `~ :r1 {| … |} ~ :r2` is two units,
/// and `{| … |} {| … |}` is two units with two fresh reifiers.
#[derive(Clone, Debug, PartialEq)]
pub struct Annotation {
    /// The reification units, in source order. Non-empty by
    /// construction (an absent tail parses as no `Annotation` at all).
    pub units: Vec<AnnotationUnit>,
    pub span: SourceSpan,
}

impl Annotation {
    /// The single annotation unit, when the tail has exactly one (the
    /// common `~ r? {| … |}?` shape).
    pub fn single_unit(&self) -> Option<&AnnotationUnit> {
        match self.units.as_slice() {
            [unit] => Some(unit),
            _ => None,
        }
    }
}

/// One reification of the annotated triple: an optional explicit
/// reifier id plus an optional `{| ... |}` body attached to it.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationUnit {
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
/// Each entry is a (verb, object) pair applied to the reifier in the
/// enclosing [`AnnotationUnit`]. The body itself is a flat list —
/// nested annotation tails on body entries are illegal per the RDF 1.2
/// grammar and rejected at parse time.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationBlock {
    pub entries: Vec<AnnotationEntry>,
    pub span: SourceSpan,
}

/// One verb-object pair inside a `{| ... |}` block.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnotationEntry {
    pub verb: AnnotationVerb,
    pub object: ObjectTerm,
    pub span: SourceSpan,
}

/// Verb of an annotation-block entry. The RDF 1.2 grammar's block body
/// is a `PropertyListPathNotEmpty`, so property paths are legal here
/// (W3C `annotation-*reifier-06`: `{| :r/:q 'ABC' |}`).
#[derive(Clone, Debug, PartialEq)]
pub enum AnnotationVerb {
    /// Plain predicate (IRI or variable).
    Simple(PredicateTerm),
    /// Property path (`:r/:q`, `:q1+`, ...).
    Path(PropertyPath),
}

impl AnnotationVerb {
    pub fn span(&self) -> SourceSpan {
        match self {
            AnnotationVerb::Simple(p) => p.span(),
            AnnotationVerb::Path(p) => p.span(),
        }
    }
}

/// RDF 1.2 triple term: `<<( subject predicate object )>>`.
///
/// In v1 a `TripleTerm` is **only** valid as the object of `rdf:reifies`
/// (bare triple-term *values* are deferred to the wave-2 PR-W2BC /
/// first-class-value epic — see the burn-down roadmap D-1). The parser
/// surfaces it via `parse_reifies_object` and the reified-triple
/// desugaring; it never flows through ordinary object-position
/// handling.
///
/// Note: `subject`/`object` may be reified triples (`SubjectTerm::
/// QuotedTriple` / `Term::QuotedTriple`) when this node was produced by
/// desugaring a *reified triple* — those denote reifier nodes and are
/// desugared recursively at lowering. A user-written `<<( ... )>>`
/// triple term still rejects nesting at parse time.
#[derive(Clone, Debug, PartialEq)]
pub struct TripleTerm {
    pub subject: SubjectTerm,
    pub predicate: PredicateTerm,
    pub object: ObjectTerm,
    pub span: SourceSpan,
}
