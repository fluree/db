//! RDF 1.2 annotation / reified-triple lowering for the SPARQL query path.
//!
//! Translates the AST shapes (`TriplePattern.annotation`,
//! `GraphPattern::AnnotationTarget`, and reified-triple terms
//! `SubjectTerm::QuotedTriple` / `Term::QuotedTriple`) into the
//! existing query IR (`Pattern::EdgeAnnotation` and
//! `Pattern::AnnotationTarget`). The IR's
//! `expand_edge_annotation_patterns` step (in `fluree-db-query`)
//! handles the f:reifies* fan-out from there.
//!
//! Reified triples desugar exactly per SPARQL 1.2: a `<< s p o ~ r? >>`
//! term denotes its reifier node `r` (fresh when unnamed) and adds the
//! pattern `r rdf:reifies <<( s p o )>>` — emitted here as a sibling
//! `Pattern::AnnotationTarget`. Nested reified triples recurse.
//!
//! Sibling triples about a reifier variable are NOT folded into
//! `body` — they sit in the surrounding scope and join via the
//! standard executor on the bound reifier var. See
//! `docs/concepts/edge-annotations.md` "SPARQL 1.2 / RDF 1.2 surface"
//! for the rationale.

use crate::ast::annotation::{Annotation, AnnotationBlock, AnnotationVerb, ReifierId, TripleTerm};
use crate::ast::term::{BlankNodeValue, QuotedTriple, SubjectTerm, Term as SparqlTerm};
use crate::span::SourceSpan;

use fluree_db_core::DatatypeConstraint;
use fluree_db_query::ir::triple::{Ref, Term as IrTerm, TriplePattern as IrTriplePattern};
use fluree_db_query::ir::Pattern;
use fluree_db_query::parse::encode::IriEncoder;

use std::collections::HashMap;

use super::path::PathObject;
use super::{LoweringContext, Result};

/// Prefix used for registry names of synthetic variables that must
/// stay invisible to `SELECT *` and unmatchable by user input. `#`
/// is comment-start in SPARQL, so no user variable can lex with this
/// prefix.
pub(super) const INTERNAL_VAR_PREFIX: &str = "#";

/// Per-BGP memo of already-desugared reified-triple occurrences, keyed
/// by source span. A quoted triple shared across several triple
/// patterns (e.g. `<< s p o >> :q1 :z1 ; :q2 :z2`) is ONE occurrence
/// and must bind ONE reifier; distinct occurrences (even lexically
/// identical ones) each mint their own.
pub(super) type ReifiedCache = HashMap<SourceSpan, Ref>;

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a triple's RDF 1.2 annotation tail into one
    /// `Pattern::EdgeAnnotation` per [`AnnotationUnit`] over the same
    /// base edge. Each unit contributes its own reifier and `{| … |}`
    /// body; multiple units join on the shared edge terms.
    pub(super) fn lower_annotation_units(
        &mut self,
        edge: IrTriplePattern,
        ann: &Annotation,
        out: &mut Vec<Pattern>,
    ) -> Result<()> {
        for unit in &ann.units {
            let annotation_ref = self.lower_reifier_id(unit.reifier.as_ref())?;
            let body = self.lower_annotation_block_body(&annotation_ref, unit.block.as_ref())?;
            out.push(Pattern::EdgeAnnotation {
                edge: edge.clone(),
                annotation: annotation_ref,
                body,
            });
        }
        Ok(())
    }

    /// Lower a `GraphPattern::AnnotationTarget` (the
    /// `?ann rdf:reifies <<( s p o )>>` form and the standalone
    /// reified-triple statement it desugars from) into
    /// `Pattern::AnnotationTarget` IR (plus any sibling targets from
    /// nested reified triples inside the triple term). Emits an empty
    /// body — surrounding sibling triples about the reifier join
    /// through the standard executor.
    pub(super) fn lower_annotation_target_pattern(
        &mut self,
        reifier: &SubjectTerm,
        triple_term: &TripleTerm,
    ) -> Result<Vec<Pattern>> {
        let mut out = Vec::new();
        let annotation_ref = self.lower_subject(reifier)?;
        let edge = self.lower_triple_term(triple_term, &mut out)?;
        if link_terms_enabled() {
            self.lower_reified_link(annotation_ref, edge, &mut out);
            return Ok(out);
        }
        out.push(Pattern::AnnotationTarget {
            annotation: annotation_ref,
            edge,
            body: Vec::new(),
        });
        Ok(out)
    }

    /// Desugar an RDF 1.2 reified triple `<< s p o ~ r? >>` used as a
    /// term: emit `r rdf:reifies <<( s p o )>>` (as
    /// `Pattern::AnnotationTarget`) into `out` and return the reifier
    /// ref that stands in the reified triple's position. Nested
    /// reified triples in `s`/`o` recurse; repeated occurrences (same
    /// source span) reuse the memoized reifier.
    pub(super) fn lower_reified_triple(
        &mut self,
        qt: &QuotedTriple,
        cache: &mut ReifiedCache,
        out: &mut Vec<Pattern>,
    ) -> Result<Ref> {
        if let Some(r) = cache.get(&qt.span) {
            return Ok(r.clone());
        }

        let annotation_ref =
            self.lower_reifier_id(qt.reifier.as_ref().and_then(|r| r.id.as_ref()))?;

        let s = match &*qt.subject {
            SubjectTerm::QuotedTriple(inner) => self.lower_reified_triple(inner, cache, out)?,
            other => self.lower_subject(other)?,
        };
        let p = self.lower_predicate(&qt.predicate)?;
        let (o, dtc) = self.lower_object_desugared(&qt.object, cache, out, true)?;

        if link_terms_enabled() {
            self.lower_reified_link(
                annotation_ref.clone(),
                IrTriplePattern { s, p, o, dtc },
                out,
            );
        } else {
            out.push(Pattern::AnnotationTarget {
                annotation: annotation_ref.clone(),
                edge: IrTriplePattern { s, p, o, dtc },
                body: Vec::new(),
            });
        }
        cache.insert(qt.span, annotation_ref.clone());
        Ok(annotation_ref)
    }

    /// Object lowering that desugars reified-triple terms to their
    /// reifier ref (emitting the `rdf:reifies` pattern into `out`).
    /// `with_constraint` selects the constraint-preserving literal
    /// lowering (annotation/reifier contexts) vs the plain path (the
    /// broad query surface, whose scan semantics must not change).
    pub(super) fn lower_object_desugared(
        &mut self,
        term: &SparqlTerm,
        cache: &mut ReifiedCache,
        out: &mut Vec<Pattern>,
        with_constraint: bool,
    ) -> Result<(IrTerm, Option<DatatypeConstraint>)> {
        match term {
            SparqlTerm::QuotedTriple(qt) => {
                let r = self.lower_reified_triple(qt, cache, out)?;
                Ok((r.into(), None))
            }
            other if with_constraint => self.lower_object_with_constraint(other),
            other => self.lower_object_with_term_constraint(other),
        }
    }

    /// Resolve the reifier id following `~`. Mints a fresh synthetic
    /// non-distinguished variable when the user wrote an anonymous
    /// `{| ... |}` (i.e. no preceding `~`) or a bare `~` with no id.
    pub(super) fn lower_reifier_id(&mut self, reifier: Option<&ReifierId>) -> Result<Ref> {
        match reifier {
            Some(ReifierId::Iri(iri)) => self.lower_iri_ref(iri),
            Some(ReifierId::BlankNode(b)) => match &b.value {
                BlankNodeValue::Labeled(label) => {
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Ref::Var(var_id))
                }
                BlankNodeValue::Anon => Ok(Ref::Var(self.fresh_blank_node_var())),
            },
            Some(ReifierId::Var(v)) => Ok(self.lower_var_ref(v)),
            None => {
                // Anonymous block / bare `~` mints a fresh non-distinguished
                // variable per SPARQL §4.1.4 — bindable inside the BGP, not
                // exposable in `SELECT *`.
                //
                // The registry key starts with `?#` — that prefix is
                // unambiguously internal because the SPARQL lexer treats `#`
                // as a comment-start outside string literals, so no user
                // variable can ever lex with this name. `lower_select_clause`
                // filters these out of `SELECT *` expansion.
                let var_id = self.vars.get_or_insert(&format!(
                    "?{}__ann_{}",
                    INTERNAL_VAR_PREFIX,
                    self.vars.len()
                ));
                Ok(Ref::Var(var_id))
            }
        }
    }

    /// Lower the `{| verb obj ; verb obj |}` body to a flat list of
    /// patterns whose subject is the reifier: `Pattern::Triple` for
    /// simple predicates, property-path patterns for path verbs
    /// (`{| :r/:q 'x' |}`), plus sibling `Pattern::AnnotationTarget`s
    /// for reified-triple objects.
    ///
    /// Each entry's object is lowered through
    /// `lower_object_with_constraint` so literal objects pin the scan
    /// to their exact datatype / language tag — same-lexical literals
    /// with different datatypes (or languages) must not cross-match.
    fn lower_annotation_block_body(
        &mut self,
        annotation: &Ref,
        block: Option<&AnnotationBlock>,
    ) -> Result<Vec<Pattern>> {
        let Some(block) = block else {
            return Ok(Vec::new());
        };
        let mut cache = ReifiedCache::new();
        let mut out = Vec::with_capacity(block.entries.len());
        for entry in &block.entries {
            match &entry.verb {
                AnnotationVerb::Simple(pred) => {
                    let p = self.lower_predicate(pred)?;
                    let (o, dtc) =
                        self.lower_object_desugared(&entry.object, &mut cache, &mut out, true)?;
                    out.push(Pattern::Triple(IrTriplePattern {
                        s: annotation.clone(),
                        p,
                        o,
                        dtc,
                    }));
                }
                AnnotationVerb::Path(path) => {
                    // Same contract as the main property-path surface: a
                    // literal object is legal (`{| :r/:q "x" |}`) and is
                    // narrowed per-arm by the dispatcher. The datatype
                    // constraint rides along so a literal endpoint on the
                    // final hop stays as precise as the `Simple` verb above.
                    let (o_term, dtc) =
                        self.lower_object_desugared(&entry.object, &mut cache, &mut out, true)?;
                    let o = PathObject::new(o_term, dtc);
                    let pats = self.lower_path_dispatch(annotation, path, &o, entry.span)?;
                    out.extend(pats);
                }
            }
        }
        Ok(out)
    }

    /// Lower the inner `<<( s p o )>>` triple-term to an IR
    /// `TriplePattern`, desugaring reified-triple terms in its
    /// subject/object positions (produced by the reified-triple
    /// desugaring — user-written triple terms reject nesting at parse
    /// time) as sibling patterns into `out`.
    ///
    /// Carries the same constraint-preserving object lowering as the
    /// annotation-block body so reified base-edge object positions
    /// match precisely.
    pub(super) fn lower_triple_term(
        &mut self,
        term: &TripleTerm,
        out: &mut Vec<Pattern>,
    ) -> Result<IrTriplePattern> {
        let mut cache = ReifiedCache::new();
        let s = match &term.subject {
            SubjectTerm::QuotedTriple(qt) => self.lower_reified_triple(qt, &mut cache, out)?,
            other => self.lower_subject(other)?,
        };
        let p = self.lower_predicate(&term.predicate)?;
        let (o, dtc) = self.lower_object_desugared(&term.object, &mut cache, out, true)?;
        Ok(IrTriplePattern { s, p, o, dtc })
    }
}

/// `FLUREE_ANNOTATION_TERMS=1` routes reified-triple patterns through the
/// `rdf:reifies` link flake and the term dictionary instead of the
/// `f:reifies*` bundle chain. Read per lowering so tests can flip it.
pub(super) fn link_terms_enabled() -> bool {
    std::env::var("FLUREE_ANNOTATION_TERMS").is_ok_and(|v| v == "1")
}

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a reified-triple pattern to the link form: one
    /// `annotation rdf:reifies ?__term` triple whose object is a triple-term
    /// handle, and `TermComponents(?__term, s, p, o)` relating the term to its
    /// components. A variable component is bound by that relation, which joins
    /// on it when another pattern bound it first, in every scope, without the
    /// lowering tracking who binds what; a bound subject lets the planner
    /// drive the relation through the dictionary's subject prefix. A constant
    /// component is also a filter on the link
    /// (`FILTER(sameTerm(PREDICATE(?__term), <p>))`), which the planner turns
    /// into the scan's handle interval and key check when the link leads. A
    /// fully constant edge composes to a constant term the scan looks up
    /// directly.
    pub(super) fn lower_reified_link(
        &mut self,
        annotation_ref: Ref,
        edge: IrTriplePattern,
        out: &mut Vec<Pattern>,
    ) {
        use fluree_db_core::FlakeValue;
        use fluree_db_query::binding::Binding;
        use fluree_db_query::ir::{Component, Expression, Function, TermComponentsPattern};

        let reifies = self.encoder.encode_ref(fluree_vocab::rdf::REIFIES);

        // Fully constant edge: compose the term itself.
        if let Some(term) = constant_term(&edge) {
            out.push(Pattern::Triple(IrTriplePattern {
                s: annotation_ref,
                p: reifies,
                o: IrTerm::Value(FlakeValue::TripleTerm(Box::new(term))),
                dtc: None,
            }));
            return;
        }

        let name = format!("?__term_{}", self.term_counter);
        self.term_counter += 1;
        let t = self.vars.get_or_insert(&name);
        out.push(Pattern::Triple(IrTriplePattern {
            s: annotation_ref,
            p: reifies,
            o: IrTerm::Var(t),
            dtc: None,
        }));

        let accessor = |f: Function| Expression::call(f, vec![Expression::Var(t)]);
        let same_term = |f: Function, constant: Expression| {
            Pattern::Filter(Expression::call(
                Function::SameTerm,
                vec![accessor(f), constant],
            ))
        };
        let IrTriplePattern { s, p, o, dtc } = edge;
        // Constant positions are filters on the link (the scan narrows on
        // them) and constants of the components relation (a constant subject
        // anchors it). Variable positions are bound by the components relation,
        // which joins on them when another pattern bound them first.
        let component = |func: Function, term: IrTerm, out: &mut Vec<Pattern>| match term {
            IrTerm::Var(v) => Component::Var(v),
            IrTerm::Sid(sid) => {
                out.push(same_term(
                    func,
                    Expression::Const(FlakeValue::Ref(sid.clone())),
                ));
                Component::Node(sid)
            }
            IrTerm::Iri(iri) => match self.encoder.encode_iri(&iri) {
                Some(sid) => {
                    out.push(same_term(
                        func,
                        Expression::Const(FlakeValue::Ref(sid.clone())),
                    ));
                    Component::Node(sid)
                }
                // An IRI in no registered namespace names nothing in this
                // ledger, so the pattern cannot match.
                None => {
                    out.push(Pattern::Filter(Expression::Const(FlakeValue::Boolean(
                        false,
                    ))));
                    Component::Any
                }
            },
            // A literal matches by term identity: its datatype or language
            // tag is part of what `<< ?s :p "chat"@fr >>` asks for.
            IrTerm::Value(v) => match &dtc {
                Some(dtc) => {
                    out.push(same_term(
                        func,
                        Expression::Resolved(Box::new(Binding::Lit {
                            val: v.clone(),
                            dtc: dtc.clone(),
                            t: None,
                            op: None,
                            p_id: None,
                        })),
                    ));
                    Component::Literal(v, dtc.clone())
                }
                None => {
                    out.push(Pattern::Filter(Expression::call(
                        Function::Eq,
                        vec![accessor(func), Expression::Const(v)],
                    )));
                    Component::Any
                }
            },
        };
        let tc = TermComponentsPattern {
            term: t,
            subject: component(Function::TripleSubject, IrTerm::from(s), out),
            predicate: component(Function::TriplePredicate, IrTerm::from(p), out),
            object: component(Function::TripleObject, o, out),
        };
        if tc.components().into_iter().any(|c| c.var().is_some())
            || matches!(tc.subject, Component::Node(_))
        {
            out.push(Pattern::TermComponents(tc));
        }
    }
}

/// The materialized term for an edge whose three positions are constants
/// and whose object datatype is known; `None` otherwise.
fn constant_term(edge: &IrTriplePattern) -> Option<fluree_db_core::TripleTermValue> {
    use fluree_db_core::FlakeValue;
    let s = match &edge.s {
        Ref::Sid(s) => s.clone(),
        _ => return None,
    };
    let p = match &edge.p {
        Ref::Sid(p) => p.clone(),
        _ => return None,
    };
    let (o, dt, lang) = match (&edge.o, &edge.dtc) {
        (IrTerm::Sid(sid), _) => (
            FlakeValue::Ref(sid.clone()),
            fluree_db_core::edge::id_datatype_sid(),
            None,
        ),
        (IrTerm::Value(v), Some(DatatypeConstraint::Explicit(dt))) => (v.clone(), dt.clone(), None),
        (IrTerm::Value(v), Some(DatatypeConstraint::LangTag(tag))) => (
            v.clone(),
            fluree_db_core::Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::rdf_names::LANG_STRING,
            ),
            Some(tag.to_string()),
        ),
        _ => return None,
    };
    Some(fluree_db_core::TripleTermValue { s, p, o, dt, lang })
}
