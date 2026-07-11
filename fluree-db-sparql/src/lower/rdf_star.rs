//! RDF-star / RDF 1.2 quoted-triple lowering for BGPs.
//!
//! Two readings of `<< s p o >>` share the same tokens:
//!
//! 1. **Legacy Fluree history form** — a reifier-less quoted-triple
//!    *subject* whose predicate is `f:t` / `f:op` expands to the inner
//!    triple plus BIND expressions extracting the metadata.
//! 2. **RDF 1.2 reified triple** — everything else. The term denotes
//!    its reifier node; the desugaring in `lower/annotation.rs` emits
//!    `r rdf:reifies <<( s p o )>>` as `Pattern::AnnotationTarget` and
//!    substitutes the reifier ref into the enclosing position.

use crate::ast::term::{ObjectTerm, PredicateTerm, SubjectTerm, Term as SparqlTerm};
use crate::ast::TriplePattern as SparqlTriplePattern;
use crate::span::SourceSpan;

use fluree_db_query::ir::triple::{Term, TriplePattern};
use fluree_db_query::ir::{Expression, Function, Pattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;
use fluree_vocab::fluree;

use std::collections::HashMap;

use super::annotation::ReifiedCache;
use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a BGP with special handling for RDF-star quoted triples
    /// and RDF 1.2 annotation tails / reified-triple terms.
    ///
    /// Legacy example:
    /// ```sparql
    /// << ex:alice ex:age ?age >> f:t ?t ; f:op ?op .
    /// ```
    ///
    /// Becomes:
    /// ```text
    /// ex:alice ex:age ?age .  (triple pattern)
    /// BIND(t(?age) AS ?t)     (metadata binding)
    /// BIND(op(?age) AS ?op)   (metadata binding)
    /// ```
    ///
    /// RDF 1.2 example:
    /// ```sparql
    /// ?doc ex:cites << ex:a ex:b ex:c ~ ?r >> .
    /// ```
    ///
    /// Becomes (conceptually):
    /// ```text
    /// ?r rdf:reifies <<( ex:a ex:b ex:c )>> .   (AnnotationTarget)
    /// ?doc ex:cites ?r .                        (triple pattern)
    /// ```
    pub(super) fn lower_bgp_with_rdf_star(
        &mut self,
        patterns: &[SparqlTriplePattern],
    ) -> Result<Vec<Pattern>> {
        // Track legacy quoted triples we've already processed (by their
        // span as key) to avoid duplicating the inner triple pattern.
        let mut legacy_processed: HashMap<SourceSpan, VarId> = HashMap::new();
        // Memo for desugared reified-triple occurrences: a quoted
        // triple shared across `;`-continued patterns binds ONE reifier.
        let mut reified_cache = ReifiedCache::new();
        let mut result = Vec::new();

        for tp in patterns {
            // Legacy carve-out: reifier-less `<< s p o >>` subject with
            // an `f:t` / `f:op` metadata predicate keeps the history
            // reading. A `~ reifier` or any other predicate selects the
            // RDF 1.2 reified-triple reading below.
            if let SubjectTerm::QuotedTriple(qt) = &tp.subject {
                if qt.reifier.is_none() {
                    if let PredicateTerm::Iri(iri) = &tp.predicate {
                        let predicate_iri = self.expand_iri(iri)?;
                        if predicate_iri == fluree::DB_T || predicate_iri == fluree::DB_OP {
                            self.lower_legacy_history_triple(
                                tp,
                                &predicate_iri,
                                &mut legacy_processed,
                                &mut result,
                            )?;
                            continue;
                        }
                    }
                }
            }

            // RDF 1.2 reading: reified-triple terms denote their
            // reifier node (desugared as sibling AnnotationTargets).
            let s = match &tp.subject {
                SubjectTerm::QuotedTriple(qt) => {
                    self.lower_reified_triple(qt, &mut reified_cache, &mut result)?
                }
                other => self.lower_subject(other)?,
            };
            let p = self.lower_predicate(&tp.predicate)?;
            // Constraint-preserving object lowering ONLY on the
            // annotation path (see `lower_object_with_constraint` —
            // plain-triple scan semantics must not change).
            let with_constraint = tp.annotation.is_some();
            let (o, dtc) = self.lower_object_desugared(
                &tp.object,
                &mut reified_cache,
                &mut result,
                with_constraint,
            )?;
            let edge = TriplePattern { s, p, o, dtc };

            match &tp.annotation {
                Some(ann) => self.lower_annotation_units(edge, ann, &mut result)?,
                None => result.push(Pattern::Triple(edge)),
            }
        }

        Ok(result)
    }

    /// Lower one legacy `<< s p ?o >> f:t|f:op ?meta` triple. The inner
    /// triple is emitted once per quoted-triple occurrence (memoized by
    /// span across the `;`-continued patterns) and each metadata
    /// predicate becomes a BIND over the object variable.
    fn lower_legacy_history_triple(
        &mut self,
        tp: &SparqlTriplePattern,
        predicate_iri: &str,
        legacy_processed: &mut HashMap<SourceSpan, VarId>,
        result: &mut Vec<Pattern>,
    ) -> Result<()> {
        let SubjectTerm::QuotedTriple(qt) = &tp.subject else {
            unreachable!("caller matched a quoted-triple subject")
        };

        // The legacy quoted-triple path (`<< s p o >> f:t ?t`)
        // is the f:t / f:op metadata annotation form — it
        // has no representation for an RDF 1.2 annotation
        // tail (`{| ... |}`). Silently dropping the tail
        // would lose user intent without warning; reject
        // explicitly. Users who want annotations on the
        // inner triple should write the standard form
        // `s p o {| ... |}` instead.
        if tp.annotation.is_some() {
            return Err(LowerError::not_implemented(
                "RDF 1.2 annotation tail (`{| ... |}`) is not supported on \
                 legacy RDF-star quoted-triple patterns (`<< s p o >> ...`); \
                 write the annotation directly on the inner triple as \
                 `s p o {| ... |}` instead",
                qt.span,
            ));
        }

        // Check if this quoted triple was already processed
        let object_var = if let Some(&var_id) = legacy_processed.get(&qt.span) {
            var_id
        } else {
            // First time seeing this quoted triple - lower the inner pattern
            let inner_tp = self.lower_quoted_triple_inner(qt)?;

            // Get the object variable from the inner pattern (needed for metadata binding)
            let obj_var = match &inner_tp.o {
                Term::Var(v) => *v,
                _ => {
                    // Object must be a variable for metadata binding to work
                    return Err(LowerError::not_implemented(
                        "RDF-star metadata annotations require the quoted triple's object to be a variable",
                        qt.span,
                    ));
                }
            };

            legacy_processed.insert(qt.span, obj_var);
            result.push(Pattern::Triple(inner_tp));
            obj_var
        };

        let function = if predicate_iri == fluree::DB_T {
            Function::T
        } else {
            Function::Op
        };
        let bound_var = self.lower_object_to_var(&tp.object)?;
        result.push(Pattern::Bind {
            var: bound_var,
            expr: Expression::call(function, vec![Expression::Var(object_var)]),
        });
        Ok(())
    }

    /// Lower the inner triple pattern from a legacy quoted triple.
    fn lower_quoted_triple_inner(
        &mut self,
        qt: &crate::ast::QuotedTriple,
    ) -> Result<TriplePattern> {
        let s = self.lower_subject(&qt.subject)?;
        let p = self.lower_predicate(&qt.predicate)?;
        let o = self.lower_object(&qt.object)?;
        Ok(TriplePattern::new(s, p, o))
    }

    /// Lower an object term to a variable ID (for BIND targets).
    fn lower_object_to_var(&mut self, term: &ObjectTerm) -> Result<VarId> {
        match term {
            SparqlTerm::Var(v) => Ok(self.register_var(v)),
            _ => Err(LowerError::not_implemented(
                "RDF-star metadata annotation object must be a variable",
                term.span(),
            )),
        }
    }
}
