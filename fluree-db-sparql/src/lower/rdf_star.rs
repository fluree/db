//! RDF-star quoted triple lowering.
//!
//! Handles the expansion of RDF-star quoted triples with metadata annotations
//! (`f:t` and `f:op`) into regular triple patterns with BIND expressions.

use crate::ast::term::{ObjectTerm, PredicateTerm, SubjectTerm, Term as SparqlTerm};
use crate::ast::TriplePattern as SparqlTriplePattern;
use crate::span::SourceSpan;

use fluree_db_query::ir::{Expression, Function, Pattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::triple::{Term, TriplePattern};
use fluree_db_query::var_registry::VarId;
use fluree_vocab::fluree;

use std::collections::HashMap;

use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a BGP with special handling for RDF-star quoted triples.
    ///
    /// When a triple pattern has a quoted triple as subject and a metadata
    /// predicate (f:t or f:op), we expand it to:
    /// 1. The inner triple pattern from the quoted triple
    /// 2. A BIND expression that extracts the metadata from the object variable
    ///
    /// Example:
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
    pub(super) fn lower_bgp_with_rdf_star(
        &mut self,
        patterns: &[SparqlTriplePattern],
    ) -> Result<Vec<Pattern>> {
        // Track quoted triples we've already processed (by their span as key)
        // to avoid duplicating the inner triple pattern
        let mut processed_quoted_triples: HashMap<SourceSpan, VarId> = HashMap::new();
        let mut result = Vec::new();

        for tp in patterns {
            match &tp.subject {
                SubjectTerm::QuotedTriple(qt) => {
                    // Check if this quoted triple was already processed
                    let object_var = if let Some(&var_id) = processed_quoted_triples.get(&qt.span) {
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

                        processed_quoted_triples.insert(qt.span, obj_var);
                        result.push(Pattern::Triple(inner_tp));
                        obj_var
                    };

                    // Check if the predicate is a metadata annotation (f:t or f:op)
                    let predicate_iri = self.get_predicate_iri(&tp.predicate)?;

                    if predicate_iri == fluree::DB_T {
                        // f:t annotation - bind t() function result
                        let bound_var = self.lower_object_to_var(&tp.object)?;
                        result.push(Pattern::Bind {
                            var: bound_var,
                            expr: Expression::call(Function::T, vec![Expression::Var(object_var)]),
                        });
                    } else if predicate_iri == fluree::DB_OP {
                        // f:op annotation - bind op() function result
                        let bound_var = self.lower_object_to_var(&tp.object)?;
                        result.push(Pattern::Bind {
                            var: bound_var,
                            expr: Expression::call(Function::Op, vec![Expression::Var(object_var)]),
                        });
                    } else {
                        // Other predicates on quoted triples are not supported
                        return Err(LowerError::not_implemented(
                            format!(
                                "RDF-star quoted triple with predicate '{predicate_iri}' (only f:t and f:op are supported)"
                            ),
                            tp.subject.span(),
                        ));
                    }
                }
                _ => {
                    // Regular triple pattern
                    let lowered = self.lower_triple_pattern(tp)?;
                    result.push(Pattern::Triple(lowered));
                }
            }
        }

        Ok(result)
    }

    /// Lower the inner triple pattern from a quoted triple.
    fn lower_quoted_triple_inner(
        &mut self,
        qt: &crate::ast::QuotedTriple,
    ) -> Result<TriplePattern> {
        let s = self.lower_subject(&qt.subject)?;
        let p = self.lower_predicate(&qt.predicate)?;
        let o = self.lower_object(&qt.object)?;
        Ok(TriplePattern::new(s, p, o))
    }

    /// Get the expanded IRI from a predicate term.
    fn get_predicate_iri(&mut self, term: &PredicateTerm) -> Result<String> {
        match term {
            PredicateTerm::Var(v) => {
                // Variable predicates can't be checked for metadata annotations
                Err(LowerError::not_implemented(
                    "RDF-star metadata predicates (f:t, f:op) cannot be variables",
                    v.span,
                ))
            }
            PredicateTerm::Iri(iri) => self.expand_iri(iri),
        }
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
