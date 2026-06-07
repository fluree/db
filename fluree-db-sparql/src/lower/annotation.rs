//! RDF 1.2 annotation lowering for the SPARQL query path.
//!
//! Translates the M4.2 AST shapes
//! (`TriplePattern.annotation` and `GraphPattern::AnnotationTarget`)
//! into the existing query IR (`Pattern::EdgeAnnotation` and
//! `Pattern::AnnotationTarget`). The IR's
//! `expand_edge_annotation_patterns` step (in `fluree-db-query`)
//! handles the f:reifies* fan-out from there.
//!
//! Sibling triples about a reifier variable are NOT folded into
//! `body` — they sit in the surrounding scope and join via the
//! standard executor on the bound reifier var. See
//! `docs/concepts/edge-annotations.md` "SPARQL 1.2 / RDF 1.2 surface"
//! for the rationale.

use crate::ast::annotation::{Annotation, AnnotationBlock, ReifierId, TripleTerm};
use crate::ast::term::{BlankNodeValue, SubjectTerm};
use crate::ast::TriplePattern as SparqlTriplePattern;

use fluree_db_query::ir::triple::{Ref, TriplePattern as IrTriplePattern};
use fluree_db_query::ir::Pattern;
use fluree_db_query::parse::encode::IriEncoder;

use super::{LoweringContext, Result};

/// Prefix used for registry names of synthetic variables that must
/// stay invisible to `SELECT *` and unmatchable by user input. `#`
/// is comment-start in SPARQL, so no user variable can lex with this
/// prefix.
pub(super) const INTERNAL_VAR_PREFIX: &str = "#";

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a SPARQL triple that carries an annotation tail into a
    /// `Pattern::EdgeAnnotation` IR node. The body comes from the
    /// annotation block's predicate-object entries; an empty/missing
    /// block produces `body: vec![]`.
    pub(super) fn lower_annotated_triple(
        &mut self,
        tp: &SparqlTriplePattern,
        ann: &Annotation,
    ) -> Result<Pattern> {
        // Use the constraint-preserving object lowering for the base
        // edge so a literal object pins datatype / language onto
        // `edge.dtc`. The where-plan expansion (`f:reifiesObject`
        // synthesis) clones `edge.dtc` onto the synthesized lookup
        // triple — without it, same-lexical literals with different
        // datatypes (or languages) would cross-match annotations.
        let s = self.lower_subject(&tp.subject)?;
        let p = self.lower_predicate(&tp.predicate)?;
        let (o, dtc) = self.lower_object_with_constraint(&tp.object)?;
        let edge = IrTriplePattern { s, p, o, dtc };
        let annotation_ref = self.lower_reifier_id(ann.reifier.as_ref())?;
        let body = self.lower_annotation_block_body(&annotation_ref, ann.block.as_ref())?;
        Ok(Pattern::EdgeAnnotation {
            edge,
            annotation: annotation_ref,
            body,
        })
    }

    /// Lower a `GraphPattern::AnnotationTarget` (the
    /// `?ann rdf:reifies <<( s p o )>>` form) into
    /// `Pattern::AnnotationTarget` IR. Always emits an empty body —
    /// surrounding sibling triples about the reifier join through the
    /// standard executor.
    pub(super) fn lower_annotation_target_pattern(
        &mut self,
        reifier: &SubjectTerm,
        triple_term: &TripleTerm,
    ) -> Result<Pattern> {
        let annotation_ref = self.lower_subject(reifier)?;
        let edge = self.lower_triple_term(triple_term)?;
        Ok(Pattern::AnnotationTarget {
            annotation: annotation_ref,
            edge,
            body: Vec::new(),
        })
    }

    /// Resolve the reifier id following `~`. Mints a fresh synthetic
    /// non-distinguished variable when the user wrote an anonymous
    /// `{| ... |}` (i.e. no preceding `~`) or a bare `~` with no id.
    fn lower_reifier_id(&mut self, reifier: Option<&ReifierId>) -> Result<Ref> {
        match reifier {
            Some(ReifierId::Iri(iri)) => self.lower_iri_ref(iri),
            Some(ReifierId::BlankNode(b)) => match &b.value {
                BlankNodeValue::Labeled(label) => {
                    let var_id = self.vars.get_or_insert(&format!("_:{label}"));
                    Ok(Ref::Var(var_id))
                }
                BlankNodeValue::Anon => {
                    let var_id = self.vars.get_or_insert(&format!("_:b{}", self.vars.len()));
                    Ok(Ref::Var(var_id))
                }
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

    /// Lower the `{| pred obj ; pred obj |}` body to a flat list of
    /// `Pattern::Triple` patterns whose subject is the reifier.
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
        let mut out = Vec::with_capacity(block.entries.len());
        for entry in &block.entries {
            let p = self.lower_predicate(&entry.predicate)?;
            let (o, dtc) = self.lower_object_with_constraint(&entry.object)?;
            out.push(Pattern::Triple(IrTriplePattern {
                s: annotation.clone(),
                p,
                o,
                dtc,
            }));
        }
        Ok(out)
    }

    /// Lower the inner `<<( s p o )>>` triple-term to an IR
    /// `TriplePattern`. The triple-term has no annotation tail and no
    /// nested triple terms (the parser already enforced both).
    ///
    /// Carries the same constraint-preserving object lowering as the
    /// annotation-block body so reified base-edge object positions
    /// match precisely.
    fn lower_triple_term(&mut self, term: &TripleTerm) -> Result<IrTriplePattern> {
        let s = self.lower_subject(&term.subject)?;
        let p = self.lower_predicate(&term.predicate)?;
        let (o, dtc) = self.lower_object_with_constraint(&term.object)?;
        Ok(IrTriplePattern { s, p, o, dtc })
    }
}

/// Sentinel marker used by `lower_bgp_with_rdf_star` to detect when a
/// triple should be lowered through the annotation path. Pulled out
/// here so the rdf_star module stays focused on the legacy
/// `f:t` / `f:op` extraction logic.
pub(super) fn triple_has_annotation(tp: &SparqlTriplePattern) -> bool {
    tp.annotation.is_some()
}
