//! CONSTRUCT query lowering.
//!
//! Converts SPARQL CONSTRUCT queries to `Query` with template patterns,
//! supporting both explicit templates and the `CONSTRUCT WHERE { ... }` shorthand.

use std::sync::Arc;

use crate::ast::annotation::{AnnotationVerb, ReifierId};
use crate::ast::query::{ConstructQuery, ConstructTemplate, SelectClause, SelectVariables};
use crate::ast::{GraphName, SubjectTerm, Term};

use fluree_db_query::ir::triple::{Ref, TriplePattern};
use fluree_db_query::ir::{
    ConstructTemplate as QueryConstructTemplate, Pattern, Query, QueryOutput,
};
use fluree_db_query::parse::encode::IriEncoder;

use super::select::BaseModifiers;
use super::{post_values_then, LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a CONSTRUCT query to a Query.
    pub(super) fn lower_construct(&mut self, construct: &ConstructQuery) -> Result<Query> {
        // Lower WHERE clause patterns
        let mut patterns = self.lower_graph_pattern(&construct.where_clause.pattern)?;

        // Lower the template. For the "CONSTRUCT WHERE { ... }" shorthand the
        // WHERE clause's triples (and their edge annotations) are the template.
        let mut construct_template = match &construct.template {
            Some(template) => self.lower_construct_template(template)?,
            None => self.extract_template_from_patterns(&patterns),
        };

        // Template blank nodes (`[ ]`, `_:a`, and the desugared cells of an RDF
        // collection `( ... )`) lower to variables whose registry name keeps the
        // `_:` prefix the term lowerer assigns. Ordinary query variables are
        // `?`-prefixed and stable Fluree blank ids (`_:fdb-...`) lower to
        // constants, so this prefix uniquely marks template blank nodes. The
        // WHERE clause never binds them, so the output path mints a fresh blank
        // node per solution for each (see `ConstructTemplate::bnode_vars`).
        construct_template.bnode_vars = construct_template
            .var_iter()
            .filter(|&v| {
                self.vars
                    .try_name(v)
                    .is_some_and(|name| name.starts_with("_:"))
            })
            .collect();

        // Solution modifiers. GROUP BY, HAVING and an aggregate ORDER BY group
        // the level (§18.2.4.1): the template is then instantiated once per
        // group that passes HAVING. The level lowers like a SELECT level that
        // projects nothing.
        let select = SelectClause {
            modifier: None,
            variables: SelectVariables::Explicit(Vec::new()),
            span: construct.span,
        };
        let values = self.lower_trailing_values(construct.values.as_deref(), &mut patterns)?;
        let level = self.lower_select_level(
            &select,
            &construct.modifiers,
            &mut patterns,
            values.as_ref(),
        )?;
        let post_values = post_values_then(values, level.binds, &mut patterns);
        let BaseModifiers {
            limit,
            offset,
            ordering,
            order_binds,
            // Lowered into `order_binds` with the level; always empty here.
            deferred_order_exprs: _,
        } = level.base;

        // A group solution binds the grouping's keys only (CONSTRUCT has no
        // SELECT expressions, so no aggregate outputs or binds reach the
        // template): any other template variable is unbound in it, and the
        // template triples that read it are skipped (§16.2). Rename those to
        // variables nothing binds; template blank nodes are minted per row.
        if let Some(grouping) = &level.grouping {
            let produced: std::collections::HashSet<_> = grouping
                .group_by_vars()
                .chain(grouping.aggregates().map(|spec| spec.output_var))
                .chain(grouping.binds().map(|(var, _)| *var))
                .collect();
            let unbound: std::collections::HashSet<_> = construct_template
                .var_iter()
                .filter(|v| !produced.contains(v) && !construct_template.bnode_vars.contains(v))
                .collect();
            for var in unbound {
                let fresh = self
                    .vars
                    .get_or_insert(&format!("?__construct_unbound_{}", self.vars.len()));
                construct_template.substitute_var(var, fresh);
            }
        }

        let ctx = self.build_jsonld_context()?;
        let ctx_val = self.build_jsonld_context_value();

        Ok(Query {
            context: ctx,
            orig_context: Some(ctx_val),
            output: QueryOutput::Construct(construct_template),
            patterns,
            reasoning: self.reasoning_config()?,
            grouping: level.grouping,
            ordering,
            order_binds,
            limit,
            offset,
            post_values,
            include_system_facts: false,
            union_default_graph: None,
            cypher_vocab: None,
            unmatched_optional: Default::default(),
        })
    }

    /// Lower a CONSTRUCT template: its triples, the `GRAPH` block each sits
    /// in, and RDF 1.2 annotations, written as an annotation tail
    /// (`?s ?p ?o ~ ?r {| ... |}`) or as `?r rdf:reifies <<( ?s ?p ?o )>>`.
    fn lower_construct_template(
        &mut self,
        template: &ConstructTemplate,
    ) -> Result<QueryConstructTemplate> {
        let mut out = QueryConstructTemplate::new(Vec::with_capacity(template.triples.len()));
        let reifies = self.encoder.encode_ref(fluree_vocab::rdf::REIFIES);
        for (i, tp) in template.triples.iter().enumerate() {
            let graph = match template.graph(i) {
                Some(GraphName::Iri(iri)) => Some(Ref::Iri(Arc::from(self.expand_iri(iri)?))),
                Some(GraphName::Var(v)) => Some(self.lower_var_ref(v)),
                None => None,
            };
            let s = self.lower_subject(&tp.subject)?;
            let p = self.lower_predicate(&tp.predicate)?;

            // `?r rdf:reifies <<( s p o )>>`: the triple term is the reified
            // triple and `?r` its reifier.
            if let Term::TripleTerm(term) = &tp.object {
                if p != reifies || tp.annotation.is_some() {
                    return Err(LowerError::not_implemented(
                        "a triple term in a CONSTRUCT template is only supported as the \
                         object of rdf:reifies",
                        tp.span,
                    ));
                }
                let mut siblings = Vec::new();
                let reified = self.lower_triple_term(term, &mut siblings)?;
                if !siblings.is_empty() {
                    return Err(LowerError::not_implemented(
                        "nested triple terms in a CONSTRUCT template",
                        tp.span,
                    ));
                }
                let triple = out.push_pattern(reified, graph.clone());
                out.push_reification(triple, s);
                continue;
            }

            // Carry the declared datatype into the template.
            // `lower_triple_pattern` ends at `TriplePattern::new(s, p, o)`,
            // which leaves `dtc: None` — fine for a WHERE pattern, where the
            // constraint would change what matches, but a CONSTRUCT template
            // is written, not matched. A datalog rule head reads this template
            // and falls back to a datatype guessed from the value when `dtc`
            // is absent, so `"2024-01-01"^^xsd:date` in a rule head stored
            // `xsd:string`: `DATATYPE()` said string and `YEAR()` was unbound,
            // while the identical head written in JSON-LD stored a real date.
            let (o, dtc) = self.lower_object_with_constraint(&tp.object)?;
            let edge = out.push_pattern(TriplePattern { s, p, o, dtc }, graph.clone());
            let Some(annotation) = &tp.annotation else {
                continue;
            };
            for unit in &annotation.units {
                let reifier = match &unit.reifier {
                    Some(ReifierId::BlankNode(b)) => {
                        self.lower_subject(&SubjectTerm::BlankNode(b.clone()))?
                    }
                    // A bare `~` or anonymous `{| |}`: a fresh blank node per
                    // solution, like `[ ]`.
                    None => Ref::Var(self.fresh_blank_node_var()),
                    other => self.lower_reifier_id(other.as_ref())?,
                };
                out.push_reification(edge, reifier.clone());
                for entry in unit.block.iter().flat_map(|b| &b.entries) {
                    let AnnotationVerb::Simple(pred) = &entry.verb else {
                        return Err(LowerError::not_implemented(
                            "a property path in a CONSTRUCT template annotation",
                            entry.span,
                        ));
                    };
                    let p = self.lower_predicate(pred)?;
                    let (o, dtc) = self.lower_object_with_constraint(&entry.object)?;
                    out.push_pattern(
                        TriplePattern {
                            s: reifier.clone(),
                            p,
                            o,
                            dtc,
                        },
                        graph.clone(),
                    );
                }
            }
        }
        Ok(out)
    }

    /// Extract the CONSTRUCT WHERE shorthand's template from the lowered WHERE
    /// patterns: every triple, and each edge annotation's link and body.
    fn extract_template_from_patterns(&self, patterns: &[Pattern]) -> QueryConstructTemplate {
        let mut out = QueryConstructTemplate::new(Vec::new());
        self.collect_triples(patterns, &mut out);
        out
    }

    /// Recursively collect triple patterns from nested pattern structures.
    fn collect_triples(&self, patterns: &[Pattern], out: &mut QueryConstructTemplate) {
        for pattern in patterns {
            match pattern {
                Pattern::Triple(tp) => {
                    out.push_pattern(tp.clone(), None);
                }
                Pattern::Optional(inner)
                | Pattern::Minus(inner)
                | Pattern::Exists(inner)
                | Pattern::NotExists(inner) => self.collect_triples(inner, out),
                Pattern::Union(branches) => {
                    for branch in branches {
                        self.collect_triples(branch, out);
                    }
                }
                Pattern::EdgeAnnotation {
                    edge,
                    annotation,
                    body,
                }
                | Pattern::AnnotationTarget {
                    edge,
                    annotation,
                    body,
                } => {
                    let triple = out.push_pattern(edge.clone(), None);
                    out.push_reification(triple, annotation.clone());
                    self.collect_triples(body, out);
                }
                // Filters, Binds, Values, PropertyPaths, Subqueries, IndexSearch, Service, and R2rml don't contribute template triples
                Pattern::Filter(_)
                | Pattern::Bind { .. }
                | Pattern::Unwind { .. }
                | Pattern::Values { .. }
                | Pattern::PropertyPath(_)
                | Pattern::ShortestPath(_)
                | Pattern::Subquery(_)
                | Pattern::IndexSearch(_)
                | Pattern::VectorSearch(_)
                | Pattern::Graph { .. }
                | Pattern::Service(_)
                | Pattern::R2rml(_)
                | Pattern::GeoSearch(_)
                | Pattern::S2Search(_)
                | Pattern::DefaultGraphSource { .. } => {}
            }
        }
    }
}
