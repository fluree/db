//! CONSTRUCT query lowering.
//!
//! Converts SPARQL CONSTRUCT queries to `Query` with template patterns,
//! supporting both explicit templates and the `CONSTRUCT WHERE { ... }` shorthand.

use crate::ast::query::{ConstructQuery, SolutionModifiers};
use crate::ast::TriplePattern as SparqlTriplePattern;

use fluree_db_query::ir::triple::TriplePattern;
use fluree_db_query::ir::{
    ConstructTemplate as QueryConstructTemplate, Pattern, Query, QueryOptions, QueryOutput,
};
use fluree_db_query::parse::encode::IriEncoder;

use super::{LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a CONSTRUCT query to a Query.
    pub(super) fn lower_construct(&mut self, construct: &ConstructQuery) -> Result<Query> {
        // Lower WHERE clause patterns
        let patterns = self.lower_graph_pattern(&construct.where_clause.pattern)?;

        // Lower template triples
        // For "CONSTRUCT WHERE { ... }" shorthand, use WHERE patterns as template
        let template_patterns = match &construct.template {
            Some(template) => {
                // Explicit template: lower each triple pattern
                self.lower_construct_template(&template.triples)?
            }
            None => {
                // Shorthand form: extract triple patterns from WHERE clause
                self.extract_template_from_patterns(&patterns)
            }
        };

        // Build construct template
        let construct_template = QueryConstructTemplate::new(template_patterns);

        // Lower solution modifiers (CONSTRUCT supports ORDER BY, LIMIT, OFFSET but not GROUP BY/HAVING)
        let options = self.lower_construct_modifiers(&construct.modifiers)?;

        let ctx = self.build_jsonld_context()?;
        let ctx_val = self.build_jsonld_context_value();

        Ok(Query {
            context: ctx,
            orig_context: Some(ctx_val),
            output: QueryOutput::Construct(construct_template),
            patterns,
            options,
            graph_select: None, // SPARQL doesn't support graph crawl
            post_values: None,
        })
    }

    /// Lower CONSTRUCT template triples to resolved TriplePatterns.
    fn lower_construct_template(
        &mut self,
        triples: &[SparqlTriplePattern],
    ) -> Result<Vec<TriplePattern>> {
        let mut result = Vec::with_capacity(triples.len());
        for tp in triples {
            result.push(self.lower_triple_pattern(tp)?);
        }
        Ok(result)
    }

    /// Extract triple patterns from lowered WHERE patterns for CONSTRUCT WHERE shorthand.
    ///
    /// Recursively walks the pattern tree and extracts all Triple patterns.
    fn extract_template_from_patterns(&self, patterns: &[Pattern]) -> Vec<TriplePattern> {
        let mut result = Vec::new();
        self.collect_triples(patterns, &mut result);
        result
    }

    /// Recursively collect triple patterns from nested pattern structures.
    fn collect_triples(&self, patterns: &[Pattern], out: &mut Vec<TriplePattern>) {
        for pattern in patterns {
            match pattern {
                Pattern::Triple(tp) => out.push(tp.clone()),
                Pattern::Optional(inner)
                | Pattern::Minus(inner)
                | Pattern::Exists(inner)
                | Pattern::NotExists(inner) => self.collect_triples(inner, out),
                Pattern::Union(branches) => {
                    for branch in branches {
                        self.collect_triples(branch, out);
                    }
                }
                // Filters, Binds, Values, PropertyPaths, Subqueries, IndexSearch, Service, and R2rml don't contribute template triples
                Pattern::Filter(_)
                | Pattern::Bind { .. }
                | Pattern::Values { .. }
                | Pattern::PropertyPath(_)
                | Pattern::Subquery(_)
                | Pattern::IndexSearch(_)
                | Pattern::VectorSearch(_)
                | Pattern::Graph { .. }
                | Pattern::Service(_)
                | Pattern::R2rml(_)
                | Pattern::GeoSearch(_)
                | Pattern::S2Search(_) => {}
            }
        }
    }

    /// Lower solution modifiers for CONSTRUCT (no GROUP BY/HAVING/aggregates).
    fn lower_construct_modifiers(&mut self, modifiers: &SolutionModifiers) -> Result<QueryOptions> {
        let mut options = QueryOptions::default();
        self.lower_base_modifiers(modifiers, &mut options)?;
        Ok(options)
    }
}
