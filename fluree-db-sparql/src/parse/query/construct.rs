//! CONSTRUCT query parsing.

use crate::ast::{ConstructQuery, ConstructTemplate, GraphPattern, TriplePattern, WhereClause};
use crate::lex::TokenKind;

impl super::Parser<'_> {
    /// Parse a CONSTRUCT query.
    ///
    /// CONSTRUCT builds RDF triples from a template.
    /// Grammar:
    ///   CONSTRUCT ConstructTemplate DatasetClause* WhereClause SolutionModifier
    ///   | CONSTRUCT DatasetClause* WHERE '{' TriplesTemplate? '}' SolutionModifier
    pub(super) fn parse_construct_query(&mut self) -> Option<ConstructQuery> {
        let start = self.stream.current_span();

        // Consume CONSTRUCT keyword
        if !self.stream.match_keyword(TokenKind::KwConstruct) {
            self.stream.error_at_current("expected CONSTRUCT");
            return None;
        }

        // Determine form based on what follows CONSTRUCT:
        // - If `{` → full form: template first, then optional dataset, then WHERE
        // - If `FROM` or `WHERE` → shorthand form: optional dataset, then WHERE
        if self.stream.check(&TokenKind::LBrace) {
            // Full form: CONSTRUCT { template } DatasetClause* WHERE { pattern }
            let template = self.parse_construct_template()?;

            // Parse optional dataset clause
            let dataset = self.parse_dataset_clause();

            // Parse WHERE clause
            let where_clause = self.parse_where_clause()?;

            // Parse solution modifiers
            let modifiers = self.parse_solution_modifiers();

            let span = start.union(self.stream.previous_span());

            Some(ConstructQuery {
                template: Some(template),
                dataset,
                where_clause,
                modifiers,
                span,
            })
        } else {
            // Shorthand form: CONSTRUCT DatasetClause* WHERE '{' TriplesTemplate? '}'
            // The WHERE block is a *triples template* only — FILTER, GRAPH,
            // OPTIONAL, BIND, UNION, sub-SELECT, etc. are NOT permitted here
            // (negative tests constructwhere05/06). The template is derived
            // from these triples at lowering time.
            let dataset = self.parse_dataset_clause();

            if !self.stream.match_keyword(TokenKind::KwWhere) {
                self.stream
                    .error_at_current("expected WHERE for CONSTRUCT shorthand form");
                return None;
            }

            let where_clause = self.parse_construct_where_shorthand()?;

            // Parse solution modifiers
            let modifiers = self.parse_solution_modifiers();

            let span = start.union(self.stream.previous_span());

            Some(ConstructQuery {
                template: None, // Shorthand - template derived from WHERE
                dataset,
                where_clause,
                modifiers,
                span,
            })
        }
    }

    /// Parse the restricted WHERE block of the `CONSTRUCT WHERE { ... }` shorthand.
    ///
    /// Per the SPARQL 1.1 grammar (`ConstructQuery`), the shorthand WHERE block is
    /// `'{' TriplesTemplate? '}'` — a basic graph pattern of triple patterns only.
    /// Anything else (FILTER, GRAPH, OPTIONAL, BIND, UNION, nested groups) is a
    /// syntax error in this position.
    fn parse_construct_where_shorthand(&mut self) -> Option<WhereClause> {
        let start = self.stream.current_span();

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream.error_at_current("expected '{' after WHERE");
            return None;
        }

        let mut triples: Vec<TriplePattern> = Vec::new();
        // Standalone reified triples (`<< s p o ~ r? >> .`) desugar to
        // `GraphPattern::AnnotationTarget` nodes; they ride alongside
        // the plain-triples BGP (SPARQL 1.2 allows `ReifiedTriple` with
        // an empty property list inside the shorthand's TriplesTemplate).
        let mut reified_targets: Vec<GraphPattern> = Vec::new();

        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            let subject = match self.parse_subject() {
                Some(s) => s,
                None => {
                    self.stream
                        .error_at_current("CONSTRUCT WHERE shorthand permits only triple patterns");
                    return None;
                }
            };

            if matches!(&subject, crate::ast::SubjectTerm::QuotedTriple(_)) && !self.is_verb_start()
            {
                let crate::ast::SubjectTerm::QuotedTriple(qt) = subject else {
                    unreachable!("matched QuotedTriple above")
                };
                reified_targets.push(self.reified_triple_to_annotation_target(qt));
            } else {
                self.parse_construct_predicate_object_list(&subject, &mut triples)?;
            }

            // Optional triple separator
            self.stream.match_token(&TokenKind::Dot);
        }

        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' after CONSTRUCT WHERE pattern");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        let pattern = if reified_targets.is_empty() {
            GraphPattern::Bgp {
                patterns: triples,
                span,
            }
        } else {
            // Mirror `parse_group_graph_pattern`'s shape: the Group
            // corresponds to the shorthand's explicit `{ }` braces.
            let mut patterns: Vec<GraphPattern> = Vec::new();
            if !triples.is_empty() {
                let bgp_span = super::span_of_triples(&triples);
                patterns.push(GraphPattern::Bgp {
                    patterns: triples,
                    span: bgp_span,
                });
            }
            patterns.append(&mut reified_targets);
            if patterns.len() == 1 {
                patterns.remove(0)
            } else {
                GraphPattern::Group { patterns, span }
            }
        };
        Some(WhereClause::new(pattern, true, span))
    }

    /// Parse a CONSTRUCT template (the triples to build).
    fn parse_construct_template(&mut self) -> Option<ConstructTemplate> {
        let start = self.stream.current_span();

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' for CONSTRUCT template");
            return None;
        }

        // Parse triple patterns (simple triples only, no property paths in templates)
        let mut triples: Vec<TriplePattern> = Vec::new();

        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            // Parse subject
            let subject = match self.parse_subject() {
                Some(s) => s,
                None => {
                    if self.stream.check(&TokenKind::RBrace) {
                        break; // Empty template is allowed
                    }
                    self.stream
                        .error_at_current("expected subject in CONSTRUCT template");
                    return None;
                }
            };

            // Parse predicate-object list (folding in any blank-node
            // property-list triples the subject produced).
            self.parse_template_triples_for_subject(&subject, &mut triples)?;

            // Optional dot
            self.stream.match_token(&TokenKind::Dot);
        }

        // Expect closing brace
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' after CONSTRUCT template");
            return None;
        }

        let span = start.union(self.stream.previous_span());

        Some(ConstructTemplate::new(triples, span))
    }
}
