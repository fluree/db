//! DESCRIBE and ASK query parsing.

use crate::ast::{AskQuery, DescribeQuery, DescribeTarget};
use crate::lex::TokenKind;

impl super::Parser<'_> {
    /// Parse an ASK query.
    ///
    /// ASK queries return a boolean indicating whether the pattern matches.
    /// Grammar: ASK DatasetClause* WhereClause SolutionModifier
    pub(super) fn parse_ask_query(&mut self) -> Option<AskQuery> {
        let start = self.stream.current_span();

        // Consume ASK keyword
        if !self.stream.match_keyword(TokenKind::KwAsk) {
            self.stream.error_at_current("expected ASK");
            return None;
        }

        // Parse optional dataset clause (FROM, FROM NAMED)
        let dataset = self.parse_dataset_clause();

        // Parse WHERE clause
        let where_clause = self.parse_where_clause()?;

        // Parse solution modifiers (though most don't make sense for ASK)
        let modifiers = self.parse_solution_modifiers();

        let span = start.union(self.stream.previous_span());

        Some(AskQuery {
            dataset,
            where_clause,
            modifiers,
            span,
        })
    }

    /// Parse a DESCRIBE query.
    ///
    /// DESCRIBE returns RDF data about resources.
    /// Grammar: DESCRIBE ( VarOrIri+ | '*' ) DatasetClause* WhereClause? SolutionModifier
    pub(super) fn parse_describe_query(&mut self) -> Option<DescribeQuery> {
        let start = self.stream.current_span();

        // Consume DESCRIBE keyword
        if !self.stream.match_keyword(TokenKind::KwDescribe) {
            self.stream.error_at_current("expected DESCRIBE");
            return None;
        }

        // Parse target: * or list of variables/IRIs
        let target = if self.stream.match_token(&TokenKind::Star) {
            DescribeTarget::Star
        } else {
            let mut resources = Vec::new();

            // Parse at least one resource
            if let Some(resource) = self.parse_var_or_iri() {
                resources.push(resource);
            } else {
                self.stream
                    .error_at_current("expected '*' or variable/IRI after DESCRIBE");
                return None;
            }

            // Parse additional resources
            while let Some(resource) = self.parse_var_or_iri() {
                resources.push(resource);
            }

            DescribeTarget::Resources(resources)
        };

        // Parse optional dataset clause
        let dataset = self.parse_dataset_clause();

        // Parse optional WHERE clause
        let where_clause = if self.stream.check_keyword(TokenKind::KwWhere)
            || self.stream.check(&TokenKind::LBrace)
        {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        // Parse solution modifiers
        let modifiers = self.parse_solution_modifiers();

        let span = start.union(self.stream.previous_span());

        Some(DescribeQuery {
            target,
            dataset,
            where_clause,
            modifiers,
            span,
        })
    }
}
