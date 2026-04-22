//! SELECT query parsing.

use crate::ast::{SelectClause, SelectModifier, SelectQuery, SelectVariable, SelectVariables, Var};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;

use super::expr::parse_expression;

impl super::Parser<'_> {
    /// Parse a SELECT query.
    pub(super) fn parse_select_query(&mut self) -> Option<SelectQuery> {
        let start = self.stream.current_span();

        // Parse SELECT clause
        let select = self.parse_select_clause()?;

        // Parse optional dataset clause (FROM, FROM NAMED)
        let dataset = self.parse_dataset_clause();

        // Parse WHERE clause
        let where_clause = self.parse_where_clause()?;

        // Parse solution modifiers
        let modifiers = self.parse_solution_modifiers();

        // Parse optional post-query VALUES clause (SPARQL grammar: ValuesClause after SolutionModifier)
        let values = if self.stream.check_keyword(TokenKind::KwValues) {
            self.parse_values_pattern().map(Box::new)
        } else {
            None
        };

        let span = start.union(self.stream.previous_span());

        Some(SelectQuery {
            select,
            dataset,
            where_clause,
            modifiers,
            values,
            span,
        })
    }

    /// Parse a SELECT clause.
    fn parse_select_clause(&mut self) -> Option<SelectClause> {
        let start = self.stream.current_span();

        // Consume SELECT keyword
        if !self.stream.match_keyword(TokenKind::KwSelect) {
            self.stream.error_at_current("expected SELECT");
            return None;
        }

        // Check for DISTINCT or REDUCED modifier
        let modifier = if self.stream.match_keyword(TokenKind::KwDistinct) {
            Some(SelectModifier::Distinct)
        } else if self.stream.match_keyword(TokenKind::KwReduced) {
            Some(SelectModifier::Reduced)
        } else {
            None
        };

        // Parse variable list or *
        let variables = if self.stream.match_token(&TokenKind::Star) {
            SelectVariables::Star
        } else {
            let vars = self.parse_select_variables()?;
            if vars.is_empty() {
                self.stream
                    .error_at_current("expected variable or * in SELECT clause");
                return None;
            }
            SelectVariables::Explicit(vars)
        };

        let span = start.union(self.stream.previous_span());

        Some(SelectClause {
            modifier,
            variables,
            span,
        })
    }

    /// Parse the variables in a SELECT clause.
    pub(super) fn parse_select_variables(&mut self) -> Option<Vec<SelectVariable>> {
        let mut vars = Vec::new();

        while let Some(var) = self.parse_select_variable() {
            vars.push(var);
        }

        Some(vars)
    }

    /// Parse a single variable or expression in SELECT.
    fn parse_select_variable(&mut self) -> Option<SelectVariable> {
        // Check for variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(SelectVariable::Var(Var::new(name.as_ref(), span)));
        }

        // Check for (expr AS ?var)
        if self.stream.check(&TokenKind::LParen) {
            let start = self.stream.current_span();
            self.stream.advance(); // consume (

            // Parse the expression
            let expr = match parse_expression(self.stream) {
                Ok(e) => e,
                Err(msg) => {
                    self.stream.error_at_current(&msg);
                    // Skip to closing paren for recovery
                    self.skip_to_closing_paren();
                    return None;
                }
            };

            // Expect AS keyword
            if !self.stream.match_keyword(TokenKind::KwAs) {
                let expr_span = start.union(self.stream.previous_span());
                self.stream.add_diagnostic(
                    Diagnostic::new(
                        DiagCode::ExpectedToken,
                        "expression in SELECT requires 'AS ?variable' alias",
                        expr_span,
                    )
                    .with_help("Use (expression AS ?variable) syntax"),
                );
                self.skip_to_closing_paren();
                return None;
            }

            // Parse the alias variable
            let (name, var_span) = match self.stream.consume_var() {
                Some(v) => v,
                None => {
                    self.stream.error_at_current("expected variable after AS");
                    self.skip_to_closing_paren();
                    return None;
                }
            };

            // Expect closing paren
            if !self.stream.match_token(&TokenKind::RParen) {
                self.stream
                    .error_at_current("expected ')' after alias variable");
                return None;
            }

            let span = start.union(self.stream.previous_span());
            return Some(SelectVariable::Expr {
                expr,
                alias: Var::new(name.as_ref(), var_span),
                span,
            });
        }

        None
    }

    /// Skip tokens until we find a closing paren at depth 0.
    pub(super) fn skip_to_closing_paren(&mut self) {
        self.stream
            .skip_balanced(&TokenKind::LParen, &TokenKind::RParen);
    }
}
