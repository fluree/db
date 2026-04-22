//! Solution modifier parsing: ORDER BY, GROUP BY, HAVING, LIMIT, OFFSET.

use crate::ast::expr::Expression;
use crate::ast::query::{GroupByClause, GroupCondition, HavingClause};
use crate::ast::{
    LimitClause, OffsetClause, OrderByClause, OrderCondition, OrderDirection, OrderExpr,
    SolutionModifiers, Var,
};
use crate::lex::TokenKind;

use super::expr::parse_expression;

impl super::Parser<'_> {
    /// Parse solution modifiers (ORDER BY, LIMIT, OFFSET).
    pub(super) fn parse_solution_modifiers(&mut self) -> SolutionModifiers {
        let mut modifiers = SolutionModifiers::new();

        // GROUP BY
        if self.stream.check_keyword(TokenKind::KwGroupBy) {
            if let Some(group_by) = self.parse_group_by() {
                modifiers = modifiers.with_group_by(group_by);
            }
        }

        // HAVING
        if self.stream.check_keyword(TokenKind::KwHaving) {
            if let Some(having) = self.parse_having() {
                modifiers = modifiers.with_having(having);
            }
        }

        // ORDER BY
        if self.stream.check_keyword(TokenKind::KwOrderBy) {
            if let Some(order_by) = self.parse_order_by() {
                modifiers = modifiers.with_order_by(order_by);
            }
        }

        // LIMIT / OFFSET (either order; BSBM uses OFFSET then LIMIT)
        //
        // SPARQL 1.1 grammar allows:
        //   LimitOffsetClauses ::= LimitClause? OffsetClause? | OffsetClause? LimitClause?
        //
        // Parse both in a loop so we accept either ordering and treat later clauses
        // as overriding earlier ones (consistent with many engines' error-tolerant parsing).
        loop {
            if self.stream.check_keyword(TokenKind::KwLimit) {
                if let Some(limit) = self.parse_limit() {
                    modifiers = modifiers.with_limit(limit);
                }
                continue;
            }
            if self.stream.check_keyword(TokenKind::KwOffset) {
                if let Some(offset) = self.parse_offset() {
                    modifiers = modifiers.with_offset(offset);
                }
                continue;
            }
            break;
        }

        modifiers
    }

    /// Parse GROUP BY clause.
    ///
    /// GroupClause ::= 'GROUP' 'BY' GroupCondition+
    /// GroupCondition ::= BuiltInCall | FunctionCall | '(' Expression ( 'AS' Var )? ')' | Var
    pub(super) fn parse_group_by(&mut self) -> Option<GroupByClause> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume GROUP

        // Consume BY (lexer returns GROUP as KwGroupBy and BY as KwBy)
        if !self.stream.match_keyword(TokenKind::KwBy) {
            self.stream.error_at_current("expected 'BY' after 'GROUP'");
            return None;
        }

        let mut conditions = Vec::new();

        // Parse at least one condition
        while let Some(cond) = self.parse_group_condition() {
            conditions.push(cond);
        }

        if conditions.is_empty() {
            self.stream
                .error_at_current("expected at least one GROUP BY condition");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(GroupByClause { conditions, span })
    }

    /// Parse a single GROUP BY condition.
    ///
    /// For MVP, we only support simple variable references.
    /// Expression forms like (expr AS ?var) will be rejected during lowering.
    fn parse_group_condition(&mut self) -> Option<GroupCondition> {
        // Check for a bare variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(GroupCondition::Var(Var::new(name.as_ref(), span)));
        }

        // Check for parenthesized expression: ( Expression ( AS Var )? )
        if self.stream.check(&TokenKind::LParen) {
            let start = self.stream.current_span();
            self.stream.advance(); // consume (

            match parse_expression(self.stream) {
                Ok(expr) => {
                    // Check for optional AS alias
                    let alias = if self.stream.match_keyword(TokenKind::KwAs) {
                        if let Some((name, span)) = self.stream.consume_var() {
                            Some(Var::new(name.as_ref(), span))
                        } else {
                            self.stream.error_at_current("expected variable after AS");
                            return None;
                        }
                    } else {
                        None
                    };

                    if !self.stream.match_token(&TokenKind::RParen) {
                        self.stream.error_at_current("expected ')'");
                        return None;
                    }

                    let span = start.union(self.stream.previous_span());
                    return Some(GroupCondition::Expr { expr, alias, span });
                }
                Err(msg) => {
                    self.stream.error_at_current(&msg);
                    return None;
                }
            }
        }

        // Not a valid group condition
        None
    }

    /// Parse HAVING clause.
    ///
    /// HavingClause ::= 'HAVING' Constraint+
    /// Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
    fn parse_having(&mut self) -> Option<HavingClause> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume HAVING

        let mut conditions: Vec<Expression> = Vec::new();

        // Parse at least one constraint
        loop {
            // HAVING constraints are typically parenthesized expressions or function calls
            // For now, we expect parenthesized expressions
            if self.stream.check(&TokenKind::LParen) {
                self.stream.advance(); // consume (
                match parse_expression(self.stream) {
                    Ok(expr) => {
                        if !self.stream.match_token(&TokenKind::RParen) {
                            self.stream.error_at_current("expected ')'");
                            return None;
                        }
                        conditions.push(expr);
                    }
                    Err(msg) => {
                        self.stream.error_at_current(&msg);
                        return None;
                    }
                }
            } else {
                // Not a parenthesized expression, stop parsing HAVING conditions
                break;
            }
        }

        if conditions.is_empty() {
            self.stream
                .error_at_current("expected at least one HAVING constraint");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(HavingClause { conditions, span })
    }

    /// Parse ORDER BY clause.
    fn parse_order_by(&mut self) -> Option<OrderByClause> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume ORDER

        // Consume BY (lexer returns ORDER as KwOrderBy and BY as KwBy)
        if !self.stream.match_keyword(TokenKind::KwBy) {
            self.stream.error_at_current("expected 'BY' after 'ORDER'");
            return None;
        }

        let mut conditions = Vec::new();

        while let Some(cond) = self.parse_order_condition() {
            conditions.push(cond);
        }

        if conditions.is_empty() {
            self.stream.error_at_current("expected ordering condition");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(OrderByClause { conditions, span })
    }

    /// Parse a single ORDER BY condition.
    fn parse_order_condition(&mut self) -> Option<OrderCondition> {
        let start = self.stream.current_span();

        // Check for ASC/DESC
        let direction = if self.stream.match_keyword(TokenKind::KwAsc) {
            OrderDirection::Asc
        } else if self.stream.match_keyword(TokenKind::KwDesc) {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };

        // After ASC/DESC, we need an expression (which may be parenthesized or a bare variable)
        // If there was an explicit ASC/DESC, expect parentheses
        let has_direction = self.stream.peek_n(0).kind != TokenKind::KwAsc
            && self.stream.previous_span().start != start.start;

        let expr = if has_direction && self.stream.check(&TokenKind::LParen) {
            // ASC/DESC followed by parenthesized expression
            self.stream.advance(); // consume (
            match parse_expression(self.stream) {
                Ok(e) => {
                    if !self.stream.match_token(&TokenKind::RParen) {
                        self.stream.error_at_current("expected ')'");
                    }
                    OrderExpr::Expr(e)
                }
                Err(msg) => {
                    self.stream.error_at_current(&msg);
                    return None;
                }
            }
        } else if let Some((name, span)) = self.stream.consume_var() {
            // Bare variable
            OrderExpr::Var(Var::new(name.as_ref(), span))
        } else if self.stream.check(&TokenKind::LParen) {
            // Parenthesized expression without ASC/DESC
            self.stream.advance(); // consume (
            match parse_expression(self.stream) {
                Ok(e) => {
                    if !self.stream.match_token(&TokenKind::RParen) {
                        self.stream.error_at_current("expected ')'");
                    }
                    OrderExpr::Expr(e)
                }
                Err(msg) => {
                    self.stream.error_at_current(&msg);
                    return None;
                }
            }
        } else {
            // Not a valid order condition
            return None;
        };

        let span = start.union(self.stream.previous_span());
        Some(OrderCondition {
            expr,
            direction,
            span,
        })
    }

    /// Parse LIMIT clause.
    pub(super) fn parse_limit(&mut self) -> Option<LimitClause> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume LIMIT

        if let Some((n, int_span)) = self.stream.consume_integer() {
            if n < 0 {
                self.stream.error_at("LIMIT must be non-negative", int_span);
                return None;
            }
            let span = start.union(int_span);
            Some(LimitClause::new(n as u64, span))
        } else {
            self.stream.error_at_current("expected integer after LIMIT");
            None
        }
    }

    /// Parse OFFSET clause.
    pub(super) fn parse_offset(&mut self) -> Option<OffsetClause> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume OFFSET

        if let Some((n, int_span)) = self.stream.consume_integer() {
            if n < 0 {
                self.stream
                    .error_at("OFFSET must be non-negative", int_span);
                return None;
            }
            let span = start.union(int_span);
            Some(OffsetClause::new(n as u64, span))
        } else {
            self.stream
                .error_at_current("expected integer after OFFSET");
            None
        }
    }

    // =========================================================================
    // Skip helpers for features not yet implemented
    // =========================================================================

    pub(super) fn skip_parenthesized_content(&mut self) {
        if !self.stream.match_token(&TokenKind::LParen) {
            return;
        }
        self.stream
            .skip_balanced(&TokenKind::LParen, &TokenKind::RParen);
    }
}
