//! Graph pattern parsing: WHERE, OPTIONAL, UNION, MINUS, FILTER, BIND, VALUES, subqueries.

use crate::ast::pattern::{GraphName, ServiceEndpoint, SubSelect};
use crate::ast::query::SelectVariables;
use crate::ast::{GraphPattern, Term, Var, WhereClause};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;
use crate::span::SourceSpan;

use super::expr::parse_expression;

/// Grammar predicate for `Constraint ::= BrackettedExpression | BuiltInCall
/// | FunctionCall`, mapped onto the expression AST after parsing.
///
/// `Bracketed` is the BrackettedExpression alternative. `FunctionCall`,
/// `If`, `Coalesce`, `Exists`, `NotExists`, and `Aggregate` cover the
/// BuiltInCall alternatives (an aggregate in FILTER is grammatically a
/// BuiltInCall — rejecting it there is a semantic-validation concern, not a
/// parse error) plus extension-function calls. Everything else — bare
/// variables, literals, IRIs, and unparenthesized unary/binary operator
/// expressions — is not a Constraint.
///
/// PR-1's P5b (bare Constraint as an ORDER BY condition) admits the same
/// alternatives; keep the two aligned if either changes.
fn is_constraint_expression(expr: &crate::ast::Expression) -> bool {
    use crate::ast::Expression;
    matches!(
        expr,
        Expression::Bracketed { .. }
            | Expression::FunctionCall { .. }
            | Expression::If { .. }
            | Expression::Coalesce { .. }
            | Expression::Exists { .. }
            | Expression::NotExists { .. }
            | Expression::Aggregate { .. }
    )
}

impl super::Parser<'_> {
    /// Parse a WHERE clause.
    pub(super) fn parse_where_clause(&mut self) -> Option<WhereClause> {
        let start = self.stream.current_span();

        // WHERE keyword is optional
        let has_where = self.stream.match_keyword(TokenKind::KwWhere);

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            if has_where {
                self.stream.error_at_current("expected '{' after WHERE");
            } else {
                self.stream.error_at_current("expected WHERE clause or '{'");
            }
            return None;
        }

        // Parse the group graph pattern
        let pattern = self.parse_group_graph_pattern()?;

        let span = start.union(self.stream.previous_span());

        Some(WhereClause::new(pattern, has_where, span))
    }

    /// Parse a group graph pattern (contents within { }).
    pub(super) fn parse_group_graph_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.previous_span(); // The opening brace

        // SPARQL grammar: GroupGraphPattern ::= '{' ( SubSelect | GroupGraphPatternSub ) '}'
        //
        // The opening '{' has already been consumed by the caller, so a leading
        // SELECT means this brace encloses a *sub-SELECT*, not a basic graph
        // pattern. Detecting it here — rather than only in the nested-'{' branch
        // of the loop below — is what makes a sub-SELECT a legal operand in EVERY
        // position that admits a group: OPTIONAL, GRAPH, SERVICE, the right arm of
        // MINUS, and every arm of UNION (and, via the free `parse_group_graph_pattern`
        // wrapper, EXISTS / NOT EXISTS). Previously those positions consumed the
        // '{' and called straight into the loop, which only recognises a
        // sub-SELECT after itself consuming a nested '{'; the leading SELECT was
        // therefore treated as an unexpected token and the projection was dropped.
        // `parse_subquery` consumes through the matching closing '}'.
        //
        // Do NOT re-add a per-caller `KwSelect` check that bypasses this: any new
        // group-opening construct inherits sub-SELECT support for free by calling
        // this function after consuming its own `{`. A local check at a call site
        // would silently reintroduce the dropped-UNION / dropped-projection bug
        // (#1435 / azure-chat #42, #43).
        if self.stream.check_keyword(TokenKind::KwSelect) {
            return self.parse_subquery(start).or_else(|| {
                // Malformed sub-SELECT: skip to the matching '}' so its tokens
                // don't leak into the enclosing scope. The opening brace was
                // already consumed by the caller, so `skip_balanced` starts at
                // depth 1 (mirrors the recovery the '{' branch performed before).
                self.stream
                    .skip_balanced(&TokenKind::LBrace, &TokenKind::RBrace);
                None
            });
        }

        let mut patterns: Vec<GraphPattern> = Vec::new();
        let mut current_triples: Vec<crate::ast::TriplePattern> = Vec::new();

        // SPARQL grammar:
        //   GroupGraphPatternSub ::= TriplesBlock? ( GraphPatternNotTriples '.'? TriplesBlock? )*
        // A '.' at group level is legal ONLY as the single optional separator
        // immediately after a GraphPatternNotTriples (OPTIONAL, UNION group,
        // MINUS, GRAPH, SERVICE, FILTER, BIND, VALUES). Separator dots inside
        // a TriplesBlock are owned by `parse_triples_block`, which consumes
        // its own single optional trailing dot. Every other dot — leading,
        // doubled, or standalone — is a syntax error (W3C syn-bad-05..14).
        let mut dot_allowed = false;

        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            // Safety: track position to detect sub-parsers that return None
            // without advancing. If we make no progress, force-advance to
            // prevent infinite loops from unhandled token types.
            let loop_start_pos = self.stream.position();

            // Check for graph pattern keywords
            if self.stream.check_keyword(TokenKind::KwOptional) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(optional) = self.parse_optional_pattern() {
                    patterns.push(optional);
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwUnion) {
                // UNION requires a preceding pattern
                self.stream.error_at_current("UNION must follow a pattern");
                self.stream.advance();
            } else if self.stream.check_keyword(TokenKind::KwMinus) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                // MINUS needs a left operand - collect all accumulated patterns
                if patterns.is_empty() {
                    self.stream
                        .error_at_current("MINUS requires a preceding pattern");
                    self.stream.advance();
                    continue;
                }

                // Combine accumulated patterns into a single left operand
                let left = super::collect_patterns_into_one(std::mem::take(&mut patterns), start);

                if let Some(right) = self.parse_minus_right_side() {
                    let span = left.span().union(self.stream.previous_span());
                    patterns.push(GraphPattern::Minus {
                        left: Box::new(left),
                        right: Box::new(right),
                        span,
                    });
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwFilter) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(filter) = self.parse_filter_pattern() {
                    patterns.push(filter);
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwGraph) {
                // GRAPH pattern - GRAPH <iri>|?var { ... }
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(graph) = self.parse_graph_pattern() {
                    patterns.push(graph);
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwService) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(service) = self.parse_service_pattern() {
                    patterns.push(service);
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwBind) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(bind) = self.parse_bind_pattern() {
                    // SPARQL 1.1 §10.1 / grammar note 12 (V5): the variable
                    // assigned by BIND must not already be in scope in this
                    // group graph pattern *up to this point* (`patterns` =
                    // the preceding siblings of this group only — a nested
                    // `{ BIND ... }` group starts a fresh scope, which is
                    // why this check lives HERE and not in `validate()`:
                    // after the single-pattern group simplification below,
                    // `{ ... { BIND(e AS ?v) } }` (legal) and
                    // `{ ... BIND(e AS ?v) }` (illegal) produce identical
                    // ASTs, so only the parser can tell them apart.
                    //
                    // `parse_sparql` refuses to produce an AST when this
                    // diagnostic fires (D-4: reject-more errors must prevent
                    // AST production; recovered-error ASTs would otherwise
                    // execute through the API's diagnostic-swallowing path).
                    if let GraphPattern::Bind { var, span, .. } = &bind {
                        let mut in_scope = Vec::new();
                        for preceding in &patterns {
                            preceding.add_in_scope_variables(&mut in_scope);
                        }
                        if let Some(first) = in_scope.iter().find(|v| v.name == var.name) {
                            self.stream.add_diagnostic(
                                Diagnostic::error(
                                    DiagCode::BindTargetAlreadyInScope,
                                    format!(
                                        "BIND target variable ?{} is already in scope \
                                         in this group",
                                        var.name
                                    ),
                                    *span,
                                )
                                .with_label(crate::diag::Label::new(
                                    first.span,
                                    "already bound here",
                                ))
                                .with_help(
                                    "The variable assigned by BIND(expr AS ?v) must not \
                                     be used earlier in the same group graph pattern \
                                     (SPARQL 1.1 §10.1). Bind to a fresh variable, or \
                                     wrap the BIND in its own { } group.",
                                ),
                            );
                        }
                    }
                    patterns.push(bind);
                }
                dot_allowed = true;
            } else if self.stream.check_keyword(TokenKind::KwValues) {
                super::flush_current_triples(&mut current_triples, &mut patterns);

                if let Some(values) = self.parse_values_pattern() {
                    patterns.push(values);
                }
                dot_allowed = true;
            } else if self.stream.check(&TokenKind::LBrace) {
                // Nested group or sub-SELECT.
                super::flush_current_triples(&mut current_triples, &mut patterns);

                self.stream.advance(); // consume {

                // `parse_group_graph_pattern` handles BOTH a basic group and a
                // `{ SELECT ... }` sub-SELECT (see the SubSelect check at its
                // top). Either is a valid left operand of a UNION, so the
                // trailing-UNION check below must run for both. Previously the
                // sub-SELECT case pushed the subquery and skipped this check, so
                // `{ SELECT ... } UNION { ... }` silently dropped the UNION (the
                // `UNION` token then hit "UNION must follow a pattern" and was
                // discarded, leaving two independent patterns).
                if let Some(inner) = self.parse_group_graph_pattern() {
                    if self.stream.check_keyword(TokenKind::KwUnion) {
                        if let Some(union) = self.parse_union_continuation(inner) {
                            patterns.push(union);
                        }
                    } else {
                        patterns.push(inner);
                    }
                }
                dot_allowed = true;
            } else if self.stream.is_term_start() {
                // Parse triple patterns (may include path patterns)
                if let Some(block_patterns) = self.parse_triples_block() {
                    for pattern in block_patterns {
                        match pattern {
                            GraphPattern::Bgp {
                                patterns: bgp_triples,
                                ..
                            } => {
                                // Merge BGP triples into current accumulator
                                current_triples.extend(bgp_triples);
                            }
                            other => {
                                // Path or other pattern - flush current triples first
                                super::flush_current_triples(&mut current_triples, &mut patterns);
                                patterns.push(other);
                            }
                        }
                    }
                }
                // `parse_triples_block` already consumed the TriplesBlock's own
                // optional trailing dot; another dot here would be doubled.
                dot_allowed = false;
            } else if self.stream.check(&TokenKind::Dot) {
                if dot_allowed {
                    // The single optional '.' after a GraphPatternNotTriples.
                    self.stream.advance();
                    dot_allowed = false;
                } else {
                    // Leading, doubled, or standalone dot: forbidden by
                    // GroupGraphPatternSub (V1 dot-structure validation).
                    self.stream.error_at_current(
                        "unexpected '.': a dot may only follow a triple pattern or a \
                         graph pattern (OPTIONAL, FILTER, GRAPH, BIND, VALUES, ...)",
                    );
                    self.stream.advance();
                }
            } else {
                // Unknown token
                self.stream
                    .error_at_current("unexpected token in graph pattern");
                self.stream.advance();
            }

            // Safety net: if no branch consumed any tokens, force-advance to prevent
            // infinite loops. This catches cases where a sub-parser returns None
            // without advancing (e.g., an unhandled token type in parse_subject).
            if self.stream.position() == loop_start_pos {
                self.stream
                    .error_at_current("parser failed to make progress — skipping token");
                self.stream.advance();
            }
        }

        // Flush any remaining triples
        super::flush_current_triples(&mut current_triples, &mut patterns);

        // Expect closing brace
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream.error_at_current("expected '}'");
            return None;
        }

        let span = start.union(self.stream.previous_span());

        // Simplify: if there's only one pattern, return it directly.
        //
        // INVARIANT (consumed by lower/pattern.rs): nested Group nodes in the AST
        // always correspond to explicitly braced `{ }` blocks from the source
        // query — never synthetic wrappers.  This simplification ensures that: a
        // single-pattern `{ }` block produces the pattern itself (not a Group),
        // and only multi-pattern blocks produce Group nodes.  The lowering layer
        // relies on this to decide when to introduce scope-boundary subqueries.
        if patterns.len() == 1 {
            Some(patterns.remove(0))
        } else {
            Some(GraphPattern::group(patterns, span))
        }
    }

    /// Parse an OPTIONAL pattern.
    pub(super) fn parse_optional_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume OPTIONAL

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream.error_at_current("expected '{' after OPTIONAL");
            return None;
        }

        let pattern = self.parse_group_graph_pattern()?;
        let span = start.union(self.stream.previous_span());

        Some(GraphPattern::Optional {
            pattern: Box::new(pattern),
            span,
        })
    }

    /// Parse a GRAPH pattern - `GRAPH <iri>|?var { ... }`
    pub(super) fn parse_graph_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume GRAPH

        // Parse the graph name (IRI or variable)
        let name = if let Some((var_name, var_span)) = self.stream.consume_var() {
            GraphName::Var(Var::new(var_name.as_ref(), var_span))
        } else if let Some(iri) = self.parse_iri_term() {
            GraphName::Iri(iri)
        } else {
            self.stream
                .error_at_current("expected IRI or variable after GRAPH");
            return None;
        };

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' after GRAPH name");
            return None;
        }

        // Parse the inner group graph pattern
        let inner = self.parse_group_graph_pattern()?;
        let span = start.union(self.stream.previous_span());

        Some(GraphPattern::Graph {
            name,
            pattern: Box::new(inner),
            span,
        })
    }

    /// Parse a SERVICE pattern: `SERVICE [SILENT] <iri>|?var { ... }`
    pub(super) fn parse_service_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume SERVICE

        let silent = self.stream.match_keyword(TokenKind::KwSilent);

        let endpoint = if let Some((var_name, var_span)) = self.stream.consume_var() {
            ServiceEndpoint::Var(Var::new(var_name.as_ref(), var_span))
        } else if let Some(iri) = self.parse_iri_term() {
            ServiceEndpoint::Iri(iri)
        } else {
            self.stream
                .error_at_current("expected IRI or variable after SERVICE");
            return None;
        };

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' after SERVICE endpoint");
            return None;
        }

        let inner = self.parse_group_graph_pattern()?;
        let span = start.union(self.stream.previous_span());

        Some(GraphPattern::Service {
            silent,
            endpoint,
            pattern: Box::new(inner),
            span,
        })
    }

    /// Parse the right side of a MINUS pattern (just the `MINUS { ... }` part).
    ///
    /// The left operand is handled by the caller.
    pub(super) fn parse_minus_right_side(&mut self) -> Option<GraphPattern> {
        self.stream.advance(); // consume MINUS

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream.error_at_current("expected '{' after MINUS");
            return None;
        }

        self.parse_group_graph_pattern()
    }

    /// Parse UNION continuations after a group.
    pub(super) fn parse_union_continuation(&mut self, left: GraphPattern) -> Option<GraphPattern> {
        let start = left.span();

        self.stream.advance(); // consume UNION

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream.error_at_current("expected '{' after UNION");
            return None;
        }

        let right = self.parse_group_graph_pattern()?;
        let span = start.union(self.stream.previous_span());

        let mut result = GraphPattern::Union {
            left: Box::new(left),
            right: Box::new(right),
            span,
        };

        // Handle chained UNIONs
        while self.stream.check_keyword(TokenKind::KwUnion) {
            self.stream.advance(); // consume UNION

            if !self.stream.match_token(&TokenKind::LBrace) {
                self.stream.error_at_current("expected '{' after UNION");
                break;
            }

            let right = self.parse_group_graph_pattern()?;
            let new_span = result.span().union(self.stream.previous_span());

            result = GraphPattern::Union {
                left: Box::new(result),
                right: Box::new(right),
                span: new_span,
            };
        }

        Some(result)
    }

    /// Parse a FILTER pattern.
    ///
    /// Syntax: `FILTER (expression)` or `FILTER expression`
    /// Note: EXISTS and NOT EXISTS are parsed as part of the expression.
    pub(super) fn parse_filter_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume FILTER

        // Parse the filter expression
        match parse_expression(self.stream) {
            Ok(expr) => {
                // Filter ::= 'FILTER' Constraint
                // Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
                // A bare variable, literal, IRI, or unparenthesized operator
                // expression (`FILTER ?x`, `FILTER ?x > 5`) is not a
                // Constraint (V2, W3C filter-missing-parens). The Filter
                // pattern is still produced for tooling; the error-severity
                // diagnostic makes the parse authoritative-fail at the API.
                if !is_constraint_expression(&expr) {
                    self.stream.error_at(
                        "FILTER requires a bracketted expression, built-in call, or \
                         function call — wrap the expression in parentheses: FILTER(...)",
                        expr.span(),
                    );
                }
                let span = start.union(self.stream.previous_span());
                Some(GraphPattern::Filter { expr, span })
            }
            Err(msg) => {
                self.stream.error_at_current(&msg);
                None
            }
        }
    }

    /// Parse a BIND pattern.
    ///
    /// Syntax: `BIND (expression AS ?var)`
    pub(super) fn parse_bind_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume BIND

        if !self.stream.match_token(&TokenKind::LParen) {
            self.stream.error_at_current("expected '(' after BIND");
            return None;
        }

        // Parse the expression
        let expr = match parse_expression(self.stream) {
            Ok(e) => e,
            Err(msg) => {
                self.stream.error_at_current(&msg);
                return None;
            }
        };

        // Expect AS
        if !self.stream.check_keyword(TokenKind::KwAs) {
            let span = start.union(self.stream.previous_span());
            self.stream.add_diagnostic(
                Diagnostic::new(
                    DiagCode::ExpectedToken,
                    "BIND requires 'AS ?variable'",
                    span,
                )
                .with_help("Use BIND(expression AS ?variable) syntax"),
            );
            return None;
        }
        self.stream.advance(); // consume AS

        // Parse the variable
        let var = if let Some((name, var_span)) = self.stream.consume_var() {
            Var::new(name.as_ref(), var_span)
        } else {
            self.stream.error_at_current("expected variable after AS");
            return None;
        };

        // Expect closing paren
        if !self.stream.match_token(&TokenKind::RParen) {
            self.stream
                .error_at_current("expected ')' after BIND expression");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(GraphPattern::Bind { expr, var, span })
    }

    /// Parse a VALUES clause.
    ///
    /// Syntax:
    /// - Single variable: `VALUES ?x { value1 value2 ... }`
    /// - Multiple variables: `VALUES (?x ?y) { (val1 val2) (val3 val4) ... }`
    pub(super) fn parse_values_pattern(&mut self) -> Option<GraphPattern> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume VALUES

        // Row shape follows the VAR-LIST shape, not the variable count:
        //   InlineDataOneVar ::= Var '{' DataBlockValue* '}'
        //   InlineDataFull   ::= ( NIL | '(' Var* ')' ) '{' ( '(' DataBlockValue* ')' | NIL )* '}'
        // A parenthesized single-var list (`VALUES (?x) { (:b) }`) therefore
        // takes parenthesized rows (W3C bindings#values7), and a bare var
        // never does.
        let parenthesized_vars =
            self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil);

        // Parse variable list
        let vars = self.parse_values_variables()?;
        let multi_var = parenthesized_vars;

        // Expect opening brace for data block
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' after VALUES variables");
            return None;
        }

        // Parse data rows
        let mut data: Vec<Vec<Option<Term>>> = Vec::new();

        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            if vars.is_empty() {
                // Zero-variable data block (`VALUES () { () … }`): each row
                // is `()` — lexed as a single Nil token — and carries no
                // bindings (`InlineDataFull ::= ( NIL | … ) '{' ( … | NIL )* '}'`).
                if self.stream.match_token(&TokenKind::Nil) {
                    data.push(Vec::new());
                } else {
                    self.stream
                        .error_at_current("expected '()' row in zero-variable VALUES data block");
                    self.stream.advance();
                }
            } else if multi_var {
                // Multiple variables: expect parenthesized row
                if let Some(row) = self.parse_values_row(vars.len()) {
                    data.push(row);
                } else {
                    // Error recovery: skip to next row or end
                    self.skip_to_next_values_row();
                }
            } else {
                // Single variable: parse single value
                if let Some(value) = self.parse_values_term() {
                    data.push(vec![value]);
                } else if self.stream.check(&TokenKind::RBrace) {
                    break;
                } else {
                    self.stream
                        .error_at_current("expected value in VALUES clause");
                    self.stream.advance();
                }
            }
        }

        // Expect closing brace
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' to close VALUES data block");
        }

        let span = start.union(self.stream.previous_span());

        Some(GraphPattern::Values { vars, data, span })
    }

    /// Parse the variable list in a VALUES clause.
    ///
    /// Returns variables either from `?var` or `(?var1 ?var2 ...)`.
    fn parse_values_variables(&mut self) -> Option<Vec<Var>> {
        let mut vars = Vec::new();

        // `VALUES () { … }` — a NIL var list (`InlineDataFull ::= ( NIL |
        // '(' Var* ')' ) …`) declares zero variables. `()` lexes as one Nil
        // token, so the LParen branch below never sees the empty case.
        if self.stream.match_token(&TokenKind::Nil) {
            return Some(vars);
        }

        // Check for parenthesized list
        if self.stream.match_token(&TokenKind::LParen) {
            // Multiple variables
            while !self.stream.check(&TokenKind::RParen) && !self.stream.is_eof() {
                if let Some((name, span)) = self.stream.consume_var() {
                    vars.push(Var::new(name.as_ref(), span));
                } else {
                    self.stream
                        .error_at_current("expected variable in VALUES variable list");
                    break;
                }
            }

            if !self.stream.match_token(&TokenKind::RParen) {
                self.stream
                    .error_at_current("expected ')' after VALUES variable list");
            }

            if vars.is_empty() {
                self.stream
                    .error_at_current("VALUES requires at least one variable");
                return None;
            }
        } else if let Some((name, span)) = self.stream.consume_var() {
            // Single variable
            vars.push(Var::new(name.as_ref(), span));
        } else {
            self.stream
                .error_at_current("expected variable or '(' after VALUES");
            return None;
        }

        Some(vars)
    }

    /// Parse a row of values in a multi-variable VALUES clause.
    ///
    /// Expects `(val1 val2 ...)` where each value is a term or UNDEF.
    fn parse_values_row(&mut self, expected_count: usize) -> Option<Vec<Option<Term>>> {
        if !self.stream.match_token(&TokenKind::LParen) {
            self.stream
                .error_at_current("expected '(' to start VALUES row");
            return None;
        }

        let mut row = Vec::with_capacity(expected_count);

        while !self.stream.check(&TokenKind::RParen) && !self.stream.is_eof() {
            if let Some(value) = self.parse_values_term() {
                row.push(value);
            } else {
                self.stream
                    .error_at_current("expected value or UNDEF in VALUES row");
                break;
            }
        }

        if !self.stream.match_token(&TokenKind::RParen) {
            self.stream
                .error_at_current("expected ')' to close VALUES row");
        }

        // Check row has correct number of values
        if row.len() != expected_count {
            self.stream.add_diagnostic(Diagnostic::new(
                DiagCode::ExpectedToken,
                format!(
                    "VALUES row has {} values but {} variables declared",
                    row.len(),
                    expected_count
                ),
                self.stream.previous_span(),
            ));
        }

        Some(row)
    }

    /// Parse a single term in a VALUES clause.
    ///
    /// Returns `Some(Some(term))` for a value, `Some(None)` for UNDEF.
    fn parse_values_term(&mut self) -> Option<Option<Term>> {
        // Check for UNDEF
        if self.stream.match_keyword(TokenKind::KwUndef) {
            return Some(None);
        }

        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(Some(Term::Iri(iri)));
        }

        // Literal
        if let Some(lit) = self.parse_literal() {
            return Some(Some(Term::Literal(lit)));
        }

        None
    }

    /// Skip to the next VALUES row (for error recovery).
    fn skip_to_next_values_row(&mut self) {
        // Skip until we find ( or }
        while !self.stream.is_eof() {
            if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::RBrace) {
                break;
            }
            self.stream.advance();
        }
    }

    /// Parse a subquery: `{ SELECT ... WHERE { ... } }`.
    ///
    /// The opening `{` has already been consumed. This function parses
    /// the SELECT clause, WHERE clause, solution modifiers, and closing `}`.
    pub(super) fn parse_subquery(&mut self, start: SourceSpan) -> Option<GraphPattern> {
        // Parse SELECT keyword
        self.stream.advance(); // consume SELECT

        // Parse DISTINCT/REDUCED
        let distinct = self.stream.match_keyword(TokenKind::KwDistinct);
        let reduced = if !distinct {
            self.stream.match_keyword(TokenKind::KwReduced)
        } else {
            false
        };

        // Parse variable list (SELECT * or SELECT ?var1 ?var2 ...)
        // Reuses the top-level SELECT parser which handles both ?var and (expr AS ?var).
        let variables = if self.stream.match_token(&TokenKind::Star) {
            SelectVariables::Star
        } else {
            let vars = self.parse_select_variables()?;
            if vars.is_empty() {
                self.stream
                    .error_at_current("expected variable or '*' after SELECT");
                return None;
            }
            SelectVariables::Explicit(vars)
        };

        // Optional WHERE keyword (can be omitted in subqueries)
        self.stream.match_keyword(TokenKind::KwWhere);

        // Parse WHERE clause pattern
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' for subquery WHERE clause");
            return None;
        }

        let pattern = self.parse_group_graph_pattern()?;

        // Parse solution modifiers (GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
        // with the same machinery as a top-level SELECT, so subqueries inherit
        // HAVING and expression/aggregate ORDER BY. `parse_solution_modifiers`
        // stops at the first non-modifier token (here, the closing `}`).
        let modifiers = self.parse_solution_modifiers();

        // Optional trailing VALUES clause:
        // `SubSelect ::= SelectClause WhereClause SolutionModifier ValuesClause`
        // (same position as a top-level query's post-query VALUES;
        // W3C bindings#inline2).
        let values = if self.stream.check_keyword(TokenKind::KwValues) {
            self.parse_values_pattern().map(Box::new)
        } else {
            None
        };

        // Expect closing brace for the subquery
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' to close subquery");
        }

        let span = start.union(self.stream.previous_span());

        let subquery = SubSelect {
            distinct,
            reduced,
            variables,
            pattern: Box::new(pattern),
            modifiers,
            values,
            span,
        };

        Some(GraphPattern::SubSelect {
            query: Box::new(subquery),
            span,
        })
    }
}
