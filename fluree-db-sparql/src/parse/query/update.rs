//! SPARQL Update parsing: INSERT, DELETE, and related operations.

use crate::ast::pattern::GraphName;
use crate::ast::update::{
    DeleteData, DeleteWhere, InsertData, Modify, QuadData, QuadPattern, QuadPatternElement,
    UpdateOperation, UsingClause,
};
use crate::ast::{GraphPattern, Iri};
use crate::lex::TokenKind;
use crate::span::SourceSpan;

/// Collect the labeled blank nodes appearing in an update operation's
/// blank-node-MINTING template positions: INSERT DATA quad data and Modify
/// INSERT templates.
///
/// Used by the request-level parser to enforce SPARQL 1.1 Update §19.6's
/// blank-node label scoping: a label may not be reused across operations of
/// one request. WHERE-clause and DELETE-side blank nodes are excluded — in
/// WHERE clauses (including DELETE WHERE, per Fluree's documented
/// extension) they act as non-distinguished matching variables governed by
/// the query-side BGP scoping rules, and strict-mode DELETE templates/data
/// reject blank nodes outright downstream (validator + lowering) — in
/// neither case is there a minted node the label-scope rule could protect,
/// and walking them over-rejected `DELETE WHERE { _:b1 … } ; DELETE WHERE
/// { _:b1 … }`, whose two labels are independently-scoped matches.
///
/// Parser-synthesized labels (`#bnpl<N>` from blank-node property lists) are
/// minted from a counter that is monotonic across the whole request, so they
/// can never collide across operations; they flow through here harmlessly.
pub(super) fn collect_template_bnode_labels(
    op: &UpdateOperation,
    out: &mut Vec<(std::sync::Arc<str>, SourceSpan)>,
) {
    fn from_triple(
        tp: &crate::ast::TriplePattern,
        out: &mut Vec<(std::sync::Arc<str>, SourceSpan)>,
    ) {
        use crate::ast::{BlankNodeValue, SubjectTerm, Term};

        fn from_subject(s: &SubjectTerm, out: &mut Vec<(std::sync::Arc<str>, SourceSpan)>) {
            match s {
                SubjectTerm::BlankNode(bn) => {
                    if let BlankNodeValue::Labeled(label) = &bn.value {
                        out.push((label.clone(), bn.span));
                    }
                }
                SubjectTerm::QuotedTriple(qt) => {
                    from_subject(&qt.subject, out);
                    if let Term::BlankNode(bn) = qt.object.as_ref() {
                        if let BlankNodeValue::Labeled(label) = &bn.value {
                            out.push((label.clone(), bn.span));
                        }
                    }
                }
                SubjectTerm::Var(_) | SubjectTerm::Iri(_) => {}
            }
        }

        from_subject(&tp.subject, out);
        if let Term::BlankNode(bn) = &tp.object {
            if let BlankNodeValue::Labeled(label) = &bn.value {
                out.push((label.clone(), bn.span));
            }
        }
        if let Some(annotation) = &tp.annotation {
            for unit in &annotation.units {
                if let Some(crate::ast::annotation::ReifierId::BlankNode(bn)) = &unit.reifier {
                    if let BlankNodeValue::Labeled(label) = &bn.value {
                        out.push((label.clone(), bn.span));
                    }
                }
                if let Some(block) = &unit.block {
                    for entry in &block.entries {
                        if let Term::BlankNode(bn) = &entry.object {
                            if let BlankNodeValue::Labeled(label) = &bn.value {
                                out.push((label.clone(), bn.span));
                            }
                        }
                    }
                }
            }
        }
    }

    fn from_elements(
        elements: &[QuadPatternElement],
        out: &mut Vec<(std::sync::Arc<str>, SourceSpan)>,
    ) {
        for el in elements {
            match el {
                QuadPatternElement::Triple(tp) => from_triple(tp, out),
                QuadPatternElement::Graph { triples, .. } => {
                    for tp in triples {
                        from_triple(tp, out);
                    }
                }
            }
        }
    }

    match op {
        UpdateOperation::InsertData(insert) => from_elements(&insert.data.quads, out),
        UpdateOperation::Modify(modify) => {
            if let Some(insert_clause) = &modify.insert_clause {
                from_elements(&insert_clause.patterns, out);
            }
        }
        // DELETE-side blank nodes never mint nodes (see the doc comment),
        // so they carry no cross-operation label identity to protect.
        UpdateOperation::DeleteData(_) | UpdateOperation::DeleteWhere(_) => {}
    }
}

impl super::Parser<'_> {
    /// Parse a SPARQL Update operation.
    ///
    /// Grammar:
    /// - INSERT DATA QuadData
    /// - DELETE DATA QuadData
    /// - DELETE WHERE QuadPattern
    /// - [WITH iri] DELETE QuadPattern [INSERT QuadPattern] [UsingClause*] WHERE GroupGraphPattern
    /// - [WITH iri] INSERT QuadPattern [UsingClause*] WHERE GroupGraphPattern
    pub(super) fn parse_update_operation(&mut self) -> Option<UpdateOperation> {
        let start = self.stream.current_span();

        // Check for WITH clause first
        let with_iri = if self.stream.check_keyword(TokenKind::KwWith) {
            self.stream.advance(); // consume WITH
            let iri = self.parse_iri_term()?;
            Some(iri)
        } else {
            None
        };

        // Dispatch based on next keyword
        if self.stream.check_keyword(TokenKind::KwInsert) {
            self.stream.advance(); // consume INSERT

            // Check for INSERT DATA
            if self.stream.check_keyword(TokenKind::KwData) {
                if with_iri.is_some() {
                    self.stream
                        .error_at_current("WITH clause not allowed with INSERT DATA");
                    return None;
                }
                return self.parse_insert_data(start);
            }

            // INSERT { ... } [USING ...] WHERE { ... }
            return self.parse_modify_insert_only(start, with_iri);
        }

        if self.stream.check_keyword(TokenKind::KwDelete) {
            self.stream.advance(); // consume DELETE

            // Check for DELETE DATA
            if self.stream.check_keyword(TokenKind::KwData) {
                if with_iri.is_some() {
                    self.stream
                        .error_at_current("WITH clause not allowed with DELETE DATA");
                    return None;
                }
                return self.parse_delete_data(start);
            }

            // Check for DELETE WHERE
            if self.stream.check_keyword(TokenKind::KwWhere) {
                if with_iri.is_some() {
                    self.stream
                        .error_at_current("WITH clause not allowed with DELETE WHERE");
                    return None;
                }
                return self.parse_delete_where(start);
            }

            // DELETE { ... } [INSERT { ... }] [USING ...] WHERE { ... }
            return self.parse_modify_delete(start, with_iri);
        }

        self.stream
            .error_at_current("expected INSERT or DELETE after WITH");
        None
    }

    /// Parse INSERT DATA { ... }
    fn parse_insert_data(&mut self, start: SourceSpan) -> Option<UpdateOperation> {
        // Consume DATA keyword
        if !self.stream.match_keyword(TokenKind::KwData) {
            self.stream.error_at_current("expected DATA after INSERT");
            return None;
        }

        // Parse quad data
        let data = self.parse_quad_data()?;

        let span = start.union(self.stream.previous_span());
        Some(UpdateOperation::InsertData(InsertData::new(data, span)))
    }

    /// Parse DELETE DATA { ... }
    fn parse_delete_data(&mut self, start: SourceSpan) -> Option<UpdateOperation> {
        // Consume DATA keyword
        if !self.stream.match_keyword(TokenKind::KwData) {
            self.stream.error_at_current("expected DATA after DELETE");
            return None;
        }

        // Parse quad data
        let data = self.parse_quad_data()?;

        let span = start.union(self.stream.previous_span());
        Some(UpdateOperation::DeleteData(DeleteData::new(data, span)))
    }

    /// Parse DELETE WHERE { ... }
    fn parse_delete_where(&mut self, start: SourceSpan) -> Option<UpdateOperation> {
        // Consume WHERE keyword
        if !self.stream.match_keyword(TokenKind::KwWhere) {
            self.stream.error_at_current("expected WHERE after DELETE");
            return None;
        }

        // Parse quad pattern
        let pattern = self.parse_quad_pattern()?;

        let span = start.union(self.stream.previous_span());
        Some(UpdateOperation::DeleteWhere(DeleteWhere::new(
            pattern, span,
        )))
    }

    /// Parse INSERT { ... } [USING ...] WHERE { ... }
    fn parse_modify_insert_only(
        &mut self,
        start: SourceSpan,
        with_iri: Option<Iri>,
    ) -> Option<UpdateOperation> {
        // Parse INSERT clause
        let insert_clause = self.parse_quad_pattern()?;

        // Parse optional USING clause
        let using = self.parse_using_clause();

        // Parse WHERE clause
        let where_pattern = self.parse_update_where_clause()?;

        let span = start.union(self.stream.previous_span());
        let mut modify = Modify::new(None, Some(insert_clause), where_pattern, span);
        if let Some(iri) = with_iri {
            modify = modify.with_graph(iri);
        }
        if let Some(u) = using {
            modify = modify.with_using(u);
        }

        Some(UpdateOperation::Modify(Box::new(modify)))
    }

    /// Parse DELETE { ... } [INSERT { ... }] [USING ...] WHERE { ... }
    fn parse_modify_delete(
        &mut self,
        start: SourceSpan,
        with_iri: Option<Iri>,
    ) -> Option<UpdateOperation> {
        // Parse DELETE clause
        let delete_clause = self.parse_quad_pattern()?;

        // Parse optional INSERT clause
        let insert_clause = if self.stream.check_keyword(TokenKind::KwInsert) {
            self.stream.advance(); // consume INSERT
            Some(self.parse_quad_pattern()?)
        } else {
            None
        };

        // Parse optional USING clause
        let using = self.parse_using_clause();

        // Parse WHERE clause
        let where_pattern = self.parse_update_where_clause()?;

        let span = start.union(self.stream.previous_span());
        let mut modify = Modify::new(Some(delete_clause), insert_clause, where_pattern, span);
        if let Some(iri) = with_iri {
            modify = modify.with_graph(iri);
        }
        if let Some(u) = using {
            modify = modify.with_using(u);
        }

        Some(UpdateOperation::Modify(Box::new(modify)))
    }

    /// Parse quad data (ground quads for INSERT DATA / DELETE DATA).
    ///
    /// Per SPARQL 1.1 Update §3.1.1, `QuadData` accepts both default-graph
    /// triples and `GRAPH VarOrIri '{' TriplesTemplate? '}'` blocks
    /// (`QuadsNotTriples`). Variables — including a variable graph name — are
    /// syntactically permitted here but rejected by the validator, since DATA
    /// must be ground.
    fn parse_quad_data(&mut self) -> Option<QuadData> {
        let start = self.stream.current_span();

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream.error_at_current("expected '{' for quad data");
            return None;
        }

        // Parse quad data elements:
        // - default-graph triples
        // - GRAPH <iri>|?g { ... } blocks (QuadsNotTriples)
        let mut quads: Vec<QuadPatternElement> = Vec::new();
        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            if self.stream.check_keyword(TokenKind::KwGraph) {
                let graph = self.parse_quad_pattern_graph_block()?;
                quads.push(graph);
                // Optional dot after GRAPH block
                self.stream.match_token(&TokenKind::Dot);
                continue;
            }

            // Default-graph triples
            let subject = match self.parse_subject() {
                Some(s) => s,
                None => {
                    if self.stream.check(&TokenKind::RBrace) {
                        break;
                    }
                    self.stream
                        .error_at_current("expected subject in quad data");
                    return None;
                }
            };

            // Parse predicate-object list (simple predicates only, no paths)
            let mut triples: Vec<crate::ast::TriplePattern> = Vec::new();
            self.parse_template_triples_for_subject(&subject, &mut triples)?;
            for t in triples {
                quads.push(QuadPatternElement::Triple(Box::new(t)));
            }

            // Optional dot
            self.stream.match_token(&TokenKind::Dot);
        }

        // Expect closing brace
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream.error_at_current("expected '}' after quad data");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(QuadData::new(quads, span))
    }

    /// Parse quad pattern (for DELETE/INSERT templates).
    fn parse_quad_pattern(&mut self) -> Option<QuadPattern> {
        let start = self.stream.current_span();

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' for quad pattern");
            return None;
        }

        // Parse quad pattern elements:
        // - default-graph triples
        // - GRAPH <iri>|?g { ... } blocks (templates)
        let mut patterns: Vec<QuadPatternElement> = Vec::new();
        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            if self.stream.check_keyword(TokenKind::KwGraph) {
                let graph = self.parse_quad_pattern_graph_block()?;
                patterns.push(graph);
                // Optional dot after GRAPH block
                self.stream.match_token(&TokenKind::Dot);
                continue;
            }

            // Default-graph triple patterns
            let subject = match self.parse_subject() {
                Some(s) => s,
                None => {
                    if self.stream.check(&TokenKind::RBrace) {
                        break;
                    }
                    self.stream
                        .error_at_current("expected subject in quad pattern");
                    return None;
                }
            };

            let mut triples: Vec<crate::ast::TriplePattern> = Vec::new();
            // Parse predicate-object list (simple predicates only, no paths)
            self.parse_template_triples_for_subject(&subject, &mut triples)?;
            for t in triples {
                patterns.push(QuadPatternElement::Triple(Box::new(t)));
            }

            // Optional dot
            self.stream.match_token(&TokenKind::Dot);
        }

        // Expect closing brace
        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' after quad pattern");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(QuadPattern::new(patterns, span))
    }

    fn parse_quad_pattern_graph_block(&mut self) -> Option<QuadPatternElement> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume GRAPH

        // Parse graph name (IRI or variable) using the same rules as query GRAPH patterns.
        let name = if let Some((var_name, var_span)) = self.stream.consume_var() {
            GraphName::Var(crate::ast::Var::new(var_name.as_ref(), var_span))
        } else if let Some(iri) = self.parse_iri_term() {
            GraphName::Iri(iri)
        } else {
            self.stream
                .error_at_current("expected IRI or variable after GRAPH");
            return None;
        };

        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' after GRAPH name");
            return None;
        }

        // Parse the inner triples (same construct-template grammar as other UPDATE templates).
        let mut triples: Vec<crate::ast::TriplePattern> = Vec::new();
        while !self.stream.check(&TokenKind::RBrace) && !self.stream.is_eof() {
            let subject = match self.parse_subject() {
                Some(s) => s,
                None => {
                    if self.stream.check(&TokenKind::RBrace) {
                        break;
                    }
                    self.stream
                        .error_at_current("expected subject in GRAPH block");
                    return None;
                }
            };
            self.parse_template_triples_for_subject(&subject, &mut triples)?;
            self.stream.match_token(&TokenKind::Dot);
        }

        if !self.stream.match_token(&TokenKind::RBrace) {
            self.stream
                .error_at_current("expected '}' after GRAPH block");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(QuadPatternElement::Graph {
            name,
            triples,
            span,
        })
    }

    /// Parse WHERE clause for update operations.
    fn parse_update_where_clause(&mut self) -> Option<GraphPattern> {
        // Require WHERE keyword
        if !self.stream.match_keyword(TokenKind::KwWhere) {
            self.stream.error_at_current("expected WHERE clause");
            return None;
        }

        // Expect opening brace
        if !self.stream.match_token(&TokenKind::LBrace) {
            self.stream
                .error_at_current("expected '{' for WHERE clause");
            return None;
        }

        // Parse group graph pattern (contents within { ... })
        self.parse_group_graph_pattern()
    }

    /// Parse USING clause for update operations.
    fn parse_using_clause(&mut self) -> Option<UsingClause> {
        if !self.stream.check_keyword(TokenKind::KwUsing) {
            return None;
        }

        let mut out: Option<UsingClause> = None;
        while self.stream.check_keyword(TokenKind::KwUsing) {
            let start = self.stream.current_span();
            self.stream.advance(); // consume USING

            // Check for NAMED
            if self.stream.check_keyword(TokenKind::KwNamed) {
                self.stream.advance(); // consume NAMED
                let iri = self.parse_iri_term()?;
                let span = start.union(iri.span);
                match &mut out {
                    Some(using) => {
                        using.named_graphs.push(iri);
                        using.span = using.span.union(span);
                    }
                    None => out = Some(UsingClause::with_named_graph(iri, span)),
                }
                continue;
            }

            // Default graph
            let iri = self.parse_iri_term()?;
            let span = start.union(iri.span);
            match &mut out {
                Some(using) => {
                    using.default_graphs.push(iri);
                    using.span = using.span.union(span);
                }
                None => out = Some(UsingClause::with_default_graph(iri, span)),
            }
        }

        out
    }
}
