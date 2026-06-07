//! Term parsing: subjects, predicates, objects, IRIs, literals, blank nodes.

use crate::ast::annotation::{Annotation, AnnotationBlock, AnnotationEntry, ReifierId, TripleTerm};
use crate::ast::path::PropertyPath;
use crate::ast::{
    BlankNode, GraphPattern, Iri, IriValue, Literal, ObjectTerm, PredicateTerm, QuotedTriple,
    SubjectTerm, Term, TriplePattern, Var, VarOrIri,
};
use crate::lex::TokenKind;
use crate::span::SourceSpan;

use super::path::parse_property_path;

use super::Verb;

impl super::Parser<'_> {
    /// Parse a simple predicate (no property paths).
    pub(super) fn parse_simple_predicate(&mut self) -> Option<PredicateTerm> {
        // 'a' keyword (rdf:type)
        if self.stream.check_keyword(TokenKind::KwA) {
            let span = self.stream.current_span();
            self.stream.advance();
            return Some(PredicateTerm::Iri(Iri::rdf_type(span)));
        }

        // Variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(PredicateTerm::Var(Var::new(name.as_ref(), span)));
        }

        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(PredicateTerm::Iri(iri));
        }

        self.stream.error_at_current("expected predicate");
        None
    }

    /// Parse a variable or IRI.
    pub(super) fn parse_var_or_iri(&mut self) -> Option<VarOrIri> {
        // Variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(VarOrIri::Var(Var::new(name.as_ref(), span)));
        }

        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(VarOrIri::Iri(iri));
        }

        None
    }

    /// Parse a subject term.
    pub(super) fn parse_subject(&mut self) -> Option<SubjectTerm> {
        // Variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(SubjectTerm::Var(Var::new(name.as_ref(), span)));
        }

        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(SubjectTerm::Iri(iri));
        }

        // Blank node
        if let Some(bnode) = self.parse_blank_node() {
            return Some(SubjectTerm::BlankNode(bnode));
        }

        // RDF-star quoted triple: << subject predicate object >>
        if self.stream.check(&TokenKind::TripleStart) {
            return self.parse_quoted_triple().map(SubjectTerm::QuotedTriple);
        }

        // RDF 1.2 triple term in subject position is a deferred shape
        // (nested triple terms / arbitrary triple-term values). Reject
        // here with a targeted message instead of falling through to
        // generic "expected subject" errors.
        if self.stream.check(&TokenKind::TripleTermStart) {
            self.stream
                .error_at_current("nested triple terms are not supported in v1");
            return None;
        }

        // RDF collection (list) syntax: ( item1 item2 ... ) or ()
        // Not yet implemented — skip and emit error so the parser doesn't infinite-loop.
        if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
            self.stream
                .error_at_current("RDF collection (list) syntax is not yet supported");
            self.skip_collection();
            return None;
        }

        None
    }

    /// Parse an RDF-star quoted triple: `<< subject predicate object >>`
    pub(super) fn parse_quoted_triple(&mut self) -> Option<QuotedTriple> {
        let start = self.stream.current_span();

        // Consume <<
        if !self.stream.match_token(&TokenKind::TripleStart) {
            return None;
        }

        // Parse the inner triple: subject, predicate, object
        let subject = self.parse_subject()?;
        let predicate = self.parse_simple_predicate()?;
        let object = self.parse_object()?;

        // Expect >>
        if !self.stream.match_token(&TokenKind::TripleEnd) {
            self.stream
                .error_at_current("expected '>>' to close quoted triple");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(QuotedTriple::new(subject, predicate, object, span))
    }

    /// Parse a verb (predicate or property path).
    ///
    /// In SPARQL, a verb is either:
    /// - VerbSimple: a variable
    /// - VerbPath: a property path (which includes simple IRIs)
    pub(super) fn parse_verb(&mut self) -> Option<Verb> {
        // Variable is always a simple predicate (VerbSimple)
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(Verb::Simple(PredicateTerm::Var(Var::new(
                name.as_ref(),
                span,
            ))));
        }

        // Check for path-starting tokens that can't be simple predicates
        if matches!(
            self.stream.peek().kind,
            TokenKind::Caret | TokenKind::Bang | TokenKind::LParen
        ) {
            // Definitely a path
            return self.parse_path_as_verb();
        }

        // IRI or 'a' - need to check if followed by path operator
        if self.stream.check_keyword(TokenKind::KwA)
            || matches!(
                self.stream.peek().kind,
                TokenKind::Iri(_) | TokenKind::PrefixedName { .. } | TokenKind::PrefixedNameNs(_)
            )
        {
            // Look ahead to see if this is a simple predicate or start of a path
            let pos = self.stream.position();

            // Try parsing as path
            match parse_property_path(self.stream) {
                Ok(path) => {
                    // Check if it's a simple path (just IRI or 'a')
                    if path.is_simple() {
                        // Convert back to simple predicate
                        match path {
                            PropertyPath::Iri(iri) => {
                                return Some(Verb::Simple(PredicateTerm::Iri(iri)));
                            }
                            PropertyPath::A { span } => {
                                return Some(Verb::Simple(PredicateTerm::Iri(Iri::rdf_type(span))));
                            }
                            _ => unreachable!("is_simple returned true for non-simple path"),
                        }
                    }
                    return Some(Verb::Path(path));
                }
                Err(_) => {
                    // Restore position and try simple predicate
                    self.stream.restore(pos);
                }
            }
        }

        self.stream
            .error_at_current("expected predicate or property path");
        None
    }

    /// Parse a property path as a verb.
    pub(super) fn parse_path_as_verb(&mut self) -> Option<Verb> {
        match parse_property_path(self.stream) {
            Ok(path) => Some(Verb::Path(path)),
            Err(msg) => {
                self.stream.error_at_current(&msg);
                None
            }
        }
    }

    /// Parse an object term.
    pub(super) fn parse_object(&mut self) -> Option<ObjectTerm> {
        // Variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(Term::Var(Var::new(name.as_ref(), span)));
        }

        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(Term::Iri(iri));
        }

        // Literal
        if let Some(lit) = self.parse_literal() {
            return Some(Term::Literal(lit));
        }

        // Blank node
        if let Some(bnode) = self.parse_blank_node() {
            return Some(Term::BlankNode(bnode));
        }

        // RDF collection (list) syntax: ( item1 item2 ... ) or ()
        // Not yet implemented — skip and emit error so the parser doesn't infinite-loop.
        if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
            self.stream
                .error_at_current("RDF collection (list) syntax is not yet supported");
            self.skip_collection();
            return None;
        }

        self.stream.error_at_current("expected object");
        None
    }

    /// Parse an IRI (full or prefixed).
    pub(super) fn parse_iri_term(&mut self) -> Option<Iri> {
        // Full IRI
        if let Some((iri, span)) = self.stream.consume_iri() {
            return Some(Iri::full(iri.as_ref(), span));
        }

        // Prefixed name with local part
        if let Some((prefix, local, span)) = self.stream.consume_prefixed_name() {
            return Some(Iri::prefixed(prefix.as_ref(), local.as_ref(), span));
        }

        // Prefixed name namespace only (e.g., "ex:" for "ex:")
        if let Some((prefix, span)) = self.stream.consume_prefixed_name_ns() {
            return Some(Iri::prefixed(prefix.as_ref(), "", span));
        }

        None
    }

    /// Parse a literal.
    pub(super) fn parse_literal(&mut self) -> Option<Literal> {
        let token = self.stream.peek();
        let span = token.span;

        match &token.kind {
            TokenKind::String(_) => {
                let token = self.stream.consume();
                if let TokenKind::String(value) = token.kind {
                    // Check for language tag or datatype
                    if let TokenKind::LangTag(lang) = &self.stream.peek().kind {
                        // Language tag: "hello"@en
                        let lang = lang.clone();
                        let lang_span = self.stream.current_span();
                        self.stream.advance();
                        let full_span = span.union(lang_span);
                        return Some(Literal::lang_string(
                            value.as_ref(),
                            lang.as_ref(),
                            full_span,
                        ));
                    } else if self.stream.match_token(&TokenKind::DoubleCaret) {
                        // Datatype: "42"^^xsd:integer
                        if let Some(dt) = self.parse_iri_term() {
                            let full_span = span.union(dt.span);
                            return Some(Literal::typed(value.as_ref(), dt, full_span));
                        }
                        return Some(Literal::string(value.as_ref(), span));
                    }
                    return Some(Literal::string(value.as_ref(), span));
                }
            }
            TokenKind::Integer(n) => {
                let n = *n;
                self.stream.advance();
                return Some(Literal::integer(n, span));
            }
            TokenKind::Decimal(_) => {
                let token = self.stream.consume();
                if let TokenKind::Decimal(s) = token.kind {
                    return Some(Literal::decimal(s.as_ref(), span));
                }
            }
            TokenKind::Double(n) => {
                let n = *n;
                self.stream.advance();
                return Some(Literal::double(n, span));
            }
            // Signed numeric literals: +/-  followed by number (SPARQL
            // INTEGER_POSITIVE, INTEGER_NEGATIVE, DECIMAL_POSITIVE, etc.)
            // The lexer tokenizes signs as Plus/Minus; we recombine here.
            TokenKind::Plus | TokenKind::Minus => {
                let is_neg = matches!(token.kind, TokenKind::Minus);
                let sign_span = span;
                // Peek at the NEXT token to see if it's a number
                if let Some(lit) = self.try_parse_signed_numeric(is_neg, sign_span) {
                    return Some(lit);
                }
            }
            TokenKind::KwTrue => {
                self.stream.advance();
                return Some(Literal::boolean(true, span));
            }
            TokenKind::KwFalse => {
                self.stream.advance();
                return Some(Literal::boolean(false, span));
            }
            _ => {}
        }

        None
    }

    /// Try to parse a signed numeric literal (`+N` or `-N`).
    ///
    /// Called when `parse_literal` sees `Plus`/`Minus` and needs to check
    /// if the next token is a number. Uses save/restore to avoid consuming
    /// the sign if the next token is not numeric.
    fn try_parse_signed_numeric(
        &mut self,
        is_neg: bool,
        sign_span: crate::span::SourceSpan,
    ) -> Option<Literal> {
        let pos = self.stream.position();
        self.stream.advance(); // consume the sign

        let next = self.stream.peek();
        match &next.kind {
            TokenKind::Integer(n) => {
                let n = if is_neg { -*n } else { *n };
                let num_span = self.stream.current_span();
                self.stream.advance();
                Some(Literal::integer(n, sign_span.union(num_span)))
            }
            TokenKind::Decimal(_) => {
                let token = self.stream.consume();
                let TokenKind::Decimal(s) = token.kind else {
                    unreachable!("already matched Decimal")
                };
                let mut signed = String::new();
                if is_neg {
                    signed.push('-');
                }
                signed.push_str(s.as_ref());
                Some(Literal::decimal(&signed, sign_span.union(token.span)))
            }
            TokenKind::Double(n) => {
                let n = if is_neg { -*n } else { *n };
                let num_span = self.stream.current_span();
                self.stream.advance();
                Some(Literal::double(n, sign_span.union(num_span)))
            }
            _ => {
                // Not a number after sign — restore position
                self.stream.restore(pos);
                None
            }
        }
    }

    /// Parse a blank node.
    pub(super) fn parse_blank_node(&mut self) -> Option<BlankNode> {
        let token = self.stream.peek();
        let span = token.span;

        match &token.kind {
            TokenKind::BlankNodeLabel(_) => {
                let token = self.stream.consume();
                if let TokenKind::BlankNodeLabel(label) = token.kind {
                    return Some(BlankNode::labeled(label.as_ref(), span));
                }
            }
            TokenKind::Anon => {
                self.stream.advance();
                return Some(BlankNode::anon(span));
            }
            TokenKind::LBracket => {
                // [ ... ] blank node syntax
                let start = self.stream.current_span();
                self.stream.advance(); // consume [

                // For now, just handle empty [] - property list notation is Phase 3
                if self.stream.match_token(&TokenKind::RBracket) {
                    let span = start.union(self.stream.previous_span());
                    return Some(BlankNode::anon(span));
                }

                // Non-empty blank node syntax - placeholder
                // Skip to ]
                let mut depth = 1;
                while depth > 0 && !self.stream.is_eof() {
                    match &self.stream.peek().kind {
                        TokenKind::LBracket => depth += 1,
                        TokenKind::RBracket => depth -= 1,
                        _ => {}
                    }
                    self.stream.advance();
                }
                let span = start.union(self.stream.previous_span());
                return Some(BlankNode::anon(span));
            }
            _ => {}
        }

        None
    }

    /// Skip an RDF collection (list) in the token stream.
    ///
    /// Handles both `Nil` (empty list `()`) and `LParen ... RParen` (non-empty list).
    /// Used for error recovery when encountering unsupported collection syntax.
    fn skip_collection(&mut self) {
        if self.stream.match_token(&TokenKind::Nil) {
            return;
        }
        debug_assert!(
            self.stream.check(&TokenKind::LParen),
            "skip_collection called on non-collection token: {:?}",
            self.stream.peek().kind
        );
        if self.stream.match_token(&TokenKind::LParen) {
            self.stream
                .skip_balanced(&TokenKind::LParen, &TokenKind::RParen);
        }
    }

    /// Check if current token can start a verb (predicate or path).
    pub(super) fn is_verb_start(&self) -> bool {
        matches!(
            self.stream.peek().kind,
            TokenKind::Var(_)
                | TokenKind::Iri(_)
                | TokenKind::PrefixedName { .. }
                | TokenKind::PrefixedNameNs(_)
                | TokenKind::KwA
                | TokenKind::Caret // inverse path
                | TokenKind::Bang  // negated property set
                | TokenKind::LParen // grouped path
        )
    }

    /// Parse triple patterns until we hit a non-triple token.
    ///
    /// Returns a list of graph patterns that may include:
    /// - BGPs (for simple triple patterns)
    /// - Path patterns (for property path expressions)
    pub(super) fn parse_triples_block(&mut self) -> Option<Vec<GraphPattern>> {
        let mut patterns = Vec::new();
        let mut triples = Vec::new();
        let mut bgp_start: Option<SourceSpan> = None;

        // Parse subject
        let subject = self.parse_subject()?;

        // Parse predicate-object list, collecting patterns
        self.parse_predicate_object_list_with_paths(
            &subject,
            &mut triples,
            &mut patterns,
            &mut bgp_start,
        )?;

        // Flush any remaining triples to a BGP
        if !triples.is_empty() {
            let span = bgp_start.unwrap_or(subject.span());
            let end_span = triples.last().map(|t| t.span).unwrap_or(span);
            patterns.push(GraphPattern::Bgp {
                patterns: std::mem::take(&mut triples),
                span: span.union(end_span),
            });
        }

        // Optional dot at end
        self.stream.match_token(&TokenKind::Dot);

        Some(patterns)
    }

    /// Parse a predicate-object list for a given subject, handling both
    /// simple predicates and property paths.
    pub(super) fn parse_predicate_object_list_with_paths(
        &mut self,
        subject: &SubjectTerm,
        triples: &mut Vec<TriplePattern>,
        patterns: &mut Vec<GraphPattern>,
        bgp_start: &mut Option<SourceSpan>,
    ) -> Option<()> {
        loop {
            // Parse verb (predicate or property path)
            let verb = self.parse_verb()?;

            match verb {
                Verb::Simple(predicate) => {
                    // RDF 1.2 reifies form: when the predicate lexically
                    // resolves to `rdf:reifies` and a triple-term is in
                    // object position, emit a `GraphPattern::AnnotationTarget`
                    // (after flushing any in-progress BGP). This is the only
                    // context in which `<<( s p o )>>` may appear in object
                    // position per the v1 contract.
                    if predicate_is_rdf_reifies(&predicate)
                        && self.stream.check(&TokenKind::TripleTermStart)
                    {
                        flush_bgp(subject, triples, patterns, bgp_start);
                        self.parse_reifies_object_list(subject, &predicate, patterns)?;
                    } else {
                        // Parse object list for simple predicate
                        self.parse_object_list(subject, &predicate, triples, bgp_start)?;
                    }
                }
                Verb::Path(path) => {
                    // Flush any accumulated triples first
                    flush_bgp(subject, triples, patterns, bgp_start);

                    // Parse objects for path pattern
                    self.parse_path_object_list(subject, &path, patterns)?;
                }
            }

            // Check for semicolon (more predicate-object pairs)
            if !self.stream.match_token(&TokenKind::Semicolon) {
                break;
            }

            // After semicolon, predicate is optional (allows trailing semicolon)
            if !self.is_verb_start() {
                break;
            }
        }

        Some(())
    }

    /// Parse an object list for a given subject and simple predicate.
    pub(super) fn parse_object_list(
        &mut self,
        subject: &SubjectTerm,
        predicate: &PredicateTerm,
        triples: &mut Vec<TriplePattern>,
        bgp_start: &mut Option<SourceSpan>,
    ) -> Option<()> {
        loop {
            // A bare `<<(` here means the user wrote a triple-term object
            // for a predicate other than `rdf:reifies`. v1 only accepts
            // triple terms as the object of `rdf:reifies`; reject with
            // the documented deferred-feature error.
            if self.stream.check(&TokenKind::TripleTermStart) {
                self.stream.error_at_current(
                    "triple terms (<<( s p o )>>) are only allowed as the object of \
                     rdf:reifies in v1; arbitrary triple-term values are deferred",
                );
                return None;
            }

            // Parse object
            let object = self.parse_object()?;

            // Track BGP start span
            if bgp_start.is_none() {
                *bgp_start = Some(subject.span());
            }

            // RDF 1.2 annotation tail: zero or more (reifier | annotationBlock).
            // Per v1 contract, accept at most one reifier and one block in any
            // order. Literal-valued objects are accepted — the lowering path
            // pins their datatype/language constraint onto the synthesized
            // `TriplePattern.dtc`.
            let annotation = self.parse_annotation_tail()?;

            // Create triple pattern (span covers subject, predicate, object,
            // and annotation tail if present).
            let mut span = subject.span().union(predicate.span()).union(object.span());
            if let Some(ann) = &annotation {
                span = span.union(ann.span);
            }
            let triple = match annotation {
                Some(ann) => TriplePattern::with_annotation(
                    subject.clone(),
                    predicate.clone(),
                    object,
                    ann,
                    span,
                ),
                None => TriplePattern::new(subject.clone(), predicate.clone(), object, span),
            };
            triples.push(triple);

            // Check for comma (more objects)
            if !self.stream.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Some(())
    }

    /// Parse a single triple term `<<( s p o )>>` after the opening
    /// `TripleTermStart` token has been verified by the caller.
    ///
    /// Strict v1 rules:
    /// - Triple-term subject must be an IRI, blank node, or variable
    ///   (no nested triple terms).
    /// - Triple-term predicate must be a simple predicate (no paths).
    /// - Triple-term object must be an ordinary term (no nested triple
    ///   terms, no annotation tails).
    fn parse_triple_term(&mut self) -> Option<TripleTerm> {
        let start = self.stream.current_span();
        if !self.stream.match_token(&TokenKind::TripleTermStart) {
            self.stream
                .error_at_current("expected '<<(' to begin triple term");
            return None;
        }

        let subject = self.parse_subject()?;
        if matches!(subject, SubjectTerm::QuotedTriple(_)) {
            self.stream
                .error_at_current("nested triple terms are not supported in v1");
            return None;
        }
        let predicate = self.parse_simple_predicate()?;

        // Reject nested triple terms in object position.
        if self.stream.check(&TokenKind::TripleTermStart) {
            self.stream
                .error_at_current("nested triple terms are not supported in v1");
            return None;
        }
        let object = self.parse_object()?;

        if !self.stream.match_token(&TokenKind::TripleTermEnd) {
            self.stream
                .error_at_current("expected ')>>' to close triple term");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(TripleTerm {
            subject,
            predicate,
            object,
            span,
        })
    }

    /// Parse the object position for an `rdf:reifies` predicate, emitting
    /// a `GraphPattern::AnnotationTarget` for each parsed triple term.
    ///
    /// SPARQL allows comma-separated objects; v1 rejects multiple triple
    /// terms per `rdf:reifies` because that would mean one reifier
    /// reifying multiple base edges (the deferred multi-triple-reifier
    /// case from the design doc).
    fn parse_reifies_object_list(
        &mut self,
        subject: &SubjectTerm,
        predicate: &PredicateTerm,
        patterns: &mut Vec<GraphPattern>,
    ) -> Option<()> {
        let triple_term = self.parse_triple_term()?;

        let span = subject.span().union(triple_term.span);
        patterns.push(GraphPattern::AnnotationTarget {
            reifier: subject.clone(),
            predicate: predicate.clone(),
            triple_term,
            span,
        });

        if self.stream.match_token(&TokenKind::Comma) {
            self.stream.error_at_current(
                "v1 rejects an annotation subject reifying more than one triple term; \
                 multi-triple reifiers are deferred",
            );
            return None;
        }

        Some(())
    }

    /// Parse the optional RDF 1.2 annotation tail after an object.
    ///
    /// Grammar: `annotation ::= ( reifier | annotationBlock )*`
    /// v1 narrowing: at most one reifier and one block, any order.
    /// Returns `Ok(None)` when no tail is present.
    ///
    /// Literal-valued objects are accepted: the constraint-preserving
    /// lowering path (`lower_object_with_constraint`) pins the literal's
    /// datatype / language tag onto the synthesized `TriplePattern.dtc`
    /// so reified base-edge object positions match exactly. Without
    /// `dtc`, same-lexical literals with different datatypes (or
    /// languages) would cross-match annotations on each other.
    fn parse_annotation_tail(&mut self) -> Option<Option<Annotation>> {
        let starts_tail =
            self.stream.check(&TokenKind::Tilde) || self.stream.check(&TokenKind::AnnotationOpen);
        if !starts_tail {
            return Some(None);
        }

        let start = self.stream.current_span();
        let mut reifier: Option<ReifierId> = None;
        let mut block: Option<AnnotationBlock> = None;
        let mut last_span = start;

        loop {
            if self.stream.check(&TokenKind::Tilde) {
                if reifier.is_some() {
                    self.stream
                        .error_at_current("at most one reifier (`~`) is allowed per triple in v1");
                    return None;
                }
                let r_span = self.stream.current_span();
                self.stream.advance(); // consume `~`
                let r = self.parse_reifier_id_after_tilde();
                last_span = r.as_ref().map(ReifierId::span).unwrap_or(r_span);
                reifier = r;
            } else if self.stream.check(&TokenKind::AnnotationOpen) {
                if block.is_some() {
                    self.stream.error_at_current(
                        "at most one annotation block (`{| ... |}`) is allowed per triple in v1",
                    );
                    return None;
                }
                let b = self.parse_annotation_block()?;
                last_span = b.span;
                block = Some(b);
            } else {
                break;
            }
        }

        let span = start.union(last_span);
        Some(Some(Annotation {
            reifier,
            block,
            span,
        }))
    }

    /// Parse the optional id following `~`. The bare `~` form (no id)
    /// returns `None`, matching the RDF 1.2 grammar `reifier ::= '~' (iri | BlankNode)?`.
    /// We extend the grammar to accept variables for SPARQL queries; the
    /// update-path lower rejects `~ ?var` in `INSERT DATA` / `DELETE DATA`.
    fn parse_reifier_id_after_tilde(&mut self) -> Option<ReifierId> {
        // Variable
        if let Some((name, span)) = self.stream.consume_var() {
            return Some(ReifierId::Var(Var::new(name.as_ref(), span)));
        }
        // Blank node (labeled `_:foo` or `[]`)
        if let Some(bnode) = self.parse_blank_node() {
            return Some(ReifierId::BlankNode(bnode));
        }
        // IRI
        if let Some(iri) = self.parse_iri_term() {
            return Some(ReifierId::Iri(iri));
        }
        // Bare `~` with no following id is valid (mints fresh on lower).
        None
    }

    /// Parse a `{| predicateObjectList |}` annotation block.
    ///
    /// Each entry is a predicate-object pair applied to the enclosing
    /// reifier. v1 rejects nested annotation tails on body entries
    /// (annotations-on-annotations are deferred).
    fn parse_annotation_block(&mut self) -> Option<AnnotationBlock> {
        let start = self.stream.current_span();
        if !self.stream.match_token(&TokenKind::AnnotationOpen) {
            self.stream
                .error_at_current("expected '{|' to begin annotation block");
            return None;
        }

        let mut entries: Vec<AnnotationEntry> = Vec::new();
        // Allow empty block: `{| |}` is a valid v1 surface (semantics
        // depends on RDF/LPG mode — see plan).
        if !self.stream.check(&TokenKind::AnnotationClose) {
            loop {
                let predicate = self.parse_simple_predicate()?;
                loop {
                    // Reject nested triple terms here too.
                    if self.stream.check(&TokenKind::TripleTermStart) {
                        self.stream.error_at_current(
                            "triple terms (<<( s p o )>>) are not allowed inside an \
                             annotation block; reify edges via `rdf:reifies` instead",
                        );
                        return None;
                    }
                    let object = self.parse_object()?;

                    // Reject nested annotation tails: annotations-on-annotations
                    // are deferred per the design doc. Surfacing a clear error
                    // here is friendlier than letting it parse and lower-rejecting.
                    if self.stream.check(&TokenKind::Tilde)
                        || self.stream.check(&TokenKind::AnnotationOpen)
                    {
                        self.stream.error_at_current(
                            "annotations on annotation-block entries are not supported \
                             in v1 (annotations-on-annotations are deferred)",
                        );
                        return None;
                    }

                    let span = predicate.span().union(object.span());
                    entries.push(AnnotationEntry {
                        predicate: predicate.clone(),
                        object,
                        span,
                    });

                    if !self.stream.match_token(&TokenKind::Comma) {
                        break;
                    }
                }
                if !self.stream.match_token(&TokenKind::Semicolon) {
                    break;
                }
                // After `;`, allow trailing semicolon before `|}`.
                if self.stream.check(&TokenKind::AnnotationClose) {
                    break;
                }
            }
        }

        if !self.stream.match_token(&TokenKind::AnnotationClose) {
            self.stream
                .error_at_current("expected '|}' to close annotation block");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(AnnotationBlock { entries, span })
    }

    /// Parse an object list for a property path, creating Path patterns.
    pub(super) fn parse_path_object_list(
        &mut self,
        subject: &SubjectTerm,
        path: &PropertyPath,
        patterns: &mut Vec<GraphPattern>,
    ) -> Option<()> {
        loop {
            // Parse object
            let object = self.parse_object()?;

            // Create path pattern
            let span = subject.span().union(path.span()).union(object.span());
            patterns.push(GraphPattern::Path {
                subject: subject.clone(),
                path: path.clone(),
                object,
                span,
            });

            // Check for comma (more objects)
            if !self.stream.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Some(())
    }

    /// Parse dataset clause (FROM and FROM NAMED).
    ///
    /// Grammar: DatasetClause* where DatasetClause ::= 'FROM' ( DefaultGraphClause | NamedGraphClause )
    /// DefaultGraphClause ::= SourceSelector (just an IRI)
    /// NamedGraphClause ::= 'NAMED' SourceSelector
    ///
    /// Fluree extension: `FROM <iri> TO <iri>` for history time range queries.
    ///
    /// Returns None if no FROM clauses are present, Some(DatasetClause) otherwise.
    pub(super) fn parse_dataset_clause(&mut self) -> Option<DatasetClause> {
        let mut default_graphs = Vec::new();
        let mut named_graphs = Vec::new();
        let mut to_graph: Option<Iri> = None;
        let mut start_span: Option<SourceSpan> = None;
        let mut end_span: Option<SourceSpan> = None;

        while self.stream.check_keyword(TokenKind::KwFrom) {
            let from_span = self.stream.current_span();
            if start_span.is_none() {
                start_span = Some(from_span);
            }
            self.stream.advance(); // consume FROM

            // Check for NAMED
            if self.stream.check_keyword(TokenKind::KwNamed) {
                self.stream.advance(); // consume NAMED

                // Parse IRI
                if let Some(iri) = self.parse_iri_term() {
                    end_span = Some(iri.span);
                    named_graphs.push(iri);
                } else {
                    self.stream
                        .error_at_current("expected IRI after FROM NAMED");
                    return None;
                }
            } else {
                // Default graph - parse IRI
                if let Some(iri) = self.parse_iri_term() {
                    end_span = Some(iri.span);
                    default_graphs.push(iri);

                    // Fluree extension: check for TO keyword for history range
                    if self.stream.check_keyword(TokenKind::KwTo) {
                        self.stream.advance(); // consume TO

                        // Parse the to_graph IRI
                        if let Some(to_iri) = self.parse_iri_term() {
                            end_span = Some(to_iri.span);
                            to_graph = Some(to_iri);
                        } else {
                            self.stream.error_at_current("expected IRI after TO");
                            return None;
                        }
                    }
                } else {
                    self.stream.error_at_current("expected IRI after FROM");
                    return None;
                }
            }
        }

        // Return None if no FROM clauses were found
        if default_graphs.is_empty() && named_graphs.is_empty() {
            return None;
        }

        let span = start_span
            .unwrap()
            .union(end_span.unwrap_or(start_span.unwrap()));

        Some(DatasetClause {
            default_graphs,
            named_graphs,
            to_graph,
            span,
        })
    }

    /// Parse predicate-object list for CONSTRUCT template (no property paths).
    ///
    /// Also used by SPARQL UPDATE `INSERT DATA` / `DELETE DATA` and
    /// `INSERT { ... }` / `DELETE { ... }` template clauses, so it must
    /// recognize the RDF 1.2 annotation tail. CONSTRUCT itself rejects
    /// annotations in the template (M4.5) at the lower layer, not the
    /// parse layer.
    pub(super) fn parse_construct_predicate_object_list(
        &mut self,
        subject: &SubjectTerm,
        triples: &mut Vec<TriplePattern>,
    ) -> Option<()> {
        loop {
            // Parse predicate (simple only - no paths in CONSTRUCT)
            let predicate = self.parse_simple_predicate()?;

            // Parse object list
            loop {
                // Triple-term objects are NOT allowed in DATA / template
                // contexts in v1 (the `~` annotation-tail form covers
                // the same content). Reject with the same documented
                // deferred-feature error used by the WHERE path.
                if self.stream.check(&TokenKind::TripleTermStart) {
                    self.stream.error_at_current(
                        "triple terms (<<( s p o )>>) are only allowed as the object of \
                         rdf:reifies in v1; use the `~ {| ... |}` annotation-tail form in \
                         INSERT/DELETE DATA and templates",
                    );
                    return None;
                }

                let object = self.parse_object()?;
                let annotation = self.parse_annotation_tail()?;
                let mut span = subject.span().union(predicate.span()).union(object.span());
                if let Some(ann) = &annotation {
                    span = span.union(ann.span);
                }
                let triple = match annotation {
                    Some(ann) => TriplePattern::with_annotation(
                        subject.clone(),
                        predicate.clone(),
                        object,
                        ann,
                        span,
                    ),
                    None => TriplePattern::new(subject.clone(), predicate.clone(), object, span),
                };
                triples.push(triple);

                if !self.stream.match_token(&TokenKind::Comma) {
                    break;
                }
            }

            // Check for semicolon (more predicate-object pairs)
            if !self.stream.match_token(&TokenKind::Semicolon) {
                break;
            }

            // After semicolon, predicate is optional
            if !self.stream.is_term_start() && !self.stream.check_keyword(TokenKind::KwA) {
                break;
            }
        }

        Some(())
    }
}

use crate::ast::DatasetClause;

/// Flush an in-progress BGP into `patterns`, clearing `triples` and
/// `bgp_start`. No-op when `triples` is empty.
fn flush_bgp(
    subject: &SubjectTerm,
    triples: &mut Vec<TriplePattern>,
    patterns: &mut Vec<GraphPattern>,
    bgp_start: &mut Option<SourceSpan>,
) {
    if triples.is_empty() {
        return;
    }
    let span = bgp_start.unwrap_or(subject.span());
    let end_span = triples.last().map(|t| t.span).unwrap_or(span);
    patterns.push(GraphPattern::Bgp {
        patterns: std::mem::take(triples),
        span: span.union(end_span),
    });
    *bgp_start = None;
}

/// Lexical check: does this predicate term resolve to `rdf:reifies`?
///
/// We handle two surface forms: a full IRI matching the standard
/// `http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies`, and the
/// prefixed form `rdf:reifies` assuming the conventional `rdf:` prefix
/// binding. Users with a non-standard `rdf:` binding can fall back to
/// the full IRI; the prefix-resolution layer at lower time will reject
/// any false positive that slips through (the actual IRI lookup will
/// not match).
fn predicate_is_rdf_reifies(predicate: &PredicateTerm) -> bool {
    match predicate {
        PredicateTerm::Iri(iri) => match &iri.value {
            IriValue::Full(s) => s.as_ref() == fluree_vocab::rdf::REIFIES,
            IriValue::Prefixed { prefix, local } => {
                prefix.as_ref() == "rdf" && local.as_ref() == "reifies"
            }
        },
        PredicateTerm::Var(_) => false,
    }
}
