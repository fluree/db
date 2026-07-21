//! Term parsing: subjects, predicates, objects, IRIs, literals, blank nodes.

use crate::ast::annotation::{
    Annotation, AnnotationBlock, AnnotationEntry, AnnotationUnit, AnnotationVerb, ReifierId,
    TripleTerm,
};
use crate::ast::path::PropertyPath;
use crate::ast::{
    BlankNode, GraphPattern, Iri, IriValue, Literal, ObjectTerm, PredicateTerm, QtReifier,
    QuotedTriple, SubjectTerm, Term, TriplePattern, Var, VarOrIri,
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

        // RDF 1.2 triple term *value* in subject position:
        // `<<( s p o )>> :p1 :o1 .` (accept-then-defer, D-1). Blank nodes
        // are allowed here (triple-pattern context — `bnode-tripleterm-*`);
        // lowering rejects the term with `not_implemented`.
        if self.stream.check(&TokenKind::TripleTermStart) {
            return self
                .parse_triple_term_value(true)
                .map(|tt| SubjectTerm::TripleTerm(Box::new(tt)));
        }

        // RDF collection (list) syntax: ( item1 item2 ... ) or ()
        if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
            return match self.parse_collection()? {
                Term::Iri(iri) => Some(SubjectTerm::Iri(iri)),
                Term::BlankNode(bnode) => Some(SubjectTerm::BlankNode(bnode)),
                _ => unreachable!("parse_collection returns an IRI or blank node"),
            };
        }

        None
    }

    /// Parse an RDF-star / RDF 1.2 (reified) quoted triple:
    /// `<< subject predicate object ( ~ reifier? )? >>`
    ///
    /// The optional in-triple `~ reifier` tail is the RDF 1.2
    /// `ReifiedTriple` form; without it the node stays eligible for the
    /// legacy Fluree `f:t`/`f:op` history reading (decided at lowering).
    pub(super) fn parse_quoted_triple(&mut self) -> Option<QuotedTriple> {
        let start = self.stream.current_span();

        // Consume <<
        if !self.stream.match_token(&TokenKind::TripleStart) {
            return None;
        }

        // Parse the inner triple: subject, predicate, object.
        // Collections are not legal inside a quoted triple — the RDF 1.2
        // grammar's rtSubject/rtObject exclude `Collection`/`NIL` (W3C
        // negative tests list-anonreifier-01/02, quoted-list-*-anonreifier).
        self.reject_collection_in_quoted_context()?;
        let subject = self.parse_subject()?;
        let predicate = self.parse_simple_predicate()?;
        self.reject_collection_in_quoted_context()?;
        let object = self.parse_object()?;

        // Optional RDF 1.2 reifier: `~ id?` before `>>`
        // (`Reifier ::= '~' VarOrReifierId?`).
        let reifier = if self.stream.check(&TokenKind::Tilde) {
            let tilde_span = self.stream.current_span();
            self.stream.advance(); // consume `~`
            let id = self.parse_reifier_id_after_tilde();
            let span = id
                .as_ref()
                .map(|r| tilde_span.union(r.span()))
                .unwrap_or(tilde_span);
            Some(QtReifier { id, span })
        } else {
            None
        };

        // Expect >>
        if !self.stream.match_token(&TokenKind::TripleEnd) {
            self.stream
                .error_at_current("expected '>>' to close quoted triple");
            return None;
        }

        let span = start.union(self.stream.previous_span());
        Some(QuotedTriple::new(subject, predicate, object, span).with_reifier(reifier))
    }

    /// Convert a parsed reified triple `<< s p o ~ r? >>` used as a
    /// *statement* (standalone, no property list — SPARQL 1.2 allows
    /// `ReifiedTriple` with an empty `PropertyListPath`) into the
    /// equivalent `GraphPattern::AnnotationTarget`:
    /// `r rdf:reifies <<( s p o )>>`, minting a fresh blank-node
    /// reifier when none was named. This is exactly the spec's
    /// desugaring; nested reified triples inside `s`/`o` ride along on
    /// the `TripleTerm` and are desugared recursively at lowering.
    pub(super) fn reified_triple_to_annotation_target(&mut self, qt: QuotedTriple) -> GraphPattern {
        let span = qt.span;
        let reifier = match qt.reifier.and_then(|r| r.id) {
            Some(ReifierId::Iri(iri)) => SubjectTerm::Iri(iri),
            Some(ReifierId::BlankNode(b)) => SubjectTerm::BlankNode(b),
            Some(ReifierId::Var(v)) => SubjectTerm::Var(v),
            None => {
                // `#` is outside PN_CHARS, so this synthetic label can
                // never collide with a user-written `_:…` label (same
                // scheme as `#bnpl…` / `#coll…`).
                let label = format!("#reif{}", self.bnode_counter);
                self.bnode_counter += 1;
                SubjectTerm::BlankNode(BlankNode::labeled(&label, span))
            }
        };
        let triple_term = TripleTerm {
            subject: *qt.subject,
            predicate: qt.predicate,
            object: *qt.object,
            span,
        };
        GraphPattern::AnnotationTarget {
            reifier,
            predicate: PredicateTerm::Iri(Iri::full(fluree_vocab::rdf::REIFIES, span)),
            triple_term: Box::new(triple_term),
            span,
        }
    }

    /// Error out when the current token starts an RDF collection (`(` or
    /// `()`), which is not legal in quoted-triple / triple-term positions
    /// (the RDF 1.2 grammar's rt/tt subject and object productions exclude
    /// `Collection` and `NIL`).
    fn reject_collection_in_quoted_context(&mut self) -> Option<()> {
        if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
            self.stream.error_at_current(
                "RDF collections ('( ... )') are not allowed inside a quoted triple or triple term",
            );
            return None;
        }
        Some(())
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

        // RDF 1.2 reified triple in object position:
        // `:s :p << :a :b :c ~ reifier? >>`. The term denotes the
        // reifier node; lowering performs the
        // `r rdf:reifies <<( a b c )>>` desugaring.
        if self.stream.check(&TokenKind::TripleStart) {
            return self
                .parse_quoted_triple()
                .map(|qt| Term::QuotedTriple(Box::new(qt)));
        }

        // RDF 1.2 triple term *value* in object position (bare, i.e. for a
        // predicate other than `rdf:reifies`): `:s :p <<( a b c )>>`, and
        // nested triple-term objects. Accept-then-defer (D-1); lowering
        // rejects with `not_implemented`.
        if self.stream.check(&TokenKind::TripleTermStart) {
            return self
                .parse_triple_term_value(true)
                .map(|tt| Term::TripleTerm(Box::new(tt)));
        }

        // RDF collection (list) syntax: ( item1 item2 ... ) or ()
        if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
            return self.parse_collection();
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
            TokenKind::BigInteger(_) => {
                let token = self.stream.consume();
                if let TokenKind::BigInteger(s) = token.kind {
                    return Some(Literal::big_integer(s.as_ref(), span));
                }
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
            TokenKind::BigInteger(_) => {
                let token = self.stream.consume();
                let TokenKind::BigInteger(s) = token.kind else {
                    unreachable!("already matched BigInteger")
                };
                let mut signed = String::new();
                if is_neg {
                    signed.push('-');
                }
                signed.push_str(s.as_ref());
                Some(Literal::big_integer(&signed, sign_span.union(token.span)))
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

                // Empty `[]` is just an anonymous blank node.
                if self.stream.match_token(&TokenKind::RBracket) {
                    let span = start.union(self.stream.previous_span());
                    return Some(BlankNode::anon(span));
                }

                // Non-empty `[ propertyListNotEmpty ]` (SPARQL 1.1 grammar rule
                // 160, BlankNodePropertyList): desugar to a fresh labeled blank
                // node and emit its inner predicate-object list as triples.
                return self.parse_blank_node_property_list(start);
            }
            _ => {}
        }

        None
    }

    /// Parse the body of a non-empty blank-node property list `[ … ]` (the
    /// opening `[` has already been consumed; `start` is its span).
    ///
    /// Desugars `[ p1 o1 ; p2 o2 ]` to a fresh labeled blank node `_b` plus the
    /// triples `_b p1 o1 . _b p2 o2 .` (SPARQL 1.1 §4.1.4). The triples are
    /// appended to `pending_bnpl_triples` for the enclosing object-list /
    /// triples-block parser to fold into its BGP; the blank node itself is
    /// returned as this term (usable in object or subject position). Nested
    /// `[ … ]` lists recurse through `parse_object` → here.
    fn parse_blank_node_property_list(&mut self, start: SourceSpan) -> Option<BlankNode> {
        // `#` is not a valid blank-node-label character (it is outside PN_CHARS),
        // so the lexer can never produce this label — a user-written `_:…` can
        // never collide with it and be accidentally joined to the synthetic node.
        let label = format!("#bnpl{}", self.bnode_counter);
        self.bnode_counter += 1;
        let subject = SubjectTerm::BlankNode(BlankNode::labeled(&label, start));

        let mut triples: Vec<TriplePattern> = Vec::new();
        let mut bgp_start: Option<SourceSpan> = None;
        loop {
            match self.parse_verb()? {
                Verb::Simple(predicate) => {
                    self.parse_object_list(&subject, &predicate, &mut triples, &mut bgp_start)?;
                }
                Verb::Path(path) => {
                    // `[ path obj ]` — the grammar's `PropertyListPathNotEmpty`
                    // allows a `VerbPath` here. A path is not a `TriplePattern`,
                    // so it rides the `pending_bnpl_patterns` channel (drained
                    // by the enclosing triples-block / path-object-list parser)
                    // instead of this list's local triples.
                    loop {
                        let object = self.parse_object()?;
                        let span = subject.span().union(path.span()).union(object.span());
                        self.pending_bnpl_patterns.push(GraphPattern::Path {
                            subject: subject.clone(),
                            path: path.clone(),
                            object,
                            span,
                        });
                        if !self.stream.match_token(&TokenKind::Comma) {
                            break;
                        }
                    }
                }
            }

            // `;`-separated predicate-object pairs; a trailing `;` is allowed.
            if !self.stream.match_token(&TokenKind::Semicolon) {
                break;
            }
            if !self.is_verb_start() {
                break;
            }
        }

        if !self.stream.match_token(&TokenKind::RBracket) {
            self.stream
                .error_at_current("expected ']' to close blank node property list");
            return None;
        }

        // Surface the nested triples to the enclosing BGP and return the node.
        self.pending_bnpl_triples.append(&mut triples);
        let span = start.union(self.stream.previous_span());
        Some(BlankNode::labeled(&label, span))
    }

    /// Parse an RDF collection `( item1 … itemN )` or the empty list `()`
    /// (which lexes as a single `Nil` token), desugaring per SPARQL 1.1
    /// §4.2.4 into `rdf:first`/`rdf:rest`/`rdf:nil` triples over fresh
    /// blank-node list cells:
    ///
    /// - `()` → the plain IRI `rdf:nil`; no triples.
    /// - `( g1 … gn )` → `_ci rdf:first gi . _ci rdf:rest _c(i+1) .` with
    ///   `_cn rdf:rest rdf:nil .`; the collection term is `_c1`.
    ///
    /// Items are full `GraphNode`s — vars, IRIs, literals, blank-node
    /// property lists, and nested collections all recurse through
    /// `parse_object`. The desugared triples ride the existing
    /// `pending_bnpl_triples` channel, so every enclosing drain site folds
    /// them into its BGP exactly like blank-node property-list triples;
    /// they add only ordinary triples (no new AST/IR/engine surface). This
    /// mirrors Fluree's Turtle ingest, which desugars collections to the
    /// same `rdf:first`/`rdf:rest` predicates.
    fn parse_collection(&mut self) -> Option<ObjectTerm> {
        // `()` (with optional interior whitespace) lexes as a single Nil
        // token — the empty list is just the IRI rdf:nil.
        if self.stream.check(&TokenKind::Nil) {
            let span = self.stream.current_span();
            self.stream.advance();
            return Some(Term::Iri(Iri::rdf_nil(span)));
        }

        let start = self.stream.current_span();
        if !self.stream.match_token(&TokenKind::LParen) {
            self.stream
                .error_at_current("expected '(' to open RDF collection");
            return None;
        }

        // Parse the GraphNode items. Nested `[ … ]` / `( … )` items emit
        // their own triples into `pending_bnpl_triples` as they parse.
        let mut items: Vec<ObjectTerm> = Vec::new();
        while !self.stream.check(&TokenKind::RParen) && !self.stream.is_eof() {
            items.push(self.parse_object()?);
        }
        if !self.stream.match_token(&TokenKind::RParen) {
            self.stream
                .error_at_current("expected ')' to close RDF collection");
            return None;
        }
        let span = start.union(self.stream.previous_span());

        // `( )` lexes as Nil, so an empty item list is unreachable via the
        // grammar (`Collection ::= '(' GraphNode+ ')'`); handle it as the
        // empty list anyway for safety.
        if items.is_empty() {
            return Some(Term::Iri(Iri::rdf_nil(span)));
        }

        // Mint the list-cell blank nodes. `#` is outside PN_CHARS, so a
        // user-written `_:…` label can never collide with these (same
        // scheme as the `#bnpl…` property-list labels).
        let cells: Vec<String> = (0..items.len())
            .map(|_| {
                let label = format!("#coll{}", self.bnode_counter);
                self.bnode_counter += 1;
                label
            })
            .collect();

        for (i, item) in items.into_iter().enumerate() {
            let cell = SubjectTerm::BlankNode(BlankNode::labeled(&cells[i], span));
            let item_span = item.span();
            self.pending_bnpl_triples.push(TriplePattern::new(
                cell.clone(),
                PredicateTerm::Iri(Iri::rdf_first(item_span)),
                item,
                item_span,
            ));
            let rest_object = if i + 1 < cells.len() {
                Term::BlankNode(BlankNode::labeled(&cells[i + 1], span))
            } else {
                Term::Iri(Iri::rdf_nil(span))
            };
            self.pending_bnpl_triples.push(TriplePattern::new(
                cell,
                PredicateTerm::Iri(Iri::rdf_rest(span)),
                rest_object,
                span,
            ));
        }

        Some(Term::BlankNode(BlankNode::labeled(&cells[0], span)))
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

        // SPARQL 1.2 standalone reified triple: `<< s p o ~ r? >> .`
        // (`ReifiedTriple PropertyListPath` with an empty property
        // list). Desugars to `r rdf:reifies <<( s p o )>>`.
        if matches!(&subject, SubjectTerm::QuotedTriple(_)) && !self.is_verb_start() {
            let SubjectTerm::QuotedTriple(qt) = subject else {
                unreachable!("matched QuotedTriple above")
            };
            let target = self.reified_triple_to_annotation_target(qt);
            // Defensive: fold any triples emitted by `[ … ]` terms
            // nested inside the reified triple.
            if !self.pending_bnpl_triples.is_empty() {
                let triples = std::mem::take(&mut self.pending_bnpl_triples);
                let bgp_span = super::span_of_triples(&triples);
                patterns.push(GraphPattern::bgp(triples, bgp_span));
            }
            patterns.push(target);
            if !self.pending_bnpl_patterns.is_empty() {
                patterns.append(&mut self.pending_bnpl_patterns);
            }
            // Optional dot at end
            self.stream.match_token(&TokenKind::Dot);
            return Some(patterns);
        }

        // A bare triple-term subject is NOT a statement: unlike a reified
        // triple (handled just above), `<<( s p o )>> .` with no
        // predicate-object list is invalid — a triple term is only a value
        // (W3C negative `tripleterm-separate-01..06`). Require a verb.
        if matches!(&subject, SubjectTerm::TripleTerm(_)) && !self.is_verb_start() {
            self.stream.error_at_current(
                "a bare triple term is not a statement; a triple term is a value — \
                 give it a predicate-object list to use it as a subject",
            );
            return None;
        }

        // A blank-node property-list or collection subject (`[ :p ?o ] …`,
        // `( ?x ) …`) emitted its inner triples (and, for a path verb inside
        // `[ … ]`, path patterns); fold the triples in before the (optional)
        // predicate-object list. The path patterns are drained at the end of
        // this block, once the BGP is flushed.
        let had_bnpl_subject =
            !self.pending_bnpl_triples.is_empty() || !self.pending_bnpl_patterns.is_empty();
        if !self.pending_bnpl_triples.is_empty() {
            if bgp_start.is_none() {
                bgp_start = Some(subject.span());
            }
            triples.append(&mut self.pending_bnpl_triples);
        }

        // Parse predicate-object list, collecting patterns. It is optional only
        // for a bare blank-node property-list subject (`[ :p ?o ] .`), which has
        // already produced its triples above; any other subject requires a verb
        // (and the normal error path reports a missing one).
        if self.is_verb_start() || !had_bnpl_subject {
            self.parse_predicate_object_list_with_paths(
                &subject,
                &mut triples,
                &mut patterns,
                &mut bgp_start,
            )?;
        }

        // Flush any remaining triples to a BGP
        if !triples.is_empty() {
            let span = bgp_start.unwrap_or(subject.span());
            let end_span = triples.last().map(|t| t.span).unwrap_or(span);
            patterns.push(GraphPattern::Bgp {
                patterns: std::mem::take(&mut triples),
                span: span.union(end_span),
            });
        }

        // Drain path patterns emitted by `[ path obj ]` property lists nested
        // anywhere in this block (subject or object position).
        if !self.pending_bnpl_patterns.is_empty() {
            patterns.append(&mut self.pending_bnpl_patterns);
        }

        // TriplesBlock ::= TriplesSameSubjectPath ( '.' TriplesBlock? )?
        // The '.' separating two same-subject blocks is mandatory: after this
        // block, a new subject term may only follow a consumed '.' (the group
        // loop re-enters `parse_triples_block` for it). The dot itself stays
        // optional before '}' or a GraphPatternNotTriples keyword (V1
        // dot-structure validation, W3C syn-bad-02/03).
        if !self.stream.match_token(&TokenKind::Dot) && self.stream.is_term_start() {
            self.stream
                .error_at_current("expected '.' between triple patterns");
        }

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
            // Parse object. A bare `<<( s p o )>>` for a predicate other
            // than `rdf:reifies` parses as a deferred triple-term value
            // (`Term::TripleTerm`, D-1); the `rdf:reifies` object case is
            // routed to `parse_reifies_object_list` before reaching here.
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

            // A blank-node property-list object (`[ :p ?o ]`) emitted its inner
            // triples into `pending_bnpl_triples`; fold them into this BGP.
            triples.append(&mut self.pending_bnpl_triples);

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

        self.reject_collection_in_quoted_context()?;
        let subject = self.parse_subject()?;
        // The `rdf:reifies` object stays strict per v1 (pr-w2a): its inner
        // subject may not be a nested triple term or reified triple. Now
        // that `parse_subject` accepts `<<(` as a value, guard both variants
        // (bare triple-term values in general BGP positions may nest — that
        // is the separate `parse_triple_term_value` path).
        if matches!(
            subject,
            SubjectTerm::QuotedTriple(_) | SubjectTerm::TripleTerm(_)
        ) {
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
        // Reified triples are not grammatical inside a triple term
        // either (`ttObject` has no `ReifiedTriple` production).
        if self.stream.check(&TokenKind::TripleStart) {
            self.stream.error_at_current(
                "reified triples (<< s p o >>) are not allowed inside a triple term",
            );
            return None;
        }
        self.reject_collection_in_quoted_context()?;
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

    /// Parse a SPARQL 1.2 triple-term *value* `<<( s p o )>>` (BGP
    /// subject/object, `VALUES`, `BIND`). The current token must be
    /// `TripleTermStart`.
    ///
    /// Accept-then-defer (burn-down decision D-1): the node is built for
    /// the syntax surface but lowers to `not_implemented`. The grammar
    /// guardrails of the RDF 1.2 `TripleTerm` production are enforced here
    /// so the negative suite stays rejected:
    /// - **subject**: variable / IRI / blank node, and — in a *pattern*
    ///   context — a nested triple term (positive `nested-tripleterm-02`,
    ///   `compound-tripleterm-subject`). Never a reified triple, a literal
    ///   (rejected by `parse_subject`), a path, or a collection.
    /// - **predicate**: a plain IRI or variable — never a path
    ///   (`quoted-path-tripleterm`), blank node (`bnode-predicate-tripleterm`),
    ///   or collection (`quoted-list-predicate-tripleterm`);
    /// - **object**: any term including a *nested* triple term
    ///   (`nested-tripleterm-*`) — never a reified triple or collection
    ///   (`quoted-list-object-tripleterm`).
    ///
    /// `in_pattern` distinguishes the two contexts the W3C suite treats
    /// differently. In a **value** context (`VALUES` / expression,
    /// `in_pattern == false`) the subject may be neither a blank node
    /// (negative `bindbnode-tripleterm`) nor a nested triple term (negative
    /// `tripleterm-subject-01..03`). In a **pattern** context
    /// (`in_pattern == true`) both are allowed (positive `bnode-tripleterm-*`,
    /// `nested-tripleterm-02`, `compound-tripleterm-subject`).
    ///
    /// A bare `<<( s p o )>> .` is not accepted as a statement (the enclosing
    /// triples-block requires a predicate-object list after the subject), so
    /// `tripleterm-separate-*` stay rejected.
    pub(super) fn parse_triple_term_value(&mut self, in_pattern: bool) -> Option<TripleTerm> {
        let start = self.stream.current_span();
        if !self.stream.match_token(&TokenKind::TripleTermStart) {
            self.stream
                .error_at_current("expected '<<(' to begin triple term");
            return None;
        }

        self.reject_collection_in_quoted_context()?;
        let subject = self.parse_subject()?;
        match &subject {
            // A reified triple is never a valid triple-term subject
            // (`ttSubject` has no `ReifiedTriple` production).
            SubjectTerm::QuotedTriple(_) => {
                self.stream.error_at_current(
                    "a reified triple (<< s p o >>) may not be the subject of a triple term",
                );
                return None;
            }
            // Only the object of a triple term may nest when the term is a
            // *value*: `<<( <<( … )>> :q :z )>>` is rejected in VALUES/BIND
            // (`tripleterm-subject-01..03`) but accepted in a triple pattern.
            SubjectTerm::TripleTerm(_) if !in_pattern => {
                self.stream.error_at_current(
                    "a triple term used as a value may not have a triple-term subject",
                );
                return None;
            }
            SubjectTerm::BlankNode(_) if !in_pattern => {
                self.stream.error_at_current(
                    "blank nodes are not allowed in a triple term used as a value",
                );
                return None;
            }
            _ => {}
        }

        let predicate = self.parse_simple_predicate()?;

        // A reified triple is not grammatical inside a triple term
        // (`ttObject` has no `ReifiedTriple` production); a nested triple
        // term *is* (handled by `parse_object` → `Term::TripleTerm`).
        if self.stream.check(&TokenKind::TripleStart) {
            self.stream.error_at_current(
                "reified triples (<< s p o >>) are not allowed inside a triple term",
            );
            return None;
        }
        self.reject_collection_in_quoted_context()?;
        let object = self.parse_object()?;
        if !in_pattern && matches!(object, Term::BlankNode(_)) {
            self.stream
                .error_at_current("blank nodes are not allowed in a triple term used as a value");
            return None;
        }

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
            triple_term: Box::new(triple_term),
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
    /// Returns `Ok(None)` when no tail is present.
    ///
    /// Elements group into [`AnnotationUnit`]s per the RDF 1.2
    /// attachment rule: each `~ reifier` starts a new unit; an
    /// annotation block attaches to an immediately preceding reifier
    /// element, otherwise it starts a fresh (anonymous-reifier) unit.
    /// So `~ :r1 {| … |} ~ :r2 {| … |}` is two units and
    /// `{| … |} {| … |}` is two units with two fresh reifiers.
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
        let mut units: Vec<AnnotationUnit> = Vec::new();
        // True while the last element parsed was a `~ reifier` that has
        // not yet received a block — the only position a block attaches
        // to instead of minting a fresh reifier.
        let mut last_was_reifier = false;
        let mut last_span = start;

        loop {
            if self.stream.check(&TokenKind::Tilde) {
                let r_span = self.stream.current_span();
                self.stream.advance(); // consume `~`
                let r = self.parse_reifier_id_after_tilde();
                let span = r.as_ref().map(ReifierId::span).unwrap_or(r_span);
                last_span = span;
                units.push(AnnotationUnit {
                    reifier: r,
                    block: None,
                    span: r_span.union(span),
                });
                last_was_reifier = true;
            } else if self.stream.check(&TokenKind::AnnotationOpen) {
                let b = self.parse_annotation_block()?;
                last_span = b.span;
                if last_was_reifier {
                    // Attach to the reifier element just parsed.
                    let unit = units.last_mut().expect("reifier unit exists");
                    unit.span = unit.span.union(b.span);
                    unit.block = Some(b);
                } else {
                    // Block with no immediately preceding reifier mints
                    // a fresh one.
                    units.push(AnnotationUnit {
                        reifier: None,
                        span: b.span,
                        block: Some(b),
                    });
                }
                last_was_reifier = false;
            } else {
                break;
            }
        }

        let span = start.union(last_span);
        Some(Some(Annotation { units, span }))
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

    /// Parse a `{| propertyListPathNotEmpty |}` annotation block.
    ///
    /// Each entry is a verb-object pair applied to the enclosing
    /// reifier; verbs may be property paths (`{| :r/:q 'ABC' |}`, W3C
    /// `annotation-*reifier-06`). Nested annotation tails on body
    /// entries are illegal per the RDF 1.2 grammar and rejected here
    /// (W3C negative `nested-annotated-path-*`).
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
                let verb = match self.parse_verb()? {
                    Verb::Simple(p) => AnnotationVerb::Simple(p),
                    Verb::Path(p) => AnnotationVerb::Path(p),
                };
                loop {
                    // A `<<( s p o )>>` object inside an annotation block
                    // (e.g. `{| ?Y <<(:s1 :p1 ?Z)>> |}`, `update-tripleterm-04`)
                    // parses as a deferred triple-term value; lowering
                    // rejects it (D-1).
                    let object = self.parse_object()?;

                    // Reject nested annotation tails: the RDF 1.2
                    // annotation-block body is a plain property list —
                    // annotations-on-annotations are not grammatical.
                    if self.stream.check(&TokenKind::Tilde)
                        || self.stream.check(&TokenKind::AnnotationOpen)
                    {
                        self.stream.error_at_current(
                            "annotations on annotation-block entries are not supported \
                             in v1 (annotations-on-annotations are deferred)",
                        );
                        return None;
                    }

                    let span = verb.span().union(object.span());
                    entries.push(AnnotationEntry {
                        verb: verb.clone(),
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

            // A blank-node property-list object emitted its inner triples; flush
            // them as their own BGP alongside the path pattern. (Same-group
            // BGPs are re-merged by the group-pattern loop, so this does not
            // introduce a join-scope boundary.)
            if !self.pending_bnpl_triples.is_empty() {
                let triples = std::mem::take(&mut self.pending_bnpl_triples);
                let bgp_span = super::span_of_triples(&triples);
                patterns.push(GraphPattern::bgp(triples, bgp_span));
            }

            // A `[ path obj ]` nested in the object emitted path patterns.
            if !self.pending_bnpl_patterns.is_empty() {
                patterns.append(&mut self.pending_bnpl_patterns);
            }

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

            // Parse object list. A bare `<<( s p o )>>` object parses as a
            // deferred triple-term value here too (CONSTRUCT / INSERT /
            // DELETE templates — `basic-tripleterm-06/07`,
            // `update-tripleterm-*`); lowering rejects it (D-1).
            loop {
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

                // A blank-node property-list object (`[ :p ?o ]`) in a CONSTRUCT /
                // INSERT/DELETE template emitted its inner triples into
                // `pending_bnpl_triples`; fold them into this template so they are
                // not dropped (and cannot leak into a later WHERE BGP).
                triples.append(&mut self.pending_bnpl_triples);

                // Property paths are not legal in templates (`ConstructTriples`
                // has no VerbPath); reject a `[ path obj ]` rather than
                // silently dropping its pattern.
                if !self.pending_bnpl_patterns.is_empty() {
                    self.pending_bnpl_patterns.clear();
                    self.stream.error_at_current(
                        "property paths inside a blank-node property list are not \
                         allowed in CONSTRUCT/UPDATE templates",
                    );
                    return None;
                }

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

    /// Parse one template `subject predicate-object-list` group, folding in any
    /// blank-node property-list triples the subject itself produced. The
    /// predicate-object list is optional only for a bare blank-node
    /// property-list subject (`[ :p ?o ]`), which already emitted its triples.
    /// Shared by the CONSTRUCT template and the INSERT/DELETE template parsers.
    pub(super) fn parse_template_triples_for_subject(
        &mut self,
        subject: &SubjectTerm,
        triples: &mut Vec<TriplePattern>,
    ) -> Option<()> {
        // Property paths are not legal in templates; reject a `[ path obj ]`
        // subject rather than silently dropping its pattern.
        if !self.pending_bnpl_patterns.is_empty() {
            self.pending_bnpl_patterns.clear();
            self.stream.error_at_current(
                "property paths inside a blank-node property list are not \
                 allowed in CONSTRUCT/UPDATE templates",
            );
            return None;
        }
        let had_bnpl_subject = !self.pending_bnpl_triples.is_empty();
        if had_bnpl_subject {
            triples.append(&mut self.pending_bnpl_triples);
        }
        if self.stream.is_term_start()
            || self.stream.check_keyword(TokenKind::KwA)
            || !had_bnpl_subject
        {
            self.parse_construct_predicate_object_list(subject, triples)?;
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
