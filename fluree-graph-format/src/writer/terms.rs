//! The term table behind a writing sink, and how a term reaches the output
//!
//! Writers need what the collecting sinks in `fluree-graph-ir` need — a
//! [`TermId`] → [`Term`] table with stable labelled-blank identity and
//! statement-scoped literal recycling — but that crate's table is private, and
//! a writer's is not quite the same table anyway: blank labels are rewritten
//! by [`BlankLabeler`] on the way *in*, so the term a writer stores is already
//! the term it will emit.

use super::blank::BlankLabeler;
use super::Deferred;
use crate::escape::{write_escaped_iri, write_escaped_ntriples_string};
use crate::prefix::{write_turtle_iri, PrefixMap};
use fluree_graph_ir::{Datatype, LiteralValue, Term, TermId};
use fluree_vocab::rdf;
use std::collections::HashMap;
use std::io::{self, Write};

/// Terms a writer has been handed, in the two lifetime classes the
/// [`GraphSink`](fluree_graph_ir::GraphSink) protocol defines.
///
/// # Memory
///
/// Literal slots recycle at every statement boundary, so their high-water mark
/// is the widest single statement. IRI and blank ids are session-scoped by
/// contract — producers cache them across statements — so the table holds one
/// entry per distinct IRI, one per distinct labelled blank node, and one per
/// *anonymous* blank node. The last of those is the only unbounded term, and
/// it is unbounded in the collecting sinks too; statement-scoping anonymous
/// blanks would be a protocol change, not a writer change.
#[derive(Debug)]
pub(crate) struct WriterTerms {
    terms: Vec<Term>,
    labeler: BlankLabeler,
    /// Input blank label → the id holding its (already rewritten) term.
    blank_ids: HashMap<Box<str>, TermId>,
    /// Slots in `terms` that hold literals, in mint order.
    literal_slots: Vec<u32>,
    /// How far into `literal_slots` the statement in flight has consumed.
    literal_cursor: usize,
}

impl WriterTerms {
    pub(crate) fn new(labeler: BlankLabeler) -> Self {
        Self {
            terms: Vec::new(),
            labeler,
            blank_ids: HashMap::new(),
            literal_slots: Vec::new(),
            literal_cursor: 0,
        }
    }

    pub(crate) fn get(&self, id: TermId) -> &Term {
        &self.terms[id.index() as usize]
    }

    pub(crate) fn iri(&mut self, iri: &str) -> TermId {
        self.push(Term::iri(iri))
    }

    /// Intern a blank node under the writer's labelling policy.
    ///
    /// A policy that refuses the label (see [`BlankNodeLabels::Preserve`](super::BlankNodeLabels))
    /// still has to return an id, because `term_blank` is infallible: the
    /// refusal is stashed and raised by the next emission, before anything the
    /// bad label would have appeared in reaches the output.
    pub(crate) fn blank(&mut self, label: Option<&str>, deferred: &mut Deferred) -> TermId {
        let Some(label) = label else {
            let minted = self.labeler.anonymous();
            return self.push(Term::blank(minted));
        };
        if let Some(&id) = self.blank_ids.get(label) {
            return id;
        }
        let output = match self.labeler.labelled(label) {
            Ok(output) => output.to_string(),
            Err(e) => {
                deferred.set(e);
                // A placeholder that is never written: the next emit fails,
                // and every emission after it fails too.
                label.to_string()
            }
        };
        let id = self.push(Term::blank(output));
        self.blank_ids.insert(label.into(), id);
        id
    }

    pub(crate) fn literal(
        &mut self,
        value: &str,
        datatype: Datatype,
        language: Option<&str>,
    ) -> TermId {
        let term = match language {
            Some(lang) => Term::lang_string(value, lang),
            None if datatype.is_xsd_string() => Term::string(value),
            None => Term::typed(value, datatype),
        };
        self.push_literal(term)
    }

    pub(crate) fn literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.push_literal(Term::Literal {
            value,
            datatype,
            language: None,
        })
    }

    /// Retire the statement's literal slots. Called at both outcomes, commit
    /// and abort, because the slots are dead either way.
    pub(crate) fn end_statement(&mut self) {
        self.literal_cursor = 0;
    }

    fn push(&mut self, term: Term) -> TermId {
        let id = TermId::new(self.terms.len() as u32);
        self.terms.push(term);
        id
    }

    fn push_literal(&mut self, term: Term) -> TermId {
        if let Some(&slot) = self.literal_slots.get(self.literal_cursor) {
            self.literal_cursor += 1;
            self.terms[slot as usize] = term;
            return TermId::new(slot);
        }
        let id = self.push(term);
        self.literal_slots.push(id.index());
        self.literal_cursor = self.literal_slots.len();
        id
    }
}

/// Write a term in N-Triples/N-Quads syntax: `<iri>`, `_:label`, or a quoted
/// literal.
pub(crate) fn write_nt_term<W: Write>(w: &mut W, term: &Term) -> io::Result<()> {
    match term {
        Term::Iri(iri) => {
            w.write_all(b"<")?;
            write_escaped_iri(w, iri)?;
            w.write_all(b">")
        }
        Term::BlankNode(id) => {
            w.write_all(b"_:")?;
            w.write_all(id.as_str().as_bytes())
        }
        Term::Literal { .. } => write_literal(w, term),
    }
}

/// Write a term in Turtle/TriG syntax — as [`write_nt_term`], except that an
/// IRI may compact to a prefixed name.
///
/// Literals are written in their fully quoted, explicitly typed form: this
/// writer never uses Turtle's `42` / `true` / `1.0` shorthands. That is what
/// makes lexical round-tripping exact — `"+1"^^xsd:integer` reads back as the
/// lexical form `+1`, where a bare `+1` would be re-canonicalized by a
/// consumer parsing in its default mode — and it sidesteps the `xsd:decimal`
/// versus `xsd:double` spelling trap entirely.
pub(crate) fn write_ttl_term<W: Write>(
    w: &mut W,
    term: &Term,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    match term {
        Term::Iri(iri) => write_turtle_iri(w, iri, prefixes),
        Term::BlankNode(id) => {
            w.write_all(b"_:")?;
            w.write_all(id.as_str().as_bytes())
        }
        Term::Literal { .. } => write_literal(w, term),
    }
}

/// Write a Turtle/N-Triples predicate, using the `a` keyword for `rdf:type`.
pub(crate) fn write_ttl_predicate<W: Write>(
    w: &mut W,
    term: &Term,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    if term.as_iri() == Some(rdf::TYPE) {
        return w.write_all(b"a");
    }
    write_ttl_term(w, term, prefixes)
}

/// Write a literal. The grammar is identical in all four text syntaxes.
fn write_literal<W: Write>(w: &mut W, term: &Term) -> io::Result<()> {
    let Term::Literal {
        value,
        datatype,
        language,
    } = term
    else {
        return write_nt_term(w, term);
    };

    w.write_all(b"\"")?;
    match value {
        // The common case, borrowed rather than re-rendered.
        LiteralValue::String(s) => write_escaped_ntriples_string(w, s)?,
        other => write_escaped_ntriples_string(w, &other.lexical())?,
    }
    w.write_all(b"\"")?;

    if let Some(lang) = language {
        w.write_all(b"@")?;
        return w.write_all(lang.as_bytes());
    }
    if datatype.is_xsd_string() {
        // A plain quoted literal already means xsd:string; spelling it out
        // would be noise, and N-Triples canonical form omits it.
        return Ok(());
    }
    w.write_all(b"^^<")?;
    write_escaped_iri(w, datatype.as_iri())?;
    w.write_all(b">")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::BlankNodeLabels;

    fn rendered(term: &Term) -> String {
        let mut buf = Vec::new();
        write_nt_term(&mut buf, term).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn terms_render_in_ntriples_syntax() {
        assert_eq!(
            rendered(&Term::iri("http://example.org/a")),
            "<http://example.org/a>"
        );
        assert_eq!(rendered(&Term::blank("b1")), "_:b1");
        assert_eq!(rendered(&Term::string("plain")), "\"plain\"");
        assert_eq!(
            rendered(&Term::lang_string("bonjour", "fr")),
            "\"bonjour\"@fr"
        );
        assert_eq!(
            rendered(&Term::integer(42)),
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
        // Doubles use the canonical xsd:double lexical form.
        assert_eq!(
            rendered(&Term::double(1_000_000.0)),
            "\"1.0E6\"^^<http://www.w3.org/2001/XMLSchema#double>"
        );
    }

    #[test]
    fn literal_escaping_covers_quotes_newlines_and_controls() {
        assert_eq!(
            rendered(&Term::string("a\"b\nc\\d\te\u{0}")),
            "\"a\\\"b\\nc\\\\d\\te\\u0000\""
        );
    }

    #[test]
    fn turtle_compacts_iris_and_abbreviates_rdf_type() {
        let mut prefixes = PrefixMap::new();
        prefixes.insert("ex", "http://example.org/");

        let mut buf = Vec::new();
        write_ttl_term(&mut buf, &Term::iri("http://example.org/a"), &prefixes).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "ex:a");

        let mut buf = Vec::new();
        write_ttl_predicate(&mut buf, &Term::iri(rdf::TYPE), &prefixes).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "a");

        // Everything else is an ordinary predicate.
        let mut buf = Vec::new();
        write_ttl_predicate(&mut buf, &Term::iri("http://example.org/p"), &prefixes).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "ex:p");
    }

    #[test]
    fn literal_slots_recycle_but_iris_and_blanks_do_not() {
        let mut terms = WriterTerms::new(BlankLabeler::new(BlankNodeLabels::Relabel));
        let mut deferred = Deferred::default();

        let iri = terms.iri("http://example.org/s");
        let blank = terms.blank(Some("x"), &mut deferred);
        let a = terms.literal("a", Datatype::xsd_string(), None);
        let b = terms.literal("b", Datatype::xsd_string(), None);
        assert_ne!(a, b);

        terms.end_statement();

        let c = terms.literal("c", Datatype::xsd_string(), None);
        let d = terms.literal("d", Datatype::xsd_string(), None);
        assert_eq!((a, b), (c, d), "the next statement reuses the slots");

        // Session-scoped ids still resolve to what they were.
        assert_eq!(terms.get(iri).as_iri(), Some("http://example.org/s"));
        assert_eq!(terms.get(blank).as_blank().unwrap().as_str(), "b1");
        assert_eq!(
            terms.blank(Some("x"), &mut deferred),
            blank,
            "one id per input label"
        );
        assert!(deferred.check().is_ok());
    }

    /// `term_blank` cannot fail, so a refused label is stashed for the next
    /// emission to raise. What matters is that it is raised at all — the
    /// alternative is a silently merged node.
    #[test]
    fn a_refused_label_is_stashed_rather_than_swallowed() {
        let mut terms = WriterTerms::new(BlankLabeler::new(BlankNodeLabels::Preserve));
        let mut deferred = Deferred::default();
        terms.blank(Some("fdbw-1"), &mut deferred);
        let err = deferred
            .check()
            .expect_err("the refusal must survive term_blank");
        assert!(err.to_string().contains("reserves"), "{err}");

        // And it is sticky: a producer that ignores one refusal does not get
        // the bad label written on its next attempt.
        let err = deferred.check().expect_err("the refusal latches");
        assert!(err.to_string().contains("already refused"), "{err}");
    }
}
