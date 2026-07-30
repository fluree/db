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
use fluree_graph_ir::{Datatype, LiteralValue, Term, TermId, TermScope};
use fluree_vocab::rdf;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;

/// Terms a writer has been handed, in the two lifetime classes the
/// [`GraphSink`](fluree_graph_ir::GraphSink) protocol defines.
///
/// # Memory
///
/// Literal slots recycle at every statement boundary, so their high-water mark
/// is the widest single statement. IRI and blank ids are session-scoped by
/// contract — producers cache them across statements — so the table holds one
/// entry per distinct IRI, one per distinct labelled blank node, and one per
/// *anonymous* blank node.
///
/// That contract is written for the producer that needs the most. A producer
/// that caches nothing can say so with
/// [`declare_term_scope`](fluree_graph_ir::GraphSink::declare_term_scope), and
/// then everything except a labelled blank recycles with the literals: the
/// table's high-water mark becomes the widest statement rather than the
/// document. Measured on a 4M-statement N-Triples document, that is the
/// difference between 814 MiB and three slots.
///
/// Labelled blanks are the exception under either scope, and not for storage
/// reasons: [`BlankLabeler`] mints a *fresh* output label per call, so the
/// mapping from input label to output label lives here or nowhere, and
/// `_:x` in the first statement and `_:x` in the last must come out as the
/// same node. Their slots are session-scoped even under
/// [`TermScope::Statement`].
#[derive(Debug)]
pub(crate) struct WriterTerms {
    /// Session-scoped slots. Everything under [`TermScope::Session`]; only
    /// labelled blanks under [`TermScope::Statement`]. Addressed by ids
    /// carrying [`SESSION_TAG`].
    session: Vec<Term>,
    /// Statement-scoped slots, reused from the start at every statement
    /// boundary. Empty unless the producer declared [`TermScope::Statement`].
    scoped: Vec<Term>,
    /// How far into `scoped` the statement in flight has consumed.
    scoped_cursor: usize,
    /// What the producer promised. Nothing recycles beyond literals until it
    /// promises something.
    scope: TermScope,
    labeler: BlankLabeler,
    /// Input blank label → the id holding its (already rewritten) term.
    blank_ids: HashMap<Box<str>, TermId>,
    /// Slots in `session` that hold literals, in mint order. Unused under
    /// [`TermScope::Statement`], where literals recycle with everything else.
    literal_slots: Vec<u32>,
    /// How far into `literal_slots` the statement in flight has consumed.
    literal_cursor: usize,
    /// Which statement is in flight. Debug builds only, and stamped into each
    /// statement-scoped id as it is minted — see [`GEN_SHIFT`].
    #[cfg(debug_assertions)]
    generation: u32,
}

/// Marks an id as addressing the session region rather than the recycled one.
///
/// The top bit, which no real index can reach: the parser refuses documents
/// past 4 GiB and every term needs at least one byte, so a table cannot hold
/// 2^31 entries in the first place. That argument covers the parser and not
/// every possible producer, so [`WriterTerms::push_session`] asserts it rather
/// than assuming it.
const SESSION_TAG: u32 = 1 << 31;

/// Debug-only: which statement a statement-scoped id was minted in, carried in
/// the ID rather than beside the slot.
///
/// Stamping the slot caught only half the violation it was written for. A
/// producer that caches an id across `end_statement` is detectable when the
/// next statement is NARROWER — the slot keeps its old stamp and the read
/// fails. When the next statement is at least as wide, which is the common
/// case, the slot has already been re-minted and carries the current stamp, so
/// the stale id read as fresh and the writer emitted the wrong term with no
/// complaint. The reviewer's probe named it exactly: `SUBJECT-TWO` written
/// where the producer meant `SUBJECT-ONE`.
///
/// Moving the stamp onto the id fixes that, because a stale id carries the
/// generation it was minted in no matter what has happened to its slot since.
/// Seven bits wrap every 128 statements, so an id cached across exactly a
/// multiple of 128 statements still slips through; that is a debug-build
/// heuristic catching an API misuse, not a soundness mechanism, and 24 bits of
/// index (16.7M slots for the widest single statement) is the more valuable
/// half of the split.
#[cfg(debug_assertions)]
const GEN_SHIFT: u32 = 24;
#[cfg(debug_assertions)]
const GEN_MASK: u32 = 0x7F << GEN_SHIFT;
#[cfg(debug_assertions)]
const INDEX_MASK: u32 = (1 << GEN_SHIFT) - 1;

impl WriterTerms {
    pub(crate) fn new(labeler: BlankLabeler) -> Self {
        Self {
            session: Vec::new(),
            scoped: Vec::new(),
            scoped_cursor: 0,
            scope: TermScope::Session,
            labeler,
            blank_ids: HashMap::new(),
            literal_slots: Vec::new(),
            literal_cursor: 0,
            #[cfg(debug_assertions)]
            generation: 0,
        }
    }

    /// Record the producer's declaration.
    ///
    /// Re-declaring the scope already in force is a no-op at any point, and
    /// that is not a leniency — it is a shape the protocol has to support.
    /// `--continue-on-error` re-enters the reader at every resync point, so a
    /// recovering line-format run declares statement scope once per surviving
    /// fragment, against a sink that is already holding terms. Asserting on the
    /// second declaration turned a legitimate producer into a debug-build
    /// panic; the integration found it, not the unit tests, because nothing on
    /// either branch alone drove a producer that re-enters.
    ///
    /// What stays refused is a CHANGE of scope once terms exist. That one would
    /// invalidate ids already handed out — the whole hazard this declaration
    /// exists to keep on the producer's side of the line — so it fails loudly
    /// in debug builds and is ignored in release, which keeps the safe scope
    /// rather than adopting the unsafe one.
    pub(crate) fn set_scope(&mut self, scope: TermScope) {
        if scope == self.scope {
            return;
        }
        debug_assert!(
            self.session.is_empty() && self.scoped.is_empty(),
            "term scope CHANGED to {scope:?} after {} term(s) were already minted \
             — ids handed out under the old scope would be invalidated",
            self.session.len() + self.scoped.len(),
        );
        if self.session.is_empty() && self.scoped.is_empty() {
            self.scope = scope;
        }
    }

    pub(crate) fn get(&self, id: TermId) -> &Term {
        let raw = id.index();
        if raw & SESSION_TAG != 0 {
            return &self.session[(raw & !SESSION_TAG) as usize];
        }
        #[cfg(debug_assertions)]
        {
            let stamped = (raw & GEN_MASK) >> GEN_SHIFT;
            debug_assert_eq!(
                stamped,
                self.generation & 0x7F,
                "term id {} was minted in statement {} and read in statement {} — the \
                 producer declared TermScope::Statement and then cached an id across \
                 end_statement(), so this read resolves to whatever later term took \
                 the slot",
                raw & INDEX_MASK,
                stamped,
                self.generation & 0x7F,
            );
            return &self.scoped[(raw & INDEX_MASK) as usize];
        }
        #[cfg(not(debug_assertions))]
        &self.scoped[raw as usize]
    }

    pub(crate) fn iri(&mut self, iri: &str) -> TermId {
        self.push_scoped_or_session(Term::iri(iri))
    }

    /// Store an IRI the producer is already holding, without a second copy.
    ///
    /// `Term::Iri` is an `Arc<str>`, and so is a caching parser's cache key, so
    /// this is a refcount bump where [`Self::iri`] is an allocation and a
    /// memcpy. The stored term outlives the producer's cache entry if it has
    /// to — that is what the `Arc` is for.
    pub(crate) fn iri_shared(&mut self, iri: &Arc<str>) -> TermId {
        self.push_scoped_or_session(Term::Iri(Arc::clone(iri)))
    }

    /// Intern a blank node under the writer's labelling policy.
    ///
    /// A policy that refuses the label (see [`BlankNodeLabels::Preserve`](super::BlankNodeLabels))
    /// still has to return an id, because `term_blank` is infallible: the
    /// refusal is stashed and raised by the next emission, before anything the
    /// bad label would have appeared in reaches the output.
    pub(crate) fn blank(&mut self, label: Option<&str>, deferred: &mut Deferred) -> TermId {
        let Some(label) = label else {
            // An anonymous node is named nowhere else, so nothing can refer to
            // it after the statement that introduced it — it recycles with the
            // rest under a statement-scoped producer.
            let minted = self.labeler.anonymous();
            return self.push_scoped_or_session(Term::blank(minted));
        };
        if let Some(&id) = self.blank_ids.get(label) {
            return id;
        }
        let output = match self.labeler.labelled(label) {
            Ok(output) => output,
            Err(e) => {
                deferred.stash_refusal(e);
                // A placeholder that is never written: the next emit fails,
                // and every emission after it fails too.
                label.into()
            }
        };
        // Session-scoped under either scope; see the type docs. The labeler
        // mints a fresh label per call, so this map is the only record that
        // two occurrences of `_:x` are one node.
        let id = self.push_session(Term::blank(output));
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

    /// Retire the statement's slots. Called at both outcomes, commit and
    /// abort, because the slots are dead either way.
    pub(crate) fn end_statement(&mut self) {
        self.literal_cursor = 0;
        self.scoped_cursor = 0;
        #[cfg(debug_assertions)]
        {
            self.generation += 1;
        }
    }

    /// Put a term where this producer's scope says it belongs.
    fn push_scoped_or_session(&mut self, term: Term) -> TermId {
        match self.scope {
            TermScope::Session => self.push_session(term),
            TermScope::Statement => self.push_scoped(term),
        }
    }

    /// Session region: grows for the document, addressed with the tag bit.
    fn push_session(&mut self, term: Term) -> TermId {
        // The "no table can hold 2^31 entries" argument is about the parser's
        // 4 GiB input cap, and this table serves producers that are not the
        // parser. Assert it rather than inherit it: past this point the tag bit
        // would collide with a real index and session ids would start reading
        // as statement-scoped ones.
        debug_assert!(
            (self.session.len() as u32) < SESSION_TAG,
            "session term table reached {} entries, where the tag bit stops being \
             free — ids from here would alias the statement-scoped region",
            self.session.len(),
        );
        let id = TermId::new(self.session.len() as u32 | SESSION_TAG);
        self.session.push(term);
        id
    }

    /// Statement region: reuse the slot this statement's Nth term used last
    /// time, or extend by one the first time a statement is this wide.
    fn push_scoped(&mut self, term: Term) -> TermId {
        let slot = self.scoped_cursor;
        self.scoped_cursor += 1;
        if slot < self.scoped.len() {
            self.scoped[slot] = term;
        } else {
            self.scoped.push(term);
        }
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                slot as u32 <= INDEX_MASK,
                "statement-scoped slot {slot} does not fit in {GEN_SHIFT} bits of index — \
                 a single statement this wide needs a different id layout",
            );
            return TermId::new(slot as u32 | ((self.generation & 0x7F) << GEN_SHIFT));
        }
        #[cfg(not(debug_assertions))]
        TermId::new(slot as u32)
    }

    fn push_literal(&mut self, term: Term) -> TermId {
        if self.scope == TermScope::Statement {
            return self.push_scoped(term);
        }
        if let Some(&slot) = self.literal_slots.get(self.literal_cursor) {
            self.literal_cursor += 1;
            self.session[slot as usize] = term;
            return TermId::new(slot | SESSION_TAG);
        }
        let id = self.push_session(term);
        self.literal_slots.push(id.index() & !SESSION_TAG);
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

    fn table(scope: TermScope) -> WriterTerms {
        let mut terms = WriterTerms::new(BlankLabeler::new(BlankNodeLabels::Relabel));
        terms.set_scope(scope);
        terms
    }

    /// How many slots the table is holding, both regions.
    fn slots(terms: &WriterTerms) -> (usize, usize) {
        (terms.session.len(), terms.scoped.len())
    }

    #[test]
    fn a_statement_scoped_producer_costs_the_widest_statement_not_the_document() {
        // The defect this exists for: one slot per term OCCURRENCE, held for
        // the whole document. On a 4M-statement N-Triples corpus that was
        // 814 MiB of table for a document whose widest statement needs three
        // slots. Before the scope declaration, `scoped` did not exist and
        // `session` here grew to 3000 — this assertion is the pin.
        let mut terms = table(TermScope::Statement);
        for i in 0..1000 {
            terms.iri(&format!("http://ex/s{i}"));
            terms.iri("http://ex/p");
            terms.literal(&format!("v{i}"), Datatype::xsd_string(), None);
            terms.end_statement();
        }
        assert_eq!(
            slots(&terms),
            (0, 3),
            "1000 statements of three terms should leave three slots, not three thousand"
        );

        // A wider statement extends the table exactly once, and the table then
        // holds the high-water mark rather than the last statement.
        for i in 0..5 {
            terms.iri(&format!("http://ex/wide{i}"));
        }
        terms.end_statement();
        terms.iri("http://ex/narrow");
        terms.end_statement();
        assert_eq!(
            slots(&terms),
            (0, 5),
            "the table holds the widest statement"
        );
    }

    #[test]
    fn a_producer_that_re_enters_may_re_declare_the_scope_it_already_declared() {
        // `--continue-on-error` re-enters the reader at every resync point, so
        // a recovering line-format run declares statement scope once per
        // surviving fragment — against a sink that is already holding terms
        // from the fragments before it. This is the shape that turned an
        // over-strict assertion into a debug-build panic on a legitimate run,
        // and neither branch's tests drove it alone: the writers had no
        // recovering producer, and recovery had no declaring reader until the
        // two met.
        let mut terms = table(TermScope::Statement);
        for _ in 0..5 {
            terms.set_scope(TermScope::Statement);
            terms.iri("http://ex/s");
            terms.iri("http://ex/p");
            terms.literal("v", Datatype::xsd_string(), None);
            terms.end_statement();
        }
        assert_eq!(
            slots(&terms),
            (0, 3),
            "re-declaring the scope in force must not disturb the table"
        );
    }

    #[test]
    fn a_shared_iri_is_stored_without_copying_its_bytes() {
        // The whole point: what the table holds is the producer's allocation,
        // not a duplicate of it. `Arc::ptr_eq` is the only way to see the
        // difference — a copy compares equal by value and costs a malloc plus a
        // memcpy per distinct IRI.
        let mut terms = table(TermScope::Session);
        let shared: Arc<str> = Arc::from("http://example.org/some/iri");

        let id = terms.iri_shared(&shared);
        let Term::Iri(stored) = terms.get(id) else {
            panic!("stored term is not an IRI");
        };
        assert!(
            Arc::ptr_eq(stored, &shared),
            "the table copied the string instead of sharing the allocation"
        );

        // The copying entry point still exists and still copies — it is what a
        // producer with no `Arc` to offer must use.
        let copied = terms.iri("http://example.org/some/iri");
        let Term::Iri(copy) = terms.get(copied) else {
            panic!("stored term is not an IRI");
        };
        assert_eq!(&**copy, &*shared, "same bytes");
        assert!(
            !Arc::ptr_eq(copy, &shared),
            "iri() must not somehow alias the caller's allocation"
        );
    }

    #[test]
    fn a_labelled_blank_keeps_one_identity_across_statements_even_when_recycling() {
        // The exception to recycling, and the reason it is an exception:
        // `BlankLabeler` mints a FRESH output label per call, so if the id for
        // `_:x` were recycled and re-minted, the second occurrence would come
        // out as a different node. Same input label, same id, same rendering.
        let mut terms = table(TermScope::Statement);
        let mut deferred = Deferred::default();

        let first = terms.blank(Some("x"), &mut deferred);
        let first_label = rendered(terms.get(first));
        terms.iri("http://ex/p");
        terms.end_statement();

        for _ in 0..100 {
            terms.iri("http://ex/filler");
            terms.end_statement();
        }

        let later = terms.blank(Some("x"), &mut deferred);
        assert_eq!(
            first, later,
            "`_:x` must be one node for the whole document"
        );
        assert_eq!(
            rendered(terms.get(later)),
            first_label,
            "the label a recycled table hands back must not drift"
        );
        // And a DIFFERENT label is still a different node.
        let other = terms.blank(Some("y"), &mut deferred);
        assert_ne!(first, other);
        assert_ne!(rendered(terms.get(other)), first_label);
    }

    #[test]
    fn without_a_declaration_ids_stay_valid_for_the_session() {
        // The Turtle parser caches term ids across statements, so the default
        // must keep every non-literal slot alive. If this ever starts
        // recycling, a cached subject id silently starts naming another term.
        let mut terms = table(TermScope::Session);
        let subject = terms.iri("http://ex/s");
        let rendering = rendered(terms.get(subject));
        for i in 0..500 {
            terms.iri(&format!("http://ex/p{i}"));
            terms.literal(&format!("v{i}"), Datatype::xsd_string(), None);
            terms.end_statement();
        }
        assert_eq!(
            rendered(terms.get(subject)),
            rendering,
            "a session-scoped id must still name the term it was minted for"
        );
        // Literals still recycle under the session scope — that part of the
        // contract is unchanged, and it is what keeps `session` at one slot
        // per distinct IRI plus one per literal POSITION.
        assert_eq!(slots(&terms).1, 0, "nothing should reach the scoped region");
        assert!(
            slots(&terms).0 <= 502,
            "literals must reuse their slot: {:?}",
            slots(&terms)
        );
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

    /// Read a one-triple document back, returning the object's lexical form,
    /// or `None` if it does not parse.
    fn reread_object(object: &str) -> Option<String> {
        use fluree_graph_ir::GraphCollectorSink;

        let document = format!("<http://ex/s> <http://ex/p> {object} .");
        let mut sink = GraphCollectorSink::new();
        // Conformant mode, not the ingest default: the ingest default
        // canonicalizes numeric literals, so a numeric added to the hostile
        // set below would silently stop testing what this means to test.
        fluree_graph_turtle::parse_with_options(
            &document,
            &mut sink,
            fluree_graph_turtle::ParserOptions::conformant(),
        )
        .ok()?;
        match &sink.into_graph().iter().next().expect("one triple").o {
            Term::Literal { value, .. } => Some(value.lexical()),
            other => panic!("expected a literal, got {other:?}"),
        }
    }

    /// `Term`'s `Display` is a debugging convenience: it writes lexical forms
    /// with **no escaping whatsoever**. Nothing in a writer output path may
    /// use it. Everything goes through [`write_nt_term`]/[`write_ttl_term`],
    /// which escape.
    ///
    /// This is the regression guard. For every hostile literal the two forms
    /// must differ, and the writer's must read back as the exact value it
    /// started as.
    #[test]
    fn the_writer_escapes_where_display_does_not() {
        for value in [
            "a\"b",
            "a\\b",
            "a\nb",
            "line one\nline two\r\nline three",
            "tab\there",
            "\"leading and trailing\"",
            "nul\u{0}inside",
            "backslash at end\\",
        ] {
            let term = Term::string(value);
            let written = rendered(&term);
            assert_ne!(
                written,
                term.to_string(),
                "Display matched the writer for {value:?} — if Display ever starts \
                 escaping, this is the wrong guard"
            );
            assert_eq!(
                reread_object(&written).as_deref(),
                Some(value),
                "the writer's own output must read back unchanged: {written}"
            );
        }
    }

    /// *Why* the ban exists, demonstrated rather than asserted.
    ///
    /// Two distinct failures, and the second is the dangerous one:
    ///
    /// - `a"b` and a literal newline produce documents the parser **rejects**.
    /// - `a\b` produces `"a\b"`, which the parser **accepts** — as a
    ///   *backspace*, because `\b` is a valid Turtle `ECHAR`. That is silent
    ///   corruption, not a crash, and no round-trip test that only checks
    ///   "did it parse" would catch it.
    #[test]
    fn display_output_is_either_unparseable_or_silently_wrong() {
        for value in ["a\"b", "a\\b", "a\nb", "backslash at end\\"] {
            let displayed = Term::string(value).to_string();
            let reread = reread_object(&displayed);
            assert_ne!(
                reread.as_deref(),
                Some(value),
                "Display round-tripped {value:?} faithfully as {displayed} — the hostile \
                 set no longer demonstrates why Display is banned from writer paths"
            );
        }

        // The corruption case, pinned exactly. The three characters `a \ b`
        // come back as the two characters `a` U+0008: the backslash became an
        // escape and *ate the `b`*. Parsed happily, wrong value, no error
        // anywhere — which is why "did the output parse?" is not a sufficient
        // check, and why Display is banned rather than merely discouraged.
        assert_eq!(
            reread_object(&Term::string("a\\b").to_string()).as_deref(),
            Some("a\u{8}")
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
