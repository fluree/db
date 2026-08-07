//! Differential guard for statement-scoped literal-slot recycling.
//!
//! The same document is parsed into a `GraphCollectorSink` that recycles
//! (`end_statement` reaches it) and into one that never recycles
//! (`end_statement` swallowed by a forwarding wrapper — "today's behavior" as
//! the oracle). Any `TermId` that outlives its statement while still
//! referenced shows up as a divergence between the two graphs.
//!
//! This is a differential rather than a set of hand-written expectations
//! because the failure mode is aliasing, not arithmetic: a wrong answer here
//! looks like a perfectly well-formed graph with one term substituted for
//! another, which per-triple assertions written by the same author who wrote
//! the recycling are unlikely to anticipate.
//!
//! Mutation-checked, and the limits are part of the record:
//!
//! - Recycling IRI slots — which the parser caches across statements — fails
//!   `hostile_document_default_mode_matches_the_no_recycle_oracle` and
//!   `hostile_document_spine_mode_matches_the_no_recycle_oracle`. The
//!   single-sink and failed-statement cases do NOT catch it, so those two are
//!   load-bearing; don't trim the corpus believing they are redundant.
//! - Always reusing literal slot 0 escapes this differential entirely. It is
//!   caught by the `fluree-graph-ir` unit tests instead
//!   (`literal_slots_are_reused_after_a_statement_ends`,
//!   `the_term_table_tracks_the_widest_statement_not_the_document`).
//!
//!   The reason, stated precisely: **no literal is minted while another
//!   literal id is live.** Not "one term id at a time" — the parser routinely
//!   holds several. `parse_reified_triple` holds a possibly-literal `object`
//!   while it mints the reifier, but the reifier grammar is `iri | BlankNode`,
//!   so the term minted there is never a literal. Only
//!   `parse_annotation_tail` breaks the rule, by holding `object` across an
//!   annotation body that can mint literals of its own — and it is
//!   unreachable from a recycling sink today (see the comment at that
//!   function).
//!
//!   So slot-0-collapse is observationally equivalent here, and this
//!   differential's power is the aliasing dimension rather than statement
//!   width. If a producer ever mints a literal while another literal id is
//!   live, that stops being true and this file needs a case for it.

use fluree_graph_ir::{
    Datatype, Graph, GraphCollectorSink, GraphSink, LiteralValue, SinkResult, TermId,
};
use fluree_graph_turtle::{parse, parse_with_options, CollectionStyle, ParserOptions};

/// Forwards every protocol method to an inner `GraphCollectorSink` EXCEPT
/// `end_statement`, which it swallows — so the inner sink never recycles a
/// literal slot. This is "today's behavior" as the oracle.
struct NoRecycle(GraphCollectorSink);

impl GraphSink for NoRecycle {
    fn on_base(&mut self, base_iri: &str) {
        self.0.on_base(base_iri);
    }
    fn on_prefix(&mut self, prefix: &str, ns: &str) {
        self.0.on_prefix(prefix, ns);
    }
    fn term_iri(&mut self, iri: &str) -> TermId {
        self.0.term_iri(iri)
    }
    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.0.term_blank(label)
    }
    fn term_literal(&mut self, v: &str, dt: Datatype, lang: Option<&str>) -> TermId {
        self.0.term_literal(v, dt, lang)
    }
    fn term_literal_value(&mut self, v: LiteralValue, dt: Datatype) -> TermId {
        self.0.term_literal_value(v, dt)
    }
    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.0.emit_triple(s, p, o)
    }
    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, i: i32) -> SinkResult {
        self.0.emit_list_item(s, p, o, i)
    }
    // end_statement deliberately NOT forwarded — this is the control arm.
}

/// Records the statement-lifecycle events in order, forwarding everything to
/// a real collector. The protocol publishes "exactly one of
/// `end_statement`/`abort_statement` per statement", and that is a claim about
/// the SEQUENCE, not about the resulting graph — a wrong sequence can still
/// produce a correct graph today and corrupt a buffering sink tomorrow. So
/// assert the sequence.
#[derive(Default)]
struct LoggingSink {
    inner: GraphCollectorSink,
    log: Vec<&'static str>,
}

impl GraphSink for LoggingSink {
    fn on_base(&mut self, b: &str) {
        self.inner.on_base(b);
    }
    fn on_prefix(&mut self, p: &str, n: &str) {
        self.inner.on_prefix(p, n);
    }
    fn term_iri(&mut self, i: &str) -> TermId {
        self.inner.term_iri(i)
    }
    fn term_blank(&mut self, l: Option<&str>) -> TermId {
        self.inner.term_blank(l)
    }
    fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
        self.inner.term_literal(v, d, l)
    }
    fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
        self.inner.term_literal_value(v, d)
    }
    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.log.push("Emit");
        self.inner.emit_triple(s, p, o)
    }
    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, i: i32) -> SinkResult {
        self.log.push("Emit");
        self.inner.emit_list_item(s, p, o, i)
    }
    fn end_statement(&mut self) {
        self.log.push("End");
        self.inner.end_statement();
    }
    fn abort_statement(&mut self) {
        self.log.push("Abort");
        self.inner.abort_statement();
    }
}

/// Two committed statements — the `@prefix` directive and one triple
/// statement — followed by a third that fails somehow.
const TWO_GOOD_THEN: &str = r#"@prefix ex: <http://example.org/> .
                 ex:a ex:p "1" ; ex:q "2" .
"#;

fn rows(g: &Graph) -> Vec<String> {
    let mut v: Vec<String> = g
        .iter()
        .map(|t| format!("{} {} {} [{:?}]", t.s, t.p, t.o, t.list_index()))
        .collect();
    v.sort();
    v
}

fn differential(doc: &str, options: ParserOptions) {
    let mut recycling = GraphCollectorSink::new();
    parse_with_options(doc, &mut recycling, options).expect("recycling parse");

    let mut control = NoRecycle(GraphCollectorSink::new());
    parse_with_options(doc, &mut control, options).expect("control parse");

    assert_eq!(
        rows(&recycling.into_graph()),
        rows(&control.0.into_graph()),
        "recycling diverged from the never-recycle oracle for:\n{doc}"
    );
}

/// Every construct that emits during descent, with literal counts that shrink
/// and grow across statement boundaries so slots get reused at every width.
const HOSTILE: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# wide statement: many literals, mixed kinds, one statement
ex:wide ex:a "l1" ; ex:b "l2"@en ; ex:c "l3"^^xsd:token ; ex:d 1 ; ex:e 2.5 ;
        ex:f 3.0e0 ; ex:g true ; ex:h "l1" , "l1" , "l2" .

# narrow statement immediately after: reuses slot 0 only
ex:narrow ex:a "only" .

# blank-node property list: emits during descent, literals at two depths
ex:outer ex:has [ ex:x "inner1" ; ex:y [ ex:z "inner2" ; ex:w "inner3" ] ] ;
         ex:tail "outer-tail" .

# collection of literals in object position
ex:coll ex:items ( "c1" "c2" "c3" ) .

# nested collection with literals at both levels
ex:nest ex:items ( "n1" ( "n2" "n3" ) "n4" ) .

# subject-position collection (always a spine) whose items are literals
( "s1" "s2" ) ex:p "s-obj" .

# empty collection then a wide statement again
ex:empty ex:e () .
ex:wide2 ex:a "z1" ; ex:b "z2" ; ex:c "z3" ; ex:d "z4" ; ex:e "z5" ; ex:f "z6" .

# repeated identical literals across statements (slot aliasing bait)
ex:r1 ex:p "same" .
ex:r2 ex:p "same" .
ex:r3 ex:p "same" , "same" , "same" .

# blank node labels that look like the anonymous mint
_:b0 ex:p "labeled0" .
_:b1 ex:p "labeled1" ; ex:q [ ex:r "anon-in-labeled" ] .
"#;

#[test]
fn hostile_document_default_mode_matches_the_no_recycle_oracle() {
    differential(HOSTILE, ParserOptions::default());
}

#[test]
fn hostile_document_spine_mode_matches_the_no_recycle_oracle() {
    differential(HOSTILE, ParserOptions::conformant());
    differential(
        HOSTILE,
        ParserOptions::new().with_collections(CollectionStyle::Spine),
    );
}

/// The bulk-import shape: one sink, many `parse()` calls. Slots retired by the
/// last statement of chunk N are handed out again in chunk N+1.
#[test]
fn one_sink_many_parses_survives_recycling() {
    let chunks = [
        r#"@prefix ex: <http://example.org/> . ex:a ex:p "1" ; ex:q "2" ; ex:r "3" ."#,
        r#"@prefix ex: <http://example.org/> . ex:b ex:p "4" ."#,
        r#"@prefix ex: <http://example.org/> . ex:c ex:p "5" ; ex:q "6" ; ex:r "7" ; ex:s "8" ."#,
    ];

    let mut recycling = GraphCollectorSink::new();
    let mut control = NoRecycle(GraphCollectorSink::new());
    for c in chunks {
        parse(c, &mut recycling).unwrap();
        parse(c, &mut control).unwrap();
    }
    assert_eq!(rows(&recycling.into_graph()), rows(&control.0.into_graph()));
}

/// A statement that FAILS after emitting contributes NOTHING: the parser
/// calls `abort_statement` and the sink rolls back to the statement
/// boundary. This is riot's semantics, and what `--continue-on-error` has to
/// guarantee — the parser emits during descent, so without the rollback a
/// rejected statement would leave partial triples behind.
#[test]
fn failed_statement_contributes_nothing() {
    // Statement 3 emits one triple, then hits a lexical error at its second
    // object — inside the statement, before its terminator.
    let doc = format!("{TWO_GOOD_THEN}                 ex:b ex:p \"3\" ; ex:q @@@ .");
    let mut sink = LoggingSink::default();
    parse(&doc, &mut sink).expect_err("statement 3 must fail");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "Emit", "End", "Emit", "Abort"],
        "the failing statement emitted once and was then aborted, and neither \
         committed statement was disturbed"
    );

    let g = sink.inner.into_graph();
    let objects: Vec<String> = g
        .iter()
        .map(|t| t.o.to_string())
        .filter(|s| s.starts_with('"'))
        .collect();
    assert!(
        !objects.iter().any(|o| o.contains("\"3\"")),
        "the failed statement's triples must be rolled back, got {objects:?}"
    );
    assert_eq!(
        objects.len(),
        2,
        "statement 2 must survive intact: {objects:?}"
    );
}

/// A statement that fails BEFORE emitting must not abort: there is nothing to
/// roll back, and a spurious `abort_statement` would discard the previous
/// statement, whose triples sit past the mark until `end_statement` advances
/// it. This is the true branch of the `emit_count` guard.
///
/// `"strsubject"` is chosen deliberately: it LEXES fine (it is a valid string
/// token) and is rejected by `parse_subject`, so the failure lands inside
/// statement 3 with zero emissions — which a lexical error could not do.
#[test]
fn failure_before_any_emit_does_not_abort() {
    let doc = format!("{TWO_GOOD_THEN}                 \"strsubject\" ex:p \"3\" .");
    let mut sink = LoggingSink::default();
    parse(&doc, &mut sink).expect_err("a literal subject must be rejected");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "Emit", "End"],
        "no Abort: the failing statement never emitted"
    );
    assert_eq!(
        sink.inner.into_graph().len(),
        2,
        "the committed statements must survive"
    );
}

/// LATCH GUARDS. `committed_current` is reset at the start of every statement;
/// if it ever stopped being reset, it would latch true after the first
/// committed statement and `abort_statement` would never fire again —
/// rollback dying silently, which is worse than the contract bug the flag
/// fixes. A latched flag turns both cases below into no-Abort / 2 triples.
///
/// Both use a COMMITTED statement followed by one that emits and then fails,
/// so the trailing Abort is mandatory.
#[test]
fn abort_still_fires_after_a_committed_statement_wrong_terminator() {
    // `ex:q` sits where the `.` belongs: it lexes fine, the triple is emitted,
    // and the statement then fails at its terminator.
    let doc = "@prefix ex: <http://example.org/> .\nex:a ex:p \"1\" .\nex:b ex:p \"2\" ex:q .\n";
    let mut sink = LoggingSink::default();
    parse(doc, &mut sink).expect_err("a non-dot terminator must fail");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "End", "Emit", "Abort"],
        "the flag must have reset: statement 3 emitted and must still abort"
    );
    assert_eq!(
        sink.inner.into_graph().len(),
        1,
        "only the committed statement's triple survives"
    );
}

#[test]
fn abort_still_fires_after_a_committed_statement_missing_final_dot() {
    // Runs out of input before the terminator — the same shape a truncated
    // file produces.
    let doc = "@prefix ex: <http://example.org/> .\nex:a ex:p \"1\" .\nex:b ex:p \"2\"\n";
    let mut sink = LoggingSink::default();
    parse(doc, &mut sink).expect_err("a missing final dot must fail");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "End", "Emit", "Abort"],
        "the flag must have reset: statement 3 emitted and must still abort"
    );
    assert_eq!(
        sink.inner.into_graph().len(),
        1,
        "only the committed statement's triple survives"
    );
}

/// A lexical error can also strike BEFORE the emit, even mid-statement: a
/// literal's `@lang`/`^^type` suffix is checked by lexing the next token, so
/// `"2" @@@` fails before the triple is emitted. Recorded because it looks
/// like the wrong-terminator case above and behaves differently — no Abort,
/// because nothing was emitted.
#[test]
fn lexical_error_in_a_literal_suffix_aborts_nothing() {
    let doc = "@prefix ex: <http://example.org/> .\nex:a ex:p \"1\" .\nex:b ex:p \"2\" @@@\n";
    let mut sink = LoggingSink::default();
    parse(doc, &mut sink).expect_err("@@@ must fail to lex");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "End"],
        "no Emit and so no Abort: the lexer failed inside the literal's suffix lookahead"
    );
    assert_eq!(sink.inner.into_graph().len(), 1);
}

/// The other guard: a statement that already COMMITTED at its `.` must not be
/// aborted either. It reaches the error arm only because the one-token
/// lookahead failed while reading the NEXT statement — the committed
/// statement is complete and valid.
///
/// `@@@` is a LEXICAL error, raised while advancing past statement 2's
/// terminator, so it surfaces from statement 2's own parse call. Before the
/// `committed_current` guard this logged a trailing `Abort`, breaking the
/// published "exactly one of end/abort per statement" contract (the graph
/// survived only because the commit had already moved the sink's mark).
#[test]
fn lexical_error_in_lookahead_does_not_abort_the_committed_statement() {
    let doc = format!("{TWO_GOOD_THEN}                 @@@ ex:p \"3\" .");
    let mut sink = LoggingSink::default();
    parse(&doc, &mut sink).expect_err("@@@ must fail to lex");

    assert_eq!(
        sink.log,
        vec!["End", "Emit", "Emit", "End"],
        "no Abort: statement 2 had already committed at its terminator"
    );
    assert_eq!(
        sink.inner.into_graph().len(),
        2,
        "the committed statements must survive"
    );
}
