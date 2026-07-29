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
//! Mutation-checked: making the recycling unsound (recycling IRI slots, which
//! the parser caches across statements) fails
//! `hostile_document_default_mode_matches_the_no_recycle_oracle` and
//! `hostile_document_spine_mode_matches_the_no_recycle_oracle`. Keep it that
//! way — a differential that cannot fail is worse than no test, and the
//! single-sink and failed-statement cases below do NOT catch that mutation on
//! their own.

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
    let mut sink = GraphCollectorSink::new();
    // Statement 1 completes (2 literals). Statement 2 emits one triple then
    // hits a syntax error before its terminator.
    let doc = r#"@prefix ex: <http://example.org/> .
                 ex:a ex:p "1" ; ex:q "2" .
                 ex:b ex:p "3" ; ex:q @@@ ."#;
    parse(doc, &mut sink).expect_err("statement 2 must fail");

    let g = sink.into_graph();
    let objects: Vec<String> = g
        .iter()
        .map(|t| t.o.to_string())
        .filter(|s| s.starts_with('"'))
        .collect();
    assert!(
        !objects.iter().any(|o| o.contains("\"3\"")),
        "the failed statement's triples must be rolled back, got {objects:?}"
    );
    // …and only the failed statement is rolled back; the committed one stays.
    assert_eq!(
        objects.len(),
        2,
        "statement 1 must survive intact: {objects:?}"
    );
    assert!(objects.iter().any(|o| o.contains("\"1\"")), "{objects:?}");
    assert!(objects.iter().any(|o| o.contains("\"2\"")), "{objects:?}");
}

/// The rollback must not fire when the statement failed before emitting —
/// there is nothing to roll back, and a spurious `abort_statement` would
/// discard the PREVIOUS statement (whose triples sit past the mark only
/// until `end_statement` advances it).
#[test]
fn failure_before_any_emit_leaves_earlier_statements_intact() {
    let mut sink = GraphCollectorSink::new();
    // Statement 2 fails at its subject, before a single triple is emitted.
    let doc = r#"@prefix ex: <http://example.org/> .
                 ex:a ex:p "1" ; ex:q "2" .
                 @@@ ex:p "3" ."#;
    parse(doc, &mut sink).expect_err("statement 2 must fail");

    let g = sink.into_graph();
    assert_eq!(g.len(), 2, "the committed statement must survive");
}
