//! Round-trip and fidelity tests for the streaming writers
//!
//! The writers' unit tests pin their *syntax*. These pin what actually
//! matters: that a document written by one of them denotes the same RDF as the
//! document it was written from. The route is always
//! parse → write → re-parse → compare, with the real Turtle parser on both
//! ends, so nothing here is checked against the writers' own idea of what
//! they emitted.
//!
//! Parsing is in [`ParserOptions::conformant`] mode throughout. The ingest
//! default canonicalizes numeric literals and flattens collections into
//! indexed items, and neither shape survives a round-trip through RDF — which
//! is the reason the conformance options exist.

use fluree_graph_format::writer::{
    BlankNodeLabels, NQuadsWriter, NTriplesWriter, TrigWriter, TurtleWriter, WriterConfig,
};
use fluree_graph_ir::{Datatype, GraphCollectorSink, GraphSink, Term, Triple};
use fluree_graph_turtle::{parse_with_options, ParserOptions};
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Isomorphism
// ---------------------------------------------------------------------------

/// The identity of a non-blank term, for comparison purposes.
///
/// A literal is its *lexical form*, datatype and language — not the
/// [`LiteralValue`](fluree_graph_ir::LiteralValue) variant that happens to
/// hold it. `Boolean(true)` and `String("true")` with `xsd:boolean` are the
/// same RDF literal, and the parser produces the first while a re-parse of
/// what the writer emits produces the second.
fn term_key(term: &Term) -> Option<String> {
    match term {
        Term::BlankNode(_) => None,
        Term::Iri(iri) => Some(format!("I\u{1}{iri}")),
        Term::Literal {
            value,
            datatype,
            language,
        } => Some(format!(
            "L\u{1}{}\u{1}{}\u{1}{}",
            value.lexical(),
            datatype.as_iri(),
            language.as_deref().unwrap_or("")
        )),
    }
}

/// Whether `a` and `b` match under a blank-node bijection being built in
/// `fwd`/`rev`.
///
/// Both directions are tracked deliberately. A one-way map would accept two
/// distinct input nodes mapping onto one output node — exactly the silent
/// merge the blank-node policy exists to prevent.
fn terms_match(
    a: &Term,
    b: &Term,
    fwd: &mut HashMap<String, String>,
    rev: &mut HashMap<String, String>,
) -> bool {
    match (a, b) {
        (Term::BlankNode(x), Term::BlankNode(y)) => {
            let (x, y) = (x.as_str().to_string(), y.as_str().to_string());
            match (fwd.get(&x).cloned(), rev.get(&y).cloned()) {
                (Some(mapped), Some(back)) => mapped == y && back == x,
                (None, None) => {
                    fwd.insert(x.clone(), y.clone());
                    rev.insert(y, x);
                    true
                }
                _ => false,
            }
        }
        _ => term_key(a).is_some() && term_key(a) == term_key(b),
    }
}

fn triples_match(
    a: &Triple,
    b: &Triple,
    fwd: &mut HashMap<String, String>,
    rev: &mut HashMap<String, String>,
) -> bool {
    terms_match(&a.s, &b.s, fwd, rev)
        && terms_match(&a.p, &b.p, fwd, rev)
        && terms_match(&a.o, &b.o, fwd, rev)
}

fn match_from(
    expected: &[Triple],
    actual: &[Triple],
    index: usize,
    fwd: &mut HashMap<String, String>,
    rev: &mut HashMap<String, String>,
    used: &mut [bool],
) -> bool {
    if index >= expected.len() {
        return true;
    }
    for candidate in 0..actual.len() {
        if used[candidate] {
            continue;
        }
        let (saved_fwd, saved_rev) = (fwd.clone(), rev.clone());
        if triples_match(&expected[index], &actual[candidate], fwd, rev) {
            used[candidate] = true;
            if match_from(expected, actual, index + 1, fwd, rev, used) {
                return true;
            }
            used[candidate] = false;
        }
        *fwd = saved_fwd;
        *rev = saved_rev;
    }
    false
}

/// Whether two triple sets are the same RDF up to a blank-node bijection.
///
/// Backtracking search — exponential in the worst case, which is fine at
/// fixture scale and is why the fixtures here stay small.
fn are_isomorphic(expected: &[Triple], actual: &[Triple]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut used = vec![false; actual.len()];
    match_from(
        expected,
        actual,
        0,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut used,
    )
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn parse_conformant(input: &str) -> Vec<Triple> {
    let mut sink = GraphCollectorSink::new();
    parse_with_options(input, &mut sink, ParserOptions::conformant())
        .unwrap_or_else(|e| panic!("parse failed: {e}\n{input}"));
    sink.finish().unwrap();
    sink.into_graph().triples().to_vec()
}

/// [`to_ntriples`] with term validation off, for the fixtures whose input is
/// deliberately not conformant RDF — see [`parse_for_comparison`].
fn to_ntriples_unvalidated(input: &str, config: &WriterConfig) -> String {
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, config);
        let options = ParserOptions::conformant().with_validation(false);
        parse_with_options(input, &mut w, options).expect("parse");
        w.finish().expect("finish");
    }
    String::from_utf8(buf).expect("UTF-8")
}

fn to_ntriples(input: &str, config: &WriterConfig) -> String {
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, config);
        parse_with_options(input, &mut w, ParserOptions::conformant()).expect("parse");
        w.finish().expect("finish");
    }
    String::from_utf8(buf).expect("UTF-8")
}

fn to_turtle(input: &str, config: &WriterConfig) -> String {
    let mut buf = Vec::new();
    {
        let mut w = TurtleWriter::with_config(&mut buf, config);
        parse_with_options(input, &mut w, ParserOptions::conformant()).expect("parse");
        w.finish().expect("finish");
    }
    String::from_utf8(buf).expect("UTF-8")
}

/// Assert that both text writers reproduce `input`'s RDF exactly.
fn assert_round_trips(input: &str) {
    let expected = parse_conformant(input);
    assert!(!expected.is_empty(), "the fixture asserts nothing");

    for (name, written) in [
        ("N-Triples", to_ntriples(input, &WriterConfig::default())),
        ("Turtle", to_turtle(input, &WriterConfig::default())),
    ] {
        let actual = parse_conformant(&written);
        assert!(
            are_isomorphic(&expected, &actual),
            "{name} round-trip lost or changed RDF.\n--- input ---\n{input}\n\
             --- written ---\n{written}\n--- expected {} triples, got {} ---",
            expected.len(),
            actual.len()
        );
    }
}

/// The document used by most of the round-trip tests: every term shape the
/// writers have to render, kept small enough for the backtracking matcher.
const KITCHEN_SINK: &str = r#"
@prefix ex: <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

ex:alice a ex:Person ;
    ex:name "Alice" ;
    ex:label "Alice"@en, "Alicia"@es ;
    ex:age 42 ;
    ex:score 99.5 ;
    ex:ratio 2.5e-1 ;
    ex:active true ;
    ex:note "quote:\" backslash:\\ newline:\n tab:\t" ;
    ex:knows _:bob, [ ex:name "anonymous" ] .

_:bob ex:name "Bob" ;
    ex:knows ex:alice .

ex:list ex:items ( "a" "b" "c" ) .
"#;

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

#[test]
fn the_kitchen_sink_survives_both_text_writers() {
    assert_round_trips(KITCHEN_SINK);
}

#[test]
fn plain_iris_survive() {
    assert_round_trips("<http://example.org/a> <http://example.org/p> <http://example.org/o> .");
}

/// Write one triple with `subject` as the subject, and return the document.
fn write_subject_iri(subject: &str) -> String {
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::new(&mut buf);
        let s = w.term_iri(subject);
        let p = w.term_iri("http://example.org/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        w.finish().unwrap();
    }
    String::from_utf8(buf).unwrap()
}

/// Re-read writer output for comparison, with term validation OFF.
///
/// The writers must faithfully reproduce terms handed to them, and a term can
/// come from a *store* rather than a document — `export.rs` has always been
/// able to emit an IRI the RDF grammar forbids, because the binary index
/// never promised to hold only grammatical ones. Those outputs are not
/// conformant RDF and are not meant to be; what is under test is that the
/// writer's bytes read back as the SAME term.
///
/// So validation is off here on purpose. Leaving it on would fail the writer
/// for its input's sins, and would make these tests silently stop covering
/// the escaping they exist to pin. Conformance is covered where it belongs:
/// `fluree-graph-turtle`'s term-validation tests and the W3C suites.
fn parse_for_comparison(document: &str) -> Vec<Triple> {
    let mut sink = GraphCollectorSink::new();
    let options = ParserOptions::conformant().with_validation(false);
    parse_with_options(document, &mut sink, options)
        .unwrap_or_else(|e| panic!("re-parse failed: {e}\n{document}"));
    sink.finish().unwrap();
    sink.into_graph().triples().to_vec()
}

fn subjects_of(document: &str) -> Vec<String> {
    parse_for_comparison(document)
        .iter()
        .filter_map(|t| t.s.as_iri().map(str::to_string))
        .collect()
}

/// An IRI carrying a character `IRIREF` forbids cannot come from the parser as
/// a raw character, but it reaches a writer two ways: from a store (which is
/// what `export.rs` does) and from a `UCHAR` in a source document. Either way
/// the writer must emit an IRI that reads back as *the same resource*.
#[test]
fn an_iri_the_grammar_forbids_survives_as_the_same_iri() {
    let written = write_subject_iri("http://example.org/a|b");
    assert!(written.contains("a\\u007Cb"), "{written}");
    assert_eq!(subjects_of(&written), vec!["http://example.org/a|b"]);
}

/// The bug this replaced, pinned as a property: escaping is **injective**.
///
/// `http://ex/a b` and `http://ex/a%20b` are two distinct resources. Under the
/// percent-encoding this writer used to do, both emitted `http://ex/a%20b` and
/// re-read as one — two resources merged into one, no error anywhere. Adopted
/// from the reviewer's `zz_merge` probe.
#[test]
fn distinct_iris_never_merge_on_the_way_out() {
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::new(&mut buf);
        for iri in ["http://ex/a b", "http://ex/a%20b"] {
            let s = w.term_iri(iri);
            let p = w.term_iri("http://ex/p");
            let o = w.term_iri("http://ex/o");
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
        }
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();

    let mut subjects = subjects_of(&written);
    subjects.sort();
    subjects.dedup();
    assert_eq!(
        subjects,
        vec!["http://ex/a b".to_string(), "http://ex/a%20b".to_string()],
        "two distinct IRIs must stay two: {written}"
    );
}

/// Our own lexer accepts `UCHAR` inside `IRIREF`, so a document that uses one
/// lexes — and the IRI it denotes must survive the writer unchanged. Adopted
/// from the reviewer's j1/j2 probes.
///
/// Note what these two escapes expand to: a space and a `<`, neither legal in
/// an IRI. Since H-8 that makes the document invalid RDF, and a *validating*
/// parse rejects it (`turtle-eval-bad-01` / `-02`; the sibling test below
/// pins that). The property here is the writer's, and survives the
/// distinction: whatever term reaches a writer — from a document the parser
/// blessed, or from a store that never asked — must come out reading as the
/// same term. Hence the unvalidated re-parse.
#[test]
fn a_uchar_sourced_iri_round_trips_unchanged() {
    let doc = "<http://ex/a\\u0020b> <http://ex/p> <http://ex/o> .\n\
               <http://ex/x\\u003Cy> <http://ex/p> <http://ex/o> .\n";
    let before = subjects_of(doc);
    assert_eq!(
        before,
        vec!["http://ex/a b", "http://ex/x<y"],
        "input parses"
    );

    let written = to_ntriples_unvalidated(doc, &WriterConfig::default());
    assert_eq!(subjects_of(&written), before, "the writer changed the IRI");
}

/// The other half: a `UCHAR` expanding to a character `IRIREF` forbids makes
/// the document invalid, and the parser says so before a writer ever sees it.
///
/// This is why the writers never have to decide what to do with an IRI holding
/// a space — under conformant options such a term cannot reach them. Both
/// fixtures are W3C negative-eval tests verbatim.
#[test]
fn a_uchar_expanding_to_a_forbidden_character_never_reaches_the_writer() {
    for escape in ["\\u0020", "\\u003C", "\\u003E"] {
        let doc = format!("<http://ex/a{escape}b> <http://ex/p> <http://ex/o> .\n");
        let mut sink = GraphCollectorSink::new();
        let err = parse_with_options(&doc, &mut sink, ParserOptions::conformant())
            .expect_err("an IRI that is not an IRI must not parse");
        assert!(
            err.to_string().contains("not allowed in an IRI"),
            "{escape}: {err}"
        );
    }
}

/// Characters above U+009F are legal in `IRIREF` and must be written through
/// raw, not escaped. Adopted from the reviewer's j3 probe — the earlier
/// forbidden set included U+007F..U+009F, which the grammar does not.
#[test]
fn high_code_points_are_written_through_untouched() {
    for iri in [
        "http://ex/\u{7F}del",
        "http://ex/\u{9F}c1",
        "http://ex/\u{a0}nbsp",
        "http://ex/\u{2028}sep",
        "http://ex/\u{FFFD}",
        "http://ex/\u{10FFFF}",
    ] {
        let written = write_subject_iri(iri);
        assert_eq!(
            subjects_of(&written),
            vec![iri.to_string()],
            "{:?} did not survive; wrote {written:?}",
            iri.escape_debug().to_string()
        );
    }
}

/// The lexical forms `PreserveLexical` exists to keep. A writer that emitted
/// Turtle's numeric shorthand would hand `+1` back as `1`, and `1.0` as
/// either `1.0` or `1.0E0` depending on which type it guessed.
#[test]
fn numeric_lexical_forms_survive_verbatim() {
    let cases = [
        ("42", "42", "integer"),
        ("+1", "+1", "integer"),
        ("01", "01", "integer"),
        ("-7", "-7", "integer"),
        ("1.0", "1.0", "decimal"),
        ("-0.50", "-0.50", "decimal"),
        ("1e0", "1e0", "double"),
        ("1.0E6", "1.0E6", "double"),
        // Beyond i64: already lexical-preserving before this work, and it
        // must stay that way.
        (
            "123456789012345678901234567890",
            "123456789012345678901234567890",
            "integer",
        ),
    ];

    for (source, lexical, datatype) in cases {
        let doc = format!("<http://ex/s> <http://ex/p> {source} .");
        let written = to_ntriples(&doc, &WriterConfig::default());
        let expected = format!("\"{lexical}\"^^<http://www.w3.org/2001/XMLSchema#{datatype}> .\n");
        assert!(
            written.ends_with(&expected),
            "`{source}` should write as {expected:?}, got {written:?}"
        );
        assert_round_trips(&doc);
    }
}

#[test]
fn language_tags_and_string_escapes_survive() {
    let doc = r#"
<http://ex/s> <http://ex/p> "plain" ;
    <http://ex/q> "tagged"@en-GB ;
    <http://ex/r> "  \t \n \r \" \\ ünïcødé ☃" .
"#;
    assert_round_trips(doc);
    let written = to_ntriples(doc, &WriterConfig::default());
    assert!(written.contains("\"tagged\"@en-GB"), "{written}");
    assert!(written.contains("\\u0000"), "{written}");
    assert!(written.contains("☃"), "non-ASCII is not escaped: {written}");
}

/// A collection parsed as a spine writes as its spine triples, and re-reads as
/// the same collection. The `( … )` spelling is not reconstructed — that is
/// the blocks tier's documented boundary, and this is what it costs.
#[test]
fn a_collection_round_trips_as_its_spine() {
    let doc = "<http://ex/s> <http://ex/p> ( \"a\" \"b\" ) .";
    assert_round_trips(doc);

    let written = to_turtle(doc, &WriterConfig::default());
    assert!(
        written.contains("rest"),
        "the spine is written out, not re-collapsed: {written}"
    );
    assert!(
        !written.contains('('),
        "no ( ) reconstruction in v1: {written}"
    );
}

// ---------------------------------------------------------------------------
// Blank-node policy (plan H-6)
// ---------------------------------------------------------------------------

/// Blank-node labels appearing in a document, as a set.
fn blank_labels(triples: &[Triple]) -> HashSet<String> {
    let mut labels = HashSet::new();
    for t in triples {
        for term in [&t.s, &t.p, &t.o] {
            if let Term::BlankNode(b) = term {
                labels.insert(b.as_str().to_string());
            }
        }
    }
    labels
}

/// The adversarial fixture the policy was designed against: labels shaped
/// exactly like the writer's mints (`b1`, `b2`, …), a reserved `_:fdb-` label,
/// and anonymous nodes, all in one document.
const ADVERSARIAL_BLANKS: &str = r#"
@prefix ex: <http://example.org/> .
_:b1 ex:p "one" .
_:b2 ex:p "two" .
_:b3 ex:p "three" .
_:fdb-01ARZ3NDEK ex:p "skolem" .
_:fdb-01ARZ3NDEM ex:p "skolem too" .
[] ex:p "anon one" .
[] ex:p "anon two" .
_:b1 ex:knows _:b2 .
_:b2 ex:knows _:fdb-01ARZ3NDEK .
"#;

/// Isomorphism cannot see a merge — two nodes collapsing into one produces a
/// graph that is still isomorphic to a *smaller* graph, and the check would
/// have to be looking for that. So this counts identities directly.
#[test]
fn relabelling_preserves_blank_node_identity_counts() {
    let before = parse_conformant(ADVERSARIAL_BLANKS);
    let written = to_ntriples(ADVERSARIAL_BLANKS, &WriterConfig::default());
    let after = parse_conformant(&written);

    assert_eq!(
        blank_labels(&before).len(),
        blank_labels(&after).len(),
        "blank nodes merged or split.\n--- written ---\n{written}"
    );
    assert_eq!(before.len(), after.len(), "{written}");
    assert!(are_isomorphic(&before, &after), "{written}");
}

/// The carve-out: `_:fdb-…` are addressable identifiers (#1432), so
/// `fluree export | fluree rdf convert` has to keep them. Everything else is
/// renamed.
#[test]
fn fdb_labels_pass_through_relabelling_verbatim() {
    let written = to_ntriples(ADVERSARIAL_BLANKS, &WriterConfig::default());
    let labels = blank_labels(&parse_conformant(&written));

    assert!(labels.contains("fdb-01ARZ3NDEK"), "{labels:?}");
    assert!(labels.contains("fdb-01ARZ3NDEM"), "{labels:?}");

    // Every other output label is a mint, and no mint can be mistaken for a
    // carve-out.
    for label in &labels {
        assert!(
            label.starts_with("fdb-") || label.starts_with('b'),
            "unexpected output label {label:?} in {labels:?}"
        );
    }
    // The document's own `_:b1` did NOT survive as `b1` by accident: it was
    // relabelled like everything else, so the mints are free to use that
    // namespace.
    assert_eq!(labels.len(), 7, "{labels:?}");
}

#[test]
fn preserve_mode_keeps_every_label_and_still_round_trips() {
    let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, &config);
        parse_with_options(ADVERSARIAL_BLANKS, &mut w, ParserOptions::conformant()).expect("parse");
        w.finish().expect("finish");
    }
    let written = String::from_utf8(buf).unwrap();

    let before = parse_conformant(ADVERSARIAL_BLANKS);
    let after = parse_conformant(&written);
    assert!(are_isomorphic(&before, &after), "{written}");

    let labels = blank_labels(&after);
    for expected in ["b1", "b2", "b3", "fdb-01ARZ3NDEK", "fdb-01ARZ3NDEM"] {
        assert!(labels.contains(expected), "{expected} lost: {labels:?}");
    }
    assert_eq!(
        blank_labels(&before).len(),
        labels.len(),
        "identity count changed: {written}"
    );

    // The two `[ ]` nodes had no label to preserve, so the writer chose one.
    // Those choices must be legal — the re-parse above already proves that,
    // since an illegal label would not have got this far — and disjoint from
    // every label the document did write.
    let user_written: HashSet<String> = ["b1", "b2", "b3", "fdb-01ARZ3NDEK", "fdb-01ARZ3NDEM"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let minted: Vec<&String> = labels.difference(&user_written).collect();
    assert_eq!(minted.len(), 2, "one label per anonymous node: {labels:?}");
    for label in minted {
        assert!(
            label.starts_with("fdbw-"),
            "a chosen label must live in the reserved namespace: {label:?}"
        );
    }
}

/// Drive one blank-node label through preserve mode and report what came back
/// out, as the *label a reader sees*.
///
/// Verbatim, not up to isomorphism: preserve mode promises exact labels, so an
/// isomorphism check would hide a rename — which is the failure this is
/// looking for. Adopted from the reviewer's w1/w2 probes.
fn preserve_label_round_trip(label: &str) -> Vec<String> {
    let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, &config);
        let s = w.term_blank(Some(label));
        let p = w.term_iri("http://ex/p");
        let o = w.term_iri("http://ex/o");
        w.emit_triple(s, p, o).expect("emitted");
        w.end_statement();
        w.finish().expect("finished");
    }
    let written = String::from_utf8(buf).unwrap();
    let parsed = parse_conformant(&written);
    assert_eq!(parsed.len(), 1, "one statement in, one out: {written:?}");
    blank_labels(&parsed).into_iter().collect()
}

/// Every label preserve mode emits must read back as *itself*.
///
/// The failure this catches is not a crash. `_:ab.` parses fine — as `_:ab`
/// followed by the statement terminator — so the node is silently renamed and
/// the document still loads. Same class as the `a\b` literal corruption:
/// checking "did it parse" would pass.
#[test]
fn preserve_mode_never_emits_a_label_that_reads_back_as_something_else() {
    // Labels the shared production accepts: out exactly as they went in.
    for legal in ["b1", "a.b", "x-y", "_under", "0lead", "\u{C0}accented"] {
        assert_eq!(
            preserve_label_round_trip(legal),
            vec![legal.to_string()],
            "{legal:?} must survive verbatim"
        );
    }

    // Labels it does not: relabelled into the reserved namespace, and the
    // *emitted* label round-trips. Never emitted raw, never renamed by the
    // reader behind our back.
    for illegal in [
        "ab.",       // trailing '.' — the silent-rename case
        "",          // the empty label
        "a b",       // space
        "a\"b",      // quote
        "a\\b",      // backslash
        "a#b",       // starts a comment
        "a,b",       // comma
        "-b1",       // the IR's internal mint
        "\u{D7}x",   // MULTIPLICATION SIGN — a PN_CHARS_BASE gap
        "\u{B7}x",   // MIDDLE DOT — PN_CHARS but illegal first
        "\u{300}x",  // COMBINING GRAVE — same
        "\u{FFFE}x", // past FDF0-FFFD
    ] {
        let out = preserve_label_round_trip(illegal);
        assert_eq!(out.len(), 1, "{illegal:?} -> {out:?}");
        assert!(
            out[0].starts_with("fdbw-"),
            "{illegal:?} was emitted as {:?} rather than relabelled",
            out[0]
        );
    }
}

/// The `_:fdb-` carve-out is the one path that emits an input label verbatim,
/// and it is in the DEFAULT mode — so it is validated like every other path.
///
/// A `fdb-` label that is not a legal `BLANK_NODE_LABEL` was never a Fluree
/// skolem (those are `fdb-<ulid>`), so there is no addressability to keep and
/// it is minted instead. Inverted from the reviewer's y3 probe, which
/// established that six of these seven emitted unusable output.
#[test]
fn the_fdb_carve_out_is_validated_like_every_other_path() {
    for label in [
        "fdb-01ARZ", // a real skolem: legal, passes through
        "fdb-a b",   // space
        "fdb-x.",    // trailing '.' — reads back as a different name
        "fdb-a(b",   // paren
        "fdb-a\\b",  // backslash
        "fdb-a\"b",  // quote
        "fdb-a#b",   // starts a comment
    ] {
        let mut buf = Vec::new();
        {
            // Relabel is the default — this is not an opt-in path.
            let mut w = NTriplesWriter::new(&mut buf);
            let s = w.term_blank(Some(label));
            let p = w.term_iri("http://ex/p");
            let o = w.term_iri("http://ex/o");
            w.emit_triple(s, p, o).expect("emitted");
            w.end_statement();
            w.finish().expect("finished");
        }
        let written = String::from_utf8(buf).unwrap();
        let labels = blank_labels(&parse_conformant(&written));
        assert_eq!(labels.len(), 1, "{label:?} -> {written:?}");
        let out = labels.into_iter().next().unwrap();

        if label == "fdb-01ARZ" {
            assert_eq!(out, label, "a legal skolem must survive verbatim");
        } else {
            assert!(
                out.starts_with('b') && out != label,
                "{label:?} was emitted as {out:?} rather than minted"
            );
        }
    }
}

/// One node in, one node out. A label the writer relabels must still be the
/// SAME node everywhere it appears — the bijection is what makes minting a
/// rename rather than a loss. Adopted from the reviewer's y1 probe.
#[test]
fn a_repeated_illegal_label_keeps_one_identity() {
    let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, &config);
        let p = w.term_iri("http://ex/p");
        // The same illegal label, once as subject and once as object.
        let a = w.term_blank(Some("a b"));
        let o = w.term_iri("http://ex/o");
        w.emit_triple(a, p, o).unwrap();
        w.end_statement();
        let a2 = w.term_blank(Some("a b"));
        let s2 = w.term_iri("http://ex/s");
        w.emit_triple(s2, p, a2).unwrap();
        w.end_statement();
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();
    let labels = blank_labels(&parse_conformant(&written));
    assert_eq!(labels.len(), 1, "one node split into {labels:?}\n{written}");
}

/// Distinct labels must stay distinct, whether legal, illegal, or anonymous —
/// they all draw from one counter, and a collision there fuses two resources.
/// Adopted from the reviewer's y2 probe.
#[test]
fn distinct_illegal_labels_never_merge() {
    let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
    let illegal = ["a b", "a(b", "\u{D7}x", "ab.", "-b1", "", "a\\b"];
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, &config);
        let p = w.term_iri("http://ex/p");
        for (i, label) in illegal.iter().enumerate() {
            let s = w.term_blank(Some(label));
            let o = w.term_literal(&format!("v{i}"), Datatype::xsd_string(), None);
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
        }
        // A legal label and an anonymous node share the same counter.
        let s = w.term_blank(Some("legal"));
        let o = w.term_literal("legal", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        let anon = w.term_blank(None);
        let o = w.term_literal("anon", Datatype::xsd_string(), None);
        w.emit_triple(anon, p, o).unwrap();
        w.end_statement();
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();
    let labels = blank_labels(&parse_conformant(&written));
    assert_eq!(
        labels.len(),
        illegal.len() + 2,
        "labels merged: {labels:?}\n{written}"
    );
    assert!(labels.contains("legal"), "{labels:?}");
}

/// The internal-mint case, end to end: the IR's own anonymous labels are
/// `-b{N}` and cannot be serialized, so preserve mode must relabel them rather
/// than refuse the document or emit something no parser reads. A hand-fed
/// producer is the only way to present one — the parser cannot lex it.
#[test]
fn preserve_mode_relabels_an_internal_mint_and_the_output_still_parses() {
    let config = WriterConfig::new().with_blank_labels(BlankNodeLabels::Preserve);
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::with_config(&mut buf, &config);
        // `-b1` is what `GraphCollectorSink` mints for an anonymous node.
        let internal = w.term_blank(Some("-b1"));
        let p = w.term_iri("http://ex/p");
        let user = w.term_blank(Some("b1"));
        w.emit_triple(internal, p, user).unwrap();
        w.end_statement();
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();

    assert!(
        !written.contains("_:-b1"),
        "an unserializable label reached the output: {written}"
    );
    let labels = blank_labels(&parse_conformant(&written));
    assert_eq!(labels.len(), 2, "the two nodes stayed distinct: {labels:?}");
    assert!(
        labels.contains("b1"),
        "the user's label survived: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l.starts_with("fdbw-")),
        "the internal mint was relabelled into the reserved namespace: {labels:?}"
    );
}

// ---------------------------------------------------------------------------
// Datasets — hand-fed, because no N-Quads or TriG parser exists yet
// ---------------------------------------------------------------------------

/// One quad, driven through the protocol the way a dataset parser would.
fn quad<S: GraphSink>(sink: &mut S, s: &str, p: &str, o: &str, g: Option<&str>) {
    let s = sink.term_iri(s);
    let p = sink.term_iri(p);
    let o = sink.term_literal(o, Datatype::xsd_string(), None);
    match g {
        Some(g) => {
            let g = sink.term_iri(g);
            sink.emit_quad(s, p, o, g).unwrap();
        }
        None => sink.emit_triple(s, p, o).unwrap(),
    }
    sink.end_statement();
}

/// The dataset both writers are fed, as `(subject, graph)` pairs.
const DATASET: [(&str, Option<&str>); 5] = [
    ("http://ex/a", None),
    ("http://ex/b", Some("http://ex/g1")),
    ("http://ex/c", Some("http://ex/g1")),
    ("http://ex/d", Some("http://ex/g2")),
    ("http://ex/e", None),
];

#[test]
fn nquads_writes_a_dataset_one_line_per_quad() {
    let mut buf = Vec::new();
    {
        let mut w = NQuadsWriter::new(&mut buf);
        for (s, g) in DATASET {
            quad(&mut w, s, "http://ex/p", "v", g);
        }
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();
    assert_eq!(
        written,
        "<http://ex/a> <http://ex/p> \"v\" .\n\
         <http://ex/b> <http://ex/p> \"v\" <http://ex/g1> .\n\
         <http://ex/c> <http://ex/p> \"v\" <http://ex/g1> .\n\
         <http://ex/d> <http://ex/p> \"v\" <http://ex/g2> .\n\
         <http://ex/e> <http://ex/p> \"v\" .\n"
    );

    // The default-graph lines are N-Triples, so a real parser can check them.
    let default_graph: String = written
        .lines()
        .filter(|l| !l.contains("http://ex/g"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(parse_conformant(&default_graph).len(), 2);
}

/// TriG's blocks contain Turtle, so the round-trip runs the real parser over
/// every block rather than trusting the writer's own account of what it wrote.
/// There is no TriG parser in the light crates yet, so the split is done here.
fn trig_blocks(document: &str) -> Vec<(Option<String>, String)> {
    let mut blocks: Vec<(Option<String>, String)> = Vec::new();
    let mut default = String::new();
    let mut lines = document.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(rest) = line.strip_prefix("GRAPH ") else {
            if !line.starts_with("@prefix") {
                default.push_str(line);
                default.push('\n');
            }
            continue;
        };
        let name = rest
            .trim_end()
            .trim_end_matches('{')
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>')
            .to_string();
        let mut body = String::new();
        for inner in lines.by_ref() {
            if inner.starts_with('}') {
                break;
            }
            body.push_str(inner);
            body.push('\n');
        }
        blocks.push((Some(name), body));
    }
    blocks.push((None, default));
    blocks
}

#[test]
fn trig_blocks_contain_parseable_turtle_for_the_right_graphs() {
    let mut buf = Vec::new();
    {
        let mut w = TrigWriter::new(&mut buf);
        for (s, g) in DATASET {
            quad(&mut w, s, "http://ex/p", "v", g);
        }
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();

    let mut by_graph: HashMap<Option<String>, Vec<String>> = HashMap::new();
    for (name, body) in trig_blocks(&written) {
        let subjects: Vec<String> = parse_conformant(&body)
            .iter()
            .filter_map(|t| t.s.as_iri().map(str::to_string))
            .collect();
        by_graph.entry(name).or_default().extend(subjects);
    }
    for subjects in by_graph.values_mut() {
        subjects.sort();
    }

    assert_eq!(
        by_graph[&None],
        vec!["http://ex/a".to_string(), "http://ex/e".to_string()],
        "{written}"
    );
    assert_eq!(
        by_graph[&Some("http://ex/g1".to_string())],
        vec!["http://ex/b".to_string(), "http://ex/c".to_string()],
        "{written}"
    );
    assert_eq!(
        by_graph[&Some("http://ex/g2".to_string())],
        vec!["http://ex/d".to_string()],
        "{written}"
    );
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

#[test]
fn statement_and_byte_counts_match_what_was_written() {
    let mut buf = Vec::new();
    let statements = {
        let mut w = NTriplesWriter::new(&mut buf);
        parse_with_options(KITCHEN_SINK, &mut w, ParserOptions::conformant()).expect("parse");
        w.finish().expect("finish");
        let stats = w.stats();
        assert_eq!(stats.bytes as usize, buf.len());
        stats.statements
    };
    let written = String::from_utf8(buf).unwrap();
    assert_eq!(statements as usize, written.lines().count());
    assert_eq!(statements as usize, parse_conformant(KITCHEN_SINK).len());
}
