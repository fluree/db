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

/// An IRI carrying a character `IRIREF` forbids cannot come from the parser —
/// it rejects the input — but it can come from a store, which is exactly the
/// case `export.rs` hits. The writer percent-encodes it so the output stays
/// parseable.
///
/// Note what this is *not*: percent-encoding changes the IRI, so the emitted
/// term denotes a different resource than the one handed in. That is the only
/// available choice — the alternative is a document no parser accepts — and it
/// matches what the exporter has always done.
#[test]
fn an_iri_the_grammar_forbids_is_percent_encoded_on_the_way_out() {
    let mut buf = Vec::new();
    {
        let mut w = NTriplesWriter::new(&mut buf);
        let s = w.term_iri("http://example.org/a|b");
        let p = w.term_iri("http://example.org/p");
        let o = w.term_literal("v", Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        w.finish().unwrap();
    }
    let written = String::from_utf8(buf).unwrap();
    assert!(written.contains("a%7Cb"), "{written}");

    let reparsed = parse_conformant(&written);
    assert_eq!(reparsed.len(), 1);
    assert_eq!(reparsed[0].s.as_iri(), Some("http://example.org/a%7Cb"));
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
