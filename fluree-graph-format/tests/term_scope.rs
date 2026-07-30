//! A producer's term-scope declaration must change the writer's memory and
//! nothing else.
//!
//! `TermScope::Statement` lets a writer recycle the slots behind term ids at
//! every statement boundary, which is what keeps a term table from growing
//! with the document. The risk it buys is exact: a slot reused while something
//! still refers to it is silent data corruption — a subject that comes out as
//! the wrong IRI, a blank node that merges with another.
//!
//! The test that catches that is differential, not descriptive. A document
//! that is BOTH valid N-Triples and valid Turtle can be read two ways: through
//! the line reader, which declares statement scope, and through the Turtle
//! parser, which declares nothing and so gets the session semantics the
//! protocol defaults to. The same document, the same writer, two different
//! scopes — the bytes must be identical. Anything the recycling breaks shows
//! up as a diff.

use fluree_graph_format::{NQuadsWriter, NTriplesWriter, WriterConfig};
use fluree_graph_ir::GraphSink;

/// Convert `doc` to N-Triples through the strict line reader — the path that
/// declares [`TermScope::Statement`].
fn through_line_reader(doc: &str) -> String {
    let mut writer = NTriplesWriter::with_config(Vec::new(), &WriterConfig::new());
    fluree_graph_turtle::parse_ntriples(doc, &mut writer).expect("parses as N-Triples");
    writer.finish().expect("finishes");
    String::from_utf8(writer.into_inner()).expect("utf-8")
}

/// Convert `doc` to N-Triples through the Turtle parser — the path that makes
/// no declaration and so gets session-scoped term ids.
fn through_turtle_parser(doc: &str) -> String {
    let mut writer = NTriplesWriter::with_config(Vec::new(), &WriterConfig::new());
    fluree_graph_turtle::parse_with_options(
        doc,
        &mut writer,
        fluree_graph_turtle::ParserOptions::conformant(),
    )
    .expect("parses as Turtle");
    writer.finish().expect("finishes");
    String::from_utf8(writer.into_inner()).expect("utf-8")
}

/// Documents that are valid under both grammars, chosen for the things
/// recycling could plausibly break.
fn shared_grammar_documents() -> Vec<(&'static str, String)> {
    vec![
        (
            "a subject recurring after many statements",
            (0..200)
                .map(|i| format!("<http://ex/s{i}> <http://ex/p> <http://ex/o{i}> .\n"))
                .chain(std::iter::once(
                    "<http://ex/s0> <http://ex/p> <http://ex/last> .\n".to_string(),
                ))
                .collect(),
        ),
        ("one labelled blank named again at the far end", {
            let mut doc = String::from("_:x <http://ex/p> <http://ex/first> .\n");
            for i in 0..200 {
                doc.push_str(&format!(
                    "<http://ex/s{i}> <http://ex/p> \"filler {i}\" .\n"
                ));
            }
            doc.push_str("<http://ex/other> <http://ex/q> _:x .\n");
            doc
        }),
        (
            "two labelled blanks that must not merge",
            String::from(
                "_:a <http://ex/p> _:b .\n\
                 _:b <http://ex/p> _:a .\n\
                 _:a <http://ex/p> _:a .\n",
            ),
        ),
        (
            "every literal shape in one document",
            String::from(
                "<http://ex/s> <http://ex/p> \"plain\" .\n\
                 <http://ex/s> <http://ex/p> \"tagged\"@en .\n\
                 <http://ex/s> <http://ex/p> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
                 <http://ex/s> <http://ex/p> \"quote \\\" and \\\\ backslash\" .\n\
                 <http://ex/s> <http://ex/p> \"tab\\tnewline\\n\" .\n",
            ),
        ),
        ("a statement much wider than its neighbours", {
            let mut doc = String::from("<http://ex/a> <http://ex/p> <http://ex/b> .\n");
            for i in 0..64 {
                doc.push_str(&format!("<http://ex/wide> <http://ex/p{i}> \"v{i}\" .\n"));
            }
            doc.push_str("<http://ex/a> <http://ex/p> <http://ex/c> .\n");
            doc
        }),
    ]
}

#[test]
fn recycling_changes_no_byte_of_the_output() {
    for (what, doc) in shared_grammar_documents() {
        let recycled = through_line_reader(&doc);
        let session = through_turtle_parser(&doc);
        assert_eq!(
            recycled, session,
            "statement-scoped and session-scoped term ids disagreed on `{what}`"
        );
        assert!(!recycled.is_empty(), "`{what}` wrote nothing");
    }
}

#[test]
fn a_labelled_blank_survives_two_hundred_statements_of_recycling() {
    // The one identity that must outlive the statement it appeared in. If the
    // blank's slot were recycled with the rest, the far-end reference would
    // come out under a different minted label and the document would describe
    // two nodes where it meant one.
    let mut doc = String::from("_:x <http://ex/p> <http://ex/first> .\n");
    for i in 0..200 {
        doc.push_str(&format!("<http://ex/s{i}> <http://ex/p> \"v{i}\" .\n"));
    }
    doc.push_str("<http://ex/other> <http://ex/q> _:x .\n");

    let out = through_line_reader(&doc);
    let labels: Vec<&str> = out
        .lines()
        .filter_map(|line| line.split_whitespace().find(|t| t.starts_with("_:")))
        .collect();
    assert_eq!(labels.len(), 2, "both references should be blank nodes");
    assert_eq!(
        labels[0], labels[1],
        "one input label must come out as one output label: {out}"
    );
}

#[test]
fn n_quads_recycles_the_graph_term_too_without_losing_the_graph() {
    // The graph name is a fourth term id, minted per line by the same reader.
    // Recycling it must not let a statement land in the previous statement's
    // graph — the failure would be silent and the document would still be
    // well-formed.
    let doc = "<http://ex/s1> <http://ex/p> <http://ex/o> <http://ex/g1> .\n\
               <http://ex/s2> <http://ex/p> <http://ex/o> <http://ex/g2> .\n\
               <http://ex/s3> <http://ex/p> <http://ex/o> .\n\
               <http://ex/s4> <http://ex/p> <http://ex/o> <http://ex/g1> .\n";

    let mut writer = NQuadsWriter::with_config(Vec::new(), &WriterConfig::new());
    fluree_graph_turtle::parse_nquads(doc, &mut writer).expect("parses");
    writer.finish().expect("finishes");
    let out = String::from_utf8(writer.into_inner()).expect("utf-8");

    assert_eq!(out, doc, "N-Quads must round-trip byte for byte");
}
