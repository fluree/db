//! Term validation (H-8): the parser's half.
//!
//! `fluree-vocab` owns the predicates and unit-tests them directly. What this
//! file covers is the part only the parser can answer: that validation runs on
//! the value AFTER escape expansion and base resolution, that it reaches every
//! position an IRI can occupy, that it is OFF by default, and that turning it
//! off leaves the ingest path byte-for-byte where it was.

use fluree_graph_ir::{GraphCollectorSink, LineIndex};
use fluree_graph_turtle::{parse_with_prefixes_base_options, ParserOptions, TurtleError};

const BASE: &str = "http://example.org/base";

fn run(doc: &str, options: ParserOptions) -> Result<usize, TurtleError> {
    let mut sink = GraphCollectorSink::new();
    parse_with_prefixes_base_options(doc, &mut sink, &[], Some(BASE), options)?;
    Ok(sink.into_graph().len())
}

fn validating() -> ParserOptions {
    ParserOptions::conformant()
}

/// Conformant options minus validation — isolates the knob from the other two,
/// so a test that says "validation did this" is not really about spine
/// collections.
fn conformant_unvalidated() -> ParserOptions {
    ParserOptions::conformant().with_validation(false)
}

fn expect_rejected(doc: &str, options: ParserOptions) -> TurtleError {
    match run(doc, options) {
        Ok(n) => panic!("expected rejection, parsed {n} statement(s):\n{doc}"),
        Err(e) => e,
    }
}

// =============================================================================
// The W3C cases this workstream exists to close
// =============================================================================

/// `turtle-eval-bad-01/02/03`. Each document LEXES — `\uXXXX` is legal source
/// — and denotes an IRI containing a space, a `<`, or a `>`. The whole point
/// is that the check must run after expansion; a source-text check sees
/// nothing wrong.
#[test]
fn escape_expanded_forbidden_characters_are_rejected() {
    for (escape, ch) in [("\\u0020", ' '), ("\\u003C", '<'), ("\\u003E", '>')] {
        let doc = format!(
            "<http://www.w3.org/2013/TurtleTests/{escape}> \
             <http://www.w3.org/2013/TurtleTests/p> \
             <http://www.w3.org/2013/TurtleTests/o> .",
        );
        let err = expect_rejected(&doc, validating());
        let message = err.to_string();
        assert!(
            message.contains("not allowed in an IRI"),
            "{escape}: {message}"
        );
        if ch != ' ' {
            assert!(message.contains(ch), "{escape}: {message}");
        }

        // And without validation it is accepted, which is what made these
        // three registered failures for as long as they were.
        assert_eq!(run(&doc, conformant_unvalidated()).unwrap(), 1, "{escape}");
    }
}

/// `turtle-syntax-bad-lang-01` and `nt-syntax-bad-lang-01`, both `"string"@1`.
#[test]
fn a_language_tag_that_is_not_a_language_tag_is_rejected() {
    let doc = "<http://ex/s> <http://ex/p> \"string\"@1 .";
    let err = expect_rejected(doc, validating());
    assert!(err.to_string().contains("must be letters"), "{err}");
    assert_eq!(run(doc, conformant_unvalidated()).unwrap(), 1);
}

#[test]
fn well_formed_language_tags_still_parse() {
    for tag in [
        "en",
        "EN",
        "en-GB",
        "en-GB-oed",
        "zh-Hans-CN",
        "x-private-1",
    ] {
        let doc = format!("<http://ex/s> <http://ex/p> \"v\"@{tag} .");
        assert_eq!(run(&doc, validating()).unwrap(), 1, "@{tag}");
    }
}

#[test]
fn malformed_language_tags_beyond_the_w3c_case_are_rejected_too() {
    // No W3C test covers these, and they are equally not `LANGTAG`.
    for tag in ["en-", "-en", "en--gb", "e1"] {
        let doc = format!("<http://ex/s> <http://ex/p> \"v\"@{tag} .");
        expect_rejected(&doc, validating());
        assert_eq!(run(&doc, conformant_unvalidated()).unwrap(), 1, "@{tag}");
    }
}

// =============================================================================
// Coverage: every position an IRI reaches the sink from
// =============================================================================

/// A bad IRI must be caught wherever it appears, not only in subject position.
/// Each of these funnels through a different arm of the parser.
#[test]
fn validation_reaches_every_iri_position() {
    let bad = "<http://ex/\\u0020bad>";
    let cases = [
        ("subject", format!("{bad} <http://ex/p> <http://ex/o> .")),
        ("predicate", format!("<http://ex/s> {bad} <http://ex/o> .")),
        ("object", format!("<http://ex/s> <http://ex/p> {bad} .")),
        (
            "datatype",
            format!("<http://ex/s> <http://ex/p> \"v\"^^{bad} ."),
        ),
        (
            "prefix namespace, via an expanded name",
            format!("@prefix e: {bad} .\ne:s <http://ex/p> <http://ex/o> ."),
        ),
        (
            "blank-node property list object",
            format!("<http://ex/s> <http://ex/p> [ <http://ex/q> {bad} ] ."),
        ),
        (
            "collection item",
            format!("<http://ex/s> <http://ex/p> ( {bad} ) ."),
        ),
    ];
    for (position, doc) in cases {
        let err = expect_rejected(&doc, validating());
        assert!(
            err.to_string().contains("not allowed in an IRI"),
            "{position}: {err}"
        );
        assert!(
            run(&doc, conformant_unvalidated()).is_ok(),
            "{position}: unvalidated parse should still accept it"
        );
    }
}

/// Resolution, not just expansion: a relative reference resolved against a
/// base that itself carries a forbidden character produces a bad IRI, and the
/// only place to notice is after resolution.
#[test]
fn validation_runs_after_base_resolution() {
    let mut sink = GraphCollectorSink::new();
    let err = parse_with_prefixes_base_options(
        "<x> <http://ex/p> <http://ex/o> .",
        &mut sink,
        &[],
        Some("http://ex/ba d/"),
        validating(),
    )
    .expect_err("a base with a space poisons everything resolved against it");
    assert!(err.to_string().contains("not allowed in an IRI"), "{err}");
}

/// `IriViolation::NotAbsolute` is **unreachable through the Turtle parser**,
/// and this records why rather than leaving an untriggerable check looking
/// like coverage.
///
/// Three routes could produce a scheme-less resolved IRI, and each is already
/// guarded upstream of validation:
///
/// 1. a relative reference with no base — the parser's long-standing
///    `IriResolution` error;
/// 2. a relative `base` argument — refused when the parse starts, because a
///    base is the thing other references resolve against;
/// 3. a relative `@base` or `@prefix` value — resolved against the base in
///    force, so what reaches a term is absolute.
///
/// The predicate stays complete anyway. It is a shared vocab-level check, unit
/// tested there, and M2's strict N-Triples reader is where it earns its keep:
/// N-Triples has no base at all, and `nt-syntax-bad-uri-06..09` — still
/// registered under cause E — are exactly relative-IRI tests.
#[test]
fn the_not_absolute_check_is_guarded_upstream_in_turtle() {
    // 1. No base.
    for options in [validating(), ParserOptions::default()] {
        let mut sink = GraphCollectorSink::new();
        let err = parse_with_prefixes_base_options(
            "<x> <http://ex/p> <http://ex/o> .",
            &mut sink,
            &[],
            None,
            options,
        )
        .expect_err("no base, relative reference");
        assert!(
            matches!(err, TurtleError::IriResolution(_)),
            "got {err:?} for validate={}",
            options.validate
        );
    }

    // 2. Relative base argument — refused before a statement is read, so
    //    validation never sees the `:relbase/x` shape resolution would make.
    let mut sink = GraphCollectorSink::new();
    let err = parse_with_prefixes_base_options(
        "<x> <http://ex/p> <http://ex/o> .",
        &mut sink,
        &[],
        Some("relbase/"),
        validating(),
    )
    .expect_err("a relative base argument is refused");
    assert!(matches!(err, TurtleError::IriResolution(_)), "{err:?}");

    // 3. Relative directive values resolve against the base in force.
    for doc in [
        "@prefix e: <rel/> .\ne:s <http://ex/p> <http://ex/o> .",
        "@base <rel/> .\n<x> <http://ex/p> <http://ex/o> .",
    ] {
        assert_eq!(run(doc, validating()).unwrap(), 1, "{doc}");
    }
}

// =============================================================================
// Diagnostics
// =============================================================================

/// The brief's requirement: a validation failure is positioned like any other
/// parse error, so `check` can render it with a caret.
#[test]
fn a_validation_error_carries_a_byte_offset_and_renders_with_a_caret() {
    let doc =
        "<http://ex/s> <http://ex/p> \"ok\" .\n<http://ex/\\u0020> <http://ex/p> <http://ex/o> .";
    let err = expect_rejected(doc, validating());

    let index = LineIndex::new(doc);
    let d = err.to_diagnostic(&index);
    assert!(d.has_position(), "{d}");
    assert_eq!(d.line, 2, "the offending statement is on line 2");
    assert_eq!(d.col, 1, "and the IRI token opens the line");

    let rendered = d.render(&index);
    assert!(rendered.contains("\n2 | "), "{rendered}");
    assert!(rendered.contains('^'), "{rendered}");
}

/// The position is the IRI TOKEN's start, not an offset inside the resolved
/// value — expansion and resolution both change the length, so an index into
/// the result maps back to nothing. The message carries the detail instead.
#[test]
fn the_position_blames_the_token_and_the_message_carries_the_detail() {
    let doc = "<http://ex/s> <http://ex/p> \"v\"^^<http://ex/d\\u0020t> .";
    let err = expect_rejected(doc, validating());
    let TurtleError::Parse { position, message } = &err else {
        panic!("validation must produce a positioned Parse error, got {err:?}");
    };

    assert_eq!(
        &doc[*position..*position + 2],
        "^^",
        "a bad datatype blames the `^^` that introduced it"
    );
    assert!(message.contains("a space"), "{message}");
    // 11 = the space's byte offset within `http://ex/d t`, which is NOT its
    // offset in the source (where it is a six-character ` ` escape). The
    // two indices genuinely differ, and the message is where the finer one
    // belongs.
    assert!(
        message.contains("position 11"),
        "the index inside the RESOLVED value: {message}"
    );
    assert!(
        doc[*position..].starts_with("^^<http://ex/d\\u0020t>"),
        "the source still holds the unexpanded escape"
    );
}

// =============================================================================
// The ingest contract
// =============================================================================

/// The pin the whole design rests on. Bulk import runs `ParserOptions::default`
/// on the 2M-statements/second path; if validation ever became a default it
/// would pay for a scan of every resolved IRI to check documents this database
/// itself produced.
///
/// Asserted as behavior, not as a flag: under the default options each of
/// these documents parses, exactly as it did before validation existed.
#[test]
fn ingest_defaults_do_not_validate() {
    assert!(!ParserOptions::default().validate);

    for doc in [
        "<http://www.w3.org/2013/TurtleTests/\\u0020> <http://ex/p> <http://ex/o> .",
        "<http://www.w3.org/2013/TurtleTests/\\u003C> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> \"string\"@1 .",
        "<http://ex/s> <http://ex/p> \"v\"^^<http://ex/d\\u0020t> .",
        "@prefix e: <http://ex/\\u0020> .\ne:s <http://ex/p> <http://ex/o> .",
    ] {
        assert!(
            run(doc, ParserOptions::default()).is_ok(),
            "ingest must be unchanged by H-8:\n{doc}"
        );
    }
}

/// Conformant means conformant — the preset carries validation, so a caller
/// asking for the faithful-RDF shape does not silently get an unfaithful one.
#[test]
fn the_conformant_preset_validates() {
    assert!(ParserOptions::conformant().validate);
    expect_rejected(
        "<http://ex/s> <http://ex/p> \"string\"@1 .",
        ParserOptions::conformant(),
    );
}

// =============================================================================
// BOM tolerance
// =============================================================================

/// A UTF-8 BOM is not whitespace and not a token start, so before this it died
/// on character one. Windows editors emit them; riot eats them.
#[test]
fn a_leading_byte_order_mark_is_consumed() {
    let doc = "\u{FEFF}<http://ex/s> <http://ex/p> <http://ex/o> .";
    assert_eq!(run(doc, validating()).unwrap(), 1);
    assert_eq!(run(doc, ParserOptions::default()).unwrap(), 1);

    // Before any directive, too — the BOM sits ahead of `@prefix`.
    let doc = "\u{FEFF}@prefix e: <http://ex/> .\ne:s e:p e:o .";
    assert_eq!(run(doc, validating()).unwrap(), 1);

    // And a document that is nothing but a BOM is an empty document.
    assert_eq!(run("\u{FEFF}", validating()).unwrap(), 0);
}

/// Consumed, not stripped: token spans are absolute offsets into the original
/// source, so removing the three bytes would shift every one of them. This is
/// the test that would fail if someone "simplified" it to a slice.
#[test]
fn a_bom_does_not_shift_token_offsets() {
    let doc = "\u{FEFF}<http://ex/s> <http://ex/p> \"ok\" .\n<http://ex/\\u0020> <http://ex/p> <http://ex/o> .";
    let err = expect_rejected(doc, validating());

    let index = LineIndex::new(doc);
    let d = err.to_diagnostic(&index);
    assert_eq!(d.line, 2, "still line 2 with a BOM in front: {d}");
    assert_eq!(
        &doc[d.byte_span.0 as usize..=d.byte_span.0 as usize],
        "<",
        "the offset must land on the IRI's opening angle bracket"
    );
}

/// Only at the start. Elsewhere U+FEFF is a zero-width no-break space — a real
/// character in the document, and a real error.
#[test]
fn a_byte_order_mark_anywhere_else_is_still_an_error() {
    let doc = "<http://ex/s> \u{FEFF}<http://ex/p> <http://ex/o> .";
    let err = expect_rejected(doc, validating());
    assert!(
        matches!(err, TurtleError::Lexer { .. }),
        "expected a lexical error, got {err:?}"
    );
}
