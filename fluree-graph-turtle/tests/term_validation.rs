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
/// The predicate stays complete anyway, and it is a *shared vocab-level*
/// check, unit tested there rather than here. Its justification is the strict
/// N-Triples reader, which M2 built as a SEPARATE reader rather than as a
/// profile on this parser — so it carries no base machinery at all, and none
/// of the three guards above exists on that path. A relative IRI reaches its
/// term check directly, which is what `nt-syntax-bad-uri-06..09` (registered
/// under cause E) require. The conclusion holds; the mechanism is a different
/// reader, not this one gaining a mode.
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

// =============================================================================
// Directive values (review F5)
// =============================================================================

/// A directive's own value is validated, not only the terms built from it.
///
/// The case that forces this: a document declares a namespace that is not an
/// IRI and then never uses the prefix. Waiting for an expansion means nothing
/// is ever checked and `fluree parse` answers "valid RDF" — which it is
/// not. `@base` matters more still, because a bad base is not inert: every
/// relative reference in the document resolves against it.
#[test]
fn a_directive_declaring_a_non_iri_is_rejected_even_if_never_used() {
    let unused_prefix = "@prefix e: <http://ex/\\u0020> .\n\
                         <http://ex/s> <http://ex/p> <http://ex/o> .";
    let err = expect_rejected(unused_prefix, validating());
    assert!(err.to_string().contains("not allowed in an IRI"), "{err}");
    assert!(
        run(unused_prefix, conformant_unvalidated()).is_ok(),
        "unvalidated keeps today's behavior"
    );

    let bad_base = "@base <http://ex/\\u0020> .\n\
                    <http://ex/s> <http://ex/p> <http://ex/o> .";
    let err = expect_rejected(bad_base, validating());
    assert!(err.to_string().contains("not allowed in an IRI"), "{err}");
    assert!(run(bad_base, conformant_unvalidated()).is_ok());
}

/// The diagnostic blames the directive's own value, not a term downstream of it.
#[test]
fn a_bad_directive_is_located_at_the_directive() {
    let doc = "<http://ex/s> <http://ex/p> \"ok\" .\n@base <http://ex/\\u0020> .";
    let err = expect_rejected(doc, validating());
    let index = LineIndex::new(doc);
    let d = err.to_diagnostic(&index);
    assert_eq!(d.line, 2, "the directive is on line 2: {d}");
    assert!(
        doc[d.byte_span.0 as usize..].starts_with("<http://ex/\\u0020>"),
        "the offset must land on the directive's IRI token"
    );
}

// =============================================================================
// The diagnostic must survive its own subject matter (review F2)
// =============================================================================

/// An offending character is by definition one an IRI may not hold, and the two
/// worst cases damage the report rather than the data: an expanded newline ends
/// the message's first line — truncating it wherever a caller reads a headline
/// — and an expanded NUL makes captured stderr binary, at which point `grep`
/// answers "no match" for text that is right there. This session hit exactly
/// that trap twice in committed source, so the message escapes.
#[test]
fn a_control_character_cannot_damage_the_diagnostic_that_reports_it() {
    for (escape, rendered) in [("\\u000A", "\\n"), ("\\u0000", "\\0"), ("\\u000D", "\\r")] {
        let doc = format!("<http://ex/a{escape}b> <http://ex/p> <http://ex/o> .");
        let err = expect_rejected(&doc, validating());
        let message = err.to_string();

        assert!(
            !message.contains('\n') || message.lines().count() == 1,
            "{escape}: a raw newline truncates the report: {message:?}"
        );
        assert!(
            !message.contains('\u{0}'),
            "{escape}: a raw NUL makes captured output binary: {message:?}"
        );
        assert!(
            message.contains(rendered),
            "{escape}: the character should appear escaped as {rendered}: {message}"
        );
    }
}

/// A language-tag diagnostic is one line, and no Turtle document can make it
/// otherwise.
///
/// Renamed from a claim it did not support. It is NOT the language-tag twin of
/// the IRI escaping test above, because the Turtle lexer cannot deliver a
/// control character to the tag path at all: its scan admits only
/// `[a-zA-Z0-9-]`, which is NARROWER than the grammar in characters and wider
/// only in shape. `@en_GB`, an accented tag and a bare `@` are lexical errors.
///
/// So what this pins is the reachable case — a tag that lexes and then fails
/// the grammar — staying single-line and control-free. The escape on that path
/// is kept for the shared predicate and future readers, not for this test.
#[test]
fn a_reachable_language_tag_diagnostic_stays_single_line() {
    for tag in ["1", "e1", "en-", "-en", "en--gb"] {
        let doc = format!("<http://ex/s> <http://ex/p> \"v\"@{tag} .");
        let message = expect_rejected(&doc, validating()).to_string();
        assert!(!message.contains('\u{0}'), "@{tag}: {message:?}");
        assert_eq!(message.lines().count(), 1, "@{tag}: {message:?}");
    }

    // And the characters that would need escaping never get that far.
    for tag in ["en_GB", "en.GB", "\u{e9}"] {
        let doc = format!("<http://ex/s> <http://ex/p> \"v\"@{tag} .");
        let err = expect_rejected(&doc, validating());
        assert!(
            matches!(err, TurtleError::Lexer { .. }),
            "@{tag} should be a LEXICAL error, not a tag violation: {err:?}"
        );
    }
}

// =============================================================================
// The base-less skip stays sound (review's optional perf item)
// =============================================================================

/// With no base, a verbatim `<...>` token skips the character scan entirely:
/// the lexer already scanned that span with `is_iri_char`, and the branch has
/// established absoluteness, so nothing is left to check.
///
/// Read this as a SMOKE TEST, not as the guard. While `is_iri_char` and the
/// forbidden set remain complements, no document exists that this could
/// distinguish — so it cannot fail while the skip is sound, and it would not
/// have caught the skip being wrong for a subtler reason either.
///
/// The two things actually holding the skip up are elsewhere: the
/// all-codepoints differential in `fluree-graph-format`
/// (`iriref_set_differential`), which fails if the two classes ever drift, and
/// the inertness of the branch itself — no resolution has happened, so there is
/// no composed string for a scan to have an opinion about. What this file adds
/// is the observation that the fixtures below, which include the boundary cases
/// (U+007F, non-ASCII), do in fact behave the same both ways.
#[test]
fn with_no_base_lexed_iris_behave_identically_validated_or_not() {
    for doc in [
        "<http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> \"v\"^^<http://ex/d> .",
        "<urn:uuid:1> <http://ex/p> <mailto:a@b.c> .",
        // Non-ASCII is legal in an IRI and must not be mistaken for suspect.
        "<http://ex/\u{e9}> <http://ex/p> <http://ex/\u{4e2d}\u{6587}> .",
        // U+007F is legal in IRIREF — the boundary the forbidden set stops at.
        "<http://ex/\u{7f}> <http://ex/p> <http://ex/o> .",
    ] {
        let mut validated = GraphCollectorSink::new();
        let a = parse_with_prefixes_base_options(doc, &mut validated, &[], None, validating());
        let mut plain = GraphCollectorSink::new();
        let b =
            parse_with_prefixes_base_options(doc, &mut plain, &[], None, conformant_unvalidated());
        assert_eq!(a.is_ok(), b.is_ok(), "disagreed on {doc:?}");
        assert!(a.is_ok(), "{doc:?} should parse: {a:?}");
    }
}

/// And the skip must NOT extend to escaped tokens — that is the whole
/// `turtle-eval-bad` class, and it has no base either.
#[test]
fn with_no_base_escaped_iris_are_still_checked() {
    for escape in ["\\u0020", "\\u003C", "\\u003E"] {
        let doc = format!("<http://ex/a{escape}b> <http://ex/p> <http://ex/o> .");
        let mut sink = GraphCollectorSink::new();
        let err = parse_with_prefixes_base_options(&doc, &mut sink, &[], None, validating())
            .expect_err("an escaped forbidden character must be caught with no base");
        assert!(err.to_string().contains("not allowed in an IRI"), "{err}");
    }
}

// ============================================================================
// The strict line reader's half — two readers, one rule
// ============================================================================
//
// The Turtle parser validates under `ParserOptions::validate`; the strict
// N-Triples/N-Quads reader validates unconditionally. What matters is that
// both reach the SAME predicates in `fluree-vocab`, because a second
// hand-rolled implementation of `LANGTAG` or of the IRIREF exclusion set is
// two implementations of one grammar, and two implementations drift.
//
// These pin the reader's side of that. `fluree-vocab` unit-tests the
// predicates themselves; what only the reader can answer is that they are
// reached at all, and on the expanded value rather than the source bytes.

use fluree_graph_turtle::{parse_nquads, parse_ntriples};

fn nt(doc: &str) -> Result<usize, TurtleError> {
    let mut sink = GraphCollectorSink::new();
    parse_ntriples(doc, &mut sink)?;
    Ok(sink.into_graph().len())
}

fn nq(doc: &str) -> Result<usize, TurtleError> {
    let mut sink = fluree_graph_ir::DatasetCollectorSink::new();
    parse_nquads(doc, &mut sink)?;
    Ok(sink.into_dataset().len())
}

/// F7, verified against the reader rather than assumed: the line grammars
/// carry no base machinery, so a relative IRI has nothing to resolve against
/// and `NotAbsolute` is genuinely reachable here. In Turtle the same reference
/// resolves against the in-scope base and never reaches the predicate.
#[test]
fn a_relative_iri_is_not_absolute_because_a_line_format_has_no_base() {
    let err = nt("<s> <http://p/> <http://o/> .\n").unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("relative IRI") && text.contains("N-Triples"),
        "expected the no-base explanation, got: {text}"
    );
    assert!(
        nq("<http://s/> <http://p/> <http://o/> <g> .\n").is_err(),
        "the graph label position is an IRI position too"
    );
}

/// The gap this wiring closed. ` ` is legal SOURCE — the byte scan cannot
/// object to it — and expands to a space, which is in the IRIREF exclusion
/// set. Before the shared predicate ran on the expanded value, the reader
/// checked only absoluteness and accepted this.
#[test]
fn an_escape_that_expands_to_a_forbidden_character_is_caught() {
    let err = nt("<http://example.org/a\\u0020b> <http://p/> <http://o/> .\n").unwrap_err();
    assert!(
        err.to_string().contains("is not an IRI"),
        "expected the shared predicate's verdict, got: {err}"
    );
    // The unescaped form is caught earlier, by the scan, with its own message.
    assert!(nt("<http://example.org/a b> <http://p/> <http://o/> .\n").is_err());
    // And a well-formed escape still parses.
    assert_eq!(
        nt("<http://example.org/a\\u0062> <http://p/> <http://o/> .\n").unwrap(),
        1
    );
}

/// The language-tag production is the shared predicate's, not a second loop.
/// `"string"@1` is the W3C case (`nt-syntax-bad-lang-01`).
#[test]
fn language_tags_go_through_the_shared_predicate() {
    for bad in ["\"s\"@1", "\"s\"@en-", "\"s\"@"] {
        let doc = format!("<http://s/> <http://p/> {bad} .\n");
        assert!(
            nt(&doc).is_err(),
            "`{bad}` is not a language tag, but the reader accepted it"
        );
    }
    for good in ["\"s\"@en", "\"s\"@en-GB-oed", "\"s\"@x-whatever-42"] {
        let doc = format!("<http://s/> <http://p/> {good} .\n");
        assert_eq!(nt(&doc).unwrap(), 1, "`{good}` is a valid language tag");
    }
}
