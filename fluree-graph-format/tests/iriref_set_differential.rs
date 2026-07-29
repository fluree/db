//! One IRIREF exclusion set, three transcriptions, checked at every codepoint.
//!
//! `IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'` is transcribed
//! independently in more than one place in this workspace, and the copies serve
//! opposite directions: the reader decides what to ACCEPT, the validator decides
//! what to REJECT, the writer decides what to ESCAPE. A drift between any two is
//! silent and asymmetric — a character the writer emits raw but the reader
//! refuses makes output this project cannot read back, and one the validator
//! blesses but the writer escapes changes an IRI's identity on the way out.
//!
//! So the agreement is asserted over the whole of Unicode rather than over a
//! hand-picked list. A hand-picked list is exactly what each copy already has in
//! its own unit tests, and it is why they could disagree while all three looked
//! tested.
//!
//! Sited here because `fluree-graph-format` is the only crate that depends on
//! all of them. Authored by the review of the H-8 term-validation branch.
//!
//! # The fourth copy, deliberately not covered
//!
//! `fluree-db-sparql/src/lex/chars.rs` carries a fourth `is_iri_char`, byte-for
//! byte identical to `fluree-graph-ir`'s today. It is NOT tested here: this is a
//! light crate and cannot depend on the SPARQL engine, so reaching it would mean
//! either siting this test in a heavyweight crate or lifting that copy. Recorded
//! rather than quietly omitted — it is the copy most likely to drift, precisely
//! because nothing binds it to the others.

use fluree_graph_format::escape_iri_into;
use fluree_graph_ir::chars::is_iri_char;
use fluree_vocab::iri::is_forbidden_iri_char;

/// Every `char`, in order. Surrogate code points are not `char`s and cannot
/// appear in a Rust `str`, so they are outside the question being asked.
fn all_chars() -> impl Iterator<Item = char> {
    (0..=0x10_FFFF_u32).filter_map(char::from_u32)
}

/// The reader's accept-set and the validator's reject-set must be exact
/// complements. They are written as independent `matches!` arms in different
/// crates, in opposite polarity, which is how they would drift.
#[test]
fn the_reader_and_the_validator_agree_at_every_codepoint() {
    let mut disagreements = Vec::new();
    for c in all_chars() {
        if is_forbidden_iri_char(c) == is_iri_char(c) {
            disagreements.push(c);
            if disagreements.len() >= 16 {
                break;
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "is_forbidden_iri_char and !is_iri_char disagree at: {:?}",
        disagreements
            .iter()
            .map(|c| format!("U+{:04X}", *c as u32))
            .collect::<Vec<_>>()
    );
}

/// And the writer must escape exactly that set — checked through the real
/// escaping entry point rather than through the private predicate behind it, so
/// the test binds the *behavior* a reader will meet and not an implementation
/// detail that could be bypassed.
#[test]
fn the_writer_escapes_exactly_what_the_grammar_forbids() {
    let mut wrong = Vec::new();
    for c in all_chars() {
        let mut out = String::new();
        escape_iri_into(&mut out, &c.to_string());

        // A forbidden character must not survive literally; a permitted one
        // must survive exactly, and unescaped.
        let escaped = out != c.to_string();
        if escaped != is_forbidden_iri_char(c) {
            wrong.push((c, out));
            if wrong.len() >= 16 {
                break;
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "writer escaping disagrees with the forbidden set at: {:?}",
        wrong
            .iter()
            .map(|(c, out)| format!("U+{:04X} -> {out:?}", *c as u32))
            .collect::<Vec<_>>()
    );
}

/// The set is not merely self-consistent — it is the one the grammar names.
/// Spelled out literally so a change to all three copies at once still has to
/// argue with the spec.
#[test]
fn the_agreed_set_is_the_one_iriref_names() {
    for c in all_chars() {
        let by_spec = matches!(
            c,
            '\u{0}'..='\u{20}' | '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\'
        );
        assert_eq!(
            is_forbidden_iri_char(c),
            by_spec,
            "U+{:04X} disagrees with the IRIREF production",
            c as u32
        );
    }
}

/// The boundaries, named individually, because an off-by-one at either end of
/// the C0 range is the likeliest single-character drift and a whole-range scan
/// reports it as one entry among many.
#[test]
fn the_c0_boundaries_are_where_they_belong() {
    assert!(is_forbidden_iri_char('\u{1F}'), "U+001F is in the range");
    assert!(is_forbidden_iri_char('\u{20}'), "space is excluded too");
    assert!(!is_forbidden_iri_char('\u{21}'), "'!' is legal");
    // U+007F is NOT excluded: the production stops at U+0020, and DEL is not
    // one of the named delimiters. Over-excluding it would reject IRIs the
    // grammar accepts, which no negative test can catch.
    assert!(!is_forbidden_iri_char('\u{7F}'), "DEL is legal in IRIREF");
    assert!(!is_forbidden_iri_char('\u{9F}'), "C1 controls are legal");
}
