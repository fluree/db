//! The fourth `IRIREF` transcription, bound to the other three.
//!
//! `fluree-graph-format/tests/iriref_set_differential.rs` checks three copies
//! of `IRIREF ::= '<' ([^#x00-#x20<>"{}|^`\] | UCHAR)* '>'` at every codepoint
//! — the reader's accept-set, the validator's reject-set, and the writer's
//! escape-set — and records in its own module docs that a FOURTH copy in
//! `fluree-db-sparql` is deliberately not covered there, because a light crate
//! cannot depend on the SPARQL engine.
//!
//! This is that coverage. `fluree-db-cli` is the smallest crate that already
//! depends on both the light crates and the SPARQL engine, so the binding is a
//! dev-dependency and adds no production edge in either direction — which is
//! why the copy stays where it is instead of being lifted.
//!
//! It is the copy most likely to drift precisely because nothing else binds
//! it: the SPARQL lexer is maintained on its own, and a change there would
//! otherwise be invisible to the RDF side until a document round-tripped
//! wrong.

/// Every `char`, in order. Surrogate code points are not `char`s and cannot
/// appear in a Rust `str`, so they are outside the question being asked.
fn all_chars() -> impl Iterator<Item = char> {
    (0..=0x10_FFFF_u32).filter_map(char::from_u32)
}

/// The SPARQL lexer's accept-set and the RDF reader's accept-set are the same
/// production and must agree at every codepoint.
///
/// Asserted over the whole of Unicode rather than a hand-picked list, for the
/// same reason the three-way differential is: a hand-picked list is what each
/// copy already has in its own unit tests, and it is how they could disagree
/// while all four looked tested.
#[test]
fn the_sparql_lexer_agrees_with_the_rdf_reader_at_every_codepoint() {
    let mut disagreements = Vec::new();
    for c in all_chars() {
        if fluree_db_sparql::lex::is_iri_char(c) != fluree_graph_ir::chars::is_iri_char(c) {
            disagreements.push(c);
            if disagreements.len() >= 16 {
                break;
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "fluree-db-sparql and fluree-graph-ir disagree on IRIREF at: {:?}",
        disagreements
            .iter()
            .map(|c| format!("U+{:04X}", *c as u32))
            .collect::<Vec<_>>()
    );
}

/// And the same against the validator, in the opposite polarity — so a lift or
/// a rewrite of either side still has to face the whole set.
#[test]
fn the_sparql_lexer_is_the_exact_complement_of_the_validator() {
    let mut disagreements = Vec::new();
    for c in all_chars() {
        if fluree_vocab::iri::is_forbidden_iri_char(c) == fluree_db_sparql::lex::is_iri_char(c) {
            disagreements.push(c);
            if disagreements.len() >= 16 {
                break;
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "is_forbidden_iri_char and !sparql::is_iri_char disagree at: {:?}",
        disagreements
            .iter()
            .map(|c| format!("U+{:04X}", *c as u32))
            .collect::<Vec<_>>()
    );
}
