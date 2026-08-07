//! A producer that declares statement scope and then caches an id must fail
//! loudly, in BOTH shapes — not just the one that is easy to catch.
//!
//! The guard's first form stamped the SLOT with the statement that last wrote
//! it. That catches the sparse shape, where the next statement is narrower and
//! the cached slot keeps an old stamp. It does not catch the dense shape, where
//! the next statement is at least as wide, the slot has already been re-minted
//! with the current stamp, and the stale id therefore reads as fresh — so the
//! writer emitted a different term than the producer named, silently. That is
//! the common shape, and it was the one that mattered.
//!
//! The stamp now lives in the ID, which carries the statement it was minted in
//! regardless of what happened to its slot. Both shapes panic.
//!
//! These tests assert on a debug-build guard, so they only mean anything in a
//! debug build; `debug_assert` compiles out in release, where the contract is
//! upheld by the producer rather than checked.

use fluree_graph_format::{NTriplesWriter, WriterConfig};
use fluree_graph_ir::{Datatype, GraphSink, TermScope};

/// The SPARSE violation: statement two is narrower, so the cached slots are
/// never re-minted.
#[test]
#[cfg(debug_assertions)]
fn a_cached_id_is_caught_when_the_next_statement_is_narrower() {
    let mut buf = Vec::new();
    let mut w = NTriplesWriter::with_config(&mut buf, &WriterConfig::new());
    w.declare_term_scope(TermScope::Statement);

    let s = w.term_iri("http://ex/ONE");
    let p = w.term_iri("http://ex/p");
    let o = w.term_iri("http://ex/o");
    w.emit_triple(s, p, o).unwrap();
    w.end_statement();

    let s2 = w.term_iri("http://ex/TWO");
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = w.emit_triple(s2, p, o);
    }));
    assert!(
        caught.is_err(),
        "a stale id survived the narrow-next-statement shape"
    );
}

/// The DENSE violation, and the reason this file exists: statement two mints at
/// least as many terms, so every cached slot has been re-minted and carries the
/// current statement. Under a slot-stamped guard this wrote `SUBJECT-TWO` where
/// the producer named `SUBJECT-ONE` and reported success.
#[test]
#[cfg(debug_assertions)]
fn a_cached_id_is_caught_when_the_next_statement_is_at_least_as_wide() {
    let mut buf = Vec::new();
    let mut w = NTriplesWriter::with_config(&mut buf, &WriterConfig::new());
    w.declare_term_scope(TermScope::Statement);

    let s1 = w.term_iri("http://ex/SUBJECT-ONE");
    let p1 = w.term_iri("http://ex/p");
    let o1 = w.term_iri("http://ex/o");
    w.emit_triple(s1, p1, o1).unwrap();
    w.end_statement();

    // Three fresh terms: every slot s1 could name has been rewritten.
    let _s2 = w.term_iri("http://ex/SUBJECT-TWO");
    let p2 = w.term_iri("http://ex/p");
    let o2 = w.term_iri("http://ex/o");

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // s1 is a statement-1 id. Its slot now holds SUBJECT-TWO.
        let _ = w.emit_triple(s1, p2, o2);
    }));
    assert!(
        caught.is_err(),
        "a stale id read as fresh because its slot had been re-minted — this is \
         the silent-corruption shape, and it must panic"
    );
}

/// The guard must not fire on correct use, or it is worthless: a producer that
/// mints fresh ids every statement runs unbothered for as long as it likes.
#[test]
fn correct_use_is_never_disturbed() {
    let mut buf = Vec::new();
    let mut w = NTriplesWriter::with_config(&mut buf, &WriterConfig::new());
    w.declare_term_scope(TermScope::Statement);
    for i in 0..300 {
        let s = w.term_iri(&format!("http://ex/s{i}"));
        let p = w.term_iri("http://ex/p");
        let o = w.term_literal(&format!("v{i}"), Datatype::xsd_string(), None);
        w.emit_triple(s, p, o).expect("emits");
        w.end_statement();
    }
    w.finish().expect("finishes");
    let out = String::from_utf8(buf).unwrap();
    assert_eq!(out.lines().count(), 300);
    assert!(out.contains("http://ex/s299"));
}
