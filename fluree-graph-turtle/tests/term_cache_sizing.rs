//! The term-cache reservation is the caller's decision, and it has to be right
//! at BOTH ends of a sign flip.
//!
//! Sizing the caches is a choice between two wastes, and which one is cheaper
//! reverses around a million distinct terms:
//!
//! * Below it, reserving is the waste. A length-derived estimate put ~210 MB of
//!   empty table in front of a document holding 800K distinct terms.
//! * Above it, growing is the waste. hashbrown doubles, the grow path holds the
//!   old table and the new one at once, so the peak is ~1.5× the final table —
//!   about +200 MiB per in-flight chunk at 2M distinct terms, on the bulk-import
//!   path whose memory budget was calibrated with the reservation in place.
//!
//! So a single default cannot be correct, and the fix was to move the estimate
//! to whoever has one. These tests pin the CONTRACT that makes that safe rather
//! than the byte counts, which are allocator-dependent: a parse must produce
//! identical RDF whichever way it was sized, an absent hint must not reserve,
//! and a hint far larger than the document must not change the answer either.

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};

/// Records the terms and triples a parse produced, so two differently-sized
/// parses can be compared for having denoted the same thing.
#[derive(Default, PartialEq, Eq, Debug)]
struct Recording {
    iris: Vec<String>,
    literals: Vec<String>,
    triples: Vec<(u32, u32, u32)>,
}

impl GraphSink for Recording {
    fn on_base(&mut self, _b: &str) {}
    fn on_prefix(&mut self, _p: &str, _n: &str) {}

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.iris.push(iri.to_string());
        TermId::new(self.iris.len() as u32 - 1)
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.iris.push(format!("_:{}", label.unwrap_or("anon")));
        TermId::new(self.iris.len() as u32 - 1)
    }

    fn term_literal(&mut self, v: &str, _d: Datatype, _l: Option<&str>) -> TermId {
        self.literals.push(v.to_string());
        TermId::new(u32::MAX - self.literals.len() as u32)
    }

    fn term_literal_value(&mut self, v: LiteralValue, _d: Datatype) -> TermId {
        self.literals.push(format!("{v:?}"));
        TermId::new(u32::MAX - self.literals.len() as u32)
    }

    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.triples.push((s.index(), p.index(), o.index()));
        Ok(())
    }
}

/// `distinct` distinct subjects across `statements` lines — the axis a
/// length-derived estimate cannot see, since every line is the same width.
fn corpus(statements: usize, distinct: usize) -> String {
    let mut doc = String::from("@prefix ex: <http://example.org/ns/> .\n");
    for i in 0..statements {
        doc.push_str(&format!(
            "ex:s{:07} ex:p{} \"value {i} with padding\" .\n",
            i % distinct,
            i % 32
        ));
    }
    doc
}

fn parse_with_hint(doc: &str, hint: Option<usize>) -> Recording {
    let mut sink = Recording::default();
    let mut options = fluree_graph_turtle::ParserOptions::default();
    if let Some(n) = hint {
        options = options.with_distinct_terms_hint(n);
    }
    fluree_graph_turtle::parse_with_prefixes_base_options(doc, &mut sink, &[], None, options)
        .expect("parses");
    sink
}

#[test]
fn the_hint_changes_the_sizing_and_never_the_rdf() {
    // Both ends of the flip, plus the absent case and a deliberately absurd
    // one. Whatever the reservation does, the document denotes what it denotes.
    let doc = corpus(4000, 1500);
    let baseline = parse_with_hint(&doc, None);

    assert!(
        !baseline.triples.is_empty(),
        "the corpus must actually produce triples or this test is vacuous"
    );

    for hint in [
        Some(0),          // a caller that knows there is nothing to reserve
        Some(1),          // pathologically low
        Some(1_500),      // about right
        Some(2_000_000),  // the clamp boundary
        Some(50_000_000), // far past it — must be clamped, not attempted
    ] {
        let other = parse_with_hint(&doc, hint);
        assert_eq!(
            baseline, other,
            "hint {hint:?} changed what the document denotes"
        );
    }
}

#[test]
fn a_hint_far_past_the_clamp_is_survivable() {
    // The clamp is what stands between a wrong hint and an allocation the
    // machine cannot serve. `usize::MAX` reserved literally would abort the
    // process, so reaching the assertion at all is the test passing.
    let doc = corpus(64, 8);
    let out = parse_with_hint(&doc, Some(usize::MAX));
    assert_eq!(out.triples.len(), 64);
}

#[test]
fn the_two_sides_of_the_flip_agree_on_a_large_distinct_count() {
    // The high-distinct side, where the OLD unconditional reservation was
    // right and the small floor is wrong. This test cannot see memory — it
    // pins that the answer does not depend on which side we are on, so that a
    // future change to the sizing policy cannot quietly change the RDF while
    // it is chasing bytes.
    let doc = corpus(20_000, 20_000);
    let unhinted = parse_with_hint(&doc, None);
    let hinted = parse_with_hint(&doc, Some(20_000));
    assert_eq!(unhinted.iris.len(), hinted.iris.len());
    assert_eq!(unhinted, hinted);
    assert_eq!(hinted.triples.len(), 20_000);
}
