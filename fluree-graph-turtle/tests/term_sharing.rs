//! The parser hands its own `Arc<str>` to the sink, so a storing sink does not
//! allocate a second copy of every distinct IRI.
//!
//! The parser keeps an `Arc<str>` per distinct IRI as its cache key whether or
//! not anyone else wants it. Before `term_iri_shared` a sink that stores terms
//! had no way to reach that allocation and made its own — on a corpus with 800K
//! distinct terms, 800K allocations and about 37 MiB of duplicate bytes.
//!
//! Two properties are worth pinning, and neither is visible from the output:
//! the parser must actually take the sharing path (a silent fall-back to the
//! default body would put the copy back with no test failing), and what it
//! hands over must be the allocation it is itself holding rather than a fresh
//! one, which is what makes the clone free.

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::sync::Arc;

/// Records which IRI entry point the producer used, and keeps what it was
/// handed so the allocation can be inspected after the parse.
#[derive(Default)]
struct SharingSink {
    shared_calls: usize,
    copying_calls: usize,
    /// Every `Arc` handed over, cloned — the clone is the point: it is what a
    /// storing sink would do, and it must not be an allocation.
    kept: Vec<Arc<str>>,
    /// Strong count observed at the moment of receipt, after this sink's own
    /// clone. Two owners means the sink and the producer are looking at one
    /// allocation rather than two copies of the same bytes.
    owners_at_receipt: Vec<usize>,
}

impl GraphSink for SharingSink {
    fn on_base(&mut self, _b: &str) {}
    fn on_prefix(&mut self, _p: &str, _n: &str) {}

    fn term_iri(&mut self, _iri: &str) -> TermId {
        self.copying_calls += 1;
        TermId::new(0)
    }

    fn term_iri_shared(&mut self, iri: &Arc<str>) -> TermId {
        self.shared_calls += 1;
        self.kept.push(Arc::clone(iri));
        self.owners_at_receipt.push(Arc::strong_count(iri));
        TermId::new(0)
    }

    fn term_blank(&mut self, _label: Option<&str>) -> TermId {
        TermId::new(0)
    }

    fn term_literal(&mut self, _v: &str, _d: Datatype, _l: Option<&str>) -> TermId {
        TermId::new(0)
    }

    fn term_literal_value(&mut self, _v: LiteralValue, _d: Datatype) -> TermId {
        TermId::new(0)
    }

    fn emit_triple(&mut self, _s: TermId, _p: TermId, _o: TermId) -> SinkResult {
        Ok(())
    }
}

const DOC: &str = "@prefix ex: <http://example.org/> .\n\
                   ex:s1 ex:p <http://example.org/absolute> .\n\
                   ex:s2 ex:p ex:s1 .\n\
                   ex:s1 ex:p \"a literal\" .\n";

#[test]
fn the_parser_hands_over_its_own_allocation_and_never_the_copying_path() {
    let mut sink = SharingSink::default();
    fluree_graph_turtle::parse_with_options(
        DOC,
        &mut sink,
        fluree_graph_turtle::ParserOptions::conformant(),
    )
    .expect("parses");

    assert!(
        sink.shared_calls > 0,
        "the parser never took the sharing path"
    );
    assert_eq!(
        sink.copying_calls, 0,
        "the parser fell back to the copying path for {} IRI(s) — the default \
         body is a correctness fallback, not a route the parser should take",
        sink.copying_calls
    );

    // One call per DISTINCT IRI: the parser's cache answers repeats without
    // troubling the sink at all, which is why the sink's copy was pure waste.
    assert_eq!(
        sink.shared_calls, 4,
        "expected one call per distinct IRI (ex:s1, ex:s2, ex:p, ex:absolute), got {}",
        sink.shared_calls
    );

    // Two owners at receipt: the producer's, and the clone this sink just made.
    // What that pins is that the sink and the producer hold ONE allocation
    // rather than two copies of the same bytes.
    //
    // It does not pin an ordering, and an earlier version of this comment
    // claimed it did — that the producer hands the term over before caching it,
    // as though that were a choice made carefully. It is forced: the cache maps
    // the IRI to the TermId, and the TermId does not exist until this call
    // returns. Dressing a fact that cannot vary as a decision is the kind of
    // sentence that makes a reader trust the next claim less.
    //
    // The count also cannot see whether the producer allocates a SECOND Arc for
    // its cache key afterwards — that is what term_sharing_witness.rs measures.
    assert!(
        sink.owners_at_receipt.iter().all(|&n| n == 2),
        "a storing sink and the producer should be the only two owners of one \
         allocation: {:?}",
        sink.owners_at_receipt
    );
}

#[test]
fn the_shared_allocation_is_the_one_the_parser_keeps_caching_with() {
    // The parser holds its key for the whole parse, so a sink that kept a clone
    // is pointing at that same allocation and not at a copy of it. Observable
    // as the strong count still exceeding this sink's own hold after the parse
    // returns... except the parser is gone by then, which is exactly the case
    // the `Arc` exists to make safe: the bytes survive their producer.
    let mut sink = SharingSink::default();
    fluree_graph_turtle::parse_with_options(
        DOC,
        &mut sink,
        fluree_graph_turtle::ParserOptions::conformant(),
    )
    .expect("parses");

    let kept = std::mem::take(&mut sink.kept);
    for arc in &kept {
        assert_eq!(
            Arc::strong_count(arc),
            1,
            "the parser should have dropped its half when it went out of scope"
        );
    }
    let mut seen: Vec<&str> = kept.iter().map(|a| &**a).collect();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![
            "http://example.org/absolute",
            "http://example.org/p",
            "http://example.org/s1",
            "http://example.org/s2",
        ],
        "the shared allocations must hold the fully expanded IRIs"
    );
}
